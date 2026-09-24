#include "server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <utility>

#include "reply.hpp"
#include "replication.hpp"

namespace {

constexpr int kBacklog = 511;  // Redis's default tcp-backlog
constexpr int kMaxEventsPerWait = 1024;
constexpr size_t kReadChunkSize = 16 * 1024;
// Upper bound on bytes buffered for a single not-yet-complete command
// (Redis: client-query-buffer-limit, default 1GB).
constexpr size_t kMaxQueryBufferBytes = 64 * 1024 * 1024;
// Upper bound on replies queued for a client that isn't reading them
// (Redis: client-output-buffer-limit). Without it, a client that pipelines
// GETs of large values and never reads can grow server memory unboundedly.
// (Replicas get a larger limit, enforced by Replication.)
constexpr size_t kMaxOutputBufferBytes = 64 * 1024 * 1024;
// A full-resync snapshot is streamed to the replica in chunks this big.
constexpr size_t kSnapshotChunkSize = 64 * 1024;

}  // namespace

Server::Server(std::string bind_addr, uint16_t port, CommandDispatcher& dispatcher)
    : bind_addr_(std::move(bind_addr)), port_(port), dispatcher_(dispatcher) {}

Server::~Server() {
    close_all();
    if (signal_fd_ >= 0) close(signal_fd_);
    if (epoll_fd_ >= 0) close(epoll_fd_);
    if (listen_fd_ >= 0) close(listen_fd_);
}

bool Server::start() {
    // SOCK_NONBLOCK: accept() on an empty queue must return EAGAIN, not block
    // the loop. SOCK_CLOEXEC: don't leak fds into any child process we fork
    // later (e.g. for background snapshotting).
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        std::perror("socket");
        return false;
    }

    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port_);
    if (inet_pton(AF_INET, bind_addr_.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "invalid bind address: " << bind_addr_ << "\n";
        return false;
    }
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("bind");
        return false;
    }
    if (listen(listen_fd_, kBacklog) < 0) {
        std::perror("listen");
        return false;
    }

    epoll_fd_ = epoll_create1(EPOLL_CLOEXEC);
    if (epoll_fd_ < 0) {
        std::perror("epoll_create1");
        return false;
    }

    // Turn SIGINT/SIGTERM into readable events on an fd instead of async
    // handlers. Blocking them first means they queue on the signalfd rather
    // than interrupting us, so there's no race between "check a stop flag"
    // and "sleep in epoll_wait".
    sigset_t mask;
    sigemptyset(&mask);
    sigaddset(&mask, SIGINT);
    sigaddset(&mask, SIGTERM);
    if (sigprocmask(SIG_BLOCK, &mask, nullptr) < 0) {
        std::perror("sigprocmask");
        return false;
    }
    signal_fd_ = signalfd(-1, &mask, SFD_NONBLOCK | SFD_CLOEXEC);
    if (signal_fd_ < 0) {
        std::perror("signalfd");
        return false;
    }

    for (int fd : {listen_fd_, signal_fd_}) {
        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::perror("epoll_ctl");
            return false;
        }
    }

    std::cout << "kv_server listening on " << bind_addr_ << ":" << port_ << "\n";
    return true;
}

void Server::set_cron(std::chrono::milliseconds interval, std::function<void()> task) {
    cron_interval_ = interval;
    cron_task_ = std::move(task);
    next_cron_ = std::chrono::steady_clock::now() + interval;
}

void Server::set_before_sleep(std::function<void()> hook) {
    before_sleep_ = std::move(hook);
}

// How long epoll_wait may sleep: until the next cron tick, or forever if
// there's no cron. This is how timers live inside an event loop without a
// separate thread — the wait itself is the timer.
int Server::cron_timeout_ms() const {
    if (!cron_task_) {
        return -1;
    }
    auto remaining = next_cron_ - std::chrono::steady_clock::now();
    if (remaining <= std::chrono::steady_clock::duration::zero()) {
        return 0;
    }
    // Round up, so we don't wake a fraction of a millisecond early and spin.
    return static_cast<int>(std::chrono::ceil<std::chrono::milliseconds>(remaining).count());
}

void Server::run_cron_if_due() {
    if (!cron_task_) {
        return;
    }
    auto now = std::chrono::steady_clock::now();
    if (now < next_cron_) {
        return;
    }
    cron_task_();
    // Schedule from now rather than next_cron_ + interval: if a tick ran
    // late, don't fire a burst of catch-up ticks back to back.
    next_cron_ = now + cron_interval_;
}

void Server::run() {
    std::vector<epoll_event> events(kMaxEventsPerWait);
    bool running = true;

    while (running) {
        int n = epoll_wait(epoll_fd_, events.data(), kMaxEventsPerWait, cron_timeout_ms());
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("epoll_wait");
            break;
        }

        for (int i = 0; i < n; ++i) {
            int fd = events[i].data.fd;
            uint32_t ev = events[i].events;

            if (fd == listen_fd_) {
                accept_clients();
                continue;
            }
            if (fd == signal_fd_) {
                std::cout << "shutdown signal received\n";
                running = false;
                continue;
            }
            if (auto w = watchers_.find(fd); w != watchers_.end()) {
                // Call a copy: the callback may unwatch or re-watch its own
                // fd, destroying the stored std::function mid-call.
                std::function<void()> on_ready = w->second;
                on_ready();
                continue;
            }

            auto it = conns_.find(fd);
            if (it == conns_.end() || it->second->closing) {
                continue;
            }
            Client& conn = *it->second;

            // EPOLLERR/EPOLLHUP are routed through the read path on purpose:
            // read() then returns 0 (peer closed) or -1 with the real errno
            // (e.g. ECONNRESET), and it also drains any bytes that arrived
            // just before the hangup.
            if (ev & (EPOLLIN | EPOLLERR | EPOLLHUP)) {
                handle_readable(conn);
            }
            if ((ev & EPOLLOUT) && !conn.closing) {
                flush(conn);
            }
        }

        // Checked after every wakeup, not only on timeout: under constant
        // client traffic epoll_wait may never time out at all.
        run_cron_if_due();
        // Before before_sleep, so the writes of resumed clients reach the
        // AOF flush below ahead of their replies.
        if (resume_held_) {
            resume_held_clients();
        }
        // Persist first, reply second. Deferring replies to here also
        // batches: every write from this iteration, across all clients,
        // shares one AOF write (and one fsync under appendfsync always).
        if (before_sleep_) {
            before_sleep_();
        }
        flush_pending_writes();
        // Closes are deferred to here: if we close()d mid-batch, a later
        // accept() in the same batch could be handed the same fd number, and
        // a stale event still queued for the old client would be applied to
        // the new one.
        close_pending();
    }

    close_all();
}

void Server::accept_clients() {
    // Level-triggered, but drain the backlog anyway: one wakeup per accepted
    // client would waste a syscall round-trip per connection under a burst.
    while (true) {
        sockaddr_in peer{};
        socklen_t peer_len = sizeof(peer);
        int fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &peer_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                // e.g. EMFILE (out of fds). Known limitation: the pending
                // connection stays queued, so level-triggered epoll will keep
                // waking us until an fd frees up.
                std::perror("accept4");
            }
            return;
        }

        // Disable Nagle's algorithm: replies are small and latency-sensitive,
        // and Nagle + the client's delayed ACK can stall a reply by ~40ms.
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

        epoll_event ev{};
        ev.events = EPOLLIN;
        ev.data.fd = fd;
        if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
            std::perror("epoll_ctl");
            close(fd);
            continue;
        }
        Client& c = add_client(fd);
        char ip[INET_ADDRSTRLEN] = "?";
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        c.addr = ip;
    }
}

Client& Server::add_client(int fd) {
    auto client = std::make_unique<Client>(next_client_id_++, fd);
    Client& c = *client;
    conns_[fd] = std::move(client);
    return c;
}

Client& Server::adopt_master(int fd, const std::string& pending) {
    Client& c = add_client(fd);
    c.is_master = true;
    c.parser.feed(pending.data(), pending.size());
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_ADD, fd, &ev) < 0) {
        std::perror("epoll_ctl");
        mark_closing(c);  // replication reconnects when it's reaped
    }
    return c;
}

bool Server::watch(int fd, bool want_write, std::function<void()> on_ready) {
    epoll_event ev{};
    ev.events = want_write ? EPOLLOUT : EPOLLIN;
    ev.data.fd = fd;
    int op = watchers_.count(fd) != 0 ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    if (epoll_ctl(epoll_fd_, op, fd, &ev) < 0) {
        std::perror("epoll_ctl");
        return false;
    }
    watchers_[fd] = std::move(on_ready);
    return true;
}

void Server::unwatch(int fd) {
    if (watchers_.erase(fd) != 0) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
    }
}

void Server::handle_readable(Client& conn) {
    // One read per wakeup, not read-until-EAGAIN: with level-triggered epoll
    // any leftover bytes re-trigger next iteration, and a single client
    // blasting data can't starve everyone else in this batch.
    char buf[kReadChunkSize];
    ssize_t n = read(conn.fd, buf, sizeof(buf));
    if (n == 0) {
        mark_closing(conn);  // peer closed
        return;
    }
    if (n < 0) {
        if (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR) {
            return;
        }
        mark_closing(conn);  // e.g. ECONNRESET
        return;
    }
    if (conn.close_after_flush) {
        return;  // already hit a protocol error; ignore further input
    }
    conn.last_interaction = std::chrono::steady_clock::now();
    conn.parser.feed(buf, static_cast<size_t>(n));
    process_input(conn);
}

void Server::set_writes_paused(bool paused) {
    if (paused == writes_paused_) {
        return;
    }
    writes_paused_ = paused;
    // Deferred to the loop rather than run here: the caller may be in the
    // middle of handling a cluster bus message.
    resume_held_ = !paused;
    std::cout << (paused ? "client writes paused\n" : "client writes unpaused\n");
}

void Server::resume_held_clients() {
    resume_held_ = false;
    // fds here can't have been reused: closes only happen at the end of an
    // iteration, and a closed client's fd is dropped from the list there.
    for (int fd : std::exchange(held_clients_, {})) {
        auto it = conns_.find(fd);
        if (it != conns_.end() && it->second->held_command && !it->second->closing) {
            process_input(*it->second);
        }
    }
}

void Server::process_buffered(Client& conn) {
    if (!conn.closing) {
        process_input(conn);
    }
}

void Server::process_input(Client& conn) {
    std::string raw;
    std::string discarded;
    // A client parked by WAIT keeps buffering input but runs nothing until
    // it's unblocked, so its replies stay in order.
    while (!conn.blocked && !conn.close_after_flush && !conn.closing) {
        RespParser::ParseResult result{RespParser::Status::kIncomplete, {}};
        if (conn.held_command) {
            // Held by a write pause: nothing behind it may overtake it.
            if (writes_paused_) {
                break;
            }
            result.status = RespParser::Status::kComplete;
            result.command = std::move(*conn.held_command);
            conn.held_command.reset();
        } else {
            result = conn.parser.try_parse_command(conn.is_master ? &raw : nullptr);
        }
        if (result.status == RespParser::Status::kIncomplete) {
            break;
        }
        if (result.status == RespParser::Status::kProtocolError) {
            if (conn.is_master) {
                std::cerr << "protocol error in the master's stream; dropping the link\n";
                mark_closing(conn);
                return;
            }
            // Framing is broken: we can't tell where the next command starts.
            reply::error(conn.out, "ERR Protocol error");
            conn.close_after_flush = true;
            break;
        }
        if (conn.is_master) {
            // Applied without a reply, then counted toward our offset.
            dispatcher_.dispatch(result.command, discarded, &conn);
            replication_->master_command_applied(raw);
        } else if (conn.repl_state != Client::ReplState::kNone) {
            // Once a connection carries the replication stream, a reply
            // would be read as stream data by the replica. (Only REPLCONF
            // ACK should arrive here anyway.)
            dispatcher_.dispatch(result.command, discarded, &conn);
        } else if (writes_paused_ && dispatcher_.is_write(result.command)) {
            // Held rather than refused: once the pause ends it runs, and
            // if this node has lost its slots meanwhile it gets MOVED and
            // the client retries at the new master.
            conn.held_command = std::move(result.command);
            held_clients_.push_back(conn.fd);
            break;
        } else {
            uint64_t before = replication_ != nullptr ? replication_->offset() : 0;
            dispatcher_.dispatch(result.command, conn.out, &conn);
            if (replication_ != nullptr && replication_->offset() != before) {
                conn.woff = replication_->offset();  // it wrote: WAIT counts from here
            }
        }
        discarded.clear();
    }

    if (!conn.close_after_flush && conn.parser.buffered_bytes() > kMaxQueryBufferBytes) {
        reply::error(conn.out, "ERR Protocol error: query buffer limit exceeded");
        conn.close_after_flush = true;
    }

    queue_write(conn);
}

void Server::queue_write(Client& conn) {
    if (!conn.pending_write) {
        conn.pending_write = true;
        pending_writes_.push_back(conn.fd);
    }
}

void Server::flush_pending_writes() {
    // fds here can't have been reused: closes only happen after this.
    for (int fd : pending_writes_) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) {
            continue;
        }
        Client& conn = *it->second;
        conn.pending_write = false;
        if (!conn.closing) {
            flush(conn);
        }
    }
    pending_writes_.clear();
}

void Server::flush(Client& conn) {
    if (conn.repl_snapshot_fd >= 0 && !refill_snapshot(conn)) {
        return;
    }
    // Write optimistically right away: the socket send buffer is almost
    // always free, so most replies go out without ever touching EPOLLOUT.
    size_t sent = 0;
    while (sent < conn.out.size()) {
        ssize_t n = send(conn.fd, conn.out.data() + sent, conn.out.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;  // kernel send buffer full; resume on EPOLLOUT
            }
            mark_closing(conn);  // e.g. EPIPE / ECONNRESET
            return;
        }
        sent += static_cast<size_t>(n);
    }
    conn.out.erase(0, sent);

    // Mid-snapshot, an empty buffer just means "read the next chunk": stay
    // subscribed to EPOLLOUT so we come back for it.
    bool sending_snapshot = conn.repl_state == Client::ReplState::kSendBulk;
    if (conn.out.empty() && !sending_snapshot) {
        set_write_interest(conn, false);
        if (conn.close_after_flush) {
            mark_closing(conn);
        }
        return;
    }

    if (conn.repl_state == Client::ReplState::kNone && conn.out.size() > kMaxOutputBufferBytes) {
        std::cerr << "closing client fd " << conn.fd << ": output buffer limit exceeded\n";
        mark_closing(conn);
        return;
    }
    // Only ask for EPOLLOUT while there's a backlog. Under level-triggered
    // mode an idle socket is always writable, so leaving EPOLLOUT on
    // permanently would make epoll_wait return immediately forever.
    set_write_interest(conn, true);
}

// Streams a full-resync snapshot to a replica one chunk per loop iteration,
// topping up only once the previous chunk has mostly gone out: the file is
// never held in memory whole, and one big transfer can't monopolize the
// loop. Returns false if the client was closed.
bool Server::refill_snapshot(Client& conn) {
    if (conn.out.size() >= kSnapshotChunkSize) {
        return true;
    }
    char buf[kSnapshotChunkSize];
    ssize_t n = read(conn.repl_snapshot_fd, buf, sizeof(buf));
    if (n < 0) {
        if (errno == EINTR) {
            return true;
        }
        std::perror("reading the snapshot for a replica");
        mark_closing(conn);
        return false;
    }
    if (n == 0) {
        replication_->snapshot_sent(conn);  // queues the writes held back meanwhile
        return true;
    }
    conn.out.append(buf, static_cast<size_t>(n));
    return true;
}

void Server::set_write_interest(Client& conn, bool enabled) {
    if (conn.write_interest == enabled) {
        return;
    }
    epoll_event ev{};
    ev.events = EPOLLIN | (enabled ? static_cast<uint32_t>(EPOLLOUT) : 0u);
    ev.data.fd = conn.fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &ev) < 0) {
        std::perror("epoll_ctl");
        mark_closing(conn);
        return;
    }
    conn.write_interest = enabled;
}

void Server::mark_closing(Client& conn) {
    if (!conn.closing) {
        conn.closing = true;
        pending_close_.push_back(conn.fd);
    }
}

void Server::close_pending() {
    for (int fd : pending_close_) {
        auto it = conns_.find(fd);
        if (it == conns_.end()) {
            continue;
        }
        if (replication_ != nullptr) {
            replication_->client_closed(*it->second);
        }
        // close() alone also removes the fd from the epoll set, but only once
        // no other descriptor refers to the same socket — explicit DEL is
        // the unambiguous version.
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        conns_.erase(it);
        held_clients_.erase(std::remove(held_clients_.begin(), held_clients_.end(), fd), held_clients_.end());
    }
    pending_close_.clear();
}

void Server::close_all() {
    for (auto& [fd, conn] : conns_) {
        if (replication_ != nullptr) {
            replication_->client_closed(*conn);
        }
        close(fd);
    }
    conns_.clear();
    for (auto& [fd, on_ready] : watchers_) {
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);  // the watcher's owner closes the fd
    }
    watchers_.clear();
    pending_writes_.clear();
    pending_close_.clear();
}
