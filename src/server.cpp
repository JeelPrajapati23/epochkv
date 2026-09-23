#include "server.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <sys/signalfd.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <csignal>
#include <cstdio>
#include <iostream>
#include <utility>

#include "reply.hpp"

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
constexpr size_t kMaxOutputBufferBytes = 64 * 1024 * 1024;

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

void Server::run() {
    std::vector<epoll_event> events(kMaxEventsPerWait);
    bool running = true;

    while (running) {
        int n = epoll_wait(epoll_fd_, events.data(), kMaxEventsPerWait, -1);
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

            auto it = conns_.find(fd);
            if (it == conns_.end() || it->second->closing) {
                continue;
            }
            Connection& conn = *it->second;

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
        int fd = accept4(listen_fd_, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
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
        conns_.emplace(fd, std::make_unique<Connection>(fd));
    }
}

void Server::handle_readable(Connection& conn) {
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

    conn.parser.feed(buf, static_cast<size_t>(n));

    while (true) {
        RespParser::ParseResult result = conn.parser.try_parse_command();
        if (result.status == RespParser::Status::kIncomplete) {
            break;
        }
        if (result.status == RespParser::Status::kProtocolError) {
            // Framing is broken: we can't tell where the next command starts.
            reply::error(conn.out, "ERR Protocol error");
            conn.close_after_flush = true;
            break;
        }
        dispatcher_.dispatch(result.command, conn.out);
    }

    if (!conn.close_after_flush && conn.parser.buffered_bytes() > kMaxQueryBufferBytes) {
        reply::error(conn.out, "ERR Protocol error: query buffer limit exceeded");
        conn.close_after_flush = true;
    }

    flush(conn);
}

void Server::flush(Connection& conn) {
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

    if (conn.out.empty()) {
        set_write_interest(conn, false);
        if (conn.close_after_flush) {
            mark_closing(conn);
        }
        return;
    }

    if (conn.out.size() > kMaxOutputBufferBytes) {
        std::cerr << "closing client fd " << conn.fd << ": output buffer limit exceeded\n";
        mark_closing(conn);
        return;
    }
    // Only ask for EPOLLOUT while there's a backlog. Under level-triggered
    // mode an idle socket is always writable, so leaving EPOLLOUT on
    // permanently would make epoll_wait return immediately forever.
    set_write_interest(conn, true);
}

void Server::set_write_interest(Connection& conn, bool enabled) {
    if (conn.write_interest == enabled) {
        return;
    }
    epoll_event ev{};
    ev.events = EPOLLIN | (enabled ? EPOLLOUT : 0);
    ev.data.fd = conn.fd;
    if (epoll_ctl(epoll_fd_, EPOLL_CTL_MOD, conn.fd, &ev) < 0) {
        std::perror("epoll_ctl");
        mark_closing(conn);
        return;
    }
    conn.write_interest = enabled;
}

void Server::mark_closing(Connection& conn) {
    if (!conn.closing) {
        conn.closing = true;
        pending_close_.push_back(conn.fd);
    }
}

void Server::close_pending() {
    for (int fd : pending_close_) {
        // close() alone also removes the fd from the epoll set, but only once
        // no other descriptor refers to the same socket — explicit DEL is
        // the unambiguous version.
        epoll_ctl(epoll_fd_, EPOLL_CTL_DEL, fd, nullptr);
        close(fd);
        conns_.erase(fd);
    }
    pending_close_.clear();
}

void Server::close_all() {
    for (auto& [fd, conn] : conns_) {
        close(fd);
    }
    conns_.clear();
    pending_close_.clear();
}
