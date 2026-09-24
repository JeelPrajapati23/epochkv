#include "replication.hpp"

#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <random>
#include <sstream>
#include <utility>

#include "file_util.hpp"
#include "persistence.hpp"
#include "reply.hpp"
#include "store.hpp"

namespace {

using ReplState = Client::ReplState;

constexpr size_t kReplIdLength = 40;
constexpr std::chrono::seconds kReconnectDelay{1};
constexpr std::chrono::seconds kCronSecond{1};
constexpr size_t kLinkReadChunk = 16 * 1024;
// A handshake reply is one short line; a longer one means garbage.
constexpr size_t kMaxHandshakeLine = 64 * 1024;
// Redis's client-output-buffer-limit for replicas (256mb hard limit). A
// replica this far behind is dropped; it reconnects and resyncs.
constexpr size_t kMaxReplicaBufferBytes = 256 * 1024 * 1024;

std::string random_replid() {
    static const char kHex[] = "0123456789abcdef";
    std::random_device rd;
    std::mt19937_64 rng((static_cast<uint64_t>(rd()) << 32) ^ rd());
    std::string id(kReplIdLength, '0');
    for (char& c : id) {
        c = kHex[rng() % 16];
    }
    return id;
}

const char* state_name(ReplState s) {
    switch (s) {
        case ReplState::kWaitBgsaveStart:
        case ReplState::kWaitBgsaveEnd:
            return "wait_bgsave";
        case ReplState::kSendBulk:
            return "send_bulk";
        case ReplState::kOnline:
            return "online";
        case ReplState::kNone:
            break;
    }
    return "none";
}

std::chrono::steady_clock::time_point now() {
    return std::chrono::steady_clock::now();
}

}  // namespace

Replication::Replication(Store& store, Persistence& persistence, Options options)
    : store_(store),
      persistence_(persistence),
      options_(options),
      replid_(random_replid()),
      replid2_(kReplIdLength, '0'),
      backlog_(options.backlog_size) {
    persistence_.set_snapshot_listener([this](bool ok) { snapshot_finished(ok); });
}

Replication::~Replication() {
    persistence_.set_snapshot_listener(nullptr);
    // The host may already be gone: close our own fds without it.
    if (link_fd_ >= 0) {
        close(link_fd_);
    }
    if (transfer_fd_ >= 0) {
        close(transfer_fd_);
    }
    if (!transfer_path_.empty()) {
        unlink(transfer_path_.c_str());
    }
}

// --- role changes -----------------------------------------------------------

void Replication::replicaof(const std::string& host, uint16_t port) {
    if (host == master_host_ && port == master_port_) {
        return;
    }
    close_link();
    master_host_ = host;
    master_port_ = port;
    link_state_ = LinkState::kConnect;
    next_connect_ = now();
    // From now on the master decides when keys expire.
    store_.set_passive_expiry(true);
    // Our replicas were following us as a master. They reconnect, and since
    // we keep our replid and offset, they can continue from where they were
    // once our own link is up.
    disconnect_replicas();
    for (Waiter& w : std::exchange(waiters_, {})) {
        reply::error(w.client->out,
                     "UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)");
        w.client->blocked = false;
        to_resume_.push_back(w.client);
    }
    std::cout << "replicating from " << host << ":" << port << "\n";
}

void Replication::replicaof_no_one() {
    if (!is_replica()) {
        return;
    }
    close_link();
    master_host_.clear();
    master_port_ = 0;
    link_state_ = LinkState::kNone;
    shift_replid();
    store_.set_passive_expiry(false);
    // Tell our replicas about the new replid by making them reconnect. They
    // ask to continue the old one (now replid2_) and get a partial resync.
    disconnect_replicas();
    std::cout << "promoted to master: new replid " << replid_ << ", previous history valid up to offset "
              << *second_replid_offset_ << "\n";
}

// Our history continues, but under a new name: we're no longer following
// the old master, so later writes are ours alone. Another replica of that
// master can still continue from us, up to the point where we diverged.
void Replication::shift_replid() {
    replid2_ = replid_;
    second_replid_offset_ = offset();
    replid_ = random_replid();
    replid_shared_ = false;
}

// --- master side --------------------------------------------------------------

void Replication::propagate(const Args& argv) {
    if (is_replica()) {
        return;
    }
    std::string cmd;
    reply::command(cmd, argv);
    feed(cmd);
}

void Replication::master_command_applied(const std::string& raw) {
    // Byte for byte what our master sent: our offset has to keep matching
    // its offset, and our replicas get exactly its stream.
    feed(raw);
}

void Replication::feed(const std::string& data) {
    backlog_.append(data);
    for (Client* r : replicas_) {
        if (r->closing) {
            continue;
        }
        switch (r->repl_state) {
            case ReplState::kOnline:
                r->out += data;
                host_->queue_write(*r);
                break;
            case ReplState::kWaitBgsaveEnd:
            case ReplState::kSendBulk:
                // Not in the snapshot being made; goes out right after it.
                r->repl_held += data;
                break;
            case ReplState::kWaitBgsaveStart:  // its snapshot will include this write
            case ReplState::kNone:
                continue;
        }
        if (r->out.size() + r->repl_held.size() > kMaxReplicaBufferBytes) {
            std::cerr << "dropping replica " << r->addr << ":" << r->repl_listening_port
                      << ": output buffer limit exceeded\n";
            host_->close_client(*r);
        }
    }
}

void Replication::psync(Client& c, const std::string& replid, int64_t offset, std::string& out) {
    if (c.repl_state != ReplState::kNone || c.is_master) {
        reply::error(out, "ERR PSYNC on a connection that is already replicating");
        return;
    }
    // A replica whose own link is down has no consistent history to offer.
    if (is_replica() && link_state_ != LinkState::kConnected) {
        reply::error(out, "NOMASTERLINK Can't SYNC while not connected with my master");
        return;
    }
    c.repl_ack_time = now();
    if (try_partial_resync(c, replid, offset, out)) {
        ++sync_partial_ok_;
        return;
    }
    if (replid != "?") {
        ++sync_partial_err_;
    }
    ++sync_full_;
    std::cout << "replica " << c.addr << ":" << c.repl_listening_port << " needs a full resync\n";
    c.repl_state = ReplState::kWaitBgsaveStart;
    replicas_.push_back(&c);
    start_snapshot_for_replicas();
}

bool Replication::try_partial_resync(Client& c, const std::string& replid, int64_t offset, std::string& out) {
    if (offset < 0) {
        return false;
    }
    uint64_t from = static_cast<uint64_t>(offset);
    bool same_history = replid == replid_ ||
                        (replid == replid2_ && second_replid_offset_ && from <= *second_replid_offset_);
    if (!same_history || !backlog_.contains(from)) {
        return false;
    }
    reply::simple_string(out, "CONTINUE " + replid_);
    backlog_.copy_from(from, out);
    c.repl_state = ReplState::kOnline;
    c.repl_ack_offset = from;
    replicas_.push_back(&c);
    replid_shared_ = true;
    std::cout << "partial resync with replica " << c.addr << ":" << c.repl_listening_port << ": sending "
              << (backlog_.offset() - from) << " bytes from offset " << from << "\n";
    return true;
}

void Replication::start_snapshot_for_replicas() {
    bool waiting = std::any_of(replicas_.begin(), replicas_.end(),
                               [](Client* r) { return r->repl_state == ReplState::kWaitBgsaveStart; });
    if (!waiting || persistence_.child_running()) {
        return;  // cron() retries once the running child is done
    }
    std::string error;
    if (!persistence_.bgsave(error)) {
        std::cerr << "can't start the snapshot for a full resync: " << error << "\n";
        for (Client* r : replicas_) {
            if (r->repl_state == ReplState::kWaitBgsaveStart) {
                host_->close_client(*r);  // it reconnects and tries again
            }
        }
        return;
    }
    // fork() just froze the dataset at exactly offset(): the snapshot plus
    // every stream byte from here on reproduces our current state.
    for (Client* r : replicas_) {
        if (r->repl_state == ReplState::kWaitBgsaveStart) {
            r->repl_state = ReplState::kWaitBgsaveEnd;
            reply::simple_string(r->out, "FULLRESYNC " + replid_ + " " + std::to_string(offset()));
            host_->queue_write(*r);
        }
    }
    replid_shared_ = true;
}

void Replication::snapshot_finished(bool ok) {
    for (Client* r : replicas_) {
        if (r->repl_state != ReplState::kWaitBgsaveEnd || r->closing) {
            continue;
        }
        if (!ok) {
            std::cerr << "snapshot for replica " << r->addr << " failed; dropping it\n";
            host_->close_client(*r);
            continue;
        }
        // Each replica gets its own fd, so each has its own read position.
        // Opened now: a later save may rename a new file over the path, but
        // this fd keeps reading the snapshot we promised.
        int fd = open(persistence_.snapshot_path().c_str(), O_RDONLY | O_CLOEXEC);
        struct stat st;
        if (fd < 0 || fstat(fd, &st) < 0) {
            std::cerr << "can't open the snapshot for replica " << r->addr << ": " << std::strerror(errno) << "\n";
            if (fd >= 0) {
                close(fd);
            }
            host_->close_client(*r);
            continue;
        }
        r->repl_snapshot_fd = fd;
        r->out += "$" + std::to_string(st.st_size) + "\r\n";
        r->repl_state = ReplState::kSendBulk;
        host_->queue_write(*r);
    }
}

void Replication::snapshot_sent(Client& c) {
    close(c.repl_snapshot_fd);
    c.repl_snapshot_fd = -1;
    c.out += c.repl_held;
    std::string().swap(c.repl_held);  // release the memory, not just the contents
    c.repl_state = ReplState::kOnline;
    c.repl_ack_time = now();
    std::cout << "replica " << c.addr << ":" << c.repl_listening_port << " is online\n";
}

void Replication::replconf_ack(Client& c, uint64_t offset) {
    if (c.repl_state == ReplState::kNone) {
        return;
    }
    c.repl_ack_offset = std::max(c.repl_ack_offset, offset);
    c.repl_ack_time = now();
}

void Replication::replconf_getack() {
    if (master_ != nullptr) {
        send_ack();
    }
}

bool Replication::wait(Client& c, int64_t numreplicas, int64_t timeout_ms, std::string& out) {
    if (is_replica()) {
        reply::error(out, "ERR WAIT cannot be used with replica instances");
        return false;
    }
    int64_t acked = acked_replicas(c.woff);
    if (acked >= numreplicas) {
        reply::integer(out, acked);
        return false;
    }
    std::optional<SteadyTime> deadline;
    if (timeout_ms > 0) {
        deadline = now() + std::chrono::milliseconds(timeout_ms);
    }
    waiters_.push_back({&c, c.woff, numreplicas, deadline});
    c.blocked = true;
    // Replicas only ACK once a second on their own; ask for one right away.
    get_ack_pending_ = true;
    return true;
}

int64_t Replication::acked_replicas(uint64_t offset) const {
    return std::count_if(replicas_.begin(), replicas_.end(), [offset](const Client* r) {
        return r->repl_state == ReplState::kOnline && r->repl_ack_offset >= offset;
    });
}

void Replication::unblock_waiters() {
    if (waiters_.empty()) {
        return;
    }
    auto t = now();
    std::vector<std::pair<Client*, int64_t>> done;
    for (auto it = waiters_.begin(); it != waiters_.end();) {
        int64_t acked = acked_replicas(it->offset);
        if (acked >= it->numreplicas || (it->deadline && t >= *it->deadline)) {
            done.emplace_back(it->client, acked);
            it = waiters_.erase(it);
        } else {
            ++it;
        }
    }
    // Resumed only after the loop: running their buffered commands may
    // park them again, which appends to waiters_.
    for (auto [c, acked] : done) {
        reply::integer(c->out, acked);
        c->blocked = false;
        host_->process_buffered(*c);
    }
}

void Replication::disconnect_replicas() {
    for (Client* r : replicas_) {
        host_->close_client(*r);
    }
}

// --- replica side -----------------------------------------------------------

void Replication::connect_to_master() {
    next_connect_ = now() + kReconnectDelay;  // if this attempt fails
    // getaddrinfo() blocks. Fine for an IP or /etc/hosts name; a slow DNS
    // server would stall the loop here (Redis has the same limitation).
    addrinfo hints{};
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;
    addrinfo* res = nullptr;
    int rc = getaddrinfo(master_host_.c_str(), std::to_string(master_port_).c_str(), &hints, &res);
    if (rc != 0) {
        std::cerr << "can't resolve master " << master_host_ << ": " << gai_strerror(rc) << "\n";
        return;
    }
    // Non-blocking connect: returns EINPROGRESS at once and the socket turns
    // writable when the handshake completes. A blocking connect() to a dead
    // host would freeze every client for the whole TCP timeout.
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0 || (connect(fd, res->ai_addr, res->ai_addrlen) < 0 && errno != EINPROGRESS) ||
        !host_->watch(fd, true, [this] { on_link_ready(); })) {
        std::cerr << "can't connect to master " << master_host_ << ":" << master_port_ << ": "
                  << std::strerror(errno) << "\n";
        freeaddrinfo(res);
        if (fd >= 0) {
            close(fd);
        }
        return;
    }
    freeaddrinfo(res);
    link_fd_ = fd;
    link_state_ = LinkState::kConnecting;
    link_last_io_ = now();
    link_buf_.clear();
    std::cout << "connecting to master " << master_host_ << ":" << master_port_ << "\n";
}

void Replication::on_link_ready() {
    if (link_state_ != LinkState::kConnecting) {
        read_link();
        return;
    }
    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(link_fd_, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
        err = errno;
    }
    if (err != 0) {
        abort_link(std::string("connect: ") + std::strerror(err));
        return;
    }
    int one = 1;
    setsockopt(link_fd_, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    // The whole handshake is pipelined; replies come back in order.
    std::string hs;
    reply::command(hs, {"PING"});
    reply::command(hs, {"REPLCONF", "listening-port", std::to_string(options_.listening_port)});
    reply::command(hs, {"REPLCONF", "capa", "psync2"});
    if (replid_shared_) {
        reply::command(hs, {"PSYNC", replid_, std::to_string(offset())});
    } else {
        reply::command(hs, {"PSYNC", "?", "-1"});
    }
    // A fresh socket's send buffer is empty, so a few hundred bytes fit.
    ssize_t n = send(link_fd_, hs.data(), hs.size(), MSG_NOSIGNAL);
    if (n != static_cast<ssize_t>(hs.size())) {
        abort_link("sending the handshake failed");
        return;
    }
    host_->watch(link_fd_, false, [this] { on_link_ready(); });
    link_state_ = LinkState::kHandshake;
    handshake_replies_ = 0;
    link_last_io_ = now();
}

void Replication::read_link() {
    char buf[kLinkReadChunk];
    ssize_t n = read(link_fd_, buf, sizeof(buf));
    if (n == 0) {
        abort_link("master closed the connection");
        return;
    }
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            abort_link(std::strerror(errno));
        }
        return;
    }
    link_last_io_ = now();
    link_buf_.append(buf, static_cast<size_t>(n));
    process_link_input();
}

void Replication::process_link_input() {
    while (link_state_ == LinkState::kHandshake || link_state_ == LinkState::kTransfer) {
        if (link_state_ == LinkState::kHandshake) {
            std::string line;
            if (!take_line(line)) {
                return;
            }
            if (handshake_replies_ < 3) {
                // PING, then the two REPLCONFs. An error to REPLCONF just
                // means an older master; an error to PING means we can't talk.
                if (handshake_replies_ == 0 && (line.empty() || line[0] == '-')) {
                    abort_link("master replied to PING with: " + line);
                    return;
                }
                ++handshake_replies_;
                continue;
            }
            handle_psync_reply(line);
            continue;
        }

        if (!transfer_remaining_) {
            std::string line;
            if (!take_line(line)) {
                return;
            }
            char* end = nullptr;
            unsigned long long size = line.size() > 1 && line[0] == '$' ? std::strtoull(line.c_str() + 1, &end, 10) : 0;
            if (end == nullptr || *end != '\0') {
                abort_link("expected the snapshot's $<length> header, got: " + line);
                return;
            }
            transfer_remaining_ = size;
        }
        size_t n = static_cast<size_t>(std::min<uint64_t>(*transfer_remaining_, link_buf_.size()));
        if (n > 0) {
            if (fileutil::write_all(transfer_fd_, link_buf_.data(), n) != n) {
                abort_link("writing the received snapshot: " + std::string(std::strerror(errno)));
                return;
            }
            link_buf_.erase(0, n);
            *transfer_remaining_ -= n;
        }
        if (*transfer_remaining_ > 0) {
            return;
        }
        finish_transfer();
    }
}

bool Replication::take_line(std::string& line) {
    // Bare "\n"s are keepalives a master sends while it prepares a snapshot.
    size_t start = link_buf_.find_first_not_of('\n');
    if (start == std::string::npos) {
        link_buf_.clear();
        return false;
    }
    size_t end = link_buf_.find("\r\n", start);
    if (end == std::string::npos) {
        link_buf_.erase(0, start);
        if (link_buf_.size() > kMaxHandshakeLine) {
            abort_link("reply line from master too long");
        }
        return false;
    }
    line = link_buf_.substr(start, end - start);
    link_buf_.erase(0, end + 2);
    return true;
}

void Replication::handle_psync_reply(const std::string& line) {
    if (line.rfind("+FULLRESYNC ", 0) == 0) {
        std::istringstream in(line.substr(12));
        std::string id;
        uint64_t off;
        if (!(in >> id >> off) || id.size() != kReplIdLength) {
            abort_link("malformed reply: " + line);
            return;
        }
        transfer_replid_ = id;
        transfer_offset_ = off;
        // Received into a file rather than memory: the snapshot can be as big
        // as the dataset, and the file becomes our own snapshot afterwards.
        transfer_path_ = fileutil::join(persistence_.dir(), "temp-repl-" + std::to_string(getpid()) + ".snap");
        transfer_fd_ = open(transfer_path_.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
        if (transfer_fd_ < 0) {
            abort_link(transfer_path_ + ": " + std::strerror(errno));
            return;
        }
        transfer_remaining_.reset();
        link_state_ = LinkState::kTransfer;
        std::cout << "full resync from master: replid " << id << ", offset " << off << "\n";
        return;
    }
    if (line == "+CONTINUE" || line.rfind("+CONTINUE ", 0) == 0) {
        std::string id = line.size() > 10 ? line.substr(10) : "";
        if (!id.empty() && id != replid_) {
            // Our master was promoted and renamed its history. What we hold
            // is the old history up to here — keep it continuable as replid2.
            replid2_ = replid_;
            second_replid_offset_ = offset();
            replid_ = id;
            disconnect_replicas();  // so they learn the new replid
        }
        std::cout << "partial resync with master from offset " << offset() << "\n";
        adopt_link();
        return;
    }
    abort_link("master refused PSYNC: " + line);
}

void Replication::finish_transfer() {
    bool synced = fsync(transfer_fd_) == 0;
    close(transfer_fd_);
    transfer_fd_ = -1;
    if (!synced) {
        abort_link("fsync of the received snapshot: " + std::string(std::strerror(errno)));
        return;
    }
    // Our dataset is about to be replaced wholesale; nothing our replicas
    // hold can be continued from it.
    disconnect_replicas();
    std::string error;
    bool ok = persistence_.replace_dataset(transfer_path_, error);
    transfer_path_.clear();  // renamed into place, or already deleted
    if (!ok) {
        abort_link("loading the master's snapshot: " + error);
        return;
    }
    replid_ = transfer_replid_;
    replid2_.assign(kReplIdLength, '0');
    second_replid_offset_.reset();
    backlog_.reset(transfer_offset_);
    std::cout << "loaded " << store_.size() << " keys from master\n";
    adopt_link();
}

// Hands the socket to the event loop as a regular Client flagged is_master:
// from here on the master's stream is parsed and executed like any client's
// commands, just without replies.
void Replication::adopt_link() {
    host_->unwatch(link_fd_);
    int fd = std::exchange(link_fd_, -1);
    std::string pending = std::move(link_buf_);
    link_buf_.clear();
    master_ = &host_->adopt_master(fd, pending);
    link_state_ = LinkState::kConnected;
    replid_shared_ = true;
    master_->last_interaction = now();
    send_ack();
    host_->process_buffered(*master_);
}

void Replication::abort_link(const std::string& why) {
    std::cerr << "replication link to " << master_host_ << ":" << master_port_ << ": " << why << "\n";
    close_link();
    link_state_ = is_replica() ? LinkState::kConnect : LinkState::kNone;
    next_connect_ = now() + kReconnectDelay;
}

void Replication::close_link() {
    if (master_ != nullptr) {
        host_->close_client(*master_);
        master_ = nullptr;
    }
    if (link_fd_ >= 0) {
        host_->unwatch(link_fd_);
        close(link_fd_);
        link_fd_ = -1;
    }
    if (transfer_fd_ >= 0) {
        close(transfer_fd_);
        transfer_fd_ = -1;
    }
    if (!transfer_path_.empty()) {
        unlink(transfer_path_.c_str());
        transfer_path_.clear();
    }
    link_buf_.clear();
}

void Replication::send_ack() {
    reply::command(master_->out, {"REPLCONF", "ACK", std::to_string(offset())});
    host_->queue_write(*master_);
}

// --- event-loop hooks -------------------------------------------------------

void Replication::client_closed(Client& c) {
    if (c.repl_snapshot_fd >= 0) {
        close(c.repl_snapshot_fd);
        c.repl_snapshot_fd = -1;
    }
    replicas_.erase(std::remove(replicas_.begin(), replicas_.end(), &c), replicas_.end());
    waiters_.erase(std::remove_if(waiters_.begin(), waiters_.end(), [&c](const Waiter& w) { return w.client == &c; }),
                   waiters_.end());
    to_resume_.erase(std::remove(to_resume_.begin(), to_resume_.end(), &c), to_resume_.end());
    if (&c == master_) {
        std::cerr << "lost the connection to master " << master_host_ << ":" << master_port_ << "\n";
        master_ = nullptr;
        link_state_ = LinkState::kConnect;
        next_connect_ = now();
    }
}

void Replication::before_sleep() {
    for (Client* c : std::exchange(to_resume_, {})) {
        host_->process_buffered(*c);
    }
    if (get_ack_pending_) {
        get_ack_pending_ = false;
        // Goes through the stream like any command, so a replica's ACK
        // covers every write before it.
        if (!replicas_.empty()) {
            propagate({"REPLCONF", "GETACK", "*"});
        }
    }
    unblock_waiters();
}

void Replication::cron() {
    auto t = now();
    unblock_waiters();  // WAIT timeouts

    switch (link_state_) {
        case LinkState::kConnect:
            if (t >= next_connect_) {
                connect_to_master();
            }
            break;
        case LinkState::kConnecting:
        case LinkState::kHandshake:
        case LinkState::kTransfer:
            if (t - link_last_io_ > options_.timeout) {
                abort_link("timed out");
            }
            break;
        case LinkState::kConnected:
            if (t - master_->last_interaction > options_.timeout) {
                std::cerr << "master timed out; reconnecting\n";
                host_->close_client(*master_);
            }
            break;
        case LinkState::kNone:
            break;
    }

    if (t - last_second_tick_ < kCronSecond) {
        return;
    }
    last_second_tick_ = t;

    if (master_ != nullptr && !master_->closing) {
        send_ack();
    }
    // A replica forwards its master's PINGs; only a real master makes them.
    if (!is_replica() && !replicas_.empty() && t - last_ping_ >= options_.ping_period) {
        last_ping_ = t;
        propagate({"PING"});
    }
    for (Client* r : replicas_) {
        if (r->closing) {
            continue;
        }
        if (r->repl_state == ReplState::kWaitBgsaveStart || r->repl_state == ReplState::kWaitBgsaveEnd) {
            r->out += "\n";  // keepalive, so the replica doesn't time out waiting
            host_->queue_write(*r);
        } else if (r->repl_state == ReplState::kOnline && t - r->repl_ack_time > options_.timeout) {
            std::cerr << "replica " << r->addr << ":" << r->repl_listening_port << " timed out\n";
            host_->close_client(*r);
        }
    }
    start_snapshot_for_replicas();
}

// --- INFO -------------------------------------------------------------------

std::string Replication::info() const {
    std::ostringstream s;
    auto t = now();
    s << "# Replication\r\n";
    s << "role:" << (is_replica() ? "slave" : "master") << "\r\n";
    if (is_replica()) {
        bool up = link_state_ == LinkState::kConnected;
        s << "master_host:" << master_host_ << "\r\n";
        s << "master_port:" << master_port_ << "\r\n";
        s << "master_link_status:" << (up ? "up" : "down") << "\r\n";
        s << "master_last_io_seconds_ago:"
          << (up ? std::chrono::duration_cast<std::chrono::seconds>(t - master_->last_interaction).count() : -1)
          << "\r\n";
        s << "master_sync_in_progress:" << (link_state_ == LinkState::kTransfer ? 1 : 0) << "\r\n";
        s << "slave_repl_offset:" << offset() << "\r\n";
        s << "slave_read_only:1\r\n";
    }
    s << "connected_slaves:" << replicas_.size() << "\r\n";
    for (size_t i = 0; i < replicas_.size(); ++i) {
        const Client* r = replicas_[i];
        s << "slave" << i << ":ip=" << r->addr << ",port=" << r->repl_listening_port
          << ",state=" << state_name(r->repl_state) << ",offset=" << r->repl_ack_offset
          << ",lag=" << std::chrono::duration_cast<std::chrono::seconds>(t - r->repl_ack_time).count() << "\r\n";
    }
    s << "master_replid:" << replid_ << "\r\n";
    s << "master_replid2:" << replid2_ << "\r\n";
    s << "master_repl_offset:" << offset() << "\r\n";
    s << "second_repl_offset:"
      << (second_replid_offset_ ? static_cast<int64_t>(*second_replid_offset_) : int64_t{-1}) << "\r\n";
    s << "repl_backlog_size:" << backlog_.capacity() << "\r\n";
    s << "repl_backlog_first_byte_offset:" << backlog_.start_offset() << "\r\n";
    s << "repl_backlog_histlen:" << backlog_.histlen() << "\r\n";
    s << "sync_full:" << sync_full_ << "\r\n";
    s << "sync_partial_ok:" << sync_partial_ok_ << "\r\n";
    s << "sync_partial_err:" << sync_partial_err_ << "\r\n";
    return s.str();
}
