#include "cluster.hpp"

#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <cctype>
#include <cerrno>
#include <charconv>
#include <cstring>
#include <fstream>
#include <iostream>
#include <sstream>
#include <utility>

#include "file_util.hpp"
#include "replication.hpp"
#include "reply.hpp"
#include "store.hpp"

namespace {

using cluster::kSlots;
using cluster::Message;
using cluster::MsgType;

constexpr size_t kReadChunk = 16 * 1024;
// A bus message is ~2KB of slot bitmap plus a little gossip; anything near
// this size means a broken or hostile peer.
constexpr size_t kMaxLinkInput = 1024 * 1024;
constexpr size_t kMaxLinkOutput = 64 * 1024 * 1024;
constexpr std::chrono::seconds kBlacklistTtl{60};
constexpr std::chrono::seconds kMinHandshakeTimeout{1};
// Every this many cron ticks (100ms each), PING the least recently heard
// from of a few random nodes: enough traffic for gossip to spread, without
// every node pinging every other node every second.
constexpr uint64_t kRandomPingEvery = 10;
constexpr size_t kRandomPingSample = 5;

// Failure reports older than node_timeout * this are ignored: an opinion
// only counts toward a quorum while it's fresh.
constexpr int kFailReportValidityMult = 2;
// A FAIL master that still has its slots (nobody took over) is trusted
// again once it's reachable and this many node timeouts have passed.
constexpr int kFailUndoTimeMult = 2;
// How long a manual failover may take before it's abandoned; the master
// unpauses its clients at the same moment.
constexpr std::chrono::seconds kManualFailoverTimeout{5};
// A master starting up reports the cluster down for this long, so a stale
// config (say, slots it lost while it was dead) gets corrected by gossip
// before it accepts writes for them.
constexpr std::chrono::seconds kWritableDelay{2};
// After being cut off from the majority, a master waits node_timeout
// (clamped to this range) before accepting writes again, for the same reason.
constexpr std::chrono::milliseconds kMinRejoinDelay{500};
constexpr std::chrono::milliseconds kMaxRejoinDelay{5000};
constexpr std::chrono::milliseconds kMinAuthTimeout{2000};

std::chrono::steady_clock::time_point now() {
    return std::chrono::steady_clock::now();
}

// Steady-clock instant as unix ms, for display (CLUSTER NODES shows when
// the last PING/PONG happened); 0 for "never".
int64_t unix_ms(std::chrono::steady_clock::time_point t) {
    if (t == std::chrono::steady_clock::time_point{}) {
        return 0;
    }
    using namespace std::chrono;
    auto age = duration_cast<milliseconds>(steady_clock::now() - t).count();
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count() - age;
}

std::string to_upper(const std::string& s) {
    std::string upper(s);
    for (char& c : upper) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return upper;
}

template <typename T>
bool parse_num(const std::string& s, T& out) {
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && ptr == s.data() + s.size() && !s.empty();
}

bool parse_slot(const std::string& s, int& slot) {
    return parse_num(s, slot) && slot >= 0 && slot < kSlots;
}

bool valid_ipv4(const std::string& ip) {
    in_addr addr{};
    return inet_pton(AF_INET, ip.c_str(), &addr) == 1;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> parts;
    std::string part;
    std::istringstream in(s);
    while (std::getline(in, part, sep)) {
        if (!part.empty()) {
            parts.push_back(part);
        }
    }
    return parts;
}

// "0-5460 5462 7000-7001" style ranges of the set bits.
template <typename F>
void for_each_range(const cluster::SlotBitmap& slots, F&& fn) {
    for (int start = 0; start < kSlots; ++start) {
        if (!slots[start]) {
            continue;
        }
        int end = start;
        while (end + 1 < kSlots && slots[end + 1]) {
            ++end;
        }
        fn(start, end);
        start = end;
    }
}

}  // namespace

Cluster::Cluster(Store& store, Replication& replication, Options options)
    : store_(store), replication_(replication), options_(std::move(options)), rng_(std::random_device{}()) {}

Cluster::~Cluster() {
    for (auto& l : links_) {
        if (!l->dead) {
            close(l->fd);  // the host may already be gone; don't unwatch through it
        }
    }
    if (listen_fd_ >= 0) {
        close(listen_fd_);
    }
}

const std::string& Cluster::myid() const {
    return myself_->id;
}

std::string Cluster::random_id() {
    static const char kHex[] = "0123456789abcdef";
    std::string id(cluster::kNodeIdLength, '0');
    for (char& c : id) {
        c = kHex[rng_() % 16];
    }
    return id;
}

// --- nodes and slots ----------------------------------------------------------

Cluster::Node* Cluster::lookup(const std::string& id) const {
    auto it = nodes_.find(id);
    return it != nodes_.end() ? it->second.get() : nullptr;
}

Cluster::Node* Cluster::create_node(std::string id, unsigned flags) {
    auto node = std::make_unique<Node>();
    node->id = std::move(id);
    node->flags = flags;
    node->ctime = now();
    Node* n = node.get();
    nodes_[n->id] = std::move(node);
    return n;
}

void Cluster::del_node(Node* n) {
    for (int slot = 0; slot < kSlots; ++slot) {
        if (owner_[slot] == n) {
            set_slot(slot, nullptr);
        }
        if (migrating_to_[slot] == n) {
            migrating_to_[slot] = nullptr;
        }
        if (importing_from_[slot] == n) {
            importing_from_[slot] = nullptr;
        }
    }
    if (n->link != nullptr) {
        free_link(n->link);
    }
    for (auto& [id, other] : nodes_) {
        other->fail_reports.erase(n->id);
    }
    if (mf_replica_ == n) {
        reset_manual_failover();
    }
    nodes_.erase(n->id);  // destroys n
    todo_save();
    todo_update_state_ = true;
}

void Cluster::rename_node(Node* n, const std::string& id) {
    auto handle = nodes_.extract(n->id);
    n->id = id;
    handle.key() = id;
    nodes_.insert(std::move(handle));
}

bool Cluster::start_handshake(const std::string& ip, uint16_t port, uint16_t cport) {
    if (!valid_ipv4(ip) || port == 0 || cport == 0) {
        return false;
    }
    for (const auto& [id, n] : nodes_) {
        if (n->is(kHandshake) && n->ip == ip && n->port == port && n->cport == cport) {
            return false;  // already in progress
        }
    }
    // A placeholder ID until the node's first PONG tells us its real one.
    Node* n = create_node(random_id(), kHandshake | kMeet);
    n->ip = ip;
    n->port = port;
    n->cport = cport;
    return true;
}

Cluster::Node* Cluster::master_of(const Node* n) const {
    if (n->is(kMaster)) {
        return const_cast<Node*>(n);
    }
    return lookup(n->master_id);
}

void Cluster::set_slot(int slot, Node* owner) {
    if (owner_[slot] != nullptr) {
        owner_[slot]->slots.reset(slot);
    }
    owner_[slot] = owner;
    if (owner != nullptr) {
        owner->slots.set(slot);
    }
    todo_update_state_ = true;
}

void Cluster::set_as_replica_of(Node* master) {
    // A master in the middle of a manual failover has just been replaced:
    // unpause its clients so their held writes get redirected.
    reset_manual_failover();
    for (int slot = 0; slot < kSlots; ++slot) {
        if (owner_[slot] == myself_) {
            set_slot(slot, nullptr);
        }
        migrating_to_[slot] = nullptr;
        importing_from_[slot] = nullptr;
    }
    myself_->flags = (myself_->flags & ~kMaster) | kReplica;
    myself_->master_id = master->id;
    follow_master();
    todo_save();
    todo_broadcast_ = true;
}

void Cluster::set_node_as_master(Node* n) {
    n->flags = (n->flags & ~kReplica) | kMaster;
    n->master_id.clear();
    todo_save();
}

uint64_t Cluster::max_epoch() const {
    uint64_t max = current_epoch_;
    for (const auto& [id, n] : nodes_) {
        max = std::max(max, n->config_epoch);
    }
    return max;
}

// Takes a new, unique config epoch without asking anyone (Redis's
// clusterBumpConfigEpochWithoutConsensus). Only safe where the admin is
// coordinating the change, as at the end of a slot migration: there is no
// vote, so two nodes bumping at once could pick the same epoch, which the
// collision rule then resolves.
bool Cluster::bump_epoch_without_consensus() {
    uint64_t max = max_epoch();
    if (myself_->config_epoch != 0 && myself_->config_epoch == max) {
        return false;  // already the highest: our claims win as they are
    }
    current_epoch_ = max + 1;
    myself_->config_epoch = current_epoch_;
    todo_save();
    todo_broadcast_ = true;
    std::cout << "cluster: config epoch bumped to " << current_epoch_ << "\n";
    return true;
}

// `sender` (a master) advertises `slots` under `config_epoch`: take every
// claim that beats what we believe. The epoch is the whole conflict rule:
// the claim made under the higher epoch is the more recent decision.
void Cluster::update_slots_config_with(Node* sender, uint64_t config_epoch, const SlotBitmap& slots) {
    if (sender == myself_) {
        return;  // nobody else decides what we own
    }
    // Our shard's master: ourselves, or the master we replicate.
    Node* our_master = master_of(myself_);
    bool took_from_our_master = false;
    std::vector<int> dirty;
    for (int slot = 0; slot < kSlots; ++slot) {
        if (!slots[slot] || owner_[slot] == sender) {
            continue;
        }
        // Mid-migration into us: the admin's SETSLOT decides, not gossip.
        if (importing_from_[slot] != nullptr) {
            continue;
        }
        if (owner_[slot] == nullptr || owner_[slot]->config_epoch < config_epoch) {
            if (owner_[slot] == myself_) {
                if (store_.count_keys_in_slot(slot) > 0) {
                    dirty.push_back(slot);
                }
                migrating_to_[slot] = nullptr;
                todo_broadcast_ = true;
            }
            if (owner_[slot] != nullptr && owner_[slot] == our_master) {
                took_from_our_master = true;
            }
            set_slot(slot, sender);
            todo_save();
        }
    }

    // Our shard lost its last slot to `sender`: `sender` replaced our master
    // (a failover), or took us over (we were the failed master, back
    // online). Either way we now follow it. A demoted master keeps its keys
    // for now: the resync from the new master replaces them all.
    if (took_from_our_master && our_master != nullptr && our_master->slots.none()) {
        if (myself_->is(kMaster)) {
            std::cout << "cluster: lost my last slot to " << sender->id << "; becoming its replica\n";
            set_as_replica_of(sender);
            return;
        }
        if (myself_->master_id != sender->id) {
            std::cout << "cluster: my master " << our_master->id << " was replaced by " << sender->id
                      << "; following it\n";
            set_as_replica_of(sender);
        }
        return;
    }

    // Keys in slots we just lost can't be reached through us any more, and
    // the new owner is the authority for them. A replica's master sends it
    // the DELs, so only a master deletes.
    if (myself_->is(kMaster)) {
        for (int slot : dirty) {
            size_t deleted = store_.delete_slot(slot);
            std::cout << "cluster: lost slot " << slot << " to " << sender->id << "; deleted " << deleted
                      << " keys\n";
        }
    }
}

// Two masters with the same config epoch can't settle a slot conflict, so
// the one with the lexicographically smaller ID moves to a fresh epoch.
// Every node applies the same rule, so exactly one of the pair bumps.
void Cluster::handle_epoch_collision(Node* sender) {
    if (!sender->is(kMaster) || !myself_->is(kMaster) || sender->config_epoch != myself_->config_epoch) {
        return;
    }
    if (sender->id <= myself_->id) {
        return;  // the other node bumps
    }
    ++current_epoch_;
    myself_->config_epoch = current_epoch_;
    todo_save();
    todo_broadcast_ = true;
    std::cout << "cluster: config epoch collision with " << sender->id << "; moved to epoch " << current_epoch_
              << "\n";
}

int Cluster::cluster_size() const {
    int size = 0;
    for (const auto& [id, n] : nodes_) {
        if (n->is(kMaster) && n->slots.any()) {
            ++size;
        }
    }
    return size;
}

void Cluster::update_state() {
    todo_update_state_ = false;
    auto t = now();
    // A slot whose owner is FAIL isn't served, even though it's assigned.
    bool ok = !options_.require_full_coverage || std::all_of(owner_.begin(), owner_.end(), [](const Node* n) {
                  return n != nullptr && !n->is(kFail);
              });

    // On the minority side of a partition, stop serving: the majority may
    // be promoting our replicas right now, and any write we accepted would
    // be lost when we rejoin as a replica. This caps the window of lost
    // writes to about one node timeout.
    int size = 0;
    int reachable = 0;
    for (const auto& [id, n] : nodes_) {
        if (n->is(kMaster) && n->slots.any()) {
            ++size;
            if (!n->is(kPFail | kFail)) {
                ++reachable;
            }
        }
    }
    if (size > 0 && reachable < size / 2 + 1) {
        ok = false;
        among_minority_time_ = t;
    }

    if (ok == state_ok_) {
        return;
    }
    // A master turning "ok" waits a little first (only with the bus up, so
    // unit tests see the state right away): after startup, or after being
    // in the minority, its slot config may be stale, and gossip needs a
    // moment to tell it about a failover that happened meanwhile.
    if (ok && myself_->is(kMaster) && host_ != nullptr) {
        auto rejoin_delay = std::clamp<std::chrono::milliseconds>(options_.node_timeout, kMinRejoinDelay, kMaxRejoinDelay);
        if (t < writable_after_ || t - among_minority_time_ < rejoin_delay) {
            return;
        }
    }
    std::cout << "cluster state changed: " << (ok ? "ok" : "fail") << "\n";
    state_ok_ = ok;
}

// --- routing ------------------------------------------------------------------

bool Cluster::route(const std::vector<const std::string*>& keys, bool write, bool asking, const Client& c,
                    std::string& error) {
    if (keys.empty()) {
        return true;
    }
    int slot = cluster::key_hash_slot(*keys[0]);
    for (size_t i = 1; i < keys.size(); ++i) {
        if (cluster::key_hash_slot(*keys[i]) != slot) {
            error = "CROSSSLOT Keys in request don't hash to the same slot";
            return false;
        }
    }
    if (!state_ok_) {
        error = "CLUSTERDOWN The cluster is down";
        return false;
    }
    Node* owner = owner_[slot];
    if (owner == nullptr) {
        error = "CLUSTERDOWN Hash slot not served";
        return false;
    }

    bool migrating = owner == myself_ && migrating_to_[slot] != nullptr;
    bool importing = importing_from_[slot] != nullptr;
    size_t missing = 0;
    if (migrating || importing) {
        for (const std::string* key : keys) {
            if (!store_.contains(*key)) {
                ++missing;
            }
        }
    }

    // Keys still here are served here; keys already moved (or never
    // existed) are the target's business now, including brand-new keys.
    if (migrating && missing > 0) {
        if (missing < keys.size()) {
            // Some keys here, some there: no single node can run it.
            error = "TRYAGAIN Multiple keys request during rehashing of slot";
        } else {
            error = "ASK " + std::to_string(slot) + " " + address(migrating_to_[slot]);
        }
        return false;
    }
    if (importing && asking) {
        if (keys.size() > 1 && missing > 0) {
            error = "TRYAGAIN Multiple keys request during rehashing of slot";
            return false;
        }
        return true;
    }

    if (owner != myself_) {
        // A replica serves reads for its own master's slots, but only to
        // clients that said (READONLY) they accept slightly stale data.
        if (!write && c.readonly && myself_->is(kReplica) && myself_->master_id == owner->id) {
            return true;
        }
        error = "MOVED " + std::to_string(slot) + " " + address(owner);
        return false;
    }
    return true;
}

std::string Cluster::address(const Node* n) const {
    return n->ip + ":" + std::to_string(n->port);
}

// --- bus: connections -----------------------------------------------------------

bool Cluster::start(ClientHost* host) {
    host_ = host;
    listen_fd_ = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (listen_fd_ < 0) {
        std::perror("cluster bus socket");
        return false;
    }
    int opt = 1;
    setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(options_.cport);
    if (inet_pton(AF_INET, options_.bind_addr.c_str(), &addr.sin_addr) != 1) {
        std::cerr << "invalid bind address: " << options_.bind_addr << "\n";
        return false;
    }
    if (bind(listen_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        std::perror("cluster bus bind");
        return false;
    }
    if (listen(listen_fd_, 511) < 0 || !host_->watch(listen_fd_, false, [this] { accept_links(); })) {
        std::perror("cluster bus listen");
        return false;
    }
    std::cout << "cluster bus listening on " << options_.bind_addr << ":" << options_.cport << ", node id "
              << myself_->id << "\n";
    writable_after_ = now() + kWritableDelay;
    state_ok_ = false;
    follow_master();
    update_state();
    return true;
}

void Cluster::accept_links() {
    while (true) {
        sockaddr_in peer{};
        socklen_t len = sizeof(peer);
        int fd = accept4(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &len, SOCK_NONBLOCK | SOCK_CLOEXEC);
        if (fd < 0) {
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                std::perror("cluster bus accept4");
            }
            return;
        }
        int one = 1;
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
        auto link = std::make_unique<Link>();
        Link* l = link.get();
        l->fd = fd;
        l->ctime = now();
        char ip[INET_ADDRSTRLEN] = "";
        inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
        l->peer_ip = ip;
        if (!host_->watch(fd, false, [this, l] { on_link_event(l); })) {
            close(fd);
            continue;
        }
        links_.push_back(std::move(link));
    }
}

void Cluster::connect_link(Node* n) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(n->cport);
    if (inet_pton(AF_INET, n->ip.c_str(), &addr.sin_addr) != 1) {
        return;
    }
    // Connecting means a PING is on its way. Count the wait from now, so a
    // node we can't even connect to still times out and gets flagged PFAIL
    // (an older outstanding PING keeps its earlier time).
    if (n->ping_sent == SteadyTime{} && !n->is(kHandshake)) {
        n->ping_sent = now();
    }
    // Non-blocking, like the replica's link to its master: a dead node must
    // not stall the event loop for a TCP connect timeout.
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return;
    }
    if (connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0 && errno != EINPROGRESS) {
        close(fd);
        return;
    }
    auto link = std::make_unique<Link>();
    Link* l = link.get();
    l->fd = fd;
    l->node = n;
    l->connecting = true;
    l->write_interest = true;
    l->ctime = now();
    if (!host_->watch(fd, true, [this, l] { on_link_event(l); })) {
        close(fd);
        return;
    }
    n->link = l;
    links_.push_back(std::move(link));
}

void Cluster::on_link_event(Link* l) {
    if (l->dead) {
        return;
    }
    if (l->connecting) {
        finish_connect(l);
        return;
    }
    // The host reports readiness without saying which kind, so try both:
    // a write attempt on a full socket or a read on an empty one just
    // returns EAGAIN.
    if (!l->out.empty()) {
        flush_link(l);
    }
    if (!l->dead) {
        read_link(l);
    }
}

void Cluster::finish_connect(Link* l) {
    int err = 0;
    socklen_t len = sizeof(err);
    if (getsockopt(l->fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0 || err != 0) {
        free_link(l);  // cron retries on its next tick
        return;
    }
    int one = 1;
    setsockopt(l->fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    l->connecting = false;
    Node* n = l->node;
    send_ping(l, n->is(kMeet) ? MsgType::kMeet : MsgType::kPing);
    n->flags &= ~kMeet;
    if (!l->dead && l->out.empty()) {
        flush_link(l);  // drops write interest
    }
}

void Cluster::read_link(Link* l) {
    char buf[kReadChunk];
    ssize_t n = read(l->fd, buf, sizeof(buf));
    if (n == 0) {
        free_link(l);
        return;
    }
    if (n < 0) {
        if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR) {
            free_link(l);
        }
        return;
    }
    l->parser.feed(buf, static_cast<size_t>(n));
    while (!l->dead) {
        RespParser::ParseResult r = l->parser.try_parse_command();
        if (r.status == RespParser::Status::kIncomplete) {
            break;
        }
        Message msg;
        if (r.status == RespParser::Status::kProtocolError || !cluster::decode(r.command, msg)) {
            std::cerr << "cluster bus: malformed message from " << (l->node ? l->node->id : l->peer_ip)
                      << "; dropping the link\n";
            free_link(l);
            return;
        }
        ++messages_received_;
        process_packet(l, msg);
    }
    if (!l->dead && l->parser.buffered_bytes() > kMaxLinkInput) {
        free_link(l);
    }
}

void Cluster::flush_link(Link* l) {
    size_t sent = 0;
    while (sent < l->out.size()) {
        ssize_t n = ::send(l->fd, l->out.data() + sent, l->out.size() - sent, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                break;
            }
            free_link(l);
            return;
        }
        sent += static_cast<size_t>(n);
    }
    l->out.erase(0, sent);
    bool want_write = !l->out.empty();
    if (want_write != l->write_interest) {
        l->write_interest = want_write;
        host_->watch(l->fd, want_write, [this, l] { on_link_event(l); });
    }
}

// Closes now, frees later: the link may be the one whose event is being
// handled further up the stack. before_sleep() reaps it.
void Cluster::free_link(Link* l) {
    if (l->dead) {
        return;
    }
    l->dead = true;
    host_->unwatch(l->fd);
    close(l->fd);
    l->fd = -1;
    if (l->node != nullptr && l->node->link == l) {
        l->node->link = nullptr;
    }
    l->node = nullptr;
}

void Cluster::reap_links() {
    links_.erase(std::remove_if(links_.begin(), links_.end(), [](const auto& l) { return l->dead; }), links_.end());
}

// --- bus: messages ----------------------------------------------------------------

void Cluster::send(Link* l, const Message& msg) {
    if (l->dead) {
        return;
    }
    cluster::encode(msg, l->out);
    ++messages_sent_;
    if (l->out.size() > kMaxLinkOutput) {
        free_link(l);
        return;
    }
    if (!l->connecting) {
        flush_link(l);
    }
}

Message Cluster::header(MsgType type) const {
    Message m;
    m.type = type;
    m.sender = myself_->id;
    m.port = options_.port;
    m.cport = options_.cport;
    m.replica = myself_->is(kReplica);
    m.master_id = myself_->master_id;
    m.current_epoch = current_epoch_;
    m.repl_offset = replication_.offset();
    if (myself_->is(kMaster) && mf_end_) {
        m.mflags |= cluster::kMsgPaused;
    }
    if (const Node* master = master_of(myself_)) {
        m.config_epoch = master->config_epoch;
        m.slots = master->slots;
    }
    return m;
}

void Cluster::broadcast(const Message& msg) {
    for (const auto& [id, n] : nodes_) {
        if (n.get() != myself_ && n->link != nullptr && !n->link->connecting && !n->is(kHandshake)) {
            send(n->link, msg);
        }
    }
}

void Cluster::send_ping(Link* l, MsgType type) {
    Message m = header(type);
    // Gossip about a few random nodes: with every node doing this on every
    // PING, news of a node reaches the whole cluster in O(log N) rounds
    // without anyone sending a full membership list.
    std::vector<const Node*> candidates;
    for (const auto& [id, n] : nodes_) {
        if (n.get() == myself_ || n.get() == l->node || n->is(kHandshake | kNoAddr)) {
            continue;
        }
        if (n->link == nullptr && n->slots.none() && !n->is(kPFail)) {
            continue;  // unreachable and serving nothing: not worth spreading
        }
        candidates.push_back(n.get());
    }
    std::shuffle(candidates.begin(), candidates.end(), rng_);
    size_t wanted = std::max<size_t>(3, nodes_.size() / 10);
    // Nodes we suspect always go along, on top of the random ones: failure
    // reports have to reach a majority of masters within the report
    // validity window, and random picks alone could keep missing one.
    std::stable_partition(candidates.begin(), candidates.end(), [](const Node* n) { return n->is(kPFail); });
    size_t suspects = std::count_if(candidates.begin(), candidates.end(), [](const Node* n) { return n->is(kPFail); });
    candidates.resize(std::min(std::max(wanted, suspects), candidates.size()));
    for (const Node* n : candidates) {
        m.gossip.push_back({n->id, n->ip, n->port, n->cport, n->is(kReplica), n->is(kPFail), n->is(kFail)});
    }
    if (type == MsgType::kPing && l->node != nullptr && l->node->ping_sent == SteadyTime{}) {
        l->node->ping_sent = now();
    }
    send(l, m);
}

void Cluster::broadcast_pong() {
    for (const auto& [id, n] : nodes_) {
        if (n->link != nullptr && !n->link->connecting && !n->is(kHandshake)) {
            send_ping(n->link, MsgType::kPong);
        }
    }
}

void Cluster::send_update(Link* l, const Node* n) {
    Message m = header(MsgType::kUpdate);
    m.update_id = n->id;
    m.update_epoch = n->config_epoch;
    m.update_slots = n->slots;
    send(l, m);
}

void Cluster::process_packet(Link* l, const Message& msg) {
    Node* sender = lookup(msg.sender);
    if (sender != nullptr && sender->is(kHandshake)) {
        sender = nullptr;
    }
    if (sender != nullptr) {
        if (msg.current_epoch > current_epoch_) {
            current_epoch_ = msg.current_epoch;
            todo_save();
        }
        if (msg.config_epoch > sender->config_epoch) {
            sender->config_epoch = msg.config_epoch;
            todo_save();
        }
        if (sender->port != msg.port || sender->cport != msg.cport) {
            sender->port = msg.port;
            if (sender->cport != msg.cport && sender->link != nullptr) {
                free_link(sender->link);  // reconnect to the new bus port
            }
            sender->cport = msg.cport;
            todo_save();
        }
        sender->repl_offset = msg.repl_offset;
        // Our master, mid manual failover, has paused its writes: the
        // offset it reports now is final. We may take over once we've
        // applied exactly that much of its stream.
        if (myself_->is(kReplica) && myself_->master_id == sender->id && (msg.mflags & cluster::kMsgPaused) &&
            mf_end_ && !mf_master_offset_) {
            mf_master_offset_ = msg.repl_offset;
            todo_handle_manual_failover_ = true;
            std::cout << "cluster: received the master's final replication offset " << msg.repl_offset
                      << " for the manual failover\n";
        }
    }

    if (msg.type == MsgType::kPing || msg.type == MsgType::kMeet) {
        // We don't know our own IP until someone connects to it: the local
        // address of a link they opened is how they reach us.
        if (myself_->ip.empty()) {
            sockaddr_in local{};
            socklen_t len = sizeof(local);
            char ip[INET_ADDRSTRLEN] = "";
            if (getsockname(l->fd, reinterpret_cast<sockaddr*>(&local), &len) == 0 &&
                inet_ntop(AF_INET, &local.sin_addr, ip, sizeof(ip)) != nullptr) {
                myself_->ip = ip;
                todo_save();
            }
        }
        // Only MEET adds an unknown node; a PING from a stranger is answered
        // but doesn't make it a member (it could be a forgotten node).
        if (sender == nullptr && msg.type == MsgType::kMeet && !l->peer_ip.empty()) {
            start_handshake(l->peer_ip, msg.port, msg.cport);
        }
        send_ping(l, MsgType::kPong);
        if (l->dead) {
            return;
        }
    }

    if (msg.type == MsgType::kUpdate) {
        if (sender == nullptr) {
            return;
        }
        Node* n = lookup(msg.update_id);
        if (n == nullptr || n->is(kHandshake) || n->config_epoch >= msg.update_epoch) {
            return;
        }
        if (n->is(kReplica)) {
            set_node_as_master(n);
        }
        n->config_epoch = msg.update_epoch;
        todo_save();
        update_slots_config_with(n, msg.update_epoch, msg.update_slots);
        return;
    }

    if (msg.type == MsgType::kFail) {
        Node* failing = sender != nullptr ? lookup(msg.fail_id) : nullptr;
        if (failing != nullptr && failing != myself_ && !failing->is(kFail) && !failing->is(kHandshake)) {
            std::cout << "cluster: FAIL message received from " << sender->id << " about " << failing->id << "\n";
            failing->flags = (failing->flags & ~kPFail) | kFail;
            failing->fail_time = now();
            todo_update_state_ = true;
            todo_handle_failover_ = true;
            todo_save();
        }
        return;
    }

    if (msg.type == MsgType::kFailoverAuthRequest) {
        if (sender != nullptr) {
            send_failover_auth_if_needed(sender, msg);
        }
        return;
    }

    if (msg.type == MsgType::kFailoverAuthAck) {
        // Only votes from masters serving slots count, and only for the
        // election we're running now (a late vote from an older one says
        // nothing about this one).
        if (sender != nullptr && sender->is(kMaster) && sender->slots.any() &&
            msg.current_epoch >= failover_auth_epoch_ && failover_auth_sent_) {
            ++failover_auth_count_;
            std::cout << "cluster: got a failover vote from " << sender->id << " (" << failover_auth_count_ << "/"
                      << needed_quorum() << ")\n";
            todo_handle_failover_ = true;
        }
        return;
    }

    if (msg.type == MsgType::kMfStart) {
        // Our replica asks for a manual failover: pause writes so our
        // offset stops moving, and tell it that offset (every PING we send
        // it until the failover ends carries it, flagged PAUSED).
        if (sender == nullptr || !sender->is(kReplica) || sender->master_id != myself_->id) {
            return;
        }
        reset_manual_failover();
        mf_end_ = now() + kManualFailoverTimeout;
        mf_replica_ = sender;
        pause_writes(true);
        std::cout << "cluster: manual failover requested by replica " << sender->id << "\n";
        Link* to = sender->link != nullptr && !sender->link->connecting ? sender->link : l;
        send_ping(to, MsgType::kPing);
        return;
    }

    // PING / PONG / MEET.
    if (Node* n = l->node) {
        if (n->is(kHandshake)) {
            if (sender != nullptr) {
                // We already know this node under its real ID (e.g. two
                // gossipers told us about it): drop the duplicate.
                del_node(n);
                return;
            }
            std::cout << "cluster: handshake with " << address(n) << " done: node " << msg.sender << "\n";
            rename_node(n, msg.sender);
            n->flags &= ~(kHandshake | kMeet);
            n->flags |= msg.replica ? kReplica : kMaster;
            n->master_id = msg.master_id;
            todo_save();
            sender = n;
        } else if (n->id != msg.sender) {
            // A different node answers at this address (it was reset and
            // took a new ID). Stop connecting; gossip will introduce the
            // new one.
            std::cout << "cluster: node " << n->id << " at " << address(n) << " now answers as " << msg.sender
                      << "\n";
            n->flags |= kNoAddr;
            n->ip.clear();
            free_link(l);
            todo_save();
            return;
        }
        if (msg.type == MsgType::kPong) {
            n->pong_received = now();
            n->ping_sent = SteadyTime{};
            if (n->is(kPFail)) {
                std::cout << "cluster: node " << n->id << " is reachable again\n";
                n->flags &= ~kPFail;
                todo_update_state_ = true;
            } else if (n->is(kFail)) {
                clear_node_failure_if_needed(n);
            }
        }
    }
    if (sender == nullptr) {
        return;
    }

    // Role changes.
    if (msg.replica) {
        if (sender->is(kMaster)) {
            // A master turned replica gives up its slots.
            std::cout << "cluster: node " << sender->id << " is now a replica\n";
            for (int slot = 0; slot < kSlots; ++slot) {
                if (owner_[slot] == sender) {
                    set_slot(slot, nullptr);
                }
            }
            sender->flags = (sender->flags & ~kMaster) | kReplica;
            todo_save();
        }
        if (sender->master_id != msg.master_id) {
            sender->master_id = msg.master_id;
            todo_save();
        }
    } else if (sender->is(kReplica)) {
        set_node_as_master(sender);
    }

    if (sender->is(kMaster)) {
        if (sender->slots != msg.slots) {
            update_slots_config_with(sender, msg.config_epoch, msg.slots);
        }
        // The sender claims a slot we know a newer owner for: tell it, so
        // a stale node (say, one back from a partition) learns quickly.
        for (int slot = 0; slot < kSlots; ++slot) {
            Node* owner = owner_[slot];
            if (msg.slots[slot] && owner != nullptr && owner != sender && owner->config_epoch > msg.config_epoch) {
                send_update(l, owner);
                break;
            }
        }
        handle_epoch_collision(sender);
    }
    process_gossip(sender, msg);
}

void Cluster::process_gossip(Node* sender, const Message& msg) {
    auto t = now();
    for (const cluster::GossipEntry& g : msg.gossip) {
        if (g.id == myself_->id) {
            continue;
        }
        if (Node* n = lookup(g.id)) {
            // A master's view of another node's health is a failure report.
            // Replicas' opinions don't count: the quorum is of masters, as
            // it's the masters that will vote on a failover.
            if (sender->is(kMaster) && !n->is(kHandshake)) {
                if (g.pfail || g.fail) {
                    if (n->fail_reports.count(sender->id) == 0) {
                        std::cout << "cluster: node " << sender->id << " reported node " << n->id
                                  << " as not reachable\n";
                    }
                    n->fail_reports[sender->id] = t;
                    mark_node_as_failing_if_needed(n);
                } else {
                    n->fail_reports.erase(sender->id);
                }
            }
            continue;
        }
        auto banned = blacklist_.find(g.id);
        if (banned != blacklist_.end() && banned->second > t) {
            continue;
        }
        // A member vouches for this node: meet it ourselves. This is how
        // one CLUSTER MEET joins a node to the whole cluster.
        start_handshake(g.ip, g.port, g.cport);
    }
}

// A replica's replication link follows its master's current address.
void Cluster::follow_master() {
    if (!myself_->is(kReplica)) {
        return;
    }
    Node* master = lookup(myself_->master_id);
    if (master == nullptr || master->ip.empty()) {
        return;
    }
    if (!replication_.is_replica() || replication_.master_host() != master->ip ||
        replication_.master_port() != master->port) {
        replication_.replicaof(master->ip, master->port);
    }
}

// --- event-loop hooks ---------------------------------------------------------------

void Cluster::cron() {
    if (host_ == nullptr) {
        return;
    }
    auto t = now();
    ++cron_iterations_;
    auto handshake_timeout = std::max<std::chrono::milliseconds>(options_.node_timeout, kMinHandshakeTimeout);

    std::vector<Node*> expired;
    for (const auto& [id, node] : nodes_) {
        Node* n = node.get();
        if (n == myself_ || n->is(kNoAddr)) {
            continue;
        }
        if (n->is(kHandshake) && t - n->ctime > handshake_timeout) {
            expired.push_back(n);
            continue;
        }
        if (n->link == nullptr) {
            connect_link(n);
        } else if (n->link->connecting && t - n->link->ctime > options_.node_timeout) {
            free_link(n->link);
        }
    }
    for (Node* n : expired) {
        std::cout << "cluster: handshake with " << address(n) << " timed out\n";
        del_node(n);
    }
    for (auto it = blacklist_.begin(); it != blacklist_.end();) {
        it = it->second <= t ? blacklist_.erase(it) : std::next(it);
    }

    auto usable = [](const Node* n) { return n->link != nullptr && !n->link->connecting && !n->is(kHandshake); };

    if (cron_iterations_ % kRandomPingEvery == 0) {
        std::vector<Node*> pool;
        for (const auto& [id, node] : nodes_) {
            if (node.get() != myself_ && usable(node.get()) && node->ping_sent == SteadyTime{}) {
                pool.push_back(node.get());
            }
        }
        std::shuffle(pool.begin(), pool.end(), rng_);
        pool.resize(std::min(pool.size(), kRandomPingSample));
        auto oldest = std::min_element(pool.begin(), pool.end(),
                                       [](const Node* a, const Node* b) { return a->pong_received < b->pong_received; });
        if (oldest != pool.end()) {
            send_ping((*oldest)->link, MsgType::kPing);
        }
    }

    for (const auto& [id, node] : nodes_) {
        Node* n = node.get();
        if (n == myself_ || !usable(n)) {
            continue;
        }
        if (n->ping_sent != SteadyTime{}) {
            // No PONG for half the timeout on a link that isn't new: the
            // connection may be wedged; a fresh one tells us more.
            if (t - n->ping_sent > options_.node_timeout / 2 && t - n->link->ctime > options_.node_timeout) {
                free_link(n->link);
            }
        } else if (t - n->pong_received > options_.node_timeout / 2) {
            // Never let a node go unpinged long enough to look dead.
            send_ping(n->link, MsgType::kPing);
        }
    }

    detect_failures();

    // Mid manual failover, the master keeps telling its replica the final
    // offset (in case the first PING was lost with a reconnecting link).
    if (myself_->is(kMaster) && mf_end_ && mf_replica_ != nullptr && usable(mf_replica_)) {
        send_ping(mf_replica_->link, MsgType::kPing);
    }
    manual_failover_check_timeout();
    if (myself_->is(kReplica)) {
        handle_manual_failover();
        handle_replica_failover();
    }

    follow_master();
    update_state();
}

void Cluster::detect_failures() {
    auto t = now();
    for (const auto& [id, node] : nodes_) {
        Node* n = node.get();
        if (n == myself_ || n->is(kHandshake | kNoAddr | kPFail | kFail)) {
            continue;
        }
        if (n->ping_sent != SteadyTime{} && t - n->ping_sent > options_.node_timeout) {
            std::cout << "cluster: *** node " << n->id << " possibly failing (no PONG for "
                      << std::chrono::duration_cast<std::chrono::milliseconds>(t - n->ping_sent).count() << "ms)\n";
            n->flags |= kPFail;
            todo_update_state_ = true;
            mark_node_as_failing_if_needed(n);
        }
    }
}

size_t Cluster::count_fail_reports(Node* n) {
    auto t = now();
    auto validity = options_.node_timeout * kFailReportValidityMult;
    for (auto it = n->fail_reports.begin(); it != n->fail_reports.end();) {
        it = t - it->second > validity ? n->fail_reports.erase(it) : std::next(it);
    }
    return n->fail_reports.size();
}

// PFAIL -> FAIL once enough masters agree (Redis's markNodeAsFailingIfNeeded).
// Our own opinion counts too if we're a master. The FAIL broadcast then
// overrides everyone's slower local detection, so the whole cluster flips
// at once and the replicas can start their election.
void Cluster::mark_node_as_failing_if_needed(Node* n) {
    if (!n->is(kPFail) || n->is(kFail)) {
        return;
    }
    size_t failures = count_fail_reports(n) + (myself_->is(kMaster) ? 1 : 0);
    if (failures < static_cast<size_t>(needed_quorum())) {
        return;
    }
    std::cout << "cluster: marking node " << n->id << " as failing (quorum reached: " << failures << "/"
              << needed_quorum() << ")\n";
    n->flags = (n->flags & ~kPFail) | kFail;
    n->fail_time = now();
    Message m = header(MsgType::kFail);
    m.fail_id = n->id;
    broadcast(m);
    todo_update_state_ = true;
    todo_handle_failover_ = true;
    todo_save();
}

// A FAIL node that answers again (Redis's clearNodeFailureIfNeeded). A
// replica, or a master without slots, is simply back. A master that still
// owns slots means no replica took over; it's trusted again only after a
// grace period, so a flapping master can't cancel an election in progress.
void Cluster::clear_node_failure_if_needed(Node* n) {
    if (!n->is(kFail)) {
        return;
    }
    bool clear = false;
    if (n->is(kReplica) || n->slots.none()) {
        std::cout << "cluster: clear FAIL state for node " << n->id << ": "
                  << (n->is(kReplica) ? "replica" : "master without slots") << " is reachable again\n";
        clear = true;
    } else if (now() - n->fail_time > options_.node_timeout * kFailUndoTimeMult) {
        std::cout << "cluster: clear FAIL state for node " << n->id
                  << ": is reachable again and nobody is serving its slots after some time\n";
        clear = true;
    }
    if (clear) {
        n->flags &= ~kFail;
        todo_update_state_ = true;
        todo_save();
    }
}

// --- failover, replica side -----------------------------------------------------------

void Cluster::cant_failover(const std::string& reason) {
    if (reason != cant_failover_reason_ && !reason.empty()) {
        std::cout << "cluster: currently unable to failover: " << reason << "\n";
    }
    cant_failover_reason_ = reason;
}

// How many sibling replicas (same master, not failed) have applied more of
// the master's stream than we have. Rank 0 is the most up to date; each
// rank adds a second to the election delay, so the best replica usually
// asks first and wins, losing the fewest writes.
int Cluster::replica_rank() const {
    const Node* master = lookup(myself_->master_id);
    if (master == nullptr) {
        return 0;
    }
    uint64_t mine = replication_.offset();
    int rank = 0;
    for (const auto& [id, n] : nodes_) {
        if (n.get() != myself_ && n->is(kReplica) && n->master_id == master->id && !n->is(kFail) &&
            n->repl_offset > mine) {
            ++rank;
        }
    }
    return rank;
}

// Redis's clusterHandleSlaveFailover(): run from the cron and whenever
// something relevant changes (a FAIL, a vote). Schedules an election,
// sends the vote requests when it's due, and promotes us on a majority.
void Cluster::handle_replica_failover() {
    todo_handle_failover_ = false;
    if (host_ == nullptr || !myself_->is(kReplica)) {
        return;
    }
    auto t = now();
    Node* master = lookup(myself_->master_id);
    bool manual = mf_end_ && mf_can_start_;
    if (master == nullptr || master->slots.none() || (!master->is(kFail) && !manual) ||
        (options_.replica_no_failover && !manual)) {
        cant_failover("");
        return;
    }

    auto auth_timeout = std::max<std::chrono::milliseconds>(options_.node_timeout * 2, kMinAuthTimeout);
    auto auth_retry_time = auth_timeout * 2;

    // How stale our copy is. The master was unreachable for about a node
    // timeout before it was flagged FAIL anyway, so that much doesn't count.
    auto data_age = t - replication_.master_last_contact();
    if (data_age > options_.node_timeout) {
        data_age -= options_.node_timeout;
    }
    if (options_.replica_validity_factor > 0 && !manual &&
        data_age > replication_.ping_period() + options_.node_timeout * options_.replica_validity_factor) {
        cant_failover("my data is too old: last contact with the master " +
                      std::to_string(std::chrono::duration_cast<std::chrono::seconds>(data_age).count()) +
                      "s ago (see --cluster-replica-validity-factor)");
        return;
    }

    // No election yet, or the last one is long over: schedule one. The fixed
    // 500ms lets the FAIL message reach every master first; the random part
    // makes it unlikely that two replicas ask in the same instant and split
    // the vote.
    if (!failover_auth_time_ || t - *failover_auth_time_ > auth_retry_time) {
        failover_auth_rank_ = replica_rank();
        failover_auth_time_ = t + std::chrono::milliseconds(500 + rng_() % 500) + std::chrono::seconds(failover_auth_rank_);
        failover_auth_count_ = 0;
        failover_auth_sent_ = false;
        if (mf_end_) {
            // Manual: the replica is known to be in sync; no need to wait.
            failover_auth_time_ = t;
            failover_auth_rank_ = 0;
            todo_handle_failover_ = true;
        }
        std::cout << "cluster: start of election delayed for "
                  << std::chrono::duration_cast<std::chrono::milliseconds>(*failover_auth_time_ - t).count()
                  << "ms (rank #" << failover_auth_rank_ << ", offset " << replication_.offset() << ")\n";
        // Let our siblings know our offset, so their ranks are right.
        broadcast_pong();
        return;
    }

    // A sibling's offset overtook ours while we were waiting: step back.
    if (!failover_auth_sent_ && !mf_end_) {
        int rank = replica_rank();
        if (rank > failover_auth_rank_) {
            *failover_auth_time_ += std::chrono::seconds(rank - failover_auth_rank_);
            std::cout << "cluster: replica rank updated to #" << rank << ", election delayed further\n";
            failover_auth_rank_ = rank;
        }
    }

    if (t < *failover_auth_time_) {
        return;
    }
    if (t - *failover_auth_time_ > auth_timeout) {
        cant_failover("election timed out, waiting for the retry time");
        return;
    }

    if (!failover_auth_sent_) {
        // A new epoch for this election: every master votes at most once
        // per epoch, so at most one replica can collect a majority in it.
        ++current_epoch_;
        failover_auth_epoch_ = current_epoch_;
        std::cout << "cluster: starting a failover election for epoch " << current_epoch_ << "\n";
        request_failover_auth();
        failover_auth_sent_ = true;
        todo_save();
        todo_update_state_ = true;
        return;
    }

    if (failover_auth_count_ >= needed_quorum()) {
        std::cout << "cluster: failover election won\n";
        if (myself_->config_epoch < failover_auth_epoch_) {
            myself_->config_epoch = failover_auth_epoch_;
            std::cout << "cluster: configEpoch set to " << myself_->config_epoch << " after successful failover\n";
        }
        failover_replace_master();
    } else {
        cant_failover("waiting for votes, but majority still not reached");
    }
}

void Cluster::request_failover_auth() {
    Message m = header(MsgType::kFailoverAuthRequest);
    // Manual failover: the master isn't FAIL, so ask the masters to vote
    // anyway (the master itself agreed, or FORCE overrode it).
    if (mf_end_) {
        m.mflags |= cluster::kMsgForceAck;
    }
    broadcast(m);
}

// Redis's clusterFailoverReplaceYourMaster(): take our master's slots.
// Our config epoch is now the highest in the cluster, so the claim wins
// on every node as our PONGs spread, and the old master (when it's back)
// and its other replicas turn into our replicas.
void Cluster::failover_replace_master() {
    Node* old_master = lookup(myself_->master_id);
    if (myself_->is(kMaster) || old_master == nullptr) {
        return;
    }
    set_node_as_master(myself_);
    replication_.replicaof_no_one();
    for (int slot = 0; slot < kSlots; ++slot) {
        if (owner_[slot] == old_master) {
            set_slot(slot, myself_);
        }
    }
    std::cout << "cluster: failover done: took over " << myself_->slots.count() << " slots from " << old_master->id
              << " with config epoch " << myself_->config_epoch << "\n";
    // Straight to disk, and straight to everyone: the new config must
    // survive a crash, and the sooner the others see it the shorter the
    // outage.
    update_state();
    if (!save_config()) {
        std::cerr << "cluster: can't save " << options_.config_file << ": " << std::strerror(errno) << "\n";
    }
    broadcast_pong();
    reset_manual_failover();
    cant_failover("");
}

// --- failover, master side ------------------------------------------------------------

// Redis's clusterSendFailoverAuthIfNeeded(): grant `requester` our vote,
// unless one of the safety rules says no. Each refusal is logged: an
// election that doesn't converge is otherwise very hard to diagnose.
void Cluster::send_failover_auth_if_needed(Node* requester, const Message& request) {
    if (myself_->is(kReplica) || myself_->slots.none()) {
        return;  // only masters serving slots vote
    }
    auto t = now();
    auto refuse = [&requester](const std::string& why) {
        std::cout << "cluster: failover auth denied to " << requester->id << ": " << why << "\n";
    };
    // process_packet() already raised our current epoch to the request's
    // if it was higher, so a lower one is an old election.
    if (request.current_epoch < current_epoch_) {
        return refuse("its current epoch " + std::to_string(request.current_epoch) + " is older than ours " +
                      std::to_string(current_epoch_));
    }
    if (last_vote_epoch_ == current_epoch_) {
        return refuse("already voted for epoch " + std::to_string(current_epoch_));
    }
    Node* master = requester->is(kReplica) ? lookup(requester->master_id) : nullptr;
    bool force = (request.mflags & cluster::kMsgForceAck) != 0;
    if (master == nullptr) {
        return refuse(requester->is(kMaster) ? "it is a master" : "I don't know its master");
    }
    if (!master->is(kFail) && !force) {
        return refuse("its master is not failing");
    }
    // One vote per master per 2 node timeouts: once we've backed one of its
    // replicas, give that one time to win before backing another.
    if (master->voted_time != SteadyTime{} && t - master->voted_time < options_.node_timeout * 2) {
        return refuse("voted for a replica of " + master->id + " less than " +
                      std::to_string(options_.node_timeout.count() * 2) + "ms ago");
    }
    // The replica must not claim slots under an older config than their
    // current owners': that would mean it's replacing a master whose slots
    // have since moved on (it's stale; the newer owner wins).
    for (int slot = 0; slot < kSlots; ++slot) {
        if (request.slots[slot] && owner_[slot] != nullptr && owner_[slot]->config_epoch > request.config_epoch) {
            return refuse("slot " + std::to_string(slot) + " has a newer config epoch " +
                          std::to_string(owner_[slot]->config_epoch) + " than the request's " +
                          std::to_string(request.config_epoch));
        }
    }

    last_vote_epoch_ = current_epoch_;
    master->voted_time = t;
    // Durable before it's sent: after a crash and restart we must still
    // know we voted in this epoch, or we could vote again for a rival.
    if (!save_config()) {
        last_vote_epoch_ = 0;
        std::cerr << "cluster: can't save " << options_.config_file << " before voting: " << std::strerror(errno)
                  << "\n";
        return;
    }
    if (requester->link == nullptr || requester->link->connecting) {
        return refuse("no link to it to send the vote on");
    }
    send(requester->link, header(MsgType::kFailoverAuthAck));
    std::cout << "cluster: failover auth granted to " << requester->id << " for epoch " << current_epoch_ << "\n";
}

// --- manual failover ------------------------------------------------------------------

// Replica: the master's final offset is known; once we've applied that
// much of its stream we hold every write it ever acknowledged, and the
// election can start.
void Cluster::handle_manual_failover() {
    todo_handle_manual_failover_ = false;
    if (!mf_end_ || mf_can_start_ || !mf_master_offset_) {
        return;
    }
    if (*mf_master_offset_ == replication_.offset()) {
        mf_can_start_ = true;
        std::cout << "cluster: all of the master's replication stream processed, manual failover can start\n";
        todo_handle_failover_ = true;
    }
}

void Cluster::manual_failover_check_timeout() {
    if (mf_end_ && now() > *mf_end_) {
        std::cout << "cluster: manual failover timed out\n";
        reset_manual_failover();
    }
}

void Cluster::reset_manual_failover() {
    if (writes_paused_) {
        pause_writes(false);
    }
    mf_end_.reset();
    mf_replica_ = nullptr;
    mf_master_offset_.reset();
    mf_can_start_ = false;
}

// Master side of a manual failover: hold client writes, and keep keys from
// expiring (passive expiry) and the replication PINGs off the stream, so
// our offset stops moving and the replica can reach it.
void Cluster::pause_writes(bool paused) {
    writes_paused_ = paused;
    if (host_ != nullptr) {
        host_->set_writes_paused(paused);
    }
    store_.set_passive_expiry(paused || replication_.is_replica());
    replication_.set_stream_paused(paused);
}

void Cluster::before_sleep() {
    // Reacting here rather than at the next cron tick shaves up to 100ms
    // off each step of an election.
    if (todo_handle_manual_failover_ && host_ != nullptr) {
        handle_manual_failover();
    }
    if (todo_handle_failover_ && host_ != nullptr) {
        handle_replica_failover();
    }
    if (todo_update_state_) {
        update_state();
    }
    if (todo_broadcast_ && host_ != nullptr) {
        todo_broadcast_ = false;
        broadcast_pong();
    }
    // Saved before this iteration's replies go out: a client told "OK" to
    // SETSLOT must not see the change forgotten by a crash.
    if (todo_save_) {
        todo_save_ = false;
        if (!save_config()) {
            std::cerr << "cluster: can't save " << options_.config_file << ": " << std::strerror(errno) << "\n";
        }
    }
    reap_links();
}

bool Cluster::shutdown() {
    return save_config();
}

// --- nodes.conf -------------------------------------------------------------------

std::string Cluster::describe_nodes(bool for_config) const {
    std::vector<const Node*> sorted;
    for (const auto& [id, n] : nodes_) {
        if (!(for_config && n->is(kHandshake))) {
            sorted.push_back(n.get());
        }
    }
    std::sort(sorted.begin(), sorted.end(), [](const Node* a, const Node* b) { return a->id < b->id; });

    std::ostringstream s;
    for (const Node* n : sorted) {
        std::vector<std::string> flags;
        if (n->is(kMyself)) flags.push_back("myself");
        if (n->is(kMaster)) flags.push_back("master");
        if (n->is(kReplica)) flags.push_back("slave");
        if (n->is(kPFail)) flags.push_back("fail?");
        if (n->is(kFail)) flags.push_back("fail");
        if (n->is(kHandshake)) flags.push_back("handshake");
        if (n->is(kNoAddr)) flags.push_back("noaddr");
        std::string flag_str;
        for (const std::string& f : flags) {
            flag_str += (flag_str.empty() ? "" : ",") + f;
        }
        bool connected = n == myself_ || (n->link != nullptr && !n->link->connecting);
        s << n->id << " " << n->ip << ":" << n->port << "@" << n->cport << " " << (flag_str.empty() ? "noflags" : flag_str)
          << " " << (n->master_id.empty() ? "-" : n->master_id) << " " << unix_ms(n->ping_sent) << " "
          << unix_ms(n->pong_received) << " " << n->config_epoch << " " << (connected ? "connected" : "disconnected");
        for_each_range(n->slots, [&s](int start, int end) {
            s << " " << start;
            if (end != start) {
                s << "-" << end;
            }
        });
        if (n == myself_) {
            for (int slot = 0; slot < kSlots; ++slot) {
                if (migrating_to_[slot] != nullptr) {
                    s << " [" << slot << "->-" << migrating_to_[slot]->id << "]";
                }
                if (importing_from_[slot] != nullptr) {
                    s << " [" << slot << "-<-" << importing_from_[slot]->id << "]";
                }
            }
        }
        s << "\n";
    }
    if (for_config) {
        s << "vars currentEpoch " << current_epoch_ << " lastVoteEpoch " << last_vote_epoch_ << "\n";
    }
    return s.str();
}

// Temp file + fsync + rename, like the snapshot: a crash mid-write leaves
// the old config, never a torn one that would lose the node's identity.
bool Cluster::save_config() {
    std::string text = describe_nodes(true);
    std::string temp = options_.config_file + ".tmp-" + std::to_string(getpid());
    int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0644);
    if (fd < 0) {
        return false;
    }
    bool ok = fileutil::write_all(fd, text.data(), text.size()) == text.size() && fsync(fd) == 0;
    ok = close(fd) == 0 && ok;
    if (!ok || !fileutil::durable_rename(temp, options_.config_file)) {
        int saved = errno;
        unlink(temp.c_str());
        errno = saved;
        return false;
    }
    return true;
}

bool Cluster::load_config(std::string& error) {
    std::ifstream in(options_.config_file);
    if (!in) {
        if (errno != ENOENT) {
            error = options_.config_file + ": " + std::strerror(errno);
            return false;
        }
        myself_ = create_node(random_id(), kMyself | kMaster);
        std::cout << "cluster: no config found, I'm " << myself_->id << "\n";
    } else {
        std::stringstream text;
        text << in.rdbuf();
        if (!parse_config(text.str(), error)) {
            error = options_.config_file + ": " + error;
            return false;
        }
        std::cout << "cluster: loaded config, I'm " << myself_->id << "\n";
    }
    // The command line wins over the file for our own address.
    myself_->port = options_.port;
    myself_->cport = options_.cport;
    if (myself_->ip.empty() && options_.bind_addr != "0.0.0.0") {
        myself_->ip = options_.bind_addr;
    }
    update_state();
    if (!save_config()) {
        error = options_.config_file + ": " + std::strerror(errno);
        return false;
    }
    return true;
}

bool Cluster::parse_config(const std::string& text, std::string& error) {
    struct PendingMove {
        int slot;
        bool migrating;
        std::string node;
    };
    std::vector<PendingMove> moves;
    std::istringstream lines(text);
    std::string line;
    while (std::getline(lines, line)) {
        std::vector<std::string> f = split(line, ' ');
        if (f.empty()) {
            continue;
        }
        if (f[0] == "vars") {
            for (size_t i = 1; i + 1 < f.size(); i += 2) {
                if (f[i] == "currentEpoch" && !parse_num(f[i + 1], current_epoch_)) {
                    error = "bad currentEpoch";
                    return false;
                }
                if (f[i] == "lastVoteEpoch" && !parse_num(f[i + 1], last_vote_epoch_)) {
                    error = "bad lastVoteEpoch";
                    return false;
                }
            }
            continue;
        }
        if (f.size() < 8 || !cluster::valid_node_id(f[0]) || lookup(f[0]) != nullptr) {
            error = "bad node line: " + line;
            return false;
        }
        std::vector<std::string> flags = split(f[2], ',');
        if (std::find(flags.begin(), flags.end(), "handshake") != flags.end()) {
            continue;
        }
        unsigned bits = 0;
        for (const std::string& fl : flags) {
            if (fl == "myself") bits |= kMyself;
            if (fl == "master") bits |= kMaster;
            if (fl == "slave") bits |= kReplica;
            if (fl == "noaddr") bits |= kNoAddr;
            if (fl == "fail?") bits |= kPFail;
            if (fl == "fail") bits |= kFail;
        }
        // ip:port@cport
        size_t colon = f[1].rfind(':');
        size_t at = f[1].find('@');
        uint16_t port = 0;
        uint16_t cport = 0;
        if (colon == std::string::npos || at == std::string::npos || at < colon ||
            !parse_num(f[1].substr(colon + 1, at - colon - 1), port) || !parse_num(f[1].substr(at + 1), cport)) {
            error = "bad address: " + f[1];
            return false;
        }
        Node* n = create_node(f[0], bits);
        if (n->is(kFail)) {
            n->fail_time = now();  // a FAIL we knew of before the restart stands
        }
        n->ip = f[1].substr(0, colon);
        n->port = port;
        n->cport = cport;
        if (f[3] != "-") {
            n->master_id = f[3];
        }
        if (!parse_num(f[6], n->config_epoch)) {
            error = "bad config epoch: " + line;
            return false;
        }
        if (n->is(kMyself)) {
            if (myself_ != nullptr) {
                error = "more than one node flagged myself";
                return false;
            }
            myself_ = n;
        }
        for (size_t i = 8; i < f.size(); ++i) {
            const std::string& tok = f[i];
            if (tok.size() > 2 && tok.front() == '[' && tok.back() == ']') {
                // [slot->-id] migrating, [slot-<-id] importing
                std::string inner = tok.substr(1, tok.size() - 2);
                size_t mig = inner.find("->-");
                size_t imp = inner.find("-<-");
                size_t pos = mig != std::string::npos ? mig : imp;
                int slot;
                if (pos == std::string::npos || !parse_slot(inner.substr(0, pos), slot)) {
                    error = "bad slot entry: " + tok;
                    return false;
                }
                moves.push_back({slot, mig != std::string::npos, inner.substr(pos + 3)});
                continue;
            }
            size_t dash = tok.find('-');
            int start;
            int end;
            if (!parse_slot(tok.substr(0, dash), start) ||
                !parse_slot(dash == std::string::npos ? tok : tok.substr(dash + 1), end) || end < start) {
                error = "bad slot range: " + tok;
                return false;
            }
            for (int slot = start; slot <= end; ++slot) {
                set_slot(slot, n);
            }
        }
    }
    if (myself_ == nullptr) {
        error = "no node is flagged myself";
        return false;
    }
    for (const PendingMove& m : moves) {
        Node* n = lookup(m.node);
        if (n == nullptr) {
            continue;  // forgotten since: the migration can't continue anyway
        }
        (m.migrating ? migrating_to_ : importing_from_)[m.slot] = n;
    }
    return true;
}

// --- CLUSTER command ----------------------------------------------------------------

std::string Cluster::info() const {
    size_t assigned = 0;
    size_t pfail = 0;
    size_t fail = 0;
    for (const Node* n : owner_) {
        if (n != nullptr) {
            ++assigned;
            pfail += n->is(kPFail) ? 1 : 0;
            fail += n->is(kFail) ? 1 : 0;
        }
    }
    const Node* master = master_of(myself_);
    std::ostringstream s;
    s << "cluster_state:" << (state_ok_ ? "ok" : "fail") << "\r\n";
    s << "cluster_slots_assigned:" << assigned << "\r\n";
    s << "cluster_slots_ok:" << assigned - pfail - fail << "\r\n";
    s << "cluster_slots_pfail:" << pfail << "\r\n";
    s << "cluster_slots_fail:" << fail << "\r\n";
    s << "cluster_known_nodes:" << nodes_.size() << "\r\n";
    s << "cluster_size:" << cluster_size() << "\r\n";
    s << "cluster_current_epoch:" << current_epoch_ << "\r\n";
    s << "cluster_my_epoch:" << (master != nullptr ? master->config_epoch : 0) << "\r\n";
    s << "cluster_stats_messages_sent:" << messages_sent_ << "\r\n";
    s << "cluster_stats_messages_received:" << messages_received_ << "\r\n";
    return s.str();
}

void Cluster::command(const Args& argv, std::string& out) {
    std::string sub = argv.size() > 1 ? to_upper(argv[1]) : "";
    size_t argc = argv.size();
    auto wrong_args = [&out, &argv] {
        reply::error(out, "ERR wrong number of arguments for 'cluster|" + argv[1] + "' command");
    };

    if (sub == "INFO" && argc == 2) {
        reply::bulk_string(out, info());
    } else if (sub == "MYID" && argc == 2) {
        reply::bulk_string(out, myself_->id);
    } else if (sub == "NODES" && argc == 2) {
        reply::bulk_string(out, describe_nodes(false));
    } else if (sub == "SLOTS" && argc == 2) {
        cmd_slots(out);
    } else if (sub == "KEYSLOT") {
        if (argc != 3) return wrong_args();
        reply::integer(out, cluster::key_hash_slot(argv[2]));
    } else if (sub == "COUNTKEYSINSLOT") {
        int slot;
        if (argc != 3) return wrong_args();
        if (!parse_slot(argv[2], slot)) return reply::error(out, "ERR Invalid slot");
        reply::integer(out, static_cast<int64_t>(store_.count_keys_in_slot(slot)));
    } else if (sub == "GETKEYSINSLOT") {
        int slot;
        int64_t count;
        if (argc != 4) return wrong_args();
        if (!parse_slot(argv[2], slot) || !parse_num(argv[3], count) || count < 0) {
            return reply::error(out, "ERR Invalid slot or number of keys");
        }
        std::vector<std::string> keys = store_.keys_in_slot(slot, static_cast<size_t>(count));
        reply::command(out, keys);  // an array of bulk strings
    } else if (sub == "ADDSLOTS" || sub == "DELSLOTS") {
        if (argc < 3) return wrong_args();
        cmd_addslots(argv, false, sub == "ADDSLOTS", out);
    } else if (sub == "ADDSLOTSRANGE" || sub == "DELSLOTSRANGE") {
        if (argc < 4 || argc % 2 != 0) return wrong_args();
        cmd_addslots(argv, true, sub == "ADDSLOTSRANGE", out);
    } else if (sub == "SETSLOT") {
        if (argc < 4) return wrong_args();
        cmd_setslot(argv, out);
    } else if (sub == "MEET") {
        if (argc != 4 && argc != 5) return wrong_args();
        cmd_meet(argv, out);
    } else if (sub == "REPLICATE") {
        if (argc != 3) return wrong_args();
        cmd_replicate(argv, out);
    } else if (sub == "FORGET") {
        if (argc != 3) return wrong_args();
        cmd_forget(argv, out);
    } else if (sub == "FAILOVER") {
        if (argc > 3) return wrong_args();
        cmd_failover(argv, out);
    } else if (sub == "COUNT-FAILURE-REPORTS") {
        if (argc != 3) return wrong_args();
        Node* n = lookup(argv[2]);
        if (n == nullptr) return reply::error(out, "ERR Unknown node " + argv[2]);
        reply::integer(out, static_cast<int64_t>(count_fail_reports(n)));
    } else if (sub == "SET-CONFIG-EPOCH") {
        uint64_t epoch;
        if (argc != 3) return wrong_args();
        if (!parse_num(argv[2], epoch)) return reply::error(out, "ERR Invalid config epoch specified: " + argv[2]);
        if (nodes_.size() > 1) {
            return reply::error(out, "ERR The user can assign a config epoch only when the node does not know any other node.");
        }
        if (myself_->config_epoch != 0) {
            return reply::error(out, "ERR Node config epoch is already non-zero");
        }
        myself_->config_epoch = epoch;
        current_epoch_ = std::max(current_epoch_, epoch);
        todo_save();
        reply::simple_string(out, "OK");
    } else if (sub == "BUMPEPOCH" && argc == 2) {
        bool bumped = bump_epoch_without_consensus();
        reply::simple_string(out, std::string(bumped ? "BUMPED " : "STILL ") + std::to_string(myself_->config_epoch));
    } else if (sub == "SAVECONFIG" && argc == 2) {
        if (!save_config()) {
            return reply::error(out, std::string("ERR error saving the cluster node config: ") + std::strerror(errno));
        }
        reply::simple_string(out, "OK");
    } else {
        reply::error(out, "ERR unknown subcommand or wrong number of arguments for '" +
                              (argc > 1 ? argv[1] : std::string()) + "'. Try CLUSTER HELP.");
    }
}

// ADDSLOTS slot... / DELSLOTS slot... / ADDSLOTSRANGE start end... /
// DELSLOTSRANGE start end... — all-or-nothing: every slot is validated
// before any is changed.
void Cluster::cmd_addslots(const Args& argv, bool range, bool add, std::string& out) {
    if (myself_->is(kReplica)) {
        return reply::error(out, "ERR Please use this command only with masters.");
    }
    std::vector<int> slots;
    for (size_t i = 2; i < argv.size(); i += range ? 2 : 1) {
        int start;
        int end;
        if (!parse_slot(argv[i], start) || (range && !parse_slot(argv[i + 1], end))) {
            return reply::error(out, "ERR Invalid or out of range slot");
        }
        if (!range) {
            end = start;
        }
        if (end < start) {
            return reply::error(out, "ERR start slot number " + std::to_string(start) +
                                         " is greater than end slot number " + std::to_string(end));
        }
        for (int slot = start; slot <= end; ++slot) {
            slots.push_back(slot);
        }
    }
    std::vector<bool> seen(kSlots);
    for (int slot : slots) {
        if (seen[slot]) {
            return reply::error(out, "ERR Slot " + std::to_string(slot) + " specified multiple times");
        }
        seen[slot] = true;
        if (add && owner_[slot] != nullptr) {
            return reply::error(out, "ERR Slot " + std::to_string(slot) + " is already busy");
        }
        if (!add && owner_[slot] == nullptr) {
            return reply::error(out, "ERR Slot " + std::to_string(slot) + " is already unassigned");
        }
    }
    for (int slot : slots) {
        if (add) {
            importing_from_[slot] = nullptr;  // we're the real owner now
        }
        set_slot(slot, add ? myself_ : nullptr);
    }
    todo_save();
    todo_broadcast_ = true;
    reply::simple_string(out, "OK");
}

// SETSLOT slot MIGRATING node | IMPORTING node | STABLE | NODE node
void Cluster::cmd_setslot(const Args& argv, std::string& out) {
    if (myself_->is(kReplica)) {
        return reply::error(out, "ERR Please use SETSLOT only with masters.");
    }
    int slot;
    if (!parse_slot(argv[2], slot)) {
        return reply::error(out, "ERR Invalid or out of range slot");
    }
    std::string action = to_upper(argv[3]);
    if (action == "STABLE") {
        if (argv.size() != 4) return reply::error(out, "ERR syntax error");
        migrating_to_[slot] = nullptr;
        importing_from_[slot] = nullptr;
        todo_save();
        return reply::simple_string(out, "OK");
    }
    if (argv.size() != 5 || (action != "MIGRATING" && action != "IMPORTING" && action != "NODE")) {
        return reply::error(out, "ERR Invalid CLUSTER SETSLOT action or number of arguments. Try CLUSTER HELP");
    }
    Node* n = lookup(argv[4]);
    if (n == nullptr || n->is(kHandshake)) {
        return reply::error(out, "ERR I don't know about node " + argv[4]);
    }
    if (n->is(kReplica)) {
        return reply::error(out, "ERR Target node is not a master");
    }

    if (action == "MIGRATING") {
        if (owner_[slot] != myself_) {
            return reply::error(out, "ERR I'm not the owner of hash slot " + std::to_string(slot));
        }
        migrating_to_[slot] = n;
    } else if (action == "IMPORTING") {
        if (owner_[slot] == myself_) {
            return reply::error(out, "ERR I'm already the owner of hash slot " + std::to_string(slot));
        }
        importing_from_[slot] = n;
    } else {  // NODE
        if (owner_[slot] == myself_ && n != myself_ && store_.count_keys_in_slot(slot) > 0) {
            return reply::error(out, "ERR Can't assign hashslot " + std::to_string(slot) +
                                         " to a different node while I still hold keys for this hash slot.");
        }
        if (store_.count_keys_in_slot(slot) == 0 && migrating_to_[slot] != nullptr) {
            migrating_to_[slot] = nullptr;
        }
        bool finishes_import = n == myself_ && importing_from_[slot] != nullptr;
        if (n == myself_) {
            importing_from_[slot] = nullptr;
        }
        set_slot(slot, n);
        // The migration is over and we own the slot, but other nodes still
        // believe the source does, under the source's epoch. A fresh,
        // highest epoch makes our claim win everywhere as it spreads.
        if (finishes_import && bump_epoch_without_consensus()) {
            std::cout << "cluster: took over slot " << slot << " with config epoch " << myself_->config_epoch << "\n";
        }
        todo_broadcast_ = true;
    }
    todo_save();
    reply::simple_string(out, "OK");
}

// MEET ip port [cport]
void Cluster::cmd_meet(const Args& argv, std::string& out) {
    int64_t port;
    int64_t cport;
    bool ok = parse_num(argv[3], port) && port > 0 && port <= 65535;
    cport = port + 10000;
    if (argv.size() == 5) {
        ok = ok && parse_num(argv[4], cport);
    }
    if (!ok || cport <= 0 || cport > 65535) {
        return reply::error(out, "ERR Invalid base port specified: " + argv[3]);
    }
    if (!valid_ipv4(argv[2])) {
        return reply::error(out, "ERR Invalid node address specified: " + argv[2] + ":" + argv[3]);
    }
    start_handshake(argv[2], static_cast<uint16_t>(port), static_cast<uint16_t>(cport));
    reply::simple_string(out, "OK");
}

// REPLICATE node-id — become a replica of that master.
void Cluster::cmd_replicate(const Args& argv, std::string& out) {
    Node* n = lookup(argv[2]);
    if (n == nullptr || n->is(kHandshake)) {
        return reply::error(out, "ERR Unknown node " + argv[2]);
    }
    if (n == myself_) {
        return reply::error(out, "ERR Can't replicate myself");
    }
    if (n->is(kReplica)) {
        return reply::error(out, "ERR I can only replicate a master, not a replica.");
    }
    // A master with slots or data would silently lose them to the full
    // resync; make the admin empty it (or move its slots) first.
    if (myself_->is(kMaster) && (myself_->slots.any() || store_.size() != 0)) {
        return reply::error(out, "ERR To set a master the node must be empty and without assigned slots.");
    }
    set_as_replica_of(n);
    reply::simple_string(out, "OK");
}

// FORGET node-id — drop the node from our table and ignore it in gossip for
// a minute, long enough to run FORGET on every other node too.
void Cluster::cmd_forget(const Args& argv, std::string& out) {
    Node* n = lookup(argv[2]);
    if (n == nullptr) {
        return reply::error(out, "ERR Unknown node " + argv[2]);
    }
    if (n == myself_) {
        return reply::error(out, "ERR I tried hard but I can't forget myself...");
    }
    if (myself_->is(kReplica) && myself_->master_id == n->id) {
        return reply::error(out, "ERR Can't forget my master!");
    }
    blacklist_[n->id] = now() + kBlacklistTtl;
    del_node(n);
    reply::simple_string(out, "OK");
}

// FAILOVER [FORCE | TAKEOVER] — run on a replica to promote it now:
//  - (default) coordinated with a live master: it pauses writes, we catch
//    up to its final offset, then win a normal election. No writes lost.
//  - FORCE: the master is unreachable; skip the catch-up, still hold an
//    election (masters vote without the master being FAIL).
//  - TAKEOVER: no election either; just take a fresh epoch and the slots.
//    For when a majority of masters is gone and no vote could succeed.
void Cluster::cmd_failover(const Args& argv, std::string& out) {
    bool force = false;
    bool takeover = false;
    if (argv.size() == 3) {
        std::string opt = to_upper(argv[2]);
        if (opt == "FORCE") {
            force = true;
        } else if (opt == "TAKEOVER") {
            takeover = true;
        } else {
            return reply::error(out, "ERR syntax error");
        }
    }
    if (myself_->is(kMaster)) {
        return reply::error(out, "ERR You should send CLUSTER FAILOVER to a replica");
    }
    Node* master = lookup(myself_->master_id);
    if (master == nullptr) {
        return reply::error(out, "ERR I'm a replica but my master is unknown to me");
    }
    if (!force && !takeover && (master->is(kFail) || master->link == nullptr || master->link->connecting)) {
        return reply::error(out, "ERR Master is down or failed, please use CLUSTER FAILOVER FORCE");
    }
    reset_manual_failover();
    mf_end_ = now() + kManualFailoverTimeout;
    if (takeover) {
        std::cout << "cluster: taking over the master (CLUSTER FAILOVER TAKEOVER)\n";
        bump_epoch_without_consensus();
        failover_replace_master();
    } else if (force) {
        std::cout << "cluster: forced failover requested (CLUSTER FAILOVER FORCE)\n";
        mf_can_start_ = true;
        failover_auth_time_.reset();  // start a fresh election now
        todo_handle_failover_ = true;
    } else {
        std::cout << "cluster: manual failover requested; asking the master to pause writes\n";
        failover_auth_time_.reset();
        send(master->link, header(MsgType::kMfStart));
    }
    reply::simple_string(out, "OK");
}

// SLOTS — per contiguous slot range: start, end, then the master and its
// replicas as [ip, port, id]. What cluster clients load as their slot map.
void Cluster::cmd_slots(std::string& out) const {
    struct Range {
        int start;
        int end;
        const Node* owner;
    };
    std::vector<Range> ranges;
    for (int slot = 0; slot < kSlots; ++slot) {
        const Node* owner = owner_[slot];
        if (owner == nullptr) {
            continue;
        }
        if (!ranges.empty() && ranges.back().owner == owner && ranges.back().end == slot - 1) {
            ranges.back().end = slot;
        } else {
            ranges.push_back({slot, slot, owner});
        }
    }
    auto node_entry = [&out](const Node* n) {
        reply::array_header(out, 3);
        reply::bulk_string(out, n->ip);
        reply::integer(out, n->port);
        reply::bulk_string(out, n->id);
    };
    reply::array_header(out, ranges.size());
    for (const Range& r : ranges) {
        std::vector<const Node*> replicas;
        for (const auto& [id, n] : nodes_) {
            if (n->is(kReplica) && n->master_id == r.owner->id && !n->is(kHandshake | kNoAddr)) {
                replicas.push_back(n.get());
            }
        }
        reply::array_header(out, 3 + replicas.size());
        reply::integer(out, r.start);
        reply::integer(out, r.end);
        node_entry(r.owner);
        for (const Node* rep : replicas) {
            node_entry(rep);
        }
    }
}

void Cluster::verify_config_with_data() {
    if (myself_->is(kReplica)) {
        return;  // our master decides what we hold
    }
    int claimed = 0;
    int importing = 0;
    for (int slot = 0; slot < kSlots; ++slot) {
        if (store_.count_keys_in_slot(slot) == 0 || owner_[slot] == myself_ || importing_from_[slot] != nullptr) {
            continue;
        }
        if (owner_[slot] == nullptr) {
            set_slot(slot, myself_);
            ++claimed;
        } else {
            importing_from_[slot] = owner_[slot];
            ++importing;
        }
    }
    if (claimed > 0 || importing > 0) {
        std::cout << "cluster: keys on disk for slots not ours: claimed " << claimed
                  << " unassigned slots, set " << importing << " slots owned elsewhere to importing\n";
        todo_save();
    }
    update_state();
}
