#include "command_dispatcher.hpp"

#include <cctype>
#include <charconv>
#include <optional>
#include <utility>

#include "client.hpp"
#include "cluster.hpp"
#include "migrate.hpp"
#include "persistence.hpp"
#include "replication.hpp"
#include "reply.hpp"
#include "snapshot.hpp"

namespace {

constexpr const char* kErrNotInteger = "ERR value is not an integer or out of range";
constexpr const char* kErrSyntax = "ERR syntax error";
constexpr const char* kErrOom = "OOM command not allowed when used memory > 'maxmemory'.";
constexpr const char* kErrNoPersistence = "ERR persistence is not enabled on this server";
constexpr const char* kErrNoReplication = "ERR replication is not available here";
constexpr const char* kErrReadOnly = "READONLY You can't write against a read only replica.";

std::string to_upper(const std::string& s) {
    std::string upper(s);
    for (char& c : upper) {
        c = static_cast<char>(std::toupper(static_cast<unsigned char>(c)));
    }
    return upper;
}

// Client-supplied text echoed inside an error reply must not contain CR/LF:
// an error is a single +/- line, so an embedded "\r\n" would end it early
// and the rest would be parsed by the client as a separate, forged reply.
std::string sanitize_for_error(const std::string& s) {
    std::string clean(s);
    for (char& c : clean) {
        if (c == '\r' || c == '\n') {
            c = ' ';
        }
    }
    return clean;
}

// Strict base-10 int64 parse: the whole string must be consumed (no "12abc",
// no leading spaces or '+'), and out-of-range values are rejected rather
// than wrapped.
bool parse_int64(const std::string& s, int64_t& out) {
    const char* first = s.data();
    const char* last = s.data() + s.size();
    auto [ptr, ec] = std::from_chars(first, last, out);
    return ec == std::errc() && ptr == last && first != last;
}

// Absolute deadline = now + amount * unit_ms, or nullopt on int64 overflow.
// A client can send any 64-bit count, so both steps are overflow-checked.
std::optional<int64_t> deadline_from(int64_t now, int64_t amount, int64_t unit_ms) {
    int64_t delta;
    int64_t when;
    if (__builtin_mul_overflow(amount, unit_ms, &delta) || __builtin_add_overflow(now, delta, &when)) {
        return std::nullopt;
    }
    return when;
}

}  // namespace

CommandDispatcher::CommandDispatcher(Store& store)
    : store_(store),
      table_{
          //                     arity  flags              keys: first last step
          {"PING", {"ping", &CommandDispatcher::cmd_ping, -1, 0, 0, 0, 0}},
          {"ECHO", {"echo", &CommandDispatcher::cmd_echo, 2, 0, 0, 0, 0}},
          {"SET", {"set", &CommandDispatcher::cmd_set, -3, kWrite | kDenyOom, 1, 1, 1}},
          {"GET", {"get", &CommandDispatcher::cmd_get, 2, 0, 1, 1, 1}},
          {"DEL", {"del", &CommandDispatcher::cmd_del, -2, kWrite, 1, -1, 1}},
          {"EXISTS", {"exists", &CommandDispatcher::cmd_exists, -2, 0, 1, -1, 1}},
          {"EXPIRE", {"expire", &CommandDispatcher::cmd_expire, 3, kWrite, 1, 1, 1}},
          {"PEXPIRE", {"pexpire", &CommandDispatcher::cmd_pexpire, 3, kWrite, 1, 1, 1}},
          {"EXPIREAT", {"expireat", &CommandDispatcher::cmd_expireat, 3, kWrite, 1, 1, 1}},
          {"PEXPIREAT", {"pexpireat", &CommandDispatcher::cmd_pexpireat, 3, kWrite, 1, 1, 1}},
          {"TTL", {"ttl", &CommandDispatcher::cmd_ttl, 2, 0, 1, 1, 1}},
          {"PTTL", {"pttl", &CommandDispatcher::cmd_pttl, 2, 0, 1, 1, 1}},
          {"PERSIST", {"persist", &CommandDispatcher::cmd_persist, 2, kWrite, 1, 1, 1}},
          {"DBSIZE", {"dbsize", &CommandDispatcher::cmd_dbsize, 1, 0, 0, 0, 0}},
          {"SAVE", {"save", &CommandDispatcher::cmd_save, 1, 0, 0, 0, 0}},
          {"BGSAVE", {"bgsave", &CommandDispatcher::cmd_bgsave, 1, 0, 0, 0, 0}},
          {"BGREWRITEAOF", {"bgrewriteaof", &CommandDispatcher::cmd_bgrewriteaof, 1, 0, 0, 0, 0}},
          {"LASTSAVE", {"lastsave", &CommandDispatcher::cmd_lastsave, 1, 0, 0, 0, 0}},
          {"REPLICAOF", {"replicaof", &CommandDispatcher::cmd_replicaof, 3, 0, 0, 0, 0}},
          {"SLAVEOF", {"slaveof", &CommandDispatcher::cmd_replicaof, 3, 0, 0, 0, 0}},
          {"REPLCONF", {"replconf", &CommandDispatcher::cmd_replconf, -3, 0, 0, 0, 0}},
          {"PSYNC", {"psync", &CommandDispatcher::cmd_psync, 3, 0, 0, 0, 0}},
          {"WAIT", {"wait", &CommandDispatcher::cmd_wait, 3, 0, 0, 0, 0}},
          {"INFO", {"info", &CommandDispatcher::cmd_info, -1, 0, 0, 0, 0}},
          {"DUMP", {"dump", &CommandDispatcher::cmd_dump, 2, 0, 1, 1, 1}},
          {"RESTORE", {"restore", &CommandDispatcher::cmd_restore, -4, kWrite | kDenyOom, 1, 1, 1}},
          {"RESTORE-ASKING",
           {"restore-asking", &CommandDispatcher::cmd_restore, -4, kWrite | kDenyOom | kAsking, 1, 1, 1}},
          // MIGRATE's keys aren't routed: it acts on whatever this node
          // holds, which is exactly what a node mid-migration must do.
          {"MIGRATE", {"migrate", &CommandDispatcher::cmd_migrate, -6, kWrite, 0, 0, 0}},
          {"CLUSTER", {"cluster", &CommandDispatcher::cmd_cluster, -2, 0, 0, 0, 0}},
          {"ASKING", {"asking", &CommandDispatcher::cmd_asking, 1, 0, 0, 0, 0}},
          {"READONLY", {"readonly", &CommandDispatcher::cmd_readonly, 1, 0, 0, 0, 0}},
          {"READWRITE", {"readwrite", &CommandDispatcher::cmd_readwrite, 1, 0, 0, 0, 0}},
      } {}

bool CommandDispatcher::loading() const {
    return persistence_ != nullptr && persistence_->loading();
}

void CommandDispatcher::propagate(const Args& argv) {
    if (propagator_) {
        propagator_(argv);
    }
}

bool CommandDispatcher::arity_ok(int arity, size_t argc) {
    if (arity >= 0) {
        return argc == static_cast<size_t>(arity);
    }
    return argc >= static_cast<size_t>(-arity);
}

bool CommandDispatcher::is_write(const Args& argv) const {
    if (argv.empty()) {
        return false;
    }
    auto it = table_.find(to_upper(argv[0]));
    return it != table_.end() && (it->second.flags & kWrite) != 0;
}

void CommandDispatcher::dispatch(const Args& argv, std::string& out, Client* client) {
    if (argv.empty()) {
        return;
    }
    // ASKING covers exactly the next command, whatever happens to it.
    bool asking = client != nullptr && client->asking;
    if (client != nullptr) {
        client->asking = false;
    }

    auto it = table_.find(to_upper(argv[0]));
    if (it == table_.end()) {
        reply::error(out, "ERR unknown command '" + sanitize_for_error(argv[0]) + "'");
        return;
    }

    const CommandSpec& spec = it->second;
    if (!arity_ok(spec.arity, argv.size())) {
        reply::error(out, std::string("ERR wrong number of arguments for '") + spec.name + "' command");
        return;
    }

    // Our master's stream is obeyed unconditionally, past every check below:
    // it already applied these writes, and refusing one would leave us
    // diverged from it for good.
    bool from_master = client != nullptr && client->is_master;

    // Cluster redirection comes first, as in Redis: a node that doesn't
    // serve the slot shouldn't judge the command any further. Skipped for
    // internal callers (AOF replay) and our master's stream, which only
    // ever carry keys this node is meant to hold.
    if (cluster_ != nullptr && client != nullptr && !from_master && spec.first_key > 0) {
        std::vector<const std::string*> keys;
        int argc = static_cast<int>(argv.size());
        int last = spec.last_key < 0 ? argc + spec.last_key : spec.last_key;
        for (int i = spec.first_key; i <= last && i < argc; i += spec.key_step) {
            keys.push_back(&argv[i]);
        }
        std::string redirect;
        if (!cluster_->route(keys, (spec.flags & kWrite) != 0, asking || (spec.flags & kAsking), *client, redirect)) {
            reply::error(out, redirect);
            return;
        }
    }

    if ((spec.flags & kWrite) && !from_master && replication_ != nullptr && replication_->is_replica()) {
        reply::error(out, kErrReadOnly);
        return;
    }

    // Both checks are skipped while replaying the AOF at startup: those
    // writes were already accepted once, and refusing them now would load a
    // different dataset than the one that was acknowledged.
    if ((spec.flags & kWrite) && persistence_ != nullptr && !loading() && !from_master) {
        std::string err = persistence_->write_error();
        if (!err.empty()) {
            reply::error(out, err);
            return;
        }
    }

    // Evict *before* the write rather than after: the command then runs
    // against a store that's within budget, and may overshoot by at most one
    // value until the next write evicts again — Redis's semantics.
    // A replica never evicts on its own: the master's evictions arrive as DELs.
    if ((spec.flags & kDenyOom) && !loading() && !from_master && !store_.evict_if_needed()) {
        reply::error(out, kErrOom);
        return;
    }

    Client* previous = std::exchange(client_, client);
    (this->*spec.handler)(argv, out);
    client_ = previous;
}

// PING [message]
void CommandDispatcher::cmd_ping(const Args& argv, std::string& out) {
    if (argv.size() == 1) {
        reply::simple_string(out, "PONG");
    } else if (argv.size() == 2) {
        reply::bulk_string(out, argv[1]);
    } else {
        reply::error(out, "ERR wrong number of arguments for 'ping' command");
    }
}

// ECHO message
void CommandDispatcher::cmd_echo(const Args& argv, std::string& out) {
    reply::bulk_string(out, argv[1]);
}

// SET key value [NX | XX] [EX seconds | PX milliseconds |
//                          EXAT unix-seconds | PXAT unix-ms | KEEPTTL]
// Replies OK, or a null bulk if an NX/XX condition prevented the write.
// Propagated as SET key value [PXAT unix-ms | KEEPTTL].
void CommandDispatcher::cmd_set(const Args& argv, std::string& out) {
    bool nx = false;
    bool xx = false;
    bool keep_ttl = false;
    std::optional<int64_t> expire_at;

    for (size_t i = 3; i < argv.size(); ++i) {
        std::string opt = to_upper(argv[i]);
        if (opt == "NX" && !xx) {
            nx = true;
        } else if (opt == "XX" && !nx) {
            xx = true;
        } else if (opt == "KEEPTTL" && !expire_at) {
            keep_ttl = true;
        } else if ((opt == "EX" || opt == "PX" || opt == "EXAT" || opt == "PXAT") && !keep_ttl && !expire_at &&
                   i + 1 < argv.size()) {
            int64_t amount;
            if (!parse_int64(argv[++i], amount)) {
                reply::error(out, kErrNotInteger);
                return;
            }
            bool relative = opt == "EX" || opt == "PX";
            int64_t unit_ms = (opt == "EX" || opt == "EXAT") ? 1000 : 1;
            expire_at = amount > 0 ? deadline_from(relative ? store_.now_ms() : 0, amount, unit_ms) : std::nullopt;
            if (!expire_at) {
                reply::error(out, "ERR invalid expire time in 'set' command");
                return;
            }
        } else {
            reply::error(out, kErrSyntax);
            return;
        }
    }

    if (nx || xx) {
        bool exists = store_.exists(argv[1]);
        if ((nx && exists) || (xx && !exists)) {
            reply::null_bulk(out);
            return;
        }
    }

    store_.set(argv[1], argv[2], keep_ttl);
    if (expire_at) {
        store_.set_expire(argv[1], *expire_at);
    }
    // "EX 10" means something different when replayed a day later, so the
    // log gets the absolute deadline instead.
    if (propagator_) {
        Args logged{"SET", argv[1], argv[2]};
        if (expire_at) {
            logged.insert(logged.end(), {"PXAT", std::to_string(*expire_at)});
        } else if (keep_ttl) {
            logged.push_back("KEEPTTL");
        }
        propagate(logged);
    }
    reply::simple_string(out, "OK");
}

// GET key
void CommandDispatcher::cmd_get(const Args& argv, std::string& out) {
    if (const std::string* value = store_.get(argv[1])) {
        reply::bulk_string(out, *value);
    } else {
        reply::null_bulk(out);
    }
}

// DEL key [key ...] — replies with how many of the keys existed.
void CommandDispatcher::cmd_del(const Args& argv, std::string& out) {
    int64_t removed = 0;
    for (size_t i = 1; i < argv.size(); ++i) {
        if (store_.del(argv[i])) {
            ++removed;
        }
    }
    if (removed > 0) {
        propagate(argv);
    }
    reply::integer(out, removed);
}

// EXISTS key [key ...] — a key named twice is counted twice, as in Redis.
void CommandDispatcher::cmd_exists(const Args& argv, std::string& out) {
    int64_t found = 0;
    for (size_t i = 1; i < argv.size(); ++i) {
        if (store_.exists(argv[i])) {
            ++found;
        }
    }
    reply::integer(out, found);
}

// EXPIRE key seconds / PEXPIRE key ms / EXPIREAT key unix-seconds /
// PEXPIREAT key unix-ms — replies 1 if the TTL was set, 0 if the key doesn't
// exist. A deadline at or before now deletes the key. Propagated as
// PEXPIREAT with the absolute deadline, or as DEL if the key was deleted.
void CommandDispatcher::expire_generic(const Args& argv, int64_t unit_ms, bool relative, const char* name,
                                       std::string& out) {
    int64_t amount;
    if (!parse_int64(argv[2], amount)) {
        reply::error(out, kErrNotInteger);
        return;
    }
    int64_t now = store_.now_ms();
    std::optional<int64_t> when = deadline_from(relative ? now : 0, amount, unit_ms);
    if (!when) {
        reply::error(out, std::string("ERR invalid expire time in '") + name + "' command");
        return;
    }
    if (!store_.set_expire(argv[1], *when)) {
        reply::integer(out, 0);
        return;
    }
    propagate(*when <= now ? Args{"DEL", argv[1]} : Args{"PEXPIREAT", argv[1], std::to_string(*when)});
    reply::integer(out, 1);
}

void CommandDispatcher::cmd_expire(const Args& argv, std::string& out) {
    expire_generic(argv, 1000, true, "expire", out);
}

void CommandDispatcher::cmd_pexpire(const Args& argv, std::string& out) {
    expire_generic(argv, 1, true, "pexpire", out);
}

void CommandDispatcher::cmd_expireat(const Args& argv, std::string& out) {
    expire_generic(argv, 1000, false, "expireat", out);
}

void CommandDispatcher::cmd_pexpireat(const Args& argv, std::string& out) {
    expire_generic(argv, 1, false, "pexpireat", out);
}

// TTL key — seconds remaining (rounded), -1 if no TTL, -2 if no such key.
void CommandDispatcher::cmd_ttl(const Args& argv, std::string& out) {
    int64_t ms = store_.pttl(argv[1]);
    reply::integer(out, ms < 0 ? ms : (ms + 500) / 1000);
}

// PTTL key — milliseconds remaining, -1 if no TTL, -2 if no such key.
void CommandDispatcher::cmd_pttl(const Args& argv, std::string& out) {
    reply::integer(out, store_.pttl(argv[1]));
}

// PERSIST key — replies 1 if a TTL was removed, 0 otherwise.
void CommandDispatcher::cmd_persist(const Args& argv, std::string& out) {
    if (!store_.persist(argv[1])) {
        reply::integer(out, 0);
        return;
    }
    propagate(argv);
    reply::integer(out, 1);
}

// DBSIZE — number of keys, including expired ones not yet reclaimed.
void CommandDispatcher::cmd_dbsize(const Args&, std::string& out) {
    reply::integer(out, static_cast<int64_t>(store_.size()));
}

// SAVE — writes a snapshot synchronously, blocking every client meanwhile.
void CommandDispatcher::cmd_save(const Args&, std::string& out) {
    if (persistence_ == nullptr) {
        reply::error(out, kErrNoPersistence);
        return;
    }
    std::string err;
    if (persistence_->save(err)) {
        reply::simple_string(out, "OK");
    } else {
        reply::error(out, err);
    }
}

// BGSAVE — writes a snapshot from a forked child; replies immediately.
void CommandDispatcher::cmd_bgsave(const Args&, std::string& out) {
    if (persistence_ == nullptr) {
        reply::error(out, kErrNoPersistence);
        return;
    }
    std::string err;
    if (persistence_->bgsave(err)) {
        reply::simple_string(out, "Background saving started");
    } else {
        reply::error(out, err);
    }
}

// BGREWRITEAOF — compacts the AOF from a forked child; queued if a
// background save is running.
void CommandDispatcher::cmd_bgrewriteaof(const Args&, std::string& out) {
    if (persistence_ == nullptr) {
        reply::error(out, kErrNoPersistence);
        return;
    }
    std::string err;
    switch (persistence_->bgrewriteaof(err)) {
        case Persistence::RewriteStart::kStarted:
            reply::simple_string(out, "Background append only file rewriting started");
            break;
        case Persistence::RewriteStart::kScheduled:
            reply::simple_string(out, "Background append only file rewriting scheduled");
            break;
        case Persistence::RewriteStart::kError:
            reply::error(out, err);
            break;
    }
}

// LASTSAVE — unix time (seconds) of the last successful snapshot.
void CommandDispatcher::cmd_lastsave(const Args&, std::string& out) {
    if (persistence_ == nullptr) {
        reply::error(out, kErrNoPersistence);
        return;
    }
    reply::integer(out, persistence_->lastsave());
}

// REPLICAOF host port — become a replica of that server, discarding our
// dataset for its. REPLICAOF NO ONE — stop replicating and become a master,
// keeping the data. (SLAVEOF is the old name.)
void CommandDispatcher::cmd_replicaof(const Args& argv, std::string& out) {
    if (replication_ == nullptr) {
        reply::error(out, kErrNoReplication);
        return;
    }
    // In a cluster, who replicates whom is cluster configuration, gossiped
    // to every node; CLUSTER REPLICATE changes it.
    if (cluster_ != nullptr) {
        reply::error(out, "ERR REPLICAOF not allowed in cluster mode.");
        return;
    }
    if (to_upper(argv[1]) == "NO" && to_upper(argv[2]) == "ONE") {
        replication_->replicaof_no_one();
        reply::simple_string(out, "OK");
        return;
    }
    int64_t port;
    if (!parse_int64(argv[2], port) || port <= 0 || port > 65535) {
        reply::error(out, "ERR Invalid master port");
        return;
    }
    replication_->replicaof(argv[1], static_cast<uint16_t>(port));
    reply::simple_string(out, "OK");
}

// REPLCONF option value [option value ...] — replica/master handshake:
//   listening-port <port>  the port the replica serves on (for INFO)
//   capa <capability>      what the replica supports; psync2 is implied
//   ack <offset>           replica -> master: stream bytes applied so far
//   getack *               master -> replica: send an ack now
// ack and getack are never answered: they travel on replication links,
// where a reply would be read as stream data.
void CommandDispatcher::cmd_replconf(const Args& argv, std::string& out) {
    if (argv.size() % 2 == 0) {
        reply::error(out, kErrSyntax);
        return;
    }
    for (size_t i = 1; i < argv.size(); i += 2) {
        std::string opt = to_upper(argv[i]);
        if (opt == "LISTENING-PORT") {
            int64_t port;
            if (!parse_int64(argv[i + 1], port) || port <= 0 || port > 65535) {
                reply::error(out, "ERR Invalid listening port");
                return;
            }
            if (client_ != nullptr) {
                client_->repl_listening_port = static_cast<uint16_t>(port);
            }
        } else if (opt == "CAPA") {
            continue;
        } else if (opt == "ACK") {
            int64_t offset;
            if (replication_ != nullptr && client_ != nullptr && parse_int64(argv[i + 1], offset) && offset >= 0) {
                replication_->replconf_ack(*client_, static_cast<uint64_t>(offset));
            }
            return;
        } else if (opt == "GETACK") {
            if (replication_ != nullptr && client_ != nullptr && client_->is_master) {
                replication_->replconf_getack();
            }
            return;
        } else {
            reply::error(out, "ERR Unrecognized REPLCONF option: " + sanitize_for_error(argv[i]));
            return;
        }
    }
    reply::simple_string(out, "OK");
}

// PSYNC replid offset — a replica asking to continue history `replid` from
// byte `offset`; "PSYNC ? -1" asks for a full resync. Replies +CONTINUE or
// +FULLRESYNC, after which this connection carries the replication stream.
void CommandDispatcher::cmd_psync(const Args& argv, std::string& out) {
    if (replication_ == nullptr || client_ == nullptr) {
        reply::error(out, kErrNoReplication);
        return;
    }
    int64_t offset;
    if (!parse_int64(argv[2], offset)) {
        reply::error(out, kErrNotInteger);
        return;
    }
    replication_->psync(*client_, argv[1], offset, out);
}

// WAIT numreplicas timeout-ms — blocks until this client's writes so far
// have been acknowledged by at least numreplicas replicas, or the timeout
// passes (0 = wait forever). Replies with how many acknowledged.
void CommandDispatcher::cmd_wait(const Args& argv, std::string& out) {
    if (replication_ == nullptr || client_ == nullptr) {
        reply::error(out, kErrNoReplication);
        return;
    }
    int64_t numreplicas;
    int64_t timeout;
    if (!parse_int64(argv[1], numreplicas) || !parse_int64(argv[2], timeout)) {
        reply::error(out, kErrNotInteger);
        return;
    }
    if (timeout < 0) {
        reply::error(out, "ERR timeout is negative");
        return;
    }
    replication_->wait(*client_, numreplicas, timeout, out);
}

// INFO [section ...] — server status as "field:value" lines. Sections so
// far: replication, cluster.
void CommandDispatcher::cmd_info(const Args& argv, std::string& out) {
    bool all = argv.size() == 1;
    bool replication_wanted = false;
    bool cluster_wanted = false;
    for (size_t i = 1; i < argv.size(); ++i) {
        std::string section = to_upper(argv[i]);
        all = all || section == "ALL" || section == "DEFAULT" || section == "EVERYTHING";
        replication_wanted = replication_wanted || section == "REPLICATION";
        cluster_wanted = cluster_wanted || section == "CLUSTER";
    }
    std::string text;
    if ((all || replication_wanted) && replication_ != nullptr) {
        text += replication_->info();
    }
    if (all || cluster_wanted) {
        text += text.empty() ? "" : "\r\n";
        text += std::string("# Cluster\r\ncluster_enabled:") + (cluster_ != nullptr ? "1" : "0") + "\r\n";
    }
    reply::bulk_string(out, text);
}

// DUMP key — the value serialized for RESTORE (null if the key is missing).
void CommandDispatcher::cmd_dump(const Args& argv, std::string& out) {
    if (const std::string* value = store_.get(argv[1])) {
        reply::bulk_string(out, snapshot::dump_value(*value));
    } else {
        reply::null_bulk(out);
    }
}

// RESTORE key ttl-ms payload [REPLACE] [ABSTTL] — creates the key from a
// DUMP payload; ttl 0 means no expiry. RESTORE-ASKING is the same command,
// allowed into a slot being imported (it's what MIGRATE sends). Propagated
// as SET key value [PXAT unix-ms], so the AOF and replicas don't need to
// understand payloads.
void CommandDispatcher::cmd_restore(const Args& argv, std::string& out) {
    bool replace = false;
    bool absttl = false;
    for (size_t i = 4; i < argv.size(); ++i) {
        std::string opt = to_upper(argv[i]);
        if (opt == "REPLACE") {
            replace = true;
        } else if (opt == "ABSTTL") {
            absttl = true;
        } else {
            reply::error(out, kErrSyntax);
            return;
        }
    }
    int64_t ttl;
    if (!parse_int64(argv[2], ttl) || ttl < 0) {
        reply::error(out, "ERR Invalid TTL value, must be >= 0");
        return;
    }
    const std::string& key = argv[1];
    if (!replace && store_.exists(key)) {
        reply::error(out, "BUSYKEY Target key name already exists.");
        return;
    }
    std::string value;
    if (!snapshot::restore_value(argv[3], value)) {
        reply::error(out, "ERR DUMP payload version or checksum are wrong");
        return;
    }
    int64_t now = store_.now_ms();
    std::optional<int64_t> expire_at;
    if (ttl > 0) {
        expire_at = absttl ? ttl : deadline_from(now, ttl, 1).value_or(INT64_MAX);
    }
    if (expire_at && *expire_at <= now) {
        // Already expired: nothing to create, but REPLACE still removes
        // the old value, as it would have been overwritten.
        if (store_.del(key)) {
            propagate({"DEL", key});
        }
        reply::simple_string(out, "OK");
        return;
    }
    store_.set(key, value);
    if (expire_at) {
        store_.set_expire(key, *expire_at);
    }
    if (propagator_) {
        Args logged{"SET", key, std::move(value)};
        if (expire_at) {
            logged.insert(logged.end(), {"PXAT", std::to_string(*expire_at)});
        }
        propagate(logged);
    }
    reply::simple_string(out, "OK");
}

// MIGRATE host port key|"" destination-db timeout-ms [COPY] [REPLACE]
//         [KEYS key ...]
// Moves keys to another node: sends them as RESTORE-ASKING, then deletes
// the ones the target accepted (unless COPY). Replies OK, or NOKEY if none
// of the keys exist here. Blocks the server for the round trip (see
// migrate.hpp for why).
void CommandDispatcher::cmd_migrate(const Args& argv, std::string& out) {
    int64_t port;
    int64_t db;
    int64_t timeout;
    if (!parse_int64(argv[2], port) || port <= 0 || port > 65535 || !parse_int64(argv[4], db) ||
        !parse_int64(argv[5], timeout)) {
        reply::error(out, kErrNotInteger);
        return;
    }
    if (db != 0) {
        reply::error(out, "ERR DB index is out of range");
        return;
    }
    bool copy = false;
    bool replace = false;
    std::vector<std::string> keys;
    for (size_t i = 6; i < argv.size(); ++i) {
        std::string opt = to_upper(argv[i]);
        if (opt == "COPY") {
            copy = true;
        } else if (opt == "REPLACE") {
            replace = true;
        } else if (opt == "KEYS") {
            if (!argv[3].empty()) {
                reply::error(out, "ERR When using MIGRATE KEYS option, the key argument must be set to the empty string");
                return;
            }
            keys.assign(argv.begin() + static_cast<std::ptrdiff_t>(i) + 1, argv.end());
            break;
        } else {
            reply::error(out, kErrSyntax);
            return;
        }
    }
    if (keys.empty()) {
        keys.push_back(argv[3]);
    }

    // TTLs travel as time remaining, as in Redis: the two nodes' clocks
    // may disagree, and relative time survives that (give or take the
    // transfer time).
    std::string request;
    std::vector<std::string> sent;
    for (const std::string& key : keys) {
        const std::string* value = store_.get(key);
        if (value == nullptr) {
            continue;
        }
        int64_t pttl = store_.pttl(key);
        Args restore{"RESTORE-ASKING", key, std::to_string(pttl > 0 ? pttl : 0), snapshot::dump_value(*value)};
        if (replace) {
            restore.push_back("REPLACE");
        }
        reply::command(request, restore);
        sent.push_back(key);
    }
    if (sent.empty()) {
        reply::simple_string(out, "NOKEY");
        return;
    }

    std::vector<std::string> replies;
    std::string io_error;
    bool io_ok = migrate::exchange(argv[1], static_cast<uint16_t>(port), request, sent.size(),
                                   std::chrono::milliseconds(timeout > 0 ? timeout : 1000), replies, io_error);
    // Delete only what the target confirmed: on a timeout or an error
    // reply the rest stays here, so a key is never lost in transit (at
    // worst it exists on both nodes, and the retry uses REPLACE).
    std::string target_error;
    Args deleted{"DEL"};
    for (size_t i = 0; i < replies.size(); ++i) {
        if (!replies[i].empty() && replies[i][0] == '-') {
            if (target_error.empty()) {
                target_error = replies[i].substr(1);
            }
        } else if (!copy && store_.del(sent[i])) {
            deleted.push_back(sent[i]);
        }
    }
    if (deleted.size() > 1) {
        propagate(deleted);
    }
    if (!io_ok) {
        reply::error(out, io_error);
    } else if (!target_error.empty()) {
        reply::error(out, "ERR Target instance replied with error: " + sanitize_for_error(target_error));
    } else {
        reply::simple_string(out, "OK");
    }
}

bool CommandDispatcher::cluster_enabled(std::string& out) {
    if (cluster_ == nullptr) {
        reply::error(out, "ERR This instance has cluster support disabled");
        return false;
    }
    return true;
}

// CLUSTER <subcommand> ... — see Cluster::command().
void CommandDispatcher::cmd_cluster(const Args& argv, std::string& out) {
    if (cluster_enabled(out)) {
        cluster_->command(argv, out);
    }
}

// ASKING — the next command may use a slot this node is importing. Sent by
// a client that just got an ASK redirect.
void CommandDispatcher::cmd_asking(const Args&, std::string& out) {
    if (cluster_enabled(out)) {
        if (client_ != nullptr) {
            client_->asking = true;
        }
        reply::simple_string(out, "OK");
    }
}

// READONLY — this connection accepts reads from a replica, which may lag
// its master. READWRITE turns that off again.
void CommandDispatcher::cmd_readonly(const Args&, std::string& out) {
    if (cluster_enabled(out)) {
        if (client_ != nullptr) {
            client_->readonly = true;
        }
        reply::simple_string(out, "OK");
    }
}

void CommandDispatcher::cmd_readwrite(const Args&, std::string& out) {
    if (cluster_enabled(out)) {
        if (client_ != nullptr) {
            client_->readonly = false;
        }
        reply::simple_string(out, "OK");
    }
}
