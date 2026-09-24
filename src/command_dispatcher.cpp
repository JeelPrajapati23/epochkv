#include "command_dispatcher.hpp"

#include <cctype>
#include <charconv>
#include <optional>

#include "persistence.hpp"
#include "reply.hpp"

namespace {

constexpr const char* kErrNotInteger = "ERR value is not an integer or out of range";
constexpr const char* kErrSyntax = "ERR syntax error";
constexpr const char* kErrOom = "OOM command not allowed when used memory > 'maxmemory'.";
constexpr const char* kErrNoPersistence = "ERR persistence is not enabled on this server";

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
          {"PING", {"ping", &CommandDispatcher::cmd_ping, -1, 0}},
          {"ECHO", {"echo", &CommandDispatcher::cmd_echo, 2, 0}},
          {"SET", {"set", &CommandDispatcher::cmd_set, -3, kWrite | kDenyOom}},
          {"GET", {"get", &CommandDispatcher::cmd_get, 2, 0}},
          {"DEL", {"del", &CommandDispatcher::cmd_del, -2, kWrite}},
          {"EXISTS", {"exists", &CommandDispatcher::cmd_exists, -2, 0}},
          {"EXPIRE", {"expire", &CommandDispatcher::cmd_expire, 3, kWrite}},
          {"PEXPIRE", {"pexpire", &CommandDispatcher::cmd_pexpire, 3, kWrite}},
          {"EXPIREAT", {"expireat", &CommandDispatcher::cmd_expireat, 3, kWrite}},
          {"PEXPIREAT", {"pexpireat", &CommandDispatcher::cmd_pexpireat, 3, kWrite}},
          {"TTL", {"ttl", &CommandDispatcher::cmd_ttl, 2, 0}},
          {"PTTL", {"pttl", &CommandDispatcher::cmd_pttl, 2, 0}},
          {"PERSIST", {"persist", &CommandDispatcher::cmd_persist, 2, kWrite}},
          {"DBSIZE", {"dbsize", &CommandDispatcher::cmd_dbsize, 1, 0}},
          {"SAVE", {"save", &CommandDispatcher::cmd_save, 1, 0}},
          {"BGSAVE", {"bgsave", &CommandDispatcher::cmd_bgsave, 1, 0}},
          {"BGREWRITEAOF", {"bgrewriteaof", &CommandDispatcher::cmd_bgrewriteaof, 1, 0}},
          {"LASTSAVE", {"lastsave", &CommandDispatcher::cmd_lastsave, 1, 0}},
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

void CommandDispatcher::dispatch(const Args& argv, std::string& out) {
    if (argv.empty()) {
        return;
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

    // Both checks are skipped while replaying the AOF at startup: those
    // writes were already accepted once, and refusing them now would load a
    // different dataset than the one that was acknowledged.
    if ((spec.flags & kWrite) && persistence_ != nullptr && !loading()) {
        std::string err = persistence_->write_error();
        if (!err.empty()) {
            reply::error(out, err);
            return;
        }
    }

    // Evict *before* the write rather than after: the command then runs
    // against a store that's within budget, and may overshoot by at most one
    // value until the next write evicts again — Redis's semantics.
    if ((spec.flags & kDenyOom) && !loading() && !store_.evict_if_needed()) {
        reply::error(out, kErrOom);
        return;
    }

    (this->*spec.handler)(argv, out);
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
