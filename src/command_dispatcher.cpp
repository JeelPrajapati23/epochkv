#include "command_dispatcher.hpp"

#include <cctype>
#include <charconv>
#include <optional>

#include "reply.hpp"

namespace {

constexpr const char* kErrNotInteger = "ERR value is not an integer or out of range";
constexpr const char* kErrSyntax = "ERR syntax error";
constexpr const char* kErrOom = "OOM command not allowed when used memory > 'maxmemory'.";

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
          {"PING", {"ping", &CommandDispatcher::cmd_ping, -1, false}},
          {"ECHO", {"echo", &CommandDispatcher::cmd_echo, 2, false}},
          {"SET", {"set", &CommandDispatcher::cmd_set, -3, true}},
          {"GET", {"get", &CommandDispatcher::cmd_get, 2, false}},
          {"DEL", {"del", &CommandDispatcher::cmd_del, -2, false}},
          {"EXISTS", {"exists", &CommandDispatcher::cmd_exists, -2, false}},
          {"EXPIRE", {"expire", &CommandDispatcher::cmd_expire, 3, false}},
          {"PEXPIRE", {"pexpire", &CommandDispatcher::cmd_pexpire, 3, false}},
          {"TTL", {"ttl", &CommandDispatcher::cmd_ttl, 2, false}},
          {"PTTL", {"pttl", &CommandDispatcher::cmd_pttl, 2, false}},
          {"PERSIST", {"persist", &CommandDispatcher::cmd_persist, 2, false}},
          {"DBSIZE", {"dbsize", &CommandDispatcher::cmd_dbsize, 1, false}},
      } {}

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

    // Evict *before* the write rather than after: the command then runs
    // against a store that's within budget, and may overshoot by at most one
    // value until the next write evicts again — Redis's semantics.
    if (spec.deny_oom && !store_.evict_if_needed()) {
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

// SET key value [NX | XX] [EX seconds | PX milliseconds | KEEPTTL]
// Replies OK, or a null bulk if an NX/XX condition prevented the write.
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
        } else if ((opt == "EX" || opt == "PX") && !keep_ttl && !expire_at && i + 1 < argv.size()) {
            int64_t amount;
            if (!parse_int64(argv[++i], amount)) {
                reply::error(out, kErrNotInteger);
                return;
            }
            int64_t unit_ms = opt == "EX" ? 1000 : 1;
            expire_at = amount > 0 ? deadline_from(store_.now_ms(), amount, unit_ms) : std::nullopt;
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

// EXPIRE key seconds / PEXPIRE key milliseconds — replies 1 if the TTL was
// set, 0 if the key doesn't exist. A non-positive TTL deletes the key.
void CommandDispatcher::expire_generic(const Args& argv, int64_t unit_ms, std::string& out) {
    int64_t amount;
    if (!parse_int64(argv[2], amount)) {
        reply::error(out, kErrNotInteger);
        return;
    }
    std::optional<int64_t> when = deadline_from(store_.now_ms(), amount, unit_ms);
    if (!when) {
        reply::error(out, std::string("ERR invalid expire time in '") + (unit_ms == 1 ? "pexpire" : "expire") +
                              "' command");
        return;
    }
    reply::integer(out, store_.set_expire(argv[1], *when) ? 1 : 0);
}

void CommandDispatcher::cmd_expire(const Args& argv, std::string& out) {
    expire_generic(argv, 1000, out);
}

void CommandDispatcher::cmd_pexpire(const Args& argv, std::string& out) {
    expire_generic(argv, 1, out);
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
    reply::integer(out, store_.persist(argv[1]) ? 1 : 0);
}

// DBSIZE — number of keys, including expired ones not yet reclaimed.
void CommandDispatcher::cmd_dbsize(const Args&, std::string& out) {
    reply::integer(out, static_cast<int64_t>(store_.size()));
}
