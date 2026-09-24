#pragma once

#include <functional>
#include <string>
#include <unordered_map>
#include <vector>

#include "store.hpp"

class Persistence;

// Routes a parsed command (argv[0] is the command name) to its handler and
// appends the RESP-encoded reply to an output buffer. Knows nothing about
// sockets — the server loop owns I/O, this owns command semantics.
class CommandDispatcher {
public:
    using Args = std::vector<std::string>;
    // Receives every change to the dataset as a deterministic command that
    // reproduces it (relative TTLs become absolute timestamps, conditions
    // like NX are dropped once they've held). Feeds the AOF.
    using Propagator = std::function<void(const Args&)>;

    explicit CommandDispatcher(Store& store);

    void set_propagator(Propagator propagator) { propagator_ = std::move(propagator); }
    // Enables SAVE/BGSAVE/BGREWRITEAOF/LASTSAVE, and refusing writes while
    // they can't be persisted. Optional: without it those commands error.
    void set_persistence(Persistence* persistence) { persistence_ = persistence; }

    // Executes `argv` and appends exactly one reply to `out`. An empty argv
    // produces no reply (Redis silently ignores an empty array, *0\r\n).
    void dispatch(const Args& argv, std::string& out);

private:
    using Handler = void (CommandDispatcher::*)(const Args&, std::string&);

    enum Flags : unsigned {
        kWrite = 1u << 0,  // may modify the dataset: refused while it can't be persisted
        // May grow memory: run eviction first, and refuse with -OOM if the
        // store is still over maxmemory (Redis's "denyoom" command flag).
        kDenyOom = 1u << 1,
    };

    struct CommandSpec {
        const char* name;  // lowercase, as it appears in error messages
        Handler handler;
        // Redis convention: N > 0 means exactly N args (including the command
        // name); N < 0 means at least |N| args.
        int arity;
        unsigned flags;
    };

    static bool arity_ok(int arity, size_t argc);
    bool loading() const;
    void propagate(const Args& argv);

    void cmd_ping(const Args& argv, std::string& out);
    void cmd_echo(const Args& argv, std::string& out);
    void cmd_set(const Args& argv, std::string& out);
    void cmd_get(const Args& argv, std::string& out);
    void cmd_del(const Args& argv, std::string& out);
    void cmd_exists(const Args& argv, std::string& out);
    void cmd_expire(const Args& argv, std::string& out);
    void cmd_pexpire(const Args& argv, std::string& out);
    void cmd_expireat(const Args& argv, std::string& out);
    void cmd_pexpireat(const Args& argv, std::string& out);
    void cmd_ttl(const Args& argv, std::string& out);
    void cmd_pttl(const Args& argv, std::string& out);
    void cmd_persist(const Args& argv, std::string& out);
    void cmd_dbsize(const Args& argv, std::string& out);
    void cmd_save(const Args& argv, std::string& out);
    void cmd_bgsave(const Args& argv, std::string& out);
    void cmd_bgrewriteaof(const Args& argv, std::string& out);
    void cmd_lastsave(const Args& argv, std::string& out);

    // relative: the argument is a TTL (EXPIRE) rather than a timestamp (EXPIREAT).
    void expire_generic(const Args& argv, int64_t unit_ms, bool relative, const char* name, std::string& out);

    Store& store_;
    Propagator propagator_;
    Persistence* persistence_ = nullptr;
    // Keyed by uppercase command name; built once, looked up per command.
    std::unordered_map<std::string, CommandSpec> table_;
};
