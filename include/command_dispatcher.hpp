#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "store.hpp"

// Routes a parsed command (argv[0] is the command name) to its handler and
// appends the RESP-encoded reply to an output buffer. Knows nothing about
// sockets — the server loop owns I/O, this owns command semantics.
class CommandDispatcher {
public:
    using Args = std::vector<std::string>;

    explicit CommandDispatcher(Store& store);

    // Executes `argv` and appends exactly one reply to `out`. An empty argv
    // produces no reply (Redis silently ignores an empty array, *0\r\n).
    void dispatch(const Args& argv, std::string& out);

private:
    using Handler = void (CommandDispatcher::*)(const Args&, std::string&);

    struct CommandSpec {
        const char* name;  // lowercase, as it appears in error messages
        Handler handler;
        // Redis convention: N > 0 means exactly N args (including the command
        // name); N < 0 means at least |N| args.
        int arity;
        // May grow memory: run eviction first, and refuse with -OOM if the
        // store is still over maxmemory (Redis's "denyoom" command flag).
        bool deny_oom;
    };

    static bool arity_ok(int arity, size_t argc);

    void cmd_ping(const Args& argv, std::string& out);
    void cmd_echo(const Args& argv, std::string& out);
    void cmd_set(const Args& argv, std::string& out);
    void cmd_get(const Args& argv, std::string& out);
    void cmd_del(const Args& argv, std::string& out);
    void cmd_exists(const Args& argv, std::string& out);
    void cmd_expire(const Args& argv, std::string& out);
    void cmd_pexpire(const Args& argv, std::string& out);
    void cmd_ttl(const Args& argv, std::string& out);
    void cmd_pttl(const Args& argv, std::string& out);
    void cmd_persist(const Args& argv, std::string& out);
    void cmd_dbsize(const Args& argv, std::string& out);

    void expire_generic(const Args& argv, int64_t unit_ms, std::string& out);

    Store& store_;
    // Keyed by uppercase command name; built once, looked up per command.
    std::unordered_map<std::string, CommandSpec> table_;
};
