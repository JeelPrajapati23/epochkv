#pragma once

#include <string>
#include <unordered_map>
#include <vector>

#include "hash_table.hpp"

// Routes a parsed command (argv[0] is the command name) to its handler and
// appends the RESP-encoded reply to an output buffer. Knows nothing about
// sockets — the server loop owns I/O, this owns command semantics.
class CommandDispatcher {
public:
    using Args = std::vector<std::string>;

    explicit CommandDispatcher(HashTable& store);

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
    };

    static bool arity_ok(int arity, size_t argc);

    void cmd_ping(const Args& argv, std::string& out);
    void cmd_echo(const Args& argv, std::string& out);
    void cmd_set(const Args& argv, std::string& out);
    void cmd_get(const Args& argv, std::string& out);
    void cmd_del(const Args& argv, std::string& out);
    void cmd_exists(const Args& argv, std::string& out);

    HashTable& store_;
    // Keyed by uppercase command name; built once, looked up per command.
    std::unordered_map<std::string, CommandSpec> table_;
};
