#include "command_dispatcher.hpp"

#include <cctype>

#include "reply.hpp"

namespace {

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

}  // namespace

CommandDispatcher::CommandDispatcher(HashTable& store)
    : store_(store),
      table_{
          {"PING", {"ping", &CommandDispatcher::cmd_ping, -1}},
          {"ECHO", {"echo", &CommandDispatcher::cmd_echo, 2}},
          {"SET", {"set", &CommandDispatcher::cmd_set, 3}},
          {"GET", {"get", &CommandDispatcher::cmd_get, 2}},
          {"DEL", {"del", &CommandDispatcher::cmd_del, -2}},
          {"EXISTS", {"exists", &CommandDispatcher::cmd_exists, -2}},
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

// SET key value
void CommandDispatcher::cmd_set(const Args& argv, std::string& out) {
    store_.set(argv[1], argv[2]);
    reply::simple_string(out, "OK");
}

// GET key
void CommandDispatcher::cmd_get(const Args& argv, std::string& out) {
    std::optional<std::string> value = store_.get(argv[1]);
    if (value) {
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
        if (store_.contains(argv[i])) {
            ++found;
        }
    }
    reply::integer(out, found);
}
