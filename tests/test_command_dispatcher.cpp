#include <catch2/catch_test_macros.hpp>

#include <string>

#include "command_dispatcher.hpp"
#include "hash_table.hpp"

namespace {

// Runs one command against a fresh reply buffer and returns the raw reply.
std::string run(CommandDispatcher& dispatcher, const CommandDispatcher::Args& argv) {
    std::string out;
    dispatcher.dispatch(argv, out);
    return out;
}

}  // namespace

TEST_CASE("PING with and without a message", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"PING"}) == "+PONG\r\n");
    REQUIRE(run(d, {"PING", "hi"}) == "$2\r\nhi\r\n");
    REQUIRE(run(d, {"PING", "a", "b"}) == "-ERR wrong number of arguments for 'ping' command\r\n");
}

TEST_CASE("ECHO returns its argument as a bulk string", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"ECHO", "hello"}) == "$5\r\nhello\r\n");
}

TEST_CASE("SET then GET round-trips; GET on a missing key is null", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"SET", "foo", "bar"}) == "+OK\r\n");
    REQUIRE(run(d, {"GET", "foo"}) == "$3\r\nbar\r\n");
    REQUIRE(run(d, {"GET", "missing"}) == "$-1\r\n");
}

TEST_CASE("command names are case-insensitive", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"set", "k", "v"}) == "+OK\r\n");
    REQUIRE(run(d, {"GeT", "k"}) == "$1\r\nv\r\n");
}

TEST_CASE("keys stay case-sensitive", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    run(d, {"SET", "key", "v"});
    REQUIRE(run(d, {"GET", "KEY"}) == "$-1\r\n");
}

TEST_CASE("DEL counts only keys that existed", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    run(d, {"SET", "a", "1"});
    run(d, {"SET", "b", "2"});
    REQUIRE(run(d, {"DEL", "a", "b", "c"}) == ":2\r\n");
    REQUIRE(run(d, {"GET", "a"}) == "$-1\r\n");
}

TEST_CASE("EXISTS counts repeated keys each time", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    run(d, {"SET", "a", "1"});
    REQUIRE(run(d, {"EXISTS", "a", "a", "missing"}) == ":2\r\n");
}

TEST_CASE("wrong arity is rejected before the handler runs", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"GET"}) == "-ERR wrong number of arguments for 'get' command\r\n");
    REQUIRE(run(d, {"SET", "k"}) == "-ERR wrong number of arguments for 'set' command\r\n");
    REQUIRE(run(d, {"DEL"}) == "-ERR wrong number of arguments for 'del' command\r\n");
    REQUIRE(store.size() == 0);
}

TEST_CASE("unknown command is an error and strips CR/LF from the echoed name", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"NOPE"}) == "-ERR unknown command 'NOPE'\r\n");
    REQUIRE(run(d, {"X\r\n+OK"}) == "-ERR unknown command 'X  +OK'\r\n");
}

TEST_CASE("empty command produces no reply", "[dispatcher]") {
    HashTable store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {}).empty());
}
