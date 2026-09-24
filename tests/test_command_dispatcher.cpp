#include <catch2/catch_test_macros.hpp>

#include <string>
#include <vector>

#include "command_dispatcher.hpp"
#include "store.hpp"

namespace {

// Runs one command against a fresh reply buffer and returns the raw reply.
std::string run(CommandDispatcher& dispatcher, const CommandDispatcher::Args& argv) {
    std::string out;
    dispatcher.dispatch(argv, out);
    return out;
}

}  // namespace

TEST_CASE("PING with and without a message", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"PING"}) == "+PONG\r\n");
    REQUIRE(run(d, {"PING", "hi"}) == "$2\r\nhi\r\n");
    REQUIRE(run(d, {"PING", "a", "b"}) == "-ERR wrong number of arguments for 'ping' command\r\n");
}

TEST_CASE("ECHO returns its argument as a bulk string", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"ECHO", "hello"}) == "$5\r\nhello\r\n");
}

TEST_CASE("SET then GET round-trips; GET on a missing key is null", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"SET", "foo", "bar"}) == "+OK\r\n");
    REQUIRE(run(d, {"GET", "foo"}) == "$3\r\nbar\r\n");
    REQUIRE(run(d, {"GET", "missing"}) == "$-1\r\n");
}

TEST_CASE("command names are case-insensitive", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"set", "k", "v"}) == "+OK\r\n");
    REQUIRE(run(d, {"GeT", "k"}) == "$1\r\nv\r\n");
}

TEST_CASE("keys stay case-sensitive", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "key", "v"});
    REQUIRE(run(d, {"GET", "KEY"}) == "$-1\r\n");
}

TEST_CASE("DEL counts only keys that existed", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "a", "1"});
    run(d, {"SET", "b", "2"});
    REQUIRE(run(d, {"DEL", "a", "b", "c"}) == ":2\r\n");
    REQUIRE(run(d, {"GET", "a"}) == "$-1\r\n");
}

TEST_CASE("EXISTS counts repeated keys each time", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "a", "1"});
    REQUIRE(run(d, {"EXISTS", "a", "a", "missing"}) == ":2\r\n");
}

TEST_CASE("wrong arity is rejected before the handler runs", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"GET"}) == "-ERR wrong number of arguments for 'get' command\r\n");
    REQUIRE(run(d, {"SET", "k"}) == "-ERR wrong number of arguments for 'set' command\r\n");
    REQUIRE(run(d, {"DEL"}) == "-ERR wrong number of arguments for 'del' command\r\n");
    REQUIRE(store.size() == 0);
}

TEST_CASE("unknown command is an error and strips CR/LF from the echoed name", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"NOPE"}) == "-ERR unknown command 'NOPE'\r\n");
    REQUIRE(run(d, {"X\r\n+OK"}) == "-ERR unknown command 'X  +OK'\r\n");
}

TEST_CASE("empty command produces no reply", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {}).empty());
}

namespace {

struct ClockedStore {
    int64_t now = 1'000'000;
    Store store;

    explicit ClockedStore(size_t maxmemory = 0, EvictionPolicy policy = EvictionPolicy::kNoEviction)
        : store(options(this, maxmemory, policy)) {}

    static Store::Options options(ClockedStore* self, size_t maxmemory, EvictionPolicy policy) {
        Store::Options o;
        o.maxmemory = maxmemory;
        o.policy = policy;
        o.clock = [self] { return self->now; };
        return o;
    }
};

}  // namespace

TEST_CASE("SET EX / PX set a TTL that TTL and PTTL report", "[dispatcher][ttl]") {
    ClockedStore cs;
    CommandDispatcher d(cs.store);
    REQUIRE(run(d, {"SET", "a", "v", "EX", "10"}) == "+OK\r\n");
    REQUIRE(run(d, {"TTL", "a"}) == ":10\r\n");
    REQUIRE(run(d, {"SET", "b", "v", "px", "1500"}) == "+OK\r\n");
    REQUIRE(run(d, {"PTTL", "b"}) == ":1500\r\n");
    REQUIRE(run(d, {"TTL", "b"}) == ":2\r\n");  // rounded, like Redis

    cs.now += 1500;
    REQUIRE(run(d, {"GET", "b"}) == "$-1\r\n");
    REQUIRE(run(d, {"TTL", "b"}) == ":-2\r\n");
}

TEST_CASE("TTL is -1 for a key without expiry, -2 for a missing key", "[dispatcher][ttl]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "k", "v"});
    REQUIRE(run(d, {"TTL", "k"}) == ":-1\r\n");
    REQUIRE(run(d, {"PTTL", "missing"}) == ":-2\r\n");
}

TEST_CASE("SET rejects bad expire values and malformed options", "[dispatcher][ttl]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"SET", "k", "v", "EX", "0"}) == "-ERR invalid expire time in 'set' command\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX", "-5"}) == "-ERR invalid expire time in 'set' command\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX", "9223372036854775807"}) ==
            "-ERR invalid expire time in 'set' command\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX", "12abc"}) == "-ERR value is not an integer or out of range\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX"}) == "-ERR syntax error\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX", "1", "PX", "1"}) == "-ERR syntax error\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "NX", "XX"}) == "-ERR syntax error\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "EX", "1", "KEEPTTL"}) == "-ERR syntax error\r\n");
    REQUIRE(run(d, {"SET", "k", "v", "BOGUS"}) == "-ERR syntax error\r\n");
    REQUIRE(store.size() == 0);
}

TEST_CASE("SET NX only creates, SET XX only overwrites", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"SET", "k", "v1", "XX"}) == "$-1\r\n");
    REQUIRE(run(d, {"SET", "k", "v1", "NX"}) == "+OK\r\n");
    REQUIRE(run(d, {"SET", "k", "v2", "NX"}) == "$-1\r\n");
    REQUIRE(run(d, {"SET", "k", "v3", "XX"}) == "+OK\r\n");
    REQUIRE(run(d, {"GET", "k"}) == "$2\r\nv3\r\n");
}

TEST_CASE("plain SET clears a TTL; SET KEEPTTL keeps it", "[dispatcher][ttl]") {
    ClockedStore cs;
    CommandDispatcher d(cs.store);
    run(d, {"SET", "k", "v", "EX", "100"});
    run(d, {"SET", "k", "v2", "KEEPTTL"});
    REQUIRE(run(d, {"TTL", "k"}) == ":100\r\n");
    run(d, {"SET", "k", "v3"});
    REQUIRE(run(d, {"TTL", "k"}) == ":-1\r\n");
}

TEST_CASE("EXPIRE / PEXPIRE / PERSIST", "[dispatcher][ttl]") {
    ClockedStore cs;
    CommandDispatcher d(cs.store);
    REQUIRE(run(d, {"EXPIRE", "missing", "10"}) == ":0\r\n");
    run(d, {"SET", "k", "v"});
    REQUIRE(run(d, {"EXPIRE", "k", "10"}) == ":1\r\n");
    REQUIRE(run(d, {"PEXPIRE", "k", "2500"}) == ":1\r\n");
    REQUIRE(run(d, {"PTTL", "k"}) == ":2500\r\n");
    REQUIRE(run(d, {"PERSIST", "k"}) == ":1\r\n");
    REQUIRE(run(d, {"PERSIST", "k"}) == ":0\r\n");
    REQUIRE(run(d, {"TTL", "k"}) == ":-1\r\n");
    REQUIRE(run(d, {"EXPIRE", "k", "abc"}) == "-ERR value is not an integer or out of range\r\n");
    REQUIRE(run(d, {"EXPIRE", "k", "9223372036854775807"}) == "-ERR invalid expire time in 'expire' command\r\n");
}

TEST_CASE("EXPIRE with a non-positive TTL deletes the key", "[dispatcher][ttl]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "k", "v"});
    REQUIRE(run(d, {"EXPIRE", "k", "-1"}) == ":1\r\n");
    REQUIRE(run(d, {"EXISTS", "k"}) == ":0\r\n");
}

TEST_CASE("DBSIZE counts keys", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"DBSIZE"}) == ":0\r\n");
    run(d, {"SET", "a", "1"});
    run(d, {"SET", "b", "2"});
    REQUIRE(run(d, {"DBSIZE"}) == ":2\r\n");
}

TEST_CASE("writes over maxmemory with noeviction are refused with -OOM; reads still work", "[dispatcher][lru]") {
    ClockedStore cs(/*maxmemory=*/200, EvictionPolicy::kNoEviction);
    CommandDispatcher d(cs.store);
    REQUIRE(run(d, {"SET", "a", std::string(300, 'x')}) == "+OK\r\n");  // was under the limit before
    REQUIRE(run(d, {"SET", "b", "v"}) == "-OOM command not allowed when used memory > 'maxmemory'.\r\n");
    REQUIRE(run(d, {"GET", "a"}).size() > 300);
    REQUIRE(run(d, {"DEL", "a"}) == ":1\r\n");  // deletes are allowed, and free memory
    REQUIRE(run(d, {"SET", "b", "v"}) == "+OK\r\n");
}

TEST_CASE("writes over maxmemory with allkeys-lru evict the oldest keys", "[dispatcher][lru]") {
    ClockedStore cs(/*maxmemory=*/1000, EvictionPolicy::kAllKeysLru);
    CommandDispatcher d(cs.store);
    for (int i = 0; i < 50; ++i) {
        REQUIRE(run(d, {"SET", "k" + std::to_string(i), "value"}) == "+OK\r\n");
    }
    REQUIRE(cs.store.evicted_keys() > 0);
    REQUIRE(run(d, {"GET", "k0"}) == "$-1\r\n");
    REQUIRE(run(d, {"GET", "k49"}) == "$5\r\nvalue\r\n");
}

namespace {

// Collects what the dispatcher propagates, one space-joined command per entry.
struct PropagationLog {
    std::vector<std::string> commands;

    void attach(CommandDispatcher& d) {
        d.set_propagator([this](const CommandDispatcher::Args& argv) {
            std::string joined;
            for (const std::string& arg : argv) {
                joined += (joined.empty() ? "" : " ") + arg;
            }
            commands.push_back(joined);
        });
    }
};

}  // namespace

TEST_CASE("SET is propagated with its TTL as an absolute PXAT, without NX/XX", "[dispatcher][propagation]") {
    ClockedStore cs;
    CommandDispatcher d(cs.store);
    PropagationLog log;
    log.attach(d);

    run(d, {"SET", "a", "1"});
    run(d, {"SET", "b", "2", "EX", "10"});
    run(d, {"SET", "c", "3", "PX", "1500", "NX"});
    run(d, {"SET", "c", "4", "NX"});  // condition failed: nothing changed, nothing logged
    run(d, {"SET", "b", "5", "XX", "KEEPTTL"});
    run(d, {"SET", "d", "6", "PXAT", "1005000"});

    REQUIRE(log.commands == std::vector<std::string>{
                                "SET a 1",
                                "SET b 2 PXAT 1010000",
                                "SET c 3 PXAT 1001500",
                                "SET b 5 KEEPTTL",
                                "SET d 6 PXAT 1005000",
                            });
}

TEST_CASE("EXPIRE-family commands are propagated as PEXPIREAT, or DEL if they delete", "[dispatcher][propagation]") {
    ClockedStore cs;
    CommandDispatcher d(cs.store);
    run(d, {"SET", "k", "v"});
    run(d, {"SET", "j", "v"});
    PropagationLog log;
    log.attach(d);

    REQUIRE(run(d, {"EXPIRE", "k", "10"}) == ":1\r\n");
    REQUIRE(run(d, {"PEXPIRE", "k", "20"}) == ":1\r\n");
    REQUIRE(run(d, {"EXPIREAT", "k", "2000"}) == ":1\r\n");
    REQUIRE(run(d, {"PEXPIREAT", "k", "3000000"}) == ":1\r\n");
    REQUIRE(run(d, {"EXPIRE", "missing", "10"}) == ":0\r\n");
    REQUIRE(run(d, {"PERSIST", "k"}) == ":1\r\n");
    REQUIRE(run(d, {"PERSIST", "k"}) == ":0\r\n");
    REQUIRE(run(d, {"EXPIRE", "j", "0"}) == ":1\r\n");

    REQUIRE(log.commands == std::vector<std::string>{
                                "PEXPIREAT k 1010000",
                                "PEXPIREAT k 1000020",
                                "PEXPIREAT k 2000000",
                                "PEXPIREAT k 3000000",
                                "PERSIST k",
                                "DEL j",
                            });
}

TEST_CASE("DEL is propagated only if it removed something; reads never are", "[dispatcher][propagation]") {
    Store store;
    CommandDispatcher d(store);
    run(d, {"SET", "k", "v"});
    PropagationLog log;
    log.attach(d);

    run(d, {"GET", "k"});
    run(d, {"EXISTS", "k"});
    run(d, {"TTL", "k"});
    run(d, {"DEL", "missing"});
    run(d, {"DEL", "k", "missing"});
    REQUIRE(log.commands == std::vector<std::string>{"DEL k missing"});
}

TEST_CASE("EXPIREAT and SET EXAT use absolute times", "[dispatcher][ttl]") {
    ClockedStore cs;  // now = 1,000,000 ms = 1000 s
    CommandDispatcher d(cs.store);
    run(d, {"SET", "a", "v", "EXAT", "1010"});
    REQUIRE(run(d, {"TTL", "a"}) == ":10\r\n");
    run(d, {"SET", "b", "v"});
    REQUIRE(run(d, {"PEXPIREAT", "b", "1002500"}) == ":1\r\n");
    REQUIRE(run(d, {"PTTL", "b"}) == ":2500\r\n");
    REQUIRE(run(d, {"EXPIREAT", "b", "999"}) == ":1\r\n");  // in the past: deleted
    REQUIRE(run(d, {"EXISTS", "b"}) == ":0\r\n");
}

TEST_CASE("persistence commands need a persistence layer", "[dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    REQUIRE(run(d, {"BGSAVE"}) == "-ERR persistence is not enabled on this server\r\n");
    REQUIRE(run(d, {"LASTSAVE"}) == "-ERR persistence is not enabled on this server\r\n");
}
