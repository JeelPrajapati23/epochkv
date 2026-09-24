#include <catch2/catch_test_macros.hpp>

#include <fstream>
#include <memory>
#include <sstream>
#include <string>
#include <vector>

#include "client.hpp"
#include "cluster.hpp"
#include "cluster_msg.hpp"
#include "cluster_slot.hpp"
#include "command_dispatcher.hpp"
#include "persistence.hpp"
#include "replication.hpp"
#include "snapshot.hpp"
#include "store.hpp"
#include "temp_dir.hpp"

namespace {

const std::string kIdA(40, 'a');
const std::string kIdB(40, 'b');

// "bar" is in slot 5061 (owned by A below), "foo" in 12182 (owned by B).
constexpr int kSlotBar = 5061;
constexpr int kSlotFoo = 12182;

// A node (A, us) in a two-master cluster whose view comes from a nodes.conf
// written by the test. No event loop: routing and CLUSTER commands work
// without the bus.
struct Fixture {
    TempDir dir;
    Store store;
    std::unique_ptr<Persistence> persistence;
    std::unique_ptr<Replication> replication;
    std::unique_ptr<Cluster> cluster;
    std::unique_ptr<CommandDispatcher> dispatcher;
    Client client{1, -1};
    std::vector<CommandDispatcher::Args> propagated;

    explicit Fixture(const std::string& config =
                         kIdA + " 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-8191\n" + kIdB +
                         " 127.0.0.1:7001@17001 master - 0 0 2 connected 8192-16383\n"
                         "vars currentEpoch 2 lastVoteEpoch 0\n") {
        if (!config.empty()) {
            std::ofstream(dir.file("nodes.conf")) << config;
        }
        Persistence::Options popts;
        popts.dir = dir.path;
        persistence = std::make_unique<Persistence>(store, popts);
        replication = std::make_unique<Replication>(store, *persistence, Replication::Options{});
        cluster = make_cluster();
        std::string error;
        REQUIRE(cluster->load_config(error));
        dispatcher = std::make_unique<CommandDispatcher>(store);
        dispatcher->set_replication(replication.get());
        dispatcher->set_cluster(cluster.get());
        dispatcher->set_propagator([this](const CommandDispatcher::Args& argv) { propagated.push_back(argv); });
    }

    std::unique_ptr<Cluster> make_cluster() {
        Cluster::Options copts;
        copts.config_file = dir.file("nodes.conf");
        copts.port = 7000;
        copts.cport = 17000;
        return std::make_unique<Cluster>(store, *replication, copts);
    }

    std::string run(const CommandDispatcher::Args& argv) {
        std::string out;
        dispatcher->dispatch(argv, out, &client);
        cluster->before_sleep();  // saves the config, as the event loop would
        return out;
    }

    std::string nodes() {
        std::string out = run({"CLUSTER", "NODES"});
        return out.substr(out.find("\r\n") + 2);
    }
};

}  // namespace

// --- key -> slot ------------------------------------------------------------

TEST_CASE("CRC16 and key slots match Redis Cluster", "[cluster]") {
    REQUIRE(cluster::crc16("123456789", 9) == 0x31C3);  // the XMODEM check value
    REQUIRE(cluster::key_hash_slot("foo") == kSlotFoo);
    REQUIRE(cluster::key_hash_slot("bar") == kSlotBar);
    REQUIRE(cluster::key_hash_slot("") == 0);
}

TEST_CASE("hash tags: only the first non-empty {...} is hashed", "[cluster]") {
    using cluster::key_hash_slot;
    REQUIRE(key_hash_slot("{user1000}.following") == key_hash_slot("{user1000}.followers"));
    REQUIRE(key_hash_slot("{user1000}.following") == key_hash_slot("user1000"));
    REQUIRE(key_hash_slot("foo{bar}{zap}") == key_hash_slot("bar"));
    REQUIRE(key_hash_slot("foo{{bar}}zap") == key_hash_slot("{bar"));
    // An empty first tag means the whole key is hashed.
    REQUIRE(key_hash_slot("foo{}{bar}") == static_cast<int>(cluster::crc16("foo{}{bar}", 10) & 16383));
    REQUIRE(key_hash_slot("{bar") == static_cast<int>(cluster::crc16("{bar", 4) & 16383));  // no closing brace
}

// --- bus messages -------------------------------------------------------------

TEST_CASE("bus messages round-trip through RESP", "[cluster]") {
    cluster::Message ping;
    ping.type = cluster::MsgType::kMeet;
    ping.sender = kIdA;
    ping.port = 7000;
    ping.cport = 17000;
    ping.current_epoch = 9;
    ping.config_epoch = 4;
    ping.slots.set(0);
    ping.slots.set(16383);
    ping.gossip.push_back({kIdB, "10.0.0.2", 7001, 17001, true});

    std::string wire;
    cluster::encode(ping, wire);
    RespParser parser;
    parser.feed(wire.data(), wire.size());
    auto parsed = parser.try_parse_command();
    REQUIRE(parsed.status == RespParser::Status::kComplete);
    cluster::Message got;
    REQUIRE(cluster::decode(parsed.command, got));
    REQUIRE(got.type == cluster::MsgType::kMeet);
    REQUIRE(got.sender == kIdA);
    REQUIRE(got.port == 7000);
    REQUIRE(got.cport == 17000);
    REQUIRE_FALSE(got.replica);
    REQUIRE(got.current_epoch == 9);
    REQUIRE(got.config_epoch == 4);
    REQUIRE(got.slots == ping.slots);
    REQUIRE(got.gossip.size() == 1);
    REQUIRE(got.gossip[0].id == kIdB);
    REQUIRE(got.gossip[0].ip == "10.0.0.2");
    REQUIRE(got.gossip[0].replica);

    cluster::Message update;
    update.type = cluster::MsgType::kUpdate;
    update.sender = kIdA;
    update.port = 1;
    update.cport = 2;
    update.replica = true;
    update.master_id = kIdB;
    update.update_id = kIdB;
    update.update_epoch = 7;
    update.update_slots.set(100);
    wire.clear();
    cluster::encode(update, wire);
    parser.feed(wire.data(), wire.size());
    parsed = parser.try_parse_command();
    REQUIRE(cluster::decode(parsed.command, got));
    REQUIRE(got.type == cluster::MsgType::kUpdate);
    REQUIRE(got.master_id == kIdB);
    REQUIRE(got.update_id == kIdB);
    REQUIRE(got.update_epoch == 7);
    REQUIRE(got.update_slots == update.update_slots);
}

TEST_CASE("malformed bus messages are rejected", "[cluster]") {
    cluster::Message ping;
    ping.sender = kIdA;
    ping.port = 7000;
    ping.cport = 17000;
    std::string wire;
    cluster::encode(ping, wire);
    RespParser parser;
    parser.feed(wire.data(), wire.size());
    std::vector<std::string> good = parser.try_parse_command().command;
    cluster::Message out;
    REQUIRE(cluster::decode(good, out));

    auto bad = good;
    bad[1] = "not-a-node-id";
    REQUIRE_FALSE(cluster::decode(bad, out));
    bad = good;
    bad[8] = "-1";  // replication offset
    REQUIRE_FALSE(cluster::decode(bad, out));
    bad = good;
    bad[10] = "short bitmap";
    REQUIRE_FALSE(cluster::decode(bad, out));
    bad = good;
    bad[11] = "1";  // claims a gossip entry that isn't there
    REQUIRE_FALSE(cluster::decode(bad, out));
    bad = good;
    bad[11] = "18446744073709551615";  // huge count must not overflow the size check
    REQUIRE_FALSE(cluster::decode(bad, out));
    bad = good;
    bad[0] = "HELLO";
    REQUIRE_FALSE(cluster::decode(bad, out));
    REQUIRE_FALSE(cluster::decode({"PING"}, out));

    // A gossip entry with an unknown health flag.
    ping.gossip.push_back({kIdB, "10.0.0.2", 7001, 17001, false, true, false});
    wire.clear();
    cluster::encode(ping, wire);
    parser.feed(wire.data(), wire.size());
    bad = parser.try_parse_command().command;
    REQUIRE(cluster::decode(bad, out));
    REQUIRE(bad.back() == "master,fail?");
    bad.back() = "master,sick";
    REQUIRE_FALSE(cluster::decode(bad, out));

    // Header-only messages carry nothing else; FAIL carries exactly a node ID.
    cluster::Message ack = ping;
    ack.type = cluster::MsgType::kFailoverAuthAck;
    wire.clear();
    cluster::encode(ack, wire);
    parser.feed(wire.data(), wire.size());
    bad = parser.try_parse_command().command;
    REQUIRE(cluster::decode(bad, out));
    bad.push_back("extra");
    REQUIRE_FALSE(cluster::decode(bad, out));
    cluster::Message fail = ping;
    fail.type = cluster::MsgType::kFail;
    fail.fail_id = "not-a-node-id";
    wire.clear();
    cluster::encode(fail, wire);
    parser.feed(wire.data(), wire.size());
    REQUIRE_FALSE(cluster::decode(parser.try_parse_command().command, out));
}

TEST_CASE("failover messages round-trip through RESP", "[cluster][failover]") {
    auto round_trip = [](const cluster::Message& m) {
        std::string wire;
        cluster::encode(m, wire);
        RespParser parser;
        parser.feed(wire.data(), wire.size());
        auto parsed = parser.try_parse_command();
        REQUIRE(parsed.status == RespParser::Status::kComplete);
        cluster::Message got;
        REQUIRE(cluster::decode(parsed.command, got));
        return got;
    };

    cluster::Message req;
    req.type = cluster::MsgType::kFailoverAuthRequest;
    req.sender = kIdB;
    req.port = 7001;
    req.cport = 17001;
    req.replica = true;
    req.master_id = kIdA;
    req.current_epoch = 12;
    req.config_epoch = 3;
    req.repl_offset = 123456789012ULL;
    req.mflags = cluster::kMsgForceAck;
    req.slots.set(0);
    req.slots.set(16383);
    cluster::Message got = round_trip(req);
    REQUIRE(got.type == cluster::MsgType::kFailoverAuthRequest);
    REQUIRE(got.replica);
    REQUIRE(got.master_id == kIdA);
    REQUIRE(got.current_epoch == 12);
    REQUIRE(got.config_epoch == 3);
    REQUIRE(got.repl_offset == 123456789012ULL);
    REQUIRE(got.mflags == cluster::kMsgForceAck);
    REQUIRE(got.slots == req.slots);

    for (auto type : {cluster::MsgType::kFailoverAuthAck, cluster::MsgType::kMfStart}) {
        cluster::Message m = req;
        m.type = type;
        REQUIRE(round_trip(m).type == type);
    }

    cluster::Message fail = req;
    fail.type = cluster::MsgType::kFail;
    fail.fail_id = kIdA;
    got = round_trip(fail);
    REQUIRE(got.type == cluster::MsgType::kFail);
    REQUIRE(got.fail_id == kIdA);

    // A PING from a paused master, gossiping one suspect and one failed node.
    cluster::Message ping;
    ping.sender = kIdA;
    ping.port = 7000;
    ping.cport = 17000;
    ping.mflags = cluster::kMsgPaused;
    ping.gossip.push_back({kIdB, "10.0.0.2", 7001, 17001, false, true, false});
    ping.gossip.push_back({std::string(40, 'c'), "10.0.0.3", 7002, 17002, true, false, true});
    got = round_trip(ping);
    REQUIRE(got.mflags == cluster::kMsgPaused);
    REQUIRE(got.gossip.size() == 2);
    REQUIRE(got.gossip[0].pfail);
    REQUIRE_FALSE(got.gossip[0].fail);
    REQUIRE_FALSE(got.gossip[0].replica);
    REQUIRE(got.gossip[1].fail);
    REQUIRE(got.gossip[1].replica);
}

// --- DUMP payloads ---------------------------------------------------------------

TEST_CASE("DUMP payloads round-trip and detect corruption", "[cluster][snapshot]") {
    for (const std::string& value : {std::string(), std::string("hello"), std::string(1000, '\0')}) {
        std::string payload = snapshot::dump_value(value);
        std::string back;
        REQUIRE(snapshot::restore_value(payload, back));
        REQUIRE(back == value);
    }
    std::string payload = snapshot::dump_value("hello");
    std::string ignored;
    std::string flipped = payload;
    flipped[2] ^= 1;
    REQUIRE_FALSE(snapshot::restore_value(flipped, ignored));
    REQUIRE_FALSE(snapshot::restore_value(payload.substr(0, payload.size() - 1), ignored));
    REQUIRE_FALSE(snapshot::restore_value("", ignored));
}

TEST_CASE("DUMP / RESTORE commands", "[cluster][dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    std::vector<CommandDispatcher::Args> propagated;
    d.set_propagator([&propagated](const CommandDispatcher::Args& argv) { propagated.push_back(argv); });
    auto run = [&d](const CommandDispatcher::Args& argv) {
        std::string out;
        d.dispatch(argv, out);
        return out;
    };

    REQUIRE(run({"DUMP", "missing"}) == "$-1\r\n");
    run({"SET", "k", "v"});
    std::string payload = snapshot::dump_value("v");
    REQUIRE(run({"DUMP", "k"}) == "$" + std::to_string(payload.size()) + "\r\n" + payload + "\r\n");

    REQUIRE(run({"RESTORE", "k", "0", payload}) == "-BUSYKEY Target key name already exists.\r\n");
    REQUIRE(run({"RESTORE", "k2", "-1", payload}) == "-ERR Invalid TTL value, must be >= 0\r\n");
    REQUIRE(run({"RESTORE", "k2", "0", "garbage"}) == "-ERR DUMP payload version or checksum are wrong\r\n");
    REQUIRE(run({"RESTORE", "k2", "0", payload, "BOGUS"}) == "-ERR syntax error\r\n");

    propagated.clear();
    REQUIRE(run({"RESTORE", "k2", "5000", snapshot::dump_value("new")}) == "+OK\r\n");
    REQUIRE(run({"GET", "k2"}) == "$3\r\nnew\r\n");
    REQUIRE(store.pttl("k2") > 4000);
    REQUIRE(propagated.size() == 1);
    REQUIRE(propagated[0].size() == 5);  // SET k2 new PXAT <ms>
    REQUIRE(propagated[0][3] == "PXAT");

    REQUIRE(run({"RESTORE", "k", "0", snapshot::dump_value("replaced"), "REPLACE"}) == "+OK\r\n");
    REQUIRE(run({"GET", "k"}) == "$8\r\nreplaced\r\n");
    // An absolute TTL in the past creates nothing (and REPLACE drops the old value).
    REQUIRE(run({"RESTORE", "k", "1", payload, "REPLACE", "ABSTTL"}) == "+OK\r\n");
    REQUIRE(run({"GET", "k"}) == "$-1\r\n");
}

TEST_CASE("MIGRATE validates arguments and reports missing keys", "[cluster][dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    auto run = [&d](const CommandDispatcher::Args& argv) {
        std::string out;
        d.dispatch(argv, out);
        return out;
    };
    REQUIRE(run({"MIGRATE", "127.0.0.1", "1", "k", "1", "100"}) == "-ERR DB index is out of range\r\n");
    REQUIRE(run({"MIGRATE", "127.0.0.1", "1", "k", "0", "100", "KEYS", "a"}) ==
            "-ERR When using MIGRATE KEYS option, the key argument must be set to the empty string\r\n");
    REQUIRE(run({"MIGRATE", "127.0.0.1", "1", "k", "0", "100"}) == "+NOKEY\r\n");
    run({"SET", "k", "v"});
    // Nothing listens on port 1: the key must stay put.
    REQUIRE(run({"MIGRATE", "127.0.0.1", "1", "k", "0", "100"}).rfind("-IOERR", 0) == 0);
    REQUIRE(run({"GET", "k"}) == "$1\r\nv\r\n");
}

// --- cluster mode off -------------------------------------------------------------

TEST_CASE("cluster commands need cluster mode", "[cluster][dispatcher]") {
    Store store;
    CommandDispatcher d(store);
    std::string out;
    d.dispatch({"CLUSTER", "INFO"}, out);
    REQUIRE(out == "-ERR This instance has cluster support disabled\r\n");
    out.clear();
    d.dispatch({"ASKING"}, out);
    REQUIRE(out == "-ERR This instance has cluster support disabled\r\n");
    out.clear();
    d.dispatch({"INFO", "cluster"}, out);
    REQUIRE(out.find("cluster_enabled:0") != std::string::npos);
}

// --- routing ------------------------------------------------------------------------

TEST_CASE("a fresh node creates its identity and waits for slots", "[cluster]") {
    Fixture f("");
    std::string id = f.run({"CLUSTER", "MYID"});
    REQUIRE(id.size() == 5 + 40 + 2);  // $40\r\n<id>\r\n
    REQUIRE(cluster::valid_node_id(id.substr(5, 40)));
    REQUIRE(f.run({"CLUSTER", "INFO"}).find("cluster_state:fail") != std::string::npos);
    REQUIRE(f.run({"GET", "foo"}) == "-CLUSTERDOWN The cluster is down\r\n");
    REQUIRE(f.run({"PING"}) == "+PONG\r\n");  // keyless commands still work

    REQUIRE(f.run({"CLUSTER", "ADDSLOTSRANGE", "0", "16383"}) == "+OK\r\n");
    REQUIRE(f.run({"SET", "foo", "1"}) == "+OK\r\n");
    REQUIRE(f.run({"CLUSTER", "COUNTKEYSINSLOT", std::to_string(kSlotFoo)}) == ":1\r\n");
    REQUIRE(f.run({"CLUSTER", "GETKEYSINSLOT", std::to_string(kSlotFoo), "10"}) == "*1\r\n$3\r\nfoo\r\n");
    REQUIRE(f.run({"CLUSTER", "KEYSLOT", "foo"}) == ":12182\r\n");

    // The identity survives a restart.
    auto again = f.make_cluster();
    std::string error;
    REQUIRE(again->load_config(error));
    REQUIRE(again->myid() == id.substr(5, 40));
    REQUIRE(again->state_ok());
}

TEST_CASE("keys in slots owned elsewhere are redirected with MOVED", "[cluster]") {
    Fixture f;
    REQUIRE(f.run({"CLUSTER", "MYID"}) == "$40\r\n" + kIdA + "\r\n");
    REQUIRE(f.cluster->state_ok());
    REQUIRE(f.run({"SET", "bar", "1"}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"SET", "foo", "1"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"DEL", "bar", "foo"}) == "-CROSSSLOT Keys in request don't hash to the same slot\r\n");
    REQUIRE(f.run({"DEL", "{bar}1", "{bar}2", "bar"}) == ":1\r\n");  // one slot via hash tags
    REQUIRE(f.run({"REPLICAOF", "127.0.0.1", "7001"}) == "-ERR REPLICAOF not allowed in cluster mode.\r\n");
    REQUIRE(f.run({"INFO", "cluster"}).find("cluster_enabled:1") != std::string::npos);
}

TEST_CASE("a migrating slot serves keys still here and ASKs for the rest", "[cluster]") {
    Fixture f;
    f.run({"SET", "bar", "1"});
    REQUIRE(f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotFoo), "MIGRATING", kIdB}) ==
            "-ERR I'm not the owner of hash slot 12182\r\n");
    REQUIRE(f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotBar), "MIGRATING", kIdB}) == "+OK\r\n");
    REQUIRE(f.nodes().find("[5061->-" + kIdB + "]") != std::string::npos);

    REQUIRE(f.run({"GET", "bar"}) == "$1\r\n1\r\n");
    REQUIRE(f.run({"GET", "{bar}gone"}) == "-ASK 5061 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"SET", "{bar}new", "x"}) == "-ASK 5061 127.0.0.1:7001\r\n");  // new keys go to the target
    REQUIRE(f.run({"EXISTS", "bar", "{bar}gone"}) == "-TRYAGAIN Multiple keys request during rehashing of slot\r\n");

    // Can't hand the slot over while keys remain here.
    REQUIRE(f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotBar), "NODE", kIdB}).rfind("-ERR Can't assign", 0) == 0);
    f.run({"DEL", "bar"});
    REQUIRE(f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotBar), "NODE", kIdB}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "bar"}) == "-MOVED 5061 127.0.0.1:7001\r\n");
    REQUIRE(f.nodes().find("->-") == std::string::npos);  // migration state cleared
}

TEST_CASE("an importing slot only serves clients that sent ASKING", "[cluster]") {
    Fixture f;
    std::string slot = std::to_string(kSlotFoo);
    REQUIRE(f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotBar), "IMPORTING", kIdB}) ==
            "-ERR I'm already the owner of hash slot 5061\r\n");
    REQUIRE(f.run({"CLUSTER", "SETSLOT", slot, "IMPORTING", kIdB}) == "+OK\r\n");

    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"ASKING"}) == "+OK\r\n");
    REQUIRE(f.run({"SET", "foo", "v"}) == "+OK\r\n");
    // ASKING is one-shot.
    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    // RESTORE-ASKING implies it (that's what MIGRATE sends).
    REQUIRE(f.run({"RESTORE-ASKING", "{foo}2", "0", snapshot::dump_value("w")}) == "+OK\r\n");
    f.run({"ASKING"});
    REQUIRE(f.run({"EXISTS", "foo", "{foo}missing"}) == "-TRYAGAIN Multiple keys request during rehashing of slot\r\n");

    // Finishing the import: we own the slot, under a fresh, highest epoch.
    REQUIRE(f.run({"CLUSTER", "SETSLOT", slot, "NODE", kIdA}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "foo"}) == "$1\r\nv\r\n");
    std::string info = f.run({"CLUSTER", "INFO"});
    REQUIRE(info.find("cluster_my_epoch:3") != std::string::npos);
    REQUIRE(info.find("cluster_current_epoch:3") != std::string::npos);
    REQUIRE(f.nodes().find("-<-") == std::string::npos);
}

TEST_CASE("CLUSTER SLOTS groups contiguous ranges per owner", "[cluster]") {
    Fixture f;
    std::string expected = "*2\r\n"
                           "*3\r\n:0\r\n:8191\r\n*3\r\n$9\r\n127.0.0.1\r\n:7000\r\n$40\r\n" + kIdA + "\r\n"
                           "*3\r\n:8192\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n:7001\r\n$40\r\n" + kIdB + "\r\n";
    REQUIRE(f.run({"CLUSTER", "SLOTS"}) == expected);
}

TEST_CASE("ADDSLOTS / DELSLOTS are validated all-or-nothing", "[cluster]") {
    Fixture f;
    REQUIRE(f.run({"CLUSTER", "ADDSLOTS", "8192"}) == "-ERR Slot 8192 is already busy\r\n");
    REQUIRE(f.run({"CLUSTER", "ADDSLOTS", "16384"}) == "-ERR Invalid or out of range slot\r\n");
    REQUIRE(f.run({"CLUSTER", "DELSLOTS", "1", "1"}) == "-ERR Slot 1 specified multiple times\r\n");
    REQUIRE(f.run({"CLUSTER", "DELSLOTSRANGE", "5", "3"}) ==
            "-ERR start slot number 5 is greater than end slot number 3\r\n");
    REQUIRE(f.run({"CLUSTER", "DELSLOTS", "0", "1"}) == "+OK\r\n");
    REQUIRE(f.run({"CLUSTER", "DELSLOTS", "0"}) == "-ERR Slot 0 is already unassigned\r\n");
    // Full coverage lost: key commands are refused.
    REQUIRE(f.run({"GET", "bar"}) == "-CLUSTERDOWN The cluster is down\r\n");
    REQUIRE(f.run({"CLUSTER", "ADDSLOTSRANGE", "0", "1"}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "bar"}) == "$-1\r\n");
}

TEST_CASE("the slot map and migration state survive a restart", "[cluster]") {
    Fixture f;
    f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotBar), "MIGRATING", kIdB});
    f.run({"CLUSTER", "SETSLOT", std::to_string(kSlotFoo), "IMPORTING", kIdB});
    std::string before = f.nodes();

    auto again = f.make_cluster();
    std::string error;
    REQUIRE(again->load_config(error));
    std::string out;
    again->command({"CLUSTER", "NODES"}, out);
    REQUIRE(out.substr(out.find("\r\n") + 2) == before);
    REQUIRE(before.find("[5061->-" + kIdB + "]") != std::string::npos);
    REQUIRE(before.find("[12182-<-" + kIdB + "]") != std::string::npos);
}

TEST_CASE("keys on disk for a slot owned elsewhere become importing", "[cluster]") {
    Fixture f;
    f.store.set("foo", "loaded");  // as if loaded from a snapshot
    f.cluster->verify_config_with_data();
    REQUIRE(f.nodes().find("[12182-<-" + kIdB + "]") != std::string::npos);
    f.run({"ASKING"});
    REQUIRE(f.run({"GET", "foo"}) == "$6\r\nloaded\r\n");
}

TEST_CASE("a replica redirects writes, and reads unless READONLY", "[cluster]") {
    Fixture f(kIdA + " 127.0.0.1:7000@17000 myself,slave " + kIdB + " 0 0 0 connected\n" + kIdB +
              " 127.0.0.1:7001@17001 master - 0 0 2 connected 0-16383\n");
    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"READONLY"}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "foo"}) == "$-1\r\n");
    REQUIRE(f.run({"SET", "foo", "1"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"READWRITE"}) == "+OK\r\n");
    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"CLUSTER", "SETSLOT", "1", "STABLE"}) == "-ERR Please use SETSLOT only with masters.\r\n");
    REQUIRE(f.run({"CLUSTER", "FORGET", kIdB}) == "-ERR Can't forget my master!\r\n");
}

TEST_CASE("REPLICATE, FORGET and epoch commands check their preconditions", "[cluster]") {
    Fixture f;
    REQUIRE(f.run({"CLUSTER", "REPLICATE", kIdA}) == "-ERR Can't replicate myself\r\n");
    REQUIRE(f.run({"CLUSTER", "REPLICATE", std::string(40, 'c')}) == "-ERR Unknown node " + std::string(40, 'c') + "\r\n");
    REQUIRE(f.run({"CLUSTER", "REPLICATE", kIdB}) ==
            "-ERR To set a master the node must be empty and without assigned slots.\r\n");
    REQUIRE(f.run({"CLUSTER", "SET-CONFIG-EPOCH", "5"}).rfind("-ERR The user can assign", 0) == 0);
    REQUIRE(f.run({"CLUSTER", "BUMPEPOCH"}) == "+BUMPED 3\r\n");
    REQUIRE(f.run({"CLUSTER", "BUMPEPOCH"}) == "+STILL 3\r\n");
    REQUIRE(f.run({"CLUSTER", "FORGET", kIdA}) == "-ERR I tried hard but I can't forget myself...\r\n");

    REQUIRE(f.run({"CLUSTER", "FORGET", kIdB}) == "+OK\r\n");
    REQUIRE(f.nodes().find(kIdB) == std::string::npos);
    REQUIRE(f.run({"GET", "foo"}) == "-CLUSTERDOWN The cluster is down\r\n");  // B's slots are unowned now
    REQUIRE(f.run({"CLUSTER", "MEET", "not-an-ip", "7001"}) == "-ERR Invalid node address specified: not-an-ip:7001\r\n");
    REQUIRE(f.run({"CLUSTER", "MEET", "127.0.0.1", "7001"}) == "+OK\r\n");
    REQUIRE(f.nodes().find("127.0.0.1:7001@17001 handshake") != std::string::npos);
    REQUIRE(f.run({"CLUSTER", "NOPE"}).rfind("-ERR unknown subcommand", 0) == 0);
}

// --- failover ---------------------------------------------------------------------

TEST_CASE("CLUSTER FAILOVER checks its preconditions", "[cluster][failover]") {
    Fixture master;
    REQUIRE(master.run({"CLUSTER", "FAILOVER"}) == "-ERR You should send CLUSTER FAILOVER to a replica\r\n");
    REQUIRE(master.run({"CLUSTER", "FAILOVER", "NOW"}) == "-ERR syntax error\r\n");
    REQUIRE(master.run({"CLUSTER", "FAILOVER", "FORCE", "TAKEOVER"}).rfind("-ERR wrong number", 0) == 0);

    Fixture orphan(kIdA + " 127.0.0.1:7000@17000 myself,slave " + std::string(40, 'c') + " 0 0 0 connected\n");
    REQUIRE(orphan.run({"CLUSTER", "FAILOVER"}) == "-ERR I'm a replica but my master is unknown to me\r\n");

    // No bus here, so the master is never connected: only FORCE / TAKEOVER.
    Fixture replica(kIdA + " 127.0.0.1:7000@17000 myself,slave " + kIdB + " 0 0 0 connected\n" + kIdB +
                    " 127.0.0.1:7001@17001 master - 0 0 2 connected 0-16383\n");
    REQUIRE(replica.run({"CLUSTER", "FAILOVER"}) ==
            "-ERR Master is down or failed, please use CLUSTER FAILOVER FORCE\r\n");
    REQUIRE(replica.run({"CLUSTER", "FAILOVER", "FORCE"}) == "+OK\r\n");
}

TEST_CASE("CLUSTER FAILOVER TAKEOVER promotes without a vote", "[cluster][failover]") {
    Fixture f(kIdA + " 127.0.0.1:7000@17000 myself,slave " + kIdB + " 0 0 0 connected\n" + kIdB +
              " 127.0.0.1:7001@17001 master - 0 0 2 connected 0-16383\n"
              "vars currentEpoch 2 lastVoteEpoch 0\n");
    REQUIRE(f.run({"GET", "foo"}) == "-MOVED 12182 127.0.0.1:7001\r\n");
    REQUIRE(f.run({"CLUSTER", "FAILOVER", "TAKEOVER"}) == "+OK\r\n");

    // Every slot of the old master is ours, under a fresh, highest epoch.
    std::string nodes = f.nodes();
    REQUIRE(nodes.find(kIdA + " 127.0.0.1:7000@17000 myself,master - 0 0 3 connected 0-16383") != std::string::npos);
    REQUIRE(nodes.find(kIdB + " 127.0.0.1:7001@17001 master - 0 0 2 disconnected\n") != std::string::npos);
    REQUIRE(f.run({"SET", "foo", "1"}) == "+OK\r\n");
    REQUIRE(f.run({"CLUSTER", "FAILOVER"}) == "-ERR You should send CLUSTER FAILOVER to a replica\r\n");

    // And it was saved: a restart keeps the new role.
    auto again = f.make_cluster();
    std::string error;
    REQUIRE(again->load_config(error));
    std::string out;
    again->command({"CLUSTER", "NODES"}, out);
    REQUIRE(out.find("myself,master - 0 0 3 connected 0-16383") != std::string::npos);
}

TEST_CASE("failure flags and the last vote survive a restart", "[cluster][failover]") {
    const std::string kIdC(40, 'c');
    Fixture f(kIdA + " 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460\n" + kIdB +
              " 127.0.0.1:7001@17001 master,fail - 0 0 2 connected 5461-10922\n" + kIdC +
              " 127.0.0.1:7002@17002 master,fail? - 0 0 3 connected 10923-16383\n"
              "vars currentEpoch 7 lastVoteEpoch 6\n");
    std::string nodes = f.nodes();
    REQUIRE(nodes.find(kIdB + " 127.0.0.1:7001@17001 master,fail ") != std::string::npos);
    REQUIRE(nodes.find(kIdC + " 127.0.0.1:7002@17002 master,fail? ") != std::string::npos);

    // B's slots aren't served while it's FAIL, and with B and C both
    // unreachable we're in the minority anyway: the cluster is down.
    std::string info = f.run({"CLUSTER", "INFO"});
    REQUIRE(info.find("cluster_state:fail") != std::string::npos);
    REQUIRE(info.find("cluster_slots_ok:5461") != std::string::npos);
    REQUIRE(info.find("cluster_slots_fail:5462") != std::string::npos);
    REQUIRE(info.find("cluster_slots_pfail:5461") != std::string::npos);
    REQUIRE(info.find("cluster_current_epoch:7") != std::string::npos);
    REQUIRE(f.run({"GET", "k"}) == "-CLUSTERDOWN The cluster is down\r\n");

    REQUIRE(f.run({"CLUSTER", "COUNT-FAILURE-REPORTS", kIdB}) == ":0\r\n");
    REQUIRE(f.run({"CLUSTER", "COUNT-FAILURE-REPORTS", std::string(40, 'd')}) ==
            "-ERR Unknown node " + std::string(40, 'd') + "\r\n");

    REQUIRE(f.run({"CLUSTER", "SAVECONFIG"}) == "+OK\r\n");
    std::ifstream in(f.dir.file("nodes.conf"));
    std::stringstream saved;
    saved << in.rdbuf();
    REQUIRE(saved.str().find("vars currentEpoch 7 lastVoteEpoch 6") != std::string::npos);
    REQUIRE(saved.str().find("master,fail -") != std::string::npos);
}

TEST_CASE("a FAIL master's slots take the cluster down; a healthy majority keeps it up", "[cluster][failover]") {
    const std::string kIdC(40, 'c');
    // Three masters, one of them FAIL: we're in the majority, but its slots
    // are unserved.
    Fixture f(kIdA + " 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460\n" + kIdB +
              " 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922\n" + kIdC +
              " 127.0.0.1:7002@17002 master,fail - 0 0 3 connected 10923-16383\n");
    REQUIRE(f.run({"CLUSTER", "INFO"}).find("cluster_state:fail") != std::string::npos);

    // Without full coverage, the slots that are still served keep working.
    Fixture partial(kIdA + " 127.0.0.1:7000@17000 myself,master - 0 0 1 connected 0-5460\n" + kIdB +
                    " 127.0.0.1:7001@17001 master - 0 0 2 connected 5461-10922\n" + kIdC +
                    " 127.0.0.1:7002@17002 master,fail - 0 0 3 connected 10923-16383\n");
    Cluster::Options copts;
    copts.config_file = partial.dir.file("nodes.conf");
    copts.port = 7000;
    copts.cport = 17000;
    copts.require_full_coverage = false;
    partial.cluster = std::make_unique<Cluster>(partial.store, *partial.replication, copts);
    std::string error;
    REQUIRE(partial.cluster->load_config(error));
    partial.dispatcher->set_cluster(partial.cluster.get());
    REQUIRE(partial.run({"CLUSTER", "INFO"}).find("cluster_state:ok") != std::string::npos);
    REQUIRE(partial.run({"GET", "bar"}) == "$-1\r\n");  // slot 5061: ours
}
