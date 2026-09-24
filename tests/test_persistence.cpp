#include <catch2/catch_test_macros.hpp>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <thread>
#include <vector>

#include "command_dispatcher.hpp"
#include "persistence.hpp"
#include "snapshot.hpp"
#include "store.hpp"
#include "temp_dir.hpp"

namespace fs = std::filesystem;

namespace {

// One server's worth of state, wired the way kv_server's main() does it,
// minus the sockets. Constructing a second Node on the same directory is a
// restart.
struct Node {
    int64_t now = 1'700'000'000'000;  // fake wall clock, unix ms
    Store store;
    Persistence persistence;
    CommandDispatcher dispatcher;

    Node(const std::string& dir, Persistence::Options options, size_t maxmemory = 0)
        : store(store_options(maxmemory)), persistence(store, with_dir(std::move(options), dir)), dispatcher(store) {
        dispatcher.set_persistence(&persistence);
        dispatcher.set_propagator([this](const CommandDispatcher::Args& argv) { persistence.propagate(argv); });
        store.set_deletion_listener([this](const std::string& key) { persistence.propagate({"DEL", key}); });
    }

    bool load(std::string* error_out = nullptr) {
        std::string error;
        bool ok = persistence.load(
            [this](const CommandDispatcher::Args& argv) {
                std::string ignored;
                dispatcher.dispatch(argv, ignored);
            },
            error);
        if (error_out != nullptr) {
            *error_out = error;
        }
        return ok;
    }

    // One command through one event-loop iteration: execute, then flush the
    // AOF the way before_sleep does before replies go out.
    std::string run(const CommandDispatcher::Args& argv) {
        std::string out;
        dispatcher.dispatch(argv, out);
        persistence.before_sleep();
        return out;
    }

    void wait_for_child() {
        for (int i = 0; i < 500 && persistence.child_running(); ++i) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            persistence.cron();
        }
        REQUIRE_FALSE(persistence.child_running());
    }

private:
    Store::Options store_options(size_t maxmemory) {
        Store::Options o;
        o.maxmemory = maxmemory;
        o.policy = EvictionPolicy::kAllKeysLru;
        o.clock = [this] { return now; };
        return o;
    }

    static Persistence::Options with_dir(Persistence::Options options, const std::string& dir) {
        options.dir = dir;
        return options;
    }
};

Persistence::Options aof_options() {
    Persistence::Options o;
    o.appendonly = true;
    o.appendfsync = FsyncPolicy::kAlways;
    o.save_points = {};
    o.auto_aof_rewrite_percentage = 0;
    return o;
}

Persistence::Options snapshot_options() {
    Persistence::Options o;
    o.save_points = {};
    return o;
}

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

std::vector<std::string> files_in(const std::string& dir) {
    std::vector<std::string> names;
    for (const auto& entry : fs::directory_iterator(dir)) {
        names.push_back(entry.path().filename().string());
    }
    return names;
}

std::string only_incr_file(const std::string& aof_dir) {
    std::string found;
    for (const std::string& name : files_in(aof_dir)) {
        if (name.find(".incr.aof") != std::string::npos) {
            REQUIRE(found.empty());
            found = name;
        }
    }
    REQUIRE_FALSE(found.empty());
    return aof_dir + "/" + found;
}

}  // namespace

TEST_CASE("the AOF replays every kind of write after a restart", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        node.run({"SET", "a", "1"});
        node.run({"SET", "b", "2"});
        node.run({"SET", "gone", "x"});
        node.run({"DEL", "gone"});
        node.run({"SET", "a", "updated"});
        node.run({"SET", "ttl", "v", "PX", "5000"});
        node.run({"SET", "persisted", "v", "EX", "100"});
        node.run({"PERSIST", "persisted"});
        node.run({"EXPIRE", "b", "50"});
    }
    Node restarted(dir.path, aof_options());
    REQUIRE(restarted.load());
    REQUIRE(restarted.store.size() == 4);
    REQUIRE(restarted.run({"GET", "a"}) == "$7\r\nupdated\r\n");
    REQUIRE(restarted.run({"PTTL", "ttl"}) == ":5000\r\n");
    REQUIRE(restarted.run({"TTL", "persisted"}) == ":-1\r\n");
    REQUIRE(restarted.run({"TTL", "b"}) == ":50\r\n");
    REQUIRE(restarted.run({"EXISTS", "gone"}) == ":0\r\n");
}

TEST_CASE("relative TTLs are logged as absolute deadlines", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        node.run({"SET", "k", "v", "EX", "10"});
        node.run({"SET", "short", "v", "PX", "100"});
    }
    Node restarted(dir.path, aof_options());
    restarted.now += 4000;  // the server was down for 4s
    REQUIRE(restarted.load());
    REQUIRE(restarted.run({"PTTL", "k"}) == ":6000\r\n");   // not a fresh 10s
    REQUIRE(restarted.run({"EXISTS", "short"}) == ":0\r\n");  // expired while down
}

TEST_CASE("keys the store evicts are logged as DEL and stay gone", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, aof_options(), /*maxmemory=*/2000);
        REQUIRE(node.load());
        for (int i = 0; i < 40; ++i) {
            node.run({"SET", "k" + std::to_string(i), "value"});
        }
        REQUIRE(node.store.evicted_keys() > 0);
    }
    // Reloaded without a memory limit: if evictions weren't logged, every
    // key would come back.
    Node restarted(dir.path, aof_options());
    REQUIRE(restarted.load());
    REQUIRE(restarted.run({"EXISTS", "k0"}) == ":0\r\n");
    REQUIRE(restarted.run({"EXISTS", "k39"}) == ":1\r\n");
    REQUIRE(restarted.store.size() < 40);
}

TEST_CASE("an incomplete command at the end of the AOF is truncated away", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        node.run({"SET", "k", "v"});
    }
    std::string incr = only_incr_file(dir.file("appendonlydir"));
    const auto good_size = fs::file_size(incr);
    std::ofstream(incr, std::ios::binary | std::ios::app) << "*3\r\n$3\r\nSET\r\n$1\r\nx";  // crash mid-write

    {
        Node restarted(dir.path, aof_options());
        REQUIRE(restarted.load());
        REQUIRE(restarted.run({"GET", "k"}) == "$1\r\nv\r\n");
        REQUIRE(fs::file_size(incr) == good_size);
        restarted.run({"SET", "after", "repair"});  // appends cleanly after the cut
    }
    Node again(dir.path, aof_options());
    REQUIRE(again.load());
    REQUIRE(again.run({"GET", "after"}) == "$6\r\nrepair\r\n");
}

TEST_CASE("corruption in the middle of the AOF refuses to load", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        node.run({"SET", "a", "1"});
        node.run({"SET", "b", "2"});
    }
    std::string incr = only_incr_file(dir.file("appendonlydir"));
    std::string data = read_file(incr);
    data[data.find("$1\r\nb")] = '#';
    std::ofstream(incr, std::ios::binary | std::ios::trunc) << data;

    Node restarted(dir.path, aof_options());
    std::string error;
    REQUIRE_FALSE(restarted.load(&error));
    REQUIRE(error.find("corrupt") != std::string::npos);
}

TEST_CASE("BGREWRITEAOF compacts history into a new base", "[persistence][aof]") {
    TempDir dir;
    std::string aof_dir = dir.file("appendonlydir");
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        for (int i = 0; i < 1000; ++i) {
            node.run({"SET", "counter", std::to_string(i)});
        }
        node.run({"SET", "other", "x"});
        auto before = fs::file_size(only_incr_file(aof_dir));

        REQUIRE(node.run({"BGREWRITEAOF"}) == "+Background append only file rewriting started\r\n");
        REQUIRE(node.run({"BGREWRITEAOF"}).rfind("-ERR Background append only file rewriting already", 0) == 0);
        // Writes during the rewrite land in the new incr, not the child's base.
        node.run({"SET", "during", "rewrite"});
        node.wait_for_child();

        auto names = files_in(aof_dir);
        REQUIRE(names.size() == 3);  // manifest, one base, one incr
        REQUIRE(fs::file_size(only_incr_file(aof_dir)) < before / 10);
        node.run({"SET", "after", "rewrite"});
    }
    Node restarted(dir.path, aof_options());
    REQUIRE(restarted.load());
    REQUIRE(restarted.store.size() == 4);
    REQUIRE(restarted.run({"GET", "counter"}) == "$3\r\n999\r\n");
    REQUIRE(restarted.run({"GET", "during"}) == "$7\r\nrewrite\r\n");
    REQUIRE(restarted.run({"GET", "after"}) == "$7\r\nrewrite\r\n");
}

TEST_CASE("the AOF auto-rewrites once it has grown enough", "[persistence][aof]") {
    TempDir dir;
    Persistence::Options options = aof_options();
    options.auto_aof_rewrite_percentage = 100;
    options.auto_aof_rewrite_min_size = 4096;
    Node node(dir.path, options);
    REQUIRE(node.load());
    for (int i = 0; i < 500; ++i) {
        node.run({"SET", "k", std::to_string(i)});
    }
    node.persistence.cron();
    REQUIRE(node.persistence.child_running());
    node.wait_for_child();
    REQUIRE(fs::file_size(only_incr_file(dir.file("appendonlydir"))) == 0);
}

TEST_CASE("turning AOF on for the first time keeps the snapshot's data", "[persistence][aof]") {
    TempDir dir;
    {
        Node node(dir.path, snapshot_options());
        REQUIRE(node.load());
        node.run({"SET", "from", "snapshot"});
        REQUIRE(node.run({"SAVE"}) == "+OK\r\n");
    }
    {
        Node node(dir.path, aof_options());
        REQUIRE(node.load());
        REQUIRE(node.run({"GET", "from"}) == "$8\r\nsnapshot\r\n");
    }
    fs::remove(dir.file("dump.snap"));  // from now on the AOF alone has it
    Node node(dir.path, aof_options());
    REQUIRE(node.load());
    REQUIRE(node.run({"GET", "from"}) == "$8\r\nsnapshot\r\n");
}

TEST_CASE("BGSAVE writes a snapshot from a child and updates LASTSAVE", "[persistence][snapshot]") {
    TempDir dir;
    {
        Node node(dir.path, snapshot_options());
        REQUIRE(node.load());
        node.run({"SET", "k", "v"});
        REQUIRE(node.run({"BGSAVE"}) == "+Background saving started\r\n");
        REQUIRE(node.run({"BGSAVE"}) == "-ERR Background save already in progress\r\n");
        node.run({"SET", "after-fork", "v"});  // not in this snapshot
        node.wait_for_child();
        REQUIRE(fs::exists(dir.file("dump.snap")));
        REQUIRE(node.run({"LASTSAVE"}).rfind(":", 0) == 0);
    }
    Node restarted(dir.path, snapshot_options());
    REQUIRE(restarted.load());
    REQUIRE(restarted.run({"GET", "k"}) == "$1\r\nv\r\n");
    REQUIRE(restarted.run({"EXISTS", "after-fork"}) == ":0\r\n");
}

TEST_CASE("a save point triggers a background save", "[persistence][snapshot]") {
    TempDir dir;
    Persistence::Options options = snapshot_options();
    options.save_points = {{0, 3}};  // as soon as there are 3 changes
    Node node(dir.path, options);
    REQUIRE(node.load());
    node.run({"SET", "a", "1"});
    node.run({"SET", "b", "2"});
    node.persistence.cron();
    REQUIRE_FALSE(node.persistence.child_running());
    node.run({"SET", "c", "3"});
    node.persistence.cron();
    REQUIRE(node.persistence.child_running());
    node.wait_for_child();
    REQUIRE(fs::exists(dir.file("dump.snap")));
}

TEST_CASE("after a failed background save, writes are refused until a save succeeds", "[persistence][snapshot]") {
    TempDir dir;
    std::string missing = dir.file("not-created-yet");
    Persistence::Options options = snapshot_options();
    options.save_points = {{3600, 1}};
    Node node(missing, options);
    REQUIRE(node.load());  // no snapshot there yet: an empty dataset is fine

    REQUIRE(node.run({"BGSAVE"}) == "+Background saving started\r\n");
    node.wait_for_child();  // the child can't create its file
    REQUIRE(node.run({"SET", "k", "v"}).rfind("-MISCONF", 0) == 0);
    REQUIRE(node.run({"GET", "k"}) == "$-1\r\n");  // reads still work

    fs::create_directory(missing);
    REQUIRE(node.run({"SAVE"}) == "+OK\r\n");
    REQUIRE(node.run({"SET", "k", "v"}) == "+OK\r\n");
}

TEST_CASE("shutdown saves a final snapshot when save points are configured", "[persistence][snapshot]") {
    TempDir dir;
    Persistence::Options options = snapshot_options();
    options.save_points = {{3600, 1}};
    {
        Node node(dir.path, options);
        REQUIRE(node.load());
        node.run({"SET", "k", "v"});
        REQUIRE(node.persistence.shutdown());
    }
    Node restarted(dir.path, options);
    REQUIRE(restarted.load());
    REQUIRE(restarted.run({"GET", "k"}) == "$1\r\nv\r\n");
}
