#include <catch2/catch_test_macros.hpp>

#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>

#include "snapshot.hpp"
#include "store.hpp"
#include "temp_dir.hpp"

namespace {

struct FakeClock {
    int64_t now = 1'000'000;
    Store::Options options() {
        Store::Options o;
        o.clock = [this] { return now; };
        return o;
    }
};

std::string read_file(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in), {});
}

void write_file(const std::string& path, const std::string& data) {
    std::ofstream(path, std::ios::binary | std::ios::trunc) << data;
}

}  // namespace

TEST_CASE("a snapshot round-trips keys, binary values and TTLs", "[snapshot]") {
    TempDir dir;
    FakeClock clock;
    Store original(clock.options());
    original.set("plain", "value");
    original.set("binary", std::string("a\0b\r\n\xff", 6));
    original.set("empty", "");
    original.set("big", std::string(200'000, 'x'));  // spans several I/O buffers
    original.set("ttl", "v");
    original.set_expire("ttl", clock.now + 5000);
    REQUIRE(snapshot::save(original, dir.file("dump.snap")));

    Store loaded(clock.options());
    std::string error;
    REQUIRE(snapshot::load(dir.file("dump.snap"), loaded, error) == snapshot::LoadResult::kOk);
    REQUIRE(loaded.size() == 5);
    REQUIRE(*loaded.get("plain") == "value");
    REQUIRE(*loaded.get("binary") == std::string("a\0b\r\n\xff", 6));
    REQUIRE(*loaded.get("empty") == "");
    REQUIRE(loaded.get("big")->size() == 200'000);
    REQUIRE(loaded.pttl("ttl") == 5000);
    REQUIRE(loaded.pttl("plain") == -1);
}

TEST_CASE("expiry is absolute: keys that expired while down are not loaded", "[snapshot]") {
    TempDir dir;
    FakeClock clock;
    Store original(clock.options());
    original.set("short", "v");
    original.set_expire("short", clock.now + 1000);
    original.set("long", "v");
    original.set_expire("long", clock.now + 60'000);
    REQUIRE(snapshot::save(original, dir.file("dump.snap")));

    clock.now += 10'000;  // "restart" ten seconds later
    Store loaded(clock.options());
    std::string error;
    REQUIRE(snapshot::load(dir.file("dump.snap"), loaded, error) == snapshot::LoadResult::kOk);
    REQUIRE(loaded.size() == 1);
    REQUIRE(loaded.pttl("long") == 50'000);
}

TEST_CASE("a missing snapshot is reported as not found, not as an error", "[snapshot]") {
    TempDir dir;
    Store store;
    std::string error;
    REQUIRE(snapshot::load(dir.file("nope.snap"), store, error) == snapshot::LoadResult::kNotFound);
}

TEST_CASE("save replaces the old snapshot and leaves no temp files", "[snapshot]") {
    TempDir dir;
    Store store;
    store.set("k", "1");
    REQUIRE(snapshot::save(store, dir.file("dump.snap")));
    store.set("k", "2");
    REQUIRE(snapshot::save(store, dir.file("dump.snap")));

    size_t files = 0;
    for (const auto& entry : std::filesystem::directory_iterator(dir.path)) {
        (void)entry;
        ++files;
    }
    REQUIRE(files == 1);

    Store loaded;
    std::string error;
    REQUIRE(snapshot::load(dir.file("dump.snap"), loaded, error) == snapshot::LoadResult::kOk);
    REQUIRE(*loaded.get("k") == "2");
}

TEST_CASE("corrupt or truncated snapshots are rejected", "[snapshot]") {
    TempDir dir;
    Store store;
    for (int i = 0; i < 100; ++i) {
        store.set("key" + std::to_string(i), "value" + std::to_string(i));
    }
    std::string path = dir.file("dump.snap");
    REQUIRE(snapshot::save(store, path));
    const std::string good = read_file(path);

    std::string error;
    auto load_into_fresh_store = [&] {
        Store fresh;
        error.clear();
        return snapshot::load(path, fresh, error);
    };

    SECTION("a flipped bit fails the checksum") {
        std::string bad = good;
        bad[good.size() / 2] ^= 0x01;
        write_file(path, bad);
        REQUIRE(load_into_fresh_store() == snapshot::LoadResult::kError);
    }
    SECTION("a truncated file is detected") {
        write_file(path, good.substr(0, good.size() - 10));
        REQUIRE(load_into_fresh_store() == snapshot::LoadResult::kError);
        REQUIRE(error.find("truncated") != std::string::npos);
    }
    SECTION("trailing garbage is detected") {
        write_file(path, good + "extra");
        REQUIRE(load_into_fresh_store() == snapshot::LoadResult::kError);
    }
    SECTION("a file that isn't a snapshot is detected") {
        write_file(path, "*1\r\n$4\r\nPING\r\n");
        REQUIRE(load_into_fresh_store() == snapshot::LoadResult::kError);
        REQUIRE(error.find("bad magic") != std::string::npos);
    }
}
