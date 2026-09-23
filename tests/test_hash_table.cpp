#include <catch2/catch_test_macros.hpp>

#include "hash_table.hpp"

TEST_CASE("set then get returns the stored value", "[hash_table]") {
    HashTable table;
    table.set("foo", "bar");
    REQUIRE(table.get("foo") == "bar");
}

TEST_CASE("get on a missing key returns nullopt", "[hash_table]") {
    HashTable table;
    REQUIRE(table.get("missing") == std::nullopt);
}

TEST_CASE("set on an existing key overwrites the value without changing size", "[hash_table]") {
    HashTable table;
    table.set("foo", "bar");
    table.set("foo", "baz");
    REQUIRE(table.get("foo") == "baz");
    REQUIRE(table.size() == 1);
}

TEST_CASE("del removes an existing key and reports success", "[hash_table]") {
    HashTable table;
    table.set("foo", "bar");
    REQUIRE(table.del("foo") == true);
    REQUIRE(table.get("foo") == std::nullopt);
    REQUIRE(table.size() == 0);
}

TEST_CASE("del on a missing key reports failure", "[hash_table]") {
    HashTable table;
    REQUIRE(table.del("missing") == false);
}

TEST_CASE("size tracks insertions and deletions across many keys, forcing a resize", "[hash_table]") {
    HashTable table;
    constexpr int kCount = 500;

    for (int i = 0; i < kCount; ++i) {
        table.set("key" + std::to_string(i), "value" + std::to_string(i));
    }
    REQUIRE(table.size() == static_cast<size_t>(kCount));

    for (int i = 0; i < kCount; ++i) {
        REQUIRE(table.get("key" + std::to_string(i)) == "value" + std::to_string(i));
    }

    for (int i = 0; i < kCount; i += 2) {
        REQUIRE(table.del("key" + std::to_string(i)) == true);
    }
    REQUIRE(table.size() == static_cast<size_t>(kCount / 2));

    for (int i = 0; i < kCount; ++i) {
        if (i % 2 == 0) {
            REQUIRE(table.get("key" + std::to_string(i)) == std::nullopt);
        } else {
            REQUIRE(table.get("key" + std::to_string(i)) == "value" + std::to_string(i));
        }
    }
}
