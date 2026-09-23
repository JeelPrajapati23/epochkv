#include <catch2/catch_test_macros.hpp>

#include "hash_table.hpp"

TEST_CASE("set then get returns the stored value", "[hash_table]") {
    HashTable<std::string> table;
    table.set("foo", "bar");
    REQUIRE(table.get("foo") == "bar");
}

TEST_CASE("get on a missing key returns nullopt", "[hash_table]") {
    HashTable<std::string> table;
    REQUIRE(table.get("missing") == std::nullopt);
}

TEST_CASE("set on an existing key overwrites the value without changing size", "[hash_table]") {
    HashTable<std::string> table;
    table.set("foo", "bar");
    table.set("foo", "baz");
    REQUIRE(table.get("foo") == "baz");
    REQUIRE(table.size() == 1);
}

TEST_CASE("del removes an existing key and reports success", "[hash_table]") {
    HashTable<std::string> table;
    table.set("foo", "bar");
    REQUIRE(table.del("foo") == true);
    REQUIRE(table.get("foo") == std::nullopt);
    REQUIRE(table.size() == 0);
}

TEST_CASE("del on a missing key reports failure", "[hash_table]") {
    HashTable<std::string> table;
    REQUIRE(table.del("missing") == false);
}

TEST_CASE("size tracks insertions and deletions across many keys, forcing a resize", "[hash_table]") {
    HashTable<std::string> table;
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

TEST_CASE("find returns a mutable pointer to the stored value", "[hash_table]") {
    HashTable<std::string> table;
    table.set("foo", "bar");
    std::string* value = table.find("foo");
    REQUIRE(value != nullptr);
    *value = "changed";
    REQUIRE(table.get("foo") == "changed");
    REQUIRE(table.find("missing") == nullptr);
}

TEST_CASE("set reports whether the key was newly inserted", "[hash_table]") {
    HashTable<int> table;
    REQUIRE(table.set("k", 1) == true);
    REQUIRE(table.set("k", 2) == false);
    REQUIRE(table.get("k") == 2);
}

TEST_CASE("table shrinks back down after mass deletion", "[hash_table]") {
    HashTable<int> table;
    for (int i = 0; i < 1000; ++i) {
        table.set("key" + std::to_string(i), i);
    }
    size_t grown = table.bucket_count();
    REQUIRE(grown > 16);

    for (int i = 0; i < 1000; ++i) {
        table.del("key" + std::to_string(i));
    }
    REQUIRE(table.size() == 0);
    REQUIRE(table.bucket_count() == 16);
}

TEST_CASE("random_entry returns only live entries and handles empty tables", "[hash_table]") {
    HashTable<int> table;
    std::mt19937_64 rng(42);
    REQUIRE(table.random_entry(rng).first == nullptr);

    for (int i = 0; i < 50; ++i) {
        table.set("key" + std::to_string(i), i);
    }
    for (int i = 0; i < 25; ++i) {
        table.del("key" + std::to_string(i));
    }
    for (int trial = 0; trial < 200; ++trial) {
        auto [key, value] = table.random_entry(rng);
        REQUIRE(key != nullptr);
        REQUIRE(*value >= 25);
        REQUIRE(*key == "key" + std::to_string(*value));
    }
}
