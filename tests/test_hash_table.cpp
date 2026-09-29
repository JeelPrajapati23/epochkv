#include <catch2/catch_test_macros.hpp>

#include <map>
#include <string>

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
    // Each halving starts only when the previous one has finished, so the
    // rest of the way down happens in rehash_step() (the server's timer).
    while (table.rehash_step(100)) {
    }
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

namespace {

// Inserts keys key<next>, key<next+1>, ... until the table is resizing to
// at least `min_buckets` buckets. Returns the next unused key number.
int insert_until_growing_to(HashTable<int>& table, int next, size_t min_buckets) {
    while (!(table.rehashing() && table.bucket_count() >= min_buckets)) {
        table.set("key" + std::to_string(next), next);
        ++next;
    }
    return next;
}

}  // namespace

TEST_CASE("a grow is spread over later writes instead of done at once", "[hash_table][rehash]") {
    HashTable<int> table;
    int n = insert_until_growing_to(table, 0, 4096);
    size_t old_buckets = table.bucket_count() / 2;

    // Every write moves at least one old bucket, so the grow is done within
    // old_buckets writes, and no second resize is needed in the meantime.
    size_t writes = 0;
    while (table.rehashing()) {
        table.set("key" + std::to_string(n), n);
        ++n;
        ++writes;
        REQUIRE(writes <= old_buckets);
    }
    REQUIRE(writes > 100);  // ...but it did take many writes, not one
    REQUIRE(table.bucket_count() == old_buckets * 2);
    REQUIRE(table.size() == static_cast<size_t>(n));
}

TEST_CASE("every operation sees a consistent table in the middle of a resize", "[hash_table][rehash]") {
    HashTable<int> table;
    int n = insert_until_growing_to(table, 0, 4096);
    std::map<std::string, int> model;
    for (int i = 0; i < n; ++i) {
        model["key" + std::to_string(i)] = i;
    }
    table.rehash_step(table.bucket_count() / 16);  // part-way, not done

    // Overwrites, deletes and inserts, each of which also moves a bucket.
    // Few enough that the grow (2048 old buckets) is still in progress.
    for (int i = 0; i < n; i += 7) {
        std::string key = "key" + std::to_string(i);
        table.set(key, -i);
        model[key] = -i;
    }
    for (int i = 1; i < n; i += 11) {
        std::string key = "key" + std::to_string(i);
        REQUIRE(table.del(key) == (model.erase(key) == 1));
    }
    for (int i = 0; i < 20; ++i) {
        std::string key = "new" + std::to_string(i);
        REQUIRE(table.set(key, i));
        model[key] = i;
    }
    REQUIRE(table.rehashing());  // the checks below must run mid-resize

    REQUIRE(table.size() == model.size());
    for (const auto& [key, value] : model) {
        REQUIRE(table.get(key) == value);
    }
    REQUIRE(table.find("key1") == nullptr);  // deleted above

    std::map<std::string, int> seen;
    table.for_each([&](const std::string& key, int value) {
        REQUIRE(seen.emplace(key, value).second);  // each entry exactly once
    });
    REQUIRE(seen == model);

    std::mt19937_64 rng(7);
    for (int trial = 0; trial < 500; ++trial) {
        auto [key, value] = table.random_entry(rng);
        REQUIRE(key != nullptr);
        REQUIRE(model.at(*key) == *value);
    }

    while (table.rehash_step(10)) {
    }
    REQUIRE(table.size() == model.size());
    for (const auto& [key, value] : model) {
        REQUIRE(table.get(key) == value);
    }
}

TEST_CASE("find doesn't move entries, so pointers survive other lookups mid-resize", "[hash_table][rehash]") {
    HashTable<int> table;
    int n = insert_until_growing_to(table, 0, 1024);
    int* first = table.find("key0");
    REQUIRE(first != nullptr);
    for (int i = 0; i < n; ++i) {
        REQUIRE(table.find("key" + std::to_string(i)) != nullptr);
    }
    REQUIRE(table.rehashing());
    REQUIRE(table.find("key0") == first);
}

TEST_CASE("a shrink of a sparse table skips at most 10 empty buckets per step", "[hash_table][rehash]") {
    HashTable<int> table;
    int n = insert_until_growing_to(table, 0, 32768);
    while (table.rehash_step(1000)) {
    }
    // Delete until a shrink starts: the old array is ~90% empty buckets.
    int deleted = 0;
    while (!table.rehashing()) {
        table.del("key" + std::to_string(deleted));
        ++deleted;
    }
    size_t old_buckets = table.bucket_count() * 2;

    // rehash_step(1) moves at most one bucket and skips at most 10 empty
    // ones, so it takes at least old_buckets / 11 steps to finish, instead
    // of one step scanning the whole array.
    size_t steps = 0;
    while (table.rehashing()) {
        table.rehash_step(1);
        ++steps;
    }
    REQUIRE(steps >= old_buckets / 11);
    REQUIRE(table.size() == static_cast<size_t>(n - deleted));
    for (int i = deleted; i < n; ++i) {
        REQUIRE(table.get("key" + std::to_string(i)) == i);
    }
}
