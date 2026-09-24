#include <catch2/catch_test_macros.hpp>

#include <algorithm>
#include <chrono>
#include <string>
#include <vector>

#include "store.hpp"

namespace {

// Tests drive time by hand instead of sleeping.
struct FakeClock {
    int64_t now = 1'000'000;
    Store::Clock fn() {
        return [this] { return now; };
    }
};

Store make_store(FakeClock& clock, size_t maxmemory = 0,
                 EvictionPolicy policy = EvictionPolicy::kAllKeysLru) {
    Store::Options options;
    options.maxmemory = maxmemory;
    options.policy = policy;
    options.clock = clock.fn();
    return Store(options);
}

constexpr std::chrono::microseconds kGenerousBudget{1'000'000};

}  // namespace

TEST_CASE("get returns the value, or nullptr for a missing key", "[store]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "v");
    REQUIRE(*store.get("k") == "v");
    REQUIRE(store.get("missing") == nullptr);
}

TEST_CASE("a key is readable until its deadline and gone at it", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "v");
    REQUIRE(store.set_expire("k", clock.now + 100));

    clock.now += 99;
    REQUIRE(store.get("k") != nullptr);
    REQUIRE(store.pttl("k") == 1);

    clock.now += 1;
    REQUIRE(store.get("k") == nullptr);
    REQUIRE(store.size() == 0);  // lazily deleted on access, not just hidden
    REQUIRE(store.expired_keys() == 1);
}

TEST_CASE("pttl distinguishes missing keys, keys without a TTL, and TTLs", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    REQUIRE(store.pttl("missing") == -2);
    store.set("k", "v");
    REQUIRE(store.pttl("k") == -1);
    store.set_expire("k", clock.now + 5000);
    REQUIRE(store.pttl("k") == 5000);
}

TEST_CASE("set_expire on a missing key fails; a past deadline deletes the key", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    REQUIRE_FALSE(store.set_expire("missing", clock.now + 100));

    store.set("k", "v");
    REQUIRE(store.set_expire("k", clock.now - 1));
    REQUIRE(store.get("k") == nullptr);
}

TEST_CASE("set clears an existing TTL unless keep_ttl", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "v1");
    store.set_expire("k", clock.now + 100);
    store.set("k", "v2", /*keep_ttl=*/true);
    REQUIRE(store.pttl("k") == 100);

    store.set("k", "v3");
    REQUIRE(store.pttl("k") == -1);
}

TEST_CASE("keep_ttl does not resurrect the TTL of an already-expired key", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "old");
    store.set_expire("k", clock.now + 10);
    clock.now += 10;
    store.set("k", "new", /*keep_ttl=*/true);
    REQUIRE(*store.get("k") == "new");
    REQUIRE(store.pttl("k") == -1);
}

TEST_CASE("persist removes a TTL", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "v");
    REQUIRE_FALSE(store.persist("k"));  // no TTL to remove
    store.set_expire("k", clock.now + 100);
    REQUIRE(store.persist("k"));
    clock.now += 1000;
    REQUIRE(store.get("k") != nullptr);
}

TEST_CASE("del on an expired key reports that nothing was deleted", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("k", "v");
    store.set_expire("k", clock.now + 10);
    clock.now += 10;
    REQUIRE_FALSE(store.del("k"));
}

TEST_CASE("active expiry reclaims keys that are never accessed again", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    for (int i = 0; i < 1000; ++i) {
        store.set("temp" + std::to_string(i), "v");
        store.set_expire("temp" + std::to_string(i), clock.now + 100);
    }
    for (int i = 0; i < 100; ++i) {
        store.set("perm" + std::to_string(i), "v");
    }

    clock.now += 100;
    // Each cycle keeps sampling while samples are mostly expired, so a few
    // cycles clear everything without ever touching the keys directly.
    size_t removed = 0;
    for (int cycle = 0; cycle < 10 && store.size() > 100; ++cycle) {
        removed += store.active_expire_cycle(kGenerousBudget);
    }
    REQUIRE(removed == 1000);
    REQUIRE(store.size() == 100);
    REQUIRE(store.get("perm0") != nullptr);
}

TEST_CASE("active expiry leaves unexpired keys alone", "[store][ttl]") {
    FakeClock clock;
    Store store = make_store(clock);
    for (int i = 0; i < 100; ++i) {
        store.set("k" + std::to_string(i), "v");
        store.set_expire("k" + std::to_string(i), clock.now + 1000);
    }
    REQUIRE(store.active_expire_cycle(kGenerousBudget) == 0);
    REQUIRE(store.size() == 100);
}

TEST_CASE("used memory returns to zero once every key is gone", "[store][memory]") {
    FakeClock clock;
    Store store = make_store(clock);
    REQUIRE(store.used_memory() == 0);
    store.set("a", "12345");
    store.set("b", "x");
    store.set_expire("b", clock.now + 10);
    store.set("a", "a much longer value than before");
    REQUIRE(store.used_memory() > 0);

    store.del("a");
    clock.now += 10;
    store.get("b");
    REQUIRE(store.size() == 0);
    REQUIRE(store.used_memory() == 0);
}

TEST_CASE("allkeys-lru evicts the least recently used key", "[store][lru]") {
    FakeClock clock;
    Store probe = make_store(clock);
    probe.set("k0", "v");
    size_t one_key = probe.used_memory();

    // Room for exactly three keys.
    Store store = make_store(clock, 3 * one_key, EvictionPolicy::kAllKeysLru);
    store.set("k0", "v");
    store.set("k1", "v");
    store.set("k2", "v");
    store.get("k0");  // k0 is now most recent; k1 is least recent
    store.set("k3", "v");

    REQUIRE(store.evict_if_needed());
    REQUIRE(store.size() == 3);
    REQUIRE(store.get("k1") == nullptr);
    REQUIRE(store.get("k0") != nullptr);
    REQUIRE(store.evicted_keys() == 1);
}

TEST_CASE("pttl does not count as a use for LRU purposes", "[store][lru]") {
    FakeClock clock;
    Store probe = make_store(clock);
    probe.set("k0", "v");
    size_t one_key = probe.used_memory();

    Store store = make_store(clock, 2 * one_key, EvictionPolicy::kAllKeysLru);
    store.set("k0", "v");
    store.set("k1", "v");
    store.pttl("k0");  // must not refresh k0
    store.set("k2", "v");

    REQUIRE(store.evict_if_needed());
    REQUIRE(store.get("k0") == nullptr);
}

TEST_CASE("noeviction refuses to free memory", "[store][lru]") {
    FakeClock clock;
    Store store = make_store(clock, 1, EvictionPolicy::kNoEviction);
    store.set("k", "v");
    REQUIRE_FALSE(store.evict_if_needed());
    REQUIRE(store.size() == 1);
}

TEST_CASE("maxmemory 0 means unlimited", "[store][lru]") {
    FakeClock clock;
    Store store = make_store(clock, 0, EvictionPolicy::kAllKeysLru);
    for (int i = 0; i < 1000; ++i) {
        store.set("k" + std::to_string(i), std::string(1000, 'x'));
    }
    REQUIRE(store.evict_if_needed());
    REQUIRE(store.size() == 1000);
}

TEST_CASE("passive expiry hides expired keys without deleting them", "[store][ttl][replica]") {
    FakeClock clock;
    Store store = make_store(clock);
    std::vector<std::string> deleted;
    store.set_deletion_listener([&](const std::string& key) { deleted.push_back(key); });
    store.set_passive_expiry(true);

    store.set("k", "v");
    store.set_expire("k", clock.now + 10);
    clock.now += 10;
    REQUIRE(store.get("k") == nullptr);
    REQUIRE_FALSE(store.exists("k"));
    REQUIRE(store.pttl("k") == -2);
    REQUIRE_FALSE(store.persist("k"));
    REQUIRE(store.active_expire_cycle(std::chrono::milliseconds(10)) == 0);
    REQUIRE(store.size() == 1);  // still physically there
    REQUIRE(deleted.empty());    // and nothing was reported as deleted
}

TEST_CASE("passive expiry: a past deadline doesn't delete; DEL still reclaims", "[store][ttl][replica]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set_passive_expiry(true);

    store.set("k", "v");
    REQUIRE(store.set_expire("k", clock.now - 1));
    REQUIRE(store.size() == 1);
    REQUIRE(store.get("k") == nullptr);
    REQUIRE_FALSE(store.del("k"));  // wasn't live...
    REQUIRE(store.size() == 0);     // ...but is gone now
    REQUIRE(store.used_memory() == 0);
}

TEST_CASE("passive expiry: SET over an expired key starts fresh, without the old TTL", "[store][ttl][replica]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set_passive_expiry(true);

    store.set("k", "old");
    store.set_expire("k", clock.now + 10);
    clock.now += 10;
    store.set("k", "new", /*keep_ttl=*/true);
    REQUIRE(*store.get("k") == "new");
    REQUIRE(store.pttl("k") == -1);
    REQUIRE(store.size() == 1);
}

TEST_CASE("turning passive expiry off lets expired keys be reclaimed again", "[store][ttl][replica]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set_passive_expiry(true);
    store.set("k", "v");
    store.set_expire("k", clock.now + 10);
    clock.now += 10;
    REQUIRE(store.get("k") == nullptr);
    REQUIRE(store.size() == 1);

    store.set_passive_expiry(false);
    REQUIRE(store.get("k") == nullptr);
    REQUIRE(store.size() == 0);
}

TEST_CASE("clear drops every key and resets memory accounting", "[store]") {
    FakeClock clock;
    Store store = make_store(clock);
    store.set("a", "1");
    store.set("b", "2");
    store.set_expire("b", clock.now + 100);
    store.clear();
    REQUIRE(store.size() == 0);
    REQUIRE(store.used_memory() == 0);
    REQUIRE(store.get("a") == nullptr);
    store.set("a", "again");
    REQUIRE(*store.get("a") == "again");
    REQUIRE(store.pttl("a") == -1);
}

TEST_CASE("keys are indexed by cluster hash slot", "[store][cluster]") {
    FakeClock clock;
    Store store = make_store(clock);
    // Same hash tag, same slot.
    store.set("{user:1}:name", "a");
    store.set("{user:1}:email", "b");
    store.set("other", "c");
    int slot = cluster::key_hash_slot("{user:1}:name");
    REQUIRE(store.count_keys_in_slot(slot) == 2);
    REQUIRE(store.count_keys_in_slot(cluster::key_hash_slot("other")) == 1);

    std::vector<std::string> keys = store.keys_in_slot(slot, 10);
    std::sort(keys.begin(), keys.end());
    REQUIRE(keys == std::vector<std::string>{"{user:1}:email", "{user:1}:name"});
    REQUIRE(store.keys_in_slot(slot, 1).size() == 1);

    // Overwriting doesn't duplicate; deleting and expiring unindex.
    store.set("{user:1}:name", "a2");
    REQUIRE(store.count_keys_in_slot(slot) == 2);
    store.del("{user:1}:name");
    REQUIRE(store.count_keys_in_slot(slot) == 1);
    store.set_expire("{user:1}:email", clock.now + 10);
    clock.now += 20;
    REQUIRE(store.get("{user:1}:email") == nullptr);
    REQUIRE(store.count_keys_in_slot(slot) == 0);

    store.clear();
    REQUIRE(store.count_keys_in_slot(cluster::key_hash_slot("other")) == 0);
}

TEST_CASE("delete_slot removes every key in the slot and notifies", "[store][cluster]") {
    FakeClock clock;
    Store store = make_store(clock);
    std::vector<std::string> deleted;
    store.set_deletion_listener([&deleted](const std::string& key) { deleted.push_back(key); });
    store.set("{t}a", "1");
    store.set("{t}b", "2");
    store.set("elsewhere", "3");
    int slot = cluster::key_hash_slot("{t}");
    REQUIRE(store.delete_slot(slot) == 2);
    REQUIRE(store.count_keys_in_slot(slot) == 0);
    REQUIRE(store.size() == 1);
    REQUIRE(deleted.size() == 2);
    REQUIRE(*store.get("elsewhere") == "3");
}
