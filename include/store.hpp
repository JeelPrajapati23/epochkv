#pragma once

#include <chrono>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <list>
#include <random>
#include <string>
#include <vector>

#include "cluster_slot.hpp"
#include "hash_table.hpp"

enum class EvictionPolicy {
    kNoEviction,  // over maxmemory: reject writes with -OOM (Redis's default)
    kAllKeysLru,  // over maxmemory: evict least-recently-used keys
};

// The keyspace: values plus per-key expiry and LRU order.
//
//   dict_     key -> {value, position in lru_}
//   expires_  key -> absolute expiry (unix ms); only keys that have a TTL
//   lru_      keys, most recently used at the front
//   slots_    per cluster hash slot, the keys in it (pointers to the key
//             strings inside lru_ nodes, which never move)
//
// Expired keys are removed lazily (any access to an expired key deletes it
// first) and actively (active_expire_cycle() samples keys with a TTL), so
// keys that are never read again still get reclaimed.
class Store {
public:
    using Clock = std::function<int64_t()>;  // current unix time in ms
    // Told about keys the store deletes on its own (expiry, eviction), as
    // opposed to deletes a command asked for. Persistence logs these as DEL
    // so a replay can't bring back a key that is already gone here.
    using DeletionListener = std::function<void(const std::string& key)>;

    struct Options {
        size_t maxmemory = 0;  // bytes; 0 = unlimited
        EvictionPolicy policy = EvictionPolicy::kNoEviction;
        Clock clock = system_now_ms;
    };

    static int64_t system_now_ms();

    Store();
    explicit Store(Options options);

    // Entries hold iterators into lru_; a copied Store would point into the
    // original's list.
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    // nullptr if the key is missing or expired. The pointer is valid until
    // the next write to the store.
    const std::string* get(const std::string& key);
    bool exists(const std::string& key);
    // exists() without counting as a use of the key (no LRU touch): for
    // checks the server makes on its own behalf, like cluster routing.
    bool contains(const std::string& key) { return lookup(key, false) != nullptr; }
    // Overwrites any existing value. Clears an existing TTL unless keep_ttl.
    void set(const std::string& key, std::string value, bool keep_ttl = false);
    bool del(const std::string& key);

    // Sets the key's absolute expiry. Returns false if the key doesn't exist.
    // A time at or before now deletes the key immediately.
    bool set_expire(const std::string& key, int64_t when_ms);
    // Removes the key's TTL. Returns false if the key doesn't exist or had none.
    bool persist(const std::string& key);
    // Remaining TTL in ms; -2 if the key doesn't exist, -1 if it has no TTL.
    int64_t pttl(const std::string& key);

    // Evicts keys until used memory is within maxmemory. Returns false if the
    // store is still over the limit (noeviction policy, or nothing to evict).
    bool evict_if_needed();

    // One round of active expiry: sample keys that have a TTL and delete the
    // expired ones, repeating while samples keep coming back mostly expired
    // and the time budget allows. Returns how many keys were deleted.
    size_t active_expire_cycle(std::chrono::microseconds budget);

    void set_deletion_listener(DeletionListener listener) { deletion_listener_ = std::move(listener); }

    // Passive expiry (on for replicas): expired keys read as missing but are
    // never deleted here — not on access, not by active_expire_cycle(), not
    // by set_expire() with a past time. The master decides when a key is
    // gone and sends a DEL; deleting on the replica's own clock would let
    // the two diverge whenever their clocks disagree.
    void set_passive_expiry(bool on) { passive_expiry_ = on; }
    bool passive_expiry() const { return passive_expiry_; }

    // Drops every key (a replica about to load its master's snapshot).
    // Doesn't notify the deletion listener.
    void clear();

    // Calls fn(key, value, expire_ms) for every key; expire_ms is -1 if the
    // key has no TTL. Includes expired keys not yet reclaimed. Read-only, so
    // it's safe in a forked child walking its copy-on-write view of the store.
    template <typename F>
    void for_each(F&& fn) const {
        dict_.for_each([&](const std::string& key, const Entry& entry) {
            const int64_t* when = expires_.find(key);
            fn(key, entry.value, when != nullptr ? *when : int64_t{-1});
        });
    }

    // Keys per cluster hash slot, including expired ones not yet reclaimed
    // (Redis's CLUSTER COUNTKEYSINSLOT / GETKEYSINSLOT have the same view).
    // Kept whether or not cluster mode is on: it's one list node per key,
    // and moving a slot to another node needs its keys without a full scan.
    size_t count_keys_in_slot(int slot) const { return slots_[slot].size(); }
    std::vector<std::string> keys_in_slot(int slot, size_t max) const;
    // Deletes every key in `slot` (this node lost the slot to another one).
    // The deletion listener is told, so the DELs reach the AOF and replicas.
    // Returns how many keys were deleted.
    size_t delete_slot(int slot);

    int64_t now_ms() const { return options_.clock(); }
    size_t size() const { return dict_.size(); }
    size_t used_memory() const { return used_memory_; }
    size_t expired_keys() const { return expired_keys_; }
    size_t evicted_keys() const { return evicted_keys_; }

private:
    struct Entry {
        std::string value;
        std::list<std::string>::iterator lru_pos;
        std::list<const std::string*>::iterator slot_pos;
    };

    // Lazily expires `key`, then returns its entry (nullptr if absent or
    // expired).
    // touch = move to the front of the LRU list.
    Entry* lookup(const std::string& key, bool touch);
    // True if `key` has expired; also deletes it unless expiry is passive.
    bool expire_if_needed(const std::string& key);
    // Deletes the key from every structure. `key` must not refer to storage
    // inside the store itself (e.g. an lru_ node) — pass a copy.
    bool remove(const std::string& key);
    bool clear_expire(const std::string& key);
    void notify_deleted(const std::string& key);

    Options options_;
    DeletionListener deletion_listener_;
    HashTable<Entry> dict_;
    HashTable<int64_t> expires_;
    std::list<std::string> lru_;
    std::vector<std::list<const std::string*>> slots_;
    std::mt19937_64 rng_;
    bool passive_expiry_ = false;

    size_t used_memory_ = 0;
    size_t expired_keys_ = 0;
    size_t evicted_keys_ = 0;
};
