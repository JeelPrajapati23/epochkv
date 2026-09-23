#pragma once

#include <cstddef>
#include <cstdint>
#include <optional>
#include <random>
#include <string>
#include <utility>
#include <vector>

// Separate-chaining hash table from string keys to V, with FNV-1a hashing.
// Grows (doubles) above load factor 0.75 and shrinks (halves) below 0.1; the
// gap between the two thresholds keeps a table hovering near one boundary
// from rehashing back and forth.
//
// Pointers returned by find()/random_entry() are invalidated by the next
// set() or del(), since either may rehash and move entries between buckets.
template <typename V>
class HashTable {
public:
    HashTable() : buckets_(kInitialBucketCount) {}

    // Inserts or overwrites. Returns true if the key was newly inserted.
    bool set(const std::string& key, V value) {
        auto& bucket = bucket_for(key);
        for (auto& entry : bucket) {
            if (entry.key == key) {
                entry.value = std::move(value);
                return false;
            }
        }
        bucket.push_back({key, std::move(value)});
        ++num_entries_;
        maybe_grow();
        return true;
    }

    V* find(const std::string& key) {
        for (auto& entry : bucket_for(key)) {
            if (entry.key == key) {
                return &entry.value;
            }
        }
        return nullptr;
    }

    const V* find(const std::string& key) const {
        for (const auto& entry : bucket_for(key)) {
            if (entry.key == key) {
                return &entry.value;
            }
        }
        return nullptr;
    }

    std::optional<V> get(const std::string& key) const {
        if (const V* value = find(key)) {
            return *value;
        }
        return std::nullopt;
    }

    bool contains(const std::string& key) const {
        return find(key) != nullptr;
    }

    bool del(const std::string& key) {
        auto& bucket = bucket_for(key);
        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (it->key == key) {
                bucket.erase(it);
                --num_entries_;
                maybe_shrink();
                return true;
            }
        }
        return false;
    }

    // Random entry, for sampling-based algorithms (active expiry). Picks a
    // random non-empty bucket, then a random entry within it — O(1) expected
    // because the shrink threshold bounds how sparse the table can get. Not
    // perfectly uniform: keys in short chains are slightly favored, the same
    // trade-off Redis's dictGetRandomKey() makes.
    // Returns {nullptr, nullptr} if the table is empty.
    template <typename Rng>
    std::pair<const std::string*, V*> random_entry(Rng& rng) {
        if (num_entries_ == 0) {
            return {nullptr, nullptr};
        }
        std::uniform_int_distribution<size_t> pick_bucket(0, buckets_.size() - 1);
        while (true) {
            auto& bucket = buckets_[pick_bucket(rng)];
            if (bucket.empty()) {
                continue;
            }
            std::uniform_int_distribution<size_t> pick_entry(0, bucket.size() - 1);
            Entry& entry = bucket[pick_entry(rng)];
            return {&entry.key, &entry.value};
        }
    }

    size_t size() const { return num_entries_; }
    size_t bucket_count() const { return buckets_.size(); }

private:
    struct Entry {
        std::string key;
        V value;
    };

    static constexpr size_t kInitialBucketCount = 16;
    static constexpr double kMaxLoadFactor = 0.75;
    static constexpr double kMinLoadFactor = 0.1;

    // FNV-1a: cheap to compute, decent avalanche behavior for short ASCII keys.
    // Constants are the standard 64-bit FNV offset basis and prime.
    static size_t hash_key(const std::string& key) {
        uint64_t hash = 14695981039346656037ULL;
        for (unsigned char c : key) {
            hash ^= c;
            hash *= 1099511628211ULL;
        }
        return static_cast<size_t>(hash);
    }

    std::vector<Entry>& bucket_for(const std::string& key) {
        return buckets_[hash_key(key) % buckets_.size()];
    }

    const std::vector<Entry>& bucket_for(const std::string& key) const {
        return buckets_[hash_key(key) % buckets_.size()];
    }

    void maybe_grow() {
        if (static_cast<double>(num_entries_) / buckets_.size() > kMaxLoadFactor) {
            rehash(buckets_.size() * 2);
        }
    }

    // Without shrinking, a table that once held 1M keys keeps 1M+ buckets
    // forever — wasted memory, and random_entry() would probe mostly empty
    // buckets once the keys are gone.
    void maybe_shrink() {
        if (buckets_.size() > kInitialBucketCount &&
            static_cast<double>(num_entries_) / buckets_.size() < kMinLoadFactor) {
            rehash(buckets_.size() / 2);
        }
    }

    void rehash(size_t new_bucket_count) {
        std::vector<std::vector<Entry>> new_buckets(new_bucket_count);
        for (auto& bucket : buckets_) {
            for (auto& entry : bucket) {
                new_buckets[hash_key(entry.key) % new_bucket_count].push_back(std::move(entry));
            }
        }
        buckets_ = std::move(new_buckets);
    }

    std::vector<std::vector<Entry>> buckets_;
    size_t num_entries_ = 0;
};
