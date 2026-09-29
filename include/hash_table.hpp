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
// Resizing is incremental, as in Redis: a resize allocates the new bucket
// array and then moves the old buckets over a few at a time — one per set()
// or del(), plus whatever rehash_step() does when the owner calls it from a
// timer. Moving everything at once would stall the caller for time
// proportional to the table's size (~1s at 3M keys). While a resize is in
// progress both arrays exist: old buckets below rehash_idx_ have already
// been moved, so a key's bucket is in the old array if its old index is
// >= rehash_idx_ and in the new array otherwise, and every lookup still
// probes exactly one bucket.
//
// Pointers returned by find()/random_entry() are invalidated by the next
// set(), del() or rehash_step(), since any of them may move entries between
// buckets. find() never moves anything (unlike Redis, whose lookups also
// advance a resize), so pointers stay valid across reads.
template <typename V>
class HashTable {
public:
    HashTable() { tables_[0].resize(kInitialBucketCount); }

    // Inserts or overwrites. Returns true if the key was newly inserted.
    bool set(const std::string& key, V value) {
        rehash_step(1);
        auto& bucket = bucket_for(key);
        for (auto& entry : bucket) {
            if (entry.key == key) {
                entry.value = std::move(value);
                return false;
            }
        }
        bucket.push_back({key, std::move(value)});
        ++num_entries_;
        maybe_resize();
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
        rehash_step(1);
        auto& bucket = bucket_for(key);
        for (auto it = bucket.begin(); it != bucket.end(); ++it) {
            if (it->key == key) {
                bucket.erase(it);
                --num_entries_;
                maybe_resize();
                return true;
            }
        }
        return false;
    }

    // Random entry, for sampling-based algorithms (active expiry). Picks a
    // random non-empty bucket, then a random entry within it — O(1) expected
    // because the shrink threshold bounds how sparse the table can get. Not
    // perfectly uniform: keys in short chains are slightly favored, the same
    // trade-off Redis's dictGetRandomKey() makes. During a resize, buckets
    // are drawn from both arrays (already-moved old buckets are empty and
    // just get skipped).
    // Returns {nullptr, nullptr} if the table is empty.
    template <typename Rng>
    std::pair<const std::string*, V*> random_entry(Rng& rng) {
        if (num_entries_ == 0) {
            return {nullptr, nullptr};
        }
        size_t first = rehashing() ? rehash_idx_ : 0;  // old buckets below this are empty
        size_t live_old = tables_[0].size() - first;
        std::uniform_int_distribution<size_t> pick_bucket(0, live_old + tables_[1].size() - 1);
        while (true) {
            size_t i = pick_bucket(rng);
            auto& bucket = i < live_old ? tables_[0][first + i] : tables_[1][i - live_old];
            if (bucket.empty()) {
                continue;
            }
            std::uniform_int_distribution<size_t> pick_entry(0, bucket.size() - 1);
            Entry& entry = bucket[pick_entry(rng)];
            return {&entry.key, &entry.value};
        }
    }

    // Calls fn(key, value) for every entry, in bucket order. fn must not
    // modify the table.
    template <typename F>
    void for_each(F&& fn) const {
        for (const auto& table : tables_) {
            for (const auto& bucket : table) {
                for (const auto& entry : bucket) {
                    fn(entry.key, entry.value);
                }
            }
        }
    }

    // Moves up to `n` old buckets into the new array if a resize is in
    // progress. Visits at most 10 empty buckets per bucket asked for, so a
    // sparse table can't make one step scan millions of buckets (Redis's
    // dictRehash() uses the same bound). Returns true while a resize is
    // still in progress.
    bool rehash_step(size_t n) {
        if (!rehashing()) {
            return false;
        }
        auto& from = tables_[0];
        auto& to = tables_[1];
        size_t empty_visits = n * 10;
        while (n > 0 && rehash_idx_ < from.size()) {
            auto& bucket = from[rehash_idx_];
            if (bucket.empty()) {
                ++rehash_idx_;
                if (--empty_visits == 0) {
                    break;
                }
                continue;
            }
            for (auto& entry : bucket) {
                to[hash_key(entry.key) % to.size()].push_back(std::move(entry));
            }
            std::vector<Entry>().swap(bucket);  // clear() would keep the capacity allocated
            ++rehash_idx_;
            --n;
        }
        if (rehash_idx_ == from.size()) {
            from = std::move(to);
            to = {};
            rehash_idx_ = 0;
            maybe_resize();  // a burst of writes may already call for the next resize
        }
        return rehashing();
    }

    bool rehashing() const { return !tables_[1].empty(); }
    size_t size() const { return num_entries_; }
    // The bucket count the table is settling on: the new array's size while
    // a resize is in progress.
    size_t bucket_count() const { return rehashing() ? tables_[1].size() : tables_[0].size(); }

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
        return const_cast<std::vector<Entry>&>(std::as_const(*this).bucket_for(key));
    }

    const std::vector<Entry>& bucket_for(const std::string& key) const {
        size_t hash = hash_key(key);
        size_t i = hash % tables_[0].size();
        if (rehashing() && i < rehash_idx_) {
            return tables_[1][hash % tables_[1].size()];
        }
        return tables_[0][i];
    }

    // Starts a resize if the load factor has left [0.1, 0.75]. Never while
    // one is already in progress: every set()/del() moves at least one old
    // bucket, so a resize finishes within tables_[0].size() writes, and a
    // grow started at load 0.75 ends below load 0.9 even if every one of
    // those writes was an insert.
    //
    // Without shrinking, a table that once held 1M keys keeps 1M+ buckets
    // forever — wasted memory, and random_entry() would probe mostly empty
    // buckets once the keys are gone.
    void maybe_resize() {
        if (rehashing()) {
            return;
        }
        size_t buckets = tables_[0].size();
        double load = static_cast<double>(num_entries_) / buckets;
        if (load > kMaxLoadFactor) {
            tables_[1].resize(buckets * 2);
        } else if (buckets > kInitialBucketCount && load < kMinLoadFactor) {
            tables_[1].resize(buckets / 2);
        }
    }

    // tables_[0] holds the entries; during a resize, tables_[1] is the new
    // bucket array and old buckets [0, rehash_idx_) have been moved into it.
    std::vector<std::vector<Entry>> tables_[2];
    size_t rehash_idx_ = 0;
    size_t num_entries_ = 0;
};
