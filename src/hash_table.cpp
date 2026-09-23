#include "hash_table.hpp"

#include <cstdint>
#include <utility>

HashTable::HashTable() : buckets_(kInitialBucketCount) {}

// FNV-1a: cheap to compute, decent avalanche behavior for short ASCII keys.
// Constants are the standard 64-bit FNV offset basis and prime.
size_t HashTable::hash_key(const std::string& key) {
    uint64_t hash = 14695981039346656037ULL;
    for (unsigned char c : key) {
        hash ^= c;
        hash *= 1099511628211ULL;
    }
    return static_cast<size_t>(hash);
}

std::vector<HashTable::Entry>& HashTable::bucket_for(const std::string& key) {
    return buckets_[hash_key(key) % buckets_.size()];
}

const std::vector<HashTable::Entry>& HashTable::bucket_for(const std::string& key) const {
    return buckets_[hash_key(key) % buckets_.size()];
}

void HashTable::set(const std::string& key, const std::string& value) {
    auto& bucket = bucket_for(key);
    for (auto& entry : bucket) {
        if (entry.key == key) {
            entry.value = value;
            return;
        }
    }
    bucket.push_back({key, value});
    ++num_entries_;
    maybe_resize();
}

std::optional<std::string> HashTable::get(const std::string& key) const {
    const auto& bucket = bucket_for(key);
    for (const auto& entry : bucket) {
        if (entry.key == key) {
            return entry.value;
        }
    }
    return std::nullopt;
}

bool HashTable::contains(const std::string& key) const {
    const auto& bucket = bucket_for(key);
    for (const auto& entry : bucket) {
        if (entry.key == key) {
            return true;
        }
    }
    return false;
}

bool HashTable::del(const std::string& key) {
    auto& bucket = bucket_for(key);
    for (auto it = bucket.begin(); it != bucket.end(); ++it) {
        if (it->key == key) {
            bucket.erase(it);
            --num_entries_;
            return true;
        }
    }
    return false;
}

size_t HashTable::size() const {
    return num_entries_;
}

void HashTable::maybe_resize() {
    if (static_cast<double>(num_entries_) / buckets_.size() <= kMaxLoadFactor) {
        return;
    }

    std::vector<std::vector<Entry>> new_buckets(buckets_.size() * 2);
    for (auto& bucket : buckets_) {
        for (auto& entry : bucket) {
            new_buckets[hash_key(entry.key) % new_buckets.size()].push_back(std::move(entry));
        }
    }
    buckets_ = std::move(new_buckets);
}
