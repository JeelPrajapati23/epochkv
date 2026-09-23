#include "store.hpp"

#include <algorithm>
#include <utility>

namespace {

// Rough per-key bookkeeping cost on top of the key/value bytes: the bucket
// entry, the LRU list node (which holds a second copy of the key), and
// allocator overhead. Redis measures real allocator usage instead; an
// estimate is enough to make maxmemory meaningful here.
constexpr size_t kEntryOverhead = 128;
constexpr size_t kExpireOverhead = 64;

// Redis's ACTIVE_EXPIRE_CYCLE_KEYS_PER_LOOP.
constexpr size_t kExpireSamplesPerLoop = 20;

size_t entry_cost(const std::string& key, size_t value_size) {
    return kEntryOverhead + 2 * key.size() + value_size;
}

size_t expire_cost(const std::string& key) {
    return kExpireOverhead + key.size();
}

}  // namespace

int64_t Store::system_now_ms() {
    // Wall clock, not steady_clock: expiry times are absolute timestamps that
    // will be written to disk and must still mean the same instant after a
    // restart. (Scheduling inside the event loop uses steady_clock instead.)
    using namespace std::chrono;
    return duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
}

Store::Store() : Store(Options{}) {}

Store::Store(Options options) : options_(std::move(options)), rng_(std::random_device{}()) {}

bool Store::expire_if_needed(const std::string& key) {
    if (expires_.size() == 0) {
        return false;  // fast path: no key has a TTL
    }
    const int64_t* when = expires_.find(key);
    if (when == nullptr || *when > now_ms()) {
        return false;
    }
    remove(key);
    ++expired_keys_;
    return true;
}

Store::Entry* Store::lookup(const std::string& key, bool touch) {
    expire_if_needed(key);
    Entry* entry = dict_.find(key);
    if (entry != nullptr && touch) {
        // splice relinks the existing node: O(1), no allocation, and every
        // iterator (including entry->lru_pos) stays valid.
        lru_.splice(lru_.begin(), lru_, entry->lru_pos);
    }
    return entry;
}

const std::string* Store::get(const std::string& key) {
    Entry* entry = lookup(key, true);
    return entry != nullptr ? &entry->value : nullptr;
}

bool Store::exists(const std::string& key) {
    return lookup(key, true) != nullptr;
}

void Store::set(const std::string& key, std::string value, bool keep_ttl) {
    // lookup() first, so an expired key is replaced as a brand-new key and
    // KEEPTTL can't resurrect its stale expiry.
    if (Entry* entry = lookup(key, true)) {
        used_memory_ = used_memory_ - entry->value.size() + value.size();
        entry->value = std::move(value);
        if (!keep_ttl) {
            clear_expire(key);
        }
        return;
    }
    used_memory_ += entry_cost(key, value.size());
    lru_.push_front(key);
    dict_.set(key, Entry{std::move(value), lru_.begin()});
}

bool Store::del(const std::string& key) {
    return lookup(key, false) != nullptr && remove(key);
}

bool Store::set_expire(const std::string& key, int64_t when_ms) {
    if (lookup(key, true) == nullptr) {
        return false;
    }
    if (when_ms <= now_ms()) {
        remove(key);
        return true;
    }
    if (int64_t* existing = expires_.find(key)) {
        *existing = when_ms;
    } else {
        expires_.set(key, when_ms);
        used_memory_ += expire_cost(key);
    }
    return true;
}

bool Store::persist(const std::string& key) {
    return lookup(key, true) != nullptr && clear_expire(key);
}

int64_t Store::pttl(const std::string& key) {
    // No touch: inspecting a TTL isn't a "use" of the key (Redis: LOOKUP_NOTOUCH).
    if (lookup(key, false) == nullptr) {
        return -2;
    }
    const int64_t* when = expires_.find(key);
    if (when == nullptr) {
        return -1;
    }
    return *when - now_ms();  // > 0: lookup() already removed it if expired
}

bool Store::evict_if_needed() {
    if (options_.maxmemory == 0) {
        return true;
    }
    while (used_memory_ > options_.maxmemory) {
        if (options_.policy == EvictionPolicy::kNoEviction || lru_.empty()) {
            return false;
        }
        std::string victim = lru_.back();  // copy: remove() frees the node
        remove(victim);
        ++evicted_keys_;
    }
    return true;
}

size_t Store::active_expire_cycle(std::chrono::microseconds budget) {
    auto deadline = std::chrono::steady_clock::now() + budget;
    size_t total_expired = 0;

    while (expires_.size() > 0) {
        size_t sampled = std::min(kExpireSamplesPerLoop, expires_.size());
        size_t expired = 0;
        int64_t now = now_ms();

        for (size_t i = 0; i < sampled && expires_.size() > 0; ++i) {
            auto [key, when] = expires_.random_entry(rng_);
            if (*when <= now) {
                std::string victim = *key;  // copy: remove() frees the entry
                remove(victim);
                ++expired;
            }
        }
        total_expired += expired;

        // If at most 25% of the sample was expired, expired keys are probably
        // rare right now — stop and leave the CPU to clients. Otherwise keep
        // going, bounded by the time budget.
        if (expired * 4 <= sampled || std::chrono::steady_clock::now() >= deadline) {
            break;
        }
    }

    expired_keys_ += total_expired;
    return total_expired;
}

bool Store::remove(const std::string& key) {
    Entry* entry = dict_.find(key);
    if (entry == nullptr) {
        return false;
    }
    used_memory_ -= entry_cost(key, entry->value.size());
    lru_.erase(entry->lru_pos);
    dict_.del(key);
    clear_expire(key);
    return true;
}

bool Store::clear_expire(const std::string& key) {
    if (!expires_.del(key)) {
        return false;
    }
    used_memory_ -= expire_cost(key);
    return true;
}
