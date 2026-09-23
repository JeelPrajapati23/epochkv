#pragma once

#include <cstddef>
#include <optional>
#include <string>
#include <vector>

class HashTable {
public:
    HashTable();

    void set(const std::string& key, const std::string& value);
    std::optional<std::string> get(const std::string& key) const;
    bool contains(const std::string& key) const;
    bool del(const std::string& key);
    size_t size() const;

private:
    struct Entry {
        std::string key;
        std::string value;
    };

    static constexpr size_t kInitialBucketCount = 16;
    static constexpr double kMaxLoadFactor = 0.75;

    static size_t hash_key(const std::string& key);
    std::vector<Entry>& bucket_for(const std::string& key);
    const std::vector<Entry>& bucket_for(const std::string& key) const;
    void maybe_resize();

    std::vector<std::vector<Entry>> buckets_;
    size_t num_entries_ = 0;
};
