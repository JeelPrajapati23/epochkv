#pragma once

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <new>
#include <optional>
#include <random>
#include <string>
#include <utility>

// Separate-chaining hash table from string keys to V, with FNV-1a hashing.
// Grows (doubles) above load factor 0.75 and shrinks (halves) below 0.1; the
// gap between the two thresholds keeps a table hovering near one boundary
// from rehashing back and forth.
//
// Each bucket is the head pointer of a singly linked chain of nodes, as in
// Redis's dict. An empty bucket costs 8 bytes, and the bucket array comes
// from calloc(), which gets large blocks as fresh zero pages from the OS:
// nothing is written up front, so allocating a resize's new array is ~free
// instead of zero-filling every bucket (98ms at 8M buckets with
// std::vector<std::vector<Entry>>).
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
// Pointers returned by find()/random_entry() stay valid until that entry is
// deleted: moving a bucket relinks its nodes rather than copying them, so a
// node never changes address. find() also never advances a resize (unlike
// Redis, whose lookups do), so reads never change the table at all.
template <typename V>
class HashTable {
public:
    HashTable() { tables_[0] = make_table(kInitialBucketCount); }

    ~HashTable() {
        for (Table& table : tables_) {
            for (size_t i = 0; i < table.size; ++i) {
                Node* node = table.buckets[i];
                while (node != nullptr) {
                    Node* next = node->next;
                    delete node;
                    node = next;
                }
            }
        }
    }

    // Copying would need a deep copy of every chain; nothing needs it, and
    // the default memberwise copy would make two tables free the same nodes.
    HashTable(const HashTable&) = delete;
    HashTable& operator=(const HashTable&) = delete;

    // The moved-from table is left as a fresh empty one, still usable.
    HashTable(HashTable&& other) : HashTable() { swap(other); }

    // Swapping hands our old nodes to `other`, whose destructor frees them.
    HashTable& operator=(HashTable&& other) noexcept {
        swap(other);
        return *this;
    }

    void swap(HashTable& other) noexcept {
        std::swap(tables_[0], other.tables_[0]);
        std::swap(tables_[1], other.tables_[1]);
        std::swap(rehash_idx_, other.rehash_idx_);
        std::swap(num_entries_, other.num_entries_);
    }

    // Inserts or overwrites. Returns true if the key was newly inserted.
    bool set(const std::string& key, V value) {
        rehash_step(1);
        Node*& head = bucket_for(key);
        for (Node* node = head; node != nullptr; node = node->next) {
            if (node->key == key) {
                node->value = std::move(value);
                return false;
            }
        }
        // At the head: O(1), and a key just written is likely to be read soon.
        head = new Node{key, std::move(value), head};
        ++num_entries_;
        maybe_resize();
        return true;
    }

    V* find(const std::string& key) {
        return const_cast<V*>(std::as_const(*this).find(key));
    }

    const V* find(const std::string& key) const {
        for (const Node* node = bucket_for(key); node != nullptr; node = node->next) {
            if (node->key == key) {
                return &node->value;
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
        // `link` points at whichever pointer leads to the current node — the
        // bucket's head or the previous node's next — so unlinking the first
        // node needs no special case.
        for (Node** link = &bucket_for(key); *link != nullptr; link = &(*link)->next) {
            Node* node = *link;
            if (node->key == key) {
                *link = node->next;
                delete node;
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
        size_t live_old = tables_[0].size - first;
        std::uniform_int_distribution<size_t> pick_bucket(0, live_old + tables_[1].size - 1);
        while (true) {
            size_t i = pick_bucket(rng);
            Node* head = i < live_old ? tables_[0].buckets[first + i] : tables_[1].buckets[i - live_old];
            if (head == nullptr) {
                continue;
            }
            size_t length = 0;
            for (Node* node = head; node != nullptr; node = node->next) {
                ++length;
            }
            std::uniform_int_distribution<size_t> pick_entry(0, length - 1);
            Node* node = head;
            for (size_t k = pick_entry(rng); k > 0; --k) {
                node = node->next;
            }
            return {&node->key, &node->value};
        }
    }

    // Calls fn(key, value) for every entry, in bucket order. fn must not
    // modify the table.
    template <typename F>
    void for_each(F&& fn) const {
        for (const Table& table : tables_) {
            for (size_t i = 0; i < table.size; ++i) {
                for (const Node* node = table.buckets[i]; node != nullptr; node = node->next) {
                    fn(node->key, node->value);
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
        Table& from = tables_[0];
        Table& to = tables_[1];
        size_t empty_visits = n * 10;
        while (n > 0 && rehash_idx_ < from.size) {
            Node*& head = from.buckets[rehash_idx_];
            if (head == nullptr) {
                ++rehash_idx_;
                if (--empty_visits == 0) {
                    break;
                }
                continue;
            }
            // Relink each node onto the head of its new bucket: no
            // allocation, and no key or value is copied or moved.
            Node* node = head;
            while (node != nullptr) {
                Node* next = node->next;
                Node*& dest = to.buckets[bucket_index(hash_key(node->key), to.size)];
                node->next = dest;
                dest = node;
                node = next;
            }
            head = nullptr;
            ++rehash_idx_;
            --n;
        }
        if (rehash_idx_ == from.size) {
            from = std::move(to);  // frees the old array; all its buckets are empty
            to = Table{};
            rehash_idx_ = 0;
            maybe_resize();  // a burst of writes may already call for the next resize
        }
        return rehashing();
    }

    bool rehashing() const { return tables_[1].size != 0; }
    size_t size() const { return num_entries_; }
    // The bucket count the table is settling on: the new array's size while
    // a resize is in progress.
    size_t bucket_count() const { return rehashing() ? tables_[1].size : tables_[0].size; }

private:
    struct Node {
        std::string key;
        V value;
        Node* next;
    };

    struct FreeDeleter {
        void operator()(Node** buckets) const { std::free(buckets); }
    };

    struct Table {
        std::unique_ptr<Node*[], FreeDeleter> buckets;
        size_t size = 0;
    };

    // Bucket counts start here and only ever double or halve (never below
    // this), so every bucket count is a power of two; bucket_index() relies
    // on it.
    static constexpr size_t kInitialBucketCount = 16;
    static_assert((kInitialBucketCount & (kInitialBucketCount - 1)) == 0, "must be a power of two");
    static constexpr double kMaxLoadFactor = 0.75;
    static constexpr double kMinLoadFactor = 0.1;

    // hash % size, as a mask: for a power-of-two size the two pick the same
    // bucket, but the compiler can't know size is one, so % costs a 64-bit
    // division (tens of cycles) where & costs one.
    static size_t bucket_index(size_t hash, size_t size) { return hash & (size - 1); }

    // calloc rather than new Node*[n]() or a vector: those write zeros into
    // every bucket up front, while calloc can skip that for fresh pages from
    // the OS, which are already zero. Relies on all-zero bits being nullptr,
    // which holds on every platform this builds for (Redis relies on it too).
    static Table make_table(size_t size) {
        Node** buckets = static_cast<Node**>(std::calloc(size, sizeof(Node*)));
        if (buckets == nullptr) {
            throw std::bad_alloc();
        }
        Table table;
        table.buckets.reset(buckets);
        table.size = size;
        return table;
    }

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

    Node*& bucket_for(const std::string& key) {
        return const_cast<Node*&>(std::as_const(*this).bucket_for(key));
    }

    Node* const& bucket_for(const std::string& key) const {
        size_t hash = hash_key(key);
        size_t i = bucket_index(hash, tables_[0].size);
        if (rehashing() && i < rehash_idx_) {
            return tables_[1].buckets[bucket_index(hash, tables_[1].size)];
        }
        return tables_[0].buckets[i];
    }

    // Starts a resize if the load factor has left [0.1, 0.75]. Never while
    // one is already in progress: every set()/del() moves at least one old
    // bucket, so a resize finishes within tables_[0].size writes, and a
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
        size_t buckets = tables_[0].size;
        double load = static_cast<double>(num_entries_) / buckets;
        if (load > kMaxLoadFactor) {
            tables_[1] = make_table(buckets * 2);
        } else if (buckets > kInitialBucketCount && load < kMinLoadFactor) {
            tables_[1] = make_table(buckets / 2);
        }
    }

    // tables_[0] holds the entries; during a resize, tables_[1] is the new
    // bucket array and old buckets [0, rehash_idx_) have been moved into it.
    Table tables_[2];
    size_t rehash_idx_ = 0;
    size_t num_entries_ = 0;
};
