// HashTable find() and set() timed in isolation, with no server around them.
// hashbench.py compiles this once per hash_table.hpp version (picked with
// -I), so two binaries differ only in the header.
//
// Usage: hashbench <keys> <lookups>
// Prints: <keys> <ns per find> <ns per insert> <checksum>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <random>
#include <string>
#include <vector>

#include "hash_table.hpp"

static double now_ns() {
    return std::chrono::duration<double, std::nano>(std::chrono::steady_clock::now().time_since_epoch()).count();
}

int main(int argc, char** argv) {
    if (argc != 3) {
        std::fprintf(stderr, "usage: %s <keys> <lookups>\n", argv[0]);
        return 2;
    }
    size_t n = std::strtoull(argv[1], nullptr, 10);
    size_t lookups = std::strtoull(argv[2], nullptr, 10);

    std::vector<std::string> keys(n);
    for (size_t i = 0; i < n; ++i) {
        char buf[32];
        std::snprintf(buf, sizeof buf, "key:%08zu", i);  // 12 bytes, as in memory.py
        keys[i] = buf;
    }

    // Inserts include the resizes they trigger, as they do in the server.
    HashTable<int> table;
    double t0 = now_ns();
    for (size_t i = 0; i < n; ++i) {
        table.set(keys[i], static_cast<int>(i));
    }
    double insert_ns = (now_ns() - t0) / n;
    while (table.rehash_step(1000)) {
    }

    // Random order, generated up front so the RNG isn't timed.
    std::mt19937_64 rng(42);
    std::vector<const std::string*> order(lookups);
    for (auto& key : order) {
        key = &keys[rng() % n];
    }

    // The checksum keeps the loop from being optimized away, and lets the
    // driver check that both versions found the same values.
    long long checksum = 0;
    t0 = now_ns();
    for (const std::string* key : order) {
        checksum += *table.find(*key);
    }
    double find_ns = (now_ns() - t0) / lookups;

    std::printf("%zu %.2f %.2f %lld\n", n, find_ns, insert_ns, checksum);
}
