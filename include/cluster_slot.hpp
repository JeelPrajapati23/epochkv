#pragma once

#include <cstddef>
#include <cstdint>
#include <string_view>

// Key -> hash slot mapping, identical to Redis Cluster's, so any Redis
// cluster client computes the same slot for a key as the server does.
//
// The keyspace is split into a fixed 16384 slots, and nodes own slots
// rather than individual keys. Adding a node means moving some slots to it;
// no key's slot ever changes.
namespace cluster {

constexpr int kSlots = 16384;

// CRC-16/XMODEM (poly 0x1021, init 0): the variant Redis Cluster uses.
uint16_t crc16(const char* data, size_t len);

// CRC16(key) mod 16384, except that if the key contains a non-empty "{tag}",
// only the tag is hashed. "{user:1}:name" and "{user:1}:email" therefore
// land in the same slot, so multi-key commands can use both. Only the first
// '{' and the first '}' after it count: "a{}{b}" has an empty first tag, so
// the whole key is hashed.
int key_hash_slot(std::string_view key);

}  // namespace cluster
