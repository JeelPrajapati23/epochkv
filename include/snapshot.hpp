#pragma once

#include <string>

#include "store.hpp"

// Point-in-time binary dump of the keyspace — this project's equivalent of
// Redis's RDB file. Used for SAVE/BGSAVE and as the base file of the AOF.
//
// Layout (fixed-width integers little-endian, lengths as LEB128 varints):
//
//   "KVSNAP" <version:u8>
//   per key:  [0xFC <expire_unix_ms:i64>] 0x00 <keylen> <key> <vallen> <value>
//   0xFF <crc32:u32>        CRC-32 (IEEE) of every byte before it
//
// Expiry is stored as an absolute wall-clock time, so a key saved with 10s
// left and loaded 60s later is correctly treated as expired.
namespace snapshot {

// Writes the store to `path` (created or truncated) and fsyncs it. Keys
// already past their expiry are skipped. Returns false on I/O error (errno
// set). Safe in a forked child: it touches nothing but the store and the file.
bool write_file(const Store& store, const std::string& path);

// write_file() to a temp file in the same directory, then durably rename it
// over `path`. A crash mid-save leaves the previous snapshot intact instead
// of a torn one, because rename() replaces the file atomically.
bool save(const Store& store, const std::string& path);

enum class LoadResult { kOk, kNotFound, kError };

// Adds every key in the file to `store`, skipping keys that expired while
// the server was down. kError (with a message) on I/O failure, bad magic,
// truncation or checksum mismatch; the caller should refuse to start rather
// than serve a dataset it knows is damaged.
LoadResult load(const std::string& path, Store& store, std::string& error);

}  // namespace snapshot
