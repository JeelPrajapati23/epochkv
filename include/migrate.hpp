#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <vector>

// The client side of MIGRATE: ships RESTORE-ASKING commands to the target
// node and collects the replies.
//
// This is deliberately *blocking*, as in Redis: MIGRATE is atomic from the
// point of view of every other client. While a key is in flight, no one
// can modify it on the source (the event loop is busy here) and no one can
// read a half-moved state. The cost is that the whole server stalls for the
// round trip, bounded by the timeout, which is why resharding tools move
// keys in small batches.
namespace migrate {

// Connects to host:port, sends `request` (pipelined commands) and reads
// back `expected` single-line replies (+OK or -ERR ...), all within
// `timeout`. On failure, `error` says whether it was the connect or the
// exchange that failed.
bool exchange(const std::string& host, uint16_t port, const std::string& request, size_t expected,
              std::chrono::milliseconds timeout, std::vector<std::string>& replies, std::string& error);

}  // namespace migrate
