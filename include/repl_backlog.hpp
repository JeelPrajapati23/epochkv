#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

// The replication backlog: a fixed-size circular buffer holding the most
// recent bytes of the replication stream.
//
// Every byte ever sent to replicas has an offset; offset() is the total
// number of bytes fed so far (Redis's master_repl_offset). A replica that
// disconnects and comes back asking to continue from offset X can be
// served from here as long as X is still in [start_offset(), offset()] —
// a partial resync. Once the backlog has wrapped past X, it needs a full
// resync instead. Bigger backlog = longer outages survived without one.
class ReplBacklog {
public:
    explicit ReplBacklog(size_t capacity);

    // Appends stream bytes, overwriting the oldest ones once full.
    void append(std::string_view data);

    // Empties the buffer and restarts the offset count at `offset` (a
    // replica that just loaded its master's snapshot taken at `offset`).
    void reset(uint64_t offset);

    uint64_t offset() const { return offset_; }
    uint64_t start_offset() const { return offset_ - histlen_; }
    size_t histlen() const { return histlen_; }
    size_t capacity() const { return buf_.size(); }

    // True if every byte from `from` to offset() is still held.
    bool contains(uint64_t from) const { return from >= start_offset() && from <= offset_; }

    // Appends bytes [from, offset()) to `out`. Requires contains(from).
    void copy_from(uint64_t from, std::string& out) const;

private:
    std::string buf_;
    size_t write_pos_ = 0;  // where the next byte goes
    size_t histlen_ = 0;    // valid bytes, <= capacity
    uint64_t offset_ = 0;
};
