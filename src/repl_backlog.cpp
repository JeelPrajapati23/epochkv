#include "repl_backlog.hpp"

#include <algorithm>

ReplBacklog::ReplBacklog(size_t capacity) : buf_(std::max<size_t>(capacity, 1), '\0') {}

void ReplBacklog::append(std::string_view data) {
    offset_ += data.size();
    // Only the last capacity() bytes can survive; skip straight to them.
    if (data.size() > buf_.size()) {
        data.remove_prefix(data.size() - buf_.size());
    }
    while (!data.empty()) {
        size_t n = std::min(data.size(), buf_.size() - write_pos_);
        buf_.replace(write_pos_, n, data.data(), n);
        write_pos_ = (write_pos_ + n) % buf_.size();
        data.remove_prefix(n);
        histlen_ = std::min(histlen_ + n, buf_.size());
    }
}

void ReplBacklog::reset(uint64_t offset) {
    write_pos_ = 0;
    histlen_ = 0;
    offset_ = offset;
}

void ReplBacklog::copy_from(uint64_t from, std::string& out) const {
    size_t len = static_cast<size_t>(offset_ - from);
    // The newest byte sits just before write_pos_; step back len bytes.
    size_t pos = (write_pos_ + buf_.size() - len) % buf_.size();
    while (len > 0) {
        size_t n = std::min(len, buf_.size() - pos);
        out.append(buf_, pos, n);
        pos = (pos + n) % buf_.size();
        len -= n;
    }
}
