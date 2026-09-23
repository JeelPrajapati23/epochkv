#include "resp_parser.hpp"

#include <limits>

namespace {

// Parses buf[begin, end) as a decimal, unsigned length. Rejects anything
// that isn't a non-empty run of digits (no sign, no trailing junk) and
// guards against overflow — this is untrusted network input, so we validate
// explicitly instead of relying on std::stoul, which throws on some bad
// input but silently accepts other bad input (e.g. leading '-', or trailing
// non-digit characters after a valid prefix).
bool parse_length(const std::string& buf, size_t begin, size_t end, size_t& out) {
    if (begin >= end) {
        return false;
    }
    size_t value = 0;
    for (size_t i = begin; i < end; ++i) {
        char c = buf[i];
        if (c < '0' || c > '9') {
            return false;
        }
        size_t digit = static_cast<size_t>(c - '0');
        if (value > (std::numeric_limits<size_t>::max() - digit) / 10) {
            return false;
        }
        value = value * 10 + digit;
    }
    out = value;
    return true;
}

}  // namespace

void RespParser::feed(const char* data, size_t len) {
    buffer_.append(data, len);
}

RespParser::ParseResult RespParser::try_parse_command() {
    if (buffer_.empty()) {
        return {Status::kIncomplete, {}};
    }
    if (buffer_[0] != '*') {
        return {Status::kProtocolError, {}};
    }

    size_t header_end = buffer_.find("\r\n", 1);
    if (header_end == std::string::npos) {
        return {Status::kIncomplete, {}};
    }

    size_t count;
    if (!parse_length(buffer_, 1, header_end, count)) {
        return {Status::kProtocolError, {}};
    }

    size_t pos = header_end + 2;
    std::vector<std::string> command;

    for (size_t i = 0; i < count; ++i) {
        if (pos >= buffer_.size()) {
            return {Status::kIncomplete, {}};
        }
        if (buffer_[pos] != '$') {
            return {Status::kProtocolError, {}};
        }

        size_t len_end = buffer_.find("\r\n", pos + 1);
        if (len_end == std::string::npos) {
            return {Status::kIncomplete, {}};
        }

        size_t str_len;
        if (!parse_length(buffer_, pos + 1, len_end, str_len)) {
            return {Status::kProtocolError, {}};
        }

        pos = len_end + 2;

        
        size_t remaining = buffer_.size() - pos;
        if (remaining < str_len || remaining - str_len < 2) {
            return {Status::kIncomplete, {}};
        }
        if (buffer_[pos + str_len] != '\r' || buffer_[pos + str_len + 1] != '\n') {
            return {Status::kProtocolError, {}};
        }

        command.push_back(buffer_.substr(pos, str_len));
        pos += str_len + 2;
    }

    buffer_.erase(0, pos);
    return {Status::kComplete, std::move(command)};
}
