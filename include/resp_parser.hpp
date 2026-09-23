#pragma once

#include <cstddef>
#include <string>
#include <vector>

class RespParser {
public:
    enum class Status {
        kComplete,       // a full command was parsed
        kIncomplete,     // not enough bytes yet; buffer left untouched, feed() more and retry
        kProtocolError,  // bytes on the wire don't match RESP grammar
    };

    struct ParseResult {
        Status status;
        std::vector<std::string> command;  // populated only when status == kComplete
    };

    // Appends newly-read socket bytes to the internal buffer.
    void feed(const char* data, size_t len);

    // Attempts to extract one complete RESP array-of-bulk-strings command
    // (e.g. *2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n) from the front of the buffer.
    // On kComplete, the consumed bytes are removed from the buffer so the
    // next call can parse whatever command follows (pipelining).
    // On kIncomplete, the buffer must be left exactly as it was.
    ParseResult try_parse_command();

private:
    std::string buffer_;

    // Helpers you'll likely need — design and add as you go:
    //   - something that finds/extracts a single \r\n-terminated line
    //     starting at a given offset in buffer_
    //   - something that reads a $<len>\r\n<data>\r\n bulk string starting
    //     at a given offset
    // Decide for yourself whether you track a read offset into buffer_ or
    // erase consumed bytes from the front each time — that's a real
    // trade-off (cheap append + O(n) erase vs. tracking an offset and
    // periodically compacting).
};
