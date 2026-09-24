#pragma once

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

// RESP2 reply encoders. Each function appends one encoded reply to `out`
// (the connection's output buffer) instead of returning a fresh string, so
// replies to pipelined commands accumulate in one buffer and go out in a
// single write() — the same shape as Redis's addReply*() family.
namespace reply {

// +<s>\r\n — short status replies like OK / PONG. `s` must not contain CR/LF.
void simple_string(std::string& out, std::string_view s);

// -<msg>\r\n — `msg` should start with an error prefix, e.g. "ERR ...".
void error(std::string& out, std::string_view msg);

// :<n>\r\n
void integer(std::string& out, int64_t n);

// $<len>\r\n<bytes>\r\n — binary-safe.
void bulk_string(std::string& out, std::string_view s);

// $-1\r\n — "no such key".
void null_bulk(std::string& out);

// *<n>\r\n followed by n bulk strings: a command as clients send it. Also
// the wire format of the AOF and of the replication stream.
void command(std::string& out, const std::vector<std::string>& argv);

}  // namespace reply
