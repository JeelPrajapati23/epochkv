#include <catch2/catch_test_macros.hpp>

#include <string>

#include "reply.hpp"

TEST_CASE("simple string encodes as +<s>CRLF", "[reply]") {
    std::string out;
    reply::simple_string(out, "OK");
    REQUIRE(out == "+OK\r\n");
}

TEST_CASE("error encodes as -<msg>CRLF", "[reply]") {
    std::string out;
    reply::error(out, "ERR boom");
    REQUIRE(out == "-ERR boom\r\n");
}

TEST_CASE("integer encodes as :<n>CRLF, including negatives", "[reply]") {
    std::string out;
    reply::integer(out, 42);
    reply::integer(out, -7);
    REQUIRE(out == ":42\r\n:-7\r\n");
}

TEST_CASE("bulk string is length-prefixed and binary-safe", "[reply]") {
    std::string out;
    reply::bulk_string(out, std::string("a\r\nb\0c", 6));
    REQUIRE(out == std::string("$6\r\na\r\nb\0c\r\n", 12));
}

TEST_CASE("empty bulk string is distinct from null bulk", "[reply]") {
    std::string out;
    reply::bulk_string(out, "");
    reply::null_bulk(out);
    REQUIRE(out == "$0\r\n\r\n$-1\r\n");
}
