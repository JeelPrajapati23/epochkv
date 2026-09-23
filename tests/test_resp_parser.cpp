#include <catch2/catch_test_macros.hpp>

#include "resp_parser.hpp"

TEST_CASE("parses a single complete command fed in one shot", "[resp_parser]") {
    RespParser parser;
    std::string msg = "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n";
    parser.feed(msg.data(), msg.size());

    auto result = parser.try_parse_command();
    REQUIRE(result.status == RespParser::Status::kComplete);
    REQUIRE(result.command == std::vector<std::string>{"GET", "foo"});
}

TEST_CASE("returns kIncomplete when a command is split across two feed() calls", "[resp_parser]") {
    RespParser parser;
    std::string first_half = "*2\r\n$3\r\nGET\r\n$3\r\nfo";
    std::string second_half = "o\r\n";

    parser.feed(first_half.data(), first_half.size());
    auto partial = parser.try_parse_command();
    REQUIRE(partial.status == RespParser::Status::kIncomplete);

    parser.feed(second_half.data(), second_half.size());
    auto complete = parser.try_parse_command();
    REQUIRE(complete.status == RespParser::Status::kComplete);
    REQUIRE(complete.command == std::vector<std::string>{"GET", "foo"});
}

TEST_CASE("parses two pipelined commands from a single feed()", "[resp_parser]") {
    RespParser parser;
    std::string msg = "*1\r\n$4\r\nPING\r\n*1\r\n$4\r\nPING\r\n";
    parser.feed(msg.data(), msg.size());

    auto first = parser.try_parse_command();
    REQUIRE(first.status == RespParser::Status::kComplete);
    REQUIRE(first.command == std::vector<std::string>{"PING"});

    auto second = parser.try_parse_command();
    REQUIRE(second.status == RespParser::Status::kComplete);
    REQUIRE(second.command == std::vector<std::string>{"PING"});
}

TEST_CASE("rejects a message that doesn't start with '*'", "[resp_parser]") {
    RespParser parser;
    std::string msg = "GET foo\r\n";
    parser.feed(msg.data(), msg.size());

    auto result = parser.try_parse_command();
    REQUIRE(result.status == RespParser::Status::kProtocolError);
}

TEST_CASE("rejects a non-digit array length", "[resp_parser]") {
    RespParser parser;
    std::string msg = "*a\r\n";
    parser.feed(msg.data(), msg.size());

    auto result = parser.try_parse_command();
    REQUIRE(result.status == RespParser::Status::kProtocolError);
}

TEST_CASE("rejects a bulk string not prefixed with '$'", "[resp_parser]") {
    RespParser parser;
    std::string msg = "*1\r\nGET\r\n";
    parser.feed(msg.data(), msg.size());

    auto result = parser.try_parse_command();
    REQUIRE(result.status == RespParser::Status::kProtocolError);
}

TEST_CASE("handles an empty array", "[resp_parser]") {
    RespParser parser;
    std::string msg = "*0\r\n";
    parser.feed(msg.data(), msg.size());

    auto result = parser.try_parse_command();
    REQUIRE(result.status == RespParser::Status::kComplete);
    REQUIRE(result.command.empty());
}
