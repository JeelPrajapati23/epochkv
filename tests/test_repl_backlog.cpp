#include <catch2/catch_test_macros.hpp>

#include <string>

#include "repl_backlog.hpp"

namespace {

std::string tail_from(const ReplBacklog& b, uint64_t from) {
    std::string out;
    b.copy_from(from, out);
    return out;
}

}  // namespace

TEST_CASE("an empty backlog only contains its current offset", "[backlog]") {
    ReplBacklog b(8);
    REQUIRE(b.offset() == 0);
    REQUIRE(b.contains(0));
    REQUIRE_FALSE(b.contains(1));
    REQUIRE(tail_from(b, 0).empty());
}

TEST_CASE("offsets count every byte ever appended", "[backlog]") {
    ReplBacklog b(8);
    b.append("abc");
    b.append("de");
    REQUIRE(b.offset() == 5);
    REQUIRE(b.start_offset() == 0);
    REQUIRE(tail_from(b, 0) == "abcde");
    REQUIRE(tail_from(b, 3) == "de");
    REQUIRE(tail_from(b, 5).empty());
}

TEST_CASE("once full, the oldest bytes are overwritten", "[backlog]") {
    ReplBacklog b(8);
    b.append("abcdef");
    b.append("ghijk");  // wraps around the end of the buffer
    REQUIRE(b.offset() == 11);
    REQUIRE(b.histlen() == 8);
    REQUIRE(b.start_offset() == 3);
    REQUIRE_FALSE(b.contains(2));
    REQUIRE(b.contains(3));
    REQUIRE(tail_from(b, 3) == "defghijk");
    REQUIRE(tail_from(b, 9) == "jk");
}

TEST_CASE("an append larger than the capacity keeps only its tail", "[backlog]") {
    ReplBacklog b(4);
    b.append("x");
    b.append("0123456789");
    REQUIRE(b.offset() == 11);
    REQUIRE(b.start_offset() == 7);
    REQUIRE(tail_from(b, 7) == "6789");
}

TEST_CASE("many small appends wrap repeatedly and stay consistent", "[backlog]") {
    ReplBacklog b(7);
    std::string all;
    for (int i = 0; i < 100; ++i) {
        std::string chunk(1 + i % 3, static_cast<char>('a' + i % 26));
        b.append(chunk);
        all += chunk;
    }
    REQUIRE(b.offset() == all.size());
    REQUIRE(tail_from(b, b.start_offset()) == all.substr(all.size() - 7));
}

TEST_CASE("reset empties the buffer and restarts at a given offset", "[backlog]") {
    ReplBacklog b(8);
    b.append("abcdef");
    b.reset(1000);
    REQUIRE(b.offset() == 1000);
    REQUIRE(b.histlen() == 0);
    REQUIRE_FALSE(b.contains(999));
    REQUIRE(b.contains(1000));
    b.append("xy");
    REQUIRE(tail_from(b, 1000) == "xy");
}
