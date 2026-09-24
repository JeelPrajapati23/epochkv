#include "reply.hpp"

namespace reply {

void simple_string(std::string& out, std::string_view s) {
    out += '+';
    out += s;
    out += "\r\n";
}

void error(std::string& out, std::string_view msg) {
    out += '-';
    out += msg;
    out += "\r\n";
}

void integer(std::string& out, int64_t n) {
    out += ':';
    out += std::to_string(n);
    out += "\r\n";
}

void bulk_string(std::string& out, std::string_view s) {
    out += '$';
    out += std::to_string(s.size());
    out += "\r\n";
    out += s;
    out += "\r\n";
}

void null_bulk(std::string& out) {
    out += "$-1\r\n";
}

void command(std::string& out, const std::vector<std::string>& argv) {
    out += '*';
    out += std::to_string(argv.size());
    out += "\r\n";
    for (const std::string& arg : argv) {
        bulk_string(out, arg);
    }
}

}  // namespace reply
