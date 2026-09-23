#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>

#include "command_dispatcher.hpp"
#include "hash_table.hpp"
#include "server.hpp"

namespace {

// Loopback-only by default, like Redis's protected mode: there's no auth yet,
// so exposing this on every interface would hand the dataset to anyone who
// can reach the port. Pass --bind 0.0.0.0 to opt in explicitly.
constexpr const char* kDefaultBindAddr = "127.0.0.1";
constexpr uint16_t kDefaultPort = 6380;

void usage(const char* prog) {
    std::cerr << "usage: " << prog << " [--bind ADDR] [--port PORT]\n";
}

}  // namespace

int main(int argc, char** argv) {
    std::string bind_addr = kDefaultBindAddr;
    uint16_t port = kDefaultPort;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if (arg == "--bind" && i + 1 < argc) {
            bind_addr = argv[++i];
        } else if (arg == "--port" && i + 1 < argc) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0 || value > 65535) {
                std::cerr << "invalid port: " << argv[i] << "\n";
                return 1;
            }
            port = static_cast<uint16_t>(value);
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    HashTable store;
    CommandDispatcher dispatcher(store);
    Server server(bind_addr, port, dispatcher);

    if (!server.start()) {
        return 1;
    }
    server.run();
    return 0;
}
