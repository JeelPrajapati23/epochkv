#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "command_dispatcher.hpp"
#include "persistence.hpp"
#include "server.hpp"
#include "store.hpp"

namespace {

// Loopback-only by default, like Redis's protected mode: there's no auth yet,
// so exposing this on every interface would hand the dataset to anyone who
// can reach the port. Pass --bind 0.0.0.0 to opt in explicitly.
constexpr const char* kDefaultBindAddr = "127.0.0.1";
constexpr uint16_t kDefaultPort = 6380;

// Redis's default hz = 10: background tasks run every 100ms.
constexpr std::chrono::milliseconds kCronInterval{100};
// Active expiry may use at most 25% of each tick (Redis's ACTIVE_EXPIRE_CYCLE_SLOW_TIME_PERC).
constexpr std::chrono::microseconds kActiveExpireBudget{25'000};

void usage(const char* prog) {
    std::cerr << "usage: " << prog
              << " [--bind ADDR] [--port PORT] [--maxmemory BYTES[kb|mb|gb]]"
                 " [--maxmemory-policy noeviction|allkeys-lru]\n"
                 "       [--dir DIR] [--dbfilename NAME] [--save \"SECONDS CHANGES ...\"]\n"
                 "       [--appendonly yes|no] [--appendfsync always|everysec|no]\n"
                 "       [--auto-aof-rewrite-percentage PCT] [--auto-aof-rewrite-min-size BYTES]\n";
}

// "3600 1 300 100" -> {{3600, 1}, {300, 100}}; "" -> no save points.
std::optional<std::vector<SavePoint>> parse_save_points(const std::string& s) {
    std::istringstream in(s);
    std::vector<SavePoint> points;
    long long seconds;
    long long changes;
    while (in >> seconds) {
        if (!(in >> changes) || seconds <= 0 || changes <= 0) {
            return std::nullopt;
        }
        points.push_back({seconds, static_cast<uint64_t>(changes)});
    }
    if (!in.eof()) {
        return std::nullopt;  // stopped on something that isn't a number
    }
    return points;
}

// "100", "64mb", "1GB" -> bytes. Units are powers of 1024, as in redis.conf.
std::optional<size_t> parse_memory(const std::string& s) {
    size_t digits = 0;
    while (digits < s.size() && std::isdigit(static_cast<unsigned char>(s[digits]))) {
        ++digits;
    }
    if (digits == 0) {
        return std::nullopt;
    }
    std::string unit;
    for (size_t i = digits; i < s.size(); ++i) {
        unit += static_cast<char>(std::tolower(static_cast<unsigned char>(s[i])));
    }
    size_t multiplier;
    if (unit.empty() || unit == "b") {
        multiplier = 1;
    } else if (unit == "kb") {
        multiplier = 1024;
    } else if (unit == "mb") {
        multiplier = 1024 * 1024;
    } else if (unit == "gb") {
        multiplier = 1024 * 1024 * 1024;
    } else {
        return std::nullopt;
    }
    size_t value;
    try {
        value = std::stoull(s.substr(0, digits));
    } catch (const std::exception&) {
        return std::nullopt;
    }
    size_t bytes;
    if (__builtin_mul_overflow(value, multiplier, &bytes)) {
        return std::nullopt;
    }
    return bytes;
}

}  // namespace

int main(int argc, char** argv) {
    std::string bind_addr = kDefaultBindAddr;
    uint16_t port = kDefaultPort;
    Store::Options store_options;
    Persistence::Options persistence_options;

    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        bool has_value = i + 1 < argc;
        if (arg == "--bind" && has_value) {
            bind_addr = argv[++i];
        } else if (arg == "--port" && has_value) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0 || value > 65535) {
                std::cerr << "invalid port: " << argv[i] << "\n";
                return 1;
            }
            port = static_cast<uint16_t>(value);
        } else if (arg == "--maxmemory" && has_value) {
            std::optional<size_t> bytes = parse_memory(argv[++i]);
            if (!bytes) {
                std::cerr << "invalid maxmemory: " << argv[i] << "\n";
                return 1;
            }
            store_options.maxmemory = *bytes;
        } else if (arg == "--maxmemory-policy" && has_value) {
            std::string policy = argv[++i];
            if (policy == "noeviction") {
                store_options.policy = EvictionPolicy::kNoEviction;
            } else if (policy == "allkeys-lru") {
                store_options.policy = EvictionPolicy::kAllKeysLru;
            } else {
                std::cerr << "invalid maxmemory-policy: " << policy << "\n";
                return 1;
            }
        } else if (arg == "--dir" && has_value) {
            persistence_options.dir = argv[++i];
        } else if (arg == "--dbfilename" && has_value) {
            persistence_options.dbfilename = argv[++i];
            if (persistence_options.dbfilename.find('/') != std::string::npos) {
                std::cerr << "dbfilename can't contain '/'; use --dir\n";
                return 1;
            }
        } else if (arg == "--save" && has_value) {
            std::optional<std::vector<SavePoint>> points = parse_save_points(argv[++i]);
            if (!points) {
                std::cerr << "invalid save points: " << argv[i] << "\n";
                return 1;
            }
            persistence_options.save_points = *points;
        } else if (arg == "--appendonly" && has_value) {
            std::string value = argv[++i];
            if (value != "yes" && value != "no") {
                std::cerr << "invalid appendonly: " << value << "\n";
                return 1;
            }
            persistence_options.appendonly = value == "yes";
        } else if (arg == "--appendfsync" && has_value) {
            std::string value = argv[++i];
            if (value == "always") {
                persistence_options.appendfsync = FsyncPolicy::kAlways;
            } else if (value == "everysec") {
                persistence_options.appendfsync = FsyncPolicy::kEverySec;
            } else if (value == "no") {
                persistence_options.appendfsync = FsyncPolicy::kNo;
            } else {
                std::cerr << "invalid appendfsync: " << value << "\n";
                return 1;
            }
        } else if (arg == "--auto-aof-rewrite-percentage" && has_value) {
            char* end = nullptr;
            long long value = std::strtoll(argv[++i], &end, 10);
            if (*end != '\0' || value < 0) {
                std::cerr << "invalid auto-aof-rewrite-percentage: " << argv[i] << "\n";
                return 1;
            }
            persistence_options.auto_aof_rewrite_percentage = static_cast<uint64_t>(value);
        } else if (arg == "--auto-aof-rewrite-min-size" && has_value) {
            std::optional<size_t> bytes = parse_memory(argv[++i]);
            if (!bytes) {
                std::cerr << "invalid auto-aof-rewrite-min-size: " << argv[i] << "\n";
                return 1;
            }
            persistence_options.auto_aof_rewrite_min_size = *bytes;
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    Store store(store_options);
    Persistence persistence(store, persistence_options);
    CommandDispatcher dispatcher(store);
    dispatcher.set_persistence(&persistence);
    dispatcher.set_propagator([&persistence](const CommandDispatcher::Args& argv) { persistence.propagate(argv); });
    // Keys the store drops on its own are logged as DEL, so replaying the
    // AOF can't resurrect them (e.g. an evicted key that had no TTL).
    store.set_deletion_listener([&persistence](const std::string& key) { persistence.propagate({"DEL", key}); });

    std::string error;
    bool loaded = persistence.load(
        [&dispatcher](const CommandDispatcher::Args& argv) {
            std::string ignored_reply;
            dispatcher.dispatch(argv, ignored_reply);
        },
        error);
    if (!loaded) {
        std::cerr << "fatal: can't load data from disk: " << error << "\n";
        return 1;
    }
    std::cout << "loaded " << store.size() << " keys from disk\n";

    Server server(bind_addr, port, dispatcher);
    server.set_cron(kCronInterval, [&store, &persistence] {
        store.active_expire_cycle(kActiveExpireBudget);
        persistence.cron();
    });
    server.set_before_sleep([&persistence] { persistence.before_sleep(); });

    if (!server.start()) {
        return 1;
    }
    server.run();
    return persistence.shutdown() ? 0 : 1;
}
