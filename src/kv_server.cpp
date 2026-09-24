#include <cctype>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <vector>

#include "cluster.hpp"
#include "command_dispatcher.hpp"
#include "file_util.hpp"
#include "persistence.hpp"
#include "replication.hpp"
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
                 "       [--auto-aof-rewrite-percentage PCT] [--auto-aof-rewrite-min-size BYTES]\n"
                 "       [--replicaof HOST PORT] [--repl-backlog-size BYTES] [--repl-timeout SECONDS]\n"
                 "       [--cluster-enabled yes|no] [--cluster-config-file NAME] [--cluster-port PORT]\n"
                 "       [--cluster-node-timeout MS] [--cluster-require-full-coverage yes|no]\n"
                 "       [--cluster-replica-validity-factor N] [--cluster-replica-no-failover yes|no]\n";
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
    Replication::Options repl_options;
    std::optional<std::pair<std::string, uint16_t>> replicaof;
    bool cluster_enabled = false;
    Cluster::Options cluster_options;
    std::string cluster_config_file = "nodes.conf";
    std::optional<uint16_t> cluster_port;

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
        } else if (arg == "--replicaof" && i + 2 < argc) {
            std::string host = argv[++i];
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0 || value > 65535) {
                std::cerr << "invalid replicaof port: " << argv[i] << "\n";
                return 1;
            }
            replicaof.emplace(host, static_cast<uint16_t>(value));
        } else if (arg == "--repl-backlog-size" && has_value) {
            std::optional<size_t> bytes = parse_memory(argv[++i]);
            if (!bytes || *bytes == 0) {
                std::cerr << "invalid repl-backlog-size: " << argv[i] << "\n";
                return 1;
            }
            repl_options.backlog_size = *bytes;
        } else if (arg == "--repl-timeout" && has_value) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0) {
                std::cerr << "invalid repl-timeout: " << argv[i] << "\n";
                return 1;
            }
            repl_options.timeout = std::chrono::seconds(value);
        } else if ((arg == "--cluster-enabled" || arg == "--cluster-require-full-coverage" ||
                    arg == "--cluster-replica-no-failover") &&
                   has_value) {
            std::string value = argv[++i];
            if (value != "yes" && value != "no") {
                std::cerr << "invalid " << arg.substr(2) << ": " << value << "\n";
                return 1;
            }
            bool& target = arg == "--cluster-enabled"                 ? cluster_enabled
                           : arg == "--cluster-require-full-coverage" ? cluster_options.require_full_coverage
                                                                      : cluster_options.replica_no_failover;
            target = value == "yes";
        } else if (arg == "--cluster-replica-validity-factor" && has_value) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value < 0 || value > 1000000) {
                std::cerr << "invalid cluster-replica-validity-factor: " << argv[i] << "\n";
                return 1;
            }
            cluster_options.replica_validity_factor = static_cast<int>(value);
        } else if (arg == "--cluster-config-file" && has_value) {
            cluster_config_file = argv[++i];
            if (cluster_config_file.find('/') != std::string::npos) {
                std::cerr << "cluster-config-file can't contain '/'; use --dir\n";
                return 1;
            }
        } else if (arg == "--cluster-port" && has_value) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0 || value > 65535) {
                std::cerr << "invalid cluster-port: " << argv[i] << "\n";
                return 1;
            }
            cluster_port = static_cast<uint16_t>(value);
        } else if (arg == "--cluster-node-timeout" && has_value) {
            char* end = nullptr;
            long value = std::strtol(argv[++i], &end, 10);
            if (*end != '\0' || value <= 0) {
                std::cerr << "invalid cluster-node-timeout: " << argv[i] << "\n";
                return 1;
            }
            cluster_options.node_timeout = std::chrono::milliseconds(value);
        } else {
            usage(argv[0]);
            return 1;
        }
    }

    if (cluster_enabled && replicaof) {
        std::cerr << "--replicaof isn't allowed in cluster mode; use CLUSTER REPLICATE\n";
        return 1;
    }
    // The bus port defaults to port + 10000, as in Redis.
    if (cluster_enabled && !cluster_port && port > 65535 - 10000) {
        std::cerr << "port " << port << " + 10000 is out of range; pass --cluster-port\n";
        return 1;
    }

    Store store(store_options);
    Persistence persistence(store, persistence_options);
    repl_options.listening_port = port;
    Replication replication(store, persistence, repl_options);
    CommandDispatcher dispatcher(store);
    dispatcher.set_persistence(&persistence);
    dispatcher.set_replication(&replication);

    std::unique_ptr<Cluster> cluster;
    if (cluster_enabled) {
        cluster_options.config_file = fileutil::join(persistence_options.dir, cluster_config_file);
        cluster_options.bind_addr = bind_addr;
        cluster_options.port = port;
        cluster_options.cport = cluster_port ? *cluster_port : static_cast<uint16_t>(port + 10000);
        cluster = std::make_unique<Cluster>(store, replication, cluster_options);
        std::string error;
        if (!cluster->load_config(error)) {
            std::cerr << "fatal: can't load the cluster config: " << error << "\n";
            return 1;
        }
        dispatcher.set_cluster(cluster.get());
    }

    // Every change to the dataset goes to both the AOF and the replicas, in
    // the same deterministic form. Not while loading from disk: that data
    // is already in the AOF, and it isn't part of the replication stream.
    auto propagate = [&persistence, &replication](const CommandDispatcher::Args& argv) {
        if (!persistence.loading()) {
            persistence.propagate(argv);
            replication.propagate(argv);
        }
    };
    dispatcher.set_propagator(propagate);
    // Keys the store drops on its own are logged as DEL, so replaying the
    // AOF can't resurrect them (e.g. an evicted key that had no TTL), and so
    // replicas — which never expire or evict keys themselves — drop them too.
    store.set_deletion_listener([propagate](const std::string& key) { propagate({"DEL", key}); });

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
    if (cluster) {
        cluster->verify_config_with_data();
    }

    Server server(bind_addr, port, dispatcher);
    server.set_replication(&replication);
    replication.attach(&server);
    Cluster* cl = cluster.get();
    server.set_cron(kCronInterval, [&store, &persistence, &replication, cl] {
        store.active_expire_cycle(kActiveExpireBudget);  // a no-op on a replica
        persistence.cron();
        replication.cron();
        if (cl != nullptr) {
            cl->cron();
        }
    });
    // Cluster first: losing a slot deletes its keys, and those DELs must
    // reach the replication stream and the AOF flush that follow.
    // Replication next: clients it unblocks may run writes, which the AOF
    // flush right after must include before their replies go out.
    server.set_before_sleep([&persistence, &replication, cl] {
        if (cl != nullptr) {
            cl->before_sleep();
        }
        replication.before_sleep();
        persistence.before_sleep();
    });

    if (!server.start()) {
        return 1;
    }
    if (cluster && !cluster->start(&server)) {
        return 1;
    }
    if (replicaof) {
        replication.replicaof(replicaof->first, replicaof->second);
    }
    server.run();
    bool ok = persistence.shutdown();
    if (cluster && !cluster->shutdown()) {
        std::cerr << "can't save the cluster config\n";
        ok = false;
    }
    return ok ? 0 : 1;
}
