#pragma once

#include <array>
#include <chrono>
#include <cstdint>
#include <memory>
#include <random>
#include <string>
#include <unordered_map>
#include <vector>

#include "client.hpp"
#include "cluster_msg.hpp"
#include "resp_parser.hpp"

class Replication;
class Store;

// Sharding across nodes, following Redis Cluster's design.
//
// Keys map to one of 16384 hash slots (cluster_slot.hpp) and each slot is
// owned by one master. A node that receives a command for a key in a slot
// it doesn't own replies with a redirect instead of forwarding it:
//
//   -MOVED <slot> <ip>:<port>  the slot lives there; update your slot map
//   -ASK <slot> <ip>:<port>    the slot is mid-migration and this key has
//                              already moved; retry there once, prefixed by
//                              ASKING, without updating your slot map
//
// Nodes find each other and agree on the slot map over the cluster bus (a
// second port, by default the client port + 10000) by gossip: every node
// PINGs the others, and every PING/PONG carries the sender's slots and
// config epoch, plus what it knows about a few other nodes. When two nodes
// claim the same slot, the claim with the higher config epoch wins; two
// masters with the same epoch make the one with the smaller node ID bump
// its epoch, so every conflict eventually resolves the same way everywhere.
//
// Slots move between masters by live resharding: the target marks the slot
// IMPORTING, the source MIGRATING, keys are moved in batches with MIGRATE
// (clients get ASK for keys already moved), and finally SETSLOT NODE hands
// the slot over, bumping the target's config epoch so the new owner wins
// in gossip.
//
// Not here yet: failure detection and automatic failover.
class Cluster {
public:
    using Args = std::vector<std::string>;
    using SteadyTime = std::chrono::steady_clock::time_point;
    using Message = cluster::Message;
    using MsgType = cluster::MsgType;
    using SlotBitmap = cluster::SlotBitmap;
    static constexpr int kSlots = cluster::kSlots;

    struct Options {
        std::string config_file = "nodes.conf";  // full path
        std::string bind_addr = "127.0.0.1";     // the bus listens here too
        uint16_t port = 0;                       // our client port
        uint16_t cport = 0;                      // our bus port
        std::chrono::milliseconds node_timeout{15000};
        // Refuse key commands unless every slot is served (Redis's
        // cluster-require-full-coverage). Otherwise a client could write a
        // key to a node, then find it "missing" once coverage changes.
        bool require_full_coverage = true;
    };

    Cluster(Store& store, Replication& replication, Options options);
    ~Cluster();

    Cluster(const Cluster&) = delete;
    Cluster& operator=(const Cluster&) = delete;

    // Loads nodes.conf, or creates a fresh node ID and writes it. The file
    // is how a restarted node keeps its identity and its view of the slots.
    bool load_config(std::string& error);
    // After the dataset is loaded: keys in slots we don't own mean an
    // interrupted migration or a stale config. Unowned slots with keys are
    // claimed; slots owned elsewhere are marked importing, so their keys
    // stay reachable (with ASKING) instead of being silently dropped.
    void verify_config_with_data();
    // Opens the bus on the host's event loop; resumes following our master
    // if the config says we're a replica. Without start() (unit tests) the
    // cluster still routes and handles CLUSTER commands, just never talks.
    bool start(ClientHost* host);
    // Final nodes.conf save at shutdown.
    bool shutdown();

    // Redis's getNodeByQuery(): may a command touching `keys` run here?
    // If not, `error` gets the reply (MOVED/ASK/CROSSSLOT/TRYAGAIN/CLUSTERDOWN).
    // `asking`: the client sent ASKING, or it's RESTORE-ASKING.
    bool route(const std::vector<const std::string*>& keys, bool write, bool asking, const Client& c,
               std::string& error);

    // CLUSTER <subcommand> ...
    void command(const Args& argv, std::string& out);
    std::string info() const;  // CLUSTER INFO

    // Event-loop hooks.
    void cron();
    void before_sleep();

    const std::string& myid() const;
    bool state_ok() const { return state_ok_; }

private:
    enum Flag : unsigned {
        kMyself = 1u << 0,
        kMaster = 1u << 1,
        kReplica = 1u << 2,
        kHandshake = 1u << 3,  // met but not yet heard from: its ID is a placeholder
        kMeet = 1u << 4,       // send MEET rather than PING when connected
        kNoAddr = 1u << 5,     // address unknown; don't try to connect
    };

    struct Link;

    struct Node {
        std::string id;
        std::string ip;
        uint16_t port = 0;
        uint16_t cport = 0;
        unsigned flags = 0;
        std::string master_id;  // replicas: whom they follow
        uint64_t config_epoch = 0;
        SlotBitmap slots;
        SteadyTime ctime{};
        SteadyTime ping_sent{};  // zero while no PING awaits its PONG
        SteadyTime pong_received{};
        Link* link = nullptr;  // our outbound connection to it

        bool is(unsigned f) const { return (flags & f) != 0; }
    };

    // One bus connection. Outbound links are ours to a node (link->node
    // set); inbound ones were accepted and only carry PINGs to answer.
    struct Link {
        int fd = -1;
        Node* node = nullptr;
        bool connecting = false;
        bool write_interest = false;
        bool dead = false;
        RespParser parser;
        std::string out;
        std::string peer_ip;
        SteadyTime ctime{};
    };

    // --- nodes and slots ---
    Node* lookup(const std::string& id) const;
    Node* create_node(std::string id, unsigned flags);
    void del_node(Node* n);
    void rename_node(Node* n, const std::string& id);
    bool start_handshake(const std::string& ip, uint16_t port, uint16_t cport);
    Node* master_of(const Node* n) const;
    void set_slot(int slot, Node* owner);  // owner may be nullptr
    void set_as_replica_of(Node* master);
    void set_node_as_master(Node* n);
    bool bump_epoch_without_consensus();
    uint64_t max_epoch() const;
    void update_slots_config_with(Node* sender, uint64_t config_epoch, const SlotBitmap& slots);
    void handle_epoch_collision(Node* sender);
    void update_state();
    std::string random_id();
    void todo_save() { todo_save_ = true; }

    // --- bus ---
    void accept_links();
    void connect_link(Node* n);
    void on_link_event(Link* l);
    void finish_connect(Link* l);
    void read_link(Link* l);
    void flush_link(Link* l);
    void free_link(Link* l);
    void reap_links();
    void send(Link* l, const Message& msg);
    Message header(MsgType type) const;
    void send_ping(Link* l, MsgType type);
    void broadcast_pong();
    void send_update(Link* l, const Node* n);
    void process_packet(Link* l, const Message& msg);
    void process_gossip(const Message& msg);
    void follow_master();

    // --- config file / output ---
    bool save_config();
    bool parse_config(const std::string& text, std::string& error);
    std::string describe_nodes(bool for_config) const;  // CLUSTER NODES / nodes.conf
    std::string address(const Node* n) const;           // ip:port for redirects

    // --- CLUSTER subcommands ---
    void cmd_addslots(const Args& argv, bool range, bool add, std::string& out);
    void cmd_setslot(const Args& argv, std::string& out);
    void cmd_meet(const Args& argv, std::string& out);
    void cmd_replicate(const Args& argv, std::string& out);
    void cmd_forget(const Args& argv, std::string& out);
    void cmd_slots(std::string& out) const;

    Store& store_;
    Replication& replication_;
    Options options_;
    ClientHost* host_ = nullptr;
    std::mt19937_64 rng_;

    std::unordered_map<std::string, std::unique_ptr<Node>> nodes_;
    Node* myself_ = nullptr;
    uint64_t current_epoch_ = 0;
    std::array<Node*, kSlots> owner_{};
    std::array<Node*, kSlots> migrating_to_{};
    std::array<Node*, kSlots> importing_from_{};
    // Forgotten nodes, not re-added from gossip until the entry expires, so
    // CLUSTER FORGET can be run on every node before any re-learns it.
    std::unordered_map<std::string, SteadyTime> blacklist_;

    bool state_ok_ = false;
    bool todo_save_ = false;
    bool todo_update_state_ = false;
    // Our own slots or epoch changed: PONG everyone now instead of waiting
    // for the next periodic PINGs to spread it.
    bool todo_broadcast_ = false;

    int listen_fd_ = -1;
    std::vector<std::unique_ptr<Link>> links_;
    uint64_t cron_iterations_ = 0;
    uint64_t messages_sent_ = 0;
    uint64_t messages_received_ = 0;
};
