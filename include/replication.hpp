#pragma once

#include <chrono>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "client.hpp"
#include "repl_backlog.hpp"

class Persistence;
class Store;

// Asynchronous master-replica replication, following Redis's PSYNC design.
//
// Master side: every change to the dataset is encoded as a command and
// appended to the replication stream (propagate()). The stream goes to each
// online replica's output buffer and into the backlog. A connecting replica
// sends PSYNC <replid> <offset>:
//  - if we know that history and the backlog still holds everything after
//    <offset>, it gets +CONTINUE and just the missing bytes (partial resync);
//  - otherwise +FULLRESYNC <replid> <offset>: we fork a snapshot, stream it
//    to the replica, then the commands that ran while it was being made.
//
// Replica side: a non-blocking handshake (PING, REPLCONF, PSYNC) driven by
// the event loop; on a full resync the snapshot is received into a file and
// loaded; then the link becomes a Client whose commands are applied without
// replies. The replica counts the stream bytes it has applied (its offset),
// reports it with REPLCONF ACK every second, and on a dropped link
// reconnects asking to continue from that offset.
//
// Replication is asynchronous: a write is acknowledged to the client before
// any replica has it, so a master crash can lose recent writes. WAIT lets a
// client block until N replicas have acknowledged its writes.
class Replication {
public:
    using Args = std::vector<std::string>;
    using SteadyTime = std::chrono::steady_clock::time_point;

    struct Options {
        uint16_t listening_port = 0;  // ours, announced to our master
        size_t backlog_size = 1024 * 1024;
        // A link silent for this long is considered dead (both directions).
        std::chrono::seconds timeout{60};
        // Master PINGs its replicas this often, so a healthy link is never silent.
        std::chrono::seconds ping_period{10};
    };

    Replication(Store& store, Persistence& persistence, Options options);
    ~Replication();

    Replication(const Replication&) = delete;
    Replication& operator=(const Replication&) = delete;

    void attach(ClientHost* host) { host_ = host; }

    bool is_replica() const { return !master_host_.empty(); }
    uint64_t offset() const { return backlog_.offset(); }
    const std::string& replid() const { return replid_; }

    // REPLICAOF host port / REPLICAOF NO ONE.
    void replicaof(const std::string& host, uint16_t port);
    void replicaof_no_one();

    // Master: appends one change to the replication stream. Ignored on a
    // replica, which forwards its master's stream verbatim instead.
    void propagate(const Args& argv);
    // Replica: `raw` (one command from our master) has just been applied.
    void master_command_applied(const std::string& raw);

    // Command backends. `c` is the client that sent the command.
    void psync(Client& c, const std::string& replid, int64_t offset, std::string& out);
    void replconf_ack(Client& c, uint64_t offset);
    void replconf_getack();
    // Replies now and returns false, or parks `c` and returns true.
    bool wait(Client& c, int64_t numreplicas, int64_t timeout_ms, std::string& out);
    std::string info() const;

    // Event-loop hooks.
    void cron();          // every server tick
    void before_sleep();  // each iteration, before replies are written
    void client_closed(Client& c);
    void snapshot_finished(bool ok);
    void snapshot_sent(Client& c);  // a replica's snapshot transfer completed

private:
    enum class LinkState {
        kNone,        // not a replica
        kConnect,     // should connect (at next_connect_)
        kConnecting,  // non-blocking connect() in flight
        kHandshake,   // PING/REPLCONF/PSYNC sent, reading their replies
        kTransfer,    // receiving the snapshot of a full resync
        kConnected,   // streaming; master_ is set
    };
    struct Waiter {
        Client* client;
        uint64_t offset;
        int64_t numreplicas;
        std::optional<SteadyTime> deadline;
    };

    // Master side.
    void feed(const std::string& data);
    bool try_partial_resync(Client& c, const std::string& replid, int64_t offset, std::string& out);
    void start_snapshot_for_replicas();
    int64_t acked_replicas(uint64_t offset) const;
    void disconnect_replicas();
    void unblock_waiters();
    void shift_replid();

    // Replica side.
    void connect_to_master();
    void on_link_ready();
    void read_link();
    void process_link_input();
    bool take_line(std::string& line);
    void handle_psync_reply(const std::string& line);
    void finish_transfer();
    void adopt_link();
    void abort_link(const std::string& why);
    void close_link();
    void send_ack();

    Store& store_;
    Persistence& persistence_;
    Options options_;
    ClientHost* host_ = nullptr;

    // Our history: replid_ names it; the backlog holds its recent tail.
    // After a promotion, replid2_ is the history we used to follow, which
    // our own replicas can still continue as long as they ask for an offset
    // no later than second_replid_offset_ (PSYNC2).
    std::string replid_;
    std::string replid2_;
    std::optional<uint64_t> second_replid_offset_;
    ReplBacklog backlog_;
    // False while no one else has seen replid_ (fresh start, or just
    // promoted): a PSYNC naming it could never succeed, so we send
    // "PSYNC ? -1" and ask for a full resync outright.
    bool replid_shared_ = false;

    std::vector<Client*> replicas_;
    std::vector<Waiter> waiters_;
    // Unblocked outside the event loop's normal flow (role change); their
    // buffered commands run at the next before_sleep().
    std::vector<Client*> to_resume_;
    bool get_ack_pending_ = false;
    SteadyTime last_ping_{};
    SteadyTime last_second_tick_{};
    uint64_t sync_full_ = 0;
    uint64_t sync_partial_ok_ = 0;
    uint64_t sync_partial_err_ = 0;

    std::string master_host_;
    uint16_t master_port_ = 0;
    LinkState link_state_ = LinkState::kNone;
    SteadyTime next_connect_{};
    int link_fd_ = -1;
    std::string link_buf_;
    SteadyTime link_last_io_{};
    int handshake_replies_ = 0;
    std::string transfer_replid_;
    uint64_t transfer_offset_ = 0;
    int transfer_fd_ = -1;
    std::string transfer_path_;
    std::optional<uint64_t> transfer_remaining_;  // unset until the $<len> header arrives
    Client* master_ = nullptr;
};
