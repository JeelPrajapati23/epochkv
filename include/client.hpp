#pragma once

#include <chrono>
#include <cstdint>
#include <functional>
#include <string>

#include "resp_parser.hpp"

// One connection on the event loop. Usually an ordinary client, but two
// kinds are special:
//  - a replica attached to us (repl_state != kNone): after PSYNC it stops
//    getting replies and instead gets the replication stream;
//  - our link to our own master (is_master): its commands are applied
//    without replies, even though a replica refuses writes from anyone else.
struct Client {
    // Master's view of an attached replica (Redis's SLAVE_STATE_*).
    enum class ReplState {
        kNone,
        kWaitBgsaveStart,  // needs a snapshot; none can start yet (a child is busy)
        kWaitBgsaveEnd,    // snapshot being written; the stream is held in repl_held
        kSendBulk,         // snapshot file being streamed out of repl_snapshot_fd
        kOnline,           // gets the live stream
    };
    using SteadyTime = std::chrono::steady_clock::time_point;

    Client(uint64_t id, int fd) : id(id), fd(fd) {}

    uint64_t id;       // unique for the life of the process, unlike fds
    int fd;
    std::string addr;  // peer IP
    RespParser parser;
    std::string out;                  // bytes not yet sent
    bool write_interest = false;      // currently registered for EPOLLOUT
    bool pending_write = false;       // queued in the server's pending-writes list
    bool close_after_flush = false;   // protocol error: send the error, then close
    bool closing = false;             // queued for close at end of this loop iteration

    // Parked by WAIT: input is still read and buffered but not executed.
    bool blocked = false;
    // Replication offset just after this client's latest write, i.e. how far
    // replicas must have acknowledged for WAIT to count them.
    uint64_t woff = 0;

    ReplState repl_state = ReplState::kNone;
    uint16_t repl_listening_port = 0;
    uint64_t repl_ack_offset = 0;
    SteadyTime repl_ack_time{};
    std::string repl_held;  // stream produced while the snapshot is made and sent
    int repl_snapshot_fd = -1;

    bool is_master = false;
    SteadyTime last_interaction{};  // last read from this peer
};

// What the replication layer needs from the event loop, kept abstract so it
// doesn't depend on the Server (which in turn calls into replication).
class ClientHost {
public:
    virtual ~ClientHost() = default;

    // Anything appended to c.out goes out at the end of this loop iteration.
    virtual void queue_write(Client& c) = 0;
    // Closes at the end of this loop iteration.
    virtual void close_client(Client& c) = 0;
    // Executes any complete commands already buffered for `c` (after WAIT
    // unblocks it, or when a master link is adopted with bytes already read).
    virtual void process_buffered(Client& c) = 0;

    // Runs `on_ready` whenever `fd` is readable (or writable, if
    // want_write). Calling it again for the same fd changes the interest.
    virtual bool watch(int fd, bool want_write, std::function<void()> on_ready) = 0;
    virtual void unwatch(int fd) = 0;

    // Turns an already-unwatched connected socket into our master's client.
    // `pending` = stream bytes already read past the handshake; they're
    // buffered, not executed, until process_buffered().
    virtual Client& adopt_master(int fd, const std::string& pending) = 0;
};
