#pragma once

#include <bitset>
#include <cstdint>
#include <string>
#include <vector>

#include "cluster_slot.hpp"

// Messages of the cluster bus, the node-to-node channel on which nodes
// gossip about each other and about who owns which slot.
//
// Each message is framed as a RESP array of bulk strings, the same framing
// as client commands, so the bus reuses the RESP parser. Redis's bus uses a
// fixed binary struct instead; RESP costs a few more bytes per message but
// is binary-safe (the slot bitmap is sent raw) and readable in a packet dump.
//
//   type sender port cport role master current-epoch config-epoch
//        repl-offset mflags slots
//   then, for PING/PONG/MEET: count, and per gossiped node: id ip port cport flags
//   or, for UPDATE:           node-id config-epoch slots
//   or, for FAIL:             node-id
//   (FAILOVER_AUTH_REQUEST, FAILOVER_AUTH_ACK and MFSTART are just the header)
namespace cluster {

using SlotBitmap = std::bitset<kSlots>;

constexpr size_t kNodeIdLength = 40;

enum class MsgType {
    kPing,
    kPong,  // reply to PING/MEET; also broadcast unprompted after a config change
    kMeet,  // PING that asks the receiver to add us even though it doesn't know us
    kUpdate,  // "your view of node X is stale: here is its newer slot config"
    kFail,    // "a majority of masters agree node X is down": flag it FAIL now
    kFailoverAuthRequest,  // a replica asks the masters to vote for its promotion
    kFailoverAuthAck,      // a master's vote
    kMfStart,              // a replica asks its master to start a manual failover
};

// Message flags (Redis's mflags).
enum MsgFlag : unsigned {
    kMsgPaused = 1u << 0,    // sender is a master with writes paused for a manual failover
    kMsgForceAck = 1u << 1,  // vote even though the master isn't FAIL (manual failover)
};

// What the sender knows about some other node. pfail/fail are the sender's
// opinion of the node's health: coming from a master, a failure report,
// the raw material of failure detection.
struct GossipEntry {
    std::string id;
    std::string ip;
    uint16_t port = 0;
    uint16_t cport = 0;
    bool replica = false;
    bool pfail = false;
    bool fail = false;
};

struct Message {
    MsgType type = MsgType::kPing;
    std::string sender;
    uint16_t port = 0;   // sender's client port
    uint16_t cport = 0;  // sender's bus port
    bool replica = false;
    std::string master_id;  // the sender's master, if it's a replica
    uint64_t current_epoch = 0;
    // The sender's config epoch and slots, or its master's if it's a
    // replica (as in Redis: a replica advertises the slots it would take
    // over in a failover).
    uint64_t config_epoch = 0;
    // Replication offset: a master's stream position, or how much of its
    // master's stream a replica has applied. Replicas compare these to rank
    // themselves in an election; a manual failover waits on the master's.
    uint64_t repl_offset = 0;
    unsigned mflags = 0;  // MsgFlag bits
    SlotBitmap slots;

    std::vector<GossipEntry> gossip;  // PING/PONG/MEET

    std::string update_id;  // UPDATE: the node whose config this is
    uint64_t update_epoch = 0;
    SlotBitmap update_slots;

    std::string fail_id;  // FAIL: the node now considered down
};

bool valid_node_id(const std::string& id);

// Appends the RESP encoding of `msg` to `out`.
void encode(const Message& msg, std::string& out);
// Parses one received RESP array. False if it isn't a well-formed message;
// the bus drops the link then, as it can't trust anything else on it.
bool decode(const std::vector<std::string>& argv, Message& msg);

std::string slots_to_bytes(const SlotBitmap& slots);
bool slots_from_bytes(const std::string& bytes, SlotBitmap& slots);

}  // namespace cluster
