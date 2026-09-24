#include "cluster_msg.hpp"

#include <charconv>

#include "reply.hpp"

namespace cluster {
namespace {

constexpr size_t kHeaderFields = 11;
constexpr size_t kGossipFields = 5;
constexpr size_t kSlotBytes = kSlots / 8;

constexpr MsgType kAllTypes[] = {
    MsgType::kPing, MsgType::kPong,  MsgType::kMeet, MsgType::kUpdate, MsgType::kFail, MsgType::kFailoverAuthRequest,
    MsgType::kFailoverAuthAck, MsgType::kMfStart,
};

const char* type_name(MsgType t) {
    switch (t) {
        case MsgType::kPing:
            return "PING";
        case MsgType::kPong:
            return "PONG";
        case MsgType::kMeet:
            return "MEET";
        case MsgType::kUpdate:
            return "UPDATE";
        case MsgType::kFail:
            return "FAIL";
        case MsgType::kFailoverAuthRequest:
            return "FAILOVER_AUTH_REQUEST";
        case MsgType::kFailoverAuthAck:
            return "FAILOVER_AUTH_ACK";
        case MsgType::kMfStart:
            return "MFSTART";
    }
    return "?";
}

bool parse_type(const std::string& s, MsgType& t) {
    for (MsgType candidate : kAllTypes) {
        if (s == type_name(candidate)) {
            t = candidate;
            return true;
        }
    }
    return false;
}

// Election and manual-failover messages carry nothing beyond the header.
bool header_only(MsgType t) {
    return t == MsgType::kFailoverAuthRequest || t == MsgType::kFailoverAuthAck || t == MsgType::kMfStart;
}

template <typename T>
bool parse_uint(const std::string& s, T& out) {
    auto [ptr, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
    return ec == std::errc() && ptr == s.data() + s.size() && !s.empty();
}

bool parse_port(const std::string& s, uint16_t& port) {
    return parse_uint(s, port) && port != 0;
}

bool parse_role(const std::string& s, bool& replica) {
    if (s != "master" && s != "slave") {
        return false;
    }
    replica = s == "slave";
    return true;
}

// "master", "slave,fail?", "master,fail": the role, then the sender's
// opinion of the node's health, in the words CLUSTER NODES uses.
std::string gossip_flags(const GossipEntry& g) {
    std::string flags = g.replica ? "slave" : "master";
    if (g.fail) {
        flags += ",fail";
    } else if (g.pfail) {
        flags += ",fail?";
    }
    return flags;
}

bool parse_gossip_flags(const std::string& s, GossipEntry& g) {
    size_t comma = s.find(',');
    if (!parse_role(s.substr(0, comma), g.replica)) {
        return false;
    }
    while (comma != std::string::npos) {
        size_t next = s.find(',', comma + 1);
        std::string flag = s.substr(comma + 1, next == std::string::npos ? std::string::npos : next - comma - 1);
        if (flag == "fail?") {
            g.pfail = true;
        } else if (flag == "fail") {
            g.fail = true;
        } else {
            return false;
        }
        comma = next;
    }
    return true;
}

}  // namespace

bool valid_node_id(const std::string& id) {
    if (id.size() != kNodeIdLength) {
        return false;
    }
    for (char c : id) {
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return false;
        }
    }
    return true;
}

std::string slots_to_bytes(const SlotBitmap& slots) {
    std::string bytes(kSlotBytes, '\0');
    for (int slot = 0; slot < kSlots; ++slot) {
        if (slots[slot]) {
            bytes[slot / 8] = static_cast<char>(bytes[slot / 8] | (1 << (slot % 8)));
        }
    }
    return bytes;
}

bool slots_from_bytes(const std::string& bytes, SlotBitmap& slots) {
    if (bytes.size() != kSlotBytes) {
        return false;
    }
    slots.reset();
    for (int slot = 0; slot < kSlots; ++slot) {
        if (static_cast<uint8_t>(bytes[slot / 8]) & (1 << (slot % 8))) {
            slots.set(slot);
        }
    }
    return true;
}

void encode(const Message& msg, std::string& out) {
    std::vector<std::string> argv{
        type_name(msg.type),
        msg.sender,
        std::to_string(msg.port),
        std::to_string(msg.cport),
        msg.replica ? "slave" : "master",
        msg.master_id.empty() ? "-" : msg.master_id,
        std::to_string(msg.current_epoch),
        std::to_string(msg.config_epoch),
        std::to_string(msg.repl_offset),
        std::to_string(msg.mflags),
        slots_to_bytes(msg.slots),
    };
    if (msg.type == MsgType::kUpdate) {
        argv.insert(argv.end(), {msg.update_id, std::to_string(msg.update_epoch), slots_to_bytes(msg.update_slots)});
    } else if (msg.type == MsgType::kFail) {
        argv.push_back(msg.fail_id);
    } else if (!header_only(msg.type)) {
        argv.push_back(std::to_string(msg.gossip.size()));
        for (const GossipEntry& g : msg.gossip) {
            argv.insert(argv.end(), {g.id, g.ip, std::to_string(g.port), std::to_string(g.cport), gossip_flags(g)});
        }
    }
    reply::command(out, argv);
}

bool decode(const std::vector<std::string>& argv, Message& msg) {
    msg = Message{};
    if (argv.size() < kHeaderFields || !parse_type(argv[0], msg.type)) {
        return false;
    }
    msg.sender = argv[1];
    if (!valid_node_id(msg.sender) || !parse_port(argv[2], msg.port) || !parse_port(argv[3], msg.cport) ||
        !parse_role(argv[4], msg.replica) || !parse_uint(argv[6], msg.current_epoch) ||
        !parse_uint(argv[7], msg.config_epoch) || !parse_uint(argv[8], msg.repl_offset) ||
        !parse_uint(argv[9], msg.mflags) || !slots_from_bytes(argv[10], msg.slots)) {
        return false;
    }
    if (argv[5] != "-") {
        if (!valid_node_id(argv[5])) {
            return false;
        }
        msg.master_id = argv[5];
    }
    const std::string* body = argv.data() + kHeaderFields;
    size_t body_size = argv.size() - kHeaderFields;

    if (msg.type == MsgType::kUpdate) {
        if (body_size != 3 || !valid_node_id(body[0]) || !parse_uint(body[1], msg.update_epoch) ||
            !slots_from_bytes(body[2], msg.update_slots)) {
            return false;
        }
        msg.update_id = body[0];
        return true;
    }
    if (msg.type == MsgType::kFail) {
        if (body_size != 1 || !valid_node_id(body[0])) {
            return false;
        }
        msg.fail_id = body[0];
        return true;
    }
    if (header_only(msg.type)) {
        return body_size == 0;
    }

    size_t count;
    // count is checked against the array size first, so a huge count can't
    // overflow the multiplication.
    if (body_size < 1 || !parse_uint(body[0], count) || count > body_size || body_size != 1 + count * kGossipFields) {
        return false;
    }
    for (size_t i = 0; i < count; ++i) {
        const std::string* f = body + 1 + i * kGossipFields;
        GossipEntry g;
        g.id = f[0];
        g.ip = f[1];
        if (!valid_node_id(g.id) || !parse_port(f[2], g.port) || !parse_port(f[3], g.cport) ||
            !parse_gossip_flags(f[4], g)) {
            return false;
        }
        msg.gossip.push_back(std::move(g));
    }
    return true;
}

}  // namespace cluster
