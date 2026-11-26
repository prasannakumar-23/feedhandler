#pragma once
// Fixed-size binary market-data message ("ITCH-style"): 40 bytes, little-endian, no padding bytes
// carrying meaning. Decoding goes through memcpy, which compilers turn into plain loads and which
// is well-defined for unaligned or type-punned buffers (a reinterpret_cast would not be).
#include <cstdint>
#include <cstring>
#include <type_traits>

namespace feed {

enum class MsgType : uint8_t { Add = 'A', Cancel = 'X', Execute = 'E', Heartbeat = 'H' };
enum class Side : uint8_t { Buy = 0, Sell = 1 };

struct Packet {
    uint64_t seq;       // 1-based, consecutive per stream. For Heartbeat: highest sequence sent so far
    uint64_t sendNs;    // sender's steady-clock timestamp (ns), for latency measurement
    uint64_t orderId;
    int64_t price;      // integer ticks
    uint32_t qty;
    uint8_t type;       // MsgType
    uint8_t side;       // Side
    uint16_t reserved;
};
static_assert(sizeof(Packet) == 40, "wire format must be exactly 40 bytes");
static_assert(std::is_trivially_copyable_v<Packet>);
static_assert(__BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__, "wire format is little-endian");

inline void encode(const Packet& p, void* out) { std::memcpy(out, &p, sizeof p); }
inline Packet decode(const void* in) { Packet p; std::memcpy(&p, in, sizeof p); return p; }
inline bool valid(const Packet& p) {
    switch ((MsgType)p.type) {
        case MsgType::Add: return p.side <= 1 && p.qty > 0 && p.orderId > 0;
        case MsgType::Cancel: return p.orderId > 0;
        case MsgType::Execute: return p.orderId > 0 && p.qty > 0;
        case MsgType::Heartbeat: return true;
    }
    return false;
}

}  // namespace feed
