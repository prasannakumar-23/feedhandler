#pragma once
// Deterministic synthetic feed: a random-walking mid price, adds on both sides, cancels and
// executions of live orders. Only ever emits messages that are valid in order (no cancel of an
// unknown id), so a receiver that applies 1..N in sequence ends with exactly the generator's book.
#include "ladder.h"

#include <vector>

namespace feed {

struct Rng {
    uint64_t s;
    explicit Rng(uint64_t seed) : s(seed ? seed : 88172645463325252ULL) {}
    uint64_t next() { s ^= s << 13; s ^= s >> 7; s ^= s << 17; return s; }
    uint64_t below(uint64_t n) { return next() % n; }
    double unit() { return (double)(next() >> 11) * (1.0 / 9007199254740992.0); }
};

struct Generated {
    std::vector<Packet> packets;  // index i holds sequence i+1
    uint64_t expectedDigest = 0;  // digest of the book after applying all packets in order
    uint64_t expectedLive = 0;
};

inline Generated generate(uint64_t n, uint64_t seed) {
    Generated g;
    g.packets.reserve(n);
    Rng r(seed);
    Ladder ref(n + 1);
    std::vector<uint64_t> live;
    std::vector<uint32_t> remaining(n + 2, 0);
    int64_t mid = 2048;
    uint64_t nextId = 0;
    for (uint64_t seq = 1; seq <= n; ++seq) {
        if (r.below(20) == 0) mid += (r.below(2) ? 1 : -1);
        if (mid < 1600) mid = 1600;
        if (mid > 2500) mid = 2500;
        Packet p{};
        p.seq = seq;
        const uint64_t roll = r.below(100);
        if (roll < 50 || live.empty()) {
            p.type = (uint8_t)MsgType::Add;
            p.side = (uint8_t)r.below(2);
            const int64_t off = (int64_t)((r.below(16) + r.below(16)) / 2);
            p.price = p.side == 0 ? mid - 1 - off : mid + 1 + off;
            p.qty = (uint32_t)(1 + r.below(100));
            p.orderId = ++nextId;
            live.push_back(p.orderId);
            remaining[p.orderId] = p.qty;
        } else {
            const size_t k = (size_t)r.below(live.size());
            p.orderId = live[k];
            if (roll < 85) {
                p.type = (uint8_t)MsgType::Cancel;
                live[k] = live.back(); live.pop_back();
            } else {
                p.type = (uint8_t)MsgType::Execute;
                p.qty = (uint32_t)(1 + r.below(remaining[p.orderId]));
                remaining[p.orderId] -= p.qty;
                if (remaining[p.orderId] == 0) { live[k] = live.back(); live.pop_back(); }
            }
        }
        ref.apply(p);
        g.packets.push_back(p);
    }
    g.expectedDigest = ref.digest();
    g.expectedLive = ref.liveOrders();
    return g;
}

}  // namespace feed
