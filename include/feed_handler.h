#pragma once
// Sequence-gap handling for a datagram feed. Takes packets in whatever order the network delivered
// them (lost, duplicated, reordered) and hands them to `deliver` exactly once each, in sequence.
//
//  * seq < expected, or already held      -> duplicate, dropped
//  * seq == expected                      -> delivered, then any held contiguous packets are drained
//  * seq > expected                       -> held in a fixed ring window. If the hole is still open once the
//                                            newest packet is `reorderTolerance` ahead of the first
//                                            missing one, it is declared lost and re-requested
//  * Heartbeat(last)                      -> lets the receiver detect lost packets at the end of a burst
//  * hole wider than the window           -> missing packets are fetched one at a time until the new packet fits
//
// `retransmit(seq)` stands in for the exchange's replay / snapshot channel. In production this is a
// request over TCP or a second multicast group; here it is a function call so the logic can be tested
// exhaustively and deterministically. No allocation happens after construction.
#include "wire.h"

#include <cstdint>

namespace feed {

struct NoHooks {
    void held(const Packet&) {}
    void duplicate(const Packet&) {}
    void gap(uint64_t /*from*/, uint64_t /*to*/, uint64_t /*missing*/) {}
};

struct HandlerStats {
    uint64_t delivered = 0, duplicates = 0, outOfOrder = 0, retransmitted = 0, gapEvents = 0;
};

template <class Deliver, class Retransmit, class Hooks = NoHooks>
class FeedHandler {
public:
    static constexpr uint64_t kWindow = 1024;  // power of two

    FeedHandler(Deliver& d, Retransmit& r, Hooks& h, uint64_t reorderTolerance = 4)
        : deliver_(d), retrans_(r), hooks_(h), tol_(reorderTolerance) {}

    void onPacket(const Packet& p) {
        if ((MsgType)p.type == MsgType::Heartbeat) {
            if (p.seq > highest_) highest_ = p.seq;
            if (highest_ >= expected_) recoverGap();
            return;
        }
        const uint64_t seq = p.seq;
        if (seq < expected_) { ++stats_.duplicates; hooks_.duplicate(p); return; }
        while (seq - expected_ >= kWindow) {  // hole wider than the window: catch up one packet at a time
            if (!has_[expected_ & (kWindow - 1)]) { ++stats_.gapEvents; hooks_.gap(expected_, expected_, 1); store(retrans_(expected_), true); ++stats_.retransmitted; }
            drain();
        }
        const size_t i = seq & (kWindow - 1);
        if (has_[i]) { ++stats_.duplicates; hooks_.duplicate(p); return; }
        if (seq != expected_) { ++stats_.outOfOrder; hooks_.held(p); }
        store(p, false);
        if (seq > highest_) highest_ = seq;
        drain();
        if (highest_ >= expected_ && highest_ - expected_ >= tol_) recoverGap();
    }

    uint64_t nextExpected() const { return expected_; }
    const HandlerStats& stats() const { return stats_; }

private:
    void store(const Packet& p, bool recovered) {
        const size_t i = p.seq & (kWindow - 1);
        slot_[i] = p; has_[i] = true; rec_[i] = recovered;
    }
    void drain() {
        while (has_[expected_ & (kWindow - 1)]) {
            const size_t i = expected_ & (kWindow - 1);
            has_[i] = false;
            ++stats_.delivered;
            deliver_(slot_[i], rec_[i]);
            ++expected_;
        }
    }
    void recoverGap() {  // fill every hole in [expected_, highest_], one window at a time
        while (expected_ <= highest_) {
            const uint64_t start = expected_;
            uint64_t missing = 0;
            for (uint64_t s = start; s <= highest_ && s - start < kWindow; ++s)
                if (!has_[s & (kWindow - 1)]) ++missing;
            if (missing) {
                ++stats_.gapEvents;
                hooks_.gap(start, highest_, missing);
                for (uint64_t s = start; s <= highest_ && s - start < kWindow; ++s)
                    if (!has_[s & (kWindow - 1)]) { store(retrans_(s), true); ++stats_.retransmitted; }
            }
            drain();
            if (expected_ == start) break;  // nothing moved: cannot happen, but never spin
        }
    }

    Deliver& deliver_;
    Retransmit& retrans_;
    Hooks& hooks_;
    uint64_t tol_;
    uint64_t expected_ = 1, highest_ = 0;
    Packet slot_[kWindow];
    bool has_[kWindow] = {};
    bool rec_[kWindow] = {};
    HandlerStats stats_;
};

}  // namespace feed
