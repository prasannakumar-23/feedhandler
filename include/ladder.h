#pragma once
// Aggregated depth book rebuilt from the feed: quantity per price level, per side, in a dense
// tick-indexed window, plus a per-order table so Cancel/Execute messages (which carry only an id)
// can find the level they affect. All storage is allocated up front; apply() never allocates.
#include "wire.h"

#include <cstdint>
#include <vector>

namespace feed {

class Ladder {
public:
    static constexpr int64_t kWindow = 4096;
    explicit Ladder(uint64_t maxOrderId, int64_t basePrice = 0)
        : base_(basePrice), orders_(maxOrderId + 1) {}

    // returns false if the message could not be applied (unknown id, price outside window, ...)
    bool apply(const Packet& p) {
        switch ((MsgType)p.type) {
            case MsgType::Add: {
                const int64_t ix = p.price - base_;
                if (ix < 0 || ix >= kWindow || p.orderId >= orders_.size() || orders_[p.orderId].live) return false;
                orders_[p.orderId] = {p.qty, (int32_t)ix, p.side, 1};
                level(p.side)[ix] += p.qty;
                ++live_;
                return true;
            }
            case MsgType::Cancel: {
                if (p.orderId >= orders_.size() || !orders_[p.orderId].live) return false;
                O& o = orders_[p.orderId];
                level(o.side)[o.ix] -= o.qty;
                o.live = 0; --live_;
                return true;
            }
            case MsgType::Execute: {
                if (p.orderId >= orders_.size() || !orders_[p.orderId].live) return false;
                O& o = orders_[p.orderId];
                const uint32_t q = p.qty < o.qty ? p.qty : o.qty;
                level(o.side)[o.ix] -= q;
                o.qty -= q;
                if (o.qty == 0) { o.live = 0; --live_; }
                return true;
            }
            default: return true;  // heartbeat
        }
    }

    // top-n non-empty levels from the best price outward: {price, qty}. For tools and tests, not hot path.
    template <class F>
    void top(Side s, int n, F&& f) const {
        if (s == Side::Buy) { for (int64_t i = kWindow - 1; i >= 0 && n > 0; --i) if (bid_[i]) { f(base_ + i, bid_[i]); --n; } }
        else                { for (int64_t i = 0; i < kWindow && n > 0; ++i) if (ask_[i]) { f(base_ + i, ask_[i]); --n; } }
    }

    // Order-sensitive hash of both sides: identical books give identical digests.
    uint64_t digest() const {
        uint64_t h = 1469598103934665603ULL;
        auto mix = [&](uint64_t v) { h ^= v; h *= 1099511628211ULL; };
        for (int64_t i = 0; i < kWindow; ++i) if (bid_[i]) { mix((uint64_t)i); mix(bid_[i]); }
        mix(0xB1D5);
        for (int64_t i = 0; i < kWindow; ++i) if (ask_[i]) { mix((uint64_t)i); mix(ask_[i]); }
        mix(live_);
        return h;
    }
    uint64_t liveOrders() const { return live_; }

private:
    struct O { uint32_t qty; int32_t ix; uint8_t side; uint8_t live; };
    uint64_t* level(uint8_t side) { return side == 0 ? bid_ : ask_; }
    int64_t base_;
    uint64_t bid_[kWindow] = {};
    uint64_t ask_[kWindow] = {};
    std::vector<O> orders_;
    uint64_t live_ = 0;
};

}  // namespace feed
