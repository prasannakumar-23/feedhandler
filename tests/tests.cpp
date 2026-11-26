// Dependency-free tests. Build with -fsanitize=thread / address,undefined for the concurrency and memory checks.
#include "feed_handler.h"
#include "gen.h"
#include "spsc_queue.h"

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <thread>
#include <vector>

using namespace feed;

static int g_fail = 0, g_checks = 0;
#define CHECK(c) do { ++g_checks; if (!(c)) { ++g_fail; std::printf("FAIL %s:%d  %s\n", __FILE__, __LINE__, #c); } } while (0)

static void testWire() {
    Packet p{}; p.seq = 42; p.orderId = 7; p.price = -5; p.qty = 9; p.type = 'A'; p.side = 1;
    alignas(8) unsigned char raw[64] = {};
    encode(p, raw + 3);  // deliberately unaligned
    Packet q = decode(raw + 3);
    CHECK(q.seq == 42 && q.orderId == 7 && q.price == -5 && q.qty == 9 && q.side == 1);
    CHECK(valid(q));
    q.qty = 0; CHECK(!valid(q));            // Add with zero quantity is rejected
    q.type = 'Q'; CHECK(!valid(q));         // unknown type is rejected
}

static void testSpscSingleThread() {
    SpscQueue<int, 4> q;
    int v = 0;
    CHECK(!q.try_pop(v));
    for (int i = 0; i < 4; ++i) CHECK(q.try_push(i));
    CHECK(!q.try_push(99));                 // full
    for (int i = 0; i < 4; ++i) { CHECK(q.try_pop(v)); CHECK(v == i); }
    CHECK(!q.try_pop(v));
    for (int round = 0; round < 100; ++round) {  // wraparound
        CHECK(q.try_push(round)); CHECK(q.try_pop(v)); CHECK(v == round);
    }
}

static void testSpscTwoThreads() {
    constexpr uint64_t N = 3000000;
    SpscQueue<uint64_t, 1024> q;
    std::atomic<bool> bad{false};
    std::thread c([&] {
        uint64_t v, expect = 0;
        while (expect < N) if (q.try_pop(v)) { if (v != expect) bad = true; ++expect; }
    });
    for (uint64_t i = 0; i < N; ++i) while (!q.try_push(i)) {}
    c.join();
    CHECK(!bad.load());
}

struct Collect {
    std::vector<uint64_t> seqs; std::vector<bool> rec; Ladder* book = nullptr;
    void operator()(const Packet& p, bool r) { seqs.push_back(p.seq); rec.push_back(r); if (book) book->apply(p); }
};
struct Retr {
    const Generated& g; uint64_t calls = 0;
    Packet operator()(uint64_t s) { ++calls; return g.packets[s - 1]; }
};

static bool exactlyOnceInOrder(const Collect& c, uint64_t n) {
    if (c.seqs.size() != n) return false;
    for (uint64_t i = 0; i < n; ++i) if (c.seqs[i] != i + 1) return false;
    return true;
}

static void testHandlerInOrder() {
    Generated g = generate(500, 1);
    Collect c; Retr r{g}; NoHooks h;
    FeedHandler<Collect, Retr> fh(c, r, h);
    for (auto& p : g.packets) fh.onPacket(p);
    CHECK(exactlyOnceInOrder(c, 500));
    CHECK(r.calls == 0);                    // perfect delivery never asks for a replay
    CHECK(fh.stats().duplicates == 0 && fh.stats().outOfOrder == 0);
}

static void testHandlerSmallReorderNoRetransmit() {
    Generated g = generate(200, 2);
    Collect c; Retr r{g}; NoHooks h;
    FeedHandler<Collect, Retr> fh(c, r, h);
    for (size_t i = 0; i + 1 < g.packets.size(); i += 2) { fh.onPacket(g.packets[i + 1]); fh.onPacket(g.packets[i]); }
    CHECK(exactlyOnceInOrder(c, 200));
    CHECK(r.calls == 0);                    // swaps inside the tolerance heal themselves
    for (bool x : c.rec) CHECK(!x);
}

static void testHandlerLossAndDuplicates() {
    Generated g = generate(100, 3);
    Collect c; Retr r{g}; NoHooks h;
    FeedHandler<Collect, Retr> fh(c, r, h);
    for (auto& p : g.packets) {
        if (p.seq == 10 || p.seq == 11 || p.seq == 50) continue;  // lost
        fh.onPacket(p);
        if (p.seq % 7 == 0) fh.onPacket(p);                       // duplicated
    }
    CHECK(exactlyOnceInOrder(c, 100));
    CHECK(fh.stats().retransmitted == 3);
    CHECK(c.rec[9] && c.rec[10] && c.rec[49] && !c.rec[0] && !c.rec[60]);
    CHECK(fh.stats().duplicates > 0);
}

static void testHandlerTailLossViaHeartbeat() {
    Generated g = generate(100, 4);
    Collect c; Retr r{g}; NoHooks h;
    FeedHandler<Collect, Retr> fh(c, r, h);
    for (auto& p : g.packets) if (p.seq <= 95) fh.onPacket(p);   // 96..100 never arrive
    CHECK(c.seqs.size() == 95);
    Packet hb{}; hb.type = 'H'; hb.seq = 100;
    fh.onPacket(hb);
    CHECK(exactlyOnceInOrder(c, 100));
}

static void testHandlerHoleWiderThanWindow() {
    constexpr uint64_t N = 5000;
    Generated g = generate(N, 5);
    Collect c; Retr r{g}; NoHooks h;
    FeedHandler<Collect, Retr> fh(c, r, h);
    for (auto& p : g.packets) if (p.seq < 10 || p.seq > 3500) fh.onPacket(p);  // 3,491 consecutive losses
    CHECK(exactlyOnceInOrder(c, N));
}

static void testHandlerFuzz() {
    for (uint64_t seed = 1; seed <= 300; ++seed) {
        const uint64_t n = 200 + seed * 13;
        Generated g = generate(n, seed);
        Rng rng(seed * 101);
        const double drop = rng.unit() * 0.4, dup = rng.unit() * 0.3, reo = rng.unit() * 0.5;
        Ladder book(n + 1);
        Collect c; c.book = &book; Retr r{g}; NoHooks h;
        FeedHandler<Collect, Retr> fh(c, r, h, 1 + rng.below(8));
        std::vector<Packet> wire;
        for (auto& p : g.packets) {
            if (rng.unit() < drop) continue;
            wire.push_back(p);
            if (rng.unit() < dup) wire.push_back(p);
        }
        for (size_t i = 0; i + 1 < wire.size(); ++i)                       // local shuffles, up to 5 positions
            if (rng.unit() < reo) std::swap(wire[i], wire[std::min(wire.size() - 1, i + 1 + (size_t)rng.below(5))]);
        for (auto& p : wire) fh.onPacket(p);
        Packet hb{}; hb.type = 'H'; hb.seq = n; fh.onPacket(hb);
        CHECK(exactlyOnceInOrder(c, n));
        CHECK(book.digest() == g.expectedDigest);
        CHECK(book.liveOrders() == g.expectedLive);
    }
}

static void testLadder() {
    Ladder L(10);
    Packet a{}; a.type = 'A'; a.orderId = 1; a.side = 0; a.price = 100; a.qty = 10; a.seq = 1;
    CHECK(L.apply(a));
    CHECK(!L.apply(a));                                  // duplicate id
    const uint64_t d1 = L.digest();
    Packet e{}; e.type = 'E'; e.orderId = 1; e.qty = 4;
    CHECK(L.apply(e));
    CHECK(L.digest() != d1);                             // digest sees partial execution
    e.qty = 99; CHECK(L.apply(e)); CHECK(L.liveOrders() == 0);  // over-execution clamps to what remains
    Packet x{}; x.type = 'X'; x.orderId = 1;
    CHECK(!L.apply(x));                                  // already gone
    Packet far = a; far.orderId = 2; far.price = 1000000;
    CHECK(!L.apply(far));                                // outside the dense window
}

int main() {
    testWire(); testSpscSingleThread(); testSpscTwoThreads();
    testHandlerInOrder(); testHandlerSmallReorderNoRetransmit(); testHandlerLossAndDuplicates();
    testHandlerTailLossViaHeartbeat(); testHandlerHoleWiderThanWindow(); testHandlerFuzz(); testLadder();
    std::printf("%d checks, %d failed\n", g_checks, g_fail);
    return g_fail ? 1 : 0;
}
