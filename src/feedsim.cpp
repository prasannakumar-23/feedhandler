// feedsim: UDP market-data pipeline demo.
//   run    sender thread --UDP loopback--> receiver thread (gap handling) --SPSC ring--> book thread
//   trace  the same handler + ladder driven by a seeded simulated channel, printed as JSON (for the web replay)
//   spsc   ring-buffer throughput microbenchmark
#include "feed_handler.h"
#include "gen.h"
#include "ladder.h"
#include "spsc_queue.h"

#include <algorithm>
#include <arpa/inet.h>
#include <atomic>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <netinet/in.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>
#include <vector>
#ifdef __linux__
#include <pthread.h>
#include <sched.h>
#endif

using namespace feed;

namespace {

uint64_t nowNs() {
    return (uint64_t)std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
}
inline void cpuRelax() {
#if defined(__x86_64__)
    __builtin_ia32_pause();
#elif defined(__aarch64__)
    asm volatile("yield");
#endif
}
void pinTo(int core) {
#ifdef __linux__
    if (core < 0) return;
    cpu_set_t set; CPU_ZERO(&set); CPU_SET(core, &set);
    pthread_setaffinity_np(pthread_self(), sizeof set, &set);
#else
    (void)core;
#endif
}

struct Args {
    uint64_t packets = 300000, seed = 1;
    double drop = 0.02, dup = 0.01, reorder = 0.01;
    int burst = 100;
    int recvCore = -1, bookCore = -1;
    std::string trace;
};
Args parse(int argc, char** argv, int from) {
    Args a;
    for (int i = from; i < argc; ++i) {
        std::string k = argv[i];
        auto val = [&]() { return i + 1 < argc ? argv[++i] : (char*)"0"; };
        if (k == "--packets") a.packets = std::strtoull(val(), nullptr, 10);
        else if (k == "--seed") a.seed = std::strtoull(val(), nullptr, 10);
        else if (k == "--drop") a.drop = std::atof(val());
        else if (k == "--dup") a.dup = std::atof(val());
        else if (k == "--reorder") a.reorder = std::atof(val());
        else if (k == "--burst") a.burst = std::atoi(val());
        else if (k == "--cores") { int r = 0, b = 0; std::sscanf(val(), "%d,%d", &r, &b); a.recvCore = r; a.bookCore = b; }
    }
    return a;
}

struct alignas(64) Item { Packet p; uint64_t enqNs; uint8_t recovered; };
using Queue = SpscQueue<Item, 65536>;

void pct(const char* name, std::vector<uint32_t>& v) {
    if (v.empty()) { std::printf("  %-28s no samples\n", name); return; }
    std::sort(v.begin(), v.end());
    auto q = [&](double p) { return v[(size_t)(p * (double)(v.size() - 1))]; };
    std::printf("  %-28s n=%-8zu p50=%-7u p90=%-7u p99=%-8u p99.9=%-8u max=%u   (ns)\n", name, v.size(), q(.5), q(.9), q(.99), q(.999), v.back());
}

// ------------------------------------------------------------------------------------------ run
int cmdRun(const Args& a) {
    Generated g = generate(a.packets, a.seed);
    const uint64_t N = a.packets;

    int rx = socket(AF_INET, SOCK_DGRAM, 0), tx = socket(AF_INET, SOCK_DGRAM, 0);
    if (rx < 0 || tx < 0) { std::perror("socket"); return 1; }
    int buf = 16 << 20;
    setsockopt(rx, SOL_SOCKET, SO_RCVBUF, &buf, sizeof buf);
    sockaddr_in addr{}; addr.sin_family = AF_INET; addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK); addr.sin_port = 0;
    if (bind(rx, (sockaddr*)&addr, sizeof addr) < 0) { std::perror("bind"); return 1; }
    socklen_t len = sizeof addr; getsockname(rx, (sockaddr*)&addr, &len);
    if (connect(tx, (sockaddr*)&addr, sizeof addr) < 0) { std::perror("connect"); return 1; }


    Queue q;
    std::atomic<bool> rxDone{false};
    std::atomic<uint64_t> sentDatagrams{0}, injectedDrops{0}, injectedDups{0}, injectedReorders{0};
    std::vector<uint32_t> handoff, e2e;
    handoff.reserve(N); e2e.reserve(N);
    uint64_t pushFull = 0, applyFail = 0;
    Ladder book(N + 1);

    // book thread: pop in-order messages, apply to the ladder
    std::thread bookT([&] {
        pinTo(a.bookCore);
        Item it;
        for (;;) {
            if (q.try_pop(it)) {
                const uint64_t t = nowNs();
                if (!book.apply(it.p)) ++applyFail;
                handoff.push_back((uint32_t)std::min<uint64_t>(t - it.enqNs, 0xFFFFFFFFu));
                if (!it.recovered) e2e.push_back((uint32_t)std::min<uint64_t>(nowNs() - it.p.sendNs, 0xFFFFFFFFu));
            } else if (rxDone.load(std::memory_order_acquire)) {
                if (!q.try_pop(it)) break;
                if (!book.apply(it.p)) ++applyFail;
            } else cpuRelax();
        }
    });

    // receiver thread: UDP -> handler (sequence/gap logic) -> SPSC
    HandlerStats hs{};
    std::thread rxT([&] {
        pinTo(a.recvCore);
        struct Sink { Queue& q; uint64_t& full; void operator()(const Packet& p, bool rec) {
            Item it{p, nowNs(), (uint8_t)rec};
            while (!q.try_push(it)) { ++full; cpuRelax(); }
        } } sink{q, pushFull};
        struct Retr { const Generated& g; Packet operator()(uint64_t seq) { return g.packets[seq - 1]; } } retr{g};
        NoHooks hooks;
        FeedHandler<Sink, Retr, NoHooks> h(sink, retr, hooks);
        unsigned char b[64];
        while (h.nextExpected() <= N) {
            const ssize_t n = recv(rx, b, sizeof b, MSG_DONTWAIT);
            if (n == (ssize_t)sizeof(Packet)) { Packet p = decode(b); if (valid(p)) h.onPacket(p); }
            else cpuRelax();
        }
        hs = h.stats();
        rxDone.store(true, std::memory_order_release);
    });

    // sender (this thread): bursts of `burst` datagrams, then a ~1 ms pause. Injects loss/dup/reorder.
    Rng r(a.seed * 7919 + 13);
    Packet held{}; bool haveHeld = false;
    auto sendP = [&](Packet p) { p.sendNs = nowNs(); unsigned char b[sizeof p]; encode(p, b); send(tx, b, sizeof b, 0); sentDatagrams++; };
    const uint64_t t0 = nowNs();
    for (uint64_t i = 0; i < N; ++i) {
        Packet p = g.packets[i];
        if (r.unit() < a.drop) { injectedDrops++; }
        else if (haveHeld) { sendP(p); sendP(held); haveHeld = false; }                 // reordered pair goes out swapped
        else if (r.unit() < a.reorder) { held = p; haveHeld = true; injectedReorders++; }
        else { sendP(p); if (r.unit() < a.dup) { sendP(p); injectedDups++; } }
        if ((i + 1) % (uint64_t)a.burst == 0) {
            std::this_thread::sleep_for(std::chrono::microseconds(1000));
            if (haveHeld && r.unit() < 0.5) { sendP(held); haveHeld = false; }
        }
    }
    if (haveHeld) sendP(held);
    Packet hb{}; hb.type = (uint8_t)MsgType::Heartbeat; hb.seq = N;
    while (!rxDone.load(std::memory_order_acquire)) { sendP(hb); std::this_thread::sleep_for(std::chrono::milliseconds(2)); }
    const double secs = (double)(nowNs() - t0) * 1e-9;
    rxT.join(); bookT.join();

    const bool ok = book.digest() == g.expectedDigest && book.liveOrders() == g.expectedLive && applyFail == 0 && hs.delivered == N;
    std::printf("packets=%llu  sent datagrams=%llu  injected: drop=%llu dup=%llu reorder=%llu  (%.1fs)\n",
        (unsigned long long)N, (unsigned long long)sentDatagrams.load(), (unsigned long long)injectedDrops.load(),
        (unsigned long long)injectedDups.load(), (unsigned long long)injectedReorders.load(), secs);
    std::printf("handler: delivered=%llu duplicates=%llu out_of_order=%llu retransmitted=%llu gap_events=%llu  ring_full_spins=%llu\n",
        (unsigned long long)hs.delivered, (unsigned long long)hs.duplicates, (unsigned long long)hs.outOfOrder,
        (unsigned long long)hs.retransmitted, (unsigned long long)hs.gapEvents, (unsigned long long)pushFull);
    pct("ring hand-off (push->apply)", handoff);
    pct("end-to-end (send->apply)", e2e);
    std::printf("book digest %016llx vs sender %016llx, live orders %llu: %s\n", (unsigned long long)book.digest(),
        (unsigned long long)g.expectedDigest, (unsigned long long)book.liveOrders(), ok ? "MATCH" : "MISMATCH");
    return ok ? 0 : 2;
}

// ------------------------------------------------------------------------------------------ trace
struct TraceOut {
    std::string events;
    void add(const std::string& s) { if (!events.empty()) events += ','; events += s; }
};
std::string book5(const Ladder& L) {
    std::string s = "\"bids\":[";
    bool f = true;
    L.top(Side::Buy, 6, [&](int64_t px, uint64_t q) { s += (f ? "" : ","); f = false; s += "[" + std::to_string(px) + "," + std::to_string(q) + "]"; });
    s += "],\"asks\":["; f = true;
    L.top(Side::Sell, 6, [&](int64_t px, uint64_t q) { s += (f ? "" : ","); f = false; s += "[" + std::to_string(px) + "," + std::to_string(q) + "]"; });
    return s + "]";
}
int cmdTrace(Args a) {
    Generated g = generate(a.packets, a.seed);
    Ladder book(a.packets + 1);
    Rng r(a.seed * 31 + 5);
    TraceOut out;
    // event under construction for the current arrival
    std::string delivered, extra;
    struct Sink { Ladder& L; std::string& d; void operator()(const Packet& p, bool rec) {
        L.apply(p);
        if (!d.empty()) d += ',';
        d += "[" + std::to_string(p.seq) + "," + (rec ? "1" : "0") + "]";
    } } sink{book, delivered};
    struct Retr { const Generated& g; Packet operator()(uint64_t s) { return g.packets[s - 1]; } } retr{g};
    struct Hooks {
        std::string& x;
        void held(const Packet&) { x += ",\"held\":true"; }
        void duplicate(const Packet&) { x += ",\"dup\":true"; }
        void gap(uint64_t f, uint64_t t, uint64_t m) { x += ",\"gap\":[" + std::to_string(f) + "," + std::to_string(t) + "," + std::to_string(m) + "]"; }
    } hooks{extra};
    FeedHandler<Sink, Retr, Hooks> h(sink, retr, hooks);

    auto describe = [&](const Packet& p) {
        std::string s = "\"seq\":" + std::to_string(p.seq) + ",\"t\":\"" + (char)p.type + "\",\"id\":" + std::to_string(p.orderId);
        if ((MsgType)p.type == MsgType::Add) s += std::string(",\"side\":\"") + (p.side ? "S" : "B") + "\",\"px\":" + std::to_string(p.price) + ",\"qty\":" + std::to_string(p.qty);
        if ((MsgType)p.type == MsgType::Execute) s += ",\"qty\":" + std::to_string(p.qty);
        return s;
    };
    auto arrive = [&](const Packet& p) {
        delivered.clear(); extra.clear();
        h.onPacket(p);
        out.add("{\"k\":\"rx\"," + describe(p) + extra + ",\"out\":[" + delivered + "],\"next\":" + std::to_string(h.nextExpected()) + "," + book5(book) + "}");
    };
    Packet held{}; bool haveHeld = false;
    for (uint64_t i = 0; i < a.packets; ++i) {
        Packet p = g.packets[i];
        if (r.unit() < a.drop) out.add("{\"k\":\"lost\",\"seq\":" + std::to_string(p.seq) + "}");
        else if (haveHeld) { arrive(p); arrive(held); haveHeld = false; }
        else if (r.unit() < a.reorder) { held = p; haveHeld = true; }
        else { arrive(p); if (r.unit() < a.dup) arrive(p); }
    }
    if (haveHeld) arrive(held);
    Packet hb{}; hb.type = (uint8_t)MsgType::Heartbeat; hb.seq = a.packets;
    arrive(hb);
    const bool ok = book.digest() == g.expectedDigest;
    std::printf("{\"seed\":%llu,\"packets\":%llu,\"drop\":%.3f,\"dup\":%.3f,\"reorder\":%.3f,\"delivered\":%llu,\"duplicates\":%llu,\"outOfOrder\":%llu,\"retransmitted\":%llu,"
                "\"digestMatches\":%s,\"digest\":\"%016llx\",\"events\":[%s]}\n",
        (unsigned long long)a.seed, (unsigned long long)a.packets, a.drop, a.dup, a.reorder, (unsigned long long)h.stats().delivered,
        (unsigned long long)h.stats().duplicates, (unsigned long long)h.stats().outOfOrder, (unsigned long long)h.stats().retransmitted,
        ok ? "true" : "false", (unsigned long long)book.digest(), out.events.c_str());
    return ok ? 0 : 2;
}

// ------------------------------------------------------------------------------------------ spsc
int cmdSpsc(const Args& a) {
    const uint64_t N = a.packets * 30;  // default 9M items
    SpscQueue<uint64_t, 4096> q;
    std::atomic<bool> go{false};
    uint64_t sum = 0;
    std::thread cons([&] {
        pinTo(a.bookCore);
        while (!go.load(std::memory_order_acquire)) cpuRelax();
        uint64_t v, got = 0, expect = 0;
        while (got < N) if (q.try_pop(v)) { if (v != expect++) { std::printf("ORDER VIOLATION\n"); std::exit(3); } sum += v; ++got; } else cpuRelax();
    });
    pinTo(a.recvCore);
    go.store(true, std::memory_order_release);
    const uint64_t t0 = nowNs();
    for (uint64_t i = 0; i < N; ++i) while (!q.try_push(i)) cpuRelax();
    cons.join();
    const double s = (double)(nowNs() - t0) * 1e-9;
    std::printf("SPSC ring: %llu items through a 4096-slot queue in %.3fs = %.1f M items/s, order preserved, checksum %s\n",
        (unsigned long long)N, s, (double)N / s / 1e6, sum == N * (N - 1) / 2 ? "ok" : "BAD");
    return sum == N * (N - 1) / 2 ? 0 : 3;
}

}  // namespace

int main(int argc, char** argv) {
    const std::string mode = argc > 1 ? argv[1] : "run";
    Args a = parse(argc, argv, 2);
    if (mode == "run") return cmdRun(a);
    if (mode == "trace") { if (a.packets == 300000) a.packets = 300; return cmdTrace(a); }
    if (mode == "spsc") return cmdSpsc(a);
    std::fprintf(stderr, "usage: feedsim run|trace|spsc [--packets N --seed S --drop p --dup p --reorder p --burst B --cores rx,book]\n");
    return 1;
}
