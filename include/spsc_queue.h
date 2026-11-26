#pragma once
// Bounded single-producer / single-consumer lock-free ring buffer.
//  * Capacity is a power of two, so the index wrap is a mask.
//  * head (consumer) and tail (producer) live on separate cache lines to avoid false sharing.
//  * Each side keeps a cached copy of the other side's index and re-reads the shared atomic only when
//    the cached value says full/empty, so the common case touches no contended cache line.
//  * Memory ordering: the producer publishes with a release store of tail, the consumer observes it
//    with an acquire load (and vice versa for head), which is exactly the happens-before needed for
//    the slot contents. Nothing stronger is used.
#include <atomic>
#include <cstddef>
#include <memory>

namespace feed {

template <class T, size_t N>
class SpscQueue {
    static_assert(N >= 2 && (N & (N - 1)) == 0, "capacity must be a power of two");

public:
    SpscQueue() : buf_(new T[N]) {}

    // producer thread only
    bool try_push(const T& v) {
        const size_t t = prod_.tail.load(std::memory_order_relaxed);
        if (t - prod_.cachedHead == N) {
            prod_.cachedHead = cons_.head.load(std::memory_order_acquire);
            if (t - prod_.cachedHead == N) return false;  // full
        }
        buf_[t & (N - 1)] = v;
        prod_.tail.store(t + 1, std::memory_order_release);
        return true;
    }

    // consumer thread only
    bool try_pop(T& out) {
        const size_t h = cons_.head.load(std::memory_order_relaxed);
        if (h == cons_.cachedTail) {
            cons_.cachedTail = prod_.tail.load(std::memory_order_acquire);
            if (h == cons_.cachedTail) return false;  // empty
        }
        out = buf_[h & (N - 1)];
        cons_.head.store(h + 1, std::memory_order_release);
        return true;
    }

    static constexpr size_t capacity() { return N; }

private:
    struct alignas(64) Consumer { std::atomic<size_t> head{0}; size_t cachedTail = 0; };
    struct alignas(64) Producer { std::atomic<size_t> tail{0}; size_t cachedHead = 0; };
    Consumer cons_;
    Producer prod_;
    std::unique_ptr<T[]> buf_;
};

}  // namespace feed
