# feedhandler

A small C++17 market-data pipeline: a binary UDP feed with injected packet loss, duplication and
reordering is received, repaired, passed through a lock-free ring buffer to a second thread, and
rebuilt into a depth book. The point is the engineering around the data, not the book:

```
sender ──UDP loopback──▶ receiver thread ──SPSC ring──▶ book thread
 (drops, dups,            parse, sequence check,          applies in-order messages,
  reorders)               gap detect + replay             records latency
```

| Piece | File | What it shows |
|---|---|---|
| 40-byte packed wire message, memcpy decode | `include/wire.h` | zero-copy-style binary protocol, no UB on unaligned input |
| Lock-free SPSC ring buffer | `include/spsc_queue.h` | cache-line separation, cached indices, acquire/release only |
| Sequence-gap handler | `include/feed_handler.h` | loss / duplicate / reorder / tail-loss recovery, fixed window, no allocation after construction |
| Depth ladder + digest | `include/ladder.h` | dense tick-indexed levels, order-sensitive hash for end-to-end checks |
| Simulator and measurements | `src/feedsim.cpp` | `run`, `spsc`, `trace` modes |

## Build and run
```
make            # feedsim + tests
make check      # 1,440 checks
make tsan       # tests under ThreadSanitizer
make asan       # tests under ASan + UBSan
./feedsim run   --packets 300000 --drop 0.02 --dup 0.01 --reorder 0.01 --cores 0,1
./feedsim spsc  --cores 0,1
```
`run` finishes with `MATCH` only if the receiver's book digest equals the digest the generator computed by
applying every message in order. That has to hold however many packets were lost on the way.

## Correctness
* Property test: 300 seeds with random loss up to 40%, duplicates, local reordering and random reorder
  tolerance. Every sequence number is delivered exactly once, in order, and the rebuilt book digest equals
  the sender's.
* Targeted tests: perfect delivery asks for no replay; small swaps heal without a replay; a 3,491-packet hole
  wider than the 1,024-slot window; tail loss detected from a heartbeat.
* Mutation check of the tests themselves: 7 deliberate bugs (duplicate not dropped, wrong reorder
  tolerance, heartbeat recovery removed, window logic removed, queue full-check off by one, execution
  not clamped, relaxed publish in the queue). The tests catch all of them, but the last one (the queue
  publishing with relaxed instead of release ordering) is only caught by ThreadSanitizer. x86's strong
  memory model hides that bug at runtime, so run `make tsan`; it would also show up on ARM.

## Measurements
Numbers depend heavily on the machine. The values below were taken on a shared 2-vCPU Linux sandbox and are
indicative only; re-run on your hardware before quoting anything. Latency includes `steady_clock` call cost.
Retransmission is simulated by a function call (a stand-in for a replay channel), and loss injection happens
in the sender, so the kernel's own drops are not modelled.

## Not done
No real replay or snapshot channel, no multicast, no kernel bypass, single instrument, no price outside the
4,096-tick window. It is a study of the receive path, not an exchange connection.

MIT licence.

## Replay page
`docs/index.html` is a static page that replays a recorded run (`docs/trace.json`, produced by `make trace`).
Serve the `docs` folder with any static host, for example GitHub Pages (Settings, Pages, branch `main`, folder `/docs`).
