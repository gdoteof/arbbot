# P1 recorder-gate benchmark — baseline (2026-07-23)

Harness: `scripts/bench_replay.py` (Python) and `rust/bins/bench-replay` (Rust),
replaying the same 1M-line slice from the head of
`data/raw-sports/kalshi-2026-07-23.jsonl` (the feed whose burst gapped the
maker probe). Same event routing on both sides: 1,171 snapshots,
966,915 deltas, 0 gaps. Machine: the production box.

## Per-event hot path (µs/event)

| stage                 | Python | Rust  |
|-----------------------|--------|-------|
| json parse            | 1.28 (json) + 3.40 (pydantic) | 0.29 (serde, typed) |
| book apply            | 5.29   | 0.07  |
| **total**             | **9.98** | **0.36** |
| throughput (events/s) | 100k   | 2.79M |

Rust is ~28× faster; both were measured with books actually seeded (a
mid-day slice with no snapshots understates Python book cost ~7×).

## Tape burst profile (recorded arrival times)

- mean 1,061 events/s; peak 1s-window 2,553 events/s; peak 100ms-window ≈ 9,900 events/s
- avg line 186 bytes → the recorder's 1MB slow-subscriber buffer holds ≈ 5,600 lines

## Interpretation (honest)

Raw parse throughput is NOT the gap mechanism: Python has ~39× headroom over
the recorded 1s peak. The gap mechanism is **stall tolerance**: a subscriber
that stops reading is disconnected once ~1MB queues — that is **≈ 2.2s at
peak burst rate** (0.57s at 100ms-burst rate). Any synchronous venue I/O in
the same loop as the socket reader (a hedge with retries, a cold TLS
handshake + 429 backoff) exceeds that during exactly the bursts that produce
fills. The probe's ~10s REST-fallback hedge ladder guarantees a disconnect →
resync → "book gapped when I needed it."

Consequences:
1. **Architecture first**: the reader must never share its thread/loop with
   blocking venue I/O (drain socket → bounded in-memory channel → consumer;
   Rust trader design already does this; Python consumers can approximate
   it). The hedge-anchor ladder removes the book dependency entirely.
2. **Rust's contribution** is the 28× compute margin (micro-bursts stay
   sub-millisecond, no GC pauses on top) and cheap task isolation making the
   reader-never-stalls property structural.
3. **P1 gate metrics** now runnable on any slice: identical event-routing
   counts (parity), µs/event by stage, sustained eps, and — for the live
   shadow — max stall injected without gap + subscriber-disconnect count.
