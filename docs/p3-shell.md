# P3 — the Rust execution shell (arb-trader), gates and soak (2026-07-23)

P2 proved the decision core (scanner + quoter) byte-identical to Python.
P3 wraps that core in the concurrent shell it will run under live, proves the
shell changes nothing, measures it, and starts the live soak. This binary is
DRY-RUN ONLY: no venue order code path, no credentials loaded (arb-recorder's
posture).

## Architecture (sans-IO core, single-writer engine)

    feed task ── bounded mpsc(65536) ──▶ engine task ──▶ per-venue executor tasks
    (socket or tape;                     books+quoters      (token bucket 8/s,
     reconnect loop,                     kill/stats on      dry-run gateway seam,
     never blocks on                    deadlines, no       hop-latency histogram)
     venue I/O)                         per-event syscalls)

- The engine owns all state; zero locks. Time enters as event ts / deadline
  ticks; order ids are injected; effects leave via channels — the impurity
  list the P2 harness had to monkeypatch in Python is structural here.
- Kill switch: 1s deadline stats data/KILL; on set => cancel_all intents +
  quoting pause; on clear => resume. Never a per-event stat (Python does
  ~5.4M stats/day in the hot path).
- Executors own their venue's rate budget, so a slow venue backs up its own
  channel only — the reader-never-stalls property from the P1 postmortem
  (docs/bench-recorder-baseline.md) is enforced by construction.

## Gates passed (2026-07-20 tape, live registry)

| Gate | Result |
|---|---|
| Shell digest = harness digest | `arb-trader --bench-tape` full day: sha256 `3bf83fa164bfabdd387c765b0b0d701dedf03711cbeaad77edfa2cb3a7be6f07`, 80 intents — identical to arb-intent AND Python scripts/intent_replay.py. The concurrent shell provably does not alter decisions. |
| Paced replay (300x) digest | unchanged — pacing does not affect decisions |
| Effect routing | 45 places + 35 cancels routed to executors, 0 drops |

## Benchmarks

Throughput (full-speed bench, channel saturated — latency not meaningful):
5,474,100 events in 55.5s = **98.6k events/s** end-to-end through the full
task pipeline (11x the Python harness's ~31k/s, on top of the 28x hot-path
margin measured in the P1 recorder bench).

Latency (paced bench, `--pace-x 300`: recorded arrival times compressed
300x, i.e. ~24k events/s sustained with bursts 300x worse than real):

| metric (socket-read -> decision done) | value |
|---|---|
| p50 | ~98 µs |
| p90 | ~1.6 ms |
| p99 | ~12.6 ms (burst queueing, high-water 5,038 events) |
| max | 37.8 ms |
| engine -> executor hop p50 / max | 49 µs / 146 µs |

At 1x real load those queues do not form; the live soak's 5-min stats lines
(journal) report the true numbers. Histogram percentiles are log2-bucket
approximations; max is exact. Reminder: hedge latency is ~90% venue RTT —
the shell's contribution is eliminating stall/jitter, not the mean.

## Live soak (started 2026-07-23)

- `arbbot-trader-rs.service`: subscribes to the Rust shadow recorder's
  socket (data/arbbot-rs.sock), quotes the full 2-leg registry with the
  parity-harness conventions (clip 5, no jitter, defaults), appends intents
  to `data/trader-rs/intents.jsonl`, stats to the journal every 5 min.
- `arbbot-trader-gate.timer` (07:50 UTC): scripts/shadow_trader_gate.py
  compares yesterday's Rust intents vs the Python trader's
  data/scan/trader-intents-*.jsonl at the DECISION level (tolerance match).
- Ops: `journalctl --user -u arbbot-trader-rs -f`;
  reports in `data/reports/trader-gate-<day>.txt`;
  stop/revert: `systemctl --user disable --now arbbot-trader-rs`.

Honest caveats for reading the daily gate:
1. It can never be a byte gate: the live Python trader runs size_jitter=2
   (random sizes), live risk caps/balances, ledger-seeded exposure, and the
   tradable-only universe; the shadow quotes every 2-leg non-rejected rel
   with harness defaults. Expect low match rates until/unless the shadow is
   configured to mirror the live runner; the soak's primary metrics are
   stability (disconnects, gaps, memory), latency, and kill-switch behavior.
   Byte-level decision parity is already pinned by the tape gate above.
2. First soak finding (fixed same day): the trader flapped on exact 30s
   boundaries — arb-tape's Broadcaster dropped ANY subscriber whose queued
   bytes passed 1MB, but the welcome/30s-rebroadcast bursts (~1.4MB of full
   books) are enqueued synchronously before the writer task drains, so even
   a fast subscriber tripped it once per heal. Unobserved before because the
   rs socket had never had a real subscriber. Cap raised to 16MB (still
   sheds a truly stalled subscriber in ~35s of peak burst); the soak is the
   regression watch. This is exactly the class of bug the soak exists to
   find before money does.

## Census re-pin + fill-ingestion core (2026-07-24)

**Python feature freeze (Geoff, 2026-07-24):** the Python money path is
feature-locked — bug fixes only until the Rust port lands — so the pins
below are against a NON-MOVING target. Decision-changing bug fixes re-run
the pin and ping the parity handshake card (ca24e4ec); config/parameter
operations (budgets, caps, vetting) are not features and don't invalidate
pins unless they change quoter/runner decision semantics.

The standing census caveat (worktree lagged `main`'s quoter) is closed:

- `src/arbbot/exec/quoter.py` synced from `main`'s working state (toxgate,
  APR hurdle `min_apr`/`resolve_years`, maker-unwind `suppress` set) and all
  three features ported to the Rust quoter (`arb-core/src/quoter.rs`).
- **Gate A (regression, defaults off):** old Python quoter, new Python
  quoter, Rust `arb-intent`, AND `arb-trader --bench-tape` (full shell) all
  reproduce the pinned digest `3bf83fa1…7be6f07` / 80 intents on the
  2026-07-20 tape (5,474,100 events) — main's additions are provably
  default-off, and the port is regression-free.
- **Gate B (features on, new parity fixtures):** harness flags added to both
  sides (`--min-apr --resolve-years --toxgate-file --suppress`).
  toxgate+suppress fixture: 59,471 intents (incl. toxgate skip records),
  digest `c889fe27…73273e5`, Python-vs-Rust diff empty. APR-hurdle combined
  fixture: 15 intents, `f8d4db04…deb683`, diff empty. Note the toxgate feed
  file-stat is an impurity in Python; the Rust port takes the feed as data
  (`Toxgate` passed in), so in the engine it arrives as Control-style state,
  never a syscall in the fold.
- **Fill-ingestion core landed** (`arb-core/src/fill.rs`): `FillLedger::
  observe_cum_fill(oid, cum) -> Option<HedgeObligation>` — cumulative
  semantics make both fill paths (private WS + poll) idempotent by
  construction; `HedgeObligation` is non-clonable, `#[must_use]`,
  mintable only by the ledger, carries its quote-time `HedgeAnchor`
  (burst-gap postmortem), and bumps a process-wide counter if dropped
  unconsumed. 8 unit tests incl. duplicate/stale reports, overfill clamp,
  fill-racing-cancel, drop-alarm. Engine wiring arrives with the private-WS
  feed task (P4 item 1's I/O half).

## Engine WAL + fill ingestion (2026-07-24) — P4 items 1 (sans-IO half) and 2

- **WAL (`--wal PATH`, `bins/arb-trader/src/wal.rs`).** Every event that
  leaves the feed channel is stamped at the engine's single merge point and
  appended as `{"seq":N,"line":"<original line, verbatim>"}` — before parsing,
  so lines the engine skips are in the incident record too. The writer is a
  dedicated OS thread behind a bounded (65,536) channel: the engine never
  touches the disk. **Overflow is crash-stop**, not drop — a WAL with silent
  holes replays "successfully" into a state the live engine never occupied,
  which is worse than no WAL.
- **Replay (`--replay-wal PATH`,** mutually exclusive with
  `--socket`/`--bench-tape`**)** feeds the embedded lines back through the
  identical engine path in seq order.
- **Gate (full 2026-07-20 tape, 5,474,100 events):** `--bench-tape --wal`
  → `3bf83fa1…7be6f07`, 80 intents (the pin, unchanged: the WAL is decision-
  neutral); `--replay-wal` of the 2.30 GB WAL it produced → the same digest
  and a byte-identical intent file. WAL line count = event count exactly.
  Cost: 66.1s vs 56.8s at channel saturation (83k vs 99k events/s) — paid
  only in the throughput bench, where the engine is 100% busy.
- **Fill ingestion.** Two new event kinds ride the SAME ordered channel:
  `order_ack` and `fill` (`{"kind":"fill","order_id","cum","venue",
  "market_id","ts_local_ns"}`; `cum` is cumulative, which is what makes the
  private-WS and poll paths idempotent against each other). Orders enter
  `arb_core::fill::FillLedger` at intent-drain time with the quote-time
  `HedgeAnchor` — the top of the OTHER leg on the side the hedge would take,
  the same side `Quoter::hedge_has_depth` gates the place on. A minted
  `HedgeObligation` emits a canonical, digest-visible intent line
  `{"anchor_price","hedge_needed":<hedge leg market>,"order_id","qty","ts"}`
  and is consumed via `into_parts` — the obligation surface only; placement
  policy comes with the venue write path. `dropped_unconsumed` is in the
  stats summary and must stay 0.
- **Regression:** the frozen tape contains no fill events, so its digest is
  unchanged with and without `--wal`. Fill semantics are gated instead by a
  self-contained synthetic tape (`bins/arb-trader/tests/fixtures/`, invented ids)
  exercising duplicate cum reports, a fill racing a replace, a foreign order
  id, and an anchor that survives a hedge-leg book move —
  `bins/arb-trader/tests/determinism.rs` asserts two runs are byte-identical
  and that WAL replay reproduces them.

## What P4 needs (the remaining provables before money)

1. Fill ingestion as ordered events (private WS -> OrderAck/Fill into the
   SAME engine channel) — *sans-IO half landed above; the private-WS feed
   task and* hedge placement through a real gateway executor remain — the
   first venue-write code path, behind the seam the dry-run executor
   already defines.
2. ~~Engine-sequenced WAL~~ — **landed** (above).
3. Deadline-driven exposure alarms + hedge-retry escalation (ports the
   systemd-timer behavior into the engine's deadline wheel).
4. Risk manager port (caps, kill, balances) with its own parity fixtures.

### Strategy layer (P4, from docs/strategy-contract.md v2 — design-only 2026-07-23)

**The engine is the sole executor.** In the end state exactly one process
places, amends, or cancels orders; every strategy — in-engine template,
bespoke Rust module, Python research sidecar, eventually lifecycle/manual
tools — is an intent *proposer* whose output passes the same arbitration
(contract §6 a–f) before touching a venue. Python never places orders in the
target architecture; it becomes untrusted by construction.

Topology gains one task next to the feed and executors:

    feed task ──────────────┐
                            ├─ mpsc ──▶ engine task ──▶ per-venue executors
    intent-gateway task ────┘           (templates +      (rate buckets =
    (data/intents.sock,                  arbitration)      budget_share)
     line-JSON, external
     intents in / verdicts
     + acks/fills out)

The gateway (contract §5) is a unix stream socket with the recorder socket's
conventions: line-JSON, hello-with-`strategy_id`, accept/reject-with-reason
per intent, acks/fills pushed back, idempotent `intent_id`s, bounded queues —
the engine never blocks on a slow client. Every external intent enters the
single event channel as an `ExternalIntent` event, so gateway traffic is in
the WAL and replays byte-exactly.

Most strategies are not trait impls but **family templates** (contract §3):
one verified Rust implementation per shape — `maker-hedge`,
`take-take-cross`, `directional-signal` — instantiated from
`config/strategies.yaml` entries whose typed `params` ARE the strategy.
Bespoke logic that fits no family implements the trait directly:

```rust
/// Pure fold: no clock, no I/O, no randomness (jitter seeds arrive as
/// Control events). Same Event enum as the engine; time is an event field.
trait Strategy {
    fn id(&self) -> StrategyId;                    // from config/strategies.yaml
    fn on_event(&mut self, ev: &Event) -> Vec<Intent>;
}

// Registration: one ordered Vec at engine construction — registration order
// IS evaluation order (contract §6d, deterministic multi-strategy replay).
// Family templates are constructed from manifest entries and registered the
// same way as bespoke modules.
fn register(strategies: Vec<Box<dyn Strategy>>, manifest: &CompiledManifest) -> Engine;
```

Arbitration hooks (engine-side, between proposer output and executor input —
identical for every source, in-process or gateway):

1. **claims gate** — intent's market ∈ the emitter's compiled
   `strategy_claim` rows, else Reject event + alarm (§6a; replaces the three
   probe-log greps and the inert ownership table).
2. **self-cross gate** — marketable intent vs any own resting order (any
   strategy) → Reject (§6b; kills the wash-fill class).
3. **shared risk view** — one exposure fold consulted per intent (§6c;
   replaces the per-process private caps).
4. **kill/pause** — global `data/KILL` Control halts every strategy, no
   opt-out; gateway intents rejected with `reason: kill`; per-strategy pause
   by Control(id) (§6e).
5. **venue budget** — manifest `budget_share` enforced in the per-venue
   executor tasks' token buckets (§6f).

Rejections are events in the WAL — arbitration decisions replay byte-exactly
like everything else.

**Migration gate = intent parity per FAMILY.** A family template is verified
once: replaying each covered strategy's tape through the harness with that
strategy's manifest `params` must produce an intent stream that diffs empty
against the Python implementation (same gate P2/P3 established for the
quoter — `trader-intents-*.jsonl` diff). One gate per family — after it
passes, per-strategy differences are config diffs reviewed as **data, not
code**, and adding a strategy to a verified family re-runs the replay with
new params rather than re-proving new logic. Bespoke `rust-module` strategies
still get an individual parity gate. Multi-strategy determinism (§6d) then
pins a digest over the merged stream. Per the migration table (contract §7):
`maker-hedge` verifies first against `make-take` (then covers
`pmus-maker-probe` by params), `take-take-cross` against the in-runner
take-take + sports-take-take, `directional-signal` against leadlag-probe /
pm-lean; probes and lifecycle scripts run as `external-intents` gateway
sidecars under enforced caps, and the diverse auditor (reconcile/mark) stays
read-only Python permanently.
