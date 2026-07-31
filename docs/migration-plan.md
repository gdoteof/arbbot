# Rust migration plan — cutover order, gates, rollbacks (2026-07-23)

Strangler order: **reads first, cancels before places, post-only before
taker, one relationship before many.** Every step ships only behind a passed
gate plus a soak, is reversible by a systemd/config flip, and gets a board
approval card before anything authoritative or money-touching flips
(deploys stay Geoff-gated).

Standing principle: **the auditor stays diverse.** The Python reconciler
(60s recon net, naked-leg detection, settle gates) is NOT ported on the
same schedule as the trader — an independent implementation in a different
language auditing venue truth is defense in depth, and it keeps auditing
the Rust trader after cutover. Port it last, if ever.

## M0 — now (both soaks running)

Rust recorder shadowing (P1, daily tape gate); Rust trader shell dry-run
(P3, daily decision gate + stability stats). Python is authoritative for
everything; nothing Rust can touch an order.

## M1 — recorder cutover: the FIRST Python replacement

What flips: `arbbot-recorder.service` runs `arb-recorder` as the
authoritative tape + socket; the Python recorder takes the shadow role
(role swap, both keep running) for 1 week, then retires to standby.
Consumers (scanner daemon, Python trader, dash freshness) just follow the
socket; tape consumers are already format-gated by `--parse-check`
(byte-identity) and the daily shadow gate.

- Gate: **one** green `arb-shadow-gate` run on the image being promoted; gap
  counter <= Python's; subscriber stability post-broadcaster-fix (zero
  unexplained disconnects); parse-check PASS over the window the gate reads.
  The gate is `arb-shadow-gate` (`systemd/arbbot-shadow-gate.{service,
  timer}`). It checks subscriber stability by ATTACHING a subscriber, because
  the recorder hang of 2026-07-29 only ever appeared under CPU contention and
  a tape diff would have been green through it.

  **The "7 consecutive green days" clause was REMOVED on Geoff's call
  2026-07-31.** It had never been satisfied — and, more to the point, never
  could be by the machinery that existed: the clause was a manual grep in §1
  of the runbook (the binary judges one day and knows nothing about any other
  run), the gate that was supposed to feed it had been an uninstalled unit
  emitting a 153-byte missing-file error since 2026-07-24, and the one run
  that ever produced a verdict said FAIL. A week of green days is only
  evidence if something is actually collecting them; what it bought in
  practice was an indefinite hold. One green run against the promotion image
  plus a two-minute rollback is the trade being made instead.

  Still NOT checked, and one green run does not cover it any more than seven
  would have: **"parse-check PASS"** is only partly covered — the gate decodes
  a byte-bounded trailing slice of the CURRENT day (~1% of a 6 GB tape) and
  never invokes `arb-recorder --parse-check`. The "gap counter <= Python's"
  clause is not checkable as written: the two recorders' sequence numbers do
  not measure the same thing (Rust synthesizes a +1 per-market counter,
  Python's Kalshi tape carries the raw per-subscription wire seq), so the gate
  requires the Rust tape's own count to be ZERO instead.
- Risk: none to positions (read-only key; no order code path in binary).
- Rollback: **not a unit swap.** The flip is two flags on the armed engine
  (`--socket`, `--health`) and both recorders keep running, so rollback is
  the same two flags back and there is no hole to backfill — the Python
  recorder never stopped. Full sequence: `docs/recorder-cutover-runbook.md`.
- Approval: board card before the flip.

## M2 — venue-write hello world: the FIRST Rust to touch an order

A one-shot `arb-order` bin (analogue of scripts/diag_kalshi_order.py):
sign, place ONE 1-contract post-only order far off-touch, GET it back,
cancel it, verify cancelled, exit. Run manually, Geoff aware, during quiet
hours. Exercises the signing, endpoint, order-id extraction,
cancel-404-is-success, and tick-format quirks against the live venue with
zero standing risk (worst case: a $0.01-cost fill to flatten by hand).

- Gate: the venue-quirk MockTransport tests (board card 7fab301e) pass
  first; then the live smoke succeeds on Kalshi, then PM-US.
- This is deliberately boring. Its only job is to make the first
  venue-write a non-event.

## M3 — first ECONOMIC trade: one-relationship vertical slice

Rust quotes, detects fills, hedges, and ledger-records ONE vetted
relationship end-to-end; Python is excluded from that relationship by
config (per-relationship ownership, per the workstream contract — never
two writers on one rel). Tight caps (clip <= 5, per-rel cap at probe
scale). Everything else stays Python.

Prerequisites (P4 build, each with its own parity fixture):
1. Fill ingestion as ordered events (private WS + poll fallback ->
   OrderAck/Fill into the engine channel; idempotence on cumQuantity).
2. Real gateway executors behind the existing dry-run seam (place/cancel/
   taker-hedge; rate budget already owned per venue).
3. Risk-manager port (caps, kill, balances) + kill-switch sweep.
4. Engine-sequenced WAL at the merge point, so any live incident replays
   byte-exactly through the parity harness.
5. Ledger writes in the exact dual-append record shape (the P0 SQLite
   import-parity gate is the compatibility test).

- Gate: rehearsal pass (429-proves-auth pattern) + a supervised first
  session; the Python reconciler and naked-leg hedger MUST see and accept
  the Rust slice's positions before it runs unsupervised (the auditor
  audits the new trader from day one).
- Rollback: config flip returns the rel to Python; kill file halts Rust
  quoting instantly (mechanism already live in the shadow).
- Approval: board card; rel selection is the trading agent's call
  (liquid, low oracle risk, probe-scale caps).

## M4 — widen rel-by-rel

Move relationships from the Python trader's universe to the Rust engine's
in config batches, gated on each batch's first-week ledger/recon record
being clean. The systemd timers migrate INTO the engine's deadline wheel
as their functions are absorbed (hedge-retry escalation and max-age alarms
first, unwind policy later); each timer retires only after its in-engine
replacement has fired correctly in production at least once. Sports engine
migrates last (its own WS stack and card).

## M5 — retire the Python trader

When its universe is empty: unit disabled, kept installed for one release
as the rollback target. The Python reconciler, settle gate, and dash stay.
Dash feed moves to Rust only after the accounting kernel (board card
a6ad626d) has produced the pinned accounting-parity digest.

## Summary of "firsts"

| First... | What | When |
|---|---|---|
| Python replaced | recorder (M1) | after one green gate run on the promotion image |
| Rust touches an order | arb-order smoke, 1 contract post-only, immediately cancelled (M2) | after quirk tests land |
| Rust trades with money | one-rel vertical slice at probe caps (M3) | after P4 build + rehearsal |
| Python trader gone | M5 | when config says universe is empty |
