# Quality campaign — the regression gate

Every PR in the 2026-07-28 code-quality campaign must clear the gates below
before it merges. Most of them are cheap; the real-tape digests are the ones
that prove a refactor changed no decision.

Run them with `scripts/gate.sh` — that script IS this document, executable, and
it is the version that counts. It lived in a scratch directory outside the repo
until 2026-07-29, which meant nobody could reproduce a gate run they had not
personally been handed, and no PR could change it under review.

## The gates

Stage 0 is the staleness check, and it is the only one that touches the network
and the only one that can exit before running anything: it fetches `origin/main`
and refuses to proceed if this branch is behind, because gating a branch is not
gating the merge (see "Why stage 0 exists" below). It fails rather than
proceeding if the fetch itself fails — an unverified base is not a fresh one.

```bash
cd rust
cargo build --workspace --release             # 1. builds (release: the digests run it)
cargo test --workspace                        # 2. all pass, 0 fail (667 @ 70952c0)
cargo clippy --all-targets --workspace        # 3. no `error` lines
cargo test -p arb-trader --test determinism   # 4. (synthetic-tape digest fold)
```

Stage 3 fails on `error` lines, and reports the warning count without failing on
it. The workspace lint table is deny-level, so the warnings it would fail on
mostly arrive as errors anyway — but "no NEW warnings", which this document used
to claim, is not what the stage enforces, and the count has been 0 throughout the
campaign.

`gate.sh` exits 0 only for a clean pass, 1 for any failure (including a stage
that did not run), 2 for bad usage, and **3 when the run passed only because an
escape hatch was taken**. A hatched run's result line says `HATCHED`, never
`PASS`, so neither `[ $? -eq 0 ]` nor `grep 'GATE RESULT: PASS'` is satisfied by
one — the two things anyone automating this will reach for. Both halves are
load-bearing, and the token half is the easy one to get wrong: a word merely
*containing* `PASS` (`PASS-HATCHED`, say) still matches that grep as a prefix,
which would leave the trap open while looking like it had been closed.

```bash
# 5. THE REAL-TAPE DIGEST DIFF — from the repo root, release build.
cargo build --release -p arb-trader --manifest-path rust/Cargo.toml
./rust/target/release/arb-trader \
    --bench-tape data/golden/bench-tape-2026-07-28.jsonl \
    --registry   data/golden/registry-pin.yaml \
    --tradable   data/golden/tradable-pin.yaml \
    --kill-file  /nonexistent/KILL 2>/dev/null | tail -1
```

(`--kill-file` is not optional — see the note under gate 6 for why running this
from the primary checkout otherwise reads the live halt switch.)

The `sha256` field of that summary line MUST equal the baseline:

| field | baseline | where it came from |
|---|---|---|
| `sha256` | `214d4e94ebc19a4a33fe3af218016d9300313ab46246d7def66d3201a755722d` | re-pinned 2026-08-23 for #82's adaptive clip (was `b84e6079…`, re-pinned 2026-08-14 by `touch_excl_self`; before that `f4141b53…`) |
| `intents` | 31703 | same |
| `would_place` | 187 | same |
| `would_cancel` | 136 | same |
| `book_events` | 675950 | property of the tape; identical in both gates |
| wall time | ~4 s | 2026-07-29, production box, `nice -n 19` |

The one decision that moved: `KXFRENCHPRES-27-FHOL:bid`, resting at 0.06, when
99 lots join that level at ts 1785243864.950475. Netting our own 5 out left 94
and read it as competition, so the quote repriced to 0.07 and gave up the FIFO
priority it already held; a quote that opened its level now holds through a
joiner behind it. Nothing else in the intent stream differs — and the direction
matters, because the first cut of this fix skipped our price unconditionally and
took the same tape to 1,212 places by walking `cpc-btc-*-140k:ask` off a
1,500,238-lot wall it had merely joined. `touch_excl_self` carries that story.

### These baselines have gone stale twice, the same way both times

A merged PR changed a decision and did not re-pin. #65 did it (fixed by #70),
and #82 did it again — its ADAPTIVE CLIP replaced a flat clip of 5 with one
sized to the hedge leg's depth, which is precisely a decision change, and both
stages moved with it. Until 2026-08-23 nobody re-pinned, so every gate run on
main in between was red for somebody else's change. The failure mode is not
subtle and it is not the author's carelessness alone: **a red gate that is red
for a reason unrelated to your diff trains everyone to read past it**, which is
the only way this gate can actually fail.

If you are staring at a changed digest, before touching anything below: build a
DETACHED worktree at main and replay there. `git worktree add --detach <dir>
<sha>` (main itself cannot be checked out twice), and symlink `config/
registry.yaml` and `config/topics.yaml` in — they are gitignored, so a worktree
has neither and `arb-recorder`'s `live_registry_carries_the_france_pmus_legs`
fails without them for reasons that have nothing to do with you. If main
produces your digest, the baseline is stale and the fix is a bisect, not a
change to your PR.

`sha256` is a rolling hash of every intent line the engine emits, in order. It
is not a checksum of the summary: two runs that place the same orders at the
same prices in a different ORDER produce different digests, and so do two runs
that differ by one skip record. That is the point — it is the gate that fails
when a refactor changes what the engine decides rather than whether it crashes.

```bash
# 6. THE SAME TAPE WITH THE MAKER APR HURDLE ON. Same three pinned inputs, plus:
    --min-apr 12 --apr-asof 2026-07-28
```

| field | baseline | where it came from |
|---|---|---|
| `sha256` | `02c9a5829910430e77678d73517cc0a5f6e79567a2ac03a84364a1091faa8bea` | re-pinned 2026-08-23 for #82's adaptive clip (was `38978b34…`, re-pinned 2026-08-14 for #65; before that `2d330021…` from #22) |
| `intents` | 31487 | same |
| `would_place` | 16 | same |
| `would_cancel` | 7 | same |
| `book_events` | 675950 | property of the tape; identical in both gates |

Gate 5 runs with no `--min-apr`, and in bench there is no risk view for the bar
to float on, so `apply_apr` resolves it to `0.0` — at which point
`Quoter::set_apr` returns early leaving `apr_margin: None` and both
`if let Some(m) = self.apr_margin` guards in `quoter.rs` — the bid one and the
ask one — are never entered at all. The entire APR
path is unreached for the whole of gate 5, and a PR that mis-sizes the bar,
breaks `resolve::years_to` or deletes the hurdle comparison outright leaves gate
5 byte-identical. Gate 6 is the only stage that can see it. The cost of the
difference is worth reading: 171 of the 187 maker quotes gate 5 blesses are
locking capital under 12%/yr.

`--apr-asof` is pinned because the hold length is measured from TODAY, so an
unpinned as-of re-digests daily. Measured 2026-07-29, `--apr-asof 2026-07-28` and
`2026-07-29` produced the identical digest of the day with the same counts —
but **not because the margins are unchanged, and reading it that way is a trap.**

`resolve::years_to` is `days.max(1) as f64 / 365.25`, so one day moves `yrs` for
every dated family; with `m = a/(1+a)` that is `dm/dday ≈ 2-3.3e-4` — two to
three units at the 4dp `quantize_4dp` in `Quoter::set_apr`, not below it. Every
margin DIFFERS between the two dates. What absorbs it is the `quantize_cent` at
the end of both quote branches: the posted price is usually `comp + tick`, cent
aligned and independent of `m`, so a ~3e-4 shift only flips a decision when
`p_max` lands within 3e-4 of a cent boundary or of `comp`.

That is a property of where THIS tape's prices happen to sit, not of the resolve
dates and not of the hold lengths. So the pin is load-bearing, not
belt-and-braces, and moving it forward is a **re-measurement every time**, never a
rubber stamp.

The exact command line behind the stage-6 row:

```bash
cd /home/geoff/claude/arbbot
rust/target/release/arb-trader \
    --bench-tape data/golden/bench-tape-2026-07-28.jsonl \
    --registry   data/golden/registry-pin.yaml \
    --tradable   data/golden/tradable-pin.yaml \
    --kill-file  /nonexistent/KILL \
    --min-apr 12 --apr-asof 2026-07-28 2>/dev/null | tail -1
```

`--kill-file /nonexistent/KILL` is not decoration. `arb-trader`'s default is the
RELATIVE `data/KILL` — the live kill switch — and the gate `cd`s to the primary
checkout to run the replay, so without this flag the gate is reading the
operator's halt file. `kill_iv` is the one select arm carrying no condition at
all (`engine/mod.rs:1395`); every neighbour has one.

The honest scope of the hazard, measured 2026-07-29: with a kill file **present**
the digest did not move and `killed` stayed `false`. In bench `budget` is
`usize::MAX` and the select is `biased`, so the feed arm wins whenever the
channel is non-empty, and it runs saturated at `chan_high_water` 65536 — the kill
arm is starved for the entire replay — though `tape_feed` runs on its own OS
thread and tokio's first `interval` tick fires immediately, so there is a startup
window where the arm can be reached before the first push lands, regardless of
throughput. So this is a latent coupling rather than a
reproducible break today. It is an accident of this tape's throughput and not a
guarantee: a slower producer lets the channel drain, the arm fires, and
`cancel_all` pushes intents into the hashed stream. Pinning the path costs
nothing and removes the coupling; the engine's own fixtures do the same.

(The test above was run with a kill file in `/tmp`. Do not create `data/KILL` to
try this — that halts the armed trader.)

## What the digests do NOT prove

The digest stages replay a frozen tape through ONE binary, `arb-trader`, in a
mode that deliberately switches several subsystems off. Everything switched off
is invisible to them, and invisible does not look different from unchanged: a
stage that could not have failed still prints `UNCHANGED` in green next to the
ones that could. We have already merged PRs on the strength of that. Three code
areas are affected, and if your change is in one of them the honest reading of a
clean gate run is "I did not accidentally touch the decision path", not "my
change is correct". The last two entries below are not code areas: one is a
failure mode of the gate itself, the other of the fail-before evidence a PR
supplies in place of it. Every entry here is the same species — **a check that is
silent for two different reasons, and reads the same either way.**

**Take-take is never exercised.** `run_cfg` builds `take_take: (!bench &&
args.take_take).then(..)`, so under `--bench-tape` the config is `None`;
`Engine::take_take_scan` destructures three `Option`s and returns on its first
line. `taketake::detect` is not called once in a 900,000 event replay. The
`take_take_found: 0` in every bench summary is STRUCTURAL — it does not mean no
crossing qualified, it means nothing looked. Two more gates sit behind the
first, so this is not one flag away from working: `hedge_retry` is also
`(!bench).then(..)`, and `tt_bar` is read from `marks.json` at engine
construction only when a take-take config exists.
A PR that changes take-take must carry a test that fails before the fix and
passes after, with the crossing written out in the fixture. Nothing in a digest
line can corroborate it, and quoting `take_take_found` as though it were a
measurement is worse than quoting nothing.

**The recorder is not even linked.** `bins/arb-trader/Cargo.toml` depends on
`arb-core`, `arb-registry` and `arb-venue`, and nothing else. `bins/arb-recorder`
has no `src/lib.rs` and no `[lib]` target, so no crate in the workspace CAN
depend on it — not one line of recorder code is in the binary the digests run.
On top of that the bench tape is a frozen file the recorder wrote days ago;
editing the recorder today cannot change a file that already exists on disk.
So `DIGEST: UNCHANGED` on a recorder PR is a tautology. It is not weak evidence,
it is not-evidence: the stage was incapable of failing. If your diff touches
`bins/arb-recorder`, the gate block reads as six passes and is really four.
The evidence a recorder PR must supply is an in-crate test in the venue module
you changed, failing before the fix. The socket-free seams already exist and are
already used this way — `kalshi::on_ws_message`, `pmintl::parse_ws_frame`,
`pmus::parse_ws_message` — so there is no infrastructure to build first. Where
the defect is a rate or a proportion rather than a shape, measure it over
recorded tape and quote the number, the way the 0.93%-of-64,553-sweeps figure in
`kalshi.rs` is quoted.

**There is no risk view and there are no gateways.** `main` builds `let risk =
(!bench).then(|| build_risk_view(..))`, so `RunCfg.risk` is
`None` for the whole replay: no bankroll, no per-class cap, no topic budget and
no utilization is ever consulted, and `risk_allowed` / `risk_rejected` are 0 in
every bench summary by construction. Separately, `arm_venues` returns an empty
map in bench — "bench/replay mode can never touch a venue" is a hard
precondition — so all three executors spawn with `sink: None`. Intents still
flow through the executor loop, and that is exactly what `would_place` /
`would_cancel` count: they are DEQUEUE counters, not send counters. `exec_sent`,
`exec_failed` and `exec_recovered` are 0 in every bench run. No `OrderSink`
implementation runs, nothing is signed, no venue rejection is parsed, no retry
ladder turns, and no rate-budget refusal happens. A PR in the order path needs
`exec.rs`'s stub-sink tests or `arb-venue`'s, and the digest is silent about it.

A worked example, from the same day this was written. #37 ("the caps believe less
capital is committed than really is") rewrote 271 lines of `quoter.rs` and 384 of
`risk.rs` to fix capital reservations leaking — and moved **neither digest by a
single byte**. Every quoter change it made sits behind `if let Some(gate) =
&self.risk`, and `risk` is `None` in bench, so both stages ran the new code and
took the old path through it. That PR's gate block showed two green digests over
a change they were structurally incapable of seeing. Two green digests were the
correct output; they were not evidence.

#36 the same night is the other half of this blind spot: 1,558 lines across
`sink.rs` and the `arb-venue` gateways — the order path itself — and again both
pins byte-identical, because `arm_venues` returns empty in bench and every
executor gets `sink: None`. So both halves have a dated instance: #37 for the
risk view, #36 for the gateways. Neither was a small change, and neither was
invisible for being subtle. They were invisible by construction.

**A stage that cannot time out can stall the campaign in silence.** This is the
fourth way, and the worst of them. The three above produce output that means less
than it appears to; this one produces no output at all. On 2026-07-29
`cargo test --workspace` deadlocked in `arb-recorder`'s `core::tests` — 1.16 s of
user CPU across 1,089 s of wall time, five of six threads parked in
`futex_do_wait`, the tokio reactor idle in `ep_poll` — and the gate did not fail,
did not warn, and did not exit. It sat there. Nothing bounded any stage until
that afternoon, and it was found by accident, during an unrelated PR, rather than
by design; there is no reason to think it was the first time.

It is not rare. Three instances were observed on 2026-07-29 alone, two of them
within the same hour, and the two that survived to be inspected were still wedged
after 65 and 30 minutes with the identical signature — six threads, one named
`core::tests::a_`, the rest in `futex_do_wait`, ~1.3 s of CPU between them. Both
were bare `cargo test` runs with no timeout around them, which is what ad-hoc
verification outside this gate looks like. Bound your runs.

Every stage is now bounded, and **a timeout is a FAIL — never a skip, never a
retry.** A deadlock that passes on a second attempt is precisely the failure that
gets ignored into production, so the gate does not offer that mercy. The budgets:

| stage | budget | why |
|---|---|---|
| 0 fetch | 120 s | one `git fetch` of a single branch; the rev-list beside it is a local object walk and is left unbounded |
| 1 build | 900 s | observed 0.06 s warm, ~34 s cold full workspace release rebuild |
| 2 test | 900 s | observed ~2-4 min including debug test-target compilation |
| 3 clippy | 900 s | same compile profile as the test stage |
| 4 determinism | 600 s | one test, but it builds `arb-trader`'s test targets first |
| 5, 6 digest | 300 s each | observed ~4-5 s of replay per run (`elapsed_s` in the summary) |

These are 20-60x the times measured on the production box under four-way cargo
contention, which is deliberate: a timeout should mean "something is wrong", not
"the box was busy". If you hit one, establish that the stage was making progress
before you raise the number — CPU time against wall time is the cheap test, and
it is what identified the deadlock above.

**And the fail-before evidence itself can be silent for two different reasons.**
This one is about the evidence a PR supplies, not about the gate, but it is the
same family and it is why it sits here. An author's fail-before shim can trip a
deny-level lint; the test binary then never compiles; and a grep for failing
tests returns EMPTY — which reads identically to "no test flips". PR #36's author
hit exactly that and caught it. `scripts/gate.sh` is immune, because rustc's
`error[E…]` matches its `^error` pattern — but most fail-before evidence in this
campaign was produced by ad-hoc runs OUTSIDE the gate, where nothing is checking.
If your evidence is "I ran it before the fix and nothing failed", confirm the
binary compiled before you believe the empty result. See task #54.

A timeout is not the only way a stage can fail to run, and the others do not
announce themselves. **Every stage treats any non-zero exit as a failure**, not
just the timeout codes: 137 is the OOM killer, which is a live risk here because
`cargo test --workspace` linking release and debug test targets is the largest
RSS consumer on a box that also runs an armed trader at Nice=10; 127 is cargo
missing from PATH; 101 is a panic. All of them truncate the captured output, and
a truncated capture contains no `error:` line — so a stage that matched only on
text used to print `TEST: ok — 0 passed, 0 failed` under a green result line.
Zero tests executed is itself a failure now, for the same reason.

## Why stage 0 exists, and why it can fail

Gating a branch is not gating the merge. On 2026-07-29 #27 renamed
`Core::snapshot_lines()` and #32 added tests calling it; each gated green in
isolation, both merged, and `cargo test` on main stopped compiling. GitHub
squash-merges without rebuilding, so N individually green PRs compose into a
broken tree — and a clean `git diff --stat` overlap says nothing about it,
because it answers "will git conflict", not "will it build". Stage 0 fetches
`origin/main` and refuses to run if this branch is behind.

It fails, rather than proceeding, when the fetch or the rev-list fails. It used
to swallow both (`|| true`, `|| echo 0`) and then print `BASE: up to date with
origin/main` — an affirmative claim about a comparison that never happened. This
box had a four-minute DNS outage on 2026-07-29, so that is a real path and not a
hypothetical one. It now prints `BASE: UNVERIFIED` and exits 1.

## The coreutils on this box are uutils, not GNU

Both `/usr/bin/timeout` and `/bin/timeout` are uutils coreutils 0.2.2; there is
no GNU coreutils installed. `grep` is ugrep 7.5.0. A gate whose logic is built on
exit codes and text matching is coupled to those implementations, and they do not
all behave the way the man page you remember says. Measured 2026-07-29:

| command | result |
|---|---|
| `timeout 1 sleep 20` | rc=124 after 1s — bounds it |
| `timeout 1 bash -c 'trap "" TERM; sleep 20'` | rc=124 after **20s** — does not |
| `timeout -k 2 1 sleep 20` | rc=**125** after 1s |
| `timeout --kill-after=2 1 sleep 20` | rc=**125** after 1s |
| `timeout --kill-after=2 1 bash -c 'trap "" TERM; sleep 20'` | rc=137 after **20s** |

So `-k` in every spelling changes a normal timeout's status from 124 to 125,
**and still does not bound a child that ignores TERM**. Adding it would break
detection while delivering nothing. It is also unnecessary for the case that
motivated the timeouts: the deadlock is a process parked in `futex_do_wait`
accruing no CPU, and that is killed by TERM — a futex-deadlocked python bounded
at exactly 3s under plain `timeout 3`, and TERM alone reaped it cleanly. Rust
test binaries install no TERM handler either.

This is the same trap as the `tail -F -n0` note already in this repo's memory:
GNU semantics recalled from memory and applied to an implementation that does not
share them. Check behaviour on this box before relying on a flag.

### Why there is no recorder golden

The obvious repair for the second blind spot is to replay a fixed venue-frame
fixture through the recorder and digest the tape it emits, giving recorder PRs a
digest stage of their own. It was assessed on 2026-07-29 and NOT built, and the
reason is not the one you would guess. It is not a lib/bin restructure — the
three seams above take frames and need no socket and no signing key, and the
existing tests already drive them. Two other things block it:

There is no venue-frame corpus, anywhere. `data/raw/`, `data/raw-rs/` and the
bench tape are the recorder's OUTPUT — normalized `TapeEvent` lines. Nothing in
this tree has ever captured the raw Kalshi/PM WS frames that go IN, so the
fixture would have to be hand-authored, and a hash over hand-authored input
carries strictly less information than the named assertions in the tests we
already have: it tells you something changed, not what, and it can only contain
the shapes its author already thought of. The trader's digest is worth having
because 900,000 lines of real tape carry shapes nobody wrote down. A synthetic
recorder digest would inherit the name and none of that.

And `ts_local_ns` is stamped from the wall clock at eight emission sites across
`kalshi.rs`, `pmus.rs` and `pmintl.rs` (`arb_core::model::now_local_ns`), so the
emitted tape is not reproducible as bytes. Digesting it means either excluding
that field — the field the bench-tape merge in "Why these inputs" below sorts on
— or threading an injectable clock through all three venue modules.

So the prerequisite is a capture: teach the recorder to tee raw frames to a file
under a flag, run a shadow against the venues long enough to catch a snapshot
burst, a wire-seq gap and a REST heal, and freeze that as the fixture. That is a
separate change, it touches the recorder, and it has to run against live venues.
Until it exists, recorder PRs pay for themselves with tests, not digests.

What the digests DO prove, for every PR: the maker quoter's decisions on 675,950
real book events are byte-identical, in order, including skip records — with the
APR hurdle both off (gate 5) and on (gate 6). That is a large and genuinely
load-bearing claim. It is just not the claim a take-take, recorder or order-path
PR needs.

## Why these inputs

The fixture is 900,000 real tape events from 2026-07-28 — 300k lines each from
the Kalshi, Polymarket INTL and PM-US recorders, merged and stably sorted by
`ts_local_ns`. Real data, so it carries the shapes a synthetic tape does not:
crossed books, sequence gaps, feed-specific level spellings, markets that go
quiet mid-day.

`registry-pin.yaml` / `tradable-pin.yaml` are frozen copies of the live config.
They are pinned rather than read from `config/` because the live registry gets
edited — a digest compared against a moving registry proves nothing.

None of it is committed (`data/` is gitignored: it is market tape). Rebuild:

```bash
D=2026-07-28
for v in kalshi polymarket_us polymarket; do head -n 300000 data/raw/$v-$D.jsonl; done \
  | awk '{ if (match($0, /"ts_local_ns":-?[0-9]+/)) k=substr($0, RSTART+15, RLENGTH-15); else k=0;
           print k "\t" $0 }' \
  | sort -k1,1n -k2 -S 2G | cut -f2- > data/golden/bench-tape-$D.jsonl
cp config/registry.yaml data/golden/registry-pin.yaml
cp config/tradable.yaml data/golden/tradable-pin.yaml
```

## When a digest is ALLOWED to change

A mismatch on either digest FAILS the gate. It has to: people merge on the last
line of the output, and a stage that detects a changed decision and then reports
`PASS` is not a weak check, it is a false statement. Until 2026-07-29 both digest
stages printed `*** CHANGED ***` and let `### GATE RESULT: PASS` follow it.

A digest may change only when the PR's whole purpose is to change a decision.
Pinning the new value is then a REVIEWED decision and not a mechanical step:
argue for it in the PR body — which decision moved, why that is the intended one,
and why the delta is the size it is — and record it here with the reason. Both
pins are re-measured, not just the one that moved: a change that shifts gate 5
and leaves gate 6 alone is as much a finding as the reverse. A refactor PR that
changes a digest is a refactor PR with a bug in it — including "obviously
equivalent" ones. Both prior incidents that motivated this campaign (a
stringly-typed id swapped at a call site, an `unwrap_or` default on a money
field) would have been invisible to `cargo test` and loud here.

`scripts/gate.sh --allow-digest-change` is the escape hatch for that case: it
downgrades a mismatch to a warning and lets the gate pass with exit 3. It prints,
inside the mismatch block, that it was used, so the flag appears in any pasted
gate output and a reviewer can see the author took it. Do not reach for it to get
a build out.

It downgrades a **changed digest** and nothing else. A replay that did not run —
missing binary, panic, OOM kill, timeout, a summary line carrying no `sha256` —
is a hard failure the flag cannot reach, because a failure to execute is not a
decision change and must never be describable as one. Those paths used to land in
the same `*** CHANGED ***` branch the hatch downgrades, which made it a blanket
"pass anyway" for both digest stages failing to run at all.

`scripts/gate.sh --skip-digest` skips BOTH digest stages, says so in the body of
the output, and appends `(DIGESTS SKIPPED)` to the result line. A skipped run is
not a gate run and must not be pasted into a PR as one.

## Scope

Rust only. `src/arbbot/` and the Python systemd units are FROZEN as of
2026-07-28 — not modified, not deleted, not run. Several of them still produce
the marks/unwind/hedge inputs the armed Rust engine reads, so they stay live
until a Rust replacement exists.
