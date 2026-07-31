# M1 recorder cutover — runbook

The exact sequence for making the Rust recorder authoritative, retiring the
Python one, what to watch afterwards, and how to get back.

**Scope changed on Geoff's call 2026-07-31.** This document used to describe a
two-flag REPOINT of the armed engine with both recorders left running for a
soak week. It now describes the **full authority swap**: `arb-recorder` takes
the canonical paths and `arbbot-recorder.service` stops. The soak week went
with the seven-green-days clause (see §1) — what replaces it is one green gate
run against the promotion image and a rollback measured in minutes.

**Still not a port.** Nothing is rewritten. Both recorders already run; what
changes is which binary owns `data/arbbot.sock`, `data/health.jsonl` and
`data/raw`. Peer inodes on 2026-07-29, before any of this:

| socket | LISTEN | connected peers |
|---|---|---|
| `data/arbbot.sock` | Python recorder (pid of `arbbot-recorder.service`) | armed `arb-trader`, dry-run `arb-trader`, `arbbot-scanner` |
| `data/arbbot-rs.sock` | `arb-recorder` (`arbbot-recorder-rs.service`) | **none** |

The Rust recorder has been a write-only shadow for its whole life. Its tape is
well covered; **its consumption path had never been exercised by anything**
until the gate in this runbook attached to it.

---

## 0. What flips, and what deliberately does not

**Nothing that reads the feed is edited.** That is the whole trick, and it is
why the earlier two-flag repoint has been abandoned in favour of this: every
consumer already names the canonical paths, so moving the *producer* onto them
carries all of them across at once, with no per-consumer edit and no config key
to argue about.

FLIPS — `arbbot-recorder.service` stops running Python and starts running
`arb-recorder`, on the paths it already advertises:

* `--data-dir data/raw` (was `data/raw-rs`)
* `--socket data/arbbot.sock` (was `data/arbbot-rs.sock`)
* `--health data/health.jsonl` (was `data/health-rs.jsonl`)
* `--shadow` comes OFF — it suppresses ntfy (`main.rs:166`), which is correct
  for a shadow and wrong for the production recorder.

The unit that flips is **`arbbot-recorder.service`**, not
`arbbot-recorder-rs.service`, and the choice is load-bearing: both trader units
carry `After=arbbot-recorder.service` / `Wants=arbbot-recorder.service`.
Promoting the `-rs` unit instead would leave the armed engine ordered against a
dead unit. `arbbot-recorder-rs.service` is stopped and disabled by the same
operation.

DOES NOT FLIP — and needs no edit, which is the point:

* **`arbbot-scanner.service`.** `arbbot.scan.daemon` takes only `--config`; its
  socket is `cfg.socket_path` from `config/recorder.yaml` — `data/arbbot.sock`.
  Under the old repoint plan that key was the blocker, because it is also what
  the Python recorder BINDS, so it could not be moved without moving the
  recorder out from under everything else. Swapping the producer instead makes
  that constraint evaporate: the key keeps its value, the scanner keeps its
  line, and it reconnects to Rust without a Python edit. It was the stated
  reason the Python recorder "cannot be stopped at M1"; it is not one any more.
* `arbbot-trader-m3.service` (armed) and `arbbot-trader-rs.service`. Both
  already say `--socket data/arbbot.sock --health data/health.jsonl`. They
  follow.
* Tape consumers. `data/raw` keeps its name and its files — `arb-tape`'s writer
  opens `create(true).append(true)` (`writer.rs:30`), as does the health writer
  (`health.rs:98`), so the day's Python tape is appended to, never truncated.
* `arbbot-dash.service`, `arbbot-marks.timer`, `arbbot-settle.timer`. None of
  them read the socket.

**The health-file feed sets must match before the swap, and for two weeks they
did not.** `record_polymarket_intl: false` (Geoff, 2026-07-31) was a
Python-only key — `src/arbbot/ops/config.py:40`, nothing in `rust/` — so Python
published two feeds and Rust three. Left alone that would have resumed INTL
recording the moment `--shadow` came off, restoring the ntfy storm the key was
set to stop (58 of 64 alerts in 6h). `arb-recorder` now honours the key, with
Python's absent-not-stale semantics. Verify, do not assume:

```bash
for f in data/health.jsonl data/health-rs.jsonl; do
    printf '%s ' "$f"
    tail -1 "$f" | python3 -c "import sys,json;print(sorted(json.load(sys.stdin)['stale']))"
done   # the two lists must be identical
```

---

## 1. Pre-flight

**Step zero, and it is not optional: the recorder that is RUNNING must be the
recorder that was tested.** On 2026-07-29 it was not — the live shadow
recorder, up since 01:53, was serving an image that does not contain the string
`DROPPED subscriber` anywhere in it, while the binary on disk (rebuilt 14:02)
does. The eviction notice, the register-exactly-once welcome and the
book-eviction logging all arrived together in PR #27 (`47e3c9b`) at 04:29 —
**two hours and thirty-six minutes after that process started.** So nothing was
broken; the fixes were simply not in the running image. `git log -S 'DROPPED
subscriber' -- rust/` returns that one commit and no other, which is the whole
proof. `cargo build` replaces the file without restarting the unit, and
`Restart=always` only fires on exit, so this can persist for days.

> Numbers in `(#N)` commit subjects in this repo are LOCAL TICKET numbers, not
> PR numbers, and the two do not line up: ticket #19 is the socket eviction
> while PR #19 (`f0f5593`) is a trader bankroll fix. `gh issue view N` will
> resolve to an unrelated PR because this repo has zero issues. Cite the SHA.

```bash
# the check, and the gate now does it for you:
ls -l /proc/$(pgrep -f 'arb-recorder --config')/exe   # must NOT say "(deleted)"
systemctl --user restart arbbot-recorder-rs.service   # if it does
```

A restart used to reset a soak clock. There is no soak clock any more — but the
weaker claim that replaces it is the same one that mattered: **the green run
has to be a green run of the image you intend to cut over to**, not of whatever
was running when the evidence was gathered.

```bash
cd ~/claude/arbbot
# 1. the gate must be GREEN. Not "green apart from"; green.
#    `nice -n 19` is NOT optional and is not what the binary does for you: the
#    unit sets Nice=19, a hand-run inherits your shell's priority, and `--load 6`
#    is six CPU burners on the box carrying the armed engine's feed — above even
#    the Rust recorder (NI 10). Doing this unniced degraded the live feed on
#    2026-07-29. The gate prints its own nice value and warns if you forget.
nice -n 19 rust/target/release/arb-shadow-gate \
    --py-dir data/raw --rs-dir data/raw-rs --socket data/arbbot-rs.sock \
    --window-s 900 --live-s 120 --load 6
# last line must read: SHADOW GATE: PASS

# 2. read the history anyway — one run is the GATE, not the whole picture.
#    `shopt -s nullglob` so that a run before any report exists prints nothing
#    instead of handing grep the literal string `data/reports/shadow-gate-*.txt`
#    — which reads as "no such file", not as "no gate has ever run".
shopt -s nullglob
for f in data/reports/shadow-gate-*.txt; do
    printf '%s ' "$f"; grep 'SHADOW GATE' "$f" | tail -1
done | tail -10
shopt -u nullglob
```

**The "7 consecutive green days" clause was removed on Geoff's call
2026-07-31**, and the loop above is what used to enforce it. Keep reading the
history: it is still the only place a run that was killed, or never fired at
all, is visible. What it must no longer do is gate the flip.

The clause is worth a paragraph rather than a silent deletion, because the way
it failed is a pattern this stack has hit twice. It was never satisfied and,
with the machinery that existed, never could be: the gate feeding it had been
an **uninstalled unit** — `systemctl --user list-unit-files 'arbbot-shadow*'`
returned nothing while `systemd/arbbot-shadow-gate.{service,timer}` sat in the
repo — writing a 153-byte `can't open file` into `data/reports/` daily and
exiting 0, from 2026-07-24 until it stopped running entirely on 07-26. The one
run that ever printed a verdict, 2026-07-23, said FAIL, on a parse bug
(`c292f04`) fixed six days ago. So the clause was not holding a bar; it was
holding a door, and nothing was ever going to open it. A week of green days is
only evidence if something is collecting them.

One green run against the promotion image, plus §4's rollback, is the trade
being made instead. It is a smaller amount of evidence and it is being taken
knowingly.

Each report may hold several runs (the unit appends), so the LAST `SHADOW GATE:`
line in a file is that file's verdict — which is what the loop prints. Four
outcomes, and they are deliberately distinguishable from one another:

| last line | meaning |
|---|---|
| `SHADOW GATE: PASS` | that day is green |
| `SHADOW GATE: FAIL` | the gate ran and decided against you; the reasons are listed just above it |
| `SHADOW GATE: NO VERDICT` | the run was KILLED before it decided — a start timeout, an OOM kill, a Ctrl-C, or a bad flag on a hand-run. **Not a green day and not a missing day.** See below before re-running |
| no file at all | the gate did not run that day |

`NO VERDICT` is not automatically "re-run it". Check `systemctl --user status
arbbot-shadow-gate.service` first: an OOM kill or a `TimeoutStartSec=600`
timeout means something took four times longer than a full run, and re-running
into the same wedge just burns another ten minutes. The one hang this stage was
built to find — a recorder that has stopped calling `accept()` — no longer
lands here: `connect()` is bounded and reports `SHADOW GATE: FAIL` naming the
socket, because a wedged recorder answering as "killed, re-run me" is an
instruction to loop.

```bash
# 3. nothing in flight. An unhedged leg across a feed change is the one thing
#    that turns a clean restart into a position problem. `hedges_undischarged`
#    in the 60s stats line is the number that counts; the log grep is a
#    cross-check, not the source.
journalctl --user -u arbbot-trader-m3.service -n 200 --no-pager | grep -i undischarged
#    arbbot-hedge.timer was RETIRED 2026-07-31 (PR #56 ef302ca); naked-leg
#    completion is --positions-recon-act inside the engine, so there is no
#    second owner to interlock with any more. Left as a check that it has not
#    been re-enabled, which would be a double hedge, not a backup.
systemctl --user is-enabled arbbot-hedge.timer    # must say: disabled

# 4. the Rust recorder is healthy and has been up long enough to have books
systemctl --user status arbbot-recorder-rs.service --no-pager | head -5
journalctl --user -u arbbot-recorder-rs.service -n 5 --no-pager   # [hb] gaps= subscribers=
```

`[hb] gaps=N` is the recorder's OWN ingest-side counter (`NotSynced` +
`GapDetected` at the venue boundary, cumulative since process start — it read
107 eleven minutes after a restart). **The gate does not read it**: it lives only
in the journal, not in `health-rs.jsonl`, and reading it would couple the gate to
a unit name. Judge it by hand and judge the SLOPE, not the value — it should be
flat between two heartbeats, and a climbing one is a venue feed resubscribing.

Do it during a quiet window. The engine holds no quotes while it is down.

---

## 2. The swap

**Order matters, and it is not the obvious one.** The armed engine comes down
FIRST and goes up LAST. Between those two points the socket disappears and
comes back owned by a different process — which the engine's reconnect loop
would survive, but surviving it means flapping quotes across a window where
neither recorder is authoritative. Taking it down cleanly instead costs one
verified venue sweep and makes the whole middle section unobserved-by-anything.

```bash
# 1. ARMED ENGINE DOWN, and watch it prove the book clean before it exits.
#    SIGTERM cancels every venue and polls the resting list; `stop` is the
#    right way (corrected 2026-07-28 — the old touch-data/KILL-first advice
#    was the worse path).
systemctl --user stop arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -n 20 --no-pager | grep -i 'clean at exit'

# 2. RUN THE GATE NOW, at full load. This is the window in which `--load 6` is
#    free: the six CPU burners exist to prove the recorder does not shed a
#    subscriber under contention, and with the engine down there is no live
#    feed for them to starve. Outside this window, drop to --load 0.
nice -n 19 rust/target/release/arb-shadow-gate \
    --py-dir data/raw --rs-dir data/raw-rs --socket data/arbbot-rs.sock \
    --window-s 900 --live-s 120 --load 6
# SHADOW GATE: PASS, or stop here.

# 3. PYTHON RECORDER DOWN, and disabled so a reboot cannot bring it back to
#    fight for the socket.
systemctl --user stop arbbot-recorder.service
systemctl --user disable arbbot-recorder.service
ss -x -p | grep arbbot.sock    # must be empty: the socket is now unowned

# 4. SHADOW DOWN. It is about to be replaced by the promoted unit, and two
#    arb-recorder processes on one PM-US key is the contention the shadow unit
#    header warns about.
systemctl --user stop arbbot-recorder-rs.service
systemctl --user disable arbbot-recorder-rs.service

# 5. PROMOTE. arbbot-recorder.service now runs arb-recorder on the canonical
#    paths, with --shadow removed so ntfy is live again.
cp systemd/arbbot-recorder.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now arbbot-recorder.service
journalctl --user -u arbbot-recorder.service -n 15 --no-pager

# 6. PROVE IT OWNS THE SOCKET before anything subscribes to it.
ss -xlp | grep arbbot.sock     # LISTEN, users:(("arb-recorder",...))

# 7. RECONNECT THE READ-ONLY CONSUMERS FIRST. They are the cheap canary: if
#    the socket is wrong, find out on the scanner, not on the armed engine.
systemctl --user restart arbbot-scanner.service arbbot-trader-rs.service
journalctl --user -u arbbot-recorder.service -n 5 --no-pager | grep subscribers

# 8. ARMED ENGINE UP, last.
systemctl --user start arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -f
```

`Restart=no` on the engine is on purpose: if it dies, read why before it starts
cancelling and re-quoting.

**Do not skip step 4.** `arbbot-recorder-rs.service` and the promoted
`arbbot-recorder.service` are the same binary reading the same PM-US key. The
`-rs` unit's own header is explicit that a second WS session on a shared key is
the contention to avoid; after the swap the `rs` credential suffix has nothing
left to isolate it from.

---

## 3. What the banner must say

Every line below is from the 2026-07-31 15:52 start, the first on the Rust
feed, and every census figure in it was byte-identical to the 13:55 start on
the Python feed — which is the actual check. (The numbers here were `16
quoters, 32 markets` / `seeded 346` until 2026-07-31: stale since the armed
drop-in widened `--rel-prefix` to `xvus-`, and quietly wrong for anyone
comparing against them.)
After the flip, **only the `[feed]` line may differ.**

```
[gate] 143 relationships -> 32 quoting; 93 not quoted (4 vetoed by registry verdict, ...)
[risk] bankroll ... balances [kalshi=... polymarket_us=...]
[risk] seeded 289 open contracts across 10 relationships from data/exec/trades.jsonl
[apr] maker hurdle 16.00%/yr = 4 + 12*util at util 1.000, holds measured from ...
arb-trader up: 32 quoters, 64 markets, mode=shadow
[feed] connected data/arbbot.sock             <-- UNCHANGED. That is the point.
[exec] ORDERS ARMED — this process can place real orders
[exec] PolymarketUs: book is clean
[exec] Kalshi: book is clean
[take-take] ARMED — bar ...%/yr (marks ..s old), cap 20ct/rel, clip 5
[engine] feed reconnected — quotes stay pulled until the welcome snapshot burst has landed
[engine] feeds healthy — quoting resumes
```

**Under the swap, NO line above should differ — including the `[feed]` one.**
The engine's flags never changed; the process on the other end of the socket
did. That makes the banner a weaker check than it was under the repoint, where
a wrong socket was visible in the log. The replacement check is §2 step 6:
prove `arb-recorder` owns `data/arbbot.sock` *before* the engine subscribes.
Read the banner for the things below, and read `ss -xlp` for the identity.

Check, in this order:

1. **`[feed] connected data/arbbot.sock`.** Same string as before the stop. If
   it is missing entirely, nothing is bound — go back to §2 step 6.
2. **`16 quoters, 32 markets`.** This comes from the registry and the tradable
   allowlist, not from the feed, so it must be **identical**. A different number
   means something other than the feed changed.
3. **`[gate] 143 relationships -> 16 quoting; 93 blocked`** — the census line.
   Same reasoning: registry-derived, must not move.
4. **Both `book is clean` lines.** Two venues, both of them. `PolymarketUs`
   first, then `Kalshi`. A missing one is a sweep that did not complete, and
   the engine is then quoting over a book it has not proven empty.
5. **`[engine] feeds healthy — quoting resumes`, within ~10s.** This is the
   line that says the welcome burst landed and `data/health.jsonl` is being
   written by its new owner. If it never comes, or `FEED STALE` repeats, the
   promoted recorder is not writing the health file — check that `--health`
   in the promoted unit says `data/health.jsonl` and not `data/health-rs.jsonl`.
   That is the failure this swap is most likely to produce, and unlike the old
   repoint it is invisible in the engine's own banner.
6. **`[risk] seeded 346 open contracts`** — from `data/exec/trades.jsonl`,
   nothing to do with the feed. It must match what it said before the stop.

Then, for the first hour:

```bash
# the stats line every 60s: gaps and staleness are the feed's report card
journalctl --user -u arbbot-trader-m3.service -f | grep -E 'FEED STALE|feeds healthy|subscription ended|EOF'
# and from the other side — the ONLY place an eviction is announced
journalctl --user -u arbbot-recorder.service -f | grep -E 'DROPPED subscriber|hb'
```

`[hb] gaps=N subscribers=3` on the recorder is the confirmation that it has its
consumers: armed trader, dry-run trader, scanner. It read `subscribers=0` for
the whole of its life as a shadow, so this number going from 0 to 3 is the
single clearest signal the swap took.

---

## 4. Rollback

**This is the part that got more expensive, and it is the price of the swap.**
Under the old repoint the Python recorder never stopped and rollback was two
flags. Now Python is stopped, so rollback is a restart of it — call it three to
four minutes, still almost all of it venue sweeps.

What has NOT changed is that nothing is destroyed. `git checkout` restores the
Python unit; the venv, the code and `config/recorder.yaml` are untouched; and
`data/raw` is append-only under both writers, so the tape has a seam at the
swap, not a hole. Nothing needs backfilling.

```bash
systemctl --user stop  arbbot-trader-m3.service    # proves the book clean on the way out
systemctl --user stop  arbbot-recorder.service     # release data/arbbot.sock

# put the Python unit back and start it
git -C ~/claude/arbbot checkout systemd/arbbot-recorder.service
cp ~/claude/arbbot/systemd/arbbot-recorder.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now arbbot-recorder.service
ss -xlp | grep arbbot.sock                        # users:(("python",...))

systemctl --user restart arbbot-scanner.service arbbot-trader-rs.service
systemctl --user start arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -n 30 --no-pager | grep -E 'feed. connected|book is clean|feeds healthy'
```

Roll back on any of:

* an eviction notice on the recorder's journal (`DROPPED subscriber #N`) — a
  subscriber has just had every resting quote pulled and is reconnecting;
* `FEED STALE` that does not clear within ~30s, or that does not correspond to
  a real venue outage in `data/health.jsonl`;
* gap counters climbing in the 60s stats line where they were flat before;
* the recorder restarting for any reason (`Restart=always` means it will come
  back, but every subscriber's feed dies with it).

**Rolling back is not free of its own risk:** `arbbot-recorder.service` is now
the name of a *Rust* unit, so a rollback that forgets the `git checkout` will
"restart the recorder" straight back into the thing being rolled back. Check
`ss -xlp` says `python`, not `arb-recorder`, before believing the rollback took.

**Do not roll back by stopping the Rust recorder.** That closes the socket
under the armed engine. Move the engine first; the recorder can be dealt with
afterwards.

---

## 5. The swap ran on a RED gate. Here is exactly why, and what it cost

2026-07-31 15:46 EDT, `data/reports/shadow-gate-2026-07-31.txt`, one red
reason and one only:

```
FAIL: coverage polymarket: 216 market(s) python published in the window are
      NOT in the rust recorder's welcome burst
```

**It is an artifact of turning INTL off, and it is not interpretable as
anything else.** The check asks whether Rust's LIVE welcome burst carries every
market Python published in the tape window. For `polymarket` (INTL — not
`polymarket_us`) it was comparing a live burst against a dead tape:

* `data/raw-rs/polymarket-2026-07-31.jsonl` mtime is **15:39:41**, which is
  `arb-recorder`'s restart timestamp to the second — not one byte written since
  `record_polymarket_intl: false` took effect.
* `data/raw/polymarket-2026-07-31.jsonl` mtime is **14:06:25**, so Python's INTL
  feed had already been silent for 49 minutes before its own 14:55 restart.
* `data/health-rs.jsonl` carries `kalshi-ws` and `polymarket_us-ws` and no
  third feed — INTL is absent, which is the designed result.

So the window the gate scored is 14:06-and-earlier on one side and 15:46 live on
the other. Zero INTL coverage is the CORRECT answer for a venue neither recorder
records.

**What was actually proven, on the two venues that have an order path:**

| | verdict |
|---|---|
| coverage kalshi | welcome has all 141 python markets |
| coverage polymarket_us | welcome has all 750 python markets |
| live subscriber, 120s under 6 burners | `live: ok`, 14,692 lines, gaps=0 unsynced=0 undecodable=0 bad_field=0 |
| load | 1.14 → peak 5.74, ceiling 40 |
| running image | `current` — §1 step zero, now checked by the binary |
| tape parse-compat | undecodable=0 bad_field=0 on all three venues |
| TOB agreement | polymarket_us 100.0%, polymarket 99.9% |

The 2026-07-23 FAIL (2,754 unparseable lines) is gone.

**This is the reflex the runbook warns about two sections up, and it is being
taken knowingly rather than accidentally.** The mitigating facts: the red venue
has no order path, is deliberately dark on both sides, and is excluded from the
engine's quote-pull decision by `DATA_ONLY_VENUES` (`feed_health.rs:40`)
regardless. The aggravating fact: a gate that says FAIL was overridden by
judgement, which is worth exactly as much as the judgement.

**Left open deliberately:** the gate cannot tell a venue that is OFF from a
venue that is BROKEN — both look like an empty welcome burst — and the fix is
for it to read `record_polymarket_intl` from the same config the recorder does,
and decline to score a venue the recorder was told not to record. Not built
here, because this gate compares a Python tape against a Rust one and the Python
tape stops existing at the end of this runbook. If a future shadow is ever cut,
build that first.

## 6. Known-open at the time of writing (2026-07-29)

* **DAY 1 CANNOT START UNTIL THE TRAILING SLICE IS CLEAR OF PRE-FIX TRADES,
  which is about 77 minutes after the recorder restart.** `bad_field > 0` is a
  gating check, PM-US `trade.size` was a stringified JSON object on every such
  line, and `c292f04` fixed it — the recorder carrying that fix was restarted
  2026-07-29 18:15 and its new trade lines read `"size":"8.0000"`. But
  `bad_field` is counted over the whole 256 MiB SLICE and not over the 900s
  window (deliberately: an undecodable line has no timestamp to place), so the
  gate stays red while that slice still reaches back past the restart. At
  PM-US's measured 58 KB/s that is ~77 minutes. **Check the first green run's
  slice actually starts after the restart before counting it as day 1.**

  Stated here rather than left to be discovered, because a gate that cannot go
  green is how the last one came to be ignored — three months of `RESULT:
  FAIL`, then three months of a 153-byte error nobody read. A run that is red
  for a reason everybody already knows about trains the same reflex.
* **The shadow recorder that gathered this evidence predated PR #27
  (`47e3c9b`); it has since been RESTARTED and the new process carries it.**
  See §1. Two consequences of the old image, both now historical: its welcome
  burst still enqueued before it registered (the LOSS window PR #27 closed), and
  every piece of shadow evidence quoted for M1 — the tape volume comparison, the
  universe census, the subscriber proof in this document — was gathered against it.
  **None of that is evidence about the binary that would be cut over to.** The
  soak clock starts at the restart, not at the date on this document, and the
  gate's `running_image_verdict` is what stops the same thing happening again.
* **A slow subscriber was shed SILENTLY on the pre-restart image.** Measured
  twice: a subscriber that connects and never reads was dropped after 70-155s
  with zero `DROPPED subscriber` lines in the recorder's journal. The cap itself
  worked — draining that socket afterwards yielded **16,127,981 bytes** against
  `MAX_BUFFER = 16_000_000` — so this was an image with no notice in it, not a
  broken eviction, and the restarted process has the notice. Re-verify it on the
  running image before the flip: the trace on the engine's side is
  `subscription ended (EOF)`, followed by every resting quote being pulled.
* **PM-US `trade.size` was not a number — FIXED in `c292f04`.** The trade
  parser dug into `.value` for `price` and not for `quantity`, so `dec_string`
  put the whole money wrapper into `TapeEvent::Trade.size`:
  `"size":"{\"currency\":\"USD\",\"value\":\"4.0000\"}"` on 11,541 of 11,541
  PM-US trade lines, every day the Rust recorder had run. The units question is
  settled and the answer is the opposite of what was written here: the value is
  **CONTRACTS, not USD notional**, despite the `currency` label — a print at
  0.2800 carrying `value 5.0000` took the resting bid from 240.0000 to
  235.0000, so the level is consumed by exactly the printed value.
  `tape.rs`'s `REAL_PMUS_TRADE` keeps a verbatim corrupt line as a test
  fixture: it is the only thing in this gate that can see that shape, since
  `size` is a `String` and both `serde_json` and `arb-recorder --parse-check`
  round-trip it happily.
* **Rust PM-US is WS-event-driven; Python polls.** So any window-shaped view of
  the Rust tape covers fewer markets than Python's — 66 fewer on PM-US over
  900s on 2026-07-29. Censused, not sampled: **all 66** appear in the same day's
  Rust tape, between 15 and 8,558 times each. Nothing is lost and the welcome
  burst carries all of them, but a backtest that slices the Rust tape by a short
  window will see a smaller universe than the same slice of the Python tape.
  (This was previously written up as the Rust recorder "deduping
  consecutive-identical snapshots". **There is no dedup in the recorder** —
  `Core::on_event` writes every event it is handed. The measured difference is
  19.4% consecutive-identical lines on Python PM-US against 2.2% on Rust, which
  is polling versus event-driven, and the practical consequence is unchanged.)
* **The welcome-coverage check cannot see a book that is tracked but stale.** It
  asks whether the recorder HAS a market, not whether it is still refreshing it.
  `[pm-ws] resnapshot sweep: 3/111 books did NOT refresh — their age is now
  unbounded`, on the live recorder eleven minutes after a restart: those three
  books are in the welcome burst and pass the check.
