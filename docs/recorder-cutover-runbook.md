# M1 recorder cutover — runbook

The exact sequence for moving the armed engine's market-data feed from the
Python recorder to the Rust one, what to watch afterwards, and how to get back.

**This is a REPOINT, not a port.** Nothing is installed, ported or rewritten.
Both recorders already run; the only thing that changes is which socket the
consumers read. Proved by peer inode on 2026-07-29:

| socket | LISTEN | connected peers |
|---|---|---|
| `data/arbbot.sock` | Python recorder (pid of `arbbot-recorder.service`) | armed `arb-trader`, dry-run `arb-trader`, `arbbot-scanner` |
| `data/arbbot-rs.sock` | `arb-recorder` (`arbbot-recorder-rs.service`) | **none** |

The Rust recorder has been a write-only shadow for its whole life. Its tape is
well covered; **its consumption path had never been exercised by anything**
until the gate in this runbook attached to it.

---

## 0. What flips, and what deliberately does not

`docs/migration-plan.md` M1 is a **role swap, both recorders keep running** for
a week. That is what this runbook does.

FLIPS — the armed engine, `arbbot-trader-m3.service`:

* `--socket data/arbbot.sock` → `data/arbbot-rs.sock`
* `--health data/health.jsonl` → `data/health-rs.jsonl`

**Both flags, or neither.** `--health` is easy to miss and is not cosmetic: it
is the file the engine reads `stale` from, and `[engine] FEED STALE — quotes
pulled` is driven by it. Repointing only `--socket` leaves the engine judging
the freshness of a feed it is no longer consuming — it would pull quotes for a
Python outage it cannot see and stay quoting through a Rust one. The two files
are format-identical and carry the same three feed names (`kalshi-ws`,
`polymarket-ws`, `polymarket_us-ws`), verified 2026-07-29.

DOES NOT FLIP:

* **`arbbot-scanner.service`.** `arbbot.scan.daemon` takes only `--config`; its
  socket is `cfg.socket_path` from `config/recorder.yaml`. That same key is
  what the PYTHON recorder BINDS, so editing it to move the scanner would move
  the Python recorder's socket out from under everything else at the same
  time. There is no per-process override. The scanner therefore stays on the
  Python socket, which is fine and is what M1 already says — **and it is the
  reason the Python recorder cannot be stopped at M1.** It also cannot be
  changed without editing Python, which is frozen.
* `arbbot-trader-rs.service` (the wide 40-relationship dry-run). Leave it on the
  Python socket for the soak week: with the armed engine on Rust and the shadow
  on Python, any divergence shows up as a difference between two live processes
  instead of having to be reasoned about.
* `arbbot-dash.service`, `arbbot-marks.timer`, `arbbot-settle.timer`,
  `arbbot-hedge.timer`. None of them read the socket.

LATER, AND NOT PART OF M1 — the full authority swap. When the Python recorder
finally retires, the clean shape is to give `arb-recorder` the canonical paths
(`--socket data/arbbot.sock --health data/health.jsonl --data-dir data/raw`)
and change nothing else: the scanner, the config and every tape consumer follow
without edits. Do not do this at M1; it removes the rollback target.

---

## 1. Pre-flight

**Step zero, and it is not optional: the recorder that is RUNNING must be the
recorder that was tested.** On 2026-07-29 it was not — the live shadow
recorder, up since 01:53, was serving an image that does not contain the string
`DROPPED subscriber` anywhere in it, while the binary on disk (rebuilt 14:02)
does. PR #19's eviction notice, PR #27's register-exactly-once welcome, and the
book-eviction logging were all in the tree and none of them were in the
process. `cargo build` replaces the file without restarting the unit, and
`Restart=always` only fires on exit, so this can persist for days.

```bash
# the check, and the gate now does it for you:
ls -l /proc/$(pgrep -f 'arb-recorder --config')/exe   # must NOT say "(deleted)"
systemctl --user restart arbbot-recorder-rs.service   # if it does
```

A restart resets the shadow's soak clock. **The 7 green days have to be seven
green days of the image you intend to cut over to**, not of whatever was
running when the evidence was gathered.

```bash
cd ~/claude/arbbot
# 1. the gate must be GREEN. Not "green apart from"; green.
rust/target/release/arb-shadow-gate \
    --py-dir data/raw --rs-dir data/raw-rs --socket data/arbbot-rs.sock \
    --window-s 900 --live-s 120 --load 6
# last line must read: SHADOW GATE: PASS

# 2. seven consecutive green days of it (migration-plan.md M1)
grep -h 'SHADOW GATE' data/reports/shadow-gate-2026-*.txt | tail -10

# 3. nothing in flight. An unhedged leg across a feed change is the one thing
#    that turns a clean restart into a position problem.
journalctl --user -u arbbot-trader-m3.service -n 200 --no-pager | grep -i undischarged
systemctl --user list-timers arbbot-hedge.timer

# 4. the Rust recorder is healthy and has been up long enough to have books
systemctl --user status arbbot-recorder-rs.service --no-pager | head -5
journalctl --user -u arbbot-recorder-rs.service -n 5 --no-pager   # [hb] gaps= subscribers=
```

Do it during a quiet window. The engine holds no quotes while it is down.

---

## 2. The flip

The armed engine's arming lives in a drop-in that is deliberately **not** in the
repo: `~/.config/systemd/user/arbbot-trader-m3.service.d/arm.conf`. That file's
`ExecStart=` is the lever.

```bash
# 1. edit the drop-in: change the two flags, nothing else
#      --socket data/arbbot.sock      ->  --socket data/arbbot-rs.sock
#      --health data/health.jsonl     ->  --health data/health-rs.jsonl
$EDITOR ~/.config/systemd/user/arbbot-trader-m3.service.d/arm.conf
systemctl --user daemon-reload

# 2. stop, and WATCH IT PROVE THE BOOK CLEAN before it exits.
#    SIGTERM cancels every venue and polls the resting list; `stop` is the
#    right way (corrected 2026-07-28 — the old touch-data/KILL-first advice
#    was the worse path).
systemctl --user stop arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -n 20 --no-pager | grep -i 'clean at exit'

# 3. start it on the new feed
systemctl --user start arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -f
```

`Restart=no` is on purpose: if it dies, read why before it starts cancelling
and re-quoting.

---

## 3. What the banner must say

Every line below is from the 2026-07-29 14:49:57 start, on the Python feed.
After the flip, **only the `[feed]` line may differ.**

```
[gate] 143 relationships -> 16 quoting; 93 blocked (4 vetoed by registry verdict, ...)
[risk] bankroll ... balances [kalshi=... polymarket_us=...]
[risk] seeded 346 open contracts across 10 relationships from data/exec/trades.jsonl
[apr] maker hurdle 16.00%/yr = 4 + 12*util at util 1.000, holds measured from ...
arb-trader up: 16 quoters, 32 markets, mode=shadow
[feed] connected data/arbbot-rs.sock          <-- THE ONE LINE THAT CHANGES
[exec] ORDERS ARMED — this process can place real orders
[exec] PolymarketUs: book is clean
[exec] Kalshi: book is clean
[take-take] ARMED — bar ...%/yr (marks ..s old), cap 20ct/rel, clip 5
[engine] feed reconnected — quotes stay pulled until the welcome snapshot burst has landed
[engine] feeds healthy — quoting resumes
```

Check, in this order:

1. **`[feed] connected data/arbbot-rs.sock`.** If it says `arbbot.sock`, the
   drop-in edit did not take — `daemon-reload` was skipped, or the edit went
   into the tracked unit rather than the drop-in, which the drop-in overrides.
2. **`16 quoters, 32 markets`.** This comes from the registry and the tradable
   allowlist, not from the feed, so it must be **identical**. A different number
   means something other than the feed changed.
3. **`[gate] 143 relationships -> 16 quoting; 93 blocked`** — the census line.
   Same reasoning: registry-derived, must not move.
4. **Both `book is clean` lines.** Two venues, both of them. `PolymarketUs`
   first, then `Kalshi`. A missing one is a sweep that did not complete, and
   the engine is then quoting over a book it has not proven empty.
5. **`[engine] feeds healthy — quoting resumes`, within ~10s.** This is the
   line that says the new welcome burst landed and the new health file is
   being read. If it never comes, or `FEED STALE` repeats, `--health` is
   pointing at the wrong file — that is the failure this flip is most likely to
   produce.
6. **`[risk] seeded 346 open contracts`** — from `data/exec/trades.jsonl`,
   nothing to do with the feed. It must match what it said before the stop.

Then, for the first hour:

```bash
# the stats line every 60s: gaps and staleness are the feed's report card
journalctl --user -u arbbot-trader-m3.service -f | grep -E 'FEED STALE|feeds healthy|subscription ended|EOF'
# and from the other side — the ONLY place an eviction is announced
journalctl --user -u arbbot-recorder-rs.service -f | grep -E 'DROPPED subscriber|hb'
```

`[hb] gaps=N subscribers=1` on the recorder is the confirmation that it now has
a consumer. It read `subscribers=0` for the whole of its life before this.

---

## 4. Rollback

**Time to restore: one edit, `daemon-reload`, stop, start — under two minutes,
and the two minutes are almost entirely the venue sweeps at stop and start.**
The Python recorder never stopped, its socket never closed, and `data/raw` kept
being written throughout, so there is nothing to catch up and no hole to
backfill.

```bash
$EDITOR ~/.config/systemd/user/arbbot-trader-m3.service.d/arm.conf   # both flags back
systemctl --user daemon-reload
systemctl --user stop  arbbot-trader-m3.service    # proves the book clean on the way out
systemctl --user start arbbot-trader-m3.service
journalctl --user -u arbbot-trader-m3.service -n 30 --no-pager | grep -E 'feed. connected|book is clean|feeds healthy'
```

Roll back on any of:

* an eviction notice on the recorder's journal (`DROPPED subscriber #N`) — the
  engine has just had every resting quote pulled and is reconnecting;
* `FEED STALE` that does not clear within ~30s, or that does not correspond to
  a real venue outage in `data/health-rs.jsonl`;
* gap counters climbing in the 60s stats line where they were flat before;
* the recorder restarting for any reason (`Restart=always` means it will come
  back, but the engine's feed dies with it).

**Do not roll back by stopping the Rust recorder.** That closes the socket
under the armed engine. Move the engine first; the recorder can be dealt with
afterwards.

---

## 5. Known-open at the time of writing (2026-07-29)

* **The running shadow recorder predates PR #27 and PR #19.** See §1. Two
  consequences beyond the missing eviction notice: the welcome burst in the
  running image still enqueues before it registers (the LOSS window PR #27
  closed), and every piece of shadow evidence quoted for M1 — the tape volume
  comparison, the dedup rates, the subscriber proof in this document — was
  gathered against that image. **Nothing here is evidence about the binary that
  would be cut over to until the recorder has been restarted and the window
  re-run.**
* **A slow subscriber is shed SILENTLY on the running image.** Measured twice:
  a subscriber that connects and never reads is dropped after 70-155s with zero
  `DROPPED subscriber` lines in the recorder's journal. The cap itself works —
  draining that socket afterwards yielded **16,127,981 bytes** against
  `MAX_BUFFER = 16_000_000` — so this is the running image having no notice in
  it, not a broken eviction. It is still what the armed engine would hit today,
  and the only trace would be on the engine's side as
  `subscription ended (EOF)`, followed by every resting quote being pulled.
* **PM-US `trade.size` is not a number.** `pmus.rs:55` reads `quantity`
  straight through `dec_string`, and the venue sends it as
  `{"currency":"USD","value":"4.0000"}`, so the tape and the socket both carry
  `"size":"{\"currency\":\"USD\",\"value\":\"4.0000\"}"`. Measured on the live
  wire, not inferred. `price` on the line above digs into `.value` correctly;
  `size` does not. Compare `pmus.rs:54` and `:55`. This is why the gate is red;
  it is a cutover blocker for the tape, and it needs the `quantity` units
  question settled (USD notional, not contracts) before it is fixed.
* The Rust recorder dedups consecutive-identical snapshots, so any window-shaped
  view of its tape covers fewer markets than Python's — 66 fewer on PM-US over
  900s on 2026-07-29, every one of which was present elsewhere in the same day's
  Rust tape. Nothing is lost and the welcome burst carries all of them, but a
  backtest that slices the Rust tape by a short window will see a smaller
  universe than the same slice of the Python tape.
