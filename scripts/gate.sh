#!/usr/bin/env bash
# Central PR gate. Run from a worktree root (the dir containing rust/ and data/).
# Usage: gate.sh [--skip-digest] [--allow-digest-change]
#
# Exit codes: 0 clean pass — and ONLY a clean pass.
#             1 a stage failed (including "did not run", which is not a pass).
#             2 bad usage.
#             3 passed only because an escape hatch was taken. Its result line
#               says HATCHED, never PASS, so neither `[ $? -eq 0 ]` nor
#               `grep 'GATE RESULT: PASS'` is satisfied by a run whose digest
#               stages were skipped or downgraded. Both halves are load-bearing:
#               a token merely CONTAINING "PASS" would still match that grep.
#
# Stages 5 and 6 (real-tape digests) need data/golden/, which only exists in the
# primary checkout. Worktrees don't have it, so we always run the digests against
# the primary checkout's data/ using the worktree's freshly built binary.
#
# READ docs/quality-campaign.md BEFORE quoting a green run as evidence. The
# digest stages have three named blind spots — take-take, the recorder, and the
# risk/gateway path — and in those areas a PASS here means "you did not touch the
# decision path", not "your change is correct".
#
# The last line of this output is what people merge on, so it must never say PASS
# about a run that did not check, or that checked and disagreed. A mismatched
# digest FAILS; `--allow-digest-change` downgrades it and says so in the output;
# `--skip-digest` says so in the output. Neither escape hatch can be used
# silently. Every stage that can block is bounded by a timeout, and a timeout is
# a FAIL — see the budgets below.

set -uo pipefail
PRIMARY=/home/geoff/claude/arbbot
ROOT="$(git rev-parse --show-toplevel)"
BASELINE_SHA=f4141b53831220c962f552955c079203ca0c7e2a7a7bb75bced5efe5e5e4bdc8
# Re-pinned 2026-08-14 (authorized by Geoff). #65 dated the DECEMBER btc150k
# rung — a deliberate resolve-dating change, and resolve dates are the APR
# hold-length input stage 6 exists to exercise — and did not re-pin this.
# Verified stale rather than flaky before touching it: three binaries (the
# Jul-31 build the armed unit is running, an unmodified ac4df9b, and PR #69's)
# replay this tape to the SAME 38978b34 digest and the same 31485/15/6 counts,
# while stage 5 still matches its baseline byte-for-byte on all three.
APR_SHA=38978b34171e4ec8b52102d71282014f1c96d5640ad77739e0e5926a614d37c9
fail=0
skip_digest=0
allow_change=0
suffix=

# Per-stage timeouts. A stage that cannot time out is a stage that can stall the
# whole campaign in silence: on 2026-07-29 `cargo test --workspace` DEADLOCKED in
# arb-recorder's core::tests (1.16s of CPU across 1089s of wall time, 5 of 6
# threads in futex_do_wait) and the gate simply sat there. That is worse than the
# three documented blind spots — those at least print something misleading, this
# printed nothing at all — and it was found by accident, not by design.
#
# A timeout is a FAIL. Never a skip, never a retry: a deadlock that passes on the
# second attempt is exactly the failure that gets ignored into production.
#
# Budgets are ~20-60x the times observed on this box on 2026-07-29 under 4-way
# cargo contention, so a timeout means "something is wrong", not "the box was
# busy". Raise one only after establishing that the stage was making progress.
T_BUILD=900       # observed 0.06s warm, ~34s cold full workspace release rebuild
T_TEST=900        # observed ~2-4 min including debug test-target compilation
T_CLIPPY=900      # same compile profile as the test stage
T_DETERMINISM=600 # one test, but it builds arb-trader's test targets first
T_DIGEST=300      # observed ~4-5s of replay per run (`elapsed_s` in the summary)

# `timeout` here is uutils 0.2.2, NOT GNU — /usr/bin/timeout and /bin/timeout are
# both it, there is no GNU coreutils on this box, and its behaviour differs from
# the manual page you remember. Measured 2026-07-29:
#
#   timeout 1 sleep 20                          -> rc=124, 1s      (bounds it)
#   timeout 1 bash -c 'trap "" TERM; sleep 20'  -> rc=124, 20s     (does NOT)
#   timeout -k 2 1 sleep 20                     -> rc=125, 1s      (!)
#   timeout --kill-after=2 1 sleep 20           -> rc=125, 1s      (!)
#
# So `-k` in EVERY spelling changes a normal timeout's status from 124 to 125,
# and still does not bound a child that ignores TERM (measured 20s with -k 2).
# Adding it would break detection here while delivering nothing — GNU semantics
# cargo-culted onto an implementation that does not share them, which is the same
# trap as the `tail -F -n0` note in this repo.
#
# It is not needed for the case that motivated the feature. The deadlock is a
# process parked in futex_do_wait accruing no CPU, and that IS killed by TERM:
# a futex-deadlocked python bounded at exactly 3s under `timeout 3` (rc=124,
# wchan futex_do_wait, 0 CPU ticks), and TERM alone reaped it cleanly. A Rust
# test binary installs no TERM handler either.
#
# `is_timeout` covers 124 and the 125 this implementation produces, but NOT 137:
# without -k our timeout never emits 137, so a 137 here is the OOM killer, and
# calling that "TIMED OUT" would send the reader hunting a hang that never
# happened. It falls to the `-ne 0` arm, which names the signal instead.
#
# This only decides which MESSAGE is printed. Every stage FAILS on any non-zero
# status regardless, so a code nobody anticipated cannot become a pass.
is_timeout() { [ "$1" = 124 ] || [ "$1" = 125 ]; }

# Run a cargo stage bounded by `timeout`, in a process group of its own, and
# REAP whatever is left in that group afterwards. Output lands in $STAGE_OUT.
#
# Measured 2026-07-29 against a genuinely hung `cargo test` (arb-recorder's
# core::tests, registration delayed past the sentinel): `timeout` DOES signal the
# whole process group here, rc=124 at the bound with zero surviving
# `arb_recorder-*` binaries. So the group kill below is not what stops the
# orphaning today — it is what NOTICES if that ever stops being true, which is
# the same reason every other check in this file exists.
#
# The hole it DOES close is the one shaped like cargo: the direct child dies on
# TERM, a GRANDCHILD survives, and the survivor still holds the stdout it
# inherited. `timeout` returns 124 at the bound, but `out=$(...)` then blocks on
# the capture pipe until that survivor exits. Measured 2026-07-29 with a
# grandchild ignoring TERM for 40s:
#
#   out=$(timeout 3 victim)   -> rc=124, but 40s elapsed   (13x past its bound)
#   run_stage 3 victim        -> rc=124, 3s elapsed, leftover group reported+reaped
#
# The gate sitting there in silence is the exact failure the budgets above were
# added to remove, so a bound that a survivor can extend is not a bound. Two
# deadlocked test binaries accumulated in one session on 2026-07-29, one of them
# parked in futex_do_wait for 63 minutes at 0% CPU, and nothing reaped them.
#
# Hence both halves: a FILE instead of a pipe, so no survivor can hold the
# capture open, and a group kill after `wait` returns, so no survivor outlives
# the stage.
#
# `setsid -w`, and the `-w` is load-bearing. setsid FORKS when its caller is
# already a process group leader; the parent then exits 0 immediately and `wait`
# reports THAT, not the stage's status. Measured 2026-07-29 under `set -m`:
#
#   run_stage 2 sleep 10  -> rc=0   (want 124)
#   run_stage 5 false     -> rc=0   (want 1)
#
# which is precisely "the stage that cannot fail prints green" — the defect the
# commit immediately before this one was written to remove — reintroduced on a
# condition (job control) that this script does not currently meet but does not
# check either. `-w` waits for the child and returns its status, and is a no-op
# in the non-forking case, so it costs nothing and closes the conditional hole.
# The pgid is read back from `ps` and then CHECKED to be the child's, because a
# read-back that is never verified is not a guard.
#
# What it does NOT do is bound a DIRECT child that ignores TERM: `timeout` waits
# for that child, so `wait` here does too, and the reap only runs afterwards.
# Nothing short of a watchdog fixes that, and it is not worth one — cargo, rustc
# and libtest binaries all die on TERM, which is why the real stages are bounded.
# Measured against a genuinely hung `cargo test`: rc=124 at the bound, zero
# surviving `arb_recorder-*`.
#
# No `-k`. See the note above: every spelling of it turns a normal timeout's 124
# into 125 on this implementation, and still does not bound a TERM-ignoring
# child. The reaping below is what -k is usually reached for, and it works.
#
# The two digest stages deliberately keep their inline `timeout`: `arb-trader` is
# timeout's DIRECT child and spawns nothing, so there is no group to leak, and
# their `| tail -1` needs the pipeline this helper replaces with a file.
STAGE_OUT=
run_stage() {
  local secs="$1"; shift
  local f pid pgid rc
  f=$(mktemp)
  setsid -w timeout "$secs" "$@" > "$f" 2>&1 &
  pid=$!
  pgid=$(ps -o pgid= -p "$pid" 2>/dev/null | tr -d ' ')
  # Only trust a pgid that IS the child's. `ps` can lose the race and report the
  # SCRIPT's group, and `kill -9 -- -$pgid` would then SIGKILL gate.sh and
  # everything sharing its group. Empty simply skips the reap.
  [ "$pgid" = "$pid" ] || pgid=
  wait "$pid"; rc=$?
  STAGE_OUT=$(cat "$f")
  rm -f "$f"
  # A non-empty group after the stage has exited is a leak. Say so — silently
  # cleaning it up would hide the regression this exists to catch.
  if [ -n "$pgid" ] && kill -0 -- "-$pgid" 2>/dev/null; then
    echo "      NOTE: process group $pgid outlived the stage; killing it. \`timeout\`"
    echo "            no longer reaps the group, so a stage can leak a wedged binary."
    kill -9 -- "-$pgid" 2>/dev/null
  fi
  return $rc
}

for a in "$@"; do
  case "$a" in
    --skip-digest) skip_digest=1 ;;
    --allow-digest-change) allow_change=1 ;;
    # Loud rather than ignored: a mistyped --allow-digest-change that silently
    # armed nothing would fail the gate for a reason its author cannot see.
    *) echo "gate.sh: unknown argument '$a'" >&2; exit 2 ;;
  esac
done

echo "### gate: $ROOT @ $(git -C "$ROOT" rev-parse --short HEAD) ($(git -C "$ROOT" branch --show-current))"

# --- 0/6 STALENESS. Gating a branch is not gating the merge.
#
# 2026-07-29: #27 renamed Core::snapshot_lines(); #32 added tests calling it.
# Each branch gated GREEN in isolation, both merged, and `cargo test` on main
# stopped compiling. GitHub squash-merges without rebuilding, so N individually
# green PRs compose into a broken tree, and a clean `git diff --stat` overlap
# says nothing about it — it answers "will git conflict", not "will it build".
#
# Both the fetch and the rev-list used to swallow their own failure — `|| true`
# and `|| echo 0` — and then print "BASE: up to date with origin/main", an
# affirmative claim about a comparison that never happened. A four-minute DNS
# outage on this box on 2026-07-29 makes a failed fetch concrete, not theoretical.
# A check that cannot report its own absence is the defect this whole file is
# about, so it says UNVERIFIED and fails rather than asserting something it does
# not know.
timeout 120 git -C "$ROOT" fetch -q origin main 2>/dev/null
frc=$?
if [ $frc -ne 0 ]; then
  echo "BASE: UNVERIFIED (git fetch exited $frc) — the staleness check did NOT run."
  echo "      This branch may be behind origin/main; nothing here can tell you."
  echo "### GATE RESULT: FAIL (staleness unverified)"
  exit 1
fi
behind=$(git -C "$ROOT" rev-list --count HEAD..origin/main 2>/dev/null)
if [ -z "$behind" ]; then
  echo "BASE: UNVERIFIED (git rev-list failed) — the staleness check did NOT run."
  echo "### GATE RESULT: FAIL (staleness unverified)"
  exit 1
fi
if [ "$behind" != "0" ]; then
  echo "STALE: this branch is $behind commit(s) behind origin/main."
  echo "       Rebase and re-run — a green gate on a stale base proves nothing"
  echo "       about the tree that will actually land."
  echo "### GATE RESULT: FAIL (stale base)"
  exit 1
fi
echo "BASE: up to date with origin/main"

echo "--- 1/6 build (workspace, release) ---"
run_stage $T_BUILD nice -n 19 cargo build --workspace --release --manifest-path "$ROOT/rust/Cargo.toml"
brc=$?; bout=$STAGE_OUT
echo "$bout" | tail -3
if is_timeout $brc; then
  echo "BUILD: FAIL — TIMED OUT after ${T_BUILD}s (suspect a hang, not slowness)"; fail=1
elif [ $brc -ne 0 ]; then
  echo "BUILD: FAIL — cargo exited $brc"; fail=1
else
  echo "BUILD: ok"
fi

echo "--- 2/6 test (workspace) ---"
run_stage $T_TEST nice -n 19 cargo test --workspace --manifest-path "$ROOT/rust/Cargo.toml"
trc=$?; tout=$STAGE_OUT
if is_timeout $trc; then
  echo "TEST: FAIL — TIMED OUT after ${T_TEST}s (suspect deadlock, not slowness)"
  echo "      2026-07-29: arb-recorder core::tests hung here with 1.16s of CPU"
  echo "      across 1089s of wall time, 5 of 6 threads in futex_do_wait. This is"
  echo "      NOT retried on purpose — a deadlock that passes on the second"
  echo "      attempt is the failure that gets ignored into production."
  fail=1
elif [ $trc -ne 0 ]; then
  # EVERY other abnormal exit, not just the ones we thought of. 137 is the OOM
  # killer, which on this box is a live risk: cargo test linking release+debug
  # test targets is the largest RSS consumer here, alongside an armed trader at
  # Nice=10. 127 is cargo missing from PATH. Both truncate the capture with no
  # `error:` line in it, so the grep below matches nothing and the count renders
  # 0 — which is how `TEST: ok — 0 passed, 0 failed` used to print under a
  # green result line.
  echo "TEST: FAIL — cargo exited $trc$([ $trc -eq 137 ] && echo ' (SIGKILL — the OOM killer, unless someone added timeout -k)')"
  echo "$tout" | tail -5
  fail=1
elif echo "$tout" | grep -qE '^(error|test result: FAILED)|panicked'; then
  echo "TEST: FAIL"; echo "$tout" | grep -E 'FAILED|^error|panicked|^test .* FAILED' | head -20; fail=1
else
  n=$(echo "$tout" | grep -oE '^test result: ok\. [0-9]+ passed' | awk '{s+=$4} END {print s}')
  nf=$(echo "$tout" | grep -oE '[0-9]+ failed' | awk '{s+=$1} END {print s}')
  # Zero tests is a failure in its own right, not a vacuous pass. A run that
  # compiled nothing and executed nothing has proved nothing.
  if [ "${n:-0}" = "0" ]; then
    echo "TEST: FAIL — cargo exited 0 but ZERO tests ran. The suite did not"
    echo "      execute, so this stage proved nothing."
    fail=1
  else
    echo "TEST: ok — ${n:-0} passed, ${nf:-0} failed"
    [ "${nf:-0}" != "0" ] && fail=1
  fi
fi

echo "--- 3/6 clippy (all targets, workspace) ---"
run_stage $T_CLIPPY nice -n 19 cargo clippy --all-targets --workspace --manifest-path "$ROOT/rust/Cargo.toml"
crc=$?; cout=$STAGE_OUT
nw=$(echo "$cout" | grep -cE '^(warning|error)')
if is_timeout $crc; then
  echo "CLIPPY: FAIL — TIMED OUT after ${T_CLIPPY}s (suspect a hang, not slowness)"; fail=1
elif [ $crc -ne 0 ]; then
  # Same rule as the test stage: any abnormal exit is a FAIL, because the
  # `^error` grep below cannot see a defect in output that was never produced.
  echo "CLIPPY: FAIL — cargo exited $crc$([ $crc -eq 137 ] && echo ' (SIGKILL — the OOM killer, unless someone added timeout -k)')"
  echo "$cout" | tail -5
  fail=1
elif echo "$cout" | grep -qE '^error'; then
  echo "CLIPPY: FAIL"; echo "$cout" | grep -E '^error' | head -10; fail=1
else
  echo "CLIPPY: ok — $nw warning/error lines"
  [ "$nw" != "0" ] && echo "$cout" | grep -E '^warning' | head -10
fi

echo "--- 4/6 determinism (synthetic tape) ---"
run_stage $T_DETERMINISM nice -n 19 cargo test -p arb-trader --test determinism --manifest-path "$ROOT/rust/Cargo.toml"
drc=$?; dout=$STAGE_OUT
if is_timeout $drc; then
  echo "DETERMINISM: FAIL — TIMED OUT after ${T_DETERMINISM}s (suspect a hang)"; fail=1
elif [ $drc -ne 0 ]; then
  echo "DETERMINISM: FAIL — cargo exited $drc"; fail=1
elif ! echo "$dout" | grep -qE 'test result: ok'; then
  echo "DETERMINISM: FAIL"; fail=1
else
  # Same rule as stage 2, for the same reason. `test result: ok. 0 passed;
  # 0 failed; 10 ignored` matches the line above and exits 0, so `#[ignore]`-ing
  # a flaky determinism test — the normal reaction to one — would silently
  # delete the whole synthetic-tape fold from the gate. Stage 2 would not catch
  # it either: it fails only on a TOTAL of zero, and 667 -> 657 still passes.
  dn=$(echo "$dout" | grep -oE '^test result: ok\. [0-9]+ passed' | awk '{s+=$4} END {print s}')
  if [ "${dn:-0}" = "0" ]; then
    echo "DETERMINISM: FAIL — reported ok but ZERO tests ran (all #[ignore]d?)."
    echo "      The synthetic-tape fold did not execute, so this stage proved"
    echo "      nothing."
    fail=1
  else
    echo "DETERMINISM: ok — ${dn} passed"
  fi
fi

# One replay of the pinned tape, compared against a pinned digest.
# $1 label, $2 expected sha, $3 expected intents/place/cancel (for the diff
# message only), $4.. extra arb-trader args.
digest_stage() {
  local label="$1" want="$2" counts="$3"; shift 3
  local line rc got ints plc cnl
  # `--kill-file /nonexistent/KILL`, because we `cd "$PRIMARY"` and arb-trader's
  # default is the RELATIVE "data/KILL" — the LIVE kill switch. `kill_iv` is the
  # one select arm carrying no condition at all (engine/mod.rs:1395 — 1389 is the
  # feed arm), so a replay started from the primary checkout is reading the
  # operator's halt file.
  #
  # Measured 2026-07-29, and the honest version is narrower than "this breaks the
  # gate": with a kill file PRESENT the digest did not move and `killed` stayed
  # false. In bench `budget` is usize::MAX and the select is `biased`, so the feed
  # arm wins whenever the channel is non-empty — and it ran saturated at
  # chan_high_water 65536, starving the kill arm for the whole replay.
  #
  # That is an accident of this tape's throughput, not a guarantee. A slower
  # producer (cold cache, contended disk) lets the channel drain, the arm fires,
  # and `cancel_all` puts intents into the hashed stream — at which point an
  # operator halting live trading turns the gate red and the message blames the
  # author's diff. There is also a startup window: `tape_feed` runs on its own OS
  # thread, and tokio's first `interval` tick fires immediately, so the arm can be
  # reached before the first push lands regardless of throughput. Pinning the path
  # costs nothing and removes the coupling entirely; the engine's own fixtures do
  # the same.
  #
  # 2>/dev/null keeps stderr out of the digest, but it also discards the reason a
  # run died — so `rc` below is the only thing that can tell us it did.
  line=$(cd "$PRIMARY" && timeout $T_DIGEST nice -n 19 "$ROOT/rust/target/release/arb-trader" \
      --bench-tape data/golden/bench-tape-2026-07-28.jsonl \
      --registry   data/golden/registry-pin.yaml \
      --tradable   data/golden/tradable-pin.yaml \
      --kill-file  /nonexistent/KILL "$@" 2>/dev/null | tail -1)
  rc=$?
  # Before any comparison: a replay that did not RUN has no digest, and reporting
  # its empty output as "*** CHANGED ***" names the wrong defect — and worse, puts
  # it on the one path `--allow-digest-change` is allowed to downgrade. A failure
  # to execute is never a decision change, so it is a hard FAIL that the escape
  # hatch cannot reach. 127 is a missing binary, 101 a panic, 137 the OOM killer,
  # and a failed `cd "$PRIMARY"` lands here too.
  if [ $rc -ne 0 ]; then
    if is_timeout $rc; then
      echo "$label: FAIL — TIMED OUT after ${T_DIGEST}s (rc=$rc). The replay never"
      echo "        finished, so this stage proved NOTHING about the decision path."
    else
      echo "$label: FAIL — the replay did not run (rc=$rc). This stage proved"
      echo "        NOTHING; it is not a digest change and --allow-digest-change"
      echo "        does not apply. Re-run without 2>/dev/null to see why."
    fi
    fail=1
    return
  fi
  got=$(echo "$line" | grep -oE '"sha256":"[0-9a-f]+"' | cut -d'"' -f4)
  # A completed replay always emits 64 hex chars. Empty means the summary line was
  # not what we think it is — a silent output-format change, not a decision change
  # — so it must not be comparable, and must not be downgradable either.
  if [ ${#got} -ne 64 ]; then
    echo "$label: FAIL — no sha256 in the summary line (got ${#got} chars)."
    echo "        The replay exited 0 but did not report a digest, so the output"
    echo "        format changed under us. Not a decision change; the escape"
    echo "        hatch does not apply."
    fail=1
    return
  fi
  ints=$(echo "$line" | grep -oE '"intents":[0-9]+' | cut -d: -f2)
  plc=$(echo "$line" | grep -oE '"would_place":[0-9]+' | cut -d: -f2)
  cnl=$(echo "$line" | grep -oE '"would_cancel":[0-9]+' | cut -d: -f2)
  if [ "$got" = "$want" ]; then
    echo "$label: UNCHANGED ($got) intents=$ints place=$plc cancel=$cnl"
    return
  fi
  echo "$label: *** CHANGED *** got=$got"
  echo "        baseline=$want"
  echo "        intents=$ints place=$plc cancel=$cnl (baseline $counts)"
  echo "        A changed digest is only acceptable when this PR's PURPOSE is to"
  echo "        change a decision, and pinning the new value is then a REVIEWED"
  echo "        decision, not a mechanical one: argue for it in the PR body and"
  echo "        record it in docs/quality-campaign.md with the reason."
  if [ "$allow_change" = 1 ]; then
    echo "        !!! --allow-digest-change WAS USED. This mismatch is downgraded to"
    echo "        !!! a warning and the gate is allowed to pass. The author took the"
    echo "        !!! escape hatch; a reviewer must confirm the new value is intended."
    suffix=" (DIGEST CHANGED — --allow-digest-change)"
  else
    fail=1
  fi
}

if [ $brc -ne 0 ]; then
  # The digests invoke the release binary unconditionally. If stage 1 did not
  # produce one, replaying whatever is on disk reports the digest of the LAST
  # good build — "DIGEST: UNCHANGED" sitting under "BUILD: FAIL". The overall
  # result is FAIL either way, but printing a green digest for a binary this run
  # did not build is the same misleading green this file exists to remove.
  echo "--- 5/6, 6/6 real-tape digests: NOT RUN (the build failed) ---"
  echo "NOT RUN: stage 1 produced no binary, so any digest here would describe an"
  echo "         EARLIER build, not this tree."
  suffix=" (DIGESTS NOT RUN — build failed)"
elif [ "$skip_digest" = 1 ]; then
  echo "--- 5/6, 6/6 real-tape digests: NOT RUN (--skip-digest) ---"
  echo "SKIPPED: NEITHER digest ran. The only two stages that can catch a changed"
  echo "         decision were not executed, so this is not a full gate run and"
  echo "         must not be pasted into a PR as one."
  [ "$allow_change" = 1 ] && echo "         (--allow-digest-change is inert here: no digest ran to downgrade.)"
  suffix=" (DIGESTS SKIPPED)"
else
echo "--- 5/6 real-tape digest ---"
  digest_stage DIGEST "$BASELINE_SHA" "31692 / 182 / 131"

# --- 6/6 The SAME tape with the maker APR hurdle turned on.
#
# Stage 5 runs with no --min-apr, and `apply_apr` resolves the bar to 0.0 in
# bench (no risk view to float on). `Quoter::set_apr` returns early on a
# non-positive bar leaving `apr_margin: None`, so both `if let Some(m) =
# self.apr_margin` guards in quoter.rs are never ENTERED for the whole of
# stage 5. A PR that breaks `resolve::years_to`, mis-sizes the bar, or deletes
# the hurdle comparison outright leaves stage 5 byte-identical.
#
# This run pins the hurdle explicitly (an explicit --min-apr wins over the risk
# view, which is how bench pins it at all) with a fixed as-of date, because the
# hold length is measured from TODAY and an unpinned asof re-digests daily. It
# cuts 31692 intents to 31485 and 182 places to 15 — i.e. 167 of the 182 maker
# quotes stage 5 blesses are locking capital under 12%/yr, and only this stage
# can see that. (152 of 182 before #65 dated the btc150k rung: dating a rung
# changes its hold length, its APR, and therefore which quotes clear the bar.)
echo "--- 6/6 real-tape digest, APR hurdle on ---"
  digest_stage "APR-DIGEST" "$APR_SHA" "31485 / 15 / 6" --min-apr 12 --apr-asof 2026-07-28
fi

# A hatched run says HATCHED, not PASS, and exits 3. Both halves matter, and the
# TOKEN is the half that is easy to get wrong: `grep 'GATE RESULT: PASS'` is the
# obvious thing to automate, and it matches "PASS-HATCHED" as a prefix — so a
# word containing PASS would have left the trap open while looking like it closed
# it. HATCHED shares no substring with PASS.
#
# A clean pass is the only 0 this script emits, and the only line carrying PASS.
if [ $fail -ne 0 ]; then
  echo "### GATE RESULT: FAIL$suffix"
  exit 1
fi
if [ -n "$suffix" ]; then
  echo "### GATE RESULT: HATCHED$suffix"
  echo "         An escape hatch was taken, so this is NOT a clean pass: one or"
  echo "         both digest stages did not run or did not match. Exit code 3."
  exit 3
fi
echo "### GATE RESULT: PASS"
exit 0
