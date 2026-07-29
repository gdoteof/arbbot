#!/usr/bin/env bash
# Offline test for the gauge half of scripts/freshness_check.sh.
#
# freshness_check.sh is the LIVE alarm path. A bug in it is worse than the gap it
# closes, because a false page trains the operator to swipe away the one
# notification that matters. So it gets exercised for real — the SAME file the
# timer runs, not a copy or a reimplementation — against captured stats lines.
#
# The fake is the ENVIRONMENT, never the code under test:
#   * a temp root whose scripts/ and config/ are SYMLINKS to this repo, so $0 is
#     the real script and `cd $(dirname $0)/..` lands in the temp root
#   * fresh data/ fixtures, so the recorder/scanner staleness checks are quiet
#     and every POST in a scenario is attributable to the gauge block
#   * stub systemctl / journalctl / curl on PATH
#   * ARBBOT_WATCHDOG_STATE_DIR pointed at the temp root — the live cooldown
#     stamps and the live gauge baseline are never touched
#
# Usage: scripts/test_freshness_gauges.sh [--with-exit-code-proof]
#
# --with-exit-code-proof additionally proves the OTHER half: that a non-zero
# exit from a Restart=no unit reaches a page at all. It creates and runs a
# TRANSIENT systemd user unit of its own. It touches no production unit and
# places no order. Off by default because a test suite should not create units.
set -uo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
PROOF=0
[ "${1:-}" = --with-exit-code-proof ] && PROOF=1

TMP=$(mktemp -d /tmp/arbbot-gaugetest.XXXXXX)
trap 'rm -rf "$TMP"' EXIT
ROOT=$TMP/root
mkdir -p "$ROOT/data/raw" "$ROOT/data/raw-rs" "$ROOT/data/scan" "$TMP/bin" "$TMP/creds"
ln -s "$REPO/scripts" "$ROOT/scripts"
ln -s "$REPO/config" "$ROOT/config"
DAY=$(date -u +%F)
touch "$ROOT/data/raw/polymarket-$DAY.jsonl" "$ROOT/data/health.jsonl" \
      "$ROOT/data/scan/opportunities-$DAY.jsonl" \
      "$ROOT/data/raw-rs/polymarket-$DAY.jsonl" "$ROOT/data/health-rs.jsonl"
echo not-a-real-topic > "$TMP/creds/ntfy_topic"

cat > "$TMP/bin/systemctl" <<'STUB'
#!/usr/bin/env bash
# --user is-active|is-failed --quiet UNIT | --user show UNIT -p PROP --value
u=${!#}; [ "$u" = --value ] && { for a in "$@"; do case $a in arbbot-*) u=$a;; esac; done; }
case "$2" in
  is-active) case " ${STUB_INACTIVE:-} " in *" $u "*) exit 3;; esac; exit 0 ;;
  is-failed) case " ${STUB_FAILED:-} " in *" $u "*) exit 0;; esac; exit 1 ;;
  show)
    p=""; n=$#
    for ((i=1;i<=n;i++)); do [ "${!i}" = -p ] && { j=$((i+1)); p=${!j}; }; done
    case "$p" in
      LoadState) echo "${STUB_LOADSTATE:-loaded}" ;;
      Result) echo exit-code ;;
      ActiveEnterTimestamp) echo "${STUB_ACTIVE_SINCE:-}" ;;
    esac ;;
esac
exit 0
STUB

cat > "$TMP/bin/journalctl" <<'STUB'
#!/usr/bin/env bash
cat "${STUB_JOURNAL:-/dev/null}" 2>/dev/null
exit 0
STUB

cat > "$TMP/bin/curl" <<'STUB'
#!/usr/bin/env bash
body=""; prio=""; want_fail=0
while [ $# -gt 0 ]; do
  case "$1" in
    -d) body=$2; shift 2 ;;
    -H) case "$2" in "Priority: "*) prio=${2#Priority: } ;; esac; shift 2 ;;
    --fail|-f) want_fail=1; shift ;;
    *)  shift ;;
  esac
done
printf 'PRIORITY=%s\n%s\n---\n' "$prio" "$body" >> "$STUB_POSTS"
# STUB_CURL_FAIL means "ntfy answered 4xx" — a revoked or mistyped topic. Real
# curl reports that as exit 22 ONLY when --fail was passed, and exits 0 without
# it. Modelling that exactly is the point: a stub that failed either way would
# let the tests pass against a script that dropped --fail.
[ -n "${STUB_CURL_FAIL:-}" ] && [ "$want_fail" = 1 ] && exit 22
exit 0
STUB
chmod +x "$TMP/bin"/*

export PATH=$TMP/bin:$PATH
export ARBBOT_WATCHDOG_STATE_DIR=$TMP
export ARBBOT_CREDENTIALS_DIR=$TMP/creds
export STUB_POSTS=$TMP/posts
unset CREDENTIALS_DIRECTORY 2>/dev/null || true

# One stats line, shaped exactly like the armed unit's. It deliberately carries
# the two SHAPES most likely to page falsely: a non-zero hedges_undischarged
# (a startup constant that never moves within a process) and kalshi_fill_gaps at
# its documented per-process floor. Every scenario below is tested against a
# line that already has both.
stats() {  # stats ELAPSED [key=val ...]
  python3 - "$@" <<'PY'
import json, sys
d = {"mode": "live", "elapsed_s": float(sys.argv[1]), "events": 1, "killed": False,
     "hedges_undischarged": 1, "kalshi_fill_gaps": 1, "hedges_pending": 0,
     "fills_unattributed": 0, "hedges_overfilled": 0, "dropped_unconsumed": 0,
     "hedges_naked": 0, "exec_dropped": 0, "exec_recovered": 0,
     "kalshi_fills_unreadable": 0, "exec_failed": 0,
     "kalshi_reconcile_failures": 0, "cancels_unresolved": 0}
for a in sys.argv[2:]:
    k, v = a.split("=", 1)
    if v == "DROP":
        d.pop(k, None)
    else:
        d[k] = float(v) if "." in v else int(v)
print(json.dumps(d, sort_keys=True))
PY
}

pass=0; failed=0
run() {
  : > "$STUB_POSTS"
  ( cd "$ROOT" && ./scripts/freshness_check.sh >"$TMP/stdout" 2>"$TMP/stderr" )
  echo $? > "$TMP/rc"
}
# Every assertion below is "no POST" or "this POST". BOTH are satisfied by a
# script that died on line 1, and "silent" is satisfied by a script that
# computed a perfectly good page and then dropped it because the ntfy topic did
# not resolve — which is a real outage this file has already had once, for seven
# days. So no verdict is trusted until the run that produced it is known to have
# completed and to have had somewhere to send to. Checked on EVERY assertion,
# not once at startup: an env change mid-suite would otherwise turn the rest of
# the file green and silent at the same time.
run_was_sane() {
  local rc; rc=$(cat "$TMP/rc" 2>/dev/null || echo missing)
  if [ "$rc" != 0 ]; then echo "the script exited $rc"; return 1; fi
  if grep -q UNRESOLVED "$TMP/stderr" 2>/dev/null; then
    echo "the ntfy topic did not resolve — every 'silent' below is meaningless"
    return 1
  fi
  return 0
}
fail() {  # fail NAME REASON [detail...]
  failed=$((failed+1)); echo "  FAIL $1"; shift
  for l in "$@"; do echo "       $l"; done
  [ -s "$TMP/stderr" ] && sed 's/^/       err: /' "$TMP/stderr"
  return 0   # else `cond && fail ... || ok ...` runs BOTH when stderr is empty
}
check() {  # check NAME EXPECT ; EXPECT is "silent" or a substring of the POST
  local name=$1 want=$2 got why
  why=$(run_was_sane) || { fail "$name" "$why"; return; }
  got=$(cat "$STUB_POSTS" 2>/dev/null)
  if [ "$want" = silent ]; then
    if [ -z "$got" ]; then pass=$((pass+1)); echo "  ok   $name"; return; fi
  elif printf '%s' "$got" | grep -qF -- "$want"; then
    pass=$((pass+1)); echo "  ok   $name"; return
  fi
  fail "$name" "wanted: $want" "got:    ${got:-<no post>}"
}
check_absent() {  # the run happened, but its POST must NOT contain this
  local name=$1 unwanted=$2 got why
  why=$(run_was_sane) || { fail "$name" "$why"; return; }
  got=$(cat "$STUB_POSTS" 2>/dev/null)
  if printf '%s' "$got" | grep -qF -- "$unwanted"; then
    fail "$name" "must NOT contain: $unwanted"
  else
    pass=$((pass+1)); echo "  ok   $name"
  fi
}
ok() { pass=$((pass+1)); echo "  ok   $1"; }
cooldown_clear() {
  rm -f "$TMP/arbbot-freshness-alerted" "$TMP/arbbot-freshness-routine" \
        "$TMP/arbbot-freshness-armed"
}
fresh() {
  rm -f "$TMP/arbbot-gauge-last" "$TMP/arbbot-gauge-last.ok" "$TMP/arbbot-gauge-last.held"
  cooldown_clear
}

export STUB_JOURNAL=$TMP/journal
export STUB_ACTIVE_SINCE="$(date -d '-2 hours' '+%a %Y-%m-%d %H:%M:%S %Z')"

echo "=== gauge verdicts ==="

# 1. Cold start seeds the baseline and says nothing — including about the two
#    standing values that a level-based rule would page on for ever.
fresh; stats 300 > "$TMP/journal"; run
check "cold start is silent (standing hedges_undischarged + gap floor)" silent

# 2. Steady state.
stats 360 > "$TMP/journal"; run
check "steady state is silent" silent

# 3. The headline gauge.
stats 420 fills_unattributed=1 > "$TMP/journal"; run
check "fills_unattributed 0->1 pages" "fills_unattributed +1 (now 1)"
check "...at high priority" "PRIORITY=high"

# 4. ...and does NOT page again while it stands there.
cooldown_clear; stats 480 fills_unattributed=1 > "$TMP/journal"; run
check "a standing fills_unattributed=1 does not re-page" silent

# 5. ...but a further rise does.
stats 540 fills_unattributed=3 > "$TMP/journal"; run
check "fills_unattributed 1->3 pages the DELTA" "fills_unattributed +2 (now 3)"

# 6. Restart: elapsed_s falls, counters reset. The two standing values come back
#    from 0, which is exactly the shape that pages falsely if unhandled.
fresh; stats 3600 > "$TMP/journal"; run
stats 120 > "$TMP/journal"; run
check "process restart is silent" silent

# 7. Rate gauges: below threshold silent, above it pages.
# exec_failed's all-time armed max is 4, but those 4 were a BURST inside
# seconds; a burst rate says nothing about a 5-minute total. 60 is a rate at
# which the order path cannot be working, not a multiple of anything measured.
fresh; stats 300 > "$TMP/journal"; run
stats 360 exec_failed=40 > "$TMP/journal"; run
check "exec_failed +40 is silent (a burst is not a broken order path)" silent
stats 420 exec_failed=140 > "$TMP/journal"; run
check "exec_failed +100 pages" "exec_failed +100"

# cancels_unresolved is SUSTAINED, not RISE. cancel.rs parks EVERY cancel on
# entry, so the gauge is the in-flight cancel count of an engine that cancels
# and re-places continuously: a level alternating 0, 2, 0, 2 is ORDINARY, and a
# rise rule pages on half of all polls, for ever.
#
# Note the cooldown_clear inside the loop and the ACCUMULATING post log: without
# both, the first page silences the remaining seven polls and the test passes
# against a rule that is firing on half of them.
fresh; : > "$STUB_POSTS"
for e in 300 360 420 480 540 600 660 720; do
  case $e in 360|480|600|720) v=2 ;; *) v=0 ;; esac
  stats $e cancels_unresolved=$v > "$TMP/journal"
  cooldown_clear
  ( cd "$ROOT" && ./scripts/freshness_check.sh >"$TMP/stdout" 2>"$TMP/stderr" )
done
check_absent "an oscillating cancels_unresolved (0,2,0,2 x4) never pages" "cancels_unresolved"

# ...but a level that does NOT come back down does, once, after 3 polls. This is
# the gauge's own documented failure mode ("a number that does not come back
# down"), and a rise rule is structurally blind to it.
fresh
stats 300 cancels_unresolved=2 > "$TMP/journal"; run
check "sustained poll 1 of 3 is silent" silent
stats 360 cancels_unresolved=2 > "$TMP/journal"; run
check "sustained poll 2 of 3 is silent" silent
stats 420 cancels_unresolved=2 > "$TMP/journal"; run
check "sustained poll 3 of 3 pages" "held at 2 across 3 consecutive checks"
check "...reporting the real span, not an implied one" "spanning 120s of engine uptime"
cooldown_clear; stats 480 cancels_unresolved=2 > "$TMP/journal"; run
check "...and does not page again while it persists" silent

# A SKIPPED poll must BREAK the run, not bridge it. Measured before this was
# fixed: a level seen at 2, then eight polls with no stats line at all (a wedged
# engine — same process, so no restart to reset anything), then seen at 2 once
# more, fired and said "3 consecutive polls" about samples spanning 55 minutes.
# The hole is not unreported: an engine that stops printing stats pages on its
# own, which is the check immediately below this one.
fresh
stats 300 cancels_unresolved=2 > "$TMP/journal"; run
stats 600 cancels_unresolved=2 > "$TMP/journal"; run     # streak now 2
: > "$TMP/journal"; run                                  # the hole
cooldown_clear
stats 3600 cancels_unresolved=2 > "$TMP/journal"; run
check_absent "a hole in the poll series breaks the streak, it does not bridge" \
  "cancels_unresolved has held"

# ...and the blind spot that leaves, on the record rather than by accident: a
# level that genuinely dips below the threshold between every poll never
# sustains. That is CORRECT by the gauge's own doc — dipping IS "coming back
# down" — and the ratchet is what makes it safe: exhausted entries are never
# removed, so a real leak raises the FLOOR monotonically until it stops dipping.
# What stays invisible is a floor of exactly 1, which the doc calls the benign
# case (one venue-rejected place, nothing resting).
fresh; : > "$STUB_POSTS"   # these loops do NOT truncate per run — see the note
                           # on the oscillation case; without this the assertion
                           # below inherits the previous scenario's POSTs
for e in 300 600 900 1200 1500 1800 2100; do
  case $e in 600|1200|1800) v=2 ;; *) v=0 ;; esac
  stats $e cancels_unresolved=$v > "$TMP/journal"
  cooldown_clear
  ( cd "$ROOT" && ./scripts/freshness_check.sh >"$TMP/stdout" 2>"$TMP/stderr" )
  echo $? > "$TMP/rc"
done
check_absent "a level that dips below the bar every poll never sustains" \
  "cancels_unresolved has held"

# ...but the ratchet climbing past the bar DOES fire, which is the case that
# makes the blind spot above acceptable.
fresh; : > "$STUB_POSTS"
for e in 300 600 900 1200 1500 1800; do
  case $e in 300) v=0 ;; 600) v=2 ;; 900) v=1 ;; 1200) v=3 ;; 1500) v=2 ;; *) v=4 ;; esac
  stats $e cancels_unresolved=$v > "$TMP/journal"
  cooldown_clear
  ( cd "$ROOT" && ./scripts/freshness_check.sh >"$TMP/stdout" 2>"$TMP/stderr" )
  echo $? > "$TMP/rc"
done
check "a ratchet floor climbing past the bar pages" "cancels_unresolved has held at 4"
check "...dated from where the contiguous run began, not the first spike" \
  "spanning 600s of engine uptime"

# A slow leak, +1 per poll, never JUMPS — the case RISE cannot see at all.
fresh
for e in 300 360 420 480 540; do
  stats $e cancels_unresolved=$(( (e-300)/60 )) > "$TMP/journal"; run
done
check "a +1-per-poll cancels_unresolved leak pages" "cancels_unresolved has held at"

fresh; stats 300 > "$TMP/journal"; run
stats 360 kalshi_fill_gaps=5 > "$TMP/journal"; run
check "kalshi_fill_gaps +4 is a NOTE, not a page" "kalshi_fill_gaps +4"
check "...at low priority on its own stamp" "PRIORITY=low"

fresh; stats 300 > "$TMP/journal"; run
stats 360 kalshi_reconcile_failures=5 > "$TMP/journal"; run
check "kalshi_reconcile_failures +5 pages" "kalshi_reconcile_failures +5"

echo "=== the gauge check reporting its own absence ==="

# 9. The summary renamed a gauge out from under us.
fresh; stats 300 > "$TMP/journal"; run
stats 360 fills_unattributed=DROP > "$TMP/journal"; run
check "a vanished gauge pages" "no longer carries fills_unattributed"

# 10. The engine is up but has stopped printing.
fresh; : > "$TMP/journal"; run
check "no stats line from a long-up unit pages" "printed NO stats line"

# 11. ...but a unit that started 30s ago has not printed one YET.
fresh; : > "$TMP/journal"
old_since=$STUB_ACTIVE_SINCE
export STUB_ACTIVE_SINCE="$(date -d '-30 seconds' '+%a %Y-%m-%d %H:%M:%S %Z')"
run
export STUB_ACTIVE_SINCE="$old_since"
check "no stats line from a just-started unit is silent" silent

# 12. Garbage where a stats line should be.
fresh; echo '{not json' > "$TMP/journal"; run
check "an unparseable stats line pages" "gauge check FAILED to run"

# ...and it must not take the GOOD baseline down with it. The script deletes
# both CANDIDATE states when the writer dies; deleting the state itself would
# reset every gauge to cold start and lose whatever rise was in flight — the
# corrupt-state hazard arriving from the other side.
fresh; stats 300 fills_unattributed=5 > "$TMP/journal"; run
cp "$TMP/arbbot-gauge-last" "$TMP/baseline-before"
echo '{not json' > "$TMP/journal"; run
cmp -s "$TMP/baseline-before" "$TMP/arbbot-gauge-last" &&
  ok "a failed gauge run leaves the existing baseline intact" ||
  fail "a transient failure destroyed a good baseline"
cooldown_clear; stats 360 fills_unattributed=5 > "$TMP/journal"; run
check "...so the next good poll does not re-page what was already reported" silent

# 12b. elapsed_s is the restart discriminator, not a gauge, so renaming it used
#      to escape the vanished-name guard — and every standing gauge would then
#      page its full value in one burst against a baseline from another process.
fresh; stats 300 > "$TMP/journal"; run
stats 360 elapsed_s=DROP fills_unattributed=7 kalshi_fill_gaps=9 > "$TMP/journal"; run
check "a vanished elapsed_s pages" "no longer carries elapsed_s"
check_absent "...and suppresses the delta burst it would have caused" "fills_unattributed +7"

echo "=== a corrupt baseline must not wedge the alarm ==="

# 13. A truncated state file used to raise an uncaught JSONDecodeError: exit 1,
#     "gauge check FAILED to run" every 30 minutes for ever, AND no gauge read
#     at all, recoverable only by a human deleting the file. Both hazards this
#     file exists to prevent, at once.
fresh; stats 300 > "$TMP/journal"; run
printf '{"fills_unattr' > "$TMP/arbbot-gauge-last"      # truncated mid-write
stats 360 > "$TMP/journal"; run
check "a truncated baseline does NOT page and does NOT wedge" silent
[ -s "$TMP/arbbot-gauge-last" ] && python3 -c "import json,sys;json.load(open(sys.argv[1]))" \
    "$TMP/arbbot-gauge-last" 2>/dev/null &&
  ok "...it reseeded a valid baseline" ||
  fail "the baseline is still unusable"
stats 420 fills_unattributed=1 > "$TMP/journal"; run
check "...and the very next poll works normally" "fills_unattributed +1"

# 14. A state file that is valid JSON of the wrong shape.
fresh; stats 300 > "$TMP/journal"; run
printf '[1,2,3]' > "$TMP/arbbot-gauge-last"
stats 360 > "$TMP/journal"; run
check "a non-object baseline reseeds silently" silent

echo "=== the trader being deliberately stopped ==="

# 13. Between supervised sessions the unit is stopped ON PURPOSE. journalctl
#     still serves the last session's stats line; evaluating it would page about
#     a process that no longer exists.
fresh; stats 300 fills_unattributed=9 > "$TMP/journal"
export STUB_INACTIVE=arbbot-trader-m3; run; unset STUB_INACTIVE
check "a stopped trader's stale stats line is not evaluated" silent
[ -f "$TMP/arbbot-gauge-last" ] &&
  fail "a stopped trader must not write the baseline" ||
  ok "a stopped trader leaves the baseline untouched"

echo "=== a suppressed alert must not retire the increment ==="

# 15. The 30-minute cooldown is shared with every other check. If an unrelated
#     outage burns it, a gauge rise inside that window must still be reported
#     once the cooldown expires — not silently absorbed into the baseline.
fresh; stats 300 > "$TMP/journal"; run
touch "$TMP/arbbot-freshness-alerted"          # something else just paged
stats 360 fills_unattributed=1 > "$TMP/journal"; run
check "a gauge rise inside the cooldown posts nothing" silent
cooldown_clear
stats 420 fills_unattributed=1 > "$TMP/journal"; run
check "...and is still reported once the cooldown expires" "fills_unattributed +1 (now 1)"

# 16. ...INCLUDING across an engine restart. Holding the numeric baseline back
#     does not survive one: the held baseline belongs to the old process, the
#     restart rule drops it to 0, the fresh process reads 0, and the verdict is
#     gone for good. The armed unit restarted 40 minutes after the real
#     2026-07-29 naked-leg event, so this is the ordinary case.
fresh; stats 3000 > "$TMP/journal"; run
touch "$TMP/arbbot-freshness-alerted"
stats 3060 hedges_naked=1 > "$TMP/journal"; run
check "a rise inside the cooldown posts nothing (restart case)" silent
stats 60 > "$TMP/journal"; run                 # engine restarted: counters at 0
cooldown_clear
stats 120 > "$TMP/journal"; run
check "...and survives the engine restarting before the cooldown expired" "hedges_naked +1"
check "...marked as replayed, not as a fresh event" "held from an earlier poll"

# 17. curl without --fail exits 0 on a 4xx, so a revoked or mistyped topic used
#     to look exactly like a delivered page — and the verdict was retired.
fresh; stats 300 > "$TMP/journal"; run
stats 360 fills_unattributed=1 > "$TMP/journal"
export STUB_CURL_FAIL=1; run; unset STUB_CURL_FAIL
check "a rejected POST still attempts delivery" "fills_unattributed +1"
[ -f "$TMP/arbbot-freshness-alerted" ] &&
  fail "a rejected POST must not burn the cooldown" ||
  ok "a rejected POST does not burn the cooldown"
stats 420 fills_unattributed=1 > "$TMP/journal"; run
check "...and the verdict is retried, not retired" "fills_unattributed +1"

echo "=== the armed unit's page cannot be deferred by gauge noise ==="

# 18. Ten new PAGE rules now share this file. The armed unit being dead must not
#     wait 30 minutes behind one of them, so it has its own stamp.
fresh; stats 300 > "$TMP/journal"
touch "$TMP/arbbot-freshness-alerted"          # the shared window is burnt
export STUB_FAILED=arbbot-trader-m3; run; unset STUB_FAILED
check "ARMED trader-m3 FAILED pages through a burnt shared cooldown" "ARMED trader-m3 FAILED"
check "...at urgent priority on its own stamp" "PRIORITY=urgent"

# 19. Both channels firing in the SAME poll: two POSTs, two stamps, and neither
#     swallows the other. The gauge verdict must not ride the urgent body (it
#     would then be retired by the armed channel's delivery) and the armed page
#     must not wait behind the gauge one.
fresh; stats 300 > "$TMP/journal"; run
stats 360 fills_unattributed=1 > "$TMP/journal"
export STUB_FAILED=arbbot-trader-m3; run; unset STUB_FAILED
check "both channels fire in one poll: the armed one" "ARMED trader-m3 FAILED"
check "...and the gauge one" "fills_unattributed +1"
n_posts=$(grep -c '^PRIORITY=' "$STUB_POSTS")
[ "$n_posts" = 2 ] &&
  ok "...as two separate POSTs ($n_posts)" ||
  fail "expected 2 POSTs, got $n_posts"
grep -q '^PRIORITY=urgent' "$STUB_POSTS" && grep -q '^PRIORITY=high' "$STUB_POSTS" &&
  ok "...one urgent, one high" ||
  fail "the two POSTs did not carry the two priorities"

echo "=== what the stubs cannot cover ==="

# The journalctl stub cats a file and ignores every flag, so nothing above
# exercises the WINDOW the real script asks for. `--since "-600s"` is a systemd
# time spec, not a shell one, and a rejected spec would make journalctl exit
# non-zero into a `2>/dev/null` — the gauge block would then see an empty stats
# line every single poll and report "no stats line" forever, or nothing at all.
# So the flag combination is checked against the REAL journalctl. Read-only.
if command -v /usr/bin/journalctl >/dev/null; then
  /usr/bin/journalctl --user -u arbbot-trader-m3 --since "-600s" -o cat \
      --no-pager >/dev/null 2>&1 &&
    ok "real journalctl accepts the exact --since window the script uses" ||
    fail "real journalctl REJECTED --since \"-600s\" — the gauge block would read nothing"
else
  echo "  skip journalctl not present"
fi

echo
if [ "$PROOF" = 1 ]; then
  echo "=== exit-code chain, end to end (transient unit, DISARMED) ==="
  # arbbot-trader-m3 has NINE Stopped records in its journal and ZERO "Main
  # process exited ... status=" lines: every halt it has ever taken exited 0. So
  # EXIT_ORDERS_LEFT_RESTING (17) -> is-failed -> page is correct BY
  # CONSTRUCTION and has never once executed. This runs it.
  #
  # A transient unit of our own, carrying m3's failure-relevant directives
  # verbatim (Restart=no, empty SuccessExitStatus, empty RestartPreventExitStatus,
  # no OnFailure=). No production unit is started, stopped or reloaded, and
  # nothing here can reach a venue.
  U=arbbot-exitproof-$$
  # --wait blocks until the unit terminates, so the state read below is the
  # settled one and this needs no polling.
  systemd-run --user --wait --unit="$U" --property=Restart=no \
      --property=SuccessExitStatus= --property=RestartPreventExitStatus= \
      --quiet /bin/sh -c 'exit 17' >/dev/null 2>&1
  # /usr/bin/systemctl explicitly: the stub is still first on PATH, and asking
  # a fake for the answer would make this proof prove nothing.
  echo "  transient unit $U: ActiveState=$(/usr/bin/systemctl --user show "$U" -p ActiveState --value)" \
       "Result=$(/usr/bin/systemctl --user show "$U" -p Result --value)" \
       "ExecMainStatus=$(/usr/bin/systemctl --user show "$U" -p ExecMainStatus --value)"
  if /usr/bin/systemctl --user is-failed --quiet "$U"; then
    pass=$((pass+1)); echo "  ok   exit 17 from a Restart=no unit satisfies is-failed"
  else
    failed=$((failed+1)); echo "  FAIL exit 17 did NOT satisfy is-failed"
  fi
  # ...and now the REAL freshness_check.sh, with real systemd underneath and
  # only the unit NAME swapped, so the page is produced by the same branch that
  # watches the armed unit.
  cat > "$TMP/bin/systemctl" <<STUB
#!/usr/bin/env bash
a=(); for x in "\$@"; do [ "\$x" = arbbot-trader-m3 ] && x=$U; a+=("\$x"); done
exec /usr/bin/systemctl "\${a[@]}"
STUB
  chmod +x "$TMP/bin/systemctl"
  fresh; : > "$TMP/journal"; run
  check "the real watchdog pages on a really-failed unit" "FAILED (Result=exit-code)"
  /usr/bin/systemctl --user reset-failed "$U" >/dev/null 2>&1
  /usr/bin/systemctl --user stop "$U" >/dev/null 2>&1
fi

echo
echo "### $pass passed, $failed failed"
[ "$failed" = 0 ] || exit 1
