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
body=""; prio=""
while [ $# -gt 0 ]; do
  case "$1" in
    -d) body=$2; shift 2 ;;
    -H) case "$2" in "Priority: "*) prio=${2#Priority: } ;; esac; shift 2 ;;
    *)  shift ;;
  esac
done
printf 'PRIORITY=%s\n%s\n---\n' "$prio" "$body" >> "$STUB_POSTS"
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
# run SCENARIO -- feeds a journal, runs the REAL script, captures the POST
run() {
  : > "$STUB_POSTS"
  ( cd "$ROOT" && ./scripts/freshness_check.sh >"$TMP/stdout" 2>"$TMP/stderr" )
}
check() {  # check NAME EXPECT ; EXPECT is "silent" or a substring of the POST
  local name=$1 want=$2 got
  got=$(cat "$STUB_POSTS" 2>/dev/null)
  if [ "$want" = silent ]; then
    if [ -z "$got" ]; then pass=$((pass+1)); echo "  ok   $name"; return; fi
  elif printf '%s' "$got" | grep -qF -- "$want"; then
    pass=$((pass+1)); echo "  ok   $name"; return
  fi
  failed=$((failed+1))
  echo "  FAIL $name"
  echo "       wanted: $want"
  echo "       got:    ${got:-<no post>}"
  [ -s "$TMP/stderr" ] && sed 's/^/       err: /' "$TMP/stderr"
}
cooldown_clear() { rm -f "$TMP/arbbot-freshness-alerted" "$TMP/arbbot-freshness-routine"; }
fresh() { rm -f "$TMP/arbbot-gauge-last" "$TMP/arbbot-gauge-last.new"; cooldown_clear; }

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
fresh; stats 300 > "$TMP/journal"; run
stats 360 exec_failed=4 > "$TMP/journal"; run
check "exec_failed +4 is silent (worst armed session total was 4)" silent
stats 420 exec_failed=40 > "$TMP/journal"; run
check "exec_failed +36 pages" "exec_failed +36"

fresh; stats 300 > "$TMP/journal"; run
stats 360 cancels_unresolved=1 > "$TMP/journal"; run
check "cancels_unresolved +1 is silent (one in flight at sample time)" silent
stats 420 cancels_unresolved=3 > "$TMP/journal"; run
check "cancels_unresolved +2 pages" "cancels_unresolved +2"

fresh; stats 300 > "$TMP/journal"; run
stats 360 kalshi_fill_gaps=5 > "$TMP/journal"; run
check "kalshi_fill_gaps +4 is a NOTE, not a page" "kalshi_fill_gaps +4"
check "...at low priority on its own stamp" "PRIORITY=low"

fresh; stats 300 > "$TMP/journal"; run
stats 360 kalshi_reconcile_failures=5 > "$TMP/journal"; run
check "kalshi_reconcile_failures +5 pages" "kalshi_reconcile_failures +5"

# 8. A ratcheted level pages once and then stops. cancels_unresolved never
#    returns to 0 once an entry is exhausted (engine/cancel.rs newly_exhausted
#    marks, it does not remove), so this is the noise case that matters.
fresh; stats 300 cancels_unresolved=4 > "$TMP/journal"; run
cooldown_clear; stats 360 cancels_unresolved=4 > "$TMP/journal"; run
cooldown_clear; stats 420 cancels_unresolved=4 > "$TMP/journal"; run
check "a ratcheted cancels_unresolved does not page for ever" silent

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
STUB_ACTIVE_SINCE="$(date -d '-30 seconds' '+%a %Y-%m-%d %H:%M:%S %Z')" run
check "no stats line from a just-started unit is silent" silent

# 12. Garbage where a stats line should be.
fresh; echo '{not json' > "$TMP/journal"; run
check "an unparseable stats line pages" "gauge check FAILED to run"

echo "=== the trader being deliberately stopped ==="

# 13. Between supervised sessions the unit is stopped ON PURPOSE. journalctl
#     still serves the last session's stats line; evaluating it would page about
#     a process that no longer exists.
fresh; stats 300 fills_unattributed=9 > "$TMP/journal"
STUB_INACTIVE=arbbot-trader-m3 run
check "a stopped trader's stale stats line is not evaluated" silent
[ -f "$TMP/arbbot-gauge-last" ] &&
  { echo "  FAIL a stopped trader must not write the baseline"; failed=$((failed+1)); } ||
  { echo "  ok   a stopped trader leaves the baseline untouched"; pass=$((pass+1)); }

echo "=== a suppressed alert must not retire the increment ==="

# 14. The 30-minute cooldown is shared with every other check. If an unrelated
#     outage burns it, a gauge rise inside that window must still be reported
#     once the cooldown expires — not silently absorbed into the baseline.
fresh; stats 300 > "$TMP/journal"; run
touch "$TMP/arbbot-freshness-alerted"          # something else just paged
stats 360 fills_unattributed=1 > "$TMP/journal"; run
check "a gauge rise inside the cooldown posts nothing" silent
cooldown_clear
stats 420 fills_unattributed=1 > "$TMP/journal"; run
check "...and is still reported once the cooldown expires" "fills_unattributed +1 (now 1)"

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
