#!/usr/bin/env bash
# Parity harness: rust/target/release/arb-gauges against scripts/gauge_deltas.py.
#
# arb-gauges is the watchdog's gauge evaluator, under the contract
# gauge_deltas.py defined: one stats line on stdin, STATE STATE.ok STATE.held
# INVOCATION_ID as arguments, PAGE|/NOTE| lines on stdout, an exit code, and the
# bytes of the two candidate states. gauge_deltas.py runs ONLY here, as the
# oracle.
#
# Both are driven through identical SEQUENCES of polls, each in its own state
# dir. After every poll the harness compares exit code, stdout and both
# candidate files byte for byte (and their modes, in the synthetic cases). Then
# each dir adopts a candidate exactly as freshness_check.sh does — .ok after a
# delivered alert, .held after a suppressed one, neither after a failed run —
# so the NEXT poll also tests what was carried. A divergence is printed and the
# Rust dir is resynced from the oracle's, so one divergence counts once instead
# of cascading down the rest of the sequence.
#
# Usage: scripts/gauge_parity.sh [--journal] [--stride N] [--boundary]
#   (default)    the synthetic contract cases below; must show 0 divergences
#   --journal    also replay every stats line arbbot-trader-m3 has printed, in
#                order, under its real InvocationID (journalctl, read-only);
#                3 polls in every 13 adopt .held, as a burnt cooldown would
#   --stride N   with --journal, replay every Nth line (5 is the watchdog's
#                real cadence against the unit's 60s --stats-every)
#   --boundary   also run inputs OUTSIDE the contract, where the two are known
#                to differ (see rust/bins/arb-gauges/src/value.rs); reported,
#                never counted as failures
#
# Scratch lives in GAUGE_PARITY_DIR (default: a fresh mktemp dir under
# ${TMPDIR:-/tmp}), never in the watchdog's state dir.
set -uo pipefail
cd "$(dirname "$0")/.."
REPO=$PWD
BIN=${ARB_GAUGES:-$REPO/rust/target/release/arb-gauges}
ORACLE=$REPO/scripts/gauge_deltas.py
JOURNAL=0 STRIDE=1 BOUNDARY=0
while [ $# -gt 0 ]; do
  case $1 in
    --journal) JOURNAL=1 ;;
    --stride) STRIDE=${2:?--stride needs N}; shift ;;
    --boundary) BOUNDARY=1 ;;
    *) echo "usage: $0 [--journal] [--stride N] [--boundary]" >&2; exit 2 ;;
  esac
  shift
done
[ -x "$BIN" ] || {
  echo "no binary at $BIN — build it: (cd rust && cargo build --release -p arb-gauges)" >&2
  exit 2
}

# The watchdog unit's locale. It decides how Python decodes stdin and the state
# file (strictly, as UTF-8), so the oracle must run under the same one.
export LANG=en_US.UTF-8
unset LC_ALL LC_CTYPE PYTHONUTF8 PYTHONIOENCODING

W=${GAUGE_PARITY_DIR:-$(mktemp -d "${TMPDIR:-/tmp}/gauge-parity.XXXXXX")}
mkdir -p "$W"
PY=$W/py RS=$W/rs
S=arbbot-gauge-last
echo "scratch: $W"

# ---------------------------------------------------------------- the steps
# One step per line, tab-separated:
#   case  NAME                 fresh state dirs for a new scenario
#   poll  ADOPT INV STATS      run both; STATS is the line, sent with a newline
#   pollb ADOPT INV BYTES      same, stdin is printf %b of BYTES, exactly
#   put   BYTES                STATE becomes printf %b of BYTES
#   rm | mkdir                 STATE is removed | becomes a directory
#   args  N                    run both with N arguments (a usage error)
# ADOPT is ok|held; INV is printf %b-decoded, and `-` means empty.

c()   { printf 'case\t%s\n' "$1"; }
p()   { printf 'poll\t%s\t%s\t%s\n' "$1" "$2" "$3"; }
pb()  { printf 'pollb\t%s\t%s\t%s\n' "$1" "$2" "$3"; }
put() { printf 'put\t%s\n' "$1"; }

# A stats line shaped like the armed unit's: `s ELAPSED [key=JSON ...]`, where
# key=DROP removes a field. It carries the two shapes most likely to page
# falsely — a non-zero hedges_undischarged and kalshi_fill_gaps at its floor.
BASE_KEYS=(mode elapsed_s events killed hedges_undischarged kalshi_fill_gaps
  hedges_pending fills_unattributed hedges_overfilled dropped_unconsumed
  hedges_naked exec_dropped exec_recovered kalshi_fills_unreadable exec_failed
  kalshi_reconcile_failures cancels_unresolved sweeps_owed maker_exit_unresolved
  positions_recon_act_unresolved positions_recon_acted)
s() {
  local -A v=([mode]='"live"' [events]=1 [killed]=false [hedges_undischarged]=1
    [kalshi_fill_gaps]=1)
  local k a extra=() out="" sep=""
  for k in "${BASE_KEYS[@]}"; do [ -v "v[$k]" ] || v[$k]=0; done
  v[elapsed_s]=$1; shift
  for a in "$@"; do
    k=${a%%=*}
    [ -v "v[$k]" ] || extra+=("$k")
    v[$k]=${a#*=}
  done
  for k in "${BASE_KEYS[@]}" "${extra[@]}"; do
    [ "${v[$k]}" = DROP ] && continue
    out+="$sep\"$k\":${v[$k]}"; sep=,
  done
  printf '{%s}' "$out"
}
INVID=a1
ok()   { p ok "$INVID" "$(s "$@")"; }
held() { p held "$INVID" "$(s "$@")"; }

RISE_ZERO=(fills_unattributed hedges_overfilled dropped_unconsumed hedges_naked
  exec_dropped exec_recovered kalshi_fills_unreadable sweeps_owed
  maker_exit_unresolved positions_recon_act_unresolved positions_recon_acted)

synthetic() {
  local e i v g

  INVID=a1
  c "cold start, steady state, a rise, a standing value, a further rise"
  ok 300.0; ok 360.0; ok 420.0 fills_unattributed=1; ok 480.0 fills_unattributed=1
  ok 540.0 fills_unattributed=3

  c "every RISE row at its threshold (silent) and one past it (fires)"
  e=300; ok "$e.0"
  for g in "${RISE_ZERO[@]}"; do
    e=$((e + 60)); ok "$e.0" "$g=1"
    e=$((e + 60)); ok "$e.0"
  done
  e=$((e + 60)); ok "$e.0" exec_failed=60
  e=$((e + 60)); ok "$e.0" exec_failed=121
  e=$((e + 60)); ok "$e.0" kalshi_reconcile_failures=2
  e=$((e + 60)); ok "$e.0" kalshi_reconcile_failures=5
  e=$((e + 60)); ok "$e.0" kalshi_fill_gaps=4
  e=$((e + 60)); ok "$e.0" kalshi_fill_gaps=8

  c "every gauge rises in one poll"
  ok 300.0
  ok 360.0 "${RISE_ZERO[@]/%/=2}" exec_failed=100 kalshi_reconcile_failures=9 \
    kalshi_fill_gaps=9 cancels_unresolved=5

  c "a counter going backwards re-clamps its baseline"
  ok 300.0 fills_unattributed=5; ok 360.0 fills_unattributed=2
  ok 420.0 fills_unattributed=3; ok 480.0 fills_unattributed=0
  ok 540.0 fills_unattributed=1

  c "a restart seen by elapsed_s alone, same InvocationID"
  ok 3600.0 fills_unattributed=5 kalshi_fill_gaps=4; ok 120.0 fills_unattributed=2
  ok 180.0

  c "a restart seen by elapsed_s, no InvocationID at all"
  INVID=-; ok 3600.0; ok 120.0 fills_unattributed=2; ok 180.0

  c "a restart seen ONLY by InvocationID: the streak a dead process built"
  INVID=i1; ok 1000.0 cancels_unresolved=2; ok 1300.0 cancels_unresolved=2
  INVID=i2; ok 1600.0 cancels_unresolved=2; ok 1900.0 cancels_unresolved=2
  ok 2200.0 cancels_unresolved=2

  c "a restart seen ONLY by InvocationID: a fresh process's own rise"
  INVID=i3; ok 60.0 fills_unattributed=5
  INVID=i4; ok 290.0 fills_unattributed=3

  c "an empty InvocationID on either side is not a restart"
  INVID=-; ok 300.0; ok 360.0
  INVID=b1; ok 420.0 fills_unattributed=1
  INVID=-; ok 480.0 fills_unattributed=1
  INVID=b2; ok 540.0
  INVID=b3; ok 600.0

  c "the InvocationID is compared after Python's str.strip()"
  INVID='\x20\tabc\n'; ok 300.0
  INVID=abc; ok 360.0
  INVID='\x1cabc\x1f'; ok 420.0
  INVID='\xc2\xa0abc\xe2\x80\x83'; ok 480.0
  INVID='abc\x0b\x0c\r'; ok 540.0
  INVID='ab c'; ok 600.0
  INVID='ab c\x7f\x01\xe2\x80\x94\xf0\x9f\x98\x80"\x5c'; ok 660.0

  INVID=a1
  c "SUSTAINED fires once after 3 polls spanning real time, and re-arms after a dip"
  for e in 300 600 900 1200; do ok "$e.0" cancels_unresolved=2; done
  ok 1500.0
  for e in 1800 2100 2400 2700; do ok "$e.0" cancels_unresolved=3; done

  c "SUSTAINED: catch-up polls 60s apart do not satisfy a 15-minute rule"
  for e in 2000 2060 2120 2420 2720 3020; do ok "$e.0" cancels_unresolved=2; done

  c "SUSTAINED: a hole longer than 900s breaks the streak"
  for e in 300 600 3600 3900 4200; do ok "$e.0" cancels_unresolved=2; done

  c "SUSTAINED at its exact edges: span 480 fires, 479.9 does not; gap 900 bridges, 900.5 breaks"
  for e in 300.0 540.0 779.9 780.0; do ok "$e" cancels_unresolved=2; done
  for e in 300.0 1200.0 2100.0 2100.5 3000.5 3901.0; do ok "$e" cancels_unresolved=2; done
  ok 4000.0
  for e in 4300.0 4540.0 4780.0; do ok "$e" cancels_unresolved=2; done
  ok 4840.0; ok 4900.0 exec_failed=61; ok 4960.0 exec_failed=61

  c "SUSTAINED: 0,2,0,2 oscillation never sustains"
  for i in $(seq 0 7); do ok "$((300 + 300 * i)).0" cancels_unresolved=$((i % 2 * 2)); done

  c "SUSTAINED: pinned at 1 is noted once, after 12 polls"
  for i in $(seq 0 13); do ok "$((300 + 300 * i)).0" cancels_unresolved=1; done

  c "SUSTAINED: 1,3,1,3 breaks the level-2 streak but not the level-1"
  for i in $(seq 0 12); do ok "$((300 + 300 * i)).0" cancels_unresolved=$((1 + i % 2 * 2)); done

  c "SUSTAINED: stuck at 3 trips both rows"
  for i in $(seq 0 12); do ok "$((300 + 300 * i)).0" cancels_unresolved=3; done

  c "SUSTAINED: a ratchet floor climbing past the bar, then a +1 leak"
  i=0
  for v in 0 2 1 3 2 4 4 0 1 2 3 4 5; do
    ok "$((300 + 300 * i)).0" cancels_unresolved=$v; i=$((i + 1))
  done

  c "SUSTAINED: a restart resets the streak"
  for e in 3000 3300 60 360 660; do ok "$e.0" cancels_unresolved=2; done

  c "SUSTAINED with no elapsed_s: the streak restarts every poll"
  for i in 1 2 3 4; do ok 0 elapsed_s=DROP cancels_unresolved=2; done
  ok 900.0 cancels_unresolved=2

  c "a gauge seen and then lost pages; one never seen is a NOTE"
  ok 300.0 sweeps_owed=DROP; ok 600.0; ok 900.0 sweeps_owed=DROP
  ok 960.0 sweeps_owed=DROP; ok 1020.0
  ok 1080.0 sweeps_owed=DROP fills_unattributed=DROP

  c "several gauges absent at once are named in sorted order"
  ok 300.0 sweeps_owed=DROP positions_recon_acted=DROP maker_exit_unresolved=DROP
  ok 360.0 sweeps_owed=DROP positions_recon_acted=DROP maker_exit_unresolved=DROP \
    fills_unattributed=DROP hedges_naked=DROP
  ok 420.0
  ok 480.0 cancels_unresolved=DROP

  c "elapsed_s vanishing pages and suppresses the delta burst"
  ok 300.0; ok 0 elapsed_s=DROP fills_unattributed=7 kalshi_fill_gaps=9
  ok 420.0 fills_unattributed=7; ok 480.0 fills_unattributed=8

  c "elapsed_s as an int, and as floats whose repr changes layout"
  ok 300 cancels_unresolved=2; ok 600 cancels_unresolved=2; ok 900 cancels_unresolved=2
  for e in 0.1 0.30000000000000004 1e-05 0.0001 123456789.123456789 1e16 \
           12345678901234567890.0 1.5e+300 5e-324 491044.1 3.0E2 2.5e-7 -0.0 \
           1000000000000000.0 9007199254740993.0; do
    ok "$e" cancels_unresolved=1 fills_unattributed=1.25
  done

  c "gauges as floats, bools, negatives and the widest 64-bit integers"
  ok 300.0 fills_unattributed=1.5 cancels_unresolved=2.5
  ok 600.0 fills_unattributed=2 cancels_unresolved=2.5
  ok 900.0 fills_unattributed=true cancels_unresolved=true
  ok 1200.0 fills_unattributed=3.25 hedges_naked=1e2 cancels_unresolved=2.5
  ok 1500.0 exec_failed=-5 hedges_naked=18446744073709551615
  ok 1800.0 hedges_naked=-9223372036854775808 exec_failed=1e300 cancels_unresolved=false
  ok 2100.0 hedges_naked=9007199254740993 exec_failed=9007199254740992.0
  ok 2400.0 hedges_naked=9007199254740995 exec_failed=0.5

  c "a null gauge seeded on a cold start fails every poll until a restart"
  INVID=n1; ok 300.0 fills_unattributed=null; ok 360.0; ok 420.0
  INVID=n2; ok 480.0; ok 540.0 fills_unattributed=1

  INVID=a1
  c "a non-numeric gauge where a rule reads it fails the run"
  ok 300.0; ok 360.0 fills_unattributed='"x"'; ok 420.0 cancels_unresolved='"x"'
  ok 480.0 cancels_unresolved=null; ok 540.0 fills_unattributed='[1]'
  ok 600.0 fills_unattributed='{"b":1,"a":2}'; ok 660.0 cancels_unresolved='[]'

  c "a structured value copied into the baseline keeps its key order"
  ok 300.0 fills_unattributed='{"b":1,"a":[2,{"d":3,"c":4}],"e":"\"é\t"}'
  ok 360.0

  c "a repeated key: the last value wins"
  pb ok a1 '{"elapsed_s":300.0,"fills_unattributed":0,"fills_unattributed":0}\n'
  pb ok a1 '{"elapsed_s":360.0,"fills_unattributed":5,"fills_unattributed":1}\n'

  c "stdin that is not one stats object"
  ok 300.0
  pb ok a1 '{not json\n'
  pb ok a1 ''
  pb ok a1 '\n'
  pb ok a1 '[1,2,3]\n'
  pb ok a1 '5\n'
  pb ok a1 'null\n'
  pb ok a1 'true\n'
  pb ok a1 '"fills_unattributed elapsed_s"\n'
  pb ok a1 '"fills_unattributed"\n'
  pb ok a1 '["fills_unattributed","elapsed_s"]\n'
  pb ok a1 '["fills_unattributed"]\n'
  pb ok a1 '{"elapsed_s": 1.0}\n{"elapsed_s": 2.0}\n'
  pb ok a1 '\xef\xbb\xbf{"elapsed_s": 420.0}\n'
  pb ok a1 '{"elapsed_s": 420.0, "x": "\xff"}\n'
  pb ok a1 '  {"elapsed_s": 420.0}  \n\n'
  ok 480.0 fills_unattributed=1

  c "stdin that is not an object, against no state at all"
  pb ok a1 '["fills_unattributed","cancels_unresolved"]\n'
  pb ok a1 '"elapsed_s"\n'
  pb ok a1 '{}\n'

  c "a failed run adopts neither candidate"
  ok 300.0 fills_unattributed=5; pb ok a1 '{not json\n'; ok 360.0 fills_unattributed=5
  ok 420.0 fills_unattributed=6

  c "an unreadable state file is a cold start"
  ok 300.0
  put '{"fills_unattr'; ok 360.0 fills_unattributed=1; ok 420.0 fills_unattributed=2
  put '[1,2,3]'; ok 480.0
  put '"x"'; ok 540.0
  put 'null'; ok 600.0
  put '5'; ok 660.0
  put '{}'; ok 720.0 fills_unattributed=3
  put ''; ok 780.0
  put '\xff\xfe{}'; ok 840.0
  put '\xef\xbb\xbf{}'; ok 900.0
  put '{"a": 1} {"b": 2}'; ok 960.0
  printf 'mkdir\n'; ok 1020.0; ok 1080.0 fills_unattributed=4
  printf 'rm\n'; ok 1140.0; ok 1200.0 fills_unattributed=5

  c "the bookkeeping fields of a state object, read with the wrong types"
  put '{"elapsed_s": 300.0, "_carry": {"PAGE|from a dict key": 1, "NOTE|another": 2}}'
  held 360.0
  put '{"elapsed_s": 300.0, "_carry": "PAGE|ab"}'; held 360.0
  put '{"elapsed_s": 300.0, "_carry": ["PAGE|kept", 1, null, ["PAGE|nested"], "no bar", "NOTE|x|y", ""]}'
  held 360.0; ok 420.0
  put '{"elapsed_s": 300.0, "_carry": 5}'; ok 360.0
  put '{"elapsed_s": 300.0, "_carry": null}'; ok 360.0
  put '{"elapsed_s": 300.0, "_carry": true}'; ok 360.0
  put '{"elapsed_s": 300.0, "_carry": ["PAGE|1","PAGE|2","PAGE|3","PAGE|4","PAGE|5","PAGE|6","PAGE|7","PAGE|8","PAGE|9","PAGE|10","PAGE|11","PAGE|12","PAGE|13","PAGE|14","PAGE|1"]}'
  held 360.0 fills_unattributed=1; held 420.0 fills_unattributed=2; ok 480.0
  put '{"elapsed_s": 300.0, "_absent": "sweeps_owed fills_unattributed", "_seen": ["fills_unattributed"]}'
  ok 360.0 fills_unattributed=DROP sweeps_owed=DROP hedges_naked=DROP
  put '{"elapsed_s": 300.0, "_absent": {"sweeps_owed": 1}}'; ok 360.0 sweeps_owed=DROP
  put '{"elapsed_s": 300.0, "_absent": 5}'; ok 360.0; ok 420.0 sweeps_owed=DROP
  put '{"elapsed_s": 300.0, "_absent": null}'; ok 360.0 sweeps_owed=DROP
  put '{"elapsed_s": 300.0, "_seen": {"fills_unattributed": 1}}'; ok 360.0 fills_unattributed=DROP
  put '{"elapsed_s": 300.0, "_seen": "fills_unattributed"}'; ok 360.0 fills_unattributed=DROP
  put '{"elapsed_s": 300.0, "_seen": [1, 1.0, true, 2.5]}'; ok 360.0
  put '{"elapsed_s": 300.0, "_seen": [2.5, 1, 1.0, true, -3]}'; pb ok a1 '{}\n'
  put '{"elapsed_s": 300.0, "_seen": [null]}'; pb ok a1 '{}\n'
  put '{"elapsed_s": 300.0, "_seen": [null, 1]}'; pb ok a1 '{}\n'
  put '{"elapsed_s": 300.0, "_seen": [["x"]]}'; ok 360.0
  put '{"elapsed_s": 300.0, "_seen": 7}'; ok 360.0
  put '{"elapsed_s": 300.0, "_seen": ["zz", "fills_unattributed", "aa"]}'; ok 360.0
  put '{"elapsed_s": 300.0, "cancels_unresolved": 2, "_streak": [1, 2]}'
  ok 600.0 cancels_unresolved=2
  put '{"elapsed_s": 300.0, "_streak": {"cancels_unresolved#2": [1]}}'; ok 600.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": 2.7, "at": 600, "since": 300, "fired": 0}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": true, "at": 600.0, "since": 0, "fired": "yes"}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": "x", "at": 600.0}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": [], "at": null, "since": [], "fired": []}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": [1], "at": 600.0}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": 2, "at": {"a": 1}}}}'
  ok 1200.0 cancels_unresolved=2
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#1": {"n": 11, "at": 800.0, "since": -5000.5, "fired": false}}}'
  ok 1000.0 cancels_unresolved=1
  put '{"elapsed_s": 300.0, "_invocation": 5}'; ok 360.0
  put '{"elapsed_s": 300.0, "_invocation": true}'; ok 360.0
  put '{"elapsed_s": 300.0, "_invocation": false}'; ok 360.0
  put '{"elapsed_s": 300.0, "_invocation": [""]}'; ok 360.0
  put '{"elapsed_s": 300.0, "_invocation": "a1"}'; ok 360.0
  put '{"elapsed_s": 9007199254740993, "_invocation": "a1"}'; ok 9007199254740992.0
  put '{"elapsed_s": 9007199254740992.0, "_invocation": "a1"}'; ok 9007199254740993
  put '{"elapsed_s": 9007199254740996.0, "_invocation": "a1"}'; ok 9007199254740995
  put '{"elapsed_s": "abc", "_invocation": "a1"}'; ok 360.0
  put '{"elapsed_s": "abc", "_invocation": "zz"}'; ok 360.0
  put '{"elapsed_s": [1, 2], "_invocation": "a1"}'
  pb ok a1 '{"elapsed_s": [1, 3], "fills_unattributed": 1}\n'
  put '{"elapsed_s": [1, 2], "_invocation": "a1"}'
  pb ok a1 '{"elapsed_s": [1, 1], "cancels_unresolved": 0}\n'
  put '{"elapsed_s": [1, 2], "_invocation": "a1"}'
  pb ok a1 '{"elapsed_s": [1, 2, 0], "fills_unattributed": 1}\n'
  put '{"elapsed_s": 300.0, "_invocation": "a1", "fills_unattributed": "x"}'; ok 360.0
  put '{"elapsed_s": 300.0, "_invocation": "a1", "fills_unattributed": 2.5}'
  ok 360.0 fills_unattributed=4
  put '{"elapsed_s": 300.0, "_invocation": "a1", "fills_unattributed": true}'
  ok 360.0 fills_unattributed=3
  put '{"elapsed_s": 300.0, "_invocation": "a1", "fills_unattributed": null}'; ok 360.0
  for v in true null '[]' '{}' '"z"'; do
    put '{"elapsed_s": 0.5, "_invocation": "a1"}'
    pb ok a1 "{\"elapsed_s\": $v, \"cancels_unresolved\": 2}\n"
    put '{"elapsed_s": 0.5, "_invocation": "a1"}'
    pb ok zz "{\"elapsed_s\": $v, \"cancels_unresolved\": 2}\n"
  done

  c "held verdicts survive the cooldown, dedupe, and are capped at 12"
  ok 300.0; held 360.0 fills_unattributed=1; held 420.0 fills_unattributed=1
  ok 480.0 fills_unattributed=1; ok 540.0 fills_unattributed=1
  for i in $(seq 1 15); do held "$((600 + 60 * i)).0" fills_unattributed=$((1 + i)); done
  ok 1600.0 fills_unattributed=16; ok 1660.0 fills_unattributed=16

  c "held verdicts survive an engine restart"
  ok 3000.0; held 3060.0 hedges_naked=1; held 60.0; ok 120.0; ok 180.0

  c "a vanished gauge toggling while held leaves one entry"
  ok 300.0; held 360.0 sweeps_owed=DROP; held 420.0; held 480.0 sweeps_owed=DROP
  held 540.0; ok 600.0 sweeps_owed=DROP; ok 660.0

  c "usage errors"
  printf 'args\t0\nargs\t3\nargs\t5\n'
}

boundary() {
  INVID=a1
  c "BOUNDARY: NaN in a state file (Python's json reads it; serde_json refuses)"
  put '{"elapsed_s": NaN, "_invocation": "a1"}'; ok 300.0 fills_unattributed=1
  c "BOUNDARY: an Infinity token on stdin"
  ok 300.0; pb ok a1 '{"elapsed_s": Infinity, "fills_unattributed": 0}\n'
  c "BOUNDARY: a lone surrogate escape in a state file"
  put '{"elapsed_s": 300.0, "_carry": ["PAGE|\x5cud800"]}'; held 360.0
  c "BOUNDARY: an integer wider than 64 bits"
  ok 300.0; ok 360.0 fills_unattributed=1180591620717411303424
  c "BOUNDARY: -0, which Python reads as the integer 0"
  ok 300.0 fills_unattributed=-0
  c "BOUNDARY: a numeric STRING where Python calls float() or int()"
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": "2", "at": 600.0, "since": 300.0}}}'
  ok 1200.0 cancels_unresolved=2
  ok 300.0; pb ok a1 '{"elapsed_s": "360", "cancels_unresolved": 0}\n'
  c "BOUNDARY: a streak count stored as a float beyond i128"
  put '{"elapsed_s": 900.0, "_invocation": "a1", "_streak": {"cancels_unresolved#2": {"n": 1e300, "at": 600.0, "since": 300.0}}}'
  ok 1200.0 cancels_unresolved=2
  c "BOUNDARY: an InvocationID that is not UTF-8"
  INVID='\xff'; ok 300.0
  INVID=a1
  c "BOUNDARY: nesting deeper than serde_json's 128 levels"
  local deep; deep=$(printf '%.0s[' $(seq 200))$(printf '%.0s]' $(seq 200))
  ok 300.0; ok 360.0 "unwatched=$deep"
}

journal_steps() {
  printf 'case\tarbbot-trader-m3 journal, every stats line (stride %s)\n' "$STRIDE"
  journalctl --user -u arbbot-trader-m3 -o json \
      --output-fields=MESSAGE,_SYSTEMD_INVOCATION_ID --no-pager 2>/dev/null |
    grep -F '"MESSAGE":"{' |
    jq -r 'select((.MESSAGE | type) == "string" and (.MESSAGE | startswith("{")))
           | "\(._SYSTEMD_INVOCATION_ID // "-")\t\(.MESSAGE)"' |
    awk -v s="$STRIDE" '(NR - 1) % s == 0 { n++; print "poll\t" (n % 13 >= 10 ? "held" : "ok") "\t" $0 }'
}

# ---------------------------------------------------------------- the replay
GROUP= CASE= STEP=0 CHECK_MODES=1
declare -A POLLS=() DIVERGED=() CASES=() FAILED=() VOCAL=()

reset_dirs() { rm -rf "$PY" "$RS"; mkdir -p "$PY" "$RS"; }

# One poll in one dir, exactly as freshness_check.sh runs it.
poll_in() {  # DIR EVALUATOR...
  local d=$1; shift
  rm -f "$d/$S.ok" "$d/$S.held"
  "$@" "$d/$S" "$d/$S.ok" "$d/$S.held" "$INV" < "$W/in" > "$d/out" 2>> "$W/stderr.log"
  echo $? > "$d/rc"
}

# ...and the caller's adoption afterwards.
adopt_in() {  # DIR ok|held
  local d=$1 rc
  read -r rc < "$d/rc"
  if [ "$rc" = 0 ]; then
    if [ "$2" = ok ]; then
      [ -f "$d/$S.ok" ] && mv -f "$d/$S.ok" "$d/$S"
    else
      [ -f "$d/$S.held" ] && mv -f "$d/$S.held" "$d/$S"
    fi
  fi
  rm -f "$d/$S.ok" "$d/$S.held"
}

same() {
  diff -r -q "$PY" "$RS" > /dev/null || return 1
  [ "$CHECK_MODES" = 1 ] || return 0
  local f
  for f in "$S.ok" "$S.held"; do
    [ -e "$PY/$f" ] || continue
    [ "$(stat -c %a "$PY/$f")" = "$(stat -c %a "$RS/$f" 2>/dev/null)" ] || return 1
  done
}

diverged() {  # STDIN-DESCRIPTION
  DIVERGED[$GROUP]=$(( ${DIVERGED[$GROUP]:-0} + 1 ))
  echo "  DIVERGED [$GROUP] $CASE — poll $STEP"
  echo "    stdin: ${1:0:300}"
  diff -r "$PY" "$RS" | head -40 | sed 's/^/    /'
  [ "$CHECK_MODES" = 1 ] && stat -c '    %a %n' "$PY/$S".* "$RS/$S".* 2>/dev/null
}

replay() {  # STEPS-FILE GROUP
  local kind a b c n=0
  GROUP=$2
  while IFS=$'\t' read -r kind a b c; do
    case $kind in
      case)
        CASE=$a STEP=0; reset_dirs; CASES[$GROUP]=$(( ${CASES[$GROUP]:-0} + 1 ))
        echo "  case: $a" ;;
      rm) rm -rf "${PY:?}/$S" "${RS:?}/$S" ;;
      mkdir) rm -rf "${PY:?}/$S" "${RS:?}/$S"; mkdir "$PY/$S" "$RS/$S" ;;
      put) rm -rf "${PY:?}/$S" "${RS:?}/$S"; printf '%b' "$a" > "$PY/$S"; cp "$PY/$S" "$RS/$S" ;;
      poll | pollb)
        STEP=$((STEP + 1)) n=$((n + 1))
        POLLS[$GROUP]=$(( ${POLLS[$GROUP]:-0} + 1 ))
        if [ "$b" = - ]; then INV=""; else printf -v INV '%b' "$b"; fi
        if [ "$kind" = pollb ]; then printf '%b' "$c" > "$W/in"; else printf '%s\n' "$c" > "$W/in"; fi
        poll_in "$PY" python3 "$ORACLE"
        poll_in "$RS" "$BIN"
        local bad=0 rc
        same || { diverged "$c"; bad=1; }
        read -r rc < "$PY/rc"
        [ "$rc" = 0 ] || FAILED[$GROUP]=$(( ${FAILED[$GROUP]:-0} + 1 ))
        if [ -s "$PY/out" ]; then
          VOCAL[$GROUP]=$(( ${VOCAL[$GROUP]:-0} + 1 ))
          [ "$CHECK_MODES" = 1 ] && cat "$PY/out" >> "$W/verdicts-$GROUP.log"
        fi
        adopt_in "$PY" "$a"
        adopt_in "$RS" "$a"
        [ "$bad" = 1 ] && { rm -rf "$RS"; cp -a "$PY" "$RS"; }
        (( n % 5000 == 0 )) && echo "  ... $n polls, ${DIVERGED[$GROUP]:-0} divergences"
        ;;
      args)
        STEP=$((STEP + 1)) n=$((n + 1))
        POLLS[$GROUP]=$(( ${POLLS[$GROUP]:-0} + 1 ))
        printf '{}\n' > "$W/in"
        local d i extra
        for d in "$PY" "$RS"; do
          extra=()
          for ((i = 0; i < a; i++)); do extra+=("$d/$S.arg$i"); done
          if [ "$d" = "$PY" ]; then
            python3 "$ORACLE" "${extra[@]}" < "$W/in" > "$d/out" 2>> "$W/stderr.log"
          else
            "$BIN" "${extra[@]}" < "$W/in" > "$d/out" 2>> "$W/stderr.log"
          fi
          echo $? > "$d/rc"
        done
        same || { diverged "(usage: $a arguments)"; rm -rf "$RS"; cp -a "$PY" "$RS"; }
        ;;
    esac
  done < "$1"
}

synthetic > "$W/synthetic.tsv"
echo "=== synthetic contract cases"
replay "$W/synthetic.tsv" contract
if [ "$BOUNDARY" = 1 ]; then
  boundary > "$W/boundary.tsv"
  echo "=== boundary cases (outside the contract; divergence expected)"
  replay "$W/boundary.tsv" boundary
fi
if [ "$JOURNAL" = 1 ]; then
  journal_steps > "$W/journal.tsv"
  echo "=== arbbot-trader-m3 journal: $(grep -c '^poll' "$W/journal.tsv") stats lines (stride $STRIDE)"
  CHECK_MODES=0
  replay "$W/journal.tsv" journal
fi

echo
fail=0
for g in contract boundary journal; do
  [ -n "${POLLS[$g]:-}" ] || continue
  echo "### $g: ${POLLS[$g]} polls in ${CASES[$g]:-0} case(s) (${VOCAL[$g]:-0} with verdicts," \
       "${FAILED[$g]:-0} exiting non-zero), ${DIVERGED[$g]:-0} divergence(s)"
  [ "$g" != boundary ] && [ "${DIVERGED[$g]:-0}" != 0 ] && fail=1
done
[ -n "${GAUGE_PARITY_DIR:-}" ] || [ "$fail" = 1 ] || rm -rf "$W"
exit "$fail"
