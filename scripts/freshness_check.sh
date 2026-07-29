#!/usr/bin/env bash
# Alert (ntfy, 30-min self-cooldown) when scan output goes stale while the
# recorder is alive — catches a dead, hung, or crash-looping scanner.
# Adversarial review P1 fix (2026-07-20): before this, nothing watched the scanner.
# 2026-07-29: extended past the Python stack. It watched arbbot-recorder,
# arbbot-scanner, data/raw and disk — and nothing else, so the Rust recorder,
# the dry-run engine, the ARMED m3 session, data/raw-rs, data/health.jsonl and
# data/health-rs.jsonl could all stop dead without a sound.
set -u
cd "$(dirname "$0")/.."
TOPIC=$(python3 -c "import yaml;print(yaml.safe_load(open('config/recorder.yaml')).get('ntfy_topic',''))" 2>/dev/null)
# The topic is a capability secret and was moved out of the (public) config into
# the credentials dir on 2026-07-22. ops/config.py falls back to that dir; this
# script did not, so for the next 7 days every alert it raised was computed and
# then dropped on the floor. Resolve in ops/config.py's ORDER, not just its last
# step: a unit that grows a systemd LoadCredential= or an ARBBOT_CREDENTIALS_DIR
# pointing elsewhere is exactly how that divergence comes back.
if [ -z "$TOPIC" ]; then
  for d in "${CREDENTIALS_DIRECTORY:-}" "${ARBBOT_CREDENTIALS_DIR:-}" "${HOME:-}/.arbbot-credentials"; do
    [ -n "$d" ] && [ -r "$d/ntfy_topic" ] &&
      { TOPIC=$(tr -d '[:space:]' < "$d/ntfy_topic"); break; }
  done
fi
DAY=$(date -u +%F)
# All state lives in /tmp, as it always has. The override exists ONLY so
# scripts/test_freshness_gauges.sh can exercise this file for real: a test that
# had to touch /tmp/arbbot-freshness-alerted would reset the LIVE 30-minute
# cooldown on every run, and one that wrote the LIVE gauge baseline would make
# the next real poll compute its deltas from fiction. Never set in the unit.
STATE_DIR=${ARBBOT_WATCHDOG_STATE_DIR:-/tmp}
STAMP=$STATE_DIR/arbbot-freshness-alerted            # paging channel (unchanged path)
STAMP_ROUTINE=$STATE_DIR/arbbot-freshness-routine    # low-priority channel, own cooldown
# The ARMED unit being dead or unwatched gets its OWN cooldown, because the
# gauge block below adds ten new conditions that can page, and every one of them
# would otherwise compete for the stamp above. This file's own comment reserves
# that window: "routine noise can never consume the 30-minute window that ARMED
# trader-m3 FAILED needs" — and a gauge page at 12:00 deferring an is-failed
# page to 12:30, on a Restart=no trader that may have left orders resting, is
# exactly that. Same severity, different stamp: they can no longer starve
# each other.
STAMP_ARMED=$STATE_DIR/arbbot-freshness-armed
now=$(date +%s)
# Returns 0 ONLY if a POST was accepted. Both early returns used to be a bare
# `return`, which yields the status of the preceding command — 0 in both cases —
# so a caller could not tell a delivered alert from a suppressed one. The gauge
# carry-forward below depends on that distinction.
alert() {  # $1 = cooldown stamp, $2 = ntfy priority, $3 = body
  [ -f "$1" ] && [ $(( now - $(stat -c %Y "$1") )) -lt 1800 ] && return 1
  # Topic checked BEFORE the stamp is touched: an unresolvable topic must not
  # burn the cooldown, and must not be silent. The 7-day outage above happened
  # precisely because this function stamped and then discarded the POST. Print
  # only the ABSENCE of the topic — it is a capability secret, never log it.
  [ -n "$TOPIC" ] ||
    { echo "watchdog: ntfy topic UNRESOLVED — alerts are disabled" >&2; return 1; }
  # --fail, and the stamp AFTER the POST, for the same reason the topic check
  # precedes it: an alert that did not land must not burn the window, and must
  # not report success. Without --fail curl exits 0 on a 4xx/5xx — a revoked or
  # mistyped topic returns HTTP 4xx and looked, to every caller, exactly like a
  # delivered page. A failure here now leaves no stamp, so the next poll retries
  # rather than waiting out a cooldown it never earned.
  curl -s --fail -m 10 -H "Title: arbbot watchdog" -H "Priority: $2" \
    -d "$3" "https://ntfy.sh/$TOPIC" >/dev/null || return 1
  touch "$1"
}
# Two severities on two cooldowns. Everything that is money, or that hides
# money, pages. A deliberate `systemctl stop` of a shadow or a dry run does not:
# it goes out at Priority: low on its OWN stamp, so routine noise can never
# consume the 30-minute window that "ARMED trader-m3 FAILED" needs.
paging=()
routine=()
armed=()
page() { paging+=("$1"); }
note() { routine+=("$1"); }
armed_page() { armed+=("$1"); }   # own stamp; see STAMP_ARMED above
age() { echo $(( now - $(stat -c %Y "$1" 2>/dev/null || echo 0) )); }

# --- Python stack ---
# arbbot-recorder is load-bearing — the armed engine reads its socket — so a
# stopped one pages even though stopping it is deliberate.
if systemctl --user is-active --quiet arbbot-recorder; then
  raw_age=$(age "data/raw/polymarket-$DAY.jsonl")
  [ "$raw_age" -gt 900 ] && page "recorder writing NOTHING for ${raw_age}s"
  # health_task appends one line per second unconditionally — it records a stale
  # feed, it does not stop writing for one. So the FILE going quiet is the only
  # thing that means the health task itself is dead, and that matters because
  # the engine gates quoting on this file: a frozen health file reads as "all
  # feeds fine" forever. Not alerting on the `stale` flags is deliberate:
  # PM-INTL blips true for ~3s every 300s here (known separate defect) and
  # paging on that would be pure noise.
  health_age=$(age data/health.jsonl)
  [ "$health_age" -gt 120 ] &&
    page "health.jsonl not appended for ${health_age}s (recorder alive = health task dead; engine gates quoting on it)"
else
  page "recorder service DOWN"
fi
if systemctl --user is-active --quiet arbbot-scanner; then
  newest_scan=$(ls -t data/scan/opportunities-$DAY.jsonl data/scan/probe-$DAY.json 2>/dev/null | head -1)
  scan_age=$(age "$newest_scan")
  # a scanner that is UP and producing nothing is the defect this script was
  # written for, and no operator asked for it — that still pages.
  [ "$scan_age" -gt 900 ] && page "scanner writing NOTHING for ${scan_age}s (service alive = hung or starved)"
else
  note "scanner service DOWN"
fi

# --- Rust stack ---
# recorder-rs is --shadow and trader-rs has no --enable-orders: neither touches
# money. Both are Restart=always/RestartSec=5, so a real crash self-heals inside
# one 5-minute poll and the state that actually survives long enough to be seen
# here is an operator's `systemctl stop` — e.g. freeing CPU for a cargo build,
# which this repo already nices to 19 because builds starve the live feed.
# Paging high on that trains the operator to swipe away the notification that
# one day reads ARMED trader-m3 FAILED. Reported, quietly, on its own cooldown.
if systemctl --user is-active --quiet arbbot-recorder-rs; then
  raw_rs_age=$(age "data/raw-rs/polymarket-$DAY.jsonl")
  [ "$raw_rs_age" -gt 900 ] && note "recorder-rs writing NOTHING for ${raw_rs_age}s"
  health_rs_age=$(age data/health-rs.jsonl)
  [ "$health_rs_age" -gt 120 ] && note "health-rs.jsonl not appended for ${health_rs_age}s (recorder-rs health task dead)"
else
  note "recorder-rs service DOWN"
fi
systemctl --user is-active --quiet arbbot-trader-rs || note "trader-rs service DOWN"
# arbbot-trader-m3 is the ARMED, real-money session. It is Restart=no on purpose
# — it is the one unit that will NOT come back by itself — but it is also
# deliberately STOPPED between supervised sessions, so is-active would page on
# every clean disarm and an alarm that cries wolf on an operator's own action is
# worse than no alarm. A clean stop leaves it inactive/Result=success; a crash
# leaves it failed. is-failed is therefore the only RUNTIME condition that means
# something is wrong.
if systemctl --user is-failed --quiet arbbot-trader-m3; then
  armed_page "ARMED trader-m3 FAILED (Result=$(systemctl --user show arbbot-trader-m3 -p Result --value)) — Restart=no, it will NOT come back on its own"
fi
# But is-failed returns 4, not 0, for a unit that is gone or unloadable, so on
# its own it makes the ARMED unit the only one whose DISAPPEARANCE is silent —
# the is-active checks above already page when the shadow units vanish.
# arbbot-trader-m3.service is not tracked in this repo and carries hand-edited
# --balance figures, so a typo'd directive plus a daemon-reload gives
# LoadState=error and an armed session watched by nothing, forever. This fires
# on a deliberate decommission too; that is the trade, because a broken unit
# file is common and accidental, a decommission is rare and deliberate, and only
# one of the two is survivable.
[ "$(systemctl --user show arbbot-trader-m3 -p LoadState --value)" = loaded ] ||
  armed_page "ARMED trader-m3 unit NOT LOADED — it is no longer being watched"

# --- ARMED engine safety gauges -------------------------------------------
# Everything above this line watches whether processes are ALIVE. Nothing
# watched what the armed one was SAYING. `engine/mod.rs::summary` prints a JSON
# line of safety counters to stdout every 60s and, measured 2026-07-29, had zero
# consumers outside rust/ — including the ones whose own doc comments call them
# THE alarm for a specific money condition. See scripts/gauge_deltas.py for the
# per-gauge page/note decision and the reasoning behind each threshold.
#
# Gated on is-active for the same reason the block above uses is-failed: this
# unit is deliberately STOPPED between supervised sessions, and journalctl
# happily serves the LAST session's stats line to a stopped unit. Evaluating
# that would page about a process that no longer exists — and about counters
# frozen at whatever they were when a human disarmed. Stopped means skip, and
# skipping also leaves the baseline untouched, which is what makes the restart
# rule in gauge_deltas.py work.
GAUGE_STATE=$STATE_DIR/arbbot-gauge-last
GAUGE_WINDOW=600   # 10x the unit's --stats-every 60
rm -f "$GAUGE_STATE.ok" "$GAUGE_STATE.held"   # never adopt a previous run's leftovers
if systemctl --user is-active --quiet arbbot-trader-m3; then
  stats=$(journalctl --user -u arbbot-trader-m3 --since "-${GAUGE_WINDOW}s" \
            -o cat --no-pager 2>/dev/null | grep '^{' | tail -1)
  if [ -z "$stats" ]; then
    # A check that cannot run must say so rather than pass. But a unit that
    # started 30s ago has not printed its first stats line yet and is not a
    # fault, so this is measured against how long the unit has actually been up
    # — otherwise every restart pages.
    started=$(date -d "$(systemctl --user show arbbot-trader-m3 \
                -p ActiveEnterTimestamp --value)" +%s 2>/dev/null || echo "$now")
    [ $(( now - started )) -gt "$GAUGE_WINDOW" ] &&
      page "ARMED trader-m3 has printed NO stats line in ${GAUGE_WINDOW}s while ACTIVE — every safety gauge is unread, and that tick shares the engine's select loop with the kill switch and the hedge deadline"
  else
    gout=$(printf '%s\n' "$stats" | python3 scripts/gauge_deltas.py \
             "$GAUGE_STATE" "$GAUGE_STATE.ok" "$GAUGE_STATE.held")
    grc=$?
    if [ $grc -ne 0 ]; then
      # Neither candidate state is trustworthy if the writer died, and adopting
      # a half-written one is how a single crash turns into an alarm that pages
      # for ever and reads nothing. gauge_deltas.py falls back to a cold start
      # on an unreadable state file precisely so this branch stays rare.
      rm -f "$GAUGE_STATE.ok" "$GAUGE_STATE.held"
      page "ARMED trader-m3 gauge check FAILED to run (gauge_deltas.py exited $grc) — the safety counters were NOT read this cycle; see the watchdog journal"
    elif [ -n "$gout" ]; then
      while IFS='|' read -r sev msg; do
        case "$sev" in PAGE) page "$msg" ;; NOTE) note "$msg" ;; esac
      done <<< "$gout"
    fi
  fi
fi

disk_pct=$(df / --output=pcent | tail -1 | tr -dc 0-9)
[ "$disk_pct" -ge 97 ] && page "disk ${disk_pct}% full"

# One POST carrying EVERY failing check rather than the first one found. The old
# script did `alert; exit 0` on the first condition, so every later check was
# never EVALUATED — a chronically down recorder did not delay the report of a
# hung scanner or a full disk, it meant they were never reported at all. Still
# one alert and one exit per channel: the cooldown mechanism is unchanged.
#
# The armed-unit channel goes out FIRST and on its own stamp, so nothing below
# can defer it (see STAMP_ARMED).
if [ ${#armed[@]} -gt 0 ]; then
  alert "$STAMP_ARMED" urgent "$(printf '%s\n' "${armed[@]}")"
fi
delivered=1   # nothing to deliver counts as delivered
if [ ${#paging[@]} -gt 0 ]; then
  delivered=0
  alert "$STAMP" high "$(printf '%s\n' "${paging[@]}" "${routine[@]}")" && delivered=1
elif [ ${#routine[@]} -gt 0 ]; then
  delivered=0
  alert "$STAMP_ROUTINE" low "$(printf '%s\n' "${routine[@]}")" && delivered=1
fi

# Which candidate state to adopt. Both advance the numeric baseline; they differ
# only in whether this poll's verdicts are RETAINED as text for the next poll to
# repeat. The 30-minute cooldown is shared with every other check here, so an
# unrelated outage at 12:00 suppresses the POST at 12:05 — and a
# `fills_unattributed +1` in that window must not be retired unreported.
#
# Holding the numeric baseline back instead does NOT work, which is why this is
# text: a held baseline belongs to the old process, so an engine restart before
# the cooldown expires drops it to 0, the fresh process reads 0, and the verdict
# is lost for good. The armed unit restarted 40 minutes after the 2026-07-29
# naked-leg event, so that is the ordinary case.
if [ "$delivered" = 1 ]; then
  [ -f "$GAUGE_STATE.ok" ] && mv -f "$GAUGE_STATE.ok" "$GAUGE_STATE"
else
  [ -f "$GAUGE_STATE.held" ] && mv -f "$GAUGE_STATE.held" "$GAUGE_STATE"
fi
rm -f "$GAUGE_STATE.ok" "$GAUGE_STATE.held"
exit 0
