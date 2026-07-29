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
STAMP=/tmp/arbbot-freshness-alerted            # paging channel (unchanged path)
STAMP_ROUTINE=/tmp/arbbot-freshness-routine    # low-priority channel, own cooldown
now=$(date +%s)
alert() {  # $1 = cooldown stamp, $2 = ntfy priority, $3 = body
  [ -f "$1" ] && [ $(( now - $(stat -c %Y "$1") )) -lt 1800 ] && return
  # Topic checked BEFORE the stamp is touched: an unresolvable topic must not
  # burn the cooldown, and must not be silent. The 7-day outage above happened
  # precisely because this function stamped and then discarded the POST. Print
  # only the ABSENCE of the topic — it is a capability secret, never log it.
  [ -n "$TOPIC" ] ||
    { echo "watchdog: ntfy topic UNRESOLVED — alerts are disabled" >&2; return; }
  touch "$1"
  curl -s -m 10 -H "Title: arbbot watchdog" -H "Priority: $2" \
    -d "$3" "https://ntfy.sh/$TOPIC" >/dev/null
}
# Two severities on two cooldowns. Everything that is money, or that hides
# money, pages. A deliberate `systemctl stop` of a shadow or a dry run does not:
# it goes out at Priority: low on its OWN stamp, so routine noise can never
# consume the 30-minute window that "ARMED trader-m3 FAILED" needs.
paging=()
routine=()
page() { paging+=("$1"); }
note() { routine+=("$1"); }
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
  page "ARMED trader-m3 FAILED (Result=$(systemctl --user show arbbot-trader-m3 -p Result --value)) — Restart=no, it will NOT come back on its own"
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
  page "ARMED trader-m3 unit NOT LOADED — it is no longer being watched"

disk_pct=$(df / --output=pcent | tail -1 | tr -dc 0-9)
[ "$disk_pct" -ge 97 ] && page "disk ${disk_pct}% full"

# One POST carrying EVERY failing check rather than the first one found. The old
# script did `alert; exit 0` on the first condition, so every later check was
# never EVALUATED — a chronically down recorder did not delay the report of a
# hung scanner or a full disk, it meant they were never reported at all. Still
# one alert and one exit per channel: the cooldown mechanism is unchanged.
if [ ${#paging[@]} -gt 0 ]; then
  alert "$STAMP" high "$(printf '%s\n' "${paging[@]}" "${routine[@]}")"
elif [ ${#routine[@]} -gt 0 ]; then
  alert "$STAMP_ROUTINE" low "$(printf '%s\n' "${routine[@]}")"
fi
exit 0
