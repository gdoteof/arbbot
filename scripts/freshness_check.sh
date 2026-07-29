#!/usr/bin/env bash
# Alert (ntfy, 30-min self-cooldown) when scan output goes stale while the
# recorder is alive — catches a dead, hung, or crash-looping scanner.
# Adversarial review P1 fix (2026-07-20): before this, nothing watched the scanner.
# 2026-07-29: extended past the Python stack. It watched arbbot-recorder,
# arbbot-scanner, data/raw and disk — and nothing else, so the Rust recorder,
# the dry-run engine, the ARMED m3 session, data/raw-rs and data/health.jsonl
# could all stop dead without a sound.
set -u
cd "$(dirname "$0")/.."
TOPIC=$(python3 -c "import yaml;print(yaml.safe_load(open('config/recorder.yaml')).get('ntfy_topic',''))" 2>/dev/null)
# The topic is a capability secret and was moved out of the (public) config into
# the credentials dir on 2026-07-22. ops/config.py falls back to that file; this
# script did not, so since the move every alert it raised was computed and then
# dropped on the floor by the `[ -n "$TOPIC" ]` guard below.
[ -z "$TOPIC" ] && [ -r "$HOME/.arbbot-credentials/ntfy_topic" ] &&
  TOPIC=$(tr -d '[:space:]' < "$HOME/.arbbot-credentials/ntfy_topic")
DAY=$(date -u +%F)
STAMP=/tmp/arbbot-freshness-alerted
now=$(date +%s)
alert() {
  [ -f "$STAMP" ] && [ $(( now - $(stat -c %Y "$STAMP") )) -lt 1800 ] && return
  touch "$STAMP"
  [ -n "$TOPIC" ] && curl -s -m 10 -H "Title: arbbot watchdog" -H "Priority: high" \
    -d "$1" "https://ntfy.sh/$TOPIC" >/dev/null
}
problems=()
problem() { problems+=("$1"); }
age() { echo $(( now - $(stat -c %Y "$1" 2>/dev/null || echo 0) )); }

# --- Python stack ---
if systemctl --user is-active --quiet arbbot-recorder; then
  raw_age=$(age "data/raw/polymarket-$DAY.jsonl")
  [ "$raw_age" -gt 900 ] && problem "recorder writing NOTHING for ${raw_age}s"
  # health_task appends one line per second unconditionally — it records a stale
  # feed, it does not stop writing for one. So the FILE going quiet is the only
  # thing that means the health task itself is dead, and that matters because
  # the engine gates quoting on this file: a frozen health file reads as "all
  # feeds fine" forever. Not alerting on the `stale` flags is deliberate:
  # PM-INTL blips true for ~3s every 300s here (known separate defect) and
  # paging on that would be pure noise.
  health_age=$(age data/health.jsonl)
  [ "$health_age" -gt 120 ] &&
    problem "health.jsonl not appended for ${health_age}s (recorder alive = health task dead; engine gates quoting on it)"
else
  problem "recorder service DOWN"
fi
if systemctl --user is-active --quiet arbbot-scanner; then
  newest_scan=$(ls -t data/scan/opportunities-$DAY.jsonl data/scan/probe-$DAY.json 2>/dev/null | head -1)
  scan_age=$(age "$newest_scan")
  [ "$scan_age" -gt 900 ] && problem "scanner writing NOTHING for ${scan_age}s (service alive = hung or starved)"
else
  problem "scanner service DOWN"
fi

# --- Rust stack ---
if systemctl --user is-active --quiet arbbot-recorder-rs; then
  raw_rs_age=$(age "data/raw-rs/polymarket-$DAY.jsonl")
  [ "$raw_rs_age" -gt 900 ] && problem "recorder-rs writing NOTHING for ${raw_rs_age}s"
else
  problem "recorder-rs service DOWN"
fi
systemctl --user is-active --quiet arbbot-trader-rs || problem "trader-rs service DOWN"
# arbbot-trader-m3 is the ARMED, real-money session. It is Restart=no on purpose
# — it is the one unit that will NOT come back by itself — but it is also
# deliberately STOPPED between supervised sessions, so is-active would page on
# every clean disarm and an alarm that cries wolf on an operator's own action is
# worse than no alarm. A clean stop leaves it inactive/Result=success; a crash
# leaves it failed. `is-failed` is therefore the only condition that means
# something is wrong. (It exits 4, not 0, if the unit is uninstalled entirely.)
if systemctl --user is-failed --quiet arbbot-trader-m3; then
  problem "ARMED trader-m3 FAILED (Result=$(systemctl --user show arbbot-trader-m3 -p Result --value)) — Restart=no, it will NOT come back on its own"
fi

disk_pct=$(df / --output=pcent | tail -1 | tr -dc 0-9)
[ "$disk_pct" -ge 97 ] && problem "disk ${disk_pct}% full"

# One POST carrying EVERY failing check rather than the first one found. With
# two stacks and a 30-minute cooldown, first-alert-wins lets a disk warning
# swallow "the armed trader died" for half an hour — the exact silence this
# script exists to break. Still one alert and one exit: the cooldown is unchanged.
[ ${#problems[@]} -gt 0 ] && alert "$(printf '%s\n' "${problems[@]}")"
exit 0
