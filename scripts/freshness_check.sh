#!/usr/bin/env bash
# Alert (ntfy, 30-min self-cooldown) when scan output goes stale while the
# recorder is alive — catches a dead, hung, or crash-looping scanner.
# Adversarial review P1 fix (2026-07-20): before this, nothing watched the scanner.
set -u
cd "$(dirname "$0")/.."
TOPIC=$(python3 -c "import yaml;print(yaml.safe_load(open('config/recorder.yaml')).get('ntfy_topic',''))" 2>/dev/null)
DAY=$(date -u +%F)
STAMP=/tmp/arbbot-freshness-alerted
now=$(date +%s)
alert() {
  [ -f "$STAMP" ] && [ $(( now - $(stat -c %Y "$STAMP") )) -lt 1800 ] && return
  touch "$STAMP"
  [ -n "$TOPIC" ] && curl -s -m 10 -H "Title: arbbot watchdog" -H "Priority: high" \
    -d "$1" "https://ntfy.sh/$TOPIC" >/dev/null
}
systemctl --user is-active --quiet arbbot-recorder || { alert "recorder service DOWN"; exit 0; }
systemctl --user is-active --quiet arbbot-scanner  || { alert "scanner service DOWN"; exit 0; }
raw_age=$(( now - $(stat -c %Y data/raw/polymarket-$DAY.jsonl 2>/dev/null || echo 0) ))
[ "$raw_age" -gt 900 ] && { alert "recorder writing NOTHING for ${raw_age}s"; exit 0; }
newest_scan=$(ls -t data/scan/opportunities-$DAY.jsonl data/scan/probe-$DAY.json 2>/dev/null | head -1)
scan_age=$(( now - $(stat -c %Y "$newest_scan" 2>/dev/null || echo 0) ))
[ "$scan_age" -gt 900 ] && alert "scanner writing NOTHING for ${scan_age}s (service alive = hung or starved)"
disk_pct=$(df / --output=pcent | tail -1 | tr -dc 0-9)
[ "$disk_pct" -ge 97 ] && alert "disk ${disk_pct}% full"
exit 0
