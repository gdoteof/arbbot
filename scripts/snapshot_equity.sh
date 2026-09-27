#!/bin/bash
# Equity history sample (arbbot-equity.timer, every 15 min).
#
# Appends the armed trader's latest venue-capital reading (data/exec/capital.json)
# to data/exec/equity_history.jsonl, which the dashboard's daily equity table
# reads. A reading older than the gate's own max_age_s is skipped, not copied:
# a stopped trader leaves its last capital.json behind, and re-stamping it would
# draw a flat line through the outage. `day` is the reading's local date, so the
# dashboard buckets days without a timezone database.
set -euo pipefail
cd "$(dirname "$0")/.."
jq -c 'select(now - .at <= .max_age_s)
  | {at, day: (.at | strflocaltime("%Y-%m-%d")),
     accounts: (.accounts | map_values(
       {available_cash_usd, reserved_cash_usd, positions_value_usd, equity_usd}))}' \
  data/exec/capital.json >> data/exec/equity_history.jsonl
