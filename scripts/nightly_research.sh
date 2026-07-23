#!/usr/bin/env bash
# Nightly ML research refresh (systemd: arbbot-research.timer, after the ETL):
#  1. rebuild the sports tob tape for yesterday (data/raw-sports, jsonl-only)
#  2. rerun the lead-lag study + retrain the sports model (leader label);
#     saves ONLY the report — arming any probe stays a human/board decision
#  3. toxgate evidence: join shadow gate scores vs the runner's actual fills
# Output: data/reports/research-YYYY-MM-DD.txt (dash-adjacent, greppable)
set -uo pipefail
cd "$(dirname "$0")/.."
DAY=$(date -u -d "yesterday" +%F)
OUT="data/reports/research-$(date -u +%F).txt"
{
  echo "=== nightly research $(date -u +%FT%TZ) (day=$DAY) ==="
  echo "--- sports tape ---"
  .venv313/bin/python scripts/build_tob_tape.py --day "$DAY" \
      --raw-dir data/raw-sports --out-prefix sports --jsonl-only 2>&1 | tail -1
  echo "--- lead-lag study ---"
  .venv-research/bin/python scripts/leadlag_study.py --days "$DAY" \
      --prefix sports --out sports_events.parquet --thresh 0.02 2>&1 | tail -3
  echo "--- sports model retrain (leader label; report only, NO save) ---"
  .venv-research/bin/python scripts/train_sports_model.py --horizon 120 \
      --label leader 2>&1 | tail -22
  echo "--- toxgate shadow vs runner fills (evidence for enforcement) ---"
  .venv-research/bin/python scripts/toxgate_evidence.py 2>&1 | tail -15
} > "$OUT" 2>&1
echo "wrote $OUT"
