#!/usr/bin/env bash
# Launch the ML toxicity live probe (board approval 6ab60913, Geoff 2026-07-23).
# Mirrors scripts/launch_runner.sh conventions. Caps enforced in-process:
# 5-contract clips, 12 fills/session, $3 max realized loss, data/KILL halts.
set -euo pipefail
cd "$(dirname "$0")/.."
export ARBBOT_CREDENTIALS_DIR="${ARBBOT_CREDENTIALS_DIR:-$HOME/.arbbot-credentials}"
setsid nohup .venv313/bin/python scripts/toxicity_probe.py --live --clip 5 \
  --max-fills 12 --max-loss-usd 3 >> data/exec/toxicity_probe_live.log 2>&1 &
echo "toxicity probe launched pid $!"
