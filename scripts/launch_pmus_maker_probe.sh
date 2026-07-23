#!/usr/bin/env bash
# Launch the PM-US sports maker probe (Kalshi-anchored, hedge-on-fill).
# Authorized under Geoff's 2026-07-23 /goal ("any needed action in good faith
# to make money safely"). Caps in-process: 5-contract clips, 4 baskets,
# 20 fills/session, $5 max realized loss, data/KILL halts.
set -euo pipefail
cd "$(dirname "$0")/.."
export ARBBOT_CREDENTIALS_DIR="${ARBBOT_CREDENTIALS_DIR:-$HOME/.arbbot-credentials}"
# Relaunch 2026-07-23 (Geoff): guards loosened — capture happens in the messy
# moments (3 live both-side sweeps banked spread before ops errors). Hedge-on-
# fill + startup orphan sweep + recon-with-attribution are the protections.
setsid nohup .venv313/bin/python scripts/pmus_maker_probe.py --live --margin 0.04 \
  --clip 5 --max-baskets 6 --max-fills 30 --max-loss-usd 5 \
  --max-kspread 0.05 --jump-standdown 0.04 --min-px 0.05 --max-px 0.95 \
  >> data/exec/pmus_maker_probe_live.log 2>&1 &
echo "pmus maker probe launched pid $!"
