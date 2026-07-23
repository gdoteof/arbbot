#!/bin/bash
# Launch the live exec runner detached, appending to data/runner.log.
# Kept as a script so callers' command lines never contain the runner's
# cmdline string (pgrep -f watchdogs would match the caller otherwise).
cd /home/geoff/claude/arbbot || exit 1
export ARBBOT_CREDENTIALS_DIR="${ARBBOT_CREDENTIALS_DIR:-$HOME/.arbbot-credentials}"
RELS=(
  xvus-france-pres-27-jeanlucmelenchon
  xvus-france-pres-27-francoishollande
  xvus-france-pres-27-brunoretailleau
  xvus-time-poty-26-zohranmamdani
  xvus-time-poty-26-christinakoch
  xvus-time-poty-26-jeremyhansen
  xvus-time-poty-26-popeleoxiv
  xvus-time-poty-26-artificialintelligence
  xvus-time-poty-26-samaltman
  xvus-brazil-pres-26-flaviobolsonaro
  xvus-fedcut-26-usfed-2026-cut
  xvus-nobel-peace-26-sudansemergencyresponser
  xvus-nobel-peace-26-doctorswithoutborders
  xvus-nobel-peace-26-volodymyrzelensky
  xvus-nobel-peace-26-unrwa
  xvus-nobel-peace-26-donaldtrump
  xvus-nobel-peace-26-francescaalbanese
)
setsid nohup .venv313/bin/python -m "arbbot.exec.main" \
  --relationship "${RELS[@]}" --clip 25 --live >> data/runner.log 2>&1 &
  # clip 5 -> 25: phase 1 of the capacity scale-up, Geoff-approved card
  # 938df47b (2026-07-23). Capacity backtest measures $162-852/day capturable
  # vs clip-5 bites; 25 is still tiny vs measured episode depth (1250-5000
  # contracts). Phase 2 (depth-aware sizing) after 24h of clean fills/recon.
echo "runner launched pid $!"
