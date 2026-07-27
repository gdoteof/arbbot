#!/bin/bash
# On runner.log silence >120s: SIGUSR1 the runner (faulthandler stack dump
# into runner.log), one dump per stall episode.
cd /home/geoff/claude/arbbot || exit 1
dumped=0
while true; do
  sleep 20
  pid=$(pgrep -f "arbbot\.exec\.main --relationship" | head -1)
  [ -z "$pid" ] && { dumped=0; continue; }
  age=$(( $(date +%s) - $(stat -c %Y data/runner.log) ))
  if [ "$age" -gt 120 ]; then
    if [ "$dumped" = 0 ]; then
      echo "$(date -u +%H:%M:%S) stall ${age}s — sending SIGUSR1 to $pid"
      kill -USR1 "$pid"
      dumped=1
    fi
  else
    dumped=0
  fi
done
