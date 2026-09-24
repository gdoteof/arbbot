#!/bin/bash
# Daily log rotation + compression (arbbot-logrotate.timer, 00:20 UTC).
#
# 1. data/health.jsonl -> data/health-YYYY-MM-DD.jsonl, GAPLESS. The engine
#    reads this file's last line every 5s and FAILS CLOSED (pulls quotes) on a
#    missing or empty file, so a plain mv would pull quotes ~1 time in 5. The
#    new file is seeded with the last complete line and swapped in by rename(2),
#    so readers see the old file or the new one, never nothing. The recorder
#    reopens per write, so its next line lands in the new file.
# 2. Closed day files -> .zst: the engine's own WAL rolls (m3-wal-YYYY-MM-DD*.jsonl,
#    renamed by the writer at midnight UTC) and the health roll above. IO is
#    rate-limited and idle-class: an unthrottled 8 GB read saturated the disk
#    and froze the recorder before. The original is deleted only after the
#    decompressed stream hashes identical to it.
set -euo pipefail
cd "$(dirname "$0")/.."
D=data
yday=$(date -u -d yesterday +%F)

h=$D/health.jsonl
if [[ -s $h && ! -e $D/health-$yday.jsonl && ! -e $D/health-$yday.jsonl.zst ]]; then
  tmp=$D/.health.jsonl.new
  # Second-to-last line: the last one can be a write in flight.
  tail -n2 "$h" | head -n1 > "$tmp"
  ln "$h" "$D/health-$yday.jsonl"
  mv -f "$tmp" "$h"
  echo "rotated $h -> $D/health-$yday.jsonl"
fi

slow() { ionice -c3 nice -n19 pv -q -L 60m "$1"; }

shopt -s nullglob
for f in $D/trader-rs/m3-wal-????-??-??*.jsonl $D/health-????-??-??.jsonl; do
  # Still being written (a roll or rename seconds ago): next run.
  (( $(date +%s) - $(stat -c %Y "$f") < 300 )) && continue
  z=$f.zst
  [[ -e $z ]] && { echo "SKIP $f: $z exists"; continue; }
  slow "$f" | nice -n19 zstd -q -6 -T2 -o "$z.tmp"
  a=$(slow "$f" | sha256sum | cut -d' ' -f1)
  b=$(zstd -dc "$z.tmp" | sha256sum | cut -d' ' -f1)
  if [[ $a != "$b" ]]; then
    echo "VERIFY FAILED $f ($a vs $b): original kept" >&2
    rm -f "$z.tmp"; continue
  fi
  mv "$z.tmp" "$z"
  echo "compressed $f ($(du -h "$f" | cut -f1) -> $(du -h "$z" | cut -f1))"
  rm -f "$f"
done
