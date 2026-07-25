# Intent-parity pins — multi-day (2026-07-24)

Python is feature-frozen (Geoff, 2026-07-24; see p3-shell.md), so these pins
are durable until cutover. Each row is a THREE-WAY byte match on the same
merged tape: Python `scripts/intent_replay.py` ≡ Rust `arb-intent` ≡ the full
concurrent shell `arb-trader --bench-tape` (intent files diff empty, digests
identical). Registry snapshot: sha256 `37429e909f5e7c29…` (the registry is
private and never committed — regenerate a snapshot and expect NEW digests if
the registry has changed).

## Defaults (harness conventions: clip 5, jitter 0, min_requote 15s)

| day | events | book_events | intents | sha256 |
|---|---|---|---|---|
| 2026-07-20 | 5,474,100 | 5,454,794 | 80 | `3bf83fa164bfabdd387c765b0b0d701dedf03711cbeaad77edfa2cb3a7be6f07` |
| 2026-07-21 | 12,577,189 | 12,256,075 | 3,064 | `dc161088a6d5b54e40120821007af6e4416a7b69252dc83b716d93c3e8a89b27` |
| 2026-07-22 | 16,359,775 | 14,516,845 | 11,218 | `d72da2059603861d54ab51c78de06af09474ed481c1ea0d6b9fb7c7f6266be20` |

07-21/07-22 are sports-heavy days — 38x/140x the intent volume of 07-20 —
so the pins now cover the quoter's full behavioral surface, not a quiet day.

## Featured knobs (census re-pin fixtures, 2026-07-20)

| fixture | intents | sha256 |
|---|---|---|
| toxgate(0.055 on 2 mkts)+suppress | 59,471 (incl. skip records) | `c889fe27759585121c55f0a24979f8333483f0522acedb5c1e6f6536e73273e5` |
| + min-apr 6.0 / resolve-years 0.5 | 15 | `f8d4db049bcb85616a9b9a7309705c277ebbe574c45fcede9a56806e88deb683` |

Featured run for 2026-07-21 (same toxgate fixture + APR knobs): see row
appended below when pinned.

## Re-run procedure

```bash
# export a day's tape (Parquet-transparent), then run all three sides:
PYTHONPATH=src python scripts/export_merged_tape.py --day <D> \
    --raw-dir /home/geoff/claude/arbbot/data/raw --out /tmp/merged-<D>.jsonl
PYTHONPATH=src python scripts/intent_replay.py --tape /tmp/merged-<D>.jsonl \
    --registry <registry-snapshot> --out /tmp/py.jsonl
rust/target/release/arb-intent   --tape /tmp/merged-<D>.jsonl --registry <registry-snapshot> --out /tmp/rs.jsonl
rust/target/release/arb-trader   --bench-tape /tmp/merged-<D>.jsonl --registry <registry-snapshot>
diff /tmp/py.jsonl /tmp/rs.jsonl   # must be empty; all three sha256 equal
```

A decision-changing Python bug fix (the only kind of change the freeze
permits) re-runs this table and pings board card ca24e4ec.
