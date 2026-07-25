#!/usr/bin/env python3
"""Export one day's merged tape (the exact raw-dict stream golden_scan.py
consumes via iter_day_merged) as a single JSONL file — the input format for
the Rust golden replayer (rust/bins/arb-golden), which validates the Rust
scanner against the pinned digests without needing a Parquet reader yet.

  python scripts/export_merged_tape.py --day 2026-07-20 --out merged.jsonl
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from arbbot.record.archive import iter_day_merged


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--out", required=True)
    args = ap.parse_args()
    n = 0
    with open(args.out, "w") as f:
        for raw in iter_day_merged(args.day, ["kalshi", "polymarket", "polymarket_us"],
                                   args.raw_dir):
            f.write(json.dumps(raw) + "\n")
            n += 1
    print(json.dumps({"day": args.day, "events": n, "out": args.out}))


if __name__ == "__main__":
    main()
