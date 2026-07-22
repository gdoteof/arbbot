"""Archive CLOSED-day JSONL to columnar Parquet, then delete the JSONL.

Fixes the disk cliff (raw tape ~2GB/day, 25-123x smaller as Parquet) and makes
analysis out-of-core queryable. The recorder is untouched — it keeps writing
today's data as append-only JSONL; this runs the next day on static files.

SAFETY: only archives days strictly before today (UTC); verifies the Parquet
row count EXACTLY equals the JSONL line count before deleting; on any mismatch
it keeps the JSONL and reports (never silently loses irreplaceable data).

Usage: python scripts/archive_to_parquet.py [--day YYYY-MM-DD] [--keep-jsonl]
Default day = yesterday (UTC). Targets: data/raw/<venue>-<day>.jsonl and
data/scan/opportunities-<day>.jsonl (the big derived log). Small scan files
(maker/lifetimes/probe) stay JSONL — the dashboard tails them live.
"""

import argparse
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from arbbot.record.archive import PARQUET_DIR

RAW_STEMS = ("kalshi", "polymarket", "polymarket_us")
# raw tapes interleave 3 heterogeneous row types; every field must survive
REQUIRED_RAW_COLS = {"kind", "venue", "market_id", "side", "price", "size",
                     "bids", "asks", "seq", "ts_local_ns"}


def line_count(path: Path) -> int:
    return int(subprocess.check_output(["grep", "-c", "", str(path)]).split()[0])


def archive_one(con, src: Path, day: str, out_dir: Path, keep: bool) -> tuple[str, int]:
    pq = out_dir / f"{src.stem}.parquet"  # src.stem already '<stem>-<day>'
    if pq.exists() and not src.exists():
        return ("already-archived", 0)
    # sample_size=-1: infer schema from ALL rows, not a sample — heterogeneous
    # tapes (snapshot/delta/trade) otherwise silently DROP columns not seen in
    # the sample (observed: delta side/price/size lost when the file starts
    # with snapshots). Full-scan inference guarantees the field union.
    con.execute(
        f"COPY (SELECT * FROM read_json_auto('{src}', format='newline_delimited', "
        f"union_by_name=true, sample_size=-1, maximum_object_size=20000000)) "
        f"TO '{pq}' (FORMAT parquet, COMPRESSION zstd)"
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    lines = line_count(src)
    if rows != lines:
        pq.unlink(missing_ok=True)
        return (f"MISMATCH rows={rows} lines={lines} — kept JSONL", 0)
    if src.parent.name == "raw" or src.stem.rsplit("-", 3)[0] in RAW_STEMS:
        cols = {d[0] for d in con.execute(f"DESCRIBE SELECT * FROM read_parquet('{pq}')").fetchall()}
        if not REQUIRED_RAW_COLS <= cols:
            pq.unlink(missing_ok=True)
            return (f"SCHEMA-LOSS missing {REQUIRED_RAW_COLS - cols} — kept JSONL", 0)
    if not keep:
        src.unlink()
    return (f"ok {lines} rows", rows)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=None, help="UTC day YYYY-MM-DD (default: yesterday)")
    ap.add_argument("--keep-jsonl", action="store_true", help="don't delete after verify")
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--scan-dir", default="data/scan")
    ap.add_argument("--out", default=PARQUET_DIR)
    args = ap.parse_args()

    today = datetime.now(timezone.utc).date()
    day = args.day or (today - timedelta(days=1)).isoformat()
    if datetime.fromisoformat(day).date() >= today:
        raise SystemExit(f"refusing to archive {day}: not a closed day (today={today})")

    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()

    counts = load_counts(out_dir)
    targets = [(Path(args.raw_dir), s) for s in RAW_STEMS] + [(Path(args.scan_dir), "opportunities")]
    for directory, stem in targets:
        src = directory / f"{stem}-{day}.jsonl"
        if not src.exists():
            # skip missing (already archived, or venue absent that day)
            continue
        status, rows = archive_one(con, src, day, out_dir, args.keep_jsonl)
        print(f"{stem}-{day}: {status}")
        if rows and not args.keep_jsonl and stem in RAW_STEMS:
            counts[stem] = counts.get(stem, 0) + rows
    save_counts(out_dir, counts)
    print(f"archived-day counts (raw venues): {counts}")


def load_counts(out_dir: Path) -> dict:
    p = out_dir / "archived_counts.json"
    return json.loads(p.read_text()) if p.exists() else {}


def save_counts(out_dir: Path, counts: dict) -> None:
    (out_dir / "archived_counts.json").write_text(json.dumps(counts))


if __name__ == "__main__":
    main()
