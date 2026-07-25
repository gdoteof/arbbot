"""Archive CLOSED-day JSONL to columnar Parquet, then delete the JSONL.

Fixes the disk cliff (raw tape ~2GB/day, 25-123x smaller as Parquet) and makes
analysis out-of-core queryable. The recorder is untouched — it keeps writing
today's data as append-only JSONL; this runs the next day on static files.

Raw tapes are written with the FIXED schema (archive.RAW_JSON_COLUMNS): no
read_json_auto inference, every scalar VARCHAR/BIGINT, bids/asks as JSON text,
ts_venue verbatim (auto-inference typed ISO strings as TIMESTAMP and silently
truncated nanoseconds). Scan files (opportunities) keep auto inference.

SAFETY: only archives days strictly before today (UTC); verifies the Parquet
row count EXACTLY equals the JSONL line count; raw tapes additionally verify
CONTENT — both sources are streamed through a canonical per-row JSON sha256
and must digest-match — before deleting; on any mismatch it keeps the JSONL
and reports (never silently loses irreplaceable data).

Usage: python scripts/archive_to_parquet.py [--day YYYY-MM-DD] [--keep-jsonl]
Default day = yesterday (UTC). Targets: data/raw/<venue>-<day>.jsonl and
data/scan/opportunities-<day>.jsonl (the big derived log). Small scan files
(maker/lifetimes/probe) stay JSONL — the dashboard tails them live.
"""

import argparse
import hashlib
import json
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path

import duckdb

from arbbot.record.archive import (PARQUET_DIR, iter_parquet_rows,
                                   raw_json_reader)
from arbbot.record.jsonl import iter_jsonl

RAW_STEMS = ("kalshi", "polymarket", "polymarket_us")


def line_count(path: Path) -> int:
    return int(subprocess.check_output(["grep", "-c", "", str(path)]).split()[0])


def canonical_row(raw: dict) -> bytes:
    """Canonical per-row JSON for content verification, applied identically to
    both sources: drop nulls (SQL NULL and JSON null alike — the read layer
    drops them, model_dump_json emits them), then sort keys, no whitespace."""
    d = {k: v for k, v in raw.items() if v is not None}
    return json.dumps(d, sort_keys=True, separators=(",", ":")).encode() + b"\n"


def content_digests(src: Path, pq: Path) -> tuple[str, str]:
    """(jsonl_sha256, parquet_sha256) of the canonical row streams — the
    parquet side goes through the read layer, so a match proves a consumer
    reading the archive sees byte-identical events to the raw tape."""
    h_jl, h_pq = hashlib.sha256(), hashlib.sha256()
    for _, raw in iter_jsonl(src):
        h_jl.update(canonical_row(raw))
    for row in iter_parquet_rows(pq):
        h_pq.update(canonical_row(row))
    return h_jl.hexdigest(), h_pq.hexdigest()


def archive_one(con, src: Path, day: str, out_dir: Path, keep: bool) -> tuple[str, int]:
    pq = out_dir / f"{src.stem}.parquet"  # src.stem already '<stem>-<day>'
    if pq.exists() and not src.exists():
        return ("already-archived", 0)
    is_raw = src.parent.name == "raw" or src.stem.rsplit("-", 3)[0] in RAW_STEMS
    # raw tapes: explicit fixed schema — inference on a heterogeneous tape has
    # silently DROPPED columns absent from a sample and TIMESTAMP-truncated
    # ts_venue. sample_size=-1 (scan files): full-scan inference for the same
    # dropped-column reason.
    reader = (raw_json_reader(src) if is_raw else
              f"read_json_auto('{src}', format='newline_delimited', "
              f"union_by_name=true, sample_size=-1, maximum_object_size=20000000)")
    con.execute(
        f"COPY (SELECT * FROM {reader}) "
        f"TO '{pq}' (FORMAT parquet, COMPRESSION zstd)"
    )
    rows = con.execute(f"SELECT count(*) FROM read_parquet('{pq}')").fetchone()[0]
    lines = line_count(src)
    if rows != lines:
        pq.unlink(missing_ok=True)
        return (f"MISMATCH rows={rows} lines={lines} — kept JSONL", 0)
    if is_raw:
        d_jl, d_pq = content_digests(src, pq)
        if d_jl != d_pq:
            pq.unlink(missing_ok=True)
            return (f"CONTENT-MISMATCH jsonl={d_jl[:16]} parquet={d_pq[:16]} "
                    f"— kept JSONL", 0)
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
