"""Build a top-of-book research tape for cross-venue pair analysis.

Streams one recorded day through BookBuilder (same reconstruction the live
system uses) and emits a row every time the top of book changes for a market
that belongs to a cross-venue pairing:

  - registry cross-venue relationships (config/registry.yaml), and
  - the sports equivalence map (data/scan/sports_equiv_map.json)
    long-ticker <-> PM moneyline pairs.

Trade events for tracked markets are passed through as-is.

Output: data/research/tob-<day>.parquet
Columns: ts_local_ns BIGINT, venue, market_id, kind ('tob'|'trade'),
         bid/ask/bid_sz/ask_sz DOUBLE (tob rows), and
         trade_price/trade_size DOUBLE, taker_side (trade rows) —
         each kind fills only its own columns, the rest are NULL.

Events come from the one read layer (arbbot.record.archive.iter_day_merged),
parquet-or-JSONL. For non-default raw dirs (data/raw-sports) the parquet
archive dir is derived as the sibling parquet-<suffix> (data/parquet-sports),
never the prod data/parquet, whose stems would collide.

    .venv313/bin/python scripts/build_tob_tape.py --day 2026-07-22
    .venv313/bin/python scripts/build_tob_tape.py --day 2026-07-22 --raw-dir data/raw-sports
"""

import argparse
import csv
import json
import tempfile
from pathlib import Path

import duckdb

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.archive import iter_day_merged
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry

CSV_COLUMNS = {"ts_local_ns": "BIGINT", "venue": "VARCHAR",
               "market_id": "VARCHAR", "kind": "VARCHAR",
               "bid": "DOUBLE", "ask": "DOUBLE",
               "bid_sz": "DOUBLE", "ask_sz": "DOUBLE",
               "trade_price": "DOUBLE", "trade_size": "DOUBLE",
               "taker_side": "VARCHAR"}


def tracked_markets(registry_path: str, sports_map_path: str) -> set[tuple[str, str]]:
    tracked: set[tuple[str, str]] = set()
    reg = Registry.load(registry_path)
    for rel in reg.relationships:
        venues = {leg.venue.value for leg in rel.legs}
        if len(venues) > 1 and rel.verdict.value != "rejected":
            for leg in rel.legs:
                tracked.add((leg.venue.value, leg.market_id))
    smp = Path(sports_map_path)
    if smp.exists():
        smap = json.loads(smp.read_text())
        for m in smap.get("matches", []):
            if m.get("kalshi_long_ticker") and m.get("pm_moneyline"):
                tracked.add(("kalshi", m["kalshi_long_ticker"]))
                tracked.add(("polymarket_us", m["pm_moneyline"]))
    return tracked


def parquet_dir_for(raw_dir: str) -> Path | None:
    """None (default sibling data/parquet) for the prod raw dir; for variants
    like data/raw-sports, the sibling parquet-sports — keeps sports stems from
    colliding with the prod archive."""
    raw = Path(raw_dir)
    if raw.name == "raw":
        return None
    return raw.parent / raw.name.replace("raw", "parquet", 1)


def run(day: str, raw_dir: str, out_path: Path,
        registry_path: str = "config/registry.yaml",
        sports_map_path: str = "data/scan/sports_equiv_map.json") -> dict:
    tracked = tracked_markets(registry_path, sports_map_path)
    tracked_ids = {mid for _, mid in tracked}
    books = BookBuilder()
    last_tob: dict[tuple[str, str], tuple] = {}
    n_ev = n_rows = 0

    tmp = tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, newline="",
        dir=out_path.parent)
    writer = csv.writer(tmp)
    writer.writerow(CSV_COLUMNS)
    venues = ["kalshi", "polymarket", "polymarket_us"]
    source = iter_day_merged(day, venues, raw_dir,
                             parquet_dir=parquet_dir_for(raw_dir))
    try:
        for raw in source:
            n_ev += 1
            if raw.get("market_id") not in tracked_ids:
                continue
            try:
                ev = parse_event(raw)
            except ValueError:
                continue
            key = (ev.venue.value, ev.market_id)
            if key not in tracked:
                continue
            if isinstance(ev, Trade):
                writer.writerow([ev.ts_local_ns, key[0], key[1], "trade",
                                 "", "", "", "",
                                 str(ev.price), str(ev.size),
                                 ev.taker_side or ""])
                n_rows += 1
                continue
            if isinstance(ev, BookSnapshot):
                books.apply_snapshot(ev)
            elif isinstance(ev, BookDelta):
                try:
                    if books.apply_delta(ev) is None:
                        continue
                except (GapDetected, NotSynced):
                    continue
            else:
                continue
            book = books.get(key[0], key[1])
            if book is None:
                continue
            bid = max(book.bids, key=lambda l: l.price, default=None)
            ask = min(book.asks, key=lambda l: l.price, default=None)
            tob = (str(bid.price) if bid else "",
                   str(ask.price) if ask else "",
                   str(bid.size) if bid else "",
                   str(ask.size) if ask else "")
            if last_tob.get(key) == tob:
                continue
            last_tob[key] = tob
            writer.writerow([ev.ts_local_ns, key[0], key[1], "tob", *tob,
                             "", "", ""])
            n_rows += 1
    finally:
        tmp.close()

    cols = ", ".join(f"'{k}': '{t}'" for k, t in CSV_COLUMNS.items())
    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT * FROM read_csv('{tmp.name}', header=true,
                                     columns={{{cols}}})
              ORDER BY ts_local_ns)
        TO '{out_path}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    Path(tmp.name).unlink()
    return {"day": day, "events_seen": n_ev, "rows": n_rows,
            "tracked_markets": len(tracked), "out": str(out_path)}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--out-dir", default="data/research")
    ap.add_argument("--out-prefix", default="tob")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{args.out_prefix}-{args.day}.parquet"
    print(json.dumps(run(args.day, args.raw_dir, out)))


if __name__ == "__main__":
    main()
