"""Build a top-of-book research tape for cross-venue pair analysis.

Streams one recorded day through BookBuilder (same reconstruction the live
system uses) and emits a row every time the top of book changes for a market
that belongs to a cross-venue pairing:

  - registry cross-venue relationships (config/registry.yaml), and
  - the sports equivalence map (data/scan/sports_equiv_map.json)
    long-ticker <-> PM moneyline pairs.

Trade events for tracked markets are passed through as-is.

Output: data/research/tob-<day>.parquet
Columns: ts_local_ns, venue, market_id, kind ('tob'|'trade'),
         bid, ask, bid_sz, ask_sz  (trade rows: bid=price, bid_sz=size)

    .venv313/bin/python scripts/build_tob_tape.py --day 2026-07-22
"""

import argparse
import csv
import heapq
import json
import tempfile
from pathlib import Path

import duckdb

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.archive import iter_day_merged
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry


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


def iter_day_jsonl(day: str, venues: list[str], raw_dir: str):
    """Direct JSONL merge (bypasses the archive layer — needed for
    data/raw-sports, whose stems would collide with the prod parquet dir).
    Files are single-writer append-ordered, so a heap merge on
    (ts_local_ns, venue, seq) is a total order."""
    from arbbot.record.jsonl import iter_jsonl

    def gen(venue):
        path = Path(raw_dir) / f"{venue}-{day}.jsonl"
        if not path.exists():
            return
        for _, raw in iter_jsonl(path):
            yield (raw.get("ts_local_ns", 0), venue, raw.get("seq", 0)), raw

    for _, raw in heapq.merge(*(gen(v) for v in venues), key=lambda t: t[0]):
        yield raw


def run(day: str, raw_dir: str, out_path: Path,
        registry_path: str = "config/registry.yaml",
        sports_map_path: str = "data/scan/sports_equiv_map.json",
        jsonl_only: bool = False) -> dict:
    tracked = tracked_markets(registry_path, sports_map_path)
    tracked_ids = {mid for _, mid in tracked}
    books = BookBuilder()
    last_tob: dict[tuple[str, str], tuple] = {}
    n_ev = n_rows = 0

    tmp = tempfile.NamedTemporaryFile(
        "w", suffix=".csv", delete=False, newline="",
        dir=out_path.parent)
    writer = csv.writer(tmp)
    writer.writerow(["ts_local_ns", "venue", "market_id", "kind",
                     "bid", "ask", "bid_sz", "ask_sz"])
    venues = ["kalshi", "polymarket", "polymarket_us"]
    source = (iter_day_jsonl(day, venues, raw_dir) if jsonl_only
              else iter_day_merged(day, venues, raw_dir))
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
                # trade rows reuse tob columns: bid=price, ask=taker_side,
                # bid_sz=size
                writer.writerow([ev.ts_local_ns, key[0], key[1], "trade",
                                 str(ev.price), ev.taker_side or "",
                                 str(ev.size), ""])
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
            writer.writerow([ev.ts_local_ns, key[0], key[1], "tob", *tob])
            n_rows += 1
    finally:
        tmp.close()

    con = duckdb.connect()
    con.execute(f"""
        COPY (SELECT ts_local_ns::BIGINT AS ts_local_ns, venue, market_id, kind,
                     bid, ask, bid_sz, ask_sz
              FROM read_csv_auto('{tmp.name}', header=true,
                                 types={{'ts_local_ns':'BIGINT','bid':'VARCHAR',
                                        'ask':'VARCHAR','bid_sz':'VARCHAR','ask_sz':'VARCHAR'}})
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
    ap.add_argument("--jsonl-only", action="store_true",
                    help="read raw JSONL directly (required for data/raw-sports)")
    args = ap.parse_args()
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{args.out_prefix}-{args.day}.parquet"
    print(json.dumps(run(args.day, args.raw_dir, out,
                         jsonl_only=args.jsonl_only)))


if __name__ == "__main__":
    main()
