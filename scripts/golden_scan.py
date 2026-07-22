"""Golden differential harness — the rewrite parity gate.

Replays a recorded day through the SAME BookBuilder + scanner the live system
uses and emits every scan decision as canonical JSON plus a SHA256 digest.
Deterministic by construction (total event order, Decimal math, no wall
clock): run it twice on the same tape, get the same digest.

Purpose: a port of the decision core (e.g. to Rust) must reproduce the digest
byte-for-byte over the same tape before it can touch money paths. Read-only —
never contacts a venue.

    .venv313/bin/python scripts/golden_scan.py --day 2026-07-21
    .venv313/bin/python scripts/golden_scan.py --day 2026-07-21 --max-events 200000
"""

import argparse
import hashlib
import json
from collections import defaultdict
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookDelta, BookSnapshot, Market
from arbbot.record.archive import iter_day_merged
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry
from arbbot.scan.scanner import scan_relationship


def run(day: str, registry_path: str, raw_dir: str, out_path,
        max_events: int = 0) -> dict:
    reg = Registry.load(registry_path)
    rels = [r for r in reg.relationships
            if len(r.legs) == 2 and r.verdict.value != "rejected"]
    by_market = defaultdict(list)
    markets = {}
    for r in rels:
        for leg in r.legs:
            by_market[(leg.venue.value, leg.market_id)].append(r)
            markets.setdefault((leg.venue, leg.market_id),
                               Market(venue=leg.venue, market_id=leg.market_id))

    books, fees = BookBuilder(), FeeSchedule()
    digest = hashlib.sha256()
    n_ev = n_book = n_opp = 0
    out = open(out_path, "w") if out_path else None
    for raw in iter_day_merged(day, ["kalshi", "polymarket", "polymarket_us"], raw_dir):
        n_ev += 1
        if max_events and n_ev > max_events:
            break
        try:
            ev = parse_event(raw)
        except ValueError:
            continue
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                continue
        else:
            continue
        n_book += 1
        for rel in by_market.get((ev.venue.value, ev.market_id), ()):
            for opp in scan_relationship(rel, books, markets, fees,
                                         ts_local_ns=ev.ts_local_ns):
                line = json.dumps(opp.to_json_dict(), sort_keys=True,
                                  separators=(",", ":"))
                digest.update(line.encode() + b"\n")
                n_opp += 1
                if out:
                    out.write(line + "\n")
    if out:
        out.close()
    return {"day": day, "events": n_ev, "book_events": n_book,
            "opportunities": n_opp, "sha256": digest.hexdigest()}


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--registry", default="config/registry.yaml")
    ap.add_argument("--raw-dir", default="data/raw")
    ap.add_argument("--out", default=None,
                    help="canonical decisions JSONL (default data/golden/golden-<day>.jsonl)")
    ap.add_argument("--max-events", type=int, default=0,
                    help="stop after N tape events (quick runs)")
    args = ap.parse_args()
    out = args.out
    if out is None:
        Path("data/golden").mkdir(parents=True, exist_ok=True)
        out = f"data/golden/golden-{args.day}.jsonl"
    res = run(args.day, args.registry, args.raw_dir, out, args.max_events)
    res["out"] = out
    print(json.dumps(res))


if __name__ == "__main__":
    main()
