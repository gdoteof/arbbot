"""Make-take simulation from the recorded tape (the Stage-4 go/no-go number).

For every valid (2-leg relationship, maker leg, side) track:
- whenever viability holds, rest 100 contracts at min(top+tick, max profitable
  price) (bids; mirrored for asks) — requoted on every book change (optimistic
  on cancel latency; every other rule is pessimistic);
- a tape trade on the maker market FILLS us only if it trades strictly
  THROUGH our quote;
- each fill hedges cross-venue at the books as of fill + 500ms; realized
  edge is net of all fees (venue-reported overrides honored); hedge-depth
  gone => counted unhedged (the bad case), edge 0.

Output: per-track fills / contracts / realized $ / unhedged, written to
data/scan/maketake-<day>.json for the dashboard.
"""

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.fees.curves import FeeSchedule, leg_fee
from arbbot.models.core import BookDelta, BookSnapshot, Role, Trade, Venue
from arbbot.record.archive import iter_day_merged
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry
from arbbot.scan.scanner import maker_ask_quote, maker_quote, no_ask_ladder, walk_cost

LAG_NS = 500_000_000
SIZE = Decimal("100")
ONE = Decimal("1")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    args = ap.parse_args()

    reg = Registry.load("config/registry.yaml")
    rels = [r for r in reg.relationships
            if len(r.legs) == 2 and r.verdict.value != "rejected"]
    by_market = defaultdict(list)
    for r in rels:
        for i, leg in enumerate(r.legs):
            by_market[(leg.venue.value, leg.market_id)].append(r)

    # merged, time-ordered event stream (Parquet-or-JSONL), sorted out-of-core
    # by DuckDB — no full-day load into memory
    events = iter_day_merged(args.day, ["kalshi", "polymarket", "polymarket_us"],
                             "data/raw")

    books = BookBuilder()
    fees = FeeSchedule()
    quotes = {}   # (rel_id, leg_idx, side) -> Decimal quote price
    tracks = defaultdict(lambda: {"fills": 0, "contracts": 0.0,
                                  "edge_usd": 0.0, "unhedged": 0})
    pending = []  # (fill_ts, rel, leg_idx, side, price, size)

    def requote(rel):
        for i, leg in enumerate(rel.legs):
            book = books.get(leg.venue.value, leg.market_id)
            mkts = {(l.venue, l.market_id): None for l in rel.legs}
            # markets meta: use registry-only (fees via schedule; tick default)
            for side in ("bid", "ask"):
                key = (rel.id, i, side)
                q = None
                if book is not None:
                    # PM US and Kalshi tick at 1c; only intl PM has 0.1c ticks
                    tick = (Decimal("0.001") if leg.venue is Venue.POLYMARKET
                            else Decimal("0.01"))
                    improved = False
                    if side == "bid":
                        p_max = maker_quote(rel, i, books, MKts, fees, SIZE)
                        t = book.best_bid()
                        if p_max is not None and t is not None and p_max >= t.price:
                            q = min(p_max, t.price + tick)
                            improved = q > t.price
                    else:
                        p_min = maker_ask_quote(rel, i, books, MKts, fees, SIZE)
                        t = book.best_ask()
                        if p_min is not None and t is not None and p_min <= t.price:
                            q = max(p_min, t.price - tick)
                            improved = q < t.price
                if q is None:
                    quotes.pop(key, None)
                else:
                    quotes[key] = (q, improved)

    def settle(now_ns):
        keep = []
        for (fts, rel, i, side, price, size) in pending:
            if now_ns < fts + LAG_NS:
                keep.append((fts, rel, i, side, price, size))
                continue
            hedge = rel.legs[1 - i]
            hb = books.get(hedge.venue.value, hedge.market_id)
            key = f"{rel.id}|{i}|{side}"
            tr = tracks[key]
            tr["fills"] += 1
            tr["contracts"] += float(size)
            if hb is None:
                tr["unhedged"] += 1
                continue
            ladder = no_ask_ladder(hb) if side == "bid" else list(hb.asks)
            cost = walk_cost(ladder, size)
            if cost is None:
                tr["unhedged"] += 1
                continue
            vwap = cost / size
            hfee = leg_fee(fees, hedge.venue, Role.TAKER, vwap, size)
            mfee = fees.fee(rel.legs[i].venue, Role.MAKER, price, size)
            if side == "bid":   # bought YES at price + bought NO at vwap
                edge = (ONE - price - vwap) * size - hfee - mfee
            else:               # sold YES at price + bought YES at vwap
                edge = (price - vwap) * size - hfee - mfee
            tr["edge_usd"] += float(edge)
        pending[:] = keep

    from arbbot.models.core import Market
    MKts = {}
    for r in rels:
        for leg in r.legs:
            k = (leg.venue, leg.market_id)
            if k not in MKts:
                MKts[k] = Market(
                    venue=leg.venue, market_id=leg.market_id,
                    # intl PM: 0.1c ticks, 5-share min; Kalshi + PM US: 1c ticks, 1 min
                    tick_size=(Decimal("0.001") if leg.venue is Venue.POLYMARKET
                               else Decimal("0.01")),
                    min_order_size=(Decimal("5") if leg.venue is Venue.POLYMARKET
                                    else Decimal("1")),
                )
    for raw in events:
        ev = parse_event(raw)
        settle(ev.ts_local_ns)
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                continue
        elif isinstance(ev, Trade):
            for rel in by_market.get((ev.venue.value, ev.market_id), []):
                for i, leg in enumerate(rel.legs):
                    if (leg.venue.value, leg.market_id) != (ev.venue.value, ev.market_id):
                        continue
                    qb = quotes.get((rel.id, i, "bid"))
                    if qb is not None:
                        q, imp = qb
                        # improved book: prints AT our price hit us first;
                        # tied top: strict-through only (queue unknown)
                        if (ev.price <= q) if imp else (ev.price < q):
                            pending.append((ev.ts_local_ns, rel, i, "bid", q, SIZE))
                    qa = quotes.get((rel.id, i, "ask"))
                    if qa is not None:
                        q, imp = qa
                        if (ev.price >= q) if imp else (ev.price > q):
                            pending.append((ev.ts_local_ns, rel, i, "ask", q, SIZE))
            continue
        for rel in by_market.get((ev.venue.value, ev.market_id), []):
            requote(rel)
    settle(float("inf"))

    rows = []
    for key, tr in tracks.items():
        rel_id, i, side = key.split("|")
        if tr["fills"] == 0:
            continue
        rows.append({"relationship_id": rel_id, "maker_leg_index": int(i),
                     "quote_side": side, **{k: round(v, 2) if isinstance(v, float) else v
                                            for k, v in tr.items()}})
    rows.sort(key=lambda r: -r["edge_usd"])
    out = {"day": args.day, "generated_at": datetime.now(timezone.utc).isoformat(),
           "note": ("Rest 100ct at min(top+tick, max profitable); fill only on "
                    "tape trade-THROUGH; hedge at +500ms books net of all fees; "
                    "requote latency assumed instant (the one optimistic rule). "
                    "Kalshi tape exists only after the WS upgrade (~15:42 UTC "
                    "on 2026-07-20)."),
           "total_edge_usd": round(sum(r["edge_usd"] for r in rows), 2),
           "total_fills": sum(r["fills"] for r in rows),
           "tracks": rows}
    (Path("data/scan") / f"maketake-{args.day}.json").write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2)[:1500])


if __name__ == "__main__":
    main()
