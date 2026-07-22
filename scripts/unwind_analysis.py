"""Unwind economics, replayed from today's tape (Geoff's capital-velocity question).

For a sample of entry crossings (first observation of each distinct crossing
spell), walk the recorded books FORWARD and price a TAKER exit of the exact
basket at every subsequent book change:

    exit_pnl(t) = exit_proceeds(t) - entry_cost      (per contract, net of
                  entry AND exit fees, venue-reported overrides honored)

Outputs per spell: hold edge (the guaranteed floor), best exit pnl within the
horizon, time to first breakeven-or-better exit, and annualized ROI under
hold-to-resolution vs best-exit. Writes a JSON summary the dashboard reads.

Usage: python scripts/unwind_analysis.py [--day YYYY-MM-DD] [--max-spells N]
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.fees.curves import FeeSchedule, leg_fee
from arbbot.models.core import BookDelta, BookSnapshot, Role, Side, Venue
from arbbot.record.archive import iter_day, iter_day_merged
from arbbot.record.jsonl import parse_event
from arbbot.registry.model import Registry
from arbbot.scan.scanner import walk_cost

HORIZON_NS = 2 * 3600 * 1_000_000_000  # 2h exit window
ZERO = Decimal("0")
ONE = Decimal("1")


def exit_proceeds(basket, books, markets, fees, size):
    """Taker-sell the basket: YES legs sell into YES bids; NO legs sell into
    NO bids (= walk YES asks at 1-p). Net of exit fees. None if depth gone."""
    total = ZERO
    for leg in basket:
        book = books.get(leg["venue"], leg["market_id"])
        if book is None:
            return None
        if leg["buy_side"] == "yes":  # we hold YES: sell at YES bids
            ladder = [(l.price, l.size) for l in book.bids]
        else:  # we hold NO: NO bid at (1 - YES ask)
            ladder = [(ONE - l.price, l.size) for l in book.asks]
        remaining, proceeds = size, ZERO
        for price, avail in ladder:
            take = min(remaining, avail)
            proceeds += take * price
            remaining -= take
            if remaining == 0:
                break
        if remaining > 0:
            return None
        vwap = proceeds / size
        venue = Venue(leg["venue"])
        market = markets.get((venue, leg["market_id"]))
        fee = leg_fee(
            fees, venue, Role.TAKER, vwap, size,
            category=market.category if market else "default",
            taker_coef_override=market.taker_fee_coef_override if market else None,
        )
        total += proceeds - fee
    return total


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--max-spells", type=int, default=150)
    ap.add_argument("--scan-dir", default="data/scan")
    ap.add_argument("--raw-dir", default="data/raw")
    args = ap.parse_args()

    # entries: first observation per (signature, spell) — approximate spells by
    # 60s gaps between observations of the same signature
    entries = []
    seen_last: dict[str, int] = {}
    for o in iter_day(args.scan_dir, "opportunities", args.day):
        sig, ts = o["signature"], int(o["ts_local_ns"])
        if sig not in seen_last or ts - seen_last[sig] > 60_000_000_000:
            entries.append(o)
        seen_last[sig] = ts
    entries = entries[: args.max_spells]
    if not entries:
        print(json.dumps({"day": args.day, "spells": 0}))
        return

    reg = Registry.load("config/registry.yaml")
    # markets metadata incl. venue-reported fee overrides isn't persisted; use
    # schedule-only fees for exits (conservative: over-charges fee-free PM legs)
    markets = {}
    fees = FeeSchedule()

    # replay all events of the day once (Parquet-or-JSONL, sorted out-of-core)
    events = iter_day_merged(args.day, ["kalshi", "polymarket", "polymarket_us"],
                             args.raw_dir)

    books = BookBuilder()
    open_entries = sorted(entries, key=lambda o: int(o["ts_local_ns"]))
    results = []
    active: list[dict] = []
    ei = 0
    for raw in events:
        ts = int(raw.get("ts_local_ns", 0))
        ev = parse_event(raw)
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                pass
        while ei < len(open_entries) and int(open_entries[ei]["ts_local_ns"]) <= ts:
            o = open_entries[ei]
            size = Decimal(o["size"])
            active.append({
                "o": o, "size": size,
                "entry_cost": Decimal(o["gross_cost"]) + Decimal(o["fees"]),
                "hold_edge": Decimal(o["net_edge_total"]),
                "t0": int(o["ts_local_ns"]),
                "best_exit": None, "t_breakeven": None,
            })
            ei += 1
        still = []
        for a in active:
            if ts - a["t0"] > HORIZON_NS:
                results.append(a)
                continue
            proceeds = exit_proceeds(a["o"]["basket"], books, markets, fees, a["size"])
            if proceeds is not None:
                pnl = proceeds - a["entry_cost"]
                if a["best_exit"] is None or pnl > a["best_exit"][1]:
                    a["best_exit"] = (ts, pnl)
                if pnl >= 0 and a["t_breakeven"] is None:
                    a["t_breakeven"] = ts
            still.append(a)
        active = still
    results.extend(active)

    # aggregate
    per_rel = defaultdict(lambda: {"spells": 0, "breakeven": 0,
                                   "tb_s": [], "best_pct": [], "hold_pct": []})
    for a in results:
        rel = a["o"]["relationship_id"]
        g = per_rel[rel]
        g["spells"] += 1
        cost = float(a["entry_cost"]) or 1.0
        g["hold_pct"].append(float(a["hold_edge"]) / cost * 100)
        if a["best_exit"]:
            g["best_pct"].append(float(a["best_exit"][1]) / cost * 100)
        if a["t_breakeven"] is not None:
            g["breakeven"] += 1
            g["tb_s"].append((a["t_breakeven"] - a["t0"]) / 1e9)

    def med(v):
        return round(sorted(v)[len(v) // 2], 4) if v else None

    out = {
        "day": args.day,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "horizon_h": 2,
        "note": ("Exit fees use the schedule (venue-reported free markets are "
                 "over-charged -> results are CONSERVATIVE); exits are TAKER "
                 "(maker exits would beat these numbers)."),
        "spells": len(results),
        "relationships": {
            rel: {
                "spells": g["spells"],
                "breakeven_exits": g["breakeven"],
                "breakeven_rate": round(g["breakeven"] / g["spells"], 3),
                "t_breakeven_s_p50": med(g["tb_s"]),
                "best_exit_pct_p50": med(g["best_pct"]),
                "hold_edge_pct_p50": med(g["hold_pct"]),
            }
            for rel, g in sorted(per_rel.items(), key=lambda kv: -kv[1]["spells"])
        },
    }
    dest = Path(args.scan_dir) / f"unwind-{args.day}.json"
    dest.write_text(json.dumps(out, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
