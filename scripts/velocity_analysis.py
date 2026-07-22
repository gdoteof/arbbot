"""Maker-exit + natural-unwind analysis (Geoff's capital-velocity follow-up).

MAKER EXIT (pessimistic trade-through rule, our standard): after each sampled
entry, rest exit orders at each leg's ENTRY price (a breakeven-before-fees
exit). A leg's exit "fills" only when the tape trades THROUGH that price in
the favorable direction. Metric: both-legs-filled rate within horizon + time.
  - held YES @ vwap: exit = resting YES ask at vwap -> fills when a trade
    prints ABOVE vwap.
  - held NO  @ (1-b): exit = resting NO ask == YES bid at b -> fills when a
    trade prints BELOW b (someone sold YES through our bid).

NATURAL UNWIND: opposite-direction crossing on the same relationship (exits
both legs at taker prices AND banks a second edge). Measured from the
opportunities stream. Today: directions are durably one-way -> rate 0; kept
as a standing metric in case regimes change.

Appends results into data/scan/unwind-<day>.json under "maker_exit".
"""

import argparse
import json
from collections import defaultdict
from pathlib import Path

from arbbot.record.archive import iter_day


def main() -> None:
    ap = argparse.ArgumentParser()
    from datetime import datetime, timezone
    ap.add_argument("--day", default=datetime.now(timezone.utc).strftime("%Y-%m-%d"))
    ap.add_argument("--max-spells", type=int, default=200)
    ap.add_argument("--horizon-s", type=int, default=7200)
    args = ap.parse_args()
    scan, raw = Path("data/scan"), Path("data/raw")

    # entries = spell starts (60s gap rule), as in unwind_analysis
    entries, seen_last = [], {}
    for o in iter_day(str(scan), "opportunities", args.day):
        sig, ts = o["signature"], int(o["ts_local_ns"])
        if sig not in seen_last or ts - seen_last[sig] > 60_000_000_000:
            entries.append(o)
        seen_last[sig] = ts
    entries = entries[: args.max_spells]

    # trades tape per market
    trades = defaultdict(list)  # (venue, market) -> [(ts, price)]
    for venue in ("kalshi", "polymarket", "polymarket_us"):
        for e in iter_day(str(raw), venue, args.day):
            if e.get("kind") == "trade":
                trades[(e["venue"], e["market_id"])].append(
                    (int(e["ts_local_ns"]), float(e["price"])))
    for k in trades:
        trades[k].sort()

    horizon_ns = args.horizon_s * 1_000_000_000
    per_rel = defaultdict(lambda: {"spells": 0, "full_exits": 0, "t_exit_s": []})
    for o in entries:
        t0 = int(o["ts_local_ns"])
        leg_fill_ts = []
        for leg in o["basket"]:
            vwap = float(leg["vwap"])
            key = (leg["venue"], leg["market_id"])
            fill = None
            for ts, px in trades.get(key, []):
                if ts <= t0 or ts > t0 + horizon_ns:
                    continue
                if leg["buy_side"] == "yes" and px > vwap:
                    fill = ts; break
                if leg["buy_side"] == "no" and px < (1 - vwap):
                    fill = ts; break
            leg_fill_ts.append(fill)
        g = per_rel[o["relationship_id"]]
        g["spells"] += 1
        if all(f is not None for f in leg_fill_ts):
            g["full_exits"] += 1
            g["t_exit_s"].append((max(leg_fill_ts) - t0) / 1e9)

    # natural unwind: opposite signature ever observed per relationship
    directions = defaultdict(set)
    for sig in seen_last:
        rel, d = sig.rsplit(":", 1)
        directions[rel].add(d)

    def med(v):
        return round(sorted(v)[len(v) // 2], 1) if v else None

    out = {
        "note": ("Maker exit = resting exits at ENTRY prices, filled only on "
                 "tape trade-THROUGH (pessimistic; ignores queue but requires "
                 "strict price improvement). Both legs must fill."),
        "relationships": {
            rel: {
                "spells": g["spells"],
                "both_legs_exit_rate": round(g["full_exits"] / g["spells"], 3),
                "t_exit_s_p50": med(g["t_exit_s"]),
                "reverse_direction_seen": len(directions.get(rel, set())) > 1,
            }
            for rel, g in sorted(per_rel.items(), key=lambda kv: -kv[1]["spells"])
        },
    }
    dest = scan / f"unwind-{args.day}.json"
    doc = json.loads(dest.read_text()) if dest.exists() else {}
    doc["maker_exit"] = out
    dest.write_text(json.dumps(doc, indent=2))
    print(json.dumps(out, indent=2))


if __name__ == "__main__":
    main()
