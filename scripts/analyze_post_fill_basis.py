"""For each taker basket, reconstruct from the recorded tape what the arb did
around our fill, with FULL book reconstruction (snapshot + deltas) on both legs.

Decomposes the takeable EDGE into a mid-basis component (real cross-venue
repricing = adverse selection / persistent basis) vs a spread component
(liquidity pulled after we consumed depth = mechanical, mean-reverting).

EDGE(t)      = pm_us_YES_bid - kalshi_YES_ask         (what we could take)
MID_BASIS(t) = pm_us_YES_mid - kalshi_YES_mid         (true relationship, spread-free)
Widening EDGE with flat MID_BASIS -> spread/impact.  Widening MID_BASIS -> real move.
"""

import json
from bisect import bisect_right

RAW = "data/raw"
DAY = "2026-07-21"

# (label, kalshi_id, pm_us_id, fill_epoch)
BASKETS = [
    ("Melenchon", "KXFRENCHPRES-27-JMEL", "ewc-pres-fra-2027-04-11-jeamel", 1784646659),
    ("Mamdani",   "KXTIME-26-ZOH",        "tpoyc-2026-zohmam",             1784646759),
    ("Fed-cut",   "KXRATECUT-26DEC31",    "cbpac-usfed-2026-cut",          1784646768),
]


def build_top(path, market_id):
    """Reconstruct top-of-book over time from snapshot+delta -> [(t, bid, ask)]."""
    bids, asks = {}, {}      # price(float) -> size(float)
    out = []
    with open(path) as f:
        for line in f:
            if market_id not in line:
                continue
            d = json.loads(line)
            if d.get("market_id") != market_id:
                continue
            kind = d.get("kind")
            t = d["ts_local_ns"] / 1e9
            if kind == "snapshot":
                bids = {float(b["price"]): float(b["size"]) for b in (d.get("bids") or [])}
                asks = {float(a["price"]): float(a["size"]) for a in (d.get("asks") or [])}
            elif kind == "delta":
                book = bids if d["side"] == "bid" else asks
                px, sz = float(d["price"]), float(d["size"])
                if sz <= 0:
                    book.pop(px, None)
                else:
                    book[px] = sz
            else:
                continue  # trades don't move the resting book
            bb = max(bids) if bids else None
            ba = min(asks) if asks else None
            if bb is not None and ba is not None:
                out.append((t, bb, ba))
    out.sort()
    return out


def at(series, t):
    ts = [s[0] for s in series]
    i = bisect_right(ts, t) - 1
    return (series[i][1], series[i][2]) if i >= 0 else None


def main():
    kfile = f"{RAW}/kalshi-{DAY}.jsonl"
    pfile = f"{RAW}/polymarket_us-{DAY}.jsonl"
    for label, kid, pid, fill in BASKETS:
        k = build_top(kfile, kid)
        p = build_top(pfile, pid)
        print(f"\n=== {label}  (fill @ {fill}, {len(k)} kalshi / {len(p)} pm book-updates) ===")
        if not k or not p:
            print("  (missing book data)")
            continue
        print(f"  {'offset':>8} {'k_bid':>6} {'k_ask':>6} {'pm_bid':>6} {'pm_ask':>6} "
              f"{'EDGE':>7} {'MIDbasis':>9} {'k_spr':>6} {'pm_spr':>6}")
        for off in (-1800, -600, -120, -30, 0, 30, 120, 300, 600, 1800, 3600):
            t = fill + off
            kk, pp = at(k, t), at(p, t)
            if not kk or not pp:
                continue
            kb, ka = kk
            pb, pa = pp
            edge = pb - ka
            midb = (pb + pa) / 2 - (kb + ka) / 2
            tag = "  <-- FILL" if off == 0 else ""
            print(f"  {off:>+8} {kb:>6.3f} {ka:>6.3f} {pb:>6.3f} {pa:>6.3f} "
                  f"{edge:>+7.3f} {midb:>+9.3f} {ka-kb:>6.3f} {pa-pb:>6.3f}{tag}")


if __name__ == "__main__":
    main()
