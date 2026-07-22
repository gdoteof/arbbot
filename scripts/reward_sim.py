"""Polymarket liquidity-reward capture simulation from recorded books.

For each pooled market in our universe, replay the day's recorded PM events,
sample the book every 60s, and score the AGGREGATE resting book with the
public formula: S(order) = ((v - s)/v)^2 * size for orders within v cents of
the size-cutoff midpoint, per side; two-sided rule Q_min = max(min(Q1,Q2),
max(Q1,Q2)/3) for mid in [0.10,0.90], strict min outside.

Our hypothetical quote: BOTH sides, min_size, at v/2 from mid. Estimated
share per sample = ourQ / (ourQ + aggregateQ). CONSERVATIVE: the aggregate
book is treated as one perfectly two-sided competitor; real competitors are
penalized individually for one-sidedness, so true share would be higher.

Writes est_capture_usd_day into data/scan/rewards.json per pool.
"""

import json
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot
from arbbot.record.archive import iter_day
from arbbot.record.jsonl import parse_event


def side_score(levels, mid, v):
    q = 0.0
    for l in levels:
        s = abs(float(l.price) - mid) * 100  # cents from mid
        if s <= v and float(l.size) > 0:
            q += ((v - s) / v) ** 2 * float(l.size)
    return q


def q_min(q1, q2, mid):
    if 0.10 <= mid <= 0.90:
        return max(min(q1, q2), max(q1, q2) / 3.0)
    return min(q1, q2)


def main() -> None:
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    scan = Path("data/scan")
    rewards = json.loads((scan / "rewards.json").read_text())
    pools = {p["token"]: p for p in rewards["pools"]}
    if not pools:
        return

    books = BookBuilder()
    samples = defaultdict(lambda: {"share_sum": 0.0, "n": 0})
    next_sample = 0
    for raw in iter_day("data/raw", "polymarket", day):
        if raw.get("market_id") not in pools:
            continue
        ev = parse_event(raw)
        if isinstance(ev, BookSnapshot):
            books.apply_snapshot(ev)
        elif isinstance(ev, BookDelta):
            try:
                books.apply_delta(ev)
            except (GapDetected, NotSynced):
                continue
        else:
            continue
        ts = ev.ts_local_ns
        if ts < next_sample:
            continue
        next_sample = ts + 60_000_000_000
        for tok, p in pools.items():
            b = books.get("polymarket", tok)
            if not b or not b.bids or not b.asks:
                continue
            mid = (float(b.bids[0].price) + float(b.asks[0].price)) / 2
            v = float(p.get("max_spread_c") or 3.0)
            min_size = float(p.get("min_size") or 100)
            q1 = side_score(b.bids, mid, v)
            q2 = side_score(b.asks, mid, v)
            comp = q_min(q1, q2, mid)
            ours_per_side = ((v - v / 2) / v) ** 2 * min_size  # at v/2 from mid
            ours = q_min(ours_per_side, ours_per_side, mid)
            if ours + comp > 0:
                s = samples[tok]
                s["share_sum"] += ours / (ours + comp)
                s["n"] += 1

    total = 0.0
    for p in rewards["pools"]:
        s = samples.get(p["token"])
        if s and s["n"]:
            share = s["share_sum"] / s["n"]
            p["est_share"] = round(share, 4)
            p["est_capture_usd_day"] = round(share * p["daily_pool_usd"], 2)
            total += p["est_capture_usd_day"]
        else:
            p["est_share"] = None
            p["est_capture_usd_day"] = None
    rewards["sim"] = {
        "config": "both sides, min_size, at half max_spread from mid",
        "est_total_usd_day": round(total, 2),
        "note": ("CONSERVATIVE: aggregate book scored as one perfectly "
                 "two-sided competitor; excludes maker rebates on fills and "
                 "the in-play multiplier. Excludes adverse selection cost of "
                 "actually being filled."),
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    (scan / "rewards.json").write_text(json.dumps(rewards, indent=2))
    top = sorted((p for p in rewards["pools"] if p.get("est_capture_usd_day")),
                 key=lambda x: -x["est_capture_usd_day"])[:6]
    print(f"est total capture: ${total}/day at config; top:")
    for p in top:
        print(f"  ${p['est_capture_usd_day']:>8.2f}/day  share {p['est_share']:.1%}  {p['title'][:48]}")


if __name__ == "__main__":
    main()
