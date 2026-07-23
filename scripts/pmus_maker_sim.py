"""Simulate a PM-US sports maker anchored on the Kalshi mid.

Strategy: rest bid = kalshi_mid - margin and ask = kalshi_mid + margin on the
PM-US moneyline (PM-US makers pay ZERO fees). Quotes reprice with --lag-s
reaction time to Kalshi moves — the recorded 5-30s PM-US lag is the edge.

Fill proxy (no PM-US trade prints in the feed): a fill happens only when the
displayed PM-US opposite side crosses THROUGH our level (ask <= our bid, or
bid >= our ask). This is pessimistic on fill count and adverse on fill
quality (book-crossing flow is the informed subset; small uninformed prints
that would fill us aren't visible), so results are a conservative bound
per-fill.

PnL: mark against the Kalshi mid (truth) at fill and at +60s/+600s.
Inventory realism: one open lot per pair-side, 30s re-quote cooldown after a
fill, positions closed at the +600s mark for accounting.

    .venv-research/bin/python scripts/pmus_maker_sim.py --day 2026-07-23 --margin 0.02
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


def load(day, rdir, prefix="sports"):
    df = pd.read_parquet(Path(rdir) / f"{prefix}-{day}.parquet")
    tob = df[df["kind"] == "tob"].copy()
    for c in ["bid", "ask"]:
        tob[c] = pd.to_numeric(tob[c], errors="coerce")
    tob = tob.dropna(subset=["bid", "ask"])
    tob = tob[(tob["ask"] > tob["bid"]) & (tob["ask"] - tob["bid"] < 0.30)]
    tob["mid"] = (tob["bid"] + tob["ask"]) / 2
    return tob


def kalshi_taker_fee(px):
    return np.ceil(7.0 * px * (1 - px)) / 100.0


def sim_pair(k, p, margin, lag_ns, hedge_lag_ns=int(1.5e9),
             cooldown_ns=int(30e9)):
    kts, kmid = k["ts_local_ns"].values, k["mid"].values
    kbid, kask = k["bid"].values, k["ask"].values
    fills = []
    last_fill = {"buy": -10**18, "sell": -10**18}
    for i in range(len(p)):
        t = int(p.iloc[i]["ts_local_ns"])
        j = np.searchsorted(kts, t - lag_ns, side="right") - 1
        if j < 0:
            continue
        fair = kmid[j]
        bid_q = np.floor((fair - margin) * 100) / 100
        ask_q = np.ceil((fair + margin) * 100) / 100
        pa, pb = p.iloc[i]["ask"], p.iloc[i]["bid"]
        for side, cond, px in (("buy", pa <= bid_q, bid_q),
                               ("sell", pb >= ask_q, ask_q)):
            if not cond or not (0.03 <= px <= 0.97):
                continue
            if t - last_fill[side] < cooldown_ns:
                continue
            last_fill[side] = t
            row = {"ts_ns": t, "side": side, "px": px, "fair_at_fill": fair}
            for h in (60, 600):
                m = np.searchsorted(kts, t + int(h * 1e9), side="right") - 1
                fm = kmid[m] if m >= 0 else np.nan
                row[f"pnl_{h}s"] = (fm - px) if side == "buy" else (px - fm)
            row["edge_at_fill"] = (fair - px) if side == "buy" else (px - fair)
            # instant Kalshi hedge at t+hedge_lag: buy PM YES -> sell Kalshi
            # YES at bid (locks bid - px); sell PM YES -> buy Kalshi YES at
            # ask (locks px - ask). Taker fee on the hedge leg.
            hj = np.searchsorted(kts, t + hedge_lag_ns, side="right") - 1
            if hj >= 0:
                if side == "buy":
                    hpx = kbid[hj]
                    row["pnl_hedged"] = hpx - px - kalshi_taker_fee(hpx)
                else:
                    hpx = kask[hj]
                    row["pnl_hedged"] = px - hpx - kalshi_taker_fee(hpx)
            else:
                row["pnl_hedged"] = np.nan
            fills.append(row)
    return fills


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--margin", type=float, default=0.02)
    ap.add_argument("--lag-s", type=float, default=1.0)
    ap.add_argument("--research-dir", default="data/research")
    ap.add_argument("--prefix", default="sports")
    args = ap.parse_args()
    rdir = Path(args.research_dir)
    pairs = [p for p in json.loads((rdir / "pairs.json").read_text())
             if p["family"] == "sports"]
    tob = load(args.day, rdir, args.prefix)
    by = dict(tuple(tob.groupby(["venue", "market_id"], sort=False)))
    fills = []
    for p in pairs:
        k = by.get(("kalshi", p["kalshi"]))
        f = by.get(("polymarket_us", p["pm"]))
        if k is None or f is None or len(k) < 10 or len(f) < 10:
            continue
        for row in sim_pair(k, f, args.margin, int(args.lag_s * 1e9)):
            row["pair_id"] = p["pair_id"]
            row["league"] = p.get("league", p["pair_id"].split("-")[1])
            fills.append(row)
    d = pd.DataFrame(fills)
    if d.empty:
        print("no fills")
        return
    d.to_parquet(rdir / f"pmus_maker_fills-{args.day}.parquet", index=False)
    print(f"margin={args.margin} lag={args.lag_s}s  fills={len(d)}  "
          f"pairs={d['pair_id'].nunique()}")
    for h in (60, 600):
        col = d[f"pnl_{h}s"].dropna()
        print(f"  pnl_{h}s: mean={col.mean():+.4f} median={col.median():+.4f} "
              f"win={(col > 0).mean():.3f} total={col.sum():+.2f}")
    hg = d["pnl_hedged"].dropna()
    print(f"  pnl_HEDGED: mean={hg.mean():+.4f} median={hg.median():+.4f} "
          f"win={(hg > 0).mean():.3f} total={hg.sum():+.2f}")
    print(f"  edge_at_fill: mean={d['edge_at_fill'].mean():+.4f} "
          f"(negative = we were already stale when crossed)")
    print(d.groupby("side")[["pnl_600s", "edge_at_fill"]].agg(["size", "mean"]).round(4))
    pp = d.groupby("pair_id")["pnl_600s"].agg(["size", "mean", "sum"]).round(4)
    print(pp.sort_values("sum", ascending=False).to_string(max_rows=14))


if __name__ == "__main__":
    main()
