"""Cross-venue lead-lag event study over top-of-book research tapes.

For each cross-venue pair (data/research/pairs.json) and each day tape
(data/research/tob-<day>.parquet):

  - Detect "jump events" on the leader venue: mid moves >= --thresh from the
    anchor (mid at the previous event), timestamped at the raw tob row.
  - For each event, record the follower's state at t (bid/ask/mid) and the
    follower's mid at t+h for each horizon, plus a taker-PnL proxy:
    up-move: mid_f(t+h) - ask_f(t); down-move: bid_f(t) - mid_f(t+h)
    (fees not included here; applied in the modeling stage).

Both directions are measured (leader=pm and leader=kalshi).

Output: data/research/events-<day>.parquet, one row per (pair, direction,
event), plus an aggregate summary on stdout.

    .venv-research/bin/python scripts/leadlag_study.py --days 2026-07-20 2026-07-21 2026-07-22
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

HORIZONS_S = [1, 5, 30, 120, 600]


def load_tape(day: str, research_dir: Path, prefix: str = "tob"):
    df = pd.read_parquet(research_dir / f"{prefix}-{day}.parquet")
    tob = df[df["kind"] == "tob"].copy()
    for c in ["bid", "ask", "bid_sz", "ask_sz"]:
        tob[c] = pd.to_numeric(tob[c], errors="coerce")
    tob = tob.dropna(subset=["bid", "ask"])
    tob = tob[(tob["ask"] > tob["bid"]) & (tob["ask"] - tob["bid"] < 0.20)]
    tob["mid"] = (tob["bid"] + tob["ask"]) / 2
    trades = df[df["kind"] == "trade"].copy()
    trades["size"] = pd.to_numeric(trades["bid_sz"], errors="coerce")
    return tob, trades


def jump_events(ts: np.ndarray, mid: np.ndarray, thresh: float):
    """(index, jump) pairs where mid moved >= thresh from the previous
    event's mid (anchor)."""
    out = []
    if len(mid) == 0:
        return out
    anchor = mid[0]
    for i in range(1, len(mid)):
        if abs(mid[i] - anchor) >= thresh:
            out.append((i, mid[i] - anchor))
            anchor = mid[i]
    return out


def study_pair(leader: pd.DataFrame, follower: pd.DataFrame,
               thresh: float, leader_trades: pd.DataFrame | None = None) -> list[dict]:
    lts, lmid = leader["ts_local_ns"].values, leader["mid"].values
    lspread = (leader["ask"] - leader["bid"]).values
    fts = follower["ts_local_ns"].values
    fbid, fask, fmid = (follower["bid"].values, follower["ask"].values,
                        follower["mid"].values)
    if leader_trades is not None and len(leader_trades):
        trts = leader_trades["ts_local_ns"].values
        trsz = np.nan_to_num(leader_trades["size"].values)
    else:
        trts, trsz = np.array([], dtype=np.int64), np.array([])
    trcum = np.concatenate(([0.0], np.cumsum(trsz)))
    rows = []
    for i, dmid in jump_events(lts, lmid, thresh):
        t = lts[i]
        # follower state at t (last update <= t); skip if none or stale >10min
        j = np.searchsorted(fts, t, side="right") - 1
        if j < 0 or t - fts[j] > 600e9:
            continue
        a = np.searchsorted(trts, t - int(60e9), side="right")
        b = np.searchsorted(trts, t, side="right")
        row = {"ts_ns": int(t), "leader_mid": lmid[i], "dmid": dmid,
               "l_spread": lspread[i],
               "l_trades_60s": int(b - a),
               "l_trade_vol_60s": float(trcum[b] - trcum[a]),
               "f_bid": fbid[j], "f_ask": fask[j], "f_mid": fmid[j],
               "f_spread": fask[j] - fbid[j],
               "f_stale_s": (t - fts[j]) / 1e9,
               "basis": lmid[i] - fmid[j]}
        up = dmid > 0
        row["up"] = up
        # execution-realistic entry: follower quote 500ms AFTER the event
        # (we can't beat our own latency; the quote may have repriced)
        e = np.searchsorted(fts, t + 0.5e9, side="right") - 1
        e = max(e, j)
        row["entry_px"] = fask[e] if up else fbid[e]
        row["entry_slip"] = (fask[e] - fask[j]) if up else (fbid[j] - fbid[e])
        for h in HORIZONS_S:
            k = np.searchsorted(fts, t + h * 1e9, side="right") - 1
            if k < 0:
                row[f"f_dmid_{h}s"] = np.nan
                row[f"taker_pnl_{h}s"] = np.nan
            else:
                row[f"f_dmid_{h}s"] = fmid[k] - fmid[j]
                row[f"taker_pnl_{h}s"] = (fmid[k] - fask[e]) if up else (fbid[e] - fmid[k])
            # leader-mid markout: sharper truth proxy for hold-to-resolution
            # (the leader venue is the liquid one)
            m = np.searchsorted(lts, t + h * 1e9, side="right") - 1
            row[f"l_mid_{h}s"] = lmid[m] if m >= 0 else np.nan
            row[f"leader_pnl_{h}s"] = ((lmid[m] - fask[e]) if up
                                       else (fbid[e] - lmid[m])) if m >= 0 else np.nan
        rows.append(row)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--thresh", type=float, default=0.01)
    ap.add_argument("--research-dir", default="data/research")
    ap.add_argument("--prefix", default="tob")
    ap.add_argument("--out", default="events.parquet")
    args = ap.parse_args()
    rdir = Path(args.research_dir)
    pairs = json.loads((rdir / "pairs.json").read_text())

    all_events = []
    for day in args.days:
        path = rdir / f"{args.prefix}-{day}.parquet"
        if not path.exists():
            print(f"skip {day}: no tape")
            continue
        tape, trades = load_tape(day, rdir, args.prefix)
        by_mkt = dict(tuple(tape.groupby(["venue", "market_id"], sort=False)))
        tr_by_mkt = dict(tuple(trades.groupby(["venue", "market_id"], sort=False))) if len(trades) else {}
        for p in pairs:
            k = by_mkt.get(("kalshi", p["kalshi"]))
            f = by_mkt.get((p["pm_venue"], p["pm"]))
            if k is None or f is None or len(k) < 5 or len(f) < 5:
                continue
            ktr = tr_by_mkt.get(("kalshi", p["kalshi"]))
            ftr = tr_by_mkt.get((p["pm_venue"], p["pm"]))
            for direction, leader, follower, ltr in (("pm->k", f, k, ftr),
                                                     ("k->pm", k, f, ktr)):
                for row in study_pair(leader, follower, args.thresh, ltr):
                    row.update({"day": day, "pair_id": p["pair_id"],
                                "family": p["family"], "direction": direction,
                                "tradable": p["tradable"]})
                    all_events.append(row)
        print(f"{day}: cumulative events {len(all_events)}")

    ev = pd.DataFrame(all_events)
    if ev.empty:
        print("no events")
        return
    out = rdir / args.out
    ev.to_parquet(out, index=False)
    print(f"wrote {out} ({len(ev)} events)\n")

    # sign-aligned response: response * sign(dmid)
    for h in HORIZONS_S:
        ev[f"aligned_{h}s"] = ev[f"f_dmid_{h}s"] * np.sign(ev["dmid"])
    summary = (ev.groupby(["direction", "family"])
                 .agg(n=("dmid", "size"),
                      med_abs_jump=("dmid", lambda s: np.median(np.abs(s))),
                      **{f"aligned_{h}s": (f"aligned_{h}s", "mean") for h in HORIZONS_S},
                      **{f"hit_{h}s": (f"aligned_{h}s", lambda s: (s > 0).mean()) for h in HORIZONS_S},
                      taker_5s=("taker_pnl_5s", "mean"),
                      taker_30s=("taker_pnl_30s", "mean"),
                      taker_120s=("taker_pnl_120s", "mean"))
                 .round(4))
    pd.set_option("display.width", 250)
    print(summary)


if __name__ == "__main__":
    main()
