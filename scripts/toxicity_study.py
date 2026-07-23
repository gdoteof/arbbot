"""Maker fill-toxicity study on Kalshi trade prints.

For every Kalshi trade in a cross-venue-paired market, compute the passive
(maker) side's mark-out: maker sold to a taker-buy at p => maker_pnl_h =
p - mid(t+h); maker bought from a taker-sell => mid(t+h) - p. Positive =
spread capture survived; negative = adverse selection.

Features are time-t observable, including the cross-venue basis (paired PM
mid minus Kalshi mid, signed toward the aggressor) — the "other venue already
moved" signal. Model: LightGBM classifier P(maker_pnl_h < 0), grouped CV by
market.

    .venv-research/bin/python scripts/toxicity_study.py --days 2026-07-21 2026-07-22 --horizon 60
"""

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd

MARKOUTS_S = [10, 60, 300, 600]


def load_day(rdir: Path, day: str):
    df = pd.read_parquet(rdir / f"tob-{day}.parquet")
    tob = df[df["kind"] == "tob"].copy()
    for c in ["bid", "ask"]:
        tob[c] = pd.to_numeric(tob[c], errors="coerce")
    tob = tob.dropna(subset=["bid", "ask"])
    tob = tob[(tob["ask"] > tob["bid"]) & (tob["ask"] - tob["bid"] < 0.20)]
    tob["mid"] = (tob["bid"] + tob["ask"]) / 2
    tr = df[df["kind"] == "trade"].copy()
    tr["price"] = pd.to_numeric(tr["bid"], errors="coerce")
    tr["taker_side"] = tr["ask"]
    tr["size"] = pd.to_numeric(tr["bid_sz"], errors="coerce")
    tr = tr.dropna(subset=["price", "size"])
    return tob, tr


def build_rows(day, tob, tr, pm_for_kalshi):
    rows = []
    tob_by = dict(tuple(tob.groupby(["venue", "market_id"], sort=False)))
    tr_by = dict(tuple(tr.groupby(["venue", "market_id"], sort=False)))
    for (venue, mkt), trades in tr_by.items():
        if venue != "kalshi":
            continue
        k = tob_by.get((venue, mkt))
        if k is None or len(k) < 10:
            continue
        kts, kmid = k["ts_local_ns"].values, k["mid"].values
        kbid, kask = k["bid"].values, k["ask"].values
        pm = None
        pm_ref = pm_for_kalshi.get(mkt)
        if pm_ref is not None:
            pm = tob_by.get(pm_ref)
        pts = pm["ts_local_ns"].values if pm is not None else None
        pmid = pm["mid"].values if pm is not None else None

        tts = trades["ts_local_ns"].values
        tpx = trades["price"].values
        tsz = trades["size"].values
        tside = trades["taker_side"].values
        szcum = np.concatenate(([0.0], np.cumsum(tsz)))
        for n in range(len(trades)):
            t = tts[n]
            j = np.searchsorted(kts, t, side="right") - 1
            if j < 5:
                continue
            side = tside[n] if tside[n] in ("buy", "sell") else (
                "buy" if tpx[n] >= kask[j] else "sell" if tpx[n] <= kbid[j] else None)
            if side is None:
                continue
            sign = 1.0 if side == "buy" else -1.0
            a = np.searchsorted(tts, t - int(60e9), side="left")
            a5 = np.searchsorted(tts, t - int(300e9), side="left")
            j5 = np.searchsorted(kts, t - int(300e9), side="left")
            realized_vol = float(np.abs(np.diff(kmid[max(j5, 1) - 1:j + 1])).sum())
            row = {"day": day, "market_id": mkt, "ts_ns": int(t),
                   "price": tpx[n], "size": tsz[n], "side": side,
                   "spread": kask[j] - kbid[j], "mid": kmid[j],
                   "dist_mid": (tpx[n] - kmid[j]) * sign,
                   "trades_60s": int(n - a), "vol_60s": float(szcum[n] - szcum[a]),
                   "trades_300s": int(n - a5), "vol_300s": float(szcum[n] - szcum[a5]),
                   "mid_vol_300s": realized_vol,
                   "hour": (t / 1e9 % 86400) / 3600.0}
            if pts is not None and len(pts):
                q = np.searchsorted(pts, t, side="right") - 1
                if q >= 0:
                    row["basis"] = pmid[q] - kmid[j]
                    row["aligned_basis"] = (pmid[q] - kmid[j]) * sign
                    row["pm_stale_s"] = (t - pts[q]) / 1e9
            for h in MARKOUTS_S:
                m = np.searchsorted(kts, t + int(h * 1e9), side="right") - 1
                mo = kmid[m] - kmid[j]
                # maker pnl: taker buy => maker sold at price
                row[f"maker_pnl_{h}s"] = (tpx[n] - kmid[m]) * sign
                row[f"markout_{h}s"] = mo * sign
            rows.append(row)
    return rows


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", nargs="+", required=True)
    ap.add_argument("--horizon", type=int, default=60)
    ap.add_argument("--research-dir", default="data/research")
    args = ap.parse_args()
    rdir = Path(args.research_dir)
    pairs = json.loads((rdir / "pairs.json").read_text())
    pm_for_kalshi = {}
    for p in pairs:
        pm_for_kalshi.setdefault(p["kalshi"], (p["pm_venue"], p["pm"]))

    rows = []
    for day in args.days:
        if not (rdir / f"tob-{day}.parquet").exists():
            continue
        tob, tr = load_day(rdir, day)
        rows += build_rows(day, tob, tr, pm_for_kalshi)
        print(f"{day}: cumulative fills {len(rows)}")
    d = pd.DataFrame(rows)
    if d.empty:
        print("no fills")
        return
    d.to_parquet(rdir / "fills.parquet", index=False)
    h = args.horizon
    print(f"\nfills: {len(d)}  (with basis: {d['aligned_basis'].notna().sum() if 'aligned_basis' in d else 0})")
    for hh in MARKOUTS_S:
        col = d[f"maker_pnl_{hh}s"]
        print(f"maker_pnl_{hh}s: mean={col.mean():.4f} median={col.median():.4f} "
              f"P(neg)={(col < 0).mean():.3f} size-wtd={np.average(col, weights=d['size']):.4f}")

    lbl = (d[f"maker_pnl_{h}s"] < 0).astype(int)
    feats = ["spread", "mid", "dist_mid", "size", "trades_60s", "vol_60s",
             "trades_300s", "vol_300s", "mid_vol_300s", "hour",
             "aligned_basis", "pm_stale_s"]
    feats = [f for f in feats if f in d.columns]
    X = d[feats].astype(float)
    if len(d) >= 300 and lbl.nunique() > 1:
        import lightgbm as lgb
        from sklearn.metrics import roc_auc_score
        from sklearn.model_selection import GroupKFold
        d["p_toxic"] = np.nan
        for trn, te in GroupKFold(n_splits=5).split(X, lbl, d["market_id"]):
            m = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05,
                                   num_leaves=15, min_child_samples=25, verbose=-1)
            m.fit(X.iloc[trn], lbl.iloc[trn])
            d.iloc[te, d.columns.get_loc("p_toxic")] = m.predict_proba(X.iloc[te])[:, 1]
        print(f"\ngrouped-CV AUC (P(maker_pnl_{h}s<0)): "
              f"{roc_auc_score(lbl, d['p_toxic']):.3f}")
        d["decile"] = pd.qcut(d["p_toxic"], 5, labels=False, duplicates="drop")
        print(d.groupby("decile").agg(
            n=("p_toxic", "size"),
            maker_pnl=(f"maker_pnl_{h}s", "mean"),
            pnl_600s=("maker_pnl_600s", "mean")).round(4))
        m_all = lgb.LGBMClassifier(n_estimators=200, learning_rate=0.05,
                                   num_leaves=15, min_child_samples=25, verbose=-1)
        m_all.fit(X, lbl)
        print("importance:", sorted(zip(feats, m_all.feature_importances_),
                                    key=lambda t: -t[1])[:8])
        m_all.booster_.save_model(str(rdir / "toxicity_model.txt"))


if __name__ == "__main__":
    main()
