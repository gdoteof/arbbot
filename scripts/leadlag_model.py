"""Train/evaluate a lead-lag taker model on event-study output.

Direction modeled: pm->k (PM jump is the signal; we would take stale Kalshi
quotes). Label: taker PnL at --horizon net of Kalshi taker fee, > 0.

Features are strictly time-t observable: jump size/sign, follower spread,
follower staleness, pre-jump basis, leader mid level, pair activity so far
that day, hour of day, league/family.

Validation: GroupKFold over pair_id (no game leaks across folds) plus a
day-holdout report when multiple days are present.

    .venv-research/bin/python scripts/leadlag_model.py --horizon 30
"""

import argparse
import json
import math
from pathlib import Path

import numpy as np
import pandas as pd
from sklearn.model_selection import GroupKFold


def kalshi_taker_fee(p: np.ndarray) -> np.ndarray:
    """Per-contract taker fee: ceil to cent of 0.07*p*(1-p)."""
    return np.ceil(7.0 * p * (1 - p)) / 100.0


def pmus_taker_fee(p: np.ndarray) -> np.ndarray:
    """Per-contract PM-US taker fee: 0.06*p*(1-p) (FeeSchedule fallback coef)."""
    return 0.06 * p * (1 - p)


def build_dataset(ev: pd.DataFrame, horizon: int, direction: str) -> pd.DataFrame:
    d = ev[ev["direction"] == direction].copy()
    d = d.dropna(subset=[f"taker_pnl_{horizon}s"])
    entry = d["entry_px"].values if "entry_px" in d else np.where(
        d["up"], d["f_ask"], d["f_bid"])
    # follower is what we trade: kalshi for pm->k, polymarket_us for k->pm
    fee = kalshi_taker_fee(entry) if direction == "pm->k" else pmus_taker_fee(entry)
    d["net_pnl"] = d[f"taker_pnl_{horizon}s"] - fee
    d["label"] = (d["net_pnl"] > 0).astype(int)
    d["abs_jump"] = d["dmid"].abs()
    d["hour"] = ((d["ts_ns"] / 1e9) % 86400) / 3600.0
    d["league"] = d["pair_id"].str.extract(r"^sports-([a-z0-9]+)-")[0].fillna("nonsports")
    d = d.sort_values("ts_ns")
    d["pair_event_no"] = d.groupby(["day", "pair_id"]).cumcount()
    d["aligned_basis"] = d["basis"] * np.sign(d["dmid"])
    return d


FEATURES = ["abs_jump", "f_spread", "f_stale_s", "aligned_basis", "leader_mid",
            "f_mid", "hour", "pair_event_no",
            "l_spread", "l_trades_60s", "l_trade_vol_60s"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=30)
    ap.add_argument("--events", default="data/research/events.parquet")
    ap.add_argument("--family", default=None, help="filter: sports|registry")
    ap.add_argument("--direction", default="pm->k", choices=["pm->k", "k->pm"])
    ap.add_argument("--model-out", default="data/research/leadlag_model.txt")
    args = ap.parse_args()

    ev = pd.read_parquet(args.events)
    if args.family:
        ev = ev[ev["family"] == args.family]
    d = build_dataset(ev, args.horizon, args.direction)
    print(f"events: {len(d)}  base rate P(net_pnl>0): {d['label'].mean():.3f}  "
          f"mean net pnl (take-all): {d['net_pnl'].mean():.4f}")
    if len(d) < 200:
        print("too few events to model; showing unconditional stats only")
        print(d.groupby("league")["net_pnl"].agg(["size", "mean", "median"]).round(4))
        return

    import lightgbm as lgb
    X, y, groups = d[FEATURES + ["league"]].copy(), d["label"], d["pair_id"]
    X["league"] = X["league"].astype("category")

    gkf = GroupKFold(n_splits=5)
    d["p_hat"] = np.nan
    for tr, te in gkf.split(X, y, groups):
        m = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                               num_leaves=15, min_child_samples=30,
                               verbose=-1)
        m.fit(X.iloc[tr], y.iloc[tr])
        d.iloc[te, d.columns.get_loc("p_hat")] = m.predict_proba(X.iloc[te])[:, 1]

    from sklearn.metrics import roc_auc_score
    print(f"grouped-CV AUC: {roc_auc_score(d['label'], d['p_hat']):.3f}")
    for q in [0.5, 0.6, 0.7, 0.8]:
        sel = d[d["p_hat"] >= q]
        if len(sel) == 0:
            continue
        print(f"  p_hat>={q}: n={len(sel)}  mean net pnl={sel['net_pnl'].mean():.4f}  "
              f"win rate={sel['label'].mean():.3f}  "
              f"total pnl/contract-event={sel['net_pnl'].sum():.2f}")

    # day holdout if we have multiple days
    days = sorted(d["day"].unique())
    if len(days) > 1:
        hold = days[-1]
        tr, te = d[d["day"] != hold], d[d["day"] == hold]
        if len(tr) > 100 and len(te) > 50:
            m = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                                   num_leaves=15, min_child_samples=30, verbose=-1)
            m.fit(tr[FEATURES + ["league"]].astype({"league": "category"}), tr["label"])
            ph = m.predict_proba(te[FEATURES + ["league"]].astype({"league": "category"}))[:, 1]
            te = te.assign(p_hat2=ph)
            print(f"day-holdout ({hold}): AUC="
                  f"{roc_auc_score(te['label'], ph):.3f}" if te["label"].nunique() > 1 else "degenerate")
            for q in [0.6, 0.7]:
                sel = te[te["p_hat2"] >= q]
                if len(sel):
                    print(f"  p_hat>={q}: n={len(sel)} mean net={sel['net_pnl'].mean():.4f} win={sel['label'].mean():.3f}")

    imp = None
    try:
        m_all = lgb.LGBMClassifier(n_estimators=300, learning_rate=0.05,
                                   num_leaves=15, min_child_samples=30, verbose=-1)
        m_all.fit(X, y)
        imp = sorted(zip(X.columns, m_all.feature_importances_),
                     key=lambda t: -t[1])
        print("feature importance:", imp)
        Path(args.model_out).parent.mkdir(exist_ok=True)
        m_all.booster_.save_model(args.model_out)
        print(f"saved {args.model_out}")
    except Exception as e:
        print("model save failed:", e)


if __name__ == "__main__":
    main()
