"""Train the sports lead-lag model (direction k->pm: trade PM-US on Kalshi
jumps). Feature order and league encoding EXACTLY match scripts/leadlag_probe.py
(league -> index in the sorted league list from the sports map).

Label: taker PnL at --horizon net of PM-US taker fee (0.06*p*(1-p)) > 0,
entry at the follower quote 500ms post-event (entry_px from the study).

Validation: (a) grouped 5-fold CV by pair, (b) time-holdout on the last 30%%
of events. Prints threshold economics for both; saves the booster trained on
ALL data to data/research/leadlag_model_sports.txt only when --save.

    .venv-research/bin/python scripts/train_sports_model.py --horizon 120 [--save]
"""

import argparse
import json
from pathlib import Path

import lightgbm as lgb
import numpy as np
import pandas as pd
from sklearn.metrics import roc_auc_score
from sklearn.model_selection import GroupKFold

FEATURES = ["abs_jump", "f_spread", "f_stale_s", "aligned_basis", "leader_mid",
            "f_mid", "hour", "pair_event_no",
            "l_spread", "l_trades_60s", "l_trade_vol_60s", "league"]
MODEL_OUT = Path("data/research/leadlag_model_sports.txt")


def league_codes() -> list[str]:
    smap = json.loads(Path("data/scan/sports_equiv_map.json").read_text())
    leagues = {m["league"] for m in smap.get("matches", [])
               if m.get("kalshi_long_ticker") and m.get("pm_moneyline")}
    return sorted(leagues)


def econ(d, col_p, thresholds):
    for q in thresholds:
        sel = d[d[col_p] >= q]
        if len(sel) == 0:
            print(f"  p>={q}: n=0")
            continue
        print(f"  p>={q}: n={len(sel):4d}  mean net={sel['net_pnl'].mean():+.4f}  "
              f"win={sel['label'].mean():.3f}  sum={sel['net_pnl'].sum():+.3f}  "
              f"pairs={sel['pair_id'].nunique()}")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--horizon", type=int, default=120)
    ap.add_argument("--events", default="data/research/sports_events.parquet")
    ap.add_argument("--label", default="follower", choices=["follower", "leader"],
                    help="PnL vs follower mid (exit-able) or leader mid "
                         "(hold-to-resolution truth proxy)")
    ap.add_argument("--save", action="store_true")
    args = ap.parse_args()

    leagues = league_codes()
    ev = pd.read_parquet(args.events)
    d = ev[(ev["direction"] == "k->pm") & (ev["family"] == "sports")].copy()
    pnl_col = (f"taker_pnl_{args.horizon}s" if args.label == "follower"
               else f"leader_pnl_{args.horizon}s")
    if pnl_col not in d.columns:
        raise SystemExit(f"{pnl_col} not in events — rerun leadlag_study.py")
    d = d.dropna(subset=[pnl_col, "entry_px"])
    fee = 0.06 * d["entry_px"] * (1 - d["entry_px"])
    d["net_pnl"] = d[pnl_col] - fee
    d["label"] = (d["net_pnl"] > 0).astype(int)
    d["abs_jump"] = d["dmid"].abs()
    d["aligned_basis"] = d["basis"] * np.sign(d["dmid"])
    d["hour"] = ((d["ts_ns"] / 1e9) % 86400) / 3600.0
    d = d.sort_values("ts_ns").reset_index(drop=True)
    d["pair_event_no"] = d.groupby(["day", "pair_id"]).cumcount()
    lg = d["pair_id"].str.extract(r"^sports-([a-z0-9]+)-")[0]
    d["league"] = lg.map(lambda x: leagues.index(x) if x in leagues else -1)

    print(f"events: {len(d)}  pairs: {d['pair_id'].nunique()}  "
          f"base P(win): {d['label'].mean():.3f}  "
          f"take-all mean net: {d['net_pnl'].mean():+.4f}")
    if len(d) < 300:
        print("NOT ENOUGH DATA (<300) — no model saved")
        return

    X, y = d[FEATURES].astype(float), d["label"]
    params = dict(n_estimators=250, learning_rate=0.05, num_leaves=15,
                  min_child_samples=25, verbose=-1)

    # (a) grouped CV by pair
    d["p_cv"] = np.nan
    for trn, te in GroupKFold(n_splits=5).split(X, y, d["pair_id"]):
        m = lgb.LGBMClassifier(**params)
        m.fit(X.iloc[trn], y.iloc[trn])
        d.iloc[te, d.columns.get_loc("p_cv")] = m.predict_proba(X.iloc[te])[:, 1]
    print(f"\n[grouped CV by pair] AUC={roc_auc_score(y, d['p_cv']):.3f}")
    econ(d, "p_cv", [0.5, 0.6, 0.65, 0.7, 0.8])

    sel = d[d["p_cv"] >= 0.65]
    if len(sel):
        print("\nper-pair @ p_cv>=0.65 (guard vs one-hot-match artifact):")
        pp = sel.groupby("pair_id")["net_pnl"].agg(["size", "mean", "sum"]).round(4)
        print(pp.sort_values("sum", ascending=False).to_string(max_rows=20))

    # (b) time holdout: last 30% of events
    cut = int(len(d) * 0.7)
    tr, te = d.iloc[:cut], d.iloc[cut:].copy()
    if te["label"].nunique() > 1:
        m = lgb.LGBMClassifier(**params)
        m.fit(tr[FEATURES].astype(float), tr["label"])
        te["p_time"] = m.predict_proba(te[FEATURES].astype(float))[:, 1]
        print(f"\n[time holdout last 30%] n={len(te)} "
              f"AUC={roc_auc_score(te['label'], te['p_time']):.3f}")
        econ(te, "p_time", [0.5, 0.6, 0.65, 0.7, 0.8])

    if args.save:
        m_all = lgb.LGBMClassifier(**params)
        m_all.fit(X, y)
        MODEL_OUT.parent.mkdir(exist_ok=True)
        m_all.booster_.save_model(str(MODEL_OUT))
        print(f"\nsaved {MODEL_OUT} (leagues: {leagues})")


if __name__ == "__main__":
    main()
