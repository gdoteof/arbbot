"""Capacity backtest: how deep into the book could take-take have gone, and
how much bankroll can the strategy absorb?

Reads the scanner's opportunity tape (archived parquet + today's JSONL),
restricted to LIVE-tradeable pairs (xvus-*, bucket=primary). Each tick row is
already an honest fill simulation: VWAP-walked edge-positive depth net of both
venues' fees at that instant.

Method:
  * EPISODES: ticks for a relationship with gaps > 120s split into episodes —
    one standing dislocation each. Naively summing ticks would overcount the
    same resting arb thousands of times.
  * CONSERVATIVE capture: one take per episode at its best net_edge_total
    moment (deep resting liquidity is taken ONCE — it does not refill).
  * FLOW-BOUNDED capture: conservative PLUS edge captured on real replenishment,
    bounded by contracts that ACTUALLY TRADED on the Kalshi leg during the
    episode (venue trade tape) — one extra take per 15-min window, sized by
    that window's traded volume. No imaginary refills.
  * BANKROLL CURVE: chronological greedy. Per-relationship cap SCALES with
    bankroll at the live ratio (15% of bankroll ~= $150 at our $980), because
    a $25k book would not keep a $150 cap. Capital locked once used (no
    in-window recycle; resolutions are months out).
  * ROC = profit / capital used. APR annualizes ROC over the capital-weighted
    time from take to each relationship's resolution date (resolve_dates).

Writes data/reports/capacity.json for the dashboard Capacity tab. Refreshed
daily 07:40 by arbbot-capacity.timer (after the parquet ETL); run manually or
`systemctl --user start arbbot-capacity` to refresh now.
"""

import datetime as dt
import json
import time
from collections import defaultdict
from pathlib import Path

import duckdb

from arbbot.exec.resolve_dates import resolve_date
from arbbot.models.core import Venue
from arbbot.registry.model import Registry

OUT = Path("data/reports/capacity.json")
GAP_S = 120           # episode split on quiet gaps
FLOW_WINDOW_S = 900   # one flow-bounded re-take per 15 min inside an episode
BANKROLLS = [100, 250, 500, 980, 2000, 5000, 10000, 25000]
PER_REL_FRAC = 0.15   # live ratio: $150 cap on the $980 bankroll


def load_ticks(con) -> list[tuple]:
    srcs = []
    for p in sorted(Path("data/parquet").glob("opportunities-*.parquet")):
        srcs.append(f"SELECT * FROM '{p}'")
    for p in sorted(Path("data/scan").glob("opportunities-*.jsonl")):
        srcs.append(
            f"SELECT relationship_id, tranche, signature, size, gross_cost, fees, "
            f"min_payoff, net_edge_total, net_edge_per_contract, bucket, ts_local_ns "
            f"FROM read_json_auto('{p}', format='newline_delimited')")
    q = " UNION ALL BY NAME ".join(f"({s})" for s in srcs)
    return con.execute(f"""
        SELECT relationship_id, ts_local_ns / 1e9 AS ts,
               CAST(size AS DOUBLE) AS size,
               CAST(gross_cost AS DOUBLE) AS cost,
               CAST(net_edge_total AS DOUBLE) AS net_total,
               CAST(net_edge_per_contract AS DOUBLE) AS edge_pc
        FROM ({q})
        WHERE relationship_id LIKE 'xvus-%' AND bucket = 'primary'
          AND CAST(net_edge_total AS DOUBLE) > 0
        ORDER BY ts
    """).fetchall()


def load_kalshi_trades(con) -> dict[str, list[tuple]]:
    """market_id -> sorted [(ts_s, contracts)] from parquet + today's raw tape."""
    srcs = [f"SELECT market_id, ts_local_ns, size FROM '{p}' WHERE kind='trade'"
            for p in sorted(Path("data/parquet").glob("kalshi-*.parquet"))]
    for p in sorted(Path("data/raw").glob("kalshi-*.jsonl")):
        srcs.append(f"SELECT market_id, ts_local_ns, size FROM read_json_auto('{p}', "
                    f"format='newline_delimited') WHERE kind='trade'")
    q = " UNION ALL BY NAME ".join(f"({s})" for s in srcs)
    out = defaultdict(list)
    for mid, ts_ns, size in con.execute(
            f"SELECT market_id, ts_local_ns, CAST(size AS DOUBLE) FROM ({q}) "
            f"ORDER BY ts_local_ns").fetchall():
        out[mid].append((ts_ns / 1e9, size))
    return out


def rel_kalshi_ticker() -> dict[str, str]:
    reg = Registry.load("config/registry.yaml")
    return {r.id: next((l.market_id for l in r.legs if l.venue is Venue.KALSHI), None)
            for r in reg.relationships}


def build_episodes(rows, trades, tickers):
    by_rel = defaultdict(list)
    for rel, ts, size, cost, net, pc in rows:
        by_rel[rel].append((ts, size, cost, net, pc))
    episodes = []
    for rel, ticks in by_rel.items():
        cur = None
        for t in ticks:
            if cur is None or t[0] - cur["end"] > GAP_S:
                if cur:
                    episodes.append(cur)
                cur = {"rel": rel, "start": t[0], "end": t[0], "ticks": []}
            cur["end"] = t[0]
            cur["ticks"].append(t)
        if cur:
            episodes.append(cur)
    for ep in episodes:
        best = max(ep["ticks"], key=lambda t: t[3])
        ep["peak_net"], ep["peak_size"] = best[3], best[1]
        ep["peak_cost"], ep["peak_edge_pc"] = best[2], best[4]
        ep["duration_s"] = ep["end"] - ep["start"]
        cost_pc = best[2] / best[1] if best[1] else 1.0
        # flow-bounded extras: per 15-min window, edge captured on contracts
        # that actually traded on the Kalshi leg (excluding the peak window's
        # first take, which consumes standing depth)
        tk = tickers.get(ep["rel"])
        flow = [t for t in trades.get(tk, []) if ep["start"] <= t[0] <= ep["end"]] if tk else []
        wtraded = defaultdict(float)
        for ts_t, sz in flow:
            wtraded[int((ts_t - ep["start"]) // FLOW_WINDOW_S)] += sz
        peak_w = int((best[0] - ep["start"]) // FLOW_WINDOW_S)
        takes = [(best[0], ep["rel"], ep["peak_net"], ep["peak_cost"])]
        flow_net = ep["peak_net"]
        for w, traded in sorted(wtraded.items()):
            if w == peak_w or traded < 1:
                continue
            # edge available that window (best tick in window), sized by traded flow
            wt = [t for t in ep["ticks"]
                  if w == int((t[0] - ep["start"]) // FLOW_WINDOW_S)]
            if not wt:
                continue
            wbest = max(wt, key=lambda t: t[4])
            qty = min(traded, wbest[1])
            net = wbest[4] * qty
            takes.append((ep["start"] + w * FLOW_WINDOW_S, ep["rel"], net, cost_pc * qty))
            flow_net += net
        ep["flow_net"] = flow_net
        ep["flow_traded"] = sum(wtraded.values())
        ep["takes"] = takes
        del ep["ticks"]
    episodes.sort(key=lambda e: e["start"])
    return episodes


def _years_to_resolution(rel_id: str, from_ts: float) -> float | None:
    d, _est = resolve_date(rel_id)
    if not d:
        return None
    try:
        rd = dt.date.fromisoformat(d)
    except ValueError:
        return None
    return max((rd - dt.date.fromtimestamp(from_ts)).days, 1) / 365.25


def bankroll_curve(episodes):
    takes = sorted((t for ep in episodes for t in ep["takes"]), key=lambda x: x[0])
    out = []
    for floor_pc in (0.005, 0.02, 0.05):
        curve = []
        for k in BANKROLLS:
            cap = float(k)
            per_rel_cap = max(150.0, k * PER_REL_FRAC)
            rel_used = defaultdict(float)
            profit = used = n = 0.0
            yr_num = yr_den = 0.0  # capital-weighted years to resolution
            for ts, rel, net, cost in takes:
                if cost <= 0 or net <= 0:
                    continue
                if net / cost < floor_pc:
                    continue
                headroom = min(cap, per_rel_cap - rel_used[rel])
                if headroom <= 1:
                    continue
                frac = min(1.0, headroom / cost)
                profit += net * frac
                used += cost * frac
                cap -= cost * frac
                rel_used[rel] += cost * frac
                n += 1
                yrs = _years_to_resolution(rel, ts)
                if yrs:
                    yr_num += cost * frac * yrs
                    yr_den += cost * frac
            roc = profit / used * 100 if used else 0.0
            wavg_yr = yr_num / yr_den if yr_den else None
            apr = roc / wavg_yr if wavg_yr else None
            curve.append({"bankroll": k, "per_rel_cap": round(per_rel_cap),
                          "profit": round(profit, 2), "capital_used": round(used, 2),
                          "takes": int(n), "roc_pct": round(roc, 2),
                          "apr_pct": round(apr, 1) if apr is not None else None,
                          "wavg_years": round(wavg_yr, 2) if wavg_yr else None})
        out.append({"edge_floor_pc": floor_pc, "curve": curve})
    return out


def main():
    t0 = time.time()
    con = duckdb.connect()
    rows = load_ticks(con)
    if not rows:
        print("no opportunity data")
        return
    trades = load_kalshi_trades(con)
    tickers = rel_kalshi_ticker()
    days = sorted({time.strftime("%Y-%m-%d", time.gmtime(r[1])) for r in rows})
    episodes = build_episodes(rows, trades, tickers)

    by_rel = defaultdict(lambda: {"episodes": 0, "conservative": 0.0, "flow_bounded": 0.0,
                                  "best_edge_pc": 0.0, "max_depth": 0.0,
                                  "cost_at_peak": 0.0, "traded": 0.0})
    daily = defaultdict(lambda: {"conservative": 0.0, "flow_bounded": 0.0, "episodes": 0})
    for ep in episodes:
        r = by_rel[ep["rel"]]
        r["episodes"] += 1
        r["conservative"] += ep["peak_net"]
        r["flow_bounded"] += ep["flow_net"]
        r["best_edge_pc"] = max(r["best_edge_pc"], ep["peak_edge_pc"])
        r["max_depth"] = max(r["max_depth"], ep["peak_size"])
        r["cost_at_peak"] = max(r["cost_at_peak"], ep["peak_cost"])
        r["traded"] += ep["flow_traded"]
        d = daily[time.strftime("%Y-%m-%d", time.gmtime(ep["start"]))]
        d["conservative"] += ep["peak_net"]
        d["flow_bounded"] += ep["flow_net"]
        d["episodes"] += 1

    doc = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "window_days": days,
        "n_episodes": len(episodes),
        "mechanics": {
            "data": "scanner opportunity tape (VWAP-walked edge-positive depth, "
                    "net of both venues' fees) + Kalshi venue trade tape",
            "refresh": "daily 07:40 via arbbot-capacity.timer (after parquet ETL); "
                       "manual: systemctl --user start arbbot-capacity",
            "universe": "xvus-* live-tradeable pairs, bucket=primary, taker-only",
            "episode_gap_s": GAP_S, "flow_window_s": FLOW_WINDOW_S,
            "per_rel_cap": f"max($150, {PER_REL_FRAC:.0%} of bankroll) — scales with bankroll",
            "notes": ["conservative = one take per episode at its best moment; "
                      "standing depth counted ONCE",
                      "flow-bounded = conservative + edge on contracts that ACTUALLY "
                      "traded (Kalshi leg) per 15-min window — no imaginary refills",
                      "bankroll curve: chronological greedy, capital locked once used",
                      "ROC = profit/capital used; APR = ROC annualized over "
                      "capital-weighted time to resolution",
                      "maker (make-take) capacity NOT included — taker-only floor"],
        },
        "daily": [{"day": d, **{k: round(v, 2) if isinstance(v, float) else v
                                for k, v in vals.items()}}
                  for d, vals in sorted(daily.items())],
        "per_relationship": sorted(
            [{"rel": r, "episodes": v["episodes"],
              "conservative": round(v["conservative"], 2),
              "flow_bounded": round(v["flow_bounded"], 2),
              "best_edge_pc": round(v["best_edge_pc"], 4),
              "max_depth_contracts": int(v["max_depth"]),
              "max_single_take_usd": round(v["cost_at_peak"], 2),
              "traded_contracts": int(v["traded"])}
             for r, v in by_rel.items()],
            key=lambda x: -x["flow_bounded"]),
        "bankroll_curve": bankroll_curve(episodes),
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(doc, indent=1))
    print(f"wrote {OUT} — {len(episodes)} episodes over {days} in {time.time()-t0:.1f}s")
    for fc in doc["bankroll_curve"]:
        print(f" edge floor {fc['edge_floor_pc']*100:.1f}c:")
        for row in fc["curve"]:
            print(f"   ${row['bankroll']:>6} (rel cap ${row['per_rel_cap']}): "
                  f"profit ${row['profit']:>7} used ${row['capital_used']:>8} "
                  f"roc {row['roc_pct']}% apr {row['apr_pct']}% ({row['takes']} takes)")


if __name__ == "__main__":
    main()
