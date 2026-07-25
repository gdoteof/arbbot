#!/usr/bin/env python3
"""P3 shadow-trader gate: compare one UTC day of the Rust dry-run trader's
intent stream (data/trader-rs/intents.jsonl) against the live Python
trader's flushed intents (data/scan/trader-intents-*.jsonl).

NOT a byte gate (that's arb-trader --bench-tape / arb-intent): the live
Python trader runs with real order ids, live risk state, and its own feed
session, so streams can only agree at the DECISION level. We normalize both
sides to (action, venue, market, side, price) events and report, per side:
counts, and the fraction of events with a counterpart on the other side —
same action/market/side, price within --price-tol, time within --tol-s.
Disagreements are the investigation queue for the soak.

  python scripts/shadow_trader_gate.py --day 2026-07-24 \
      [--rs data/trader-rs/intents.jsonl] [--py-dir data/scan] \
      [--tol-s 30] [--price-tol 0.01]
"""

import argparse
import glob
import json
from datetime import datetime, timezone
from pathlib import Path


def day_bounds(day: str) -> tuple[float, float]:
    t0 = datetime.strptime(day, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp()
    return t0, t0 + 86400.0


def norm(rec: dict):
    """-> (ts, action, venue, market, side, price) or None."""
    for action in ("place", "cancel"):
        if action in rec:
            return (float(rec["ts"]), action, rec.get("venue", ""),
                    rec[action], rec.get("side", ""), float(rec.get("price") or 0))
    return None


def load(paths, lo, hi):
    out = []
    for p in paths:
        if not Path(p).exists():
            continue
        with open(p) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    ev = norm(json.loads(line))
                except (ValueError, KeyError):
                    continue
                if ev and lo <= ev[0] < hi:
                    out.append(ev)
    out.sort()
    return out


def match_rate(a, b, tol_s, price_tol):
    """Fraction of events in `a` with a counterpart in `b`."""
    if not a:
        return None, []
    used = set()
    misses = []
    hits = 0
    for ev in a:
        ts, action, venue, market, side, price = ev
        found = None
        for j, cand in enumerate(b):
            if j in used:
                continue
            cts, caction, cvenue, cmarket, cside, cprice = cand
            if cts > ts + tol_s:
                break
            if (caction == action and cvenue == venue and cmarket == market
                    and cside == side and abs(cts - ts) <= tol_s
                    and abs(cprice - price) <= price_tol + 1e-9):
                found = j
                break
        if found is None:
            misses.append(ev)
        else:
            used.add(found)
            hits += 1
    return hits / len(a), misses


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--day", required=True)
    ap.add_argument("--rs", default="data/trader-rs/intents.jsonl")
    ap.add_argument("--py-dir", default="data/scan")
    ap.add_argument("--tol-s", type=float, default=30.0)
    ap.add_argument("--price-tol", type=float, default=0.01)
    args = ap.parse_args()

    lo, hi = day_bounds(args.day)
    rs = load([args.rs], lo, hi)
    py = load(sorted(glob.glob(f"{args.py_dir}/trader-intents-*.jsonl")), lo, hi)

    res = {"day": args.day, "rs_intents": len(rs), "py_intents": len(py)}
    if not py:
        res["note"] = ("no Python trader intents this day (trader idle/off) — "
                       "soak-only day, no decision comparison possible")
    rs_rate, rs_miss = match_rate(rs, py, args.tol_s, args.price_tol)
    py_rate, py_miss = match_rate(py, rs, args.tol_s, args.price_tol)
    res["rs_matched_in_py"] = None if rs_rate is None else round(rs_rate, 4)
    res["py_matched_in_rs"] = None if py_rate is None else round(py_rate, 4)
    print(json.dumps(res))
    for tag, misses in (("RS-only", rs_miss if py else []), ("PY-only", py_miss)):
        for ev in misses[:40]:
            ts, action, venue, market, side, price = ev
            print(f"  {tag} {datetime.fromtimestamp(ts, timezone.utc):%H:%M:%S} "
                  f"{action} {venue} {market} {side} @ {price}")
        if len(misses) > 40:
            print(f"  {tag}: ... {len(misses) - 40} more")


if __name__ == "__main__":
    main()
