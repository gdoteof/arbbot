"""Toxgate enforcement evidence: join shadow gate scores against the runner's
actual maker fills — were the high-gate moments the losing fills?

Reads data/scan/toxgate-shadow.jsonl (30s samples per market) and the ledger's
make-take fills; for each fill, finds the nearest shadow sample (<=120s) for
the filled Kalshi market and buckets fill profitability by gate score.

    .venv-research/bin/python scripts/toxgate_evidence.py
"""

import json
from pathlib import Path

import numpy as np


def main() -> None:
    shadow = []
    p = Path("data/scan/toxgate-shadow.jsonl")
    if p.exists():
        for line in p.read_text().splitlines():
            try:
                shadow.append(json.loads(line))
            except ValueError:
                pass
    if not shadow:
        print("no shadow samples yet")
        return
    by_ticker = {}
    for s in shadow:
        by_ticker.setdefault(s["ticker"], []).append((s["ts"], s))
    for v in by_ticker.values():
        v.sort()

    fills = []
    for line in Path("data/exec/trades.jsonl").read_text().splitlines():
        try:
            d = json.loads(line)
        except ValueError:
            continue
        if d.get("strategy") != "make-take" or d.get("status") != "open":
            continue
        kleg = next((l for l in d.get("legs", []) if l.get("venue") == "kalshi"
                     and l.get("role") == "maker"), None)
        if not kleg:
            continue
        fills.append((d["ts"], kleg["market_id"],
                      float(d.get("profit_usd") or 0),
                      float(d.get("qty") or 0)))
    joined = []
    for ts, kt, profit, qty in fills:
        samples = by_ticker.get(kt)
        if not samples:
            continue
        tss = [x[0] for x in samples]
        i = int(np.searchsorted(tss, ts)) - 1
        if i < 0 or ts - tss[i] > 120:
            continue
        s = samples[i][1]
        joined.append((max(s.get("bid", 0), s.get("ask", 0)), profit, qty))
    print(f"shadow samples: {len(shadow)}  make-take fills: {len(fills)}  "
          f"joined (gate<=120s before fill): {len(joined)}")
    if len(joined) < 10:
        print("not enough joined fills yet — keep accumulating")
        return
    arr = np.array(joined)
    for lo, hi in [(0, 0.1), (0.1, 0.3), (0.3, 1.01)]:
        sel = arr[(arr[:, 0] >= lo) & (arr[:, 0] < hi)]
        if len(sel):
            print(f"  gate [{lo},{hi}): n={len(sel):4d}  "
                  f"mean locked/ct={np.average(sel[:,1]/np.maximum(sel[:,2],1)):+.4f}")
    print("enforcement case: high-gate bucket should show materially worse "
          "locked profit; flip quoter flag only when it does.")


if __name__ == "__main__":
    main()
