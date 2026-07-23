"""Cross-venue efficiency scan over the generic sports equivalence map.

Reads data/scan/sports_equiv_map.json (sports_equiv_discovery.py), fetches
live top-of-book on both venues for every matched TWO-WAY game (tennis,
baseball — soccer's draw leg needs 3-way handling, reported separately), and
measures where the inefficiency lives:

  per game: kalshi spread, PM spread, and the two take-take edges net of ~2c
  fees (K-cheap direction and PM-cheap direction).

Output: per-league spread/edge stats + any live crossings. Read-only.
"""

import json
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

import httpx

from arbbot.record.kalshi import REST_BASE

MAP = Path("data/scan/sports_equiv_map.json")
OUT = Path("data/scan/sports_equiv_edges.json")
FEE = Decimal("0.02")
TWO_WAY = {"itfme", "itfwo", "wta", "atp", "kbo", "npb", "mlb"}


def norm(s):
    return "".join(ch for ch in (s or "").lower() if ch.isalnum())


def team_match(a, b):
    ta, tb = set(norm(x) for x in (a or "").split()), set(norm(x) for x in (b or "").split())
    ta.discard(""); tb.discard("")
    return bool(ta and tb) and (ta <= tb or tb <= ta)


def main():
    doc = json.loads(MAP.read_text())
    games = [m for m in doc["matches"] if m["league"] in TWO_WAY]
    c = httpx.Client(timeout=20)

    # batch kalshi quotes
    tickers = sorted({mk["ticker"] for g in games for mk in g["kalshi_markets"]})
    kq = {}
    for i in range(0, len(tickers), 50):
        r = c.get(f"{REST_BASE}/markets", params={"tickers": ",".join(tickers[i:i+50])}).json()
        for m in r.get("markets", []):
            b, a = m.get("yes_bid_dollars"), m.get("yes_ask_dollars")
            kq[m["ticker"]] = (Decimal(str(b)) if b else None, Decimal(str(a)) if a else None)

    rows, stats = [], defaultdict(lambda: {"n": 0, "k_spread": [], "p_spread": [],
                                           "edges": [], "crossed": 0})
    for g in games:
        try:
            j = c.get("https://gateway.polymarket.us/v1/events",
                      params={"slug": g["pm_slug"]}).json()
            ev = (j.get("events") or [{}])[0]
            mkt = next((m for m in ev.get("markets") or []
                        if (m.get("slug") or "").startswith("aec-")
                        and m.get("marketType") == "moneyline"), None)
            if mkt is None:
                continue
            ml = mkt["slug"]
            sides = mkt.get("marketSides") or []
            long_team = next(((s.get("team") or {}).get("name") or s.get("description")
                              for s in sides if s.get("long")), None)
            b = c.get(f"https://gateway.polymarket.us/v1/markets/{ml}/bbo").json().get("marketData", {})
            p_bid = Decimal(b["bestBid"]["value"]) if (b.get("bestBid") or {}).get("value") else None
            p_ask = Decimal(b["bestAsk"]["value"]) if (b.get("bestAsk") or {}).get("value") else None
        except Exception:
            continue
        # kalshi market for the PM long team
        kt = next((mk["ticker"] for mk in g["kalshi_markets"]
                   if team_match(mk.get("team") or "", long_team or "")), None)
        if kt is None or kt not in kq:
            continue
        k_bid, k_ask = kq[kt]
        if None in (p_bid, p_ask, k_bid, k_ask):
            continue
        edge_k = float(p_bid - k_ask - FEE)   # buy K YES + PM NO
        edge_p = float(k_bid - p_ask - FEE)   # buy PM YES + K NO
        st = stats[g["league"]]
        st["n"] += 1
        st["k_spread"].append(float(k_ask - k_bid))
        st["p_spread"].append(float(p_ask - p_bid))
        best = max(edge_k, edge_p)
        st["edges"].append(best)
        if best > 0:
            st["crossed"] += 1
        rows.append({"league": g["league"], "teams": g["teams"], "date": g["date"],
                     "kalshi": kt, "pm": ml,
                     "k_bid": float(k_bid), "k_ask": float(k_ask),
                     "p_bid": float(p_bid), "p_ask": float(p_ask),
                     "edge_net": round(best, 4)})
        time.sleep(0.05)

    print(f"{'league':8s} {'games':>5s} {'k_spread':>9s} {'pm_spread':>9s} "
          f"{'best_edge':>9s} {'crossed':>7s}")
    summary = {}
    for lg, st in sorted(stats.items()):
        if not st["n"]:
            continue
        avg = lambda x: sum(x) / len(x)
        summary[lg] = {"games": st["n"], "avg_k_spread": round(avg(st["k_spread"]), 3),
                       "avg_pm_spread": round(avg(st["p_spread"]), 3),
                       "avg_best_edge_net": round(avg(st["edges"]), 3),
                       "max_edge_net": round(max(st["edges"]), 3),
                       "crossed_now": st["crossed"]}
        s = summary[lg]
        print(f"{lg:8s} {s['games']:5d} {s['avg_k_spread']:9.3f} {s['avg_pm_spread']:9.3f} "
              f"{s['avg_best_edge_net']:9.3f} {s['crossed_now']:7d}")
    rows.sort(key=lambda r: -r["edge_net"])
    print("\ntop 10 by live net edge:")
    for r in rows[:10]:
        print(f"  {r['league']:7s} {r['teams'][:38]:38s} edge {r['edge_net']*100:+.1f}c "
              f"(K {r['k_bid']}/{r['k_ask']} PM {r['p_bid']}/{r['p_ask']})")
    OUT.write_text(json.dumps({"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                               "summary": summary, "rows": rows}, indent=1))
    print(f"\nwrote {OUT}")


if __name__ == "__main__":
    main()
