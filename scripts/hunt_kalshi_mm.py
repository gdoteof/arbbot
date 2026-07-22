"""Is Kalshi market-makeable? Scan the full open catalog for markets with a
capturable spread AND real two-sided flow. Spread capture must clear the maker
fee (~1c/contract each side after ceil, so ~2c round-trip) plus an adverse-
selection buffer, so we want spread >= ~3c with meaningful 24h volume.

Reports the volume-weighted opportunity by category and the top individual
markets, so we can judge whether MM is plausible before building a backtester.
"""

import time
import collections

import httpx
from arbbot.record.kalshi import REST_BASE

MAX_PAGES = 120
MIN_SPREAD_C = 3.0     # cents of spread to bother quoting
MIN_VOL24 = 100.0      # contracts traded in last 24h


def get_page(c, params):
    delay = 1.0
    for _ in range(6):
        r = c.get(f"{REST_BASE}/events", params=params)
        if r.status_code == 429:
            time.sleep(delay); delay = min(delay * 2, 10); continue
        r.raise_for_status(); return r.json()
    r.raise_for_status()


def f(m, k):
    v = m.get(k)
    try: return float(v) if v not in (None, "") else None
    except (TypeError, ValueError): return None


def main():
    c = httpx.Client(timeout=25)
    cursor = None; mkts = []
    for _ in range(MAX_PAGES):
        params = {"with_nested_markets": "true", "status": "open", "limit": 100}
        if cursor: params["cursor"] = cursor
        j = get_page(c, params)
        for e in j.get("events", []):
            for m in e.get("markets", []):
                m["_cat"] = e.get("category")
                mkts.append(m)
        cursor = j.get("cursor")
        if not cursor: break
        time.sleep(0.4)

    cand = []
    for m in mkts:
        if m.get("status") not in ("active", "open"): continue
        b, a = f(m, "yes_bid_dollars"), f(m, "yes_ask_dollars")
        v24 = f(m, "volume_24h_fp") or 0
        if not (b and a and 0 < b < a < 1): continue
        spread_c = (a - b) * 100
        if spread_c < MIN_SPREAD_C or v24 < MIN_VOL24: continue
        mid = (a + b) / 2
        # net edge per round-trip if we capture the spread (both sides fill),
        # minus ~2c fees; adverse selection not modeled here (needs backtest)
        net_c = spread_c - 2.0
        cand.append({"t": m["ticker"], "cat": m.get("_cat"), "spread_c": spread_c,
                     "net_c": net_c, "v24": v24, "mid": mid})

    cand.sort(key=lambda x: -x["v24"])
    bycat = collections.Counter()
    catvol = collections.Counter()
    for x in cand:
        bycat[x["cat"]] += 1; catvol[x["cat"]] += x["v24"]
    print(f"scanned {len(mkts)} markets; {len(cand)} with spread>={MIN_SPREAD_C:.0f}c "
          f"AND 24h vol>={MIN_VOL24:.0f}\n")
    print("by category (count / total 24h vol):")
    for cat, n in bycat.most_common():
        print(f"  {cat}: {n} markets, {catvol[cat]:,.0f} contracts/24h")
    print(f"\ntop 25 by 24h volume (spread net of ~2c fees):")
    print(f"{'vol24':>9} {'spread':>7} {'net':>6} {'mid':>5}  ticker [cat]")
    for x in cand[:25]:
        print(f"{x['v24']:>9,.0f} {x['spread_c']:>6.1f}c {x['net_c']:>5.1f}c "
              f"{x['mid']:>5.2f}  {x['t']} [{x['cat']}]")


if __name__ == "__main__":
    main()
