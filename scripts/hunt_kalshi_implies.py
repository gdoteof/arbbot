"""Hunt maker-viable Kalshi implies ladders across the FULL catalog.

Scalar threshold events (strike_type greater/less) decompose into a monotone
implies ladder: P(>=X) is decreasing in X, so adjacent rungs (lo<hi) satisfy
(>=hi) implies (>=lo). Riskless structural spread on an adjacent pair, using
live catalog top-of-book:  edge = yes_bid(hi) - yes_ask(lo)   (sell the
less-likely rung, buy the more-likely rung; payoff differential >= 0 always).
Where this is near/above zero WITH liquidity, a maker resting inside the spread
has room. We report the best adjacent-pair edge per event, ranked.

Read-only, auth-free catalog. Caps pagination so it finishes quickly.
"""

import time

import httpx
from arbbot.record.kalshi import REST_BASE

SCALAR = {"greater", "greater_or_equal", "less", "less_or_equal"}
MAX_PAGES = 120
MIN_LIQ = 50.0  # dollars of resting liquidity on both legs to count


def get_page(c, params):
    """GET with backoff on 429."""
    delay = 1.0
    for _ in range(6):
        r = c.get(f"{REST_BASE}/events", params=params)
        if r.status_code == 429:
            time.sleep(delay)
            delay = min(delay * 2, 10)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()


def strike(m):
    for k in ("cap_strike", "floor_strike"):
        v = m.get(k)
        if v is not None:
            return float(v)
    return None


def f(m, k):
    v = m.get(k)
    return float(v) if v not in (None, "") else None


def main():
    c = httpx.Client(timeout=25)
    cursor = None
    events = []
    for _ in range(MAX_PAGES):
        params = {"with_nested_markets": "true", "status": "open", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        j = get_page(c, params)
        events.extend(j.get("events", []))
        cursor = j.get("cursor")
        if not cursor:
            break
        time.sleep(0.4)

    ladder_events = 0
    results = []
    for e in events:
        mkts = [m for m in e.get("markets", [])
                if m.get("strike_type") in SCALAR and strike(m) is not None
                and m.get("status") in ("active", "open")]
        if len(mkts) < 2:
            continue
        ladder_events += 1
        # order by strike; P(yes) should be monotone decreasing for 'greater*'
        mkts.sort(key=strike)
        greater = mkts[0].get("strike_type", "").startswith("greater")
        def twosided(m):
            b, a = f(m, "yes_bid_dollars"), f(m, "yes_ask_dollars")
            return b and a and 0 < b < a < 1  # real bid AND ask, not empty
        best = None
        for lo, hi in zip(mkts, mkts[1:]):
            # for 'greater', lower strike = MORE likely; for 'less', reverse
            more, less = (lo, hi) if greater else (hi, lo)
            if not (twosided(more) and twosided(less)):
                continue
            bid_less = f(less, "yes_bid_dollars")
            ask_more = f(more, "yes_ask_dollars")
            # tradeable size in $: sell less at its bid, buy more at its ask
            liq = min((f(less, "yes_bid_size_fp") or 0) * bid_less,
                      (f(more, "yes_ask_size_fp") or 0) * ask_more)
            edge = bid_less - ask_more
            if best is None or edge > best["edge"]:
                best = {"edge": edge, "liq": liq,
                        "more": more["ticker"], "ask_more": ask_more,
                        "less": less["ticker"], "bid_less": bid_less}
        if best:
            results.append({"event": e["event_ticker"], "cat": e.get("category"),
                            "title": (e.get("title") or "")[:50],
                            "rungs": len(mkts), **best})

    results.sort(key=lambda x: -x["edge"])
    pos = [x for x in results if x["edge"] > 0]
    posliq = [x for x in pos if x["liq"] >= MIN_LIQ]
    import statistics
    liqs = [x["liq"] for x in results]
    print(f"scanned {len(events)} open events; {ladder_events} scalar-ladder events\n"
          f"adjacent-pair edge: {len(pos)} positive, {len(posliq)} positive w/ >=${MIN_LIQ:.0f} liq\n"
          f"liquidity_dollars across candidates: max={max(liqs):.0f} "
          f"median={statistics.median(liqs):.1f} nonzero={sum(1 for l in liqs if l>0)}\n")
    print(f"{'edge':>7} {'liq$':>8}  event / best adjacent pair")
    for x in results[:30]:
        print(f"{x['edge']*100:>6.2f}c {x['liq']:>8.0f}  {x['event']} [{x['cat']}] {x['title']}")
        print(f"{'':>17}buy {x['more']}@{x['ask_more']:.3f}  sell {x['less']}@{x['bid_less']:.3f}")


if __name__ == "__main__":
    main()
