"""Settlement sweeper: convert settled baskets into realized profit.

For every open ledger basket (net of unwinds/corrections), check whether its
Kalshi leg's market has FINALIZED. A complete cross-venue basket pays exactly
$1/contract at resolution regardless of outcome (that's the construction), so
a finalized basket realizes payoff qty*$1 minus recorded cost. Appends a
closing record in the unwind convention ({status: "unwound", closes_ts}) so
marks / risk seeding / the dashboard all see the basket close and the profit
move from locked to realized. Sports baskets settle same-day — this is what
recycles their capital on the books.

Read-only on venues (no orders). Designed for an hourly timer.
"""

import json
import time

import httpx

from arbbot.exec import ledgerdb
from arbbot.exec.ledgerdb import dual_append
from arbbot.record.kalshi import REST_BASE


def main():
    conn = ledgerdb.connect()
    try:
        baskets = ledgerdb.open_baskets_db(conn)
    finally:
        conn.close()
    with_k = [(t, next((l for l in t.get("legs", []) if l.get("venue") == "kalshi"), None))
              for t in baskets]
    leans = [t for t in baskets
             if t.get("strategy") in ("pm-lean", "ml-leadlag-probe")
             and t.get("kalshi_ref")]
    with_k = [(t, k) for t, k in with_k if k and len(t.get("legs", [])) == 2]
    if not with_k:
        print("no open two-leg baskets")
        return
    c = httpx.Client(timeout=20)
    tickers = sorted({k["market_id"] for _, k in with_k} | {t["kalshi_ref"] for t in leans})
    status = {}
    for i in range(0, len(tickers), 50):
        r = c.get(f"{REST_BASE}/markets", params={"tickers": ",".join(tickers[i:i+50])}).json()
        for m in r.get("markets", []):
            status[m["ticker"]] = (m.get("status"), m.get("result"))
    settled = 0
    for t, k in with_k:
        st, result = status.get(k["market_id"], (None, None))
        if st != "finalized" or result not in ("yes", "no"):
            continue
        qty = float(t.get("qty", 0))
        cost = float(t.get("cost_usd", 0))
        rec = {"ts": time.time(), "relationship_id": t["relationship_id"],
               "title": t.get("title"), "strategy": "settlement", "status": "unwound",
               "closes_ts": t["ts"], "qty": qty,
               "kalshi_result": result,
               "proceeds_usd": qty,                       # $1/ct at resolution by construction
               "realized_pnl_usd": round(qty - cost, 4)}
        dual_append(rec, source="py:settle_baskets")
        settled += 1
        print(f"SETTLED {t['relationship_id'][:55]} x{qty:.0f} "
              f"result={result} realized=${qty - cost:+.2f}")
    # PM-lean riders: single-leg directional — settle via the referenced
    # Kalshi market's result (PM NO wins iff result == "no")
    for t in leans:
        st, result = status.get(t["kalshi_ref"], (None, None))
        if st != "finalized" or result not in ("yes", "no"):
            continue
        qty = float(t.get("qty", 0))
        cost = float(t.get("cost_usd", 0))
        side = (t.get("legs") or [{}])[0].get("side", "no")
        won = (side == "no" and result == "no") or (side == "yes" and result == "yes")
        proceeds = qty if won else 0.0
        rec = {"ts": time.time(), "relationship_id": t["relationship_id"],
               "title": t.get("title"), "strategy": "settlement", "status": "unwound",
               "closes_ts": t["ts"], "qty": qty, "kalshi_result": result,
               "proceeds_usd": proceeds,
               "realized_pnl_usd": round(proceeds - cost, 4)}
        dual_append(rec, source="py:settle_baskets")
        settled += 1
        print(f"SETTLED LEAN {t['relationship_id'][:50]} x{qty:.0f} "
              f"{'WON' if won else 'LOST'} realized=${proceeds - cost:+.2f}")
    print(f"{settled} basket(s)/lean(s) settled of {len(with_k) + len(leans)} open")


if __name__ == "__main__":
    main()
