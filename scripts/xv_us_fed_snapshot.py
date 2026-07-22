"""First cross-venue edge measurement on the COMPLIANT venue: Polymarket US
(QCX) vs Kalshi, for the same July 2026 FOMC decision. Both venues decompose
the decision into per-outcome YES/NO markets that map 1:1.

Cross-venue-equivalent arb for an outcome X (both venues pay $1 iff X):
  buy YES where ask is low, sell YES where bid is high (selling YES == buying
  NO on that venue). Gross riskless edge = max(kalshi_bid - pmus_ask,
  pmus_bid - kalshi_ask).  Positive => a real gap.

Read-only, no account needed. Snapshot (top-of-book) only.
"""

import httpx
from decimal import Decimal

from arbbot.record.kalshi import REST_BASE
from arbbot.fees.curves import FeeSchedule

PMUS = "https://gateway.polymarket.us/v1"
PM_EVENT = "usfed-fomc-2026-07-29"
K_EVENT = "KXFEDDECISION-26JUL"
# PM US outcome slug suffix -> Kalshi market suffix
MAP = {"nochng": "H0", "hike25": "H25", "cut25": "C25", "cut50": "C26", "hike50": "H26"}
LABEL = {"nochng": "no change", "hike25": "hike 25", "cut25": "cut 25",
         "cut50": "cut >25", "hike50": "hike >25"}


def d(x):
    try: return Decimal(str(x))
    except Exception: return None


def main():
    c = httpx.Client(timeout=20)

    # PM US: bbo per outcome market (fetch event via tag route; /events/{slug}
    # wants a numeric id, but the tag listing returns nested markets)
    evs = c.get(f"{PMUS}/events", params={"tag_slug": "fed-decision", "limit": 50}).json()["events"]
    ev = next(e for e in evs if e.get("slug") == PM_EVENT)
    pm = {}
    mkts = ev.get("markets") or []
    for m in mkts:
        suf = m["slug"].split("-")[-1]
        bbo = c.get(f"{PMUS}/markets/{m['slug']}/bbo").json().get("marketData", {})
        pm[suf] = (d((bbo.get("bestBid") or {}).get("value")),
                   d((bbo.get("bestAsk") or {}).get("value")))

    # Kalshi: yes bid/ask per outcome market
    r = c.get(f"{REST_BASE}/events", params={"series_ticker": "KXFEDDECISION",
              "with_nested_markets": "true", "status": "open", "limit": 50})
    kev = next(e for e in r.json()["events"] if e["event_ticker"] == K_EVENT)
    kal = {}
    for m in kev["markets"]:
        suf = m["ticker"].split("-")[-1]
        kal[suf] = (d(m.get("yes_bid_dollars")), d(m.get("yes_ask_dollars")))

    fees = FeeSchedule()
    print(f"Cross-venue snapshot: PM US {PM_EVENT}  vs  Kalshi {K_EVENT}")
    print(f"{'outcome':<10} {'PM bid/ask':>13} {'Kalshi bid/ask':>16} {'gross xv edge':>14}  direction")
    best = Decimal("-9")
    for suf, ksuf in MAP.items():
        pb, pa = pm.get(suf, (None, None))
        kb, ka = kal.get(ksuf, (None, None))
        if None in (pb, pa, kb, ka):
            print(f"{LABEL[suf]:<10}  missing leg (pm={pm.get(suf)} kal={kal.get(ksuf)})")
            continue
        e1 = kb - pa   # buy YES on PM (ask), sell YES on Kalshi (bid)
        e2 = pb - ka   # buy YES on Kalshi (ask), sell YES on PM (bid)
        edge = max(e1, e2)
        best = max(best, edge)
        direction = "buy PM / sell Kalshi" if e1 >= e2 else "buy Kalshi / sell PM"
        print(f"{LABEL[suf]:<10} {f'{pb}/{pa}':>13} {f'{kb}/{ka}':>16} "
              f"{edge*100:>12.1f}c  {direction if edge>0 else '-'}")
    print(f"\nbest gross cross-venue edge: {best*100:.1f}c/contract "
          f"(fees: Kalshi taker ~1-2c + PM US fee TBD; gross must clear that)")


if __name__ == "__main__":
    main()
