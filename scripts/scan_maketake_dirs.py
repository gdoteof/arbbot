"""Scan xvus- pairs for maker-viable + HEDGEABLE opportunities in each
direction, to pick a small live make-take set covering both:

  maker ASK on PM US  (PM YES dear): rest ask ~ inside PM ask; on fill sell YES,
     hedge BUY YES on Kalshi -> needs Kalshi ASK depth. edge ~ pm_ask - k_ask.
  maker BID on PM US  (PM YES cheap): rest bid ~ inside PM bid; on fill hold YES,
     hedge SELL YES on Kalshi -> needs Kalshi BID depth. edge ~ k_bid - pm_bid.

Read-only. Paced to respect the public gateway rate limit.
"""

import time
from decimal import Decimal

import httpx

from arbbot.models.core import Venue
from arbbot.record.kalshi import REST_BASE
from arbbot.registry.model import Registry

CLIP = 5
FEE = Decimal("0.01")          # Kalshi taker hedge ~1c/ct; PM maker free
MIN_EDGE = Decimal("0.03")
MAX_EDGE = Decimal("0.15")     # >15c almost always = mis-equivalence, not real edge
# families whose two venues resolve DIFFERENTLY (leaderboard snapshot, release
# date, definitional) -> huge fake edges, a fill would be unhedged. Exclude.
SKIP_FAMILIES = ("bestai", "gpt6", "aliens", "btcmax", "btc150k")


def main():
    reg = Registry.load("config/registry.yaml")
    pairs = [r for r in reg.relationships if r.id.startswith("xvus-")]
    c = httpx.Client(timeout=20)

    # batch Kalshi books
    ktk = sorted({next(l.market_id for l in r.legs if l.venue is Venue.KALSHI) for r in pairs})
    kbook = {}
    for i in range(0, len(ktk), 50):
        for m in c.get(f"{REST_BASE}/markets", params={"tickers": ",".join(ktk[i:i+50])}).json().get("markets", []):
            kbook[m["ticker"]] = (Decimal(str(m.get("yes_bid_dollars") or 0)), Decimal(str(m.get("yes_ask_dollars") or 0)),
                                  Decimal(str(m.get("yes_bid_size_fp") or 0)), Decimal(str(m.get("yes_ask_size_fp") or 0)))

    ask_ops, bid_ops = [], []
    for r in pairs:
        if any(fam in r.id for fam in SKIP_FAMILIES):
            continue
        kt = next(l.market_id for l in r.legs if l.venue is Venue.KALSHI)
        pslug = next(l.market_id for l in r.legs if l.venue is Venue.POLYMARKET_US)
        kb, ka, kbs, kas = kbook.get(kt, (None,)*4)
        if kb is None:
            continue
        try:
            b = c.get(f"https://gateway.polymarket.us/v1/markets/{pslug}/bbo").json().get("marketData", {})
        except Exception:
            continue
        pbid = (b.get("bestBid") or {}).get("value"); pask = (b.get("bestAsk") or {}).get("value")
        time.sleep(0.12)
        if not (pbid and pask):
            continue
        pbid, pask = Decimal(pbid), Decimal(pask)
        # maker ASK on PM (sell YES ~pask, hedge buy Kalshi YES @ka): edge, hedge=Kalshi ask depth
        ask_edge = (pask - Decimal("0.01")) - ka - FEE
        if MIN_EDGE <= ask_edge <= MAX_EDGE and kas >= CLIP:
            ask_ops.append((float(ask_edge), r.id, f"PMask {pask} / Kask {ka} kaSz {kas}"))
        # maker BID on PM (buy YES ~pbid, hedge sell Kalshi YES @kb): edge, hedge=Kalshi bid depth
        bid_edge = kb - (pbid + Decimal("0.01")) - FEE
        if MIN_EDGE <= bid_edge <= MAX_EDGE and kbs >= CLIP:
            bid_ops.append((float(bid_edge), r.id, f"PMbid {pbid} / Kbid {kb} kbSz {kbs}"))

    print("=== MAKER-ASK on PM US (PM dear; hedge buys Kalshi YES, needs Kalshi ASK depth) ===")
    for e, rid, d in sorted(ask_ops, reverse=True)[:10]:
        print(f"  {e*100:5.1f}c  {rid:<42} {d}")
    print(f"  ({len(ask_ops)} viable+hedgeable)")
    print("\n=== MAKER-BID on PM US (PM cheap; hedge sells Kalshi YES, needs Kalshi BID depth) ===")
    for e, rid, d in sorted(bid_ops, reverse=True)[:10]:
        print(f"  {e*100:5.1f}c  {rid:<42} {d}")
    print(f"  ({len(bid_ops)} viable+hedgeable)")


if __name__ == "__main__":
    main()
