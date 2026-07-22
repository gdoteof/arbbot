"""Supervised single make-take capture on ONE cross-venue-equivalent pair.

REST a maker quote on the PM US leg (fee-free maker) at a price that, if lifted,
leaves a profitable Kalshi taker hedge; poll for a fill; the instant any fills,
cancel the remainder and hedge exactly the filled qty on Kalshi. This BEATS the
taker basket by capturing a tick of spread and paying zero PM maker fee.

Bounded + supervised: rests for --timeout-s then cancels if unfilled (a maker
quote that never crosses costs nothing). DRY-RUN by default.

Direction handled: PM US YES is the DEAR leg -> rest a maker ASK (sell YES /
open NO) on PM US, hedge by buying YES on Kalshi. (The cheap-PM case would rest
a bid; not wired — assert if it arises.)

Usage: python scripts/make_take.py --rel <id> --size N [--min-edge 0.03]
       [--timeout-s 120] [--live]
"""

import argparse
import json
import pathlib
import time
from decimal import Decimal

import httpx

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.models.core import Venue
from arbbot.record.kalshi import REST_BASE, load_private_key
from arbbot.registry.model import Registry

CRED = pathlib.Path("~/.arbbot-credentials").expanduser()
KALSHI_FEE = Decimal("0.01")   # ~taker fee/ct on the hedge
TICK = Decimal("0.01")


def kalshi_top(c, t):
    m = c.get(f"{REST_BASE}/markets", params={"tickers": t}).json()["markets"][0]
    return Decimal(str(m["yes_bid_dollars"])), Decimal(str(m["yes_ask_dollars"])), Decimal(str(m.get("yes_ask_size_fp") or 0))


def pmus_top(c, slug):
    b = c.get(f"https://gateway.polymarket.us/v1/markets/{slug}/bbo").json().get("marketData", {})
    bid, ask = (b.get("bestBid") or {}), (b.get("bestAsk") or {})
    return (Decimal(bid["value"]) if bid.get("value") else None,
            Decimal(ask["value"]) if ask.get("value") else None)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rel", required=True)
    ap.add_argument("--size", type=int, default=25)
    ap.add_argument("--min-edge", type=Decimal, default=Decimal("0.03"))
    ap.add_argument("--timeout-s", type=int, default=120)
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    reg = Registry.load("config/registry.yaml")
    rel = next(r for r in reg.relationships if r.id == args.rel)
    kleg = next(l for l in rel.legs if l.venue is Venue.KALSHI)
    pleg = next(l for l in rel.legs if l.venue is Venue.POLYMARKET_US)
    kgw = KalshiOrderGateway((CRED / "kalshi_api_key_id").read_text().strip(),
                             load_private_key((CRED / "kalshi_private_key.pem").read_bytes()), live=args.live)
    pgw = PolymarketUsOrderGateway((CRED / "polymarket_usa_key_id").read_text().strip(),
                                   (CRED / "polymarket_usa_private_key").read_text().strip(), live=args.live)
    c = httpx.Client(timeout=20)

    kb, ka, kas = kalshi_top(c, kleg.market_id)
    pb, pa = pmus_top(c, pleg.market_id)
    if None in (pb, pa):
        raise SystemExit("no PM US book; abort")

    # Rest a maker ASK on PM US improving the current best ask by a tick, as long
    # as it stays profitable to hedge: sell YES @ q, buy Kalshi YES @ ka ->
    # profit/ct = q - ka - fees.  Need q >= ka + fees + min_edge, and q must
    # remain a maker (>= best bid + tick, <= best ask).
    floor = ka + KALSHI_FEE + args.min_edge
    q = pa - TICK                       # improve the ask by one tick
    if q < pb + TICK:                   # keep it a maker, inside the spread
        q = pb + TICK
    if q < floor:
        raise SystemExit(f"no profitable maker ask: best improvable {q} < floor {floor} "
                         f"(ka={ka} +fee+edge). ABORT")
    edge = q - ka - KALSHI_FEE
    print(f"[{'LIVE' if args.live else 'DRY'}] make-take {rel.id}")
    print(f"  Kalshi {kleg.market_id}: bid/ask {kb}/{ka} (ask sz {kas})")
    print(f"  PM US  {pleg.market_id}: bid/ask {pb}/{pa}")
    print(f"  REST maker ASK (open NO) @ {q}  -> if lifted, hedge Kalshi YES @~{ka}, "
          f"edge ~{edge*100:.1f}c/ct (PM maker fee $0)")
    if args.size > kas and args.live:
        print(f"  NOTE: size {args.size} > Kalshi ask depth {kas}; hedge may be partial")

    resting = pgw.place_yes(pleg.market_id, "ask", q, args.size, post_only=True)
    oid = resting.get("id") or (resting.get("order") or {}).get("id")
    print(f"  rested maker order: {oid}")
    if not args.live:
        print("  (dry-run: not polling; live would watch for a fill then hedge)")
        return

    deadline = time.time() + args.timeout_s
    filled = 0
    while time.time() < deadline:
        time.sleep(2.0)
        f = pgw.filled_qty(oid)
        if f > filled:
            print(f"  FILL: {f}/{args.size} lifted")
            filled = f
        if filled >= args.size:
            break
    # stop resting the remainder, then RE-FETCH the authoritative fill (a lift
    # can land between the last poll and the cancel) before hedging
    pgw.cancel(oid, market_slug=pleg.market_id)
    filled = pgw.filled_qty(oid)
    if filled < 1:
        print(f"  no fill in {args.timeout_s}s — cancelled, nothing to hedge (no cost)")
        return
    hedge_px = min(ka + TICK, Decimal("0.99"))
    print(f"  hedging Kalshi buy YES {filled} @ {hedge_px} (IOC)")
    r2 = kgw.place_yes(kleg.market_id, "bid", hedge_px, filled, post_only=False)
    print("   ->", r2)
    _record(rel, kleg, pleg, filled, q, r2)
    print("\nDONE — make-take captured. Verify both venues.")


def _record(rel, kleg, pleg, qty, pm_ask, r2):
    k_fee = Decimal(str((r2 or {}).get("average_fee_paid") or 0)) * qty
    k_px = Decimal(str((r2 or {}).get("average_fill_price") or 0))
    pm_no_cost = (Decimal(1) - pm_ask) * qty
    cost = k_px * qty + k_fee + pm_no_cost
    rec = {"ts": time.time(), "relationship_id": rel.id, "title": rel.id, "qty": qty,
           "strategy": "make-take",
           "legs": [
               {"venue": "kalshi", "market_id": kleg.market_id, "action": "buy_yes",
                "side": "yes", "role": "taker", "qty": qty, "avg_price": str(k_px),
                "fees": str(k_fee), "order_id": (r2 or {}).get("order_id")},
               {"venue": "polymarket_us", "market_id": pleg.market_id, "action": "buy_no",
                "side": "no", "role": "maker", "qty": qty, "yes_price": str(pm_ask),
                "cost": str(pm_no_cost)},
           ],
           "cost_usd": float(cost), "payoff_usd": float(qty),
           "profit_usd": float(Decimal(qty) - cost), "status": "open"}
    p = pathlib.Path("data/exec/trades.jsonl")
    with open(p, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"   recorded make-take to {p} (profit ~${rec['profit_usd']:.2f})")


if __name__ == "__main__":
    main()
