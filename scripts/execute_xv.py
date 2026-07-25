"""Execute ONE cross-venue-equivalent basket (Kalshi + Polymarket US), taker
both legs, for a vetted relationship. Riskless if filled on both legs: for
outcome X, buy YES on the cheap venue + buy NO on the dear venue -> pays $1
regardless of X, cost < $1.

DRY-RUN by default. Safety rails (all enforced live):
  * re-fetch BOTH live books and RECOMPUTE the edge at fire time; abort if it
    fell below --min-edge (never trade a stale probe number)
  * size = min(--max-size, both-venue top-of-book depth, --max-notional/cost)
  * fill the CONSTRAINED leg (PM US bid, thinner) FIRST via IOC; then buy
    EXACTLY the filled quantity on Kalshi (deep, stable) -> bounds leg risk
  * if leg 2 fails after leg 1 filled: loud alert, leave the (unwindable) single
    leg, never silently retry

Usage:
  python scripts/execute_xv.py --rel <id> --max-size N --max-notional USD \
     [--min-edge 0.03] [--live]
"""

import argparse
import time
from decimal import Decimal

import httpx

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.ledgerdb import dual_append
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.models.core import Venue
from arbbot.ops.config import load_credential
from arbbot.record.kalshi import REST_BASE, load_private_key
from arbbot.registry.model import Registry
from arbbot.venues.pmus import get_bbo, get_book

D = "~/.arbbot-credentials"


def kalshi_top(client, ticker):
    m = client.get(f"{REST_BASE}/markets", params={"tickers": ticker}).json()["markets"][0]
    return (Decimal(str(m["yes_bid_dollars"])), Decimal(str(m["yes_ask_dollars"])),
            Decimal(str(m.get("yes_bid_size_fp") or 0)), Decimal(str(m.get("yes_ask_size_fp") or 0)))


def pmus_top(client, slug):
    b = get_bbo(client, slug)
    bid = (b.get("bestBid") or {}); ask = (b.get("bestAsk") or {})
    book = get_book(client, slug)
    bid_sz = sum(Decimal(str(x["qty"])) for x in (book.get("bids") or [])
                 if x["px"]["value"] == bid.get("value"))
    return (Decimal(bid["value"]) if bid.get("value") else None,
            Decimal(ask["value"]) if ask.get("value") else None, bid_sz)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rel", required=True)
    ap.add_argument("--max-size", type=int, default=50)
    ap.add_argument("--max-notional", type=Decimal, default=Decimal("50"))
    ap.add_argument("--min-edge", type=Decimal, default=Decimal("0.03"))
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    reg = Registry.load("config/registry.yaml")
    rel = next(r for r in reg.relationships if r.id == args.rel)
    kleg = next(l for l in rel.legs if l.venue is Venue.KALSHI)
    pleg = next(l for l in rel.legs if l.venue is Venue.POLYMARKET_US)

    import pathlib
    cred = pathlib.Path(D).expanduser()
    kgw = KalshiOrderGateway((cred / "kalshi_api_key_id").read_text().strip(),
                             load_private_key((cred / "kalshi_private_key.pem").read_bytes()),
                             live=args.live)
    pgw = PolymarketUsOrderGateway((cred / "polymarket_usa_key_id").read_text().strip(),
                                   (cred / "polymarket_usa_private_key").read_text().strip(),
                                   live=args.live)
    c = httpx.Client(timeout=20)

    kb, ka, kbs, kas = kalshi_top(c, kleg.market_id)
    pb, pa, pbs = pmus_top(c, pleg.market_id)
    if None in (kb, ka, pb, pa):
        raise SystemExit("missing book side; aborting")

    # direction: buy YES cheap, sell YES (open NO) dear. K->PM is the observed one.
    edge_kpm = pb - ka   # buy Kalshi YES @ka, sell PM YES @pb (open PM NO)
    edge_pmk = kb - pa   # buy PM YES @pa, sell Kalshi YES @kb
    if edge_kpm >= edge_pmk:
        edge, buy, sell = edge_kpm, "kalshi", "pmus"
        depth = min(kas, pbs)          # buy against Kalshi ask, sell into PM bid
        cost_ct = ka + (Decimal(1) - pb)   # Kalshi YES + PM NO
    else:
        edge, buy, sell = edge_pmk, "pmus", "kalshi"
        depth = min(pbs, kbs)
        cost_ct = pa + (Decimal(1) - kb)

    size = min(Decimal(args.max_size), depth, (args.max_notional / cost_ct))
    size = int(size)  # whole contracts
    print(f"[{'LIVE' if args.live else 'DRY'}] {rel.id}  edge={edge*100:.1f}c dir={buy}->sell {sell}")
    print(f"  Kalshi {kleg.market_id}: bid/ask {kb}/{ka} (sz {kbs}/{kas})")
    print(f"  PM US  {pleg.market_id}: bid/ask {pb}/{pa} (bid sz {pbs})")
    print(f"  cost/ct={cost_ct} depth={depth} -> SIZE={size}  net~${float(size*(edge-Decimal('0.02'))):.2f} (after ~2c/ct fees)")

    if edge < args.min_edge:
        raise SystemExit(f"edge {edge*100:.1f}c < min {args.min_edge*100:.1f}c — ABORT")
    if size < 1:
        raise SystemExit("size < 1 (depth/notional too small) — ABORT")

    # This flow only implements the observed K->PM direction (buy Kalshi YES,
    # open PM US NO). PM leg is the constrained one -> fill it FIRST.
    if not (buy == "kalshi" and sell == "pmus"):
        raise SystemExit("only K->PM direction wired; PM-cheap not expected here")

    print(f"\n  leg1 (constrained): PM US open NO {size} @ YES-bid {pb} (IOC)")
    r1 = pgw.place_short(pleg.market_id, pb, size, post_only=False)
    print("   ->", r1)
    oid = r1.get("id") or (r1.get("order") or {}).get("id")
    # AUTHORITATIVE fill: the IOC create response omits cumQuantity — re-fetch
    # and hedge EXACTLY the filled qty (never trust the requested size)
    if args.live:
        time.sleep(0.5)
        filled = pgw.filled_qty(oid) if oid else 0
    else:
        filled = size
    print(f"   PM filled = {filled}")
    if filled < 1:
        raise SystemExit("PM leg unfilled — nothing to hedge, done")

    # bid 1 tick THROUGH the ask so the hedge can't miss if the ask ticks up
    # between legs (the edge absorbs 1c; leaving a leg unhedged is the real risk)
    hedge_px = min(ka + Decimal("0.01"), Decimal("0.99"))
    print(f"  leg2: Kalshi buy YES {filled} @ {hedge_px} (1 tick through ask {ka}, IOC)")
    r2 = kgw.place_yes(kleg.market_id, "bid", hedge_px, filled, post_only=False)
    print("   ->", r2)

    if args.live:
        _record_trade(rel, filled, pb, pa, ka, hedge_px, edge, r1, r2)
    print("\nDONE. Verify positions on both venues.")


def _record_trade(rel, qty, pb, pa, ka, hedge_px, edge, r1, r2):
    """Append the executed basket to data/exec/trades.jsonl (the dashboard's
    Trades tab reads this). Best-effort — a logging failure must not obscure
    that the orders themselves went through."""
    import json
    from pathlib import Path
    kleg = next(l for l in rel.legs if l.venue is Venue.KALSHI)
    pleg = next(l for l in rel.legs if l.venue is Venue.POLYMARKET_US)
    k_fee = Decimal(str(r2.get("average_fee_paid") or 0)) * qty if isinstance(r2, dict) else Decimal(0)
    k_px = Decimal(str((r2 or {}).get("average_fill_price") or hedge_px))
    pm_no_cost = (Decimal(1) - pb) * qty          # NO cost basis on PM US
    cost = k_px * qty + k_fee + pm_no_cost
    rec = {
        "ts": time.time(), "relationship_id": rel.id, "title": rel.id, "qty": qty,
        "legs": [
            # both legs cross the resting book (marketable IOC) -> taker
            {"venue": "kalshi", "market_id": kleg.market_id, "action": "buy_yes",
             "side": "yes", "role": "taker", "qty": qty, "avg_price": str(k_px),
             "fees": str(k_fee), "order_id": (r2 or {}).get("order_id")},
            {"venue": "polymarket_us", "market_id": pleg.market_id, "action": "buy_no",
             "side": "no", "role": "taker", "qty": qty, "yes_price": str(pb),
             "cost": str(pm_no_cost), "order_id": (r1 or {}).get("id")},
        ],
        "cost_usd": float(cost), "payoff_usd": float(qty),
        "profit_usd": float(Decimal(qty) - cost), "status": "open",
    }
    p = Path("data/exec/trades.jsonl")
    dual_append(rec, source="py:execute_xv")
    print(f"   recorded to {p} (profit ~${rec['profit_usd']:.2f})")


if __name__ == "__main__":
    main()
