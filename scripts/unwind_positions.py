"""Auto-unwind executor for HARD unwind signals (forward hold APR < floor).

For each open ledger basket whose LIVE-recomputed marks say unwind_hard
(re-fetches top-of-book at execution time — never trusts a stale marks.json),
closes both legs and appends a compensating "unwound" record to the ledger
(append-only; see arbbot.exec.ledger for the netting convention).

Guards:
  * live unwind_hard AND live mark_pnl > 0 (never unwind at a loss)
  * standard-direction baskets only (long K YES + long PM NO); inverted
    maker baskets are skipped with an alert
  * legging-safe: PM (thin) leg IOC first with polled fill confirmation,
    then exact-filled-qty on Kalshi; partial fills unwind partially
  * the 60s reconciliation net backstops any legging miss

Dry-run by default; --live to execute. Designed for a 5-min systemd timer.
"""

import argparse
import json
import sys
import time
from decimal import Decimal
from pathlib import Path

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))
from mark_positions import compute_row, kalshi_books, pmus_topbook  # noqa: E402

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.ledger import open_baskets, parse_lines
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_credential, load_recorder_config
from arbbot.record.kalshi import load_private_key

LEDGER = Path("data/exec/trades.jsonl")
KTICK = Decimal("0.01")   # sweep one level past the touch on the Kalshi close


def close_basket(t: dict, row: dict, k_bid, p_ask, pgw, kgw, live: bool,
                 k_ask=None, p_bid=None) -> None:
    qty = int(t["qty"])
    rid = t["relationship_id"]
    kleg = next(l for l in t["legs"] if l["venue"] == "kalshi")
    pleg = next(l for l in t["legs"] if l["venue"] == "polymarket_us")
    inverted = kleg.get("side") == "no"  # Kalshi NO + PM YES basket (v2)
    if inverted:
        print(f"[UNWIND] {rid} x{qty} (inverted): close PM YES @ bid {p_bid} "
              f"then close Kalshi NO via buy YES @ {k_ask} | "
              f"fwd_apr={row['forward_hold_apr']} mark=${row['mark_pnl_usd']:.2f}",
              flush=True)
    else:
        print(f"[UNWIND] {rid} x{qty}: buy PM YES @ {p_ask} (close NO) then "
              f"sell Kalshi YES @ {k_bid} | fwd_apr={row['forward_hold_apr']} "
              f"mark=${row['mark_pnl_usd']:.2f}", flush=True)
    if not live:
        print("[UNWIND] dry-run — no orders placed", flush=True)
        return
    if inverted:
        # PM leg first (thin): close held YES via BUY_SHORT offset, IOC at bid
        r1 = pgw.place_short(pleg["market_id"], Decimal(str(p_bid)), qty,
                             post_only=False)
    else:
        # PM leg: buy back YES (closes the NO) — IOC at the ask
        r1 = pgw.place_yes(pleg["market_id"], "bid", Decimal(str(p_ask)), qty, post_only=False)
    oid1 = r1.get("id") or r1.get("order_id")
    filled = 0
    for _ in range(6):  # PM fill reporting lags the IOC (2026-07-22 lesson)
        filled = pgw.filled_qty(oid1) if oid1 else 0
        if filled >= 1:
            break
        time.sleep(0.5)
    if filled < 1:
        print(f"[UNWIND] {rid} PM close reports unfilled — abort, recon verifies", flush=True)
        return
    if inverted:
        # Kalshi leg: close held NO by buying YES, one tick through the ask
        kgw.place_yes(kleg["market_id"], "bid",
                      min(Decimal(str(k_ask)) + KTICK, Decimal("0.99")),
                      filled, post_only=False)
    else:
        # Kalshi leg: sell YES, one tick through the bid, exact filled qty
        kgw.place_yes(kleg["market_id"], "ask", max(Decimal(str(k_bid)) - KTICK, KTICK),
                      filled, post_only=False)
    frac = filled / qty
    if inverted:
        proceeds = (Decimal(str(p_bid)) + (Decimal(1) - Decimal(str(k_ask)))) * filled
        legs = [{"venue": "kalshi", "market_id": kleg["market_id"],
                 "side": "no", "action": "close_via_buy_yes", "qty": filled,
                 "yes_price": str(k_ask)},
                {"venue": "polymarket_us", "market_id": pleg["market_id"],
                 "side": "yes", "action": "close_via_buy_short", "qty": filled,
                 "yes_price": str(p_bid)}]
    else:
        proceeds = (Decimal(str(k_bid)) + (Decimal(1) - Decimal(str(p_ask)))) * filled
        legs = [{"venue": "kalshi", "market_id": kleg["market_id"],
                 "side": "yes", "action": "sell", "qty": filled, "yes_price": str(k_bid)},
                {"venue": "polymarket_us", "market_id": pleg["market_id"],
                 "side": "no", "action": "sell", "qty": filled, "yes_price": str(p_ask)}]
    rec = {"ts": time.time(), "relationship_id": rid, "title": t.get("title"),
           "strategy": "unwind", "status": "unwound", "closes_ts": t["ts"],
           "qty": filled,
           "legs": legs,
           "proceeds_usd": float(proceeds),
           "realized_pnl_usd": float(proceeds) - float(t["cost_usd"]) * frac}
    with open(LEDGER, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[UNWIND] {rid} closed x{filled}/{qty} proceeds=${float(proceeds):.2f} "
          f"realized=${rec['realized_pnl_usd']:.2f}", flush=True)
    Alerter(load_recorder_config().ntfy_topic).alert(
        f"arbbot UNWIND {rid} x{filled} realized ${rec['realized_pnl_usd']:.2f} "
        f"(fwd apr was {row['forward_hold_apr']}%)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    records = parse_lines(LEDGER.read_text().splitlines()) if LEDGER.exists() else []
    baskets = open_baskets(records)
    if not baskets:
        print("no open baskets")
        return

    c = httpx.Client(timeout=20)
    kb = kalshi_books(c, [next(l["market_id"] for l in t["legs"] if l["venue"] == "kalshi")
                          for t in baskets])
    pgw = kgw = None
    hard = 0
    for t in baskets:
        kleg = next(l for l in t["legs"] if l["venue"] == "kalshi")
        pleg = next(l for l in t["legs"] if l["venue"] == "polymarket_us")
        k_bid, k_ask = kb.get(kleg["market_id"], (None, None))
        p_bid, p_ask = pmus_topbook(c, pleg["market_id"])
        row = compute_row(t, k_bid, k_ask, p_bid, p_ask)
        # capital-constrained displacement (Geoff 2026-07-23): when the class
        # budget is >=95% used, ALSO unwind SOFT-signal positions (profitable,
        # fwd APR below the 12% hurdle) — early unwinds have realized very
        # high annualized returns and the freed capital redeploys into the
        # opportunities the cap is otherwise blocking.
        CLASS_BUDGET = 980 * 0.35   # keep in sync with RiskConfig
        open_notional = sum(float(b.get("qty") or 0) for b in baskets)
        constrained = open_notional >= 0.95 * CLASS_BUDGET
        eligible = row.get("unwind_hard") or (constrained and row.get("unwind_signal"))
        if not eligible:
            continue
        hard += 1
        # v2 (2026-07-23): inverted baskets (Kalshi NO + PM YES) are handled
        # by close_basket's inverted branch — no more v1 skip.
        if not row.get("mark_pnl_usd") or row["mark_pnl_usd"] <= 0:
            print(f"[UNWIND] {t['relationship_id']} hard but mark "
                  f"{row.get('mark_pnl_usd')} <= 0 — not unwinding at a loss", flush=True)
            continue
        if pgw is None and args.live:
            kid = load_credential("kalshi_api_key_id").decode().strip()
            kgw = KalshiOrderGateway(kid, load_private_key(load_credential("kalshi_private_key.pem")),
                                     live=True)
            us_id = load_credential("polymarket_usa_key_id").decode().strip()
            us_key = load_credential("polymarket_usa_private_key").decode().strip()
            pgw = PolymarketUsOrderGateway(us_id, us_key, live=True)
        close_basket(t, row, k_bid, p_ask, pgw, kgw, args.live,
                     k_ask=k_ask, p_bid=p_bid)
    print(f"checked {len(baskets)} open baskets, {hard} hard-unwind")


if __name__ == "__main__":
    main()
