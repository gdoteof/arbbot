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
from mark_positions import compute_row, exit_fees_usd, kalshi_books, pmus_topbook  # noqa: E402

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.ledger import open_baskets, parse_lines
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_credential, load_recorder_config
from arbbot.record.kalshi import load_private_key

LEDGER = Path("data/exec/trades.jsonl")
KTICK = Decimal("0.01")   # sweep one level past the touch on the Kalshi close


PAUSE_REQ = Path("data/exec/.quote_pause_requests.json")


def request_quote_pause(rid: str, seconds: float = 120.0) -> None:
    """Ask the runner to pull its maker quotes on this relationship before we
    close the basket — our own resting quotes are often the touch on these
    thin books, and self-trade prevention deadlocks the unwind IOC otherwise
    (card ed6a5910: 14 soft unwinds blocked)."""
    try:
        reqs = json.loads(PAUSE_REQ.read_text())
    except (OSError, ValueError):
        reqs = {}
    reqs[rid] = time.time() + seconds
    reqs = {k: v for k, v in reqs.items() if v > time.time()}
    PAUSE_REQ.write_text(json.dumps(reqs))


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
    request_quote_pause(rid)
    time.sleep(8)  # runner checks the request file every ~5s and pulls quotes
    # RE-PRICE against the book WITHOUT our own quotes (card ed6a5910 pt.2):
    # on thin books our maker quotes ARE the touch, so pre-pause prices — and
    # marks' liq values generally — are flattered by ourselves. Only unwind if
    # it is still profitable against the real residual book.
    import httpx as _hx
    _c2 = _hx.Client(timeout=15)
    kb2 = kalshi_books(_c2, [kleg["market_id"]]).get(kleg["market_id"], (None, None))
    pb2 = pmus_topbook(_c2, pleg["market_id"])
    k_bid2, k_ask2 = kb2
    p_bid2, p_ask2 = pb2
    # direction-correct liquidation: an inverted basket closes Kalshi by BUYING
    # yes @ ask and sells PM YES @ bid, so pricing it off (k_bid, p_ask) — as
    # this guard did — checked a trade we never place
    if inverted:
        k_exit2, p_exit2 = k_ask2, p_bid2
        liq_ct2 = ((Decimal(1) - Decimal(str(k_ask2))) + Decimal(str(p_bid2))
                   if not (k_ask2 is None or p_bid2 is None) else None)
    else:
        k_exit2, p_exit2 = k_bid2, p_ask2
        liq_ct2 = (Decimal(str(k_bid2)) + (Decimal(1) - Decimal(str(p_ask2)))
                   if not (k_bid2 is None or p_ask2 is None) else None)
    if liq_ct2 is None:
        print(f"[UNWIND] {rid} book empty after quote pause — skip", flush=True)
        return
    # net of REAL both-leg taker exit fees (card b83b0449): the flat 1c/ct this
    # used to subtract understated the round trip by up to 3.3x, so the
    # "never unwind at a loss" guard could green-light a losing close
    fees2 = exit_fees_usd(Decimal(str(k_exit2)), Decimal(str(p_exit2)), Decimal(qty))
    liq2 = liq_ct2 * qty - fees2
    mark2 = float(liq2) - float(t["cost_usd"])
    if mark2 <= 0:
        print(f"[UNWIND] {rid} not profitable against the residual book "
              f"(mark {mark2:+.2f} net of ${float(fees2):.2f} exit fees vs "
              f"{row['mark_pnl_usd']:+.2f} with our quotes) — skip", flush=True)
        return
    # execute against the same refreshed book the guard just cleared
    k_bid, k_ask, p_bid, p_ask = k_bid2, k_ask2, p_bid2, p_ask2
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
        kr = kgw.place_yes(kleg["market_id"], "bid",
                           min(Decimal(str(k_ask)) + KTICK, Decimal("0.99")),
                           filled, post_only=False)
    else:
        # Kalshi leg: sell YES, one tick through the bid, exact filled qty
        kr = kgw.place_yes(kleg["market_id"], "ask", max(Decimal(str(k_bid)) - KTICK, KTICK),
                           filled, post_only=False)
    # CONFIRM the Kalshi close (2026-07-23: fire-and-forget sells silently
    # failed during venue 429s, stranding longs mid-unwind after the PM close)
    koid = (kr.get("order") or {}).get("order_id") or kr.get("order_id") or ""
    kf = 0
    for _ in range(8):
        try:
            kf = kgw.filled_qty(koid)
        except Exception:
            pass
        if kf >= filled:
            break
        time.sleep(0.5)
    if kf < filled:
        # one repriced retry at the FRESH touch before giving up (2026-07-23:
        # a fedcut sell missed, yet the record booked full proceeds — 4 YES
        # stranded 2h until a recon page)
        with __import__("contextlib").suppress(Exception):
            kb3 = kalshi_books(httpx.Client(timeout=15),
                               [kleg["market_id"]]).get(kleg["market_id"], (None, None))
            if inverted and kb3[1] is not None:
                kr2 = kgw.place_yes(kleg["market_id"], "bid",
                                    min(Decimal(str(kb3[1])) + KTICK, Decimal("0.99")),
                                    filled - kf, post_only=False)
            elif not inverted and kb3[0] is not None:
                kr2 = kgw.place_yes(kleg["market_id"], "ask",
                                    max(Decimal(str(kb3[0])) - KTICK, KTICK),
                                    filled - kf, post_only=False)
            else:
                kr2 = {}
            koid2 = (kr2.get("order") or {}).get("order_id") or kr2.get("order_id") or ""
            for _ in range(8):
                try:
                    kf += kgw.filled_qty(koid2) if koid2 else 0
                except Exception:
                    pass
                if kf >= filled:
                    break
                time.sleep(0.5)
    if kf < filled:
        print(f"[UNWIND] {rid} KALSHI close underfilled {kf}/{filled} — stranded "
              f"position; proceeds recorded for the FILLED portion only; recon "
              f"flags the remainder, completion appends a correction", flush=True)
        Alerter(load_recorder_config().ntfy_topic).alert(
            f"arbbot UNWIND {rid}: Kalshi close underfilled {kf}/{filled} — "
            f"{filled - kf} stranded, ledger records actuals only")
    frac = filled / qty
    # proceeds count ONLY what each leg actually closed — an unfilled Kalshi
    # remainder is stranded inventory, not proceeds; recon + a later
    # completion (with a correction record) recover it at the real price
    if inverted:
        gross = (Decimal(str(p_bid)) * filled
                 + (Decimal(1) - Decimal(str(k_ask))) * kf)
        fees = exit_fees_usd(Decimal(str(k_ask)), Decimal(str(p_bid)),
                             Decimal(kf), Decimal(filled))
        legs = [{"venue": "kalshi", "market_id": kleg["market_id"],
                 "side": "no", "action": "close_via_buy_yes", "qty": kf,
                 "yes_price": str(k_ask)},
                {"venue": "polymarket_us", "market_id": pleg["market_id"],
                 "side": "yes", "action": "close_via_buy_short", "qty": filled,
                 "yes_price": str(p_bid)}]
    else:
        gross = (Decimal(str(k_bid)) * kf
                 + (Decimal(1) - Decimal(str(p_ask))) * filled)
        fees = exit_fees_usd(Decimal(str(k_bid)), Decimal(str(p_ask)),
                             Decimal(kf), Decimal(filled))
        legs = [{"venue": "kalshi", "market_id": kleg["market_id"],
                 "side": "yes", "action": "sell", "qty": kf, "yes_price": str(k_bid)},
                {"venue": "polymarket_us", "market_id": pleg["market_id"],
                 "side": "no", "action": "sell", "qty": filled, "yes_price": str(p_ask)}]
    # proceeds are recorded NET of the exit taker fees we just paid — the
    # accounting kernel takes proceeds_usd/realized_pnl_usd at face value
    # (fold() does no fee math), so booking them gross overstated realized P&L
    # by the whole round trip and hid the error inside `slippage` (b83b0449)
    proceeds = gross - fees
    rec = {"ts": time.time(), "relationship_id": rid, "title": t.get("title"),
           "strategy": "unwind", "status": "unwound", "closes_ts": t["ts"],
           "qty": filled,
           "legs": legs,
           "proceeds_usd": float(proceeds),
           "gross_proceeds_usd": float(gross),
           "exit_fees_usd": float(fees),
           "realized_pnl_usd": float(proceeds) - float(t["cost_usd"]) * frac}
    if kf < filled:
        rec["stranded_kalshi_qty"] = filled - kf
    with open(LEDGER, "a") as f:
        f.write(json.dumps(rec) + "\n")
    print(f"[UNWIND] {rid} closed x{filled}/{qty} proceeds=${float(proceeds):.2f} "
          f"(gross ${float(gross):.2f} - fees ${float(fees):.2f}) "
          f"realized=${rec['realized_pnl_usd']:.2f}", flush=True)
    Alerter(load_recorder_config().ntfy_topic).alert(
        f"arbbot UNWIND {rid} x{filled} realized ${rec['realized_pnl_usd']:.2f} "
        f"(fwd apr was {row['forward_hold_apr']}%)")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    if Path("data/KILL").exists():  # global kill switch (card 39140c2e)
        print("data/KILL present — unwinder halted")
        return

    records = parse_lines(LEDGER.read_text().splitlines()) if LEDGER.exists() else []
    baskets = open_baskets(records)
    if not baskets:
        print("no open baskets")
        return

    c = httpx.Client(timeout=20)
    kb = kalshi_books(c, [l["market_id"] for t in baskets
                          for l in t["legs"] if l["venue"] == "kalshi"])
    pgw = kgw = None
    hard = 0
    for t in baskets:
        kleg = next((l for l in t["legs"] if l["venue"] == "kalshi"), None)
        pleg = next((l for l in t["legs"] if l["venue"] == "polymarket_us"), None)
        if kleg is None or pleg is None:
            # single-venue basket (e.g. a research probe leg) — not a cross-venue
            # pair this unwinder can price or close; leave it to its owner
            print(f"[UNWIND] {t['relationship_id']} skipped — not a kalshi/pm-us pair "
                  f"(legs: {[l['venue'] for l in t['legs']]})", flush=True)
            continue
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
        # probe-owned baskets (coordination contract: research manages its own
        # positions) — surface the signal, never auto-execute against them
        if t.get("strategy") in ("pmus-maker-probe", "ml-leadlag-probe"):
            print(f"[UNWIND] {t['relationship_id']} signal on PROBE basket "
                  f"({t.get('strategy')}) — alerting, not acting (ownership)", flush=True)
            Alerter(load_recorder_config().ntfy_topic).alert(
                f"arbbot: unwind signal on probe basket {t['relationship_id']} "
                f"({t.get('strategy')}, fwd_apr={row.get('forward_hold_apr')}) — "
                f"research side to action")
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
