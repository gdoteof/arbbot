"""Auto-hedge naked PM US legs — ONLY when completing the basket locks profit.

For every registry pair where PM-US net-short exceeds our Kalshi-YES holding
(a naked short leg, however it arose), buy the missing Kalshi YES qty IOC at a
limit that GUARANTEES the completed basket costs < $1 net of the Kalshi taker
fee plus a minimum locked margin. If the book doesn't offer that price, do
nothing — the 5-min timer retries until it does (Geoff 2026-07-22: "hedge only
if profitable; otherwise find a profitable hedge in the future").

Completed baskets are appended to the ledger (status=open, strategy=take-take)
so marks / risk seeding / auto-unwind track them. Kalshi-long-over-PM naked
(imb > 0) is alerted but not auto-hedged (v1).

Dry-run by default; --live to execute. Positions reads carry the PM API glitch
guards (empty read = retry; imbalance confirmed by a second fetch).
"""

import argparse
import json
import pathlib
import time
from decimal import Decimal

import httpx

from arbbot.exec import ledgerdb
from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.ledgerdb import dual_append
from arbbot.models.core import Venue
from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_recorder_config
from arbbot.record.kalshi import REST_BASE, load_private_key, sign_headers
from arbbot.registry.model import Registry
from arbbot.venues.pmus import PmusSession

D = pathlib.Path.home() / ".arbbot-credentials"
LEDGER = pathlib.Path("data/exec/trades.jsonl")
MIN_LOCK = Decimal("0.005")   # minimum locked profit per contract to act


def _retry(fn, tries=3, delay=0.8):
    last = None
    for i in range(tries):
        try:
            return fn()
        except Exception as e:  # noqa: BLE001
            last = e
            if i < tries - 1:
                time.sleep(delay * (i + 1))
    raise last


def kalshi_positions(c, kid, kkey):
    r = c.get(REST_BASE + "/portfolio/positions",
              headers=sign_headers(kid, kkey, "GET", "/trade-api/v2/portfolio/positions"))
    r.raise_for_status()
    return {p["ticker"]: float(p.get("position_fp") or 0)
            for p in r.json().get("market_positions", [])}


def pmus_positions(c):
    kid = (D / "polymarket_usa_key_id").read_text().strip()
    key = (D / "polymarket_usa_private_key").read_text().strip()
    # empty-read glitch guard (2026-07-22 API quirk) lives in get_positions:
    # it raises so _retry re-fetches instead of trusting a glitched snapshot
    pos = PmusSession(kid, key, client=c).get_positions()
    out = {}
    for slug, v in pos.items():
        out[slug] = {"net": float(v.get("netPosition") or 0),
                     "cost_per_share": float((v.get("costPerShare") or {}).get("value") or 0)}
    return out


def pmus_positions_consensus(c, attempts=4):
    """The PM positions endpoint intermittently DROPS positions from the
    response (and serves empty/stale snapshots — all seen 2026-07-22). Only
    act on two consecutive reads that agree exactly."""
    prev = None
    for _ in range(attempts):
        cur = _retry(lambda: pmus_positions(c))
        key = {k: v["net"] for k, v in cur.items()}
        if prev is not None and key == prev:
            return cur
        prev = key
        time.sleep(2)
    raise RuntimeError("PM positions unstable across reads — not acting")


def kalshi_ask(c, ticker):
    """(ask, tick) — tick from the market's price_ranges step (deci-cent
    markets take 0.001 prices, penny markets 400 anything finer)."""
    r = c.get(f"{REST_BASE}/markets", params={"tickers": ticker}).json()
    m = (r.get("markets") or [{}])[0]
    a = m.get("yes_ask_dollars")
    step = Decimal("0.01")
    for pr in m.get("price_ranges") or []:
        if pr.get("step"):
            step = Decimal(str(pr["step"]))
            break
    return (Decimal(str(a)) if a is not None else None), step


def kalshi_taker_fee(p: Decimal) -> Decimal:
    return (Decimal("0.07") * p * (1 - p)).quantize(Decimal("0.0001"))


def probe_owned_slugs():
    """PM slugs the research probes are actively working (coordination
    contract): never treat their positions as our naked legs to complete.
    Authoritative source: the ownership table (owner 'probe-*')."""
    owned = set()
    try:
        conn = ledgerdb.connect()
        try:
            owned.update(ledgerdb.owned_by(conn, "probe-"))
        finally:
            conn.close()
    except Exception:  # noqa: BLE001 — coordination read must not block hedging
        pass
    # TRANSITIONAL: union in the probes' own logs until every probe registers
    # its markets via ledgerdb.claim_ownership — drop this grep once they do.
    for pf in ("data/exec/pmus_maker_probe.jsonl", "data/exec/toxicity_probe.jsonl",
               "data/exec/leadlag_probe.jsonl"):
        try:
            for ln in pathlib.Path(pf).read_text().splitlines()[-800:]:
                try:
                    r = json.loads(ln)
                except ValueError:
                    continue
                slug = r.get("pm") or r.get("pair")
                if slug and r.get("action") not in ("dry_quote", "dry_run_would_trade"):
                    owned.add(slug)
        except OSError:
            continue
    return owned


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    c = httpx.Client(timeout=20)
    kid = (D / "kalshi_api_key_id").read_text().strip()
    kkey = load_private_key((D / "kalshi_private_key.pem").read_bytes())
    kpos = _retry(lambda: kalshi_positions(c, kid, kkey))
    ppos = pmus_positions_consensus(c)

    reg = Registry.load("config/registry.yaml")
    pairs = []
    for rel in reg.relationships:
        kt = next((l.market_id for l in rel.legs if l.venue is Venue.KALSHI), None)
        ps = next((l.market_id for l in rel.legs if l.venue is Venue.POLYMARKET_US), None)
        if kt and ps:
            pairs.append((rel.id, kt, ps))
    # sports pairings from the generic equivalence map — same 1:1 relation,
    # same profit-only completion (frozen-book ghost fills leave PM-short
    # excess; 2026-07-22 Babel)
    smap_f = pathlib.Path("data/scan/sports_equiv_map.json")
    if smap_f.exists():
        for sm in json.loads(smap_f.read_text()).get("matches", []):
            if sm.get("kalshi_long_ticker") and sm.get("pm_moneyline"):
                pairs.append((f"sports-{sm['league']}-{sm['teams'][:30]}",
                              sm["kalshi_long_ticker"], sm["pm_moneyline"]))
    gw = None
    acted = 0
    owned = probe_owned_slugs()
    for rel_id, kt, ps in pairs:
        if ps not in ppos:
            continue
        if ps in owned:
            continue  # research probe's market — their position management, not ours
        imb = kpos.get(kt, 0) + ppos[ps]["net"]
        if imb >= -0.5:
            if imb >= 1:
                print(f"[HEDGE] {rel_id} Kalshi-long naked imb {imb:+.0f} — not auto-hedged (v1)")
            continue
        naked = int(round(-imb))
        pm_basis = Decimal(str(ppos[ps]["cost_per_share"]))   # NO cost/ct = 1 - sold YES px
        ask, tick = kalshi_ask(c, kt)
        if ask is None:
            print(f"[HEDGE] {rel_id} naked x{naked}: no Kalshi ask — waiting")
            continue
        # completed basket cost = pm_basis + kalshi fill price (+ taker fee);
        # limit = the highest price still locking MIN_LOCK, floored to the tick
        limit = Decimal(1) - pm_basis - kalshi_taker_fee(ask) - MIN_LOCK
        limit = (limit / tick).to_integral_value(rounding="ROUND_FLOOR") * tick
        if ask > limit:
            print(f"[HEDGE] {rel_id} naked x{naked}: ask {ask} > profit limit "
                  f"{limit:.4f} (pm basis {pm_basis}) — waiting for a better book")
            continue
        print(f"[HEDGE] {rel_id} x{naked}: buy Kalshi YES IOC limit {limit:.4f} "
              f"(ask {ask}, locks >= {MIN_LOCK}/ct)", flush=True)
        if not args.live:
            print("[HEDGE] dry-run — no orders placed")
            continue
        if gw is None:
            gw = KalshiOrderGateway(kid, kkey, live=True)
        r = gw.place_yes(kt, "bid", limit, naked, post_only=False)
        oid = r.get("order_id") or (r.get("order") or {}).get("order_id")
        filled = 0
        for _ in range(6):
            filled = gw.filled_qty(oid) if oid else 0
            if filled >= 1:
                break
            time.sleep(0.5)
        if filled < 1:
            print(f"[HEDGE] {rel_id} IOC unfilled — book moved; next run retries")
            continue
        acted += 1
        cost_ct = float(pm_basis) + float(ask)  # conservative: assumes fill at ask
        rec = {"ts": time.time(), "relationship_id": rel_id,
               "title": f"{rel_id} (naked-leg hedge)", "qty": int(filled),
               "strategy": "take-take", "resolves_by": None, "resolves_estimated": True,
               "legs": [{"venue": "kalshi", "market_id": kt, "side": "yes",
                         "role": "taker", "qty": int(filled), "yes_price": str(ask)},
                        {"venue": "polymarket_us", "market_id": ps, "side": "no",
                         "role": "taker", "qty": int(filled),
                         "yes_price": str(Decimal(1) - pm_basis)}],
               "cost_usd": round(cost_ct * filled, 4),
               "payoff_usd": float(filled),
               "profit_usd": round((1 - cost_ct) * filled, 4),
               "status": "open"}
        dual_append(rec, source="py:hedge_naked_legs")
        Alerter(load_recorder_config().ntfy_topic).alert(
            f"arbbot HEDGED naked {rel_id} x{filled} @ <= {limit:.3f} — basket complete, "
            f"locks ~${(1 - cost_ct) * filled:.2f}")
        print(f"[HEDGE] {rel_id} hedged x{filled}/{naked}, ledger updated", flush=True)
    print(f"done — {acted} hedges placed")


if __name__ == "__main__":
    main()
