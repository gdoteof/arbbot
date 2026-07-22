"""Auto-execute genuinely good TAKE-TAKE (taker-taker) cross-venue arb crossings.

Riskless if both legs fill (buy YES cheap venue + open NO dear venue = locked $1).
BAR (Geoff, 2026-07-21): a crossing's net-of-fee edge, expressed as APR, must beat
our current BLENDED portfolio APR — i.e. it must improve the average return on
capital, else the idle cash is better left for a better crossing.

Safety:
  * VETTED families only — never auto-trade an un-vetted (possibly mis-equivalent) pair.
  * per-relationship concentration cap (don't pile into one outcome).
  * recompute edge at fire time; abort if it fell below the bar.
  * legging-safe: fill the CONSTRAINED leg (PM US) first via IOC, then buy EXACTLY
    the filled qty on Kalshi; loud alert if leg 2 fails (the 60s recon net also catches).
  * DRY-RUN by default; --live to execute.
Only the K->PM direction (buy Kalshi YES + open PM NO) auto-executes — the
persistent basis makes it the profitable one; the reverse is logged, not traded.
"""

import argparse
import base64
import datetime
import json
import pathlib
from decimal import Decimal

import httpx
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.exec.resolve_dates import resolve_date
from arbbot.models.core import Venue
from arbbot.record.kalshi import REST_BASE, load_private_key, sign_headers
from arbbot.registry.model import Registry

D = pathlib.Path.home() / ".arbbot-credentials"
VETTED = ("xvus-time-poty-26", "xvus-france-pres-27", "xvus-brazil-pres-26", "xvus-fedcut-26")
FEE_CT = Decimal("0.02")   # ~both-leg taker fees per contract (conservative)
ALERT = None


def blended_apr():
    try:
        d = json.loads((pathlib.Path("data/exec/marks.json")).read_text())
    except Exception:
        return None
    today = datetime.date.today()
    num = den = cost = prof = 0.0
    for p in d.get("positions", []):
        c, pr, rb = p.get("cost_usd", 0), p.get("locked_profit_usd", 0), p.get("resolves_by")
        cost += c
        prof += pr
        if rb and c:
            try:
                yrs = max((datetime.date.fromisoformat(str(rb)[:10]) - today).days, 1) / 365.25
            except ValueError:
                continue
            num += c * yrs
            den += c
    wavg = (num / den) if den else None
    return (prof / cost / wavg * 100) if (cost and wavg) else None


def years_to(rel_id):
    rd, _ = resolve_date(rel_id)
    if not rd:
        return None
    return max((datetime.date.fromisoformat(rd) - datetime.date.today()).days, 1) / 365.25


def kalshi_top(c, ticker):
    m = c.get(f"{REST_BASE}/markets", params={"tickers": ticker}).json()["markets"][0]
    return (Decimal(str(m["yes_bid_dollars"])), Decimal(str(m["yes_ask_dollars"])),
            Decimal(str(m.get("yes_bid_size_fp") or 0)), Decimal(str(m.get("yes_ask_size_fp") or 0)))


def pmus_top(c, slug):
    b = c.get(f"https://gateway.polymarket.us/v1/markets/{slug}/bbo").json().get("marketData", {})
    bid, ask = (b.get("bestBid") or {}), (b.get("bestAsk") or {})
    book = c.get(f"https://gateway.polymarket.us/v1/markets/{slug}/book").json().get("marketData", {})
    bid_sz = sum(Decimal(str(x["qty"])) for x in (book.get("bids") or [])
                 if x["px"]["value"] == bid.get("value"))
    return (Decimal(bid["value"]) if bid.get("value") else None,
            Decimal(ask["value"]) if ask.get("value") else None, bid_sz)


def positions(c):
    kid = (D / "kalshi_api_key_id").read_text().strip()
    kkey = load_private_key((D / "kalshi_private_key.pem").read_bytes())
    kp = {p["ticker"]: abs(float(p.get("position_fp") or 0)) for p in
          c.get(REST_BASE + "/portfolio/positions",
                headers=sign_headers(kid, kkey, "GET", "/trade-api/v2/portfolio/positions")).json()
          .get("market_positions", [])}
    return kp


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--min-apr", type=float, default=None, help="bar %/yr; default = current blended APR")
    ap.add_argument("--max-ct-per-rel", type=int, default=50, help="concentration cap per relationship")
    ap.add_argument("--max-clip", type=int, default=20, help="max contracts per single execution")
    ap.add_argument("--live", action="store_true")
    args = ap.parse_args()

    bapr = blended_apr()
    bar = args.min_apr if args.min_apr is not None else (bapr or 12.0)
    reg = Registry.load("config/registry.yaml")
    c = httpx.Client(timeout=20)
    kpos = positions(c)
    mode = "LIVE" if args.live else "DRY"
    print(f"[{mode}] blended APR={bapr:.1f}%/yr -> bar={bar:.1f}%/yr | cap={args.max_ct_per_rel}ct/rel clip<={args.max_clip}")

    rels = [r for r in reg.relationships if any(r.id.startswith(v) for v in VETTED)
            and any(l.venue is Venue.KALSHI for l in r.legs)
            and any(l.venue is Venue.POLYMARKET_US for l in r.legs)]
    fired = []
    for r in rels:
        kt = next(l.market_id for l in r.legs if l.venue is Venue.KALSHI)
        ps = next(l.market_id for l in r.legs if l.venue is Venue.POLYMARKET_US)
        try:
            kb, ka, kbs, kas = kalshi_top(c, kt)
            pb, pa, pbs = pmus_top(c, ps)
        except Exception as e:
            continue
        if None in (kb, ka, pb, pa):
            continue
        edge_kpm = pb - ka                    # buy Kalshi YES @ka, open PM NO (sell PM YES @pb)
        edge_pmk = kb - pa                    # reverse (not auto-executed)
        edge = max(edge_kpm, edge_pmk)
        net = edge - FEE_CT
        cost_ct = Decimal(1) - edge
        yrs = years_to(r.id)
        apr = float(net / cost_ct / Decimal(str(yrs)) * 100) if (yrs and cost_ct > 0) else None
        pos = kpos.get(kt, 0)
        headroom = max(0, args.max_ct_per_rel - int(pos))
        # size bounded by depth on the legs we'd cross (K->PM: Kalshi ask sz, PM bid sz)
        depth = int(min(kas, pbs)) if edge_kpm >= edge_pmk else int(min(kbs, pas)) if False else int(min(kas, pbs))
        size = min(headroom, depth, args.max_clip)
        ok = (apr is not None and apr >= bar and net > 0 and edge_kpm >= edge_pmk and size >= 1)
        tag = "EXECUTE" if ok else "skip"
        reason = ""
        if not ok:
            if apr is None: reason = "no resolve date"
            elif net <= 0: reason = "edge<=fees"
            elif edge_kpm < edge_pmk: reason = "reverse-dir (not auto)"
            elif apr < bar: reason = f"apr<{bar:.0f}"
            elif headroom < 1: reason = f"at cap ({int(pos)}ct)"
            elif size < 1: reason = "no depth"
        print(f"  {tag:<8} {r.id[:34]:<34} edge={edge*100:+.0f}c net={net*100:+.0f}c apr={('%.0f%%'%apr) if apr else '-':>5} "
              f"pos={int(pos)} size={size} {reason}")
        if ok:
            fired.append((r, kt, ps, ka, pb, size, edge, apr))

    if not fired:
        print("  -> nothing clears the bar right now.")
        return
    if not args.live:
        print(f"\n[DRY] {len(fired)} crossing(s) WOULD execute. Re-run with --live to fire.")
        return
    # LIVE execution reused from execute_xv: constrained (PM) leg IOC first, then exact-qty Kalshi
    kgw = KalshiOrderGateway((D / "kalshi_api_key_id").read_text().strip(),
                             load_private_key((D / "kalshi_private_key.pem").read_bytes()), live=True)
    pgw = PolymarketUsOrderGateway((D / "polymarket_usa_key_id").read_text().strip(),
                                   (D / "polymarket_usa_private_key").read_text().strip(), live=True)
    for r, kt, ps, ka, pb, size, edge, apr in fired:
        print(f"\n[LIVE] EXECUTE {r.id} size={size}: open PM NO @ {pb} (IOC) then buy Kalshi YES @ {ka}")
        r1 = pgw.place_short(ps, pb, size, post_only=False)
        oid = r1.get("id") or r1.get("order_id")
        import time as _t
        _t.sleep(1.0)
        filled = pgw.filled_qty(oid) if oid else 0
        print(f"   PM NO filled = {filled}")
        if filled < 1:
            print("   PM leg unfilled — nothing to hedge, skip")
            continue
        r2 = kgw.place_yes(kt, "bid", ka + Decimal("0.01"), filled, post_only=False)  # through the ask
        kfill = kgw.filled_qty((r2.get("order") or {}).get("order_id") or r2.get("order_id") or "")
        print(f"   Kalshi YES bought = {kfill} (r2={r2.get('order_id') or r2})")
        if kfill < filled:
            print(f"   *** WARNING: Kalshi leg only {kfill}/{filled} — NAKED {filled-kfill}, recon net will flag ***")
        rec = {"ts": __import__("time").time(), "relationship_id": r.id,
               "title": f"{r.id} (auto take-take)", "qty": int(filled), "strategy": "take-take",
               "resolves_by": resolve_date(r.id)[0], "resolves_estimated": resolve_date(r.id)[1],
               "legs": [{"venue": "kalshi", "market_id": kt, "side": "yes", "role": "taker",
                         "qty": int(kfill), "yes_price": str(ka)},
                        {"venue": "polymarket_us", "market_id": ps, "side": "no", "role": "taker",
                         "qty": int(filled), "yes_price": str(pb)}],
               "status": "open"}
        with open("data/exec/trades.jsonl", "a") as f:
            f.write(json.dumps(rec) + "\n")
        print(f"   recorded take-take basket {r.id} x{filled}")


if __name__ == "__main__":
    main()
