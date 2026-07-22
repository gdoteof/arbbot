"""Proving-grounds trader runner (multi-relationship).

    python -m arbbot.exec.main --relationship RELA RELB ... [--live] [--clip 5]

DRY-RUN unless --live. Subscribes to the recorder's Unix socket, rebuilds
books, and drives one Quoter PER relationship off the shared feed — so a set of
make-take pairs runs in ONE process (one socket, shared gateways/risk, a single
kill switch) rather than N fragile background jobs. On --live it rehearses the
signed order path once per venue, then rests maker quotes and hedges (taker) on
the other leg when a maker order fills.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import time
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_gateway import PolymarketOrderGateway
from arbbot.exec.quoter import Quoter
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookDelta, BookSnapshot, Venue
from arbbot.ops.alerts import Alerter
from arbbot.ops.config import load_credential, load_recorder_config
from arbbot.exec.resolve_dates import resolve_date
from arbbot.record.jsonl import parse_event
from arbbot.record.kalshi import REST_BASE, KalshiCatalog, load_private_key, sign_headers
from arbbot.registry.model import Registry
from arbbot.risk.manager import RiskConfig, RiskManager

# --- trigger-based take-take auto-execution (riskless cross-venue arb) ---
TT_VETTED = ("xvus-time-poty-26", "xvus-france-pres-27", "xvus-brazil-pres-26", "xvus-fedcut-26")
TT_FEE = Decimal("0.02")     # ~both-leg taker fees per contract (conservative)
TT_MAX_CLIP = 10             # max contracts per single take-take execution
TT_CAP = 50                  # per-relationship concentration cap (contracts)
TT_COOLDOWN = 30.0           # s between fires on the same relationship


def _record_maker_fill(cfg, rel, i, side, filled, maker_px, hedge_res, latency_ms=None,
                       hedge_gw=None) -> None:
    """Append a captured make-take basket to data/exec/trades.jsonl (dashboard
    Trades tab). Maker leg = the quoted leg (fee-free); hedge leg = taker."""
    maker_leg = rel.legs[i]
    hedge_leg = rel.legs[1 - i]
    ho = hedge_res.get("hedge_order") or {}
    # PM US create responses omit execution data (fills report async), which
    # recorded avg_price=0/fees=0 AND understated cost_usd on Kalshi-maker
    # fills — re-fetch the authoritative order for its avgPx + commission.
    # The fill data lands asynchronously (an immediate re-fetch still shows
    # avgPx=0), so retry briefly until cumQuantity reflects the fill.
    if not ho.get("average_fill_price") and ho.get("id") and hedge_gw is not None \
            and hasattr(hedge_gw, "get_order"):
        with contextlib.suppress(Exception):
            for _ in range(5):
                o = hedge_gw.get_order(ho["id"])
                if float(o.get("cumQuantity") or 0) >= 1 and (o.get("avgPx") or {}).get("value"):
                    break
                time.sleep(0.3)
            ho = {"order_id": o.get("id"),
                  "average_fill_price": (o.get("avgPx") or {}).get("value"),
                  # PM commission is a TOTAL — pre-divide so the ×filled below holds
                  "average_fee_paid": (Decimal(str((o.get("commissionNotionalTotalCollected") or {}).get("value") or 0))
                                       / filled if filled else 0)}
    h_px = Decimal(str(ho.get("average_fill_price") or 0))
    h_fee = Decimal(str(ho.get("average_fee_paid") or 0)) * filled
    maker_px = Decimal(str(maker_px))
    # maker ask opens NO at cost (1 - px); a maker bid buys YES at px
    maker_cost = (Decimal(1) - maker_px) * filled if side == "ask" else maker_px * filled
    # hedge capital: maker ask -> hedge BUYS YES at h_px; maker bid -> hedge
    # SELLS YES (= opens NO) so its capital is 1 - h_px, not h_px.
    h_cost = h_px * filled if side == "ask" else (Decimal(1) - h_px) * filled
    cost = float(h_cost + h_fee + maker_cost)
    from arbbot.exec.resolve_dates import resolve_date
    rdate, rest = resolve_date(rel.id)
    rec = {"ts": time.time(), "relationship_id": rel.id, "title": rel.id,
           "qty": int(filled), "strategy": "make-take",
           "resolves_by": rdate, "resolves_estimated": rest,
           "legs": [
               {"venue": maker_leg.venue.value, "market_id": maker_leg.market_id,
                "side": "no" if side == "ask" else "yes", "role": "maker",
                "qty": int(filled), "yes_price": str(maker_px), "cost": str(maker_cost)},
               {"venue": hedge_leg.venue.value, "market_id": hedge_leg.market_id,
                "side": "yes" if side == "ask" else "no", "role": "taker",
                "qty": int(filled), "avg_price": str(h_px), "fees": str(h_fee),
                "order_id": ho.get("order_id")},
           ],
           "cost_usd": cost, "payoff_usd": float(filled),
           "profit_usd": float(filled) - cost, "status": "open",
           "hedge_latency_ms": (round(latency_ms, 1) if latency_ms is not None else None)}
    p = Path("data/exec/trades.jsonl")
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "a") as f:
        f.write(json.dumps(rec) + "\n")


async def run(rel_ids: list[str], live: bool, clip: int, config_path: str) -> None:
    cfg = load_recorder_config(config_path)
    reg = Registry.load(cfg.registry_path)
    rels = [next(r for r in reg.relationships if r.id == rid) for rid in rel_ids]
    all_legs = [l for r in rels for l in r.legs]
    venues = {l.venue for l in all_legs}

    gateways = {}
    kid = load_credential("kalshi_api_key_id")
    pem = load_credential("kalshi_private_key.pem")
    if Venue.KALSHI in venues:
        assert kid and pem, "Kalshi leg needs the trade key via LoadCredential"
        gateways[Venue.KALSHI] = KalshiOrderGateway(
            kid.decode().strip(), load_private_key(pem), live=live)
    if Venue.POLYMARKET in venues:
        pm_key = load_credential("polymarket_private_key")
        assert pm_key, "PM leg needs polymarket_private_key via LoadCredential"
        gateways[Venue.POLYMARKET] = PolymarketOrderGateway(pm_key.decode().strip(), live=live)
    pmus_kid = pmus_key = None  # captured for the private-WS fill listener
    if Venue.POLYMARKET_US in venues:
        from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
        us_id = load_credential("polymarket_usa_key_id")
        us_key = load_credential("polymarket_usa_private_key")
        assert us_id and us_key, "PM US leg needs polymarket_usa_key_id + _private_key"
        pmus_kid, pmus_key = us_id.decode().strip(), us_key.decode().strip()
        gateways[Venue.POLYMARKET_US] = PolymarketUsOrderGateway(
            pmus_kid, pmus_key, live=live)

    # market metadata for ALL legs across all relationships
    markets = {}
    from arbbot.record.polymarket import GammaCatalog, ClobRest
    from arbbot.record.polymarket_us import PolymarketUsCatalog
    kt = sorted({l.market_id for l in all_legs if l.venue is Venue.KALSHI})
    pt = sorted({l.market_id for l in all_legs if l.venue is Venue.POLYMARKET})
    put = {l.market_id for l in all_legs if l.venue is Venue.POLYMARKET_US}
    if kt:
        for m in await KalshiCatalog().markets(kt):
            markets[(m.venue, m.market_id)] = m
    if pt:
        pm = await GammaCatalog().markets_by_token_ids(pt)
        pm = await ClobRest().taker_fee_overrides(pm)
        for m in pm:
            markets[(m.venue, m.market_id)] = m
    if put:
        for m in await PolymarketUsCatalog().markets_by_tags(cfg.polymarket_us_tags or ["fed-decision", "cpi"]):
            if m.market_id in put:
                markets[(m.venue, m.market_id)] = m
        missing = put - {mid for (v, mid) in markets if v is Venue.POLYMARKET_US}
        if missing:
            print(f"WARNING: no PM US metadata for {missing} — check polymarket_us_tags", flush=True)

    risk = RiskManager(config=RiskConfig(
        bankroll=Decimal("980"),            # actual capital (~$800 idle + ~$180 locked)
        per_rel_cap=Decimal("150"),         # Geoff-approved max per name (oracle-scaled)
        kill_switch_file="data/KILL"))
    import httpx
    risk.balances = {}
    if Venue.KALSHI in venues:
        from arbbot.record.kalshi import sign_headers, REST_BASE
        hdr = sign_headers(kid.decode().strip(), load_private_key(pem), "GET",
                           "/trade-api/v2/portfolio/balance")
        bal = httpx.get(REST_BASE + "/portfolio/balance", headers=hdr, timeout=15).json()
        risk.balances[Venue.KALSHI] = Decimal(bal["balance_dollars"])
    if Venue.POLYMARKET in venues:
        from py_clob_client.client import ClobClient
        from py_clob_client.clob_types import BalanceAllowanceParams, AssetType
        pmk = load_credential("polymarket_private_key").decode().strip()
        cc = ClobClient("https://clob.polymarket.com",
                        key=pmk if pmk.startswith("0x") else "0x"+pmk, chain_id=137)
        cc.set_api_creds(cc.create_or_derive_api_creds())
        col = cc.get_balance_allowance(BalanceAllowanceParams(asset_type=AssetType.COLLATERAL))
        risk.balances[Venue.POLYMARKET] = Decimal(col["balance"]) / Decimal(10**6)
    if Venue.POLYMARKET_US in venues:
        gw = gateways[Venue.POLYMARKET_US]
        bal = httpx.get("https://api.polymarket.us/v1/account/balances",
                        headers=gw._headers("GET", "/v1/account/balances"), timeout=15).json()
        risk.balances[Venue.POLYMARKET_US] = Decimal(str(bal["balances"][0]["buyingPower"]))

    # quote_venue=None => quote EVERY maker-eligible leg (both venues), both
    # sides; each quote is gated by net-of-fee edge in _target, so Kalshi rests
    # a maker order whenever its edge after the Kalshi maker fee beats PM US.
    # size jitter only (anti-fingerprint): vary clip within [clip-2, clip].
    # Price jitter and deadband hysteresis both removed — Geoff judged anything
    # that rests off the optimal price costs top-of-book priority on these thin
    # books; the 15s min_requote_s throttle alone handles churn.
    quoters = {r.id: Quoter(rel=r, gateways=gateways, risk=risk, markets=markets,
                            fees=FeeSchedule(), clip=clip, safety_ticks=0,
                            quote_venue=None, size_jitter=2) for r in rels}
    rel_by_id = {r.id: r for r in rels}
    alerter = Alerter(cfg.ntfy_topic)

    # seed exposure from the trades ledger so per-name caps survive restarts —
    # without this, exposure resets to zero every restart and the cap becomes
    # "per session" (Mamdani reached $78 against a $10 design cap that way).
    ledger = Path("data/exec/trades.jsonl")
    if ledger.exists():
        for line in ledger.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                t = json.loads(line)
            except ValueError:
                continue
            rel = rel_by_id.get(t.get("relationship_id"))
            if rel is not None and t.get("status") == "open":
                risk.record_open(rel, Decimal(str(t.get("qty", 0))))
        seeded = {k: str(v) for k, v in risk.exposure.by_relationship.items()}
        if seeded:
            print(f"seeded open exposure from ledger: {seeded}", flush=True)

    mode = "LIVE" if live else "DRY-RUN"
    balstr = " ".join(f"{v.value}=${b}" for v, b in risk.balances.items())
    print(f"[{mode}] {len(rels)} quoters clip={clip} {balstr}", flush=True)
    for r in rels:
        legs = "+".join(sorted({l.venue.value for l in r.legs}))
        print(f"   {r.id} maker-eligible={legs}", flush=True)

    if live:
        # rehearse the signed order path ONCE per venue (dedup) — 1ct
        # far-from-market place/confirm/cancel; abort all if any venue fails
        done = set()
        for l in all_legs:
            if l.venue in done:
                continue
            gw = gateways.get(l.venue)
            if gw is None or not hasattr(gw, "rehearse"):
                continue
            done.add(l.venue)
            reh = gw.rehearse(l.market_id)
            print(f"[LIVE] rehearsal {l.venue.value}: {reh}", flush=True)
            if not reh.get("rested"):
                print(f"[LIVE] rehearsal FAILED on {l.venue.value} — ABORTING", flush=True)
                alerter.alert(f"arbbot trader: rehearsal FAILED {l.venue.value}")
                return

    # route each market's events to the relationships that reference it
    leg_to_rels: dict[tuple[str, str], list[str]] = defaultdict(list)
    for r in rels:
        for l in r.legs:
            leg_to_rels[(l.venue.value, l.market_id)].append(r.id)

    books = BookBuilder()
    # cumulative qty already hedged per resting order_id — shared by the
    # event-driven WS path and the poll fallback so a fill is hedged EXACTLY
    # once (whichever sees it first hedges the increment; the other sees none).
    hedged_by_oid: dict[str, int] = {}

    def _process_fill(rid: str, i: int, side: str, rq, cum: int) -> None:
        """Hedge the newly-filled increment of a resting maker order and record
        it. Idempotent across WS+poll via hedged_by_oid; pops the quote once
        fully filled so on_book requotes fresh. cum = cumulative filled qty."""
        q = quoters[rid]
        r = rel_by_id[rid]
        oid = rq.order_id
        new = int(cum) - hedged_by_oid.get(oid, 0)
        if new >= 1:
            t0 = time.time()
            res = q.hedge_fill(i, side, books, new)
            lat_ms = (time.time() - t0) * 1000.0  # detect->hedge-confirmed = exposure window
            hedged_by_oid[oid] = int(cum)
            print(f"[LIVE] maker FILL {rid} {r.legs[i].market_id} {side} "
                  f"+{new} (cum {cum}) -> hedge {res} ({lat_ms:.0f}ms)", flush=True)
            alerter.alert(f"arbbot make-take FILL {rid} {side} x{new} "
                          f"hedged={res.get('hedged')} {lat_ms:.0f}ms")
            _record_maker_fill(cfg, r, i, side, new, rq.price, res, lat_ms,
                               hedge_gw=q.gateways.get(r.legs[1 - i].venue))
        if int(cum) >= rq.count:  # fully filled -> stop resting; on_book requotes
            q._resting.pop((i, side), None)
            q._last_quote_ts.pop((i, side), None)
            hedged_by_oid.pop(oid, None)

    # ---- trigger-based take-take: on EVERY book tick, check the riskless
    # cross-venue crossing; fire if its net-of-fee APR beats our blended APR.
    import datetime as _dt
    tt = {"bar": 12.0, "pos": {}, "last_fire": {}, "last_refresh": 0.0}
    tt_rel_ids = {r.id for r in rels if any(r.id.startswith(v) for v in TT_VETTED)}

    def _tt_refresh():
        # bar = blended portfolio APR (marks.json); pos = live Kalshi positions (caps)
        try:
            m = json.loads((Path("data/exec/marks.json")).read_text())
            today = _dt.date.today()
            num = den = cost = prof = 0.0
            for p in m.get("positions", []):
                cc, pr, rb = p.get("cost_usd", 0), p.get("locked_profit_usd", 0), p.get("resolves_by")
                cost += cc; prof += pr
                if rb and cc:
                    try:
                        yy = max((_dt.date.fromisoformat(str(rb)[:10]) - today).days, 1) / 365.25
                    except ValueError:
                        continue
                    num += cc * yy; den += cc
            if cost and den:
                tt["bar"] = prof / cost / (num / den) * 100
        except Exception:
            pass
        kgw = gateways.get(Venue.KALSHI)
        if kgw is not None:
            with contextlib.suppress(Exception):
                import httpx as _h
                hdr = sign_headers(kgw.kid, kgw.key, "GET", "/trade-api/v2/portfolio/positions")
                pj = _h.get(REST_BASE + "/portfolio/positions", headers=hdr, timeout=10).json()
                tt["pos"] = {p["ticker"]: abs(float(p.get("position_fp") or 0))
                             for p in pj.get("market_positions", [])}

    def _take_take(rid: str) -> None:
        rel = rel_by_id[rid]
        kleg = next((l for l in rel.legs if l.venue is Venue.KALSHI), None)
        pleg = next((l for l in rel.legs if l.venue is Venue.POLYMARKET_US), None)
        if not kleg or not pleg:
            return
        kbook = books.get("kalshi", kleg.market_id)
        pbook = books.get("polymarket_us", pleg.market_id)
        if kbook is None or pbook is None:
            return
        ka, pb = kbook.best_ask(), pbook.best_bid()
        if ka is None or pb is None:
            return
        edge = pb.price - ka.price            # K->PM: buy Kalshi YES @ka, open PM NO @pb
        net = edge - TT_FEE
        if net <= 0:
            return
        rd, _est = resolve_date(rid)
        if not rd:
            return
        yrs = max((_dt.date.fromisoformat(rd) - _dt.date.today()).days, 1) / 365.25
        apr = float(net / (Decimal(1) - edge) / Decimal(str(yrs)) * 100)
        if apr < tt["bar"]:
            return
        if time.monotonic() - tt["last_fire"].get(rid, 0.0) < TT_COOLDOWN:
            return
        pos = tt["pos"].get(kleg.market_id, 0)
        size = min(TT_CAP - int(pos), int(min(ka.size, pb.size)), TT_MAX_CLIP)
        if size < 1:
            return
        tt["last_fire"][rid] = time.monotonic()
        pgw, kgw = gateways.get(Venue.POLYMARKET_US), gateways.get(Venue.KALSHI)
        if pgw is None or kgw is None:
            return
        print(f"[LIVE] TAKE-TAKE {rid} edge={edge*100:.0f}c apr={apr:.0f}% bar={tt['bar']:.0f}% "
              f"size={size}: PM NO @ {pb.price} then Kalshi YES @ {ka.price}", flush=True)
        try:  # legging-safe: constrained PM leg IOC first, then EXACT-qty Kalshi
            r1 = pgw.place_short(pleg.market_id, pb.price, size, post_only=False)
            oid1 = r1.get("id") or r1.get("order_id")
            filled = pgw.filled_qty(oid1) if oid1 else 0
            if filled < 1:
                print(f"[LIVE] TAKE-TAKE {rid} PM leg unfilled — abort (nothing naked)", flush=True)
                return
            kgw.place_yes(kleg.market_id, "bid", ka.price + _tick_k(kleg), filled, post_only=False)
            tt["pos"][kleg.market_id] = int(pos) + filled  # local until next refresh
            alerter.alert(f"arbbot TAKE-TAKE {rid} x{filled} edge={edge*100:.0f}c apr={apr:.0f}%")
            rec = {"ts": time.time(), "relationship_id": rid, "title": f"{rid} (auto take-take)",
                   "qty": int(filled), "strategy": "take-take",
                   "resolves_by": rd, "resolves_estimated": _est,
                   "legs": [{"venue": "kalshi", "market_id": kleg.market_id, "side": "yes",
                             "role": "taker", "qty": int(filled), "yes_price": str(ka.price)},
                            {"venue": "polymarket_us", "market_id": pleg.market_id, "side": "no",
                             "role": "taker", "qty": int(filled), "yes_price": str(pb.price)}],
                   "cost_usd": float((ka.price + (Decimal(1) - pb.price)) * filled),
                   "payoff_usd": float(filled),
                   "profit_usd": float((Decimal(1) - ka.price - (Decimal(1) - pb.price)) * filled),
                   "status": "open"}
            with open(Path("data/exec/trades.jsonl"), "a") as f:
                f.write(json.dumps(rec) + "\n")
            print(f"[LIVE] TAKE-TAKE filled+recorded {rid} x{filled}", flush=True)
        except Exception as e:
            print(f"[LIVE] TAKE-TAKE {rid} EXEC ERROR {type(e).__name__}: {e} "
                  f"— 60s recon net will flag any naked leg", flush=True)

    def _tick_k(leg):
        m = markets.get((leg.venue, leg.market_id))
        return m.tick_size if m else Decimal("0.01")

    async def pmus_fill_ws() -> None:
        """Event-driven fill detection: PM US pushes an order execution the
        instant our resting quote fills (sub-second), vs the ~2s poll. Hedges
        immediately. The poll below stays as a fallback for WS gaps."""
        import websockets
        from arbbot.record.polymarket_us import ws_auth_headers
        path = "/v1/ws/private"
        url = "wss://api.polymarket.us" + path
        while True:
            try:
                async with websockets.connect(
                        url, additional_headers=ws_auth_headers(pmus_kid, pmus_key, path),
                        ping_interval=20, open_timeout=10) as ws:
                    await ws.send(json.dumps({"subscribe": {
                        "requestId": "orders",
                        "subscriptionType": "SUBSCRIPTION_TYPE_ORDER"}}))
                    print("[LIVE] PM US private WS connected — event-driven fills", flush=True)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        ex = (msg.get("orderSubscriptionUpdate") or {}).get("execution") or {}
                        if ex.get("type") not in ("EXECUTION_TYPE_FILL", "EXECUTION_TYPE_PARTIAL_FILL"):
                            continue
                        order = ex.get("order") or {}
                        oid = order.get("id")
                        try:
                            cum = int(float(order.get("cumQuantity") or 0))
                        except (TypeError, ValueError):
                            continue
                        if not oid or cum < 1:
                            continue
                        for rid, q in quoters.items():
                            match = next(((i, side, rq) for (i, side), rq
                                          in list(q._resting.items()) if rq.order_id == oid), None)
                            if match:
                                _process_fill(rid, match[0], match[1], match[2], cum)
                                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[LIVE] PM US private WS dropped: {type(e).__name__}: {e}; reconnecting",
                      flush=True)
                await asyncio.sleep(2)

    async def socket_loop() -> None:
        last_flush = 0.0
        last_fill_check = 0.0
        last_hb = time.monotonic()
        hb_events = hb_onbook = 0
        if live:
            _tt_refresh()  # populate bar + position caps before the first tick
        while True:
            writer = None
            try:
                reader, writer = await asyncio.open_unix_connection(cfg.socket_path)
                while True:
                    line = await reader.readline()
                    if not line:
                        break
                    hb_events += 1
                    # liveness heartbeat: quiet-vs-stuck is unanswerable without
                    # this — books can legitimately sleep for long stretches.
                    if time.monotonic() - last_hb > 60:
                        n_rest = sum(len(q._resting) for q in quoters.values())
                        print(f"[hb] events/min={hb_events} on_book={hb_onbook} "
                              f"resting={n_rest}", flush=True)
                        last_hb = time.monotonic()
                        hb_events = hb_onbook = 0
                    try:
                        ev = parse_event(json.loads(line))
                    except ValueError:
                        continue  # truncated line at recorder cut-over
                    if isinstance(ev, BookSnapshot):
                        books.apply_snapshot(ev)
                    elif isinstance(ev, BookDelta):
                        with contextlib.suppress(GapDetected, NotSynced):
                            books.apply_delta(ev)
                    else:
                        continue
                    for rid in leg_to_rels.get((ev.venue.value, ev.market_id), ()):
                        q = quoters[rid]
                        hb_onbook += 1
                        try:
                            q.on_book(books)
                        except Exception as e:  # never let one quote error kill the loop; SURFACE it
                            print(f"[on_book error {rid}] {type(e).__name__}: {e}", flush=True)
                        for a in q.overdue_alarms(time.time()):
                            alerter.alert(f"arbbot trader: {a}")
                        # trigger-based take-take on this book tick (sub-second)
                        if live and rid in tt_rel_ids:
                            try:
                                _take_take(rid)
                            except Exception as e:
                                print(f"[take-take error {rid}] {type(e).__name__}: {e}", flush=True)
                    now = time.monotonic()
                    if live and now - tt["last_refresh"] > 60:
                        tt["last_refresh"] = now
                        _tt_refresh()
                    # fallback fill detection (WS is primary): poll each resting
                    # order every 2s; _process_fill dedupes vs the WS path.
                    if live and now - last_fill_check > 2.0:
                        last_fill_check = now
                        for rid, q in quoters.items():
                            r = rel_by_id[rid]
                            for (i, side), rq in list(q._resting.items()):
                                gw = gateways.get(r.legs[i].venue)
                                if gw is None or not hasattr(gw, "filled_qty"):
                                    continue
                                with contextlib.suppress(Exception):
                                    filled = gw.filled_qty(rq.order_id)
                                    if filled >= 1:
                                        _process_fill(rid, i, side, rq, filled)
                    if now - last_flush > 10:
                        for rid, q in quoters.items():
                            if q.intents:
                                with open(Path(cfg.scan_dir) / f"trader-intents-{rid}.jsonl", "a") as f:
                                    for it in q.intents:
                                        f.write(json.dumps(it) + "\n")
                                q.intents.clear()
                        last_flush = now
            except (ConnectionError, FileNotFoundError, OSError):
                pass
            finally:
                # close the old connection before reconnecting — leaking it
                # leaves a zombie the recorder keeps writing into (Send-Q grows
                # until it notices and drops the client).
                if writer is not None:
                    with contextlib.suppress(Exception):
                        writer.close()
            await asyncio.sleep(2)

    async def conn_warmer() -> None:
        """Keep each gateway's HTTP pool warm: a cold TCP+TLS handshake adds
        40-70ms to a hedge (measured 2026-07-22) — more than the entire warm
        hedge round-trip. A cheap authenticated GET every 25s holds keep-alive
        below typical idle timeouts."""
        print("[LIVE] connection warmer on (25s/gateway)", flush=True)
        while True:
            await asyncio.sleep(25)
            for v, gw in gateways.items():
                with contextlib.suppress(Exception):
                    if v is Venue.KALSHI:
                        gw.client.get(gw.base + "/portfolio/balance",
                                      headers=gw._headers("GET", "/portfolio/balance"))
                    elif v is Venue.POLYMARKET_US:
                        gw.client.get(gw.base + "/v1/account/balances",
                                      headers=gw._headers("GET", "/v1/account/balances"))

    tasks = [socket_loop()]
    if live and Venue.POLYMARKET_US in gateways and pmus_kid and pmus_key:
        tasks.append(pmus_fill_ws())
    if live:
        tasks.append(conn_warmer())
    try:
        await asyncio.gather(*tasks)
    finally:
        # graceful shutdown: cancel every resting quote + sweep both venues so a
        # restart never orphans a live untracked order (naked-fill risk).
        if live:
            for q in quoters.values():
                with contextlib.suppress(Exception):
                    q.cancel_all()
            for gw in gateways.values():
                if hasattr(gw, "cancel_all_open"):
                    with contextlib.suppress(Exception):
                        gw.cancel_all_open()
            print("[LIVE] shutdown: cancelled all resting orders", flush=True)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--relationship", required=True, nargs="+",
                    help="one or more relationship ids to quote in this process")
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--clip", type=int, default=5)
    ap.add_argument("--config", default="config/recorder.yaml")
    args = ap.parse_args()
    # translate SIGTERM (kill) into KeyboardInterrupt so the run() finally-block
    # runs its graceful order-cancel on restart instead of dying mid-flight.
    import signal
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        asyncio.run(run(args.relationship, args.live, args.clip, args.config))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
