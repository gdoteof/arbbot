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
from decimal import ROUND_UP, Decimal
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
TT_MAX_CLIP = 100            # safety ceiling only (card 938df47b: depth-aware
                             # sizing) — effective size is bounded by displayed
                             # depth min(ka.size, pb.size), TT_CAP, and the
                             # risk manager's per-rel/topic/class caps
TT_CAP = 50                  # per-relationship concentration cap (contracts)
TT_COOLDOWN = 30.0           # s between fires on the same relationship
# floating APR bar (Geoff 2026-07-22): scales with capital utilization of the
# class budget — near-idle capital should grab modest APRs (floor ~= the
# trivially-redeployable yield), while a full book demands fresh take-take
# beat what maker capital earns before crowding it out. Linear in utilization;
# Geoff's 8% reference sits at ~1/3 utilization.
TT_APR_FLOOR = 4.0
TT_APR_CEIL = 16.0


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
        # per-attempt try/except: one 429/timeout must not abandon the refetch
        # (2026-07-22: a suppressed first-attempt 429 left avg_price=0 /
        # order_id=null in the ledger and a -$1 phantom loss on the dash)
        o = None
        for _ in range(8):
            try:
                o = hedge_gw.get_order(ho["id"])
                if float(o.get("cumQuantity") or 0) >= 1 and (o.get("avgPx") or {}).get("value"):
                    break
            except Exception:
                pass
            time.sleep(0.6)
        if o is not None and (o.get("avgPx") or {}).get("value"):
            ho = {"order_id": o.get("id"),
                  "average_fill_price": (o.get("avgPx") or {}).get("value"),
                  # PM commission is a TOTAL — pre-divide so the ×filled below holds
                  "average_fee_paid": (Decimal(str((o.get("commissionNotionalTotalCollected") or {}).get("value") or 0))
                                       / filled if filled else 0)}
        else:
            # venue unreachable: fall back to the book price the hedge was
            # placed at (close to truth, never a fabricated 0) and flag it
            ho = {"order_id": ho.get("id"),
                  "average_fill_price": hedge_res.get("px"),
                  "average_fee_paid": 0, "price_estimated": True}
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
                "order_id": ho.get("order_id"),
                **({"price_estimated": True} if ho.get("price_estimated") else {})},
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
    # HARD tradable gate (card c9ac7d1d): quote only what a human vetted
    # (registry vetted_by) or what config/tradable.yaml explicitly allowlists
    # with provenance (pre-field approvals). Convention is not a gate.
    try:
        import yaml as _y
        _allow = set((_y.safe_load(Path("config/tradable.yaml").read_text())
                      or {}).get("allow") or [])
    except (OSError, ValueError):
        _allow = set()
    _dropped = [r for r in rels
                if r.vetted_by.value != "human" and r.id not in _allow]
    for _r in _dropped:
        print(f"[GATE] {_r.id} NOT tradable (vetted_by={_r.vetted_by.value}, "
              f"not allowlisted) — excluded from quoting", flush=True)
    rels = [r for r in rels if r not in _dropped]
    assert rels, "no tradable relationships survive the vetting gate"
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

    # topic budgets: config/topics.yaml (untracked — reveals family strategy)
    topics_cfg = {}
    tf = Path("config/topics.yaml")
    if tf.exists():
        import yaml as _yaml
        topics_cfg = _yaml.safe_load(tf.read_text()) or {}
    risk = RiskManager(config=RiskConfig(
        bankroll=Decimal("980"),            # actual capital (~$800 idle + ~$180 locked)
        per_rel_cap=Decimal("150"),         # Geoff-approved max per name (oracle-scaled)
        topics=topics_cfg.get("topics", []),
        default_topic_budget=topics_cfg.get("default_topic_budget"),
        default_only_below_util=topics_cfg.get("default_only_below_util"),
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
    # maker APR hurdle (card 80ff7987): each quoter knows its time-to-resolve
    # so _target can require the locked edge to annualize above the floating
    # bar (propagated in _tt_refresh). No date => no hurdle (edge-only gating).
    for rid2, q2 in quoters.items():
        rd2, _ = resolve_date(rid2)
        if rd2:
            import datetime as _dt2
            q2.resolve_years = max(
                (_dt2.date.fromisoformat(rd2) - _dt2.date.today()).days, 1) / 365.25
    alerter = Alerter(cfg.ntfy_topic)

    # seed exposure from the trades ledger so per-name caps survive restarts —
    # without this, exposure resets to zero every restart and the cap becomes
    # "per session" (Mamdani reached $78 against a $10 design cap that way).
    from arbbot.exec.ledger import open_baskets, parse_lines
    ledger = Path("data/exec/trades.jsonl")
    tt_fired_seed: dict[str, int] = {}  # PM slug -> open take-take qty (cap floor)
    if ledger.exists():
        # net of appended unwind records — an unwound basket frees exposure/cap
        for t in open_baskets(parse_lines(ledger.read_text().splitlines())):
            rel = rel_by_id.get(t.get("relationship_id"))
            if rel is None:
                continue
            risk.record_open(rel, Decimal(str(t.get("qty", 0))))
            if t.get("strategy") == "take-take":
                for l in t.get("legs", []):
                    if l.get("venue") == "polymarket_us":
                        tt_fired_seed[l["market_id"]] = (
                            tt_fired_seed.get(l["market_id"], 0) + int(t.get("qty", 0)))
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
            # a 429 here is throttling from our own restart churn, not a broken
            # order path — back off and retry instead of dying at startup
            reh = None
            throttled = False
            for attempt in range(3):
                try:
                    reh = gw.rehearse(l.market_id)
                    break
                except Exception as e:
                    throttled = "429" in str(e)
                    wait = 60 * (attempt + 1)
                    print(f"[LIVE] rehearsal {l.venue.value} attempt {attempt+1} failed "
                          f"({type(e).__name__}: {e}) — retrying in {wait}s", flush=True)
                    await asyncio.sleep(wait)
            if reh is None:
                if throttled:
                    # a 429 PROVES the signed path works (authenticated,
                    # understood, throttled) — venue rate limiting must not
                    # keep the trader down (2026-07-23: shared API budget with
                    # the research probes caused persistent 429s at startup)
                    print(f"[LIVE] rehearsal {l.venue.value} rate-limited — order path "
                          f"auth verified by the 429 itself; proceeding", flush=True)
                    continue
                print(f"[LIVE] rehearsal {l.venue.value} unreachable after retries — ABORTING", flush=True)
                alerter.alert(f"arbbot trader: rehearsal unreachable {l.venue.value}")
                return
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
    # "fired" = cumulative PM take-take qty per slug, never overwritten by
    # refresh and seeded from the ledger at startup: the PM positions API
    # transiently returns empty (seen 2026-07-22) and a glitched-empty ppos
    # must not free phantom cap headroom — not even in a fresh session.
    tt = {"bar": 12.0, "pos": {}, "ppos": {}, "fired": dict(tt_fired_seed),
          "last_fire": {}, "last_refresh": 0.0}
    tt_rel_ids = {r.id for r in rels if any(r.id.startswith(v) for v in TT_VETTED)}

    def _tt_refresh():
        # bar floats with utilization of the class budget (see TT_APR_FLOOR);
        # pos = live venue positions (caps)
        cap = float(risk.config.bankroll * risk.config.per_class_cap)
        util = min(max(float(risk.exposure.total) / cap if cap else 1.0, 0.0), 1.0)
        tt["bar"] = TT_APR_FLOOR + (TT_APR_CEIL - TT_APR_FLOOR) * util
        for q4 in quoters.values():  # makers clear the same bar (card 80ff7987)
            q4.min_apr = tt["bar"]
        kgw = gateways.get(Venue.KALSHI)
        if kgw is not None:
            with contextlib.suppress(Exception):
                import httpx as _h
                hdr = sign_headers(kgw.kid, kgw.key, "GET", "/trade-api/v2/portfolio/positions")
                pj = _h.get(REST_BASE + "/portfolio/positions", headers=hdr, timeout=10).json()
                tt["pos"] = {p["ticker"]: abs(float(p.get("position_fp") or 0))
                             for p in pj.get("market_positions", [])}
        # PM-side net positions too: the cap must see a PM leg whose Kalshi
        # hedge never landed (2026-07-22: wrong "unfilled" abort left naked PM
        # shorts invisible to a Kalshi-only cap, which then kept re-firing).
        pmgw = gateways.get(Venue.POLYMARKET_US)
        if pmgw is not None and hasattr(pmgw, "_headers"):
            with contextlib.suppress(Exception):
                pj = pmgw.client.get(pmgw.base + "/v1/portfolio/positions",
                                     headers=pmgw._headers("GET", "/v1/portfolio/positions")).json()
                newp = {k: abs(float(v.get("netPosition") or 0))
                        for k, v in pj.get("positions", {}).items()}
                # the PM positions API transiently returns EMPTY (seen 4x on
                # 2026-07-22, let a capped rel re-fire) — an empty read while we
                # hold prior state is a glitch, never a real flat portfolio
                if newp or not tt["ppos"]:
                    tt["ppos"] = newp
                else:
                    print("[LIVE] PM positions read empty — keeping previous (glitch guard)",
                          flush=True)

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
        pos = max(tt["pos"].get(kleg.market_id, 0), tt["ppos"].get(pleg.market_id, 0),
                  tt["fired"].get(pleg.market_id, 0))
        size = min(TT_CAP - int(pos), int(min(ka.size, pb.size)), TT_MAX_CLIP)
        if size < 1:
            return
        # topic budget / utilization gate (same risk dimensions the maker
        # obeys) — take-take passes its APR so a GREAT opportunity may use the
        # bounded class-cap overflow (idle cash shouldn't watch a 25%+ APR go by)
        rd_check = risk.check_order(rel, Decimal(size), {}, opportunity_apr=apr)
        if not rd_check.allowed:
            tt["last_fire"][rid] = time.monotonic()  # throttle re-checks to the cooldown
            print(f"[LIVE] TAKE-TAKE {rid} blocked by risk: {rd_check.reasons}", flush=True)
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
            # PM fill reporting can lag the IOC by seconds — a single immediate
            # read said "unfilled" on orders that HAD filled (2026-07-22),
            # leaving naked PM shorts. Poll before concluding.
            filled = 0
            for _ in range(6):
                filled = pgw.filled_qty(oid1) if oid1 else 0
                if filled >= 1:
                    break
                time.sleep(0.5)
            if filled < 1:
                tt["fired"][pleg.market_id] = tt["fired"].get(pleg.market_id, 0) + size
                print(f"[LIVE] TAKE-TAKE {rid} PM leg reports unfilled after 3s — abort; "
                      f"counting attempted size against cap until recon verifies", flush=True)
                return
            kgw.place_yes(kleg.market_id, "bid", ka.price + _tick_k(kleg), filled, post_only=False)
            tt["pos"][kleg.market_id] = int(pos) + filled  # local until next refresh
            tt["fired"][pleg.market_id] = tt["fired"].get(pleg.market_id, 0) + filled
            risk.record_open(rel, Decimal(filled))  # keep the utilization bar honest in-session
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

    # ---- maker-unwind (card 1ed32918): rest passive EXIT asks on held ----
    # standard-direction baskets the unwind policy flags (soft or hard, from
    # marks.json) — capture maker spread on the way out instead of paying
    # taker. The 5-min taker unwinder stays the backstop for hard signals.
    # One basket per rel per cycle (oldest flagged); exit ask on the Kalshi
    # leg, on fill the PM NO closes taker-side; recon + hedge timer backstop
    # any legging miss. Entry quoter's ask side is suppressed while an exit
    # quote is resting (two order-owners on one side both react to fills).
    EXIT_LOCK = Decimal("0.005")    # min locked profit/ct vs basis
    EXIT_SLIP = Decimal("0.01")     # assume PM close 1 tick through the ask
    exit_q: dict[str, dict] = {}    # rid -> resting exit order state
    exit_ghost: dict[str, tuple] = {}  # rid -> (price, qty) of last cancelled exit
    exit_last = {"ts": 0.0}

    def _exit_drop(rid: str, st: dict, cancel: bool) -> None:
        kgw = gateways.get(Venue.KALSHI)
        if cancel and kgw is not None:
            with contextlib.suppress(Exception):
                kgw.cancel(st["order_id"])
            # remember the footprint for one refresh: the book's WS view lags
            # the cancel, and our own ghost must not read as competition
            exit_ghost[rid] = (st["price"], st["qty"])
            quoters[rid].intents.append(
                {"ts": time.time(), "cancel": st["kt"], "venue": "kalshi",
                 "side": "ask", "price": str(st["price"]),
                 "order_id": st["order_id"], "tag": "exit"})
        quoters[rid].suppress.discard((st["kt"], "ask"))
        exit_q.pop(rid, None)

    def _exit_kbook(kt: str):
        """REST orderbook for exit pricing. The in-memory book can carry
        phantom levels on quiet markets (suppressed gap deltas): a ghost of
        our own cancelled 10c ask survived restarts and priced the Hollande
        exit at 9c against a real 13c touch (2026-07-24). Passive exits are
        slow — venue truth beats microlatency here."""
        import httpx as _hx
        kgw2 = gateways.get(Venue.KALSHI)
        hdr = sign_headers(kgw2.kid, kgw2.key, "GET",
                           f"/trade-api/v2/markets/{kt}/orderbook")
        ob = (_hx.get(f"{REST_BASE}/markets/{kt}/orderbook", headers=hdr,
                      timeout=10).json().get("orderbook_fp")) or {}
        yes = [(Decimal(p), Decimal(s)) for p, s in ob.get("yes_dollars") or []]
        no = [(Decimal(p), Decimal(s)) for p, s in ob.get("no_dollars") or []]
        kb = max((p for p, s in yes if s > 0), default=None)
        asks = sorted((Decimal(1) - p, s) for p, s in no if s > 0)
        return kb, asks

    def _exit_refresh() -> None:
        if not live or time.monotonic() - exit_last["ts"] < 15:
            return
        exit_last["ts"] = time.monotonic()
        kgw = gateways.get(Venue.KALSHI)
        if kgw is None:
            return
        try:
            mdoc = json.loads(Path("data/exec/marks.json").read_text())
            recs = parse_lines(Path("data/exec/trades.jsonl").read_text().splitlines())
        except (OSError, ValueError):
            return
        # candidates come from marks' maker_exit_eligible (Geoff 2026-07-24:
        # the maker exit's OWN economics — locks >=0.5c/ct AND holding no
        # longer better — not the taker mark, which held positions whose
        # passive exit already locked profit). Live pricing below still
        # enforces profit vs the current book at placement time.
        flagged = {(m["relationship_id"], m.get("ts")): m
                   for m in mdoc.get("positions", [])
                   if m.get("maker_exit_eligible")}
        want: dict[str, dict] = {}
        # OLDEST eligible basket per rel, deterministically — an unstable pick
        # cancel/replaces every cycle, and each replace leaves a ghost of our
        # own order in the book view that we then undercut (2026-07-24: the
        # Hollande exit walked 12c -> 9c against a 13c competing ask)
        for b in sorted(open_baskets(recs), key=lambda b: b.get("ts") or 0):
            rid = b.get("relationship_id")
            if (rid, b.get("ts")) not in flagged or rid not in quoters or rid in want:
                continue
            kleg = next((l for l in b["legs"] if l["venue"] == "kalshi"), None)
            pleg = next((l for l in b["legs"] if l["venue"] == "polymarket_us"), None)
            if not kleg or not pleg or kleg.get("side") != "yes":
                continue  # standard direction only (v1)
            qty = int(b["qty"])
            if qty < 1:
                continue
            want[rid] = {"kt": kleg["market_id"], "ps": pleg["market_id"],
                         "qty": qty, "basket_ts": b["ts"],
                         "cost_ct": Decimal(str(b["cost_usd"])) / qty,
                         "title": b.get("title")}
        for rid, st in list(exit_q.items()):
            if rid not in want or want[rid]["basket_ts"] != st["basket_ts"]:
                _exit_drop(rid, st, cancel=True)
        for rid, w in want.items():
            st = exit_q.get(rid)
            pbook = books.get("polymarket_us", w["ps"])
            if pbook is None:
                continue
            pa = pbook.best_ask()
            try:
                kb_px, kasks = _exit_kbook(w["kt"])
            except Exception:
                continue  # venue read failed — next cycle retries
            if kb_px is None or pa is None or int(pa.size) < 1:
                continue
            # size to the basket; PM top-level depth only caps at PLACEMENT
            # (keying qty to live pa.size churned a cancel/replace every cycle
            # as PM's displayed size flapped — 2026-07-24). A thinner book at
            # fill time is handled by the partial-close alert + hedge timer.
            qty = st["qty"] if st else max(1, min(w["qty"], int(pa.size)))
            # profitable floor: fill px + (1 - pm close px) - basis >= EXIT_LOCK
            min_px = w["cost_ct"] - (Decimal(1) - pa.price) + EXIT_LOCK + EXIT_SLIP
            # competing ask with OUR footprint subtracted out: the current
            # resting exit AND the ghost of the last replaced/cancelled one —
            # WS book updates lag our cancels, and treating our own ghost as
            # competition walked the price down one tick per cycle to the
            # profit floor (2026-07-24 Hollande, 12c -> 9c vs a 13c ask)
            ours = {}
            if st:
                ours[st["price"]] = ours.get(st["price"], Decimal(0)) + Decimal(st["qty"])
            gp = exit_ghost.get(rid)
            if gp:
                ours[gp[0]] = ours.get(gp[0], Decimal(0)) + Decimal(gp[1])
            comp = None
            for lpx, lsz in kasks:
                if lsz - ours.get(lpx, Decimal(0)) > 0:
                    comp = lpx
                    break
            exit_ghost.pop(rid, None)  # one-cycle memory, consumed
            tick = Decimal("0.01")
            target = max(min_px, (comp - tick) if comp is not None else min_px)
            target = max(target, kb_px + tick)  # post-only: never cross the bid
            target = target.quantize(tick, rounding=ROUND_UP)
            if target > Decimal("0.99"):
                if st:
                    _exit_drop(rid, st, cancel=True)
                continue
            if st:
                if st["price"] == target and st["qty"] == qty:
                    continue  # already resting there
                if target > st["price"] and st["price"] >= min_px \
                        and time.monotonic() - st.get("placed_mono", 0.0) < 600:
                    # upward (more passive) repricing is SLOW: an adversary
                    # bot posts one tick inside us whenever we rest high and
                    # pulls when we drop — chasing its vanish/reappear cycle
                    # churned 12c<->10c every 15s (2026-07-24). Hold a
                    # profitable, best-or-tied price; recapture passivity at
                    # most every 10 min. Downward (stay best) and min_px
                    # (stay profitable) moves are immediate.
                    continue
                _exit_drop(rid, st, cancel=True)
            try:
                r = kgw.place_yes(w["kt"], "ask", target, qty, post_only=True)
                oid = (r.get("order") or {}).get("order_id") or r.get("order_id")
            except Exception as e:
                print(f"[EXIT] {rid} place failed: {type(e).__name__}: {e}", flush=True)
                continue
            if not oid:
                continue
            exit_q[rid] = {**w, "order_id": oid, "price": target, "qty": qty,
                           "filled": 0, "placed_mono": time.monotonic()}
            quoters[rid].suppress.add((w["kt"], "ask"))
            quoters[rid].intents.append(
                {"ts": time.time(), "place": w["kt"], "venue": "kalshi",
                 "side": "ask", "price": str(target), "count": qty,
                 "order_id": oid, "tag": "exit"})
            print(f"[EXIT] {rid} resting ask x{qty} @ {target} (basis {w['cost_ct']:.3f}, "
                  f"min_px {min_px:.4f}, comp {comp}, kb {kb_px}, pa {pa.price}, "
                  f"locks >= {EXIT_LOCK}/ct after PM close)", flush=True)

    def _exit_fill(rid: str, st: dict, cum: int) -> None:
        delta = cum - st["filled"]
        st["filled"] = cum
        if delta < 1:
            return
        pgw = gateways.get(Venue.POLYMARKET_US)
        pbook = books.get("polymarket_us", st["ps"])
        pa = pbook.best_ask() if pbook is not None else None
        px = min((pa.price if pa is not None else Decimal("0.99")) + Decimal("0.01"),
                 Decimal("0.99"))
        pf = 0
        try:  # close the PM NO leg taker-side, exact delta, confirmed (fill-lag LAW)
            r1 = pgw.place_yes(st["ps"], "bid", px, delta, post_only=False)
            oid1 = r1.get("id") or r1.get("order_id")
            quoters[rid].intents.append(
                {"ts": time.time(), "place": st["ps"], "venue": "polymarket_us",
                 "side": "bid", "price": str(px), "count": delta,
                 "order_id": oid1, "tag": "exit-close"})
            for _ in range(6):
                pf = pgw.filled_qty(oid1) if oid1 else 0
                if pf >= delta:
                    break
                time.sleep(0.5)
        except Exception as e:
            print(f"[EXIT] {rid} PM close error: {type(e).__name__}: {e}", flush=True)
        m = min(delta, pf)
        if pf < delta:
            alerter.alert(f"arbbot EXIT {rid}: PM close {pf}/{delta} — naked PM NO "
                          f"remainder, hedge timer/recon will complete it")
        if m < 1:
            return
        proceeds = (st["price"] + Decimal(1) - px) * m
        realized = float(proceeds - st["cost_ct"] * m)
        rec = {"ts": time.time(), "relationship_id": rid, "title": st.get("title"),
               "strategy": "unwind", "status": "unwound", "closes_ts": st["basket_ts"],
               "qty": int(m),
               "legs": [{"venue": "kalshi", "market_id": st["kt"], "side": "yes",
                         "action": "sell", "qty": int(m), "yes_price": str(st["price"]),
                         "role": "maker"},
                        {"venue": "polymarket_us", "market_id": st["ps"], "side": "no",
                         "action": "sell", "qty": int(m), "yes_price": str(px)}],
               "proceeds_usd": float(proceeds),
               "realized_pnl_usd": realized,
               "note": "maker-unwind exit (card 1ed32918)"}
        with open(Path("data/exec/trades.jsonl"), "a") as f:
            f.write(json.dumps(rec) + "\n")
        risk.record_close(rel_by_id[rid], Decimal(m))
        alerter.alert(f"arbbot MAKER-UNWIND {rid} x{m} @ {st['price']} "
                      f"realized ${realized:.2f}")
        print(f"[EXIT] {rid} filled x{m} @ {st['price']}, PM closed @ {px}, "
              f"realized ${realized:.2f}", flush=True)
        if st["filled"] >= st["qty"]:
            _exit_drop(rid, st, cancel=False)

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
                # READER/PROCESSOR DECOUPLE (rust-rewrite bench finding, card
                # 23d15e3f): the recorder disconnects a subscriber at ~1MB of
                # queued backlog (~2.2s at burst peak), and our inline venue
                # I/O (hedge REST + confirmed-fill polls, 3-5s) exceeds that
                # during exactly the bursts that produce fills. The reader
                # task ONLY drains the socket into a bounded in-memory queue;
                # processing (which may block) consumes from the queue, so
                # stalls grow OUR buffer instead of the recorder's.
                q_lines: asyncio.Queue = asyncio.Queue(maxsize=200_000)

                async def _drain():
                    while True:
                        ln = await reader.readline()
                        if not ln:
                            await q_lines.put(None)
                            return
                        try:
                            q_lines.put_nowait(ln)
                        except asyncio.QueueFull:
                            # shed oldest by draining one — never block the reader
                            with contextlib.suppress(Exception):
                                q_lines.get_nowait()
                            with contextlib.suppress(Exception):
                                q_lines.put_nowait(ln)

                drain_task = asyncio.create_task(_drain())
                pause_until: dict[str, float] = {}
                pause_mtime = [0.0]
                pause_f = Path("data/exec/.quote_pause_requests.json")
                last_pause_check = [0.0]

                def _check_pause_requests():
                    now2 = time.time()
                    if now2 - last_pause_check[0] < 5:
                        return
                    last_pause_check[0] = now2
                    try:
                        mt = pause_f.stat().st_mtime
                    except OSError:
                        return
                    if mt == pause_mtime[0]:
                        return
                    pause_mtime[0] = mt
                    try:
                        reqs = json.loads(pause_f.read_text())
                    except (OSError, ValueError):
                        return
                    for rid2, until in reqs.items():
                        if until > now2 and pause_until.get(rid2, 0) < until:
                            pause_until[rid2] = until
                            q2 = quoters.get(rid2)
                            if q2 is not None:
                                with contextlib.suppress(Exception):
                                    q2.cancel_all()
                                print(f"[LIVE] quote PAUSE {rid2} until "
                                      f"+{until - now2:.0f}s (unwinder handshake)", flush=True)

                # FEED-HEALTH QUOTE PULL (Geoff ask, card 0a7e5478): a stale
                # venue feed invalidates CROSS-VENUE pricing on both sides (each
                # venue's quote is hedge-priced against the other's book), so
                # any critical-feed staleness cancels ALL resting quotes until
                # the recorder reports the feeds healthy again. Signal source =
                # the recorder's health.jsonl (venue-heartbeat-aware — mere
                # event silence on sleepy political books does NOT trip it).
                feed_paused = [False]
                last_health_check = [0.0]
                health_f = Path("data/health.jsonl")

                def _check_feed_health():
                    now3 = time.time()
                    if now3 - last_health_check[0] < 10:
                        return
                    last_health_check[0] = now3
                    try:
                        with open(health_f, "rb") as hf:
                            hf.seek(max(hf.seek(0, 2) - 2048, 0))
                            tail_lines = hf.read().decode(errors="ignore").strip().splitlines()
                        h = json.loads(tail_lines[-1])
                    except Exception:
                        return
                    if now3 - h.get("ts", 0) > 30:
                        stale_now = True   # health writer itself silent = recorder down
                    else:
                        st = h.get("stale", {})
                        stale_now = bool(st.get("kalshi-ws") or st.get("polymarket_us-ws"))
                    if stale_now and not feed_paused[0]:
                        feed_paused[0] = True
                        for q3 in quoters.values():
                            with contextlib.suppress(Exception):
                                q3.cancel_all()
                        print("[LIVE] FEED STALE — all quotes PULLED until feeds recover",
                              flush=True)
                        alerter.alert("arbbot trader: feed stale — quotes pulled")
                    elif not stale_now and feed_paused[0]:
                        feed_paused[0] = False
                        print("[LIVE] feeds healthy — quoting resumes", flush=True)

                while True:
                    # timeout so the periodic tasks below (2s fill-poll
                    # fallback, TT refresh, pause/feed checks, intent flush)
                    # keep running on a QUIET book — without it a resting fill
                    # could sit undetected until the next book event arrives
                    # (card ad08160c: quiet book + WS gap + fill = naked leg).
                    try:
                        line = await asyncio.wait_for(q_lines.get(), timeout=1.0)
                    except asyncio.TimeoutError:
                        line = b""          # no event — periodic tasks only
                    if line is None:
                        break
                    _check_pause_requests()
                    _check_feed_health()
                    # liveness heartbeat: quiet-vs-stuck is unanswerable without
                    # this — books can legitimately sleep for long stretches.
                    if time.monotonic() - last_hb > 60:
                        n_rest = sum(len(q._resting) for q in quoters.values())
                        print(f"[hb] events/min={hb_events} on_book={hb_onbook} "
                              f"resting={n_rest}", flush=True)
                        last_hb = time.monotonic()
                        hb_events = hb_onbook = 0
                    ev = None
                    if line:
                        hb_events += 1
                        try:
                            ev = parse_event(json.loads(line))
                        except ValueError:
                            ev = None  # truncated line at recorder cut-over
                    if isinstance(ev, BookSnapshot):
                        books.apply_snapshot(ev)
                    elif isinstance(ev, BookDelta):
                        with contextlib.suppress(GapDetected, NotSynced):
                            books.apply_delta(ev)
                    else:
                        ev = None  # non-book event — still run periodic tasks
                    for rid in (leg_to_rels.get((ev.venue.value, ev.market_id), ())
                                if ev is not None else ()):
                        q = quoters[rid]
                        hb_onbook += 1
                        if feed_paused[0]:
                            continue  # stale feed — cross-venue pricing invalid
                        if pause_until.get(rid, 0) > time.time():
                            continue  # unwinder is closing this basket — stay out of the book
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
                    _exit_refresh()  # maker-unwind exits (self-throttled, 15s)
                    # fallback fill detection (WS is primary): poll each resting
                    # order every 2s; _process_fill dedupes vs the WS path.
                    if live and now - last_fill_check > 2.0:
                        last_fill_check = now
                        kgw_x = gateways.get(Venue.KALSHI)
                        for rid_x, st_x in list(exit_q.items()):  # maker-unwind fills
                            with contextlib.suppress(Exception):
                                fx = kgw_x.filled_qty(st_x["order_id"]) if kgw_x else 0
                                if fx > st_x["filled"]:
                                    _exit_fill(rid_x, st_x, fx)
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
                with contextlib.suppress(Exception):
                    drain_task.cancel()
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
            for rid_s, st_s in list(exit_q.items()):  # maker-unwind exit asks
                _exit_drop(rid_s, st_s, cancel=True)
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
