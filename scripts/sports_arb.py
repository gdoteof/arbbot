"""WS-driven cross-venue sports arb (MLB), real-time detection.

Both venues stream live game books over websocket. Kalshi book is maintained via
the recorder's proven KalshiWsBook (snapshot+delta) fed into the shared
BookBuilder; PM US frames carry the full book each tick (read directly). On every
update we recompute the cross-venue edge. Gaps live for seconds -> capturable
with ~100ms execution. DRY by default (logs); --live wiring is the next step.
"""

import argparse
import asyncio
import contextlib
import datetime
import json
import pathlib
import re
import time

from decimal import Decimal

import httpx
import websockets

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.ops.alerts import Alerter
from arbbot.record.kalshi import (REST_BASE, KalshiWsBook, load_private_key,
                                  sign_headers, ws_subscribe_frame)
from arbbot.record.polymarket_us import subscribe_frame, ws_auth_headers

D = pathlib.Path.home() / ".arbbot-credentials"
MON = {"JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
       "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12}
FEE = 0.02       # round-trip fee crossing BOTH books (take-take)
MT_FEE = 0.01    # make-take: only the hedge leg is a taker; maker fee is small
MIN_EDGE = 0.02
MAX_SPREAD = 0.03
CLIP = 5              # contracts per capture (tiny to start)
CAP_PER_GAME = 20     # max contracts per game
COOLDOWN = 20.0       # s between captures on the same game

# make-take adverse-selection PROBE (opt-in via --mt-probe). Deliberately tiny:
# the whole point is to measure how badly a resting sports quote gets picked off,
# not to make money. Bounded so even a total failure costs a few dollars.
MT_CLIP = 2            # contracts per make-take quote
MT_LIFETIME_CAP = 20   # total contracts to fill across the experiment, then stop
MT_MAX_CONCURRENT = 8  # resting orders in flight at once (cast a wide net for fills)
MT_MIN_EXP = 0.015     # only quote when the make-take lock LOOKS >= +1.5c (bigger spikes only)
MT_QUOTE_TTL = 120.0   # hard cap on how long one quote rests before we reprice
MT_REQUOTE_COOLDOWN = 5.0  # per (game,side) wait after a cancel before re-resting


def execute(g, kbid, kask, pbid, pask, kg, pg, alerter, state):
    """Legging-safe cross-venue capture. Buy the CHEAP away-leg first (limit IOC =
    won't overpay if it moved), then sell exactly-filled on the DEAR leg; flatten
    any excess if the sell underfills. Records + alerts."""
    kt, slug = g["kt"], g["slug"]
    key = f"{g['away']}@{g['home']}"
    if time.time() - state["last"].get(key, 0) < COOLDOWN:
        return
    if state["pos"].get(key, 0) >= CAP_PER_GAME:
        return
    # direction: buy cheap away, sell dear away
    if (kbid - pask) >= (pbid - kask):
        cheap, dear, buy_px, sell_px = "pm", "k", Decimal(str(pask)), Decimal(str(kbid))
    else:
        cheap, dear, buy_px, sell_px = "k", "pm", Decimal(str(kask)), Decimal(str(pbid))
    qty = min(CLIP, CAP_PER_GAME - state["pos"].get(key, 0))
    state["last"][key] = time.time()

    def buy(v, px, q):
        if v == "k":
            r = kg.place_yes(kt, "bid", px, q, post_only=False)
            return kg.filled_qty((r.get("order") or {}).get("order_id") or r.get("order_id") or "")
        r = pg.place_yes(slug, "bid", px, q, post_only=False)
        return pg.filled_qty(r.get("id") or r.get("order_id") or "")

    def sell(v, px, q):
        if v == "k":
            r = kg.place_yes(kt, "ask", px, q, post_only=False)
            return kg.filled_qty((r.get("order") or {}).get("order_id") or r.get("order_id") or "")
        r = pg.place_short(slug, px, q, post_only=False)
        return pg.filled_qty(r.get("id") or r.get("order_id") or "")

    print(f"[{time.strftime('%H:%M:%S')}] CAPTURE {key} x{qty}: buy {cheap}@{buy_px} then sell {dear}@{sell_px}", flush=True)
    try:
        f1 = buy(cheap, buy_px, qty)
        if f1 < 1:
            print(f"  leg1 (buy {cheap}) didn't fill (price moved) — abort, nothing naked", flush=True)
            return
        f2 = sell(dear, sell_px, f1)
        if f2 < f1:  # couldn't hedge all -> flatten the excess long on the cheap venue
            excess = f1 - f2
            print(f"  *** leg2 hedged {f2}/{f1}; FLATTENING {excess} on {cheap} ***", flush=True)
            sell(cheap, Decimal("0.01"), excess)  # marketable dump to close the naked long
            alerter.alert(f"arbbot SPORTS leg2 short {key}: hedged {f2}/{f1}, flattened {excess}")
        state["pos"][key] = state["pos"].get(key, 0) + f2
        prof = float((sell_px - buy_px) * f2) - FEE * f2
        alerter.alert(f"arbbot SPORTS CAPTURE {key} x{f2} +${prof:.2f} riskless")
        rec = {"ts": time.time(), "relationship_id": f"sports-mlb-{key}", "title": f"MLB {key} (cross-venue)",
               "qty": int(f2), "strategy": "sports-take-take", "resolves_by": None, "resolves_estimated": False,
               "legs": [{"venue": "kalshi", "market_id": kt, "role": "taker",
                         "side": "yes" if dear == "k" else "no", "qty": int(f2), "yes_price": str(sell_px if dear == "k" else buy_px)},
                        {"venue": "polymarket_us", "market_id": slug, "role": "taker",
                         "side": "no" if dear == "k" else "yes", "qty": int(f2), "yes_price": str(sell_px if dear == "pm" else buy_px)}],
               "cost_usd": float(min(buy_px, Decimal(1) - sell_px) * f2), "payoff_usd": float(f2),
               "profit_usd": prof, "status": "open"}
        with open("data/exec/trades.jsonl", "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        print(f"  CAPTURED {key} x{f2}  +${prof:.2f} riskless (recorded)", flush=True)
    except Exception as e:
        print(f"  *** SPORTS EXEC ERROR {key}: {type(e).__name__}: {e} — 60s recon net will flag any naked leg ***", flush=True)


def todays_games(c):
    today = datetime.date.today()
    ms = c.get(f"{REST_BASE}/markets", params={"series_ticker": "KXMLBGAME", "status": "open", "limit": 400}).json().get("markets", [])
    tmp = {}
    for x in ms:
        m = re.match(r"KXMLBGAME-(\d{2})([A-Z]{3})(\d{2})(\d{4})([A-Z]+)-([A-Z]+)", x["ticker"])
        if not m:
            continue
        yy, mon, dd, _t, concat, team = m.groups()
        try:
            date = datetime.date(2000 + int(yy), MON[mon], int(dd))
        except (KeyError, ValueError):
            continue
        if date != today:
            continue
        tmp.setdefault(concat, {})[team] = x["ticker"]
    games = {}
    for concat, sides in tmp.items():
        if len(sides) != 2:
            continue
        away = next((t for t in sides if concat.startswith(t)), None)
        home = next((t for t in sides if t != away), None)
        if away and home:
            games[concat] = {"kt": sides[away], "away": away, "home": home,
                             "slug": f"aec-mlb-{away.lower()}-{home.lower()}-{today.isoformat()}"}
    return games


async def kalshi_ws(games, books, stop):
    kid = (D / "kalshi_api_key_id").read_text().strip()
    key = load_private_key((D / "kalshi_private_key.pem").read_bytes())
    tickers = [g["kt"] for g in games.values()]
    while not stop.is_set():
        try:
            kwb = KalshiWsBook()
            seqs = {}  # per-ticker monotonic seq (a global counter creates per-ticker gaps)

            def nextseq(kt):
                seqs[kt] = seqs.get(kt, 0) + 1
                return seqs[kt]
            async with websockets.connect("wss://api.elections.kalshi.com/trade-api/ws/v2",
                                           additional_headers=sign_headers(kid, key, "GET", "/trade-api/ws/v2"),
                                           ping_interval=None, open_timeout=10) as ws:
                await ws.send(ws_subscribe_frame(tickers))
                async for raw in ws:
                    m = json.loads(raw)
                    t = m.get("type")
                    if t == "orderbook_snapshot":
                        kt = m["msg"]["market_ticker"]
                        books.apply_snapshot(kwb.on_snapshot(m["msg"], nextseq(kt)))
                    elif t == "orderbook_delta":
                        kt = m["msg"]["market_ticker"]
                        with contextlib.suppress(GapDetected, NotSynced):
                            books.apply_delta(kwb.on_delta(m["msg"], nextseq(kt)))
        except Exception as e:
            print(f"[kalshi ws] {type(e).__name__}: {e}; reconnect", flush=True)
            await asyncio.sleep(2)


async def pmus_ws(games, pm_bbo, stop):
    kid = (D / "polymarket_usa_key_id").read_text().strip()
    key = (D / "polymarket_usa_private_key").read_text().strip()
    slugs = [g["slug"] for g in games.values()]
    while not stop.is_set():
        try:
            async with websockets.connect("wss://api.polymarket.us/v1/ws/markets",
                                           additional_headers=ws_auth_headers(kid, key),
                                           ping_interval=None, open_timeout=10) as ws:
                for i in range(0, len(slugs), 20):
                    await ws.send(subscribe_frame(f"b{i}", "SUBSCRIPTION_TYPE_MARKET_DATA", slugs[i:i + 20]))
                async for raw in ws:
                    md = json.loads(raw).get("marketData")
                    if not md:
                        continue
                    bids = [float(b["px"]["value"]) for b in (md.get("bids") or [])]
                    # PM US denotes the ask side as "offers" (not "asks")
                    asks = [float(a["px"]["value"]) for a in (md.get("offers") or md.get("asks") or [])]
                    pm_bbo[md.get("marketSlug")] = (max(bids) if bids else None, min(asks) if asks else None)
        except Exception as e:
            print(f"[pmus ws] {type(e).__name__}: {e}; reconnect", flush=True)
            await asyncio.sleep(2)


async def lines_writer(games, books, pm_bbo, stop):
    """Dump the engine's REAL-TIME cross-venue lines to a file every ~3s so the
    dashboard can display them (never a fresh REST poll — that shows phantom
    crossings). Crossed = genuine net-of-fee edge in the live books."""
    out = pathlib.Path("data/exec/sports_lines.json")
    out.parent.mkdir(parents=True, exist_ok=True)
    tick = 0
    while not stop.is_set():
        await asyncio.sleep(3)
        tick += 1
        rows = []
        for g in games.values():
            kbook = books.get("kalshi", g["kt"])
            pm = pm_bbo.get(g["slug"])
            row = {"game": f"{g['away']}@{g['home']}", "away": g["away"], "home": g["home"]}
            if kbook and kbook.best_bid() and kbook.best_ask():
                row["k_bid"], row["k_ask"] = float(kbook.best_bid().price), float(kbook.best_ask().price)
            if pm and None not in pm:
                row["pm_bid"], row["pm_ask"] = pm[0], pm[1]
            if all(k in row for k in ("k_bid", "k_ask", "pm_bid", "pm_ask")):
                edge = max(row["pm_bid"] - row["k_ask"], row["k_bid"] - row["pm_ask"]) - FEE
                row["edge_c"] = round(edge * 100, 1)
                row["crossed"] = edge > 0
                # make-take CEILING: rest a maker quote on one leg (save that
                # spread), take the hedge on the other. Best of resting on
                # either venue = the venues' quote disagreement, one taker fee.
                # This is adverse-selection-BLIND — on fast sports books our
                # rest is the stale quote informed takers pick off, so realized
                # make-take is worse. It bounds the opportunity, not what we'd net.
                mt = max(abs(row["pm_bid"] - row["k_bid"]),
                         abs(row["pm_ask"] - row["k_ask"])) - MT_FEE
                row["mt_edge_c"] = round(mt * 100, 1)
                row["mt_crossed"] = mt > 0
            rows.append(row)
        rows.sort(key=lambda r: -(r.get("edge_c") or -99))
        doc = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
               "games": rows, "n_live": sum(1 for r in rows if "edge_c" in r),
               "n_crossed": sum(1 for r in rows if r.get("crossed")),
               "n_mt_crossed": sum(1 for r in rows if r.get("mt_crossed"))}
        try:
            out.write_text(json.dumps(doc))
        except OSError:
            pass
        # time series: append the best edge across live games every ~15s to a
        # per-day file (survives the engine's 15-min restarts; bounded to a day).
        live = [r for r in rows if "edge_c" in r]
        if live and tick % 5 == 1:
            best = max(live, key=lambda r: r["edge_c"])
            best_mt = max(r["mt_edge_c"] for r in live)
            hist = out.parent / f"sports_edge-{time.strftime('%Y-%m-%d', time.gmtime())}.jsonl"
            try:
                with hist.open("a") as f:
                    f.write(json.dumps({"t": int(time.time()), "best": best["edge_c"],
                                        "best_mt": best_mt, "game": best["game"],
                                        "n": len(live)}) + "\n")
            except OSError:
                pass


async def detector(games, books, pm_bbo, stop, live, kg, pg, alerter, state):
    last = {}
    hb = 0.0
    while not stop.is_set():
        await asyncio.sleep(0.2)
        if time.time() - hb > 60:   # heartbeat with max live cross-venue edge
            hb = time.time()
            n = 0
            best = -9.0
            bestg = None
            for g in games.values():
                kbook = books.get("kalshi", g["kt"]); pm = pm_bbo.get(g["slug"])
                if kbook and kbook.best_bid() and kbook.best_ask() and pm and None not in pm:
                    n += 1
                    e = max(pm[0] - float(kbook.best_ask().price), float(kbook.best_bid().price) - pm[1]) - FEE
                    if e > best:
                        best, bestg = e, f"{g['away']}@{g['home']} K={float(kbook.best_bid().price):.2f}/{float(kbook.best_ask().price):.2f} PM={pm[0]:.2f}/{pm[1]:.2f}"
            print(f"[{time.strftime('%H:%M:%S')}] hb: {n}/{len(games)} live books | best edge {best*100:+.1f}c [{bestg}]", flush=True)
        for g in games.values():
            kbook = books.get("kalshi", g["kt"])
            pm = pm_bbo.get(g["slug"])
            if kbook is None or pm is None or None in pm:
                continue
            kbb, kba = kbook.best_bid(), kbook.best_ask()
            if not kbb or not kba:
                continue
            kbid, kask = float(kbb.price), float(kba.price)
            pbid, pask = pm
            edge = max(pbid - kask, kbid - pask) - FEE
            if edge >= MIN_EDGE and (kask - kbid) <= MAX_SPREAD and (pask - pbid) <= MAX_SPREAD:
                key = f"{g['away']}@{g['home']}"
                if time.time() - last.get(key, 0) > 3:
                    last[key] = time.time()
                    side = "buy PM/sell K" if (kbid - pask) >= (pbid - kask) else "buy K/sell PM"
                    print(f"[{time.strftime('%H:%M:%S')}] SPORTS-DISLOCATION {key}: "
                          f"K={kbid:.2f}/{kask:.2f} PM={pbid:.2f}/{pask:.2f} edge={edge*100:+.1f}c ({side})"
                          + (" -> EXECUTE" if live else ""), flush=True)
                    if live:
                        execute(g, kbid, kask, pbid, pask, kg, pg, alerter, state)


def _mt_candidates(games, books, pm_bbo):
    """Every (game, side) whose make-take lock currently LOOKS profitable.
    bid side: rest Kalshi buy-YES @ k_bid, hedge = sell PM YES @ p_bid.
    ask side: rest Kalshi sell-YES @ k_ask, hedge = buy PM YES @ p_ask.
    A >6c spike can be on either side — quoting both casts the widest net."""
    out = []
    for g in games.values():
        kbook = books.get("kalshi", g["kt"]); pm = pm_bbo.get(g["slug"])
        if not (kbook and kbook.best_bid() and kbook.best_ask() and pm and None not in pm):
            continue
        kbid, kask = float(kbook.best_bid().price), float(kbook.best_ask().price)
        pbid, pask = pm
        if (kask - kbid) > MAX_SPREAD or (pask - pbid) > MAX_SPREAD:
            continue
        for side, exp, ref, hedge_ref in (
                ("bid", pbid - kbid - MT_FEE, kbid, pbid),
                ("ask", kask - pask - MT_FEE, kask, pask)):
            if exp >= MT_MIN_EXP:
                out.append({"g": g, "key": f"{g['away']}@{g['home']}", "side": side,
                            "exp": exp, "ref": ref, "hedge_ref": hedge_ref})
    out.sort(key=lambda c: -c["exp"])
    return out


async def mt_probe(games, books, pm_bbo, stop, kg, pg, alerter, state):
    """Make-take ADVERSE-SELECTION probe — CONCURRENT net. Rests tiny Kalshi
    makers on MANY (game, side) pairs at once so we're already sitting there
    when a spike lands; on any fill, immediately hedges PM and records
    expected-vs-realized lock (the shortfall = the pickoff). Lifetime-capped,
    concurrency-capped, cancels ONLY its own orders by id (never a blanket
    cancel — the political runner shares this Kalshi account)."""
    rec_path = pathlib.Path("data/exec/sports_mt_probe.jsonl")
    filled_total = 0; naked_total = 0  # seed from prior fills so the cap holds across restarts
    if rec_path.exists():
        with contextlib.suppress(OSError, ValueError):
            for l in rec_path.read_text().splitlines():
                if l.strip():
                    d = json.loads(l)
                    filled_total += int(d.get("qty", 0))
                    naked_total += int(d.get("naked", 0))
    active = state["mt_active"]          # (key,side) -> order dict (also swept on shutdown)
    cooldown = {}                        # (key,side) -> earliest re-quote time
    # startup self-heal: cancel any orphaned SPORTS makers left resting by a
    # prior run that didn't shut down cleanly (e.g. hard kill / SIGTERM before
    # the finally ran). Only KXMLBGAME tickers — never the political runner's
    # resting orders on this shared Kalshi account.
    with contextlib.suppress(Exception):
        swept = 0
        for o in kg.resting_orders():
            if o.get("status") == "resting" and str(o.get("ticker", "")).startswith("KXMLBGAME"):
                with contextlib.suppress(Exception):
                    kg.cancel(o.get("order_id")); swept += 1
        if swept:
            print(f"MT startup: swept {swept} orphaned sports maker(s) from a prior run", flush=True)

    def hedge_and_record(o, f):
        nonlocal filled_total, naked_total
        g, side = o["g"], o["side"]
        pm = pm_bbo.get(g["slug"])
        # PROFIT-ONLY hedge: IOC at the breakeven limit. It fills only against a
        # book still at/through breakeven; if the hedge venue has moved past it,
        # the IOC MISSES and we HOLD the naked leg (never lock a loss). A miss IS
        # the pickoff we're proving.
        hf = 0; fill_px = None; realized = None
        try:
            if side == "bid":                          # long Kalshi YES -> sell PM YES
                limit = round(o["ref"] + MT_FEE, 2)    # breakeven sell price
                hr = pg.place_short(g["slug"], Decimal(str(limit)), int(f), post_only=False)
                hf = pg.filled_qty(hr.get("id") or hr.get("order_id") or "")
                if hf:
                    fill_px = pm[0] if (pm and pm[0] is not None) else limit
                    realized = fill_px - o["ref"] - MT_FEE
            else:                                      # short Kalshi YES -> buy PM YES
                limit = round(o["ref"] - MT_FEE, 2)    # breakeven buy price
                hr = pg.place_yes(g["slug"], "bid", Decimal(str(limit)), int(f), post_only=False)
                hf = pg.filled_qty(hr.get("id") or hr.get("order_id") or "")
                if hf:
                    fill_px = pm[1] if (pm and pm[1] is not None) else limit
                    realized = o["ref"] - fill_px - MT_FEE
        except Exception as e:
            print(f"  *** MT hedge ERROR {o['key']} {side}: {type(e).__name__}: {e} ***", flush=True)
            alerter.alert(f"arbbot MT hedge error {o['key']}")
        naked = int(f) - int(hf)
        filled_total += int(f)
        naked_total += naked
        picked = hf < f                                # any unhedged qty = picked off (held)
        rec = {"ts": time.time(), "game": o["key"], "side": side, "qty": int(f),
               "hedged": int(hf), "naked": naked, "ref_px": o["ref"],
               "exp_lock_c": round(o["exp"] * 100, 2),
               "captured_lock_c": (round(realized * 100, 2) if realized is not None else None),
               "picked_off": bool(picked)}
        with contextlib.suppress(OSError):
            with rec_path.open("a") as fh:
                fh.write(json.dumps(rec) + "\n")
        if picked:
            print(f"[{time.strftime('%H:%M:%S')}] MT-FILL {o['key']} {side} x{int(f)}: exp {rec['exp_lock_c']:+.1f}c "
                  f"-> HEDGE MISSED, holding {naked} naked [PICKED OFF] [{filled_total}/{MT_LIFETIME_CAP}, naked~{naked_total}]", flush=True)
            alerter.alert(f"arbbot MT PICKED OFF {o['key']} {side} — holding {naked} naked (exp {rec['exp_lock_c']:+.1f}c)")
        else:
            print(f"[{time.strftime('%H:%M:%S')}] MT-FILL {o['key']} {side} x{int(f)}: exp {rec['exp_lock_c']:+.1f}c "
                  f"-> CAPTURED {rec['captured_lock_c']:+.1f}c hedged {int(hf)}/{int(f)} [{filled_total}/{MT_LIFETIME_CAP}]", flush=True)
            alerter.alert(f"arbbot MT CAPTURED {o['key']} {side} x{int(f)} {rec['captured_lock_c']:+.1f}c")

    if filled_total >= MT_LIFETIME_CAP:
        print(f"MT-PROBE already at lifetime cap ({filled_total}) — not quoting.", flush=True)
    while not stop.is_set():
        await asyncio.sleep(1.0)
        now = time.time()
        # 1) service resting orders: fill -> hedge; stale/expired -> cancel
        for k, o in list(active.items()):
            try:
                f = kg.filled_qty(o["oid"])
            except Exception:
                f = 0
            if f >= 1:
                with contextlib.suppress(Exception):
                    kg.cancel(o["oid"])          # cancel any unfilled remainder
                del active[k]; cooldown[k] = now + MT_REQUOTE_COOLDOWN
                hedge_and_record(o, f)
                continue
            kbook = books.get("kalshi", o["g"]["kt"]); pm = pm_bbo.get(o["g"]["slug"])
            stale = now - o["t0"] > MT_QUOTE_TTL
            if kbook and kbook.best_bid() and kbook.best_ask() and pm and None not in pm:
                kbid, kask = float(kbook.best_bid().price), float(kbook.best_ask().price)
                if o["side"] == "bid":
                    stale = stale or kbid > o["ref"] + 1e-9 or (pm[0] - o["ref"] - MT_FEE) < MT_MIN_EXP
                else:
                    stale = stale or kask < o["ref"] - 1e-9 or (o["ref"] - pm[1] - MT_FEE) < MT_MIN_EXP
            if stale:
                with contextlib.suppress(Exception):
                    kg.cancel(o["oid"])
                del active[k]; cooldown[k] = now + MT_REQUOTE_COOLDOWN
        # 2) top up the net with new quotes (respect concurrency + lifetime cap)
        if filled_total >= MT_LIFETIME_CAP:
            if not active:
                break
            continue
        for c in _mt_candidates(games, books, pm_bbo):
            if len(active) >= MT_MAX_CONCURRENT:
                break
            k = (c["key"], c["side"])
            if k in active or now < cooldown.get(k, 0):
                continue
            side = c["side"]
            try:
                r = kg.place_yes(c["g"]["kt"], side, Decimal(str(c["ref"])), MT_CLIP, post_only=True)
            except Exception as e:
                print(f"  MT place failed {c['key']} {side}: {type(e).__name__}: {e}", flush=True)
                cooldown[k] = now + MT_REQUOTE_COOLDOWN
                continue
            oid = (r.get("order") or {}).get("order_id") or r.get("order_id") or ""
            if not oid:
                cooldown[k] = now + MT_REQUOTE_COOLDOWN
                continue
            active[k] = {"oid": oid, "g": c["g"], "key": c["key"], "side": side,
                         "ref": c["ref"], "hedge_ref": c["hedge_ref"], "exp": c["exp"], "t0": now}
            print(f"[{time.strftime('%H:%M:%S')}] MT-QUOTE {c['key']} {side} {MT_CLIP}@{c['ref']:.2f} "
                  f"(exp {c['exp']*100:+.1f}c) [{len(active)} resting]", flush=True)
    if filled_total >= MT_LIFETIME_CAP:
        print(f"[{time.strftime('%H:%M:%S')}] MT-PROBE lifetime cap reached ({filled_total}) "
              f"— quoting stopped. Results: data/exec/sports_mt_probe.jsonl", flush=True)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--seconds", type=int, default=0, help="run for N seconds then exit (0=forever)")
    ap.add_argument("--mt-probe", action="store_true",
                    help="run the tiny make-take adverse-selection probe (rests real Kalshi makers)")
    args = ap.parse_args()
    games = todays_games(httpx.Client(timeout=15))
    print(f"tracking {len(games)} live MLB games via WS ({'LIVE' if args.live else 'DRY'})", flush=True)
    if not games:
        await asyncio.sleep(300)  # no games now — wait before exit so the outer loop re-checks calmly
        return
    books, pm_bbo, stop = BookBuilder(), {}, asyncio.Event()
    kg = KalshiOrderGateway((D / "kalshi_api_key_id").read_text().strip(),
                            load_private_key((D / "kalshi_private_key.pem").read_bytes()), live=args.live)
    pg = PolymarketUsOrderGateway((D / "polymarket_usa_key_id").read_text().strip(),
                                  (D / "polymarket_usa_private_key").read_text().strip(), live=args.live)
    alerter = Alerter(None)
    state = {"last": {}, "pos": {}, "mt_active": {}}
    tasks = [asyncio.create_task(kalshi_ws(games, books, stop)),
             asyncio.create_task(pmus_ws(games, pm_bbo, stop)),
             asyncio.create_task(lines_writer(games, books, pm_bbo, stop)),
             asyncio.create_task(detector(games, books, pm_bbo, stop, args.live, kg, pg, alerter, state))]
    if args.mt_probe and args.live:
        print("make-take probe ARMED (tiny, capped) — will rest real Kalshi makers", flush=True)
        tasks.append(asyncio.create_task(mt_probe(games, books, pm_bbo, stop, kg, pg, alerter, state)))
    try:
        if args.seconds:
            await asyncio.sleep(args.seconds)
            stop.set()
            for t in tasks:
                t.cancel()
        else:
            await asyncio.gather(*tasks)
    finally:
        # cancel ONLY the probe's own resting orders (by id) — never a blanket
        # sweep; the political runner rests on this same Kalshi account.
        for o in list(state.get("mt_active", {}).values()):
            with contextlib.suppress(Exception):
                kg.cancel(o["oid"])
        if state.get("mt_active"):
            print(f"shutdown: canceled {len(state['mt_active'])} resting probe order(s)", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
