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
# PM-LEAN experiment (Geoff 2026-07-22): settled sample shows our Kalshi-held
# sides lost 13/18 (~72%) vs ~55-60% implied by entry prices — the PM leg of
# our arbs keeps being the winning side. Test: ride 2 extra UNHEDGED contracts
# of the PM leg on each capture (dear=pm direction only — the generic hedge
# service can profitably complete PM-shorts later; salvage rules attach).
# Directional by design, capped tiny.
LEAN_CLIP = 2
LEAN_CAP = 20          # lifetime contracts, then the experiment stops
LEAN_STATE = pathlib.Path("data/exec/pm_lean_state.json")


def lean_used():
    try:
        return int(json.loads(LEAN_STATE.read_text()).get("used", 0))
    except (OSError, ValueError):
        return 0


def lean_add(n):
    with contextlib.suppress(OSError):
        LEAN_STATE.write_text(json.dumps({"used": lean_used() + n}))

# make-take adverse-selection PROBE (opt-in via --mt-probe). Deliberately tiny:
# the whole point is to measure how badly a resting sports quote gets picked off,
# not to make money. Bounded so even a total failure costs a few dollars.
MT_CLIP = 2            # contracts per make-take quote
MT_LIFETIME_CAP = 60   # round 2 budget (round 1 used 20: touch 9/10 picked off,
                       # deep 0 fills, wide-book touch the clear winner)
MT_MAX_CONCURRENT = 8  # resting orders in flight at once (cast a wide net for fills)
# ROUND 2 (Geoff 2026-07-22: "focus on the actual winner"): WIDE-BOOK TOUCH
# ONLY — quote at the Kalshi touch only when the buffer to the PM hedge anchor
# is >= 5c. Tight-book touch (2c buffers) was 9-for-9 picked off; the deep arm
# (6c off-touch) never filled in 90 min. Distance-to-hedge-anchor is the edge.
MT_MIN_EXP = 0.05
MT_QUOTE_TTL = 120.0   # hard cap on how long one quote rests before we reprice
# DEEP arm (Geoff 2026-07-22: "sit deeper in the pool... make informed flow pay
# a premium"): rest OFF-touch at a price that locks MT_DEEP_LOCK vs the current
# PM hedge anchor if filled. Deep quotes are patient (long TTL, no outbid
# repricing — being outbid is the design) and cancel only when PM drifts toward
# us enough to shrink the would-be lock below MT_DEEP_STALE.
MT_DEEP_LOCK = 0.06     # premium the filler pays if PM hasn't moved
MT_DEEP_STALE = 0.03    # cancel a deep quote when lock-now falls below this
MT_DEEP_TTL = 600.0     # deep quotes are patient
MT_PER_STYLE = 4        # concurrency split: 4 touch + 4 deep
# probe spread rule (Geoff 2026-07-22: the big mt edges live on WIDE Kalshi
# books): the HEDGE venue (PM) must be tight — that anchor is what makes the
# lock real — but the QUOTE venue (Kalshi) spread is unlimited; resting inside
# a wide Kalshi spread against a pinned PM book is the whole opportunity.
# Wide-BOTH-venues "ceilings" are quote disagreement between mush books and
# stay excluded.
MT_HEDGE_MAX_SPREAD = 0.04
MT_REQUOTE_COOLDOWN = 5.0  # per (game,side) wait after a cancel before re-resting


INTENTS = pathlib.Path("data/scan/trader-intents-sports.jsonl")


def _intent(**ev):
    """Order-lifecycle event in the political runner's intents format so the
    dashboard Order Activity tab shows sports orders too."""
    ev.setdefault("ts", time.time())
    try:
        INTENTS.parent.mkdir(parents=True, exist_ok=True)
        with INTENTS.open("a") as f:
            f.write(json.dumps(ev) + "\n")
    except OSError:
        pass


# PM books sometimes FREEZE (displayed quotes, nothing matchable — IOCs
# through the touch expire; seen 2026-07-22 Babel): after such an impossible
# miss, blacklist the slug so the detector/probe/rehedge stop trusting it.
PM_DEAD: dict = {}
PM_DEAD_TTL = 600.0
PM_DEAD_COUNT: dict = {}     # slug -> times blacklisted (escalates the TTL)
PM_DEATHS: list = []         # recent (ts) of blacklist events — circuit input
CIRCUIT_OPEN_UNTIL = [0.0]   # venue-wide halt: >=5 deaths in 10min -> 30min off
CIRCUIT_DEATHS = 5
CIRCUIT_WINDOW = 600.0
CIRCUIT_HALT = 1800.0


def circuit_open():
    import time as _t
    return _t.time() < CIRCUIT_OPEN_UNTIL[0]
# last WS tick per PM slug: a frozen/suspended book stops ticking, and
# executing against a display that isn't ticking is how Babel/Jones legs
# went naked (2026-07-22). Take-take requires a LIVE book.
PM_LAST_TICK: dict = {}
PM_TICK_EWMA: dict = {}      # slug -> smoothed inter-tick interval (s)
PM_RESUB_REQ: dict = {}      # slug -> ts of heal request (resubscribe probe)
PM_FRESH_FLOOR = 10.0        # never call a book stale faster than this
PM_FRESH_CEIL = 120.0        # never trust silence longer than this
PM_HEAL_TIMEOUT = 10.0       # no frame within this after a resubscribe = frozen


def pm_note_tick(slug):
    import time as _t
    now = _t.time()
    last = PM_LAST_TICK.get(slug)
    if last is not None:
        dt = now - last
        ew = PM_TICK_EWMA.get(slug)
        PM_TICK_EWMA[slug] = dt if ew is None else 0.8 * ew + 0.2 * dt
    PM_LAST_TICK[slug] = now
    PM_RESUB_REQ.pop(slug, None)  # any frame proves liveness — heal complete


PROBE_SLUGS: set = set()
PROBE_SLUGS_TS = [0.0]


def probe_owned(slug):
    """PM slugs the research probes actively quote (refreshed 60s from their
    logs): the take-take detector must not execute against them — we would be
    crossing our own account (wash fills, dueling recons)."""
    import time as _t
    now = _t.time()
    if now - PROBE_SLUGS_TS[0] > 60:
        PROBE_SLUGS_TS[0] = now
        PROBE_SLUGS.clear()
        for pf in ("data/exec/pmus_maker_probe.jsonl", "data/exec/toxicity_probe.jsonl"):
            try:
                for ln in pathlib.Path(pf).read_text().splitlines()[-400:]:
                    try:
                        r = json.loads(ln)
                    except ValueError:
                        continue
                    if r.get("action") in ("quote", "recon_flatten") and r.get("pm") \
                            and now - r.get("ts", 0) < 3600:
                        PROBE_SLUGS.add(r["pm"])
            except OSError:
                continue
    return slug in PROBE_SLUGS


def pm_fresh(slug):
    """Adaptive: a book is fresh if silent for less than 5x its OWN typical
    cadence (clamped 10..120s). When it goes stale, request a HEAL — the WS
    task resubscribes, which makes PM push a full book frame if the market is
    alive. Silence past the heal timeout = frozen -> blacklist."""
    import time as _t
    now = _t.time()
    last = PM_LAST_TICK.get(slug, 0)
    ew = PM_TICK_EWMA.get(slug)
    limit = max(PM_FRESH_FLOOR, min(PM_FRESH_CEIL, 5 * ew)) if ew else PM_FRESH_CEIL
    if now - last < limit:
        return True
    req = PM_RESUB_REQ.get(slug)
    if req is None:
        PM_RESUB_REQ[slug] = now          # ask the WS task to resubscribe
    elif now - req > PM_HEAL_TIMEOUT:
        mark_pm_dead(slug, f"no frame {now - last:.0f}s after heal probe")
        PM_RESUB_REQ.pop(slug, None)
    return False


def pm_dead(slug):
    import time as _t
    return PM_DEAD.get(slug, 0) > _t.time()


def mark_pm_dead(slug, why=""):
    import time as _t
    now = _t.time()
    n = PM_DEAD_COUNT.get(slug, 0) + 1
    PM_DEAD_COUNT[slug] = n
    ttl = min(PM_DEAD_TTL * (2 ** (n - 1)), 7200.0)  # escalate repeat offenders
    PM_DEAD[slug] = now + ttl
    PM_DEATHS.append(now)
    del PM_DEATHS[:-50]
    recent = sum(1 for t0 in PM_DEATHS if now - t0 < CIRCUIT_WINDOW)
    print(f"  *** PM BOOK UNTRADEABLE {slug} ({why}) — blacklisted {ttl:.0f}s "
          f"(x{n}) ***", flush=True)
    if recent >= CIRCUIT_DEATHS and not circuit_open():
        CIRCUIT_OPEN_UNTIL[0] = now + CIRCUIT_HALT
        print(f"  *** CIRCUIT BREAKER OPEN: {recent} PM books died in "
              f"{CIRCUIT_WINDOW/60:.0f}min — ALL execution halted {CIRCUIT_HALT/60:.0f}min "
              f"(venue-wide incident) ***", flush=True)


NAKED_FILE = pathlib.Path("data/exec/sports_naked.json")
MIN_REHEDGE_LOCK = 0.005   # re-hedge a naked hold only if it locks >= 0.5c/ct


def load_naked():
    try:
        return json.loads(NAKED_FILE.read_text())
    except (OSError, ValueError):
        return []


def save_naked(holds):
    with contextlib.suppress(OSError):
        NAKED_FILE.write_text(json.dumps(holds, indent=1))


def _confirm_ioc_fill(pg, oid, qty, tries=10, wait=0.5):
    """Authoritative IOC fill count: poll filled_qty, then confirm against the
    order record (cumQuantity). PM's fill reporting lags — a premature 0 here
    caused the 2026-07-22 refire runaway (130 shorts from a 2-lot hedge)."""
    hf = 0
    for _ in range(tries):
        with contextlib.suppress(Exception):
            hf = pg.filled_qty(oid)
        if hf >= qty:
            return int(hf)
        time.sleep(wait)
    with contextlib.suppress(Exception):
        o = pg.get_order(oid)
        hf = max(hf, int(float(o.get("cumQuantity") or 0)))
    return int(hf)



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
            oid = (r.get("order") or {}).get("order_id") or r.get("order_id") or ""
            _intent(place=kt, venue="kalshi", side="bid", price=str(px), count=q, order_id=oid)
            return _confirm_ioc_fill(kg, oid, q)
        r = pg.place_yes(slug, "bid", px, q, post_only=False)
        oid = r.get("id") or r.get("order_id") or ""
        _intent(place=slug, venue="polymarket_us", side="bid", price=str(px), count=q, order_id=oid)
        return _confirm_ioc_fill(pg, oid, q)

    def sell(v, px, q):
        if v == "k":
            r = kg.place_yes(kt, "ask", px, q, post_only=False)
            oid = (r.get("order") or {}).get("order_id") or r.get("order_id") or ""
            _intent(place=kt, venue="kalshi", side="ask", price=str(px), count=q, order_id=oid)
            return _confirm_ioc_fill(kg, oid, q)
        r = pg.place_short(slug, px, q, post_only=False)
        oid = r.get("id") or r.get("order_id") or ""
        _intent(place=slug, venue="polymarket_us", side="ask", price=str(px), count=q, order_id=oid)
        return _confirm_ioc_fill(pg, oid, q)

    print(f"[{time.strftime('%H:%M:%S')}] CAPTURE {key} x{qty}: buy {cheap}@{buy_px} then sell {dear}@{sell_px}", flush=True)
    try:
        f1 = buy(cheap, buy_px, qty)
        if f1 < 1:
            print(f"  leg1 (buy {cheap}) didn't fill (price moved) — abort, nothing naked", flush=True)
            return
        f2 = sell(dear, sell_px, f1)
        if f2 < f1:  # couldn't hedge all -> flatten the excess long on the cheap venue
            excess = f1 - f2
            if f2 == 0 and dear == "pm":
                # a marketable IOC at the displayed touch that fills NOTHING
                # means the PM book is frozen — stop trusting this market
                mark_pm_dead(slug, "leg2 IOC expired at displayed touch")
            print(f"  *** leg2 hedged {f2}/{f1}; FLATTENING {excess} on {cheap} ***", flush=True)
            fl = sell(cheap, Decimal("0.01"), excess)  # marketable dump to close the naked long
            if fl < excess:
                # flatten ALSO missed — register the remainder as a watched
                # naked hold so the rehedge watcher works it profit-only
                rem = excess - fl
                holds = load_naked()
                holds.append({"ts": time.time(), "game": key,
                              "side": "bid" if cheap == "k" else "ask",
                              "qty": int(rem), "ref_px": float(buy_px),
                              "kt": kt, "slug": slug})
                save_naked(holds)
                print(f"  *** flatten missed {rem} — registered as watched naked hold ***", flush=True)
            alerter.alert(f"arbbot SPORTS leg2 short {key}: hedged {f2}/{f1}, flattened {excess}")
        state["pos"][key] = state["pos"].get(key, 0) + f1  # cap on INVENTORY, not hedged qty
        if f2 < 1:
            return  # nothing completed — never write a qty-0 basket record
        # PM-LEAN rider: extra unhedged contracts of the PM leg (the side that
        # keeps winning). Only in the sell-PM direction; profit-only salvage
        # via the generic hedge service; settlement decides otherwise.
        if dear == "pm" and lean_used() < LEAN_CAP:
            ln = min(LEAN_CLIP, LEAN_CAP - lean_used())
            try:
                lr = pg.place_short(slug, sell_px, ln, post_only=False)
                loid = lr.get("id") or lr.get("order_id") or ""
                _intent(place=slug, venue="polymarket_us", side="ask",
                        price=str(sell_px), count=ln, order_id=loid)
                lf = _confirm_ioc_fill(pg, loid, ln)
            except Exception as e:
                print(f"  lean rider failed: {type(e).__name__}: {e}", flush=True)
                lf = 0
            if lf >= 1:
                lean_add(int(lf))
                lg0 = g.get("league", "mlb")
                lrec = {"ts": time.time(), "relationship_id": f"sports-lean-{lg0}-{key}",
                        "title": f"{lg0.upper()} {key} (PM-lean rider)", "qty": int(lf),
                        "strategy": "pm-lean", "status": "open",
                        "kalshi_ref": kt,
                        "legs": [{"venue": "polymarket_us", "market_id": slug, "side": "no",
                                  "role": "taker", "qty": int(lf), "yes_price": str(sell_px)}],
                        "cost_usd": float((Decimal(1) - sell_px) * lf),
                        "payoff_usd": float(lf), "profit_usd": 0.0}
                with open("data/exec/trades.jsonl", "a") as fh:
                    fh.write(json.dumps(lrec) + "\n")
                print(f"  LEAN rider x{lf} PM short @ {sell_px} [{lean_used()}/{LEAN_CAP}]", flush=True)
        prof = float((sell_px - buy_px) * f2) - FEE * f2
        alerter.alert(f"arbbot SPORTS CAPTURE {key} x{f2} +${prof:.2f} riskless")
        lg = g.get("league", "mlb")
        rec = {"ts": time.time(), "relationship_id": f"sports-{lg}-{key}",
               "title": f"{lg.upper()} {key} (cross-venue)",
               "qty": int(f2), "strategy": "sports-take-take", "resolves_by": None, "resolves_estimated": False,
               "legs": [{"venue": "kalshi", "market_id": kt, "role": "taker",
                         "side": "no" if dear == "k" else "yes", "qty": int(f2), "yes_price": str(sell_px if dear == "k" else buy_px)},
                        {"venue": "polymarket_us", "market_id": slug, "role": "taker",
                         "side": "yes" if dear == "k" else "no", "qty": int(f2), "yes_price": str(sell_px if dear == "pm" else buy_px)}],
               "cost_usd": float(min(buy_px, Decimal(1) - sell_px) * f2), "payoff_usd": float(f2),
               "profit_usd": prof, "status": "open"}
        with open("data/exec/trades.jsonl", "a") as fh:
            fh.write(json.dumps(rec) + "\n")
        print(f"  CAPTURED {key} x{f2}  +${prof:.2f} riskless (recorded)", flush=True)
    except Exception as e:
        print(f"  *** SPORTS EXEC ERROR {key}: {type(e).__name__}: {e} — 60s recon net will flag any naked leg ***", flush=True)


MAP_FILE = pathlib.Path("data/scan/sports_equiv_map.json")
MAP_LEAGUES = {"itfme", "itfwo", "wta", "atp", "kbo", "npb", "mlb"}  # 2-way only


def map_games():
    """Trade-ready pairings from the generic equivalence map
    (sports_equiv_discovery.py) — every 2-way league, TAKE-TAKE only.
    Same shape as todays_games entries: kt = Kalshi market whose YES == the
    PM moneyline's YES (long) side."""
    if not MAP_FILE.exists():
        return {}
    doc = json.loads(MAP_FILE.read_text())
    today = datetime.date.today()
    out = {}
    for m in doc.get("matches", []):
        if m.get("league") not in MAP_LEAGUES:
            continue
        if not (m.get("pm_moneyline") and m.get("kalshi_long_ticker")):
            continue
        try:
            gdate = datetime.date.fromisoformat(m.get("date") or "")
        except ValueError:
            continue
        if not (today <= gdate <= today + datetime.timedelta(days=1)):
            continue
        a, _, b = (m.get("teams") or "").partition(" vs ")
        start_ts = None
        if m.get("start_time"):
            try:
                start_ts = datetime.datetime.fromisoformat(
                    m["start_time"].replace("Z", "+00:00")).timestamp()
            except ValueError:
                pass
        key = f"{m['league']}:{m['pm_moneyline']}"
        out[key] = {"kt": m["kalshi_long_ticker"], "slug": m["pm_moneyline"],
                    "away": (m.get("pm_long_team") or a)[:20],
                    "home": (b if m.get("pm_long_team", a) != b else a)[:20],
                    "league": m["league"], "start_ts": start_ts}
    return out


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

                async def _healer():
                    n = 0
                    while True:
                        await asyncio.sleep(2)
                        req = [sl for sl, t0 in PM_RESUB_REQ.items()
                               if time.time() - t0 < PM_HEAL_TIMEOUT and sl in slugs]
                        if req:
                            n += 1
                            await ws.send(subscribe_frame(f"heal{n}", "SUBSCRIPTION_TYPE_MARKET_DATA",
                                                          req[:20]))
                heal_task = asyncio.create_task(_healer())
                async for raw in ws:
                    md = json.loads(raw).get("marketData")
                    if not md:
                        continue
                    bids = [float(b["px"]["value"]) for b in (md.get("bids") or [])]
                    # PM US denotes the ask side as "offers" (not "asks")
                    asks = [float(a["px"]["value"]) for a in (md.get("offers") or md.get("asks") or [])]
                    pm_bbo[md.get("marketSlug")] = (max(bids) if bids else None, min(asks) if asks else None)
                    pm_note_tick(md.get("marketSlug"))
        except Exception as e:
            print(f"[pmus ws] {type(e).__name__}: {e}; reconnect", flush=True)
            await asyncio.sleep(2)
        finally:
            with contextlib.suppress(Exception):
                heal_task.cancel()


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
            # top-N take-take edges (rows already sorted by edge desc) so the
            # dashboard can draw rank series, not just the single best
            tops = [{"g": r["game"][:26], "e": r["edge_c"]} for r in live[:5]]
            try:
                with hist.open("a") as f:
                    f.write(json.dumps({"t": int(time.time()), "best": best["edge_c"],
                                        "best_mt": best_mt, "game": best["game"],
                                        "n": len(live), "tops": tops}) + "\n")
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
            ews = sorted(v for v in PM_TICK_EWMA.values() if v is not None)
            med = ews[len(ews) // 2] if ews else None
            p90 = ews[int(len(ews) * 0.9)] if ews else None
            print(f"[{time.strftime('%H:%M:%S')}] hb: {n}/{len(games)} live books | "
                  f"best edge {best*100:+.1f}c [{bestg}] | pm cadence med={med and round(med,1)}s "
                  f"p90={p90 and round(p90,1)}s dead={sum(1 for v in PM_DEAD.values() if v > time.time())}", flush=True)
        for g in games.values():
            kbook = books.get("kalshi", g["kt"])
            pm = pm_bbo.get(g["slug"])
            if kbook is None or pm is None or None in pm or pm_dead(g["slug"]) \
                    or not pm_fresh(g["slug"]):
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
                    if live and not circuit_open() and not probe_owned(g["slug"]):
                        execute(g, kbid, kask, pbid, pask, kg, pg, alerter, state)


def _mt_candidates(games, books, pm_bbo):
    """Every (game, side) whose make-take lock currently LOOKS profitable.
    bid side: rest Kalshi buy-YES @ k_bid, hedge = sell PM YES @ p_bid.
    ask side: rest Kalshi sell-YES @ k_ask, hedge = buy PM YES @ p_ask.
    A >6c spike can be on either side — quoting both casts the widest net."""
    out = []
    now = time.time()
    for g in games.values():
        # HORIZON GUARD (Geoff 2026-07-22): only probe games that resolve soon —
        # started already or starting within 12h. A game with an unknown start
        # is only trusted when it came from native MLB discovery (today-only by
        # construction). Never rest a maker on anything long-dated.
        st = g.get("start_ts")
        if st is not None:
            if st > now + 12 * 3600:   # starts more than 12h from now
                continue
        elif g.get("league", "mlb") != "mlb":
            continue
        kbook = books.get("kalshi", g["kt"]); pm = pm_bbo.get(g["slug"])
        if not (kbook and kbook.best_bid() and kbook.best_ask() and pm and None not in pm):
            continue
        if pm_dead(g["slug"]):
            continue  # frozen PM book -> no hedge anchor -> no probe quote
        kbid, kask = float(kbook.best_bid().price), float(kbook.best_ask().price)
        pbid, pask = pm
        if (pask - pbid) > MT_HEDGE_MAX_SPREAD:
            continue  # no trustworthy hedge anchor -> the "edge" is illusory
        for side, exp, ref, hedge_ref in (
                ("bid", pbid - kbid - MT_FEE, kbid, pbid),
                ("ask", kask - pask - MT_FEE, kask, pask)):
            if exp >= MT_MIN_EXP:
                out.append({"g": g, "key": f"{g['away']}@{g['home']}", "side": side,
                            "exp": exp, "ref": ref, "hedge_ref": hedge_ref,
                            "style": "touch"})
            # deep arm RETIRED after round 1 (28 placements, 0 fills in 90
            # min — informed flow takes the touch, it doesn't sweep the pool)
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
            if o.get("status") == "resting" and any(str(o.get("ticker", "")).startswith(px)
                    for px in ("KXMLBGAME", "KXITF", "KXWTA", "KXATP", "KXKBO", "KXNPB")):
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
                _intent(place=g["slug"], venue="polymarket_us", side="ask",
                        price=str(limit), count=int(f),
                        order_id=hr.get("id") or hr.get("order_id"))
                hf = _confirm_ioc_fill(pg, hr.get("id") or hr.get("order_id") or "", int(f))
                if hf:
                    fill_px = pm[0] if (pm and pm[0] is not None) else limit
                    realized = fill_px - o["ref"] - MT_FEE
            else:                                      # short Kalshi YES -> buy PM YES
                limit = round(o["ref"] - MT_FEE, 2)    # breakeven buy price
                hr = pg.place_yes(g["slug"], "bid", Decimal(str(limit)), int(f), post_only=False)
                _intent(place=g["slug"], venue="polymarket_us", side="bid",
                        price=str(limit), count=int(f),
                        order_id=hr.get("id") or hr.get("order_id"))
                hf = _confirm_ioc_fill(pg, hr.get("id") or hr.get("order_id") or "", int(f))
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
               "style": o.get("style", "touch"),
               "hedged": int(hf), "naked": naked, "ref_px": o["ref"],
               "exp_lock_c": round(o["exp"] * 100, 2),
               "captured_lock_c": (round(realized * 100, 2) if realized is not None else None),
               "picked_off": bool(picked)}
        with contextlib.suppress(OSError):
            with rec_path.open("a") as fh:
                fh.write(json.dumps(rec) + "\n")
        if picked:
            holds = load_naked()
            holds.append({"ts": time.time(), "game": o["key"], "side": side,
                          "qty": naked, "ref_px": o["ref"],
                          "kt": g["kt"], "slug": g["slug"]})
            save_naked(holds)
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
                _intent(cancel=o["g"]["kt"], venue="kalshi", side=o["side"],
                        price=f"{o['ref']:.2f}", order_id=o["oid"])
                del active[k]; cooldown[k] = now + MT_REQUOTE_COOLDOWN
                hedge_and_record(o, f)
                continue
            kbook = books.get("kalshi", o["g"]["kt"]); pm = pm_bbo.get(o["g"]["slug"])
            deep = o.get("style") == "deep"
            stale = now - o["t0"] > (MT_DEEP_TTL if deep else MT_QUOTE_TTL)
            if kbook and kbook.best_bid() and kbook.best_ask() and pm and None not in pm:
                kbid, kask = float(kbook.best_bid().price), float(kbook.best_ask().price)
                if deep:
                    # deep quotes NEVER reprice on being outbid (that's the
                    # design) — only when PM drifts toward us and the would-be
                    # lock shrinks below the floor
                    lock_now = (pm[0] - o["ref"] - MT_FEE) if o["side"] == "bid" \
                               else (o["ref"] - pm[1] - MT_FEE)
                    stale = stale or lock_now < MT_DEEP_STALE
                elif o["side"] == "bid":
                    stale = stale or kbid > o["ref"] + 1e-9 or (pm[0] - o["ref"] - MT_FEE) < MT_MIN_EXP
                else:
                    stale = stale or kask < o["ref"] - 1e-9 or (o["ref"] - pm[1] - MT_FEE) < MT_MIN_EXP
            if stale:
                with contextlib.suppress(Exception):
                    kg.cancel(o["oid"])
                _intent(cancel=o["g"]["kt"], venue="kalshi", side=o["side"],
                        price=f"{o['ref']:.2f}", order_id=o["oid"])
                del active[k]; cooldown[k] = now + MT_REQUOTE_COOLDOWN
        # 2) top up the net with new quotes (respect concurrency + lifetime cap)
        if filled_total >= MT_LIFETIME_CAP:
            if not active:
                break
            continue
        for c in _mt_candidates(games, books, pm_bbo):
            if len(active) >= MT_MAX_CONCURRENT:
                break
            style = c.get("style", "touch")
            if sum(1 for a in active.values() if a.get("style", "touch") == style) >= MT_PER_STYLE:
                continue
            k = (c["key"], c["side"], style)
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
                         "ref": c["ref"], "hedge_ref": c["hedge_ref"], "exp": c["exp"],
                         "style": style, "t0": now}
            _intent(place=c["g"]["kt"], venue="kalshi", side=side,
                    price=f"{c['ref']:.2f}", count=MT_CLIP, order_id=oid)
            print(f"[{time.strftime('%H:%M:%S')}] MT-QUOTE[{style}] {c['key']} {side} {MT_CLIP}@{c['ref']:.2f} "
                  f"(exp {c['exp']*100:+.1f}c) [{len(active)} resting]", flush=True)
    if filled_total >= MT_LIFETIME_CAP:
        print(f"[{time.strftime('%H:%M:%S')}] MT-PROBE lifetime cap reached ({filled_total}) "
              f"— quoting stopped. Results: data/exec/sports_mt_probe.jsonl", flush=True)


async def rehedge_watcher(pm_bbo, books, stop, live, pg, kg, alerter):
    """Actively re-hedge accidentally-naked sports legs when the match swings
    back (Geoff 2026-07-22): a probe pickoff holds e.g. long Kalshi YES @ref;
    if PM's bid later returns above ref+fee+margin, sell PM YES IOC and turn
    the naked hold into a completed profitable basket. Profit-only — never
    locks a loss; holds simply ride to resolution if the hedge never returns.
    State survives restarts via data/exec/sports_naked.json."""
    while not stop.is_set():
        await asyncio.sleep(2)
        holds = load_naked()
        if not holds:
            continue
        changed = False
        now = time.time()
        for h in list(holds):
            # NOTE: no early skip when PM's feed is absent — the FLATTEN exit
            # (same-venue sell-back) must still be evaluated; only the hedge
            # branch needs a live PM book.
            pm = pm_bbo.get(h["slug"])
            pbid, pask = pm if (pm and None not in pm) else (None, None)
            qty = int(h["qty"])
            if qty < 1:
                holds.remove(h); changed = True
                continue
            if now < h.get("next_try", 0):
                continue  # attempt cooldown — never machine-gun a laggy venue
            if pm_dead(h["slug"]):
                pass  # frozen PM book: hedge unavailable, but FLATTEN may be
            # FLATTEN exit (Geoff 2026-07-22): selling the naked leg back on
            # its OWN venue at a profit beats waiting for the cross-venue
            # hedge — take whichever profit-only exit pays more right now.
            kbook = books.get("kalshi", h["kt"])
            flat_lock = None
            if kbook and kbook.best_bid() and kbook.best_ask():
                if h["side"] == "bid":     # long Kalshi YES: flatten = sell @ bid
                    flat_lock = float(kbook.best_bid().price) - h["ref_px"] - MT_FEE
                else:                       # short Kalshi YES: flatten = buy @ ask
                    flat_lock = h["ref_px"] - float(kbook.best_ask().price) - MT_FEE
            hedge_lock = None
            pm_ok = (not pm_dead(h["slug"])) and pbid is not None and pask is not None \
                    and pm_fresh(h["slug"]) and not circuit_open()
            if pm_ok:
                hedge_lock = (pbid - h["ref_px"] - MT_FEE) if h["side"] == "bid" \
                             else (h["ref_px"] - pask - MT_FEE)
            if flat_lock is not None and flat_lock >= MIN_REHEDGE_LOCK and \
                    (hedge_lock is None or flat_lock >= hedge_lock):
                if not live:
                    continue
                try:
                    if h["side"] == "bid":
                        px = Decimal(str(round(h["ref_px"] + MT_FEE + MIN_REHEDGE_LOCK, 2)))
                        r2 = kg.place_yes(h["kt"], "ask", px, qty, post_only=False)
                    else:
                        px = Decimal(str(round(h["ref_px"] - MT_FEE - MIN_REHEDGE_LOCK, 2)))
                        r2 = kg.place_yes(h["kt"], "bid", px, qty, post_only=False)
                    oid2 = (r2.get("order") or {}).get("order_id") or r2.get("order_id") or ""
                    _intent(place=h["kt"], venue="kalshi",
                            side="ask" if h["side"] == "bid" else "bid",
                            price=str(px), count=qty, order_id=oid2)
                    ff = _confirm_ioc_fill(kg, oid2, qty)
                except Exception as e:
                    print(f"  *** FLATTEN ERROR {h['game']}: {type(e).__name__}: {e} ***", flush=True)
                    h["next_try"] = now + 60; changed = True
                    continue
                if ff >= 1:
                    print(f"[{time.strftime('%H:%M:%S')}] FLATTENED naked {h['game']} {h['side']} "
                          f"x{ff}/{qty} on Kalshi — realized ~+{flat_lock*100:.1f}c/ct roundtrip", flush=True)
                    alerter.alert(f"arbbot FLATTENED naked {h['game']} x{ff} ~+{flat_lock*100:.1f}c/ct")
                    with contextlib.suppress(OSError):
                        with open("data/exec/trades.jsonl", "a") as fh:
                            fh.write(json.dumps({
                                "ts": time.time(), "relationship_id": f"sports-flatten-{h['game']}",
                                "title": f"{h['game']} (naked flatten)", "qty": int(ff),
                                "strategy": "flatten", "status": "realized",
                                "cost_usd": round(h["ref_px"] * ff, 4),
                                "payoff_usd": round((h["ref_px"] + flat_lock) * ff, 4),
                                "profit_usd": round(flat_lock * ff, 4),
                                "realized_pnl_usd": round(flat_lock * ff, 4)}) + "\n")
                    h["qty"] = qty - int(ff)
                    if h["qty"] < 1:
                        holds.remove(h)
                    changed = True
                else:
                    h["next_try"] = now + 120; changed = True
                continue
            if not pm_ok:
                continue  # no profitable flatten and hedge venue unavailable
            try:
                if h["side"] == "bid":     # long Kalshi YES -> need to SELL PM YES
                    lock = pbid - h["ref_px"] - MT_FEE
                    if lock < MIN_REHEDGE_LOCK:
                        continue
                    limit = round(h["ref_px"] + MT_FEE + MIN_REHEDGE_LOCK, 2)
                    if not live:
                        continue
                    hr = pg.place_short(h["slug"], Decimal(str(limit)), qty, post_only=False)
                    oid = hr.get("id") or hr.get("order_id") or ""
                    _intent(place=h["slug"], venue="polymarket_us", side="ask",
                            price=str(limit), count=qty, order_id=oid)
                    hf = _confirm_ioc_fill(pg, oid, qty)
                else:                       # short Kalshi YES -> need to BUY PM YES
                    lock = h["ref_px"] - pask - MT_FEE
                    if lock < MIN_REHEDGE_LOCK:
                        continue
                    limit = round(h["ref_px"] - MT_FEE - MIN_REHEDGE_LOCK, 2)
                    if not live:
                        continue
                    hr = pg.place_yes(h["slug"], "bid", Decimal(str(limit)), qty, post_only=False)
                    oid = hr.get("id") or hr.get("order_id") or ""
                    _intent(place=h["slug"], venue="polymarket_us", side="bid",
                            price=str(limit), count=qty, order_id=oid)
                    hf = _confirm_ioc_fill(pg, oid, qty)
            except Exception as e:
                print(f"  *** REHEDGE ERROR {h['game']}: {type(e).__name__}: {e} ***", flush=True)
                h["next_try"] = now + 60; changed = True
                continue
            if hf < 1:
                # PM fill reporting lags MINUTES sometimes (2026-07-22 runaway:
                # an unguarded refire every 2s stacked 130 shorts) — back off
                # hard and reconcile next round with the authoritative order.
                # A confirmed zero on a marketable IOC also means the book may
                # be FROZEN — blacklist it for a while.
                mark_pm_dead(h["slug"], "rehedge IOC confirmed zero")
                h["next_try"] = now + 120; changed = True
                continue
            if hf >= 1:
                # record at the ACTUAL average fill price (order record), never
                # the IOC limit — limit-priced legs made cost and profit
                # disagree on the dashboard (Geoff caught it 2026-07-22)
                fill_px = None
                with contextlib.suppress(Exception):
                    o2 = pg.get_order(oid)
                    v = (o2.get("avgPx") or {}).get("value")
                    if v:
                        fill_px = float(v)
                if fill_px is None:
                    fill_px = float(limit)
                if h["side"] == "bid":
                    lock = fill_px - h["ref_px"] - MT_FEE
                else:
                    lock = h["ref_px"] - fill_px - MT_FEE
                limit = fill_px  # legs/cost/profit all derive from the same price
                locked = round(lock * 100, 1)
                print(f"[{time.strftime('%H:%M:%S')}] REHEDGED {h['game']} {h['side']} "
                      f"x{hf}/{qty} @{fill_px} — naked hold converted, ~+{locked}c/ct", flush=True)
                alerter.alert(f"arbbot REHEDGED naked {h['game']} x{hf} ~+{locked}c/ct")
                rec = {"ts": time.time(), "relationship_id": f"sports-rehedge-{h['game']}",
                       "title": f"{h['game']} (naked rehedge)", "qty": int(hf),
                       "strategy": "sports-take-take", "resolves_by": None,
                       "resolves_estimated": False,
                       "legs": [{"venue": "kalshi", "market_id": h["kt"],
                                 "side": "yes" if h["side"] == "bid" else "no", "role": "maker",
                                 "qty": int(hf), "yes_price": str(h["ref_px"])},
                                {"venue": "polymarket_us", "market_id": h["slug"],
                                 "side": "no" if h["side"] == "bid" else "yes", "role": "taker",
                                 "qty": int(hf), "yes_price": str(limit)}],
                       "cost_usd": float((Decimal(str(h["ref_px"])) + (Decimal(1) - Decimal(str(limit)))) * hf)
                                   if h["side"] == "bid" else
                                   float(((Decimal(1) - Decimal(str(h["ref_px"]))) + Decimal(str(limit))) * hf),
                       "payoff_usd": float(hf),
                       "profit_usd": round(lock * hf, 4), "status": "open"}
                with contextlib.suppress(OSError):
                    with open("data/exec/trades.jsonl", "a") as fh:
                        fh.write(json.dumps(rec) + "\n")
                h["qty"] = qty - int(hf)
                if h["qty"] < 1:
                    holds.remove(h)
                changed = True
        if changed:
            save_naked(holds)


async def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--seconds", type=int, default=0, help="run for N seconds then exit (0=forever)")
    ap.add_argument("--mt-probe", action="store_true",
                    help="run the tiny make-take adverse-selection probe (rests real Kalshi makers)")
    args = ap.parse_args()
    games = todays_games(httpx.Client(timeout=15))
    n_mlb = len(games)
    # merge the generic equivalence map (all 2-way leagues) — native MLB
    # discovery wins on ticker collisions (it carries game-time precision)
    native_kts = {g["kt"] for g in games.values()}
    for key, g in map_games().items():
        if g["kt"] not in native_kts:
            games[key] = g
    by_lg = {}
    for g in games.values():
        by_lg[g.get("league", "mlb")] = by_lg.get(g.get("league", "mlb"), 0) + 1
    print(f"tracking {len(games)} games via WS ({'LIVE' if args.live else 'DRY'}) "
          f"— native MLB {n_mlb}, map: {by_lg}", flush=True)
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
             asyncio.create_task(detector(games, books, pm_bbo, stop, args.live, kg, pg, alerter, state)),
             asyncio.create_task(rehedge_watcher(pm_bbo, books, stop, args.live, pg, kg, alerter))]
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
    # systemd's 6h recycle sends SIGTERM — convert it so main()'s finally
    # block runs and cancels any resting probe quotes instead of orphaning
    # them as GTC makers on wide books (naked-fill risk).
    import signal
    signal.signal(signal.SIGTERM, lambda *_: (_ for _ in ()).throw(KeyboardInterrupt()))
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
