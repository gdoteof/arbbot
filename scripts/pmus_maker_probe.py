"""PM-US sports maker probe, Kalshi-anchored, hedge-on-fill (locked spread).

Edge (validated in sim, pessimistic fill proxy, 2026-07-23 night tape):
PM-US sports lags Kalshi by 5-30s and PM-US MAKERS PAY ZERO FEES. Rest
post-only quotes at kalshi_mid +/- --margin on mapped moneylines; when PM-US
flow crosses into us, IMMEDIATELY hedge the exact qty on Kalshi IOC — payoff
locks at (hedge_px - fill_px) per contract regardless of outcome
(sim: +1.2c/contract mean, ~90% win at margin 0.04).

Guards (tail control from sim): skip quoting when kalshi spread > 0.03, when
kalshi mid moved > 0.02 in the last 5s, or price outside [0.10, 0.90];
per-pair stop after -$0.25 cumulative; basket + session caps; data/KILL.

Ledger: two-leg take-take-shaped baskets (PM-US YES + Kalshi NO or the
mirror), so the settle sweeper realizes them at game finalization.

    .venv313/bin/python scripts/pmus_maker_probe.py            # dry run
    bash scripts/launch_pmus_maker_probe.sh                    # live
"""

import argparse
import asyncio
import contextlib
import json
import time
from collections import defaultdict, deque
from decimal import Decimal
from pathlib import Path

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.exec.ledgerdb import dual_append
from arbbot.models.core import BookDelta, BookSnapshot, Trade
from arbbot.record.jsonl import parse_event

SOCKET_PATH = "data/arbbot-sports.sock"
SPORTS_MAP = Path("data/scan/sports_equiv_map.json")
PROBE_LOG = Path("data/exec/pmus_maker_probe.jsonl")
LEDGER = Path("data/exec/trades.jsonl")
KILL = Path("data/KILL")


def kalshi_taker_fee(px: float, qty: int) -> float:
    import math
    return math.ceil(7.0 * qty * px * (1 - px)) / 100.0


def load_pairs():
    """Sports-map pairs, EXCLUDING past-dated slugs (joint recon 6b54d1ce:
    quoting yesterday's match against a frozen anchor produced the orphaned
    fills — a slug's trailing date must be today-or-later UTC)."""
    import re
    today = time.strftime("%Y-%m-%d", time.gmtime())
    smap = json.loads(SPORTS_MAP.read_text())
    out = {}
    for m in smap.get("matches", []):
        kt, pm = m.get("kalshi_long_ticker"), m.get("pm_moneyline")
        if not (kt and pm):
            continue
        dm = re.search(r"(\d{4}-\d{2}-\d{2})$", pm)
        if dm and dm.group(1) < today:
            continue
        out[pm] = {"kalshi": kt, "pm": pm, "league": m["league"],
                   "teams": m.get("teams", "")}
    return out


def top(book):
    bid = max(book.bids, key=lambda l: l.price, default=None)
    ask = min(book.asks, key=lambda l: l.price, default=None)
    if bid is None or ask is None or ask.price <= bid.price:
        return None
    return float(bid.price), float(ask.price)


class PairState:
    def __init__(self, pair):
        self.pair = pair
        self.orders = {}          # side -> {id, px, qty, cum}
        self.last_reprice = defaultdict(float)
        self.kmids = deque()      # (ts_ns, mid) 5s window
        self.pnl = 0.0
        self.stopped = False
        self.last_fill = defaultdict(float)
        self.status = None


class MakerProbe:
    def __init__(self, args):
        self.args = args
        self.pairs = load_pairs()
        self.books = BookBuilder()
        self.ps = {pm: PairState(p) for pm, p in self.pairs.items()}
        self.session_fills = 0
        self.realized = 0.0
        self.open_baskets = 0
        self.stopped = False
        self.pending_confirm = []   # (t0, delays, st, side, cur)
        self.hedged_net = defaultdict(int)  # slug -> expected net PM position
        self.by_oid = {}            # order_id -> (st, side, cur) for WS fills
        self.us_id = self.us_key = None
        self.pm_gw = None
        self.k_gw = None
        if args.live:
            from arbbot.ops.config import load_credential
            from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
            from arbbot.exec.kalshi_gateway import KalshiOrderGateway
            from arbbot.record.kalshi import load_private_key
            us_id = load_credential("polymarket_usa_key_id")
            us_key = load_credential("polymarket_usa_private_key")
            kid = load_credential("kalshi_api_key_id")
            kpem = load_credential("kalshi_private_key.pem")
            if not (us_id and us_key and kid and kpem):
                raise SystemExit("credentials missing")
            self.us_id = us_id.decode().strip()
            self.us_key = us_key.decode().strip()
            self.pm_gw = PolymarketUsOrderGateway(
                self.us_id, self.us_key, live=True)
            self._startup_sweep()
            self.k_gw = KalshiOrderGateway(
                kid.decode().strip(), load_private_key(kpem), live=True)
            print("[LIVE] PM-US + Kalshi gateways armed", flush=True)

    def _startup_sweep(self):
        """No orphans: cancel every open order of OURS (this probe's slug
        universe) left by a previous run — a dead process's resting orders
        produced incident #2. By order id, slug-scoped (contract rule 4)."""
        import random
        orders = None
        for attempt in range(6):
            try:
                oo = self.pm_gw.open_orders()
                orders = oo if isinstance(oo, list) else oo.get("orders", [])
                break
            except Exception as e:
                if "429" in str(e) and attempt < 5:
                    # shared API budget (coordination contract): back off with
                    # jitter — MUST eventually sweep, or prior-run quotes stay
                    # orphaned on the book
                    wait = 20 + random.uniform(0, 20)
                    print(f"[SWEEP] 429 — retry in {wait:.0f}s", flush=True)
                    time.sleep(wait)
                else:
                    raise SystemExit(
                        f"startup sweep failed (refusing to quote blind): {e}")
        if orders is None:
            raise SystemExit("startup sweep failed after retries")
        mine = [o for o in orders
                if (o.get("marketSlug") or o.get("market_slug")) in self.pairs]
        for o in mine:
            oid = o.get("id") or o.get("orderId")
            slug = o.get("marketSlug") or o.get("market_slug")
            try:
                self.pm_gw.cancel(oid, market_slug=slug)
                print(f"[SWEEP] cancelled orphan {oid} on {slug}", flush=True)
            except Exception as e:
                raise SystemExit(f"orphan cancel failed for {oid}: {e} — "
                                 "refusing to start with unknown resting orders")

    def log(self, rec):
        PROBE_LOG.parent.mkdir(exist_ok=True)
        with PROBE_LOG.open("a") as f:
            f.write(json.dumps(rec) + "\n")

    def intents(self, rec):
        """Runner-schema order-lifecycle log -> dash Order-activity tab."""
        rec.setdefault("ts", time.time())
        rec.setdefault("venue", "polymarket_us")
        with Path("data/scan/trader-intents-mlmaker.jsonl").open("a") as f:
            f.write(json.dumps(rec) + "\n")

    # ---------- quoting ----------
    def guards(self, st, now_ns):
        """(kbid, kask, kmid) when quotable, else (None, reason)."""
        kt = st.pair["kalshi"]
        kb = self.books.get("kalshi", kt)
        if kb is None:
            return None, "no kalshi book"
        t = top(kb)
        if t is None:
            return None, "empty kalshi book"
        a = self.args
        kbid, kask = t
        kmid = (kbid + kask) / 2
        if kask - kbid > a.max_kspread:
            return None, f"kalshi spread {round((kask - kbid) * 100)}c"
        # frozen/settled book guard: the anchor must be alive
        if now_ns - kb.ts_local_ns > int(120e9):
            return None, f"kalshi stale {int((now_ns - kb.ts_local_ns) / 1e9)}s"
        cutoff = now_ns - int(5e9)
        while st.kmids and st.kmids[0][0] < cutoff:
            st.kmids.popleft()
        if st.kmids and abs(kmid - st.kmids[0][1]) > a.jump_standdown:
            return None, "mid-jump"
        if not (a.min_px <= kmid <= a.max_px):
            return None, f"price {kmid:.2f} outside bounds"
        return (kbid, kask, kmid), None

    async def manage(self):
        a = self.args
        while not self.stopped:
            await asyncio.sleep(2.0)
            if KILL.exists() or self.realized <= -a.max_loss_usd \
                    or self.session_fills >= a.max_fills:
                print(f"[STOP] realized={self.realized:+.2f} "
                      f"fills={self.session_fills} kill={KILL.exists()}", flush=True)
                await self.cancel_all()
                self.stopped = True
                return
            now_ns = time.time_ns()
            # cap concurrently-quoted pairs: bounds REST volume on the shared
            # API budget (placements/cancels), trading workstream has priority
            self.active_pairs = sum(1 for s in self.ps.values() if s.orders)
            for st in self.ps.values():
                try:
                    await self.manage_pair(st, now_ns)
                except Exception as e:
                    print(f"[{st.pair['pm']}] error {type(e).__name__}: {e}",
                          flush=True)
            await self.run_pending_confirms()

    async def run_pending_confirms(self):
        now = time.time()
        keep = []
        for t0, delays, st, side, cur in self.pending_confirm:
            if delays and now - t0 >= delays[0]:
                delays = delays[1:]
                with contextlib.suppress(Exception):
                    await self.settle_fill_delta(st, side, cur)
            if delays:
                keep.append((t0, delays, st, side, cur))
        self.pending_confirm = keep

    async def pmus_fill_ws(self):
        """Primary fill signal: PM-US private ORDER WS (same pattern as the
        trading runner). Event-driven, near-zero REST budget, immune to the
        fill-reporting poll lag. Filters to OUR order ids — the runner shares
        this account and sees its own stream."""
        if not self.args.live:
            return
        import websockets
        from arbbot.record.polymarket_us import ws_auth_headers
        path = "/v1/ws/private"
        url = "wss://api.polymarket.us" + path
        while not self.stopped:
            try:
                async with websockets.connect(
                        url,
                        additional_headers=ws_auth_headers(self.us_id, self.us_key, path),
                        ping_interval=20, open_timeout=10) as ws:
                    await ws.send(json.dumps({"subscribe": {
                        "requestId": "probe-orders",
                        "subscriptionType": "SUBSCRIPTION_TYPE_ORDER"}}))
                    print("[WS] private ORDER stream connected", flush=True)
                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except ValueError:
                            continue
                        ex = (msg.get("orderSubscriptionUpdate") or {}).get("execution") or {}
                        if ex.get("type") not in ("EXECUTION_TYPE_FILL",
                                                  "EXECUTION_TYPE_PARTIAL_FILL"):
                            continue
                        order = ex.get("order") or {}
                        oid = order.get("id")
                        ent = self.by_oid.get(oid)
                        if not ent:
                            continue  # not ours (runner/sports engine order)
                        st, side, cur = ent
                        try:
                            cum = int(float(order.get("cumQuantity") or 0))
                        except (TypeError, ValueError):
                            continue
                        new = cum - cur["cum"]
                        if new > 0:
                            cur["cum"] = cum
                            st.last_fill[side] = time.time()
                            await self.hedge(st, side, new, cur["px"], cur)
            except asyncio.CancelledError:
                raise
            except Exception as e:
                print(f"[WS] dropped: {type(e).__name__}: {e}; reconnecting",
                      flush=True)
                await asyncio.sleep(3)

    async def reconcile_positions(self):
        """Authoritative backstop: venue positions vs expectation every 120s
        (shared API budget — back off with jitter on 429). Any unexpected
        net -> immediate flatten + alert."""
        import random
        pth = "/v1/portfolio/positions"
        while not self.stopped:
            await asyncio.sleep(120)
            if not self.args.live:
                continue
            try:
                # off-loop (to_thread) AND priority="critical": a budget
                # time.sleep would stall the event loop that services WS
                # fills/hedges; the 429 jittered backoff below is this
                # loop's throttle.
                r = await asyncio.to_thread(
                    self.pm_gw.session.get, pth, priority="critical")
                r.raise_for_status()
                positions = r.json().get("positions", {})
            except Exception as e:
                if "429" in str(e):
                    extra = 60 + random.uniform(0, 120)
                    print(f"[RECON] 429 — backing off {extra:.0f}s", flush=True)
                    await asyncio.sleep(extra)
                else:
                    print(f"[RECON] read failed: {type(e).__name__}: {e}",
                          flush=True)
                continue
            for slug, st in self.ps.items():
                net = 0
                p = positions.get(slug)
                if p:
                    with contextlib.suppress(Exception):
                        net = int(float(p.get("netPosition", 0) or 0))
                # joint-recon fix (6b54d1ce): fold OWN order fills into
                # expected before calling anything unexpected — the previous
                # blind comparison flattened the probe's own unrecognized
                # fills in a loop.
                own = self.hedged_net[slug]
                for oid, (ost, oside, ocur) in list(self.by_oid.items()):
                    if ost.pair["pm"] != slug:
                        continue
                    with contextlib.suppress(Exception):
                        o = await asyncio.to_thread(self.pm_gw.get_order, oid)
                        cum = int(float(o.get("cumQuantity", 0) or 0))
                        new = cum - ocur["cum"]
                        if new > 0:  # late-recognized fill: hedge it now
                            ocur["cum"] = cum
                            await self.hedge(ost, oside, new, ocur["px"], ocur)
                            own = self.hedged_net[slug]
                unexpected = net - own
                if unexpected != 0 and not getattr(st, "recon_latched", False):
                    st.recon_latched = True  # one corrective action per pair
                    st.stopped = True        # and stop quoting it
                    print(f"[RECON-NAKED] {slug} net={net} expected={own} — "
                          f"flattening {unexpected} ONCE, pair stopped", flush=True)
                    self.log({"ts": time.time(), "action": "recon_naked",
                              "pm": slug, "net": net, "expected": own})
                    await self.flatten_unexpected(st, unexpected)

    async def flatten_unexpected(self, st, qty):
        """qty>0: unexpected long -> offset short; qty<0: unexpected short ->
        buy back. IOC with a 2c pad through the touch."""
        kb = self.books.get("polymarket_us", st.pair["pm"])
        t = top(kb) if kb else None
        if t is None:
            print(f"[RECON] no book for {st.pair['pm']} — will retry", flush=True)
            return
        try:
            if qty < 0:
                px = min(0.99, round(t[1] + 0.02, 2))
                r = await asyncio.to_thread(
                    self.pm_gw.place_yes, st.pair["pm"], "bid",
                    Decimal(str(px)), -qty, post_only=False)
            else:
                px = max(0.01, round(t[0] - 0.02, 2))
                r = await asyncio.to_thread(
                    self.pm_gw.place_yes, st.pair["pm"], "ask",
                    Decimal(str(px)), qty, post_only=False)
            self.log({"ts": time.time(), "action": "recon_flatten",
                      "pm": st.pair["pm"], "qty": qty,
                      "order_id": r.get("id") or r.get("order_id")})
            self.session_fills += 1
        except Exception as e:
            print(f"[RECON] flatten failed {st.pair['pm']}: "
                  f"{type(e).__name__}: {e}", flush=True)

    async def manage_pair(self, st, now_ns):
        a = self.args
        g, reason = self.guards(st, now_ns)
        pmb = self.books.get("polymarket_us", st.pair["pm"])
        pmt = top(pmb) if pmb else None
        want = {}
        room = bool(st.orders) or getattr(self, "active_pairs", 0) < 10
        if pmt is None:
            reason = reason or "no PM book"
        elif st.stopped:
            reason = "pair-stop"
        elif not room:
            reason = "pair cap (10)"
        elif g and self.open_baskets >= a.max_baskets:
            reason = "basket cap"
        if g and pmt and not st.stopped and room \
                and self.open_baskets < a.max_baskets:
            kbid, kask, kmid = g
            import math
            bid_q = math.floor((kmid - a.margin) * 100) / 100
            ask_q = math.ceil((kmid + a.margin) * 100) / 100
            if 0.03 <= bid_q <= 0.97 and pmt[1] > bid_q:
                want["buy"] = bid_q
            if 0.03 <= ask_q <= 0.97 and pmt[0] < ask_q:
                want["sell"] = ask_q
        # dashboard status snapshot (candidate-pair tab)
        st.status = {
            "pm": st.pair["pm"], "teams": st.pair["teams"],
            "league": st.pair["league"],
            "k_bid": g[0] if g else None, "k_ask": g[1] if g else None,
            "pm_bid": pmt[0] if pmt else None,
            "pm_ask": pmt[1] if pmt else None,
            "our_bid": (st.orders.get("buy") or {}).get("px", want.get("buy")),
            "our_ask": (st.orders.get("sell") or {}).get("px", want.get("sell")),
            "bid_live": "buy" in st.orders, "ask_live": "sell" in st.orders,
            "guard": reason, "pnl": round(st.pnl, 4),
        }
        s = st.status
        s["dist_buy"] = (round(s["pm_ask"] - s["our_bid"], 4)
                         if s["pm_ask"] is not None and s["our_bid"] is not None else None)
        s["dist_sell"] = (round(s["our_ask"] - s["pm_bid"], 4)
                          if s["pm_bid"] is not None and s["our_ask"] is not None else None)
        anchor = g  # (kbid, kask, kmid) at quote time, persisted with the order
        for side in ("buy", "sell"):
            cur = st.orders.get(side)
            tgt = want.get(side)
            if cur:
                await self.poll_fill(st, side)
                cur = st.orders.get(side)
            if cur and (tgt is None or abs(cur["px"] - tgt) >= 0.01):
                if time.time() - st.last_reprice[side] > 2.0:
                    await self.cancel(st, side)
                    cur = None
            if tgt is not None and not cur \
                    and time.time() - st.last_reprice[side] > 2.0 \
                    and time.time() - st.last_fill[side] > 30.0:
                await self.place(st, side, tgt, anchor)

    async def place(self, st, side, px, anchor=None):
        a = self.args
        st.last_reprice[side] = time.time()
        if not st.orders:  # pair becomes active: count against the 10-pair cap
            self.active_pairs = getattr(self, "active_pairs", 0) + 1
        rec = {"ts": time.time(), "pm": st.pair["pm"], "action": "quote",
               "side": side, "px": px, "qty": a.clip}
        if not a.live:
            rec["action"] = "dry_quote"
            st.orders[side] = {"id": "dry", "px": px, "qty": a.clip, "cum": 0}
            self.log(rec)
            return
        try:
            if side == "buy":
                r = await asyncio.to_thread(
                    self.pm_gw.place_yes, st.pair["pm"], "bid",
                    Decimal(str(px)), a.clip, post_only=True)
            else:
                r = await asyncio.to_thread(
                    self.pm_gw.place_yes, st.pair["pm"], "ask",
                    Decimal(str(px)), a.clip, post_only=True)
            oid = r.get("id") or r.get("order_id")
            cur = {"id": oid, "px": px, "qty": a.clip, "cum": 0,
                   "last_poll": time.time(),
                   # rung-1 hedge anchor (rust-bot postmortem review): the
                   # quote-time Kalshi top survives book gaps AND REST 429s
                   "anchor_bid": anchor[0] if anchor else None,
                   "anchor_ask": anchor[1] if anchor else None,
                   "anchor_ts": time.time()}
            st.orders[side] = cur
            if oid:
                self.by_oid[oid] = (st, side, cur)
            rec["order_id"] = oid
            self.intents({"place": st.pair["pm"],
                          "side": "bid" if side == "buy" else "ask",
                          "price": str(px), "count": a.clip, "order_id": oid})
        except Exception as e:
            rec["error"] = f"{type(e).__name__}: {e}"
            self.intents({"place_failed": st.pair["pm"], "reason": str(e)[:120]})
        self.log(rec)

    async def cancel(self, st, side):
        st.last_reprice[side] = time.time()
        cur = st.orders.pop(side, None)
        if not cur or cur["id"] == "dry" or not self.args.live:
            return
        self.log({"ts": time.time(), "pm": st.pair["pm"], "action": "cancel",
                  "side": side, "order_id": cur["id"]})
        ok = False
        for attempt in range(2):
            try:
                await asyncio.to_thread(self.pm_gw.cancel, cur["id"],
                                        market_slug=st.pair["pm"])
                self.intents({"cancel": st.pair["pm"],
                              "side": "bid" if side == "buy" else "ask",
                              "price": str(cur["px"]), "order_id": cur["id"]})
                ok = True
                break
            except Exception as e:
                if attempt == 0:
                    await asyncio.sleep(1.0)
                else:
                    # LOUD: a silently-failed cancel = live orphan (incident #2)
                    print(f"[CANCEL-FAIL] {cur['id']} on {st.pair['pm']}: "
                          f"{type(e).__name__}: {e}", flush=True)
                    self.log({"ts": time.time(), "action": "cancel_failed",
                              "pm": st.pair["pm"], "order_id": cur["id"]})
        # race: fills that landed before the cancel. PM fill reporting LAGS
        # (this exact hole produced the 2026-07-23 naked -5 incident), so a
        # single immediate check is not enough — park the order for delayed
        # re-checks at +3s/+10s/+30s.
        with contextlib.suppress(Exception):
            await self.settle_fill_delta(st, side, cur)
        self.pending_confirm.append(
            (time.time(), [3.0, 10.0, 30.0], st, side, cur))

    async def poll_fill(self, st, side):
        # REST fallback only — the private ORDER WS is the primary fill
        # signal. Contract with the trading workstream: poll sparingly
        # (>=30s), the shared PM-US API budget 429s under load.
        cur = st.orders.get(side)
        if not cur or cur["id"] == "dry" or not self.args.live:
            return
        if time.time() - cur.get("last_poll", 0) < 30.0:
            return
        cur["last_poll"] = time.time()
        await self.settle_fill_delta(st, side, cur)
        cur = st.orders.get(side)
        if cur and cur["cum"] >= cur["qty"]:
            st.orders.pop(side, None)

    async def settle_fill_delta(self, st, side, cur):
        got = 0
        with contextlib.suppress(Exception):
            got = await asyncio.to_thread(self.pm_gw.filled_qty, cur["id"])
        with contextlib.suppress(Exception):
            o = await asyncio.to_thread(self.pm_gw.get_order, cur["id"])
            got = max(got, int(float(o.get("cumQuantity", 0) or 0)))
        new = got - cur["cum"]
        if new > 0:
            cur["cum"] = got
            st.last_fill[side] = time.time()
            await self.hedge(st, side, new, cur["px"], cur)

    # ---------- hedge ----------
    async def hedge(self, st, side, qty, fill_px, cur=None):
        self.session_fills += 1
        kt = st.pair["kalshi"]
        rec = {"ts": time.time(), "pm": st.pair["pm"], "action": "fill",
               "side": side, "qty": qty, "px": fill_px}
        self.log(rec)
        print(f"[FILL] {st.pair['teams']} {side} {qty}@{fill_px} — hedging",
              flush=True)
        hedged = 0
        hpx_avg = 0.0
        for attempt in range(6):
            kb = self.books.get("kalshi", kt)
            t = top(kb) if kb else None
            if t is None and attempt >= 1:
                # local book gapped (bursts kill it exactly when fills come —
                # first live fill 2026-07-23 lost its hedge this way): fall
                # back to one REST snapshot for the hedge anchor
                try:
                    from arbbot.record.kalshi import KalshiCatalog
                    if not hasattr(self, "_kcat"):
                        self._kcat = KalshiCatalog()
                    snap = await self._kcat.orderbook(kt, 0)
                    bid = max(snap.bids, key=lambda l: l.price, default=None)
                    ask = min(snap.asks, key=lambda l: l.price, default=None)
                    if bid and ask and ask.price > bid.price:
                        t = (float(bid.price), float(ask.price))
                except Exception as e:
                    self.log({"ts": time.time(), "action": "hedge_rest_fallback_err",
                              "pm": st.pair["pm"], "err": str(e)[:100]})
            if t is None and attempt >= 3 and cur \
                    and cur.get("anchor_bid") is not None:
                # rung 1: quote-time anchor — stale but always available;
                # IOC limits bound the damage (no fill -> unwind rung)
                t = (cur["anchor_bid"], cur["anchor_ask"])
                self.log({"ts": time.time(), "action": "hedge_anchor_rung",
                          "pm": st.pair["pm"],
                          "anchor_age_s": round(time.time() - cur.get("anchor_ts", 0), 1)})
            if t is None:
                await asyncio.sleep(1.0)
                continue
            kbid, kask = t
            # buy fill: we are long PM YES -> sell Kalshi YES at bid
            # sell fill: short PM YES -> buy Kalshi YES at ask
            px = kbid if side == "buy" else kask
            px = max(0.01, min(0.99, round(px - 0.01 * attempt, 2))) if side == "buy" \
                else max(0.01, min(0.99, round(px + 0.01 * attempt, 2)))
            need = qty - hedged
            try:
                r = await asyncio.to_thread(
                    self.k_gw.place_yes, kt, "ask" if side == "buy" else "bid",
                    Decimal(str(px)), need, post_only=False)
                oid = (r.get("order") or {}).get("order_id") or r.get("order_id")
                self.intents({"place": kt, "venue": "kalshi",
                              "side": "ask" if side == "buy" else "bid",
                              "price": str(px), "count": need, "order_id": oid})
                await asyncio.sleep(1.0)
                f = 0
                with contextlib.suppress(Exception):
                    f = await asyncio.to_thread(self.k_gw.filled_qty, oid)
                if f:
                    hpx_avg = (hpx_avg * hedged + px * f) / (hedged + f)
                    hedged += f
                if hedged >= qty:
                    break
            except Exception as e:
                self.log({"ts": time.time(), "action": "hedge_error",
                          "pm": st.pair["pm"], "err": f"{type(e).__name__}: {e}"})
                await asyncio.sleep(0.5)
        rec2 = {"ts": time.time(), "pm": st.pair["pm"], "action": "hedge",
                "side": side, "qty": qty, "hedged": hedged, "hpx": hpx_avg}
        if hedged:
            fee = kalshi_taker_fee(hpx_avg, hedged)
            if side == "buy":
                locked = (hpx_avg - fill_px) * hedged - fee
                legs = [
                    {"venue": "polymarket_us", "market_id": st.pair["pm"],
                     "side": "yes", "role": "maker", "qty": hedged,
                     "yes_price": str(fill_px),
                     "cost": str(round(fill_px * hedged, 4))},
                    {"venue": "kalshi", "market_id": kt, "side": "no",
                     "role": "taker", "qty": hedged,
                     "avg_price": str(round(1 - hpx_avg, 4)),
                     "fees": str(fee)},
                ]
                cost = fill_px * hedged + (1 - hpx_avg) * hedged + fee
            else:
                locked = (fill_px - hpx_avg) * hedged - fee
                legs = [
                    {"venue": "polymarket_us", "market_id": st.pair["pm"],
                     "side": "no", "role": "maker", "qty": hedged,
                     "yes_price": str(fill_px),
                     "cost": str(round((1 - fill_px) * hedged, 4))},
                    {"venue": "kalshi", "market_id": kt, "side": "yes",
                     "role": "taker", "qty": hedged,
                     "avg_price": str(round(hpx_avg, 4)), "fees": str(fee)},
                ]
                cost = (1 - fill_px) * hedged + hpx_avg * hedged + fee
            self.realized += locked
            st.pnl += locked
            self.open_baskets += 1
            self.hedged_net[st.pair["pm"]] += hedged if side == "buy" else -hedged
            dual_append({
                "ts": time.time(),
                "relationship_id": f"pmm-{st.pair['pm']}",
                "title": f"PM-US maker probe: {st.pair['teams']} "
                         f"({st.pair['league']})",
                "qty": hedged, "strategy": "pmus-maker-probe",
                "legs": legs, "cost_usd": round(cost, 4),
                "payoff_usd": hedged, "profit_usd": round(hedged - cost, 4),
                "status": "open",
                "note": f"locked={locked:+.4f} margin={self.args.margin}",
            }, source="probe:pmus-maker")
            rec2["locked"] = round(locked, 4)
            print(f"[HEDGED] {st.pair['teams']} {side} {hedged}/{qty} "
                  f"@{hpx_avg:.2f} locked={locked:+.3f} "
                  f"session={self.realized:+.2f}", flush=True)
        naked = qty - hedged
        if naked > 0:
            # unwind the naked PM remainder at market (taker) — never carry
            rec2["naked"] = naked
            try:
                kb = self.books.get("polymarket_us", st.pair["pm"])
                t = top(kb) if kb else None
                if t and self.args.live:
                    if side == "buy":
                        r = await asyncio.to_thread(
                            self.pm_gw.place_yes, st.pair["pm"], "ask",
                            Decimal(str(t[0])), naked, post_only=False)
                    else:
                        r = await asyncio.to_thread(
                            self.pm_gw.place_yes, st.pair["pm"], "bid",
                            Decimal(str(t[1])), naked, post_only=False)
                    rec2["unwind_order"] = r.get("id") or r.get("order_id")
                print(f"[UNWIND] naked {naked} on {st.pair['pm']}", flush=True)
            except Exception as e:
                rec2["unwind_error"] = f"{type(e).__name__}: {e}"
        if st.pnl < -0.25:
            st.stopped = True
            print(f"[PAIR-STOP] {st.pair['pm']} pnl={st.pnl:+.2f}", flush=True)
        self.log(rec2)

    async def cancel_all(self):
        for st in self.ps.values():
            for side in ("buy", "sell"):
                await self.cancel(st, side)

    # ---------- events ----------
    async def socket_loop(self):
        pm_index = {p["kalshi"]: pm for pm, p in self.pairs.items()}
        while not self.stopped:
            try:
                reader, _ = await asyncio.open_unix_connection(SOCKET_PATH)
                print("connected to sports recorder socket", flush=True)
                while not self.stopped:
                    line = await reader.readline()
                    if not line:
                        break
                    try:
                        ev = parse_event(json.loads(line))
                    except ValueError:
                        continue
                    if isinstance(ev, BookSnapshot):
                        self.books.apply_snapshot(ev)
                    elif isinstance(ev, BookDelta):
                        with contextlib.suppress(GapDetected, NotSynced):
                            self.books.apply_delta(ev)
                    else:
                        continue
                    if ev.venue.value == "kalshi" and ev.market_id in pm_index:
                        st = self.ps[pm_index[ev.market_id]]
                        b = self.books.get("kalshi", ev.market_id)
                        t = top(b) if b else None
                        if t:
                            st.kmids.append(
                                (ev.ts_local_ns, (t[0] + t[1]) / 2))
            except (ConnectionRefusedError, FileNotFoundError):
                await asyncio.sleep(5)
            except Exception as e:
                print(f"socket error: {type(e).__name__}: {e}", flush=True)
                await asyncio.sleep(2)

    async def status_writer(self):
        """Dashboard feed: data/exec/pmus_maker_status.json every 5s."""
        out = Path("data/exec/pmus_maker_status.json")
        while not self.stopped:
            await asyncio.sleep(5)
            pairs = [st.status for st in self.ps.values() if st.status]
            doc = {
                "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "session": {
                    "live": self.args.live, "realized": round(self.realized, 4),
                    "fills": self.session_fills, "open_baskets": self.open_baskets,
                    "quoting": sum(1 for s in self.ps.values() if s.orders),
                    "candidates": len(pairs), "margin": self.args.margin,
                    "max_loss_usd": self.args.max_loss_usd,
                    "stopped": self.stopped,
                },
                "pairs": pairs,
            }
            tmp = out.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(doc))
            tmp.replace(out)

    async def run(self):
        await asyncio.gather(self.socket_loop(), self.manage(),
                             self.pmus_fill_ws(), self.reconcile_positions(),
                             self.status_writer())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--live", action="store_true")
    ap.add_argument("--margin", type=float, default=0.04)
    ap.add_argument("--clip", type=int, default=5)
    ap.add_argument("--max-baskets", type=int, default=4)
    ap.add_argument("--max-fills", type=int, default=20)
    ap.add_argument("--max-loss-usd", type=float, default=5.0)
    # guard tuning (loosened 2026-07-23 per Geoff: capture happens in the
    # messy moments; keep hedge-on-fill as the real protection)
    ap.add_argument("--max-kspread", type=float, default=0.05)
    ap.add_argument("--jump-standdown", type=float, default=0.04)
    ap.add_argument("--min-px", type=float, default=0.05)
    ap.add_argument("--max-px", type=float, default=0.95)
    args = ap.parse_args()
    asyncio.run(MakerProbe(args).run())


if __name__ == "__main__":
    main()
