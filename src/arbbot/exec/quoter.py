"""Maker-quoter for one relationship: rest a maker order on the safe leg,
hedge taker on the other leg when filled. DRY-RUN by default (the gateway
logs intended orders and never touches the venue).

Scope for proving grounds: 2-leg relationships whose maker basket is riskless
(equivalent either leg; implies -> consequent leg only). One resting clip at a
time per side; replace when the target price moves; cancel when viability
drops. On fill, immediately market the hedge on the other leg through the
TradeStateMachine so leg risk is a tracked state with a max-age alarm.

    book update ─▶ target = min(top+tick, p_max - safety)
                    │ risk ok? existing order at target?
                    ├─ no order  ─▶ place clip (FLAT ─▶ LEG1_WORKING)
                    ├─ moved     ─▶ cancel+replace
                    └─ unviable  ─▶ cancel  (─▶ FLAT)
    fill  ─────────▶ LEG1_FILLED ─▶ hedge taker on other leg ─▶ HEDGED
"""

from __future__ import annotations

import contextlib
import json
import random
import time
from pathlib import Path
from dataclasses import dataclass, field
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import Optional

from arbbot.book.builder import BookBuilder
from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.state_machine import TradeEvent, TradeStateMachine
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import Market, Venue
from arbbot.registry.model import Relationship, RelationshipType
from arbbot.risk.manager import RiskManager
from arbbot.scan.scanner import maker_ask_quote, maker_quote, no_ask_ladder, walk_cost


def maker_leg_indices(rel: Relationship) -> list[int]:
    """Which legs may carry a riskless maker quote for this relationship."""
    if rel.type in (RelationshipType.CROSS_VENUE_EQUIVALENT,
                    RelationshipType.EQUIVALENT_PAIR):
        return [0, 1]
    if rel.type is RelationshipType.IMPLIES:
        return [1]  # consequent only (proven in maker-quote validity check)
    return []


@dataclass
class RestingQuote:
    order_id: str
    side: str
    price: Decimal
    count: int
    machine: TradeStateMachine


TOXGATE_FILE = Path("data/exec/toxgate.json")
TOXGATE_MAX = 0.03          # expected adverse cost/ct above this -> don't rest
TOXGATE_MAX_AGE = 120.0     # feed older than this is ignored (fail-open)
_toxgate_cache = {"mtime": 0.0, "doc": None}


def _toxgate(market_id: str, side: str):
    """Score from the research toxicity shadow feed, or None (fail-open)."""
    try:
        mt = TOXGATE_FILE.stat().st_mtime
        if mt != _toxgate_cache["mtime"]:
            _toxgate_cache["doc"] = json.loads(TOXGATE_FILE.read_text())
            _toxgate_cache["mtime"] = mt
        doc = _toxgate_cache["doc"]
        if not doc or time.time() - doc.get("ts", 0) > TOXGATE_MAX_AGE:
            return None
        return (doc.get("markets", {}).get(market_id) or {}).get(side)
    except Exception:
        return None


@dataclass
class Quoter:
    rel: Relationship
    gateways: dict          # Venue -> order gateway (Kalshi and/or Polymarket)
    risk: RiskManager
    markets: dict
    fees: FeeSchedule
    clip: int = 5
    quote_venue: Optional[Venue] = None  # restrict maker quoting to one venue
    safety_ticks: int = 0  # extra ticks inside p_max as slippage buffer
    min_requote_s: float = 15.0  # keep a profitable resting quote this long before repricing (cut API churn)
    # APR hurdle (card 80ff7987: clip-25 fill locked 23ct at 2.5% APR — capital
    # deployed to resolution must clear the same floating bar as take-take).
    # The runner propagates min_apr = tt bar on every refresh; 0 disables.
    min_apr: float = 0.0         # %/yr the locked edge must annualize to
    resolve_years: Optional[float] = None  # time to resolution (from resolve_date)
    # anti-fingerprint jitter (0 => disabled, exact original behavior):
    price_jitter_ticks: int = 0  # rest 0..N ticks MORE PASSIVE than the aggressive cap
    size_jitter: int = 0         # rest clip - random(0..N) contracts (varies size, never exceeds clip)
    deadband_ticks: int = 0      # hold a still-profitable quote until available improvement exceeds a randomized 1..N-tick deadband
    # (market_id, side) pairs another subsystem owns right now (maker-unwind
    # exit asks, card 1ed32918) — _target returns None so any entry quote on
    # that side cancels and none rests; two order-owners on one side would
    # both react to fills (the runaway-bug shape).
    suppress: set = field(default_factory=set)
    _resting: dict[tuple[int, str], RestingQuote] = field(default_factory=dict)
    _last_quote_ts: dict[tuple[int, str], float] = field(default_factory=dict)
    _place_failed: set = field(default_factory=set)  # (i,side) backed off after a failed place
    _deadband: dict[tuple[int, str], Decimal] = field(default_factory=dict)  # current randomized deadband per key
    intents: list[dict] = field(default_factory=list)

    def _tick(self, leg) -> Decimal:
        # use the VENUE-REPORTED tick (PM US = 0.01, Kalshi = 0.01, intl PM =
        # 0.001). A wrong tick sends sub-tick prices the venue rounds, so our
        # tracked price stops matching the book and the self-exclusion breaks.
        m = self.markets.get((leg.venue, leg.market_id))
        if m is not None:
            return m.tick_size
        return Decimal("0.001") if leg.venue is Venue.POLYMARKET else Decimal("0.01")

    def _clip_size(self) -> int:
        """Jittered order size — never exceeds clip (so the hedge-depth guard,
        which checks for `clip`, stays sufficient)."""
        if self.size_jitter <= 0:
            return self.clip
        return max(1, self.clip - random.randint(0, self.size_jitter))

    def _jitter_price(self, target: Decimal, side: str, tick: Decimal) -> Decimal:
        """Rest 0..price_jitter_ticks MORE PASSIVE than the aggressive cap
        (`target`): lower for a bid, higher for an ask. Always further from the
        touch, so it stays profitable and never beats the book by more than the
        min step (the cap already sits one tick inside the competition)."""
        if self.price_jitter_ticks <= 0:
            return target
        off = tick * random.randint(0, self.price_jitter_ticks)
        p = target - off if side == "bid" else target + off
        return p.quantize(tick, rounding=(ROUND_DOWN if side == "bid" else ROUND_UP))

    def _hold_in_deadband(self, cur: RestingQuote, target: Decimal, side: str,
                          key: tuple[int, str]) -> bool:
        """Keep an existing quote (skip reprice) when it's still on the
        profitable side of the aggressive cap AND the improvement available is
        within its randomized deadband. deadband_ticks<=0 => exact-match, the
        original behavior."""
        if self.deadband_ticks <= 0:
            return cur.price == target
        db = self._deadband.get(key, Decimal(0))
        if side == "bid":
            return target - db <= cur.price <= target
        return target <= cur.price <= target + db

    def _touch_excl_self(self, book, i: int, side: str) -> Optional[Decimal]:
        """Best price on `side` from OTHER participants — our own resting order
        subtracted out. Prevents quoting one tick inside ourselves (which walked
        the quote toward the mid until it got picked off)."""
        rq = self._resting.get((i, side))
        levels = book.bids if side == "bid" else book.asks  # best-first
        for lvl in levels:
            size = lvl.size
            if rq is not None and lvl.price == rq.price:
                size -= Decimal(rq.count)
            if size > 0:
                return lvl.price
        return None

    def _apr_margin(self) -> Decimal:
        """Extra per-contract lock required so a fill annualizes >= min_apr
        over the hold: lock/(1-lock)/yrs >= apr  =>  lock >= a/(1+a) with
        a = apr*yrs. Tightens the maker price cap by this amount."""
        if self.min_apr <= 0 or not self.resolve_years:
            return Decimal(0)
        a = Decimal(str(self.min_apr)) / 100 * Decimal(str(self.resolve_years))
        return (a / (1 + a)).quantize(Decimal("0.0001"))

    def _target(self, books: BookBuilder, i: int, side: str) -> Optional[Decimal]:
        leg = self.rel.legs[i]
        if (leg.market_id, side) in self.suppress:
            return None  # side owned by maker-unwind — cancel/stay out
        book = books.get(leg.venue.value, leg.market_id)
        if book is None:
            return None
        tick = self._tick(leg)
        buf = tick * self.safety_ticks
        cur = self._resting.get((i, side))
        comp = self._touch_excl_self(book, i, side)  # best COMPETING price (not us)
        if side == "bid":
            p_max = maker_quote(self.rel, i, books, self.markets, self.fees,
                                Decimal(self.clip))
            if p_max is None:
                return None
            p_max -= self._apr_margin()  # fill must annualize >= min_apr
            # no competitor on our side: hold a still-profitable quote, don't
            # invent a more aggressive price to chase a fill
            if comp is None:
                return cur.price if (cur and cur.price <= p_max - buf) else None
            if p_max - buf < comp:  # can't get inside the competition profitably
                return None
            # exactly one tick better than THEM, then hold. Quantize DOWN to the
            # tick so we never send a sub-tick price the venue would round (which
            # breaks self-recognition and restarts the walk).
            raw = min(p_max - buf, comp + tick)
            # stay STRICTLY below the ask — a post_only maker that touches/crosses
            # the opposite touch is rejected (Kalshi 400). On a 1-tick book this
            # clamps us to join best bid rather than cross.
            ba = book.best_ask()
            if ba is not None:
                raw = min(raw, ba.price - tick)
            if raw < comp:  # too tight to post at/above best bid passively
                return None
            return raw.quantize(tick, rounding=ROUND_DOWN)
        else:
            p_min = maker_ask_quote(self.rel, i, books, self.markets, self.fees,
                                    Decimal(self.clip))
            if p_min is None:
                return None
            p_min += self._apr_margin()  # fill must annualize >= min_apr
            if comp is None:
                return cur.price if (cur and cur.price >= p_min + buf) else None
            if p_min + buf > comp:
                return None
            # one tick better (lower) than THEM; quantize UP to the tick so we
            # stay at/above the min-profitable price and tick-aligned
            raw = max(p_min + buf, comp - tick)
            # stay STRICTLY above the bid (passive maker; post_only never crosses)
            bb = book.best_bid()
            if bb is not None:
                raw = max(raw, bb.price + tick)
            if raw > comp:  # too tight to post at/below best ask passively
                return None
            return raw.quantize(tick, rounding=ROUND_UP)

    def on_book(self, books: BookBuilder) -> None:
        """Re-evaluate every quotable (leg, side); place/replace/cancel."""
        if self.risk.kill_switch_active():
            self.cancel_all()
            return
        for i in maker_leg_indices(self.rel):
            leg = self.rel.legs[i]
            if leg.venue not in self.gateways:
                continue
            if self.quote_venue is not None and leg.venue is not self.quote_venue:
                continue
            for side in ("bid", "ask"):
                target = self._target(books, i, side)
                key = (i, side)
                cur = self._resting.get(key)
                gw = self.gateways[leg.venue]
                # hedge-depth guard: only quote a side we could actually hedge if
                # filled. A maker bid (hold YES on fill) hedges by SELLING YES on
                # the other leg (needs its BID depth); a maker ask hedges by
                # BUYING YES (needs its ASK depth). No depth for the clip => don't
                # rest a quote that could leave an unhedged leg.
                if target is not None and not self._hedge_has_depth(books, i, side):
                    target = None
                # toxgate (card 059ce700): the research toxicity model's live
                # shadow feed scores expected adverse cost per (market, side).
                # A toxic side is unviable — cancel any resting quote, rest
                # nothing. Advisory + fail-open: stale/missing feed never blocks.
                if target is not None:
                    tox = _toxgate(leg.market_id, side)
                    if tox is not None and tox > TOXGATE_MAX:
                        self.intents.append({"ts": time.time(),
                                             "skip": [f"toxgate {side} {tox:.3f} > {TOXGATE_MAX}"]})
                        target = None
                if target is None:
                    if cur:
                        gw.cancel(cur.order_id, market_slug=leg.market_id)
                        cur.machine.apply(TradeEvent.LEG1_CANCELLED, time.time())
                        self.intents.append({"ts": time.time(), "cancel": leg.market_id,
                                             "venue": leg.venue.value, "side": side,
                                             "price": str(cur.price), "order_id": cur.order_id})
                        del self._resting[key]
                        self._last_quote_ts.pop(key, None)
                    continue
                # hysteresis: hold a still-profitable resting quote until the
                # improvement available exceeds its RANDOMIZED deadband — cuts
                # churn AND makes our reprice cadence unpredictable. (deadband
                # off => exact-match, the original behavior.)
                if cur is not None and self._hold_in_deadband(cur, target, side, key):
                    continue
                # reprice throttle: an existing quote is still PROFITABLE (target
                # not None), so don't churn cancel/replace on every tick — keep
                # it until min_requote_s elapses. (target None already cancels
                # promptly above, so an unviable quote is never held.)
                if cur and (time.monotonic() - self._last_quote_ts.get(key, 0.0)) < self.min_requote_s:
                    continue
                # back off a side whose last placement FAILED — without this a
                # rejected order (no resting quote to throttle on) retries every
                # tick and spams the venue.
                if key in self._place_failed and \
                        (time.monotonic() - self._last_quote_ts.get(key, 0.0)) < self.min_requote_s:
                    continue
                q = self._clip_size()          # jittered size (never exceeds clip)
                notional = Decimal(q)          # ~$ at risk on a $1 contract
                decision = self.risk.check_order(
                    self.rel, notional, {self.rel.legs[i].venue: notional})
                if not decision.allowed:
                    self.intents.append({"ts": time.time(), "skip": decision.reasons})
                    continue
                # capture the order we're replacing so the activity feed can
                # render this as a MOVE (old price -> new price) rather than an
                # unrelated cancel+place pair.
                replaced_oid = old_price = None
                if cur:
                    gw.cancel(cur.order_id, market_slug=leg.market_id)
                    replaced_oid, old_price = cur.order_id, str(cur.price)
                    del self._resting[key]
                price = self._jitter_price(target, side, self._tick(leg))  # rest more passive by 0..N ticks
                try:
                    res = gw.place_yes(leg.market_id, side, price, q)
                except Exception as e:  # venue rejected (400/429/etc) — back off, don't spam
                    self._place_failed.add(key)
                    self._last_quote_ts[key] = time.monotonic()
                    if replaced_oid:  # pulled the old order but the replace failed
                        self.intents.append({"ts": time.time(), "cancel": leg.market_id,
                                             "venue": leg.venue.value, "side": side,
                                             "price": old_price, "order_id": replaced_oid})
                    self.intents.append({"ts": time.time(), "place_failed": leg.market_id,
                                         "venue": leg.venue.value, "side": side,
                                         "reason": f"{type(e).__name__}: {e}"[:200]})
                    continue
                # order-id field varies by venue: Kalshi order_id (or nested
                # order.order_id), Polymarket US 'id'. Missing => don't track a
                # phantom None-id quote (it can't be cancelled/hedged).
                oid = ((res.get("order") or {}).get("order_id")
                       or res.get("order_id") or res.get("id"))
                if oid is None:
                    self._place_failed.add(key)
                    self._last_quote_ts[key] = time.monotonic()
                    if replaced_oid:
                        self.intents.append({"ts": time.time(), "cancel": leg.market_id,
                                             "venue": leg.venue.value, "side": side,
                                             "price": old_price, "order_id": replaced_oid})
                    self.intents.append({"ts": time.time(), "place_failed": leg.market_id,
                                         "venue": leg.venue.value, "side": side,
                                         "reason": "no order id in response"})
                    continue
                self._place_failed.discard(key)
                m = TradeStateMachine(trade_id=f"{self.rel.id}:{i}:{side}:{oid}",
                                      relationship_id=self.rel.id)
                m.apply(TradeEvent.PLACE_LEG1, time.time())
                self._resting[key] = RestingQuote(oid, side, price, q, m)
                self._last_quote_ts[key] = time.monotonic()
                if self.deadband_ticks > 0:  # fresh randomized deadband for the next hold window
                    self._deadband[key] = self._tick(leg) * random.randint(1, self.deadband_ticks)
                evt = {"ts": time.time(), "place": self.rel.legs[i].market_id,
                       "venue": leg.venue.value, "side": side, "price": str(price),
                       "count": q, "order_id": oid}
                if replaced_oid:
                    evt["replaces"] = replaced_oid
                    evt["old_price"] = old_price
                self.intents.append(evt)

    def _hedge_has_depth(self, books: BookBuilder, i: int, side: str) -> bool:
        """Is there >= clip of hedge liquidity at the touch on the other leg?
        maker bid -> hedge sells YES (needs hedge best-bid size);
        maker ask -> hedge buys YES (needs hedge best-ask size)."""
        hedge_leg = self.rel.legs[1 - i]
        b = books.get(hedge_leg.venue.value, hedge_leg.market_id)
        if b is None:
            return False
        lvl = b.best_bid() if side == "bid" else b.best_ask()
        return lvl is not None and lvl.size >= Decimal(self.clip)

    def hedge_fill(self, i: int, side: str, books: BookBuilder, qty: int) -> dict:
        """Place the taker hedge for `qty` contracts on the OTHER leg. Pure
        placement — does NOT touch the resting quote or the state machine, so
        callers can drive partial fills (hedge each increment, keep resting)."""
        hedge_leg = self.rel.legs[1 - i]
        hedge_gw = self.gateways.get(hedge_leg.venue)
        if hedge_gw is None:
            return {"hedged": False, "reason": "no hedge gateway"}
        # hedge: bid-side filled => hold YES(maker), buy NO(hedge); ask-side =>
        # hold NO(maker), buy YES(hedge). Buying NO = a YES-ask on that leg.
        hedge_side = "ask" if side == "bid" else "bid"
        book = books.get(hedge_leg.venue.value, hedge_leg.market_id)
        if book is None or not (book.best_ask() and book.best_bid()):
            return {"hedged": False, "reason": "no hedge book"}
        # marketable taker: cross up to the ask to buy YES, down to the bid to
        # sell YES (== buy NO). price is always on the YES axis.
        px = book.best_ask().price if hedge_side == "bid" else book.best_bid().price
        res = hedge_gw.place_yes(hedge_leg.market_id, hedge_side, px, qty, post_only=False)
        self.risk.record_open(self.rel, Decimal(qty))
        # px rides along so the recorder has an honest fallback price if the
        # venue's fill report is delayed/unreachable (429) at record time
        return {"hedged": True, "px": str(px), "hedge_order": res}

    def on_fill(self, i: int, side: str, books: BookBuilder,
                filled_count: int | None = None) -> dict:
        """Called when our resting maker order fills: hedge the ACTUAL filled
        quantity (taker) on the other leg. We stop resting this quote (pop) and
        let on_book re-quote fresh next tick — avoids partial-fill drift."""
        key = (i, side)
        rq = self._resting.pop(key, None)
        self._last_quote_ts.pop(key, None)
        if rq is None:
            return {"error": "no such resting order"}
        qty = int(filled_count) if filled_count is not None else rq.count
        rq.machine.apply(TradeEvent.LEG1_FILL_FULL, time.time())
        rq.machine.apply(TradeEvent.PLACE_HEDGE, time.time())
        res = self.hedge_fill(i, side, books, qty)
        if res.get("hedged"):
            rq.machine.apply(TradeEvent.HEDGE_FILL_FULL, time.time())
        else:
            rq.machine.apply(TradeEvent.HEDGE_TIMEOUT, time.time())
        res["state"] = rq.machine.state.value
        return res

    def cancel_all(self) -> None:
        for key, rq in list(self._resting.items()):
            i = key[0]
            with contextlib.suppress(Exception):
                self.gateways[self.rel.legs[i].venue].cancel(
                    rq.order_id, market_slug=self.rel.legs[i].market_id)
            del self._resting[key]
            self._last_quote_ts.pop(key, None)
        # belt-and-suspenders: sweep any UNTRACKED resting orders on venues that
        # support a one-shot cancel-all (kill switch must leave nothing resting)
        for gw in self.gateways.values():
            if hasattr(gw, "cancel_all_open"):
                with contextlib.suppress(Exception):
                    gw.cancel_all_open()

    def overdue_alarms(self, now_s: float) -> list[str]:
        return [f"{rq.machine.trade_id} stuck {rq.machine.state.value}"
                for rq in self._resting.values()
                if rq.machine.exposed and rq.machine.overdue(now_s)]
