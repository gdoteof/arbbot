"""Edge scanner: pure functions from (relationship, books, fees) -> opportunities.

Core idea (generalizes every relationship type):

    A BASKET assigns each leg a side (YES or NO). Its payoff in outcome
    state s is sum(side==YES ? s_i : 1-s_i). If the basket's MINIMUM payoff
    over the relationship's feasible states is m, and you can buy the basket
    for cost + fees < m, that is an arb regardless of outcome.

    equivalent [A,B]:  {YES A, NO B}   min payoff 1   (and the mirror)
    bundle:            {YES, NO}       min payoff 1
    date ladder [e,l]: {NO e, YES l}   min payoff 1   ("impossible" state excluded)
    partition:         {YES * n}       payoff exactly 1;  {NO * n} pays n-1
    rollup [T,p..]:    {NO T, YES p..} min payoff 1

    We enumerate all 2^n side assignments (n <= ~8), keep baskets whose
    min payoff >= 1, and price each against executable depth net of fees.

Buying NO from a YES-denominated book: a NO contract at price q is available
where the YES book bids at 1-q — so the NO ask ladder is the YES bid ladder
with price mapped p -> 1-p, size unchanged.

Executability rules (first-class, per eng review Issue 3):
- prices come off the book, so they are on-tick by construction;
- basket size is clipped to per-leg depth, floored to whole contracts if any
  leg is on Kalshi, and buckets to "sub_minimum" when below any leg's
  min_order_size — sub-minimum opportunities never pollute the primary count.
"""

from __future__ import annotations

import itertools
from dataclasses import asdict, dataclass, field
from decimal import ROUND_DOWN, Decimal
from typing import Optional

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule, leg_fee
from arbbot.models.core import Book, Level, Market, Role, Side, Venue
from arbbot.registry.model import Relationship

ZERO = Decimal("0")
ONE = Decimal("1")


def no_ask_ladder(book: Book) -> list[Level]:
    """NO-side ask ladder derived from YES bids (best/cheapest NO first)."""
    return [Level(price=ONE - l.price, size=l.size) for l in book.bids]


def walk_cost(ladder: list[Level], size: Decimal) -> Optional[Decimal]:
    """Total cost to take `size` contracts from an ask ladder (best first).
    None if the ladder is too shallow."""
    remaining = size
    cost = ZERO
    for level in ladder:
        take = min(remaining, level.size)
        cost += take * level.price
        remaining -= take
        if remaining == 0:
            return cost
    return None


def max_depth(ladder: list[Level]) -> Decimal:
    return sum((l.size for l in ladder), ZERO)


# Hot-path types are stdlib dataclasses, NOT pydantic models: the scanner
# constructs thousands of these per second, and pydantic-core (2.46.4) was
# observed SEGV-ing in BasketLeg construction under exactly that load
# (journal 2026-07-20 01:34, faulthandler stack at scanner.py:197). Plain
# dataclasses have no native-code surface and are faster; validation is
# unnecessary here because every field comes from already-validated inputs.
@dataclass(frozen=True)
class BasketLeg:
    venue: Venue
    market_id: str
    buy_side: Side  # which contract we BUY on this leg
    role: Role
    vwap: Decimal = ZERO
    fee: Decimal = ZERO


@dataclass(frozen=True)
class Opportunity:
    relationship_id: str
    tranche: str
    basket: list[BasketLeg]
    signature: str  # stable identity for lifetime tracking
    size: Decimal
    gross_cost: Decimal
    fees: Decimal
    min_payoff: Decimal
    net_edge_total: Decimal
    net_edge_per_contract: Decimal
    bucket: str  # "primary" | "sub_minimum"
    ts_local_ns: int
    max_hold_close_time: Optional[str] = None  # latest close among legs

    def to_json_dict(self) -> dict:
        d = asdict(self)
        for k in ("size", "gross_cost", "fees", "min_payoff",
                  "net_edge_total", "net_edge_per_contract"):
            d[k] = str(d[k])
        for leg in d["basket"]:
            leg["venue"] = leg["venue"].value
            leg["buy_side"] = leg["buy_side"].value
            leg["role"] = leg["role"].value
            leg["vwap"] = str(leg["vwap"])
            leg["fee"] = str(leg["fee"])
        return d


def _feasible_min_payoff(rel: Relationship, sides: tuple[Side, ...]) -> Decimal:
    payoffs = []
    for state in rel.feasible_states():
        p = sum(
            (state[i] if sides[i] is Side.YES else 1 - state[i]) for i in range(len(sides))
        )
        payoffs.append(p)
    return Decimal(min(payoffs))


def scan_relationship(
    rel: Relationship,
    books: BookBuilder,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    ts_local_ns: int,
    min_edge_per_contract: Decimal = ZERO,
    max_basket_size: Decimal = Decimal("10000"),
) -> list[Opportunity]:
    """All profitable baskets for one relationship at current books.

    Every leg is priced as a TAKER buy (maker-priceable quoting is a separate
    function). Emits both primary and sub_minimum buckets; net edge must
    exceed min_edge_per_contract (the failure-cost-adjusted breakeven, once
    Stage 2 derives it — 0 until then so Stage 1 records everything).
    """
    n = len(rel.legs)
    leg_books: list[Book] = []
    for leg in rel.legs:
        b = books.get(leg.venue.value, leg.market_id)
        if b is None or books.is_crossed(leg.venue.value, leg.market_id):
            return []  # unsynced or self-crossed book: data quality, not alpha
        leg_books.append(b)

    out: list[Opportunity] = []
    # Full 2^n basket enumeration is only tractable for small n. For wide
    # relationships (negRisk exclusives can have 35+ legs) the profitable
    # baskets are the canonical ones: all-YES (partition buy side) and
    # all-NO (sell-the-bundle side). Mixed baskets on wide sets are noise.
    if n <= 10:
        side_candidates = itertools.product((Side.YES, Side.NO), repeat=n)
    else:
        side_candidates = [tuple([Side.YES] * n), tuple([Side.NO] * n)]
    for sides in side_candidates:
        min_payoff = _feasible_min_payoff(rel, sides)
        if min_payoff < 1:
            continue
        ladders = [
            (list(b.asks) if s is Side.YES else no_ask_ladder(b))
            for b, s in zip(leg_books, sides)
        ]
        if any(not lad for lad in ladders):
            continue
        size = min(*(max_depth(lad) for lad in ladders), max_basket_size)
        if any(leg.venue is Venue.KALSHI for leg in rel.legs):
            size = size.quantize(ONE, rounding=ROUND_DOWN)  # whole contracts
        if size <= 0:
            continue

        # shrink until profitable at depth-walked prices (start full, halve)
        best: Optional[Opportunity] = None
        candidate = size
        for _ in range(12):
            opp = _price_basket(
                rel, sides, ladders, markets, fees, candidate, ts_local_ns
            )
            if opp and opp.net_edge_per_contract > min_edge_per_contract:
                best = opp
                break
            candidate = (candidate / 2).quantize(ONE, rounding=ROUND_DOWN) or ZERO
            if candidate <= 0:
                break
        if best:
            out.append(best)
    return out


def _price_basket(
    rel: Relationship,
    sides: tuple[Side, ...],
    ladders: list[list[Level]],
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    size: Decimal,
    ts_local_ns: int,
) -> Optional[Opportunity]:
    gross = ZERO
    total_fees = ZERO
    legs_out: list[BasketLeg] = []
    min_orders: list[Decimal] = []
    close_times: list[str] = []
    for leg, side, ladder in zip(rel.legs, sides, ladders):
        cost = walk_cost(ladder, size)
        if cost is None:
            return None
        vwap = cost / size
        market = markets.get((leg.venue, leg.market_id))
        fee = leg_fee(
            fees, leg.venue, Role.TAKER, vwap, size,
            category=market.category if market else "default",
            taker_coef_override=market.taker_fee_coef_override if market else None,
        )
        gross += cost
        total_fees += fee
        legs_out.append(
            BasketLeg(
                venue=leg.venue, market_id=leg.market_id, buy_side=side,
                role=Role.TAKER, vwap=vwap, fee=fee,
            )
        )
        if market:
            min_orders.append(market.min_order_size)
            if market.close_time:
                close_times.append(market.close_time)
    min_payoff = _feasible_min_payoff(rel, sides)
    net_total = min_payoff * size - gross - total_fees
    bucket = "primary" if all(size >= mo for mo in min_orders) else "sub_minimum"
    sig = f"{rel.id}:" + "".join("Y" if s is Side.YES else "N" for s in sides)
    return Opportunity(
        relationship_id=rel.id,
        tranche=rel.tranche.value,
        basket=legs_out,
        signature=sig,
        size=size,
        gross_cost=gross,
        fees=total_fees,
        min_payoff=min_payoff,
        net_edge_total=net_total,
        net_edge_per_contract=net_total / size,
        bucket=bucket,
        ts_local_ns=ts_local_ns,
        max_hold_close_time=max(close_times) if close_times else None,
    )


def maker_quote(
    rel: Relationship,
    maker_leg_index: int,
    books: BookBuilder,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    hedge_size: Decimal,
    min_edge_per_contract: Decimal = ZERO,
) -> Optional[Decimal]:
    """Max YES-bid price to REST on the maker leg such that, on fill, taking
    the hedge legs at current books still clears fees + threshold.

    Returns the quote price rounded DOWN to the maker market's tick, or None
    when no positive-edge quote exists (e.g. hedge depth too thin).
    Only supports 2-leg relationships (equivalent / ladder) for now — the
    Stage 4 maker engine generalizes this.
    """
    if len(rel.legs) != 2:
        return None
    # The maker+hedge basket must be RISKLESS for THIS relationship type:
    # bid-side maker holds YES(maker)+NO(hedge). For equivalents min payoff
    # is 1; for EXCLUSIVE pairs it is 0 (the other trigger can fire) — the
    # 2026-07-20 phantom-margin bug Geoff caught on the NEH pairs.
    _sides = tuple(Side.YES if k == maker_leg_index else Side.NO
                   for k in range(2))
    if _feasible_min_payoff(rel, _sides) < 1:
        return None
    hedge_index = 1 - maker_leg_index
    maker_leg, hedge_leg = rel.legs[maker_leg_index], rel.legs[hedge_index]
    hedge_book = books.get(hedge_leg.venue.value, hedge_leg.market_id)
    if hedge_book is None:
        return None
    # We rest a YES bid on the maker leg; on fill we hold YES(maker market),
    # hedge by buying NO on the hedge leg (min payoff 1 for equivalent).
    hedge_cost = walk_cost(no_ask_ladder(hedge_book), hedge_size)
    if hedge_cost is None:
        return None
    hedge_vwap = hedge_cost / hedge_size
    hedge_market = markets.get((hedge_leg.venue, hedge_leg.market_id))
    maker_market = markets.get((maker_leg.venue, maker_leg.market_id))
    if maker_market is None:
        return None
    hedge_fee = leg_fee(
        fees, hedge_leg.venue, Role.TAKER, hedge_vwap, hedge_size,
        category=hedge_market.category if hedge_market else "default",
        taker_coef_override=hedge_market.taker_fee_coef_override if hedge_market else None,
    )
    # p_max: 1 - hedge_vwap - per-contract fees - threshold, maker fee at p
    # (iterate once: compute with fee at rough p, re-round down to tick)
    rough = ONE - hedge_vwap - hedge_fee / hedge_size - min_edge_per_contract
    if rough <= 0:
        return None
    maker_fee = fees.fee(
        maker_leg.venue, Role.MAKER, rough, hedge_size, category=maker_market.category
    )
    p = rough - maker_fee / hedge_size
    if p <= 0:
        return None
    tick = maker_market.tick_size
    p = (p / tick).quantize(ONE, rounding=ROUND_DOWN) * tick
    return p if p > 0 else None


def maker_ask_quote(
    rel: Relationship,
    maker_leg_index: int,
    books: BookBuilder,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    hedge_size: Decimal,
    min_edge_per_contract: Decimal = ZERO,
) -> Optional[Decimal]:
    """Min YES-ask price to REST on the maker leg such that, on fill (we sold
    YES at p, i.e. hold NO), taking YES on the hedge leg still clears fees +
    threshold. Mirror of maker_quote. 2-leg relationships only; returns the
    quote rounded UP to the maker market's tick, or None."""
    from decimal import ROUND_CEILING

    if len(rel.legs) != 2:
        return None
    # ask-side maker holds NO(maker)+YES(hedge): validate risklessness too
    _sides = tuple(Side.NO if k == maker_leg_index else Side.YES
                   for k in range(2))
    if _feasible_min_payoff(rel, _sides) < 1:
        return None
    hedge_leg = rel.legs[1 - maker_leg_index]
    maker_leg = rel.legs[maker_leg_index]
    hedge_book = books.get(hedge_leg.venue.value, hedge_leg.market_id)
    if hedge_book is None:
        return None
    # we sold YES(maker) => hold NO(maker); hedge by buying YES on hedge leg
    hedge_cost = walk_cost(list(hedge_book.asks), hedge_size)
    if hedge_cost is None:
        return None
    hedge_vwap = hedge_cost / hedge_size
    hedge_market = markets.get((hedge_leg.venue, hedge_leg.market_id))
    maker_market = markets.get((maker_leg.venue, maker_leg.market_id))
    if maker_market is None:
        return None
    hedge_fee = leg_fee(
        fees, hedge_leg.venue, Role.TAKER, hedge_vwap, hedge_size,
        category=hedge_market.category if hedge_market else "default",
        taker_coef_override=hedge_market.taker_fee_coef_override if hedge_market else None,
    )
    # payoff 1 total: sold YES at p + bought YES at hedge_vwap -> profit
    # p - hedge_vwap - fees >= threshold  =>  p_min
    rough = hedge_vwap + hedge_fee / hedge_size + min_edge_per_contract
    maker_fee = fees.fee(
        maker_leg.venue, Role.MAKER, min(rough, ONE), hedge_size,
        category=maker_market.category,
    )
    p = rough + maker_fee / hedge_size
    if p >= 1:
        return None
    tick = maker_market.tick_size
    p = (p / tick).quantize(ONE, rounding=ROUND_CEILING) * tick
    return p if p < 1 else None


def probe_relationship(
    rel: Relationship,
    books: BookBuilder,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    probe_size: Decimal = Decimal("10"),
) -> Optional[float]:
    """Best per-contract net edge across canonical baskets at a small probe
    size, INCLUDING NEGATIVE values — the 'how close is this market to
    crossing' signal for relationships that never produce opportunities.
    None = books not ready (missing/crossed legs)."""
    n = len(rel.legs)
    leg_books = []
    for leg in rel.legs:
        b = books.get(leg.venue.value, leg.market_id)
        if b is None or books.is_crossed(leg.venue.value, leg.market_id):
            return None
        leg_books.append(b)
    if n <= 10:
        candidates = list(itertools.product((Side.YES, Side.NO), repeat=n))
    else:
        candidates = [tuple([Side.YES] * n), tuple([Side.NO] * n)]
    best: Optional[float] = None
    for sides in candidates:
        min_payoff = _feasible_min_payoff(rel, sides)
        if min_payoff < 1:
            continue
        total = ZERO
        fees_total = ZERO
        ok = True
        for leg, side, b in zip(rel.legs, sides, leg_books):
            ladder = list(b.asks) if side is Side.YES else no_ask_ladder(b)
            cost = walk_cost(ladder, probe_size)
            if cost is None:
                ok = False
                break
            vwap = cost / probe_size
            market = markets.get((leg.venue, leg.market_id))
            fees_total += leg_fee(
                fees, leg.venue, Role.TAKER, vwap, probe_size,
                category=market.category if market else "default",
                taker_coef_override=market.taker_fee_coef_override if market else None,
            )
            total += cost
        if not ok:
            continue
        edge = float((min_payoff * probe_size - total - fees_total) / probe_size)
        if best is None or edge > best:
            best = edge
    return best


class MakerViabilityTracker:
    """Tracks, per (relationship, maker leg, quote side), whether we could
    RIGHT NOW rest a quote at-or-inside the current top of book AND hedge
    profitably cross-venue — the leading indicator for make-take, measured
    without modeling fills at all. Emits completed viability SPELLS
    (start, end, duration) plus entry margins, so 'what fraction of the day
    were we viable at top' is a direct aggregation."""

    def __init__(self) -> None:
        # key -> (start_ns, margin_at_start, quote, top)
        self._open: dict[tuple[str, int, str], tuple[int, float, str, str]] = {}

    def observe(
        self,
        rel: Relationship,
        books: BookBuilder,
        markets: dict[tuple[Venue, str], Market],
        fees: FeeSchedule,
        ts_ns: int,
        hedge_size: Decimal = Decimal("100"),
        rewards: dict | None = None,
    ) -> list[dict]:
        """Returns spell records that CLOSED this tick. `rewards` maps
        PM token id -> {max_spread_c, daily_pool_usd}: when the viable quote
        also sits inside the rewards band around the market's own midpoint,
        the spell is a DOUBLE-DIP (paid to rest while waiting for the arb
        fill) — the closest thing to Geoff's slam-dunk this structure offers."""
        closed: list[dict] = []
        if len(rel.legs) != 2:
            return closed
        for i, leg in enumerate(rel.legs):
            book = books.get(leg.venue.value, leg.market_id)
            for side in ("bid", "ask"):
                key = (rel.id, i, side)
                viable = False
                margin = 0.0
                in_band = False
                pool = 0.0
                quote = top = None
                if book is not None:
                    if side == "bid":
                        q = maker_quote(rel, i, books, markets, fees, hedge_size)
                        t = book.best_bid()
                        # viable: our max profitable bid is AT/ABOVE current top
                        if q is not None and t is not None and q >= t.price:
                            viable, quote, top = True, q, t.price
                            margin = float(q - t.price)
                    else:
                        q = maker_ask_quote(rel, i, books, markets, fees, hedge_size)
                        t = book.best_ask()
                        # viable: our min profitable ask is AT/BELOW current top
                        if q is not None and t is not None and q <= t.price:
                            viable, quote, top = True, q, t.price
                            margin = float(t.price - q)
                if viable and rewards and leg.venue is Venue.POLYMARKET:
                    cfg = rewards.get(leg.market_id)
                    bb_, ba_ = book.best_bid(), book.best_ask()
                    if cfg and bb_ and ba_ and quote is not None:
                        mid = (bb_.price + ba_.price) / 2
                        in_band = abs(float(quote - mid)) * 100 <= float(
                            cfg.get("max_spread_c") or 0)
                        pool = float(cfg.get("daily_pool_usd") or 0)
                open_spell = self._open.get(key)
                if viable and open_spell is None:
                    self._open[key] = (ts_ns, margin, str(quote), str(top),
                                       in_band, pool)
                elif not viable and open_spell is not None:
                    start_ns, m0, q0, t0, band0, pool0 = self._open.pop(key)
                    closed.append({
                        "relationship_id": rel.id,
                        "maker_leg": rel.legs[i].venue.value + ":" + rel.legs[i].market_id,
                        "maker_leg_index": i,
                        "quote_side": side,
                        "start_ns": start_ns,
                        "end_ns": ts_ns,
                        "duration_s": round((ts_ns - start_ns) / 1e9, 3),
                        "margin_at_start": m0,
                        "quote_at_start": q0,
                        "top_at_start": t0,
                        "rewards_band": band0,
                        "pool_usd": pool0,
                    })
        return closed

    def open_spells(self, ts_ns: int) -> list[dict]:
        """Currently-open spells (for dashboards: viable right now + how long)."""
        return [
            {"relationship_id": rid, "maker_leg_index": i, "quote_side": side,
             "open_for_s": round((ts_ns - start) / 1e9, 1), "margin_at_start": m,
             "rewards_band": band}
            for (rid, i, side), (start, m, _q, _t, band, _p) in self._open.items()
        ]


class LifetimeTracker:
    """Tracks how long each opportunity signature persists across scans —
    the doc's 'lifetime' output, the number that says whether a non-colocated
    bot could ever have captured it."""

    def __init__(self) -> None:
        self._open: dict[str, tuple[int, Opportunity]] = {}

    def observe(
        self, present: list[Opportunity], ts_local_ns: int
    ) -> list[tuple[Opportunity, int]]:
        """Feed one scan's opportunities; returns opportunities that CLOSED
        this scan as (first_observation, lifetime_ns)."""
        seen = {o.signature for o in present}
        closed = []
        for sig in list(self._open):
            if sig not in seen:
                first_ts, first_opp = self._open.pop(sig)
                closed.append((first_opp, ts_local_ns - first_ts))
        for o in present:
            self._open.setdefault(o.signature, (o.ts_local_ns, o))
        return closed

    def open_count(self) -> int:
        return len(self._open)
