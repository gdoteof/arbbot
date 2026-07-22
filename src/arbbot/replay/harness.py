"""Replay harness — the edge-validation gate (premise 1).

Feeds recorded events through the same BookBuilder/scanner the live system
uses, then asks: had we tried to CAPTURE each detected opportunity with a
pessimistic execution lag, what would we actually have gotten?

    events (ts order) ──> BookBuilder ──> scanner detects opp at t0
                                             │
                              try to execute at t0 + lag (300/500/1000ms)
                                             │
                       reprice the SAME basket on the books as of t0+lag
                                             │
              captured (edge remains) / decayed (edge gone) / vanished (depth gone)

Maker-fill model (pessimistic, per eng review + outside voice):
- a resting quote at price p counts as FILLED only when the tape trades
  THROUGH p (strictly better-for-the-counterparty price), never merely at p;
- the hedge is priced on the hedge book as of fill time + lag;
- results are still an OPTIMISTIC BOUND (historical makers' cancel loops
  were faster than ours) — stated in every report.

Determinism: input events are totally ordered by (ts_local_ns, venue, seq);
no wall clock, no randomness. Same recording -> identical output.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Iterable, Optional

from pydantic import BaseModel

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.fees.curves import FeeSchedule, leg_fee
from arbbot.models.core import (
    BookDelta,
    BookEvent,
    BookSnapshot,
    Market,
    Side,
    Trade,
    Venue,
)
from arbbot.registry.model import Registry, Relationship
from arbbot.scan.scanner import (
    Opportunity,
    no_ask_ladder,
    scan_relationship,
    walk_cost,
)

MS = 1_000_000  # ns per ms
ZERO = Decimal("0")
ONE = Decimal("1")


class CaptureResult(BaseModel):
    lag_ms: int
    opportunity: Opportunity
    outcome: str  # "captured" | "decayed" | "vanished"
    realized_edge_total: Decimal  # at lagged prices (0 when not captured)


class MakerFill(BaseModel):
    relationship_id: str
    quote_price: Decimal
    fill_ts_ns: int
    hedge_edge_total: Decimal  # realized edge after hedging at fill+lag
    hedged: bool


class ReplayStats(BaseModel):
    lag_ms: int
    detected: int
    captured: int
    decayed: int
    vanished: int
    detected_edge_total: Decimal
    realized_edge_total: Decimal

    @property
    def capture_rate(self) -> float:
        return self.captured / self.detected if self.detected else 0.0


def _apply(books: BookBuilder, event: BookEvent) -> None:
    if isinstance(event, BookSnapshot):
        books.apply_snapshot(event)
    elif isinstance(event, BookDelta):
        try:
            books.apply_delta(event)
        except (GapDetected, NotSynced):
            pass  # replay sees the later snapshot that the recorder fetched


def _reprice_basket(
    opp: Opportunity,
    books: BookBuilder,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
) -> Optional[Decimal]:
    """Re-cost the exact detected basket at current (lagged) books.
    None => depth vanished; otherwise realized net edge total."""
    gross = ZERO
    total_fees = ZERO
    for leg in opp.basket:
        book = books.get(leg.venue.value, leg.market_id)
        if book is None:
            return None
        ladder = list(book.asks) if leg.buy_side is Side.YES else no_ask_ladder(book)
        cost = walk_cost(ladder, opp.size)
        if cost is None:
            return None
        market = markets.get((leg.venue, leg.market_id))
        gross += cost
        total_fees += leg_fee(
            fees, leg.venue, leg.role, cost / opp.size, opp.size,
            category=market.category if market else "default",
            taker_coef_override=market.taker_fee_coef_override if market else None,
        )
    return opp.min_payoff * opp.size - gross - total_fees


class ReplayEngine:
    """One pass over recorded events, evaluating capture at a fixed lag."""

    def __init__(
        self,
        registry: Registry,
        markets: dict[tuple[Venue, str], Market],
        fees: FeeSchedule,
        lag_ms: int,
        min_edge_per_contract: Decimal = ZERO,
    ):
        self.registry = registry
        self.markets = markets
        self.fees = fees
        self.lag_ns = lag_ms * MS
        self.lag_ms = lag_ms
        self.min_edge = min_edge_per_contract
        self.books = BookBuilder()
        self._pending: list[Opportunity] = []  # detected, awaiting lag expiry
        self._seen: set[tuple[str, int]] = set()
        self.results: list[CaptureResult] = []

    def run(self, events: Iterable[BookEvent]) -> ReplayStats:
        for event in events:
            self._settle_due(event.ts_local_ns)
            _apply(self.books, event)
            if isinstance(event, Trade):
                continue
            ts = event.ts_local_ns
            for rel in self.registry.affected_by(event.venue, event.market_id):
                for opp in scan_relationship(
                    rel, self.books, self.markets, self.fees, ts, self.min_edge
                ):
                    key = (opp.signature, ts)
                    if key not in self._seen:
                        self._seen.add(key)
                        # one pending attempt per signature at a time
                        if not any(
                            p.signature == opp.signature for p in self._pending
                        ):
                            self._pending.append(opp)
        self._settle_due(None)
        return self._stats()

    def _settle_due(self, now_ns: Optional[int]) -> None:
        still = []
        for opp in self._pending:
            if now_ns is not None and now_ns < opp.ts_local_ns + self.lag_ns:
                still.append(opp)
                continue
            realized = _reprice_basket(opp, self.books, self.markets, self.fees)
            if realized is None:
                outcome, realized = "vanished", ZERO
            elif realized > 0:
                outcome = "captured"
            else:
                outcome, realized = "decayed", ZERO
            self.results.append(
                CaptureResult(
                    lag_ms=self.lag_ms,
                    opportunity=opp,
                    outcome=outcome,
                    realized_edge_total=realized,
                )
            )
        self._pending = still

    def _stats(self) -> ReplayStats:
        det = len(self.results)
        cap = [r for r in self.results if r.outcome == "captured"]
        return ReplayStats(
            lag_ms=self.lag_ms,
            detected=det,
            captured=len(cap),
            decayed=sum(1 for r in self.results if r.outcome == "decayed"),
            vanished=sum(1 for r in self.results if r.outcome == "vanished"),
            detected_edge_total=sum(
                (r.opportunity.net_edge_total for r in self.results), ZERO
            ),
            realized_edge_total=sum((r.realized_edge_total for r in cap), ZERO),
        )


def lag_sweep(
    registry: Registry,
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    events: list[BookEvent],
    lags_ms: tuple[int, ...] = (300, 500, 1000),
    min_edge_per_contract: Decimal = ZERO,
) -> list[ReplayStats]:
    """The Stage 2 headline: capture rate per lag. Deterministic."""
    return [
        ReplayEngine(registry, markets, fees, lag, min_edge_per_contract).run(events)
        for lag in lags_ms
    ]


def maker_fill_replay(
    rel: Relationship,
    maker_leg_index: int,
    quote_price: Decimal,
    quote_size: Decimal,
    events: list[BookEvent],
    markets: dict[tuple[Venue, str], Market],
    fees: FeeSchedule,
    hedge_lag_ms: int = 500,
) -> list[MakerFill]:
    """Pessimistic maker-fill validation for a hypothetical resting YES bid.

    Fill rule: the tape must trade THROUGH the quote (trade price strictly
    below the bid) on the maker leg's market. On fill, hedge by buying NO on
    the other leg at books as of fill_ts + hedge_lag.
    """
    from arbbot.models.core import Role

    maker_leg = rel.legs[maker_leg_index]
    hedge_leg = rel.legs[1 - maker_leg_index]
    books = BookBuilder()
    fills: list[MakerFill] = []
    pending_fill_ts: Optional[int] = None
    for event in events:
        if (
            pending_fill_ts is not None
            and event.ts_local_ns >= pending_fill_ts + hedge_lag_ms * MS
        ):
            hedge_book = books.get(hedge_leg.venue.value, hedge_leg.market_id)
            hedged = False
            edge = ZERO
            if hedge_book is not None:
                cost = walk_cost(no_ask_ladder(hedge_book), quote_size)
                if cost is not None:
                    market = markets.get((hedge_leg.venue, hedge_leg.market_id))
                    fee = leg_fee(
                        fees, hedge_leg.venue, Role.TAKER, cost / quote_size, quote_size,
                        category=market.category if market else "default",
                        taker_coef_override=market.taker_fee_coef_override if market else None,
                    )
                    maker_market = markets.get((maker_leg.venue, maker_leg.market_id))
                    maker_fee = fees.fee(
                        maker_leg.venue, Role.MAKER, quote_price, quote_size,
                        category=maker_market.category if maker_market else "default",
                    )
                    edge = ONE * quote_size - quote_price * quote_size - cost - fee - maker_fee
                    hedged = True
            fills.append(
                MakerFill(
                    relationship_id=rel.id,
                    quote_price=quote_price,
                    fill_ts_ns=pending_fill_ts,
                    hedge_edge_total=edge,
                    hedged=hedged,
                )
            )
            pending_fill_ts = None
        _apply(books, event)
        if (
            pending_fill_ts is None
            and isinstance(event, Trade)
            and event.venue == maker_leg.venue
            and event.market_id == maker_leg.market_id
            and event.price < quote_price  # trade THROUGH, never merely at
        ):
            pending_fill_ts = event.ts_local_ns
    return fills
