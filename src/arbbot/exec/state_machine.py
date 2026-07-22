"""Leg-risk state machine — unhedged inventory is a designed-for state.

    FLAT ──place──> LEG1_WORKING ──full fill──────> LEG1_FILLED
      ^                │   │                            │
      │             cancel  └─partial fill──> LEG1_PARTIAL ──rest filled──┐
      │                │                          │  │                    │
      │                v                          │  └─cancel remainder──>│
      └──────────── FLAT                          v                       v
                                              (policy: requote/unwind) LEG1_FILLED
                                                                          │ hedge order
                                                                          v
     SETTLED <──resolution── RESOLVING <──hedge full fill── HEDGING
        ^                                                     │ reject/timeout/moved
        │                                                     v
     UNWOUND <──unwind fill── UNWINDING <──escalate── HEDGE_FAILED
                                              (work-the-hedge -> cross-spread -> unwind)

Every state that carries unhedged exposure has a max-age alarm; exceeding it
is an alert + forced escalation, never a silent wait. Invalid transitions
raise — an impossible event is a bug or venue weirdness that must surface.
"""

from __future__ import annotations

import enum
from decimal import Decimal
from typing import Callable, Optional

from pydantic import BaseModel, Field


class TradeState(str, enum.Enum):
    FLAT = "flat"
    LEG1_WORKING = "leg1_working"
    LEG1_PARTIAL = "leg1_partial"
    LEG1_FILLED = "leg1_filled"
    HEDGING = "hedging"
    HEDGE_FAILED = "hedge_failed"
    HEDGED = "hedged"
    UNWINDING = "unwinding"
    RESOLVING = "resolving"
    SETTLED = "settled"
    UNWOUND = "unwound"


class TradeEvent(str, enum.Enum):
    PLACE_LEG1 = "place_leg1"
    LEG1_FILL_FULL = "leg1_fill_full"
    LEG1_FILL_PARTIAL = "leg1_fill_partial"
    LEG1_REST_FILLED = "leg1_rest_filled"
    LEG1_CANCELLED = "leg1_cancelled"
    LEG1_REMAINDER_CANCELLED = "leg1_remainder_cancelled"
    PLACE_HEDGE = "place_hedge"
    HEDGE_FILL_FULL = "hedge_fill_full"
    HEDGE_REJECTED = "hedge_rejected"
    HEDGE_TIMEOUT = "hedge_timeout"
    RETRY_HEDGE = "retry_hedge"
    START_UNWIND = "start_unwind"
    UNWIND_FILLED = "unwind_filled"
    MARKET_RESOLVED = "market_resolved"
    POSITION_SETTLED = "position_settled"


# The exhaustive transition table. Anything not listed is invalid.
TRANSITIONS: dict[tuple[TradeState, TradeEvent], TradeState] = {
    (TradeState.FLAT, TradeEvent.PLACE_LEG1): TradeState.LEG1_WORKING,
    (TradeState.LEG1_WORKING, TradeEvent.LEG1_FILL_FULL): TradeState.LEG1_FILLED,
    (TradeState.LEG1_WORKING, TradeEvent.LEG1_FILL_PARTIAL): TradeState.LEG1_PARTIAL,
    (TradeState.LEG1_WORKING, TradeEvent.LEG1_CANCELLED): TradeState.FLAT,
    (TradeState.LEG1_PARTIAL, TradeEvent.LEG1_REST_FILLED): TradeState.LEG1_FILLED,
    (TradeState.LEG1_PARTIAL, TradeEvent.LEG1_REMAINDER_CANCELLED): TradeState.LEG1_FILLED,
    (TradeState.LEG1_FILLED, TradeEvent.PLACE_HEDGE): TradeState.HEDGING,
    (TradeState.LEG1_FILLED, TradeEvent.START_UNWIND): TradeState.UNWINDING,
    (TradeState.HEDGING, TradeEvent.HEDGE_FILL_FULL): TradeState.HEDGED,
    (TradeState.HEDGING, TradeEvent.HEDGE_REJECTED): TradeState.HEDGE_FAILED,
    (TradeState.HEDGING, TradeEvent.HEDGE_TIMEOUT): TradeState.HEDGE_FAILED,
    (TradeState.HEDGE_FAILED, TradeEvent.RETRY_HEDGE): TradeState.HEDGING,
    (TradeState.HEDGE_FAILED, TradeEvent.START_UNWIND): TradeState.UNWINDING,
    (TradeState.HEDGED, TradeEvent.MARKET_RESOLVED): TradeState.RESOLVING,
    (TradeState.UNWINDING, TradeEvent.UNWIND_FILLED): TradeState.UNWOUND,
    (TradeState.RESOLVING, TradeEvent.POSITION_SETTLED): TradeState.SETTLED,
}

TERMINAL_STATES = {TradeState.SETTLED, TradeState.UNWOUND, TradeState.FLAT}

# States carrying real POSITION exposure — a filled leg awaiting/ missing its
# hedge — MUST have max-age alarms. A plain resting maker quote (LEG1_WORKING,
# unfilled) is NOT exposure: it holds no position and is cancellable any time,
# and it legitimately rests for a long while waiting for flow, so it must not
# alarm (that would spam alerts and mask a genuine unhedged-position alert).
EXPOSED_STATES = {
    TradeState.LEG1_PARTIAL,
    TradeState.LEG1_FILLED,
    TradeState.HEDGING,
    TradeState.HEDGE_FAILED,
    TradeState.UNWINDING,
}

DEFAULT_MAX_AGE_S: dict[TradeState, float] = {
    TradeState.LEG1_WORKING: 30.0,
    TradeState.LEG1_PARTIAL: 10.0,
    TradeState.LEG1_FILLED: 5.0,
    TradeState.HEDGING: 10.0,
    TradeState.HEDGE_FAILED: 5.0,
    TradeState.UNWINDING: 60.0,
}

# HEDGE_FAILED escalation ladder (policy order is fixed; sizes/prices per class)
ESCALATION = ("work_the_hedge", "cross_the_spread", "unwind_leg1")


class InvalidTransition(Exception):
    pass


class TradeStateMachine(BaseModel):
    trade_id: str
    relationship_id: str
    state: TradeState = TradeState.FLAT
    entered_at_s: float = 0.0
    filled_size: Decimal = Decimal("0")
    hedged_size: Decimal = Decimal("0")
    escalation_step: int = 0
    max_age_s: dict[TradeState, float] = Field(default_factory=lambda: dict(DEFAULT_MAX_AGE_S))
    history: list[str] = Field(default_factory=list)

    def apply(self, event: TradeEvent, now_s: float) -> TradeState:
        nxt = TRANSITIONS.get((self.state, event))
        if nxt is None:
            raise InvalidTransition(f"{self.state.value} + {event.value}")
        self.history.append(f"{self.state.value}->{nxt.value}:{event.value}")
        if event is TradeEvent.RETRY_HEDGE:
            self.escalation_step = min(self.escalation_step + 1, len(ESCALATION) - 1)
        self.state = nxt
        self.entered_at_s = now_s
        return nxt

    def overdue(self, now_s: float) -> bool:
        limit = self.max_age_s.get(self.state)
        return limit is not None and (now_s - self.entered_at_s) > limit

    def next_escalation(self) -> str:
        return ESCALATION[min(self.escalation_step, len(ESCALATION) - 1)]

    @property
    def exposed(self) -> bool:
        return self.state in EXPOSED_STATES


def alarm_scan(
    machines: list[TradeStateMachine],
    now_s: float,
    alert: Optional[Callable[[str], None]] = None,
) -> list[TradeStateMachine]:
    """Every overdue exposed trade -> alert + returned for forced escalation.
    Designed for tired humans at 3am: nothing exposed can quietly linger."""
    overdue = [m for m in machines if m.exposed and m.overdue(now_s)]
    if alert:
        for m in overdue:
            alert(
                f"arbbot: trade {m.trade_id} stuck in {m.state.value} "
                f"for >{m.max_age_s[m.state]}s (rel {m.relationship_id})"
            )
    return overdue
