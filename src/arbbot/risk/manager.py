"""Risk manager: tail-first sizing, caps, kill switch, balance bands,
fee-per-fill reconciliation.

Sizing is TAIL-FIRST (eng review D11): the per-relationship max notional is
set so a both-legs-lose settlement event costs at most `tail_fraction` of
bankroll. In a two-leg arb the worst case loses the full gross cost of both
legs, so max notional per relationship = tail_fraction * bankroll. This is
the primary defense against the catastrophic failure mode; the generic caps
below are secondary. No trade count can prove tail safety — only sizing can.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Callable, Optional

from pydantic import BaseModel, Field

from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import Role, Venue
from arbbot.registry.model import OracleRisk, Relationship

ZERO = Decimal("0")

# Higher settlement-process risk => smaller fraction of the tail budget.
ORACLE_RISK_SCALER = {
    OracleRisk.LOW: Decimal("1.0"),
    OracleRisk.MEDIUM: Decimal("0.5"),
    OracleRisk.HIGH: Decimal("0.25"),
}


class RiskConfig(BaseModel):
    bankroll: Decimal
    tail_fraction: Decimal = Decimal("0.02")  # max bankroll lost on one blowup
    # absolute per-relationship cap (contracts ~ $): when set, replaces the
    # tail_fraction formula (still oracle-risk scaled). Geoff-approved at $150
    # for the low-oracle-risk cross-venue book (2026-07-21).
    per_rel_cap: Optional[Decimal] = None
    per_class_cap: Decimal = Decimal("0.25")  # of bankroll, summed per rel type
    global_cap: Decimal = Decimal("0.50")  # of bankroll, all open notional
    balance_band: Decimal = Decimal("0.30")  # alert when venue share drifts past
    kill_switch_file: str = "data/KILL"


class OpenExposure(BaseModel):
    by_relationship: dict[str, Decimal] = Field(default_factory=dict)
    by_class: dict[str, Decimal] = Field(default_factory=dict)

    @property
    def total(self) -> Decimal:
        return sum(self.by_relationship.values(), ZERO)


class RiskDecision(BaseModel):
    allowed: bool
    max_notional: Decimal
    reasons: list[str] = Field(default_factory=list)


class RiskManager(BaseModel):
    config: RiskConfig
    exposure: OpenExposure = Field(default_factory=OpenExposure)
    balances: dict[Venue, Decimal] = Field(default_factory=dict)
    _kill: bool = False

    def kill_switch_active(self) -> bool:
        return self._kill or Path(self.config.kill_switch_file).exists()

    def trigger_kill(self) -> None:
        self._kill = True
        Path(self.config.kill_switch_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.config.kill_switch_file).touch()

    def relationship_cap(self, rel: Relationship) -> Decimal:
        base = (self.config.per_rel_cap if self.config.per_rel_cap is not None
                else self.config.bankroll * self.config.tail_fraction)
        return base * ORACLE_RISK_SCALER[rel.oracle_risk]

    def check_order(
        self,
        rel: Relationship,
        notional: Decimal,
        venue_costs: dict[Venue, Decimal],
    ) -> RiskDecision:
        """Gate every new basket. venue_costs: cash needed per venue."""
        reasons: list[str] = []
        if self.kill_switch_active():
            return RiskDecision(allowed=False, max_notional=ZERO, reasons=["kill switch active"])

        cap = self.relationship_cap(rel)
        open_rel = self.exposure.by_relationship.get(rel.id, ZERO)
        headroom = cap - open_rel
        if notional > headroom:
            reasons.append(
                f"per-relationship tail cap: notional {notional} > headroom {headroom} "
                f"(cap {cap}, oracle_risk={rel.oracle_risk.value})"
            )

        class_cap = self.config.bankroll * self.config.per_class_cap
        open_class = self.exposure.by_class.get(rel.type.value, ZERO)
        if open_class + notional > class_cap:
            reasons.append(f"class cap: {open_class}+{notional} > {class_cap}")

        global_cap = self.config.bankroll * self.config.global_cap
        if self.exposure.total + notional > global_cap:
            reasons.append(f"global cap: {self.exposure.total}+{notional} > {global_cap}")

        for venue, cost in venue_costs.items():
            avail = self.balances.get(venue, ZERO)
            if cost > avail:
                reasons.append(f"insufficient {venue.value} balance: {cost} > {avail}")

        return RiskDecision(
            allowed=not reasons, max_notional=max(headroom, ZERO), reasons=reasons
        )

    def record_open(self, rel: Relationship, notional: Decimal) -> None:
        self.exposure.by_relationship[rel.id] = (
            self.exposure.by_relationship.get(rel.id, ZERO) + notional
        )
        self.exposure.by_class[rel.type.value] = (
            self.exposure.by_class.get(rel.type.value, ZERO) + notional
        )

    def record_close(self, rel: Relationship, notional: Decimal) -> None:
        self.exposure.by_relationship[rel.id] = max(
            self.exposure.by_relationship.get(rel.id, ZERO) - notional, ZERO
        )
        self.exposure.by_class[rel.type.value] = max(
            self.exposure.by_class.get(rel.type.value, ZERO) - notional, ZERO
        )

    def balance_imbalance_alert(self) -> Optional[str]:
        """Alert when one venue holds a share of capital outside the band —
        rebalancing itself stays manual (design: NOT in scope)."""
        total = sum(self.balances.values(), ZERO)
        if total <= 0 or len(self.balances) < 2:
            return None
        for venue, bal in self.balances.items():
            share = bal / total
            if share < self.config.balance_band:
                return (
                    f"arbbot: venue balance imbalance — {venue.value} holds "
                    f"{share:.0%} of capital (band {self.config.balance_band:.0%}); "
                    f"manual rebalance suggested"
                )
        return None


class FeeReconciler:
    """Venue-charged fee vs fee-module prediction, per fill. A mismatch is the
    fee-schedule-drift detector (eng review OV#12) — alert, never ignore."""

    def __init__(self, fees: FeeSchedule, tolerance: Decimal = Decimal("0.01")):
        self.fees = fees
        self.tolerance = tolerance
        self.mismatches: list[dict] = []

    def check_fill(
        self,
        venue: Venue,
        role: Role,
        price: Decimal,
        size: Decimal,
        venue_charged: Decimal,
        category: str = "default",
        alert: Optional[Callable[[str], None]] = None,
    ) -> bool:
        predicted = self.fees.fee(venue, role, price, size, category=category)
        ok = abs(venue_charged - predicted) <= self.tolerance
        if not ok:
            record = {
                "venue": venue.value,
                "role": role.value,
                "price": str(price),
                "size": str(size),
                "predicted": str(predicted),
                "charged": str(venue_charged),
            }
            self.mismatches.append(record)
            if alert:
                alert(
                    f"arbbot: FEE MISMATCH on {venue.value} {role.value} fill — "
                    f"predicted {predicted}, charged {venue_charged} "
                    f"(schedule change? halt sizing until verified)"
                )
        return ok
