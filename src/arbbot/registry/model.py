"""Relationship registry — the vetted tradable universe.

This schema is load-bearing twice over: it is the hand-vetted Stage 1
universe AND the Stage 4 agent's output format. `verdict` always judges the
`direction` relation (for cross-venue equivalence, that relation is identity).

Relationship kinds and the state-space they constrain:

  equivalent      legs [a, b]        outcome(a) == outcome(b)  (cross-venue)
  equivalent-pair legs [a, b]        outcome(a) == outcome(b)  (any venues,
                                     e.g. Kalshi duplicate parlays)
  bundle          legs [yes, no]     same market: exactly one pays
  partition       legs [x1..xn]      EXACTLY one of n outcomes pays (exhaustive)
  exclusive       legs [x1..xn]      AT MOST one pays (non-exhaustive sets —
                                     only the sell-the-bundle side is an arb)
  date_ladder     legs [early, late] outcome(early) <= outcome(late)
                                     ("by March" implies "by June")
  implies         legs [ante, cons]  outcome(ante) <= outcome(cons)
                                     (presidency implies nomination; superset
                                     parlay implies subset parlay)
  rollup          legs [total, p1..pn]  outcome(total) == max(outcome(p_i))

The scanner turns each kind into a feasible-outcome-state set and prices
baskets against the minimum payoff over feasible states.
"""

from __future__ import annotations

import enum
from decimal import Decimal
from typing import Optional

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

from arbbot.models.core import Role, Side, Venue


class RelationshipType(str, enum.Enum):
    CROSS_VENUE_EQUIVALENT = "cross-venue-equivalent"
    EQUIVALENT_PAIR = "equivalent-pair"
    BUNDLE = "bundle"
    PARTITION = "partition"
    EXCLUSIVE = "exclusive"
    DATE_LADDER = "date-ladder"
    IMPLIES = "implies"
    ROLLUP = "rollup"


class Verdict(str, enum.Enum):
    EQUIVALENT = "equivalent"  # the claimed direction relation holds
    EQUIVALENT_WITH_CAVEAT = "equivalent-with-caveat"
    REJECTED = "rejected"


class OracleRisk(str, enum.Enum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class Tranche(str, enum.Enum):
    HEAD = "head"
    LONG_TAIL = "long-tail"


class VettedBy(str, enum.Enum):
    HUMAN = "human"
    AGENT = "agent"
    MECHANICAL = "mechanical"  # rule-generated (e.g. sports equiv discovery)


class Leg(BaseModel):
    model_config = ConfigDict(frozen=True)
    venue: Venue
    market_id: str
    side: Side = Side.YES
    role: Role = Role.TAKER


_MIN_LEGS = {
    RelationshipType.CROSS_VENUE_EQUIVALENT: 2,
    RelationshipType.EQUIVALENT_PAIR: 2,
    RelationshipType.BUNDLE: 2,
    RelationshipType.PARTITION: 2,
    RelationshipType.EXCLUSIVE: 2,
    RelationshipType.DATE_LADDER: 2,
    RelationshipType.IMPLIES: 2,
    RelationshipType.ROLLUP: 2,
}
_EXACT_LEGS = {
    RelationshipType.CROSS_VENUE_EQUIVALENT: 2,
    RelationshipType.EQUIVALENT_PAIR: 2,
    RelationshipType.BUNDLE: 2,
    RelationshipType.IMPLIES: 2,
    # DATE_LADDER is n-rung: legs ordered earliest deadline first
}


class Relationship(BaseModel):
    id: str
    type: RelationshipType
    legs: list[Leg]
    direction: str = ""  # human-readable statement of the invariant
    rule_citations: dict[str, str] = Field(default_factory=dict)
    verdict: Verdict = Verdict.REJECTED
    caveats: list[str] = Field(default_factory=list)
    vetted_by: VettedBy = VettedBy.AGENT
    confidence: Decimal = Decimal("0")
    vetted_at: Optional[str] = None
    oracle_risk: OracleRisk = OracleRisk.MEDIUM
    tranche: Tranche = Tranche.LONG_TAIL
    # Stored classification (P0 registry unification). None = derive from the
    # id via arbbot.registry.categorize; existing YAML loads unchanged.
    category: Optional[str] = None
    topic: Optional[str] = None
    event_date: Optional[str] = None  # ISO date the underlying event occurs

    @model_validator(mode="after")
    def _validate(self) -> "Relationship":
        n = len(self.legs)
        if n < _MIN_LEGS[self.type]:
            raise ValueError(f"{self.type.value} needs >= {_MIN_LEGS[self.type]} legs, got {n}")
        exact = _EXACT_LEGS.get(self.type)
        if exact is not None and n != exact:
            raise ValueError(f"{self.type.value} needs exactly {exact} legs, got {n}")
        if not (Decimal(0) <= self.confidence <= Decimal(1)):
            raise ValueError("confidence must be in [0,1]")
        if self.type is RelationshipType.CROSS_VENUE_EQUIVALENT:
            if self.legs[0].venue == self.legs[1].venue:
                raise ValueError("cross-venue-equivalent legs must be on different venues")
        if self.type is RelationshipType.BUNDLE:
            if self.legs[0].venue != self.legs[1].venue:
                raise ValueError("bundle legs must be on one venue")
        return self

    @property
    def tradable(self) -> bool:
        """Only vetted-relation entries enter the tradable universe.
        Agent-vetted entries are recordable/scannable but NOT tradable until a
        human verdict lands (the vetting gate, premise 2)."""
        return self.verdict in (Verdict.EQUIVALENT, Verdict.EQUIVALENT_WITH_CAVEAT) and (
            self.vetted_by is VettedBy.HUMAN
        )

    def feasible_states(self) -> list[tuple[int, ...]]:
        """Enumerate feasible outcome vectors (one 0/1 entry per leg's market).

        This is the ground truth the scanner prices against: an arb exists when
        a basket's cost + fees < its minimum payoff over these states.
        """
        n = len(self.legs)
        if self.type in (
            RelationshipType.CROSS_VENUE_EQUIVALENT,
            RelationshipType.EQUIVALENT_PAIR,
        ):
            return [(0, 0), (1, 1)]
        if self.type is RelationshipType.BUNDLE:
            # legs are YES and NO of the same market: outcomes are complementary
            return [(0, 1), (1, 0)]
        if self.type is RelationshipType.IMPLIES:
            # legs [antecedent, consequent]: antecedent=1 implies consequent=1
            return [(0, 0), (0, 1), (1, 1)]
        if self.type is RelationshipType.DATE_LADDER:
            # n rungs ordered earliest-deadline first; cumulative criterion
            # means once true, all later rungs true: states are the n+1
            # monotone nondecreasing step vectors (0..0), (0..01), ... (1..1)
            return [
                tuple(0 if i < n - k else 1 for i in range(n)) for k in range(n + 1)
            ]
        if self.type is RelationshipType.PARTITION:
            return [tuple(1 if i == j else 0 for i in range(n)) for j in range(n)]
        if self.type is RelationshipType.EXCLUSIVE:
            # at most one pays: one-hot vectors PLUS the all-zero vector
            states = [tuple(0 for _ in range(n))]
            states += [tuple(1 if i == j else 0 for i in range(n)) for j in range(n)]
            return states
        if self.type is RelationshipType.ROLLUP:
            # legs [total, p1..pn]: total == max(parts)
            states = []
            for mask in range(2 ** (n - 1)):
                parts = tuple((mask >> i) & 1 for i in range(n - 1))
                states.append((max(parts) if parts else 0, *parts))
            return states
        raise AssertionError(f"unhandled type {self.type}")


class Registry(BaseModel):
    relationships: list[Relationship] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "Registry":
        ids = [r.id for r in self.relationships]
        if len(ids) != len(set(ids)):
            raise ValueError("duplicate relationship ids")
        return self

    def market_ids(self) -> set[tuple[Venue, str]]:
        return {(leg.venue, leg.market_id) for r in self.relationships for leg in r.legs}

    def affected_by(self, venue: Venue, market_id: str) -> list[Relationship]:
        """Scanner API keyed by market-id -> affected relationships (the hook
        that lets Stage 4 add dirty-marking without restructuring)."""
        return [
            r
            for r in self.relationships
            if any(leg.venue == venue and leg.market_id == market_id for leg in r.legs)
        ]

    @classmethod
    def load(cls, path: str) -> "Registry":
        with open(path) as f:
            raw = yaml.safe_load(f) or {}
        return cls.model_validate(raw)

    def dump(self, path: str) -> None:
        with open(path, "w") as f:
            yaml.safe_dump(self.model_dump(mode="json"), f, sort_keys=False)
