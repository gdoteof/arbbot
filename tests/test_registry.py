"""Registry schema validation and feasible-state enumeration tests."""

from decimal import Decimal

import pytest

from arbbot.models.core import Role, Side, Venue
from arbbot.registry.model import (
    Leg,
    OracleRisk,
    Registry,
    Relationship,
    RelationshipType,
    Tranche,
    Verdict,
    VettedBy,
)


def leg(venue=Venue.KALSHI, mid="M1", side=Side.YES, role=Role.TAKER):
    return Leg(venue=venue, market_id=mid, side=side, role=role)


def rel(**kw):
    defaults = dict(
        id="r1",
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[leg(Venue.KALSHI, "K1"), leg(Venue.POLYMARKET, "P1")],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
        confidence=Decimal("0.9"),
    )
    defaults.update(kw)
    return Relationship(**defaults)


def test_cross_venue_requires_two_venues():
    with pytest.raises(ValueError):
        rel(legs=[leg(Venue.KALSHI, "A"), leg(Venue.KALSHI, "B")])


def test_bundle_requires_one_venue():
    with pytest.raises(ValueError):
        Relationship(
            id="b1",
            type=RelationshipType.BUNDLE,
            legs=[leg(Venue.KALSHI, "A", Side.YES), leg(Venue.POLYMARKET, "B", Side.NO)],
        )


def test_leg_count_enforced():
    with pytest.raises(ValueError):
        rel(type=RelationshipType.DATE_LADDER, legs=[leg()])
    with pytest.raises(ValueError):
        Relationship(id="p", type=RelationshipType.PARTITION, legs=[leg()])


def test_confidence_bounds():
    with pytest.raises(ValueError):
        rel(confidence=Decimal("1.5"))


def test_tradable_gate_requires_human_vetting():
    r = rel(vetted_by=VettedBy.AGENT)
    assert not r.tradable  # agent-proposed is scannable, never tradable
    assert rel(vetted_by=VettedBy.HUMAN).tradable
    assert not rel(verdict=Verdict.REJECTED).tradable


def test_feasible_states_equivalent():
    assert set(rel().feasible_states()) == {(0, 0), (1, 1)}


def test_feasible_states_bundle():
    r = Relationship(
        id="b",
        type=RelationshipType.BUNDLE,
        legs=[leg(Venue.KALSHI, "A", Side.YES), leg(Venue.KALSHI, "A", Side.NO)],
    )
    assert set(r.feasible_states()) == {(0, 1), (1, 0)}


def test_feasible_states_date_ladder_excludes_impossible():
    r = rel(
        type=RelationshipType.DATE_LADDER,
        legs=[leg(Venue.KALSHI, "MAR"), leg(Venue.KALSHI, "JUN")],
    )
    states = set(r.feasible_states())
    assert (1, 0) not in states  # "by March" true but "by June" false is impossible
    assert states == {(0, 0), (0, 1), (1, 1)}


def test_feasible_states_n_rung_ladder_monotone_steps():
    r = rel(
        type=RelationshipType.DATE_LADDER,
        legs=[leg(Venue.KALSHI, m) for m in ("AUG", "SEP", "OCT", "NOV")],
    )
    states = set(r.feasible_states())
    assert len(states) == 5  # n+1 step vectors
    for s in states:
        # once true, stays true at later deadlines
        assert all(s[i] <= s[i + 1] for i in range(len(s) - 1))
    assert (0, 1, 0, 1) not in states


def test_feasible_states_partition_exactly_one():
    r = Relationship(
        id="p",
        type=RelationshipType.PARTITION,
        legs=[leg(Venue.KALSHI, m) for m in ("A", "B", "C")],
    )
    states = r.feasible_states()
    assert all(sum(s) == 1 for s in states)
    assert len(states) == 3


def test_feasible_states_rollup_total_is_max():
    r = Relationship(
        id="ro",
        type=RelationshipType.ROLLUP,
        legs=[leg(Venue.KALSHI, m) for m in ("TOT", "P1", "P2")],
    )
    for total, *parts in r.feasible_states():
        assert total == max(parts)


def test_registry_roundtrip_and_lookup(tmp_path):
    reg = Registry(
        relationships=[
            rel(),
            rel(
                id="r2",
                type=RelationshipType.DATE_LADDER,
                legs=[leg(Venue.KALSHI, "MAR"), leg(Venue.KALSHI, "JUN")],
                tranche=Tranche.LONG_TAIL,
                oracle_risk=OracleRisk.HIGH,
            ),
        ]
    )
    p = tmp_path / "registry.yaml"
    reg.dump(str(p))
    loaded = Registry.load(str(p))
    assert loaded == reg
    assert {r.id for r in loaded.affected_by(Venue.KALSHI, "MAR")} == {"r2"}
    assert (Venue.POLYMARKET, "P1") in loaded.market_ids()


def test_registry_rejects_duplicate_ids():
    with pytest.raises(ValueError):
        Registry(relationships=[rel(), rel()])


def test_feasible_states_exclusive_includes_all_zero():
    r = Relationship(
        id="ex",
        type=RelationshipType.EXCLUSIVE,
        legs=[leg(Venue.POLYMARKET, m) for m in ("A", "B", "C")],
    )
    states = set(r.feasible_states())
    assert (0, 0, 0) in states  # non-exhaustive: nobody on the list may win
    assert all(sum(s) <= 1 for s in states)
    assert len(states) == 4


def test_feasible_states_implies_and_equivalent_pair():
    imp = Relationship(
        id="imp",
        type=RelationshipType.IMPLIES,
        legs=[leg(Venue.POLYMARKET, "PRES"), leg(Venue.POLYMARKET, "NOM")],
    )
    assert (1, 0) not in set(imp.feasible_states())
    eq = Relationship(
        id="eqp",
        type=RelationshipType.EQUIVALENT_PAIR,
        legs=[leg(Venue.KALSHI, "A"), leg(Venue.KALSHI, "B")],  # same venue OK
    )
    assert set(eq.feasible_states()) == {(0, 0), (1, 1)}


def test_exclusive_arb_is_sell_side_only():
    """Scanner semantics check: for EXCLUSIVE, buy-all-YES has min payoff 0
    (all-zero state), buy-all-NO has min payoff n-1 — only the sell side arbs."""
    r = Relationship(
        id="ex2",
        type=RelationshipType.EXCLUSIVE,
        legs=[leg(Venue.POLYMARKET, m) for m in ("A", "B")],
    )
    from arbbot.scan.scanner import _feasible_min_payoff
    from arbbot.models.core import Side

    assert _feasible_min_payoff(r, (Side.YES, Side.YES)) == 0
    assert _feasible_min_payoff(r, (Side.NO, Side.NO)) == 1
