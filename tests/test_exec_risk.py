"""State machine exhaustive tests + risk manager + fee reconciliation."""

from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from arbbot.exec.state_machine import (
    DEFAULT_MAX_AGE_S,
    ESCALATION,
    EXPOSED_STATES,
    TERMINAL_STATES,
    TRANSITIONS,
    InvalidTransition,
    TradeEvent,
    TradeState,
    TradeStateMachine,
    alarm_scan,
)
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import Role, Venue
from arbbot.registry.model import (
    Leg,
    OracleRisk,
    Relationship,
    RelationshipType,
    Verdict,
    VettedBy,
)
from arbbot.risk.manager import FeeReconciler, OpenExposure, RiskConfig, RiskManager

D = Decimal


def machine():
    return TradeStateMachine(trade_id="t1", relationship_id="eq1")


# --- state machine ---------------------------------------------------------

def test_happy_path_taker_taker():
    m = machine()
    for ev, expected in [
        (TradeEvent.PLACE_LEG1, TradeState.LEG1_WORKING),
        (TradeEvent.LEG1_FILL_FULL, TradeState.LEG1_FILLED),
        (TradeEvent.PLACE_HEDGE, TradeState.HEDGING),
        (TradeEvent.HEDGE_FILL_FULL, TradeState.HEDGED),
        (TradeEvent.MARKET_RESOLVED, TradeState.RESOLVING),
        (TradeEvent.POSITION_SETTLED, TradeState.SETTLED),
    ]:
        assert m.apply(ev, 0.0) is expected
    assert m.state in TERMINAL_STATES


def test_partial_fill_and_remainder_cancel_path():
    m = machine()
    m.apply(TradeEvent.PLACE_LEG1, 0)
    m.apply(TradeEvent.LEG1_FILL_PARTIAL, 1)
    assert m.state is TradeState.LEG1_PARTIAL and m.exposed
    m.apply(TradeEvent.LEG1_REMAINDER_CANCELLED, 2)
    assert m.state is TradeState.LEG1_FILLED


def test_hedge_failed_escalation_ladder_ends_in_unwind():
    m = machine()
    m.apply(TradeEvent.PLACE_LEG1, 0)
    m.apply(TradeEvent.LEG1_FILL_FULL, 1)
    m.apply(TradeEvent.PLACE_HEDGE, 2)
    m.apply(TradeEvent.HEDGE_TIMEOUT, 3)
    assert m.state is TradeState.HEDGE_FAILED
    assert m.next_escalation() == ESCALATION[0]
    m.apply(TradeEvent.RETRY_HEDGE, 4)
    m.apply(TradeEvent.HEDGE_REJECTED, 5)
    assert m.next_escalation() == ESCALATION[1]
    m.apply(TradeEvent.RETRY_HEDGE, 6)
    m.apply(TradeEvent.HEDGE_REJECTED, 7)
    assert m.next_escalation() == ESCALATION[2] == "unwind_leg1"
    m.apply(TradeEvent.START_UNWIND, 8)
    m.apply(TradeEvent.UNWIND_FILLED, 9)
    assert m.state is TradeState.UNWOUND


def test_invalid_transitions_raise():
    m = machine()
    with pytest.raises(InvalidTransition):
        m.apply(TradeEvent.HEDGE_FILL_FULL, 0)  # can't hedge-fill from FLAT
    m.apply(TradeEvent.PLACE_LEG1, 0)
    with pytest.raises(InvalidTransition):
        m.apply(TradeEvent.POSITION_SETTLED, 1)


def test_every_exposed_state_has_max_age_alarm():
    """The review property: no unhedged state can linger silently."""
    for state in EXPOSED_STATES:
        assert state in DEFAULT_MAX_AGE_S, f"{state} lacks a max-age alarm"
        assert DEFAULT_MAX_AGE_S[state] > 0


def test_every_nonterminal_state_has_an_exit():
    reachable_sources = {s for (s, _e) in TRANSITIONS}
    for state in TradeState:
        if state in TERMINAL_STATES:
            continue
        assert state in reachable_sources, f"{state} is a dead end"


def test_alarm_scan_flags_overdue_exposed_only():
    stuck = machine()
    stuck.apply(TradeEvent.PLACE_LEG1, 0)
    stuck.apply(TradeEvent.LEG1_FILL_FULL, 0)  # LEG1_FILLED, max age 5s
    fresh = machine()
    fresh.apply(TradeEvent.PLACE_LEG1, 100)
    alerts = []
    overdue = alarm_scan([stuck, fresh], now_s=100.0, alert=alerts.append)
    assert overdue == [stuck]
    assert "stuck in leg1_filled" in alerts[0]


@given(st.lists(st.sampled_from(list(TradeEvent)), max_size=25))
def test_machine_never_reaches_undefined_state(events):
    """Property: any event sequence either transitions legally or raises —
    the machine can never silently land outside the defined state set."""
    m = machine()
    for ev in events:
        try:
            m.apply(ev, 0.0)
        except InvalidTransition:
            pass
        assert m.state in set(TradeState)


# --- risk manager ----------------------------------------------------------

def rel(oracle=OracleRisk.LOW):
    return Relationship(
        id="eq1",
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[
            Leg(venue=Venue.KALSHI, market_id="K"),
            Leg(venue=Venue.POLYMARKET, market_id="P"),
        ],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
        oracle_risk=oracle,
    )


def manager(tmp_path, bankroll="10000"):
    cfg = RiskConfig(bankroll=D(bankroll), kill_switch_file=str(tmp_path / "KILL"))
    m = RiskManager(config=cfg)
    m.balances = {Venue.KALSHI: D("5000"), Venue.POLYMARKET: D("5000")}
    return m


def test_tail_first_sizing(tmp_path):
    m = manager(tmp_path)
    # 2% of 10k = $200 max notional at LOW oracle risk
    d = m.check_order(rel(), D("200"), {Venue.KALSHI: D("100")})
    assert d.allowed
    d = m.check_order(rel(), D("201"), {Venue.KALSHI: D("100")})
    assert not d.allowed and "tail cap" in d.reasons[0]
    # HIGH oracle risk quarters the budget: $50
    d = m.check_order(rel(OracleRisk.HIGH), D("51"), {})
    assert not d.allowed


def test_exposure_accumulates_and_releases(tmp_path):
    m = manager(tmp_path)
    m.record_open(rel(), D("150"))
    assert not m.check_order(rel(), D("100"), {}).allowed  # 150+100 > 200
    m.record_close(rel(), D("150"))
    assert m.check_order(rel(), D("100"), {}).allowed


def test_balance_refusal_and_imbalance_alert(tmp_path):
    m = manager(tmp_path)
    d = m.check_order(rel(), D("100"), {Venue.KALSHI: D("6000")})
    assert not d.allowed and "insufficient" in d.reasons[-1]
    m.balances = {Venue.KALSHI: D("9000"), Venue.POLYMARKET: D("1000")}
    alert = m.balance_imbalance_alert()
    assert alert and "polymarket" in alert
    m.balances = {Venue.KALSHI: D("5000"), Venue.POLYMARKET: D("5000")}
    assert m.balance_imbalance_alert() is None


def test_kill_switch_blocks_everything(tmp_path):
    m = manager(tmp_path)
    m.trigger_kill()
    d = m.check_order(rel(), D("1"), {})
    assert not d.allowed and d.reasons == ["kill switch active"]
    # file flag alone also kills (operator can touch data/KILL by hand)
    m2 = manager(tmp_path)
    assert m2.kill_switch_active()  # same tmp_path: KILL file exists


def test_fee_reconciler_detects_schedule_drift():
    fr = FeeReconciler(FeeSchedule())
    alerts = []
    ok = fr.check_fill(
        Venue.KALSHI, Role.TAKER, D("0.50"), D("100"), D("1.75"), alert=alerts.append
    )
    assert ok and not alerts
    ok = fr.check_fill(
        Venue.KALSHI, Role.TAKER, D("0.50"), D("100"), D("3.50"), alert=alerts.append
    )
    assert not ok
    assert "FEE MISMATCH" in alerts[0]
    assert len(fr.mismatches) == 1
