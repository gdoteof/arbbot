"""Replay harness: lag capture semantics, trade-through maker fills, determinism."""

from decimal import Decimal

from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookDelta, BookSnapshot, Level, Market, Trade, Venue
from arbbot.registry.model import (
    Leg,
    Registry,
    Relationship,
    RelationshipType,
    Verdict,
    VettedBy,
)
from arbbot.replay.harness import MS, ReplayEngine, lag_sweep, maker_fill_replay

D = Decimal
FS = FeeSchedule()


def rel():
    return Relationship(
        id="eq1",
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[
            Leg(venue=Venue.KALSHI, market_id="K"),
            Leg(venue=Venue.POLYMARKET, market_id="P"),
        ],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
    )


def markets():
    return {
        (Venue.KALSHI, "K"): Market(venue=Venue.KALSHI, market_id="K"),
        (Venue.POLYMARKET, "P"): Market(venue=Venue.POLYMARKET, market_id="P"),
    }


def snap(venue, mid, bids, asks, seq, ts_ms):
    return BookSnapshot(
        venue=venue,
        market_id=mid,
        bids=[Level(price=D(p), size=D(s)) for p, s in bids],
        asks=[Level(price=D(p), size=D(s)) for p, s in asks],
        seq=seq,
        ts_local_ns=ts_ms * MS,
    )


def crossing_events(persist_ms):
    """Crossing appears at t=0 and is taken away at t=persist_ms."""
    return [
        snap(Venue.KALSHI, "K", [("0.40", "100")], [("0.44", "100")], 1, 0),
        snap(Venue.POLYMARKET, "P", [("0.52", "100")], [("0.56", "100")], 1, 0),
        snap(Venue.POLYMARKET, "P", [("0.40", "100")], [("0.56", "100")], 2, persist_ms),
        # trailing event so lag windows can expire
        snap(Venue.KALSHI, "K", [("0.40", "100")], [("0.44", "100")], 2, persist_ms + 5000),
    ]


def test_slow_edge_captured_fast_edge_decays():
    reg = Registry(relationships=[rel()])
    # persists 2s: captured at every lag in the sweep
    stats = lag_sweep(reg, markets(), FS, crossing_events(2000))
    assert [s.lag_ms for s in stats] == [300, 500, 1000]
    for s in stats:
        assert s.detected >= 1 and s.captured >= 1
        assert s.realized_edge_total > 0
    # persists 400ms: captured at 300ms lag, gone at 500/1000ms
    stats = lag_sweep(reg, markets(), FS, crossing_events(400))
    by_lag = {s.lag_ms: s for s in stats}
    assert by_lag[300].captured >= 1
    assert by_lag[500].captured == 0
    assert by_lag[1000].captured == 0
    assert by_lag[500].decayed + by_lag[500].vanished >= 1


def test_replay_deterministic():
    reg = Registry(relationships=[rel()])
    a = lag_sweep(reg, markets(), FS, crossing_events(700))
    b = lag_sweep(reg, markets(), FS, crossing_events(700))
    assert [s.model_dump() for s in a] == [s.model_dump() for s in b]


def test_maker_fill_only_on_trade_through():
    """Resting YES bid at 0.45 on Kalshi: a trade AT 0.45 does not fill;
    a trade at 0.44 (through) fills, hedged on PM books 500ms later."""
    events = [
        snap(Venue.POLYMARKET, "P", [("0.60", "100")], [("0.64", "100")], 1, 0),
        Trade(venue=Venue.KALSHI, market_id="K", price=D("0.45"), size=D("50"),
              seq=1, ts_local_ns=100 * MS),
        Trade(venue=Venue.KALSHI, market_id="K", price=D("0.44"), size=D("50"),
              seq=2, ts_local_ns=200 * MS),
        snap(Venue.POLYMARKET, "P", [("0.60", "100")], [("0.64", "100")], 2, 1000),
    ]
    fills = maker_fill_replay(
        rel(), 0, D("0.45"), D("50"), events, markets(), FS, hedge_lag_ms=500
    )
    assert len(fills) == 1
    f = fills[0]
    assert f.fill_ts_ns == 200 * MS
    assert f.hedged
    # bought YES at 0.45, hedged NO at 0.40 (PM bid 0.60): payoff 1
    # edge = (1 - 0.45 - 0.40)*50 - fees
    assert f.hedge_edge_total > 0
    assert f.hedge_edge_total < D("0.15") * 50  # fees came out


def test_maker_unhedgeable_fill_reported_not_hidden():
    """Fill arrives but the hedge book is empty at fill+lag: the fill is
    reported with hedged=False — adverse selection made visible, not dropped."""
    events = [
        Trade(venue=Venue.KALSHI, market_id="K", price=D("0.40"), size=D("50"),
              seq=1, ts_local_ns=0),
        Trade(venue=Venue.KALSHI, market_id="K", price=D("0.99"), size=D("1"),
              seq=2, ts_local_ns=2000 * MS),
    ]
    fills = maker_fill_replay(
        rel(), 0, D("0.45"), D("50"), events, markets(), FS, hedge_lag_ms=500
    )
    assert len(fills) == 1
    assert not fills[0].hedged
    assert fills[0].hedge_edge_total == 0
