"""Maker viability: ask-side quote math and spell tracking."""

from decimal import Decimal

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookSnapshot, Level, Market, Venue
from arbbot.registry.model import Leg, Relationship, RelationshipType, Verdict, VettedBy
from arbbot.scan.scanner import MakerViabilityTracker, maker_ask_quote, maker_quote

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


def snap(venue, mid, bids, asks, seq=1, ts=1):
    return BookSnapshot(
        venue=venue, market_id=mid,
        bids=[Level(price=D(p), size=D(s)) for p, s in bids],
        asks=[Level(price=D(p), size=D(s)) for p, s in asks],
        seq=seq, ts_local_ns=ts,
    )


def test_maker_ask_quote_rounds_up_and_bounds():
    bb = BookBuilder()
    # hedge leg (PM) asks YES at 0.40: sell YES on Kalshi above 0.40+fees
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", [], [("0.40", "200")]))
    q = maker_ask_quote(rel(), 0, bb, markets(), FS, D("100"))
    assert q is not None
    assert q == q.quantize(D("0.01"))  # on Kalshi tick
    assert q > D("0.40")  # must clear hedge cost + fees
    # hedge too expensive -> no viable ask below 1
    bb2 = BookBuilder()
    bb2.apply_snapshot(snap(Venue.POLYMARKET, "P", [], [("0.995", "200")]))
    assert maker_ask_quote(rel(), 0, bb2, markets(), FS, D("100")) is None


def test_viability_spell_opens_and_closes():
    bb = BookBuilder()
    mkts = markets()
    tr = MakerViabilityTracker()
    r = rel()
    # PM bids YES at 0.60 (=> hedge NO cheap at 0.40); Kalshi top bid is 0.40:
    # our max profitable bid (~0.55) is ABOVE the top bid -> bid-side viable
    bb.apply_snapshot(snap(Venue.KALSHI, "K", [("0.40", "100")], [("0.99", "1")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", [("0.60", "200")], [("0.99", "1")]))
    assert tr.observe(r, bb, mkts, FS, ts_ns=1_000_000_000) == []
    open_now = tr.open_spells(2_000_000_000)
    assert any(s["quote_side"] == "bid" and s["maker_leg_index"] == 0 for s in open_now)
    # hedge collapses: PM bid drops to 0.41 -> max bid ~0.36 < top 0.40 -> close
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", [("0.41", "200")], [("0.99", "1")], seq=2))
    closed = tr.observe(r, bb, mkts, FS, ts_ns=5_000_000_000)
    spells = [s for s in closed if s["quote_side"] == "bid" and s["maker_leg_index"] == 0]
    assert len(spells) == 1
    s = spells[0]
    assert s["duration_s"] == 4.0
    assert s["margin_at_start"] > 0
    assert s["relationship_id"] == "eq1"


def test_not_viable_when_quote_below_top():
    """Existing top bid already above our profitable price -> not viable.
    (Quoting AT the top ties the queue and counts as viable; only a top bid
    strictly above our max profitable price kills viability.)"""
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", [("0.61", "100")], [("0.99", "1")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", [("0.60", "200")], [("0.99", "1")]))
    tr = MakerViabilityTracker()
    tr.observe(rel(), bb, markets(), FS, ts_ns=1)
    assert not any(
        s["maker_leg_index"] == 0 and s["quote_side"] == "bid"
        for s in tr.open_spells(2)
    )


def test_exclusive_pairs_never_maker_viable():
    """REGRESSION (phantom-margin bug, 2026-07-20): YES+NO across an
    EXCLUSIVE pair is not riskless (min payoff 0) — maker quotes must
    refuse, killing the bogus $0.4+ NEH margins."""
    from arbbot.registry.model import RelationshipType as RT
    ex = Relationship(
        id="ex", type=RT.EXCLUSIVE,
        legs=[Leg(venue=Venue.POLYMARKET, market_id="A"),
              Leg(venue=Venue.POLYMARKET, market_id="B")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN,
    )
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.POLYMARKET, "A", [("0.60", "500")], [("0.70", "500")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "B", [("0.20", "500")], [("0.30", "500")]))
    mkts = {(Venue.POLYMARKET, m): Market(venue=Venue.POLYMARKET, market_id=m)
            for m in ("A", "B")}
    for i in (0, 1):
        assert maker_quote(ex, i, bb, mkts, FS, D("100")) is None
        assert maker_ask_quote(ex, i, bb, mkts, FS, D("100")) is None
    # implies [antecedent, consequent]: maker on CONSEQUENT is riskless
    imp = Relationship(
        id="imp", type=RT.IMPLIES,
        legs=[Leg(venue=Venue.POLYMARKET, market_id="A"),
              Leg(venue=Venue.POLYMARKET, market_id="B")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN,
    )
    assert maker_quote(imp, 1, bb, mkts, FS, D("100")) is not None  # YES(cons)+NO(ante) safe
    assert maker_quote(imp, 0, bb, mkts, FS, D("100")) is None       # YES(ante)+NO(cons) unsafe
