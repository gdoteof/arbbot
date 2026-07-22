"""Book reconstruction: unit + property tests (any valid event sequence
reconstructs exactly; gaps and duplicates behave per the state machine)."""

from decimal import Decimal

import pytest
from hypothesis import given, strategies as st

from arbbot.book.builder import BookBuilder, GapDetected, NotSynced
from arbbot.models.core import BookDelta, BookSnapshot, Level, Venue


def snap(seq=1, bids=(("0.40", "100"),), asks=(("0.42", "80"),)):
    return BookSnapshot(
        venue=Venue.KALSHI,
        market_id="M",
        bids=[Level(price=Decimal(p), size=Decimal(s)) for p, s in bids],
        asks=[Level(price=Decimal(p), size=Decimal(s)) for p, s in asks],
        seq=seq,
        ts_local_ns=seq,
    )


def delta(seq, side="bid", price="0.40", size="50"):
    return BookDelta(
        venue=Venue.KALSHI,
        market_id="M",
        side=side,
        price=Decimal(price),
        size=Decimal(size),
        seq=seq,
        ts_local_ns=seq,
    )


def test_delta_before_snapshot_raises():
    with pytest.raises(NotSynced):
        BookBuilder().apply_delta(delta(1))


def test_snapshot_sorts_levels():
    b = BookBuilder().apply_snapshot(
        snap(bids=(("0.30", "1"), ("0.40", "1")), asks=(("0.60", "1"), ("0.50", "1")))
    )
    assert [l.price for l in b.bids] == [Decimal("0.40"), Decimal("0.30")]
    assert [l.price for l in b.asks] == [Decimal("0.50"), Decimal("0.60")]


def test_delta_updates_replaces_and_removes():
    bb = BookBuilder()
    bb.apply_snapshot(snap())
    b = bb.apply_delta(delta(2, "bid", "0.40", "60"))  # replace size
    assert b.bids[0].size == Decimal("60")
    b = bb.apply_delta(delta(3, "bid", "0.41", "10"))  # new best level
    assert b.bids[0].price == Decimal("0.41")
    b = bb.apply_delta(delta(4, "bid", "0.41", "0"))  # size 0 removes
    assert b.bids[0].price == Decimal("0.40")


def test_duplicate_and_stale_dropped_silently():
    bb = BookBuilder()
    bb.apply_snapshot(snap(seq=5))
    assert bb.apply_delta(delta(5)) is None
    assert bb.apply_delta(delta(3)) is None
    assert bb.get("kalshi", "M").seq == 5


def test_gap_raises_and_kills_book():
    bb = BookBuilder()
    bb.apply_snapshot(snap(seq=1))
    with pytest.raises(GapDetected):
        bb.apply_delta(delta(4))
    assert bb.get("kalshi", "M") is None  # dead book unreadable until resync
    with pytest.raises(NotSynced):
        bb.apply_delta(delta(5))


def test_crossed_book_detection():
    bb = BookBuilder()
    bb.apply_snapshot(snap(bids=(("0.45", "1"),), asks=(("0.42", "1"),)))
    assert bb.is_crossed("kalshi", "M")


# --- property: replaying deltas reconstructs the same book -----------------

level_st = st.tuples(
    st.integers(min_value=1, max_value=99), st.integers(min_value=0, max_value=500)
)


@given(ops=st.lists(st.tuples(st.sampled_from(["bid", "ask"]), level_st), max_size=40))
def test_book_state_equals_last_write_wins_map(ops):
    """The book after applying deltas equals a naive dict of last size per
    (side, price) with zeros removed — the model-based oracle."""
    bb = BookBuilder()
    bb.apply_snapshot(snap(seq=0, bids=(), asks=()))
    oracle: dict[tuple[str, int], int] = {}
    for i, (side, (cents, size)) in enumerate(ops, start=1):
        bb.apply_delta(delta(i, side, f"0.{cents:02d}", str(size)))
        oracle[(side, cents)] = size
    book = bb.get("kalshi", "M")
    for side_name, levels in (("bid", book.bids), ("ask", book.asks)):
        expect = {
            Decimal(f"0.{c:02d}"): Decimal(s)
            for (sd, c), s in oracle.items()
            if sd == side_name and s > 0
        }
        assert {l.price: l.size for l in levels} == expect
        prices = [l.price for l in levels]
        assert prices == sorted(prices, reverse=(side_name == "bid"))
