"""Report golden tests: fixture day + zero-opportunity day; scan loop integration."""

import json
from decimal import Decimal

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookSnapshot, Level, Market, Venue
from arbbot.registry.model import Leg, Relationship, RelationshipType, Tranche, Verdict, VettedBy
from arbbot.report.daily import annualized_return_on_locked_capital, build_report, render_text
from arbbot.scan.loop import ScanLoop

D = Decimal


def test_roc_math():
    # $4 edge on $92 locked for 73 days -> (4/92) * (365/73) = ~21.7%/yr
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    close = (now + timedelta(days=73)).isoformat()
    roc = annualized_return_on_locked_capital(
        D("4"), D("92"), int(now.timestamp() * 1e9), close
    )
    assert roc is not None
    assert abs(float(roc) - (4 / 92) * 5) < 0.01
    assert annualized_return_on_locked_capital(D("4"), D("92"), 0, None) is None
    assert annualized_return_on_locked_capital(D("4"), D("0"), 0, "x") is None


def test_zero_opportunity_day_renders_sanely(tmp_path):
    report = build_report(tmp_path, "2026-07-20")
    text = render_text(report)
    assert report["opportunity_observations"] == 0
    assert "quiet day is data" in text


def test_scan_loop_end_to_end_report(tmp_path):
    """record -> scan -> report on a fixture: the Stage 1 critical path."""
    rel = Relationship(
        id="eq1",
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[
            Leg(venue=Venue.KALSHI, market_id="K"),
            Leg(venue=Venue.POLYMARKET, market_id="P"),
        ],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
        tranche=Tranche.HEAD,
    )
    from arbbot.registry.model import Registry

    books = BookBuilder()
    markets = {
        (Venue.KALSHI, "K"): Market(
            venue=Venue.KALSHI, market_id="K", close_time="2027-01-01T00:00:00Z"
        ),
        (Venue.POLYMARKET, "P"): Market(
            venue=Venue.POLYMARKET, market_id="P", close_time="2027-01-01T00:00:00Z"
        ),
    }
    loop = ScanLoop(
        Registry(relationships=[rel]), markets, books, FeeSchedule(), tmp_path
    )

    def snap(venue, mid, bids, asks, seq):
        s = BookSnapshot(
            venue=venue,
            market_id=mid,
            bids=[Level(price=D(p), size=D(sz)) for p, sz in bids],
            asks=[Level(price=D(p), size=D(sz)) for p, sz in asks],
            seq=seq,
            ts_local_ns=seq,
        )
        books.apply_snapshot(s)
        return s

    day = "2026-07-20"
    # 1: crossing appears
    e1 = snap(Venue.KALSHI, "K", [("0.40", "100")], [("0.44", "100")], 1)
    assert loop.on_event(e1, day) == []  # K known, P missing -> nothing yet
    e2 = snap(Venue.POLYMARKET, "P", [("0.52", "100")], [("0.56", "100")], 2)
    opps = loop.on_event(e2, day)
    assert len(opps) == 1
    # 2: crossing disappears -> lifetime record
    e3 = snap(Venue.POLYMARKET, "P", [("0.40", "100")], [("0.56", "100")], 3)
    assert loop.on_event(e3, day) == []

    report = build_report(tmp_path, day)
    assert report["opportunity_observations"] == 1
    assert report["closed_opportunities"] == 1
    assert report["groups"][0]["relationship_id"] == "eq1"
    assert report["groups"][0]["tranche"] == "head"
    roc = report["groups"][0]["annualized_return_on_locked_capital"]["p50"]
    assert roc > 0  # positive annualized return on the fixture
    text = render_text(report)
    assert "eq1" in text and "head" in text
