"""Scanner tests: basket enumeration, depth walking, fee thresholds, tick/size
rules, lifetime tracking, maker quoting."""

from decimal import Decimal

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookSnapshot, Level, Market, Role, Side, Venue
from arbbot.registry.model import (
    Leg,
    Relationship,
    RelationshipType,
    Tranche,
    Verdict,
    VettedBy,
)
from arbbot.scan.scanner import (
    LifetimeTracker,
    maker_quote,
    no_ask_ladder,
    scan_relationship,
    walk_cost,
)

FS = FeeSchedule()
D = Decimal


def snap(venue, mid, bids, asks, seq=1):
    return BookSnapshot(
        venue=venue,
        market_id=mid,
        bids=[Level(price=D(p), size=D(s)) for p, s in bids],
        asks=[Level(price=D(p), size=D(s)) for p, s in asks],
        seq=seq,
        ts_local_ns=seq,
    )


def market(venue, mid, tick="0.01", min_size="1", close="2026-08-01T00:00:00Z"):
    return Market(
        venue=venue, market_id=mid, tick_size=D(tick), min_order_size=D(min_size),
        close_time=close,
    )


def equivalent(kid="K", pid="P"):
    return Relationship(
        id="eq1",
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[
            Leg(venue=Venue.KALSHI, market_id=kid),
            Leg(venue=Venue.POLYMARKET, market_id=pid),
        ],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
        tranche=Tranche.HEAD,
    )


def markets_for(rel):
    return {(l.venue, l.market_id): market(l.venue, l.market_id) for l in rel.legs}


def test_walk_cost_and_no_ladder():
    ladder = [Level(price=D("0.40"), size=D("10")), Level(price=D("0.45"), size=D("10"))]
    assert walk_cost(ladder, D("15")) == D("0.40") * 10 + D("0.45") * 5
    assert walk_cost(ladder, D("25")) is None
    book = snap(Venue.KALSHI, "K", bids=[("0.60", "7")], asks=[])
    bb = BookBuilder()
    bb.apply_snapshot(book)
    nl = no_ask_ladder(bb.get("kalshi", "K"))
    assert nl[0].price == D("0.40") and nl[0].size == D("7")


def test_equivalent_pair_detects_real_crossing():
    """Kalshi YES ask 0.44; Polymarket YES bid 0.52 (=> NO ask 0.48).
    Basket cost 0.92 + fees < 1.00 -> arb."""
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[("0.40", "100")], asks=[("0.44", "100")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "100")], asks=[("0.56", "100")]))
    rel = equivalent()
    opps = scan_relationship(rel, bb, markets_for(rel), FS, ts_local_ns=1)
    assert len(opps) == 1
    o = opps[0]
    assert o.signature == "eq1:YN"  # buy YES Kalshi, buy NO Polymarket
    assert o.size == D("100")
    assert o.gross_cost == D("92.00")
    # Kalshi taker fee at 0.44 x 100 + PM us taker on 0.48 x 100
    assert o.fees == FS.fee(Venue.KALSHI, Role.TAKER, D("0.44"), D(100)) + FS.fee(
        Venue.POLYMARKET, Role.TAKER, D("0.48"), D(100)
    )
    assert o.net_edge_total == D("100") - o.gross_cost - o.fees
    assert o.bucket == "primary"


def test_fees_kill_marginal_edge():
    """0.99 gross cost: 1c spread is eaten by the Kalshi taker fee."""
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[], asks=[("0.50", "100")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.51", "100")], asks=[]))
    rel = equivalent()
    opps = scan_relationship(rel, bb, markets_for(rel), FS, ts_local_ns=1)
    assert opps == []  # cost 0.99 + ~2.5c fees > 1.00


def test_missing_or_crossed_book_yields_nothing():
    rel = equivalent()
    bb = BookBuilder()
    assert scan_relationship(rel, bb, markets_for(rel), FS, 1) == []
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[("0.60", "1")], asks=[("0.55", "1")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "1")], asks=[("0.56", "1")]))
    assert scan_relationship(rel, bb, markets_for(rel), FS, 1) == []


def test_kalshi_leg_floors_size_to_whole_contracts():
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[], asks=[("0.30", "5.7")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.75", "9.3")], asks=[]))
    rel = equivalent()
    opps = scan_relationship(rel, bb, markets_for(rel), FS, 1)
    assert opps and opps[0].size == D("5")


def test_sub_minimum_bucket():
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[], asks=[("0.30", "3")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.75", "3")], asks=[]))
    rel = equivalent()
    mkts = markets_for(rel)
    mkts[(Venue.POLYMARKET, "P")] = market(Venue.POLYMARKET, "P", min_size="5")
    opps = scan_relationship(rel, bb, mkts, FS, 1)
    assert opps and opps[0].bucket == "sub_minimum"


def test_date_ladder_violation_priced_as_no_early_yes_late():
    """P(early) must be <= P(late). Books violating it: early bid 0.60,
    late ask 0.35 -> buy NO early (0.40) + YES late (0.35) = 0.75 < 1."""
    early = Relationship(
        id="ladder",
        type=RelationshipType.DATE_LADDER,
        legs=[
            Leg(venue=Venue.KALSHI, market_id="MAR"),
            Leg(venue=Venue.KALSHI, market_id="JUN"),
        ],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
    )
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "MAR", bids=[("0.60", "50")], asks=[("0.65", "50")]))
    bb.apply_snapshot(snap(Venue.KALSHI, "JUN", bids=[("0.30", "50")], asks=[("0.35", "50")]))
    mkts = {(l.venue, l.market_id): market(l.venue, l.market_id) for l in early.legs}
    opps = scan_relationship(early, bb, mkts, FS, 1)
    assert len(opps) == 1
    assert opps[0].signature == "ladder:NY"
    assert opps[0].gross_cost == (D("0.40") + D("0.35")) * 50


def test_partition_all_yes_sum_below_one():
    rel = Relationship(
        id="part",
        type=RelationshipType.PARTITION,
        legs=[Leg(venue=Venue.KALSHI, market_id=m) for m in ("A", "B", "C")],
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
    )
    bb = BookBuilder()
    for mid, ask in (("A", "0.30"), ("B", "0.30"), ("C", "0.30")):
        bb.apply_snapshot(snap(Venue.KALSHI, mid, bids=[("0.05", "100")], asks=[(ask, "100")]))
    mkts = {(l.venue, l.market_id): market(l.venue, l.market_id) for l in rel.legs}
    opps = scan_relationship(rel, bb, mkts, FS, 1)
    sigs = {o.signature for o in opps}
    assert "part:YYY" in sigs  # 0.90 + fees < 1.00


def test_emitted_edges_respect_threshold():
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[], asks=[("0.44", "100")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "100")], asks=[]))
    rel = equivalent()
    opps = scan_relationship(
        rel, bb, markets_for(rel), FS, 1, min_edge_per_contract=D("0.50")
    )
    assert opps == []  # 4c/contract edge < 50c threshold


def test_maker_quote_rounds_down_to_tick():
    """Rest a YES bid on Kalshi hedged by PM NO at 0.48 vwap: p_max ~
    1 - 0.48 - pm_fee/contract - maker_fee -> rounded DOWN to 1c tick."""
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "100")], asks=[]))
    rel = equivalent()
    q = maker_quote(rel, 0, bb, markets_for(rel), FS, hedge_size=D("100"))
    assert q is not None
    assert q == q.quantize(D("0.01"))
    assert q < D("0.52")
    # thin hedge book -> no quote
    bb2 = BookBuilder()
    bb2.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "10")], asks=[]))
    assert maker_quote(rel, 0, bb2, markets_for(rel), FS, hedge_size=D("100")) is None


def test_wide_exclusive_scans_fast_canonical_baskets_only():
    """35-leg negRisk exclusive must scan in milliseconds (canonical all-YES /
    all-NO baskets), not enumerate 2^35 side assignments (the event-loop
    stall found live on 2026-07-20)."""
    import time

    from arbbot.registry.model import Registry  # noqa: F401

    n = 35
    legs = [Leg(venue=Venue.POLYMARKET, market_id=f"T{i}") for i in range(n)]
    rel_wide = Relationship(
        id="wide",
        type=RelationshipType.EXCLUSIVE,
        legs=legs,
        verdict=Verdict.EQUIVALENT,
        vetted_by=VettedBy.HUMAN,
    )
    bb = BookBuilder()
    mkts = {}
    for i in range(n):
        # each market bids 0.05: sum of bids = 1.75 > 1 -> sell-side arb
        bb.apply_snapshot(
            snap(Venue.POLYMARKET, f"T{i}", bids=[("0.05", "100")], asks=[("0.10", "100")])
        )
        mkts[(Venue.POLYMARKET, f"T{i}")] = market(Venue.POLYMARKET, f"T{i}")
    t0 = time.monotonic()
    opps = scan_relationship(rel_wide, bb, mkts, FS, 1)
    assert time.monotonic() - t0 < 2.0
    # all-NO basket: cost = 35 * 0.95 = 33.25 < min payoff 34 (n-1)
    assert len(opps) == 1 and opps[0].signature == "wide:" + "N" * n


def test_lifetime_tracker_open_persist_close():
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[], asks=[("0.44", "100")]))
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.52", "100")], asks=[]))
    rel = equivalent()
    opps = scan_relationship(rel, bb, markets_for(rel), FS, ts_local_ns=1000)
    lt = LifetimeTracker()
    assert lt.observe(opps, 1000) == []
    assert lt.open_count() == 1
    assert lt.observe(opps, 2000) == []  # persists
    closed = lt.observe([], 5000)
    assert len(closed) == 1
    first, lifetime = closed[0]
    assert first.ts_local_ns == 1000 and lifetime == 4000
    assert lt.open_count() == 0
