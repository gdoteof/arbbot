"""Quoter: quote placement/replace/cancel + hedge-on-fill, dry-run gateway."""

from decimal import Decimal

from cryptography.hazmat.primitives.asymmetric import rsa

from arbbot.book.builder import BookBuilder
from arbbot.exec.kalshi_gateway import KalshiOrderGateway
from arbbot.exec.quoter import Quoter, maker_leg_indices
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookSnapshot, Level, Market, Venue
from arbbot.registry.model import (
    Leg, Relationship, RelationshipType, Verdict, VettedBy,
)
from arbbot.risk.manager import RiskConfig, RiskManager

D = Decimal
KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)


def fed_pair():
    # both legs Kalshi; implies [t3.75 antecedent, t3.50 consequent]
    return Relationship(
        id="kalshi-fed", type=RelationshipType.IMPLIES,
        legs=[Leg(venue=Venue.KALSHI, market_id="T375"),
              Leg(venue=Venue.KALSHI, market_id="T350")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN,
    )


def markets():
    return {(Venue.KALSHI, m): Market(venue=Venue.KALSHI, market_id=m)
            for m in ("T375", "T350")}


def snap(mid, bids, asks, seq=1):
    return BookSnapshot(venue=Venue.KALSHI, market_id=mid,
                        bids=[Level(price=D(p), size=D(s)) for p, s in bids],
                        asks=[Level(price=D(p), size=D(s)) for p, s in asks],
                        seq=seq, ts_local_ns=seq)


def mk(tmp):
    gw = KalshiOrderGateway("kid", KEY, live=False)
    risk = RiskManager(config=RiskConfig(bankroll=D("520"),
                                         kill_switch_file=str(tmp / "KILL")))
    risk.balances = {Venue.KALSHI: D("440")}
    return Quoter(rel=fed_pair(), gateways={Venue.KALSHI: gw}, risk=risk,
                  markets=markets(), fees=FeeSchedule(), clip=5)


def test_maker_leg_indices():
    assert maker_leg_indices(fed_pair()) == [1]  # consequent only


def test_touch_excl_self_ignores_our_own_order(tmp_path):
    from arbbot.exec.quoter import RestingQuote
    from arbbot.exec.state_machine import TradeStateMachine
    q = mk(tmp_path)
    bb = BookBuilder()
    # our 5-ct bid sits at 0.56 (best), a competitor bids 0.55; our 5-ct ask at
    # 0.60 (best), competitor asks 0.61
    bb.apply_snapshot(snap("T350", [("0.56", "5"), ("0.55", "500")],
                                    [("0.60", "5"), ("0.61", "500")], seq=1))
    b = bb.get("kalshi", "T350")
    sm = lambda: TradeStateMachine(trade_id="t", relationship_id="r")
    q._resting[(1, "bid")] = RestingQuote("o1", "bid", D("0.56"), 5, sm())
    q._resting[(1, "ask")] = RestingQuote("o2", "ask", D("0.60"), 5, sm())
    # our own order is subtracted out -> the touch is the COMPETITOR, so we hold
    # one tick inside them rather than undercutting ourselves toward the mid
    assert q._touch_excl_self(b, 1, "bid") == D("0.55")
    assert q._touch_excl_self(b, 1, "ask") == D("0.61")


def test_touch_excl_self_keeps_level_when_others_joined(tmp_path):
    from arbbot.exec.quoter import RestingQuote
    from arbbot.exec.state_machine import TradeStateMachine
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T350", [("0.56", "20")], [("0.99", "1")], seq=1))
    q._resting[(1, "bid")] = RestingQuote(
        "o", "bid", D("0.56"), 5, TradeStateMachine(trade_id="t", relationship_id="r"))
    # 20 at our price, only 5 is ours -> 15 of others remain, still a competitor
    assert q._touch_excl_self(bb.get("kalshi", "T350"), 1, "bid") == D("0.56")


def test_hedge_depth_guard(tmp_path):
    # maker leg 1 (T350) hedges on leg 0 (T375): a maker ask hedges by BUYING
    # YES (needs T375 ask depth); a maker bid hedges by SELLING YES (needs T375
    # bid depth). Thin bid, deep ask => bid side unhedgeable, ask side ok.
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "2")], [("0.75", "500")]))
    assert q._hedge_has_depth(bb, 1, "ask") is True
    assert q._hedge_has_depth(bb, 1, "bid") is False


def test_places_quote_when_viable(tmp_path):
    q = mk(tmp_path)
    bb = BookBuilder()
    # consequent T350: bid book so a maker bid can improve; antecedent gives hedge
    # implies riskless basket NO(t3.75)+YES(t3.50): maker YES-bid on T350,
    # hedge buys NO on T375. Make T375 NO cheap (YES bid high) so hedge is cheap.
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    placed = [i for i in q.intents if "place" in i]
    assert placed, "should place a maker quote on the consequent leg"
    assert placed[0]["place"] == "T350"
    # target is at/above the current top bid, rounded to cent
    assert D(placed[0]["price"]) >= D("0.55")


def test_replace_on_price_move_and_cancel_on_unviable(tmp_path):
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    n_after_first = len(q._resting)
    assert n_after_first == 1
    # hedge collapses (T375 YES bid drops -> NO expensive) -> unviable -> cancel
    bb.apply_snapshot(snap("T375", [("0.30", "500")], [("0.99", "1")], seq=2))
    q.on_book(bb)
    assert len(q._resting) == 0
    assert any(r["op"] == "cancel" for r in q.gateways[Venue.KALSHI].sent)


def _jitter_book():
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    return bb


def _jitter_q(tmp, **kw):
    base = mk(tmp)
    return Quoter(rel=fed_pair(), gateways=base.gateways, risk=base.risk,
                  markets=markets(), fees=FeeSchedule(), clip=5, **kw)


def test_jitter_never_more_aggressive_than_cap_and_size_bounded(tmp_path):
    import random
    # no-jitter cap (the one-tick-inside price we'd place deterministically)
    q0 = mk(tmp_path); q0.on_book(_jitter_book())
    cap = D([i for i in q0.intents if "place" in i][0]["price"])
    random.seed(0)
    for _ in range(40):
        q = _jitter_q(tmp_path, price_jitter_ticks=1, size_jitter=2, deadband_ticks=2)
        q.on_book(_jitter_book())
        pl = [i for i in q.intents if "place" in i][0]
        px = D(pl["price"])
        assert px <= cap, "jitter must never beat the cap (never more aggressive)"
        assert px >= cap - D("0.01"), "jitter bounded to 1 tick more passive"
        assert 3 <= pl["count"] <= 5, "size jitter within [clip-2, clip]"


def test_jitter_deadband_prevents_churn(tmp_path):
    import random
    random.seed(1)
    # min_requote_s=0 so ONLY the deadband can prevent a reprice
    q = _jitter_q(tmp_path, price_jitter_ticks=1, size_jitter=2, deadband_ticks=2,
                  min_requote_s=0.0)
    bb = _jitter_book()
    q.on_book(bb)
    n1 = len([i for i in q.intents if "place" in i])
    assert n1 == 1
    q.on_book(bb)  # identical book, no time throttle -> deadband must hold
    n2 = len([i for i in q.intents if "place" in i])
    assert n2 == 1, "a still-profitable jittered quote must hold, not churn"


def test_hedge_on_fill_transitions_to_hedged(tmp_path):
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.75", "500")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    res = q.on_fill(1, "bid", bb)
    assert res["hedged"] is True
    assert res["state"] == "hedged"
    # a hedge buy order was sent on the antecedent leg T375
    assert any(r.get("body", {}).get("ticker") == "T375" for r in q.gateways[Venue.KALSHI].sent)


def test_kill_switch_cancels_all(tmp_path):
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    assert len(q._resting) == 1
    q.risk.trigger_kill()
    q.on_book(bb)
    assert len(q._resting) == 0


def test_dry_run_never_live(tmp_path):
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    assert all(r["live"] is False for r in q.gateways[Venue.KALSHI].sent)


class FailingGateway:
    """Gateway whose place always fails — by raising (venue 400) or by
    returning a response with NO order id (pmus-order-id-field-is-id quirk).
    Pins the back-off path: no phantom RestingQuote, no per-tick retry spam."""
    def __init__(self, mode):
        self.mode = mode
        self.places = 0
    def place_yes(self, market_id, side, price, count, post_only=True):
        self.places += 1
        if self.mode == "raise":
            raise RuntimeError("400 post-only would cross")
        return {"status": "accepted"}  # no order_id / order.order_id / id
    def cancel(self, order_id, market_slug=None):
        return None


def _viable_book():
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    return bb


def _failing_q(tmp, mode):
    risk = RiskManager(config=RiskConfig(bankroll=D("520"),
                                         kill_switch_file=str(tmp / "KILL")))
    risk.balances = {Venue.KALSHI: D("440")}
    gw = FailingGateway(mode)
    return gw, Quoter(rel=fed_pair(), gateways={Venue.KALSHI: gw}, risk=risk,
                      markets=markets(), fees=FeeSchedule(), clip=5)


def test_no_order_id_in_response_is_place_failure(tmp_path):
    gw, q = _failing_q(tmp_path, "noid")
    q.on_book(_viable_book())
    assert gw.places == 1
    assert q._resting == {}, "must not track a phantom None-id quote"
    assert any("place_failed" in i for i in q.intents)
    assert (1, "bid") in q._place_failed


def test_failed_place_backs_off_min_requote_s(tmp_path):
    for mode in ("noid", "raise"):
        gw, q = _failing_q(tmp_path, mode)
        bb = _viable_book()
        q.on_book(bb)
        assert gw.places == 1
        q.on_book(bb)  # immediately again: inside min_requote_s => no retry spam
        assert gw.places == 1, f"mode={mode}: failed place must back off"


def test_one_tick_book_joins_best_bid_never_crosses(tmp_path):
    # kalshi-postonly-cross-rejected-400: on a 1-tick book (bid 0.55/ask 0.56)
    # the target clamps strictly below the ask — join best bid, never cross
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.56", "500")]))
    q.on_book(bb)
    placed = [i for i in q.intents if "place" in i]
    assert placed and placed[0]["price"] == "0.55"


class FakePMGateway:
    """Stand-in Polymarket gateway (records intended orders)."""
    def __init__(self): self.sent = []
    def place_yes(self, token_id, side, price, count):
        oid = f"pm-{len(self.sent)}"
        self.sent.append({"venue": "polymarket", "token": token_id, "side": side,
                          "price": str(price), "count": count, "op": "place", "live": False})
        return {"order_id": oid, "status": "dry_run"}
    def cancel(self, oid):
        self.sent.append({"op": "cancel", "order_id": oid, "live": False})
        return {"status": "dry_cancel"}


def xv_pair():
    return Relationship(
        id="xv", type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[Leg(venue=Venue.KALSHI, market_id="KX"),
              Leg(venue=Venue.POLYMARKET, market_id="TOK")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN)


def test_cross_venue_quote_on_pm_hedge_on_kalshi(tmp_path):
    """Maker on the PM leg (fee-free, reward-bearing), hedge taker on Kalshi."""
    rel = xv_pair()
    kgw = KalshiOrderGateway("kid", KEY, live=False)
    pgw = FakePMGateway()
    risk = RiskManager(config=RiskConfig(bankroll=D("520"),
                                         kill_switch_file=str(tmp_path / "KILL")))
    risk.balances = {Venue.KALSHI: D("440"), Venue.POLYMARKET: D("60")}
    mkts = {(Venue.KALSHI, "KX"): Market(venue=Venue.KALSHI, market_id="KX"),
            (Venue.POLYMARKET, "TOK"): Market(venue=Venue.POLYMARKET, market_id="TOK",
                                              tick_size=D("0.001"), min_order_size=D("5"))}
    q = Quoter(rel=rel, gateways={Venue.KALSHI: kgw, Venue.POLYMARKET: pgw},
               risk=risk, markets=mkts, fees=FeeSchedule(), clip=5,
               quote_venue=Venue.POLYMARKET)
    bb = BookBuilder()
    # Kalshi hedge cheap (YES bid 0.60 -> NO 0.40); PM bid 0.55 so a maker YES-bid
    # improves inside the profitable band
    bb.apply_snapshot(BookSnapshot(venue=Venue.KALSHI, market_id="KX",
        bids=[Level(price=D("0.60"), size=D("500"))], asks=[Level(price=D("0.99"), size=D("1"))],
        seq=1, ts_local_ns=1))
    bb.apply_snapshot(BookSnapshot(venue=Venue.POLYMARKET, market_id="TOK",
        bids=[Level(price=D("0.30"), size=D("500"))], asks=[Level(price=D("0.99"), size=D("1"))],
        seq=1, ts_local_ns=1))
    q.on_book(bb)
    # only the PM leg is quoted (quote_venue filter)
    assert pgw.sent, "should rest a maker quote on the PM leg"
    assert all(s.get("venue") == "polymarket" for s in pgw.sent if s["op"] == "place")
    assert not any(r["op"] == "place" for r in kgw.sent), "kalshi is hedge-only here"
    # fill the PM maker bid -> hedge buys NO on Kalshi (a YES-ask on KX)
    res = q.on_fill(1, "bid", bb)
    assert res["hedged"] is True and res["state"] == "hedged"
    assert any(r.get("body", {}).get("ticker") == "KX" for r in kgw.sent)


def test_apr_hurdle_tightens_and_blocks(tmp_path):
    """card 80ff7987: with min_apr set, the maker price cap tightens by the
    per-contract lock needed to annualize >= min_apr over the hold; a thin
    edge on a long-dated market stops quoting entirely."""
    base = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    base.on_book(bb)
    base_px = D([i for i in base.intents if "place" in i][0]["price"])

    # hurdle on but not binding (edge >> margin): price unchanged — the
    # margin tightens the CAP, it never walks a joining quote lower
    q = mk(tmp_path)
    q.min_apr, q.resolve_years = 12.0, 1.0
    assert q._apr_margin() > 0
    q.on_book(bb)
    placed = [i for i in q.intents if "place" in i]
    assert placed and D(placed[0]["price"]) == base_px

    # extreme hurdle: nothing rests
    q2 = mk(tmp_path)
    q2.min_apr, q2.resolve_years = 500.0, 1.0
    q2.on_book(bb)
    assert not [i for i in q2.intents if "place" in i]

    # no resolve date => hurdle disabled, identical to base behavior
    q3 = mk(tmp_path)
    q3.min_apr, q3.resolve_years = 12.0, None
    assert q3._apr_margin() == 0


def test_no_instant_repost_after_cancel(tmp_path, monkeypatch):
    """fraalb sawtooth (card 6fb469da): a side we just cancelled must not be
    re-posted on the next book event. A 500-1000 lot maker walks the ask down a
    tick at a time; we chase to our floor, cancel, they pull, and the re-post
    was unthrottled — 961 places + 420 cancels in 24h on one relationship."""
    import arbbot.exec.quoter as qm
    clock = {"t": 1000.0}
    monkeypatch.setattr(qm.time, "monotonic", lambda: clock["t"])
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    assert len(q._resting) == 1
    placed = lambda: sum(1 for i in q.intents if "place" in i)
    n = placed()

    # hedge collapses -> unviable -> prompt cancel (cancels must stay immediate)
    bb.apply_snapshot(snap("T375", [("0.30", "500")], [("0.99", "1")], seq=2))
    clock["t"] += 1
    q.on_book(bb)
    assert len(q._resting) == 0

    # ...and recovers a second later: the old code re-posted on this very event
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")], seq=3))
    clock["t"] += 1
    q.on_book(bb)
    assert placed() == n, "re-entry within min_requote_s must be throttled"
    assert not q._resting

    # past the throttle it re-enters normally
    clock["t"] += q.min_requote_s
    q.on_book(bb)
    assert placed() == n + 1
    assert len(q._resting) == 1


def test_fill_clears_the_requote_throttle(tmp_path):
    """The re-entry throttle must not outlive a FILL: on_fill drops the
    timestamp so the side can re-quote fresh next tick (whether it actually
    does is then the risk manager's call, not the throttle's)."""
    q = mk(tmp_path)
    bb = BookBuilder()
    bb.apply_snapshot(snap("T375", [("0.70", "500")], [("0.99", "1")]))
    bb.apply_snapshot(snap("T350", [("0.55", "500")], [("0.99", "1")]))
    q.on_book(bb)
    key = next(iter(q._resting))
    assert key in q._last_quote_ts
    q.on_fill(key[0], key[1], bb)
    assert key not in q._last_quote_ts, "a fill must not leave the side throttled"
