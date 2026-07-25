"""Intent-parity harness: deterministic Quoter replay over a synthetic tape.

The pinned canonical line below is the cross-language contract — the Rust
side of the parity harness pins the SAME byte string for this fixture.
"""

import hashlib
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from intent_replay import replay

from arbbot.models.core import Venue
from arbbot.registry.model import (
    Leg, Relationship, RelationshipType, Verdict, VettedBy,
)

# first intent of the fixture replay, canonical JSON (sort_keys, no spaces)
PINNED_PLACE_LINE = (
    '{"count":5,"order_id":"m1","place":"P","price":"0.31",'
    '"side":"bid","ts":2.0,"venue":"polymarket_us"}'
)


def xv_rel():
    return Relationship(
        id="xv-test", type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[Leg(venue=Venue.KALSHI, market_id="K"),
              Leg(venue=Venue.POLYMARKET_US, market_id="P")],
        verdict=Verdict.EQUIVALENT, vetted_by=VettedBy.HUMAN)


def snap(venue, mid, bids, asks, seq, ts_ns):
    return {"kind": "snapshot", "venue": venue, "market_id": mid,
            "bids": [{"price": p, "size": s} for p, s in bids],
            "asks": [{"price": p, "size": s} for p, s in asks],
            "seq": seq, "ts_local_ns": ts_ns}


def tape():
    return [
        # K deep bid 0.60 => hedge NO costs 0.40; P not yet booked: no quote
        snap("kalshi", "K", [("0.60", "500")], [("0.99", "1")], 1, 1_000_000_000),
        # P bid 0.30 => maker YES-bid one tick inside at 0.31 (m1)
        snap("polymarket_us", "P", [("0.30", "500")], [("0.99", "1")], 1, 2_000_000_000),
        # P bid moves 0.5s later: still profitable => min_requote_s throttle
        # (tape time) must HOLD the quote, no reprice intent
        snap("polymarket_us", "P", [("0.32", "500")], [("0.99", "1")], 2, 2_500_000_000),
        # K bid collapses: P maker quote unviable => cancel m1 (a K-leg bid
        # becomes viable at the same tick and places first, leg order: m2)
        snap("kalshi", "K", [("0.30", "500")], [("0.99", "1")], 2, 20_000_000_000),
    ]


def run(tmp_path, name="intents.jsonl"):
    out = tmp_path / name
    res = replay(tape(), [xv_rel()], str(out))
    return res, out.read_text()


def test_place_throttle_cancel_stream(tmp_path):
    res, text = run(tmp_path)
    lines = text.splitlines()
    intents = [json.loads(l) for l in lines]
    assert res["events"] == res["book_events"] == 4
    assert res["intents"] == 3
    # place with the first global order id
    assert intents[0]["place"] == "P" and intents[0]["order_id"] == "m1"
    assert intents[0]["price"] == "0.31" and intents[0]["ts"] == 2.0
    # throttle: the 2.5s book move produced NO intent (nothing between ts 2 and 20)
    assert all(i["ts"] in (2.0, 20.0) for i in intents)
    # unviable book cancels the resting quote
    cancels = [i for i in intents if "cancel" in i]
    assert cancels == [{"cancel": "P", "order_id": "m1", "price": "0.31",
                        "side": "bid", "ts": 20.0, "venue": "polymarket_us"}]
    # digest is the fold of the emitted lines
    assert res["sha256"] == hashlib.sha256(text.encode()).hexdigest()


def test_pinned_canonical_line(tmp_path):
    _, text = run(tmp_path)
    assert text.splitlines()[0] == PINNED_PLACE_LINE


def test_double_run_is_byte_identical(tmp_path):
    res1, text1 = run(tmp_path, "a.jsonl")
    res2, text2 = run(tmp_path, "b.jsonl")
    assert text1 == text2
    assert res1 == res2
