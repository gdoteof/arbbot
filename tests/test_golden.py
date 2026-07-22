"""Golden differential harness: canonical serialization + determinism.

These pin the properties a rewrite (e.g. Rust) diffs against: the scanner's
decision stream over identical inputs must be byte-identical, and the
canonical JSON encoding must be stable (sorted keys, Decimals as strings).
"""

import hashlib
import json
from decimal import Decimal

from arbbot.book.builder import BookBuilder
from arbbot.fees.curves import FeeSchedule
from arbbot.models.core import BookSnapshot, Level, Venue
from arbbot.scan.scanner import scan_relationship

from test_scanner import equivalent, markets_for, snap

D = Decimal
FS = FeeSchedule()


def _crossed_books():
    """Books where K-YES ask + P-NO ask < $1 -> at least one opportunity."""
    bb = BookBuilder()
    bb.apply_snapshot(snap(Venue.KALSHI, "K", bids=[("0.38", "50")], asks=[("0.40", "50")]))
    # P YES bid 0.55 -> NO ask 0.45; basket 0.40 + 0.45 = 0.85 < 1
    bb.apply_snapshot(snap(Venue.POLYMARKET, "P", bids=[("0.55", "50")], asks=[("0.60", "50")]))
    return bb


def _decision_stream():
    rel = equivalent()
    bb = _crossed_books()
    lines = []
    for opp in scan_relationship(rel, bb, markets_for(rel), FS, ts_local_ns=7):
        lines.append(json.dumps(opp.to_json_dict(), sort_keys=True,
                                separators=(",", ":")))
    return lines


def test_scan_emits_on_crossed_books():
    assert _decision_stream(), "fixture books must produce an opportunity"


def test_decision_stream_is_deterministic():
    a, b = _decision_stream(), _decision_stream()
    assert a == b
    ha = hashlib.sha256(("\n".join(a)).encode()).hexdigest()
    hb = hashlib.sha256(("\n".join(b)).encode()).hexdigest()
    assert ha == hb


def test_canonical_encoding_stable():
    """Decimals serialize as strings, keys sorted — the parity contract."""
    line = _decision_stream()[0]
    d = json.loads(line)
    assert list(d.keys()) == sorted(d.keys())
    for k in ("size", "gross_cost", "fees", "min_payoff",
              "net_edge_total", "net_edge_per_contract"):
        assert isinstance(d[k], str), f"{k} must be a Decimal string, not float"
    for leg in d["basket"]:
        assert isinstance(leg["vwap"], str) and isinstance(leg["fee"], str)
    # re-encoding the parsed dict reproduces the exact line (no float drift)
    assert json.dumps(d, sort_keys=True, separators=(",", ":")) == line
