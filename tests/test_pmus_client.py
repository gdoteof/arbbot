"""Shared PM-US client: signing, cross-process rate budget, gateway parity."""

import base64
import sqlite3
import time
from decimal import Decimal

import httpx
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

from arbbot.exec.polymarket_us_gateway import PolymarketUsOrderGateway
from arbbot.venues import pmus

# a throwaway Ed25519 seed, base64-encoded like the real key file
_seed = ed25519.Ed25519PrivateKey.generate().private_bytes(
    encoding=serialization.Encoding.Raw,
    format=serialization.PrivateFormat.Raw,
    encryption_algorithm=serialization.NoEncryption(),
)
KEY_B64 = base64.b64encode(_seed).decode()


# REFERENCE: the pre-consolidation gateway._headers implementation, verbatim
# (any drift in the shared module against this is a live-auth regression).
def _old_gateway_headers(kid, secret_key_b64, method, path, ts):
    priv = ed25519.Ed25519PrivateKey.from_private_bytes(
        base64.b64decode(secret_key_b64)[:32])
    sig = base64.b64encode(priv.sign(f"{ts}{method}{path}".encode())).decode()
    return {"X-PM-Access-Key": kid, "X-PM-Timestamp": ts,
            "X-PM-Signature": sig, "Content-Type": "application/json"}


# ---------------------------------------------------------------- signing ----

def test_sign_headers_matches_reference_and_verifies():
    priv = pmus.load_ed25519(KEY_B64)
    h = pmus.sign_headers("kid-1", priv, "GET", "/v1/portfolio/positions",
                          ts="1700000000000")
    ref = _old_gateway_headers("kid-1", KEY_B64, "GET",
                               "/v1/portfolio/positions", "1700000000000")
    # Ed25519 is deterministic: same key/ts/message => same signature
    assert h["X-PM-Signature"] == ref["X-PM-Signature"]
    assert h["X-PM-Access-Key"] == "kid-1"
    assert h["X-PM-Timestamp"] == "1700000000000"
    assert "Content-Type" not in h  # WS handshakes reuse these headers bare
    priv.public_key().verify(base64.b64decode(h["X-PM-Signature"]),
                             b"1700000000000GET/v1/portfolio/positions")


def test_gateway_headers_identical_to_old_implementation(monkeypatch):
    monkeypatch.setattr(time, "time", lambda: 1700000000.0)
    g = PolymarketUsOrderGateway("kid-1", KEY_B64, live=False)
    assert g._headers("POST", "/v1/orders") == _old_gateway_headers(
        "kid-1", KEY_B64, "POST", "/v1/orders", "1700000000000")


# ------------------------------------------------------------- rate budget ----

def test_budget_consumes_tokens(tmp_path):
    db = tmp_path / "t.db"
    for _ in range(3):
        pmus.consume_budget(db, budget=5)
    row = sqlite3.connect(db).execute(
        "SELECT venue, used FROM rate_budget").fetchone()
    assert row == ("polymarket_us", 3)


def test_budget_blocks_until_window_rolls(tmp_path, monkeypatch):
    db = tmp_path / "t.db"
    pmus.consume_budget(db, budget=1)
    slept = []

    def fake_sleep(s):  # "the window rolls" instead of really sleeping
        slept.append(s)
        conn = sqlite3.connect(db)
        conn.execute("UPDATE rate_budget SET window_start_ns = window_start_ns - ?",
                     (pmus.WINDOW_NS,))
        conn.commit()
        conn.close()

    monkeypatch.setattr(time, "sleep", fake_sleep)
    pmus.consume_budget(db, budget=1)   # bucket spent -> must wait for the roll
    assert len(slept) == 1 and 0 < slept[0] <= 60
    row = sqlite3.connect(db).execute("SELECT used FROM rate_budget").fetchone()
    assert row[0] == 1  # fresh window, one token used


def test_budget_window_roll_resets_used(tmp_path):
    db = tmp_path / "t.db"
    pmus.consume_budget(db, budget=2)
    pmus.consume_budget(db, budget=2)
    conn = sqlite3.connect(db)
    conn.execute("UPDATE rate_budget SET window_start_ns = window_start_ns - ?",
                 (pmus.WINDOW_NS,))
    conn.commit()
    conn.close()
    pmus.consume_budget(db, budget=2)   # expired window: rolls, no block
    row = sqlite3.connect(db).execute("SELECT used FROM rate_budget").fetchone()
    assert row[0] == 1


# ----------------------------------------------------------------- session ----

def _session(db, handler):
    return pmus.PmusSession("kid", KEY_B64, db_path=db,
                            client=httpx.Client(transport=httpx.MockTransport(handler)))


def test_session_signs_and_consumes_budget(tmp_path):
    db = tmp_path / "exec" / "t.db"
    seen = []

    def handler(req):
        seen.append(req)
        return httpx.Response(200, json={"positions": {"s": {"netPosition": "-2"}}})

    pos = _session(db, handler).get_positions()
    assert pos["s"]["netPosition"] == "-2"
    assert seen[0].headers["x-pm-access-key"] == "kid"
    assert seen[0].headers["x-pm-signature"]
    assert str(seen[0].url) == "https://api.polymarket.us/v1/portfolio/positions"
    row = sqlite3.connect(db).execute(
        "SELECT venue, used FROM rate_budget").fetchone()
    assert row == ("polymarket_us", 1)


def test_critical_priority_never_opens_the_db(tmp_path):
    db = tmp_path / "exec" / "t.db"   # parent dir would be created by connect()
    s = _session(db, lambda req: httpx.Response(
        200, json={"order": {"id": "o1", "cumQuantity": "3"}}))
    o = s.get_order("o1", priority="critical")
    assert o["id"] == "o1"
    assert not db.parent.exists()


def test_get_positions_empty_glitch_raises(tmp_path):
    db = tmp_path / "t.db"
    s = _session(db, lambda req: httpx.Response(200, json={"positions": {}}))
    with pytest.raises(RuntimeError, match="glitch"):
        s.get_positions()
    assert s.get_positions(allow_empty=True) == {}


def test_gateway_order_path_is_critical(tmp_path, monkeypatch):
    # live order placement must never touch the budget DB (never waits)
    called = []
    monkeypatch.setattr(pmus, "consume_budget",
                        lambda *a, **k: called.append(1))
    g = PolymarketUsOrderGateway("kid", KEY_B64, live=True,
                                 client=httpx.Client(transport=httpx.MockTransport(
                                     lambda req: httpx.Response(200, json={"id": "x1"}))))
    g.place_yes("slug", "bid", Decimal("0.50"), 1)
    g.get_order("x1")
    assert called == []


# ------------------------------------------------------- public gateway reads ----

def test_top_of_book_parses_bbo():
    client = httpx.Client(transport=httpx.MockTransport(lambda req: httpx.Response(
        200, json={"marketData": {"bestBid": {"value": "0.44"}, "bestAsk": {}}})))
    bid, ask = pmus.top_of_book(client, "some-slug")
    assert bid == Decimal("0.44") and ask is None


def test_get_book_hits_public_gateway():
    seen = []

    def handler(req):
        seen.append(str(req.url))
        return httpx.Response(200, json={"marketData": {"bids": [], "offers": []}})

    book = pmus.get_book(httpx.Client(transport=httpx.MockTransport(handler)), "s-1")
    assert seen == ["https://gateway.polymarket.us/v1/markets/s-1/book"]
    assert "offers" in book  # ask side is 'offers' on this venue
