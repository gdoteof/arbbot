"""hedge_naked_legs.py venue quirks (card 7fab301e):

- kalshi-deci-cent-ticks: the market's price_ranges[].step defines the tick
  (deci-cent markets take 0.001 prices; a penny market 400s anything finer);
  the profit-locking hedge limit must be FLOORED to that grid.
- kalshi-positions fields: /portfolio/positions position_fp is a plain SIGNED
  contract count ("-3.00" == short 3).

MockTransport + tmp cwd only; dry-run — no orders, no venue.
"""

import importlib.util
import json
import pathlib
import sys
from decimal import Decimal

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

spec = importlib.util.spec_from_file_location(
    "hedge_naked_legs",
    pathlib.Path(__file__).parent.parent / "scripts" / "hedge_naked_legs.py")
hn = importlib.util.module_from_spec(spec)
spec.loader.exec_module(hn)

KEY = rsa.generate_private_key(public_exponent=65537, key_size=2048)
KEY_PEM = KEY.private_bytes(serialization.Encoding.PEM,
                            serialization.PrivateFormat.PKCS8,
                            serialization.NoEncryption())
PM_SEED_B64 = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="

REGISTRY_YAML = """\
relationships:
- id: xvus-test-pair
  type: cross-venue-equivalent
  legs:
  - {venue: kalshi, market_id: KXTEST-26, side: 'yes', role: taker}
  - {venue: polymarket_us, market_id: pm-test-26, side: 'yes', role: taker}
  verdict: equivalent
  vetted_by: human
"""


def mock_client(handler):
    return httpx.Client(transport=httpx.MockTransport(handler))


def test_kalshi_ask_parses_deci_cent_step():
    c = mock_client(lambda req: httpx.Response(200, json={"markets": [
        {"ticker": "KXDECI", "yes_ask_dollars": "0.053",
         "price_ranges": [{"step": "0.001"}]}]}))
    ask, tick = hn.kalshi_ask(c, "KXDECI")
    assert ask == Decimal("0.053")
    assert tick == Decimal("0.001")


def test_kalshi_ask_defaults_to_penny_without_price_ranges():
    c = mock_client(lambda req: httpx.Response(200, json={"markets": [
        {"ticker": "KXPENNY", "yes_ask_dollars": "0.05"}]}))
    ask, tick = hn.kalshi_ask(c, "KXPENNY")
    assert ask == Decimal("0.05")
    assert tick == Decimal("0.01")


def test_kalshi_ask_no_ask_returns_none():
    c = mock_client(lambda req: httpx.Response(200, json={"markets": [{}]}))
    ask, tick = hn.kalshi_ask(c, "KXNONE")
    assert ask is None and tick == Decimal("0.01")


def test_kalshi_positions_position_fp_is_signed_count():
    c = mock_client(lambda req: httpx.Response(200, json={"market_positions": [
        {"ticker": "KXLONG", "position_fp": "5.00"},
        {"ticker": "KXSHORT", "position_fp": "-3.00"}]}))
    pos = hn.kalshi_positions(c, "kid", KEY)
    assert pos == {"KXLONG": 5.0, "KXSHORT": -3.0}


def test_hedge_limit_floored_to_deci_cent_tick(tmp_path, monkeypatch, capsys):
    """Naked PM short x5, pm basis 0.60, Kalshi ask 0.10 on a 0.001 market:
    raw limit = 1 - 0.60 - fee(0.0063) - 0.005 = 0.3887 -> already on the
    0.001 grid it stays 0.3880 after the floor; on a penny market the same
    number must floor to 0.3800 (never send a sub-tick price)."""
    creds = tmp_path / "creds"
    creds.mkdir()
    (creds / "kalshi_api_key_id").write_text("kid")
    (creds / "kalshi_private_key.pem").write_bytes(KEY_PEM)
    (creds / "polymarket_usa_key_id").write_text("pmkid")
    (creds / "polymarket_usa_private_key").write_text(PM_SEED_B64)
    monkeypatch.setattr(hn, "D", creds)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "registry.yaml").write_text(REGISTRY_YAML)
    (tmp_path / "data" / "exec").mkdir(parents=True)

    steps = {"v": [{"step": "0.001"}]}

    def handler(req):
        if req.url.host == "api.polymarket.us":
            return httpx.Response(200, json={"positions": {
                "pm-test-26": {"netPosition": "-5",
                               "costPerShare": {"value": "0.6000", "currency": "USD"}}}})
        if req.url.path.endswith("/portfolio/positions"):
            return httpx.Response(200, json={"market_positions": []})
        return httpx.Response(200, json={"markets": [
            {"ticker": "KXTEST-26", "yes_ask_dollars": "0.10",
             "price_ranges": steps["v"]}]})

    real_client = httpx.Client
    monkeypatch.setattr(hn.httpx, "Client", lambda **kw: real_client(
        transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(hn.time, "sleep", lambda s: None)
    monkeypatch.setattr(sys, "argv", ["hedge_naked_legs"])  # dry-run

    hn.main()
    out = capsys.readouterr().out
    assert "IOC limit 0.3880" in out, "deci-cent market: limit floored to 0.001 grid"
    assert "dry-run — no orders placed" in out

    steps["v"] = []  # same book, penny market -> coarser floor
    hn.main()
    out = capsys.readouterr().out
    assert "IOC limit 0.3800" in out, "penny market: 0.3887 must floor to 0.38"
