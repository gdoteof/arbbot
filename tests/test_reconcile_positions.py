"""reconcile_positions.py venue-quirk guards (card 7fab301e):

- pmus-positions-empty-glitch: an EMPTY PM positions read is a glitch (we
  always hold PM positions) -> raise for retry; all-empty ends in "RECON
  error", never a false NAKED.
- pmus-positions-partial-stale-sticky: a PARTIAL read (a ledger-expected slug
  missing) is DEGRADED — skip naked evaluation entirely, don't page.
- two-consecutive-runs rule: a real imbalance alerts only when seen on two
  consecutive runs (PM staleness is sticky within a run).
- xv-settlement-skew: an imbalance whose Kalshi market is no longer active is
  SETTLING (the sweeper's domain), not naked — suppressed.

Everything is MockTransport + tmp cwd; no venue is ever touched.
"""

import base64
import importlib.util
import json
import pathlib

import httpx
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

spec = importlib.util.spec_from_file_location(
    "reconcile_positions",
    pathlib.Path(__file__).parent.parent / "scripts" / "reconcile_positions.py")
rp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(rp)

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
- id: xvus-test-pair2
  type: cross-venue-equivalent
  legs:
  - {venue: kalshi, market_id: KXTEST2-26, side: 'yes', role: taker}
  - {venue: polymarket_us, market_id: pm-other-26, side: 'yes', role: taker}
  verdict: equivalent
  vetted_by: human
"""


def ledger_open(rel_id, kt, slug, qty=5, ts=1000.0):
    return {"ts": ts, "relationship_id": rel_id, "qty": qty, "status": "open",
            "strategy": "take-take", "cost_usd": qty * 0.9, "payoff_usd": qty,
            "legs": [{"venue": "kalshi", "market_id": kt, "side": "yes", "qty": qty},
                     {"venue": "polymarket_us", "market_id": slug, "side": "no", "qty": qty}]}


def setup_env(tmp_path, monkeypatch, kalshi_rows, pm_reads, ledger_rows,
              kalshi_market_status="active"):
    """tmp cwd + fake creds + MockTransport router. pm_reads is a list of
    positions dicts served in order (last repeats). Returns the call log."""
    creds = tmp_path / "creds"
    creds.mkdir()
    (creds / "kalshi_api_key_id").write_text("kid")
    (creds / "kalshi_private_key.pem").write_bytes(KEY_PEM)
    (creds / "polymarket_usa_key_id").write_text("pmkid")
    (creds / "polymarket_usa_private_key").write_text(PM_SEED_B64)
    monkeypatch.setattr(rp, "D", creds)
    monkeypatch.chdir(tmp_path)
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "registry.yaml").write_text(REGISTRY_YAML)
    (tmp_path / "data" / "exec").mkdir(parents=True)
    (tmp_path / "data" / "exec" / "trades.jsonl").write_text(
        "\n".join(json.dumps(r) for r in ledger_rows) + "\n")

    calls = {"pm": 0}
    pm_reads = list(pm_reads)

    def handler(req):
        path = req.url.path
        if req.url.host == "api.polymarket.us":
            i = min(calls["pm"], len(pm_reads) - 1)
            calls["pm"] += 1
            return httpx.Response(200, json={"positions": {
                slug: {"netPosition": str(net),
                       "costPerShare": {"value": "0.6000", "currency": "USD"}}
                for slug, net in pm_reads[i].items()}})
        if path.endswith("/portfolio/positions"):
            return httpx.Response(200, json={"market_positions": [
                {"ticker": t, "position_fp": f"{q:.2f}",
                 "market_exposure_dollars": "2.0", "realized_pnl_dollars": "0",
                 "fees_paid_dollars": "0"} for t, q in kalshi_rows.items()]})
        if path.endswith("/markets"):
            ts = (req.url.params.get("tickers") or "").split(",")
            return httpx.Response(200, json={"markets": [
                {"ticker": t, "status": kalshi_market_status,
                 "yes_bid_dollars": "0.40", "yes_ask_dollars": "0.45"}
                for t in ts if t]})
        return httpx.Response(200, json={"marketData": {}})  # PM bbo etc.

    real_client = httpx.Client
    monkeypatch.setattr(rp.httpx, "Client", lambda **kw: real_client(
        transport=httpx.MockTransport(handler)))
    monkeypatch.setattr(rp.time, "sleep", lambda s: None)
    return calls


def test_empty_pm_read_is_glitch_retried_then_recon_error(tmp_path, monkeypatch, capsys):
    # PM serves EMPTY every time: retried (3x), then RECON error — a glitched
    # empty read must NEVER evaluate as "everything naked".
    calls = setup_env(tmp_path, monkeypatch,
                      kalshi_rows={"KXTEST-26": 5}, pm_reads=[{}],
                      ledger_rows=[ledger_open("xvus-test-pair", "KXTEST-26", "pm-test-26")])
    rp.main()
    out = capsys.readouterr().out
    assert "RECON error RuntimeError" in out
    assert "NAKED" not in out
    assert calls["pm"] == 3, "empty read must be retried, not trusted"


def test_empty_pm_read_recovers_on_retry(tmp_path, monkeypatch, capsys):
    setup_env(tmp_path, monkeypatch,
              kalshi_rows={"KXTEST-26": 5},
              pm_reads=[{}, {"pm-test-26": -5}],
              ledger_rows=[ledger_open("xvus-test-pair", "KXTEST-26", "pm-test-26")])
    rp.main()
    out = capsys.readouterr().out
    assert "RECON ok — 1 baskets balanced" in out


def test_partial_pm_read_missing_ledger_leg_is_degraded_not_naked(tmp_path, monkeypatch, capsys):
    # ledger says pm-other-26 MUST exist; the read dropped it (platform
    # incident). Without the guard, pair2 would read kYES+5/pm+0 = naked.
    setup_env(tmp_path, monkeypatch,
              kalshi_rows={"KXTEST-26": 5, "KXTEST2-26": 5},
              pm_reads=[{"pm-test-26": -5}],
              ledger_rows=[
                  ledger_open("xvus-test-pair", "KXTEST-26", "pm-test-26", ts=1000.0),
                  ledger_open("xvus-test-pair2", "KXTEST2-26", "pm-other-26", ts=1001.0)])
    rp.main()
    rp.main()  # even consecutive degraded runs never page
    out = capsys.readouterr().out
    assert out.count("RECON degraded") == 2
    assert "NAKED" not in out


def test_real_naked_alerts_only_on_second_consecutive_run(tmp_path, monkeypatch, capsys):
    # genuine imbalance (kYES+5 vs pmNet-3, slug PRESENT so not degraded):
    # first sighting stays silent; the second run confirms and pages.
    setup_env(tmp_path, monkeypatch,
              kalshi_rows={"KXTEST-26": 5},
              pm_reads=[{"pm-test-26": -3}],
              ledger_rows=[ledger_open("xvus-test-pair", "KXTEST-26", "pm-test-26")])
    rp.main()
    first = capsys.readouterr().out
    assert "NAKED" not in first
    rp.main()
    second = capsys.readouterr().out
    assert "RECON NAKED xvus-test-pair:kYES+5/pmNet-3/imb+2" in second


def test_settlement_skew_finalized_kalshi_market_suppresses_naked(tmp_path, monkeypatch, capsys):
    # same imbalance, but the Kalshi market is FINALIZED: one venue settled
    # before the other — the settlement sweeper's domain, not a naked leg.
    setup_env(tmp_path, monkeypatch,
              kalshi_rows={"KXTEST-26": 5},
              pm_reads=[{"pm-test-26": -3}],
              ledger_rows=[ledger_open("xvus-test-pair", "KXTEST-26", "pm-test-26")],
              kalshi_market_status="finalized")
    rp.main()
    rp.main()  # second consecutive sighting would normally page
    out = capsys.readouterr().out
    assert "NAKED" not in out
    # the dashboard snapshot marks the row as settling, not naked
    rows = json.loads((tmp_path / "data" / "exec" / "positions.json").read_text())["rows"]
    assert any(r.get("kt") == "KXTEST-26" and r.get("settling") for r in rows)
