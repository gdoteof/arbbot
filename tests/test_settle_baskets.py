"""settle_baskets.py settlement gate (card 7fab301e, kalshi-positions-and-
settlement-fields): a basket settles ONLY when its Kalshi market is status
'finalized' AND result is 'yes'/'no' — a finalized-but-void (or still-active)
market must not realize anything. A settled cross-venue basket pays exactly
$1/contract regardless of outcome. MockTransport + tmp cwd; read-only on
venues by construction.
"""

import importlib.util
import json
import pathlib

import httpx

spec = importlib.util.spec_from_file_location(
    "settle_baskets",
    pathlib.Path(__file__).parent.parent / "scripts" / "settle_baskets.py")
sb = importlib.util.module_from_spec(spec)
sb.__spec__ = spec
spec.loader.exec_module(sb)


def basket(rel_id, kt, qty=5, cost=4.5, ts=1000.0):
    return {"ts": ts, "relationship_id": rel_id, "title": rel_id, "qty": qty,
            "cost_usd": cost, "payoff_usd": qty, "status": "open",
            "strategy": "take-take",
            "legs": [{"venue": "kalshi", "market_id": kt, "side": "yes", "qty": qty},
                     {"venue": "polymarket_us", "market_id": "pm-x", "side": "no", "qty": qty}]}


def test_settles_only_finalized_with_yes_no_result(tmp_path, monkeypatch, capsys):
    monkeypatch.chdir(tmp_path)
    (tmp_path / "data" / "exec").mkdir(parents=True)
    ledger = tmp_path / "data" / "exec" / "trades.jsonl"
    ledger.write_text("\n".join(json.dumps(b) for b in [
        basket("rel-settled", "K-FIN", ts=1000.0),
        basket("rel-void", "K-VOID", ts=1001.0),
        basket("rel-active", "K-ACT", ts=1002.0)]) + "\n")

    status = {"K-FIN": ("finalized", "no"),      # settles (either result pays $1/ct)
              "K-VOID": ("finalized", None),     # finalized but VOID result -> no
              "K-ACT": ("active", None)}         # not finalized -> no

    def handler(req):
        ts = (req.url.params.get("tickers") or "").split(",")
        return httpx.Response(200, json={"markets": [
            {"ticker": t, "status": status[t][0], "result": status[t][1]}
            for t in ts if t in status]})

    real_client = httpx.Client
    monkeypatch.setattr(sb.httpx, "Client", lambda **kw: real_client(
        transport=httpx.MockTransport(handler)))
    sb.main()

    recs = [json.loads(l) for l in ledger.read_text().splitlines()]
    settled = [r for r in recs if r.get("strategy") == "settlement"]
    assert len(settled) == 1
    s = settled[0]
    assert s["relationship_id"] == "rel-settled"
    assert s["status"] == "unwound" and s["closes_ts"] == 1000.0
    assert s["kalshi_result"] == "no"
    assert s["proceeds_usd"] == 5.0                     # $1/ct by construction
    assert abs(s["realized_pnl_usd"] - 0.5) < 1e-9      # qty - cost
    assert "1 basket(s)/lean(s) settled of 3 open" in capsys.readouterr().out
