"""Dashboard aggregation: incremental tailing, range scoping, health read."""

import json
import time
from datetime import datetime, timezone

from arbbot.dash.data import DashboardData


def _write_registry(tmp_path):
    p = tmp_path / "registry.yaml"
    p.write_text("""
relationships:
  - id: eq1
    type: cross-venue-equivalent
    legs:
      - {venue: kalshi, market_id: K, side: "yes", role: taker}
      - {venue: polymarket, market_id: P, side: "yes", role: taker}
    verdict: equivalent
    vetted_by: human
    confidence: 0.9
    tranche: head
""")
    return p


def opp(day_ts_ns, rel="eq1", edge=0.01, bucket="primary"):
    return {
        "relationship_id": rel, "tranche": "head", "bucket": bucket,
        "signature": f"{rel}:YN", "size": "100", "gross_cost": "92",
        "net_edge_total": str(edge * 100), "net_edge_per_contract": str(edge),
        "min_payoff": "1",
        "basket": [
            {"venue": "kalshi", "role": "taker", "buy_side": "yes",
             "market_id": "K", "vwap": "0.44", "fee": "1.5"},
            {"venue": "polymarket", "role": "taker", "buy_side": "no",
             "market_id": "P", "vwap": "0.48", "fee": str(100 - 92 - 1.5 - edge * 100)},
        ],
        "ts_local_ns": day_ts_ns, "max_hold_close_time": "2027-01-01T00:00:00Z",
    }


def setup_dirs(tmp_path):
    scan = tmp_path / "scan"; scan.mkdir()
    raw = tmp_path / "raw"; raw.mkdir()
    health = tmp_path / "health.jsonl"
    reg = _write_registry(tmp_path)
    return scan, raw, health, reg


def test_summary_aggregates_and_range(tmp_path):
    scan, raw, health, reg = setup_dirs(tmp_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    now_ns = time.time_ns()
    with open(scan / f"opportunities-{today}.jsonl", "w") as f:
        for _ in range(3):
            f.write(json.dumps(opp(now_ns)) + "\n")
    with open(scan / f"lifetimes-{today}.jsonl", "w") as f:
        f.write(json.dumps({**opp(now_ns), "lifetime_ns": 5_000_000_000}) + "\n")
    with open(scan / "opportunities-2020-01-01.jsonl", "w") as f:
        f.write(json.dumps(opp(1_577_836_800_000_000_000)) + "\n")  # old day
    with open(raw / f"kalshi-{today}.jsonl", "w") as f:
        f.write('{"kind":"snapshot"}\n{"kind":"delta"}\n')
    health.write_text(json.dumps({"ts": time.time(), "stale": {"kalshi-ws": False}}) + "\n")

    d = DashboardData(scan, raw, health, reg)
    s = d.summary("today")
    assert s["hero"]["observations"] == 3
    assert s["tiles"]["closed_crossings"] == 1
    assert s["tiles"]["lifetime_p50_s"] == 5.0
    assert s["tiles"]["universe"] == {"total": 1, "vetted": 1}
    assert s["tiles"]["events_recorded"]["kalshi"] == 2
    row = s["relationships"][0]
    assert row["id"] == "eq1" and row["vetted"] == "human"
    assert row["edge_p50"] == 0.01
    assert row["roc_p50"] > 0
    assert s["health"]["recorder"] == "ok"
    assert s["health"]["feeds"] == {"kalshi-ws": "ok"}
    # all-range picks up the 2020 day too
    assert d.summary("all")["hero"]["observations"] == 4
    assert len(d.summary("all")["hourly"]) >= 2


def test_incremental_tail_appends_only(tmp_path):
    scan, raw, health, reg = setup_dirs(tmp_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    p = scan / f"opportunities-{today}.jsonl"
    with open(p, "w") as f:
        f.write(json.dumps(opp(time.time_ns())) + "\n")
    d = DashboardData(scan, raw, health, reg)
    assert d.summary("today")["hero"]["observations"] == 1
    with open(p, "a") as f:
        f.write(json.dumps(opp(time.time_ns())) + "\n")
        f.write('{"partial')  # crash-truncated tail must not break parsing
    d._last_refresh = 0  # bypass throttle
    assert d.summary("today")["hero"]["observations"] == 2
    # completing the partial line later parses it exactly once
    with open(p, "a") as f:
        f.write('-line"}\n')
    d._last_refresh = 0
    s = d.summary("today")
    assert s["hero"]["observations"] == 2  # junk line ignored, no double-count


def test_fee_scenarios_reprice_stored_baskets(tmp_path):
    """pm-zero removes the PM taker fee (edge rises by fee/size); stress-2x
    doubles all fees (edge falls); scanned reproduces the stored number."""
    scan, raw, health, reg = setup_dirs(tmp_path)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(scan / f"opportunities-{today}.jsonl", "w") as f:
        f.write(json.dumps(opp(time.time_ns(), edge=0.01)) + "\n")
    d = DashboardData(scan, raw, health, reg)
    scanned = d.summary("today", "scanned")["relationships"][0]["edge_p50"]
    d._last_refresh = 0
    pmzero = d.summary("today", "pm-zero")["relationships"][0]["edge_p50"]
    d._last_refresh = 0
    stress = d.summary("today", "stress-2x")["relationships"][0]["edge_p50"]
    assert scanned == 0.01
    # PM leg fee was 100-92-1.5-1 = 5.5 -> pm-zero edge = 0.01 + 5.5/100
    assert abs(pmzero - 0.065) < 1e-9
    assert stress < scanned  # doubled fees can only hurt
    # unknown scenario falls back to scanned
    d._last_refresh = 0
    assert d.summary("today", "nonsense")["fee_scenario"] == "scanned"


def test_missing_everything_is_graceful(tmp_path):
    d = DashboardData(tmp_path / "nope", tmp_path / "nope2",
                      tmp_path / "no-health", tmp_path / "no-reg.yaml")
    s = d.summary("today")
    assert s["hero"]["observations"] == 0
    assert s["health"]["recorder"] == "unknown"
