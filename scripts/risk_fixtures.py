#!/usr/bin/env python3
"""Risk-manager parity fixtures — the Python reference side of the arb-core
risk port (P4 provable #4 in docs/p3-shell.md).

Drives the PYTHON RiskManager.check_order through a deterministic case matrix
and emits each case's inputs (as decimal STRINGS) plus the observed decision
(allow/deny + reasons + clamped max_notional) to tests/fixtures/risk/cases.json.
The Rust port (rust/crates/arb-core/src/risk.rs) replays these and must match
byte-for-byte on (allowed, reasons, max_notional).

Determinism seams (no money-path src/ change — the decision is made a pure
function of DATA here, same style as scripts/intent_replay.py):
  * kill switch: config.kill_switch_file points at a nonexistent path and the
    private `_kill` flag carries the case's kill state, so kill_switch_active()
    reduces to the input bool — no file on disk is ever consulted.
  * topic_weakest_fwd_apr: monkeypatched to return the case's supplied value
    instead of reading data/exec/marks.json. The weakest deployed forward APR
    is therefore a decision INPUT, not I/O.

  python scripts/risk_fixtures.py            # write cases.json
  python scripts/risk_fixtures.py --stdout   # print, don't write
"""

from __future__ import annotations

import argparse
import json
import sys
from decimal import Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from arbbot.models.core import Venue
from arbbot.registry.model import Leg, OracleRisk, Relationship, RelationshipType
from arbbot.risk import manager as manager_module
from arbbot.risk.manager import OpenExposure, RiskConfig, RiskManager, TopicBudget

OUT = Path(__file__).resolve().parent.parent / "tests" / "fixtures" / "risk" / "cases.json"

_RISK = {"low": OracleRisk.LOW, "medium": OracleRisk.MEDIUM, "high": OracleRisk.HIGH}


def _dec(v):
    return None if v is None else Decimal(str(v))


def _build_manager(inp: dict) -> RiskManager:
    """Construct a RiskManager fully determined by the case's input data."""
    c = inp["config"]
    cfg = RiskConfig(
        bankroll=Decimal(c["bankroll"]),
        tail_fraction=Decimal(c["tail_fraction"]),
        per_rel_cap=_dec(c["per_rel_cap"]),
        per_class_cap=Decimal(c["per_class_cap"]),
        global_cap=Decimal(c["global_cap"]),
        overflow_min_apr=Decimal(c["overflow_min_apr"]),
        overflow_frac=Decimal(c["overflow_frac"]),
        default_topic_budget=_dec(c["default_topic_budget"]),
        default_only_below_util=_dec(c["default_only_below_util"]),
        topics=[
            TopicBudget(
                family=t["family"],
                budget_usd=Decimal(t["budget_usd"]),
                only_below_util=_dec(t["only_below_util"]),
            )
            for t in c["topics"]
        ],
        kill_switch_file="/nonexistent/arbbot-risk-fixture-KILL",
    )
    exp = OpenExposure(
        by_relationship={k: Decimal(v) for k, v in inp["exposure"]["by_relationship"]},
        by_class={k: Decimal(v) for k, v in inp["exposure"]["by_class"]},
        by_topic={k: Decimal(v) for k, v in inp["exposure"]["by_topic"]},
    )
    m = RiskManager(config=cfg, exposure=exp)
    m.balances = {Venue(v): Decimal(b) for v, b in inp["balances"]}
    m._kill = bool(inp["kill"])
    return m


def run_case(name: str, inp: dict) -> dict:
    m = _build_manager(inp)

    # Seam: weakest deployed forward APR is an input, not marks.json I/O.
    weakest = inp["weakest_fwd_apr"]
    weakest_f = None if weakest is None else float(weakest)
    manager_module.RiskManager.topic_weakest_fwd_apr = (  # type: ignore[method-assign]
        lambda self, topic: weakest_f
    )
    try:
        r = _RISK[inp["rel"]["oracle_risk"]]
        rel = Relationship(
            id=inp["rel"]["id"],
            type=RelationshipType(inp["rel"]["type"]),
            legs=[
                Leg(venue=Venue.KALSHI, market_id="a"),
                Leg(venue=Venue.KALSHI, market_id="b"),
            ],
            oracle_risk=r,
        )
        venue_costs = {Venue(v): Decimal(c) for v, c in inp["venue_costs"]}
        opp = inp["opportunity_apr"]
        opp_f = None if opp is None else float(opp)
        decision = m.check_order(
            rel, Decimal(inp["notional"]), venue_costs, opportunity_apr=opp_f
        )
    finally:
        del manager_module.RiskManager.topic_weakest_fwd_apr

    return {
        "name": name,
        "input": inp,
        "output": {
            "allowed": decision.allowed,
            "max_notional": str(decision.max_notional),
            "reasons": list(decision.reasons),
        },
    }


# ---- case matrix -----------------------------------------------------------

def base_config(**over) -> dict:
    c = {
        "bankroll": "1000",
        "tail_fraction": "0.02",
        "per_rel_cap": None,
        "per_class_cap": "0.35",
        "global_cap": "0.50",
        "overflow_min_apr": "25.0",
        "overflow_frac": "0.10",
        "default_topic_budget": None,
        "default_only_below_util": None,
        "topics": [],
    }
    c.update(over)
    return c


def base_input(**over) -> dict:
    inp = {
        "config": base_config(),
        "rel": {"id": "rel-x", "type": "equivalent-pair", "oracle_risk": "low"},
        "exposure": {"by_relationship": [], "by_class": [], "by_topic": []},
        "balances": [["kalshi", "100000"], ["polymarket_us", "100000"]],
        "notional": "10",
        "venue_costs": [["kalshi", "5"]],
        "opportunity_apr": None,
        "weakest_fwd_apr": None,
        "kill": False,
    }
    inp.update(over)
    return inp


def topic(family, budget, gate=None):
    return {"family": family, "budget_usd": str(budget), "only_below_util": gate}


def cases() -> list[dict]:
    out: list[tuple[str, dict]] = []
    add = lambda n, i: out.append((n, i))

    # --- normal allow ---
    add("normal_allow", base_input())

    # --- kill switch ---
    add("kill_on", base_input(kill=True, notional="5"))
    add("kill_on_masks_all", base_input(kill=True, notional="999999"))

    # --- per-relationship tail cap (cap = bankroll*0.02*scaler; low=1.0 -> 20) ---
    add("rel_cap_below", base_input(notional="19"))
    add("rel_cap_at", base_input(notional="20"))          # == headroom 20.000 -> allow
    add("rel_cap_above", base_input(notional="21"))       # deny
    # oracle-risk scalers: medium 0.5 -> cap 10, high 0.25 -> cap 5
    add("rel_cap_medium", base_input(rel={"id": "rel-x", "type": "equivalent-pair",
                                          "oracle_risk": "medium"}, notional="11"))
    add("rel_cap_high", base_input(rel={"id": "rel-x", "type": "equivalent-pair",
                                        "oracle_risk": "high"}, notional="6"))
    # absolute per_rel_cap replaces tail formula (still scaled): 150*0.5 -> 75
    add("rel_cap_abs_medium",
        base_input(config=base_config(per_rel_cap="150"),
                   rel={"id": "rel-x", "type": "equivalent-pair", "oracle_risk": "medium"},
                   notional="76"))
    # concentration: prior open in same rel eats headroom (cap 20, open 15 -> hr 5)
    add("rel_concentration_deny",
        base_input(exposure={"by_relationship": [["rel-x", "15"]],
                             "by_class": [["equivalent-pair", "15"]],
                             "by_topic": [["other", "15"]]},
                   notional="6"))
    # open already exceeds cap -> negative headroom, max_notional clamps to 0
    add("rel_over_cap_negative_headroom",
        base_input(exposure={"by_relationship": [["rel-x", "30"]],
                             "by_class": [["equivalent-pair", "30"]],
                             "by_topic": [["other", "30"]]},
                   notional="0"))

    # --- class cap (bankroll*0.35 = 350) with overflow ---
    # hard_ceiling = 350 + bankroll*overflow_frac(0.10)=100 -> 450
    add("class_cap_below",
        base_input(exposure={"by_relationship": [], "by_class": [["equivalent-pair", "340"]],
                             "by_topic": []}, notional="5"))
    add("class_cap_at",
        base_input(exposure={"by_relationship": [], "by_class": [["equivalent-pair", "340"]],
                             "by_topic": []}, notional="10"))   # 350 == cap -> allow
    add("class_cap_above_no_apr",
        base_input(exposure={"by_relationship": [], "by_class": [["equivalent-pair", "340"]],
                             "by_topic": []}, notional="11"))   # deny, no hint
    add("class_cap_above_apr_below",
        base_input(exposure={"by_relationship": [], "by_class": [["equivalent-pair", "340"]],
                             "by_topic": []}, notional="11", opportunity_apr="10.0"))  # deny+hint
    add("class_cap_overflow_ok",
        base_input(config=base_config(per_rel_cap="10000"),
                   exposure={"by_relationship": [], "by_class": [["equivalent-pair", "340"]],
                             "by_topic": []}, notional="60",     # 400 <= 450 ceiling
                   opportunity_apr="30.0"))                       # allow (bounded breach)
    add("class_cap_overflow_apr_at_min",
        base_input(config=base_config(per_rel_cap="10000"),
                   exposure={"by_relationship": [], "by_class": [["equivalent-pair", "400"]],
                             "by_topic": []}, notional="50",     # 450 == ceiling
                   opportunity_apr="25.0"))                       # apr==min -> allow
    add("class_cap_overflow_above_ceiling",
        base_input(config=base_config(per_rel_cap="10000"),
                   exposure={"by_relationship": [], "by_class": [["equivalent-pair", "400"]],
                             "by_topic": []}, notional="51",     # 451 > 450 ceiling
                   opportunity_apr="99.0"))                       # deny despite great apr

    # --- global cap (bankroll*0.50 = 500) ---
    # spread across two rels so per-rel cap not the binding reason
    add("global_cap_above",
        base_input(config=base_config(per_rel_cap="10000"),
                   exposure={"by_relationship": [["r1", "300"], ["r2", "195"]],
                             "by_class": [["equivalent-pair", "495"]],
                             "by_topic": []},
                   rel={"id": "r1", "type": "equivalent-pair", "oracle_risk": "low"},
                   notional="10"))   # total 495+10 = 505 > 500

    # --- topic budgets ---
    tcfg = lambda **o: base_config(topics=[topic("macron", 100)], **o)
    # relationship id must contain '-macron-' for topic_of to match
    trel = {"id": "fr-macron-24", "type": "equivalent-pair", "oracle_risk": "low"}
    add("topic_budget_below",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]}, notional="10"))
    add("topic_budget_at",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]}, notional="20"))  # ==100 allow
    add("topic_budget_above_no_apr",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]}, notional="21"))  # deny no hint
    add("topic_budget_above_apr_no_marks",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]},
                   notional="21", opportunity_apr="30.0"))  # bar=overflow_min 25 -> allow
    add("topic_budget_above_apr_below_min",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]},
                   notional="21", opportunity_apr="10.0"))  # bar 25, apr 10 -> deny+hint
    add("topic_budget_weakest_bar",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]},
                   notional="21", opportunity_apr="12.0",
                   weakest_fwd_apr="8.0"))  # bar=min(8,25)=8, apr 12>=8 -> allow
    add("topic_budget_weakest_bar_deny",
        base_input(config=tcfg(per_rel_cap="10000"), rel=trel,
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["macron", "80"]]},
                   notional="21", opportunity_apr="5.0",
                   weakest_fwd_apr="8.0"))  # bar 8, apr 5 -> deny + (needs apr>=8)
    # default topic budget path (unlisted topic 'other')
    add("default_topic_budget_deny",
        base_input(config=base_config(per_rel_cap="10000", default_topic_budget="50"),
                   exposure={"by_relationship": [], "by_class": [],
                             "by_topic": [["other", "45"]]}, notional="10"))  # 55>50

    # --- topic gate (only_below_util) ---
    # util = exposure.total / class_cap. class_cap = 1000*0.35 = 350
    gcfg = base_config(per_rel_cap="10000", topics=[topic("macron", 10000, gate="0.50")])
    add("topic_gate_open",
        base_input(config=gcfg, rel=trel,
                   exposure={"by_relationship": [["r1", "100"]], "by_class": [],
                             "by_topic": [["macron", "0"]]},
                   notional="10"))   # util 100/350=0.28 < 0.50 -> allow
    add("topic_gate_blocked",
        base_input(config=gcfg, rel=trel,
                   exposure={"by_relationship": [["r1", "200"]], "by_class": [],
                             "by_topic": [["macron", "0"]]},
                   notional="10"))   # util 200/350=0.57 >= 0.50 -> deny
    add("default_topic_gate_blocked",
        base_input(config=base_config(per_rel_cap="10000", default_only_below_util="0.30"),
                   exposure={"by_relationship": [["r1", "200"]], "by_class": [],
                             "by_topic": []},
                   notional="10"))   # util 0.57 >= 0.30 -> deny (topic 'other')

    # --- balances ---
    add("balance_insufficient",
        base_input(balances=[["kalshi", "3"]], venue_costs=[["kalshi", "5"]]))
    add("balance_missing_venue",
        base_input(balances=[["kalshi", "100000"]],
                   venue_costs=[["polymarket_us", "5"]]))  # avail defaults to 0
    add("balance_exact_ok",
        base_input(balances=[["kalshi", "5"]], venue_costs=[["kalshi", "5"]]))
    add("balance_two_venues_order",
        base_input(balances=[["kalshi", "1"], ["polymarket_us", "1"]],
                   venue_costs=[["kalshi", "5"], ["polymarket_us", "5"]]))  # two reasons in order

    # --- zero / negative edges ---
    add("zero_notional_allow", base_input(notional="0"))
    add("zero_bankroll",
        base_input(config=base_config(bankroll="0"), notional="0",
                   venue_costs=[]))   # every cap is 0; notional 0 within them
    add("negative_notional",
        base_input(notional="-5", venue_costs=[]))  # -5 > headroom? no; caps ok -> allow

    # --- multiple simultaneous reasons ---
    add("multi_reason",
        base_input(exposure={"by_relationship": [["rel-x", "100"]],
                             "by_class": [["equivalent-pair", "600"]],
                             "by_topic": [["other", "0"]]},
                   balances=[["kalshi", "1"]],
                   notional="30", venue_costs=[["kalshi", "5"]]))

    return [run_case(n, i) for n, i in out]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--stdout", action="store_true")
    args = ap.parse_args()
    doc = {"cases": cases()}
    blob = json.dumps(doc, indent=1, sort_keys=False)
    if args.stdout:
        print(blob)
        return
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(blob + "\n")
    print(f"wrote {len(doc['cases'])} cases -> {OUT} ({len(blob)} bytes)")


if __name__ == "__main__":
    main()
