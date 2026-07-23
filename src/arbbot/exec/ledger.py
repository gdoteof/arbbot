"""Ledger netting: open baskets minus compensating unwind records.

The ledger (data/exec/trades.jsonl) is APPEND-ONLY — the live runner appends
fills concurrently, so closing a basket must never rewrite lines. Instead an
unwind appends {"strategy": "unwind", "status": "unwound", "closes_ts": <ts of
the open record>, "qty": n} and every consumer nets through this helper.
"""

import json
from typing import Iterable


def parse_lines(lines: Iterable[str]) -> list[dict]:
    out = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            out.append(json.loads(line))
        except ValueError:
            continue
    return out


def apply_corrections(records: list[dict]) -> list[dict]:
    """Fold correction records into their targets and drop them.

    A correction is {"status": "correction", "relationship_id": ..,
    "corrects_ts": <ts of the record being corrected>, "fields": {..}} —
    appended when a recorded value proves wrong (e.g. a hedge response lost to
    a 429 recorded avg_price=0). The ledger stays append-only and auditable;
    every consumer sees the corrected view. Targets are matched by
    (relationship_id, ts); `fields` are shallow-merged (a "legs" value
    replaces the whole list). Returns copies; input order preserved.
    """
    fixes: dict[tuple, dict] = {}
    for r in records:
        if r.get("status") == "correction" and r.get("corrects_ts") is not None:
            fixes.setdefault((r.get("relationship_id"), r["corrects_ts"]), {}).update(
                r.get("fields") or {})
    if not fixes:
        return records
    out = []
    for r in records:
        if r.get("status") == "correction":
            continue
        f = fixes.get((r.get("relationship_id"), r.get("ts")))
        if f:
            r = {**r, **f}
        out.append(r)
    return out


def open_baskets(records: list[dict]) -> list[dict]:
    """Open ledger records with qty reduced by matching unwound records.

    Corrections are folded first (see apply_corrections). Match key is
    (relationship_id, closes_ts == open record's ts). A fully unwound basket
    is dropped; a partial unwind returns a COPY of the record with reduced
    qty and proportionally reduced cost/payoff/profit.
    """
    records = apply_corrections(records)
    unwound: dict[tuple, float] = {}
    for r in records:
        if r.get("status") == "unwound" and r.get("closes_ts") is not None:
            k = (r.get("relationship_id"), r["closes_ts"])
            unwound[k] = unwound.get(k, 0) + float(r.get("qty", 0))

    out = []
    for r in records:
        if r.get("status") != "open":
            continue
        closed = unwound.get((r.get("relationship_id"), r.get("ts")), 0)
        qty = float(r.get("qty", 0))
        if closed <= 0:
            out.append(r)
            continue
        remaining = qty - closed
        if remaining <= 0:
            continue
        scaled = dict(r)
        frac = remaining / qty
        scaled["qty"] = remaining if remaining % 1 else int(remaining)
        for f in ("cost_usd", "payoff_usd", "profit_usd"):
            if f in scaled and scaled[f] is not None:
                scaled[f] = float(scaled[f]) * frac
        out.append(scaled)
    return out
