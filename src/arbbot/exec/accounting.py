"""One pure P&L fold over ledger events (card 988c18d8).

Everything a basket will ever pay is fixed at open (locked_at_open = the
riskless lock). Later events — unwind, settlement, flatten — only CONVERT
unrealized into realized. The fold produces one row per basket and enforces
the identity, exactly, in Decimal:

    locked_at_open == realized + remaining_locked + slippage

where slippage := (closed_frac * locked_at_open) - realized. Negative
slippage means the close beat the lock (typical of early unwinds). Standalone
realized events (naked settlements, flattens) fold as their own rows with
locked_at_open = 0, so the identity covers them too.

Marks enter as events keyed (relationship_id, ts) — never a render-time
sidecar join — and set unrealized_mark on the open remainder.

Floats exist only at render time (see row_floats). The digest pins the
per-basket rows byte-for-byte: the third parity gate alongside the golden
book digest and the intent digest, and the Rust kernel's target.
"""

import hashlib
import json
from decimal import Decimal
from typing import Optional

from arbbot.exec.ledger import apply_corrections

CENT4 = Decimal("0.0001")


def _d(v, default: str = "0") -> Decimal:
    if v is None:
        return Decimal(default)
    return Decimal(str(v)).quantize(CENT4)


def fold(records: list[dict], marks: Optional[dict] = None) -> dict:
    """Fold ledger records (+ optional marks.json doc) into per-basket rows.

    Returns {"rows": [row], "totals": {..}, "digest": str} with Decimal
    values inside rows/totals. Rows are sorted by (relationship_id, ts).
    """
    records = apply_corrections(records)
    mark_by_key = {}
    for m in (marks or {}).get("positions", []):
        mark_by_key[(m.get("relationship_id"), m.get("ts"))] = m

    rows_by_key: dict[tuple, dict] = {}

    def _new_row(rel, ts, r):
        return {
            "relationship_id": rel, "ts": ts,
            "title": r.get("title"), "strategy": r.get("strategy"),
            "qty": _d(r.get("qty")),
            "cost": Decimal(0), "payoff": Decimal(0),
            "locked_at_open": Decimal(0),
            "closed_qty": Decimal(0), "proceeds": Decimal(0),
            "realized": Decimal(0),
            "unrealized_mark": None,
            "flags": [],
        }

    # pass 1: opens and standalone realized events establish rows
    for r in records:
        key = (r.get("relationship_id"), r.get("ts"))
        if r.get("status") == "open":
            row = rows_by_key.setdefault(key, _new_row(*key, r))
            row["cost"] = _d(r.get("cost_usd"))
            row["payoff"] = _d(r.get("payoff_usd"))
            row["locked_at_open"] = (_d(r.get("profit_usd"))
                                     if r.get("profit_usd") is not None
                                     else row["payoff"] - row["cost"])
        elif r.get("status") == "realized":
            row = rows_by_key.setdefault(key, _new_row(*key, r))
            row["closed_qty"] = row["qty"]
            row["realized"] += _d(r.get("realized_pnl_usd"))
            row["flags"].append("standalone_realized")

    # pass 2: closing events convert unrealized -> realized on their basket
    for r in records:
        if r.get("status") != "unwound":
            continue
        key = (r.get("relationship_id"), r.get("closes_ts"))
        row = rows_by_key.get(key)
        if row is None:  # closing event with no visible open — surface, keep
            row = rows_by_key.setdefault(
                (r.get("relationship_id"), r.get("ts")), _new_row(
                    r.get("relationship_id"), r.get("ts"), r))
            row["qty"] = Decimal(0)
            row["flags"].append("orphan_close")
        row["closed_qty"] += _d(r.get("qty"))
        row["proceeds"] += _d(r.get("proceeds_usd"))
        row["realized"] += _d(r.get("realized_pnl_usd"))

    # derive: remaining, expected, slippage, identity — exact by construction,
    # so the INVARIANT TESTS are about flags, monotonicity and rollups
    for key, row in rows_by_key.items():
        if row["closed_qty"] > row["qty"]:
            row["flags"].append("over_closed")
        qty = row["qty"]
        closed_frac = (row["closed_qty"] / qty) if qty else Decimal(1)
        remaining_frac = max(Decimal(0), Decimal(1) - closed_frac)
        row["remaining_qty"] = (qty - row["closed_qty"]) if qty else Decimal(0)
        row["remaining_locked"] = (row["locked_at_open"] * remaining_frac
                                   ).quantize(CENT4)
        expected = row["locked_at_open"] - row["remaining_locked"]
        row["slippage"] = expected - row["realized"]
        m = mark_by_key.get(key)
        if m is not None and row["remaining_qty"] > 0:
            row["unrealized_mark"] = _d(m.get("mark_pnl_usd"))

    rows = sorted(rows_by_key.values(),
                  key=lambda r: (r["relationship_id"] or "", r["ts"] or 0))
    totals = {
        "locked_at_open": sum((r["locked_at_open"] for r in rows), Decimal(0)),
        "realized": sum((r["realized"] for r in rows), Decimal(0)),
        "remaining_locked": sum((r["remaining_locked"] for r in rows), Decimal(0)),
        "slippage": sum((r["slippage"] for r in rows), Decimal(0)),
        "unrealized_mark": sum((r["unrealized_mark"] for r in rows
                                if r["unrealized_mark"] is not None), Decimal(0)),
        "open_baskets": sum(1 for r in rows if r["remaining_qty"] > 0),
        "rows": len(rows),
    }
    return {"rows": rows, "totals": totals, "digest": digest(rows)}


def identity_error(row: dict) -> Decimal:
    """locked_at_open - (realized + remaining_locked + slippage); 0 when the
    identity holds. Kept as a function so tests and the parity gate share one
    definition."""
    return (row["locked_at_open"]
            - (row["realized"] + row["remaining_locked"] + row["slippage"]))


def digest(rows: list[dict]) -> str:
    """sha256 over the canonical per-basket rows — the Rust kernel's
    byte-for-byte target."""
    canon = [
        {k: (str(v) if isinstance(v, Decimal) else v)
         for k, v in sorted(row.items())}
        for row in rows
    ]
    return hashlib.sha256(
        json.dumps(canon, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def row_floats(row: dict) -> dict:
    """Render-time view of a row: Decimals to floats, for JSON payloads."""
    return {k: (float(v) if isinstance(v, Decimal) else v)
            for k, v in row.items()}
