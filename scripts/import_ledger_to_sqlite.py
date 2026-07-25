#!/usr/bin/env python3
"""One-shot (re-runnable) importer: trades.jsonl -> data/exec/trading.db.

Reference semantics come from exec/ledger.py verbatim: corrections are folded
first (each fold also lands in audit_log with the pre-correction before-image),
open records become basket+leg rows, unwound/realized records become
basket_close rows matched by (relationship_id, closes_ts == legacy_ts).
Unmatched closes import with basket_id NULL and are REPORTED, never dropped.
Records whose corrections rewrite status away from 'open' (e.g. the live
file's `superseded` voids) are — by reference semantics — not baskets and do
not import; the JSONL remains the archive of voided originals.

Idempotency keys are content hashes of the raw record (ledgerdb.record_key) —
the SAME keys the live dual-write path mints — so this script doubles as the
nightly backfill/heal job: records whose live mirror failed import here, and
re-running over the append-only file is a no-op for already-present lines.
A correction pass re-applies every correction idempotently, healing any
correction whose live UPDATE was missed.

--check runs the parity gate: v_open_baskets must reproduce
ledger.open_baskets() exactly (same basket set and qty; money within 1 µUSD).

Usage:
  python scripts/import_ledger_to_sqlite.py --ledger data/exec/trades.jsonl \
      --db data/exec/trading.db --check
"""

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from arbbot.exec import ledgerdb
from arbbot.exec.ledger import apply_corrections, open_baskets, parse_lines


def import_ledger(ledger_path: Path, db_path: Path) -> dict:
    raw: list[dict] = []
    for ln in ledger_path.open():
        ln = ln.strip()
        if not ln:
            continue
        try:
            raw.append(json.loads(ln))
        except ValueError:
            continue

    corrected = apply_corrections(raw)
    # apply_corrections drops correction rows; re-align the raw records with
    # the surviving corrected ones — idempotency keys hash the RAW record,
    # matching what the live dual-write path minted at append time.
    survivors = [r for r in raw if r.get("status") != "correction"]
    assert len(survivors) == len(corrected)

    # before-images for audit: old values of corrected fields, keyed by target
    fixes: dict[tuple, list[dict]] = {}
    by_key = {(r.get("relationship_id"), r.get("ts")): r for r in raw}
    for r in raw:
        if r.get("status") == "correction" and r.get("corrects_ts") is not None:
            target = by_key.get((r.get("relationship_id"), r["corrects_ts"]))
            if target is not None:
                before = {k: target.get(k) for k in (r.get("fields") or {})}
                fixes.setdefault((r.get("relationship_id"), r["corrects_ts"]), []).append(
                    {"reason": r.get("reason"), "before": before})

    corrected_status = {(r.get("relationship_id"), r.get("ts")): r.get("status")
                        for r in corrected}
    stats = {"baskets": 0, "closes": 0, "orphan_closes": [], "audits": 0,
             "skipped": 0, "unhealed": []}
    conn = ledgerdb.connect(db_path)
    try:
        # pass 1: baskets (corrected values, raw-record keys)
        for raw_rec, rec in zip(survivors, corrected):
            if rec.get("status") != "open":
                continue
            bid = ledgerdb.record_basket(
                conn, rec, idempotency_key=ledgerdb.record_key(raw_rec), source="import")
            if bid is None:
                stats["skipped"] += 1
                continue
            stats["baskets"] += 1
            for fix in fixes.get((rec.get("relationship_id"), rec.get("ts")), []):
                conn.execute(
                    "INSERT INTO audit_log (ts_ns, tbl, pk, actor, reason, before_json)"
                    " VALUES (?,?,?,?,?,?)",
                    (time.time_ns(), "basket", str(bid), "import", fix["reason"],
                     json.dumps(fix["before"], separators=(",", ":"), sort_keys=True)))
                stats["audits"] += 1
            conn.commit()
        # pass 2: closes (unwound + standalone realized)
        for raw_rec, rec in zip(survivors, corrected):
            if rec.get("status") not in ("unwound", "realized"):
                continue
            cid = ledgerdb.record_close(
                conn, rec, idempotency_key=ledgerdb.record_key(raw_rec), source="import")
            if cid is None:
                stats["skipped"] += 1
                continue
            stats["closes"] += 1
            row = conn.execute("SELECT basket_id FROM basket_close WHERE close_id = ?",
                               (cid,)).fetchone()
            if row["basket_id"] is None and rec.get("closes_ts") is not None:
                stats["orphan_closes"].append(
                    (rec.get("relationship_id"), rec.get("closes_ts")))
        # pass 3: heal — re-apply corrections whose live UPDATE was missed
        # (no-op when the stored raw_json already reflects the correction).
        # Corrections whose target is not an open basket in the corrected view
        # (voided records, close-record fixes) have no basket row by design.
        for r in raw:
            if r.get("status") != "correction" or r.get("corrects_ts") is None:
                continue
            target_key = (r.get("relationship_id"), r["corrects_ts"])
            if corrected_status.get(target_key) != "open":
                continue
            if not ledgerdb.apply_correction(conn, r, actor="import"):
                stats["unhealed"].append(target_key)
    finally:
        conn.close()
    return stats


def check_parity(ledger_path: Path, db_path: Path) -> list[str]:
    """Compare v_open_baskets against ledger.open_baskets(). Returns problems."""
    expected = open_baskets(parse_lines(ledger_path.open()))
    exp = {(r.get("relationship_id"), r.get("ts")): r for r in expected}

    conn = ledgerdb.connect(db_path, readonly=True)
    try:
        rows = conn.execute(
            "SELECT relationship_id, legacy_ts, open_qty, open_cost_usd_u,"
            " open_payoff_usd_u, open_profit_usd_u FROM v_open_baskets").fetchall()
    finally:
        conn.close()
    got = {(r["relationship_id"], r["legacy_ts"]): r for r in rows}

    problems = []
    for k in exp.keys() - got.keys():
        problems.append(f"missing from DB: {k}")
    for k in got.keys() - exp.keys():
        problems.append(f"unexpected in DB: {k}")
    for k in exp.keys() & got.keys():
        e, g = exp[k], got[k]
        if float(e.get("qty", 0)) != float(g["open_qty"]):
            problems.append(f"{k}: qty {e.get('qty')} != {g['open_qty']}")
        for jf, dbf in (("cost_usd", "open_cost_usd_u"), ("payoff_usd", "open_payoff_usd_u"),
                        ("profit_usd", "open_profit_usd_u")):
            ev = e.get(jf)
            if ev is None:
                continue
            if abs(round(float(ev) * 1_000_000) - (g[dbf] or 0)) > 1:
                problems.append(f"{k}: {jf} {ev} != {g[dbf]}µ$")
    return problems


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ledger", type=Path, default=Path("data/exec/trades.jsonl"))
    ap.add_argument("--db", type=Path, default=ledgerdb.DB_PATH)
    ap.add_argument("--check", action="store_true", help="run parity gate after import")
    args = ap.parse_args()

    stats = import_ledger(args.ledger, args.db)
    print(f"imported: {stats['baskets']} baskets, {stats['closes']} closes, "
          f"{stats['audits']} correction audits, {stats['skipped']} already-present")
    for orphan in stats["orphan_closes"]:
        print(f"WARNING orphan close (no matching basket): {orphan}")
    for key in stats["unhealed"]:
        print(f"WARNING correction target missing from DB: {key}")

    if args.check:
        problems = check_parity(args.ledger, args.db)
        if problems:
            for p in problems:
                print(f"PARITY FAIL {p}")
            return 1
        print("parity OK: v_open_baskets == ledger.open_baskets()")
    return 0


if __name__ == "__main__":
    sys.exit(main())
