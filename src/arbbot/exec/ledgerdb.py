"""SQLite trading-state store (data/exec/trading.db) — schema + writers.

System-of-record-in-waiting for the trade ledger (see plan: strangler P0).
During the dual-write phase trades.jsonl remains SoR: every writer calls
dual_append(), which appends the JSONL line byte-identically to the old
hand-rolled writers and then best-effort mirrors the record into SQLite.

Idempotency keys are content hashes of the raw record (record_key), so the
live dual-write path and the importer/backfill (scripts/
import_ledger_to_sqlite.py) produce the SAME key for the same record — a
missed mirror self-heals on the next backfill run, and the parity check
(v_open_baskets == ledger.open_baskets) guards the whole arrangement.

Unit conventions (see schema.sql): money INTEGER micro-USD, prices INTEGER
micro-probability, timestamps INTEGER ns. Conversions go through Decimal so
venue decimal-strings like "0.0280" land exactly.
"""

import hashlib
import json
import sqlite3
import time
from decimal import Decimal
from importlib import resources
from pathlib import Path

DB_PATH = Path("data/exec/trading.db")
LEDGER_PATH = Path("data/exec/trades.jsonl")

MICRO = Decimal(1_000_000)

CLOSE_KINDS = ("unwind", "settlement", "naked-settlement", "flatten", "expiry")


def usd_u(v) -> int | None:
    """Dollars (float/str/Decimal) -> micro-USD int."""
    if v is None:
        return None
    return int((Decimal(str(v)) * MICRO).to_integral_value())


def pu(v) -> int | None:
    """Probability (str/float/Decimal in [0,1]) -> micro-probability int."""
    if v is None:
        return None
    return int((Decimal(str(v)) * MICRO).to_integral_value())


def ns(ts: float | None) -> int | None:
    """Legacy float-seconds ts -> ns int."""
    if ts is None:
        return None
    return int(Decimal(str(ts)) * 1_000_000_000)


def _int_qty(v) -> int:
    """Ledger qty are contracts; some writers emit them as floats (5.0)."""
    iv = round(float(v))
    if abs(iv - float(v)) > 1e-6:
        raise ValueError(f"non-integral qty {v!r}")
    return iv


def record_key(rec: dict) -> str:
    """Content-hash idempotency key — identical for dual-write and backfill."""
    canon = json.dumps(rec, separators=(",", ":"), sort_keys=True)
    return "rec:" + hashlib.sha1(canon.encode()).hexdigest()[:16]


def connect(path: Path | str = DB_PATH, *, readonly: bool = False) -> sqlite3.Connection:
    """Open the trading DB with the standard pragmas (and apply the schema
    when writable). Actor attribution happens per-operation: writers pass
    `source`/`actor` to the record/correction functions."""
    if readonly:
        conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5.0)
    else:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(path, timeout=5.0)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    if not readonly:
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = FULL")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript(resources.files("arbbot.exec").joinpath("schema.sql").read_text())
        conn.commit()
    return conn


def _basket_cols(rec: dict) -> dict:
    return {
        "ts_ns": ns(rec["ts"]), "legacy_ts": rec["ts"],
        "relationship_id": rec.get("relationship_id"),
        "strategy": rec.get("strategy"), "title": rec.get("title"),
        "qty": _int_qty(rec["qty"]),
        "cost_usd_u": usd_u(rec.get("cost_usd")),
        "payoff_usd_u": usd_u(rec.get("payoff_usd")),
        "profit_usd_u": usd_u(rec.get("profit_usd")),
        "resolves_by": rec.get("resolves_by"),
        "resolves_estimated": 1 if rec.get("resolves_estimated") else 0,
        "hedge_latency_ms": rec.get("hedge_latency_ms"),
        "note": rec.get("note"),
        "voided": 0 if rec.get("status") == "open" else 1,
        "raw_json": json.dumps(rec, separators=(",", ":"), sort_keys=True),
    }


def _leg_row(leg: dict) -> dict:
    """Normalize one drifted-JSONL leg shape to leg-table columns.

    Maker legs carry yes_price+cost; taker legs avg_price+fees; unwind legs
    action+yes_price. price_pu is always the yes-equivalent price the record
    carried; missing economics stay NULL (probe legs exist without prices).
    """
    price = leg.get("yes_price") if leg.get("yes_price") is not None else leg.get("avg_price")
    return {
        "venue": leg["venue"],
        "market_id": leg["market_id"],
        "side": leg.get("side"),
        "role": leg.get("role") or leg.get("action"),
        "qty": _int_qty(leg.get("qty") or 0),
        "price_pu": pu(price),
        "cost_usd_u": usd_u(leg.get("cost")),
        "fees_usd_u": usd_u(leg.get("fees")) or 0,
        "venue_order_id": leg.get("order_id"),
        "price_estimated": 1 if leg.get("price_estimated") else 0,
        "raw_json": json.dumps(leg, separators=(",", ":"), sort_keys=True),
    }


def _insert_legs(conn: sqlite3.Connection, basket_id: int, legs: list[dict]) -> None:
    for i, leg in enumerate(legs or []):
        row = _leg_row(leg)
        conn.execute(
            """INSERT INTO leg (basket_id, leg_index, venue, market_id, side, role,
                 qty, price_pu, cost_usd_u, fees_usd_u, venue_order_id,
                 price_estimated, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (basket_id, i, row["venue"], row["market_id"], row["side"], row["role"],
             row["qty"], row["price_pu"], row["cost_usd_u"], row["fees_usd_u"],
             row["venue_order_id"], row["price_estimated"], row["raw_json"]))


def record_basket(conn: sqlite3.Connection, rec: dict, *, idempotency_key: str,
                  source: str) -> int | None:
    """Insert an open-basket ledger record (status=='open' JSONL shape).

    Returns basket_id, or None if idempotency_key already exists (no-op).
    """
    c = _basket_cols(rec)
    with conn:
        cur = conn.execute(
            """INSERT INTO basket (idempotency_key, ts_ns, legacy_ts, relationship_id,
                 strategy, title, qty, cost_usd_u, payoff_usd_u, profit_usd_u,
                 resolves_by, resolves_estimated, hedge_latency_ms, note, voided,
                 source, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(idempotency_key) DO NOTHING""",
            (idempotency_key, c["ts_ns"], c["legacy_ts"], c["relationship_id"],
             c["strategy"], c["title"], c["qty"], c["cost_usd_u"], c["payoff_usd_u"],
             c["profit_usd_u"], c["resolves_by"], c["resolves_estimated"],
             c["hedge_latency_ms"], c["note"], c["voided"], source, c["raw_json"]))
        if cur.rowcount == 0:
            return None
        basket_id = cur.lastrowid
        _insert_legs(conn, basket_id, rec.get("legs"))
    return basket_id


def record_close(conn: sqlite3.Connection, rec: dict, *, idempotency_key: str,
                 source: str, basket_id: int | None = None) -> int | None:
    """Insert a close/realized ledger record (status 'unwound' or 'realized').

    basket_id: pass the target basket when known; when None and the record
    carries closes_ts, the (relationship_id, closes_ts==legacy_ts) match is
    attempted here — unmatched closes insert with basket_id NULL (visible
    orphan, never dropped). Returns close_id or None on idempotent no-op.
    """
    if basket_id is None and rec.get("closes_ts") is not None:
        row = conn.execute(
            "SELECT basket_id FROM basket WHERE relationship_id IS ? AND legacy_ts = ?",
            (rec.get("relationship_id"), rec["closes_ts"])).fetchone()
        basket_id = row["basket_id"] if row else None
    strategy = rec.get("strategy")
    kind = strategy if strategy in CLOSE_KINDS else (
        "settlement" if rec.get("kalshi_result") is not None else "unwind")
    with conn:
        cur = conn.execute(
            """INSERT INTO basket_close (idempotency_key, basket_id, ts_ns, legacy_ts,
                 kind, qty, realized_pnl_usd_u, proceeds_usd_u, fees_usd_u, note,
                 source, raw_json)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
               ON CONFLICT(idempotency_key) DO NOTHING""",
            (idempotency_key, basket_id, ns(rec["ts"]), rec["ts"], kind,
             _int_qty(rec["qty"]), usd_u(rec.get("realized_pnl_usd")),
             usd_u(rec.get("proceeds_usd")), usd_u(rec.get("fees_usd")) or 0,
             rec.get("note"), source,
             json.dumps(rec, separators=(",", ":"), sort_keys=True)))
        return cur.lastrowid if cur.rowcount else None


def apply_correction(conn: sqlite3.Connection, rec: dict, *, actor: str) -> bool:
    """Apply a status=='correction' record as an audited UPDATE.

    The target basket is matched by (relationship_id, corrects_ts); its
    corrected view is rebuilt by merging `fields` into the stored raw_json
    (exactly ledger.apply_corrections semantics — a "legs" value replaces
    the whole list, a non-'open' "status" voids the basket). No-ops when the
    stored values already reflect the correction (backfill idempotency).
    The update triggers capture before-images; this function then annotates
    those audit rows with actor + the correction's reason, all in one txn.
    Returns False when no target exists — the caller should warn; the
    nightly backfill heals that case.
    """
    row = conn.execute(
        "SELECT basket_id, raw_json FROM basket"
        " WHERE relationship_id IS ? AND legacy_ts = ?",
        (rec.get("relationship_id"), rec.get("corrects_ts"))).fetchone()
    if row is None:
        return False
    current = json.loads(row["raw_json"])
    corrected = {**current, **(rec.get("fields") or {})}
    if corrected == current:
        return True
    c = _basket_cols(corrected)
    bid = row["basket_id"]
    with conn:
        conn.execute(
            """UPDATE basket SET qty=?, cost_usd_u=?, payoff_usd_u=?, profit_usd_u=?,
                 strategy=?, title=?, resolves_by=?, resolves_estimated=?,
                 hedge_latency_ms=?, note=?, voided=?, raw_json=?
               WHERE basket_id=?""",
            (c["qty"], c["cost_usd_u"], c["payoff_usd_u"], c["profit_usd_u"],
             c["strategy"], c["title"], c["resolves_by"], c["resolves_estimated"],
             c["hedge_latency_ms"], c["note"], c["voided"], c["raw_json"], bid))
        if "legs" in (rec.get("fields") or {}):
            conn.execute("DELETE FROM leg WHERE basket_id=?", (bid,))
            _insert_legs(conn, bid, corrected.get("legs"))
        conn.execute(
            "UPDATE audit_log SET actor=?, reason=? WHERE actor IS NULL"
            " AND (pk = ? OR pk LIKE ?)",
            (actor, rec.get("reason"), str(bid), f"{bid}:%"))
    return True


def open_baskets_db(conn: sqlite3.Connection) -> list[dict]:
    """DB read path for ledger.open_baskets(parse_lines(...)) — identical
    dict shapes. Each non-voided basket is rebuilt from its (corrected)
    raw_json; qty is reduced by SUM(basket_close.qty) with cost/payoff/profit
    scaled proportionally, mirroring exec/ledger.py (the frozen reference)
    arithmetic exactly. basket_id order == append order == JSONL file order."""
    rows = conn.execute(
        """SELECT b.raw_json, IFNULL(c.closed, 0) AS closed
           FROM basket b
           LEFT JOIN (SELECT basket_id, SUM(qty) AS closed FROM basket_close
                      WHERE basket_id IS NOT NULL GROUP BY basket_id) c
             USING (basket_id)
           WHERE b.voided = 0 ORDER BY b.basket_id""").fetchall()
    out = []
    for row in rows:
        r = json.loads(row["raw_json"])
        closed = float(row["closed"])
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


def all_records_db(conn: sqlite3.Connection) -> list[dict]:
    """Corrected view of every ledger record, in ledger (ts) order — the DB
    read path for apply_corrections(parse_lines(...)). Basket rows carry the
    corrected raw_json (a voided original keeps its rewritten status, e.g.
    'superseded'); close rows are the unwound/realized records. Correction
    records themselves are folded away, exactly as the JSONL path drops them.
    (Import-built DBs omit voided originals by design — see the importer.)"""
    rows = [(r["legacy_ts"], r["raw_json"]) for r in conn.execute(
        "SELECT legacy_ts, raw_json FROM basket")]
    rows += [(r["legacy_ts"], r["raw_json"]) for r in conn.execute(
        "SELECT legacy_ts, raw_json FROM basket_close")]
    rows.sort(key=lambda t: (t[0] is None, t[0]))
    return [json.loads(raw) for _, raw in rows]


def claim_ownership(conn: sqlite3.Connection, relationship_id: str, owner: str,
                    note: str | None = None) -> None:
    """Register `owner` as the order-placing process for a relationship/market
    (upsert — last claim wins; see the ownership table in schema.sql)."""
    with conn:
        conn.execute(
            """INSERT INTO ownership (relationship_id, owner, updated_ns, note)
               VALUES (?,?,?,?)
               ON CONFLICT(relationship_id) DO UPDATE SET owner=excluded.owner,
                 updated_ns=excluded.updated_ns, note=excluded.note""",
            (relationship_id, owner, time.time_ns(), note))


def owned_by(conn: sqlite3.Connection, owner_prefix: str) -> list[str]:
    """Relationship/market ids whose registered owner starts with the prefix
    (e.g. 'probe-' -> everything the research probes have claimed)."""
    return [r["relationship_id"] for r in conn.execute(
        "SELECT relationship_id FROM ownership WHERE owner LIKE ?",
        (owner_prefix + "%",))]


def mirror_record(conn: sqlite3.Connection, rec: dict, *, source: str) -> None:
    """Route one raw ledger record to the right table (idempotent)."""
    status = rec.get("status")
    if status == "open":
        record_basket(conn, rec, idempotency_key=record_key(rec), source=source)
    elif status in ("unwound", "realized"):
        record_close(conn, rec, idempotency_key=record_key(rec), source=source)
    elif status == "correction":
        if not apply_correction(conn, rec, actor=source):
            print(f"[ledgerdb] WARNING correction target not in DB "
                  f"({rec.get('relationship_id')}, {rec.get('corrects_ts')}) "
                  f"— nightly backfill will heal", flush=True)


def dual_append(rec: dict, *, source: str, ledger_path: Path = LEDGER_PATH,
                db_path: Path = DB_PATH) -> None:
    """Append `rec` to trades.jsonl (SoR, byte-identical to the old writers)
    and best-effort mirror it into the trading DB. DB failure never raises —
    trading must not die because the DB is locked; the backfill heals."""
    ledger_path.parent.mkdir(parents=True, exist_ok=True)
    with open(ledger_path, "a") as f:
        f.write(json.dumps(rec) + "\n")
    try:
        conn = connect(db_path)
        try:
            mirror_record(conn, rec, source=source)
        finally:
            conn.close()
    except Exception as e:  # noqa: BLE001 — deliberate: JSONL is SoR
        print(f"[ledgerdb] WARNING dual-write mirror failed "
              f"({type(e).__name__}: {e}) — nightly backfill will heal", flush=True)
