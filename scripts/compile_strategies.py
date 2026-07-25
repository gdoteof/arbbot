#!/usr/bin/env python3
"""Compile the strategy manifest (config/strategies.yaml) into a SCRATCH DB.

Dry-run compiler for strategy contract v2 (docs/strategy-contract.md §2-§4):
validates the manifest, then emits `strategy` + `strategy_claim` rows and
reads the compile report back out of SQLite (the round-trip).

DESIGN-ONLY. The contract's end state compiles into data/exec/trading.db, but
nothing reads these tables yet — so this script REFUSES any --db under a
`data/` directory. The DDL lives here (schema.sql conventions), not in
schema.sql: the manifest is not load-bearing until the P4 engine reads it.

Usage: python scripts/compile_strategies.py [--manifest PATH] [--db PATH]
"""

import argparse
import json
import re
import sqlite3
import sys
import time
from collections import defaultdict
from pathlib import Path

import yaml

# §3 families: a family name means the entry IS the strategy (template + params).
FAMILIES = {"maker-hedge", "take-take-cross", "directional-signal"}
# §4 `family`: the non-family sentinels.
NON_FAMILIES = {"bespoke", "external-intents", "audit-readonly", "manual-tool",
                "retires"}
# §2.1 target_runtime, minus the parameterized rust-template(<family>) form.
SIMPLE_RUNTIMES = {"rust-module", "external-intents", "audit-readonly",
                   "manual-tool", "retires"}
TODAY = {"python-daemon", "python-timer", "python-manual", "none"}
KILL = {"honors", "DOES-NOT-CHECK (gap)", "inherits", "engine", "n/a"}
CLAIM_KINDS = ("rel_ids", "slug_patterns", "registry_query")

ID_RE = re.compile(r"^[a-z0-9]+(-[a-z0-9]+)*$")
RUST_TEMPLATE_RE = re.compile(r"^rust-template\((.+)\)$")

# Legacy ledger-tag collisions the contract documents (§1.1 / §7): (sharer,
# tag-owning strategy). Every other collision is an error — a new strategy may
# not silently write another's tag.
DOCUMENTED_TAG_COLLISIONS = [
    ("naked-hedge", "take-take"),
    ("make-take-manual", "make-take"),
    ("auto-take-take", "take-take"),
    ("sports-rehedge", "sports-take-take"),
    ("flatten-probe-residual", "pmus-maker-probe"),
]

DDL = """
-- ============ strategy manifest: compiled from config/strategies.yaml ============
-- Written ONLY by scripts/compile_strategies.py, which validates the manifest
-- (docs/strategy-contract.md §4) and rewrites both tables wholesale in one
-- transaction. `strategy_claim` is the compile source the inert `ownership`
-- table becomes: one row per claim pattern, and the engine rejects intents
-- outside the compiled set (§6a).

CREATE TABLE IF NOT EXISTS strategy (
  id             TEXT PRIMARY KEY,
  kind           TEXT NOT NULL,     -- maker|taker|directional|lifecycle|manual|audit
  family         TEXT NOT NULL,     -- §3 family name, or bespoke|external-intents|...
  target_runtime TEXT NOT NULL,     -- §2.1 enum, incl. rust-template(<family>)
  today          TEXT NOT NULL,     -- python-daemon|python-timer|python-manual|none
  source         TEXT,              -- dual_append source value (legacy convention)
  intents_stream TEXT,
  kill           TEXT NOT NULL,
  ledger_tags    TEXT NOT NULL,     -- JSON array
  close_kinds    TEXT NOT NULL,     -- JSON array (CLOSE_KINDS values)
  state_files    TEXT NOT NULL,     -- JSON array; exclusive per strategy
  params_json    TEXT,              -- family entries only: the template config
  raw_json       TEXT NOT NULL,     -- the manifest entry verbatim
  compiled_ns    INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS strategy_claim (
  strategy_id TEXT NOT NULL REFERENCES strategy(id),
  claim_index INTEGER NOT NULL,
  venue       TEXT CHECK (venue IS NULL OR venue IN
                ('kalshi','polymarket','polymarket_us')),
  kind        TEXT NOT NULL CHECK (kind IN
                ('rel_ids','slug_patterns','registry_query')),
  pattern     TEXT NOT NULL,
  PRIMARY KEY (strategy_id, claim_index)
);
CREATE INDEX IF NOT EXISTS ix_strategy_claim_pattern
  ON strategy_claim(kind, pattern);
"""


def tag_allowlist() -> dict[str, set[str]]:
    """ledger tag -> the strategy ids permitted to write it."""
    allowed: dict[str, set[str]] = {}
    for sharer, owner in DOCUMENTED_TAG_COLLISIONS:
        allowed.setdefault(owner, {owner}).add(sharer)
    return allowed


def load_manifest(path: Path) -> list[dict]:
    raw = yaml.safe_load(path.read_text()) or {}
    return list(raw.get("strategies") or [])


def validate(entries: list[dict]) -> list[str]:
    """All validation errors (not just the first) against §2-§4."""
    errors: list[str] = []
    seen_ids: set[str] = set()
    owners: dict[str, list[str]] = defaultdict(list)   # state_file -> ids
    taggers: dict[str, list[str]] = defaultdict(list)  # ledger_tag -> ids

    for i, e in enumerate(entries):
        if not isinstance(e, dict):
            errors.append(f"entry {i}: not a mapping")
            continue
        sid = e.get("id")
        where = sid if isinstance(sid, str) else f"entry {i}"
        if not isinstance(sid, str) or not ID_RE.match(sid):
            errors.append(f"{where}: id must be kebab-case")
        elif sid in seen_ids:
            errors.append(f"{sid}: duplicate id")
        else:
            seen_ids.add(sid)

        family = e.get("family")
        if family not in FAMILIES | NON_FAMILIES:
            errors.append(f"{where}: unknown family {family!r}")

        runtime = e.get("target_runtime")
        m = RUST_TEMPLATE_RE.match(runtime) if isinstance(runtime, str) else None
        if m:
            if m.group(1) not in FAMILIES:
                errors.append(f"{where}: rust-template names unknown family "
                              f"{m.group(1)!r}")
            elif m.group(1) != family:
                errors.append(f"{where}: target_runtime {runtime} disagrees with "
                              f"family {family!r}")
        elif runtime not in SIMPLE_RUNTIMES:
            errors.append(f"{where}: unknown target_runtime {runtime!r}")

        if e.get("today") not in TODAY:
            errors.append(f"{where}: unknown today {e.get('today')!r}")
        if e.get("kill") not in KILL:
            errors.append(f"{where}: unknown kill {e.get('kill')!r}")

        if family in FAMILIES and not e.get("params"):
            errors.append(f"{where}: family {family} entry has no params")

        for f in e.get("state_files") or []:
            owners[f].append(where)
        for t in e.get("ledger_tags") or []:
            taggers[t].append(where)

    for f, ids in sorted(owners.items()):
        if len(ids) > 1:
            errors.append(f"state_file {f} owned by {len(ids)} entries: "
                          f"{', '.join(ids)}")

    allowed = tag_allowlist()
    for t, ids in sorted(taggers.items()):
        if len(ids) > 1 and not set(ids) <= allowed.get(t, set()):
            errors.append(f"ledger_tag {t} written by {', '.join(ids)} — "
                          f"undocumented collision")

    return errors


def resolve_db(spec: str) -> str:
    """Scratch-only: anything under a data/ directory is refused."""
    if spec == ":memory:":
        return spec
    path = Path(spec).expanduser().resolve()
    if "data" in path.parts:
        raise ValueError(f"refusing to compile into {path}: design-only, no "
                         f"writes under data/")
    return str(path)


def claim_rows(entry: dict) -> list[tuple]:
    """One row per claim pattern, tagged with its kind."""
    rows = []
    for claim in entry.get("claims") or []:
        venue = claim.get("venue")
        for kind in CLAIM_KINDS:
            value = claim.get(kind)
            if value is None:
                continue
            patterns = value if isinstance(value, list) else [value]
            for p in patterns:
                rows.append((entry["id"], len(rows), venue, kind, str(p)))
    return rows


def compile_strategies(conn: sqlite3.Connection, entries: list[dict]) -> None:
    conn.executescript(DDL)
    now_ns = time.time_ns()
    try:
        conn.execute("DELETE FROM strategy_claim")
        conn.execute("DELETE FROM strategy")
        for e in entries:
            conn.execute(
                "INSERT INTO strategy (id, kind, family, target_runtime, today,"
                " source, intents_stream, kill, ledger_tags, close_kinds,"
                " state_files, params_json, raw_json, compiled_ns)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (e["id"], e["kind"], e["family"], e["target_runtime"], e["today"],
                 e.get("source"), e.get("intents_stream"), e["kill"],
                 json.dumps(e.get("ledger_tags") or []),
                 json.dumps(e.get("close_kinds") or []),
                 json.dumps(e.get("state_files") or []),
                 json.dumps(e["params"]) if e.get("params") else None,
                 json.dumps(e, sort_keys=True, default=str), now_ns))
            conn.executemany(
                "INSERT INTO strategy_claim (strategy_id, claim_index, venue,"
                " kind, pattern) VALUES (?,?,?,?,?)", claim_rows(e))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise


def report(conn: sqlite3.Connection) -> list[str]:
    """Compile report, read back OUT of the scratch DB — the round-trip."""
    q = lambda sql: list(conn.execute(sql))  # noqa: E731
    n = q("SELECT COUNT(*) FROM strategy")[0][0]
    n_claims = q("SELECT COUNT(*) FROM strategy_claim")[0][0]
    lines = [f"compiled {n} strategies, {n_claims} claim patterns"]

    for col in ("family", "target_runtime"):
        lines.append(f"  by {col}:")
        for name, cnt in q(f"SELECT {col}, COUNT(*) FROM strategy GROUP BY 1"
                           f" ORDER BY 2 DESC, 1"):
            lines.append(f"    {name:32s} {cnt}")

    lines.append("  documented ledger_tag collisions:")
    for tag, ids in q("SELECT t.value, GROUP_CONCAT(s.id, ', ')"
                      " FROM strategy s, json_each(s.ledger_tags) t"
                      " GROUP BY 1 HAVING COUNT(*) > 1 ORDER BY 1"):
        lines.append(f"    {tag:32s} {ids}")

    gaps = q("SELECT id FROM strategy WHERE kill LIKE 'DOES-NOT-CHECK%'"
             " ORDER BY id")
    lines.append(f"  kill gaps (DOES-NOT-CHECK) — {len(gaps)}:")
    lines += [f"    {r[0]}" for r in gaps]
    return lines


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--manifest", default="config/strategies.yaml")
    ap.add_argument("--db", default=":memory:", help="scratch DB; not under data/")
    args = ap.parse_args()

    try:
        db = resolve_db(args.db)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2

    entries = load_manifest(Path(args.manifest))
    errors = validate(entries)
    if errors:
        print(f"{args.manifest}: {len(errors)} validation error(s)",
              file=sys.stderr)
        for e in errors:
            print(f"  {e}", file=sys.stderr)
        return 1

    conn = sqlite3.connect(db)
    try:
        compile_strategies(conn, entries)
        print(f"{args.manifest} -> {db}")
        print("\n".join(report(conn)))
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
