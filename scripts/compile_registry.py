#!/usr/bin/env python3
"""Compile the YAML relationship registries into the trading DB.

Sources, in precedence order (first occurrence of an id wins):
  1. config/registry.yaml           hand-vetted universe
  2. config/registry-rejected.yaml  if present; stub entries (id/why only)
                                    aren't Relationship-shaped and are skipped
  3. data/registry/*.yaml           generated registries (e.g. sports-<date>.yaml
                                    from sports_equiv_discovery.py), sorted

Every entry is validated through the pydantic model; category/topic are taken
from the YAML when present, else derived via arbbot.registry.categorize.
Both `registry` and `registry_leg` are rewritten wholesale in ONE transaction
(idempotent — rerun any time), then `v_tradable_relationships` is the gate
downstream readers use.

Usage: PYTHONPATH=src python scripts/compile_registry.py [--registry PATH] [--db PATH]
"""

import argparse
import subprocess
import time
from collections import Counter
from pathlib import Path

import yaml

from arbbot.exec import ledgerdb
from arbbot.registry.categorize import (
    category_of,
    load_game_league,
    load_topic_families,
    topic_of,
)
from arbbot.registry.model import Registry, Relationship


def git_hash(path: Path) -> str | None:
    """Content (blob) hash of the source file — works for untracked files."""
    try:
        out = subprocess.run(["git", "hash-object", str(path)],
                             capture_output=True, text=True, timeout=10)
        return out.stdout.strip() or None
    except (OSError, subprocess.SubprocessError):
        return None


def load_sources(registry_path: Path, rejected_path: Path,
                 generated_dir: Path) -> tuple[list[tuple[Relationship, Path]], int]:
    """(relationship, source_file) pairs, deduped by id; + n stub-skipped."""
    out: list[tuple[Relationship, Path]] = []
    seen: set[str] = set()
    skipped = 0

    def add(rels: list[Relationship], src: Path) -> None:
        nonlocal skipped
        for r in rels:
            if r.id in seen:
                skipped += 1
                print(f"  skip duplicate id {r.id} ({src})")
                continue
            seen.add(r.id)
            out.append((r, src))

    add(Registry.load(str(registry_path)).relationships, registry_path)

    if rejected_path.exists():
        raw = yaml.safe_load(rejected_path.read_text()) or {}
        full, stubs = [], 0
        for entry in raw.get("relationships") or []:
            try:
                full.append(Relationship.model_validate(entry))
            except ValueError:
                stubs += 1  # id/why tombstones — recorded verdicts, not legs
        if stubs:
            print(f"  {rejected_path}: {stubs} stub entries (id/why only) skipped")
        skipped += stubs
        add(full, rejected_path)

    for gen in sorted(generated_dir.glob("*.yaml")):
        add(Registry.load(str(gen)).relationships, gen)

    return out, skipped


def compile_registry(db_path: Path, pairs: list[tuple[Relationship, Path]],
                     game_league: dict, families: tuple) -> None:
    now_ns = time.time_ns()
    hashes = {p: git_hash(p) for p in {src for _, src in pairs}}
    conn = ledgerdb.connect(db_path)
    try:
        conn.execute("DELETE FROM registry_leg")
        conn.execute("DELETE FROM registry")
        for rel, src in pairs:
            conn.execute(
                "INSERT INTO registry (id, type, category, topic, verdict, vetted_by,"
                " oracle_risk, tranche, event_date, direction, confidence, vetted_at,"
                " source_file, source_git_hash, compiled_ns)"
                " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (rel.id, rel.type.value,
                 rel.category or category_of(rel.id, game_league=game_league),
                 rel.topic or topic_of(rel.id, families=families),
                 rel.verdict.value, rel.vetted_by.value, rel.oracle_risk.value,
                 rel.tranche.value, rel.event_date, rel.direction or None,
                 float(rel.confidence), rel.vetted_at,
                 str(src), hashes[src], now_ns))
            for i, leg in enumerate(rel.legs):
                conn.execute(
                    "INSERT INTO registry_leg (rel_id, leg_index, venue, market_id,"
                    " no_market_id, side, role) VALUES (?,?,?,?,?,?,?)",
                    (rel.id, i, leg.venue.value, leg.market_id, None,
                     leg.side.value, leg.role.value))
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    finally:
        conn.close()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", default="config/registry.yaml")
    ap.add_argument("--rejected", default="config/registry-rejected.yaml")
    ap.add_argument("--generated-dir", default="data/registry")
    ap.add_argument("--db", default=str(ledgerdb.DB_PATH))
    ap.add_argument("--topics", default="config/topics.yaml")
    ap.add_argument("--equiv-map", default="data/scan/sports_equiv_map.json")
    args = ap.parse_args()

    pairs, skipped = load_sources(Path(args.registry), Path(args.rejected),
                                  Path(args.generated_dir))
    compile_registry(Path(args.db), pairs,
                     game_league=load_game_league(args.equiv_map),
                     families=load_topic_families(args.topics))

    conn = ledgerdb.connect(Path(args.db), readonly=True)
    n = conn.execute("SELECT COUNT(*) FROM registry").fetchone()[0]
    n_tradable = conn.execute(
        "SELECT COUNT(*) FROM v_tradable_relationships").fetchone()[0]
    cats = Counter(dict(conn.execute(
        "SELECT category, COUNT(*) FROM registry GROUP BY category")))
    conn.close()

    print(f"compiled {n} relationships ({skipped} skipped) -> {args.db}; "
          f"{n_tradable} tradable")
    for cat, cnt in sorted(cats.items(), key=lambda kv: -kv[1]):
        print(f"  {cat:20s} {cnt}")


if __name__ == "__main__":
    main()
