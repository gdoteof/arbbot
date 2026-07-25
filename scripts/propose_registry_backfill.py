#!/usr/bin/env python3
"""One-shot: propose category/topic backfill for the hand-vetted registry.

Reads config/registry.yaml READ-ONLY, computes category/topic for every
relationship via arbbot.registry.categorize, and writes a PROPOSED patched
YAML to --out (NEVER into config/) plus a per-id diff summary to stdout —
for Geoff to eyeball and apply to the live file himself. Only adds the two
keys where absent; existing keys and everything else are left verbatim
(raw YAML round-trip, not a pydantic re-dump, so no default-materialization
noise in the diff).

Usage: PYTHONPATH=src python scripts/propose_registry_backfill.py \
    --registry config/registry.yaml --out /path/to/registry.proposed.yaml
"""

import argparse
import tempfile
from collections import Counter
from pathlib import Path

import yaml

from arbbot.registry.categorize import (
    category_of,
    load_game_league,
    load_topic_families,
    topic_of,
)
from arbbot.registry.model import Registry


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--registry", default="config/registry.yaml")
    ap.add_argument("--out",
                    default=str(Path(tempfile.gettempdir()) / "registry.proposed.yaml"))
    ap.add_argument("--topics", default="config/topics.yaml")
    ap.add_argument("--equiv-map", default="data/scan/sports_equiv_map.json")
    args = ap.parse_args()

    out = Path(args.out)
    if out.resolve().parent == Path(args.registry).resolve().parent:
        raise SystemExit("refusing to write into the registry's config dir")

    game_league = load_game_league(args.equiv_map)
    families = load_topic_families(args.topics)

    raw = yaml.safe_load(Path(args.registry).read_text()) or {}
    cats, tops = Counter(), Counter()
    n_added = 0
    for rel in raw.get("relationships") or []:
        cat = rel.get("category") or category_of(rel["id"], game_league=game_league)
        top = rel.get("topic") or topic_of(rel["id"], families=families)
        added = []
        if "category" not in rel:
            rel["category"] = cat
            added.append(f"category={cat}")
        if "topic" not in rel:
            rel["topic"] = top
            added.append(f"topic={top}")
        if added:
            n_added += 1
            print(f"  {rel['id']:50s} + {' '.join(added)}")
        cats[cat] += 1
        tops[top] += 1

    Registry.model_validate(raw)  # proposed file must still load
    out.write_text(yaml.safe_dump(raw, sort_keys=False, allow_unicode=True))

    print(f"\n{n_added} of {len(raw.get('relationships') or [])} entries patched "
          f"-> {out} (live file untouched)")
    print("categories:", dict(sorted(cats.items(), key=lambda kv: -kv[1])))
    print("topics:    ", dict(sorted(tops.items(), key=lambda kv: -kv[1])))


if __name__ == "__main__":
    main()
