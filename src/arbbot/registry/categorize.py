"""Centralized category/topic derivation from relationship ids.

Faithful ports of the two id-string-parsing derivations that grew up inline:

  category_of  <- dash.data DashData._category (per-category P&L breakdown)
  topic_of     <- risk.manager.RiskManager.topic_of (topic budget lookup)

P0 registry unification: these become THE reference implementations; the
compiler (scripts/compile_registry.py) stamps their output onto relationships
that don't carry explicit category/topic fields, and dash/risk swap to the
stored fields later. Until that swap the rules here must not drift from the
originals — every quirk (e.g. `"sports-lean-*" -> sports-misc`, category
matching families as bare substrings while topic requires dash-delimited
matches) is intentional, pinned by tests/test_categorize.py.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Mapping, Optional

import yaml

# dash.data FAMILIES — category families matched as bare substrings of the id.
CATEGORY_FAMILIES = ("france-pres-27", "time-poty-26", "nobel-peace-26",
                     "brazil-pres-26", "fedcut-26", "bestai-26dec")

# league segments that map sports-<seg>-... ids straight to a category
_SPORT_LEAGUES = ("mlb", "itfme", "itfwo", "wta", "atp", "kbo", "npb")

DEFAULT_GAME_LEAGUE_PATH = Path("data/scan/sports_equiv_map.json")
DEFAULT_TOPICS_PATH = Path("config/topics.yaml")


def load_game_league(path: Path | str = DEFAULT_GAME_LEAGUE_PATH) -> dict[str, str]:
    """game-key -> league map from the sports equiv map, so rehedge/flatten
    records (whose ids carry the game, not the league) still land in their
    real category. Missing/corrupt map -> {} (same as dash's suppress)."""
    try:
        smap = json.loads(Path(path).read_text())
    except (OSError, ValueError):
        return {}
    return {sm["teams"].replace(" vs ", "@")[:30]: sm["league"]
            for sm in smap.get("matches", []) if sm.get("teams")}


def category_of(relationship_id: Optional[str],
                game_league: Optional[Mapping[str, str]] = None) -> str:
    """Dashboard P&L category for a relationship id (dash.data._category)."""
    rid = str(relationship_id or "")
    if rid.startswith("sports-"):
        seg = rid.split("-", 2)[1] if "-" in rid[7:] else "misc"
        if seg in _SPORT_LEAGUES:
            return f"sports-{seg}"
        if seg in ("rehedge", "flatten"):
            game = rid.split("-", 2)[2] if rid.count("-") >= 2 else ""
            lg = (game_league or {}).get(game[:30])
            return f"sports-{lg}" if lg else "sports-misc"
        return "sports-misc"
    for f2 in CATEGORY_FAMILIES:
        if f2 in rid:
            return f2
    return "other"


def load_topic_families(path: Path | str = DEFAULT_TOPICS_PATH) -> tuple[str, ...]:
    """Family keys from config/topics.yaml (untracked). Missing file -> ()."""
    try:
        raw = yaml.safe_load(Path(path).read_text()) or {}
    except (OSError, yaml.YAMLError):
        return ()
    return tuple(t["family"] for t in raw.get("topics") or [] if t.get("family"))


def topic_of(relationship_id: str,
             families: Optional[Iterable[str]] = None) -> str:
    """Longest configured family whose key appears dash-delimited in the id;
    else 'other' (risk.manager.RiskManager.topic_of)."""
    if families is None:
        families = load_topic_families()
    hay = f"-{relationship_id}-"
    best = ""
    for family in families:
        if f"-{family}-" in hay and len(family) > len(best):
            best = family
    return best or "other"
