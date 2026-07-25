"""P0 registry unification: model additions (category/topic/event_date,
VettedBy.MECHANICAL), the registry compiler (scripts/compile_registry.py ->
registry/registry_leg tables + v_tradable_relationships), and the sports
discovery Relationship-YAML emission. The view must agree with
Relationship.tradable for everything the pydantic gate covers, plus admit
the mechanical sports carve-out."""

import importlib.util
import json
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from arbbot.exec import ledgerdb
from arbbot.models.core import Role, Side, Venue
from arbbot.registry.model import (
    Leg,
    Registry,
    Relationship,
    RelationshipType,
    Verdict,
    VettedBy,
)

LIVE_REGISTRY = Path("/home/geoff/claude/arbbot/config/registry.yaml")


def _load_script(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).parent.parent / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def rel(rid="r1", vetted_by=VettedBy.HUMAN, verdict=Verdict.EQUIVALENT, **kw):
    defaults = dict(
        id=rid,
        type=RelationshipType.CROSS_VENUE_EQUIVALENT,
        legs=[Leg(venue=Venue.KALSHI, market_id=f"K-{rid}"),
              Leg(venue=Venue.POLYMARKET_US, market_id=f"p-{rid}", side=Side.NO)],
        verdict=verdict,
        vetted_by=vetted_by,
        confidence=Decimal("0.9"),
    )
    defaults.update(kw)
    return Relationship(**defaults)


# ---------- model additions ----------

def test_new_fields_default_none_and_roundtrip(tmp_path):
    r = rel()
    assert r.category is None and r.topic is None and r.event_date is None
    r2 = rel(rid="r2", category="sports", topic="mlb", event_date="2026-07-23",
             vetted_by=VettedBy.MECHANICAL)
    p = tmp_path / "reg.yaml"
    Registry(relationships=[r, r2]).dump(str(p))
    loaded = Registry.load(str(p))
    assert loaded.relationships[1].category == "sports"
    assert loaded.relationships[1].topic == "mlb"
    assert loaded.relationships[1].vetted_by is VettedBy.MECHANICAL


def test_mechanical_is_not_pydantic_tradable():
    # the pydantic gate stays human-only; mechanical is admitted ONLY via the
    # sports carve-out in v_tradable_relationships
    assert not rel(vetted_by=VettedBy.MECHANICAL, category="sports").tradable


@pytest.mark.skipif(not LIVE_REGISTRY.exists(), reason="live registry not present")
def test_live_registry_still_loads():
    reg = Registry.load(str(LIVE_REGISTRY))
    assert len(reg.relationships) > 0
    assert all(r.category is None for r in reg.relationships)  # pre-backfill


# ---------- compiler ----------

@pytest.fixture
def compiled(tmp_path):
    cr = _load_script("compile_registry")
    reg_yaml = tmp_path / "registry.yaml"
    Registry(relationships=[
        rel("xvus-time-poty-26-somebody"),
        rel("xv-agent-only", vetted_by=VettedBy.AGENT),
        rel("xv-rejected", verdict=Verdict.REJECTED),
        rel("sports-mlb-A@B", vetted_by=VettedBy.MECHANICAL,
            category="sports", topic="mlb", event_date="2026-07-23"),
        rel("mech-not-sports", vetted_by=VettedBy.MECHANICAL),
    ]).dump(str(reg_yaml))
    db = tmp_path / "trading.db"
    pairs, skipped = cr.load_sources(reg_yaml, tmp_path / "absent.yaml", tmp_path / "gen")
    cr.compile_registry(db, pairs, game_league={},
                        families=("time-poty-26",))
    return cr, reg_yaml, db, pairs


def test_compile_rows_and_derivation(compiled):
    _, _, db, pairs = compiled
    assert len(pairs) == 5
    conn = ledgerdb.connect(db, readonly=True)
    rows = {r["id"]: dict(r) for r in conn.execute("SELECT * FROM registry")}
    assert len(rows) == 5
    # derived when absent, stored when present
    assert rows["xvus-time-poty-26-somebody"]["category"] == "time-poty-26"
    assert rows["xvus-time-poty-26-somebody"]["topic"] == "time-poty-26"
    assert rows["sports-mlb-A@B"]["category"] == "sports"  # explicit field wins
    assert rows["sports-mlb-A@B"]["event_date"] == "2026-07-23"
    assert rows["xv-agent-only"]["topic"] == "other"
    legs = list(conn.execute(
        "SELECT * FROM registry_leg WHERE rel_id=? ORDER BY leg_index",
        ("sports-mlb-A@B",)))
    assert [(l["venue"], l["market_id"], l["side"], l["role"]) for l in legs] == [
        ("kalshi", "K-sports-mlb-A@B", "yes", "taker"),
        ("polymarket_us", "p-sports-mlb-A@B", "no", "taker")]
    conn.close()


def test_tradable_view_gate(compiled):
    _, _, db, _ = compiled
    conn = ledgerdb.connect(db, readonly=True)
    tradable = {r["id"] for r in conn.execute("SELECT id FROM v_tradable_relationships")}
    conn.close()
    # human+equivalent yes; mechanical only with sports category; agent/rejected no
    assert tradable == {"xvus-time-poty-26-somebody", "sports-mlb-A@B"}


def test_compile_idempotent(compiled):
    cr, _, db, pairs = compiled
    cr.compile_registry(db, pairs, game_league={}, families=("time-poty-26",))
    conn = ledgerdb.connect(db, readonly=True)
    assert conn.execute("SELECT COUNT(*) FROM registry").fetchone()[0] == 5
    assert conn.execute("SELECT COUNT(*) FROM registry_leg").fetchone()[0] == 10
    conn.close()


@pytest.mark.skipif(not LIVE_REGISTRY.exists(), reason="live registry not present")
def test_view_matches_pydantic_tradable_on_live_registry(tmp_path):
    cr = _load_script("compile_registry")
    reg = Registry.load(str(LIVE_REGISTRY))
    pairs = [(r, LIVE_REGISTRY) for r in reg.relationships]
    db = tmp_path / "trading.db"
    cr.compile_registry(db, pairs, game_league={}, families=())
    conn = ledgerdb.connect(db, readonly=True)
    view = {r["id"] for r in conn.execute("SELECT id FROM v_tradable_relationships")}
    conn.close()
    assert view == {r.id for r in reg.relationships if r.tradable}


# ---------- sports discovery emission ----------

FIXTURE_MATCH = {
    "league": "mlb", "kalshi_series": "KXMLBGAME", "date": "2026-07-23",
    "teams": "Tampa Bay Rays vs Toronto Blue Jays",
    "pm_slug": "mlb-tb-tor-2026-07-23",
    "pm_moneyline": "aec-mlb-tb-tor-2026-07-23-ml",
    "pm_long_team": "Tampa Bay Rays",
    "kalshi_event": "KXMLBGAME-26JUL23TBTOR",
    "kalshi_long_ticker": "KXMLBGAME-26JUL23TBTOR-TB",
}


def test_emit_registry_yaml(tmp_path, monkeypatch):
    sed = _load_script("sports_equiv_discovery")
    map_path = tmp_path / "map.json"
    map_path.write_text(json.dumps({
        "generated_at": "2026-07-23T00:00:00Z",
        "matches": [FIXTURE_MATCH,
                    {**FIXTURE_MATCH, "pm_moneyline": None},  # unpaired: skipped
                    FIXTURE_MATCH]}))  # duplicate: deduped
    out = sed.emit_registry_yaml(map_path, out_dir=tmp_path / "registry")
    reg = Registry.load(str(out))
    assert len(reg.relationships) == 1
    r = reg.relationships[0]
    # away/home truncated to 20 chars exactly like the sports_arb game key
    assert r.id == "sports-mlb-Tampa Bay Rays@Toronto Blue Jays"
    assert r.type is RelationshipType.CROSS_VENUE_EQUIVALENT
    assert r.vetted_by is VettedBy.MECHANICAL and r.verdict is Verdict.EQUIVALENT
    assert r.category == "sports" and r.topic == "mlb"
    assert r.event_date == "2026-07-23"
    assert [(l.venue, l.market_id, l.side, l.role) for l in r.legs] == [
        (Venue.KALSHI, "KXMLBGAME-26JUL23TBTOR-TB", Side.YES, Role.TAKER),
        (Venue.POLYMARKET_US, "aec-mlb-tb-tor-2026-07-23-ml", Side.NO, Role.TAKER)]
    assert not r.tradable  # mechanical: tradable only via the DB view


def test_emit_registry_yaml_missing_map(tmp_path):
    sed = _load_script("sports_equiv_discovery")
    assert sed.emit_registry_yaml(tmp_path / "nope.json",
                                  out_dir=tmp_path / "registry") is None


def test_emitted_yaml_compiles_tradable(tmp_path):
    sed = _load_script("sports_equiv_discovery")
    cr = _load_script("compile_registry")
    map_path = tmp_path / "map.json"
    map_path.write_text(json.dumps({"matches": [FIXTURE_MATCH]}))
    gen_dir = tmp_path / "registry"
    sed.emit_registry_yaml(map_path, out_dir=gen_dir)
    main_yaml = tmp_path / "registry.yaml"
    Registry(relationships=[rel("xvus-time-poty-26-somebody")]).dump(str(main_yaml))
    pairs, _ = cr.load_sources(main_yaml, tmp_path / "absent.yaml", gen_dir)
    db = tmp_path / "trading.db"
    cr.compile_registry(db, pairs, game_league={}, families=())
    conn = ledgerdb.connect(db, readonly=True)
    tradable = {r["id"] for r in conn.execute("SELECT id FROM v_tradable_relationships")}
    n_legs = conn.execute(
        "SELECT COUNT(*) FROM registry_leg JOIN registry ON id = rel_id"
        " WHERE venue='kalshi'").fetchone()[0]
    conn.close()
    assert len(tradable) == 2 and any(t.startswith("sports-mlb-") for t in tradable)
    assert n_legs == 2
