"""Strategy-contract v2 verification: the manifest (config/strategies.yaml)
round-trips through scripts/compile_strategies.py into a scratch DB.

The compile is design-only — the live trading DB must stay untouched, so the
compiler refuses any --db under data/ and these tests only ever use tmp_path.
"""

import importlib.util
import json
import sqlite3
from pathlib import Path

import pytest
import yaml

MANIFEST = Path(__file__).parent.parent / "config" / "strategies.yaml"

# Resolved 2026-07-24: the dry-run found sports_arb's detector and its
# rehedge watcher (same process, two manifest identities) both declaring
# data/exec/sports_naked.json — violating §4 single ownership. The manifest
# now assigns the file to sports-take-take (the detector writes it); the
# rehedge watcher is annotated as its in-process lifecycle counterpart.
KNOWN_MANIFEST_ERRORS = []

EXPECTED_FAMILIES = {
    "external-intents": 6, "manual-tool": 5, "directional-signal": 2,
    "maker-hedge": 2, "take-take-cross": 2, "audit-readonly": 1, "retires": 1,
}


def _load_script(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).parent.parent / "scripts" / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def cs():
    return _load_script("compile_strategies")


def entry(sid="s1", **kw):
    e = dict(id=sid, kind="taker", family="external-intents",
             target_runtime="external-intents", today="python-manual",
             claims=[], ledger_tags=[], close_kinds=[], state_files=[],
             source=None, intents_stream=None, kill="n/a")
    e.update(kw)
    return e


def errors_for(cs, *entries):
    return cs.validate(list(entries))


# ---------- the manifest as written ----------

def test_real_manifest_has_only_the_known_defect(cs):
    assert cs.validate(cs.load_manifest(MANIFEST)) == KNOWN_MANIFEST_ERRORS


@pytest.fixture
def compiled(cs, tmp_path, monkeypatch, capsys):
    """The real manifest (validates clean since the sports_naked.json
    ownership fix), compiled into a scratch DB through the CLI."""
    entries = cs.load_manifest(MANIFEST)
    manifest = tmp_path / "strategies.yaml"
    manifest.write_text(yaml.safe_dump({"strategies": entries}))
    db = tmp_path / "scratch.db"
    monkeypatch.setattr(
        "sys.argv", ["compile_strategies", "--manifest", str(manifest),
                     "--db", str(db)])
    assert cs.main() == 0
    return db, capsys.readouterr().out


def test_happy_path_compiles_19_entries(compiled):
    db, out = compiled
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM strategy").fetchone()[0] == 19
    families = dict(conn.execute(
        "SELECT family, COUNT(*) FROM strategy GROUP BY 1"))
    assert families == EXPECTED_FAMILIES
    # family entries carry their template config; sentinels don't
    with_params = {r[0] for r in conn.execute(
        "SELECT id FROM strategy WHERE params_json IS NOT NULL")}
    assert with_params == {"make-take", "pmus-maker-probe", "take-take",
                           "sports-take-take", "leadlag-probe", "pm-lean"}
    raw = json.loads(conn.execute(
        "SELECT raw_json FROM strategy WHERE id='make-take'").fetchone()[0])
    assert raw["params"]["anchor"] == "riskless-hedge-cost"
    conn.close()
    assert "compiled 19 strategies" in out
    assert "kill gaps (DOES-NOT-CHECK) — 7" in out


def test_claims_compile_one_row_per_pattern(compiled):
    db, _ = compiled
    conn = sqlite3.connect(db)
    assert conn.execute("SELECT COUNT(*) FROM strategy_claim").fetchone()[0] == 26
    # take-take: one rel_ids row per prefix
    assert [r[0] for r in conn.execute(
        "SELECT pattern FROM strategy_claim WHERE strategy_id='take-take'"
        " AND kind='rel_ids' ORDER BY claim_index")] == [
        "xvus-time-poty-26", "xvus-france-pres-27", "xvus-brazil-pres-26",
        "xvus-fedcut-26"]
    # make-take's single claim carries both a registry_query and rel_ids
    assert [(r[0], r[1]) for r in conn.execute(
        "SELECT kind, pattern FROM strategy_claim WHERE strategy_id='make-take'"
        " ORDER BY claim_index")] == [
        ("rel_ids", "xvus-france-pres-27-*"),
        ("rel_ids", "xvus-time-poty-26-*"),
        ("registry_query",
         "vetted cross-venue relationships passed via --relationship")]
    # venue survives onto every pattern of its claim
    assert conn.execute(
        "SELECT venue FROM strategy_claim WHERE strategy_id='toxicity-probe'"
    ).fetchone()[0] == "kalshi"
    # claims: [] compiles to no rows, and that is not an error
    assert conn.execute(
        "SELECT COUNT(*) FROM strategy_claim WHERE strategy_id='reconcile-audit'"
    ).fetchone()[0] == 0
    conn.close()


# ---------- validation ----------

def test_duplicate_id_fails(cs):
    assert errors_for(cs, entry("dup"), entry("dup")) == ["dup: duplicate id"]


def test_non_kebab_id_fails(cs):
    assert errors_for(cs, entry("Make_Take")) == ["Make_Take: id must be kebab-case"]


def test_overlapping_state_files_fail(cs):
    errs = errors_for(cs, entry("a", state_files=["data/exec/x.json"]),
                      entry("b", state_files=["data/exec/x.json"]))
    assert errs == ["state_file data/exec/x.json owned by 2 entries: a, b"]


def test_unknown_family_fails(cs):
    assert errors_for(cs, entry("a", family="scalping")) == [
        "a: unknown family 'scalping'"]


def test_rust_template_must_match_family_field(cs):
    assert errors_for(cs, entry("a", family="maker-hedge",
                                target_runtime="rust-template(take-take-cross)",
                                params={"clip": 5})) == [
        "a: target_runtime rust-template(take-take-cross) disagrees with "
        "family 'maker-hedge'"]
    assert errors_for(cs, entry("a", family="maker-hedge",
                                target_runtime="rust-template(nope)",
                                params={"clip": 5})) == [
        "a: rust-template names unknown family 'nope'"]


def test_family_entry_needs_params(cs):
    assert errors_for(cs, entry("a", family="maker-hedge",
                                target_runtime="rust-template(maker-hedge)")) == [
        "a: family maker-hedge entry has no params"]


def test_undocumented_ledger_tag_collision_fails(cs):
    assert errors_for(cs, entry("a", ledger_tags=["take-take"]),
                      entry("b", ledger_tags=["take-take"])) == [
        "ledger_tag take-take written by a, b — undocumented collision"]
    # the documented sharers are allowed
    assert errors_for(cs, entry("take-take", ledger_tags=["take-take"]),
                      entry("naked-hedge", ledger_tags=["take-take"]),
                      entry("auto-take-take", ledger_tags=["take-take"])) == []


def test_bad_enums_fail(cs):
    assert errors_for(cs, entry("a", target_runtime="python-daemon")) == [
        "a: unknown target_runtime 'python-daemon'"]
    assert errors_for(cs, entry("a", today="rust")) == ["a: unknown today 'rust'"]
    assert errors_for(cs, entry("a", kill="maybe")) == ["a: unknown kill 'maybe'"]


# ---------- the scratch-DB guard ----------

def test_db_under_data_is_refused(cs, tmp_path, monkeypatch, capsys):
    target = tmp_path / "data" / "exec" / "trading.db"
    with pytest.raises(ValueError, match="no writes under data/"):
        cs.resolve_db(str(target))
    monkeypatch.setattr("sys.argv",
                        ["compile_strategies", "--db", str(target)])
    assert cs.main() == 2
    assert "refusing to compile into" in capsys.readouterr().err
    assert not target.exists()


def test_memory_db_is_the_default(cs):
    assert cs.resolve_db(":memory:") == ":memory:"
