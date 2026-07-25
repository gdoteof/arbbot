"""Recon guard state: the legacy .recon_*.json blobs migrate once into the
recon_* tables (schema.sql) and the dot-files are renamed *.migrated."""

import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

from reconcile_positions import _migrate_dotfiles

from arbbot.exec import ledgerdb

NAKED = "MLB A@B:kAWAY+1/pmAWAY+0/imb+1"


def test_dotfile_migration_once(tmp_path):
    d = tmp_path / "exec"
    d.mkdir()
    (d / ".recon_pm_n.json").write_text('{"n": 12}')
    (d / ".recon_state.json").write_text(json.dumps([NAKED]))
    (d / ".recon_ignore.json").write_text(json.dumps(
        [{"fragment": "france-pres", "max_imb": 2, "expires": time.time() + 3600}]))
    (d / ".recon_degraded.json").write_text(json.dumps(
        {"ts": 1753200000.0, "pm_n": 3, "baseline": 12, "missing": ["slug-a"]}))
    db = ledgerdb.connect(tmp_path / "trading.db")
    try:
        _migrate_dotfiles(db, d)
        assert db.execute("SELECT n_positions FROM recon_baseline"
                          " WHERE venue='polymarket_us'").fetchone()[0] == 12
        s = db.execute("SELECT * FROM recon_sighting").fetchone()
        assert s["imbalance_key"] == NAKED and s["runs"] == 1
        ig = db.execute("SELECT * FROM recon_ignore").fetchone()
        assert ig["fragment"] == "france-pres" and ig["max_imb"] == 2
        assert ig["expires_ns"] > time.time_ns()
        inc = db.execute("SELECT * FROM recon_incident").fetchone()
        assert inc["n_positions"] == 3 and inc["baseline"] == 12
        assert json.loads(inc["missing_json"]) == ["slug-a"]
        # dot-files renamed; a second call is a no-op
        for name in (".recon_pm_n.json", ".recon_state.json",
                     ".recon_ignore.json", ".recon_degraded.json"):
            assert not (d / name).exists()
            assert (d / (name + ".migrated")).exists()
        _migrate_dotfiles(db, d)
        assert db.execute("SELECT COUNT(*) FROM recon_incident").fetchone()[0] == 1
    finally:
        db.close()


def test_migration_skipped_when_table_populated(tmp_path):
    """A populated table wins — a stray leftover dot-file must not clobber it."""
    d = tmp_path / "exec"
    d.mkdir()
    (d / ".recon_pm_n.json").write_text('{"n": 5}')
    db = ledgerdb.connect(tmp_path / "trading.db")
    try:
        db.execute("INSERT INTO recon_baseline (venue, n_positions, updated_ns)"
                   " VALUES ('polymarket_us', 9, 1)")
        db.commit()
        _migrate_dotfiles(db, d)
        assert db.execute("SELECT n_positions FROM recon_baseline"
                          " WHERE venue='polymarket_us'").fetchone()[0] == 9
        assert (d / ".recon_pm_n.json").exists()  # untouched, not renamed
    finally:
        db.close()
