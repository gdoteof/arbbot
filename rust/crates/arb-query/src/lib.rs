//! arb-query — read layer over the recorded tape.
//!
//! The tape lives in two formats at once: today's day is append-only JSONL
//! (the recorder's write format — crash-safe and greppable) and closed days are
//! Parquet (the read format — columnar, ~20-25x smaller, and the reason a
//! one-relationship query is milliseconds instead of minutes). Callers should
//! never care which; `source_for` resolves it, exactly as
//! `src/arbbot/record/archive.py:60` does on the Python side.
//!
//! Deliberately NOT a query engine. It reads columns and folds them. Anything
//! that wants SQL over the whole archive is research, and research keeps
//! DuckDB.

use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

pub mod intents;
pub mod opps;

/// Which physical file backs a `<stem>-<day>`, and in what format.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Parquet(PathBuf),
    Jsonl(PathBuf),
}

impl Source {
    pub fn path(&self) -> &Path {
        match self {
            Source::Parquet(p) | Source::Jsonl(p) => p,
        }
    }
}

/// Is this file a COMPLETE Parquet file? Every Parquet file opens and closes
/// with the 4-byte magic `PAR1`, and the closing copy is written LAST — so a
/// truncated or zero-byte ETL output fails this while `is_file()` passes it.
///
/// Deliberately NOT a parse: one open, one stat and two 4-byte reads. This sits
/// on the read path of every query and the file it guards is up to 128 MB, so
/// the check has to cost nothing next to the read it is protecting. It cannot
/// prove the body is intact; it proves the writer got to the end, which is the
/// failure that actually happens here.
pub fn parquet_is_complete(path: &Path) -> bool {
    const MAGIC: [u8; 4] = *b"PAR1";
    let Ok(mut f) = std::fs::File::open(path) else { return false };
    // The shortest a Parquet file can be: magic, 4-byte footer length, magic.
    if f.metadata().map(|m| m.len()).unwrap_or(0) < 12 {
        return false;
    }
    let (mut head, mut tail) = ([0u8; 4], [0u8; 4]);
    f.read_exact(&mut head).is_ok()
        && f.seek(SeekFrom::End(-4)).is_ok()
        && f.read_exact(&mut tail).is_ok()
        && head == MAGIC
        && tail == MAGIC
}

/// Parquet archive preferred, live JSONL as fallback, `None` only when the day
/// has no file at all. Mirrors `archive.source_for`, so a day resolves the same
/// way in both stacks — with the one deliberate exception below.
///
/// A Parquet that is not a complete FILE is not an archive. A failed nightly
/// ETL left `kalshi-2026-07-27.parquet` at zero bytes beside the good 128 MB
/// tape, and `is_file()` handed every query the empty one: silent, total data
/// loss on read, with the whole day still sitting in `data/raw`.
///
/// The two stacks therefore no longer agree, and the Python side is NOT
/// dormant: `archive.source_for` (`record/archive.py:96`) still takes any
/// Parquet that `exists()`, and `arbbot-report.timer` runs that chain nightly
/// at 07:30 with every script's output on /dev/null. It WROTE the zero-byte
/// file above and then read it back the same minute
/// (`velocity_analysis.py --day`, `maketake_sim.py --day`). The freeze is on
/// EDITING Python, not on running it, so this is the stricter side of a
/// divergence that is out of scope here — not one that cannot bite.
pub fn source_for(jsonl_dir: &str, parquet_dir: &str, stem: &str, day: &str) -> Option<Source> {
    let pq = PathBuf::from(parquet_dir).join(format!("{stem}-{day}.parquet"));
    let pq_exists = pq.is_file();
    if pq_exists && parquet_is_complete(&pq) {
        return Some(Source::Parquet(pq));
    }
    let jl = PathBuf::from(jsonl_dir).join(format!("{stem}-{day}.jsonl"));
    if jl.is_file() {
        return Some(Source::Jsonl(jl));
    }
    // Unreadable archive, no raw tape behind it: this file may be the only
    // copy of the day there is. That takes corruption AFTER a passing verify —
    // the ETL deletes the JSONL only once the row count and a canonical-row
    // sha256 both match, unlinking the Parquet on either mismatch
    // (`archive_to_parquet.py:84-92`) — so it is rare, not impossible, and the
    // answer is the same either way. Hand it back and let the read fail loudly
    // naming the file; `None` would drop the day out of the range without a
    // word.
    pq_exists.then_some(Source::Parquet(pq))
}

/// Every available source for `stem` across the inclusive day range, oldest
/// first. Missing days are skipped, not an error — a gap in the archive is a
/// fact to report, not a reason to fail a dashboard request.
pub fn sources_for_range(
    jsonl_dir: &str,
    parquet_dir: &str,
    stem: &str,
    from_day: &str,
    to_day: &str,
) -> Vec<Source> {
    let mut days: Vec<String> = Vec::new();
    for dir in [parquet_dir, jsonl_dir] {
        let Ok(rd) = std::fs::read_dir(dir) else { continue };
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            let Some(base) = name.strip_suffix(".parquet").or_else(|| name.strip_suffix(".jsonl"))
            else {
                continue;
            };
            let Some(rest) = base.strip_prefix(stem) else { continue };
            let Some(day) = rest.strip_prefix('-') else { continue };
            if day.len() == 10
                && day.as_bytes()[4] == b'-'
                && day >= from_day
                && day <= to_day
                && !days.contains(&day.to_string())
            {
                days.push(day.to_string());
            }
        }
    }
    days.sort();
    days.iter()
        .filter_map(|d| source_for(jsonl_dir, parquet_dir, stem, d))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A complete Parquet file in the 12 bytes `parquet_is_complete` reads:
    /// magic, footer length, magic. These tests are about which file gets
    /// CHOSEN, so they never need a real archive body.
    const COMPLETE: &[u8] = b"PAR1\0\0\0\0PAR1";

    /// Per-test directory. Tests run in parallel, so keying only on pid lets one
    /// test's fixture files satisfy another's "should not exist" assertion.
    fn tmp(name: &str) -> PathBuf {
        let base =
            std::env::temp_dir().join(format!("arb-query-test-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let _ = std::fs::create_dir_all(base.join("scan"));
        let _ = std::fs::create_dir_all(base.join("parquet"));
        base
    }

    #[test]
    fn parquet_wins_over_jsonl_for_the_same_day() {
        let b = tmp("prefer");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        std::fs::write(j.join("opportunities-2026-07-20.jsonl"), b"").unwrap();
        std::fs::write(p.join("opportunities-2026-07-20.parquet"), COMPLETE).unwrap();
        std::fs::write(j.join("opportunities-2026-07-21.jsonl"), b"").unwrap();

        // archived day -> parquet
        match source_for(&js, &ps, "opportunities", "2026-07-20") {
            Some(Source::Parquet(_)) => {}
            other => panic!("expected parquet, got {other:?}"),
        }
        // live day -> jsonl
        match source_for(&js, &ps, "opportunities", "2026-07-21") {
            Some(Source::Jsonl(_)) => {}
            other => panic!("expected jsonl, got {other:?}"),
        }
        assert!(source_for(&js, &ps, "opportunities", "2026-07-22").is_none());

        let all = sources_for_range(&js, &ps, "opportunities", "2026-07-19", "2026-07-30");
        assert_eq!(all.len(), 2, "{all:?}");
        // oldest first, and the archived day is not double-counted
        assert!(matches!(all[0], Source::Parquet(_)));
        assert!(matches!(all[1], Source::Jsonl(_)));

        let _ = std::fs::remove_dir_all(&b);
    }

    /// The live defect. A failed nightly ETL left a ZERO-BYTE
    /// `kalshi-2026-07-27.parquet` next to a good 128,685,072-byte JSONL, and
    /// every query for that day resolved to the empty file — the whole tape
    /// read as an archived day with nothing in it. An archive only outranks the
    /// raw tape if it is actually a file.
    #[test]
    fn a_broken_parquet_does_not_shadow_the_jsonl_behind_it() {
        let b = tmp("broken");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        for d in ["2026-07-20", "2026-07-21"] {
            std::fs::write(j.join(format!("opportunities-{d}.jsonl")), b"").unwrap();
        }
        std::fs::write(p.join("opportunities-2026-07-20.parquet"), b"").unwrap();
        // Bytes, but no closing magic: the ETL died before it wrote the footer,
        // so a size check alone would call this an archive.
        std::fs::write(p.join("opportunities-2026-07-21.parquet"), b"PAR1rows rows rows").unwrap();

        for d in ["2026-07-20", "2026-07-21"] {
            match source_for(&js, &ps, "opportunities", d) {
                Some(Source::Jsonl(_)) => {}
                other => panic!("{d}: expected the raw tape, got {other:?}"),
            }
        }
        let all = sources_for_range(&js, &ps, "opportunities", "2026-07-19", "2026-07-30");
        assert_eq!(all.len(), 2, "{all:?}");
        assert!(all.iter().all(|s| matches!(s, Source::Jsonl(_))), "{all:?}");

        let _ = std::fs::remove_dir_all(&b);
    }

    /// The ETL deletes the raw tape once it believes it archived one, so a
    /// broken Parquet with no JSONL behind it may be the only copy of that day
    /// there is. It is still returned, because the reader's error names the
    /// file — while dropping the day would report a missing tape as an empty
    /// one and lose the day silently, which is the same lie this fix removes.
    #[test]
    fn a_broken_parquet_with_no_raw_behind_it_is_still_returned() {
        let b = tmp("onlycopy");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        std::fs::write(p.join("opportunities-2026-07-20.parquet"), b"").unwrap();
        match source_for(&js, &ps, "opportunities", "2026-07-20") {
            Some(Source::Parquet(_)) => {}
            other => panic!("expected the broken parquet, got {other:?}"),
        }
        let _ = std::fs::remove_dir_all(&b);
    }

    /// What the check actually reads, spelled out: the magic at BOTH ends.
    #[test]
    fn completeness_is_the_magic_at_both_ends() {
        let b = tmp("magic");
        let f = |name: &str, bytes: &[u8]| -> bool {
            let p = b.join(name);
            std::fs::write(&p, bytes).unwrap();
            parquet_is_complete(&p)
        };
        assert!(f("ok.parquet", COMPLETE));
        assert!(!f("empty.parquet", b""), "zero bytes");
        assert!(!f("short.parquet", b"PAR1PAR1"), "no room for a footer length");
        assert!(!f("head.parquet", b"PAR1rows rows rows"), "footer never written");
        assert!(!f("tail.parquet", b"rows rows rowsPAR1"), "header never written");
        assert!(!parquet_is_complete(&b.join("nothing.parquet")), "absent");
        let _ = std::fs::remove_dir_all(&b);
    }

    #[test]
    fn range_bounds_are_inclusive_and_filter() {
        let b = tmp("range");
        let (j, p) = (b.join("scan"), b.join("parquet"));
        let (js, ps) = (j.to_string_lossy().to_string(), p.to_string_lossy().to_string());
        for d in ["2026-07-20", "2026-07-21", "2026-07-22"] {
            std::fs::write(j.join(format!("opportunities-{d}.jsonl")), b"").unwrap();
        }
        let got = sources_for_range(&js, &ps, "opportunities", "2026-07-21", "2026-07-22");
        assert_eq!(got.len(), 2);
        assert!(got[0].path().to_string_lossy().contains("2026-07-21"));
        let _ = std::fs::remove_dir_all(&b);
    }
}
