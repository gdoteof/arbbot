//! Append-only JSONL tape writer — one file per venue per UTC day,
//! line-at-a-time appends (crash leaves at most one torn final line, which
//! the readers tolerate). Mirrors src/arbbot/record/jsonl.py JsonlWriter.

use arb_core::model::TapeEvent;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Read, Write};
use std::path::{Path, PathBuf};

pub struct JsonlWriter {
    data_dir: PathBuf,
    handles: HashMap<String, File>,
}

impl JsonlWriter {
    pub fn new(data_dir: impl AsRef<Path>) -> std::io::Result<Self> {
        std::fs::create_dir_all(&data_dir)?;
        Ok(Self { data_dir: data_dir.as_ref().to_owned(), handles: HashMap::new() })
    }

    pub fn path_for(&self, venue: &str, utc_day: &str) -> PathBuf {
        self.data_dir.join(format!("{venue}-{utc_day}.jsonl"))
    }

    pub fn write(&mut self, event: &TapeEvent, utc_day: &str) -> std::io::Result<()> {
        let venue = event.venue().as_str();
        let key = format!("{venue}-{utc_day}");
        if !self.handles.contains_key(&key) {
            let mut fh = OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(self.path_for(venue, utc_day))?;
            heal_torn_tail(&mut fh)?;
            self.handles.insert(key.clone(), fh);
        }
        let fh = self.handles.get_mut(&key).expect("just inserted");
        let mut line = event.to_json_line();
        line.push('\n');
        fh.write_all(line.as_bytes())
    }
}

/// If the file does not end in a newline, add one before appending.
///
/// A crash mid-write leaves a torn final line. Reopening in append mode then
/// writes the next record ONTO that line, fusing two records into one that
/// parses as neither — turning a tolerable torn tail into permanent mid-file
/// corruption. Observed in the Python tape: `kalshi-2026-07-27.jsonl` line
/// 377,457 is a truncated trade with a snapshot welded to it, written seconds
/// after the 08:21 crash-restart. One line in 377k, and `--parse-check`
/// rejects the whole day for it.
///
/// Costs one seek and one byte read per file open, which happens once per
/// venue per day.
fn heal_torn_tail(fh: &mut File) -> std::io::Result<()> {
    let len = fh.seek(SeekFrom::End(0))?;
    if len == 0 {
        return Ok(());
    }
    fh.seek(SeekFrom::Start(len - 1))?;
    let mut last = [0u8; 1];
    fh.read_exact(&mut last)?;
    if last[0] != b'\n' {
        fh.write_all(b"\n")?;
    }
    Ok(())
}

/// Current UTC day as YYYY-MM-DD (no chrono: civil-date math from epoch days).
pub fn utc_day() -> String {
    let secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock before epoch")
        .as_secs() as i64;
    let days = secs.div_euclid(86_400);
    civil_from_days(days)
}

/// Howard Hinnant's civil_from_days algorithm.
fn civil_from_days(z: i64) -> String {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates() {
        assert_eq!(civil_from_days(0), "1970-01-01");
        assert_eq!(civil_from_days(20_657), "2026-07-23");
    }

    #[test]
    fn writes_per_venue_day_files() {
        let dir = std::env::temp_dir().join(format!("arb-tape-test-{}", std::process::id()));
        let mut w = JsonlWriter::new(&dir).unwrap();
        let ev: TapeEvent = serde_json::from_str(
            r#"{"kind":"trade","venue":"kalshi","market_id":"T","price":"0.10","size":"1","taker_side":null,"seq":1,"ts_local_ns":5,"ts_venue":null}"#,
        )
        .unwrap();
        w.write(&ev, "2026-07-23").unwrap();
        w.write(&ev, "2026-07-23").unwrap();
        let content = std::fs::read_to_string(w.path_for("kalshi", "2026-07-23")).unwrap();
        assert_eq!(content.lines().count(), 2);
        assert_eq!(content.lines().next().unwrap(), ev.to_json_line());
        std::fs::remove_dir_all(dir).ok();
    }
    #[test]
    fn a_torn_tail_is_healed_instead_of_fused() {
        let dir = std::env::temp_dir().join(format!("arb-tape-heal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("kalshi-2026-07-27.jsonl");
        // simulate a crash mid-write: a partial record with NO trailing newline
        std::fs::write(&path, b"{\"kind\":\"trade\",\"venue\":\"kal").unwrap();

        let mut fh = OpenOptions::new().create(true).read(true).append(true).open(&path).unwrap();
        heal_torn_tail(&mut fh).unwrap();
        fh.write_all(b"{\"kind\":\"snapshot\"}\n").unwrap();
        drop(fh);

        let text = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2, "the two records must not fuse: {text:?}");
        assert_eq!(lines[1], "{\"kind\":\"snapshot\"}", "the new record is intact");
        // the torn line stays torn — recoverable as ONE bad line, not two lost
        assert!(lines[0].starts_with("{\"kind\":\"trade\""));

        // healing an already-clean file must be a no-op
        let before = std::fs::read_to_string(&path).unwrap();
        let mut fh = OpenOptions::new().create(true).read(true).append(true).open(&path).unwrap();
        heal_torn_tail(&mut fh).unwrap();
        drop(fh);
        assert_eq!(std::fs::read_to_string(&path).unwrap(), before, "no spurious newline");

        let _ = std::fs::remove_dir_all(&dir);
    }

}

