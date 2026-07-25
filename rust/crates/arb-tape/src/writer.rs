//! Append-only JSONL tape writer — one file per venue per UTC day,
//! line-at-a-time appends (crash leaves at most one torn final line, which
//! the readers tolerate). Mirrors src/arbbot/record/jsonl.py JsonlWriter.

use arb_core::model::TapeEvent;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
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
            let fh = OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.path_for(venue, utc_day))?;
            self.handles.insert(key.clone(), fh);
        }
        let fh = self.handles.get_mut(&key).expect("just inserted");
        let mut line = event.to_json_line();
        line.push('\n');
        fh.write_all(line.as_bytes())
    }
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
}
