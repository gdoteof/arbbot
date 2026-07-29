//! arb-tob — build a per-market top-of-book series from the raw tape.
//!
//! The Rust port of `scripts/build_tob_tape.py`, which produces exactly the
//! series the pair drill-down needs but was never put on a timer (its last
//! output is `tob-2026-07-22.parquet`, and the tape has moved on).
//!
//! This is the one view that genuinely earns precomputation. Everything else
//! the dashboard shows is folded straight off the tape per request, but a
//! price panel needs book STATE over time, which means replaying multi-GB days
//! through a `BookBuilder` — far too slow to do while someone waits.
//!
//! Two properties make it cheap:
//!
//!   1. A market's book depends only on its own venue's events, so each venue
//!      file replays INDEPENDENTLY. No cross-venue time merge, and the files
//!      can be processed in parallel.
//!   2. Samples are emitted on CHANGE, rate-limited to one per interval, not
//!      on a fixed grid for every market. Quiet markets cost nothing, and a
//!      consumer forward-fills between samples.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::model::TapeEvent;
use arb_query::Source;
use serde::{Deserialize, Serialize};

pub mod raw;
pub mod series;

pub const DEFAULT_INTERVAL_NS: i64 = 60_000_000_000; // 60s

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TobSample {
    pub venue: String,
    pub market_id: String,
    pub ts_local_ns: i64,
    pub bid: Option<String>,
    pub bid_size: Option<String>,
    pub ask: Option<String>,
    pub ask_size: Option<String>,
}

#[derive(Debug, Default, Clone, Serialize)]
pub struct Stats {
    pub events: u64,
    pub samples: u64,
    pub markets: u64,
    /// REAL sequence breaks: a delta arrived out of order on a synced book.
    /// These are the ones that mean lost data.
    pub gaps: u64,
    /// Deltas that arrived before any snapshot for that market, so there was no
    /// book to apply them to. This is normally NOT damage — it is the day
    /// boundary. The recorder writes one file per UTC day, but book state
    /// carries across midnight, so a day opens mid-stream: the busiest Kalshi
    /// market on 2026-07-26 begins with `delta seq=522` and its deltas are
    /// contiguous (2 breaks in 111,561). A market's series therefore starts at
    /// its first snapshot WITHIN the day, not at midnight. Counted separately
    /// because conflating it with `gaps` makes a healthy tape look 43% broken.
    pub not_synced: u64,
    pub parse_failures: u64,
}

#[derive(Default)]
struct MarketState {
    last_emit_ns: i64,
    last: Option<(Option<String>, Option<String>)>, // (bid, ask) prices only
}

/// Replay one venue's day and write TOB samples as JSONL to `out`.
pub fn build_source(
    src: &Source,
    interval_ns: i64,
    out: &mut impl Write,
) -> Result<Stats, String> {
    let mut books = BookBuilder::new();
    let mut state: HashMap<(String, String), MarketState> = HashMap::new();
    let mut st = Stats::default();

    let on_event = |ev: TapeEvent,
                    books: &mut BookBuilder,
                    state: &mut HashMap<(String, String), MarketState>,
                    st: &mut Stats,
                    out: &mut dyn Write|
     -> Result<(), String> {
        st.events += 1;
        let venue = ev.venue();
        let market = ev.market_id().to_string();
        let ts = event_ts(&ev);
        match books.apply_event(&ev) {
            Ok(()) => {}
            Err(ApplyError::NotSynced) => {
                st.not_synced += 1;
                return Ok(());
            }
            Err(ApplyError::GapDetected { .. }) => {
                // The book is dropped; it recovers at the next snapshot.
                st.gaps += 1;
                return Ok(());
            }
        }
        let Some(book) = books.get(venue, &market) else { return Ok(()) };
        let bid = book.bids.first().map(|l| l.price.clone());
        let bid_sz = book.bids.first().map(|l| l.size.clone());
        let ask = book.asks.first().map(|l| l.price.clone());
        let ask_sz = book.asks.first().map(|l| l.size.clone());

        let key = (venue.as_str().to_string(), market.clone());
        let ms = state.entry(key).or_default();
        let changed = ms.last.as_ref() != Some(&(bid.clone(), ask.clone()));
        // A market's FIRST observation always emits. Without this the interval
        // gate suppresses it whenever `ts` is small relative to the interval,
        // because `last_emit_ns` starts at 0 — invisible against real epoch
        // timestamps, but wrong, and it would silently drop the opening quote
        // of any market whose clock starts near zero.
        let first = ms.last.is_none();
        if changed && (first || ts - ms.last_emit_ns >= interval_ns) {
            ms.last = Some((bid.clone(), ask.clone()));
            ms.last_emit_ns = ts;
            let s = TobSample {
                venue: venue.as_str().to_string(),
                market_id: market,
                ts_local_ns: ts,
                bid,
                bid_size: bid_sz,
                ask,
                ask_size: ask_sz,
            };
            let line = serde_json::to_string(&s).map_err(|e| e.to_string())?;
            out.write_all(line.as_bytes()).map_err(|e| e.to_string())?;
            out.write_all(b"\n").map_err(|e| e.to_string())?;
            st.samples += 1;
        }
        Ok(())
    };

    match src {
        Source::Jsonl(p) => {
            // Streamed: a raw PM-US day is ~6.5 GB, so reading it whole is not
            // an option the way it is for the much smaller scan tape.
            let f = std::fs::File::open(p).map_err(|e| format!("open {}: {e}", p.display()))?;
            for line in BufReader::new(f).lines() {
                let Ok(line) = line else { continue };
                let t = line.trim_matches(char::from(0)).trim();
                if t.is_empty() {
                    continue;
                }
                match serde_json::from_str::<TapeEvent>(t) {
                    Ok(ev) => on_event(ev, &mut books, &mut state, &mut st, out)?,
                    Err(_) => st.parse_failures += 1,
                }
            }
        }
        Source::Parquet(p) => {
            for ev in raw::iter_parquet(p)? {
                match ev {
                    Ok(ev) => on_event(ev, &mut books, &mut state, &mut st, out)?,
                    Err(_) => st.parse_failures += 1,
                }
            }
        }
    }

    st.markets = state.len() as u64;
    Ok(st)
}

fn event_ts(ev: &TapeEvent) -> i64 {
    match ev {
        TapeEvent::Snapshot { ts_local_ns, .. }
        | TapeEvent::Delta { ts_local_ns, .. }
        | TapeEvent::Trade { ts_local_ns, .. } => *ts_local_ns,
    }
}

/// Build every venue's ToB series for one day, in parallel, writing to
/// `out_dir`. Shared by the CLI and the dashboard's on-demand trigger so the
/// two cannot drift.
///
/// Writes go to `<name>.tmp` and are RENAMED into place. A reader must never
/// see a half-built series, and rename is atomic on the same filesystem — this
/// matters precisely because the dashboard reads these files while a rebuild
/// may be running.
///
/// Cost, measured 2026-07-27 on real tape: ~30s wall, ~2 cores, under 40 MB
/// RSS, ~10 MB output per day — for 16-20M events across 6.7 GB of JSONL. It
/// streams, so memory does not scale with the tape.
pub fn build_day(
    raw_dir: &str,
    parquet_dir: &str,
    out_dir: &str,
    day: &str,
    interval_ns: i64,
    venues: &[&str],
) -> Result<Vec<(String, Stats)>, String> {
    std::fs::create_dir_all(out_dir).map_err(|e| format!("create {out_dir}: {e}"))?;
    let results: Vec<(String, Result<Stats, String>)> = std::thread::scope(|s| {
        let hs: Vec<_> = venues
            .iter()
            .map(|venue| {
                s.spawn(move || {
                    let Some(src) = arb_query::source_for(raw_dir, parquet_dir, venue, day) else {
                        return (venue.to_string(), Err("no tape for this day".to_string()));
                    };
                    let final_path = format!("{out_dir}/tob-{venue}-{day}.jsonl");
                    let tmp = format!("{final_path}.tmp");
                    let r = (|| -> Result<Stats, String> {
                        let f = std::fs::File::create(&tmp)
                            .map_err(|e| format!("create {tmp}: {e}"))?;
                        let mut w = std::io::BufWriter::new(f);
                        let st = build_source(&src, interval_ns, &mut w)?;
                        w.flush().map_err(|e| e.to_string())?;
                        drop(w);
                        std::fs::rename(&tmp, &final_path)
                            .map_err(|e| format!("rename {tmp}: {e}"))?;
                        Ok(st)
                    })();
                    if r.is_err() {
                        let _ = std::fs::remove_file(&tmp);
                    }
                    (venue.to_string(), r)
                })
            })
            .collect();
        hs.into_iter().filter_map(|h| h.join().ok()).collect()
    });

    let mut out = Vec::new();
    for (venue, r) in results {
        match r {
            Ok(st) => out.push((venue, st)),
            // A venue with no tape for the day is normal, not fatal.
            Err(e) if e.starts_with("no tape") => {}
            Err(e) => return Err(format!("{venue}: {e}")),
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jsonl(lines: &[&str], tag: &str) -> std::path::PathBuf {
        let p = std::env::temp_dir()
            .join(format!("arb-tob-{}-{tag}.jsonl", std::process::id()));
        std::fs::write(&p, lines.join("\n")).unwrap();
        p
    }

    fn samples(out: &[u8]) -> Vec<TobSample> {
        String::from_utf8_lossy(out)
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    const SNAP: &str = r#"{"kind":"snapshot","venue":"kalshi","market_id":"M","bids":[{"price":"0.20","size":"10"}],"asks":[{"price":"0.22","size":"5"}],"seq":1,"ts_local_ns":0,"ts_venue":null}"#;

    #[test]
    fn emits_on_change_and_respects_the_interval() {
        // seq 2 moves the bid at t=30s (inside the interval -> suppressed),
        // seq 3 moves it again at t=120s (past the interval -> emitted).
        let p = jsonl(
            &[
                SNAP,
                r#"{"kind":"delta","venue":"kalshi","market_id":"M","side":"bid","price":"0.21","size":"7","seq":2,"ts_local_ns":30000000000,"ts_venue":null}"#,
                r#"{"kind":"delta","venue":"kalshi","market_id":"M","side":"bid","price":"0.23","size":"3","seq":3,"ts_local_ns":120000000000,"ts_venue":null}"#,
            ],
            "interval",
        );
        let mut out = Vec::new();
        let st = build_source(&Source::Jsonl(p.clone()), DEFAULT_INTERVAL_NS, &mut out)
            .expect("builds");
        let got = samples(&out);
        assert_eq!(st.events, 3);
        assert_eq!(got.len(), 2, "snapshot + the late move only: {got:?}");
        assert_eq!(got[0].bid.as_deref(), Some("0.20"));
        assert_eq!(got[0].ask.as_deref(), Some("0.22"));
        assert_eq!(got[1].ts_local_ns, 120_000_000_000);
        assert_eq!(got[1].bid.as_deref(), Some("0.23"));
        let _ = std::fs::remove_file(p);
    }

    #[test]
    fn a_quiet_market_costs_nothing_after_its_first_sample() {
        // Same book restated far apart in time: no change, so no new rows.
        let p = jsonl(
            &[
                SNAP,
                r#"{"kind":"snapshot","venue":"kalshi","market_id":"M","bids":[{"price":"0.20","size":"10"}],"asks":[{"price":"0.22","size":"5"}],"seq":9,"ts_local_ns":600000000000,"ts_venue":null}"#,
            ],
            "quiet",
        );
        let mut out = Vec::new();
        build_source(&Source::Jsonl(p.clone()), DEFAULT_INTERVAL_NS, &mut out).expect("builds");
        assert_eq!(samples(&out).len(), 1);
        let _ = std::fs::remove_file(p);
    }

    /// The cross-crate half of the resolver fix, and the one that was easy to
    /// miss because it lives in neither file that was edited: `POST
    /// /api/rollup --day 2026-07-27` was BROKEN before it.
    ///
    /// A zero-byte `kalshi-2026-07-27.parquet` outranked the good 128 MB raw
    /// tape beside it, `iter_parquet` failed on the empty file, and `build_day`
    /// returns `Err` for the WHOLE day on any venue's error — so one broken
    /// archive took out every venue's series with the raw tape sitting there
    /// readable. Only "no tape for this day" is tolerated, and this was not
    /// that.
    #[test]
    fn a_broken_archive_does_not_take_the_whole_day_down_with_it() {
        let base = std::env::temp_dir().join(format!("arb-tob-day-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        for d in ["raw", "parquet", "out"] {
            std::fs::create_dir_all(base.join(d)).unwrap();
        }
        std::fs::write(base.join("raw/kalshi-2026-07-27.jsonl"), SNAP).unwrap();
        std::fs::write(base.join("parquet/kalshi-2026-07-27.parquet"), b"").unwrap();
        let dir = |d: &str| base.join(d).to_string_lossy().to_string();

        let got = build_day(
            &dir("raw"),
            &dir("parquet"),
            &dir("out"),
            "2026-07-27",
            DEFAULT_INTERVAL_NS,
            &["kalshi"],
        )
        .expect("the raw tape is right there");
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].1.samples, 1, "built from the raw tape, not the empty archive");
        let _ = std::fs::remove_dir_all(&base);
    }

    #[test]
    fn damaged_lines_are_counted_not_fatal() {
        let p = jsonl(&[SNAP, "\0\0\0", "{not json", ""], "damaged");
        let mut out = Vec::new();
        let st = build_source(&Source::Jsonl(p.clone()), DEFAULT_INTERVAL_NS, &mut out)
            .expect("builds despite damage");
        // The NUL run trims to empty and is skipped as a blank line; only the
        // torn JSON counts as a parse failure. Both are non-fatal, which is
        // the property under test.
        assert_eq!(st.parse_failures, 1, "torn line");
        assert_eq!(samples(&out).len(), 1);
        let _ = std::fs::remove_file(p);
    }
}
