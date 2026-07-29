//! Two-tape comparison — the semantics of `scripts/shadow_gate.py`, ported.
//!
//! The Python gate's RESULT line failed on exactly ONE condition: a Rust line
//! its `parse_event` could not read. Volume and TOB were printed and judged by
//! eye ("poll-mode lags are expected"). This port keeps that judgement split
//! and leaves volume/TOB advisory.
//!
//! Of the two things gated on THIS side, one is a clause of M1's gate in
//! `docs/migration-plan.md` (parse-compat) and one is a repair of a clause that
//! could not be checked as written: "gap counter <= Python's" compares two
//! numbers that do not measure the same thing, so what is gated is `rust == 0`.
//! The reasoning is in `main.rs`, at the check. The MARKET UNIVERSE is not
//! gated here at all — the plan does not ask for it, and asking it of a deduped
//! tape is how this gate cried wolf on 66 markets that were never missing. The
//! welcome burst answers it in `live.rs`.
//!
//! `undecodable` and `bad_field` are both counted over the WHOLE TAIL SLICE and
//! not over the time window. They have to be: an undecodable line has no
//! readable timestamp, so it cannot be placed in a window at all, and counting
//! its partner after the window filter meant the two numbers on the report
//! covered different spans while the gate ORed them together.
//!
//! ONE CHECK IS NEW, and it is the check that matters. Python's `parse_event`
//! rejected the Rust PM-US `trade` lines because pydantic could not coerce
//! `size` to a Decimal. Rust cannot reproduce that by deserializing alone:
//! `TapeEvent::Trade.size` is a `String`, so
//! `"size":"{\"currency\":\"USD\",\"value\":\"4.0000\"}"` round-trips happily
//! through both `serde_json` and `arb-recorder --parse-check`. A faithful port
//! that only counted decode failures would be GREEN on the very corruption the
//! last real Python run was RED on. So every decimal-shaped field is parsed
//! with `Dec` as well, and a field that is not a number is `bad_field`.

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::dec::Dec;
use arb_core::model::TapeEvent;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;

pub const VENUES: [&str; 3] = ["kalshi", "polymarket", "polymarket_us"];

/// bucket -> market -> (best_bid, best_ask)
pub type Samples = BTreeMap<i64, HashMap<String, (String, String)>>;

#[derive(Default, Debug, PartialEq, Eq)]
pub struct TapeStats {
    /// Lines read from the tail slice, before the time-window filter.
    pub lines: u64,
    pub in_window: u64,
    pub snapshot: u64,
    pub delta: u64,
    pub trade: u64,
    /// Did not deserialize into a `TapeEvent` at all. Over the whole slice.
    pub undecodable: u64,
    /// Deserialized, but a price/size field is not a decimal number. Over the
    /// whole slice, like `undecodable` and unlike everything else here.
    pub bad_field: u64,
    /// `GapDetected` — a sequence hole. The counter `migration-plan.md` names.
    pub gaps: u64,
    /// `NotSynced` — a delta for a market whose snapshot is older than the
    /// window. EXPECTED on a tail slice and never gated on; it is reported so
    /// that a gap count of zero cannot be mistaken for a healthy replay when in
    /// truth nothing ever synced.
    pub unsynced: u64,
    pub first_ts_ns: i64,
    pub last_ts_ns: i64,
    pub markets: HashSet<String>,
}

impl TapeStats {
    pub fn covered_s(&self) -> f64 {
        if self.in_window == 0 {
            return 0.0;
        }
        (self.last_ts_ns - self.first_ts_ns) as f64 / 1e9
    }
}

fn dec_ok(s: &str) -> bool {
    Dec::parse(s).is_ok()
}

/// Every field the venue sends as a number, checked as one. A `String` field
/// holding a JSON object is the defect this exists to name.
pub fn decimal_fields_ok(ev: &TapeEvent) -> bool {
    match ev {
        TapeEvent::Snapshot { bids, asks, .. } => {
            bids.iter().chain(asks.iter()).all(|l| dec_ok(&l.price) && dec_ok(&l.size))
        }
        TapeEvent::Delta { price, size, .. } | TapeEvent::Trade { price, size, .. } => {
            dec_ok(price) && dec_ok(size)
        }
    }
}

pub fn ts_of(ev: &TapeEvent) -> i64 {
    match ev {
        TapeEvent::Snapshot { ts_local_ns, .. }
        | TapeEvent::Delta { ts_local_ns, .. }
        | TapeEvent::Trade { ts_local_ns, .. } => *ts_local_ns,
    }
}

/// `ts_local_ns` of the last line that PARSES, read from the file's last 64 KiB.
///
/// The last line of a live tape is being written under us, so "the last line"
/// is not necessarily a line. Taking the newest one that parses is both the
/// torn-line tolerance and the answer.
pub fn last_ts(path: &Path) -> std::io::Result<Option<i64>> {
    let f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let mut r = BufReader::new(f);
    r.seek(SeekFrom::Start(len.saturating_sub(64 * 1024)))?;
    let mut last = None;
    for line in r.lines() {
        if let Ok(ev) = serde_json::from_str::<TapeEvent>(&line?) {
            last = Some(ts_of(&ev));
        }
    }
    Ok(last)
}

/// Replay the tail of one tape, keeping only events inside `window`.
///
/// The slice is bounded by BYTES because these tapes reach 6 GB a day and a
/// gate that reads them whole cannot run beside an armed engine. A window that
/// the byte bound could not reach back to shows up as a short `covered_s`,
/// which the caller compares between the two sides rather than assuming.
pub fn replay(
    path: &Path,
    window: (i64, i64),
    tail_bytes: u64,
    sample_ns: i64,
) -> std::io::Result<(TapeStats, Samples)> {
    let mut stats = TapeStats::default();
    let mut samples: Samples = BTreeMap::new();
    let mut books = BookBuilder::new();
    let f = std::fs::File::open(path)?;
    let len = f.metadata()?.len();
    let mut r = BufReader::new(f);
    if len > tail_bytes {
        r.seek(SeekFrom::Start(len - tail_bytes))?;
        let mut partial = String::new();
        r.read_line(&mut partial)?;
    }
    // A live tape's final line is written under our feet, so a decode failure
    // there is torn, not corrupt. Hold the last failure back and only count it
    // once another line proves it was not the end.
    let mut held_undecodable = false;
    for line in r.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        stats.lines += 1;
        if held_undecodable {
            stats.undecodable += 1;
            held_undecodable = false;
        }
        let ev: TapeEvent = match serde_json::from_str(&line) {
            Ok(ev) => ev,
            Err(_) => {
                held_undecodable = true;
                continue;
            }
        };
        // BEFORE the window filter, so that this counter and `undecodable`
        // cover the same span — the gate ORs them into one parse-compat
        // verdict, and two counters over two different spans is not one
        // verdict.
        if !decimal_fields_ok(&ev) {
            stats.bad_field += 1;
        }
        let ts = ts_of(&ev);
        if ts < window.0 || ts > window.1 {
            continue;
        }
        stats.in_window += 1;
        if stats.first_ts_ns == 0 {
            stats.first_ts_ns = ts;
        }
        stats.last_ts_ns = ts;
        match &ev {
            TapeEvent::Snapshot { .. } => stats.snapshot += 1,
            TapeEvent::Delta { .. } => stats.delta += 1,
            TapeEvent::Trade { .. } => stats.trade += 1,
        }
        stats.markets.insert(ev.market_id().to_owned());
        match books.apply_event(&ev) {
            Ok(()) => {}
            Err(ApplyError::GapDetected { .. }) => {
                stats.gaps += 1;
                continue;
            }
            Err(ApplyError::NotSynced) => {
                stats.unsynced += 1;
                continue;
            }
        }
        if let Some(b) = books.get(ev.venue(), ev.market_id()) {
            if let (Some(bid), Some(ask)) = (b.bids.first(), b.asks.first()) {
                samples
                    .entry(ts / sample_ns)
                    .or_default()
                    .insert(ev.market_id().to_owned(), (bid.price.clone(), ask.price.clone()));
            }
        }
    }
    Ok((stats, samples))
}

/// Carry-forward align the two sample streams and compare the closest buckets —
/// the Python gate's `tob_agreement`, verbatim in behaviour.
///
/// f64, not `Dec`: this number is advisory (the Python gate printed it and let
/// a human judge it), the tolerance is a tenth of a cent, and feed prices carry
/// four decimals — far inside a double's exact range.
pub fn tob_agreement(
    py: &Samples,
    rs: &Samples,
    tolerance: f64,
) -> (u64, u64, Vec<(f64, String, i64)>) {
    let (mut agree, mut differ) = (0u64, 0u64);
    let mut worst: Vec<(f64, String, i64)> = Vec::new();
    let mut last_rs: HashMap<String, (String, String)> = HashMap::new();
    let buckets: std::collections::BTreeSet<i64> = py.keys().chain(rs.keys()).copied().collect();
    for bucket in buckets {
        if let Some(m) = rs.get(&bucket) {
            for (mkt, tob) in m {
                last_rs.insert(mkt.clone(), tob.clone());
            }
        }
        let Some(m) = py.get(&bucket) else { continue };
        for (mkt, (pb, pa)) in m {
            let Some((rb, ra)) = last_rs.get(mkt) else { continue };
            let f = |s: &String| s.parse::<f64>().unwrap_or(f64::NAN);
            let (db, da) = ((f(pb) - f(rb)).abs(), (f(pa) - f(ra)).abs());
            if db <= tolerance && da <= tolerance {
                agree += 1;
            } else {
                differ += 1;
                worst.push((db.max(da), mkt.clone(), bucket));
            }
        }
    }
    worst.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    worst.truncate(5);
    (agree, differ, worst)
}

/// Markets the Python tape saw in the window and the Rust tape did not.
///
/// The cutover evidence says the Rust recorder captures a strict SUPERSET.
/// That is a claim about coverage and this is the check that keeps it true;
/// the reverse direction (Rust-only markets) is reported but never gates.
pub fn missing_from_rs(py: &TapeStats, rs: &TapeStats) -> Vec<String> {
    let mut v: Vec<String> = py.markets.difference(&rs.markets).cloned().collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The line the LIVE Rust recorder wrote on 2026-07-29, copied verbatim.
    /// `size` is a JSON object that has been stringified into a decimal field.
    const REAL_PMUS_TRADE: &str = r#"{"kind":"trade","venue":"polymarket_us","market_id":"rdc-boe-2026-07-30-hike25","price":"0.0100","size":"{\"currency\":\"USD\",\"value\":\"4.0000\"}","taker_side":"sell","seq":11,"ts_local_ns":1785283214334476739,"ts_venue":"2026-07-29T00:00:14.041369725Z"}"#;

    const GOOD_TRADE: &str = r#"{"kind":"trade","venue":"polymarket_us","market_id":"rdc-boe-2026-07-30-hike25","price":"0.0100","size":"4.0000","taker_side":"sell","seq":11,"ts_local_ns":1785283214334476739,"ts_venue":null}"#;

    /// The whole reason this gate does not stop at `serde_json`. If this ever
    /// starts failing because the line no longer DESERIALIZES, the decimal
    /// check has become redundant — until then it is the only thing that sees
    /// this corruption.
    #[test]
    fn a_stringified_money_object_deserializes_fine_and_is_still_not_a_number() {
        let ev: TapeEvent = serde_json::from_str(REAL_PMUS_TRADE).expect("decodes as TapeEvent");
        assert!(!decimal_fields_ok(&ev), "a JSON object in `size` must not read as a decimal");
        let ok: TapeEvent = serde_json::from_str(GOOD_TRADE).expect("decodes");
        assert!(decimal_fields_ok(&ok));
    }

    fn write(dir: &Path, name: &str, lines: &[&str]) -> std::path::PathBuf {
        let p = dir.join(name);
        std::fs::write(&p, format!("{}\n", lines.join("\n"))).expect("write");
        p
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-sgate-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&d).expect("tmpdir");
        d
    }

    fn snap(mkt: &str, seq: u64, ts: i64, bid: &str, ask: &str) -> String {
        format!(
            r#"{{"kind":"snapshot","venue":"kalshi","market_id":"{mkt}","bids":[{{"price":"{bid}","size":"10"}}],"asks":[{{"price":"{ask}","size":"10"}}],"seq":{seq},"ts_local_ns":{ts},"ts_venue":null}}"#
        )
    }

    #[test]
    fn a_tape_full_of_stringified_money_is_counted_as_bad_fields() {
        let dir = tmpdir("badfield");
        let p = write(&dir, "t.jsonl", &[REAL_PMUS_TRADE, GOOD_TRADE]);
        let (st, _) = replay(&p, (0, i64::MAX), u64::MAX, 1_000_000_000).expect("replay");
        assert_eq!(st.trade, 2);
        assert_eq!(st.undecodable, 0, "both lines are valid JSON — decode alone sees nothing");
        assert_eq!(st.bad_field, 1);
        // Both parse-compat counters cover the SLICE, not the window. A corrupt
        // line outside the window is still a corrupt line, and the gate ORs
        // these two together — counting them over different spans made one
        // verdict out of two measurements of different things.
        let (out, _) = replay(&p, (0, 1), u64::MAX, 1_000_000_000).expect("replay");
        assert_eq!(out.in_window, 0, "the window excludes both lines");
        assert_eq!(out.bad_field, 1, "and the corrupt one is still counted");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_market_the_python_tape_has_and_rust_does_not_is_reported() {
        let dir = tmpdir("universe");
        let py = write(&dir, "py.jsonl", &[&snap("A", 1, 10, "0.40", "0.60"), &snap("B", 1, 11, "0.10", "0.20")]);
        let rs = write(&dir, "rs.jsonl", &[&snap("A", 1, 10, "0.40", "0.60")]);
        let (pys, _) = replay(&py, (0, i64::MAX), u64::MAX, 1_000_000_000).expect("py");
        let (rss, _) = replay(&rs, (0, i64::MAX), u64::MAX, 1_000_000_000).expect("rs");
        assert_eq!(missing_from_rs(&pys, &rss), vec!["B".to_string()]);
        assert!(missing_from_rs(&rss, &pys).is_empty());
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn a_sequence_hole_counts_as_a_gap_and_the_window_bounds_the_replay() {
        let dir = tmpdir("gaps");
        let d = |seq: u64, ts: i64| {
            format!(
                r#"{{"kind":"delta","venue":"kalshi","market_id":"A","side":"bid","price":"0.41","size":"5","seq":{seq},"ts_local_ns":{ts},"ts_venue":null}}"#
            )
        };
        let p = write(
            &dir,
            "t.jsonl",
            &[&snap("A", 1, 100, "0.40", "0.60"), &d(2, 200), &d(9, 300), &d(10, 400)],
        );
        let (all, _) = replay(&p, (0, i64::MAX), u64::MAX, 1_000_000_000).expect("all");
        assert_eq!((all.in_window, all.gaps, all.unsynced), (4, 1, 1));
        // seq 9 gapped and REMOVED the book, so seq 10 is NotSynced — one gap,
        // one unsynced, which is what a real hole looks like downstream.
        let (win, _) = replay(&p, (150, 250), u64::MAX, 1_000_000_000).expect("win");
        assert_eq!(win.in_window, 1, "the time window bounds what is compared");
        std::fs::remove_dir_all(dir).ok();
    }

    #[test]
    fn tob_agreement_carries_the_rust_side_forward() {
        let mut py: Samples = BTreeMap::new();
        let mut rs: Samples = BTreeMap::new();
        py.entry(1).or_default().insert("A".into(), ("0.40".into(), "0.60".into()));
        py.entry(2).or_default().insert("A".into(), ("0.40".into(), "0.60".into()));
        // Rust only sampled bucket 1; bucket 2 must carry forward and agree.
        rs.entry(1).or_default().insert("A".into(), ("0.40".into(), "0.60".into()));
        let (agree, differ, _) = tob_agreement(&py, &rs, 0.01);
        assert_eq!((agree, differ), (2, 0));

        let mut rs2: Samples = BTreeMap::new();
        rs2.entry(1).or_default().insert("A".into(), ("0.30".into(), "0.60".into()));
        let (agree, differ, worst) = tob_agreement(&py, &rs2, 0.01);
        assert_eq!((agree, differ), (0, 2));
        assert!((worst[0].0 - 0.10).abs() < 1e-9, "worst diff is the bid gap: {worst:?}");
    }
}
