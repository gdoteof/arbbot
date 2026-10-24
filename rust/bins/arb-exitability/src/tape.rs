//! The archive, replayed into one-minute top-of-book grids.
//!
//! Each venue's days are folded IN ORDER through one `BookBuilder`, so a book
//! synced on Monday is still synced on Tuesday's first delta. A venue with no
//! readable file for a day has unknown books that day: its builder is reset and
//! every market reads as missing until the next snapshot, rather than carrying
//! Monday's touch through a day nobody recorded.
//!
//! Grid convention: `bid[k]`/`ask[k]` are the top of book at the START of minute
//! `k`, before any of that minute's events. `lo[k]`/`hi[k]` are the lowest and
//! highest trade prints DURING minute `k`. A quote rested on minute `k`'s touch
//! therefore meets exactly the trades that could have filled it.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufRead, BufReader};

use arb_core::book::BookBuilder;
use arb_core::model::{BookSide, Level, TapeEvent, Venue};
use arb_query::Source;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::RowAccessor;
use parquet::schema::parser::parse_message_type;

pub const MINUTE_NS: i64 = 60_000_000_000;
pub const DAY_MINUTES: usize = 1440;

/// One market's minute grid. `NaN` is "unknown": no synced book, or no trade.
pub struct Series {
    pub bid: Vec<f32>,
    pub ask: Vec<f32>,
    pub lo: Vec<f32>,
    pub hi: Vec<f32>,
}

impl Series {
    fn new(n: usize) -> Self {
        Series {
            bid: vec![f32::NAN; n],
            ask: vec![f32::NAN; n],
            lo: vec![f32::NAN; n],
            hi: vec![f32::NAN; n],
        }
    }
}

/// Physical schema subset of the archived venue tape. Must match the writer's
/// types exactly or the projection is rejected.
const PROJECTION: &str = "
message duckdb_schema {
  OPTIONAL BYTE_ARRAY kind (UTF8);
  OPTIONAL BYTE_ARRAY market_id (UTF8);
  OPTIONAL BYTE_ARRAY side (UTF8);
  OPTIONAL BYTE_ARRAY price (UTF8);
  OPTIONAL BYTE_ARRAY size (UTF8);
  OPTIONAL BYTE_ARRAY bids (UTF8);
  OPTIONAL BYTE_ARRAY asks (UTF8);
  OPTIONAL INT64 seq (INT_64);
  OPTIONAL INT64 ts_local_ns (INT_64);
}
";

/// Per-market fill state while one venue is folded.
struct Cursor {
    bid: f32,
    ask: f32,
    /// The next grid index not yet written.
    filled_to: usize,
}

struct Fold<'a> {
    t0_ns: i64,
    n: usize,
    books: BookBuilder,
    series: HashMap<String, Series>,
    cursors: HashMap<String, Cursor>,
    venue: Venue,
    tracked: &'a HashSet<String>,
}

impl Fold<'_> {
    fn minute(&self, ts_ns: i64) -> Option<usize> {
        let k = ts_ns.checked_sub(self.t0_ns)? / MINUTE_NS;
        usize::try_from(k).ok().filter(|k| *k < self.n)
    }

    /// Write the current touch into every minute up to and including `k`.
    fn fill_through(&mut self, market: &str, k: usize) {
        let (Some(c), Some(s)) = (self.cursors.get_mut(market), self.series.get_mut(market)) else { return };
        let end = (k + 1).min(self.n);
        if c.filled_to < end {
            s.bid[c.filled_to..end].fill(c.bid);
            s.ask[c.filled_to..end].fill(c.ask);
            c.filled_to = end;
        }
    }

    fn trade(&mut self, market: &str, ts_ns: i64, price: &str) {
        let (Some(k), Ok(p)) = (self.minute(ts_ns), price.parse::<f32>()) else { return };
        if let Some(s) = self.series.get_mut(market) {
            s.lo[k] = if s.lo[k].is_nan() { p } else { s.lo[k].min(p) };
            s.hi[k] = if s.hi[k].is_nan() { p } else { s.hi[k].max(p) };
        }
    }

    fn book(&mut self, ev: &TapeEvent, market: &str, ts_ns: i64) {
        if let Some(k) = self.minute(ts_ns) {
            self.fill_through(market, k);
        }
        // A gap removes the book; the touch then reads as unknown until the
        // venue's next snapshot resyncs it.
        let _ = self.books.apply_event(ev);
        let top = self.books.get(self.venue, market).map(|b| {
            let px = |l: Option<&Level>| l.and_then(|l| l.price.parse::<f32>().ok()).unwrap_or(f32::NAN);
            (px(b.bids.first()), px(b.asks.first()))
        });
        if let Some(c) = self.cursors.get_mut(market) {
            (c.bid, c.ask) = top.unwrap_or((f32::NAN, f32::NAN));
        }
    }

    /// A day with no readable file: forget every book and mark them unknown.
    fn blank_from(&mut self, k: usize) {
        let markets: Vec<String> = self.cursors.keys().cloned().collect();
        for m in &markets {
            if k > 0 {
                self.fill_through(m, k - 1);
            }
            self.books.remove(self.venue, m);
            if let Some(c) = self.cursors.get_mut(m) {
                (c.bid, c.ask) = (f32::NAN, f32::NAN);
            }
        }
    }

    fn finish(mut self) -> HashMap<String, Series> {
        let markets: Vec<String> = self.cursors.keys().cloned().collect();
        for m in &markets {
            self.fill_through(m, self.n.saturating_sub(1));
        }
        self.series
    }
}

/// Every tracked market of one venue over `days`, as minute grids starting at
/// `t0_ns` (the first day's 00:00 UTC).
pub fn load_venue(
    venue: Venue,
    days: &[String],
    jsonl_dir: &str,
    parquet_dir: &str,
    tracked: &HashSet<String>,
    t0_ns: i64,
) -> Result<(HashMap<String, Series>, Vec<String>), String> {
    let n = days.len() * DAY_MINUTES;
    let mut fold = Fold {
        t0_ns,
        n,
        books: BookBuilder::new(),
        series: tracked.iter().map(|m| (m.clone(), Series::new(n))).collect(),
        cursors: tracked
            .iter()
            .map(|m| (m.clone(), Cursor { bid: f32::NAN, ask: f32::NAN, filled_to: 0 }))
            .collect(),
        venue,
        tracked,
    };
    let mut missing = Vec::new();
    for (d, day) in days.iter().enumerate() {
        match arb_query::source_for(jsonl_dir, parquet_dir, venue.as_str(), day) {
            Some(Source::Parquet(p)) => fold_parquet(&mut fold, &p)?,
            Some(Source::Jsonl(p)) => fold_jsonl(&mut fold, &p)?,
            None => {
                missing.push(day.clone());
                fold.blank_from(d * DAY_MINUTES);
            }
        }
    }
    Ok((fold.finish(), missing))
}

fn fold_parquet(fold: &mut Fold, path: &std::path::Path) -> Result<(), String> {
    let at = |e: &dyn std::fmt::Display| format!("{}: {e}", path.display());
    let proj = parse_message_type(PROJECTION).map_err(|e| format!("projection: {e}"))?;
    let reader = SerializedFileReader::new(File::open(path).map_err(|e| at(&e))?).map_err(|e| at(&e))?;
    for row in reader.get_row_iter(Some(proj)).map_err(|e| at(&e))? {
        let row = row.map_err(|e| at(&e))?;
        let Ok(market) = row.get_string(1) else { continue };
        if !fold.tracked.contains(market.as_str()) {
            continue;
        }
        let market = market.clone();
        let ts = row.get_long(8).unwrap_or(0);
        let text = |i: usize| row.get_string(i).ok().cloned().unwrap_or_default();
        let ev = match row.get_string(0).map(String::as_str) {
            Ok("trade") => {
                fold.trade(&market, ts, &text(3));
                continue;
            }
            Ok("snapshot") => {
                let levels = |i: usize| serde_json::from_str::<Vec<Level>>(&text(i)).unwrap_or_default();
                TapeEvent::Snapshot {
                    venue: fold.venue,
                    market_id: market.clone(),
                    bids: levels(5),
                    asks: levels(6),
                    seq: row.get_long(7).unwrap_or(0).max(0) as u64,
                    ts_local_ns: ts,
                    ts_venue: None,
                }
            }
            Ok("delta") => {
                let Some(side) = BookSide::parse(&text(2)) else { continue };
                TapeEvent::Delta {
                    venue: fold.venue,
                    market_id: market.clone(),
                    side,
                    price: text(3),
                    size: text(4),
                    seq: row.get_long(7).unwrap_or(0).max(0) as u64,
                    ts_local_ns: ts,
                    ts_venue: None,
                }
            }
            _ => continue,
        };
        fold.book(&ev, &market, ts);
    }
    Ok(())
}

/// The fallback for a day whose archive is missing or incomplete. These files
/// run to a gigabyte, so they are streamed, and a line is only parsed once its
/// market id is known to be tracked.
fn fold_jsonl(fold: &mut Fold, path: &std::path::Path) -> Result<(), String> {
    let f = File::open(path).map_err(|e| format!("{}: {e}", path.display()))?;
    for line in BufReader::new(f).lines() {
        let line = line.map_err(|e| format!("{}: {e}", path.display()))?;
        let Some(market) = market_of(&line) else { continue };
        if !fold.tracked.contains(market) {
            continue;
        }
        let Ok(ev) = serde_json::from_str::<TapeEvent>(&line) else { continue };
        let market = market.to_string();
        match &ev {
            TapeEvent::Trade { price, ts_local_ns, .. } => fold.trade(&market, *ts_local_ns, price),
            TapeEvent::Snapshot { ts_local_ns, .. } | TapeEvent::Delta { ts_local_ns, .. } => {
                let ts = *ts_local_ns;
                fold.book(&ev, &market, ts);
            }
        }
    }
    Ok(())
}

/// The `market_id` value of one tape line, without parsing the line.
fn market_of(line: &str) -> Option<&str> {
    const KEY: &str = "\"market_id\":\"";
    let start = line.find(KEY)? + KEY.len();
    let len = line.get(start..)?.find('"')?;
    line.get(start..start + len)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fold(tracked: &HashSet<String>, n: usize) -> Fold<'_> {
        Fold {
            t0_ns: 0,
            n,
            books: BookBuilder::new(),
            series: tracked.iter().map(|m| (m.clone(), Series::new(n))).collect(),
            cursors: tracked
                .iter()
                .map(|m| (m.clone(), Cursor { bid: f32::NAN, ask: f32::NAN, filled_to: 0 }))
                .collect(),
            venue: Venue::Kalshi,
            tracked,
        }
    }

    fn snap(bid: &str, ask: &str, seq: u64, ts: i64) -> TapeEvent {
        let l = |p: &str| vec![Level { price: p.into(), size: "10".into() }];
        TapeEvent::Snapshot {
            venue: Venue::Kalshi,
            market_id: "M".into(),
            bids: l(bid),
            asks: l(ask),
            seq,
            ts_local_ns: ts,
            ts_venue: None,
        }
    }

    /// Minute k holds the touch as it stood when minute k OPENED, so a change
    /// part-way through minute 1 first shows at minute 2.
    #[test]
    fn a_minute_holds_the_touch_it_opened_with() {
        let tracked: HashSet<String> = ["M".to_string()].into();
        let mut f = fold(&tracked, 4);
        f.book(&snap("0.40", "0.45", 1, 10), "M", 10);
        f.book(&snap("0.41", "0.44", 2, MINUTE_NS + 5), "M", MINUTE_NS + 5);
        let s = f.finish();
        let s = &s["M"];
        assert!(s.bid[0].is_nan(), "nothing was synced when minute 0 opened");
        assert_eq!(s.bid[1], 0.40);
        assert_eq!((s.bid[2], s.ask[2]), (0.41, 0.44));
        assert_eq!(s.bid[3], 0.41, "the last touch carries to the end");
    }

    #[test]
    fn trades_keep_the_minutes_extremes() {
        let tracked: HashSet<String> = ["M".to_string()].into();
        let mut f = fold(&tracked, 2);
        f.trade("M", 1, "0.30");
        f.trade("M", 2, "0.34");
        f.trade("M", 3, "0.31");
        let s = f.finish();
        assert_eq!((s["M"].lo[0], s["M"].hi[0]), (0.30, 0.34));
        assert!(s["M"].lo[1].is_nan());
    }

    /// An unrecorded day must not inherit yesterday's touch.
    #[test]
    fn a_missing_day_blanks_the_books() {
        let tracked: HashSet<String> = ["M".to_string()].into();
        let mut f = fold(&tracked, 6);
        f.book(&snap("0.40", "0.45", 1, 0), "M", 0);
        f.blank_from(3);
        let s = f.finish();
        assert_eq!(s["M"].bid[2], 0.40);
        assert!(s["M"].bid[3].is_nan() && s["M"].bid[5].is_nan());
    }

    #[test]
    fn the_market_id_is_read_without_parsing_the_line() {
        let l = r#"{"kind":"delta","venue":"kalshi","market_id":"KXHEISMAN-27-BJACK","side":"ask"}"#;
        assert_eq!(market_of(l), Some("KXHEISMAN-27-BJACK"));
        assert_eq!(market_of(r#"{"kind":"status"}"#), None);
    }
}
