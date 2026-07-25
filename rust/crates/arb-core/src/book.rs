//! BookBuilder — port of src/arbbot/book/builder.py (one implementation, one
//! behavior). State machine per (venue, market): UNSYNCED --snapshot-->
//! SYNCED --delta(expected)--> SYNCED; gap => book removed + GapDetected;
//! stale/duplicate delta dropped.

use crate::dec::Dec;
use crate::model::{BookSide, Level, TapeEvent, Venue};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub struct Book {
    pub venue: Venue,
    pub market_id: String,
    pub bids: Vec<Level>, // sorted descending by price
    pub asks: Vec<Level>, // sorted ascending by price
    pub seq: u64,
    pub ts_local_ns: i64,
    pub ts_venue: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ApplyError {
    /// Delta before any snapshot.
    NotSynced,
    /// Sequence gap: book removed; caller must request a fresh snapshot.
    GapDetected { expected: u64, got: u64 },
}

fn px(l: &Level) -> Dec {
    Dec::parse(&l.price).unwrap_or(Dec::ZERO)
}

fn sort_levels(levels: &mut [Level], descending: bool) {
    // stable, matching Python sorted()
    if descending {
        levels.sort_by(|a, b| px(b).cmp_num(&px(a)));
    } else {
        levels.sort_by(|a, b| px(a).cmp_num(&px(b)));
    }
}

#[derive(Default)]
pub struct BookBuilder {
    books: BTreeMap<(Venue, String), Book>,
}

impl BookBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn get(&self, venue: Venue, market_id: &str) -> Option<&Book> {
        self.books.get(&(venue, market_id.to_owned()))
    }

    pub fn remove(&mut self, venue: Venue, market_id: &str) {
        self.books.remove(&(venue, market_id.to_owned()));
    }

    pub fn apply_snapshot(
        &mut self,
        venue: Venue,
        market_id: &str,
        mut bids: Vec<Level>,
        mut asks: Vec<Level>,
        seq: u64,
        ts_local_ns: i64,
        ts_venue: Option<String>,
    ) {
        sort_levels(&mut bids, true);
        sort_levels(&mut asks, false);
        self.books.insert(
            (venue, market_id.to_owned()),
            Book { venue, market_id: market_id.to_owned(), bids, asks, seq, ts_local_ns, ts_venue },
        );
    }

    /// Ok(true) = applied; Ok(false) = stale/duplicate dropped.
    pub fn apply_delta(
        &mut self,
        venue: Venue,
        market_id: &str,
        side: BookSide,
        price: &str,
        size: &str,
        seq: u64,
        ts_local_ns: i64,
        ts_venue: Option<String>,
    ) -> Result<bool, ApplyError> {
        let key = (venue, market_id.to_owned());
        let book = self.books.get_mut(&key).ok_or(ApplyError::NotSynced)?;
        if seq <= book.seq {
            return Ok(false);
        }
        if seq != book.seq + 1 {
            let expected = book.seq + 1;
            self.books.remove(&key);
            return Err(ApplyError::GapDetected { expected, got: seq });
        }
        let p = Dec::parse(price).unwrap_or(Dec::ZERO);
        let sz_pos = Dec::parse(size).map(|d| d.is_positive()).unwrap_or(false);
        let levels = match side {
            BookSide::Bid => &mut book.bids,
            BookSide::Ask => &mut book.asks,
        };
        levels.retain(|l| px(l).cmp_num(&p) != std::cmp::Ordering::Equal);
        if sz_pos {
            levels.push(Level { price: price.to_owned(), size: size.to_owned() });
        }
        sort_levels(levels, matches!(side, BookSide::Bid));
        book.seq = seq;
        book.ts_local_ns = ts_local_ns;
        book.ts_venue = ts_venue;
        Ok(true)
    }

    /// Route a normalized event. Returns Err on gap/desync so the caller can
    /// request a resnapshot (trades pass through untouched).
    pub fn apply_event(&mut self, ev: &TapeEvent) -> Result<(), ApplyError> {
        match ev {
            TapeEvent::Snapshot { venue, market_id, bids, asks, seq, ts_local_ns, ts_venue } => {
                self.apply_snapshot(
                    *venue, market_id, bids.clone(), asks.clone(), *seq, *ts_local_ns,
                    ts_venue.clone(),
                );
                Ok(())
            }
            TapeEvent::Delta { venue, market_id, side, price, size, seq, ts_local_ns, ts_venue } => {
                self.apply_delta(
                    *venue, market_id, *side, price, size, *seq, *ts_local_ns, ts_venue.clone(),
                )
                .map(|_| ())
            }
            TapeEvent::Trade { .. } => Ok(()),
        }
    }

    /// Current state as synthetic snapshot events (the welcome payload).
    pub fn snapshot_events(&self) -> Vec<TapeEvent> {
        self.books
            .values()
            .map(|b| TapeEvent::Snapshot {
                venue: b.venue,
                market_id: b.market_id.clone(),
                bids: b.bids.clone(),
                asks: b.asks.clone(),
                seq: b.seq,
                ts_local_ns: b.ts_local_ns,
                ts_venue: b.ts_venue.clone(),
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lvl(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    #[test]
    fn snapshot_then_deltas() {
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "T", vec![lvl("0.40", "10"), lvl("0.43", "5")],
                          vec![lvl("0.50", "7")], 1, 100, None);
        let b = bb.get(Venue::Kalshi, "T").unwrap();
        assert_eq!(b.bids[0].price, "0.43"); // sorted desc
        // new total at existing level replaces it
        assert!(bb.apply_delta(Venue::Kalshi, "T", BookSide::Bid, "0.4300", "8", 2, 101, None).unwrap());
        let b = bb.get(Venue::Kalshi, "T").unwrap();
        assert_eq!(b.bids[0].size, "8"); // numeric price match across scales
        // zero removes
        assert!(bb.apply_delta(Venue::Kalshi, "T", BookSide::Bid, "0.43", "0", 3, 102, None).unwrap());
        assert_eq!(bb.get(Venue::Kalshi, "T").unwrap().bids.len(), 1);
        // duplicate dropped
        assert!(!bb.apply_delta(Venue::Kalshi, "T", BookSide::Bid, "0.40", "9", 3, 103, None).unwrap());
        // gap removes book
        assert_eq!(
            bb.apply_delta(Venue::Kalshi, "T", BookSide::Bid, "0.40", "9", 9, 104, None),
            Err(ApplyError::GapDetected { expected: 4, got: 9 })
        );
        assert!(bb.get(Venue::Kalshi, "T").is_none());
        // delta before snapshot
        assert_eq!(
            bb.apply_delta(Venue::Kalshi, "T", BookSide::Bid, "0.40", "9", 1, 105, None),
            Err(ApplyError::NotSynced)
        );
    }
}
