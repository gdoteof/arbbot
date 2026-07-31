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

impl Book {
    /// The crossing, if this book is CROSSED: `Some((best_bid, best_ask))` when
    /// `best_bid >= best_ask`, else `None`. Prices are returned verbatim so the
    /// caller can name the corruption in an operator-facing message.
    ///
    /// THE definition, in one place. `scan_relationship`, the take-take
    /// detector (4542e5f) and the maker quoter all ask this question, and three
    /// spellings of it would be three chances to disagree.
    ///
    /// Why this is corruption and not a market state: both venues are CLOBs
    /// that match on crossing AND on equality, so a resting bid at or above a
    /// resting ask would already have traded. If we see one, OUR copy of the
    /// book is wrong — a level added and never removed, or a missed resync —
    /// and every price derived from it is fiction. Observed 2026-07-28:
    /// KXRATECUT-26DEC31 carried a phantom ask at 0.0730 under a 0.1760 bid,
    /// which read as a 9.7c crossing worth 20%/yr against a PM-US book that in
    /// truth agreed with Kalshi at ~17c.
    ///
    /// Three deliberate edges:
    ///   * **A locked book (`bid == ask`) counts.** Not because locking is
    ///     always corrupt on every venue in the world, but because on these two
    ///     it is, and because there is no room to act on it: the quoter posts
    ///     one tick inside, and one tick inside a locked book is through it.
    ///   * **One side empty is NOT crossed.** Nothing contradicts anything; the
    ///     pricing paths already refuse for want of depth. Calling it crossed
    ///     would silence every one-sided book on the venue.
    ///   * **Top-of-book is EXHAUSTIVE, not a heuristic.** `bids` is sorted
    ///     descending and `asks` ascending (`sort_levels`), so `bids[0]` is the
    ///     max bid and `asks[0]` the min ask. `max_bid < min_ask` therefore
    ///     implies every bid is below every ask: a book that is crossed only
    ///     deeper in the ladder cannot exist here, and the walkers that price
    ///     against depth need no separate check.
    ///
    /// Zero-size levels are NOT filtered, matching `is_crossed`'s original
    /// spelling here and take-take's `top()`. `apply_delta` cannot create one
    /// (a non-positive size removes the level); only a snapshot could, and a
    /// zero-size level at the touch is itself a book we should not price off.
    ///
    /// Compares with `Dec` — this module's native price comparison, the same
    /// one `sort_levels` and `apply_delta` use — so the check needs no decimal
    /// `Cx` and is callable from anywhere that holds a `&Book`. Feed prices
    /// carry at most 6 decimal places, far inside both `Dec`'s exact i128
    /// mantissa and `Cx`'s 28 digits, so the two agree on every price the tape
    /// can contain.
    pub fn crossing(&self) -> Option<(&str, &str)> {
        let (b, a) = (self.bids.first()?, self.asks.first()?);
        if px(b).cmp_num(&px(a)) == std::cmp::Ordering::Less {
            return None;
        }
        Some((b.price.as_str(), a.price.as_str()))
    }

    /// `crossing().is_some()` — see there for the definition and why.
    pub fn is_crossed(&self) -> bool {
        self.crossing().is_some()
    }
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

    /// Every PM-US market this builder holds a book for, with its best YES ASK.
    ///
    /// The ONE price read `crate::maker_exit` has for the leg it closes: PM-US's
    /// gateway has no `market_quote`, so the engine's own subscription is the
    /// only PM-US book in the process. A market with an EMPTY ask side is
    /// omitted rather than reported at zero — "no ask" and "an ask at zero" are
    /// the mistake `arb_venue::gateway::Quote` documents on the other venue, and
    /// a close priced at zero would read as free.
    pub fn pm_us_asks(&self) -> Vec<(String, String)> {
        self.books
            .iter()
            .filter(|((v, _), _)| *v == Venue::PolymarketUs)
            .filter_map(|((_, m), b)| b.asks.first().map(|l| (m.clone(), l.price.clone())))
            .collect()
    }

    pub fn remove(&mut self, venue: Venue, market_id: &str) {
        self.books.remove(&(venue, market_id.to_owned()));
    }

    // The arguments ARE the wire event, field for field. `apply_event` below is
    // the struct-shaped door; this one exists for callers holding the fields
    // already destructured, and a parameter struct would just be `TapeEvent`.
    #[allow(clippy::too_many_arguments)]
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
    // Same as `apply_snapshot`: the arguments are the wire event's fields.
    #[allow(clippy::too_many_arguments)]
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

    fn book(bids: Vec<Level>, asks: Vec<Level>) -> Book {
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "T", bids, asks, 1, 1, None);
        bb.get(Venue::Kalshi, "T").expect("just inserted").clone()
    }

    /// The definition, pinned on the real 2026-07-28 numbers. Every case here
    /// is a decision `crossing()`'s doc comment defends; if one flips, the
    /// engine either trades corruption or stops trading a sane market.
    #[test]
    fn crossing_is_bid_at_or_above_ask_and_nothing_else() {
        // INVERTED — KXRATECUT-26DEC31 as the tape recorded it: a phantom ask
        // at 0.0730 pinned under a 0.1770 bid.
        let b = book(vec![lvl("0.1770", "305")], vec![lvl("0.0730", "26")]);
        assert_eq!(b.crossing(), Some(("0.1770", "0.0730")));
        assert!(b.is_crossed());

        // LOCKED — bid == ask. Corrupt on both of our venues (they match on
        // equality too), and unquotable regardless: one tick inside is through.
        let b = book(vec![lvl("0.0730", "305")], vec![lvl("0.0730", "26")]);
        assert_eq!(b.crossing(), Some(("0.0730", "0.0730")));

        // ONE TICK APART is a normal tight book and must stay tradable —
        // getting this wrong in the tight direction silently stops the engine.
        let b = book(vec![lvl("0.0720", "305")], vec![lvl("0.0730", "26")]);
        assert_eq!(b.crossing(), None);
        // the real KXRATECUT book was never crossed: bid 0.1760 / ask 0.1820
        let b = book(vec![lvl("0.1760", "305")], vec![lvl("0.1820", "26")]);
        assert_eq!(b.crossing(), None);

        // ONE SIDE EMPTY is not a contradiction — nothing to cross against.
        // Calling it crossed would silence every one-sided book on the venue.
        assert_eq!(book(vec![lvl("0.90", "5")], vec![]).crossing(), None);
        assert_eq!(book(vec![], vec![lvl("0.10", "5")]).crossing(), None);
        assert_eq!(book(vec![], vec![]).crossing(), None);

        // trailing-zero scales must not decide it: "0.4300" == "0.43"
        assert_eq!(book(vec![lvl("0.4300", "5")], vec![lvl("0.43", "5")]).crossing(),
                   Some(("0.4300", "0.43")));
        assert_eq!(book(vec![lvl("0.4300", "5")], vec![lvl("0.44", "5")]).crossing(), None);
    }

    /// Top-of-book is exhaustive, not a sample: the ladders are sorted, so a
    /// book that is sane at the touch cannot be crossed deeper down. This is
    /// what lets the depth walkers (`walk_cost`) skip a per-level check.
    #[test]
    fn a_sane_touch_means_no_level_anywhere_crosses() {
        // deliberately supplied out of order, as a venue snapshot may be
        let b = book(
            vec![lvl("0.30", "5"), lvl("0.48", "5"), lvl("0.40", "5")],
            vec![lvl("0.70", "5"), lvl("0.49", "5"), lvl("0.60", "5")],
        );
        assert_eq!(b.bids[0].price, "0.48");
        assert_eq!(b.asks[0].price, "0.49");
        assert_eq!(b.crossing(), None);
        for bid in &b.bids {
            for ask in &b.asks {
                assert!(px(bid) < px(ask), "{} vs {}", bid.price, ask.price);
            }
        }
    }
}
