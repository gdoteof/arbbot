//! The event side of maker-exit: live books, wakes, and announcement blackouts.
//!
//! A resting exit is a free option to whoever sees the other venue move first.
//! Kalshi leads and PM-US follows by seconds (2026-09-30 08:30: Kalshi repriced
//! 0.55 -> 0.61 and our PM-US ask at 0.57 was swept six seconds later), so an
//! exit checked on a timer is an exit that gets picked off. The engine therefore
//! hands every book change to [`book_changed`] and every fill of an exit order
//! to [`wake_fill`], and the scheduler's reactor re-checks exactly the lots
//! those events touch, against the book as it is now.
use super::*;
use std::sync::{Arc, OnceLock, RwLock};

/// One market's book as the exit path reads it: the same four facts the tick
/// publish carries, kept per market so one book event replaces one entry.
#[derive(Debug)]
pub(super) struct LiveBook {
    venue: Venue,
    bid: Option<String>,
    ask: Option<String>,
    bids: Option<Vec<Level>>,
    asks: Option<Vec<Level>>,
}

/// Market -> its current book. Replaced wholesale by the engine's tick
/// publish, patched per market by [`book_changed`], emptied by
/// [`clear_live_books`]. Entries are `Arc`s so a reader copies pointers under
/// the lock, never ladders: the engine takes this lock on every book event.
static LIVE: Mutex<BTreeMap<String, Arc<LiveBook>>> = Mutex::new(BTreeMap::new());

/// Markets a busy lot rests on or closes on. The engine wakes the reactor for
/// a book change only on these.
static WATCHED: RwLock<BTreeSet<String>> = RwLock::new(BTreeSet::new());

/// Market -> venue ids of orders reported filled there (empty: the book moved).
/// Coalesces bursts: the reactor takes the whole map per wake, so a thousand
/// deltas on one market are one check.
static DIRTY: Mutex<BTreeMap<String, BTreeSet<String>>> = Mutex::new(BTreeMap::new());

fn notify() -> &'static tokio::sync::Notify {
    static N: OnceLock<tokio::sync::Notify> = OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

/// Move the book maps out of a published view into [`LIVE`].
pub(super) fn take_books(view: &mut EngineView) -> BTreeMap<String, Arc<LiveBook>> {
    fn entry(out: &mut BTreeMap<String, LiveBook>, m: String, venue: Venue) -> &mut LiveBook {
        out.entry(m).or_insert(LiveBook { venue, bid: None, ask: None, bids: None, asks: None })
    }
    let mut out = BTreeMap::new();
    for (m, p) in std::mem::take(&mut view.pm_ask) {
        entry(&mut out, m, Venue::PolymarketUs).ask = Some(p);
    }
    for (m, p) in std::mem::take(&mut view.pm_bid) {
        entry(&mut out, m, Venue::PolymarketUs).bid = Some(p);
    }
    for (m, l) in std::mem::take(&mut view.pm_ask_depth) {
        entry(&mut out, m, Venue::PolymarketUs).asks = Some(l);
    }
    for (m, l) in std::mem::take(&mut view.pm_bid_depth) {
        entry(&mut out, m, Venue::PolymarketUs).bids = Some(l);
    }
    for (m, p) in std::mem::take(&mut view.k_ask) {
        entry(&mut out, m, Venue::Kalshi).ask = Some(p);
    }
    for (m, p) in std::mem::take(&mut view.k_bid) {
        entry(&mut out, m, Venue::Kalshi).bid = Some(p);
    }
    for (m, l) in std::mem::take(&mut view.k_ask_depth) {
        entry(&mut out, m, Venue::Kalshi).asks = Some(l);
    }
    for (m, l) in std::mem::take(&mut view.k_bid_depth) {
        entry(&mut out, m, Venue::Kalshi).bids = Some(l);
    }
    out.into_iter().map(|(m, b)| (m, Arc::new(b))).collect()
}

pub(super) fn replace_live(books: BTreeMap<String, Arc<LiveBook>>) {
    if let Ok(mut g) = LIVE.lock() {
        *g = books;
    }
}

/// Put the live books back into a view whose book maps are empty. `only`
/// restricts it to the markets a single lot reads, which is what keeps a
/// per-event check from copying every ladder the engine holds.
pub(super) fn overlay(view: &mut EngineView, only: Option<&[&str]>) {
    let books: Vec<(String, Arc<LiveBook>)> = match LIVE.lock() {
        Ok(g) => match only {
            Some(ms) => ms
                .iter()
                .filter_map(|m| g.get(*m).map(|b| (m.to_string(), b.clone())))
                .collect(),
            None => g.iter().map(|(m, b)| (m.clone(), b.clone())).collect(),
        },
        Err(_) => return,
    };
    for (m, b) in books {
        let (bid, ask, bids, asks) = match b.venue {
            Venue::PolymarketUs => (
                &mut view.pm_bid,
                &mut view.pm_ask,
                &mut view.pm_bid_depth,
                &mut view.pm_ask_depth,
            ),
            Venue::Kalshi => (
                &mut view.k_bid,
                &mut view.k_ask,
                &mut view.k_bid_depth,
                &mut view.k_ask_depth,
            ),
            _ => continue,
        };
        if let Some(p) = &b.bid {
            bid.insert(m.clone(), p.clone());
        }
        if let Some(p) = &b.ask {
            ask.insert(m.clone(), p.clone());
        }
        if let Some(l) = &b.bids {
            bids.insert(m.clone(), l.clone());
        }
        if let Some(l) = &b.asks {
            asks.insert(m, l.clone());
        }
    }
}

/// The engine applied a book event. `None` means the book may not be priced
/// from (crossed, or gone), which removes the market exactly as the tick
/// publish would by leaving it out.
///
/// Called from the engine's feed loop on every book event while the view is
/// being published and the feed is trusted, so it does one clone of the two
/// ladders outside the lock and a pointer swap inside it.
pub fn book_changed(venue: Venue, market: &str, book: Option<(&[Level], &[Level])>) {
    if !matches!(venue, Venue::Kalshi | Venue::PolymarketUs) {
        return;
    }
    let entry = book.map(|(bids, asks)| {
        Arc::new(LiveBook {
            venue,
            bid: bids.first().map(|l| l.price.clone()),
            ask: asks.first().map(|l| l.price.clone()),
            bids: Some(bids.to_vec()),
            asks: Some(asks.to_vec()),
        })
    });
    if let Ok(mut g) = LIVE.lock() {
        match entry {
            Some(e) => {
                g.insert(market.to_owned(), e);
            }
            None => {
                g.remove(market);
            }
        }
    }
    if WATCHED.read().is_ok_and(|w| w.contains(market)) {
        mark(market, None);
    }
}

/// The feed can no longer be trusted, or the engine was killed: no book in the
/// exit path may be priced from until events rebuild it. Every resting exit
/// then fails its keep-check on its next look and is pulled.
pub fn clear_live_books() {
    if let Ok(mut g) = LIVE.lock() {
        g.clear();
    }
    let watched: Vec<String> = WATCHED.read().map(|w| w.iter().cloned().collect()).unwrap_or_default();
    for m in watched {
        mark(&m, None);
    }
}

/// A fill was reported on an order this process placed outside the engine.
/// Unconditional, unlike book wakes: fills are rare and a missed one is a
/// naked leg waiting for the next pass.
pub fn wake_fill(market: &str, venue_order_id: &str) {
    mark(market, Some(venue_order_id));
}

pub(super) fn mark(market: &str, filled: Option<&str>) {
    if let Ok(mut g) = DIRTY.lock() {
        let ids = g.entry(market.to_owned()).or_default();
        if let Some(id) = filled {
            ids.insert(id.to_owned());
        }
    }
    notify().notify_one();
}

pub(super) fn set_watched(markets: BTreeSet<String>) {
    if let Ok(mut g) = WATCHED.write() {
        *g = markets;
    }
}

/// The markets touched since the last call, waiting until there is one.
pub(super) async fn dirty() -> BTreeMap<String, BTreeSet<String>> {
    loop {
        let d = DIRTY.lock().map(|mut g| std::mem::take(&mut *g)).unwrap_or_default();
        if !d.is_empty() {
            return d;
        }
        notify().notified().await;
    }
}

#[cfg(test)]
pub(super) fn reset() {
    if let Ok(mut g) = LIVE.lock() {
        g.clear();
    }
    if let Ok(mut g) = DIRTY.lock() {
        g.clear();
    }
    set_watched(BTreeSet::new());
}

// ------------------------------------------------------------------ blackouts ---

/// No exit may rest on a family from `from` on. A scheduled announcement moves
/// both venues at the same instant, so no book event can pull a resting exit
/// before the winning side lifts it at its pre-announcement price.
#[derive(serde::Deserialize)]
struct Blackout {
    family: String,
    /// UTC, `YYYY-MM-DDTHH:MM:SSZ`.
    from: String,
    #[serde(default)]
    why: String,
}

/// Parsed windows `(family, from_epoch_s, why)`.
type Windows = Vec<(String, f64, String)>;

/// `Ok` = the parsed windows; `Err` = a file that exists and cannot be
/// trusted, which blacks out EVERY exit until fixed.
static BLACKOUTS: Mutex<Result<Windows, String>> = Mutex::new(Ok(Vec::new()));

pub(super) fn parse_blackouts(text: &str) -> Result<Windows, String> {
    if text.trim().is_empty() {
        return Ok(Vec::new());
    }
    let rows: Vec<Blackout> = serde_yaml::from_str(text).map_err(|e| e.to_string())?;
    rows.into_iter()
        .map(|b| {
            if b.family.is_empty() {
                return Err("a blackout with an empty family would match every relationship".into());
            }
            let from = crate::taketake::parse_iso8601_z(&b.from)
                .ok_or_else(|| format!("{}: `from` must be YYYY-MM-DDTHH:MM:SSZ, got {:?}", b.family, b.from))?;
            Ok((b.family, from, b.why))
        })
        .collect()
}

/// Re-read the blackout file. Missing is "none"; unreadable or unparseable
/// fails closed.
pub(super) fn load_blackouts(path: &str) {
    let parsed = match std::fs::read_to_string(path) {
        Ok(text) => parse_blackouts(&text).map_err(|e| format!("{path}: {e}")),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("{path}: {e}")),
    };
    if let Ok(mut g) = BLACKOUTS.lock() {
        *g = parsed;
    }
}

#[cfg(test)]
pub(super) fn set_blackouts(rows: Result<Windows, String>) {
    if let Ok(mut g) = BLACKOUTS.lock() {
        *g = rows;
    }
}

/// Why no exit may rest on `rel_id` at `now`, if one may not.
pub(super) fn blackout(rel_id: &str, now: f64) -> Option<String> {
    let g = BLACKOUTS.lock().ok()?;
    match &*g {
        Err(e) => Some(format!("the exit blackout file is damaged ({e}) — no exit rests until it is fixed")),
        Ok(rows) => rows
            .iter()
            .find(|(family, from, _)| now >= *from && rel_id.contains(family.as_str()))
            .map(|(family, _, why)| format!("exit blackout on `{family}`: {why}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lv(p: &str) -> Level {
        Level { price: p.into(), size: "10".into() }
    }

    #[test]
    fn blackouts_parse_strictly_and_fail_closed() {
        let rows = parse_blackouts(
            "- family: tsla-q3-deliv\n  from: 2026-10-01T04:00:00Z\n  why: deliveries report\n",
        )
        .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].1, 1_790_827_200.0);
        assert!(parse_blackouts("- family: x\n  from: 2026-10-01\n").is_err());
        assert!(parse_blackouts("- family: ''\n  from: 2026-10-01T04:00:00Z\n").is_err());
        assert!(parse_blackouts("").unwrap().is_empty());
    }

    /// The file this binary ships with must parse: a typo in it blacks out
    /// every exit in the book.
    #[test]
    fn the_shipped_blackout_file_parses() {
        let rows = parse_blackouts(include_str!("../../../../../config/exit-blackouts.yaml")).unwrap();
        assert!(rows.iter().any(|(f, _, _)| f == "tsla-q3-deliv"));
        assert!(rows.iter().any(|(f, _, _)| f == "nobel-peace-26"));
    }

    #[test]
    fn a_blackout_starts_at_its_instant_and_a_damaged_file_blacks_out_everything() {
        let _g = super::super::test_serial();
        set_blackouts(Ok(vec![("tsla-q3".into(), 100.0, "report".into())]));
        assert!(blackout("xvus-tsla-q3-deliv-q3-above-480k", 99.0).is_none());
        assert!(blackout("xvus-tsla-q3-deliv-q3-above-480k", 100.0).is_some());
        assert!(blackout("xvus-nobel-peace-26-unrwa", 100.0).is_none());
        set_blackouts(Err("bad yaml".into()));
        assert!(blackout("xvus-nobel-peace-26-unrwa", 0.0).is_some());
        set_blackouts(Ok(Vec::new()));
    }

    #[test]
    fn a_book_event_replaces_one_market_and_wakes_only_watched_markets() {
        let _g = super::super::test_serial();
        reset_view();
        publish_view(EngineView {
            apr_bar: 1.0,
            global_cap_usd: 100.0,
            pm_ask: BTreeMap::from([("pm".to_string(), "0.40".to_string())]),
            k_bid: BTreeMap::from([("K".to_string(), "0.55".to_string())]),
            ..Default::default()
        });
        let v = engine_view().unwrap();
        assert_eq!(v.pm_ask["pm"], "0.40");
        assert_eq!(v.k_bid["K"], "0.55");
        set_watched(BTreeSet::from(["K".to_string()]));
        book_changed(Venue::Kalshi, "K", Some((&[lv("0.61")], &[lv("0.62")])));
        book_changed(Venue::PolymarketUs, "pm", Some((&[lv("0.39")], &[lv("0.41")])));
        let v = engine_view().unwrap();
        assert_eq!(v.k_bid["K"], "0.61");
        assert_eq!(v.k_ask["K"], "0.62");
        assert_eq!(v.pm_ask["pm"], "0.41");
        let d = DIRTY.lock().map(|mut g| std::mem::take(&mut *g)).unwrap();
        assert_eq!(d, BTreeMap::from([("K".to_string(), BTreeSet::new())]));
        // A crossed book leaves the view; a cleared feed leaves nothing.
        book_changed(Venue::Kalshi, "K", None);
        assert!(!engine_view().unwrap().k_bid.contains_key("K"));
        clear_live_books();
        assert!(engine_view().unwrap().pm_ask.is_empty());
        wake_fill("pm", "CT1");
        let d = DIRTY.lock().map(|mut g| std::mem::take(&mut *g)).unwrap();
        assert_eq!(d.get("pm"), Some(&BTreeSet::from(["CT1".to_string()])));
        reset_view();
    }

    #[test]
    fn a_scoped_view_carries_only_the_markets_asked_for() {
        let _g = super::super::test_serial();
        reset_view();
        publish_view(EngineView { apr_bar: 1.0, global_cap_usd: 100.0, ..Default::default() });
        book_changed(Venue::Kalshi, "K", Some((&[lv("0.61")], &[lv("0.62")])));
        book_changed(Venue::Kalshi, "K2", Some((&[lv("0.11")], &[lv("0.12")])));
        let v = engine_view_of(&["K"]).unwrap();
        assert_eq!(v.k_bid.len(), 1);
        assert_eq!(v.k_bid["K"], "0.61");
        reset_view();
    }
}
