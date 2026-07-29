//! RecorderCore — venue-agnostic sink: persist, rebroadcast, build books.
//! Mirrors record/recorder.py RecorderCore. Shared behind a Mutex: the
//! per-event work is microseconds of local I/O and map updates; feed tasks
//! never do venue I/O while holding it.

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::model::TapeEvent;
use arb_tape::broadcast::Broadcaster;
use arb_tape::writer::{utc_day, JsonlWriter};
use std::sync::Mutex;

pub struct Core {
    inner: Mutex<Inner>,
    pub broadcaster: Broadcaster,
}

struct Inner {
    writer: JsonlWriter,
    books: BookBuilder,
    pub gap_count: u64,
    evicted: std::collections::HashSet<(arb_core::model::Venue, String)>,
}

/// Shared JSON helper: a venue field that may arrive as string or number,
/// rendered the way Python's str()/Decimal(str()) pipeline renders it.
pub fn dec_string(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// One side of a book from a venue's raw level array: drop non-positive sizes,
/// then sort by price (`descending` for bids).
///
/// `pick` is the only venue-specific part. Both Polymarket feeds carried a
/// verbatim copy of everything around it, differing on four characters — INTL
/// spells a level `{"price","size"}`, US spells it `{"px":{"value"},"qty"}` —
/// so the drop-and-sort rule that decides what the book looks like was defined
/// twice and could be corrected in one of them.
pub fn sorted_levels(
    raw: Option<&serde_json::Value>,
    descending: bool,
    pick: impl Fn(&serde_json::Value) -> Option<(String, String)>,
) -> Vec<arb_core::model::Level> {
    use arb_core::dec::Dec;
    let mut lv: Vec<arb_core::model::Level> = raw
        .and_then(serde_json::Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|l| {
                    let (price, size) = pick(l)?;
                    Dec::parse(&size).ok().filter(Dec::is_positive)?;
                    Some(arb_core::model::Level { price, size })
                })
                .collect()
        })
        .unwrap_or_default();
    lv.sort_by(|a, b| {
        let (pa, pb) =
            (Dec::parse(&a.price).unwrap_or(Dec::ZERO), Dec::parse(&b.price).unwrap_or(Dec::ZERO));
        if descending { pb.cmp_num(&pa) } else { pa.cmp_num(&pb) }
    });
    lv
}

/// Run a WS session, and when it ends — cleanly or not — say why and start
/// another two seconds later. Never returns.
///
/// All three feed tasks were this same loop written out, which meant three
/// copies of the reconnect delay.
pub async fn reconnect_forever<F, Fut>(tag: &str, mut session: F)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = anyhow::Result<()>>,
{
    loop {
        if let Err(e) = session().await {
            eprintln!("[{tag}] session ended: {e:#}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

/// Synthesized per-market monotonic sequence (mirrors SeqCounter).
#[derive(Default)]
pub struct SeqCounter(std::collections::HashMap<String, u64>);

impl SeqCounter {
    pub fn next(&mut self, market_id: &str) -> u64 {
        let e = self.0.entry(market_id.to_owned()).or_insert(0);
        *e += 1;
        *e
    }
}

impl Core {
    pub fn new(writer: JsonlWriter, broadcaster: Broadcaster) -> Self {
        Self {
            inner: Mutex::new(Inner {
                writer,
                books: BookBuilder::new(),
                gap_count: 0,
                evicted: Default::default(),
            }),
            broadcaster,
        }
    }

    /// Persist + publish + apply. Returns the market_id needing a fresh
    /// snapshot when a gap/desync was detected, else None.
    pub fn on_event(&self, ev: &TapeEvent) -> Option<String> {
        let mut inner = self.inner.lock().expect("core lock");
        if let Err(e) = inner.writer.write(ev, &utc_day()) {
            eprintln!("[recorder] tape write failed: {e}");
        }
        let mut line = ev.to_json_line();
        line.push('\n');
        self.broadcaster.publish(&line);
        match inner.books.apply_event(ev) {
            Ok(()) => None,
            Err(ApplyError::NotSynced) | Err(ApplyError::GapDetected { .. }) => {
                inner.gap_count += 1;
                Some(ev.market_id().to_owned())
            }
        }
    }

    pub fn snapshot_lines(&self) -> Vec<String> {
        let inner = self.inner.lock().expect("core lock");
        inner
            .books
            .snapshot_events()
            .iter()
            .map(|e| {
                let mut l = e.to_json_line();
                l.push('\n');
                l
            })
            .collect()
    }

    pub fn rebroadcast_snapshots(&self) {
        for line in self.snapshot_lines() {
            self.broadcaster.publish(&line);
        }
    }

    /// Drop a book AND remember that it was dropped.
    ///
    /// Deleting the map entry is not eviction on its own: every refresher in
    /// the recorder walks the STARTUP universe, so whatever the universe
    /// maintainer closed is re-fetched and re-published by the next integrity
    /// sweep, and the 30s rebroadcast resumes shipping the frozen book to the
    /// engine — which is the exact thing the eviction exists to stop
    /// (2026-07-20 review). The sweep therefore has to be able to ASK, and this
    /// is the object it asks: the eviction decision keeps its one owner (the
    /// venue status poll in `main`) instead of gaining a second copy next to
    /// each sweep.
    ///
    /// One-way, for the process's lifetime. A settled Kalshi market does not
    /// reopen; a restart rebuilds the universe from the registry.
    pub fn evict_book(&self, venue: arb_core::model::Venue, market_id: &str) {
        let mut inner = self.inner.lock().expect("core lock");
        inner.books.remove(venue, market_id);
        inner.evicted.insert((venue, market_id.to_owned()));
    }

    pub fn is_evicted(&self, venue: arb_core::model::Venue, market_id: &str) -> bool {
        self.inner.lock().expect("core lock").evicted.contains(&(venue, market_id.to_owned()))
    }

    pub fn gap_count(&self) -> u64 {
        self.inner.lock().expect("core lock").gap_count
    }
}

/// Reconnect if the socket delivers NOTHING for this long.
///
/// Two bugs made this necessary, and they hid each other:
///
///  1. A 5s read timeout returned "no frame" and the loop continued forever.
///     On a HALF-OPEN socket — TCP alive, server silent, writes still
///     succeeding into the void — nothing ever errored, so the session never
///     reconnected. That is the 959-second PM-intl outage on 2026-07-25.
///
///  2. Kalshi and PM-US additionally beat the liveness tracker ON THE TIMEOUT,
///     reasoning that a quiet market is healthy. That makes a dead socket
///     report perfectly healthy forever and is why those two venues showed a
///     spotless zero-stale record that could not be trusted.
///
/// Silence is only ambiguous on a quiet feed, and these are not quiet: the
/// recorded days run ~7 events/s on Kalshi, ~87/s on PM-intl and ~140/s on
/// PM-US. A full minute of nothing is a dead socket, not a lull.
pub const STALL_RECONNECT_S_DEFAULT: u64 = 60;

/// Stall threshold, overridable ONLY so the half-open-socket test does not have
/// to take a real minute. Production never sets it.
pub fn stall_reconnect_s() -> u64 {
    std::env::var("ARBBOT_STALL_RECONNECT_S")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(STALL_RECONNECT_S_DEFAULT)
}

/// WS endpoint override, for pointing a test at a local server. Production
/// never sets these; the constants remain the real venues.
pub fn ws_url(env_key: &str, default: &str) -> String {
    std::env::var(env_key).unwrap_or_else(|_| default.to_string())
}
