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
            inner: Mutex::new(Inner { writer, books: BookBuilder::new(), gap_count: 0 }),
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

    pub fn evict_book(&self, venue: arb_core::model::Venue, market_id: &str) {
        self.inner.lock().expect("core lock").books.remove(venue, market_id);
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
pub const STALL_RECONNECT_S: u64 = 60;
