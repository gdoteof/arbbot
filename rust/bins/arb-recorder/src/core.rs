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
///
/// `None` for anything that is NOT a scalar — object, array, bool. That arm
/// used to read `other => other.to_string()`, and that one line is the whole of
/// ticket #58. PM-US sends a trade's `quantity` inside its money wrapper,
/// `{"currency":"USD","value":"5.0000"}`; the recorder stringified the WHOLE
/// OBJECT into `TapeEvent::Trade.size`, which is a `String` and therefore
/// accepts it. 11,541 of 11,541 PM-US trade lines on the Rust tape carry a size
/// like `"{\"currency\":\"USD\",\"value\":\"4.0000\"}"` — 100%, across every
/// day the Rust recorder has run.
///
/// Nothing caught it, and could not have: `serde_json` round-trips it, and so
/// does `arb-recorder --parse-check`, because that gate asks only whether a
/// line re-serializes BYTE-IDENTICALLY and a string that was already a string
/// when it arrived always does. Python never hit it either — it does
/// `Decimal(str(t["quantity"]))`, which raises on a dict and lost the trade
/// entirely, which is why the Python tape has ZERO PM-US trades to compare
/// against.
///
/// So the rule is: a shape that is not a scalar is not a VALUE. Returning
/// `Option` hands the decision to the caller instead of handing it a
/// plausible-looking string, and every caller drops the field, the level or the
/// event. Some of them were already protected by accident — `sorted_levels`
/// runs `Dec::parse` over a level's SIZE, and a stringified object does not
/// parse — but a level's PRICE, every trade's price and size, and both INTL
/// delta fields had nothing between the venue and the tape writer at all.
pub fn dec_string(v: &serde_json::Value) -> Option<String> {
    match v {
        serde_json::Value::String(s) => Some(s.clone()),
        serde_json::Value::Number(n) => Some(n.to_string()),
        serde_json::Value::Null => Some(String::new()),
        other => {
            warn_composite(other);
            None
        }
    }
}

/// Composite values refused since process start, for the heartbeat.
///
/// Without this the fix is a DETECTABILITY REGRESSION, which is the one thing
/// it must not be. Today a composite reaches the tape as visible garbage that a
/// parse-compat gate can be pointed at; after this change the field, level or
/// event simply is not there, and an absence leaves no trace on the tape at
/// all. `warn_composite` alone does not close that gap: it is ONE flag for
/// three feeds and every field, so the first composite anywhere in the process
/// permanently silences every later one — PM-intl emits one malformed frame at
/// 03:00, PM-US changes `px`'s shape at 14:00, and every PM-US book quietly
/// loses levels for the rest of the process lifetime with nothing logged and
/// nothing written. A monotone count is what stays true after the log has been
/// spent.
///
/// Counts refused VALUES, not dropped events: a trade with two bad legs is one
/// lost print and two increments, because `parse_ws_message` resolves both legs
/// before deciding. Read it as "the venue sent us something unreadable N
/// times", not as a tape-line deficit.
static DROPPED_COMPOSITE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Monotone since process start; printed by the heartbeat next to `gaps`.
pub fn dropped_composite() -> u64 {
    DROPPED_COMPOSITE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Count it always, and say it ONCE per process with the offending payload.
///
/// A venue shape change is not a per-event condition, it is a standing one: the
/// feeds run at ~7/87/140 events per second, so a line each would be a flooded
/// journal and an unreadable one. Same reasoning as `evict_book`, which logs on
/// the transition rather than on every poll that re-reports it. The COUNT has
/// no such problem and is not rate-limited — see `DROPPED_COMPOSITE`.
fn warn_composite(v: &serde_json::Value) {
    DROPPED_COMPOSITE.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    static WARNED: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
    if !WARNED.swap(true, std::sync::atomic::Ordering::Relaxed) {
        let shape = shape_preview(v);
        eprintln!(
            "[recorder] a venue sent a COMPOSITE where a scalar belongs: {shape}\n\
             [recorder] the field/level/event is being DROPPED rather than written to the tape \
             as a stringified object (ticket #58). Logged once per process; the running count \
             is on the [hb] line as dropped_composite."
        );
    }
}

/// The offending payload, bounded for the journal.
///
/// `chars().take()`, not `String::truncate` — truncate panics unless the byte
/// offset lands on a char boundary, and venue payloads carry non-ASCII (market
/// titles, subject names). A diagnostic that can panic inside the feed loop is
/// worse than the defect it reports. Same spelling as `gateway/pmus.rs`'s
/// raw-body diagnostic.
///
/// Split out of `warn_composite` so a test can reach it AT ALL. `warn_composite`
/// fires once per process, so an assertion driven through it would silently
/// evaporate whenever any other test in the binary hit a composite first — a
/// test that proves nothing depending on thread order is worse than none.
fn shape_preview(v: &serde_json::Value) -> String {
    v.to_string().chars().take(300).collect()
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

    /// Hand the current books, as snapshot event lines, to `use_lines` WHILE
    /// STILL HOLDING the core lock.
    ///
    /// This is the whole of the fix for two defects that were one defect. The
    /// old `snapshot_lines()` captured the books under the lock and RELEASED it
    /// before returning, so its caller published them with the feed running:
    ///
    ///  * the 30s heal captured `Snapshot{M,100}`, a feed task then published
    ///    `Delta{M,101}`, and the heal loop published the snapshot after it.
    ///    Wire order delta-101 then snapshot-100, and `apply_snapshot` inserts
    ///    UNCONDITIONALLY: the engine rewound M to seq 100 with a pulled bid
    ///    restored, priced against it, then gapped on seq 102 and deleted the
    ///    book — dark for up to 30s with quotes resting on both venues.
    ///  * the welcome burst was the same shape one layer down: state captured
    ///    with the lock released, ~1,163 lines enqueued, and only then the
    ///    subscriber registered. Deltas published inside that window were
    ///    queued for nobody — a LOSS, gapping the engine on the reconnect it
    ///    had just been forced into.
    ///
    /// `on_event` publishes while holding this lock, so holding it across the
    /// USE — not just the capture — is what makes the two atomic against the
    /// feed.
    ///
    /// What that costs is SMALLER than "feed tasks now wait out a burst", which
    /// is the easy thing to say and charges the whole burst to a change that
    /// did not introduce it. `snapshot_lines()` ALREADY held this lock across
    /// `snapshot_events()` plus 1,163 `to_json_line()` calls — the acquisition
    /// below is unchanged and only `use_lines(..)` moved inside it. The NEW
    /// hold is the publish loop alone: 1,163 `subs` lock/unlock pairs and, per
    /// subscriber, an `Arc<str>` and an unbounded `send`. The live recorder
    /// runs at `subscribers=0` (the trader is still on the Python socket), so
    /// today that is tens of microseconds; ~1-2ms at five subscribers.
    ///
    /// For scale: `JsonlWriter::write` does an unbuffered `write(2)` per event
    /// under this same lock at ~230 events/s, so the lock is already
    /// syscall-bound at roughly 0.1% duty and this adds ~0.017%.
    ///
    /// LOCK ORDER is core then subs, everywhere and only that way. `on_event`,
    /// `rebroadcast_snapshots` and the welcome all take the core lock first and
    /// reach `Broadcaster`'s `subs` lock second (`publish`, or the registration
    /// inside `add_subscriber`). Nothing under `subs` takes the core lock, so
    /// the nesting cannot invert. Returning the lines instead — letting the
    /// broadcaster take `subs` and then ask for state — is exactly the
    /// inversion, which is why `welcome` takes a callback.
    pub fn with_snapshot_lines(&self, use_lines: &mut dyn FnMut(Vec<String>)) {
        let inner = self.inner.lock().expect("core lock");
        use_lines(
            inner
                .books
                .snapshot_events()
                .iter()
                .map(|e| {
                    let mut l = e.to_json_line();
                    l.push('\n');
                    l
                })
                .collect(),
        );
    }

    pub fn rebroadcast_snapshots(&self) {
        self.with_snapshot_lines(&mut |lines| {
            for line in lines {
                self.broadcaster.publish(&line);
            }
        });
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
    /// Loud, and REVERSIBLE, because it now costs more than a map entry.
    /// Dropping a book is cheap — the next sweep rebuilds it. Leaving the sweep
    /// is not: that market's only remaining heal is the REST resnapshot a
    /// `NotSynced` delta triggers, so its bound goes from the sweep's 300s to
    /// however often it is evicted again. Kalshi's status vocabulary is wider
    /// than the terminal states — this repo's own captured catalog has 20
    /// markets at `inactive`, one of them with a 2029 close time and no result
    /// — and the caller's predicate is a whitelist, so an unrecognised spelling
    /// evicts. That is the right direction to fail (a settled market must stop
    /// being published), but only if it is announced and only if the venue can
    /// take it back. Silent and one-way, it was a permanent 6x widening of the
    /// very bound `resnap_slice` exists to hold.
    ///
    /// Logged on the TRANSITION only: the universe poll re-reports every
    /// settled market every 1800s forever, and a line each would bury the one
    /// that matters.
    pub fn evict_book(&self, venue: arb_core::model::Venue, market_id: &str, why: &str) {
        let first = {
            let mut inner = self.inner.lock().expect("core lock");
            inner.books.remove(venue, market_id);
            inner.evicted.insert((venue, market_id.to_owned()))
        };
        if first {
            eprintln!(
                "[recorder] evicted {}/{market_id} ({why}): book dropped and OUT of the \
                 integrity sweep until the venue reports it live again",
                venue.as_str()
            );
        }
    }

    /// The venue says it is live after all. Only the eviction mark is lifted —
    /// the book itself comes back on the next sweep.
    pub fn restore_book(&self, venue: arb_core::model::Venue, market_id: &str) {
        let was = self
            .inner
            .lock()
            .expect("core lock")
            .evicted
            .remove(&(venue, market_id.to_owned()));
        if was {
            eprintln!(
                "[recorder] {}/{market_id} is live again — back in the integrity sweep",
                venue.as_str()
            );
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::model::{BookSide, Level, TapeEvent, Venue};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use tokio::io::AsyncBufReadExt;
    use tokio::net::UnixStream;

    /// Enough books that a burst is a real burst. The market the feed hammers
    /// is the LAST one in `BookBuilder`'s ordering, so before the fix almost
    /// the whole burst sat between the capture and its snapshot reaching
    /// the wire — the window the deltas landed in.
    const MARKETS: usize = 400;
    const HOT: &str = "M399";
    const DELTAS: u64 = 5_000;
    const BURSTS: usize = 30;
    const END: &str = "{\"end\":true}\n";

    fn snap(market: &str, seq: u64) -> TapeEvent {
        TapeEvent::Snapshot {
            venue: Venue::Kalshi,
            market_id: market.to_owned(),
            bids: vec![Level { price: "0.40".into(), size: "10".into() }],
            asks: vec![Level { price: "0.60".into(), size: "10".into() }],
            seq,
            ts_local_ns: 1,
            ts_venue: None,
        }
    }

    fn delta(seq: u64) -> TapeEvent {
        TapeEvent::Delta {
            venue: Venue::Kalshi,
            market_id: HOT.to_owned(),
            side: BookSide::Bid,
            price: "0.40".into(),
            size: "11".into(),
            seq,
            ts_local_ns: 1,
            ts_venue: None,
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("arb-core-{tag}-{}", std::process::id()));
        std::fs::create_dir_all(&d).expect("tmpdir");
        d
    }

    fn new_core(dir: &Path) -> Arc<Core> {
        Arc::new(Core::new(
            JsonlWriter::new(dir.join("tape")).expect("writer"),
            Broadcaster::new(arb_tape::broadcast::MAX_BUFFER),
        ))
    }

    /// A Core holding MARKETS books at seq 1, serving the socket. Seeding runs
    /// before anything subscribes, so it publishes to nobody.
    async fn seeded(dir: &Path) -> Arc<Core> {
        let core = new_core(dir);
        for i in 0..MARKETS {
            core.on_event(&snap(&format!("M{i:03}"), 1));
        }
        let (b, c, sock) = (core.broadcaster.clone(), core.clone(), dir.join("t.sock"));
        tokio::spawn(async move {
            b.serve(&sock, move |register: &mut dyn FnMut(Vec<String>)| {
                c.with_snapshot_lines(register)
            })
            .await
            .ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        core
    }

    async fn subscribe(dir: &Path) -> tokio::io::BufReader<UnixStream> {
        tokio::io::BufReader::new(UnixStream::connect(dir.join("t.sock")).await.expect("connect"))
    }

    /// Connected AND REGISTERED — which are not the same instant, and the gap
    /// between them is what hung this file.
    ///
    /// `UnixStream::connect` returns as soon as the kernel queues the connection
    /// on the listener's backlog. The welcome burst and the push into `subs`
    /// happen later, on the accept task, and only once it has won the CORE lock
    /// — which `std::sync::Mutex` hands to nobody in particular, so a feed
    /// thread re-locking in a tight loop can barge past it for the whole run.
    /// Everything published in that window goes to an EMPTY subscriber list,
    /// including the `END` sentinel `wire` blocks on, and then the read never
    /// returns.
    ///
    /// That is the whole of the 2026-07-29 "deadlock": no lock was ever held by
    /// anyone. Waiting on the registration is the difference between a test that
    /// hangs at 0% CPU and one that cannot.
    ///
    /// BOUNDED, because the whole point of this PR is that an unbounded
    /// test-side wait wedges the gate instead of failing it.
    async fn subscribed(dir: &Path, core: &Core) -> tokio::io::BufReader<UnixStream> {
        let reader = subscribe(dir).await;
        for _ in 0..5_000 {
            if core.broadcaster.subscriber_count() > 0 {
                return reader;
            }
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        panic!("the acceptor never registered the subscriber within 5s");
    }

    /// Everything the subscriber saw, in wire order, up to the sentinel.
    async fn wire(reader: &mut tokio::io::BufReader<UnixStream>) -> Vec<(String, String, u64)> {
        let mut out = Vec::new();
        let mut line = String::new();
        loop {
            line.clear();
            let n = reader.read_line(&mut line).await.expect("read");
            assert!(n > 0, "socket closed before the sentinel");
            let v: serde_json::Value = serde_json::from_str(&line).expect("wire json");
            if v.get("end").is_some() {
                return out;
            }
            out.push((
                v["kind"].as_str().expect("kind").to_owned(),
                v["market_id"].as_str().expect("market_id").to_owned(),
                v["seq"].as_u64().expect("seq"),
            ));
        }
    }

    /// DEFECT 1. A delta published while the 30s heal is in flight must never
    /// be followed on the wire by an older snapshot of the same market: the
    /// engine's `apply_snapshot` inserts unconditionally, so that rewinds the
    /// book, and the next real delta gaps it out of the engine's view.
    ///
    /// The invariant established here is LOCAL and that is what the assertion
    /// checks: between a heal burst and the deltas published concurrently with
    /// it, this market's wire seq does not go backwards. It is NOT the global
    /// claim that a market's wire seq only ever rises, which production breaks
    /// twice on purpose — `kalshi::ws_session` builds its `SeqCounter` inside
    /// the session (kalshi.rs:315), so every reconnect restarts every Kalshi
    /// market at 1, and trades take an independent counter keyed `{t}|tape`
    /// (kalshi.rs:428) under the same `market_id`, so a trade at seq 12 follows
    /// a delta at seq 500. Neither harms the engine (a reconnect is
    /// snapshot-led and `apply_snapshot` inserts unconditionally; trades are
    /// inert for the book), and this fixture sees neither: one market, its own
    /// snapshots and deltas, one session.
    ///
    /// Cannot spuriously PASS. After the fix `on_event` and
    /// `rebroadcast_snapshots` take the same lock, so a burst and a delta are
    /// strictly ordered and the per-subscriber channel is FIFO — the ordering
    /// is monotone by construction whatever the scheduler does. The concurrency
    /// exists to WITNESS the old bug, not to establish the new guarantee: a
    /// feed thread runs deltas without pause across BURSTS heals, and the old
    /// lock-free publish loop is interrupted many times over.
    ///
    /// It CAN spuriously fail RED under heavy load: if `queued` crosses
    /// MAX_BUFFER during the ~40ms burst the subscriber is evicted and the read
    /// hits `socket closed before the sentinel`. Known, not defended against.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_rebroadcast_never_trails_a_newer_delta_with_an_older_snapshot() {
        let dir = tmpdir("rebroadcast");
        let core = seeded(&dir).await;
        // registered, not merely connected — see `subscribed`. Nothing is
        // publishing yet, so the accept task takes the core lock uncontended.
        let mut reader = subscribed(&dir, &core).await;
        let collect = tokio::spawn(async move { wire(&mut reader).await });

        let stop = Arc::new(AtomicBool::new(false));
        let (started, feeding) = tokio::sync::oneshot::channel();
        let feed = {
            let (core, stop) = (core.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                started.send(()).expect("feed start");
                let mut seq = 2;
                while !stop.load(Ordering::Relaxed) {
                    core.on_event(&delta(seq));
                    seq += 1;
                }
            })
        };
        feeding.await.expect("feed started");
        for _ in 0..BURSTS {
            core.rebroadcast_snapshots();
            // the lock is not fair; without a yield the burst loop can starve
            // the feed and there would be nothing to interleave
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;
        }
        stop.store(true, Ordering::Relaxed);
        feed.await.expect("feed");
        core.broadcaster.publish(END);

        let hot: Vec<(String, u64)> = collect
            .await
            .expect("reader")
            .into_iter()
            .filter(|(_, m, _)| m == HOT)
            .map(|(k, _, s)| (k, s))
            .collect();
        let bursts = hot.iter().filter(|(k, _)| k == "snapshot").count();
        let deltas = hot.iter().filter(|(k, _)| k == "delta").count();
        assert!(bursts >= BURSTS, "burst snapshots missing ({bursts})");
        assert!(deltas >= BURSTS, "feed did not overlap the bursts ({deltas} deltas)");
        let mut last = 0;
        for (kind, seq) in &hot {
            assert!(*seq >= last, "wire went backwards for {HOT}: {last} then a {kind} at {seq}");
            last = *seq;
        }
        std::fs::remove_dir_all(dir).ok();
    }

    /// DEFECT 2, the same root cause one layer down. A subscriber connecting
    /// while the feed runs must not have an event dropped on the floor between
    /// its welcome snapshot and its registration — that is a LOSS, and the
    /// engine gaps and deletes the book on the very reconnect it was already
    /// flat-footed for.
    ///
    /// Cannot spuriously PASS, for the same reason: after the fix the welcome
    /// is built and registered under the core lock, so `on_event` cannot
    /// publish inside that window at all and the delta run is contiguous by
    /// construction.
    ///
    /// It used to be able to HANG rather than fail, which is strictly worse than
    /// either colour: the feed was a fixed 5000 events and the accept task only
    /// had to lose the core lock for all of them, after which the sentinel was
    /// published to nobody and the read blocked forever at 0% CPU. The feed now
    /// runs until the subscriber is registered, so the subscriber cannot land
    /// after the feed is done and there is no race left to lose.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_subscriber_connecting_mid_feed_loses_nothing() {
        let dir = tmpdir("welcome");
        let core = seeded(&dir).await;

        let (started, feeding) = tokio::sync::oneshot::channel();
        let feed = {
            let core = core.clone();
            tokio::task::spawn_blocking(move || {
                started.send(()).expect("feed start");
                // Deltas UNTIL the subscriber is registered, then a fixed run
                // after it. The fixed `2..=DELTAS+1` this replaces could run out
                // first — the accept task only has to lose the core lock for
                // ~5000 iterations, which is what CPU contention makes it do —
                // and then `publish(END)` below went to an empty subscriber list
                // and `wire` waited forever for a sentinel already dropped on
                // the floor.
                //
                // Gating the feed on the registration makes "connected mid-feed"
                // true BY CONSTRUCTION rather than by winning a race, and it
                // cannot deadlock: the accept task needs only the core lock,
                // which this loop releases between every event.
                //
                // BOUNDED. `tmpdir()` is under `std::env::temp_dir()`, which is
                // tmpfs on this box, so every event here is an unbuffered
                // `write_all` into RAM — an unbounded spin would burn a core and
                // tens of GB of it against `T_TEST=900`, and `subscribe`'s
                // `.expect("connect")` can panic while this closure is still
                // running, at which point dropping the runtime waits forever for
                // a `spawn_blocking` that never ends. Swapping a bounded hang
                // for an unbounded one is the defect this PR exists to remove.
                // Panicking here instead ends the closure, so `feed.await`
                // returns `Err` and the runtime drops cleanly.
                let mut seq = 2;
                while core.broadcaster.subscriber_count() == 0 {
                    core.on_event(&delta(seq));
                    seq += 1;
                    assert!(seq < 10 * DELTAS, "the acceptor never registered after {seq} events");
                }
                for _ in 0..DELTAS {
                    core.on_event(&delta(seq));
                    seq += 1;
                }
            })
        };
        feeding.await.expect("feed started");
        let mut reader = subscribe(&dir).await;
        feed.await.expect("feed");
        core.broadcaster.publish(END);

        let hot: Vec<(String, u64)> = wire(&mut reader)
            .await
            .into_iter()
            .filter(|(_, m, _)| m == HOT)
            .map(|(k, _, s)| (k, s))
            .collect();
        // Guaranteed by the feed gate above rather than hoped for: the connect
        // lands mid-feed, so AT LEAST DELTAS events follow the welcome (exactly
        // DELTAS, plus however many the feed emitted between the registration
        // and its next check of it). The two "connected too late" assertions
        // this replaces were the same race, checked for after the fact.
        assert!(
            hot.len() > DELTAS as usize,
            "the feed ran out before the connect: {} {HOT} lines, want > {DELTAS}",
            hot.len()
        );
        assert_eq!(hot[0].0, "snapshot", "first {HOT} line is the welcome snapshot");
        for (i, (kind, seq)) in hot.iter().enumerate() {
            let want = hot[0].1 + i as u64;
            // abs_diff, not `seq - want`: format args are evaluated only on
            // failure, and the duplicate/reorder direction underflows into
            // "attempt to subtract with overflow" instead of a diagnostic.
            assert_eq!(*seq, want, "lost {} event(s) at the connect ({kind})", seq.abs_diff(want));
        }
        std::fs::remove_dir_all(dir).ok();
    }

    /// The helper itself, since the three feed modules only reach it through
    /// their own shapes. Scalars are unchanged — that half must be pinned too,
    /// or "reject everything" would pass the composite half and silently empty
    /// the tape.
    #[test]
    fn dec_string_takes_scalars_and_refuses_composites() {
        use serde_json::json;
        assert_eq!(dec_string(&json!("0.6300")).as_deref(), Some("0.6300"));
        assert_eq!(dec_string(&json!(1700000000123i64)).as_deref(), Some("1700000000123"));
        assert_eq!(dec_string(&json!(null)).as_deref(), Some(""));
        // The literal PM-US payload of ticket #58. It used to come back as
        // `{"currency":"USD","value":"5.0000"}` — a String, so `TapeEvent`
        // accepted it and `--parse-check` re-serialized it byte-identically.
        assert_eq!(dec_string(&json!({"currency": "USD", "value": "5.0000"})), None);
        assert_eq!(dec_string(&json!(["5.0000"])), None);
        assert_eq!(dec_string(&json!(true)), None);
    }

    /// The drop has to leave a TRACE, or this whole change trades visible
    /// garbage on the tape for an invisible absence. `warn_composite` speaks
    /// once per process and is therefore already spent by the second incident;
    /// the count is what the heartbeat can still read afterwards.
    ///
    /// Asserted as a LOWER BOUND on our own calls, not as an exact delta. The
    /// counter is process-global and the test binary is multi-threaded, so any
    /// `assert_eq!` on it races `dec_string_takes_scalars_and_refuses_composites`
    /// above and would be a flake. The other direction — that a SCALAR never
    /// counts — is structural rather than asserted here: the only increment
    /// lives in `warn_composite`, which only the non-scalar match arm calls.
    #[test]
    fn a_refused_composite_is_counted_after_the_one_log_line_is_spent() {
        use serde_json::json;
        let before = dropped_composite();
        assert_eq!(dec_string(&json!({"currency": "USD", "value": "5.0000"})), None);
        assert_eq!(dec_string(&json!({"currency": "USD"})), None);
        assert!(
            dropped_composite() >= before + 2,
            "two refused composites must both be counted: {before} -> {}",
            dropped_composite()
        );
    }

    /// The diagnostic's own bound — the one line in it that could take the
    /// recorder down, and it fires exactly when things are already going wrong.
    ///
    /// `String::truncate(300)` panics unless byte 300 is a char boundary, and
    /// venue payloads carry non-ASCII. The precondition below is asserted
    /// rather than assumed: if `serde_json` ever escapes non-ASCII, byte 300
    /// becomes a boundary, the old spelling would no longer panic and this test
    /// would be quietly proving nothing — so it fails instead.
    ///
    /// Driven through `shape_preview` rather than `warn_composite` on purpose;
    /// see there.
    #[test]
    fn the_shape_preview_bounds_a_payload_without_splitting_a_char() {
        // 3-byte chars after a 10-byte prefix: byte 300 lands mid-sequence.
        let v = serde_json::json!({ "title": "€".repeat(400) });
        let raw = v.to_string();
        assert!(
            !raw.is_char_boundary(300),
            "precondition: byte 300 must split a char or this test proves nothing"
        );
        assert!(
            raw.chars().count() > 300,
            "precondition: the payload must be longer than the bound or nothing is truncated"
        );
        let preview = shape_preview(&v);
        assert_eq!(preview.chars().count(), 300, "bounded by CHARS, not bytes");
        assert!(raw.starts_with(&preview), "the preview must be a prefix of the payload");
    }

    /// The mechanism both of the above rest on, deterministically and with no
    /// second thread: the core lock is still held while the caller USES the
    /// lines, not merely while they are captured.
    #[test]
    fn with_snapshot_lines_holds_the_core_lock_across_the_use() {
        let dir = tmpdir("locked");
        let core = new_core(&dir);
        let mut held = false;
        core.with_snapshot_lines(&mut |_lines| held = core.inner.try_lock().is_err());
        assert!(held, "the core lock was released before the lines were used");
        std::fs::remove_dir_all(dir).ok();
    }
}
