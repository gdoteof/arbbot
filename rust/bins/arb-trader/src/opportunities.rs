//! The opportunity stream: every two-leg Kalshi + PM-US registry pair priced
//! as a taker basket against the books this engine receives, written to
//! `<dir>/opportunities-<UTC day>.jsonl` for the dashboard (`--opportunities-out`).
//!
//! OFF THE DECISION PATH. The engine hands each feed line to
//! [`Emitter::offer`] after it has acted on it — a `try_send` of a `String` it
//! already owns — and a dedicated OS thread keeps its own copy of the books,
//! prices them and writes. The engine's books, quoters and intents are never
//! read or touched from here. Live, a full queue drops the line and counts it,
//! and the thread then discards every book it holds: a book that missed a
//! delta is wrong until its next snapshot, and the recorder rebroadcasts one
//! for every book every 30s. A bench or WAL replay waits for room instead, so
//! its stream is a function of the tape alone. Nothing in this file may panic
//! — an armed process halts and sweeps on a panic in any thread — so every
//! I/O failure is counted instead.
//!
//! Pricing is `arb_core::scan::scan_relationship` under `arb_core::fees`, the
//! engine's own depth walk and fee curves: every side assignment whose worst
//! case pays at least $1, sized to the shallower ladder's full depth and halved
//! until the depth-walked net edge is positive. Records keep the field names
//! and 6dp decimal strings of `src/arbbot/scan/scanner.py`'s, plus one field.
//! Where a field means something different:
//!
//!   * `fees` charges every PM-US leg the schedule's 0.06 coefficient. The
//!     Python scanner used the venue's per-market `feeCoefficient` where its
//!     tag catalog reported one, so on such a market the edge here runs
//!     higher — and, since a basket is halved only until its edge turns
//!     positive, `size` can differ as well.
//!   * `max_hold_close_time` is always null: there is no market catalog here.
//!   * `bucket` is always `primary`: every leg's minimum order is taken as one
//!     contract, and a basket with a Kalshi leg is sized in whole contracts.
//!   * `ts_local_ns` is the newest recorder timestamp on a book line this
//!     thread has read, not a wall-clock read at scan time.
//!   * `engine_scope` is new: `quoted` (this process quotes the pair),
//!     `not_tradable` (the registry verdict does not greenlight it) or
//!     `out_of_scope` (greenlit, but outside `--rel-prefix`).
//!   * A line is written when a crossing appears or changes, at most once a
//!     second, and once a minute while it holds unchanged — not on every book
//!     event. See [`SigState`].
//!
//! Like the Python scanner it reads no market status and no feed health: a
//! frozen book on a closed market can show as a crossing until a snapshot
//! replaces it, and a FEED_DOWN line clears every book.

use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::mpsc::{sync_channel, Receiver, SyncSender, TryRecvError};
use std::sync::Arc;

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::fees::FeeSchedule;
use arb_core::model::{BookSide, Venue};
use arb_core::scan::{scan_relationship, Cx, MarketMeta, Opportunity, Rel, RelLeg, RelType, D};

/// Lines the engine may get ahead of this thread by before it drops one. The
/// feed channel's own depth, so a burst that fits there fits here.
const QUEUE: usize = 65536;
/// The least time between two lines for one signature while it is changing.
const MIN_GAP_NS: i64 = 1_000_000_000;
const NS_PER_MIN: i64 = 60_000_000_000;
const NS_PER_DAY: i64 = 86_400_000_000_000;

/// Whether this process quotes a relationship, and if not, why not — the two
/// facts the dashboard's `not tradable` / `out of engine scope` verdicts are
/// built from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EngineScope {
    Quoted,
    NotTradable,
    OutOfScope,
}

impl EngineScope {
    pub fn as_str(self) -> &'static str {
        match self {
            EngineScope::Quoted => "quoted",
            EngineScope::NotTradable => "not_tradable",
            EngineScope::OutOfScope => "out_of_scope",
        }
    }
}

/// Where to write and what to price.
pub struct Spec {
    pub dir: String,
    pub rels: Vec<(Rel, EngineScope)>,
}

impl Spec {
    pub fn load(dir: &str, registry: &str, rel_prefixes: &[String]) -> Spec {
        let reg = arb_registry::Registry::load(registry).expect("read registry");
        Spec { dir: dir.to_string(), rels: universe(&reg, rel_prefixes) }
    }
}

/// Every two-leg relationship with one Kalshi leg and one PM-US leg, except
/// those the registry records as `rejected` (documented traps, which the
/// Python scanner skipped too). `EngineScope::Quoted` is exactly `load_quoters`'
/// rule for such a pair: a greenlit verdict, inside `--rel-prefix`.
pub fn universe(reg: &arb_registry::Registry, rel_prefixes: &[String]) -> Vec<(Rel, EngineScope)> {
    reg.relationships
        .iter()
        .filter(|r| r.verdict.as_deref() != Some("rejected"))
        .filter_map(|r| {
            let rtype = RelType::parse(r.kind.as_deref()?)?;
            let legs = r
                .legs
                .iter()
                .map(|l| Some(RelLeg { venue: Venue::parse(&l.venue)?, market_id: l.market_id.clone() }))
                .collect::<Option<Vec<_>>>()?;
            let venues: Vec<Venue> = legs.iter().map(|l| l.venue).collect();
            if venues.len() != 2
                || !venues.contains(&Venue::Kalshi)
                || !venues.contains(&Venue::PolymarketUs)
            {
                return None;
            }
            let scope = if !r.tradable() {
                EngineScope::NotTradable
            } else if !rel_prefixes.is_empty()
                && !rel_prefixes.iter().any(|p| r.id.starts_with(p.as_str()))
            {
                EngineScope::OutOfScope
            } else {
                EngineScope::Quoted
            };
            let tranche = r.tranche.clone().unwrap_or_else(|| "long-tail".into());
            Some((Rel { id: r.id.clone(), rtype, tranche, legs }, scope))
        })
        .collect()
}

#[derive(Default)]
struct Gauges {
    /// Lines the engine could not queue. Any rise empties this thread's books.
    dropped: AtomicU64,
    written: AtomicU64,
    write_errors: AtomicU64,
}

/// The engine's handle on the stream.
pub struct Emitter {
    tx: Option<SyncSender<String>>,
    /// Wait for room rather than drop: bench and WAL replay, where nothing is
    /// live to stall and the stream should not depend on thread scheduling.
    lossless: bool,
    thread: Option<std::thread::JoinHandle<()>>,
    gauges: Arc<Gauges>,
}

impl Emitter {
    pub fn spawn(spec: Spec, lossless: bool) -> Emitter {
        let gauges = Arc::new(Gauges::default());
        let count = |s: EngineScope| spec.rels.iter().filter(|(_, x)| *x == s).count();
        eprintln!(
            "[opportunities] pricing {} kalshi/polymarket_us pairs ({} quoted, {} not tradable, \
             {} out of scope) -> {}/opportunities-<UTC day>.jsonl",
            spec.rels.len(),
            count(EngineScope::Quoted),
            count(EngineScope::NotTradable),
            count(EngineScope::OutOfScope),
            spec.dir
        );
        let (tx, rx) = sync_channel::<String>(QUEUE);
        let g = gauges.clone();
        match std::thread::Builder::new().name("opportunities".into()).spawn(move || drain(spec, rx, &g)) {
            Ok(h) => Emitter { tx: Some(tx), lossless, thread: Some(h), gauges },
            Err(e) => {
                eprintln!("[opportunities] could not start the writer thread ({e}); no stream this run");
                Emitter { tx: None, lossless, thread: None, gauges }
            }
        }
    }

    /// Queue one feed line the engine has finished with. Never blocks unless
    /// `lossless`.
    pub fn offer(&self, line: String) {
        if let Some(tx) = &self.tx {
            let sent = if self.lossless { tx.send(line).is_ok() } else { tx.try_send(line).is_ok() };
            if !sent {
                self.gauges.dropped.fetch_add(1, AtomicOrdering::Relaxed);
            }
        }
    }

    /// Stop queueing, let the thread price and write what it holds, and wait
    /// for it. The gauges stay readable.
    pub fn close(&mut self) {
        self.tx = None;
        if let Some(h) = self.thread.take() {
            let _ = h.join();
        }
    }

    pub fn gauges(&self) -> serde_json::Value {
        let g = &self.gauges;
        serde_json::json!({
            "written": g.written.load(AtomicOrdering::Relaxed),
            "dropped": g.dropped.load(AtomicOrdering::Relaxed),
            "write_errors": g.write_errors.load(AtomicOrdering::Relaxed),
        })
    }
}

/// The thread body: apply each line, price what it moved, write what is due,
/// and flush whenever the queue is empty — so a tail is at most one line
/// behind, and a burst is written in large pieces.
fn drain(spec: Spec, rx: Receiver<String>, g: &Gauges) {
    let mut scan = Scanner::new(spec.rels);
    let mut files = DayFiles::new(&spec.dir);
    let mut out = Vec::new();
    let mut drops_seen = 0u64;
    let mut next = rx.recv().ok();
    while let Some(line) = next {
        // A line queued after a drop is received after the drop was counted,
        // so checking here means no line is ever applied to a book that
        // missed one.
        let drops = g.dropped.load(AtomicOrdering::Relaxed);
        if drops != drops_seen {
            if drops_seen == 0 {
                eprintln!(
                    "[opportunities] the engine dropped a line on a full queue; every book is \
                     discarded and rebuilt from snapshots (`opportunities.dropped` counts)"
                );
            }
            drops_seen = drops;
            scan.reset(&mut out);
        }
        scan.on_line(&line, &mut out);
        scan.price(&mut out);
        files.write(&mut out, g);
        next = match rx.try_recv() {
            Ok(line) => Some(line),
            Err(TryRecvError::Empty) => {
                files.flush(g);
                rx.recv().ok()
            }
            Err(TryRecvError::Disconnected) => None,
        };
    }
    scan.reset(&mut out);
    files.write(&mut out, g);
    files.flush(g);
}

/// One observation of a crossing: its record minus the timestamp and scope
/// (`prefix`, which is also its identity for change detection), and its edge.
#[derive(Clone)]
struct Obs {
    prefix: String,
    edge: D,
    ts: i64,
}

/// One open crossing — a relationship and a side assignment — and what has
/// been written about it.
///
/// A line is written when the crossing appears; when its priced content (size,
/// prices, fees, edge) changes, at most once per [`MIN_GAP_NS`]; once per UTC
/// minute while it holds unchanged; and, if a change is still unwritten, when
/// it vanishes. Within a throttled interval the highest-edge state is the one
/// written, and the latest state follows it once the gap allows. That keeps
/// what the dashboard reads off the tail — each pair's best edge over the
/// window, how long ago it was last seen, the minutes it was seen in — while a
/// crossing that sits for an hour costs sixty lines rather than one per book
/// event. On the Python scanner's 2026-10-22 file (291,601 lines) the same
/// signatures changed state 8,551 times across 17,265 signature-minutes.
struct SigState {
    sig: String,
    last: Obs,
    /// The best unwritten state since the last write. `None` means every
    /// observation since then equals what was written.
    best: Option<Obs>,
    written: Option<String>,
    written_at: Option<i64>,
}

impl SigState {
    fn new(sig: String, obs: Obs) -> SigState {
        SigState { sig, best: Some(obs.clone()), last: obs, written: None, written_at: None }
    }

    fn observe(&mut self, cx: &mut Cx, obs: Obs) {
        if self.written.as_deref() != Some(obs.prefix.as_str()) {
            let better = match &self.best {
                Some(b) => cx.cmp(obs.edge, b.edge) != std::cmp::Ordering::Less,
                None => true,
            };
            if better {
                self.best = Some(obs.clone());
            }
        }
        self.last = obs;
    }

    /// The `(ts, prefix)` to write at `now`, if one is due.
    fn due(&mut self, now: i64) -> Option<(i64, String)> {
        let gap_ok = self.written_at.is_none_or(|t| now.saturating_sub(t) >= MIN_GAP_NS);
        if gap_ok {
            if let Some(b) = self.best.take() {
                self.written = Some(b.prefix.clone());
                self.written_at = Some(now);
                if self.last.prefix != b.prefix {
                    self.best = Some(self.last.clone());
                }
                return Some((b.ts, b.prefix));
            }
        }
        let new_minute =
            self.written_at.is_some_and(|t| t.div_euclid(NS_PER_MIN) != now.div_euclid(NS_PER_MIN));
        if self.best.is_none() && new_minute {
            self.written_at = Some(now);
            return Some((now, self.last.prefix.clone()));
        }
        None
    }
}

fn record(prefix: &str, ts: i64, scope: EngineScope) -> String {
    format!("{prefix}\"ts_local_ns\":{ts},\"engine_scope\":\"{}\"}}", scope.as_str())
}

/// `canonical_line` cut before its last key, `ts_local_ns`, so the timestamp
/// can be stamped when the line is written.
fn prefix_of(cx: &mut Cx, o: &Opportunity) -> Option<String> {
    let mut line = o.canonical_line(cx);
    let cut = line.rfind("\"ts_local_ns\":")?;
    line.truncate(cut);
    Some(line)
}

/// Books, the pricing universe and the open crossings. Pure: lines in,
/// `(ts, record)` pairs out, so every rule above is testable without a thread
/// or a file.
struct Scanner {
    cx: Cx,
    fees: FeeSchedule,
    books: BookBuilder,
    rels: Vec<(Rel, EngineScope)>,
    by_market: HashMap<Venue, HashMap<String, Vec<usize>>>,
    dirty: BTreeSet<usize>,
    open: Vec<Vec<SigState>>,
    clock_ns: i64,
}

impl Scanner {
    fn new(rels: Vec<(Rel, EngineScope)>) -> Scanner {
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let mut by_market: HashMap<Venue, HashMap<String, Vec<usize>>> = HashMap::new();
        for (ri, (rel, _)) in rels.iter().enumerate() {
            for leg in &rel.legs {
                by_market.entry(leg.venue).or_default().entry(leg.market_id.clone()).or_default().push(ri);
            }
        }
        let open = rels.iter().map(|_| Vec::new()).collect();
        Scanner { cx, fees, books: BookBuilder::new(), rels, by_market, dirty: BTreeSet::new(), open, clock_ns: 0 }
    }

    /// Apply one feed line. Snapshots and deltas are read exactly as
    /// `Engine::on_feed_line` reads them; only books some pair prices are kept.
    fn on_line(&mut self, line: &str, out: &mut Vec<(i64, String)>) {
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else { return };
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        if kind == crate::feed::FEED_DOWN {
            self.reset(out);
            return;
        }
        if kind != "snapshot" && kind != "delta" {
            return;
        }
        let Some(venue) = v.get("venue").and_then(|x| x.as_str()).and_then(Venue::parse) else { return };
        let Some(market_id) = v.get("market_id").and_then(|x| x.as_str()) else { return };
        let seq = v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0);
        let ts_local_ns = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
        let ts_venue = v.get("ts_venue").and_then(|x| x.as_str()).map(str::to_owned);
        self.clock_ns = self.clock_ns.max(ts_local_ns);
        let Some(idxs) = self.by_market.get(&venue).and_then(|m| m.get(market_id)) else { return };
        let moved = if kind == "snapshot" {
            let (Some(bids), Some(asks)) =
                (crate::engine::levels_of(v.get("bids")), crate::engine::levels_of(v.get("asks")))
            else {
                return;
            };
            self.books.apply_snapshot(venue, market_id, bids, asks, seq, ts_local_ns, ts_venue);
            true
        } else {
            let Some(side) = v.get("side").and_then(|x| x.as_str()).and_then(BookSide::parse) else {
                return;
            };
            let (Some(price), Some(size)) =
                (v.get("price").and_then(|x| x.as_str()), v.get("size").and_then(|x| x.as_str()))
            else {
                return;
            };
            match self.books.apply_delta(venue, market_id, side, price, size, seq, ts_local_ns, ts_venue) {
                Ok(applied) => applied,
                // The builder removed the book: whatever crossed on it is gone.
                Err(ApplyError::GapDetected { .. }) => true,
                Err(ApplyError::NotSynced) => false,
            }
        };
        if moved {
            self.dirty.extend(idxs.iter().copied());
        }
    }

    /// Price every relationship whose books moved since the last call, then
    /// emit whatever is due.
    fn price(&mut self, out: &mut Vec<(i64, String)>) {
        let now = self.clock_ns;
        for ri in std::mem::take(&mut self.dirty) {
            self.rescan(ri, now, out);
        }
        for (ri, states) in self.open.iter_mut().enumerate() {
            let Some((_, scope)) = self.rels.get(ri) else { continue };
            for st in states.iter_mut() {
                if let Some((ts, prefix)) = st.due(now) {
                    out.push((ts, record(&prefix, ts, *scope)));
                }
            }
        }
    }

    fn rescan(&mut self, ri: usize, now: i64, out: &mut Vec<(i64, String)>) {
        let Scanner { cx, fees, books, rels, open, .. } = self;
        let (Some((rel, scope)), Some(states)) = (rels.get(ri), open.get_mut(ri)) else { return };
        let one = cx.one;
        let metas = move |_: &RelLeg| MarketMeta {
            category: "default".into(),
            min_order_size: one,
            close_time: None,
            taker_coef_override: None,
        };
        let seen: Vec<(String, Obs)> = scan_relationship(cx, fees, rel, books, &metas, 0)
            .iter()
            .filter_map(|o| {
                let prefix = prefix_of(cx, o)?;
                Some((o.signature.clone(), Obs { prefix, edge: o.net_edge_per_contract, ts: now }))
            })
            .collect();
        states.retain_mut(|st| {
            if seen.iter().any(|(sig, _)| *sig == st.sig) {
                return true;
            }
            if let Some(b) = st.best.take() {
                out.push((b.ts, record(&b.prefix, b.ts, *scope)));
            }
            false
        });
        for (sig, obs) in seen {
            match states.iter_mut().find(|st| st.sig == sig) {
                Some(st) => st.observe(cx, obs),
                None => states.push(SigState::new(sig, obs)),
            }
        }
    }

    /// Every book is suspect: write what is unwritten and start again from
    /// snapshots.
    fn reset(&mut self, out: &mut Vec<(i64, String)>) {
        for (ri, states) in self.open.iter_mut().enumerate() {
            let Some((_, scope)) = self.rels.get(ri) else { continue };
            for st in states.drain(..) {
                if let Some(b) = st.best {
                    out.push((b.ts, record(&b.prefix, b.ts, *scope)));
                }
            }
        }
        self.books = BookBuilder::new();
        self.dirty.clear();
    }
}

/// `<dir>/opportunities-<UTC day>.jsonl`, the day taken from each record's own
/// timestamp, opened for append.
struct DayFiles {
    dir: std::path::PathBuf,
    day: Option<i64>,
    w: Option<std::io::BufWriter<std::fs::File>>,
    warned: bool,
}

impl DayFiles {
    fn new(dir: &str) -> DayFiles {
        DayFiles { dir: dir.into(), day: None, w: None, warned: false }
    }

    fn path(&self, day: i64) -> std::path::PathBuf {
        self.dir.join(format!("opportunities-{}.jsonl", arb_core::resolve::iso_from_day(day)))
    }

    fn write(&mut self, out: &mut Vec<(i64, String)>, g: &Gauges) {
        if out.is_empty() {
            return;
        }
        // A file that failed to open is retried on the next call, not per line.
        if self.w.is_none() {
            self.day = None;
        }
        for (ts, line) in out.drain(..) {
            let day = ts.div_euclid(NS_PER_DAY);
            if self.day != Some(day) {
                self.open(day, g);
            }
            let ok = self.w.as_mut().is_some_and(|w| writeln!(w, "{line}").is_ok());
            if ok {
                g.written.fetch_add(1, AtomicOrdering::Relaxed);
            } else {
                self.fail(g, "write");
            }
        }
    }

    fn flush(&mut self, g: &Gauges) {
        if self.w.as_mut().is_some_and(|w| w.flush().is_err()) {
            self.fail(g, "flush");
        }
    }

    fn open(&mut self, day: i64, g: &Gauges) {
        if self.w.take().is_some_and(|mut w| w.flush().is_err()) {
            self.fail(g, "flush");
        }
        self.day = Some(day);
        let path = self.path(day);
        let f = std::fs::create_dir_all(&self.dir)
            .and_then(|_| std::fs::OpenOptions::new().create(true).append(true).open(&path));
        match f {
            Ok(f) => self.w = Some(std::io::BufWriter::new(f)),
            Err(e) => {
                if !self.warned {
                    eprintln!("[opportunities] cannot open {}: {e}", path.display());
                }
                self.fail(g, "open");
            }
        }
    }

    fn fail(&mut self, g: &Gauges, what: &str) {
        g.write_errors.fetch_add(1, AtomicOrdering::Relaxed);
        if !self.warned {
            self.warned = true;
            eprintln!("[opportunities] {what} failed; counting further failures in `opportunities.write_errors`");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const K: &str = "KXHEISMAN-27-MTONE";
    const P: &str = "tec-cfb-heisman-2026-12-13-w-malton";
    /// 2026-10-22T00:00:00Z
    const DAY0: i64 = 1_792_627_200_000_000_000;

    fn rel(scope: EngineScope) -> (Rel, EngineScope) {
        (
            Rel {
                id: "xvus-heisman-26-malachitoney".into(),
                rtype: RelType::CrossVenueEquivalent,
                tranche: "head".into(),
                legs: vec![
                    RelLeg { venue: Venue::Kalshi, market_id: K.into() },
                    RelLeg { venue: Venue::PolymarketUs, market_id: P.into() },
                ],
            },
            scope,
        )
    }

    fn levels(lv: &[(&str, &str)]) -> String {
        let v: Vec<String> =
            lv.iter().map(|(p, s)| format!(r#"{{"price":"{p}","size":"{s}"}}"#)).collect();
        format!("[{}]", v.join(","))
    }

    fn snap(venue: &str, market: &str, seq: u64, ts: i64, bids: &[(&str, &str)], asks: &[(&str, &str)]) -> String {
        format!(
            r#"{{"kind":"snapshot","venue":"{venue}","market_id":"{market}","seq":{seq},"ts_local_ns":{ts},"ts_venue":null,"bids":{},"asks":{}}}"#,
            levels(bids),
            levels(asks)
        )
    }

    fn delta(venue: &str, market: &str, seq: u64, ts: i64, side: &str, price: &str, size: &str) -> String {
        format!(
            r#"{{"kind":"delta","venue":"{venue}","market_id":"{market}","side":"{side}","price":"{price}","size":"{size}","seq":{seq},"ts_local_ns":{ts},"ts_venue":null}}"#
        )
    }

    /// The books behind the Python scanner's 2026-10-22 line for this pair:
    /// buy Kalshi NO at 0.94 (its YES bid 0.06 x 19) and PM-US YES at 0.05.
    /// PM-US bids sit at 0.02 so the tests can offer down to 0.03 without
    /// locking that book.
    fn crossing(ts: i64) -> [String; 2] {
        [
            snap("kalshi", K, 1, ts, &[("0.06", "19")], &[("0.07", "500")]),
            snap("polymarket_us", P, 1, ts, &[("0.02", "500")], &[("0.05", "300")]),
        ]
    }

    /// Feed lines exactly as `drain` does, returning what would be written.
    fn run(scan: &mut Scanner, lines: &[String]) -> Vec<(i64, serde_json::Value)> {
        let mut out = Vec::new();
        for l in lines {
            scan.on_line(l, &mut out);
            scan.price(&mut out);
        }
        out.into_iter()
            .map(|(ts, l)| (ts, serde_json::from_str(&l).expect("record is JSON")))
            .collect()
    }

    /// The record matches the Python scanner's field for field, with the
    /// engine's own fee curves landing on the same numbers for this book.
    #[test]
    fn a_record_has_the_scanner_shape_and_the_engines_prices() {
        let mut scan = Scanner::new(vec![rel(EngineScope::Quoted)]);
        let got = run(&mut scan, &crossing(DAY0 + 5));
        assert_eq!(got.len(), 1, "{got:?}");
        let (ts, r) = &got[0];
        assert_eq!(*ts, DAY0 + 5);
        let want = serde_json::json!({
            "relationship_id": "xvus-heisman-26-malachitoney", "tranche": "head",
            "basket": [
                {"venue": "kalshi", "market_id": K, "buy_side": "no", "role": "taker",
                 "vwap": "0.940000", "fee": "0.080000"},
                {"venue": "polymarket_us", "market_id": P, "buy_side": "yes", "role": "taker",
                 "vwap": "0.050000", "fee": "0.054150"}],
            "signature": "xvus-heisman-26-malachitoney:NY", "size": "19",
            "gross_cost": "18.810000", "fees": "0.134150", "min_payoff": "1.000000",
            "net_edge_total": "0.055850", "net_edge_per_contract": "0.002939",
            "bucket": "primary", "ts_local_ns": DAY0 + 5, "max_hold_close_time": null,
            "engine_scope": "quoted"
        });
        assert_eq!(*r, want);
    }

    #[test]
    fn the_scope_label_travels_with_the_pair() {
        for s in [EngineScope::Quoted, EngineScope::NotTradable, EngineScope::OutOfScope] {
            let mut scan = Scanner::new(vec![rel(s)]);
            let got = run(&mut scan, &crossing(DAY0));
            assert_eq!(got[0].1["engine_scope"], s.as_str());
        }
    }

    /// Only greenlit pairs inside `--rel-prefix` are `quoted`; rejected pairs,
    /// single-venue pairs and wider relationships are not priced at all.
    #[test]
    fn the_universe_is_every_two_leg_kalshi_pmus_pair_but_the_rejected() {
        let yaml = r#"
relationships:
  - {id: xvus-a, type: cross-venue-equivalent, verdict: equivalent, vetted_by: human,
     legs: [{venue: kalshi, market_id: K1}, {venue: polymarket_us, market_id: P1}]}
  - {id: other-b, type: cross-venue-equivalent, verdict: equivalent, vetted_by: agent,
     legs: [{venue: kalshi, market_id: K2}, {venue: polymarket_us, market_id: P2}]}
  - {id: xvus-c, type: cross-venue-equivalent, verdict: unreviewed, vetted_by: agent,
     legs: [{venue: polymarket_us, market_id: P3}, {venue: kalshi, market_id: K3}]}
  - {id: xvus-d, type: cross-venue-equivalent, verdict: rejected, vetted_by: human,
     legs: [{venue: kalshi, market_id: K4}, {venue: polymarket_us, market_id: P4}]}
  - {id: kalshi-e, type: implies, verdict: equivalent, vetted_by: human,
     legs: [{venue: kalshi, market_id: K5}, {venue: kalshi, market_id: K6}]}
  - {id: xv-f, type: cross-venue-equivalent, verdict: equivalent, vetted_by: human,
     legs: [{venue: kalshi, market_id: K7}, {venue: polymarket, market_id: T7}]}
"#;
        let dir = std::env::temp_dir().join(format!("opps-universe-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("registry.yaml");
        std::fs::write(&path, yaml).unwrap();
        let reg = arb_registry::Registry::load(path.to_str().unwrap()).unwrap();
        let got: Vec<(String, EngineScope)> = universe(&reg, &["xvus-".to_string()])
            .into_iter()
            .map(|(r, s)| (r.id, s))
            .collect();
        assert_eq!(
            got,
            vec![
                ("xvus-a".to_string(), EngineScope::Quoted),
                ("other-b".to_string(), EngineScope::OutOfScope),
                ("xvus-c".to_string(), EngineScope::NotTradable),
            ]
        );
        let unscoped: Vec<EngineScope> = universe(&reg, &[]).into_iter().map(|(_, s)| s).collect();
        assert_eq!(unscoped, vec![EngineScope::Quoted, EngineScope::Quoted, EngineScope::NotTradable]);
    }

    /// A book that ticks a thousand times inside one second yields one line for
    /// the change, carrying the best edge seen; a crossing that then sits
    /// unchanged costs one line a minute; and the latest state is not lost.
    #[test]
    fn volume_is_bounded_per_signature_and_the_best_edge_survives() {
        let mut scan = Scanner::new(vec![rel(EngineScope::Quoted)]);
        let t0 = DAY0 + 10 * NS_PER_MIN;
        assert_eq!(run(&mut scan, &crossing(t0)).len(), 1, "a new crossing is written at once");

        // 1000 deltas in 0.5s move PM-US ask depth behind the 19 the basket
        // takes, which prices identically; then a 0.04 offer appears and is
        // pulled, the one strictly better edge in the interval.
        let mut lines = Vec::new();
        for i in 0..1000u64 {
            let ts = t0 + 1_000 + (i as i64) * 500_000;
            let size = (300 + i % 7).to_string();
            lines.push(delta("polymarket_us", P, 2 + i, ts, "ask", "0.05", &size));
        }
        let peak = t0 + 600_000_000;
        lines.push(delta("polymarket_us", P, 1002, peak, "ask", "0.04", "19"));
        lines.push(delta("polymarket_us", P, 1003, peak + 1, "ask", "0.04", "0"));
        let mut written = Vec::new();
        for l in &lines {
            written.extend(run(&mut scan, std::slice::from_ref(l)));
        }
        assert!(written.is_empty(), "inside the gap nothing more is written: {written:?}");

        // The gap expires on the next line anywhere: the best edge of the
        // interval is written, stamped when it was seen.
        let later = snap("kalshi", "UNRELATED", 1, t0 + MIN_GAP_NS + 1, &[], &[]);
        let got = run(&mut scan, std::slice::from_ref(&later));
        assert_eq!(got.len(), 1, "{got:?}");
        assert_eq!(got[0].0, peak);
        assert_eq!(got[0].1["basket"][1]["vwap"], "0.040000");

        // The current state (0.05) differs from what was written, so it
        // follows one gap later.
        let got = run(&mut scan, &[snap("kalshi", "UNRELATED", 2, t0 + 2 * MIN_GAP_NS + 2, &[], &[])]);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1["basket"][1]["vwap"], "0.050000");

        // Five quiet minutes of unrelated feed: one heartbeat per minute.
        let mut beats = 0;
        for s in 0..300i64 {
            let ts = t0 + 3 * MIN_GAP_NS + s * 1_000_000_000;
            beats += run(&mut scan, &[snap("kalshi", "UNRELATED", 3 + s as u64, ts, &[], &[])]).len();
        }
        assert_eq!(beats, 5, "one line per UTC minute while unchanged");

        // A change that is still throttled when the crossing vanishes is
        // written as it goes, so a short spike is never lost.
        let t1 = t0 + 10 * NS_PER_MIN;
        assert_eq!(run(&mut scan, &[delta("polymarket_us", P, 1004, t1, "ask", "0.045", "19")]).len(), 1);
        let spike = run(&mut scan, &[delta("polymarket_us", P, 1005, t1 + 10, "ask", "0.03", "19")]);
        assert!(spike.is_empty(), "throttled");
        let gone = run(&mut scan, &[snap("kalshi", K, 9, t1 + 20, &[("0.01", "5")], &[("0.99", "5")])]);
        assert_eq!(gone.len(), 1, "{gone:?}");
        assert_eq!(gone[0].1["basket"][1]["vwap"], "0.030000");
        assert!(scan.open[0].is_empty(), "the crossing is closed");
    }

    /// FEED_DOWN empties the books: no heartbeat may claim a crossing the
    /// engine can no longer see, until snapshots rebuild it.
    #[test]
    fn a_feed_outage_clears_every_book() {
        let mut scan = Scanner::new(vec![rel(EngineScope::Quoted)]);
        assert_eq!(run(&mut scan, &crossing(DAY0)).len(), 1);
        let down = r#"{"kind":"feed_down","note":"read error","ts":1.0}"#.to_string();
        run(&mut scan, &[down]);
        assert!(scan.books.get(Venue::Kalshi, K).is_none());
        let quiet = run(&mut scan, &[snap("kalshi", "UNRELATED", 1, DAY0 + 2 * NS_PER_MIN, &[], &[])]);
        assert!(quiet.is_empty(), "no heartbeat after the outage: {quiet:?}");
        // a delta before the welcome snapshot is not a book
        let d = delta("polymarket_us", P, 2, DAY0 + 3 * NS_PER_MIN, "ask", "0.05", "300");
        assert!(run(&mut scan, &[d]).is_empty());
        assert_eq!(run(&mut scan, &crossing(DAY0 + 4 * NS_PER_MIN)).len(), 1);
    }

    /// Wait for the writer thread to have written `n` lines, so a test can
    /// act between two lines the way the engine would.
    fn wait_written(g: &Gauges, n: u64) {
        let t = std::time::Instant::now();
        while g.written.load(AtomicOrdering::Relaxed) < n {
            assert!(t.elapsed() < std::time::Duration::from_secs(10), "writer thread stalled");
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!("opps-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Through the real thread and files: each record lands in the file for
    /// its own UTC day, a crossing open across midnight is re-stated in the
    /// new day's file, and closing the emitter writes everything before it
    /// returns.
    #[test]
    fn files_roll_at_utc_midnight() {
        let dir = scratch("roll");
        let mut e = Emitter::spawn(Spec { dir: dir.to_string_lossy().into_owned(), rels: vec![rel(EngineScope::Quoted)] }, false);
        let before = DAY0 - 2 * NS_PER_MIN;
        for l in crossing(before) {
            e.offer(l);
        }
        e.offer(snap("kalshi", "UNRELATED", 1, DAY0 + 1, &[], &[]));
        e.close();
        let read = |day: &str| {
            std::fs::read_to_string(dir.join(format!("opportunities-{day}.jsonl"))).unwrap_or_default()
        };
        let (old, new) = (read("2026-10-21"), read("2026-10-22"));
        assert_eq!(old.lines().count(), 1, "{old}");
        assert_eq!(new.lines().count(), 1, "{new}");
        let first: serde_json::Value = serde_json::from_str(old.lines().next().unwrap()).unwrap();
        let next: serde_json::Value = serde_json::from_str(new.lines().next().unwrap()).unwrap();
        assert_eq!(first["ts_local_ns"], before);
        assert_eq!(next["ts_local_ns"], DAY0 + 1);
        assert_eq!(first["signature"], next["signature"]);
        assert_eq!(e.gauges(), serde_json::json!({"written": 2, "dropped": 0, "write_errors": 0}));
    }

    /// A line the engine could not queue empties the books before the next
    /// line is applied: the open crossing is forgotten rather than restated
    /// off a book that may have missed a delta.
    #[test]
    fn a_dropped_line_discards_the_books() {
        for dropped in [false, true] {
            let g = Arc::new(Gauges::default());
            let (tx, rx) = sync_channel::<String>(8);
            let dir = scratch(if dropped { "drop-yes" } else { "drop-no" });
            let spec = Spec { dir: dir.to_string_lossy().into_owned(), rels: vec![rel(EngineScope::Quoted)] };
            let g2 = g.clone();
            let h = std::thread::spawn(move || drain(spec, rx, &g2));
            for l in crossing(DAY0) {
                tx.send(l).unwrap();
            }
            wait_written(&g, 1);
            if dropped {
                g.dropped.fetch_add(1, AtomicOrdering::Relaxed);
            }
            tx.send(snap("kalshi", "UNRELATED", 1, DAY0 + 2 * NS_PER_MIN, &[], &[])).unwrap();
            drop(tx);
            h.join().unwrap();
            let want = if dropped { 1 } else { 2 };
            assert_eq!(g.written.load(AtomicOrdering::Relaxed), want, "dropped={dropped}");
        }
    }
}
