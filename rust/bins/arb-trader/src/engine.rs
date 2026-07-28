//! Single-owner engine task: books + quoters + decision policy live here and
//! nowhere else (no locks). Consumes the feed channel; emits canonical intent
//! lines (identical bytes to arb-intent / scripts/intent_replay.py) and
//! routes effect commands to per-venue executors. Time-based behavior runs on
//! deadlines (tokio intervals) in the same select loop — kill-switch watch
//! and stats — never on per-event syscalls.

use crate::exec::{Action, ExecCmd, ExecStats};
use arb_venue::gateway::{CancelRequest, PlaceRequest, Side as VenueSide, Tif};
use crate::feed::FeedMsg;
use crate::hist::Hist;
use crate::wal::Wal;
use arb_core::book::{ApplyError, BookBuilder};
use arb_core::fees::FeeSchedule;
use arb_core::fill::{dropped_unconsumed, FillLedger, HedgeAnchor};
use arb_core::model::{BookSide, Level, Venue};
use arb_core::quoter::Quoter;
use arb_core::scan::{Cx, Rel};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use tokio::sync::mpsc;

pub struct RunCfg {
    pub out_path: Option<String>,
    pub kill_file: String,
    pub stats_every_s: u64,
    pub bench: bool,
    /// Engine-sequenced write-ahead log (crate::wal); None = off.
    pub wal_path: Option<String>,
    /// Recorder health feed to watch; None = check disabled (bench/replay,
    /// which have no live feed and must stay byte-deterministic).
    pub health_file: Option<String>,
    /// Shared risk view. The quoters consult it per place; the engine feeds it
    /// exposure on fills. None = risk off (bench/replay).
    pub risk: Option<std::sync::Arc<crate::risk::RiskView>>,
}

/// Feeds whose staleness invalidates pricing. A cross-venue quote on EITHER
/// venue is hedge-priced against the OTHER venue's book, so one stale critical
/// feed makes both sides wrong — which is why staleness pulls every quote, not
/// just the stale venue's.
const CRITICAL_FEEDS: [&str; 2] = ["kalshi-ws", "polymarket_us-ws"];

/// The recorder writes a health line per tick; the file is large, so read a
/// tail window rather than the whole thing.
fn last_line(path: &str, window: u64) -> Option<String> {
    use std::io::{Read, Seek, SeekFrom};
    let mut f = std::fs::File::open(path).ok()?;
    let len = f.metadata().ok()?.len();
    f.seek(SeekFrom::Start(len.saturating_sub(window))).ok()?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).ok()?;
    // The window can start mid-codepoint; lossy is fine, we only parse JSON.
    String::from_utf8_lossy(&buf).lines().last().map(|s| s.to_string())
}

/// `None` = feeds healthy; `Some(reason)` = pull the quotes.
///
/// FAIL-CLOSED, unlike the Python original (`exec/main.py` returned early and
/// left the state unchanged when the file could not be read). Python's version
/// caught the realistic failure — recorder dies, `ts` goes old — but a health
/// file that is deleted or never appears would leave it quoting forever on a
/// feed it cannot see. Refusing to quote when we cannot prove the feed is
/// healthy is the direction that cannot lose money.
fn feed_stale_reason(path: &str, now_wall: f64) -> Option<String> {
    let Some(line) = last_line(path, 4096) else {
        return Some(format!("health file {path} unreadable"));
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&line) else {
        return Some(format!("health file {path} has no parseable line"));
    };
    let ts = v.get("ts").and_then(|t| t.as_f64()).unwrap_or(0.0);
    let age = now_wall - ts;
    if age > 30.0 {
        // The health WRITER is silent, which means the recorder is down — a
        // strictly worse condition than any single feed going quiet.
        return Some(format!("recorder silent for {age:.0}s"));
    }
    let stale = v.get("stale");
    let bad: Vec<&str> = CRITICAL_FEEDS
        .iter()
        .copied()
        .filter(|f| {
            stale.and_then(|s| s.get(*f)).and_then(|b| b.as_bool()).unwrap_or(false)
        })
        .collect();
    (!bad.is_empty()).then(|| format!("{} stale", bad.join(", ")))
}

fn wall_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

pub fn parse_venue(s: &str) -> Option<Venue> {
    Some(match s {
        "kalshi" => Venue::Kalshi,
        "polymarket" => Venue::Polymarket,
        "polymarket_us" => Venue::PolymarketUs,
        _ => return None,
    })
}

fn levels_of(v: Option<&serde_json::Value>) -> Option<Vec<Level>> {
    let mut out = Vec::new();
    for l in v?.as_array()? {
        let price = l.get("price")?.as_str()?.to_owned();
        let size = l.get("size")?.as_str()?.to_owned();
        let p: f64 = price.parse().ok()?;
        if !(0.0..=1.0).contains(&p) {
            return None;
        }
        out.push(Level { price, size });
    }
    Some(out)
}

/// Quote-time hedge anchor for a maker order resting on `rel`'s
/// `market_id`/`side`: the top of the book on the OTHER leg, on the side the
/// hedge would TAKE. A maker bid that fills leaves us long, so the hedge sells
/// into the hedge leg's bid; a maker ask that fills leaves us short, so the
/// hedge lifts the hedge leg's ask. That is the same side selection
/// `Quoter::hedge_has_depth` gates places on, so a place intent always has a
/// live top level here — `HedgeAnchor::side` therefore names the hedge-leg
/// BOOK side whose price is in `HedgeAnchor::price`.
///
/// Captured at PLACE time, never at fill time (burst-gap postmortem: the burst
/// that fills you is the burst that gaps your book).
fn hedge_anchor(
    rel: &Rel,
    market_id: &str,
    side: &str,
    books: &BookBuilder,
    ts: f64,
) -> Option<HedgeAnchor> {
    let side: &'static str = match side {
        "bid" => "bid",
        "ask" => "ask",
        _ => return None,
    };
    let i = rel.legs.iter().position(|l| l.market_id == market_id)?;
    let hedge = rel.legs.get(1 - i)?;
    let book = books.get(hedge.venue, &hedge.market_id)?;
    let lvl = if side == "bid" { book.bids.first() } else { book.asks.first() }?;
    Some(HedgeAnchor {
        venue: hedge.venue.as_str(),
        market_id: hedge.market_id.clone(),
        side,
        price: lvl.price.clone(),
        ts,
    })
}

pub async fn run(
    mut quoters: Vec<Quoter>,
    by_market: HashMap<(Venue, String), Vec<usize>>,
    mut rx: mpsc::Receiver<FeedMsg>,
    exec_txs: HashMap<Venue, mpsc::Sender<ExecCmd>>,
    exec_stats: Arc<ExecStats>,
    cfg: RunCfg,
) -> serde_json::Value {
    let mut cx = Cx::default();
    let fees = FeeSchedule::new(&mut cx);
    let mut books = BookBuilder::new();
    let mut digest = Sha256::new();
    let decision = Hist::new();
    let (mut n_ev, mut n_book, mut n_int) = (0u64, 0u64, 0u64);
    let mut next_oid: u64 = 0;
    let mut intents: Vec<String> = Vec::new();
    let mut killed = false;
    // Feed-health pull (card 0a7e5478). Holds the REASON, not just a flag, so
    // a pulled engine can always say why it is silent. Starts pulled when the
    // check is on: we have not yet proven the feeds are healthy, and the first
    // tick either clears it or names the problem.
    let mut feed_reason: Option<String> =
        cfg.health_file.is_some().then(|| "startup — feeds not yet proven healthy".to_string());
    let mut last_now: f64 = 0.0;
    let mut chan_hw: usize = 0;
    let mut fills = FillLedger::new();
    // order id -> (relationship id, class). A fill arrives with our order id
    // only, but exposure is booked per relationship, so the mapping is captured
    // at place time when the rel is in hand.
    let mut order_rel: HashMap<String, (String, &'static str)> = HashMap::new();
    // venue's order id -> ours, learned from order_ack.
    let mut venue_oid: HashMap<String, String> = HashMap::new();
    let (mut n_ack, mut n_fill, mut n_hedge) = (0u64, 0u64, 0u64);
    let t_start = std::time::Instant::now();
    let mut wal = cfg.wal_path.as_deref().map(Wal::spawn);

    let mut out = cfg.out_path.as_ref().map(|p| {
        if let Some(dir) = std::path::Path::new(p).parent() {
            std::fs::create_dir_all(dir).expect("out dir");
        }
        std::io::BufWriter::new(
            std::fs::OpenOptions::new().create(true).append(true).open(p).expect("out"),
        )
    });

    let mut kill_iv = tokio::time::interval(std::time::Duration::from_secs(1));
    kill_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut stats_iv =
        tokio::time::interval(std::time::Duration::from_secs(cfg.stats_every_s.max(1)));
    stats_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut feed_iv = tokio::time::interval(std::time::Duration::from_secs(5));
    feed_iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // $rel: the relationship whose quoter emitted these intents (for the
    // hedge-anchor lookup at place time), or None for intents that rest
    // nothing (hedge obligations).
    macro_rules! drain_intents {
        ($rel:expr) => {
            for l in intents.drain(..) {
                digest.update(l.as_bytes());
                digest.update(b"\n");
                n_int += 1;
                if let Some(o) = out.as_mut() {
                    writeln!(o, "{l}").expect("write out");
                    if !cfg.bench {
                        o.flush().expect("flush out"); // tail -f visibility; ~80/day live
                    }
                }
                // route the effect to its venue executor (dry-run gateway seam)
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&l) {
                    // fill-ledger bookkeeping: orders enter the ledger at place
                    // time carrying their quote-time hedge anchor, so a later
                    // fill knows where to hedge without re-reading the book.
                    let ts_ev = v.get("ts").and_then(|x| x.as_f64()).unwrap_or(0.0);
                    if let (Some(mkt), Some(oid), Some(count)) = (
                        v.get("place").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                        v.get("count").and_then(|x| x.as_i64()),
                    ) {
                        let side = v.get("side").and_then(|x| x.as_str()).unwrap_or("");
                        let anchor = $rel
                            .and_then(|r| hedge_anchor(r, mkt, side, &books, ts_ev));
                        fills.register_order(oid, mkt, count, anchor);
                        if let Some(r) = $rel {
                            order_rel.insert(oid.to_string(), (r.id.clone(), r.rtype.as_str()));
                        }
                        // an amend retires the old id, but a fill can still
                        // race it — observe_cancel KEEPS the record.
                        if let Some(roid) = v.get("replaces").and_then(|x| x.as_str()) {
                            fills.observe_cancel(roid);
                        }
                    } else if let Some(oid) =
                        v.get("cancel").and(v.get("order_id")).and_then(|x| x.as_str())
                    {
                        fills.observe_cancel(oid);
                    }
                    // Build the REAL request from the intent. The quoter only
                    // ever rests post-only GTC makers, so tif/post_only are
                    // fixed here; a taker hedge will carry its own.
                    // `client_order_id` is our own order id, which is what makes
                    // a retried place idempotent at the venue.
                    let action = if let (Some(market), Some(oid)) = (
                        v.get("place").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                    ) {
                        Some(Action::Place(PlaceRequest {
                            market: market.to_string(),
                            side: match v.get("side").and_then(|x| x.as_str()) {
                                Some("ask") => VenueSide::Ask,
                                _ => VenueSide::Bid,
                            },
                            price: v
                                .get("price")
                                .and_then(|x| x.as_str())
                                .unwrap_or("0")
                                .to_string(),
                            qty: v.get("count").and_then(|x| x.as_i64()).unwrap_or(0),
                            tif: Tif::Gtc,
                            post_only: true,
                            client_order_id: oid.to_string(),
                        }))
                    } else if let (Some(market), Some(oid)) = (
                        v.get("cancel").and_then(|x| x.as_str()),
                        v.get("order_id").and_then(|x| x.as_str()),
                    ) {
                        // PM-US REQUIRES the market slug in the cancel body and
                        // we refuse to self-resolve it; the cancel intent
                        // carries it, so it rides along here. Kalshi ignores it.
                        Some(Action::Cancel(CancelRequest {
                            order_id: oid.to_string(),
                            market_slug: Some(market.to_string()),
                        }))
                    } else {
                        None
                    };
                    if let (Some(action), Some(venue)) = (
                        action,
                        v.get("venue").and_then(|x| x.as_str()).and_then(parse_venue),
                    ) {
                        if let Some(tx) = exec_txs.get(&venue) {
                            let cmd = ExecCmd {
                                t_read: std::time::Instant::now(),
                                action,
                            };
                            if tx.try_send(cmd).is_err() {
                                exec_stats
                                    .dropped
                                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        };
    }

    macro_rules! summary {
        () => {{
            let elapsed = t_start.elapsed().as_secs_f64();
            serde_json::json!({
                "mode": if cfg.bench { "bench" } else { "shadow" },
                "events": n_ev, "book_events": n_book, "intents": n_int,
                "killed": killed,
                "feed_pulled": feed_reason.is_some(),
                "risk_allowed": cfg.risk.as_ref().map(|r| r.stats().0).unwrap_or(0),
                "risk_rejected": cfg.risk.as_ref().map(|r| r.stats().1).unwrap_or(0),
                "order_acks": n_ack, "fills": n_fill, "hedge_obligations": n_hedge,
                // programming-bug alarm: an obligation that was minted and
                // never hedged (arb_core::fill) — must stay 0.
                "dropped_unconsumed": dropped_unconsumed(),
                "would_place": exec_stats.placed.load(std::sync::atomic::Ordering::Relaxed),
                "would_cancel": exec_stats.cancelled.load(std::sync::atomic::Ordering::Relaxed),
                "exec_dropped": exec_stats.dropped.load(std::sync::atomic::Ordering::Relaxed),
                "exec_sent": exec_stats.sent.load(std::sync::atomic::Ordering::Relaxed),
                "exec_failed": exec_stats.failed.load(std::sync::atomic::Ordering::Relaxed),
                "chan_high_water": chan_hw,
                "decision_latency": decision.summary(),
                "exec_hop_latency": exec_stats.hop.summary(),
                "elapsed_s": (elapsed * 10.0).round() / 10.0,
                "eps": if elapsed > 0.0 { (n_ev as f64 / elapsed) as u64 } else { 0 },
            })
        }};
    }

    loop {
        tokio::select! {
            biased;
            msg = rx.recv() => {
                let Some(m) = msg else { break }; // feed closed (bench EOF)
                n_ev += 1;
                chan_hw = chan_hw.max(rx.len());
                // THE merge point: everything that reaches the engine passes
                // here exactly once, so this is where the WAL sequence is
                // assigned — before any parsing, so lines the engine skips are
                // still in the incident record verbatim.
                if let Some(w) = wal.as_mut() {
                    w.append(&m.line);
                }
                let Ok(v) = serde_json::from_str::<serde_json::Value>(&m.line) else { continue };
                let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
                let Some(venue) = v.get("venue").and_then(|x| x.as_str()).and_then(parse_venue)
                else { continue };
                let Some(market_id) = v.get("market_id").and_then(|x| x.as_str()).map(str::to_owned)
                else { continue };
                let seq = v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0);
                let ts_local_ns = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
                let ts_venue = v.get("ts_venue").and_then(|x| x.as_str()).map(str::to_owned);
                match kind {
                    "snapshot" => {
                        let (Some(bids), Some(asks)) =
                            (levels_of(v.get("bids")), levels_of(v.get("asks")))
                        else { continue };
                        books.apply_snapshot(venue, &market_id, bids, asks, seq, ts_local_ns, ts_venue);
                    }
                    "delta" => {
                        let side = match v.get("side").and_then(|x| x.as_str()) {
                            Some("bid") => BookSide::Bid,
                            Some("ask") => BookSide::Ask,
                            _ => continue,
                        };
                        let (Some(price), Some(size)) = (
                            v.get("price").and_then(|x| x.as_str()),
                            v.get("size").and_then(|x| x.as_str()),
                        ) else { continue };
                        match books.apply_delta(venue, &market_id, side, price, size, seq,
                                                ts_local_ns, ts_venue) {
                            Ok(_) => {}
                            Err(ApplyError::GapDetected { .. }) | Err(ApplyError::NotSynced) => continue,
                        }
                    }
                    // Own-order lifecycle events (P4 item 1). Schema, both
                    // kinds, on the SAME ordered channel as book events:
                    //   {"kind":"order_ack","venue":<kalshi|polymarket|
                    //    polymarket_us>,"market_id":str,"order_id":str,
                    //    "ts_local_ns":int}
                    //   {"kind":"fill","venue":...,"market_id":str,
                    //    "order_id":str,"cum":int,"ts_local_ns":int}
                    // `cum` is the venue's CUMULATIVE filled count for that
                    // order — not a delta — which is what makes the private-WS
                    // and poll paths idempotent against each other
                    // (arb_core::fill). `order_id` is ours (the id in the place
                    // intent). Unknown kinds keep being skipped.
                    "order_ack" => {
                        // The ledger already registered the order at place
                        // time (ids are ours), so an ack changes no decision
                        // state and emits no intent: digest-invisible.
                        //
                        // It carries ONE thing the engine cannot know
                        // otherwise: the venue's id for our order. Fills arrive
                        // under that id, so without this mapping a fill on a
                        // live order would match nothing and the hedge would
                        // never fire.
                        if let (Some(ours), Some(theirs)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("venue_order_id").and_then(|x| x.as_str()),
                        ) {
                            venue_oid.insert(theirs.to_string(), ours.to_string());
                        }
                        n_ack += 1;
                        last_now = ts_local_ns as f64 / 1e9;
                        decision.record(m.t_read.elapsed().as_nanos() as u64);
                        continue;
                    }
                    "fill" => {
                        let (Some(reported), Some(cum)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("cum").and_then(|x| x.as_i64()),
                        ) else { continue };
                        // A venue reports its own id; the ledger knows ours.
                        // Fall through to the reported id when it is already
                        // ours (the dry-run/replay case, and the poll path
                        // which looks orders up by our id).
                        let oid: &str =
                            venue_oid.get(reported).map(|s| s.as_str()).unwrap_or(reported);
                        n_fill += 1;
                        let now = ts_local_ns as f64 / 1e9;
                        last_now = now;
                        if let Some(ob) = fills.observe_cum_fill(oid, cum) {
                            // Book the new exposure BEFORE the hedge intent, so
                            // the next quote sees capital this fill just spent.
                            if let (Some(rv), Some((rid, class))) =
                                (cfg.risk.as_ref(), order_rel.get(oid))
                            {
                                rv.record_open(rid, class, ob.qty() as f64);
                            }
                            // No anchor => no hedge target. The obligation is
                            // deliberately left unconsumed so the ledger's
                            // dropped_unconsumed() alarm surfaces it instead of
                            // an exposed leg vanishing silently.
                            if let Some(a) = ob.anchor().cloned() {
                                let (f_oid, _order_market, qty, _) = ob.into_parts();
                                n_hedge += 1;
                                intents.push(
                                    json!({"hedge_needed": a.market_id, "order_id": f_oid,
                                           "qty": qty, "anchor_price": a.price, "ts": now})
                                    .to_string(),
                                );
                                // The obligation surface only: this line IS the
                                // exposure record. Hedge PLACEMENT policy
                                // arrives with the venue write path.
                                drain_intents!(Option::<&Rel>::None);
                            }
                        }
                        decision.record(m.t_read.elapsed().as_nanos() as u64);
                        continue;
                    }
                    _ => continue,
                }
                n_book += 1;
                let now = ts_local_ns as f64 / 1e9;
                last_now = now;
                if !killed && feed_reason.is_none() {
                    if let Some(idxs) = by_market.get(&(venue, market_id)) {
                        for &qi in idxs {
                            quoters[qi].on_book(&mut cx, &fees, &books, now, &mut next_oid, &mut intents);
                            drain_intents!(Some(&quoters[qi].rel));
                        }
                    }
                }
                decision.record(m.t_read.elapsed().as_nanos() as u64);
            }
            _ = kill_iv.tick() => {
                let kill_now = std::path::Path::new(&cfg.kill_file).exists();
                if kill_now && !killed {
                    killed = true;
                    eprintln!("[engine] KILL switch on ({}) — cancelling all resting quotes", cfg.kill_file);
                    for q in quoters.iter_mut() {
                        q.cancel_all(&mut cx, last_now, &mut intents);
                        drain_intents!(Some(&q.rel));
                    }
                } else if !kill_now && killed {
                    killed = false;
                    eprintln!("[engine] KILL switch cleared — quoting resumes");
                }
            }
            _ = feed_iv.tick(), if cfg.health_file.is_some() => {
                let path = cfg.health_file.as_deref().expect("guarded above");
                let was_stale = feed_reason.is_some();
                let now_reason = feed_stale_reason(path, wall_now());
                // Log on any CHANGE of reason, not just healthy<->stale: an
                // engine that is silent must always be able to say why, and
                // "unreadable path" vs "recorder silent" are different bugs.
                if now_reason != feed_reason {
                    match &now_reason {
                        Some(why) => {
                            eprintln!("[engine] FEED STALE ({why}) — quotes pulled");
                            // Only sweep on the way IN; a stale->stale reason
                            // change has nothing resting left to cancel.
                            if !was_stale {
                                for q in quoters.iter_mut() {
                                    q.cancel_all(&mut cx, last_now, &mut intents);
                                    drain_intents!(Some(&q.rel));
                                }
                            }
                        }
                        None => eprintln!("[engine] feeds healthy — quoting resumes"),
                    }
                    feed_reason = now_reason;
                }
            }
            _ = stats_iv.tick(), if !cfg.bench => {
                println!("{}", summary!());
                if let Some(o) = out.as_mut() { o.flush().expect("flush"); }
            }
        }
    }

    if let Some(o) = out.as_mut() {
        o.flush().expect("final flush");
    }
    let mut s = summary!();
    if cfg.bench {
        s["sha256"] = serde_json::json!(format!("{:x}", digest.finalize()));
    }
    s
}

#[cfg(test)]
mod feed_health_tests {
    use super::*;
    use std::io::Write;

    fn health_file(lines: &[&str]) -> (tempdir::Dir, String) {
        let d = tempdir::Dir::new();
        let p = d.path().join("health.jsonl");
        let mut f = std::fs::File::create(&p).unwrap();
        for l in lines {
            writeln!(f, "{l}").unwrap();
        }
        let s = p.to_string_lossy().to_string();
        (d, s)
    }

    const NOW: f64 = 1_000_000.0;

    fn line(ts: f64, kalshi: bool, pmus: bool) -> String {
        format!(
            r#"{{"ts":{ts},"stale":{{"kalshi-ws":{kalshi},"polymarket_us-ws":{pmus},"polymarket-ws":true}}}}"#
        )
    }

    #[test]
    fn healthy_feeds_do_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }

    /// polymarket (INTL) is not a critical feed — the money path is Kalshi and
    /// PM-US, so intl staleness must not pull quotes. The fixture always sets
    /// polymarket-ws stale.
    #[test]
    fn a_non_critical_feed_going_stale_does_not_pull() {
        let (_d, p) = health_file(&[&line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }

    #[test]
    fn either_critical_feed_pulls_all_quotes() {
        for (k, pm, want) in [(true, false, "kalshi-ws"), (false, true, "polymarket_us-ws")] {
            let (_d, p) = health_file(&[&line(NOW - 1.0, k, pm)]);
            let why = feed_stale_reason(&p, NOW).expect("must pull");
            assert!(why.contains(want), "{why}");
        }
    }

    /// The health writer going quiet means the recorder is down — worse than
    /// any single feed, and the flags in the last line are stale evidence.
    #[test]
    fn a_silent_recorder_is_stale_even_when_the_last_line_looked_healthy() {
        let (_d, p) = health_file(&[&line(NOW - 120.0, false, false)]);
        let why = feed_stale_reason(&p, NOW).expect("must pull");
        assert!(why.contains("recorder silent"), "{why}");
    }

    /// Only the LAST line counts; an old healthy line must not rescue a new
    /// stale one.
    #[test]
    fn only_the_last_line_is_read() {
        let (_d, p) = health_file(&[&line(NOW - 5.0, false, false), &line(NOW - 1.0, true, false)]);
        assert!(feed_stale_reason(&p, NOW).is_some(), "the newest line is stale");
    }

    /// FAIL-CLOSED: no file, no readable line, or garbage all pull the quotes.
    /// Python left the state unchanged here, which would quote forever on a
    /// feed it could not see.
    #[test]
    fn an_unreadable_health_file_pulls_quotes() {
        assert!(feed_stale_reason("/nonexistent/health.jsonl", NOW).is_some());
        let (_d, p) = health_file(&["not json at all"]);
        assert!(feed_stale_reason(&p, NOW).is_some());
        let (_d2, p2) = health_file(&[]);
        assert!(feed_stale_reason(&p2, NOW).is_some(), "an empty file proves nothing");
    }

    /// A line with no `ts` is treated as infinitely old, not as ts=now.
    #[test]
    fn a_line_without_a_timestamp_is_stale() {
        let (_d, p) = health_file(&[r#"{"stale":{"kalshi-ws":false,"polymarket_us-ws":false}}"#]);
        assert!(feed_stale_reason(&p, NOW).is_some());
    }

    /// The tail window can start mid-codepoint; that must not panic or hide a
    /// healthy line.
    #[test]
    fn a_large_file_reads_only_its_tail() {
        let pad = format!(r#"{{"ts":1,"note":"{}"}}"#, "é".repeat(3000));
        let (_d, p) = health_file(&[&pad, &line(NOW - 1.0, false, false)]);
        assert_eq!(feed_stale_reason(&p, NOW), None);
    }
}

/// Minimal scratch dir for tests (no dev-dependency needed).
#[cfg(test)]
mod tempdir {
    pub struct Dir(std::path::PathBuf);
    impl Dir {
        pub fn new() -> Dir {
            let base = std::env::temp_dir().join(format!(
                "arb-trader-test-{}-{:?}",
                std::process::id(),
                std::thread::current().id()
            ));
            let _ = std::fs::remove_dir_all(&base);
            std::fs::create_dir_all(&base).unwrap();
            Dir(base)
        }
        pub fn path(&self) -> &std::path::Path {
            &self.0
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}
