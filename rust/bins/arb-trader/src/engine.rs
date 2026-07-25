//! Single-owner engine task: books + quoters + decision policy live here and
//! nowhere else (no locks). Consumes the feed channel; emits canonical intent
//! lines (identical bytes to arb-intent / scripts/intent_replay.py) and
//! routes effect commands to per-venue executors. Time-based behavior runs on
//! deadlines (tokio intervals) in the same select loop — kill-switch watch
//! and stats — never on per-event syscalls.

use crate::exec::{Action, ExecCmd, ExecStats};
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
    let mut last_now: f64 = 0.0;
    let mut chan_hw: usize = 0;
    let mut fills = FillLedger::new();
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
                    let action = if v.get("place").is_some() {
                        Some(Action::Place)
                    } else if v.get("cancel").is_some() {
                        Some(Action::Cancel)
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
                "order_acks": n_ack, "fills": n_fill, "hedge_obligations": n_hedge,
                // programming-bug alarm: an obligation that was minted and
                // never hedged (arb_core::fill) — must stay 0.
                "dropped_unconsumed": dropped_unconsumed(),
                "would_place": exec_stats.placed.load(std::sync::atomic::Ordering::Relaxed),
                "would_cancel": exec_stats.cancelled.load(std::sync::atomic::Ordering::Relaxed),
                "exec_dropped": exec_stats.dropped.load(std::sync::atomic::Ordering::Relaxed),
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
                        // time (ids are ours), so an ack is observation only:
                        // no state change, no intent, digest-invisible.
                        n_ack += 1;
                        last_now = ts_local_ns as f64 / 1e9;
                        decision.record(m.t_read.elapsed().as_nanos() as u64);
                        continue;
                    }
                    "fill" => {
                        let (Some(oid), Some(cum)) = (
                            v.get("order_id").and_then(|x| x.as_str()),
                            v.get("cum").and_then(|x| x.as_i64()),
                        ) else { continue };
                        n_fill += 1;
                        let now = ts_local_ns as f64 / 1e9;
                        last_now = now;
                        if let Some(ob) = fills.observe_cum_fill(oid, cum) {
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
                if !killed {
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
