//! Per-venue executor tasks — the effects boundary of the P3 shell.
//!
//! DRY-RUN BY DEFAULT: an executor with no [`OrderSink`] counts its command and
//! drops it, loading no credentials (arb-recorder's posture). A sink is only
//! ever supplied by `--enable-orders`, and only once its preconditions pass.
//!
//! Each executor owns its venue's rate budget (token bucket) and records the
//! engine->executor hop latency — the point where the order goes to the wire. A
//! slow venue therefore backs up ITS executor channel only; the engine never
//! blocks on venue I/O (the P1 postmortem's gap mechanism).

use crate::hist::Hist;
use crate::sink::OrderSink;
use arb_venue::gateway::{CancelRequest, PlaceRequest};
use arb_core::model::Venue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

/// The effect, carrying the ORDER — not just its shape.
///
/// This used to be a bare `Place`/`Cancel` with no market, price, quantity or
/// side: enough to measure the engine->executor hop, not enough to put an order
/// on a wire. The requests are arb-venue's venue-neutral types, so the same
/// value the executor counts today is the value a gateway will send.
pub enum Action {
    Place(PlaceRequest),
    Cancel(CancelRequest),
    /// Cancel EVERYTHING resting on this venue and verify it is gone.
    ///
    /// The kill switch's per-quote cancels only reach orders the engine still
    /// has ids for; this reaches the rest, and unlike a cancel it proves the
    /// outcome. Halting is the one moment where "probably cancelled" is not
    /// good enough.
    SweepAndVerify,
}

pub struct ExecCmd {
    pub t_read: Instant,
    pub action: Action,
}

pub struct ExecStats {
    pub hop: Hist,
    pub placed: AtomicU64,
    pub cancelled: AtomicU64,
    pub dropped: AtomicU64, // engine try_send failures (executor backlogged)
    /// Commands that actually reached a venue (0 in dry-run).
    pub sent: AtomicU64,
    /// Venue rejections/errors. Counted separately from `sent` so a venue that
    /// refuses everything cannot read as a working order path.
    pub failed: AtomicU64,
}

/// `acks` is the SAME channel the feed writes to. A venue reply is an event
/// like any other: it enters the one ordered channel, so it lands in the WAL
/// and replays with everything else.
pub fn spawn_executors(
    rate_per_s: f64,
    mut sinks: HashMap<Venue, Arc<dyn OrderSink>>,
    acks: Option<mpsc::Sender<crate::feed::FeedMsg>>,
) -> (HashMap<Venue, mpsc::Sender<ExecCmd>>, Arc<ExecStats>) {
    let stats = Arc::new(ExecStats {
        hop: Hist::new(),
        placed: AtomicU64::new(0),
        cancelled: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
        sent: AtomicU64::new(0),
        failed: AtomicU64::new(0),
    });
    let mut txs = HashMap::new();
    for venue in [Venue::Kalshi, Venue::Polymarket, Venue::PolymarketUs] {
        let sink = sinks.remove(&venue);
        let acks = acks.clone();
        let (tx, mut rx) = mpsc::channel::<ExecCmd>(1024);
        let st = stats.clone();
        tokio::spawn(async move {
            let mut tokens = rate_per_s.max(0.0);
            let mut last = Instant::now();
            while let Some(cmd) = rx.recv().await {
                st.hop.record(cmd.t_read.elapsed().as_nanos() as u64);
                if rate_per_s > 0.0 {
                    // token bucket: this venue's API budget, owned here
                    loop {
                        let now = Instant::now();
                        tokens = (tokens
                            + now.duration_since(last).as_secs_f64() * rate_per_s)
                            .min(rate_per_s);
                        last = now;
                        if tokens >= 1.0 {
                            tokens -= 1.0;
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                }
                match &cmd.action {
                    Action::Place(_) => st.placed.fetch_add(1, Ordering::Relaxed),
                    Action::Cancel(_) | Action::SweepAndVerify => {
                        st.cancelled.fetch_add(1, Ordering::Relaxed)
                    }
                };
                let Some(sink) = sink.clone() else { continue }; // dry-run: counted, dropped
                // Not a per-order verb: it owns its own blocking + polling, so
                // it is handled before the place/cancel dispatch below.
                if matches!(cmd.action, Action::SweepAndVerify) {
                    match crate::sink::cancel_all_and_verify(sink).await {
                        Ok(()) => eprintln!("[exec] {venue:?}: kill sweep verified clean"),
                        Err(e) => {
                            st.failed.fetch_add(1, Ordering::Relaxed);
                            eprintln!("[exec] {venue:?}: KILL SWEEP FAILED — {e}");
                        }
                    }
                    continue;
                }
                // The gateways block; running one on this worker would stall
                // every other task on it.
                let st2 = st.clone();
                // (our order id, market) for the ack: a fill arrives under the
                // VENUE's id, and this is the only place both are in hand.
                let ours = match &cmd.action {
                    Action::Place(p) => Some((p.client_order_id.clone(), p.market.clone())),
                    Action::Cancel(_) | Action::SweepAndVerify => None,
                };
                let res = tokio::task::spawn_blocking(move || match &cmd.action {
                    Action::Place(p) => sink.place(p).map(Some),
                    Action::Cancel(c) => sink.cancel(c).map(|_| None),
                    // handled above, before this dispatch
                    Action::SweepAndVerify => Ok(None),
                })
                .await;
                match res {
                    Ok(Ok(venue_oid)) => {
                        st2.sent.fetch_add(1, Ordering::Relaxed);
                        match (venue_oid, ours) {
                            (Some(vid), Some((our_id, market))) => {
                                eprintln!("[exec] {venue:?} placed {our_id} -> {vid}");
                                if let Some(tx) = &acks {
                                    let line = serde_json::json!({
                                        "kind": "order_ack",
                                        "venue": venue.as_str(),
                                        "market_id": market,
                                        "order_id": our_id,
                                        "venue_order_id": vid,
                                        "ts_local_ns": now_ns(),
                                    })
                                    .to_string();
                                    let _ = tx
                                        .send(crate::feed::FeedMsg {
                                            line,
                                            t_read: Instant::now(),
                                        })
                                        .await;
                                }
                            }
                            _ => eprintln!("[exec] {venue:?} cancelled"),
                        }
                    }
                    Ok(Err(e)) => {
                        st2.failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!("[exec] {venue:?} FAILED: {e}");
                    }
                    Err(e) => {
                        st2.failed.fetch_add(1, Ordering::Relaxed);
                        eprintln!("[exec] {venue:?} task panicked: {e}");
                    }
                }
            }
        });
        txs.insert(venue, tx);
    }
    (txs, stats)
}
