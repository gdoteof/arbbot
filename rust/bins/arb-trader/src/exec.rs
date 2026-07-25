//! Per-venue executor tasks — the effects boundary of the P3 shell.
//!
//! DRY-RUN ONLY: this binary contains NO venue order code path and loads NO
//! credentials (same posture as arb-recorder). Each executor owns its venue's
//! rate budget (token bucket) and records the engine->executor hop latency —
//! the point where a live gateway would write the order to the wire. A slow
//! venue therefore backs up ITS executor channel only; the engine never
//! blocks on venue I/O (the P1 postmortem's gap mechanism).

use crate::hist::Hist;
use arb_core::model::Venue;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::mpsc;

#[derive(Clone, Copy)]
pub enum Action {
    Place,
    Cancel,
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
}

pub fn spawn_executors(
    rate_per_s: f64,
) -> (HashMap<Venue, mpsc::Sender<ExecCmd>>, Arc<ExecStats>) {
    let stats = Arc::new(ExecStats {
        hop: Hist::new(),
        placed: AtomicU64::new(0),
        cancelled: AtomicU64::new(0),
        dropped: AtomicU64::new(0),
    });
    let mut txs = HashMap::new();
    for venue in [Venue::Kalshi, Venue::Polymarket, Venue::PolymarketUs] {
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
                match cmd.action {
                    Action::Place => st.placed.fetch_add(1, Ordering::Relaxed),
                    Action::Cancel => st.cancelled.fetch_add(1, Ordering::Relaxed),
                };
            }
        });
        txs.insert(venue, tx);
    }
    (txs, stats)
}
