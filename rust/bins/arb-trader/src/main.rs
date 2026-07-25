//! arb-trader — the P3 execution shell around the parity-validated decision
//! core (arb-core). DRY-RUN ONLY in this phase: no venue order code path, no
//! credentials loaded (same posture as arb-recorder).
//!
//! Architecture (sans-IO core, single-writer engine):
//!   feed task (socket|tape) -> bounded channel -> engine task (books,
//!   quoters, kill/stats deadlines) -> per-venue executor tasks (rate
//!   budgets, dry-run gateway seam).
//!
//! Shadow soak (live feed from the Rust shadow recorder's socket):
//!   arb-trader --socket data/arbbot-rs.sock --registry config/registry.yaml \
//!       --out data/trader-rs/intents.jsonl
//!
//! Bench + shell digest gate (must reproduce arb-intent / Python
//! scripts/intent_replay.py byte-for-byte over the same tape — proves the
//! concurrent shell does not alter decisions):
//!   arb-trader --bench-tape merged-<day>.jsonl --registry config/registry.yaml \
//!       [--max-events N] [--out intents.jsonl]
//!
//! Engine-sequenced WAL (src/wal.rs) + byte-exact incident replay — the
//! replay's digest must equal the original run's:
//!   arb-trader --socket ... --wal data/trader-rs/wal.jsonl
//!   arb-trader --replay-wal data/trader-rs/wal.jsonl --registry ...

mod engine;
mod exec;
mod feed;
mod hist;
mod wal;

use arb_core::model::Venue;
use arb_core::quoter::Quoter;
use arb_core::scan::{Rel, RelLeg, RelType};
use serde::Deserialize;
use std::collections::HashMap;

#[derive(Deserialize)]
struct RegistryDoc {
    #[serde(default)]
    relationships: Vec<RelDoc>,
}
#[derive(Deserialize)]
struct RelDoc {
    id: String,
    #[serde(rename = "type")]
    rtype: String,
    #[serde(default = "default_verdict")]
    verdict: String,
    #[serde(default = "default_tranche")]
    tranche: String,
    legs: Vec<LegDoc>,
}
fn default_verdict() -> String {
    "rejected".into()
}
fn default_tranche() -> String {
    "long-tail".into()
}
#[derive(Deserialize)]
struct LegDoc {
    venue: String,
    market_id: String,
}

struct Args {
    socket: Option<String>,
    bench_tape: Option<String>,
    /// Replay a WAL produced by --wal through the identical engine path.
    replay_wal: Option<String>,
    /// Engine-sequenced write-ahead log path (see src/wal.rs).
    wal: Option<String>,
    registry: String,
    out: Option<String>,
    max_events: u64,
    pace_x: f64,
    kill_file: String,
    stats_every_s: u64,
    rate_per_s: f64,
    /// Quote only relationships whose id starts with one of these prefixes
    /// (repeatable). Empty = full registry (existing behavior). Lets the
    /// shadow mirror the live runner's --relationship universe so the daily
    /// decision gate compares like against like.
    rel_prefixes: Vec<String>,
}

fn parse_args() -> Args {
    let mut a = Args {
        socket: None,
        bench_tape: None,
        replay_wal: None,
        wal: None,
        registry: "config/registry.yaml".into(),
        out: None,
        max_events: 0,
        pace_x: 0.0,
        kill_file: "data/KILL".into(),
        stats_every_s: 60,
        rate_per_s: -1.0, // sentinel: default by mode below
        rel_prefixes: Vec::new(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--socket" => a.socket = it.next(),
            "--bench-tape" => a.bench_tape = it.next(),
            "--replay-wal" => a.replay_wal = it.next(),
            "--wal" => a.wal = it.next(),
            "--registry" => a.registry = it.next().expect("--registry value"),
            "--out" => a.out = it.next(),
            "--max-events" => {
                a.max_events = it.next().expect("n").parse().expect("int")
            }
            "--pace-x" => a.pace_x = it.next().expect("x").parse().expect("float"),
            "--kill-file" => a.kill_file = it.next().expect("--kill-file value"),
            "--stats-every" => {
                a.stats_every_s = it.next().expect("s").parse().expect("int")
            }
            "--rate-limit" => {
                a.rate_per_s = it.next().expect("per-s").parse().expect("float")
            }
            "--rel-prefix" => {
                a.rel_prefixes.push(it.next().expect("--rel-prefix value"))
            }
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    let sources =
        [&a.socket, &a.bench_tape, &a.replay_wal].iter().filter(|s| s.is_some()).count();
    if sources != 1 {
        eprintln!("exactly one of --socket, --bench-tape or --replay-wal is required");
        std::process::exit(2);
    }
    a
}

fn load_quoters(
    registry: &str,
    rel_prefixes: &[String],
) -> (Vec<Quoter>, HashMap<(Venue, String), Vec<usize>>) {
    let text = std::fs::read_to_string(registry).expect("read registry");
    let doc: RegistryDoc = serde_yaml::from_str(&text).expect("parse registry");
    let quoters: Vec<Quoter> = doc
        .relationships
        .into_iter()
        .filter(|r| r.legs.len() == 2 && r.verdict != "rejected")
        .filter(|r| {
            rel_prefixes.is_empty() || rel_prefixes.iter().any(|p| r.id.starts_with(p.as_str()))
        })
        .filter_map(|r| {
            Some(Quoter::new(Rel {
                id: r.id,
                rtype: RelType::from_str(&r.rtype)?,
                tranche: r.tranche,
                legs: r
                    .legs
                    .into_iter()
                    .map(|l| {
                        Some(RelLeg {
                            venue: engine::parse_venue(&l.venue)?,
                            market_id: l.market_id,
                        })
                    })
                    .collect::<Option<Vec<_>>>()?,
            }))
        })
        .collect();
    let mut by_market: HashMap<(Venue, String), Vec<usize>> = HashMap::new();
    for (qi, q) in quoters.iter().enumerate() {
        for leg in &q.rel.legs {
            by_market.entry((leg.venue, leg.market_id.clone())).or_default().push(qi);
        }
    }
    (quoters, by_market)
}

#[tokio::main]
async fn main() {
    let args = parse_args();
    // replay is a bench-shaped run: offline source, digest emitted, so its
    // output can be diffed byte-for-byte against the run that wrote the WAL.
    let bench = args.bench_tape.is_some() || args.replay_wal.is_some();
    // rate budget: off in bench (pure decision throughput), 8/s/venue shadow
    let rate = if args.rate_per_s >= 0.0 {
        args.rate_per_s
    } else if bench {
        0.0
    } else {
        8.0
    };

    let (quoters, by_market) = load_quoters(&args.registry, &args.rel_prefixes);
    eprintln!(
        "arb-trader up: {} quoters, {} markets, mode={}",
        quoters.len(),
        by_market.len(),
        if bench { "bench" } else { "shadow" }
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<feed::FeedMsg>(65536);
    if let Some(tape) = args.bench_tape.clone() {
        let (max, pace) = (args.max_events, args.pace_x);
        std::thread::spawn(move || feed::tape_feed(tape, max, pace, tx));
    } else if let Some(wal) = args.replay_wal.clone() {
        let max = args.max_events;
        std::thread::spawn(move || feed::wal_replay_feed(wal, max, tx));
    } else if let Some(sock) = args.socket.clone() {
        tokio::spawn(feed::socket_feed(sock, tx));
    }

    let (exec_txs, exec_stats) = exec::spawn_executors(rate);
    let cfg = engine::RunCfg {
        out_path: args.out,
        kill_file: args.kill_file,
        stats_every_s: args.stats_every_s,
        bench,
        wal_path: args.wal,
    };
    let summary = engine::run(quoters, by_market, rx, exec_txs, exec_stats, cfg).await;
    println!("{summary}");
}
