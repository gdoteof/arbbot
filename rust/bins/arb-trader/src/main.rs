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
use std::collections::HashMap;

struct Args {
    socket: Option<String>,
    bench_tape: Option<String>,
    /// Replay a WAL produced by --wal through the identical engine path.
    replay_wal: Option<String>,
    /// Engine-sequenced write-ahead log path (see src/wal.rs).
    wal: Option<String>,
    registry: String,
    /// Allowlist half of the tradable gate (config/tradable.yaml). A missing
    /// file is an EMPTY allowlist — the gate then falls back to registry
    /// human-vetting alone, which never widens it.
    tradable: String,
    /// Recorder health feed. Critical-feed staleness pulls ALL quotes, because
    /// cross-venue prices are hedge-priced against the other venue's book, so
    /// one stale feed invalidates BOTH sides. Empty string disables the check.
    health: String,
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
        tradable: "config/tradable.yaml".into(),
        health: "data/health.jsonl".into(),
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
            "--tradable" => a.tradable = it.next().expect("--tradable value"),
            "--health" => a.health = it.next().expect("--health value"),
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
    tradable: &str,
    rel_prefixes: &[String],
) -> (Vec<Quoter>, HashMap<(Venue, String), Vec<usize>>) {
    let reg = arb_registry::Registry::load(registry).expect("read registry");
    // THE GATE (card c9ac7d1d, exec/main.py:131-146): a relationship is
    // tradable only if it is HUMAN-vetted in the registry or explicitly
    // allowlisted in config/tradable.yaml. An agent verdict is not enough.
    // A missing allowlist file is an empty allowlist, which is the
    // conservative direction — it never widens the gate.
    let allow = arb_registry::Allowlist::load(tradable);
    let total = reg.relationships.len();
    let mut n_gated = 0usize;

    let quoters: Vec<Quoter> = reg
        .relationships
        .into_iter()
        .filter(|r| r.legs.len() == 2)
        .filter(|r| {
            let ok = r.tradable(&allow);
            if !ok {
                n_gated += 1;
            }
            ok
        })
        .filter(|r| {
            rel_prefixes.is_empty() || rel_prefixes.iter().any(|p| r.id.starts_with(p.as_str()))
        })
        .filter_map(|r| {
            Some(Quoter::new(Rel {
                id: r.id,
                rtype: RelType::from_str(r.kind.as_deref()?)?,
                tranche: r.tranche.unwrap_or_else(|| "long-tail".into()),
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

    eprintln!(
        "[gate] {total} relationships -> {} quoting; {n_gated} blocked (not human-vetted \
         and not in {tradable}, which lists {} ids)",
        quoters.len(),
        allow.len()
    );

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

    let (quoters, by_market) = load_quoters(&args.registry, &args.tradable, &args.rel_prefixes);
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
        // bench/replay must stay byte-deterministic and have no live feed.
        health_file: (!bench && !args.health.is_empty()).then(|| args.health.clone()),
    };
    let summary = engine::run(quoters, by_market, rx, exec_txs, exec_stats, cfg).await;
    println!("{summary}");
}

#[cfg(test)]
mod gate_tests {
    use super::*;

    fn scratch(name: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-trader-gate-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// One entry per gate outcome: human-vetted, agent-vetted, rejected, and
    /// an agent-vetted one that the allowlist rescues.
    const REGISTRY: &str = r#"
relationships:
  - id: human-ok
    type: cross-venue-equivalent
    verdict: equivalent
    vetted_by: human
    legs:
      - {venue: kalshi, market_id: K1}
      - {venue: polymarket_us, market_id: P1}
  - id: agent-only
    type: cross-venue-equivalent
    verdict: equivalent
    vetted_by: agent
    legs:
      - {venue: kalshi, market_id: K2}
      - {venue: polymarket_us, market_id: P2}
  - id: rejected-one
    type: cross-venue-equivalent
    verdict: rejected
    vetted_by: human
    legs:
      - {venue: kalshi, market_id: K3}
      - {venue: polymarket_us, market_id: P3}
  - id: allowlisted
    type: cross-venue-equivalent
    verdict: equivalent
    vetted_by: agent
    legs:
      - {venue: kalshi, market_id: K4}
      - {venue: polymarket_us, market_id: P4}
"#;

    fn ids(reg: &str, allow: &str, prefixes: &[String]) -> Vec<String> {
        let (qs, _) = load_quoters(reg, allow, prefixes);
        qs.into_iter().map(|q| q.rel.id).collect()
    }

    /// The gate (card c9ac7d1d): agent-vetted is NOT enough, and a `rejected`
    /// verdict never quotes however it was vetted.
    #[test]
    fn only_human_vetted_or_allowlisted_relationships_quote() {
        let d = scratch("basic");
        let reg = d.join("registry.yaml");
        std::fs::write(&reg, REGISTRY).unwrap();
        let allow = d.join("tradable.yaml");
        std::fs::write(&allow, "allow:\n  - allowlisted\n").unwrap();

        let got = ids(reg.to_str().unwrap(), allow.to_str().unwrap(), &[]);
        assert_eq!(got, vec!["human-ok".to_string(), "allowlisted".to_string()]);
    }

    /// A MISSING allowlist is an empty one — the gate narrows, never widens.
    /// This is the direction that matters: a typo'd path must not open the gate.
    #[test]
    fn a_missing_allowlist_file_narrows_the_gate() {
        let d = scratch("noallow");
        let reg = d.join("registry.yaml");
        std::fs::write(&reg, REGISTRY).unwrap();

        let got = ids(reg.to_str().unwrap(), "/nonexistent/tradable.yaml", &[]);
        assert_eq!(got, vec!["human-ok".to_string()], "only registry vetting survives");
    }

    /// The gate composes with --relationship rather than replacing it: the
    /// prefix filter can only ever narrow what the gate already permitted.
    #[test]
    fn the_prefix_filter_cannot_widen_the_gate() {
        let d = scratch("prefix");
        let reg = d.join("registry.yaml");
        std::fs::write(&reg, REGISTRY).unwrap();
        let allow = d.join("tradable.yaml");
        std::fs::write(&allow, "allow: []\n").unwrap();

        // asking for an agent-only rel by prefix still does NOT quote it
        let got = ids(reg.to_str().unwrap(), allow.to_str().unwrap(), &["agent".to_string()]);
        assert!(got.is_empty(), "prefix must not bypass the gate, got {got:?}");
    }
}
