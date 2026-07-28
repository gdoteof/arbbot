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
mod fills;
mod hist;
mod ledger;
mod risk;
mod sink;
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
    /// Arm the venue order path. OFF by default and refused unless every
    /// precondition in `order_preconditions` holds — this engine has never
    /// placed an order, and the flag is the only thing that can change that.
    enable_orders: bool,
    /// Run the startup sweep against the live venues and EXIT, without ever
    /// quoting. The safe way to exercise the reconciliation path.
    sweep_only: bool,
    /// Credential suffixes for the order path (`--cred-suffix pmus=rs_trader`).
    cred_suffix: Vec<(String, String)>,
    /// Append-only trade ledger; open baskets seed the risk view's exposure.
    ledger: String,
    /// Capital config for the risk gate.
    exec_yaml: String,
    topics_yaml: String,
    /// `--balance venue=usd`, repeatable. The dry-run engine holds no
    /// credentials, so venue cash is supplied here; the per-venue cash check
    /// sees $0 for any venue omitted, which refuses its orders.
    balances: Vec<(String, String)>,
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
        enable_orders: false,
        sweep_only: false,
        cred_suffix: Vec::new(),
        ledger: "data/exec/trades.jsonl".into(),
        exec_yaml: "config/exec.yaml".into(),
        topics_yaml: "config/topics.yaml".into(),
        balances: Vec::new(),
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
            "--enable-orders" => a.enable_orders = true,
            "--sweep-only" => {
                a.enable_orders = true;
                a.sweep_only = true;
            }
            "--cred-suffix" => {
                let kv = it.next().expect("--cred-suffix venue=suffix");
                let (v, sfx) = kv.split_once('=').expect("--cred-suffix wants venue=suffix");
                a.cred_suffix.push((v.to_string(), sfx.to_string()));
            }
            "--ledger" => a.ledger = it.next().expect("--ledger value"),
            "--exec-config" => a.exec_yaml = it.next().expect("--exec-config value"),
            "--topics" => a.topics_yaml = it.next().expect("--topics value"),
            "--balance" => {
                let kv = it.next().expect("--balance venue=usd");
                let (v, amt) = kv.split_once('=').expect("--balance wants venue=usd");
                a.balances.push((v.to_string(), amt.to_string()));
            }
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
) -> (Vec<Quoter>, HashMap<(Venue, String), Vec<usize>>, HashMap<String, (String, String)>) {
    let reg = arb_registry::Registry::load(registry).expect("read registry");
    // THE GATE (card c9ac7d1d, exec/main.py:131-146): a relationship is
    // tradable only if it is HUMAN-vetted in the registry or explicitly
    // allowlisted in config/tradable.yaml. An agent verdict is not enough.
    // A missing allowlist file is an empty allowlist, which is the
    // conservative direction — it never widens the gate.
    let allow = arb_registry::Allowlist::load(tradable);
    let total = reg.relationships.len();
    let mut n_gated = 0usize;

    // Metadata for the risk view, keyed by id: oracle_risk scales the per-rel
    // cap and the type is the class-exposure key, and neither is carried on
    // `Rel`. Built from the FULL registry, not the quoting subset — a basket we
    // no longer quote still consumes capital.
    let mut rel_meta: HashMap<String, (String, String)> = HashMap::new();
    for r in &reg.relationships {
        rel_meta.insert(
            r.id.clone(),
            (
                r.oracle_risk.clone().unwrap_or_else(|| "high".into()),
                r.kind.clone().unwrap_or_else(|| "unknown".into()),
            ),
        );
    }

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
    (quoters, by_market, rel_meta)
}

/// Everything that must hold before this process may touch a venue.
///
/// Encoded here rather than in a runbook: a checklist that only exists in prose
/// is one nobody runs. Returns the sinks on success, or the list of what is
/// missing — never a partially-armed engine.
fn order_preconditions(
    args: &Args,
    bench: bool,
) -> Result<HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>, Vec<String>> {
    let mut missing: Vec<String> = Vec::new();

    if bench {
        missing.push("bench/replay mode can never place orders".into());
    }
    if args.socket.is_none() {
        missing.push("no --socket: orders require the live feed".into());
    }
    if args.balances.is_empty() {
        missing.push(
            "no --balance: the risk gate would see $0 cash and refuse everything anyway".into(),
        );
    }
    if args.health.is_empty() {
        missing.push("--health disabled: quoting on an unwatched feed".into());
    }
    // Exposure IS seeded from the ledger at startup — but only if it is
    // readable. An engine that cannot see the open book must not size into it.
    if let Err(e) = ledger::read(&args.ledger) {
        missing.push(format!("trade ledger unreadable, so exposure is unknown: {e}"));
    }
    // Both venues now push fills over their private WS channels (src/fills.rs).
    // Credentials are checked below when the sinks are built; a fill feed that
    // cannot authenticate is the same failure as an order path that cannot.
    //
    // Startup reconciliation is handled by `startup_sweep`, not by a
    // precondition: the engine starts from a clean book by CANCELLING whatever
    // is resting (Geoff's call, 2026-07-28). Arming aborts if that sweep cannot
    // be proven to have worked.

    if !missing.is_empty() {
        return Err(missing);
    }

    let mut sinks: HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> = HashMap::new();
    let suffix = |v: &str| {
        args.cred_suffix.iter().find(|(k, _)| k == v).map(|(_, s)| s.clone())
    };
    match build_kalshi_sink(suffix("kalshi").as_deref()) {
        Ok(s) => {
            sinks.insert(Venue::Kalshi, s);
        }
        Err(e) => missing.push(format!("kalshi: {e}")),
    }
    match build_pmus_sink(suffix("pmus").or_else(|| suffix("polymarket_us")).as_deref()) {
        Ok(s) => {
            sinks.insert(Venue::PolymarketUs, s);
        }
        Err(e) => missing.push(format!("polymarket_us: {e}")),
    }
    if missing.is_empty() { Ok(sinks) } else { Err(missing) }
}

/// Cancel every resting order on every armed venue, then PROVE the book is
/// empty before the engine is allowed to quote.
///
/// This destroys real orders, including any a previous run or another tool left
/// behind — which is the point, and why it only ever runs behind
/// `--enable-orders`. A 2xx from cancel-all is not proof; the resting list is,
/// and both venues' lists lag a write, so it polls.
async fn startup_sweep(
    sinks: &HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>,
) -> Result<(), String> {
    for (venue, sink) in sinks {
        let s = sink.clone();
        let before = tokio::task::spawn_blocking(move || s.resting_order_ids())
            .await
            .map_err(|e| format!("{venue:?}: sweep task panicked: {e}"))?
            .map_err(|e| format!("{venue:?}: cannot list resting orders: {e}"))?;
        if before.is_empty() {
            eprintln!("[exec] {venue:?}: nothing resting");
            continue;
        }
        eprintln!("[exec] {venue:?}: CANCELLING {} resting order(s): {}", before.len(),
                  before.join(" "));
        let s = sink.clone();
        tokio::task::spawn_blocking(move || s.cancel_all_open())
            .await
            .map_err(|e| format!("{venue:?}: sweep task panicked: {e}"))?
            .map_err(|e| format!("{venue:?}: cancel_all_open: {e}"))?;

        let mut left = before.clone();
        for _ in 0..10 {
            let s = sink.clone();
            left = tokio::task::spawn_blocking(move || s.resting_order_ids())
                .await
                .map_err(|e| format!("{venue:?}: sweep task panicked: {e}"))?
                .map_err(|e| format!("{venue:?}: cannot list resting orders: {e}"))?;
            if left.is_empty() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
        if !left.is_empty() {
            return Err(format!(
                "{venue:?}: {} order(s) SURVIVED the sweep: {}",
                left.len(),
                left.join(" ")
            ));
        }
        eprintln!("[exec] {venue:?}: book is clean");
    }
    Ok(())
}

fn credential(name: &str) -> Result<String, String> {
    let dir = std::env::var_os("CREDENTIALS_DIRECTORY")
        .or_else(|| std::env::var_os("ARBBOT_CREDENTIALS_DIR"))
        .ok_or("no credentials dir (set ARBBOT_CREDENTIALS_DIR)")?;
    let p = std::path::PathBuf::from(dir).join(name);
    std::fs::read(&p)
        .map_err(|e| format!("missing credential {name}: {e}"))
        .map(|b| String::from_utf8_lossy(&b).trim().to_string())
}

fn build_kalshi_sink(
    suffix: Option<&str>,
) -> Result<std::sync::Arc<dyn sink::OrderSink>, String> {
    let (id, pem) = match suffix {
        Some(s) => (format!("kalshi_{s}_api_key_id"), format!("kalshi_{s}_private_key.pem")),
        None => ("kalshi_api_key_id".into(), "kalshi_private_key.pem".into()),
    };
    let signer = arb_venue::KalshiSigner::from_pkcs8_pem(credential(&id)?, &credential(&pem)?)
        .map_err(|e| e.to_string())?;
    let transport =
        arb_venue::transport::HttpTransport::new("https://api.elections.kalshi.com/trade-api/v2", 15)
            .map_err(|e| e.to_string())?;
    Ok(std::sync::Arc::new(arb_venue::gateway::KalshiGateway::with_transport(
        signer,
        arb_venue::ratelimit::RateLimiter::from_per_minute(60.0, 60.0, 0),
        transport,
    )))
}

fn build_pmus_sink(
    suffix: Option<&str>,
) -> Result<std::sync::Arc<dyn sink::OrderSink>, String> {
    let infix = suffix.map(|s| format!("_{s}")).unwrap_or_default();
    let signer = arb_venue::PmusSigner::from_secret_b64(
        credential(&format!("polymarket_usa{infix}_key_id"))?,
        &credential(&format!("polymarket_usa{infix}_private_key"))?,
    )
    .map_err(|e| e.to_string())?;
    let transport = arb_venue::transport::HttpTransport::new("https://api.polymarket.us", 15)
        .map_err(|e| e.to_string())?;
    Ok(std::sync::Arc::new(arb_venue::gateway::PmusGateway::with_transport(
        signer,
        arb_venue::ratelimit::RateLimiter::from_per_minute(60.0, 60.0, 0),
        transport,
    )))
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

    let (mut quoters, by_market, rel_meta) =
        load_quoters(&args.registry, &args.tradable, &args.rel_prefixes);

    // Risk is OFF in bench/replay: those pin a decision digest, and a capital
    // gate is not part of that contract.
    let risk = (!bench).then(|| {
        let rv = std::sync::Arc::new(risk::RiskView::load(
            &args.exec_yaml,
            &args.topics_yaml,
            args.balances.clone(),
            rel_meta.iter().map(|(k, (o, _))| (k.clone(), o.clone())).collect(),
        ));
        eprintln!("[risk] {}", rv.describe());
        if args.balances.is_empty() {
            eprintln!(
                "[risk] NO --balance given: the per-venue cash check sees $0 and \
                 will refuse every order. Pass --balance kalshi=<usd> etc."
            );
        }
        // Seed exposure from the open baskets in the trade ledger
        // (exec/main.py:264). Without this the caps reset on every restart and
        // the engine believes the whole book is free.
        match ledger::read(&args.ledger) {
            Ok(recs) => {
                let open = ledger::open_exposure(recs);
                let mut total = 0.0;
                let mut unknown = 0usize;
                let mut seeded: Vec<(String, f64)> = Vec::new();
                for (rel_id, qty) in open {
                    // An id missing from the registry cannot be classified, so
                    // it is booked under `unknown`: it still counts toward the
                    // GLOBAL cap (which sums per-relationship exposure) without
                    // inflating a class cap it may not belong to. Python
                    // skipped these entirely, which understated the book.
                    let class = match rel_meta.get(&rel_id) {
                        Some((_, k)) => k.as_str(),
                        None => {
                            unknown += 1;
                            "unknown"
                        }
                    };
                    rv.record_open(&rel_id, class, qty);
                    total += qty;
                    seeded.push((rel_id, qty));
                }
                seeded.sort_by(|a, b| b.1.total_cmp(&a.1));
                eprintln!(
                    "[risk] seeded {:.0} open contracts across {} relationships from {}{}",
                    total,
                    seeded.len(),
                    args.ledger,
                    if unknown > 0 {
                        format!(" ({unknown} not in the registry, booked as `unknown`)")
                    } else {
                        String::new()
                    }
                );
                for (id, q) in seeded.iter().take(5) {
                    eprintln!("[risk]   {q:>7.0}  {id}");
                }
            }
            Err(e) => {
                // Fail LOUD: an unreadable ledger means unknown exposure, and
                // an engine that silently assumes zero would size up into a
                // book it cannot see.
                eprintln!("[risk] CANNOT READ THE TRADE LEDGER: {e}");
                eprintln!("[risk] exposure is UNKNOWN — caps cannot be trusted this run");
            }
        }
        for q in quoters.iter_mut() {
            q.set_risk(Some(rv.clone() as std::sync::Arc<dyn arb_core::quoter::RiskGate>));
        }
        rv
    });
    eprintln!(
        "arb-trader up: {} quoters, {} markets, mode={}",
        quoters.len(),
        by_market.len(),
        if bench { "bench" } else { "shadow" }
    );

    let (tx, rx) = tokio::sync::mpsc::channel::<feed::FeedMsg>(65536);
    // An extra sender keeps the channel OPEN, so the engine would never see the
    // feed's EOF — which in bench/replay means it never terminates. Only clone
    // when there is genuinely something to send back (order acks and fills).
    let tx_acks = args.enable_orders.then(|| tx.clone());
    if let Some(tape) = args.bench_tape.clone() {
        let (max, pace) = (args.max_events, args.pace_x);
        std::thread::spawn(move || feed::tape_feed(tape, max, pace, tx));
    } else if let Some(wal) = args.replay_wal.clone() {
        let max = args.max_events;
        std::thread::spawn(move || feed::wal_replay_feed(wal, max, tx));
    } else if let Some(sock) = args.socket.clone() {
        tokio::spawn(feed::socket_feed(sock, tx));
    }

    let sinks = if args.enable_orders {
        match order_preconditions(&args, bench) {
            Ok(s) => {
                if args.sweep_only {
                    eprintln!("[exec] sweep-only: reconciling the book, then exiting");
                } else {
                    eprintln!("[exec] ORDERS ARMED — this process can place real orders");
                }
                s
            }
            Err(missing) => {
                eprintln!("[exec] --enable-orders REFUSED. Unmet preconditions:");
                for m in &missing {
                    eprintln!("[exec]   - {m}");
                }
                std::process::exit(9);
            }
        }
    } else {
        HashMap::new()
    };
    // Reconcile BEFORE anything can quote: the engine has no memory of orders a
    // previous run left resting, and it cannot cancel what it never had an id
    // for. Start from a book we know is empty.
    if !sinks.is_empty() {
        if let Err(e) = startup_sweep(&sinks).await {
            eprintln!("[exec] STARTUP SWEEP FAILED: {e}");
            eprintln!("[exec] refusing to arm: the book could not be proven clean");
            std::process::exit(10);
        }
        if args.sweep_only {
            eprintln!("[exec] --sweep-only: book reconciled, exiting without quoting");
            return;
        }
    }
    // The fill feed runs whenever the order path does: a live order with no
    // fill feed is an unhedged position waiting to happen.
    if !sinks.is_empty() {
        let sfx = args
            .cred_suffix
            .iter()
            .find(|(k, _)| k == "pmus" || k == "polymarket_us")
            .map(|(_, v)| v.clone());
        let infix = sfx.map(|s| format!("_{s}")).unwrap_or_default();
        match (
            credential(&format!("polymarket_usa{infix}_key_id")),
            credential(&format!("polymarket_usa{infix}_private_key")),
        ) {
            (Ok(kid), Ok(sec)) => {
                if let Some(t) = tx_acks.clone() {
                    tokio::spawn(fills::pmus_fill_feed(kid, sec, t));
                }
            }
            (a, b) => {
                eprintln!(
                    "[fills] cannot start polymarket_us fill feed: {}",
                    a.err().or(b.err()).unwrap_or_default()
                );
            }
        }

        let ksfx = args.cred_suffix.iter().find(|(k, _)| k == "kalshi").map(|(_, v)| v.clone());
        let (kid_n, pem_n) = match &ksfx {
            Some(x) => (format!("kalshi_{x}_api_key_id"), format!("kalshi_{x}_private_key.pem")),
            None => ("kalshi_api_key_id".into(), "kalshi_private_key.pem".into()),
        };
        match (credential(&kid_n), credential(&pem_n)) {
            (Ok(kid), Ok(pem)) => {
                if let Some(t) = tx_acks.clone() {
                    tokio::spawn(fills::kalshi_fill_feed(kid, pem, t));
                }
            }
            (a, b) => {
                eprintln!(
                    "[fills] cannot start kalshi fill feed: {}",
                    a.err().or(b.err()).unwrap_or_default()
                );
            }
        }
    }
    let armed = !sinks.is_empty();
    let acks = if sinks.is_empty() { None } else { tx_acks.clone() };
    let (exec_txs, exec_stats) = exec::spawn_executors(rate, sinks, acks);
    let cfg = engine::RunCfg {
        out_path: args.out,
        kill_file: args.kill_file,
        stats_every_s: args.stats_every_s,
        bench,
        wal_path: args.wal,
        // bench/replay must stay byte-deterministic and have no live feed.
        health_file: (!bench && !args.health.is_empty()).then(|| args.health.clone()),
        risk: risk.clone(),
        // Only an ARMED engine books baskets. A dry run writing here would
        // invent exposure that the next startup would seed from as if real.
        ledger_path: (!exec_txs.is_empty() && armed).then(|| args.ledger.clone()),
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
        let (qs, _, _) = load_quoters(reg, allow, prefixes);
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

#[cfg(test)]
mod sweep_tests {
    use super::*;
    use arb_venue::gateway::{CancelRequest, PlaceRequest};
    use arb_venue::VenueError;
    use std::sync::Mutex;

    /// Reports `resting` until cancel_all_open is called, then reports
    /// `after_cancel` — so a sink that never clears models a failed sweep.
    struct MockSink {
        resting: Mutex<Vec<String>>,
        after_cancel: Vec<String>,
        cancelled: Mutex<bool>,
    }
    impl sink::OrderSink for MockSink {
        fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
            unreachable!("the sweep never places")
        }
        fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
            unreachable!("the sweep uses cancel_all_open")
        }
        fn cancel_all_open(&self) -> Result<(), VenueError> {
            *self.cancelled.lock().unwrap() = true;
            *self.resting.lock().unwrap() = self.after_cancel.clone();
            Ok(())
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
            Ok(self.resting.lock().unwrap().clone())
        }
    }

    fn sinks(
        resting: &[&str],
        after: &[&str],
    ) -> (HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>, std::sync::Arc<MockSink>) {
        let m = std::sync::Arc::new(MockSink {
            resting: Mutex::new(resting.iter().map(|s| s.to_string()).collect()),
            after_cancel: after.iter().map(|s| s.to_string()).collect(),
            cancelled: Mutex::new(false),
        });
        let mut h: HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> = HashMap::new();
        h.insert(Venue::Kalshi, m.clone());
        (h, m)
    }

    #[tokio::test]
    async fn a_clean_book_needs_no_cancel() {
        let (h, m) = sinks(&[], &[]);
        startup_sweep(&h).await.unwrap();
        assert!(!*m.cancelled.lock().unwrap(), "nothing resting => no cancel sent");
    }

    #[tokio::test]
    async fn resting_orders_are_cancelled_and_the_book_verified_empty() {
        let (h, m) = sinks(&["a", "b"], &[]);
        startup_sweep(&h).await.unwrap();
        assert!(*m.cancelled.lock().unwrap());
    }

    /// The property that makes this safe to arm behind: if the sweep cannot be
    /// PROVEN to have worked, arming must not proceed. A 2xx from cancel-all is
    /// not proof — the resting list is.
    #[tokio::test]
    async fn a_sweep_that_leaves_orders_resting_is_an_error() {
        let (h, _m) = sinks(&["a", "b"], &["b"]);
        let err = startup_sweep(&h).await.unwrap_err();
        assert!(err.contains("SURVIVED"), "{err}");
        assert!(err.contains('b'), "names what is left: {err}");
    }
}
