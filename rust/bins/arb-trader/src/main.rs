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
mod taketake;
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
    /// Second, unmissable confirmation. `--enable-orders` ALONE is inert.
    ///
    /// This exists because of a real incident (2026-07-28): `--enable-orders`
    /// had been a safe way to PRINT unmet preconditions, and the moment the
    /// last one was cleared that same command armed the engine and rested 31
    /// live orders. A flag whose meaning silently changes from "explain" to
    /// "trade" is a trap; two flags cannot be typed by muscle memory.
    confirm_live: bool,
    /// Run the startup sweep against the live venues and EXIT, without ever
    /// quoting. The safe way to exercise the reconciliation path.
    sweep_only: bool,
    /// Credential suffixes for the order path (`--cred-suffix pmus=rs_trader`).
    cred_suffix: Vec<(String, String)>,
    /// Append-only trade ledger; open baskets seed the risk view's exposure.
    ledger: String,
    /// Seconds before an unfilled hedge is retried.
    hedge_retry_s: f64,
    /// How far worse than the anchor a retry may price. The anchor is where the
    /// basket was known profitable, so this is the edge we will give up to stop
    /// being naked; 0 means never give any up.
    hedge_max_slip: String,
    /// Seconds naked before it stops being a retry and becomes an alarm.
    hedge_alarm_s: f64,
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
    /// Run the take-take detector on every book event.
    take_take: bool,
    /// Detect and log crossings without ever placing — the shadow step. Also
    /// forced on whenever the order path is unarmed.
    tt_detect_only: bool,
    /// Per-relationship concentration cap for take-take, in contracts.
    tt_max_ct_per_rel: i64,
    /// Contracts per single take-take execution.
    tt_max_clip: i64,
    /// Marks file the blended-APR bar is derived from.
    marks: String,
}

/// The default Args, factored out of `parse_args` so the precondition tests can
/// build one without going through the command line.
fn default_args() -> Args {
    Args {
        socket: None,
        bench_tape: None,
        replay_wal: None,
        wal: None,
        registry: "config/registry.yaml".into(),
        tradable: "config/tradable.yaml".into(),
        health: "data/health.jsonl".into(),
        enable_orders: false,
        confirm_live: false,
        sweep_only: false,
        cred_suffix: Vec::new(),
        ledger: "data/exec/trades.jsonl".into(),
        hedge_retry_s: 5.0,
        hedge_max_slip: "0.01".into(),
        hedge_alarm_s: 60.0,
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
        take_take: false,
        tt_detect_only: false,
        // Python's auto_take_take.py defaults: 50ct/rel, clip 20.
        tt_max_ct_per_rel: 50,
        tt_max_clip: 20,
        marks: "data/exec/marks.json".into(),
    }
}

fn parse_args() -> Args {
    let mut a = default_args();
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
            "--yes-trade-live" => a.confirm_live = true,
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
            "--hedge-retry-s" => {
                a.hedge_retry_s = it.next().expect("s").parse().expect("float")
            }
            "--hedge-max-slip" => {
                a.hedge_max_slip = it.next().expect("--hedge-max-slip value")
            }
            "--hedge-alarm-s" => {
                a.hedge_alarm_s = it.next().expect("s").parse().expect("float")
            }
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
            "--take-take" => a.take_take = true,
            "--take-take-detect-only" => {
                a.take_take = true;
                a.tt_detect_only = true;
            }
            "--tt-max-ct-per-rel" => {
                a.tt_max_ct_per_rel =
                    it.next().and_then(|v| v.parse().ok()).expect("--tt-max-ct-per-rel value")
            }
            "--tt-max-clip" => {
                a.tt_max_clip = it.next().and_then(|v| v.parse().ok()).expect("--tt-max-clip value")
            }
            "--marks" => a.marks = it.next().expect("--marks value"),
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    let sources =
        [&a.socket, &a.bench_tape, &a.replay_wal].iter().filter(|s| s.is_some()).count();
    // `--sweep-only` never quotes, so it needs no event source at all. Requiring
    // one made the cancel tool harder to run than the trading path (2026-07-28:
    // clearing a leaked order needed a socket AND two invented balances).
    if sources > 1 || (sources == 0 && !a.sweep_only) {
        eprintln!(
            "exactly one of --socket, --bench-tape or --replay-wal is required \
             (--sweep-only needs none)"
        );
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
                            venue: Venue::parse(&l.venue)?,
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

/// Everything that must hold before this process may CANCEL — credentials, and
/// nothing else.
///
/// Split out from `order_preconditions` after a real incident (2026-07-28):
/// clearing the order the shutdown sweep had leaked meant running
/// `--sweep-only`, which refused without `--balance`, `--health`, `--socket` and
/// a readable ledger. None of those has anything to do with cancelling, so two
/// balance figures were INVENTED under time pressure in order to cancel a real
/// order. A safety check that obstructs the safety tool is a bug: the cancel
/// path must be the easiest thing in this binary to run.
fn cancel_preconditions(
    args: &Args,
    bench: bool,
) -> Result<HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>, Vec<String>> {
    let mut missing: Vec<String> = Vec::new();
    if bench {
        missing.push("bench/replay mode can never touch a venue".into());
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

/// The extra conditions that only PLACING requires. Every one of these is about
/// sizing or watching a new position; none of them is needed to take one off.
fn place_preconditions(args: &Args) -> Vec<String> {
    let mut missing: Vec<String> = Vec::new();
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
    missing
}

/// Everything that must hold before this process may PLACE an order.
///
/// Encoded here rather than in a runbook: a checklist that only exists in prose
/// is one nobody runs. Returns the sinks on success, or the list of what is
/// missing — never a partially-armed engine.
///
/// Both venues push fills over their private WS channels (src/fills.rs).
/// Credentials are checked by `cancel_preconditions`; a fill feed that cannot
/// authenticate is the same failure as an order path that cannot.
///
/// Startup reconciliation is handled by `startup_sweep`, not by a precondition:
/// the engine starts from a clean book by CANCELLING whatever is resting
/// (Geoff's call, 2026-07-28). Arming aborts if that sweep cannot be proven to
/// have worked.
fn order_preconditions(
    args: &Args,
    bench: bool,
) -> Result<HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>, Vec<String>> {
    // Place checks FIRST and short-circuit, as before the split: a report-only
    // run that is already refused has no business reading credentials, and the
    // dry-run posture is "loads no credentials" (see the module header).
    let missing = place_preconditions(args);
    if !missing.is_empty() {
        return Err(missing);
    }
    cancel_preconditions(args, bench)
}

/// Cancel resting orders on the way OUT, on SIGTERM or Ctrl-C.
///
/// The engine had no signal handler and did not cancel on exit, so stopping an
/// armed process left its quotes resting on the venue with nothing owning
/// them. That is exactly how the 2026-07-28 orphans happened: an armed run
/// exited and left 13 live Kalshi orders that no process would ever cancel,
/// re-quote, or hedge if they filled. `systemctl stop` was enough to cause it.
///
/// The startup sweep already covered the NEXT run; this covers the gap in
/// between, which is the window where a fill goes unhedged.
///
/// It then leaked an order anyway, at 15:40 on 2026-07-28, in four composing
/// ways — all four now live in `exec::halt_and_sweep` and `sink::SweepPolicy`:
/// nothing latched the executors, so one placed a maker order ONE SECOND into
/// the sweep; the verify could only observe it; the `Err` arm fell into the same
/// `exit(0)`, so systemd recorded a clean stop; and the venues were swept
/// sequentially against a 90s `TimeoutStopSec`.
///
/// The adversarial review then found three more ways to exit 0 with an order
/// resting, all of them in the same shape as the original: an EMPTY resting
/// list believed on one read, a place still on the wire when that list was
/// read, and a discarded hedge leaving a filled leg naked. All three now feed
/// `ShutdownOutcome::exit_code`.
///
/// Still bounded by design — a venue that will not answer must not stop the
/// process from dying — but "bounded" now means a NON-ZERO exit and a shouting
/// log line rather than a silent success. The startup sweep remains the backstop.
fn spawn_shutdown_sweep() {
    tokio::spawn(async move {
        let mut term = match tokio::signal::unix::signal(
            tokio::signal::unix::SignalKind::terminate(),
        ) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[exec] WARNING: no SIGTERM handler ({e}) — exit will NOT cancel");
                return;
            }
        };
        let why = tokio::select! {
            _ = term.recv() => "SIGTERM",
            r = tokio::signal::ctrl_c() => {
                if r.is_err() { return; }
                "interrupt"
            }
        };
        let out = exec::halt_and_sweep(why).await;
        for l in out.report() {
            eprintln!("{l}");
        }
        if out.already_halting {
            // A WAL crash-stop or a ledger failure got here first and owns the
            // exit. Exiting now would overwrite its verdict with ours — and ours
            // is `0`, because we swept nothing.
            return;
        }
        std::process::exit(out.exit_code());
    });
}

/// What `--sweep-only` is about to destroy, and under whose key.
///
/// A pure function so it can be tested without credentials — proving the banner
/// says "PRIMARY" when no suffix was given is the whole point, and doing it for
/// real would mean a live venue call.
fn sweep_only_blast_radius(cred_suffix: &[(String, String)]) -> Vec<String> {
    let ident = |keys: &[&str]| {
        cred_suffix
            .iter()
            .find(|(k, _)| keys.contains(&k.as_str()))
            .map(|(_, s)| format!("suffix `{s}`"))
            .unwrap_or_else(|| "PRIMARY key — SHARED WITH THE PYTHON STACK".into())
    };
    vec![
        "[exec] --sweep-only CANCELS EVERY RESTING ORDER ON THE WHOLE ACCOUNT,".into(),
        "[exec]   not just the ones this process placed. Keys in use:".into(),
        format!("[exec]   kalshi        -> {}", ident(&["kalshi"])),
        format!("[exec]   polymarket_us -> {}", ident(&["pmus", "polymarket_us"])),
    ]
}

/// Cancel every resting order on every armed venue, then PROVE the book is
/// empty before the engine is allowed to quote.
///
/// This destroys real orders, including any a previous run or another tool left
/// behind — which is the point, and why it only ever runs behind
/// `--enable-orders`. A 2xx from cancel-all is not proof; the resting list is,
/// and both venues' lists lag a write, so it polls.
///
/// It sweeps UNCONDITIONALLY. There used to be a `before.is_empty()` fast path
/// that skipped both the cancel-all and the polling, so ONE transiently empty
/// read — a rate-limited page, a partial page, or the same write-lag this
/// function's own contract is built around — armed the engine on top of a
/// previous run's book. That is the worst possible place for a fail-open: this
/// sweep is the last backstop for every leak the process cannot clean up itself
/// (SIGKILL, an OOM kill under `MemoryMax=1G`, an abort, a double panic), and
/// the engine holds no ids for those orders, so its own kill path can never
/// reach them either. Two extra API calls per venue per start is the entire cost.
///
/// The pre-read survives only as a log line for the human, and is now
/// best-effort: an unreadable list is a reason to sweep, not a reason to stop.
async fn startup_sweep(
    sinks: &HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>,
    pol: &sink::SweepPolicy,
) -> Result<(), String> {
    for (venue, sink) in sinks {
        let s = sink.clone();
        match tokio::task::spawn_blocking(move || s.resting_order_ids()).await {
            Ok(Ok(before)) if before.is_empty() => {
                eprintln!("[exec] {venue:?}: nothing resting (unconfirmed) — sweeping anyway");
            }
            Ok(Ok(before)) => {
                eprintln!("[exec] {venue:?}: CANCELLING {} resting order(s): {}", before.len(),
                          before.join(" "));
            }
            Ok(Err(e)) => eprintln!("[exec] {venue:?}: cannot list resting orders ({e}) — \
                                     sweeping blind, the sweep is the authority"),
            Err(e) => eprintln!("[exec] {venue:?}: list task panicked ({e}) — sweeping anyway"),
        }
        sink::cancel_all_and_verify_with(sink.clone(), pol.clone())
            .await
            .map_err(|e| format!("{venue:?}: {e}"))?;
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

/// The capital gate every quoter consults, sized from `--exec-config` /
/// `--topics` and seeded with the exposure already on the book.
fn build_risk_view(
    args: &Args,
    quoters: &mut [Quoter],
    rel_meta: &HashMap<String, (String, String)>,
) -> std::sync::Arc<risk::RiskView> {
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
    seed_exposure_from_ledger(&rv, &args.ledger, rel_meta);
    for q in quoters.iter_mut() {
        q.set_risk(Some(rv.clone() as std::sync::Arc<dyn arb_core::quoter::RiskGate>));
    }
    rv
}

/// Seed exposure from the open baskets in the trade ledger (exec/main.py:264).
/// Without this the caps reset on every restart and the engine believes the
/// whole book is free.
fn seed_exposure_from_ledger(
    rv: &risk::RiskView,
    ledger_path: &str,
    rel_meta: &HashMap<String, (String, String)>,
) {
    match ledger::read(ledger_path) {
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
                ledger_path,
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
}

/// Start the single event source `parse_args` has already guaranteed: a tape, a
/// WAL replay, or the live socket.
fn spawn_feed(args: &Args, tx: tokio::sync::mpsc::Sender<feed::FeedMsg>) {
    if let Some(tape) = args.bench_tape.clone() {
        let (max, pace) = (args.max_events, args.pace_x);
        std::thread::spawn(move || feed::tape_feed(tape, max, pace, tx));
    } else if let Some(wal) = args.replay_wal.clone() {
        let max = args.max_events;
        std::thread::spawn(move || feed::wal_replay_feed(wal, max, tx));
    } else if let Some(sock) = args.socket.clone() {
        tokio::spawn(feed::socket_feed(sock, tx));
    }
}

/// Print the arming checklist and send nothing. This is what `--enable-orders`
/// does on its own.
fn report_preconditions(args: &Args, bench: bool) {
    match order_preconditions(args, bench) {
        Ok(_) => {
            eprintln!("[exec] preconditions OK — arming would place REAL orders.");
            eprintln!("[exec] add --yes-trade-live to actually arm. Nothing was sent.");
        }
        Err(missing) => {
            eprintln!("[exec] --enable-orders blocked. Unmet preconditions:");
            for m in &missing {
                eprintln!("[exec]   - {m}");
            }
        }
    }
}

/// The venues this process may write to. Empty unless `--enable-orders`, and
/// the process dies rather than return a partial set: an engine that quotes on
/// one venue because the other refused is a naked-leg machine.
fn arm_venues(args: &Args, bench: bool) -> HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> {
    if args.sweep_only {
        // BLAST RADIUS, said out loud, and said BEFORE the preconditions are
        // even checked so it is on screen whether or not the run proceeds.
        // `cancel_all_open` is account-wide — not per-relationship and not
        // per-process — so with no `--cred-suffix` this is the PRIMARY key and it
        // cancels the Python stack's resting orders too. Clearing the 15:40
        // orphan was run exactly that way; it happened to find one order, which
        // was luck, not safety.
        for l in sweep_only_blast_radius(&args.cred_suffix) {
            eprintln!("{l}");
        }
    }
    if !args.enable_orders {
        return HashMap::new();
    }
    // A pure cancel run is held to the CANCEL checklist, not the place one.
    let checked = if args.sweep_only {
        cancel_preconditions(args, bench)
    } else {
        order_preconditions(args, bench)
    };
    match checked {
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
}

/// The fill feed runs whenever the order path does: a live order with no
/// fill feed is an unhedged position waiting to happen.
fn spawn_fill_feeds(args: &Args, tx_acks: Option<&tokio::sync::mpsc::Sender<feed::FeedMsg>>) {
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
            if let Some(t) = tx_acks {
                tokio::spawn(fills::pmus_fill_feed(kid, sec, t.clone()));
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
            if let Some(t) = tx_acks {
                tokio::spawn(fills::kalshi_fill_feed(kid, pem, t.clone()));
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

/// What the engine is allowed to do this run. Every "off in bench/replay" and
/// "off unless armed" decision is spelled out here rather than inferred later.
fn run_cfg(
    args: Args,
    bench: bool,
    armed: bool,
    has_executors: bool,
    risk: Option<std::sync::Arc<risk::RiskView>>,
) -> engine::RunCfg {
    engine::RunCfg {
        out_path: args.out,
        kill_file: args.kill_file,
        stats_every_s: args.stats_every_s,
        bench,
        wal_path: args.wal,
        // bench/replay must stay byte-deterministic and have no live feed.
        health_file: (!bench && !args.health.is_empty()).then(|| args.health.clone()),
        risk,
        // Only an ARMED engine books baskets. A dry run writing here would
        // invent exposure that the next startup would seed from as if real.
        ledger_path: (has_executors && armed).then(|| args.ledger.clone()),
        // Off in bench/replay (byte-determinism) and pointless unarmed, but
        // kept ON in the dry-run shadow so the retry policy is exercised
        // against the live book long before it is trusted with money.
        hedge_retry: (!bench).then(|| engine::HedgeRetry {
            interval_s: args.hedge_retry_s,
            max_slip: args.hedge_max_slip.clone(),
            alarm_after_s: args.hedge_alarm_s,
        }),
        // Off in bench/replay: it reads the wall clock and a marks file, and
        // both would break byte-exact replay.
        take_take: (!bench && args.take_take).then(|| engine::TakeTake {
            max_ct_per_rel: args.tt_max_ct_per_rel,
            max_clip: args.tt_max_clip,
            marks_path: args.marks.clone(),
            // Detection is free and unarmed; FIRING additionally requires the
            // order path, so take-take can never place from a dry run.
            detect_only: args.tt_detect_only || !armed,
            // Armed, the gate must outlast place -> fill -> hedge -> book, or
            // the same crossing re-fires before exposure catches up; the hedge
            // alarm threshold is exactly that horizon. Unarmed it only keeps
            // the log readable.
            cooldown_s: if args.tt_detect_only || !armed { 5.0 } else { args.hedge_alarm_s },
        }),
        armed,
    }
}

/// `engine::run` returns when the feed channel closes, which in bench/replay
/// is EOF and correct. ARMED it should be unreachable — `socket_feed`
/// reconnects forever and holds its sender — but "unreachable" is not a reason
/// to make it the one exit that does not cancel. Every other way out of this
/// process sweeps; so does this one.
async fn sweep_after_engine_exit() {
    eprintln!("[exec] the engine loop ended while ARMED — sweeping before exit");
    let out = exec::halt_and_sweep("engine loop ended").await;
    for l in out.report() {
        eprintln!("{l}");
    }
    if !out.already_halting {
        std::process::exit(out.exit_code());
    }
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
    let risk = (!bench).then(|| build_risk_view(&args, &mut quoters, &rel_meta));
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
    spawn_feed(&args, tx);

    // --enable-orders alone only REPORTS. Arming needs the second flag, so
    // checking the preconditions can never place an order by accident.
    if args.enable_orders && !args.sweep_only && !args.confirm_live {
        report_preconditions(&args, bench);
        return;
    }
    let sinks = arm_venues(&args, bench);
    // Reconcile BEFORE anything can quote: the engine has no memory of orders a
    // previous run left resting, and it cannot cancel what it never had an id
    // for. Start from a book we know is empty.
    if !sinks.is_empty() {
        // Hand the sinks to the halt path and arm the panic hook BEFORE the
        // first venue write, so every way out of this process — SIGTERM, a WAL
        // crash-stop, a panic on any thread — cancels first. Previously only
        // SIGTERM did, and only after the engine was already quoting.
        exec::register_sinks(sinks.clone());
        exec::install_armed_panic_hook();
        if let Err(e) = startup_sweep(&sinks, &sink::SweepPolicy::default()).await {
            eprintln!("[exec] STARTUP SWEEP FAILED: {e}");
            eprintln!("[exec] refusing to arm: the book could not be proven clean");
            std::process::exit(10);
        }
        if args.sweep_only {
            eprintln!("[exec] --sweep-only: book reconciled, exiting without quoting");
            return;
        }
        spawn_shutdown_sweep();
        spawn_fill_feeds(&args, tx_acks.as_ref());
    }

    let armed = !sinks.is_empty();
    let acks = if sinks.is_empty() { None } else { tx_acks.clone() };
    let (exec_txs, exec_stats) = exec::spawn_executors(rate, sinks, acks);
    let cfg = run_cfg(args, bench, armed, !exec_txs.is_empty(), risk);
    let summary = engine::run(quoters, by_market, rx, exec_txs, exec_stats, cfg).await;
    println!("{summary}");
    if armed {
        sweep_after_engine_exit().await;
    }
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

    /// Same shape as production, with the sleeps taken out.
    fn fast() -> sink::SweepPolicy {
        sink::SweepPolicy {
            poll_delay: std::time::Duration::ZERO,
            ..sink::SweepPolicy::default()
        }
    }

    /// D2, the inverse of the test that used to be here. `a_clean_book_needs_no_
    /// cancel` asserted the `before.is_empty()` fast path — and that fast path
    /// was the fail-open. An empty-LOOKING book is still swept.
    #[tokio::test]
    async fn a_clean_looking_book_is_swept_anyway() {
        let (h, m) = sinks(&[], &[]);
        startup_sweep(&h, &fast()).await.unwrap();
        assert!(
            *m.cancelled.lock().unwrap(),
            "an unconfirmed empty read is not a reason to skip the sweep"
        );
    }

    /// **D2 — the startup sweep must not arm on a single empty read.** Ported
    /// from the adversarial review, where it passed.
    ///
    /// This sweep is the last backstop for every leak the process cannot clean up
    /// itself: SIGKILL, an OOM kill under `MemoryMax=1G`, an abort, a double
    /// panic — precisely the cases the panic hook's own documented limits leave
    /// open. One transiently empty read (a rate-limited page, a partial page, the
    /// same write-lag the sweep is built around) used to arm the engine on top of
    /// a previous run's book that it holds no ids for, so its own kill path could
    /// never reach those orders either.
    #[tokio::test]
    async fn one_transiently_empty_read_does_not_arm_the_engine_on_a_dirty_book() {
        /// First read empty, then the truth — and a cancel-all that works.
        struct FirstReadEmpty {
            reads: std::sync::Mutex<u32>,
            resting: std::sync::Mutex<Vec<String>>,
            cancelled: std::sync::Mutex<bool>,
        }
        impl sink::OrderSink for FirstReadEmpty {
            fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
                unreachable!("a sweep never places")
            }
            fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
                unreachable!("a sweep uses cancel_all_open")
            }
            fn cancel_all_open(&self) -> Result<(), VenueError> {
                *self.cancelled.lock().unwrap() = true;
                self.resting.lock().unwrap().clear();
                Ok(())
            }
            fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
                let mut n = self.reads.lock().unwrap();
                *n += 1;
                Ok(if *n == 1 { Vec::new() } else { self.resting.lock().unwrap().clone() })
            }
        }
        let m = std::sync::Arc::new(FirstReadEmpty {
            reads: std::sync::Mutex::new(0),
            resting: std::sync::Mutex::new(vec![
                "66e1c799-507b-4a59-89aa-ec23ad14b990".to_string()
            ]),
            cancelled: std::sync::Mutex::new(false),
        });
        let mut h: HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> = HashMap::new();
        h.insert(Venue::Kalshi, m.clone());

        startup_sweep(&h, &fast()).await.expect("the sweep clears it and proves it");
        assert!(*m.cancelled.lock().unwrap(), "the cancel-all was issued despite read #1");
        assert!(
            m.resting.lock().unwrap().is_empty(),
            "and the previous run's order is gone, not armed on top of"
        );
    }

    #[tokio::test]
    async fn resting_orders_are_cancelled_and_the_book_verified_empty() {
        let (h, m) = sinks(&["a", "b"], &[]);
        startup_sweep(&h, &fast()).await.unwrap();
        assert!(*m.cancelled.lock().unwrap());
    }

    /// The property that makes this safe to arm behind: if the sweep cannot be
    /// PROVEN to have worked, arming must not proceed. A 2xx from cancel-all is
    /// not proof — the resting list is.
    #[tokio::test]
    async fn a_sweep_that_leaves_orders_resting_is_an_error() {
        let (h, _m) = sinks(&["a", "b"], &["b"]);
        let err = startup_sweep(&h, &fast()).await.unwrap_err();
        assert!(err.contains("SURVIVED"), "{err}");
        assert!(err.contains('b'), "names what is left: {err}");
    }
}

/// The cancel path must be the easiest thing in this binary to run. On
/// 2026-07-28 it was the hardest: `--sweep-only` demanded --balance, --health,
/// --socket and a readable ledger before it would cancel a real leaked order.
#[cfg(test)]
mod precondition_tests {
    use super::*;

    #[test]
    fn placing_requires_a_feed_cash_health_and_a_readable_ledger() {
        let mut a = default_args();
        a.ledger = "/nonexistent/dir/trades.jsonl".into();
        a.health = String::new();
        let missing = place_preconditions(&a).join("\n");
        assert!(missing.contains("--socket"), "{missing}");
        assert!(missing.contains("--balance"), "{missing}");
        assert!(missing.contains("--health"), "{missing}");
        assert!(missing.contains("ledger"), "{missing}");
    }

    /// The split itself: NONE of the place-only conditions may block a cancel.
    #[test]
    fn cancelling_requires_none_of_them() {
        let mut a = default_args();
        a.ledger = "/nonexistent/dir/trades.jsonl".into();
        a.health = String::new();
        // No credentials in the test env, so this errors — the point is WHAT it
        // complains about. It must be credentials and nothing else.
        let missing = match cancel_preconditions(&a, false) {
            Ok(_) => Vec::new(),
            Err(m) => m,
        };
        let joined = missing.join("\n");
        for forbidden in ["--socket", "--balance", "--health", "ledger"] {
            assert!(
                !joined.contains(forbidden),
                "a cancel must not be gated on {forbidden}: {joined}"
            );
        }
    }

    /// A blanket cancel under the shared primary key has to announce itself. The
    /// 15:40 orphan was cleared with exactly this command and no kalshi suffix,
    /// which would have cancelled the Python stack's book too; it found one order,
    /// so nothing was lost, but the command said nothing about the risk.
    #[test]
    fn sweep_only_names_the_key_it_will_cancel_under() {
        let none = sweep_only_blast_radius(&[]).join("\n");
        assert!(none.contains("WHOLE ACCOUNT"), "{none}");
        assert_eq!(
            none.matches("PRIMARY key").count(),
            2,
            "both venues warn when no suffix is given: {none}"
        );
        assert!(none.contains("SHARED WITH THE PYTHON STACK"), "{none}");

        let both = sweep_only_blast_radius(&[
            ("kalshi".into(), "rs_trader".into()),
            ("pmus".into(), "rs_trader".into()),
        ])
        .join("\n");
        assert!(!both.contains("PRIMARY key"), "a suffixed run is scoped: {both}");
        assert_eq!(both.matches("suffix `rs_trader`").count(), 2);

        // The real trap: a pmus suffix given, kalshi forgotten.
        let half = sweep_only_blast_radius(&[("pmus".into(), "rs_trader".into())]).join("\n");
        assert!(half.contains("kalshi        -> PRIMARY key"), "{half}");
        assert!(half.contains("polymarket_us -> suffix `rs_trader`"), "{half}");
    }

    /// A bench/replay run may not touch a venue even to cancel: its "book" is a
    /// tape, and the account it would cancel against is the live one.
    #[test]
    fn bench_can_never_touch_a_venue() {
        let err = match cancel_preconditions(&default_args(), true) {
            Ok(_) => panic!("bench must never be handed a live sink"),
            Err(m) => m.join("\n"),
        };
        assert!(err.contains("bench/replay"), "{err}");
    }
}
