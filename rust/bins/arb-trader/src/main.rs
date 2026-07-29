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

// `Engine::summary` is one flat `serde_json::json!` of ~50 gauges, and the
// macro recurses once per key, so the default limit of 128 is a cap on how
// many things this engine may report about itself. It is not a design budget
// and nothing should be left out of the summary to stay under it.
//
// It bit on 2026-07-29: #31 added `exec_recovered` and this change added
// `toxgate_stale` and `maker_apr_bar`. Each fit alone; together they did not,
// so both branches gated green and the merge would not have built — the same
// shape as #33, caught here only because the gate now refuses a stale base.
#![recursion_limit = "256"]

mod engine;
mod exec;
mod feed;
mod fills;
mod hist;
mod ledger;
mod orphan;
mod risk;
mod taketake;
mod sink;
mod wal;

use arb_core::model::{BookSide, Venue};
use arb_core::quoter::{Quoter, Toxgate};
use arb_core::scan::{Cx, Rel, RelLeg, RelType};
use std::collections::{HashMap, HashSet};

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
    /// Restore the pre-2026-07-29 account-wide sweep when ownership cannot be
    /// determined. Loud, off by default — see `KalshiGateway::with_unscoped_sweep`.
    sweep_unscoped: bool,
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
    /// MAKER APR hurdle, %/yr: the extra per-contract lock a resting quote must
    /// carry so that a fill annualizes to at least this over the hold to
    /// resolution (`Quoter::set_apr`). `None` FLOATS it with capital
    /// utilization, which is the policy; `Some(0.0)` disables it.
    min_apr: Option<f64>,
    /// The day the hold is measured FROM, `YYYY-MM-DD`. Defaults to today UTC,
    /// re-derived on every refresh; pin it to make a bench replay
    /// reproducible, because the hurdle shrinks as the resolve date nears.
    apr_asof: Option<String>,
    /// Research toxicity feed. `None` = OFF, because no writer for it exists
    /// (see `install_policy`); a path turns the gate on.
    toxgate: Option<String>,
    /// `--suppress market:side`, repeatable: (market, side) pairs some OTHER
    /// order-owner holds, which the quoter cancels out of and stays out of.
    suppress: Vec<(String, BookSide)>,
}

/// The floating maker APR bar (Geoff 2026-07-22, card 80ff7987), port of
/// `exec/main.py` `TT_APR_FLOOR` / `TT_APR_CEIL`:
///
/// > scales with capital utilization of the class budget — near-idle capital
/// > should grab modest APRs (floor ~= the trivially-redeployable yield), while
/// > a full book demands fresh take-take beat what maker capital earns before
/// > crowding it out. Linear in utilization; Geoff's 8% reference sits at ~1/3
/// > utilization.
///
/// Python applied the SAME number to the makers — `for q4 in quoters.values():
/// q4.min_apr = tt["bar"]`, with the comment "makers clear the same bar (card
/// 80ff7987)" — which is the policy this binary was missing entirely.
const APR_FLOOR: f64 = 4.0;
const APR_CEIL: f64 = 16.0;

/// The maker APR bar at a given class-budget utilization.
pub fn apr_bar(util: f64) -> f64 {
    APR_FLOOR + (APR_CEIL - APR_FLOOR) * util.clamp(0.0, 1.0)
}

/// The half of the quoter policy that goes STALE, and so has to be re-derived
/// while the process runs rather than installed once (`Engine::tox_tick` and
/// `Engine::apr_tick`). `suppress` is absent because it does not drift: it is
/// an operator declaration, fixed for the run.
struct Policy {
    /// Research toxicity feed to re-read; `None` = the gate is off.
    toxgate_file: Option<String>,
    /// APR hurdle inputs to re-apply; `None` = never (bench/replay).
    apr: Option<engine::AprCfg>,
    /// The (bar, day) already installed, for the gauge — a bench run has a
    /// hurdle it will never refresh, and it must still be reportable.
    apr_installed: (f64, String),
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
        sweep_unscoped: false,
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
        min_apr: None, // float with utilization; see `apr_bar`
        apr_asof: None,
        toxgate: None,
        suppress: Vec::new(),
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
            "--sweep-unscoped" => a.sweep_unscoped = true,
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
            "--min-apr" => {
                a.min_apr = Some(it.next().expect("pct").parse().expect("float"))
            }
            "--apr-asof" => a.apr_asof = it.next(),
            "--toxgate" => a.toxgate = it.next(),
            "--suppress" => {
                // "market_id:side" — side must be bid|ask, as arb-intent.
                let v = it.next().expect("--suppress market:side");
                let (m, s) = v.rsplit_once(':').expect("--suppress wants market:side");
                let side = BookSide::parse(s).unwrap_or_else(|| panic!("bad suppress side {s}"));
                a.suppress.push((m.to_string(), side));
            }
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

/// Quoter indices touching each market, so a book event fans out to only the
/// quoters that can care about it.
type MarketIndex = HashMap<(Venue, String), Vec<usize>>;
/// Relationship id -> (oracle_risk, kind). See `rel_meta` below for why.
type RelMeta = HashMap<String, (String, String)>;

fn load_quoters(
    registry: &str,
    tradable: &str,
    rel_prefixes: &[String],
) -> (Vec<Quoter>, MarketIndex, RelMeta) {
    let reg = arb_registry::Registry::load(registry).expect("read registry");
    // THE GATE (card c9ac7d1d, exec/main.py:131-146): this process QUOTES a
    // relationship only if it is HUMAN-vetted in the registry or explicitly
    // allowlisted in config/tradable.yaml. An agent verdict is not enough.
    // A missing allowlist file is an empty allowlist, which is the
    // conservative direction — it never widens the gate. A registry veto
    // (`Relationship::veto`) beats both halves.
    let allow = arb_registry::Allowlist::load(tradable);
    let total = reg.relationships.len();
    let mut n_gated = 0usize;
    let mut n_vetoed = 0usize;

    // Metadata for the risk view, keyed by id: oracle_risk scales the per-rel
    // cap and the type is the class-exposure key, and neither is carried on
    // `Rel`. Built from the FULL registry, not the quoting subset — a basket we
    // no longer quote still consumes capital.
    let mut rel_meta: RelMeta = HashMap::new();
    // Which ids would survive `config/tradable.yaml` being emptied — i.e. the
    // ones deleting a line does NOT revoke. Collected here because the registry
    // is consumed below and `Rel` does not carry the verdict.
    let mut human_vetted: std::collections::BTreeSet<String> = Default::default();
    for r in &reg.relationships {
        if r.human_vetted() {
            human_vetted.insert(r.id.clone());
        }
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
            // A veto is not just another blocked rel. If the id is ALSO
            // allowlisted, this line is the only place the operator learns the
            // revocation they wrote actually took effect — say it by name.
            //
            // SCOPE, and it is the dangerous half: this gate binds THIS PROCESS
            // and nothing else. No other component reads the registry verdict or
            // the allowlist — `arbbot-hedge.timer` fires the naked-leg hedger
            // every 5 minutes and places live Kalshi orders without consulting
            // either. So a revocation stops the quoter and does NOT stop the
            // account from trading the pair, and a line that reads as
            // account-wide tells the operator the risk is gone when it is not.
            if let Some(v) = r.veto() {
                n_vetoed += 1;
                if allow.contains(&r.id) {
                    eprintln!(
                        "[gate] VETO {}: registry verdict {v:?} overrides its entry in \
                         {tradable} — this process will not quote it. That is the whole \
                         effect: the naked-leg hedger does not read this gate and can \
                         still place orders on the pair.",
                        r.id
                    );
                }
            }
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
                rtype: RelType::parse(r.kind.as_deref()?)?,
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
        "[gate] {total} relationships -> {} quoting; {n_gated} not quoted ({n_vetoed} vetoed by \
         registry verdict, rest not human-vetted and not in {tradable}, which lists {} ids). \
         Quoting is the only thing this gate decides; it is not an account-wide halt.",
        quoters.len(),
        allow.len()
    );

    // The allowlist half of the gate is the half an operator revokes by DELETING
    // a line, and a deletion is invisible from in here: this process holds no
    // previous allowlist, so "removed" and "never listed" are the same state. It
    // cannot report the removal. What it can report is the present set the
    // removal is meant to shrink, which is what an operator checks a revoke
    // against — and the absence of an id from this list is the confirmation the
    // aggregate census above can only hint at with a changed number.
    //
    // One line, listing ids, and only these ids: bounded by `tradable.yaml`, a
    // file a human writes, and emitted ONCE at startup. Naming the not-quoted
    // set instead would be a line per relationship over most of the registry —
    // volume that scales with the registry is how #41 got to 355k lines.
    let allow_only: Vec<&str> = quoters
        .iter()
        .map(|q| q.rel.id.as_str())
        .filter(|id| !human_vetted.contains(*id))
        .collect();
    eprintln!(
        "[gate] quoting on {tradable} alone (deleting the line stops the quoter, and \
         nothing else): {}",
        if allow_only.is_empty() { "(none)".into() } else { allow_only.join(", ") }
    );

    let mut by_market: MarketIndex = HashMap::new();
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
    match build_kalshi_sink(suffix("kalshi").as_deref(), args.sweep_unscoped) {
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
fn sweep_only_blast_radius(cred_suffix: &[(String, String)], unscoped: bool) -> Vec<String> {
    let ident = |keys: &[&str]| {
        cred_suffix
            .iter()
            .find(|(k, _)| keys.contains(&k.as_str()))
            .map(|(_, s)| format!("suffix `{s}`"))
            .unwrap_or_else(|| "PRIMARY key — SHARED WITH THE PYTHON STACK".into())
    };
    let kalshi_scope = if unscoped {
        vec![
            "[exec]     --sweep-unscoped IS SET: THE WHOLE ACCOUNT, including every".into(),
            "[exec]     order another workstream placed under this key.".into(),
        ]
    } else {
        vec![
            "[exec]     scoped to ids THIS STACK minted (m…/h…/t…/rehearse-…/sweep-…);".into(),
            "[exec]     another workstream's orders under this key are LEFT RESTING.".into(),
        ]
    };
    let mut out = vec![
        "[exec] --sweep-only DESTROYS REAL RESTING ORDERS. Keys in use:".into(),
        format!("[exec]   kalshi        -> {}", ident(&["kalshi"])),
    ];
    out.extend(kalshi_scope);
    out.push(format!("[exec]   polymarket_us -> {}", ident(&["pmus", "polymarket_us"])));
    out.push("[exec]     THE WHOLE ACCOUNT — PM-US carries no id of ours to scope by,".into());
    out.push("[exec]     so this cancels orders this stack never placed.".into());
    out
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
/// `Ok(unconfirmed)` — the venues that swept without proving. Empty is the
/// normal answer; anything in it means the caller armed over a book no venue
/// could confirm, and every caller has to say so rather than print "clean".
async fn startup_sweep(
    sinks: &HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>>,
    pol: &sink::SweepPolicy,
) -> Result<Vec<String>, String> {
    let mut unconfirmed: Vec<String> = Vec::new();
    for (venue, sink) in sinks {
        let s = sink.clone();
        match tokio::task::spawn_blocking(move || s.resting_order_ids()).await {
            Ok(Ok(before)) if before.is_empty() => {
                // NOT the word "unconfirmed": that is the verdict line below,
                // and this one fires on every single start. A `grep -i
                // unconfirmed` that matches the boring line twice a start is
                // exactly how the grave one gets trained away.
                eprintln!(
                    "[exec] {venue:?}: pre-read shows nothing resting (one read, not \
                     evidence) — sweeping anyway"
                );
            }
            Ok(Ok(before)) => {
                eprintln!("[exec] {venue:?}: CANCELLING {} resting order(s): {}", before.len(),
                          before.join(" "));
            }
            Ok(Err(e)) => eprintln!("[exec] {venue:?}: cannot list resting orders ({e}) — \
                                     sweeping blind, the sweep is the authority"),
            Err(e) => eprintln!("[exec] {venue:?}: list task panicked ({e}) — sweeping anyway"),
        }
        match sink::cancel_all_and_verify_with(sink.clone(), pol.clone()).await {
            Ok(()) => eprintln!("[exec] {venue:?}: book is clean"),
            // The cancel-all went in and NOTHING was ever observed resting — we
            // just could not read the confirmation. Arm, loudly.
            //
            // Refusing here would be fail-closed on a premise nobody has
            // verified: no capture in this repo shows what PM-US returns for an
            // EMPTY book, so if that shape is one `open_orders` cannot parse,
            // this refusal would be permanent and the engine could never start
            // again — an outage manufactured out of an empty book. Fail-closed
            // is earned where the premise is checked (Kalshi's tag echo is, and
            // its failure comes back through `cancel_all_open`, so it still
            // refuses below); it is not earned here.
            Err(e) if e.is_only_unconfirmed() => {
                // `###`, the same mechanism `ShutdownOutcome::report` reserves
                // for a non-zero exit. "Loud" in wording alone is not loud
                // against a journal whose 60s cadence is a 2 KB JSON blob.
                eprintln!("[exec] ###########################################################");
                eprintln!("[exec] ### ARMING ON A BOOK NO VENUE COULD CONFIRM            ###");
                eprintln!("[exec] ###########################################################");
                eprintln!("[exec] ### {venue:?}: {e}");
                eprintln!(
                    "[exec] ###   cancel-all was ACCEPTED and nothing was seen resting, but \
                     the resting list could not be READ. The body above is a response shape \
                     this repo has never captured — PIN IT and tighten the check."
                );
                unconfirmed.push(format!("{venue:?}: {e}"));
            }
            Err(e) => return Err(format!("{venue:?}: {e}")),
        }
    }
    Ok(unconfirmed)
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
    unscoped_sweep: bool,
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
    Ok(std::sync::Arc::new(
        arb_venue::gateway::KalshiGateway::with_transport(
            signer,
            arb_venue::ratelimit::RateLimiter::from_per_minute(60.0, 0),
            transport,
        )
        .with_unscoped_sweep(unscoped_sweep),
    ))
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
        arb_venue::ratelimit::RateLimiter::from_per_minute(60.0, 0),
        transport,
    )))
}

/// The research toxicity feed as loaded.
///
/// The two fields are INDEPENDENT, and collapsing them is a fail-open. A stale
/// document must still be INSTALLED: it is the only record of which (market,
/// side) pairs the model covers, and `Toxgate::verdict` needs exactly that to
/// answer `Untrusted` for a covered side while leaving an uncovered one alone.
/// Returning `Err` and dropping the document — which this function did in its
/// first cut — leaves the quoter holding NO gate, which is the fail-open the
/// whole change exists to remove. Measured on the pinned tape: it digested
/// identically to having no gate at all.
struct ToxLoad {
    /// The document, if it parsed — stale or not.
    gate: Option<std::sync::Arc<Toxgate>>,
    /// Why it may not be scored against, if it may not be. Unreadable,
    /// unparseable and stale all land here; only the first two also cost us
    /// the coverage map.
    stale: Option<String>,
}

fn load_toxgate(path: &str, now: f64) -> ToxLoad {
    let doc = match std::fs::read_to_string(path) {
        Ok(d) => d,
        Err(e) => return ToxLoad { gate: None, stale: Some(format!("{path}: {e}")) },
    };
    let Some(gate) = Toxgate::from_json(&doc) else {
        return ToxLoad { gate: None, stale: Some(format!("{path}: not a toxgate document")) };
    };
    // BOTH directions, and the same two bounds `Toxgate::verdict` applies — a
    // gauge that disagreed with the refusal would be worse than no gauge. The
    // future arm is not symmetry for its own sake: an unbounded forward `ts`
    // reads as permanently current, which disables this reload, the gauge and
    // every `Untrusted` verdict at once.
    let age = now - gate.ts;
    let stale = if age > arb_core::quoter::TOXGATE_MAX_AGE {
        Some(format!(
            "{path}: feed is {age:.0}s old (max {:.0}s) — the research writer is not running",
            arb_core::quoter::TOXGATE_MAX_AGE
        ))
    } else if age < -arb_core::quoter::TOXGATE_MAX_SKEW {
        Some(format!(
            "{path}: feed is stamped {:.0}s in the FUTURE — a clock or a unit is wrong, and a \
             forward ts would otherwise never expire",
            -age
        ))
    } else {
        None
    };
    ToxLoad { gate: Some(std::sync::Arc::new(gate)), stale }
}

/// Size and install the maker APR hurdle on every quoter. Shared by startup
/// and by `Engine::apr_tick`, because BOTH of its terms drift: utilization
/// moves as baskets book, and the hold shortens every day.
///
/// Returns `(bar, asof, relationships with no resolve date)`.
fn apply_apr(
    quoters: &mut [Quoter],
    min_apr: Option<f64>,
    asof: Option<&str>,
    risk: Option<&risk::RiskView>,
) -> (f64, String, Vec<String>) {
    let bar = match (min_apr, risk) {
        // An explicit --min-apr wins everywhere, which is how a bench replay
        // pins the hurdle it is measuring.
        (Some(x), _) => x,
        (None, Some(rv)) => apr_bar(rv.utilization()),
        // No risk view is bench/replay: no utilization to float on, and a
        // digest pinned to a fixed tape cannot survive a bar that moves.
        (None, None) => 0.0,
    };
    let asof = asof
        .map(str::to_string)
        .unwrap_or_else(|| arb_core::resolve::today_iso(arb_core::clock::now_s()));
    let mut cx = Cx::default();
    let mut undated: Vec<String> = Vec::new();
    for q in quoters.iter_mut() {
        // Per relationship, not one global constant: these resolve on dates
        // seventeen months apart, and one shared `resolve_years` would size the
        // hurdle wrong for every family but one.
        let years = arb_core::resolve::years_to(&q.rel.id, &asof);
        if years.is_none() && bar > 0.0 {
            undated.push(q.rel.id.clone());
        }
        q.set_apr(&mut cx, bar, years);
    }
    (bar, asof, undated)
}

/// The three quoter policies this binary knew how to build and never installed.
///
/// `Quoter::new` hardcodes `apr_margin: None`, an empty `suppress` and
/// `toxgate: None`, and until now `arb-trader` called only `set_risk` — so the
/// APR hurdle at quoter.rs:213/:244 and the toxicity skip at :413 were
/// unreachable in the live process. `bins/arb-intent`, a replay tool, was the
/// only caller of the other three setters in the workspace. The engine
/// therefore rested maker quotes locking as little as one tick — 0.27%/yr on
/// france-pres-27 money committed until 2027 — and quoted sides the research
/// feed scored at up to 7x `TOXGATE_MAX`.
///
/// Returns what the engine has to keep CURRENT — see [`Policy`].
fn install_policy(args: &Args, quoters: &mut [Quoter], risk: Option<&risk::RiskView>) -> Policy {
    let (bar, asof, undated) =
        apply_apr(quoters, args.min_apr, args.apr_asof.as_deref(), risk);
    if bar > 0.0 {
        match (args.min_apr, risk) {
            (None, Some(rv)) => eprintln!(
                "[apr] maker hurdle {bar:.2}%/yr = {APR_FLOOR} + {}*util at util {:.3}, \
                 holds measured from {asof}",
                APR_CEIL - APR_FLOOR,
                rv.utilization()
            ),
            _ => eprintln!("[apr] maker hurdle {bar}%/yr (explicit), holds from {asof}"),
        }
        if !undated.is_empty() {
            // Not a refusal — `arb_core::resolve` has no date for these
            // families, and inventing one would size a real hurdle off a
            // guess. It IS a fail-open, so it is named rather than silent.
            eprintln!(
                "[apr] NO RESOLVE DATE, so NO HURDLE, for {} relationship(s): {}",
                undated.len(),
                undated.join(" ")
            );
        }
    } else {
        eprintln!("[apr] maker hurdle OFF — quotes may lock as little as one tick");
    }

    // `--suppress` is a STATIC operator declaration and nothing more. Python's
    // runner mutates this set as its maker-unwind rests and pulls exit asks
    // (exec/main.py:726 adds, :572 discards); this binary has no maker-unwind,
    // so nothing populates it dynamically and no code here pretends otherwise.
    let suppress: HashSet<(String, BookSide)> = args.suppress.iter().cloned().collect();
    if !suppress.is_empty() {
        let mut names: Vec<String> =
            suppress.iter().map(|(m, s)| format!("{m}:{}", s.as_str())).collect();
        names.sort();
        eprintln!("[quoter] suppressed, another owner holds these sides: {}", names.join(" "));
    }
    for q in quoters.iter_mut() {
        q.set_suppress(suppress.clone());
    }

    // `risk.is_none()` IS bench/replay, and neither term of the bar exists
    // there, so there is nothing to refresh — but the bar that WAS installed
    // is still reported, because an explicit `--min-apr` works in bench.
    let apr = risk.is_some().then(|| engine::AprCfg {
        min_apr: args.min_apr,
        asof: args.apr_asof.clone(),
    });
    let apr_installed = (bar, asof);

    // OFF unless a path is given, and that is not caution — it is that NOTHING
    // WRITES THIS FILE. `scripts/toxgate_daemon.py` (216 lines) and
    // `scripts/toxgate_evidence.py` were deleted in 3e4e80d, the rust rewrite;
    // no Rust replacement, no unit, no timer exists, and the Python stack is
    // frozen so the deleted one may not simply be run. Every remaining mention
    // of `toxgate.json` in the tree is a READER.
    //
    // Defaulting this on would therefore not mean "gate on, feed occasionally
    // stale" — it would mean the gate can never clear, and `arbbot-trader-rs`
    // (Restart=always, no --bench-tape) would go dark on its next restart and
    // stay dark, taking the dashboard's /intents view with it. Turning it on is
    // an explicit `--toxgate <path>` the day something writes one.
    let Some(path) = args.toxgate.as_deref().filter(|p| !p.is_empty()) else {
        eprintln!("[toxgate] OFF (no --toxgate) — no adverse-selection gate on maker quotes");
        return Policy { toxgate_file: None, apr, apr_installed };
    };
    let load = load_toxgate(path, arb_core::clock::now_s());
    if let Some(gate) = &load.gate {
        eprintln!("[toxgate] {} markets scored, gate at {}/ct",
                  gate.markets.len(), arb_core::quoter::TOXGATE_MAX);
        for q in quoters.iter_mut() {
            q.set_toxgate(Some(gate.clone()));
        }
    }
    match (&load.stale, &load.gate) {
        (None, _) => {}
        // Installed but not current: the quoter withholds every side this
        // document covers, and `tox_tick` clears it when a fresh file appears.
        (Some(why), Some(_)) => {
            eprintln!("[toxgate] STALE ({why}) — every side it covers is withheld")
        }
        // Nothing parsed, so we do not even know WHICH sides the model covers
        // and cannot withhold them individually. Loud, because until a readable
        // file appears there is no gate on this book at all.
        (Some(why), None) => eprintln!(
            "[toxgate] UNREADABLE ({why}) — NO adverse-selection gate is in force"
        ),
    }
    Policy { toxgate_file: Some(path.to_string()), apr, apr_installed }
}

/// The trade ledger as read ONCE at startup.
///
/// Both seeds fold over the SAME snapshot, and that is a correctness
/// requirement rather than an optimisation — see `seed_exposure_from_census`.
type LedgerRead = Result<Vec<serde_json::Value>, String>;

/// The capital gate every quoter consults, sized from `--exec-config` /
/// `--topics` and seeded with the exposure already on the book.
fn build_risk_view(
    args: &Args,
    quoters: &mut [Quoter],
    rel_meta: &HashMap<String, (String, String)>,
    ledger: &LedgerRead,
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
    seed_exposure_from_ledger(&rv, ledger, &args.ledger, rel_meta);
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
    ledger: &LedgerRead,
    ledger_path: &str,
    rel_meta: &HashMap<String, (String, String)>,
) {
    match ledger {
        Ok(recs) => {
            let open = ledger::open_exposure(recs.clone());
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

/// Hedge obligations a PREVIOUS run of this unit minted and never booked.
///
/// `seed_exposure_from_ledger` above cannot see these: `book_basket` writes the
/// ledger only when the hedge FILLS, so an obligation whose hedge never filled
/// leaves the ledger empty while the OTHER leg is real at the venue. That is
/// exactly what happened at 00:53:50 on 2026-07-29 — see `orphan`, which also
/// explains why the census reads this engine's own `--out` stream instead of a
/// venue position, and why it reports rather than hedges.
///
/// Returns the contract count for the standing `hedges_undischarged` gauge, so
/// this is visible to a monitor every stats tick and not only in the scrollback
/// of a startup nobody was watching.
///
/// AND SEEDS THE RISK VIEW WITH IT. The census was a display line and nothing
/// else: its only consumer was `RunCfg::hedges_undischarged`, which `summary()`
/// prints. So a restart that correctly REPORTED an undischarged obligation
/// still believed the relationship was flat, and would size a fresh basket up
/// to the full per-relationship cap on top of a real, unhedged position. The
/// leg is at the venue; it belongs in the number the caps are measured against.
///
/// NO DOUBLE COUNT, by construction rather than by care: `Undischarged::missing`
/// is `owed - booked`, and `booked` is exactly the ledger's own record — the one
/// `seed_exposure_from_ledger` has already seeded from. What is seeded here is
/// the remainder that the ledger does not contain. When the obligation is later
/// completed (by `arbbot-hedge.timer`, which owns naked-leg completion) a basket
/// is appended and the NEXT startup's census reads `owed - booked = 0`, so the
/// same contracts move from this seed to the ledger seed without ever being in
/// both. Nothing in THIS process can book them: the maker order belongs to a
/// previous run, so `book_basket`'s `order_rel` lookup misses and says so.
///
/// THAT PARTITION HOLDS ONLY IF BOTH SEEDS FOLD OVER THE SAME SNAPSHOT, which
/// is why the ledger is read ONCE in `main` and passed to both. It used to be
/// read twice, with `startup_sweep`'s venue round-trip — seconds — in between,
/// and `arbbot-hedge.timer` fires every 5 minutes. A completing basket landing
/// in that window is seen by NEITHER seed: the ledger read already happened, so
/// it books nothing, and the census read does see it, so `draw_down` discharges
/// the obligation and `missing()` is 0. The contracts are then invisible to
/// every cap for the life of the process — the exact direction this whole
/// change exists to close, and reached through its own fix. The ordering was
/// the unlucky one of the two: census-first would have DOUBLE counted, which is
/// merely conservative. Nor is it a low-correlation race — a restart carrying a
/// naked leg is precisely the state in which that timer is trying to write.
fn report_undischarged_hedges(
    args: &Args,
    quoters: &[Quoter],
    risk: Option<&risk::RiskView>,
    rel_meta: &HashMap<String, (String, String)>,
    ledger: &LedgerRead,
    armed: bool,
    bench: bool,
) -> u64 {
    // bench/replay reads no ledger and must stay byte-deterministic; an
    // offline tape has no previous run of its own to answer for.
    if bench {
        return 0;
    }
    // ONLY an armed run. `run_cfg` gives an unarmed engine `ledger_path: None`,
    // so a shadow books nothing at all — every obligation it has ever minted
    // would read as undischarged, forever, and the one line that matters would
    // drown in them.
    if !armed {
        return 0;
    }
    let Some(out) = args.out.as_deref() else {
        eprintln!(
            "[hedge] no --out: this run cannot record its own hedge obligations, so a \
             restart will not be able to see one it left naked. Pass --out."
        );
        return 0;
    };
    let Ok(ledger) = ledger else {
        // `place_preconditions` already refuses to arm on this and names the
        // damage; saying it twice here would only bury it.
        return 0;
    };
    // Every leg of every relationship this run quotes: an obligation is owed on
    // the OTHER leg, and either leg can be the other one.
    let mut rel_of = std::collections::BTreeMap::new();
    for q in quoters {
        for l in &q.rel.legs {
            rel_of.insert(
                l.market_id.clone(),
                (q.rel.id.clone(), l.venue.as_str().to_string()),
            );
        }
    }
    let found =
        orphan::undischarged(&std::fs::read_to_string(out).unwrap_or_default(), ledger.clone());
    for l in orphan::report(&found, &rel_of, arb_core::clock::now_s()) {
        eprintln!("{l}");
    }
    if let Some(rv) = risk {
        seed_exposure_from_census(rv, &found, &registry_class_of(&args.registry, rel_meta));
    }
    found.iter().map(|u| u.missing() as u64).sum()
}

/// Hedge market -> (relationship id, class) over the FULL registry.
///
/// Deliberately NOT `rel_of` above, which is built from the QUOTERS. That map
/// answers "could this run hedge it", which is the report's question. The CLASS
/// of the exposure is a different question and a registry fact: it is still in
/// hand for a relationship merely outside `--rel-prefix` or gated out this run,
/// and every obligation in `--out` was minted by a run that did quote it. Using
/// the quoting subset there routed those to `unknown`, which is strictly LOOSER
/// — no quoter can carry rtype `unknown`, so that bucket is inert against the
/// class cap and only the global cap holds it. `rel_meta` is the same
/// full-registry classification `seed_exposure_from_ledger` uses, keyed by id;
/// this only supplies the id.
///
/// A registry that will not load leaves this empty, and the caller falls back
/// to `unknown` — `load_quoters` has already `expect`ed the same file several
/// frames earlier, so this is unreachable in practice and is not a second place
/// to die.
fn registry_class_of(
    registry: &str,
    rel_meta: &HashMap<String, (String, String)>,
) -> HashMap<String, (String, String)> {
    let mut out = HashMap::new();
    let Ok(reg) = arb_registry::Registry::load(registry) else { return out };
    for r in &reg.relationships {
        let Some((_, class)) = rel_meta.get(&r.id) else { continue };
        for l in &r.legs {
            out.insert(l.market_id.clone(), (r.id.clone(), class.clone()));
        }
    }
    out
}

/// Book the census as open exposure, so the caps are measured against the leg
/// that is actually at the venue.
///
/// THIS TIGHTENS A CAP THAT IS ALREADY NEARLY FULL. The seeded contracts land
/// in the same class the ledger seed has already largely filled, so the class
/// refusal tightens by exactly the seeded quantity and the remaining headroom
/// there is materially reduced: a clip that passes today can be refused once
/// this lands. That is the intended behaviour — the contracts are real and the
/// point of this change is that the gate should see them — but it is worth
/// knowing before it happens rather than after. `utilization()` does not move,
/// because the same exposure already clamps it to its ceiling.
///
/// Figures deliberately omitted: this repository is public, while
/// `config/registry.yaml` and `data/` (which carries the position ledger) are
/// gitignored. `config/exec.yaml` is tracked, so the bankroll and the class
/// ratio are public by the owner's choice; what is open against them is not.
///
/// The classification rule is `seed_exposure_from_ledger`'s, because it is the
/// same problem: what cannot be classified is booked under `unknown`, which
/// counts toward the GLOBAL cap (a sum over per-relationship exposure) without
/// inflating a class cap it may not belong to. An obligation on a market the
/// registry no longer carries at all has no relationship id here, so it is
/// booked under the market instead, which keeps it in the global sum and in the
/// per-relationship listing an operator reads.
fn seed_exposure_from_census(
    rv: &risk::RiskView,
    found: &[orphan::Undischarged],
    class_of: &HashMap<String, (String, String)>,
) {
    for u in found {
        let qty = u.missing() as f64;
        if qty <= 0.0 {
            continue;
        }
        let (rel_id, class) = match class_of.get(&u.hedge_market) {
            Some((rel_id, class)) => (rel_id.clone(), class.clone()),
            None => (format!("unknown:{}", u.hedge_market), "unknown".to_string()),
        };
        rv.record_open(&rel_id, &class, qty);
        eprintln!(
            "[risk] seeded {qty:.0} undischarged contract(s) on {rel_id} (class {class}, \
             hedge owed on {}) — the ledger does not carry them, because `book_basket` \
             writes only when the HEDGE fills. The caps now see this leg.",
            u.hedge_market
        );
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
        //
        // Kalshi's `cancel_all_open` is scoped to ids this stack minted, so it
        // no longer cancels the Python stack's resting orders — but PM-US's
        // still cancels its whole account (no client-order-id on the wire), and
        // `--sweep-unscoped` puts Kalshi back that way on purpose. Neither is
        // per-relationship. Clearing the 15:40 orphan was run account-wide; it
        // happened to find one order, which was luck, not safety.
        for l in sweep_only_blast_radius(&args.cred_suffix, args.sweep_unscoped) {
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
fn spawn_fill_feeds(
    args: &Args,
    tx_acks: Option<&tokio::sync::mpsc::Sender<feed::FeedMsg>>,
    kalshi: Option<std::sync::Arc<dyn sink::OrderSink>>,
) {
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
                // The sink is the fill feed's read handle for venue truth, so
                // its reconciliation spends the SAME background budget as the
                // rest of the process (quirk `xv-shared-api-budget`).
                tokio::spawn(fills::kalshi_fill_feed(kid, pem, t.clone(), kalshi.clone()));
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
    undischarged: u64,
    policy: Policy,
) -> engine::RunCfg {
    engine::RunCfg {
        hedges_undischarged: undischarged,
        out_path: args.out,
        kill_file: args.kill_file,
        stats_every_s: args.stats_every_s,
        bench,
        wal_path: args.wal,
        // bench/replay must stay byte-deterministic and have no live feed.
        health_file: (!bench && !args.health.is_empty()).then(|| args.health.clone()),
        toxgate_file: policy.toxgate_file,
        apr: policy.apr,
        apr_installed: policy.apr_installed,
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
    // ONE read of the trade ledger, folded by BOTH seeds. Reading it twice
    // straddles `startup_sweep`'s venue round-trip, and a basket appended by
    // arbbot-hedge.timer inside that window reaches neither seed — see
    // `seed_exposure_from_census`.
    let ledger = ledger::read(&args.ledger);
    // Risk is OFF in bench/replay: those pin a decision digest, and a capital
    // gate is not part of that contract. It is built BEFORE the policy because
    // the maker APR hurdle floats with the utilization this view holds.
    let risk = (!bench).then(|| build_risk_view(&args, &mut quoters, &rel_meta, &ledger));
    let policy = install_policy(&args, &mut quoters, risk.as_deref());
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
        let unconfirmed = match startup_sweep(&sinks, &sink::SweepPolicy::default()).await {
            Ok(u) => u,
            Err(e) => {
                eprintln!("[exec] STARTUP SWEEP FAILED: {e}");
                eprintln!("[exec] refusing to arm: the book could not be proven clean");
                std::process::exit(10);
            }
        };
        if args.sweep_only {
            // The verdict, not just the fact that we finished. This is the tool
            // the halt banner tells a human to reach for, so "reconciled" over
            // a book no venue could confirm — and an exit 0 with it — is the
            // one answer it must never give.
            if unconfirmed.is_empty() {
                eprintln!("[exec] --sweep-only: book reconciled, exiting without quoting");
                return;
            }
            eprintln!(
                "[exec] ### --sweep-only: the cancel went in, but {} venue(s) could NOT \
                 CONFIRM the book: {}",
                unconfirmed.len(),
                unconfirmed.join("; ")
            );
            eprintln!(
                "[exec] ### exiting {} — nothing was seen resting, and nothing is proven",
                exec::EXIT_BOOK_UNCONFIRMED
            );
            std::process::exit(exec::EXIT_BOOK_UNCONFIRMED);
        }
        spawn_shutdown_sweep();
        spawn_fill_feeds(&args, tx_acks.as_ref(), sinks.get(&Venue::Kalshi).cloned());
    }

    let armed = !sinks.is_empty();
    // Before the first quote: what did the LAST run of this unit leave naked?
    // ...and it does not merely REPORT it: an obligation the ledger cannot see
    // is still exposure, so it is seeded into the same risk view the ledger
    // seeded, before the first quote consults it.
    let undischarged = report_undischarged_hedges(
        &args,
        &quoters,
        risk.as_deref(),
        &rel_meta,
        &ledger,
        armed,
        bench,
    );
    let acks = if sinks.is_empty() { None } else { tx_acks.clone() };
    let (exec_txs, exec_stats) = exec::spawn_executors(rate, sinks, acks);
    let cfg =
        run_cfg(args, bench, armed, !exec_txs.is_empty(), risk, undischarged, policy);
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
  - id: rejected-and-allowlisted
    type: cross-venue-equivalent
    verdict: rejected
    vetted_by: agent
    legs:
      - {venue: kalshi, market_id: K5}
      - {venue: polymarket_us, market_id: P5}
  - id: unknown-verdict-and-allowlisted
    type: cross-venue-equivalent
    verdict: not-equivalent
    vetted_by: agent
    legs:
      - {venue: kalshi, market_id: K6}
      - {venue: polymarket_us, market_id: P6}
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

    /// Revoking a pair must actually revoke it. Editing the entry to
    /// `verdict: rejected` — the registry's only revocation — used to leave an
    /// ALLOWLISTED rel quoting, because the allowlist half of the gate never
    /// looked at the verdict. An unrecognised verdict fails the same way.
    #[test]
    fn a_rejected_verdict_vetoes_an_allowlisted_relationship() {
        let d = scratch("veto");
        let reg = d.join("registry.yaml");
        std::fs::write(&reg, REGISTRY).unwrap();
        let allow = d.join("tradable.yaml");
        std::fs::write(
            &allow,
            "allow:\n  - allowlisted\n  - rejected-and-allowlisted\n  \
             - unknown-verdict-and-allowlisted\n",
        )
        .unwrap();

        let got = ids(reg.to_str().unwrap(), allow.to_str().unwrap(), &[]);
        assert_eq!(
            got,
            vec!["human-ok".to_string(), "allowlisted".to_string()],
            "the allowlist must still grant, and must never override a verdict"
        );
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

    /// A resting list this process cannot READ must not be able to stop it
    /// starting, when the cancel-all itself was accepted and nothing was ever
    /// seen resting.
    ///
    /// This is the second blocking review finding. Nobody has captured what
    /// PM-US returns for an EMPTY book; if that shape is one `open_orders`
    /// cannot parse, a fail-closed proof would refuse to arm on every start and
    /// exit 17 on every shutdown, permanently, over an empty book. Absence of
    /// evidence is not evidence of a leak — so it arms, and the raw body goes
    /// in the log so the shape finally gets captured.
    #[tokio::test]
    async fn an_unreadable_resting_list_arms_loudly_instead_of_refusing() {
        struct Unreadable {
            cancelled: std::sync::Mutex<bool>,
        }
        impl sink::OrderSink for Unreadable {
            fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
                unreachable!("a sweep never places")
            }
            fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
                unreachable!("a sweep uses cancel_all_open")
            }
            fn cancel_all_open(&self) -> Result<(), VenueError> {
                *self.cancelled.lock().unwrap() = true;
                Ok(()) // the venue ACCEPTED the cancel
            }
            fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
                Err(VenueError::Parse {
                    endpoint: "pmus:open_orders",
                    detail: "missing field `orders` — body was: {}".into(),
                })
            }
        }
        let m = std::sync::Arc::new(Unreadable { cancelled: std::sync::Mutex::new(false) });
        let mut h: HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> = HashMap::new();
        h.insert(Venue::PolymarketUs, m.clone());
        let unconfirmed = startup_sweep(&h, &fast())
            .await
            .expect("an unread list is not a leak, and must not manufacture an outage");
        assert!(*m.cancelled.lock().unwrap(), "the cancel-all still went in");
        // THE RETURNED VALUE, not just the Ok. It is what drives the `###`
        // banner and, on `--sweep-only`, exit 18 — and this assertion was
        // missing, so deleting the `unconfirmed.push` left every test green
        // while `--sweep-only` exited 0 over a book no venue could confirm.
        assert_eq!(unconfirmed.len(), 1, "the venue must be REPORTED, not just tolerated");
        assert!(unconfirmed[0].contains("PolymarketUs"), "{unconfirmed:?}");
        assert!(
            unconfirmed[0].contains("could NOT be proven clean"),
            "carries the verdict the banner prints: {unconfirmed:?}"
        );
    }

    /// ...and a book that IS proven clean reports nothing, or the banner and
    /// exit 18 would fire on every ordinary start.
    #[tokio::test]
    async fn a_clean_book_reports_nothing_unconfirmed() {
        let (h, _m) = sinks(&[], &[]);
        assert!(startup_sweep(&h, &fast()).await.unwrap().is_empty());
    }

    /// ...but the leniency is exactly that narrow. A cancel-all the venue never
    /// ACCEPTED means the sweep never got its instruction in, so there is no
    /// basis to proceed — that stays fail-closed whatever the list said.
    #[tokio::test]
    async fn a_cancel_all_the_venue_refused_still_refuses_to_arm() {
        struct RefusesBoth;
        impl sink::OrderSink for RefusesBoth {
            fn place(&self, _r: &PlaceRequest) -> Result<String, VenueError> {
                unreachable!()
            }
            fn cancel(&self, _r: &CancelRequest) -> Result<(), VenueError> {
                unreachable!()
            }
            fn cancel_all_open(&self) -> Result<(), VenueError> {
                Err(VenueError::Transport("429".into()))
            }
            fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
                Err(VenueError::Transport("503".into()))
            }
        }
        let mut h: HashMap<Venue, std::sync::Arc<dyn sink::OrderSink>> = HashMap::new();
        h.insert(Venue::Kalshi, std::sync::Arc::new(RefusesBoth));
        let err = startup_sweep(&h, &fast()).await.unwrap_err();
        assert!(err.contains("could NOT be proven clean"), "{err}");
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
        let none = sweep_only_blast_radius(&[], false).join("\n");
        assert!(none.contains("WHOLE ACCOUNT"), "{none}");
        assert_eq!(
            none.matches("PRIMARY key").count(),
            2,
            "both venues warn when no suffix is given: {none}"
        );
        assert!(none.contains("SHARED WITH THE PYTHON STACK"), "{none}");
        // ...and WHICH venue is the wide one, now that Kalshi's sweep is scoped
        // to ids this stack minted and PM-US's still cannot be.
        let (k, p) = none.split_once("polymarket_us").expect("{none}");
        assert!(!k.contains("WHOLE ACCOUNT"), "kalshi is scoped now: {k}");
        assert!(p.contains("WHOLE ACCOUNT"), "PM-US has no id of ours to scope by: {p}");

        // ...and the override moves Kalshi back, which the banner MUST say: the
        // flag exists to re-arm the shared-account risk, so a run that is about
        // to take it has to see that on screen.
        let un = sweep_only_blast_radius(&[], true).join("\n");
        let (uk, _) = un.split_once("polymarket_us").expect("{un}");
        assert!(uk.contains("--sweep-unscoped IS SET"), "{uk}");
        assert!(uk.contains("WHOLE ACCOUNT"), "the wider radius must be stated: {uk}");

        let both = sweep_only_blast_radius(
            &[("kalshi".into(), "rs_trader".into()), ("pmus".into(), "rs_trader".into())],
            false,
        )
        .join("\n");
        assert!(!both.contains("PRIMARY key"), "a suffixed run is scoped: {both}");
        assert_eq!(both.matches("suffix `rs_trader`").count(), 2);

        // The real trap: a pmus suffix given, kalshi forgotten.
        let half =
            sweep_only_blast_radius(&[("pmus".into(), "rs_trader".into())], false).join("\n");
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

/// **The three quoter policies this binary built and never installed.**
///
/// `Quoter::new` hardcodes `apr_margin: None`, an empty `suppress` and
/// `toxgate: None`. `arb-trader` called `set_risk` and nothing else, so all
/// three survived into every armed session — `bins/arb-intent`, a replay tool,
/// was the only caller of the other three setters in the entire workspace.
///
/// The quoter's policy fields are private, and left that way on purpose: what
/// is asserted here is what an operator would see — the same relationship, the
/// same book, and a DIFFERENT decision once the policy is installed. A test
/// that read the fields would pass against a quoter that stored them and then
/// ignored them.
#[cfg(test)]
mod policy_wiring_tests {
    use super::*;
    use arb_core::book::BookBuilder;
    use arb_core::fees::FeeSchedule;
    use arb_core::intent::Intent;
    use arb_core::model::Level;

    /// Deliberately in a family `arb_core::resolve` has a date for:
    /// `xvus-france-pres-27` resolves 2027-04-25, so the hurdle has a real hold
    /// to annualize over. It is also the family the defect was measured on.
    const REGISTRY: &str = r#"
relationships:
  - id: xvus-france-pres-27-test
    type: cross-venue-equivalent
    verdict: equivalent
    vetted_by: human
    legs:
      - {venue: kalshi, market_id: K}
      - {venue: polymarket_us, market_id: P}
"#;

    /// The as-of day every APR assertion here is measured from. Pinned: the
    /// hurdle shrinks as 2027-04-25 approaches, so a test reading the wall
    /// clock would start failing on its own one day.
    const ASOF: &str = "2026-07-29";

    fn scratch(name: &str) -> std::path::PathBuf {
        let d =
            std::env::temp_dir().join(format!("arb-trader-policy-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn lvl(p: &str, s: &str) -> Level {
        Level { price: p.into(), size: s.into() }
    }

    /// Kalshi bid 0.60 funds a PM-US maker YES bid (hedging NO costs 0.40);
    /// PM-US is already bid `pm_bid`, so the quote we would rest sits one tick
    /// inside it.
    fn books(pm_bid: &str) -> BookBuilder {
        let mut bb = BookBuilder::new();
        bb.apply_snapshot(Venue::Kalshi, "K", vec![lvl("0.60", "500")],
                          vec![lvl("0.99", "1")], 1, 1_000_000_000, None);
        bb.apply_snapshot(Venue::PolymarketUs, "P", vec![lvl(pm_bid, "500")],
                          vec![lvl("0.99", "1")], 1, 1_000_000_000, None);
        bb
    }

    /// Args that reach the quoter through the REAL path — `load_quoters` then
    /// `install_policy` — with every mode default pinned, so a test asserts on
    /// what it set and nothing else.
    fn args(dir: &std::path::Path) -> Args {
        let reg = dir.join("registry.yaml");
        std::fs::write(&reg, REGISTRY).unwrap();
        let mut a = default_args();
        a.registry = reg.to_string_lossy().into_owned();
        a.tradable = "/nonexistent/tradable.yaml".into(); // registry vetting alone
        a.apr_asof = Some(ASOF.into());
        a.min_apr = Some(0.0);
        a.toxgate = None;
        a
    }

    /// Wire the policy onto real quoters and decide ONE book event with them.
    /// `risk` is None, i.e. every assertion below rides on an EXPLICIT
    /// `--min-apr`; the floating default has its own test.
    fn decide(a: &Args, pm_bid: &str, now: f64) -> Vec<Intent> {
        let (mut quoters, _, _) = load_quoters(&a.registry, &a.tradable, &[]);
        assert_eq!(quoters.len(), 1, "the fixture relationship must survive the gate");
        install_policy(a, &mut quoters, None);

        let bb = books(pm_bid);
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let mut oid = 0u64;
        let mut intents = Vec::new();
        quoters[0].on_book(&mut cx, &fees, &bb, now, &mut oid, &mut intents);
        intents
    }

    fn place_on<'a>(intents: &'a [Intent], market: &str) -> Option<&'a arb_core::intent::Place> {
        intents.iter().find_map(|i| match i {
            Intent::Place(p) if p.place == market => Some(p),
            _ => None,
        })
    }

    fn skips(intents: &[Intent]) -> Vec<String> {
        intents
            .iter()
            .filter_map(|i| match i {
                Intent::Skip(s) => Some(s.skip.join("; ")),
                _ => None,
            })
            .collect()
    }

    /// THE hurdle, on the family it was measured on. With none, the engine
    /// rests a quote one tick inside the touch — on france-pres-27 that locks
    /// $0.01 on $4.99 committed until 2027-04-25, which is 0.27%/yr.
    #[test]
    fn the_apr_hurdle_reaches_the_live_quoter() {
        let d = scratch("apr");
        let mut a = args(&d);

        let off = decide(&a, "0.55", 1_000.0);
        let p = place_on(&off, "P").expect("with no hurdle the tick-lock quote rests");
        assert_eq!(p.price, "0.56", "one tick inside the 0.55 touch");

        // Same book, same relationship, hurdle at the bar a nearly-full book
        // asks for (`apr_bar(0.87)` ~ 14.5).
        a.min_apr = Some(apr_bar(0.87));
        let on = decide(&a, "0.55", 1_000.0);
        assert!(
            place_on(&on, "P").is_none(),
            "a 0.27%/yr lock must not rest under a 14.5%/yr hurdle: {on:?}"
        );

        // ...and the hurdle narrows the quote rather than switching the maker
        // off: a book that leaves room still gets one.
        let wide = decide(&a, "0.30", 1_000.0);
        assert!(
            place_on(&wide, "P").is_some(),
            "the hurdle must not silence a genuinely profitable book: {wide:?}"
        );
    }

    /// **The bar FLOATS with capital utilization** — Geoff 2026-07-22, card
    /// 80ff7987, ported from `exec/main.py:_tt_refresh`, which pushed the same
    /// number onto every maker ("makers clear the same bar").
    ///
    /// A flat constant is wrong in the expensive direction. `utilization()`
    /// divides the exposure accumulator — CONTRACTS, as Python's
    /// `risk.exposure.total` was — by the dollar class budget, and on
    /// 2026-07-29 the ledger seeds 346 open contracts against $343, so util
    /// clamps to 1.000 and the policy asks the CEILING. A flat 12.0 would have
    /// rested quotes the policy refuses, exactly when capital is scarcest.
    #[test]
    fn the_hurdle_floats_with_capital_utilization() {
        assert_eq!(apr_bar(0.0), APR_FLOOR, "idle capital takes the floor");
        assert_eq!(apr_bar(1.0), APR_CEIL, "a full book demands the ceiling");
        // Geoff's 8% reference sits at ~1/3 utilization (exec/main.py:47-51).
        assert!((apr_bar(1.0 / 3.0) - 8.0).abs() < 0.01, "{}", apr_bar(1.0 / 3.0));

        // The live figure, and the crossing that makes a flat 12.0 unsafe.
        let live = apr_bar(346.0 / 343.0);
        assert_eq!(live, APR_CEIL, "an over-cap book is clamped to the ceiling");
        assert!(live > 12.0, "a flat 12.0 would UNDER-charge a full book: {live}");
        // 12.0 is the bar only at util 2/3, and take-take's own DEFAULT_BAR_APR
        // is the cold-start arm of `Bar::tradable`, never the bar in force with
        // a live portfolio. The two are not the same number and never were.
        assert!((apr_bar(2.0 / 3.0) - crate::taketake::DEFAULT_BAR_APR).abs() < 0.01);
    }

    /// `--suppress market:side`, the same spelling `arb-intent` takes. Nothing
    /// in this binary populates it dynamically (see `install_policy`), so the
    /// property is only that an operator's declaration reaches the quoter.
    #[test]
    fn the_suppress_set_reaches_the_live_quoter() {
        let d = scratch("suppress");
        let mut a = args(&d);
        assert!(place_on(&decide(&a, "0.30", 1_000.0), "P").is_some(), "control");

        a.suppress = vec![("P".into(), BookSide::Bid)];
        let held = decide(&a, "0.30", 1_000.0);
        assert!(
            place_on(&held, "P").is_none(),
            "a side another owner holds must not be quoted: {held:?}"
        );
    }

    /// The toxicity gate. `TOXGATE_MAX` is 0.03/ct and the research feed was
    /// scoring live sides at 0.0822-0.2199 — up to 7x — while the skip at
    /// quoter.rs:413 was unreachable in this process.
    #[test]
    fn the_toxgate_reaches_the_live_quoter() {
        let d = scratch("toxgate");
        let mut a = args(&d);
        assert!(place_on(&decide(&a, "0.30", 1_000.0), "P").is_some(), "control");

        // The feed's clock and the quoter's are both epoch seconds, so a
        // fixture has to be stamped NOW to be current.
        let now = arb_core::clock::now_s();
        let f = d.join("toxgate.json");
        std::fs::write(&f, format!(r#"{{"ts": {now}, "markets": {{"P": {{"bid": 0.0822}}}}}}"#))
            .unwrap();
        a.toxgate = Some(f.to_string_lossy().into_owned());

        let gated = decide(&a, "0.30", now);
        assert_eq!(skips(&gated), vec!["toxgate bid 0.082 > 0.03"]);
        assert!(place_on(&gated, "P").is_none(), "a toxic side rests nothing: {gated:?}");
    }

    /// A feed that cannot be READ is not a feed that said yes. The file on disk
    /// when this was written was stamped 2026-07-26 and nothing had reloaded it
    /// since, so the gate an armed run installed was three days past
    /// `TOXGATE_MAX_AGE` — and answered every consultation with a free pass.
    #[test]
    fn a_stale_toxgate_file_does_not_silently_permit() {
        let d = scratch("stale");
        let now = arb_core::clock::now_s();
        let f = d.join("toxgate.json");
        let path = f.to_string_lossy().into_owned();
        std::fs::write(&f, format!(r#"{{"ts": {}, "markets": {{}}}}"#, now - 3.0 * 86_400.0))
            .unwrap();

        let load = load_toxgate(&path, now);
        let why = load.stale.expect("3 days old must not read as current");
        assert!(why.contains("old"), "it must say the AGE refused it: {why}");
        // ...and the DOCUMENT survives the refusal. Dropping it is a
        // fail-open: the quoter would hold no gate, so it could not tell a
        // side this model covers from one it has never scored, and the pinned
        // tape would digest exactly as if no gate existed. It did, at first.
        assert!(load.gate.is_some(), "a stale document is still the coverage map");

        // A MISSING file is a refusal too — but it costs the coverage map,
        // which is a strictly worse position and is logged as its own case.
        let gone = load_toxgate("/nonexistent/toxgate.json", now);
        assert!(gone.stale.is_some() && gone.gate.is_none());
        // ...and so is a document that cannot say how old it is.
        std::fs::write(&f, r#"{"markets": {}}"#).unwrap();
        assert!(load_toxgate(&path, now).stale.is_some(), "no ts is not a fresh ts");

        // The path is still handed to the engine, which is what re-reads it and
        // reports the gauge — an unusable feed is refused, never skipped over.
        let mut a = args(&d);
        a.toxgate = Some(path.clone());
        assert_eq!(install_policy(&a, &mut [], None).toxgate_file, Some(path));
    }

    /// **A stale feed must still be INSTALLED, or the refusal cannot happen.**
    ///
    /// Caught by the pinned tape, not by a unit test: with the stale document
    /// dropped on the floor, `--toxgate <the real 2.7-day-old file>` digested
    /// byte-identically to having no gate at all (`f4141b53…`, 182 places).
    /// The quoter held `toxgate: None`, so `verdict` was never consulted and
    /// every covered side quoted freely — the exact fail-open this change
    /// exists to remove, reintroduced one layer up.
    #[test]
    fn a_stale_feed_is_installed_so_the_covered_side_is_actually_withheld() {
        let d = scratch("stale-installed");
        let now = arb_core::clock::now_s();
        let f = d.join("toxgate.json");
        // Covers P, and harmlessly clean at 0.001 — the AGE is the refusal.
        std::fs::write(
            &f,
            format!(
                r#"{{"ts": {}, "markets": {{"P": {{"bid": 0.001}}}}}}"#,
                now - 3.0 * 86_400.0
            ),
        )
        .unwrap();
        let mut a = args(&d);
        a.toxgate = Some(f.to_string_lossy().into_owned());

        let gated = decide(&a, "0.30", now);
        assert!(
            place_on(&gated, "P").is_none(),
            "a covered side on a stale opinion must be withheld: {gated:?}"
        );
        assert!(
            skips(&gated).iter().any(|s| s.contains("toxgate feed")),
            "and say the feed's age is why: {gated:?}"
        );
    }

    /// **The toxgate is OFF unless asked for, because NOTHING WRITES ITS FILE.**
    ///
    /// `scripts/toxgate_daemon.py` and `scripts/toxgate_evidence.py` were
    /// deleted in 3e4e80d (the rust rewrite) and never replaced; every
    /// remaining mention of `toxgate.json` in the tree is a reader, and the
    /// Python stack is frozen so the deleted writer may not be run either. A
    /// default-on gate could therefore never clear, and `arbbot-trader-rs`
    /// (Restart=always, no `--bench-tape`) would withhold every scored side
    /// from its next restart onward.
    #[test]
    fn the_toxgate_is_off_unless_a_path_is_given() {
        let d = scratch("default-off");
        let mut a = args(&d);

        a.toxgate = None;
        assert_eq!(install_policy(&a, &mut [], None).toxgate_file, None, "no flag, no gate");

        a.toxgate = Some(String::new());
        assert_eq!(install_policy(&a, &mut [], None).toxgate_file, None, "empty is off too");

        a.toxgate = Some("/pinned/toxgate.json".into());
        assert_eq!(
            install_policy(&a, &mut [], None).toxgate_file,
            Some("/pinned/toxgate.json".to_string()),
            "and a path turns it on, wherever it points"
        );
    }

    /// bench/replay pins a decision digest against a fixed tape, and the
    /// floating hurdle cannot be pinned by one: it rides on a utilization the
    /// tape does not contain. `risk: None` IS bench here — same condition, one
    /// place. An explicit `--min-apr` still wins, which is how the hurdle's
    /// effect gets replayed against the golden tape at a fixed bar.
    #[test]
    fn bench_has_no_floating_bar_but_honors_an_explicit_one() {
        let d = scratch("bench");
        let mut a = args(&d);
        a.min_apr = None; // float — but there is nothing to float on

        let (mut quoters, _, _) = load_quoters(&a.registry, &a.tradable, &[]);
        let apr = install_policy(&a, &mut quoters, None).apr;
        assert!(apr.is_none(), "nothing to refresh without a risk view");

        let bb = books("0.55");
        let decide_now = |quoters: &mut Vec<Quoter>| {
            let mut cx = Cx::default();
            let fees = FeeSchedule::new(&mut cx);
            let (mut oid, mut intents) = (0u64, Vec::new());
            quoters[0].on_book(&mut cx, &fees, &bb, 1_000.0, &mut oid, &mut intents);
            intents
        };
        assert_eq!(
            place_on(&decide_now(&mut quoters), "P").map(|p| p.price.as_str()),
            Some("0.56"),
            "bench must decide as it always has"
        );

        // ...and an explicit bar bites in bench exactly as it does live.
        a.min_apr = Some(APR_CEIL);
        let (mut quoters, _, _) = load_quoters(&a.registry, &a.tradable, &[]);
        install_policy(&a, &mut quoters, None);
        assert!(place_on(&decide_now(&mut quoters), "P").is_none());
    }

    /// `--min-apr -5` used to hit a `< 0.0` sentinel and silently become 12.0.
    /// A nonsense bar must be the bar you asked for, not a different one.
    #[test]
    fn a_negative_min_apr_is_taken_literally_not_read_as_a_sentinel() {
        let d = scratch("negative");
        let mut a = args(&d);
        a.min_apr = Some(-5.0);
        let (mut quoters, _, _) = load_quoters(&a.registry, &a.tradable, &[]);
        let (bar, _, _) = apply_apr(&mut quoters, a.min_apr, a.apr_asof.as_deref(), None);
        assert_eq!(bar, -5.0, "the sentinel must not swallow a real value");
        // `set_apr` treats <= 0 as OFF, so the decision is the unhurdled one.
        install_policy(&a, &mut quoters, None);
        let bb = books("0.55");
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let (mut oid, mut intents) = (0u64, Vec::new());
        quoters[0].on_book(&mut cx, &fees, &bb, 1_000.0, &mut oid, &mut intents);
        assert_eq!(place_on(&intents, "P").map(|p| p.price.as_str()), Some("0.56"));
    }
}

/// A hedge obligation a previous run never discharged is EXPOSURE, and the caps
/// have to see it.
///
/// `book_basket` writes `data/exec/trades.jsonl` only when the HEDGE fills, so
/// an obligation whose hedge never filled leaves no ledger record —
/// `seed_exposure_from_ledger` seeds that relationship at zero. `orphan`
/// computed the missing contracts correctly and the number went into
/// `RunCfg::hedges_undischarged`, whose ONLY consumer is the display line in
/// `Engine::summary`. Nothing called `record_open` for it, so the new run
/// believed the relationship was flat and would size a fresh basket up to the
/// full per-relationship cap on top of a real, unhedged position.
#[cfg(test)]
mod undischarged_seed_tests {
    use super::*;
    use arb_core::model::Venue;
    use arb_core::quoter::RiskGate;
    use arb_core::scan::{Rel, RelLeg, RelType};

    /// The 2026-07-29 incident's pair, already named in `arb_core`'s own
    /// crossed-book fixtures. The qty is sized so that the $150
    /// per-relationship cap is the thing that answers; it is not the live one.
    const REL: &str = "xvus-fedcut-26-usfed-2026-cut";
    const KMKT: &str = "KXRATECUT-26DEC31";
    const MAKER: &str = "t1785282065001";

    fn mint(qty: i64) -> String {
        format!(
            r#"{{"anchor_price":"0.1080","hedge_needed":"{KMKT}","order_id":"{MAKER}","qty":{qty},"ts":1785300830.6798358}}"#
        )
    }

    /// The basket `book_basket` writes once the hedge finally fills, with leg 1
    /// naming the maker order the obligation was minted from.
    fn basket(qty: i64) -> serde_json::Value {
        serde_json::from_str(&format!(
            r#"{{"ts":1.0,"relationship_id":"{REL}","qty":{qty},"status":"open","legs":[
                 {{"venue":"polymarket_us","market_id":"P","qty":{qty},"order_id":"{MAKER}"}},
                 {{"venue":"kalshi","market_id":"{KMKT}","qty":{qty}}}]}}"#
        ))
        .expect("fixture")
    }

    /// A scratch dir per TEST, not per process: these run in parallel and each
    /// removes its own on the way out.
    fn dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join(format!("arb-undischarged-{}-{tag}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        std::fs::write(d.join("exec.yaml"), "bankroll_usd: 980\nper_class_cap: 0.35\n")
            .unwrap();
        d
    }

    /// A fresh risk view, as `build_risk_view` makes one: $980 bankroll, both
    /// venues funded, this relationship classified `low` (so the per-rel cap is
    /// the full $150).
    fn view(d: &std::path::Path) -> risk::RiskView {
        risk::RiskView::load(
            d.join("exec.yaml").to_str().unwrap(),
            "/nonexistent/topics.yaml",
            vec![
                ("kalshi".to_string(), "1000".to_string()),
                ("polymarket_us".to_string(), "1000".to_string()),
            ],
            HashMap::from([(REL.to_string(), "low".to_string())]),
        )
    }

    fn rel() -> Rel {
        Rel {
            id: REL.into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: KMKT.into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        }
    }

    /// What `registry_class_of` builds from the FULL registry: the hedge
    /// market's relationship and its class.
    fn class_of() -> HashMap<String, (String, String)> {
        HashMap::from([(
            KMKT.to_string(),
            (REL.to_string(), "cross-venue-equivalent".to_string()),
        )])
    }

    /// THE DEFECT. The obligation is 148 contracts against a $150 cap, so a
    /// clip of 5 must be refused — and was allowed, because the census reached
    /// a gauge and not the gate.
    #[test]
    fn an_undischarged_obligation_does_not_re_authorise_the_per_rel_cap() {
        let d = dir("no-reauth");
        let found = orphan::undischarged(&mint(148), vec![]);
        assert_eq!(found.len(), 1, "the census still finds it");
        assert_eq!(found[0].missing(), 148);

        // Unseeded — the state every restart came up in.
        let flat = view(&d);
        assert!(
            flat.check(&rel(), Venue::Kalshi, 5, None).allowed,
            "an unseeded view believes the relationship is flat"
        );

        let v = view(&d);
        seed_exposure_from_census(&v, &found, &class_of());
        assert_eq!(v.open_ct(REL), 148.0, "the leg is real at the venue");
        let dec = v.check(&rel(), Venue::Kalshi, 5, None);
        assert!(!dec.allowed, "148 open against a 150 cap leaves no room for 5");
        assert!(
            dec.reasons.iter().any(|r| r.contains("per-relationship")),
            "{:?}",
            dec.reasons
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// NO DOUBLE COUNT, and it is structural rather than careful: `missing()`
    /// is `owed - booked`, and `booked` is the ledger's own record — the one
    /// `seed_exposure_from_ledger` has already seeded from. The moment
    /// `arbbot-hedge.timer` completes the hedge and a basket is appended, the
    /// census reads 0 and seeds nothing, so the same contracts move from this
    /// seed to the ledger seed without ever being in both.
    #[test]
    fn a_booked_obligation_is_seeded_by_the_ledger_and_not_again_here() {
        let d = dir("booked-once");
        let ledger = vec![basket(148)];
        assert_eq!(
            crate::ledger::open_exposure(ledger.clone()).get(REL),
            Some(&148.0),
            "the ledger seed carries it once the hedge fills"
        );
        let found = orphan::undischarged(&mint(148), ledger);
        assert!(found.is_empty(), "and the census then finds nothing to add");

        let v = view(&d);
        seed_exposure_from_census(&v, &found, &class_of());
        assert_eq!(v.open_ct(REL), 0.0, "nothing may be counted twice");

        // ...and a PARTIAL booking seeds only the remainder.
        let part = orphan::undischarged(&mint(148), vec![basket(100)]);
        let v = view(&d);
        seed_exposure_from_census(&v, &part, &class_of());
        assert_eq!(v.open_ct(REL), 48.0);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// ...AND THAT HOLDS FOR THE PROCESS THAT ACTUALLY DISCHARGES THESE.
    ///
    /// `book_basket` stamps leg 1 with its maker order id, but this engine does
    /// not complete these obligations — `arbbot-hedge.timer` does, from venue
    /// truth, and `scripts/hedge_naked_legs.py` writes legs with NO `order_id`.
    /// So the two seeds keyed on different things: `ledger::open_exposure`
    /// counts the basket under `relationship_id` while the census's `booked`
    /// found no order to credit, and the SAME contracts were seeded twice
    /// under the same relationship and the same class. `--out` is append-only
    /// across restarts, so it repeated on every startup and never healed.
    ///
    /// The assertion is the sum of BOTH seeds against the truth, because that
    /// sum is what the caps are measured on.
    #[test]
    fn the_two_seeds_sum_to_the_truth_when_the_python_timer_completes_the_hedge() {
        let d = dir("python-timer");
        // The timer's own record shape: no `order_id` on either leg.
        let hedged: serde_json::Value = serde_json::from_str(&format!(
            r#"{{"ts":1785300900.0,"relationship_id":"{REL}","title":"{REL} (naked-leg hedge)",
                 "qty":148,"strategy":"take-take","status":"open","legs":[
                 {{"venue":"kalshi","market_id":"{KMKT}","side":"yes","role":"taker",
                   "qty":148,"yes_price":"0.11"}},
                 {{"venue":"polymarket_us","market_id":"P","side":"no","role":"taker",
                   "qty":148,"yes_price":"0.86"}}]}}"#
        ))
        .expect("fixture");

        // Seed 1: the ledger, keyed on relationship_id.
        let ledger_open = crate::ledger::open_exposure(vec![hedged.clone()]);
        assert_eq!(ledger_open.get(REL), Some(&148.0));

        // Seed 2: the census. The basket is younger than the obligation and
        // names its hedge market, so it discharges it.
        let found = orphan::undischarged(&mint(148), vec![hedged]);
        assert!(found.is_empty(), "the timer completed it");

        let v = view(&d);
        v.record_open(REL, "cross-venue-equivalent", ledger_open[REL]);
        seed_exposure_from_census(&v, &found, &class_of());
        assert_eq!(
            v.open_ct(REL),
            148.0,
            "148 contracts exist; seeding 296 is a phantom that repeats every startup"
        );
        let _ = std::fs::remove_dir_all(&d);
    }

    /// THE WINDOW between two reads, which is why there is now only one.
    ///
    /// `startup_sweep`'s venue round-trip used to sit between the ledger read
    /// and the census read, and `arbbot-hedge.timer` appends every 5 minutes. A
    /// completing basket landing in that window is seen by NEITHER seed: the
    /// ledger read already happened so it books nothing, and the census read
    /// does see it, so `draw_down` discharges the obligation and `missing()` is
    /// 0. What this pins is the ARITHMETIC of that loss — the structural
    /// guarantee is the shared `&LedgerRead` both seeds now take, which is the
    /// compiler's job rather than a test's.
    #[test]
    fn two_snapshots_lose_the_basket_that_lands_between_them() {
        let d = dir("split-read");
        let hedged: serde_json::Value = serde_json::from_str(&format!(
            r#"{{"ts":1785300900.0,"relationship_id":"{REL}","qty":148,"status":"open",
                 "legs":[{{"venue":"kalshi","market_id":"{KMKT}","qty":148}},
                         {{"venue":"polymarket_us","market_id":"P","qty":148}}]}}"#
        ))
        .expect("fixture");

        // TWO snapshots: the ledger seed reads before the append, the census
        // reads after it.
        let v = view(&d);
        for (rel, q) in crate::ledger::open_exposure(vec![]) {
            v.record_open(&rel, "cross-venue-equivalent", q);
        }
        let after = orphan::undischarged(&mint(148), vec![hedged.clone()]);
        seed_exposure_from_census(&v, &after, &class_of());
        assert_eq!(
            v.open_ct(REL),
            0.0,
            "the window: the ledger seed missed the basket and the census \
             discharged the obligation against it, so neither carries the contracts"
        );

        // ONE snapshot, both seeds — whichever side of the append it falls.
        for snapshot in [vec![], vec![hedged]] {
            let v = view(&d);
            for (rel, q) in crate::ledger::open_exposure(snapshot.clone()) {
                v.record_open(&rel, "cross-venue-equivalent", q);
            }
            let found = orphan::undischarged(&mint(148), snapshot);
            seed_exposure_from_census(&v, &found, &class_of());
            assert_eq!(v.open_ct(REL), 148.0, "the contracts are real either way");
        }
        let _ = std::fs::remove_dir_all(&d);
    }

    /// An obligation on a market this run does not quote has no relationship id
    /// here at all — it is outside `--rel-prefix`. `seed_exposure_from_ledger`'s
    /// rule applies: what cannot be classified is booked under `unknown`, which
    /// counts toward the GLOBAL cap without inflating a class cap it may not
    /// belong to.
    #[test]
    fn an_obligation_outside_this_run_still_counts_toward_the_global_cap() {
        let d = dir("outside-run");
        // 450 against the $490 global cap (980 x 0.50) leaves room for 40.
        let found = orphan::undischarged(&mint(450), vec![]);
        let v = view(&d);
        seed_exposure_from_census(&v, &found, &HashMap::new());
        assert_eq!(v.open_ct(&format!("unknown:{KMKT}")), 450.0);

        let dec = v.check(&rel(), Venue::Kalshi, 50, None);
        assert!(!dec.allowed, "450 + 50 > 490");
        assert!(dec.reasons.iter().any(|r| r.contains("global cap")), "{:?}", dec.reasons);
        assert!(
            !dec.reasons.iter().any(|r| r.contains("class cap")),
            "an unclassifiable id must not inflate a class cap it may not belong to: {:?}",
            dec.reasons
        );
        let _ = std::fs::remove_dir_all(&d);
    }
}
