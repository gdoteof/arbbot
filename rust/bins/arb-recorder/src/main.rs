//! arb-recorder — Rust flight recorder (P1 of the strangler plan).
//!
//! Behavioral transliteration of `python -m arbbot.record.main`: same tape
//! files, same socket line-JSON protocol, same welcome/heal/eviction
//! semantics. Credential posture preserved structurally: this binary
//! contains NO order-placement code path — the only signing is the read-only
//! Kalshi recorder key and the PM-US market-data WS handshake.
//!
//!   arb-recorder --config config/recorder.yaml \
//!       [--data-dir data/raw-rs] [--socket data/arbbot-rs.sock] \
//!       [--health data/health-rs.jsonl] [--shadow]
//!   arb-recorder --parse-check <tape.jsonl>     # byte-identity gate, then exit

mod config;
mod core;
mod health;
mod kalshi;
mod pmintl;
mod pmus;
mod sign;

use crate::core::Core;
use anyhow::{Context, Result};
use arb_core::model::{TapeEvent, Venue};
use arb_tape::broadcast::{Broadcaster, MAX_BUFFER};
use arb_tape::writer::JsonlWriter;
use std::io::BufRead;
use std::sync::Arc;

struct Args {
    config: String,
    data_dir: Option<String>,
    socket: Option<String>,
    health: Option<String>,
    shadow: bool,
    parse_check: Option<String>,
    /// Shadow-safety: force REST polling per venue (never open a WS session
    /// that could contend with the live Python recorder's key sessions).
    /// --poll-only sets both; --kalshi-poll-only / --pmus-poll-only are the
    /// per-venue versions (PM-US has no read-only key, so its WS stays
    /// poll-only in shadows even when Kalshi runs its own rs key).
    kalshi_poll_only: bool,
    pmus_poll_only: bool,
    pmus_poll_interval_s: f64,
    kalshi_poll_interval_s: Option<f64>,
    /// Credential-name infix for the Kalshi recorder key, e.g. "rs" loads
    /// kalshi_recorder_rs_api_key_id / kalshi_recorder_rs_private_key.pem —
    /// the shadow's OWN read-only key, so it never shares a WS session with
    /// the live Python recorder's key.
    kalshi_cred_suffix: Option<String>,
    pmus_cred_suffix: Option<String>,
}

fn parse_args() -> Args {
    let mut a = Args {
        config: "config/recorder.yaml".into(),
        data_dir: None,
        socket: None,
        health: None,
        shadow: false,
        parse_check: None,
        kalshi_poll_only: false,
        pmus_poll_only: false,
        pmus_poll_interval_s: 4.0,
        kalshi_poll_interval_s: None,
        kalshi_cred_suffix: None,
        pmus_cred_suffix: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--config" => a.config = it.next().expect("--config value"),
            "--data-dir" => a.data_dir = it.next(),
            "--socket" => a.socket = it.next(),
            "--health" => a.health = it.next(),
            "--shadow" => a.shadow = true,
            "--poll-only" => {
                a.kalshi_poll_only = true;
                a.pmus_poll_only = true;
            }
            "--kalshi-poll-only" => a.kalshi_poll_only = true,
            "--pmus-poll-only" => a.pmus_poll_only = true,
            "--pmus-poll-interval" => {
                a.pmus_poll_interval_s =
                    it.next().expect("--pmus-poll-interval value").parse().expect("float")
            }
            "--kalshi-poll-interval" => {
                a.kalshi_poll_interval_s =
                    Some(it.next().expect("--kalshi-poll-interval value").parse().expect("float"))
            }
            "--kalshi-cred-suffix" => a.kalshi_cred_suffix = it.next(),
            "--pmus-cred-suffix" => a.pmus_cred_suffix = it.next(),
            "--parse-check" => a.parse_check = it.next(),
            other => {
                eprintln!("unknown arg: {other}");
                std::process::exit(2);
            }
        }
    }
    a
}

/// The offline gate: every line of an existing tape must deserialize into
/// TapeEvent and re-serialize BYTE-IDENTICALLY. Proves the Rust models are
/// wire-exact over real data before any live shadow run.
fn parse_check(path: &str) -> Result<()> {
    let f = std::fs::File::open(path).with_context(|| format!("open {path}"))?;
    let reader = std::io::BufReader::new(f);
    let (mut n, mut mismatches) = (0u64, 0u64);
    let mut lines_iter = reader.lines().peekable();
    while let Some(line) = lines_iter.next() {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        n += 1;
        let ev: TapeEvent = match serde_json::from_str(&line) {
            Ok(ev) => ev,
            Err(e) => {
                if lines_iter.peek().is_none() {
                    println!("note: torn final line tolerated ({e})");
                    n -= 1;
                    continue;
                }
                anyhow::bail!("line {n}: parse failed: {e}\n{line}");
            }
        };
        let out = ev.to_json_line();
        if out != line {
            mismatches += 1;
            if mismatches <= 5 {
                println!("MISMATCH line {n}:\n  in : {line}\n  out: {out}");
            }
        }
    }
    println!(
        "{{\"file\":\"{path}\",\"lines\":{n},\"mismatches\":{mismatches},\"result\":\"{}\"}}",
        if mismatches == 0 { "PASS" } else { "FAIL" }
    );
    if mismatches > 0 {
        std::process::exit(1);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = parse_args();
    if let Some(path) = &args.parse_check {
        return parse_check(path);
    }

    let mut cfg = config::load_recorder_config(&args.config)?;
    if let Some(d) = args.data_dir {
        cfg.data_dir = d;
    }
    if let Some(s) = args.socket {
        cfg.socket_path = s;
    }
    if let Some(h) = args.health {
        cfg.health_path = h;
    }
    if let Some(k) = args.kalshi_poll_interval_s {
        cfg.kalshi_poll_interval_s = k;
    }
    let ntfy = if args.shadow || cfg.ntfy_topic.is_empty() {
        None
    } else {
        Some(cfg.ntfy_topic.clone())
    };

    let universe = config::load_universe(&cfg.registry_path)?;
    println!(
        "universe: {} kalshi tickers, {} pm tokens (registry {})",
        universe.kalshi_tickers.len(),
        universe.pm_tokens.len(),
        cfg.registry_path
    );

    let writer = JsonlWriter::new(&cfg.data_dir)?;
    let broadcaster = Broadcaster::new(MAX_BUFFER);
    let core = Arc::new(Core::new(writer, broadcaster.clone()));
    let liveness = Arc::new(health::Liveness::new(10.0));

    // socket server with welcome snapshots
    {
        let core_w = core.clone();
        let b = broadcaster.clone();
        let sock = cfg.socket_path.clone();
        tokio::spawn(async move {
            let welcome = move |register: &mut dyn FnMut(Vec<String>)| {
                core_w.with_snapshot_lines(register)
            };
            if let Err(e) = b.serve(&sock, welcome).await {
                eprintln!("[recorder] socket server died: {e}");
            }
        });
    }

    let kalshi_catalog = Arc::new(kalshi::KalshiCatalog::new());
    let pmus_catalog = Arc::new(pmus::PmusCatalog::new());
    let clob = Arc::new(pmintl::ClobRest::new());

    // PM-US universe from tags (credential-free public catalog)
    let mut pmus_slugs: Vec<String> = Vec::new();
    if !cfg.polymarket_us_tags.is_empty() {
        match pmus_catalog.markets_by_tags(&cfg.polymarket_us_tags).await {
            Ok(markets) => {
                pmus_slugs =
                    markets.iter().filter(|m| !m.closed).map(|m| m.slug.clone()).collect();
            }
            Err(e) => eprintln!("[recorder] pmus catalog failed at startup: {e:#}"),
        }
    }

    let mut tasks = Vec::new();

    // Kalshi: WS with the READ-ONLY recorder key, else REST poll
    let infix = args
        .kalshi_cred_suffix
        .as_ref()
        .map(|s| format!("_{s}"))
        .unwrap_or_default();
    let rec_key_id = if args.kalshi_poll_only {
        None
    } else {
        config::load_credential_str(&format!("kalshi_recorder{infix}_api_key_id"))
    };
    let rec_key_pem = if args.kalshi_poll_only {
        None
    } else {
        config::load_credential(&format!("kalshi_recorder{infix}_private_key.pem"))
    };
    match (rec_key_id, rec_key_pem) {
        (Some(kid), Some(pem)) => {
            let signer = sign::KalshiSigner::new(kid, &pem)?;
            println!("kalshi: real-time WS (read-only key)");
            tasks.push(tokio::spawn(kalshi::ws_task(
                core.clone(),
                liveness.clone(),
                universe.kalshi_tickers.clone(),
                signer,
                kalshi_catalog.clone(),
            )));
        }
        _ => {
            println!("kalshi: REST polling (no recorder credentials)");
            tasks.push(tokio::spawn(kalshi::poll_task(
                core.clone(),
                liveness.clone(),
                universe.kalshi_tickers.clone(),
                kalshi_catalog.clone(),
                cfg.kalshi_poll_interval_s,
            )));
        }
    }

    // Polymarket intl: public WS
    if !universe.pm_tokens.is_empty() {
        tasks.push(tokio::spawn(pmintl::ws_task(
            core.clone(),
            liveness.clone(),
            universe.pm_tokens.clone(),
            clob.clone(),
        )));
    }

    // Polymarket US: authed WS when the key exists, else REST poll.
    //
    // The suffix mirrors --kalshi-cred-suffix so a SHADOW can run on its own
    // key. Without it the shadow opens a second WS session on the very key the
    // live recorder holds, which is the contention the unit header warns about.
    //
    // NOTE: PM-US has no read-only key tier — any key is trade-capable. That is
    // acceptable here only because this binary has NO order code path: the only
    // endpoints it can reach are the Kalshi/PM market-data hosts and ntfy, and
    // it references no order route at all.
    let pm_infix = args
        .pmus_cred_suffix
        .as_ref()
        .map(|s| format!("_{s}"))
        .unwrap_or_default();
    if !pmus_slugs.is_empty() {
        let us_id = if args.pmus_poll_only {
            None
        } else {
            config::load_credential_str(&format!("polymarket_usa{pm_infix}_key_id"))
        };
        let us_key = if args.pmus_poll_only {
            None
        } else {
            config::load_credential_str(&format!("polymarket_usa{pm_infix}_private_key"))
        };
        match (us_id, us_key) {
            (Some(kid), Some(key)) => {
                let signer = sign::PmusSigner::new(kid, &key)?;
                println!(
                    "polymarket_us: real-time WS {} markets (tags={:?})",
                    pmus_slugs.len(),
                    cfg.polymarket_us_tags
                );
                tasks.push(tokio::spawn(pmus::ws_task(
                    core.clone(),
                    liveness.clone(),
                    pmus_slugs.clone(),
                    signer,
                )));
            }
            _ => {
                println!(
                    "polymarket_us: REST polling {} markets (tags={:?})",
                    pmus_slugs.len(),
                    cfg.polymarket_us_tags
                );
                tasks.push(tokio::spawn(pmus::poll_task(
                    core.clone(),
                    liveness.clone(),
                    pmus_slugs.clone(),
                    pmus_catalog.clone(),
                    args.pmus_poll_interval_s,
                )));
            }
        }
    }

    // health + 30s rebroadcast heal
    tasks.push(tokio::spawn(health::health_task(
        liveness.clone(),
        core.clone(),
        cfg.health_path.clone(),
        ntfy,
        30.0,
    )));

    // universe maintenance: evict closed/resolved books so the 30s
    // rebroadcast stops re-feeding frozen books (2026-07-20 review)
    {
        let core = core.clone();
        let kalshi_catalog = kalshi_catalog.clone();
        let clob = clob.clone();
        let kalshi_tickers = universe.kalshi_tickers.clone();
        let pm_tokens = universe.pm_tokens.clone();
        tasks.push(tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_secs(1800)).await;
                if let Ok(statuses) = kalshi_catalog.statuses(&kalshi_tickers).await {
                    for (ticker, status) in statuses {
                        // Symmetric, and the symmetry is the point. This is a
                        // WHITELIST of live spellings, so anything the venue
                        // invents evicts — which is the right way to fail, but
                        // eviction now also removes the market from the 300s
                        // integrity sweep. Kalshi's vocabulary is wider than
                        // the terminal states (`inactive` appears in this
                        // repo's captured catalog on markets whose close time
                        // is years away), and the whitelist needing BOTH
                        // "active" and "open" is evidence it has drifted
                        // before. One-way, a single poll returning a spelling
                        // this list has not learned would cost that market its
                        // heal cadence for the life of the process. The else
                        // branch costs one line and bounds that at one cycle.
                        if matches!(status.as_str(), "active" | "open" | "") {
                            core.restore_book(Venue::Kalshi, &ticker);
                        } else {
                            core.evict_book(Venue::Kalshi, &ticker, &format!("status {status:?}"));
                        }
                    }
                }
                // No symmetry available here: `closed_tokens` reports only the
                // closed ones, so a token's absence is not a statement that it
                // is live. Polymarket eviction stays one-way — and pmintl's
                // sweep does not consult it yet either.
                if let Ok(closed) = clob.closed_tokens(&pm_tokens).await {
                    for tok in closed {
                        core.evict_book(Venue::Polymarket, &tok, "gamma: closed");
                    }
                }
            }
        }));
    }

    println!(
        "arb-recorder up: data_dir={} socket={} health={}{}",
        cfg.data_dir,
        cfg.socket_path,
        cfg.health_path,
        if args.shadow { " [SHADOW]" } else { "" }
    );

    // run until killed; heartbeat with gap counter for the shadow gate
    let core_hb = core.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
            println!("[hb] gaps={} subscribers={}", core_hb.gap_count(),
                     core_hb.broadcaster.subscriber_count());
        }
    });
    for t in tasks {
        let _ = t.await;
    }
    Ok(())
}
