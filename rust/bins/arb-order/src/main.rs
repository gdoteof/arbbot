//! arb-order — the FIRST Rust binary that can touch an order.
//! docs/migration-plan.md M2.
//!
//! It does exactly one thing, on purpose: place ONE 1-contract post-only YES
//! bid at 1c (far below any real bid), GET it back to prove it RESTED, cancel
//! it, verify the cancel, exit. It is deliberately boring. Its only job is to
//! make the first venue-write a non-event — it exercises signing, the endpoint
//! paths, order-id extraction, the cancel-404-is-success quirk and the tick
//! format against the live venue with ~zero standing risk.
//!
//!   arb-order --market KXSOMETICKER            # DRY: prints the plan, no network
//!   arb-order --market KXSOMETICKER --live     # actually places and cancels
//!
//! SAFETY
//!   * Dry by default. Reaching the venue requires typing --live.
//!   * data/KILL halts it, same file the engine honors.
//!   * 1 contract at 1c. Worst case is a $0.01 fill to flatten by hand.
//!   * post_only: it can only ever rest. A crossing post_only is a 400 and is
//!     surfaced, never retried as a taker.
//!   * It uses the TRADE-CAPABLE Kalshi key (kalshi_api_key_id /
//!     kalshi_private_key.pem) — NOT the read-only recorder keys. That is the
//!     point of the exercise, and it is why nothing else in the Rust stack
//!     links arb-venue.

use arb_venue::gateway::{KalshiGateway, VenueGateway};
use arb_venue::ratelimit::RateLimiter;
use arb_venue::transport::HttpTransport;
use arb_venue::KalshiSigner;
use std::path::PathBuf;

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";

struct Args {
    market: String,
    live: bool,
    preflight_only: bool,
    cred_suffix: Option<String>,
    kill_file: String,
    base: String,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        market: String::new(),
        live: false,
        preflight_only: false,
        cred_suffix: None,
        kill_file: "data/KILL".into(),
        base: KALSHI_REST.into(),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--market" => a.market = it.next().ok_or("--market needs a ticker")?,
            "--live" => a.live = true,
            // Signed READ only: proves signing/auth against the live venue
            // with zero write. Safe to run with a read-only key.
            "--preflight-only" => {
                a.live = true;
                a.preflight_only = true;
            }
            "--cred-suffix" => a.cred_suffix = it.next(),
            "--kill-file" => a.kill_file = it.next().ok_or("--kill-file needs a path")?,
            "--base" => a.base = it.next().ok_or("--base needs a url")?,
            "-h" | "--help" => return Err("usage".into()),
            other => return Err(format!("unknown argument {other}")),
        }
    }
    if a.market.is_empty() && !a.preflight_only {
        return Err("--market is required".into());
    }
    Ok(a)
}

/// systemd LoadCredential dir or ARBBOT_CREDENTIALS_DIR (mirrors ops/config.py
/// and arb-recorder's config::credentials_dir).
fn credentials_dir() -> Option<PathBuf> {
    std::env::var_os("CREDENTIALS_DIRECTORY")
        .or_else(|| std::env::var_os("ARBBOT_CREDENTIALS_DIR"))
        .map(PathBuf::from)
}

fn load_credential(name: &str) -> Option<String> {
    let dir = credentials_dir()?;
    std::fs::read(dir.join(name))
        .ok()
        .filter(|b| !b.is_empty())
        .map(|b| String::from_utf8_lossy(&b).trim().to_string())
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("arb-order: {e}");
            eprintln!("usage: arb-order --market <TICKER> [--live] [--cred-suffix S]");
            eprintln!("       arb-order --preflight-only [--cred-suffix S]   (signed read, no write)");
            std::process::exit(2);
        }
    };

    // Same kill file the engine honors. Checked before anything else.
    if std::path::Path::new(&args.kill_file).exists() {
        eprintln!("arb-order: KILL file present ({}) — refusing to run", args.kill_file);
        std::process::exit(3);
    }

    // Base name = the TRADE-CAPABLE key. A suffix selects a read-only key,
    // which is useful for proving auth without write rights.
    let (id_name, pem_name) = match &args.cred_suffix {
        Some(s) => (format!("kalshi_{s}_api_key_id"), format!("kalshi_{s}_private_key.pem")),
        None => ("kalshi_api_key_id".to_string(), "kalshi_private_key.pem".to_string()),
    };

    if args.preflight_only {
        println!("mode        : PREFLIGHT ONLY — signed read, no order will be placed");
    }
    println!("market      : {}", if args.market.is_empty() { "(none — preflight)" } else { &args.market });
    println!("credentials : {id_name}");
    println!("endpoint    : {}", args.base);
    if args.preflight_only {
        println!("plan        : GET /portfolio/balance — a signed READ, nothing else");
    } else {
        println!("plan        : place 1 contract, YES bid @ 0.0100, post_only");
        println!("              GET it back, require status=resting");
        println!("              DELETE it (404 counts as already-gone)");
    }

    if !args.live {
        println!();
        println!("DRY RUN — nothing was sent. Re-run with --live to actually place.");
        return;
    }

    let key_id = match load_credential(&id_name) {
        Some(v) => v,
        None => {
            eprintln!("arb-order: missing credential {id_name} (set ARBBOT_CREDENTIALS_DIR)");
            std::process::exit(4);
        }
    };
    let pem = match load_credential(&pem_name) {
        Some(v) => v,
        None => {
            eprintln!("arb-order: missing credential {pem_name}");
            std::process::exit(4);
        }
    };
    let signer = match KalshiSigner::from_pkcs8_pem(key_id, &pem) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("arb-order: bad key material: {e}");
            std::process::exit(4);
        }
    };

    let transport = match HttpTransport::new(&args.base, 15) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("arb-order: {e}");
            std::process::exit(5);
        }
    };
    let gw = KalshiGateway::with_transport(
        signer,
        RateLimiter::from_per_minute(60.0, 60.0, 0),
        transport,
    );

    // Preflight: a signed READ. If this fails the key or the signing is wrong
    // and there is no reason to attempt a write.
    println!();
    match gw.balances() {
        Ok(b) => println!("[preflight] auth OK — balance ${}", b.balance_dollars),
        Err(e) => {
            eprintln!("[preflight] FAILED: {e}");
            eprintln!("arb-order: not attempting a write");
            std::process::exit(6);
        }
    }

    if args.preflight_only {
        println!();
        println!("PREFLIGHT PASS — signing and auth work against the live venue.");
        println!("No order was placed.");
        return;
    }

    match gw.rehearse(&args.market) {
        Ok(oid) => {
            println!("[rehearse]  order {oid}: placed, rested, cancelled");
            println!();
            println!("PASS — the Rust order path works against the live venue.");
        }
        Err(e) => {
            eprintln!("[rehearse]  FAILED: {e}");
            eprintln!();
            eprintln!("An order may still be RESTING. Verify on the venue and cancel by hand.");
            std::process::exit(7);
        }
    }
}
