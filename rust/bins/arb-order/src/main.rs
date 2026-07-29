//! arb-order — the FIRST Rust binary that can touch an order.
//! docs/migration-plan.md M2.
//!
//! It does exactly one thing, on purpose: place ONE 1-contract post-only YES
//! bid at 1c (far below any real bid), GET it back to prove it RESTED, cancel
//! it, verify the cancel, exit. It is deliberately boring. Its only job is to
//! make the first venue-write a non-event — it exercises signing, the endpoint
//! paths, order-id extraction, the cancel quirks and the tick format against
//! the live venue with ~zero standing risk.
//!
//!   arb-order --market KXSOMETICKER                    # DRY: prints the plan
//!   arb-order --market KXSOMETICKER --live             # places and cancels
//!   arb-order --venue pmus --market some-slug --live   # PM-US (market = slug)
//!   arb-order --venue pmus --preflight-only            # signed READ, no write
//!
//! SAFETY
//!   * Dry by default. Reaching the venue requires typing --live.
//!   * data/KILL halts it, same file the engine honors.
//!   * 1 contract at 1c. Worst case is a $0.01 fill to flatten by hand.
//!   * post-only: it can only ever rest. A crossing post-only is rejected and
//!     surfaced, never retried as a taker.
//!   * Every exit path after a successful place cancels the order.
//!   * It uses the TRADE-CAPABLE keys. That is the point of the exercise, and
//!     it is why nothing else in the Rust stack links arb-venue.

use arb_venue::gateway::{KalshiGateway, PmusGateway, VenueGateway};
use arb_venue::ratelimit::RateLimiter;
use arb_venue::transport::HttpTransport;
use arb_venue::{KalshiSigner, PmusSigner};
use std::path::PathBuf;

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";
const PMUS_REST: &str = "https://api.polymarket.us";

#[derive(PartialEq, Clone, Copy)]
enum Venue {
    Kalshi,
    Pmus,
}

impl Venue {
    fn default_base(self) -> &'static str {
        match self {
            Venue::Kalshi => KALSHI_REST,
            Venue::Pmus => PMUS_REST,
        }
    }
    fn name(self) -> &'static str {
        match self {
            Venue::Kalshi => "kalshi",
            Venue::Pmus => "polymarket_us",
        }
    }
    /// Credential file names for an optional suffix. Base name == the
    /// trade-capable key on both venues.
    fn creds(self, suffix: Option<&str>) -> (String, String) {
        match self {
            Venue::Kalshi => match suffix {
                Some(s) => (format!("kalshi_{s}_api_key_id"), format!("kalshi_{s}_private_key.pem")),
                None => ("kalshi_api_key_id".into(), "kalshi_private_key.pem".into()),
            },
            Venue::Pmus => {
                let infix = suffix.map(|s| format!("_{s}")).unwrap_or_default();
                (
                    format!("polymarket_usa{infix}_key_id"),
                    format!("polymarket_usa{infix}_private_key"),
                )
            }
        }
    }
}

struct Args {
    venue: Venue,
    market: String,
    live: bool,
    preflight_only: bool,
    verify_all: bool,
    cred_suffix: Option<String>,
    kill_file: String,
    base: Option<String>,
}

fn parse_args() -> Result<Args, String> {
    let mut a = Args {
        venue: Venue::Kalshi,
        market: String::new(),
        live: false,
        preflight_only: false,
        verify_all: false,
        cred_suffix: None,
        kill_file: "data/KILL".into(),
        base: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--venue" => {
                a.venue = match it.next().as_deref() {
                    Some("kalshi") => Venue::Kalshi,
                    Some("pmus") | Some("polymarket_us") => Venue::Pmus,
                    other => return Err(format!("--venue must be kalshi|pmus, got {other:?}")),
                }
            }
            "--market" => a.market = it.next().ok_or("--market needs a ticker/slug")?,
            "--live" => a.live = true,
            // Signed READ only: proves signing/auth against the live venue
            // with zero write.
            "--preflight-only" => {
                a.live = true;
                a.preflight_only = true;
            }
            // Exercise the FULL order surface, including the kill-switch
            // sweep, instead of just the rehearsal.
            "--verify-all" => {
                a.live = true;
                a.verify_all = true;
            }
            "--cred-suffix" => a.cred_suffix = it.next(),
            "--kill-file" => a.kill_file = it.next().ok_or("--kill-file needs a path")?,
            "--base" => a.base = it.next(),
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

fn load_credential(name: &str) -> Result<String, String> {
    let dir = credentials_dir().ok_or("no credentials dir (set ARBBOT_CREDENTIALS_DIR)")?;
    std::fs::read(dir.join(name))
        .map_err(|e| format!("missing credential {name}: {e}"))
        .and_then(|b| {
            let s = String::from_utf8_lossy(&b).trim().to_string();
            if s.is_empty() { Err(format!("credential {name} is empty")) } else { Ok(s) }
        })
}

fn die(code: i32, msg: impl std::fmt::Display) -> ! {
    eprintln!("arb-order: {msg}");
    std::process::exit(code)
}

fn main() {
    let args = match parse_args() {
        Ok(a) => a,
        Err(e) => {
            eprintln!("arb-order: {e}");
            eprintln!("usage: arb-order [--venue kalshi|pmus] --market <TICKER|SLUG> [--live]");
            eprintln!("       arb-order [--venue ...] --preflight-only    (signed read, no write)");
            std::process::exit(2);
        }
    };

    // Same kill file the engine honors. Checked before anything else.
    if std::path::Path::new(&args.kill_file).exists() {
        die(3, format_args!("KILL file present ({}) — refusing to run", args.kill_file));
    }

    let (id_name, key_name) = args.venue.creds(args.cred_suffix.as_deref());
    let base = args.base.clone().unwrap_or_else(|| args.venue.default_base().to_string());

    if args.preflight_only {
        println!("mode        : PREFLIGHT ONLY — signed read, no order will be placed");
    }
    println!("venue       : {}", args.venue.name());
    println!(
        "market      : {}",
        if args.market.is_empty() { "(none — preflight)" } else { &args.market }
    );
    println!("credentials : {id_name}");
    println!("endpoint    : {base}");
    if args.preflight_only {
        println!("plan        : a signed READ of balances — nothing else");
    } else if args.verify_all {
        println!("plan        : FULL SURFACE — balances, positions, place 1 contract,");
        println!("              order status, then cancel_all_open (the kill switch),");
        println!("              then prove the order is no longer resting");
    } else {
        println!("plan        : place 1 contract, YES bid @ 0.0100, post-only");
        println!("              GET it back, require it to be resting and unfilled");
        println!("              cancel it, and cancel it even if a step above fails");
    }

    if !args.live {
        println!();
        println!("DRY RUN — nothing was sent. Re-run with --live to actually place.");
        return;
    }

    let transport = || match HttpTransport::new(&base, 15) {
        Ok(t) => t,
        Err(e) => die(5, e),
    };
    let limiter = || RateLimiter::from_per_minute(60.0, 60.0, 0);

    println!();
    match args.venue {
        Venue::Kalshi => {
            let key_id = load_credential(&id_name).unwrap_or_else(|e| die(4, e));
            let pem = load_credential(&key_name).unwrap_or_else(|e| die(4, e));
            let signer =
                KalshiSigner::from_pkcs8_pem(key_id, &pem).unwrap_or_else(|e| die(4, e));
            let gw = KalshiGateway::with_transport(signer, limiter(), transport());
            match gw.balances() {
                Ok(b) => println!("[preflight] auth OK — balance ${}", b.balance_dollars),
                Err(e) => {
                    eprintln!("[preflight] FAILED: {e}");
                    die(6, "not attempting a write");
                }
            }
            if args.preflight_only {
                return preflight_done();
            }
            if args.verify_all {
                return finish_verify(verify_all(&gw, &args.market, "kalshi"));
            }
            finish(gw.rehearse(&args.market));
        }
        Venue::Pmus => {
            let key_id = load_credential(&id_name).unwrap_or_else(|e| die(4, e));
            let secret = load_credential(&key_name).unwrap_or_else(|e| die(4, e));
            let signer =
                PmusSigner::from_secret_b64(key_id, &secret).unwrap_or_else(|e| die(4, e));
            let gw = PmusGateway::with_transport(signer, limiter(), transport());
            match gw.balances() {
                Ok(b) => match b.balances.first() {
                    Some(first) => {
                        println!("[preflight] auth OK — buying power ${}", first.buying_power)
                    }
                    None => println!("[preflight] auth OK — no balance rows returned"),
                },
                Err(e) => {
                    eprintln!("[preflight] FAILED: {e}");
                    die(6, "not attempting a write");
                }
            }
            if args.preflight_only {
                return preflight_done();
            }
            if args.verify_all {
                return finish_verify(verify_all(&gw, &args.market, "polymarket_us"));
            }
            finish(gw.rehearse(&args.market));
        }
    }
}

fn finish_verify(res: Result<(), String>) {
    match res {
        Ok(()) => {
            println!();
            println!("PASS — place, status, cancel and the kill-switch sweep all work live.");
        }
        Err(e) => {
            eprintln!("[verify]    FAILED: {e}");
            std::process::exit(8);
        }
    }
}

/// Exercise the WHOLE order surface against the live venue, not just the
/// rehearsal: reads, then place -> status -> **cancel_all_open** -> confirm
/// gone. The sweep is the kill switch, and until it has actually cancelled a
/// real resting order it is only a hypothesis.
fn verify_all<G: VenueGateway>(gw: &G, market: &str, venue: &str) -> Result<(), String> {
    use arb_venue::gateway::{PlaceRequest, Side, Tif};

    println!("[verify]    positions...");
    gw.positions().map_err(|e| format!("positions: {e}"))?;
    println!("[verify]    positions OK");

    println!("[verify]    placing 1 contract @ 0.0100 post-only...");
    let placed = gw
        .place(&PlaceRequest {
            market: market.to_string(),
            side: Side::Bid,
            price: "0.0100".into(),
            qty: 1,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: format!("sweep-{}", std::process::id()),
        })
        .map_err(|e| format!("place: {e}"))?;
    let oid = G::order_id(&placed);
    println!("[verify]    placed {oid}");

    // From here an order EXISTS. Any early return must still sweep, so the
    // result is captured rather than propagated with `?`.
    let checked = (|| -> Result<(), String> {
        for _ in 0..8 {
            match gw.order_status(&oid) {
                Ok(_) => return Ok(()),
                Err(arb_venue::VenueError::Status { status: 404, .. }) => {
                    std::thread::sleep(std::time::Duration::from_millis(500))
                }
                Err(e) => return Err(format!("order_status: {e}")),
            }
        }
        Err(format!("order {oid} never became visible"))
    })();
    if checked.is_ok() {
        println!("[verify]    order_status OK");
    }

    println!("[verify]    cancel_all_open (the kill-switch sweep)...");
    let swept = gw.cancel_all_open().map_err(|e| format!("cancel_all_open: {e}"));
    if swept.is_ok() {
        println!("[verify]    sweep returned OK");
    }

    // Whatever happened above, prove nothing of ours is left resting. A 200
    // from cancel_all_open only says the venue accepted the request; the
    // resting list is the evidence. Both venues' lists lag a write, so poll.
    let gone = (|| -> Result<bool, String> {
        for _ in 0..10 {
            let resting = gw.resting_order_ids().map_err(|e| format!("resting list: {e}"))?;
            if !resting.contains(&oid) {
                return Ok(true);
            }
            std::thread::sleep(std::time::Duration::from_millis(500));
        }
        Ok(false)
    })();

    checked?;
    swept?;
    match gone {
        Ok(true) => {
            println!("[verify]    {venue}: {oid} is no longer resting — the sweep works");
            Ok(())
        }
        Ok(false) => Err(format!(
            "order {oid} SURVIVED cancel_all_open — CHECK THE VENUE AND CANCEL BY HAND"
        )),
        Err(e) => Err(e),
    }
}

fn preflight_done() {
    println!();
    println!("PREFLIGHT PASS — signing and auth work against the live venue.");
    println!("No order was placed.");
}

fn finish(res: Result<String, arb_venue::VenueError>) {
    match res {
        Ok(oid) => {
            println!("[rehearse]  order {oid}: placed, rested, cancelled");
            println!();
            println!("PASS — the Rust order path works against the live venue.");
        }
        Err(e) => {
            eprintln!("[rehearse]  FAILED: {e}");
            eprintln!();
            eprintln!("If the error above does not say the order was cancelled or recovered,");
            eprintln!("verify on the venue and cancel by hand.");
            std::process::exit(7);
        }
    }
}
