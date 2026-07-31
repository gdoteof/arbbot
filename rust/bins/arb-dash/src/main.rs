//! arb-dash — instrument over the double-entry books.
//!
//! Binds 127.0.0.1 only and never touches a venue order path: it holds no
//! credentials and cannot place, cancel or move an order.
//!
//! It is read-only with ONE exception: `POST /api/rollup` rebuilds the local
//! ToB series. That writes files under the rollup dir and burns ~30s of two
//! cores, so it is deliberately POST-only (a prefetch or crawler cannot fire
//! it), single-flight (one build at a time), and atomic (temp file + rename,
//! so a reader never sees a half-built series). Deliberately runs on a DIFFERENT port from the Python
//! dash (4748) so both can be open side by side while the numbers are compared.
//!
//! No HTTP crate: the workspace's only dependencies are serde/serde_json, and a
//! localhost read-only instrument does not justify pulling in a runtime.
//!
//! Updates are PUSHED, not polled: `GET /api/stream` is a Server-Sent Events
//! connection that emits one line whenever the files a view reads actually
//! change. An idle dashboard therefore makes zero requests, and a change shows
//! up in about a second instead of on the next timer tick. SSE rather than a
//! WebSocket because the traffic is one-directional and SSE is plain HTTP —
//! no handshake, no framing, no dependency, and the browser reconnects on its
//! own. That connection is long-lived, so the accept loop serves each
//! connection on its own thread; serially, one stream would wedge the server.
//!
//!   arb-dash --kalshi-dir <dir> --pmus-dir <dir> --pmus-deposits <usd> \
//!            [--data-dir data] [--port 4749] [--kalshi-balance <usd>]
//!
//! Layout: `http` is the socket and the router, `stream` the two SSE channels,
//! `rollup` the one write surface, `endpoints` the JSON behind each view and
//! `series` its charts. `integrity` and `trades` are views that were already
//! big enough to own a file before the rest of this was split up.

mod architecture;
mod endpoints;
mod http;
mod integrity;
mod rollup;
mod series;
mod stream;
mod trades;

use std::process::exit;

use arb_core::clock::now_secs;

/// Polymarket INTERNATIONAL charges a taker fee that depends on the market's
/// category: 0.00 geopolitics, 0.04 politics/finance/tech, 0.05
/// sports/economics/culture/weather, 0.07 crypto. The scanner reads that
/// category off the market metadata; this dashboard does not have it, and was
/// hardcoding "politics" — which is not a neutral guess. On the 43 registry
/// pairs with an intl leg it OVERSTATES edge on the crypto ones (btc-1m,
/// btc-dip-10k) and the sports/weather ones (f1, earthquake, volcano, meteor),
/// and UNDERSTATES it on the geopolitics ones (taiwan, iran, nato, putin).
///
/// Priced at the documented default instead, which is the same 0.04 but says
/// what it is, and rows with an intl leg are flagged so the number is read
/// with that caveat. The real fix is plumbing per-market category through to
/// the dashboard.
const FEE_CATEGORY: &str = "default";

/// The three tapes everything here is built from. Every view that names a file
/// per venue names these, in this order.
const VENUES: [&str; 3] = ["kalshi", "polymarket", "polymarket_us"];

pub struct Args {
    kalshi_dir: String,
    pmus_dir: String,
    pmus_deposits: String,
    kalshi_balance: Option<String>,
    data_dir: String,
    scan_dir: String,
    raw_dir: String,
    parquet_dir: String,
    rollup_dir: String,
    intents_path: String,
    /// Append-only trade ledger. The Trades view derives every number from
    /// this file and stores nothing, so it cannot disagree with what the
    /// engine booked.
    ledger_path: String,
    registry: String,
    tradable: String,
    /// The capital caps and per-topic budgets the ENGINE reads. Same files,
    /// same defaults as `arb-trader`'s own `--exec-config` / `--topics-config`,
    /// so the Now view cannot describe a budget the engine is not using.
    exec_config: String,
    topics_config: String,
    port: u16,
    /// Epoch seconds at startup — the age of anything passed on the command
    /// line rather than read from a file.
    started_at: u64,
}

#[cfg(test)]
impl Args {
    /// An `Args` whose every path points at nothing, for the endpoint builders
    /// that parse a URL or fold a value in memory. A path that does not exist
    /// is deliberate: a test that silently picked up this machine's real
    /// `data/` would pass here and nowhere else.
    fn for_test() -> Args {
        Args {
            kalshi_dir: "/nonexistent/kalshi".into(),
            pmus_dir: String::new(),
            pmus_deposits: "0".into(),
            kalshi_balance: None,
            data_dir: "/nonexistent/data".into(),
            scan_dir: "/nonexistent/data/scan".into(),
            raw_dir: "/nonexistent/data/raw".into(),
            parquet_dir: "/nonexistent/data/parquet".into(),
            rollup_dir: "/nonexistent/data/rollup".into(),
            intents_path: "/nonexistent/data/intents.jsonl".into(),
            ledger_path: "/nonexistent/data/trades.jsonl".into(),
            registry: "/nonexistent/config/registry.yaml".into(),
            tradable: "/nonexistent/config/tradable.yaml".into(),
            exec_config: "/nonexistent/config/exec.yaml".into(),
            topics_config: "/nonexistent/config/topics.yaml".into(),
            port: 0,
            started_at: 0,
        }
    }
}

fn main() {
    let mut a = Args {
        kalshi_dir: String::new(),
        pmus_dir: String::new(),
        pmus_deposits: String::new(),
        kalshi_balance: None,
        data_dir: "data".into(),
        scan_dir: "data/scan".into(),
        raw_dir: "data/raw".into(),
        parquet_dir: "data/parquet".into(),
        rollup_dir: "data/rollup".into(),
        intents_path: "data/trader-rs/intents.jsonl".into(),
        ledger_path: "data/exec/trades.jsonl".into(),
        registry: "config/registry.yaml".into(),
        tradable: "config/tradable.yaml".into(),
        exec_config: "config/exec.yaml".into(),
        topics_config: "config/topics.yaml".into(),
        port: 4749,
        started_at: now_secs(),
    };
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--kalshi-dir" => a.kalshi_dir = v,
            "--pmus-dir" => a.pmus_dir = v,
            "--pmus-deposits" => a.pmus_deposits = v,
            "--kalshi-balance" => a.kalshi_balance = Some(v),
            "--data-dir" => a.data_dir = v,
            "--scan-dir" => a.scan_dir = v,
            "--raw-dir" => a.raw_dir = v,
            "--parquet-dir" => a.parquet_dir = v,
            "--rollup-dir" => a.rollup_dir = v,
            "--intents" => a.intents_path = v,
            "--ledger" => a.ledger_path = v,
            "--registry" => a.registry = v,
            "--exec-config" => a.exec_config = v,
            "--topics-config" => a.topics_config = v,
            "--tradable" => a.tradable = v,
            "--port" => a.port = v.parse().unwrap_or(4749),
            other => {
                eprintln!("unknown arg: {other}");
                exit(2);
            }
        }
        i += 2;
    }
    if a.kalshi_dir.is_empty() {
        eprintln!("--kalshi-dir is required");
        exit(2);
    }

    http::serve(a);
}
