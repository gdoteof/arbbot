//! arb-dash — read-only instrument over the double-entry books.
//!
//! Binds 127.0.0.1 only, reads local files, never writes and never touches a
//! venue order path. Deliberately runs on a DIFFERENT port from the Python
//! dash (4748) so both can be open side by side while the numbers are compared.
//!
//! No HTTP crate: the workspace's only dependencies are serde/serde_json, and a
//! localhost read-only instrument does not justify pulling in a runtime.
//!
//!   arb-dash --kalshi-dir <dir> --pmus-dir <dir> --pmus-deposits <usd> \
//!            [--data-dir data] [--port 4749] [--kalshi-balance <usd>]

mod integrity;

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::process::exit;

use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{Balances, PmusImport, Position};
use arb_ledger::{accounts, report, Journal};

const PAGE: &str = include_str!("index.html");

struct Args {
    kalshi_dir: String,
    pmus_dir: String,
    pmus_deposits: String,
    kalshi_balance: Option<String>,
    data_dir: String,
    port: u16,
}

fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Option<T> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}

/// Rebuild the books from venue snapshots on every request. The whole import is
/// ~250 entries and takes microseconds; holding no state means the page can
/// never show a number the files no longer support.
fn books_json(a: &Args) -> String {
    let mut cx = Cx::default();
    let mut j = Journal::new();

    let deposits: Vec<Deposit> =
        read_json(&format!("{}/kalshi_deposits.json", a.kalshi_dir)).unwrap_or_default();
    let fills: Vec<Fill> =
        read_json(&format!("{}/kalshi_fills.json", a.kalshi_dir)).unwrap_or_default();
    let settlements: Vec<Settlement> =
        read_json(&format!("{}/kalshi_settlements.json", a.kalshi_dir)).unwrap_or_default();
    if let Err(e) = (KalshiImport { deposits, fills, settlements }).apply(&mut cx, &mut j) {
        return format!("{{\"error\":\"kalshi import: {e}\"}}");
    }

    let mut pmus_buying_power: Option<String> = None;
    if !a.pmus_dir.is_empty() {
        if let Some(balances) =
            read_json::<Balances>(&format!("{}/pmus_balances.json", a.pmus_dir))
        {
            pmus_buying_power = Some(balances.buying_power_str());
            let positions: Vec<Position> =
                read_json(&format!("{}/pmus_positions.json", a.pmus_dir)).unwrap_or_default();
            if let Err(e) =
                (PmusImport { deposits_usd: a.pmus_deposits.clone(), balances, positions })
                    .apply(&mut cx, &mut j)
            {
                return format!("{{\"error\":\"pmus import: {e}\"}}");
            }
        }
    }

    let mut rep = report::build(&mut cx, &j);
    if let Some(kb) = &a.kalshi_balance {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_KALSHI, kb));
    }
    if let Some(bp) = &pmus_buying_power {
        rep.reconciliations
            .push(report::reconcile(&mut cx, &j, accounts::CASH_PMUS, bp));
    }
    serde_json::to_string(&rep).unwrap_or_else(|_| "{}".into())
}

fn respond(mut s: TcpStream, status: &str, ctype: &str, body: &str) {
    let out = format!(
        "HTTP/1.1 {status}\r\nContent-Type: {ctype}\r\nContent-Length: {}\r\n\
         Cache-Control: no-store\r\nConnection: close\r\n\r\n{body}",
        body.len()
    );
    let _ = s.write_all(out.as_bytes());
    let _ = s.flush();
}

fn handle(s: TcpStream, a: &Args) {
    let mut line = String::new();
    {
        let mut r = BufReader::new(match s.try_clone() {
            Ok(c) => c,
            Err(_) => return,
        });
        if r.read_line(&mut line).is_err() {
            return;
        }
    }
    let path = line.split_whitespace().nth(1).unwrap_or("/");
    let path = path.split('?').next().unwrap_or("/");
    match path {
        "/" => respond(s, "200 OK", "text/html; charset=utf-8", PAGE),
        "/api/books" => respond(s, "200 OK", "application/json", &books_json(a)),
        "/api/integrity" => {
            let i = integrity::build(&a.data_dir);
            let body = serde_json::to_string(&i).unwrap_or_else(|_| "{}".into());
            respond(s, "200 OK", "application/json", &body)
        }
        _ => respond(s, "404 Not Found", "text/plain", "not found"),
    }
}

fn main() {
    let mut a = Args {
        kalshi_dir: String::new(),
        pmus_dir: String::new(),
        pmus_deposits: String::new(),
        kalshi_balance: None,
        data_dir: "data".into(),
        port: 4749,
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

    let addr = format!("127.0.0.1:{}", a.port);
    let l = match TcpListener::bind(&addr) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("cannot bind {addr}: {e}");
            exit(1);
        }
    };
    println!("arb-dash on http://{addr}  (read-only, 127.0.0.1 only)");
    for s in l.incoming().flatten() {
        handle(s, &a);
    }
}
