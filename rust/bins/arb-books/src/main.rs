//! arb-books — build the double-entry journal from venue truth and report.
//!
//! Reads Kalshi history previously fetched to JSON (the venue snapshots are
//! account data and are deliberately NOT committed; see the public-repo rule).
//! Prints the trial balance, every account, and the reconciliation of
//! `cash:kalshi` against the venue's reported balance.
//!
//!   arb-books --kalshi-dir <dir> [--kalshi-balance <dollars>] [--out <jsonl>]
//!
//! Expects <dir>/kalshi_{deposits,fills,settlements}.json.

use std::process::exit;

use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{short_contracts, Balances, PmusImport, Position};
use arb_ledger::{accounts, is_zero, Journal};

fn read_one<T: serde::de::DeserializeOwned>(path: &str) -> T {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            exit(2);
        }
    };
    match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("cannot parse {path}: {e}");
            exit(2);
        }
    }
}

fn read<T: serde::de::DeserializeOwned>(path: &str) -> Vec<T> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => {
            eprintln!("cannot read {path}: {e}");
            exit(2);
        }
    };
    match serde_json::from_str(&text) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("cannot parse {path}: {e}");
            exit(2);
        }
    }
}

fn main() {
    let mut dir = String::new();
    let mut pmus_dir = String::new();
    let mut pmus_deposits = String::new();
    let mut venue_balance: Option<String> = None;
    let mut out: Option<String> = None;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--kalshi-dir" => {
                i += 1;
                dir = args.get(i).cloned().unwrap_or_default();
            }
            "--kalshi-balance" => {
                i += 1;
                venue_balance = args.get(i).cloned();
            }
            "--pmus-dir" => {
                i += 1;
                pmus_dir = args.get(i).cloned().unwrap_or_default();
            }
            "--pmus-deposits" => {
                i += 1;
                pmus_deposits = args.get(i).cloned().unwrap_or_default();
            }
            "--out" => {
                i += 1;
                out = args.get(i).cloned();
            }
            other => {
                eprintln!("unknown arg: {other}");
                exit(2);
            }
        }
        i += 1;
    }
    if dir.is_empty() {
        eprintln!("--kalshi-dir is required");
        exit(2);
    }

    let deposits: Vec<Deposit> = read(&format!("{dir}/kalshi_deposits.json"));
    let fills: Vec<Fill> = read(&format!("{dir}/kalshi_fills.json"));
    let settlements: Vec<Settlement> = read(&format!("{dir}/kalshi_settlements.json"));
    println!(
        "loaded kalshi: {} deposits, {} fills, {} settlements",
        deposits.len(),
        fills.len(),
        settlements.len()
    );

    let mut cx = Cx::default();
    let mut j = Journal::new();
    if let Err(e) = (KalshiImport { deposits, fills, settlements }).apply(&mut cx, &mut j) {
        eprintln!("KALSHI IMPORT FAILED: {e}");
        exit(1);
    }

    if !pmus_dir.is_empty() {
        if pmus_deposits.is_empty() {
            eprintln!("--pmus-deposits is required with --pmus-dir (PM-US has no deposits endpoint)");
            exit(2);
        }
        let balances: Balances = read_one(&format!("{pmus_dir}/pmus_balances.json"));
        let positions: Vec<Position> = read(&format!("{pmus_dir}/pmus_positions.json"));
        println!("loaded pmus: {} positions", positions.len());

        // Free cross-check that the position set is complete: PM-US withholds
        // $1.00 per short contract, so short size must equal marginRequirement.
        let shorts = short_contracts(&mut cx, &positions);
        let mr = cx.parse_exact(&arb_ledger::pmus::PmusImport {
            deposits_usd: pmus_deposits.clone(),
            balances: read_one(&format!("{pmus_dir}/pmus_balances.json")),
            positions: vec![],
        }
        .margin_requirement());
        let d = cx.sub(shorts, mr);
        println!(
            "  short contracts {shorts} vs marginRequirement {mr} -> {}",
            if is_zero(&mut cx, d) { "MATCH".to_string() } else { format!("DIFF {d}") }
        );

        if let Err(e) = (PmusImport { deposits_usd: pmus_deposits, balances, positions })
            .apply(&mut cx, &mut j)
        {
            eprintln!("PMUS IMPORT FAILED: {e}");
            exit(1);
        }
    }
    println!("entries posted: {}", j.len());

    let tb = j.trial_balance(&mut cx);
    let balanced = is_zero(&mut cx, tb);
    println!("\nTRIAL BALANCE: {tb}  {}", if balanced { "OK" } else { "*** NOT ZERO ***" });

    println!("\n{:<40} {:>16}", "account", "balance");
    println!("{}", "-".repeat(58));
    for (acct, bal) in j.balances(&mut cx) {
        if is_zero(&mut cx, bal) {
            continue;
        }
        println!("{acct:<40} {bal:>16}");
    }

    let qtys = j.quantities(&mut cx);
    println!("\nopen positions: {}", qtys.len());
    for (acct, q) in &qtys {
        println!("  {acct:<46} {q:>10}");
    }

    // Statement of position, at COST basis. Marks are deliberately excluded:
    // the hard book (cash + cost) is the part that reconciles to the venue.
    let bals = j.balances(&mut cx);
    let mut cash = cx.zero();
    let mut positions = cx.zero();
    let mut fees = cx.zero();
    let mut realized = cx.zero();
    let mut capital = cx.zero();
    for (acct, bal) in &bals {
        if acct.starts_with("cash:") {
            cash = cx.add(cash, *bal);
        } else if acct.starts_with("pos:") {
            positions = cx.add(positions, *bal);
        } else if acct.starts_with("expense:fees:") {
            fees = cx.add(fees, *bal);
        } else if acct.starts_with("pnl:") {
            realized = cx.add(realized, *bal);
        } else if acct == accounts::EQUITY_CAPITAL {
            capital = cx.add(capital, *bal);
        }
    }
    let capital_in = arb_ledger::neg(&mut cx, capital);
    let assets = cx.add(cash, positions);
    let net = cx.sub(assets, capital_in);
    // realized is a debit balance when it is a LOSS; flip for display.
    let trading = arb_ledger::neg(&mut cx, realized);

    println!("\nSTATEMENT OF POSITION (cost basis)");
    println!("  capital in            {capital_in:>14}");
    println!("  cash                  {cash:>14}");
    println!("  positions at cost     {positions:>14}");
    println!("  total assets          {assets:>14}");
    println!("  ------------------------------------");
    println!("  trading (realized)    {trading:>14}");
    println!("  fees                  {fees:>14}");
    println!("  NET vs capital        {net:>14}");

    if let Some(vb) = venue_balance {
        let want = cx.parse_exact(&vb);
        let got = j.balance(&mut cx, accounts::CASH_KALSHI);
        let diff = cx.sub(got, want);
        println!("\nRECONCILIATION — cash:kalshi vs venue balance");
        println!("  books  {got}");
        println!("  venue  {want}");
        println!("  diff   {diff}  {}", if is_zero(&mut cx, diff) { "EXACT" } else { "*** MISMATCH ***" });
    }

    if let Some(path) = out {
        if let Err(e) = std::fs::write(&path, j.to_jsonl()) {
            eprintln!("cannot write {path}: {e}");
            exit(2);
        }
        println!("\njournal -> {path}");
    }
}
