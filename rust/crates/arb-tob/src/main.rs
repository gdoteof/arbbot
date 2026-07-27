//! arb-tob — build the top-of-book series for a day.
//!
//!   arb-tob --day 2026-07-26 [--raw-dir data/raw] [--parquet-dir data/parquet]
//!           [--out-dir data/rollup] [--interval-s 60] [--venue kalshi]
//!
//! Venues are replayed in PARALLEL: a market's book depends only on its own
//! venue's events, so there is no cross-venue ordering to preserve.

use std::process::exit;

use arb_tob::{build_day, Stats, DEFAULT_INTERVAL_NS};

const VENUES: [&str; 3] = ["kalshi", "polymarket", "polymarket_us"];

fn main() {
    let mut day = String::new();
    let mut raw_dir = "data/raw".to_string();
    let mut parquet_dir = "data/parquet".to_string();
    let mut out_dir = "data/rollup".to_string();
    let mut interval_ns = DEFAULT_INTERVAL_NS;
    let mut only_venue: Option<String> = None;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        let v = args.get(i + 1).cloned().unwrap_or_default();
        match args[i].as_str() {
            "--day" => day = v,
            "--raw-dir" => raw_dir = v,
            "--parquet-dir" => parquet_dir = v,
            "--out-dir" => out_dir = v,
            "--venue" => only_venue = Some(v),
            "--interval-s" => {
                interval_ns = v.parse::<f64>().map(|s| (s * 1e9) as i64).unwrap_or(DEFAULT_INTERVAL_NS)
            }
            other => {
                eprintln!("unknown arg: {other}");
                exit(2);
            }
        }
        i += 2;
    }
    if day.is_empty() {
        eprintln!("--day YYYY-MM-DD is required");
        exit(2);
    }
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        eprintln!("cannot create {out_dir}: {e}");
        exit(2);
    }

    let venues: Vec<&str> = match &only_venue {
        Some(v) => VENUES.iter().copied().filter(|x| x == v).collect(),
        None => VENUES.to_vec(),
    };

    let t0 = std::time::Instant::now();
    let results = match build_day(&raw_dir, &parquet_dir, &out_dir, &day, interval_ns, &venues) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("{e}");
            exit(1);
        }
    };

    let mut total = Stats::default();
    for (venue, st) in &results {
        println!(
            "  {venue:<15} events {:>10}  samples {:>7}  markets {:>5}  gaps {:>5}  presync {:>8}  bad {:>4}",
            st.events, st.samples, st.markets, st.gaps, st.not_synced, st.parse_failures
        );
        total.events += st.events;
        total.samples += st.samples;
        total.markets += st.markets;
        total.gaps += st.gaps;
        total.not_synced += st.not_synced;
        total.parse_failures += st.parse_failures;
    }
    println!(
        "\n  TOTAL           events {:>10}  samples {:>7}  markets {:>5}  gaps {:>5}  presync {:>8}  bad {:>4}   in {:.2}s",
        total.events,
        total.samples,
        total.markets,
        total.gaps,
        total.not_synced,
        total.parse_failures,
        t0.elapsed().as_secs_f64()
    );
}
