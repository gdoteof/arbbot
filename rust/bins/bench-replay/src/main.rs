//! Rust half of the P1 recorder-gate benchmark.
//!
//! Replays a recorded tape slice through the same logical stages as
//! scripts/bench_replay.py — line read, typed parse (serde, the TapeEvent
//! shape from the plan), book apply — and prints per-stage µs/event and
//! events/sec for direct comparison against the Python numbers.
//!
//!   cargo run --release -- <tape.jsonl>

#![allow(dead_code)] // seq/ts fields deserialized for realism, not all read

use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::io::{BufRead, BufReader};
use std::time::Instant;

#[derive(Deserialize)]
struct Level<'a> {
    price: &'a str,
    size: &'a str,
}

#[derive(Deserialize)]
#[serde(tag = "kind")]
enum TapeEvent<'a> {
    #[serde(rename = "snapshot")]
    Snapshot {
        venue: &'a str,
        market_id: &'a str,
        #[serde(borrow)]
        bids: Vec<Level<'a>>,
        asks: Vec<Level<'a>>,
        seq: u64,
        ts_local_ns: i64,
    },
    #[serde(rename = "delta")]
    Delta {
        venue: &'a str,
        market_id: &'a str,
        side: &'a str,
        price: &'a str,
        size: &'a str,
        seq: u64,
        ts_local_ns: i64,
    },
    #[serde(rename = "trade")]
    Trade {},
}

/// price/size decimal-string -> micro-units (exact for venue precision)
fn micro(s: &str) -> u64 {
    let (int, frac) = match s.split_once('.') {
        Some((i, f)) => (i, f),
        None => (s, ""),
    };
    let mut v: u64 = int.parse::<u64>().unwrap_or(0) * 1_000_000;
    let mut scale = 100_000u64;
    for c in frac.bytes().take(6) {
        v += (c - b'0') as u64 * scale;
        scale /= 10;
    }
    v
}

#[derive(Default)]
struct Book {
    bids: BTreeMap<u64, u64>,
    asks: BTreeMap<u64, u64>,
    seq: u64,
}

fn main() {
    let path = std::env::args().nth(1).expect("usage: bench-replay <tape.jsonl>");

    let t0 = Instant::now();
    let f = std::fs::File::open(&path).expect("open tape");
    let lines: Vec<String> = BufReader::new(f).lines().map(|l| l.unwrap()).collect();
    let t_read = t0.elapsed();
    let n = lines.len();

    // stage: typed parse
    let t0 = Instant::now();
    let mut parsed = 0u64;
    for ln in &lines {
        if serde_json::from_str::<TapeEvent>(ln).is_ok() {
            parsed += 1;
        }
    }
    let t_parse = t0.elapsed();

    // stage: parse + book apply (parse repeated so stages stay comparable
    // to the Python harness, which also re-walks its stage inputs)
    let mut books: HashMap<(String, String), Book> = HashMap::new();
    let (mut snaps, mut deltas, mut gaps) = (0u64, 0u64, 0u64);
    let t0 = Instant::now();
    for ln in &lines {
        let Ok(ev) = serde_json::from_str::<TapeEvent>(ln) else { continue };
        match ev {
            TapeEvent::Snapshot { venue, market_id, bids, asks, seq, .. } => {
                snaps += 1;
                let b = books
                    .entry((venue.to_owned(), market_id.to_owned()))
                    .or_default();
                b.bids = bids.iter().map(|l| (micro(l.price), micro(l.size))).collect();
                b.asks = asks.iter().map(|l| (micro(l.price), micro(l.size))).collect();
                b.seq = seq;
            }
            TapeEvent::Delta { venue, market_id, side, price, size, seq, .. } => {
                deltas += 1;
                let key = (venue.to_owned(), market_id.to_owned());
                let Some(b) = books.get_mut(&key) else {
                    gaps += 1; // NotSynced
                    continue;
                };
                if seq != b.seq + 1 {
                    gaps += 1; // GapDetected
                    books.remove(&key);
                    continue;
                }
                b.seq = seq;
                let ladder = if side == "bid" { &mut b.bids } else { &mut b.asks };
                let (p, s) = (micro(price), micro(size));
                if s == 0 {
                    ladder.remove(&p);
                } else {
                    ladder.insert(p, s);
                }
            }
            TapeEvent::Trade {} => {}
        }
    }
    let t_apply = t0.elapsed();
    let t_book = t_apply.saturating_sub(t_parse); // book-only ≈ apply - parse

    let us = |d: std::time::Duration| d.as_secs_f64() / n as f64 * 1e6;
    let total = t_parse + t_book;
    println!(
        "{{\"events\":{n},\"parsed\":{parsed},\"snapshots\":{snaps},\"deltas\":{deltas},\
         \"gap_or_notsynced\":{gaps},\
         \"stage_us_per_event\":{{\"readline\":{:.3},\"serde_parse\":{:.3},\"book_apply\":{:.3},\
         \"total_hot_path\":{:.3}}},\"throughput_eps\":{{\"hot_path\":{},\"parse_only\":{}}}}}",
        us(t_read),
        us(t_parse),
        us(t_book),
        us(total),
        (n as f64 / total.as_secs_f64()) as u64,
        (n as f64 / t_parse.as_secs_f64()) as u64,
    );
}
