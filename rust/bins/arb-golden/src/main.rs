//! arb-golden — the P2 parity gate: replay a day's merged tape through the
//! Rust BookBuilder + scanner and emit the canonical decision stream +
//! SHA256, to be compared byte-for-byte against Python's golden_scan.py
//! (pinned digests in tests/golden_digests.json).
//!
//!   arb-golden --tape merged-<day>.jsonl --registry config/registry.yaml \
//!       [--out decisions.jsonl] [--max-events N]
//!
//! Mirrors scripts/golden_scan.py exactly: only snapshot/delta events scan
//! (gap/desync deltas skip the scan; stale-duplicate deltas still scan);
//! pydantic-parity validation = a snapshot with any level price outside
//! [0,1] is skipped as a parse failure.

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::fees::FeeSchedule;
use arb_core::model::{BookSide, Level, Venue};
use arb_core::scan::{scan_relationship, Cx, MarketMeta, Rel, RelLeg, RelType};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io::{BufRead, Write};

#[derive(Deserialize)]
struct RegistryDoc {
    #[serde(default)]
    relationships: Vec<RelDoc>,
}
#[derive(Deserialize)]
struct RelDoc {
    id: String,
    #[serde(rename = "type")]
    rtype: String,
    #[serde(default = "default_verdict")]
    verdict: String,
    #[serde(default = "default_tranche")]
    tranche: String,
    legs: Vec<LegDoc>,
}
fn default_verdict() -> String {
    "rejected".into()
}
fn default_tranche() -> String {
    "long-tail".into()
}
#[derive(Deserialize)]
struct LegDoc {
    venue: String,
    market_id: String,
}

fn load_rels(path: &str) -> Vec<Rel> {
    let text = std::fs::read_to_string(path).expect("read registry");
    let doc: RegistryDoc = serde_yaml::from_str(&text).expect("parse registry");
    doc.relationships
        .into_iter()
        .filter(|r| r.legs.len() == 2 && r.verdict != "rejected")
        .filter_map(|r| {
            Some(Rel {
                id: r.id,
                rtype: RelType::parse(&r.rtype)?,
                tranche: r.tranche,
                legs: r
                    .legs
                    .into_iter()
                    .map(|l| {
                        Some(RelLeg { venue: Venue::parse(&l.venue)?, market_id: l.market_id })
                    })
                    .collect::<Option<Vec<_>>>()?,
            })
        })
        .collect()
}

fn levels_of(v: Option<&serde_json::Value>) -> Option<Vec<Level>> {
    let mut out = Vec::new();
    for l in v?.as_array()? {
        let price = l.get("price")?.as_str()?.to_owned();
        let size = l.get("size")?.as_str()?.to_owned();
        // pydantic Level validator: price must be within [0,1]
        let p: f64 = price.parse().ok()?;
        if !(0.0..=1.0).contains(&p) {
            return None;
        }
        out.push(Level { price, size });
    }
    Some(out)
}

fn main() {
    let mut tape = String::new();
    let mut registry = "config/registry.yaml".to_string();
    let mut out_path: Option<String> = None;
    let mut max_events: u64 = 0;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--tape" => tape = it.next().expect("--tape"),
            "--registry" => registry = it.next().expect("--registry"),
            "--out" => out_path = it.next(),
            "--max-events" => max_events = it.next().expect("n").parse().expect("int"),
            other => panic!("unknown arg {other}"),
        }
    }

    let rels = load_rels(&registry);
    let mut by_market: HashMap<(Venue, String), Vec<usize>> = HashMap::new();
    for (i, r) in rels.iter().enumerate() {
        for leg in &r.legs {
            by_market.entry((leg.venue, leg.market_id.clone())).or_default().push(i);
        }
    }

    let mut cx = Cx::default();
    let fees = FeeSchedule::new(&mut cx);
    let mut books = BookBuilder::new();
    let mut digest = Sha256::new();
    let (mut n_ev, mut n_book, mut n_opp) = (0u64, 0u64, 0u64);
    let mut out = out_path.map(|p| std::io::BufWriter::new(std::fs::File::create(p).expect("out")));

    let f = std::fs::File::open(&tape).expect("open tape");
    for line in std::io::BufReader::new(f).lines() {
        let line = line.expect("read line");
        if line.is_empty() {
            continue;
        }
        n_ev += 1;
        if max_events > 0 && n_ev > max_events {
            break;
        }
        let v: serde_json::Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let kind = v.get("kind").and_then(|k| k.as_str()).unwrap_or("");
        let venue = match v.get("venue").and_then(|x| x.as_str()).and_then(Venue::parse) {
            Some(x) => x,
            None => continue,
        };
        let market_id = match v.get("market_id").and_then(|x| x.as_str()) {
            Some(m) => m.to_owned(),
            None => continue,
        };
        let seq = v.get("seq").and_then(|x| x.as_u64()).unwrap_or(0);
        let ts_local_ns = v.get("ts_local_ns").and_then(|x| x.as_i64()).unwrap_or(0);
        let ts_venue = v.get("ts_venue").and_then(|x| x.as_str()).map(|s| s.to_owned());

        match kind {
            "snapshot" => {
                let (Some(bids), Some(asks)) =
                    (levels_of(v.get("bids")), levels_of(v.get("asks")))
                else {
                    continue; // pydantic parse failure parity
                };
                books.apply_snapshot(venue, &market_id, bids, asks, seq, ts_local_ns, ts_venue);
            }
            "delta" => {
                let side = match v.get("side").and_then(|x| x.as_str()) {
                    Some("bid") => BookSide::Bid,
                    Some("ask") => BookSide::Ask,
                    _ => continue,
                };
                let (Some(price), Some(size)) = (
                    v.get("price").and_then(|x| x.as_str()),
                    v.get("size").and_then(|x| x.as_str()),
                ) else {
                    continue;
                };
                match books.apply_delta(
                    venue, &market_id, side, price, size, seq, ts_local_ns, ts_venue,
                ) {
                    Ok(_) => {}
                    Err(ApplyError::GapDetected { .. }) | Err(ApplyError::NotSynced) => continue,
                }
            }
            _ => continue, // trades don't scan
        }
        n_book += 1;
        if let Some(idxs) = by_market.get(&(venue, market_id)) {
            for &i in idxs {
                let rel = &rels[i];
                let metas =
                    |_: &RelLeg| MarketMeta::default_for_golden(&mut Cx::default());
                for opp in scan_relationship(&mut cx, &fees, rel, &books, &metas, ts_local_ns) {
                    let l = opp.canonical_line(&mut cx);
                    digest.update(l.as_bytes());
                    digest.update(b"\n");
                    n_opp += 1;
                    if let Some(o) = out.as_mut() {
                        writeln!(o, "{l}").expect("write out");
                    }
                }
            }
        }
    }
    println!(
        "{{\"events\":{n_ev},\"book_events\":{n_book},\"opportunities\":{n_opp},\"sha256\":\"{:x}\"}}",
        digest.finalize()
    );
}
