//! arb-intent — the P2 intent-parity gate: replay a day's merged tape
//! through the Rust Quoter and emit the canonical intent stream + SHA256,
//! compared byte-for-byte against scripts/intent_replay.py (Python).
//!
//!   arb-intent --tape merged-<day>.jsonl --registry config/registry.yaml \
//!       [--out intents.jsonl] [--max-events N]

use arb_core::book::{ApplyError, BookBuilder};
use arb_core::fees::FeeSchedule;
use arb_core::model::{BookSide, Level, Venue};
use arb_core::quoter::{Quoter, Toxgate};
use arb_core::scan::{Cx, Rel, RelLeg, RelType};
use std::sync::Arc;
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

fn parse_venue(s: &str) -> Option<Venue> {
    Some(match s {
        "kalshi" => Venue::Kalshi,
        "polymarket" => Venue::Polymarket,
        "polymarket_us" => Venue::PolymarketUs,
        _ => return None,
    })
}

fn levels_of(v: Option<&serde_json::Value>) -> Option<Vec<Level>> {
    let mut out = Vec::new();
    for l in v?.as_array()? {
        let price = l.get("price")?.as_str()?.to_owned();
        let size = l.get("size")?.as_str()?.to_owned();
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
    let mut min_apr: f64 = 0.0;
    let mut resolve_years: Option<f64> = None;
    let mut toxgate_file: Option<String> = None;
    let mut suppress: std::collections::HashSet<(String, &'static str)> =
        std::collections::HashSet::new();
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--tape" => tape = it.next().expect("--tape"),
            "--registry" => registry = it.next().expect("--registry"),
            "--out" => out_path = it.next(),
            "--max-events" => max_events = it.next().expect("n").parse().expect("int"),
            "--min-apr" => min_apr = it.next().expect("f").parse().expect("float"),
            "--resolve-years" => {
                resolve_years = Some(it.next().expect("f").parse().expect("float"))
            }
            "--toxgate-file" => toxgate_file = it.next(),
            "--suppress" => {
                // "market_id:side" — side must be bid|ask
                let v = it.next().expect("--suppress market:side");
                let (m, s) = v.rsplit_once(':').expect("market:side");
                let side: &'static str = match s {
                    "bid" => "bid",
                    "ask" => "ask",
                    other => panic!("bad suppress side {other}"),
                };
                suppress.insert((m.to_owned(), side));
            }
            other => panic!("unknown arg {other}"),
        }
    }
    let toxgate: Option<Arc<Toxgate>> = toxgate_file.map(|p| {
        let v: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(&p).expect("read toxgate"))
                .expect("parse toxgate");
        let ts = v.get("ts").and_then(|x| x.as_f64()).unwrap_or(0.0);
        let mut markets = std::collections::HashMap::new();
        if let Some(ms) = v.get("markets").and_then(|x| x.as_object()) {
            for (mid, sides) in ms {
                let mut by_side = std::collections::HashMap::new();
                if let Some(so) = sides.as_object() {
                    for (side, score) in so {
                        if let Some(f) = score.as_f64() {
                            by_side.insert(side.clone(), f);
                        }
                    }
                }
                markets.insert(mid.clone(), by_side);
            }
        }
        Arc::new(Toxgate { ts, markets })
    });

    let text = std::fs::read_to_string(&registry).expect("read registry");
    let doc: RegistryDoc = serde_yaml::from_str(&text).expect("parse registry");
    let mut quoters: Vec<Quoter> = doc
        .relationships
        .into_iter()
        .filter(|r| r.legs.len() == 2 && r.verdict != "rejected")
        .filter_map(|r| {
            Some(Quoter::new(Rel {
                id: r.id,
                rtype: RelType::from_str(&r.rtype)?,
                tranche: r.tranche,
                legs: r
                    .legs
                    .into_iter()
                    .map(|l| {
                        Some(RelLeg { venue: parse_venue(&l.venue)?, market_id: l.market_id })
                    })
                    .collect::<Option<Vec<_>>>()?,
            }))
        })
        .collect();
    let mut by_market: HashMap<(Venue, String), Vec<usize>> = HashMap::new();
    for (qi, q) in quoters.iter().enumerate() {
        for leg in &q.rel.legs {
            by_market.entry((leg.venue, leg.market_id.clone())).or_default().push(qi);
        }
    }

    let mut cx = Cx::default();
    for q in quoters.iter_mut() {
        q.set_apr(&mut cx, min_apr, resolve_years);
        q.set_suppress(suppress.clone());
        q.set_toxgate(toxgate.clone());
    }
    let fees = FeeSchedule::new(&mut cx);
    let mut books = BookBuilder::new();
    let mut digest = Sha256::new();
    let (mut n_ev, mut n_book, mut n_int) = (0u64, 0u64, 0u64);
    let mut next_oid: u64 = 0;
    let mut out =
        out_path.map(|p| std::io::BufWriter::new(std::fs::File::create(p).expect("out")));
    let mut intents: Vec<String> = Vec::new();

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
        let venue = match v.get("venue").and_then(|x| x.as_str()).and_then(parse_venue) {
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
                    continue;
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
            _ => continue,
        }
        n_book += 1;
        let now = ts_local_ns as f64 / 1e9;
        if let Some(idxs) = by_market.get(&(venue, market_id)) {
            for &qi in idxs {
                quoters[qi].on_book(&mut cx, &fees, &books, now, &mut next_oid, &mut intents);
                for l in intents.drain(..) {
                    digest.update(l.as_bytes());
                    digest.update(b"\n");
                    n_int += 1;
                    if let Some(o) = out.as_mut() {
                        writeln!(o, "{l}").expect("write out");
                    }
                }
            }
        }
    }
    println!(
        "{{\"events\":{n_ev},\"book_events\":{n_book},\"intents\":{n_int},\"sha256\":\"{:x}\"}}",
        digest.finalize()
    );
}
