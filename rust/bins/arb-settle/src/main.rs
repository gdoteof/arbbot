//! arb-settle — the settlement sweep. Baskets that ran to resolution become
//! `unwound` records in the trade ledger.
//!
//! A basket held to resolution is closed by the venues, not by an order, so no
//! fill ever tells the engine it ended. This reads the ledger for lots that are
//! still open, asks each venue's PUBLIC market endpoint whether the leg's
//! market has paid, and appends one closing record per lot once EVERY leg has.
//! The engine's exposure-release poll and the Trades tab both read that record
//! the way they read any other exit.
//!
//!   arb-settle [--ledger data/exec/trades.jsonl]           # DRY: prints the plan
//!   arb-settle [--ledger data/exec/trades.jsonl] --write   # appends it
//!
//! Each leg is recorded at ITS OWN venue's settlement value, never at an
//! assumed $1.00 for the pair. A hedged basket collects exactly $1.00/ct when
//! the two venues resolve the question the same way; when they do not, the
//! record says what each actually paid and the run says so on stderr.
//!
//! WHAT IS NOT BOOKED: an open record that is not a two-sided basket on Kalshi
//! and PM-US (a naked leg, a leg on another venue). Those are counted and left
//! open — the Trades tab can only price an exit that flattens both sides.
//!
//! No credentials and no orders: market data only. Account cash is not read,
//! so a lot whose venue position differed from its ledger record still settles
//! here at what the LEDGER says it held.

use std::collections::{BTreeSet, HashMap};
use std::process::exit;

use serde_json::{json, Value};

// The engine's own ledger module, compiled in rather than re-implemented. This
// binary appends to the file the engine arms on, so the read that refuses a
// torn line and the append that heals one and fsyncs have to be the same code.
#[allow(dead_code)]
#[path = "../../arb-trader/src/ledger.rs"]
mod ledger;

const KALSHI_REST: &str = "https://api.elections.kalshi.com/trade-api/v2";
const PMUS_GATEWAY: &str = "https://gateway.polymarket.us/v1";
const KALSHI: &str = "kalshi";
const PMUS: &str = "polymarket_us";
/// Tickers per Kalshi `/markets` request, under its default page of 100.
const KALSHI_PAGE: usize = 50;
const SOURCE: &str = "arb-settle";
const EPS: f64 = 1e-9;

fn str_of<'a>(r: &'a Value, field: &str) -> Option<&'a str> {
    r.get(field).and_then(Value::as_str)
}

fn f64_of(r: &Value, field: &str) -> Option<f64> {
    r.get(field).and_then(Value::as_f64)
}

/// A number the writer may have spelled as a string (`"price": "1"`).
fn num(v: Option<&Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// One held leg of an open lot.
#[derive(Clone, Debug, PartialEq)]
struct Leg {
    venue: String,
    market: String,
    /// The side HELD: `true` is YES.
    yes: bool,
    qty: f64,
}

/// An open basket, net of whatever has already been unwound from it.
#[derive(Debug)]
struct Lot {
    rel: String,
    /// The entry record's `ts` — with `rel`, the identity a close names.
    ts: f64,
    qty: f64,
    legs: Vec<Leg>,
}

/// Which side a leg holds, or `None` for one that is not a plain long.
///
/// The same reading as the Trades tab's `leg_dir`, arm for arm: that tab
/// prices the record this writes, so the two must agree on what was held. The
/// engine spells a YES bid `bid` and a NO bid — an ask on YES from flat —
/// `ask`.
fn held_yes(action: &str, side: &str) -> Option<bool> {
    if action == "sell" || action.starts_with("close_via_") {
        return None;
    }
    match (action, side) {
        ("buy_no", _) | (_, "no") => Some(false),
        ("buy_yes", _) | (_, "yes") => Some(true),
        (_, "bid") => Some(true),
        (_, "ask") => Some(false),
        _ => None,
    }
}

/// The record's legs scaled to what is still open — or `None` unless they make
/// a two-sided basket: every leg a readable long on Kalshi or PM-US, with YES
/// and NO each covering the whole lot.
fn basket_legs(rec: &Value, booked: f64, scale: f64) -> Option<Vec<Leg>> {
    let mut legs = Vec::new();
    let (mut yes_qty, mut no_qty) = (0.0, 0.0);
    for l in rec.get("legs")?.as_array()? {
        let venue = str_of(l, "venue")?;
        if venue != KALSHI && venue != PMUS {
            return None;
        }
        let yes = held_yes(str_of(l, "action").unwrap_or(""), str_of(l, "side").unwrap_or(""))?;
        let qty = num(l.get("qty")).unwrap_or(booked) * scale;
        if yes {
            yes_qty += qty;
        } else {
            no_qty += qty;
        }
        legs.push(Leg {
            venue: venue.to_string(),
            market: str_of(l, "market_id")?.to_string(),
            yes,
            qty,
        });
    }
    let open = booked * scale;
    (yes_qty >= open - EPS && no_qty >= open - EPS).then_some(legs)
}

/// The open lots this sweep can settle, and how many open records it cannot.
///
/// The netting is `marks::open_baskets`'s: an `unwound` record is matched to
/// the `open` record it closes on `(relationship_id, closes_ts)`, and a partial
/// unwind leaves the remainder open. That is also what makes a run repeatable —
/// the record this appends is itself an `unwound`, so the lot is gone from the
/// next fold.
fn open_lots(records: Vec<Value>) -> (Vec<Lot>, usize) {
    let records = ledger::apply_corrections(records);
    let mut unwound: HashMap<(String, u64), f64> = HashMap::new();
    for r in &records {
        if str_of(r, "status") != Some("unwound") {
            continue;
        }
        if let (Some(rel), Some(ct)) = (str_of(r, "relationship_id"), f64_of(r, "closes_ts")) {
            *unwound.entry((rel.to_string(), ct.to_bits())).or_default() +=
                f64_of(r, "qty").unwrap_or(0.0);
        }
    }
    let (mut lots, mut unsupported) = (Vec::new(), 0);
    for r in &records {
        if str_of(r, "status") != Some("open") {
            continue;
        }
        let (Some(rel), Some(ts)) = (str_of(r, "relationship_id"), f64_of(r, "ts")) else {
            continue;
        };
        let booked = f64_of(r, "qty").unwrap_or(0.0);
        let closed = unwound.get(&(rel.to_string(), ts.to_bits())).copied().unwrap_or(0.0);
        let qty = booked - closed;
        if booked <= 0.0 || qty <= EPS {
            continue;
        }
        match basket_legs(r, booked, qty / booked) {
            Some(legs) => lots.push(Lot { rel: rel.to_string(), ts, qty, legs }),
            None => unsupported += 1,
        }
    }
    (lots, unsupported)
}

/// A market that has paid: which way, and when the venue says it did.
#[derive(Clone, Debug, PartialEq)]
struct Resolved {
    yes: bool,
    at: String,
}

/// `(venue, market)` to its resolution. A market that has not paid is absent.
type Results = HashMap<(String, String), Resolved>;

/// Kalshi, from one row of `GET /markets`. It pays on `finalized`;
/// `determined` — the result is known, the money has not moved — is still open.
fn kalshi_resolved(m: &Value) -> Option<Resolved> {
    if str_of(m, "status") != Some("finalized") {
        return None;
    }
    let yes = match str_of(m, "result")? {
        "yes" => true,
        "no" => false,
        _ => return None,
    };
    Some(Resolved { yes, at: str_of(m, "settlement_ts").unwrap_or("").to_string() })
}

/// PM-US, from `GET /market/slug/{slug}`.
///
/// Read off `marketSides`, where the `long` side is YES and a resolved market
/// prices its two sides exactly 1 and 0. NOT off `outcomes`/`outcomePrices`: on
/// a market that resolved YES those read `["No","Yes"]` / `["1","0"]`. And not
/// off `/bbo`'s `settlementPx`, which on a live market is the daily mark.
fn pmus_resolved(body: &Value) -> Option<Resolved> {
    let m = body.get("market")?;
    if str_of(m, "status") != Some("MARKET_STATUS_RESOLVED") {
        return None;
    }
    let (mut long, mut short) = (None, None);
    for s in m.get("marketSides")?.as_array()? {
        let px = num(s.get("price"));
        if s.get("long").and_then(Value::as_bool)? {
            long = px;
        } else {
            short = px;
        }
    }
    let (long, short) = (long?, short?);
    let is = |px: f64, want: f64| (px - want).abs() < EPS;
    let yes = if is(long, 1.0) && is(short, 0.0) {
        true
    } else if is(long, 0.0) && is(short, 1.0) {
        false
    } else {
        return None;
    };
    Some(Resolved { yes, at: str_of(m, "endDate").unwrap_or("").to_string() })
}

fn get_json(http: &reqwest::blocking::Client, url: &str) -> Result<Value, String> {
    let r = http.get(url).send().map_err(|e| format!("GET {url}: {e}"))?;
    let status = r.status();
    let body = r.text().map_err(|e| format!("GET {url}: {e}"))?;
    if !status.is_success() {
        return Err(format!("GET {url}: HTTP {status}"));
    }
    serde_json::from_str(&body).map_err(|e| format!("GET {url}: {e}"))
}

/// Ask the venues about the markets the open lots hold.
///
/// Kalshi first, a page of tickers per request. PM-US is one request per
/// market, so it is asked only about lots whose Kalshi legs have all paid —
/// on an ordinary run, none.
fn resolutions(http: &reqwest::blocking::Client, lots: &[Lot]) -> Result<Results, String> {
    let mut out = Results::new();
    let tickers: Vec<&str> = lots
        .iter()
        .flat_map(|lot| &lot.legs)
        .filter(|l| l.venue == KALSHI)
        .map(|l| l.market.as_str())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    for page in tickers.chunks(KALSHI_PAGE) {
        let body = get_json(http, &format!("{KALSHI_REST}/markets?tickers={}", page.join(",")))?;
        for m in body.get("markets").and_then(Value::as_array).into_iter().flatten() {
            if let (Some(ticker), Some(r)) = (str_of(m, "ticker"), kalshi_resolved(m)) {
                out.insert((KALSHI.to_string(), ticker.to_string()), r);
            }
        }
    }
    let slugs: BTreeSet<&str> = lots
        .iter()
        .filter(|lot| {
            lot.legs
                .iter()
                .filter(|l| l.venue == KALSHI)
                .all(|l| out.contains_key(&(l.venue.clone(), l.market.clone())))
        })
        .flat_map(|lot| &lot.legs)
        .filter(|l| l.venue == PMUS)
        .map(|l| l.market.as_str())
        .collect();
    for slug in slugs {
        let body = get_json(http, &format!("{PMUS_GATEWAY}/market/slug/{slug}"))?;
        if let Some(r) = pmus_resolved(&body) {
            out.insert((PMUS.to_string(), slug.to_string()), r);
        }
    }
    Ok(out)
}

/// Whole contracts as integers, the way the engine books them.
fn qty_json(q: f64) -> Value {
    if q.fract() == 0.0 {
        json!(q as i64)
    } else {
        json!(q)
    }
}

/// The closing record for a lot whose every leg has paid, and what the legs
/// collected — or `None` while any leg has not.
///
/// The legs are written as the Trades tab reads an exit: each held side SOLD
/// at its venue's YES settlement price, so a sold YES collects `yes_price` and
/// a sold NO collects `1 - yes_price`. `realized_pnl_usd` is absent for the
/// reason it is absent from every exit the engine writes — the entry's fees
/// are not on this ledger — and that tab derives it from these proceeds
/// against the entry's basis. `fees: 0` is a fact, not a model: neither venue
/// charges for settlement.
fn settlement(lot: &Lot, results: &Results, ts: f64) -> Option<(Value, f64)> {
    let mut legs = Vec::new();
    let mut paid = 0.0;
    for l in &lot.legs {
        let r = results.get(&(l.venue.clone(), l.market.clone()))?;
        if l.yes == r.yes {
            paid += l.qty;
        }
        legs.push(json!({
            "venue": l.venue,
            "market_id": l.market,
            "action": "sell",
            "side": if l.yes { "yes" } else { "no" },
            "qty": qty_json(l.qty),
            "yes_price": if r.yes { "1.0000" } else { "0.0000" },
            "role": "settlement",
            "fees": 0,
            "settled_at": r.at,
        }));
    }
    let mut note = String::new();
    if (paid - lot.qty).abs() > EPS {
        note.push_str(&format!(
            "THE VENUES DID NOT PAY THIS BASKET $1.00/CT: its legs collected ${paid:.2} on {} \
             contract(s). ",
            lot.qty
        ));
    }
    note.push_str(
        "held to resolution: every leg's market has paid, and each leg is recorded at its own \
         venue's settlement value — a held side that won collects $1.00/ct, one that lost \
         collects nothing. Settlement is not an order and carries no fee.",
    );
    let rec = json!({
        "ts": ts,
        "relationship_id": lot.rel,
        "title": format!("{} (rust settlement)", lot.rel),
        "strategy": "settlement",
        "status": "unwound",
        "closes_ts": lot.ts,
        "qty": qty_json(lot.qty),
        "source": SOURCE,
        "legs": legs,
        "note": note,
    });
    Some((rec, paid))
}

fn now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs_f64())
        .unwrap_or(0.0)
}

fn die(msg: &str) -> ! {
    eprintln!("[settle] {msg}");
    exit(1);
}

fn main() {
    let mut ledger_path = "data/exec/trades.jsonl".to_string();
    let mut write = false;
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--ledger" => {
                i += 1;
                ledger_path = args.get(i).cloned().unwrap_or_default();
            }
            "--write" => write = true,
            other => {
                eprintln!("unknown arg: {other}\nusage: arb-settle [--ledger <jsonl>] [--write]");
                exit(2);
            }
        }
        i += 1;
    }

    let records = ledger::read(&ledger_path).unwrap_or_else(|e| die(&e));
    let (lots, unsupported) = open_lots(records);
    let http = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(20))
        .build()
        .unwrap_or_else(|e| die(&format!("http client: {e}")));
    let results = resolutions(&http, &lots).unwrap_or_else(|e| die(&e));

    let started = now();
    let mut settled = 0usize;
    for lot in &lots {
        // `(relationship_id, ts)` is a record's identity, so no two records of
        // one run may share a `ts`. A microsecond is four ulps at this
        // magnitude.
        let ts = started + settled as f64 * 1e-6;
        let Some((rec, paid)) = settlement(lot, &results, ts) else {
            let has_paid = |l: &&Leg| results.contains_key(&(l.venue.clone(), l.market.clone()));
            if let Some(l) = lot.legs.iter().find(has_paid) {
                println!(
                    "WAITING {} x{}: {} {} has paid, another leg has not",
                    lot.rel, lot.qty, l.venue, l.market
                );
            }
            continue;
        };
        if write {
            ledger::append(&ledger_path, &rec).unwrap_or_else(|e| die(&e));
        }
        println!(
            "{} {} x{} paid ${paid:.2}",
            if write { "SETTLED" } else { "WOULD SETTLE" },
            lot.rel,
            lot.qty
        );
        if (paid - lot.qty).abs() > EPS {
            eprintln!(
                "[settle] DIVERGED {} x{}: the venues paid ${paid:.2}, not ${:.2} — they did \
                 not resolve this pair the same way",
                lot.rel, lot.qty, lot.qty
            );
        }
        settled += 1;
    }
    println!(
        "{settled} lot(s) {} of {} open; {unsupported} other open record(s) are not a two-sided \
         kalshi/polymarket_us basket and are left open",
        if write { "settled" } else { "would settle (dry run, pass --write)" },
        lots.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    /// Real shape: an engine `maker-hedge` basket. Kalshi `ask` is a NO bid,
    /// PM-US `bid` is a YES bid.
    const OPEN: &str = r#"{"fees_pending":true,"legs":[{"market_id":"KXTSLA-26OCTDELIV-470000","order_id":"m1790215722063","qty":21,"role":"maker","side":"ask","venue":"kalshi","yes_price":"0.4500"},{"market_id":"kpic-tsla-dlvrs-2026-q3-above-470k","order_id":"h1790215722001","qty":21,"role":"taker","side":"bid","venue":"polymarket_us","yes_price":"0.4200"}],"qty":21,"relationship_id":"xvus-tsla-q3-deliv-q3-above-470k","source":"arb-trader","status":"open","strategy":"maker-hedge","ts":1790216786.3069088}"#;
    const REL: &str = "xvus-tsla-q3-deliv-q3-above-470k";
    const K: &str = "KXTSLA-26OCTDELIV-470000";
    const P: &str = "kpic-tsla-dlvrs-2026-q3-above-470k";

    fn paid(k_yes: Option<bool>, p_yes: Option<bool>) -> Results {
        let mut r = Results::new();
        let at = |yes| Resolved { yes, at: "2026-10-02T14:30:43Z".into() };
        if let Some(y) = k_yes {
            r.insert((KALSHI.into(), K.into()), at(y));
        }
        if let Some(y) = p_yes {
            r.insert((PMUS.into(), P.into()), at(y));
        }
        r
    }

    /// What the Trades tab will read the record's legs as having collected.
    fn proceeds(rec: &Value) -> f64 {
        rec["legs"]
            .as_array()
            .unwrap()
            .iter()
            .map(|l| {
                assert_eq!(l["action"], "sell");
                let px: f64 = l["yes_price"].as_str().unwrap().parse().unwrap();
                let qty = l["qty"].as_f64().unwrap();
                if l["side"] == "yes" { px * qty } else { (1.0 - px) * qty }
            })
            .sum()
    }

    #[test]
    fn an_engine_basket_is_one_lot_holding_no_on_kalshi_and_yes_on_pmus() {
        let (lots, unsupported) = open_lots(vec![v(OPEN)]);
        assert_eq!((lots.len(), unsupported), (1, 0));
        let lot = &lots[0];
        assert_eq!((lot.rel.as_str(), lot.qty), (REL, 21.0));
        assert_eq!(lot.legs[0], Leg { venue: KALSHI.into(), market: K.into(), yes: false, qty: 21.0 });
        assert_eq!(lot.legs[1], Leg { venue: PMUS.into(), market: P.into(), yes: true, qty: 21.0 });
    }

    #[test]
    fn a_partial_unwind_leaves_the_remainder_and_a_full_one_leaves_nothing() {
        let close = |qty: u32| {
            v(&format!(
                r#"{{"ts":1790300000.5,"relationship_id":"{REL}","status":"unwound","closes_ts":1790216786.3069088,"qty":{qty}}}"#
            ))
        };
        let (lots, _) = open_lots(vec![v(OPEN), close(15)]);
        assert_eq!(lots[0].qty, 6.0);
        assert!(lots[0].legs.iter().all(|l| (l.qty - 6.0).abs() < EPS), "legs scale with the lot");

        let (lots, unsupported) = open_lots(vec![v(OPEN), close(21)]);
        assert_eq!((lots.len(), unsupported), (0, 0));
    }

    #[test]
    fn an_open_record_that_is_not_a_two_sided_basket_is_counted_and_left_alone() {
        let naked = r#"{"ts":1.0,"relationship_id":"mltox-x","status":"open","qty":5,"legs":[{"venue":"kalshi","market_id":"KXA","side":"bid","qty":5}]}"#;
        let same_side = r#"{"ts":2.0,"relationship_id":"r2","status":"open","qty":5,"legs":[{"venue":"kalshi","market_id":"KXA","side":"bid","qty":5},{"venue":"polymarket_us","market_id":"a","side":"bid","qty":5}]}"#;
        let elsewhere = r#"{"ts":3.0,"relationship_id":"r3","status":"open","qty":5,"legs":[{"venue":"kalshi","market_id":"KXA","side":"bid","qty":5},{"venue":"polymarket","market_id":"a","side":"ask","qty":5}]}"#;
        let sold = r#"{"ts":4.0,"relationship_id":"r4","status":"open","qty":5,"legs":[{"venue":"kalshi","market_id":"KXA","action":"sell","side":"yes","qty":5},{"venue":"polymarket_us","market_id":"a","side":"ask","qty":5}]}"#;
        let (lots, unsupported) = open_lots(vec![v(naked), v(same_side), v(elsewhere), v(sold)]);
        assert_eq!((lots.len(), unsupported), (0, 4));
    }

    #[test]
    fn kalshi_pays_on_finalized_not_on_determined() {
        let row = |status: &str, result: &str| {
            v(&format!(
                r#"{{"ticker":"{K}","status":"{status}","result":"{result}","settlement_ts":"2026-10-02T14:30:43.970747Z"}}"#
            ))
        };
        assert_eq!(
            kalshi_resolved(&row("finalized", "no")),
            Some(Resolved { yes: false, at: "2026-10-02T14:30:43.970747Z".into() })
        );
        assert!(kalshi_resolved(&row("finalized", "yes")).unwrap().yes);
        assert_eq!(kalshi_resolved(&row("determined", "yes")), None);
        assert_eq!(kalshi_resolved(&row("active", "")), None);
        assert_eq!(kalshi_resolved(&row("finalized", "")), None, "a result that is neither is not one");
    }

    /// Real shape, cut down: a market that resolved YES. Its `outcomePrices`
    /// read "No = 1".
    fn pmus_market(status: &str, long_px: &str, short_px: &str) -> Value {
        v(&format!(
            r#"{{"market":{{"slug":"{P}","endDate":"2026-10-02T14:35:53Z","closed":true,"status":"{status}","outcomes":"[\"No\",\"Yes\"]","outcomePrices":"[\"1\",\"0\"]","marketSides":[{{"description":"Yes","price":"{long_px}","long":true}},{{"description":"No","price":"{short_px}","long":false}}]}}}}"#
        ))
    }

    #[test]
    fn pmus_is_read_off_its_sides_not_its_outcome_arrays() {
        assert_eq!(
            pmus_resolved(&pmus_market("MARKET_STATUS_RESOLVED", "1", "0")),
            Some(Resolved { yes: true, at: "2026-10-02T14:35:53Z".into() })
        );
        assert!(!pmus_resolved(&pmus_market("MARKET_STATUS_RESOLVED", "0", "1")).unwrap().yes);
        assert_eq!(pmus_resolved(&pmus_market("MARKET_STATUS_OPEN", "1", "0")), None);
        assert_eq!(
            pmus_resolved(&pmus_market("MARKET_STATUS_RESOLVED", "0.42", "0.58")),
            None,
            "a resolved market prices its sides 1 and 0, or this cannot say which way it went"
        );
    }

    #[test]
    fn nothing_is_booked_until_every_leg_has_paid() {
        let (lots, _) = open_lots(vec![v(OPEN)]);
        assert!(settlement(&lots[0], &paid(None, None), 9.0).is_none());
        assert!(settlement(&lots[0], &paid(Some(true), None), 9.0).is_none());
        assert!(settlement(&lots[0], &paid(None, Some(true)), 9.0).is_none());
        assert!(settlement(&lots[0], &paid(Some(true), Some(true)), 9.0).is_some());
    }

    #[test]
    fn a_settled_basket_closes_its_lot_at_one_dollar_a_contract_whichever_side_won() {
        let (lots, _) = open_lots(vec![v(OPEN)]);
        for won in [true, false] {
            let (rec, collected) = settlement(&lots[0], &paid(Some(won), Some(won)), 9.0).unwrap();
            assert_eq!(collected, 21.0);
            assert_eq!(proceeds(&rec), 21.0);
            assert_eq!((&rec["status"], &rec["strategy"]), (&json!("unwound"), &json!("settlement")));
            assert_eq!(rec["qty"], json!(21), "whole contracts stay integers");
            assert!(!rec["note"].as_str().unwrap().contains("DID NOT PAY"));
            assert!(rec.get("realized_pnl_usd").is_none());
            // The winning leg is the one that collected, on its own venue.
            let k = &rec["legs"][0];
            assert_eq!((&k["venue"], &k["side"]), (&json!(KALSHI), &json!("no")));
            assert_eq!(k["yes_price"], if won { "1.0000" } else { "0.0000" });
        }
    }

    /// `closes_ts` is matched on the bits of the float, so it has to survive
    /// being written and read back.
    #[test]
    fn the_record_names_its_entry_exactly_and_is_not_booked_twice() {
        let (lots, _) = open_lots(vec![v(OPEN)]);
        let (rec, _) = settlement(&lots[0], &paid(Some(true), Some(true)), 1791180000.25).unwrap();
        let reread = v(&serde_json::to_string(&rec).unwrap());
        assert_eq!(reread["closes_ts"].as_f64().unwrap().to_bits(), 1790216786.3069088f64.to_bits());

        let (lots, unsupported) = open_lots(vec![v(OPEN), reread]);
        assert_eq!((lots.len(), unsupported), (0, 0), "the lot it closed is gone from the next fold");
    }

    #[test]
    fn venues_that_disagree_are_booked_at_what_each_paid() {
        let (lots, _) = open_lots(vec![v(OPEN)]);
        // Held NO on Kalshi and YES on PM-US. Kalshi says YES, PM-US says NO:
        // both legs lost.
        let (rec, collected) = settlement(&lots[0], &paid(Some(true), Some(false)), 9.0).unwrap();
        assert_eq!((collected, proceeds(&rec)), (0.0, 0.0));
        assert!(rec["note"].as_str().unwrap().starts_with("THE VENUES DID NOT PAY THIS BASKET $1.00/CT"));
        // The other way round, both won.
        let (rec, collected) = settlement(&lots[0], &paid(Some(false), Some(true)), 9.0).unwrap();
        assert_eq!((collected, proceeds(&rec)), (42.0, 42.0));
    }

    #[test]
    fn a_fractional_remainder_keeps_its_fraction() {
        let open = OPEN.replace(r#""qty":21"#, r#""qty":1.6"#);
        let (lots, _) = open_lots(vec![v(&open)]);
        let (rec, collected) = settlement(&lots[0], &paid(Some(true), Some(true)), 9.0).unwrap();
        assert_eq!(rec["qty"], json!(1.6));
        assert!((collected - 1.6).abs() < EPS);
    }
}
