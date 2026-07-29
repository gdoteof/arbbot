//! Trades this system made, with their accounting.
//!
//! Source of truth is the append-only ledger (`data/exec/trades.jsonl`).
//! Everything here is DERIVED on each request — nothing is stored twice, so
//! the tab cannot drift from what the engine booked.
//!
//! THE LEDGER IS APPEND-ONLY, so "what is open" is a FOLD, not a lookup.
//! Closing a basket never rewrites its line, it appends a compensating record.
//! `arb-trader/src/ledger.rs` folds this same file to seed risk exposure at
//! startup and the two MUST agree. When they did not (2026-07-28 audit) the
//! trader saw 341 open contracts while this tab reported 608, and this tab
//! reported +$110.12 of profit on a book whose own double-entry statement said
//! −$5.72. The fold is:
//!
//!   1. `correction` records are folded into their target and dropped
//!   2. `unwound` records net against the `open` record their `closes_ts` names
//!   3. what survives, with qty reduced by any partial unwind, is open
//!
//! A folded `status: superseded` means "this record is a duplicate, remove it".
//!
//! THE LEDGER HAS TWO RECORD SHAPES and they account differently. Getting this
//! wrong is not cosmetic: the first version of this file priced only the newer
//! shape and reported the book as $3.76 in the RED when it was in profit.
//!
//!   * Python-era (no `source`): legs carry `action: buy_yes|buy_no` and often
//!     their own settled `fees`; the record carries `cost_usd` and
//!     `payoff_usd`. `cost_usd` is ALL-IN — fees are already inside it.
//!   * Engine (`source: arb-trader`): legs carry `side: bid|ask` and prices,
//!     and the record says `fees_pending` because the engine does not read
//!     fill reports. Cost must be derived and fees MODELLED.
//!
//! ONLY A HEDGED BASKET IS LOCKED. Legs long YES and long NO covering the same
//! contract count pay exactly $1.00 per contract at resolution whatever the
//! event does, so profit is payoff minus cost and it is a fact, not a forecast.
//! Nothing else is. One leg, both legs on the same side, or a leg whose side
//! this file cannot read is a DIRECTIONAL bet that pays $0.00 or $1.00 and
//! nobody knows which until the event resolves: it prices as `net_usd: null`
//! with an `unpriced_reason` and is excluded from every total. Pretending
//! otherwise is what turned 9 naked legs into "+$26.92 locked" and promoted a
//! naked Kalshi punt to the top of the strategy board.
//!
//! For a CLOSED record the ledger already knows the answer and this file must
//! not re-derive it: `realized_pnl_usd` (or `profit_usd`) is bank truth,
//! including the naked settlements that paid $0.00. Deriving instead from an
//! assumed $1.00 payoff is how a $2.16 realised LOSS displayed as +$4.00.
//! A closed record carrying neither field cannot be priced and says so.
//!
//! Where a number cannot be established it is `null` with a reason. A
//! plausible-looking dollar amount in a column of dollars is worse than a
//! blank, because a human acts on it.
//!
//! Status matters too. Only `open` positions still tie up capital, so only
//! they carry an APR-to-hold; `realized` and `unwound` are history, and
//! `correction` is a compensating adjustment, not a new position.

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::resolve::{resolve_date, today_iso, years_between};
use arb_core::scan::Cx;
use std::collections::{HashMap, HashSet};

/// Contract counts are compared, never trusted to be integral — `qty` is a
/// float in half this file (`2.0`, `5.0`).
const EPS: f64 = 1e-9;

fn num(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

/// Open baskets and the unwinds that close them are matched on
/// `(relationship_id, ts)` with ts by bit pattern — the same predicate
/// `arb-trader/src/ledger.rs` uses, so the trader and this tab fold identically.
///
/// CORRECTIONS ARE NOT KEYED THIS WAY. See `apply_corrections`: requiring a
/// `relationship_id` there silently discarded two retractions worth $7.37.
type Key = (String, u64);

fn key_of(rel: Option<&str>, ts: Option<f64>) -> Option<Key> {
    Some((rel?.to_string(), ts?.to_bits()))
}

fn rel_of(r: &serde_json::Value) -> Option<&str> {
    r.get("relationship_id").and_then(|v| v.as_str())
}

fn status_of(r: &serde_json::Value) -> &str {
    r.get("status").and_then(|v| v.as_str()).unwrap_or("open")
}

/// What a leg did to our exposure.
///
/// `side` ALONE IS NOT ENOUGH, and reading only `side` was a fail-open bug
/// (2026-07-28 audit). A leg with `action: "sell"` on the YES side is a CREDIT
/// against an obligation, not a debit for an asset — 54 such legs across 28
/// records are already on disk. Priced by side alone, a record whose two legs
/// were both SOLD at 0.19 rendered `cost_usd 41.00 / payoff_usd 41.00`, pure
/// fiction; and an OPEN record selling YES @0.10 and selling NO @0.70 collects
/// $0.80 against the $1.00 it owes — a 20c/ct LOSS — and reported as
/// `hedged: true, net +1.80, apr 52.8%`. That is the same failure class as
/// pricing a naked leg as locked profit.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Leg {
    /// Bought this side: a debit, and one half of a hedged basket.
    LongYes,
    LongNo,
    /// Sold, or bought to close a short: a credit against an obligation. This
    /// file prices LONG baskets only, so such a leg makes the record
    /// unpriceable here rather than mispriced.
    Short,
    /// Not readable — e.g. the `side: "mixed"` the maker probe writes. Never
    /// guessed: a guess here decides whether a bet counts as hedged.
    Unknown,
}

fn leg_dir(action: &str, side: &str) -> Leg {
    // A sale or a close is never an opening debit, whatever `side` says.
    if action == "sell" || action.starts_with("close_via_") {
        return Leg::Short;
    }
    match (action, side) {
        ("buy_no", _) | (_, "no") => Leg::LongNo,
        ("buy_yes", _) | (_, "yes") => Leg::LongYes,
        (_, "bid") => Leg::LongYes, // engine: bought YES at this price
        // engine: an ask on YES from flat IS a bid on NO at 1-px on both these
        // venues' order books — still a debit, still collateralised.
        (_, "ask") => Leg::LongNo,
        _ => Leg::Unknown,
    }
}

/// Step 1 of the fold: shallow-merge each `correction`'s `fields` into the
/// record its `corrects_ts` names, then drop the corrections. A correction is
/// not a no-op — it is the ledger retracting a value, so ignoring it leaves the
/// superseded number standing.
///
/// TARGETED BY `corrects_ts`, NOT BY `(relationship_id, corrects_ts)`. Two of
/// the 45 corrections on disk carry NO `relationship_id`, and keying on the pair
/// discarded both (2026-07-28 audit) — they retract `realized_pnl_usd +2.6369`
/// to `-0.2348` and `+4.1308` to `-0.3660`, so `realized_net_usd` read $7.3685
/// too high. Their own notes quote the prior value ("CORRECTED from +2.6369"),
/// which matches the target's field exactly, so the intent was never in doubt.
/// `ts` is unique across the file, so `corrects_ts` identifies one record.
///
/// `arb-trader/src/ledger.rs` has the same pair-keying and is UNHARMED by it: it
/// folds for open exposure and never reads `realized_pnl_usd`, while both these
/// corrections amend records that are already `unwound`. Parity with the trader
/// is silent here, so the two folds legitimately differ on this point.
///
/// A correction that DOES name a relationship must name the target's own, and a
/// nameless correction is applied only when exactly one record carries that ts —
/// an ambiguous retraction is dropped and reported rather than guessed.
///
/// Returns `(corrections seen, records amended, corrections that reached nothing)`.
fn apply_corrections(records: &mut Vec<serde_json::Value>) -> (u64, u64, u64) {
    // (target ts bits, relationship_id IF the correction named one, fields).
    // A Vec, not a map: file order is the precedence order, and three
    // corrections target one fedcut record on disk where last-wins is correct.
    let mut fixes: Vec<(u64, Option<String>, serde_json::Map<String, serde_json::Value>)> =
        Vec::new();
    let mut corrections = 0u64;
    for r in records.iter() {
        if status_of(r) != "correction" {
            continue;
        }
        corrections += 1;
        let Some(ts) = num(r.get("corrects_ts")) else { continue };
        let Some(f) = r.get("fields").and_then(|v| v.as_object()) else { continue };
        fixes.push((ts.to_bits(), rel_of(r).map(str::to_string), f.clone()));
    }
    records.retain(|r| status_of(r) != "correction");
    if fixes.is_empty() {
        return (corrections, 0, corrections);
    }
    let mut per_ts: HashMap<u64, usize> = HashMap::new();
    for r in records.iter() {
        if let Some(ts) = num(r.get("ts")) {
            *per_ts.entry(ts.to_bits()).or_default() += 1;
        }
    }

    let mut used = vec![false; fixes.len()];
    let mut amended = 0u64;
    for r in records.iter_mut() {
        // Read everything needed off `r` first so nothing borrows it when it is
        // mutated below.
        let Some(ts) = num(r.get("ts")).map(f64::to_bits) else { continue };
        let rel = rel_of(r).map(str::to_string);
        let unique_ts = per_ts.get(&ts).copied().unwrap_or(0) == 1;
        let mut hit = false;
        for (i, (fts, frel, f)) in fixes.iter().enumerate() {
            if *fts != ts {
                continue;
            }
            match frel {
                // Named: it must name THIS relationship.
                Some(fr) if Some(fr.as_str()) != rel.as_deref() => continue,
                // Nameless: only safe while the ts picks out one record.
                None if !unique_ts => continue,
                _ => {}
            }
            let Some(obj) = r.as_object_mut() else { continue };
            for (kk, vv) in f {
                obj.insert(kk.clone(), vv.clone());
            }
            used[i] = true;
            hit = true;
        }
        if hit {
            amended += 1;
        }
    }
    let unmatched = corrections - used.iter().filter(|u| **u).count() as u64;
    (corrections, amended, unmatched)
}

/// Step 2 of the fold: unwound quantity per `open` record, keyed by the
/// `closes_ts` the unwind names.
///
/// Three ways an unwind can fail to close what it claims, all reported rather
/// than absorbed, because each one leaves a closed basket on the open book AND
/// books its P&L on the unwind row — a silent double count:
///   * no `closes_ts`, or one that names nothing (2 on disk today)
///   * a `closes_ts` that names a record which is not `open` — it can never
///     reduce anything, so it is an orphan, not a match
///   * a missing or non-positive `qty`, which would leave the basket fully open.
///     The target's remaining exposure is then unknowable, so it is returned in
///     `unusable` and the target prices as `null` instead of as fully on.
fn unwind_index(records: &[serde_json::Value]) -> (HashMap<Key, f64>, u64, HashSet<Key>) {
    let open_keys: HashSet<Key> = records
        .iter()
        .filter(|r| status_of(r) == "open")
        .filter_map(|r| key_of(rel_of(r), num(r.get("ts"))))
        .collect();
    let mut closed: HashMap<Key, f64> = HashMap::new();
    let mut orphans = 0u64;
    let mut unusable: HashSet<Key> = HashSet::new();
    for r in records {
        if status_of(r) != "unwound" {
            continue;
        }
        match key_of(rel_of(r), num(r.get("closes_ts"))) {
            Some(k) if open_keys.contains(&k) => match num(r.get("qty")) {
                Some(q) if q.is_finite() && q > 0.0 => *closed.entry(k).or_default() += q,
                _ => {
                    unusable.insert(k);
                }
            },
            _ => orphans += 1,
        }
    }
    (closed, orphans, unusable)
}

/// Why a record's P&L could not be established. Specific on purpose: "unknown"
/// with no reason is the same dead end as a wrong number.
fn unhedged_reason(
    n_legs: usize,
    unknown: bool,
    short: bool,
    yes_q: f64,
    no_q: f64,
    q: f64,
) -> String {
    if n_legs == 0 {
        "no legs recorded — nothing to price".into()
    } else if unknown {
        "a leg's side is not readable (e.g. \"mixed\") — cannot tell whether this is hedged".into()
    } else if short {
        "SHORT leg(s): sold or closed, so this collects a credit against an obligation. This \
         view prices long baskets only and will not guess at the net"
            .into()
    } else if n_legs == 1 {
        "NAKED: one leg, so this pays $0.00 or $1.00/contract depending on the outcome".into()
    } else {
        format!(
            "NAKED: legs cover {yes_q} YES against {no_q} NO on {q} contracts — not fully hedged"
        )
    }
}

/// Build the `/api/trades` payload from the ledger text.
pub fn build(ledger_text: &str, fee_category: &str, now_s: f64) -> serde_json::Value {
    let mut cx = Cx::default();
    let sched = FeeSchedule::new(&mut cx);
    let today = today_iso(now_s);

    let mut unparsed = 0u64;
    let mut records: Vec<serde_json::Value> = Vec::new();
    for line in ledger_text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(line) {
            Ok(r) => records.push(r),
            // A torn tail line is normal: the engine appends while we read.
            Err(_) => unparsed += 1,
        }
    }

    let (corrections, corrections_applied, corrections_unmatched) =
        apply_corrections(&mut records);
    let superseded = records.iter().filter(|r| status_of(r) == "superseded").count() as u64;
    records.retain(|r| status_of(r) != "superseded");
    let (unwound_qty, orphan_unwinds, unusable_unwinds) = unwind_index(&records);

    let mut rows: Vec<serde_json::Value> = Vec::new();
    // Open and realized are tracked apart on purpose: adding them would report
    // money already banked as if it were still working.
    let (mut open_n, mut open_ct, mut open_cost, mut open_net) = (0u64, 0.0f64, 0.0f64, 0.0f64);
    // The open positions this file REFUSES to price. Their contracts and their
    // capital are facts and are counted; their profit is not known, so it is
    // absent from open_net_usd rather than guessed.
    let (mut naked_n, mut naked_ct, mut naked_cost) = (0u64, 0.0f64, 0.0f64);
    let (mut real_n, mut real_net, mut real_unpriced) = (0u64, 0.0f64, 0u64);
    let mut closed_by_unwind = 0u64;
    let mut fees_settled_total = 0.0f64;
    let mut fees_modelled_total = 0.0f64;
    let mut split: std::collections::BTreeMap<(String, String), (u64, f64, f64)> =
        Default::default();
    // trades, contracts, counted net, unpriced, closed-by-unwind
    let mut by_strategy: std::collections::BTreeMap<String, (u64, f64, f64, u64, u64)> =
        Default::default();

    for rec in &records {
        let ledger_status = status_of(rec).to_string();
        let rel_id = rel_of(rec).unwrap_or("").to_string();
        // `as_i64()` here read every float qty ("qty": 5.0) as 0 and dropped 45
        // real contracts out of the totals with no warning. The finite filter is
        // not paranoia: one NaN would propagate into `open_contracts`, and
        // serde_json serialises NaN as `null`, so the whole count would vanish
        // rather than the one record.
        let qty_booked = num(rec.get("qty")).filter(|q| q.is_finite()).unwrap_or(0.0);
        let ts = num(rec.get("ts")).unwrap_or(0.0);
        let source = rec.get("source").and_then(|v| v.as_str()).unwrap_or("python").to_string();
        let strategy = rec
            .get("strategy")
            .and_then(|v| v.as_str())
            .unwrap_or("make-take")
            .to_string();
        let empty = vec![];
        let legs = rec.get("legs").and_then(|v| v.as_array()).unwrap_or(&empty);

        // --- fees -----------------------------------------------------------
        // Settled fees beat modelled ones wherever the ledger has them. A
        // record that reports its own fees is bank truth; a modelled number is
        // this dashboard's opinion, and the two must not be presented alike.
        let mut fees = 0.0f64;
        let mut settled = false;
        let mut leg_out = Vec::new();
        let mut derived_cost = 0.0f64;
        let (mut yes_qty, mut no_qty) = (0.0f64, 0.0f64);
        let (mut unknown_leg, mut short_leg) = (false, false);
        for l in legs {
            let venue_s = l.get("venue").and_then(|v| v.as_str()).unwrap_or("").to_string();
            // Older records predate the role field. Say "unrecorded" rather
            // than leaving a blank cell that reads like a missing value in the
            // UI — and never silently default it to maker, which would price
            // the leg at the cheaper coefficient.
            let role_s = match l.get("role").and_then(|v| v.as_str()) {
                Some(r) if !r.is_empty() => r.to_string(),
                _ => "unrecorded".to_string(),
            };
            let side = l.get("side").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let action = l.get("action").and_then(|v| v.as_str()).unwrap_or("").to_string();
            let lqty = num(l.get("qty")).unwrap_or(qty_booked);
            let px = num(l.get("avg_price")).or_else(|| num(l.get("yes_price")));

            let leg_fee = match num(l.get("fees")) {
                Some(f) => {
                    settled = true;
                    f
                }
                None => {
                    // Only an explicit `maker` gets the maker coefficient.
                    // An unrecorded role prices as a TAKER: overstating our
                    // own costs is the safe direction for an accounting view,
                    // and defaulting the other way would quietly flatter it.
                    let role = if role_s == "maker" { Role::Maker } else { Role::Taker };
                    match (Venue::parse(&venue_s), px) {
                        (Some(v), Some(px)) => {
                            let p = cx.parse_exact(&format!("{px}"));
                            let sz = cx.parse_exact(&format!("{lqty}"));
                            let f = sched.fee(&mut cx, v, role, p, sz, fee_category);
                            cx.emit_6dp(f).parse::<f64>().unwrap_or(0.0)
                        }
                        _ => 0.0,
                    }
                }
            };
            fees += leg_fee;

            // What this leg cost per contract. `yes_price` is always quoted on
            // the YES side, so buying NO costs (1 - yes_price). A leg with no
            // readable side, no price, or a SOLD direction contributes nothing
            // and poisons the derivation — silently treating it as 0.0 booked
            // whole notionals as profit, and treating a sale as a purchase
            // booked a guaranteed loss as locked profit.
            let dir = leg_dir(&action, &side);
            match (dir, px) {
                (Leg::LongYes, Some(px)) => {
                    yes_qty += lqty;
                    derived_cost += px * lqty;
                }
                (Leg::LongNo, Some(px)) => {
                    no_qty += lqty;
                    derived_cost += (1.0 - px) * lqty;
                }
                (Leg::Short, _) => short_leg = true,
                _ => unknown_leg = true,
            }

            let e = split.entry((venue_s.clone(), role_s.clone())).or_insert((0, 0.0, 0.0));
            e.0 += 1;
            e.1 += lqty;
            e.2 += leg_fee;

            leg_out.push(serde_json::json!({
                "venue": venue_s,
                "market_id": l.get("market_id").and_then(|v| v.as_str()).unwrap_or(""),
                "side": if side.is_empty() { action.clone() } else { side },
                // `action` was dropped from the payload entirely, so nothing
                // downstream could see that a leg was SOLD. Both it and the
                // resolved direction are now carried.
                "action": action,
                "direction": match dir {
                    Leg::LongYes => "long_yes",
                    Leg::LongNo => "long_no",
                    Leg::Short => "short",
                    Leg::Unknown => "unknown",
                },
                "role": role_s,
                "qty": lqty,
                "price": px,
                "fee_usd": leg_fee,
            }));
        }

        // Hedged means both sides are BOUGHT and each side covers the whole
        // basket. Only then is the $1.00/contract payoff a fact. A sold leg
        // disqualifies the record: the obligation runs the other way and this
        // file does not model credits.
        let hedged = legs.len() >= 2
            && !unknown_leg
            && !short_leg
            && qty_booked > 0.0
            && yes_qty >= qty_booked - EPS
            && no_qty >= qty_booked - EPS;

        // --- fold steps 2 and 3: how much of this basket is still on --------
        let is_open = ledger_status == "open";
        let own_key = if is_open { key_of(Some(&rel_id), Some(ts)) } else { None };
        let closed_qty =
            own_key.as_ref().and_then(|k| unwound_qty.get(k).copied()).unwrap_or(0.0);
        // An unwind named this basket but recorded no usable qty, so how much
        // came off is unknowable. Reporting the basket as fully on would count
        // it once here and again on the unwind row.
        let remainder_unknown = own_key.as_ref().is_some_and(|k| unusable_unwinds.contains(k));
        let remaining = if closed_qty > 0.0 { qty_booked - closed_qty } else { qty_booked };
        let fully_closed = is_open && closed_qty > 0.0 && remaining <= EPS;
        // The displayed status is the FOLDED one. A line still reading `open`
        // that a later record already unwound is the double count itself.
        let status = if fully_closed { "closed".to_string() } else { ledger_status.clone() };
        // A partial unwind returned part of the capital and banked part of the
        // profit on the unwind row. What is left is pro-rata: the cost basis is
        // an average, so scaling is exact.
        let scale = if is_open && qty_booked > 0.0 {
            (remaining / qty_booked).clamp(0.0, 1.0)
        } else {
            1.0
        };

        // --- cost, payoff, profit -------------------------------------------
        // The record's own cost is authoritative and ALL-IN (fees inside).
        // Derived cost is ex-fees, so fees must then be subtracted separately.
        let (cost_full, fees_in_cost) = match num(rec.get("cost_usd")) {
            Some(c) => (Some(c), true),
            None if hedged => (Some(derived_cost), false),
            None => (None, false),
        };
        let payoff_full = if is_open && !hedged {
            // An open directional position has NO known payoff. The writer
            // stamps the maximum ($1.00/contract); printing that as "payoff"
            // beside a null net invites the reader to subtract cost from it,
            // which is exactly the arithmetic that overstated this book.
            None
        } else {
            num(rec.get("payoff_usd")).or(if hedged { Some(qty_booked) } else { None })
        };

        // How the net was established, so an auditor never has to guess whether
        // a number came off the ledger or out of this file.
        let derived_from = if fees_in_cost { "ledger:cost_usd" } else { "derived:leg_prices" };
        let derive = |c: f64, p: f64| if fees_in_cost { p - c } else { p - c - fees };
        let (net_full, net_source, unpriced_reason) = if fully_closed {
            (
                None,
                None,
                Some("closed by a later unwind record — its P&L is on that row".to_string()),
            )
        } else if remainder_unknown {
            (
                None,
                None,
                Some(
                    "an unwind record names this basket but carries no usable qty — how much is \
                     still on cannot be established"
                        .to_string(),
                ),
            )
        } else if is_open {
            if !hedged {
                (
                    None,
                    None,
                    Some(unhedged_reason(
                        legs.len(),
                        unknown_leg,
                        short_leg,
                        yes_qty,
                        no_qty,
                        qty_booked,
                    )),
                )
            } else {
                match (cost_full, payoff_full) {
                    (Some(c), Some(p)) => (Some(derive(c, p)), Some(derived_from), None),
                    _ => (
                        None,
                        None,
                        Some("no cost recorded and none derivable from the legs".to_string()),
                    ),
                }
            }
        } else {
            // A closed record's P&L is bank truth. `profit_usd` and
            // `realized_pnl_usd` agree everywhere both exist.
            match num(rec.get("realized_pnl_usd")).or_else(|| num(rec.get("profit_usd"))) {
                Some(p) => (Some(p), Some("ledger:realized_pnl"), None),
                None => match (hedged, cost_full, payoff_full) {
                    (true, Some(c), Some(p)) => (Some(derive(c, p)), Some(derived_from), None),
                    _ => (
                        None,
                        None,
                        Some(
                            "closed with no realized_pnl_usd — P&L not recoverable from this \
                             record"
                                .to_string(),
                        ),
                    ),
                },
            }
        };

        let cost = cost_full.map(|c| c * scale);
        let payoff = payoff_full.map(|p| p * scale);
        let net = net_full.map(|n| n * scale);

        let (resolves, estimated) = match resolve_date(&rel_id) {
            Some((d, est)) => (Some(d.to_string()), est),
            None => (None, false),
        };
        // APR only means something while the capital is still committed.
        // A realized trade's money is back and earning elsewhere.
        let years = resolves.as_deref().and_then(|d| years_between(&today, d));
        let apr = match (net, cost, years) {
            (Some(n), Some(c), Some(y)) if status == "open" && c > 0.0 && y > 0.0 => {
                Some(n / c / y * 100.0)
            }
            _ => None,
        };

        if settled {
            fees_settled_total += fees;
        } else {
            fees_modelled_total += fees;
        }
        // What this row contributes to the headline. Zero for a row whose P&L
        // is booked somewhere else (a closed basket) or is not known (naked).
        let mut counted = 0.0f64;
        let mut unpriced_here = false;
        match status.as_str() {
            "open" => match net {
                Some(n) => {
                    open_n += 1;
                    open_ct += remaining;
                    open_cost += cost.unwrap_or(0.0);
                    open_net += n;
                    counted = n;
                }
                None => {
                    naked_n += 1;
                    naked_ct += remaining;
                    naked_cost += cost.unwrap_or(0.0);
                    unpriced_here = true;
                }
            },
            "closed" => closed_by_unwind += 1,
            _ => {
                real_n += 1;
                match net {
                    Some(n) => {
                        real_net += n;
                        counted = n;
                    }
                    None => {
                        real_unpriced += 1;
                        unpriced_here = true;
                    }
                }
            }
        }
        let s = by_strategy.entry(strategy.clone()).or_insert((0, 0.0, 0.0, 0, 0));
        s.0 += 1;
        s.1 += qty_booked;
        s.2 += counted;
        if unpriced_here {
            s.3 += 1;
        }
        if status == "closed" {
            s.4 += 1;
        }

        rows.push(serde_json::json!({
            "ts": ts,
            "relationship_id": rel_id,
            "strategy": strategy,
            "source": source,
            "status": status,
            "ledger_status": ledger_status,
            "hedged": hedged,
            "qty": remaining,
            "qty_booked": qty_booked,
            "cost_usd": cost,
            "cost_booked_usd": cost_full,
            "payoff_usd": payoff,
            "fees_usd": fees,
            "fees_settled": settled,
            "net_usd": net,
            "net_source": net_source,
            "unpriced_reason": unpriced_reason,
            "resolves_by": resolves,
            "resolves_estimated": estimated,
            "apr_pct": apr,
            "legs": leg_out,
        }));
    }

    // Newest first: the question on opening this tab is almost always "what
    // did it just do", not "what did it do three weeks ago".
    rows.sort_by(|a, b| {
        let (x, y) = (a["ts"].as_f64().unwrap_or(0.0), b["ts"].as_f64().unwrap_or(0.0));
        y.total_cmp(&x)
    });

    // Capital-weighted, not a mean of per-trade APRs: a $2 trade at 40%/yr and
    // a $200 trade at 5%/yr do not average to 22%. Unpriced rows carry no APR,
    // so they cannot lean on this number either.
    let mut num_w = 0.0f64;
    let mut den_w = 0.0f64;
    for r in &rows {
        if let (Some(c), Some(a)) = (r["cost_usd"].as_f64(), r["apr_pct"].as_f64()) {
            num_w += c * a;
            den_w += c;
        }
    }
    let blended_apr = if den_w > 0.0 { Some(num_w / den_w) } else { None };

    serde_json::json!({
        "as_of": today,
        "totals": {
            "trades": rows.len(),
            // Every open position: priced and unpriced. Contracts and capital
            // are facts whether or not the profit can be established.
            "open_trades": open_n + naked_n,
            "open_contracts": open_ct + naked_ct,
            "open_cost_usd": open_cost + naked_cost,
            // ...but the profit total counts only the hedged, priceable ones.
            "open_net_usd": open_net,
            "open_priced_trades": open_n,
            "open_unpriced_trades": naked_n,
            "open_unpriced_contracts": naked_ct,
            "open_unpriced_cost_usd": naked_cost,
            "realized_trades": real_n,
            "realized_net_usd": real_net,
            "realized_unpriced_trades": real_unpriced,
            "closed_by_unwind": closed_by_unwind,
            "unpriced_rows": naked_n + real_unpriced,
            "fees_settled_usd": fees_settled_total,
            "fees_modelled_usd": fees_modelled_total,
            "blended_apr_pct": blended_apr,
        },
        "by_venue_role": split.into_iter().map(|((v, r), (legs, ct, fee))| {
            serde_json::json!({"venue": v, "role": r, "legs": legs,
                               "contracts": ct, "fees_usd": fee})
        }).collect::<Vec<_>>(),
        // `net_usd` sums to open_net_usd + realized_net_usd exactly: a row whose
        // P&L is unknown or already booked elsewhere contributes 0 and is
        // counted in `unpriced`/`closed_by_unwind` instead.
        "by_strategy": by_strategy.into_iter().map(|(k, (n, ct, net, unpriced, closed))| {
            serde_json::json!({"strategy": k, "trades": n, "contracts": ct, "net_usd": net,
                               "unpriced": unpriced, "closed_by_unwind": closed})
        }).collect::<Vec<_>>(),
        "fee_note": format!(
            "settled fees come from the ledger; modelled fees are priced by arb_core::fees at \
             category '{fee_category}' for records the engine booked with fees_pending"
        ),
        "corrections": corrections,
        "corrections_applied": corrections_applied,
        // A correction that reaches no record is a RETRACTION THROWN AWAY, and
        // the number it retracted is still on the screen. Nothing in the payload
        // distinguished that from legitimate coalescing (three corrections on one
        // fedcut record, last-wins), so a dropped retraction was invisible.
        "corrections_unmatched": corrections_unmatched,
        "superseded": superseded,
        "orphan_unwinds": orphan_unwinds,
        "unusable_unwinds": unusable_unwinds.len(),
        "unparsed_lines": unparsed,
        "rows": rows,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The take-take this engine made on 2026-07-28: sell PM YES @0.08, buy
    /// Kalshi YES @0.04 on 5 contracts. Engine shape — fees_pending, so cost
    /// must be derived and fees modelled.
    const ENGINE: &str = r#"{"ts":1785250000.0,"relationship_id":"xvus-nobel-peace-26-donaldtrump","qty":5,"strategy":"take-take","status":"open","source":"arb-trader","fees_pending":true,"legs":[{"venue":"polymarket_us","market_id":"tac-nobel-peace-2026-10-09-dontru","side":"ask","role":"taker","qty":5,"yes_price":"0.0800"},{"venue":"kalshi","market_id":"KXNOBELPEACE-26-DJT","side":"bid","role":"taker","qty":5,"yes_price":"0.0400"}]}"#;

    /// A real Python-era record: buy_yes + buy_no, settled per-leg fees, and
    /// an all-in `cost_usd`.
    const PYTHON: &str = r#"{"ts":1784646659.716,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","qty":50,"status":"open","legs":[{"venue":"kalshi","market_id":"KXFRENCHPRES-27-JMEL","action":"buy_yes","qty":50,"avg_price":"0.1100","fees":"0.3427","role":"taker","side":"yes"},{"venue":"polymarket_us","market_id":"ewc-pres-fra-2027-04-11-jeamel","action":"buy_no","qty":50,"yes_price":"0.2500","role":"taker","side":"no"}],"cost_usd":43.9027,"payoff_usd":50.0}"#;

    /// Real shape, `data/exec/trades.jsonl`: an `unwound` record with NO legs.
    /// Its own `realized_pnl_usd` is a $1.08 LOSS. Booking `payoff = qty` with
    /// `cost = 0` reported it as +$15.00.
    const NOLEGS_UNWIND: &str = r#"{"ts":1784950000.0,"relationship_id":"pmm-aec-atp-jaifar-gonbue-2026-07-22","strategy":"unwind","status":"unwound","closes_ts":1784900000.0,"qty":15,"realized_pnl_usd":-1.0756,"note":"exit fills"}"#;

    /// Real shape: a naked settlement whose own note says the cost was $2.16
    /// and it paid nothing. The dash displayed +$4.00.
    const NAKED_SETTLE: &str = r#"{"ts":1784960000.0,"relationship_id":"sports-mlb-WSH@COL","strategy":"naked-settlement","status":"realized","qty":4,"realized_pnl_usd":-2.16,"kalshi_result":"no","note":"yes cost $2.16 paid 0"}"#;

    /// Real shape: 5 naked long Kalshi YES @0.17. One leg, so the payoff is
    /// $0.00 or $5.00 and nobody knows which.
    const NAKED_OPEN: &str = r#"{"ts":1784970000.0,"relationship_id":"mltox-KXPRESNOMD-28-GN","strategy":"ml-toxicity-probe","status":"open","qty":5,"cost_usd":0.85,"legs":[{"venue":"kalshi","market_id":"KXPRESNOMD-28-GN","side":"yes","role":"maker","qty":5,"avg_price":"0.17"}]}"#;

    /// Real shape: every `sports-*` settlement carries a FLOAT qty.
    const FLOAT_QTY: &str = r#"{"ts":1784980000.0,"relationship_id":"sports-rehedge-Tamara Korpatsch@Julia Stusek","strategy":"settlement","status":"unwound","closes_ts":1784979000.0,"qty":2.0,"proceeds_usd":1.4,"realized_pnl_usd":0.14}"#;

    /// Real shape: 7 corrections on disk say "this record is a duplicate".
    const SUPERSEDE: &str = r#"{"ts":1784990000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"correction","corrects_ts":1784646659.716,"reason":"duplicate of the accurate record","fields":{"status":"superseded"}}"#;

    fn totals(out: &serde_json::Value, k: &str) -> f64 {
        out["totals"][k].as_f64().unwrap_or_else(|| panic!("totals.{k} missing or not a number"))
    }

    #[test]
    fn engine_shape_derives_cost_and_models_fees() {
        let out = build(ENGINE, "default", 1785250000.0);
        let r = &out["rows"][0];
        // buy Kalshi YES @0.04 + sell PM YES @0.08 (== buy NO @0.92) = 0.96/ct
        assert!((r["cost_usd"].as_f64().unwrap() - 4.80).abs() < 1e-9);
        assert_eq!(r["payoff_usd"], 5.0);
        assert_eq!(r["fees_settled"], false, "engine records are fees_pending");
        assert!(r["fees_usd"].as_f64().unwrap() > 0.0);
        // net = payoff - cost - modelled fees, and must be under the 0.20 gross
        let net = r["net_usd"].as_f64().unwrap();
        assert!(net > 0.0 && net < 0.20, "net {net} should be a shaved 0.20");
    }

    /// The regression that made the whole tab wrong: pricing a Python record
    /// with the engine's rules reported the book in the red.
    #[test]
    fn python_shape_uses_its_own_settled_cost_and_fees() {
        let out = build(PYTHON, "default", 1784646659.0);
        let r = &out["rows"][0];
        assert_eq!(r["fees_settled"], true, "the ledger reported real fees");
        assert!((r["cost_usd"].as_f64().unwrap() - 43.9027).abs() < 1e-9);
        // cost_usd is ALL-IN, so fees must NOT be subtracted a second time
        assert!((r["net_usd"].as_f64().unwrap() - 6.0973).abs() < 1e-6);
    }

    #[test]
    fn both_shapes_together_are_in_profit() {
        let out = build(&format!("{PYTHON}\n{ENGINE}"), "default", 1785250000.0);
        let t = &out["totals"];
        assert_eq!(t["open_trades"], 2);
        assert!(t["open_net_usd"].as_f64().unwrap() > 0.0, "this book is in profit");
    }

    /// Realized money is back in the account; counting it as open would report
    /// banked profit as if it were still working. This record is hedged and
    /// carries both cost and payoff, so its P&L is still derivable without a
    /// `realized_pnl_usd` field.
    #[test]
    fn realized_is_kept_apart_from_open() {
        let done = PYTHON.replace(r#""status":"open""#, r#""status":"realized""#);
        let out = build(&format!("{ENGINE}\n{done}"), "default", 1785250000.0);
        let t = &out["totals"];
        assert_eq!(t["open_trades"], 1);
        assert_eq!(t["realized_trades"], 1);
        assert!(t["realized_net_usd"].as_f64().unwrap() > 0.0);
    }

    /// A correction is a compensating append, not a position.
    #[test]
    fn corrections_are_not_positions() {
        let c = PYTHON.replace(r#""status":"open""#, r#""status":"correction""#);
        let out = build(&format!("{ENGINE}\n{c}"), "default", 1785250000.0);
        assert_eq!(out["totals"]["trades"], 1);
        assert_eq!(out["corrections"], 1);
    }

    /// APR is only meaningful while the capital is still committed.
    #[test]
    fn only_open_positions_carry_an_apr() {
        let done = ENGINE.replace(r#""status":"open""#, r#""status":"realized""#);
        let out = build(&done, "default", 1785250000.0);
        assert!(out["rows"][0]["apr_pct"].is_null(), "realized money earns nothing here");
    }

    #[test]
    fn role_changes_the_modelled_fee() {
        let taker = build(ENGINE, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
        let as_maker = ENGINE.replace(r#""role":"taker""#, r#""role":"maker""#);
        let maker =
            build(&as_maker, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
        assert!(maker < taker, "maker legs must not pay the taker fee ({maker} vs {taker})");
    }

    #[test]
    fn splits_make_and_take_by_venue() {
        let out = build(ENGINE, "default", 1785250000.0);
        let split = out["by_venue_role"].as_array().unwrap();
        assert_eq!(split.len(), 2);
        for s in split {
            assert_eq!(s["role"], "taker");
            assert_eq!(s["contracts"], 5.0);
        }
    }

    #[test]
    fn a_torn_tail_line_does_not_lose_the_rest() {
        let out = build(&format!("{ENGINE}\n{{\"ts\":1.0,\"relat"), "default", 1785250000.0);
        assert_eq!(out["totals"]["trades"], 1);
        assert_eq!(out["unparsed_lines"], 1);
    }

    // ----- F1: a record with no legs is not free money ----------------------

    /// Was: `have_derived` stayed true through an empty leg loop, so cost was
    /// $0.00 and payoff was qty — the whole notional booked as profit.
    #[test]
    fn a_closed_record_with_no_legs_uses_its_own_realized_pnl() {
        let out = build(NOLEGS_UNWIND, "default", 1785250000.0);
        let r = &out["rows"][0];
        assert!(
            (r["net_usd"].as_f64().unwrap() + 1.0756).abs() < 1e-9,
            "must be the ledger's -1.0756 loss, not +15.00: {}",
            r["net_usd"]
        );
        assert_eq!(r["net_source"], "ledger:realized_pnl");
        assert!((totals(&out, "realized_net_usd") + 1.0756).abs() < 1e-9);
    }

    /// The single worst row on the live board: its own note says "yes cost
    /// $2.16 paid 0" and it displayed as +$4.00.
    #[test]
    fn a_naked_settlement_shows_its_loss_not_its_notional() {
        let out = build(NAKED_SETTLE, "default", 1785250000.0);
        let r = &out["rows"][0];
        assert!((r["net_usd"].as_f64().unwrap() + 2.16).abs() < 1e-9, "got {}", r["net_usd"]);
        assert!(totals(&out, "realized_net_usd") < 0.0);
    }

    /// A closed record the ledger never priced must be blank, not zero-cost.
    #[test]
    fn a_closed_record_with_no_pnl_at_all_is_null_and_counted() {
        let rec = r#"{"ts":1784905478.772228,"relationship_id":"mltox-KXPRESNOMD-28-GN","strategy":"ml-toxicity-probe","status":"unwound","closes_ts":null,"qty":4,"note":"manual flatten"}"#;
        let out = build(rec, "default", 1785250000.0);
        assert!(out["rows"][0]["net_usd"].is_null(), "no P&L is recoverable from this record");
        assert!(!out["rows"][0]["unpriced_reason"].is_null());
        assert_eq!(out["totals"]["realized_unpriced_trades"], 1);
        assert_eq!(totals(&out, "realized_net_usd"), 0.0);
        assert_eq!(out["orphan_unwinds"], 1, "a null closes_ts closes nothing");
    }

    // ----- F2: the unwound / closes_ts fold --------------------------------

    /// 51 of 52 unwind records on disk pointed at a line still marked `open`,
    /// so 265 contracts and $212 of capital were counted twice.
    #[test]
    fn a_fully_unwound_basket_leaves_the_open_book() {
        let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":50,"realized_pnl_usd":2.0}"#;
        let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
        let t = &out["totals"];
        assert_eq!(t["open_trades"], 0, "the basket is closed");
        assert_eq!(totals(&out, "open_contracts"), 0.0);
        assert_eq!(totals(&out, "open_cost_usd"), 0.0, "$43.90 is back in the account");
        assert_eq!(totals(&out, "open_net_usd"), 0.0);
        assert_eq!(t["closed_by_unwind"], 1);
        assert_eq!(totals(&out, "realized_net_usd"), 2.0, "counted once, on the unwind row");
        // the open line is still visible as history, but folded to `closed`
        let closed =
            out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "closed").unwrap();
        assert_eq!(closed["ledger_status"], "open", "the ledger line is never rewritten");
        assert!(closed["net_usd"].is_null());
    }

    /// The live case: 24 of 50 melenchon contracts were unwound, and the dash
    /// still showed 50 working and $43.90 tied up.
    #[test]
    fn a_partial_unwind_pro_rates_the_remainder() {
        let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":24,"proceeds_usd":22.56,"realized_pnl_usd":1.5}"#;
        let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
        assert_eq!(totals(&out, "open_contracts"), 26.0, "50 booked, 24 unwound");
        let want_cost = 43.9027 * 26.0 / 50.0;
        assert!((totals(&out, "open_cost_usd") - want_cost).abs() < 1e-9);
        let want_net = 6.0973 * 26.0 / 50.0;
        assert!((totals(&out, "open_net_usd") - want_net).abs() < 1e-6);
        let open = out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "open").unwrap();
        assert_eq!(open["qty"], 26.0);
        assert_eq!(open["qty_booked"], 50.0);
    }

    /// An unwind matches ONE basket by (relationship_id, ts) — matching every
    /// basket on the relationship would free exposure that is still on.
    #[test]
    fn an_unwind_only_closes_the_basket_it_names() {
        let other = PYTHON.replace("1784646659.716", "1784646999.0");
        let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"unwound","closes_ts":1784646659.716,"qty":50,"realized_pnl_usd":0.0}"#;
        let out = build(&format!("{PYTHON}\n{other}\n{unwind}"), "default", 1785250000.0);
        assert_eq!(totals(&out, "open_contracts"), 50.0, "the other basket is untouched");
    }

    // ----- F3: naked legs are not locked profit ---------------------------

    /// 9 open records had exactly one leg and contributed 49% of all reported
    /// "locked" profit. A one-leg record is a directional bet.
    #[test]
    fn a_single_leg_open_position_is_unpriced_not_locked() {
        let out = build(NAKED_OPEN, "default", 1785250000.0);
        let r = &out["rows"][0];
        assert_eq!(r["hedged"], false);
        assert!(r["net_usd"].is_null(), "a naked bet has no locked profit: {}", r["net_usd"]);
        assert!(r["payoff_usd"].is_null(), "and no known payoff either");
        assert!(r["apr_pct"].is_null());
        assert!(r["unpriced_reason"].as_str().unwrap().contains("NAKED"));
        let t = &out["totals"];
        assert_eq!(totals(&out, "open_net_usd"), 0.0, "excluded from the headline");
        assert_eq!(t["open_unpriced_trades"], 1);
        // ...but its capital and its contracts are real and still counted
        assert_eq!(totals(&out, "open_contracts"), 5.0);
        assert!((totals(&out, "open_cost_usd") - 0.85).abs() < 1e-9);
        assert!(t["blended_apr_pct"].is_null(), "nothing priceable to average");
    }

    /// `ml-toxicity-probe +$49.75` was the top line on the strategy board and
    /// was 25 naked Kalshi contracts costing $4.25.
    #[test]
    fn a_naked_strategy_does_not_top_the_board() {
        let five = (0..5)
            .map(|i| NAKED_OPEN.replace("1784970000.0", &format!("17849700{i:02}.0")))
            .collect::<Vec<_>>()
            .join("\n");
        let out = build(&five, "default", 1785250000.0);
        let s = out["by_strategy"].as_array().unwrap();
        assert_eq!(s.len(), 1);
        assert_eq!(s[0]["net_usd"], 0.0, "not +49.75");
        assert_eq!(s[0]["unpriced"], 5);
    }

    /// Two legs on the SAME side is not a hedge either.
    #[test]
    fn two_legs_on_the_same_side_are_not_hedged() {
        let same = ENGINE.replace(r#""side":"ask""#, r#""side":"bid""#);
        let out = build(&same, "default", 1785250000.0);
        assert_eq!(out["rows"][0]["hedged"], false);
        assert!(out["rows"][0]["net_usd"].is_null());
    }

    // ----- direction: a SOLD leg is a credit, not a purchase ----------------

    /// The latent money bug. Selling YES @0.10 and selling NO @0.70 collects
    /// $0.80 against the $1.00 it owes — a guaranteed 20c/ct LOSS. Reading only
    /// `side` priced it as `hedged: true, net +1.804, apr 52.8%` and put it top
    /// of the strategy board: the same failure class as pricing a naked leg as
    /// locked profit.
    #[test]
    fn selling_both_sides_is_not_a_locked_profit() {
        let rec = r#"{"ts":1784990001.0,"relationship_id":"xvus-fedcut-26-usfed-2026-cut","strategy":"take-take","status":"open","qty":10,"legs":[{"venue":"kalshi","market_id":"KXRATECUT-26DEC31","side":"yes","action":"sell","qty":10,"yes_price":"0.1000"},{"venue":"polymarket_us","market_id":"cbpac-usfed-2026-cut","side":"no","action":"sell","qty":10,"yes_price":"0.3000"}]}"#;
        let out = build(rec, "default", 1785250000.0);
        let r = &out["rows"][0];
        assert_eq!(r["hedged"], false, "both legs were SOLD");
        assert!(
            r["net_usd"].is_null(),
            "a credit-and-obligation is not locked profit: {}",
            r["net_usd"]
        );
        assert!(r["cost_usd"].is_null(), "no cost may be derived from sold legs");
        assert!(r["apr_pct"].is_null());
        assert!(r["unpriced_reason"].as_str().unwrap().contains("SHORT"));
        assert_eq!(totals(&out, "open_net_usd"), 0.0);
        assert_eq!(out["totals"]["open_unpriced_trades"], 1);
    }

    /// The real disk shape: 54 legs across 28 records carry `action: "sell"`.
    /// This one rendered `cost_usd 41.00 / payoff_usd 41.00` for a record whose
    /// two legs were both sold at 0.19. Its net comes off the ledger and is
    /// unaffected; the cost and payoff columns were fiction.
    #[test]
    fn a_sold_exit_record_reports_no_derived_cost_or_payoff() {
        let rec = r#"{"ts":1784728156.5523076,"relationship_id":"xvus-fedcut-26-usfed-2026-cut","strategy":"unwind","status":"unwound","closes_ts":1784646768.338,"qty":41,"legs":[{"venue":"kalshi","market_id":"KXRATECUT-26DEC31","side":"yes","action":"sell","qty":41,"yes_price":"0.1900"},{"venue":"polymarket_us","market_id":"cbpac-usfed-2026-cut","side":"no","action":"sell","qty":41,"yes_price":"0.1900"}],"proceeds_usd":41.0,"realized_pnl_usd":1.1583}"#;
        let out = build(rec, "default", 1785250000.0);
        let r = &out["rows"][0];
        assert_eq!(r["hedged"], false);
        assert!(r["cost_usd"].is_null(), "41.00 was invented from 0.19 + (1-0.19)");
        assert!(r["payoff_usd"].is_null(), "41.00 was invented from qty");
        assert!((r["net_usd"].as_f64().unwrap() - 1.1583).abs() < 1e-9, "ledger truth stands");
        assert_eq!(r["net_source"], "ledger:realized_pnl");
    }

    /// `action` was dropped from the payload entirely, so nothing downstream
    /// could tell a bought leg from a sold one.
    #[test]
    fn the_payload_carries_leg_direction() {
        let out = build(PYTHON, "default", 1785250000.0);
        let legs = out["rows"][0]["legs"].as_array().unwrap();
        assert_eq!(legs[0]["action"], "buy_yes");
        assert_eq!(legs[0]["direction"], "long_yes");
        assert_eq!(legs[1]["direction"], "long_no");
        let sold = PYTHON.replace(r#""action":"buy_yes""#, r#""action":"sell""#);
        let out = build(&sold, "default", 1785250000.0);
        assert_eq!(out["rows"][0]["legs"][0]["direction"], "short");
    }

    /// Buying to close a short is not an opening debit either.
    #[test]
    fn a_close_via_buy_leg_is_a_short_not_a_purchase() {
        let rec = PYTHON.replace(r#""action":"buy_no""#, r#""action":"close_via_buy_short""#);
        let out = build(&rec, "default", 1785250000.0);
        assert_eq!(out["rows"][0]["hedged"], false);
        assert_eq!(out["rows"][0]["legs"][1]["direction"], "short");
    }

    // ----- unwind records that cannot do their job -------------------------

    /// An unwind with no usable qty would leave the basket fully open AND book
    /// its own P&L: the double count this fold exists to prevent. The remainder
    /// is unknowable, so the basket must price as null, not as fully on.
    #[test]
    fn an_unwind_with_no_usable_qty_makes_the_remainder_unknown() {
        let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"realized_pnl_usd":1.5}"#;
        let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
        assert_eq!(out["unusable_unwinds"], 1);
        let open = out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "open").unwrap();
        assert!(open["net_usd"].is_null(), "how much is still on is unknown");
        assert!(open["unpriced_reason"].as_str().unwrap().contains("no usable qty"));
        assert_eq!(totals(&out, "open_net_usd"), 0.0, "not counted as fully open");
        assert!((totals(&out, "realized_net_usd") - 1.5).abs() < 1e-9);
    }

    /// An unwind naming a record that is not `open` can never reduce anything,
    /// so it is an orphan — counting it as matched hid a live basket.
    #[test]
    fn an_unwind_naming_a_non_open_record_is_an_orphan() {
        let first = r#"{"ts":1784700000.0,"relationship_id":"r1","strategy":"unwind","status":"unwound","closes_ts":1784600000.0,"qty":5,"realized_pnl_usd":0.5}"#;
        let second = r#"{"ts":1784700001.0,"relationship_id":"r1","strategy":"unwind","status":"unwound","closes_ts":1784700000.0,"qty":5,"realized_pnl_usd":0.25}"#;
        let out = build(&format!("{first}\n{second}"), "default", 1785250000.0);
        assert_eq!(out["orphan_unwinds"], 2, "neither names an open basket");
    }

    /// The maker probe writes `side: "mixed"`. Guessing which way it leans is
    /// how a directional position becomes a riskless one on the screen.
    #[test]
    fn an_unreadable_leg_side_is_never_guessed() {
        let rec = r#"{"ts":1784970000.0,"relationship_id":"pmm-aec-atp-jaifar-gonbue-2026-07-22","strategy":"pmus-maker-probe","status":"open","qty":15,"legs":[{"venue":"polymarket_us","market_id":"aec-atp-jaifar-gonbue-2026-07-22","side":"mixed","role":"maker+taker","qty":15}]}"#;
        let out = build(rec, "default", 1785250000.0);
        assert_eq!(out["rows"][0]["hedged"], false);
        assert!(out["rows"][0]["net_usd"].is_null());
        assert!(out["rows"][0]["cost_usd"].is_null(), "no price, so no cost may be invented");
        assert!(out["rows"][0]["unpriced_reason"].as_str().unwrap().contains("not readable"));
    }

    // ----- F4: corrections amend, they do not vanish ------------------------

    /// A correction carries a `fields` object that AMENDS its target. Dropping
    /// it leaves the value the ledger explicitly retracted on the screen.
    #[test]
    fn a_correction_amends_the_record_it_names() {
        let fix = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"correction","corrects_ts":1784646659.716,"reason":"actual PM fill 0.70, not the 0.76 limit","fields":{"cost_usd":40.0}}"#;
        let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
        assert_eq!(out["corrections"], 1);
        assert_eq!(out["corrections_applied"], 1);
        assert!((totals(&out, "open_cost_usd") - 40.0).abs() < 1e-9, "the corrected cost wins");
        assert!((totals(&out, "open_net_usd") - 10.0).abs() < 1e-9);
    }

    /// Two of the 45 corrections on disk carry NO `relationship_id`. Keying on
    /// `(relationship_id, corrects_ts)` discarded both, and they retract
    /// `realized_pnl_usd` from +2.6369 to -0.2348 and from +4.1308 to -0.3660 —
    /// $7.3685 of overstated realized P&L, counted in `corrections` and then
    /// silently thrown away.
    #[test]
    fn a_correction_without_a_relationship_id_still_finds_its_target() {
        let target = r#"{"ts":1784815976.3063905,"relationship_id":"pmm-aec-itfme-vindul-rapper-2026-07-23","strategy":"pmus-maker-probe","status":"unwound","closes_ts":1784800000.0,"qty":5,"realized_pnl_usd":2.6369}"#;
        let fix = r#"{"ts":1784818208.5212176,"strategy":"pmus-maker-probe","status":"correction","corrects_ts":1784815976.3063905,"fields":{"realized_pnl_usd":-0.2348,"note":"CORRECTED from +2.6369 (bad entry-price assumption)"}}"#;
        let out = build(&format!("{target}\n{fix}"), "default", 1785250000.0);
        assert!(
            (totals(&out, "realized_net_usd") + 0.2348).abs() < 1e-9,
            "the retraction must win, not the +2.6369 it retracts: {}",
            out["totals"]["realized_net_usd"]
        );
        assert_eq!(out["corrections_applied"], 1);
        assert_eq!(out["corrections_unmatched"], 0);
    }

    /// A correction that reaches nothing is a retraction thrown away, and the
    /// value it retracted is still on the screen. It must be counted so the page
    /// can say so — silence here is what hid the two above.
    #[test]
    fn a_correction_that_reaches_no_record_is_reported() {
        let fix = r#"{"ts":1784818208.5212176,"status":"correction","corrects_ts":1700000000.0,"fields":{"realized_pnl_usd":-9.99}}"#;
        let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
        assert_eq!(out["corrections"], 1);
        assert_eq!(out["corrections_applied"], 0);
        assert_eq!(out["corrections_unmatched"], 1);
    }

    /// A correction that names a relationship must name the target's own —
    /// dropping the pair check entirely would let one amend another's record.
    #[test]
    fn a_correction_naming_the_wrong_relationship_is_not_applied() {
        let fix = r#"{"ts":1784818208.0,"relationship_id":"some-other-thing","status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":1.0}}"#;
        let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
        assert!((totals(&out, "open_cost_usd") - 43.9027).abs() < 1e-9, "target untouched");
        assert_eq!(out["corrections_unmatched"], 1);
    }

    /// Several corrections on one target coalesce in file order — three do on
    /// disk. That is legitimate, and must NOT count as unmatched.
    #[test]
    fn corrections_on_one_target_coalesce_last_wins() {
        let a = r#"{"ts":1784700001.0,"status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":10.0}}"#;
        let b = r#"{"ts":1784700002.0,"status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":20.0}}"#;
        let out = build(&format!("{PYTHON}\n{a}\n{b}"), "default", 1785250000.0);
        assert!((totals(&out, "open_cost_usd") - 20.0).abs() < 1e-9, "the later correction wins");
        assert_eq!(out["corrections_applied"], 1, "one record amended");
        assert_eq!(out["corrections_unmatched"], 0, "both reached the target");
    }

    /// 7 corrections on disk say `status: superseded` — "this line is a
    /// duplicate". All 7 were still being counted as positions.
    #[test]
    fn a_superseded_record_is_removed_from_the_book() {
        let out = build(&format!("{PYTHON}\n{SUPERSEDE}"), "default", 1785250000.0);
        assert_eq!(out["superseded"], 1);
        assert_eq!(out["totals"]["trades"], 0, "the duplicate is gone, not repriced");
        assert_eq!(totals(&out, "open_contracts"), 0.0);
        assert_eq!(totals(&out, "open_net_usd"), 0.0);
    }

    // ----- F5: float qty --------------------------------------------------

    /// `as_i64()` returns None for `2.0`, so qty read as 0, cost as None and
    /// the record fell out of every total behind a `net_usd: null`.
    #[test]
    fn a_float_qty_is_not_read_as_zero() {
        let out = build(FLOAT_QTY, "default", 1785250000.0);
        assert_eq!(out["rows"][0]["qty"], 2.0);
        assert!((totals(&out, "realized_net_usd") - 0.14).abs() < 1e-9);
        let s = &out["by_strategy"][0];
        assert_eq!(s["contracts"], 2.0, "45 real contracts were invisible");
        assert!((s["net_usd"].as_f64().unwrap() - 0.14).abs() < 1e-9);
    }

    /// One poisoned qty must cost one row, not the whole count: `open_contracts`
    /// is a sum, and serde_json writes a NaN sum as `null`.
    #[test]
    fn a_non_finite_qty_cannot_poison_the_totals() {
        let bad = NAKED_OPEN.replace(r#""qty":5,"cost_usd""#, r#""qty":"NaN","cost_usd""#);
        let out = build(&format!("{PYTHON}\n{bad}"), "default", 1785250000.0);
        assert_eq!(totals(&out, "open_contracts"), 50.0, "the good basket survives");
        assert!((totals(&out, "open_net_usd") - 6.0973).abs() < 1e-6);
        assert_eq!(out["totals"]["open_unpriced_trades"], 1);
    }

    #[test]
    fn a_float_qty_on_a_leg_is_not_read_as_zero() {
        let f = ENGINE.replace(r#""qty":5,"yes_price":"0.0800""#, r#""qty":5.0,"yes_price":"0.0800""#);
        let out = build(&f, "default", 1785250000.0);
        let legs = out["rows"][0]["legs"].as_array().unwrap();
        assert_eq!(legs[0]["qty"], 5.0);
        assert!((out["rows"][0]["cost_usd"].as_f64().unwrap() - 4.80).abs() < 1e-9);
    }

    // ----- the whole-file invariant ---------------------------------------

    /// The strategy table must decompose the headline exactly. If it does not,
    /// one of the two is counting something the other is not.
    #[test]
    fn by_strategy_sums_to_the_headline() {
        let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":24,"realized_pnl_usd":1.5}"#;
        let text = format!(
            "{PYTHON}\n{ENGINE}\n{unwind}\n{NOLEGS_UNWIND}\n{NAKED_SETTLE}\n{NAKED_OPEN}\n\
             {FLOAT_QTY}"
        );
        let out = build(&text, "default", 1785250000.0);
        let want = totals(&out, "open_net_usd") + totals(&out, "realized_net_usd");
        let got: f64 = out["by_strategy"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| s["net_usd"].as_f64().unwrap())
            .sum();
        assert!((got - want).abs() < 1e-9, "by_strategy {got} vs totals {want}");
    }

    /// No row may claim a net without a source for it, and no row may carry a
    /// null net without saying why. That pair of rules is the whole fix.
    #[test]
    fn every_row_either_has_a_priced_net_or_says_why_not() {
        let text = format!(
            "{PYTHON}\n{ENGINE}\n{NOLEGS_UNWIND}\n{NAKED_SETTLE}\n{NAKED_OPEN}\n{FLOAT_QTY}\n\
             {SUPERSEDE}"
        );
        let out = build(&text, "default", 1785250000.0);
        for r in out["rows"].as_array().unwrap() {
            if r["net_usd"].is_null() {
                assert!(
                    r["unpriced_reason"].is_string(),
                    "silent null on {}: {r}",
                    r["relationship_id"]
                );
                assert!(r["apr_pct"].is_null(), "an unpriced row cannot have an APR");
            } else {
                assert!(r["net_source"].is_string(), "unsourced net on {}", r["relationship_id"]);
            }
        }
    }
}
