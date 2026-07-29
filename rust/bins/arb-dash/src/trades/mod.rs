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
//!
//! The work splits four ways, and every one of them is a place a wrong answer
//! has already cost money: `fold` decides what is still on, `legs` decides
//! what each leg did, `pricing` turns that into dollars or into a stated
//! refusal, and `totals` adds up only what may be added up.

mod fold;
mod legs;
mod pricing;
mod totals;

#[cfg(test)]
mod tests;

use arb_core::resolve::{resolve_date, today_iso, years_between};

use fold::Ledger;
use legs::{Legs, Modeller, Splits};
use pricing::Priced;
use totals::Totals;

/// Contract counts are compared, never trusted to be integral — `qty` is a
/// float in half this file (`2.0`, `5.0`).
const EPS: f64 = 1e-9;

fn num(v: Option<&serde_json::Value>) -> Option<f64> {
    let v = v?;
    v.as_f64().or_else(|| v.as_str().and_then(|s| s.parse().ok()))
}

fn rel_of(r: &serde_json::Value) -> Option<&str> {
    r.get("relationship_id").and_then(|v| v.as_str())
}

fn status_of(r: &serde_json::Value) -> &str {
    r.get("status").and_then(|v| v.as_str()).unwrap_or("open")
}

/// One ledger record, folded, read and priced. The row the tab renders and the
/// numbers the totals add are the SAME object, so the two cannot disagree.
struct Trade {
    ts: f64,
    relationship_id: String,
    strategy: String,
    source: String,
    /// The FOLDED status, not the one on disk.
    status: String,
    ledger_status: String,
    hedged: bool,
    /// Contracts still working, after any partial unwind.
    qty: f64,
    qty_booked: f64,
    fees: f64,
    fees_settled: bool,
    priced: Priced,
    resolves: Option<String>,
    resolves_estimated: bool,
    apr: Option<f64>,
    legs: Vec<serde_json::Value>,
}

impl Trade {
    fn to_json(&self) -> serde_json::Value {
        serde_json::json!({
            "ts": self.ts,
            "relationship_id": self.relationship_id,
            "strategy": self.strategy,
            "source": self.source,
            "status": self.status,
            "ledger_status": self.ledger_status,
            "hedged": self.hedged,
            "qty": self.qty,
            "qty_booked": self.qty_booked,
            "cost_usd": self.priced.cost,
            "cost_booked_usd": self.priced.cost_booked,
            "payoff_usd": self.priced.payoff,
            "fees_usd": self.fees,
            "fees_settled": self.fees_settled,
            "net_usd": self.priced.net,
            "net_source": self.priced.source,
            "unpriced_reason": self.priced.unpriced_reason,
            "resolves_by": self.resolves,
            "resolves_estimated": self.resolves_estimated,
            "apr_pct": self.apr,
            "legs": self.legs,
        })
    }
}

/// Build the `/api/trades` payload from the ledger text.
pub fn build(ledger_text: &str, fee_category: &str, now_s: f64) -> serde_json::Value {
    let mut modeller = Modeller::new();
    let today = today_iso(now_s);
    let ledger = Ledger::fold(ledger_text);

    let mut rows: Vec<serde_json::Value> = Vec::new();
    let mut splits = Splits::default();
    let mut totals = Totals::default();

    for rec in &ledger.records {
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
        let raw_legs = rec.get("legs").and_then(|v| v.as_array()).unwrap_or(&empty);

        let legs = Legs::read(&mut modeller, raw_legs, qty_booked, fee_category, &mut splits);
        let hedged = pricing::hedged(&legs, qty_booked);
        let rem = ledger.remainder(&rel_id, ts, &ledger_status, qty_booked);
        let priced = pricing::price(rec, &legs, hedged, qty_booked, &rem);

        let (resolves, resolves_estimated) = match resolve_date(&rel_id) {
            Some((d, est)) => (Some(d.to_string()), est),
            None => (None, false),
        };
        let years = resolves.as_deref().and_then(|d| years_between(&today, d));
        let apr = pricing::apr(&priced, &rem.status, years);

        let trade = Trade {
            ts,
            relationship_id: rel_id,
            strategy,
            source,
            status: rem.status,
            ledger_status,
            hedged,
            qty: rem.qty,
            qty_booked,
            fees: legs.fees,
            fees_settled: legs.settled,
            priced,
            resolves,
            resolves_estimated,
            apr,
            legs: legs.payload,
        };
        totals.add(&trade);
        rows.push(trade.to_json());
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
        "totals": totals.headline(rows.len(), blended_apr),
        "by_venue_role": splits.to_json(),
        "by_strategy": totals.by_strategy(),
        "fee_note": format!(
            "settled fees come from the ledger; modelled fees are priced by arb_core::fees at \
             category '{fee_category}' for records the engine booked with fees_pending"
        ),
        "corrections": ledger.corrections,
        "corrections_applied": ledger.corrections_applied,
        // A correction that reaches no record is a RETRACTION THROWN AWAY, and
        // the number it retracted is still on the screen. Nothing in the payload
        // distinguished that from legitimate coalescing (three corrections on one
        // fedcut record, last-wins), so a dropped retraction was invisible.
        "corrections_unmatched": ledger.corrections_unmatched,
        "superseded": ledger.superseded,
        "orphan_unwinds": ledger.orphan_unwinds,
        "unusable_unwinds": ledger.unusable_unwinds(),
        "unparsed_lines": ledger.unparsed,
        "rows": rows,
    })
}
