//! Cost, payoff, net — and, where none of the three can be established, the
//! reason why.
//!
//! Every `None` this module returns is deliberate. A plausible-looking dollar
//! amount in a column of dollars is worse than a blank, because a human acts
//! on it; so a number that cannot be established leaves with an
//! `unpriced_reason` attached and is excluded from every total.

use super::entry::RoundTrip;
use super::fold::Remainder;
use super::legs::Legs;
use super::{num, EPS};

/// Hedged means both sides are BOUGHT and each side covers the whole
/// basket. Only then is the $1.00/contract payoff a fact. A sold leg
/// disqualifies the record: the obligation runs the other way and this
/// file does not model credits.
pub fn hedged(l: &Legs, qty_booked: f64) -> bool {
    l.n >= 2
        && !l.unknown_leg
        && !l.short_leg
        && qty_booked > 0.0
        && l.yes_qty >= qty_booked - EPS
        && l.no_qty >= qty_booked - EPS
}

/// Flattened means every leg SOLD and the two sides together cover the whole
/// exit — the exact mirror of [`hedged`]. Only then are the legs' proceeds the
/// WHOLE of what this exit collected, and only then may the entry basis be
/// subtracted from them.
///
/// The single-leg case is why this is a predicate and not an assumption. The
/// `naked-close` record on disk sold ONE Kalshi leg of a two-leg basket and
/// says so in its own note ("RECONCILE BY HAND"); measured against that
/// basket's full $0.98/ct basis its $0.29 of proceeds reads as a $3.45 LOSS
/// that nobody took. It fails here and stays unpriced.
pub fn flattened(l: &Legs, qty: f64) -> bool {
    l.n >= 2
        && !l.long_leg
        && !l.unknown_leg
        && !l.short_unreadable
        && qty > 0.0
        && l.sold_yes_qty >= qty - EPS
        && l.sold_no_qty >= qty - EPS
}

/// The exits whose two leg prices are on the WRONG VENUES, and are known to be.
///
/// `arb-trader/src/maker_exit.rs::close_record` took its two fills as (the leg
/// that RESTED, the leg CLOSED by IOC) and wrote them straight onto the Kalshi
/// and PM-US legs. Under `rest-kalshi` those coincide; under `rest-pmus` they
/// are reversed, so EVERY `rest-pmus` record written before the fix carries its
/// Kalshi price on the PM-US leg and vice versa. PR #97 fixed the writer on
/// 2026-08-25 and said in as many words that correcting the records already on
/// disk was a separate migration. That migration has not been run: the ledger
/// is still byte-identical to the backup taken when #97 landed.
///
/// So these records cannot be priced from their legs, and the difference is not
/// cosmetic — transposing the two prices moves the derived P&L by
/// `2 * (pm_px - k_px) * qty`, which on the 11 affected records ranges from 2c
/// to 38c per contract and flips most of them between profit and loss. They
/// price as `null` with the reason below rather than as fabricated losses.
///
/// THIS PREDICATE IS A DATED FACT, NOT A HEURISTIC. The boundary is the restart
/// that put the fixed binary in front of the ledger —
/// `arbbot-trader-m3.service` at 2026-08-25T00:22:28-04:00, per its journal —
/// and no exit was written during the restart window: the last affected record
/// is 00:21:15 and the first correct one 00:28:32. It is deliberately NOT keyed
/// on the note text, which #97 did not change in a way that separates the two
/// versions, nor on `lock_ct`, which is a decision-time figure that legitimately
/// disagrees with a fill.
///
/// DELETE THIS, and the reason it emits, the moment the migration appends its
/// `correction` records. The fold already applies those, so the rows light up
/// with no other change here.
const SWAP_FIXED_TS: f64 = 1_787_631_748.0;

pub fn legs_are_swapped(rec: &serde_json::Value) -> bool {
    rec.get("strategy").and_then(|v| v.as_str()) == Some("maker-exit")
        && rec.get("maker_exit_shape").and_then(|v| v.as_str()) == Some("rest-pmus")
        && num(rec.get("ts")).is_some_and(|ts| ts < SWAP_FIXED_TS)
}

/// Why an exit could not be priced against its entry. As with `unhedged_reason`,
/// the specific answer is the point: "we never found the entry" and "one leg of
/// two traded" need different people.
fn unclosed_reason(l: &Legs, rt: Option<&RoundTrip>, q: f64) -> String {
    if rt.is_none() {
        return "closed with no realized_pnl_usd, and the entry it names (`closes_ts`) is not \
                an open record in this ledger with a readable cost — there is no basis to price \
                the proceeds against"
            .into();
    }
    if l.n == 0 {
        "closed with no realized_pnl_usd and no legs — nothing says what the exit collected".into()
    } else if l.long_leg {
        "closed with no realized_pnl_usd, and a leg here BOUGHT — this record opens exposure as \
             well as closing it, so its proceeds are not the whole story"
            .into()
    } else if l.short_unreadable {
        "closed with no realized_pnl_usd, and a leg's sale is not readable (no price, or a \
             `close_via_*` that BUYS to close rather than sells) — proceeds cannot be totalled"
            .into()
    } else {
        let (y, n) = (l.sold_yes_qty, l.sold_no_qty);
        format!(
            "closed with no realized_pnl_usd, and the legs sold {y} YES against {n} NO on {q} \
                 contracts — a partial flatten, so what came back cannot be set against the whole \
                 basis. RECONCILE BY HAND"
        )
    }
}

/// Why a record's P&L could not be established. Specific on purpose: "unknown"
/// with no reason is the same dead end as a wrong number.
fn unhedged_reason(l: &Legs, q: f64) -> String {
    if l.n == 0 {
        "no legs recorded — nothing to price".into()
    } else if l.unknown_leg {
        "a leg's side is not readable (e.g. \"mixed\") — cannot tell whether this is hedged".into()
    } else if l.short_leg {
        "SHORT leg(s): sold or closed, so this collects a credit against an obligation. This \
         view prices long baskets only and will not guess at the net"
            .into()
    } else if l.n == 1 {
        "NAKED: one leg, so this pays $0.00 or $1.00/contract depending on the outcome".into()
    } else {
        let (yes_q, no_q) = (l.yes_qty, l.no_qty);
        format!(
            "NAKED: legs cover {yes_q} YES against {no_q} NO on {q} contracts — not fully hedged"
        )
    }
}

/// The money on one record, already pro-rated by whatever the fold left on.
pub struct Priced {
    /// Pro-rated to the remaining basket. This is what ties up capital now.
    pub cost: Option<f64>,
    /// The whole basket as the ledger booked it, before the fold scaled it.
    pub cost_booked: Option<f64>,
    pub payoff: Option<f64>,
    pub net: Option<f64>,
    /// How the net was established, so an auditor never has to guess whether
    /// a number came off the ledger or out of this file.
    pub source: Option<&'static str>,
    pub unpriced_reason: Option<String>,
    /// The round trip, where this record closed one: what the entry cost, what
    /// the exit collected, and how long the capital was out. Present on a row
    /// whether or not the net came from the ledger — a ledger-truth P&L still
    /// needs the basis and the holding period to have an APR.
    pub entry_cost: Option<f64>,
    pub exit_proceeds: Option<f64>,
}

pub fn price(
    rec: &serde_json::Value,
    l: &Legs,
    hedged: bool,
    qty_booked: f64,
    rem: &Remainder,
    rt: Option<&RoundTrip>,
) -> Priced {
    // The record's own cost is authoritative and ALL-IN (fees inside).
    // Derived cost is ex-fees, so fees must then be subtracted separately.
    let (cost_full, fees_in_cost) = match num(rec.get("cost_usd")) {
        Some(c) => (Some(c), true),
        None if hedged => (Some(l.derived_cost), false),
        None => (None, false),
    };
    let payoff_full = if rem.is_open && !hedged {
        // An open directional position has NO known payoff. The writer
        // stamps the maximum ($1.00/contract); printing that as "payoff"
        // beside a null net invites the reader to subtract cost from it,
        // which is exactly the arithmetic that overstated this book.
        None
    } else {
        num(rec.get("payoff_usd")).or(if hedged { Some(qty_booked) } else { None })
    };

    // Only meaningful once the exit is known to be a clean, whole flatten — and
    // never for a record whose two prices are on the wrong venues, where the
    // total is arithmetically fine and attributed to the wrong books.
    let flat_proceeds =
        (flattened(l, qty_booked) && !legs_are_swapped(rec)).then_some(l.proceeds);

    let derived_from = if fees_in_cost { "ledger:cost_usd" } else { "derived:leg_prices" };
    let derive = |c: f64, p: f64| if fees_in_cost { p - c } else { p - c - l.fees };
    let (net_full, net_source, unpriced_reason) = if rem.fully_closed {
        (
            None,
            None,
            Some("closed by a later unwind record — its P&L is on that row".to_string()),
        )
    } else if rem.unknown {
        (
            None,
            None,
            Some(
                "an unwind record names this basket but carries no usable qty — how much is \
                 still on cannot be established"
                    .to_string(),
            ),
        )
    } else if rem.is_open {
        if !hedged {
            (None, None, Some(unhedged_reason(l, qty_booked)))
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
        // `realized_pnl_usd` agree everywhere both exist, and neither is ever
        // re-derived while one of them is on the record.
        match num(rec.get("realized_pnl_usd")).or_else(|| num(rec.get("profit_usd"))) {
            Some(p) => (Some(p), Some("ledger:realized_pnl"), None),
            None => match (hedged, cost_full, payoff_full) {
                (true, Some(c), Some(p)) => (Some(derive(c, p)), Some(derived_from), None),
                // THE ROUND TRIP. An exit that flattened the whole basket, and
                // whose entry this ledger still holds, is priced the only way
                // it can be: what it sold, less what it cost to put on, less
                // the exit's own fees. Entry fees are already inside
                // `entry_cost`; exit fees are `l.fees`, modelled where the
                // engine booked `fees_pending`.
                _ if legs_are_swapped(rec) => (
                    None,
                    None,
                    Some(
                        "this record's two leg prices are on the WRONG VENUES — a `rest-pmus` \
                             exit written before the 2026-08-25 fix (PR #97), whose ledger \
                             migration has not been run. Its P&L is recoverable only by \
                             transposing the two prices, which is the migration's job and not \
                             this view's"
                            .to_string(),
                    ),
                ),
                _ => match (flat_proceeds, rt) {
                    (Some(p), Some(rt)) => {
                        (Some(p - rt.entry_cost - l.fees), Some("derived:exit_vs_entry"), None)
                    }
                    _ => (None, None, Some(unclosed_reason(l, rt, qty_booked))),
                },
            },
        }
    };

    Priced {
        cost: cost_full.map(|c| c * rem.scale),
        cost_booked: cost_full,
        payoff: payoff_full.map(|p| p * rem.scale),
        net: net_full.map(|n| n * rem.scale),
        source: net_source,
        unpriced_reason,
        // Pro-rated by nothing: a `closed`/`unwound` record IS its own qty, and
        // `rem.scale` is 1.0 for anything that is not a partially-unwound OPEN
        // basket. Scaling here would halve an exit that closed half a lot.
        entry_cost: rt.map(|rt| rt.entry_cost),
        exit_proceeds: flat_proceeds,
    }
}

/// APR only means something while the capital is still committed.
/// A realized trade's money is back and earning elsewhere.
pub fn apr(p: &Priced, status: &str, years: Option<f64>) -> Option<f64> {
    match (p.net, p.cost, years) {
        (Some(n), Some(c), Some(y)) if status == "open" && c > 0.0 && y > 0.0 => {
            Some(n / c / y * 100.0)
        }
        _ => None,
    }
}
