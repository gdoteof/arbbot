//! Cost, payoff, net — and, where none of the three can be established, the
//! reason why.
//!
//! Every `None` this module returns is deliberate. A plausible-looking dollar
//! amount in a column of dollars is worse than a blank, because a human acts
//! on it; so a number that cannot be established leaves with an
//! `unpriced_reason` attached and is excluded from every total.

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
}

pub fn price(
    rec: &serde_json::Value,
    l: &Legs,
    hedged: bool,
    qty_booked: f64,
    rem: &Remainder,
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
        // `realized_pnl_usd` agree everywhere both exist.
        match num(rec.get("realized_pnl_usd")).or_else(|| num(rec.get("profit_usd"))) {
            Some(p) => (Some(p), Some("ledger:realized_pnl"), None),
            None => match (hedged, cost_full, payoff_full) {
                (true, Some(c), Some(p)) => (Some(derive(c, p)), Some(derived_from), None),
                _ => (
                    None,
                    None,
                    Some(
                        "closed with no realized_pnl_usd — P&L not recoverable from this record"
                            .to_string(),
                    ),
                ),
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
