//! What each leg of a basket did: its direction, its fee, and what it cost.
//!
//! This is the pass that decides whether a record is a hedge or a punt.
//! Everything downstream — `hedged`, the derived cost, the unpriced reason —
//! is read off the `Legs` this module returns, so a leg misread here is a
//! wrong dollar figure on the screen, not a cosmetic one.

use std::collections::BTreeMap;

use arb_core::fees::{FeeSchedule, Role};
use arb_core::model::Venue;
use arb_core::scan::Cx;

use super::num;

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

impl Leg {
    fn name(self) -> &'static str {
        match self {
            Leg::LongYes => "long_yes",
            Leg::LongNo => "long_no",
            Leg::Short => "short",
            Leg::Unknown => "unknown",
        }
    }
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

/// Which side a SALE gave up, for the exit legs of an unwind.
///
/// `close_via_*` IS NOT A SALE and must never reach here as one. Those legs
/// BUY to close a short — a debit — and reading their price as proceeds would
/// book the cost of closing as income. There is one such record on disk and it
/// carries its own `realized_pnl_usd`, so refusing to read it costs nothing and
/// guessing at it would cost $0.57.
fn sold_side(action: &str, side: &str) -> Option<bool> {
    if action != "sell" {
        return None;
    }
    match side {
        "yes" | "bid" => Some(true),
        "no" | "ask" => Some(false),
        _ => None,
    }
}

/// The fee model and the arena it needs. `FeeSchedule::fee` wants a `&mut Cx`
/// alongside the schedule, so the two travel together for the whole build.
pub struct Modeller {
    cx: Cx,
    sched: FeeSchedule,
}

impl Modeller {
    pub fn new() -> Modeller {
        let mut cx = Cx::default();
        let sched = FeeSchedule::new(&mut cx);
        Modeller { cx, sched }
    }

    /// What `arb_core::fees` says this leg cost, for a record the engine booked
    /// with `fees_pending`.
    ///
    /// Only an explicit `maker` gets the maker coefficient. An unrecorded role
    /// prices as a TAKER: overstating our own costs is the safe direction for
    /// an accounting view, and defaulting the other way would quietly flatter
    /// it.
    fn model(&mut self, venue: &str, role: &str, px: Option<f64>, qty: f64, cat: &str) -> f64 {
        let role = if role == "maker" { Role::Maker } else { Role::Taker };
        match (Venue::parse(venue), px) {
            (Some(v), Some(px)) => {
                let p = self.cx.parse_exact(&format!("{px}"));
                let sz = self.cx.parse_exact(&format!("{qty}"));
                let f = self.sched.fee(&mut self.cx, v, role, p, sz, cat);
                self.cx.emit_6dp(f).parse::<f64>().unwrap_or(0.0)
            }
            _ => 0.0,
        }
    }
}

/// Legs summed by `(venue, role)` across the whole ledger — the make/take
/// split the tab shows.
#[derive(Default)]
pub struct Splits(BTreeMap<(String, String), VenueRole>);

#[derive(Default)]
struct VenueRole {
    legs: u64,
    contracts: f64,
    fees: f64,
}

impl Splits {
    fn add(&mut self, venue: &str, role: &str, qty: f64, fee: f64) {
        let e = self.0.entry((venue.to_string(), role.to_string())).or_default();
        e.legs += 1;
        e.contracts += qty;
        e.fees += fee;
    }

    pub fn to_json(&self) -> Vec<serde_json::Value> {
        self.0
            .iter()
            .map(|((v, r), s)| {
                serde_json::json!({"venue": v, "role": r, "legs": s.legs,
                                   "contracts": s.contracts, "fees_usd": s.fees})
            })
            .collect()
    }
}

/// One record's legs, read.
pub struct Legs {
    pub n: usize,
    /// Settled fees beat modelled ones wherever the ledger has them. A record
    /// that reports its own fees is bank truth; a modelled number is this
    /// dashboard's opinion, and the two must not be presented alike.
    pub fees: f64,
    pub settled: bool,
    /// What the legs cost, ex-fees. Only meaningful once the record is known
    /// to be hedged — see `pricing::hedged`.
    pub derived_cost: f64,
    pub yes_qty: f64,
    pub no_qty: f64,
    pub unknown_leg: bool,
    pub short_leg: bool,
    /// The mirror of `derived_cost` for an EXIT: what the sold legs collected,
    /// ex-fees. `yes_price` is always quoted on the YES side, so a sold NO
    /// collects (1 - yes_price).
    pub proceeds: f64,
    pub sold_yes_qty: f64,
    pub sold_no_qty: f64,
    /// A leg that BOUGHT. An exit record is a clean flatten only if nothing in
    /// it opened new exposure.
    pub long_leg: bool,
    /// A sold leg whose side or price this file could not read — including
    /// every `close_via_*`. It makes `proceeds` incomplete, so the record
    /// cannot be priced from its legs.
    pub short_unreadable: bool,
    pub payload: Vec<serde_json::Value>,
}

impl Legs {
    pub fn read(
        m: &mut Modeller,
        legs: &[serde_json::Value],
        qty_booked: f64,
        fee_category: &str,
        splits: &mut Splits,
    ) -> Legs {
        let mut out = Legs {
            n: legs.len(),
            fees: 0.0,
            settled: false,
            derived_cost: 0.0,
            yes_qty: 0.0,
            no_qty: 0.0,
            unknown_leg: false,
            short_leg: false,
            proceeds: 0.0,
            sold_yes_qty: 0.0,
            sold_no_qty: 0.0,
            long_leg: false,
            short_unreadable: false,
            payload: Vec::new(),
        };
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
                    out.settled = true;
                    f
                }
                None => m.model(&venue_s, &role_s, px, lqty, fee_category),
            };
            out.fees += leg_fee;

            // What this leg cost per contract. `yes_price` is always quoted on
            // the YES side, so buying NO costs (1 - yes_price). A leg with no
            // readable side, no price, or a SOLD direction contributes nothing
            // and poisons the derivation — silently treating it as 0.0 booked
            // whole notionals as profit, and treating a sale as a purchase
            // booked a guaranteed loss as locked profit.
            let dir = leg_dir(&action, &side);
            match (dir, px) {
                (Leg::LongYes, Some(px)) => {
                    out.long_leg = true;
                    out.yes_qty += lqty;
                    out.derived_cost += px * lqty;
                }
                (Leg::LongNo, Some(px)) => {
                    out.long_leg = true;
                    out.no_qty += lqty;
                    out.derived_cost += (1.0 - px) * lqty;
                }
                // A sale still disqualifies the record from being priced as a
                // long basket — `short_leg` is unchanged and `hedged` still
                // reads it. What is new is that the sale is also READ, because
                // an unwind is made of nothing else and its proceeds are the
                // only thing that says what the exit got.
                (Leg::Short, _) => {
                    out.short_leg = true;
                    match (sold_side(&action, &side), px) {
                        (Some(true), Some(px)) => {
                            out.sold_yes_qty += lqty;
                            out.proceeds += px * lqty;
                        }
                        (Some(false), Some(px)) => {
                            out.sold_no_qty += lqty;
                            out.proceeds += (1.0 - px) * lqty;
                        }
                        _ => out.short_unreadable = true,
                    }
                }
                _ => out.unknown_leg = true,
            }

            splits.add(&venue_s, &role_s, lqty, leg_fee);

            out.payload.push(serde_json::json!({
                "venue": venue_s,
                "market_id": l.get("market_id").and_then(|v| v.as_str()).unwrap_or(""),
                "side": if side.is_empty() { action.clone() } else { side },
                // `action` was dropped from the payload entirely, so nothing
                // downstream could see that a leg was SOLD. Both it and the
                // resolved direction are now carried.
                "action": action,
                "direction": dir.name(),
                "role": role_s,
                "qty": lqty,
                "price": px,
                "fee_usd": leg_fee,
            }));
        }
        out
    }
}
