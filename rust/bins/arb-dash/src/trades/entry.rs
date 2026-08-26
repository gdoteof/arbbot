//! What a closed basket COST to put on — the other half of a round trip.
//!
//! An exit record says what it sold and for how much. On its own that is a
//! PROCEEDS figure, not a profit: the entry it retired is a different record,
//! written hours or weeks earlier, and until the two are joined there is
//! nothing to subtract. That gap is not academic — the entire Rust maker-exit
//! programme (24 records on disk, every profitable close the flywheel has
//! made) priced as `net_usd: null` because the exit rows carry no
//! `realized_pnl_usd` and nothing here looked up what they closed.
//!
//! This module is the join. It is keyed by the same `(relationship_id,
//! closes_ts)` pair `fold::unwind_index` uses to net quantities, so an exit
//! that prices here is exactly an exit that closes something there.
//!
//! THE BASIS IS ALL-IN. Entry fees are inside it, whether the ledger reported
//! them (`cost_usd`) or this file modelled them, because the number it is
//! subtracted from — exit proceeds — is gross of nothing. Mixing the two
//! conventions understates every close by its entry fees.

use std::collections::HashMap;

use super::legs::Legs;

/// One entry, reduced to what pricing an exit against it needs.
pub struct Entry {
    /// All-in cost per contract. Per contract, not per basket, because an
    /// exit is sized to a lot and may retire only part of what it names —
    /// the basis is an average, so scaling it is exact.
    pub cost_per_ct: f64,
    /// When the capital went out. The other end of the holding period.
    pub ts: f64,
    /// The entry's own fees were reported by the venue, not modelled here.
    /// An exit's realized P&L is only as settled as BOTH ends of it.
    pub fees_settled: bool,
}

/// Every `open` record, indexed by what an unwind's `closes_ts` would name.
#[derive(Default)]
pub struct Entries(HashMap<(String, u64), Entry>);

impl Entries {
    /// `ts` is unique across the ledger, so one key names one entry. A
    /// duplicate would be a `superseded` record, and the fold has already
    /// dropped those by the time this runs.
    pub fn insert(
        &mut self,
        rel_id: &str,
        ts: f64,
        qty_booked: f64,
        cost_all_in: Option<f64>,
        l: &Legs,
    ) {
        let Some(cost) = cost_all_in else { return };
        if !qty_booked.is_finite() || qty_booked <= 0.0 || !cost.is_finite() {
            return;
        }
        self.0.insert(
            (rel_id.to_string(), ts.to_bits()),
            Entry { cost_per_ct: cost / qty_booked, ts, fees_settled: l.settled },
        );
    }

    pub fn get(&self, rel_id: &str, closes_ts: Option<f64>) -> Option<&Entry> {
        self.0.get(&(rel_id.to_string(), closes_ts?.to_bits()))
    }
}

/// An exit joined to the entry it retired.
pub struct RoundTrip {
    pub entry_ts: f64,
    /// The capital this exit gave back, at the basis it went out at.
    pub entry_cost: f64,
    pub held_days: f64,
    pub fees_settled: bool,
}

/// A HOLD SHORTER THAN A MINUTE IS NOT A HOLDING PERIOD, and annualizing over
/// one is the same dead end as dividing by zero.
///
/// This is not a hypothetical. Four `pmus-maker-probe` pairs on disk were
/// written one MILLISECOND apart — the probe booked an entry and its exit in
/// the same breath, so the capital was never really out. Annualized, one of
/// them reads −10,646,155%/yr, and that is a number a human acts on. Their
/// P&L is real and is still shown; only the RATE is refused, because there is
/// no meaningful period to state it over.
///
/// The threshold is deliberately NOT the one-day floor
/// `arb_core::resolve::years_to` uses. That floor is for a hold-to-resolution
/// horizon measured in whole days; applied to a six-minute maker exit it would
/// understate the return 240-fold, which is a lie in the other direction. A
/// real six-minute turn keeps its four-figure rate here, and the `held_days`
/// beside it says why it is four figures.
const MIN_HELD_S: f64 = 60.0;
const DAYS_PER_YEAR: f64 = 365.25;

pub fn join(
    entries: &Entries,
    rel_id: &str,
    closes_ts: Option<f64>,
    exit_ts: f64,
    qty: f64,
) -> Option<RoundTrip> {
    let e = entries.get(rel_id, closes_ts)?;
    if !qty.is_finite() || qty <= 0.0 {
        return None;
    }
    Some(RoundTrip {
        entry_ts: e.ts,
        entry_cost: e.cost_per_ct * qty,
        // TRUE elapsed time, never floored. It is displayed as a duration, and
        // a 1ms pair that renders as "1m" would hide exactly the thing that
        // makes its rate meaningless.
        held_days: (exit_ts - e.ts) / 86400.0,
        fees_settled: e.fees_settled,
    })
}

/// The annualized return this round trip actually earned.
///
/// Reported with no cap and no smoothing, because a cap is a number nobody can
/// audit — but never alone: the payload carries `held_days` beside it so a
/// four-figure APR on a six-minute turn reads as what it is. What IS refused is
/// a period too short to be one; see [`MIN_HELD_S`].
pub fn apr(net: Option<f64>, rt: Option<&RoundTrip>) -> Option<f64> {
    let (net, rt) = (net?, rt?);
    let held_s = rt.held_days * 86400.0;
    if held_s < MIN_HELD_S || rt.entry_cost <= 0.0 {
        return None;
    }
    Some(net / rt.entry_cost / (rt.held_days / DAYS_PER_YEAR) * 100.0)
}
