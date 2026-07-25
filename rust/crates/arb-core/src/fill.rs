//! Fill ingestion + the exactly-once-hedge invariant as a type (P4 item 1,
//! sans-IO half; design: migration plan "Trader concurrency").
//!
//! Both fill paths — private WS and the poll fallback — funnel into ONE
//! `FillLedger::observe_cum_fill` caller. Venue fill reports are cumulative,
//! so duplicates and replays are no-ops by construction; only an INCREASE in
//! cumulative fill mints a `HedgeObligation` for the delta. The obligation is
//! non-clonable, `#[must_use]`, constructed only here, and consumed by value
//! by the hedger — the `hedged_by_oid` dict invariant from the Python trader
//! becomes compiler-checked. Dropping one unconsumed bumps a process-wide
//! counter the engine alarms on (arb-core stays pure: no I/O in Drop).
//!
//! The obligation carries its quote-time hedge anchor (burst-gap postmortem:
//! "the burst that fills you is the burst that gaps your book") so hedge
//! placement never depends on book liveness.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};

/// Quote-time hedge anchor: where the hedge leg stood when the maker order
/// was placed/refreshed. Prices stay strings (venue-verbatim decimals).
#[derive(Clone, Debug, PartialEq)]
pub struct HedgeAnchor {
    pub venue: &'static str,
    pub market_id: String,
    /// hedge-leg BOOK side the anchor price came from — consistent with
    /// `price` (maker bid fills => we sell into the hedge bid => "bid").
    /// Matches the engine's capture convention (see engine.rs, which mirrors
    /// `Quoter::hedge_has_depth` side selection).
    pub side: &'static str,
    pub price: String,
    /// tape/event time the anchor was captured (never a clock read)
    pub ts: f64,
}

struct OrderRec {
    market_key: String,
    resting: i64,
    cum_filled: i64,
    anchor: Option<HedgeAnchor>,
    cancelled: bool,
}

/// Process-wide count of obligations dropped unconsumed — a programming bug
/// turned observable. The engine reads this on its stats deadline and alarms.
static DROPPED_UNCONSUMED: AtomicU64 = AtomicU64::new(0);

pub fn dropped_unconsumed() -> u64 {
    DROPPED_UNCONSUMED.load(Ordering::Relaxed)
}

/// An unhedged fill. Cannot be cloned, cannot be constructed outside
/// `FillLedger`, and must be consumed (`into_parts`) by exactly one hedger.
#[must_use = "an unconsumed HedgeObligation is an exposed leg — hedge it or it alarms on drop"]
#[derive(Debug)]
pub struct HedgeObligation {
    order_id: String,
    market_id: String,
    /// contracts newly filled (the delta this observation minted)
    qty: i64,
    anchor: Option<HedgeAnchor>,
    consumed: bool,
}

impl HedgeObligation {
    pub fn order_id(&self) -> &str {
        &self.order_id
    }
    pub fn market_id(&self) -> &str {
        &self.market_id
    }
    pub fn qty(&self) -> i64 {
        self.qty
    }
    pub fn anchor(&self) -> Option<&HedgeAnchor> {
        self.anchor.as_ref()
    }

    /// Consume by value — the single hedger takes ownership of the exposure.
    pub fn into_parts(mut self) -> (String, String, i64, Option<HedgeAnchor>) {
        self.consumed = true;
        (
            std::mem::take(&mut self.order_id),
            std::mem::take(&mut self.market_id),
            self.qty,
            self.anchor.take(),
        )
    }
}

impl Drop for HedgeObligation {
    fn drop(&mut self) {
        if !self.consumed {
            DROPPED_UNCONSUMED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

/// Single-owner fill state: lives in the engine task, no locks. Orders are
/// registered at place time (with their quote-time hedge anchor), and every
/// fill report from every path goes through `observe_cum_fill`.
#[derive(Default)]
pub struct FillLedger {
    orders: HashMap<String, OrderRec>,
}

impl FillLedger {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a resting order at place time. Re-registering the same id
    /// (venue retry echo) keeps existing fill state.
    pub fn register_order(
        &mut self,
        order_id: &str,
        market_id: &str,
        resting: i64,
        anchor: Option<HedgeAnchor>,
    ) {
        self.orders.entry(order_id.to_owned()).or_insert(OrderRec {
            market_key: market_id.to_owned(),
            resting,
            cum_filled: 0,
            anchor,
            cancelled: false,
        });
    }

    /// Mark cancelled. The record is KEPT: a venue may report a fill that
    /// raced the cancel, and that fill still needs its hedge.
    pub fn observe_cancel(&mut self, order_id: &str) {
        if let Some(rec) = self.orders.get_mut(order_id) {
            rec.cancelled = true;
        }
    }

    /// The single funnel for both fill paths. Cumulative semantics:
    /// duplicate or stale reports (cum <= seen) return None; an increase
    /// mints exactly one obligation for the delta, clamped to resting size.
    /// Unknown order ids return None (caller alarms — foreign fill).
    pub fn observe_cum_fill(&mut self, order_id: &str, cum: i64) -> Option<HedgeObligation> {
        let rec = self.orders.get_mut(order_id)?;
        let cum = cum.min(rec.resting); // venue over-report clamps
        if cum <= rec.cum_filled {
            return None; // duplicate / out-of-order — idempotent by construction
        }
        let delta = cum - rec.cum_filled;
        rec.cum_filled = cum;
        Some(HedgeObligation {
            order_id: order_id.to_owned(),
            market_id: rec.market_key.clone(),
            qty: delta,
            anchor: rec.anchor.clone(),
            consumed: false,
        })
    }

    /// Fully-settled records (filled to resting or cancelled with all fills
    /// hedged) can be pruned by the engine's audit deadline.
    pub fn prune_settled(&mut self) {
        self.orders
            .retain(|_, r| !(r.cancelled || r.cum_filled >= r.resting));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn anchor() -> HedgeAnchor {
        HedgeAnchor {
            venue: "polymarket_us",
            market_id: "hedge-mkt".into(),
            side: "ask",
            price: "0.41".into(),
            ts: 1000.0,
        }
    }

    #[test]
    fn duplicate_cum_reports_mint_one_obligation() {
        // WS reports cum=3, then the poll fallback reports the same cum=3:
        // exactly one obligation, for 3.
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 5, Some(anchor()));
        let ob = fl.observe_cum_fill("k1", 3).expect("first report mints");
        assert_eq!(ob.qty(), 3);
        assert_eq!(ob.market_id(), "mkt");
        assert!(fl.observe_cum_fill("k1", 3).is_none(), "duplicate is a no-op");
        let _ = ob.into_parts();
    }

    #[test]
    fn out_of_order_report_is_a_noop() {
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 5, None);
        fl.observe_cum_fill("k1", 4).expect("mint").into_parts();
        assert!(fl.observe_cum_fill("k1", 2).is_none(), "stale cum ignored");
    }

    #[test]
    fn increasing_cum_mints_deltas() {
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 10, Some(anchor()));
        let a = fl.observe_cum_fill("k1", 2).expect("mint 2");
        assert_eq!(a.qty(), 2);
        a.into_parts();
        let b = fl.observe_cum_fill("k1", 7).expect("mint 5 more");
        assert_eq!(b.qty(), 5);
        assert_eq!(b.anchor().expect("anchor carried").price, "0.41");
        b.into_parts();
    }

    #[test]
    fn overfill_clamps_to_resting() {
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 5, None);
        let ob = fl.observe_cum_fill("k1", 9).expect("mint");
        assert_eq!(ob.qty(), 5, "venue over-report clamped to resting size");
        ob.into_parts();
        assert!(fl.observe_cum_fill("k1", 9).is_none());
    }

    #[test]
    fn unknown_order_id_is_none() {
        let mut fl = FillLedger::new();
        assert!(fl.observe_cum_fill("ghost", 1).is_none());
    }

    #[test]
    fn fill_racing_cancel_still_mints() {
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 5, Some(anchor()));
        fl.observe_cancel("k1");
        let ob = fl.observe_cum_fill("k1", 2).expect("racing fill still hedges");
        assert_eq!(ob.qty(), 2);
        ob.into_parts();
    }

    #[test]
    fn dropping_unconsumed_alarms_consuming_does_not() {
        let mut fl = FillLedger::new();
        fl.register_order("k1", "mkt", 5, None);
        fl.register_order("k2", "mkt", 5, None);
        let before = dropped_unconsumed();
        let consumed = fl.observe_cum_fill("k1", 1).expect("mint");
        consumed.into_parts(); // hedged — no alarm
        assert_eq!(dropped_unconsumed(), before);
        let leaked = fl.observe_cum_fill("k2", 1).expect("mint");
        drop(leaked); // NOT hedged — the counter must catch it
        assert_eq!(dropped_unconsumed(), before + 1);
    }

    #[test]
    fn prune_keeps_live_orders_only() {
        let mut fl = FillLedger::new();
        fl.register_order("live", "mkt", 5, None);
        fl.register_order("done", "mkt", 5, None);
        fl.register_order("gone", "mkt", 5, None);
        fl.observe_cum_fill("done", 5).expect("full fill").into_parts();
        fl.observe_cancel("gone");
        fl.prune_settled();
        assert!(fl.observe_cum_fill("live", 1).is_some());
        assert!(fl.observe_cum_fill("done", 5).is_none());
        assert!(fl.observe_cum_fill("gone", 1).is_none());
    }
}
