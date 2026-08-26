//! The headline numbers and the strategy board.
//!
//! Every row is added here exactly once, from the same `Trade` the tab
//! displays, so a total cannot disagree with the rows under it. What a row
//! contributes to the headline is ZERO whenever its P&L is booked somewhere
//! else (a basket a later unwind closed) or is not known (a naked bet) — those
//! rows are counted in `unpriced` / `closed_by_unwind` instead. That is what
//! makes `by_strategy` sum to `open_net_usd + realized_net_usd` exactly.

use std::collections::BTreeMap;

use super::Trade;

/// Positions with the capital behind them. Contracts and capital are facts
/// whether or not the profit can be established, so the unpriced book gets the
/// same three counters as the priced one.
#[derive(Default)]
struct Capital {
    n: u64,
    contracts: f64,
    cost: f64,
}

impl Capital {
    fn add(&mut self, qty: f64, cost: Option<f64>) {
        self.n += 1;
        self.contracts += qty;
        self.cost += cost.unwrap_or(0.0);
    }
}

/// One line of the strategy board.
#[derive(Default)]
struct Strategy {
    trades: u64,
    contracts: f64,
    net: f64,
    unpriced: u64,
    closed_by_unwind: u64,
    /// The realized side, so the strategy board shows WHICH strategies earned
    /// the book's rate. Kept apart from `net`, which spans open and closed
    /// alike and must keep summing to the headline.
    rated_net: f64,
    dollar_years: f64,
}

#[derive(Default)]
pub struct Totals {
    // Open and realized are tracked apart on purpose: adding them would report
    // money already banked as if it were still working.
    open: Capital,
    open_net: f64,
    // The open positions this file REFUSES to price. Their contracts and their
    // capital are facts and are counted; their profit is not known, so it is
    // absent from open_net_usd rather than guessed.
    naked: Capital,
    real_n: u64,
    real_net: f64,
    real_unpriced: u64,
    /// Closed rows this file priced ITSELF, from the exit's proceeds against
    /// the entry's basis, because the record carried no `realized_pnl_usd`.
    /// Counted apart from `real_n` so the headline can never present a derived
    /// figure as bank truth.
    real_derived: u64,
    closed_by_unwind: u64,
    /// The round trips: closed rows whose entry this ledger still holds, and
    /// the capital-time behind them.
    round_trips: u64,
    real_cost: f64,
    /// DOLLAR-YEARS, the denominator of the realized rate. Weighting realized
    /// APRs by capital alone would let a $1 position held six minutes at
    /// 200,000%/yr set the book's headline rate; weighting by capital x time
    /// gives that turn the weight it earned, which is almost none.
    real_dollar_years: f64,
    real_apr_net: f64,
    /// Round trips whose holding period is long enough to state a rate over.
    /// `round_trips` counts the closes; this counts the ones the rate is ON.
    rated_trips: u64,
    fees_settled: f64,
    fees_modelled: f64,
    by_strategy: BTreeMap<String, Strategy>,
}

impl Totals {
    pub fn add(&mut self, t: &Trade) {
        if t.fees_settled {
            self.fees_settled += t.fees;
        } else {
            self.fees_modelled += t.fees;
        }
        // What this row contributes to the headline. Zero for a row whose P&L
        // is booked somewhere else (a closed basket) or is not known (naked).
        let mut counted = 0.0f64;
        let mut unpriced_here = false;
        match t.status.as_str() {
            "open" => match t.priced.net {
                Some(n) => {
                    self.open.add(t.qty, t.priced.cost);
                    self.open_net += n;
                    counted = n;
                }
                None => {
                    self.naked.add(t.qty, t.priced.cost);
                    unpriced_here = true;
                }
            },
            "closed" => self.closed_by_unwind += 1,
            _ => {
                self.real_n += 1;
                if t.priced.source == Some("derived:exit_vs_entry") {
                    self.real_derived += 1;
                }
                match t.priced.net {
                    Some(n) => {
                        self.real_net += n;
                        counted = n;
                    }
                    None => {
                        self.real_unpriced += 1;
                        unpriced_here = true;
                    }
                }
                // The realized rate is built only from rows that have BOTH
                // ends of the trip: a net and the capital-time it earned it
                // over. A row missing either contributes to neither side of
                // the ratio, so the rate stays a statement about the trips it
                // can actually see.
                if let (Some(rt), Some(n)) = (t.round_trip.as_ref(), t.priced.net) {
                    self.round_trips += 1;
                    self.real_cost += rt.entry_cost;
                    // ONLY rows with an establishable rate. A 1ms probe pair has
                    // a real P&L and NO capital-time, so it belongs to neither
                    // side of this ratio — letting its loss into the numerator
                    // while it adds nothing to the denominator gives it infinite
                    // weight, which is the opposite of what the weighting is for.
                    if t.realized_apr.is_some() {
                        self.rated_trips += 1;
                        self.real_dollar_years += rt.entry_cost * rt.held_days / 365.25;
                        self.real_apr_net += n;
                        let s = self.by_strategy.entry(t.strategy.clone()).or_default();
                        s.rated_net += n;
                        s.dollar_years += rt.entry_cost * rt.held_days / 365.25;
                    }
                }
            }
        }
        let s = self.by_strategy.entry(t.strategy.clone()).or_default();
        s.trades += 1;
        s.contracts += t.qty_booked;
        s.net += counted;
        if unpriced_here {
            s.unpriced += 1;
        }
        if t.status == "closed" {
            s.closed_by_unwind += 1;
        }
    }

    /// The book's realized rate: total profit over total dollar-years. This is
    /// a money-weighted return, not a mean of per-trade APRs — a $2 turn at
    /// 40,000%/yr and a $200 hold at 5%/yr do not average to anything a human
    /// should read.
    pub fn realized_apr(&self) -> Option<f64> {
        (self.real_dollar_years > 0.0).then(|| self.real_apr_net / self.real_dollar_years * 100.0)
    }

    pub fn headline(
        &self,
        trades: usize,
        blended_apr: Option<f64>,
        realized_apr: Option<f64>,
    ) -> serde_json::Value {
        serde_json::json!({
            "trades": trades,
            // Every open position: priced and unpriced. Contracts and capital
            // are facts whether or not the profit can be established.
            "open_trades": self.open.n + self.naked.n,
            "open_contracts": self.open.contracts + self.naked.contracts,
            "open_cost_usd": self.open.cost + self.naked.cost,
            // ...but the profit total counts only the hedged, priceable ones.
            "open_net_usd": self.open_net,
            "open_priced_trades": self.open.n,
            "open_unpriced_trades": self.naked.n,
            "open_unpriced_contracts": self.naked.contracts,
            "open_unpriced_cost_usd": self.naked.cost,
            "realized_trades": self.real_n,
            "realized_net_usd": self.real_net,
            "realized_unpriced_trades": self.real_unpriced,
            // Closed rows priced from the exit's own proceeds against the
            // entry's basis rather than from a `realized_pnl_usd` the record
            // never carried. Their fees are MODELLED at both ends.
            "realized_derived_trades": self.real_derived,
            // Round trips: closed rows joined to the entry they retired. Their
            // capital and the time it was out are what the realized rate is a
            // rate ON, so all three travel together.
            "round_trips": self.round_trips,
            "realized_cost_usd": self.real_cost,
            "realized_dollar_years": self.real_dollar_years,
            "realized_apr_pct": realized_apr,
            // Round trips too short to annualize — their P&L is real and is in
            // `realized_net_usd`, but no rate is stated over a 1ms hold.
            "unrated_round_trips": self.round_trips - self.rated_trips,
            "closed_by_unwind": self.closed_by_unwind,
            "unpriced_rows": self.naked.n + self.real_unpriced,
            "fees_settled_usd": self.fees_settled,
            "fees_modelled_usd": self.fees_modelled,
            "blended_apr_pct": blended_apr,
        })
    }

    /// `net_usd` sums to open_net_usd + realized_net_usd exactly: a row whose
    /// P&L is unknown or already booked elsewhere contributes 0 and is
    /// counted in `unpriced`/`closed_by_unwind` instead.
    pub fn by_strategy(&self) -> Vec<serde_json::Value> {
        self.by_strategy
            .iter()
            .map(|(k, s)| {
                serde_json::json!({"strategy": k, "trades": s.trades, "contracts": s.contracts,
                                   "net_usd": s.net, "unpriced": s.unpriced,
                                   "closed_by_unwind": s.closed_by_unwind,
                                   "realized_net_usd": s.rated_net,
                                   "dollar_years": s.dollar_years,
                                   "realized_apr_pct": (s.dollar_years > 0.0)
                                       .then(|| s.rated_net / s.dollar_years * 100.0)})
            })
            .collect()
    }
}
