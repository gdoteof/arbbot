//! Kalshi importer: venue truth -> journal entries.
//!
//! Kalshi is the one venue whose account can be rebuilt exactly. See
//! `docs/venue-quirks.md` "Kalshi — accounting / cash" for the six mechanics
//! this depends on. The load-bearing one:
//!
//!   Fills come back ONLY as `buy/yes` or `sell/no`, both priced on the YES
//!   axis. A `sell no` that fits inside the long position CLOSES it (proceeds
//!   qty*yes_px). A `sell no` BEYOND the long position opens a short, which is
//!   economically buying NO at (1 - yes_px) — cash out, not cash in. Getting
//!   that backwards puts the reconciliation $156.67 out.
//!
//! Fees come from the fill's `fee_cost` (what the venue actually charged,
//! ceil-to-cent per order), never from the modelled curve. Settlement
//! `fee_cost` is IGNORED: it restates the market's fill fees and booking it
//! would double-count.

use arb_core::scan::{Cx, D};
use serde::Deserialize;

use crate::{accounts, neg, Entry, Journal, LedgerError, Posting, Source};

#[derive(Debug, Deserialize)]
pub struct Deposit {
    pub id: String,
    pub amount_cents: i64,
    pub status: String,
    #[serde(default)]
    pub created_ts: i64,
}

#[derive(Debug, Deserialize)]
pub struct Fill {
    pub fill_id: String,
    pub ticker: String,
    pub action: String,
    pub side: String,
    pub count_fp: String,
    pub yes_price_dollars: String,
    pub fee_cost: String,
    #[serde(default)]
    pub is_taker: bool,
    #[serde(default)]
    pub ts: i64,
}

#[derive(Debug, Deserialize)]
pub struct Settlement {
    pub ticker: String,
    /// CENTS, and NET of yes-vs-no holdings in the market.
    pub revenue: i64,
    #[serde(default)]
    pub settled_time: String,
    #[serde(default)]
    pub yes_total_cost_dollars: String,
    #[serde(default)]
    pub no_total_cost_dollars: String,
}

/// Running per-market state: signed qty (YES axis) and cost basis (>= 0).
#[derive(Clone, Copy)]
struct Lot {
    qty: D,
    basis: D,
}

pub struct KalshiImport {
    pub deposits: Vec<Deposit>,
    pub fills: Vec<Fill>,
    pub settlements: Vec<Settlement>,
}

impl KalshiImport {
    /// Build entries in chronological order. Fills are sorted by `ts`; ordering
    /// matters because whether a sell closes a long or opens a short depends on
    /// the position at that instant.
    pub fn apply(mut self, cx: &mut Cx, j: &mut Journal) -> Result<(), LedgerError> {
        for d in &self.deposits {
            if d.status != "applied" {
                continue;
            }
            let cents = cx.from_i64(d.amount_cents);
            let hundred = cx.parse_exact("100");
            let amt = cx.div(cents, hundred);
            let credit = neg(cx, amt);
            j.post(
                cx,
                Entry {
                    id: format!("kalshi:deposit:{}", d.id),
                    ts: d.created_ts,
                    source: Source::VenueDeposit,
                    memo: format!("kalshi deposit {}", d.id),
                    postings: vec![
                        Posting::new(accounts::CASH_KALSHI, amt),
                        Posting::new(accounts::EQUITY_CAPITAL, credit),
                    ],
                },
            )?;
        }

        self.fills.sort_by_key(|f| f.ts);
        let mut lots: std::collections::HashMap<String, Lot> = std::collections::HashMap::new();

        for f in &self.fills {
            let qty = cx.parse_exact(&f.count_fp);
            let px = cx.parse_exact(&f.yes_price_dollars);
            let fee = cx.parse_exact(&f.fee_cost);
            let acct = accounts::position("kalshi", &f.ticker);
            let fee_acct = accounts::fees("kalshi", f.is_taker);
            let zero = cx.zero();
            let lot = *lots.get(&f.ticker).unwrap_or(&Lot { qty: zero, basis: zero });

            // Symmetric handling. A fill can REDUCE the existing position and
            // OPEN the opposite one in a single print, and the two halves price
            // differently: the long side trades at yes_px, the short side at
            // (1 - yes_px) with the cash sign inverted. Handling only one
            // direction leaves basis on a closed market and cash adrift.
            let buying = f.action == "buy";
            let signed = if buying { qty } else { neg(cx, qty) };
            let no_px = cx.one_minus(px);

            // How much of this fill offsets the position we already hold?
            let holding = if buying {
                // covering a short: the open size is -qty when qty < 0
                if cx.cmp(lot.qty, zero) == std::cmp::Ordering::Less {
                    neg(cx, lot.qty)
                } else {
                    zero
                }
            } else if cx.cmp(lot.qty, zero) == std::cmp::Ordering::Greater {
                lot.qty
            } else {
                zero
            };
            let reduce = cx.min(qty, holding);
            let open = cx.sub(qty, reduce);

            // Exit price for the side being closed; entry price for the side
            // being opened. Short legs live on the NO axis.
            let exit_px = if buying { no_px } else { px };
            let entry_px = if buying { px } else { no_px };

            let proceeds = cx.mul(reduce, exit_px);
            let relieved = if crate::is_zero(cx, reduce) {
                zero
            } else {
                let mag = if cx.cmp(lot.qty, zero) == std::cmp::Ordering::Less {
                    neg(cx, lot.qty)
                } else {
                    lot.qty
                };
                let frac = cx.div(reduce, mag);
                cx.mul(lot.basis, frac)
            };
            let realized = cx.sub(proceeds, relieved);
            let opened_cost = cx.mul(open, entry_px);

            let basis_delta = {
                let a = neg(cx, relieved);
                cx.add(a, opened_cost)
            };
            // Cash: money in from what we closed, out for what we opened, less
            // the fee the venue actually charged.
            let cash_delta = {
                let a = cx.sub(proceeds, opened_cost);
                cx.sub(a, fee)
            };

            let mut postings: Vec<Posting> = Vec::new();
            postings.push(Posting::with_qty(&acct, basis_delta, signed));
            if !crate::is_zero(cx, fee) {
                postings.push(Posting::new(fee_acct, fee));
            }
            if !crate::is_zero(cx, realized) {
                postings.push(Posting::new(accounts::PNL_REALIZED, neg(cx, realized)));
            }
            postings.push(Posting::new(accounts::CASH_KALSHI, cash_delta));

            let new_lot = Lot {
                qty: cx.add(lot.qty, signed),
                basis: cx.add(lot.basis, basis_delta),
            };

            lots.insert(f.ticker.clone(), new_lot);
            j.post(
                cx,
                Entry {
                    id: format!("kalshi:fill:{}", f.fill_id),
                    ts: f.ts,
                    source: Source::VenueFill,
                    memo: format!("{} {} {} @ {}", f.action, f.side, f.ticker, f.yes_price_dollars),
                    postings,
                },
            )?;
        }

        // Settlements: relieve whatever basis remains, credit net revenue
        // (CENTS, and already netted across yes/no holdings), realize the rest.
        for s in &self.settlements {
            let zero = cx.zero();
            let acct = accounts::position("kalshi", &s.ticker);
            let lot = *lots.get(&s.ticker).unwrap_or(&Lot { qty: zero, basis: zero });
            let cents = cx.from_i64(s.revenue);
            let hundred = cx.parse_exact("100");
            let revenue = cx.div(cents, hundred);
            if crate::is_zero(cx, lot.basis) && crate::is_zero(cx, revenue) && crate::is_zero(cx, lot.qty) {
                continue;
            }
            let realized = cx.sub(revenue, lot.basis);
            let mut postings = vec![Posting::new(accounts::CASH_KALSHI, revenue)];
            postings.push(Posting::with_qty(&acct, neg(cx, lot.basis), neg(cx, lot.qty)));
            if !crate::is_zero(cx, realized) {
                postings.push(Posting::new(accounts::PNL_REALIZED, neg(cx, realized)));
            }
            j.post(
                cx,
                Entry {
                    id: format!("kalshi:settle:{}:{}", s.ticker, s.settled_time),
                    ts: 0,
                    source: Source::VenueSettlement,
                    memo: format!("settle {}", s.ticker),
                    postings,
                },
            )?;
            lots.insert(s.ticker.clone(), Lot { qty: zero, basis: zero });
        }

        // NOTE: no separate margin posting for Kalshi. The $1.00/contract
        // collateral is already inside a short's cost basis — opening a short
        // at yes_px costs (1 - yes_px), which IS the $1 withheld net of the
        // proceeds received, and `balance_dollars` is reported net of it. A
        // margin account here would double-count. PM-US differs: it reports
        // `marginRequirement` explicitly and `currentBalance` GROSS of it, so
        // that importer does need `accounts::MARGIN_PMUS`.
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn imp(fills: Vec<Fill>, settlements: Vec<Settlement>) -> KalshiImport {
        KalshiImport { deposits: vec![], fills, settlements }
    }

    /// Compare numerically, not by string: `Dec` display carries the scale of
    /// the arithmetic that produced it ("5" vs "5.00"), which is a
    /// presentation artifact, not a value difference.
    #[track_caller]
    fn assert_amount(cx: &mut Cx, actual: D, expected: &str) {
        let want = cx.parse_exact(expected);
        assert_eq!(
            cx.cmp(actual, want),
            std::cmp::Ordering::Equal,
            "expected {expected}, got {actual}"
        );
    }

    fn fill(id: &str, ticker: &str, action: &str, qty: &str, px: &str, fee: &str, ts: i64) -> Fill {
        Fill {
            fill_id: id.into(),
            ticker: ticker.into(),
            action: action.into(),
            side: if action == "buy" { "yes".into() } else { "no".into() },
            count_fp: qty.into(),
            yes_price_dollars: px.into(),
            fee_cost: fee.into(),
            is_taker: true,
            ts,
        }
    }

    /// Buy 10 @ 0.20 (fee 0.05), sell 10 @ 0.30 (fee 0.05).
    /// Cash: -2.05 then +2.95 => +0.90. Realized 1.00, fees 0.10.
    #[test]
    fn round_trip_long_realizes_gain_net_of_fees() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        imp(
            vec![
                fill("f1", "KXA", "buy", "10", "0.20", "0.05", 1),
                fill("f2", "KXA", "sell", "10", "0.30", "0.05", 2),
            ],
            vec![],
        )
        .apply(&mut cx, &mut j)
        .expect("applies");

        let v = j.balance(&mut cx, accounts::CASH_KALSHI); assert_amount(&mut cx, v, "0.90");
        let v = j.balance(&mut cx, accounts::PNL_REALIZED); assert_amount(&mut cx, v, "-1.00");
        let v = j.balance(&mut cx, accounts::FEES_KALSHI_TAKER); assert_amount(&mut cx, v, "0.10");
        let v = j.qty(&mut cx, &accounts::position("kalshi", "KXA")); assert_amount(&mut cx, v, "0");
    }

    /// A sell with no long behind it opens a short, which is buying NO at
    /// (1 - yes_px): selling 5 @ 0.20 costs 5*0.80 = 4.00 of cash. That 4.00
    /// IS the $5.00 collateral net of the $1.00 proceeds, and Kalshi reports
    /// `balance_dollars` already net of it — so there is no margin posting.
    #[test]
    fn sell_beyond_long_opens_short_and_costs_cash() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        imp(vec![fill("f1", "KXB", "sell", "5", "0.20", "0", 1)], vec![])
            .apply(&mut cx, &mut j)
            .expect("applies");

        let v = j.qty(&mut cx, &accounts::position("kalshi", "KXB")); assert_amount(&mut cx, v, "-5");
        let v = j.balance(&mut cx, &accounts::position("kalshi", "KXB")); assert_amount(&mut cx, v, "4.00");
        let v = j.balance(&mut cx, accounts::CASH_KALSHI); assert_amount(&mut cx, v, "-4.00");
        let v = j.balance(&mut cx, accounts::MARGIN_KALSHI); assert_amount(&mut cx, v, "0");
    }

    /// The asymmetry that put the real reconciliation $27.01 out: a BUY that
    /// covers a short must relieve the short's basis and realize, not pile on
    /// more basis. Short 5 @ 0.20 (basis 4.00), then buy 5 back @ 0.10 (NO
    /// worth 0.90) => proceeds 4.50, realized +0.50, flat position, zero basis.
    #[test]
    fn buy_covering_a_short_relieves_basis_and_realizes() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        imp(
            vec![
                fill("f1", "KXD", "sell", "5", "0.20", "0", 1),
                fill("f2", "KXD", "buy", "5", "0.10", "0", 2),
            ],
            vec![],
        )
        .apply(&mut cx, &mut j)
        .expect("applies");

        let v = j.qty(&mut cx, &accounts::position("kalshi", "KXD")); assert_amount(&mut cx, v, "0");
        let v = j.balance(&mut cx, &accounts::position("kalshi", "KXD")); assert_amount(&mut cx, v, "0");
        let v = j.balance(&mut cx, accounts::PNL_REALIZED); assert_amount(&mut cx, v, "-0.50");
        let v = j.balance(&mut cx, accounts::CASH_KALSHI); assert_amount(&mut cx, v, "0.50");
    }

    /// Settlement relieves basis and credits NET revenue in cents.
    #[test]
    fn settlement_relieves_basis_and_realizes() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        imp(
            vec![fill("f1", "KXC", "buy", "10", "0.40", "0", 1)],
            vec![Settlement {
                ticker: "KXC".into(),
                revenue: 1000,
                settled_time: "t".into(),
                yes_total_cost_dollars: "4.00".into(),
                no_total_cost_dollars: "0".into(),
            }],
        )
        .apply(&mut cx, &mut j)
        .expect("applies");

        let v = j.balance(&mut cx, accounts::CASH_KALSHI); assert_amount(&mut cx, v, "6.00");
        let v = j.balance(&mut cx, accounts::PNL_REALIZED); assert_amount(&mut cx, v, "-6.00");
        let v = j.qty(&mut cx, &accounts::position("kalshi", "KXC")); assert_amount(&mut cx, v, "0");
    }
}
