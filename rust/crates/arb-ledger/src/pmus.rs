//! Polymarket US importer: opening balances from venue truth + one named plug.
//!
//! PM-US is the opposite of Kalshi. It exposes ONLY current balances and
//! current positions — no fills, no settlements, no deposits
//! (`pmus-no-history-endpoints` in docs/venue-quirks.md). So the account cannot
//! be rebuilt; it can only be OPENED at a point in time:
//!
//!   1. the funding total, which a human supplies (Geoff: $525 = a $500
//!      deposit plus a $25 outage-goodwill credit),
//!   2. every currently-open position at the venue's own `baseCost`, with its
//!      `fees` booked as a real expense,
//!   3. one reconciling entry to `pnl:realized:pmus_unitemized` for the
//!      residual — P&L on positions that closed and vanished.
//!
//! Cash convention matches Kalshi: `cash:pmus` holds the SPENDABLE figure
//! (`buyingPower`), not `currentBalance`. Both venues withhold $1.00 per short
//! contract, and in both the collateral is already inside the position's cost
//! basis, so booking it again as margin would double-count. `marginRequirement`
//! is reported, not posted.

use arb_core::scan::{Cx, D};
use serde::Deserialize;

use crate::{accounts, is_zero, neg, Entry, Journal, LedgerError, Posting, Source};

/// PM-US wraps money as `{"value": "77.2800", "currency": "USD"}` in positions
/// but as a bare JSON number in balances. Accept both, and go through the JSON
/// literal rather than an f64 so no binary-float rounding enters the books.
fn money(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(o) => o
            .get("value")
            .map(|x| money(x))
            .unwrap_or_else(|| "0".into()),
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Number(n) => n.to_string(),
        _ => "0".into(),
    }
}

#[derive(Debug, Deserialize)]
pub struct Balances {
    #[serde(rename = "buyingPower")]
    pub buying_power: serde_json::Value,
    #[serde(rename = "currentBalance")]
    pub current_balance: serde_json::Value,
    #[serde(rename = "marginRequirement")]
    #[serde(default)]
    pub margin_requirement: serde_json::Value,
}

impl Balances {
    /// The SPENDABLE figure, and the one `cash:pmus` reconciles against.
    pub fn buying_power_str(&self) -> String {
        money(&self.buying_power)
    }

    pub fn current_balance_str(&self) -> String {
        money(&self.current_balance)
    }

    /// Reported, never posted — see the module docs.
    pub fn margin_requirement_str(&self) -> String {
        money(&self.margin_requirement)
    }
}

#[derive(Debug, Deserialize)]
pub struct Position {
    #[serde(rename = "marketMetadata")]
    pub market_metadata: serde_json::Value,
    #[serde(rename = "netPositionDecimal")]
    pub net_position: String,
    #[serde(rename = "baseCost")]
    pub base_cost: serde_json::Value,
    #[serde(default)]
    pub fees: serde_json::Value,
}

impl Position {
    pub fn slug(&self) -> String {
        self.market_metadata
            .get("slug")
            .and_then(|s| s.as_str())
            .unwrap_or("unknown")
            .to_string()
    }
}

pub struct PmusImport {
    /// Human-supplied funding total. PM-US has no deposits endpoint.
    pub deposits_usd: String,
    pub balances: Balances,
    pub positions: Vec<Position>,
}

impl PmusImport {
    pub fn apply(self, cx: &mut Cx, j: &mut Journal) -> Result<(), LedgerError> {
        let dep = cx.parse_exact(&self.deposits_usd);
        let credit = neg(cx, dep);
        j.post(
            cx,
            Entry {
                id: "pmus:opening:capital".into(),
                ts: 0,
                source: Source::Manual,
                memo: format!(
                    "pmus funding {} (operator-supplied; venue exposes no deposits endpoint)",
                    self.deposits_usd
                ),
                postings: vec![
                    Posting::new(accounts::CASH_PMUS, dep),
                    Posting::new(accounts::EQUITY_CAPITAL, credit),
                ],
            },
        )?;

        let mut positions: Vec<&Position> = self.positions.iter().collect();
        positions.sort_by_key(|p| p.slug());
        for p in positions {
            let slug = p.slug();
            let basis = cx.parse_exact(&money(&p.base_cost));
            let fee = cx.parse_exact(&money(&p.fees));
            let qty = cx.parse_exact(&p.net_position);
            if is_zero(cx, basis) && is_zero(cx, qty) {
                continue;
            }
            // Cash actually left the account at `cost` = baseCost + fees; the
            // venue's `cost` field is the fee-inclusive one, `baseCost` is not.
            let gross = cx.add(basis, fee);
            let acct = accounts::position("pmus", &slug);
            let mut postings = vec![Posting::with_qty(&acct, basis, qty)];
            if !is_zero(cx, fee) {
                postings.push(Posting::new(accounts::FEES_PMUS_TAKER, fee));
            }
            postings.push(Posting::new(accounts::CASH_PMUS, neg(cx, gross)));
            j.post(
                cx,
                Entry {
                    id: format!("pmus:opening:position:{slug}"),
                    ts: 0,
                    source: Source::Reconciliation,
                    memo: format!("pmus opening position {slug} at venue baseCost"),
                    postings,
                },
            )?;
        }

        // Whatever is left between the books and the venue's spendable cash is
        // realized P&L on positions that closed and vanished. Named, not hidden.
        let want = cx.parse_exact(&money(&self.balances.buying_power));
        let have = j.balance(cx, accounts::CASH_PMUS);
        let gap = cx.sub(want, have);
        if !is_zero(cx, gap) {
            let postings = vec![
                Posting::new(accounts::CASH_PMUS, gap),
                Posting::new(accounts::PNL_PMUS_UNITEMIZED, neg(cx, gap)),
            ];
            j.post(
                cx,
                Entry {
                    id: "pmus:opening:unitemized-pnl".into(),
                    ts: 0,
                    source: Source::Reconciliation,
                    memo: "residual to venue buyingPower: realized P&L on closed \
                           PM-US positions, unrecoverable (no history endpoints)"
                        .into(),
                    postings,
                },
            )?;
        }
        Ok(())
    }

    /// Reported, never posted — the collateral is already inside position basis.
    pub fn margin_requirement(&self) -> String {
        money(&self.balances.margin_requirement)
    }

    pub fn current_balance(&self) -> String {
        money(&self.balances.current_balance)
    }
}

/// Sum of |qty| over short positions. PM-US withholds $1.00 per short contract,
/// so this should equal `marginRequirement` exactly — a free cross-check that
/// the position set is complete.
pub fn short_contracts(cx: &mut Cx, positions: &[Position]) -> D {
    let mut total = cx.zero();
    for p in positions {
        let q = cx.parse_exact(&p.net_position);
        let zero = cx.zero();
        if cx.cmp(q, zero) == std::cmp::Ordering::Less {
            let m = neg(cx, q);
            total = cx.add(total, m);
        }
    }
    total
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pos(slug: &str, net: &str, base: &str, fee: &str) -> Position {
        Position {
            market_metadata: serde_json::json!({ "slug": slug }),
            net_position: net.into(),
            base_cost: serde_json::json!({ "value": base, "currency": "USD" }),
            fees: serde_json::json!({ "value": fee, "currency": "USD" }),
        }
    }

    fn bal(bp: &str, cb: &str, mr: &str) -> Balances {
        Balances {
            buying_power: serde_json::json!(bp),
            current_balance: serde_json::json!(cb),
            margin_requirement: serde_json::json!(mr),
        }
    }

    #[test]
    fn opening_books_positions_fees_and_names_the_residual() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        PmusImport {
            deposits_usd: "525.00".into(),
            balances: bal("329.29805", "653.15805", "323.86"),
            positions: vec![pos("a", "-84", "77.28", "0.35"), pos("b", "-50", "44.00", "0.30")],
        }
        .apply(&mut cx, &mut j)
        .expect("applies");

        let tb = j.trial_balance(&mut cx);
        assert!(is_zero(&mut cx, tb), "trial balance {tb}");

        // cash lands exactly on the venue's spendable figure
        let cash = j.balance(&mut cx, accounts::CASH_PMUS);
        let want = cx.parse_exact("329.29805");
        assert_eq!(cx.cmp(cash, want), std::cmp::Ordering::Equal, "cash {cash}");

        let fees = j.balance(&mut cx, accounts::FEES_PMUS_TAKER);
        let want_fees = cx.parse_exact("0.65");
        assert_eq!(cx.cmp(fees, want_fees), std::cmp::Ordering::Equal, "fees {fees}");

        // 525 - (77.28+0.35) - (44.00+0.30) = 403.07; venue says 329.29805, so
        // 73.77195 was lost on positions that closed and vanished (debit).
        let resid = j.balance(&mut cx, accounts::PNL_PMUS_UNITEMIZED);
        let want_resid = cx.parse_exact("73.77195");
        assert_eq!(cx.cmp(resid, want_resid), std::cmp::Ordering::Equal, "residual {resid}");
    }

    #[test]
    fn short_contracts_should_equal_margin_requirement() {
        let mut cx = Cx::default();
        let positions = vec![pos("a", "-84", "77.28", "0"), pos("b", "-50", "44.00", "0")];
        let n = short_contracts(&mut cx, &positions);
        let want = cx.parse_exact("134");
        assert_eq!(cx.cmp(n, want), std::cmp::Ordering::Equal, "short {n}");
    }
}
