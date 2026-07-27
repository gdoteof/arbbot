//! The serializable view of the books — one shape, shared by the CLI and the
//! dashboard so they can never disagree about a number.
//!
//! Everything here is COST basis. Marks are deliberately excluded: the hard
//! book (cash + cost) is the part that reconciles to venue truth, and mixing a
//! mark into it is how the old accounting produced numbers nobody trusted.

use arb_core::scan::{Cx, D};
use serde::Serialize;

use crate::{accounts, is_zero, neg, Journal};

#[derive(Debug, Serialize)]
pub struct AccountRow {
    pub account: String,
    pub balance: String,
}

#[derive(Debug, Serialize)]
pub struct PositionRow {
    pub account: String,
    pub venue: String,
    pub market: String,
    pub qty: String,
    pub cost: String,
}

#[derive(Debug, Serialize)]
pub struct Statement {
    pub capital_in: String,
    pub cash: String,
    pub positions_at_cost: String,
    pub total_assets: String,
    /// Positive = gain. Stored as a credit internally, flipped here.
    pub trading_realized: String,
    pub fees: String,
    pub net_vs_capital: String,
}

#[derive(Debug, Serialize)]
pub struct Reconciliation {
    pub account: String,
    pub books: String,
    pub venue: String,
    pub diff: String,
    pub exact: bool,
}

#[derive(Debug, Serialize)]
pub struct Report {
    pub entries: usize,
    pub trial_balance: String,
    pub balanced: bool,
    pub statement: Statement,
    pub accounts: Vec<AccountRow>,
    pub positions: Vec<PositionRow>,
    pub reconciliations: Vec<Reconciliation>,
}

fn split_position(account: &str) -> (String, String) {
    // "pos:<venue>:<market>" — market may itself contain ':'.
    let mut it = account.splitn(3, ':');
    it.next();
    let venue = it.next().unwrap_or("").to_string();
    let market = it.next().unwrap_or("").to_string();
    (venue, market)
}

pub fn build(cx: &mut Cx, j: &Journal) -> Report {
    let bals = j.balances(cx);
    let qtys = j.quantities(cx);

    let mut cash = cx.zero();
    let mut positions_cost = cx.zero();
    let mut fees = cx.zero();
    let mut realized = cx.zero();
    let mut capital = cx.zero();
    let mut accounts_out: Vec<AccountRow> = Vec::new();

    for (acct, bal) in &bals {
        if acct.starts_with("cash:") {
            cash = cx.add(cash, *bal);
        } else if acct.starts_with("pos:") {
            positions_cost = cx.add(positions_cost, *bal);
        } else if acct.starts_with("expense:fees:") {
            fees = cx.add(fees, *bal);
        } else if acct.starts_with("pnl:") {
            realized = cx.add(realized, *bal);
        } else if acct == accounts::EQUITY_CAPITAL {
            capital = cx.add(capital, *bal);
        }
        if !is_zero(cx, *bal) {
            accounts_out.push(AccountRow {
                account: acct.clone(),
                balance: bal.to_string(),
            });
        }
    }

    let capital_in = neg(cx, capital);
    let total_assets = cx.add(cash, positions_cost);
    let net = cx.sub(total_assets, capital_in);
    let trading = neg(cx, realized);

    let mut positions: Vec<PositionRow> = Vec::new();
    for (acct, qty) in &qtys {
        let (venue, market) = split_position(acct);
        let cost = match bals.get(acct) {
            Some(c) => c.to_string(),
            None => "0".into(),
        };
        positions.push(PositionRow {
            account: acct.clone(),
            venue,
            market,
            qty: qty.to_string(),
            cost,
        });
    }

    let tb = j.trial_balance(cx);
    let balanced = is_zero(cx, tb);

    Report {
        entries: j.len(),
        trial_balance: tb.to_string(),
        balanced,
        statement: Statement {
            capital_in: capital_in.to_string(),
            cash: cash.to_string(),
            positions_at_cost: positions_cost.to_string(),
            total_assets: total_assets.to_string(),
            trading_realized: trading.to_string(),
            fees: fees.to_string(),
            net_vs_capital: net.to_string(),
        },
        accounts: accounts_out,
        positions,
        reconciliations: Vec::new(),
    }
}

/// Compare a books account against a venue-reported figure. This is the only
/// claim the dashboard makes that a venue can contradict, so it is explicit.
pub fn reconcile(cx: &mut Cx, j: &Journal, account: &str, venue_value: &str) -> Reconciliation {
    let books = j.balance(cx, account);
    let venue: D = cx.parse_exact(venue_value);
    let diff = cx.sub(books, venue);
    Reconciliation {
        account: account.to_string(),
        books: books.to_string(),
        venue: venue.to_string(),
        diff: diff.to_string(),
        exact: is_zero(cx, diff),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Entry, Posting, Source};

    #[test]
    fn statement_identity_holds() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let d = cx.parse_exact("100.00");
        let c = neg(&mut cx, d);
        j.post(
            &mut cx,
            Entry {
                id: "d".into(),
                ts: 0,
                source: Source::VenueDeposit,
                memo: String::new(),
                postings: vec![
                    Posting::new(accounts::CASH_KALSHI, d),
                    Posting::new(accounts::EQUITY_CAPITAL, c),
                ],
            },
        )
        .unwrap();

        let r = build(&mut cx, &j);
        assert!(r.balanced);
        assert_eq!(r.statement.capital_in, "100.00");
        assert_eq!(r.statement.total_assets, "100.00");
        // net = assets - capital = 0, and trading - fees must equal it
        let net = cx.parse_exact(&r.statement.net_vs_capital);
        assert!(is_zero(&mut cx, net));
    }

    #[test]
    fn reconcile_flags_a_mismatch() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let d = cx.parse_exact("10.00");
        let c = neg(&mut cx, d);
        j.post(
            &mut cx,
            Entry {
                id: "d".into(),
                ts: 0,
                source: Source::VenueDeposit,
                memo: String::new(),
                postings: vec![
                    Posting::new(accounts::CASH_KALSHI, d),
                    Posting::new(accounts::EQUITY_CAPITAL, c),
                ],
            },
        )
        .unwrap();

        let ok = reconcile(&mut cx, &j, accounts::CASH_KALSHI, "10.00");
        assert!(ok.exact, "{ok:?}");
        let bad = reconcile(&mut cx, &j, accounts::CASH_KALSHI, "9.99");
        assert!(!bad.exact);
        assert_eq!(bad.diff, "0.01");
    }
}
