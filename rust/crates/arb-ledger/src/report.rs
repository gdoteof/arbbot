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
    /// Everything that matched none of the buckets above — `margin:*`,
    /// `equity:opening_balance`, `suspense:*`. Six accounts in the chart do, and
    /// they used to drop out of `total_assets` silently, which turned a $20
    /// PM-US reconciliation gap into `net_vs_capital: -20.00` with
    /// `trading_realized: 0` and `fees: 0` — a $20 trading loss no trade
    /// produced — while `balanced` stayed true and the same report's `accounts`
    /// list showed the $20 sitting in suspense. Nothing in production posts
    /// those accounts YET, but `kalshi.rs`'s note says the PM-US importer needs
    /// `MARGIN_PMUS`, so the first importer that posts margin makes the
    /// statement wrong. Naming the residue makes the identity checkable:
    /// `net_vs_capital == trading_realized - fees - unclassified`.
    pub unclassified: String,
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
    let mut unclassified = cx.zero();
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
        } else {
            // The residue, named rather than dropped. See `Statement`.
            unclassified = cx.add(unclassified, *bal);
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
            unclassified: unclassified.to_string(),
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

    /// The identity that CAN fail:
    /// `net_vs_capital == trading_realized - fees - unclassified`.
    ///
    /// It is not forced by the journal — every entry sums to zero regardless —
    /// it holds because `build` routes every account into exactly one bucket
    /// INCLUDING the catch-all. An earlier version of this comment claimed the
    /// five named prefixes covered everything; they do not (six accounts in
    /// `accounts.rs` match none), which is why `unclassified` exists and why the
    /// identity has a third term. Misfile one prefix — drop `expense:fees:`, or
    /// classify a `pos:` account as cash — and this goes red while the trial
    /// balance stays a contented zero. That is why the removed
    /// `assert!(r.balanced)` was worth nothing here: `post` already refuses
    /// anything that could make it false.
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
        assert_eq!(r.statement.capital_in, "100.00");
        assert_eq!(r.statement.total_assets, "100.00");
        let net = cx.parse_exact(&r.statement.net_vs_capital);
        assert!(is_zero(&mut cx, net), "nothing traded yet: {net}");

        // Now trade: buy 10 @ 0.40 with a 0.05 fee, sell them at 0.50.
        let cost = cx.parse_exact("4.00");
        let fee = cx.parse_exact("0.05");
        let out = cx.parse_exact("-4.05");
        let qty = cx.parse_exact("10");
        j.post(
            &mut cx,
            Entry {
                id: "buy".into(),
                ts: 1,
                source: Source::VenueFill,
                memo: String::new(),
                postings: vec![
                    Posting::with_qty("pos:kalshi:KXA", cost, qty),
                    Posting::new(accounts::FEES_KALSHI_TAKER, fee),
                    Posting::new(accounts::CASH_KALSHI, out),
                ],
            },
        )
        .unwrap();
        let proceeds = cx.parse_exact("5.00");
        let relieve = cx.parse_exact("-4.00");
        let gain = cx.parse_exact("-1.00");
        let flat = cx.parse_exact("-10");
        j.post(
            &mut cx,
            Entry {
                id: "sell".into(),
                ts: 2,
                source: Source::VenueFill,
                memo: String::new(),
                postings: vec![
                    Posting::new(accounts::CASH_KALSHI, proceeds),
                    Posting::with_qty("pos:kalshi:KXA", relieve, flat),
                    Posting::new(accounts::PNL_REALIZED, gain),
                ],
            },
        )
        .unwrap();

        let r = build(&mut cx, &j);
        assert_eq!(r.statement.trading_realized, "1.00");
        assert_eq!(r.statement.fees, "0.05");
        assert_eq!(r.statement.unclassified, "0", "nothing outside the buckets");
        assert_identity(&mut cx, &r);
    }

    /// `net_vs_capital == trading_realized - fees - unclassified`, checked from
    /// the report's own serialized strings — which is what a reader sees.
    #[track_caller]
    fn assert_identity(cx: &mut Cx, r: &Report) {
        let net = cx.parse_exact(&r.statement.net_vs_capital);
        let trading = cx.parse_exact(&r.statement.trading_realized);
        let fees = cx.parse_exact(&r.statement.fees);
        let unc = cx.parse_exact(&r.statement.unclassified);
        let want = {
            let a = cx.sub(trading, fees);
            cx.sub(a, unc)
        };
        assert_eq!(
            cx.cmp(net, want),
            std::cmp::Ordering::Equal,
            "net {net} != trading - fees - unclassified {want}: an account escaped its bucket"
        );
    }

    /// A suspense balance must not read as a trading loss.
    ///
    /// `suspense:venue_discrepancy:pmus` is the crate's own honest "we do not
    /// understand $X" account, and it matched no bucket: a $20 gap surfaced as
    /// `net_vs_capital: -20.00` against `trading_realized: 0` and `fees: 0` — a
    /// loss with no trade behind it — while `balanced` stayed true. Now the $20
    /// is named, and the identity accounts for it.
    #[test]
    fn a_suspense_balance_is_named_not_disguised_as_a_trading_loss() {
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
                    Posting::new(accounts::CASH_PMUS, d),
                    Posting::new(accounts::EQUITY_CAPITAL, c),
                ],
            },
        )
        .unwrap();
        // The venue says we hold $20 less than the books do, cause unknown.
        let gap = cx.parse_exact("20.00");
        let out = neg(&mut cx, gap);
        j.post(
            &mut cx,
            Entry {
                id: "recon".into(),
                ts: 1,
                source: Source::Reconciliation,
                memo: "venue is $20 short of books".into(),
                postings: vec![
                    Posting::new(accounts::SUSPENSE_PMUS, gap),
                    Posting::new(accounts::CASH_PMUS, out),
                ],
            },
        )
        .unwrap();

        let r = build(&mut cx, &j);
        assert_eq!(r.statement.unclassified, "20.00", "the gap is NAMED");
        assert_eq!(r.statement.trading_realized, "0", "no trade produced it");
        assert_eq!(r.statement.fees, "0");
        assert_eq!(r.statement.net_vs_capital, "-20.00");
        assert_identity(&mut cx, &r);
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
