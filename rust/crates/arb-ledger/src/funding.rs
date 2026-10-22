//! Funding: cash that entered or left an account without a trade, booked from
//! each venue's own history like any fill.
//!
//! * Kalshi: `/portfolio/deposits` and `/portfolio/withdrawals`.
//! * PM-US: the balance-change rows of `/v1/portfolio/activities`
//!   (`pmus-activities-endpoint` in docs/venue-quirks.md).
//!
//! Deposits and withdrawals are contributed capital, `equity:capital`.
//! Everything else PM-US credits is income it paid us, booked to
//! `income:venue_credits:pmus` so it lands in profit and not in capital:
//! liquidity-program payouts (which arrive as `ACTIVITY_TYPE_TRANSFER`), deposit
//! bonuses (`ACTIVITY_TYPE_REFERRAL_BONUS`), fee rebates, goodwill credits.
//!
//! A status whose cash effect is not established is REFUSED rather than
//! guessed. A deposit that has not landed is skipped (it cannot have moved the
//! balance yet); a withdrawal or a partial refund in any state but complete is
//! an error, because guessing either way misstates capital.

use std::collections::BTreeMap;

use arb_core::scan::{Cx, D};
use serde_json::Value;

use crate::kalshi::Deposit;
use crate::{accounts, neg, Entry, Posting, Source};

/// One entry's effect on capital and on venue-credit income, positive = in.
#[derive(Debug, Clone, PartialEq)]
pub struct Flow {
    pub ts: i64,
    pub venue: &'static str,
    pub capital: D,
    pub credit: D,
}

/// The venue that issued a funding entry, from its id prefix.
fn venue_of(id: &str) -> Option<&'static str> {
    if id.starts_with("kalshi:") {
        Some("kalshi")
    } else if id.starts_with("pmus:") {
        Some("polymarket_us")
    } else {
        None
    }
}

fn cents(cx: &mut Cx, c: i64) -> D {
    let c = cx.from_i64(c);
    let hundred = cx.from_i64(100);
    cx.div(c, hundred)
}

fn capital_entry(cx: &mut Cx, id: String, ts: i64, memo: String, cash_acct: &str, amount: D) -> Entry {
    let credit = neg(cx, amount);
    Entry {
        id,
        ts,
        source: Source::VenueDeposit,
        memo,
        postings: vec![
            Posting::new(cash_acct, amount),
            Posting::new(accounts::EQUITY_CAPITAL, credit),
        ],
    }
}

/// An `applied` Kalshi deposit; `None` for one that has not landed.
pub fn kalshi_deposit(cx: &mut Cx, d: &Deposit) -> Option<Entry> {
    if d.status != "applied" {
        return None;
    }
    let amt = cents(cx, d.amount_cents);
    Some(capital_entry(
        cx,
        format!("kalshi:deposit:{}", d.id),
        d.created_ts,
        format!("kalshi deposit {}", d.id),
        accounts::CASH_KALSHI,
        amt,
    ))
}

/// Kalshi deposits and withdrawals. Withdrawals share the deposit shape
/// (`amount_cents`, `status`, `created_ts`). No withdrawal has been seen, so
/// `applied` is the only status booked and any other is an error.
pub fn kalshi(cx: &mut Cx, deposits: &[Deposit], withdrawals: &[Deposit]) -> Result<Vec<Entry>, String> {
    let mut out: Vec<Entry> = deposits.iter().filter_map(|d| kalshi_deposit(cx, d)).collect();
    for w in withdrawals {
        if w.status != "applied" {
            return Err(format!(
                "kalshi withdrawal {} has status `{}`, whose effect on the balance is not \
                 established; refusing to guess",
                w.id, w.status
            ));
        }
        let amt = cents(cx, w.amount_cents.abs());
        let out_amt = neg(cx, amt);
        out.push(capital_entry(
            cx,
            format!("kalshi:withdrawal:{}", w.id),
            w.created_ts,
            format!("kalshi withdrawal {}", w.id),
            accounts::CASH_KALSHI,
            out_amt,
        ));
    }
    Ok(out)
}

/// `YYYY-MM-DDTHH:MM:SS[.fraction]Z` -> epoch seconds. The one shape PM-US
/// writes; anything else is refused rather than read loosely.
fn epoch_s(s: &str) -> Option<i64> {
    let b = s.as_bytes();
    if b.len() < 20 || b[10] != b'T' || b[13] != b':' || b[16] != b':' || !s.ends_with('Z') {
        return None;
    }
    let frac = &s[19..s.len() - 1];
    if !(frac.is_empty() || (frac.starts_with('.') && frac[1..].bytes().all(|c| c.is_ascii_digit()))) {
        return None;
    }
    let day = arb_core::resolve::parse_iso(s)?;
    let h: i64 = s.get(11..13)?.parse().ok()?;
    let m: i64 = s.get(14..16)?.parse().ok()?;
    let sec: i64 = s.get(17..19)?.parse().ok()?;
    if h > 23 || m > 59 || sec > 60 {
        return None;
    }
    Some(day * 86400 + h * 3600 + m * 60 + sec)
}

/// A plain signed decimal, so `"Infinity"` or `"NaN"` (both of which the
/// decimal parser accepts) can never become an amount.
fn plain_decimal(s: &str) -> bool {
    let digits = s.strip_prefix('-').unwrap_or(s);
    let mut parts = digits.splitn(2, '.');
    let int = parts.next().unwrap_or("");
    let frac = parts.next().unwrap_or("0");
    !int.is_empty() && int.bytes().all(|c| c.is_ascii_digit()) && !frac.is_empty() && frac.bytes().all(|c| c.is_ascii_digit())
}

/// The balance-change rows of PM-US activity history. Trade and resolution
/// rows are ignored here.
pub fn pmus(cx: &mut Cx, activities: &[Value]) -> Result<Vec<Entry>, String> {
    let mut out = Vec::new();
    for a in activities {
        let kind = a["type"].as_str().unwrap_or("");
        let b = &a["accountBalanceChange"];
        if b.is_null() {
            if kind == "ACTIVITY_TYPE_TRADE" || kind == "ACTIVITY_TYPE_POSITION_RESOLUTION" {
                continue;
            }
            return Err(format!("pmus activity of type `{kind}` has no accountBalanceChange"));
        }
        let id = b["transactionId"].as_str().ok_or("pmus balance change without transactionId")?;
        let status = b["status"].as_str().unwrap_or("");
        let value = b["amount"]["value"].as_str().unwrap_or("");
        if !plain_decimal(value) {
            return Err(format!("pmus balance change {id}: amount `{value}` is not a plain decimal"));
        }
        let ts = b["createTime"]
            .as_str()
            .and_then(epoch_s)
            .ok_or_else(|| format!("pmus balance change {id}: unreadable createTime"))?;
        let deposit = matches!(kind, "ACTIVITY_TYPE_ACCOUNT_DEPOSIT" | "ACTIVITY_TYPE_ACCOUNT_ADVANCED_DEPOSIT");
        let withdrawal = kind == "ACTIVITY_TYPE_ACCOUNT_WITHDRAWAL";
        let credit = matches!(
            kind,
            "ACTIVITY_TYPE_TRANSFER"
                | "ACTIVITY_TYPE_REFERRAL_BONUS"
                | "ACTIVITY_TYPE_TAKER_FEE_REBATE"
                | "ACTIVITY_TYPE_LIQUIDITY_PROGRAM"
        );
        if !(deposit || withdrawal || credit) {
            return Err(format!("pmus balance change {id} has unknown type `{kind}`"));
        }
        match status {
            "ACCOUNT_BALANCE_CHANGE_STATUS_COMPLETED" => {}
            "ACCOUNT_BALANCE_CHANGE_STATUS_PENDING" | "ACCOUNT_BALANCE_CHANGE_STATUS_REJECTED"
                if !withdrawal =>
            {
                continue
            }
            other => {
                return Err(format!(
                    "pmus {kind} {id} has status `{other}`, whose effect on the balance is not \
                     established; refusing to guess"
                ))
            }
        }
        let amt = cx.parse_exact(value);
        let reason = b["transferReason"].as_str().unwrap_or("");
        let label = kind.trim_start_matches("ACTIVITY_TYPE_").to_ascii_lowercase();
        let memo = if reason.is_empty() || reason == "TRANSFER_REASON_UNSPECIFIED" {
            format!("pmus {label} {id}")
        } else {
            format!("pmus {label} {id} ({})", reason.trim_start_matches("TRANSFER_REASON_").to_ascii_lowercase())
        };
        let entry_id = format!("pmus:funding:{id}");
        if credit {
            let income = neg(cx, amt);
            out.push(Entry {
                id: entry_id,
                ts,
                source: Source::VenueDeposit,
                memo,
                postings: vec![
                    Posting::new(accounts::CASH_PMUS, amt),
                    Posting::new(accounts::INCOME_CREDITS_PMUS, income),
                ],
            });
        } else {
            // PM-US's sign convention for a withdrawal amount is unobserved;
            // the direction comes from the type, never from the sign.
            let zero = cx.zero();
            let mag = if cx.cmp(amt, zero) == std::cmp::Ordering::Less { neg(cx, amt) } else { amt };
            let signed = if withdrawal { neg(cx, mag) } else { mag };
            out.push(capital_entry(cx, entry_id, ts, memo, accounts::CASH_PMUS, signed));
        }
    }
    Ok(out)
}

/// What each funding entry did to capital and to credit income.
pub fn flows(cx: &mut Cx, entries: &[Entry]) -> Vec<Flow> {
    let mut out = Vec::new();
    for e in entries {
        let Some(venue) = venue_of(&e.id) else { continue };
        let mut capital = cx.zero();
        let mut credit = cx.zero();
        for p in &e.postings {
            let a = p.amount(cx);
            if p.account == accounts::EQUITY_CAPITAL {
                capital = cx.sub(capital, a);
            } else if p.account.starts_with("income:") {
                credit = cx.sub(credit, a);
            }
        }
        out.push(Flow { ts: e.ts, venue, capital, credit });
    }
    out
}

/// Capital in and credits received, per venue.
pub fn totals(cx: &mut Cx, flows: &[Flow]) -> BTreeMap<&'static str, (D, D)> {
    let mut out: BTreeMap<&'static str, (D, D)> = BTreeMap::new();
    for f in flows {
        let zero = cx.zero();
        let (c, k) = out.get(f.venue).copied().unwrap_or((zero, zero));
        let c = cx.add(c, f.capital);
        let k = cx.add(k, f.credit);
        out.insert(f.venue, (c, k));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Journal;
    use serde_json::json;

    fn change(kind: &str, id: &str, status: &str, value: &str) -> Value {
        json!({"type": kind, "accountBalanceChange": {
            "transactionId": id, "status": format!("ACCOUNT_BALANCE_CHANGE_STATUS_{status}"),
            "amount": {"value": value, "currency": "USD"},
            "createTime": "2026-07-21T13:14:49.250563463Z"}})
    }

    fn dep(id: &str, cents: i64, status: &str) -> Deposit {
        Deposit { id: id.into(), amount_cents: cents, status: status.into(), created_ts: 1 }
    }

    #[track_caller]
    fn amount(cx: &mut Cx, v: D, want: &str) {
        let w = cx.parse_exact(want);
        assert_eq!(cx.cmp(v, w), std::cmp::Ordering::Equal, "want {want}, got {v}");
    }

    /// The 2026-10-22 shape of the account: one deposit, a deposit bonus, a
    /// goodwill transfer and liquidity payouts that arrive as TRANSFER. Only
    /// the deposit is capital; the rest is income.
    #[test]
    fn only_deposits_are_capital_and_every_other_credit_is_income() {
        let mut cx = Cx::default();
        let acts = vec![
            change("ACTIVITY_TYPE_ACCOUNT_DEPOSIT", "D1", "COMPLETED", "500"),
            change("ACTIVITY_TYPE_REFERRAL_BONUS", "B1", "COMPLETED", "50"),
            change("ACTIVITY_TYPE_TRANSFER", "T1", "COMPLETED", "25"),
            change("ACTIVITY_TYPE_TRANSFER", "T2", "COMPLETED", "3.76"),
            json!({"type": "ACTIVITY_TYPE_TRADE", "trade": {"id": "X"}}),
        ];
        let entries = pmus(&mut cx, &acts).unwrap();
        let mut j = Journal::new();
        for e in entries.clone() {
            j.post(&mut cx, e).unwrap();
        }
        let v = j.balance(&mut cx, accounts::CASH_PMUS);
        amount(&mut cx, v, "578.76");
        let v = j.balance(&mut cx, accounts::EQUITY_CAPITAL);
        amount(&mut cx, v, "-500");
        let v = j.balance(&mut cx, accounts::INCOME_CREDITS_PMUS);
        amount(&mut cx, v, "-78.76");
        let f = flows(&mut cx, &entries);
        assert_eq!(f[0].ts, 1784639689);
        let t = totals(&mut cx, &f);
        let (c, k) = t["polymarket_us"];
        amount(&mut cx, c, "500");
        amount(&mut cx, k, "78.76");
    }

    #[test]
    fn a_withdrawal_takes_capital_out_whatever_sign_the_venue_writes() {
        let mut cx = Cx::default();
        for v in ["40", "-40"] {
            let e = pmus(&mut cx, &[change("ACTIVITY_TYPE_ACCOUNT_WITHDRAWAL", "W", "COMPLETED", v)]).unwrap();
            let f = flows(&mut cx, &e);
            amount(&mut cx, f[0].capital, "-40");
        }
        let e = kalshi(&mut cx, &[dep("a", 43998, "applied"), dep("b", 100, "pending")], &[dep("w", 1000, "applied")])
            .unwrap();
        let f = flows(&mut cx, &e);
        let t = totals(&mut cx, &f);
        amount(&mut cx, t["kalshi"].0, "429.98");
    }

    /// A deposit that has not landed has not moved the balance; a withdrawal or
    /// a partial refund in a state we have never seen could have, either way.
    #[test]
    fn an_unestablished_status_is_refused_not_guessed() {
        let mut cx = Cx::default();
        assert!(pmus(&mut cx, &[change("ACTIVITY_TYPE_ACCOUNT_DEPOSIT", "P", "PENDING", "9")]).unwrap().is_empty());
        assert!(pmus(&mut cx, &[change("ACTIVITY_TYPE_ACCOUNT_WITHDRAWAL", "P", "PENDING", "9")]).is_err());
        assert!(pmus(&mut cx, &[change("ACTIVITY_TYPE_TRANSFER", "R", "PARTIALLY_REFUNDED", "9")]).is_err());
        assert!(kalshi(&mut cx, &[], &[dep("w", 1, "pending")]).is_err());
        assert!(pmus(&mut cx, &[change("ACTIVITY_TYPE_SOMETHING_NEW", "N", "COMPLETED", "9")]).is_err());
    }

    #[test]
    fn an_amount_or_time_that_is_not_plain_is_refused() {
        let mut cx = Cx::default();
        assert!(pmus(&mut cx, &[change("ACTIVITY_TYPE_TRANSFER", "I", "COMPLETED", "Infinity")]).is_err());
        let mut bad_time = change("ACTIVITY_TYPE_TRANSFER", "T", "COMPLETED", "1");
        bad_time["accountBalanceChange"]["createTime"] = json!("2026-07-21 13:14:49");
        assert!(pmus(&mut cx, &[bad_time]).is_err());
        assert_eq!(epoch_s("2026-07-21T13:14:49Z"), Some(1784639689));
    }
}
