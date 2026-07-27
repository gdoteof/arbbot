//! arb-ledger: double-entry books for the cross-venue arb account.
//!
//! Replaces `src/arbbot/exec/accounting.py`'s `fold()`, whose stated invariant
//! `locked_at_open == realized + remaining_locked + slippage` is a TAUTOLOGY —
//! `accounting.py:110-111` defines `slippage` as exactly that residual, so it
//! holds for every possible input and can detect nothing. Here the invariant is
//! that every entry sums to zero and the trial balance stays zero, which CAN
//! fail, plus reconciliation against venue-reported cash and positions.
//!
//! Money is `arb_core::scan::D` in the shared 28-digit HALF_EVEN context —
//! the same arithmetic Python's `decimal` defaults to.
//!
//! Sign convention: debits positive, credits negative. Position accounts carry
//! a signed `qty` memo alongside a value that is the COST BASIS (always >= 0),
//! mirroring Kalshi, which reports `position_fp` signed but
//! `market_exposure_dollars` positive regardless of side.

use std::collections::BTreeMap;

use arb_core::scan::{Cx, D};

pub mod accounts;
pub mod kalshi;
pub mod pmus;

/// Where a posting's authority comes from. `Reconciliation` entries are the
/// only ones allowed to move a suspense account, and they must never be
/// written from a single unconfirmed venue read (see `pmus-positions-empty-glitch`
/// and `pmus-positions-partial-stale-sticky` in docs/venue-quirks.md).
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    VenueDeposit,
    VenueFill,
    VenueSettlement,
    Derived,
    Manual,
    Reconciliation,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Posting {
    pub account: String,
    /// Signed USD. Debit positive, credit negative.
    pub amount: String,
    /// Signed contract count, for position accounts only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub qty: Option<String>,
}

impl Posting {
    pub fn new(account: impl Into<String>, amount: D) -> Posting {
        Posting { account: account.into(), amount: amount.to_string(), qty: None }
    }

    pub fn with_qty(account: impl Into<String>, amount: D, qty: D) -> Posting {
        Posting {
            account: account.into(),
            amount: amount.to_string(),
            qty: Some(qty.to_string()),
        }
    }

    pub fn amount(&self, cx: &mut Cx) -> D {
        cx.parse_exact(&self.amount)
    }

    pub fn qty_d(&self, cx: &mut Cx) -> Option<D> {
        self.qty.as_ref().map(|q| cx.parse_exact(q))
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Entry {
    /// Stable, idempotent key. For venue-sourced entries this MUST derive from
    /// the venue's own id (fill_id, settlement ticker+time, deposit id) so a
    /// re-import is a no-op rather than a double posting.
    pub id: String,
    pub ts: i64,
    pub source: Source,
    pub memo: String,
    pub postings: Vec<Posting>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum LedgerError {
    /// Postings do not sum to zero. Carries the residual.
    Unbalanced { id: String, residual: String },
    Empty { id: String },
    Duplicate { id: String },
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LedgerError::Unbalanced { id, residual } => {
                write!(f, "entry {id} does not balance: residual {residual}")
            }
            LedgerError::Empty { id } => write!(f, "entry {id} has no postings"),
            LedgerError::Duplicate { id } => write!(f, "entry {id} already posted"),
        }
    }
}

#[derive(Default)]
pub struct Journal {
    entries: Vec<Entry>,
    seen: std::collections::HashSet<String>,
}

impl Journal {
    pub fn new() -> Journal {
        Journal::default()
    }

    /// Post an entry, enforcing I1 (sums to zero) and I8 (idempotent by id).
    /// Rejecting rather than repairing is deliberate: a book that silently
    /// absorbs an imbalance is the failure mode we are replacing.
    pub fn post(&mut self, cx: &mut Cx, e: Entry) -> Result<(), LedgerError> {
        if e.postings.is_empty() {
            return Err(LedgerError::Empty { id: e.id });
        }
        if self.seen.contains(&e.id) {
            return Err(LedgerError::Duplicate { id: e.id });
        }
        let mut sum = cx.zero();
        for p in &e.postings {
            let a = p.amount(cx);
            sum = cx.add(sum, a);
        }
        if !is_zero(cx, sum) {
            return Err(LedgerError::Unbalanced { id: e.id, residual: sum.to_string() });
        }
        self.seen.insert(e.id.clone());
        self.entries.push(e);
        Ok(())
    }

    /// Idempotent post: a duplicate id is a no-op, not an error. Use when
    /// re-importing venue history that may overlap what is already booked.
    pub fn post_idempotent(&mut self, cx: &mut Cx, e: Entry) -> Result<bool, LedgerError> {
        if self.seen.contains(&e.id) {
            return Ok(false);
        }
        self.post(cx, e).map(|_| true)
    }

    pub fn entries(&self) -> &[Entry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// I3: the trial balance must be exactly zero after every entry.
    pub fn trial_balance(&self, cx: &mut Cx) -> D {
        let mut sum = cx.zero();
        for e in &self.entries {
            for p in &e.postings {
                let a = p.amount(cx);
                sum = cx.add(sum, a);
            }
        }
        sum
    }

    pub fn balance(&self, cx: &mut Cx, account: &str) -> D {
        let mut sum = cx.zero();
        for e in &self.entries {
            for p in &e.postings {
                if p.account == account {
                    let a = p.amount(cx);
                    sum = cx.add(sum, a);
                }
            }
        }
        sum
    }

    pub fn qty(&self, cx: &mut Cx, account: &str) -> D {
        let mut sum = cx.zero();
        for e in &self.entries {
            for p in &e.postings {
                if p.account == account {
                    if let Some(q) = p.qty_d(cx) {
                        sum = cx.add(sum, q);
                    }
                }
            }
        }
        sum
    }

    /// All account balances, sorted. BTreeMap so reports are deterministic.
    pub fn balances(&self, cx: &mut Cx) -> BTreeMap<String, D> {
        let mut out: BTreeMap<String, D> = BTreeMap::new();
        for e in &self.entries {
            for p in &e.postings {
                let a = p.amount(cx);
                let cur = match out.get(&p.account) {
                    Some(v) => *v,
                    None => cx.zero(),
                };
                out.insert(p.account.clone(), cx.add(cur, a));
            }
        }
        out
    }

    /// Signed quantity per position account, sorted; zero-qty accounts dropped.
    pub fn quantities(&self, cx: &mut Cx) -> BTreeMap<String, D> {
        let mut out: BTreeMap<String, D> = BTreeMap::new();
        for e in &self.entries {
            for p in &e.postings {
                if let Some(q) = p.qty_d(cx) {
                    let cur = match out.get(&p.account) {
                        Some(v) => *v,
                        None => cx.zero(),
                    };
                    out.insert(p.account.clone(), cx.add(cur, q));
                }
            }
        }
        out.retain(|_, v| !is_zero(cx, *v));
        out
    }

    /// Append-only JSONL: one entry per line, postings nested. The entry is the
    /// atomic unit — splitting postings across lines lets a torn write produce
    /// an unbalanced book.
    pub fn to_jsonl(&self) -> String {
        let mut s = String::new();
        for e in &self.entries {
            s.push_str(&serde_json::to_string(e).expect("entry serializes"));
            s.push('\n');
        }
        s
    }

    pub fn from_jsonl(cx: &mut Cx, text: &str) -> Result<Journal, LedgerError> {
        let mut j = Journal::new();
        for line in text.lines() {
            let t = line.trim();
            if t.is_empty() {
                continue;
            }
            let e: Entry = match serde_json::from_str(t) {
                Ok(e) => e,
                Err(_) => continue,
            };
            j.post(cx, e)?;
        }
        Ok(j)
    }
}

pub fn is_zero(cx: &mut Cx, v: D) -> bool {
    let z = cx.zero();
    cx.cmp(v, z) == std::cmp::Ordering::Equal
}

/// Negate without needing a dedicated context op.
pub fn neg(cx: &mut Cx, v: D) -> D {
    let z = cx.zero();
    cx.sub(z, v)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn e(id: &str, postings: Vec<Posting>) -> Entry {
        Entry {
            id: id.into(),
            ts: 0,
            source: Source::Manual,
            memo: String::new(),
            postings,
        }
    }

    #[test]
    fn balanced_entry_posts() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let d = cx.parse_exact("439.78");
        let c = neg(&mut cx, d);
        let entry = e(
            "dep-1",
            vec![
                Posting::new(accounts::CASH_KALSHI, d),
                Posting::new(accounts::EQUITY_CAPITAL, c),
            ],
        );
        assert!(j.post(&mut cx, entry).is_ok());
        let tb = j.trial_balance(&mut cx);
        assert!(is_zero(&mut cx, tb));
        assert_eq!(j.balance(&mut cx, accounts::CASH_KALSHI).to_string(), "439.78");
    }

    #[test]
    fn unbalanced_entry_is_rejected() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let a = cx.parse_exact("10.00");
        let b = cx.parse_exact("-9.99");
        let entry = e(
            "bad",
            vec![
                Posting::new(accounts::CASH_KALSHI, a),
                Posting::new(accounts::EQUITY_CAPITAL, b),
            ],
        );
        match j.post(&mut cx, entry) {
            Err(LedgerError::Unbalanced { residual, .. }) => {
                assert_eq!(residual, "0.01");
            }
            other => panic!("expected Unbalanced, got {other:?}"),
        }
        assert!(j.is_empty(), "rejected entry must not land in the journal");
    }

    /// The regression that motivates this crate: the Python fold's identity is
    /// algebraically forced, so it cannot detect a missing fee. Here, omitting
    /// the fee leg leaves a residual and the entry is refused.
    #[test]
    fn missing_fee_leg_cannot_balance() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let cost = cx.parse_exact("5.50");
        let fee = cx.parse_exact("0.35");
        let gross = cx.add(cost, fee);
        let cash = neg(&mut cx, gross);
        let without_fee = e(
            "fill-nofee",
            vec![
                Posting::new("pos:kalshi:KXTEST", cost),
                Posting::new(accounts::CASH_KALSHI, cash),
            ],
        );
        assert!(matches!(
            j.post(&mut cx, without_fee),
            Err(LedgerError::Unbalanced { .. })
        ));

        let with_fee = e(
            "fill-fee",
            vec![
                Posting::new("pos:kalshi:KXTEST", cost),
                Posting::new(accounts::FEES_KALSHI_TAKER, fee),
                Posting::new(accounts::CASH_KALSHI, cash),
            ],
        );
        assert!(j.post(&mut cx, with_fee).is_ok());
    }

    #[test]
    fn duplicate_id_is_rejected_and_idempotent_post_is_a_noop() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let d = cx.parse_exact("1.00");
        let c = neg(&mut cx, d);
        let mk = || {
            e(
                "same-id",
                vec![
                    Posting::new(accounts::CASH_KALSHI, d),
                    Posting::new(accounts::EQUITY_CAPITAL, c),
                ],
            )
        };
        assert!(j.post(&mut cx, mk()).is_ok());
        assert!(matches!(j.post(&mut cx, mk()), Err(LedgerError::Duplicate { .. })));
        assert_eq!(j.post_idempotent(&mut cx, mk()), Ok(false));
        assert_eq!(j.len(), 1);
    }

    #[test]
    fn qty_tracks_signed_position_and_survives_jsonl_roundtrip() {
        let mut cx = Cx::default();
        let mut j = Journal::new();
        let cost = cx.parse_exact("4.19");
        let q = cx.parse_exact("-5");
        let entry = e(
            "short",
            vec![
                Posting::with_qty("pos:kalshi:KXPRESNOMD-28-GN", cost, q),
                Posting::new(accounts::CASH_KALSHI, neg(&mut cx, cost)),
            ],
        );
        j.post(&mut cx, entry).expect("posts");
        assert_eq!(
            j.qty(&mut cx, "pos:kalshi:KXPRESNOMD-28-GN").to_string(),
            "-5"
        );

        let text = j.to_jsonl();
        let j2 = Journal::from_jsonl(&mut cx, &text).expect("reparses");
        assert_eq!(j2.len(), j.len());
        let tb = j2.trial_balance(&mut cx);
        assert!(is_zero(&mut cx, tb));
        assert_eq!(
            j2.qty(&mut cx, "pos:kalshi:KXPRESNOMD-28-GN").to_string(),
            "-5"
        );
    }
}
