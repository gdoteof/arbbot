//! Chart of accounts.
//!
//! Every account here is something a VENUE can confirm, or an explicitly named
//! place to park what it cannot. Baskets and relationships are deliberately NOT
//! accounts — they are analysis groupings that change (and have broken: a
//! 0.43 ms `closes_ts` mismatch orphaned a close in the old ledger). They ride
//! as dimensions on the entry memo instead.
//!
//! Deliberately absent: `locked_at_open`, `remaining_locked`, `slippage`.
//! Those are derived reports over cost basis versus the $1 basket payoff.
//! Promoting them to accounts is what produced the old tautological identity.

/// Spendable cash the venue reports as the account balance.
/// Kalshi: `/portfolio/balance` -> `balance_dollars`.
pub const CASH_KALSHI: &str = "cash:kalshi";
/// PM-US: `/v1/account/balances` -> `currentBalance` (NOT `buyingPower`,
/// which is net of margin — see `pmus-margin-is-one-dollar-per-short`).
pub const CASH_PMUS: &str = "cash:pmus";

/// Cash encumbered as short collateral. Both venues hold exactly $1.00 per
/// short contract. Kalshi does not itemize it, so it is derived from position;
/// PM-US reports it directly as `marginRequirement`.
pub const MARGIN_KALSHI: &str = "margin:kalshi";
pub const MARGIN_PMUS: &str = "margin:pmus";

/// Deposits and withdrawals of real capital.
pub const EQUITY_CAPITAL: &str = "equity:capital";
/// The one-time migration plug: posted once, dated, never touched again.
pub const EQUITY_OPENING: &str = "equity:opening_balance";

pub const PNL_REALIZED: &str = "pnl:realized";

/// PM-US realized P&L that cannot be itemized. PM-US exposes no fill or
/// settlement history (`pmus-no-history-endpoints`), so P&L on a FULLY closed
/// position is unrecoverable — the position simply vanishes from
/// `/v1/portfolio/positions`. This account holds that residual so it is never
/// mistaken for verified, trade-level P&L. It should stop growing once we
/// record our own fills forward from cutover.
pub const PNL_PMUS_UNITEMIZED: &str = "pnl:realized:pmus_unitemized";

/// Split by venue AND role: the whole maker-vs-taker question turns on this,
/// and Kalshi's maker fee is 0.0175*P*(1-P), NOT zero (curves.py:69) — 23
/// maker legs went unbooked in the old ledger on the assumption it was.
pub const FEES_KALSHI_TAKER: &str = "expense:fees:kalshi:taker";
pub const FEES_KALSHI_MAKER: &str = "expense:fees:kalshi:maker";
pub const FEES_PMUS_TAKER: &str = "expense:fees:pmus:taker";
/// Confirmed zero (curves.py:91-97). Kept so a nonzero one is VISIBLE.
pub const FEES_PMUS_MAKER: &str = "expense:fees:pmus:maker";

/// Cash the venue and the books disagree about. A nonzero balance here is the
/// honest replacement for "slippage 19.51": we do not understand $X.
pub const SUSPENSE_KALSHI: &str = "suspense:venue_discrepancy:kalshi";
pub const SUSPENSE_PMUS: &str = "suspense:venue_discrepancy:pmus";
/// Venue positions with no ledger provenance (e.g. KXPRESPERSON-28-JVAN +5,
/// which the old ledger had never heard of).
pub const SUSPENSE_POSITION: &str = "suspense:unidentified_position";

/// Position account for a market, valued at COST BASIS (always >= 0) with a
/// signed qty memo — mirroring Kalshi, which reports `position_fp` signed but
/// `market_exposure_dollars` positive regardless of side.
pub fn position(venue: &str, market: &str) -> String {
    format!("pos:{venue}:{market}")
}

pub fn fees(venue: &str, taker: bool) -> &'static str {
    match (venue, taker) {
        ("kalshi", true) => FEES_KALSHI_TAKER,
        ("kalshi", false) => FEES_KALSHI_MAKER,
        ("polymarket_us", true) => FEES_PMUS_TAKER,
        ("polymarket_us", false) => FEES_PMUS_MAKER,
        _ => FEES_KALSHI_TAKER,
    }
}

pub fn cash(venue: &str) -> &'static str {
    match venue {
        "polymarket_us" => CASH_PMUS,
        _ => CASH_KALSHI,
    }
}

pub fn margin(venue: &str) -> &'static str {
    match venue {
        "polymarket_us" => MARGIN_PMUS,
        _ => MARGIN_KALSHI,
    }
}

pub fn suspense_cash(venue: &str) -> &'static str {
    match venue {
        "polymarket_us" => SUSPENSE_PMUS,
        _ => SUSPENSE_KALSHI,
    }
}
