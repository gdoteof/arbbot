//! Typed response structs for the money-path endpoints.
//!
//! Contract (task spec):
//! - Unknown fields are TOLERATED (serde ignores them by default).
//! - Missing REQUIRED fields become [`VenueError::MissingField`] — never a
//!   silent zero/default.
//! - Money fields stay STRINGS — no float coercion. Integer COUNTS (contract
//!   quantities) are numbers on the wire and are parsed as integers.
//!
//! Every fixture shape here is drawn from the live-captured contracts in
//! `tests/test_venue_contracts.py` / `test_venue_contract_gaps.py`.

use std::collections::BTreeMap;

use serde::Deserialize;

use crate::error::{from_serde, VenueError};

fn parse<'a, T: Deserialize<'a>>(endpoint: &'static str, body: &'a str) -> Result<T, VenueError> {
    serde_json::from_str::<T>(body).map_err(|e| from_serde(endpoint, &e))
}

// ------------------------------------------------------------------ Kalshi ---

/// One row from GET /portfolio/orders (history) or the `{"order": ...}` in a
/// single-order GET. `fill_count_fp` is a PLAIN contract count string
/// ("2.00" == 2), not a scaled fixed-point.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrder {
    pub order_id: String,
    /// ABSENT on a create response — `POST /portfolio/events/orders` answers
    /// with `{client_order_id, fill_count, order_id, remaining_count, ts_ms}`
    /// and no status at all (observed live 2026-07-27). Requiring it here is
    /// what made the first two live smokes fail after the order was already
    /// resting. A create tells you an order EXISTS; only a GET tells you what
    /// it is doing.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub ticker: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default)]
    pub fill_count_fp: Option<String>,
    /// The create response's spelling of the same thing.
    #[serde(default)]
    pub fill_count: Option<String>,
    #[serde(default)]
    pub remaining_count: Option<String>,
    /// Our own idempotency tag. It is the ONLY way to find an order we created
    /// but whose create response we failed to read.
    #[serde(default)]
    pub client_order_id: Option<String>,
}

impl KalshiOrder {
    /// A create response has no status, so this is false for one — deliberately.
    /// "It exists" and "it is resting" are different claims.
    pub fn is_resting(&self) -> bool {
        self.status.as_deref() == Some("resting")
    }

    /// Cumulative filled contracts, replicating Python
    /// `int(float(o.get("fill_count_fp") or 0))` exactly (0 when absent).
    pub fn filled_qty(&self) -> i64 {
        self.fill_count_fp
            .as_deref()
            .or(self.fill_count.as_deref())
            .and_then(|s| s.parse::<f64>().ok())
            .map(|f| f as i64)
            .unwrap_or(0)
    }
}

/// `{"order": { ... }}` envelope (single-order GET, create response).
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrderEnvelope {
    pub order: KalshiOrder,
}

/// GET /portfolio/orders — HISTORY, paginated. `cursor` absent on the last page.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiOrdersPage {
    #[serde(default)]
    pub orders: Vec<KalshiOrder>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// GET /portfolio/balance. `balance_dollars` is money → string.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiBalance {
    pub balance_dollars: String,
}

/// GET /portfolio/positions.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiPositions {
    #[serde(default)]
    pub market_positions: Vec<KalshiMarketPosition>,
}

/// `position_fp` is a SIGNED contract count string; the *_dollars are money.
#[derive(Debug, Clone, Deserialize)]
pub struct KalshiMarketPosition {
    pub ticker: String,
    #[serde(default)]
    pub position_fp: Option<String>,
    #[serde(default)]
    pub market_exposure_dollars: Option<String>,
    #[serde(default)]
    pub realized_pnl_dollars: Option<String>,
    #[serde(default)]
    pub fees_paid_dollars: Option<String>,
}

pub fn kalshi_order_envelope(body: &str) -> Result<KalshiOrderEnvelope, VenueError> {
    parse("kalshi:order", body)
}

/// The CREATE response, which does NOT have one fixed shape.
///
/// `POST /portfolio/events/orders` (the current create path) does not answer
/// with the `{"order": ...}` envelope a single-order GET uses. Python tolerates
/// this by reaching for `resp["order"]["order_id"]` and falling back to
/// `resp["order_id"]`; a strict envelope parse instead fails with "missing
/// required field `order`" AFTER the order is already resting on the venue —
/// which is exactly how the first live smoke left an order behind
/// (2026-07-27).
///
/// So accept every shape the venue has been seen to return, and if none match,
/// put the RAW body in the error: a create response we cannot read is a fact we
/// need in the log, not a guess to make twice.
pub fn kalshi_created_order(body: &str) -> Result<KalshiOrder, VenueError> {
    if let Ok(env) = serde_json::from_str::<KalshiOrderEnvelope>(body) {
        return Ok(env.order);
    }
    if let Ok(o) = serde_json::from_str::<KalshiOrder>(body) {
        return Ok(o);
    }
    if let Ok(page) = serde_json::from_str::<KalshiOrdersPage>(body) {
        if let Some(o) = page.orders.into_iter().next() {
            return Ok(o);
        }
    }
    let mut detail = String::from("unrecognized create response: ");
    detail.push_str(&body.chars().take(600).collect::<String>());
    Err(VenueError::Parse { endpoint: "kalshi:create", detail })
}
pub fn kalshi_orders_page(body: &str) -> Result<KalshiOrdersPage, VenueError> {
    parse("kalshi:orders", body)
}
pub fn kalshi_balance(body: &str) -> Result<KalshiBalance, VenueError> {
    parse("kalshi:balance", body)
}
pub fn kalshi_positions(body: &str) -> Result<KalshiPositions, VenueError> {
    parse("kalshi:positions", body)
}

// ------------------------------------------------------------------- PM-US ---

/// A PM-US money value: `{"value": "0.2600", "currency": "USD"}`.
#[derive(Debug, Clone, Deserialize)]
pub struct MoneyVal {
    pub value: String,
    #[serde(default)]
    pub currency: Option<String>,
}

/// A PM-US order. Quantities are wire NUMBERS (int); money is nested string
/// `MoneyVal`. On CREATE the response omits execution data entirely
/// (`{"id": ..., "executions": []}`) — `cum_quantity`/`avg_px` are then `None`.
#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmOrder {
    pub id: String,
    #[serde(default)]
    pub market_slug: Option<String>,
    #[serde(default)]
    pub side: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub quantity: Option<i64>,
    #[serde(default)]
    pub cum_quantity: Option<i64>,
    #[serde(default)]
    pub leaves_quantity: Option<i64>,
    #[serde(default)]
    pub avg_px: Option<MoneyVal>,
    #[serde(default)]
    pub commission_notional_total_collected: Option<MoneyVal>,
    #[serde(default)]
    pub executions: Option<Vec<serde_json::Value>>,
}

/// Typed encoding of the `pmus-create-omits-fill-data` / `avgPx=0-on-create`
/// quirk: a create response carries NO fill data; the authoritative re-fetched
/// order carries `cumQuantity` + `avgPx`. Callers must branch on this, never
/// read `avgPx` off a create response (it would be absent/zero).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PmFillState {
    /// Create response (or any order before the async fill lands).
    NoFillData,
    /// Authoritative order: cumulative filled qty + average fill price (string).
    Filled { cum_quantity: i64, avg_px: String },
}

impl PmOrder {
    pub fn fill_state(&self) -> PmFillState {
        match (self.cum_quantity, self.avg_px.as_ref()) {
            (Some(q), Some(px)) if q >= 1 => PmFillState::Filled {
                cum_quantity: q,
                avg_px: px.value.clone(),
            },
            _ => PmFillState::NoFillData,
        }
    }

    /// Filled contracts from `cumQuantity` (0 if unknown) — the PM analogue of
    /// Python `int(float(o.get("cumQuantity") or 0))`.
    pub fn filled_qty(&self) -> i64 {
        self.cum_quantity.unwrap_or(0)
    }
}

/// `{"order": {...}}` or a bare order — the get_order path unwraps `order` if
/// present (`j.get("order", j)`).
pub fn pmus_order(body: &str) -> Result<PmOrder, VenueError> {
    #[derive(Deserialize)]
    struct Envelope {
        order: PmOrder,
    }
    // Try the envelope first; fall back to a bare order (create response shape).
    if let Ok(env) = serde_json::from_str::<Envelope>(body) {
        return Ok(env.order);
    }
    parse("pmus:order", body)
}

/// GET /v1/portfolio/positions — a dict KEYED BY SLUG with string netPosition.
#[derive(Debug, Clone, Deserialize)]
pub struct PmPositions {
    #[serde(default)]
    pub positions: BTreeMap<String, PmPosition>,
    #[serde(default)]
    pub next_cursor: Option<String>,
    #[serde(default)]
    pub eof: Option<bool>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmPosition {
    pub net_position: String,
    #[serde(default)]
    pub qty_sold: Option<String>,
    #[serde(default)]
    pub avg_px: Option<MoneyVal>,
}

pub fn pmus_positions(body: &str) -> Result<PmPositions, VenueError> {
    parse("pmus:positions", body)
}

/// GET /v1/account/balances — `{"balances": [{"buyingPower": "..."}]}`.
#[derive(Debug, Clone, Deserialize)]
pub struct PmBalances {
    pub balances: Vec<PmBalance>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct PmBalance {
    pub buying_power: String,
}

pub fn pmus_balances(body: &str) -> Result<PmBalances, VenueError> {
    parse("pmus:balances", body)
}
