//! Request-shaping — builds the exact JSON bodies the Python gateways send.
//!
//! These return `serde_json::Value` so parity tests can compare keys+values
//! order-independently against the Python dicts. Money/price/count values are
//! passed in ALREADY formatted as strings (the caller owns decimal formatting,
//! per the stack's decimal-parity convention) — this module never touches a
//! float. See `kalshi_gateway.py::place_yes` and
//! `polymarket_us_gateway.py::_order_body`.

use serde_json::{json, Value};

use crate::gateway::{Side, Tif};

/// Kalshi integer count formatted as Python's `f"{count:.2f}"` (fixed 2 dp).
pub fn kalshi_count(count: i64) -> String {
    format!("{count}.00")
}

/// POST /trade-api/v2/portfolio/events/orders body.
/// `price` is the YES-axis probability pre-formatted to 4 dp (Python
/// `f"{price:.4f}"`). `side` is the raw Kalshi bid/ask.
pub fn kalshi_place_body(
    ticker: &str,
    side: Side,
    price_4dp: &str,
    count: i64,
    client_order_id: &str,
    post_only: bool,
) -> Value {
    json!({
        "ticker": ticker,
        "side": side.kalshi(),
        "count": kalshi_count(count),
        "price": price_4dp,
        "time_in_force": if post_only { "good_till_canceled" } else { "immediate_or_cancel" },
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": client_order_id,
        "post_only": post_only,
    })
}

/// PM-US order body (used bare for POST /v1/orders; wrapped as `{"request":
/// body}` for /v1/order/preview — see [`pmus_preview_body`]).
pub fn pmus_order_body(
    slug: &str,
    side: Side,
    price_4dp: &str,
    count: i64,
    post_only: bool,
) -> Value {
    json!({
        "marketSlug": slug,
        "type": "ORDER_TYPE_LIMIT",
        "price": { "value": price_4dp, "currency": "USD" },
        "quantity": count,
        "tif": if post_only { "TIME_IN_FORCE_GOOD_TILL_CANCEL" } else { "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL" },
        "intent": match side { Side::Bid => "ORDER_INTENT_BUY_LONG", Side::Ask => "ORDER_INTENT_BUY_SHORT" },
        "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
        "participateDontInitiate": post_only,
    })
}

/// preview wraps the order body: `{"request": body}` (create does NOT — a
/// mismatched wrapper gives a misleading "Market not found").
pub fn pmus_preview_body(body: Value) -> Value {
    json!({ "request": body })
}

/// PM-US cancel body — the marketSlug MUST ride in the body (P6 quirk).
pub fn pmus_cancel_body(market_slug: &str) -> Value {
    json!({ "marketSlug": market_slug })
}

impl Side {
    /// Raw Kalshi side token.
    fn kalshi(self) -> &'static str {
        match self {
            Side::Bid => "bid",
            Side::Ask => "ask",
        }
    }
}

impl Tif {
    /// Unused by the JSON builders (they inline from post_only, matching
    /// Python), kept for the executor seam so callers can name the intent.
    pub fn is_maker(self) -> bool {
        matches!(self, Tif::Gtc)
    }
}
