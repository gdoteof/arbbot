//! Read-only capital snapshots. Deposits never enter these calculations.
use crate::VenueError;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Holding {
    pub market: String,
    pub quantity: String,
    pub value_usd: Option<String>,
    pub mark_updated_at: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountCapital {
    pub available_cash_usd: String,
    pub reserved_cash_usd: String,
    pub positions_value_usd: String,
    pub equity_usd: String,
    pub valuation: String,
    pub holdings: Vec<Holding>,
}
fn bad(message: &str) -> VenueError {
    VenueError::Parse {
        endpoint: "capital",
        detail: message.into(),
    }
}
// Fixed six-place money: no floats or silent defaults on the money path.
fn money(v: &Value) -> Result<i128, VenueError> {
    let text = match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Object(o) => {
            return money(o.get("value").ok_or_else(|| bad("missing money value"))?)
        }
        _ => return Err(bad("missing money")),
    };
    let (whole, frac) = text.split_once('.').unwrap_or((&text, ""));
    if whole.starts_with('-')
        || !whole.bytes().all(|b| b.is_ascii_digit())
        || whole.is_empty()
        || !frac.bytes().all(|b| b.is_ascii_digit())
        || (frac.len() > 6 && frac[6..].bytes().any(|b| b != b'0'))
    {
        return Err(bad("invalid nonnegative USD amount"));
    }
    let w: i128 = whole.parse().map_err(|_| bad("USD overflow"))?;
    let f = format!("{:.<6}", &frac[..frac.len().min(6)]).replace('.', "0");
    w.checked_mul(1_000_000)
        .and_then(|w| w.checked_add(f.parse().ok()?))
        .ok_or_else(|| bad("USD overflow"))
}
fn emit(n: i128) -> String {
    format!("{}.{:06}", n / 1_000_000, n % 1_000_000)
}
fn field(v: &Value, key: &str) -> Result<i128, VenueError> {
    money(&v[key])
}
fn account(
    cash: i128,
    reserved: i128,
    positions: i128,
    valuation: &str,
    holdings: Vec<Holding>,
) -> Result<AccountCapital, VenueError> {
    let equity = cash
        .checked_add(reserved)
        .and_then(|n| n.checked_add(positions))
        .ok_or_else(|| bad("equity overflow"))?;
    Ok(AccountCapital {
        available_cash_usd: emit(cash),
        reserved_cash_usd: emit(reserved),
        positions_value_usd: emit(positions),
        equity_usd: emit(equity),
        valuation: valuation.into(),
        holdings,
    })
}
pub(super) fn kalshi(body: &str) -> Result<AccountCapital, VenueError> {
    let v: Value = serde_json::from_str(body).map_err(|_| bad("invalid Kalshi balance JSON"))?;
    let cash = field(&v, "balance_dollars")?;
    let positions = if v.get("portfolio_value_dollars").is_some() {
        field(&v, "portfolio_value_dollars")?
    } else {
        field(&v, "portfolio_value")?
            .checked_div(100)
            .ok_or_else(|| bad("invalid cents"))?
    };
    account(cash, 0, positions, "venue portfolio value", vec![])
}
pub(super) fn pmus(balances: &str, positions: &str) -> Result<AccountCapital, VenueError> {
    let b: Value = serde_json::from_str(balances).map_err(|_| bad("invalid PM-US balance JSON"))?;
    let p: Value =
        serde_json::from_str(positions).map_err(|_| bad("invalid PM-US positions JSON"))?;
    let rows = b["balances"]
        .as_array()
        .ok_or_else(|| bad("missing PM-US balances"))?;
    let usd: Vec<_> = rows.iter().filter(|b| b["currency"] == "USD").collect();
    if usd.len() != 1 {
        return Err(bad("expected exactly one USD account"));
    }
    let b = usd[0];
    let cash = field(b, "buyingPower")?;
    // Margin is already in cashValue of the collateralized position. Never add it twice.
    let unencumbered = field(b, "currentBalance")?
        .checked_sub(field(b, "marginRequirement")?)
        .ok_or_else(|| bad("invalid margin"))?;
    let reserved = unencumbered
        .checked_sub(cash)
        .filter(|n| *n >= 0)
        .ok_or_else(|| bad("inconsistent PM-US cash identity"))?;
    if p["nextCursor"].as_str().is_some_and(|s| !s.is_empty())
        || p["next_cursor"].as_str().is_some_and(|s| !s.is_empty())
        || p["eof"] == false
    {
        return Err(bad("truncated PM-US positions"));
    }
    let positions = p["positions"]
        .as_object()
        .ok_or_else(|| bad("missing PM-US positions"))?;
    if positions.is_empty() && field(b, "marginRequirement")? > 0 {
        return Err(bad("empty positions with outstanding margin"));
    }
    let mut total = 0i128;
    let mut holdings = vec![];
    for (market, p) in positions {
        let qty = p["netPositionDecimal"]
            .as_str()
            .or_else(|| p["netPosition"].as_str())
            .ok_or_else(|| bad("missing position quantity"))?;
        let q: f64 = qty.parse().map_err(|_| bad("invalid position quantity"))?;
        if !q.is_finite() {
            return Err(bad("invalid position quantity"));
        }
        if q == 0. {
            continue;
        }
        let value = field(p, "cashValue")?;
        total = total
            .checked_add(value)
            .ok_or_else(|| bad("position total overflow"))?;
        holdings.push(Holding {
            market: market.clone(),
            quantity: qty.into(),
            value_usd: Some(emit(value)),
            mark_updated_at: p["updateTime"].as_str().map(str::to_owned),
        });
    }
    account(
        cash,
        reserved,
        total,
        "venue cashValue; mark timestamps may lag",
        holdings,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn kalshi_cents_are_not_dollars_and_unknown_is_not_zero() {
        let a = kalshi(r#"{"balance_dollars":"157.8480","portfolio_value":18949}"#).unwrap();
        assert_eq!(a.equity_usd, "347.338000");
        assert!(kalshi(r#"{"balance_dollars":"157.8480"}"#).is_err());
    }
    #[test]
    fn pm_margin_is_not_counted_twice_and_reserved_cash_is_not_lost() {
        let b = r#"{"balances":[{"currency":"USD","buyingPower":100,"currentBalance":151,"marginRequirement":50}]}"#;
        let p = r#"{"positions":{"a":{"netPosition":"-50","netPositionDecimal":"-50.25","cashValue":{"value":"45.1000"}}}}"#;
        let a = pmus(b, p).unwrap();
        assert_eq!(a.equity_usd, "146.100000");
        assert_eq!(a.reserved_cash_usd, "1.000000");
        assert_eq!(a.holdings[0].quantity, "-50.25");
        assert!(pmus(b, r#"{"positions":{}}"#).is_err());
        assert!(pmus(
            b,
            &p.replace("\"positions\"", "\"eof\":false,\"positions\"")
        )
        .is_err());
    }
    #[test]
    fn missing_and_malformed_money_fail_closed() {
        for v in [
            serde_json::json!(null),
            serde_json::json!("NaN"),
            serde_json::json!("-1"),
            serde_json::json!("0.0000001"),
        ] {
            assert!(money(&v).is_err());
        }
        assert_eq!(money(&serde_json::json!("0.000001")).unwrap(), 1);
    }
}
