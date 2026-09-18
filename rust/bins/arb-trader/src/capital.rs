//! One automatic venue-capital poll shared by risk and the dashboard.
use arb_core::{book::BookBuilder, model::Venue, scan::Cx};
use arb_venue::gateway::capital::AccountCapital;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Instant,
};
type Sink = Arc<dyn crate::sink::OrderSink>;
type Quotes = BTreeMap<String, (String, String)>;
static MARKS: Mutex<Option<(Instant, Quotes, Quotes)>> = Mutex::new(None);

pub fn publish_marks(books: &BookBuilder) {
    let now = arb_core::clock::now_secs() as i64 * 1_000_000_000;
    let mut pm = BTreeMap::new();
    let mut kalshi = BTreeMap::new();
    for (venue, markets, quotes) in [
        (Venue::PolymarketUs, books.pm_us_asks(), &mut pm),
        (Venue::Kalshi, books.kalshi_bids(), &mut kalshi),
    ] {
        for (market, _) in markets {
            let Some(b) = books.get(venue, &market) else {
                continue;
            };
            if now - b.ts_local_ns > 180_000_000_000
                || b.ts_local_ns > now + 2_000_000_000
                || b.crossing().is_some()
            {
                continue;
            }
            if let (Some(bid), Some(ask)) = (b.bids.first(), b.asks.first()) {
                quotes.insert(market, (bid.price.clone(), ask.price.clone()));
            }
        }
    }
    *MARKS.lock().expect("capital marks") = Some((Instant::now(), pm, kalshi));
}
fn mark_account(a: &mut AccountCapital, revalue: bool) {
    let quotes = MARKS
        .lock()
        .expect("capital marks")
        .as_ref()
        .filter(|(at, _, _)| at.elapsed().as_secs() <= 180)
        .map(|(_, pm, k)| if revalue { pm.clone() } else { k.clone() })
        .unwrap_or_default();
    apply_marks(a, &quotes, revalue);
}
fn apply_marks(a: &mut AccountCapital, quotes: &Quotes, revalue: bool) {
    let mut cx = Cx::default();
    let mut total = cx.zero();
    let mut fresh = 0;
    for h in &mut a.holdings {
        if let (Some((bid, ask)), Some(q)) = (quotes.get(&h.market), cx.parse(&h.quantity)) {
            let signed = h.quantity.parse::<f64>().unwrap_or(0.);
            let price = cx.parse(if signed < 0. { ask } else { bid });
            if let Some(p) = price.filter(|p| {
                let zero = cx.zero();
                cx.cmp(*p, zero) != std::cmp::Ordering::Less
                    && cx.cmp(*p, cx.one) != std::cmp::Ordering::Greater
            }) {
                let zero = cx.zero();
                let (qty, price) = if signed < 0. {
                    (cx.sub(zero, q), cx.one_minus(p))
                } else {
                    (q, p)
                };
                let value = cx.mul(qty, price);
                h.value_usd = Some(cx.emit_6dp(value));
                h.mark_updated_at = Some(format!("live book at {}", arb_core::clock::now_secs()));
                fresh += 1;
            }
        }
        if let Some(v) = h.value_usd.as_deref().and_then(|v| cx.parse(v)) {
            total = cx.add(total, v);
        }
    }
    if !revalue {
        a.valuation = format!(
            "venue portfolio total; {fresh}/{} positions independently marked from fresh books",
            a.holdings.len()
        );
        return;
    }
    a.positions_value_usd = cx.emit_6dp(total);
    let cash = cx.parse(&a.available_cash_usd).expect("validated cash");
    let reserved = cx.parse(&a.reserved_cash_usd).expect("validated reserve");
    let liquid = cx.add(cash, reserved);
    let equity = cx.add(liquid, total);
    a.equity_usd = cx.emit_6dp(equity);
    a.valuation=format!("{fresh}/{} positions marked from fresh books; remaining values are venue-reported and may lag",a.holdings.len());
}
pub async fn read(k: &Sink, p: &Sink) -> Result<Vec<(String, AccountCapital)>, String> {
    let k = k.clone();
    let p = p.clone();
    let (k, p) = tokio::join!(
        tokio::task::spawn_blocking(move || k.account_capital()),
        tokio::task::spawn_blocking(move || p.account_capital())
    );
    let mut k = k
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("kalshi: {e}"))?;
    let mut p = p
        .map_err(|e| e.to_string())?
        .map_err(|e| format!("pmus: {e}"))?;
    mark_account(&mut k, false);
    mark_account(&mut p, true);
    Ok(vec![("kalshi".into(), k), ("polymarket_us".into(), p)])
}
pub fn publish(
    path: &str,
    accounts: &[(String, AccountCapital)],
    risk: &crate::risk::RiskView,
) -> Result<(), String> {
    let body = serde_json::json!({"at":arb_core::clock::now_secs(),"max_age_s":crate::risk::BALANCE_MAX_AGE.as_secs(),
        "pid":std::process::id(),"started_at":*crate::PROCESS_STARTED_AT,"accounts":accounts.iter().cloned().collect::<BTreeMap<_,_>>(),
        "risk_policy":risk.capital_policy()});
    crate::marks::write_atomic(path, &format!("{body}\n"))
}

pub async fn follow_snapshot(risk: Arc<crate::risk::RiskView>) {
    risk.expect_live_capital();
    loop {
        let result = (|| -> Result<(), String> {
            let text =
                std::fs::read_to_string("data/exec/capital.json").map_err(|e| e.to_string())?;
            let v: serde_json::Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
            let at = v["at"].as_u64().ok_or("missing snapshot time")?;
            let age = arb_core::clock::now_secs()
                .checked_sub(at)
                .ok_or("future snapshot")?;
            let accounts: BTreeMap<String, AccountCapital> =
                serde_json::from_value(v["accounts"].clone()).map_err(|e| e.to_string())?;
            risk.set_cached_capital(&accounts.into_iter().collect::<Vec<_>>(), age)
        })();
        if let Err(e) = result {
            eprintln!("[capital] shadow waiting for venue snapshot: {e}");
        }
        tokio::time::sleep(std::time::Duration::from_secs(10)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn kalshi_marks_longs_and_shorts_but_preserves_venue_total() {
        let mut a = AccountCapital {
            available_cash_usd: "10".into(),
            reserved_cash_usd: "0".into(),
            positions_value_usd: "50".into(),
            equity_usd: "60".into(),
            valuation: "venue".into(),
            holdings: [("long", "2.5"), ("short", "-3"), ("unpriced", "1")]
                .into_iter()
                .map(|(market, quantity)| arb_venue::gateway::capital::Holding {
                    market: market.into(),
                    quantity: quantity.into(),
                    value_usd: None,
                    mark_updated_at: None,
                })
                .collect(),
        };
        apply_marks(
            &mut a,
            &BTreeMap::from([
                ("long".into(), ("0.20".into(), "0.30".into())),
                ("short".into(), ("0.40".into(), "0.60".into())),
            ]),
            false,
        );
        assert_eq!(a.holdings[0].value_usd.as_deref(), Some("0.500000"));
        assert_eq!(a.holdings[1].value_usd.as_deref(), Some("1.200000"));
        assert!(a.holdings[2].value_usd.is_none());
        assert_eq!(a.positions_value_usd, "50");
        assert_eq!(a.equity_usd, "60");
        assert!(a.valuation.contains("2/3"));
    }
    #[test]
    fn fresh_quotes_mark_fractional_shorts_without_adding_margin_again() {
        let mut a = AccountCapital {
            available_cash_usd: "100".into(),
            reserved_cash_usd: "1".into(),
            positions_value_usd: "2".into(),
            equity_usd: "103".into(),
            valuation: "venue".into(),
            holdings: vec![arb_venue::gateway::capital::Holding {
                market: "m".into(),
                quantity: "-10.5".into(),
                value_usd: Some("2".into()),
                mark_updated_at: Some("old".into()),
            }],
        };
        apply_marks(
            &mut a,
            &BTreeMap::from([("m".into(), ("0.29".into(), "0.30".into()))]),
            true,
        );
        assert_eq!(a.positions_value_usd, "7.350000");
        assert_eq!(a.equity_usd, "108.350000");
        assert!(a.valuation.starts_with("1/1"));
        apply_marks(&mut a, &BTreeMap::new(), true);
        assert!(a.valuation.starts_with("0/1"));
        assert_eq!(a.equity_usd, "108.350000");
    }
}
