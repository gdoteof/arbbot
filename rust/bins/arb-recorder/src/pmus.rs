//! Polymarket US (QCX DCM) feed — authed market-data WS (MARKET_DATA is a
//! self-contained full-book snapshot; no delta/seq reconstruction) with
//! chunked 150-slug subscriptions, or credential-free REST polling.
//! Transliteration of record/polymarket_us.py + the recorder tasks.

use crate::core::{dec_string, Core, SeqCounter, STALL_RECONNECT_S};
use crate::health::Liveness;
use crate::sign::PmusSigner;
use anyhow::Result;
use arb_core::dec::Dec;
use arb_core::model::{now_local_ns, Level, TakerSide, TapeEvent, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

pub const GATEWAY_BASE: &str = "https://gateway.polymarket.us/v1";
pub const WS_MARKETS_URL: &str = "wss://api.polymarket.us/v1/ws/markets";
pub const WS_MARKETS_PATH: &str = "/v1/ws/markets";

fn levels(raw: Option<&Value>, descending: bool) -> Vec<Level> {
    let mut lv: Vec<Level> = raw
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|l| {
                    let px = dec_string(l.get("px")?.get("value")?);
                    let sz = dec_string(l.get("qty")?);
                    Dec::parse(&sz).ok().filter(Dec::is_positive)?;
                    Some(Level { price: px, size: sz })
                })
                .collect()
        })
        .unwrap_or_default();
    lv.sort_by(|a, b| {
        let (pa, pb) = (
            Dec::parse(&a.price).unwrap_or(Dec::ZERO),
            Dec::parse(&b.price).unwrap_or(Dec::ZERO),
        );
        if descending { pb.cmp_num(&pa) } else { pa.cmp_num(&pb) }
    });
    lv
}

pub fn normalize_book(slug: &str, md: &Value, seq: u64) -> TapeEvent {
    TapeEvent::Snapshot {
        venue: Venue::PolymarketUs,
        market_id: slug.to_owned(),
        bids: levels(md.get("bids"), true),
        asks: levels(md.get("offers"), false), // ask side = "offers"
        seq,
        ts_local_ns: now_local_ns(),
        ts_venue: Some(dec_string(md.get("transactTime").unwrap_or(&Value::String(String::new())))),
    }
}

fn parse_ws_message(msg: &Value, seq: &mut SeqCounter) -> Vec<TapeEvent> {
    if let Some(md) = msg.get("marketData") {
        let slug = md.get("marketSlug").and_then(Value::as_str).unwrap_or("").to_owned();
        let s = seq.next(&slug);
        return vec![normalize_book(&slug, md, s)];
    }
    if let Some(t) = msg.get("trade") {
        let slug = t.get("marketSlug").and_then(Value::as_str).unwrap_or("").to_owned();
        let taker = t
            .get("taker")
            .and_then(|x| x.get("intent"))
            .and_then(Value::as_str)
            .unwrap_or("");
        let s = seq.next(&format!("{slug}|tape"));
        let price = t.get("price").and_then(|p| p.get("value")).map(dec_string).unwrap_or_default();
        let size = t.get("quantity").map(dec_string).unwrap_or_default();
        return vec![TapeEvent::Trade {
            venue: Venue::PolymarketUs,
            market_id: slug,
            price,
            size,
            taker_side: Some(if taker.contains("BUY") { TakerSide::Buy } else { TakerSide::Sell }),
            seq: s,
            ts_local_ns: now_local_ns(),
            ts_venue: Some(dec_string(t.get("tradeTime").unwrap_or(&Value::String(String::new())))),
        }];
    }
    vec![] // heartbeat/error/lite ignored
}

// ---------- REST ----------

pub struct PmusCatalog {
    client: reqwest::Client,
    base: String,
}

pub struct PmusMarket {
    pub slug: String,
    pub closed: bool,
}

impl PmusCatalog {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(20))
                .build()
                .expect("reqwest client"),
            base: GATEWAY_BASE.to_owned(),
        }
    }

    pub async fn markets_by_tags(&self, tags: &[String]) -> Result<Vec<PmusMarket>> {
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for tag in tags {
            let r = self
                .client
                .get(format!("{}/events", self.base))
                .query(&[("tag_slug", tag.as_str()), ("limit", "200")])
                .send()
                .await?
                .error_for_status()?;
            let v: Value = r.json().await?;
            for e in v.get("events").and_then(Value::as_array).unwrap_or(&vec![]) {
                for m in e.get("markets").and_then(Value::as_array).unwrap_or(&vec![]) {
                    if let Some(slug) = m.get("slug").and_then(Value::as_str) {
                        if seen.insert(slug.to_owned()) {
                            out.push(PmusMarket {
                                slug: slug.to_owned(),
                                closed: m.get("closed").and_then(Value::as_bool).unwrap_or(false),
                            });
                        }
                    }
                }
            }
        }
        Ok(out)
    }

    pub async fn book(&self, slug: &str, seq: u64) -> Result<TapeEvent> {
        let r = self
            .client
            .get(format!("{}/markets/{slug}/book", self.base))
            .send()
            .await?
            .error_for_status()?;
        let v: Value = r.json().await?;
        let md = v.get("marketData").cloned().unwrap_or(Value::Object(Default::default()));
        Ok(normalize_book(slug, &md, seq))
    }
}

// ---------- tasks ----------

pub async fn ws_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    slugs: Vec<String>,
    signer: PmusSigner,
) {
    loop {
        if let Err(e) = ws_session(&core, &liveness, &slugs, &signer).await {
            eprintln!("[pmus-ws] session ended: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn ws_session(
    core: &Core,
    liveness: &Liveness,
    slugs: &[String],
    signer: &PmusSigner,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    let mut req = WS_MARKETS_URL.into_client_request()?;
    for (k, v) in signer.headers("GET", WS_MARKETS_PATH) {
        req.headers_mut().insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
            v.parse()?,
        );
    }
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await?;
    // chunked subscriptions: unknown venue-side caps on marketSlugs per
    // request — several smaller subscriptions avoid silent partial coverage
    for (i, chunk) in slugs.chunks(150).enumerate() {
        let base = i * 150;
        let sub = |req_id: String, sub_type: &str| {
            json!({"subscribe": {"requestId": req_id, "subscriptionType": sub_type,
                                  "marketSlugs": chunk}})
            .to_string()
        };
        ws.send(Message::Text(sub(format!("book{base}"), "SUBSCRIPTION_TYPE_MARKET_DATA"))).await?;
        ws.send(Message::Text(sub(format!("tape{base}"), "SUBSCRIPTION_TYPE_TRADE"))).await?;
    }
    liveness.beat("polymarket_us-ws");
    let mut seq = SeqCounter::default();
    let mut last_frame = tokio::time::Instant::now();
    loop {
        let frame = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Err(_) => {
                // Deliberately NOT a liveness beat — see STALL_RECONNECT_S.
                if last_frame.elapsed() > Duration::from_secs(STALL_RECONNECT_S) {
                    anyhow::bail!(
                        "no frame in {STALL_RECONNECT_S}s — socket is half-open, reconnecting"
                    );
                }
                continue;
            }
            Ok(None) => anyhow::bail!("ws closed"),
            Ok(Some(f)) => f?,
        };
        last_frame = tokio::time::Instant::now();
        liveness.beat("polymarket_us-ws");
        let text = match frame {
            tokio_tungstenite::tungstenite::Message::Text(t) => t,
            tokio_tungstenite::tungstenite::Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => continue,
        };
        if let Ok(msg) = serde_json::from_str::<Value>(&text) {
            for ev in parse_ws_message(&msg, &mut seq) {
                core.on_event(&ev);
            }
        }
    }
}

pub async fn poll_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    slugs: Vec<String>,
    catalog: Arc<PmusCatalog>,
    interval_s: f64,
) {
    let mut seq = SeqCounter::default();
    loop {
        for slug in &slugs {
            match catalog.book(slug, seq.next(slug)).await {
                Ok(snap) => {
                    core.on_event(&snap);
                    liveness.beat("polymarket_us-rest");
                }
                Err(e) => {
                    eprintln!("[pmus-poll] {slug}: {e:#}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(interval_s / slugs.len().max(1) as f64))
                .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn market_data_becomes_snapshot_with_offers_as_asks() {
        let md = json!({"marketSlug": "s-1",
                        "bids": [{"px": {"value": "0.6300"}, "qty": "33072.3800"},
                                 {"px": {"value": "0.6200"}, "qty": "0"}],
                        "offers": [{"px": {"value": "0.6600"}, "qty": "5.0000"}],
                        "transactTime": "2026-07-23T01:02:03.123456789Z"});
        let mut seq = SeqCounter::default();
        let evs = parse_ws_message(&json!({"marketData": md}), &mut seq);
        match &evs[0] {
            TapeEvent::Snapshot { bids, asks, ts_venue, .. } => {
                assert_eq!(bids.len(), 1); // zero-size filtered
                assert_eq!(bids[0].price, "0.6300");
                assert_eq!(asks[0].price, "0.6600");
                assert_eq!(ts_venue.as_deref(), Some("2026-07-23T01:02:03.123456789Z"));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn trade_takes_tape_seq_stream() {
        let mut seq = SeqCounter::default();
        let t = json!({"trade": {"marketSlug": "s-1", "price": {"value": "0.55"},
                                  "quantity": "12.0000",
                                  "taker": {"intent": "INTENT_BUY_LONG"},
                                  "tradeTime": "2026-07-23T02:03:04Z"}});
        match &parse_ws_message(&t, &mut seq)[0] {
            TapeEvent::Trade { taker_side, seq, .. } => {
                assert_eq!(*taker_side, Some(TakerSide::Buy));
                assert_eq!(*seq, 1); // independent |tape stream
            }
            _ => panic!(),
        }
    }
}
