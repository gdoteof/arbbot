//! Polymarket international CLOB feed — public WS (book/deltas/tape, no seq
//! on the wire: per-market seq synthesized), text PING keepalive, gap ->
//! REST re-snapshot, periodic 300s integrity re-snapshot.
//! Transliteration of record/polymarket.py + polymarket_ws_task.

use crate::core::{dec_string, Core, SeqCounter};
use crate::health::Liveness;
use anyhow::Result;
use arb_core::dec::Dec;
use arb_core::model::{now_local_ns, BookSide, Level, TakerSide, TapeEvent, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

pub const CLOB_BASE: &str = "https://clob.polymarket.com";
pub const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
pub const WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

fn levels(raw: Option<&Value>, descending: bool) -> Vec<Level> {
    let mut lv: Vec<Level> = raw
        .and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|l| {
                    let price = dec_string(l.get("price")?);
                    let size = dec_string(l.get("size")?);
                    Dec::parse(&size).ok().filter(Dec::is_positive)?;
                    Some(Level { price, size })
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

fn ts_venue_of(msg: &Value) -> Option<String> {
    Some(dec_string(msg.get("timestamp").unwrap_or(&Value::String(String::new()))))
}

fn book_event(msg: &Value, seq: u64) -> Option<TapeEvent> {
    Some(TapeEvent::Snapshot {
        venue: Venue::Polymarket,
        market_id: dec_string(msg.get("asset_id")?),
        bids: levels(msg.get("bids"), true),
        asks: levels(msg.get("asks"), false),
        seq,
        ts_local_ns: now_local_ns(),
        ts_venue: ts_venue_of(msg),
    })
}

fn parse_ws_frame(frame: &str, seq: &mut SeqCounter) -> Vec<TapeEvent> {
    if frame == "PONG" {
        return vec![];
    }
    let data: Value = match serde_json::from_str(frame) {
        Ok(v) => v,
        Err(_) => return vec![],
    };
    let msgs: Vec<&Value> = match &data {
        Value::Array(a) => a.iter().collect(),
        other => vec![other],
    };
    let mut out = Vec::new();
    for m in msgs {
        match m.get("event_type").and_then(Value::as_str) {
            Some("book") => {
                if let Some(asset) = m.get("asset_id") {
                    let s = seq.next(&dec_string(asset));
                    out.extend(book_event(m, s));
                }
            }
            Some("price_change") => {
                for ch in m.get("price_changes").and_then(Value::as_array).unwrap_or(&vec![]) {
                    let Some(asset) = ch.get("asset_id") else { continue };
                    let asset = dec_string(asset);
                    let s = seq.next(&asset);
                    let side_buy = ch
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .eq_ignore_ascii_case("BUY");
                    out.push(TapeEvent::Delta {
                        venue: Venue::Polymarket,
                        market_id: asset,
                        side: if side_buy { BookSide::Bid } else { BookSide::Ask },
                        price: dec_string(ch.get("price").unwrap_or(&Value::Null)),
                        size: dec_string(ch.get("size").unwrap_or(&Value::Null)),
                        seq: s,
                        ts_local_ns: now_local_ns(),
                        ts_venue: ts_venue_of(m),
                    });
                }
            }
            Some("last_trade_price") => {
                if let Some(asset) = m.get("asset_id") {
                    let asset = dec_string(asset);
                    let s = seq.next(&asset);
                    let buy = m
                        .get("side")
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .eq_ignore_ascii_case("BUY");
                    out.push(TapeEvent::Trade {
                        venue: Venue::Polymarket,
                        market_id: asset,
                        price: dec_string(m.get("price").unwrap_or(&Value::Null)),
                        size: dec_string(m.get("size").unwrap_or(&Value::Null)),
                        taker_side: Some(if buy { TakerSide::Buy } else { TakerSide::Sell }),
                        seq: s,
                        ts_local_ns: now_local_ns(),
                        ts_venue: ts_venue_of(m),
                    });
                }
            }
            _ => {} // tick_size_change / market_resolved: catalog refresher's job
        }
    }
    out
}

pub struct ClobRest {
    client: reqwest::Client,
    base: String,
    gamma: String,
}

impl ClobRest {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("reqwest client"),
            base: CLOB_BASE.to_owned(),
            gamma: GAMMA_BASE.to_owned(),
        }
    }

    pub async fn book(&self, token_id: &str, seq: u64) -> Result<TapeEvent> {
        let r = self
            .client
            .get(format!("{}/book", self.base))
            .query(&[("token_id", token_id)])
            .send()
            .await?
            .error_for_status()?;
        let v: Value = r.json().await?;
        Ok(TapeEvent::Snapshot {
            venue: Venue::Polymarket,
            market_id: token_id.to_owned(),
            bids: levels(v.get("bids"), true),
            asks: levels(v.get("asks"), false),
            seq,
            ts_local_ns: now_local_ns(),
            ts_venue: ts_venue_of(&v),
        })
    }

    /// token_id -> closed? via Gamma (repeated clob_token_ids params,
    /// comma-joined 422s), batches of 20. Used only for book eviction.
    pub async fn closed_tokens(&self, token_ids: &[String]) -> Result<Vec<String>> {
        let mut closed = Vec::new();
        for chunk in token_ids.chunks(20) {
            let params: Vec<(&str, &str)> =
                chunk.iter().map(|t| ("clob_token_ids", t.as_str())).collect();
            let r = self
                .client
                .get(format!("{}/markets", self.gamma))
                .query(&params)
                .send()
                .await?
                .error_for_status()?;
            let v: Value = r.json().await?;
            for m in v.as_array().unwrap_or(&vec![]) {
                if m.get("closed").and_then(Value::as_bool).unwrap_or(false) {
                    if let Ok(toks) = serde_json::from_str::<Vec<String>>(
                        m.get("clobTokenIds").and_then(Value::as_str).unwrap_or("[]"),
                    ) {
                        closed.extend(toks);
                    }
                }
            }
        }
        Ok(closed)
    }
}

pub async fn ws_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    token_ids: Vec<String>,
    clob: Arc<ClobRest>,
) {
    loop {
        if let Err(e) = ws_session(&core, &liveness, &token_ids, &clob).await {
            eprintln!("[pm-ws] session ended: {e:#}");
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

async fn ws_session(
    core: &Core,
    liveness: &Liveness,
    token_ids: &[String],
    clob: &ClobRest,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(WS_MARKET_URL).await?;
    ws.send(Message::Text(
        json!({"type": "market", "assets_ids": token_ids, "custom_feature_enabled": true})
            .to_string(),
    ))
    .await?;
    liveness.beat("polymarket-ws");
    let mut seq = SeqCounter::default();
    let mut last_ping = tokio::time::Instant::now();
    let mut last_resnap = tokio::time::Instant::now();
    loop {
        let frame = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Err(_) => None,
            Ok(None) => anyhow::bail!("ws closed"),
            Ok(Some(f)) => Some(f?),
        };
        if last_ping.elapsed() > Duration::from_secs(10) {
            ws.send(Message::Text("PING".into())).await?;
            last_ping = tokio::time::Instant::now();
        }
        if let Some(frame) = frame {
            liveness.beat("polymarket-ws");
            let text = match frame {
                Message::Text(t) => t,
                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                _ => continue,
            };
            for ev in parse_ws_frame(&text, &mut seq) {
                if let Some(need) = core.on_event(&ev) {
                    let s = seq.next(&need);
                    if let Ok(snap) = clob.book(&need, s).await {
                        core.on_event(&snap);
                    }
                }
            }
        }
        if last_resnap.elapsed() > Duration::from_secs(300) {
            for tid in token_ids {
                let s = seq.next(tid);
                if let Ok(snap) = clob.book(tid, s).await {
                    core.on_event(&snap);
                }
            }
            last_resnap = tokio::time::Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_parsing_matches_python_shapes() {
        let mut seq = SeqCounter::default();
        assert!(parse_ws_frame("PONG", &mut seq).is_empty());
        let book = json!({"event_type": "book", "asset_id": "123",
                          "bids": [{"price": "0.55", "size": "50"}],
                          "asks": [{"price": "0.60", "size": "0"}],
                          "timestamp": 1700000000123i64})
        .to_string();
        match &parse_ws_frame(&book, &mut seq)[0] {
            TapeEvent::Snapshot { bids, asks, ts_venue, .. } => {
                assert_eq!(bids.len(), 1);
                assert!(asks.is_empty()); // zero size filtered
                assert_eq!(ts_venue.as_deref(), Some("1700000000123"));
            }
            _ => panic!(),
        }
        let pc = json!([{"event_type": "price_change", "timestamp": "17",
                         "price_changes": [{"asset_id": "123", "side": "SELL",
                                             "price": "0.61", "size": "9"}]}])
        .to_string();
        match &parse_ws_frame(&pc, &mut seq)[0] {
            TapeEvent::Delta { side, seq: s, .. } => {
                assert_eq!(*side, BookSide::Ask);
                assert_eq!(*s, 2); // shares the book's per-asset stream
            }
            _ => panic!(),
        }
    }
}
