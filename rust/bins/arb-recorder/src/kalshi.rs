//! Kalshi feed — authed WS (read-only recorder key) with REST resync, or
//! credential-free REST polling. Transliteration of record/kalshi.py +
//! kalshi_ws_task/kalshi_poll_task with every quirk preserved:
//! - orderbook_delta carries a CHANGE (delta_fp), not a new total — running
//!   sizes are accumulated per (ticker, raw side, price) and the RESULTING
//!   total is emitted (KalshiWsBook).
//! - both yes_dollars and no_dollars ladders are BIDS; NO bid at q == YES
//!   ask at 1-q (scale-preserving).
//! - wire `seq` is per-SUBSCRIPTION (sid); a per-MARKET seq is synthesized
//!   for the shared BookBuilder (2026-07-20 incident).
//! - trades get their own `|tape` seq stream (traded-markets-die bug).

use crate::core::{
    dec_string, reconnect_forever, stall_reconnect_s, ws_url, Core, SeqCounter,
};
use crate::health::Liveness;
use crate::sign::KalshiSigner;
use anyhow::Result;
use arb_core::dec::Dec;
use arb_core::model::{now_local_ns, BookSide, Level, TakerSide, TapeEvent, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

pub const REST_BASE: &str = "https://api.elections.kalshi.com/trade-api/v2";
pub const WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
pub const WS_PATH: &str = "/trade-api/ws/v2";

// ---------- normalization ----------

fn pair_levels(v: Option<&Value>) -> Vec<(String, String)> {
    v.and_then(Value::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|pq| {
                    let pq = pq.as_array()?;
                    Some((dec_string(pq.first()?), dec_string(pq.get(1)?)))
                })
                .collect()
        })
        .unwrap_or_default()
}

/// orderbook_fp -> YES-denominated snapshot; both ladders are bids.
pub fn normalize_orderbook(ticker: &str, fp: &Value, seq: u64) -> TapeEvent {
    let mut bids: Vec<Level> = pair_levels(fp.get("yes_dollars"))
        .into_iter()
        .filter(|(_, q)| Dec::parse(q).map(|d| d.is_positive()).unwrap_or(false))
        .map(|(p, q)| Level { price: p, size: q })
        .collect();
    let mut asks: Vec<Level> = pair_levels(fp.get("no_dollars"))
        .into_iter()
        .filter(|(_, q)| Dec::parse(q).map(|d| d.is_positive()).unwrap_or(false))
        .filter_map(|(p, q)| {
            Some(Level { price: Dec::parse(&p).ok()?.one_minus().to_string(), size: q })
        })
        .collect();
    bids.sort_by(|a, b| {
        Dec::parse(&b.price).unwrap_or(Dec::ZERO).cmp_num(&Dec::parse(&a.price).unwrap_or(Dec::ZERO))
    });
    asks.sort_by(|a, b| {
        Dec::parse(&a.price).unwrap_or(Dec::ZERO).cmp_num(&Dec::parse(&b.price).unwrap_or(Dec::ZERO))
    });
    TapeEvent::Snapshot {
        venue: Venue::Kalshi,
        market_id: ticker.to_owned(),
        bids,
        asks,
        seq,
        ts_local_ns: now_local_ns(),
        ts_venue: None,
    }
}

pub fn normalize_ws_trade(msg: &Value, seq: u64) -> Option<TapeEvent> {
    Some(TapeEvent::Trade {
        venue: Venue::Kalshi,
        market_id: msg.get("market_ticker")?.as_str()?.to_owned(),
        price: dec_string(msg.get("yes_price_dollars")?),
        size: dec_string(msg.get("count_fp")?),
        taker_side: Some(if msg.get("taker_side").and_then(Value::as_str) == Some("yes") {
            TakerSide::Buy
        } else {
            TakerSide::Sell
        }),
        seq,
        ts_local_ns: now_local_ns(),
        ts_venue: Some(dec_string(msg.get("ts_ms").unwrap_or(&Value::String(String::new())))),
    })
}

/// Running-size accumulator translating Kalshi's change-deltas into
/// new-total YES-denominated deltas.
#[derive(Default)]
pub struct KalshiWsBook {
    sizes: HashMap<(String, String, String), Dec>,
}

impl KalshiWsBook {
    pub fn on_snapshot(&mut self, msg: &Value, seq: u64) -> Option<TapeEvent> {
        let ticker = msg.get("market_ticker")?.as_str()?.to_owned();
        self.sizes.retain(|k, _| k.0 != ticker);
        let mut fp = serde_json::Map::new();
        for side in ["yes", "no"] {
            let ladder = msg
                .get(format!("{side}_dollars_fp"))
                .or_else(|| msg.get(format!("{side}_dollars")))
                .cloned()
                .unwrap_or(Value::Array(vec![]));
            for (p, q) in pair_levels(Some(&ladder)) {
                if let Ok(d) = Dec::parse(&q) {
                    self.sizes.insert((ticker.clone(), side.to_owned(), p), d);
                }
            }
            fp.insert(format!("{side}_dollars"), ladder);
        }
        Some(normalize_orderbook(&ticker, &Value::Object(fp), seq))
    }

    pub fn on_delta(&mut self, msg: &Value, seq: u64) -> Option<TapeEvent> {
        let ticker = msg.get("market_ticker")?.as_str()?.to_owned();
        let side = msg.get("side")?.as_str()?.to_owned();
        let price_str = dec_string(msg.get("price_dollars")?);
        let change = Dec::parse(&dec_string(msg.get("delta_fp")?)).ok()?;
        let key = (ticker.clone(), side.clone(), price_str.clone());
        let old = self.sizes.get(&key).copied().unwrap_or(Dec::ZERO);
        let mut new_size = old.add(&change);
        if !new_size.is_positive() {
            new_size = Dec::ZERO;
            self.sizes.remove(&key);
        } else {
            self.sizes.insert(key, new_size);
        }
        let price = Dec::parse(&price_str).ok()?;
        let (yes_side, yes_price) = if side == "yes" {
            (BookSide::Bid, price_str)
        } else {
            (BookSide::Ask, price.one_minus().to_string())
        };
        Some(TapeEvent::Delta {
            venue: Venue::Kalshi,
            market_id: ticker,
            side: yes_side,
            price: yes_price,
            size: new_size.to_string(),
            seq,
            ts_local_ns: now_local_ns(),
            ts_venue: None,
        })
    }
}

// ---------- REST ----------

pub struct KalshiCatalog {
    pub client: reqwest::Client,
    pub base: String,
}

impl KalshiCatalog {
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(15))
                .build()
                .expect("reqwest client"),
            base: REST_BASE.to_owned(),
        }
    }

    pub async fn orderbook(&self, ticker: &str, seq: u64) -> Result<TapeEvent> {
        let r = self
            .client
            .get(format!("{}/markets/{ticker}/orderbook", self.base))
            .query(&[("depth", "0")])
            .send()
            .await?
            .error_for_status()?;
        let v: Value = r.json().await?;
        let fp = v.get("orderbook_fp").cloned().unwrap_or(Value::Object(Default::default()));
        Ok(normalize_orderbook(ticker, &fp, seq))
    }

    /// ticker -> status string, in batches of 50 (for universe eviction).
    pub async fn statuses(&self, tickers: &[String]) -> Result<HashMap<String, String>> {
        let mut out = HashMap::new();
        for chunk in tickers.chunks(50) {
            let r = self
                .client
                .get(format!("{}/markets", self.base))
                .query(&[("tickers", chunk.join(","))])
                .send()
                .await?
                .error_for_status()?;
            let v: Value = r.json().await?;
            for m in v.get("markets").and_then(Value::as_array).unwrap_or(&vec![]) {
                if let (Some(t), Some(s)) =
                    (m.get("ticker").and_then(Value::as_str), m.get("status").and_then(Value::as_str))
                {
                    out.insert(t.to_owned(), s.to_owned());
                }
            }
        }
        Ok(out)
    }
}

// ---------- tasks ----------

fn ws_subscribe_frame(tickers: &[String]) -> String {
    json!({"id": 1, "cmd": "subscribe",
           "params": {"channels": ["orderbook_delta", "trade"], "market_tickers": tickers}})
        .to_string()
}

/// How long a Kalshi book may go without a REST refresh, and in how many
/// slices that refresh is spread.
///
/// The periodic resnapshot used to ask the WS for `action: get_snapshot` over
/// every sid at once. That call does not work — the gap handler below moved off
/// it in 2026-07-20 and says so in its own comment, "the WS get_snapshot path
/// produced zero snapshots in practice" — so the periodic has been a no-op ever
/// since, and Kalshi books were healed only when a gap happened to be detected.
/// Measured on both recorders 2026-07-29: median book age 292s, p90 2,293s,
/// 49% of books past the 300s this interval claims to guarantee, one at 74,082s.
///
/// It sweeps by REST now, the same call the gap handler uses. Doing all 191
/// tickers in one burst would block the WS read for ~19s, so the universe is
/// covered a slice at a time: every `RESNAP_TICK_S` a tenth of it is refreshed,
/// which keeps the blocking window ~2s while still bounding every book's age at
/// `RESNAP_FULL_S`. Same reasoning as pacing the broadcaster's rebroadcast —
/// a periodic burst sized like the whole universe is a burst that hurts.
const RESNAP_FULL_S: u64 = 300;
const RESNAP_CHUNKS: usize = 10;
const RESNAP_TICK_S: u64 = RESNAP_FULL_S / RESNAP_CHUNKS as u64;

/// How many tickers one resnapshot tick refreshes. Rounds UP, so `RESNAP_CHUNKS`
/// ticks always cover the whole universe rather than leaving a remainder that
/// never gets swept.
fn resnap_batch(total: usize, chunks: usize) -> usize {
    if total == 0 {
        return 0;
    }
    total.div_ceil(chunks.max(1))
}

fn ws_snapshot_request(sids: &[i64]) -> String {
    json!({"id": 99, "cmd": "update_subscription",
           "params": {"sids": sids, "action": "get_snapshot"}})
        .to_string()
}

pub async fn ws_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    tickers: Vec<String>,
    signer: KalshiSigner,
    catalog: Arc<KalshiCatalog>,
) {
    reconnect_forever("kalshi-ws", || ws_session(&core, &liveness, &tickers, &signer, &catalog))
        .await
}

async fn ws_session(
    core: &Core,
    liveness: &Liveness,
    tickers: &[String],
    signer: &KalshiSigner,
    catalog: &KalshiCatalog,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    let mut req = ws_url("ARBBOT_WS_KALSHI", WS_URL).into_client_request()?;
    for (k, v) in signer.headers("GET", WS_PATH) {
        req.headers_mut().insert(
            reqwest::header::HeaderName::from_bytes(k.as_bytes())?,
            v.parse()?,
        );
    }
    let (mut ws, _) = tokio_tungstenite::connect_async(req).await?;
    ws.send(Message::Text(ws_subscribe_frame(tickers))).await?;
    liveness.beat("kalshi-ws");

    let mut book = KalshiWsBook::default();
    let mut sid_seq: HashMap<i64, i64> = HashMap::new();
    let mut mseq = SeqCounter::default();
    let mut last_resnap = tokio::time::Instant::now();
    let mut last_frame = tokio::time::Instant::now();
    let mut resnap_cursor = 0usize;

    loop {
        // The integrity sweep. Gated on `tickers`, not `sid_seq`: a book that
        // never got a subscription confirmation is exactly the one that needs
        // refreshing, and keying off sids meant the markets in the worst shape
        // were the ones the sweep skipped.
        if last_resnap.elapsed() > Duration::from_secs(RESNAP_TICK_S) && !tickers.is_empty() {
            let batch = resnap_batch(tickers.len(), RESNAP_CHUNKS);
            let mut failed = 0usize;
            for i in 0..batch {
                let t = &tickers[(resnap_cursor + i) % tickers.len()];
                let s = mseq.next(t);
                match catalog.orderbook(t, s).await {
                    Ok(snap) => {
                        core.on_event(&snap);
                    }
                    Err(_) => failed += 1,
                }
            }
            resnap_cursor = (resnap_cursor + batch) % tickers.len();
            // A silent failure here is a book that keeps whatever state it was
            // left in, which is how one reached 74,082s old against a 300s
            // interval. Counted, not logged per ticker.
            if failed > 0 {
                eprintln!(
                    "[kalshi-ws] resnapshot slice: {failed}/{batch} books did NOT refresh — \
                     their age is now unbounded"
                );
            }
            last_resnap = tokio::time::Instant::now();
        }
        let frame = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Err(_) => {
                // Deliberately NOT a liveness beat. Beating here made a dead
                // socket look healthy forever; the tracker must report what the
                // feed actually delivered.
                if last_frame.elapsed() > Duration::from_secs(stall_reconnect_s()) {
                    anyhow::bail!(
                        "no frame in {}s — socket is half-open, reconnecting", stall_reconnect_s()
                    );
                }
                continue;
            }
            Ok(None) => anyhow::bail!("ws closed"),
            Ok(Some(f)) => f?,
        };
        last_frame = tokio::time::Instant::now();
        liveness.beat("kalshi-ws");
        let text = match frame {
            tokio_tungstenite::tungstenite::Message::Text(t) => t,
            tokio_tungstenite::tungstenite::Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
            _ => continue,
        };
        let m: Value = match serde_json::from_str(&text) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let typ = m.get("type").and_then(Value::as_str).unwrap_or("");
        let sid = m.get("sid").and_then(Value::as_i64);
        let seq = m.get("seq").and_then(Value::as_i64);
        if matches!(typ, "orderbook_snapshot" | "orderbook_delta") {
            if let (Some(sid), Some(seq)) = (sid, seq) {
                let expected = sid_seq.get(&sid).copied();
                if typ == "orderbook_delta" {
                    if let Some(exp) = expected {
                        if seq != exp + 1 {
                            ws.send(Message::Text(ws_snapshot_request(&[sid]))).await?;
                            sid_seq.remove(&sid);
                            continue;
                        }
                    }
                }
                sid_seq.insert(sid, seq);
            }
        }
        let payload = m.get("msg").cloned().unwrap_or(Value::Object(Default::default()));
        match typ {
            "orderbook_snapshot" => {
                if let Some(t) = payload.get("market_ticker").and_then(Value::as_str) {
                    let s = mseq.next(t);
                    if let Some(ev) = book.on_snapshot(&payload, s) {
                        core.on_event(&ev);
                    }
                }
            }
            "orderbook_delta" => {
                if let Some(t) = payload.get("market_ticker").and_then(Value::as_str) {
                    let t = t.to_owned();
                    let s = mseq.next(&t);
                    if let Some(ev) = book.on_delta(&payload, s) {
                        if core.on_event(&ev).is_some() {
                            // gap: REST snapshot resync (auth-free, proven —
                            // the WS get_snapshot path produced zero
                            // snapshots in practice, 2026-07-20)
                            let s2 = mseq.next(&t);
                            if let Ok(snap) = catalog.orderbook(&t, s2).await {
                                core.on_event(&snap);
                            }
                        }
                    }
                }
            }
            "trade" => {
                if let Some(t) = payload.get("market_ticker").and_then(Value::as_str) {
                    let s = mseq.next(&format!("{t}|tape"));
                    if let Some(ev) = normalize_ws_trade(&payload, s) {
                        core.on_event(&ev);
                    }
                }
            }
            _ => {}
        }
    }
}

pub async fn poll_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    tickers: Vec<String>,
    catalog: Arc<KalshiCatalog>,
    interval_s: f64,
) {
    let mut seq = SeqCounter::default();
    loop {
        for ticker in &tickers {
            match catalog.orderbook(ticker, seq.next(ticker)).await {
                Ok(snap) => {
                    core.on_event(&snap);
                    liveness.beat("kalshi-rest");
                }
                Err(e) => {
                    eprintln!("[kalshi-poll] {ticker}: {e:#}");
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }
            tokio::time::sleep(Duration::from_secs_f64(
                interval_s / tickers.len().max(1) as f64,
            ))
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn delta_accumulates_changes_into_totals() {
        let mut b = KalshiWsBook::default();
        let snap = json!({"market_ticker": "T",
                          "yes_dollars_fp": [["0.4300", "697.00"]],
                          "no_dollars_fp": [["0.5000", "600.00"]]});
        let ev = b.on_snapshot(&snap, 1).unwrap();
        match &ev {
            TapeEvent::Snapshot { bids, asks, .. } => {
                assert_eq!(bids[0].price, "0.4300");
                assert_eq!(asks[0].price, "0.5000"); // 1 - 0.5000
                assert_eq!(asks[0].size, "600.00");
            }
            _ => panic!(),
        }
        // change of -100 on the yes ladder -> new total 597.00
        let d = json!({"market_ticker": "T", "side": "yes",
                       "price_dollars": "0.4300", "delta_fp": "-100.00"});
        match b.on_delta(&d, 2).unwrap() {
            TapeEvent::Delta { side, price, size, .. } => {
                assert_eq!(side, BookSide::Bid);
                assert_eq!(price, "0.4300");
                assert_eq!(size, "597.00");
            }
            _ => panic!(),
        }
        // NO-side change maps to YES ask at 1-q
        let d2 = json!({"market_ticker": "T", "side": "no",
                        "price_dollars": "0.5000", "delta_fp": "-600.00"});
        match b.on_delta(&d2, 3).unwrap() {
            TapeEvent::Delta { side, price, size, .. } => {
                assert_eq!(side, BookSide::Ask);
                assert_eq!(price, "0.5000");
                assert_eq!(size, "0"); // clamped, level removed
            }
            _ => panic!(),
        }
    }

    /// One full cycle must touch EVERY ticker, remainder included.
    ///
    /// The point of slicing the sweep is to bound each book's age at
    /// `RESNAP_FULL_S`. That guarantee only holds if `RESNAP_CHUNKS` ticks
    /// cover the universe — a batch that rounds DOWN leaves a tail that is
    /// never swept, which is the same "healed only by luck" state this change
    /// exists to end. 191 tickers over 10 chunks is 20 per tick, not 19.
    #[test]
    fn a_full_cycle_refreshes_every_ticker() {
        for total in [1usize, 7, 10, 11, 191, 1109] {
            let batch = resnap_batch(total, RESNAP_CHUNKS);
            let mut seen = vec![false; total];
            let mut cursor = 0usize;
            for _ in 0..RESNAP_CHUNKS {
                for i in 0..batch {
                    seen[(cursor + i) % total] = true;
                }
                cursor = (cursor + batch) % total;
            }
            assert!(
                seen.iter().all(|s| *s),
                "total={total} batch={batch}: {} ticker(s) never swept in a full cycle",
                seen.iter().filter(|s| !**s).count()
            );
        }
        assert_eq!(resnap_batch(191, 10), 20, "must round up, not down");
        assert_eq!(resnap_batch(0, 10), 0, "an empty universe asks for nothing");
    }

    /// The blocking window is what makes this safe to run inline in the WS read
    /// loop: one slice, not the whole universe.
    #[test]
    fn a_slice_is_a_small_fraction_of_the_universe() {
        let batch = resnap_batch(191, RESNAP_CHUNKS);
        assert!(batch <= 191 / 5, "a slice of {batch} is too much to block the reader on");
        assert_eq!(RESNAP_TICK_S * RESNAP_CHUNKS as u64, RESNAP_FULL_S);
    }
}
