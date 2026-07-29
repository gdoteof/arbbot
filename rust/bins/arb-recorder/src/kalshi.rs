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

    /// A REST orderbook heals the accumulator, not just the published book.
    ///
    /// `sizes` is the only place a level's true total lives, and `on_snapshot`
    /// is its only reset — which the venue sends once per ticker per SESSION
    /// (kalshi-ws-snapshots-only-on-subscribe). So a REST refresh that went
    /// straight to `Core::on_event` handed truth to the BookBuilder and left
    /// `sizes` accumulating from a total the venue had already stopped agreeing
    /// with: the next delta on that level republished the poisoned running sum
    /// and undid the heal. Through this the same reset runs, and a change lost
    /// to a dropped frame survives one sweep instead of the whole session.
    ///
    /// Only the two ladders `normalize_orderbook` reads are carried over, so the
    /// event published here is exactly the one the REST path built for itself.
    pub fn on_rest_snapshot(&mut self, ticker: &str, fp: &Value, seq: u64) -> Option<TapeEvent> {
        self.on_snapshot(
            &json!({"market_ticker": ticker,
                    "yes_dollars": fp.get("yes_dollars").cloned().unwrap_or(Value::Array(vec![])),
                    "no_dollars": fp.get("no_dollars").cloned().unwrap_or(Value::Array(vec![]))}),
            seq,
        )
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

    /// Raw `orderbook_fp` ladders. Deliberately NOT a ready-made `TapeEvent`:
    /// this call returned one for the credential-free poller, the WS paths
    /// reused it for their heals, and a heal that publishes without telling
    /// `KalshiWsBook` about it is the whole defect. Each caller now says what
    /// the snapshot means for its own state.
    pub async fn orderbook_fp(&self, ticker: &str) -> Result<Value> {
        let r = self
            .client
            .get(format!("{}/markets/{ticker}/orderbook", self.base))
            .query(&[("depth", "0")])
            .send()
            .await?
            .error_for_status()?;
        let v: Value = r.json().await?;
        Ok(v.get("orderbook_fp").cloned().unwrap_or(Value::Object(Default::default())))
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

/// One slice of the integrity sweep: `resnap_batch` tickers from `cursor`,
/// refreshed THROUGH `book` (see `on_rest_snapshot`). Returns how far to
/// advance the cursor — the FULL slice width, skipped tickers included, so a
/// closed market cannot shift the sweep's phase and cost the universe the
/// coverage guarantee `a_full_cycle_refreshes_every_ticker` pins.
///
/// It walks the ticker list built at STARTUP, which is why it has to ask which
/// of those markets is still live. `main`'s universe maintainer evicts the
/// books of markets the venue no longer reports as active, precisely so the 30s
/// rebroadcast stops feeding the engine a frozen book — and from the moment
/// this sweep stopped being a no-op it re-fetched those same tickers within
/// 300s, republished them into the BookBuilder and undid the eviction. The
/// eviction was written against a sweep that did nothing.
async fn resnap_slice(
    core: &Core,
    book: &mut KalshiWsBook,
    mseq: &mut SeqCounter,
    catalog: &KalshiCatalog,
    tickers: &[String],
    cursor: usize,
) -> usize {
    let batch = resnap_batch(tickers.len(), RESNAP_CHUNKS);
    let (mut tried, mut failed) = (0usize, 0usize);
    for i in 0..batch {
        let t = &tickers[(cursor + i) % tickers.len()];
        if core.is_evicted(Venue::Kalshi, t) {
            continue;
        }
        tried += 1;
        let s = mseq.next(t);
        match catalog.orderbook_fp(t).await {
            Ok(fp) => {
                if let Some(snap) = book.on_rest_snapshot(t, &fp, s) {
                    core.on_event(&snap);
                }
            }
            Err(_) => failed += 1,
        }
    }
    // A silent failure here is a book that keeps whatever state it was left in,
    // which is how one reached 74,082s old against a 300s interval. Counted,
    // not logged per ticker.
    if failed > 0 {
        eprintln!(
            "[kalshi-ws] resnapshot slice: {failed}/{tried} books did NOT refresh — \
             their age is now unbounded"
        );
    }
    batch
}

/// One WS message: wire-seq bookkeeping, then the book or tape update. Returns
/// the ticker whose book needs a REST resnapshot, if the BookBuilder found a
/// per-market desync applying it.
///
/// Split out of the read loop so the gap branch below can be exercised without
/// a socket and a signing key.
fn on_ws_message(
    core: &Core,
    book: &mut KalshiWsBook,
    sid_seq: &mut HashMap<i64, i64>,
    mseq: &mut SeqCounter,
    m: &Value,
) -> Option<String> {
    let typ = m.get("type").and_then(Value::as_str).unwrap_or("");
    let sid = m.get("sid").and_then(Value::as_i64);
    let seq = m.get("seq").and_then(Value::as_i64);
    if matches!(typ, "orderbook_snapshot" | "orderbook_delta") {
        if let (Some(sid), Some(seq)) = (sid, seq) {
            // A gap in the wire sequence is NOT a reason to drop THIS message.
            //
            // `seq` counts messages on the SID, and one sid carries every
            // subscribed ticker, so a gap says "a message was lost somewhere in
            // the universe" and cannot say whose. The message in hand is
            // meanwhile intact: a valid `delta_fp` for a NAMED ticker. Take the
            // two cases and dropping it never wins. If that ticker is not the
            // one that lost a frame, its accumulator was CORRECT and the drop
            // is the only thing that puts an offset in it. If it is, the
            // accumulator is already short the lost change and applying leaves
            // it short by exactly that — while dropping makes it short by the
            // lost change PLUS this one. Skipping only ever added a second
            // offset to react to damage it could not locate.
            //
            // `delta_fp` is a CHANGE, so each one skipped is an offset that
            // lasts until the sweep heals that ticker: 0.93% of 64,553 sweep
            // comparisons over 7.6h of live tape disagreed with REST truth, 27
            // levels holding the SAME nonzero offset across >=3 consecutive
            // sweeps.
            //
            // Skipping also bypassed the only per-market recovery this file
            // has. Applying the delta is what makes the BookBuilder report a
            // desync, and that is what triggers the targeted REST resnapshot
            // the caller does. What the branch did instead was ask the WS for
            // `get_snapshot`, which the 2026-07-20 note below records as having
            // produced zero snapshots in practice.
            //
            // So there is nothing to do here but re-anchor, say it happened,
            // and leave the damage to the sweep that bounds it.
            if let Some(exp) = sid_seq.insert(sid, seq) {
                if typ == "orderbook_delta" && seq != exp + 1 {
                    eprintln!(
                        "[kalshi-ws] sid {sid} seq {exp} -> {seq}: a book message was lost; \
                         which ticker it belonged to is unknowable (seq is per-sid) and its \
                         running sizes are off until the next REST sweep heals it"
                    );
                }
            }
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
                        return Some(t);
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
    None
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
        // were the ones the sweep skipped. It refreshes THROUGH `book`, which is
        // what bounds the damage of a dropped delta — see `on_rest_snapshot`.
        if last_resnap.elapsed() > Duration::from_secs(RESNAP_TICK_S) && !tickers.is_empty() {
            let advance =
                resnap_slice(core, &mut book, &mut mseq, catalog, tickers, resnap_cursor).await;
            resnap_cursor = (resnap_cursor + advance) % tickers.len();
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
        if let Some(t) = on_ws_message(core, &mut book, &mut sid_seq, &mut mseq, &m) {
            // gap: REST snapshot resync (auth-free, proven — the WS
            // get_snapshot path produced zero snapshots in practice,
            // 2026-07-20)
            let s2 = mseq.next(&t);
            if let Ok(fp) = catalog.orderbook_fp(&t).await {
                if let Some(snap) = book.on_rest_snapshot(&t, &fp, s2) {
                    core.on_event(&snap);
                }
            }
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
            let s = seq.next(ticker);
            // No accumulator on this path: REST totals ARE the book.
            match catalog.orderbook_fp(ticker).await {
                Ok(fp) => {
                    core.on_event(&normalize_orderbook(ticker, &fp, s));
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

    fn test_core(tag: &str) -> (Core, std::path::PathBuf) {
        use arb_tape::broadcast::{Broadcaster, MAX_BUFFER};
        use arb_tape::writer::JsonlWriter;
        let dir = std::env::temp_dir().join(format!("arb-kalshi-{tag}-{}", std::process::id()));
        (Core::new(JsonlWriter::new(&dir).expect("writer"), Broadcaster::new(MAX_BUFFER)), dir)
    }

    /// A wire-seq gap must not cost a SECOND delta.
    ///
    /// `seq` is per-SID and one sid carries the whole universe, so the lost
    /// message and the message after it need not be the same market's — and
    /// when they are not, dropping the second one takes a CORRECT accumulator
    /// and puts a permanent offset in it, in addition to the one the gap
    /// already caused elsewhere. Here the frame lost at seq 3 is A's and the
    /// delta at seq 4 is B's: B's book was never wrong, and its change must
    /// land.
    #[test]
    fn a_wire_gap_does_not_discard_the_next_tickers_delta() {
        let (core, dir) = test_core("gap");
        let mut book = KalshiWsBook::default();
        let mut sid_seq: HashMap<i64, i64> = HashMap::new();
        let mut mseq = SeqCounter::default();
        for (t, seq) in [("A", 1), ("B", 2)] {
            let m = json!({"type": "orderbook_snapshot", "sid": 1, "seq": seq,
                           "msg": {"market_ticker": t,
                                   "yes_dollars_fp": [["0.4300", "697.00"]],
                                   "no_dollars_fp": [["0.5000", "600.00"]]}});
            assert_eq!(on_ws_message(&core, &mut book, &mut sid_seq, &mut mseq, &m), None);
        }
        let d = json!({"type": "orderbook_delta", "sid": 1, "seq": 4,
                       "msg": {"market_ticker": "B", "side": "yes",
                               "price_dollars": "0.4300", "delta_fp": "-100.00"}});
        assert_eq!(
            on_ws_message(&core, &mut book, &mut sid_seq, &mut mseq, &d),
            None,
            "a wire gap is not a per-market desync — there is nothing named to resnapshot"
        );
        let lines = core.snapshot_lines();
        let b = lines.iter().find(|l| l.contains(r#""market_id":"B""#)).expect("B has a book");
        assert!(b.contains("597.00"), "B's change was discarded with A's gap: {b}");
        assert_eq!(core.gap_count(), 0, "a wire gap must not be reported as a per-market gap");
        // ...and the next message re-anchors rather than being read as a gap too.
        let d2 = json!({"type": "orderbook_delta", "sid": 1, "seq": 5,
                        "msg": {"market_ticker": "B", "side": "yes",
                                "price_dollars": "0.4300", "delta_fp": "-97.00"}});
        on_ws_message(&core, &mut book, &mut sid_seq, &mut mseq, &d2);
        let lines = core.snapshot_lines();
        let b = lines.iter().find(|l| l.contains(r#""market_id":"B""#)).expect("B has a book");
        assert!(b.contains("500.00"), "the delta after the gapped one was discarded: {b}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An evicted book must not come back on the next sweep.
    ///
    /// The universe maintainer evicts a market the venue stopped reporting as
    /// active so the 30s rebroadcast stops shipping its frozen book to the
    /// engine. The sweep walks the STARTUP ticker list, so before this it
    /// re-fetched that market by REST within 300s and republished it — the
    /// eviction had a 30-second half-life.
    #[tokio::test]
    async fn the_sweep_does_not_resurrect_an_evicted_book() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        // an /orderbook endpoint that records what it was asked for
        let asked: Arc<std::sync::Mutex<Vec<String>>> = Arc::new(std::sync::Mutex::new(vec![]));
        let http = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
        let base = format!("http://{}", http.local_addr().expect("http addr"));
        {
            let asked = asked.clone();
            tokio::spawn(async move {
                while let Ok((mut s, _)) = http.accept().await {
                    let asked = asked.clone();
                    tokio::spawn(async move {
                        let mut buf = [0u8; 2048];
                        let n = s.read(&mut buf).await.unwrap_or(0);
                        asked
                            .lock()
                            .expect("asked lock")
                            .push(String::from_utf8_lossy(&buf[..n]).into_owned());
                        let body = r#"{"orderbook_fp":{"yes_dollars":[["0.4300","697.00"]],"no_dollars":[]}}"#;
                        let _ = s
                            .write_all(
                                format!(
                                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\n\
                                     content-length: {}\r\nconnection: close\r\n\r\n{body}",
                                    body.len()
                                )
                                .as_bytes(),
                            )
                            .await;
                    });
                }
            });
        }

        let (core, dir) = test_core("evict");
        let catalog = KalshiCatalog {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("client"),
            base,
        };
        let tickers: Vec<String> = ["A", "B", "C"].iter().map(|t| (*t).to_owned()).collect();
        let mut book = KalshiWsBook::default();
        let mut mseq = SeqCounter::default();

        // B was live, then the venue stopped calling it active
        let fp = json!({"yes_dollars": [["0.4300", "697.00"]], "no_dollars": []});
        core.on_event(&normalize_orderbook("B", &fp, mseq.next("B")));
        core.evict_book(Venue::Kalshi, "B");

        let mut cursor = 0usize;
        for _ in 0..RESNAP_CHUNKS {
            let advance =
                resnap_slice(&core, &mut book, &mut mseq, &catalog, &tickers, cursor).await;
            assert_eq!(advance, resnap_batch(tickers.len(), RESNAP_CHUNKS));
            cursor = (cursor + advance) % tickers.len();
        }
        let books = core.snapshot_lines().join("");
        assert!(
            !books.contains(r#""market_id":"B""#),
            "the sweep resurrected an evicted book: {books}"
        );
        assert!(
            !asked.lock().expect("asked lock").iter().any(|r| r.contains("/markets/B/")),
            "the sweep asked the venue for an evicted ticker"
        );
        // ...and it is still a sweep: the live tickers were refreshed.
        for t in ["A", "C"] {
            assert!(books.contains(&format!(r#""market_id":"{t}""#)), "{t} was never swept");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

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

    /// A change lost to a dropped frame must not outlive the sweep.
    ///
    /// `delta_fp` is a CHANGE, so `sizes` is the only record of a level's true
    /// total and one missed frame offsets it forever: the wire seq is per-sid
    /// over all ~191 tickers, so the gap handler cannot even name the ticker
    /// whose change it lost, and the venue re-snapshots only on subscribe. The
    /// offset is invisible to `crossing()` — an inflated bid SIZE does not
    /// invert the touch — and surfaces as depth that is not there when the maker
    /// clip or a take-take walks the book, so the fill comes back short and the
    /// cross-venue leg is left naked. Periodic REST truth is the only thing that
    /// ends it, and only if it reaches the accumulator.
    #[test]
    fn a_rest_heal_ends_a_dropped_delta() {
        let mut b = KalshiWsBook::default();
        b.on_snapshot(
            &json!({"market_ticker": "T",
                    "yes_dollars_fp": [["0.4300", "697.00"]],
                    "no_dollars_fp": [["0.5000", "600.00"]]}),
            1,
        );
        // The venue takes 100 off the yes level (697 -> 597) and we never see
        // that frame. `sizes` still says 697.00; nothing in the feed disagrees.
        // The sweep pulls REST truth for the ticker.
        let fp = json!({"yes_dollars": [["0.4300", "597.00"]],
                        "no_dollars": [["0.5000", "600.00"]]});
        match b.on_rest_snapshot("T", &fp, 2).unwrap() {
            TapeEvent::Snapshot { bids, .. } => assert_eq!(bids[0].size, "597.00"),
            _ => panic!(),
        }
        // The next change lands on venue truth: 597 - 97 = 500. When the sweep
        // published without telling the accumulator, this one delta undid the
        // heal and republished 697 - 97 = 600 — phantom depth, permanently.
        let d = json!({"market_ticker": "T", "side": "yes",
                       "price_dollars": "0.4300", "delta_fp": "-97.00"});
        match b.on_delta(&d, 3).unwrap() {
            TapeEvent::Delta { size, .. } => {
                assert_eq!(size, "500.00", "the accumulator kept the change it had lost")
            }
            _ => panic!(),
        }
    }

    /// Healing must not change what the sweep PUBLISHES. The REST path's own
    /// event is the reference; `on_rest_snapshot` only adds the reset behind it.
    #[test]
    fn a_healed_snapshot_publishes_what_the_rest_path_published() {
        let fp = json!({"yes_dollars": [["0.4100", "10.00"], ["0.4300", "597.00"]],
                        "no_dollars": [["0.5000", "600.00"]]});
        let mut b = KalshiWsBook::default();
        match (normalize_orderbook("T", &fp, 7), b.on_rest_snapshot("T", &fp, 7).unwrap()) {
            (
                TapeEvent::Snapshot { market_id: m1, bids: b1, asks: a1, seq: s1, .. },
                TapeEvent::Snapshot { market_id: m2, bids: b2, asks: a2, seq: s2, .. },
            ) => assert_eq!((m1, b1, a1, s1), (m2, b2, a2, s2)),
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
