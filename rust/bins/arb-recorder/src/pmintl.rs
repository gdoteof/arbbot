//! Polymarket international CLOB feed — public WS (book/deltas/tape, no seq
//! on the wire: per-market seq synthesized), text PING keepalive, gap ->
//! REST re-snapshot, periodic 300s integrity re-snapshot.
//! Transliteration of record/polymarket.py + polymarket_ws_task.

use crate::core::{
    dec_string, reconnect_forever, sorted_levels, stall_reconnect_s, ws_url, Core, SeqCounter,
};
use crate::health::Liveness;
use anyhow::Result;
use arb_core::model::{now_local_ns, BookSide, Level, TakerSide, TapeEvent, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

pub const CLOB_BASE: &str = "https://clob.polymarket.com";
pub const GAMMA_BASE: &str = "https://gamma-api.polymarket.com";
pub const WS_MARKET_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

fn levels(raw: Option<&Value>, descending: bool) -> Vec<Level> {
    sorted_levels(raw, descending, |l| {
        Some((dec_string(l.get("price")?), dec_string(l.get("size")?)))
    })
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
                    // Trades get their OWN seq stream. Sharing the book's
                    // per-asset counter spends a sequence number that no book
                    // update ever consumes, so the NEXT delta arrives one ahead
                    // of what `apply_delta` expects, is read as a gap, and the
                    // book is DELETED — on every trade, forever. The busier the
                    // market the faster it dies.
                    //
                    // Kalshi and PM-US have always taken `|tape` here and
                    // Python's recorder carries the scar in a comment ("the
                    // traded-markets-die P1"); this path is the one that missed
                    // it, and it is also the only one of the three with no test
                    // pinning it. Measured on the live rs socket 2026-07-29
                    // before the fix: INTL books p90 22,520s stale, 49.5% older
                    // than the 300s resnapshot that is supposed to be their
                    // floor, and 6 of 11 crossed books on the venue.
                    let s = seq.next(&format!("{asset}|tape"));
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



/// A full integrity sweep: every configured book is re-snapshotted at least
/// this often. A parameter of `ws_session` only so the test can shorten it;
/// production is always this constant.
const RESNAP_EVERY: Duration = Duration::from_secs(300);

/// Sweep failures, counted over a cycle and reported when the cycle ends — or
/// when the SESSION does, whichever comes first, which is why this is a `Drop`
/// impl and not a line at the bottom of the loop.
///
/// Every exit from that loop is a `?` or a `bail!`, and the count is a session
/// local, so a report that only fired on a completed cycle would discard up to
/// 300s of failures on reconnect. The old all-at-once sweep could only ever
/// lose ~13s of them. The correlation is adverse: network trouble drops the
/// socket and fails REST together, so the report would go missing exactly when
/// it matters. Live tape 2026-07-29 has cycles that emitted only 72/111 and
/// 92/111 distinct assets — 39 and 19 silent failures in one cycle.
struct SweepFailures {
    failed: usize,
    total: usize,
}

impl SweepFailures {
    /// Report and reset. A silent failure is a book that keeps whatever state
    /// it was left in, which is how one reached 26 HOURS old while the interval
    /// claimed 300s. Counted rather than logged per-token: one line per
    /// configured token would bury it.
    fn report(&mut self) {
        if self.failed > 0 {
            eprintln!(
                "[pm-ws] resnapshot sweep: {}/{} books did NOT refresh — their age is now \
                 unbounded",
                self.failed, self.total
            );
            self.failed = 0;
        }
    }
}

impl Drop for SweepFailures {
    fn drop(&mut self) {
        self.report();
    }
}

pub async fn ws_task(
    core: Arc<Core>,
    liveness: Arc<Liveness>,
    token_ids: Vec<String>,
    clob: Arc<ClobRest>,
) {
    reconnect_forever("pm-ws", || {
        ws_session(&core, &liveness, &token_ids, &clob, RESNAP_EVERY)
    })
    .await
}

async fn ws_session(
    core: &Core,
    liveness: &Liveness,
    token_ids: &[String],
    clob: &ClobRest,
    resnap_every: Duration,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::Message;

    let (mut ws, _) = tokio_tungstenite::connect_async(ws_url("ARBBOT_WS_PMINTL", WS_MARKET_URL)).await?;
    ws.send(Message::Text(
        json!({"type": "market", "assets_ids": token_ids, "custom_feature_enabled": true})
            .to_string(),
    ))
    .await?;
    liveness.beat("polymarket-ws");
    let mut seq = SeqCounter::default();
    let mut last_ping = tokio::time::Instant::now();
    let mut last_resnap = tokio::time::Instant::now();
    let mut last_frame = tokio::time::Instant::now();
    // The sweep, sliced: one book per step rather than all of them at once.
    let resnap_step = resnap_every / token_ids.len().max(1) as u32;
    let mut resnap_cursor = 0usize;
    let mut resnap = SweepFailures { failed: 0, total: token_ids.len() };
    loop {
        let frame = match tokio::time::timeout(Duration::from_secs(5), ws.next()).await {
            Err(_) => {
                if last_frame.elapsed() > Duration::from_secs(stall_reconnect_s()) {
                    anyhow::bail!(
                        "no frame in {}s — socket is half-open, reconnecting", stall_reconnect_s()
                    );
                }
                None
            }
            Ok(None) => anyhow::bail!("ws closed"),
            Ok(Some(f)) => Some(f?),
        };
        if last_ping.elapsed() > Duration::from_secs(10) {
            ws.send(Message::Text("PING".into())).await?;
            last_ping = tokio::time::Instant::now();
        }
        if let Some(frame) = frame {
            last_frame = tokio::time::Instant::now();
            liveness.beat("polymarket-ws");
            let text = match frame {
                Message::Text(t) => t,
                Message::Binary(b) => String::from_utf8_lossy(&b).into_owned(),
                _ => continue,
            };
            for ev in parse_ws_frame(&text, &mut seq) {
                if let Some(need) = core.on_event(&ev) {
                    // A gap DELETED this book (`BookBuilder::apply_delta`
                    // removes it so a desynced book can never be read as live),
                    // so this REST call is the only thing that brings it back.
                    // Swallowing the error left the market dark until the 300s
                    // sweep happened to catch it, or forever if that failed too.
                    let s = seq.next(&need);
                    match clob.book(&need, s).await {
                        Ok(snap) => {
                            core.on_event(&snap);
                        }
                        Err(e) => eprintln!(
                            "[pm-ws] gap on {need}: book was dropped and the REST resnapshot \
                             FAILED ({e:#}) — that market is dark until the next sweep"
                        ),
                    }
                }
            }
        }
        // The integrity sweep — ONE book per step, not all of them at once.
        //
        // This is the same task `ws.next()` is polled on, so a sweep that
        // re-snapshots every token back to back leaves the socket UNREAD for
        // the whole burst. Measured on the live rs recorder 2026-07-29: the
        // tape carries bursts of exactly 111 snapshots (the registry's entire
        // PM leg) spanning 12.7-16.5s, and `polymarket-ws` was flagged stale
        // once every 312.4-313.4s — the 300s interval plus the burst it was
        // hiding. Blind ~4% of the time, on a fixed, predictable schedule.
        //
        // Sliced the way `kalshi.rs` slices its sweep, and for the same reason
        // spelled out over `RESNAP_FULL_S` there. The slice is a single token
        // because `clob.book` takes one token per request regardless, so there
        // is no batching to win by taking more; the cycle is still
        // `resnap_every`, so the floor under every book's age is unchanged, and
        // the request RATE is unchanged — only its burstiness.
        //
        // Raising the interval was the tempting fix and is the wrong one: it
        // makes each stall longer and every book older.
        if last_resnap.elapsed() > resnap_step {
            last_resnap = tokio::time::Instant::now();
            if let Some(tid) = token_ids.get(resnap_cursor) {
                let s = seq.next(tid);
                match clob.book(tid, s).await {
                    Ok(snap) => {
                        core.on_event(&snap);
                    }
                    Err(_) => resnap.failed += 1,
                }
                resnap_cursor += 1;
            }
            if resnap_cursor >= token_ids.len() {
                resnap.report();
                resnap_cursor = 0;
            }
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

    /// A trade must NOT consume a sequence number from the book's stream.
    ///
    /// This is the whole bug. `apply_delta` demands `seq == book.seq + 1`; a
    /// trade that eats a number no book update consumes makes the next delta
    /// look like a gap, and a gap DELETES the book. So every trade killed its
    /// own market, and the busiest markets died fastest — INTL books were
    /// measured at p90 22,520s stale on 2026-07-29, against a 300s resnapshot
    /// that is meant to be their ceiling.
    ///
    /// `pmus.rs` has had `trade_takes_tape_seq_stream` since it was written and
    /// `kalshi.rs` takes `|tape` too. This path had neither the suffix nor a
    /// test, which is exactly why it was the one that broke.
    #[test]
    fn a_trade_does_not_consume_the_books_sequence() {
        let mut seq = SeqCounter::default();
        let book = json!({"event_type": "book", "asset_id": "A",
                          "bids": [{"price": "0.55", "size": "50"}],
                          "asks": [{"price": "0.60", "size": "50"}],
                          "timestamp": 1700000000123i64})
        .to_string();
        let snap_seq = match &parse_ws_frame(&book, &mut seq)[0] {
            TapeEvent::Snapshot { seq, .. } => *seq,
            _ => panic!("expected a snapshot"),
        };

        // A trade lands between the snapshot and the next delta.
        let trade = json!({"event_type": "last_trade_price", "asset_id": "A",
                           "side": "BUY", "price": "0.57", "size": "3",
                           "timestamp": 1700000000456i64})
        .to_string();
        match &parse_ws_frame(&trade, &mut seq)[0] {
            TapeEvent::Trade { seq, .. } => {
                assert_eq!(*seq, 1, "a trade rides its own |tape stream, starting at 1")
            }
            _ => panic!("expected a trade"),
        }

        // ...and the delta after it must still be the snapshot's successor.
        let pc = json!([{"event_type": "price_change", "timestamp": "17",
                         "price_changes": [{"asset_id": "A", "side": "SELL",
                                            "price": "0.61", "size": "9"}]}])
        .to_string();
        match &parse_ws_frame(&pc, &mut seq)[0] {
            TapeEvent::Delta { seq, .. } => assert_eq!(
                *seq,
                snap_seq + 1,
                "the delta after a trade must be seq+1 or apply_delta reads a gap and drops \
                 the book"
            ),
            _ => panic!("expected a delta"),
        }
    }

    /// The same thing proven end-to-end through the book builder, because the
    /// seq arithmetic above is only interesting for what it does to the book.
    #[test]
    fn a_trade_does_not_destroy_the_book() {
        use arb_core::book::BookBuilder;
        let mut seq = SeqCounter::default();
        let mut books = BookBuilder::new();
        let book = json!({"event_type": "book", "asset_id": "A",
                          "bids": [{"price": "0.55", "size": "50"}],
                          "asks": [{"price": "0.60", "size": "50"}],
                          "timestamp": 1700000000123i64})
        .to_string();
        for ev in parse_ws_frame(&book, &mut seq) {
            books.apply_event(&ev).expect("snapshot applies");
        }
        let trade = json!({"event_type": "last_trade_price", "asset_id": "A",
                           "side": "BUY", "price": "0.57", "size": "3",
                           "timestamp": 1700000000456i64})
        .to_string();
        for ev in parse_ws_frame(&trade, &mut seq) {
            books.apply_event(&ev).expect("a trade is not applied to the book");
        }
        let pc = json!([{"event_type": "price_change", "timestamp": "17",
                         "price_changes": [{"asset_id": "A", "side": "SELL",
                                            "price": "0.61", "size": "9"}]}])
        .to_string();
        for ev in parse_ws_frame(&pc, &mut seq) {
            books.apply_event(&ev).expect("the delta after a trade must NOT be a gap");
        }
        assert!(
            books.get(Venue::Polymarket, "A").is_some(),
            "the book must still exist after a trade — it used to be deleted by one"
        );
    }

    /// The cycle-end report and the session-end `Drop` report are the same
    /// counter, so a cycle that reports must not leave anything for `Drop` to
    /// say again.
    #[test]
    fn a_reported_sweep_failure_is_not_reported_twice() {
        let mut f = SweepFailures { failed: 3, total: 111 };
        f.report();
        assert_eq!(f.failed, 0, "a reported failure must not be repeated when the session ends");
    }

    /// The integrity sweep must not stop the socket being read.
    ///
    /// The sweep runs on the read path, so re-snapshotting every token back to
    /// back left `ws.next()` unpolled for the whole burst. Measured on the
    /// live rs recorder 2026-07-29: snapshot bursts of exactly 111 assets
    /// spanning 12.7-16.5s in the tape, and a `polymarket-ws` stale episode
    /// every 312.4-313.4s (300s of interval plus the burst).
    ///
    /// WHAT THIS PROVES: against a `/book` endpoint that takes 100ms and 12
    /// tokens on a 2.4s cycle, the feed never goes 600ms without a frame while
    /// every one of the 12 books is still refreshed inside one cycle, and no
    /// swept snapshot desyncs the delta stream. Put the all-at-once sweep back
    /// and the same session goes silent for 1.2s and is flagged stale — the
    /// live symptom, in miniature. (Measured both ways; see the PR.)
    ///
    /// WHAT IT DOES NOT PROVE: anything about the real venue's latency, and
    /// NOT that the read path is never blocked — it is still blocked for one
    /// REST round trip per step, by design. The claim is bounded stalls, not
    /// no stalls.
    #[tokio::test]
    async fn a_sweep_does_not_stop_the_socket_being_read() {
        use crate::health::Liveness;
        use arb_tape::broadcast::{Broadcaster, MAX_BUFFER};
        use arb_tape::writer::JsonlWriter;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;
        use tokio_tungstenite::tungstenite::Message;

        let tokens: Vec<String> = (0..12).map(|i| format!("T{i:02}")).collect();

        // a /book endpoint as slow as the real one
        let http = TcpListener::bind("127.0.0.1:0").await.expect("bind http");
        let clob_base = format!("http://{}", http.local_addr().expect("http addr"));
        tokio::spawn(async move {
            while let Ok((mut s, _)) = http.accept().await {
                tokio::spawn(async move {
                    let mut buf = [0u8; 2048];
                    let _ = s.read(&mut buf).await;
                    tokio::time::sleep(Duration::from_millis(100)).await;
                    let body = r#"{"bids":[{"price":"0.55","size":"50"}],"asks":[{"price":"0.60","size":"50"}],"timestamp":"1"}"#;
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

        // a market socket that never stops talking, on T00 so the sweep and the
        // delta stream share an asset (and therefore a sequence stream)
        let wsl = TcpListener::bind("127.0.0.1:0").await.expect("bind ws");
        std::env::set_var(
            "ARBBOT_WS_PMINTL",
            format!("ws://{}/", wsl.local_addr().expect("ws addr")),
        );
        tokio::spawn(async move {
            let (s, _) = wsl.accept().await.expect("accept");
            let mut srv = tokio_tungstenite::accept_async(s).await.expect("handshake");
            let _ = srv.next().await; // the subscribe frame
            let book = json!({"event_type": "book", "asset_id": "T00",
                              "bids": [{"price": "0.55", "size": "50"}],
                              "asks": [{"price": "0.60", "size": "50"}],
                              "timestamp": 1i64})
            .to_string();
            if srv.send(Message::Text(book)).await.is_err() {
                return;
            }
            let pc = json!([{"event_type": "price_change", "timestamp": "1",
                             "price_changes": [{"asset_id": "T00", "side": "SELL",
                                                "price": "0.61", "size": "9"}]}])
            .to_string();
            while srv.send(Message::Text(pc.clone())).await.is_ok() {
                tokio::time::sleep(Duration::from_millis(40)).await;
            }
        });

        let dir = std::env::temp_dir().join(format!("arb-pmintl-sweep-{}", std::process::id()));
        let core = Arc::new(Core::new(
            JsonlWriter::new(&dir).expect("writer"),
            Broadcaster::new(MAX_BUFFER),
        ));
        let liveness = Arc::new(Liveness::new(0.6));
        let clob = Arc::new(ClobRest {
            client: reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .expect("client"),
            base: clob_base,
            gamma: String::new(),
        });
        {
            let (core, liveness, clob, tokens) =
                (core.clone(), liveness.clone(), clob.clone(), tokens.clone());
            tokio::spawn(async move {
                let _ =
                    ws_session(&core, &liveness, &tokens, &clob, Duration::from_millis(2400)).await;
            });
        }

        // 3.6s covers a full 2.4s sweep cycle plus the 0.6s it would take to
        // flag the feed stale after an all-at-once burst started.
        let mut stale_ticks = 0usize;
        for _ in 0..90 {
            tokio::time::sleep(Duration::from_millis(40)).await;
            if liveness.check().get("polymarket-ws").copied().unwrap_or(false) {
                stale_ticks += 1;
            }
        }
        assert_eq!(stale_ticks, 0, "the feed went >600ms unread while the sweep ran");
        // ...and not by skipping the sweep: every book must still be refreshed.
        let books = core.snapshot_lines().join("");
        for t in &tokens {
            assert!(books.contains(t.as_str()), "{t} was never re-snapshotted");
        }
        // a swept snapshot takes a number from the same per-asset sequence
        // stream as T00's deltas; if it took the wrong one the book is DELETED
        assert_eq!(core.gap_count(), 0, "a swept snapshot desynced the delta stream");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
