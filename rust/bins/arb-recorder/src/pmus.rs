//! Polymarket US (QCX DCM) feed — authed market-data WS (MARKET_DATA is a
//! self-contained full-book snapshot; no delta/seq reconstruction) with
//! chunked 150-slug subscriptions, or credential-free REST polling.
//! Transliteration of record/polymarket_us.py + the recorder tasks.

use crate::core::{
    dec_string, reconnect_forever, sorted_levels, stall_reconnect_s, ws_url, Core, SeqCounter,
};
use crate::health::Liveness;
use crate::sign::PmusSigner;
use anyhow::Result;
use arb_core::model::{now_local_ns, Level, TakerSide, TapeEvent, Venue};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;

pub const GATEWAY_BASE: &str = "https://gateway.polymarket.us/v1";
pub const WS_MARKETS_URL: &str = "wss://api.polymarket.us/v1/ws/markets";
pub const WS_MARKETS_PATH: &str = "/v1/ws/markets";

/// Unwrap PM-US's money type, `{"currency":"USD","value":"5.0000"}`.
///
/// The `currency` label travels with the WRAPPER, not with the meaning: the
/// venue reuses the same type for a price (dollars) and for a trade's
/// `quantity` (CONTRACTS — see `trade_size_is_the_contract_count_inside_the_
/// money_wrapper`, which settles it against the book depth the print consumes).
/// Reading `currency` as the unit is the trap this helper exists to keep in one
/// place.
///
/// A field that arrives BARE is taken as-is, which is the same spelling the
/// venue already uses for a book level's `qty` in this very feed. A field that
/// is an object without `value` is a shape nobody has seen: `dec_string`
/// refuses it and says so.
fn money_value(v: &Value) -> Option<String> {
    dec_string(v.get("value").unwrap_or(v))
}

fn levels(raw: Option<&Value>, descending: bool) -> Vec<Level> {
    sorted_levels(raw, descending, |l| {
        Some((money_value(l.get("px")?)?, dec_string(l.get("qty")?)?))
    })
}

pub fn normalize_book(slug: &str, md: &Value, seq: u64) -> TapeEvent {
    TapeEvent::Snapshot {
        venue: Venue::PolymarketUs,
        market_id: slug.to_owned(),
        bids: levels(md.get("bids"), true),
        asks: levels(md.get("offers"), false), // ask side = "offers"
        seq,
        ts_local_ns: now_local_ns(),
        ts_venue: dec_string(md.get("transactTime").unwrap_or(&Value::String(String::new()))),
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
        // BOTH legs are money-wrapped, and `size` is a contract count despite
        // the wrapper's `"currency":"USD"` — see `money_value`. Resolved before
        // the sequence number is taken, so a frame we cannot read does not also
        // spend a seq.
        //
        // All-or-nothing on the COMPOSITE case, which is ticket #58's: an
        // absent or unreadable leg costs the whole trade, where
        // `unwrap_or_default` used to write `price: ""` — a lie of the same
        // kind as a stringified object. It is NOT all-or-nothing on an explicit
        // JSON `null`: `dec_string` maps null to `Some("")` (that arm is what
        // keeps this change inert for the fields that legitimately arrive
        // absent), so `{"currency":"USD","value":null}` still writes a trade
        // with an empty price. Unchanged from before this fix, so not a
        // regression, and no such payload has ever been seen on four full
        // tapes — but it is a hole, not a guarantee, and saying otherwise here
        // would claim more than the code does.
        let (Some(price), Some(size)) =
            (t.get("price").and_then(money_value), t.get("quantity").and_then(money_value))
        else {
            return vec![];
        };
        let s = seq.next(&format!("{slug}|tape"));
        return vec![TapeEvent::Trade {
            venue: Venue::PolymarketUs,
            market_id: slug,
            price,
            size,
            taker_side: Some(if taker.contains("BUY") { TakerSide::Buy } else { TakerSide::Sell }),
            seq: s,
            ts_local_ns: now_local_ns(),
            ts_venue: dec_string(t.get("tradeTime").unwrap_or(&Value::String(String::new()))),
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
    reconnect_forever("pmus-ws", || ws_session(&core, &liveness, &slugs, &signer)).await
}

async fn ws_session(
    core: &Core,
    liveness: &Liveness,
    slugs: &[String],
    signer: &PmusSigner,
) -> Result<()> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    // The signed headers below are computed over the compiled-in WS_MARKETS_PATH,
    // not over this URL, so the override cannot disturb auth.
    let mut req = ws_url("ARBBOT_WS_PMUS", WS_MARKETS_URL).into_client_request()?;
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
        let t = json!({"trade": {"marketSlug": "s-1", "price": {"value": "0.55", "currency": "USD"},
                                  "quantity": {"value": "12.0000", "currency": "USD"},
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

    /// PROVENANCE, because this test's whole subject is not claiming more than
    /// you have. RECORDED, byte for byte, from the 2026-07-29 tape line for this
    /// market: `marketSlug`, `tradeTime`, `price`'s value, and `quantity` —
    /// the entire wrapper verbatim, since that stringified object is precisely
    /// what the defect wrote into `size`. RECONSTRUCTED: `price`'s wrapper (the
    /// recorder already unwrapped `price` before writing, so the tape holds
    /// `"0.2800"` and the wrapper around it is known from the call site, not
    /// from the recording), and `taker.intent`, which the tape NEVER stores —
    /// it keeps only the derived `taker_side`. `BUY_SHORT` is DERIVED, not
    /// observed: the tape says `taker_side: "buy"`, so the intent contained
    /// "BUY" and was not `SELL_LONG`; the book below says the aggressor
    /// consumed the resting BID; and hitting a resting YES bid is
    /// `ORDER_INTENT_BUY_SHORT` (`docs/venue-quirks.md`,
    /// `pmus-intent-buy-short`). Neither field is load-bearing for the
    /// assertion, which is exactly why an invented value could sit here
    /// unnoticed.
    ///
    /// UNITS. `quantity` arrives inside the venue's generic money wrapper —
    /// `{"currency":"USD","value":"5.0000"}` — and the `currency: USD` label is
    /// the whole trap, because it reads as "these are dollars". It is not: it
    /// belongs to the WRAPPER, not to the meaning.
    ///
    /// The book settles it by ATTRIBUTION, not by adjacency. PM-US stamps the
    /// post-trade snapshot's `transactTime` byte-identically to the trade's
    /// `tradeTime`, so the snapshot carrying this print's consequence is
    /// IDENTIFIED rather than merely next to it:
    /// `2026-07-29T01:56:27.328744187Z` appears on exactly one snapshot for
    /// this market. The resting bid at 0.2800 holds 240.0000 across the five
    /// snapshots before it, and reads 235.0000 on it and after — consumed by
    /// EXACTLY the printed `value`. Nothing else can account for the move:
    /// this is the ONLY trade on this market all day, so no interleaving is
    /// possible. Book sizes are contracts (`qty` is the bare decimal the venue
    /// also takes as a bare integer `quantity` on `POST /v1/orders`), so the
    /// printed value is contracts. USD notional would have had to remove
    /// 5.0000/0.28 = 17.86 from that level, and it did not.
    ///
    /// Not a one-off. Sweeping the same tape for every print whose post-trade
    /// snapshot is identified this way: thousands match contracts and **zero**
    /// match notional. The exact count moves with how you treat prints sharing
    /// one snapshot (3,113 by the sweep in this PR's transcript); the zero does
    /// not. A non-integral `value` is NOT evidence against contracts either —
    /// PM-US positions are genuinely fractional, and 1,899 of those matches
    /// carry a fractional size (`51.4800`, `1.0500`) agreeing to the cent with
    /// an equally fractional level.
    #[test]
    fn trade_size_is_the_contract_count_inside_the_money_wrapper() {
        let mut seq = SeqCounter::default();
        let t = json!({"trade": {
            "marketSlug": "ewc-pres-bra-2026-10-04-flabol",
            "price": {"currency": "USD", "value": "0.2800"},
            "quantity": {"currency": "USD", "value": "5.0000"},
            "taker": {"intent": "ORDER_INTENT_BUY_SHORT"},
            "tradeTime": "2026-07-29T01:56:27.328744187Z"}});
        match &parse_ws_message(&t, &mut seq)[0] {
            TapeEvent::Trade { price, size, .. } => {
                assert_eq!(price, "0.2800");
                assert_eq!(size, "5.0000", "size must be the contract count, not the wrapper");
            }
            _ => panic!(),
        }
    }

    /// A shape nobody anticipated must COST us the event, not be laundered into
    /// a decimal-shaped string.
    ///
    /// The money wrapper without its `value` stands in for any future venue
    /// change. Before this was fixed the whole object went into `size` as
    /// `"{\"currency\":\"USD\"}"` and then round-tripped `serde_json` and
    /// `arb-recorder --parse-check` untouched, because both only ask whether the
    /// string survives re-serialization — and a string that was already a string
    /// when it arrived always does.
    #[test]
    fn a_trade_field_of_an_unknown_shape_is_dropped_not_stringified() {
        let mut seq = SeqCounter::default();
        let t = json!({"trade": {
            "marketSlug": "s-1",
            "price": {"currency": "USD", "value": "0.2800"},
            "quantity": {"currency": "USD"},
            "taker": {"intent": "ORDER_INTENT_BUY_LONG"},
            "tradeTime": "2026-07-29T01:56:27Z"}});
        assert!(
            parse_ws_message(&t, &mut seq).is_empty(),
            "a trade whose quantity is not a decimal must not reach the tape at all"
        );
    }

    /// The same exposure one call site over: `quantity` was never special, the
    /// silent `to_string()` under it was.
    ///
    /// A level's SIZE happens to be safe already — `sorted_levels` runs
    /// `Dec::parse` over it and a stringified object does not parse, so the
    /// level is dropped. Nothing checks the PRICE. A composite there survived
    /// into `Level.price` verbatim, and the sort reads it back as `Dec::ZERO`
    /// (`sorted_levels`' `unwrap_or`).
    ///
    /// Where that lands is NOT symmetric, and the benign-sounding half is the
    /// bid half. Bids sort descending, so a `Dec::ZERO` price goes to the far
    /// end and reads as a deep quote. Asks sort ASCENDING (`sorted_levels`'
    /// other branch), so the same bad level sorts to `asks[0]` — the BEST
    /// OFFER, the top of book the engine prices off.
    ///
    /// What actually saves us is downstream and is not the sort at all:
    /// `book.rs`'s `px` parses that same price back to `Dec::ZERO`, so
    /// `crossing()` sees bid >= ask and reports the book CROSSED, and every
    /// consumer filters crossed books out (`scan.rs`, `hedge.rs`,
    /// `quoter.rs`). The corrupt book is refused rather than priced. That is
    /// the safe outcome, but it is safety by a corruption check catching a
    /// poisoned top of book — not by the bad level being harmless.
    ///
    /// That accidental, ASYMMETRIC half-protection is exactly why this is fixed
    /// in `dec_string` and not at the `quantity` call site.
    #[test]
    fn a_book_level_of_an_unknown_shape_is_dropped_not_stringified() {
        let md = json!({"marketSlug": "s-1",
                        "bids": [{"px": {"value": {"amount": "0.6300"}}, "qty": "42.0000"},
                                 {"px": {"value": "0.6200"}, "qty": "17.0000"}],
                        "offers": [],
                        "transactTime": "2026-07-23T01:02:03.123456789Z"});
        let mut seq = SeqCounter::default();
        match &parse_ws_message(&json!({"marketData": md}), &mut seq)[0] {
            TapeEvent::Snapshot { bids, .. } => {
                assert_eq!(bids.len(), 1, "the composite-price level must be dropped");
                assert_eq!(bids[0].price, "0.6200");
            }
            _ => panic!(),
        }
    }
}
