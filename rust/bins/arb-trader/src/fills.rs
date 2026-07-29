//! Live own-order fill feed (P4 item 1).
//!
//! PM-US pushes an execution the instant a resting quote fills — sub-second,
//! versus the ~2s REST poll it replaces. The task translates venue frames into
//! the engine's `fill` events and writes them to the SAME ordered channel as
//! book events, so a fill is sequenced, WAL'd and replayed like everything
//! else. It holds credentials but no order code path: it can only read.
//!
//! The `cum` this module emits is CUMULATIVE per order, which is what lets
//! `arb_core::fill` mint the hedge obligation exactly once for a fill reported
//! twice. WHERE that number comes from differs by venue, and the difference is
//! load-bearing: PM-US sends the venue's own cumulative `cumQuantity`, so its
//! `cum` is venue truth. Kalshi sends per-fill DELTAS, so `KalshiFills` sums
//! them locally — its `cum` is what this process received, not what the venue
//! filled. The two agree only while no frame is lost. See `KalshiFills` for why
//! that is an open exposure rather than a solved one, and `kalshi_fill_gaps()`
//! for the signal that it may have happened.

use crate::feed::FeedMsg;
use arb_core::clock::now_ns;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
use tokio::sync::mpsc::Sender;

const PMUS_WS: &str = "wss://api.polymarket.us/v1/ws/private";
const PMUS_WS_PATH: &str = "/v1/ws/private";

/// Translate one PM-US private frame into a `fill` line, or `None` if it is not
/// a fill we can act on.
///
/// Only FILL / PARTIAL_FILL count. Other execution types (acks, cancels,
/// rejects) arrive on the same subscription and must not be mistaken for fills.
pub fn pmus_fill_line(raw: &str) -> Option<String> {
    let msg: Value = serde_json::from_str(raw).ok()?;
    let ex = msg.get("orderSubscriptionUpdate")?.get("execution")?;
    let kind = ex.get("type").and_then(|t| t.as_str())?;
    if kind != "EXECUTION_TYPE_FILL" && kind != "EXECUTION_TYPE_PARTIAL_FILL" {
        return None;
    }
    let order = ex.get("order")?;
    let oid = order.get("id").and_then(|x| x.as_str())?;
    // cumQuantity arrives as a number or a string depending on the frame.
    let cum = order
        .get("cumQuantity")
        .and_then(|q| q.as_i64().or_else(|| q.as_str().and_then(|s| s.parse().ok())))
        .unwrap_or(0);
    if oid.is_empty() || cum < 1 {
        return None;
    }
    let market = order
        .get("marketSlug")
        .and_then(|x| x.as_str())
        .unwrap_or_default();
    Some(
        serde_json::json!({
            "kind": "fill",
            "venue": "polymarket_us",
            "market_id": market,
            "order_id": oid,
            "cum": cum,
            "ts_local_ns": now_ns(),
        })
        .to_string(),
    )
}

/// Connect, subscribe to the ORDER channel, and forward fills forever.
/// Reconnects on any drop — a gap here is a fill we never hedged.
pub async fn pmus_fill_feed(key_id: String, secret_b64: String, tx: Sender<FeedMsg>) {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    loop {
        let attempt = async {
            let signer = arb_venue::PmusSigner::from_secret_b64(key_id.clone(), &secret_b64)
                .map_err(|e| format!("signer: {e}"))?;
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let mut req = PMUS_WS.into_client_request().map_err(|e| e.to_string())?;
            // Same ed25519 scheme as REST: ts + GET + path, on the handshake.
            for (k, v) in signer.headers(&ts, "GET", PMUS_WS_PATH) {
                req.headers_mut().insert(
                    k,
                    v.parse().map_err(|_| format!("bad header {k}"))?,
                );
            }
            let (mut ws, _) = tokio_tungstenite::connect_async(req)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            ws.send(Message::Text(
                serde_json::json!({"subscribe": {
                    "requestId": "orders",
                    "subscriptionType": "SUBSCRIPTION_TYPE_ORDER"}})
                .to_string(),
            ))
            .await
            .map_err(|e| format!("subscribe: {e}"))?;
            eprintln!("[fills] polymarket_us private WS connected");

            while let Some(frame) = ws.next().await {
                let msg = frame.map_err(|e| format!("read: {e}"))?;
                let Message::Text(raw) = msg else { continue };
                if let Some(line) = pmus_fill_line(&raw) {
                    eprintln!("[fills] {line}");
                    if tx.send(FeedMsg { line, t_read: Instant::now() }).await.is_err() {
                        return Ok::<(), String>(()); // engine gone
                    }
                }
            }
            Err("stream ended".to_string())
        };

        match attempt.await {
            Ok(()) => return,
            Err(e) => eprintln!("[fills] polymarket_us dropped ({e}); reconnecting in 2s"),
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(kind: &str, cum: &str) -> String {
        format!(
            r#"{{"orderSubscriptionUpdate":{{"execution":{{"type":"{kind}",
               "order":{{"id":"BGT1","marketSlug":"will-x","cumQuantity":{cum}}}}}}}}}"#
        )
    }

    #[test]
    fn a_fill_becomes_a_fill_event() {
        let line = pmus_fill_line(&frame("EXECUTION_TYPE_FILL", "3")).expect("a fill");
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["kind"], "fill");
        assert_eq!(v["venue"], "polymarket_us");
        assert_eq!(v["order_id"], "BGT1", "the VENUE's id — the engine maps it");
        assert_eq!(v["market_id"], "will-x");
        assert_eq!(v["cum"], 3);
    }

    #[test]
    fn a_partial_fill_counts_too() {
        assert!(pmus_fill_line(&frame("EXECUTION_TYPE_PARTIAL_FILL", "1")).is_some());
    }

    /// Acks, cancels and rejects share this subscription. Treating one as a
    /// fill would mint a hedge for an order that never traded.
    #[test]
    fn non_fill_executions_are_ignored() {
        for kind in [
            "EXECUTION_TYPE_NEW",
            "EXECUTION_TYPE_CANCELED",
            "EXECUTION_TYPE_REJECTED",
            "EXECUTION_TYPE_UNSPECIFIED",
        ] {
            assert!(pmus_fill_line(&frame(kind, "3")).is_none(), "{kind} is not a fill");
        }
    }

    /// cumQuantity is a number in some frames and a string in others.
    #[test]
    fn cum_quantity_is_read_either_way() {
        let n = pmus_fill_line(&frame("EXECUTION_TYPE_FILL", "2")).unwrap();
        let s = pmus_fill_line(&frame("EXECUTION_TYPE_FILL", "\"2\"")).unwrap();
        let (nv, sv): (Value, Value) =
            (serde_json::from_str(&n).unwrap(), serde_json::from_str(&s).unwrap());
        assert_eq!(nv["cum"], 2);
        assert_eq!(sv["cum"], 2);
    }

    /// A zero-fill frame is not a fill — emitting it would register a hedge
    /// obligation for nothing.
    #[test]
    fn a_zero_cum_is_not_a_fill() {
        assert!(pmus_fill_line(&frame("EXECUTION_TYPE_FILL", "0")).is_none());
    }

    #[test]
    fn unrelated_frames_are_ignored() {
        for raw in [
            "{}",
            r#"{"heartbeat":1}"#,
            r#"{"orderSubscriptionUpdate":{}}"#,
            "not json",
        ] {
            assert!(pmus_fill_line(raw).is_none(), "{raw}");
        }
    }
}

// ------------------------------------------------------------------ Kalshi ---

const KALSHI_WS: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
const KALSHI_WS_PATH: &str = "/trade-api/ws/v2";

/// Kalshi fill-WS gaps: every reconnect is a window in which a fill may have
/// happened. Whether the missed frames come back on resubscribe is NOT
/// established (see `KalshiFills`), and the running totals there are a local
/// sum, so a gap may have left them short of the venue with nothing else to say
/// so — `fills_unattributed` counts frames that ARRIVED and `dropped_unconsumed`
/// counts obligations that were MINTED, and a lost frame is neither. Non-zero
/// here means reconcile against Kalshi's fill history by hand.
static KALSHI_FILL_GAPS: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_fill_gaps() -> u64 {
    KALSHI_FILL_GAPS.load(Ordering::Relaxed)
}

/// The contract count in a Kalshi fill payload, or the diagnostic to log.
///
/// Split out so the unreadable path is testable and, above all, SAYS
/// something. It read one spelling of one field and folded every other shape
/// into `unwrap_or(0)` -> silent `None`, which is why a shape change here would
/// be invisible rather than loud. That is not hypothetical for this field
/// family: Kalshi's create response ships `fill_count` where the docs say
/// `fill_count_fp` (pinned live in `arb-venue/tests/order_path.rs`, a rename
/// that "broke two live smokes"), and the PM-US parser above already reads its
/// own quantity as number-or-string. Read both here too.
fn kalshi_count(msg: &Value) -> Result<i64, String> {
    let raw = msg.get("count_fp");
    // Fixed-point, but plain: "2.00" is 2 contracts, not scaled
    // (`kalshi-fill-count-fp-plain-count`).
    let n = raw
        .and_then(|x| x.as_f64().or_else(|| x.as_str().and_then(|s| s.parse::<f64>().ok())))
        .map(|f| f as i64);
    match n {
        Some(n) if n > 0 => Ok(n),
        _ => Err(format!(
            "count_fp is unreadable or non-positive ({}) — NOT counted, and the trade_id is \
             deliberately NOT consumed, so a corrected or replayed frame for this trade can \
             still count. A persistent one is a payload shape change, and every fill behind \
             it is unhedged.",
            raw.map(|v| v.to_string()).unwrap_or_else(|| "field absent".into())
        )),
    }
}

/// Kalshi's `fill` channel differs from PM-US's in two ways that matter:
///
///  1. `count_fp` is a per-fill DELTA, not a cumulative total. The engine's
///     contract is CUMULATIVE, so this accumulates.
///  2. The payload carries no `client_order_id`, only Kalshi's own `order_id` —
///     so the engine's venue-id mapping is load-bearing here, not optional.
///
/// Accumulating is only safe if it is idempotent, which is what `trade_id` is
/// for: a duplicate frame, or a resubscribe that replays, must not hedge the
/// same fill twice. The key is claimed only once a frame has yielded a usable
/// count — a frame we could not READ must stay re-countable, because burning
/// its id makes the corrected or replayed frame for that trade look like one we
/// already hedged, and those contracts then mint no obligation at all.
///
/// UNRESOLVED, and the reason `kalshi_fill_gaps()` exists: the running total is
/// what THIS PROCESS received, not what the venue filled. Whether Kalshi replays
/// the fills missed during a WS gap on resubscribe was never established — the
/// commit that introduced this channel asserted it in prose, and nothing in the
/// repo tests or records it: Python never subscribed to this channel (its only
/// private WS is PM-US), `docs/venue-quirks.md` has no entry for it, and there
/// is no captured live frame — the test fixtures below are hand-written. The
/// one adjacent behavior that IS documented points the other way:
/// `kalshi-ws-snapshots-only-on-subscribe` records that Kalshi's WS does not
/// re-send state after a gap, and its port requirement is "do not rely on the
/// venue re-sending snapshots". So the dedupe defends against a replay, and
/// NOTHING defends against a loss: if Kalshi does not replay, a gap
/// under-reports this total permanently and the missing contracts are naked.
#[derive(Default)]
pub struct KalshiFills {
    seen: std::collections::HashSet<String>,
    cum: std::collections::HashMap<String, i64>,
}

impl KalshiFills {
    pub fn line(&mut self, raw: &str) -> Option<String> {
        let v: Value = serde_json::from_str(raw).ok()?;
        if v.get("type").and_then(|t| t.as_str())? != "fill" {
            return None;
        }
        let msg = v.get("msg")?;
        let order_id = msg.get("order_id").and_then(|x| x.as_str())?;
        let trade_id = msg.get("trade_id").and_then(|x| x.as_str())?;
        // VALIDATE, then claim the dedupe key. The other order loses a fill:
        // an unreadable count returned `None` while `trade_id` was already
        // spent, so the replay that would have fixed it was discarded as
        // "already counted" and those contracts never reached the ledger.
        let n = match kalshi_count(msg) {
            Ok(n) => n,
            Err(why) => {
                eprintln!("[fills] kalshi fill {trade_id} on order {order_id}: {why}");
                return None;
            }
        };
        if !self.seen.insert(trade_id.to_string()) {
            return None; // already counted — never hedge a fill twice
        }
        let entry = self.cum.entry(order_id.to_string()).or_insert(0);
        *entry += n;
        let cum = *entry;
        let market = msg.get("market_ticker").and_then(|x| x.as_str()).unwrap_or_default();
        Some(
            serde_json::json!({
                "kind": "fill",
                "venue": "kalshi",
                "market_id": market,
                "order_id": order_id,
                "cum": cum,
                "ts_local_ns": now_ns(),
            })
            .to_string(),
        )
    }
}

pub async fn kalshi_fill_feed(key_id: String, pem: String, tx: Sender<FeedMsg>) {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    // State lives ACROSS reconnects: IF Kalshi replays, the dedupe is what stops
    // that becoming a double hedge. It does nothing for the other direction —
    // see `KalshiFills` and `kalshi_fill_gaps()`.
    let mut state = KalshiFills::default();

    loop {
        let attempt = async {
            let signer = arb_venue::KalshiSigner::from_pkcs8_pem(key_id.clone(), &pem)
                .map_err(|e| format!("signer: {e}"))?;
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis().to_string())
                .unwrap_or_default();
            let mut req = KALSHI_WS.into_client_request().map_err(|e| e.to_string())?;
            for (k, v) in signer.headers(&ts, "GET", KALSHI_WS_PATH) {
                req.headers_mut()
                    .insert(k, v.parse().map_err(|_| format!("bad header {k}"))?);
            }
            let (mut ws, _) = tokio_tungstenite::connect_async(req)
                .await
                .map_err(|e| format!("connect: {e}"))?;
            // No market filter: we want every fill on this account, including
            // one on an order this process did not place.
            ws.send(Message::Text(
                serde_json::json!({"id": 1, "cmd": "subscribe",
                                   "params": {"channels": ["fill"]}})
                .to_string(),
            ))
            .await
            .map_err(|e| format!("subscribe: {e}"))?;
            eprintln!("[fills] kalshi fill channel connected");

            while let Some(frame) = ws.next().await {
                let msg = frame.map_err(|e| format!("read: {e}"))?;
                let Message::Text(raw) = msg else { continue };
                if let Some(line) = state.line(&raw) {
                    eprintln!("[fills] {line}");
                    if tx.send(FeedMsg { line, t_read: Instant::now() }).await.is_err() {
                        return Ok::<(), String>(());
                    }
                }
            }
            Err("stream ended".to_string())
        };

        match attempt.await {
            Ok(()) => return,
            Err(e) => {
                KALSHI_FILL_GAPS.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[fills] kalshi dropped ({e}); reconnecting in 2s. Any fill inside this \
                     gap is recovered ONLY if Kalshi replays on resubscribe, which this repo \
                     has never established — if it does not, the running fill totals are \
                     short of the venue and those contracts are naked. kalshi_fill_gaps={}",
                    kalshi_fill_gaps()
                );
            }
        }
        if tx.is_closed() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    }
}

#[cfg(test)]
mod kalshi_tests {
    use super::*;

    fn frame(trade: &str, order: &str, count: &str) -> String {
        format!(
            r#"{{"type":"fill","sid":1,"msg":{{"trade_id":"{trade}","order_id":"{order}",
               "market_ticker":"KXTEST","is_taker":false,"side":"yes","action":"buy",
               "count_fp":"{count}","yes_price_dollars":"0.42"}}}}"#
        )
    }

    /// Kalshi sends DELTAS; the engine's contract is cumulative.
    #[test]
    fn deltas_accumulate_into_a_cumulative_count() {
        let mut s = KalshiFills::default();
        let a: Value = serde_json::from_str(&s.line(&frame("t1", "o1", "2.00")).unwrap()).unwrap();
        let b: Value = serde_json::from_str(&s.line(&frame("t2", "o1", "3.00")).unwrap()).unwrap();
        assert_eq!(a["cum"], 2, "first fill");
        assert_eq!(b["cum"], 5, "2 + 3, not 3");
        assert_eq!(b["venue"], "kalshi");
        assert_eq!(b["order_id"], "o1", "Kalshi's id — no client_order_id in this payload");
        assert_eq!(b["market_id"], "KXTEST");
    }

    /// THE reason accumulation is safe: a reconnect replays fills, and hedging
    /// the same trade twice would open a naked leg.
    #[test]
    fn a_replayed_trade_id_is_ignored() {
        let mut s = KalshiFills::default();
        assert!(s.line(&frame("t1", "o1", "2.00")).is_some());
        assert!(s.line(&frame("t1", "o1", "2.00")).is_none(), "same trade_id twice");
        let c: Value = serde_json::from_str(&s.line(&frame("t2", "o1", "1.00")).unwrap()).unwrap();
        assert_eq!(c["cum"], 3, "the duplicate must not have counted");
    }

    /// Orders accumulate independently.
    #[test]
    fn each_order_has_its_own_running_total() {
        let mut s = KalshiFills::default();
        s.line(&frame("t1", "o1", "4.00"));
        let b: Value = serde_json::from_str(&s.line(&frame("t2", "o2", "1.00")).unwrap()).unwrap();
        assert_eq!(b["cum"], 1, "o2 starts at its own zero");
    }

    #[test]
    fn non_fill_frames_are_ignored() {
        let mut s = KalshiFills::default();
        for raw in [
            r#"{"type":"subscribed","sid":1}"#,
            r#"{"type":"orderbook_delta","msg":{}}"#,
            r#"{"type":"error","msg":{"code":6}}"#,
            "{}",
            "not json",
        ] {
            assert!(s.line(raw).is_none(), "{raw}");
        }
    }

    #[test]
    fn a_zero_count_is_not_a_fill() {
        let mut s = KalshiFills::default();
        assert!(s.line(&frame("t1", "o1", "0.00")).is_none());
    }

    /// A frame with no readable count, otherwise well-formed.
    fn countless(trade: &str, order: &str) -> String {
        format!(
            r#"{{"type":"fill","sid":1,"msg":{{"trade_id":"{trade}","order_id":"{order}",
               "market_ticker":"KXTEST","is_taker":false,"side":"yes","action":"buy",
               "fill_count":"2.00","yes_price_dollars":"0.42"}}}}"#
        )
    }

    /// The dedupe key used to be claimed BEFORE the payload was validated, so a
    /// frame whose count we could not read spent its `trade_id` on the way to a
    /// silent `None`. The corrected or replayed frame for that same trade then
    /// read as "already counted" and was dropped: no obligation, no hedge, no
    /// counter anywhere. A frame we failed to read must stay re-countable.
    #[test]
    fn an_unreadable_count_does_not_burn_the_trade_id() {
        let mut s = KalshiFills::default();
        assert!(s.line(&countless("t1", "o1")).is_none(), "no readable count => not a fill");
        let v: Value = serde_json::from_str(
            &s.line(&frame("t1", "o1", "2.00")).expect("t1 must still be countable"),
        )
        .unwrap();
        assert_eq!(v["cum"], 2, "the contracts reach the ledger on the second try");
    }

    /// ...and it must SAY so. A silent skip is what makes a payload shape change
    /// invisible: the whole point of the diagnostic is that the shape which
    /// broke it is in the log rather than guessed at.
    #[test]
    fn an_unreadable_count_reports_why() {
        let absent: Value = serde_json::from_str(r#"{"order_id":"o1"}"#).unwrap();
        let why = kalshi_count(&absent).expect_err("an absent count is not a count");
        assert!(why.contains("count_fp"), "name the field: {why}");
        assert!(why.contains("field absent"), "and the shape seen: {why}");

        // The exact rename Kalshi already shipped on this field family
        // (`fill_count` for `fill_count_fp`) must be loud, not zero.
        let renamed: Value = serde_json::from_str(r#"{"fill_count":"2.00"}"#).unwrap();
        assert!(kalshi_count(&renamed).is_err(), "a renamed field is unreadable, not empty");

        let zero: Value = serde_json::from_str(r#"{"count_fp":"0.00"}"#).unwrap();
        assert!(kalshi_count(&zero).is_err(), "non-positive reports too");
    }

    /// `count_fp` is a string in every frame this repo has ever written down —
    /// but none of those were captured from the venue, and the PM-US parser
    /// already reads its own quantity either way. A number must not read as 0.
    #[test]
    fn a_numeric_count_fp_is_read_like_the_string_one() {
        let mut s = KalshiFills::default();
        let numeric = r#"{"type":"fill","sid":1,"msg":{"trade_id":"t1","order_id":"o1",
                          "market_ticker":"KXTEST","count_fp":2}}"#;
        let a: Value =
            serde_json::from_str(&s.line(numeric).expect("a numeric count is a count")).unwrap();
        assert_eq!(a["cum"], 2);
        let b: Value = serde_json::from_str(&s.line(&frame("t2", "o1", "3.00")).unwrap()).unwrap();
        assert_eq!(b["cum"], 5, "both spellings accumulate into the same total");
    }

    /// KNOWN LIMITATION, pinned rather than fixed (see `KalshiFills`): `cum` is
    /// what this process received, not what the venue filled. A frame lost in a
    /// WS gap is under-reported forever — the next frame resumes from the short
    /// total, so the obligation minted is short by the missing contracts and
    /// they are naked. Nothing in this type can tell; only `kalshi_fill_gaps()`
    /// says a gap happened. Change this test only with venue evidence about
    /// replay-on-resubscribe, not with a reconstruction argument.
    #[test]
    fn a_frame_lost_in_a_gap_is_under_reported_forever() {
        let mut s = KalshiFills::default();
        s.line(&frame("t1", "o1", "4.00")).expect("4 filled and delivered");
        // ...WS drops. t2 fills 3 more during the gap and is never delivered.
        let c: Value = serde_json::from_str(&s.line(&frame("t3", "o1", "3.00")).unwrap()).unwrap();
        assert_eq!(c["cum"], 7, "7 received; the venue filled 10, and nothing here knows");
    }
}
