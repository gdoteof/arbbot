//! Live own-order fill feed (P4 item 1).
//!
//! PM-US pushes an execution the instant a resting quote fills — sub-second,
//! versus the ~2s REST poll it replaces. The task translates venue frames into
//! the engine's `fill` events and writes them to the SAME ordered channel as
//! book events, so a fill is sequenced, WAL'd and replayed like everything
//! else. It holds credentials but no order code path: it can only read.
//!
//! `cum` is the venue's CUMULATIVE filled count, never a delta. That is what
//! makes this feed and a poll idempotent against each other — both can report
//! the same fill and `arb_core::fill` mints the hedge obligation once.

use crate::feed::FeedMsg;
use futures_util::{SinkExt, StreamExt};
use serde_json::Value;
use std::time::Instant;
use tokio::sync::mpsc::Sender;

const PMUS_WS: &str = "wss://api.polymarket.us/v1/ws/private";
const PMUS_WS_PATH: &str = "/v1/ws/private";

fn now_ns() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0)
}

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
                .to_string()
                .into(),
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
