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
            // No gap counter here, and that is a judgement rather than an
            // oversight: PM-US sends the venue's own cumulative `cumQuantity`,
            // so the next frame for an order carries the full total and a gap
            // heals itself — the asymmetry that makes `kalshi_fill_gaps()`
            // necessary on the other feed. It heals only if a next frame comes,
            // though: if the gap contains an order's LAST fill, nothing later
            // restates it and those contracts are as naked as Kalshi's. A gauge
            // that fires on every reconnect while being wrong about it almost
            // every time is the noise that gets a real alarm muted, so the
            // terminal case is left to arbbot-hedge.timer's venue-truth read
            // (see `kalshi_fill_gaps`), which covers both venues.
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

/// Windows in which a Kalshi fill may have happened unseen — **including the
/// boot window, which is counted as the first one, so 1 is the healthy floor
/// and 0 is only ever "the feed never started".**
///
/// Counting boot is the whole point. This is a per-process counter, and
/// `KalshiFills` is per-process state: a restart throws away `seen` and `cum`
/// while the venue's positions persist, so the downtime is a real gap and the
/// next run would otherwise open reporting a clean 0 with contracts naked from
/// the run before. That mistake is documented four gauges up in the same stats
/// block — `hedges_undischarged` "read 0 after the 01:34 restart on 2026-07-29
/// while a PM-US short was still real at the venue", which is why THAT gauge is
/// seeded from persisted state at startup (`orphan::undischarged`). This one is
/// not seeded, because seeding it would mean reading venue fill history, which
/// is the reconciliation this change deliberately does not build. Counting boot
/// is the honest substitute: it says "there was a window", not "nothing was
/// lost". The unit is `Restart=always`/`RestartSec=5`, so restarts are routine.
///
/// A gap is only an actual loss if Kalshi does not replay on resubscribe, which
/// is NOT established (see `KalshiFills`). No other gauge can see it either:
/// `fills_unattributed` counts frames that ARRIVED, `dropped_unconsumed` counts
/// obligations that were MINTED, and a frame that never came is neither.
///
/// RUNBOOK, because a bounded window is checkable and "reconcile by hand" is
/// not: each increment is stamped in the reconnect log below, and
/// `arbbot-hedge.timer` independently reads Kalshi venue truth every 5 minutes
/// (`scripts/hedge_naked_legs.py` GETs `/portfolio/positions` and keys on
/// `position_fp`), printing the exact shape this defect produces —
/// `[HEDGE] <rel> Kalshi-long naked imb +N — not auto-hedged (v1)`. Compare its
/// journal either side of the window to turn "possible loss" into a fact.
/// Caveats that make it a floor and not a proof: that timer iterates only
/// registry pairs while this channel subscribes account-wide, so a fill on an
/// unpaired market is invisible to it; the Kalshi-long direction is `print`ed,
/// not `Alerter`ed, so nothing pages; and it is frozen Python, so this is a
/// stopgap, not the long-term answer.
static KALSHI_FILL_GAPS: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_fill_gaps() -> u64 {
    KALSHI_FILL_GAPS.load(Ordering::Relaxed)
}

/// Kalshi fill frames whose count could not be read (see `kalshi_count`).
///
/// The gauge above is for the risk this change could not establish; this one is
/// for the risk it has live evidence of. If Kalshi renames `count_fp` the way it
/// already renamed `fill_count_fp` on the create response
/// (`arb-venue/tests/order_path.rs`), every Kalshi fill is skipped and every
/// other gauge reads healthy — `fills: 0`, `fills_unattributed: 0`,
/// `dropped_unconsumed: 0` — because a skipped frame mints nothing and claims
/// nothing. Without this, the only signal is a stderr line nothing parses.
/// Must stay 0.
static KALSHI_FILLS_UNREADABLE: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_fills_unreadable() -> u64 {
    KALSHI_FILLS_UNREADABLE.load(Ordering::Relaxed)
}

/// Fractional contracts filled but not yet reportable, summed across orders, in
/// HUNDREDTHS. A live total, not a counter: it falls as siblings complete.
///
/// `count_fp` is fractional (see `count_fp_hundredths`) and the engine hedges
/// whole contracts, so a `0.98` piece is banked until its `4.02` sibling makes
/// five. Usually that is the same instant. But an order can END fractional —
/// four in the live history do, one of them (`fee6b733`) having filled `0.41`
/// and nothing else — and then the dust sits here forever, a real if tiny
/// position nothing hedges.
///
/// It gets a gauge because the alternative is silence: a banked piece emits no
/// line, mints no obligation and moves no other counter. The parser it replaced
/// at least SHOUTED about these frames, by mistakenly calling them unreadable.
/// Losing the shout without gaining the number would have made a visible bug
/// invisible. Steady single digits are the normal resting state; hundreds mean
/// something is filling in dust and never completing.
static KALSHI_FILL_DUST: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

pub fn kalshi_fill_dust_hundredths() -> i64 {
    KALSHI_FILL_DUST.load(Ordering::Relaxed)
}

/// The contract count in a Kalshi fill payload, or the diagnostic to log.
///
/// Split out so the unreadable path is testable and, above all, SAYS something.
/// It read one spelling of one field and folded every other shape into
/// `unwrap_or(0)` -> silent `None`, which is what would have made a shape change
/// invisible rather than loud. A rename is not hypothetical for this field
/// family: Kalshi's create response ships `fill_count` where the docs say
/// `fill_count_fp` (pinned verbatim in `arb-venue/tests/order_path.rs`, a rename
/// that "broke two live smokes").
///
/// A rename is answered by failing LOUD, not by guessing: this returns `Err` for
/// an unknown spelling and the tests below pin that. The REST parser
/// (`arb-venue/src/resp.rs`) does fall back `fill_count_fp` -> `fill_count`, but
/// that is a FIELD-NAME fallback between two `Option<String>` fields — it is
/// precedent for renames, not for type tolerance, and inheriting a silent name
/// fallback here would recreate the class of bug this function exists to end.
///
/// Reading a bare JSON number is therefore speculative hardening, and labelled
/// as such. The live evidence says the wire form is a string: five Kalshi fill
/// lines in `data/trader-rs/m3-wal.jsonl` were produced by the PRE-FIX parser,
/// which read `count_fp` only via `.as_str()`, so those frames carried an
/// f64-parseable string. The precedent for tolerating both is the PM-US parser
/// at the top of this file, whose `cumQuantity` genuinely arrives both ways.
/// The cost of being wrong is asymmetric — a number read as 0 is an unhedged
/// fill — so it is worth a line.
fn kalshi_count(msg: &Value) -> Result<i64, String> {
    let raw = msg.get("count_fp");
    let n = raw.and_then(|x| match x {
        // The observed wire form. Parsed EXACTLY, never through f64 — see
        // `count_fp_hundredths`.
        Value::String(s) => count_fp_hundredths(s),
        // Speculative, as above, and the only lossy step in this function.
        // `{:.6}` rather than `{:.2}` because `{:.2}` ROUNDS: 2.999 would
        // become "3.00" and read as above what the venue filled, which is the
        // one direction this must never go. Six places then truncate to two.
        Value::Number(_) => x.as_f64().and_then(|f| count_fp_hundredths(&format!("{f:.6}"))),
        _ => None,
    });
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

/// `count_fp` in HUNDREDTHS of a contract, by exact string arithmetic.
///
/// THE COUNT IS NOT ALWAYS AN INTEGER, which this module assumed for as long as
/// it has existed. 22 of the 217 live fills in `data/venue/kalshi_fills.json`
/// are fractional, and they pair up inside one order: `2.13` and `1.87` on one
/// 4-lot, `0.98` and `4.02` on one 5-lot. Kalshi splits a fill across price
/// levels and the pieces sum to the order's size.
///
/// The old parser read the field through `as f64` and then `as i64`, which
/// TRUNCATES each piece independently: 2.13 -> 2 and 1.87 -> 1 is three
/// contracts on an order the venue filled four. Worse, a piece below 1.00
/// truncated to 0, hit the non-positive arm above, and was SKIPPED — bumping
/// `kalshi_fills_unreadable`, the gauge documented "must stay 0", while its
/// contracts went to the venue unhedged. Replaying the whole live history
/// through it loses **11.27 contracts** of which 9 are whole and recoverable
/// (the remaining 2.27 is sub-contract dust this parser banks but still cannot
/// hedge), and skips 5 frames. None of that needs a gap or a reconnect: it is a
/// fill the engine is simply never told about, on ~6.6% of orders.
///
/// Accumulating in hundredths and flooring only at the point of emission makes
/// the arithmetic the venue's: 213 + 187 = 400 is 4 contracts, exactly. All 217
/// rows carry exactly two decimals, so this is lossless on every value the
/// venue has been observed to send. A third decimal is TRUNCATED rather than
/// rounded, which keeps the invariant that matters: the running total can never
/// exceed what the venue actually filled, so `observe_cum_fill` — monotone, and
/// clamped to the resting size — can never be made to mint a contract that does
/// not exist.
fn count_fp_hundredths(raw: &str) -> Option<i64> {
    let s = raw.trim();
    let s = s.strip_prefix('+').unwrap_or(s);
    if s.starts_with('-') {
        return None; // a negative fill is not a fill
    }
    let (whole, frac) = s.split_once('.').unwrap_or((s, ""));
    if whole.is_empty() && frac.is_empty() {
        return None;
    }
    if !whole.bytes().all(|b| b.is_ascii_digit()) || !frac.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let w: i64 = if whole.is_empty() { 0 } else { whole.parse().ok()? };
    let mut cents = 0i64;
    for (i, b) in frac.bytes().take(2).enumerate() {
        cents += i64::from(b - b'0') * if i == 0 { 10 } else { 1 };
    }
    w.checked_mul(100)?.checked_add(cents)
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
/// private WS is PM-US), and `docs/venue-quirks.md` has no entry for it. The one
/// adjacent behavior that IS documented points the other way:
/// `kalshi-ws-snapshots-only-on-subscribe` records that Kalshi's WS does not
/// re-send state after a gap, and its port requirement is "do not rely on the
/// venue re-sending snapshots". So the dedupe defends against a replay, and
/// NOTHING defends against a loss: if Kalshi does not replay, a gap
/// under-reports this total permanently and the missing contracts are naked.
///
/// What live evidence there is does not reach the question. `data/trader-rs/
/// m3-wal.jsonl` holds five Kalshi fill lines, but they are this parser's
/// OUTPUT, not raw venue frames — they pin that `count_fp` arrived as an
/// f64-parseable string (the pre-fix parser could read nothing else) and no more
/// than that. All five are `cum: 5` on five distinct order ids: one frame per
/// order, so they exercise neither multi-frame accumulation nor a resubscribe.
/// The raw frame shape is still unpinned by any capture, and the fixtures below
/// are hand-written. Settling replay needs a live probe — drop the socket
/// mid-fill, resubscribe, see what arrives.
#[derive(Default)]
pub struct KalshiFills {
    seen: std::collections::HashSet<String>,
    /// order id -> contracts x100. HUNDREDTHS, because `count_fp` is fractional
    /// on about 10% of live fills and truncating each piece loses contracts —
    /// see `count_fp_hundredths`. Floored to whole contracts once, on emission.
    hundredths: std::collections::HashMap<String, i64>,
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
                KALSHI_FILLS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
                eprintln!("[fills] kalshi fill {trade_id} on order {order_id}: {why}");
                return None;
            }
        };
        if !self.seen.insert(trade_id.to_string()) {
            return None; // already counted — never hedge a fill twice
        }
        let entry = self.hundredths.entry(order_id.to_string()).or_insert(0);
        let before = *entry;
        *entry += n;
        let (cum, prev) = (*entry / 100, before / 100);
        // Track the fraction that is banked but not yet hedgeable.
        KALSHI_FILL_DUST.fetch_add((*entry % 100) - (before % 100), Ordering::Relaxed);
        if cum == prev {
            // A sub-contract piece. COUNTED — its trade_id is spent and its
            // hundredths are banked — but there is no whole contract to hedge
            // yet, and reporting an unchanged cumulative total would be a
            // no-op at the ledger anyway.
            return None;
        }
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
    //
    // It does NOT live across restarts, and this is where that is counted. The
    // state below starts empty while the venue's positions do not, so the
    // downtime before this line is a gap of exactly the same kind as a
    // reconnect, and the largest one: it is the whole window in which no
    // process was subscribed. Counting it here is what stops the next run from
    // opening with a clean 0 over contracts the last run left naked.
    KALSHI_FILL_GAPS.fetch_add(1, Ordering::Relaxed);
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
                     short of the venue and those contracts are naked. Check the \
                     arbbot-hedge.timer journal either side of this line for a Kalshi-long \
                     naked imbalance (see kalshi_fill_gaps). kalshi_fill_gaps={}",
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
        // Takes the turn too: a zero count is a `type: fill` frame that reaches
        // `kalshi_count`'s Err arm, so it BUMPS `KALSHI_FILLS_UNREADABLE` on its
        // way to returning None. Without the lock this raced the two tests that
        // assert an exact delta on that counter and failed roughly one run in
        // six — a gate that is red at random is worse than one that is slow.
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        assert!(s.line(&frame("t1", "o1", "0.00")).is_none());
    }

    /// The gauges in this module are process-global and libtest runs these
    /// threads in parallel, so every test that asserts a DELTA on one takes
    /// turns — and so does every test that MOVES one, even transiently.
    /// `KALSHI_FILL_DUST` made that second half matter: a fractional fill
    /// raises it and its sibling lowers it again, so a test that only nets to
    /// zero still perturbs a concurrent reader mid-flight.
    static COUNTER: std::sync::Mutex<()> = std::sync::Mutex::new(());

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
        let _g = COUNTER.lock();
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

    /// The restart hole, which is why boot counts as gap #1.
    ///
    /// A RESTART is the largest gap there is, and the state that makes the
    /// running total meaningful does not survive it: the new process starts at
    /// zero for an order the venue has already filled 4 on, so the next delta
    /// mints an obligation for 3 against a venue total of 7. Constructing the
    /// second `KalshiFills` is the point — it is the restart, and it is what
    /// distinguishes this from `deltas_accumulate_into_a_cumulative_count`
    /// above, which asserts the same arithmetic within one instance.
    ///
    /// What this does NOT do is guard the limitation against a future fix. A
    /// reconstruction (seed `cum` from Kalshi's fill history) would add a
    /// constructor and leave `default()` behaving exactly like this, so this
    /// test would stay green through it. The `KalshiFills` doc comment is what
    /// carries that argument; a test cannot.
    #[test]
    fn a_restart_starts_the_running_total_over_at_zero() {
        let mut before = KalshiFills::default();
        let a: Value =
            serde_json::from_str(&before.line(&frame("t1", "o1", "4.00")).unwrap()).unwrap();
        assert_eq!(a["cum"], 4);

        // ...the process restarts. `seen` and `cum` are gone; o1 is not.
        let mut after = KalshiFills::default();
        let b: Value =
            serde_json::from_str(&after.line(&frame("t2", "o1", "3.00")).unwrap()).unwrap();
        assert_eq!(b["cum"], 3, "not 7 — the venue filled 7 and the ledger is told 3");
    }

    /// A rename of `count_fp` skips every Kalshi fill while `fills`,
    /// `fills_unattributed` and `dropped_unconsumed` all read healthy, because a
    /// skipped frame mints nothing and claims nothing. This counter is the only
    /// gauge that moves, so it is the only way that failure is visible to
    /// anything that is not a human reading stderr.
    #[test]
    fn an_unreadable_count_is_counted_not_just_logged() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let before = kalshi_fills_unreadable();
        assert!(s.line(&countless("t1", "o1")).is_none());
        assert_eq!(kalshi_fills_unreadable(), before + 1, "the skip must be countable");
        assert!(s.line(&frame("t2", "o1", "2.00")).is_some());
        assert_eq!(kalshi_fills_unreadable(), before + 1, "a good frame must not count");
    }

    // --------------------------------------------- fractional count_fp (K7) ---

    /// `count_fp` IS FRACTIONAL — 22 of the 217 live fills in
    /// `data/venue/kalshi_fills.json`, and they pair up inside one order. This
    /// exact pair is one 4-lot (`d538e727`, KXBRPRES-26-FBOL, 2026-07-24).
    ///
    /// The old parser read the field through `as f64 as i64`, truncating each
    /// piece on its own: 2.13 -> 2, 1.87 -> 1, three contracts on an order the
    /// venue filled four. Nothing about this needs a gap or a reconnect — it is
    /// a contract the engine is simply never told about.
    #[test]
    fn a_fractional_fill_pair_sums_to_the_venues_whole_contract() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let a: Value = serde_json::from_str(&s.line(&frame("t1", "o1", "2.13")).unwrap()).unwrap();
        assert_eq!(a["cum"], 2, "2.13 filled is 2 WHOLE contracts to hedge; the 0.13 is banked");
        let b: Value = serde_json::from_str(&s.line(&frame("t2", "o1", "1.87")).unwrap()).unwrap();
        assert_eq!(b["cum"], 4, "213 + 187 = 400, which is 4 contracts — the old parser said 3");
    }

    /// The other half of the same defect: a piece below 1.00 truncated to 0,
    /// reached `kalshi_count`'s non-positive arm and was SKIPPED — bumping
    /// `kalshi_fills_unreadable`, the gauge documented "must stay 0", while its
    /// contracts went unhedged. Five of the 217 live fills are this shape.
    /// `0.98` + `4.02` is one 5-lot (`297ddcd1`, KXPRESNOMD-28-GN).
    #[test]
    fn a_sub_contract_piece_is_banked_not_called_unreadable() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let before = kalshi_fills_unreadable();
        assert!(s.line(&frame("t1", "o1", "0.98")).is_none(), "no whole contract yet");
        assert_eq!(kalshi_fills_unreadable(), before, "0.98 is READABLE — not a shape change");
        let v: Value = serde_json::from_str(&s.line(&frame("t2", "o1", "4.02")).unwrap()).unwrap();
        assert_eq!(v["cum"], 5, "98 + 402 = 500");
    }

    /// Order of arrival must not change the total. `0.01` then `4.99` reports 0
    /// then 5; `4.99` then `0.01` reports 4 then 5. Monotone either way, and
    /// never above the venue — both pairs are real, from KXPRESNOMD-28-GN.
    #[test]
    fn fractional_pieces_floor_the_same_total_in_either_order() {
        let _g = COUNTER.lock();
        let mut a = KalshiFills::default();
        assert!(a.line(&frame("t1", "o1", "0.01")).is_none());
        let v: Value = serde_json::from_str(&a.line(&frame("t2", "o1", "4.99")).unwrap()).unwrap();
        assert_eq!(v["cum"], 5);

        let mut b = KalshiFills::default();
        let x: Value = serde_json::from_str(&b.line(&frame("t1", "o1", "4.99")).unwrap()).unwrap();
        assert_eq!(x["cum"], 4, "4.99 filled is 4 whole contracts, never 5");
        let y: Value = serde_json::from_str(&b.line(&frame("t2", "o1", "0.01")).unwrap()).unwrap();
        assert_eq!(y["cum"], 5);
    }

    /// A banked piece emits no line, mints no obligation and moves no other
    /// gauge, so without this one it is invisible — and an order CAN end
    /// fractional: `fee6b733` in the live history filled 0.41 and nothing else.
    /// The parser this replaced at least shouted about those frames, by
    /// mistakenly calling them unreadable.
    #[test]
    fn banked_dust_is_visible_and_falls_when_its_sibling_lands() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let before = kalshi_fill_dust_hundredths();
        assert!(s.line(&frame("t1", "o1", "0.41")).is_none());
        assert_eq!(kalshi_fill_dust_hundredths(), before + 41, "0.41 is banked and SAID");
        assert!(s.line(&frame("t2", "o1", "0.59")).is_some());
        assert_eq!(kalshi_fill_dust_hundredths(), before, "41 + 59 = 100: a whole contract, no dust");
    }

    /// Exact string arithmetic, never f64: `2.13` as a float is
    /// 2.1299999999999998, and `* 100.0` truncated is 212 — a hundredth short,
    /// which is a whole contract short once 47 of them accumulate.
    #[test]
    fn count_fp_is_parsed_as_exact_hundredths() {
        assert_eq!(count_fp_hundredths("2.13"), Some(213));
        assert_eq!(count_fp_hundredths("1.87"), Some(187));
        assert_eq!(count_fp_hundredths("25.00"), Some(2500));
        assert_eq!(count_fp_hundredths("0.01"), Some(1));
        assert_eq!(count_fp_hundredths("3"), Some(300), "no decimal point at all");
        assert_eq!(count_fp_hundredths("2.1"), Some(210), "one decimal is tenths, not hundredths");
        // A third decimal TRUNCATES, never rounds up: the running total must
        // stay a floor on what the venue actually filled.
        assert_eq!(count_fp_hundredths("2.139"), Some(213));
        assert_eq!(count_fp_hundredths("2.999"), Some(299));
        assert_eq!(count_fp_hundredths("-1.00"), None, "a negative fill is not a fill");
        assert_eq!(count_fp_hundredths("2.0e1"), None);
        assert_eq!(count_fp_hundredths(""), None);
    }

    /// ...including on the numeric branch, which has to go through a format
    /// string. `{:.2}` would ROUND — 2.999 to "3.00" — reporting a contract the
    /// venue did not fill, which is the one direction the floor invariant
    /// forbids.
    #[test]
    fn a_numeric_count_fp_floors_rather_than_rounds() {
        let v: Value = serde_json::from_str(r#"{"count_fp":2.999}"#).unwrap();
        assert_eq!(kalshi_count(&v), Ok(299), "2.999 is 2 contracts and change, not 3");
    }
}
