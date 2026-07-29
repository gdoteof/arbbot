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
//! them locally — and a frame lost while the socket was dark would leave that
//! sum permanently short of the venue, with the contracts filled and unhedged.
//!
//! So the Kalshi sum is RECONCILED: on every reconnect the venue's own fill
//! history for the window is read back through `GET /portfolio/fills` and
//! MERGED, not overwritten. Merging is the whole design — those rows carry the
//! same `trade_id` the WS dedupes on, so venue truth and the socket write into
//! one set and one accumulator, and neither can double-count the other. That is
//! what makes the repair independent of whether Kalshi replays on resubscribe,
//! which this repo still has not established. See `KalshiFills`,
//! `kalshi_fills_recovered()` for the signal that a frame really was lost,
//! `kalshi_reconcile_failures()` for the signal that the repair is down, and
//! `kalshi_reconcile_rejected()` for the guard that keeps it safe against the
//! one assumption no capture in this repo can settle.
//!
//! It holds credentials and, when armed, the Kalshi order sink — for the fill
//! HISTORY read only. There is still no order code path here.

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

/// Fills the venue had that this process's WS never delivered, recovered by
/// `reconcile` reading `GET /portfolio/fills`.
///
/// This is the gauge that says THE DEFECT HAPPENED. `kalshi_fill_gaps` counts
/// windows in which a fill *could* have been lost and cannot tell a blip over
/// an empty book from a blip over a live 25-lot; this counts trades that were
/// real, were ours, and reached the hedge path only because the reconciliation
/// went and got them. Nonzero is not an error — it is the repair working.
static KALSHI_FILLS_RECOVERED: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_fills_recovered() -> u64 {
    KALSHI_FILLS_RECOVERED.load(Ordering::Relaxed)
}

/// Reconciliations that could not be completed: the venue refused, the local
/// background budget was spent, the response could not be parsed, or the
/// history was longer than the page cap.
///
/// Must stay 0. While it is rising the engine is back to exactly its old
/// posture — a local running sum with no venue truth behind it — so
/// `kalshi_fill_gaps` becomes the only signal again and the runbook below is
/// live. A failed reconciliation changes NO state.
///
/// RUNBOOK for that state, because a bounded window is checkable and "reconcile
/// by hand" is not: each failure is stamped in the log with the window it could
/// not read. `arbbot-hedge.timer` independently reads Kalshi venue truth every
/// 5 minutes (`scripts/hedge_naked_legs.py` GETs `/portfolio/positions` and
/// keys on `position_fp`), printing the exact shape this defect produces —
/// `[HEDGE] <rel> Kalshi-long naked imb +N — not auto-hedged (v1)`. Caveats
/// that make it a floor and not a proof: that timer iterates only registry
/// pairs while this channel subscribes account-wide, so a fill on an unpaired
/// market is invisible to it; the Kalshi-long direction is `print`ed, not
/// `Alerter`ed, so nothing pages; and it is frozen Python.
static KALSHI_RECONCILE_FAILURES: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_reconcile_failures() -> u64 {
    KALSHI_RECONCILE_FAILURES.load(Ordering::Relaxed)
}

/// Orders whose venue rows this process REFUSED to merge, because merging them
/// would have pushed the running total above what the venue reports for the
/// same window.
///
/// THE CHECK THAT MAKES THE MERGE SAFE TO SHIP. The whole design rests on WS
/// `trade_id` and REST `trade_id` being the same identifier, and no raw WS fill
/// frame has ever been captured, so that rests on them being the same field
/// name from the same account. If they are NOT, every venue row looks new: an
/// order 12-filled and 12-hedged over the socket would take 12 more from the
/// reconciliation, `observe_cum_fill` clamps that to the RESTING size rather
/// than to zero, and 12 extra contracts get hedged the wrong way.
///
/// It is checkable for free from data already in hand. The window starts at
/// this process's start, so the venue's rows for it are a SUPERSET of what the
/// socket delivered — if the ids share a space. Project the merge before doing
/// it: `local + (rows whose trade_id is new)` must not exceed the venue's own
/// total for that order. Under matching ids the already-seen rows drop out and
/// it lands exactly on the venue's total; under mismatched ids nothing drops
/// out and it lands at roughly double. An order the socket never saw projects
/// to exactly the venue total either way, so the case this repair exists for is
/// untouched.
///
/// A REST read that simply LAGS the socket trips this too — Kalshi is not
/// read-your-writes (`Settle::retry_404`) — and that is fine: both causes want
/// the same response, which is to merge nothing for that order and try again on
/// the next reconnect.
///
/// TELLING THE TWO APART IS NOT THIS COUNTER'S JOB, IT IS THE LOG'S. Two
/// earlier versions of this comment claimed otherwise and both were wrong, so
/// the retractions are worth keeping: "lag is transient, a mismatch climbs with
/// the fill rate" is false because BOTH climb with the fill rate — the
/// reconciliation fires immediately after resubscribe, exactly when a post-gap
/// burst is landing, so lag-driven rejections are expected on a busy account
/// rather than alarming. "Under lag `kalshi_fills_recovered` keeps moving" is
/// false in both directions: recovering nothing is the COMMON case on a healthy
/// reconnect, so recovered sits at zero under lag too; and under a mismatch it
/// still MOVES, because an order the socket never saw has `local == 0`, passes
/// the guard by construction, and merges.
///
/// The discriminant that IS true is the ORDER ID in the refusal log below.
/// `local` never shrinks and the window never advances, so under a mismatch the
/// guard reduces to `local > 0` and the same order is refused on every
/// reconcile for the life of the process; the per-reconcile refusal count also
/// equals the number of orders this process has counted a fill on, not the one
/// or two that were racing. Under lag the order merges as soon as REST catches
/// up and its id stops appearing. So: THE SAME ORDER ID REPEATING ACROSS
/// CONSECUTIVE REFUSALS is the mismatch signature. Evidence, not proof — an
/// order that is filling continuously could race two reconciles 30s apart —
/// but it is the only signal here that distinguishes rather than merely counts.
///
/// What the guard does and does not contain, stated exactly, because the
/// tempting summary ("a mismatch makes this inert") is another overclaim: for
/// an order this process has already counted a fill on, a mismatch is refused
/// and nothing is double-hedged. For an order with `local == 0` the merge is
/// CORRECT even under a mismatch — the venue's rows are that order's real fills
/// and there is nothing local to double them against. The residual is a
/// mismatch AND a replay together: the socket would redeliver that fill under
/// an id the merge has not banked, and it would count twice. That needs both
/// unproven venue behaviours at once, and it is self-detecting afterwards —
/// `local` is then above venue truth, so the order is refused from the next
/// reconcile on, which is exactly the signature above.
static KALSHI_RECONCILE_REJECTED: AtomicU64 = AtomicU64::new(0);

pub fn kalshi_reconcile_rejected() -> u64 {
    KALSHI_RECONCILE_REJECTED.load(Ordering::Relaxed)
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
/// THE RUNNING TOTAL IS RECONCILED AGAINST VENUE TRUTH, and what makes that
/// safe is that `GET /portfolio/fills` returns IDENTITIES, not a count. Every
/// row carries the same `trade_id` this channel dedupes on — verified across
/// every row of the live dump, where `fill_id == trade_id` on all of them — so
/// `merge_venue` feeds the venue's fills through the SAME
/// `claim` the WS uses. A fill the socket missed enters the set and is counted;
/// a fill it already delivered is a duplicate and is not. One accumulator, one
/// dedupe set, two sources writing to them the same way.
///
/// That is what removes the dependency on the open question. Whether Kalshi
/// replays the missed fills on resubscribe is still NOT established — the
/// commit that introduced this channel asserted it in prose, nothing in the
/// repo tests or records it, Python never subscribed to this channel, and the
/// one adjacent documented behaviour points the other way
/// (`kalshi-ws-snapshots-only-on-subscribe`: this WS does not re-send state
/// after a gap). It no longer decides anything. If Kalshi replays, the replayed
/// frames carry trade_ids the reconciliation has already banked and `claim`
/// drops them. If it does not, the reconciliation is the only way those
/// contracts ever arrive. Both branches land on the same total.
///
/// THE INVARIANT TO PRESERVE IN ANY FUTURE EDIT: the emitted count is the floor
/// of a sum over a DISTINCT subset of the order's real fills. It can therefore
/// never exceed what the venue filled, so `arb_core::fill::observe_cum_fill` —
/// monotone, and clamped to the resting size — can never be made to mint a
/// contract that does not exist. Overwriting the total with a NUMBER instead of
/// merging identities breaks exactly this, and a replay would then hedge the
/// recovered contracts a second time: directional exposure the opposite way,
/// which is worse than the missed hedge it repairs.
///
/// The one assumption left is that WS `trade_id` and REST `trade_id` are the
/// same identifier. No raw WS fill frame has ever been captured — the five
/// Kalshi lines in `data/trader-rs/m3-wal.jsonl` are this parser's OUTPUT, all
/// `cum: 5` on five distinct order ids, so they exercise neither multi-frame
/// accumulation nor a resubscribe — so it rests on them being the same field
/// name from the same account. That assumption is not merely documented: it is
/// CHECKED on every merge, per order, against the venue's own total for the
/// window. See `kalshi_reconcile_rejected`, which is what turns a wrong guess
/// here into an inert reconciliation plus a counter rather than a double hedge.
#[derive(Default)]
pub struct KalshiFills {
    seen: std::collections::HashSet<String>,
    /// order id -> contracts x100. HUNDREDTHS, because `count_fp` is fractional
    /// on about 10% of live fills and truncating each piece loses contracts —
    /// see `count_fp_hundredths`. Floored to whole contracts once, on emission.
    hundredths: std::collections::HashMap<String, i64>,
}

/// What one claimed fill did to its order's running total.
struct Claimed {
    /// Whole contracts filled on the order, after this fill.
    cum: i64,
    /// ...and whether that number MOVED. A sub-contract piece is counted
    /// without being reportable: the engine hedges whole contracts, and the
    /// piece is banked so its sibling completes it.
    moved: bool,
}

impl KalshiFills {
    /// Bank one fill. `None` if this trade was already counted — from the WS,
    /// from a replay of it, or from the venue's own history.
    ///
    /// THE single place the running total changes. Both sources go through it
    /// precisely so neither can invent an accumulation rule of its own.
    fn claim(&mut self, trade_id: &str, order_id: &str, hundredths: i64) -> Option<Claimed> {
        if !self.seen.insert(trade_id.to_string()) {
            return None; // already counted — never hedge a fill twice
        }
        let entry = self.hundredths.entry(order_id.to_string()).or_insert(0);
        let before = *entry;
        *entry += hundredths;
        // Track the fraction that is banked but not yet hedgeable.
        KALSHI_FILL_DUST.fetch_add((*entry % 100) - (before % 100), Ordering::Relaxed);
        let cum = *entry / 100;
        Some(Claimed { cum, moved: cum > before / 100 })
    }

    /// Merge the venue's own fill history in, and return the lines to emit.
    ///
    /// The rows are venue truth for the window they cover, but they are MERGED
    /// rather than believed: each is offered to `claim`, which counts it only
    /// if its `trade_id` is new. So this can add contracts the WS never
    /// delivered and can never re-add one it did — including one delivered
    /// after this read was issued, because the socket is already resubscribed
    /// by the time it runs and the dedupe covers the overlap.
    ///
    /// GUARDED PER ORDER, and that guard is what makes it safe to ship against
    /// an unverified id space — see `kalshi_reconcile_rejected` for the whole
    /// argument. The projection below is what the merge WOULD produce; if it
    /// exceeds the venue's own total for that order, the ids are not merging
    /// the way the design assumes and this refuses to touch the order at all.
    pub fn merge_venue(&mut self, rows: &[arb_venue::resp::KalshiFillRow]) -> Vec<String> {
        use std::collections::{HashMap, HashSet};
        // ONE ROW PER TRADE, before anything counts them.
        //
        // The guard below reduces to `local > (already-seen rows)`, which is
        // only an invariant while that second term is a sum over DISTINCT
        // fills. A row repeated inside this page set would add to the venue
        // total on both passes and to the projection on neither — because its
        // trade_id is already spent — so each repeat would LOOSEN the guard by
        // exactly its own size, and under a foreign id space the already-seen
        // total is precisely the quantity the guard fires on.
        //
        // Repeats are not exotic: this walks a cursor over a list that grows at
        // the head, at the moment a post-gap burst is landing, and it takes two
        // pages as soon as the process's own window holds more than one page of
        // fills. Whether Kalshi's cursor actually repeats a row across pages is
        // unprobed — which is the point. Checking is free from rows already in
        // hand, and this is the same assumption the walk in
        // `KalshiGateway::fills_since` refuses to make about sort order.
        let mut in_page: HashSet<&str> = HashSet::new();
        let rows: Vec<&arb_venue::resp::KalshiFillRow> =
            rows.iter().filter(|r| in_page.insert(r.trade_id.as_str())).collect();

        // The venue's own total per order, and what merging would make ours.
        let mut venue: HashMap<&str, i64> = HashMap::new();
        let mut projected: HashMap<&str, i64> = HashMap::new();
        for r in &rows {
            let Some(n) = count_fp_hundredths(&r.count_fp).filter(|n| *n > 0) else { continue };
            *venue.entry(&r.order_id).or_insert(0) += n;
            if !self.seen.contains(&r.trade_id) {
                *projected.entry(&r.order_id).or_insert(0) += n;
            }
        }
        let rejected: std::collections::HashSet<&str> = venue
            .iter()
            .filter(|(oid, v)| {
                let local = self.hundredths.get(**oid).copied().unwrap_or(0);
                local + projected.get(**oid).copied().unwrap_or(0) > **v
            })
            .map(|(oid, _)| *oid)
            .collect();
        for oid in &rejected {
            KALSHI_RECONCILE_REJECTED.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[fills] kalshi reconcile REFUSED order {oid}: merging the venue's rows would \
                 take this process to {} hundredths against the venue's own {} for the same \
                 window. Either the REST fill list lags the socket (Kalshi is not \
                 read-your-writes) or WS and REST trade_ids are different id spaces — and the \
                 second would double-hedge. TELL THEM APART BY THIS ORDER ID: under lag it \
                 merges once REST catches up and stops appearing; under a mismatch the SAME \
                 id is refused on every reconcile for the life of this process. NOTHING \
                 merged for this order; the next reconnect retries. \
                 kalshi_reconcile_rejected={}",
                self.hundredths.get(*oid).copied().unwrap_or(0)
                    + projected.get(*oid).copied().unwrap_or(0),
                venue.get(*oid).copied().unwrap_or(0),
                kalshi_reconcile_rejected()
            );
        }

        let mut out = Vec::new();
        let mut recovered = 0u64;
        for r in rows {
            if rejected.contains(r.order_id.as_str()) {
                continue;
            }
            // Same posture as the WS path: say so, do not guess, and leave the
            // trade re-countable.
            let Some(n) = count_fp_hundredths(&r.count_fp).filter(|n| *n > 0) else {
                KALSHI_FILLS_UNREADABLE.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "[fills] kalshi venue fill {} on order {}: count_fp `{}` unreadable or \
                     non-positive — NOT counted",
                    r.trade_id, r.order_id, r.count_fp
                );
                continue;
            };
            let Some(c) = self.claim(&r.trade_id, &r.order_id, n) else { continue };
            recovered += 1;
            if c.moved {
                out.push(fill_line(&r.order_id, r.market(), c.cum));
            }
        }
        if recovered > 0 {
            KALSHI_FILLS_RECOVERED.fetch_add(recovered, Ordering::Relaxed);
        }
        out
    }

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
        let c = self.claim(trade_id, order_id, n)?;
        if !c.moved {
            // A sub-contract piece. COUNTED — its trade_id is spent and its
            // hundredths are banked — but there is no whole contract to hedge
            // yet, and reporting an unchanged cumulative total would be a
            // no-op at the ledger anyway.
            return None;
        }
        let market = msg.get("market_ticker").and_then(|x| x.as_str()).unwrap_or_default();
        Some(fill_line(order_id, market, c.cum))
    }
}

/// The engine's `fill` line. One writer, so the WS and the venue read cannot
/// disagree about the schema the engine parses.
fn fill_line(order_id: &str, market: &str, cum: i64) -> String {
    serde_json::json!({
        "kind": "fill",
        "venue": "kalshi",
        "market_id": market,
        "order_id": order_id,
        "cum": cum,
        "ts_local_ns": now_ns(),
    })
    .to_string()
}

/// Shortest interval between two reconciliations.
///
/// The reconnect backoff is 2s, so an unthrottled reconcile would issue a paged
/// venue read every two seconds against a 60/min background budget shared with
/// every other read this process makes.
///
/// It DEFERS rather than drops. Dropping was wrong for a reason that only shows
/// up in the failure it causes: the reconnect is the only trigger, so "the next
/// one will cover it" assumes a next one, and a flap at gap #3 twelve seconds
/// after gap #2 followed by a socket that stays up all day means that fill is
/// never reconciled at all. What IS dropped is a request made while one is
/// already queued, and that is free: the window is [process start, now], so the
/// queued reconciliation covers strictly more ground than the one being
/// dropped.
const RECONCILE_MIN_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// Shared state, so the reconciliation can run as its OWN task.
///
/// It cannot run inline. `spawn_blocking` keeps the tokio worker free, but a
/// task that awaits it stops polling its socket — and tungstenite only answers
/// Pings while polled. With the 15s HTTP timeout and a 5-page cap, an inline
/// reconcile can leave the fill socket unpolled for over a minute, long enough
/// for Kalshi to close it: a gap CAUSED by the repair for the last gap.
type Shared = std::sync::Arc<std::sync::Mutex<KalshiFills>>;

/// Queue a reconciliation. At most one is ever queued or running.
fn spawn_reconcile(
    state: Shared,
    sink: std::sync::Arc<dyn crate::sink::OrderSink>,
    since_s: i64,
    tx: Sender<FeedMsg>,
    gate: std::sync::Arc<tokio::sync::Mutex<Option<Instant>>>,
) {
    tokio::spawn(async move {
        // Already queued or running: that one covers this window too.
        let Ok(mut last) = gate.try_lock() else { return };
        if let Some(t) = *last {
            let waited = t.elapsed();
            if waited < RECONCILE_MIN_INTERVAL {
                tokio::time::sleep(RECONCILE_MIN_INTERVAL - waited).await;
            }
        }
        reconcile(&state, &sink, since_s, &tx).await;
        // Stamped after the ATTEMPT, success or not, so a venue that is
        // refusing cannot be hammered every 2s by a flapping socket.
        *last = Some(Instant::now());
    });
}

/// Read the venue's fill history for this process's lifetime and merge it.
///
/// `Priority::Background` all the way down (`KalshiGateway::fills_since`): this
/// is a poll, no hedge waits on it, and a spent budget must refuse it rather
/// than earn a venue-side 429 (quirk `xv-shared-api-budget`).
///
/// FAILURE CHANGES NOTHING. A refused, unreachable, unparseable or truncated
/// read merges no rows, so the engine keeps exactly what it had before this
/// existed: a local running sum. It is counted (`kalshi_reconcile_failures`)
/// and logged with the window it could not read, because the one thing worse
/// than being blind is being blind quietly.
async fn reconcile(
    state: &Shared,
    sink: &std::sync::Arc<dyn crate::sink::OrderSink>,
    since_s: i64,
    tx: &Sender<FeedMsg>,
) {
    let s = sink.clone();
    let rows = match tokio::task::spawn_blocking(move || s.fills_since(since_s)).await {
        Ok(Ok(rows)) => rows,
        Ok(Err(e)) => {
            KALSHI_RECONCILE_FAILURES.fetch_add(1, Ordering::Relaxed);
            eprintln!(
                "[fills] kalshi reconcile FAILED ({e}) — the fill totals for the window since \
                 unix {since_s} are this process's local sum with nothing behind them, exactly \
                 as before this existed. kalshi_reconcile_failures={}",
                kalshi_reconcile_failures()
            );
            return;
        }
        Err(e) => {
            KALSHI_RECONCILE_FAILURES.fetch_add(1, Ordering::Relaxed);
            eprintln!("[fills] kalshi reconcile task failed ({e}); window since unix {since_s}");
            return;
        }
    };
    let n = rows.len();
    // Scoped: a std guard is not Send and must not be held across the sends.
    let lines = { state.lock().expect("fill state").merge_venue(&rows) };
    for line in lines {
        eprintln!("[fills] RECOVERED {line}");
        if tx.send(FeedMsg { line, t_read: Instant::now() }).await.is_err() {
            return; // engine gone
        }
    }
    eprintln!(
        "[fills] kalshi reconcile ok — {n} venue fill(s) in the window since unix {since_s}; \
         kalshi_fills_recovered={} rejected={}",
        kalshi_fills_recovered(),
        kalshi_reconcile_rejected()
    );
}

/// `sink` is the Kalshi order sink, present only when the process is armed. It
/// is what the reconciliation reads venue truth through — the SAME sink, and so
/// the same background token bucket, as every other venue read here. Without
/// orders there is nothing to hedge and nothing to reconcile, so `None` simply
/// runs the feed as it always ran.
pub async fn kalshi_fill_feed(
    key_id: String,
    pem: String,
    tx: Sender<FeedMsg>,
    sink: Option<std::sync::Arc<dyn crate::sink::OrderSink>>,
) {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Message;

    // State lives ACROSS reconnects: the dedupe is what stops a replay becoming
    // a double hedge, and it is now also what stops the RECONCILIATION becoming
    // one — both sources claim the same `trade_id`s out of the same set.
    //
    // It does NOT live across restarts, and this is where that is counted. The
    // state below starts empty while the venue's positions do not, so the
    // downtime before this line is a gap of exactly the same kind as a
    // reconnect, and the largest one: it is the whole window in which no
    // process was subscribed. It is also the one gap this cannot reconcile —
    // see `kalshi_fill_gaps` for what covers it instead.
    KALSHI_FILL_GAPS.fetch_add(1, Ordering::Relaxed);
    let state: Shared = std::sync::Arc::new(std::sync::Mutex::new(KalshiFills::default()));
    let gate = std::sync::Arc::new(tokio::sync::Mutex::new(None::<Instant>));
    // The reconciliation window opens HERE, not at the venue's epoch. A fill
    // older than this process belongs to an order it never registered, so
    // replaying one would reach `FillOutcome::Unknown`, sit in
    // `unclaimed_fills` for 15s and alarm as money we cannot explain — turning
    // a repair into a false alarm on every restart.
    //
    // `+1` because the venue's `ts` is in whole SECONDS: without it a fill from
    // the same second as this line, but before it, reads as inside the window.
    // The second it costs is safe — an order cannot be placed before the
    // preconditions that spawn this task have been met.
    let since_s = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
        + 1;

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

            // AFTER the resubscribe, never before: the read then covers
            // [process start, now] while the socket covers [subscribe, ...],
            // and the two overlap. A fill landing in that overlap is claimed
            // once, by whichever arrives first. Reconciling before subscribing
            // would leave the interval between them covered by neither.
            //
            // The BOOT gap is skipped (`kalshi_fill_gaps() == 1` only here):
            // the window is empty by construction, and reading further back
            // would pull in fills on orders this engine never registered.
            if let (Some(s), true) = (sink.as_ref(), kalshi_fill_gaps() > 1) {
                spawn_reconcile(
                    state.clone(),
                    s.clone(),
                    since_s,
                    tx.clone(),
                    gate.clone(),
                );
            }

            while let Some(frame) = ws.next().await {
                let msg = frame.map_err(|e| format!("read: {e}"))?;
                let Message::Text(raw) = msg else { continue };
                // Scoped: the guard must not be alive across the send below.
                let out = { state.lock().expect("fill state").line(&raw) };
                if let Some(line) = out {
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
                    "[fills] kalshi dropped ({e}); reconnecting in 2s, then reconciling the \
                     window against GET /portfolio/fills — a fill inside this gap is \
                     recovered whether or not Kalshi replays, because the venue's rows carry \
                     the same trade_id the dedupe uses. Watch for the reconcile line that \
                     follows: FAILED means the totals are a local sum again \
                     (kalshi_reconcile_failures runbook), REFUSED means they are a local sum \
                     for that order and possibly always will be \
                     (kalshi_reconcile_rejected). kalshi_fill_gaps={}",
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

    // ------------------------------------------ reconciliation (venue truth) ---

    use arb_venue::resp::KalshiFillRow;

    fn row(trade: &str, order: &str, count: &str) -> KalshiFillRow {
        KalshiFillRow {
            trade_id: trade.into(),
            order_id: order.into(),
            count_fp: count.into(),
            market_ticker: Some("KXTEST".into()),
            ticker: None,
            ts: 1_767_225_600,
        }
    }

    /// Contracts the engine's own ledger would hedge for one emitted line. The
    /// REAL consumer: `observe_cum_fill` is the single funnel every fill path
    /// goes through, and the whole question here is what IT is told.
    fn hedged(fl: &mut arb_core::fill::FillLedger, order_id: &str, line: &str) -> i64 {
        let cum = serde_json::from_str::<Value>(line).unwrap()["cum"].as_i64().expect("cum");
        match fl.observe_cum_fill(order_id, cum) {
            arb_core::fill::FillOutcome::Minted(ob) => ob.into_parts().2,
            _ => 0,
        }
    }

    /// THE test this whole change exists for.
    ///
    /// A fill is lost while the socket is dark. The reconciliation reads the
    /// venue's own history and the contracts reach the hedge path. Then Kalshi
    /// replays the gap on resubscribe — the behaviour this repo has never been
    /// able to establish — and NOTHING is hedged twice.
    ///
    /// It works because the venue's rows carry the same `trade_id` the WS
    /// dedupes on: the replayed frame is already in `seen`, so `claim` drops
    /// it. Overwriting the running total with a NUMBER instead — the obvious
    /// repair — would add the recovered contracts a second time here.
    #[test]
    fn venue_truth_survives_a_replay_because_it_carries_trade_ids() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let mut fl = arb_core::fill::FillLedger::new();
        fl.register_order("o1", "KXTEST", 25, None);
        let mut total = 0;

        total += hedged(&mut fl, "o1", &s.line(&frame("t1", "o1", "2.00")).unwrap());
        total += hedged(&mut fl, "o1", &s.line(&frame("t2", "o1", "3.00")).unwrap());
        assert_eq!(total, 5, "5 filled, 5 hedged");

        // The socket drops. The venue fills 4 more on t3 and no frame arrives.
        // On reconnect the reconciliation reads the window back: t1 and t2 are
        // already counted, t3 is not.
        let lines = s.merge_venue(&[
            row("t1", "o1", "2.00"),
            row("t2", "o1", "3.00"),
            row("t3", "o1", "4.00"),
        ]);
        assert_eq!(lines.len(), 1, "only the trade the socket missed is reported");
        total += hedged(&mut fl, "o1", &lines[0]);
        assert_eq!(total, 9, "the lost 4 are recovered and hedged");

        // ...and NOW Kalshi replays the gap. t3's frame arrives for the first
        // time on this socket, but not for the first time in `seen`.
        assert!(s.line(&frame("t3", "o1", "4.00")).is_none(), "the replay is a duplicate");

        // The next genuine fill continues from venue truth, not from a total
        // that is short by the gap.
        total += hedged(&mut fl, "o1", &s.line(&frame("t4", "o1", "6.00")).unwrap());
        assert_eq!(total, 15, "9 + 6 — the running sum was HEALED, not just reported over");
    }

    /// ...and if Kalshi does NOT replay, the same sequence lands on the same
    /// total. That is the point: the branch is no longer load-bearing.
    #[test]
    fn the_total_is_the_same_whether_or_not_the_venue_replays() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        s.line(&frame("t1", "o1", "2.00"));
        s.merge_venue(&[row("t1", "o1", "2.00"), row("t3", "o1", "4.00")]);
        // No replay of t3 — just the next real fill.
        let v: Value = serde_json::from_str(&s.line(&frame("t4", "o1", "6.00")).unwrap()).unwrap();
        assert_eq!(v["cum"], 12, "2 + 4 + 6, identical to the replay path above");
    }

    /// THE GUARD, and the reason this is shippable against an id space no
    /// capture in this repo can confirm.
    ///
    /// If WS `trade_id` and REST `trade_id` were different id spaces, every
    /// venue row would look new. An order 12-filled and 12-hedged over the
    /// socket would take 12 MORE from the reconciliation — and
    /// `observe_cum_fill` clamps that to the RESTING size, not to zero, so on a
    /// 25-lot it mints 12 extra contracts hedged the wrong way.
    ///
    /// It is detectable for free: the window starts at process start, so the
    /// venue's rows for it are a superset of what the socket delivered — if the
    /// ids share a space. Project the merge first. Under matching ids the
    /// already-seen rows drop out and it lands exactly on the venue's total;
    /// under mismatched ids nothing drops out and it lands at double.
    #[test]
    fn a_foreign_id_space_is_refused_rather_than_double_counted() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let mut fl = arb_core::fill::FillLedger::new();
        fl.register_order("o1", "KXTEST", 25, None);

        let mut total = 0;
        total += hedged(&mut fl, "o1", &s.line(&frame("ws-a", "o1", "5.00")).unwrap());
        total += hedged(&mut fl, "o1", &s.line(&frame("ws-b", "o1", "7.00")).unwrap());
        assert_eq!(total, 12, "12 filled over the socket, 12 hedged");

        // The venue reports the SAME twelve contracts under ids from a
        // different space.
        let before = kalshi_reconcile_rejected();
        let lines = s.merge_venue(&[row("rest-a", "o1", "5.00"), row("rest-b", "o1", "7.00")]);
        assert!(lines.is_empty(), "nothing may be emitted for an order that does not reconcile");
        assert_eq!(kalshi_reconcile_rejected(), before + 1, "and the refusal must be countable");

        // The state is untouched, so the next real fill continues from 12.
        let v: Value = serde_json::from_str(&s.line(&frame("ws-c", "o1", "3.00")).unwrap()).unwrap();
        assert_eq!(v["cum"], 15, "12 + 3 — not 24 + 3");
    }

    /// ...and a row REPEATED inside the page set must not buy the merge past
    /// that guard.
    ///
    /// The guard reduces to `local > (already-seen rows)`, which is an
    /// invariant only while the second term is a sum over DISTINCT fills. A
    /// repeat adds to the venue total on both passes and to the projection on
    /// neither — its trade_id is already spent — so it loosens the guard by
    /// exactly its own size, and under a foreign id space that is precisely
    /// the quantity being tested against.
    ///
    /// The trace, and none of it is exotic: gap #1 loses an order's first fill,
    /// the reconciliation recovers it (`local` is 0, so the guard correctly
    /// cannot fire); the socket returns and delivers genuinely new contracts;
    /// gap #2's walk takes two pages — which it does as soon as the process's
    /// own window holds more than one page of fills — and they overlap, the
    /// ordinary artifact of paging a list that grows at the head while a
    /// post-gap burst is landing. Whether Kalshi's cursor really repeats a row
    /// is unprobed, which is the reason to check rather than assume.
    #[test]
    fn a_row_repeated_across_pages_cannot_loosen_the_guard() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let mut fl = arb_core::fill::FillLedger::new();
        fl.register_order("o1", "KXTEST", 25, None);
        let mut total = 0;

        // Gap #1 missed o1's first fill entirely. `local` is 0, so the guard
        // cannot fire and the recovery goes through — correctly.
        let lines = s.merge_venue(&[row("rest-b", "o1", "5.00")]);
        total += hedged(&mut fl, "o1", &lines[0]);
        assert_eq!(total, 5, "the terminal case still reconciles");

        // The socket returns and delivers 5 genuinely new contracts. Under a
        // FOREIGN id space the venue will report that same fill as `rest-a`.
        total += hedged(&mut fl, "o1", &s.line(&frame("ws-a", "o1", "5.00")).unwrap());
        assert_eq!(total, 10);

        // Gap #2. Two pages, overlapping on the row already merged in step 1.
        let before = kalshi_reconcile_rejected();
        let lines = s.merge_venue(&[
            row("rest-b", "o1", "5.00"),
            row("rest-b", "o1", "5.00"), // the repeat
            row("rest-a", "o1", "5.00"),
        ]);
        assert!(lines.is_empty(), "the repeat must not pay for the merge");
        assert_eq!(kalshi_reconcile_rejected(), before + 1, "it is a refusal, and counted");

        // ...and the ledger never saw a second helping of those 5 contracts.
        assert_eq!(total, 10, "10 filled, 10 hedged — not 15");
        let v: Value = serde_json::from_str(&s.line(&frame("ws-c", "o1", "2.00")).unwrap()).unwrap();
        assert_eq!(v["cum"], 12, "the running total is untouched by the refusal");
    }

    /// The guard must not fire on the case the repair exists FOR: an order
    /// whose fills the socket never saw at all projects to exactly the venue's
    /// own total, which is not an excess.
    #[test]
    fn an_order_the_socket_never_saw_still_reconciles() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let before = kalshi_reconcile_rejected();
        let lines = s.merge_venue(&[row("t1", "lost", "4.00")]);
        assert_eq!(lines.len(), 1, "the terminal case — the only fill was lost in the gap");
        assert_eq!(kalshi_reconcile_rejected(), before, "and it is not a mismatch");
        let v: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v["cum"], 4);
        assert_eq!(v["order_id"], "lost");
    }

    /// A REST list that LAGS the socket trips the same guard — Kalshi is not
    /// read-your-writes — and that is the right answer for it too: merge
    /// nothing for that order and let the next reconnect retry. It must not
    /// poison the OTHER orders in the same read.
    #[test]
    fn a_lagging_venue_list_refuses_one_order_without_touching_the_rest() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        assert!(s.line(&frame("t1", "o1", "2.00")).is_some());
        assert!(s.line(&frame("t2", "o1", "3.00")).is_some());
        // The venue has caught up with t1 but not t2, while o2 is complete.
        let lines = s.merge_venue(&[row("t1", "o1", "2.00"), row("t9", "o2", "1.00")]);
        assert_eq!(lines.len(), 1, "o2 still reconciles");
        let v: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(v["order_id"], "o2");
        // ...and o1 kept its own total, so the next fill continues from 5.
        let w: Value = serde_json::from_str(&s.line(&frame("t3", "o1", "1.00")).unwrap()).unwrap();
        assert_eq!(w["cum"], 6);
    }

    /// A reconciliation that recovers nothing must change nothing — it runs on
    /// EVERY reconnect and the common case is that nothing was lost.
    #[test]
    fn a_reconcile_that_recovers_nothing_emits_nothing() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        assert!(s.line(&frame("t1", "o1", "2.00")).is_some());
        let before = kalshi_fills_recovered();
        assert!(s.merge_venue(&[row("t1", "o1", "2.00")]).is_empty());
        assert_eq!(kalshi_fills_recovered(), before, "nothing recovered, so nothing counted");
    }

    /// Fractional pieces merge like whole ones, and the count that reaches the
    /// engine is still the floor. `2.13` over the socket plus `1.87` recovered
    /// is the venue's 4 — the same real 4-lot the parser tests use, split across
    /// the two sources.
    #[test]
    fn a_recovered_fractional_piece_completes_its_sibling() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let a: Value = serde_json::from_str(&s.line(&frame("t1", "o1", "2.13")).unwrap()).unwrap();
        assert_eq!(a["cum"], 2);
        let lines = s.merge_venue(&[row("t1", "o1", "2.13"), row("t2", "o1", "1.87")]);
        let b: Value = serde_json::from_str(&lines[0]).unwrap();
        assert_eq!(b["cum"], 4, "213 + 187 = 400 across two sources");
    }

    /// A venue row we cannot read must be SAID, not skipped silently, and must
    /// not consume its trade — same posture as the WS path, for the same
    /// reason: a corrected or re-read row for that trade must still count.
    #[test]
    fn an_unreadable_venue_row_is_counted_and_stays_recountable() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let before = kalshi_fills_unreadable();
        assert!(s.merge_venue(&[row("t1", "o1", "not-a-number")]).is_empty());
        assert_eq!(kalshi_fills_unreadable(), before + 1);
        let lines = s.merge_venue(&[row("t1", "o1", "2.00")]);
        assert_eq!(lines.len(), 1, "t1 must still be countable on the next read");
    }

    /// The market comes from `market_ticker` when present and `ticker`
    /// otherwise — the live rows carry both, the WS frame only the first, and
    /// an empty market_id would strand the fill in the engine.
    #[test]
    fn a_venue_row_reports_its_market_either_spelling() {
        let _g = COUNTER.lock();
        let mut s = KalshiFills::default();
        let mut r = row("t1", "o1", "1.00");
        r.market_ticker = None;
        r.ticker = Some("KXFROMTICKER".into());
        let v: Value = serde_json::from_str(&s.merge_venue(&[r])[0]).unwrap();
        assert_eq!(v["market_id"], "KXFROMTICKER");
        assert_eq!(v["venue"], "kalshi");
    }
}
