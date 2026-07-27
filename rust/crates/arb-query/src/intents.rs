//! Reconstruct what the engine would have RESTING right now.
//!
//! `arb-trader` is the dry-run shell: it subscribes to a recorder socket, runs
//! the real quoter, and appends its order intents to a JSONL file. It holds no
//! credentials and has no venue order code path, so the stream is exactly "the
//! trades that would be being made if trading were enabled".
//!
//! This is a different thing from the scenario view. That one is a calculator
//! over top-of-book snapshots, useful for comparing execution styles across
//! pairs. This is the ENGINE's own decisions — the same quoter that would run
//! live — so where they disagree, this one is the answer.
//!
//! Three record shapes, and the state machine is the whole job:
//!   place                       -> the order becomes live
//!   place + replaces: <old_id>  -> new order live, old one dead (a reprice)
//!   cancel                      -> the order is dead
//! Missing the `replaces` link would leave every superseded quote on the book
//! forever; the real stream is 32,023 reprices against 8,548 opens.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
struct Raw {
    #[serde(default)]
    place: Option<String>,
    #[serde(default)]
    cancel: Option<String>,
    order_id: String,
    #[serde(default)]
    replaces: Option<String>,
    #[serde(default)]
    venue: Option<String>,
    #[serde(default)]
    side: Option<String>,
    #[serde(default)]
    price: Option<String>,
    #[serde(default)]
    count: Option<f64>,
    #[serde(default)]
    ts: f64,
}

#[derive(Debug, Clone, Serialize)]
pub struct LiveOrder {
    pub order_id: String,
    pub venue: String,
    pub market: String,
    pub side: String,
    pub price: String,
    pub count: f64,
    pub ts: f64,
    /// How many times this quote has been moved since it first went up. A high
    /// number is the quoter chasing, which is a cost signal, not activity.
    pub reprices: u32,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct IntentState {
    pub live: Vec<LiveOrder>,
    pub opens: u64,
    pub reprices: u64,
    pub cancels: u64,
    pub total: u64,
    /// Timestamp of the last intent seen, so the view can show its age rather
    /// than implying the engine is running when the file is hours old.
    pub last_ts: f64,
    pub parse_failures: u64,
}

/// Fold the whole stream. The file must be read from the START: an order placed
/// early can still be live, so a tail would invent phantom cancels.
pub fn reconstruct(path: &str) -> Result<IntentState, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    Ok(fold(&text))
}

pub fn fold(text: &str) -> IntentState {
    let mut live: HashMap<String, LiveOrder> = HashMap::new();
    let mut st = IntentState::default();

    for line in text.lines() {
        let t = line.trim_matches(char::from(0)).trim();
        if t.is_empty() {
            continue;
        }
        let Ok(r) = serde_json::from_str::<Raw>(t) else {
            st.parse_failures += 1;
            continue;
        };
        st.total += 1;
        if r.ts > st.last_ts {
            st.last_ts = r.ts;
        }

        if let Some(market) = r.place {
            let carried = r
                .replaces
                .as_ref()
                .and_then(|old| live.remove(old))
                .map(|o| o.reprices + 1);
            if carried.is_some() {
                st.reprices += 1;
            } else {
                st.opens += 1;
            }
            live.insert(
                r.order_id.clone(),
                LiveOrder {
                    order_id: r.order_id,
                    venue: r.venue.unwrap_or_default(),
                    market,
                    side: r.side.unwrap_or_default(),
                    price: r.price.unwrap_or_default(),
                    count: r.count.unwrap_or(0.0),
                    ts: r.ts,
                    reprices: carried.unwrap_or(0),
                },
            );
        } else if r.cancel.is_some() {
            st.cancels += 1;
            live.remove(&r.order_id);
        }
    }

    let mut out: Vec<LiveOrder> = live.into_values().collect();
    out.sort_by(|a, b| b.ts.partial_cmp(&a.ts).unwrap_or(std::cmp::Ordering::Equal));
    st.live = out;
    st
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_reprice_replaces_rather_than_duplicates() {
        let s = r#"
{"place":"M","order_id":"m1","venue":"kalshi","side":"bid","price":"0.10","count":5,"ts":1.0}
{"place":"M","order_id":"m2","replaces":"m1","venue":"kalshi","side":"bid","price":"0.11","count":5,"ts":2.0}
"#;
        let st = fold(s);
        assert_eq!(st.live.len(), 1, "the superseded quote must not stay on the book");
        assert_eq!(st.live[0].order_id, "m2");
        assert_eq!(st.live[0].price, "0.11");
        assert_eq!(st.live[0].reprices, 1, "reprice count carries across the replace");
        assert_eq!(st.opens, 1);
        assert_eq!(st.reprices, 1);
        assert_eq!(st.last_ts, 2.0);
    }

    #[test]
    fn cancel_removes_and_reprice_depth_accumulates() {
        let s = r#"
{"place":"A","order_id":"m1","venue":"kalshi","side":"bid","price":"0.10","count":5,"ts":1.0}
{"place":"B","order_id":"m2","venue":"polymarket_us","side":"ask","price":"0.90","count":5,"ts":2.0}
{"place":"A","order_id":"m3","replaces":"m1","venue":"kalshi","side":"bid","price":"0.12","count":5,"ts":3.0}
{"place":"A","order_id":"m4","replaces":"m3","venue":"kalshi","side":"bid","price":"0.13","count":5,"ts":4.0}
{"cancel":"B","order_id":"m2","venue":"polymarket_us","side":"ask","price":"0.90","ts":5.0}
"#;
        let st = fold(s);
        assert_eq!(st.live.len(), 1, "only the twice-repriced A survives");
        assert_eq!(st.live[0].market, "A");
        assert_eq!(st.live[0].reprices, 2, "chasing depth must accumulate");
        assert_eq!(st.opens, 2);
        assert_eq!(st.reprices, 2);
        assert_eq!(st.cancels, 1);
        assert_eq!(st.total, 5);
    }

    #[test]
    fn damaged_lines_are_counted_not_fatal() {
        let s = "{\"place\":\"M\",\"order_id\":\"m1\",\"ts\":1.0}\n\0\0\0\n{bad\n";
        let st = fold(s);
        assert_eq!(st.live.len(), 1);
        assert_eq!(st.parse_failures, 1, "NUL run trims to empty; only the torn line fails");
    }
}
