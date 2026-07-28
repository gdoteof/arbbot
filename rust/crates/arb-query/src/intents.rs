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
    /// OPTIONAL, and this matters more than it looks. A `skip` intent carries
    /// no order id, so a required field here made every skip line fail to
    /// deserialize — and the failure `continue`s BEFORE `last_ts` is updated.
    /// The dashboard therefore reported "NOT RUNNING" for an engine writing
    /// every second, purely because it had been skipping rather than placing
    /// (2026-07-28: 28 minutes of topic-budget skips read as a dead engine,
    /// and ~365k of 374k lines were silently counted as parse failures).
    #[serde(default)]
    order_id: Option<String>,
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
    old_price: Option<String>,
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
            // A place without an id cannot be tracked, cancelled or matched
            // to a fill. Count it and move on rather than inventing a key.
            let Some(oid) = r.order_id.clone() else {
                st.parse_failures += 1;
                continue;
            };
            live.insert(
                oid.clone(),
                LiveOrder {
                    order_id: oid,
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
            if let Some(oid) = r.order_id.as_ref() {
                live.remove(oid);
            }
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

    /// A skip intent has no `order_id`, and it MUST still advance `last_ts`.
    /// When it did not, the dashboard called a busy engine "NOT RUNNING":
    /// every line for 28 minutes was a topic-budget skip, so the last
    /// recognised timestamp was half an hour old while the file grew every
    /// second. ~365k of 374k lines were also being counted as parse failures.
    #[test]
    fn skip_intents_are_liveness_too() {
        let text = concat!(
            r#"{"place":"M","order_id":"m1","venue":"kalshi","side":"bid","price":"0.10","count":5,"ts":10.0}"#,
            "\n",
            r#"{"skip":["topic budget [x]: 1+5 > 5"],"ts":99.0}"#,
            "\n",
        );
        let st = fold(text);
        assert_eq!(st.last_ts, 99.0, "a skip is the engine speaking, not silence");
        assert_eq!(st.parse_failures, 0, "a skip is a valid intent, not a broken line");
        assert_eq!(st.total, 2);
        assert_eq!(st.live.len(), 1, "and it must not disturb the resting book");
    }

    /// A cancel record carries no market, so the history must remember where
    /// each order lived or cancels vanish from the series.
    #[test]
    fn history_follows_a_pair_including_its_cancels() {
        let s = r#"
{"place":"A","order_id":"m1","venue":"kalshi","side":"bid","price":"0.10","count":5,"ts":1.0}
{"place":"Z","order_id":"m9","venue":"kalshi","side":"bid","price":"0.50","count":5,"ts":1.5}
{"place":"A","order_id":"m2","replaces":"m1","old_price":"0.10","venue":"kalshi","side":"bid","price":"0.12","count":5,"ts":2.0}
{"cancel":"A","order_id":"m2","venue":"kalshi","side":"bid","price":"0.12","ts":3.0}
"#;
        let markets = vec![("kalshi".to_string(), "A".to_string())];
        let h = history_for(s, &markets);
        assert_eq!(h.len(), 3, "the unrelated market Z must not appear: {h:?}");
        assert_eq!(h[0].kind, "open");
        assert_eq!(h[1].kind, "reprice");
        assert_eq!(h[1].from_price.as_deref(), Some("0.10"), "a reprice shows the move");
        assert_eq!(h[2].kind, "cancel", "cancel carries no market; must be resolved by order id");
        assert!(h.windows(2).all(|w| w[0].ts <= w[1].ts), "time ordered");
    }

    #[test]
    fn damaged_lines_are_counted_not_fatal() {
        let s = "{\"place\":\"M\",\"order_id\":\"m1\",\"ts\":1.0}\n\0\0\0\n{bad\n";
        let st = fold(s);
        assert_eq!(st.live.len(), 1);
        assert_eq!(st.parse_failures, 1, "NUL run trims to empty; only the torn line fails");
    }
}

/// One quote movement, for charting a single pair's intent history.
#[derive(Debug, Clone, Serialize)]
pub struct Event {
    pub ts: f64,
    pub venue: String,
    pub market: String,
    pub side: String,
    pub price: String,
    /// "open" | "reprice" | "cancel"
    pub kind: &'static str,
    /// Present on a reprice: where the quote came from, so a chart can show
    /// the move rather than just the destination.
    pub from_price: Option<String>,
}

/// Every intent touching any of `markets`, in time order. This is the series
/// for one pair: the engine's own quote, moving.
pub fn history_for(text: &str, markets: &[(String, String)]) -> Vec<Event> {
    let want = |v: &str, m: &str| markets.iter().any(|(mv, mm)| mv == v && mm == m);
    let mut out = Vec::new();
    // A cancel record carries no market, so remember what each order was on.
    let mut where_of: HashMap<String, (String, String)> = HashMap::new();

    for line in text.lines() {
        let t = line.trim_matches(char::from(0)).trim();
        if t.is_empty() {
            continue;
        }
        let Ok(r) = serde_json::from_str::<Raw>(t) else { continue };
        let venue = r.venue.clone().unwrap_or_default();
        if let Some(market) = &r.place {
            let Some(oid) = r.order_id.clone() else { continue };
            where_of.insert(oid, (venue.clone(), market.clone()));
            if !want(&venue, market) {
                continue;
            }
            out.push(Event {
                ts: r.ts,
                venue,
                market: market.clone(),
                side: r.side.clone().unwrap_or_default(),
                price: r.price.clone().unwrap_or_default(),
                kind: if r.replaces.is_some() { "reprice" } else { "open" },
                from_price: r.old_price.clone(),
            });
        } else if r.cancel.is_some() {
            let Some(oid) = r.order_id.as_ref() else { continue };
            let Some((v, m)) = where_of.get(oid).cloned() else { continue };
            if !want(&v, &m) {
                continue;
            }
            out.push(Event {
                ts: r.ts,
                venue: v,
                market: m,
                side: r.side.clone().unwrap_or_default(),
                price: r.price.clone().unwrap_or_default(),
                kind: "cancel",
                from_price: None,
            });
        }
    }
    out.sort_by(|a, b| a.ts.partial_cmp(&b.ts).unwrap_or(std::cmp::Ordering::Equal));
    out
}
