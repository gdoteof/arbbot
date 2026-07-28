//! Reading open exposure out of the append-only trade ledger.
//!
//! Port of `arbbot/exec/ledger.py`. `data/exec/trades.jsonl` is APPEND-ONLY —
//! closing a basket never rewrites a line, it appends a compensating record —
//! so "what is open" is a fold, not a lookup:
//!
//!   1. `correction` records are folded into their target and dropped
//!   2. `unwound` records net against the `open` record they close
//!   3. what survives, with qty reduced by any partial unwind, is open
//!
//! The engine needs this at startup: without it, risk caps reset on every
//! restart and the engine believes the whole book is free.

use serde_json::Value;
use std::collections::HashMap;

/// Records are matched on `(relationship_id, ts)` where ts is a float. Python
/// uses the float itself as a dict key, so bit-equality is the same predicate —
/// identical parses of the same literal give identical doubles.
type Key = (String, u64);

fn key_of(rel: Option<&str>, ts: Option<f64>) -> Option<Key> {
    Some((rel?.to_string(), ts?.to_bits()))
}

fn rel_of(r: &Value) -> Option<&str> {
    r.get("relationship_id").and_then(|v| v.as_str())
}

fn f64_of(r: &Value, field: &str) -> Option<f64> {
    r.get(field).and_then(|v| v.as_f64())
}

fn status_of(r: &Value) -> &str {
    r.get("status").and_then(|v| v.as_str()).unwrap_or("")
}

/// Fold `correction` records into their targets and drop them.
///
/// A correction is appended when a recorded value proves wrong (a hedge
/// response lost to a 429 that recorded avg_price=0). `fields` are
/// shallow-merged — a `legs` value replaces the whole list.
fn apply_corrections(records: Vec<Value>) -> Vec<Value> {
    let mut fixes: HashMap<Key, serde_json::Map<String, Value>> = HashMap::new();
    for r in &records {
        if status_of(r) == "correction" {
            if let Some(k) = key_of(rel_of(r), f64_of(r, "corrects_ts")) {
                if let Some(f) = r.get("fields").and_then(|v| v.as_object()) {
                    fixes.entry(k).or_default().extend(f.clone());
                }
            }
        }
    }
    if fixes.is_empty() {
        return records.into_iter().filter(|r| status_of(r) != "correction").collect();
    }
    records
        .into_iter()
        .filter(|r| status_of(r) != "correction")
        .map(|mut r| {
            if let Some(k) = key_of(rel_of(&r), f64_of(&r, "ts")) {
                if let (Some(f), Some(obj)) = (fixes.get(&k), r.as_object_mut()) {
                    for (kk, vv) in f {
                        obj.insert(kk.clone(), vv.clone());
                    }
                }
            }
            r
        })
        .collect()
}

/// Open contracts per relationship, net of unwinds. Partial unwinds reduce the
/// basket rather than dropping it; a fully unwound basket disappears.
pub fn open_exposure(records: Vec<Value>) -> HashMap<String, f64> {
    let records = apply_corrections(records);

    let mut unwound: HashMap<Key, f64> = HashMap::new();
    for r in &records {
        if status_of(r) == "unwound" {
            if let Some(k) = key_of(rel_of(r), f64_of(r, "closes_ts")) {
                *unwound.entry(k).or_default() += f64_of(r, "qty").unwrap_or(0.0);
            }
        }
    }

    let mut out: HashMap<String, f64> = HashMap::new();
    for r in &records {
        if status_of(r) != "open" {
            continue;
        }
        let Some(rel) = rel_of(r) else { continue };
        let qty = f64_of(r, "qty").unwrap_or(0.0);
        let closed = key_of(Some(rel), f64_of(r, "ts"))
            .and_then(|k| unwound.get(&k).copied())
            .unwrap_or(0.0);
        let remaining = if closed <= 0.0 { qty } else { qty - closed };
        if remaining > 0.0 {
            *out.entry(rel.to_string()).or_default() += remaining;
        }
    }
    out
}

pub fn read(path: &str) -> Result<Vec<Value>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    // Unparseable lines are SKIPPED, not fatal: the live runner appends
    // concurrently, so the last line can be a partial write.
    Ok(text
        .lines()
        .filter(|l| !l.trim().is_empty())
        .filter_map(|l| serde_json::from_str::<Value>(l).ok())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(s: &str) -> Value {
        serde_json::from_str(s).unwrap()
    }

    #[test]
    fn an_untouched_open_basket_is_fully_open() {
        let recs = vec![v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#)];
        assert_eq!(open_exposure(recs).get("r1"), Some(&50.0));
    }

    #[test]
    fn a_fully_unwound_basket_frees_all_its_exposure() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":50}"#),
        ];
        assert!(open_exposure(recs).get("r1").is_none());
    }

    /// The case that makes this a fold rather than a filter.
    #[test]
    fn a_partial_unwind_reduces_the_basket() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":20}"#),
        ];
        assert_eq!(open_exposure(recs).get("r1"), Some(&30.0));
    }

    /// Several unwinds against one basket accumulate.
    #[test]
    fn repeated_partial_unwinds_accumulate() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":20}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":25}"#),
        ];
        assert_eq!(open_exposure(recs).get("r1"), Some(&5.0));
    }

    /// An unwind matches ONE basket by (relationship_id, ts) — not every basket
    /// on the relationship. Getting this wrong frees exposure that is still on.
    #[test]
    fn an_unwind_only_closes_the_basket_it_names() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"open","relationship_id":"r1","ts":2.0,"qty":30}"#),
            v(r#"{"status":"unwound","relationship_id":"r1","closes_ts":1.0,"qty":50}"#),
        ];
        assert_eq!(open_exposure(recs).get("r1"), Some(&30.0));
    }

    #[test]
    fn a_correction_is_folded_into_its_target_and_dropped() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"correction","relationship_id":"r1","corrects_ts":1.0,"fields":{"qty":40}}"#),
        ];
        let e = open_exposure(recs);
        assert_eq!(e.get("r1"), Some(&40.0), "corrected qty wins");
        assert_eq!(e.len(), 1, "the correction record itself is not exposure");
    }

    #[test]
    fn realized_and_unknown_statuses_are_not_open() {
        let recs = vec![
            v(r#"{"status":"realized","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"relationship_id":"r2","ts":2.0,"qty":10}"#),
        ];
        assert!(open_exposure(recs).is_empty(), "only status=open counts");
    }

    #[test]
    fn exposure_sums_across_baskets_on_one_relationship() {
        let recs = vec![
            v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#),
            v(r#"{"status":"open","relationship_id":"r1","ts":2.0,"qty":30}"#),
            v(r#"{"status":"open","relationship_id":"r2","ts":3.0,"qty":7}"#),
        ];
        let e = open_exposure(recs);
        assert_eq!(e.get("r1"), Some(&80.0));
        assert_eq!(e.get("r2"), Some(&7.0));
    }

    /// A partial write at the tail must not lose every earlier record.
    #[test]
    fn a_truncated_last_line_is_skipped_not_fatal() {
        let dir = std::env::temp_dir().join(format!("arb-ledger-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\
             \n{\"status\":\"open\",\"relationship_id\"",
        )
        .unwrap();
        let recs = read(p.to_str().unwrap()).unwrap();
        assert_eq!(recs.len(), 1);
        assert_eq!(open_exposure(recs).get("r1"), Some(&50.0));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
