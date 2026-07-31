//! Reading and writing the append-only trade ledger — the record of money.
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
pub(crate) fn apply_corrections(records: Vec<Value>) -> Vec<Value> {
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

/// What this engine stamps every basket it authors with.
///
/// The single-author rule reads off this field: a record carrying it was written
/// by `arb-trader`, anything else was written by somebody else.
pub const SOURCE: &str = "arb-trader";

/// How close in time two bookings of the same relationship-and-markets have to
/// be before the second one is treated as a possible double-book rather than a
/// second trade.
///
/// 300s because that is `arbbot-hedge.timer`'s period: the frozen Python hedger
/// wakes every 5 minutes, reads venue positions, and completes any PM-US short
/// it finds uncovered — which is the same naked leg this engine's own hedge
/// retry is chasing. One timer period is therefore the whole window in which
/// the two can be working the same leg without either knowing.
pub const CONTEST_WINDOW_S: f64 = 300.0;

/// A basket's identity INDEPENDENT of how its author spells leg sides.
///
/// This is the crux of catching a cross-writer double-book. The two writers
/// describe one position in two vocabularies: this engine records the ORDER it
/// sent (`polymarket_us side=ask` / `kalshi side=bid`), the Python hedger
/// records the POSITION that resulted (`kalshi side=yes` / `polymarket_us
/// side=no`). Leg order differs too. Everything that survives both spellings is
/// the relationship and the set of markets, so that is the key — and comparing
/// on anything richer would silently stop matching the very record it exists to
/// find.
fn identity(r: &Value) -> Option<(String, Vec<String>)> {
    let rel = rel_of(r)?.to_string();
    let mut markets: Vec<String> = r
        .get("legs")?
        .as_array()?
        .iter()
        .filter_map(|l| l.get("market_id").and_then(|v| v.as_str()).map(str::to_string))
        .collect();
    if markets.is_empty() {
        return None;
    }
    markets.sort();
    Some((rel, markets))
}

/// Every record in the file that parses, skipping the ones that do not.
///
/// Deliberately LENIENT where [`read`] is strict, and the difference is which
/// question is being asked. `read` gates ARMING: a line it cannot parse is
/// unknown exposure, so it refuses and nothing trades. This one gates a
/// duplicate check on a basket that has ALREADY traded — both legs are filled
/// and the money is spent — so refusing to write it because some unrelated line
/// is torn would lose the basket outright, which is the one outcome worse than
/// booking it twice. A missing file reads as empty for the same reason: the
/// first basket ever booked must not be blocked by the absence of the file it
/// is about to create.
fn parsed_records(path: &str) -> Vec<Value> {
    match std::fs::read_to_string(path) {
        Ok(text) => text.lines().filter_map(|l| serde_json::from_str::<Value>(l).ok()).collect(),
        Err(_) => Vec::new(),
    }
}

/// What happened to a basket offered to [`append_basket`].
#[derive(Debug, PartialEq)]
pub enum Booking {
    /// Written, and nothing else in the ledger describes this position.
    Booked,
    /// NOT written: `(relationship_id, ts)` is already in the file.
    AlreadyBooked,
    /// Written, but another author has an open basket on the same relationship
    /// and the same markets inside [`CONTEST_WINDOW_S`] — their `ts`.
    Contested(Vec<f64>),
}

/// Append a basket under the single-author rule.
///
/// 2026-07-30 is why this is not just [`append`]. Kalshi went
/// `409 trading_is_paused` for 31 minutes with a take-take's PM-US leg already
/// short one contract. This engine retried its hedge 336 times; the frozen
/// Python hedger, which reads VENUE POSITIONS and completes any uncovered PM-US
/// short, woke every 5 minutes and retried the same hedge. When the venue
/// reopened both got through 600ms apart, and the ledger took TWO complete
/// baskets for one PM-US leg — `n_open` 30 -> 32 for a position that had only
/// grown by one.
///
/// Two guards, because the two failures are not equally decidable:
///
///   * `(relationship_id, ts)` already present -> REFUSE. That pair is the key
///     `apply_corrections` matches `corrects_ts` on and `open_exposure` matches
///     `closes_ts` on, so a duplicate does not merely double-count: it makes
///     every future correction and unwind ambiguous about which record it
///     addresses. Nothing legitimate needs to write it twice.
///
///   * same identity, different author, inside the window -> WRITE ANYWAY, and
///     say so. Content cannot tell "one position booked twice" from "two real
///     fills seconds apart" — on 2026-07-30 it was the second, and both
///     contracts are still in the account — so suppressing here would delete
///     exposure on a guess. The record goes in, carries `contested_with_ts`
///     naming the other, and the caller alarms with the correction to append if
///     a human finds it really was one position.
pub fn append_basket(path: &str, mut rec: Value) -> Result<Booking, String> {
    let existing = parsed_records(path);

    if let Some(k) = key_of(rel_of(&rec), f64_of(&rec, "ts")) {
        if existing.iter().any(|r| key_of(rel_of(r), f64_of(r, "ts")) == Some(k.clone())) {
            return Ok(Booking::AlreadyBooked);
        }
    }

    let ts = f64_of(&rec, "ts");
    let contested: Vec<f64> = match (identity(&rec), ts) {
        (Some(id), Some(ts)) => existing
            .iter()
            .filter(|r| status_of(r) == "open")
            .filter(|r| r.get("source").and_then(|v| v.as_str()) != Some(SOURCE))
            .filter(|r| identity(r).as_ref() == Some(&id))
            .filter_map(|r| f64_of(r, "ts"))
            .filter(|t| (ts - t).abs() <= CONTEST_WINDOW_S)
            .collect(),
        _ => Vec::new(),
    };
    if !contested.is_empty() {
        if let Some(obj) = rec.as_object_mut() {
            obj.insert("contested_with_ts".to_string(), serde_json::json!(contested));
        }
    }

    append(path, &rec)?;
    Ok(if contested.is_empty() { Booking::Booked } else { Booking::Contested(contested) })
}

/// Append one money record ATOMICALLY.
///
/// One `write(2)` for the whole line. What this used to do — `writeln!(f,
/// "{rec}")` — serializes a `Value` token by token through the `io::Write`
/// shim: a ~600-byte basket left the process as ~60 separate writes, and a
/// crash between two of them (the unit runs under `MemoryMax=1G` and can be
/// OOM-killed) welds the NEXT record onto the stump. The result is one line
/// that parses as neither — two baskets destroyed by one crash.
/// `arb-tape/src/writer.rs` documents that exact damage in the tape, where it
/// cost one line in 377k; here every line is a position.
///
/// Then `fsync`. This is the record of money already spent — both legs have
/// filled by the time we are called — so the gap between the write and a
/// durable record is a window in which a host crash makes the position
/// invisible to every future exposure fold, freeing the caps against it and
/// leaving nothing to unwind it. A basket is a handful of records a day, not
/// 400k tape events, so durability here is bought at a price nothing notices.
pub fn append(path: &str, rec: &Value) -> Result<(), String> {
    use std::io::Write as _;
    // Serialize FIRST: nothing touches the file until the whole line exists.
    let mut line = serde_json::to_string(rec).map_err(|e| format!("serialize record: {e}"))?;
    line.push('\n');
    if let Some(dir) = std::path::Path::new(path).parent() {
        if !dir.as_os_str().is_empty() {
            std::fs::create_dir_all(dir).map_err(|e| format!("mkdir {}: {e}", dir.display()))?;
        }
    }
    let mut f = std::fs::OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)
        .map_err(|e| format!("open {path}: {e}"))?;
    heal_torn_tail(&mut f).map_err(|e| format!("heal {path}: {e}"))?;
    // O_APPEND: the write lands at the end whatever the read offset is now.
    f.write_all(line.as_bytes()).map_err(|e| format!("write {path}: {e}"))?;
    // sync_data, not sync_all: the appended bytes and the new size must be on
    // the platter; nobody depends on the mtime.
    f.sync_data().map_err(|e| format!("fsync {path}: {e}"))?;
    Ok(())
}

/// If the file does not end in a newline, add one before appending.
///
/// Same defence, and the same reasoning, as `arb_tape::writer::heal_torn_tail`
/// — duplicated rather than shared because that one is private to the tape
/// crate and hoisting it would touch files outside this change. A torn final
/// line stays ONE bad line (recoverable, and `read` refuses to arm on it)
/// instead of becoming permanent mid-file corruption that eats the next record
/// too.
fn heal_torn_tail(f: &mut std::fs::File) -> std::io::Result<()> {
    use std::io::{Read, Seek, SeekFrom, Write};
    let len = f.seek(SeekFrom::End(0))?;
    if len == 0 {
        return Ok(());
    }
    f.seek(SeekFrom::Start(len - 1))?;
    let mut last = [0u8; 1];
    f.read_exact(&mut last)?;
    if last[0] != b'\n' {
        f.write_all(b"\n")?;
    }
    Ok(())
}

/// What is actually wrong with a line, in the terms the REPAIR depends on.
///
/// The first version of this decided by position alone — last line without a
/// trailing newline meant "torn tail", anything else meant "two records may have
/// fused". That is wrong for every non-crash cause: a BOM, a bare `NaN` from
/// `json.dumps`, or a stump that a later heal had already newline-terminated all
/// reported a fusion, sending whoever is reading this at 3am to hunt for a
/// destroyed basket when the repair is deleting one byte.
///
/// So ask the parser instead of the line number. Unexpected EOF means the record
/// is incomplete. A complete value with content after it means two records in
/// one line. A syntax error exactly where another record begins is the welded
/// stump. Anything else is honestly just unparseable, quoted verbatim.
fn diagnose(l: &str) -> String {
    use serde::Deserialize as _;
    let mut de = serde_json::Deserializer::from_str(l);
    match Value::deserialize(&mut de) {
        Ok(_) => match de.end() {
            // A whole record, then more content: two records in one line.
            Err(e) => format!("TWO RECORDS FUSED — a complete record with content welded after it ({e})"),
            // Parsed on the retry. Only reachable if the file changed under us.
            Ok(()) => "parsed on retry — the file changed while being read".to_string(),
        },
        Err(e) if e.is_eof() => {
            format!("TRUNCATED — the record is incomplete, an append was interrupted ({e})")
        }
        Err(e) => {
            // Is a WHOLE record sitting inside this line? Then it is a welded
            // stump: a partial record with the next one written onto it, which
            // is the damage a non-atomic write leaves behind.
            //
            // The error column cannot answer this — `{` is legal inside a JSON
            // string, so `..."qt{"status":...` parses the fusion point INTO the
            // key (`qt{`) and reports the break two tokens later. So look for a
            // suffix that is itself a ledger record. Requiring
            // `relationship_id` is what keeps a merely truncated record's own
            // trailing `"fields":{...}` from reading as a second record; it
            // under-claims rather than over-claims, which is the direction that
            // does not send anyone hunting for a basket that still exists.
            if welded_record(l) {
                format!("TWO RECORDS FUSED — a partial record with the next welded into it ({e})")
            } else {
                format!("UNPARSEABLE — not a fusion; one bad byte or an out-of-spec number ({e})")
            }
        }
    }
}

/// Does a complete ledger record start part-way into this line? Only ever asked
/// of a line that already failed to parse, so the scan costs nothing that
/// matters.
fn welded_record(l: &str) -> bool {
    // Byte scan, and `get` rather than `[..]`: a line that fails to parse can
    // easily be one whose first bytes are a multi-byte BOM, and slicing off a
    // char boundary there would panic in the middle of a money path.
    let b = l.as_bytes();
    for at in 1..b.len().saturating_sub(1) {
        if b[at] != b'{' || b[at + 1] != b'"' {
            continue;
        }
        // There must be a record STUMP in front of it, not just a stray prefix:
        // a leading BOM on an otherwise perfect record would otherwise read as
        // a second record welded onto nothing.
        if !b[..at].contains(&b'{') {
            continue;
        }
        let Some(tail) = l.get(at..) else { continue };
        if let Ok(v) = serde_json::from_str::<Value>(tail) {
            if v.get("relationship_id").is_some() {
                return true;
            }
        }
    }
    false
}

/// Read the ledger, or say why it cannot be trusted.
///
/// A line that will not parse is UNKNOWN MONEY — an unknown size on unknown
/// markets — so it is counted, named, and the whole read FAILS.
/// `place_preconditions` already refuses to arm when this errors ("exposure is
/// unknown"), and that is exactly the right response to a lost basket:
/// returning a short book instead would silently understate the exposure the
/// caps are sized against.
///
/// What this used to do was `filter_map(..ok())`, justified by a reader racing
/// the live runner's concurrent append. That premise is gone: every writer now
/// emits one line in one `write(2)` (`append` above, and `ledgerdb.py`'s
/// `dual_append`), and a reader of a regular file cannot observe half of one.
/// A torn line therefore means a crash lost a record — not something to skip
/// past on the way to sizing new orders.
pub fn read(path: &str) -> Result<Vec<Value>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("read {path}: {e}"))?;
    let ends_clean = text.is_empty() || text.ends_with('\n');
    let total = text.lines().count();
    let mut out: Vec<Value> = Vec::new();
    let mut bad: Vec<String> = Vec::new();
    for (i, l) in text.lines().enumerate() {
        if l.trim().is_empty() {
            continue;
        }
        match serde_json::from_str::<Value>(l) {
            Ok(v) => out.push(v),
            Err(_) => bad.push(format!(
                "line {} of {} ({} bytes, {}): {}",
                i + 1,
                total,
                l.len(),
                if i + 1 == total && !ends_clean {
                    "no trailing newline"
                } else {
                    "newline-terminated"
                },
                diagnose(l),
            )),
        }
    }
    if !bad.is_empty() {
        return Err(format!(
            "{path}: {} of {} records UNREADABLE, so exposure is unknown — {}",
            bad.len(),
            out.len() + bad.len(),
            bad.join("; ")
        ));
    }
    Ok(out)
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
        assert!(!open_exposure(recs).contains_key("r1"));
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

    /// A scratch dir per test — tests share a process, so the pid alone is not
    /// a unique name.
    fn tmp(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("arb-ledger-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    /// A torn tail is UNKNOWN MONEY, and it is named rather than dropped.
    ///
    /// The old reader returned `Ok` with the surviving record and said nothing,
    /// so a crash that lost a basket looked exactly like a clean startup.
    #[test]
    fn a_torn_final_line_is_counted_not_silently_dropped() {
        let dir = tmp("torn");
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\
             \n{\"status\":\"open\",\"relationship_id\"",
        )
        .unwrap();
        let e = read(p.to_str().unwrap()).expect_err("a lost record must not read as success");
        assert!(e.contains("1 of 2 records UNREADABLE"), "{e}");
        assert!(e.contains("line 3 of 3"), "the bad line is named: {e}");
        assert!(e.contains("no trailing newline"), "with its position: {e}");
        assert!(e.contains("TRUNCATED"), "and diagnosed: {e}");
        assert!(e.contains("exposure is unknown"), "which is why it matters: {e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The diagnosis must earn the operator's trust: a stray byte is not a
    /// destroyed basket. Each of these used to be reported as a fusion purely
    /// because it was not the unterminated final line.
    #[test]
    fn the_diagnosis_names_the_actual_damage_not_its_position() {
        let full = r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#;

        // A byte-order mark: one byte to delete, nothing lost.
        let d = diagnose(&format!("\u{feff}{full}"));
        assert!(d.starts_with("UNPARSEABLE"), "{d}");

        // json.dumps' bare NaN, which serde_json refuses: a value to repair.
        let d = diagnose(r#"{"status":"open","qty":NaN}"#);
        assert!(d.starts_with("UNPARSEABLE"), "{d}");

        // A stump that a LATER heal newline-terminated: truncated, mid-file.
        let d = diagnose(r#"{"status":"open","relationship_id":"r2","ts":2.0,"qt"#);
        assert!(d.starts_with("TRUNCATED"), "{d}");

        // The real C7 damage: a stump with the next record welded into it.
        let d = diagnose(&format!(r#"{{"status":"open","ts":2.0,"qt{full}"#));
        assert!(d.starts_with("TWO RECORDS FUSED"), "{d}");

        // And two whole records welded by a lost newline.
        let d = diagnose(&format!("{full}{full}"));
        assert!(d.starts_with("TWO RECORDS FUSED"), "{d}");
    }

    /// A stump the Python writers weld onto. `arbbot-hedge.timer` fires every
    /// 5 minutes into `dual_append`, so a Rust stump is MORE likely to be
    /// followed by a Python append than a Rust one — which is why `dual_append`
    /// now heals too (src/arbbot/exec/ledgerdb.py). This pins what the reader
    /// says if some other unhealed writer ever welds again: fusion, named as
    /// fusion, and no arming.
    #[test]
    fn a_welded_pair_is_reported_as_a_fusion_wherever_it_sits() {
        let dir = tmp("welded");
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\
             {\"status\":\"open\",\"relationship_id\":\"r2\",\"ts\":2.0,\"qt\
             {\"status\":\"open\",\"relationship_id\":\"r3\",\"ts\":3.0,\"qty\":7}\n",
        )
        .unwrap();
        let e = read(p.to_str().unwrap()).expect_err("two lost baskets");
        assert!(e.contains("TWO RECORDS FUSED"), "{e}");
        assert!(e.contains("newline-terminated"), "position is context, not diagnosis: {e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The C7 damage itself: two records fused by a non-atomic write. It parses
    /// as neither, is not at the tail, and must never be skipped past — this is
    /// two baskets, not one.
    #[test]
    fn two_fused_records_are_refused_not_skipped() {
        let dir = tmp("fused");
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\
             {\"status\":\"open\",\"relationship_id\":\"r2\",\"ts\":2.0,\"q{\"status\":\"open\",\
             \"relationship_id\":\"r3\",\"ts\":3.0,\"qty\":7}\n",
        )
        .unwrap();
        let e = read(p.to_str().unwrap()).expect_err("fused records must not read as success");
        assert!(e.contains("line 2"), "{e}");
        assert!(e.contains("TWO RECORDS FUSED"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A clean file still reads clean — the strict reader must not cry wolf on
    /// blank lines or a trailing newline.
    #[test]
    fn a_clean_ledger_reads_without_complaint() {
        let dir = tmp("clean");
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\n\
             {\"status\":\"open\",\"relationship_id\":\"r2\",\"ts\":2.0,\"qty\":7}\n",
        )
        .unwrap();
        let recs = read(p.to_str().unwrap()).expect("clean");
        assert_eq!(recs.len(), 2);
        let e = open_exposure(recs);
        assert_eq!(e.get("r1"), Some(&50.0));
        assert_eq!(e.get("r2"), Some(&7.0));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// One record, one line, compact — byte-identical to what `dual_append`
    /// writes from Python, and to a single `write(2)`.
    #[test]
    fn append_writes_exactly_one_compact_line() {
        let dir = tmp("append");
        let p = dir.join("trades.jsonl");
        let a = v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#);
        let b = v(r#"{"status":"open","relationship_id":"r2","ts":2.0,"qty":7}"#);
        append(p.to_str().unwrap(), &a).expect("append a");
        append(p.to_str().unwrap(), &b).expect("append b");
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(
            text,
            format!(
                "{}\n{}\n",
                serde_json::to_string(&a).unwrap(),
                serde_json::to_string(&b).unwrap()
            )
        );
        assert_eq!(read(p.to_str().unwrap()).unwrap().len(), 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An interrupted write, constructed byte-for-byte: the previous process
    /// died having emitted part of a record and no newline. The NEXT record
    /// must land as its own line — under the old `writeln!` path it was welded
    /// onto the stump and both baskets were gone.
    #[test]
    fn a_record_survives_an_interrupted_previous_write() {
        let dir = tmp("interrupted");
        let p = dir.join("trades.jsonl");
        std::fs::write(
            &p,
            "{\"status\":\"open\",\"relationship_id\":\"r1\",\"ts\":1.0,\"qty\":50}\n\
             {\"status\":\"open\",\"relationship_id\":\"r2\",\"ts\":2.0,\"qt",
        )
        .unwrap();
        let survivor = v(r#"{"status":"open","relationship_id":"r3","ts":3.0,"qty":7}"#);
        append(p.to_str().unwrap(), &survivor).expect("append");

        let text = std::fs::read_to_string(&p).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 3, "the stump must not eat the new record: {text:?}");
        assert_eq!(lines[2], serde_json::to_string(&survivor).unwrap());
        // r1 and r3 are intact and recoverable; exactly ONE line is unknown.
        let e = read(p.to_str().unwrap()).expect_err("the stump is still unknown money");
        assert!(e.contains("1 of 3 records UNREADABLE"), "{e}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Healing an already-clean file adds nothing.
    #[test]
    fn append_does_not_insert_a_spurious_blank_line() {
        let dir = tmp("noblank");
        let p = dir.join("trades.jsonl");
        let a = v(r#"{"status":"open","relationship_id":"r1","ts":1.0,"qty":50}"#);
        append(p.to_str().unwrap(), &a).expect("first");
        append(p.to_str().unwrap(), &a).expect("second");
        let text = std::fs::read_to_string(&p).unwrap();
        assert!(!text.contains("\n\n"), "{text:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A write that cannot happen must be reported, not swallowed: the caller
    /// crash-stops on it (engine.rs `book_basket`) rather than trading on.
    #[test]
    fn a_failed_append_is_an_error_not_a_shrug() {
        // /proc exists and is not writable by anyone, so the open fails while
        // the path itself is perfectly well-formed.
        let e = append("/proc/trades.jsonl", &v(r#"{"status":"open"}"#))
            .expect_err("must not report success");
        assert!(e.contains("open /proc/trades.jsonl"), "{e}");
    }

    /// The 2026-07-30 Kuleba pair, byte-for-byte off the live ledger: the
    /// Python hedger's record (position sides `yes`/`no`, Kalshi leg first, no
    /// `source`) and this engine's (order sides `bid`/`ask`, PM-US leg first).
    /// They are the two spellings `identity` has to see through.
    fn kuleba_python() -> Value {
        v(r#"{"ts":1785402005.539014,"relationship_id":"xvus-nobel-peace-26-mykolakuleba",
            "title":"xvus-nobel-peace-26-mykolakuleba (naked-leg hedge)","qty":1,
            "strategy":"take-take","status":"open","cost_usd":0.72,"profit_usd":0.28,
            "legs":[{"venue":"kalshi","market_id":"KXNOBELPEACE-27-MKUL","side":"yes",
                     "role":"taker","qty":1,"yes_price":"0.0800"},
                    {"venue":"polymarket_us","market_id":"tac-nobel-peace-2026-10-09-mykkul",
                     "side":"no","role":"taker","qty":1,"yes_price":"0.36"}]}"#)
    }

    fn kuleba_engine(ts: f64) -> Value {
        let mut r = v(r#"{"relationship_id":"xvus-nobel-peace-26-mykolakuleba","qty":1,
            "strategy":"take-take","status":"open","source":"arb-trader","fees_pending":true,
            "legs":[{"venue":"polymarket_us","market_id":"tac-nobel-peace-2026-10-09-mykkul",
                     "side":"ask","role":"taker","qty":1,"yes_price":"0.3600"},
                    {"venue":"kalshi","market_id":"KXNOBELPEACE-27-MKUL","side":"bid",
                     "role":"taker","qty":1,"yes_price":"0.0800"}]}"#);
        r.as_object_mut().unwrap().insert("ts".into(), serde_json::json!(ts));
        r
    }

    /// THE DEFECT. Two writers, two vocabularies, one relationship, 1.7s apart —
    /// and `open_exposure` counted two baskets for a leg that had grown by one.
    /// Matching on anything that includes leg SIDE or leg ORDER misses this.
    #[test]
    fn the_python_hedgers_spelling_of_the_same_basket_is_recognised() {
        let dir = tmp("contested");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();
        append(path, &kuleba_python()).expect("seed the other writer's record");

        let out = append_basket(path, kuleba_engine(1785402003.8191998)).expect("book");
        assert_eq!(
            out,
            Booking::Contested(vec![1785402005.539014]),
            "the other author's booking must be named, not missed"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A contested basket is STILL WRITTEN. On 2026-07-30 both fills were real —
    /// the engine's own hedge and the Python hedger's IOC both traded when
    /// Kalshi came back from `trading_is_paused` — so suppressing the second
    /// record would have deleted a contract the account actually holds. The
    /// record goes in, carries the pointer, and the caller alarms.
    #[test]
    fn a_contested_basket_is_written_and_still_counts_as_exposure() {
        let dir = tmp("contested-written");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();
        append(path, &kuleba_python()).expect("seed");
        append_basket(path, kuleba_engine(1785402003.8191998)).expect("book");

        let recs = read(path).expect("clean");
        assert_eq!(recs.len(), 2, "nothing was suppressed");
        let ours = recs.iter().find(|r| r["source"] == "arb-trader").expect("ours");
        assert_eq!(
            ours["contested_with_ts"],
            serde_json::json!([1785402005.539014]),
            "the record names what it may be a duplicate of"
        );
        assert_eq!(
            open_exposure(recs).get("xvus-nobel-peace-26-mykolakuleba"),
            Some(&2.0),
            "both contracts stay visible to the caps until a human decides"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `(relationship_id, ts)` is what `corrects_ts` and `closes_ts` address a
    /// basket by. Two records under one key make every future correction and
    /// unwind ambiguous, so the second is refused outright.
    #[test]
    fn the_same_key_is_never_written_twice() {
        let dir = tmp("samekey");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();
        assert_eq!(append_basket(path, kuleba_engine(1.0)).unwrap(), Booking::Booked);
        assert_eq!(append_basket(path, kuleba_engine(1.0)).unwrap(), Booking::AlreadyBooked);
        assert_eq!(read(path).unwrap().len(), 1, "the replay wrote nothing");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Our OWN earlier basket is not a contest. A hedge obligation can fill in
    /// two parts and book twice, and the maker path books a fresh basket every
    /// time a quote fills — flagging those would cry wolf on ordinary trading.
    #[test]
    fn our_own_earlier_basket_is_not_a_contest() {
        let dir = tmp("ourown");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();
        assert_eq!(append_basket(path, kuleba_engine(1.0)).unwrap(), Booking::Booked);
        assert_eq!(append_basket(path, kuleba_engine(2.0)).unwrap(), Booking::Booked);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Different markets, and an older booking on the same markets, are both
    /// ordinary. The window is one `arbbot-hedge.timer` period because that is
    /// how long the two hedgers can be working the same leg unaware.
    #[test]
    fn a_different_basket_or_an_old_one_is_not_a_contest() {
        let dir = tmp("nocontest");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();

        let mut elsewhere = kuleba_python();
        elsewhere.as_object_mut().unwrap().insert("relationship_id".into(), "other".into());
        append(path, &elsewhere).expect("seed");
        assert_eq!(
            append_basket(path, kuleba_engine(1785402003.8191998)).unwrap(),
            Booking::Booked,
            "another relationship is another position"
        );

        let dir2 = tmp("nocontest-old");
        let p2 = dir2.join("trades.jsonl");
        let path2 = p2.to_str().unwrap();
        append(path2, &kuleba_python()).expect("seed");
        assert_eq!(
            append_basket(path2, kuleba_engine(1785402005.539014 + CONTEST_WINDOW_S + 1.0))
                .unwrap(),
            Booking::Booked,
            "past the window it is a second trade, not a double-book"
        );
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(&dir2);
    }

    /// The duplicate check must FAIL OPEN. By the time `book_basket` is called
    /// both legs have filled and the money is spent, so refusing to write it
    /// because some unrelated line is torn would lose the basket outright —
    /// which is the one outcome worse than booking it twice.
    #[test]
    fn a_torn_line_elsewhere_does_not_block_the_book() {
        let dir = tmp("failopen");
        let p = dir.join("trades.jsonl");
        let path = p.to_str().unwrap();
        std::fs::write(&p, "{\"status\":\"open\",\"relationship_id\"\n").unwrap();
        assert_eq!(append_basket(path, kuleba_engine(1.0)).unwrap(), Booking::Booked);
        let text = std::fs::read_to_string(&p).unwrap();
        assert_eq!(text.lines().count(), 2, "the basket was written: {text:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
