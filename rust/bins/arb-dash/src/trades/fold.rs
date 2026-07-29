//! Folding the append-only ledger into "what is actually on".
//!
//! The three steps named in the module header live here: corrections merged
//! into their targets, `superseded` lines dropped, and unwinds netted against
//! the `open` records their `closes_ts` names. Everything the fold NOTICED —
//! a retraction that reached nothing, an unwind that closes nothing — is
//! carried out on `Ledger` rather than swallowed, because each one leaves a
//! wrong number on a screen a human acts on.

use std::collections::{HashMap, HashSet};

use super::{num, rel_of, status_of, EPS};

/// Open baskets and the unwinds that close them are matched on
/// `(relationship_id, ts)` with ts by bit pattern — the same predicate
/// `arb-trader/src/ledger.rs` uses, so the trader and this tab fold identically.
///
/// CORRECTIONS ARE NOT KEYED THIS WAY. See `apply_corrections`: requiring a
/// `relationship_id` there silently discarded two retractions worth $7.37.
type Key = (String, u64);

fn key_of(rel: Option<&str>, ts: Option<f64>) -> Option<Key> {
    Some((rel?.to_string(), ts?.to_bits()))
}

/// The ledger after the fold: the records that survive, plus everything the
/// fold could not reconcile. The counters are reported, never absorbed.
pub struct Ledger {
    pub records: Vec<serde_json::Value>,
    unwound_qty: HashMap<Key, f64>,
    unusable: HashSet<Key>,
    pub unparsed: u64,
    pub corrections: u64,
    pub corrections_applied: u64,
    pub corrections_unmatched: u64,
    pub superseded: u64,
    pub orphan_unwinds: u64,
}

impl Ledger {
    /// Parse the ledger text and run all three fold steps over it.
    pub fn fold(ledger_text: &str) -> Ledger {
        let mut unparsed = 0u64;
        let mut records: Vec<serde_json::Value> = Vec::new();
        for line in ledger_text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            match serde_json::from_str::<serde_json::Value>(line) {
                Ok(r) => records.push(r),
                // A torn tail line is normal: the engine appends while we read.
                Err(_) => unparsed += 1,
            }
        }

        let (corrections, corrections_applied, corrections_unmatched) =
            apply_corrections(&mut records);
        let superseded = records.iter().filter(|r| status_of(r) == "superseded").count() as u64;
        records.retain(|r| status_of(r) != "superseded");
        let (unwound_qty, orphan_unwinds, unusable) = unwind_index(&records);

        Ledger {
            records,
            unwound_qty,
            unusable,
            unparsed,
            corrections,
            corrections_applied,
            corrections_unmatched,
            superseded,
            orphan_unwinds,
        }
    }

    /// An unwind that names nothing it can reduce is reported, not matched.
    pub fn unusable_unwinds(&self) -> usize {
        self.unusable.len()
    }

    /// Steps 2 and 3 of the fold, applied to ONE record: how much of this
    /// basket is still on, and what its status folds to.
    pub fn remainder(
        &self,
        rel_id: &str,
        ts: f64,
        ledger_status: &str,
        qty_booked: f64,
    ) -> Remainder {
        let is_open = ledger_status == "open";
        let own_key = if is_open { key_of(Some(rel_id), Some(ts)) } else { None };
        let closed_qty =
            own_key.as_ref().and_then(|k| self.unwound_qty.get(k).copied()).unwrap_or(0.0);
        // An unwind named this basket but recorded no usable qty, so how much
        // came off is unknowable. Reporting the basket as fully on would count
        // it once here and again on the unwind row.
        let unknown = own_key.as_ref().is_some_and(|k| self.unusable.contains(k));
        let remaining = if closed_qty > 0.0 { qty_booked - closed_qty } else { qty_booked };
        let fully_closed = is_open && closed_qty > 0.0 && remaining <= EPS;
        // The displayed status is the FOLDED one. A line still reading `open`
        // that a later record already unwound is the double count itself.
        let status = if fully_closed { "closed".to_string() } else { ledger_status.to_string() };
        // A partial unwind returned part of the capital and banked part of the
        // profit on the unwind row. What is left is pro-rata: the cost basis is
        // an average, so scaling is exact.
        let scale = if is_open && qty_booked > 0.0 {
            (remaining / qty_booked).clamp(0.0, 1.0)
        } else {
            1.0
        };
        Remainder { is_open, status, qty: remaining, scale, unknown, fully_closed }
    }
}

/// What the fold leaves of one basket.
pub struct Remainder {
    /// The LEDGER said `open`. Not the same as `status == "open"`: a basket a
    /// later record fully unwound is open on disk and closed here.
    pub is_open: bool,
    /// The FOLDED status — what the tab displays.
    pub status: String,
    /// Contracts still working, after any partial unwind.
    pub qty: f64,
    /// The fraction of the booked basket `qty` represents. Cost, payoff and
    /// net are all pro-rated by it.
    pub scale: f64,
    /// An unwind claimed this basket but said nothing usable about how much.
    pub unknown: bool,
    pub fully_closed: bool,
}

/// Step 1 of the fold: shallow-merge each `correction`'s `fields` into the
/// record its `corrects_ts` names, then drop the corrections. A correction is
/// not a no-op — it is the ledger retracting a value, so ignoring it leaves the
/// superseded number standing.
///
/// TARGETED BY `corrects_ts`, NOT BY `(relationship_id, corrects_ts)`. Two of
/// the 45 corrections on disk carry NO `relationship_id`, and keying on the pair
/// discarded both (2026-07-28 audit) — they retract `realized_pnl_usd +2.6369`
/// to `-0.2348` and `+4.1308` to `-0.3660`, so `realized_net_usd` read $7.3685
/// too high. Their own notes quote the prior value ("CORRECTED from +2.6369"),
/// which matches the target's field exactly, so the intent was never in doubt.
/// `ts` is unique across the file, so `corrects_ts` identifies one record.
///
/// `arb-trader/src/ledger.rs` has the same pair-keying and is UNHARMED by it: it
/// folds for open exposure and never reads `realized_pnl_usd`, while both these
/// corrections amend records that are already `unwound`. Parity with the trader
/// is silent here, so the two folds legitimately differ on this point.
///
/// A correction that DOES name a relationship must name the target's own, and a
/// nameless correction is applied only when exactly one record carries that ts —
/// an ambiguous retraction is dropped and reported rather than guessed.
///
/// Returns `(corrections seen, records amended, corrections that reached nothing)`.
fn apply_corrections(records: &mut Vec<serde_json::Value>) -> (u64, u64, u64) {
    // (target ts bits, relationship_id IF the correction named one, fields).
    // A Vec, not a map: file order is the precedence order, and three
    // corrections target one fedcut record on disk where last-wins is correct.
    let mut fixes: Vec<(u64, Option<String>, serde_json::Map<String, serde_json::Value>)> =
        Vec::new();
    let mut corrections = 0u64;
    for r in records.iter() {
        if status_of(r) != "correction" {
            continue;
        }
        corrections += 1;
        let Some(ts) = num(r.get("corrects_ts")) else { continue };
        let Some(f) = r.get("fields").and_then(|v| v.as_object()) else { continue };
        fixes.push((ts.to_bits(), rel_of(r).map(str::to_string), f.clone()));
    }
    records.retain(|r| status_of(r) != "correction");
    if fixes.is_empty() {
        return (corrections, 0, corrections);
    }
    let mut per_ts: HashMap<u64, usize> = HashMap::new();
    for r in records.iter() {
        if let Some(ts) = num(r.get("ts")) {
            *per_ts.entry(ts.to_bits()).or_default() += 1;
        }
    }

    let mut used = vec![false; fixes.len()];
    let mut amended = 0u64;
    for r in records.iter_mut() {
        // Read everything needed off `r` first so nothing borrows it when it is
        // mutated below.
        let Some(ts) = num(r.get("ts")).map(f64::to_bits) else { continue };
        let rel = rel_of(r).map(str::to_string);
        let unique_ts = per_ts.get(&ts).copied().unwrap_or(0) == 1;
        let mut hit = false;
        for (i, (fts, frel, f)) in fixes.iter().enumerate() {
            if *fts != ts {
                continue;
            }
            match frel {
                // Named: it must name THIS relationship.
                Some(fr) if Some(fr.as_str()) != rel.as_deref() => continue,
                // Nameless: only safe while the ts picks out one record.
                None if !unique_ts => continue,
                _ => {}
            }
            let Some(obj) = r.as_object_mut() else { continue };
            for (kk, vv) in f {
                obj.insert(kk.clone(), vv.clone());
            }
            used[i] = true;
            hit = true;
        }
        if hit {
            amended += 1;
        }
    }
    let unmatched = corrections - used.iter().filter(|u| **u).count() as u64;
    (corrections, amended, unmatched)
}

/// Step 2 of the fold: unwound quantity per `open` record, keyed by the
/// `closes_ts` the unwind names.
///
/// Three ways an unwind can fail to close what it claims, all reported rather
/// than absorbed, because each one leaves a closed basket on the open book AND
/// books its P&L on the unwind row — a silent double count:
///   * no `closes_ts`, or one that names nothing (2 on disk today)
///   * a `closes_ts` that names a record which is not `open` — it can never
///     reduce anything, so it is an orphan, not a match
///   * a missing or non-positive `qty`, which would leave the basket fully open.
///     The target's remaining exposure is then unknowable, so it is returned in
///     `unusable` and the target prices as `null` instead of as fully on.
fn unwind_index(records: &[serde_json::Value]) -> (HashMap<Key, f64>, u64, HashSet<Key>) {
    let open_keys: HashSet<Key> = records
        .iter()
        .filter(|r| status_of(r) == "open")
        .filter_map(|r| key_of(rel_of(r), num(r.get("ts"))))
        .collect();
    let mut closed: HashMap<Key, f64> = HashMap::new();
    let mut orphans = 0u64;
    let mut unusable: HashSet<Key> = HashSet::new();
    for r in records {
        if status_of(r) != "unwound" {
            continue;
        }
        match key_of(rel_of(r), num(r.get("closes_ts"))) {
            Some(k) if open_keys.contains(&k) => match num(r.get("qty")) {
                Some(q) if q.is_finite() && q > 0.0 => *closed.entry(k).or_default() += q,
                _ => {
                    unusable.insert(k);
                }
            },
            _ => orphans += 1,
        }
    }
    (closed, orphans, unusable)
}
