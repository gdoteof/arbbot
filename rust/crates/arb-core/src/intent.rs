//! What the decision core DECIDED, as a type rather than as a string.
//!
//! The quoter used to build `json!({...}).to_string()` and push it into a
//! `Vec<String>`; the trader parsed each line straight back with
//! `serde_json::from_str` and re-read every field through
//! `.get("x").and_then(|v| v.as_str()).unwrap_or("")`. Forty-nine of those
//! chains stood between a decision and the order it became, and each one is the
//! shape the 2026-07-28 audit named as needing types rather than lints: a
//! missing price silently became `"0"`, a missing side silently became a BID,
//! and every id was a `&str` interchangeable with every other id. None of that
//! is representable here — `price`, `count` and `order_id` are required fields
//! of the variant that has them, so a place that lacks one does not compile,
//! and `side` is a `BookSide`, so a place whose side is neither bid nor ask
//! cannot be BUILT and cannot be PARSED. The first cut of this enum left `side`
//! a `String` and the trader still ended `if p.side == "ask" { Ask } else
//! { Bid }`: the value could no longer be missing, but "asl" was still a buy.
//!
//! The JSON is now produced exactly once, at the write boundary
//! (`Intent::to_line`), and never re-parsed by the engine.
//!
//! # The emitted bytes are a contract
//!
//! These lines are appended to `data/trader-rs/intents.jsonl`, folded by
//! `arb_query::intents` for the dashboard, and hashed into the golden digest
//! that every refactor in this repo has to reproduce. One byte of difference is
//! a changed decision until proven otherwise.
//!
//! Two rules follow, and both are load-bearing:
//!
//! 1. **Fields are DECLARED in alphabetical order**, because that is the order
//!    they are emitted in. `serde_json`'s object is a `BTreeMap` (the
//!    `preserve_order` feature is not enabled), so the `json!` macro these
//!    structs replaced sorted its keys on the way out. A `#[derive(Serialize)]`
//!    struct emits DECLARATION order instead, so the declaration is the sort.
//!    A field inserted in the wrong place changes the line; the round-trip
//!    tests below re-serialise verbatim production lines and compare bytes,
//!    which is what catches it.
//! 2. **An absent key stays absent.** The key SET varies per variant and per
//!    case — a plain place carries seven keys, a reprice nine, a hedge retry
//!    ten — so every optional field is skipped when empty rather than emitted
//!    as `null`. `arb_query::intents::fold` reads `place`/`cancel`/`skip` by
//!    presence, so a `"cancel":null` on a place would make it a cancel.
//!
//! The struct field names are the WIRE key names — `place` and `cancel` hold a
//! market id, which reads oddly — precisely so that rule 1 can be checked by
//! eye against the declaration.

use crate::model::{BookSide, Venue};
use serde::{Deserialize, Serialize};

/// One decision. Serialises to the object it always was; the variant is chosen
/// on deserialisation by which discriminator key is present (`place`,
/// `cancel`, `hedge_needed`, `skip`), which is why this is `untagged` — the
/// wire has no tag field and adding one would change the bytes.
///
/// The four key sets are disjoint (each variant requires a key no other variant
/// has), so variant ORDER here carries no meaning and no ambiguity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Intent {
    Place(Place),
    Cancel(Cancel),
    HedgeNeeded(HedgeNeeded),
    Skip(Skip),
}

/// Which strategy asked for an order. Absent on an ordinary maker quote.
///
/// Read by the trader to decide two things a `&str` comparison used to decide:
/// whether the order gets a hedge of its own (a hedge must NOT, or its fill
/// mints another hedge), and which strategy the ledger books the basket under.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tag {
    #[serde(rename = "hedge")]
    Hedge,
    #[serde(rename = "take-take")]
    TakeTake,
}

/// Rest (or cross) an order. Fields are in wire order — see the module note.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Place {
    pub count: i64,
    /// Present on a reprice: the price the replaced order rested at, so the
    /// dashboard can chart the move rather than just the destination.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub old_price: Option<String>,
    pub order_id: String,
    /// The market id. The KEY `place` is what makes this line a place.
    pub place: String,
    pub price: String,
    /// The order this one supersedes. An amend is ONE intent carrying
    /// `replaces`, and the trader turns it into a cancel AND a place, cancel
    /// first (see `engine::cancel::intent_actions`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replaces: Option<String>,
    /// Which hedge attempt this is, from the second on. Absent on a first
    /// attempt, which is what makes the retries visible in the tape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry: Option<u32>,
    pub side: BookSide,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tag: Option<Tag>,
    /// A marketable IOC rather than a resting post-only quote. Absent means
    /// false on the wire, so it is skipped when false rather than emitted.
    #[serde(default, skip_serializing_if = "is_false")]
    pub taker: bool,
    pub ts: f64,
    pub venue: Venue,
}

/// Pull a resting order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Cancel {
    /// The market id. The KEY `cancel` is what makes this line a cancel.
    pub cancel: String,
    pub order_id: String,
    pub price: String,
    pub side: BookSide,
    pub ts: f64,
    pub venue: Venue,
}

/// A maker fill has minted a hedge obligation. A record, not an order — the
/// place that discharges it is a separate `Place` carrying `Tag::Hedge`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HedgeNeeded {
    pub anchor_price: String,
    /// The market id the hedge is owed on.
    pub hedge_needed: String,
    pub order_id: String,
    pub qty: i64,
    pub ts: f64,
}

/// A decision NOT to trade, and why. The commonest line by far (784,344 of the
/// 843,426 on disk on 2026-07-28), and it is liveness evidence as much as a
/// record: an engine that is skipping is an engine that is running.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Skip {
    pub skip: Vec<String>,
    pub ts: f64,
}

#[allow(clippy::trivially_copy_pass_by_ref)]
fn is_false(b: &bool) -> bool {
    !*b
}

impl Intent {
    /// The one place an intent becomes JSON. Every consumer downstream of the
    /// engine reads the FILE; nothing inside the engine parses this back.
    ///
    /// Infallible in practice: every field is a string, a number, a bool or a
    /// `Vec<String>`, so there is no map-with-non-string-keys and no non-finite
    /// float for `serde_json` to refuse.
    pub fn to_line(&self) -> String {
        serde_json::to_string(self).expect("an Intent is always representable as JSON")
    }

    /// The venue whose executor owns the effect, or `None` for the two variants
    /// that are records rather than orders. Read at the dispatch site so the
    /// venue the command is SENT to and the venue the command is BUILT from
    /// cannot drift apart.
    pub fn venue(&self) -> Option<Venue> {
        match self {
            Intent::Place(p) => Some(p.venue),
            Intent::Cancel(c) => Some(c.venue),
            Intent::HedgeNeeded(_) | Intent::Skip(_) => None,
        }
    }

    /// Parse one line back. Used by the tests that pin the wire format and by
    /// anything reading an intents file after the fact; the engine does not.
    pub fn from_line(s: &str) -> Result<Intent, serde_json::Error> {
        serde_json::from_str(s)
    }
}

/// The wire format, pinned against lines this engine really wrote.
///
/// Every fixture below is verbatim from `data/trader-rs/*.jsonl` — copied, not
/// composed — and together they are ALL SIX key sets present in the 843,426
/// intent lines on disk on 2026-07-28. Parse, re-serialise, compare BYTES: that
/// catches a field declared out of alphabetical order, an optional field
/// emitted as `null`, and a float that stopped printing its `.0`, none of which
/// a field-by-field assertion would.
#[cfg(test)]
mod wire_format_tests {
    use super::*;

    /// The six shapes, in descending order of how many lines carry each.
    const REAL_LINES: &[&str] = &[
        // skip — 784,344 lines
        r#"{"skip":["topic [other] gated to util<0.5: at 0.94"],"ts":1785200862.1322198}"#,
        // reprice — 38,893 lines
        r#"{"count":5,"old_price":"0.27","order_id":"m138","place":"tpoyc-2026-daramo","price":"0.26","replaces":"m84","side":"ask","ts":1785179830.118253,"venue":"polymarket_us"}"#,
        // plain place — 10,659 lines
        r#"{"count":5,"order_id":"m1","place":"cpc-btc-150k-08-31-2026","price":"0.02","side":"ask","ts":1785178914.2950244,"venue":"polymarket_us"}"#,
        // cancel — 9,518 lines. The market id is a PM CTF token id, and the
        // venue is the one venue we no longer trade: both must still parse.
        r#"{"cancel":"107505882767731489358349912513945399560393482969656700824895970500493757150417","order_id":"m131","price":"0.05","side":"bid","ts":1785179898.967656,"venue":"polymarket"}"#,
        // take-take place — 8 lines, all from the armed M3 run
        r#"{"count":5,"order_id":"t1","place":"tac-nobel-peace-2026-10-09-dontru","price":"0.0800","side":"ask","tag":"take-take","taker":true,"ts":1785257179.783811,"venue":"polymarket_us"}"#,
        // hedge_needed — 4 lines
        r#"{"anchor_price":"0.0400","hedge_needed":"KXNOBELPEACE-26-DJT","order_id":"t1","qty":5,"ts":1785257180.1629453}"#,
    ];

    #[test]
    fn every_real_shape_round_trips_byte_for_byte() {
        for line in REAL_LINES {
            let it = Intent::from_line(line)
                .unwrap_or_else(|e| panic!("no variant matched a line this engine wrote: {e}\n{line}"));
            assert_eq!(&it.to_line(), line, "re-serialised bytes must be identical");
        }
    }

    /// A hedge retry is the tenth-key case and the only one that carries
    /// `retry`. Composed rather than copied — no retry has fired live yet — so
    /// it is the one shape the corpus cannot vouch for; the ordering rule is
    /// what it pins (`replaces` < `retry` < `side`).
    #[test]
    fn a_hedge_retry_puts_retry_between_replaces_and_side() {
        let line = r#"{"count":5,"order_id":"h2","place":"KXTEST","price":"0.30","retry":2,"side":"bid","tag":"hedge","taker":true,"ts":1.0,"venue":"kalshi"}"#;
        let it = Intent::from_line(line).expect("parse");
        assert_eq!(it.to_line(), line);
    }

    /// The key SET is the discriminator, so an absent field must not become
    /// `null`: `arb_query::intents::fold` treats any present `cancel` as a
    /// cancel, and would read a place carrying `"cancel":null` as one.
    #[test]
    fn absent_fields_stay_absent_rather_than_null() {
        let place = Intent::Place(Place {
            count: 5,
            old_price: None,
            order_id: "m1".into(),
            place: "M".into(),
            price: "0.10".into(),
            replaces: None,
            retry: None,
            side: BookSide::Bid,
            tag: None,
            taker: false,
            ts: 1.0,
            venue: Venue::Kalshi,
        });
        let line = place.to_line();
        assert!(!line.contains("null"), "{line}");
        assert!(!line.contains("taker"), "false is absent on the wire: {line}");
        assert_eq!(
            line,
            r#"{"count":5,"order_id":"m1","place":"M","price":"0.10","side":"bid","ts":1.0,"venue":"kalshi"}"#
        );
    }

    /// Keys are emitted sorted because they are DECLARED sorted. This asserts
    /// the property directly, on a fully-populated value of every variant, so a
    /// field added in the wrong position fails here and not only in the golden
    /// digest four steps later.
    #[test]
    fn every_variant_emits_its_keys_in_sorted_order() {
        let all = [
            Intent::Place(Place {
                count: 5,
                old_price: Some("0.09".into()),
                order_id: "m2".into(),
                place: "M".into(),
                price: "0.10".into(),
                replaces: Some("m1".into()),
                retry: Some(3),
                side: BookSide::Bid,
                tag: Some(Tag::Hedge),
                taker: true,
                ts: 1.0,
                venue: Venue::Kalshi,
            }),
            Intent::Cancel(Cancel {
                cancel: "M".into(),
                order_id: "m1".into(),
                price: "0.10".into(),
                side: BookSide::Bid,
                ts: 1.0,
                venue: Venue::PolymarketUs,
            }),
            Intent::HedgeNeeded(HedgeNeeded {
                anchor_price: "0.44".into(),
                hedge_needed: "M".into(),
                order_id: "m1".into(),
                qty: 5,
                ts: 1.0,
            }),
            Intent::Skip(Skip { skip: vec!["why".into()], ts: 1.0 }),
        ];
        for it in &all {
            let line = it.to_line();
            let v: serde_json::Value = serde_json::from_str(&line).expect("valid json");
            let keys: Vec<&String> = v.as_object().expect("an object").keys().collect();
            let mut sorted = keys.clone();
            sorted.sort();
            assert_eq!(keys, sorted, "declaration order is the wire order: {line}");
        }
    }

    /// `ts` is the engine's tape clock and the dashboard's liveness signal. An
    /// integral one has to keep printing `.0` — Python's `json.dumps` did, the
    /// `json!` macro did, and a line that lost it would differ by a byte.
    #[test]
    fn an_integral_timestamp_keeps_its_trailing_zero() {
        let it = Intent::Skip(Skip { skip: vec!["x".into()], ts: 1785257814.0 });
        assert_eq!(it.to_line(), r#"{"skip":["x"],"ts":1785257814.0}"#);
    }

    /// The place/cancel/skip/hedge_needed key is the discriminator, and getting
    /// it wrong sends an order the engine meant to pull (or pulls one it meant
    /// to send).
    #[test]
    fn each_shape_deserialises_to_the_variant_its_keys_name() {
        let got: Vec<&str> = REAL_LINES
            .iter()
            .map(|l| match Intent::from_line(l).expect("parse") {
                Intent::Place(_) => "place",
                Intent::Cancel(_) => "cancel",
                Intent::HedgeNeeded(_) => "hedge_needed",
                Intent::Skip(_) => "skip",
            })
            .collect();
        assert_eq!(got, ["skip", "place", "place", "cancel", "place", "hedge_needed"]);
    }

    /// A torn line is a torn line, not a variant nobody thought of. The shadow
    /// file has exactly one (a run of spaces with a fragment past it), and
    /// `arb_query::intents::fold` already counts that class rather than dying
    /// on it — this only pins that the enum does not silently accept one.
    #[test]
    fn a_damaged_line_fails_to_parse_rather_than_matching_a_variant() {
        assert!(Intent::from_line(r#"{"place":"M","order_"#).is_err());
        assert!(Intent::from_line(r#"{"ts":1.0}"#).is_err(), "no discriminator key");
    }

    /// A side that is neither `bid` nor `ask` is REFUSED, and that is the whole
    /// point of the type.
    ///
    /// While `side` was a `String` this line parsed happily and the trader ended
    /// `if p.side == "ask" { Ask } else { Bid }`, so `"asl"`, `"BID"`, `""` and
    /// `"sell"` all reached the venue as a BUY on the bid — the wrong side of
    /// the book, at a price chosen for the other one. It cannot get that far
    /// now: it stops at the parse, and the arm that used to absorb it does not
    /// exist (`intent_actions` matches `BookSide` exhaustively).
    #[test]
    fn a_side_that_is_not_a_side_is_refused_rather_than_read_as_a_bid() {
        for bad in ["asl", "BID", "", "sell", "buy", "yes"] {
            let line = format!(
                r#"{{"count":5,"order_id":"m1","place":"M","price":"0.10","side":"{bad}","ts":1.0,"venue":"kalshi"}}"#
            );
            assert!(
                Intent::from_line(&line).is_err(),
                "a place whose side is {bad:?} must not parse — it used to become a BID"
            );
        }
        // ...and the two real spellings still do, unchanged.
        for good in ["bid", "ask"] {
            let line = format!(
                r#"{{"count":5,"order_id":"m1","place":"M","price":"0.10","side":"{good}","ts":1.0,"venue":"kalshi"}}"#
            );
            assert_eq!(Intent::from_line(&line).expect("parse").to_line(), line);
        }
    }

    /// The venue rides on the intent so the dispatch site cannot pair a command
    /// with the wrong venue's executor. A record carries none.
    #[test]
    fn only_the_variants_that_are_orders_name_a_venue() {
        let venues: Vec<Option<Venue>> =
            REAL_LINES.iter().map(|l| Intent::from_line(l).expect("parse").venue()).collect();
        assert_eq!(
            venues,
            [
                None,
                Some(Venue::PolymarketUs),
                Some(Venue::PolymarketUs),
                Some(Venue::Polymarket),
                Some(Venue::PolymarketUs),
                None,
            ]
        );
    }
}
