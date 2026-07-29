//! The 41 tests that hold this file's accounting honest.
//!
//! Every one of them is an incident: a number this tab once showed a human
//! that was wrong, and the shape of ledger record that produced it. They are
//! deliberately end-to-end through `build()` — the invariants they encode are
//! properties of the whole payload, not of any one step of it.

use super::*;

/// The take-take this engine made on 2026-07-28: sell PM YES @0.08, buy
/// Kalshi YES @0.04 on 5 contracts. Engine shape — fees_pending, so cost
/// must be derived and fees modelled.
const ENGINE: &str = r#"{"ts":1785250000.0,"relationship_id":"xvus-nobel-peace-26-donaldtrump","qty":5,"strategy":"take-take","status":"open","source":"arb-trader","fees_pending":true,"legs":[{"venue":"polymarket_us","market_id":"tac-nobel-peace-2026-10-09-dontru","side":"ask","role":"taker","qty":5,"yes_price":"0.0800"},{"venue":"kalshi","market_id":"KXNOBELPEACE-26-DJT","side":"bid","role":"taker","qty":5,"yes_price":"0.0400"}]}"#;

/// A real Python-era record: buy_yes + buy_no, settled per-leg fees, and
/// an all-in `cost_usd`.
const PYTHON: &str = r#"{"ts":1784646659.716,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","qty":50,"status":"open","legs":[{"venue":"kalshi","market_id":"KXFRENCHPRES-27-JMEL","action":"buy_yes","qty":50,"avg_price":"0.1100","fees":"0.3427","role":"taker","side":"yes"},{"venue":"polymarket_us","market_id":"ewc-pres-fra-2027-04-11-jeamel","action":"buy_no","qty":50,"yes_price":"0.2500","role":"taker","side":"no"}],"cost_usd":43.9027,"payoff_usd":50.0}"#;

/// Real shape, `data/exec/trades.jsonl`: an `unwound` record with NO legs.
/// Its own `realized_pnl_usd` is a $1.08 LOSS. Booking `payoff = qty` with
/// `cost = 0` reported it as +$15.00.
const NOLEGS_UNWIND: &str = r#"{"ts":1784950000.0,"relationship_id":"pmm-aec-atp-jaifar-gonbue-2026-07-22","strategy":"unwind","status":"unwound","closes_ts":1784900000.0,"qty":15,"realized_pnl_usd":-1.0756,"note":"exit fills"}"#;

/// Real shape: a naked settlement whose own note says the cost was $2.16
/// and it paid nothing. The dash displayed +$4.00.
const NAKED_SETTLE: &str = r#"{"ts":1784960000.0,"relationship_id":"sports-mlb-WSH@COL","strategy":"naked-settlement","status":"realized","qty":4,"realized_pnl_usd":-2.16,"kalshi_result":"no","note":"yes cost $2.16 paid 0"}"#;

/// Real shape: 5 naked long Kalshi YES @0.17. One leg, so the payoff is
/// $0.00 or $5.00 and nobody knows which.
const NAKED_OPEN: &str = r#"{"ts":1784970000.0,"relationship_id":"mltox-KXPRESNOMD-28-GN","strategy":"ml-toxicity-probe","status":"open","qty":5,"cost_usd":0.85,"legs":[{"venue":"kalshi","market_id":"KXPRESNOMD-28-GN","side":"yes","role":"maker","qty":5,"avg_price":"0.17"}]}"#;

/// Real shape: every `sports-*` settlement carries a FLOAT qty.
const FLOAT_QTY: &str = r#"{"ts":1784980000.0,"relationship_id":"sports-rehedge-Tamara Korpatsch@Julia Stusek","strategy":"settlement","status":"unwound","closes_ts":1784979000.0,"qty":2.0,"proceeds_usd":1.4,"realized_pnl_usd":0.14}"#;

/// Real shape: 7 corrections on disk say "this record is a duplicate".
const SUPERSEDE: &str = r#"{"ts":1784990000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"correction","corrects_ts":1784646659.716,"reason":"duplicate of the accurate record","fields":{"status":"superseded"}}"#;

fn totals(out: &serde_json::Value, k: &str) -> f64 {
    out["totals"][k].as_f64().unwrap_or_else(|| panic!("totals.{k} missing or not a number"))
}

#[test]
fn engine_shape_derives_cost_and_models_fees() {
    let out = build(ENGINE, "default", 1785250000.0);
    let r = &out["rows"][0];
    // buy Kalshi YES @0.04 + sell PM YES @0.08 (== buy NO @0.92) = 0.96/ct
    assert!((r["cost_usd"].as_f64().unwrap() - 4.80).abs() < 1e-9);
    assert_eq!(r["payoff_usd"], 5.0);
    assert_eq!(r["fees_settled"], false, "engine records are fees_pending");
    assert!(r["fees_usd"].as_f64().unwrap() > 0.0);
    // net = payoff - cost - modelled fees, and must be under the 0.20 gross
    let net = r["net_usd"].as_f64().unwrap();
    assert!(net > 0.0 && net < 0.20, "net {net} should be a shaved 0.20");
}

/// The regression that made the whole tab wrong: pricing a Python record
/// with the engine's rules reported the book in the red.
#[test]
fn python_shape_uses_its_own_settled_cost_and_fees() {
    let out = build(PYTHON, "default", 1784646659.0);
    let r = &out["rows"][0];
    assert_eq!(r["fees_settled"], true, "the ledger reported real fees");
    assert!((r["cost_usd"].as_f64().unwrap() - 43.9027).abs() < 1e-9);
    // cost_usd is ALL-IN, so fees must NOT be subtracted a second time
    assert!((r["net_usd"].as_f64().unwrap() - 6.0973).abs() < 1e-6);
}

#[test]
fn both_shapes_together_are_in_profit() {
    let out = build(&format!("{PYTHON}\n{ENGINE}"), "default", 1785250000.0);
    let t = &out["totals"];
    assert_eq!(t["open_trades"], 2);
    assert!(t["open_net_usd"].as_f64().unwrap() > 0.0, "this book is in profit");
}

/// Realized money is back in the account; counting it as open would report
/// banked profit as if it were still working. This record is hedged and
/// carries both cost and payoff, so its P&L is still derivable without a
/// `realized_pnl_usd` field.
#[test]
fn realized_is_kept_apart_from_open() {
    let done = PYTHON.replace(r#""status":"open""#, r#""status":"realized""#);
    let out = build(&format!("{ENGINE}\n{done}"), "default", 1785250000.0);
    let t = &out["totals"];
    assert_eq!(t["open_trades"], 1);
    assert_eq!(t["realized_trades"], 1);
    assert!(t["realized_net_usd"].as_f64().unwrap() > 0.0);
}

/// A correction is a compensating append, not a position.
#[test]
fn corrections_are_not_positions() {
    let c = PYTHON.replace(r#""status":"open""#, r#""status":"correction""#);
    let out = build(&format!("{ENGINE}\n{c}"), "default", 1785250000.0);
    assert_eq!(out["totals"]["trades"], 1);
    assert_eq!(out["corrections"], 1);
}

/// APR is only meaningful while the capital is still committed.
#[test]
fn only_open_positions_carry_an_apr() {
    let done = ENGINE.replace(r#""status":"open""#, r#""status":"realized""#);
    let out = build(&done, "default", 1785250000.0);
    assert!(out["rows"][0]["apr_pct"].is_null(), "realized money earns nothing here");
}

#[test]
fn role_changes_the_modelled_fee() {
    let taker = build(ENGINE, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
    let as_maker = ENGINE.replace(r#""role":"taker""#, r#""role":"maker""#);
    let maker =
        build(&as_maker, "default", 1785250000.0)["rows"][0]["fees_usd"].as_f64().unwrap();
    assert!(maker < taker, "maker legs must not pay the taker fee ({maker} vs {taker})");
}

#[test]
fn splits_make_and_take_by_venue() {
    let out = build(ENGINE, "default", 1785250000.0);
    let split = out["by_venue_role"].as_array().unwrap();
    assert_eq!(split.len(), 2);
    for s in split {
        assert_eq!(s["role"], "taker");
        assert_eq!(s["contracts"], 5.0);
    }
}

#[test]
fn a_torn_tail_line_does_not_lose_the_rest() {
    let out = build(&format!("{ENGINE}\n{{\"ts\":1.0,\"relat"), "default", 1785250000.0);
    assert_eq!(out["totals"]["trades"], 1);
    assert_eq!(out["unparsed_lines"], 1);
}

// ----- F1: a record with no legs is not free money ----------------------

/// Was: `have_derived` stayed true through an empty leg loop, so cost was
/// $0.00 and payoff was qty — the whole notional booked as profit.
#[test]
fn a_closed_record_with_no_legs_uses_its_own_realized_pnl() {
    let out = build(NOLEGS_UNWIND, "default", 1785250000.0);
    let r = &out["rows"][0];
    assert!(
        (r["net_usd"].as_f64().unwrap() + 1.0756).abs() < 1e-9,
        "must be the ledger's -1.0756 loss, not +15.00: {}",
        r["net_usd"]
    );
    assert_eq!(r["net_source"], "ledger:realized_pnl");
    assert!((totals(&out, "realized_net_usd") + 1.0756).abs() < 1e-9);
}

/// The single worst row on the live board: its own note says "yes cost
/// $2.16 paid 0" and it displayed as +$4.00.
#[test]
fn a_naked_settlement_shows_its_loss_not_its_notional() {
    let out = build(NAKED_SETTLE, "default", 1785250000.0);
    let r = &out["rows"][0];
    assert!((r["net_usd"].as_f64().unwrap() + 2.16).abs() < 1e-9, "got {}", r["net_usd"]);
    assert!(totals(&out, "realized_net_usd") < 0.0);
}

/// A closed record the ledger never priced must be blank, not zero-cost.
#[test]
fn a_closed_record_with_no_pnl_at_all_is_null_and_counted() {
    let rec = r#"{"ts":1784905478.772228,"relationship_id":"mltox-KXPRESNOMD-28-GN","strategy":"ml-toxicity-probe","status":"unwound","closes_ts":null,"qty":4,"note":"manual flatten"}"#;
    let out = build(rec, "default", 1785250000.0);
    assert!(out["rows"][0]["net_usd"].is_null(), "no P&L is recoverable from this record");
    assert!(!out["rows"][0]["unpriced_reason"].is_null());
    assert_eq!(out["totals"]["realized_unpriced_trades"], 1);
    assert_eq!(totals(&out, "realized_net_usd"), 0.0);
    assert_eq!(out["orphan_unwinds"], 1, "a null closes_ts closes nothing");
}

// ----- F2: the unwound / closes_ts fold --------------------------------

/// 51 of 52 unwind records on disk pointed at a line still marked `open`,
/// so 265 contracts and $212 of capital were counted twice.
#[test]
fn a_fully_unwound_basket_leaves_the_open_book() {
    let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":50,"realized_pnl_usd":2.0}"#;
    let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
    let t = &out["totals"];
    assert_eq!(t["open_trades"], 0, "the basket is closed");
    assert_eq!(totals(&out, "open_contracts"), 0.0);
    assert_eq!(totals(&out, "open_cost_usd"), 0.0, "$43.90 is back in the account");
    assert_eq!(totals(&out, "open_net_usd"), 0.0);
    assert_eq!(t["closed_by_unwind"], 1);
    assert_eq!(totals(&out, "realized_net_usd"), 2.0, "counted once, on the unwind row");
    // the open line is still visible as history, but folded to `closed`
    let closed =
        out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "closed").unwrap();
    assert_eq!(closed["ledger_status"], "open", "the ledger line is never rewritten");
    assert!(closed["net_usd"].is_null());
}

/// The live case: 24 of 50 melenchon contracts were unwound, and the dash
/// still showed 50 working and $43.90 tied up.
#[test]
fn a_partial_unwind_pro_rates_the_remainder() {
    let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":24,"proceeds_usd":22.56,"realized_pnl_usd":1.5}"#;
    let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
    assert_eq!(totals(&out, "open_contracts"), 26.0, "50 booked, 24 unwound");
    let want_cost = 43.9027 * 26.0 / 50.0;
    assert!((totals(&out, "open_cost_usd") - want_cost).abs() < 1e-9);
    let want_net = 6.0973 * 26.0 / 50.0;
    assert!((totals(&out, "open_net_usd") - want_net).abs() < 1e-6);
    let open = out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "open").unwrap();
    assert_eq!(open["qty"], 26.0);
    assert_eq!(open["qty_booked"], 50.0);
}

/// An unwind matches ONE basket by (relationship_id, ts) — matching every
/// basket on the relationship would free exposure that is still on.
#[test]
fn an_unwind_only_closes_the_basket_it_names() {
    let other = PYTHON.replace("1784646659.716", "1784646999.0");
    let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"unwound","closes_ts":1784646659.716,"qty":50,"realized_pnl_usd":0.0}"#;
    let out = build(&format!("{PYTHON}\n{other}\n{unwind}"), "default", 1785250000.0);
    assert_eq!(totals(&out, "open_contracts"), 50.0, "the other basket is untouched");
}

// ----- F3: naked legs are not locked profit ---------------------------

/// 9 open records had exactly one leg and contributed 49% of all reported
/// "locked" profit. A one-leg record is a directional bet.
#[test]
fn a_single_leg_open_position_is_unpriced_not_locked() {
    let out = build(NAKED_OPEN, "default", 1785250000.0);
    let r = &out["rows"][0];
    assert_eq!(r["hedged"], false);
    assert!(r["net_usd"].is_null(), "a naked bet has no locked profit: {}", r["net_usd"]);
    assert!(r["payoff_usd"].is_null(), "and no known payoff either");
    assert!(r["apr_pct"].is_null());
    assert!(r["unpriced_reason"].as_str().unwrap().contains("NAKED"));
    let t = &out["totals"];
    assert_eq!(totals(&out, "open_net_usd"), 0.0, "excluded from the headline");
    assert_eq!(t["open_unpriced_trades"], 1);
    // ...but its capital and its contracts are real and still counted
    assert_eq!(totals(&out, "open_contracts"), 5.0);
    assert!((totals(&out, "open_cost_usd") - 0.85).abs() < 1e-9);
    assert!(t["blended_apr_pct"].is_null(), "nothing priceable to average");
}

/// `ml-toxicity-probe +$49.75` was the top line on the strategy board and
/// was 25 naked Kalshi contracts costing $4.25.
#[test]
fn a_naked_strategy_does_not_top_the_board() {
    let five = (0..5)
        .map(|i| NAKED_OPEN.replace("1784970000.0", &format!("17849700{i:02}.0")))
        .collect::<Vec<_>>()
        .join("\n");
    let out = build(&five, "default", 1785250000.0);
    let s = out["by_strategy"].as_array().unwrap();
    assert_eq!(s.len(), 1);
    assert_eq!(s[0]["net_usd"], 0.0, "not +49.75");
    assert_eq!(s[0]["unpriced"], 5);
}

/// Two legs on the SAME side is not a hedge either.
#[test]
fn two_legs_on_the_same_side_are_not_hedged() {
    let same = ENGINE.replace(r#""side":"ask""#, r#""side":"bid""#);
    let out = build(&same, "default", 1785250000.0);
    assert_eq!(out["rows"][0]["hedged"], false);
    assert!(out["rows"][0]["net_usd"].is_null());
}

// ----- direction: a SOLD leg is a credit, not a purchase ----------------

/// The latent money bug. Selling YES @0.10 and selling NO @0.70 collects
/// $0.80 against the $1.00 it owes — a guaranteed 20c/ct LOSS. Reading only
/// `side` priced it as `hedged: true, net +1.804, apr 52.8%` and put it top
/// of the strategy board: the same failure class as pricing a naked leg as
/// locked profit.
#[test]
fn selling_both_sides_is_not_a_locked_profit() {
    let rec = r#"{"ts":1784990001.0,"relationship_id":"xvus-fedcut-26-usfed-2026-cut","strategy":"take-take","status":"open","qty":10,"legs":[{"venue":"kalshi","market_id":"KXRATECUT-26DEC31","side":"yes","action":"sell","qty":10,"yes_price":"0.1000"},{"venue":"polymarket_us","market_id":"cbpac-usfed-2026-cut","side":"no","action":"sell","qty":10,"yes_price":"0.3000"}]}"#;
    let out = build(rec, "default", 1785250000.0);
    let r = &out["rows"][0];
    assert_eq!(r["hedged"], false, "both legs were SOLD");
    assert!(
        r["net_usd"].is_null(),
        "a credit-and-obligation is not locked profit: {}",
        r["net_usd"]
    );
    assert!(r["cost_usd"].is_null(), "no cost may be derived from sold legs");
    assert!(r["apr_pct"].is_null());
    assert!(r["unpriced_reason"].as_str().unwrap().contains("SHORT"));
    assert_eq!(totals(&out, "open_net_usd"), 0.0);
    assert_eq!(out["totals"]["open_unpriced_trades"], 1);
}

/// The real disk shape: 54 legs across 28 records carry `action: "sell"`.
/// This one rendered `cost_usd 41.00 / payoff_usd 41.00` for a record whose
/// two legs were both sold at 0.19. Its net comes off the ledger and is
/// unaffected; the cost and payoff columns were fiction.
#[test]
fn a_sold_exit_record_reports_no_derived_cost_or_payoff() {
    let rec = r#"{"ts":1784728156.5523076,"relationship_id":"xvus-fedcut-26-usfed-2026-cut","strategy":"unwind","status":"unwound","closes_ts":1784646768.338,"qty":41,"legs":[{"venue":"kalshi","market_id":"KXRATECUT-26DEC31","side":"yes","action":"sell","qty":41,"yes_price":"0.1900"},{"venue":"polymarket_us","market_id":"cbpac-usfed-2026-cut","side":"no","action":"sell","qty":41,"yes_price":"0.1900"}],"proceeds_usd":41.0,"realized_pnl_usd":1.1583}"#;
    let out = build(rec, "default", 1785250000.0);
    let r = &out["rows"][0];
    assert_eq!(r["hedged"], false);
    assert!(r["cost_usd"].is_null(), "41.00 was invented from 0.19 + (1-0.19)");
    assert!(r["payoff_usd"].is_null(), "41.00 was invented from qty");
    assert!((r["net_usd"].as_f64().unwrap() - 1.1583).abs() < 1e-9, "ledger truth stands");
    assert_eq!(r["net_source"], "ledger:realized_pnl");
}

/// `action` was dropped from the payload entirely, so nothing downstream
/// could tell a bought leg from a sold one.
#[test]
fn the_payload_carries_leg_direction() {
    let out = build(PYTHON, "default", 1785250000.0);
    let legs = out["rows"][0]["legs"].as_array().unwrap();
    assert_eq!(legs[0]["action"], "buy_yes");
    assert_eq!(legs[0]["direction"], "long_yes");
    assert_eq!(legs[1]["direction"], "long_no");
    let sold = PYTHON.replace(r#""action":"buy_yes""#, r#""action":"sell""#);
    let out = build(&sold, "default", 1785250000.0);
    assert_eq!(out["rows"][0]["legs"][0]["direction"], "short");
}

/// Buying to close a short is not an opening debit either.
#[test]
fn a_close_via_buy_leg_is_a_short_not_a_purchase() {
    let rec = PYTHON.replace(r#""action":"buy_no""#, r#""action":"close_via_buy_short""#);
    let out = build(&rec, "default", 1785250000.0);
    assert_eq!(out["rows"][0]["hedged"], false);
    assert_eq!(out["rows"][0]["legs"][1]["direction"], "short");
}

// ----- unwind records that cannot do their job -------------------------

/// An unwind with no usable qty would leave the basket fully open AND book
/// its own P&L: the double count this fold exists to prevent. The remainder
/// is unknowable, so the basket must price as null, not as fully on.
#[test]
fn an_unwind_with_no_usable_qty_makes_the_remainder_unknown() {
    let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"realized_pnl_usd":1.5}"#;
    let out = build(&format!("{PYTHON}\n{unwind}"), "default", 1785250000.0);
    assert_eq!(out["unusable_unwinds"], 1);
    let open = out["rows"].as_array().unwrap().iter().find(|r| r["status"] == "open").unwrap();
    assert!(open["net_usd"].is_null(), "how much is still on is unknown");
    assert!(open["unpriced_reason"].as_str().unwrap().contains("no usable qty"));
    assert_eq!(totals(&out, "open_net_usd"), 0.0, "not counted as fully open");
    assert!((totals(&out, "realized_net_usd") - 1.5).abs() < 1e-9);
}

/// An unwind naming a record that is not `open` can never reduce anything,
/// so it is an orphan — counting it as matched hid a live basket.
#[test]
fn an_unwind_naming_a_non_open_record_is_an_orphan() {
    let first = r#"{"ts":1784700000.0,"relationship_id":"r1","strategy":"unwind","status":"unwound","closes_ts":1784600000.0,"qty":5,"realized_pnl_usd":0.5}"#;
    let second = r#"{"ts":1784700001.0,"relationship_id":"r1","strategy":"unwind","status":"unwound","closes_ts":1784700000.0,"qty":5,"realized_pnl_usd":0.25}"#;
    let out = build(&format!("{first}\n{second}"), "default", 1785250000.0);
    assert_eq!(out["orphan_unwinds"], 2, "neither names an open basket");
}

/// The maker probe writes `side: "mixed"`. Guessing which way it leans is
/// how a directional position becomes a riskless one on the screen.
#[test]
fn an_unreadable_leg_side_is_never_guessed() {
    let rec = r#"{"ts":1784970000.0,"relationship_id":"pmm-aec-atp-jaifar-gonbue-2026-07-22","strategy":"pmus-maker-probe","status":"open","qty":15,"legs":[{"venue":"polymarket_us","market_id":"aec-atp-jaifar-gonbue-2026-07-22","side":"mixed","role":"maker+taker","qty":15}]}"#;
    let out = build(rec, "default", 1785250000.0);
    assert_eq!(out["rows"][0]["hedged"], false);
    assert!(out["rows"][0]["net_usd"].is_null());
    assert!(out["rows"][0]["cost_usd"].is_null(), "no price, so no cost may be invented");
    assert!(out["rows"][0]["unpriced_reason"].as_str().unwrap().contains("not readable"));
}

// ----- F4: corrections amend, they do not vanish ------------------------

/// A correction carries a `fields` object that AMENDS its target. Dropping
/// it leaves the value the ledger explicitly retracted on the screen.
#[test]
fn a_correction_amends_the_record_it_names() {
    let fix = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","status":"correction","corrects_ts":1784646659.716,"reason":"actual PM fill 0.70, not the 0.76 limit","fields":{"cost_usd":40.0}}"#;
    let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
    assert_eq!(out["corrections"], 1);
    assert_eq!(out["corrections_applied"], 1);
    assert!((totals(&out, "open_cost_usd") - 40.0).abs() < 1e-9, "the corrected cost wins");
    assert!((totals(&out, "open_net_usd") - 10.0).abs() < 1e-9);
}

/// Two of the 45 corrections on disk carry NO `relationship_id`. Keying on
/// `(relationship_id, corrects_ts)` discarded both, and they retract
/// `realized_pnl_usd` from +2.6369 to -0.2348 and from +4.1308 to -0.3660 —
/// $7.3685 of overstated realized P&L, counted in `corrections` and then
/// silently thrown away.
#[test]
fn a_correction_without_a_relationship_id_still_finds_its_target() {
    let target = r#"{"ts":1784815976.3063905,"relationship_id":"pmm-aec-itfme-vindul-rapper-2026-07-23","strategy":"pmus-maker-probe","status":"unwound","closes_ts":1784800000.0,"qty":5,"realized_pnl_usd":2.6369}"#;
    let fix = r#"{"ts":1784818208.5212176,"strategy":"pmus-maker-probe","status":"correction","corrects_ts":1784815976.3063905,"fields":{"realized_pnl_usd":-0.2348,"note":"CORRECTED from +2.6369 (bad entry-price assumption)"}}"#;
    let out = build(&format!("{target}\n{fix}"), "default", 1785250000.0);
    assert!(
        (totals(&out, "realized_net_usd") + 0.2348).abs() < 1e-9,
        "the retraction must win, not the +2.6369 it retracts: {}",
        out["totals"]["realized_net_usd"]
    );
    assert_eq!(out["corrections_applied"], 1);
    assert_eq!(out["corrections_unmatched"], 0);
}

/// A correction that reaches nothing is a retraction thrown away, and the
/// value it retracted is still on the screen. It must be counted so the page
/// can say so — silence here is what hid the two above.
#[test]
fn a_correction_that_reaches_no_record_is_reported() {
    let fix = r#"{"ts":1784818208.5212176,"status":"correction","corrects_ts":1700000000.0,"fields":{"realized_pnl_usd":-9.99}}"#;
    let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
    assert_eq!(out["corrections"], 1);
    assert_eq!(out["corrections_applied"], 0);
    assert_eq!(out["corrections_unmatched"], 1);
}

/// A correction that names a relationship must name the target's own —
/// dropping the pair check entirely would let one amend another's record.
#[test]
fn a_correction_naming_the_wrong_relationship_is_not_applied() {
    let fix = r#"{"ts":1784818208.0,"relationship_id":"some-other-thing","status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":1.0}}"#;
    let out = build(&format!("{PYTHON}\n{fix}"), "default", 1785250000.0);
    assert!((totals(&out, "open_cost_usd") - 43.9027).abs() < 1e-9, "target untouched");
    assert_eq!(out["corrections_unmatched"], 1);
}

/// Several corrections on one target coalesce in file order — three do on
/// disk. That is legitimate, and must NOT count as unmatched.
#[test]
fn corrections_on_one_target_coalesce_last_wins() {
    let a = r#"{"ts":1784700001.0,"status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":10.0}}"#;
    let b = r#"{"ts":1784700002.0,"status":"correction","corrects_ts":1784646659.716,"fields":{"cost_usd":20.0}}"#;
    let out = build(&format!("{PYTHON}\n{a}\n{b}"), "default", 1785250000.0);
    assert!((totals(&out, "open_cost_usd") - 20.0).abs() < 1e-9, "the later correction wins");
    assert_eq!(out["corrections_applied"], 1, "one record amended");
    assert_eq!(out["corrections_unmatched"], 0, "both reached the target");
}

/// 7 corrections on disk say `status: superseded` — "this line is a
/// duplicate". All 7 were still being counted as positions.
#[test]
fn a_superseded_record_is_removed_from_the_book() {
    let out = build(&format!("{PYTHON}\n{SUPERSEDE}"), "default", 1785250000.0);
    assert_eq!(out["superseded"], 1);
    assert_eq!(out["totals"]["trades"], 0, "the duplicate is gone, not repriced");
    assert_eq!(totals(&out, "open_contracts"), 0.0);
    assert_eq!(totals(&out, "open_net_usd"), 0.0);
}

// ----- F5: float qty --------------------------------------------------

/// `as_i64()` returns None for `2.0`, so qty read as 0, cost as None and
/// the record fell out of every total behind a `net_usd: null`.
#[test]
fn a_float_qty_is_not_read_as_zero() {
    let out = build(FLOAT_QTY, "default", 1785250000.0);
    assert_eq!(out["rows"][0]["qty"], 2.0);
    assert!((totals(&out, "realized_net_usd") - 0.14).abs() < 1e-9);
    let s = &out["by_strategy"][0];
    assert_eq!(s["contracts"], 2.0, "45 real contracts were invisible");
    assert!((s["net_usd"].as_f64().unwrap() - 0.14).abs() < 1e-9);
}

/// One poisoned qty must cost one row, not the whole count: `open_contracts`
/// is a sum, and serde_json writes a NaN sum as `null`.
#[test]
fn a_non_finite_qty_cannot_poison_the_totals() {
    let bad = NAKED_OPEN.replace(r#""qty":5,"cost_usd""#, r#""qty":"NaN","cost_usd""#);
    let out = build(&format!("{PYTHON}\n{bad}"), "default", 1785250000.0);
    assert_eq!(totals(&out, "open_contracts"), 50.0, "the good basket survives");
    assert!((totals(&out, "open_net_usd") - 6.0973).abs() < 1e-6);
    assert_eq!(out["totals"]["open_unpriced_trades"], 1);
}

#[test]
fn a_float_qty_on_a_leg_is_not_read_as_zero() {
    let f = ENGINE.replace(r#""qty":5,"yes_price":"0.0800""#, r#""qty":5.0,"yes_price":"0.0800""#);
    let out = build(&f, "default", 1785250000.0);
    let legs = out["rows"][0]["legs"].as_array().unwrap();
    assert_eq!(legs[0]["qty"], 5.0);
    assert!((out["rows"][0]["cost_usd"].as_f64().unwrap() - 4.80).abs() < 1e-9);
}

// ----- the whole-file invariant ---------------------------------------

/// The strategy table must decompose the headline exactly. If it does not,
/// one of the two is counting something the other is not.
#[test]
fn by_strategy_sums_to_the_headline() {
    let unwind = r#"{"ts":1784700000.0,"relationship_id":"xvus-france-pres-27-jeanlucmelenchon","strategy":"unwind","status":"unwound","closes_ts":1784646659.716,"qty":24,"realized_pnl_usd":1.5}"#;
    let text = format!(
        "{PYTHON}\n{ENGINE}\n{unwind}\n{NOLEGS_UNWIND}\n{NAKED_SETTLE}\n{NAKED_OPEN}\n\
         {FLOAT_QTY}"
    );
    let out = build(&text, "default", 1785250000.0);
    let want = totals(&out, "open_net_usd") + totals(&out, "realized_net_usd");
    let got: f64 = out["by_strategy"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| s["net_usd"].as_f64().unwrap())
        .sum();
    assert!((got - want).abs() < 1e-9, "by_strategy {got} vs totals {want}");
}

/// No row may claim a net without a source for it, and no row may carry a
/// null net without saying why. That pair of rules is the whole fix.
#[test]
fn every_row_either_has_a_priced_net_or_says_why_not() {
    let text = format!(
        "{PYTHON}\n{ENGINE}\n{NOLEGS_UNWIND}\n{NAKED_SETTLE}\n{NAKED_OPEN}\n{FLOAT_QTY}\n\
         {SUPERSEDE}"
    );
    let out = build(&text, "default", 1785250000.0);
    for r in out["rows"].as_array().unwrap() {
        if r["net_usd"].is_null() {
            assert!(
                r["unpriced_reason"].is_string(),
                "silent null on {}: {r}",
                r["relationship_id"]
            );
            assert!(r["apr_pct"].is_null(), "an unpriced row cannot have an APR");
        } else {
            assert!(r["net_source"].is_string(), "unsourced net on {}", r["relationship_id"]);
        }
    }
}
