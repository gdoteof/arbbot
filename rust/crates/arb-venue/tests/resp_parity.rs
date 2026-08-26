//! Response-parsing + request-shaping parity, against the live-captured shapes
//! in the Python contract tests (tests/test_venue_contracts.py /
//! test_venue_contract_gaps.py). Encodes the known quirks as typed asserts.

use serde_json::json;

use arb_venue::error::VenueError;
use arb_venue::gateway::Side;
use arb_venue::resp::{self, PmFillState};
use arb_venue::wire;

// ------------------------------------------------- request-shaping (wire) ---

#[test]
fn kalshi_place_body_matches_python() {
    // kalshi_gateway.py::place_yes body shape; count "{count:.2f}", price 4dp.
    let got = wire::kalshi_place_body("KXTIME-26-ZOH", Side::Bid, "0.0520", 5, "COID123", true);
    let want = json!({
        "ticker": "KXTIME-26-ZOH",
        "side": "bid",
        "count": "5.00",
        "price": "0.0520",
        "time_in_force": "good_till_canceled",
        "self_trade_prevention_type": "taker_at_cross",
        "client_order_id": "COID123",
        "post_only": true,
    });
    assert_eq!(got, want);
    // taker hedge (post_only=false) flips only the TIF
    let taker = wire::kalshi_place_body("KXTIME-26-ZOH", Side::Ask, "0.9500", 3, "C2", false);
    assert_eq!(taker["time_in_force"], "immediate_or_cancel");
    assert_eq!(taker["side"], "ask");
}

#[test]
fn pmus_order_body_matches_python() {
    // polymarket_us_gateway.py::_order_body — bid=BUY_LONG, ask=BUY_SHORT.
    let bid = wire::pmus_order_body("tpoyc-2026-zohmam", Side::Bid, "0.4100", 5, true);
    assert_eq!(
        bid,
        json!({
            "marketSlug": "tpoyc-2026-zohmam",
            "type": "ORDER_TYPE_LIMIT",
            "price": {"value": "0.4100", "currency": "USD"},
            "quantity": 5,
            "tif": "TIME_IN_FORCE_GOOD_TILL_CANCEL",
            "intent": "ORDER_INTENT_BUY_LONG",
            "manualOrderIndicator": "MANUAL_ORDER_INDICATOR_AUTOMATIC",
            "participateDontInitiate": true,
        })
    );
    let ask = wire::pmus_order_body("tpoyc-2026-zohmam", Side::Ask, "0.2600", 4, false);
    assert_eq!(ask["intent"], "ORDER_INTENT_BUY_SHORT");
    assert_eq!(ask["tif"], "TIME_IN_FORCE_IMMEDIATE_OR_CANCEL");
    assert_eq!(ask["participateDontInitiate"], false);
}

#[test]
fn pmus_create_bare_preview_wrapped_cancel_has_slug() {
    // P6/P7 quirks: create takes the BARE body; only preview wraps in {"request"};
    // cancel MUST carry marketSlug in the body.
    let body = wire::pmus_order_body("tpoyc-2026-zohmam", Side::Ask, "0.2600", 4, false);
    let preview = wire::pmus_preview_body(body.clone());
    assert_eq!(preview.as_object().unwrap().keys().collect::<Vec<_>>(), vec!["request"]);
    assert_eq!(preview["request"]["marketSlug"], "tpoyc-2026-zohmam");
    assert!(body.get("request").is_none(), "create body is bare");
    assert_eq!(wire::pmus_cancel_body("tpoyc-2026-zohmam"), json!({"marketSlug": "tpoyc-2026-zohmam"}));
}

// ------------------------------------------------------- Kalshi responses ---

#[test]
fn kalshi_order_fill_count_is_plain_count() {
    // K3: fill_count_fp "2.00" == 2 contracts.
    let env = resp::kalshi_order_envelope(r#"{"order":{"order_id":"o1","status":"executed","fill_count_fp":"2.00"}}"#).unwrap();
    assert_eq!(env.order.filled_qty(), 2);
    // A 200 whose count field is ABSENT must be UNKNOWN, never "did not
    // trade": the hedge-verification read acts on a low number by sending
    // another order. Every recorded row carries it at zero ("0.00"), so an
    // absent one is a payload shape change.
    assert_eq!(
        resp::kalshi_order_envelope(r#"{"order":{"order_id":"o1","status":"resting"}}"#)
            .unwrap()
            .order
            .try_filled_qty(),
        None,
        "absent is not zero"
    );
    assert_eq!(
        resp::kalshi_order_envelope(r#"{"order":{"order_id":"o1","fill_count_fp":"wat"}}"#)
            .unwrap()
            .order
            .try_filled_qty(),
        None,
        "unparseable is not zero"
    );
    assert_eq!(
        resp::kalshi_order_envelope(r#"{"order":{"order_id":"o1","fill_count_fp":"0.00"}}"#)
            .unwrap()
            .order
            .try_filled_qty(),
        Some(0),
        "...but a real zero still reads as zero, or no hedge could ever be retried"
    );
    // A NEGATIVE is refused, and this guard fails in the PERMITTING direction
    // if it is dropped, which is why it needs pinning rather than a comment.
    // The hedge-verification read compares `n <= credited` to decide whether a
    // retry may go out; a negative satisfies that against any credited count,
    // so a floored -1 would AUTHORISE a place on an attempt whose fill nobody
    // has read. Same for a non-finite: `inf >= 0.0` is true, so `is_finite` is
    // load-bearing on its own and is dropped independently.
    for bad in ["-1.00", "-0.01", "inf", "NaN"] {
        let body = format!(r#"{{"order":{{"order_id":"o1","fill_count_fp":"{bad}"}}}}"#);
        assert_eq!(
            resp::kalshi_order_envelope(&body).unwrap().order.try_filled_qty(),
            None,
            "`{bad}` is not a fill count — it must read as UNKNOWN, never as a low number"
        );
    }
    // absent fill_count_fp -> 0 (never a panic)
    let env0 = resp::kalshi_order_envelope(r#"{"order":{"order_id":"o1","status":"resting"}}"#).unwrap();
    assert_eq!(env0.order.filled_qty(), 0);
}

#[test]
fn kalshi_orders_page_history_and_cursor() {
    // K1: page carries history rows + cursor; last page omits cursor.
    let p1 = resp::kalshi_orders_page(
        r#"{"orders":[{"order_id":"r1","status":"resting","ticker":"KXTIME-26-ZOH"},
                     {"order_id":"a1","status":"canceled"}],"cursor":"CUR2"}"#,
    )
    .unwrap();
    assert_eq!(p1.orders.len(), 2);
    assert_eq!(p1.cursor.as_deref(), Some("CUR2"));
    let last = resp::kalshi_orders_page(r#"{"orders":[{"order_id":"r2","status":"resting"}]}"#).unwrap();
    assert!(last.cursor.is_none(), "last page has no cursor");
}

#[test]
fn kalshi_balance_and_positions_money_as_strings() {
    let bal = resp::kalshi_balance(r#"{"balance_dollars":"100"}"#).unwrap();
    assert_eq!(bal.balance_dollars, "100");
    // position_fp is a SIGNED count string; *_dollars stay strings.
    let pos = resp::kalshi_positions(
        r#"{"market_positions":[{"ticker":"KXTIME-26-ZOH","position_fp":"-14","market_exposure_dollars":"3.50","realized_pnl_dollars":"0","fees_paid_dollars":"0.02"}]}"#,
    )
    .unwrap();
    assert_eq!(pos.market_positions[0].position_fp.as_deref(), Some("-14"));
    assert_eq!(pos.market_positions[0].market_exposure_dollars.as_deref(), Some("3.50"));
}

#[test]
fn kalshi_missing_required_field_is_typed_error() {
    // order_id is required — a row without it is a bug, not a default.
    let err = resp::kalshi_order_envelope(r#"{"order":{"status":"resting"}}"#).unwrap_err();
    match err {
        VenueError::MissingField { field, .. } => assert_eq!(field, "order_id"),
        other => panic!("expected MissingField, got {other:?}"),
    }
}

#[test]
fn kalshi_unknown_fields_tolerated() {
    let env = resp::kalshi_order_envelope(
        r#"{"order":{"order_id":"o1","status":"resting","brand_new_field":42,"nested":{"x":1}}}"#,
    )
    .unwrap();
    assert_eq!(env.order.order_id, "o1");
}

// -------------------------------------------------------- PM-US responses ---

const PM_CREATE: &str = r#"{"id":"BD819026675P","executions":[]}"#;
const PM_ORDER_FILLED: &str = r#"{"id":"BD819026675P","marketSlug":"tpoyc-2026-zohmam",
    "side":"ORDER_SIDE_SELL","quantity":4,"cumQuantity":4,"leavesQuantity":0,
    "state":"ORDER_STATE_FILLED","avgPx":{"value":"0.2600","currency":"USD"},
    "commissionNotionalTotalCollected":{"value":"0.0500","currency":"USD"},
    "outcomeSide":"OUTCOME_SIDE_NO"}"#;

/// The PM-US half of the hedge-verification read, which had no test at all.
///
/// Same contract as `KalshiOrder::try_filled_qty` and the same reason for it:
/// the caller acts on a low number by SENDING ANOTHER ORDER, so absent must be
/// unknown and a negative must be refused rather than floored. A negative
/// satisfies `n <= credited` against any credited count, so letting one through
/// authorises a place on an attempt whose fill nobody has read.
#[test]
fn pmus_try_filled_qty_answers_unknown_rather_than_a_low_number() {
    // Only a CREATE omits cumQuantity, and this is never read off one — but if
    // it ever is, the answer is UNKNOWN, not "did not trade".
    assert_eq!(resp::pmus_order(PM_CREATE).unwrap().try_filled_qty(), None, "absent is not zero");
    // A real zero must still read as zero, or no hedge could ever be retried.
    let zero = r#"{"id":"pm-1","marketSlug":"will-x","state":"STATE_OPEN","cumQuantity":0}"#;
    assert_eq!(resp::pmus_order(zero).unwrap().try_filled_qty(), Some(0));
    // ...and note this is a DIFFERENT question from `fill_state`, which also
    // wants avgPx and so calls a perfectly readable 0 `NoFillData`.
    assert_eq!(resp::pmus_order(zero).unwrap().fill_state(), PmFillState::NoFillData);
    assert_eq!(resp::pmus_order(PM_ORDER_FILLED).unwrap().try_filled_qty(), Some(4));
    let neg = r#"{"id":"pm-1","marketSlug":"will-x","cumQuantity":-1}"#;
    assert_eq!(
        resp::pmus_order(neg).unwrap().try_filled_qty(),
        None,
        "a negative is not a fill — it must never reach the `n <= credited` comparison"
    );
}

#[test]
fn pmus_create_omits_fill_data_typed_variant() {
    // Quirk `pmus-create-omits-fill-data` / avgPx=0-on-create, as a typed variant.
    let create = resp::pmus_order(PM_CREATE).unwrap();
    assert_eq!(create.id, "BD819026675P");
    assert_eq!(create.fill_state(), PmFillState::NoFillData);
    assert_eq!(create.filled_qty(), 0);
    assert!(create.avg_px.is_none(), "create NEVER carries avgPx");

    let filled = resp::pmus_order(PM_ORDER_FILLED).unwrap();
    assert_eq!(
        filled.fill_state(),
        PmFillState::Filled { cum_quantity: 4, avg_px: "0.2600".to_string() }
    );
    assert_eq!(filled.filled_qty(), 4);
    // commission is a TOTAL (string) — caller divides by qty, never coerced to float here.
    assert_eq!(filled.commission_notional_total_collected.unwrap().value, "0.0500");
}

#[test]
fn pmus_order_envelope_is_unwrapped() {
    // get_order does j.get("order", j): an {"order": ...} envelope unwraps.
    let env = format!(r#"{{"order":{PM_ORDER_FILLED}}}"#);
    let o = resp::pmus_order(&env).unwrap();
    assert_eq!(o.id, "BD819026675P");
    assert_eq!(o.filled_qty(), 4);
}

#[test]
fn pmus_positions_dict_keyed_by_slug() {
    // P5: positions is a DICT keyed by slug with string netPosition ("-14").
    let pos = resp::pmus_positions(
        r#"{"positions":{"tpoyc-2026-popleo":{"netPosition":"-14","qtySold":"14","avgPx":{"value":"0.7550","currency":"USD"}}},"nextCursor":"","eof":true}"#,
    )
    .unwrap();
    let p = &pos.positions["tpoyc-2026-popleo"];
    assert_eq!(p.net_position, "-14");
    assert_eq!(p.avg_px.as_ref().unwrap().value, "0.7550");
    assert_eq!(pos.eof, Some(true));
}

#[test]
fn pmus_balances_buying_power_string() {
    let bal = resp::pmus_balances(r#"{"balances":[{"buyingPower":"100"}]}"#).unwrap();
    assert_eq!(bal.balances[0].buying_power, "100");
}

#[test]
fn pmus_missing_id_is_typed_error() {
    let err = resp::pmus_order(r#"{"executions":[]}"#).unwrap_err();
    match err {
        VenueError::MissingField { field, .. } => assert_eq!(field, "id"),
        other => panic!("expected MissingField, got {other:?}"),
    }
}

// ------------------------------------- the money fields, against a real payload ---

/// THE LINK THE UNIT TESTS CANNOT COVER: that `KalshiOrder` actually
/// DESERIALIZES the fill-cost fields out of a real venue response.
///
/// `maker_exit::fill_price` is pinned against these numbers already, but it is
/// handed a `FilledCost` built in the test. If the wire names were wrong —
/// changed, or never right — `filled_cost()` would read absent money as "0",
/// price the fill at 1.0000, fail its own guard and fall back to the limit. The
/// whole of PR #101 would become a silent no-op, and every symptom of that is
/// identical to "the venue did not answer".
///
/// So this drives the real captured body: the 16:19:32Z close on KXTIME-26-POP,
/// sent at a limit of 0.1200 and filled by Kalshi at the touch. 3.9208 on 5
/// contracts is 0.7842 of NO, so 0.2158 of YES received — 9.6c better than the
/// limit, which is exactly the money the ledger was throwing away.
#[test]
fn a_real_filled_order_carries_its_money_fields() {
    // Through `kalshi_order_envelope`, which is what `order_status_at` calls on
    // the live GET — so this drives the same parse the money path does.
    let raw = include_str!("fixtures/venue/kalshi_order_filled.json");
    let order = arb_venue::resp::kalshi_order_envelope(raw)
        .expect("a real single-order GET body parses")
        .order;

    assert_eq!(order.filled_qty(), 5);
    let fc = order.filled_cost().expect("a filled order has a cost — if this is None the \
                                         wire names moved and #101 is a silent no-op");
    assert_eq!(fc.qty, "5.00");
    assert_eq!(fc.taker_cost_usd, "3.920800", "the field #101 exists to read");
    assert_eq!(fc.maker_cost_usd, "0.000000");
    assert_eq!(fc.taker_fees_usd, "0.059300");
    assert!(fc.complement, "action=sell side=yes: the notional is the NO side");

    // ...and the arithmetic the trader does on it, restated here so both halves
    // of the conversion are pinned in one place against one real row. The
    // trader works in decimal and quantizes to 4dp, so 0.21584 becomes the
    // 0.2158 `maker_exit::fill_price` is pinned to — either way it is nowhere
    // near the 0.1200 limit this order was sent at.
    let per_ct = 3.920800_f64 / 5.0;
    let yes = 1.0 - per_ct;
    assert!((yes - 0.21584).abs() < 1e-9, "0.21584 YES received, got {yes}");
    assert!(yes > 0.1200, "and better than the limit, which is the whole point");
}

/// A CREATE RESPONSE HAS NO FILL DATA, and must answer `None` rather than a
/// zero-cost fill. Reading absent money as $0.00 would price a sell at 1.0000
/// and a buy at 0.0000 — both of which are "free money" shaped.
#[test]
fn an_order_with_no_money_fields_reports_no_cost() {
    let create = r#"{"order_id":"x","client_order_id":"m1","fill_count":"0.00"}"#;
    let o = arb_venue::resp::kalshi_created_order(create).expect("a create response parses");
    assert_eq!(o.filled_cost(), None, "nothing filled, so there is no price");
}
