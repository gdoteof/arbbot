//! The M2 gate for PM-US (QCX). Same contract as the Kalshi order-path tests:
//! assert on what actually went on the wire.
//!
//! PM-US differs from Kalshi in ways that are easy to get wrong by analogy, so
//! each difference gets its own test:
//!   * create takes the BARE body; only /v1/order/preview wraps it
//!   * cancel needs the marketSlug in the BODY, and it is REQUIRED
//!   * a failed cancel is an ERROR — Kalshi's 404-is-success does NOT transfer
//!   * cancel-all is one call, not a paginated sweep
//!   * the create response carries no fill data at all

use arb_venue::gateway::{
    CancelBy, CancelRequest, PlaceRequest, PmusGateway, Side, Tif, VenueGateway,
};
use arb_venue::ratelimit::RateLimiter;
use arb_venue::transport::{Response, Transport};
use arb_venue::{PmusSigner, VenueError};
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Mutex;

#[derive(Debug, Clone)]
struct Sent {
    method: String,
    path: String,
    body: Option<Value>,
    headers: Vec<(String, String)>,
}

struct MockTransport {
    replies: Mutex<Vec<(u16, String)>>,
    sent: Mutex<Vec<Sent>>,
}

impl MockTransport {
    fn new(replies: Vec<(u16, &str)>) -> Self {
        Self {
            replies: Mutex::new(replies.into_iter().map(|(s, b)| (s, b.to_string())).collect()),
            sent: Mutex::new(Vec::new()),
        }
    }
    fn sent(&self) -> Vec<Sent> {
        self.sent.lock().unwrap().clone()
    }
}

impl Transport for MockTransport {
    fn send(
        &self,
        method: &str,
        path: &str,
        _query: Option<&str>,
        headers: &[(&'static str, String)],
        body: Option<&Value>,
    ) -> Result<Response, VenueError> {
        self.sent.lock().unwrap().push(Sent {
            method: method.to_string(),
            path: path.to_string(),
            body: body.cloned(),
            headers: headers.iter().map(|(k, v)| (k.to_string(), v.clone())).collect(),
        });
        let mut r = self.replies.lock().unwrap();
        if r.is_empty() {
            panic!("MockTransport: unexpected extra request {method} {path}");
        }
        let (status, body) = r.remove(0);
        Ok(Response { status, body })
    }
}

fn seed32() -> [u8; 32] {
    let v: Value = serde_json::from_str(include_str!("fixtures/venue/sigs.json")).unwrap();
    let raw = hex::decode(v["pmus"]["seed_hex"].as_str().unwrap()).unwrap();
    let mut s = [0u8; 32];
    s.copy_from_slice(&raw[..32]);
    s
}

fn signer() -> PmusSigner {
    let v: Value = serde_json::from_str(include_str!("fixtures/venue/sigs.json")).unwrap();
    PmusSigner::from_seed(v["pmus"]["api_key_id"].as_str().unwrap(), &seed32())
}

fn gw(replies: Vec<(u16, &str)>) -> PmusGateway<MockTransport> {
    PmusGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(600.0, 0),
        MockTransport::new(replies),
    )
    .with_settle(std::time::Duration::ZERO, 1)
}

const SLUG: &str = "will-x-happen";
/// A create response: an id and `executions: []`, and NO fill data.
const CREATED: &str = r#"{"id":"pm-1","marketSlug":"will-x-happen","state":"STATE_OPEN","executions":[]}"#;
const OPEN_ORDER: &str = r#"{"id":"pm-1","marketSlug":"will-x-happen","state":"STATE_OPEN","cumQuantity":0}"#;
/// A row from `/v1/orders/open`, in the live-captured field shape
/// (`resp_parity.rs::PM_ORDER_FILLED`, recorded 2026-07-22): `quantity` is the
/// ORIGINAL size, and there is no limit price on the row at all.
///
/// `side` is deliberately ABSENT rather than invented. The only live capture of
/// that field is `ORDER_SIDE_SELL`, from an order placed as
/// `ORDER_INTENT_BUY_SHORT`; what PM-US reports for a `BUY_LONG` has never been
/// recorded here. `recover_place` does not match on it for exactly that reason,
/// so a fixture asserting a spelling nobody has seen would be inventing wire
/// data in the file whose job is pinning it.
const OPEN_ORDER_5: &str =
    r#"{"id":"pm-lost","marketSlug":"will-x-happen","quantity":5,"cumQuantity":0,"leavesQuantity":5}"#;

fn place_req() -> PlaceRequest {
    PlaceRequest {
        market: SLUG.into(),
        side: Side::Bid,
        price: "0.0100".into(),
        qty: 1,
        tif: Tif::Gtc,
        post_only: true,
        client_order_id: String::new(),
    }
}

// ---------------------------------------------------------------- quirks ---

/// Create takes the BARE order body. Wrapping it in `{"request": ...}` — the
/// shape preview wants — answers with a misleading "Market not found".
#[test]
fn create_posts_the_bare_order_body_not_the_preview_wrapper() {
    let g = gw(vec![(200, CREATED)]);
    let o = g.place(&place_req()).unwrap();
    assert_eq!(o.id, "pm-1");

    let s = &g.transport.sent()[0];
    assert_eq!(s.method, "POST");
    assert_eq!(s.path, "/v1/orders");
    let b = s.body.as_ref().unwrap();
    assert!(b.get("request").is_none(), "create must NOT be wrapped: {b}");
    assert_eq!(b["marketSlug"], SLUG);
    assert_eq!(b["type"], "ORDER_TYPE_LIMIT");
    assert_eq!(b["price"]["value"], "0.0100");
    assert_eq!(b["price"]["currency"], "USD");
    assert_eq!(b["quantity"], 1);
    assert_eq!(b["tif"], "TIME_IN_FORCE_GOOD_TILL_CANCEL");
    assert_eq!(b["intent"], "ORDER_INTENT_BUY_LONG", "bid opens a long");
    assert_eq!(b["participateDontInitiate"], true, "post-only");
}

/// ...and preview is the one endpoint that DOES wrap.
#[test]
fn preview_wraps_the_body_in_request() {
    let g = gw(vec![(200, r#"{"fills":[]}"#)]);
    g.preview(&place_req()).unwrap();
    let s = &g.transport.sent()[0];
    assert_eq!(s.path, "/v1/order/preview");
    let b = s.body.as_ref().unwrap();
    assert_eq!(b["request"]["marketSlug"], SLUG, "preview IS wrapped: {b}");
}

/// An ask opens a SHORT (buy NO) on the same YES price axis.
#[test]
fn an_ask_opens_a_short_on_the_yes_axis() {
    let g = gw(vec![(200, CREATED)]);
    let mut req = place_req();
    req.side = Side::Ask;
    g.place(&req).unwrap();
    let b = g.transport.sent()[0].body.clone().unwrap();
    assert_eq!(b["intent"], "ORDER_INTENT_BUY_SHORT");
}

/// The marketSlug rides in the cancel BODY (it is not in the path).
#[test]
fn cancel_carries_the_market_slug_in_the_body() {
    let g = gw(vec![(200, "{}")]);
    g.cancel(&CancelRequest {
        by: CancelBy::VenueId("pm-1".into()),
        market_slug: Some(SLUG.into()),
    })
    .unwrap();
    let s = &g.transport.sent()[0];
    assert_eq!(s.method, "POST");
    assert_eq!(s.path, "/v1/order/pm-1/cancel");
    assert_eq!(s.body.as_ref().unwrap()["marketSlug"], SLUG);
}

/// A cancel without the slug must fail LOCALLY. We deliberately do not
/// self-resolve it from open_orders — doing that on every reprice hammered the
/// API into 429s.
#[test]
fn a_cancel_without_a_slug_fails_before_reaching_the_wire() {
    let g = gw(vec![]);
    match g.cancel(&CancelRequest { by: CancelBy::VenueId("pm-1".into()), market_slug: None }) {
        Err(VenueError::MissingField { field, .. }) => assert_eq!(field, "market_slug"),
        other => panic!("expected a local MissingField, got {other:?}"),
    }
    assert!(g.transport.sent().is_empty(), "nothing may reach the wire");
}

/// PM-US has NO client-order-id field on the wire — `pmus_order_body` never
/// sends one, so the venue has never seen an `m…` id and has nothing to resolve
/// one against. A cancel we cannot address must be REFUSED, not sent: on
/// 2026-07-28 eleven `POST /v1/order/m…/cancel` calls answered <300 and were
/// logged as `[exec] PolymarketUs cancelled` while every quote kept resting.
#[test]
fn a_client_id_cancel_is_refused_before_the_wire() {
    let g = gw(vec![]);
    match g.cancel(&CancelRequest {
        by: CancelBy::ClientId("m1785257819026".into()),
        market_slug: Some(SLUG.into()),
    }) {
        Err(VenueError::MissingField { endpoint: "pmus cancel", field }) => {
            assert!(field.contains("venue order id"), "{field}");
        }
        other => panic!("PM-US cannot resolve our id; expected a refusal, got {other:?}"),
    }
    assert!(g.transport.sent().is_empty(), "nothing may reach the wire");
}

/// ...and an empty venue id must not POST to `/v1/order//cancel` either.
#[test]
fn an_empty_venue_id_is_refused_before_the_wire() {
    let g = gw(vec![]);
    assert!(matches!(
        g.cancel(&CancelRequest {
            by: CancelBy::VenueId(String::new()),
            market_slug: Some(SLUG.into()),
        }),
        Err(VenueError::MissingField { endpoint: "pmus cancel", .. })
    ));
    assert!(g.transport.sent().is_empty());
}

/// Kalshi's "404 on cancel means already gone" does NOT transfer. On PM-US a
/// failed cancel is an error — believing it succeeded is what accumulated
/// stray orders.
#[test]
fn a_failed_pmus_cancel_is_an_error_unlike_kalshi() {
    for status in [404u16, 400, 500] {
        let g = gw(vec![(status, r#"{"message":"nope"}"#)]);
        match g.cancel(&CancelRequest {
            by: CancelBy::VenueId("pm-1".into()),
            market_slug: Some(SLUG.into()),
        }) {
            Err(VenueError::Status { status: s, .. }) => assert_eq!(s, status),
            other => panic!("HTTP {status} must be an error, got {other:?}"),
        }
    }
}

/// Cancel-all is ONE idempotent call — no listing, no pagination.
#[test]
fn cancel_all_open_is_a_single_call() {
    let g = gw(vec![(200, "{}")]);
    g.cancel_all_open().unwrap();
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 1, "exactly one request");
    assert_eq!(sent[0].method, "POST");
    assert_eq!(sent[0].path, "/v1/orders/open/cancel");
}

/// The create response carries no fill data at all, so it can never be read
/// for a fill. Only the re-fetched order is authoritative.
#[test]
fn a_create_response_never_carries_fill_data() {
    let g = gw(vec![(200, CREATED)]);
    let o = g.place(&place_req()).unwrap();
    assert_eq!(o.fill_state(), arb_venue::resp::PmFillState::NoFillData);
    assert_eq!(o.filled_qty(), 0);
    assert!(o.avg_px.is_none(), "avgPx must be absent on a create");
}

/// order_status unwraps `{"order": ...}` but also accepts a bare order.
#[test]
fn order_status_accepts_both_the_envelope_and_a_bare_order() {
    for body in [
        r#"{"order":{"id":"pm-1","cumQuantity":3,"avgPx":{"value":"0.4200","currency":"USD"}}}"#,
        r#"{"id":"pm-1","cumQuantity":3,"avgPx":{"value":"0.4200","currency":"USD"}}"#,
    ] {
        let g = gw(vec![(200, body)]);
        let o = g.order_status("pm-1").unwrap();
        assert_eq!(o.id, "pm-1");
        assert_eq!(
            o.fill_state(),
            arb_venue::resp::PmFillState::Filled { cum_quantity: 3, avg_px: "0.4200".into() }
        );
    }
}

/// open_orders accepts a bare array or an `{"orders": [...]}` envelope.
#[test]
fn open_orders_accepts_both_shapes() {
    for body in [
        r#"[{"id":"pm-1"},{"id":"pm-2"}]"#,
        r#"{"orders":[{"id":"pm-1"},{"id":"pm-2"}]}"#,
    ] {
        let g = gw(vec![(200, body)]);
        let os = g.open_orders().unwrap();
        assert_eq!(os.len(), 2);
        assert_eq!(os[0].id, "pm-1");
    }
}

/// ...but a 200 that is NEITHER a list nor an envelope carrying `orders` is a
/// body we could not READ, and it must never answer "the book is empty".
///
/// The envelope's `orders` had `#[serde(default)]` — Python's
/// `j.get("orders", [])` ported literally — so any json object deserialized to
/// an EMPTY BOOK. `resting_order_ids` is the only evidence
/// `cancel_all_and_verify` accepts, so a 200 error envelope here was laundered
/// into the proof that lets a halt exit 0. Same defect as
/// `KalshiOrdersPage`, reached through the fallback instead of the struct.
#[test]
fn a_200_with_no_orders_key_is_unreadable_not_an_empty_book() {
    for body in [
        r#"{"error":{"code":"internal_error"}}"#,
        r#"{"data":{"orders":[]}}"#,
        "{}",
    ] {
        match gw(vec![(200, body)]).open_orders() {
            Err(VenueError::Parse { endpoint: "pmus:open_orders", .. }) => {}
            other => panic!("{body} must be unreadable, got {other:?}"),
        }
        assert!(
            gw(vec![(200, body)]).resting_order_ids().is_err(),
            "the sweep's PROOF read must fail too, not answer `empty`: {body}"
        );
    }
    // an empty book still reads as one — on the shape the venue actually sends.
    assert!(gw(vec![(200, "[]")]).resting_order_ids().unwrap().is_empty());
    assert!(gw(vec![(200, r#"{"orders":[]}"#)]).resting_order_ids().unwrap().is_empty());
}

// ------------------------------------------- a place whose answer was lost ---

/// PM-US HAS NO CLIENT ORDER ID ON THE WIRE, so a create whose response is lost
/// leaves an order that nothing can name. Kalshi recovers from this by its own
/// `client_order_id` (`cancel_by_client_order_id`, added after the first live
/// smoke left an order resting on 2026-07-27); the only handle PM-US offers is
/// the open-orders row itself.
///
/// The transport maps a reqwest timeout to `Transport` after 15s, so this is not
/// hypothetical: the order rests, no ack is emitted, `oid_venue` never learns
/// the id, the cancel cannot be addressed and a fill on it arrives under an id
/// the engine cannot attribute.
#[test]
fn a_place_whose_response_was_lost_is_found_by_its_market_and_size() {
    let open = format!(
        r#"[{{"id":"other-market","marketSlug":"different","quantity":5}},{OPEN_ORDER_5}]"#
    );
    let g = gw(vec![(200, &open)]);
    let mut req = place_req();
    req.qty = 5;
    assert_eq!(
        g.recover_place(&req, &HashSet::new()).unwrap(),
        Some("pm-lost".to_string()),
        "the order the venue took while we could not read the answer"
    );
    assert_eq!(g.transport.sent()[0].path, "/v1/orders/open", "one read, no guessing");
}

/// Nothing matching means nothing to adopt. A recovery that invents an order
/// would be worse than the leak it recovers from.
#[test]
fn a_place_the_venue_really_refused_recovers_nothing() {
    let g = gw(vec![(200, r#"[{"id":"someone-else","marketSlug":"different","quantity":5}]"#)]);
    let mut req = place_req();
    req.qty = 5;
    assert_eq!(g.recover_place(&req, &HashSet::new()).unwrap(), None);
}

/// The size has to match too: an order of a different size on the same market is
/// not the one we just placed.
#[test]
fn an_order_of_another_size_on_the_same_market_is_not_ours() {
    let g = gw(vec![(200, &format!("[{OPEN_ORDER_5}]"))]);
    let mut req = place_req();
    req.qty = 25;
    assert_eq!(g.recover_place(&req, &HashSet::new()).unwrap(), None);
}

/// THE scope rule. Both venues' resting lists LAG a write, so an order we placed
/// (or even cancelled) a moment ago is still on them. Adopting one of those as
/// the id of a DIFFERENT order would point the next cancel at the wrong order.
#[test]
fn an_order_this_process_already_claimed_is_never_adopted_again() {
    let g = gw(vec![(200, &format!("[{OPEN_ORDER_5}]"))]);
    let mut req = place_req();
    req.qty = 5;
    let claimed: HashSet<String> = ["pm-lost".to_string()].into_iter().collect();
    assert_eq!(g.recover_place(&req, &claimed).unwrap(), None);
}

/// Two indistinguishable candidates is a REFUSAL with a name, never a coin
/// flip. The account is SHARED (docs/venue-quirks.md
/// §xv-graceful-shutdown-cancels-orders: "scope any sweep to orders this process
/// owns"), and an adopted order gets cancelled later — cancelling another
/// workstream's order is worse than the leak.
#[test]
fn two_indistinguishable_candidates_are_refused_not_guessed_between() {
    let open = format!(
        r#"[{OPEN_ORDER_5},{{"id":"pm-twin","marketSlug":"will-x-happen","quantity":5}}]"#
    );
    let g = gw(vec![(200, &open)]);
    let mut req = place_req();
    req.qty = 5;
    match g.recover_place(&req, &HashSet::new()) {
        Err(VenueError::Status { endpoint: "pmus recover_place", body, .. }) => {
            assert!(body.contains("SHARED"), "{body}");
            assert!(body.contains("pm-lost") && body.contains("pm-twin"), "names both: {body}");
        }
        other => panic!("an ambiguous match must be refused, got {other:?}"),
    }
}

/// A read that fails is not an empty book: it must not read as "nothing to
/// recover", because that is the answer that leaves the order resting.
#[test]
fn an_unreadable_open_orders_read_is_an_error_not_an_empty_answer() {
    let g = gw(vec![(500, "boom")]);
    assert!(g.recover_place(&place_req(), &HashSet::new()).is_err());
}

/// Positions are a dict KEYED BY SLUG with a string netPosition.
#[test]
fn positions_are_keyed_by_slug_with_string_quantities() {
    let g = gw(vec![(
        200,
        r#"{"positions":{"will-x-happen":{"netPosition":"-25","avgPx":{"value":"0.3300"}}}}"#,
    )]);
    let p = g.positions().unwrap();
    assert_eq!(p.positions["will-x-happen"].net_position, "-25");
}

/// Ed25519 signing is deterministic, so this verifies the exact bytes the
/// venue will check: ts + METHOD + path, with the four headers present.
#[test]
fn the_request_is_signed_over_ts_method_and_path() {
    let g = gw(vec![(200, CREATED)]);
    g.place(&place_req()).unwrap();
    let s = &g.transport.sent()[0];

    let h = |k: &str| s.headers.iter().find(|(hk, _)| hk == k).map(|(_, v)| v.clone());
    let ts = h("X-PM-Timestamp").expect("a timestamp header");
    let sig = h("X-PM-Signature").expect("a signature header");
    assert!(h("X-PM-Access-Key").is_some(), "a key-id header");
    assert_eq!(
        h("Content-Type").as_deref(),
        Some("application/json"),
        "PM-US requires the JSON content type"
    );

    use base64::Engine;
    use ed25519_dalek::{Signature, Verifier};
    let vk = ed25519_dalek::SigningKey::from_bytes(&seed32()).verifying_key();

    let sig_bytes = base64::engine::general_purpose::STANDARD.decode(&sig).unwrap();
    let sig = Signature::from_slice(&sig_bytes).unwrap();
    assert!(
        vk.verify(format!("{ts}POST/v1/orders").as_bytes(), &sig).is_ok(),
        "signature must cover ts + METHOD + path"
    );
}

// ------------------------------------------------ leave nothing behind ---

#[test]
fn rehearse_places_one_contract_at_a_penny_then_cancels() {
    let g = gw(vec![(200, CREATED), (200, OPEN_ORDER), (200, "{}")]);
    assert_eq!(g.rehearse(SLUG).unwrap(), "pm-1");

    let sent = g.transport.sent();
    assert_eq!(sent.len(), 3, "create, status, cancel");
    let b = sent[0].body.as_ref().unwrap();
    assert_eq!(b["quantity"], 1);
    assert_eq!(b["price"]["value"], "0.0100");
    assert_eq!(b["participateDontInitiate"], true);
    assert_eq!(sent[2].path, "/v1/order/pm-1/cancel");
    assert_eq!(sent[2].body.as_ref().unwrap()["marketSlug"], SLUG);
}

/// A status call that errors must STILL cancel — same guarantee as Kalshi,
/// where getting this wrong left a live order resting.
#[test]
fn a_status_error_still_cancels_the_order() {
    let g = gw(vec![(200, CREATED), (500, "boom"), (200, "{}")]);
    assert!(g.rehearse(SLUG).is_err());
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[2].path, "/v1/order/pm-1/cancel", "must cancel anyway");
}

/// A rehearsal order that actually FILLED is a failure, not a pass.
#[test]
fn a_filled_rehearsal_order_is_a_failure() {
    let filled = r#"{"id":"pm-1","cumQuantity":1,"avgPx":{"value":"0.0100"}}"#;
    let g = gw(vec![(200, CREATED), (200, filled), (200, "{}")]);
    match g.rehearse(SLUG) {
        Err(VenueError::Status { body, .. }) => assert!(body.contains("FILLED"), "{body}"),
        other => panic!("expected a filled-order failure, got {other:?}"),
    }
}

/// A cancel that fails is escalated loudly — this is the only outcome that
/// really leaves something on the venue.
#[test]
fn a_cancel_that_fails_says_check_the_venue() {
    let g = gw(vec![(200, CREATED), (200, OPEN_ORDER), (500, "nope")]);
    match g.rehearse(SLUG) {
        Err(VenueError::Status { body, .. }) => {
            assert!(body.contains("COULD NOT BE CANCELLED"), "{body}");
            assert!(body.contains("CHECK THE VENUE"), "{body}");
        }
        other => panic!("expected a loud escalation, got {other:?}"),
    }
}

/// `xv-shared-api-budget` is PM-US's quirk first — it is the metered venue
/// (`BUDGET_PER_MIN = 30`), and the Python this ports never opened the budget
/// for an order at all: "the order/hedge path must bypass it and never wait".
/// A drained budget refuses a background READ and lets the order path through.
#[test]
fn a_drained_budget_refuses_a_read_but_never_the_order_path() {
    let g = PmusGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0), // not one token, ever
        MockTransport::new(vec![(200, CREATED), (200, "{}"), (200, "[]")]),
    );
    let hedge = PlaceRequest {
        market: SLUG.into(),
        side: Side::Ask,
        price: "0.3000".into(),
        qty: 5,
        tif: Tif::Ioc,
        post_only: false,
        client_order_id: String::new(),
    };
    assert_eq!(g.place(&hedge).unwrap().id, "pm-1");
    g.cancel(&CancelRequest {
        by: CancelBy::VenueId("pm-1".into()),
        market_slug: Some(SLUG.into()),
    })
    .unwrap();
    assert_eq!(g.transport.sent().len(), 2, "hedge and cancel both went");

    // ...as does the resting-order read the sweep's proof-of-clean depends on.
    assert!(g.resting_order_ids().is_ok(), "a halt must not be refusable");
    assert_eq!(g.transport.sent().len(), 3);

    // The budget still exists, and still refuses what it is there to meter.
    assert!(matches!(g.positions(), Err(VenueError::RateLimited { .. })));
    assert_eq!(g.transport.sent().len(), 3, "the background read never reached the wire");
}

/// The inert default still cannot reach a venue.
#[test]
fn the_default_pmus_gateway_is_not_wired() {
    let g = PmusGateway::new(signer(), RateLimiter::from_per_minute(60.0, 0));
    assert_eq!(g.place(&place_req()).unwrap_err(), VenueError::NotWired);
    assert_eq!(
        g.cancel(&CancelRequest {
            by: CancelBy::VenueId("pm-1".into()),
            market_slug: Some(SLUG.into())
        })
        .unwrap_err(),
        VenueError::NotWired
    );
    assert_eq!(g.rehearse(SLUG).unwrap_err(), VenueError::NotWired);
}

/// The EXACT /v1/account/balances body PM-US returned on 2026-07-27. Every
/// money field is a JSON NUMBER, not a string — the first PM-US preflight
/// failed on exactly this. Pinned verbatim.
#[test]
fn the_real_pmus_balances_response_parses() {
    const LIVE: &str = r#"{"balances":[{"currentBalance":653.15805,"currency":"USD","lastUpdated":null,"buyingPower":329.29805,"assetNotional":0,"assetAvailable":0,"pendingCredit":0,"openOrders":0,"unsettledFunds":0,"pendingWithdrawals":[],"marginRequirement":323.86}]}"#;
    let g = gw(vec![(200, LIVE)]);
    let b = g.balances().expect("the live balances shape must parse");
    assert_eq!(b.balances[0].buying_power, "329.29805", "digits preserved exactly");
}

/// ...and a string still parses, so the venue may send either.
#[test]
fn a_string_money_field_still_parses() {
    let g = gw(vec![(200, r#"{"balances":[{"buyingPower":"329.29805"}]}"#)]);
    assert_eq!(g.balances().unwrap().balances[0].buying_power, "329.29805");
}

/// Order PRICES are strings and must stay strings — a float never crosses this
/// boundary, whichever way the venue sends it.
#[test]
fn a_numeric_price_is_kept_as_an_exact_string() {
    let g = gw(vec![(200, r#"{"id":"pm-1","cumQuantity":2,"avgPx":{"value":0.4225}}"#)]);
    let o = g.order_status("pm-1").unwrap();
    assert_eq!(
        o.fill_state(),
        arb_venue::resp::PmFillState::Filled { cum_quantity: 2, avg_px: "0.4225".into() }
    );
}

// ---------------------------------------- net_positions (naked-leg recon) ---

/// The normalized read: slug -> SIGNED contract count, from the string
/// `netPosition`.
#[test]
fn net_positions_are_signed_counts_keyed_by_slug() {
    let g = gw(vec![(
        200,
        r#"{"positions":{"will-x":{"netPosition":"-25"},"will-y":{"netPosition":"3.5"}}}"#,
    )]);
    let p = g.net_positions().expect("read");
    assert_eq!(p["will-x"], -25.0, "shorts keep their sign");
    assert_eq!(p["will-y"], 3.5, "and PM positions are genuinely fractional");
}

/// THE EMPTY GLITCH (`pmus-positions-empty-glitch`). This endpoint serves an
/// empty map while positions are held, and the documented consequence is false
/// NAKED alerts. It must error so a caller re-fetches, exactly as
/// `PmusSession.get_positions` raises unless `allow_empty`.
#[test]
fn an_empty_positions_map_is_the_glitch_not_a_flat_account() {
    let g = gw(vec![(200, r#"{"positions":{}}"#)]);
    match g.net_positions() {
        Err(VenueError::Status { body, .. }) => assert!(body.contains("EMPTY"), "{body}"),
        other => panic!("an empty read must not be reported as a flat account: {other:?}"),
    }
    // ...and the RAW read is unchanged: `positions()` is still the honest
    // primitive, so nothing that already reads it starts erroring.
    let g = gw(vec![(200, r#"{"positions":{}}"#)]);
    assert!(g.positions().expect("raw read still works").positions.is_empty());
}

/// A partial map is refused for the same reason: missing slugs read as zero,
/// and this endpoint is documented to serve partial sets during incidents.
#[test]
fn a_truncated_positions_map_is_refused() {
    for body in [
        r#"{"positions":{"will-x":{"netPosition":"-25"}},"next_cursor":"c1"}"#,
        // ...and the camelCase spelling this venue actually uses everywhere
        // else, which is the one it would send if it ever paginated at all.
        r#"{"positions":{"will-x":{"netPosition":"-25"}},"nextCursor":"c1"}"#,
        r#"{"positions":{"will-x":{"netPosition":"-25"}},"eof":false}"#,
    ] {
        let g = gw(vec![(200, body)]);
        match g.net_positions() {
            Err(VenueError::Status { body: b, .. }) => assert!(b.contains("TRUNCATED"), "{b}"),
            other => panic!("a partial map must be refused: {other:?}"),
        }
    }
}

/// A `netPosition` that will not parse is an error, never 0.
#[test]
fn an_unreadable_net_position_is_not_zero() {
    let g = gw(vec![(200, r#"{"positions":{"will-x":{"netPosition":"many"}}}"#)]);
    match g.net_positions() {
        Err(VenueError::Parse { detail, .. }) => assert!(detail.contains("will-x"), "{detail}"),
        other => panic!("a count we cannot read must not become 0: {other:?}"),
    }
}

/// THE 2026-07-31 SHAPE: `/v1/portfolio/positions` answering 503, repeatedly.
/// The one answer it must never produce is "nothing held".
#[test]
fn a_503_is_not_an_empty_portfolio() {
    let g = gw(vec![(503, "upstream unavailable")]);
    assert!(g.net_positions().is_err());
}

// ------------------------------------------------- spendable_cash (C13) ---

/// THE DOUBLE-COUNT TRAP, pinned. `currentBalance = buyingPower +
/// marginRequirement` (quirk `pmus-margin-is-one-dollar-per-short`), and the
/// margin is $1.00 per short contract that the position's own cost basis
/// already carries — so spending against `currentBalance` counts it twice, the
/// bug the 2026-08-14 accounting audit found. The wire body here is the live
/// 2026-08-14 shape: 653.15805 = 329.29805 + 323.86.
#[test]
fn pmus_spendable_cash_is_buying_power_and_never_current_balance() {
    let g = gw(vec![(
        200,
        r#"{"balances":[{"currentBalance":653.15805,"marginRequirement":323.86,
            "buyingPower":329.29805}]}"#,
    )]);
    assert_eq!(g.spendable_cash().expect("read"), "329.29805");
}

/// NO ROWS is an error, never "$0" — [`VenueGateway::net_positions`]' rule. An
/// empty answer is an affirmative claim about the account, and this one reads
/// as "this venue can buy nothing", which closes every basket with a leg on it
/// while the endpoint is merely being odd.
#[test]
fn an_empty_balances_array_is_refused_rather_than_read_as_no_money() {
    let g = gw(vec![(200, r#"{"balances":[]}"#)]);
    match g.spendable_cash() {
        Err(VenueError::Status { body, .. }) => assert!(body.contains("EMPTY"), "{body}"),
        other => panic!("an empty array must not be reported as $0 spendable: {other:?}"),
    }
}
