//! The M2 gate (docs/migration-plan.md): the venue-quirk tests the first live
//! venue-write must pass BEFORE it is fired at a real account.
//!
//! Every request is captured, so these assert on what actually went on the
//! wire — path, query, signed message, body, method — not on what the code
//! meant to send. The fixtures are the shapes the Python gateway recorded from
//! the live venue (tests/test_venue_contracts.py, now retired with the Python
//! trader).

use arb_venue::gateway::{CancelRequest, KalshiGateway, PlaceRequest, Side, Tif, VenueGateway};
use arb_venue::ratelimit::RateLimiter;
use arb_venue::transport::{Response, Transport};
use arb_venue::{KalshiSigner, VenueError};
use serde_json::Value;
use std::sync::Mutex;

#[derive(Debug, Clone)]
struct Sent {
    method: String,
    path: String,
    query: Option<String>,
    body: Option<Value>,
    headers: Vec<(String, String)>,
}

/// Canned (status, body) per call, in order; records every request.
struct MockTransport {
    replies: Mutex<Vec<(u16, String)>>,
    sent: Mutex<Vec<Sent>>,
}

impl MockTransport {
    fn new(replies: Vec<(u16, &str)>) -> Self {
        Self {
            replies: Mutex::new(
                replies.into_iter().map(|(s, b)| (s, b.to_string())).collect(),
            ),
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
        query: Option<&str>,
        headers: &[(&'static str, String)],
        body: Option<&Value>,
    ) -> Result<Response, VenueError> {
        self.sent.lock().unwrap().push(Sent {
            method: method.to_string(),
            path: path.to_string(),
            query: query.map(|q| q.to_string()),
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

fn signer() -> KalshiSigner {
    let v: Value =
        serde_json::from_str(include_str!("fixtures/venue/sigs.json")).unwrap();
    KalshiSigner::from_pkcs8_pem(
        v["kalshi"]["api_key_id"].as_str().unwrap(),
        v["kalshi"]["private_key_pkcs8_pem"].as_str().unwrap(),
    )
    .unwrap()
}

fn gw(replies: Vec<(u16, &str)>) -> KalshiGateway<MockTransport> {
    KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(600.0, 600.0, 0),
        MockTransport::new(replies),
    )
    .with_settle(std::time::Duration::ZERO, 1)
}

const ORDER_RESTING: &str = r#"{"order":{"order_id":"o-1","status":"resting","ticker":"KXTEST","side":"yes","action":"buy"}}"#;

fn place_req() -> PlaceRequest {
    PlaceRequest {
        market: "KXTEST".into(),
        side: Side::Bid,
        price: "0.0100".into(),
        qty: 1,
        tif: Tif::Gtc,
        post_only: true,
        client_order_id: "c-1".into(),
    }
}

// ---------------------------------------------------------------- quirks ---

/// K2, the one that fails closed as a mystery 401: the signed message is
/// `ts + METHOD + path` with NO query string, even when the URL carries one.
#[test]
fn the_signature_covers_the_path_and_never_the_query() {
    let g = gw(vec![(200, r#"{"orders":[],"cursor":null}"#)]);
    g.all_orders().unwrap();
    let s = &g.transport.sent()[0];
    assert_eq!(s.path, "/trade-api/v2/portfolio/orders");
    assert_eq!(s.query.as_deref(), Some("limit=100"), "query rides the URL");

    let ts = s.headers.iter().find(|(k, _)| k == "KALSHI-ACCESS-TIMESTAMP").unwrap().1.clone();
    let sig = s.headers.iter().find(|(k, _)| k == "KALSHI-ACCESS-SIGNATURE").unwrap().1.clone();
    let v: Value = serde_json::from_str(include_str!("fixtures/venue/sigs.json")).unwrap();
    let verifier = arb_venue::KalshiVerifier::from_public_key_pem(
        v["kalshi"]["public_key_spki_pem"].as_str().unwrap(),
    )
    .unwrap();
    use base64::Engine;
    let raw = base64::engine::general_purpose::STANDARD.decode(&sig).unwrap();
    assert!(
        verifier.verify(&ts, "GET", "/trade-api/v2/portfolio/orders", &raw),
        "signature must cover the bare path"
    );
    assert!(
        !verifier.verify(&ts, "GET", "/trade-api/v2/portfolio/orders?limit=100", &raw),
        "signing the query would be a 401 in production"
    );
}

/// Create moved to /portfolio/events/orders — the legacy POST /portfolio/orders
/// now 410s, so a wrong path here is a hard outage of the order path.
#[test]
fn place_posts_to_the_events_orders_path_with_the_v2_body() {
    let g = gw(vec![(201, ORDER_RESTING)]);
    let o = g.place(&place_req()).unwrap();
    assert_eq!(o.order_id, "o-1");
    let s = &g.transport.sent()[0];
    assert_eq!(s.method, "POST");
    assert_eq!(s.path, "/trade-api/v2/portfolio/events/orders");
    let b = s.body.as_ref().unwrap();
    assert_eq!(b["ticker"], "KXTEST");
    assert_eq!(b["count"], "1.00", "count is a 2dp STRING, not an int");
    assert_eq!(b["price"], "0.0100");
    assert_eq!(b["post_only"], true);
    assert_eq!(b["time_in_force"], "good_till_canceled");
}

/// K5: a post_only order that would cross is rejected with a 400. It must
/// surface — a crossing post_only means our book view was wrong, and
/// swallowing it would silently turn a maker into a taker.
#[test]
fn a_crossing_post_only_400_is_surfaced_not_swallowed() {
    let g = gw(vec![(400, r#"{"error":{"code":"post_only_would_cross"}}"#)]);
    match g.place(&place_req()) {
        Err(VenueError::Status { status, endpoint, .. }) => {
            assert_eq!(status, 400);
            assert_eq!(endpoint, "kalshi place");
        }
        other => panic!("expected a surfaced 400, got {other:?}"),
    }
}

/// K4: 404 on cancel means the order is already gone, which is exactly what
/// cancel wanted. Treating it as an error orphans resting orders on retry.
#[test]
fn cancel_treats_404_as_success() {
    let g = gw(vec![(404, r#"{"error":"order not found"}"#)]);
    g.cancel(&CancelRequest { order_id: "o-1".into(), market_slug: None })
        .expect("404 on cancel is success");
    let s = &g.transport.sent()[0];
    assert_eq!(s.method, "DELETE");
    assert_eq!(s.path, "/trade-api/v2/portfolio/events/orders/o-1");
}

/// ...but a real failure is still a failure.
#[test]
fn cancel_still_fails_on_a_server_error() {
    let g = gw(vec![(500, "boom")]);
    assert!(matches!(
        g.cancel(&CancelRequest { order_id: "o-1".into(), market_slug: None }),
        Err(VenueError::Status { status: 500, .. })
    ));
}

/// K1: /portfolio/orders is paginated and `?status=resting` returns nothing,
/// so the sweep pages the FULL list and filters client-side. Without this,
/// older resting orders are never cancelled — naked-leg risk.
#[test]
fn cancel_all_open_pages_the_full_history_and_cancels_only_resting() {
    let page1 = r#"{"orders":[{"order_id":"a","status":"resting"},{"order_id":"b","status":"canceled"}],"cursor":"CUR"}"#;
    let page2 = r#"{"orders":[{"order_id":"c","status":"executed"},{"order_id":"d","status":"resting"}],"cursor":null}"#;
    let g = gw(vec![(200, page1), (200, page2), (200, "{}"), (200, "{}")]);
    g.cancel_all_open().unwrap();

    let sent = g.transport.sent();
    assert_eq!(sent[0].query.as_deref(), Some("limit=100"));
    assert_eq!(sent[1].query.as_deref(), Some("limit=100&cursor=CUR"), "follows the cursor");
    let cancelled: Vec<&str> = sent[2..].iter().map(|s| s.path.as_str()).collect();
    assert_eq!(
        cancelled,
        vec![
            "/trade-api/v2/portfolio/events/orders/a",
            "/trade-api/v2/portfolio/events/orders/d",
        ],
        "only the RESTING orders, across both pages"
    );
}

/// The rehearsal contract: place 1 contract at 1c, confirm it RESTS, cancel.
#[test]
fn rehearse_places_one_contract_at_a_penny_confirms_it_rests_then_cancels() {
    let g = gw(vec![(201, ORDER_RESTING), (200, ORDER_RESTING), (200, "{}")]);
    let oid = g.rehearse("KXTEST").unwrap();
    assert_eq!(oid, "o-1");

    let sent = g.transport.sent();
    assert_eq!(sent.len(), 3, "place, status, cancel — nothing else");

    let b = sent[0].body.as_ref().unwrap();
    assert_eq!(b["count"], "1.00", "exactly ONE contract");
    assert_eq!(b["price"], "0.0100", "1c — far below any real bid");
    assert_eq!(b["post_only"], true, "must rest, never cross");
    assert_eq!(b["side"], "bid", "YES axis, bid side");

    assert_eq!(sent[1].method, "GET");
    assert_eq!(sent[1].path, "/trade-api/v2/portfolio/orders/o-1");
    assert_eq!(sent[2].method, "DELETE");
}

/// If the order did not rest, the rehearsal FAILS loudly rather than reporting
/// a pass — an order that vanishes between place and status is the exact
/// failure this exists to catch.
#[test]
fn rehearse_fails_when_the_order_never_rested() {
    let executed = r#"{"order":{"order_id":"o-1","status":"executed"}}"#;
    let g = gw(vec![(201, ORDER_RESTING), (200, executed), (200, "{}")]);
    match g.rehearse("KXTEST") {
        Err(VenueError::Status { endpoint: "kalshi rehearse", body, .. }) => {
            assert!(body.contains("did not rest"), "{body}");
        }
        other => panic!("expected a rehearsal failure, got {other:?}"),
    }
}

/// A missing money field is an error, never a silent 0.
#[test]
fn a_missing_balance_field_is_a_typed_error() {
    let g = gw(vec![(200, r#"{}"#)]);
    assert!(matches!(g.balances(), Err(VenueError::MissingField { .. })));
}

/// The local budget refuses before the venue has to 429 us.
#[test]
fn an_exhausted_local_rate_budget_refuses_without_sending() {
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0.0, 0),
        MockTransport::new(vec![]),
    );
    assert!(matches!(g.place(&place_req()), Err(VenueError::RateLimited { .. })));
    assert!(g.transport.sent().is_empty(), "nothing may reach the wire");
}

// ------------------------------------------- create-response shapes (live) ---

/// The create path does NOT answer with the single-order GET envelope. The
/// first live smoke (2026-07-27) failed with "missing required field `order`"
/// AFTER the order was already resting. Accept every shape Python tolerates.
#[test]
fn a_create_response_is_read_in_any_of_its_shapes() {
    for (label, body) in [
        ("enveloped", r#"{"order":{"order_id":"o-1","status":"resting"}}"#),
        ("bare", r#"{"order_id":"o-1","status":"resting"}"#),
        ("plural", r#"{"orders":[{"order_id":"o-1","status":"resting"}]}"#),
    ] {
        let g = gw(vec![(201, body)]);
        let o = g.place(&place_req()).unwrap_or_else(|e| panic!("{label}: {e}"));
        assert_eq!(o.order_id, "o-1", "{label}");
    }
}

/// An unreadable create must carry the RAW body, so the shape is in the log
/// rather than guessed at a second time.
#[test]
fn an_unreadable_create_reports_the_raw_body() {
    let g = gw(vec![(201, r#"{"weird":"shape"}"#), (200, r#"{"orders":[],"cursor":null}"#)]);
    match g.place(&place_req()) {
        Err(VenueError::Parse { endpoint: "kalshi:create", detail }) => {
            assert!(detail.contains(r#"{"weird":"shape"}"#), "{detail}");
        }
        other => panic!("expected a Parse error carrying the body, got {other:?}"),
    }
}

// ------------------------------------------------ leave nothing behind ---

/// The rehearsal's whole promise. If the status check fails, the order must
/// STILL be cancelled — the first live run left one resting because the error
/// path returned early.
#[test]
fn a_failed_status_check_still_cancels_the_order() {
    let executed = r#"{"order":{"order_id":"o-1","status":"executed"}}"#;
    let g = gw(vec![(201, ORDER_RESTING), (200, executed), (200, "{}")]);
    assert!(g.rehearse("KXTEST").is_err());
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 3, "place, status, cancel");
    assert_eq!(sent[2].method, "DELETE", "the order MUST be cancelled anyway");
    assert_eq!(sent[2].path, "/trade-api/v2/portfolio/events/orders/o-1");
}

/// Same promise when the status call itself errors out.
#[test]
fn a_status_call_that_errors_still_cancels_the_order() {
    let g = gw(vec![(201, ORDER_RESTING), (500, "boom"), (200, "{}")]);
    assert!(g.rehearse("KXTEST").is_err());
    let sent = g.transport.sent();
    assert_eq!(sent[2].method, "DELETE", "a 500 on status must not orphan the order");
}

/// A cancel that fails is escalated in the loudest terms — this is the only
/// outcome where something really is left on the venue.
#[test]
fn a_cancel_that_fails_says_check_the_venue() {
    let g = gw(vec![(201, ORDER_RESTING), (200, ORDER_RESTING), (500, "nope")]);
    match g.rehearse("KXTEST") {
        Err(VenueError::Status { body, .. }) => {
            assert!(body.contains("COULD NOT BE CANCELLED"), "{body}");
            assert!(body.contains("CHECK THE VENUE"), "{body}");
        }
        other => panic!("expected a loud escalation, got {other:?}"),
    }
}

/// The nastiest case: the POST reached the venue but we could not read the
/// reply, so we never learned the order id. The client_order_id is the only
/// handle on it — sweep, cancel, and report the recovery.
#[test]
fn an_unreadable_create_recovers_the_orphan_by_client_order_id() {
    // create (unreadable) -> all_orders page -> DELETE the match
    let page = r#"{"orders":[
        {"order_id":"other","status":"resting","client_order_id":"someone-else"},
        {"order_id":"orphan","status":"resting","client_order_id":"THE-COID"}
    ],"cursor":null}"#;
    let g = gw(vec![(201, r#"{"unreadable":true}"#), (200, page), (200, "{}")]);

    // rehearse mints its own coid, so drive the recovery path directly.
    match g.place(&place_req()) {
        Err(VenueError::Parse { .. }) => {}
        other => panic!("expected an unreadable create, got {other:?}"),
    }
    let recovered = g.cancel_by_client_order_id("THE-COID").unwrap();
    assert_eq!(recovered.as_deref(), Some("orphan"));
    let sent = g.transport.sent();
    assert_eq!(sent[2].method, "DELETE");
    assert_eq!(
        sent[2].path, "/trade-api/v2/portfolio/events/orders/orphan",
        "only the order carrying OUR client_order_id"
    );
}

/// Nothing of ours resting => nothing to clean up, and nothing cancelled.
#[test]
fn the_orphan_sweep_cancels_nothing_when_no_order_is_ours() {
    let page = r#"{"orders":[{"order_id":"other","status":"resting","client_order_id":"not-ours"}],"cursor":null}"#;
    let g = gw(vec![(200, page)]);
    assert_eq!(g.cancel_by_client_order_id("THE-COID").unwrap(), None);
    assert_eq!(g.transport.sent().len(), 1, "no DELETE may be sent");
}

/// The EXACT create body Kalshi returned on 2026-07-27. No `status`, no
/// `ticker`, and `fill_count` rather than `fill_count_fp`. Pinned verbatim so
/// the shape that broke two live smokes can never break a third.
#[test]
fn the_real_kalshi_create_response_parses() {
    const LIVE: &str = r#"{"client_order_id":"rehearse-1785197117443","fill_count":"0.00","order_id":"e68b62c0-b2f7-4c2c-b7f0-9d0d86965cfc","remaining_count":"1.00","ts_ms":1785197117488}"#;
    let g = gw(vec![(201, LIVE)]);
    let o = g.place(&place_req()).expect("the live create shape must parse");
    assert_eq!(o.order_id, "e68b62c0-b2f7-4c2c-b7f0-9d0d86965cfc");
    assert_eq!(o.client_order_id.as_deref(), Some("rehearse-1785197117443"));
    assert_eq!(o.filled_qty(), 0, "fill_count is read like fill_count_fp");
    assert!(o.status.is_none(), "a create carries NO status");
    assert!(!o.is_resting(), "'exists' must never be mistaken for 'resting'");
}

/// Following the create with the GET is therefore mandatory, not belt-and-
/// braces: the create alone cannot tell us the order rested.
#[test]
fn rehearse_passes_on_the_real_create_shape_via_the_status_get() {
    const LIVE: &str = r#"{"client_order_id":"c","fill_count":"0.00","order_id":"o-1","remaining_count":"1.00","ts_ms":1}"#;
    let g = gw(vec![(201, LIVE), (200, ORDER_RESTING), (200, "{}")]);
    assert_eq!(g.rehearse("KXTEST").unwrap(), "o-1");
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 3);
    assert_eq!(sent[1].path, "/trade-api/v2/portfolio/orders/o-1", "status comes from the GET");
    assert_eq!(sent[2].method, "DELETE");
}

/// Kalshi is not read-your-writes: a GET on a just-placed order 404s for a
/// beat. That 404 means "not yet", not "no such order" — the create already
/// proved it exists. The third live smoke (2026-07-27) failed exactly here.
#[test]
fn a_404_on_a_freshly_created_order_is_retried_not_believed() {
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(600.0, 600.0, 0),
        MockTransport::new(vec![
            (201, ORDER_RESTING),
            (404, r#"{"error":{"code":"not_found"}}"#), // query service lagging
            (404, r#"{"error":{"code":"not_found"}}"#),
            (200, ORDER_RESTING),                        // ...now visible
            (200, "{}"),                                 // cancel
        ]),
    )
    .with_settle(std::time::Duration::ZERO, 8);

    assert_eq!(g.rehearse("KXTEST").unwrap(), "o-1", "the lag must not fail the run");
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 5, "place, 3 status polls, cancel");
    assert_eq!(sent[4].method, "DELETE");
}

/// But a 404 that never clears still fails — and still cancels.
#[test]
fn an_order_that_never_becomes_visible_fails_and_is_still_cancelled() {
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(600.0, 600.0, 0),
        MockTransport::new(vec![
            (201, ORDER_RESTING),
            (404, "{}"),
            (404, "{}"),
            (200, "{}"), // cancel still fires
        ]),
    )
    .with_settle(std::time::Duration::ZERO, 2);

    assert!(g.rehearse("KXTEST").is_err());
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 4);
    assert_eq!(sent[3].method, "DELETE", "never leave it resting");
}
