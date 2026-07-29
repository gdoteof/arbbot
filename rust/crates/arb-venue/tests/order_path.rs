//! The M2 gate (docs/migration-plan.md): the venue-quirk tests the first live
//! venue-write must pass BEFORE it is fired at a real account.
//!
//! Every request is captured, so these assert on what actually went on the
//! wire — path, query, signed message, body, method — not on what the code
//! meant to send. The fixtures are the shapes the Python gateway recorded from
//! the live venue (tests/test_venue_contracts.py, now retired with the Python
//! trader).

use arb_venue::gateway::{
    CancelBy, CancelRequest, KalshiGateway, PlaceRequest, Side, Tif, VenueGateway,
};
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
        RateLimiter::from_per_minute(600.0, 0),
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
/// Sound ONLY for a venue id — see `a_client_id_absent_from_the_account_is_not_a_cancel`.
#[test]
fn cancel_treats_404_as_success() {
    let g = gw(vec![(404, r#"{"error":"order not found"}"#)]);
    g.cancel(&CancelRequest { by: CancelBy::VenueId("o-1".into()), market_slug: None })
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
        g.cancel(&CancelRequest { by: CancelBy::VenueId("o-1".into()), market_slug: None }),
        Err(VenueError::Status { status: 500, .. })
    ));
}

// ------------------------------------------- cancelling the RIGHT order ---
//
// Until 2026-07-28 every per-order cancel this stack sent carried OUR
// client_order_id (`m1785257819045`) in the path. Kalshi answered 404, K4 above
// mapped that to success, and the order kept resting. `CancelBy` makes the two
// id namespaces distinct so a caller has to say which one it has.

/// A client-id cancel resolves through the venue's OWN order list and then
/// deletes the venue's id — never our own.
#[test]
fn a_client_id_cancel_is_resolved_to_the_venues_own_order_id() {
    let page = r#"{"orders":[
        {"order_id":"someone-else","status":"resting","client_order_id":"m999"},
        {"order_id":"66e1c799-507b","status":"resting","client_order_id":"m1785257819045"}
    ],"cursor":null}"#;
    let g = gw(vec![(200, page), (200, "{}")]);
    g.cancel(&CancelRequest {
        by: CancelBy::ClientId("m1785257819045".into()),
        market_slug: None,
    })
    .expect("a resolvable client id must cancel");

    let sent = g.transport.sent();
    assert_eq!(sent.len(), 2, "one list read, one DELETE");
    assert_eq!(sent[1].method, "DELETE");
    assert_eq!(
        sent[1].path, "/trade-api/v2/portfolio/events/orders/66e1c799-507b",
        "the VENUE's id — addressing our own m… id is the 2026-07-28 phantom cancel"
    );
}

/// THE finding, in one assertion: our order id must never appear in a cancel
/// path. A `VenueId` is the only thing that ever reaches the URL.
#[test]
fn our_own_order_id_never_appears_in_a_cancel_url() {
    let page = r#"{"orders":[{"order_id":"venue-abc","status":"resting","client_order_id":"m1"}],"cursor":null}"#;
    let g = gw(vec![(200, page), (200, "{}")]);
    g.cancel(&CancelRequest { by: CancelBy::ClientId("m1".into()), market_slug: None }).unwrap();
    for s in g.transport.sent() {
        assert!(
            !s.path.ends_with("/m1"),
            "a cancel addressed with our client_order_id reached the wire: {}",
            s.path
        );
    }
}

/// An order the account has never seen must NOT read as a successful cancel.
/// The order list lags a write (see `a_404_on_a_freshly_created_order_...`), so
/// "absent" can mean "not visible yet" just as easily as "never accepted" — and
/// either way nothing was cancelled. This is the case K4's 404 rule used to
/// swallow.
///
/// The message is deliberately plain rather than an alarm: the commonest cause
/// is a place the venue REJECTED, where there was never anything here to cancel.
#[test]
fn a_client_id_absent_from_the_account_is_not_a_cancel() {
    let g = gw(vec![(200, r#"{"orders":[],"cursor":null}"#)]);
    match g.cancel(&CancelRequest { by: CancelBy::ClientId("m1".into()), market_slug: None }) {
        Err(VenueError::Status { endpoint: "kalshi cancel", body, .. }) => {
            assert!(body.contains("nothing cancelled"), "{body}");
            assert!(body.contains("m1"), "the id must be in the error: {body}");
            assert!(
                body.to_uppercase() != body,
                "a routine, expected outcome must not be shouted: {body}"
            );
        }
        other => panic!("expected a refusal, got {other:?}"),
    }
    assert_eq!(g.transport.sent().len(), 1, "no DELETE may be sent");
}

/// ...but an order that IS on the account and already finished is a real
/// "already gone", which is the outcome cancel wanted. Distinguishing this from
/// the case above is the whole point.
#[test]
fn a_client_id_whose_order_is_already_finished_is_success() {
    for status in ["canceled", "executed"] {
        let page = format!(
            r#"{{"orders":[{{"order_id":"v-1","status":"{status}","client_order_id":"m1"}}],"cursor":null}}"#
        );
        let g = gw(vec![(200, &page)]);
        g.cancel(&CancelRequest { by: CancelBy::ClientId("m1".into()), market_slug: None })
            .unwrap_or_else(|e| panic!("{status} is already gone, not an error: {e}"));
        assert_eq!(g.transport.sent().len(), 1, "nothing to DELETE");
    }
}

/// An empty id would DELETE the collection path. Refuse locally.
#[test]
fn an_empty_order_id_never_reaches_the_wire() {
    let g = gw(vec![]);
    assert!(matches!(
        g.cancel(&CancelRequest { by: CancelBy::VenueId(String::new()), market_slug: None }),
        Err(VenueError::MissingField { endpoint: "kalshi cancel", .. })
    ));
    assert!(g.transport.sent().is_empty());
}

/// K1: /portfolio/orders is paginated and `?status=resting` returns nothing,
/// so the sweep pages the FULL list and filters client-side. Without this,
/// older resting orders are never cancelled — naked-leg risk.
#[test]
fn cancel_all_open_pages_the_full_history_and_cancels_only_resting() {
    let page1 = r#"{"orders":[{"order_id":"a","status":"resting","client_order_id":"m1"},{"order_id":"b","status":"canceled","client_order_id":"m2"}],"cursor":"CUR"}"#;
    let page2 = r#"{"orders":[{"order_id":"c","status":"executed","client_order_id":"m3"},{"order_id":"d","status":"resting","client_order_id":"m4"}],"cursor":null}"#;
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

// ------------------------------------------ an unreadable list is not empty ---

/// A 200 whose body has no top-level `orders` is a body we could not READ, and
/// the one answer it must never give is "the book is empty".
///
/// `KalshiOrdersPage` defaulted every field, so any json object deserialized:
/// an error envelope served with 200 became `Ok(vec![])`, `cancel_all_open`
/// cancelled nothing and returned `Ok(())`, and `resting_order_ids` handed the
/// sweep the empty list it accepts as PROOF. Latent — neither venue has been
/// seen serving such a body — but this is the one place a wrong default is an
/// affirmative claim rather than a missing value.
#[test]
fn a_200_with_no_orders_key_is_unreadable_not_an_empty_book() {
    for body in [
        r#"{"error":{"code":"internal_error","message":"try again"}}"#,
        r#"{"data":{"orders":[]}}"#, // a re-nesting: `orders` is not top-level
        "{}",
    ] {
        match gw(vec![(200, body)]).all_orders() {
            Err(VenueError::MissingField { endpoint: "kalshi:orders", field }) => {
                assert_eq!(field, "orders", "{body}")
            }
            other => panic!("{body} must be unreadable, got {other:?}"),
        }
        assert!(
            gw(vec![(200, body)]).resting_order_ids().is_err(),
            "the sweep's PROOF read must fail too, not answer `empty`: {body}"
        );
        assert!(
            gw(vec![(200, body)]).cancel_all_open().is_err(),
            "a sweep over a book it never read must not return Ok: {body}"
        );
    }
}

// -------------------------------------------------- whose orders these are ---

/// THE scope rule (docs/venue-quirks.md §xv-graceful-shutdown-cancels-orders:
/// "scope any sweep to orders this process owns"). `arbbot-hedge.timer` places
/// real orders under this same primary Kalshi key every 5 minutes, and the
/// sweep — which runs on every armed start, every kill and every shutdown —
/// cancelled every resting order on the account regardless of whose it was.
///
/// The other workstream's order must also stay OUT of the proof, or a sweep
/// could never come back clean while that timer holds a quote.
#[test]
fn the_sweep_cancels_this_stacks_orders_and_leaves_another_workstreams_alone() {
    // the Python gateway tags every order `uuid.uuid4().hex` — 32 hex chars.
    let page = r#"{"orders":[
        {"order_id":"theirs","status":"resting","client_order_id":"9f1c4b7e2a6d40518c3b9e0f7a2d5c81"},
        {"order_id":"ours","status":"resting","client_order_id":"m1785257819045"}
    ],"cursor":null}"#;
    let g = gw(vec![(200, page), (200, "{}")]);
    g.cancel_all_open().unwrap();
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 2, "one list read, ONE cancel: {sent:?}");
    assert_eq!(
        sent[1].path, "/trade-api/v2/portfolio/events/orders/ours",
        "the other workstream's order must never reach a cancel"
    );

    let g = gw(vec![(200, page)]);
    assert_eq!(
        g.resting_order_ids().unwrap(),
        vec!["ours"],
        "and it must not count against a proof this sweep cannot act on"
    );
}

/// The catch an UNSCOPED sweep was the only backstop for, and the thing scoping
/// must not cost: a place that reached the venue and rested while its answer was
/// lost. The engine holds no venue id for it and its own cancel path can never
/// reach it.
///
/// It survives scoping because the tag goes out IN THE CREATE BODY, so the
/// order is already carrying it the first time the list shows it — no ack
/// needed. Same for an order a PREVIOUS run left resting through a SIGKILL:
/// `is_ours` is a property of this codebase's ids, not of one process's seed,
/// so both of the ids below are ours despite neither being minted here.
#[test]
fn a_place_whose_ack_never_came_back_is_still_caught_by_the_sweep() {
    let page = r#"{"orders":[
        {"order_id":"never-acked","status":"resting","client_order_id":"m1785257819053"},
        {"order_id":"last-run","status":"resting","client_order_id":"h1785171419001"}
    ],"cursor":null}"#;
    let g = gw(vec![(200, page), (200, "{}"), (200, "{}")]);
    g.cancel_all_open().unwrap();
    let cancelled: Vec<String> = g.transport.sent()[1..].iter().map(|s| s.path.clone()).collect();
    assert_eq!(
        cancelled,
        vec![
            "/trade-api/v2/portfolio/events/orders/never-acked",
            "/trade-api/v2/portfolio/events/orders/last-run",
        ],
        "an order this process could not name, and one from a run that is gone"
    );
}

/// A resting row with NO `client_order_id` is unattributable, and that is the
/// ownership-level form of the unreadable list the sweep already refuses to
/// treat as proof. Kalshi's create body REQUIRES the field, so this shape means
/// the LIST has stopped reporting the tag the whole scope rests on — fail
/// closed rather than let scoping quietly re-open the defect above.
#[test]
fn a_resting_order_with_no_client_order_id_is_neither_cancelled_nor_proven_clean() {
    let page = r#"{"orders":[{"order_id":"untagged","status":"resting"}],"cursor":null}"#;
    let g = gw(vec![(200, page)]);
    g.cancel_all_open().unwrap();
    assert_eq!(g.transport.sent().len(), 1, "nothing of ours claims it, so no cancel");

    let g = gw(vec![(200, page)]);
    assert_eq!(
        g.resting_order_ids().unwrap(),
        vec!["untagged"],
        "...but the book is NOT proven clean over it"
    );
}

/// The grammar itself, in the two directions that matter on a shared key.
#[test]
fn ownership_is_decided_by_the_tag_this_stack_mints() {
    use arb_venue::gateway::is_ours;
    for id in ["m1785257819045", "h1", "t42", "rehearse-1785197117443", "sweep-1234", "m0"] {
        assert!(is_ours(id), "{id} is minted by this tree");
    }
    for id in [
        "9f1c4b7e2a6d40518c3b9e0f7a2d5c81", // Python: uuid.uuid4().hex
        "",
        "m",             // a prefix with no counter is not an id
        "m17852578-1",   // nor one with anything but digits behind it
        "manual-order",  // a word that merely starts with `m`
        "theirs",
    ] {
        assert!(!is_ours(id), "{id} must never be swept as ours");
    }
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

/// The local budget refuses a background READ before the venue has to 429 us.
#[test]
fn an_exhausted_local_rate_budget_refuses_a_read_without_sending() {
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0),
        MockTransport::new(vec![]),
    );
    assert!(matches!(g.balances(), Err(VenueError::RateLimited { .. })));
    assert!(matches!(g.order_status("o-1"), Err(VenueError::RateLimited { .. })));
    assert!(g.transport.sent().is_empty(), "nothing may reach the wire");
}

/// A hedge / take-take leg: IOC, not post-only. It is sent because a maker leg
/// has ALREADY filled, so it is the one call that must never be refused.
fn hedge_req() -> PlaceRequest {
    PlaceRequest {
        market: "KXTEST".into(),
        side: Side::Ask,
        price: "0.3000".into(),
        qty: 5,
        tif: Tif::Ioc,
        post_only: false,
        client_order_id: "h-1".into(),
    }
}

/// `xv-shared-api-budget`: "the order/hedge path must bypass it and never
/// wait." Before 2026-07-29 it did neither, and this is the hazard that left —
/// a LATENT one, never yet fired: the busiest armed run wrote 417 orders in
/// 5.2h (~1.3/min) against a bucket of 60, and no `exec_failed` on record is a
/// rate-limit refusal (the only four, 2026-07-28 12:51, are Kalshi 409
/// `order_already_exists`). The hazard: a drained bucket answers `RateLimited`
/// before anything is signed or sent, `run_executor` logs FAILED and drops the
/// command, and the maker leg that has ALREADY filled is left naked while the
/// bucket refills at 1/s under a quoter that keeps spending it.
#[test]
fn a_drained_budget_does_not_refuse_the_hedge_or_its_cancel() {
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0), // not one token, ever
        MockTransport::new(vec![(201, ORDER_RESTING), (200, "{}")]),
    );
    assert_eq!(g.place(&hedge_req()).unwrap().order_id, "o-1");
    g.cancel(&CancelRequest { by: CancelBy::VenueId("o-1".into()), market_slug: None })
        .unwrap();

    let sent = g.transport.sent();
    assert_eq!(sent.len(), 2, "both reached the wire");
    assert_eq!(sent[0].method, "POST");
    assert_eq!(sent[1].method, "DELETE");
}

/// A reprice is a cancel plus a place, and both spend a CRITICAL token — two
/// per quote update, against the 60 the live sinks are built with, refilling
/// at 1/s. Ordinary quoting therefore draws down the budget a hedge would need,
/// and a reprice rate above one every two seconds exhausts it. Neither half
/// spends anything now, and the READ budget they were sharing is untouched.
#[test]
fn repricing_starves_neither_the_hedge_nor_the_read_budget() {
    const REPRICES: usize = 200;
    let mut replies: Vec<(u16, &str)> = Vec::new();
    for _ in 0..REPRICES {
        replies.push((200, "{}")); // cancel the resting quote
        replies.push((201, ORDER_RESTING)); // ...and re-place it
    }
    replies.push((201, ORDER_RESTING)); // the hedge, after all that quoting
    replies.push((200, ORDER_RESTING)); // and a background status poll

    let g = KalshiGateway::with_transport(
        signer(),
        // A read budget of exactly ONE token: 200 reprices must not take it.
        RateLimiter::from_per_minute(1.0, 0),
        MockTransport::new(replies),
    );
    for _ in 0..REPRICES {
        g.cancel(&CancelRequest { by: CancelBy::VenueId("o-1".into()), market_slug: None })
            .unwrap();
        g.place(&place_req()).unwrap(); // post-only GTC: a maker quote
    }

    assert!(g.place(&hedge_req()).is_ok(), "{REPRICES} reprices cost the hedge nothing");
    assert!(g.order_status("o-1").is_ok(), "nor the read budget its only token");
    assert_eq!(g.transport.sent().len(), REPRICES * 2 + 2);
}

/// The kill sweep is an order-path operation that begins with a READ: Kalshi's
/// `cancel_all_open` pages `all_orders()` before it can cancel anything, and
/// `resting_order_ids` — the only evidence `cancel_all_and_verify` accepts —
/// is the same read. Metering those left the halt itself refusable by a token
/// bucket, which is the hazard this whole change exists to remove.
#[test]
fn a_drained_budget_does_not_refuse_the_kill_sweep() {
    let page = r#"{"orders":[{"order_id":"66e1c799-507b","status":"resting","client_order_id":"m1"}],"cursor":null}"#;
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0), // not one token, ever
        MockTransport::new(vec![(200, page), (200, "{}")]),
    );
    assert_eq!(g.resting_order_ids().unwrap(), vec!["66e1c799-507b"], "the proof read goes");

    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0),
        MockTransport::new(vec![(200, page), (200, "{}")]),
    );
    g.cancel_all_open().expect("a halt must not be refusable by a local budget");
    let sent = g.transport.sent();
    assert_eq!(sent.len(), 2, "the list read, then the DELETE");
    assert_eq!(sent[1].method, "DELETE");
}

/// The other order-path call that must read first: a client-id cancel resolves
/// OUR id against the venue's own order list (`find_ours`). The engine emits
/// one on `CancelWork::Escalate` — a place whose ack never arrived — so this is
/// an armed path, and refusing its read refuses the cancel.
#[test]
fn a_drained_budget_does_not_refuse_a_cancel_that_must_read_first() {
    let page = r#"{"orders":[
        {"order_id":"66e1c799-507b","status":"resting","client_order_id":"m1785257819045"}
    ],"cursor":null}"#;
    let g = KalshiGateway::with_transport(
        signer(),
        RateLimiter::from_per_minute(0.0, 0), // not one token, ever
        MockTransport::new(vec![(200, page), (200, "{}")]),
    );
    g.cancel(&CancelRequest {
        by: CancelBy::ClientId("m1785257819045".into()),
        market_slug: None,
    })
    .expect("a resolvable client id must cancel on any budget");

    let sent = g.transport.sent();
    assert_eq!(sent.len(), 2, "one list read, one DELETE");
    assert_eq!(sent[1].path, "/trade-api/v2/portfolio/events/orders/66e1c799-507b");
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
        RateLimiter::from_per_minute(600.0, 0),
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
        RateLimiter::from_per_minute(600.0, 0),
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
