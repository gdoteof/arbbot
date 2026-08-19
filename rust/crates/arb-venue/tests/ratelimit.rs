//! RateLimiter is a pure type with injected `now_ns` — deterministic, no clock.

use arb_venue::gateway::{
    CancelBy, CancelRequest, KalshiGateway, PlaceRequest, Side, Tif, VenueGateway,
};
use arb_venue::ratelimit::{Priority, RateLimiter, TokenBucket};
use arb_venue::sign::PmusSigner;
use arb_venue::{KalshiSigner, PmusGateway, VenueError};

const SEC: u64 = 1_000_000_000;

// `1 * SEC` reads as a point on the injected timeline, next to `11 * SEC` and
// `100 * SEC`. Reducing it to a bare `SEC` would hide which line moves the
// clock and by how much, which is the only thing this test is about.
#[allow(clippy::identity_op)]
#[test]
fn bucket_drains_then_refills_on_injected_clock() {
    // capacity 3, refill 1/sec, start at t=0
    let mut b = TokenBucket::new(3.0, 1.0, 0);
    assert!(b.try_acquire(0));
    assert!(b.try_acquire(0));
    assert!(b.try_acquire(0));
    assert!(!b.try_acquire(0), "empty after 3 spends at t=0");
    // 1 second later exactly one token is back
    assert!(b.try_acquire(1 * SEC));
    assert!(!b.try_acquire(1 * SEC));
    // 10 seconds later refills but never past capacity
    assert_eq!(b.available(11 * SEC), 3.0);
}

#[test]
fn non_monotonic_now_never_mints_time() {
    let mut b = TokenBucket::new(2.0, 1.0, 100 * SEC);
    assert!(b.try_acquire(100 * SEC));
    assert!(b.try_acquire(100 * SEC));
    // clock goes backwards: no negative refill, still empty
    assert!(!b.try_acquire(50 * SEC));
}

/// The order path draws from NO budget. `xv-shared-api-budget`: "the
/// order/hedge path must bypass it and never wait." A critical bucket, however
/// large, is still a bucket that CAN run out, and the call it would run out on
/// is the one that must never fail.
#[test]
fn the_order_path_is_never_refused_and_spends_nothing() {
    // 30/min background; starts full.
    let mut rl = RateLimiter::from_per_minute(30.0, 0);
    for _ in 0..30 {
        assert!(rl.try_acquire(Priority::Background, 0));
    }
    assert!(!rl.try_acquire(Priority::Background, 0), "the read budget is spent");

    // Nothing left to give, and the order path goes anyway — a thousand times,
    // at the same instant, with the clock never advancing.
    for _ in 0..1000 {
        assert!(rl.try_acquire(Priority::Critical, 0), "a hedge is never refused");
    }
    // ...and none of that came out of the read budget: at 30/min exactly one
    // token is back two seconds later, which is all that ever should be.
    assert!(rl.try_acquire(Priority::Background, 2 * SEC));
    assert!(!rl.try_acquire(Priority::Background, 2 * SEC));
}

// ------------------------------------------------------ NotWired seam ----

fn kalshi_stub() -> KalshiGateway {
    // any valid 32-byte pkcs8 not needed — reuse a throwaway from the signer,
    // but the stub never signs; build with a minimal RSA key from the fixture.
    let pem = include_str!("fixtures/venue/sigs.json");
    let v: serde_json::Value = serde_json::from_str(pem).unwrap();
    let signer = KalshiSigner::from_pkcs8_pem(
        v["kalshi"]["api_key_id"].as_str().unwrap(),
        v["kalshi"]["private_key_pkcs8_pem"].as_str().unwrap(),
    )
    .unwrap();
    KalshiGateway::new(signer, RateLimiter::from_per_minute(30.0, 0))
}

#[test]
fn gateways_are_not_wired() {
    let g = kalshi_stub();
    let place = PlaceRequest {
        market: "KXTIME-26-ZOH".into(),
        side: Side::Bid,
        price: "0.0520".into(),
        qty: 5,
        tif: Tif::Gtc,
        post_only: true,
        client_order_id: "c1".into(),
    };
    assert_eq!(g.place(&place).unwrap_err(), VenueError::NotWired);
    assert_eq!(
        g.cancel(&CancelRequest { by: CancelBy::VenueId("o1".into()), market_slug: None })
            .unwrap_err(),
        VenueError::NotWired
    );
    assert_eq!(g.order_status("o1").unwrap_err(), VenueError::NotWired);
    assert_eq!(g.cancel_all_open().unwrap_err(), VenueError::NotWired);
    assert_eq!(g.rehearse("KXTIME-26-ZOH").unwrap_err(), VenueError::NotWired);
    assert_eq!(g.balances().unwrap_err(), VenueError::NotWired);
    assert_eq!(g.positions().unwrap_err(), VenueError::NotWired);
    // ...and the normalized cash read too: an unwired gateway must refuse, not
    // report an account with no money in it, which is what a cash gate would
    // act on by refusing every order.
    assert_eq!(g.spendable_cash().unwrap_err(), VenueError::NotWired);

    // PM-US stub is likewise inert.
    let pm = PmusGateway::new(
        PmusSigner::from_seed("k", &[7u8; 32]),
        RateLimiter::from_per_minute(30.0, 0),
    );
    assert_eq!(pm.positions().unwrap_err(), VenueError::NotWired);
    assert_eq!(pm.balances().unwrap_err(), VenueError::NotWired);
    assert_eq!(pm.spendable_cash().unwrap_err(), VenueError::NotWired);
}
