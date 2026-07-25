//! RateLimiter is a pure type with injected `now_ns` — deterministic, no clock.

use arb_venue::gateway::{CancelRequest, KalshiGateway, PlaceRequest, Side, Tif, VenueGateway};
use arb_venue::ratelimit::{Priority, RateLimiter, TokenBucket};
use arb_venue::sign::PmusSigner;
use arb_venue::{KalshiSigner, PmusGateway, VenueError};

const SEC: u64 = 1_000_000_000;

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

#[test]
fn critical_and_background_are_independent() {
    // 60/min critical, 30/min background; start full.
    let mut rl = RateLimiter::from_per_minute(60.0, 30.0, 0);
    // drain background to empty (30 tokens) without touching critical
    for _ in 0..30 {
        assert!(rl.try_acquire(Priority::Background, 0));
    }
    assert!(!rl.try_acquire(Priority::Background, 0), "background empty");
    // critical bucket is untouched — the hedge path is not starved
    assert!(rl.try_acquire(Priority::Critical, 0));
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
    KalshiGateway::new(signer, RateLimiter::from_per_minute(60.0, 30.0, 0))
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
        g.cancel(&CancelRequest { order_id: "o1".into(), market_slug: None }).unwrap_err(),
        VenueError::NotWired
    );
    assert_eq!(g.order_status("o1").unwrap_err(), VenueError::NotWired);
    assert_eq!(g.cancel_all_open().unwrap_err(), VenueError::NotWired);
    assert_eq!(g.rehearse("KXTIME-26-ZOH").unwrap_err(), VenueError::NotWired);
    assert_eq!(g.balances().unwrap_err(), VenueError::NotWired);
    assert_eq!(g.positions().unwrap_err(), VenueError::NotWired);

    // PM-US stub is likewise inert.
    let pm = PmusGateway::new(
        PmusSigner::from_seed("k", &[7u8; 32]),
        RateLimiter::from_per_minute(30.0, 30.0, 0),
    );
    assert_eq!(pm.positions().unwrap_err(), VenueError::NotWired);
    assert_eq!(pm.balances().unwrap_err(), VenueError::NotWired);
}
