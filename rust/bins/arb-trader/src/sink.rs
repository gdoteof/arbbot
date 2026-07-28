//! The order sink — where an effect stops being a record and becomes a wire.
//!
//! `None` (dry-run) is the default everywhere: the executor counts the command
//! and drops it. A `Some` sink is only ever constructed by `--enable-orders`,
//! and only after the preconditions in `main::order_preconditions` are met.
//!
//! The gateways are blocking (one-shot-tool shaped, see arb-venue), so calls go
//! through `spawn_blocking` — a blocking venue call on a tokio worker would
//! stall unrelated tasks, which is precisely the reader-never-stalls property
//! the shell exists to preserve.

use arb_venue::gateway::{CancelRequest, KalshiGateway, PlaceRequest, PmusGateway, VenueGateway};
use arb_venue::transport::Transport;
use arb_venue::VenueError;

/// What an executor may do to a venue. Deliberately only two verbs: the engine
/// amends by cancel+place, and nothing here can read the account.
pub trait OrderSink: Send + Sync {
    /// Returns the venue's order id.
    fn place(&self, req: &PlaceRequest) -> Result<String, VenueError>;
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError>;
}

// Generic over the transport, not pinned to HTTP: a gateway carrying the
// NotWired transport is still a valid (inert) sink, and tests drive a mock.
impl<T: Transport + Send + Sync> OrderSink for KalshiGateway<T> {
    fn place(&self, req: &PlaceRequest) -> Result<String, VenueError> {
        VenueGateway::place(self, req).map(|o| o.order_id)
    }
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        VenueGateway::cancel(self, req)
    }
}

impl<T: Transport + Send + Sync> OrderSink for PmusGateway<T> {
    fn place(&self, req: &PlaceRequest) -> Result<String, VenueError> {
        VenueGateway::place(self, req).map(|o| o.id)
    }
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        VenueGateway::cancel(self, req)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_venue::gateway::{Side, Tif};
    use arb_venue::ratelimit::RateLimiter;
    use arb_venue::transport::{Response, Transport};
    use arb_venue::KalshiSigner;
    use serde_json::Value;
    use std::sync::Mutex;

    struct Mock {
        replies: Mutex<Vec<(u16, String)>>,
        sent: Mutex<Vec<(String, String, Option<Value>)>>,
    }
    impl Transport for Mock {
        fn send(
            &self,
            method: &str,
            path: &str,
            _q: Option<&str>,
            _h: &[(&'static str, String)],
            body: Option<&Value>,
        ) -> Result<Response, VenueError> {
            self.sent.lock().unwrap().push((
                method.to_string(),
                path.to_string(),
                body.cloned(),
            ));
            let (status, body) = self.replies.lock().unwrap().remove(0);
            Ok(Response { status, body })
        }
    }

    fn signer() -> KalshiSigner {
        let v: Value = serde_json::from_str(include_str!(
            "../../../crates/arb-venue/tests/fixtures/venue/sigs.json"
        ))
        .unwrap();
        KalshiSigner::from_pkcs8_pem(
            v["kalshi"]["api_key_id"].as_str().unwrap(),
            v["kalshi"]["private_key_pkcs8_pem"].as_str().unwrap(),
        )
        .unwrap()
    }

    /// The engine's PlaceRequest reaches the venue intact — this is the whole
    /// point of making ExecCmd order-shaped.
    #[test]
    fn a_place_command_reaches_the_venue_with_the_engines_order() {
        let mock = Mock {
            replies: Mutex::new(vec![(
                201,
                r#"{"order_id":"srv-1","client_order_id":"m42"}"#.to_string(),
            )]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 600.0, 0),
            mock,
        );
        let oid = OrderSink::place(
            &gw,
            &PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Ask,
                price: "0.4200".into(),
                qty: 5,
                tif: Tif::Gtc,
                post_only: true,
                client_order_id: "m42".into(),
            },
        )
        .unwrap();
        assert_eq!(oid, "srv-1", "the venue's id, not ours");

        let sent = gw.transport.sent.lock().unwrap();
        let (method, path, body) = &sent[0];
        assert_eq!(method, "POST");
        assert_eq!(path, "/trade-api/v2/portfolio/events/orders");
        let b = body.as_ref().unwrap();
        assert_eq!(b["ticker"], "KXTEST");
        assert_eq!(b["side"], "ask");
        assert_eq!(b["price"], "0.4200");
        assert_eq!(b["count"], "5.00");
        assert_eq!(b["post_only"], true);
        assert_eq!(
            b["client_order_id"], "m42",
            "our order id rides along — that is what makes a retry idempotent"
        );
    }

    #[test]
    fn a_cancel_command_reaches_the_venue() {
        let mock = Mock {
            replies: Mutex::new(vec![(200, "{}".to_string())]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 600.0, 0),
            mock,
        );
        OrderSink::cancel(
            &gw,
            &CancelRequest { order_id: "srv-1".into(), market_slug: Some("KXTEST".into()) },
        )
        .unwrap();
        let sent = gw.transport.sent.lock().unwrap();
        assert_eq!(sent[0].0, "DELETE");
        assert_eq!(sent[0].1, "/trade-api/v2/portfolio/events/orders/srv-1");
    }

    /// A venue rejection surfaces as an error rather than being counted as a
    /// successful place.
    #[test]
    fn a_rejected_place_is_an_error() {
        let mock = Mock {
            replies: Mutex::new(vec![(400, r#"{"error":"post_only_would_cross"}"#.to_string())]),
            sent: Mutex::new(Vec::new()),
        };
        let gw = KalshiGateway::with_transport(
            signer(),
            RateLimiter::from_per_minute(600.0, 600.0, 0),
            mock,
        );
        assert!(OrderSink::place(
            &gw,
            &PlaceRequest {
                market: "KXTEST".into(),
                side: Side::Bid,
                price: "0.4200".into(),
                qty: 5,
                tif: Tif::Gtc,
                post_only: true,
                client_order_id: "m1".into(),
            },
        )
        .is_err());
    }
}
