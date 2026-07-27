//! arb-venue — the venue-adapter layer.
//!
//! Where the venue scar tissue consolidates (docs/migration-plan.md "Venue
//! adapters"): request signing, request-shaping, and response-parsing, each at
//! byte-parity with the Python gateways. There is deliberately NO network
//! transport here (no reqwest/hyper) — the real executors land later behind
//! arb-trader's dry-run seam, reusing these pieces.

pub mod error;
pub mod gateway;
pub mod ratelimit;
pub mod resp;
pub mod sign;
pub mod transport;
pub mod wire;

pub use error::VenueError;
pub use gateway::{
    CancelRequest, KalshiGateway, PlaceRequest, PmusGateway, Side, Tif, VenueGateway,
};
pub use ratelimit::{Priority, RateLimiter, TokenBucket};
pub use sign::{KalshiSigner, KalshiVerifier, PmusSigner};
pub use transport::{HttpTransport, NotWired, Response, Transport};
