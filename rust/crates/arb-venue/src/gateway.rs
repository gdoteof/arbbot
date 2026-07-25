//! The `VenueGateway` trait — the typed executor seam. Signatures only: the
//! stub implementations here return [`VenueError::NotWired`] because this crate
//! has NO transport (no reqwest/hyper). Real executors land later behind
//! arb-trader's dry-run seam (docs/migration-plan.md M2/M3), reusing the
//! signers/wire/resp types in this crate.

use crate::error::VenueError;
use crate::ratelimit::RateLimiter;
use crate::resp;
use crate::sign::{KalshiSigner, PmusSigner};

/// Raw venue side on the YES price axis. `Bid` buys YES; `Ask` sells YES
/// (== opens NO on PM-US via BUY_SHORT).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

/// Time in force. `Gtc` rests as a maker; `Ioc` is a taker hedge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tif {
    Gtc,
    Ioc,
}

/// A place request in venue-neutral terms. `price` is a pre-formatted decimal
/// string (the stack's decimal-parity convention) — no float ever crosses this
/// boundary.
#[derive(Debug, Clone)]
pub struct PlaceRequest {
    pub market: String,
    pub side: Side,
    pub price: String,
    pub qty: i64,
    pub tif: Tif,
    pub post_only: bool,
    pub client_order_id: String,
}

/// A cancel request. PM-US requires the `market_slug` in the body; Kalshi
/// cancels by id alone (`market_slug` ignored there).
#[derive(Debug, Clone)]
pub struct CancelRequest {
    pub order_id: String,
    pub market_slug: Option<String>,
}

/// The venue-adapter interface. Every method is fallible; the only outcome in
/// this build is [`VenueError::NotWired`].
pub trait VenueGateway {
    /// Order-shaped response of a place / single-order status call.
    type Order;
    /// Balance response.
    type Balances;
    /// Positions response.
    type Positions;

    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError>;
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError>;
    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError>;
    /// Cancel every RESTING order owned by this account (kill-switch sweep).
    fn cancel_all_open(&self) -> Result<(), VenueError>;
    /// Place one far-off-touch contract, confirm it rests, cancel it. Live
    /// auth rehearsal — returns the order id on success.
    fn rehearse(&self, market: &str) -> Result<String, VenueError>;
    fn balances(&self) -> Result<Self::Balances, VenueError>;
    fn positions(&self) -> Result<Self::Positions, VenueError>;
}

/// Kalshi gateway stub. Holds the signer + rate limiter it WILL use; every
/// method is [`VenueError::NotWired`] until the transport lands.
pub struct KalshiGateway {
    pub signer: KalshiSigner,
    pub limiter: RateLimiter,
}

impl KalshiGateway {
    pub fn new(signer: KalshiSigner, limiter: RateLimiter) -> Self {
        Self { signer, limiter }
    }
}

impl VenueGateway for KalshiGateway {
    type Order = resp::KalshiOrder;
    type Balances = resp::KalshiBalance;
    type Positions = resp::KalshiPositions;

    fn place(&self, _req: &PlaceRequest) -> Result<Self::Order, VenueError> {
        Err(VenueError::NotWired)
    }
    fn cancel(&self, _req: &CancelRequest) -> Result<(), VenueError> {
        Err(VenueError::NotWired)
    }
    fn order_status(&self, _order_id: &str) -> Result<Self::Order, VenueError> {
        Err(VenueError::NotWired)
    }
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        Err(VenueError::NotWired)
    }
    fn rehearse(&self, _market: &str) -> Result<String, VenueError> {
        Err(VenueError::NotWired)
    }
    fn balances(&self) -> Result<Self::Balances, VenueError> {
        Err(VenueError::NotWired)
    }
    fn positions(&self) -> Result<Self::Positions, VenueError> {
        Err(VenueError::NotWired)
    }
}

/// PM-US gateway stub. Same contract as [`KalshiGateway`].
pub struct PmusGateway {
    pub signer: PmusSigner,
    pub limiter: RateLimiter,
}

impl PmusGateway {
    pub fn new(signer: PmusSigner, limiter: RateLimiter) -> Self {
        Self { signer, limiter }
    }
}

impl VenueGateway for PmusGateway {
    type Order = resp::PmOrder;
    type Balances = resp::PmBalances;
    type Positions = resp::PmPositions;

    fn place(&self, _req: &PlaceRequest) -> Result<Self::Order, VenueError> {
        Err(VenueError::NotWired)
    }
    fn cancel(&self, _req: &CancelRequest) -> Result<(), VenueError> {
        Err(VenueError::NotWired)
    }
    fn order_status(&self, _order_id: &str) -> Result<Self::Order, VenueError> {
        Err(VenueError::NotWired)
    }
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        Err(VenueError::NotWired)
    }
    fn rehearse(&self, _market: &str) -> Result<String, VenueError> {
        Err(VenueError::NotWired)
    }
    fn balances(&self) -> Result<Self::Balances, VenueError> {
        Err(VenueError::NotWired)
    }
    fn positions(&self) -> Result<Self::Positions, VenueError> {
        Err(VenueError::NotWired)
    }
}
