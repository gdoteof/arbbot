//! The `VenueGateway` trait — the typed executor seam. Signatures only: the
//! stub implementations here return [`VenueError::NotWired`] because this crate
//! has NO transport (no reqwest/hyper). Real executors land later behind
//! arb-trader's dry-run seam (docs/migration-plan.md M2/M3), reusing the
//! signers/wire/resp types in this crate.

use crate::error::VenueError;
use crate::ratelimit::{Priority, RateLimiter};
use crate::resp;
use crate::sign::{KalshiSigner, PmusSigner};
use crate::transport::{NotWired, Transport};
use crate::wire;
use serde_json::Value;
use std::sync::Mutex;

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

/// Kalshi V2 API paths. The signature covers the FULL path including the
/// `/trade-api/v2` prefix (Python `_headers` prepends it), so these constants
/// are what gets signed AND what builds the URL — one string, no drift.
/// Create moved here; the legacy `POST /portfolio/orders` now 410s.
const K_PLACE: &str = "/trade-api/v2/portfolio/events/orders";
/// GET/list + status still live under `/portfolio/orders`.
const K_ORDERS: &str = "/trade-api/v2/portfolio/orders";
const K_BALANCE: &str = "/trade-api/v2/portfolio/balance";
const K_POSITIONS: &str = "/trade-api/v2/portfolio/positions";

fn now_ns() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn ts_ms() -> String {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis().to_string())
        .unwrap_or_else(|_| "0".into())
}

/// Kalshi gateway. Generic over its [`Transport`]; the default is
/// [`NotWired`], so a gateway built with [`KalshiGateway::new`] still cannot
/// reach a venue — the inert seam is preserved, and reaching a venue is an
/// explicit act ([`KalshiGateway::with_transport`]).
pub struct KalshiGateway<T: Transport = NotWired> {
    pub signer: KalshiSigner,
    pub limiter: Mutex<RateLimiter>,
    pub transport: T,
    /// Kalshi's create and query services are NOT read-your-writes: a GET on a
    /// just-placed order 404s for a beat (observed live 2026-07-27), and the
    /// order LIST lags further still. Python papered over this with a flat
    /// `time.sleep(1.0)`; poll instead, so a fast venue costs nothing and a
    /// slow one still succeeds. Zero delay in tests.
    settle_delay: std::time::Duration,
    settle_attempts: u32,
}

impl KalshiGateway<NotWired> {
    /// The inert gateway: holds the signer + rate limiter it WILL use, with
    /// no transport behind it.
    pub fn new(signer: KalshiSigner, limiter: RateLimiter) -> Self {
        Self::with_transport(signer, limiter, NotWired)
    }
}

impl<T: Transport> KalshiGateway<T> {
    pub fn with_transport(signer: KalshiSigner, limiter: RateLimiter, transport: T) -> Self {
        Self {
            signer,
            limiter: Mutex::new(limiter),
            transport,
            settle_delay: std::time::Duration::from_millis(500),
            settle_attempts: 8,
        }
    }

    /// Override the create-visibility poll (tests use a zero delay).
    pub fn with_settle(mut self, delay: std::time::Duration, attempts: u32) -> Self {
        self.settle_delay = delay;
        self.settle_attempts = attempts;
        self
    }

    /// `order_status`, tolerating the window where a just-created order is not
    /// yet visible to the query service. A 404 here means "not yet", NOT "no
    /// such order" — the create already told us it exists.
    fn order_status_settled(&self, order_id: &str) -> Result<resp::KalshiOrder, VenueError> {
        let mut last = None;
        for attempt in 0..self.settle_attempts.max(1) {
            match self.order_status(order_id) {
                Err(VenueError::Status { status: 404, endpoint, body }) => {
                    last = Some(VenueError::Status { status: 404, endpoint, body });
                    if attempt + 1 < self.settle_attempts.max(1) && !self.settle_delay.is_zero() {
                        std::thread::sleep(self.settle_delay);
                    }
                }
                other => return other,
            }
        }
        Err(last.unwrap_or(VenueError::Status {
            endpoint: "kalshi order_status",
            status: 404,
            body: format!("order {order_id} never became visible"),
        }))
    }

    /// Sign `path` (never the query — quirk K2) and send. Spends one token of
    /// `priority` first; an exhausted local budget refuses rather than earning
    /// a venue-side 429.
    fn call(
        &self,
        priority: Priority,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<&Value>,
    ) -> Result<crate::transport::Response, VenueError> {
        {
            let mut lim = self.limiter.lock().expect("rate limiter mutex");
            if !lim.try_acquire(priority, now_ns()) {
                return Err(VenueError::RateLimited {
                    priority: match priority {
                        Priority::Critical => "critical",
                        Priority::Background => "background",
                    },
                });
            }
        }
        let ts = ts_ms();
        let headers = self.signer.headers(&ts, method, path);
        self.transport.send(method, path, query, &headers, body)
    }

    /// ALL orders across pages. `/portfolio/orders` is paginated (100/page +
    /// cursor) and `?status=resting` returns NOTHING, so page the full list and
    /// filter status client-side. Skipping pagination orphans older resting
    /// orders — that is naked-leg risk, not a cosmetic bug.
    pub fn all_orders(&self) -> Result<Vec<resp::KalshiOrder>, VenueError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let q = cursor.as_ref().map(|c| format!("limit=100&cursor={c}"));
            let r = self.call(
                Priority::Background,
                "GET",
                K_ORDERS,
                q.as_deref().or(Some("limit=100")),
                None,
            )?;
            if r.status != 200 {
                return Err(VenueError::Status {
                    endpoint: "kalshi orders",
                    status: r.status,
                    body: r.body,
                });
            }
            let page = resp::kalshi_orders_page(&r.body)?;
            out.extend(page.orders);
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        Ok(out)
    }

    /// Find a RESTING order by our own `client_order_id` and cancel it.
    /// `Ok(None)` means there was nothing to clean up. This is the recovery
    /// path for a create whose response we could not read: the order may exist
    /// under an id we never learned, and the client_order_id is the only handle
    /// we have on it.
    pub fn cancel_by_client_order_id(&self, coid: &str) -> Result<Option<String>, VenueError> {
        let found = self.all_orders()?.into_iter().find(|o| {
            o.is_resting() && o.client_order_id.as_deref() == Some(coid)
        });
        match found {
            Some(o) => {
                self.cancel(&CancelRequest { order_id: o.order_id.clone(), market_slug: None })?;
                Ok(Some(o.order_id))
            }
            None => Ok(None),
        }
    }
}

impl<T: Transport> VenueGateway for KalshiGateway<T> {
    type Order = resp::KalshiOrder;
    type Balances = resp::KalshiBalance;
    type Positions = resp::KalshiPositions;

    fn place(&self, req: &PlaceRequest) -> Result<Self::Order, VenueError> {
        let body = wire::kalshi_place_body(
            &req.market,
            req.side,
            &req.price,
            req.qty,
            &req.client_order_id,
            req.post_only,
        );
        let r = self.call(Priority::Critical, "POST", K_PLACE, None, Some(&body))?;
        // A post_only order that would cross is a 400, NOT a silent taker
        // fill (quirk K5). Surfacing it is the whole point: crossing when we
        // asked to rest means our book view was wrong.
        if r.status >= 300 {
            return Err(VenueError::Status {
                endpoint: "kalshi place",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_created_order(&r.body)
    }

    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        let path = format!("{K_PLACE}/{}", req.order_id);
        let r = self.call(Priority::Critical, "DELETE", &path, None, None)?;
        // 404 == already gone == successfully cancelled (quirk K4). Treating
        // it as an error would orphan a resting order on a retry path.
        if r.status == 404 || r.status < 300 {
            return Ok(());
        }
        Err(VenueError::Status { endpoint: "kalshi cancel", status: r.status, body: r.body })
    }

    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError> {
        let path = format!("{K_ORDERS}/{order_id}");
        let r = self.call(Priority::Background, "GET", &path, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi order_status",
                status: r.status,
                body: r.body,
            });
        }
        Ok(resp::kalshi_order_envelope(&r.body)?.order)
    }

    /// Kill-switch sweep. `/portfolio/orders` returns history (canceled and
    /// executed too), so filter to `resting` — never try to cancel a dead order.
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        for o in self.all_orders()? {
            if o.is_resting() {
                self.cancel(&CancelRequest { order_id: o.order_id, market_slug: None })?;
            }
        }
        Ok(())
    }

    /// Live auth rehearsal (docs/migration-plan.md M2): place ONE contract at
    /// 1c — far below any real bid, so the fill risk is ~zero and the worst
    /// case is a $0.01 position to flatten by hand — confirm it rests, cancel
    /// it. Proves the signed POST/GET/DELETE path against the real account.
    fn rehearse(&self, market: &str) -> Result<String, VenueError> {
        let coid = format!("rehearse-{}", ts_ms());
        let placed = self.place(&PlaceRequest {
            market: market.to_string(),
            side: Side::Bid,
            price: "0.0100".into(),
            qty: 1,
            tif: Tif::Gtc,
            post_only: true,
            client_order_id: coid.clone(),
        });

        let oid = match placed {
            Ok(o) => o.order_id,
            // A place that FAILED TO PARSE may still have reached the venue —
            // the POST was accepted, we just could not read the answer. That is
            // how the first live smoke left an order resting (2026-07-27). The
            // client_order_id is our only handle on it, so sweep for it.
            Err(e @ VenueError::Parse { .. }) | Err(e @ VenueError::MissingField { .. }) => {
                match self.cancel_by_client_order_id(&coid) {
                    Ok(Some(id)) => {
                        return Err(VenueError::Status {
                            endpoint: "kalshi rehearse",
                            status: 0,
                            body: format!(
                                "unreadable create response ({e}); \
                                 recovered and CANCELLED orphan order {id}"
                            ),
                        })
                    }
                    Ok(None) => return Err(e),
                    Err(sweep) => {
                        return Err(VenueError::Status {
                            endpoint: "kalshi rehearse",
                            status: 0,
                            body: format!(
                                "unreadable create response ({e}); \
                                 orphan sweep ALSO failed ({sweep}) — \
                                 CHECK THE VENUE BY HAND"
                            ),
                        })
                    }
                }
            }
            Err(e) => return Err(e),
        };

        // From here the order EXISTS. Every exit path below must cancel it, or
        // the rehearsal leaves behind exactly what it exists to prove we never
        // leave behind.
        let rested = match self.order_status_settled(&oid) {
            Ok(seen) if seen.is_resting() => Ok(()),
            Ok(seen) => Err(VenueError::Status {
                endpoint: "kalshi rehearse",
                status: 0,
                body: format!(
                    "order {oid} did not rest (status={:?})",
                    seen.status.as_deref().unwrap_or("<absent>")
                ),
            }),
            Err(e) => Err(e),
        };

        let cancelled = self.cancel(&CancelRequest { order_id: oid.clone(), market_slug: None });

        match (rested, cancelled) {
            (Ok(()), Ok(())) => Ok(oid),
            (Err(e), Ok(())) => Err(e), // check failed, but nothing left resting
            (Ok(()), Err(c)) | (Err(_), Err(c)) => Err(VenueError::Status {
                endpoint: "kalshi rehearse",
                status: 0,
                body: format!("order {oid} COULD NOT BE CANCELLED ({c}) — CHECK THE VENUE"),
            }),
        }
    }

    fn balances(&self) -> Result<Self::Balances, VenueError> {
        let r = self.call(Priority::Background, "GET", K_BALANCE, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi balance",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_balance(&r.body)
    }

    fn positions(&self) -> Result<Self::Positions, VenueError> {
        let r = self.call(Priority::Background, "GET", K_POSITIONS, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi positions",
                status: r.status,
                body: r.body,
            });
        }
        resp::kalshi_positions(&r.body)
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
