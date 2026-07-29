//! The Kalshi half of the gateway — paths, the paginated order list, and the
//! cancel path that can resolve OUR client_order_id against the venue's own
//! book. Shared types and helpers live in the parent module.

use super::{spend_token, ts_ms, CancelBy, CancelRequest, PlaceRequest, Settle, Side, Tif,
            VenueGateway};
use crate::error::VenueError;
use crate::ratelimit::{Priority, RateLimiter};
use crate::resp;
use crate::sign::KalshiSigner;
use crate::transport::{NotWired, Transport};
use crate::wire;
use serde_json::Value;
use std::sync::Mutex;

/// Kalshi V2 API paths. The signature covers the FULL path including the
/// `/trade-api/v2` prefix (Python `_headers` prepends it), so these constants
/// are what gets signed AND what builds the URL — one string, no drift.
/// Create moved here; the legacy `POST /portfolio/orders` now 410s.
const K_PLACE: &str = "/trade-api/v2/portfolio/events/orders";
/// GET/list + status still live under `/portfolio/orders`.
const K_ORDERS: &str = "/trade-api/v2/portfolio/orders";
const K_BALANCE: &str = "/trade-api/v2/portfolio/balance";
const K_POSITIONS: &str = "/trade-api/v2/portfolio/positions";

/// Kalshi gateway. Generic over its [`Transport`]; the default is
/// [`NotWired`], so a gateway built with [`KalshiGateway::new`] still cannot
/// reach a venue — the inert seam is preserved, and reaching a venue is an
/// explicit act ([`KalshiGateway::with_transport`]).
pub struct KalshiGateway<T: Transport = NotWired> {
    pub signer: KalshiSigner,
    pub limiter: Mutex<RateLimiter>,
    pub transport: T,
    settle: Settle,
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
            settle: Settle::default(),
        }
    }

    /// Override the create-visibility poll (tests use a zero delay).
    pub fn with_settle(mut self, delay: std::time::Duration, attempts: u32) -> Self {
        self.settle = Settle { delay, attempts };
        self
    }

    /// `order_status`, tolerating the window where a just-created order is not
    /// yet visible to the query service. See [`Settle::retry_404`].
    fn order_status_settled(&self, order_id: &str) -> Result<resp::KalshiOrder, VenueError> {
        self.settle.retry_404("kalshi order_status", order_id, || self.order_status(order_id))
    }

    /// Sign `path` (never the query — quirk K2) and send. Spends one token of
    /// `priority` first; an exhausted local budget refuses a background READ
    /// rather than earning a venue-side 429. The order path spends nothing —
    /// see [`super::spend_token`].
    fn call(
        &self,
        priority: Priority,
        method: &str,
        path: &str,
        query: Option<&str>,
        body: Option<&Value>,
    ) -> Result<crate::transport::Response, VenueError> {
        spend_token(&self.limiter, priority)?;
        let ts = ts_ms();
        let headers = self.signer.headers(&ts, method, path);
        self.transport.send(method, path, query, &headers, body)
    }

    /// ALL orders across pages. `/portfolio/orders` is paginated (100/page +
    /// cursor) and `?status=resting` returns NOTHING, so page the full list and
    /// filter status client-side. Skipping pagination orphans older resting
    /// orders — that is naked-leg risk, not a cosmetic bug.
    ///
    /// [`Priority::Critical`], not Background, because this is not a poll: it
    /// is a limb of the cancel path. Every caller is one — [`Self::find_ours`]
    /// resolves a client-id cancel through it, [`Self::cancel_all_open`] is the
    /// kill sweep, and `resting_order_ids` is the only evidence
    /// `cancel_all_and_verify` accepts. A read the order path cannot complete
    /// without is the order path, so refusing it locally refuses the cancel:
    /// a halt that a token bucket can turn into "NOT CLEAN at exit" is the same
    /// hazard as the hedge this priority exists to protect.
    pub fn all_orders(&self) -> Result<Vec<resp::KalshiOrder>, VenueError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let q = cursor.as_ref().map(|c| format!("limit=100&cursor={c}"));
            let r = self.call(
                Priority::Critical,
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

    /// Where an order carrying OUR `client_order_id` stands on this account.
    /// The three cases are deliberately distinct: "resting", "on the account
    /// but finished" and "not here at all" call for three different answers
    /// from a cancel, and collapsing them is how a cancel reports success for
    /// an order it never touched.
    fn find_ours(&self, coid: &str) -> Result<Ours, VenueError> {
        let mut seen = false;
        for o in self.all_orders()? {
            if o.client_order_id.as_deref() != Some(coid) {
                continue;
            }
            if o.is_resting() {
                return Ok(Ours::Resting(o.order_id));
            }
            seen = true;
        }
        Ok(if seen { Ours::Gone } else { Ours::Absent })
    }

    /// Find a RESTING order by our own `client_order_id` and cancel it.
    /// `Ok(None)` means there was nothing to clean up. This is the recovery
    /// path for a create whose response we could not read: the order may exist
    /// under an id we never learned, and the client_order_id is the only handle
    /// we have on it.
    pub fn cancel_by_client_order_id(&self, coid: &str) -> Result<Option<String>, VenueError> {
        match self.find_ours(coid)? {
            Ours::Resting(id) => {
                self.cancel(&CancelRequest {
                    by: CancelBy::VenueId(id.clone()),
                    market_slug: None,
                })?;
                Ok(Some(id))
            }
            Ours::Gone | Ours::Absent => Ok(None),
        }
    }
}

/// The state of an order we tagged with a `client_order_id`, as the venue's own
/// order list reports it.
enum Ours {
    /// Still resting, under the venue's own id.
    Resting(String),
    /// On the account but finished (canceled or executed) — a cancel has
    /// nothing left to do.
    Gone,
    /// Not on the account at all.
    Absent,
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

    fn order_id(order: &Self::Order) -> String {
        order.order_id.clone()
    }

    /// DELETE /portfolio/events/orders/{venue order id}.
    ///
    /// A [`CancelBy::ClientId`] target is resolved against the venue's own order
    /// list first — that is the recovery handle for an order whose id we never
    /// learned. "Not on the account" is an ERROR there, never success: the order
    /// list also LAGS a write (see [`Self::order_status_settled`]), so absence
    /// can mean "not visible yet" just as easily as "never accepted", and
    /// claiming a cancel we did not make is the failure mode this whole path
    /// exists to end.
    fn cancel(&self, req: &CancelRequest) -> Result<(), VenueError> {
        let oid = match &req.by {
            CancelBy::VenueId(id) => id.clone(),
            CancelBy::ClientId(coid) => match self.find_ours(coid)? {
                Ours::Resting(id) => id,
                // Genuinely already gone. This is the ONE case where "we
                // cancelled nothing" is the right answer.
                Ours::Gone => return Ok(()),
                // Not on the account. Still an error rather than a success —
                // nothing was cancelled and the order list LAGS a write, so this
                // cannot be read as proof the order is gone. Stated plainly, not
                // shouted: the commonest cause is a place the venue rejected, in
                // which case there was never anything here to cancel.
                Ours::Absent => {
                    return Err(VenueError::Status {
                        endpoint: "kalshi cancel",
                        status: 0,
                        body: format!(
                            "no order carries client_order_id `{coid}`; nothing cancelled \
                             (the place may have been rejected, or the order list has not \
                             caught up)"
                        ),
                    })
                }
            },
        };
        if oid.is_empty() {
            // An empty id would DELETE the collection path, which is not a
            // cancel of anything and may not be a no-op either.
            return Err(VenueError::MissingField {
                endpoint: "kalshi cancel",
                field: "order_id".into(),
            });
        }
        let path = format!("{K_PLACE}/{oid}");
        let r = self.call(Priority::Critical, "DELETE", &path, None, None)?;
        // 404 == already gone == successfully cancelled (quirk K4). Treating
        // it as an error would orphan a resting order on a retry path.
        //
        // That reading is only sound because `oid` is the VENUE's own id: a 404
        // on an id the venue never issued means "wrong id", not "already gone",
        // and mapping THAT to success is what hid every phantom cancel until
        // 2026-07-28. `CancelBy` makes an unresolved id impossible to send here.
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

    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
        Ok(self.all_orders()?.into_iter().filter(|o| o.is_resting()).map(|o| o.order_id).collect())
    }

    /// Kill-switch sweep. `/portfolio/orders` returns history (canceled and
    /// executed too), so filter to `resting` — never try to cancel a dead order.
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        for o in self.all_orders()? {
            if o.is_resting() {
                self.cancel(&CancelRequest {
                    by: CancelBy::VenueId(o.order_id),
                    market_slug: None,
                })?;
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

        let cancelled =
            self.cancel(&CancelRequest { by: CancelBy::VenueId(oid.clone()), market_slug: None });

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
