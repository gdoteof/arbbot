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
use std::collections::{BTreeMap, HashSet};
use std::sync::Mutex;

/// Kalshi V2 API paths. The signature covers the FULL path including the
/// `/trade-api/v2` prefix (Python `_headers` prepends it), so these constants
/// are what gets signed AND what builds the URL — one string, no drift.
/// Create moved here; the legacy `POST /portfolio/orders` now 410s.
const K_PLACE: &str = "/trade-api/v2/portfolio/events/orders";
/// GET/list + status still live under `/portfolio/orders`.
const K_ORDERS: &str = "/trade-api/v2/portfolio/orders";
/// Fill history — the venue's own record of every fill, with the `trade_id`
/// the WS `fill` frame dedupes on. See [`KalshiGateway::fills_since`].
const K_FILLS: &str = "/trade-api/v2/portfolio/fills";
const K_BALANCE: &str = "/trade-api/v2/portfolio/balance";
/// Pages of fill history one `fills_since` will walk before it gives up.
///
/// 100 rows a page, so 5 pages is 500 fills — comfortably more than the busiest
/// quoting day this stack has recorded, against a window that is normally
/// seconds. A venue that keeps handing back cursors must not be able to spin a
/// reconciliation forever on a background budget. Exceeding it is an ERROR, not
/// a truncated list: see [`KalshiGateway::fills_since`].
const K_FILLS_MAX_PAGES: usize = 5;
/// Pages of ORDER history one cursor walk will read before it gives up.
///
/// The Python this ports capped at 50 (`kalshi_gateway.py:85-104`, quirk
/// `kalshi-orders-list-history-paginated`) and the port dropped the cap. It is
/// worth restoring now that [`KalshiGateway::recover_place`] puts this walk on a
/// new trigger: the LONGEST walk is the one that finds nothing, which is exactly
/// the recovery's `Ours::Absent` path, it runs inside the executor's serial
/// blocking hop, and each page can cost the transport's own 15s timeout.
///
/// An ERROR, not a truncated list, for the same reason as
/// [`K_FILLS_MAX_PAGES`]: a caller cannot tell a prefix from the whole account,
/// and `resting_order_ids` answering `Ok(vec![])` over a prefix is "PROVEN
/// clean" over a book nobody read. 50 pages is 5,000 rows against a history in
/// the hundreds, so reaching it means the cursor is not terminating.
const K_ORDERS_MAX_PAGES: usize = 50;
/// The PUBLIC market listing. Unsigned in the Python (`kalshi_ask` sends a bare
/// GET), signed here because [`KalshiGateway::call`] signs everything and a
/// signed request to a public endpoint is accepted — one code path rather than a
/// second, unsigned one that would drift.
const K_MARKETS: &str = "/trade-api/v2/markets";
const K_POSITIONS: &str = "/trade-api/v2/portfolio/positions";
/// Pages of the POSITION list one [`KalshiGateway::net_positions`] will walk.
///
/// An ERROR, not a truncated map, for the same reason as [`K_FILLS_MAX_PAGES`]
/// — and the consequence here is the sharper one. A position list read short
/// reports the tickers it left out as ZERO, and zero on our side of a basket
/// whose other leg IS read is exactly the shape of a naked leg. 20 pages is
/// well past any position count this account has held.
const K_POSITIONS_MAX_PAGES: usize = 20;

/// Kalshi gateway. Generic over its [`Transport`]; the default is
/// [`NotWired`], so a gateway built with [`KalshiGateway::new`] still cannot
/// reach a venue — the inert seam is preserved, and reaching a venue is an
/// explicit act ([`KalshiGateway::with_transport`]).
pub struct KalshiGateway<T: Transport = NotWired> {
    pub signer: KalshiSigner,
    pub limiter: Mutex<RateLimiter>,
    pub transport: T,
    settle: Settle,
    unscoped_sweep: bool,
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
            unscoped_sweep: false,
        }
    }

    /// Override the create-visibility poll (tests use a zero delay).
    pub fn with_settle(mut self, delay: std::time::Duration, attempts: u32) -> Self {
        self.settle = Settle { delay, attempts };
        self
    }

    /// THE OPERATOR ESCAPE HATCH (`--sweep-unscoped`): go back to cancelling
    /// every resting order on the account, whoever placed it.
    ///
    /// It exists because this change added a way for the engine to REFUSE TO
    /// START. If the venue stops echoing `client_order_id` on its order list,
    /// a scoped sweep can neither attribute nor prove, `startup_sweep` fails and
    /// `main` exits 10 — and the documented manual remedy,
    /// `scripts/kalshi_cancel_all.py`, is Python and forbidden by the Rust-only
    /// scope. A safety gate with no override is an outage waiting for a venue
    /// change nobody controls.
    ///
    /// It is OFF by default and must stay that way: on it re-arms the exact
    /// shared-account failure `docs/venue-quirks.md`
    /// §`xv-graceful-shutdown-cancels-orders` names.
    pub fn with_unscoped_sweep(mut self, unscoped: bool) -> Self {
        self.unscoped_sweep = unscoped;
        self
    }

    /// `order_status`, tolerating the window where a just-created order is not
    /// yet visible to the query service. See [`Settle::retry_404`].
    fn order_status_settled(&self, order_id: &str) -> Result<resp::KalshiOrder, VenueError> {
        self.settle.retry_404("kalshi order_status", order_id, || self.order_status(order_id))
    }

    /// The single-order GET at a chosen priority. `order_status` is a
    /// BACKGROUND poll; the hedge-verification read is on the order path and
    /// must not be refusable by a token bucket (see
    /// [`VenueGateway::order_filled_qty`]). One body, so the two cannot drift
    /// about the path, the status check or the envelope.
    fn order_status_at(
        &self,
        priority: Priority,
        order_id: &str,
    ) -> Result<resp::KalshiOrder, VenueError> {
        let path = format!("{K_ORDERS}/{order_id}");
        let r = self.call(priority, "GET", &path, None, None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi order_status",
                status: r.status,
                body: r.body,
            });
        }
        Ok(resp::kalshi_order_envelope(&r.body)?.order)
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
        Ok(self.listing()?.orders)
    }

    /// [`Self::all_orders`], keeping whether the walk actually FINISHED.
    ///
    /// The trailing page of a cursor walk is the one place the required-`orders`
    /// rule could bite a legitimate response. `orders` is required so that a
    /// FIRST page without it cannot read as "the book is empty" — that is the
    /// whole of defect A. A CONTINUATION page is a different question: page 1
    /// already proved the list is readable and already carries real rows, so a
    /// final `{"cursor":""}` with `orders` omitted cannot make us conclude
    /// anything about emptiness. Hard-erroring the entire sweep on it would be
    /// fail-closed on a shape nobody has captured.
    ///
    /// So the walk ENDS there instead — and says it was cut short, because the
    /// one thing it must not do is let a truncated walk read as a complete one.
    /// A page we never saw could hold a resting order of ours, and
    /// `resting_order_ids` returning early would be "PROVEN clean" over exactly
    /// that. Incomplete means: cancel what we did find, prove nothing.
    fn listing(&self) -> Result<Listing, VenueError> {
        self.listing_for(None)
    }

    /// [`Self::listing`], allowed to stop once ONE named order has been found
    /// resting.
    ///
    /// The sweep needs the account; [`Self::find_ours`] needs one row, and
    /// `/portfolio/orders` is the full HISTORY — hundreds of finished rows on
    /// this account, 100 to a page, every page a signed request on the order
    /// path's own priority. Paging the remainder after the answer is already in
    /// hand buys nothing and spends exactly the requests a 429 mid-cancel would
    /// cost us (quirk `xv-shared-api-budget`).
    ///
    /// STOPPING EARLY IS ONLY SOUND BECAUSE THE PREFIX ALREADY ANSWERS. A
    /// resting row carrying our tag tells the whole truth about that order, so
    /// no later page can change it — the same reasoning `find_ours` already
    /// applies to a walk that was cut short. Every other question — "not on the
    /// account", "clean" — still needs the complete walk and still gets one:
    /// nothing stops unless the row was FOUND, and `stop_at` is `None` for
    /// every caller but one.
    ///
    /// It is not a claim about sort order. A venue that lists oldest-first puts
    /// a just-placed order on the LAST page and this saves nothing; the worst
    /// case is exactly the walk we did before.
    ///
    /// TWO ANSWERS STILL COST THE FULL WALK, and both are on the recovery's
    /// hot path, so do not read this as a bound:
    ///   * `Ours::Gone` — an order that already EXECUTED is not `is_resting()`,
    ///     so it never stops the walk. A later page could still hold a RESTING
    ///     row for the same tag, and that answer has to win;
    ///   * `Ours::Absent` — "not on the account" is only sound over the whole
    ///     account, which is the point of [`Self::find_ours`]'s not-found arm.
    ///
    /// [`K_ORDERS_MAX_PAGES`] is what actually bounds those.
    fn listing_for(&self, stop_at: Option<&str>) -> Result<Listing, VenueError> {
        let mut out = Vec::new();
        let mut cursor: Option<String> = None;
        let mut cut_short: Option<String> = None;
        let mut pages = 0usize;
        loop {
            if pages >= K_ORDERS_MAX_PAGES {
                return Err(VenueError::Status {
                    endpoint: "kalshi orders",
                    status: 0,
                    body: format!(
                        "the order list still has a cursor after {K_ORDERS_MAX_PAGES} pages; \
                         refusing to return a truncated list, which the caller cannot tell \
                         from a complete one"
                    ),
                });
            }
            let q = cursor.as_ref().map(|c| format!("limit=100&cursor={c}"));
            let r = self.call(
                Priority::Critical,
                "GET",
                K_ORDERS,
                q.as_deref().or(Some("limit=100")),
                None,
            )?;
            pages += 1;
            if r.status != 200 {
                return Err(VenueError::Status {
                    endpoint: "kalshi orders",
                    status: r.status,
                    body: r.body,
                });
            }
            let page = match resp::kalshi_orders_page(&r.body) {
                Ok(p) => p,
                // Only AFTER a good page. The first page keeps the hard error.
                Err(e) if cursor.is_some() => {
                    cut_short = Some(format!(
                        "the cursor walk stopped at a page that could not be read ({e}); \
                         body was: {}",
                        r.body.chars().take(300).collect::<String>()
                    ));
                    break;
                }
                Err(e) => return Err(e),
            };
            let answered = stop_at.is_some_and(|coid| {
                page.orders
                    .iter()
                    .any(|o| o.is_resting() && o.client_order_id.as_deref() == Some(coid))
            });
            out.extend(page.orders);
            if answered {
                break;
            }
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        Ok(Listing { orders: out, cut_short })
    }

    /// Every fill on this account at or after `min_ts` (unix SECONDS), oldest
    /// first.
    ///
    /// This is venue truth for the private `fill` WS channel, and it is truth
    /// with IDENTITIES rather than a count: each row carries the `trade_id`
    /// that channel dedupes on, so a caller can merge the venue's fills into
    /// its own set instead of overwriting a running total with a number. That
    /// difference is the whole reason a reconciliation is safe — see
    /// `arb-trader`'s `KalshiFills::claim`.
    ///
    /// [`Priority::Background`], and this one really is a poll: unlike
    /// [`Self::all_orders`] no order-path limb needs it, a hedge never waits on
    /// it, and refusing it locally when the budget is spent is exactly right
    /// (quirk `xv-shared-api-budget`). A refused reconciliation leaves the
    /// engine no worse than it was; a refused cancel leaves an order resting.
    ///
    /// `min_ts` is filtered CLIENT-side. Kalshi may well accept a `min_ts`
    /// query parameter, but nothing in this repo has ever sent one, and a
    /// parameter the venue rejects fails the whole read while one it silently
    /// ignores costs only pages.
    ///
    /// THE SORT ORDER IS NOT ASSUMED. Every row of the live dump in
    /// `data/venue/kalshi_fills.json` is newest-first, but nothing in this repo
    /// produces that dump, so "the venue sorts descending" and "whatever
    /// fetched it sorted descending" are indistinguishable from the artifact.
    /// Stopping early on that assumption would, against an oldest-first venue,
    /// discard page 1 as entirely pre-window and return `Ok(vec![])` — a
    /// permanently inert reconciliation reporting itself healthy, which is the
    /// exact silent-zero `resp::kalshi_fills_page` exists to prevent. So the
    /// walk stops early only on a page it can SEE is descending, and otherwise
    /// pages to the cursor's end.
    ///
    /// Running out of pages with a cursor still live is an ERROR, not a partial
    /// answer, for the same reason: a truncated list is indistinguishable from
    /// a complete one to the caller, and this one is used to decide whether
    /// contracts are unhedged.
    pub fn fills_since(&self, min_ts: i64) -> Result<Vec<resp::KalshiFillRow>, VenueError> {
        let mut out: Vec<resp::KalshiFillRow> = Vec::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        loop {
            if pages >= K_FILLS_MAX_PAGES {
                return Err(VenueError::Status {
                    endpoint: "kalshi fills",
                    status: 0,
                    body: format!(
                        "fill history still has a cursor after {K_FILLS_MAX_PAGES} pages; \
                         refusing to return a truncated list, which the caller cannot tell \
                         from a complete one"
                    ),
                });
            }
            let q = cursor.as_ref().map(|c| format!("limit=100&cursor={c}"));
            let r = self.call(
                Priority::Background,
                "GET",
                K_FILLS,
                q.as_deref().or(Some("limit=100")),
                None,
            )?;
            pages += 1;
            if r.status != 200 {
                return Err(VenueError::Status {
                    endpoint: "kalshi fills",
                    status: r.status,
                    body: r.body,
                });
            }
            let page = resp::kalshi_fills_page(&r.body)?;
            // A row with no `ts` reads as 0 and would look pre-window, so
            // absence is treated as "inside the window" and kept. The trade_id
            // merge makes an extra row free; a missing one is a fill nobody
            // hedges.
            //
            // Two or more rows, non-increasing, AT LEAST ONE STRICT DECREASE,
            // and one of them already outside the window: only then is "no
            // later page can help" a fact rather than a guess.
            //
            // The strict decrease is not pedantry. Equal timestamps are common
            // here — a fill split across price levels reports its pieces on the
            // same second — and a page whose `ts` never changes is
            // simultaneously non-increasing and non-decreasing, i.e. evidence
            // of neither direction. A one-row page proves nothing either.
            let descending = page.fills.len() >= 2
                && page.fills.windows(2).all(|w| w[0].ts >= w[1].ts)
                && page.fills.windows(2).any(|w| w[0].ts > w[1].ts);
            let has_older = page.fills.iter().any(|f| f.ts != 0 && f.ts < min_ts);
            out.extend(page.fills.into_iter().filter(|f| f.ts == 0 || f.ts >= min_ts));
            if descending && has_older {
                break;
            }
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        // Oldest first, whatever order the venue used. Stable, so rows sharing
        // a timestamp keep the venue's own sequence.
        out.sort_by_key(|r| r.ts);
        Ok(out)
    }

    /// Where an order carrying OUR `client_order_id` stands on this account.
    /// The three cases are deliberately distinct: "resting", "on the account
    /// but finished" and "not here at all" call for three different answers
    /// from a cancel, and collapsing them is how a cancel reports success for
    /// an order it never touched.
    /// A TRUNCATED walk cannot answer "not here at all". `all_orders` discards
    /// `cut_short` — a refactor artifact of this change — and reading a PREFIX
    /// of the account as the account is how `Ours::Absent` becomes a wrong
    /// diagnosis: the caller then prints "the place may have been rejected, or
    /// the order list has not caught up" about an order that is simply on a page
    /// nobody read. `Absent` is only sound over a COMPLETE list, so the
    /// truncation is surfaced instead.
    ///
    /// Finding it despite the truncation is still an answer — a prefix that
    /// contains the order tells the truth about it — so only the not-found arm
    /// has to care.
    ///
    /// AND NEITHER CAN A LIST THAT DOES NOT ECHO THE TAG. `Absent` is a claim
    /// about the venue; over a list that carries no `client_order_id` on ANY row
    /// it is a claim about this lookup being structurally blind, and the two
    /// read identically. `Book::echoes_tags` is the check the sweep already
    /// makes for exactly this reason and it is checked here over the SAME bytes
    /// — the premise a scope rests on cannot be verified against a different
    /// read than the one it is applied to.
    ///
    /// It belongs on the not-found arm ONLY. A row that matched carried the tag,
    /// which demonstrates the echo outright: the walk may have stopped early, so
    /// there is no complete list to check, and there is nothing left to check
    /// FOR.
    fn find_ours(&self, coid: &str) -> Result<Ours, VenueError> {
        let listing = self.listing_for(Some(coid))?;
        let mut gone: Option<String> = None;
        for o in &listing.orders {
            if o.client_order_id.as_deref() != Some(coid) {
                continue;
            }
            if o.is_resting() {
                return Ok(Ours::Resting(o.order_id.clone()));
            }
            // First finished row wins, and only if no resting one turns up:
            // the walk keeps going precisely so a resting row on a later page
            // still beats it.
            gone.get_or_insert(o.order_id.clone());
        }
        if let Some(id) = gone {
            return Ok(Ours::Gone(id));
        }
        truncated(listing.cut_short)?;
        if let Some(e) = Book::read(listing.orders).premise_broken() {
            return Err(e);
        }
        Ok(Ours::Absent)
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
            Ours::Gone(_) | Ours::Absent => Ok(None),
        }
    }
}

/// `Err` when the walk that produced this result was cut short. The cancels have
/// already gone out by the time this is consulted — this decides only what the
/// operation REPORTS, and reporting success is what would arm the engine.
fn truncated(cut_short: Option<String>) -> Result<(), VenueError> {
    match cut_short {
        Some(why) => Err(VenueError::Parse { endpoint: "kalshi:orders", detail: why }),
        None => Ok(()),
    }
}

/// One cursor walk, and whether it reached the end.
struct Listing {
    orders: Vec<resp::KalshiOrder>,
    /// `Some(why)` when a CONTINUATION page could not be read, so the rows below
    /// are a prefix of the account, not the account.
    cut_short: Option<String>,
}

/// The account history, partitioned the way a scoped sweep needs it.
///
/// Built from ONE paginated read, because the premise the scope rests on has to
/// be checked against the same bytes the scope is applied to.
struct Book {
    /// Resting orders carrying a tag of ours — cancel these, and they must be
    /// gone before the book is proven.
    ours: Vec<String>,
    /// Resting rows with NO `client_order_id` at all. Not cancelled — nothing
    /// claims them — and not counted against the proof either: an order a human
    /// places through Kalshi's own web UI has no tag, and must not be able to
    /// stop this process arming.
    untagged: usize,
    /// Did ANY row in the whole history — resting or finished, ours or theirs —
    /// carry a `client_order_id`?
    ///
    /// THIS IS THE PREMISE, CHECKED RATHER THAN ASSUMED. Every scoping decision
    /// below is worthless if the list does not echo the tag, and NOTHING in this
    /// repo has ever demonstrated that it does: the only live-provenance list
    /// row on record (`tests/test_venue_contracts.py`, 2026-07-21) does not
    /// carry the field, and the production path that reads it —
    /// [`KalshiGateway::find_ours`] — has never fired on the armed unit, so its
    /// deliberately quiet "nothing cancelled" would have hidden a non-echoing
    /// list indefinitely. That quiet is why `find_ours` consults this check on
    /// its own not-found arm rather than leaving it to the sweep: it is now
    /// reached by the lost-place recovery too, which would otherwise report the
    /// LAG about a lookup that is structurally blind.
    ///
    /// History is the right place to look and costs nothing extra: this account
    /// has hundreds of finished rows, both stacks have always sent a
    /// `client_order_id` (Kalshi's create body REQUIRES it —
    /// [`crate::wire::kalshi_place_body`]), so one tag anywhere proves the field
    /// survives the round trip. Zero tags across a non-empty history is the
    /// signature of the premise being false, and is the only case that refuses.
    echoes_tags: bool,
    /// Rows seen at all, so an empty account is not mistaken for a broken one.
    rows: usize,
}

impl Book {
    fn read(orders: Vec<resp::KalshiOrder>) -> Self {
        let mut b = Book { ours: Vec::new(), untagged: 0, echoes_tags: false, rows: orders.len() };
        for o in orders {
            match o.client_order_id.as_deref() {
                Some(c) => {
                    b.echoes_tags = true;
                    if o.is_resting() && super::is_ours(c) {
                        b.ours.push(o.order_id);
                    }
                }
                None if o.is_resting() => b.untagged += 1,
                None => {}
            }
        }
        b
    }

    /// The refusal, when the premise the scope rests on is demonstrably false.
    ///
    /// Deliberately NOT a silent fallback in either direction. Cancelling the
    /// whole account anyway is the documented failure this change exists to
    /// stop; answering "clean" is the defect the other half of this change
    /// exists to stop. So it is named, it carries the numbers, and it names the
    /// flag that overrides it — `--sweep-unscoped` restores the old
    /// account-wide behaviour for an operator who has decided to accept it.
    /// Say out loud how many resting rows this sweep declined to attribute.
    ///
    /// `untagged` was computed and thrown away, which left the ONE residual in
    /// this design with no signal at all: the premise check asks whether the tag
    /// survives the round trip ANYWHERE in history, but the proof relies on it
    /// being present on EVERY resting row of ours. A venue that echoed the tag
    /// on finished rows but dropped it from resting ones would pass the premise,
    /// land our own resting orders in `untagged`, and let `resting_order_ids`
    /// answer `Ok(vec![])` — "PROVEN clean" over our own book.
    ///
    /// Tightening the check cannot fix that without wedging the web-UI order
    /// this design deliberately tolerates, so the residual stays and this line
    /// is how anyone would ever notice it. On a healthy account it is silent;
    /// a count that tracks our own quote count is the tell.
    fn report_untagged(&self) {
        if self.untagged > 0 {
            eprintln!(
                "[venue] kalshi sweep: {} resting order(s) carry NO client_order_id — not \
                 cancelled and not counted as ours. Expected for orders placed by hand in \
                 the venue's own UI; if this tracks THIS engine's quote count, the list has \
                 stopped echoing the tag on resting rows and the sweep is proving nothing.",
                self.untagged
            );
        }
    }

    /// Read by BOTH sweep halves and by [`KalshiGateway::find_ours`], so the
    /// wording names the shared premise rather than the sweep alone: a lookup by
    /// our own id — a client-id cancel, a lost-place recovery — is exactly as
    /// blind as a scoped sweep when the tag does not come back.
    fn premise_broken(&self) -> Option<VenueError> {
        (!self.echoes_tags && self.rows > 0).then(|| VenueError::Status {
            endpoint: "kalshi orders",
            status: 0,
            body: format!(
                "not one of {} order(s) in this account's history carries a \
                 `client_order_id`, so the order list is not echoing the tag EVERY \
                 lookup by our own id depends on — the scoped sweep, the client-id \
                 cancel, and the recovery of a place whose answer was lost — and \
                 NOTHING here can be attributed to this process. \
                 Refusing to cancel blind on a SHARED key, and refusing to call the \
                 book clean. Re-run with --sweep-unscoped to cancel EVERY resting \
                 order on the account instead (the pre-2026-07-29 behaviour)",
                self.rows
            ),
        })
    }
}

/// The state of an order we tagged with a `client_order_id`, as the venue's own
/// order list reports it.
enum Ours {
    /// Still resting, under the venue's own id.
    Resting(String),
    /// On the account but finished (canceled or executed) — a cancel has
    /// nothing left to do. It carries the venue's id anyway, because a CANCEL
    /// is not the only thing that needs one: a fill arrives under the venue's
    /// id and nothing else, so an order that finished by EXECUTING is exactly
    /// the one whose id the engine still has to learn
    /// ([`KalshiGateway::recover_place`]).
    Gone(String),
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

    fn fills_since(&self, min_ts: i64) -> Result<Vec<resp::KalshiFillRow>, VenueError> {
        KalshiGateway::fills_since(self, min_ts)
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
                Ours::Gone(_) => return Ok(()),
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

    /// Find an order this process placed but could not read the answer for.
    ///
    /// THE PRODUCTION CALLER for the handle this gateway has always had.
    /// `client_order_id` goes out IN THE CREATE BODY (Kalshi requires it —
    /// [`wire::kalshi_place_body`]), so an order the venue took is carrying our
    /// tag before any ack exists; [`Self::cancel_by_client_order_id`] was
    /// written for a lost create on 2026-07-27 — that is the incident, not
    /// evidence the path has ever fired — and [`Self::cancel_all_open`] scopes
    /// itself with the same tag. What had no caller was
    /// this hook — `arb-trader`'s executor asks every sink for a lost place and
    /// Kalshi answered with the trait DEFAULT, `Ok(None)`: "the venue never saw
    /// it", asserted about an order that may well be resting. PM-US got its
    /// implementation and Kalshi, which has the better handle of the two, kept
    /// the default.
    ///
    /// ON THE ACCOUNT AT ALL is the question, not "resting". Two different
    /// orders need the id back and only one of them is resting:
    ///   * a resting maker, so it can be CANCELLED. Nothing else can address it
    ///     — a fill on it would arrive under an id the engine never learned;
    ///   * an IOC hedge that already EXECUTED, so its fill has something to be
    ///     attributed TO. `engine::fill::on_order_ack` is the join: the ack maps
    ///     venue id to ours and REPLAYS a fill that beat it. The incident
    ///     recorded there — an ack merely 48 ms late, the fill dropped, the
    ///     obligation credited 0, the retry buying the hedge a second time — is
    ///     that same race, and a lost response is it with no ack coming at all.
    ///
    /// So both arms hand the id back. Adopting a finished order costs nothing:
    /// the ack only teaches the engine an id, and a cancel later sent to a dead
    /// order is the 404 quirk K4 already calls success.
    ///
    /// IT DOES NOT ON ITS OWN CLOSE THE DOUBLE HEDGE, and must not be read as if
    /// it does. `HOLD_FOR_ACK` and the transport's own timeout are both 15s, so
    /// on a TIMED-OUT hedge the hold expires at the instant this read begins and
    /// the retry is already dispatched; once dispatched nothing re-examines it.
    /// This restores the id — which is what makes the duplicate visible and
    /// attributable — and `engine::hedge` is where the race itself is fixed.
    ///
    /// ABSENT IS AN ERROR, NOT `Ok(None)`. The order list LAGS a write (see
    /// [`Settle`]), so "not on the account" and "not on the account YET" are
    /// the same bytes — and this is only reached when the place's answer was
    /// LOST, where the safe reading is that the venue has it. `Ok(None)` is
    /// silent at the caller; the error is what makes it say CHECK THE VENUE.
    /// [`Self::cancel`] has treated the same `Ours::Absent` the same way for
    /// the same reason.
    ///
    /// `claimed` is unused, and that is a property of the handle rather than an
    /// oversight. It exists because PM-US matches on market and size, so a
    /// resting list that lags can offer back an order this process already
    /// owns; a `client_order_id` is minted once and identifies ONE order, so
    /// there is no id here to confuse with another.
    fn recover_place(
        &self,
        req: &PlaceRequest,
        _claimed: &HashSet<String>,
    ) -> Result<Option<String>, VenueError> {
        match self.find_ours(&req.client_order_id)? {
            Ours::Resting(id) | Ours::Gone(id) => Ok(Some(id)),
            Ours::Absent => Err(VenueError::Status {
                endpoint: "kalshi recover_place",
                status: 0,
                body: format!(
                    "no order on this account carries client_order_id `{}`, and the order \
                     list LAGS a write — so this cannot be read as proof the venue never \
                     took the place whose answer we lost",
                    req.client_order_id
                ),
            }),
        }
    }

    fn order_status(&self, order_id: &str) -> Result<Self::Order, VenueError> {
        self.order_status_at(Priority::Background, order_id)
    }

    fn order_filled_qty(&self, order_id: &str) -> Result<i64, VenueError> {
        let o = self.settle.retry_404("kalshi order_filled_qty", order_id, || {
            self.order_status_at(Priority::Critical, order_id)
        })?;
        o.try_filled_qty().ok_or_else(|| VenueError::Parse {
            endpoint: "kalshi order_filled_qty",
            detail: format!(
                "order {order_id} answered 200 with no readable fill count (fill_count_fp={:?} \
                 fill_count={:?}). Every recorded row carries it, `0.00` included, so this is a \
                 payload shape change — NOT evidence the order did not trade.",
                o.fill_count_fp, o.fill_count
            ),
        })
    }

    fn order_fill(&self, order_id: &str) -> Result<Option<crate::resp::OrderFill>, VenueError> {
        let o = self.settle.retry_404("kalshi order_fill", order_id, || {
            self.order_status_at(Priority::Critical, order_id)
        })?;
        Ok(o.filled_cost().map(crate::resp::OrderFill::Notional))
    }

    /// The evidence half of the sweep, scoped to match the cancel half.
    fn resting_order_ids(&self) -> Result<Vec<String>, VenueError> {
        let listing = self.listing()?;
        // A walk that was cut short cannot prove anything: the page we never
        // read could hold a resting order of ours, and answering "empty" here is
        // precisely the "PROVEN clean over a book nobody read" defect.
        if let Some(why) = listing.cut_short {
            return Err(VenueError::Parse { endpoint: "kalshi:orders", detail: why });
        }
        if self.unscoped_sweep {
            return Ok(listing
                .orders
                .into_iter()
                .filter(|o| o.is_resting())
                .map(|o| o.order_id)
                .collect());
        }
        let book = Book::read(listing.orders);
        match book.premise_broken() {
            Some(e) => Err(e),
            None => Ok(book.ours),
        }
    }

    /// Kill-switch sweep. `/portfolio/orders` returns history (canceled and
    /// executed too), so filter to `resting` — never try to cancel a dead order.
    ///
    /// And filter to OURS. This looped every resting order on the account
    /// regardless of `client_order_id`, against the port requirement in
    /// `docs/venue-quirks.md` §`xv-graceful-shutdown-cancels-orders`, on the
    /// key `arbbot-hedge.timer` also trades under. The material was already
    /// here — [`Self::find_ours`] matches on exactly this field — and the sweep
    /// was the one caller not using it.
    ///
    /// SCOPING DOES NOT COST THE UNACKED-ORDER CATCH, which is the reason an
    /// unscoped sweep was worth keeping. The `client_order_id` goes out IN THE
    /// CREATE BODY, so an order whose ack we never read is already carrying our
    /// tag when it starts resting: it is ours on the very next list read, with
    /// no ack, no venue id and no local record needed. That is a STRONGER
    /// handle than the sweep had before — the old blanket cancel could only
    /// catch it by catching everything — and it is the same handle
    /// [`Self::cancel_by_client_order_id`] recovers a lost create with. Orders a
    /// PREVIOUS run left behind are caught for the same reason: `is_ours` is a
    /// property of this codebase's ids, not of one process's seed.
    ///
    /// AND THE SCOPE IS NEVER ASSUMED. All of the above is void if the order
    /// list does not echo the tag, which nothing in this repo has ever shown it
    /// does — so [`Book`] checks that against the same read, and refuses rather
    /// than quietly cancelling nothing. See [`Book::echoes_tags`].
    fn cancel_all_open(&self) -> Result<(), VenueError> {
        // A cut-short walk cancels everything it DID read and then still
        // REPORTS the truncation. Both halves matter and the first cut had only
        // the first: withholding the cancel would leave read orders resting to
        // protect a proof we could never give, but returning `Ok(())` sets
        // `cancel_accepted` in the sweep, which makes the whole failure
        // `is_only_unconfirmed()` — and the engine ARMS over pages 3..n it never
        // read. That is defect A relocated one layer up, and it is likeliest
        // exactly when page 1 holds no resting rows, which is the common case.
        //
        // Note the asymmetry it fixes: a continuation page with status != 200 is
        // already fail-closed (`listing` returns Err), so only the
        // 200-with-unreadable-body page could arm — the same hypothesised body
        // class this whole change exists to refuse.
        let listing = self.listing()?;
        let cut_short = listing.cut_short;
        let orders = listing.orders;
        if self.unscoped_sweep {
            for o in orders {
                if o.is_resting() {
                    self.cancel(&CancelRequest {
                        by: CancelBy::VenueId(o.order_id),
                        market_slug: None,
                    })?;
                }
            }
            return truncated(cut_short);
        }
        let book = Book::read(orders);
        if let Some(e) = book.premise_broken() {
            return Err(e);
        }
        book.report_untagged();
        for oid in book.ours {
            self.cancel(&CancelRequest { by: CancelBy::VenueId(oid), market_slug: None })?;
        }
        truncated(cut_short)
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

    /// One market's top of book and tick ladder — port of
    /// `scripts/hedge_naked_legs.py:89-100`'s `kalshi_ask`, with the two things
    /// that function gets wrong left OUT rather than carried over. Both are
    /// named on [`super::Quote`]: an empty side is `None` and not zero, and the
    /// ladder is not collapsed to its first rung.
    ///
    /// [`Priority::Critical`], not `Background`, on the same argument
    /// `all_orders` makes for itself: a hedge cannot be priced without this, and
    /// `Background` is a hard `try_acquire` rather than a wait — so metering it
    /// would let an empty bucket turn into "no quote", and a hedge refused for
    /// want of a token is a naked leg (`ratelimit.rs`). It is ONE request, on a
    /// 5-minute cycle, for at most a couple of markets, which is well inside
    /// what quirk `xv-shared-api-budget`'s order-path corollary already accepts.
    ///
    /// The venue answering about a DIFFERENT ticker, or about none, is an error
    /// and not an empty quote: `tickers=` is a filter, and a filter the venue
    /// ignored would otherwise price one market's hedge off another's book.
    fn market_quote(&self, market: &str) -> Result<super::Quote, VenueError> {
        let q = format!("tickers={market}");
        let r = self.call(Priority::Critical, "GET", K_MARKETS, Some(&q), None)?;
        if r.status != 200 {
            return Err(VenueError::Status {
                endpoint: "kalshi markets",
                status: r.status,
                body: r.body,
            });
        }
        let page = resp::kalshi_markets(&r.body)?;
        let Some(m) = page.markets.into_iter().find(|m| m.ticker == market) else {
            return Err(VenueError::Parse {
                endpoint: "kalshi markets",
                detail: format!("no row for {market} in the listing response"),
            });
        };
        // "0.0000" is how this venue spells an EMPTY side; see `Quote::yes_bid`.
        let side = |s: String| {
            let zero = s.trim().parse::<f64>().map(|v| v == 0.0).unwrap_or(false);
            if zero {
                None
            } else {
                Some(s)
            }
        };
        Ok(super::Quote {
            market: m.ticker,
            status: m.status,
            yes_bid: side(m.yes_bid_dollars),
            yes_ask: side(m.yes_ask_dollars),
            ladder: m
                .price_ranges
                .into_iter()
                .map(|p| (p.start, p.end, p.step))
                .collect(),
        })
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

    /// Ticker -> signed contract count, over the WHOLE position list.
    ///
    /// Walks the cursor, unlike [`Self::positions`], which reads one page and
    /// ignores it. That single page is one of the four objections `orphan.rs`
    /// raises against reconciling anything from venue positions — "`positions()`
    /// on both gateways reads ONE PAGE and ignores the cursor" — and it is fatal
    /// HERE and not in the balance-shaped callers, because a ticker left off the
    /// last page comes back as no entry at all, a caller reads that as zero, and
    /// zero on our side of a basket whose other leg WAS read is precisely the
    /// shape of a naked leg. Running out of pages is an error, not a short map
    /// (see [`K_POSITIONS_MAX_PAGES`]).
    ///
    /// `position_fp` ABSENT is 0 — that is what
    /// `scripts/hedge_naked_legs.py:57` does (`float(p.get("position_fp") or
    /// 0)`) and the field is optional in the wire shape. `position_fp` PRESENT
    /// and unparseable is an error: a count folded to zero by a decoder is the
    /// same false-naked reading as one dropped by a truncated page, and this is
    /// the one place in the file where a `0` default would be a claim rather
    /// than a blank.
    fn net_positions(&self) -> Result<BTreeMap<String, f64>, VenueError> {
        let mut out: BTreeMap<String, f64> = BTreeMap::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        loop {
            if pages >= K_POSITIONS_MAX_PAGES {
                return Err(VenueError::Status {
                    endpoint: "kalshi positions",
                    status: 0,
                    body: format!(
                        "the position list still has a cursor after {K_POSITIONS_MAX_PAGES} \
                         pages; refusing to return a truncated map, whose missing tickers a \
                         caller cannot tell from a flat account"
                    ),
                });
            }
            // No query on the FIRST page: that is the request this endpoint has
            // always been sent, and a `limit` the venue happens to reject would
            // fail a read that works today. Paging only adds the cursor.
            let q = cursor.as_ref().map(|c| format!("cursor={c}"));
            let r = self.call(Priority::Background, "GET", K_POSITIONS, q.as_deref(), None)?;
            pages += 1;
            if r.status != 200 {
                return Err(VenueError::Status {
                    endpoint: "kalshi positions",
                    status: r.status,
                    body: r.body,
                });
            }
            let page = resp::kalshi_positions(&r.body)?;
            for p in page.market_positions {
                let q = match p.position_fp.as_deref() {
                    None => 0.0,
                    Some(s) => s.trim().parse::<f64>().map_err(|_| VenueError::Parse {
                        endpoint: "kalshi positions",
                        detail: format!("{}: position_fp {s:?} is not a number", p.ticker),
                    })?,
                };
                out.insert(p.ticker, q);
            }
            match page.cursor {
                Some(c) if !c.is_empty() => cursor = Some(c),
                _ => break,
            }
        }
        Ok(out)
    }

    /// `balance_dollars`, UNMODIFIED.
    ///
    /// Not "cash minus short collateral", and the symmetry with PM-US that
    /// invites is a trap: this number is ALREADY net of it. Quirk
    /// `kalshi-short-collateral-one-dollar-per-contract` says a net-short YES
    /// position encumbers $1.00 per contract *of `balance_dollars`* — so the
    /// field is the analogue of PM-US's `buyingPower`, not of its
    /// `currentBalance`, and deducting shorts here again would double-deduct
    /// them.
    fn spendable_cash(&self) -> Result<String, VenueError> {
        Ok(self.balances()?.balance_dollars)
    }
}
