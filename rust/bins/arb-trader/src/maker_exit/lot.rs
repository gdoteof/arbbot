//! Durable, order-specific accounting for concurrent lot exits on one market.
//! Account position deltas cannot identify fills when sibling exits are live.
use super::*;
use arb_venue::gateway::{PlaceRequest, Side, Tif};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::sync::Arc;
type Sink = Arc<dyn crate::sink::OrderSink>;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct State {
    pub rel_id: String,
    pub market: String,
    opened_bits: u64,
    active: Option<Active>,
    #[serde(skip)]
    latched: bool,
}

#[derive(Clone, Serialize, Deserialize)]
struct Active {
    order: Order,
    /// Original lot quantities and bases, oldest first. Empty in old checkpoints.
    #[serde(default)]
    allocations: Vec<Order>,
    #[serde(default)]
    regroup: bool,
    rest: Receipt,
    // Known only after the resting order is terminal; never inferred from net positions.
    filled: Option<i64>,
    hedged: i64,
    hedge: Option<Receipt>,
    attempts: u32,
}

#[derive(Clone, Serialize, Deserialize)]
struct Receipt {
    client: String,
    id: Option<String>,
    price: String,
    qty: i64,
    // A durable ledger deduplication key, allocated BEFORE sending the order.
    book_ts: f64,
    #[serde(default)]
    allocation_ts: Vec<f64>,
}

impl Receipt {
    fn new(price: String, qty: i64) -> Self {
        let client = client_order_id();
        let book_ts = client[1..].parse::<u64>().expect("numeric exit id") as f64 / 1e6;
        Self {
            client,
            id: None,
            price,
            qty,
            book_ts,
            allocation_ts: Vec::new(),
        }
    }
    fn request(&self, order: &Order, hedge: bool) -> PlaceRequest {
        let venue = if hedge { order.shape.close_venue() } else { order.shape.rest_venue() };
        let sell = order.direction.sells(venue);
        let passive = !hedge && order.cross.is_none();
        PlaceRequest {
            market: if hedge {
                order.close_market()
            } else {
                order.rest_market()
            }
            .into(),
            side: if sell { Side::Ask } else { Side::Bid },
            price: self.price.clone(),
            qty: self.qty,
            tif: if passive { Tif::Gtc } else { Tif::Ioc },
            post_only: passive,
            client_order_id: self.client.clone(),
        }
    }
}

pub(super) fn lot_key(rel: &str, opened_bits: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(rel.as_bytes());
    digest.update(opened_bits.to_be_bytes());
    format!("{:x}", digest.finalize())
}

/// Identical execution terms only; crossing plans remain separate transactions.
pub(super) fn group_key(o: &Order) -> String {
    let mut cx = Cx::default();
    let price = cx
        .parse(&o.limit)
        .map(|p| {
            let s = p.to_standard_notation_string();
            if s.contains('.') {
                s.trim_end_matches('0').trim_end_matches('.').to_owned()
            } else {
                s
            }
        })
        .unwrap_or_else(|| o.limit.clone());
    serde_json::to_string(&(
        o.market.as_str(),
        o.pm_market.as_str(),
        o.shape.tag(),
        o.direction,
        price,
        o.cross.as_ref().map(|_| o.closes_ts.to_bits()),
    ))
    .unwrap()
}

fn aggregate(members: &[Order]) -> Result<Order, String> {
    let mut order = members.first().ok_or("empty group")?.clone();
    let key = group_key(&order);
    let mut seen = BTreeSet::new();
    let mut qty = 0i64;
    for m in members {
        if m.qty < 1
            || group_key(m) != key
            || !seen.insert(lot_key(&m.rel_id, m.closes_ts.to_bits()))
        {
            return Err("incompatible prices, directions, or duplicate lot reservation".into());
        }
        qty = qty.checked_add(m.qty).ok_or("group quantity overflow")?;
    }
    if members.len() > 1 && order.cross.is_some() {
        return Err("crosses are not coalesced".into());
    }
    order.qty = qty;
    Ok(order)
}

impl Active {
    fn members(&self) -> &[Order] {
        if self.allocations.is_empty() {
            std::slice::from_ref(&self.order)
        } else {
            &self.allocations
        }
    }
}

/// Slice confirmed fills across original lot quantities, preserving every basis.
fn allocate(
    members: &[Order],
    mut offset: i64,
    mut qty: i64,
) -> Result<Vec<(usize, Order)>, String> {
    if offset < 0 || qty < 0 {
        return Err("negative fill allocation".into());
    }
    let mut out = Vec::new();
    for (i, m) in members.iter().enumerate() {
        let skip = offset.min(m.qty);
        offset -= skip;
        let take = qty.min(m.qty - skip);
        if take > 0 {
            let mut part = m.clone();
            part.qty = take;
            out.push((i, part));
            qty -= take;
        }
    }
    if qty != 0 || offset != 0 {
        return Err("fills exceed reserved group inventory".into());
    }
    Ok(out)
}

fn group_hedge_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    a: &Active,
    qty: i64,
    v: &EngineView,
) -> Result<String, String> {
    let mut limit: Option<D> = None;
    for (_, part) in allocate(a.members(), a.hedged, qty)? {
        let price = hedge_limit(cx, fees, &part, part.qty, v)?;
        let price = cx.parse(&price).ok_or("invalid member hedge price")?;
        limit = Some(match limit {
            None => price,
            Some(prev) => if a.order.direction.sells(a.order.shape.close_venue()) {
                if cx.cmp(price, prev) == Ordering::Greater { price } else { prev }
            } else if cx.cmp(price, prev) == Ordering::Less { price } else { prev },
        });
    }
    limit
        .map(|p| cx.quantize_4dp(p).to_standard_notation_string())
        .ok_or("empty hedge allocation".into())
}

impl State {
    pub fn new(exit: &crate::unwind::Exit) -> Self {
        Self {
            rel_id: exit.rel_id.clone(),
            market: exit.market_id.clone(),
            opened_bits: exit.opened_ts.to_bits(),
            active: None,
            latched: false,
        }
    }
    pub fn key(&self) -> String {
        lot_key(&self.rel_id, self.opened_bits)
    }
    pub fn reserved_keys(&self) -> Vec<String> {
        self.active
            .as_ref()
            .map(|a| {
                a.members()
                    .iter()
                    .map(|o| lot_key(&o.rel_id, o.closes_ts.to_bits()))
                    .collect()
            })
            .unwrap_or_default()
    }
    pub fn passive_key(&self) -> Option<String> {
        self.active
            .as_ref()
            .filter(|a| a.order.cross.is_none())
            .map(|a| group_key(&a.order))
    }
    /// The close-ladder contracts this lot's resting order still speaks for:
    /// its market and its full quantity while nothing has filled.
    pub fn passive_claim(&self) -> Option<(String, i64)> {
        self.active
            .as_ref()
            .filter(|a| a.order.cross.is_none() && a.filled.is_none())
            .map(|a| (close_depth_key(a.order.direction, a.order.shape, &a.order.market, &a.order.pm_market), a.order.qty))
    }
    pub fn request_regroup(&mut self) {
        if let Some(a) = &mut self.active {
            a.regroup = true;
        }
    }
    pub fn matches(&self, exit: &crate::unwind::Exit) -> bool {
        self.rel_id == exit.rel_id && self.opened_bits == exit.opened_ts.to_bits()
    }
    pub fn pm_market(&self) -> Option<&str> {
        self.active.as_ref().map(|a| a.order.pm_market.as_str())
    }
    pub fn busy(&self) -> bool {
        self.active.is_some()
    }
    fn latch(&mut self) {
        if !self.latched {
            latch_market(&self.market);
            self.latched = true;
        }
    }
    fn unlatch(&mut self) {
        if self.latched {
            unlatch_market(&self.market);
            self.latched = false;
        }
    }
    fn save(&self, ledger: &str) -> Result<(), String> {
        let dir = std::path::PathBuf::from(format!("{ledger}.maker-exits"));
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let path = dir.join(format!("{}.json", self.key()));
        let temp = path.with_extension("tmp");
        let bytes = serde_json::to_vec(self).map_err(|e| e.to_string())?;
        let mut f = std::fs::File::create(&temp).map_err(|e| e.to_string())?;
        f.write_all(&bytes)
            .and_then(|_| f.sync_all())
            .map_err(|e| e.to_string())?;
        std::fs::rename(temp, path).map_err(|e| e.to_string())?;
        std::fs::File::open(dir)
            .and_then(|f| f.sync_all())
            .map_err(|e| e.to_string())
    }
}

pub(super) fn load(ledger: &str) -> Result<Vec<State>, String> {
    let dir = format!("{ledger}.maker-exits");
    let entries = match std::fs::read_dir(&dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e.to_string()),
    };
    let mut states = Vec::new();
    for entry in entries {
        let path = entry.map_err(|e| e.to_string())?.path();
        if path.extension().and_then(|s| s.to_str()) != Some("json") {
            continue;
        }
        let mut state: State =
            serde_json::from_slice(&std::fs::read(&path).map_err(|e| e.to_string())?)
                .map_err(|e| format!("{}: {e}", path.display()))?;
        if path.file_stem().and_then(|s| s.to_str()) != Some(state.key().as_str()) {
            return Err(format!(
                "exit checkpoint identity mismatch: {}",
                path.display()
            ));
        }
        if let Some(active) = &state.active {
            let combined = aggregate(active.members())?;
            if combined.qty != active.order.qty
                || group_key(&combined) != group_key(&active.order)
                || lot_key(&combined.rel_id, combined.closes_ts.to_bits()) != state.key()
                || active
                    .members()
                    .windows(2)
                    .any(|w| w[0].closes_ts > w[1].closes_ts)
            {
                return Err(format!(
                    "invalid grouped exit reservation: {}",
                    path.display()
                ));
            }
        }
        if state.busy() {
            // Block fresh exits until every pre-restart order has been reconciled.
            state.latch();
            states.push(state);
        }
    }
    Ok(states)
}

fn checkpoint(state: &State, ledger: &str) {
    if let Err(e) = state.save(ledger) {
        // Continuing could place or hedge twice after a crash. The supervisor
        // restarts through the startup sweep and the last durable checkpoint.
        eprintln!("[maker-exit] durable exit checkpoint failed: {e}; stopping trader");
        std::process::exit(19);
    }
}

// Only explicit business rejections prove no order was accepted. A timeout,
// 5xx, or task failure retains the request for recovery by client id.
fn rejected(e: &arb_venue::VenueError) -> bool {
    matches!(
        e,
        arb_venue::VenueError::Status {
            status: 400 | 401 | 403 | 404 | 422 | 429,
            ..
        }
    ) || e.retry() == arb_venue::error::Retry::MarketHalted
}

async fn send(sink: &Sink, req: PlaceRequest) -> Result<String, (bool, String)> {
    let s = sink.clone();
    match tokio::task::spawn_blocking(move || s.place(&req)).await {
        Ok(Ok(id)) => {
            crate::engine::fill::note_sidecar_order(&id);
            Ok(id)
        }
        Ok(Err(e)) => Err((rejected(&e), e.to_string())),
        Err(e) => Err((false, e.to_string())),
    }
}

pub(super) async fn place(
    live: &mut Live,
    order: Order,
    view: &EngineView,
    k: &Sink,
    p: &Sink,
) -> Vec<String> {
    place_group(live, vec![order], view, k, p).await
}

pub(super) async fn place_group(
    live: &mut Live,
    mut members: Vec<Order>,
    view: &EngineView,
    k: &Sink,
    p: &Sink,
) -> Vec<String> {
    members.sort_by(|a, b| {
        a.closes_ts
            .total_cmp(&b.closes_ts)
            .then(a.rel_id.cmp(&b.rel_id))
    });
    let order = match aggregate(&members) {
        Ok(o) => o,
        Err(e) => return vec![format!("[maker-exit] refusing invalid group: {e}")],
    };
    let mut state = live.lot.take().expect("lot owner");
    assert!(!state.busy(), "a lot cannot reserve its inventory twice");
    assert_eq!(
        state.key(),
        lot_key(&order.rel_id, order.closes_ts.to_bits()),
        "group must belong to oldest lot"
    );
    let rest = Receipt::new(order.rest_limit().into(), order.qty);
    let req = rest.request(&order, false);
    let (sink, _) = sinks(order.shape, k, p);
    state.active = Some(Active {
        order: order.clone(),
        allocations: members.clone(),
        regroup: false,
        rest,
        filled: None,
        hedged: 0,
        hedge: None,
        attempts: 0,
    });
    checkpoint(&state, &live.ledger_path); // Before the external side effect.
    let line = match send(sink, req).await {
        Ok(id) => {
            state.active.as_mut().unwrap().rest.id = Some(id.clone());
            PLACED.fetch_add(1, AtomicOrd::Relaxed);
            format!(
                "[maker-exit] {} {}x {} @ {} [{}] lot={}:{} members={} — venue id {id}",
                if order.cross.is_some() {
                    "CROSSED"
                } else {
                    "RESTED"
                },
                order.qty,
                order.rest_market(),
                order.rest_limit(),
                order.shape.tag(),
                order.rel_id,
                order.closes_ts,
                members.len()
            )
        }
        Err((true, e)) => {
            state.active = None;
            format!(
                "[maker-exit] PLACE REFUSED lot={}:{}: {e}",
                order.rel_id, order.closes_ts
            )
        }
        Err((false, e)) => {
            state.latch();
            format!("[maker-exit] PLACE UNCERTAIN lot={}:{}: {e}; retaining reservation and client id for recovery",
                order.rel_id, order.closes_ts)
        }
    };
    checkpoint(&state, &live.ledger_path);
    live.lot = Some(state);
    live.publish_working(live.working_set(None));
    let mut out = vec![line];
    if order.cross.is_some() && live.lot.as_ref().is_some_and(State::busy) {
        out.extend(manage(live, Some(view), k, p).await);
    }
    out
}

/// How long after a lost place answer Kalshi's "no order carries this
/// client id" becomes proof the place never landed. The order list's lag is
/// the gateway's `Settle` window — seconds — so five minutes is far past it.
/// Without this a 503'd place (Kalshi's Thursday maintenance, 2026-09-24:
/// 14 lots) latched its lot for good: the checkpoint persists the latch, so
/// a restart reloaded it, and every pass walked the whole order list again.
const ABSENT_IS_PROOF_AFTER_S: f64 = 300.0;

/// `Ok(false)`: the REST place was lost and Kalshi, long after its list could
/// lag, has no order under our client id — it never landed. Never returned for
/// a hedge: a hedge's rest leg already filled, and releasing it could hedge twice.
async fn recover(
    sink: &Sink,
    receipt: &mut Receipt,
    order: &Order,
    hedge: bool,
) -> Result<bool, String> {
    if let Some(id) = &receipt.id {
        crate::engine::fill::note_sidecar_order(id);
        return Ok(true);
    }
    let venue = if hedge {
        order.shape.close_venue()
    } else {
        order.shape.rest_venue()
    };
    if venue == Venue::PolymarketUs {
        // PM-US has no client id on the wire. Its existing recovery helper
        // matches market/quantity only, which can adopt a sibling lot's order.
        return Err(format!("PM-US acknowledgement lost for {}; retaining reservation; venue order identity requires reconciliation", receipt.client));
    }
    let req = receipt.request(order, hedge);
    let s = sink.clone();
    match tokio::task::spawn_blocking(move || s.recover_place(&req, &Default::default())).await {
        Ok(Ok(Some(id))) => {
            crate::engine::fill::note_sidecar_order(&id);
            receipt.id = Some(id);
            Ok(true)
        }
        Ok(Err(arb_venue::VenueError::Status { endpoint: "kalshi recover_place", status: 0, .. }))
            if !hedge && wall_now() - receipt.book_ts > ABSENT_IS_PROOF_AFTER_S =>
        {
            Ok(false)
        }
        other => Err(format!(
            "client {} is still unconfirmed ({other:?}); no replacement will be sent",
            receipt.client
        )),
    }
}

async fn terminal(sink: &Sink, receipt: &Receipt) -> Result<Option<i64>, String> {
    let s = sink.clone();
    let id = receipt.id.clone().ok_or("missing order id")?;
    match tokio::task::spawn_blocking(move || s.terminal_filled_qty(&id)).await {
        Ok(Ok(Some(n))) if n >= 0 && n <= receipt.qty => Ok(Some(n)),
        Ok(Ok(None)) => Ok(None),
        other => Err(format!("final fill quantity is unconfirmed: {other:?}")),
    }
}

async fn cancel(sink: &Sink, receipt: &Receipt, order: &Order) -> Vec<String> {
    let r = Resting {
        order: order.clone(),
        venue_order_id: receipt.id.clone().unwrap(),
        client_order_id: receipt.client.clone(),
        since: placed_at(receipt.book_ts),
    };
    cancel_at_venue(sink, &r).await.0
}

/// The monotonic instant a lot order was placed, from its persisted wall-clock
/// id (`book_ts`), so the "pulled after Ns resting" line reports the order's
/// real age — including across a restart — rather than 0s.
fn placed_at(book_ts: f64) -> Instant {
    let age = (wall_now() - book_ts).max(0.0);
    Instant::now().checked_sub(std::time::Duration::from_secs_f64(age)).unwrap_or_else(Instant::now)
}

pub(super) async fn manage(
    live: &mut Live,
    view: Option<&EngineView>,
    k: &Sink,
    p: &Sink,
) -> Vec<String> {
    live.publish_working(live.working_set(None));
    if let Some(a) = live.lot.as_ref().and_then(|s| s.active.as_ref()) {
        live.request_suppress(
            candidate_keys_for(&a.order.market, &a.order.pm_market, a.order.direction)
                .into_iter()
                .chain(cross_keys_for(&a.order.market, &a.order.pm_market, a.order.direction))
                .collect(),
        );
    }
    let mut state = live.lot.take().expect("lot owner");
    let mut out = Vec::new();
    let result = advance(
        &mut state,
        &live.ledger_path,
        &mut live.cx,
        &live.fees,
        view,
        k,
        p,
        &mut out,
    )
    .await;
    if let Err(why) = result {
        state.latch();
        out.push(format!(
            "[maker-exit] lot={}:{}: {why}",
            state.rel_id,
            f64::from_bits(state.opened_bits)
        ));
    }
    if !state.busy() {
        state.unlatch();
    }
    checkpoint(&state, &live.ledger_path);
    let suppress = state
        .active
        .as_ref()
        .map(|a| {
            candidate_keys_for(&a.order.market, &a.order.pm_market, a.order.direction)
                .into_iter()
                .chain(cross_keys_for(&a.order.market, &a.order.pm_market, a.order.direction))
                .collect()
        })
        .unwrap_or_default();
    live.lot = Some(state);
    live.request_suppress(suppress);
    live.publish_working(live.working_set(None));
    out
}

#[allow(clippy::too_many_arguments)]
async fn advance(
    state: &mut State,
    ledger: &str,
    cx: &mut Cx,
    fees: &FeeSchedule,
    view: Option<&EngineView>,
    k: &Sink,
    p: &Sink,
    out: &mut Vec<String>,
) -> Result<(), String> {
    let a = state.active.as_mut().expect("active lot");
    let (rest_sink, hedge_sink) = sinks(a.order.shape, k, p);
    if !recover(rest_sink, &mut a.rest, &a.order, false).await? {
        out.push(format!(
            "[maker-exit] lot={}:{}: client {} never reached Kalshi (absent {:.0}s after the lost place); releasing the reservation",
            a.order.rel_id,
            a.order.closes_ts,
            a.rest.client,
            wall_now() - a.rest.book_ts
        ));
        state.active = None;
        return Ok(());
    }
    // Receipt recovery must be durable even if the next read fails.
    checkpoint(state, ledger);
    let a = state.active.as_mut().unwrap();
    if a.filled.is_none() {
        let done = terminal(rest_sink, &a.rest).await?;
        let done = if done.is_some() {
            done
        } else {
            let s = rest_sink.clone();
            let id = a.rest.id.clone().unwrap();
            let filled = match tokio::task::spawn_blocking(move || s.filled_qty(&id)).await {
                Ok(Ok(n)) if n >= 0 && n <= a.order.qty => n,
                other => return Err(format!("cannot read resting fills: {other:?}")),
            };
            // Each member must still pay at its own basis, and the GROUP must
            // still have close depth for its whole quantity: members were
            // sized within one shared budget, so the sum is what the ladder
            // has to back.
            let stale = if a.regroup || a.order.cross.is_some() || filled != 0 {
                None
            } else {
                match view {
                    None => Some("no engine view to hold it against".to_string()),
                    Some(v) => a
                        .members()
                        .iter()
                        .chain(std::iter::once(&a.order))
                        .find_map(|o| still_pays(cx, fees, o, v).err()),
                }
            };
            if let Some(why) = stale {
                out.push(format!("[maker-exit] PULLING {} — {why}", a.order.rest_market()));
            } else if !a.regroup && a.order.cross.is_none() && filled == 0 {
                state.unlatch();
                return Ok(());
            }
            out.extend(cancel(rest_sink, &a.rest, &a.order).await);
            terminal(rest_sink, &a.rest).await?
        };
        a.filled =
            Some(done.ok_or("cancellation is not yet terminal; retaining the lot reservation")?);
        checkpoint(state, ledger);
    }
    let a = state.active.as_mut().unwrap();
    if a.filled == Some(0) {
        out.push(format!(
            "[maker-exit] lot={}:{}: order terminal, zero fills",
            a.order.rel_id, a.order.closes_ts
        ));
        state.active = None;
        return Ok(());
    }
    if a.hedged == a.filled.unwrap() {
        // A crash can land after the last receipt was booked and checkpointed,
        // but before the active reservation was cleared. Do not send a zero IOC.
        state.active = None;
        return Ok(());
    }
    // Up to one hedge is sent per pass. Terminal proof is required before another.
    for _ in 0..2 {
        let a = state.active.as_mut().unwrap();
        if let Some(receipt) = &mut a.hedge {
            recover(hedge_sink, receipt, &a.order, true).await?;
            let got = terminal(hedge_sink, receipt)
                .await?
                .ok_or("hedge IOC is not terminal; waiting before retrying")?;
            if got > 0 {
                let close_px = filled_price_or_limit(
                    cx,
                    hedge_sink,
                    receipt.id.as_ref().unwrap(),
                    &receipt.price,
                    a.order.direction.sells(a.order.shape.close_venue()),
                )
                .await;
                let rest_px = filled_price_or_limit(
                    cx,
                    rest_sink,
                    a.rest.id.as_ref().unwrap(),
                    &a.rest.price,
                    a.order.direction.sells(a.order.shape.rest_venue()),
                )
                .await;
                let (k_px, p_px) = fills_by_venue(&a.order, &rest_px, &close_px);
                let (rest_px, close_px) = if plausible_pair_direction(cx, a.order.direction, k_px, p_px) {
                    (rest_px, close_px)
                } else {
                    (a.rest.price.clone(), receipt.price.clone())
                };
                let members = if a.allocations.is_empty() {
                    vec![a.order.clone()]
                } else {
                    a.allocations.clone()
                };
                for (index, part) in allocate(&members, a.hedged, got)? {
                    let ts = if receipt.allocation_ts.is_empty() && members.len() == 1 {
                        receipt.book_ts
                    } else {
                        *receipt
                            .allocation_ts
                            .get(index)
                            .ok_or("missing allocation booking key")?
                    };
                    let rec = close_record(&part, &rest_px, &close_px, part.qty, ts);
                    let booked = ledger::append_basket(ledger, rec)?;
                    if !matches!(booked, ledger::Booking::AlreadyBooked) {
                        CLOSED.fetch_add(1, AtomicOrd::Relaxed);
                    }
                    out.push(format!(
                        "[maker-exit] CLOSED {}x {} lot={}:{} hedge={} (oldest-first)",
                        part.qty,
                        part.market,
                        part.rel_id,
                        part.closes_ts,
                        receipt.id.as_ref().unwrap()
                    ));
                }
                // All allocation records must be durable before this receipt is forgotten.
                std::fs::OpenOptions::new()
                    .append(true)
                    .open(ledger)
                    .and_then(|f| f.sync_all())
                    .map_err(|e| e.to_string())?;
                a.hedged += got;
            }
            a.hedge = None;
            checkpoint(state, ledger);
            let a = state.active.as_ref().unwrap();
            if a.hedged == a.filled.unwrap() {
                state.active = None;
                return Ok(());
            }
            // Preserve the unclosed quantity; no account-level inference can
            // mistake a sibling's fill for this lot's hedge.
            return Err(format!(
                "hedge partly filled; {} contract(s) still owed",
                a.filled.unwrap() - a.hedged
            ));
        }
        let a = state.active.as_mut().unwrap();
        let view = view.ok_or("no fresh engine view to price the hedge")?;
        let owed = a.filled.unwrap() - a.hedged;
        a.attempts += 1;
        let price = match group_hedge_limit(cx, fees, a, owed, view) {
            Ok(limit) => limit,
            Err(why) if a.attempts <= HEAL_PROFITABLE_CYCLES => return Err(why),
            Err(_) => {
                let venue = a.order.shape.close_venue();
                let selling = a.order.direction.sells(venue);
                let touch = match (venue, selling) {
                    (Venue::PolymarketUs, false) => view.pm_ask.get(a.order.close_market()),
                    (Venue::PolymarketUs, true) => view.pm_bid.get(a.order.close_market()),
                    (Venue::Kalshi, false) => view.k_ask.get(a.order.close_market()),
                    (Venue::Kalshi, true) => view.k_bid.get(a.order.close_market()),
                    _ => None,
                }
                .and_then(|s| cx.parse(s))
                .ok_or("no executable hedge book")?;
                // A Kalshi hedge steps on its own ladder; PM-US on the flat cent.
                let ladder = match venue {
                    Venue::Kalshi => view.k_ladder.get(a.order.close_market()).map(Vec::as_slice),
                    _ => None,
                };
                let limit = kalshi_step(cx, ladder, touch, selling)
                    .ok_or("no legal hedge price through the touch")?;
                if !cx.is_pos(limit) || cx.cmp(limit, cx.one) != Ordering::Less {
                    return Err("hedge price outside (0, 1)".into());
                }
                cx.quantize_4dp(limit).to_standard_notation_string()
            }
        };
        let mut receipt = Receipt::new(price, owed);
        receipt.allocation_ts = a
            .members()
            .iter()
            .map(|_| {
                let id = client_order_id();
                id[1..].parse::<u64>().expect("numeric exit id") as f64 / 1e6
            })
            .collect();
        let req = receipt.request(&a.order, true);
        a.hedge = Some(receipt);
        checkpoint(state, ledger);
        match send(hedge_sink, req).await {
            Ok(id) => state.active.as_mut().unwrap().hedge.as_mut().unwrap().id = Some(id),
            Err((true, why)) => {
                state.active.as_mut().unwrap().hedge = None;
                return Err(format!("hedge refused: {why}"));
            }
            Err((false, why)) => return Err(format!("hedge acceptance unknown: {why}")),
        }
        checkpoint(state, ledger);
    }
    Err("hedge completion still pending".into())
}

/// Price the second leg using the actual first-leg limit and fee role.
fn hedge_limit(
    cx: &mut Cx,
    fees: &FeeSchedule,
    o: &Order,
    qty: i64,
    v: &EngineView,
) -> Result<String, String> {
    if o.direction.inverted() {
        let mut normalized = o.clone();
        normalized.direction = Direction::Standard;
        normalized.limit = complement(cx, &o.limit).ok_or("invalid inverse rest limit")?;
        if let (Some(src), Some(dst)) = (&o.cross, &mut normalized.cross) {
            dst.limit = complement(cx, &src.limit).ok_or("invalid inverse cross limit")?;
        }
        let view = inverse_view(cx, v);
        let limit = hedge_limit(cx, fees, &normalized, qty, &view)?;
        return complement(cx, &limit).ok_or("invalid normalized hedge limit".into());
    }
    if o.cross.is_none() {
        return price_close(cx, fees, o, qty, v);
    }
    let first = cx.parse(o.rest_limit()).ok_or("invalid first-leg limit")?;
    let kb = cx.parse(&o.k_basis).ok_or("invalid Kalshi basis")?;
    let pb = cx.parse(&o.pm_basis).ok_or("invalid PM basis")?;
    let limit = match o.shape {
        Shape::RestKalshi => {
            let limit = close_limit(
                cx,
                fees,
                first,
                kb,
                pb,
                qty,
                Role::Taker,
                Role::Taker,
                MIN_LOCK,
            )?;
            let ask = v
                .pm_ask
                .get(&o.pm_market)
                .and_then(|s| cx.parse(s))
                .ok_or("no PM ask")?;
            if cx.cmp(ask, limit) == Ordering::Greater {
                return Err("PM ask exceeds close ceiling".into());
            }
            limit
        }
        Shape::RestPmUs => {
            let ladder = v.k_ladder.get(&o.market).cloned()
                .unwrap_or_else(|| vec![("0".into(), "1".into(), "0.01".into())]);
            let limit = exit_limit(
                cx,
                fees,
                &ladder,
                kb,
                pb,
                first,
                qty,
                Role::Taker,
                Role::Taker,
                MIN_LOCK,
            )?;
            let bid = v
                .k_bid
                .get(&o.market)
                .and_then(|s| cx.parse(s))
                .ok_or("no Kalshi bid")?;
            if cx.cmp(limit, bid) == Ordering::Greater {
                return Err("Kalshi bid below close floor".into());
            }
            limit
        }
    };
    Ok(cx.quantize_4dp(limit).to_standard_notation_string())
}

#[cfg(test)]
// The existing synchronous registry-test lock must also exclude synchronous
// tests elsewhere. No spawned venue task acquires this test-only lock.
#[allow(clippy::await_holding_lock)]
mod tests {
    use super::*;
    use crate::maker_exit::tests::{cand, open_basket, resting_exit, view};
    use std::collections::{BTreeMap, VecDeque};

    #[test]
    fn inverse_requests_use_the_held_contract_direction_for_both_shapes_and_crosses() {
        for shape in [Shape::RestKalshi, Shape::RestPmUs] {
            let mut order = resting_exit(3).order;
            order.direction = Direction::Inverse;
            order.shape = shape;
            let rest = Receipt::new("0.4000".into(), 3).request(&order, false);
            let hedge = Receipt::new("0.5000".into(), 3).request(&order, true);
            assert_eq!(rest.side, if order.direction.sells(shape.rest_venue()) { Side::Ask } else { Side::Bid });
            assert_eq!(hedge.side, if order.direction.sells(shape.close_venue()) { Side::Ask } else { Side::Bid });
            order.cross = Some(Cross { limit: "0.4000".into(), lock_ct: "0.02".into() });
            let first = Receipt::new("0.4000".into(), 3).request(&order, false);
            assert_eq!(first.side, rest.side);
            assert_eq!(first.tif, Tif::Ioc);
            assert!(!first.post_only);
        }
    }

    #[derive(Clone)]
    struct VenueOrder {
        req: PlaceRequest,
        filled: i64,
        terminal: bool,
    }
    #[derive(Default)]
    struct Venue {
        orders: Mutex<BTreeMap<String, VenueOrder>>,
        ioc_fills: Mutex<VecDeque<i64>>,
        unreadable: Mutex<bool>,
        cancel_blocked: Mutex<bool>,
        lose_ack: Mutex<bool>,
        // A 503 before the venue took the order; recovery then answers like Kalshi.
        drop_place: Mutex<bool>,
        // Terminal IOC proof may lag its fill count.
        ioc_pending: Mutex<bool>,
    }
    impl Venue {
        fn fill(&self, id: &str, qty: i64, terminal: bool) {
            let mut orders = self.orders.lock().unwrap();
            let order = orders.get_mut(id).unwrap();
            order.filled = qty;
            order.terminal = terminal;
        }
        fn iocs(&self) -> Vec<PlaceRequest> {
            self.orders
                .lock()
                .unwrap()
                .values()
                .filter(|o| !o.req.post_only)
                .map(|o| o.req.clone())
                .collect()
        }
    }
    impl crate::sink::OrderSink for Venue {
        fn place(&self, req: &PlaceRequest) -> Result<String, arb_venue::VenueError> {
            if *self.drop_place.lock().unwrap() {
                return Err(arb_venue::VenueError::Status {
                    endpoint: "kalshi place",
                    status: 503,
                    body: "service_unavailable".into(),
                });
            }
            let mut orders = self.orders.lock().unwrap();
            let id = format!("order-{}", orders.len());
            let filled = if req.post_only {
                0
            } else {
                self.ioc_fills
                    .lock()
                    .unwrap()
                    .pop_front()
                    .unwrap_or(req.qty)
            };
            let terminal = !req.post_only && !*self.ioc_pending.lock().unwrap();
            orders.insert(
                id.clone(),
                VenueOrder {
                    req: req.clone(),
                    filled,
                    terminal,
                },
            );
            if *self.lose_ack.lock().unwrap() {
                Err(arb_venue::VenueError::NotWired)
            } else {
                Ok(id)
            }
        }
        fn cancel(
            &self,
            req: &arb_venue::gateway::CancelRequest,
        ) -> Result<(), arb_venue::VenueError> {
            if !*self.cancel_blocked.lock().unwrap() {
                let arb_venue::gateway::CancelBy::VenueId(id) = &req.by else {
                    panic!("venue id required")
                };
                self.orders.lock().unwrap().get_mut(id).unwrap().terminal = true;
            }
            Ok(())
        }
        fn filled_qty(&self, id: &str) -> Result<i64, arb_venue::VenueError> {
            if *self.unreadable.lock().unwrap() {
                return Err(arb_venue::VenueError::NotWired);
            }
            Ok(self.orders.lock().unwrap().get(id).unwrap().filled)
        }
        fn terminal_filled_qty(&self, id: &str) -> Result<Option<i64>, arb_venue::VenueError> {
            if *self.unreadable.lock().unwrap() {
                return Err(arb_venue::VenueError::NotWired);
            }
            let orders = self.orders.lock().unwrap();
            let o = orders.get(id).unwrap();
            Ok(o.terminal.then_some(o.filled))
        }
        fn recover_place(
            &self,
            req: &PlaceRequest,
            _: &std::collections::HashSet<String>,
        ) -> Result<Option<String>, arb_venue::VenueError> {
            let found = self
                .orders
                .lock()
                .unwrap()
                .iter()
                .find(|(_, o)| o.req.client_order_id == req.client_order_id)
                .map(|(id, _)| id.clone());
            if found.is_none() && *self.drop_place.lock().unwrap() {
                return Err(arb_venue::VenueError::Status {
                    endpoint: "kalshi recover_place",
                    status: 0,
                    body: "no order on this account carries client_order_id".into(),
                });
            }
            Ok(found)
        }
        fn cancel_all_open(&self) -> Result<(), arb_venue::VenueError> {
            unreachable!()
        }
        fn resting_order_ids(&self) -> Result<Vec<String>, arb_venue::VenueError> {
            unreachable!()
        }
        fn net_positions(&self) -> Result<BTreeMap<String, f64>, arb_venue::VenueError> {
            panic!("a sibling's position changes must never be used to infer this order's fills")
        }
    }
    struct Fixture {
        path: String,
        k: Arc<Venue>,
        p: Arc<Venue>,
        owners: ExitOwners,
    }
    impl Fixture {
        fn new() -> Self {
            let path = format!(
                "/tmp/arbbot-lot-exits-{}-{}.jsonl",
                std::process::id(),
                client_order_id()
            );
            std::fs::write(&path, "").unwrap();
            Self {
                path,
                k: Arc::new(Venue::default()),
                p: Arc::new(Venue::default()),
                owners: ExitOwners::default(),
            }
        }
        fn owner(&self, ts: f64, qty: i64) -> Live {
            ledger::append(&self.path, &open_basket(ts, qty, "0.22", "0.19")).unwrap();
            let mut l = Live::new(false, self.path.clone());
            l.scope_market = Some("K-a".into());
            l.lot = Some(State::new(&cand(qty, ts)));
            l.owners = Some(self.owners.clone());
            l
        }
        async fn place(&self, live: &mut Live, ts: f64, qty: i64, shape: Shape) {
            let mut o = resting_exit(qty).order;
            o.closes_ts = ts;
            o.shape = shape;
            if shape == Shape::RestPmUs {
                o.limit = "0.0100".into();
            }
            let k: Sink = self.k.clone();
            let p: Sink = self.p.clone();
            let out = place(live, o, &view("0.20"), &k, &p).await;
            assert!(live.lot.as_ref().unwrap().busy(), "{out:?}");
        }
        async fn manage(&self, live: &mut Live) -> Vec<String> {
            let k: Sink = self.k.clone();
            let p: Sink = self.p.clone();
            manage(live, Some(&view("0.20")), &k, &p).await
        }
        fn closes(&self) -> Vec<Value> {
            ledger::read(&self.path)
                .unwrap()
                .into_iter()
                .filter(|r| r["status"] == "unwound")
                .collect()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.path);
            let _ = std::fs::remove_dir_all(format!("{}.maker-exits", self.path));
        }
    }
    fn id(live: &Live) -> String {
        live.lot
            .as_ref()
            .unwrap()
            .active
            .as_ref()
            .unwrap()
            .rest
            .id
            .clone()
            .unwrap()
    }

    fn member(ts: f64, qty: i64) -> Order {
        let mut o = resting_exit(qty).order;
        o.closes_ts = ts;
        o
    }

    #[test]
    fn grouping_requires_identical_execution_terms_and_never_rounds_prices() {
        let a = member(1., 3);
        let mut b = member(2., 7);
        assert_eq!(aggregate(&[a.clone(), b.clone()]).unwrap().qty, 10);
        b.limit = format!("{}0", a.limit);
        assert_eq!(group_key(&a), group_key(&b));
        b.limit = "0.30001".into();
        let mut c = b.clone();
        c.limit = "0.30002".into();
        assert_ne!(group_key(&b), group_key(&c));
        b = member(2., 7);
        b.shape = Shape::RestPmUs;
        assert!(aggregate(&[a.clone(), b]).is_err());
        let mut b = member(2., 7);
        b.pm_market = "different".into();
        assert!(aggregate(&[a.clone(), b]).is_err());
        assert!(aggregate(&[a.clone(), a]).is_err());
    }

    #[test]
    fn allocations_keep_basis_and_choose_the_strictest_hedge_limit() {
        let old = member(1., 3);
        let mut new = member(2., 7);
        new.k_basis = "0.2500".into();
        let mut cx = Cx::default();
        let fees = FeeSchedule::new(&mut cx);
        let active = Active {
            order: aggregate(&[old.clone(), new.clone()]).unwrap(),
            allocations: vec![old.clone(), new.clone()],
            regroup: false,
            rest: Receipt::new(old.limit.clone(), 10),
            filled: Some(6),
            hedged: 0,
            hedge: None,
            attempts: 0,
        };
        let pieces = allocate(active.members(), 2, 4).unwrap();
        assert_eq!(pieces[0].1.k_basis, old.k_basis);
        assert_eq!(pieces[1].1.k_basis, new.k_basis);
        assert_eq!(
            pieces.iter().map(|(_, o)| o.qty).collect::<Vec<_>>(),
            vec![1, 3]
        );
        let group = group_hedge_limit(&mut cx, &fees, &active, 6, &view("0.20")).unwrap();
        let group = cx.parse(&group).unwrap();
        for (_, part) in allocate(active.members(), 0, 6).unwrap() {
            let price = hedge_limit(&mut cx, &fees, &part, part.qty, &view("0.20")).unwrap();
            let price = cx.parse(&price).unwrap();
            assert_ne!(cx.cmp(group, price), Ordering::Greater);
        }
        assert!(allocate(active.members(), 9, 2).is_err());
    }

    #[tokio::test]
    async fn grouped_partial_fill_is_fifo_and_retry_survives_restart() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 3);
        let _b = f.owner(2., 7);
        f.p.ioc_fills.lock().unwrap().extend([2, 4]);
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        place_group(
            &mut a,
            vec![member(2., 7), member(1., 3)],
            &view("0.20"),
            &k,
            &p,
        )
        .await;
        assert_eq!(f.k.orders.lock().unwrap().len(), 1);
        assert_eq!(f.k.orders.lock().unwrap()[&id(&a)].req.qty, 10);
        assert_eq!(a.lot.as_ref().unwrap().reserved_keys().len(), 2);
        f.k.fill(&id(&a), 6, true);
        f.manage(&mut a).await;
        assert_eq!(f.closes()[0]["closes_ts"], 1.);
        assert_eq!(f.closes()[0]["qty"], 2);
        a.lot.as_mut().unwrap().unlatch();
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        let rows = f.closes();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            (rows[1]["closes_ts"].as_f64(), rows[1]["qty"].as_i64()),
            (Some(1.), Some(1))
        );
        assert_eq!(
            (rows[2]["closes_ts"].as_f64(), rows[2]["qty"].as_i64()),
            (Some(2.), Some(3))
        );
        assert_eq!(
            f.p.iocs().iter().map(|o| o.qty).collect::<Vec<_>>(),
            vec![6, 4]
        );
        assert!(!a.lot.as_ref().unwrap().busy());
    }

    #[tokio::test]
    async fn grouped_booking_replays_after_crash_between_allocation_records() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 3);
        let _b = f.owner(2., 7);
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        *f.p.ioc_pending.lock().unwrap() = true;
        place_group(
            &mut a,
            vec![member(1., 3), member(2., 7)],
            &view("0.20"),
            &k,
            &p,
        )
        .await;
        f.k.fill(&id(&a), 6, true);
        f.manage(&mut a).await;
        let before = a.lot.clone().unwrap();
        f.p.fill("order-0", 6, true);
        f.manage(&mut a).await;
        // Retain only the first allocation to simulate death between appends.
        let rows = ledger::read(&f.path).unwrap();
        let text = rows[..rows.len() - 1]
            .iter()
            .map(|r| format!("{r}\n"))
            .collect::<String>();
        std::fs::write(&f.path, text).unwrap();
        before.save(&f.path).unwrap();
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        assert_eq!(f.closes().len(), 2);
        assert_eq!(
            f.closes()
                .iter()
                .map(|r| r["qty"].as_i64().unwrap())
                .sum::<i64>(),
            6
        );
        assert_eq!(f.p.iocs().len(), 1);
        assert!(!a.lot.as_ref().unwrap().busy());
    }

    #[tokio::test]
    async fn regroup_keeps_all_reservations_until_cancel_is_confirmed() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 3);
        let _b = f.owner(2., 7);
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        place_group(
            &mut a,
            vec![member(1., 3), member(2., 7)],
            &view("0.20"),
            &k,
            &p,
        )
        .await;
        a.lot.as_mut().unwrap().request_regroup();
        *f.k.cancel_blocked.lock().unwrap() = true;
        f.manage(&mut a).await;
        assert_eq!(a.lot.as_ref().unwrap().reserved_keys().len(), 2);
        assert!(f.p.iocs().is_empty());
        f.k.fill(&id(&a), 4, true);
        f.manage(&mut a).await;
        assert!(a.lot.as_ref().unwrap().reserved_keys().is_empty());
        assert_eq!(f.closes()[0]["qty"], 3);
        assert_eq!(f.closes()[1]["qty"], 1);
    }

    #[tokio::test]
    async fn multiple_lots_on_one_market_rest_full_inventory_and_book_their_own_partial_fills() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 34);
        let mut b = f.owner(2., 21);
        f.place(&mut a, 1., 34, Shape::RestKalshi).await;
        f.place(&mut b, 2., 21, Shape::RestKalshi).await;
        assert_eq!(f.k.orders.lock().unwrap().len(), 2);
        assert_eq!(
            f.k.orders
                .lock()
                .unwrap()
                .values()
                .map(|o| o.req.qty)
                .sum::<i64>(),
            55
        );
        assert_ne!(id(&a), id(&b));
        assert!(a.claim_pm_market("p-a") && b.claim_pm_market("p-a"));
        a.request_suppress(candidate_keys("K-a", "p-a").into_iter().collect());
        b.request_suppress(candidate_keys("K-a", "p-a").into_iter().collect());
        f.k.fill(&id(&a), 7, false);
        f.k.fill(&id(&b), 9, false);
        f.manage(&mut a).await;
        assert!(b.lot.as_ref().unwrap().busy());
        assert!(!suppress_requests().is_empty(), "sibling keeps suppression");
        f.manage(&mut b).await;
        let rows = f.closes();
        assert_eq!(rows.len(), 2);
        assert_eq!(
            (rows[0]["closes_ts"].as_f64(), rows[0]["qty"].as_i64()),
            (Some(1.), Some(7))
        );
        assert_eq!(
            (rows[1]["closes_ts"].as_f64(), rows[1]["qty"].as_i64()),
            (Some(2.), Some(9))
        );
        assert_eq!(
            crate::naked_act::open_lots(&ledger::read(&f.path).unwrap(), "r1")
                .iter()
                .map(|(_, n, _)| n)
                .sum::<i64>(),
            39
        );
    }

    #[tokio::test]
    async fn cancel_must_be_terminal_before_hedging_and_late_fills_are_included() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 20);
        f.place(&mut a, 1., 20, Shape::RestKalshi).await;
        f.k.fill(&id(&a), 3, false);
        *f.k.cancel_blocked.lock().unwrap() = true;
        f.manage(&mut a).await;
        assert!(f.p.iocs().is_empty());
        f.k.fill(&id(&a), 8, true);
        f.manage(&mut a).await;
        assert_eq!(
            f.p.iocs().iter().map(|r| r.qty).collect::<Vec<_>>(),
            vec![8]
        );
        assert_eq!(f.closes()[0]["qty"], 8);
    }

    #[tokio::test]
    async fn partial_hedge_retries_only_its_confirmed_shortfall_after_restart() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 34);
        f.p.ioc_fills.lock().unwrap().extend([3, 7]);
        f.place(&mut a, 1., 34, Shape::RestKalshi).await;
        f.k.fill(&id(&a), 10, true);
        f.manage(&mut a).await;
        assert_eq!(f.closes()[0]["qty"], 3);
        // Simulated process death: recover exclusively from disk.
        a.lot.as_mut().unwrap().unlatch();
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        assert!(!a.lot.as_ref().unwrap().busy());
        assert_eq!(
            f.p.iocs().iter().map(|r| r.qty).collect::<Vec<_>>(),
            vec![10, 7]
        );
        assert_eq!(
            f.closes()
                .iter()
                .map(|r| r["qty"].as_i64().unwrap())
                .sum::<i64>(),
            10
        );
    }

    #[tokio::test]
    async fn unconfirmed_hedge_is_not_retried_or_attributed_to_a_sibling() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 10);
        *f.p.ioc_pending.lock().unwrap() = true;
        f.place(&mut a, 1., 10, Shape::RestKalshi).await;
        f.k.fill(&id(&a), 10, true);
        f.manage(&mut a).await;
        f.manage(&mut a).await;
        assert_eq!(f.p.iocs().len(), 1);
        assert!(f.closes().is_empty());
        f.p.fill("order-0", 10, true);
        f.manage(&mut a).await;
        assert_eq!(f.p.iocs().len(), 1);
        assert_eq!(f.closes()[0]["qty"], 10);
    }

    #[tokio::test]
    async fn booking_replay_after_crash_is_idempotent() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 12);
        *f.p.ioc_pending.lock().unwrap() = true;
        f.place(&mut a, 1., 12, Shape::RestKalshi).await;
        f.k.fill(&id(&a), 12, true);
        f.manage(&mut a).await;
        let checkpoint_before_booking = a.lot.clone().unwrap();
        f.p.fill("order-0", 12, true);
        f.manage(&mut a).await;
        // Crash after the ledger append, before the checkpoint commits.
        checkpoint_before_booking.save(&f.path).unwrap();
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        assert_eq!(f.closes().len(), 1);
        assert_eq!(f.p.iocs().len(), 1);
        assert!(!a.lot.as_ref().unwrap().busy());
    }

    #[tokio::test]
    async fn lost_kalshi_ack_recovers_exact_order_but_pmus_never_adopts_a_sibling() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 12);
        *f.k.lose_ack.lock().unwrap() = true;
        f.place(&mut a, 1., 12, Shape::RestKalshi).await;
        f.manage(&mut a).await;
        assert_eq!(id(&a), "order-0");
        assert_eq!(f.k.orders.lock().unwrap().len(), 1);
        let mut b = f.owner(2., 12);
        *f.p.lose_ack.lock().unwrap() = true;
        f.place(&mut b, 2., 12, Shape::RestPmUs).await;
        let out = f.manage(&mut b).await.join("\n");
        assert!(out.contains("identity requires reconciliation"), "{out}");
        assert!(b
            .lot
            .as_ref()
            .unwrap()
            .active
            .as_ref()
            .unwrap()
            .rest
            .id
            .is_none());
        assert_eq!(f.p.orders.lock().unwrap().len(), 1);
        b.lot.as_mut().unwrap().unlatch();
    }

    #[tokio::test]
    async fn kalshi_absence_releases_a_503_place_only_after_the_list_could_lag() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 12);
        *f.k.drop_place.lock().unwrap() = true;
        f.place(&mut a, 1., 12, Shape::RestKalshi).await;
        let out = f.manage(&mut a).await.join("\n");
        assert!(out.contains("still unconfirmed"), "{out}");
        assert!(a.lot.as_ref().unwrap().latched);
        a.lot.as_mut().unwrap().active.as_mut().unwrap().rest.book_ts -= ABSENT_IS_PROOF_AFTER_S + 1.0;
        let out = f.manage(&mut a).await.join("\n");
        assert!(out.contains("never reached Kalshi"), "{out}");
        let lot = a.lot.as_ref().unwrap();
        assert!(!lot.busy() && !lot.latched);
        assert!(f.k.orders.lock().unwrap().is_empty());
        assert!(f.closes().is_empty());
    }

    #[tokio::test]
    async fn opposite_shapes_on_same_market_close_only_their_own_fills() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 11);
        let mut b = f.owner(2., 17);
        f.place(&mut a, 1., 11, Shape::RestKalshi).await;
        f.place(&mut b, 2., 17, Shape::RestPmUs).await;
        f.k.fill(&id(&a), 11, true);
        f.p.fill(&id(&b), 17, true);
        f.manage(&mut a).await;
        f.manage(&mut b).await;
        assert_eq!(f.closes().len(), 2);
        assert_eq!(f.p.iocs()[0].qty, 11);
        assert_eq!(f.k.iocs()[0].qty, 17);
    }

    #[test]
    fn parallel_order_ids_and_checkpoint_keys_do_not_collide() {
        let ids: BTreeSet<_> = (0..10000).map(|_| client_order_id()).collect();
        assert_eq!(ids.len(), 10000);
        assert_ne!(
            State::new(&cand(1, 1.)).key(),
            State::new(&cand(1, 2.)).key()
        );
    }
    #[tokio::test]
    async fn crash_after_last_receipt_checkpoint_does_not_send_a_zero_quantity_hedge() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 12);
        f.place(&mut a, 1., 12, Shape::RestKalshi).await;
        let state = a.lot.as_mut().unwrap();
        let active = state.active.as_mut().unwrap();
        active.filled = Some(12);
        active.hedged = 12;
        state.save(&f.path).unwrap();
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        assert!(f.p.iocs().is_empty());
        assert!(!a.lot.as_ref().unwrap().busy());
    }

    #[tokio::test]
    async fn startup_sweep_fill_is_reconciled_before_replacing_the_order() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 25);
        f.place(&mut a, 1., 25, Shape::RestKalshi).await;
        // The supervisor's startup sweep cancels an order that filled partly.
        f.k.fill(&id(&a), 6, true);
        a.lot = Some(load(&f.path).unwrap().pop().unwrap());
        f.manage(&mut a).await;
        assert_eq!(f.k.orders.lock().unwrap().len(), 1);
        assert_eq!(f.p.iocs()[0].qty, 6);
        assert_eq!(f.closes()[0]["qty"], 6);
        assert!(!a.lot.as_ref().unwrap().busy());
    }

    #[tokio::test]
    async fn dark_engine_pulls_orders_but_defers_hedges_until_prices_are_fresh() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 25);
        f.place(&mut a, 1., 25, Shape::RestKalshi).await;
        f.k.fill(&id(&a), 6, false);
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        manage(&mut a, None, &k, &p).await;
        assert!(f.k.orders.lock().unwrap().values().all(|o| o.terminal));
        assert!(f.p.iocs().is_empty());
        f.manage(&mut a).await;
        assert_eq!(f.closes()[0]["qty"], 6);
    }
    #[tokio::test]
    async fn crossed_exit_closes_immediately_using_the_cross_price_and_taker_fees() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 12);
        let mut order = resting_exit(12).order;
        order.cross = Some(Cross {
            limit: "0.3000".into(),
            lock_ct: "0.0800".into(),
        });
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        let out = place(&mut a, order, &view("0.20"), &k, &p).await;
        assert!(!a.lot.as_ref().unwrap().busy(), "{out:?}");
        assert_eq!(f.k.iocs().len(), 1);
        assert_eq!(f.p.iocs().len(), 1);
        let rows = f.closes();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["qty"], 12);
        assert!(rows[0]["legs"]
            .as_array()
            .unwrap()
            .iter()
            .all(|l| l["role"] == "taker"));
    }

    /// Members are sized within one shared budget, so the ladder has to back
    /// their SUM. Each member alone still fits when the depth falls to seven;
    /// the ten-lot group does not, and it is pulled with the reason logged.
    #[tokio::test]
    async fn a_group_is_pulled_when_the_ladder_no_longer_backs_its_total() {
        let _g = crate::naked_act::TEST_SERIAL.lock().await;
        let _v = test_serial();
        let f = Fixture::new();
        let mut a = f.owner(1., 3);
        let _b = f.owner(2., 7);
        let k: Sink = f.k.clone();
        let p: Sink = f.p.clone();
        let ladder = |size: &str| {
            let mut v = view("0.20");
            v.pm_ask_depth.insert("p-a".into(), vec![Level { price: "0.20".into(), size: size.into() }]);
            v
        };
        place_group(&mut a, vec![member(2., 7), member(1., 3)], &ladder("10"), &k, &p).await;
        assert_eq!(a.lot.as_ref().unwrap().passive_claim(), Some((
            close_depth_key(Direction::Standard, Shape::RestKalshi, "K-a", "p-a"), 10
        )));
        let out = manage(&mut a, Some(&ladder("10")), &k, &p).await;
        assert!(!out.iter().any(|l| l.contains("PULLING")), "ten back ten: {out:?}");
        assert!(a.lot.as_ref().unwrap().busy());
        let out = manage(&mut a, Some(&ladder("7")), &k, &p).await;
        let why = out.iter().find(|l| l.contains("PULLING")).unwrap_or_else(|| panic!("{out:?}"));
        assert!(why.contains("depth fell to 7"), "{why}");
        assert_eq!(a.lot.as_ref().unwrap().passive_claim(), None, "nothing rests, nothing is claimed");
    }

    /// The cancel log reports the order's age from its persisted id, not 0s.
    #[test]
    fn a_pulled_lot_order_reports_its_real_resting_age() {
        let age = placed_at(wall_now() - 300.0).elapsed().as_secs_f64();
        assert!((299.0..=301.0).contains(&age), "{age}");
        let future = placed_at(wall_now() + 60.0).elapsed().as_secs_f64();
        assert!(future < 1.0, "a clock-skewed id clamps to now: {future}");
    }

}
