//! Price each lot independently, then reserve identical passive quotes together.
//!
//! Two drivers share the lot owners. The PASS plans and places on its cadence
//! and is the backstop for every order it owns. The REACTOR runs on the
//! engine's book and fill events and touches only the lots an event names, so
//! a rest that stops paying is pulled, and a fill is hedged, on the event that
//! made it so — not behind a pass that walks every other lot first.
use super::*;
use std::sync::Arc;
type Sink = Arc<dyn crate::sink::OrderSink>;
type Worker = Arc<tokio::sync::Mutex<Live>>;

fn fatal(reason: &str) -> ! {
    eprintln!("[maker-exit] {reason}; stopping for recovery");
    std::process::exit(19)
}

fn owner(
    template: &Live,
    state: lot::State,
    registry: &ExitOwners,
    cache: &Arc<Mutex<BTreeMap<String, Quote>>>,
) -> Live {
    let mut live = Live::new(template.shadow, template.ledger_path.clone());
    live.take_ok = template.take_ok;
    live.scope_market = Some(state.market.clone());
    live.lot = Some(state);
    live.owners = Some(registry.clone());
    live.quote_cache = Some(cache.clone());
    live.plan_only = true;
    live.publish_working(live.working_set(None));
    if let Some(pm) = live.lot.as_ref().and_then(lot::State::pm_market) {
        if !live.claim_pm_market(pm) {
            fatal("conflicting recovered exit ownership");
        }
        live.request_suppress(
            cross_keys_for(live.scope_market.as_ref().unwrap(), pm, Direction::Standard)
                .into_iter()
                .chain(candidate_keys_for(live.scope_market.as_ref().unwrap(), pm, Direction::Standard))
                .collect(),
        );
    }
    live
}

type Watched = (Vec<String>, Vec<String>);

/// Every lot owner, and what each one's orders can be woken by.
///
/// No lock here is held across an `await`: callers take a snapshot of the
/// `Arc`s and lock one owner at a time, so the pass and the reactor can only
/// ever wait on the same single owner, never on each other.
#[derive(Default)]
struct Owners {
    workers: std::sync::Mutex<BTreeMap<String, Worker>>,
    /// Owner key -> (markets its orders rest or close on, their venue ids).
    watch: std::sync::Mutex<BTreeMap<String, Watched>>,
    /// Owner keys with a reactor task queued, and whether a fill of theirs is
    /// among the events it carries.
    queued: std::sync::Mutex<BTreeMap<String, bool>>,
}

impl Owners {
    fn snapshot(&self) -> Vec<(String, Worker)> {
        self.workers
            .lock()
            .unwrap()
            .iter()
            .map(|(k, w)| (k.clone(), w.clone()))
            .collect()
    }
    fn get(&self, key: &str) -> Option<Worker> {
        self.workers.lock().unwrap().get(key).cloned()
    }
    fn is_empty(&self) -> bool {
        self.workers.lock().unwrap().is_empty()
    }
    fn insert_with(&self, key: String, make: impl FnOnce() -> Live) {
        self.workers
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(make())));
    }
    /// Record what `live`'s orders can be woken by, after anything changed it.
    /// A lot that has just become busy is checked once straight away: the book
    /// can move during the place round trip, and no later event need come.
    fn note(&self, key: &str, live: &Live) {
        let now = live.lot.as_ref().and_then(lot::State::watch);
        let (fresh, markets) = {
            let mut g = self.watch.lock().unwrap();
            let fresh = match &now {
                Some(w) => g.insert(key.to_owned(), w.clone()).is_none(),
                None => {
                    g.remove(key);
                    false
                }
            };
            let markets = g.values().flat_map(|(ms, _)| ms.iter().cloned()).collect();
            (fresh, markets)
        };
        react::set_watched(markets);
        if fresh {
            if let Some((ms, _)) = now {
                for m in ms {
                    react::mark(&m, None);
                }
            }
        }
    }
}

async fn reservations(owners: &Owners) -> BTreeSet<String> {
    let mut reserved = BTreeSet::new();
    for (_, w) in owners.snapshot() {
        let live = w.lock().await;
        for key in live.lot.as_ref().unwrap().reserved_keys() {
            if !reserved.insert(key) {
                fatal("duplicate durable exit lot reservation");
            }
        }
    }
    reserved
}

fn groups(plans: Vec<Order>) -> BTreeMap<String, Vec<Order>> {
    let mut result: BTreeMap<String, Vec<Order>> = BTreeMap::new();
    for plan in plans {
        result.entry(lot::group_key(&plan)).or_default().push(plan);
    }
    for members in result.values_mut() {
        members.sort_by(|a, b| {
            a.closes_ts
                .total_cmp(&b.closes_ts)
                .then(a.rel_id.cmp(&b.rel_id))
        });
    }
    result
}
fn log(lines: Vec<String>) {
    for line in lines {
        eprintln!("{line}");
    }
}
async fn pace() {
    tokio::time::sleep(Duration::from_millis(500)).await;
}

/// How long a fill id that names no owner is kept for, in case the owner
/// that placed it has not yet been re-indexed.
const UNMATCHED_FILL_RETRIES: u8 = 5;
const UNMATCHED_FILL_RETRY_MS: u64 = 200;

/// Wake exactly the owners each event names. Book events cost a pure check;
/// venue calls happen only inside [`lot::react`] when one is warranted.
async fn reactor(owners: Arc<Owners>, k: Sink, p: Sink) {
    let mut unmatched: BTreeMap<String, u8> = BTreeMap::new();
    loop {
        let dirty = react::dirty().await;
        let watch = owners.watch.lock().unwrap().clone();
        let mut matched = BTreeSet::new();
        for (key, (markets, ids)) in &watch {
            let mut hit = false;
            let mut fill = false;
            for m in markets {
                if let Some(filled) = dirty.get(m) {
                    hit = true;
                    for id in filled.iter().filter(|id| ids.contains(id)) {
                        fill = true;
                        matched.insert(id.clone());
                    }
                }
            }
            if hit {
                schedule(&owners, key, fill, &k, &p);
            }
        }
        // A fill can land between a place returning its id and the owner being
        // re-indexed. Hold an id nobody claims for a moment and ask again.
        for (market, ids) in &dirty {
            for id in ids.iter().filter(|id| !matched.contains(*id)) {
                let n = unmatched.entry(id.clone()).or_insert(0);
                if *n < UNMATCHED_FILL_RETRIES {
                    *n += 1;
                    let (m, id) = (market.clone(), id.clone());
                    tokio::spawn(async move {
                        tokio::time::sleep(Duration::from_millis(UNMATCHED_FILL_RETRY_MS)).await;
                        react::mark(&m, Some(&id));
                    });
                }
            }
        }
        // Exhausted ids stay, so their last retry cannot restart the count.
        // They are fills of the other sidecar (`--positions-recon-act`): a few
        // a day.
        unmatched.retain(|id, _| !matched.contains(id));
    }
}

fn schedule(owners: &Arc<Owners>, key: &str, fill: bool, k: &Sink, p: &Sink) {
    {
        let mut q = owners.queued.lock().unwrap();
        if let Some(f) = q.get_mut(key) {
            *f |= fill;
            return;
        }
        q.insert(key.to_owned(), fill);
    }
    let Some(w) = owners.get(key) else {
        owners.queued.lock().unwrap().remove(key);
        return;
    };
    let (owners, key, k, p) = (owners.clone(), key.to_owned(), k.clone(), p.clone());
    tokio::spawn(async move {
        let mut live = w.lock().await;
        // Dequeued only once the owner is ours: an event that arrives while
        // this waits rides along; one that arrives after queues a fresh look.
        let fill = owners.queued.lock().unwrap().remove(&key).unwrap_or(false);
        log(lot::react(&mut live, fill, &k, &p).await);
        owners.note(&key, &live);
    });
}

pub(super) async fn run(template: Live, cfg: Cfg, k: Sink, p: Sink) {
    let registry = ExitOwners::default();
    let cache = Arc::new(Mutex::new(BTreeMap::new()));
    let owners = Arc::new(Owners::default());
    if !template.shadow {
        let states = lot::load(&template.ledger_path).unwrap_or_else(|e| {
            eprintln!("[maker-exit] cannot recover durable exits: {e}");
            std::process::exit(19);
        });
        for state in states {
            owners.insert_with(state.key(), || owner(&template, state, &registry, &cache));
        }
    }
    reservations(&owners).await;
    for (key, w) in owners.snapshot() {
        owners.note(&key, &*w.lock().await);
    }
    tokio::spawn(reactor(owners.clone(), k.clone(), p.clone()));
    loop {
        react::load_blackouts(&cfg.blackouts_path);
        cache.lock().unwrap().clear();
        // Reconcile known orders before admitting any new inventory.
        for (key, w) in owners.snapshot() {
            let mut live = w.lock().await;
            if live.lot.as_ref().unwrap().busy() {
                log(cycle(&mut live, &cfg, &k, &p).await);
                owners.note(&key, &live);
                drop(live);
                pace().await;
            } else {
                live.publish_working(live.working_set(None));
            }
        }
        if let Ok(view) = engine_view() {
            let marks = std::fs::read_to_string(&cfg.marks_path).unwrap_or_default();
            if let Ok((exits, _)) = crate::unwind::select_passive(
                &marks,
                view.apr_bar,
                view.global_cap_usd,
                &cfg.rel_prefixes,
                wall_now(),
            ) {
                for exit in exits {
                    let state = lot::State::new(&exit);
                    owners.insert_with(state.key(), || owner(&template, state, &registry, &cache));
                }
            }
        }
        let reserved = reservations(&owners).await;
        // One close-depth budget per ladder for the whole pass: resting groups
        // claim first, then each lot planned here claims what it was sized to.
        let mut claims: BTreeMap<String, i64> = BTreeMap::new();
        for (_, w) in owners.snapshot() {
            if let Some((close, qty)) = w.lock().await.lot.as_ref().unwrap().passive_claim() {
                *claims.entry(close).or_default() += qty;
            }
        }
        let mut plans = Vec::new();
        for (key, w) in owners.snapshot() {
            let mut live = w.lock().await;
            live.planned = None;
            if live.lot.as_ref().unwrap().busy() {
                continue;
            }
            if reserved.contains(&key) {
                live.request_suppress(BTreeSet::new());
                live.publish_working(BTreeSet::new());
                continue;
            }
            // Nothing new rests into a scheduled announcement; what already
            // rests there is pulled by its keep-check.
            if react::blackout(&live.lot.as_ref().unwrap().rel_id, wall_now()).is_some() {
                live.request_suppress(BTreeSet::new());
                live.publish_working(BTreeSet::new());
                continue;
            }
            live.depth_claims = claims.clone();
            log(cycle(&mut live, &cfg, &k, &p).await);
            live.depth_claims.clear();
            owners.note(&key, &live);
            if let Some(order) = live.planned.take() {
                *claims.entry(close_depth_key(order.direction, order.shape, &order.market, &order.pm_market)).or_default() += order.qty;
                plans.push(order);
            }
        }
        for (group, members) in groups(plans) {
            // New arrivals join after confirmed cancellation and fresh repricing.
            // Never amend a quantity while its fills are uncertain.
            let mut incumbents = Vec::new();
            for (key, w) in owners.snapshot() {
                if w.lock().await.lot.as_ref().unwrap().passive_key().as_ref() == Some(&group) {
                    incumbents.push((key, w));
                }
            }
            if !incumbents.is_empty() {
                for (key, w) in incumbents {
                    let mut live = w.lock().await;
                    live.lot.as_mut().unwrap().request_regroup();
                    log(cycle(&mut live, &cfg, &k, &p).await);
                    owners.note(&key, &live);
                    drop(live);
                    pace().await;
                }
                continue;
            }
            let first = &members[0];
            if outstanding_for(Some(&first.market)) != 0 {
                continue;
            }
            let Ok(view) = engine_view() else {
                break;
            };
            let key = lot::lot_key(&first.rel_id, first.closes_ts.to_bits());
            let w = owners.get(&key).unwrap();
            let mut live = w.lock().await;
            // The reactor may have changed this owner since it was planned.
            if live.lot.as_ref().unwrap().busy() {
                continue;
            }
            let l = &mut *live;
            if members.iter().any(|o| {
                o.cross.is_none() && still_pays(&mut l.cx, &l.fees, o, &view).is_err()
            }) {
                continue;
            }
            log(lot::place_group(&mut live, members, &view, &k, &p).await);
            owners.note(&key, &live);
            drop(live);
            pace().await;
        }
        if owners.is_empty() {
            publish_working(BTreeSet::new());
        }
        // All owners share this scheduler; refresh completed pass heartbeats.
        for (_, w) in owners.snapshot() {
            let live = w.lock().await;
            live.publish_working(live.working_set(None));
        }
        tokio::time::sleep(CYCLE).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::maker_exit::tests::resting_exit;
    #[test]
    fn production_groups_equal_quotes_in_fifo_order_but_preserves_other_prices() {
        let mut old = resting_exit(3).order;
        old.closes_ts = 1.;
        let mut new = old.clone();
        new.closes_ts = 2.;
        new.qty = 7;
        let mut other = old.clone();
        other.closes_ts = 3.;
        other.limit = "0.9900".into();
        let grouped = groups(vec![new, other, old.clone()]);
        assert_eq!(grouped.len(), 2);
        let members = &grouped[&lot::group_key(&old)];
        assert_eq!(
            members.iter().map(|o| o.closes_ts).collect::<Vec<_>>(),
            vec![1., 2.]
        );
        assert_eq!(members.iter().map(|o| o.qty).sum::<i64>(), 10);
    }
}
