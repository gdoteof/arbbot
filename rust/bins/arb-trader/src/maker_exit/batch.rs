//! Price each lot independently, then reserve identical passive quotes together.
use super::*;
use std::sync::Arc;
type Sink = Arc<dyn crate::sink::OrderSink>;

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

fn reservations(workers: &BTreeMap<String, Live>) -> BTreeSet<String> {
    let mut reserved = BTreeSet::new();
    for live in workers.values() {
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

pub(super) async fn run(template: Live, cfg: Cfg, k: Sink, p: Sink) {
    let registry = ExitOwners::default();
    let cache = Arc::new(Mutex::new(BTreeMap::new()));
    let mut workers = BTreeMap::new();
    if !template.shadow {
        let states = lot::load(&template.ledger_path).unwrap_or_else(|e| {
            eprintln!("[maker-exit] cannot recover durable exits: {e}");
            std::process::exit(19);
        });
        for state in states {
            workers.insert(state.key(), owner(&template, state, &registry, &cache));
        }
    }
    reservations(&workers);
    loop {
        cache.lock().unwrap().clear();
        // Reconcile known orders before admitting any new inventory.
        for live in workers.values_mut() {
            if live.lot.as_ref().unwrap().busy() {
                log(cycle(live, &cfg, &k, &p).await);
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
                    workers
                        .entry(state.key())
                        .or_insert_with(|| owner(&template, state, &registry, &cache));
                }
            }
        }
        let reserved = reservations(&workers);
        // One close-depth budget per ladder for the whole pass: resting groups
        // claim first, then each lot planned here claims what it was sized to.
        let mut claims: BTreeMap<String, i64> = BTreeMap::new();
        for live in workers.values() {
            if let Some((close, qty)) = live.lot.as_ref().unwrap().passive_claim() {
                *claims.entry(close).or_default() += qty;
            }
        }
        let mut plans = Vec::new();
        for (key, live) in &mut workers {
            live.planned = None;
            if live.lot.as_ref().unwrap().busy() {
                continue;
            }
            if reserved.contains(key) {
                live.request_suppress(BTreeSet::new());
                live.publish_working(BTreeSet::new());
                continue;
            }
            live.depth_claims = claims.clone();
            log(cycle(live, &cfg, &k, &p).await);
            live.depth_claims.clear();
            if let Some(order) = live.planned.take() {
                *claims.entry(close_depth_key(order.direction, order.shape, &order.market, &order.pm_market)).or_default() += order.qty;
                plans.push(order);
            }
        }
        for (group, members) in groups(plans) {
            // New arrivals join after confirmed cancellation and fresh repricing.
            // Never amend a quantity while its fills are uncertain.
            let incumbents: Vec<_> = workers
                .iter()
                .filter(|(_, l)| l.lot.as_ref().unwrap().passive_key().as_ref() == Some(&group))
                .map(|(key, _)| key.clone())
                .collect();
            if !incumbents.is_empty() {
                for key in incumbents {
                    let live = workers.get_mut(&key).unwrap();
                    live.lot.as_mut().unwrap().request_regroup();
                    log(cycle(live, &cfg, &k, &p).await);
                    pace().await;
                }
                continue;
            }
            if outstanding() != 0 {
                break;
            }
            let Ok(view) = engine_view() else {
                break;
            };
            let first = &members[0];
            let key = lot::lot_key(&first.rel_id, first.closes_ts.to_bits());
            let live = workers.get_mut(&key).unwrap();
            if members.iter().any(|o| {
                o.cross.is_none() && still_pays(&mut live.cx, &live.fees, o, &view).is_err()
            }) {
                continue;
            }
            log(lot::place_group(live, members, &view, &k, &p).await);
            pace().await;
        }
        if workers.is_empty() {
            publish_working(BTreeSet::new());
        }
        // All owners share this scheduler; refresh completed pass heartbeats.
        for live in workers.values() {
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
