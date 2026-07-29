//! Top N baskets by edge, priced off the current book.
//!
//! Same four steps as the intents view: read the URL, load the rollup, price
//! each pair, rank and summarise.

use arb_core::fees::FeeSchedule;
use arb_core::model::Venue;
use arb_core::scan::{Cx, D};
use arb_registry::{Allowlist, Registry, Relationship};
use arb_scenario::{price, Priced, Quote, Scenario};
use arb_tob::series;

use crate::endpoints::{
    coverage_by_venue, coverage_json, rollup_paths, stalest_age_s, venue_age_s, Coverage, Latest,
    MAX_COVERAGE_AGE_S,
};
use crate::http::query_param;
use crate::{integrity, Args, FEE_CATEGORY};

/// The knobs this view takes off the URL, with their defaults.
struct Query {
    scenario: Scenario,
    clip_s: String,
    n: usize,
    day: String,
    only_tradable: bool,
    max_spread_s: String,
}

impl Query {
    fn parse(a: &Args, query: &str) -> Query {
        Query {
            scenario: query_param(query, "scenario")
                .and_then(|s| Scenario::parse(&s))
                .unwrap_or(Scenario::TakeTake),
            clip_s: query_param(query, "clip").unwrap_or_else(|| "25".into()),
            n: query_param(query, "n").and_then(|s| s.parse().ok()).unwrap_or(25),
            day: query_param(query, "day")
                .unwrap_or_else(|| integrity::build(&a.data_dir).today),
            // Default to the permitted universe. Detected-but-not-permitted edge is
            // genuinely worth seeing (it is the case for vetting a pair), but it should
            // be asked for, not ranked first by default.
            only_tradable: query_param(query, "all").as_deref() != Some("1"),
            max_spread_s: query_param(query, "max_spread").unwrap_or_else(|| "0.05".into()),
        }
    }
}

/// The arithmetic context and the sizes every row is priced at, fixed for the
/// whole request.
struct Pricing {
    cx: Cx,
    sched: FeeSchedule,
    clip: D,
    max_spread: D,
}

impl Pricing {
    fn new(q: &Query) -> Pricing {
        let mut cx = Cx::default();
        let sched = FeeSchedule::new(&mut cx);
        let clip = cx.parse_exact(&q.clip_s);
        let max_spread = cx.parse_exact(&q.max_spread_s);
        Pricing { cx, sched, clip, max_spread }
    }

    /// Every scenario is priced for every pair, so switching the view is a
    /// re-sort rather than a re-read, and a pair that is unprofitable to take
    /// but profitable to make is visible as such.
    ///
    /// `None` when the scenario the caller asked for does not price at all.
    fn all_scenarios(
        &mut self,
        want: Scenario,
        va: Venue,
        vb: Venue,
        qa: &Quote,
        qb: &Quote,
    ) -> Option<(Priced, serde_json::Map<String, serde_json::Value>)> {
        let mut by_scenario = serde_json::Map::new();
        let mut chosen: Option<Priced> = None;
        for sc in Scenario::all() {
            if let Some(p) = price(
                &mut self.cx, &self.sched, sc, va, vb, qa, qb, self.clip, FEE_CATEGORY,
                self.max_spread,
            ) {
                if sc == want {
                    chosen = Some(p.clone());
                }
                by_scenario.insert(sc.as_str().into(), serde_json::to_value(&p).unwrap());
            }
        }
        Some((chosen?, by_scenario))
    }
}

/// What one registry pair produced on this board.
enum Row {
    /// A ranked row.
    Ranked(serde_json::Value),
    /// Permitted, two priceable legs — and the rollup carried no book for a
    /// leg on EACH of these venues, so the pair was never evaluated at all.
    /// Counted rather than silently dropped: 32 of 43 tradable pairs have a
    /// polymarket_us leg, and a board that is quietly 32 pairs short reads to
    /// an operator as "no edge today" and stands them down.
    ///
    /// Both legs when both are missing. Naming only leg A points the operator
    /// at kalshi — leg A on 38 of the 40 tradable pairs — which is the venue
    /// the chips beside the banner have just certified green, and telling
    /// someone to rebuild the venue that is working is worse than saying
    /// nothing.
    NoBook(Vec<String>),
    /// Not this board's question: not two legs, an unknown venue, not
    /// permitted when only permitted pairs were asked for, or unpriceable
    /// under the chosen scenario.
    Skip,
}

/// One row of the board, priced off the two legs' books and gated on the
/// coverage of the two VENUES those books came from.
fn row(
    p: &mut Pricing,
    q: &Query,
    r: &Relationship,
    allow: &Allowlist,
    latest: &Latest,
    cov: &Coverage,
    now_ns: i64,
) -> Row {
    if r.legs.len() != 2 {
        return Row::Skip;
    }
    let (la, lb) = (&r.legs[0], &r.legs[1]);
    let (Some(va), Some(vb)) = (Venue::parse(&la.venue), Venue::parse(&lb.venue)) else {
        return Row::Skip;
    };
    let tradable = r.tradable(allow);
    // Ahead of the book lookup, not after the pricing as it used to be, so a
    // pair counted as un-evaluated is counted in the SAME universe the board
    // is showing.
    if q.only_tradable && !tradable {
        return Row::Skip;
    }
    let ka = (la.venue.clone(), la.market_id.clone());
    let kb = (lb.venue.clone(), lb.market_id.clone());
    let (Some(sa), Some(sb)) = (latest.get(&ka), latest.get(&kb)) else {
        return Row::NoBook(
            [(&ka, &la.venue), (&kb, &lb.venue)]
                .iter()
                .filter(|(k, _)| !latest.contains_key(*k))
                .map(|(_, v)| (*v).clone())
                .collect(),
        );
    };
    let qa = Quote {
        bid: sa.bid.clone(), bid_size: sa.bid_size.clone(),
        ask: sa.ask.clone(), ask_size: sa.ask_size.clone(),
    };
    let qb = Quote {
        bid: sb.bid.clone(), bid_size: sb.bid_size.clone(),
        ask: sb.ask.clone(), ask_size: sb.ask_size.clone(),
    };

    let Some((priced, by_scenario)) = p.all_scenarios(q.scenario, va, vb, &qa, &qb) else {
        return Row::Skip;
    };
    // Actionable is a claim about NOW, and this row's "now" is the older of the
    // two VENUES it is priced off — a live kalshi feed says nothing about a
    // polymarket_us book that stopped six hours ago. Without any coverage term
    // the board reported "10 actionable" off 22-hour-old quotes; with one that
    // took the newest sample anywhere it reported them off a dead venue.
    let coverage_age_s = venue_age_s(cov, &la.venue).max(venue_age_s(cov, &lb.venue));
    let actionable =
        coverage_age_s <= MAX_COVERAGE_AGE_S && priced.profitable && priced.fill_plausible;
    Row::Ranked(serde_json::json!({
        "relationship_id": r.id,
        "tradable": tradable,
        "verdict": r.verdict,
        "intl_leg": la.venue == "polymarket" || lb.venue == "polymarket",
        "leg_a": format!("{}:{}", la.venue, la.market_id),
        "leg_b": format!("{}:{}", lb.venue, lb.market_id),
        "quote_age_a_s": (now_ns - sa.ts_local_ns) / 1_000_000_000,
        "quote_age_b_s": (now_ns - sb.ts_local_ns) / 1_000_000_000,
        // Raw, unlike the intents view, which maps `i64::MAX` to -1 here. It
        // cannot arise on this row: reaching it required BOTH legs' markets in
        // `latest`, and both are registry legs, so each of the two venues has a
        // registry-backed sample and neither can be the absent case. The page
        // reads -1 as "no coverage" on both views, so if that invariant ever
        // breaks this line has to map too.
        "coverage_age_s": coverage_age_s,
        "actionable": actionable,
        "edge_per_contract": priced.edge_per_contract,
        "priced": priced,
        "scenarios": by_scenario,
    }))
}

/// Actionable first, then plausible fills, THEN by edge. Sorting on raw edge
/// alone puts un-fillable wide-spread rests at the top, which is exactly
/// backwards for a view whose job is to say what to trade — and leaving the
/// coverage term out of the sort while the COUNT applies it puts rows priced
/// off a dead venue above the ones an operator can actually trade.
fn rank(rows: &mut [serde_json::Value]) {
    rows.sort_by(|x, y| {
        let act = |v: &serde_json::Value| v["actionable"].as_bool().unwrap_or(false);
        let ok = |v: &serde_json::Value| v["priced"]["fill_plausible"].as_bool().unwrap_or(false);
        let f = |v: &serde_json::Value| {
            v["edge_per_contract"].as_str().and_then(|s| s.parse::<f64>().ok()).unwrap_or(f64::MIN)
        };
        act(y)
            .cmp(&act(x))
            .then(ok(y).cmp(&ok(x)))
            .then(f(y).partial_cmp(&f(x)).unwrap_or(std::cmp::Ordering::Equal))
    });
}

/// Top N baskets by edge under a chosen execution style.
///
/// Ranked on CURRENT quotes — the latest sample in the ToB rollup — so every
/// row carries its quote age. This is as fresh as the rollup, not as fresh as
/// the venue, and saying so is the difference between an instrument and a lie.
pub fn json(a: &Args, query: &str) -> String {
    let q = Query::parse(a, query);

    let reg = match Registry::load(&a.registry) {
        Ok(r) => r,
        Err(e) => return format!("{{\"error\":\"{}\"}}", e.replace('"', "'")),
    };
    let allow = Allowlist::load(&a.tradable);

    let paths = rollup_paths(&a.rollup_dir, &q.day);
    if paths.is_empty() {
        return format!(
            "{{\"error\":\"no ToB rollup for {} — run arb-tob --day {}\"}}", q.day, q.day
        );
    }
    let latest = series::latest_by_market(&paths);
    let now_ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as i64)
        .unwrap_or(0);
    // How far the rollup COVERS, per venue — the same gate the intents view
    // applies, and for the same reason. Ranking today's board off a rollup
    // built this morning is ranking this morning's board. Per-market sample age
    // cannot stand in for it: the series emits on change, so a quiet market's
    // old sample is still the current book.
    let cov = coverage_by_venue(&latest, &reg, now_ns);
    let coverage_age_s = stalest_age_s(&cov);
    let rollup_current = coverage_age_s <= MAX_COVERAGE_AGE_S;

    let mut pricing = Pricing::new(&q);
    let mut rows: Vec<serde_json::Value> = Vec::new();
    // The other half of the same lie. A venue with no rollup at all drops out
    // of `rollup_paths`, every pair with a leg there fails the book lookup, and
    // the board comes back SMALLER with no field saying so — "0 actionable"
    // that means "never evaluated" reads exactly like "no edge".
    let mut no_book: std::collections::BTreeMap<String, usize> = Default::default();
    let mut unevaluated = 0usize;
    for r in &reg.relationships {
        match row(&mut pricing, &q, r, &allow, &latest, &cov, now_ns) {
            Row::Ranked(v) => rows.push(v),
            Row::NoBook(venues) => {
                // PAIRS in the headline, LEGS in the attribution: a pair
                // missing both books is one pair and two broken venues, so the
                // per-venue counts can sum higher than the total and the banner
                // says so rather than pretending they partition.
                unevaluated += 1;
                for v in venues {
                    *no_book.entry(v).or_default() += 1;
                }
            }
            Row::Skip => {}
        }
    }

    rank(&mut rows);
    let total = rows.len();
    let profitable = rows
        .iter()
        .filter(|v| v["priced"]["profitable"].as_bool().unwrap_or(false))
        .count();
    let actionable =
        rows.iter().filter(|v| v["actionable"].as_bool().unwrap_or(false)).count();
    rows.truncate(q.n);

    serde_json::json!({
        "day": q.day,
        "scenario": q.scenario.as_str(),
        "clip": q.clip_s,
        "priced_pairs": total,
        "profitable": profitable,
        "actionable": actionable,
        "unevaluated_pairs": unevaluated,
        "unevaluated_by_venue": no_book,
        "rollup_current": rollup_current,
        "rollup_coverage_age_s": if coverage_age_s == i64::MAX { -1 } else { coverage_age_s },
        "venues": coverage_json(&cov, &latest),
        "max_coverage_age_s": MAX_COVERAGE_AGE_S,
        "max_spread": q.max_spread_s,
        "fee_category": FEE_CATEGORY,
        "only_tradable": q.only_tradable,
        "shown": rows.len(),
        "rows": rows,
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::{json, Query};
    use crate::Args;
    use arb_scenario::Scenario;

    fn parse(query: &str) -> Query {
        Query::parse(&Args::for_test(), query)
    }

    const DAY: &str = "2026-07-27";

    /// One permitted cross-venue pair. `verdict: equivalent` + `vetted_by:
    /// human` is the registry half of the tradable gate, so no allowlist file
    /// is needed and `Args::for_test`'s nonexistent one stays nonexistent.
    ///
    /// The second is REJECTED and has no book on either leg, so it is the
    /// pair that would land in `unevaluated_pairs` if the permitted-universe
    /// filter had stayed BEHIND the book lookup. Every board test below
    /// therefore also pins that reorder.
    const REGISTRY: &str = "\
relationships:
  - id: xvus-fixture
    verdict: equivalent
    vetted_by: human
    legs:
      - venue: kalshi
        market_id: K1
      - venue: polymarket_us
        market_id: P1
  - id: xvus-rejected
    verdict: rejected
    vetted_by: human
    legs:
      - venue: kalshi
        market_id: K2
      - venue: polymarket_us
        market_id: P2
";

    fn now_ns() -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0)
    }

    /// A ToB line for one market, aged relative to the wall clock the endpoint
    /// itself reads. Sizes are 100 against a clip of 25 so depth never gates,
    /// and 0.30 against 0.75 is a deep arb: the only thing under test is
    /// freshness.
    fn sample(venue: &str, market: &str, age_s: i64, bid: &str, ask: &str) -> String {
        serde_json::json!({
            "venue": venue,
            "market_id": market,
            "ts_local_ns": now_ns() - age_s * 1_000_000_000,
            "bid": bid, "bid_size": "100",
            "ask": ask, "ask_size": "100",
        })
        .to_string()
    }

    fn args(name: &str) -> (Args, std::path::PathBuf) {
        let base =
            std::env::temp_dir().join(format!("arb-dash-current-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("rollup")).unwrap();
        std::fs::write(base.join("registry.yaml"), REGISTRY).unwrap();
        let mut a = Args::for_test();
        a.rollup_dir = base.join("rollup").to_string_lossy().to_string();
        a.registry = base.join("registry.yaml").to_string_lossy().to_string();
        (a, base)
    }

    /// A venue the rollup covers. Not calling this for a venue is how a venue
    /// with NO rollup is spelled.
    fn rolled_up(base: &std::path::Path, venue: &str, lines: &[String]) {
        std::fs::write(base.join(format!("rollup/tob-{venue}-{DAY}.jsonl")), lines.join("\n"))
            .unwrap();
    }

    fn board(a: &Args) -> serde_json::Value {
        let raw = json(a, &format!("day={DAY}"));
        serde_json::from_str(&raw).unwrap_or_else(|e| panic!("not json ({e}): {raw}"))
    }

    fn venue(d: &serde_json::Value, name: &str) -> serde_json::Value {
        d["venues"]
            .as_array()
            .expect("a venues array")
            .iter()
            .find(|v| v["venue"] == name)
            .unwrap_or_else(|| panic!("no {name} entry in {d}"))
            .clone()
    }

    /// The live defect. The polymarket_us recorder dies at 09:00; its rollup
    /// still exists and still ends at 09:00, while kalshi's runs to the rebuild
    /// at 15:00. Coverage taken as the newest sample ANYWHERE reads seconds, so
    /// the board painted ROLLUP CURRENT and counted this pair actionable off a
    /// six-hour-dead book — the exact headline the coverage gate was built to
    /// stop, one venue too coarse.
    #[test]
    fn a_venue_that_stopped_covering_cannot_borrow_another_venues_freshness() {
        let (a, base) = args("stale-venue");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(&base, "polymarket_us", &[sample("polymarket_us", "P1", 6 * 3600, "0.75", "0.76")]);
        let d = board(&a);
        assert_eq!(d["priced_pairs"], 1, "the book is old, not absent — it still prices: {d}");
        assert_eq!(d["profitable"], 1, "and it is still profitable at those prices: {d}");
        assert_eq!(d["actionable"], 0, "priced off a six-hour-dead polymarket_us book: {d}");
        assert_eq!(d["rows"][0]["actionable"], false, "and the row says so too: {d}");
        assert_eq!(d["rows"][0]["coverage_age_s"], 6 * 3600, "the stalest of the two legs: {d}");
        assert_eq!(d["rollup_current"], false, "one dead venue is not a current rollup: {d}");
        assert_eq!(venue(&d, "kalshi")["current"], true, "kalshi is genuinely still live: {d}");
        assert_eq!(venue(&d, "polymarket_us")["current"], false, "{d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The same defect, on the population that actually makes it reachable.
    /// Most of a venue's tape is markets no pair prices — the pm-us universe
    /// comes from `polymarket_us_tags`, 749 markets against 88 registry legs on
    /// the real 07-28 tape — so coverage taken over every market read one
    /// second off a Billboard market and called this row actionable while the
    /// only book it is priced off had stopped six hours earlier.
    ///
    /// P1 is the row's own leg and it is the venue's whole registry coverage
    /// here: this is the TOTAL form, every counted leg stopped. A single frozen
    /// market is not what this catches, on this venue or any other — see
    /// `coverage_by_venue`'s residual.
    #[test]
    fn a_market_we_do_not_price_does_not_certify_the_one_this_row_needs() {
        let (a, base) = args("unpriced-noise");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(
            &base,
            "polymarket_us",
            &[
                sample("polymarket_us", "P1", 6 * 3600, "0.75", "0.76"),
                sample("polymarket_us", "ccpc-bilbrd-1album-any2026-eminem", 1, "0.50", "0.51"),
                sample("polymarket_us", "ccpc-bilbrd-1album-any2026-ladgag", 2, "0.44", "0.45"),
            ],
        );
        let d = board(&a);
        assert_eq!(d["priced_pairs"], 1, "the arb book is old, not absent — it still prices: {d}");
        assert_eq!(d["profitable"], 1, "and still profitable at those prices: {d}");
        assert_eq!(d["actionable"], 0, "a Billboard tick is not evidence about P1: {d}");
        assert_eq!(d["rows"][0]["coverage_age_s"], 6 * 3600, "{d}");
        assert_eq!(venue(&d, "polymarket_us")["coverage_age_s"], 6 * 3600, "{d}");
        assert_eq!(venue(&d, "polymarket_us")["current"], false, "{d}");
        assert_eq!(venue(&d, "polymarket_us")["recorded"], true, "the tape is being written: {d}");
        assert_eq!(d["rollup_current"], false, "{d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// And the state the operator has to be able to tell apart from the one
    /// below: the venue's rollup is being written, and not one market the
    /// registry names is in it. Coverage is ABSENT, exactly as for a missing
    /// file — but `recorded` is TRUE, because the repair is the recorder's
    /// subscription and not the rollup, and a board that says "rebuild the
    /// rollup" here sends the operator to replay the same markets again.
    #[test]
    fn a_rollup_of_markets_we_never_price_is_a_subscription_hole_not_a_rollup_one() {
        let (a, base) = args("unpriced-only");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(
            &base,
            "polymarket_us",
            &[
                sample("polymarket_us", "ccpc-bilbrd-1album-any2026-eminem", 1, "0.50", "0.51"),
                sample("polymarket_us", "ccpc-bilbrd-1album-any2026-ladgag", 2, "0.44", "0.45"),
            ],
        );
        let d = board(&a);
        assert_eq!(venue(&d, "polymarket_us")["coverage_age_s"], -1, "unpriced is not covered: {d}");
        assert_eq!(venue(&d, "polymarket_us")["current"], false, "{d}");
        assert_eq!(venue(&d, "polymarket_us")["recorded"], true, "the tape IS there: {d}");
        assert_eq!(venue(&d, "polymarket")["recorded"], false, "and this one is not: {d}");
        assert_eq!(d["priced_pairs"], 0, "no book for P1, so nothing prices: {d}");
        assert_eq!(d["unevaluated_pairs"], 1, "counted, not silently dropped: {d}");
        assert_eq!(d["unevaluated_by_venue"]["polymarket_us"], 1, "{d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// And the regression this must not become. Per-MARKET age cannot be the
    /// gate: the series emits on change and 33 of the 40 priceable tradable
    /// pairs in the real 07-28 tape have a leg more than six hours behind, so
    /// gating on it would empty the board. Here P1 — the priced row's own leg —
    /// last moved 19 hours ago, while P2 (also a registry leg) ticked 50s ago
    /// and an unpriced catalog market ticks constantly. The venue is COVERED,
    /// the row is ACTIONABLE, and its coverage reads the venue's, not the
    /// market's.
    ///
    /// P2 is the REJECTED pair's leg, and it certifies the venue anyway. The
    /// basis is ALL registry legs because coverage is a claim about what the
    /// recorder is delivering and a verdict is a claim about a pair, so
    /// narrowing it to the tradable ones would let a vetting decision change
    /// what a venue's freshness reads — and does, measurably: six tradable
    /// polymarket legs put that venue at 10,832-62,617s on three real tapes
    /// with the feed alive.
    #[test]
    fn a_quiet_arb_leg_on_a_covered_venue_still_makes_an_actionable_row() {
        let (a, base) = args("quiet-leg");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(
            &base,
            "polymarket_us",
            &[
                sample("polymarket_us", "P1", 19 * 3600, "0.75", "0.76"),
                sample("polymarket_us", "P2", 50, "0.60", "0.61"),
                sample("polymarket_us", "ccpc-bilbrd-1album-any2026-eminem", 1, "0.50", "0.51"),
            ],
        );
        let d = board(&a);
        assert_eq!(d["rollup_current"], true, "a quiet market is not a dead venue: {d}");
        assert_eq!(venue(&d, "polymarket_us")["coverage_age_s"], 50, "P2's tick covers it: {d}");
        assert_eq!(d["actionable"], 1, "the board must not empty: {d}");
        assert_eq!(d["rows"][0]["actionable"], true, "{d}");
        assert_eq!(d["rows"][0]["quote_age_b_s"], 19 * 3600, "the leg really is that quiet: {d}");
        assert_eq!(d["rows"][0]["coverage_age_s"], 50, "and that is not what gates it: {d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The mirror, and the one that turns a false positive into a silent false
    /// NEGATIVE. With no polymarket_us rollup the pair fails its book lookup
    /// and vanishes: the board comes back smaller, every count honest about the
    /// rows it kept and silent about the ones it never evaluated. "0
    /// actionable" then reads as "no edge today" and the operator stands down.
    #[test]
    fn a_venue_with_no_rollup_is_a_counted_hole_not_a_quietly_smaller_board() {
        let (a, base) = args("no-rollup");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        let d = board(&a);
        assert_eq!(d["priced_pairs"], 0, "no polymarket_us book, so nothing to price: {d}");
        assert_eq!(d["actionable"], 0);
        assert_eq!(d["unevaluated_pairs"], 1, "the pair left the board unannounced: {d}");
        assert_eq!(d["unevaluated_by_venue"]["polymarket_us"], 1, "and unattributed: {d}");
        assert_eq!(venue(&d, "polymarket_us")["coverage_age_s"], -1, "no rollup, not fresh: {d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The other direction, which matters just as much: a fix that warns on
    /// every board is the same defect pointing the other way. A rollup covering
    /// both venues is CURRENT and its profitable, fillable row is ACTIONABLE.
    #[test]
    fn a_rollup_covering_every_venue_still_reports_current_and_still_counts_rows() {
        let (a, base) = args("healthy");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(&base, "polymarket_us", &[sample("polymarket_us", "P1", 20, "0.75", "0.76")]);
        let d = board(&a);
        assert_eq!(d["rollup_current"], true, "{d}");
        assert_eq!(d["actionable"], 1, "{d}");
        assert_eq!(d["rows"][0]["actionable"], true, "{d}");
        assert_eq!(d["unevaluated_pairs"], 0, "nothing was dropped, so nothing to warn about: {d}");
        assert_eq!(venue(&d, "kalshi")["current"], true, "{d}");
        assert_eq!(venue(&d, "polymarket_us")["current"], true, "{d}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The permitted-universe filter moved AHEAD of the book lookup so the
    /// un-evaluated count is about the same pairs the board is showing. That
    /// reorder decides what a permitted pair IS, so it is pinned end to end
    /// rather than by inspection.
    ///
    /// `xvus-rejected` has no book on either leg. On the permitted board it
    /// must reach NEITHER output — a pair we are forbidden to trade is not a
    /// hole in the board, and counting it would have the banner tell an
    /// operator to go rebuild a rollup for a pair that would still be
    /// untradable afterwards. Ask for the whole universe and it IS evaluated,
    /// still unpriced, and now correctly counted — against BOTH its venues,
    /// because both books are the ones that are missing.
    #[test]
    fn a_pair_we_may_not_trade_is_not_a_hole_in_the_permitted_board() {
        let (a, base) = args("rejected");
        rolled_up(&base, "kalshi", &[sample("kalshi", "K1", 10, "0.29", "0.30")]);
        rolled_up(&base, "polymarket_us", &[sample("polymarket_us", "P1", 20, "0.75", "0.76")]);
        let d = board(&a);
        assert_eq!(d["priced_pairs"], 1, "a rejected verdict is not tradable: {d}");
        assert_eq!(d["unevaluated_pairs"], 0, "nor is it a pair we failed to evaluate: {d}");
        assert_eq!(d["unevaluated_by_venue"], serde_json::json!({}), "{d}");

        let raw = json(&a, &format!("day={DAY}&all=1"));
        let all: serde_json::Value = serde_json::from_str(&raw).expect("json");
        assert_eq!(all["priced_pairs"], 1, "still unpriced — it has no book: {all}");
        assert_eq!(all["unevaluated_pairs"], 1, "and in the full universe it is counted: {all}");
        assert_eq!(all["unevaluated_by_venue"]["kalshi"], 1, "both legs are missing: {all}");
        assert_eq!(all["unevaluated_by_venue"]["polymarket_us"], 1, "{all}");
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The defaults are the whole view when someone opens `/current` with no
    /// query at all, which is how it is normally opened.
    #[test]
    fn the_default_board_is_the_permitted_universe_at_a_twenty_five_clip() {
        let q = parse("");
        assert_eq!(q.scenario, Scenario::TakeTake);
        assert_eq!(q.clip_s, "25");
        assert_eq!(q.n, 25);
        assert_eq!(q.max_spread_s, "0.05");
        assert!(q.only_tradable, "detected-but-not-permitted edge must be asked for");
    }

    /// Detected is not permitted. Only the exact `all=1` opens the untradable
    /// universe: a bare `?all`, or any other value, leaves the board showing
    /// what this system is actually allowed to trade.
    #[test]
    fn only_an_explicit_all_flag_opens_the_untradable_universe() {
        assert!(!parse("all=1").only_tradable);
        assert!(parse("all=0").only_tradable);
        assert!(parse("all=true").only_tradable);
        assert!(parse("all").only_tradable, "a bare flag carries no value");
        assert!(parse("all=").only_tradable, "and neither does an empty one");
    }

    /// `?n=` garbage must fall back to the default, not to zero: `truncate(0)`
    /// renders an empty board, which reads as "no edge anywhere" rather than
    /// as a bad URL.
    #[test]
    fn an_unreadable_row_count_falls_back_to_the_default_not_to_zero() {
        assert_eq!(parse("n=abc").n, 25);
        assert_eq!(parse("n=-1").n, 25);
        assert_eq!(parse("n=").n, 25);
        assert_eq!(parse("n=3").n, 3);
        assert_eq!(parse("n=0").n, 0, "an explicit zero is still the operator's choice");
    }

    /// An unknown scenario name ranks the board on take-take rather than
    /// erroring — but it must not silently rank on a DIFFERENT one, because
    /// the page echoes `scenario` back and the operator reads it as confirmed.
    #[test]
    fn an_unknown_scenario_ranks_on_take_take() {
        assert_eq!(parse("scenario=nonsense").scenario, Scenario::TakeTake);
        assert_eq!(parse("scenario=make-a-take-b").scenario, Scenario::MakeATakeB);
        assert_eq!(parse("scenario=take-a-make-b").scenario, Scenario::TakeAMakeB);
    }

    /// The day names the rollup files this view reads. When the URL gives one
    /// it wins; when it does not, the default must be a real date — a blank
    /// would build `tob-kalshi-.jsonl` and report "no ToB rollup" for a day
    /// that was rolled up perfectly well.
    #[test]
    fn the_day_comes_off_the_url_or_is_a_real_date() {
        assert_eq!(parse("day=2026-07-01").day, "2026-07-01");
        let today = parse("").day;
        assert!(
            arb_core::resolve::parse_iso(&today).is_some(),
            "the default day must be a date, got {today:?}"
        );
    }

    /// Clip and max-spread are carried as STRINGS all the way to `Cx`, which
    /// parses them exactly. Reading them as f64 here would round a decimal
    /// before the exact arithmetic ever saw it.
    #[test]
    fn the_sizes_stay_strings_so_the_decimal_arithmetic_stays_exact() {
        let q = parse("clip=1000&max_spread=0.02");
        assert_eq!(q.clip_s, "1000");
        assert_eq!(q.max_spread_s, "0.02");
    }
}
