//! The engine's risk view — the [`RiskGate`] the quoters consult.
//!
//! `arb_core::risk::check_order` is the pure fold (ported with fixtures); this
//! is the state it folds over. The engine owns it because exposure is a
//! whole-book quantity: a per-quoter copy would let each relationship spend the
//! same headroom.
//!
//! Config comes from the same files the Python runner used (`config/exec.yaml`,
//! `config/topics.yaml`) so the caps are one source of truth, not a second
//! transcription.

use arb_core::model::Venue;
use arb_core::quoter::{RiskGate, RiskVerdict};
use arb_core::risk::{check_order, ConfigIn, ExposureIn, Input, RelIn, TopicIn};
use arb_core::scan::Rel;
use std::collections::HashMap;
use std::sync::Mutex;

/// Python `RiskConfig` defaults (src/arbbot/risk/manager.py:37-59). Only
/// bankroll and per_class_cap are file-driven; the rest are code defaults there
/// and are pinned here so the two cannot drift silently.
const TAIL_FRACTION: &str = "0.02";
const PER_REL_CAP: &str = "150";
const GLOBAL_CAP: &str = "0.50";
const OVERFLOW_MIN_APR: &str = "0.12";
const OVERFLOW_FRAC: &str = "0.10";

#[derive(Default)]
struct Exposure {
    by_rel: HashMap<String, f64>,
    by_class: HashMap<String, f64>,
    by_topic: HashMap<String, f64>,
}

pub struct RiskView {
    bankroll: String,
    per_class_cap: String,
    topics: Vec<(String, String, Option<String>)>, // family, budget, only_below_util
    default_topic_budget: Option<String>,
    default_only_below_util: Option<String>,
    /// Why the topic caps cannot be trusted, when they cannot. Set only for a
    /// `topics.yaml` that EXISTS and is damaged; the budgets are then $0, which
    /// refuses every order rather than granting every family unlimited room.
    topics_corrupt: Option<String>,
    /// venue -> available cash. Empty means the per-venue cash check sees $0
    /// and refuses everything, so this is REQUIRED for the gate to pass — an
    /// unconfigured balance fails closed, which is the right direction. BOTH of
    /// a basket's legs are checked against this (see `venue_costs`), so a venue
    /// omitted from `--balance` closes every relationship that touches it.
    ///
    /// STILL A HAND-TYPED CONSTANT (audit C13): `--balance kalshi=340.09` is
    /// passed in the unit file and is never decremented as capital is deployed,
    /// so this gate catches "that venue is not funded at all", not "we have
    /// spent our cash". Wiring the real figure needs the gateways' `balances()`
    /// at startup and after each fill, which lives in main.rs/arb-venue.
    balances: Vec<(String, String)>,
    /// rel id -> oracle_risk, from the registry. It scales the per-rel cap and
    /// is not carried on `Rel`, so it is looked up by id.
    oracle_risk: HashMap<String, String>,
    exposure: Mutex<Exposure>,
    /// Counts, for the stats line. Rejections are not errors — a gate that
    /// never fires is a gate nobody can see working.
    pub checked: Mutex<(u64, u64)>, // (allowed, rejected)
}

/// The per-topic caps, or the reason they cannot be trusted.
#[derive(Default)]
struct Topics {
    list: Vec<(String, String, Option<String>)>, // family, budget, only_below_util
    default_budget: Option<String>,
    default_gate: Option<String>,
    corrupt: Option<String>,
}

impl Topics {
    /// FAIL CLOSED. A damaged cap file must not widen a cap, so the default
    /// budget becomes $0: every topic — including `other` — then has no room and
    /// every check refuses. The engine still STARTS, deliberately: a process
    /// that runs and refuses can still sweep the book it inherited and can still
    /// answer the kill switch, where one that will not start leaves the previous
    /// run's quotes resting.
    fn corrupt(why: String) -> Topics {
        eprintln!("[risk] TOPIC CAPS UNUSABLE: {why}");
        eprintln!(
            "[risk] every per-topic budget is forced to $0 — no order of nonzero size will \
             pass the gate until config/topics.yaml is repaired (it is gitignored; git \
             cannot restore it)"
        );
        Topics {
            list: Vec::new(),
            default_budget: Some("0".to_string()),
            default_gate: None,
            corrupt: Some(why),
        }
    }
}

/// `config/topics.yaml` -> caps.
///
/// This used to return empty on BOTH read failure and parse failure, which
/// deleted every per-topic budget AND the low-utilisation gate — the only cap
/// file in the system that widened when it broke, and the only one git cannot
/// restore. Today it is what keeps the france/unlisted families closed.
///
/// The two failures are now distinguished on purpose:
///   * ABSENT is a configuration — "no per-topic budgets". The per-rel, class,
///     global and per-venue cash caps all still apply, so absence narrows the
///     rule set rather than removing the ceiling.
///   * PRESENT AND UNUSABLE is damage — unreadable, unparseable, not a mapping,
///     missing `default_topic_budget`, or carrying a named family with no
///     budget. A truncated file still parses as valid YAML, so "parsed" is not
///     "intact". Damage fails closed.
fn topics_from_yaml(path: &str) -> Topics {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("[risk] no {path}: running with NO per-topic budgets");
            return Topics::default();
        }
        // Exists but cannot be read (permissions, EIO): not a configuration.
        Err(e) => return Topics::corrupt(format!("cannot read {path}: {e}")),
    };
    let doc = match serde_yaml::from_str::<serde_yaml::Value>(&text) {
        Ok(d) => d,
        Err(e) => return Topics::corrupt(format!("{path} does not parse: {e}")),
    };
    if doc.as_mapping().is_none() {
        // An empty or truncated-to-nothing file parses as YAML null, which the
        // old code read as "no budgets configured".
        return Topics::corrupt(format!("{path} is not a mapping (empty or truncated?)"));
    }
    let num = |v: Option<&serde_yaml::Value>| -> Option<String> {
        let v = v?;
        v.as_str().map(|s| s.to_string()).or_else(|| v.as_f64().map(|f| f.to_string()))
    };
    let mut out = Vec::new();
    if let Some(t) = doc.get("topics") {
        let Some(list) = t.as_sequence() else {
            return Topics::corrupt(format!("{path}: `topics` is not a list"));
        };
        for (i, t) in list.iter().enumerate() {
            let Some(family) = t.get("family").and_then(|f| f.as_str()) else {
                return Topics::corrupt(format!("{path}: topic #{} has no `family`", i + 1));
            };
            // Skipping this entry — what the old code did — silently deleted
            // that family's cap, which is UNLIMITED for the family somebody
            // took the trouble to name.
            let Some(budget) = num(t.get("budget_usd")) else {
                return Topics::corrupt(format!(
                    "{path}: topic `{family}` has no usable `budget_usd`"
                ));
            };
            out.push((family.to_string(), budget, num(t.get("only_below_util"))));
        }
    }
    // A file that exists MUST carry `default_topic_budget`. Without it,
    // `arb_core::risk`'s `if let Some(budget)` skips the topic check entirely
    // and the `other` catch-all — where every relationship not matching a named
    // family lands — is uncapped, along with the utilisation gate.
    //
    // This is the likeliest damage of all, and the first version of this fix
    // missed it: the live file ENDS with its two `default_*` keys, so a write
    // cut short (or an editor saving half) leaves a valid SHORTER `topics:`
    // list and drops both defaults. `out` is then non-empty, so an
    // `out.is_empty()` guard never fires: verified A/B on one order — 40 open on
    // xvus-france-pres-27, asking 25 more against a $60 family budget — refused
    // on the intact file, ALLOWED on the tail-truncated one, with `describe()`
    // reporting a contented `topics 1`. Failing closed only when a NAMED family
    // lacks a budget while failing open when the unnamed catch-all does is the
    // wrong way round: `other` is the default destination for everything.
    let default_budget = num(doc.get("default_topic_budget"));
    if default_budget.is_none() {
        return Topics::corrupt(format!(
            "{path} has no `default_topic_budget`, which leaves the `other` \
             catch-all uncapped (truncated at the tail?)"
        ));
    }
    Topics {
        list: out,
        default_budget,
        default_gate: num(doc.get("default_only_below_util")),
        corrupt: None,
    }
}

impl RiskView {
    /// `balances` are venue->cash pairs. The dry-run engine has no credentials,
    /// so they arrive on the command line; once the engine reads them from the
    /// venue at startup (as the Python runner did) that becomes the source.
    pub fn load(
        exec_yaml: &str,
        topics_yaml: &str,
        balances: Vec<(String, String)>,
        oracle_risk: HashMap<String, String>,
    ) -> RiskView {
        let (mut bankroll, mut per_class) = ("980".to_string(), "0.35".to_string());
        if let Ok(text) = std::fs::read_to_string(exec_yaml) {
            if let Ok(doc) = serde_yaml::from_str::<serde_yaml::Value>(&text) {
                if let Some(b) = doc.get("bankroll_usd").and_then(|v| v.as_f64()) {
                    bankroll = b.to_string();
                }
                if let Some(c) = doc.get("per_class_cap").and_then(|v| v.as_f64()) {
                    per_class = c.to_string();
                }
            }
        }
        let t = topics_from_yaml(topics_yaml);
        RiskView {
            bankroll,
            per_class_cap: per_class,
            topics: t.list,
            default_topic_budget: t.default_budget,
            default_only_below_util: t.default_gate,
            topics_corrupt: t.corrupt,
            balances,
            oracle_risk,
            exposure: Mutex::new(Exposure::default()),
            checked: Mutex::new((0, 0)),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "bankroll ${} per_class {} topics {} balances [{}]",
            self.bankroll,
            self.per_class_cap,
            match &self.topics_corrupt {
                Some(why) => format!("UNUSABLE (all budgets $0): {why}"),
                None => self.topics.len().to_string(),
            },
            self.balances
                .iter()
                .map(|(v, b)| format!("{v}=${b}"))
                .collect::<Vec<_>>()
                .join(" ")
        )
    }

    /// A fill opened `qty` more contracts on this relationship — the exposure
    /// the NEXT check must see. Python: `risk.record_open(rel, filled)`.
    /// Takes the id/class rather than a `&Rel` so the engine can record from a
    /// fill without holding the quoter's relationship.
    pub fn record_open(&self, rel_id: &str, rtype: &str, qty: f64) {
        let mut e = self.exposure.lock().expect("exposure");
        *e.by_rel.entry(rel_id.to_string()).or_default() += qty;
        *e.by_class.entry(rtype.to_string()).or_default() += qty;
        let topic = topic_of(rel_id, &self.topics);
        *e.by_topic.entry(topic).or_default() += qty;
    }

    /// Open contracts on one relationship. The take-take concentration cap is
    /// measured against this, so it must see the SAME exposure the caps do —
    /// including what the startup ledger seed put there.
    pub fn open_ct(&self, rel_id: &str) -> f64 {
        self.exposure.lock().expect("exposure").by_rel.get(rel_id).copied().unwrap_or(0.0)
    }

    pub fn stats(&self) -> (u64, u64) {
        *self.checked.lock().expect("checked")
    }

    fn config(&self) -> ConfigIn {
        ConfigIn {
            bankroll: self.bankroll.clone(),
            tail_fraction: TAIL_FRACTION.into(),
            per_rel_cap: Some(PER_REL_CAP.into()),
            per_class_cap: self.per_class_cap.clone(),
            global_cap: GLOBAL_CAP.into(),
            overflow_min_apr: OVERFLOW_MIN_APR.into(),
            overflow_frac: OVERFLOW_FRAC.into(),
            default_topic_budget: self.default_topic_budget.clone(),
            default_only_below_util: self.default_only_below_util.clone(),
            topics: self
                .topics
                .iter()
                .map(|(f, b, u)| TopicIn {
                    family: f.clone(),
                    budget_usd: b.clone(),
                    only_below_util: u.clone(),
                })
                .collect(),
        }
    }
}

/// Same longest-family-match rule as `arb_core::risk::topic_of`, which is
/// private to that module.
fn topic_of(rel_id: &str, topics: &[(String, String, Option<String>)]) -> String {
    let hay = format!("-{rel_id}-");
    let mut best = "";
    for (family, _, _) in topics {
        if hay.contains(&format!("-{family}-")) && family.len() > best.len() {
            best = family;
        }
    }
    if best.is_empty() { "other".to_string() } else { best.to_string() }
}

/// Every venue whose cash a basket on `rel` would spend, charged the full
/// `notional` each.
///
/// The gate used to carry ONE entry — the leg being quoted — so the hedge leg's
/// venue was never cash-checked. Fund PM-US, starve Kalshi, and take-take opens
/// leg 1 and then cannot buy the hedge at ANY price, because the constraint is
/// cash and not price: a permanent naked short with no price-driven escape
/// (audit C13a).
///
/// Each venue is charged the whole notional rather than its share of the ~$1.00
/// basket, because the gate is handed a size and no prices. Over-reserving
/// refuses a basket we could just afford; under-reserving opens one we cannot
/// hedge. Two legs on the SAME venue are charged once, not twice — the two legs
/// of a basket cost ~$1.00 between them, not $1.00 each.
///
/// EXPECTED SIDE EFFECT, not a regression: `config/registry.yaml` has 14
/// `kalshi+polymarket` and 24 `polymarket+polymarket` relationships whose leg
/// sits on the geoblocked NON-US `polymarket`, which no `--balance` names. All
/// 38 now close with `insufficient polymarket balance`. That is correct — there
/// is no order path on that venue to hedge with, so a basket that needs one
/// cannot be opened — but it will look like 38 relationships suddenly stopped
/// quoting. They were never hedgeable.
fn venue_costs(rel: &Rel, quoted: Venue, notional: &str) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::new();
    // The quoted leg first, so its reason reads first in a multi-venue refusal.
    for v in std::iter::once(quoted).chain(rel.legs.iter().map(|l| l.venue)) {
        let name = v.as_str().to_string();
        if !out.iter().any(|(k, _)| *k == name) {
            out.push((name, notional.to_string()));
        }
    }
    out
}

impl RiskGate for RiskView {
    /// Gates OPENING risk only.
    ///
    /// INVARIANT: never consult this for a HEDGE. Refusing a hedge leaves the
    /// first leg naked, which is strictly worse than being a little over
    /// budget — the cap exists to bound how much we open, and the hedge is what
    /// makes what we already opened safe. The engine's hedge path consults risk
    /// nowhere (`grep -n 'risk' engine.rs`: stats, `record_open`, `open_ct`, and
    /// this check on the take-take ENTRY), and it must stay that way. Gating
    /// both legs' cash here is what keeps that honest: the hedge's venue is
    /// proven fundable BEFORE the entry that obliges us to hedge.
    fn check(&self, rel: &Rel, venue: Venue, notional: i64) -> RiskVerdict {
        let n = notional.to_string();
        let e = self.exposure.lock().expect("exposure");
        let pairs = |m: &HashMap<String, f64>| -> Vec<(String, String)> {
            m.iter().map(|(k, v)| (k.clone(), v.to_string())).collect()
        };
        let inp = Input {
            config: self.config(),
            rel: RelIn {
                id: rel.id.clone(),
                rtype: rel.rtype.as_str().to_string(),
                // Unknown oracle_risk is treated as HIGH (0.25x cap), not low:
                // a relationship we cannot classify is not one to size up.
                oracle_risk: self
                    .oracle_risk
                    .get(&rel.id)
                    .cloned()
                    .unwrap_or_else(|| "high".to_string()),
            },
            exposure: ExposureIn {
                by_relationship: pairs(&e.by_rel),
                by_class: pairs(&e.by_class),
                by_topic: pairs(&e.by_topic),
            },
            balances: self.balances.clone(),
            notional: n.clone(),
            venue_costs: venue_costs(rel, venue, &n),
            // The maker path passes no APR, so the great-opportunity overflow
            // never applies to a resting quote (quoter.py:302 — only the
            // take-take path supplies one).
            opportunity_apr: None,
            weakest_fwd_apr: None,
            // KILL is handled by the engine's own deadline, which cancels
            // everything rather than merely refusing new orders.
            kill: false,
        };
        drop(e);
        let d = check_order(&inp);
        let mut c = self.checked.lock().expect("checked");
        if d.allowed {
            c.0 += 1;
        } else {
            c.1 += 1;
        }
        RiskVerdict { allowed: d.allowed, reasons: d.reasons }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arb_core::scan::{RelLeg, RelType};

    fn rel(id: &str) -> Rel {
        Rel {
            id: id.into(),
            rtype: RelType::CrossVenueEquivalent,
            tranche: "head".into(),
            legs: vec![
                RelLeg { venue: Venue::Kalshi, market_id: "K".into() },
                RelLeg { venue: Venue::PolymarketUs, market_id: "P".into() },
            ],
        }
    }

    fn view(balances: Vec<(&str, &str)>, oracle: &str) -> RiskView {
        view_with_topics("/nonexistent/topics.yaml", balances, oracle)
    }

    /// Both venues funded — the shape the unit file actually passes
    /// (`--balance kalshi=340.09 --balance polymarket_us=349.42`). A basket
    /// spends on both legs, so a one-venue view is a REFUSAL fixture now.
    fn funded(oracle: &str) -> RiskView {
        view(vec![("kalshi", "1000"), ("polymarket_us", "1000")], oracle)
    }

    fn view_with_topics(topics: &str, balances: Vec<(&str, &str)>, oracle: &str) -> RiskView {
        let mut o = HashMap::new();
        o.insert("r1".to_string(), oracle.to_string());
        RiskView::load(
            "/nonexistent/exec.yaml", // falls back to the pinned defaults
            topics,
            balances.into_iter().map(|(a, b)| (a.to_string(), b.to_string())).collect(),
            o,
        )
    }

    fn write_topics(tag: &str, body: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join(format!("arb-risk-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join("topics.yaml");
        std::fs::write(&p, body).unwrap();
        p
    }

    /// FAIL-CLOSED: an unconfigured venue balance reads as $0 cash, so the
    /// order is refused rather than waved through. Getting this backwards would
    /// let a misconfigured engine trade with imaginary capital.
    #[test]
    fn no_balance_configured_refuses_every_order() {
        let v = view(vec![], "low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed);
        assert!(d.reasons.iter().any(|r| r.contains("insufficient kalshi balance")), "{:?}", d.reasons);
    }

    #[test]
    fn a_funded_venue_within_caps_is_allowed() {
        let v = funded("low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(d.allowed, "{:?}", d.reasons);
    }

    /// C13a. Quoting Kalshi obliges us to hedge on PM-US, so PM-US's cash is
    /// part of the decision. Before this, only the quoted leg was checked: the
    /// order rested, filled, and the hedge could not be bought at any price
    /// because the constraint was cash — a permanent naked short.
    #[test]
    fn the_hedge_legs_venue_is_cash_gated_too() {
        let v = view(vec![("kalshi", "340")], "low"); // PM-US starved
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed, "a basket we cannot hedge must not be opened");
        assert!(
            d.reasons.iter().any(|r| r.contains("insufficient polymarket_us balance")),
            "{:?}",
            d.reasons
        );
    }

    /// ...and symmetrically, since take-take enters on PM-US and hedges on
    /// Kalshi. This is the exact failure the audit describes: fund PM-US,
    /// starve Kalshi.
    #[test]
    fn a_take_take_entry_is_refused_when_the_kalshi_hedge_is_unfunded() {
        let v = view(vec![("polymarket_us", "349")], "low");
        let d = v.check(&rel("r1"), Venue::PolymarketUs, 5);
        assert!(!d.allowed, "leg 1 must not fire without cash for leg 2");
        assert!(
            d.reasons.iter().any(|r| r.contains("insufficient kalshi balance")),
            "{:?}",
            d.reasons
        );
    }

    /// Cash is still checked per VENUE — funding one is not funding both — and
    /// the quoted leg is still charged. Both directions refuse on one balance.
    #[test]
    fn cash_is_checked_on_every_venue_the_basket_spends_on() {
        let v = view(vec![("kalshi", "340")], "low");
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        assert!(!v.check(&rel("r1"), Venue::PolymarketUs, 5).allowed);
        let both = funded("low");
        assert!(both.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        assert!(both.check(&rel("r1"), Venue::PolymarketUs, 5).allowed);
    }

    /// Two legs on ONE venue cost ~$1.00 between them, not $1.00 each, so the
    /// venue is charged once. Charging twice would be a second, invented cap.
    #[test]
    fn one_venue_carrying_both_legs_is_charged_once() {
        let mut r = rel("r1");
        r.legs[1].venue = Venue::Kalshi;
        let costs = venue_costs(&r, Venue::Kalshi, "5");
        assert_eq!(costs, vec![("kalshi".to_string(), "5".to_string())]);
    }

    /// The quoted venue is charged even if it is somehow not among the legs —
    /// it is the venue whose cash the order spends.
    #[test]
    fn the_quoted_venue_is_always_charged() {
        let mut r = rel("r1");
        r.legs.clear();
        assert_eq!(
            venue_costs(&r, Venue::Kalshi, "5"),
            vec![("kalshi".to_string(), "5".to_string())]
        );
    }

    /// Exposure accumulates across fills and eventually closes the per-rel cap
    /// ($150 at oracle_risk=low). This is the whole point of the engine owning
    /// one view: per-quoter copies would each spend the same headroom.
    #[test]
    fn accumulated_exposure_eventually_refuses() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        v.record_open("r1", "cross-venue-equivalent", 150.0);
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed, "150 open against a 150 cap leaves no headroom");
        assert!(d.reasons.iter().any(|r| r.contains("per-relationship")), "{:?}", d.reasons);
    }

    /// oracle_risk scales the per-rel cap (low 1.0, medium 0.5, high 0.25), so
    /// a high-risk name refuses far sooner on the same exposure.
    #[test]
    fn oracle_risk_scales_the_cap() {
        for (oracle, open, want_allowed) in
            // caps: low 150, medium 75, high 37.5; a clip of 5 needs that much headroom
            [("low", 100.0, true), ("medium", 100.0, false), ("high", 35.0, false)]
        {
            let v = funded(oracle);
            v.record_open("r1", "cross-venue-equivalent", open);
            assert_eq!(
                v.check(&rel("r1"), Venue::Kalshi, 5).allowed,
                want_allowed,
                "oracle_risk={oracle} open={open}"
            );
        }
    }

    /// An unknown relationship is treated as HIGH oracle risk, not low: a name
    /// we cannot classify is not one to size up.
    #[test]
    fn an_unclassified_relationship_gets_the_tightest_cap() {
        let v = funded("low");
        v.record_open("unknown-rel", "cross-venue-equivalent", 30.0);
        let d = v.check(&rel("unknown-rel"), Venue::Kalshi, 10);
        assert!(!d.allowed, "unknown => high risk => 0.25x cap: {:?}", d.reasons);
    }

    #[test]
    fn allowed_and_rejected_are_counted_for_the_stats_line() {
        let v = funded("low");
        v.check(&rel("r1"), Venue::Kalshi, 5);
        v.record_open("r1", "cross-venue-equivalent", 150.0); // fills the per-rel cap
        v.check(&rel("r1"), Venue::Kalshi, 5); // => refused
        assert_eq!(v.stats(), (1, 1));
    }

    // ---- C11: config/topics.yaml must not widen a cap when it breaks ----

    /// A file that EXISTS but does not parse is damage, and damage must not read
    /// as "no budgets configured". The old code returned empty on a parse error,
    /// which deleted every per-topic budget AND the low-utilisation gate — and
    /// the file is gitignored, so nothing could restore it.
    #[test]
    fn a_corrupt_topics_file_does_not_grant_unlimited_budget() {
        let p = write_topics("corrupt", "topics: [ {family: nobel-peace-26, budget_usd: 80 }\n");
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        let d = v.check(&rel("r1"), Venue::Kalshi, 5);
        assert!(!d.allowed, "a damaged cap file must refuse, not wave through");
        assert!(d.reasons.iter().any(|r| r.contains("topic budget")), "{:?}", d.reasons);
        assert!(v.describe().contains("UNUSABLE"), "and say so: {}", v.describe());
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// The likeliest damage of all, and the one the first version of this fix
    /// let through: the live file ends with its two `default_*` keys, so a
    /// truncation at the tail leaves a VALID shorter `topics:` list and drops
    /// them. `default_topic_budget: None` makes `arb_core::risk` skip the topic
    /// check outright, uncapping the `other` catch-all and deleting the util
    /// gate — while the surviving named families still look configured.
    #[test]
    fn a_tail_truncated_topics_file_still_fails_closed() {
        // Intact: france is capped at $60, and 40 open leaves no room for 25.
        let intact = write_topics(
            "intact-tail",
            "topics:\n  - {family: france-pres-27, budget_usd: 60}\n\
             default_topic_budget: 30\ndefault_only_below_util: 0.5\n",
        );
        let v = view_with_topics(
            intact.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        let r = rel("xvus-france-pres-27-djt");
        v.record_open("xvus-france-pres-27-djt", "cross-venue-equivalent", 40.0);
        assert!(!v.check(&r, Venue::Kalshi, 25).allowed, "40+25 > 60");

        // Same file, cut after the last topic: the list still parses.
        let cut = write_topics(
            "cut-tail",
            "topics:\n  - {family: france-pres-27, budget_usd: 60}\n",
        );
        let v = view_with_topics(
            cut.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        v.record_open("xvus-france-pres-27-djt", "cross-venue-equivalent", 40.0);
        let d = v.check(&r, Venue::Kalshi, 25);
        assert!(!d.allowed, "a dropped default must not widen the cap: {:?}", d.reasons);
        assert!(v.describe().contains("default_topic_budget"), "{}", v.describe());
        // and the catch-all `other` — where everything unlisted lands — too
        assert!(!v.check(&rel("xvus-unlisted-thing"), Venue::Kalshi, 5).allowed);
        let _ = std::fs::remove_dir_all(intact.parent().unwrap());
        let _ = std::fs::remove_dir_all(cut.parent().unwrap());
    }

    /// An empty (or truncated-to-nothing) file parses as valid YAML null, so
    /// "it parsed" is not "it is intact".
    #[test]
    fn an_empty_topics_file_is_treated_as_damage() {
        let p = write_topics("empty", "");
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// A named family with an unusable budget used to be skipped, which left
    /// that family — the one somebody took the trouble to name — uncapped.
    #[test]
    fn a_topic_entry_without_a_budget_is_damage_not_a_deleted_cap() {
        let p = write_topics(
            "nobudget",
            "topics:\n  - {family: nobel-peace-26, budget_usd: 80}\n  \
             - {family: france-pres-27, only_below_util: 0.5}\ndefault_topic_budget: 30\n",
        );
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(v.describe().contains("france-pres-27"), "{}", v.describe());
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// ABSENT is a configuration, not damage: no per-topic budgets, and the
    /// per-rel/class/global/cash caps all still apply. The engine must stay
    /// startable and tradable in that shape — the whole test suite above runs
    /// in it.
    #[test]
    fn an_absent_topics_file_is_a_legitimate_no_budget_config() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5).allowed);
        assert!(!v.describe().contains("UNUSABLE"), "{}", v.describe());
    }

    /// The happy path still caps: the real file's shape, and a family over its
    /// budget is refused while one under it is not.
    #[test]
    fn a_valid_topics_file_still_applies_its_budgets() {
        let p = write_topics(
            "valid",
            "topics:\n  - {family: nobel-peace-26, budget_usd: 80}\n  \
             - {family: france-pres-27, budget_usd: 60, only_below_util: 0.5}\n\
             default_topic_budget: 30\ndefault_only_below_util: 0.5\n",
        );
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(v.describe().contains("topics 2"), "{}", v.describe());
        let r = rel("xvus-nobel-peace-26-djt");
        assert!(v.check(&r, Venue::Kalshi, 5).allowed);
        v.record_open("xvus-nobel-peace-26-djt", "cross-venue-equivalent", 80.0);
        let d = v.check(&r, Venue::Kalshi, 5);
        assert!(!d.allowed, "80 open against an $80 family budget: {:?}", d.reasons);
        assert!(
            d.reasons.iter().any(|r| r.contains("topic budget [nobel-peace-26]")),
            "{:?}",
            d.reasons
        );
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }
}
