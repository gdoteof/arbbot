//! The engine's risk view — the [`RiskGate`] the quoters consult.
//!
//! `arb_core::risk::check_order` is the pure fold (ported with fixtures); this
//! is the state it folds over. The engine owns it because exposure is a
//! whole-book quantity: a per-quoter copy would let each relationship spend the
//! same headroom.
//!
//! SHARING THE MAP WAS NEVER ENOUGH, though, and that sentence used to stop
//! here. Exposure moved only in [`RiskView::record_open`], whose only live
//! caller is the FILL path, so the shared view was a shared SNAPSHOT: the
//! quoter consulted it once per (leg, side), rested the order, and every other
//! quoting relationship consulted the identical number. Let a class hold one
//! clip of headroom: the first quote passes, rests, and every other quoting
//! relationship x 2 legs x 2 sides passes against that same unchanged figure —
//! and all those orders are live at the venue at the same time. The cap
//! bounded the ORDER ENTRY RATE, not the capital. So an
//! allowed check now RESERVES its clip against the (market, side) the order
//! rests on, and the reservation is folded into the exposure the NEXT check
//! sees. Every way one is given back is enumerated on the `reserved` field
//! below — not linked, because it is private and the link would be dead.
//!
//! Config comes from the same files the Python runner used (`config/exec.yaml`,
//! `config/topics.yaml`) so the caps are one source of truth, not a second
//! transcription.

use arb_core::model::{BookSide, Venue};
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

/// The (relationship, market, side) one resting quote sits on. There is at most
/// one of ours per slot — that is what `Quoter::resting` is keyed by — so a
/// reprice REPLACES its own reservation rather than stacking a second one.
type Slot = (String, String, BookSide);

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
    /// Why the bankroll and class cap cannot be trusted, when they cannot. Both
    /// are then $0 — see `Caps::corrupt`.
    caps_corrupt: Option<String>,
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
    /// Capital COMMITTED by a quote that is resting and has not filled, by the
    /// slot it rests on -> (class, contracts). Folded into the exposure every
    /// `check` sees, so headroom can be spent only once.
    ///
    /// EVERY WAY A RESTING QUOTE GOES AWAY, and which of them releases:
    ///   * FILLED — `consume`, from the engine's fill path, moves the filled
    ///     contracts out of the reservation and into real exposure
    ///     (`record_open`). A partial fill leaves the remainder reserved,
    ///     because the remainder is still resting.
    ///   * CANCELLED — `Quoter::on_book`'s cancel arm calls `release` as it
    ///     drops its `resting` entry. YES.
    ///   * PULLED FOR A STALE CAP REFUSAL — `CAP_REFUSAL_PULL_S` (#35) takes a
    ///     quote off when the caps have refused its replacement for long
    ///     enough. YES, and it is the release that matters most: the quote is
    ///     coming off BECAUSE capital is scarce, so leaking it there ratchets
    ///     the cap shut exactly when it is already jammed.
    ///   * REPLACED (an amend) — the new check writes the SAME slot, so the
    ///     old reservation is overwritten rather than leaked, and the check
    ///     that authorises the amend does not count the quote it is replacing
    ///     against itself. YES.
    ///   * SWEPT AT A HALT — `Quoter::cancel_all` releases every slot it pulls,
    ///     and it has exactly two callers: `kill_tick` and `pull_quotes` (the
    ///     feed-stale pull). YES, and they are what drains this map several
    ///     times an hour on a live feed. The SHUTDOWN sweep is not one of them
    ///     — `spawn_shutdown_sweep` is a venue-level cancel that touches no
    ///     quoter — but the process is exiting, so the map goes with it. The
    ///     venue `SweepAndVerify` that follows a halt reaches orders we hold no
    ///     id for; those were released when the quoter last dropped them, or
    ///     belong to a previous process.
    ///   * REJECTED BY THE VENUE — NO, and this one cannot be closed here. A
    ///     rejected place produces no engine event at all (`exec.rs` counts
    ///     `exec_failed` and tells the engine nothing), so the QUOTER still
    ///     believes it rests. The reservation therefore lasts exactly as long
    ///     as that belief and is released by the cancel or the amend the next
    ///     book event on that market produces. It over-reserves, which refuses
    ///     rather than permits; it is unbounded only for a market whose feed
    ///     goes silent forever, which is the pre-existing "market goes quietly
    ///     dark" defect and not a new one.
    ///   * ORPHANED BY A LOST ACK — `recover_place` adopts the order and emits
    ///     an `order_ack`, so the order really is resting and the reservation
    ///     is correct. A recovery that finds nothing is the rejected case.
    ///
    /// A marketable IOC reserves NOTHING (`rests_on: None`): it does not rest,
    /// there is no cancel to release it, and take-take's own cooldown gate is
    /// what bounds re-firing in the window before its fill books.
    reserved: Mutex<HashMap<Slot, (&'static str, f64)>>,
    /// Counts, for the stats line. Rejections are not errors — a gate that
    /// never fires is a gate nobody can see working.
    pub checked: Mutex<(u64, u64)>, // (allowed, rejected)
}

/// What BOTH cap loaders in this file accept as a number.
///
/// They used to disagree: the topic loader took `60` or `"60"`, the exec loader
/// took only `60` and silently kept its compiled-in default for `"60"` — so the
/// same quoting, in the same format, in the same directory, meant "that is the
/// cap" in one file and "your edit did nothing" in the other. Quoting is an
/// editing accident, not a semantic.
///
/// A string that is not a number is not a number, in either file: the value is
/// carried to `arb_core::risk` as a decimal STRING and parsed there under an
/// `expect`, so `"none"` would panic the gate mid-run rather than refuse. Same
/// for `nan`/`inf`, which `f64::from_str` and libdecnumber both accept: an
/// infinite cap is no cap, and NaN refuses everything by accident rather than
/// by design. The string is TRIMMED before it is returned, not merely before it
/// is validated — `"500 "` is a decimal to a human and to `f64::from_str`, but
/// libdecnumber's numeric-string syntax admits no surrounding whitespace.
///
/// None therefore means "not a usable number", which is NOT the same question
/// as "was the key there" — see `unusable`.
fn num(v: Option<&serde_yaml::Value>) -> Option<String> {
    let v = v?;
    let n = v
        .as_str()
        .map(|s| s.trim().to_string())
        .or_else(|| v.as_f64().map(|f| f.to_string()))?;
    n.parse::<f64>().ok().filter(|f| f.is_finite()).map(|_| n)
}

/// The key is PRESENT and its value is not a usable number.
///
/// `num` answers None to both "no such key" and "unusable value", and for the
/// caps those two collapse safely: either way there is no cap, and no cap is
/// damage. For `only_below_util` they are OPPOSITES. Absent means "this family
/// has no utilisation gate", which is a configuration. Unusable must mean
/// damage, because the gate is a RESTRICTION and `arb_core::risk` applies it
/// under `if let Some(gate_s) = gate` — a None there does not fail closed, it
/// deletes the check. `only_below_util: 50%` is exactly the typo the field's
/// own units invite, and the live config/topics.yaml carries two of these.
fn unusable(v: Option<&serde_yaml::Value>) -> bool {
    v.is_some() && num(v).is_none()
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
            // ...and a gate that cannot be READ is not a family without a gate.
            if unusable(t.get("only_below_util")) {
                return Topics::corrupt(format!(
                    "{path}: topic `{family}` has an unusable `only_below_util`"
                ));
            }
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
    // The catch-all's gate, which covers every family without one of its own.
    if unusable(doc.get("default_only_below_util")) {
        return Topics::corrupt(format!(
            "{path} has an unusable `default_only_below_util`, which would delete the \
             utilisation gate for every family that does not set its own"
        ));
    }
    Topics {
        list: out,
        default_budget,
        default_gate: num(doc.get("default_only_below_util")),
        corrupt: None,
    }
}

/// The bankroll and the class cap, or the reason they cannot be trusted.
struct Caps {
    bankroll: String,
    per_class: String,
    corrupt: Option<String>,
}

impl Caps {
    /// FAIL CLOSED, the same rule and the same reasoning as `Topics::corrupt`.
    /// The bankroll becomes $0, and the class cap and the global cap are both
    /// fractions of it (`bankroll * per_class_cap`, `bankroll * 0.50`), so every
    /// order of nonzero size is refused.
    ///
    /// The engine still STARTS, and here that matters MORE than it does for
    /// topics: `arm_venues` exits with code 9 the moment a precondition is
    /// unmet, and it does so BEFORE `startup_sweep` runs. A cap file that
    /// refused to arm would therefore leave the previous run's quotes resting on
    /// the venue with nothing owning them — the exact 2026-07-28 orphan. A
    /// process that runs and refuses cancels the book it inherited, answers the
    /// kill switch, and sweeps on the way out.
    fn corrupt(why: String) -> Caps {
        eprintln!("[risk] CAPITAL CAPS UNUSABLE: {why}");
        eprintln!(
            "[risk] bankroll and per_class_cap are forced to $0 — the class and global caps \
             are both fractions of the bankroll, so no order of nonzero size will pass the \
             gate until config/exec.yaml is repaired (it IS tracked: `git checkout` it)"
        );
        Caps { bankroll: "0".to_string(), per_class: "0".to_string(), corrupt: Some(why) }
    }
}

/// `config/exec.yaml` -> the bankroll and the class cap.
///
/// This used to be four `if let`s with no `else`, each falling back to a
/// compiled-in `980` / `0.35`, and it is the OUTERMOST cap: the bankroll scales
/// both the class cap and the global cap, and nothing sits behind it. Four
/// distinct failures all read as "980 is what the file says":
///   * the file cannot be read (permissions, EIO, a `--exec-config` typo),
///   * it does not parse (a 5-line file caught mid-write),
///   * a key is absent (`per_class_cap` is the LAST line — a tail truncation
///     drops it and leaves valid YAML behind, exactly as it does in topics.yaml),
///   * the value is not a YAML float — and `bankroll_usd: "500"` is an ordinary
///     edit, not damage. See `num`.
///
/// `describe()` then printed a contented `bankroll $980 per_class 0.35`,
/// indistinguishable from a good load, because it was numerically identical to
/// one: the live file happens to hold exactly those two values, which is what
/// kept this latent.
///
/// ABSENCE is damage here, unlike in `topics_from_yaml`. The distinction is not
/// arbitrary: `topics.yaml` is gitignored and optional, and its absence NARROWS
/// the rule set (the per-rel, class, global and per-venue cash caps all still
/// apply). `exec.yaml` is tracked, so a correct checkout always has one, and its
/// absence removes the only ceiling there is — leaving a transcription of a
/// Python default in place of the file this module's header calls the one source
/// of truth.
///
/// A wrong working directory is NOT among the cases this sees, though it looks
/// like it should be: `load_quoters` runs first and its `Registry::load(...)
/// .expect("read registry")` dies several frames earlier on the same kind of
/// relative default. What reaches here is exec.yaml going missing on its own —
/// a `--exec-config` pointed at nothing, or the file deleted beside an intact
/// registry.
fn caps_from_yaml(path: &str) -> Caps {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) => return Caps::corrupt(format!("cannot read {path}: {e}")),
    };
    let doc = match serde_yaml::from_str::<serde_yaml::Value>(&text) {
        Ok(d) => d,
        Err(e) => return Caps::corrupt(format!("{path} does not parse: {e}")),
    };
    // An empty or truncated-to-nothing file parses as YAML null, on which
    // `get` is None — so it lands here rather than needing its own arm.
    let Some(bankroll) = num(doc.get("bankroll_usd")) else {
        return Caps::corrupt(format!("{path} has no usable `bankroll_usd`"));
    };
    let Some(per_class) = num(doc.get("per_class_cap")) else {
        return Caps::corrupt(format!("{path} has no usable `per_class_cap`"));
    };
    Caps { bankroll, per_class, corrupt: None }
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
        let c = caps_from_yaml(exec_yaml);
        let t = topics_from_yaml(topics_yaml);
        RiskView {
            bankroll: c.bankroll,
            per_class_cap: c.per_class,
            topics: t.list,
            default_topic_budget: t.default_budget,
            default_only_below_util: t.default_gate,
            topics_corrupt: t.corrupt,
            caps_corrupt: c.corrupt,
            balances,
            oracle_risk,
            exposure: Mutex::new(Exposure::default()),
            reserved: Mutex::new(HashMap::new()),
            checked: Mutex::new((0, 0)),
        }
    }

    pub fn describe(&self) -> String {
        format!(
            "{} topics {} balances [{}]",
            match &self.caps_corrupt {
                Some(why) => format!("CAPITAL CAPS UNUSABLE (bankroll $0): {why},"),
                None => format!("bankroll ${} per_class {}", self.bankroll, self.per_class_cap),
            },
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

    /// `qty` of the quote resting on this slot has FILLED, so it is no longer a
    /// reservation — the caller has just booked it as real exposure through
    /// `record_open`, and counting it in both places would refuse capital twice.
    ///
    /// The remainder stays reserved: a 5-lot that fills 3 is still resting 2.
    /// A slot with nothing left is dropped, and a slot that was never reserved
    /// (a take-take IOC, or a fill on an order this run inherited) is a no-op.
    pub fn consume(&self, rel_id: &str, market_id: &str, side: BookSide, qty: f64) {
        let mut r = self.reserved.lock().expect("reserved");
        let key = (rel_id.to_string(), market_id.to_string(), side);
        if let Some((_, left)) = r.get_mut(&key) {
            *left -= qty;
            if *left <= 0.0 {
                r.remove(&key);
            }
        }
    }

    /// Contracts currently held against the caps by quotes that are resting and
    /// have not filled.
    ///
    /// In the stats line because a cap that refuses on invisible state cannot be
    /// debugged: `risk_rejected` rising with `risk_reserved` is the caps working,
    /// and `risk_reserved` that only ever rises is a reservation leak — the one
    /// failure of this mechanism that is worse than the bug it fixes.
    pub fn reserved_ct(&self) -> f64 {
        self.reserved.lock().expect("reserved").values().map(|(_, q)| q).sum()
    }

    /// Open contracts on one relationship. The take-take concentration cap is
    /// measured against this, so it must see the SAME exposure the caps do —
    /// including what the startup ledger seed put there.
    ///
    /// Reservations are deliberately NOT included: this is the take-take
    /// CONCENTRATION cap, which asks how much of one relationship we are
    /// holding, not how much capital is committed. The capital question is the
    /// `check` below, and take-take asks that one too.
    pub fn open_ct(&self, rel_id: &str) -> f64 {
        self.exposure.lock().expect("exposure").by_rel.get(rel_id).copied().unwrap_or(0.0)
    }

    /// How much of the capital this engine may deploy IS deployed, in [0, 1] —
    /// the term the floating APR bar rides on (`Engine::apr_tick`, reported as
    /// `maker_apr_bar`). Port of `exec/main.py:_tt_refresh`:
    ///
    /// ```python
    /// cap  = risk.config.bankroll * risk.config.per_class_cap
    /// util = min(max(risk.exposure.total / cap if cap else 1.0, 0.0), 1.0)
    /// ```
    ///
    /// THE PYTHON IS A GLOBAL NUMERATOR OVER A PER-CLASS DENOMINATOR, and this
    /// no longer copies it. `risk.exposure.total` sums EVERY class; `bankroll *
    /// per_class_cap` is what ONE class may hold ($343). Their ratio is not a
    /// fraction of anything, and it is not bounded by 1 either — five classes
    /// may each fill their own budget. A book holding 200 contracts of
    /// `cross-venue-equivalent` and 143 of `exclusive`, with NEITHER class past
    /// two thirds of its own $343, read 1.000 FULL and pinned the hurdle at the
    /// 16%/yr ceiling.
    ///
    /// Live on 2026-07-31 the error was smaller and one-sided, because every
    /// open relationship happens to be `cross-venue-equivalent`: $322 read 0.94
    /// and charged 15.3%/yr, with $168 of the $490 this engine is allowed to
    /// deploy still unspent. Against that $490 it is 0.66, and 11.9%/yr.
    ///
    /// `bankroll * GLOBAL_CAP` is the denominator because it bounds the SAME
    /// quantity the numerator sums. `arb_core::risk`'s global check refuses
    /// when `sum(by_relationship) + notional > bankroll * global_cap`, so 1.0
    /// here means exactly "the next contract is refused" and 0.0 means an empty
    /// book. Nothing about the per-class cap is weakened: it is a separate,
    /// tighter check, measured per class against `by_class` — which is where a
    /// per-class question belongs and is not what this scalar can answer, since
    /// one bar is installed on every quoter regardless of class.
    ///
    /// NOT THE SAME NUMBER AS THE `util` IN AN `only_below_util` REFUSAL, from
    /// here on. `arb_core::risk::check_order` computes its own for the topic
    /// gate and still divides by the class cap — which is the denominator
    /// `config/topics.yaml` documents that field with, and which two of the
    /// Python-parity fixtures pin the printed value of. Repointing it is a
    /// change to what a config key MEANS, with a frozen generator behind the
    /// fixtures; it is not this change. At live exposure both readings sit
    /// above every configured 0.5 gate, so no gate changes hands on it today.
    ///
    /// Exposure is in CONTRACTS and the cap in dollars, which is the same
    /// ~$1-a-contract equivalence `RiskGate::check` is built on and `risk.rs`
    /// already divides by. A cap of zero reads as FULL, not empty: it is the
    /// direction that demands more of a new quote rather than less.
    ///
    /// RESERVATIONS ARE NOT COUNTED HERE. This is what the maker APR hurdle
    /// rides on (`Engine::apr_tick`), and floating that bar on capital that is
    /// only committed is a policy change, not a fix. The undischarged-hedge
    /// seed DOES move it, because that seed is a `record_open`.
    ///
    /// That exclusion matters MORE now, and the sentence this replaces said the
    /// opposite: it argued the seed had "no effect, since the live book already
    /// clamps this to 1.000", which was written at 346 open contracts against
    /// $343. Nothing clamps at $490 — the live book reads 0.59 — so every
    /// contract now moves the bar, and a book whose capital is committed to
    /// RESTING quotes rather than fills still reads as idle here.
    pub fn utilization(&self) -> f64 {
        let cap = self.global_cap_usd();
        if cap <= 0.0 {
            return 1.0;
        }
        let total: f64 = self.exposure.lock().expect("exposure").by_rel.values().sum();
        (total / cap).clamp(0.0, 1.0)
    }

    /// The dollar denominator [`Self::utilization`] divides by: bankroll times
    /// the GLOBAL cap — the same ceiling `arb_core::risk` bounds the same
    /// whole-book exposure sum against.
    ///
    /// Extracted so a caller can ask whether that denominator is REAL before
    /// trusting a number derived from it. `utilization` answers a cap of zero
    /// with `1.0` — full — which is the safe direction for an ENTRY (demand
    /// more of a new quote) and the unsafe one for an EXIT (charge the ceiling
    /// hurdle, so liquidate everything): see `crate::unwind`. One definition,
    /// because two would drift.
    ///
    /// A damaged `exec.yaml` is still degenerate here: `Caps::corrupt` forces
    /// the BANKROLL to "0", and every cap in this file is a fraction of it.
    pub fn global_cap_usd(&self) -> f64 {
        self.bankroll.parse::<f64>().unwrap_or(0.0) * GLOBAL_CAP.parse::<f64>().unwrap_or(0.0)
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
    fn check(
        &self,
        rel: &Rel,
        venue: Venue,
        notional: i64,
        rests_on: Option<(&str, BookSide)>,
    ) -> RiskVerdict {
        let n = notional.to_string();
        let e = self.exposure.lock().expect("exposure");
        let mut by_rel = e.by_rel.clone();
        let mut by_class = e.by_class.clone();
        let mut by_topic = e.by_topic.clone();
        drop(e);
        // Capital already committed by quotes that are RESTING. Without this
        // the gate is a pure read and every resting quote in the process spends
        // the same headroom — see the module header.
        //
        // This slot's own reservation is skipped, because a reprice on it
        // REPLACES that quote: charging the amend for the order it is about to
        // cancel would refuse repricing exactly when the book is fullest, which
        // is when a stale quote is most dangerous.
        let mine = rests_on.map(|(m, s)| (rel.id.clone(), m.to_string(), s));
        {
            let r = self.reserved.lock().expect("reserved");
            for (slot, (class, qty)) in r.iter() {
                if Some(slot) == mine.as_ref() {
                    continue;
                }
                *by_rel.entry(slot.0.clone()).or_default() += qty;
                *by_class.entry((*class).to_string()).or_default() += qty;
                *by_topic.entry(topic_of(&slot.0, &self.topics)).or_default() += qty;
            }
        }
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
                by_relationship: pairs(&by_rel),
                by_class: pairs(&by_class),
                by_topic: pairs(&by_topic),
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
        let d = check_order(&inp);
        // RESERVE. The quoter rests the order the instant this returns allowed
        // — there is no branch between them — so this is the moment the capital
        // is committed, and it stays committed until the quote fills or comes
        // off. An amend overwrites its own slot rather than adding to it.
        if d.allowed {
            if let Some(slot) = mine {
                self.reserved
                    .lock()
                    .expect("reserved")
                    .insert(slot, (rel.rtype.as_str(), notional as f64));
            }
        }
        let mut c = self.checked.lock().expect("checked");
        if d.allowed {
            c.0 += 1;
        } else {
            c.1 += 1;
        }
        RiskVerdict { allowed: d.allowed, reasons: d.reasons }
    }

    fn release(&self, rel_id: &str, market_id: &str, side: BookSide) {
        self.reserved
            .lock()
            .expect("reserved")
            .remove(&(rel_id.to_string(), market_id.to_string(), side));
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
        view_with_configs(&valid_exec(), topics, balances, oracle)
    }

    fn view_with_configs(
        exec: &str,
        topics: &str,
        balances: Vec<(&str, &str)>,
        oracle: &str,
    ) -> RiskView {
        let mut o = HashMap::new();
        o.insert("r1".to_string(), oracle.to_string());
        RiskView::load(
            exec,
            topics,
            balances.into_iter().map(|(a, b)| (a.to_string(), b.to_string())).collect(),
            o,
        )
    }

    fn write_cfg(tag: &str, name: &str, body: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir()
            .join(format!("arb-risk-test-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        let p = d.join(name);
        std::fs::write(&p, body).unwrap();
        p
    }

    fn write_topics(tag: &str, body: &str) -> std::path::PathBuf {
        write_cfg(tag, "topics.yaml", body)
    }

    fn write_exec(tag: &str, body: &str) -> std::path::PathBuf {
        write_cfg(tag, "exec.yaml", body)
    }

    /// A valid `exec.yaml` for the fixtures that are not ABOUT exec.yaml: an
    /// absent one is damage now, so every view needs a real file. Holds what the
    /// live `config/exec.yaml` holds, which is what the old fallback assumed.
    fn valid_exec() -> String {
        static P: std::sync::OnceLock<std::path::PathBuf> = std::sync::OnceLock::new();
        P.get_or_init(|| write_exec("exec-ok", "bankroll_usd: 980\nper_class_cap: 0.35\n"))
            .to_string_lossy()
            .into_owned()
    }

    /// FAIL-CLOSED: an unconfigured venue balance reads as $0 cash, so the
    /// order is refused rather than waved through. Getting this backwards would
    /// let a misconfigured engine trade with imaginary capital.
    #[test]
    fn no_balance_configured_refuses_every_order() {
        let v = view(vec![], "low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
        assert!(!d.allowed);
        assert!(d.reasons.iter().any(|r| r.contains("insufficient kalshi balance")), "{:?}", d.reasons);
    }

    #[test]
    fn a_funded_venue_within_caps_is_allowed() {
        let v = funded("low");
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
        assert!(d.allowed, "{:?}", d.reasons);
    }

    /// C13a. Quoting Kalshi obliges us to hedge on PM-US, so PM-US's cash is
    /// part of the decision. Before this, only the quoted leg was checked: the
    /// order rested, filled, and the hedge could not be bought at any price
    /// because the constraint was cash — a permanent naked short.
    #[test]
    fn the_hedge_legs_venue_is_cash_gated_too() {
        let v = view(vec![("kalshi", "340")], "low"); // PM-US starved
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
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
        let d = v.check(&rel("r1"), Venue::PolymarketUs, 5, None);
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
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        assert!(!v.check(&rel("r1"), Venue::PolymarketUs, 5, None).allowed);
        let both = funded("low");
        assert!(both.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        assert!(both.check(&rel("r1"), Venue::PolymarketUs, 5, None).allowed);
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
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        v.record_open("r1", "cross-venue-equivalent", 150.0);
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
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
                v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed,
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
        let d = v.check(&rel("unknown-rel"), Venue::Kalshi, 10, None);
        assert!(!d.allowed, "unknown => high risk => 0.25x cap: {:?}", d.reasons);
    }

    #[test]
    fn allowed_and_rejected_are_counted_for_the_stats_line() {
        let v = funded("low");
        v.check(&rel("r1"), Venue::Kalshi, 5, None);
        v.record_open("r1", "cross-venue-equivalent", 150.0); // fills the per-rel cap
        v.check(&rel("r1"), Venue::Kalshi, 5, None); // => refused
        assert_eq!(v.stats(), (1, 1));
    }

    // ---- committed-but-unfilled capital: the cap bounded the order entry
    //      RATE, not the money ----
    //
    // Every test above this line hands `rests_on: None`, which is the gate as a
    // pure read — the take-take path's shape, and what the whole gate used to
    // be. The maker path passes a slot, because a quote that RESTS commits
    // capital from the moment it is allowed.

    /// **N QUOTES CANNOT EACH SPEND THE SAME HEADROOM.**
    ///
    /// The class cap is $343 (980 x 0.35) and 335 contracts are already open,
    /// so there is room for exactly ONE clip of 5. `check` was a pure read, so
    /// the first quote passed at 335+5, rested — and the second saw the same
    /// 335, as did the third, and every (leg, side) of every quoting
    /// relationship. All of them were live at the venue simultaneously, so one
    /// wide move filled a cap's worth of orders through a cap that had refused
    /// nothing.
    #[test]
    fn a_second_quote_cannot_spend_the_headroom_the_first_is_holding() {
        let v = funded("low");
        v.record_open("already-open", "cross-venue-equivalent", 335.0);
        let first = v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid)));
        assert!(first.allowed, "{:?}", first.reasons);
        assert_eq!(v.reserved_ct(), 5.0, "an allowed quote holds its clip");

        let second = v.check(&rel("r2"), Venue::Kalshi, 5, Some(("K", BookSide::Bid)));
        assert!(!second.allowed, "335 open + 5 RESTING + 5 > 343: {:?}", second.reasons);
        assert!(
            second.reasons.iter().any(|r| r.contains("class cap")),
            "{:?}",
            second.reasons
        );
        assert_eq!(v.reserved_ct(), 5.0, "and a refusal reserves nothing");
    }

    /// ...and the four (leg, side) quotes of ONE relationship are four quotes
    /// too. This is the same defect inside a single `Quoter::on_book` call,
    /// where nothing between the checks could ever have moved the exposure —
    /// there is no fill in that window by construction.
    ///
    /// Clip 40 against the $150 per-relationship cap: three fit, the fourth
    /// does not.
    #[test]
    fn the_four_sides_of_one_relationship_cannot_each_spend_the_per_rel_cap() {
        let v = funded("low");
        let r = rel("r1");
        for (market, side) in
            [("K", BookSide::Bid), ("K", BookSide::Ask), ("P", BookSide::Bid)]
        {
            let d = v.check(&r, Venue::Kalshi, 40, Some((market, side)));
            assert!(d.allowed, "{market}/{}: {:?}", side.as_str(), d.reasons);
        }
        let fourth = v.check(&r, Venue::Kalshi, 40, Some(("P", BookSide::Ask)));
        assert!(!fourth.allowed, "3x40 resting + 40 > 150: {:?}", fourth.reasons);
        assert!(
            fourth.reasons.iter().any(|r| r.contains("per-relationship")),
            "{:?}",
            fourth.reasons
        );
        assert_eq!(v.reserved_ct(), 120.0);
    }

    /// RELEASE ON CANCEL — the path the quoter drives. A cap that never gives
    /// capital back ratchets shut and silently stops all trading, which is
    /// worse than the overshoot the reservation exists to stop.
    #[test]
    fn a_released_slot_gives_its_capital_back() {
        let v = funded("low");
        v.record_open("r1", "cross-venue-equivalent", 145.0); // 5 of headroom left
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        assert!(
            !v.check(&rel("r1"), Venue::Kalshi, 5, Some(("P", BookSide::Bid))).allowed,
            "the first quote is holding the last of the headroom"
        );

        v.release("r1", "K", BookSide::Bid);
        assert_eq!(v.reserved_ct(), 0.0);
        assert!(
            v.check(&rel("r1"), Venue::Kalshi, 5, Some(("P", BookSide::Bid))).allowed,
            "the headroom must come back when the quote does not"
        );
    }

    /// RELEASE ON REPRICE. An amend replaces the quote on its own slot, so the
    /// cap must not be asked for both. Charging it twice would refuse repricing
    /// exactly when the book is fullest — which is when a stale resting quote is
    /// most dangerous — and would leak a reservation per reprice besides.
    #[test]
    fn repricing_is_not_charged_for_the_quote_it_replaces() {
        let v = funded("low");
        v.record_open("r1", "cross-venue-equivalent", 145.0);
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);

        let amend = v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid)));
        assert!(amend.allowed, "an amend replaces its own quote: {:?}", amend.reasons);
        assert_eq!(v.reserved_ct(), 5.0, "and does not stack a second reservation");

        // ...while a DIFFERENT slot is a genuinely second quote.
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Ask))).allowed);
    }

    /// RELEASE ON FILL. Filled contracts stop being capital merely COMMITTED
    /// and become real exposure, so `consume` gives up exactly what filled —
    /// counting it in both places would refuse the same dollars twice, and
    /// releasing the whole slot would free the part still resting.
    #[test]
    fn a_fill_moves_capital_from_reserved_to_open_and_only_once() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        assert_eq!(v.reserved_ct(), 5.0);

        // 3 of 5 fill: 3 are open, 2 are still resting.
        v.record_open("r1", "cross-venue-equivalent", 3.0);
        v.consume("r1", "K", BookSide::Bid, 3.0);
        assert_eq!((v.open_ct("r1"), v.reserved_ct()), (3.0, 2.0));

        // ...and then the rest. Nothing is double counted at either step.
        v.record_open("r1", "cross-venue-equivalent", 2.0);
        v.consume("r1", "K", BookSide::Bid, 2.0);
        assert_eq!((v.open_ct("r1"), v.reserved_ct()), (5.0, 0.0));
    }

    /// A slot holding nothing is released harmlessly, and that is load-bearing
    /// rather than defensive: a fully filled quote's slot is already gone when
    /// the quoter next cancels it (the quoter has no fill path and still
    /// believes it rests), so this is the ordinary sequence, not an edge.
    #[test]
    fn releasing_a_slot_that_holds_nothing_is_harmless() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        v.consume("r1", "K", BookSide::Bid, 5.0);
        assert_eq!(v.reserved_ct(), 0.0);
        v.release("r1", "K", BookSide::Bid);
        v.release("r1", "never-quoted", BookSide::Ask);
        assert_eq!(v.reserved_ct(), 0.0);
    }

    /// A REFUSED check reserves nothing. The quoter consults the gate on every
    /// book event, so a refusal that reserved would ratchet a full cap shut in
    /// seconds and never reopen it.
    #[test]
    fn a_refused_check_reserves_nothing_so_a_full_cap_does_not_ratchet() {
        let v = funded("low");
        v.record_open("r1", "cross-venue-equivalent", 150.0); // per-rel cap, exactly
        for _ in 0..10 {
            assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, Some(("K", BookSide::Bid))).allowed);
        }
        assert_eq!(v.reserved_ct(), 0.0);
    }

    /// A marketable IOC does not rest, so it reserves nothing — nothing would
    /// ever release it. An IOC that does not fill dies at the venue and produces
    /// no cancel; take-take's own cooldown gate bounds the window instead.
    #[test]
    fn a_marketable_ioc_reserves_nothing() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        assert_eq!(v.reserved_ct(), 0.0);
    }

    // ---- the capital-scarcity signal: a whole-book numerator needs a
    //      whole-book denominator ----
    //
    // `utilization()` is what `crate::apr_bar` turns into the maker hurdle
    // installed on every quoter and reported as `maker_apr_bar`. It summed
    // EVERY class and divided by what ONE class may hold.

    /// **FIVE CLASSES CAN EACH FILL THEIR OWN BUDGET, SO THEIR SUM IS NOT A
    /// FRACTION OF ONE OF THEM.**
    ///
    /// The pure form of the defect, and the one that does not need a bad
    /// config to show up: two classes, neither past two thirds of its own $343,
    /// and the old denominator called the book 1.000 FULL and charged the
    /// 16%/yr ceiling. `by_class` is maintained for exactly this reason and was
    /// not consulted.
    #[test]
    fn a_book_spread_across_classes_is_not_full_because_two_classes_sum_past_one() {
        let v = funded("low");
        v.record_open("a", "cross-venue-equivalent", 200.0);
        v.record_open("b", "exclusive", 143.0);

        // 343 of the $490 this engine may deploy: 70%, not 100%.
        assert!(
            (v.utilization() - 343.0 / 490.0).abs() < 1e-9,
            "343 across two classes: {}",
            v.utilization()
        );
        assert!(
            crate::apr_bar(v.utilization()) < crate::APR_CEIL - 3.0,
            "a book with $147 unspent must not be charged the ceiling: {}",
            crate::apr_bar(v.utilization())
        );
        // ...and the per-class cap is untouched by that reading: it is still
        // checked per class, and it still has room here.
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
    }

    /// **THE LIVE 2026-07-31 BOOK IS NOT 94% OF ANYTHING.**
    ///
    /// `[risk] seeded 289 open contracts across 10 relationships`, plus the one
    /// undischarged hedge leg seeded after it — and every one of them is
    /// `cross-venue-equivalent`, so the live book is a single class and the old
    /// reading was wrong in SIZE rather than in kind. It divided 290 by that
    /// class's $343 and reported 0.845, charging 14.15%/yr, while $200 of the
    /// $490 this engine may deploy sat unspent. Against $490: 0.592, 11.10%/yr.
    ///
    /// The earlier snapshot of the same day, $322, is the second row: 0.939 and
    /// 15.27%/yr became 0.657 and 11.89%/yr.
    #[test]
    fn the_live_2026_07_31_book_reads_against_the_capital_it_may_deploy() {
        for (open, want_util, want_bar) in [(290.0, 0.5918, 11.102), (322.0, 0.6571, 11.886)] {
            let v = funded("low");
            assert_eq!(v.global_cap_usd(), 490.0, "$980 x GLOBAL_CAP 0.50");
            // The two biggest live relationships, then the rest in one lump —
            // the seed line prints only the top five.
            v.record_open("xvus-time-poty-26-zohranmamdani", "cross-venue-equivalent", 97.0);
            v.record_open("xvus-france-pres-27-brunoretailleau", "cross-venue-equivalent", 84.0);
            v.record_open("the-smaller-eight", "cross-venue-equivalent", open - 181.0);

            let util = v.utilization();
            assert!((util - want_util).abs() < 0.001, "{open} open reads {util}");
            let bar = crate::apr_bar(util);
            assert!((bar - want_bar).abs() < 0.01, "{open} open charges {bar}%/yr");
            assert!(
                util < open / 343.0,
                "the class budget is not the denominator any more: {util}"
            );
        }
    }

    /// **1.0 MEANS "THE NEXT CONTRACT IS REFUSED", AND THAT IS WHY THIS IS THE
    /// DENOMINATOR.**
    ///
    /// `arb_core::risk`'s global check refuses when
    /// `sum(by_relationship) + notional > bankroll * global_cap` — the same sum
    /// this divides, against the same ceiling — so the two agree at the edge.
    /// Under the class budget they could not: at 489 contracts the old reading
    /// was already clamped to 1.000 with $147 still admissible.
    #[test]
    fn utilization_reads_full_exactly_when_the_global_cap_has_no_headroom() {
        let v = funded("low");
        v.record_open("a", "cross-venue-equivalent", 200.0);
        v.record_open("b", "exclusive", 200.0);
        v.record_open("c", "implies", 89.0); // 489 of $490, no class near $343
        assert!(v.utilization() < 1.0, "a dollar of headroom is not a full book");
        assert!(
            v.check(&rel("r1"), Venue::Kalshi, 1, None).allowed,
            "and the cap agrees there is a dollar left"
        );

        v.record_open("c", "implies", 1.0); // 490 of $490
        assert_eq!(v.utilization(), 1.0);
        let d = v.check(&rel("r1"), Venue::Kalshi, 1, None);
        assert!(!d.allowed, "the cap agrees there is not");
        assert!(d.reasons.iter().any(|r| r.contains("global cap")), "{:?}", d.reasons);
    }

    /// **A CAP THAT DID NOT LOAD READS AS FULL, NEVER AS EMPTY.**
    ///
    /// Preserved across the denominator change, and the direction is the whole
    /// point: empty means "quote everything at the 4%/yr floor" on a config
    /// nobody can trust. `Caps::corrupt` forces the BANKROLL to "0" and every
    /// cap in this file is a fraction of it, so the global one is degenerate
    /// for the same four failures the class one was.
    #[test]
    fn a_cap_that_did_not_load_reads_as_full_not_empty() {
        let cases = [
            ("missing", None),
            ("unparseable", Some("bankroll_usd: 980\nper_class_cap: [0.35\n")),
            ("tail-cut", Some("bankroll_usd: 980\n")),
            ("zero-bankroll", Some("bankroll_usd: 0\nper_class_cap: 0.35\n")),
        ];
        for (tag, body) in cases {
            let p = match body {
                Some(b) => write_exec(&format!("util-{tag}"), b).to_string_lossy().into_owned(),
                None => "/nonexistent/exec.yaml".to_string(),
            };
            let v = view_with_configs(
                &p,
                "/nonexistent/topics.yaml",
                vec![("kalshi", "1000"), ("polymarket_us", "1000")],
                "low",
            );
            assert_eq!(v.global_cap_usd(), 0.0, "{tag}");
            // On an EMPTY book, which would otherwise be the 0.0 end of the
            // clamp — the fail-closed answer has to beat the arithmetic.
            assert_eq!(v.utilization(), 1.0, "{tag}: a $0 cap is not an empty book");
            assert_eq!(crate::apr_bar(v.utilization()), crate::APR_CEIL, "{tag}");
            if body.is_some() {
                let _ = std::fs::remove_dir_all(std::path::Path::new(&p).parent().unwrap());
            }
        }
    }

    /// Both ends of the clamp. An over-cap book asks the ceiling and no more —
    /// `apr_bar` clamps too, but a utilization above 1 would still be a lie in
    /// every gauge that prints it.
    #[test]
    fn utilization_clamps_at_both_ends() {
        let v = funded("low");
        assert_eq!(v.utilization(), 0.0, "an empty book is empty");

        for (i, class) in
            ["cross-venue-equivalent", "exclusive", "implies", "date-ladder", "partition"]
                .iter()
                .enumerate()
        {
            v.record_open(&format!("r{i}"), class, 100.0);
        }
        assert_eq!(v.utilization(), 1.0, "500 of $490 is 1.0, not 1.02");
        v.record_open("runaway", "exclusive", 10_000.0);
        assert_eq!(v.utilization(), 1.0, "and it stays there");
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
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
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
        assert!(!v.check(&r, Venue::Kalshi, 25, None).allowed, "40+25 > 60");

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
        let d = v.check(&r, Venue::Kalshi, 25, None);
        assert!(!d.allowed, "a dropped default must not widen the cap: {:?}", d.reasons);
        assert!(v.describe().contains("default_topic_budget"), "{}", v.describe());
        // and the catch-all `other` — where everything unlisted lands — too
        assert!(!v.check(&rel("xvus-unlisted-thing"), Venue::Kalshi, 5, None).allowed);
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
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
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
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// ABSENT is a configuration, not damage: no per-topic budgets, and the
    /// per-rel/class/global/cash caps all still apply. The engine must stay
    /// startable and tradable in that shape — the whole test suite above runs
    /// in it.
    #[test]
    fn an_absent_topics_file_is_a_legitimate_no_budget_config() {
        let v = funded("low");
        assert!(v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
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
        assert!(v.check(&r, Venue::Kalshi, 5, None).allowed);
        v.record_open("xvus-nobel-peace-26-djt", "cross-venue-equivalent", 80.0);
        let d = v.check(&r, Venue::Kalshi, 5, None);
        assert!(!d.allowed, "80 open against an $80 family budget: {:?}", d.reasons);
        assert!(
            d.reasons.iter().any(|r| r.contains("topic budget [nobel-peace-26]")),
            "{:?}",
            d.reasons
        );
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    // ---- config/exec.yaml must not widen a cap when it breaks either ----

    /// Every way of losing the exec caps lands here: no order passes, and the
    /// startup line does not read like a good load. Each of the four used to
    /// leave the compiled-in `980` / `0.35` — numerically identical to the live
    /// file, so nothing downstream could tell.
    fn assert_caps_failed_closed(exec: &std::path::Path) {
        let v = view_with_configs(
            exec.to_str().unwrap(),
            "/nonexistent/topics.yaml",
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        let d = v.check(&rel("r1"), Venue::Kalshi, 5, None);
        assert!(!d.allowed, "a cap that did not load must refuse: {:?}", d.reasons);
        assert!(d.reasons.iter().any(|r| r.contains("class cap")), "{:?}", d.reasons);
        let desc = v.describe();
        assert!(desc.contains("CAPITAL CAPS UNUSABLE"), "and say so: {desc}");
        assert!(!desc.contains("bankroll $980"), "the permissive default: {desc}");
        assert!(!desc.contains("per_class 0.35"), "the permissive default: {desc}");
    }

    /// Mode 1: exists, cannot be read. A DIRECTORY at the path gives EISDIR for
    /// any uid, where `chmod 000` is still readable as root.
    #[test]
    fn an_unreadable_exec_file_does_not_leave_the_permissive_default() {
        let d = std::env::temp_dir()
            .join(format!("arb-risk-test-{}-exec-unreadable", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        assert_caps_failed_closed(&d);
        let _ = std::fs::remove_dir_all(&d);
    }

    /// ...and the wrong working directory is the same failure with a friendlier
    /// cause. `exec.yaml` is TRACKED, so a correct checkout always has one:
    /// absent is damage here, unlike the gitignored topics.yaml.
    #[test]
    fn an_absent_exec_file_is_damage_not_a_default() {
        assert_caps_failed_closed(std::path::Path::new("/nonexistent/exec.yaml"));
    }

    /// Mode 2: caught mid-write. The file is five lines long, so the window is
    /// small but the whole file is inside it.
    #[test]
    fn an_unparseable_exec_file_does_not_leave_the_permissive_default() {
        let p = write_exec("exec-unparseable", "bankroll_usd: 980\nper_class_cap: [0.35\n");
        assert_caps_failed_closed(&p);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// Mode 3: the tail truncation, which still parses. `per_class_cap` is the
    /// LAST line of the live file — exactly the shape that already defeated the
    /// first version of the topics fix.
    #[test]
    fn an_exec_file_missing_per_class_cap_does_not_keep_the_old_one() {
        let p = write_exec("exec-tail-cut", "bankroll_usd: 500\n");
        assert_caps_failed_closed(&p);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// Mode 4, which needs no damage at all: `bankroll_usd: "500"` is an
    /// ordinary edit. The topic loader has always taken it; the exec loader
    /// dropped it on the floor and kept 980. Both spellings must now mean the
    /// same cap — which is the point of the shared `num`.
    #[test]
    fn a_quoted_number_is_the_same_cap_as_an_unquoted_one() {
        for (i, body) in [
            "bankroll_usd: 500\nper_class_cap: 0.35\n",
            "bankroll_usd: \"500\"\nper_class_cap: \"0.35\"\n",
        ]
        .iter()
        .enumerate()
        {
            let p = write_exec(&format!("exec-num-{i}"), body);
            let v = view_with_configs(
                p.to_str().unwrap(),
                "/nonexistent/topics.yaml",
                vec![("kalshi", "1000"), ("polymarket_us", "1000")],
                "low",
            );
            assert!(v.describe().contains("bankroll $500 per_class 0.35"), "{}", v.describe());
            // 500 * 0.35 = 175, so 170 open in the class leaves no room for 10.
            // The silently-kept 980 default would have allowed it: 343.
            v.record_open("other-rel", "cross-venue-equivalent", 170.0);
            let d = v.check(&rel("r1"), Venue::Kalshi, 10, None);
            assert!(!d.allowed, "the cap must be the file's: {:?}", d.reasons);
            assert!(
                d.reasons.iter().any(|r| r.contains("class cap") && r.contains("175")),
                "{:?}",
                d.reasons
            );
            let _ = std::fs::remove_dir_all(p.parent().unwrap());
        }
    }

    /// A value that is not a FINITE number is not a number in either loader.
    /// `none` reaches `arb_core::risk` as a decimal string and is parsed there
    /// under an `expect`, so accepting it would panic the gate mid-run; `.inf`
    /// and `nan` are worse, because libdecnumber accepts them — an infinite cap
    /// is no cap, and NaN refuses everything by accident rather than by design.
    #[test]
    fn a_value_that_is_not_a_finite_number_is_damage_not_a_cap() {
        for (i, v) in ["none", ".inf", "\"nan\"", "\"1e400\""].iter().enumerate() {
            let p = write_exec(
                &format!("exec-notanumber-{i}"),
                &format!("bankroll_usd: {v}\nper_class_cap: 0.35\n"),
            );
            assert_caps_failed_closed(&p);
            let _ = std::fs::remove_dir_all(p.parent().unwrap());
        }
    }

    /// `num` must TRIM what it returns, not merely what it validates: `"500 "`
    /// is a decimal to `f64::from_str` but not to libdecnumber, whose
    /// numeric-string syntax admits no surrounding whitespace — and the value is
    /// carried there as a string, under an `expect`.
    #[test]
    fn a_padded_number_is_trimmed_before_the_decimal_parser_sees_it() {
        let p = write_exec("exec-padded", "bankroll_usd: \"500 \"\nper_class_cap: \" 0.35\"\n");
        let v = view_with_configs(
            p.to_str().unwrap(),
            "/nonexistent/topics.yaml",
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(v.describe().contains("bankroll $500 per_class 0.35"), "{}", v.describe());
        // The check itself is the assertion: an untrimmed "500 " panics here.
        v.record_open("other-rel", "cross-venue-equivalent", 170.0);
        let d = v.check(&rel("r1"), Venue::Kalshi, 10, None);
        assert!(d.reasons.iter().any(|r| r.contains("class cap") && r.contains("175")), "{:?}", d.reasons);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    // ---- and `only_below_util`, where None is not damage but DELETION ----

    /// The gate is a RESTRICTION, so an unreadable one must not read as "this
    /// family has no gate": `arb_core::risk` applies it under `if let Some(..)`,
    /// and a None there deletes the utilisation check outright. `50%` is the
    /// typo the field's own units invite — it is documented as a fraction — and
    /// the live config/topics.yaml gates france-pres-27 exactly this way.
    #[test]
    fn an_unusable_only_below_util_is_damage_not_a_deleted_gate() {
        let p = write_topics(
            "gate-typo",
            "topics:\n  - {family: france-pres-27, budget_usd: 60, only_below_util: 50%}\n\
             default_topic_budget: 30\ndefault_only_below_util: 0.5\n",
        );
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(v.describe().contains("only_below_util"), "{}", v.describe());
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// Same for the catch-all's gate, which covers every family that does not
    /// set one — including `other`, where everything unlisted lands.
    #[test]
    fn an_unusable_default_only_below_util_is_damage_too() {
        let p = write_topics(
            "default-gate-typo",
            "topics:\n  - {family: france-pres-27, budget_usd: 60}\n\
             default_topic_budget: 30\ndefault_only_below_util: 50%\n",
        );
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(v.describe().contains("default_only_below_util"), "{}", v.describe());
        assert!(!v.check(&rel("r1"), Venue::Kalshi, 5, None).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }

    /// ...but an ABSENT gate is still a configuration, not damage: most families
    /// in the live file have no `only_below_util` of their own.
    #[test]
    fn an_absent_only_below_util_is_still_just_no_gate() {
        let p = write_topics(
            "gate-absent",
            "topics:\n  - {family: nobel-peace-26, budget_usd: 80}\ndefault_topic_budget: 30\n",
        );
        let v = view_with_topics(
            p.to_str().unwrap(),
            vec![("kalshi", "1000"), ("polymarket_us", "1000")],
            "low",
        );
        assert!(!v.describe().contains("UNUSABLE"), "{}", v.describe());
        assert!(v.check(&rel("xvus-nobel-peace-26-djt"), Venue::Kalshi, 5, None).allowed);
        let _ = std::fs::remove_dir_all(p.parent().unwrap());
    }
}
