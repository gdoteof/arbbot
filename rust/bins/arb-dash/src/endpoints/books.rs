//! The double-entry statement, and the claims a venue could contradict.
//!
//! Four steps, in this order: import each venue's snapshot into a fresh
//! journal, build the reconciliation rows, age the provenance, assemble. The
//! journal is thrown away at the end of the request — see `json`.

use arb_core::clock::now_secs;
use arb_core::scan::Cx;
use arb_ledger::kalshi::{Deposit, Fill, KalshiImport, Settlement};
use arb_ledger::pmus::{self, Balances, PmusImport, Position};
use arb_ledger::{accounts, report, Journal};

use crate::endpoints::age_secs;
use crate::Args;

/// A missing or unparseable snapshot is an ERROR, never an empty list. Books
/// built from files that are not there balance perfectly and mean nothing: the
/// old `unwrap_or_default()` turned "the fills file is gone" into a clean,
/// zeroed, trial-balance-ZERO statement — the most convincing possible way to
/// show numbers that are not true.
fn read_json<T: serde::de::DeserializeOwned>(path: &str) -> Result<T, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{path}: {e}"))?;
    serde_json::from_str(&text).map_err(|e| format!("{path}: {e}"))
}

/// A venue snapshot older than this is not evidence of anything current. The
/// 2026-07-28 audit found both reconciliation rows reading EXACT over snapshots
/// 20.2 hours old, which is a third state — frozen — not a pass.
const SNAPSHOT_STALE_S: u64 = 3600;

/// One row of the reconciliation table, with an explicit claim about whether it
/// is a check at all.
///
/// `arb-ledger`'s `report.rs` says of `reconcile()`: *"This is the only claim
/// the dashboard makes that a venue can contradict, so it is explicit."* Two of
/// the rows this dashboard asked it for could not be contradicted by anything
/// (2026-07-28 audit): `cash:kalshi` is compared against `--kalshi-balance`, a
/// constant typed at startup, and `cash:pmus` against the very `buyingPower`
/// that `PmusImport::apply`'s plug entry forces it to equal. Both read EXACT
/// for every possible input — the tautology `arb-ledger`'s own crate header
/// says the crate exists to kill.
///
/// They are kept, because the books figure beside the venue figure is worth
/// seeing, but they now carry `falsifiable: false` and render as NOT A CHECK,
/// and rows that genuinely can fail are computed next to them.
fn recon_row(
    cx: &mut Cx,
    account: &str,
    books: &str,
    venue: &str,
    falsifiable: bool,
    why: &str,
) -> serde_json::Value {
    let b = cx.parse_exact(books);
    let v = cx.parse_exact(venue);
    let d = cx.sub(b, v);
    let exact = arb_ledger::is_zero(cx, d);
    serde_json::json!({
        "account": account,
        "books": b.to_string(),
        "venue": v.to_string(),
        "diff": d.to_string(),
        "exact": exact,
        "falsifiable": falsifiable,
        "why": why,
    })
}

/// The Kalshi snapshot, into the journal. Pushes one provenance row per file
/// the reconciliation leans on.
fn import_kalshi(
    a: &Args,
    cx: &mut Cx,
    j: &mut Journal,
    sources: &mut Vec<serde_json::Value>,
) -> Result<(), String> {
    let kd = format!("{}/kalshi_deposits.json", a.kalshi_dir);
    let kf = format!("{}/kalshi_fills.json", a.kalshi_dir);
    let ks = format!("{}/kalshi_settlements.json", a.kalshi_dir);
    let (deposits, fills, settlements): (Vec<Deposit>, Vec<Fill>, Vec<Settlement>) =
        match (read_json(&kd), read_json(&kf), read_json(&ks)) {
            (Ok(d), Ok(f), Ok(s)) => (d, f, s),
            (Err(e), _, _) | (_, Err(e), _) | (_, _, Err(e)) => {
                return Err(format!("kalshi snapshot unreadable — {}", e.replace('"', "'")))
            }
        };
    if let Err(e) = (KalshiImport { deposits, fills, settlements }).apply(cx, j) {
        return Err(format!("kalshi import: {e}"));
    }

    // Provenance, one row per number the reconciliation leans on.
    sources.push(serde_json::json!({ "what": "kalshi fills", "from": kf, "age_s": age_secs(&kf) }));
    sources.push(
        serde_json::json!({ "what": "kalshi settlements", "from": ks, "age_s": age_secs(&ks) }),
    );
    sources
        .push(serde_json::json!({ "what": "kalshi deposits", "from": kd, "age_s": age_secs(&kd) }));
    Ok(())
}

/// The PM-US snapshot, into the journal.
///
/// The four figures come back as strings so the reconciliation can be built
/// after the import has consumed the snapshot. `short_contracts` must be
/// computed BEFORE the positions move into `PmusImport`.
///
/// `None` when no `--pmus-dir` was given; the Kalshi half of the statement
/// stands on its own.
type PmusFigures = (String, String, String, String);
fn import_pmus(
    a: &Args,
    cx: &mut Cx,
    j: &mut Journal,
    sources: &mut Vec<serde_json::Value>,
) -> Result<Option<PmusFigures>, String> {
    if a.pmus_dir.is_empty() {
        return Ok(None);
    }
    let pb = format!("{}/pmus_balances.json", a.pmus_dir);
    let pp = format!("{}/pmus_positions.json", a.pmus_dir);
    let (balances, positions): (Balances, Vec<Position>) =
        match (read_json(&pb), read_json(&pp)) {
            (Ok(b), Ok(p)) => (b, p),
            (Err(e), _) | (_, Err(e)) => {
                // Silently skipping this used to drop half the portfolio
                // and its reconciliation row, leaving a statement that
                // looked complete.
                return Err(format!("pmus snapshot unreadable — {}", e.replace('"', "'")))
            }
        };
    let shorts = pmus::short_contracts(cx, &positions).to_string();
    let figures = (
        balances.buying_power_str(),
        balances.current_balance_str(),
        balances.margin_requirement_str(),
        shorts,
    );
    sources.push(serde_json::json!({ "what": "pm-us balances", "from": pb,
                                     "age_s": age_secs(&pb) }));
    sources.push(serde_json::json!({ "what": "pm-us positions", "from": pp,
                                     "age_s": age_secs(&pp) }));
    if let Err(e) = (PmusImport { deposits_usd: a.pmus_deposits.clone(), balances, positions })
        .apply(cx, j)
    {
        return Err(format!("pmus import: {e}"));
    }
    Ok(Some(figures))
}

/// The reconciliation table. Two of these rows are displays and one is a real
/// check; each says which it is, and why, in its own `why`.
fn recon_checks(
    a: &Args,
    cx: &mut Cx,
    j: &Journal,
    pmus: &Option<PmusFigures>,
    sources: &mut Vec<serde_json::Value>,
) -> Vec<serde_json::Value> {
    let mut checks: Vec<serde_json::Value> = Vec::new();
    if let Some(kb) = &a.kalshi_balance {
        let books = j.balance(cx, accounts::CASH_KALSHI).to_string();
        checks.push(recon_row(
            cx,
            accounts::CASH_KALSHI,
            &books,
            kb,
            false,
            "compared against --kalshi-balance, a constant typed at startup. A number that \
             cannot move cannot disagree, so this is a display of the books figure, not a \
             check. Re-pull the venue balance and restart to actually check it.",
        ));
        sources.push(serde_json::json!({
            "what": "kalshi cash balance",
            "from": "--kalshi-balance (fixed at startup)",
            "age_s": now_secs().saturating_sub(a.started_at),
            "frozen": true,
        }));
    }
    if let Some((bp, cb, mr, shorts)) = pmus {
        let books = j.balance(cx, accounts::CASH_PMUS).to_string();
        checks.push(recon_row(
            cx,
            accounts::CASH_PMUS,
            &books,
            bp,
            false,
            "PmusImport::apply posts pmus:opening:unitemized-pnl for exactly the gap between \
             the books and buyingPower (PM-US publishes no trade history), so cash:pmus is \
             forced equal to buyingPower for every possible input. Making this a real check \
             means removing that plug, which lives in arb-ledger.",
        ));
        // NOT A CHECK, though it looks like one. I first shipped this as
        // falsifiable because the three fields are reported separately — but
        // docs/venue-quirks.md:273 documents and verifies
        // `buyingPower = currentBalance - marginRequirement` as a PM-US
        // IDENTITY, and the books side is plug-forced to buyingPower. So this
        // compares buyingPower to buyingPower through a venue identity and
        // cannot fail on any error in our books. Worse, it cries wolf: the
        // identity has only ever been observed with openOrders, unsettledFunds
        // and pendingCredit all zero, and with resting quotes up PM-US reports
        // `buyingPower = currentBalance - marginRequirement - openOrders`, which
        // would report a failure and blame the snapshot for the ordinary state
        // of a market maker. Accounting for those three fields means teaching
        // `arb_ledger::pmus::Balances` to deserialize them.
        let derived = {
            let c = cx.parse_exact(cb);
            let m = cx.parse_exact(mr);
            cx.sub(c, m).to_string()
        };
        checks.push(recon_row(
            cx,
            "cash:pmus vs currentBalance − marginRequirement",
            &books,
            &derived,
            false,
            "a PM-US identity (documented in docs/venue-quirks.md), and the books side is the \
             plug-forced buyingPower, so this compares buyingPower to itself and cannot fail on \
             an error in our books. It also ignores openOrders, unsettledFunds and pendingCredit, \
             which PM-US subtracts from buyingPower — shown for provenance only.",
        ));
        // FALSIFIABLE. PM-US withholds $1.00 of collateral per short contract,
        // so |short contracts| must equal marginRequirement exactly. This is
        // the one accounting cross-check in the codebase that can genuinely
        // fail; it existed only in the hand-run arb-books CLI, which has no
        // unit, so nothing was watching it.
        checks.push(recon_row(
            cx,
            "pmus short contracts vs marginRequirement",
            shorts,
            mr,
            true,
            "PM-US withholds $1.00 per short contract, so these must match exactly. A gap \
             means the positions snapshot is incomplete — some short is missing from the \
             books but is still costing collateral.",
        ));
    }
    checks
}

/// Age every provenance row that has a file behind it, and report the oldest.
///
/// Stale is a third state, not a pass: a green EXACT over a 20-hour-old
/// snapshot says nothing about the account now.
fn age_sources(sources: &mut [serde_json::Value]) -> Option<u64> {
    let mut snapshot_age: Option<u64> = None;
    for s in sources.iter_mut() {
        if s.get("frozen").is_some() {
            continue;
        }
        let age = s.get("age_s").and_then(|v| v.as_u64());
        s["stale"] = serde_json::json!(age.map(|x| x > SNAPSHOT_STALE_S));
        if let Some(x) = age {
            snapshot_age = Some(snapshot_age.map_or(x, |m: u64| m.max(x)));
        }
    }
    snapshot_age
}

/// Rebuild the books from venue snapshots on every request. The whole import is
/// ~250 entries and takes microseconds; holding no state means the page can
/// never show a number the files no longer support.
pub fn json(a: &Args) -> String {
    let mut cx = Cx::default();
    let mut j = Journal::new();
    let mut sources: Vec<serde_json::Value> = Vec::new();

    if let Err(e) = import_kalshi(a, &mut cx, &mut j, &mut sources) {
        return format!("{{\"error\":\"{e}\"}}");
    }
    let pmus = match import_pmus(a, &mut cx, &mut j, &mut sources) {
        Ok(p) => p,
        Err(e) => return format!("{{\"error\":\"{e}\"}}"),
    };

    let rep = report::build(&mut cx, &j);
    let checks = recon_checks(a, &mut cx, &j, &pmus, &mut sources);
    sources.push(serde_json::json!({
        "what": "pm-us capital in",
        "from": "--pmus-deposits (fixed at startup)",
        "age_s": now_secs().saturating_sub(a.started_at),
        "frozen": true,
    }));
    let snapshot_age = age_sources(&mut sources);

    let falsifiable = checks.iter().filter(|c| c["falsifiable"] == true).count();
    let failed =
        checks.iter().filter(|c| c["falsifiable"] == true && c["exact"] == false).count();

    let mut v = serde_json::to_value(&rep).unwrap_or_else(|_| serde_json::json!({}));
    v["reconciliations"] = serde_json::Value::Array(checks);
    v["checks_falsifiable"] = serde_json::json!(falsifiable);
    v["checks_failed"] = serde_json::json!(failed);
    v["snapshot_age_s"] = serde_json::json!(snapshot_age);
    v["snapshot_stale"] = serde_json::json!(snapshot_age.map(|x| x > SNAPSHOT_STALE_S));
    v["sources"] = serde_json::Value::Array(sources);
    v.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// PM-US's four figures in the order `import_pmus` returns them:
    /// buyingPower, currentBalance, marginRequirement, |short contracts|.
    fn pmus_figures(bp: &str, cb: &str, mr: &str, shorts: &str) -> Option<PmusFigures> {
        Some((bp.into(), cb.into(), mr.into(), shorts.into()))
    }

    fn checks(a: &Args, pmus: &Option<PmusFigures>) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
        let mut cx = Cx::default();
        let j = Journal::new();
        let mut sources = Vec::new();
        let out = recon_checks(a, &mut cx, &j, pmus, &mut sources);
        (out, sources)
    }

    fn row<'a>(checks: &'a [serde_json::Value], account: &str) -> &'a serde_json::Value {
        checks.iter().find(|c| c["account"] == account).expect("reconciliation row")
    }

    /// The 2026-07-28 audit found both reconciliation rows reading EXACT and
    /// neither of them able to read anything else. `cash:kalshi` is compared
    /// against a constant typed at startup; `cash:pmus` against the very
    /// buyingPower that `PmusImport::apply`'s plug entry forces it to equal.
    /// They are kept because the figures are worth seeing, but a page that
    /// renders them as passing checks is a page that manufactures confidence.
    #[test]
    fn the_two_cash_rows_declare_themselves_not_checks() {
        let mut a = Args::for_test();
        a.kalshi_balance = Some("123.45".into());
        let (out, _) = checks(&a, &pmus_figures("10.00", "11.00", "1.00", "1.00"));
        assert_eq!(row(&out, accounts::CASH_KALSHI)["falsifiable"], false);
        assert_eq!(row(&out, accounts::CASH_PMUS)["falsifiable"], false);
        assert_eq!(
            row(&out, "cash:pmus vs currentBalance − marginRequirement")["falsifiable"],
            false,
            "a PM-US identity compared to itself through a plug entry"
        );
        for c in &out {
            assert!(
                c["why"].as_str().is_some_and(|w| w.len() > 40),
                "{} carries no explanation of what it is",
                c["account"]
            );
        }
    }

    /// The one accounting cross-check in this codebase that can genuinely
    /// fail. PM-US withholds $1.00 of collateral per short contract, so
    /// |shorts| must equal marginRequirement exactly — and this existed only
    /// in the hand-run `arb-books` CLI, which has no unit, so nothing was
    /// watching it.
    #[test]
    fn the_short_contracts_row_is_the_one_that_can_fail() {
        let a = Args::for_test();
        let (out, _) = checks(&a, &pmus_figures("10.00", "51.00", "41.00", "41.00"));
        let c = row(&out, "pmus short contracts vs marginRequirement");
        assert_eq!(c["falsifiable"], true);
        assert_eq!(c["exact"], true, "41 shorts against $41.00 of margin");
        assert_eq!(out.iter().filter(|c| c["falsifiable"] == true).count(), 1);
    }

    /// A short missing from the positions snapshot is still costing collateral
    /// at the venue, so the gap shows up here and nowhere else.
    #[test]
    fn a_short_missing_from_the_snapshot_fails_the_check() {
        let a = Args::for_test();
        let (out, _) = checks(&a, &pmus_figures("10.00", "51.00", "41.00", "40.00"));
        let c = row(&out, "pmus short contracts vs marginRequirement");
        assert_eq!(c["exact"], false);
        assert_eq!(c["diff"], "-1.00", "one contract short of the margin PM-US is holding");
    }

    /// With no PM-US snapshot the Kalshi half stands on its own — but nothing
    /// falsifiable is left, and the payload must be able to say so rather than
    /// showing a green statement built from one unfalsifiable row.
    #[test]
    fn without_a_pmus_snapshot_nothing_falsifiable_remains() {
        let mut a = Args::for_test();
        a.kalshi_balance = Some("123.45".into());
        let (out, _) = checks(&a, &None);
        assert_eq!(out.len(), 1);
        assert_eq!(out.iter().filter(|c| c["falsifiable"] == true).count(), 0);
    }

    /// No `--kalshi-balance` means no row at all, not a row against zero: a
    /// books balance compared to an absent venue figure would read as a
    /// catastrophic discrepancy on a set of books that are fine.
    #[test]
    fn an_absent_kalshi_balance_produces_no_row_rather_than_a_zero() {
        let a = Args::for_test();
        let (out, _) = checks(&a, &None);
        assert!(out.is_empty());
    }

    /// `--kalshi-balance` is typed once at startup and never re-read, so its
    /// provenance row must say `frozen` — that flag is what keeps `age_sources`
    /// from aging it against a file mtime it does not have.
    #[test]
    fn the_typed_kalshi_balance_is_marked_frozen_in_provenance() {
        let mut a = Args::for_test();
        a.kalshi_balance = Some("123.45".into());
        let (_, sources) = checks(&a, &None);
        assert_eq!(sources.len(), 1);
        assert_eq!(sources[0]["frozen"], true);
    }

    // ----- provenance ageing ----------------------------------------------

    /// Stale is a THIRD state, not a pass. A green EXACT over a 20.2-hour-old
    /// snapshot — the state the audit actually found — says nothing about the
    /// account now, and the oldest source is what the page reports.
    #[test]
    fn the_reported_snapshot_age_is_the_oldest_source() {
        let mut sources = vec![
            serde_json::json!({ "what": "kalshi fills", "age_s": 30 }),
            serde_json::json!({ "what": "pm-us positions", "age_s": 72_000 }),
        ];
        assert_eq!(age_sources(&mut sources), Some(72_000));
        assert_eq!(sources[0]["stale"], false);
        assert_eq!(sources[1]["stale"], true);
    }

    /// One hour is the line, and it is exclusive: a snapshot pulled exactly an
    /// hour ago is still evidence.
    #[test]
    fn a_snapshot_is_stale_past_an_hour() {
        let at = |age: u64| {
            let mut s = vec![serde_json::json!({ "what": "x", "age_s": age })];
            age_sources(&mut s);
            s[0]["stale"].clone()
        };
        assert_eq!(at(SNAPSHOT_STALE_S), false);
        assert_eq!(at(SNAPSHOT_STALE_S + 1), true);
    }

    /// A figure typed at startup has no file behind it, so ageing it against
    /// this process's uptime would report a fresh dashboard as having fresh
    /// venue data. It is skipped entirely and cannot pull the headline age.
    #[test]
    fn a_frozen_source_is_not_aged_and_does_not_move_the_headline() {
        let mut sources = vec![
            serde_json::json!({ "what": "kalshi fills", "age_s": 30 }),
            serde_json::json!({ "what": "kalshi cash balance", "age_s": 99_999, "frozen": true }),
        ];
        assert_eq!(age_sources(&mut sources), Some(30));
        assert!(sources[1].get("stale").is_none(), "a frozen row carries no staleness verdict");
    }

    /// A source whose file could not be stat-ed has an UNKNOWN age, and
    /// unknown must not render as fresh. `null` is the honest answer.
    #[test]
    fn a_source_with_no_age_is_unknown_not_fresh() {
        let mut sources = vec![serde_json::json!({ "what": "pm-us balances", "age_s": null })];
        assert_eq!(age_sources(&mut sources), None);
        assert!(sources[0]["stale"].is_null());
    }
}
