//! arb-exitability — how likely each pair is to let the engine sell a lot back
//! before resolution, how soon, and for how much. The nightly refit behind the
//! dashboard's Exitability view.
//!
//! The entry bar prices every lot as held to resolution. A pair whose basis
//! keeps round-tripping is worth far more than that, because the lot is sold
//! back in days and the capital goes round again. This measures it per pair:
//!
//!   1. replay the archived books of the last `--days` days into minute grids;
//!   2. simulate every maker entry the tape would have filled, and each hour
//!      after it in which the engine's exit rule could have sold it (`sim`);
//!   3. replay our own lots the same way and measure what share of them get
//!      out at all, and how often one of those takes a chance it is offered
//!      (`fit::calibrate`);
//!   4. turn the entries into a calibrated, shrunk p per pair (`fit`).
//!
//! Reads only: the archive, the registry and the ledger. Writes
//! `<out>/latest.json` (when the run is the newest one) and appends one line to
//! `<out>/history.jsonl`. It places no orders and decides nothing — the entry
//! bar does not read it.
//!
//!   arb-exitability [--as-of YYYY-MM-DD] [--days 28] [--hurdle 50]
//!                   [--parquet-dir data/parquet] [--raw-dir data/raw]
//!                   [--registry config/registry.yaml]
//!                   [--ledger data/exec/trades.jsonl] [--out-dir data/exitability]
//!
//! `--as-of` defaults to yesterday (UTC), the newest closed day; passing older
//! days backfills the history.

mod fit;
mod lots;
mod sim;
mod tape;

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::io::Write;
use std::process::exit;

use arb_core::model::Venue;
use arb_core::resolve::{iso_from_day, parse_iso, resolve_date};
use serde_json::{json, Value};

use crate::fit::Obs;
use crate::sim::{Dir, Entry, PairSim};
use crate::tape::{DAY_MINUTES, MINUTE_NS};

// The engine's ledger reader, compiled in as `arb-settle` does: corrections
// must fold here exactly as they fold for the engine.
#[allow(dead_code)]
#[path = "../../arb-trader/src/ledger.rs"]
mod ledger;

/// Simulation stops this many days before the resolve date. Kalshi closes a
/// release-driven market at the print and the archive then carries its last
/// book forward; the dates in `arb_core::resolve` sit a day or two after the
/// event, so two days keeps a frozen book from reading as an exit.
const RESOLVE_GUARD_DAYS: i64 = 2;
/// Per pair, how many of the newest simulated entries the drill-down lists.
const RECENT: usize = 40;

struct Args {
    as_of: Option<String>,
    days: usize,
    hurdle_pct: f64,
    parquet_dir: String,
    raw_dir: String,
    registry: String,
    ledger: String,
    out_dir: String,
}

fn parse_args() -> Args {
    let mut a = Args {
        as_of: None,
        days: 28,
        hurdle_pct: 50.0,
        parquet_dir: "data/parquet".into(),
        raw_dir: "data/raw".into(),
        registry: "config/registry.yaml".into(),
        ledger: "data/exec/trades.jsonl".into(),
        out_dir: "data/exitability".into(),
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < argv.len() {
        let v = argv.get(i + 1).cloned().unwrap_or_default();
        match argv[i].as_str() {
            "--as-of" => a.as_of = Some(v),
            "--days" => a.days = v.parse().unwrap_or(a.days),
            "--hurdle" => a.hurdle_pct = v.parse().unwrap_or(a.hurdle_pct),
            "--parquet-dir" => a.parquet_dir = v,
            "--raw-dir" => a.raw_dir = v,
            "--registry" => a.registry = v,
            "--ledger" => a.ledger = v,
            "--out-dir" => a.out_dir = v,
            other => {
                eprintln!("unknown argument {other}");
                exit(2);
            }
        }
        i += 2;
    }
    if a.days < 2 {
        eprintln!("--days must be at least 2");
        exit(2);
    }
    a
}

struct Pair {
    rel: String,
    family: String,
    kalshi: String,
    pmus: String,
    vetted_by: Option<String>,
    verdict: Option<String>,
    resolves: Option<&'static str>,
}

/// The family a pair is pooled with: its id without the last segment
/// (`xvus-time-poty-26-zohranmamdani` -> `xvus-time-poty-26`).
fn family_of(rel: &str) -> String {
    rel.rsplit_once('-').map_or(rel, |(f, _)| f).to_string()
}

/// Every non-rejected Kalshi + PM-US pair whose legs are both YES.
fn pairs(registry: &str) -> Result<Vec<Pair>, String> {
    let reg = arb_registry::Registry::load(registry)?;
    Ok(reg
        .relationships
        .iter()
        .filter(|r| r.verdict.as_deref() != Some("rejected") && r.legs.len() == 2)
        .filter(|r| r.legs.iter().all(|l| l.side.as_deref() == Some("yes")))
        .filter_map(|r| {
            let leg = |v: &str| r.legs.iter().find(|l| l.venue == v).map(|l| l.market_id.clone());
            Some(Pair {
                rel: r.id.clone(),
                family: family_of(&r.id),
                kalshi: leg("kalshi")?,
                pmus: leg("polymarket_us")?,
                vetted_by: r.vetted_by.clone(),
                verdict: r.verdict.clone(),
                resolves: resolve_date(&r.id).map(|(d, _)| d),
            })
        })
        .collect())
}

/// Which exit an observation counts.
#[derive(Clone, Copy)]
enum Model {
    /// The engine's rule, maker or cross.
    Engine,
    /// The engine's rule, crossing both legs only.
    Cross,
    /// $1.00 or better: the bar a par exit would set.
    Par,
}

fn ob(e: &Entry, model: Model) -> Obs<'_> {
    Obs {
        chances: match model {
            Model::Engine => &e.chances,
            Model::Cross => &e.chances_cross,
            Model::Par => &e.chances_par,
        },
        cost: e.cost,
        watched: e.watched as f64,
    }
}

fn obs(entries: &[Entry], model: Model) -> Vec<Obs<'_>> {
    entries.iter().map(|e| ob(e, model)).collect()
}

fn entry_days<'a>(entries: impl IntoIterator<Item = &'a Entry>) -> f64 {
    entries.into_iter().map(|e| e.t / DAY_MINUTES).collect::<BTreeSet<_>>().len() as f64
}

/// One pair's fit under one reading of its entries.
struct Shrunk {
    own: fit::Survival,
    p: f64,
    p_lcb: f64,
}

fn round(x: f64, places: i32) -> f64 {
    let m = 10f64.powi(places);
    // `+ 0.0` turns a negative zero (an empty float sum) into a plain one.
    (x * m).round() / m + 0.0
}

fn opt(x: Option<f64>, places: i32) -> Value {
    x.filter(|v| v.is_finite()).map_or(Value::Null, |v| json!(round(v, places)))
}

fn median(mut xs: Vec<f64>) -> Option<f64> {
    xs.sort_by(f64::total_cmp);
    (!xs.is_empty()).then(|| xs[xs.len() / 2])
}

fn write_atomic(path: &str, body: &str) -> Result<(), String> {
    let tmp = format!("{path}.tmp");
    std::fs::write(&tmp, body).map_err(|e| format!("write {tmp}: {e}"))?;
    std::fs::rename(&tmp, path).map_err(|e| format!("rename {tmp}: {e}"))
}

fn run(a: &Args) -> Result<String, String> {
    let last_day = match &a.as_of {
        Some(d) => parse_iso(d).ok_or_else(|| format!("--as-of {d} is not YYYY-MM-DD"))?,
        None => (arb_core::clock::now_secs() / 86_400) as i64 - 1,
    };
    let first_day = last_day - a.days as i64 + 1;
    let days: Vec<String> = (first_day..=last_day).map(iso_from_day).collect();
    let as_of = iso_from_day(last_day);
    let t0_ns = first_day * 86_400 * 1_000_000_000;
    let n = days.len() * DAY_MINUTES;

    let pairs = pairs(&a.registry)?;
    let markets = |pick: fn(&Pair) -> &String| pairs.iter().map(|p| pick(p).clone()).collect::<HashSet<_>>();
    let (kalshi_markets, pmus_markets) = (markets(|p| &p.kalshi), markets(|p| &p.pmus));
    let (kalshi, pmus) = std::thread::scope(|s| {
        let k = s.spawn(|| {
            tape::load_venue(Venue::Kalshi, &days, &a.raw_dir, &a.parquet_dir, &kalshi_markets, t0_ns)
        });
        let p = s.spawn(|| {
            tape::load_venue(Venue::PolymarketUs, &days, &a.raw_dir, &a.parquet_dir, &pmus_markets, t0_ns)
        });
        (k.join(), p.join())
    });
    let (kalshi, k_missing) = kalshi.map_err(|_| "kalshi reader panicked")??;
    let (pmus, p_missing) = pmus.map_err(|_| "polymarket_us reader panicked")??;

    let hurdle = a.hurdle_pct / 100.0;
    // Minutes from the window's start to a pair's resolve, and what holding to
    // it is worth at minute t (`maker_exit::reservation_floor`). A pair with no
    // resolve rule has no hold value, so only the profit floors bind.
    let resolve_min = |p: &Pair| p.resolves.and_then(parse_iso).map(|d| (d - first_day) * DAY_MINUTES as i64);
    let hold_for = |p: &Pair| {
        let res = resolve_min(p);
        move |t: usize| res.map_or(0.0, |r| 1.0 / (1.0 + hurdle * ((r - t as i64).max(0) as f64 / 1440.0) / 365.25))
    };
    let sims: Vec<(&Pair, PairSim)> = pairs
        .iter()
        .filter_map(|p| {
            let (k, m) = (kalshi.get(&p.kalshi)?, pmus.get(&p.pmus)?);
            let end = resolve_min(p)
                .map_or(n as i64, |r| r - RESOLVE_GUARD_DAYS * DAY_MINUTES as i64)
                .clamp(0, n as i64) as usize;
            let k_maker = if arb_core::fees::kalshi_charges_makers(&p.kalshi) { sim::KALSHI_MAKER } else { 0.0 };
            Some((p, sim::simulate(k, m, k_maker, end, fit::HORIZON_MIN as usize, hold_for(p))))
        })
        .filter(|(_, s)| s.valid_minutes > 0)
        .collect();

    // Calibration: our own lots entered inside the window, against the tape.
    // Each counts the exit chances the tape offered it: up to and including
    // the one it took, for a lot out by a maker exit before the window
    // closed; and up to now, or to when it closed, for one not out that way,
    // whether still held, settled or closed by some other path. Leaving those
    // out would measure only the lots that were quick.
    let records = ledger::apply_corrections(ledger::read(&a.ledger)?);
    let window_end_s = (t0_ns as f64 + n as f64 * MINUTE_NS as f64) / 1e9;
    let mut calib: Vec<(usize, bool)> = Vec::new();
    let (mut held, mut closed) = (0, 0);
    let by_rel: HashMap<&str, &(&Pair, PairSim)> = sims.iter().map(|x| (x.0.rel.as_str(), x)).collect();
    for lot in lots::lots(&records) {
        let Some((pair, s)) = by_rel.get(lot.rel.as_str()) else { continue };
        let t0 = ((lot.ts * 1e9 - t0_ns as f64) / MINUTE_NS as f64).floor();
        if t0 < 0.0 || lot.ts >= window_end_s {
            continue;
        }
        let (out_at, exact) = match lot.exit_ts {
            Some(x) if x <= window_end_s => (x, true),
            _ => (lot.closed_ts.unwrap_or(f64::INFINITY).min(window_end_s), false),
        };
        let limit = ((out_at - lot.ts) / 60.0).max(0.0) as usize;
        let offered = s.engine_chances(lot.dir, t0 as usize, lot.cost, limit, hold_for(pair)).len();
        // A lot out before the tape's first chance took one the tape did not see.
        let offered = if exact { offered.max(1) } else { offered };
        if offered == 0 {
            continue;
        }
        calib.push((offered, exact));
        if !exact {
            if lot.closed_ts.is_some_and(|c| c <= window_end_s) { closed += 1 } else { held += 1 }
        }
    }
    let cal = fit::calibrate(&calib);
    let c = cal.take;

    // Each pair's p over the entries `keep` admits, shrunk toward its family
    // WITHOUT it (shrinking toward a family that contains it counts its own
    // evidence twice, entirely so in a family of one), and that toward
    // everything.
    let shrunk = |keep: fn(&Entry) -> bool| -> Vec<Shrunk> {
        let kept: Vec<Vec<&Entry>> = sims.iter().map(|(_, s)| s.entries.iter().filter(|e| keep(e)).collect()).collect();
        let view: Vec<Vec<Obs>> = kept.iter().map(|es| es.iter().map(|e| ob(e, Model::Engine)).collect()).collect();
        let everything = fit::survival(&view.iter().flatten().copied().collect::<Vec<_>>(), c).p;
        (0..sims.len())
            .map(|i| {
                let others: Vec<usize> =
                    (0..sims.len()).filter(|&j| j != i && sims[j].0.family == sims[i].0.family).collect();
                let o: Vec<Obs> = others.iter().flat_map(|&j| view[j].iter().copied()).collect();
                let fam = fit::shrink(fit::survival(&o, c).p, entry_days(others.iter().flat_map(|&j| kept[j].iter().copied())), everything);
                let own = fit::survival(&view[i], c);
                let days = entry_days(kept[i].iter().copied());
                let p = if view[i].is_empty() { fam } else { fit::shrink(own.p, days, fam) };
                Shrunk { p, p_lcb: fit::lower_bound(p, days), own }
            })
            .collect()
    };
    let fits = shrunk(|_| true);
    // The lens prices lots the hold rule refuses, and those exit on their own
    // cost: `cost + EXIT_FLOOR` is their floor, not the hold value. A fatter
    // lock waits for the hold value, further above what it cost, and gains
    // more when it gets it, so the lens reads its p, exit time and gain off
    // the entries whose floor was their cost.
    let lens = shrunk(|e| e.cost_bound);
    let engine: Vec<Vec<Obs>> = sims.iter().map(|(_, s)| obs(&s.entries, Model::Engine)).collect();
    let global = fit::survival(&engine.iter().flatten().copied().collect::<Vec<_>>(), c);
    let days_of: Vec<f64> = sims.iter().map(|(_, s)| entry_days(&s.entries)).collect();

    let mut families: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, (p, _)) in sims.iter().enumerate() {
        families.entry(p.family.as_str()).or_default().push(i);
    }
    let families: Vec<Value> = families
        .iter()
        .map(|(f, idx)| {
            let o: Vec<Obs> = idx.iter().flat_map(|&i| engine[i].iter().copied()).collect();
            let d = entry_days(idx.iter().flat_map(|&i| &sims[i].1.entries));
            json!({"family": f, "p": round(fit::shrink(fit::survival(&o, c).p, d, global.p), 4),
                   "entry_days": d, "entries": o.len()})
        })
        .collect();

    let day_s = |t: usize| (t0_ns as f64 / 1e9) + (t * 60) as f64;
    let mut out_pairs = Vec::new();
    let mut history = serde_json::Map::new();
    for (i, (p, s)) in sims.iter().enumerate() {
        let e = &s.entries;
        let Shrunk { own: best, p: p_shrunk, p_lcb } = &fits[i];
        let cross = fit::survival(&obs(e, Model::Cross), c);
        let par = fit::survival(&obs(e, Model::Par), c);
        let recent: Vec<Value> = e
            .iter()
            .rev()
            .take(RECENT)
            .map(|x| {
                json!({"ts": round(day_s(x.t), 0), "dir": x.dir.as_str(), "shape": x.shape.as_str(),
                       "lock": round(1.0 - x.cost, 4),
                       "chances": x.chances.len(),
                       "exit_h": x.chances.first().map(|z| round(z.minutes as f64 / 60.0, 2)),
                       "exit_gain": x.chances.first().map(|z| round(z.proceeds - x.cost, 4)),
                       "exit_proceeds": x.chances.first().map(|z| round(z.proceeds, 4)),
                       "cross_exit_h": x.chances_cross.first().map(|z| round(z.minutes as f64 / 60.0, 2)),
                       "par_h": x.chances_par.first().map(|z| round(z.minutes as f64 / 60.0, 2)),
                       "watched_h": round(x.watched as f64 / 60.0, 1)})
            })
            .collect();
        let l = &lens[i];
        history.insert(
            p.rel.clone(),
            json!({"p": round(*p_shrunk, 4), "p_lcb": round(*p_lcb, 4), "entries": e.len()}),
        );
        out_pairs.push(json!({
            "rel": p.rel, "family": p.family, "kalshi": p.kalshi, "pmus": p.pmus,
            "vetted_by": p.vetted_by, "verdict": p.verdict, "resolves": p.resolves,
            "valid_minutes": s.valid_minutes,
            "entries": e.len(), "entry_days": days_of[i],
            "entries_kalshi_yes": e.iter().filter(|x| x.dir == Dir::KYes).count(),
            "exits_tape": e.iter().filter(|x| !x.chances.is_empty()).count(),
            "median_lock": opt(median(e.iter().map(|x| 1.0 - x.cost).collect()), 4),
            // Entries clearing each lock on the `clear_step` grid: the dashboard
            // reads it at whatever margin is in force when it is viewed.
            "clearing": s.clearing,
            "p_tape": round(best.p_tape, 4),
            "p_unshrunk": round(best.p, 4),
            "p": round(*p_shrunk, 4),
            "p_lcb": round(*p_lcb, 4),
            "p_cross": round(cross.p, 4),
            "p_par": round(par.p, 4),
            "mean_gain": opt(best.mean_gain, 4),
            "mean_gain_par": opt(par.mean_gain, 4),
            "mean_exit_days": opt(best.mean_exit_min.map(|m| m / 1440.0), 3),
            "median_exit_days": opt(best.median_exit_min.map(|m| m / 1440.0), 3),
            "curve": best.curve.iter().map(|v| round(*v, 4)).collect::<Vec<_>>(),
            "curve_tape": best.curve_tape.iter().map(|v| round(*v, 4)).collect::<Vec<_>>(),
            // What the lens reads: the entries whose exit floor was their cost.
            "lens_entries": e.iter().filter(|x| x.cost_bound).count(),
            "lens_p_lcb": round(l.p_lcb, 4),
            "lens_exit_days": opt(l.own.mean_exit_min.map(|m| m / 1440.0), 3),
            "lens_gain": opt(l.own.mean_gain, 4),
            "kalshi_vol_1h": opt(s.kalshi_vol_1h, 5),
            "basis_sd": opt(s.basis_sd, 5),
            "recent": recent,
        }));
    }

    let generated_at = arb_core::clock::now_secs();
    let doc = json!({
        "as_of": as_of, "first_day": days.first(), "window_days": a.days,
        "generated_at": generated_at,
        "horizon_days": fit::HORIZON_MIN / 1440.0, "curve_days": fit::CURVE_DAYS,
        "prior_days": fit::PRIOR_DAYS, "calibration_hurdle_pct": a.hurdle_pct,
        "exit_floor": sim::EXIT_FLOOR, "clear_step": sim::CLEAR_STEP,
        "fees": {"kalshi_taker": sim::KALSHI_TAKER, "kalshi_maker": sim::KALSHI_MAKER,
                 "kalshi_maker_free_series": arb_core::fees::KALSHI_FREE_MAKER_SERIES,
                 "pmus_taker": sim::PMUS_TAKER, "pmus_maker": 0.0},
        "missing_days": {"kalshi": k_missing, "polymarket_us": p_missing},
        "calibration": {"share": round(c.share, 3), "rate": round(c.rate, 3), "lots": cal.lots,
                        "offered": cal.offered, "taken": cal.taken,
                        "held": held, "closed": closed, "estimated": cal.estimated},
        "global": {"entries": engine.iter().map(Vec::len).sum::<usize>(), "p_tape": round(global.p_tape, 4),
                   "p": round(global.p, 4), "curve": global.curve.iter().map(|v| round(*v, 4)).collect::<Vec<_>>()},
        "families": families,
        "pairs": out_pairs,
    });

    std::fs::create_dir_all(&a.out_dir).map_err(|e| format!("mkdir {}: {e}", a.out_dir))?;
    let latest = format!("{}/latest.json", a.out_dir);
    let newer_exists = std::fs::read_to_string(&latest)
        .ok()
        .and_then(|t| serde_json::from_str::<Value>(&t).ok())
        .and_then(|v| v.get("as_of").and_then(Value::as_str).map(|d| d > as_of.as_str()))
        .unwrap_or(false);
    if !newer_exists {
        write_atomic(&latest, &doc.to_string())?;
    }
    let line = json!({"as_of": as_of, "generated_at": generated_at, "share": round(c.share, 3), "rate": round(c.rate, 3),
                      "calibration_lots": cal.lots, "p_global": round(global.p, 4), "pairs": history});
    let hist = format!("{}/history.jsonl", a.out_dir);
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&hist)
        .and_then(|mut f| f.write_all(format!("{line}\n").as_bytes()))
        .map_err(|e| format!("append {hist}: {e}"))?;
    Ok(format!(
        "as_of {as_of}: {} pairs, {} entries, take {:.2} of lots at {:.2} a chance over {} lots ({} of {} chances \
         taken; {} held, {} closed without a maker exit){}, p(global) {:.2}{}",
        sims.len(),
        engine.iter().map(Vec::len).sum::<usize>(),
        c.share,
        c.rate,
        cal.lots,
        cal.taken,
        cal.offered,
        held,
        closed,
        if cal.estimated { "" } else { " (UNCALIBRATED)" },
        global.p,
        if newer_exists { "; latest.json is newer, left alone" } else { "" },
    ))
}

fn main() {
    let a = parse_args();
    match run(&a) {
        Ok(summary) => println!("{summary}"),
        Err(e) => {
            eprintln!("arb-exitability: {e}");
            exit(1);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::family_of;

    #[test]
    fn a_family_is_the_id_without_its_last_segment() {
        assert_eq!(family_of("xvus-time-poty-26-zohranmamdani"), "xvus-time-poty-26");
        assert_eq!(family_of("xvus-fedmtg-26oct-nochng"), "xvus-fedmtg-26oct");
        assert_eq!(family_of("solo"), "solo");
    }
}
