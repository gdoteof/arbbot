//! arb-exitability — how likely each pair is to give a lot back at par, and how
//! soon. The nightly refit behind the dashboard's Exitability view.
//!
//! The entry bar prices every lot as held to resolution. A pair whose basis
//! keeps round-tripping is worth far more than that, because the lot comes back
//! at par in days and the capital goes round again. This measures it per pair:
//!
//!   1. replay the archived books of the last `--days` days into minute grids;
//!   2. simulate every maker entry the tape would have filled, and the first
//!      minute each could have exited at par (`sim`);
//!   3. replay our own lots the same way and measure how much later than the
//!      tape's first chance we actually got out (`fit::calibrate`);
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

fn obs(entries: &[Entry], cross: bool) -> Vec<Obs> {
    entries
        .iter()
        .map(|e| {
            let tau = if cross { e.tau_cross } else { e.tau_best };
            Obs { minutes: tau.unwrap_or(e.watched) as f64, exited: tau.is_some() }
        })
        .collect()
}

fn entry_days(entries: &[Entry]) -> f64 {
    entries.iter().map(|e| e.t / DAY_MINUTES).collect::<BTreeSet<_>>().len() as f64
}

fn round(x: f64, places: i32) -> f64 {
    let m = 10f64.powi(places);
    (x * m).round() / m
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
    // Minutes from the window's start to a pair's guarded resolve, and the
    // hold value at minute t (`maker_exit::reservation_floor`).
    let resolve_min = |p: &Pair| p.resolves.and_then(parse_iso).map(|d| (d - first_day) * DAY_MINUTES as i64);
    let sims: Vec<(&Pair, PairSim)> = pairs
        .iter()
        .filter_map(|p| {
            let (k, m) = (kalshi.get(&p.kalshi)?, pmus.get(&p.pmus)?);
            let end = resolve_min(p)
                .map_or(n as i64, |r| r - RESOLVE_GUARD_DAYS * DAY_MINUTES as i64)
                .clamp(0, n as i64) as usize;
            Some((p, sim::simulate(k, m, end)))
        })
        .filter(|(_, s)| s.valid_minutes > 0)
        .collect();

    // Calibration: our own lots entered inside the window, against the tape.
    // A lot out by a maker exit before the window closed gives its real time;
    // one still held (or closed some other way) gives the time it HAS been
    // held, a lower bound. Leaving the slow ones out would measure only the
    // lots that were quick, so they count, at no less than they are known to be.
    let records = ledger::apply_corrections(ledger::read(&a.ledger)?);
    let window_end_s = (t0_ns as f64 + n as f64 * MINUTE_NS as f64) / 1e9;
    let (mut calib, mut censored): (Vec<(f64, f64)>, usize) = (Vec::new(), 0);
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
        let res = resolve_min(pair);
        let floor = |t: usize| {
            res.map_or(0.0, |r| 1.0 / (1.0 + hurdle * ((r - t as i64).max(0) as f64 / 1440.0) / 365.25))
        };
        let Some(sim_min) = s.engine_exit(lot.dir, t0 as usize, lot.cost, floor) else { continue };
        let real_min = (out_at - lot.ts) / 60.0;
        // A held lot whose tape exit is still ahead of it says nothing yet.
        if exact || real_min > sim_min as f64 {
            calib.push((real_min, sim_min as f64));
            censored += usize::from(!exact);
        }
    }
    let cal = fit::calibrate(&calib);
    let k = cal.k;

    // Pool, then shrink pair -> family -> everything.
    let all: Vec<Entry> = sims.iter().flat_map(|(_, s)| s.entries.iter().cloned()).collect();
    let global = fit::survival(&obs(&all, false), k);
    let mut fam_entries: BTreeMap<&str, Vec<Entry>> = BTreeMap::new();
    for (p, s) in &sims {
        fam_entries.entry(p.family.as_str()).or_default().extend(s.entries.iter().cloned());
    }
    let fam_p: BTreeMap<&str, (f64, f64, usize)> = fam_entries
        .iter()
        .map(|(f, es)| {
            let d = entry_days(es);
            (*f, (fit::shrink(fit::survival(&obs(es, false), k).p, d, global.p), d, es.len()))
        })
        .collect();

    let day_s = |t: usize| (t0_ns as f64 / 1e9) + (t * 60) as f64;
    let mut out_pairs = Vec::new();
    let mut history = serde_json::Map::new();
    for (p, s) in &sims {
        let e = &s.entries;
        let best = fit::survival(&obs(e, false), k);
        let cross = fit::survival(&obs(e, true), k);
        let days_seen = entry_days(e);
        let fam = fam_p.get(p.family.as_str()).map_or(global.p, |f| f.0);
        let p_shrunk = if e.is_empty() { fam } else { fit::shrink(best.p, days_seen, fam) };
        let p_lcb = fit::lower_bound(p_shrunk, days_seen);
        let recent: Vec<Value> = e
            .iter()
            .rev()
            .take(RECENT)
            .map(|x| {
                json!({"ts": round(day_s(x.t), 0), "dir": x.dir.as_str(), "shape": x.shape.as_str(),
                       "lock": round(1.0 - x.cost, 4),
                       "exit_h": x.tau_best.map(|m| round(m as f64 / 60.0, 2)),
                       "cross_exit_h": x.tau_cross.map(|m| round(m as f64 / 60.0, 2)),
                       "watched_h": round(x.watched as f64 / 60.0, 1)})
            })
            .collect();
        // Every entry's lock, ascending: the dashboard counts how many clear
        // a margin at whatever bar is in force when it is read.
        let mut locks: Vec<f64> = e.iter().map(|x| round(1.0 - x.cost, 4)).collect();
        locks.sort_by(f64::total_cmp);
        history.insert(
            p.rel.clone(),
            json!({"p": round(p_shrunk, 4), "p_lcb": round(p_lcb, 4), "entries": e.len()}),
        );
        out_pairs.push(json!({
            "rel": p.rel, "family": p.family, "kalshi": p.kalshi, "pmus": p.pmus,
            "vetted_by": p.vetted_by, "verdict": p.verdict, "resolves": p.resolves,
            "valid_minutes": s.valid_minutes,
            "entries": e.len(), "entry_days": days_seen,
            "entries_kalshi_yes": e.iter().filter(|x| x.dir == Dir::KYes).count(),
            "exits_tape": e.iter().filter(|x| x.tau_best.is_some()).count(),
            "median_lock": opt(median(e.iter().map(|x| 1.0 - x.cost).collect()), 4),
            "locks": locks,
            "p_tape": round(best.p_tape, 4),
            "p_unshrunk": round(best.p, 4),
            "p": round(p_shrunk, 4),
            "p_lcb": round(p_lcb, 4),
            "p_cross": round(cross.p, 4),
            "mean_exit_days": opt(best.mean_exit_min.map(|m| m / 1440.0), 3),
            "median_exit_days": opt(best.median_exit_min.map(|m| m / 1440.0), 3),
            "curve": best.curve.iter().map(|v| round(*v, 4)).collect::<Vec<_>>(),
            "curve_tape": best.curve_tape.iter().map(|v| round(*v, 4)).collect::<Vec<_>>(),
            "curve_cross": cross.curve.iter().map(|v| round(*v, 4)).collect::<Vec<_>>(),
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
        "fees": {"kalshi_taker": sim::KALSHI_TAKER, "kalshi_maker": sim::KALSHI_MAKER,
                 "pmus_taker": sim::PMUS_TAKER, "pmus_maker": 0.0},
        "missing_days": {"kalshi": k_missing, "polymarket_us": p_missing},
        "calibration": {"k": round(cal.k, 3), "lots": cal.lots, "held": censored, "estimated": cal.estimated,
                        "median_ratio": opt(cal.median_ratio, 3)},
        "global": {"entries": all.len(), "p_tape": round(global.p_tape, 4), "p": round(global.p, 4),
                   "curve": global.curve.iter().map(|v| round(*v, 4)).collect::<Vec<_>>()},
        "families": fam_p.iter().map(|(f, (p, d, n))| json!({"family": f, "p": round(*p, 4),
                     "entry_days": d, "entries": n})).collect::<Vec<_>>(),
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
    let line = json!({"as_of": as_of, "generated_at": generated_at, "k": round(cal.k, 3),
                      "calibration_lots": cal.lots, "p_global": round(global.p, 4), "pairs": history});
    let hist = format!("{}/history.jsonl", a.out_dir);
    std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(&hist)
        .and_then(|mut f| f.write_all(format!("{line}\n").as_bytes()))
        .map_err(|e| format!("append {hist}: {e}"))?;
    Ok(format!(
        "as_of {as_of}: {} pairs, {} entries, k {:.2} over {} lots ({} still held){}, p(global) {:.2}{}",
        sims.len(),
        all.len(),
        cal.k,
        cal.lots,
        censored,
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
