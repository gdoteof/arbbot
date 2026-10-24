//! Counterfactual entries and exits on one pair's minute grids. Pure.
//!
//! A basket is long YES on one venue and long NO on the other:
//!
//!   * `Dir::KYes` — long Kalshi YES + long PM-US NO;
//!   * `Dir::KNo`  — long Kalshi NO + long PM-US YES.
//!
//! ENTRY is the engine's maker shape: a quote rests one tick inside the touch
//! on one venue (or joins it when the spread is one tick) and the other leg is
//! taken when it fills. A rested quote counts as filled in a minute when a
//! trade prints at or through its price; one that only JOINED the touch is at
//! the back of the queue, so it needs a print a full tick through.
//!
//! EXIT is the ENGINE'S exit, not par. The engine does not wait for $1.00: it
//! sells once proceeds, net of exit fees, clear the lot's cost by `EXIT_FLOOR`
//! and beat holding to resolution at the hurdle (`hold(t)`), which together
//! are its `reservation_floor`. Two ways out:
//!
//!   * a maker exit on either venue (same fill proxy, other leg taken);
//!   * crossing both legs, admitted on prices one tick worse than the touch on
//!     each leg (as `maker_exit::decide` scores a cross) and filled at the touch.
//!
//! With weeks to resolution `hold(t)` sits well under $1, which is why most of
//! our real exits land below par after fees, and the fastest ones furthest
//! below.
//!
//! The tape does not say when the engine WILL exit, only when it could have:
//! each entry gets its exit CHANCES, the first clearing minute of every hour
//! in which the rule could be met (`chances`). How many of those the engine
//! actually takes is measured on our own lots (`fit::calibrate`). Crossing
//! alone is kept as a second model, and $1.00 or better as a reference.

use crate::tape::Series;

pub const KALSHI_TAKER: f64 = 0.07;
pub const KALSHI_MAKER: f64 = 0.0175;
/// PM-US's exchange-wide taker coefficient since 2026-09-19 (`fees.rs`).
/// Applied to the whole window; before that date it was 0.06, so earlier exits
/// read slightly harder than they were.
pub const PMUS_TAKER: f64 = 0.0695;
pub const TICK: f64 = 0.01;
/// Below this lock an entry is not worth simulating: the engine's own floor.
pub const MIN_LOCK: f64 = 0.005;
/// Profit over cost any exit must keep (`maker_exit::EXIT_FLOOR`).
pub const EXIT_FLOOR: f64 = 0.005;
/// Grid step of `PairSim::clearing`: a tenth of a cent of lock.
pub const CLEAR_STEP: f64 = 0.001;
/// One simulated entry per (direction, shape) per hour. A standing quote fills
/// many minutes in a row; counting each as a separate trade would weight one
/// long dislocation as dozens of independent ones.
pub const THIN_MIN: usize = 60;
const PAR: f64 = 1.0 - 1e-9;

fn fee(c: f64, p: f64) -> f64 {
    c * p * (1.0 - p)
}

/// Where a maker quote rests: one tick inside the touch, or AT it when the
/// spread is a single tick.
fn improve_bid(b: f64, a: f64) -> f64 {
    if a - b >= 2.0 * TICK - 1e-6 { (b + TICK).min(a - TICK) } else { b }
}

fn improve_ask(b: f64, a: f64) -> f64 {
    if a - b >= 2.0 * TICK - 1e-6 { (a - TICK).max(b + TICK) } else { a }
}

fn joined(q: f64, touch: f64) -> bool {
    (q - touch).abs() < 1e-6
}

/// A resting buy at `q` filled during the minute whose lowest print was `lo`.
fn filled_buy(q: f64, bid: f64, lo: f64) -> bool {
    lo.is_finite() && lo <= if joined(q, bid) { q - TICK + 1e-4 } else { q + 1e-6 }
}

fn filled_sell(q: f64, ask: f64, hi: f64) -> bool {
    hi.is_finite() && hi >= if joined(q, ask) { q + TICK - 1e-4 } else { q - 1e-6 }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    KYes,
    KNo,
}

impl Dir {
    pub fn as_str(self) -> &'static str {
        match self {
            Dir::KYes => "kalshi-yes",
            Dir::KNo => "kalshi-no",
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Shape {
    /// The quote rested on Kalshi; PM-US was taken.
    RestKalshi,
    /// The quote rested on PM-US; Kalshi was taken.
    RestPmus,
}

impl Shape {
    pub fn as_str(self) -> &'static str {
        match self {
            Shape::RestKalshi => "rest-kalshi",
            Shape::RestPmus => "rest-pmus",
        }
    }
}

#[derive(Clone, Debug)]
pub struct Entry {
    pub t: usize,
    pub dir: Dir,
    pub shape: Shape,
    /// Basket cost per contract including entry fees. The lock is `1 - cost`.
    pub cost: f64,
    /// The engine's exit chances inside the horizon.
    pub chances: Vec<Exit>,
    /// The same with crossing both legs the only way out.
    pub chances_cross: Vec<Exit>,
    /// The same at $1.00 or better: the bar a par exit would set.
    pub chances_par: Vec<Exit>,
    /// Minutes of tape after the entry: how long it was watched.
    pub watched: usize,
    /// The engine's exit floor for this lot was its own cost, not the hold
    /// value: a lot the hold rule refuses, or nearly.
    pub cost_bound: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Exit {
    /// Minutes after the entry.
    pub minutes: usize,
    /// What the basket sold for per contract, net of exit fees.
    pub proceeds: f64,
}

/// One minute of both books, `None` unless both are two-sided and sane.
#[derive(Clone, Copy)]
struct Tick {
    kb: f64,
    ka: f64,
    pb: f64,
    pa: f64,
    klo: f64,
    khi: f64,
    plo: f64,
    phi: f64,
}

fn tick(k: &Series, p: &Series, t: usize) -> Option<Tick> {
    let g = |v: &[f32]| f64::from(v[t]);
    let x = Tick {
        kb: g(&k.bid),
        ka: g(&k.ask),
        pb: g(&p.bid),
        pa: g(&p.ask),
        klo: g(&k.lo),
        khi: g(&k.hi),
        plo: g(&p.lo),
        phi: g(&p.hi),
    };
    let sane = |b: f64, a: f64| b.is_finite() && a.is_finite() && b > 0.0 && a < 1.0 && b < a;
    (sane(x.kb, x.ka) && sane(x.pb, x.pa)).then_some(x)
}

/// The two entry shapes of one direction at one minute: (shape, cost, filled).
fn entries_at(x: &Tick, dir: Dir) -> [(Shape, f64, bool); 2] {
    let (qkb, qka) = (improve_bid(x.kb, x.ka), improve_ask(x.kb, x.ka));
    let (qpb, qpa) = (improve_bid(x.pb, x.pa), improve_ask(x.pb, x.pa));
    match dir {
        Dir::KYes => [
            (
                Shape::RestKalshi,
                qkb + (1.0 - x.pb) + fee(KALSHI_MAKER, qkb) + fee(PMUS_TAKER, x.pb),
                filled_buy(qkb, x.kb, x.klo),
            ),
            (Shape::RestPmus, x.ka + (1.0 - qpa) + fee(KALSHI_TAKER, x.ka), filled_sell(qpa, x.pa, x.phi)),
        ],
        Dir::KNo => [
            (
                Shape::RestKalshi,
                (1.0 - qka) + x.pa + fee(KALSHI_MAKER, qka) + fee(PMUS_TAKER, x.pa),
                filled_sell(qka, x.ka, x.khi),
            ),
            (Shape::RestPmus, (1.0 - x.kb) + qpb + fee(KALSHI_TAKER, x.kb), filled_buy(qpb, x.pb, x.plo)),
        ],
    }
}

/// What the basket sells for in one minute, net of exit fees.
#[derive(Clone, Copy)]
struct ExitAt {
    /// Crossing both legs at the touch: what a cross fills at.
    cross: f64,
    /// Crossing one tick worse on each leg: what a cross is admitted on.
    cross_gate: f64,
    /// The better maker exit; `NaN` when neither venue's exit quote filled.
    maker: f64,
}

const NO_EXIT: ExitAt = ExitAt { cross: f64::NAN, cross_gate: f64::NAN, maker: f64::NAN };

fn exit_at(x: &Tick, dir: Dir) -> ExitAt {
    let (qkb, qka) = (improve_bid(x.kb, x.ka), improve_ask(x.kb, x.ka));
    let (qpb, qpa) = (improve_bid(x.pb, x.pa), improve_ask(x.pb, x.pa));
    let better = |a: Option<f64>, b: Option<f64>| [a, b].into_iter().flatten().fold(f64::NAN, f64::max);
    match dir {
        Dir::KYes => {
            // Sell the Kalshi YES at `k`, buy the PM-US YES back at `pm`, both taken.
            let cross = |k: f64, pm: f64| k + (1.0 - pm) - fee(KALSHI_TAKER, k) - fee(PMUS_TAKER, pm);
            let maker_k = filled_sell(qka, x.ka, x.khi)
                .then(|| qka + (1.0 - x.pa) - fee(KALSHI_MAKER, qka) - fee(PMUS_TAKER, x.pa));
            let maker_p = filled_buy(qpb, x.pb, x.plo).then(|| x.kb + (1.0 - qpb) - fee(KALSHI_TAKER, x.kb));
            ExitAt { cross: cross(x.kb, x.pa), cross_gate: cross(x.kb - TICK, x.pa + TICK), maker: better(maker_k, maker_p) }
        }
        Dir::KNo => {
            // Buy the Kalshi YES back at `k`, sell the PM-US YES at `pm`, both taken.
            let cross = |k: f64, pm: f64| (1.0 - k) + pm - fee(KALSHI_TAKER, k) - fee(PMUS_TAKER, pm);
            let maker_k = filled_buy(qkb, x.kb, x.klo)
                .then(|| (1.0 - qkb) + x.pb - fee(KALSHI_MAKER, qkb) - fee(PMUS_TAKER, x.pb));
            let maker_p = filled_sell(qpa, x.pa, x.phi).then(|| (1.0 - x.ka) + qpa - fee(KALSHI_TAKER, x.ka));
            ExitAt { cross: cross(x.ka, x.pb), cross_gate: cross(x.ka + TICK, x.pb - TICK), maker: better(maker_k, maker_p) }
        }
    }
}

/// The exit chances of a lot entered at `t0`, up to `limit` minutes on: the
/// first minute of each hour after the entry in which selling would have
/// cleared `floor(t)`, and what it would have fetched. An hour, not a minute:
/// a basket that clears all afternoon is a few chances, not hundreds, and a
/// blip and its echo inside one hour are one. `NaN` values never clear.
fn chances(v: &[ExitAt], t0: usize, limit: usize, floor: impl Fn(usize) -> f64, cross_only: bool) -> Vec<Exit> {
    let hour = |t: usize| (t - t0 - 1) / 60;
    let mut out: Vec<Exit> = Vec::new();
    for (t, x) in v.iter().enumerate().take(t0 + 1 + limit).skip(t0 + 1) {
        if out.last().is_some_and(|c| hour(t0 + c.minutes) == hour(t)) {
            continue;
        }
        let f = floor(t);
        let by_cross = (x.cross_gate >= f).then_some(x.cross);
        let by_maker = (!cross_only && x.maker >= f).then_some(x.maker);
        if let Some(proceeds) = by_cross.into_iter().chain(by_maker).reduce(f64::max) {
            out.push(Exit { minutes: t - t0, proceeds });
        }
    }
    out
}

/// The engine's floor for a lot that cost `cost`: its `reservation_floor`.
fn engine_floor(cost: f64, hold: &impl Fn(usize) -> f64) -> impl Fn(usize) -> f64 + '_ {
    move |t| (cost + EXIT_FLOOR).max(hold(t))
}

/// Fills `(minute, lock)` in time order, thinned to one per `THIN_MIN`, among
/// only those whose lock is at least `m`.
fn thinned(fills: &[(usize, f64)], m: f64) -> usize {
    let mut last: Option<usize> = None;
    let mut n = 0;
    for &(t, lock) in fills {
        if lock >= m - 1e-9 && last.is_none_or(|l| t - l >= THIN_MIN) {
            last = Some(t);
            n += 1;
        }
    }
    n
}

/// `clearing[j]` = entries whose lock is at least `j * CLEAR_STEP`, with each
/// margin thinned on its own. Thinning once over every lock would let a thin
/// fill early in an hour hide a fat one later in it from every margin the thin
/// one fails.
fn clearing(fills: &[&[(usize, f64)]]) -> Vec<usize> {
    let mut curve = Vec::new();
    loop {
        let m = curve.len() as f64 * CLEAR_STEP;
        match fills.iter().map(|f| thinned(f, m)).sum() {
            0 => return curve,
            n => curve.push(n),
        }
    }
}

pub struct PairSim {
    pub entries: Vec<Entry>,
    pub valid_minutes: usize,
    /// Standard deviation of the Kalshi mid's one-hour change.
    pub kalshi_vol_1h: Option<f64>,
    /// Standard deviation of the Kalshi-minus-PM-US mid.
    pub basis_sd: Option<f64>,
    /// Entries clearing each lock on the `CLEAR_STEP` grid: see `clearing`.
    pub clearing: Vec<usize>,
    /// Exit values per direction per minute, kept for replaying real lots.
    exits: [Vec<ExitAt>; 2],
}

fn sd(xs: &[f64]) -> Option<f64> {
    if xs.len() < 2 {
        return None;
    }
    let m = xs.iter().sum::<f64>() / xs.len() as f64;
    Some((xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt())
}

/// Simulate every entry on `[0, end)` and its exit chances over the next
/// `horizon` minutes. `hold(t)` is what holding to resolution is worth at
/// minute `t`, at the hurdle.
pub fn simulate(k: &Series, p: &Series, end: usize, horizon: usize, hold: impl Fn(usize) -> f64) -> PairSim {
    let end = end.min(k.bid.len()).min(p.bid.len());
    let ticks: Vec<Option<Tick>> = (0..end).map(|t| tick(k, p, t)).collect();
    let dirs = [Dir::KYes, Dir::KNo];
    let mut exits = [vec![NO_EXIT; end], vec![NO_EXIT; end]];
    for (t, x) in ticks.iter().enumerate() {
        if let Some(x) = x {
            for (d, dir) in dirs.iter().enumerate() {
                exits[d][t] = exit_at(x, *dir);
            }
        }
    }
    let mut entries = Vec::new();
    let mut fills: [[Vec<(usize, f64)>; 2]; 2] = Default::default();
    let mut last = [[None::<usize>; 2]; 2];
    for (t, x) in ticks.iter().enumerate() {
        let Some(x) = x else { continue };
        for (d, dir) in dirs.iter().enumerate() {
            for (s, (shape, cost, filled)) in entries_at(x, *dir).into_iter().enumerate() {
                if !filled || 1.0 - cost < MIN_LOCK {
                    continue;
                }
                fills[d][s].push((t, 1.0 - cost));
                if last[d][s].is_some_and(|l| t - l < THIN_MIN) {
                    continue;
                }
                last[d][s] = Some(t);
                entries.push(Entry {
                    t,
                    dir: *dir,
                    shape,
                    cost,
                    chances: chances(&exits[d], t, horizon, engine_floor(cost, &hold), false),
                    chances_cross: chances(&exits[d], t, horizon, engine_floor(cost, &hold), true),
                    chances_par: chances(&exits[d], t, horizon, |_| PAR, false),
                    watched: end - 1 - t,
                    cost_bound: cost + EXIT_FLOOR >= hold(t),
                });
            }
        }
    }
    let clearing = clearing(&fills.iter().flatten().map(Vec::as_slice).collect::<Vec<_>>());
    let mids: Vec<Option<(f64, f64)>> =
        ticks.iter().map(|x| x.map(|x| ((x.kb + x.ka) / 2.0, (x.pb + x.pa) / 2.0))).collect();
    let basis: Vec<f64> = mids.iter().flatten().map(|(km, pm)| km - pm).collect();
    let moves: Vec<f64> = (60..mids.len())
        .filter_map(|t| Some(mids[t]?.0 - mids[t - 60]?.0))
        .collect();
    PairSim {
        entries,
        valid_minutes: basis.len(),
        kalshi_vol_1h: sd(&moves),
        basis_sd: sd(&basis),
        clearing,
        exits,
    }
}

impl PairSim {
    /// The engine's exit chances for a REAL lot entered at minute `t0` for
    /// `cost`, over the next `limit` minutes: what the calibration measures
    /// our real exits against.
    pub fn engine_chances(&self, dir: Dir, t0: usize, cost: f64, limit: usize, hold: impl Fn(usize) -> f64) -> Vec<Exit> {
        chances(&self.exits[usize::from(dir == Dir::KNo)], t0, limit, engine_floor(cost, &hold), false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A grid where both books hold one touch, with `trades` printed on Kalshi.
    fn flat(n: usize, k: (f32, f32), p: (f32, f32)) -> (Series, Series) {
        let s = |(b, a): (f32, f32)| Series {
            bid: vec![b; n],
            ask: vec![a; n],
            lo: vec![f32::NAN; n],
            hi: vec![f32::NAN; n],
        };
        (s(k), s(p))
    }

    #[test]
    fn a_maker_quote_one_tick_inside_fills_on_a_print_at_its_price() {
        assert!(filled_buy(0.41, 0.40, 0.41));
        assert!(!filled_buy(0.41, 0.40, 0.42));
        // Joined at the touch: behind the queue, so the print must go through.
        assert!(!filled_buy(0.40, 0.40, 0.40));
        assert!(filled_buy(0.40, 0.40, 0.39));
        assert!(filled_sell(0.44, 0.45, 0.44));
        assert!(!filled_sell(0.45, 0.45, 0.45));
        assert!(!filled_buy(0.41, 0.40, f64::NAN), "no print, no fill");
    }

    #[test]
    fn the_quote_improves_a_wide_touch_and_joins_a_one_tick_one() {
        assert!((improve_bid(0.40, 0.45) - 0.41).abs() < 1e-12);
        assert!((improve_bid(0.40, 0.41) - 0.40).abs() < 1e-12);
        assert!((improve_ask(0.40, 0.45) - 0.44).abs() < 1e-12);
    }

    /// Kalshi cheap against PM-US: a Kalshi YES bid fills at minute 1 for a
    /// ~7c lock. At minute 4 the basis half-closes, so crossing out sells for
    /// ~0.965: under par, but its tick-worse price clears cost + 0.5c and a 0.94
    /// hold value, so the engine takes it. Par only arrives at minute 7.
    #[test]
    fn an_entry_exits_when_the_engine_would_not_when_par_comes() {
        let n = 10;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        k.lo[1] = 0.41; // a seller hits our 0.41 bid
        for t in 4..7 {
            (k.bid[t], k.ask[t]) = (0.52, 0.54);
        }
        for t in 7..n {
            (k.bid[t], k.ask[t]) = (0.60, 0.62);
        }
        let s = simulate(&k, &p, n, n, |_| 0.94);
        let e: Vec<&Entry> = s.entries.iter().filter(|e| e.dir == Dir::KYes).collect();
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].t, 1);
        assert_eq!(e[0].shape, Shape::RestKalshi);
        let lock = 1.0 - e[0].cost;
        assert!(lock > 0.06 && lock < 0.08, "0.41 + 0.50 plus fees: {lock}");
        let x = e[0].chances_cross[0];
        assert_eq!(x.minutes, 3, "the half-closed basis is a cross the engine takes");
        assert!(x.proceeds > 0.95 && x.proceeds < 1.0, "under par: {}", x.proceeds);
        assert_eq!(e[0].chances.len(), 1, "minutes 4..9 are one hour: one chance");
        assert_eq!(e[0].chances[0].minutes, 3);
        assert_eq!(e[0].chances_par[0].minutes, 6, "0.60 + 0.48 - fees clears par at minute 7");
        assert_eq!(e[0].watched, n - 2);
        assert!(!e[0].cost_bound, "0.93 + 0.5c is under the 0.94 hold value, which is the floor");
        assert!(simulate(&k, &p, n, n, |_| 0.9).entries[0].cost_bound, "and over a 0.90 one");
        // With resolution tomorrow holding is worth $1, and nothing short of par is worth selling for.
        let s = simulate(&k, &p, n, n, |_| 1.0);
        let e = s.entries.iter().find(|e| e.dir == Dir::KYes).unwrap();
        assert_eq!(e.chances_cross[0].minutes, 6);
    }

    /// The same quote filling every minute is one dislocation, not sixty.
    #[test]
    fn a_standing_fill_is_thinned_to_one_entry_an_hour() {
        let n = 200;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        k.lo = vec![0.40; n];
        let s = simulate(&k, &p, n, n, |_| 0.0);
        let ts: Vec<usize> = s.entries.iter().filter(|e| e.dir == Dir::KYes).map(|e| e.t).collect();
        assert_eq!(ts, vec![0, 60, 120, 180]);
        assert!(s.entries.iter().all(|e| e.chances.is_empty()), "the basis never closes");
    }

    #[test]
    fn an_entry_without_a_lock_is_not_simulated() {
        let n = 5;
        let (mut k, p) = flat(n, (0.50, 0.55), (0.50, 0.52));
        k.lo = vec![0.49; n];
        assert!(simulate(&k, &p, n, n, |_| 0.0).entries.iter().all(|e| e.dir != Dir::KYes));
    }

    /// A crossed or one-sided book is not a price.
    #[test]
    fn a_crossed_book_is_no_minute_at_all() {
        let (k, p) = flat(3, (0.50, 0.45), (0.50, 0.52));
        assert_eq!(simulate(&k, &p, 3, 3, |_| 0.0).valid_minutes, 0);
    }

    /// A cross is admitted on its tick-worse price and paid at the touch; a
    /// maker exit at its own price. Either must clear cost by half a cent and
    /// beat holding.
    #[test]
    fn a_cross_is_admitted_a_tick_worse_and_every_exit_must_beat_holding() {
        let n = 10;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        for t in 3..n {
            (k.bid[t], k.ask[t]) = (0.46, 0.48);
        }
        let s = simulate(&k, &p, n, n, |_| 0.0);
        let x3 = s.exits[0][3];
        assert!(x3.cross - x3.cross_gate > 0.019, "two ticks between the touch and the gate");
        let first = |s: &PairSim, cost: f64, hold: f64| s.engine_chances(Dir::KYes, 0, cost, n, |_| hold).first().copied();
        let x = first(&s, x3.cross_gate - 0.006, 0.0).expect("clears by 0.6c");
        assert_eq!((x.minutes, x.proceeds), (3, x3.cross), "paid at the touch");
        assert_eq!(first(&s, x3.cross_gate - 0.004, 0.0), None, "under 0.5c");
        assert_eq!(first(&s, x3.cross_gate - 0.006, 1.0), None, "under the hold value");
        // A Kalshi print at our 0.47 exit ask makes a maker exit available at minute 5.
        k.hi[5] = 0.47;
        let s = simulate(&k, &p, n, n, |_| 0.0);
        let m = s.exits[0][5].maker;
        assert!(m.is_finite() && m > x3.cross, "resting inside the spread beats crossing it");
        let x = first(&s, m - 0.01, 0.0).expect("half a cent clears a maker exit");
        assert_eq!((x.minutes, x.proceeds), (5, m));
    }

    /// Clearing for two and a half hours straight is three chances; clearing
    /// twice inside one hour is one.
    #[test]
    fn a_chance_is_an_hour_that_clears() {
        let n = 400;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        for t in 10..160 {
            (k.bid[t], k.ask[t]) = (0.60, 0.62);
        }
        for t in [250, 280] {
            (k.bid[t], k.ask[t]) = (0.60, 0.62);
        }
        let s = simulate(&k, &p, n, n, |_| 0.0);
        let mins: Vec<usize> = s.engine_chances(Dir::KYes, 0, 0.9, n, |_| 0.0).iter().map(|x| x.minutes).collect();
        assert_eq!(mins, vec![10, 61, 121, 250]);
        assert_eq!(s.engine_chances(Dir::KYes, 0, 0.9, 100, |_| 0.0).len(), 2, "cut at the limit");
    }

    /// A thin fill early in the hour must not hide a fat one later in it from
    /// a margin only the fat one clears.
    #[test]
    fn each_margin_is_thinned_on_its_own() {
        let fills = [(0, 0.006), (10, 0.15), (70, 0.15), (75, 0.006)];
        assert_eq!(thinned(&fills, 0.0), 2, "0 and 70");
        assert_eq!(thinned(&fills, 0.12), 2, "10 and 70, not 70 alone");
        let curve = clearing(&[&fills]);
        assert_eq!(curve.len(), 151, "nothing clears 15.1c");
        assert_eq!((curve[0], curve[6], curve[7], curve[150]), (2, 2, 2, 2));
        assert_eq!(clearing(&[&fills, &[(0, 0.01)]])[10], 3, "summed over the cells");
    }
}
