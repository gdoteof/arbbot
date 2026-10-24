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
//! EXIT is "back at par": the first minute the basket sells for at least $1.00
//! net of exit fees, which returns the entry's whole lock. Two exit models:
//!
//!   * `best` — the better of a maker exit on either venue (same fill proxy,
//!     other leg taken) and crossing both legs;
//!   * `cross` — crossing both legs only. Always fillable, never optimistic.
//!
//! Because the par threshold does not depend on the entry price, the first
//! passage from every minute is one backward pass per model.

use crate::tape::Series;

pub const KALSHI_TAKER: f64 = 0.07;
pub const KALSHI_MAKER: f64 = 0.0175;
/// PM-US's exchange-wide taker coefficient since 2026-10-19. Applied to the
/// whole window; before that date it was 0.06, so earlier exits read slightly
/// harder than they were.
pub const PMUS_TAKER: f64 = 0.0695;
pub const TICK: f64 = 0.01;
/// Below this lock an entry is not worth simulating: the engine's own floor.
pub const MIN_LOCK: f64 = 0.005;
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
    lo.is_finite() && lo <= if joined(q, bid) { q - TICK + 1e-4 } else { q + 1e-9 }
}

fn filled_sell(q: f64, ask: f64, hi: f64) -> bool {
    hi.is_finite() && hi >= if joined(q, ask) { q + TICK - 1e-4 } else { q - 1e-9 }
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
    /// Minutes to the first par exit under each model; `None` = none seen.
    pub tau_best: Option<usize>,
    pub tau_cross: Option<usize>,
    /// Minutes of tape after the entry: how long it was watched.
    pub watched: usize,
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

/// What the basket sells for this minute, net of exit fees: (cross, best).
fn exit_at(x: &Tick, dir: Dir) -> (f64, f64) {
    let (qkb, qka) = (improve_bid(x.kb, x.ka), improve_ask(x.kb, x.ka));
    let (qpb, qpa) = (improve_bid(x.pb, x.pa), improve_ask(x.pb, x.pa));
    let (cross, maker_k, maker_p) = match dir {
        Dir::KYes => (
            x.kb + (1.0 - x.pa) - fee(KALSHI_TAKER, x.kb) - fee(PMUS_TAKER, x.pa),
            filled_sell(qka, x.ka, x.khi)
                .then(|| qka + (1.0 - x.pa) - fee(KALSHI_MAKER, qka) - fee(PMUS_TAKER, x.pa)),
            filled_buy(qpb, x.pb, x.plo).then(|| x.kb + (1.0 - qpb) - fee(KALSHI_TAKER, x.kb)),
        ),
        Dir::KNo => (
            (1.0 - x.ka) + x.pb - fee(KALSHI_TAKER, x.ka) - fee(PMUS_TAKER, x.pb),
            filled_buy(qkb, x.kb, x.klo)
                .then(|| (1.0 - qkb) + x.pb - fee(KALSHI_MAKER, qkb) - fee(PMUS_TAKER, x.pb)),
            filled_sell(qpa, x.pa, x.phi).then(|| (1.0 - x.ka) + qpa - fee(KALSHI_TAKER, x.ka)),
        ),
    };
    let best = [maker_k, maker_p].into_iter().flatten().fold(cross, f64::max);
    (cross, best)
}

/// `next[t]` = the first minute `>= t` whose value clears par.
fn next_par(v: &[f64]) -> Vec<Option<usize>> {
    let mut next = vec![None; v.len() + 1];
    for t in (0..v.len()).rev() {
        next[t] = if v[t] >= PAR { Some(t) } else { next[t + 1] };
    }
    next
}

pub struct PairSim {
    pub entries: Vec<Entry>,
    pub valid_minutes: usize,
    /// Standard deviation of the Kalshi mid's one-hour change.
    pub kalshi_vol_1h: Option<f64>,
    /// Standard deviation of the Kalshi-minus-PM-US mid.
    pub basis_sd: Option<f64>,
    /// `best` exit value per direction per minute, kept for replaying real lots.
    best: [Vec<f64>; 2],
}

fn sd(xs: &[f64]) -> Option<f64> {
    if xs.len() < 2 {
        return None;
    }
    let m = xs.iter().sum::<f64>() / xs.len() as f64;
    Some((xs.iter().map(|x| (x - m).powi(2)).sum::<f64>() / (xs.len() - 1) as f64).sqrt())
}

/// Simulate every entry on `[0, end)` and its first par exit.
pub fn simulate(k: &Series, p: &Series, end: usize) -> PairSim {
    let end = end.min(k.bid.len()).min(p.bid.len());
    let ticks: Vec<Option<Tick>> = (0..end).map(|t| tick(k, p, t)).collect();
    let dirs = [Dir::KYes, Dir::KNo];
    let mut cross = [vec![f64::NAN; end], vec![f64::NAN; end]];
    let mut best = [vec![f64::NAN; end], vec![f64::NAN; end]];
    for (t, x) in ticks.iter().enumerate() {
        if let Some(x) = x {
            for (d, dir) in dirs.iter().enumerate() {
                (cross[d][t], best[d][t]) = exit_at(x, *dir);
            }
        }
    }
    let next_cross = [next_par(&cross[0]), next_par(&cross[1])];
    let next_best = [next_par(&best[0]), next_par(&best[1])];
    let mut entries = Vec::new();
    let mut last = [[None::<usize>; 2]; 2];
    for (t, x) in ticks.iter().enumerate() {
        let Some(x) = x else { continue };
        for (d, dir) in dirs.iter().enumerate() {
            for (s, (shape, cost, filled)) in entries_at(x, *dir).into_iter().enumerate() {
                if !filled || 1.0 - cost < MIN_LOCK || last[d][s].is_some_and(|l| t - l < THIN_MIN) {
                    continue;
                }
                last[d][s] = Some(t);
                let after = |next: &[Option<usize>]| next[t + 1].map(|h| h - t);
                entries.push(Entry {
                    t,
                    dir: *dir,
                    shape,
                    cost,
                    tau_best: after(&next_best[d]),
                    tau_cross: after(&next_cross[d]),
                    watched: end - 1 - t,
                });
            }
        }
    }
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
        best,
    }
}

impl PairSim {
    /// Minutes until a real lot entered at minute `t0` for `cost` would first
    /// have exited under the ENGINE'S rule rather than at par: proceeds of at
    /// least `cost + 0.02`, and never below what holding is worth at the
    /// hurdle (`floor(t)`, from `maker_exit::reservation_floor`). This is what
    /// the calibration compares our real exits against.
    pub fn engine_exit(&self, dir: Dir, t0: usize, cost: f64, floor: impl Fn(usize) -> f64) -> Option<usize> {
        let v = &self.best[usize::from(dir == Dir::KNo)];
        (t0 + 1..v.len()).find(|&t| v[t] >= (cost + 0.02).max(floor(t))).map(|t| t - t0)
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

    /// Kalshi cheap against PM-US: a Kalshi YES bid fills at minute 1, and the
    /// basis then closes until the basket sells for more than $1 at minute 5.
    #[test]
    fn an_entry_waits_for_the_first_par_exit() {
        let n = 10;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        k.lo[1] = 0.41; // a seller hits our 0.41 bid
        for t in 5..n {
            // Kalshi rallies past PM-US: sell K YES at 0.56, buy back PM-US YES at 0.52.
            (k.bid[t], k.ask[t]) = (0.56, 0.58);
        }
        let s = simulate(&k, &p, n);
        let e: Vec<&Entry> = s.entries.iter().filter(|e| e.dir == Dir::KYes).collect();
        assert_eq!(e.len(), 1);
        assert_eq!(e[0].t, 1);
        assert_eq!(e[0].shape, Shape::RestKalshi);
        let lock = 1.0 - e[0].cost;
        assert!(lock > 0.06 && lock < 0.08, "0.41 + 0.50 plus fees: {lock}");
        assert_eq!(e[0].tau_cross, Some(4), "0.56 + 0.48 - fees clears par at minute 5");
        assert_eq!(e[0].watched, n - 2);
    }

    /// The same quote filling every minute is one dislocation, not sixty.
    #[test]
    fn a_standing_fill_is_thinned_to_one_entry_an_hour() {
        let n = 200;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        k.lo = vec![0.40; n];
        let s = simulate(&k, &p, n);
        let ts: Vec<usize> = s.entries.iter().filter(|e| e.dir == Dir::KYes).map(|e| e.t).collect();
        assert_eq!(ts, vec![0, 60, 120, 180]);
        assert!(s.entries.iter().all(|e| e.tau_best.is_none()), "the basis never closes");
    }

    #[test]
    fn an_entry_without_a_lock_is_not_simulated() {
        let n = 5;
        let (mut k, p) = flat(n, (0.50, 0.55), (0.50, 0.52));
        k.lo = vec![0.49; n];
        assert!(simulate(&k, &p, n).entries.iter().all(|e| e.dir != Dir::KYes));
    }

    /// A crossed or one-sided book is not a price.
    #[test]
    fn a_crossed_book_is_no_minute_at_all() {
        let (k, p) = flat(3, (0.50, 0.45), (0.50, 0.52));
        assert_eq!(simulate(&k, &p, 3).valid_minutes, 0);
    }

    #[test]
    fn the_engine_rule_needs_two_cents_and_the_hold_value() {
        let n = 10;
        let (mut k, p) = flat(n, (0.40, 0.45), (0.50, 0.52));
        for t in 3..n {
            (k.bid[t], k.ask[t]) = (0.46, 0.48);
        }
        let s = simulate(&k, &p, n);
        let v = s.best[0][3];
        assert_eq!(s.engine_exit(Dir::KYes, 0, v - 0.025, |_| 0.0), Some(3));
        assert_eq!(s.engine_exit(Dir::KYes, 0, v - 0.015, |_| 0.0), None, "under 2c");
        assert_eq!(s.engine_exit(Dir::KYes, 0, v - 0.025, |_| 1.0), None, "under the hold value");
    }
}
