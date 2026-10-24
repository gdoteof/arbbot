//! From simulated entries to a per-pair exit probability. Pure.
//!
//! `p` is the chance a lot gets the engine's exit within `HORIZON_MIN`. Three
//! corrections stand between the tape and that number:
//!
//!   1. **The take.** The tape says when an exit was AVAILABLE, as one chance
//!      per hour that cleared the rule (`sim::chances`). The engine takes some
//!      and misses the rest: queue position, depth for a whole lot, one exit
//!      at a time, blackouts. Our own lots split two ways: most that get out
//!      take one of their first few chances, and some are offered dozens and
//!      take none. So a `Take` is a share of lots the engine can get out at
//!      all and the rate it takes each chance one of those is offered, both
//!      measured on our lots. One rate alone would be set by the lots that
//!      never get out, and read every pair by how many chances it offers.
//!   2. **Censoring.** An entry near the end of the window was watched for
//!      less time than one near the start; Kaplan-Meier counts each for as
//!      long as it was watched instead of calling the late ones failures.
//!   3. **Shrinkage.** A pair with a few days of entries says little on its
//!      own, so its estimate is pulled toward its family's, and the family's
//!      toward everything's, in proportion to how many days each has seen.
//!      Entry DAYS, not entries: hourly entries inside one dislocation share
//!      one outcome, so they are not independent evidence.

use crate::sim::Exit;

/// The horizon `p` is measured over. Past it, a lot is assumed held to
/// resolution — no credit for an exit the window was too short to see.
pub const HORIZON_MIN: f64 = 14.0 * 1440.0;
/// Pseudo-days of prior weight in the shrinkage.
pub const PRIOR_DAYS: f64 = 5.0;
/// One-sided 80% normal quantile, for `p_lcb`.
const Z80: f64 = 1.2816;
/// Below this many calibration lots the take rate is not estimated at all.
pub const MIN_CALIBRATION_LOTS: usize = 10;

/// One simulated entry: its exit chances, what it cost, and how many minutes
/// it was watched.
#[derive(Clone, Copy)]
pub struct Obs<'a> {
    pub chances: &'a [Exit],
    pub cost: f64,
    pub watched: f64,
}

/// What share of lots the engine can get out at all, and the chance it takes
/// each exit chance one of those is offered.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Take {
    pub share: f64,
    pub rate: f64,
}

impl Take {
    /// Every chance taken: the tape alone.
    pub const TAPE: Take = Take { share: 1.0, rate: 1.0 };
}

/// The pooled exit distribution under `take`: Kaplan-Meier over expected
/// counts. Of an entry, `share` can get out; at its j-th chance
/// (1 - rate)^(j - 1) of that is still held, and `rate` of it exits there.
/// The rest never does. The entry leaves the risk set when watching stops
/// (exits first at a tie: an entry censored at t was still at risk at t).
/// With `Take::TAPE` this is plain Kaplan-Meier on the first chance.
///
/// Returns (minute, probability mass, mean proceeds over cost) per chance minute.
fn jumps(obs: &[Obs], take: Take) -> Vec<(f64, f64, f64)> {
    // (minute, watching stopped, entry, proceeds over cost)
    let mut ev: Vec<(f64, bool, usize, f64)> = Vec::new();
    for (i, o) in obs.iter().enumerate() {
        ev.extend(o.chances.iter().map(|x| (x.minutes as f64, false, i, x.proceeds - o.cost)));
        ev.push((o.watched, true, i, 0.0));
    }
    ev.sort_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
    // What of each entry could still get out; `1 - share` of it never will.
    let mut held = vec![take.share; obs.len()];
    let mut at_risk = obs.len() as f64;
    let mut survive = 1.0;
    let mut out = Vec::new();
    let mut i = 0;
    while i < ev.len() {
        let t = ev[i].0;
        let (mut exits, mut gain) = (0.0, 0.0);
        while i < ev.len() && ev[i].0 == t && !ev[i].1 {
            let (_, _, e, g) = ev[i];
            let w = held[e] * take.rate;
            (exits, gain, held[e]) = (exits + w, gain + w * g, held[e] - w);
            i += 1;
        }
        if exits > 0.0 && at_risk > 1e-12 {
            let mass = survive * exits / at_risk;
            out.push((t, mass, gain / exits));
            survive -= mass;
        }
        at_risk -= exits;
        while i < ev.len() && ev[i].0 == t {
            at_risk -= held[ev[i].2] + (1.0 - take.share);
            held[ev[i].2] = 0.0;
            i += 1;
        }
    }
    out
}

fn cdf(jumps: &[(f64, f64, f64)], at: f64) -> f64 {
    jumps.iter().take_while(|x| x.0 <= at).map(|x| x.1).sum()
}

/// The first minute by which `q` of the mass in `jumps` has arrived.
fn quantile(jumps: &[(f64, f64, f64)], q: f64) -> Option<f64> {
    let mut f = 0.0;
    jumps
        .iter()
        .find(|x| {
            f += x.1;
            f >= q - 1e-12
        })
        .map(|x| x.0)
}

#[derive(Clone, Debug, PartialEq)]
pub struct Calibration {
    pub take: Take,
    pub lots: usize,
    /// Chances our lots were offered, through the one taken or until now.
    pub offered: usize,
    pub taken: usize,
    pub estimated: bool,
}

/// The `Take` from `(chances offered, out)` per lot, by maximum likelihood on
/// a grid: a lot out at its n-th chance has likelihood
/// share * rate * (1 - rate)^(n - 1), and one not out after n chances,
/// 1 - share + share * (1 - rate)^n. A lot offered no chance says nothing.
/// Unestimated (`Take::TAPE`) below `MIN_CALIBRATION_LOTS`.
pub fn calibrate(lots: &[(usize, bool)]) -> Calibration {
    let lots: Vec<(usize, bool)> = lots.iter().copied().filter(|(n, _)| *n > 0).collect();
    let offered = lots.iter().map(|(n, _)| n).sum();
    let taken = lots.iter().filter(|(_, out)| *out).count();
    let estimated = lots.len() >= MIN_CALIBRATION_LOTS;
    let ll = |s: f64, r: f64| -> f64 {
        lots.iter()
            .map(|&(n, out)| {
                let miss = (1.0 - r).powi(n as i32 - 1);
                if out { (s * r * miss).ln() } else { (1.0 - s + s * miss * (1.0 - r)).ln() }
            })
            .sum()
    };
    let grid = (1..=GRID).map(|i| i as f64 / GRID as f64);
    let take = if !estimated {
        Take::TAPE
    } else {
        grid.clone()
            .flat_map(|s| grid.clone().map(move |r| Take { share: s, rate: r }))
            .max_by(|a, b| ll(a.share, a.rate).total_cmp(&ll(b.share, b.rate)))
            .expect("a non-empty grid")
    };
    Calibration { take, lots: lots.len(), offered, taken, estimated }
}

/// Steps per unit of the calibration grid.
const GRID: usize = 200;

/// The curve's sample points, in days.
pub const CURVE_DAYS: [f64; 7] = [1.0 / 24.0, 0.25, 1.0, 2.0, 3.0, 7.0, 14.0];

#[derive(Clone, Debug)]
pub struct Survival {
    /// P(exit within `HORIZON_MIN`) were every chance taken: the tape alone.
    pub p_tape: f64,
    /// The same at the take.
    pub p: f64,
    /// Mean minutes to the exit taken, among lots out inside the horizon,
    /// from the estimator rather than the exits seen: an entry late in the
    /// window can only show a quick exit, so the plain mean runs short.
    pub mean_exit_min: Option<f64>,
    pub median_exit_min: Option<f64>,
    /// Mean proceeds over cost of the exit taken, among the same lots.
    pub mean_gain: Option<f64>,
    /// `CURVE_DAYS`, on the tape alone and at the take.
    pub curve_tape: Vec<f64>,
    pub curve: Vec<f64>,
}

pub fn survival(obs: &[Obs], take: Take) -> Survival {
    let tape = jumps(obs, Take::TAPE);
    let j: Vec<_> = jumps(obs, take).into_iter().filter(|x| x.0 <= HORIZON_MIN).collect();
    let mass: f64 = j.iter().map(|x| x.1).sum();
    let mean = |f: fn(&(f64, f64, f64)) -> f64| (mass > 0.0).then(|| j.iter().map(|x| x.1 * f(x)).sum::<f64>() / mass);
    Survival {
        p_tape: cdf(&tape, HORIZON_MIN),
        p: mass,
        mean_exit_min: mean(|x| x.0),
        median_exit_min: quantile(&j, mass / 2.0).filter(|_| mass > 0.0),
        mean_gain: mean(|x| x.2),
        curve_tape: CURVE_DAYS.iter().map(|d| cdf(&tape, d * 1440.0)).collect(),
        curve: CURVE_DAYS.iter().map(|d| cdf(&j, d * 1440.0)).collect(),
    }
}

/// `p` pulled toward `prior` with `PRIOR_DAYS` of weight.
pub fn shrink(p: f64, days: f64, prior: f64) -> f64 {
    (days * p + PRIOR_DAYS * prior) / (days + PRIOR_DAYS)
}

/// A one-sided 80% lower bound on a shrunk `p` resting on `days` of evidence.
pub fn lower_bound(p: f64, days: f64) -> f64 {
    (p - Z80 * (p * (1.0 - p) / (days + PRIOR_DAYS)).sqrt()).max(0.0)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Entries from (chance minutes, minutes watched); every chance sells for
    /// 2c over cost.
    fn entries(spec: &[(&[usize], f64)]) -> Vec<(Vec<Exit>, f64)> {
        spec.iter()
            .map(|(m, w)| (m.iter().map(|&minutes| Exit { minutes, proceeds: 0.97 }).collect(), *w))
            .collect()
    }

    fn obs(e: &[(Vec<Exit>, f64)]) -> Vec<Obs<'_>> {
        e.iter().map(|(chances, w)| Obs { chances, cost: 0.95, watched: *w }).collect()
    }

    fn km(e: &[(Vec<Exit>, f64)], at: f64) -> f64 {
        cdf(&jumps(&obs(e), Take::TAPE), at)
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn km_without_censoring_is_the_plain_fraction() {
        let e = entries(&[(&[1], 1000.0), (&[2], 1000.0), (&[5], 1000.0), (&[], 100.0)]);
        assert!(close(km(&e, 2.0), 0.5));
        assert!(close(km(&e, 0.5), 0.0));
        assert!(close(km(&e, 1000.0), 0.75));
    }

    /// A lot watched for only 3 minutes is not a failure at 10: it leaves the
    /// risk set, and the rest decide.
    #[test]
    fn a_short_watch_is_not_counted_as_a_failure() {
        let e = entries(&[(&[1], 1000.0), (&[], 3.0), (&[5], 1000.0), (&[], 20.0)]);
        // S(1) = 3/4; censor at 3 leaves 2 at risk; S(5) = 3/4 * 1/2.
        assert!(close(km(&e, 10.0), 1.0 - 0.375));
    }

    #[test]
    fn an_exit_and_a_censoring_at_the_same_minute_count_the_exit_first() {
        let e = entries(&[(&[2], 1000.0), (&[], 2.0)]);
        assert!(close(km(&e, 2.0), 0.5));
    }

    /// Three chances at a take rate of one half: out at the first with 1/2,
    /// the second with 1/4, the third with 1/8.
    #[test]
    fn each_chance_is_taken_at_the_take_rate_until_one_is() {
        let e = entries(&[(&[60, 120, 180], 1000.0)]);
        let s = survival(&obs(&e), Take { share: 1.0, rate: 0.5 });
        assert!(close(s.p_tape, 1.0));
        assert!(close(s.p, 0.875));
        assert!(close(s.mean_exit_min.unwrap(), (0.5 * 60.0 + 0.25 * 120.0 + 0.125 * 180.0) / 0.875));
        assert_eq!(s.median_exit_min, Some(60.0), "half of the 7/8 is out at the first chance");
        assert!(close(s.mean_gain.unwrap(), 0.02));
    }

    /// B is watched past its first chance but not to A's second, so the
    /// estimate lets A speak for it there: 1/2 out at 60, then half the rest.
    #[test]
    fn a_take_rate_and_censoring_combine() {
        let e = entries(&[(&[60, 120], 1000.0), (&[60], 90.0)]);
        assert!(close(survival(&obs(&e), Take { share: 1.0, rate: 0.5 }).p, 0.75));
    }

    /// With a share of 0.6, only 0.6 of an entry can ever get out, but the
    /// rest stays at risk: it is a lot still held, not one gone.
    #[test]
    fn the_share_that_never_gets_out_stays_at_risk() {
        let e = entries(&[(&[60], 1000.0), (&[], 30.0), (&[120], 1000.0)]);
        // The second leaves at 30. At 60, 0.6 of the first exits out of 2 at
        // risk; at 120, 0.6 of the third out of the 1.4 left, which still
        // counts the 0.4 of the first that never gets out.
        let s = survival(&obs(&e), Take { share: 0.6, rate: 1.0 });
        assert!(close(s.p, 0.6 / 2.0 + (1.0 - 0.6 / 2.0) * 0.6 / 1.4), "{}", s.p);
    }

    /// One entry exits on day 1, one is watched only a day and a half, one
    /// exits on day 10. The exits seen average 5.5 days; but the day-10 exit
    /// carries the censored entry's share too, so the estimate is 6.4.
    #[test]
    fn the_mean_exit_time_counts_what_censoring_hid() {
        let d = 1440.0;
        let e = entries(&[(&[1440], 30.0 * d), (&[], 1.5 * d), (&[14_400], 30.0 * d), (&[], 30.0 * d)]);
        let s = survival(&obs(&e), Take::TAPE);
        assert!(close(s.p, 0.625));
        assert!(close(s.mean_exit_min.unwrap(), 6.4 * d), "{:?}", s.mean_exit_min);
        assert_eq!(s.median_exit_min, Some(10.0 * d));
    }

    /// Half our lots out at their first chance and half offered a hundred and
    /// taking none: half are catchable and those are caught at once, which
    /// one rate (10 of 1010) would call a 1% take of every chance.
    #[test]
    fn lots_that_never_get_out_are_a_share_not_a_slow_rate() {
        let mut lots = vec![(1, true); 10];
        lots.extend(vec![(100, false); 10]);
        lots.push((0, false));
        let c = calibrate(&lots);
        assert!(c.estimated && (c.take.share - 0.5).abs() < 0.01 && c.take.rate > 0.95, "{:?}", c.take);
        assert_eq!((c.lots, c.offered, c.taken), (20, 1010, 10), "the lot offered nothing is left out");
        let all_second = calibrate(&[(2, true); 12]);
        assert_eq!(all_second.take, Take { share: 1.0, rate: 0.5 }, "every lot out, each at its second chance");
        let few = calibrate(&[(3, true); 3]);
        assert!(!few.estimated && few.take == Take::TAPE);
    }

    #[test]
    fn shrinkage_and_the_bound_move_the_right_way() {
        assert!(close(shrink(1.0, 5.0, 0.0), 0.5));
        assert!(close(shrink(0.8, 0.0, 0.3), 0.3), "no evidence, all prior");
        assert!(lower_bound(0.6, 5.0) < lower_bound(0.6, 50.0));
        assert!(lower_bound(0.6, 50.0) < 0.6);
        assert!(close(lower_bound(0.0, 1.0), 0.0));
    }
}
