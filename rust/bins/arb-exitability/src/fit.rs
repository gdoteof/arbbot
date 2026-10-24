//! From simulated entries to a per-pair exit probability. Pure.
//!
//! `p` is the chance a lot exits back at par within `HORIZON_MIN` of real
//! time. Three corrections stand between the tape and that number:
//!
//!   1. **Censoring.** An entry near the end of the window was watched for
//!      less time than one near the start; Kaplan-Meier counts each for as
//!      long as it was watched instead of calling the late ones failures.
//!   2. **Calibration.** The tape says when an exit was AVAILABLE; real exits
//!      come later (queue position, one exit at a time, blackouts). `k` is how
//!      much later, measured on our own lots, and real time `h` is read off the
//!      tape's curve at `h / k`.
//!   3. **Shrinkage.** A pair with a few days of entries says little on its
//!      own, so its estimate is pulled toward its family's, and the family's
//!      toward everything's, in proportion to how many days each has seen.
//!      Entry DAYS, not entries: hourly entries inside one dislocation share
//!      one outcome, so they are not independent evidence.

/// The horizon `p` is measured over. Past it, a lot is assumed held to
/// resolution — no credit for an exit the window was too short to see.
pub const HORIZON_MIN: f64 = 14.0 * 1440.0;
/// Pseudo-days of prior weight in the shrinkage.
pub const PRIOR_DAYS: f64 = 5.0;
/// One-sided 80% normal quantile, for `p_lcb`.
const Z80: f64 = 1.2816;
/// Below this many calibration lots the slowdown is not estimated at all.
pub const MIN_CALIBRATION_LOTS: usize = 10;
/// The slowdown is clamped here. Below 1 would credit us with exits faster
/// than the tape offered them; above 20 a 14-day horizon reads under a day.
const K_RANGE: (f64, f64) = (1.0, 20.0);

/// One observation: minutes until the exit, or until watching stopped.
#[derive(Clone, Copy, Debug)]
pub struct Obs {
    pub minutes: f64,
    pub exited: bool,
}

/// Kaplan-Meier estimate of P(exit within `at` minutes).
pub fn km(obs: &[Obs], at: f64) -> f64 {
    let mut o: Vec<Obs> = obs.to_vec();
    // At a tie, exits before censorings: a lot censored at t was still at risk at t.
    o.sort_by(|a, b| a.minutes.total_cmp(&b.minutes).then(b.exited.cmp(&a.exited)));
    let mut at_risk = o.len() as f64;
    let mut survive = 1.0;
    let mut i = 0;
    while i < o.len() && o[i].minutes <= at {
        let t = o[i].minutes;
        let (mut exits, mut gone) = (0.0, 0.0);
        while i < o.len() && o[i].minutes == t {
            if o[i].exited {
                exits += 1.0;
            }
            gone += 1.0;
            i += 1;
        }
        if at_risk > 0.0 {
            survive *= 1.0 - exits / at_risk;
        }
        at_risk -= gone;
    }
    1.0 - survive
}

#[derive(Clone, Debug, PartialEq)]
pub struct Calibration {
    /// Real exit time over the tape's first-available exit time.
    pub k: f64,
    pub lots: usize,
    /// The median ratio, reported beside `k` because the two disagree: most
    /// lots exit close to the first chance, and a minority miss it and wait
    /// for a later one.
    pub median_ratio: Option<f64>,
    pub estimated: bool,
}

/// `k` = the geometric mean of (real minutes / tape minutes) over our lots,
/// clamped to `K_RANGE`. Unestimated (k = 1) below `MIN_CALIBRATION_LOTS`.
pub fn calibrate(pairs: &[(f64, f64)]) -> Calibration {
    let mut ratios: Vec<f64> = pairs
        .iter()
        .filter(|(real, sim)| real.is_finite() && sim.is_finite())
        .map(|(real, sim)| real.max(1.0) / sim.max(1.0))
        .collect();
    ratios.sort_by(f64::total_cmp);
    let median_ratio = (!ratios.is_empty()).then(|| ratios[ratios.len() / 2]);
    if ratios.len() < MIN_CALIBRATION_LOTS {
        return Calibration { k: 1.0, lots: ratios.len(), median_ratio, estimated: false };
    }
    let geo = (ratios.iter().map(|r| r.ln()).sum::<f64>() / ratios.len() as f64).exp();
    Calibration { k: geo.clamp(K_RANGE.0, K_RANGE.1), lots: ratios.len(), median_ratio, estimated: true }
}

/// The curve's sample points, in days.
pub const CURVE_DAYS: [f64; 7] = [1.0 / 24.0, 0.25, 1.0, 2.0, 3.0, 7.0, 14.0];

#[derive(Clone, Debug)]
pub struct Survival {
    /// P(exit within `HORIZON_MIN`) on the tape, uncalibrated.
    pub p_tape: f64,
    /// The same in real time: the tape's curve read at `HORIZON_MIN / k`.
    pub p: f64,
    /// Mean real minutes to exit among lots that exit inside the horizon.
    pub mean_exit_min: Option<f64>,
    pub median_exit_min: Option<f64>,
    /// `CURVE_DAYS` on the tape and in real time.
    pub curve_tape: Vec<f64>,
    pub curve: Vec<f64>,
}

pub fn survival(obs: &[Obs], k: f64) -> Survival {
    let mut inside: Vec<f64> =
        obs.iter().filter(|o| o.exited && o.minutes <= HORIZON_MIN / k).map(|o| o.minutes * k).collect();
    inside.sort_by(f64::total_cmp);
    let mean = (!inside.is_empty()).then(|| inside.iter().sum::<f64>() / inside.len() as f64);
    Survival {
        p_tape: km(obs, HORIZON_MIN),
        p: km(obs, HORIZON_MIN / k),
        mean_exit_min: mean,
        median_exit_min: (!inside.is_empty()).then(|| inside[inside.len() / 2]),
        curve_tape: CURVE_DAYS.iter().map(|d| km(obs, d * 1440.0)).collect(),
        curve: CURVE_DAYS.iter().map(|d| km(obs, d * 1440.0 / k)).collect(),
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

    fn ev(m: f64) -> Obs {
        Obs { minutes: m, exited: true }
    }

    fn cens(m: f64) -> Obs {
        Obs { minutes: m, exited: false }
    }

    fn close(a: f64, b: f64) -> bool {
        (a - b).abs() < 1e-9
    }

    #[test]
    fn km_without_censoring_is_the_plain_fraction() {
        let o = [ev(1.0), ev(2.0), ev(5.0), cens(100.0)];
        assert!(close(km(&o, 2.0), 0.5));
        assert!(close(km(&o, 0.5), 0.0));
        assert!(close(km(&o, 1000.0), 0.75));
    }

    /// A lot watched for only 3 minutes is not a failure at 10: it leaves the
    /// risk set, and the rest decide.
    #[test]
    fn a_short_watch_is_not_counted_as_a_failure() {
        let o = [ev(1.0), cens(3.0), ev(5.0), cens(20.0)];
        // S(1) = 3/4; censor at 3 leaves 2 at risk; S(5) = 3/4 * 1/2.
        assert!(close(km(&o, 10.0), 1.0 - 0.375));
    }

    #[test]
    fn an_exit_and_a_censoring_at_the_same_minute_count_the_exit_first() {
        let o = [ev(2.0), cens(2.0)];
        assert!(close(km(&o, 2.0), 0.5));
    }

    #[test]
    fn calibration_is_the_geometric_mean_and_refuses_thin_samples() {
        let few = calibrate(&[(40.0, 10.0); 3]);
        assert!(!few.estimated && close(few.k, 1.0));
        let mut pairs = vec![(10.0, 10.0); 6];
        pairs.extend(vec![(160.0, 10.0); 6]);
        let c = calibrate(&pairs);
        assert!(c.estimated && close(c.k, 4.0), "sqrt(1 * 16) = 4, got {}", c.k);
        assert_eq!(c.lots, 12);
    }

    #[test]
    fn calibration_never_credits_exits_faster_than_the_tape() {
        let c = calibrate(&[(1.0, 50.0); 20]);
        assert!(close(c.k, 1.0));
    }

    /// A slowdown of 4 reads the 14-day horizon off the tape curve at 3.5 days.
    #[test]
    fn calibration_reads_the_curve_earlier() {
        let o = [ev(2.0 * 1440.0), ev(5.0 * 1440.0), cens(30.0 * 1440.0)];
        let s = survival(&o, 4.0);
        assert!(close(s.p_tape, 2.0 / 3.0));
        assert!(close(s.p, 1.0 / 3.0));
        assert_eq!(s.mean_exit_min, Some(8.0 * 1440.0), "the 2-day exit, in real time");
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
