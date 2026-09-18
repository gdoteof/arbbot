//! Legal passive prices from the venue's per-market price ranges.
use crate::scan::{Cx, D};
use std::cmp::Ordering;

#[derive(Clone, Debug)]
pub struct PriceGrid(Vec<D>);

impl PriceGrid {
    pub fn from_ranges(ranges: &[(String, String, String)]) -> Option<Self> {
        let mut cx = Cx::default();
        let mut end = cx.zero();
        let min_step = cx.parse_exact("0.0001");
        let mut prices = Vec::new();
        for (start, stop, step) in ranges {
            let (start, stop, step) = (cx.parse(start)?, cx.parse(stop)?, cx.parse(step)?);
            if !start.is_finite() || !stop.is_finite() || !step.is_finite()
                || cx.cmp(start, end) != Ordering::Equal
                || cx.cmp(stop, start) != Ordering::Greater
                || cx.cmp(stop, cx.one) == Ordering::Greater
                || cx.cmp(step, min_step) == Ordering::Less {
                return None;
            }
            let units = cx.div(step, min_step);
            let integral = cx.quantize_int_down(units);
            if cx.cmp(units, integral) != Ordering::Equal { return None; }
            let mut p = start;
            while cx.cmp(p, stop) == Ordering::Less {
                if cx.is_pos(p) { prices.push(p); }
                p = cx.add(p, step);
                if prices.len() > 10_000 { return None; }
            }
            // Adjacent bands must meet on the grid, not introduce a gap.
            if cx.cmp(p, stop) != Ordering::Equal { return None; }
            end = stop;
        }
        (cx.cmp(end, cx.one) == Ordering::Equal && !prices.is_empty()).then_some(Self(prices))
    }

    pub fn cents() -> &'static Self {
        static GRID: std::sync::OnceLock<PriceGrid> = std::sync::OnceLock::new();
        GRID.get_or_init(|| Self::from_ranges(&[("0".into(), "1".into(), "0.01".into())]).unwrap())
    }

    pub fn floor(&self, cx: &mut Cx, p: D) -> Option<D> {
        let i = self.0.partition_point(|x| cx.cmp(*x, p) != Ordering::Greater);
        i.checked_sub(1).map(|j| self.0[j])
    }
    pub fn ceil(&self, cx: &mut Cx, p: D) -> Option<D> {
        let i = self.0.partition_point(|x| cx.cmp(*x, p) == Ordering::Less);
        self.0.get(i).copied()
    }
    pub fn below(&self, cx: &mut Cx, p: D) -> Option<D> {
        let i = self.0.partition_point(|x| cx.cmp(*x, p) == Ordering::Less);
        i.checked_sub(1).map(|j| self.0[j])
    }
    pub fn above(&self, cx: &mut Cx, p: D) -> Option<D> {
        let i = self.0.partition_point(|x| cx.cmp(*x, p) != Ordering::Greater);
        self.0.get(i).copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tapered_band_boundaries_use_the_grid_on_the_correct_side() {
        let grid = PriceGrid::from_ranges(&[
            ("0".into(), "0.10".into(), "0.001".into()),
            ("0.10".into(), "0.90".into(), "0.01".into()),
            ("0.90".into(), "1".into(), "0.001".into()),
        ]).unwrap();
        let mut cx = Cx::default();
        let p = cx.parse_exact("0.10");
        assert_eq!(grid.below(&mut cx, p), Some(cx.parse_exact("0.099")));
        assert_eq!(grid.above(&mut cx, p), Some(cx.parse_exact("0.11")));
        let p = cx.parse_exact("0.90");
        assert_eq!(grid.below(&mut cx, p), Some(cx.parse_exact("0.89")));
        assert_eq!(grid.above(&mut cx, p), Some(cx.parse_exact("0.901")));
    }
    #[test]
    fn bad_ranges_refuse_instead_of_guessing_a_tick() {
        for step in ["0", "-1", "NaN", "Infinity", "0.00001", "0.03"] {
            assert!(PriceGrid::from_ranges(&[("0".into(), "1".into(), step.into())]).is_none());
        }
        assert!(PriceGrid::from_ranges(&[]).is_none());
    }
}
