//! Lock-free log2-bucket latency histogram — O(1) memory for unbounded
//! soaks (a Vec of raw samples would grow ~80MB/day). Percentiles are
//! bucket-resolution approximations (each bucket spans [2^i, 2^{i+1}) ns);
//! max is exact.

use std::sync::atomic::{AtomicU64, Ordering};

const NBUCKETS: usize = 48;

pub struct Hist {
    buckets: [AtomicU64; NBUCKETS],
    count: AtomicU64,
    sum: AtomicU64,
    max: AtomicU64,
}

impl Hist {
    pub fn new() -> Self {
        Hist {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum: AtomicU64::new(0),
            max: AtomicU64::new(0),
        }
    }

    pub fn record(&self, ns: u64) {
        let b = (63 - ns.max(1).leading_zeros() as usize).min(NBUCKETS - 1);
        self.buckets[b].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(ns, Ordering::Relaxed);
        self.max.fetch_max(ns, Ordering::Relaxed);
    }

    fn percentile(&self, p: f64) -> u64 {
        let total = self.count.load(Ordering::Relaxed);
        if total == 0 {
            return 0;
        }
        let target = ((p * total as f64).ceil() as u64).max(1);
        let mut seen = 0u64;
        for (i, b) in self.buckets.iter().enumerate() {
            seen += b.load(Ordering::Relaxed);
            if seen >= target {
                // geometric midpoint of [2^i, 2^{i+1})
                return (1u64 << i) + (1u64 << i) / 2;
            }
        }
        self.max.load(Ordering::Relaxed)
    }

    pub fn summary(&self) -> serde_json::Value {
        let count = self.count.load(Ordering::Relaxed);
        let sum = self.sum.load(Ordering::Relaxed);
        let mean = sum.checked_div(count).unwrap_or(0);
        serde_json::json!({
            "count": count,
            "mean_ns": mean,
            "p50_ns": self.percentile(0.50),
            "p90_ns": self.percentile(0.90),
            "p99_ns": self.percentile(0.99),
            "p999_ns": self.percentile(0.999),
            "max_ns": self.max.load(Ordering::Relaxed),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// An empty histogram reports zeroes rather than dividing by zero. These two
    /// tests exist to pin the exact numbers `summary()` emits, so that reworking
    /// the reporting cannot move them by accident.
    #[test]
    fn an_empty_histogram_reports_zeroes() {
        let s = Hist::new().summary();
        assert_eq!(s["count"], 0);
        assert_eq!(s["mean_ns"], 0);
        assert_eq!(s["p50_ns"], 0);
        assert_eq!(s["max_ns"], 0);
    }

    /// `mean_ns` is the exact sum over the exact count (integer division);
    /// percentiles are the midpoint `1.5 * 2^i` of the bucket the target sample
    /// lands in, so they are only accurate to a factor of two; `max_ns` is exact.
    ///
    /// NOTE the `p99 > max` below (24576 > 20000). That is not a typo: a bucket
    /// midpoint can exceed every sample in the bucket, so a percentile can report
    /// higher than the exact maximum. This test pins TODAY's behaviour, warts
    /// included. Whoever reworks the percentile reporting should change this
    /// assertion deliberately, not assume it was describing something correct.
    #[test]
    fn mean_is_exact_and_percentiles_are_bucket_midpoints() {
        let h = Hist::new();
        for ns in [1_000u64, 2_000, 3_000, 20_000] {
            h.record(ns);
        }
        let s = h.summary();
        assert_eq!(s["count"], 4);
        assert_eq!(s["mean_ns"], 6_500); // 26_000 / 4
        assert_eq!(s["max_ns"], 20_000);
        assert_eq!(s["p50_ns"], 1_536); // 2nd of 4 is in bucket 10 = [1024, 2048)
        assert_eq!(s["p99_ns"], 24_576); // 4th of 4 is in bucket 14 = [16384, 32768)
    }
}
