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
        let mean = if count > 0 { self.sum.load(Ordering::Relaxed) / count } else { 0 };
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
