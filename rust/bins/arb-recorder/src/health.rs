//! Per-CONNECTION liveness + health.jsonl + 30s snapshot rebroadcast —
//! mirrors LivenessTracker/health_task in record/recorder.py (quiet markets
//! are healthy; silent connections are not).

use crate::core::Core;
use std::collections::HashMap;
use std::io::Write;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub struct Liveness {
    stale_after: Duration,
    inner: Mutex<LivenessInner>,
}

#[derive(Default)]
struct LivenessInner {
    last: HashMap<String, Instant>,
    stale_seconds: HashMap<String, f64>,
}

impl Liveness {
    pub fn new(stale_after_s: f64) -> Self {
        Self {
            stale_after: Duration::from_secs_f64(stale_after_s),
            inner: Mutex::new(LivenessInner::default()),
        }
    }

    pub fn beat(&self, conn: &str) {
        self.inner.lock().expect("liveness lock").last.insert(conn.to_owned(), Instant::now());
    }

    pub fn check(&self) -> HashMap<String, bool> {
        let mut inner = self.inner.lock().expect("liveness lock");
        let now = Instant::now();
        let stale_after = self.stale_after;
        let statuses: HashMap<String, bool> = inner
            .last
            .iter()
            .map(|(c, t)| (c.clone(), now.duration_since(*t) > stale_after))
            .collect();
        for (conn, stale) in &statuses {
            if *stale {
                *inner.stale_seconds.entry(conn.clone()).or_insert(0.0) += 1.0;
            }
        }
        statuses
    }

    pub fn stale_seconds(&self) -> HashMap<String, f64> {
        self.inner.lock().expect("liveness lock").stale_seconds.clone()
    }
}

pub async fn health_task(
    liveness: Arc<Liveness>,
    core: Arc<Core>,
    health_path: String,
    ntfy_topic: Option<String>,
    resync_every_s: f64,
) {
    let client = reqwest::Client::new();
    let mut alerted: std::collections::HashSet<String> = Default::default();
    let mut since_resync = 0.0f64;
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        since_resync += 1.0;
        if since_resync >= resync_every_s {
            since_resync = 0.0;
            core.rebroadcast_snapshots();
        }
        let status = liveness.check();
        for (conn, stale) in &status {
            if *stale && !alerted.contains(conn) {
                alerted.insert(conn.clone());
                if let Some(topic) = &ntfy_topic {
                    let _ = client
                        .post(format!("https://ntfy.sh/{topic}"))
                        .body(format!("arbbot recorder-rs: feed '{conn}' stale"))
                        .send()
                        .await;
                }
            } else if !*stale {
                alerted.remove(conn);
            }
        }
        let line = serde_json::json!({
            "ts": std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0),
            "stale": status,
            "stale_seconds_total": liveness.stale_seconds(),
        });
        if let Ok(mut f) =
            std::fs::OpenOptions::new().create(true).append(true).open(&health_path)
        {
            let _ = writeln!(f, "{line}");
        }
    }
}
