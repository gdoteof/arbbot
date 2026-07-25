//! Engine-sequenced write-ahead log (P4 provable #2, docs/p3-shell.md).
//!
//! Every event that leaves the feed channel is stamped, at the engine's single
//! merge point, with a monotonically increasing sequence number and appended
//! verbatim:
//!
//!     {"seq":N,"line":"<the original feed line, JSON-escaped>"}
//!
//! `seq` is the engine's merge order — the only order that matters, since the
//! engine is a pure fold over that sequence. Replaying the embedded lines
//! through the identical engine path (`--replay-wal`) therefore reproduces an
//! incident byte-exactly, intent digest included. The line is stored as an
//! opaque string, not re-parsed JSON: a re-serialized event is a different
//! event, and events the engine skips (unknown kinds, malformed lines) are
//! part of the incident too.
//!
//! `seq` restarts at 1 each process run; the file is opened append-only (a WAL
//! never clobbers the previous run's record), so file order — not the seq
//! value alone — is the replay authority across a restart boundary.
//!
//! The writer never runs in the engine task: the engine `try_send`s to a
//! bounded channel drained by a dedicated OS thread, so a slow disk backs up
//! the WAL queue and nothing else. The engine's own loop does no file I/O.
//!
//! **Overflow policy: crash-stop.** If the bounded queue is full (or the
//! writer thread has died), the process exits immediately instead of skipping
//! the line. A WAL with silent holes is worse than no WAL: it replays
//! "successfully" into a state the live engine never occupied, so an incident
//! review draws conclusions from a fiction. Refusing to run is the honest
//! failure. (The buffered tail is lost on that exit — but the hole that caused
//! it is already unrecoverable, so there is nothing left worth saving.)

use std::io::Write;
use tokio::sync::mpsc;

/// Same order of magnitude as the feed channel: absorbs a full burst without
/// the engine ever waiting on the disk.
const WAL_QUEUE: usize = 65536;

pub struct Wal {
    tx: mpsc::Sender<String>,
    next_seq: u64,
}

impl Wal {
    pub fn spawn(path: &str) -> Wal {
        if let Some(dir) = std::path::Path::new(path).parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir).expect("wal dir");
            }
        }
        let f = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .expect("open wal");
        let (tx, mut rx) = mpsc::channel::<String>(WAL_QUEUE);
        std::thread::Builder::new()
            .name("wal".into())
            .spawn(move || {
                let mut w = std::io::BufWriter::with_capacity(1 << 20, f);
                while let Some(rec) = rx.blocking_recv() {
                    w.write_all(rec.as_bytes()).expect("wal write");
                    w.write_all(b"\n").expect("wal write");
                    // Caught up => flush, so a live WAL is at most one event
                    // behind the engine. Under a burst the buffer fills and
                    // amortizes instead.
                    if rx.is_empty() {
                        w.flush().expect("wal flush");
                    }
                }
                w.flush().expect("wal final flush");
            })
            .expect("spawn wal thread");
        Wal { tx, next_seq: 0 }
    }

    /// Stamp and enqueue one merged event. Never blocks; see the crash-stop
    /// policy above.
    pub fn append(&mut self, line: &str) {
        self.next_seq += 1;
        let rec = format!(
            "{{\"seq\":{},\"line\":{}}}",
            self.next_seq,
            serde_json::to_string(line).expect("json string")
        );
        if self.tx.try_send(rec).is_err() {
            eprintln!(
                "[wal] FATAL: queue full or writer dead at seq {} — crash-stop \
                 rather than continue with a WAL that has a silent hole",
                self.next_seq
            );
            std::process::exit(70);
        }
    }
}
