//! Unix-socket fan-out — the line-JSON protocol every Python subscriber
//! already speaks, verbatim. Mirrors src/arbbot/record/recorder.py
//! UnixBroadcaster: welcome snapshots on connect, slow subscribers are
//! DISCONNECTED (never allowed to backpressure the recorder), periodic
//! rebroadcast heals gapped subscribers.
//!
//! Implementation: one writer task per subscriber fed by an unbounded
//! channel with byte accounting; queued bytes over `max_buffer` drops the
//! subscriber — same policy as the Python transport-buffer check.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::mpsc;

// Bytes queued per subscriber before it is dropped as slow. NOT 1MB: the
// welcome and the 30s rebroadcast heal enqueue the whole universe's books
// (~1.4MB today) SYNCHRONOUSLY before the writer task gets scheduled to
// drain, so a 1MB cap dropped even fast subscribers once per burst — found
// 2026-07-23 by the first real subscriber of the rs socket (arb-trader
// shadow flapping on exact 30s boundaries). 16MB never triggers on a burst
// but still sheds a genuinely stalled subscriber within ~35s of recorded
// peak rate (the Python recorder's transport-buffer policy is measured at
// the socket, so it never had this enqueue-race).
pub const MAX_BUFFER: usize = 16_000_000;

struct Subscriber {
    tx: mpsc::UnboundedSender<Arc<str>>,
    queued: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct Broadcaster {
    subs: Arc<Mutex<Vec<Subscriber>>>,
    max_buffer: usize,
}

impl Broadcaster {
    pub fn new(max_buffer: usize) -> Self {
        Self { subs: Arc::new(Mutex::new(Vec::new())), max_buffer }
    }

    /// Bind the socket and accept subscribers forever. `welcome` supplies
    /// the current book state as event lines for each new connection.
    pub async fn serve(
        &self,
        socket_path: impl AsRef<Path>,
        welcome: impl Fn() -> Vec<String> + Send + Sync + 'static,
    ) -> std::io::Result<()> {
        let path: PathBuf = socket_path.as_ref().to_owned();
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        let welcome = Arc::new(welcome);
        loop {
            let (stream, _) = listener.accept().await?;
            self.add_subscriber(stream, welcome.clone());
        }
    }

    fn add_subscriber(
        &self,
        stream: UnixStream,
        welcome: Arc<impl Fn() -> Vec<String> + Send + Sync + 'static>,
    ) {
        let (tx, mut rx) = mpsc::unbounded_channel::<Arc<str>>();
        let queued = Arc::new(AtomicUsize::new(0));
        for line in welcome() {
            queued.fetch_add(line.len(), Ordering::Relaxed);
            let _ = tx.send(Arc::from(line));
        }
        self.subs.lock().expect("subs lock").push(Subscriber { tx, queued: queued.clone() });
        tokio::spawn(async move {
            let (_read_half, mut write_half) = stream.into_split();
            // subscribers never send; dropping the read half closes on EOF
            while let Some(line) = rx.recv().await {
                queued.fetch_sub(line.len(), Ordering::Relaxed);
                if write_half.write_all(line.as_bytes()).await.is_err() {
                    break;
                }
            }
            let _ = write_half.shutdown().await;
        });
    }

    /// Publish one event line to all subscribers; disconnect any whose
    /// queued bytes exceed the cap (the write path never blocks).
    pub fn publish(&self, line_with_newline: &str) {
        let mut subs = self.subs.lock().expect("subs lock");
        subs.retain(|s| {
            if s.queued.load(Ordering::Relaxed) > self.max_buffer {
                return false; // dropping tx ends the writer task -> disconnect
            }
            s.queued.fetch_add(line_with_newline.len(), Ordering::Relaxed);
            s.tx.send(Arc::from(line_with_newline)).is_ok()
        });
    }

    pub fn subscriber_count(&self) -> usize {
        self.subs.lock().expect("subs lock").len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncBufReadExt;

    #[tokio::test]
    async fn welcome_then_published_lines() {
        let dir = std::env::temp_dir().join(format!("arb-bcast-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("t.sock");
        let b = Broadcaster::new(MAX_BUFFER);
        let b2 = b.clone();
        let s2 = sock.clone();
        tokio::spawn(async move {
            b2.serve(&s2, || vec!["{\"w\":1}\n".to_string()]).await.ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let stream = UnixStream::connect(&sock).await.unwrap();
        let mut reader = tokio::io::BufReader::new(stream);
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"w\":1}\n");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        b.publish("{\"e\":2}\n");
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        assert_eq!(line, "{\"e\":2}\n");
        std::fs::remove_dir_all(dir).ok();
    }

    #[tokio::test]
    async fn slow_subscriber_is_dropped() {
        let b = Broadcaster::new(10); // tiny cap
        let dir = std::env::temp_dir().join(format!("arb-bcast2-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let sock = dir.join("t.sock");
        let b2 = b.clone();
        let s2 = sock.clone();
        tokio::spawn(async move {
            b2.serve(&s2, Vec::new).await.ok();
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let _stream = UnixStream::connect(&sock).await.unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert_eq!(b.subscriber_count(), 1);
        // saturate way past the cap without the subscriber reading fast enough
        for _ in 0..1000 {
            b.publish("0123456789ABCDEF\n");
        }
        assert_eq!(b.subscriber_count(), 0);
        std::fs::remove_dir_all(dir).ok();
    }
}
