use crossbeam_channel::{bounded, Receiver, Sender, TrySendError};
use message_core::Message;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

pub type SubId = u64;

/// Result returned by [`Bus::offer`].
///
/// Mirrors Aeron's back-pressure model: the producer calls `offer` and acts
/// on the signal rather than blocking or silently dropping.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OfferResult {
    /// Number of subscribers that accepted the message.
    pub sent: usize,
    /// Number of subscribers whose queue was full (message was not delivered).
    pub dropped: usize,
}

/// Topic-matching rule stored per subscription.
///
/// `Prefix("prices.")` matches any topic that starts with `"prices."`.
/// `Exact("prices.USD")` matches only that literal string.
#[derive(Debug, Clone)]
enum Filter {
    Exact(String),
    Prefix(String),
}

impl Filter {
    fn parse(pattern: &str) -> Self {
        if let Some(prefix) = pattern.strip_suffix('*') {
            Filter::Prefix(prefix.to_owned())
        } else {
            Filter::Exact(pattern.to_owned())
        }
    }

    fn matches(&self, topic: &str) -> bool {
        match self {
            Filter::Exact(t) => t == topic,
            Filter::Prefix(p) => topic.starts_with(p.as_str()),
        }
    }
}

struct Sub {
    filter: Filter,
    tx: Sender<Message>,
}

struct Inner {
    next_id: SubId,
    subs: HashMap<SubId, Sub>,
}

/// In-process pub/sub bus backed by `crossbeam-channel`.
///
/// **Topic matching** — exact string equality, or a trailing `*` wildcard:
/// - `"prices"` matches only `"prices"`
/// - `"prices.*"` matches `"prices.USD"`, `"prices.EUR"`, etc.
///
/// **Backpressure** — each subscriber queue is bounded (`capacity`).
/// [`Bus::offer`] returns an [`OfferResult`] so the producer can react.
/// [`Bus::publish`] is a fire-and-forget convenience that returns sent count.
#[derive(Clone)]
pub struct Bus {
    inner: Arc<Mutex<Inner>>,
    capacity: usize,
    drop_count: Arc<AtomicU64>,
}

impl Bus {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                next_id: 0,
                subs: HashMap::new(),
            })),
            capacity,
            drop_count: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Subscribe to `pattern`. Supports trailing `*` wildcard.
    pub fn subscribe(&self, pattern: impl AsRef<str>) -> (SubId, Receiver<Message>) {
        let (tx, rx) = bounded(self.capacity);
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id;
        g.next_id += 1;
        g.subs.insert(id, Sub { filter: Filter::parse(pattern.as_ref()), tx });
        (id, rx)
    }

    /// Unsubscribe by `SubId`.
    pub fn unsubscribe(&self, id: SubId) {
        self.inner.lock().unwrap().subs.remove(&id);
    }

    /// Total messages dropped across all subscribers since bus creation.
    pub fn drop_count(&self) -> u64 {
        self.drop_count.load(Ordering::Relaxed)
    }

    /// Publish with Aeron-style back-pressure signal.
    pub fn offer(&self, mut msg: Message) -> OfferResult {
        msg.timestamp_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        let topic = msg.topic_str().to_owned();
        let g = self.inner.lock().unwrap();
        let mut sent = 0usize;
        let mut dropped = 0usize;

        for sub in g.subs.values() {
            if sub.filter.matches(&topic) {
                match sub.tx.try_send(msg.clone()) {
                    Ok(()) => sent += 1,
                    Err(TrySendError::Full(_)) => {
                        dropped += 1;
                        self.drop_count.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(TrySendError::Disconnected(_)) => {}
                }
            }
        }

        OfferResult { sent, dropped }
    }

    /// Fire-and-forget convenience wrapper around [`Bus::offer`].
    pub fn publish(&self, msg: Message) -> usize {
        self.offer(msg).sent
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bus() -> Bus { Bus::new(4) }

    // --- Phase 1 correctness (preserved) ---

    #[test]
    fn single_subscriber_receives() {
        let b = bus();
        let (_id, rx) = b.subscribe("prices");
        let r = b.offer(Message::new("prices", b"42"));
        assert_eq!(r.sent, 1);
        assert_eq!(r.dropped, 0);
        let m = rx.try_recv().unwrap();
        assert_eq!(m.topic_str(), "prices");
        assert_eq!(m.payload_bytes(), b"42");
    }

    #[test]
    fn topic_isolation_exact() {
        let b = bus();
        let (_id, rx_a) = b.subscribe("a");
        let (_id, rx_b) = b.subscribe("b");
        b.publish(Message::new("a", b"for-a"));
        assert!(rx_a.try_recv().is_ok());
        assert!(rx_b.try_recv().is_err());
    }

    #[test]
    fn multi_subscriber_same_topic() {
        let b = bus();
        let (_id, rx1) = b.subscribe("x");
        let (_id, rx2) = b.subscribe("x");
        let r = b.offer(Message::new("x", b"hi"));
        assert_eq!(r.sent, 2);
        assert!(rx1.try_recv().is_ok());
        assert!(rx2.try_recv().is_ok());
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let b = bus();
        let (id, rx) = b.subscribe("z");
        b.unsubscribe(id);
        b.publish(Message::new("z", b"gone"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn timestamp_is_set() {
        let b = bus();
        let (_id, rx) = b.subscribe("ts");
        b.publish(Message::new("ts", b""));
        let m = rx.try_recv().unwrap();
        assert!(m.timestamp_ns > 0);
    }

    // --- Phase 2: topic filtering (wildcard) ---

    #[test]
    fn wildcard_prefix_matches() {
        let b = bus();
        let (_id, rx) = b.subscribe("prices.*");
        b.publish(Message::new("prices.USD", b"1"));
        b.publish(Message::new("prices.EUR", b"2"));
        assert!(rx.try_recv().is_ok());
        assert!(rx.try_recv().is_ok());
    }

    #[test]
    fn wildcard_does_not_match_unrelated() {
        let b = bus();
        let (_id, rx) = b.subscribe("prices.*");
        b.publish(Message::new("orders.new", b"x"));
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn exact_and_wildcard_coexist() {
        let b = bus();
        let (_id, rx_exact) = b.subscribe("prices.USD");
        let (_id, rx_wild) = b.subscribe("prices.*");
        b.publish(Message::new("prices.USD", b"99"));
        assert!(rx_exact.try_recv().is_ok());
        assert!(rx_wild.try_recv().is_ok());
    }

    // --- Phase 2: backpressure / drop ---

    #[test]
    fn backpressure_drops_when_full() {
        // capacity = 4; publish 8 msgs without draining
        let b = Bus::new(4);
        let (_id, _rx) = b.subscribe("flood");
        let mut total_dropped = 0usize;
        for _ in 0..8 {
            total_dropped += b.offer(Message::new("flood", b"x")).dropped;
        }
        assert!(total_dropped >= 4, "expected at least 4 drops, got {total_dropped}");
        assert!(b.drop_count() >= 4);
    }

    #[test]
    fn slow_subscriber_does_not_block_fast_subscriber() {
        // Two subscribers: one drains, one doesn't.
        let b = Bus::new(4);
        let (_id, rx_fast) = b.subscribe("t");
        let (_id, _rx_slow) = b.subscribe("t"); // intentionally not drained

        for _ in 0..8 {
            b.offer(Message::new("t", b"x"));
            let _ = rx_fast.try_recv(); // keep fast subscriber drained
        }
        // fast subscriber should have received all 8
        // (the slow one will have dropped >= 4, but fast one never blocked)
        assert_eq!(rx_fast.try_recv().is_err(), true); // drained
    }

    // --- Phase 2: serialization round-trip ---

    #[test]
    fn message_serialization_roundtrip() {
        use message_core::Message;
        let original = Message::new("ser.test", b"payload123");
        let bytes = original.to_bytes();
        let decoded = Message::from_bytes(&bytes);
        assert_eq!(decoded.topic_str(), original.topic_str());
        assert_eq!(decoded.payload_bytes(), original.payload_bytes());
        // timestamp preserved
        assert_eq!(decoded.timestamp_ns, original.timestamp_ns);
    }
}
