/// A single pub/sub message.
///
/// `#[repr(C)]` for ABI stability. Sized to 64 bytes (one cache line)
/// so the Phase 4 ring buffer can index without false sharing.
#[repr(C)]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    /// Unix nanoseconds at publish time (set by the bus, not the caller).
    pub timestamp_ns: u64,
    /// Topic string — fixed-size to keep the struct on one cache line.
    pub topic: [u8; 32],
    /// Payload — fixed-size to keep the struct on one cache line.
    pub payload: [u8; 24],
}

const _SIZE_CHECK: () = assert!(std::mem::size_of::<Message>() == 64);
const _ALIGN_CHECK: () = assert!(std::mem::align_of::<Message>() == 8);

impl Message {
    /// Construct a new `Message`, truncating topic/payload if longer than the fixed fields.
    pub fn new(topic: &str, payload: &[u8]) -> Self {
        let mut t = [0u8; 32];
        let tlen = topic.len().min(32);
        t[..tlen].copy_from_slice(&topic.as_bytes()[..tlen]);

        let mut p = [0u8; 24];
        let plen = payload.len().min(24);
        p[..plen].copy_from_slice(&payload[..plen]);

        Self {
            timestamp_ns: 0,
            topic: t,
            payload: p,
        }
    }

    pub fn topic_str(&self) -> &str {
        let end = self.topic.iter().position(|&b| b == 0).unwrap_or(32);
        std::str::from_utf8(&self.topic[..end]).unwrap_or("")
    }

    pub fn payload_bytes(&self) -> &[u8] {
        let end = self.payload.iter().rposition(|&b| b != 0).map(|i| i + 1).unwrap_or(0);
        &self.payload[..end]
    }

    /// Serialize to a 64-byte array (the struct's raw repr).
    pub fn to_bytes(&self) -> [u8; 64] {
        // SAFETY: Message is #[repr(C)] with no padding on these types.
        unsafe { std::mem::transmute_copy(self) }
    }

    /// Deserialize from a 64-byte array produced by [`Message::to_bytes`].
    pub fn from_bytes(bytes: &[u8; 64]) -> Self {
        // SAFETY: any bit pattern is valid for u8/[u8;N] fields.
        unsafe { std::mem::transmute_copy(bytes) }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn size_is_64_bytes() {
        assert_eq!(std::mem::size_of::<Message>(), 64);
    }

    #[test]
    fn roundtrip_topic_payload() {
        let m = Message::new("orders", b"hello");
        assert_eq!(m.topic_str(), "orders");
        assert_eq!(m.payload_bytes(), b"hello");
    }

    #[test]
    fn topic_truncation() {
        let long = "a".repeat(40);
        let m = Message::new(&long, b"");
        assert_eq!(m.topic_str().len(), 32);
    }
}
