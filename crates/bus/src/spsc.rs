/// SPSC lock-free ring buffer — Phase 4 core.
///
/// Design decisions (see README / HANDOFF.md):
/// - Power-of-two capacity with bitmask indexing (`head & mask`) avoids
///   modulo division on every enqueue/dequeue.
/// - `CachePadded` separates head and tail onto different cache lines so the
///   producer and consumer never false-share a cache line.
/// - `UnsafeCell<MaybeUninit<T>>` slots: no heap allocation per message,
///   no Option<T> overhead, correct initialization tracking.
/// - Acquire/Release ordering on head/tail: the write to a slot
///   happens-before the Release store of head; the consumer sees the slot
///   only after an Acquire load. No SeqCst — SPSC does not need it.
/// - Ownership-enforced SPSC: `Producer` and `Consumer` are distinct types;
///   constructing two producers is a compile error.
use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

// ── Cache padding ────────────────────────────────────────────────────────────

/// Pads `T` to a full 64-byte cache line, preventing false sharing between
/// the producer's head cursor and the consumer's tail cursor.
#[repr(align(64))]
struct CachePadded<T>(T);

impl<T> std::ops::Deref for CachePadded<T> {
    type Target = T;
    #[inline]
    fn deref(&self) -> &T { &self.0 }
}

// ── Wait strategies ──────────────────────────────────────────────────────────

/// What the caller does when the queue is transiently full (producer) or
/// empty (consumer).
///
/// - `BusySpin` burns CPU but achieves the lowest latency; appropriate when
///   a core is dedicated to this thread (see Phase 4 core-pinning results).
/// - `Yield` is friendlier to the OS scheduler; latency tail widens.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WaitStrategy {
    BusySpin,
    Yield,
}

// ── Inner shared state ───────────────────────────────────────────────────────

struct Inner<T> {
    /// Next slot index to write into (owned by Producer).
    head: CachePadded<AtomicUsize>,
    /// Next slot index to read from (owned by Consumer).
    tail: CachePadded<AtomicUsize>,
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
    /// `capacity - 1`; capacity is a power of two.
    mask: usize,
}

// SAFETY: Inner<T> is accessed by at most one Producer and one Consumer
// simultaneously. The Producer exclusively writes `head` and slot[head&mask];
// the Consumer exclusively writes `tail` and reads slot[tail&mask]. The
// atomic loads/stores with Acquire/Release provide the necessary ordering.
unsafe impl<T: Send> Send for Inner<T> {}
unsafe impl<T: Send> Sync for Inner<T> {}

// ── Public handles ───────────────────────────────────────────────────────────

/// The sole sender for an SPSC queue. Not `Clone` — SPSC means one producer.
pub struct Producer<T> {
    inner: Arc<Inner<T>>,
}

/// The sole receiver for an SPSC queue. Not `Clone`.
pub struct Consumer<T> {
    inner: Arc<Inner<T>>,
}

/// Construct an SPSC queue with `capacity` slots (must be a power of two).
/// Returns `(Producer, Consumer)` — the two halves must be sent to different
/// threads.
pub fn spsc<T>(capacity: usize) -> (Producer<T>, Consumer<T>) {
    assert!(
        capacity.is_power_of_two() && capacity > 0,
        "SPSC capacity must be a non-zero power of two, got {capacity}"
    );
    let slots = (0..capacity)
        .map(|_| UnsafeCell::new(MaybeUninit::uninit()))
        .collect::<Vec<_>>()
        .into_boxed_slice();

    let inner = Arc::new(Inner {
        head: CachePadded(AtomicUsize::new(0)),
        tail: CachePadded(AtomicUsize::new(0)),
        slots,
        mask: capacity - 1,
    });
    (Producer { inner: inner.clone() }, Consumer { inner })
}

// ── Producer ─────────────────────────────────────────────────────────────────

impl<T> Producer<T> {
    /// Non-blocking enqueue. Returns `Err(value)` immediately if the queue
    /// is full (Aeron-style backpressure signal).
    #[inline]
    pub fn try_send(&self, value: T) -> Result<(), T> {
        let head = self.inner.head.load(Ordering::Relaxed);
        // Acquire: synchronise with Consumer's Release store of tail.
        let tail = self.inner.tail.load(Ordering::Acquire);

        if head.wrapping_sub(tail) > self.inner.mask {
            return Err(value); // full
        }

        // SAFETY: `head & mask` is only written by this Producer.
        unsafe {
            (*self.inner.slots[head & self.inner.mask].get()).write(value);
        }
        // Release: makes the slot write visible before the head advance.
        self.inner.head.store(head.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Blocking enqueue using the given [`WaitStrategy`].
    #[inline]
    pub fn send(&self, mut value: T, strategy: WaitStrategy) {
        loop {
            match self.try_send(value) {
                Ok(()) => return,
                Err(v) => {
                    value = v;
                    match strategy {
                        WaitStrategy::BusySpin => std::hint::spin_loop(),
                        WaitStrategy::Yield    => std::thread::yield_now(),
                    }
                }
            }
        }
    }

    pub fn capacity(&self) -> usize { self.inner.mask + 1 }
}

// ── Consumer ─────────────────────────────────────────────────────────────────

impl<T> Consumer<T> {
    /// Non-blocking dequeue. Returns `None` immediately if the queue is empty.
    #[inline]
    pub fn try_recv(&self) -> Option<T> {
        let tail = self.inner.tail.load(Ordering::Relaxed);
        // Acquire: synchronise with Producer's Release store of head.
        let head = self.inner.head.load(Ordering::Acquire);

        if tail == head {
            return None; // empty
        }

        // SAFETY: `tail & mask` was written by the Producer and not yet read.
        let value = unsafe {
            (*self.inner.slots[tail & self.inner.mask].get()).assume_init_read()
        };
        // Release: makes the slot free before advertising the tail advance.
        self.inner.tail.store(tail.wrapping_add(1), Ordering::Release);
        Some(value)
    }

    /// Blocking dequeue using the given [`WaitStrategy`].
    #[inline]
    pub fn recv(&self, strategy: WaitStrategy) -> T {
        loop {
            if let Some(v) = self.try_recv() { return v; }
            match strategy {
                WaitStrategy::BusySpin => std::hint::spin_loop(),
                WaitStrategy::Yield    => std::thread::yield_now(),
            }
        }
    }

    pub fn capacity(&self) -> usize { self.inner.mask + 1 }
}

// ── Drop: drain remaining elements ───────────────────────────────────────────

impl<T> Drop for Inner<T> {
    fn drop(&mut self) {
        let head = self.head.load(Ordering::Relaxed);
        let mut tail = self.tail.load(Ordering::Relaxed);
        while tail != head {
            // SAFETY: these slots were written but not yet consumed.
            unsafe { (*self.slots[tail & self.mask].get()).assume_init_drop() };
            tail = tail.wrapping_add(1);
        }
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_send_recv() {
        let (tx, rx) = spsc::<u64>(4);
        assert_eq!(tx.try_send(42), Ok(()));
        assert_eq!(rx.try_recv(), Some(42));
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn full_returns_err() {
        let (tx, rx) = spsc::<u32>(4);
        for i in 0..4 { tx.try_send(i).unwrap(); }
        assert!(tx.try_send(99).is_err());
        // drain one, then it should accept again
        let _ = rx.try_recv();
        assert!(tx.try_send(99).is_ok());
    }

    #[test]
    fn wrap_around() {
        let (tx, rx) = spsc::<u64>(4);
        // Fill, drain, fill again — exercises the bitmask wrap
        for round in 0..4u64 {
            for i in 0..4 { tx.try_send(round * 4 + i).unwrap(); }
            for i in 0..4 { assert_eq!(rx.try_recv(), Some(round * 4 + i)); }
        }
        assert_eq!(rx.try_recv(), None);
    }

    #[test]
    fn capacity_must_be_power_of_two() {
        let result = std::panic::catch_unwind(|| spsc::<u8>(3));
        assert!(result.is_err());
    }

    #[test]
    fn drop_drains_unread_elements() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let counter = Arc::new(AtomicUsize::new(0));

        #[derive(Debug)]
        struct Counted(Arc<AtomicUsize>);
        impl Drop for Counted {
            fn drop(&mut self) { self.0.fetch_add(1, Ordering::Relaxed); }
        }

        let c = counter.clone();
        let (tx, rx) = spsc::<Counted>(4);
        tx.try_send(Counted(c.clone())).unwrap();
        tx.try_send(Counted(c.clone())).unwrap();
        drop(tx);
        drop(rx); // Inner::drop should call assume_init_drop on 2 unread slots
        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn threaded_spsc() {
        use std::thread;
        const N: usize = 100_000;
        let (tx, rx) = spsc::<u64>(1024);
        let producer = thread::spawn(move || {
            for i in 0..N as u64 {
                tx.send(i, WaitStrategy::Yield);
            }
        });
        let mut sum = 0u64;
        for _ in 0..N {
            sum += rx.recv(WaitStrategy::Yield);
        }
        producer.join().unwrap();
        assert_eq!(sum, (0..N as u64).sum::<u64>());
    }
}
