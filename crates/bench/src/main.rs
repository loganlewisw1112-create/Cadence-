/// Phase 1 baseline: throughput smoke test (not a latency benchmark).
/// Phase 3 will replace this with hdrhistogram + quanta.
use bus::Bus;
use message_core::Message;
use std::time::Instant;

const N: usize = 1_000_000;

fn main() {
    let b = Bus::new(N);
    let (_id, rx) = b.subscribe("bench");

    let t0 = Instant::now();
    for i in 0..N {
        let payload = (i as u64).to_le_bytes();
        b.publish(Message::new("bench", &payload));
    }
    let pub_ns = t0.elapsed().as_nanos();

    let mut count = 0usize;
    while rx.try_recv().is_ok() {
        count += 1;
    }

    println!(
        "published {N} msgs in {pub_ns}ns ({:.1} Mmsg/s); received {count}",
        N as f64 / (pub_ns as f64 / 1e9) / 1e6
    );
}
