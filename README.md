# Cadence

> A lock-free, low-latency in-process pub/sub message bus written in Rust.

Cadence is a portfolio-grade systems project that demonstrates the engineering behind high-performance messaging — the same class of problem solved in production by [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/), [Aeron](https://github.com/real-logic/aeron), and [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue). The goal is to build it from scratch, benchmark it honestly, and document every design decision and tradeoff.

---

## Project Status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Foundation — workspace, `Message` struct, crossbeam-channel pub/sub | ✅ Complete |
| 2 | Correctness — wildcard routing, bounded queues, backpressure, serialization | ✅ Complete |
| 3 | Metrics — nanosecond timestamps, HDR latency histograms, JSON export | 🔜 Next |
| 4 | Optimization — hand-written SPSC ring buffer, core pinning, honest benchmarks | 🔜 Planned |
| 5 | Polish — diagrams, reproducible benchmark artifacts, dual-audience README | 🔜 Planned |

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                   Cadence Workspace                  │
│                                                      │
│  ┌──────────────┐   ┌────────────────────────────┐  │
│  │ message-core │   │           bus              │  │
│  │              │   │                            │  │
│  │  Message     │──▶│  Bus::subscribe(pattern)   │  │
│  │  [repr(C)]   │   │  Bus::offer(msg)           │  │
│  │  64 bytes    │   │  Bus::unsubscribe(id)      │  │
│  │  1 cache line│   │  Bus::drop_count()         │  │
│  └──────────────┘   └────────────────────────────┘  │
│                              │                       │
│              ┌───────────────┴──────────────┐        │
│              ▼                              ▼        │
│  ┌───────────────────┐       ┌───────────────────┐  │
│  │        cli        │       │       bench       │  │
│  │  cadence binary   │       │  throughput smoke │  │
│  │  smoke demo       │       │  (Phase 3: HDR)   │  │
│  └───────────────────┘       └───────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Message struct — 64 bytes, one cache line

```rust
#[repr(C)]
pub struct Message {
    pub timestamp_ns: u64,      //  8 bytes — set by bus at publish time
    pub topic:        [u8; 32], // 32 bytes — fixed, null-padded
    pub payload:      [u8; 24], // 24 bytes — fixed, null-padded
}
// Total: 64 bytes == 1 cache line. Verified at compile time.
```

Fixing the size now means Phase 4's ring buffer can index without false sharing. The struct is `#[repr(C)]` for ABI stability and safe byte-level serialization.

### Topic routing

Subscriptions support exact match or trailing-wildcard prefix:

```
"prices"     — matches only "prices"
"prices.*"   — matches "prices.USD", "prices.EUR", …
```

Implemented as a simple enum dispatch (`Filter::Exact` / `Filter::Prefix`). A trie/Patricia-trie (à la ZeroMQ/nanomsg) is documented as a stretch goal for future work.

### Backpressure — Aeron-style `offer()`

```rust
let result: OfferResult = bus.offer(msg);
// result.sent    — subscribers that accepted
// result.dropped — subscribers whose queue was full
```

Each subscriber queue is bounded (`capacity` set at bus construction). Full queues are skipped non-blocking; the producer observes the signal and decides what to do. This matches Aeron's `Publication::offer()` model. A global atomic drop counter is also exposed via `bus.drop_count()`.

---

## Getting Started

### Prerequisites

- [Rust stable](https://rustup.rs) (1.70+)

```bash
# macOS / Linux
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Windows — download and run rustup-init.exe from https://rustup.rs
```

### Clone & build

```bash
git clone https://github.com/loganlewisw1112-create/Cadence-.git
cd Cadence-
cargo build --all
```

### Run tests

```bash
cargo test --all
```

Expected output:

```
running 11 tests   # bus crate
test tests::backpressure_drops_when_full ... ok
test tests::exact_and_wildcard_coexist ... ok
test tests::message_serialization_roundtrip ... ok
test tests::multi_subscriber_same_topic ... ok
test tests::single_subscriber_receives ... ok
test tests::slow_subscriber_does_not_block_fast_subscriber ... ok
test tests::timestamp_is_set ... ok
test tests::topic_isolation_exact ... ok
test tests::unsubscribe_stops_delivery ... ok
test tests::wildcard_does_not_match_unrelated ... ok
test tests::wildcard_prefix_matches ... ok

running 3 tests   # message-core crate
test tests::roundtrip_topic_payload ... ok
test tests::size_is_64_bytes ... ok
test tests::topic_truncation ... ok

test result: ok. 14 passed; 0 failed
```

### Run the CLI smoke demo

```bash
cargo run -p cli
# [1719530412000000000ns] demo => "hello from cadence"
```

### Run the Phase 1 throughput baseline

```bash
cargo run -p bench --release
# published 1000000 msgs in ~300ms (~3 Mmsg/s); received 1000000
```

> **Note:** Phase 3 will replace this with an HDRHistogram + `quanta` latency benchmark. The above number is a rough throughput figure only — it includes allocation and lock overhead and is not a latency claim.

---

## Crate layout

```
cadence/
├── Cargo.toml                  # workspace root
├── rust-toolchain.toml         # pins stable channel
├── crates/
│   ├── message-core/           # Message struct, serialization
│   ├── bus/                    # Bus, Filter, OfferResult
│   ├── cli/                    # cadence binary (smoke demo)
│   └── bench/                  # throughput / latency benchmarks
└── .github/workflows/ci.yml    # test + Miri CI
```

---

## CI

GitHub Actions runs on every push:

- `cargo test --all` on stable
- `cargo clippy --all -- -D warnings`
- `cargo miri test -p message-core` on nightly (catches UB in `unsafe` serialization)

---

## Design decisions & tradeoffs

### Why fixed-size fields instead of `Vec<u8>`?

A heap-allocated payload would require a pointer indirection on every receive and would break cache-line alignment. Phase 4's ring buffer pre-allocates a contiguous array of `Message` slots — dynamic payloads would require a sidecar allocator (à la Chronicle Queue's `MappedBytes`), adding complexity without benefit for the intra-process use case.

### Why `crossbeam-channel` in Phase 1–2?

It is the de-facto standard for bounded MPSC in Rust, battle-tested, and provides a clean correctness baseline. Phase 4 replaces the hot path with a hand-written SPSC ring buffer and benchmarks both — so the crossbeam baseline is a feature, not a placeholder.

### Why Aeron-style `offer()` instead of blocking `send()`?

Blocking producers in a low-latency bus is unacceptable: one slow subscriber stalls all others. `offer()` returns a backpressure signal immediately; the producer decides whether to retry, drop, or route to a dead-letter queue. This mirrors Aeron's `Publication::offer()` which returns a position or a negative status code.

### Why not io_uring / AF_XDP / DPDK?

- **io_uring** is planned as an optional Phase 4b for a persistence/ingest path comparison against epoll. It is not the primary optimization target.
- **AF_XDP / DPDK** require a supported NIC with zero-copy drivers and a second machine for meaningful benchmarks. Running them over `veth` produces non-indicative numbers (see [xsk-rs docs](https://github.com/DouglasGray/xsk-rs)). They are documented as future work only.

---

## Benchmarking philosophy

Latency benchmarks follow the methodology in `BENCHMARKING.md`:

- **`quanta`** for TSC-based nanosecond timestamps on the hot path
- **`hdrhistogram`** for latency distribution (p50 / p95 / p99 / p99.9 / max)
- **Coordinated omission** handled explicitly — open-loop constant-arrival-rate generator
- **Full hardware disclosure** — CPU model, cache topology (`hwloc`), OS, Rust version
- **Honest reporting** — results documented even where the ring buffer does not outperform crossbeam at a given burst size or percentile

---

## Limitations

- **Single machine, single NUMA node.** Cross-NUMA latency effects are cited as a hypothesis but not measured — this hardware cannot demonstrate them credibly.
- **No kernel bypass.** All messaging is in-process. Network-layer zero-copy (AF_XDP, DPDK) is out of scope.
- **Fixed payload size (24 bytes).** Large messages require a sidecar strategy not yet implemented.
- **No persistence.** Messages are in-memory only. A persistence path is a Phase 4b stretch goal.

---

## Future work

- MPSC ring buffer (Phase 4 stretch)
- Persistent log (io_uring vs. epoll, Phase 4b)
- Shared-memory IPC (Iceoryx-style) for cross-process messaging
- AF_XDP / DPDK for kernel-bypass networking (requires appropriate hardware)
- Cross-NUMA latency benchmarks (requires multi-socket hardware)

---

## How AI tooling was used

This project was built with [Claude Code](https://claude.ai/code) (Anthropic) as the primary coding agent, interchangeable with Cursor Pro and Codex Pro. The AI agent:

- Implemented code from a spec written in `TASKS.md` and `PROJECT_PLAN.md`
- Was constrained to build one phase at a time with no forward speculation
- Did not choose the architecture — scope decisions (ring buffer over io_uring, Aeron-style backpressure, benchmarking methodology) were made by the project owner and locked in `HANDOFF.md` before any code was written

The local repo is the source of truth. `HANDOFF.md` persists context between agent sessions so any tool can pick up exactly where the last one stopped.

---

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) — cache-line padded ring buffer, wait strategies
- [Aeron](https://github.com/real-logic/aeron) — backpressure model, log-structured IPC
- [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) — off-heap persistence, HDR latency methodology
- [ZeroMQ / NNG](https://nanomsg.org/) — topic routing patterns
- [Iceoryx](https://github.com/eclipse-iceoryx/iceoryx) — zero-copy shared-memory IPC
- [xsk-rs](https://github.com/DouglasGray/xsk-rs) — AF_XDP Rust bindings (future work reference)
- [hdrhistogram](https://crates.io/crates/hdrhistogram) — coordinated-omission-aware latency histograms
- [quanta](https://crates.io/crates/quanta) — TSC-based high-resolution clock

---

*Built on Rust stable. Tested on Windows 11. CI runs on Ubuntu (GitHub Actions).*
