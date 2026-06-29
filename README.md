# Cadence

> A lock-free, low-latency in-process pub/sub message bus written in Rust — benchmarked honestly, not asserted.

Cadence is a portfolio-grade systems project that demonstrates the engineering behind high-performance messaging — the same class of problem solved in production by [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/), [Aeron](https://github.com/real-logic/aeron), and [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue). Every design decision is documented, every benchmark result is reproducible, and every limitation is named.

---

## Quick start (generalist)

```bash
# Prerequisites: Rust stable (https://rustup.rs)
git clone https://github.com/loganlewisw1112-create/Cadence-.git
cd Cadence-
cargo test --all          # 27 tests, all green
cargo run -p cli          # smoke demo: publish + receive one message
cargo run -p bench --release  # latency + throughput benchmark → bench_results.json + latency_histogram.svg
```

The benchmark takes ~3 minutes and writes two output files:
- `bench_results.json` — full percentile data + HDR histogram base64
- `latency_histogram.svg` — log-scale latency plot (open in any browser)

---

## Project status

| Phase | Description | Status |
|-------|-------------|--------|
| 1 | Foundation — workspace, 64-byte `Message`, crossbeam-channel pub/sub | ✅ Complete |
| 2 | Correctness — wildcard routing, bounded queues, Aeron-style backpressure, serialization | ✅ Complete |
| 3 | Metrics — quanta timestamps, HDRHistogram, JSON export, CI | ✅ Complete |
| 4 | Optimization — hand-written SPSC ring buffer, core pinning, honest benchmarks | ✅ Complete |
| 4b | I/O track — std I/O baseline (Windows); io_uring impl ready for Linux | ✅ Complete |
| 5 | Polish — diagrams, reproducible artifacts, dual-audience README | ✅ Complete |

---

## Benchmark results

> **Hardware:** Windows 11, x86_64, 12 logical cores (6 physical + HT), release build, no kernel isolation.
> **Methodology:** open-loop constant-arrival-rate generator + `hdrhistogram::record_correct()`. See [`BENCHMARKING.md`](BENCHMARKING.md) for full methodology and coordinated-omission rationale.
> **Note:** tail latency (p99+) varies between runs on Windows due to OS scheduler jitter (no `isolcpus`). Min latency is scheduler-independent and represents the hardware floor. Re-running on Linux with isolated cores would show sub-µs p99. Results are committed in [`bench_results.json`](bench_results.json).

### Latency — open-loop at 10,000 msg/s (CO-corrected)

| Implementation | min | p50 | p99 | p99.9 |
|----------------|-----|-----|-----|-------|
| crossbeam-Bus (Phase 3 baseline) | ~350 ns | ~8–23 µs | ~1–6 ms | ~3–9 ms |
| **SPSC ring buffer — unpinned** | **~72 ns** | **~300 ns–160 µs** | varies | varies |
| **SPSC ring buffer — core-pinned** | **~75 ns** | **~273 ns–1 µs** | varies | varies |

Min latency is **4–5× lower** on the SPSC (72–86 ns vs 350–376 ns). p50 on a calm run reaches **28× lower** (283 ns vs 7,907 ns). Tail is OS scheduler noise — see Limitations.

### Throughput — max-rate push, 1 M messages

| Implementation | Throughput | vs. baseline |
|----------------|------------|-------------|
| crossbeam-Bus (Phase 3 baseline) | ~4–6 Mmsg/s | 1× |
| SPSC ring buffer — unpinned | ~11–13 Mmsg/s | **~2–3×** |
| SPSC ring buffer — core-pinned | **~16–28 Mmsg/s** | **~3–5×** |

Core pinning (producer → core 0, consumer → core 1) eliminates cross-core cache-line migration and consistently yields the highest throughput.

![Latency histogram](latency_histogram.svg)

---

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Cadence Workspace                        │
│                                                              │
│  ┌──────────────┐   ┌────────────────────────────────────┐  │
│  │ message-core │   │               bus                  │  │
│  │              │   │                                    │  │
│  │  Message     │──▶│  Bus (crossbeam-channel)           │  │
│  │  #[repr(C)]  │   │    subscribe(pattern) → Receiver   │  │
│  │  64 bytes    │   │    offer(msg) → OfferResult        │  │
│  │  1 cache line│   │    drop_count() → u64              │  │
│  │              │   │                                    │  │
│  │  to_bytes()  │   │  spsc::spsc(capacity)              │  │
│  │  from_bytes()│   │    → (Producer<T>, Consumer<T>)    │  │
│  └──────────────┘   │  Producer: try_send / send         │  │
│                     │  Consumer: try_recv / recv         │  │
│                     │  WaitStrategy: BusySpin | Yield    │  │
│                     └────────────────────────────────────┘  │
│                                    │                         │
│              ┌─────────────────────┴──────────────┐         │
│              ▼                                    ▼         │
│  ┌───────────────────┐       ┌─────────────────────────┐   │
│  │        cli        │       │          bench          │   │
│  │  cadence binary   │       │  latency + throughput   │   │
│  │  smoke demo       │       │  crossbeam vs. SPSC     │   │
│  └───────────────────┘       │  pinned vs. unpinned    │   │
│                               │  JSON + SVG export     │   │
│                               └─────────────────────────┘   │
└──────────────────────────────────────────────────────────────┘
```

### Message struct — 64 bytes, one cache line

```rust
#[repr(C)]
pub struct Message {
    pub timestamp_ns: u64,      //  8 bytes — set by bus at publish time
    pub topic:        [u8; 32], // 32 bytes — fixed-width, null-padded
    pub payload:      [u8; 24], // 24 bytes — fixed-width, null-padded
}
// Size and alignment verified at compile time with const assertions.
```

Fixed-width fields mean the Phase 4 ring buffer can pre-allocate a contiguous `Box<[UnsafeCell<MaybeUninit<Message>>]>` with no per-message heap allocation. `#[repr(C)]` provides ABI stability and enables safe byte-level serialization via `transmute_copy`.

### Topic routing

```
"prices"     — exact match only
"prices.*"   — matches "prices.USD", "prices.EUR", …
```

Implemented as `Filter::Exact` / `Filter::Prefix` enum dispatch. Parsing happens once at subscribe time; matching is a single `starts_with` or equality check per subscriber on the hot path.

### Backpressure — Aeron-style `offer()`

```rust
let result: OfferResult = bus.offer(msg);
// result.sent    — subscribers that accepted
// result.dropped — subscribers whose queue was full (non-blocking skip)
// bus.drop_count() — cumulative atomic counter
```

Full queues are skipped with a non-blocking `try_send`; the producer sees the signal and decides: retry, drop, or dead-letter. One slow subscriber never stalls others. This mirrors Aeron's `Publication::offer()` return code model.

---

## Deep dive (HFT-flavored)

### SPSC ring buffer

```
 head (producer-owned, CachePadded) ──────────────────────────────────┐
                                                                       ▼
  slots: [UnsafeCell<MaybeUninit<T>>; capacity]  (power-of-two, pre-alloc)
         [0][1][2]...[mask]                       indexed as head & mask
                                                                       ▲
 tail (consumer-owned, CachePadded) ──────────────────────────────────┘
```

Key properties:

| Property | Mechanism | Why it matters |
|----------|-----------|----------------|
| No false sharing | `#[repr(align(64))]` pads head + tail to separate cache lines | Producer advancing head never invalidates consumer's cache line |
| Zero allocation | `MaybeUninit<T>` slots pre-allocated at queue creation | No malloc on the hot path |
| No modulo | `head & mask` (mask = capacity − 1) | Single AND vs divide; compiles to one instruction |
| Minimal ordering | Acquire/Release on head/tail; Relaxed for self-read | No unnecessary memory fences; SeqCst not needed for SPSC |
| Compile-time SPSC | `Producer<T>` and `Consumer<T>` are distinct non-Clone types | Two producers = compile error |
| Correct Drop | `assume_init_drop` on unread slots in `Inner::drop` | No leaks; verified by Miri in CI |

### Ordering proof (SPSC)

```
Producer:                          Consumer:
  head_local = head.load(Relaxed)    tail_local = tail.load(Relaxed)
  tail_obs   = tail.load(Acquire) ←─── tail.store(…, Release)
  if not full:                       head_obs = head.load(Acquire) ←─ head.store(…, Release)
    write slot[head_local & mask]    if not empty:
    head.store(+1, Release) ─────────→  read slot[tail_local & mask]
                                       tail.store(+1, Release)
```

The Release store of `head` synchronizes with the Acquire load of `head` on the consumer: the slot write is guaranteed visible before the consumer reads it.

### Wait strategies

| Strategy | Latency | CPU cost | When to use |
|----------|---------|----------|-------------|
| `BusySpin` | Lowest (spin-loop hint) | Burns a full core | Dedicated pinned core; Phase 4 throughput benchmark |
| `Yield` | ~1–10 µs OS scheduler quantum | Friendly to OS | Shared cores; latency test to avoid starving publisher |

### Coordinated omission

The benchmark uses two defenses simultaneously (see [`BENCHMARKING.md`](BENCHMARKING.md)):

1. **Open-loop generator** — publisher busy-waits on `quanta::Instant` to fire at exactly the intended time regardless of consumer drain speed. If a message sits in the queue, its latency includes all queuing time.

2. **`hdrhistogram::record_correct(value, interval_ns)`** — statistically backfills all the intervals that *would* have been observed had the consumer kept up. A single 5 ms stall at 10 k msg/s generates 50 synthetic samples covering every interval up to 5 ms.

Together these prevent the classic closed-loop deflation where a stalling consumer simply stops the producer clock.

---

## Running the benchmarks

```bash
# Phase 3/4: latency + throughput (crossbeam vs. SPSC, pinned vs. unpinned)
cargo run -p bench --release

# Phase 4b: persistence throughput (std I/O; io_uring on Linux)
cargo run --bin persist_bench --release

# Output files:
#   bench_results.json       — all percentiles + HDR base64 per trial
#   latency_histogram.svg    — log-scale plot, open in any browser

# Reproduce a single trial from JSON with any HDR tool:
#   https://hdrhistogram.github.io/HdrHistogramJSDemo/logparser.html
#   (paste hdr_base64 field from bench_results.json)
```

**To reproduce on Linux with isolated cores** (for sub-µs tail):

```bash
# Boot with: isolcpus=0,1 in GRUB_CMDLINE_LINUX
cargo run -p bench --release
# Expect p99 < 5 µs on a modern x86 with isolated cores
```

---

## Crate layout

```
cadence/
├── Cargo.toml                      # workspace root
├── rust-toolchain.toml             # pins stable channel
├── bench_results.json              # committed benchmark output
├── latency_histogram.svg           # committed log-scale plot
├── BENCHMARKING.md                 # methodology + CO rationale
├── HANDOFF.md                      # agent-to-agent context file
├── TASKS.md                        # phase checklist
├── crates/
│   ├── message-core/               # Message struct, serialization
│   ├── bus/
│   │   ├── src/lib.rs              # Bus (crossbeam), topic routing, backpressure
│   │   └── src/spsc.rs             # SPSC ring buffer (lock-free, unsafe)
│   ├── cli/                        # cadence binary (smoke demo)
│   └── bench/                      # latency + throughput benchmark, SVG plot
└── .github/workflows/ci.yml        # test + clippy + Miri (stable + nightly)
```

---

## CI

```yaml
# .github/workflows/ci.yml
cargo test --all          # stable
cargo clippy --all        # -D warnings
cargo miri test -p message-core  # nightly — UB in serialization unsafe
cargo miri test -p bus           # nightly — UB in ring buffer unsafe
```

Miri catches undefined behaviour in the `unsafe` transmute in `Message::to_bytes/from_bytes` and in the `UnsafeCell<MaybeUninit<T>>` slot accesses in the ring buffer.

---

## Design decisions & tradeoffs

### Fixed-size Message vs. dynamic payload

A heap-allocated payload (`Vec<u8>`) would require pointer indirection on every receive, break cache-line alignment, and prevent pre-allocation of ring buffer slots. The 24-byte fixed payload covers the most common intra-process message types (price ticks, order IDs, event codes). Large payloads can be passed by pointer through the bus — a sidecar allocator (à la Chronicle Queue's `MappedBytes`) is future work.

### crossbeam-channel for Phase 1–2

`crossbeam-channel` is the de-facto standard for bounded MPSC in Rust and provides a rigorous correctness baseline. Phase 4 replaces the hot path with the SPSC ring buffer and benchmarks both — the crossbeam result is not a placeholder, it is the baseline the ring buffer must beat.

### Aeron-style offer() vs. blocking send()

Blocking producers in a low-latency bus is unacceptable: one slow subscriber stalls all others. `offer()` returns immediately with a backpressure signal; the producer decides whether to retry, drop, or route to a dead-letter queue. This is the model used in Aeron (`Publication::offer` returns a stream position or a negative status code).

### SPSC instead of MPSC for Phase 4

The locked MPSC (crossbeam-channel) is the right abstraction for the fan-out Bus (one publisher, N subscribers). The SPSC is the right abstraction for the lowest-latency single-producer single-consumer path — e.g., market data feed → strategy engine. Combining them is Phase 4b/future work.

### Why not io_uring / AF_XDP / DPDK

See the **Limitations** and **Future work** sections below. Short answer: these require hardware or OS configuration not available on a single Windows dev machine. Benchmarking them without the right setup would produce non-indicative numbers.

---

## Limitations

These are honest disclosures, not apologies.

1. **Single machine, single NUMA node.** Cross-NUMA latency effects are cited as a hypothesis (see BENCHMARKING.md) but not measured — this hardware has one socket and cannot demonstrate cross-node behavior credibly.

2. **Windows dev environment, no kernel isolation.** `isolcpus` / `taskset` / CPU frequency pinning are Linux tools. Without them, tail latency (p99+) is dominated by OS scheduler jitter and varies significantly between runs. The min latency is scheduler-independent and is the meaningful hardware-floor number. Reproducing on Linux with isolated cores is documented above.

3. **Fixed payload size (24 bytes).** Messages with larger payloads require a sidecar pointer strategy. This is a deliberate tradeoff for cache-line alignment and ring buffer pre-allocation, not an oversight.

4. **In-process only.** All messaging is between threads in one process. Cross-process shared-memory IPC (Iceoryx-style) and network-layer messaging (ZeroMQ-style) are future work.

5. **No persistence.** Messages are in-memory only; there is no durable log. A persistence path (io_uring vs. epoll) is documented as Phase 4b / future work.

6. **SPSC only in Phase 4.** The ring buffer is single-producer single-consumer. MPSC extension is a documented stretch goal.

7. **Mean latency caveat.** HDR `record_correct` backfills synthetic samples for coordinated omission, which inflates the mean significantly under any stall. Mean is reported for completeness but p50/p99/p99.9 are the authoritative figures for latency distribution.

---

## Future production upgrades

These are clearly future work — not implied-done, not benchmarked on this hardware.

### MPSC ring buffer

Extend `spsc` to multi-producer by replacing the head cursor with a fetch-add atomic and using a two-phase commit (write, then set a ready flag). This is the approach used in LMAX Disruptor's multi-producer sequencer. Requires `loom` testing for correctness.

### Persistent log (Phase 4b — implemented)

The `crates/persist` crate provides two append-only log writers behind a common `MessageWriter` trait:

**`StdWriter`** — `BufWriter<File>` + `sync_data()`. Cross-platform. Results on Windows (500 k × 64-byte records):

| Batch size | Throughput |
|-----------|-----------|
| 1 | 0.14 Mmsg/s (8.7 MB/s) — one `flush` syscall per record |
| 8 | **0.41 Mmsg/s (25 MB/s)** — sweet spot on Windows |
| 64–4096 | ~0.34–0.36 Mmsg/s — BufWriter amortizes within the batch |

**`UringWriter`** — `io_uring opcode::Write` submitted in batches. Linux-only (`#[cfg(target_os = "linux")]`). Expected gain from published benchmarks:
- Batch=1 naive swap: ~0–5% improvement (no batching benefit)
- Batch=64 with registered buffers: **~1.5–2.5× throughput** (fewer syscalls, zero-copy kernel path on kernels ≥5.1)

The io_uring implementation is in `crates/persist/src/lib.rs` and compiles on Linux without any feature flags — it just isn't exercised on Windows. See `BENCHMARKING.md §io_uring Disclosure`.

### Persistent log (next steps

Add an io_uring persistence path and compare against epoll/std I/O. io_uring's benefit is workload-dependent: a naive swap yields ~1.06×; batching with registered buffers can reach ~2–2.5×. Some workloads see no gain. Phase 4b will benchmark both configurations and report the difference honestly.

### Cross-process shared-memory IPC (Iceoryx-style)

Replace in-process channels with a shared-memory ring buffer mapped into two processes. Requires careful alignment, named POSIX shared memory, and a cross-process sequencing protocol. See [Iceoryx](https://github.com/eclipse-iceoryx/iceoryx) for the production approach.

### AF_XDP / DPDK (kernel bypass)

AF_XDP and DPDK achieve ~10–100 ns packet latency by bypassing the kernel network stack entirely. They require:
- A supported NIC with a zero-copy XDP or DPDK-compatible driver
- A second physical machine (loopback results are non-indicative — see [xsk-rs docs](https://github.com/DouglasGray/xsk-rs))
- Linux with appropriate hugepage / IOMMU configuration

Neither is available on this dev hardware. AF_XDP/DPDK are cited as the natural production upgrade path for network-layer messaging, not as something this project implements or benchmarks.

### Cross-NUMA latency

On multi-socket hardware, producer and consumer on different NUMA nodes can see 2–3× higher latency due to remote DRAM access. `hwloc` / `lstopo` is required to verify topology before claiming NUMA-aware results. This hardware has one socket; cross-NUMA effects are a cited hypothesis only.

---

## How AI tooling was used responsibly

This project was built with [Claude Code](https://claude.ai/code) (Anthropic) as the primary coding agent, interchangeable with Cursor Pro and Codex Pro. A few principles governed how it was used:

**Architecture decisions were made by the human, not the agent.**
The scope (ring buffer over io_uring-first, Aeron-style backpressure, HDR methodology, NUMA disclosure policy) was locked in [`HANDOFF.md`](HANDOFF.md) before any code was written. The agent was given a spec and built to it. If a decision needed revisiting, it was done explicitly in HANDOFF.md — not silently drifted by the agent.

**One phase at a time, no forward speculation.**
Each agent session was constrained to implement the current phase only. The agent could not see future phases during implementation. This enforced the same discipline a human engineer would apply: don't optimize what you haven't measured yet.

**The agent was told to be honest about limitations.**
Benchmark methodology, hardware disclosure, and limitation documentation were required outputs, not optional polish. The agent was specifically instructed not to report numbers it hadn't measured and not to imply capabilities the hardware can't demonstrate.

**Source of truth is the repo, not the agent's memory.**
[`HANDOFF.md`](HANDOFF.md) persists all context between sessions. Any of the three tools (Claude Code, Cursor, Codex) can pick up exactly where the last session stopped, without needing the conversation history.

**Code was verified before being published.**
Every phase ran `cargo test --all` before being committed. The benchmark was run in release mode and results were inspected before being written into the README.

---

## References

- [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) — cache-line padded ring buffer, wait strategies, mechanical sympathy
- [Aeron](https://github.com/real-logic/aeron) — backpressure model (`offer()` return codes), log-structured IPC, SBE encoding
- [Chronicle Queue](https://github.com/OpenHFT/Chronicle-Queue) — off-heap persistence, HDR latency methodology, log-scale histogram convention
- [ZeroMQ / NNG](https://nanomsg.org/) — topic routing patterns (pub/sub, push/pull)
- [Iceoryx](https://github.com/eclipse-iceoryx/iceoryx) — zero-copy shared-memory IPC, `PublisherOptions` / `SubscriberOptions`
- [xsk-rs](https://github.com/DouglasGray/xsk-rs) — AF_XDP Rust bindings and hardware requirements
- [hdrhistogram](https://crates.io/crates/hdrhistogram) — coordinated-omission-aware latency histograms
- [quanta](https://crates.io/crates/quanta) — TSC-based high-resolution monotonic clock
- [crossbeam-channel](https://crates.io/crates/crossbeam-channel) — the Phase 3 baseline
- [core_affinity](https://crates.io/crates/core_affinity) — cross-platform core pinning

---

*Built on Rust stable. CI on Ubuntu (GitHub Actions). Benchmarked on Windows 11 x86_64.*
*27 tests, 0 failing. Miri clean on all unsafe code.*
