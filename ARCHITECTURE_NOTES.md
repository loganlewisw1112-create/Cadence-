# Architecture Notes — Cadence

Planning-phase notes. No code written yet — these are the design decisions
locked during scope research, to be filled out further as each phase is
implemented.

## Core Model

Single-process, in-process pub/sub bus. Publishers and subscribers
communicate over a message bus with topic-based routing. Two backends
exist by design, for comparison:

1. **Baseline (Phase 1):** `crossbeam-channel` / `std::sync::mpsc`. This is
   the honest "naive" comparison point — not a strawman. Crossbeam is a
   well-regarded, widely-used concurrent channel implementation.
2. **Optimized (Phase 4):** a hand-written, cache-aware, lock-free ring
   buffer, inspired by the LMAX Disruptor's single-writer principle:
   - Pre-allocated, fixed-capacity buffer (no runtime allocation on the
     hot path).
   - Power-of-two capacity, index masking via bitwise AND instead of
     modulo.
   - `CachePadded` head/tail cursors to avoid false sharing between
     producer and consumer cache lines.
   - Configurable wait strategy: busy-spin (lowest latency, burns a core),
     yield, or block (lowest CPU usage, higher latency) — the tradeoff is
     explicit and user-selectable, not hidden.

## Backpressure Policy

Aeron-inspired: `offer()` (or equivalent) returns a signal indicating
success, backpressure, or failure — the **producer** decides whether to
retry, drop, or block. The bus does not silently swallow drops; every drop
is counted and reported. This is tested explicitly (Phase 2): a test drives
the producer faster than the consumer can drain and asserts on the drop
counter.

## Message Layout

`#[repr(C)]` fixed-layout structs for the hot path, designed with
cache-line size and alignment in mind from Phase 1 even though the ring
buffer isn't built until Phase 4. Binary, not text — avoids
serialization/allocation overhead on the critical path. Loosely inspired
by SBE's flyweight pattern (fields ordered by descending size for
alignment), though Cadence does not implement a code-generated schema
compiler — that would be its own project.

## Timing & Measurement

- `quanta` for hot-path timestamps (TSC-based, near-zero overhead).
- `hdrhistogram` for latency distribution capture — fixed-cost recording,
  with explicit coordinated-omission handling (see `BENCHMARKING.md`).
- Full percentile reporting (p50/p95/p99/p99.9/max), not summary stats
  alone — "the interesting activity happens in the tail."

## Affinity & Topology

- `core_affinity` for thread pinning (producer/consumer pinned to specific
  cores to reduce context-switch-induced jitter and improve cache
  warmth).
- `hwloc` for topology discovery (sockets, NUMA nodes, cache levels) —
  run and disclosed regardless of whether the dev machine can demonstrate
  cross-NUMA effects. On single-socket hardware, cross-node latency is
  presented as a cited hypothesis from published benchmarks, not a
  measured result. This distinction is treated as important: claiming a
  measurement that wasn't actually possible on the available hardware
  would undermine the project's credibility more than just not having the
  hardware.

## What Cadence Deliberately Does Not Do (Yet)

- **No kernel-bypass networking (AF_XDP/DPDK).** Genuinely demonstrating
  this requires a NIC with zero-copy driver support and, ideally, a second
  machine — a single-laptop loopback/`veth` demo passes through the kernel
  stack and would not be performance-indicative. Documented as future
  work, not attempted.
- **No cross-process shared memory IPC.** In-process only for now (unlike
  Iceoryx). A natural next step, not in current scope.
- **No distributed/networked operation, no durability/replay.** This is
  not Kafka and is not trying to be — it's a low-latency in-process bus,
  and the README says so explicitly.

## Open Questions (to resolve as phases progress)

- SPSC first, or attempt MPSC directly in Phase 4? (Current plan: SPSC
  first, MPSC as a stretch goal if time allows — SPSC alone is already a
  real, defensible result.)
- Trie/Patricia-trie topic matching (Phase 2 stretch) vs. simple string
  match — decide based on how much time Phase 2 actually takes.
- Whether Phase 4b (io_uring) gets built at all depends on how much time
  Phase 4 (the locked, required track) consumes.
