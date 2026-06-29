# Cadence — Expert Local Repo Build Plan

> Formerly "Microsecond Message Bus." Renamed to **Cadence** — the name signals
> *predictable, low-jitter delivery*, not just raw speed. That's the more
> sophisticated story for a ring-buffer-based bus and the one that flatters
> both HFT and generalist infra reviewers. See "Naming Rationale" below.

## Portfolio Lane

Low-latency systems / quant infrastructure / real-time event pipelines

## Portfolio Headline

> A lock-free, low-latency pub/sub message bus in Rust, with benchmarked
> p50/p95/p99/p99.9 latency, throughput, and dropped-message reporting —
> naive channel baseline vs. a hand-written cache-aware ring buffer, measured
> honestly.

## Objective

Prove systems-level skill: queues, timestamps, routing, backpressure,
lock-free data structures, CPU/cache mechanics, rigorous benchmarking,
latency-distribution reporting, and honest performance disclosure.

## Recommended Stack

Rust workspace. No cloud dependency. Optional dashboard later with Next.js.

### Crate stack (locked)

- **Baseline concurrency:** `crossbeam-channel`, `std::sync::mpsc` (the
  honest comparison targets — not strawmen)
- **Optimized ring buffer (Phase 4):** hand-written, no crate — this is the
  skill being demonstrated. `crossbeam-utils::CachePadded` for false-sharing
  avoidance. Reference (not dependency): `disruptor` (nicholassm/disruptor-rs)
  and `rtrb` as prior art / secondary benchmark targets.
- **Timing:** `quanta` (TSC-based)
- **Latency stats:** `hdrhistogram` (native Rust port, supports
  coordinated-omission correction via `record_correct`)
- **Throughput microbenchmarks only:** `criterion` (wrong tool for
  end-to-end latency distributions — see BENCHMARKING.md)
- **Serialization:** `zerocopy` / `bytemuck` for fixed-layout `#[repr(C)]`
  messages on the hot path (closest to SBE's flyweight model); `bincode` or
  `postcard` acceptable for the simpler path if needed
- **Affinity:** `core_affinity` for thread pinning; `hwloc`/`hwloc2` for
  topology discovery (sockets, NUMA nodes, cache levels) — used for
  disclosure even where only single-node pinning is demonstrated
- **Optional I/O track (Phase 4b):** `io-uring` crate, compared honestly
  against epoll/std I/O on a persistence/ingest path
- **Verification:** Miri in CI for any `unsafe` in the ring buffer; consider
  a `loom` model-checking pass on the SPSC/MPSC core

## Local Repo Compatibility

This project must be buildable by any of these tools:

- Cursor Pro
- Codex Pro
- Claude Code Pro

The project state must live in the repo files, especially:

- `PROJECT_PLAN.md`
- `TASKS.md`
- `HANDOFF.md`
- `README.md`
- `BENCHMARKING.md`

Do not depend on a single tool's chat memory.

## MVP Scope

- Rust workspace with `message-core`, `bus`, `cli`, and `bench` modules.
- Publisher and subscriber CLI modes.
- Topic-based routing (simple match initially; trie/Patricia-trie considered
  in Phase 2 if time allows — cite ZeroMQ/nanomsg precedent).
- Timestamped binary messages (`#[repr(C)]`, cache-line-aware layout).
- Latency stats: min, mean, p50, p95, p99, **p99.9**, max — full
  distribution via `hdrhistogram`, not just summary stats.
- Dropped-message counter and bounded-queue backpressure with an explicit,
  documented drop-vs-retry policy (Aeron-style: `offer()` returns a signal;
  the producer decides).
- Benchmark mode comparing the naive channel baseline
  (`crossbeam-channel` / `std::sync::mpsc`) vs. the hand-written optimized
  ring buffer — both at burst size 1 and burst size N, both pinned and
  unpinned.
- Benchmark JSON export, plus committed HdrHistogram logs for
  reproducibility/audit.

## Build Phases

### Phase 1: Foundation
Create Rust workspace, message structs (with `CachePadded` and power-of-two
capacity decisions baked into the design from day one — even though the
ring buffer isn't built yet), CLI skeleton, and simple in-process
publish/subscribe over `crossbeam-channel`. Pin the Rust toolchain version.

### Phase 2: Correctness
Add topic routing, bounded queues, delivery tests, dropped-message tests,
and serialization tests.
- Explicit backpressure policy: a test that drives producer faster than
  consumer and asserts on the drop counter (not just that the queue is
  "bounded" in name).
- Miri in CI for any `unsafe` introduced.

### Phase 3: Metrics
Add nanosecond timestamping (`quanta`), latency histogram (`hdrhistogram`),
throughput metrics, and JSON benchmark export.
- Require full percentile reporting: p50/p95/p99/p99.9/max, not just
  min/mean/max.
- Explicit coordinated-omission handling: either an open-loop
  constant-arrival-rate generator, or `record_correct(value,
  expected_interval)`. Document which, and why.

### Phase 4: Optimization — **Option A (locked)**
Hand-write a cache-aware lock-free ring buffer (LMAX-Disruptor-style:
pre-allocated, single-writer principle, power-of-two sizing with bitmask
indexing, `CachePadded` head/tail cursors, configurable wait strategy —
busy-spin/yield/block). Add core pinning via `core_affinity`. Benchmark
head-to-head against the Phase 1 `crossbeam-channel`/`std::mpsc` baseline,
at burst size 1 and burst size N, pinned vs. unpinned.

- Reference `disruptor-rs`'s published deltas (32ns vs 65ns at burst 1; 8ns
  vs 29ns at burst 100 — mean-only, no percentiles, on a 2016 MacBook Pro)
  as an illustrative *achievable delta*, not a target to match.
- Run an `hwloc` topology dump regardless of hardware. On single-socket
  dev hardware, NUMA cross-node effects are **not measurable** — say so
  explicitly rather than faking it. Present cross-node latency as a cited
  hypothesis (~330ns same-node vs ~590ns cross-node, per hwloc/OpenMPI
  tutorial material on a specific Xeon) rather than a measured result.
- If the ring buffer does *not* beat the baseline on this hardware at some
  burst size or percentile, report that honestly and investigate — a
  correctly-diagnosed miss is a stronger signal than a hidden one.

#### Phase 4b: I/O track (optional, time-permitting)
io_uring vs. epoll/std I/O on a persistence or ingest path, reported
honestly. Per TU Darmstadt's VLDB 2026 study (arXiv:2512.04859), naive
io_uring swaps yield only ~1.06x, while batching + registered buffers can
reach ~2.05–2.5x — and some workloads see no gain or a regression vs epoll.
Demonstrating *when* it helps and when it doesn't is the credible result;
do not claim a universal win.

#### Future Work (explicitly scoped, not built)
- **AF_XDP / DPDK kernel bypass.** Documented as future work only. A
  `veth`-pair demo on a single laptop passes through the kernel stack and
  is explicitly non-indicative of real kernel-bypass performance (per
  xsk-rs's own docs) — claiming numbers here would be the single fastest
  way to lose credibility with an HFT reviewer who knows the space. State
  what real AF_XDP would require (supported NIC + zero-copy driver mode +
  eBPF/XDP program + ideally a second machine) and stop there.
- **Cross-process shared-memory IPC** (Iceoryx-style zero-copy), as a
  natural next step beyond in-process pub/sub.

### Phase 5: Portfolio Polish
- Diagrams, benchmark screenshots (log-scale latency histograms — the
  Disruptor/Chronicle convention), reproducible commands, full
  hardware/spec disclosure.
- Dedicated `BENCHMARKING.md` (methodology + environment template) — see
  that file for the full standard.
- "Design Decisions & Tradeoffs" section in the README.
- "Limitations" section: single machine, single NUMA node, no
  kernel-bypass, mean-vs-tail caveats, "measured on my hardware; results
  will vary."
- README written for both audiences: a top section legible to generalists
  (what/why/how-to-run in 30 seconds) and a deeper section with
  lock-free/tail-latency detail for HFT-flavored readers.

## Acceptance Criteria

- Runs locally with no cloud dependency.
- Produces repeatable benchmark output in release mode.
- Includes tests for message delivery, topic filtering, and dropped-message
  behavior — including an explicit backpressure test (producer-faster-than-
  consumer, drop counter assertion).
- Benchmark methodology avoids coordinated omission and is documented in
  `BENCHMARKING.md`.
- README contains real benchmark results, full hardware/environment
  disclosure, and limitations.
- No exaggerated performance claims without measured evidence. Where a
  result can't be honestly measured on available hardware (e.g.
  cross-NUMA, kernel bypass), it is labeled as future work, not claimed.

## Required Files Before Coding

Every agent must read:

1. Root `AGENTS.md`
2. Root `REPO_STANDARD.md`
3. Root `TOOL_SWITCHING_GUIDE.md`
4. This `PROJECT_PLAN.md`
5. This project's `TASKS.md`
6. This project's `HANDOFF.md`
7. This project's `BENCHMARKING.md` (once Phase 3 begins)

## First Agent Prompt

```txt
Read AGENTS.md, REPO_STANDARD.md, and this PROJECT_PLAN.md. Build Phase 1
only for Cadence. Create a Rust workspace with a simple publisher/subscriber
CLI over crossbeam-channel and basic tests. Bake CachePadded and
power-of-two capacity decisions into the message/queue design now, even
though the optimized ring buffer isn't built until Phase 4. Do not optimize
yet. Update HANDOFF.md with changed files, commands run, and next step.
```

## Naming Rationale

"Microsecond Message Bus" was descriptive but generic and reads as a
category, not a product. Conventions in this space favor a single
evocative word: Aeron, Disruptor, Chronicle, Iceoryx, Agrona — short,
pronounceable, suggestive of speed/time/flow/precision, not literally
descriptive. Avoid anything containing "message bus," "MQ," "pubsub,"
"fast," or "micro."

**Cadence** was chosen over the alternatives (Tachyon, Celerity, Volley,
Conduit, Relay, Synapse) because it leads with the more sophisticated
half of the story: *predictable, low-jitter delivery*, not just peak
speed. That's a deliberate signal that the project understands tail
latency (p99.9) matters more than mean throughput — which is the exact
distinction a careful HFT or infra reviewer is looking for.

## Final Portfolio README Requirements

The final README must include:

- Portfolio headline
- Problem being solved
- Why this maps to high-value development roles (both HFT/quant and
  generalist backend/infra framing)
- Architecture diagram
- Tech stack
- Local setup
- Test/benchmark commands
- Demo script
- Design decisions & engineering tradeoffs
- Known limitations (explicit, including what was *not* measured)
- Future production upgrades (incl. AF_XDP/DPDK, cross-NUMA, shared-memory
  IPC — clearly future, not implied-done)
- How Cursor/Codex/Claude were used responsibly
