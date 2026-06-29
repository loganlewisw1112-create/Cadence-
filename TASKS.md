# Tasks — Cadence

## Phase 1: Foundation
> Create Rust workspace, message structs, CLI skeleton, and simple in-process publish/subscribe over crossbeam-channel.
- [ ] Confirm files and current repo state
- [ ] Pin Rust toolchain version (`rust-toolchain.toml`)
- [ ] Scaffold workspace: `message-core`, `bus`, `cli`, `bench`
- [ ] Define message struct with `#[repr(C)]`, cache-line/power-of-two
      sizing considered now (even though the optimized ring buffer is
      Phase 4)
- [ ] Implement basic in-process pub/sub over `crossbeam-channel`
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md

## Phase 2: Correctness
> Add topic routing, bounded queues, delivery tests, dropped-message tests, and serialization tests.
- [ ] Confirm files and current repo state
- [ ] Implement topic-based routing (simple match; trie/Patricia-trie
      stretch goal — cite ZeroMQ/nanomsg precedent if implemented)
- [ ] Implement bounded queue with explicit drop-vs-retry policy
      (Aeron-style: `offer()` returns a backpressure signal; producer
      decides)
- [ ] Test: message delivery correctness
- [ ] Test: topic filtering
- [ ] Test: backpressure — drive producer faster than consumer, assert on
      drop counter (not just "queue is bounded")
- [ ] Test: serialization round-trip
- [ ] Add Miri to CI for any `unsafe` introduced so far
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md

## Phase 3: Metrics
> Add nanosecond timestamping, latency histogram, throughput metrics, and JSON benchmark export.
- [ ] Confirm files and current repo state
- [ ] Integrate `quanta` for TSC-based hot-path timestamps
- [ ] Integrate `hdrhistogram` for latency distribution capture
- [ ] Decide and document coordinated-omission handling: open-loop
      constant-arrival-rate generator OR `record_correct(value,
      expected_interval)` — write the decision into `BENCHMARKING.md`
- [ ] Report full percentiles: min, mean, p50, p95, p99, p99.9, max
      (not just min/mean/max)
- [ ] Implement throughput metrics
- [ ] Implement JSON benchmark export
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md

## Phase 4: Optimization — Option A (locked: ring buffer + pinning)
> Hand-write a cache-aware lock-free ring buffer, add core pinning, benchmark honestly against the Phase 1 baseline.
- [ ] Confirm files and current repo state
- [ ] Implement SPSC ring buffer: pre-allocated, single-writer principle,
      power-of-two capacity with bitmask indexing, `CachePadded`
      head/tail cursors
- [ ] Implement configurable wait strategy (busy-spin / yield / block)
- [ ] (Stretch) Extend to MPSC if time allows
- [ ] Add `core_affinity` thread pinning
- [ ] Add `hwloc` topology dump (sockets, NUMA nodes, cache levels) for
      disclosure — run regardless of whether NUMA effects are measurable
      on this hardware
- [ ] Benchmark: ring buffer vs. `crossbeam-channel`/`std::mpsc`, at burst
      size 1 and burst size N
- [ ] Benchmark: pinned vs. unpinned, same ring buffer
- [ ] Run Miri (and consider `loom`) against the ring buffer's `unsafe`
      core
- [ ] Document result honestly even if the ring buffer doesn't win at
      every burst size / percentile on this hardware
- [ ] Explicitly mark cross-NUMA effects as future work / cited hypothesis
      if only single-socket hardware is available — do not claim measured
      numbers that weren't measured
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md

### Phase 4b: I/O track (optional, time-permitting)
> io_uring vs. epoll/std I/O on a persistence/ingest path, reported honestly.
- [ ] Confirm files and current repo state
- [ ] Implement a persistence/ingest path using the `io-uring` crate
- [ ] Implement the same path over epoll/std I/O as the comparison baseline
- [ ] Benchmark both; report where io_uring helps, where it doesn't, and
      why (batching/registered buffers vs. naive swap)
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md

### Future Work (document only — do not implement or benchmark)
- [ ] Write up AF_XDP/DPDK requirements and why they're out of scope for a
      single-machine build (no supported NIC/zero-copy driver, no second
      machine, veth demo would be non-indicative per xsk-rs docs)
- [ ] Write up cross-process shared-memory IPC (Iceoryx-style) as a
      natural next step

## Phase 5: Portfolio Polish
> Add diagrams, benchmark screenshots, reproducible commands, and a hardware/spec disclosure.
- [ ] Confirm files and current repo state
- [ ] Create `BENCHMARKING.md` (methodology + environment disclosure
      template) if not already finalized in Phase 3
- [ ] Generate log-scale latency histogram plots (Disruptor/Chronicle
      convention)
- [ ] Commit HdrHistogram logs for reproducibility/audit
- [ ] Write architecture diagram
- [ ] Write "Design Decisions & Tradeoffs" section
- [ ] Write "Limitations" section (single machine, single NUMA node, no
      kernel bypass, mean-vs-tail caveats)
- [ ] Write "Future production upgrades" section (AF_XDP/DPDK, cross-NUMA,
      shared-memory IPC — clearly future, not implied-done)
- [ ] Write dual-audience README pass (generalist quick-start +
      HFT-flavored depth section)
- [ ] Write "How Cursor/Codex/Claude were used responsibly" section
- [ ] Add/update tests or verification command
- [ ] Update README if needed
- [ ] Update HANDOFF.md
