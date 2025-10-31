# SimpleDB Performance Alignment Plan

## Summary

Codify a performance engineering strategy for SimpleDB that aligns with the guidance in the [sled theoretical performance guide](https://sled.rs/perf.html). The focus is to:

- Stabilize benchmark hardware and collect reproducible baselines.
- Map the end-to-end request pipeline, align stage throughput, and add backpressure.
- Trim hot-path work, batch background tasks, and optimize memory layout.
- Separate synchronous durability from background I/O to reduce write amplification.
- Establish disciplined profiling and benchmarking practices before changing code.

This document captures the actions required to make the benchmarking data trustworthy and to prioritize future optimizations.

## Hardware Baseline & Reproducibility

- **Reference platform**: Dell XPS 13-9370, Intel i7-8650U (4C/8T, 1 socket), 16 GB DDR4-2133, Netac NVMe SSD 512 GB (PCIe), Ubuntu Linux 6.8.0-86-generic.
- **Storage capability (fio, direct I/O)**:
  - Sequential write (1 MiB blocks): 1.27 GiB/s, 1.27k IOPS.
  - Sequential read (1 MiB blocks): 2.97 GiB/s, 2.97k IOPS.
  - Random write (4 KiB blocks): 83.5k IOPS, 326 MiB/s.
  - Random read (4 KiB blocks): 109k IOPS, 426 MiB/s.
- **Governor configuration**: Keep the runner on the `performance` governor to avoid frequency drift; verify with `cpupower frequency-info`. Revert to `powersave` only when not benchmarking.
- **Benchmark hygiene**:
  - Pin microbenchmarks to dedicated cores when possible (`taskset`).
  - Warm caches explicitly before timing; record medians and percentiles.
  - Log environment metadata (kernel, governor, turbo state) inside benchmark output.

## Stage Alignment & Backpressure

- Instrument planner, executor, buffer manager, and file manager stages with coarse timers to surface mismatched service rates.
- Track queue depth (or wait time) per stage; expose metrics from buffer manager (e.g., number of waiting transactions) to detect bottlenecks.
- Implement explicit backpressure: cap outstanding pins or transactions and return errors/timeouts when limits are exceeded rather than letting queues grow unbounded.
- Document target throughput per stage and align them (speed up the slowest stage, or throttle upstream) to avoid “worst of all worlds” latency/throughput behavior highlighted in the sled guide.

## Hot Path Simplification

- Audit buffer manager operations (`pin`, `unpin`) to ensure no unnecessary stats collection, logging, or allocations occur on every call. Move optional work behind feature flags or background aggregators.
- Cache-friendly layout: pack frequently-read metadata together, avoid `Arc` churn by reusing handles, and pre-allocate vectors for long-lived structures (catalog, schema objects).
- Use DHAT or other allocation profilers to identify short-lived allocations that can be stack-allocated or pooled.

## Background & Batching Strategy

- Shift expensive maintenance tasks (buffer stats, eviction logging, tracing) off the critical path. Accumulate data and flush/batch periodically in a worker thread.
- Align with sled guidance: only perform the work required for correctness synchronously; everything else should be deferred.

## Durability & Write Path Improvements

- Separate Write-Ahead Log (WAL) persistence from buffer eviction.
  - Current dirty eviction forces synchronous flush; plan for background flusher to empty dirty frames outside the request path.
  - Implement group commit (batch WAL flush) to amortize fsync cost across transactions.
- Verify WAL ordering: ensure log fsync happens before data flush, but allow the latter to happen asynchronously.
- Provide instrumentation to measure flush batching effectiveness (e.g., number of pages per batch, average flush latency).

## Profiling Workflow

- Standardize profiling commands:
  - `perf record --call-graph dwarf -- cargo bench --bench buffer_pool -- 100 12 -- --json`
  - Flamegraph generation for both single-threaded and multi-threaded benches.
  - Macro-benchmark deletion profiling (temporarily disable components to bound potential gains).
- Track metrics over time in the docs repo (link perf captures, flamegraphs) to avoid repeated work.
- Combine CPU profiling with allocation profiling (DHAT, heaptrack) to detect time-space trade-offs.

## Benchmark Reporting Enhancements

- Extend JSON output to include p50/p90/p99 latencies and standard deviation for better insight into variability.
- Include cache hit/miss counters and buffer queue depths in benchmark logs to correlate with throughput changes.
- Optionally rerun each suite multiple times and average results to reduce noise, but document the methodology in `docs/benchmarking.md`.

## Next Steps

1. Add instrumentation hooks to measure stage latencies and queue depths.
2. Implement buffer manager backpressure and background dirty-page writer.
3. Extend benchmark JSON to report latency percentiles and environment metadata.
4. Define a profiling checklist (perf, flamegraph, allocation) and store references in `docs/performance_alignment_plan.md`.
5. Reset historical benchmark data (gh-pages) after stabilizing the runner to establish the new baseline.

## References

- sled theoretical performance guide: <https://sled.rs/perf.html>
- `fio` measurements captured on 2025-10-31 (see Hardware Baseline section).
