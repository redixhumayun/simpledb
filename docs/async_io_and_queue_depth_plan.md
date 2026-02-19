# Async I/O and Queue-Depth Plan

## Problem

Current direct-vs-buffered benchmarks are primarily synchronous 4KiB request loops (depth ~= 1).
That setup can overstate per-operation overhead and underutilize NVMe parallelism, especially for direct I/O.

Result: we can conclude current behavior correctly, but we still lack data for "direct I/O with in-flight concurrency".

## Goal

Add a minimal async/batched I/O path and targeted benchmarks that measure performance as queue depth increases.

Primary question: does direct I/O become more competitive when we overlap I/O and raise outstanding requests?

## Scope

1. Add async-capable read/write interface at file-manager layer.
2. Add queue-depth benchmarks (qd=1,4,16,32) for seq and random read-heavy workloads.
3. Keep existing synchronous path and benchmarks unchanged for baseline continuity.

## Non-goals

1. Rewriting the full query engine around async in the first pass.
2. Changing WAL durability semantics.
3. Replacing all existing benchmark suites.

## Where to implement

### Layer 1: File manager (required)

Add async-capable APIs in `FileSystemInterface`/`FileManager` (or parallel trait) to submit multiple operations and collect completions.

Options:
1. Linux-first `io_uring` backend.
2. Interim worker-pool + blocking pread/pwrite backend (portable, simpler).

### Layer 2: Buffer manager miss path (incremental)

Support prefetch windows for scans / miss-heavy paths so multiple page reads can be outstanding.

### Layer 3: Executor scan operators (optional in phase 1)

Pipeline page processing with prefetch to overlap CPU and storage latency.

## Benchmark additions

Add an async-focused suite (or extension of `io_patterns`) with:

1. `seq_read_qd{1,4,16,32}`
2. `rand_read_qd{1,4,16,32}`
3. `multistream_scan_qd{1,4,16,32}`

Output metrics:
1. Mean / p50 / p95 latency
2. Throughput (IOPS, MB/s)
3. Optional queue-depth utilization stats (submitted vs completed)

## Experiment matrix

Run with:
1. buffered vs direct
2. `capped` regimes (`hot`, `pressure`, `thrash`)
3. optional 1GiB cgroup memory cap for pressure testing

## Success criteria

1. Reproducible queue-depth scaling curves.
2. Clear comparison of direct vs buffered at qd=1 and qd>1.
3. Actionable guidance on when direct I/O should be enabled.

## Risks

1. Async implementation complexity can mask storage effects with software overhead.
2. Hard-to-compare results if benchmark definitions diverge from existing suites.

## Implementation phases

1. Phase 1: add async/batched file I/O abstraction and microbenchmarks.
2. Phase 2: add buffer-manager prefetch window support.
3. Phase 3: evaluate optional scan-operator pipeline integration.
