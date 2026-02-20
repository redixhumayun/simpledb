# Async I/O and Queue-Depth Plan

## Problem

Current direct-vs-buffered benchmarks are primarily synchronous 4KiB request loops (depth ~= 1).
That setup can overstate per-operation overhead and underutilize NVMe parallelism, especially for direct I/O.

Result: we can conclude current behavior correctly, but we still lack data for "direct I/O with in-flight concurrency".

## Goal

Add a minimal async/batched I/O path and targeted benchmarks that measure performance as queue depth increases.

Primary question: does direct I/O become more competitive when we overlap I/O and raise outstanding requests?

## Concurrency Model Matrix

Queue-depth outcomes depend on both file-manager serialization and API shape.

```text
                     API: Sync/blocking              API: Async/batched
FM serialized     A) QD ~1 mostly                 B) Some QD possible
(global lock)     - simple                         - only if lock not held across wait
                  - weak Direct I/O gains          - lock contention still caps scaling

FM de-serialized  C) QD from many threads         D) Best QD/scaling
(no hot lock)     - helps MT workloads             - single-thread can pipeline
                  - single-thread still ~1         - strongest Direct I/O upside
```

Practical interpretation:
1. For single-thread 4KiB request loops, only **D** is expected to materially shift direct-vs-buffered outcomes.
2. **C** helps mainly when workloads already have concurrent callers.
3. **B** can improve queue depth only if submit and wait are decoupled (do not hold a global lock while waiting).

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

## Prerequisite: switch from seek+read to pread/pwrite

Before any async work, the FM should be migrated from `seek + read/write` to positional
I/O (`pread`/`pwrite`). In Rust this is `std::os::unix::fs::FileExt::read_at`/`write_at`.

### Why this matters

The global `Mutex` wrapping `FileManager` is currently held across the full seek→read/write
sequence. That means concurrent threads queue behind the lock for the entire NVMe round-trip
(~70-100µs per 4K random read on the PM9A1), giving QD=1 from the storage device's perspective
regardless of thread count.

With `read_at`/`write_at`, the file offset is passed as an argument — no shared fd position
state. The lock only needs to cover the `HashMap` lookup to retrieve the file descriptor.
The blocking syscall then happens outside the lock, allowing concurrent threads to have
multiple requests in flight simultaneously.

```
seek + read:  lock → seek → [hold across blocking read] → unlock   (QD = 1)
read_at:      lock → lookup fd → unlock → read_at(fd, buf, offset) (QD = N threads)
```

### Scope

- Applies to both `IoMode::Buffered` and `IoMode::Direct` — the serialization problem is
  identical for both paths. Do not limit this to the direct I/O path.
- `read_at`/`write_at` are POSIX and available on Linux and macOS. No platform gating needed.
  Only `O_DIRECT` itself remains Linux-gated.
- `append` is the exception: it is an inherently read-modify sequence (query length → write
  at that offset) and must still hold the lock across both steps to prevent two threads
  appending to the same offset.

### Implementation note

`FileSystemInterface::read` and `write` currently take `&mut self`. To release the lock
before the I/O syscall, these methods need to become `&self` with interior mutability on
the open file handles (e.g. wrapping each `OpenFile` in a `Mutex` or using `RwLock`).
This is a trait signature change that ripples through all implementors.

### Effect on benchmarks

- Single-threaded benchmarks (`sequential_read`, `random_read`, etc.): no improvement.
  One thread, no contention, pread vs seek+read is equivalent.
- Concurrent benchmarks (`concurrent_io_shared`, `concurrent_io_sharded`,
  `multi_stream_scan`): meaningful improvement for direct I/O, where there is no page
  cache to absorb misses. Threads will actually reach the NVMe simultaneously.

This change moves the FM from quadrant A to quadrant C in the concurrency matrix above.
io_uring (quadrant D) is still needed for single-thread pipelining.
