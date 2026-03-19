# Direct I/O Zero-Copy Baseline Benchmarks

**Date**: 2026-03-19
**Branch**: `enhance/direct-io-zero-copy` (pre-change baseline)
**Commit**: `49dbde8`
**Purpose**: Baseline before eliminating bounce-buffer copies in the direct I/O path (`repr(align)` on `PageBytes`).

## Build Configuration

```
cargo bench --bench io_patterns --no-default-features --features replacement_lru --features page-4k
SIMPLEDB_BENCH_BUFFERS=12
```

Features active: `replacement_lru`, `page-4k`, `direct-io` (default).
Page size: 4096 bytes. Buffer pool: 12 buffers.

## What we are measuring

With direct I/O enabled, the current code has two full-page `memcpy` calls per I/O:

- **Read**: io-uring completes into `AlignedBuf` → `copy_from_slice` into `PageBytes`
- **Write**: `copy_from_slice` from `PageBytes` into `AlignedBuf` → `pwrite`

The planned fix adds `#[repr(align(4096))]` to `PageBytes`, making the embedded array
naturally O_DIRECT-aligned so the bounce buffer is eliminated entirely.

These results are the pre-fix baseline.

---

## Phase 1 — Raw I/O Throughput (1000 ops, no buffer pool eviction)

| Benchmark              | Mean (ms) | CI low | CI high |
|------------------------|-----------|--------|---------|
| Sequential Read        | 1.9792    | 1.9653 | 1.9940  |
| Sequential Write       | 1.7404    | 1.7261 | 1.7580  |
| Random Read            | 2.0252    | 2.0146 | 2.0367  |
| Random Write           | 1.7358    | 1.7203 | 1.7537  |

## Phase 1 — Queue Depth Scaling (Sequential Read, 1000 ops)

| Queue Depth | Mean (ms) | Throughput (Kelem/s) |
|-------------|-----------|----------------------|
| QD=1        | 1.9686    | 507.97               |
| QD=16       | 2.0054    | 498.66               |
| QD=32       | 2.0127    | 496.84               |

## Phase 1 — Queue Depth Scaling (Random Read, 1000 ops)

| Queue Depth | Mean (ms) | Throughput (Kelem/s) |
|-------------|-----------|----------------------|
| QD=1        | 2.0418    | 489.77               |
| QD=16       | 1.9874    | 503.17               |
| QD=32       | 2.0269    | 493.37               |

## Phase 2 — WAL

| Benchmark             | Mean      | Throughput          |
|-----------------------|-----------|---------------------|
| append no-fsync       | 233.21 ms | 4,288 elem/s        |
| append immediate-fsync| 892.40 ms | 112 elem/s          |
| group commit (10)     | 1.1028 s  | 907 elem/s          |

## Phase 3 — Mixed R/W (500 ops each pattern)

| Benchmark                     | Mean       | Throughput          |
|-------------------------------|------------|---------------------|
| Mixed 70/30 no-fsync          | 38.934 ms  | 12,842 elem/s       |
| Mixed 70/30 immediate-fsync   | 1.3461 s   | 371 elem/s          |
| Mixed 70/30 group-10          | 170.25 ms  | 2,937 elem/s        |
| Mixed 10/90 no-fsync          | 109.51 ms  | 4,566 elem/s        |
| Mixed 10/90 immediate-fsync   | 4.0173 s   | 124 elem/s          |
| Mixed 10/90 group-10          | 504.87 ms  | 990 elem/s          |

## Phase 4 — Concurrent I/O (200 ops/thread)

| Benchmark          | Mean       | Throughput          |
|--------------------|------------|---------------------|
| 2T  no-fsync       | 15.671 ms  | 12,762 elem/s       |
| 2T  group-10       | 69.436 ms  | 2,880 elem/s        |
| 4T  no-fsync       | 30.048 ms  | 13,312 elem/s       |
| 4T  group-10       | 140.03 ms  | 2,857 elem/s        |
| 8T  no-fsync       | 60.719 ms  | 13,175 elem/s       |
| 8T  group-10       | 283.40 ms  | 2,823 elem/s        |
| 16T no-fsync       | 125.80 ms  | 12,719 elem/s       |
| 16T group-10       | 564.63 ms  | 2,834 elem/s        |

## Phase 5 — Durability

| Benchmark                                     | Mean      |
|-----------------------------------------------|-----------|
| Random Write immediate-fsync / data-nosync    | 892.37 ms |
| Random Write immediate-fsync / data-fsync     | 1.2361 s  |

## Phase 7 — Cache-Adverse Access Patterns (1000 blocks)

| Benchmark                  | Mean       |
|----------------------------|------------|
| One-pass Seq Scan          | 2.0279 ms  |
| Low-locality Rand Read     | 2.0268 ms  |
| Multi-stream Scan          | 717.49 µs  |

## Phase 8 — Cache Eviction (1000 blocks, 12-buffer pool forces eviction)

| Benchmark                     | Mean       |
|-------------------------------|------------|
| One-pass Seq Scan + Evict     | 3.8365 ms  |
| Low-locality Rand Read + Evict| 58.604 ms  |
| Multi-stream Scan + Evict     | 2.4442 ms  |

---

## Notes

- Queue depth scaling (QD 1 → 32) shows essentially flat throughput (~490–510 Kelem/s). This is
  expected on a single-disk setup where the bounce-buffer copy is CPU-bound and the disk is
  already saturated at QD=1. Higher QD adds no benefit here.
- Write throughput is slightly higher than read (~1.74 ms vs ~1.98 ms for seq) despite the extra
  copy; likely due to OS write coalescing even with O_DIRECT.
- The `Low-locality Rand Read+Evict` (Phase 8) at 58.6 ms vs 2.0 ms for cached reads is the
  clearest signal of real disk I/O cost — a 29× penalty. The bounce-buffer copies are baked into
  this number.
- After the `repr(align)` change, Phase 8 and the raw read throughput numbers are the best
  candidates to show improvement. Phase 1 writes and concurrent no-fsync paths are also expected
  to improve.
