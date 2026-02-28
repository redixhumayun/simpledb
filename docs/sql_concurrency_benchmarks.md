# SQL Concurrency Benchmarks for Lock-Granularity Work

## Goal
Add a minimal macro-level benchmark set that can detect performance changes from lock-granularity changes (issue #59) without expanding scope into a large benchmark matrix.

## Benchmarks to Add

1. `SQL Concurrent SELECT (same page, disjoint RID)`
- Multiple concurrent transactions.
- All reads target rows that live on the same page.
- Each worker reads different rows (disjoint RID set).
- Purpose: isolate shared-lock/read-latch behavior under contention on one page.

2. `SQL Concurrent UPDATE (same page, disjoint RID)`
- Multiple concurrent transactions.
- All updates target rows on the same page.
- Each worker updates different rows (disjoint RID set).
- Purpose: isolate write-lock/upgrade path and exclusive access behavior.

3. `SQL Concurrent Mixed 80/20 RW (same page, disjoint RID)`
- Multiple concurrent transactions.
- 80% reads, 20% updates, same-page/disjoint-RID targeting.
- Purpose: approximate realistic mixed contention while still stressing lock granularity.

## Why These Three

- `SELECT` and `UPDATE` independently expose regressions that a mixed workload can hide.
- `80/20` provides the primary macro signal for practical throughput/latency impact.
- Same-page/disjoint-RID targeting directly exercises the lock-scope change in #59.

## Measurements

Track at minimum:

1. Throughput (`ops/s` or `txns/s`)
2. Latency (Criterion/Bencher primary latency signal; p95 where available)
3. Timeout/retry count

Implementation note:
- Current benchmark stack is Criterion + Bencher CI adapter (`rust_criterion`), not the old custom JSON benchmark framework.
- Throughput and timeout/retry counters are **not** emitted automatically; benchmark code must compute/report them explicitly.
- For `SELECT` benchmarks, execute the scan (consume iterator), not just `create_query_plan(...)`.

## Non-Goals

- Not a full concurrency benchmark matrix.
- Not replacing existing buffer/I/O microbenchmarks.
- Not adding planner/optimizer benchmarks here.
