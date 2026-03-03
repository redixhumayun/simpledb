# Index Macro Benchmark Plan

## Goal

Add macro-level benchmarks that exercise index-backed query and update paths so index
concurrency and lock changes can be measured explicitly.

## Scope

Benchmark end-to-end SQL workloads that rely on B-tree indexes, not only buffer/page
microbenchmarks.

## Required Workloads

1. **Index point lookup (read-only)**
   - `SELECT ... WHERE indexed_col = ?`
   - single-thread and multi-thread throughput/latency

2. **Index range scan (read-only)**
   - `SELECT ... WHERE indexed_col BETWEEN ? AND ?`
   - short and long ranges to capture leaf traversal behavior

3. **Index-backed insert/update/delete**
   - `INSERT` into indexed table
   - `UPDATE indexed_col = ... WHERE ...`
   - `DELETE ... WHERE indexed_col = ...`
   - measure write throughput and lock wait/timeout behavior under contention

4. **Mixed read/write index workload**
   - 80/20 and 50/50 read-write mixes
   - same-index contention across multiple concurrent transactions

## Benchmark Dimensions

- Thread counts: `1, 2, 4, 8` (or current CI-safe subset)
- Data distributions: uniform + skewed (hot keys)
- Cardinalities: small, medium, large table sizes
- Query selectivity: highly selective vs moderately selective

## Metrics

- Throughput (ops/s)
- p50/p95 latency
- Lock timeouts/retries (if any)
- Optional: lock wait time instrumentation for index lock key

## Acceptance Criteria

- New benchmark group(s) in `benches/simple_bench.rs` (or a dedicated `benches/index_bench.rs`)
- Reproducible benchmark setup and teardown
- Documentation in `benches/README.md` for running only index benchmarks
- Baseline results captured in `docs/benchmarks/` for Linux + macOS (when available)

## Notes

- This complements `docs/btree_concurrency_plan.md` by quantifying performance impact of
  table-S/X index locking now and finer-grained latch crabbing in future work.
