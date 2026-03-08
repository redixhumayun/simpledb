# B-Tree Concurrency Baseline (Table-Level Index Locking)

This document captures baseline numbers before latch crabbing and fine-grained logical index locks.
Use this as the performance floor for comparisons.

## How to reproduce

```bash
# From repository root, on the commit that introduced the benchmark:
CI=1 cargo bench --bench simple_bench \
    --no-default-features --features replacement_lru --features page-4k \
    -- "Index Concurrency"
```

`CI=1` uses the fast Criterion profile (1 s warmup, 5 s measurement, 100 samples).
Remove `CI=1` for full profiling runs.

## Workload parameters

| Parameter | Value |
|---|---|
| Workers (`CONC_WORKERS`) | 4 |
| Ops per worker (`CONC_OPS_PER_WORKER`) | 24 |
| Total elements per iteration | 96 |
| Pre-populated rows (lookup baseline) | 200 |
| Per-op transaction granularity | 1 txn per op (create `BTreeIndex` -> op -> commit) |
| Locking model | table-level `S` (read) / `X` (write) on `index_lock_table_id` |

## Environment

| Field | Value |
|---|---|
| CPU | Intel Xeon E3-1275 v5 @ 3.60 GHz (8 logical cores) |
| OS kernel | Linux 6.8.0-100-generic |
| Rust toolchain | rustc 1.92.0 (ded5c06cf 2025-12-08) |
| Cargo | 1.92.0 (344c4567c 2025-10-21) |
| Feature flags | `replacement_lru`, `page-4k` |
| Criterion profile | `CI=1` (1 s warmup, 5 s measurement, 100 samples) |

## Results

| Benchmark | Time / iter (low-high, 95% CI) | Throughput (elem/s) |
|---|---|---|
| Concurrent INSERT disjoint-key | 1.654 s - 1.711 s | 56.1 - 58.0 (mean **57.2**) |
| Concurrent LOOKUP pre-populated | 797 ms - 804 ms | 119.4 - 120.4 (mean **119.9**) |
| Concurrent mixed 80/20 RW | 984 ms - 1.062 s | 90.4 - 97.6 (mean **94.3**) |

## Interpretation

- INSERT is slowest (~57 elem/s) because each insert takes global index table `X`; workers serialize.
- LOOKUP is fastest (~120 elem/s) because readers share table `S`.
- Mixed 80/20 sits between due to write intervals blocking readers.

Expected after `IS`/`IX` + `IndexKey`/`IndexRange` + crabbing:

- INSERT should scale toward multi-writer throughput.
- LOOKUP should remain similar or improve slightly.
- Mixed should move closer to lookup throughput.
