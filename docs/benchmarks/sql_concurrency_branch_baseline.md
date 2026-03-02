# SQL Concurrency Baseline (Branch-Local)

Branch-local baseline for before/after lock-granularity changes.

- Commit: `3a1c0d5`
- Captured: `2026-03-02 01:13:32 UTC`
- Benchmark binary: `target/release/deps/simple_bench-c756f2129f3691a9`
- Flags: `--bench --noplot --sample-size 10 --warm-up-time 0.5 --measurement-time 1`

## Commands

```bash
BIN=$(ls target/release/deps/simple_bench-* | head -n1)

$BIN --bench --noplot --sample-size 10 --warm-up-time 0.5 --measurement-time 1 \
  "Concurrent SELECT same-page disjoint-id"

$BIN --bench --noplot --sample-size 10 --warm-up-time 0.5 --measurement-time 1 \
  "Concurrent UPDATE same-page disjoint-id"

$BIN --bench --noplot --sample-size 10 --warm-up-time 0.5 --measurement-time 1 \
  "Concurrent mixed 80/20 RW same-page disjoint-id"
```

## Results

### Concurrent SELECT same-page disjoint-id

- time: `[834.44 ms, 847.82 ms, 862.97 ms]`
- throughput: `[111.24, 113.23, 115.05] elem/s`
- counters observed: `retries=0 timeouts=0 errors=0`

### Concurrent UPDATE same-page disjoint-id

- time: `[1.7297 s, 1.7517 s, 1.7791 s]`
- throughput: `[53.960, 54.803, 55.502] elem/s`
- counters observed: `retries=0 timeouts=0 errors=0`

### Concurrent mixed 80/20 RW same-page disjoint-id

- time: `[1.0285 s, 1.0379 s, 1.0466 s]`
- throughput: `[91.722, 92.493, 93.344] elem/s`
- counters observed: `retries=0 timeouts=0 errors=0`

## Notes

- This file is intentionally branch-scoped and can be replaced as baseline runs are refreshed.
- Use same flags/command shape for after-change runs to keep comparison fair.
