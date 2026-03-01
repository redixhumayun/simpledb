# SQL Concurrency Baseline (Branch-Local)

Branch-local baseline for before/after lock-granularity changes.

- Commit: `22620aa`
- Captured: `2026-03-01 00:13:36 UTC`
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

- time: `[840.04 ms, 849.79 ms, 859.13 ms]`
- throughput: `[111.74, 112.97, 114.28] elem/s`
- counters observed: `retries=0 timeouts=0 errors=0`

### Concurrent UPDATE same-page disjoint-id

- time: `[78.361 s, 78.590 s, 78.783 s]`
- throughput: `[1.2185, 1.2215, 1.2251] elem/s`
- counters observed per iteration: around `retries=192 timeouts=288 errors=0`

### Concurrent mixed 80/20 RW same-page disjoint-id

- pilot counters observed: around `retries=32-33 timeouts=47-48 errors=0` (iters=1)
- Criterion estimate for this config: about `137.14 s` for 10 samples
- Full analyzed interval not captured in this snapshot (run intentionally stopped early)

## Notes

- This file is intentionally branch-scoped and can be replaced as baseline runs are refreshed.
- Use same flags/command shape for after-change runs to keep comparison fair.
