# SimpleDB Benchmarks

Stdlib-only benchmarking framework with CLI filtering support.

## Quick Start

```bash
# SQL benchmarks - run all
cargo bench --bench simple_bench -- 50

# SQL benchmarks - run only INSERT
cargo bench --bench simple_bench -- 50 "INSERT"

# Buffer pool benchmarks - run all
cargo bench --bench buffer_pool -- 50 12

# Buffer pool benchmarks - run only Random K=10
cargo bench --bench buffer_pool -- 50 12 "Random (K=10,"
# Buffer pool benchmarks - run only the 8-thread pin workload
cargo bench --bench buffer_pool -- 100 12 pin:t8
```

## Filtering Benchmarks

Filter using substring matching on benchmark names (Phase 1-4) or case tokens (Phase 5):

```bash
# Syntax: -- <iterations> [num_buffers] [filter]
cargo bench --bench buffer_pool -- 100 12 "Pin/Unpin"      # Phase 1: Pin/Unpin only
cargo bench --bench buffer_pool -- 100 12 "Zipfian"        # Phase 2+4: Zipfian tests
cargo bench --bench buffer_pool -- 100 12 "Random (K=10,"  # Specific K value (not K=100)
cargo bench --bench simple_bench -- 100 "SELECT"           # Both SELECT benchmarks

# Phase 5 case tokens (no quotes needed):
cargo bench --bench buffer_pool -- 200 12 pin:t4           # Multi-threaded pin, 4 threads
cargo bench --bench buffer_pool -- 200 12 hotset:t8_k4     # Hot-set contention, 8 threads, K=4
```

**Use case:** Isolate specific workloads for profiling (flamegraphs, perf analysis) without noise from other benchmarks.

## Benchmark Suites

### simple_bench.rs
SQL operation benchmarks: INSERT, SELECT (table scan + COUNT), UPDATE, DELETE

### buffer_pool.rs
Buffer manager microbenchmarks across 5 phases:
- **Phase 1:** Core latency (pin/unpin hit, cold pin, dirty eviction)
- **Phase 2:** Access patterns (sequential, repeated, random, zipfian) - single + multi-threaded
- **Phase 3:** Pool size scaling, memory pressure
- **Phase 4:** Hit rate measurement
- **Phase 5:** Concurrent access, hotset contention, starvation  
  _Case tokens:_ `pin:t2`, `pin:t4`, `pin:t8`, `hotset:t4_k4`, `hotset:t8_k4`

## JSON Output for CI

Both benchmarks support `--json` flag for machine-readable output:

```bash
cargo bench --bench buffer_pool -- 50 12 --json
# Output: [{"name":"Pin/Unpin (hit)","unit":"ns","value":583},...]
```

**Important:** Filters are **ignored** in JSON mode. All benchmarks always run and output when `--json` is specified, regardless of filter argument.

**Why?** JSON mode is for CI only. CI needs complete historical data across all benchmarks for trend tracking. Filtering would create gaps in the time-series data displayed at the GitHub Pages dashboard.

```bash
# These produce identical output (all benchmarks):
cargo bench --bench buffer_pool -- 50 12 --json
cargo bench --bench buffer_pool -- 50 12 --json "Random"
```

## CI Integration

**Files:**
- `.github/workflows/benchmark.yml` - CI workflow
- Performance tracked at: https://redixhumayun.github.io/simpledb/dev/bench/

**How it works:**
1. CI runs benchmarks with `--json` flag on every push
2. JSON output captured and stored via `github-action-benchmark`
3. Historical data committed to `gh-pages` branch
4. Charts rendered at GitHub Pages URL

**View graphs:** Click individual benchmark names in the dashboard to see trends over time.

## Implementation

**Framework:** `src/benchmark_framework.rs`
- `parse_bench_args()` - CLI arg parsing (iterations, num_buffers, json flag, filter string)
- `should_run()` - Substring matching for filtering
- `benchmark()` - Timing harness with warmup, mean, median, stddev

**Pattern:** Each benchmark conditionally executes based on filter:
```rust
if should_run("benchmark_name", filter_ref) {
    results.push(run_benchmark(...));
}
```
