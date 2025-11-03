# SimpleDB Benchmarks

Stdlib-only benchmarking framework with CLI filtering support.

## Hardware

All benchmarks are run on the following hardware

- **Platform**: Dell XPS 13-9370, Intel i7-8650U (4C/8T, 1 socket), 16 GB DDR4-2133, Netac NVMe SSD 512 GB (PCIe), Ubuntu Linux 6.8.0-86-generic.
- **Storage capability (fio, direct I/O)**:
  - Sequential write (1 MiB blocks): 1.27 GiB/s, 1.27k IOPS.
  - Sequential read (1 MiB blocks): 2.97 GiB/s, 2.97k IOPS.
  - Random write (4 KiB blocks): 83.5k IOPS, 326 MiB/s.
  - Random read (4 KiB blocks): 109k IOPS, 426 MiB/s.

  Commands (run from a scratch directory with ~4 GiB free):

  ```bash
  fio --name=seqwrite --filename=fiotest.bin --size=4G --bs=1M --rw=write --direct=1 --ioengine=libaio --numjobs=1 --iodepth=16
  fio --name=seqread  --filename=fiotest.bin --size=4G --bs=1M --rw=read  --direct=1 --ioengine=libaio --numjobs=1 --iodepth=16
  fio --name=randwrite --filename=fiotest.bin --size=4G --bs=4k --rw=randwrite --direct=1 --ioengine=libaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randread  --filename=fiotest.bin --size=4G --bs=4k --rw=randread  --direct=1 --ioengine=libaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  ```

- **Platform**: MacBook Pro (14-inch, 2021) M1 Pro (6P+2E cores), 16 GB unified memory, Apple SSD AP0512R 512 GB, macOS Sequoia.
- **Storage capability (fio, direct I/O)**:
  - Sequential write (1 MiB blocks): 4.80 GiB/s, 4.8k IOPS.
  - Sequential read (1 MiB blocks): 6.45 GiB/s, 6.4k IOPS.
  - Random write (4 KiB blocks): 19.4k IOPS, 75.7 MiB/s.
  - Random read (4 KiB blocks): 42.1k IOPS, 165 MiB/s.

  Commands (run from a scratch directory with ≥4 GiB free):

  ```bash
  fio --name=seqwrite --filename=/tmp/fiotest.bin --size=4G --bs=1M --rw=write --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=16
  fio --name=seqread  --filename=/tmp/fiotest.bin --size=4G --bs=1M --rw=read  --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=16
  fio --name=randwrite --filename=/tmp/fiotest.bin --size=4G --bs=4k --rw=randwrite --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randread  --filename=/tmp/fiotest.bin --size=4G --bs=4k --rw=randread  --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  ```

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
