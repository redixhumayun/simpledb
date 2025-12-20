# SimpleDB Benchmarks

Stdlib-only benchmarking framework with CLI filtering support.

## Hardware

All benchmarks reference the following machines.

### Linux runner (Dell XPS 13‑9370, Netac NVMe, Ubuntu 6.8.0‑86)

- **CPU / Memory**: Intel i7‑8650U (4C/8T), 16 GB DDR4‑2133
- **Storage**: Netac 512 GB NVMe (PCIe)
- **fio (direct I/O)**  
  - Sequential write, 1 MiB: **1.27 GiB/s · 1.27 k IOPS**  
  - Sequential read, 1 MiB: **2.97 GiB/s · 2.97 k IOPS**  
  - Random write, 4 KiB: **326 MiB/s · 83.5 k IOPS**  
  - Random read, 4 KiB: **426 MiB/s · 109 k IOPS** 
  - Random write, 4 KiB + per-op `fdatasync`: **≈0.80 MiB/s · ≈200 IOPS**

  ```bash
  fio --name=seqwrite --filename=fiotest.bin --size=4G --bs=1M --rw=write --direct=1 --ioengine=libaio --numjobs=1 --iodepth=16
  fio --name=seqread  --filename=fiotest.bin --size=4G --bs=1M --rw=read  --direct=1 --ioengine=libaio --numjobs=1 --iodepth=16
  fio --name=randwrite --filename=fiotest.bin --size=4G --bs=4k --rw=randwrite --direct=1 --ioengine=libaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randread  --filename=fiotest.bin --size=4G --bs=4k --rw=randread  --direct=1 --ioengine=libaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randwrite_fsync --filename=fiotest.bin --size=2G --bs=4k --rw=randwrite --direct=1 --ioengine=libaio --iodepth=1 --fsync=1 --time_based --runtime=60
  ```

### macOS runner (MacBook Pro 14" 2021, M1 Pro, Apple SSD AP0512R, macOS Sequoia)

- **CPU / Memory**: Apple M1 Pro (6P+2E cores), 16 GB unified
- **Storage**: Apple SSD AP0512R 512 GB (Apple Fabric NVMe)
- **fio (direct I/O)**  
  - Sequential write, 1 MiB: **4.80 GiB/s · 4.8 k IOPS**  
  - Sequential read, 1 MiB: **6.45 GiB/s · 6.4 k IOPS**  
  - Random write, 4 KiB: **75.7 MiB/s · 19.4 k IOPS**  
  - Random read, 4 KiB: **165 MiB/s · 42.1 k IOPS**  
  - Random write, 4 KiB + per-op `F_FULLFSYNC`: **≈27.9 MiB/s · ≈7.1 k IOPS**

  ```bash
  fio --name=seqwrite --filename=/tmp/fiotest.bin --size=4G --bs=1M --rw=write --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=16
  fio --name=seqread  --filename=/tmp/fiotest.bin --size=4G --bs=1M --rw=read  --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=16
  fio --name=randwrite --filename=/tmp/fiotest.bin --size=4G --bs=4k --rw=randwrite --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randread  --filename=/tmp/fiotest.bin --size=4G --bs=4k --rw=randread  --direct=1 --ioengine=posixaio --numjobs=1 --iodepth=32 --runtime=60 --time_based
  fio --name=randwrite_fsync_full --filename=/tmp/fiotest.bin --size=2G --bs=4k --rw=randwrite --direct=1 --ioengine=posixaio --iodepth=1 --fsync=1 --time_based --runtime=60
  ```

I'm not sure of the below explanation. It's a conclusion I arrived at while chatting with LLM's.

> **Note:** The per-operation durability test shows a massive gap (≈200 IOPS vs ≈7 k IOPS). Apple’s controller acknowledges `F_FULLFSYNC` after staging data in a capacitor-backed, power-loss-protected cache; the Netac NVMe must program TLC NAND immediately on each `fdatasync`, incurring ~5 ms per flush. This is purely a hardware/firmware difference.

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

## Replacement Policy Summary

All runs use `cargo bench --bench buffer_pool -- 100 12` (pool=12, block=4 KiB). Raw logs live in `docs/benchmarks/replacement_policies/…`.

### macOS (M1 Pro, macOS Sequoia)
|Benchmark (Phase)|Replacement LRU (4KB pages)|Replacement Clock (4KB pages)|Replacement SIEVE (4KB pages)|
|---|---|---|---|
|Pin/Unpin hit|**0.506 µs**|0.555 µs|0.622 µs|
|Cold pin|6.673 µs|**5.677 µs**|8.456 µs|
|Sequential Scan|—|—|—|
|Seq Scan MT x4|—|—|—|
|Seq Scan MT x16|—|—|—|
|Repeated Access|5.278 M ops/s (100 % hits)|**6.402 M ops/s (100 % hits)**|5.988 M ops/s (100 % hits)|
|Repeated Access MT x4|1.362 M ops/s|**3.006 M ops/s**|2.734 M ops/s|
|Repeated Access MT x16|0.352 M ops/s|**1.783 M ops/s**|1.729 M ops/s|
|Random K=10|4.930 M ops/s (100 % hits)|5.588 M ops/s (100 % hits)|**5.665 M ops/s (100 % hits)**|
|Random MT x4 K=10|1.299 M ops/s|2.604 M ops/s|**2.720 M ops/s**|
|Random MT x16 K=10|0.332 M ops/s|**1.660 M ops/s**|1.593 M ops/s|
|Random K=50|4.829 M ops/s (100 % hits)|5.538 M ops/s (100 % hits)|**5.551 M ops/s (100 % hits)**|
|Random MT x4 K=50|0.849 M ops/s|3.044 M ops/s|**3.090 M ops/s**|
|Random MT x16 K=50|0.301 M ops/s|**2.017 M ops/s**|2.012 M ops/s|
|Random K=100|5.166 M ops/s (100 % hits)|5.942 M ops/s (100 % hits)|**5.992 M ops/s (100 % hits)**|
|Random MT x4 K=100|0.831 M ops/s|**3.102 M ops/s**|3.059 M ops/s|
|Random MT x16 K=100|0.287 M ops/s|**2.171 M ops/s**|2.069 M ops/s|
|Zipfian|5.273 M ops/s (99 % hits)|5.888 M ops/s (99 % hits)|**6.102 M ops/s (99 % hits)**|
|Zipfian MT x4|0.879 M ops/s|**3.303 M ops/s**|3.112 M ops/s|
|Zipfian MT x16|0.293 M ops/s|**2.127 M ops/s**|2.050 M ops/s|
|pin:t1|5.165 M ops/s|5.925 M ops/s|**5.933 M ops/s**|
|pin:t2|2.762 M ops/s|**8.939 M ops/s**|7.615 M ops/s|
|pin:t8|0.739 M ops/s|**4.718 M ops/s**|4.715 M ops/s|
|pin:t16|0.452 M ops/s|**4.413 M ops/s**|4.359 M ops/s|
|pin:t64|0.226 M ops/s|**4.126 M ops/s**|4.081 M ops/s|
|pin:t256|0.195 M ops/s|**2.878 M ops/s**|2.850 M ops/s|
|hotset:t1_k4|4.444 M ops/s|**5.074 M ops/s**|5.009 M ops/s|
|hotset:t2_k4|2.342 M ops/s|**5.698 M ops/s**|5.593 M ops/s|
|hotset:t8_k4|0.628 M ops/s|1.695 M ops/s|**1.703 M ops/s**|
|hotset:t16_k4|0.388 M ops/s|**1.346 M ops/s**|1.292 M ops/s|
|hotset:t64_k4|0.259 M ops/s|**1.105 M ops/s**|1.098 M ops/s|
|hotset:t256_k4|0.238 M ops/s|**0.965 M ops/s**|0.939 M ops/s|

### Linux (i7-8650U, Ubuntu 6.8.0-86)
|Benchmark (Phase)|Replacement LRU (4KB pages)|Replacement Clock (4KB pages)|Replacement SIEVE (4KB pages)|
|---|---|---|---|
|Pin/Unpin hit|0.725 µs|**0.710 µs**|**0.710 µs**|
|Cold pin|4.600 µs|**4.134 µs**|4.177 µs|
|Sequential Scan|—|—|—|
|Seq Scan MT x4|—|—|—|
|Seq Scan MT x16|—|—|—|
|Repeated Access|1.246 M ops/s (100 % hits)|**1.384 M ops/s (100 % hits)**|1.358 M ops/s (100 % hits)|
|Repeated Access MT x4|0.728 M ops/s|**3.535 M ops/s**|3.442 M ops/s|
|Repeated Access MT x16|0.485 M ops/s|**2.855 M ops/s**|2.851 M ops/s|
|Random K=10|1.061 M ops/s (100 % hits)|1.162 M ops/s (100 % hits)|**1.166 M ops/s (100 % hits)**|
|Random MT x4 K=10|0.694 M ops/s|**2.633 M ops/s**|2.152 M ops/s|
|Random MT x16 K=10|0.512 M ops/s|**2.201 M ops/s**|2.189 M ops/s|
|Random K=50|1.157 M ops/s (100 % hits)|1.266 M ops/s (100 % hits)|**1.275 M ops/s (100 % hits)**|
|Random MT x4 K=50|0.652 M ops/s|2.228 M ops/s|**2.272 M ops/s**|
|Random MT x16 K=50|0.504 M ops/s|**2.227 M ops/s**|2.200 M ops/s|
|Random K=100|1.233 M ops/s (100 % hits)|**1.322 M ops/s (100 % hits)**|1.310 M ops/s (100 % hits)|
|Random MT x4 K=100|0.614 M ops/s|2.475 M ops/s|**3.240 M ops/s**|
|Random MT x16 K=100|0.530 M ops/s|**2.205 M ops/s**|2.197 M ops/s|
|Zipfian|1.149 M ops/s (99 % hits)|**1.241 M ops/s (99 % hits)**|1.228 M ops/s (99 % hits)|
|Zipfian MT x4|0.706 M ops/s|3.347 M ops/s|**3.702 M ops/s**|
|Zipfian MT x16|0.663 M ops/s|**2.373 M ops/s**|2.366 M ops/s|
|pin:t1|1.221 M ops/s|1.302 M ops/s|**1.307 M ops/s**|
|pin:t2|1.296 M ops/s|**2.324 M ops/s**|2.195 M ops/s|
|pin:t8|0.665 M ops/s|3.558 M ops/s|**3.593 M ops/s**|
|pin:t16|0.633 M ops/s|3.603 M ops/s|**4.185 M ops/s**|
|pin:t64|0.607 M ops/s|3.137 M ops/s|**3.629 M ops/s**|
|pin:t256|0.541 M ops/s|2.113 M ops/s|**2.407 M ops/s**|
|hotset:t1_k4|1.212 M ops/s|1.266 M ops/s|**1.287 M ops/s**|
|hotset:t2_k4|1.384 M ops/s|**2.041 M ops/s**|1.997 M ops/s|
|hotset:t8_k4|0.617 M ops/s|3.127 M ops/s|**3.439 M ops/s**|
|hotset:t16_k4|0.569 M ops/s|2.738 M ops/s|**3.283 M ops/s**|
|hotset:t64_k4|0.539 M ops/s|2.470 M ops/s|**2.938 M ops/s**|
|hotset:t256_k4|0.505 M ops/s|1.818 M ops/s|**2.156 M ops/s**|

_Notes_:  
- Times are means from Phase 1 latency benches. Throughputs are means from Phase 2 (Repeated/Random) and Phase 5 (pin:t2/pin:t8 plus the new pin:t16/hot-set:t16 oversubscription cases).  
- Phase 3 (pool/memory scaling) is not summarized here—see the raw log files for those details.

### Updating Replacement-Policy Data

To refresh the raw benchmark logs and the summary tables above:

1. Run the benchmark collector on each platform (defaults fill in title/env automatically):
   ```bash
   python3 scripts/bench/run_buffer_pool.py --platform macos --iterations 100 --num-buffers 12
   python3 scripts/bench/run_buffer_pool.py --platform linux --iterations 100 --num-buffers 12
   ```
   Artifacts land in `docs/benchmarks/replacement_policies/raw/<platform>/`.

2. Rebuild the docs/tables from those artifacts:
   ```bash
   python3 scripts/bench/render_replacement_policy_docs.py --platforms macos linux
   ```

Step (1) captures both the JSON payload (for table generation) and the exact `cargo bench` log (for the per-platform markdown files). 

Step (2) rewrites:
- `docs/benchmarks/replacement_policies/{platform}_buffer_pool.md`
- The macOS/Linux tables in this README

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
  _Case tokens:_ `pin:t2`, `pin:t4`, `pin:t8`, `pin:t16`, `hotset:t4_k4`, `hotset:t8_k4`, `hotset:t16_k4`

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
