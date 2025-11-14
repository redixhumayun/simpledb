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
|Benchmark (Phase)|Master (first-unpinned)|Replacement LRU|Replacement Clock|Replacement SIEVE|
|---|---|---|---|---|
|Pin/Unpin hit latency (P1)|**0.319 µs**|0.918 µs|0.973 µs|1.32 µs|
|Cold pin latency (P1)|**4.95 µs**|7.03 µs|7.38 µs|10.14 µs|
|Sequential Scan throughput (P2)|0.159 M blocks/s (0 % hits)|0.158 M blocks/s (0 % hits)|0.251 M blocks/s (0 % hits)|**0.282 M blocks/s (0 % hits)**|
|Sequential Scan MT throughput (P2)|0.150 M blocks/s|0.129 M blocks/s|0.150 M blocks/s|**0.172 M blocks/s**|
|Repeated Access throughput (P2)|0.302 M ops/s (0 % hits)|3.531 M ops/s (100 % hits)|3.792 M ops/s (100 % hits)|**3.799 M ops/s (100 % hits)**|
|Repeated Access MT throughput (P2)|0.267 M ops/s|0.948 M ops/s|1.041 M ops/s|**1.068 M ops/s**|
|Random K=10 throughput (P2)|0.316 M ops/s (10 % hits)|3.500 M ops/s (100 % hits)|**3.798 M ops/s (100 % hits)**|3.773 M ops/s (100 % hits)|
|Random K=50 throughput (P2)|0.302 M ops/s (2 % hits)|0.571 M ops/s (23 % hits)|**0.601 M ops/s (26 % hits)**|0.581 M ops/s (21 % hits)|
|Random K=100 throughput (P2)|0.299 M ops/s (2 % hits)|0.462 M ops/s (15 % hits)|**0.530 M ops/s (10 % hits)**|0.499 M ops/s (13 % hits)|
|Zipfian throughput (P2)|0.324 M ops/s (9 % hits)|**1.741 M ops/s (82 % hits)**|1.532 M ops/s (76 % hits)|1.261 M ops/s (70 % hits)|
|Zipfian MT throughput (P2)|0.276 M ops/s|0.487 M ops/s|0.544 M ops/s|**0.547 M ops/s**|
|Multi-thread pin:t2 (P5)|0.29 M ops/s|1.34 M ops/s|**1.50 M ops/s**|1.29 M ops/s|
|Multi-thread pin:t8 (P5)|0.11 M ops/s|0.19 M ops/s|0.16 M ops/s|**0.19 M ops/s**|
|Multi-thread pin:t16 (P5)|—|0.10 M ops/s|**0.14 M ops/s**|0.10 M ops/s|
|Hot-set t8_k4 (P5)|0.68 M ops/s|0.50 M ops/s|0.70 M ops/s|**0.71 M ops/s**|
|Hot-set t16_k4 (P5)|—|0.33 M ops/s|0.54 M ops/s|**0.55 M ops/s**|

### Linux (i7-8650U, Ubuntu 6.8.0-86)
|Benchmark (Phase)|Master (first-unpinned)|Replacement LRU|Replacement Clock|Replacement SIEVE|
|---|---|---|---|---|
|Pin/Unpin hit latency (P1)|0.829 µs|1.09 µs|**0.800 µs**|1.06 µs|
|Cold pin latency (P1)|6.41 µs|4.49 µs|4.11 µs|**4.05 µs**|
|Sequential Scan throughput (P2)|0.163 M blocks/s (0 % hits)|**0.255 M blocks/s (0 % hits)**|0.253 M blocks/s (0 % hits)|0.250 M blocks/s (0 % hits)|
|Sequential Scan MT throughput (P2)|0.121 M blocks/s|**0.179 M blocks/s**|0.171 M blocks/s|0.178 M blocks/s|
|Repeated Access throughput (P2)|0.162 M ops/s (0 % hits)|1.180 M ops/s (100 % hits)|1.189 M ops/s (100 % hits)|**1.224 M ops/s (100 % hits)**|
|Repeated Access MT throughput (P2)|0.175 M ops/s|1.212 M ops/s|**1.260 M ops/s**|1.253 M ops/s|
|Random K=10 throughput (P2)|0.175 M ops/s (10 % hits)|1.201 M ops/s (100 % hits)|1.138 M ops/s (100 % hits)|**1.204 M ops/s (100 % hits)**|
|Random K=50 throughput (P2)|0.165 M ops/s (3 % hits)|**0.308 M ops/s (22 % hits)**|0.297 M ops/s (21 % hits)|0.300 M ops/s (20 % hits)|
|Random K=100 throughput (P2)|0.163 M ops/s (1 % hits)|**0.279 M ops/s (13 % hits)**|0.255 M ops/s (12 % hits)|0.276 M ops/s (12 % hits)|
|Zipfian throughput (P2)|0.175 M ops/s (9 % hits)|**0.756 M ops/s (76 % hits)**|0.566 M ops/s (77 % hits)|0.537 M ops/s (68 % hits)|
|Zipfian MT throughput (P2)|0.174 M ops/s|**0.753 M ops/s**|0.611 M ops/s|0.557 M ops/s|
|Multi-thread pin:t2 (P5)|0.14 M ops/s|**0.22 M ops/s**|0.21 M ops/s|0.22 M ops/s|
|Multi-thread pin:t8 (P5)|0.13 M ops/s|**0.18 M ops/s**|0.13 M ops/s|0.18 M ops/s|
|Multi-thread pin:t16 (P5)|—|0.12 M ops/s|**0.12 M ops/s**|0.10 M ops/s|
|Hot-set t8_k4 (P5)|**1.00 M ops/s**|0.81 M ops/s|0.86 M ops/s|0.86 M ops/s|
|Hot-set t16_k4 (P5)|—|0.76 M ops/s|**0.91 M ops/s**|0.90 M ops/s|

_Notes_:  
- Times are means from Phase 1 latency benches. Throughputs are means from Phase 2 (Repeated/Random) and Phase 5 (pin:t2/pin:t8 plus the new pin:t16/hot-set:t16 oversubscription cases).  
- `—` indicates the master (first-unpinned) policy has not been rerun yet with the new oversubscription cases.
- Clock shows higher hit-path latency on macOS due to the extra hand mutex; Linux latency stays near parity with LRU/master.
- Phase 3 (pool/memory scaling) is not summarized here—see the raw log files for those details.

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
