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

| Benchmark (Phase)                  | Master (first-unpinned) | Replacement LRU | Replacement Clock |
|------------------------------------|-------------------------|-----------------|-------------------|
| Pin/Unpin hit latency (P1)         | 0.319 µs                | **0.290 µs**    | **0.272 µs**      |
| Cold pin latency (P1)              | 4.95 µs                 | **2.61 µs**     | **2.26 µs**       |
| Sequential Scan throughput (P2)    | 0.159 M blocks/s (0 % hits) | **0.242 M blocks/s (0 %)** | **0.329 M blocks/s (0 %)** |
| Sequential Scan MT throughput (P2) | 0.150 M blocks/s        | **0.191 M blocks/s** | 0.145 M blocks/s  |
| Repeated Access throughput (P2)    | 0.30 M ops/s (0 % hits) | **3.56 M ops/s (100 %)** | **3.81 M ops/s (100 %)** |
| Repeated Access MT throughput (P2) | 0.27 M ops/s            | **0.96 M ops/s**| **1.06 M ops/s**  |
| Random K=10 throughput (P2)        | 0.32 M ops/s (10 % hits) | **3.50 M ops/s (100 %)** | **3.82 M ops/s (100 %)** |
| Random K=50 throughput (P2)        | 0.30 M ops/s (2 % hits) | **0.62 M ops/s (22 %)**| **0.60 M ops/s (27 %)**  |
| Random K=100 throughput (P2)       | 0.30 M ops/s (2 % hits) | **0.54 M ops/s (14 %)**| **0.54 M ops/s (14 %)**  |
| Zipfian throughput (P2)            | 0.324 M ops/s (9 % hits) | **1.51 M ops/s (77 %)** | **1.50 M ops/s (76 %)** |
| Zipfian MT throughput (P2)         | 0.276 M ops/s           | **0.55 M ops/s**| **0.63 M ops/s**  |
| Multi-thread pin:t2 (P5)           | 0.29 M ops/s            | **1.25 M ops/s**| **1.47 M ops/s**  |
| Multi-thread pin:t8 (P5)           | 0.11 M ops/s            | **0.23 M ops/s**| 0.16 M ops/s      |

### Linux (i7-8650U, Ubuntu 6.8.0-86)

| Benchmark (Phase)                  | Master (first-unpinned) | Replacement LRU | Replacement Clock |
|------------------------------------|-------------------------|-----------------|-------------------|
| Pin/Unpin hit latency (P1)         | 0.829 µs                | **0.804 µs**    | **0.793 µs**      |
| Cold pin latency (P1)              | 6.41 µs                 | **4.11 µs**     | 4.57 µs           |
| Sequential Scan throughput (P2)    | 0.163 M blocks/s (0 % hits) | **0.251 M blocks/s (0 %)** | **0.255 M blocks/s (0 %)** |
| Sequential Scan MT throughput (P2) | 0.121 M blocks/s        | **0.182 M blocks/s** | 0.160 M blocks/s  |
| Repeated Access throughput (P2)    | 0.16 M ops/s (0 % hits) | **1.18 M ops/s (100 %)** | **1.25 M ops/s (100 %)** |
| Repeated Access MT throughput (P2) | 0.18 M ops/s            | **1.23 M ops/s**| **1.21 M ops/s**  |
| Random K=10 throughput (P2)        | 0.18 M ops/s (10 % hits) | **1.20 M ops/s (100 %)** | **1.25 M ops/s (100 %)** |
| Random K=50 throughput (P2)        | 0.17 M ops/s (3 % hits) | **0.31 M ops/s (23 %)**| **0.31 M ops/s (24 %)**  |
| Random K=100 throughput (P2)       | 0.16 M ops/s (1 % hits) | **0.27 M ops/s (10 %)**| **0.28 M ops/s (13 %)**  |
| Zipfian throughput (P2)            | 0.175 M ops/s (9 % hits) | **0.69 M ops/s (81 %)** | **0.67 M ops/s (76 %)** |
| Zipfian MT throughput (P2)         | 0.174 M ops/s           | **0.70 M ops/s**| **0.65 M ops/s**  |
| Multi-thread pin:t2 (P5)           | 0.15 M ops/s            | **0.22 M ops/s**| **0.22 M ops/s**  |
| Multi-thread pin:t8 (P5)           | 0.13 M ops/s            | **0.18 M ops/s**| 0.14 M ops/s      |

_Notes_:  
- Times are means from Phase 1 latency benches. Throughputs are means from Phase 2 (Repeated/Random) and Phase 5 (pin:t2/pin:t8).  
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
