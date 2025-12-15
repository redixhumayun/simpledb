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
|Pin/Unpin hit|**0.274 µs**|2.049 µs|1.280 µs|
|Cold pin|**2.403 µs**|6.369 µs|11.944 µs|
|Sequential Scan|**0.336 M blocks/s (0 % hits)**|0.264 M blocks/s (0 % hits)|0.202 M blocks/s (0 % hits)|
|Seq Scan MT x4|**0.246 M blocks/s**|0.200 M blocks/s|0.243 M blocks/s|
|Seq Scan MT x16|0.120 M blocks/s|**0.126 M blocks/s**|0.117 M blocks/s|
|Repeated Access|3.530 M ops/s (100 % hits)|3.726 M ops/s (100 % hits)|**3.827 M ops/s (100 % hits)**|
|Repeated Access MT x4|0.878 M ops/s|**0.952 M ops/s**|0.926 M ops/s|
|Repeated Access MT x16|0.294 M ops/s|**0.525 M ops/s**|0.511 M ops/s|
|Random K=10|3.495 M ops/s (100 % hits)|**3.821 M ops/s (100 % hits)**|3.790 M ops/s (100 % hits)|
|Random MT x4 K=10|0.853 M ops/s|0.909 M ops/s|**0.965 M ops/s**|
|Random MT x16 K=10|0.281 M ops/s|0.543 M ops/s|**0.564 M ops/s**|
|Random K=50|0.641 M ops/s (28 % hits)|**0.641 M ops/s (24 % hits)**|0.631 M ops/s (19 % hits)|
|Random MT x4 K=50|0.320 M ops/s|0.258 M ops/s|**0.325 M ops/s**|
|Random MT x16 K=50|0.139 M ops/s|**0.192 M ops/s**|0.172 M ops/s|
|Random K=100|**0.564 M ops/s (12 % hits)**|0.545 M ops/s (12 % hits)|0.562 M ops/s (8 % hits)|
|Random MT x4 K=100|0.288 M ops/s|0.224 M ops/s|**0.294 M ops/s**|
|Random MT x16 K=100|0.131 M ops/s|**0.161 M ops/s**|0.138 M ops/s|
|Zipfian|**1.680 M ops/s (74 % hits)**|1.661 M ops/s (73 % hits)|1.285 M ops/s (71 % hits)|
|Zipfian MT x4|0.524 M ops/s|**0.618 M ops/s**|0.534 M ops/s|
|Zipfian MT x16|0.219 M ops/s|**0.371 M ops/s**|0.314 M ops/s|
|pin:t1|3.360 M ops/s|3.591 M ops/s|**3.616 M ops/s**|
|pin:t2|1.271 M ops/s|**1.516 M ops/s**|1.118 M ops/s|
|pin:t8|**0.254 M ops/s**|0.186 M ops/s|0.246 M ops/s|
|pin:t16|0.112 M ops/s|**0.158 M ops/s**|0.111 M ops/s|
|pin:t64|0.062 M ops/s|**0.149 M ops/s**|0.057 M ops/s|
|pin:t256|0.032 M ops/s|**0.136 M ops/s**|0.026 M ops/s|
|hotset:t1_k4|3.391 M ops/s|3.657 M ops/s|**3.659 M ops/s**|
|hotset:t2_k4|1.585 M ops/s|1.746 M ops/s|**1.751 M ops/s**|
|hotset:t8_k4|0.476 M ops/s|**0.683 M ops/s**|0.662 M ops/s|
|hotset:t16_k4|0.311 M ops/s|**0.523 M ops/s**|0.512 M ops/s|
|hotset:t64_k4|0.232 M ops/s|**0.406 M ops/s**|0.401 M ops/s|
|hotset:t256_k4|0.210 M ops/s|**0.380 M ops/s**|0.365 M ops/s|

### Linux (i7-8650U, Ubuntu 6.8.0-86)
|Benchmark (Phase)|Replacement LRU (4KB pages)|Replacement Clock (4KB pages)|Replacement SIEVE (4KB pages)|
|---|---|---|---|
|Pin/Unpin hit|0.809 µs|**0.808 µs**|0.810 µs|
|Cold pin|4.688 µs|**4.033 µs**|4.071 µs|
|Sequential Scan|**0.267 M blocks/s (0 % hits)**|0.248 M blocks/s (0 % hits)|0.267 M blocks/s (0 % hits)|
|Seq Scan MT x4|0.184 M blocks/s|0.181 M blocks/s|**0.186 M blocks/s**|
|Seq Scan MT x16|**0.135 M blocks/s**|0.121 M blocks/s|0.129 M blocks/s|
|Repeated Access|1.180 M ops/s (100 % hits)|1.130 M ops/s (100 % hits)|**1.190 M ops/s (100 % hits)**|
|Repeated Access MT x4|**1.202 M ops/s**|1.196 M ops/s|1.194 M ops/s|
|Repeated Access MT x16|**0.844 M ops/s**|0.808 M ops/s|0.771 M ops/s|
|Random K=10|**1.198 M ops/s (100 % hits)**|1.069 M ops/s (100 % hits)|1.042 M ops/s (100 % hits)|
|Random MT x4 K=10|1.123 M ops/s|**1.176 M ops/s**|1.155 M ops/s|
|Random MT x16 K=10|0.819 M ops/s|0.815 M ops/s|**0.827 M ops/s**|
|Random K=50|**0.321 M ops/s (19 % hits)**|0.276 M ops/s (22 % hits)|0.306 M ops/s (23 % hits)|
|Random MT x4 K=50|**0.248 M ops/s**|0.219 M ops/s|0.233 M ops/s|
|Random MT x16 K=50|0.165 M ops/s|**0.174 M ops/s**|0.149 M ops/s|
|Random K=100|**0.291 M ops/s (11 % hits)**|0.260 M ops/s (13 % hits)|0.258 M ops/s (11 % hits)|
|Random MT x4 K=100|**0.219 M ops/s**|0.196 M ops/s|0.202 M ops/s|
|Random MT x16 K=100|**0.142 M ops/s**|0.142 M ops/s|0.121 M ops/s|
|Zipfian|**0.661 M ops/s (79 % hits)**|0.599 M ops/s (74 % hits)|0.497 M ops/s (71 % hits)|
|Zipfian MT x4|0.679 M ops/s|**0.724 M ops/s**|0.578 M ops/s|
|Zipfian MT x16|0.460 M ops/s|**0.462 M ops/s**|0.352 M ops/s|
|pin:t1|1.111 M ops/s|**1.167 M ops/s**|1.158 M ops/s|
|pin:t2|0.234 M ops/s|**0.251 M ops/s**|0.242 M ops/s|
|pin:t8|**0.176 M ops/s**|0.122 M ops/s|0.167 M ops/s|
|pin:t16|0.099 M ops/s|**0.122 M ops/s**|0.098 M ops/s|
|pin:t64|0.081 M ops/s|**0.127 M ops/s**|0.068 M ops/s|
|pin:t256|0.057 M ops/s|**0.113 M ops/s**|0.041 M ops/s|
|hotset:t1_k4|1.074 M ops/s|**1.181 M ops/s**|1.131 M ops/s|
|hotset:t2_k4|1.210 M ops/s|**1.390 M ops/s**|1.330 M ops/s|
|hotset:t8_k4|0.825 M ops/s|**0.885 M ops/s**|0.883 M ops/s|
|hotset:t16_k4|0.777 M ops/s|0.908 M ops/s|**0.911 M ops/s**|
|hotset:t64_k4|0.765 M ops/s|**0.981 M ops/s**|0.976 M ops/s|
|hotset:t256_k4|0.678 M ops/s|0.716 M ops/s|**0.726 M ops/s**|

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
