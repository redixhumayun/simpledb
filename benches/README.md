# SimpleDB Benchmarks

This directory contains the stdlib-only benchmarking framework for SimpleDB.

## Usage

Run benchmarks with default iterations (10):

```bash
cargo run --bin simple_bench
```

Or specify custom iteration count:

```bash
cargo run --bin simple_bench 50    # 50 iterations per operation
cargo run --bin simple_bench 100   # 100 iterations per operation
```

## What's Benchmarked

The benchmark suite measures performance of core database operations:

- **INSERT**: Single row insertion with transaction commit
- **SELECT**: Table scan and query execution  
- **UPDATE**: Single record modification
- **DELETE**: Single record removal

## Output Format

```
SimpleDB Stdlib-Only Benchmark Suite
====================================
Running benchmarks with 50 iterations per operation

Operation            |       Mean |     Median |     StdDev |    Iters
----------------------------------------------------------------------
INSERT (empty table) |     7.22ms |     7.16ms |   229.59µs |       50
SELECT (table scan)  |     2.76ms |     2.75ms |    42.68µs |       50
```

## Buffer Pool Benchmarks

These are implemented as a Cargo bench target (`benches/buffer_pool.rs`). Per `Cargo.toml`, run it with:

```bash
cargo bench --bench buffer_pool
```

You can pass arguments to control iterations and buffer pool size (parsed by the benchmark itself):

```bash
# Syntax: cargo bench --bench buffer_pool -- <iterations> <num_buffers>
cargo bench --bench buffer_pool -- 50 32
cargo bench --bench buffer_pool -- 100 128
```

Notes:
- **iterations**: number of timing samples per microbenchmark (default 10)
- **num_buffers**: buffer pool size in frames (default 12)
- Output is printed directly by the benchmark (harness is disabled), covering:
  - Phase 1: core latency (pin/unpin hit, cold pin, dirty eviction)
  - Phase 2: access patterns (sequential, repeated, random K, zipfian)
  - Phase 3: pool size scaling + memory pressure
  - Phase 4: hit-rate stats
  - Phase 5: concurrent access and starvation