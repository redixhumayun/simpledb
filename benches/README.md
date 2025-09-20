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