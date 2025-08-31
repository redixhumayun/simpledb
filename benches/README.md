# SimpleDB Benchmarks

This directory contains the stdlib-only benchmarking framework for SimpleDB.

## Usage

Run benchmarks with:

```bash
cargo run --bin simple_bench
```

## What's Benchmarked

The benchmark suite measures performance of core database operations:

- **INSERT**: Single row insertion with transaction commit
- **SELECT**: Table scan and query execution  
- **UPDATE**: Single record modification
- **DELETE**: Single record removal

## Output Format

```
Operation            |       Mean |     Median |     StdDev |    Iters
----------------------------------------------------------------------
INSERT (empty table) |     7.68ms |     7.32ms |   996.90µs |       10
SELECT (table scan)  |     2.75ms |     2.74ms |     8.35µs |       10
```

## Implementation

- **Framework**: `benches/benchmark_framework.rs` - Timing and statistics calculation
- **Binary**: `src/bin/simple_bench.rs` - Main benchmark executable  
- **Dependencies**: None - uses only Rust stdlib
- **Statistics**: Mean, median, and standard deviation calculated manually

## Design Principles

- **Zero dependencies**: Maintains SimpleDB's stdlib-only philosophy
- **Educational value**: Simple, understandable benchmark implementation
- **Accurate timing**: Uses `std::time::Instant` with statistical analysis
- **Real workloads**: Tests actual SQL operations through the query planner