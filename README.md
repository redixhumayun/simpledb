## Overview
SimpleDB is a Rust port of the Java implementation by Edward Sciore. You can read about the Java implementation in Sciore's book, [Database Design & Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7).

This port is mainly for pedagagical and experimentation reasons. I wanted to understand how query engines worked in more detail and I also wanted a playground to experiment with different ideas.

## Dependency Policy

Core engine paths are intended to remain dependency-free beyond the Rust standard library.

Allowed exceptions:
- platform bindings that expose OS primitives not available in `std` (for example `libc`)
- non-engine tooling dependencies (for example benchmarking with `criterion`, or CLI parsing with `clap`)

Third-party crates on the critical execution path (read/write/transaction engine internals) require explicit design rationale.

## Usage

Run the CLI:
```bash
cargo run --bin simpledb
```

Example commands:
```sql
CREATE TABLE USERS(id int, name varchar(50))
INSERT INTO USERS(id, name) VALUES (1, 'Alice')
SELECT * FROM USERS
```

### Core Features

The database supports ACID transactions, along with some other niceties like
* A buffer pool to manage memory
* A WAL to ensure durability
* A catalog to manage metadata for all tables
* A query engine with a simple optimizer

### Benchmarks

Look at the [benchmarks README](benches/README.md) for more details.

Run an individual buffer-pool workload (useful for profiling):
```bash
cargo bench --bench buffer_pool -- 100 12 --filter pin:t8   # Only the 8-thread pin benchmark
```

#### CI Benchmark Tracking

All pushes to master and PRs automatically run benchmarks via [Bencher](https://bencher.dev):
- Runs `buffer_pool`, `io_patterns`, and `simple_bench` suites
- Results tracked historically per branch on the [Bencher dashboard](https://bencher.dev/perf/simpledb)
- PRs are compared against the master baseline using a t-test (95% confidence)
- Regression alerts posted as PR comments when performance degrades

**Adding new benchmarks:** Just create a file in `benches/`:
```bash
# Create your benchmark
touch benches/io_benchmark.rs

# Add [[bench]] entry to Cargo.toml
[[bench]]
name = "io_benchmark"
harness = false

# Implement using Criterion — CI automatically picks it up
```

### Roadmap

See the full project roadmap in [docs/ROADMAP.md](docs/ROADMAP.md).

### WAL Documentation

See [docs/WAL.md](docs/WAL.md) for WAL architecture, recovery model, invariants, tradeoffs, and current gaps.
