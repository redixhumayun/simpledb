## Overview
SimpleDB is a Rust port of the Java implementation by Edward Sciore. You can read about the Java implementation in Sciore's book, [Database Design & Implementation](https://link.springer.com/book/10.1007/978-3-030-33836-7).

This port is mainly for pedagagical and experimentation reasons. I wanted to understand how query engines worked in more detail and I also wanted a playground to experiment with different ideas.

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
cargo bench --bench buffer_pool -- 100 12 pin:t8   # Only the 8-thread pin benchmark
```

#### CI Benchmark Tracking

All PRs automatically run **all benchmarks** via auto-discovery:
- Discovers and runs all cargo benchmarks in `benches/*.rs`
- Uses standard `cargo bench` for discovery (no manual lists needed!)
- Results stored historically in the `gh-pages` branch
- Compared against previous runs with 5% alert threshold
- Posted as PR comments when significant changes are detected (>5%)
- **Never block merges** - alerts are informational only

**Adding new benchmarks:** Just create a file in `benches/`:
```bash
# Create your benchmark
touch benches/io_benchmark.rs

# Add [[bench]] entry to Cargo.toml
[[bench]]
name = "io_benchmark"
harness = false

# Implement with --json flag support
# CI automatically discovers and runs it!
```

#### Performance Label

Add the `performance` label to your PR to:
- Generate a detailed base-vs-PR comparison report
- Get side-by-side performance metrics for ALL benchmarks in a PR comment

This is useful when:
- Implementing cache eviction algorithms (LRU, Clock, etc.)
- Modifying buffer manager code
- Making changes that could impact memory/disk I/O performance

#### JSON Output

Benchmarks support JSON output for CI integration:
```bash
# Run a specific benchmark with JSON
cargo bench --bench simple_bench -- 50 12 --json
cargo bench --bench buffer_pool -- 50 12 --json

# Run ALL benchmarks with auto-discovery (used in CI)
python3 scripts/run_all_benchmarks.py 50 12 output.json
```

### Roadmap

See the full project roadmap in [docs/ROADMAP.md](docs/ROADMAP.md).
