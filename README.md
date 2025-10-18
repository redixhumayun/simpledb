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

### Benchmarks

Run performance benchmarks:
```bash
cargo run --bin simple_bench
```

Or with custom iterations:
```bash
cargo run --bin simple_bench 100
```

Run buffer pool benchmarks:
```bash
cargo bench --bench buffer_pool -- 50 12
```

#### CI Benchmark Tracking

All PRs automatically track CRUD operation performance (INSERT, SELECT, UPDATE, DELETE). Results are:
- Stored historically in the `gh-pages` branch
- Compared against previous runs with 5% alert threshold
- Posted as PR comments when significant changes are detected (>5%)
- **Never block merges** - alerts are informational only

#### Performance Label

Add the `performance` label to your PR to:
- Run comprehensive buffer pool benchmarks (all 5 phases)
- Generate a detailed base-vs-PR comparison report
- Get side-by-side performance metrics in a PR comment

This is useful when:
- Implementing cache eviction algorithms (LRU, Clock, etc.)
- Modifying buffer manager code
- Making changes that could impact memory/disk I/O performance

#### JSON Output

Benchmarks support JSON output for CI integration:
```bash
# CRUD benchmarks
cargo run --bin simple_bench 50 --json

# Buffer pool benchmarks (Phase 1 only)
cargo bench --bench buffer_pool -- 50 12 --json
```

### Core Features

The database supports ACID transactions, along with some other niceties like 
* A buffer pool to manage memory
* A WAL to ensure durability
* A catalog to manage metadata for all tables
* A query engine with a simple optimizer

### Roadmap

See the full project roadmap in [docs/ROADMAP.md](docs/ROADMAP.md).