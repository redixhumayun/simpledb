# SimpleDB

This documentation provides information about the SimpleDB project and also provides development workflow for Claude Code agents.

## Project Overview

**Purpose**: A simple SQL database which is a port of an existing SimpleDB database written in Java to Rust. It is mainly for pedagogical purposes and also as a way to experiment with Rust code and performance optimizations

**Tech Stack**: Rust, Python

**Repository**: https://github.com/redixhumayun/simpledb

## Architecture Overview

Core engine paths should remain dependency-free beyond the Rust standard library.
Allowed exceptions:
- platform bindings that expose OS primitives not available in `std` (for example `libc`)
- non-engine tooling dependencies (for example benchmark harnesses like `criterion` and CLI parsing like `clap`)

Do not introduce third-party crates on the critical read/write/transaction execution path without explicit design discussion and rationale.

The code is designed to construct and answer typical SQL queries. The code will construct a query tree that will use the pull-based iterator pattern in a way that is probably typical in most SQL systems. However, the code leans towards readability rather than performance.

There is a test suite which provides basic coverage to ensure the code still works. This can be run with `cargo test`. Never run tests in serial by specifying threads as 1 as this will hide isolation issues. Always make sure tests pass when you make any changes.

## Development Workflow

### Git Workflow (REQUIRED)
1. **Always start by syncing with master**:
   ```bash
   git checkout master
   git pull origin master
   ```

2. **Create feature branch with descriptive name**:
   ```bash
   git checkout -b feature/descriptive-name
   # or fix/bug-description, enhance/improvement-name
   ```

3. **Work autonomously using available tools** until blocked

4. **Test thoroughly before committing**:
   Testing requires running tests with combinations of compiler flags. Run these commands one after another; do not use `--test-threads=1`.
   ```bash
   cargo build
   cargo test --no-default-features --features replacement_lru --features page-4k
   # Verify build works and tests pass

   cargo test --no-default-features --features replacement_clock --features page-4k
   # Verify build works and tests pass

   cargo test --no-default-features --features replacement_sieve --features page-4k
   # Verify build works and tests pass

   cargo test --no-default-features --features replacement_lru --features page-4k --features direct-io
   # Verify direct-io build works and tests pass
   ```

5. **Run benchmarks before committing only when asked**
   ```bash
   cargo run --bin simpledb
   # verify the CLI starts up without errors

   SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench buffer_pool --no-default-features --features replacement_lru --features page-4k
   # verify that the operations complete and see results

   SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench buffer_pool --no-default-features --features replacement_clock --features page-4k
   # verify that the operations complete and see results

   SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench buffer_pool --no-default-features --features replacement_sieve --features page-4k
   # verify that the operations complete and see results

   # Filter to a specific benchmark (Criterion passes filter after --)
   SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench buffer_pool -- "Sequential Scan"
   ```

6. **Run cargo formatting before committing**
   ```bash
   # check whether clippy reports errors
   cargo clippy -- -D warnings
   # check cargo formatting
   cargo fmt -- --check
   # run cargo fix
   cargo clippy --fix
   # fix remaining errors before committing
   ```

7. **Create PR with descriptive title and summary**
   - Include what was implemented
   - Note any breaking changes

### Profiling Workflow (REQUIRED for bottleneck analysis)
Use this workflow when asked to profile a benchmark/workload and identify hotspots.

1. **Build benchmark binaries first**
   ```bash
   cargo bench --bench simple_bench --no-run
   BIN=$(find target/release/deps -maxdepth 1 -type f -name 'simple_bench-*' -executable | head -n1)
   ```

2. **Use profiling-oriented benchmark settings (not quick CI settings)**
   - Prefer longer runs for stable perf samples: `--warm-up-time 3 --measurement-time 15 --sample-size 20`

3. **Record perf data for one filtered workload**
   ```bash
   mkdir -p /tmp/sql_profile
   perf record -e cpu-clock -F 199 -g -o /tmp/sql_profile/workload_perf.data -- \
     "$BIN" --bench --noplot --warm-up-time 3 --measurement-time 15 --sample-size 20 \
     "Concurrent UPDATE same-page disjoint-id"
   ```

4. **Generate flamegraph from perf.data**
   ```bash
   # Prefer Brendan Gregg FlameGraph scripts on this machine
   perf script -i /tmp/sql_profile/workload_perf.data | /home/ci/FlameGraph/stackcollapse-perf.pl > /tmp/sql_profile/workload_perf.folded
   /home/ci/FlameGraph/flamegraph.pl /tmp/sql_profile/workload_perf.folded > /tmp/sql_profile/workload_flamegraph.svg
   ```

5. **Interpretation guardrails**
   - Start with kernel vs user split. If syscall/FS paths dominate (`pwrite/fsync/ext4`), engine CPU is likely not the primary bottleneck.
   - In lock-heavy workloads, flamegraphs alone may miss causal waiting details. If lock wait paths dominate, add targeted lock wait metrics in code and rerun.
   - Always report sample count from perf output. Very low sample counts reduce confidence.

## Architecture Decisions

Significant design decisions are tracked in `DECISIONS.md` at the repo root. Each entry is a one-line summary with a pointer to a detail file in `docs/decisions/`.

Before making a significant architectural change — deadlock strategy, lock granularity, concurrency model, storage layout, etc. — check `DECISIONS.md` first. The detail files capture context, alternatives considered, and the reasoning behind what was chosen. This prevents re-litigating settled decisions and helps understand why the code is the way it is.

When a decision is made or reversed, add or update the relevant entry and detail file as part of the same commit.

## Response Style

Follow these rules when you are responding to the user.

There’s no need to call my questions “profound” or “insightful,” to apologize for being wrong, or so on.  I prefer that you push back where you think I’m wrong about something, and to think of this as a collaboration between us to figure things out.

Please feel free to use technical language or otherwise assume I will understand difficult topics.  I will ask questions if I don’t understand.  Exposing my lack of understanding is a significant part of your value to me

Be extremely concise. Sacrifice grammar for the sake of concision.
