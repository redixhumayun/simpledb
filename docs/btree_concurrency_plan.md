# B-Tree Concurrency Plan

## Current Status (Code-As-Of-Now)

The code is already past the state described in older notes:

- Index operations do take logical table locks on an index-specific table-id namespace.
- `BTreeIndex::before_first` acquires index table-`S`.
- `BTreeIndex::insert` / `delete` acquire index table-`X`.
- Physical page access is via per-page `RwLock` latches (`pin_read_guard` / `pin_write_guard`).

What this means today:

- Read/read can run concurrently.
- Write/write is serialized.
- Read/write is blocked by table `S` vs `X`.
- Tree traversal/update code is not yet latch-coupled (no parent-child guard handoff).

So current implementation is safe but coarse; it does not provide concurrent read+write index
access.

## Required Changes: Logical Locking Layer

Goal: keep 2PL correctness while removing global index serialization.

### 1. Switch index entry locks from `S/X` to intent locks where possible

- Read path: acquire index table `IS`.
- Write path: acquire index table `IX`.
- Keep locks until commit/rollback (unchanged 2PL lifetime).

This requires exposing `lock_table_is` at `Transaction` API level (it already exists internally
in `ConcurrencyManager`).

### 1a. Deadlock strategy for index logical locks

Use the existing project-wide policy: **timeout-only deadlock resolution**.

- Keep waiting on condvar until timeout, then return lock timeout error.
- Do not reintroduce wait-die now.
- Keep this aligned with `docs/decisions/001-wait-die.md`.

### 2. Add index-specific logical lock targets (not just table/row)

Current lock targets are only:

- `Table { table_id }`
- `Row { table_id, block, slot }`

For index concurrency, add logical targets for index contents, e.g.:

- `IndexKey { index_id, key }` for point protection.
- `IndexRange { index_id, left, right }` (or gap/next-key equivalent) for range/phantom protection.

Without these, `IS/IX` alone allows physically concurrent operations but does not fully define
serializable behavior for predicates/ranges.

### 2a. Lock model choice for this project: Option 1 (`IndexKey` + `IndexRange`)

Adopt explicit point and range lock targets:

- `IndexKey { index_id, key }`
- `IndexRange { index_id, low, high }` using canonical half-open interval semantics: `[low, high)`

Usage rules:

- Equality read (`k = v`): acquire `S` on `IndexKey(v)`.
- Equality write (insert/delete/update key `v`): acquire `X` on `IndexKey(v)`.
- Range read (`low <= k < high`): acquire `S` on `IndexRange(low, high)`.
- Range delete/update by predicate: acquire `X` on affected `IndexRange(low, high)`.
- Insert key `k`: must conflict-check against overlapping `IndexRange` locks and acquire `X` on
  `IndexKey(k)`.

Conflict checks happen during lock acquire (not as a separate post-check step) to avoid TOCTOU
races.

Why both lock types:

- `IndexKey` keeps point operations cheap and precise.
- `IndexRange` prevents phantoms (new/deleted keys entering/leaving a predicate result set).

Duplicate-key behavior:

- Current B-tree index semantics are non-unique (duplicate keys are allowed).
- Preserve this behavior.
- `IndexKey(k)` therefore protects the logical key `k` and all duplicate entries under `k`.

### 2b. Compatibility matrix (`IndexKey` / `IndexRange`)

Use standard `S`/`X` mode compatibility when lock targets overlap in keyspace:

| Held \\ Requested | S | X |
|---|---|---|
| S | Compatible | Conflict |
| X | Conflict | Conflict |

Overlap rules:

- `IndexKey(k1)` vs `IndexKey(k2)`: overlap iff `k1 == k2`.
- `IndexRange(r1)` vs `IndexRange(r2)`: overlap iff intervals intersect (respecting inclusive/exclusive bounds).
- `IndexKey(k)` vs `IndexRange(r)`: overlap iff `k` is contained in `r`.

Examples:

- `S(IndexKey(25))` with `S(IndexRange[20,30])` -> compatible.
- `X(IndexKey(25))` with `S(IndexRange[20,30])` -> conflict.
- `S(IndexKey(40))` with `X(IndexRange[20,30])` -> compatible (no overlap).
- `X(IndexRange[10,20])` with `X(IndexRange[20,30))` -> no overlap if only right endpoint is open at `20`; conflict if both include `20`.

### 2c. Isolation claim

With strict 2PL plus `IndexKey`/`IndexRange` conflicts, predicate conflicts are represented
(including phantom-producing inserts/deletes into a scanned range). Under that model, the
serializable claim is valid for indexed predicate access paths.

### 3. Keep lock namespace separation for indexes

Continue using the existing index lock key namespace:

```
index lock key = 0x4000_0000 | indexed_table_id
```

This is already implemented and should remain the logical lock identity root for index-level
targets.

### 4. Fairness / wake-up policy (deferred)

Current lock release behavior uses `notify_all()`; keep this as-is for now.

- No fairness/writer-preference redesign in this phase.
- Revisit only if starvation/contention evidence appears under real workloads.

## Required Changes: Internal B-Tree API / Latching

Goal: implement latch crabbing in B-tree code paths, independent of logical 2PL locks.

### 1. Add traversal primitives that hold parent + child temporarily

Current search/insert logic acquires a latch on one node, reads child pointer, then releases.
Crabbing needs explicit handoff:

1. latch parent
2. choose child
3. latch child
4. release parent when child is safe

This needs new internal helpers that return guard-carrying traversal state instead of plain
`BlockId`/`usize`.

### 2. Separate read traversal from write traversal with safety checks

- Read traversal: read-latch coupling down the tree.
- Write traversal: write-latch coupling; release ancestors once child is "safe":
  - insert-safe: child not full
  - delete-safe: child above minimum occupancy (when delete rebalancing is introduced)

### 3. Refactor split propagation to work with latch-held path context

Current split flow is recursive and reacquires pages (`insert_entry`, `split_page`,
`make_new_root`) without explicit path latch ownership.

Crabbing-friendly flow should:

- maintain an explicit path stack of latched nodes during descent,
- perform local split,
- propagate separator upward through latched ancestors,
- release latches in a deterministic order.

### 4. Keep public `Index` trait stable; change internals

No public planner/scan API change is required for crabbing. Existing `Index` trait can stay:

- `before_first`
- `next`
- `get_data_rid`
- `insert`
- `delete`

Required changes are inside `BTreeIndex`, `BTreeInternal`, and `BTreeLeaf` internals.

## Acceptance Criteria

- Logical layer:
  - Index reads use `IS`; writes use `IX` plus appropriate fine-grained index locks.
  - No global table-`X` requirement for ordinary index writes.
- Physical layer:
  - Read traversal uses latch coupling.
  - Write traversal uses safe-node latch crabbing.
  - Split path does not rely on unlatch/re-latch races for parent updates.
- Behavior:
  - concurrent read+read allowed,
  - concurrent read+write allowed where key/range locks permit,
  - write/write conflicts are resolved by logical key/range locks (not global index `X`),
  - existing single-thread tests and new concurrent index tests pass.

## Testing Scope

### 1. Lock manager unit tests (near `LockTable`)

Add focused tests for:

- `IndexKey` vs `IndexKey` overlap and `S/X` compatibility.
- `IndexRange` vs `IndexRange` interval-overlap behavior under `[low, high)`.
- `IndexKey` vs `IndexRange` containment/compatibility.
- Boundary cases around interval edges.

These tests validate lock semantics directly, independent of B-tree structure.

### 2. End-to-end B-tree/integration tests

Add correctness tests for phantom behavior and predicate stability across transactions.

Minimum required phantom test scenario:

1. `T1` starts and reads range `[20,30)` (holding the corresponding logical range lock) and records count `n`.
2. `T2` attempts to insert key `25` and commit.
3. `T1` re-reads the same range `[20,30)` in the same transaction.
4. Assert no phantom in `T1`:
   - either `T2` is blocked/aborted until `T1` ends, or
   - `T1`'s second read still returns count `n` before `T1` commits.

Important: existing concurrent B-tree tests and 80/20 RW benchmark are useful stress/smoke
coverage, but they do not by themselves prove `IndexKey`/`IndexRange` logical lock semantics
or phantom prevention guarantees.

## Baseline: Index Concurrency Benchmark (table-level locking)

These numbers represent the performance floor before latch crabbing or fine-grained logical
locking is introduced. Use them as the comparison reference when evaluating improvements or
regressions.

### How to reproduce

```bash
# From the repository root, on the same commit that introduced the benchmark:
CI=1 cargo bench --bench simple_bench \
    --no-default-features --features replacement_lru --features page-4k \
    -- "Index Concurrency"
```

`CI=1` activates the fast criterion profile (1 s warmup, 5 s measurement, 100 samples). Remove
it for a full run (~165 s per benchmark function at the current workload size).

### Workload parameters

| Parameter | Value |
|---|---|
| Workers (`CONC_WORKERS`) | 4 |
| Ops per worker (`CONC_OPS_PER_WORKER`) | 24 |
| Total elements per iteration | 96 |
| Pre-populated rows (lookup baseline) | 200 |
| Per-op transaction granularity | 1 txn per op (create `BTreeIndex` → op → commit) |
| Locking model | table-level `S` (read) / `X` (write) on `index_lock_table_id` |

### Environment

| Field | Value |
|---|---|
| CPU | Intel Xeon E3-1275 v5 @ 3.60 GHz (8 logical cores) |
| OS kernel | Linux 6.8.0-100-generic |
| Rust toolchain | rustc 1.92.0 (ded5c06cf 2025-12-08) |
| Cargo | 1.92.0 (344c4567c 2025-10-21) |
| Feature flags | `replacement_lru`, `page-4k` |
| Criterion profile | `CI=1` (1 s warmup, 5 s measurement, 100 samples) |

### Results (branch `feature/btree-concurrency`, tree contains uncommitted changes on top of b8274f6)

The benchmark code itself is part of the uncommitted diff; commit hash `b8274f6` is the last
committed ancestor. Once this branch is merged, the commit SHA of the merge should be recorded
here.

| Benchmark | Time / iter (low–high, 95% CI) | Throughput (elem/s) |
|---|---|---|
| Concurrent INSERT disjoint-key | 1.654 s – 1.711 s | 56.1 – 58.0 (mean **57.2**) |
| Concurrent LOOKUP pre-populated | 797 ms – 804 ms | 119.4 – 120.4 (mean **119.9**) |
| Concurrent mixed 80/20 RW | 984 ms – 1.062 s | 90.4 – 97.6 (mean **94.3**) |

### Interpretation

- **INSERT is the slowest** (~57 elem/s) because every insert must acquire a global index
  table-`X` lock, serializing all 4 workers. Effective throughput == single-writer throughput.
- **LOOKUP is fastest** (~120 elem/s) because multiple readers can hold `S` simultaneously;
  4 workers genuinely overlap.
- **Mixed 80/20** sits between the two: the 20% write fraction introduces table-`X` interludes
  that block the concurrent readers.

After latch crabbing + `IS`/`IX` / `IndexKey`/`IndexRange` logical locking:

- INSERT throughput should scale toward multi-writer concurrency (expected significant gain).
- LOOKUP throughput should remain roughly the same or improve slightly (already concurrent).
- Mixed throughput should approach LOOKUP levels as write-induced serialization decreases.

## References

- `docs/lock_granularity_refactor_notes.md` — lock granularity design
- `docs/deadlock_handling.md` — deadlock prevention policy
- `docs/decisions/001-wait-die.md` — timeout-only deadlock decision
- PR #82 — heap-level row locking (prerequisite)
