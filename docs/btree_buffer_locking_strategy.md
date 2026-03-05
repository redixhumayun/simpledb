# B-Tree Buffer/Locking Deadlock RCA and Direction

## Why this document exists

We repeatedly hit lock-order deadlocks while implementing B-tree latch crabbing under concurrency.

This is not a one-off bug in one function. Page-latch operations and buffer metadata/replacement operations were interleaved without a strict boundary, so opposite lock orders became possible.

This note captures:
- what triggered the failures,
- what deadlock cycles we observed,
- what decision we are now taking.

## What triggered the issue

During Phase 3 write-latch crabbing, writers held parent->child latches through descent and split propagation.

At the same time, write-heavy tests (`test_concurrent_writes_disjoint_ranges`, `test_concurrent_split_stress`) committed frequently, driving `flush_all()` often.

That increased overlap between:
- page-latch holding code (`btree::structural::descend_write`, mutable page/view drop paths),
- buffer-manager lifecycle/replacement code (`flush_all`, `record_hit`, `unpin`, eviction paths).

## Concrete deadlock cycles observed

### Cycle A (initial)

- Writer path held page write latch and, during mutable view drop, called `mark_modified()`.
- `mark_modified()` acquired frame metadata mutex.
- Commit flush path held frame metadata mutex and then waited for page write latch.

Cycle: `page -> meta` (writer) vs `meta -> page` (flush).

### Cycle B (after narrow fix attempt)

Even after removing one `mark_modified` inversion, deadlocks remained through crabbing + replacement bookkeeping:

- Writer holds `page(root)` while crabbing.
- Writer attempts to pin child; pin path may run replacement hit bookkeeping.
- Under LRU/SIEVE-style updates, hit bookkeeping can lock metadata for multiple frames.
- Commit/flush holds `meta(root)` and waits for `page(root)`.
- Writer, while holding `page(root)`, waits on metadata lock(s) that can include/root through replacement structure coupling.

Same class reappears through a different entry point.

## Root architectural mismatch

Two lock domains are being mixed in the same critical sections:

- `page` latch: tight, structural/data-path lock (B-tree correctness during traversal/modification),
- `frame/meta` and replacement locks: broader lifecycle/cache-management locks (pin/hit/evict/flush accounting).

When page-latched code enters lifecycle/replacement locking, or lifecycle code waits on page latches while holding metadata locks, opposite order cycles are easy to create.

## Decision: global ordering and phase boundary

We are **not** moving to a lock-free buffer manager in this phase.

We are adopting:

1. **Global lock order**
- `meta -> page` is the only allowed order whenever both are needed.
- `page -> meta` is disallowed everywhere.

2. **Eliminate existing `page -> meta` inversions**
- Remove/rework all paths that call metadata-locking operations while a page latch is held.
- This includes page/view drop-time `mark_modified`-style paths and any equivalent write/undo/format flows.

3. **Fast vs slow buffer-manager pin semantics**
- Fast pin path: resident-only, no replacement/eviction bookkeeping that can block on global/frame metadata structures.
- Slow path: hit bookkeeping and miss/eviction lifecycle work.

4. **Crabbing constraint**
- While holding B-tree latches, crabbing may use only the fast pin path.
- If operation needs non-fast behavior (e.g., miss or slow-path hit/eviction work), it must:
  - release all held B-tree latches first,
  - run slow path,
  - re-enter descent/retry.

## Buffer-manager semantics required to avoid future deadlocks

To make the above stable, buffer manager must guarantee:

1. No `page -> meta` acquisition path exists.
2. Eviction/replacement bookkeeping does not hold policy/global metadata locks while waiting for page latches.
3. Slow-path waits are done without B-tree latches held by caller thread.
4. Fast-path pin while latched never triggers slow-path bookkeeping implicitly.
5. If no evictable frame exists, waiting is on global availability progression, not on a specific page-latched frame dependency.

## Why this direction

This keeps current architecture intact (no lock-free rewrite) while enforcing a strict, auditable lock discipline:

- structural page-latch work remains tight-scoped,
- lifecycle/replacement work remains broader-scoped but outside latched crabbing sections,
- global order stays consistent (`meta -> page`), removing the inversion class that caused current deadlocks.

## Scope decision

Current decision is:

- do a full inversion cleanup (`page -> meta` removal),
- enforce fast/slow pin split in crabbing,
- codify and test required buffer-manager semantics above.

We are intentionally not taking on a full lock-free/state-machine buffer-manager redesign in this iteration.
