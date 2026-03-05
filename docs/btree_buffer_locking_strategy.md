# B-Tree Buffer/Locking Deadlock RCA and Architecture Direction

## Why this document exists

We have repeatedly hit lock-order deadlocks while implementing B-tree latch crabbing under concurrency.

This is not a one-off bug in a single function. The issue is architectural: page latches and buffer metadata/replacement bookkeeping are intertwined in ways that allow opposite lock orders across code paths.

The goal of this note is to capture:
- what started the issue,
- why small local fixes keep failing,
- the architecture-level direction required to remove this entire class of deadlocks.

## What triggered the issue

During Phase 3 B-tree write-latch crabbing, writers hold write latches across parent->child descent and split propagation.

At the same time, transactions commit frequently in write-heavy tests (`test_concurrent_writes_disjoint_ranges`, `test_concurrent_split_stress`), which drives `flush_all()` heavily.

This increased overlap between:
- page-latch holding code (`btree::structural::descend_write`, mutable page/view drops), and
- buffer manager metadata/replacement code (`flush_all`, `record_hit`, `unpin`).

That overlap exposed lock-order inversion cycles.

## Concrete deadlock cycles observed

### Cycle A (initial)

- Writer path held page write lock and, during mutable page/view drop, called `mark_modified()`.
- `mark_modified()` acquired frame metadata mutex.
- Commit flush path held frame metadata mutex and then waited for page write lock.

Cycle: `page -> meta` (writer) vs `meta -> page` (flush).

### Cycle B (after local fix for A)

Even after removing the direct `mark_modified` inversion, deadlocks remained.

- Writer path holds `page(root)` while crabbing.
- Writer calls `pin_write_guard(child)`; `pin` calls replacement `record_hit`.
- Under LRU, `record_hit` can lock metadata for multiple frames (target + head/prev/next), not just the child frame.
- Commit flush holds `meta(root)` and waits for `page(root)`.
- Writer, while holding `page(root)`, waits for `meta(root)` indirectly via `record_hit` or `unpin` side effects.

Same cycle class reappears through a different entry point.

## Root cause class

`FrameMeta` currently multiplexes unrelated concerns behind one mutex:
- pin count,
- dirty info (`txn`/`lsn`),
- block binding,
- replacement-policy node state.

Because pin/unpin/hit/flush/evict all touch this same lock, and page operations run concurrently, multiple paths can create opposite lock orders involving `page` and `meta`.

Local fixes (field drop order, one call-site reorder) can remove one instance, but other paths still recreate the cycle.

## Why this cannot be solved reliably with more local patches

As long as all metadata concerns share one coarse frame mutex:
- any new path that touches metadata while a page latch is held can reintroduce `page -> meta`,
- any maintenance path that holds metadata while waiting on page can reintroduce `meta -> page`.

Given current replacement code shape (especially LRU multi-node updates), this is structurally fragile.

## Architecture direction (single coherent strategy)

### Principle

Remove frame-local blocking metadata mutex from transaction hot paths. Keep page-content locking (`RwLock<Page>`) and manage metadata with non-blocking primitives + policy-owned synchronization.

### High-level shape

1. Per-frame hot metadata via atomics
- `pin_count`
- dirty markers (`dirty`, `txn`, `lsn`)
- eviction/reservation state flag

2. Replacement policy state under policy mutex
- LRU/Clock/Sieve structures owned by policy module
- keyed by stable `frame_idx`, not frame mutex guards

3. Strict lock-phase rule
- No code holding page latch may acquire policy/resident-management locks.
- No maintenance path may hold policy lock while waiting for page lock.

4. Eviction reservation protocol
- select candidate frame with `pin_count == 0`
- reserve it (`evicting` CAS/state transition)
- perform flush/rebind outside policy critical section

### Result expected

This removes the lock edge that causes recurring inversion cycles.

## Tradeoff and complexity note

This direction increases interleaving and requires explicit state-machine invariants.

Correctness must be enforced via:
- frame lifecycle state transitions,
- pin/evict exclusion invariants,
- conditional dirty clear semantics,
- residency map consistency guarantees.

So this is not a tiny patch; it is an architectural refactor. But it is the clear path to stop recurring deadlock regressions under latch crabbing.

## Scope decision

This document intentionally avoids proposing another narrow hotfix. The recommendation is to execute the architecture refactor above rather than stacking local lock-order patches.
