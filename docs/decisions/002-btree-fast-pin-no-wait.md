# 002 — B-Tree Fast Pin Path Is No-Wait

## Context

The `Concurrent INSERT disjoint-key` benchmark exposed a deadlock in the B-tree latch-crabbing path.

The important property of latch crabbing in this codebase is that traversal may hold page latches while trying to move to the next child. That only works if the child acquisition path is restart-friendly: if the next page cannot be acquired immediately, traversal must give up, release its current latches, and restart.

That contract already existed for non-resident pages. If traversal encountered a cache miss, it returned to the caller, slow-pinned the page outside the latched path, and retried from the root.

The bug was that the resident fast path was not actually no-wait. `BufferManager::pin_fast()` could still block on internal buffer-manager locks, so a latched B-tree traversal could enter a lower-level wait cycle instead of restarting.

## Decision

Make the fast-pin path explicitly distinguish:

- `Ready`: page acquisition succeeded immediately
- `NotResident`: page is absent; caller may use the slow pin path outside latches
- `Contended`: fast path would have to wait; caller must drop the current latched path and retry

In other words: the entire latch-crabbing path is no-wait. Any internal contention encountered while acquiring the next child page is treated as restart, not wait.

## Alternatives Considered

**Best-effort/deferred replacement bookkeeping:** plausible, but it attacks one trigger rather than preserving a clear traversal contract.

**Change commit-time flush semantics:** possible, but larger durability / recovery implications and not necessary to restore the intended latch-crabbing behavior.

**Keep blocking fast pin and rely on lock ordering:** rejected. The point of restart-oriented traversal is to avoid waiting while holding page latches.

## Consequences

- B-tree traversal now distinguishes cache miss from contention instead of treating both as slow-pin.
- Fast pin is allowed to fail spuriously under contention; this may increase retries.
- The design trades some throughput under contention for simpler deadlock avoidance.
- The benchmark hang is resolved locally without changing commit semantics.

## Validation

- `cargo test`
- `SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench simple_bench --no-default-features --features replacement_lru --features page-4k -- "Concurrent INSERT disjoint-key" --noplot`

The benchmark completes locally after these changes instead of hanging indefinitely.
