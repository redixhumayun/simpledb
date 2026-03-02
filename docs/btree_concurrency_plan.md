# B-Tree Concurrency Plan

## Current State

B-tree operations (`BTreeIndex::insert`, `BTreeIndex::delete`, `before_first`/`next`) call
`txn.pin_read_guard`/`pin_write_guard` directly without acquiring any logical transaction lock
first. This means concurrent transactions can interleave index reads and writes without 2PL
protection, violating isolation.

This was masked before the lock-granularity refactor (PR #82) because `pin_read_guard`/
`pin_write_guard` implicitly called `slock`/`xlock` on the page's `BlockId`. That coupling
has been removed; logical locks are now the caller's responsibility.

## Why Index Concurrency Is Harder Than Heap

On the heap, locking at row granularity is straightforward: lock the row, pin the page, read
or write, unpin. The lock target is stable for the lifetime of the operation.

B-tree structural modifications (splits, root promotions) touch multiple pages in a single
logical operation. Naive page-level locking creates two problems:

1. **Lock ordering**: acquiring locks top-down on traversal and bottom-up on split creates
   potential deadlock cycles.
2. **Lock duration**: holding write locks on upper nodes while splitting lower nodes
   serializes the entire tree.

## Recommended Approach: Latch Coupling (Crabbing)

Separate logical transaction locks (2PL, held to commit) from physical page latches (held
only during traversal):

### Reads (lookup / range scan)
1. Acquire table-S logical lock on the index at entry.
2. Traverse top-down, holding a read latch on each node only until the child latch is
   acquired (latch coupling / crab down).
3. Release parent latch before acquiring child latch.

### Writes (insert / delete)
1. Acquire table-X logical lock on the index at entry.
2. Traverse top-down with write latches, but release parent latch as soon as the child is
   known to be "safe" (not full for insert, not at minimum occupancy for delete).
3. If a split or merge is required, re-traverse with write latches held all the way down.

The table-S/table-X logical lock ensures 2PL correctness across transactions. The latch
crabbing keeps physical page contention short-lived.

## Lock Key

Each index needs a stable lock identity distinct from the heap table it indexes. Use a
dedicated namespace in the `table_id` space:

```
index lock key = 0x4000_0000 | indexed_table_id
```

This avoids collision with heap table IDs (0–`0x3FFF_FFFF`) and temp table IDs
(`0x8000_0000+`).

The index entry points (`BTreeIndex::new`, `insert`, `delete`, `before_first`) already
receive a `txn: Arc<Transaction>`; add `index_table_id: u32` alongside and call
`txn.lock_table_s`/`txn.lock_table_x` before any pin call.

## Acceptance Criteria

- `BTreeIndex::before_first` / `next` acquire table-S on the index before traversal.
- `BTreeIndex::insert` / `delete` acquire table-X on the index before modification.
- Concurrent read+read on the same index is not blocked.
- Concurrent read+write or write+write on the same index serializes correctly.
- New concurrent index tests cover the above scenarios.
- Existing single-threaded B-tree tests continue to pass.

## References

- `docs/lock_granularity_refactor_notes.md` — lock granularity design
- `docs/deadlock_handling.md` — deadlock prevention policy
- PR #82 — heap-level row locking (prerequisite)
