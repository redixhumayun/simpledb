# B-Tree Concurrent Insert Deadlock

## Summary

The `Concurrent INSERT disjoint-key` benchmark reliably hangs, with worker threads blocked
in the kernel on `futex_wait_queue` and no forward progress.

Despite the benchmark shape, the deadlock is not currently caused by concurrent B-tree split
propagation. The immediate root cause is a lock-order cycle in the buffer manager / commit /
replacement-policy paths that concurrent inserts happen to trigger frequently.

## Reproduction

```bash
SIMPLEDB_BENCH_BUFFERS=12 cargo bench --bench simple_bench \
  --no-default-features --features replacement_lru --features page-4k \
  -- "Concurrent INSERT disjoint-key" --noplot
```

The benchmark hangs indefinitely. To confirm the hang, check thread wait channels:

```bash
pid=$(ps aux | grep "simple_bench-" | grep -v grep | awk '{print $2}')
for tid in $(ls /proc/$pid/task/); do
  echo "Thread $tid: $(cat /proc/$pid/task/$tid/wchan)"
done
```

All worker threads will show `futex_wait_queue`. CPU usage decays toward zero while elapsed
time freezes.

To localize the blocking point, attach `gdb` to the benchmark child process:

```bash
gdb -batch -ex "set pagination off" -ex "thread apply all bt" -p "$pid"
```

In reproduced hangs, the key stacks are:

- one thread in `btree::traversal::try_descend_write_fast()` trying to pin the next page
- one thread in `buffer_manager::flush_all()` / `BufferFrame::flush_locked()` during commit
- one thread in `replacement::lru::PolicyState::record_hit()` during pin-path bookkeeping

No hung thread was observed in `propagate_split_up`, `split_leaf_inplace`,
`maybe_make_new_root`, or `IndexFreeList::allocate`.

## Root Cause

### The old split-propagation theory is stale

This file originally blamed concurrent split propagation: multiple inserters each holding tree
page latches while trying to acquire more latches or the meta/free-list page. That no longer
matches the code.

Split-capable inserts are serialized by `SplitGate`, so only one thread can execute the
structural split path at a time. The observed hangs also do not show threads blocked in the
split code itself.

### Actual lock-order inversion

The deadlock is caused by inconsistent lock acquisition order across three subsystems:

1. **B-tree write descent**
   - `try_descend_write_fast()` acquires page write latches while descending the tree.
   - While still holding those page latches, it may try to pin another page.
   - Pinning takes the per-block latch-table mutex and may enter replacement-policy
     bookkeeping.

2. **Commit-time flush**
   - `Transaction::commit()` calls `BufferManager::flush_all()`.
   - `flush_all()` first locks frame metadata (`FrameMeta` mutex), then `flush_locked()` tries
     to acquire the page write latch for the frame.

3. **Replacement-policy bookkeeping**
   - On a resident hit, `BufferManager::try_to_pin()` calls `PolicyState::record_hit()`.
   - LRU `record_hit()` takes the global LRU mutex and then locks one or more frame metadata
     mutexes for neighboring frames.

These paths acquire locks in conflicting orders:

- B-tree descent: `page latch -> block latch / pin path -> frame meta`
- Commit flush: `frame meta -> page latch`
- LRU hit path: `block latch / pin path -> frame meta`

### Observed deadlock shape

The reproduced stacks are consistent with a cycle like this:

```text
Thread A: holds page write latch during B-tree descent
          -> waiting in pin path / latch-table path for another page

Thread B: holds pin-path state and enters LRU record_hit
          -> waiting for a frame-meta mutex

Thread C: holds that frame-meta mutex in flush_all during commit
          -> waiting for the page write latch held by Thread A
```

That is enough for circular wait even without any split propagation or meta-page allocation.

### Why concurrent inserts trigger it

Concurrent inserts are a good trigger because they combine:

- B-tree write-latch crabbing during descent
- repeated page pinning / unpinning on the hot path
- per-operation `commit()`, which runs `flush_all()` frequently
- a small buffer pool in the repro (`SIMPLEDB_BENCH_BUFFERS=12`), increasing pressure on the
  replacement path

The bug is therefore exposed by the insert benchmark, but it is not fundamentally a
split-specific deadlock.

### Why wait-die doesn't recover it

The wait-die protocol is implemented only in `LockTable::acquire()` — the logical lock
layer (table, row, index key locks). Page latches are `RwLock<PageBytes>` inside
`BufferFrame`, acquired via `pin_write_guard()` which has no timeout, no deadlock
detection, and no abort path.

The sequence of events here is instead:
1. Threads acquire logical locks successfully (disjoint keys, so no logical-lock conflict)
2. Threads enter B-tree descent, pin pages, and commit dirty work
3. A lock-order cycle forms among page latches, frame-meta mutexes, and pin-path / replacement
   bookkeeping
4. Threads block in the kernel on `futex_wait_queue`
5. Wait-die never sees the cycle because it exists below the logical lock layer

## Fix Space

This note is documenting root cause, not selecting a fix. Any eventual fix likely needs to
address lock ordering in the buffer manager rather than B-tree split propagation itself.

Promising directions include:

- enforcing one global lock order across `page latch`, `FrameMeta`, latch-table entries, and
  replacement-policy state
- ensuring pin-path bookkeeping never waits on locks while a caller still holds page latches
- decoupling or deferring commit-time `flush_all()` so it does not participate in the same
  lock graph as active B-tree descent
