# 006 — Direct-I/O Page Writeback Uses Snapshot-First Batched Flush

## Context

Issue #102 wanted data-page flushes in `direct-io` builds to use batched `io_uring` writes instead of one blocking write per dirty page.

That change was not just I/O plumbing.

The flush path sits on a sensitive concurrency boundary:

- page and frame locking uses the `meta -> page` order
- B-tree traversal has restart-oriented paths that should not gain new waits
- WAL-before-data ordering must still hold
- the runtime is now `steal + no-force`, so commit should not wait on data-page flush behavior

The key design question was whether direct-I/O writeback should submit resident page memory directly or first copy stable page bytes into owned snapshot buffers.

## Decision

Use a snapshot-first writeback protocol for dirty data pages.

The chosen protocol is:

- transactions mark frames dirty with a per-frame generation and governing WAL LSN
- once a dirty frame becomes flush-eligible, it may enter the dirty queue
- the flusher claims one dirty generation while holding `meta -> page`
- the flusher stamps page-header LSN and CRC, then copies the page bytes into an owned aligned snapshot buffer
- page locks are released before any kernel I/O wait
- the flusher flushes WAL up to the max LSN in the batch
- direct-I/O builds submit those stable snapshots through batched `io_uring` writes
- writeback completion clears dirty state only if the completed generation still matches the frame; otherwise completion clears only the in-flight writeback state and leaves the newer dirty generation resident

This implies an explicit frame writeback protocol with:

- dirty generation per frame
- at most one in-flight writeback generation per frame
- explicit distinction between `dirty` and `writeback in progress`
- generation-based completion reconciliation

## Why this shape

This was chosen to minimize correctness risk while still delivering the intended direct-I/O batching change.

The main reasons:

1. It preserves the existing lock and restart model.

Holding a page lock across `io_uring` completion waits would couple kernel I/O latency to page residency and writer progress. That would be a larger concurrency-model change than this work needed.

2. It keeps transaction commit decoupled from data-page flush.

Under the chosen no-force direction, commit durability should depend on WAL durability, not on waiting for data-page writeback. Snapshot-first writeback fits that split cleanly.

3. It localizes the concurrency contract.

The transaction side and flush side coordinate through explicit frame state rather than through ad hoc assumptions about whether the resident page buffer is safe to write concurrently.

4. It makes stale completion safe.

If generation `g` is being written out and the page is dirtied again to `g+1`, completion for `g` must not incorrectly mark the frame clean. The generation protocol makes that rule explicit.

5. It is the smallest viable step toward batched direct-I/O writeback.

The extra page copy is a real cost, but it is simpler than introducing a full zero-copy protocol that would need every writer path to define behavior while writeback is in flight.

## Alternatives Considered

**In-place zero-copy direct writeback of resident page memory:** rejected for now.

That approach avoids the snapshot memcpy, but it requires a stronger concurrency contract:

- resident page bytes must remain stable for the full I/O lifetime
- writers must either block, restart, or otherwise coordinate with in-flight writeback
- latch-order and restart behavior across the buffer manager and B-tree code would need a careful re-audit

That is a larger and riskier design change than the first batched direct-I/O implementation warranted.

## Consequences

- direct-I/O writeback now uses stable snapshot buffers, not resident page memory
- dirty-page flush can batch writes while still respecting WAL-before-data ordering
- frame state is more explicit because writeback coordination is now part of the buffer protocol
- normal commit latency is not coupled to data-page flush latency
- one extra memcpy is paid per flushed page

## Notes

- If profiling later shows snapshot memcpy is a dominant cost, the next step would be to evaluate a stricter zero-copy protocol as a separate decision, not as a small optimization inside the current one
