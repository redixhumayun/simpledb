# Direct I/O Flush Design Options

Context: issue #102 wants data-page flushes in `direct-io` builds to use batched `io_uring` writes instead of one blocking `pwrite` per dirty page.

Constraint to preserve:

- do not hold buffer/page locks across kernel I/O waits
- preserve the current lock-order invariant: `meta -> page`
- do not introduce new wait cycles into restart-oriented B-tree paths

## Option 1: Snapshot Then Submit

Shape:

1. Briefly take `meta -> page`.
2. Update page CRC.
3. Copy the 4 KiB page into an owned aligned `PageBytes` snapshot.
4. Release locks.
5. Flush WAL to the max LSN in the batch.
6. Submit `io_uring` writes from the snapshots.
7. On completion, reacquire `meta` and clear dirty state only if the frame was not dirtied again.

Correctness notes:

- If the page is mutated after the snapshot is taken, the snapshot is stale but still usable as an older durable image.
- In that case the frame must remain dirty so the newer in-memory version is flushed later.
- The dangerous case is overlapping writes for the same frame/block: an older snapshot completing after a newer one can overwrite the newer on-disk image.
- So this option still needs explicit per-frame writeback state.
- Required rules:
  - at most one in-flight data write per frame/block
  - mutations after snapshot must not let completion clear newer dirty state
  - completion must clear dirty only if the frame generation/dirty state still matches the submitted snapshot
  - if the frame changed after snapshot, completion clears only the writeback-in-progress state and leaves the frame dirty

Pros:

- preserves current concurrency model
- avoids holding locks while waiting for CQEs
- low deadlock risk
- smallest design change

Cons:

- not zero-copy; adds one memcpy per flushed page
- completion path must avoid clearing newer dirty state

## Option 2: In-Place Zero-Copy Writeback

Shape:

1. Briefly take `meta -> page`.
2. Update page CRC.
3. Mark the frame `writeback_in_progress`.
4. Release locks.
5. Submit `io_uring` writes using the resident page buffer directly.
6. Block new writers from mutating the page until CQE completion.
7. On completion, clear writeback state and dirty state.

Correctness notes:

- This option removes the stale-snapshot overwrite hazard because writeback uses the resident page buffer directly rather than an older copied image.
- But same-frame update contention still exists.
- While a direct writeback is in flight, the resident page bytes are the DMA source for that write.
- A concurrent writer cannot safely mutate that page in place until writeback completes.
- So a writer encountering `writeback_in_progress` must either:
  - wait, after releasing latches and backing out of restart-sensitive paths
  - fail/restart and retry later
  - or use a different buffer/versioning scheme such as copy-on-write or double buffering
- Without an additional versioning scheme, this option serializes same-frame updates earlier in the write path rather than later in the flush path.

Pros:

- preserves zero-copy direct writeback
- avoids the snapshot memcpy

Cons:

- larger concurrency-model change
- every writer path must define behavior for `writeback_in_progress`
- if any path waits incorrectly while holding latches, deadlock or livelock risk returns
- needs a careful audit of restart-vs-wait behavior across the buffer manager and B-tree code

## Current Read

If minimizing correctness risk is the priority, prefer Option 1.

If preserving zero-copy is the priority, Option 2 needs a writeback-state design first; it is not just an I/O plumbing change.
