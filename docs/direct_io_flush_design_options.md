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

## Chosen Direction

For now, use Option 1: snapshot then submit.

Why:

- it keeps the transaction path and flush path more cleanly separated
- it avoids holding page locks across kernel I/O waits
- it fits the current no-force direction, where commit should not sit on data-page flush behavior
- it gives us a simpler first protocol for correctness under concurrency
- the extra 4 KiB memcpy is unlikely to dominate until writeback throughput and buffer-manager coordination are already much faster than they are today

This does give up zero-copy direct writeback for now. That is acceptable for the first implementation. If snapshot memcpy later shows up clearly in profiling, we can revisit the zero-copy path with a stricter frame-state protocol.

## Protocol Sketch

The frame state is the interface between the transaction subsystem and the flush subsystem.

Initial requirements:

- dirty generation per frame
- at most one in-flight data write per frame
- explicit writeback-in-progress state
- completion clears dirty only if the completed generation still matches the current frame generation

High-level flow:

1. Transaction mutates a page, appends WAL, and marks the frame dirty at generation `g`.
2. Flusher selects a dirty frame that is not already in writeback.
3. Flusher takes `meta -> page`, claims writeback for generation `g`, and copies the page bytes into an owned aligned snapshot buffer.
4. Flusher releases the page lock immediately after the copy.
5. Flusher submits the snapshot buffer through `io_uring`.
6. If transactions mutate the resident page again, the frame advances to generation `g+1` and remains dirty.
7. On CQE completion for generation `g`:
   - if the frame is still at generation `g`, clear dirty and writeback state
   - if the frame has advanced beyond `g`, clear only writeback state and leave the frame dirty

This means completion reconciles against frame generation rather than blindly marking the page clean.

## Batching

The first version should batch submissions, but keep batching policy simple.

Start with:

- a queue of ready snapshot writes
- a fixed maximum batch size
- submit when the batch reaches that size, or when the oldest queued write exceeds a short age threshold

Do not start with adaptive batching. Add that only if profiling shows the fixed policy is leaving throughput on the table.

The age threshold should use a monotonic elapsed-time source such as `Instant`, not wall-clock time.

The flusher also needs a timed wakeup path. If a partial batch is queued and no new writes arrive, those writes must still be submitted once the oldest queued write crosses the age threshold. In practice that means the flusher should wait on a condvar or similar event source with a timeout, waking either on new work or on timeout expiry.

## Benchmarking

The first performance comparison should be direct-I/O against direct-I/O.

Compare:

- current `master` with the existing direct-I/O flush path
- the new branch with snapshot-based direct-I/O writeback

That keeps the benchmark focused on engine behavior:

- frame/writeback protocol
- snapshot memcpy cost
- batched submission
- dirty-page flush throughput

It avoids the lower-signal buffered-vs-direct comparison, where page cache effects dominate and make buffer-resident workloads favor buffered I/O.

For this comparison, we do not need to construct a working set larger than machine RAM just to make the results meaningful. Both sides already use direct I/O for the relevant data path.

We still need one new targeted workload for the flush redesign. Existing benchmarks are useful for broad regression checking, but they do not directly isolate background dirty-page writeback, re-dirtying while an older write is in flight, or batched flush throughput.

Preferred benchmark shape:

- one new background-flush benchmark family
- a small number of scenarios inside it, such as:
  - steady dirty-page production plus flush
  - re-dirty while older writeback is in flight
  - limited buffer-pool / eviction pressure

Existing benchmarks to keep for this work:

- `benches/buffer_pool.rs` dirty-eviction coverage
- `benches/io_patterns.rs` WAL coverage
- `benches/io_patterns.rs` durability coverage
- selected write-heavy end-to-end cases in `benches/simple_bench.rs`

If direct-vs-buffered comparison is no longer a goal, the following `io_patterns` groups are lower signal and should be good candidates for removal or replacement:

- `Phase1/IO Throughput`
- `Phase1/Queue Depth`
- `Phase7/Cache Adverse`
- `Phase8/Cache Evict`

## Scope For First Implementation

- decouple normal transaction commit from data-page flush
- keep rollback and recovery correctness intact
- implement snapshot-based direct-I/O writeback for dirty data pages
- keep one in-flight write per frame
- add tests for generation reconciliation and stale-completion handling

Non-goals for the first implementation:

- zero-copy direct writeback
- adaptive batching
- a more general lock-free frame-state machine
