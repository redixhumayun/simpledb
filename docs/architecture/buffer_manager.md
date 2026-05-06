# Buffer Manager

This document describes the current buffer manager architecture.

The goal is to explain the buffer manager as a whole:

- what responsibilities it owns
- what state it maintains
- how pin / eviction / writeback fit together
- which invariants lower layers rely on

## Responsibilities

The buffer manager is the in-memory residency and writeback boundary for database pages.

It is responsible for:

- mapping `BlockId` values to resident frames
- pinning and unpinning frames on behalf of transactions
- coordinating replacement policy state
- writing dirty data pages back to disk
- preserving WAL-before-data ordering at page flush time
- providing a resident-only fast pin path for restart-oriented B-tree traversal

It is not responsible for:

- database-level locking between transactions
- WAL append policy
- query planning or executor ownership

Those concerns live in the lock table, `LogManager`, and higher transaction/query layers.

## Core Structures

### `BufferManager`

`BufferManager` is the owner of the subsystem. It holds:

- the fixed-size `buffer_pool`
- the file and log manager handles
- availability counters
- sharded latch and residency tables
- replacement policy state
- dirty-page queue and background flush coordination state

Operationally, it is the place where page residency, replacement, and writeback policy meet.

### `BufferFrame`

Each frame contains:

- the resident page bytes
- frame metadata
- handles needed for single-frame read / write / flush operations

`BufferFrame` is where page-local flush work happens:

- stamp page-header LSN from frame metadata
- update page CRC
- snapshot or write page bytes

### `FrameMeta`

`FrameMeta` is the shared protocol state for one frame.

The current shape is explicit:

- residency state: free vs resident block
- pin count
- flush/writeback state
- replacement-policy metadata
- stable frame index for queue/policy bookkeeping

The important design point is that frame state is no longer just “dirty or clean”.
It is the contract between transaction-side mutation and flush-side writeback.

### Residency Tables and Latches

Two sharded tables support the main pin path:

- latch shards: per-block internal latches used to serialize residency changes
- resident shards: `BlockId -> Weak<BufferFrame>` mapping for resident lookup

This lets the buffer manager keep the hot path scoped to a block rather than a global table lock.

### Replacement Policy Boundary

Replacement policy is abstracted behind `PolicyState`.

The buffer manager delegates:

- hit recording
- assignment notifications
- victim selection

The buffer manager still decides whether a frame is actually eligible to be reused.

## Frame Lifecycle

At a high level, a frame moves through these states:

1. free or resident
2. pinned or unpinned
3. clean, dirty, or writeback-in-progress

These axes are intentionally modeled separately. Residency, active use, and flush/writeback coordination answer different questions and should not be collapsed into one overloaded flag.

## Main Flows

### Pin Hit

On a resident hit:

1. shard to the block
2. take the block latch
3. confirm the resident mapping is still valid
4. update replacement-policy hit state
5. increment frame pin count
6. update availability accounting if the frame moved from unpinned to pinned

This is the common path for buffer-resident access.

### Pin Miss and Eviction

On a miss:

1. shard to the block
2. confirm the block is not already resident
3. ask the replacement policy for an evictable frame
4. remove the old residency mapping if needed
5. flush the victim if it still holds a dirty page image
6. read the requested block into the frame
7. install the new residency mapping
8. pin the frame for the caller

The replacement policy chooses candidates, but the buffer manager enforces the real safety rules around flush and reuse.

### Unpin

Unpin is where transaction-side page use hands control back toward replacement and flushing.

When the final pin is released, the buffer manager may:

- increase general availability
- increase clean-frame slack if the frame is now clean and unpinned
- enqueue the frame for writeback if it is dirty and newly flushable

This is why pin-count transitions are modeled explicitly instead of open-coding the accounting at each call site.

### Mark Dirty

A page mutation eventually calls into the buffer manager to mark the frame modified with:

- the transaction id
- the governing WAL LSN

Dirtying a frame:

- advances its dirty generation
- updates flush metadata
- may enqueue the frame immediately if it is already unpinned

This keeps queueing and state transitions centralized at the frame-protocol boundary.

## Writeback Protocol

### Why the Protocol Exists

The buffer manager now runs in a `steal + no-force` runtime:

- pages may flush before commit
- commit does not force data-page flush

That means dirty data-page writeback must be coordinated explicitly and safely under concurrency.

### Snapshot-First Writeback

The chosen protocol is snapshot-first.

For one flushable dirty generation:

1. take `meta -> page`
2. claim the current dirty generation for writeback
3. stamp page-header LSN and recompute page CRC
4. copy page bytes into an owned aligned snapshot buffer
5. release the page lock
6. flush WAL up to the required LSN
7. write the snapshot to disk
8. reconcile completion against current frame generation

The page lock is not held across kernel I/O waits.

### Generations

Each dirty image has a generation.

This solves the stale-completion problem:

- generation `g` may be claimed and written out
- the resident page may be dirtied again to generation `g+1` before completion
- completion for `g` must not clear the newer dirty state

So completion is generation-based:

- if the completed generation still matches the resident dirty generation, the frame becomes clean
- otherwise the writeback state is cleared but the frame remains dirty and may be requeued

### Dirty Queue and Background Flusher

Dirty frames become queue candidates once they are flushable.

The background flusher is pressure-driven:

- it watches a dirty queue
- it wakes on enqueue, timeout, or shutdown
- it precleans pages when the count of clean unpinned frames falls below a target

This is meant to reduce the chance that foreground misses must pay the full dirty-eviction cost synchronously.

### Synchronous Forced Flush

`flush_all(txn)` is still used for rollback and recovery paths.

That path reuses the same writeback protocol:

- claim snapshots for the target transaction’s dirty generations
- flush batches with WAL-before-data ordering
- reconcile completion back into frame state

The important distinction is operational:

- normal commit is WAL-only
- rollback/recovery still explicitly force data-page durability where required

## Availability Accounting

The buffer manager tracks two related but distinct notions:

- `num_available`: frames whose pin count is zero
- `clean_unpinned`: frames that are both unpinned and clean enough to count as immediate eviction slack

The second number exists because “available” is not the same as “cheap to reuse”.

A dirty unpinned frame may be technically available, but reusing it still requires writeback work.

## Fast Pin Path

The fast pin API exists for B-tree latch-crabbing and restart-oriented traversal.

Its contract is intentionally narrow:

- only succeeds for already-resident pages
- never waits on internal locks
- distinguishes:
  - ready
  - not resident
  - contended

That lets upper layers restart instead of introducing new waits in sensitive traversal paths.

## Invariants

The main buffer-manager invariants are:

1. Lock order is `meta -> page` on flush/writeback coordination paths.
2. WAL must be durable before page bytes for the governing page LSN reach disk.
3. At most one writeback generation is in flight for a frame at a time.
4. Writeback completion must not clear newer dirty state.
5. Replacement must not reuse pinned frames.
6. Replacement should treat writeback-in-progress frames as ineligible victims.
7. Residency-table validation must be rechecked after internal locking because weak resident entries can go stale.

## Tradeoffs

1. The state machine is more explicit and safer, but more complex than a simple dirty bit plus pin count.
2. Snapshot-first writeback pays one memcpy per flushed page.
3. Background precleaning reduces foreground writeback cost, but adds coordination state and pacing policy.
4. The buffer manager now carries more architectural weight because no-force durability pushes more responsibility onto flush and recovery boundaries.

## Relationship To Other Docs

- [docs/architecture/WAL.md](./WAL.md) explains durability, recovery, and WAL ordering.
- [docs/architecture/transaction_runtime.md](./transaction_runtime.md) explains transaction authority and why write lifecycle control is explicit.
- [docs/decisions/006-snapshot-writeback-protocol.md](../decisions/006-snapshot-writeback-protocol.md) records why snapshot-first writeback was chosen over the main alternative.
