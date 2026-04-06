# Transaction Runtime

This document describes the current transaction runtime architecture.

For the reasoning behind this shape, see:

- `docs/decisions/004-transaction-write-session-asymmetry.md`

## Overview

The transaction runtime is intentionally asymmetric.

- The read path stays shareable and compatible with the current executor shape.
- The write path is explicit and session-based.
- Lower layers should retain only the authority they actually need.

The core split is not `TxnReadState` vs `TxnWriteState` as separate public runtime objects. The implemented shape is narrower:

- `Transaction` remains the shared outer handle used by higher layers.
- `PinState` owns transaction-local pin bookkeeping for RAII pin/unpin.
- `TransactionWriteSession` owns exclusive transaction-local write authority.
- lower write and recovery helpers depend on narrow write-capability traits.

## Authority Boundaries

### Shared Outer Handle

`Transaction` is still the shared runtime handle used by:

- execution/planning context
- scans and cursors
- record/index runtime objects

This keeps the read/query path compatible with the current executor without pushing session lifetimes through `Scan`, `Plan::open`, or operator state.

### Pin State

`PinState` exists so lower RAII/read-side layers do not need `Arc<Transaction>`.

It owns transaction-local pin bookkeeping and supports:

- `pin`
- `pin_fast`
- `unpin`
- `unpin_all`
- buffer lookup for pinned blocks

`BufferHandle` now points at `Arc<PinState>` instead of `Arc<Transaction>`. That means page guards and iterators retain only pin/unpin authority, not broad transaction lifecycle authority.

This is the key read-side/back-edge rule:

- acceptable: upward ownership to narrow pin state for RAII
- not acceptable: lower RAII helpers holding full `Arc<Transaction>` just to unpin on drop

### Write Session

Write lifecycle goes through `TransactionWriteSession`.

`Transaction::write_session()` acquires the transaction-local write mutex and returns a non-`Arc` session object that:

- pins write guards
- exposes commit/rollback/recover
- implements the narrow write traits used by lower layers

The important invariant is:

- write guards borrow from the write session
- `commit(self)` / `rollback(self)` / `recover(self)` consume the session

That means Rust rejects finishing a transaction while a write guard derived from that session is still alive.

## Lower-Layer Capability Rules

Lower write layers should not depend on broad transaction authority when a narrower capability is sufficient.

The current narrow traits are:

- `RecoveryWriteContext`
- `TransactionWriteContext`

These traits are used so helpers can depend on the write operations they actually need, such as:

- transaction id
- write-page pinning
- fast write pinning
- append/log access for B-tree write helpers

This is the key write-side rule:

- top-level write workflows acquire a `TransactionWriteSession`
- lower helpers consume a passed write capability
- lower helpers should not call `write_session()` themselves once they are already in a write workflow

That rule prevents hidden re-acquisition of transaction-local write authority and avoids self-deadlock on the write mutex.

## Read Path

The read path still uses the shared transaction handle.

That is deliberate. In the current executor, one transaction can have multiple long-lived read-side objects at once, including:

- scans
- iterators
- B-tree cursors
- operators with multiple active child scans

The architecture therefore keeps read-side ownership compatible with long-lived shared scan state instead of forcing borrowed read-session lifetimes through the query layer.

## Write Path

The write path is explicit.

Typical shape:

```rust
let txn = db.new_tx();
let ws = txn.write_session();

{
    let mut page = ws.pin_write_guard(&block_id)?;
    page.set_int(...);
    page.mark_modified(ws.txn_id(), lsn);
}

ws.commit()?;
```

Lower write helpers should receive `&TransactionWriteSession` or a narrower `TransactionWriteContext` view instead of reacquiring write authority from `Arc<Transaction>`.

## Recovery

Recovery remains orchestrated by `Transaction`, but recovery internals should depend on narrow write capability rather than broad transaction authority.

That is why rollback/recovery callbacks use `RecoveryWriteContext` instead of conceptually taking full transaction authority.

## Invariants To Preserve

- `BufferHandle` and similar RAII helpers should retain only `PinState`-level authority.
- Write lifecycle should go through `TransactionWriteSession`.
- Lower write helpers should consume passed write capability instead of acquiring their own session.
- Read-side API changes should be evaluated against executor lifetime fallout, not just local transaction ergonomics.