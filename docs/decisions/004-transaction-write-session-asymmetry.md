# 004 — Transaction Write Authority Is Explicit And Asymmetric

## Context

The transaction API had two related problems.

First, broad transaction authority was available everywhere through `Arc<Transaction>` plus `&self` methods. That meant Rust could not prevent a caller from holding a write page guard and then calling `commit` or `rollback` on the same transaction. Under the old force-at-commit design, the concrete failure mode was a self-deadlock: a live `PageWriteGuard` could keep a page write latch held while commit-time flushing tried to reacquire that same latch.

```rust
let txn = Arc::new(Transaction::new(...));
let txn2 = Arc::clone(&txn);

let guard = txn.pin_write_guard(&block)?;
txn2.commit()?; // this type-checks; the compiler sees another shared handle
```

With only shared `Arc<Transaction>` ownership, there is no borrow relationship between `guard` and `txn2.commit()`, so Rust cannot reject the overlap.

Second, lower layers retained broader transaction authority than they needed. `BufferHandle` held `Arc<Transaction>` even though it only needed transaction-local pin/unpin behavior for RAII. Recovery and B-tree write helpers could also conceptually reach back into the full transaction object instead of depending on narrower write capabilities.

An earlier direction considered a fuller split between read and write transaction state, with immutable read sessions and mutable write sessions. The intent was to push more guard discipline into the type system.

That direction ran into a hard constraint in the current executor architecture: the read path is not actually read-only at the transaction layer.

- read pinning mutates transaction-local pin bookkeeping
- `BufferHandle::drop` unpins and mutates that same bookkeeping
- `HeapIterator` can keep pins alive across many executor calls
- one transaction can have multiple active read-side objects at once, including scans, iterators, and B-tree cursors

Making read sessions truly borrow immutable transaction state would therefore either:

- reintroduce synchronized interior mutability inside the read state anyway, or
- force read-session lifetimes through `Scan`, `Plan::open`, and executor/operator state

The second cost is the important one. If read-side objects borrowed a read session, scan state would start looking like this:

```rust
struct TableScan<'a> {
    heap_iter: Option<HeapIterator<'a>>,
}

fn open<'a>(&self, ...) -> Box<dyn Scan + 'a>
```

That lifetime would then spread through plan opening, scan composition, and operator state across the executor.

That was too invasive for the concrete bug being addressed.

## Decision

Use an intentionally asymmetric transaction authority model.

- Keep the read path shareable and compatible with the current executor shape.
- Make write authority explicit through `TransactionWriteSession`.
- Make lower RAII/read-side ownership point at narrow pin state, not full transaction authority.
- Make lower write helpers depend on narrow write capabilities passed from the caller instead of acquiring their own write session from `Arc<Transaction>`.

In practice this means:

- `Transaction` remains the shared outer handle used by the executor and higher-level runtime objects.
- `PinState` owns transaction-local pin bookkeeping needed by `BufferHandle` and guard/iterator RAII.
- `TransactionWriteSession` owns exclusive transaction-local write authority for write page guards and lifecycle operations such as `commit`, `rollback`, and `recover`.
- lower write layers use narrow traits such as `TransactionWriteContext` and `RecoveryWriteContext` instead of depending on broad transaction authority.

## Why this shape

This design targets the actual hazard without forcing a larger executor rewrite.

The important type-system property is on the write path:

- write guards borrow from `TransactionWriteSession`
- `commit(self)` / `rollback(self)` / `recover(self)` consume the session
- the compiler therefore rejects finishing a transaction while a write guard derived from that session is still alive

```rust
let ws = txn.write_session();
let page = ws.pin_write_guard(&block)?;
ws.commit()?; // does not compile; `page` still borrows `ws`
```

That closes the deadlock-producing overlap we cared about.

At the same time, the design avoids treating the read path as more borrow-friendly than it really is. Read-side pin ownership still has to support long-lived scans and iterators, so the architecture keeps that path shareable instead of pushing session lifetimes into the query layer.

The asymmetry is intentional. The runtime already has asymmetric needs:

- writes need exclusive lifecycle control
- reads need long-lived shareable scan state
- write guards are the primary commit/deadlock hazard

## Alternatives Considered

**Symmetric read/write sessions over split transaction state:** rejected for the current codebase shape. Read pinning still mutates transaction-local state, and borrow-checking the read path would have forced lifetimes through executor and scan APIs or required synchronized mutation inside the read state anyway.

**Keep broad `Arc<Transaction>` authority everywhere and rely on discipline:** rejected. That was the source of the write-guard/commit overlap bug and kept lower layers coupled to more authority than they needed.

**Remove shared outer transaction ownership entirely:** rejected for now. It would likely require a much broader rewrite of scans, plans, and executor ownership, with little value beyond the narrower write-side safety we actually needed.

## Consequences

- The transaction runtime is explicitly asymmetric rather than aesthetically symmetric.
- Read-side code stays compatible with the current executor shape.
- Write-side code must acquire and thread explicit write authority.
- Lower layers should not call `write_session()` on their own once they are already in a write workflow.
- Narrow capability traits become the preferred boundary for lower write and recovery helpers.

## What this does not imply

- It does not eliminate all interior mutability from transaction-local state.
- It does not make the read path compile-time incompatible with `commit` while read guards are alive.
- It does not use the transaction write mutex as database-level concurrency control; inter-transaction concurrency still comes from the lock table, page latches, buffer manager, and WAL synchronization.
