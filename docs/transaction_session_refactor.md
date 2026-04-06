# Transaction Session Refactor

Tracking Issues: [#63](https://github.com/redixhumayun/simpledb/issues/63), [#98](https://github.com/redixhumayun/simpledb/issues/98)

## Motivation

- The current `Transaction` API is centered on `Arc<Transaction>` plus `&self` methods. That means Rust cannot prevent a caller from holding a page guard and then calling `commit`/`rollback` on the same transaction.
- The concrete hazard is write-side: a live `PageWriteGuard` can hold a page write latch while `Transaction::commit -> RecoveryManager::commit -> BufferManager::flush_all` tries to take that write latch again.
- Lower layers currently retain a broad backward edge to full transaction authority: `BufferHandle` stores `Arc<Transaction>`, and page guards / iterators own that handle for RAII unpin.
- We want compile-time protection for the dangerous write-side overlap without forcing the entire executor onto session-borrowed read lifetimes.

## What Changed In Our Understanding

The earlier version of this doc assumed we could make read and write sessions symmetric:

- `read_session()` would expose only immutable access
- `write_session()` would expose mutable access
- txn-local bookkeeping like `BufferList` could become plain unsynchronized state

That does not fit the current engine.

Read paths are not purely read-only at the transaction layer:

- `pin_read_guard` mutates txn-local pin bookkeeping
- `BufferHandle::drop` mutates it again during unpin
- `HeapIterator` can keep a pin alive across many executor calls
- one transaction can have multiple active read-side objects at once (`TableScan`, `ChunkScan`, `SortScan`, B-tree cursors, joins)

If read sessions borrowed immutable txn state and read guards / iterators also borrowed that session, executor lifetimes would spread through `Scan`, `Plan::open`, and most physical operators. That is likely too invasive and not clearly a performance win.

## Revised Goal

The refactor should target these outcomes:

1. Make `commit` / `rollback` illegal while a write guard derived from the same transaction session is alive.
2. Remove the backward edge from lower layers to full `Transaction` authority.
3. Keep the read path shareable and compatible with the current executor shape, even if that means keeping synchronized pin bookkeeping.
4. Avoid a coarse design where every read pin pays both an outer transaction lock and an inner buffer-list lock.

## Revised Architecture

### Split Transaction Authority

The implemented shape is narrower than the earlier `TransactionHandle` / `TxnReadState` / `TxnWriteState` sketch.

Today the code keeps `Transaction` as the shared outer handle and splits authority this way:

```rust
pub struct Transaction {
    write_mutex: Mutex<()>,
    pin_state: Arc<PinState>,
    // other transaction state
}

pub struct TransactionWriteSession<'a> {
    txn: &'a Arc<Transaction>,
    _write_guard: MutexGuard<'a, ()>,
}
```

The important split is semantic, not cosmetic:

- `PinState` owns shared transaction-local pin bookkeeping needed by guards and iterators.
- `TransactionWriteSession` owns exclusive transaction-local write/lifecycle authority.

### Read Path: Shared State, Synchronized Pins

The current code keeps read pinning on shared pin state rather than introducing a borrowed read-session API.

```rust
struct PinState {
    buffer_list: BufferList,
}
```

Read-side guard ownership should point only at pin state, not at the whole transaction:

```rust
pub struct BufferHandle {
    block_id: BlockId,
    pin_state: Arc<PinState>,
}
```

This keeps current executor behavior workable:

- multiple scans can stay open in one transaction
- `HeapIterator` can keep a page pinned across many `next()` calls
- lower layers no longer need `Arc<Transaction>`

### Write Path: Borrowed Session, Compile-Time Exclusion

Write operations go through an exclusive session.

```rust
impl Transaction {
    pub fn write_session(self: &Arc<Self>) -> TransactionWriteSession<'_> {
        TransactionWriteSession {
            txn: self,
            _write_guard: self.write_mutex.lock().unwrap(),
        }
    }
}

impl TransactionWriteSession<'_> {
    pub fn pin_write_guard<'a>(&'a self, block: &BlockId) -> SimpleDBResult<PageWriteGuard<'a>> {
        // write pin path
        unimplemented!()
    }

    pub fn commit(self) -> SimpleDBResult<()> {
        // flush, release locks, unpin all
        unimplemented!()
    }
}
```

The critical property is that write guards borrow from the session, and `commit(self)` consumes that same session. That makes this illegal:

```rust
let ws = txn.write_session();
let page = ws.pin_write_guard(&block)?;
ws.commit()?; // does not compile while `page` is alive
```

This is the main safety win. It addresses the concrete deadlock-producing misuse without forcing the read path into the same lifetime model.

### Intentional Asymmetry

The API should be asymmetric.

That is acceptable because the engine is already asymmetric in reality:

- write guards are the primary commit/deadlock hazard
- reads need to remain shareable and long-lived inside scans / iterators
- writes need exclusive lifecycle authority

Trying to force full symmetry between read and write sessions leads to significantly worse executor lifetimes and likely extra synchronization with little payoff.

## What This Refactor Does Not Promise

- It does not remove all interior mutability from transaction-local state.
- It does not give a compile-time error for `commit()` while a read guard or `HeapIterator` is alive, unless the executor is radically reshaped around borrowed read sessions.
- It does not make read sessions purely immutable in the sense of “no txn-local mutation happens”; read pin bookkeeping is still mutation.
- It does not use transaction locking as database concurrency control. Inter-transaction concurrency still comes from the lock table, page latches, buffer manager, and WAL synchronization.

## Backward Edge Policy

The goal is not “no backward edge at all”. The goal is “no broad backward edge to full transaction authority”.

Acceptable:

- `PageReadGuard` / `PageWriteGuard` / `HeapIterator` own a narrow handle to pin state for RAII unpin
- recovery code receives a narrow write capability that can pin pages and mark them dirty

Not acceptable:

- lower layers storing `Arc<Transaction>` and thereby retaining access to broad transaction methods

In other words, a narrow upward pointer for RAII ownership is fine. A broad upward pointer to the whole transaction object is what we want to eliminate.

## Recovery Implication

Rollback and recovery still need explicit write authority.

Today WAL undo uses a transaction-facing interface that can repeatedly:

- pin a page for write
- mutate it
- mark it dirty

The new design still needs that capability, but it should be expressed as a narrow write/recovery authority, not as “recovery can call arbitrary methods on `Arc<Transaction>`”.

This is another reason the refactor should split broad transaction authority into narrower capabilities.

## Migration Plan

1. Introduce `TransactionHandle`, `TxnReadState`, and `TxnWriteState` without changing executor call sites yet.
2. Replace `BufferHandle -> Arc<Transaction>` with `BufferHandle -> Arc<PinState>`.
3. Keep read pin/unpin on synchronized shared pin state so existing scans and iterators continue to work.
4. Introduce `TransactionWriteSession` and move `pin_write_guard`, `commit`, `rollback`, and recovery-sensitive write operations onto it.
5. Update write-side call sites to acquire a write session explicitly before pinning writable pages or finishing the transaction.
6. Replace the current rollback/recovery callback surface with a narrower write capability.
7. Remove legacy `Arc<Transaction>` write-entry APIs once callers have migrated.

## Non-Goals For This Phase

- Reworking the executor around session-borrowed read lifetimes
- Making `Plan::open` / `Scan` / `TableCursor` lifetime-parameterized
- Removing synchronized txn-local pin bookkeeping from the read path
- Redesigning logical/physical planner boundaries

## References

- Related roadmap item: [#98](https://github.com/redixhumayun/simpledb/issues/98)
- Original motivating bug: [#63](https://github.com/redixhumayun/simpledb/issues/63)
- Related architecture cleanup: [#96](https://github.com/redixhumayun/simpledb/issues/96)
