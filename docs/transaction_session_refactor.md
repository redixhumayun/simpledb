# Transaction Session Refactor

Tracking Issue: [#63](https://github.com/redixhumayun/simpledb/issues/63)

## Motivation
- `transaction_tests::test_transaction_multi_threaded_single_reader_single_writer` now deadlocks because `PageWriteGuard` holds `RwLockWriteGuard<Page>` across `Transaction::commit`, and `RecoveryManager::commit -> BufferManager::flush_all` tries to grab the same write lock again.
- Root cause: the `Transaction` API exposes everything through `Arc<Transaction>` and `&self`, so Rust cannot prevent overlapping lifetimes between guard objects and `commit/rollback`. Guard discipline is entirely manual; forgetting to scope a guard leads to hangs.
- We also paid for widespread interior mutability (`RefCell`, `Cell`, `Arc<Mutex<_>>`) solely because methods only take shared references. This obscures ownership, complicates reasoning about aliasing, and forces runtime checks instead of compile-time guarantees.
- Enforcing RAII guard scoping via the type system eliminates this deadlock class, simplifies Transaction internals, and sets the stage for future intra-query parallelism without reintroducing `set_int/get_int` style helper APIs.

## Current Design (Problems)
- Callers clone `Arc<Transaction>` into threads and invoke `pin_read_guard` / `pin_write_guard` / `commit` directly on `&Arc<Transaction>`.
- `BufferHandle` stores its own `Arc<Transaction>` to call `pin_internal/unpin_internal` in `Drop`, so buffer pins live even after `Transaction` methods return.
- Nothing prevents a write guard from living longer than mutation scopes; tests currently hold guards through `commit`.
- `BufferList.buffers`, `ConcurrencyManager.locks`, and similar fields rely on `RefCell`/`Cell` to mutate behind `&self`, adding runtime borrow checks and making aliasing implicit.

## Proposed Architecture

### TransactionHandle + Sessions
```rust
pub struct TransactionHandle(Arc<RwLock<TransactionInner>>);

impl TransactionHandle {
    pub fn read_session(&self) -> TransactionReadSession<'_> {
        TransactionReadSession { guard: self.0.read().unwrap() }
    }

    pub fn write_session(&self) -> TransactionWriteSession<'_> {
        TransactionWriteSession { guard: self.0.write().unwrap() }
    }
}

pub struct TransactionReadSession<'a> {
    guard: RwLockReadGuard<'a, TransactionInner>,
}

pub struct TransactionWriteSession<'a> {
    guard: RwLockWriteGuard<'a, TransactionInner>,
}
```
- Read sessions expose purely `&TransactionInner` APIs (pin read guard, catalog ops) and can coexist across threads.
- Write sessions expose `&'a mut TransactionInner` so mutation helpers require exclusive borrows, enforced by Rust.

### Guard APIs require `&mut self`
```rust
impl TransactionInner {
    pub fn pin_write_guard<'a>(&'a mut self, block: &BlockId) -> PageWriteGuard<'a> {
        self.concurrency_manager.xlock(block)?;
        let handle = BufferHandle::new(self, block.clone());
        PageWriteGuard::new(handle, ...)
    }

    pub fn commit(&mut self) -> Result<()> {
        self.recovery_manager.commit();
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all();
        Ok(())
    }
}
```
- Because `pin_write_guard` borrows `&mut self`, the compiler will not allow `commit(&mut self)` (or consumption of the write session) while any guard derived from that mutable borrow is alive.
- `BufferHandle` now stores `&'a mut TransactionInner` instead of `Arc<Transaction>`, so dropping the handle automatically releases the mutable borrow when it unpins.

### Guard Lifetimes enforce RAII
```rust
let tx = Arc::new(TransactionHandle::new(...));
let writer = tx.clone();
std::thread::spawn(move || {
    let mut session = writer.write_session();
    {
        let mut page = session.pin_write_guard(&block);
        page.set_int(80, 1);
        page.mark_modified(session.txn_id(), Lsn::MAX);
    } // guard drops, mutable borrow released
    session.commit().unwrap(); // now allowed
});
```
- Attempting to call `session.commit()` while `page` is still alive fails to compile, removing the previously silent deadlock path.

### Simplifying TransactionInner state
With exclusive access guaranteed:
- Replace `BufferList.buffers: RefCell<HashMap<...>>` with a plain `HashMap`; tracking pins no longer needs runtime borrow checks.
- `ConcurrencyManager.locks` can drop `RefCell` and use `HashMap` directly.
- `BufferHandle` avoids cloning `Arc<Transaction>` and no longer risks use-after-free of transaction internals.
- Shared subsystems (`BufferManager`, `FileManager`, `LogManager`) keep their own synchronization because they are cross-transaction resources.

### Parallelism Story
- Read-heavy workloads: multiple threads call `read_session()` simultaneously and pin pages without blocking each other.
- Writers: still serialize on the write session (matching strict 2PL requirements), but guard scoping ensures flushing can never deadlock.
- Future intra-query parallelism splits scans into read sessions; write sessions remain short-lived for mutation phases.

## Migration Plan
1. Introduce `TransactionHandle`, `TransactionInner`, and session types while keeping existing API behind feature flag or adapter.
2. Update guard constructors to require `&mut TransactionInner` and propagate lifetimes through `BufferHandle`, `PageReadGuard`, `PageWriteGuard`.
3. Refactor tests and call sites to obtain sessions before pinning/committing; enforce guard scoping idioms in documentation.
4. Remove interior-mutability wrappers (`RefCell`, `Cell`) that become redundant once exclusive borrows exist.
5. Delete legacy `Transaction` methods that take `Arc<Self>` directly and update docs/examples.

## Open Questions
- Should `read_session()` allow pinning multiple blocks simultaneously, or do we expose a finer-grained borrow (e.g., `PageReadCursor`) to reduce lock hold time?
- Do we need a `try_write_session()` to avoid blocking intra-query parallel operators, or is blocking acceptable under strict 2PL?
- How do we stage the migration without breaking existing public API consumers? (Option: keep `Arc<Transaction>` facade that internally acquires sessions.)

## References
- Deadlock surfaced in `transaction_tests::test_transaction_multi_threaded_single_reader_single_writer` after guard/view migration.
- Related roadmap items: Intra-query parallelism (#32), Read/Write handle design (#29).
