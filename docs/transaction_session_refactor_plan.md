# Transaction Session Refactor Plan

Tracking issue: [#63](https://github.com/redixhumayun/simpledb/issues/63)

## Goal

Refactor transaction ownership so phase 1 transactions are effectively single-threaded per transaction, transaction-local interior mutability is removed where possible, and `commit`/`rollback` cannot overlap unsafely with live page guards.

This plan intentionally does **not** optimize for same-transaction parallel read sessions in phase 1. Cross-transaction concurrency must remain intact. Parallel same-transaction reads can be added later after the ownership model is simplified.

## Design Constraints

- Prefer `Mutex<TransactionInner>` over `RwLock<TransactionInner>` in phase 1. If same-transaction parallel reads are not supported yet, `RwLock` adds cost and misleading semantics.
- Global shared subsystems keep their own synchronization: `BufferManager`, `LockTable`, `LogManager`, and file manager internals.
- Transaction-local state should become ordinary Rust fields where exclusive session access makes that possible.
- All newly introduced data structures must be documented with Rust doc comments.
- Every phase must leave the repo in a buildable, testable state.

## Current Pressure Points

- `Transaction` exposes operational APIs through `Arc<Transaction>` and `&self`, so Rust cannot prevent `commit` from overlapping with live guards.
- `BufferHandle` owns `Arc<Transaction>` and mutates transaction state from `Drop`, extending transaction activity beyond the originating method call.
- `BufferList` and `ConcurrencyManager` rely on `RefCell`/`Cell` because transaction methods currently mutate through shared references.
- Long-lived scan/iterator objects (`TableScan`, `HeapIterator`, `RecordPage`) hold transaction-owned state across calls and therefore constrain how aggressively lifetimes can be tightened.

Relevant code:

- `src/main.rs:9566`
- `src/main.rs:9649`
- `src/main.rs:11406`
- `src/main.rs:14062`
- `src/main.rs:8480`
- `src/page.rs:2427`

## Target Shape For Phase 1

```rust
pub struct TransactionHandle {
    inner: Arc<Mutex<TransactionInner>>,
}

pub struct TransactionSession<'a> {
    guard: MutexGuard<'a, TransactionInner>,
}
```

- `TransactionHandle` is the clonable identity object.
- `TransactionSession` is the exclusive operational capability.
- `TransactionInner` owns transaction-local machinery and should expose ordinary `&mut self` methods where possible.

The important caveat: introducing the outer session lock alone is not enough. The hard primitive is replacing `BufferHandle { txn: Arc<Transaction> }` with a session-owned pin abstraction that can safely outlive a single method call without reintroducing the old aliasing model.

## Primitives To Build First

### 1. Transaction lifecycle state

Introduce a documented transaction state enum, likely something like:

```rust
enum TransactionState {
    Active,
    Committed,
    RolledBack,
    Recovering,
    Closed,
}
```

This should replace ad hoc transaction completion flags such as `BufferList::txn_committed`.

### 2. Session-oriented transaction operations trait

`RecoveryManager::{rollback,recover}` currently depend on `TransactionOperations` implemented for `Arc<Transaction>`. Replace that dependency with a trait implemented for session types so undo/recovery logic stops depending on raw shared transaction ownership.

### 3. Session-local pin abstraction

Introduce a documented primitive such as `SessionPin`, `PinnedBlock`, or similar.

Responsibilities:

- hold the pin on a block for the duration of the guard/iterator
- record pending dirty metadata if needed
- update transaction-local pin bookkeeping on drop
- avoid owning `Arc<Transaction>`

This primitive is the key enabler for removing `BufferHandle`'s dependency on shared transaction ownership.

### 4. Scan/cursor boundary updates

Long-lived scan and iterator types currently assume they can own `Arc<Transaction>` directly. Once sessions become the operational API, those objects will need to own narrower primitives (pins, block cursors, or explicit session-bound helpers) rather than the entire transaction object.

## Interior Mutability Removal Scope

### Can become ordinary fields in phase 1

Assuming same-transaction parallel reads are explicitly out of scope for phase 1:

- `BufferList.buffers: RefCell<HashMap<...>>` -> `HashMap<...>`
- `BufferList.txn_committed: Cell<bool>` -> `bool` or `TransactionState`
- `ConcurrencyManager.table_locks` -> `HashMap<...>`
- `ConcurrencyManager.row_locks` -> `HashMap<...>`
- `ConcurrencyManager.index_locks` -> `HashMap<...>`
- `BufferHandle.pending_modified: Cell<Option<...>>` -> plain `Option<...>` once the new pin primitive owns dirty-tracking responsibility

### Must remain synchronized separately

- `BufferManager`
- `LockTable`
- `LogManager`
- file manager internals
- global transaction id generation

These are cross-transaction shared resources and should not be pulled under the transaction session lock.

## Implementation Phases

### Phase 0: scope lock-in

- Document that phase 1 does not support same-transaction parallel reads.
- Prefer `Mutex<TransactionInner>` over `RwLock<TransactionInner>`.
- Keep cross-transaction concurrency semantics unchanged.

Exit criteria:

- design documented
- no code changes required yet

### Phase 1: introduce handle/inner/session shells

- Add documented `TransactionHandle`, `TransactionInner`, `TransactionSession`, and `TransactionState` types.
- Keep a temporary adapter layer so the repo still compiles while call sites are migrated incrementally.
- Begin moving lifecycle-sensitive logic behind the session boundary.

Exit criteria:

- code compiles
- no externally visible behavior regressions

### Phase 2: migrate recovery/undo to sessions

- Replace `TransactionOperations for Arc<Transaction>` with a session-oriented trait.
- Update `RecoveryManager::rollback` and `RecoveryManager::recover` to operate through sessions.
- Preserve rollback/recovery semantics before touching broader executor code.

Why this phase comes early:

- it removes a central `Arc<Transaction>` dependency with relatively contained surface area
- it validates that sessions can support undo/recovery without yet rewriting all scans

Exit criteria:

- rollback and recovery tests remain green

### Phase 3: introduce session-local pin ownership

- Add the new documented pin primitive replacing `BufferHandle { txn: Arc<Transaction> }`.
- Rework `PageReadGuard` and `PageWriteGuard` to own the new pin primitive.
- Rework `HeapIterator` to own the new pin primitive instead of a transaction-backed handle.

This is the critical phase. Without it, the session model does not actually change the ownership story.

Exit criteria:

- RAII pin tests migrated and passing
- no deadlocks introduced in iterator/scan tests

### Phase 4: remove transaction-local interior mutability

- Convert `BufferList` and `ConcurrencyManager` to plain fields and `&mut self` methods.
- Replace completion flags with explicit transaction state.
- Simplify transaction-local mutation paths to use ordinary borrowing.

Exit criteria:

- `RefCell`/`Cell` removed from transaction-local bookkeeping targeted by this refactor
- tests pass across replacement policy feature sets

### Phase 5: migrate storage and executor call sites

- Migrate `TableScan`, `RecordPage`, temp tables, planner-owned transaction references, and index paths away from direct `Arc<Transaction>` operational calls.
- Narrow long-lived state to the minimum primitive needed: session helpers, pins, or cursors.
- Keep feature set behavior identical.

This is likely the widest churn phase.

Exit criteria:

- storage/executor code no longer depends on raw `Arc<Transaction>` for operational methods
- B-tree fast pin / retry paths remain correct

### Phase 6: remove adapter layer

- Delete legacy operational APIs on `Arc<Transaction>`.
- Tighten docs and examples around session usage.
- Convert the previous runtime deadlock footgun into an impossible or unrepresentable API pattern.

Exit criteria:

- old API gone
- public transaction usage is session-oriented

## Validation After Every Phase

Run these commands after each phase and fix failures before moving on:

```bash
cargo build
cargo test --no-default-features --features replacement_lru --features page-4k
cargo test --no-default-features --features replacement_clock --features page-4k
cargo test --no-default-features --features replacement_sieve --features page-4k
cargo test --no-default-features --features replacement_lru --features page-4k --features direct-io
```

Also run formatting/lint checks before any commit boundary:

```bash
cargo clippy -- -D warnings
cargo fmt -- --check
```

Benchmark smoke checks should confirm no deadlocks or hangs after nontrivial phases:

```bash
cargo bench --bench buffer_pool --no-default-features --features replacement_lru --features page-4k
```

## Edge Cases To Preserve

- committing with live guards must stop being a valid API path
- multiple live pins to the same block in one transaction must remain correct
- late drops of guard/pin objects must have well-defined semantics during the migration
- rollback and recovery must preserve current undo behavior
- B-tree fast resident-only pin paths must still support retry-oriented traversal without deadlock
- cross-transaction concurrency tests must continue to pass

## Migration Hazards

- Replacing `BufferHandle` too late leaves the new session API mostly cosmetic.
- Replacing it too early without a narrower pin primitive can break scans/iterators that hold pins across calls.
- Using `RwLock` too early may imply parallel-read support that the rest of the design does not yet uphold.
- Preserving the old "handles may outlive commit and become no-op on drop" behavior may weaken the cleanup benefits of the new model; decide explicitly whether to preserve or tighten this contract during phase 3.

## Recommended Phase 1 Policy Decision

Phase 1 should deliberately prioritize correctness and ownership clarity over same-transaction parallelism:

- one active operational session per transaction
- ordinary mutable transaction-local fields
- explicit lifecycle state
- preserve cross-transaction concurrency
- revisit parallel same-transaction reads only after ownership and pin primitives are simpler
