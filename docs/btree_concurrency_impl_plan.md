# B-Tree Concurrency: Implementation Plan

## Current State Summary

- Index reads: `lock_table_s(index_lock_table_id)` — concurrent reads work
- Index writes: `lock_table_x(index_lock_table_id)` — all writes serialize
- No latch crabbing: one page latched at a time, released before next acquired
- No `IndexKey`/`IndexRange` lock targets exist yet
- `LockTarget` only has `Table` and `Row` variants

---

## Flagged Missing Primitives

These are required for implementation but not yet present in the codebase:

**1. `Transaction::lock_table_is` is not exposed**
`ConcurrencyManager::lock_table_is` exists (line 10431) but `Transaction` only exposes
`lock_table_ix`, `lock_table_s`, `lock_table_x`. The plan requires `lock_table_is` at
`Transaction` API level. Must add `pub fn lock_table_is`.

**2. No `IndexKey` / `IndexRange` `LockTarget` variants**
`LockTarget` (line 10292) has only `Table` and `Row`. New variants are needed:
```rust
IndexKey { index_id: u32, key: Constant },
IndexRange { index_id: u32, low: Constant, high: Constant }, // [low, high)
```
`Constant` already derives `Hash + Eq + Ord` (line 8500), so this is syntactically straightforward.

**3. `LockTable::acquire` conflict detection is equality-based only**
The HashMap keyed by `LockTarget` means conflict detection is O(1) exact-match. For
`IndexRange`, we need to detect *overlap* between intervals and between a point (`IndexKey`)
and a range (`IndexRange`). This requires iterating over all index-related entries in the map,
not just looking up by key. The acquire logic must be extended to do a linear scan over all
entries with the same `index_id` before granting an IndexKey or IndexRange lock.

**4. No `lock_index_key_s/x` / `lock_index_range_s/x` methods**
Neither `ConcurrencyManager` nor `Transaction` has these. Need to add them + tracking in
`ConcurrencyManager`'s local state (a new `index_locks` field, since `table_locks`/`row_locks`
don't cover these new targets).

**5. Latch guard path stack: `WriteCtx<'a>` with `Vec<PageWriteGuard<'a>>`**
`PageWriteGuard<'a>` carries lifetime `'a` tied to the `&'a Arc<Transaction>` borrow in
`pin_write_guard`. Since `&Arc<T>` is a shared reference, calling `pin_write_guard` multiple
times on the same `txn` borrow is valid — each returned guard shares the same `'a`. A
`Vec<PageWriteGuard<'a>>` is therefore expressible, and `WriteCtx` carries a lifetime
parameter:

```rust
struct WriteCtx<'a> {
    leaf_guard: PageWriteGuard<'a>,
    ancestor_guards: Vec<PageWriteGuard<'a>>,  // root → parent, bottom element is direct parent
}
```

`descend_write` signature:
```rust
fn descend_write<'a>(
    txn: &'a Arc<Transaction>,
    ...
) -> SimpleDBResult<WriteCtx<'a>>
```

All `structural` functions take `ctx: &mut WriteCtx<'_>`. The lifetime propagates cleanly
through the call chain. No `Vec<BlockId>` + re-latch pattern is needed; guards are held
continuously from descent through split propagation.

Consequence: `Retry` is genuinely rare — only needed when a structural race occurs after
fine-grained locking is introduced in Phase 4. In Phase 3 (coarse table-X still held), no
structural race is possible and `Retry` should never fire.

---

## Phases

### Phase 1: Lock Manager Extensions

**Goal:** Add `IndexKey`/`IndexRange` to the lock system with correct conflict semantics.

Steps:
1. Add `Transaction::lock_table_is` (trivial — delegate to `ConcurrencyManager`).
2. Add `LockTarget::IndexKey` and `LockTarget::IndexRange` variants.
3. Extend `LockTable::acquire` conflict detection:
   - For `IndexKey` request: scan all `IndexKey`/`IndexRange` entries with same `index_id`;
     apply overlap rules.
   - For `IndexRange` request: same.
   - For `Table`/`Row`: existing path unchanged.
4. Add `index_locks: RefCell<HashMap<(u32, Constant, Option<Constant>), LockMode>>` (or typed
   enum) to `ConcurrencyManager` for local tracking.
5. Add `ConcurrencyManager::lock_index_key_s/x` and `lock_index_range_s/x`.
6. Expose on `Transaction`: `lock_index_key_s/x(index_id, key)`,
   `lock_index_range_s(index_id, low, high)`, `lock_index_range_x(index_id, low, high)`.
7. Add `release()` to clear `index_locks` at commit/rollback.

**Tests:** Lock manager unit tests (per design doc §Testing §1):
- `IndexKey` vs `IndexKey`: same key conflicts S/X, different keys don't.
- `IndexRange` vs `IndexRange`: overlapping intervals conflict, disjoint don't; open-endpoint
  edge cases.
- `IndexKey` vs `IndexRange`: containment conflicts, outside doesn't.
- Boundary cases (`[20,30)` vs key `30` — no overlap).

Deliverable: all lock unit tests pass; no btree changes yet.

---

### Phase 2: Read-Path Latch Crabbing

**Goal:** Replace current single-page-at-a-time read traversal with latch-coupled descent.
Keep existing coarse `S/X` logical locking (correctness unchanged).

Steps:
1. Extract internal `mod traversal` in `btree.rs`.
2. Define `ReadCursor`:
   ```rust
   struct ReadCursor {
       leaf_block: BlockId,
       current_slot: Option<usize>,
       search_key: Constant,
   }
   ```
   Read traversal does not need to hold guards past each hop (read-latch on parent released
   as soon as child is latched), so `ReadCursor` stores `BlockId` only — no lifetime
   parameter needed.
3. Implement `descend_read(txn, root_block, ..., key) -> SimpleDBResult<ReadCursor>`:
   - Acquire read-latch on root internal node.
   - Find child block; acquire read-latch on child; release parent latch.
   - Repeat until leaf level.
   - Perform `hop_right` at leaf level (read-latch on current leaf; if search_key ≥ high_key:
     latch right sibling, release current).
4. Implement `ReadCursor::next_matching(txn, ...) -> SimpleDBResult<bool>`:
   - Read-latch current leaf, advance slot; if at end, follow sibling link (latch sibling,
     release current).
5. Refactor `BTreeIndex::before_first` and `next` to use `ReadCursor`.
6. Keep `lock_for_read()` (table-S) in place.

**Tests:** Existing concurrent read tests must continue to pass.

---

### Phase 3: Write-Path Latch Crabbing

**Goal:** Implement safe-node crabbing for insert/delete traversal, holding all ancestor
write-latches in `WriteCtx<'a>` through split propagation.

Steps:
1. Define `WriteCtx<'a>`:
   ```rust
   struct WriteCtx<'a> {
       leaf_guard: PageWriteGuard<'a>,
       // Ancestors from root downward; last element is direct parent of leaf.
       // Unsafe ancestors (those that may split) are retained; safe ancestors are dropped
       // during descent once a safe node is identified.
       ancestor_guards: Vec<PageWriteGuard<'a>>,
   }
   ```
2. Implement `descend_write<'a>(txn: &'a Arc<Transaction>, root_block, ..., key) -> SimpleDBResult<WriteCtx<'a>>`:
   - Acquire write-latch on root internal; push to `ancestor_guards`.
   - For each internal node: acquire write-latch on child.
     - If child is "safe" (not full for insert): drop all guards in `ancestor_guards`
       (releases latches on released ancestors), clear the vec, push only child.
     - If child is not safe: push child to `ancestor_guards` and continue.
   - At leaf level: the last internal guard remains in `ancestor_guards` as direct parent;
     acquire write-latch on leaf, store as `leaf_guard`.
3. Implement `structural::apply_leaf_insert(ctx: &mut WriteCtx<'_>, key, rid) -> SimpleDBResult<StructuralOutcome>`:
   - Operate on `ctx.leaf_guard`.
   - Insert at correct slot; if fits: return `Stable`. If full: split leaf, update sibling
     links, return `SplitLeaf(separator)`.
4. Implement `structural::propagate_split_up(ctx: &mut WriteCtx<'_>, split: LeafSplit) -> SimpleDBResult<PropagationOutcome>`:
   - Pop from `ctx.ancestor_guards` (bottom-up).
   - For each ancestor guard: apply separator insertion.
     - If fits: drop remaining ancestor guards (latches released), return `Absorbed`.
     - If full: split internal, produce new separator, continue popping.
   - If `ancestor_guards` is empty: return `RootSplit`.
   - No re-latching; guards are held continuously from descent.
5. Implement `structural::maybe_make_new_root(txn, root_block, split: InternalSplit)`.
6. Implement `structural::apply_leaf_delete(ctx: &mut WriteCtx<'_>, key, rid)`.
7. Refactor `BTreeIndex::insert` and `delete` to use `descend_write` + structural API.
8. Keep `lock_for_write()` (table-X) in place.

**Retry under Phase 3:** With coarse table-X still held, no concurrent writer can enter the
tree. `Retry` should be unreachable in this phase; add a `debug_assert!(false, "retry should
not fire under table-X")` for safety.

**Tests:** All existing concurrent write and split-stress tests must pass.

---

### Phase 4: Switch to IS/IX + IndexKey/IndexRange Logical Locks

**Goal:** Remove global index serialization; replace with intent + fine-grained logical locks.

Steps:
1. Replace `lock_for_read()` (table-S) with:
   - `txn.lock_table_is(index_lock_table_id)` (intent-shared)
   - `txn.lock_index_key_s(index_id, search_key)` for point reads
   - Or `txn.lock_index_range_s(index_id, low, high)` for range scans (see note below)
2. Replace `lock_for_write()` (table-X) with:
   - `txn.lock_table_ix(index_lock_table_id)` (intent-exclusive)
   - `txn.lock_index_key_x(index_id, key)` before leaf insert/delete
3. For insert: conflict against overlapping `IndexRange` locks is handled implicitly by Phase 1
   lock acquisition conflict detection.
4. `Retry` becomes load-bearing here: two concurrent writers on overlapping keys may race at
   the physical level even if the logical lock is granted (e.g., a concurrent split moved the
   key to a sibling page between descent and leaf insert). Remove the `debug_assert` from
   Phase 3; handle `Retry` with a bounded loop (`MAX_RETRIES` as in design doc sketch).

**Note — range scan lock bounds:** `before_first` takes a point key (equality lookup). Range
scan locking requires the caller to supply `[low, high)`. Since the current `Index` trait has
no range-scan method, the initial implementation uses `lock_index_key_s(key)` for point
lookups only. Range locking to be added when a range-scan API is introduced. Documented scope
boundary.

**Tests:** Phantom prevention test (per design doc §Testing §2). Write/write conflicts now
resolved by key-level locks rather than global table-X.

---

### Phase 5: Correctness and Regression Tests

**Goal:** Validate all acceptance criteria from the design doc.

Tests to add/verify:
- Phantom prevention: T1 holds `S(IndexRange[20,30))`, T2 tries to insert key 25 → T2 blocks
  until T1 commits.
- Write/write on same key: T1 and T2 both insert key 42 → one blocks, not both proceed.
- Read/write on disjoint keys: T1 reads key 10, T2 inserts key 99 → fully concurrent, no
  blocking.
- All prior concurrent tests pass.
- Run full benchmark suite; INSERT throughput should improve meaningfully vs baseline
  (~57 elem/s).

---

## What Is NOT In Scope

Per the design doc, explicitly deferred:
- Delete rebalancing (merge/redistribute) — delete-safe condition for crabbing not needed yet
- Range-scan API on `Index` trait
- Fairness/writer-preference for lock wakeup
- Wait-die deadlock prevention (stays timeout-only)
