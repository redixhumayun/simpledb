# B-Tree Concurrency Plan

## Current Status (Code-As-Of-Now)

The code is already past the state described in older notes:

- Index operations do take logical table locks on an index-specific table-id namespace.
- `BTreeIndex::before_first` acquires index table-`S`.
- `BTreeIndex::insert` / `delete` acquire index table-`X`.
- Physical page access is via per-page `RwLock` latches (`pin_read_guard` / `pin_write_guard`).

What this means today:

- Read/read can run concurrently.
- Write/write is serialized.
- Read/write is blocked by table `S` vs `X`.
- Tree traversal/update code is not yet latch-coupled (no parent-child guard handoff).

So current implementation is safe but coarse; it does not provide concurrent read+write index
access.

## Required Changes: Logical Locking Layer

Goal: keep 2PL correctness while removing global index serialization.

### 1. Switch index entry locks from `S/X` to intent locks where possible

- Read path: acquire index table `IS`.
- Write path: acquire index table `IX`.
- Keep locks until commit/rollback (unchanged 2PL lifetime).

This requires exposing `lock_table_is` at `Transaction` API level (it already exists internally
in `ConcurrencyManager`).

### 1a. Deadlock strategy for index logical locks

Use the existing project-wide policy: **timeout-only deadlock resolution**.

- Keep waiting on condvar until timeout, then return lock timeout error.
- Do not reintroduce wait-die now.
- Keep this aligned with `docs/decisions/001-wait-die.md`.

### 2. Add index-specific logical lock targets (not just table/row)

Current lock targets are only:

- `Table { table_id }`
- `Row { table_id, block, slot }`

For index concurrency, add logical targets for index contents, e.g.:

- `IndexKey { index_id, key }` for point protection.
- `IndexRange { index_id, left, right }` (or gap/next-key equivalent) for range/phantom protection.

Without these, `IS/IX` alone allows physically concurrent operations but does not fully define
serializable behavior for predicates/ranges.

### 2a. Lock model choice for this project: Option 1 (`IndexKey` + `IndexRange`)

Adopt explicit point and range lock targets:

- `IndexKey { index_id, key }`
- `IndexRange { index_id, low, high }` using canonical half-open interval semantics: `[low, high)`

Usage rules:

- Equality read (`k = v`): acquire `S` on `IndexKey(v)`.
- Equality write (insert/delete/update key `v`): acquire `X` on `IndexKey(v)`.
- Range read (`low <= k < high`): acquire `S` on `IndexRange(low, high)`.
- Range delete/update by predicate: acquire `X` on affected `IndexRange(low, high)`.
- Insert key `k`: must conflict-check against overlapping `IndexRange` locks and acquire `X` on
  `IndexKey(k)`.

Conflict checks happen during lock acquire (not as a separate post-check step) to avoid TOCTOU
races.

Why both lock types:

- `IndexKey` keeps point operations cheap and precise.
- `IndexRange` prevents phantoms (new/deleted keys entering/leaving a predicate result set).

Duplicate-key behavior:

- Current B-tree index semantics are non-unique (duplicate keys are allowed).
- Preserve this behavior.
- `IndexKey(k)` therefore protects the logical key `k` and all duplicate entries under `k`.

### 2b. Compatibility matrix (`IndexKey` / `IndexRange`)

Use standard `S`/`X` mode compatibility when lock targets overlap in keyspace:

| Held \\ Requested | S | X |
|---|---|---|
| S | Compatible | Conflict |
| X | Conflict | Conflict |

Overlap rules:

- `IndexKey(k1)` vs `IndexKey(k2)`: overlap iff `k1 == k2`.
- `IndexRange(r1)` vs `IndexRange(r2)`: overlap iff intervals intersect (respecting inclusive/exclusive bounds).
- `IndexKey(k)` vs `IndexRange(r)`: overlap iff `k` is contained in `r`.

Examples:

- `S(IndexKey(25))` with `S(IndexRange[20,30])` -> compatible.
- `X(IndexKey(25))` with `S(IndexRange[20,30])` -> conflict.
- `S(IndexKey(40))` with `X(IndexRange[20,30])` -> compatible (no overlap).
- `X(IndexRange[10,20])` with `X(IndexRange[20,30))` -> no overlap if only right endpoint is open at `20`; conflict if both include `20`.

### 2c. Isolation claim

With strict 2PL plus `IndexKey`/`IndexRange` conflicts, predicate conflicts are represented
(including phantom-producing inserts/deletes into a scanned range). Under that model, the
serializable claim is valid for indexed predicate access paths.

### 3. Keep lock namespace separation for indexes

Continue using the existing index lock key namespace:

```
index lock key = 0x4000_0000 | indexed_table_id
```

This is already implemented and should remain the logical lock identity root for index-level
targets.

### 4. Fairness / wake-up policy (deferred)

Current lock release behavior uses `notify_all()`; keep this as-is for now.

- No fairness/writer-preference redesign in this phase.
- Revisit only if starvation/contention evidence appears under real workloads.

## Required Changes: Internal B-Tree API / Latching

Goal: implement latch crabbing in B-tree code paths, independent of logical 2PL locks.

### 0. Boundary rule (must hold)

Keep traversal concerns and structural-update concerns separated in control flow:

- No implicit traversal during structural updates.
- No structural mutation inside traversal helpers.

Rationale:

- Prevents relatch races: structural code must mutate using the already-latched path, not by
  re-searching and reacquiring pages after tree state may have changed.
- Preserves latch-order predictability: traversal owns top-down latch coupling; structural code
  should not introduce ad hoc latch acquisition order.
- Makes restart behavior explicit: structural code can return `restart`/`retry` signals at clear
  boundaries rather than embedding hidden traversal side effects.

### 1. Add traversal primitives that hold parent + child temporarily

Current search/insert logic acquires a latch on one node, reads child pointer, then releases.
Crabbing needs explicit handoff:

1. latch parent
2. choose child
3. latch child
4. release parent when child is safe

This needs new internal helpers that return guard-carrying traversal state instead of plain
`BlockId`/`usize`.

### 2. Separate read traversal from write traversal with safety checks

- Read traversal: read-latch coupling down the tree with explicit fast-hit/slow-miss behavior.
  - While any B-tree latch is held, traversal may only use fast resident pin.
  - Fast hit path: pin resident child, acquire child read latch, then release parent latch and continue.
  - Fast miss path: release all held B-tree latches first, run slow/full pin path outside latch scope, then restart descent from root.
  - While latched, traversal must not attempt replacement-policy metadata operations (`record_hit`-style updates), including best-effort `try_lock` variants.
  - Missing fast-path hit accounting may degrade replacement quality; this is a performance tradeoff, not an index-correctness issue.
- Write traversal: write-latch coupling; release ancestors once child is "safe":
  - While any B-tree latch is held, traversal/structural code may only use fast resident pin.
  - Fast hit path: pin resident child/newly-needed page, acquire write latch, and continue traversal.
  - Fast miss path: release all held B-tree latches (`WriteCtx`), run slow/full pin outside latch scope, then restart from root.
  - Before any leaf/internal insert mutation, call an exact non-mutating fit check API:
    - `would_insert_fit(...) -> bool` on leaf/internal mutable views.
    - If `true`, perform normal mutation path.
    - If `false`, do not mutate; enter split escalation.
  - Split escalation (fine-grained locking target model):
    1. release all page latches,
    2. acquire index-wide split gate (exclusive) that blocks other writers for the split window,
    3. restart from root and re-check `would_insert_fit(...)`,
    4. if still not fit, perform split/propagation under split gate,
    5. release page latches, then release split gate.
  - If an escalated pass hits slow miss, release page latches and split gate, perform slow/full pin, then reacquire split gate and restart from root.
  - Split gate ordering rule: never wait on split gate while holding page latches.
  - Read path is not split-gate-blocked; readers continue via read-latch crabbing and only wait on normal page-latch contention.
  - Re-check after escalation also serves as structural revalidation when tree shape/root-height changed while waiting for split gate.
  - Split gate lifetime must be RAII-scoped so all error/rollback paths release it deterministically.
  - Structural functions must not perform implicit traversal/research; they operate only on already-latched context.
  - Pre-mutation restart rule: restart/slow-required outcomes are allowed only before first page mutation/WAL emission in an attempt.
  - Overflow/duplicate-key edge cases must follow the same pre-mutation fit/escalation boundary (no mutation before escalation decision).
  - insert-safe: child not full
  - delete-safe: child above minimum occupancy (when delete rebalancing is introduced)

### 3. Refactor split propagation to work with latch-held path context

Current split flow is recursive and reacquires pages (`insert_entry`, `split_page`,
`make_new_root`) without explicit path latch ownership.

Crabbing-friendly flow should:

- maintain an explicit path stack of latched nodes during descent,
- perform an exact pre-mutation fit check (`would_insert_fit`) before mutating leaf/internal pages,
- escalate to an index-wide split gate before any split mutation when fit check fails,
- restart from root under split gate and re-check fit before mutating,
- perform split + upward propagation while split gate is held,
- release page latches in deterministic order, then release split gate.

Ordering / safety constraints:

- never wait on split gate while holding page latches,
- restart/slow-required transitions are allowed only before first mutation/WAL emission in an attempt,
- structural functions do not perform implicit traversal; caller controls restart boundaries.

### 4. Keep public `Index` trait stable; change internals

No public planner/scan API change is required for crabbing. Existing `Index` trait can stay:

- `before_first`
- `next`
- `get_data_rid`
- `insert`
- `delete`

Required changes are inside `BTreeIndex`, `BTreeInternal`, and `BTreeLeaf` internals.

## New Code Sketches

> Note on type updates:
> Existing retry-oriented sketch/result types from older sections (for example `StructuralOutcome` and `PropagationOutcome` carrying generic `Retry` paths) should be narrowed for split-gate semantics:
> - keep explicit split results (`Absorbed`, `RootSplit`, concrete split payloads),
> - move split escalation to write-state transitions (`NeedSplitEscalation`) before mutation,
> - avoid post-mutation retry outcomes in structural/propagation result types.

### Read Traversal State Machine

```rust
// Read traversal state machine (conceptual sketch)

enum ReadState {
    Start { search_key: Constant },
    DescendFast { current: BlockId, search_key: Constant },
    HopRightFast { leaf: BlockId, search_key: Constant },
    NeedSlowPin { block: BlockId, search_key: Constant },
    RetryFromRoot { search_key: Constant },
    Ready { cursor: ReadCursor },
    Failed { err: SimpleDBError },
}

fn before_first_state_machine(
    txn: &Arc<Transaction>,
    root: BlockId,
    key: Constant,
) -> Result<ReadCursor> {
    let mut st = ReadState::Start { search_key: key };

    loop {
        st = match st {
            ReadState::Start { search_key } => {
                // logical locks acquired once before loop in real code
                ReadState::DescendFast {
                    current: root.clone(),
                    search_key,
                }
            }

            ReadState::DescendFast {
                current,
                search_key,
            } => {
                // hold read latch on `current`
                let guard = txn.pin_read_guard_fast(&current)?;
                let Some(guard) = guard else {
                    ReadState::NeedSlowPin {
                        block: current,
                        search_key,
                    }
                };
                let view = guard.into_btree_internal_page_view(...)?;

                let child = find_child(&view, &search_key)?;
                if view.btree_level() == 0 {
                    // release parent before leaf positioning
                    drop(view);
                    ReadState::HopRightFast {
                        leaf: child,
                        search_key,
                    }
                } else {
                    // crab: child fast-pin while parent latched
                    let child_guard = txn.pin_read_guard_fast(&child)?;
                    let Some(child_guard) = child_guard else {
                        drop(view); // release parent latch before slow path
                        ReadState::NeedSlowPin {
                            block: child,
                            search_key,
                        }
                    };
                    let child_view = child_guard.into_btree_internal_page_view(...)?
                    ;
                    drop(view); // release parent
                    let next_block = child_view.block_id().clone();
                    drop(child_view);
                    ReadState::DescendFast {
                        current: next_block,
                        search_key,
                    }
                }
            }

            ReadState::HopRightFast {
                mut leaf,
                search_key,
            } => {
                // hold read latch on current leaf
                let guard = txn.pin_read_guard_fast(&leaf)?;
                let Some(guard) = guard else {
                    ReadState::NeedSlowPin {
                        block: leaf,
                        search_key,
                    }
                };
                let view = guard.into_btree_leaf_page_view(...)?
                ;

                let should_hop = view.high_key().is_some_and(|hk| search_key >= hk);
                if !should_hop {
                    let slot = view.find_slot_before(&search_key);
                    drop(view);
                    ReadState::Ready {
                        cursor: ReadCursor {
                            leaf_block: leaf,
                            current_slot: slot,
                            search_key,
                        },
                    }
                } else if let Some(rsib) = view.right_sibling_block() {
                    let next = BlockId::new(..., rsib);
                    let next_guard = txn.pin_read_guard_fast(&next)?;
                    let Some(next_guard) = next_guard else {
                        drop(view); // release current leaf before slow path
                        ReadState::NeedSlowPin {
                            block: next,
                            search_key,
                        }
                    };
                    drop(next_guard); // illustrative: would continue with next as current
                    drop(view);
                    leaf = next;
                    ReadState::HopRightFast { leaf, search_key }
                } else {
                    let slot = view.find_slot_before(&search_key);
                    drop(view);
                    ReadState::Ready {
                        cursor: ReadCursor {
                            leaf_block: leaf,
                            current_slot: slot,
                            search_key,
                        },
                    }
                }
            }

            ReadState::NeedSlowPin { block, search_key } => {
                // invariant: no B-tree latches held here
                let g = txn.pin_read_guard(&block)?; // full/slow path
                drop(g); // warm resident state, then restart cleanly
                ReadState::RetryFromRoot { search_key }
            }

            ReadState::RetryFromRoot { search_key } => ReadState::DescendFast {
                current: root.clone(),
                search_key,
            },

            ReadState::Ready { cursor } => return Ok(cursor),
            ReadState::Failed { err } => return Err(err),
        };
    }
}
```

### Write Traversal State Machine

```rust
// Write traversal state machine (conceptual sketch)

enum WriteState<'a> {
    Start { key: Constant, rid: RID },
    DescendFastNormal { key: Constant, rid: RID },
    NeedSlowPinNormal { block: BlockId, key: Constant, rid: RID },
    CheckLeafFit { ctx: WriteCtx<'a>, key: Constant, rid: RID },
    NeedSplitEscalation { key: Constant, rid: RID },
    AcquireSplitGate { key: Constant, rid: RID },
    DescendFastEscalated { key: Constant, rid: RID },
    NeedSlowPinEscalated { block: BlockId, key: Constant, rid: RID },
    ApplyInsertNoSplit { ctx: WriteCtx<'a>, key: Constant, rid: RID },
    ApplySplitPropagate { ctx: WriteCtx<'a>, key: Constant, rid: RID },
    ApplyRootUpdate { split: SplitResult },
    Done,
    Failed(SimpleDBError),
}

fn insert_state_machine(index: &mut BTreeIndex, key: Constant, rid: RID) -> Result<()> {
    // Logical write locks acquired once before the loop.
    index.txn.lock_table_ix(index.index_lock_table_id)?;
    index.txn.lock_index_key_x(index.index_lock_table_id, key.clone())?;

    let mut st = WriteState::Start { key, rid };
    let mut split_gate: Option<IndexSplitGateGuard> = None;

    loop {
        st = match st {
            WriteState::Start { key, rid } => WriteState::DescendFastNormal { key, rid },

            WriteState::DescendFastNormal { key, rid } => {
                match traversal::try_descend_write_fast(
                    &index.txn,
                    &index.root_block,
                    &index.internal_layout,
                    &index.index_file_name,
                    &key,
                )? {
                    WriteTraverseOutcome::Ready(ctx) => WriteState::CheckLeafFit { ctx, key, rid },
                    WriteTraverseOutcome::NeedSlowPin(block) => {
                        WriteState::NeedSlowPinNormal { block, key, rid }
                    }
                }
            }

            WriteState::NeedSlowPinNormal { block, key, rid } => {
                // Invariant: no B-tree latches held.
                let g = index.txn.pin_write_guard(&block)?; // full/slow path
                drop(g);
                WriteState::DescendFastNormal { key, rid }
            }

            WriteState::CheckLeafFit { ctx, key, rid } => {
                if structural::leaf_would_insert_fit(&ctx, &key, rid)? {
                    WriteState::ApplyInsertNoSplit { ctx, key, rid }
                } else if split_gate.is_none() {
                    drop(ctx); // no mutation performed yet
                    WriteState::NeedSplitEscalation { key, rid }
                } else {
                    // Escalated pass and still does not fit: now run split propagation.
                    WriteState::ApplySplitPropagate { ctx, key, rid }
                }
            }

            WriteState::NeedSplitEscalation { key, rid } => {
                WriteState::AcquireSplitGate { key, rid }
            }

            WriteState::AcquireSplitGate { key, rid } => {
                // Ordering rule: never wait on split gate while holding page latches.
                split_gate = Some(index.split_gate.lock_exclusive(index.index_lock_table_id));
                WriteState::DescendFastEscalated { key, rid }
            }

            WriteState::DescendFastEscalated { key, rid } => {
                match traversal::try_descend_write_fast(
                    &index.txn,
                    &index.root_block,
                    &index.internal_layout,
                    &index.index_file_name,
                    &key,
                )? {
                    WriteTraverseOutcome::Ready(ctx) => WriteState::CheckLeafFit { ctx, key, rid },
                    WriteTraverseOutcome::NeedSlowPin(block) => {
                        WriteState::NeedSlowPinEscalated { block, key, rid }
                    }
                }
            }

            WriteState::NeedSlowPinEscalated { block, key, rid } => {
                // Page latches are not held here.
                // Policy: do not hold split gate across slow I/O work.
                split_gate = None;
                let g = index.txn.pin_write_guard(&block)?; // full/slow path
                drop(g);
                WriteState::AcquireSplitGate { key, rid }
            }

            WriteState::ApplyInsertNoSplit { mut ctx, key, rid } => {
                structural::apply_leaf_insert_no_split(&mut ctx, &index.txn, &index.leaf_layout, key, rid)?;
                drop(ctx);
                split_gate = None;
                WriteState::Done
            }

            WriteState::ApplySplitPropagate { mut ctx, key, rid } => {
                let maybe_root_split = structural::apply_leaf_split_and_propagate(
                    &mut ctx,
                    &index.txn,
                    &index.leaf_layout,
                    &index.internal_layout,
                    &index.index_file_name,
                    key,
                    rid,
                )?;
                drop(ctx);
                if let Some(split) = maybe_root_split {
                    WriteState::ApplyRootUpdate { split }
                } else {
                    split_gate = None;
                    WriteState::Done
                }
            }

            WriteState::ApplyRootUpdate { split } => {
                let new_root = structural::maybe_make_new_root(
                    &index.txn,
                    index.tree_height as u8,
                    split,
                    &index.index_file_name,
                    &index.internal_layout,
                )?;
                let old_root = index.root_block.block_num;
                let old_height = index.tree_height;
                index.apply_root_update(
                    old_root,
                    new_root.block_num,
                    old_height,
                    old_height.saturating_add(1),
                )?;
                split_gate = None;
                WriteState::Done
            }

            WriteState::Done => return Ok(()),
            WriteState::Failed(err) => return Err(err.into()),
        };
    }
}

// Post-mutation rule:
// After first WAL/page mutation in an attempt, this machine must not transition
// to NeedSlow* or retry states; only complete or fail/rollback.
//
// Retry budget rule:
// If bounded retries are exhausted, return an operation error and rollback the
// transaction. Because retries occur only pre-mutation, no partial structural
// mutation from the failed attempt should remain.
```

### Code Sketches

This section is kept for reference to earlier design direction; prefer `New Code Sketches` above for current implementation-oriented state-machine flow.

This section is illustrative. Names/signatures may change during implementation, but boundary
rules and ownership intent should remain.

#### A. Internal module shape

```text
btree.rs
  mod traversal {
    // latch protocol only
    struct ReadCursor { ... }
    struct WriteCtx { ... }

    fn descend_read(...) -> ReadCursor
    fn descend_write(...) -> WriteCtx
    fn hop_right_read(cursor: &mut ReadCursor, ...) -> Result<...>
  }

  mod structural {
    // page mutation + split propagation only
    struct Separator {
      key: Constant,
      left_block: usize,
      right_block: usize,
    }

    struct LeafSplit(Separator);
    struct InternalSplit(Separator);

    enum StructuralOutcome {
      Stable,
      Retry,
      SplitLeaf(LeafSplit),
      SplitInternal(InternalSplit),
    }

    enum PropagationOutcome {
      Absorbed,
      Retry,
      RootSplit(InternalSplit),
    }

    fn apply_leaf_insert(
      ctx: &mut WriteCtx,
      key: &Constant,
      rid: RID
    ) -> Result<StructuralOutcome>
    fn apply_internal_insert(
      ctx: &mut WriteCtx,
      split: InternalSplit
    ) -> Result<StructuralOutcome>
    fn apply_leaf_delete(ctx: &mut WriteCtx, key: &Constant, rid: RID) -> Result<...>
    fn propagate_split_up(ctx: &mut WriteCtx, split: LeafSplit) -> Result<PropagationOutcome>
    fn maybe_make_new_root(...) -> Result<...>
  }
```

`BTreeInternal` / `BTreeLeaf` remain page-level helpers (slot ops, split primitives, header
updates). They do not own top-down traversal policy.

#### B. Core internal types

```text
ReadCursor
  - current_leaf_block: BlockId
  - current_slot: Option<usize>
  - search_key: Constant
  - (optional) cached leaf read-guard while scanning

WriteCtx
  - leaf_block: BlockId
  - ancestor_path: Vec<BlockId>   // or latched ancestor handles
  - op: Insert | Delete
  - key: Constant
```

Implementation may store full guards instead of `BlockId`s where lifetimes permit. If not, store
stable path metadata plus explicit relatch/revalidate steps.

Type boundary between traversal and structural:

- `traversal::descend_read(...) -> ReadCursor`: consumed by read orchestration (`before_first`,
  `next`, sibling hops). Not passed to structural mutation APIs.
- `traversal::descend_write(...) -> WriteCtx`: handoff object passed into structural APIs for
  insert/delete/split propagation.

Split outcome boundary:

- Keep split kinds explicit at the structural API boundary:
  - `SplitLeaf(LeafSplit)`
  - `SplitInternal(InternalSplit)`
- Distinguish root-creating propagation explicitly via `PropagationOutcome::RootSplit(...)`.
- Carry transient restart explicitly as `Retry` in both structural and propagation outcomes.
- This improves API readability and makes cascade handling explicit in the type system.

#### C. `BTreeIndex` control-flow sketch

```rust
impl Index for BTreeIndex {
    fn before_first(&mut self, key: &Constant) {
        self.txn.lock_table_is(self.index_lock_table_id).unwrap();
        self.read_cursor = Some(traversal::descend_read(
            &self.txn,
            &self.root_block,
            &self.internal_layout,
            &self.leaf_layout,
            &self.index_file_name,
            key,
        ).unwrap());
    }

    fn next(&mut self) -> bool {
        let cursor = self.read_cursor.as_mut().expect("before_first first");
        cursor.next_matching(
            &self.txn,
            &self.leaf_layout,
            &self.index_file_name,
        ).unwrap_or(false)
    }

    fn insert(&mut self, key: &Constant, rid: &RID) {
        self.txn.lock_table_ix(self.index_lock_table_id).unwrap();
        const MAX_RETRIES: usize = 16;
        for _attempt in 0..MAX_RETRIES {
            let mut ctx = traversal::descend_write(/*...*/, key, /*Insert*/).unwrap();
            match structural::apply_leaf_insert(&mut ctx, key, *rid).unwrap() {
                structural::StructuralOutcome::Stable => return,
                structural::StructuralOutcome::Retry => continue,
                structural::StructuralOutcome::SplitLeaf(leaf_split) => {
                    match structural::propagate_split_up(&mut ctx, leaf_split).unwrap() {
                        structural::PropagationOutcome::Absorbed => return,
                        structural::PropagationOutcome::Retry => continue,
                        structural::PropagationOutcome::RootSplit(root_split) => {
                            structural::maybe_make_new_root(/*...*/, root_split).unwrap();
                            return;
                        }
                    }
                }
                structural::StructuralOutcome::SplitInternal(_) => {
                    unreachable!("leaf insert should not directly yield internal split")
                }
            }
        }
        panic!("btree insert exceeded retry budget");
    }

    fn delete(&mut self, key: &Constant, rid: &RID) {
        self.txn.lock_table_ix(self.index_lock_table_id).unwrap();
        let mut ctx = traversal::descend_write(/*...*/, key, /*Delete*/).unwrap();
        structural::apply_leaf_delete(&mut ctx, key, *rid).unwrap();
    }
}
```

## Acceptance Criteria

- Logical layer:
  - Index reads use `IS`; writes use `IX` plus appropriate fine-grained index locks.
  - No global table-`X` requirement for ordinary index writes.
- Physical layer:
  - Read traversal uses latch coupling.
  - Write traversal uses safe-node latch crabbing.
  - Split path does not rely on unlatch/re-latch races for parent updates.
- Behavior:
  - concurrent read+read allowed,
  - concurrent read+write allowed where key/range locks permit,
  - write/write conflicts are resolved by logical key/range locks (not global index `X`),
  - existing single-thread tests and new concurrent index tests pass.

## Testing Scope

### 1. Lock manager unit tests (near `LockTable`)

Add focused tests for:

- `IndexKey` vs `IndexKey` overlap and `S/X` compatibility.
- `IndexRange` vs `IndexRange` interval-overlap behavior under `[low, high)`.
- `IndexKey` vs `IndexRange` containment/compatibility.
- Boundary cases around interval edges.

These tests validate lock semantics directly, independent of B-tree structure.

### 2. End-to-end B-tree/integration tests

Add correctness tests for phantom behavior and predicate stability across transactions.

Minimum required phantom test scenario:

1. `T1` starts and reads range `[20,30)` (holding the corresponding logical range lock) and records count `n`.
2. `T2` attempts to insert key `25` and commit.
3. `T1` re-reads the same range `[20,30)` in the same transaction.
4. Assert no phantom in `T1`:
   - either `T2` is blocked/aborted until `T1` ends, or
   - `T1`'s second read still returns count `n` before `T1` commits.

Important: existing concurrent B-tree tests and 80/20 RW benchmark are useful stress/smoke
coverage, but they do not by themselves prove `IndexKey`/`IndexRange` logical lock semantics
or phantom prevention guarantees.

### 3. State-Machine / Escalation Tests

Add targeted tests for new write/read state-machine semantics:

- Pre-mutation split escalation:
  - insertion that would split must escalate before first mutation/WAL in that attempt.
- Escalated slow miss policy:
  - when escalated path hits slow miss, split gate is released, slow pin runs, split gate is re-acquired, and operation restarts from root.
- Revalidation after split-gate wait:
  - writer blocked on split gate must restart from root and re-check fit/path after acquiring gate.
- Split gate ordering:
  - no code path waits on split gate while holding page latches.
- Post-mutation no-retry invariant:
  - once first mutation/WAL is emitted in an attempt, state machine does not transition to retry/NeedSlow outcomes.
- Duplicate-key/overflow escalation edge:
  - duplicate-key insert paths that trigger overflow/split must still obey pre-mutation escalation boundary.

## Baseline: Index Concurrency Benchmark (table-level locking)

These numbers represent the performance floor before latch crabbing or fine-grained logical
locking is introduced. Use them as the comparison reference when evaluating improvements or
regressions.

### How to reproduce

```bash
# From the repository root, on the same commit that introduced the benchmark:
CI=1 cargo bench --bench simple_bench \
    --no-default-features --features replacement_lru --features page-4k \
    -- "Index Concurrency"
```

`CI=1` activates the fast criterion profile (1 s warmup, 5 s measurement, 100 samples). Remove
it for a full run (~165 s per benchmark function at the current workload size).

### Workload parameters

| Parameter | Value |
|---|---|
| Workers (`CONC_WORKERS`) | 4 |
| Ops per worker (`CONC_OPS_PER_WORKER`) | 24 |
| Total elements per iteration | 96 |
| Pre-populated rows (lookup baseline) | 200 |
| Per-op transaction granularity | 1 txn per op (create `BTreeIndex` → op → commit) |
| Locking model | table-level `S` (read) / `X` (write) on `index_lock_table_id` |

### Environment

| Field | Value |
|---|---|
| CPU | Intel Xeon E3-1275 v5 @ 3.60 GHz (8 logical cores) |
| OS kernel | Linux 6.8.0-100-generic |
| Rust toolchain | rustc 1.92.0 (ded5c06cf 2025-12-08) |
| Cargo | 1.92.0 (344c4567c 2025-10-21) |
| Feature flags | `replacement_lru`, `page-4k` |
| Criterion profile | `CI=1` (1 s warmup, 5 s measurement, 100 samples) |

### Results (branch `feature/btree-concurrency`, tree contains uncommitted changes on top of b8274f6)

The benchmark code itself is part of the uncommitted diff; commit hash `b8274f6` is the last
committed ancestor. Once this branch is merged, the commit SHA of the merge should be recorded
here.

| Benchmark | Time / iter (low–high, 95% CI) | Throughput (elem/s) |
|---|---|---|
| Concurrent INSERT disjoint-key | 1.654 s – 1.711 s | 56.1 – 58.0 (mean **57.2**) |
| Concurrent LOOKUP pre-populated | 797 ms – 804 ms | 119.4 – 120.4 (mean **119.9**) |
| Concurrent mixed 80/20 RW | 984 ms – 1.062 s | 90.4 – 97.6 (mean **94.3**) |

### Interpretation

- **INSERT is the slowest** (~57 elem/s) because every insert must acquire a global index
  table-`X` lock, serializing all 4 workers. Effective throughput == single-writer throughput.
- **LOOKUP is fastest** (~120 elem/s) because multiple readers can hold `S` simultaneously;
  4 workers genuinely overlap.
- **Mixed 80/20** sits between the two: the 20% write fraction introduces table-`X` interludes
  that block the concurrent readers.

After latch crabbing + `IS`/`IX` / `IndexKey`/`IndexRange` logical locking:

- INSERT throughput should scale toward multi-writer concurrency (expected significant gain).
- LOOKUP throughput should remain roughly the same or improve slightly (already concurrent).
- Mixed throughput should approach LOOKUP levels as write-induced serialization decreases.

## Implementation Notes

Use staged rollout to preserve stability while refactoring:

1. Add required tests/benchmarks first and capture baseline numbers under current serialized index
   locking.
2. Implement read-path refactor first.
3. Implement write-path refactor next.
4. Enable/complete latch-crabbing behavior internally while keeping current serialized logical
   locking in place.
5. Remove serialized index locking last (`S/X` at index entry points), switching to `IS/IX` plus
   `IndexKey`/`IndexRange` logical locks.

Guardrails for this rollout:

- Up through step 4, existing correctness should continue to pass because coarse serialized
  logical locks are still active.
- Phantom/key-range semantic tests may be added early but should be expected to fail (or be
  ignored) until step 5 introduces `IndexKey`/`IndexRange` locking.

## References

- `docs/lock_granularity_refactor_notes.md` — lock granularity design
- `docs/deadlock_handling.md` — deadlock prevention policy
- `docs/decisions/001-wait-die.md` — timeout-only deadlock decision
- PR #82 — heap-level row locking (prerequisite)
