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

## Phases

### Phase 1 (this doc's implementation target)

- Logical locking: `IS`/`IX` + `IndexKey`/`IndexRange`.
- Traversal: read/write latch crabbing.
- Split handling: index split gate, with readers holding shared split gate for full scan lifetime (Option B).
- Delete/rebalance: deferred (no merge/borrow implementation in this phase).

### Phase 2 (deferred)

- Remove reader dependence on shared split gate during scans (reader-independent split invariants).
- Revisit split-gate fairness/writer starvation policy if needed.
- Implement delete underflow handling (merge/borrow/rebalance) under same concurrency model.

## Global Invariants (must hold)

- Never wait on split gate while holding page latches.
- Restart/slow-required transitions are allowed only before first page mutation/WAL emission in an attempt.
- Structural functions do not perform implicit traversal/research; caller owns restart boundaries.
- While any B-tree page latch is held, only fast resident pin paths are allowed; no replacement-policy metadata work.
- Global logical lock order is fixed and normative (table intents -> index ranges -> index keys -> row locks).

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

Range-boundary normalization requirements:

- Represent unbounded predicates explicitly (`-inf`/`+inf` sentinels), not as ad hoc `None` checks in conflict code.
- Normalize all predicates to `[low, high)` before lock acquire.
- Empty ranges (`low >= high` after normalization) acquire no range lock.
- Use the same comparator/collation as B-tree key ordering for overlap checks.

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

### 2d. Operation-to-lock mapping (compact reference)

| Operation | Logical locks | Physical latching |
|---|---|---|
| Point lookup `k=v` | table `IS`, `S(IndexKey(v))` | Read crabbing |
| Range lookup `[low, high)` | table `IS`, `S(IndexRange(low, high))` | Read crabbing |
| Point insert/delete `k=v` | table `IX`, `X(IndexKey(v))`, conflict against overlapping `IndexRange` | Write crabbing; split gate only on split path |
| Range delete/update | table `IX`, `X(IndexRange(low, high))` (+ key locks if needed by executor path) | Write crabbing |

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

### 5. Global logical lock acquisition order (required)

To avoid cross-resource deadlocks, all code paths that combine heap/index resources must follow one
global order.

- Acquire table intent locks first (`IS`/`IX`) in ascending lock-key order.
- Acquire index predicate locks next, in canonical order:
  - `IndexRange(index_id, low, high)` ascending by `(index_id, low, high)`,
  - then `IndexKey(index_id, key)` ascending by `(index_id, key)`.
- Acquire heap row locks last (`Row(table_id, block, slot)` ascending).
- If a path needs a lock that is earlier than one it already holds, release/restart; do not violate order.

Enforcement note (required):

- The order above is normative and must be enforced via a single shared lock-acquisition helper/path.
- Mixed heap+index DML code paths must not perform ad hoc lock acquisition ordering in callers.

## Required Changes: Internal B-Tree API / Latching

Goal: implement latch crabbing in B-tree code paths, independent of logical 2PL locks.

### 0. Boundary rule (must hold)

Keep traversal and structural-update concerns separated:

- No implicit traversal during structural updates.
- No structural mutation inside traversal helpers.

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
  - Required policy for this phase (Option B): readers acquire split gate in shared mode in `before_first` and hold it for the full scan lifetime (`before_first` + all `next` calls) until scan close or transaction end.
  - Split writers hold split gate exclusive for split windows.
  - This is correctness-first and simpler to encode than full reader-independent split invariants.
  - Tradeoff accepted in this phase: long scans can delay split writers; split-heavy workloads may temporarily reduce read/write overlap.
  - Re-check after escalation also serves as structural revalidation when tree shape/root-height changed while waiting for split gate.
  - Split gate lifetime must be RAII-scoped so all error/rollback paths release it deterministically.
  - Split gate fairness is not guaranteed in this phase; writer starvation under sustained split contention is accepted for now.
  - Overflow/duplicate-key edge cases must follow the same pre-mutation fit/escalation boundary (no mutation before escalation decision).
  - insert-safe: child not full
  - delete-safe: child above minimum occupancy (when delete rebalancing is introduced)
  - Global invariants above apply to all paths.

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

Ordering / safety constraints: see `Global Invariants (must hold)`.

### 4. Keep public `Index` trait stable; change internals

No public planner/scan API change is required for crabbing. Existing `Index` trait can stay:

- `before_first`
- `next`
- `get_data_rid`
- `insert`
- `delete`

Required changes are inside `BTreeIndex`, `BTreeInternal`, and `BTreeLeaf` internals.

### 5. Root metadata publication protocol (required)

Root pointer / height updates must be published with an explicit protocol.

- Source of truth is meta page block 0 (`root_block`, `tree_height`), updated via WAL-backed root-update records.
- Root update writer protocol:
  1. hold split gate exclusive,
  2. acquire meta page write latch,
  3. append root-update WAL record,
  4. write (`root_block`, `tree_height`) + checksum/LSN,
  5. release meta latch, then split gate.
- Reader protocol:
  - read `(root_block, tree_height, structure_version)` before descent,
  - on restart conditions (slow-miss restart, split-race detection, or changed structure version), restart from root using freshly read metadata.
- Metadata note:
  - Current meta `version` field is a format/layout version and must not be reused as a structure-change epoch.
  - Add a separate monotonic `structure_version` (or equivalent epoch) if version-checked restart is implemented.

### 6. `structure_version` contract (required)

Define and enforce a structural epoch used for stale-read/restart detection.

- Storage:
  - Add `structure_version: u64` to the B-tree meta page header.
  - Keep existing `version: u8` unchanged as format/layout version.
- Writer update points:
  - Increment `structure_version` on every committed structural change that can alter descent shape:
    - root replacement / root height change,
    - leaf split,
    - internal split,
    - any future merge/borrow/rebalance.
  - Do not increment for in-page non-structural mutations (insert/delete without split/merge).
- Publication:
  - `structure_version` update is part of the same WAL-backed meta update transaction as structural publish.
  - Readers must never observe new root/height with old `structure_version` (or inverse) after publish.
- Reader protocol:
  - Capture `(root_block, tree_height, structure_version)` before descent.
  - On restart boundary, reread metadata; if `structure_version` changed, restart from new root.
  - In this phase (shared split gate for scans), version-check restarts are still required for robustness.
- Overflow:
  - Use wrapping increment (`wrapping_add(1)`) for `u64`.
  - Equality compare only (`changed := new != old`); do not assume monotonic ordering across wrap.
- Recovery:
  - Recovery must restore a self-consistent tuple `(root_block, tree_height, structure_version)`.

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
  - Read path acquires split gate shared in `before_first` and holds it through scan completion (or transaction end) in this phase.
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

## Baseline Performance Reference

Full benchmark baseline details moved to:

- `docs/benchmarks/btree_concurrency_baseline.md`

Headline baseline (table-level index `S/X` locking):

- Concurrent INSERT disjoint-key: mean **57.2 elem/s**
- Concurrent LOOKUP pre-populated: mean **119.9 elem/s**
- Concurrent mixed 80/20 RW: mean **94.3 elem/s**

## Out of Scope (Phase 1)

- Delete underflow handling (`merge` / `borrow` / rebalance).
- Reader-independent split protocol (readers without shared split gate).
- Split-gate fairness redesign.

## Implementation Notes

Phase-oriented rollout:

1. Capture baseline tests/bench numbers under current table-level index locking.
2. Land internal read/write crabbing and split-gate machinery while keeping coarse entry-point `S/X`.
3. Switch logical locking to `IS`/`IX` + `IndexKey`/`IndexRange`.
4. Validate phantom semantics and contention behavior; tune only if evidence shows starvation/pathology.

Guardrail: up through step 2, correctness should remain protected by coarse entry-point serialization.

## References

- `docs/benchmarks/btree_concurrency_baseline.md` — full baseline benchmark details
- `docs/btree_buffer_locking_strategy.md` — latch/metadata lock-order constraints
- `docs/deadlock_handling.md` — deadlock prevention policy
- `docs/decisions/001-wait-die.md` — timeout-only deadlock decision
- PR #82 — heap-level row locking (prerequisite)
