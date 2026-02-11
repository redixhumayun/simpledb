# WAL Logging Integration with Typed Page Views

## Overview

This document outlines the integration of write-ahead logging (WAL) with the new typed page view architecture. Currently, page mutations through `HeapPageViewMut`, `BTreeLeafPageViewMut`, and `BTreeInternalPageViewMut` mark pages as dirty but **do not log changes**, breaking crash recovery guarantees.

## Problem Statement

### Current State

**Legacy path (with WAL logging)**:
```rust
Transaction::set_int(block_id, offset, value) {
    concurrency_manager.xlock(block_id);
    let old_value = page.get_int(offset);
    let lsn = recovery_manager.set_int(block_id, offset, old_value);  // ← Logs change
    page.set_int(offset, value);
    page.mark_modified(txn_id, lsn);
}
```

**New view-based path (no WAL logging)**:
```rust
RecordPage::set_int(slot, field, value) {
    let guard = txn.pin_write_guard(&block_id);
    let mut view = guard.into_heap_view_mut(&layout);
    view.row_mut(slot).set_column(field, &Constant::Int(value));  // ← No logging!
    // HeapPageViewMut::drop marks dirty but has no LSN
}
```

### Consequences

1. **No durability**: Changes are not logged to WAL, so crash recovery cannot replay them
2. **No undo**: Rollback cannot revert uncommitted changes (no before-images in log)
3. **Checkpoint/flush unsafe**: Dirty pages can reach disk without corresponding WAL records, violating WAL protocol
4. **MVCC incomplete**: HeapTuple has `xmin`/`xmax` fields but no LSN tracking for ARIES-style recovery

## Design Goals

1. **Minimal overhead**: Logging should not duplicate work or add unnecessary allocations
2. **Type safety**: Leverage page kinds to determine what needs logging
3. **Composability**: Support logging for complex operations (insert → update → redirect)
4. **Testability**: Easy to verify logging correctness in unit tests
5. **Compatibility**: Work with existing `RecoveryManager` and log record formats

## Design Decisions

### Physical Tuple-Level Logging

We use **physical tuple-level logging** rather than logical (schema-aware) logging.

**What this means**: Log records contain raw tuple bytes, not field names and typed values.

```rust
// Physical tuple-level (our approach)
LogRecord::TupleUpdate {
    block_id: ("test.db", 5),
    slot: 3,
    old_tuple: vec![0x00, 0x00, ...],  // full tuple bytes before
    new_tuple: vec![0x00, 0x00, ...],  // full tuple bytes after
}

// Logical (NOT our approach)
LogRecord::SetColumn {
    block_id: ("test.db", 5),
    slot: 3,
    field: "age",
    old_value: Constant::Int(25),
    new_value: Constant::Int(30),
}
```

**Rationale**:
1. **Varlength-ready**: When tuple size changes (varlength fields), physical logging still works—just log the new tuple bytes. Logical logging at field granularity breaks when fields change size.
2. **Schema-independent recovery**: Redo/undo just copies bytes; no need for schema/layout at recovery time.
3. **PostgreSQL precedent**: PostgreSQL uses physical tuple images in their heap WAL records.

**Trade-offs**:
- Larger log records (full tuple × 2 vs just changed field)
- Less human-readable logs
- Can't optimize multi-field updates (each update logs full tuple)

### LSN Placement: Page Header

Store LSN in the **page header**, not per-tuple.

**Rationale**:
1. Simpler implementation—one LSN per page
2. Sufficient for our recovery model
3. `BTreeLeafHeaderMut` already has `set_lsn()` at bytes 24-31
4. `HeapHeaderMut` has reserved bytes 26-31 available for LSN

## Proposed Architecture

### 1. Extend PageViewMut with Logging Context

```rust
pub struct HeapPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    layout: &'a Layout,
    dirty: Rc<Cell<bool>>,
    log_context: Option<LogContext>,  // ← For creating RowLogContext
    page_lsn: Rc<Cell<Option<Lsn>>>,  // ← Tracks max LSN across row ops
}

/// Logging context for WAL integration.
/// Owned (not references) to avoid lifetime complexity.
/// Held by HeapPageViewMut, used to create RowLogContext for each row_mut() call.
struct LogContext {
    log_manager: Arc<Mutex<LogManager>>,
    block_id: BlockId,
    txn_id: usize,
}

impl LogContext {
    fn new(
        log_manager: Arc<Mutex<LogManager>>,
        block_id: BlockId,
        txn_id: usize,
    ) -> Self {
        Self {
            log_manager,
            block_id,
            txn_id,
        }
    }
}
```

### 1b. LogContext Creation Flow

`Transaction` creates the `LogContext` and attaches it to `PageWriteGuard`:

```rust
impl Transaction {
    pub fn pin_write_guard(&self, block_id: &BlockId) -> PageWriteGuard<'_> {
        let handle = self.buffer_list.pin_for_write(block_id);
        // ... existing guard creation ...

        // Attach logging context to the guard
        let log_ctx = LogContext::new(
            Arc::clone(&self.log_manager),
            block_id.clone(),
            self.tx_id as usize,
        );
        guard.set_log_context(log_ctx);
        guard
    }
}

impl PageWriteGuard<'_> {
    /// Store log context for later use by views
    pub fn set_log_context(&mut self, ctx: LogContext) {
        self.log_context = Some(ctx);
    }

    /// Transfer log context to HeapPageViewMut
    pub fn into_heap_view_mut(mut self, layout: &Layout) -> SimpleDBResult<HeapPageViewMut<'_>> {
        let log_ctx = self.log_context.take();  // move ownership to view
        HeapPageViewMut::new(self, layout, log_ctx)
    }
}
```

This way logging is automatic—callers don't need to wire it up manually.

### 2. Physical Tuple Operations

With physical tuple-level logging, operations capture raw bytes before/after mutation:

```rust
/// Captures tuple state for physical logging
struct TupleSnapshot {
    slot: SlotId,
    offset: u16,           // tuple offset in page
    bytes: Vec<u8>,        // full tuple bytes including header
}

impl TupleSnapshot {
    /// Capture current tuple state before mutation
    fn capture(page: &HeapPage, slot: SlotId) -> Option<Self> {
        let line_ptr = page.line_ptr(slot)?;
        let bytes = page.tuple_bytes(slot)?.to_vec();
        Some(Self {
            slot,
            offset: line_ptr.offset(),
            bytes,
        })
    }
}
```

Redo/undo operations are simple byte copies:

```rust
impl LogRecord {
    /// Redo: restore new tuple bytes
    fn redo_tuple_update(&self, page_bytes: &mut [u8]) {
        if let LogRecord::HeapTupleUpdate { slot, new_offset, new_tuple, .. } = self {
            // Write new tuple bytes at new_offset
            let end = *new_offset as usize + new_tuple.len();
            page_bytes[*new_offset as usize..end].copy_from_slice(new_tuple);
            // Update line pointer to point to new location
        }
    }

    /// Undo: restore old tuple bytes
    fn undo_tuple_update(&self, page_bytes: &mut [u8]) {
        if let LogRecord::HeapTupleUpdate { slot, old_offset, old_tuple, .. } = self {
            // Write old tuple bytes at old_offset
            let end = *old_offset as usize + old_tuple.len();
            page_bytes[*old_offset as usize..end].copy_from_slice(old_tuple);
            // Update line pointer to point to old location
        }
    }
}
```

### 3. Log on Row Drop

Each row mutation is logged immediately when `LogicalRowMut` is dropped. This allows multiple tuples to be modified per view.

```rust
/// Context passed to LogicalRowMut for logging on drop
struct RowLogContext {
    log_manager: Arc<Mutex<LogManager>>,
    block_id: BlockId,
    txn_id: usize,
    slot: SlotId,
    before_image: TupleSnapshot,
}

pub struct LogicalRowMut<'a> {
    tuple: HeapTupleMut<'a>,
    layout: Layout,
    dirty: Rc<Cell<bool>>,
    row_log_ctx: Option<RowLogContext>,  // ← logging context for this row
    page_lsn: Rc<Cell<Option<Lsn>>>,     // ← shared with HeapPageViewMut
}

impl<'a> HeapPageViewMut<'a> {
    fn new(
        guard: PageWriteGuard<'a>,
        layout: &'a Layout,
        log_context: Option<LogContext>,
    ) -> SimpleDBResult<Self> {
        HeapPageMut::new(guard.bytes_mut())?;
        Ok(Self {
            guard,
            layout,
            dirty: Rc::new(Cell::new(false)),
            log_context,
            page_lsn: Rc::new(Cell::new(None)),  // track max LSN across all row ops
        })
    }

    /// Returns a mutable row, capturing tuple state for WAL logging
    pub fn row_mut(&mut self, slot: SlotId) -> Option<LogicalRowMut<'_>> {
        // Capture before-image for THIS row
        let row_log_ctx = if let Some(log_ctx) = &self.log_context {
            let page = self.build_page();
            TupleSnapshot::capture(&page, slot).map(|before| RowLogContext {
                log_manager: Arc::clone(&log_ctx.log_manager),
                block_id: log_ctx.block_id.clone(),
                txn_id: log_ctx.txn_id,
                slot,
                before_image: before,
            })
        } else {
            None
        };

        let layout_clone = self.layout.clone();
        let dirty = self.dirty.clone();
        let page_lsn = self.page_lsn.clone();
        let heap_tuple_mut = self.resolve_live_tuple_mut(slot)?;

        Some(LogicalRowMut {
            tuple: heap_tuple_mut,
            layout: layout_clone,
            dirty,
            row_log_ctx,
            page_lsn,
        })
    }
}

impl Drop for LogicalRowMut<'_> {
    fn drop(&mut self) {
        if self.dirty.get() {
            if let Some(ctx) = self.row_log_ctx.take() {
                // Capture after-image
                let after_bytes = self.tuple.as_bytes().to_vec();

                let record = LogRecord::HeapTupleUpdate {
                    txnum: ctx.txn_id,
                    block_id: ctx.block_id,
                    slot: ctx.slot,
                    old_offset: ctx.before_image.offset,
                    old_tuple: ctx.before_image.bytes,
                    new_offset: ctx.before_image.offset,  // same for in-place
                    new_tuple: after_bytes,
                };

                if let Ok(lsn) = record.write_log_record(ctx.log_manager) {
                    // Update shared page LSN (keep max)
                    let current = self.page_lsn.get().unwrap_or(0);
                    if lsn > current {
                        self.page_lsn.set(Some(lsn));
                    }
                }
            }
        }
    }
}

impl Drop for HeapPageViewMut<'_> {
    fn drop(&mut self) {
        if self.dirty.get() {
            // Use the max LSN from all row operations
            let lsn = self.page_lsn.get().unwrap_or(Lsn::MAX);
            self.guard.mark_modified(self.guard.txn_id(), lsn);
        }
    }
}
```

This design supports multiple row modifications per view:
```rust
let mut view = guard.into_heap_view_mut(&layout);

let row1 = view.row_mut(0);
row1.set_column("a", 1);
drop(row1);  // logs HeapTupleUpdate for slot 0 ✓

let row2 = view.row_mut(1);
row2.set_column("b", 2);
drop(row2);  // logs HeapTupleUpdate for slot 1 ✓

drop(view);  // marks page modified with max LSN from both ops
```

### 4. Update RecoveryManager

Physical logging simplifies redo/undo—just byte copies:

```rust
impl RecoveryManager {
    /// Redo: apply physical tuple change during crash recovery
    fn redo(&self, txn: &dyn TransactionOperations, log_record: &LogRecord) -> Result<(), Box<dyn Error>> {
        match log_record {
            LogRecord::HeapTupleInsert { block_id, slot, offset, tuple } => {
                let mut guard = txn.pin_write_guard(block_id);
                let bytes = guard.bytes_mut();
                // Write tuple bytes at offset
                let end = *offset as usize + tuple.len();
                bytes[*offset as usize..end].copy_from_slice(tuple);
                // Update line pointer for slot
                // ...
            }
            LogRecord::HeapTupleUpdate { block_id, slot, new_offset, new_tuple, .. } => {
                let mut guard = txn.pin_write_guard(block_id);
                let bytes = guard.bytes_mut();
                // Write new tuple bytes
                let end = *new_offset as usize + new_tuple.len();
                bytes[*new_offset as usize..end].copy_from_slice(new_tuple);
            }
            LogRecord::HeapTupleDelete { block_id, slot, .. } => {
                let mut guard = txn.pin_write_guard(block_id);
                // Mark line pointer as dead
                // ...
            }
            _ => {}
        }
        Ok(())
    }

    /// Undo: restore old tuple bytes during rollback
    fn undo(&self, txn: &dyn TransactionOperations, log_record: &LogRecord) {
        match log_record {
            LogRecord::HeapTupleInsert { block_id, slot, .. } => {
                // Undo insert = mark slot as free
                let mut guard = txn.pin_write_guard(block_id);
                // Mark line pointer as free
                // ...
            }
            LogRecord::HeapTupleUpdate { block_id, slot, old_offset, old_tuple, .. } => {
                // Undo update = restore old bytes
                let mut guard = txn.pin_write_guard(block_id);
                let bytes = guard.bytes_mut();
                let end = *old_offset as usize + old_tuple.len();
                bytes[*old_offset as usize..end].copy_from_slice(old_tuple);
            }
            LogRecord::HeapTupleDelete { block_id, slot, offset, old_tuple } => {
                // Undo delete = restore tuple and mark live
                let mut guard = txn.pin_write_guard(block_id);
                let bytes = guard.bytes_mut();
                let end = *offset as usize + old_tuple.len();
                bytes[*offset as usize..end].copy_from_slice(old_tuple);
                // Mark line pointer as live
            }
            _ => {}
        }
    }
}
```

### 5. Extend LogRecord Types

Physical tuple-level log records for heap operations:

```rust
pub enum LogRecord {
    // Existing (legacy, to be deprecated)
    Start(usize),
    Commit(usize),
    Rollback(usize),
    Checkpoint,
    SetInt { txnum: usize, block_id: BlockId, offset: usize, old_val: i32 },
    SetString { txnum: usize, block_id: BlockId, offset: usize, old_val: String },

    // New: Physical tuple-level operations
    HeapTupleInsert {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,              // where tuple was written
        tuple: Vec<u8>,           // full tuple bytes (for undo: mark slot free)
    },
    HeapTupleUpdate {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        old_offset: u16,          // tuple location before
        old_tuple: Vec<u8>,       // full tuple bytes before (for undo)
        new_offset: u16,          // tuple location after (may differ if relocated)
        new_tuple: Vec<u8>,       // full tuple bytes after (for redo)
    },
    HeapTupleDelete {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,
        old_tuple: Vec<u8>,       // full tuple bytes (for undo: restore)
    },

    // Future: B-tree physical operations (Phase 3)
    //
    // Physical entry-level records for leaf/internal pages.
    // `entry` stores the encoded entry bytes; undo replays the bytes into the slot/offset.
    BTreeLeafInsert {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,
        entry: Vec<u8>,
    },
    BTreeLeafDelete {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,
        entry: Vec<u8>,
    },
    BTreeInternalInsert {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,
        entry: Vec<u8>,
    },
    BTreeInternalDelete {
        txnum: usize,
        block_id: BlockId,
        slot: SlotId,
        offset: u16,
        entry: Vec<u8>,
    },
    //
    // Split record is DELTA-BASED (no full page images).
    // Entry movement during split is still logged via BTree*Insert/Delete records.
    // This record captures only structural metadata needed to undo allocation/link rewrites.
    BTreePageSplit {
        txnum: usize,
        page_kind: PageType,          // IndexLeaf or IndexInternal
        left_block_id: BlockId,
        right_block_id: BlockId,
        old_left_high_key: Option<Vec<u8>>,
        old_left_right_sibling: Option<u32>,     // leaf-only
        old_left_overflow: Option<u32>,          // leaf-only
        old_left_rightmost_child: Option<u32>,   // internal-only
        old_meta_first_free: Option<u32>,        // if free-list is maintained
    },
    // Root pointer update (metadata/catalog).
    // Encodes metadata/root-state transition caused by root split.
    // If root is rewritten in-place, old_root_block == new_root_block.
    BTreeRootUpdate {
        txnum: usize,
        meta_block_id: BlockId,
        old_root_block: u32,
        new_root_block: u32,
        old_tree_height: u16,
        new_tree_height: u16,
    },
}
```

**Log record size considerations**:
- `HeapTupleUpdate` is the largest: stores both old and new tuple bytes
- Typical tuple size: 50-200 bytes → update record: 100-400 bytes + header
- Trade-off accepted for simpler recovery logic

## Migration Strategy

### Phase 1: Add Logging Infrastructure (No Behavior Change)
1. Add `LogContext` struct with `block_id`, `txn_id`, `log_manager`, `before_image: Cell<Option<TupleSnapshot>>`
2. Add `TupleSnapshot` struct for capturing tuple bytes before mutation
3. Add new `LogRecord` variants: `HeapTupleInsert`, `HeapTupleUpdate`, `HeapTupleDelete`
4. Add serialization/deserialization for new log record types
5. Add `log_context: Option<LogContext>` field to `HeapPageViewMut` (initially `None`)
6. Add LSN field to `HeapHeaderMut` (use reserved bytes 24-31)
7. Tests pass with logging disabled (log_context = None)

### Phase 2: Wire Up Heap Operations
1. Update `Transaction::pin_write_guard` to pass log manager reference
2. `HeapPageViewMut::new` constructs `LogContext` when log manager available
3. `HeapPageViewMut::row_mut()` captures before-image via `TupleSnapshot::capture()`
4. `LogicalRowMut::drop()` logs `HeapTupleUpdate` with before/after bytes
5. `HeapPageViewMut::insert_row_mut()` logs `HeapTupleInsert`
6. Add `redo()` and `undo()` implementations for new log record types
7. `HeapPageViewMut::drop()` uses actual LSN from log_context instead of `Lsn::MAX`
8. Test crash recovery with heap updates

### Phase 3: Wire Up B-Tree Operations
1. Add logging to `BTreeLeafPageViewMut::insert_entry` / `delete_entry`
2. Add logging to `BTreeInternalPageViewMut::insert_entry` / `delete_entry`
3. Handle split operations (multiple log records in one operation)
4. Test B-tree crash recovery

### Cascading Splits (Clarification)

When a split propagates upward, we emit:
- **one `BTreePageSplit` per split** (allocation + structural delta)
- **`BTreeLeaf/InternalInsert` + `BTreeLeaf/InternalDelete`** for moved entries
- **one `BTreeInternalInsert`** for parent separator insertion

So a k-level cascade yields one structural split record per level plus entry-move and
parent-insert records at each level.

**Undo order:** process WAL backwards; each record undoes independently.
- Entry-move records undo first (restoring tuple distribution).
- `BTreePageSplit` undoes last for that level (restore left-page structural fields and
  logically free/deallocate the right page).

**Root split:** special handling is required because root metadata changes
(whether root is rewritten in-place or a new root page is allocated).
Two options:
1. **Dedicated root record**:
   `BTreeRootUpdate { meta_block_id, old_root_block, new_root_block, old_tree_height, new_tree_height }`
   - Root pointer lives in index catalog metadata (not page headers).
   - Undo: restore metadata fields to old values.
   - Redo: apply new metadata fields.
2. **Root as normal split + root update**:
   - Use `BTreePageSplit` for structural split delta.
   - Emit `BTreeRootUpdate` for metadata/root-pointer transition.

We recommend option (2): keep `BTreePageSplit` uniform and log root pointer changes
separately via `BTreeRootUpdate`.

### Free-List Plumbing Required for Undo Deallocation

Undoing a split or root update may need to deallocate a page that was newly allocated
during the forward operation. Deallocation should be **logical** (reusable page), not
file truncation.

Current code already has metadata support:
- `BTreeMetaPage.first_free_block` exists in the meta header.
- `PageType::Free` exists as a valid page discriminator.

Missing pieces to implement:
1. Meta setters/getters for free-list head mutation in mutable meta views
   (`set_first_free_block`).
2. A free-page header field for `next_free` pointer (singly linked list of free blocks).
3. Allocation path that first pops from `first_free_block` before appending.
4. Deallocation helper to mark a page `Free` and push it onto free list.

Undo behavior once plumbing exists:
1. **Undo split (`BTreePageSplit`)**
   - Undo entry movement via `BTree*Insert/Delete` records (reverse WAL order).
   - Restore left-page structural fields from `BTreePageSplit`.
   - Deallocate `right_block_id`: mark page `Free`, set its `next_free` to prior
     meta head, update `meta.first_free_block` to `right_block_id`.
2. **Undo root update (`BTreeRootUpdate`)**
   - Restore `meta.root_block` and `meta.tree_height` to old values.
   - If `old_root_block != new_root_block`, deallocate `new_root_block` using the
     same free-list push operation.
   - If equal (in-place root rewrite), no page deallocation is required.

### Phase 4: Remove Legacy Logging Paths
1. Deprecate `Transaction::set_int` / `set_string` (raw offset-based logging)
2. Migrate remaining callers to view-based API
3. Remove old log record types

### Phase 5: Optimize
1. Batch multiple column updates into single log record
2. Use physical logging for bulk operations
3. Add compression for repetitive log entries

## Logging Protocol Details

### WAL Protocol Guarantees

1. **Log-first rule**: Log record must reach stable storage before dirty page can be written
2. **LSN ordering**: Page LSN ≤ log tail LSN at all times
3. **Undo before redo**: During recovery, undo incomplete transactions before redoing committed ones

### LSN Tracking

With physical logging, LSN is tracked at the page level:

```rust
/// Add LSN field to heap header (bytes 24-31, currently reserved)
impl HeapHeaderMut<'_> {
    pub fn lsn(&self) -> u64 {
        u64::from_le_bytes(self.bytes[24..32].try_into().unwrap())
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.bytes[24..32].copy_from_slice(&lsn.to_le_bytes());
    }
}

impl HeapPageViewMut<'_> {
    pub fn insert_row_mut(&mut self) -> Result<(SlotId, LogicalRowMut<'_>), Box<dyn Error>> {
        // Allocate slot and write tuple
        let slot = self.insert_tuple(&tuple_bytes)?;

        // Log the insert with full tuple bytes
        if let Some(log_ctx) = &self.log_context {
            let page = self.build_page();
            let line_ptr = page.line_ptr(slot).unwrap();
            let tuple_bytes = page.tuple_bytes(slot).unwrap().to_vec();

            let record = LogRecord::HeapTupleInsert {
                txnum: log_ctx.txn_id,
                block_id: log_ctx.block_id.clone(),
                slot,
                offset: line_ptr.offset(),
                tuple: tuple_bytes,
            };
            let lsn = record.write_log_record(log_ctx.log_manager.clone())?;
            log_ctx.lsn.set(Some(lsn));
        }

        self.dirty.set(true);
        Ok((slot, LogicalRowMut { /* ... */ }))
    }
}

impl Drop for HeapPageViewMut<'_> {
    fn drop(&mut self) {
        if self.dirty.get() {
            let lsn = self.log_context
                .as_ref()
                .and_then(|ctx| ctx.lsn.get())
                .unwrap_or(Lsn::MAX);

            // Update page header LSN
            if lsn != Lsn::MAX {
                let mut page = self.build_mut_page();
                page.header.set_lsn(lsn);
            }

            self.guard.mark_modified(self.guard.txn_id(), lsn);
        }
    }
}
```

### Handling Complex Operations

Some operations involve multiple page mutations. With physical logging, we capture complete before/after state:

```rust
// Example: Update that causes tuple to relocate (varlength growth)
impl HeapPageViewMut<'_> {
    pub fn update_with_redirect(
        &mut self,
        slot: SlotId,
        new_tuple_bytes: &[u8],
    ) -> Result<SlotId, Box<dyn Error>> {
        // Capture old tuple state
        let page = self.build_page();
        let old_line_ptr = page.line_ptr(slot).ok_or("slot not found")?;
        let old_tuple = page.tuple_bytes(slot).ok_or("tuple not found")?.to_vec();
        let old_offset = old_line_ptr.offset();

        // Allocate new slot for larger tuple
        let new_slot = self.allocate_tuple(new_tuple_bytes)?;
        let new_page = self.build_page();
        let new_line_ptr = new_page.line_ptr(new_slot).unwrap();
        let new_offset = new_line_ptr.offset();

        // Redirect old slot to new slot
        self.redirect_slot(slot, new_slot)?;

        // Log as HeapTupleUpdate with different old/new offsets
        if let Some(log_ctx) = &self.log_context {
            let record = LogRecord::HeapTupleUpdate {
                txnum: log_ctx.txn_id,
                block_id: log_ctx.block_id.clone(),
                slot,
                old_offset,
                old_tuple,
                new_offset,
                new_tuple: new_tuple_bytes.to_vec(),
            };
            let lsn = record.write_log_record(log_ctx.log_manager.clone())?;
            log_ctx.lsn.set(Some(lsn));
        }

        Ok(new_slot)
    }
}
```

Note: The `HeapTupleUpdate` record stores both offsets, allowing undo to restore the tuple at its original location even if it was relocated.

## Testing Strategy

### Recovery Model

The current system uses **undo-only recovery** with a **force** buffer policy (all dirty pages
flushed on commit). This means:
- Committed data is always durable on disk — no redo pass needed.
- Recovery only undoes incomplete transactions.
- The new `HeapTupleUpdate` records store both old and new tuple bytes, which is sufficient
  for redo if we ever move to a no-force policy, but redo is not implemented or needed today.

### Rollback Tests (Undo)

These verify that `RecoveryManager::rollback()` correctly undoes heap operations for the
current transaction.

#### 1. Rollback insert — slot is freed

Insert a row via `insert_row_mut`, then rollback. Verify the slot is no longer live
(the page should have no live tuples).

#### 2. Rollback update — old value restored

Insert a row with value A, commit. In a new transaction, update to value B via `row_mut`,
then rollback. Read back and verify value is A.

#### 3. Rollback delete — tuple restored

Insert a row, commit. In a new transaction, delete via `delete_slot`, then rollback.
Read back and verify the row is live with original values.

#### 4. Rollback multiple operations in one transaction

In a single transaction: insert row 1, insert row 2, update row 1, delete row 2.
Rollback. Verify: row 1 is gone (insert undone), row 2 is gone (insert undone).
Operations undo in reverse log order.

#### 5. Rollback update with string field

Same as test 2 but with a varchar column. Ensures variable-length tuple bytes are
correctly captured and restored by `TupleSnapshot`.

### Recovery Tests (Crash Undo)

These verify that `RecoveryManager::recover()` undoes incomplete transactions found in
the WAL after a simulated crash.

#### 6. Recovery undoes uncommitted insert

Insert a row but don't commit. Call `recover()`. Verify the slot is freed — the insert
is rolled back because the transaction never committed.

#### 7. Recovery leaves committed data intact

Transaction A inserts a row and commits. Transaction B inserts a row but doesn't commit.
Call `recover()`. Verify A's row is still live, B's row is freed.

#### 8. Recovery undoes uncommitted update

Insert and commit a row with value A. In a new transaction, update to value B but don't
commit. Call `recover()`. Verify value is A.

#### 9. Recovery undoes uncommitted delete

Insert and commit a row. In a new transaction, delete the row but don't commit.
Call `recover()`. Verify the row is restored.

### Log Record Serialization Tests

#### 10. Round-trip serialization for HeapTupleInsert

Create a `HeapTupleInsert` record, serialize to bytes, deserialize back. Verify all
fields match (txnum, block_id, slot, offset, tuple bytes).

#### 11. Round-trip serialization for HeapTupleUpdate

Same for `HeapTupleUpdate` — verify old_offset, old_tuple, new_offset, new_tuple all
survive the round-trip.

#### 12. Round-trip serialization for HeapTupleDelete

Same for `HeapTupleDelete`.

### Edge Case Tests

#### 13. Multiple row updates in one page view

Get `row_mut` for slot 0, modify, drop. Get `row_mut` for slot 1, modify, drop.
Drop the view. Verify both updates are logged as separate `HeapTupleUpdate` records
and the page LSN reflects the max of both.

#### 14. Insert then immediate update in same transaction

`insert_row_mut`, set column to A, drop row. `row_mut` on same slot, set column to B,
drop row. Verify two log records: one `HeapTupleInsert` and one `HeapTupleUpdate`.
Rollback should undo both, leaving the slot free.

#### 15. No-op row_mut (no columns changed)

Get `row_mut` but don't call `set_column`. Drop. Verify no `HeapTupleUpdate` log record
is written (the dirty flag should be false).

## Performance Considerations

### Overhead Analysis

**Per-operation costs** (physical tuple-level logging):
- Capture before-image: 1 memcpy of tuple bytes (already in cache)
- Serialize log record: tuple_size × 2 + ~50 bytes header
- Append to log: 1 sequential write (batched by LogManager)
- Update page LSN: cheap in-memory write

**Log record sizes** (assuming 100-byte average tuple):
- `HeapTupleInsert`: ~150 bytes (tuple + header)
- `HeapTupleUpdate`: ~250 bytes (old tuple + new tuple + header)
- `HeapTupleDelete`: ~150 bytes (old tuple + header)

**Potential optimizations** (future):
1. **Same-size updates**: If tuple size unchanged, only log changed bytes (delta)
2. **Page-level logging**: For bulk operations, log entire page image
3. **Log compression**: Compress tuple bytes in log records
4. **Lazy logging**: Delay logging until transaction commits (violates strict WAL)

### Benchmark Targets

- Logging overhead should be < 15% for typical OLTP workloads (higher than logical due to full tuple copies)
- Recovery time should scale linearly with log size
- No performance regression for read-only transactions
- Redo/undo should be faster than logical (simple memcpy vs schema lookups)

## Open Questions

1. ~~**Physical vs logical logging**~~: **DECIDED** → Physical tuple-level logging (see Design Decisions)

2. **Undo granularity**: Should undo reverse individual operations or full transactions?
   - Per-operation: More flexible, supports partial rollback
   - Per-transaction: Simpler, matches current design
   - **Leaning toward**: Per-operation (current `rollback()` iterates log records individually)

3. ~~**LSN placement**~~: **DECIDED** → Page header (see Design Decisions)

4. **Checkpointing**: How do checkpoints interact with view-based mutations?
   - Need to ensure all dirty pages have valid LSNs before checkpoint
   - May need barrier in HeapPageViewMut::drop

## References

- **ARIES paper**: Mohan et al., "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"
- **PostgreSQL WAL**: https://www.postgresql.org/docs/current/wal-intro.html
- **docs/record_management.md**: Current page layout and view architecture
- **Issue #59**: Transaction lock integration (related concurrency work)

## WAL Rule Enforcement

### The Problem

The WAL protocol requires: **a log record must reach stable storage before the dirty page
it describes can be written to disk.** This ensures that on crash, we can always undo
changes by replaying the log.

Currently, `BufferFrame::flush_to_disk` flushes the log up to the frame's LSN before
writing the page:

```rust
fn flush_to_disk(&self, ...) {
    if let (Some(block_id), Some(lsn)) = (meta.block_id.clone(), meta.lsn) {
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        // ... write page to disk
    }
}
```

This is correct **if** the LSN on the frame always reflects the latest log record for that
page. With the new view-based logging, the LSN flows as:

1. `LogicalRowMut::drop()` writes log record → gets LSN → updates shared `page_lsn` Rc
2. `HeapPageViewMut::drop()` reads `page_lsn` → writes to page header → calls
   `mark_modified(txn_id, lsn)` which sets the frame's LSN

The risk: if `mark_modified` is called with `Lsn::MAX` (the fallback when no log context
exists), the buffer manager will try to flush LSN `MAX`, which is meaningless. This
currently happens when `dirty` is true but `page_lsn` is `None` — for example, if a
mutation occurs without logging (a bug, now that all public mutating methods log).

### What To Verify

1. Every path that sets `dirty = true` on `HeapPageViewMut` also produces a log record
   with a real LSN. The `Lsn::MAX` fallback in `HeapPageViewMut::drop` should be
   unreachable in normal operation.
2. `BufferFrame::flush_to_disk` should assert or warn if it sees `Lsn::MAX`, since that
   indicates a page was dirtied without logging.
3. The `format_as_heap` / `format_as_btree_*` methods on `PageWriteGuard` call
   `mark_modified` with `Lsn::MAX` — these are page initialization paths that don't go
   through views. These are safe because formatting an empty page doesn't need undo, but
   they should be audited if we ever add redo.

### Future: No-Force Policy

If we move to a no-force buffer policy (don't flush all dirty pages on commit), then:
- Committed pages may not be on disk after commit returns.
- Recovery would need a **redo pass** to replay committed changes.
- The existing log records already store enough data for redo (`HeapTupleInsert` has
  tuple bytes, `HeapTupleUpdate` has `new_tuple`).
- Recovery would need to compare page LSN vs log record LSN to decide whether to redo.
- This is not needed today since we use force (flush-on-commit).

## Status

**Current**: Phase 2 substantially complete for heap operations.

### What's Done
- `LogContext` carries `log_manager`, `block_id`, `txn_id` from Transaction → PageWriteGuard
- `HeapPageViewMut` creates its own `page_lsn` tracker
- `RowLogContext` + `TupleSnapshot` capture before/after images per row operation
- All public mutating methods on `HeapPageViewMut` log:
  - `insert_row_mut()` → `HeapTupleInsert` (via `LogicalRowMut::drop`)
  - `row_mut()` → `HeapTupleUpdate` (via `LogicalRowMut::drop`)
  - `delete_slot()` → `HeapTupleDelete` (via `RowLogContext::write_delete_log`)
  - `update_tuple()` → `HeapTupleUpdate` (logged directly)
- `HeapPageViewMut::drop()` writes LSN to page header and marks frame modified
- Undo implemented for all three record types via `HeapPageMut::undo_insert/update/delete`
- Serialization/deserialization for all new log record variants
- Non-public helpers (`insert_tuple`, `redirect_slot`, `write_bytes`, `tuple_ref`) are private

### Decisions Made
- Physical tuple-level logging (not logical)
- LSN in page header (not per-tuple)
- Undo-only recovery (no redo) — sufficient with force buffer policy
- `LogContext` carries only infrastructure (no LSN); `page_lsn` owned by view
- Start with heap operations only; B-tree deferred to Phase 3

### Next Steps
- Write recovery tests (see Testing Strategy above)
- B-tree logging (Phase 3)
- Audit `Lsn::MAX` usage to ensure WAL rule is not violated
- Remove legacy `SetInt`/`SetString` logging paths (Phase 4)
