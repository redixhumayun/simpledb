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
    // BTreeEntryInsert { ... }
    // BTreeEntryDelete { ... }
    // BTreePageSplit { ... }
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

### Unit Tests

```rust
#[test]
fn test_logged_column_update_redo() {
    let (db, _dir) = SimpleDB::new_for_test(8, 5000);
    let txn = db.new_tx();

    // Insert row
    let block_id = txn.append("test_file");
    let mut guard = txn.pin_write_guard(&block_id);
    guard.format_as_heap();
    let mut view = guard.into_heap_view_mut(&layout).unwrap();
    let (slot, mut row) = view.insert_row_mut().unwrap();
    row.set_column("id", &Constant::Int(42));
    drop(view);
    drop(guard);

    // Force log to disk
    txn.commit().unwrap();

    // Simulate crash and recovery
    drop(txn);
    drop(db);

    // Reopen and verify
    let db2 = SimpleDB::new(/* same path */, 8, false, 5000);
    let txn2 = db2.new_tx();
    txn2.recover().unwrap();

    // Read value back
    let guard = txn2.pin_read_guard(&block_id);
    let view = guard.into_heap_view(&layout).unwrap();
    let row = view.row(slot).unwrap();
    assert_eq!(row.get_column("id").unwrap(), Constant::Int(42));
}

#[test]
fn test_logged_column_update_undo() {
    let (db, _dir) = SimpleDB::new_for_test(8, 5000);
    let txn = db.new_tx();

    // Insert and update
    // ... (setup)
    let mut row = view.row_mut(slot).unwrap();
    row.set_column("id", &Constant::Int(100));
    drop(view);

    // Rollback
    txn.rollback().unwrap();

    // Verify old value restored
    let guard = txn.pin_read_guard(&block_id);
    let view = guard.into_heap_view(&layout).unwrap();
    let row = view.row(slot).unwrap();
    assert_eq!(row.get_column("id").unwrap(), Constant::Int(42));
}
```

### Integration Tests

1. **Crash during transaction**: Insert rows, crash before commit, verify rollback
2. **Crash during checkpoint**: Dirty pages flushing, verify recovery completes correctly
3. **Mixed operations**: Insert, update, delete in single transaction, crash, verify correct final state
4. **B-tree recovery**: Insert keys causing splits, crash, verify tree structure intact
5. **Concurrent transactions**: Multiple transactions, one crashes, others unaffected

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

## Status

**Current**: Design complete for heap operations
**Next steps**: Implement Phase 1 (logging infrastructure)

### Decisions Made
- Physical tuple-level logging (not logical)
- LSN in page header (not per-tuple)
- Start with heap operations only; B-tree deferred to Phase 3
