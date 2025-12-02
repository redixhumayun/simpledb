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

## Proposed Architecture

### 1. Extend PageViewMut with Logging Context

```rust
pub struct HeapPageViewMut<'a, K: PageKind> {
    guard: PageWriteGuard<'a>,
    page_ref: &'a mut Page<K>,
    layout: &'a Layout,
    dirty: Rc<Cell<bool>>,
    log_context: Option<LogContext<'a>>,  // ← New field
}

struct LogContext<'a> {
    recovery_mgr: &'a RecoveryManager,
    block_id: &'a BlockId,
    txn_id: usize,
    lsn: Cell<Option<Lsn>>,
}
```

### 2. Define Loggable Operations

Create a trait for operations that need WAL logging:

```rust
trait LoggablePageOp {
    /// Generate a log record for this operation
    fn log_record(&self, block_id: &BlockId) -> LogRecord;

    /// Apply this operation to a page (for redo)
    fn apply(&self, page: &mut Page<impl PageKind>) -> Result<(), Box<dyn Error>>;

    /// Reverse this operation (for undo)
    fn undo(&self) -> Box<dyn LoggablePageOp>;
}
```

Example implementations:

```rust
struct SetColumnOp {
    slot: SlotId,
    field: String,
    old_value: Constant,
    new_value: Constant,
}

impl LoggablePageOp for SetColumnOp {
    fn log_record(&self, block_id: &BlockId) -> LogRecord {
        LogRecord::SetColumn {
            block_id: block_id.clone(),
            slot: self.slot,
            field: self.field.clone(),
            old_value: self.old_value.clone(),
        }
    }

    fn apply(&self, page: &mut Page<HeapPage>) -> Result<(), Box<dyn Error>> {
        // Set the column value
        // (implementation details)
        Ok(())
    }

    fn undo(&self) -> Box<dyn LoggablePageOp> {
        Box::new(SetColumnOp {
            slot: self.slot,
            field: self.field.clone(),
            old_value: self.new_value.clone(),
            new_value: self.old_value.clone(),
        })
    }
}
```

### 3. Update LogicalRowMut to Log Changes

```rust
impl<'a> LogicalRowMut<'a> {
    pub fn set_column(&mut self, field: &str, value: &Constant) -> Option<()> {
        if let Some(log_ctx) = &self.log_context {
            // Read old value for undo
            let old_value = self.get_column(field)?;

            // Create operation
            let op = SetColumnOp {
                slot: self.slot,
                field: field.to_string(),
                old_value,
                new_value: value.clone(),
            };

            // Log it
            let lsn = log_ctx.recovery_mgr.log_operation(
                log_ctx.block_id,
                log_ctx.txn_id,
                &op,
            )?;
            log_ctx.lsn.set(Some(lsn));
        }

        // Perform mutation
        self.heap_tuple_mut.set_column_bytes(field, value);
        self.dirty.set(true);
        Some(())
    }
}
```

### 4. Update RecoveryManager

Add methods to log high-level operations:

```rust
impl RecoveryManager {
    /// Log a page operation and return its LSN
    pub fn log_operation(
        &self,
        block_id: &BlockId,
        txn_id: usize,
        op: &dyn LoggablePageOp,
    ) -> Result<Lsn, Box<dyn Error>> {
        let log_record = op.log_record(block_id);
        let bytes = log_record.serialize();
        self.log_manager.append(&bytes)
    }

    /// Redo: apply operations during crash recovery
    pub fn redo(&self, log_record: &LogRecord) -> Result<(), Box<dyn Error>> {
        match log_record {
            LogRecord::SetColumn { block_id, slot, field, new_value } => {
                // Pin page, create view, apply change
                let guard = self.txn.pin_write_guard(block_id);
                let mut view = guard.into_heap_view_mut(&self.layout)?;
                view.row_mut(*slot)?.set_column(field, new_value);
            }
            // ... other record types
        }
        Ok(())
    }

    /// Undo: reverse operations during rollback
    pub fn undo(&self, log_record: &LogRecord) -> Result<(), Box<dyn Error>> {
        let undo_op = log_record.to_undo_op();
        self.log_operation(&log_record.block_id, log_record.txn_id, &*undo_op)
    }
}
```

### 5. Extend LogRecord Types

```rust
pub enum LogRecord {
    // Existing
    SetInt { block_id: BlockId, offset: usize, old_value: i32 },
    SetString { block_id: BlockId, offset: usize, old_value: String },

    // New: Schema-aware operations
    SetColumn {
        block_id: BlockId,
        slot: SlotId,
        field: String,
        old_value: Constant,
    },
    InsertRow {
        block_id: BlockId,
        slot: SlotId,
        values: Vec<Constant>,
    },
    DeleteRow {
        block_id: BlockId,
        slot: SlotId,
        old_values: Vec<Constant>,
    },
    RedirectSlot {
        block_id: BlockId,
        old_slot: SlotId,
        new_slot: SlotId,
    },

    // B-tree operations
    BTreeInsert {
        block_id: BlockId,
        slot: SlotId,
        key: Constant,
        value: BTreeValue,  // RID for leaf, child_block for internal
    },
    BTreeDelete {
        block_id: BlockId,
        slot: SlotId,
        old_key: Constant,
        old_value: BTreeValue,
    },
    BTreeSplit {
        old_block: BlockId,
        new_block: BlockId,
        split_key: Constant,
        moved_entries: Vec<BTreeEntry>,
    },
}

enum BTreeValue {
    Leaf(RID),
    Internal(usize),  // child block number
}
```

## Migration Strategy

### Phase 1: Add Logging Infrastructure (No Behavior Change)
1. Add `LogContext` field to `HeapPageViewMut` (initially `None`)
2. Define `LoggablePageOp` trait and initial implementations
3. Add new `LogRecord` variants
4. Tests pass with logging disabled

### Phase 2: Wire Up Heap Operations
1. Update `Transaction::pin_write_guard` to pass `RecoveryManager` reference
2. `HeapPageViewMut::new` constructs `LogContext` with recovery manager
3. `LogicalRowMut::set_column` logs before mutation
4. Add redo/undo implementations for heap operations
5. Test crash recovery with heap updates

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

```rust
impl HeapPageViewMut<'_, HeapPage> {
    pub fn insert_row_mut(&mut self) -> Result<(SlotId, LogicalRowMut<'_>), Box<dyn Error>> {
        // Allocate slot
        let slot = self.page_ref.allocate_tuple(&empty_tuple_bytes)?;

        // Log allocation
        if let Some(log_ctx) = &self.log_context {
            let lsn = log_ctx.recovery_mgr.log_operation(
                log_ctx.block_id,
                log_ctx.txn_id,
                &AllocateTupleOp { slot },
            )?;
            log_ctx.lsn.set(Some(lsn));
        }

        self.dirty.set(true);
        Ok((slot, LogicalRowMut { /* ... */ }))
    }
}

impl<'a, K: PageKind> Drop for HeapPageViewMut<'a, K> {
    fn drop(&mut self) {
        if self.dirty.get() {
            let lsn = self.log_context
                .as_ref()
                .and_then(|ctx| ctx.lsn.get())
                .unwrap_or(Lsn::MAX);
            self.guard.mark_modified(self.guard.txn_id(), lsn);
        }
    }
}
```

### Handling Complex Operations

Some operations involve multiple page mutations:

```rust
// Example: Update that causes tuple to grow and redirect
impl HeapPageViewMut<'_, HeapPage> {
    pub fn update_with_redirect(
        &mut self,
        slot: SlotId,
        new_data: &[u8],
    ) -> Result<SlotId, Box<dyn Error>> {
        // Read old data
        let old_data = self.tuple_bytes(slot)?;

        // Allocate new slot
        let new_slot = self.allocate_tuple(new_data)?;

        // Redirect old slot
        self.redirect_slot(slot, new_slot)?;

        // Log as compound operation
        if let Some(log_ctx) = &self.log_context {
            let op = UpdateWithRedirectOp {
                old_slot: slot,
                new_slot,
                old_data: old_data.to_vec(),
                new_data: new_data.to_vec(),
            };
            let lsn = log_ctx.recovery_mgr.log_operation(
                log_ctx.block_id,
                log_ctx.txn_id,
                &op,
            )?;
            log_ctx.lsn.set(Some(lsn));
        }

        Ok(new_slot)
    }
}
```

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

**Per-operation costs**:
- Read old value for undo: 1 page access (already in cache)
- Serialize log record: ~100-500 bytes depending on operation
- Append to log: 1 sequential write (batched by LogManager)
- Update LSN tracking: cheap in-memory operation

**Optimizations**:
1. **Batching**: Group multiple column updates into single log record
2. **Physical logging**: For bulk operations, log page image instead of logical ops
3. **Lazy logging**: Delay logging until transaction commits (violates strict WAL but faster)
4. **Log compression**: Delta-encode repetitive fields

### Benchmark Targets

- Logging overhead should be < 10% for typical OLTP workloads
- Recovery time should scale linearly with log size
- No performance regression for read-only transactions

## Open Questions

1. **Physical vs logical logging**: Should we log physical page images or logical operations?
   - Physical: Simpler redo, but larger log size
   - Logical: Smaller logs, but more complex redo logic

2. **Undo granularity**: Should undo reverse individual operations or full transactions?
   - Per-operation: More flexible, supports partial rollback
   - Per-transaction: Simpler, matches current design

3. **LSN placement**: Where should we store page LSN?
   - In HeapTupleHeader: Fine-grained, but wastes space
   - In PageHeader: Coarse-grained, but simpler

4. **Checkpointing**: How do checkpoints interact with view-based mutations?
   - Need to ensure all dirty pages have valid LSNs before checkpoint
   - May need barrier in HeapPageViewMut::drop

## References

- **ARIES paper**: Mohan et al., "ARIES: A Transaction Recovery Method Supporting Fine-Granularity Locking and Partial Rollbacks Using Write-Ahead Logging"
- **PostgreSQL WAL**: https://www.postgresql.org/docs/current/wal-intro.html
- **docs/record_management.md**: Current page layout and view architecture
- **Issue #59**: Transaction lock integration (related concurrency work)

## Status

**Current**: Design phase
**Next steps**: Implement Phase 1 (logging infrastructure)
