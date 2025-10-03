# ReadHandle and WriteHandle Implementation Guide

## Overview

This document outlines the design and implementation of `ReadHandle` and `WriteHandle` types to provide **compile-time enforcement of read/write access control** for buffer management.

## Motivation

Currently, `BufferHandle` provides RAII lifetime management but doesn't distinguish between read and write access. This leads to:
- No compile-time prevention of writes through read-only scans
- Unclear intent when pinning buffers (is this for reading or writing?)
- Manual lock management separate from buffer pinning

**Goal:** Integrate buffer pinning with lock acquisition to provide type-safe read/write semantics.

---

## Design

### Core Types

```rust
/// Shared read access to a pinned buffer
pub struct ReadHandle {
    block_id: BlockId,
    transaction: Arc<Transaction>,
}

/// Exclusive write access to a pinned buffer
pub struct WriteHandle {
    block_id: BlockId,
    transaction: Arc<Transaction>,
}
```

### Key Design Principles

1. **Type Safety**: `ReadHandle` cannot call write methods (enforced at compile time)
2. **Lock Integration**: Handles acquire locks on creation, release on drop
3. **RAII**: Both pin/unpin AND lock/unlock are managed automatically
4. **Pedagogical**: Teaches reader-writer pattern clearly

---

## API Design

### Transaction Methods

```rust
impl Transaction {
    /// Pin for read access (shared lock)
    pub fn pin_read(&self, block_id: &BlockId) -> Result<ReadHandle, Error> {
        // Acquire shared lock first
        self.concurrency_manager.slock(block_id)?;

        // Then pin the buffer
        self.buffer_list.pin(block_id);

        Ok(ReadHandle {
            block_id: block_id.clone(),
            transaction: Arc::clone(self),
        })
    }

    /// Pin for write access (exclusive lock)
    pub fn pin_write(&self, block_id: &BlockId) -> Result<WriteHandle, Error> {
        // Acquire exclusive lock first
        self.concurrency_manager.xlock(block_id)?;

        // Then pin the buffer
        self.buffer_list.pin(block_id);

        Ok(WriteHandle {
            block_id: block_id.clone(),
            transaction: Arc::clone(self),
        })
    }
}
```

### ReadHandle API

```rust
impl ReadHandle {
    /// Get block ID this handle refers to
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    /// Read integer at offset
    pub fn get_int(&self, offset: usize) -> Result<i32, Error> {
        self.transaction.get_int(&self.block_id, offset)
    }

    /// Read string at offset
    pub fn get_string(&self, offset: usize) -> Result<String, Error> {
        self.transaction.get_string(&self.block_id, offset)
    }

    // NO set_int() or set_string() methods!
}
```

### WriteHandle API

```rust
impl WriteHandle {
    /// Get block ID this handle refers to
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }

    /// Read integer at offset
    pub fn get_int(&self, offset: usize) -> Result<i32, Error> {
        self.transaction.get_int(&self.block_id, offset)
    }

    /// Read string at offset
    pub fn get_string(&self, offset: usize) -> Result<String, Error> {
        self.transaction.get_string(&self.block_id, offset)
    }

    /// Write integer at offset (only WriteHandle can do this!)
    pub fn set_int(&self, offset: usize, val: i32, log: bool) -> Result<(), Error> {
        self.transaction.set_int(&self.block_id, offset, val, log)
    }

    /// Write string at offset (only WriteHandle can do this!)
    pub fn set_string(&self, offset: usize, val: &str, log: bool) -> Result<(), Error> {
        self.transaction.set_string(&self.block_id, offset, val, log)
    }
}
```

### Drop Implementation

```rust
impl Drop for ReadHandle {
    fn drop(&mut self) {
        // Unpin the buffer
        self.transaction.unpin_internal(&self.block_id);

        // Release the shared lock
        self.transaction.concurrency_manager.unlock(&self.block_id);
    }
}

impl Drop for WriteHandle {
    fn drop(&mut self) {
        // Unpin the buffer
        self.transaction.unpin_internal(&self.block_id);

        // Release the exclusive lock
        self.transaction.concurrency_manager.unlock(&self.block_id);
    }
}
```

---

## Usage Examples

### RecordPage (Write Access)

```rust
struct RecordPage {
    tx: Arc<Transaction>,
    handle: WriteHandle,  // Exclusive access for modifications
    layout: Layout,
}

impl RecordPage {
    fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Result<Self, Error> {
        let handle = tx.pin_write(&block_id)?;
        Ok(Self { tx, handle, layout })
    }

    fn set_int(&self, slot: usize, field: &str, val: i32) -> Result<(), Error> {
        let offset = self.offset(slot) + self.layout.offset(field)?;
        self.handle.set_int(offset, val, true)  // ✅ Allowed
    }

    fn get_int(&self, slot: usize, field: &str) -> Result<i32, Error> {
        let offset = self.offset(slot) + self.layout.offset(field)?;
        self.handle.get_int(offset)  // ✅ Also allowed
    }
}
```

### TableScan (Read-Only Mode)

```rust
struct TableScan {
    tx: Arc<Transaction>,
    handle: ReadHandle,  // Shared access for reading
    layout: Layout,
}

impl TableScan {
    fn new_readonly(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Result<Self, Error> {
        let handle = tx.pin_read(&block_id)?;
        Ok(Self { tx, handle, layout })
    }

    fn get_int(&self, field: &str) -> Result<i32, Error> {
        let offset = self.layout.offset(field)?;
        self.handle.get_int(offset)  // ✅ Allowed
    }

    // fn set_int(&self, field: &str, val: i32) -> Result<(), Error> {
    //     self.handle.set_int(...)  // ❌ Compile error! ReadHandle has no set_int
    // }
}
```

---

## Benefits

### 1. Compile-Time Safety

```rust
let read_handle = txn.pin_read(&block_id)?;
read_handle.get_int(0)?;     // ✅ OK
read_handle.set_int(0, 42)?; // ❌ Compile error: method not found

let write_handle = txn.pin_write(&block_id)?;
write_handle.get_int(0)?;     // ✅ OK
write_handle.set_int(0, 42)?; // ✅ OK
```

### 2. Clear Intent

```rust
// Old (unclear intent)
txn.pin(&block_id);

// New (clear intent)
let handle = txn.pin_read(&block_id)?;   // I'm reading
let handle = txn.pin_write(&block_id)?;  // I'm writing
```

### 3. Automatic Lock Management

No more manual lock/unlock calls scattered throughout the code:

```rust
// Old (manual locking)
txn.slock(&block_id)?;
txn.pin(&block_id);
// ... do work ...
txn.unpin(&block_id);
txn.unlock(&block_id);  // Easy to forget!

// New (automatic)
let handle = txn.pin_read(&block_id)?;
// ... do work ...
// Drop automatically unpins AND unlocks
```

### 4. Pedagogical Value

Clearly demonstrates:
- Reader-writer pattern
- Type-driven design
- Lock coupling with resource management
- Rust's ownership system preventing data races

---

## Implementation Strategy

### Phase 1: Add New Types Alongside Existing

1. Implement `ReadHandle` and `WriteHandle` structs
2. Add `pin_read()` and `pin_write()` methods to `Transaction`
3. Keep existing `BufferHandle` and `pin()` for compatibility
4. Add tests for new handle types

### Phase 2: Gradual Migration

1. Identify components that only read (scans, indexes)
2. Migrate read-only components to `ReadHandle`
3. Identify components that write (RecordPage, updates)
4. Migrate write components to `WriteHandle`
5. Update tests incrementally

### Phase 3: Cleanup

1. Remove `BufferHandle` (if fully migrated)
2. Remove manual lock/unlock calls (now handled by Drop)
3. Remove `pin()` method (replaced by `pin_read()`/`pin_write()`)
4. Verify all tests pass

---

## Migration Checklist

### Components to Migrate

- [ ] **RecordPage** → `WriteHandle` (needs write access)
- [ ] **TableScan** → `ReadHandle` or `WriteHandle` (depends on mode)
- [ ] **ChunkScan** → `ReadHandle` (read-only)
- [ ] **ProductScan** → `ReadHandle` (read-only)
- [ ] **SelectScan** → `ReadHandle` (read-only)
- [ ] **ProjectScan** → `ReadHandle` (read-only)
- [ ] **SortScan** → `WriteHandle` for temp tables, `ReadHandle` for reading
- [ ] **IndexSelectScan** → `ReadHandle` (read-only)
- [ ] **HashIndex** → `WriteHandle` (needs write access)
- [ ] **BTreePage** → `WriteHandle` (needs write access)

### Lock Management Cleanup

After migration, remove manual lock calls from:
- [ ] Transaction::get_int / set_int
- [ ] Transaction::get_string / set_string
- [ ] RecordPage methods
- [ ] Index operations

---

## Testing Strategy

### Unit Tests

```rust
#[test]
fn test_read_handle_cannot_write() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let handle = txn.pin_read(&block_id).unwrap();

    // This should not compile:
    // handle.set_int(0, 42);

    // This should work:
    let val = handle.get_int(0).unwrap();
}

#[test]
fn test_write_handle_can_read_and_write() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let handle = txn.pin_write(&block_id).unwrap();

    handle.set_int(0, 42, true).unwrap();
    assert_eq!(handle.get_int(0).unwrap(), 42);
}

#[test]
fn test_locks_released_on_drop() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    {
        let _handle = txn.pin_read(&block_id).unwrap();
        // Shared lock acquired
    }

    // Lock should be released after drop
    let _handle2 = txn.pin_write(&block_id).unwrap();  // Should succeed
}
```

### Concurrency Tests

```rust
#[test]
fn test_multiple_readers_allowed() {
    let txn1 = Arc::new(Transaction::new(/*...*/));
    let txn2 = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let handle1 = txn1.pin_read(&block_id).unwrap();
    let handle2 = txn2.pin_read(&block_id).unwrap();  // ✅ Should work

    // Both can read simultaneously
    let val1 = handle1.get_int(0).unwrap();
    let val2 = handle2.get_int(0).unwrap();
}

#[test]
fn test_writer_blocks_readers() {
    let txn1 = Arc::new(Transaction::new(/*...*/));
    let txn2 = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let write_handle = txn1.pin_write(&block_id).unwrap();

    // This should timeout or return error:
    let result = txn2.pin_read(&block_id);
    assert!(result.is_err());
}
```

---

## Open Questions

1. **Clone semantics**: Should `ReadHandle::clone()` be allowed?
   - Pro: Multiple references to same read access
   - Con: Pin count tracking complexity

2. **Lock upgrade**: Should we support `ReadHandle::upgrade()` → `WriteHandle`?
   - Pro: Useful for read-then-write patterns
   - Con: Deadlock potential with multiple upgraders

3. **Downgrade**: Should we support `WriteHandle::downgrade()` → `ReadHandle`?
   - Pro: Release exclusive lock while keeping pin
   - Con: Added complexity

4. **Handle lifetime**: Should handles be constrained to transaction lifetime?
   - Currently: Can outlive commit (become "zombies")
   - Alternative: Use lifetime parameters to prevent

---

## Future Enhancements

### 1. Lock-Free Reads

For read-only transactions, consider lock-free MVCC:
```rust
struct SnapshotHandle {
    block_id: BlockId,
    version: u64,
    transaction: Arc<Transaction>,
}
```

### 2. Batch Pinning

For scans that need multiple blocks:
```rust
fn pin_range_read(&self, blocks: &[BlockId]) -> Result<Vec<ReadHandle>, Error>
```

### 3. Intent Locks

Add `IntentHandle` for hierarchical locking:
```rust
struct IntentReadHandle { /* ... */ }
struct IntentWriteHandle { /* ... */ }
```

---

## References

- Issue #9: [Convert manual pin/unpin to RAII Buffer Guard](https://github.com/redixhumayun/simpledb/issues/9)
- Related pattern: Rust's `std::sync::RwLock<T>` (read/write guards)
- Database Systems: The Complete Book (Garcia-Molina et al.) - Chapter on concurrency control
