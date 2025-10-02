# RAII Buffer Handles Implementation Guide

## Overview

This document provides implementation-level details for converting manual pin/unpin buffer management to RAII (Resource Acquisition Is Initialization) buffer handles.

## Core Concept

**BufferHandle Semantics:** A handle represents a **pinned block reference** (lifetime management only), NOT a block accessor (operations).

- Handle manages: Pin lifetime via RAII (Drop trait)
- Transaction provides: Operations (get_int, set_int, locking, WAL)

This separation matches database literature and provides clear layering.

---

## Architecture

### Current (Manual Pin/Unpin)

```
RecordPage/Scan:
  - Stores: tx: Arc<Transaction>, block_id: BlockId
  - Constructor: tx.pin(&block_id)
  - Destructor/close(): tx.unpin(&block_id)  ← Manual, error-prone
```

### Target (RAII Handle)

```
RecordPage/Scan:
  - Stores: tx: Arc<Transaction>, handle: BufferHandle
  - Constructor: handle = tx.pin(&block_id)
  - Destructor: (automatic via BufferHandle::drop())  ← RAII
```

---

## Implementation Steps

### Step 1: Define BufferHandle Struct

**Location:** `src/main.rs` (before Transaction definition)

```rust
/// A handle representing a pinned buffer.
/// The buffer is automatically unpinned when the handle is dropped.
///
/// The handle does NOT provide data access - use Transaction methods
/// and pass the handle's block_id.
///
/// # Lifetime
/// - Created by Transaction::pin()
/// - Clone increments pin count
/// - Drop decrements pin count
///
/// # Example
/// ```
/// let handle = txn.pin(&block_id);
/// let value = txn.get_int(handle.block_id(), offset)?;
/// // handle automatically unpins on drop
/// ```
pub struct BufferHandle {
    block_id: BlockId,
    transaction: Arc<Transaction>,
}

impl BufferHandle {
    /// Get the block ID this handle refers to
    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
}
```

### Step 2: Implement Clone Trait

**Semantic:** Cloning a handle **increments the pin count** (creates new ownership)

```rust
impl Clone for BufferHandle {
    fn clone(&self) -> Self {
        // Pin the block again (increment pin count)
        self.transaction.pin(&self.block_id);

        Self {
            block_id: self.block_id.clone(),
            transaction: Arc::clone(&self.transaction),
        }
    }
}
```

**Why this works:**
- Each BufferHandle owns one pin
- Clone = new handle = new pin
- Used by MultiBufferProductScan when cloning scans (line 418-419 in main.rs)

### Step 3: Implement Drop Trait

**Semantic:** Dropping a handle **decrements the pin count** (releases ownership)

```rust
impl Drop for BufferHandle {
    fn drop(&mut self) {
        // Unpin the block (decrement pin count)
        self.transaction.unpin(&self.block_id);
    }
}
```

**Invariant:** `Number of live BufferHandles = Pin count for that block`

### Step 4: Update Transaction::pin() to Return Handle

**Current signature:**
```rust
fn pin(&self, block_id: &BlockId) {
    self.buffer_list.pin(block_id);
}
```

**New signature:**
```rust
fn pin(&self, block_id: &BlockId) -> BufferHandle {
    self.buffer_list.pin(block_id);
    BufferHandle {
        block_id: block_id.clone(),
        transaction: Arc::clone(self),  // self is &Arc<Transaction> in practice
    }
}
```

**Note:** Since Transaction is always used as `Arc<Transaction>`, you may need to adjust the signature:

```rust
// Option 1: Require Arc<Transaction> as receiver
impl Transaction {
    fn pin(self: &Arc<Self>, block_id: &BlockId) -> BufferHandle {
        self.buffer_list.pin(block_id);
        BufferHandle {
            block_id: block_id.clone(),
            transaction: Arc::clone(self),
        }
    }
}

// Option 2: Create handle from caller's Arc
// (Keep pin signature unchanged, create handle at call site)
```

### Step 5: Update RecordPage

**Current:**
```rust
#[derive(Clone)]
struct RecordPage {
    tx: Arc<Transaction>,
    block_id: BlockId,
    layout: Layout,
}

impl RecordPage {
    fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        tx.pin(&block_id);  // Manual pin
        Self { tx, block_id, layout }
    }
}

// Need manual unpin somewhere (currently missing - bug!)
```

**New:**
```rust
#[derive(Clone)]
struct RecordPage {
    tx: Arc<Transaction>,
    handle: BufferHandle,  // Owns the pin
    layout: Layout,
}

impl RecordPage {
    fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = tx.pin(&block_id);  // Returns handle
        Self { tx, handle, layout }
    }

    // No explicit Drop needed - handle drops automatically!
}

// Clone automatically handled by derive(Clone) which calls handle.clone()
```

**Data access:**
```rust
impl RecordPage {
    fn get_int(&self, slot: usize, field_name: &str) -> i32 {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();

        // Access through transaction, using handle's block_id
        self.tx.get_int(self.handle.block_id(), offset).unwrap()
    }
}
```

### Step 6: Update ChunkScan

**Current Clone implementation (line 704-722):**
```rust
impl Clone for ChunkScan {
    fn clone(&self) -> Self {
        for block_num in self.first_block_num..=self.last_block_num {
            let block_id = BlockId::new(self.file_name.clone(), block_num);
            self.txn.pin(&block_id);  // Manual pin
        }

        Self {
            txn: Arc::clone(&self.txn),
            buffer_list: self.buffer_list.clone(),  // Vec<RecordPage>
            // ...
        }
    }
}
```

**New (automatic via RecordPage::clone):**
```rust
impl Clone for ChunkScan {
    fn clone(&self) -> Self {
        // No manual pins needed!
        // RecordPage::clone() automatically clones handles, which pins

        Self {
            txn: Arc::clone(&self.txn),
            buffer_list: self.buffer_list.clone(),  // Each RecordPage.handle clones
            // ...
        }
    }
}
```

### Step 7: Update TableScan

**Current Clone (line 7796-7811):**
```rust
impl Clone for TableScan {
    fn clone(&self) -> Self {
        if let Some(block_id) = self.record_page.as_ref().map(|rp| &rp.block_id) {
            self.txn.pin(block_id);  // Manual pin
        }

        Self {
            txn: Arc::clone(&self.txn),
            record_page: self.record_page.clone(),  // Option<RecordPage>
            // ...
        }
    }
}
```

**New (automatic):**
```rust
impl Clone for TableScan {
    fn clone(&self) -> Self {
        // No manual pin needed!
        // RecordPage::clone() handles it

        Self {
            txn: Arc::clone(&self.txn),
            record_page: self.record_page.clone(),  // Auto-pins if Some
            // ...
        }
    }
}
```

### Step 8: Remove Explicit close() Methods

**Current pattern:**
```rust
impl ChunkScan {
    fn close(&mut self) {
        for record_page in &self.buffer_list {
            self.txn.unpin(&record_page.block_id);  // Manual unpin
        }
    }
}
```

**After RAII:**
- Remove `close()` methods that only exist for unpinning
- Handles automatically unpin on drop
- Keep `close()` only if it does other cleanup (locks, resources)

### Step 9: Handle Transaction Commit/Rollback Safely

**Problem:** Handles can outlive transaction commit, causing double-unpin.

**Current (line 8701):**
```rust
fn commit(&self) -> Result<(), Box<dyn Error>> {
    // ...
    self.buffer_list.unpin_all();  // Unpins ALL buffers
    Ok(())
}
```

**Issue:** If handles exist after commit:
1. Transaction calls `unpin_all()` → buffers unpinned, HashMap cleared
2. Handle drops → calls `unpin()` → **panics** (buffer not in HashMap)

**Solution: Track Committed State**

Update BufferList to track if transaction ended:

```rust
struct BufferList {
    buffers: RefCell<HashMap<BlockId, HashMapValue>>,
    buffer_manager: Arc<BufferManager>,
    committed: Cell<bool>,  // NEW: Track if transaction committed/rolled back
}

impl BufferList {
    fn new(buffer_manager: Arc<BufferManager>) -> Self {
        Self {
            buffers: RefCell::new(HashMap::new()),
            buffer_manager,
            committed: Cell::new(false),  // Initially not committed
        }
    }

    fn unpin(&self, block_id: &BlockId) {
        if self.committed.get() {
            // Transaction already ended and unpinned everything
            // This is a BufferHandle cleaning up after commit (no-op)
            return;
        }

        // Normal unpin logic (existing code)
        assert!(self.buffers.borrow().contains_key(block_id));
        // ... rest of unpin implementation ...
    }

    fn unpin_all(&self) {
        // Unpin all buffers in BufferManager
        let mut buffer_guard = self.buffers.borrow_mut();
        for buffer in buffer_guard.values() {
            self.buffer_manager.lock().unwrap().unpin(Arc::clone(&buffer.buffer));
        }
        buffer_guard.clear();

        // Mark as committed so future unpin() calls are no-ops
        self.committed.set(true);
    }
}
```

**Behavior:**
- Before commit: `unpin()` works normally
- After commit: `unpin()` becomes no-op (safe)
- Handles can safely outlive commit (become "zombies" but don't panic)

---

## Testing Strategy

### Test 1: Basic Pin/Unpin
```rust
#[test]
fn test_buffer_handle_raii() {
    let (db, _dir) = SimpleDB::new_for_test(400, 3);
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    {
        let handle = txn.pin(&block_id);
        // Verify pin count = 1
        assert_eq!(txn.buffer_list.get_buffer(&block_id).unwrap().lock().unwrap().pins, 1);
    }

    // After drop, pin count = 0
    assert!(txn.buffer_list.get_buffer(&block_id).is_none());
}
```

### Test 2: Clone Increments Pins
```rust
#[test]
fn test_handle_clone_pins() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let handle1 = txn.pin(&block_id);
    let handle2 = handle1.clone();

    // Both handles should keep block pinned
    assert_eq!(pin_count(&txn, &block_id), 2);

    drop(handle1);
    assert_eq!(pin_count(&txn, &block_id), 1);

    drop(handle2);
    assert_eq!(pin_count(&txn, &block_id), 0);
}
```

### Test 3: Scan Clone Works
```rust
#[test]
fn test_scan_clone_with_handles() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let layout = /* ... */;

    let chunk_scan = ChunkScan::new(Arc::clone(&txn), layout, "test", 0, 2);
    let cloned_scan = chunk_scan.clone();

    // Both scans should have independent pins
    // Verify pin counts are doubled

    drop(chunk_scan);
    // Cloned scan still works
    for _ in cloned_scan {
        // ...
    }
}
```

### Test 4: No Leaks After Transaction Commit
```rust
#[test]
fn test_no_leaks_after_commit() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let handle = txn.pin(&BlockId::new("test".to_string(), 0));

    txn.commit().unwrap();

    // All buffers should be unpinned
    // Even though handle still exists
    assert_eq!(txn.available_buffs(), 3);  // All buffers available
}
```

### Test 5: Handle Safely Drops After Commit
```rust
#[test]
fn test_handle_drop_after_commit() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);
    let handle = txn.pin(&block_id);

    // Commit unpins everything
    txn.commit().unwrap();

    // Handle still exists - this should NOT panic
    drop(handle);  // Should be no-op (committed flag prevents double-unpin)

    // Verify no crash
}
```

### Test 6: Multiple Handles After Commit
```rust
#[test]
fn test_multiple_handles_after_commit() {
    let txn = Arc::new(Transaction::new(/*...*/));
    let block_id = BlockId::new("test".to_string(), 0);

    let handle1 = txn.pin(&block_id);
    let handle2 = handle1.clone();
    let handle3 = handle2.clone();

    txn.commit().unwrap();

    // All three handles should drop safely
    drop(handle1);  // no-op
    drop(handle2);  // no-op
    drop(handle3);  // no-op
}
```

---

## Transaction Lifecycle & Handle Semantics

### Can BufferHandle outlive Transaction commit?

**Yes.** Handles can exist after `txn.commit()` is called.

**Why?** With `Arc<Transaction>`, the handle keeps the transaction object alive, but the transaction can still be in a "committed" state.

### What operations work on a handle after transaction ends?

After `txn.commit()`:

| Operation | Works? | Behavior |
|-----------|--------|----------|
| `handle.block_id()` | ✅ Yes | Returns BlockId (no transaction interaction) |
| `handle.clone()` | ✅ Yes | Calls `txn.pin()`, which checks committed flag |
| `drop(handle)` | ✅ Yes | Calls `txn.unpin()`, which becomes no-op if committed |
| `txn.get_int(handle.block_id(), ...)` | ❌ No | Locks released, may panic or return error |

**Key insight:** Handles become "zombie" objects after commit - safe to drop, but shouldn't be used for data access.

### Who unpins the buffers when transaction ends?

**Both Transaction AND BufferHandle, but safely:**

1. **Transaction.commit()** calls `buffer_list.unpin_all()`:
   - Unpins all buffers in BufferManager
   - Clears HashMap
   - Sets `committed = true`

2. **BufferHandle.drop()** calls `buffer_list.unpin()`:
   - If `committed == false`: Normal unpin (decrements count)
   - If `committed == true`: No-op (early return)

**Result:** No double-unpin panics, handles can safely outlive commit.

### Recommended Usage Pattern

**Best practice:** Drop handles before commit (explicit ownership)

```rust
{
    let txn = Arc::new(Transaction::new(/*...*/));

    {
        let handle = txn.pin(&block_id);
        // ... use handle ...
    }  // handle dropped here

    txn.commit()?;  // Clean commit, no zombie handles
}
```

**Also supported:** Handles outlive commit (but are zombies)

```rust
let txn = Arc::new(Transaction::new(/*...*/));
let handle = txn.pin(&block_id);

txn.commit()?;  // Unpins everything, sets committed=true

drop(handle);  // Safe no-op
```

---

## Advanced: ReadHandle and WriteHandle (Future Enhancement)

### Motivation

Currently, BufferHandle doesn't distinguish between read and write access. Future enhancement could add:
- **ReadHandle**: Shared access (multiple readers)
- **WriteHandle**: Exclusive access (single writer)

This would enforce **reader-writer semantics at compile time**.

### Design

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

impl Transaction {
    /// Pin for read access (shared lock)
    pub fn pin_read(&self, block_id: &BlockId) -> Result<ReadHandle, _> {
        self.concurrency_manager.slock(block_id)?;  // Acquire shared lock
        self.buffer_list.pin(block_id);
        Ok(ReadHandle {
            block_id: block_id.clone(),
            transaction: Arc::clone(self),
        })
    }

    /// Pin for write access (exclusive lock)
    pub fn pin_write(&self, block_id: &BlockId) -> Result<WriteHandle, _> {
        self.concurrency_manager.xlock(block_id)?;  // Acquire exclusive lock
        self.buffer_list.pin(block_id);
        Ok(WriteHandle {
            block_id: block_id.clone(),
            transaction: Arc::clone(self),
        })
    }
}

impl ReadHandle {
    /// Read-only access
    pub fn get_int(&self, offset: usize) -> Result<i32, _> {
        self.transaction.get_int(&self.block_id, offset)
    }

    // No set methods!
}

impl WriteHandle {
    /// Read access
    pub fn get_int(&self, offset: usize) -> Result<i32, _> {
        self.transaction.get_int(&self.block_id, offset)
    }

    /// Write access (only WriteHandle can do this)
    pub fn set_int(&self, offset: usize, val: i32, log: bool) -> Result<(), _> {
        self.transaction.set_int(&self.block_id, offset, val, log)
    }
}

impl Drop for ReadHandle {
    fn drop(&mut self) {
        self.transaction.unpin(&self.block_id);
        self.transaction.concurrency_manager.unlock(&self.block_id);  // Release slock
    }
}

impl Drop for WriteHandle {
    fn drop(&mut self) {
        self.transaction.unpin(&self.block_id);
        self.transaction.concurrency_manager.unlock(&self.block_id);  // Release xlock
    }
}
```

### Benefits

1. **Type Safety:** Compiler prevents writes through ReadHandle
2. **Clear Intent:** `pin_read()` vs `pin_write()` documents access pattern
3. **Lock Management:** Locks automatically released on drop
4. **Pedagogical:** Teaches reader-writer pattern

### Usage Example

```rust
struct RecordPage {
    tx: Arc<Transaction>,
    handle: WriteHandle,  // Exclusive access for modifications
    layout: Layout,
}

impl RecordPage {
    fn new_for_write(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = tx.pin_write(&block_id).unwrap();
        Self { tx, handle, layout }
    }

    fn set_int(&self, slot: usize, field: &str, val: i32) {
        let offset = self.offset(slot) + self.layout.offset(field).unwrap();
        self.handle.set_int(offset, val, true).unwrap();  // ✅ Allowed
    }
}

// Read-only scan
struct ScanReader {
    tx: Arc<Transaction>,
    handle: ReadHandle,  // Shared access
}

impl ScanReader {
    fn get_int(&self, offset: usize) -> i32 {
        self.handle.get_int(offset).unwrap()  // ✅ Allowed
        // self.handle.set_int(...)  // ❌ Compile error!
    }
}
```

### Implementation Notes

**When to implement:**
- After basic BufferHandle is working and tested
- When you want compile-time enforcement of read/write access
- During concurrency/locking refactor

**Complexity:**
- Requires splitting current Transaction methods into read/write variants
- Need to update all scan types to choose ReadHandle or WriteHandle
- Locks now managed by handles (simplifies concurrency management)

**Migration Strategy:**
1. Keep existing `BufferHandle` (no locks, just pins)
2. Add `ReadHandle` and `WriteHandle` alongside
3. Gradually migrate call sites
4. Eventually deprecate `BufferHandle` in favor of Read/Write variants

---

## Summary

### Core Implementation

1. **BufferHandle:** Manages pin lifetime, stores `BlockId` + `Arc<Transaction>`
2. **Clone:** Increments pin count (creates new ownership)
3. **Drop:** Decrements pin count (releases ownership)
4. **Transaction::pin():** Returns BufferHandle
5. **RecordPage/Scans:** Store handle instead of block_id

### Key Invariants

- `Number of live BufferHandles = Pin count`
- Handle doesn't provide data access (Transaction does)
- Clone = new pin, Drop = unpin
- No reference cycles (Transaction doesn't own handles)

### Migration Path

1. Implement BufferHandle with Clone/Drop
2. Update Transaction::pin() to return handle
3. Update RecordPage to store handle
4. Update scans to use handles
5. Remove manual close() methods
6. Test thoroughly
7. (Future) Add ReadHandle/WriteHandle for type-safe access control
