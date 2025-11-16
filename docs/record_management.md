# Record Management and Page Format Redesign

## Overview

This document outlines the comprehensive redesign of SimpleDB's page format to use a line-pointer array plus heap layout. The current implementation treats pages as raw byte arrays, requiring higher-level components to manually manage record organization. The redesign moves that knowledge into the Page itself, enabling variable-length records, MVCC-friendly metadata, and deterministic on-disk images suitable for direct I/O.

## Motivation

The current Page implementation is pedagogically simple but lacks the structure needed for efficient record management:

- **No built-in slot tracking**: Must scan records to find empty slots
- **No presence bitmap**: Can't quickly determine which slots are occupied
- **No ID table**: Can't support variable-length records efficiently
- **Raw byte access**: Higher-level components must manage all structure

These limitations make it difficult to implement advanced features like variable-length strings, record compaction, and efficient space reclamation.

From the implementation experience (see [Claude chat](https://claude.ai/chat/b36c7658-2311-479b-acc9-945e95ea2ba5)):
> "Upon trying to implement this approach, I noticed that I have to do a bunch of nonsense to get around the design of the `Page` and `Transaction` objects because those don't have the concept & affordances of bitmaps and ID tables. If I truly want to modify this, I should change the actual `Page` format so that it has knowledge of the bitmap and the ID table. That way, all these changes can go through the `RecoveryManager` correctly."

---

## Current Architecture

### Layer Abstractions

SimpleDB's record management uses a "lens" analogy where each layer provides a different view of the same data. Based on [Chapter 6 discussion](https://claude.ai/chat/8dce4c6d-507f-4b8d-98ce-61a819d3c88f):

```
┌────────────────────────────────────────────────────────────────┐
│                    ABSTRACTION LAYERS                           │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  TableScan Layer                                               │
│  • Iterator interface over all records                         │
│  • Handles movement between blocks automatically               │
│  • Abstracts away slots and blocks from clients                │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  RecordPage Layer                                              │
│  • Understands how records are organized within a block        │
│  • Uses slots to organize records                              │
│  • Knows about record structure through Layout                 │
│  • Provides record-level operations within a single block      │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Page Layer                                                    │
│  • In-memory view of block's contents                          │
│  • Methods to read/write basic types (int, string)             │
│  • Handle byte ordering and type conversion                    │
│  • Currently: Just raw bytes (Vec<u8>)                         │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Block Layer                                                   │
│  • Fixed-size chunk identified by filename + block number      │
│  • Location-based access to data                               │
│                                                                 │
├────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Raw Storage Layer                                             │
│  • Raw bytes in files with no inherent meaning                 │
│                                                                 │
└────────────────────────────────────────────────────────────────┘
```

### Current Page Structure

```rust
pub struct Page {
    pub contents: Vec<u8>,  // Raw bytes with no structure
}

impl Page {
    fn get_int(&self, offset: usize) -> i32 { ... }
    fn set_int(&mut self, offset: usize, n: i32) { ... }
    fn get_string(&self, offset: usize) -> String { ... }
    fn set_string(&mut self, offset: usize, s: &str) { ... }
}
```

**Characteristics:**
- Page is just a byte array wrapper
- No knowledge of record organization
- All structure managed by RecordPage layer
- Simple but inflexible

### Current Record Organization

```
CURRENT PAGE LAYOUT (4096 bytes)
═══════════════════════════════════════════════════════════════════

┌────────────────────────────────────────────────────────────────┐
│                     Sequential Fixed-Length Slots               │
├──────────┬──────────┬──────────┬──────────┬──────────┬─────────┤
│ Record 0 │ Record 1 │ Record 2 │ Record 3 │   ...    │  Free   │
│ (slot)   │ (slot)   │ (slot)   │ (slot)   │          │  Space  │
└──────────┴──────────┴──────────┴──────────┴──────────┴─────────┘

Each record slot:
┌──────┬─────────────────────────────┐
│ Flag │      Field Data             │
│ 4B   │  (determined by Layout)     │
└──────┴─────────────────────────────┘

Slot N located at: offset = N × layout.slot_size

Problems:
✗ Must know Layout to calculate offsets
✗ No way to find free slots without scanning
✗ Deleted records leave gaps (wasted space)
✗ All records must be same size (fixed by Layout)
✗ No built-in metadata about page state
```

### Key Components

**Schema**: Logical structure of records
```rust
let mut schema = Schema::new();
schema.add_int_field("student_id");
schema.add_string_field("name", 20);
schema.add_int_field("grade");
```
Defines **what** fields exist and their types.

**Layout**: Physical organization
```rust
let layout = Layout::new(schema);
// Calculates:
// - student_id at offset 4
// - name at offset 8
// - grade at offset 32
// - slot_size = 36 bytes
```
Defines **where** fields are stored.

**Record ID (RID)**: Unique identifier
```rust
struct RID {
    block_num: usize,
    slot: usize,
}
```
Pinpoints exact record location for direct access and indexing.

---

## Proposed Architecture

### Page Structure

```rust
pub struct Page {
    header: PageHeader,       // Metadata about page state
    line_ptrs: Vec<LinePtr>,  // Slot array growing downward
    record_space: Vec<u8>,    // Heap growing upward
}

#[repr(u8)]
enum PageType {
    Heap = 0,
    IndexLeaf = 1,
    IndexInternal = 2,
    Overflow = 3,
    Meta = 4,
    Free = 255,
}

struct PageHeader {
    page_type: PageType,  // Heap, index leaf, etc.
    slot_count: u16,      // Number of active slots
    free_lower: u16,      // End of line-pointer array
    free_upper: u16,      // Start of free heap space
    free_ptr: u32,        // Next heap allocation offset (bump)
    crc32: u32,           // Page checksum
    latch_word: u64,      // Spin/seqlock metadata
    free_head: u16,       // Head of free-slot freelist (slot id) or 0xFFFF if none
    reserved: [u8; 6],    // Future (LSN, FSM hints, padding)
}

/// 4-byte packed line pointer: offset + length + state
#[derive(Clone, Copy)]
struct LinePtr(u32);

impl LinePtr {
    // bits 31..16: offset (u16, supports page sizes up to 64KB)
    // bits 15..4 : length (12 bits, up to 4095 bytes)
    // bits 3..0  : state  (4 bits: FREE, LIVE, DEAD, REDIRECT, COMPRESSED, etc.)
}
```

### Physical Layout (4096 bytes)

```
┌───────────────────────────────────────────────────────────────┐
│                         4096-byte Page                         │
├──────────┬─────────────────────┬───────────────────────────────┤
│ Header   │ Line Ptr Array      │         Record Heap           │
│ 32 bytes │ grows downward      │         grows upward          │
├──────────┼─────────────────────┼───────────────────────────────┤
│ page_type│ lp[0] lp[1] ...     │  Tuples with MVCC + nullmap   │
│ free_*   │ free_lower ►        │  free_upper/free_ptr ►        │
│ crc32    │                     │                               │
└──────────┴─────────────────────┴───────────────────────────────┘
   0          32             free_lower            free_upper    4096
```

### Size Calculations

Starting from first principles:

**1. Minimum record size:**
- Tuple header: 24 bytes (`xmin` 8B + `xmax` 8B + `flags` 2B + `nullmap_ptr` 2B + `payload_len` 4B)
- Minimum field: 4 bytes (one integer)
- **Minimum record = 28 bytes**

**2. Theoretical maximum slots:**
- Page size: 4096 bytes
- Theoretical max: page bytes / min line pointer stride ≈ 4096 / 4 = 1024 line pointers (before they collide with heap)

**3. Practical slot count: dynamic**
- Line pointers grow until they meet the heap; no hard cap like the bitmap+ID table approach.

**4. Line pointer size:**
- 4 bytes per slot (u16 offset, 12-bit len, 4-bit state)

**5. Slot directory size:**
- `4 * slot_count`; adjusts to workload (many small tuples → more pointers, few large tuples → fewer).

**6. Header size:**
- page_type + slot_count/free-lower/free-upper (6 bytes total)
- free_ptr: 4 bytes (u32)
- crc32: 4 bytes
- latch_word: 8 bytes
- free_head: 2 bytes
- reserved bytes: 6 bytes
- **Header = 32 bytes** (padded from 30 for alignment/power-of-two)

**7. Record space:**
- 4096 - 32 - 4*slot_count = **variable**, shared between line pointers and heap

### Component Details

**Header (32 bytes):**
```
Offset 0:   page_type (u8)
Offset 1:   reserved_type_flags (u8)
Offset 2-3: slot_count (u16)
Offset 4-5: free_lower (u16)   // end of line-pointer array
Offset 6-7: free_upper (u16)   // start of free heap
Offset 8-11: free_ptr (u32)    // bump cursor for heap inserts
Offset 12-15: crc32 (u32)      // checksum of entire page
Offset 16-23: latch_word (u64) // spin/seqlock metadata
Offset 24-25: free_head (u16)  // freelist head slot id, 0xFFFF if none
Offset 26-31: reserved bytes   // LSN, FSM hints, padding
```
- Byte math: 2 (type+flags) + 2 + 2 + 4 + 4 + 8 + 2 + 6 = 30; padded to 32 for alignment and future growth.
- `free_lower/free_upper`: contiguous free space is `free_upper - free_lower`; compaction slides these together.
- `free_ptr`: bump allocator cursor; typically equals `free_upper` after compaction.
- `free_head`: O(1) free-slot allocation via freelist threaded through FREE line pointers.
- `crc32`: compute over the full 4KB image with the crc32 field zeroed; store little-endian; recompute after any change.
- `latch_word`: reserved for per-page concurrency. Seqlock-style: writers set it odd, mutate page, recompute CRC, bump to next even; readers retry if they observe odd or changed value. Could alternatively encode a tiny spinlock owner id.
- `reserved`: space for per-page LSN, FSM hints, and other recovery metadata.

### Free Space Tracking & FSM Integration

- `free_lower` and `free_upper` mirror PostgreSQL-style bounds; publish `(block_id, free_upper - free_lower)` into a free-space map.
- Reserved bytes can later hold largest-hole/fragmentation hints if we measure them.
- Reserved bytes are also the planned home for per-page LSN to coordinate WAL redo/undo; redo must rewrite line pointers and heap consistently before recomputing CRC.
- Page latch protocol (future use of `latch_word`):
  - Readers: check `latch_word`; if odd, retry; read page; recheck; retry on change/odd.
  - Writers: spin/CAS to make `latch_word` odd (exclusive), mutate page, recompute CRC, bump to next even.
  - Logical locks live in the lock table; latches are short critical-section guards only.
- PageType-specific invariants:
  - Heap: tuple header with xmin/xmax/flags/nullmap_ptr; line pointers reference heap tuples; REDIRECT allowed; compaction/vacuum apply.
  - IndexLeaf / IndexInternal: line pointers reference index cells (key + child/row ref); MVCC semantics may differ; REDIRECT typically unused.
  - Overflow: line pointers reference spill fragments; heap payload is raw continuation.
  - Meta / Free: reserved for catalog/meta or reclaimed pages; interpretation defined by higher layers.
- Visual (line pointers down, heap up):
```
| offset 0  --------------------------- Header (32B) --------------------------- |
| 32      lp[0] lp[1] ... lp[n-1]  (4B each)   free_lower ►                     |
|                <------------------------- free space ---------------------> <- free_upper/free_ptr |
|                                  heap tuples grow upward                      |
| 4096  ---------------------------------------------------------------------- |
```

**Line Pointer Array (no bitmap/ID table):**
```
LinePtr (4 bytes):
  bits 31..16: offset (u16) up to 64KB page
  bits 15..4 : length (12 bits) up to 4095 bytes
  bits 3..0  : state (FREE, LIVE, DEAD, REDIRECT, COMPRESSED, ...)
```
- When state=FREE, the length field stores `next_free` slot id (0xFFF sentinel for end of list); `free_head` points to the first free slot.
- Slot-state encoding (4 bits, example):
  - 0: FREE
  - 1: LIVE
  - 2: DEAD (tombstone, reclaimable)
  - 3: REDIRECT (follow offset to replacement tuple)
  - 4: COMPRESSED
  - 15: RESERVED (future)

**Record Space (heap):**
- Starts at `free_upper`; grows upward as tuples are appended/moved.

**Tuple Header (per record, 24 bytes):**
```
Offset +0..+3 : payload_len (u32)
Offset +4..+11: xmin (u64)        // creator Txn id
Offset +12..+19: xmax (u64)       // deleter/updater Txn id
Offset +20..+21: flags (u16)      // tombstone, HOT redirect, compression bits
Offset +22..+23: nullmap_ptr (u16)// offset within payload
```
- `flags` include tombstone/HOT/compression; `nullmap_ptr` is the column NULL bitmap anchor.
- Recommendation: place the NULL bitmap immediately after the tuple header to keep payload parsing simple and compact.

---

## Design Details

### Compaction Flow

1. Scan line pointers for state=LIVE (or REDIRECT); gather slot ids and their offsets/lengths.
2. Sort by current offset to walk heap tuples in physical order.
3. Copy each live tuple to the top of the heap starting at `free_lower`; update that slot’s line pointer (offset/len/state).
4. After the last copy, set `free_upper` and `free_ptr` to the next free byte; rebuild freelist (`free_head`) from slots in FREE/DEAD state; zero remaining heap for deterministic images.
5. Recompute `crc32`; publish new free-space value to the FSM.

Slot ids (RIDs) remain stable; only their offsets change.

### Page Operations

**Initialize new page:**
```rust
impl Page {
    fn init_as_record_page(&mut self) {
        // Clear header
        self.set_u16(2, 0);    // slot_count = 0
        self.set_u16(4, 64);   // free_lower right after bitmap
        self.set_u16(6, 576);  // free_upper start of heap
        self.set_u32(8, 576);  // free_ptr = start of record space

        // Clear bitmap (all slots free)
        for i in 0..32 {
            self.contents[8 + i] = 0;
        }

        // Clear ID table (all offsets = 0)
        for i in 0..512 {
            self.contents[64 + i] = 0;
        }
    }
}
```

**Allocate new slot:**
```rust
impl Page {
    fn allocate_slot(&mut self, record_size: usize)
        -> Result<(usize, usize), Error>
    {
        // 1. Find free slot in bitmap
        let slot = self.bitmap.find_free()
            .ok_or("No free slots available")?;

        // 2. Get current free pointer
        let offset = self.header.free_ptr as usize;

        // 3. Check if enough space
        if offset + record_size > 4096 {
            return Err("Insufficient space for record".into());
        }

        // 4. Update bitmap
        self.bitmap.set(slot);

        // 5. Update ID table
        self.id_table.set(slot, offset as u16);

        // 6. Update free pointer
        self.header.free_ptr += record_size as u32;

        // 7. Increment slot count
        self.header.slot_count += 1;

        Ok((slot, offset))
    }
}
```

**Get record location:**
```rust
impl Page {
    fn get_record_offset(&self, slot: usize) -> Option<usize> {
        if !self.bitmap.is_set(slot) {
            return None;  // Slot not used
        }

        let offset = self.id_table.get(slot);
        if offset == 0 {
            return None;  // Invalid offset
        }

        Some(offset as usize)
    }
}
```

**Delete record:**
```rust
impl Page {
    fn delete_slot(&mut self, slot: usize) -> Result<(), Error> {
        if !self.bitmap.is_set(slot) {
            return Err("Slot not in use".into());
        }

        // 1. Clear bitmap
        self.bitmap.clear(slot);

        // 2. Clear ID table entry
        self.id_table.set(slot, 0);

        // 3. Decrement slot count
        self.header.slot_count -= 1;

        // Note: Free space not reclaimed until compaction

        Ok(())
    }
}
```

### RecordPage Integration

RecordPage will use the new Page API:

```rust
impl RecordPage {
    fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = tx.pin(&block_id);

        // Initialize page structure if new
        if tx.block_size() == handle.page().header.free_ptr {
            handle.page().init_as_record_page();
        }

        Self { tx, handle, layout }
    }

    fn insert_after(&self, slot: Option<usize>) -> Result<usize, Error> {
        // Use Page's allocation logic
        let (new_slot, offset) = self.handle
            .page()
            .allocate_slot(self.layout.slot_size)?;

        // Write record at offset
        self.write_record_at(new_slot, offset);

        Ok(new_slot)
    }

    fn is_slot_used(&self, slot: usize) -> bool {
        self.handle.page().bitmap.is_set(slot)
    }
}
```

### Benefits Over Current Design

```
FEATURE COMPARISON
═══════════════════════════════════════════════════════════════════

Operation              Current          Proposed
─────────────────────────────────────────────────────────────────
Check slot used        O(1) read flag   O(1) bitmap check
Find free slot         O(n) scan slots  O(n) scan bitmap*
Get record offset      O(1) calculate   O(1) ID table lookup
Variable-length        Not possible     Supported via ID table
Space reclamation      Not possible     Possible with compaction
Direct slot access     Yes              Yes
Record metadata        None             Header tracks state

* Future: Could maintain free list for O(1) allocation
```

**Key Advantages:**

1. **Structured metadata**: Page knows its own state
2. **Variable-length ready**: ID table provides indirection
3. **Space efficiency**: Can compact and reclaim space
4. **Recovery-friendly**: Structured format easier to log/recover
5. **Pedagogically clear**: Explicitly shows record management concepts

---

## Implementation Strategy

### Phase 1: Core Page Redesign

**Goal:** Implement new Page structure without breaking existing code

**Tasks:**
- [ ] Define new Page struct with header, bitmap, ID table
- [ ] Implement PageHeader with slot_count and free_ptr
- [ ] Implement RecordBitmap with bit manipulation
  - [ ] `is_set(slot) -> bool`
  - [ ] `set(slot)`
  - [ ] `clear(slot)`
  - [ ] `find_free() -> Option<usize>`
- [ ] Implement IdTable with offset tracking
  - [ ] `get(slot) -> u16`
  - [ ] `set(slot, offset)`
- [ ] Add Page initialization method
- [ ] Add unit tests for each component

**Complexity:** 2-3 days

**Acceptance Criteria:**
- Page struct compiles with new fields
- Bitmap operations work correctly
- ID table stores and retrieves offsets
- Tests verify all operations

---

### Phase 2: RecordPage Integration

**Goal:** Update RecordPage to use new Page structure

**Tasks:**
- [ ] Modify RecordPage to use Page bitmap for slot checking
- [ ] Update slot allocation to use Page's allocate_slot()
- [ ] Use ID table for record offset lookup
- [ ] Update iteration to use bitmap for finding used slots
- [ ] Ensure slot flag compatibility (transition period)
- [ ] Update all RecordPage tests

**Complexity:** 3-4 days

**Acceptance Criteria:**
- RecordPage works with new Page structure
- Existing RecordPage tests pass
- Slot allocation uses new mechanism
- Record access uses ID table

---

### Phase 3: Recovery Manager Updates

**Goal:** Ensure WAL and recovery work with new page format

**Tasks:**
- [ ] Update RecoveryManager to understand structured pages
- [ ] Ensure WAL logs properly initialize page structure
- [ ] Handle page initialization in recovery
- [ ] Test crash recovery with new page format
- [ ] Verify redo/undo operations work correctly

**Complexity:** 3-5 days

**Acceptance Criteria:**
- WAL logs include page structure initialization
- Recovery correctly rebuilds pages
- All recovery tests pass
- No data loss in crash scenarios

---

### Phase 4: Buffer Pool Integration

**Goal:** Ensure BufferManager works with new Page format

**Tasks:**
- [ ] Verify buffer assignment initializes page structure
- [ ] Update Page serialization for disk writes
- [ ] Ensure proper deserialization on reads
- [ ] Test buffer eviction and reload
- [ ] Verify all buffer pool tests pass

**Complexity:** 2-3 days

**Acceptance Criteria:**
- Buffers correctly serialize new page format
- Pages deserialize correctly on reload
- Buffer pool tests pass
- No data corruption on eviction/reload

---

### Phase 5: Compaction (Optional Future Work)

**Goal:** Implement record space compaction to reclaim deleted space

**Tasks:**
- [ ] Implement Page compaction algorithm
- [ ] Update ID table offsets after compaction
- [ ] Decide compaction trigger policy
- [ ] Add compaction tests
- [ ] Measure space savings

**Complexity:** 4-6 days

**Acceptance Criteria:**
- Compaction reclaims deleted space
- ID table correctly updated
- No data corruption during compaction
- Measurable space savings

---

## Trade-offs and Considerations

### Fixed Overhead

**Cost:** Every page pays 576 bytes (14.1%) for metadata

```
Overhead breakdown:
  Header:   32 bytes (0.8%)
  Bitmap:   32 bytes (0.8%)
  ID table: 512 bytes (12.5%)
  ─────────────────────────
  Total:    576 bytes (14.1% of 4096-byte page)
```

**Is it worth it?**
- ✓ For large records: Yes (small percentage of total)
- ✗ For tiny records: Questionable (high relative overhead)
- ✓ For variable-length: Absolutely (enables key feature)

### Complexity Increase

**Current:** Page is just `Vec<u8>` - extremely simple

**Proposed:** Page has 4 components with internal structure

**Justification:**
- Pedagogically valuable to show real database concepts
- Necessary foundation for advanced features
- Still simpler than production databases (e.g., PostgreSQL's page format)

### Slot Limit: 256 vs 512

**Why 256?**
- Bitmap fits in 32 bytes (power of 2)
- ID table is 512 bytes (reasonable size)
- Sufficient for most use cases
- Simpler arithmetic

**Could use 512:**
- Would need 64-byte bitmap
- Would need 1024-byte ID table
- Less space for records (2972 bytes vs 3544)
- Not worth the trade-off

---

## Related Issues

- **Supersedes:** Issue #7 (Store bitmap for presence checking) - now part of Page structure
- **Supersedes:** Issue #8 (Implement ID table for variable length strings) - now part of Page structure
- **Enables:** Variable-length record support (future work)
- **Enables:** Record compaction and space reclamation (future work)
- **Prerequisite for:** Advanced type system with variable-length types

---

## References

### Claude Code Conversations

1. **[Chapter 6 Discussion](https://claude.ai/chat/8dce4c6d-507f-4b8d-98ce-61a819d3c88f)** - Core abstractions and layer design
2. **[Implementation Challenges](https://claude.ai/chat/b36c7658-2311-479b-acc9-945e95ea2ba5)** - Issues with current Page design and need for structured format
3. **[Additional Design Discussion](https://claude.ai/chat/191a1311-abdf-47c6-b92a-1e7fb617f545)** - Further refinements to page layout

### Industry References

- **PostgreSQL Page Format:** [PostgreSQL Internals](https://www.interdb.jp/pg/pgsql05.html) - Industry example of structured page layout
- **MySQL InnoDB Page Structure:** Shows similar concepts with page directory and heap organization
- **SQLite Page Format:** Simpler example with B-tree page organization

---

## Appendix: Example Page State (Line Pointers)

```
EXAMPLE: Page with 3 records inserted
═══════════════════════════════════════════════════════════════════

Header (offset 0-31):
  page_type = Heap
  slot_count = 3
  free_lower = 44      // 32B header + 3*4B line ptrs
  free_upper = 720
  free_ptr   = 720
  free_head  = 0xFFFF  // no free slots
  crc32      = 0xDEADBEEF

Line Pointer Array (from offset 32):
  lp[0] = offset 576, len 48, state=LIVE
  lp[1] = offset   0, len any, state=FREE (unused)
  lp[2] = offset 624, len 48, state=LIVE
  lp[3] = offset 672, len 48, state=LIVE
  free_lower = 32 + 4*3 = 44

Record Space (heap, offset 720-4095):
  @ 576: Record for slot 0 (48 bytes)
  @ 624: Record for slot 2 (48 bytes)
  @ 672: Record for slot 3 (48 bytes)
  @ 720: Free space begins

Visual:
┌──────────────────────────────────────────────────────────────┐
│ Header: type=Heap, count=3, free_ptr=720                      │
├──────────────────────────────────────────────────────────────┤
│ Line Ptrs: [ (576,LIVE), (FREE), (624,LIVE), (672,LIVE) ]     │
├──────────────────────────────────────────────────────────────┤
│ 576: [Rec 0] (slot 0)                                         │
│ 624: [Rec 2] (slot 2)                                         │
│ 672: [Rec 3] (slot 3)                                         │
│ 720: ████████ Free Space ████████████████████████████          │
└──────────────────────────────────────────────────────────────┘
```

This structured format provides the foundation for efficient record management in SimpleDB.
