# Record Management and Page Format Redesign

## Overview

This document outlines the comprehensive redesign of SimpleDB's page format to integrate bitmap and ID table structures directly into the Page abstraction. The current implementation treats pages as raw byte arrays, requiring higher-level components to manually manage record organization. This redesign moves that knowledge into the Page itself, enabling more efficient record management and laying the groundwork for variable-length records.

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

### New Page Structure

The redesigned Page struct integrates record management metadata:

```rust
pub struct Page {
    header: PageHeader,           // Metadata about page state
    bitmap: RecordBitmap,         // Track slot occupancy
    id_table: IdTable,            // Map slots to record offsets
    record_space: Vec<u8>,        // Actual record data
}

struct PageHeader {
    slot_count: u32,      // Number of slots currently used
    free_ptr: u32,        // Offset where free space begins
}

struct RecordBitmap {
    bits: [u8; 32],       // 256 bits for slot presence
}

struct IdTable {
    offsets: [u16; 256],  // 2-byte offsets per slot
}
```

### Physical Layout

```
PROPOSED PAGE LAYOUT (4096 bytes)
═══════════════════════════════════════════════════════════════════

┌───────────────────────────────────────────────────────────────┐
│                         4096-byte Page                         │
├────────┬────────┬─────────────────┬────────────────────────────┤
│ Header │ Bitmap │    ID Table     │       Record Space         │
│8 bytes │32 bytes│   512 bytes     │       3544 bytes           │
├────────┼────────┼─────────────────┼────────────────────────────┤
│        │        │                 │                            │
│ - Slot │ - 1 bit│ - 2-byte offset │  - Records grow from here  │
│  count │  per   │   per slot      │    toward the left         │
│ - Free │  slot  │ - Points to     │  - Each record has a       │
│  ptr   │ 256 max│   record start  │    4-byte header           │
└────────┴────────┴─────────────────┴────────────────────────────┘
   0        8          40                552              4096
```

### Size Calculations

Starting from first principles:

**1. Minimum record size:**
- Record header: 4 bytes (2B length + 2B flags)
- Minimum field: 4 bytes (one integer)
- **Minimum record = 8 bytes**

**2. Theoretical maximum slots:**
- Page size: 4096 bytes
- Theoretical max: 4096 / 8 = 512 slots

**3. Practical slot count: 256**
- Use 256 instead of 512 for practical reasons
- Still allows good space utilization
- Keeps bitmap and ID table reasonably sized

**4. Bitmap size:**
- 256 slots × 1 bit = 256 bits
- **Bitmap = 32 bytes**

**5. ID table size:**
- Each entry: 2 bytes (can address up to 65,536 byte offsets >> 4,096)
- 256 entries × 2 bytes = **ID table = 512 bytes**

**6. Header size:**
- slot_count: 4 bytes (u32)
- free_ptr: 4 bytes (u32)
- **Header = 8 bytes**

**7. Record space:**
- 4096 - 8 - 32 - 512 = **3544 bytes**

### Component Details

**Header (8 bytes):**
```
Offset 0-3: slot_count (u32)
  • How many slots are currently in use
  • Incremented when new slot allocated
  • Used to find next available slot

Offset 4-7: free_ptr (u32)
  • Points to start of free space in record area
  • Initially: 552 (right after ID table)
  • Updated when records are inserted
```

**Bitmap (32 bytes):**
```
Offset 8-39: 256 bits for slot presence

┌──────────────────────────────────────┐
│ Byte 0 │ Byte 1 │ ... │ Byte 31     │
│ 8 bits │ 8 bits │ ... │ 8 bits      │
└──────────────────────────────────────┘

Bit N = 1: Slot N is occupied
Bit N = 0: Slot N is free

Operations:
  • is_set(slot): Check if slot occupied - O(1)
  • set(slot): Mark slot as occupied - O(1)
  • clear(slot): Mark slot as free - O(1)
  • find_free(): Scan for first free slot - O(n)
```

**ID Table (512 bytes):**
```
Offset 40-551: 256 entries × 2 bytes

┌──────────────────────────────────┐
│ Slot 0 offset (u16)              │
│ Slot 1 offset (u16)              │
│ Slot 2 offset (u16)              │
│ ...                              │
│ Slot 255 offset (u16)            │
└──────────────────────────────────┘

Entry = 0: Slot is unused
Entry > 0: Offset to record start (relative to page start)

Benefits:
  • Direct O(1) lookup of record location
  • Enables variable-length records
  • Can rearrange records without changing slot IDs
  • Foundation for compaction
```

**Record Space (3544 bytes):**
```
Offset 552-4095: Variable-length record storage

Records are placed at offsets pointed to by ID table:
┌────────────────────────────────────┐
│ Record at offset 552 (slot 0)     │
├────────────────────────────────────┤
│ Record at offset 604 (slot 5)     │
├────────────────────────────────────┤
│ Record at offset 650 (slot 2)     │
├────────────────────────────────────┤
│           Free space...             │
└────────────────────────────────────┘
```

---

## Design Details

### Page Operations

**Initialize new page:**
```rust
impl Page {
    fn init_as_record_page(&mut self) {
        // Clear header
        self.set_u32(0, 0);    // slot_count = 0
        self.set_u32(4, 552);  // free_ptr = start of record space

        // Clear bitmap (all slots free)
        for i in 0..32 {
            self.contents[8 + i] = 0;
        }

        // Clear ID table (all offsets = 0)
        for i in 0..512 {
            self.contents[40 + i] = 0;
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

**Cost:** Every page pays 552 bytes (13.5%) for metadata

```
Overhead breakdown:
  Header:    8 bytes (0.2%)
  Bitmap:   32 bytes (0.8%)
  ID table: 512 bytes (12.5%)
  ─────────────────────────
  Total:    552 bytes (13.5% of 4096-byte page)
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

## Appendix: Example Page State

```
EXAMPLE: Page with 3 records inserted
═══════════════════════════════════════════════════════════════════

Header (offset 0-7):
  slot_count = 3
  free_ptr = 696

Bitmap (offset 8-39):
  [0x05, 0x00, ...] = 0b00000101
  Meaning: Slots 0 and 2 are occupied, slot 1 is free

ID Table (offset 40-551):
  [0] = 552  (slot 0 at offset 552)
  [1] = 0    (slot 1 unused)
  [2] = 600  (slot 2 at offset 600)
  [3] = 648  (slot 3 at offset 648)
  ...

Record Space (offset 552-4095):
  @ 552: Record for slot 0 (48 bytes)
  @ 600: Record for slot 2 (48 bytes)
  @ 648: Record for slot 3 (48 bytes)
  @ 696: Free space begins

Visual:
┌──────────────────────────────────────────────────────────────┐
│ Header: count=3, free=696                                     │
├──────────────────────────────────────────────────────────────┤
│ Bitmap: [1,0,1,1,0,0,...]                                    │
├──────────────────────────────────────────────────────────────┤
│ ID Table: [552, 0, 600, 648, 0, ...]                         │
├──────────────────────────────────────────────────────────────┤
│ 552: [Rec 0] (slot 0)                                        │
│ 600: [Rec 2] (slot 2)                                        │
│ 648: [Rec 3] (slot 3)                                        │
│ 696: ████████ Free Space ████████████████████████████         │
└──────────────────────────────────────────────────────────────┘
```

This structured format provides the foundation for efficient record management in SimpleDB.
