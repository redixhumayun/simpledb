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

### RecordPage + Layout Integration

Once the Page owns the physical concerns (line pointers, heap management, redirects), the existing `RecordPage` layer can shift to a thin logical wrapper:

1. **RID → LinePtr:** Higher layers still identify tuples via `RID { block_num, slot }`. `RecordPage` pins the page, fetches the slot’s line pointer, follows any HOT redirects, and obtains `(offset, length)`.
2. **LinePtr → HeapTuple:** Using the offset/length, `RecordPage` asks `Page::tuple(slot)` for a `HeapTuple` view (header + payload slice). Page never touches schema-specific details; it just vouches for the bytes.
3. **HeapTuple → LogicalRow:** `Layout` (which already owns schema + per-column definitions) now interprets the tuple payload. It knows where the null bitmap sits (`nullmap_ptr`), which columns are fixed-width vs. varlen, and how to decode them. The result is a `LogicalRow` (or similarly named view) that exposes typed getters without exposing raw bytes.
4. **Lazy decoding:** A `LogicalRow` can delay actual deserialization until a caller requests a column. It keeps references to the `HeapTuple` slice plus the `Layout`. When `row.get_string("name")` runs, the layout consults the null bitmap to see if column 1 is NULL, and only then reads the length prefix + string bytes. This keeps scans cheap when a plan only touches a subset of columns.

**Responsibilities by layer:**

- *Page:* slot allocation, freelist management, redirects, tuple byte access. No schema awareness.
- *RecordPage:* wraps a page and current block id to expose CRUD + iteration in terms of `RID`s. Handles redirect chasing, delegating tuple decoding to Layout.
- *Layout:* serializes/deserializes a tuple payload given a schema. Provides helpers like `encode_row(&RowValues) -> Vec<u8>` and `decode_field(HeapTuple, column_idx) -> Value` using the null bitmap + per-column offsets.

### Buffer/Transaction Integration & Page Guards

- **Coarse buffer lock stays (for now):** each `BufferFrame` continues to sit behind a single `Mutex`. That mutex protects frame metadata (pin count, replacement list links, dirty bits) *and* provides safety while the new APIs are wired up. We’ll refine later, but nothing else in the system needs to change yet.
- **PageGuard abstraction:** introduce a guard object that:
  1. Pins a block (RAII via the existing `BufferHandle`).
  2. Acquires the per-page latch stored in `PageHeader::latch_word` (shared for readers, exclusive for writers).
  3. Uses internal `unsafe` plumbing to reinterpret the frame’s bytes as `Page<HeapPage>` and exposes a safe wrapper (`PageView`) with the full tuple API.
  4. Releases the latch and unpins on `Drop`.
- **Concurrency semantics:** page latches enforce access: multiple `PageGuard`s can hold shared (read) latches simultaneously, but only one guard may hold the exclusive (write) latch. The borrow checker can then trust that any method returning `&mut PageView` really has exclusive access because it’s tied to the guard’s lifetime + latch token.
- **RecordPage / Transaction usage:** higher layers stop calling `tx.get_int`/`set_string`. Instead they request `PageGuard`s (read or write) from the transaction, operate via the safe `PageView` API, and drop the guard when done.
- **Why unsafe is contained:** All `unsafe` stays inside the guard implementation (where we pull raw pointers out of the frame). Consumers only see a safe API that mirrors Rust’s borrowing rules, with the latch providing the runtime guarantee that those rules aren’t violated.

High-level sketch:

```rust
pub enum LatchMode {
    Shared,
    Exclusive,
}

pub struct PageGuard<'a, K: PageKind> {
    handle: BufferHandle,     // pins/unpins via Drop
    latch: PageLatchToken,    // releases latch on Drop
    page: PageView<'a, K>,    // safe wrapper exposing Page API
}

impl<'a, K: PageKind> PageGuard<'a, K> {
    pub fn pin(
        txn: Arc<Transaction>,
        block_id: BlockId,
        mode: LatchMode,
    ) -> Result<Self, Error> {
        let handle = BufferHandle::new(block_id.clone(), txn.clone());
        let frame = txn.buffer_manager.lookup(&block_id)?; // Arc<Mutex<BufferFrame>>
        let bytes = frame.borrow_bytes()?;                 // grabs &mut [u8] under the mutex
        drop(frame);                                       // release coarse lock immediately
        let latch = latch_page(bytes, mode)?;              // CAS/spin on PageHeader::latch_word
        let page = PageView::new(bytes);                   // wraps raw ptr with lifetime marker
        Ok(PageGuard { handle, latch, page })
    }

    pub fn page(&mut self) -> &mut PageView<'_, K> {
        &mut self.page
    }
}

impl<'a, K: PageKind> Drop for PageGuard<'a, K> {
    fn drop(&mut self) {
        self.latch.release();
        // BufferHandle’s Drop automatically unpins
    }
}
```

`PageView<'a, K>` is a thin wrapper that implements `Deref<Target = Page<K>>` / `DerefMut`, using `unsafe` inside to reinterpret the buffer bytes once the latch guarantees exclusivity.

### Locking Strategy & Migration Plan

#### Why both logical locks and page latches?
- `LockTable` handles logical isolation (table/row). Once it moves to RID granularity, two writers on different rows of the same page must be allowed to proceed; otherwise throughput collapses.
- We still need a short-lived physical latch on the buffer frame to prevent torn writes while multiple txns touch the same page. That’s what `PageGuard` and the page latch provide.
- Issue #59 tracks refactoring `LockTable` to `{table, rid}` keys so logical locks only block real conflicts while page latches guard the bytes.

#### Seqlock vs. RwLock
- A latch word + seqlock allows optimistic readers: writer sets version odd → mutate → bump to even; readers sample `seq0`, read, then sample `seq1`. If `seq0` or `seq1` is odd or they differ, retry. The high bits of `latch_word` should be ≥32 bits to avoid frequent wraparound.
- If we’re fine with classic shared/exclusive guards, we can wrap the page bytes in `RwLock<PageBytes>` and hand out `PageReadGuard`/`PageWriteGuard` without any seqlock logic. This keeps everything in safe Rust (no raw pointers) at the cost of a per-frame lock object.

#### Phased rollout
1. **Introduce helpers:** keep `Arc<Mutex<BufferFrame>>` but add `FrameMeta` + helper methods (`with_meta`, `with_page_access`) so callers stop poking fields directly.
2. **Migrate users:** update BufferManager, replacement policy, and intrusive DLL code to use the helpers; behavior unchanged because the outer mutex still serializes everything.
   - 2a. Convert `buffer_pool` entries to `Arc<BufferFrame>` with internal `meta: Mutex<FrameMeta>` and `page: RwLock<PageBytes>`, keeping the public API shape intact during the swap.
   - 2b. Update pin/unpin, eviction, and replacement-policy codepaths to lock only `meta` or `page` as needed, ensuring statistics and `BufferHandle` semantics remain unchanged.
   - 2c. Teach transactional callers (`Transaction::get_int/set_*`, `RecordPage`, recovery) to acquire the appropriate read/write guard from the frame instead of taking a coarse mutex.
   - 2d. Re-run the full AGENTS.md test + benchmark matrix (all feature-flag combinations) with explicit command-level timeouts to catch deadlocks introduced by the split before moving on.
3. **Split locks:** replace the outer `Mutex<BufferFrame>` with `Arc<BufferFrame>` containing `meta: Mutex<FrameMeta>` plus `page: RwLock<PageBytes>`. Helpers now lock the specific primitive they need.
4. **Add `PageReadGuard` / `PageWriteGuard`:** wrap the `RwLock` guards + pin token; expose new `Transaction::pin_read/pin_write` APIs that return these guards (legacy `get_*` delegate internally during the transition).
5. **Refactor RecordPage/TableScan:** switch to guard-based access + Layout decoding; delete the old offset-based helpers.

This staging keeps the code compiling/testable at each step and makes it clear where unsafe code (if any) is isolated.

With this split, TableScan / executor nodes only interact with `RecordPage` and `LogicalRow` abstractions. They never calculate offsets or manage line pointers; Page keeps physical invariants, Layout keeps schema semantics, and RecordPage stitches them together.

---

## Design Details

### Compaction Flow

1. Scan line pointers for state=LIVE (or REDIRECT); gather slot ids and their offsets/lengths.
2. Sort by current offset to walk heap tuples in physical order.
3. Copy each live tuple to the top of the heap starting at `free_lower`; update that slot’s line pointer (offset/len/state).
4. After the last copy, set `free_upper` and `free_ptr` to the next free byte; rebuild freelist (`free_head`) from slots in FREE/DEAD state; zero remaining heap for deterministic images.
5. Recompute `crc32`; publish new free-space value to the FSM.

Slot ids (RIDs) remain stable; only their offsets change.

> **SlotId definition:** in code this is a thin type alias over the slot index (`type SlotId = u16` or `usize` depending on build). It’s the same value stored in `RID.slot`, i.e., the position in the line-pointer array.

### Page Operations

**Initialize new page:**
```rust
impl Page {
    fn init_heap_page(&mut self) {
        self.header = PageHeader::new(PageType::Heap);
        self.line_ptrs.clear();
        self.record_space.fill(0);
        // free_lower already equals PAGE_HEADER_SIZE
        // free_upper/free_ptr already point to end of page
        // free_head = NO_FREE_SLOT (freelist empty)
    }
}
```

**Allocate new slot:**
```rust
impl Page {
    fn allocate_tuple(&mut self, tuple_bytes: &[u8]) -> Result<SlotId, Error> {
        let len = tuple_bytes.len() as u16;
        let (lower, upper) = self.header.free_bounds();
        let needed = len;

        if lower + needed > upper {
            return Err("insufficient free space".into());
        }

        // Reuse a FREE slot if freelist populated, otherwise append.
        let slot = self
            .pop_free_slot()
            .unwrap_or_else(|| self.push_line_ptr(LinePtr::new(0, 0, LineState::Free)));

        // Carve space from the heap top (grows downward).
        let new_upper = upper - needed;
        self.record_space[new_upper as usize..(new_upper + needed) as usize]
            .copy_from_slice(tuple_bytes);

        self.line_ptrs[slot as usize] =
            LinePtr::new(new_upper, len, LineState::Live);

        self.header.set_free_bounds(lower, new_upper);
        self.header.set_free_ptr(new_upper as u32);
        self.header.set_slot_count(self.header.slot_count() + 1);

        Ok(slot)
    }
}
```

**Get record location:**
```rust
impl Page {
    fn tuple_bytes(&self, slot: SlotId) -> Option<&[u8]> {
        let lp = self.line_ptrs.get(slot as usize)?;
        if lp.state() != LineState::Live as u8 {
            return None;
        }
        let offset = lp.offset() as usize;
        let length = lp.length() as usize;
        Some(&self.record_space[offset..offset + length])
    }
}
```

**Delete record:**
```rust
impl Page {
    fn delete_slot(&mut self, slot: usize) -> Result<(), Error> {
        let lp = self.line_ptrs.get_mut(slot).ok_or("invalid slot")?;
        if lp.state() != LineState::Live as u8 {
            return Err("slot not live".into());
        }

        lp.mark_dead();                 // visible to iterators/GC
        self.push_free_slot(slot as u16); // add to freelist for reuse
        self.header
            .set_slot_count(self.header.slot_count().saturating_sub(1));
        // Heap bytes reclaimed only during compaction.

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

        if tx.block_size() == handle.page().header.free_ptr {
            handle.page().init_heap_page();
        }

        Self { tx, handle, layout }
    }

    fn insert_after(&self, slot: Option<usize>) -> Result<usize, Error> {
        let tuple_bytes = self.layout.build_tuple_buffer(/* fields */);
        let new_slot = self
            .handle
            .page()
            .allocate_tuple(&tuple_bytes)?;
        self.write_record_at(new_slot, tuple_bytes);

        Ok(new_slot)
    }

    fn is_slot_used(&self, slot: usize) -> bool {
        self.handle.page().line_ptr_state(slot) == Some(LineState::Live)
    }
}
```

### Benefits Over Current Design

```
FEATURE COMPARISON
═══════════════════════════════════════════════════════════════════

Operation              Current          Proposed
─────────────────────────────────────────────────────────────────
Check slot used        O(1) read flag   O(1) line ptr state check
Find free slot         O(n) scan slots  O(1) freelist (fallback scan)
Get record offset      O(1) calculate   O(1) line ptr lookup
Variable-length        Not possible     Supported via heap indirection
Space reclamation      Not possible     Possible with compaction
Direct slot access     Yes              Yes
Record metadata        None             Header tracks state

* Freelist threaded through FREE pointers; scan only when freelist empty.
```

**Key Advantages:**

1. **Structured metadata**: Page knows its own state
2. **Variable-length ready**: Line pointers + heap indirection support arbitrary tuple sizes
3. **Space efficiency**: Can compact and reclaim space
4. **Recovery-friendly**: Structured format easier to log/recover
5. **Pedagogically clear**: Explicitly shows record management concepts

### Encoding Page Variants with the Type System

Rather than handing every component a single `Page` struct plus a runtime `page_type` enum, treat the storage buffer as generic over a *page kind* that encodes its policies at compile time:

```rust
/// Shared storage: header + line pointers + heap bytes.
pub struct Page<K: PageKind> {
    header: PageHeader,
    line_ptrs: Vec<LinePtr>,
    record_space: Vec<u8>,
    kind: PhantomData<K>,
}

pub trait PageKind {
    const PAGE_TYPE: PageType;
    type Alloc<'a>: PageAllocator<'a>;
    type Iter<'a>: Iterator;

    fn allocator<'a>(page: &'a mut Page<Self>) -> Self::Alloc<'a>;
    fn iter<'a>(page: &'a Page<Self>) -> Self::Iter<'a>;
}

pub trait PageAllocator<'a> {
    type Output;
    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Error>;
}
```

**Heap pages** implement `PageKind` with freelist-driven allocation and a simple slot iterator:

```rust
pub struct HeapPage;

impl PageKind for HeapPage {
    const PAGE_TYPE: PageType = PageType::Heap;
    type Alloc<'a> = HeapAllocator<'a>;
    type Iter<'a> = HeapIter<'a>;

    fn allocator<'a>(page: &'a mut Page<Self>) -> Self::Alloc<'a> {
        HeapAllocator { page }
    }

    fn iter<'a>(page: &'a Page<Self>) -> Self::Iter<'a> {
        HeapIter { page, next: 0 }
    }
}

struct HeapAllocator<'a> {
    page: &'a mut Page<HeapPage>,
}

impl<'a> PageAllocator<'a> for HeapAllocator<'a> {
    type Output = SlotId;

    fn insert(&mut self, bytes: &[u8]) -> Result<SlotId, Error> {
        self.page.allocate_tuple(bytes)
    }
}
```

**B-tree leaf pages** use the same storage but expose an ordered allocator that keeps key slots sorted:

```rust
pub struct BTreeLeafPage;

impl PageKind for BTreeLeafPage {
    const PAGE_TYPE: PageType = PageType::IndexLeaf;
    type Alloc<'a> = BTreeLeafAllocator<'a>;
    type Iter<'a> = BTreeLeafIter<'a>;

    fn allocator<'a>(page: &'a mut Page<Self>) -> Self::Alloc<'a> {
        BTreeLeafAllocator { page }
    }

    fn iter<'a>(page: &'a Page<Self>) -> Self::Iter<'a> {
        BTreeLeafIter { page, current: 0 }
    }
}

struct BTreeLeafAllocator<'a> {
    page: &'a mut Page<BTreeLeafPage>,
}

impl<'a> PageAllocator<'a> for BTreeLeafAllocator<'a> {
    type Output = (SlotId, Option<SplitInfo>);

    fn insert(&mut self, entry: &[u8]) -> Result<Self::Output, Error> {
        // 1. binary-search existing line ptrs by key
        // 2. shift line ptrs to open space
        // 3. allocate tuple bytes + update LinePtr
        // 4. optionally return split metadata
        todo!()
    }
}
```

By splitting policies this way the compiler ensures heap-only APIs (freelist reuse) never run on B-tree pages, while B-tree-specific invariants (sorted keys, split metadata) remain encapsulated. Both variants still share the low-level line-pointer + heap layout, so WAL/CRC logic stays uniform.

---

## Implementation Strategy

### Phase 1: Core Page Redesign

**Goal:** Implement new Page structure without breaking existing code

**Tasks:**
- [ ] Define new Page struct with header, line-pointer array, and heap buffer
- [ ] Implement PageHeader with slot_count, free_ptr, and free_lower/free_upper helpers
- [ ] Implement LinePtr packing/unpacking helpers
  - [ ] `offset()/length()/state()`
  - [ ] mutation helpers + freelist threading
- [ ] Implement freelist management (`push_free_slot` / `pop_free_slot`)
- [ ] Implement tuple allocation/deallocation API described above
- [ ] Add Page initialization method
- [ ] Add unit tests for each component

**Complexity:** 2-3 days

**Acceptance Criteria:**
- Page struct compiles with new fields
- Line pointer packing/unpacking verified
- Freelist logic reuses slots correctly
- Tests verify all operations

---

### Phase 2: RecordPage Integration

**Goal:** Update RecordPage to use new Page structure

**Tasks:**
- [ ] Modify RecordPage to inspect line pointer states for slot checking
- [ ] Update slot allocation to call `allocate_tuple()`
- [ ] Use line pointer offsets for tuple lookup
- [ ] Update iteration to walk line pointers, skipping FREE/DEAD slots
- [ ] Ensure slot flag compatibility (transition period)
- [ ] Update all RecordPage tests

**Complexity:** 3-4 days

**Acceptance Criteria:**
- RecordPage works with new Page structure
- Existing RecordPage tests pass
- Slot allocation uses new mechanism
- Record access uses `LinePtr` metadata

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
- [ ] Update line pointer offsets after compaction
- [ ] Decide compaction trigger policy
- [ ] Add compaction tests
- [ ] Measure space savings

**Complexity:** 4-6 days

**Acceptance Criteria:**
- Compaction reclaims deleted space
- Line pointers correctly updated
- No data corruption during compaction
- Measurable space savings

---

## Trade-offs and Considerations

### Metadata Footprint

**Cost:** Only the 32-byte header is fixed; the line-pointer array grows (4 bytes per slot) and shrinks with workload. Free space is simply `free_upper - free_lower`.

**Implications:**
- Pages with many tiny tuples spend more bytes on line pointers, but still less than the old fixed bitmap + ID table tax.
- Pages with a few large tuples pay almost nothing beyond the header.
- Variable-length records are natural because heap bytes are contiguous and referenced indirectly.

### Complexity Increase

**Current:** Page is just `Vec<u8>` - extremely simple

**Proposed:** Page has 4 components with internal structure

**Justification:**
- Pedagogically valuable to show real database concepts
- Necessary foundation for advanced features
- Still simpler than production databases (e.g., PostgreSQL's page format)

### Slot Count Dynamics

- No baked-in cap: slots exist as long as `free_lower + 4 * slot_count < free_upper`.
- Maximum slots therefore depend on tuple size distribution; pathological case (28-byte tuples) still yields ≈1000 slots in 4 KB.
- Freelist ensures we can recycle slot IDs without reshaping the array.

---

## Related Issues

- **Supersedes:** Issue #7 (Store bitmap for presence checking) - addressed by maintaining explicit line-pointer states + freelist
- **Supersedes:** Issue #8 (Implement ID table for variable length strings) - addressed by heap indirection referenced through line pointers
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
