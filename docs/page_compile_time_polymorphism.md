# Page Compile-Time Polymorphism Refactor

## Problem Statement

Currently, page types (`HeapPageZeroCopy`, `BTreeLeafPageZeroCopy`, `BTreeInternalPageZeroCopy`, etc.) duplicate field definitions and common logic:

```rust
// Current duplication
struct HeapPageZeroCopy<'a> {
    header: HeapHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: HeapRecordSpace<'a>,
}

struct BTreeLeafPageZeroCopy<'a> {
    header: BTreeLeafHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,      // duplicated!
    record_space: BTreeRecordSpace<'a>,
}

struct BTreeInternalPageZeroCopy<'a> {
    header: BTreeInternalHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,      // duplicated!
    record_space: BTreeRecordSpace<'a>,
}
```

Common operations (slot allocation, free space management, compaction, line pointer manipulation) are implemented separately for each page type, leading to:
- Code duplication
- Higher maintenance burden
- Increased chance of bugs from divergent implementations

## Solution: Compile-Time Polymorphism with Zero-Sized Markers

Use zero-sized marker types to parameterize generic page structs. The compiler monomorphizes these at compile time, providing full type safety and specialization with zero runtime cost.

### Key Principle

**Same underlying structure, different marker type = different page type**

The fields are identical across page types:
- Header (but different concrete type)
- Line pointer array (same)
- Record space (same)

Only the marker changes, which determines the header type and type-specific behavior.

## Architecture Overview

### Three Generic Base Structs

All page functionality builds on three parameterized structs:

1. **`Page<'a, K: PageKind>`** - Read-only page view
2. **`PageMut<'a, K: PageKind>`** - Mutable page (pre-split)
3. **`PageParts<'a, K: PageKind>`** - Split guard with disjoint borrows (post-split)

### Component Hierarchy

```
Zero-Sized Markers (Heap, BTreeLeaf, BTreeInternal)
  ↓
PageKind Trait (associates headers, defines behavior hooks)
  ↓
Generic Structs (Page<K>, PageMut<K>, PageParts<K>)
  ↓
Type Aliases (HeapPageZeroCopy = Page<Heap>, etc.)
  ↓
Type-Specific Extensions (impl Page<Heap>, impl PageParts<BTreeLeaf>, etc.)
```

## Component Breakdown

### 1. Zero-Sized Markers

Markers are empty structs that exist only at compile time:

```rust
/// Marker for heap pages (table rows)
pub struct Heap;

/// Marker for B-tree leaf pages
pub struct BTreeLeaf;

/// Marker for B-tree internal pages
pub struct BTreeInternal;

/// Marker for meta pages
pub struct Meta;
```

These types:
- Have zero size (`std::mem::size_of::<Heap>() == 0`)
- Exist only during compilation
- Encode page-type-specific information via trait implementations

### 2. PageKind Trait

The existing `PageKind` trait is extended to associate concrete header types:

```rust
pub trait PageKind: Sized {
    /// Page type discriminator
    const PAGE_TYPE: PageType;

    /// Header size in bytes
    const HEADER_SIZE: usize;

    /// Read-only header type for this page kind
    type HeaderRef<'a>: HeaderReader<'a>;

    /// Mutable header type for this page kind
    type HeaderMut<'a>: HeaderHelpers;

    // Shared implementations (already exist)
    fn parse_layout(bytes: &[u8]) -> SimpleDBResult<ParsedLayout<'_>> { ... }
    fn calculate_split_offsets(free_lower: u16) -> SimpleDBResult<SplitOffsets> { ... }
    fn prepare_split<'a, H>(...) -> SimpleDBResult<SplitPreparation> { ... }

    // Behavior hooks for type-specific logic
    fn is_slot_live(lp: &LinePtr) -> bool;
    fn init_slot(ptrs: &mut LinePtrArrayMut, slot: SlotId, offset: u16, size: u16);
    fn delete_slot_impl<'a>(parts: &mut PageParts<'a, Self>, slot: SlotId) -> SimpleDBResult<()>;
}
```

### 3. Marker Implementations

Each marker implements `PageKind` with its specific types and behavior:

```rust
impl PageKind for Heap {
    const PAGE_TYPE: PageType = PageType::Heap;
    const HEADER_SIZE: usize = PAGE_HEADER_SIZE_BYTES as usize;

    type HeaderRef<'a> = HeapHeaderRef<'a>;
    type HeaderMut<'a> = HeapHeaderMut<'a>;

    fn is_slot_live(lp: &LinePtr) -> bool {
        lp.state() == LineState::Live
    }

    fn init_slot(ptrs: &mut LinePtrArrayMut, slot: SlotId, offset: u16, size: u16) {
        ptrs.set(slot, LinePtr::new(offset, size, LineState::Live));
    }

    fn delete_slot_impl<'a>(parts: &mut PageParts<'a, Self>, slot: SlotId) -> SimpleDBResult<()> {
        // Heap uses freelist
        let mut lp = parts.line_ptrs.as_ref().get(slot);
        lp.mark_free();
        parts.line_ptrs.set(slot, lp);
        parts.push_to_freelist(slot);
        Ok(())
    }
}

impl PageKind for BTreeLeaf {
    const PAGE_TYPE: PageType = PageType::IndexLeaf;
    const HEADER_SIZE: usize = PAGE_HEADER_SIZE_BYTES as usize;

    type HeaderRef<'a> = BTreeLeafHeaderRef<'a>;
    type HeaderMut<'a> = BTreeLeafHeaderMut<'a>;

    fn is_slot_live(lp: &LinePtr) -> bool {
        true  // BTree slots don't have dead state
    }

    fn init_slot(ptrs: &mut LinePtrArrayMut, slot: SlotId, offset: u16, size: u16) {
        ptrs.set(slot, LinePtr::new(offset, size, LineState::Live));
    }

    fn delete_slot_impl<'a>(parts: &mut PageParts<'a, Self>, slot: SlotId) -> SimpleDBResult<()> {
        // BTree physically removes slots
        parts.line_ptrs.delete(slot);
        parts.header.set_slot_count(parts.header.as_ref().slot_count() - 1);
        Ok(())
    }
}

impl PageKind for BTreeInternal {
    const PAGE_TYPE: PageType = PageType::IndexInternal;
    const HEADER_SIZE: usize = PAGE_HEADER_SIZE_BYTES as usize;

    type HeaderRef<'a> = BTreeInternalHeaderRef<'a>;
    type HeaderMut<'a> = BTreeInternalHeaderMut<'a>;

    fn is_slot_live(lp: &LinePtr) -> bool {
        true
    }

    fn init_slot(ptrs: &mut LinePtrArrayMut, slot: SlotId, offset: u16, size: u16) {
        ptrs.set(slot, LinePtr::new(offset, size, LineState::Live));
    }

    fn delete_slot_impl<'a>(parts: &mut PageParts<'a, Self>, slot: SlotId) -> SimpleDBResult<()> {
        parts.line_ptrs.delete(slot);
        parts.header.set_slot_count(parts.header.as_ref().slot_count() - 1);
        Ok(())
    }
}
```

### 4. Generic Page Structs

#### Read-Only Page

```rust
/// Read-only zero-copy page view
pub struct Page<'a, K: PageKind> {
    header: K::HeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: RecordSpace<'a>,
    _marker: PhantomData<K>,
}

impl<'a, K: PageKind> Page<'a, K> {
    /// Parse and validate page from bytes
    pub fn new(bytes: &'a [u8]) -> SimpleDBResult<Self> {
        let layout = K::parse_layout(bytes)?;

        let header = K::HeaderRef::new(layout.header);

        // Validate free_upper bounds
        let free_upper = header.free_upper() as usize;
        let page_size = PAGE_SIZE_BYTES as usize;
        if free_upper < header.free_lower() as usize || free_upper > page_size {
            return Err("page free_upper out of bounds".into());
        }

        Ok(Self::from_parts(
            header,
            layout.line_ptrs,
            layout.records,
            layout.base_offset,
        ))
    }

    fn from_parts(
        header: K::HeaderRef<'a>,
        line_ptr_bytes: &'a [u8],
        record_space_bytes: &'a [u8],
        base_offset: usize,
    ) -> Self {
        Self {
            header,
            line_pointers: LinePtrArray::with_len(line_ptr_bytes, header.slot_count() as usize),
            record_space: RecordSpace::new(record_space_bytes, base_offset),
            _marker: PhantomData,
        }
    }

    /// Shared read operations
    pub fn slot_count(&self) -> usize {
        self.header.slot_count() as usize
    }

    pub fn free_space(&self) -> u16 {
        self.header.free_upper().saturating_sub(self.header.free_lower())
    }

    pub fn line_ptr(&self, slot: SlotId) -> Option<LinePtr> {
        if slot >= self.line_pointers.len() {
            None
        } else {
            Some(self.line_pointers.get(slot))
        }
    }
}
```

#### Mutable Page (Pre-Split)

```rust
/// Mutable zero-copy page (before splitting into parts)
pub struct PageMut<'a, K: PageKind> {
    header: K::HeaderMut<'a>,
    body_bytes: &'a mut [u8],
    _marker: PhantomData<K>,
}

impl<'a, K: PageKind> PageMut<'a, K> {
    pub fn new(bytes: &'a mut [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, body_bytes) = bytes.split_at_mut(K::HEADER_SIZE);
        let header = K::HeaderMut::new(header_bytes);

        if header.as_ref().page_type() != K::PAGE_TYPE {
            return Err("wrong page type".into());
        }

        Ok(Self {
            header,
            body_bytes,
            _marker: PhantomData,
        })
    }

    pub fn update_crc32(&mut self) {
        self.header.update_crc32(self.body_bytes);
    }

    pub fn verify_crc32(&mut self) -> bool {
        self.header.verify_crc32(self.body_bytes)
    }

    /// Split into disjoint borrows for mutation
    pub fn split(&mut self) -> SimpleDBResult<PageParts<'_, K>> {
        let prep = K::prepare_split(&self.header.as_ref(), self.body_bytes.len(), "page")?;

        let (line_ptr_bytes, record_space_bytes) = self.body_bytes.split_at_mut(prep.lp_capacity);

        Ok(PageParts {
            header: K::HeaderMut::new(self.header.bytes_mut()),
            line_ptrs: LinePtrArrayMut::with_len(line_ptr_bytes, prep.slot_count),
            record_space: RecordSpaceMut::new(record_space_bytes, prep.base_offset),
            _marker: PhantomData,
        })
    }
}
```

#### Page Parts (Post-Split Guard)

```rust
/// Guard holding disjoint mutable views over page components
pub struct PageParts<'a, K: PageKind> {
    header: K::HeaderMut<'a>,
    line_ptrs: LinePtrArrayMut<'a>,
    record_space: RecordSpaceMut<'a>,
    _marker: PhantomData<K>,
}

impl<'a, K: PageKind> PageParts<'a, K> {
    /// Shared slot allocation logic
    pub fn allocate_slot(&mut self, size: u16) -> SimpleDBResult<SlotId> {
        let (lower, upper) = {
            let hdr = self.header.as_ref();
            (hdr.free_lower(), hdr.free_upper())
        };

        if upper - lower < size + LinePtrBytes::LINE_PTR_BYTES as u16 {
            return Err("insufficient space for slot".into());
        }

        let new_upper = upper - size;
        let new_lower = lower + LinePtrBytes::LINE_PTR_BYTES as u16;

        self.header.set_free_upper(new_upper);
        self.header.set_free_lower(new_lower);

        let slot = self.header.as_ref().slot_count();
        self.header.set_slot_count(slot + 1);

        // Call marker-specific initialization
        K::init_slot(&mut self.line_ptrs, slot as usize, new_upper, size);

        Ok(slot as usize)
    }

    /// Shared deletion logic (delegates to marker)
    pub fn delete_slot(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        if slot >= self.header.as_ref().slot_count() as usize {
            return Err("slot out of bounds".into());
        }

        // Delegate to marker-specific implementation
        K::delete_slot_impl(self, slot)
    }

    /// Shared compaction logic
    pub fn compact(&mut self) -> SimpleDBResult<()> {
        let mut write_offset = PAGE_SIZE_BYTES;

        for slot in 0..self.header.as_ref().slot_count() as usize {
            let lp = self.line_ptrs.as_ref().get(slot);

            // Marker determines what's "live"
            if K::is_slot_live(&lp) {
                let (offset, len) = lp.offset_and_length();
                write_offset -= len as u16;
                self.record_space.copy_within(offset, write_offset as usize, len);

                let mut new_lp = lp;
                new_lp.set_offset(write_offset);
                self.line_ptrs.set(slot, new_lp);
            }
        }

        self.header.set_free_upper(write_offset);
        Ok(())
    }
}
```

### 5. Shared Helper Structs (Not Parameterized)

These don't need markers since they're truly generic:

```rust
/// Record space for extracting byte ranges
pub struct RecordSpace<'a> {
    bytes: &'a [u8],
    base_offset: usize,
}

impl<'a> RecordSpace<'a> {
    pub fn new(bytes: &'a [u8], base_offset: usize) -> Self {
        Self { bytes, base_offset }
    }

    pub fn get_bytes(&self, ptr: LinePtr) -> Option<&'a [u8]> {
        let offset = ptr.offset() as usize;
        let length = ptr.length() as usize;
        let relative = offset.checked_sub(self.base_offset)?;
        self.bytes.get(relative..relative + length)
    }
}

pub struct RecordSpaceMut<'a> {
    bytes: &'a mut [u8],
    base_offset: usize,
}

impl<'a> RecordSpaceMut<'a> {
    pub fn new(bytes: &'a mut [u8], base_offset: usize) -> Self {
        Self { bytes, base_offset }
    }

    pub fn write_entry(&mut self, offset: usize, bytes: &[u8]) {
        let relative = offset.checked_sub(self.base_offset).expect("offset precedes record space");
        let end = relative + bytes.len();
        self.bytes[relative..end].copy_from_slice(bytes);
    }

    pub fn copy_within(&mut self, src_offset: usize, dst_offset: usize, len: usize) {
        if len == 0 { return; }
        let src_rel = src_offset.checked_sub(self.base_offset).expect("source precedes record space");
        let dst_rel = dst_offset.checked_sub(self.base_offset).expect("destination precedes record space");
        self.bytes.copy_within(src_rel..src_rel + len, dst_rel);
    }
}
```

### 6. Type Aliases (Backward Compatibility)

Replace existing concrete types with aliases:

```rust
// Read-only pages
pub type HeapPageZeroCopy<'a> = Page<'a, Heap>;
pub type BTreeLeafPageZeroCopy<'a> = Page<'a, BTreeLeaf>;
pub type BTreeInternalPageZeroCopy<'a> = Page<'a, BTreeInternal>;

// Mutable pages
pub type HeapPageZeroCopyMut<'a> = PageMut<'a, Heap>;
pub type BTreeLeafPageZeroCopyMut<'a> = PageMut<'a, BTreeLeaf>;
pub type BTreeInternalPageZeroCopyMut<'a> = PageMut<'a, BTreeInternal>;

// Page parts
pub type HeapPageParts<'a> = PageParts<'a, Heap>;
pub type BTreeLeafPageParts<'a> = PageParts<'a, BTreeLeaf>;
pub type BTreeInternalPageParts<'a> = PageParts<'a, BTreeInternal>;
```

### 7. Type-Specific Extensions

Add page-type-specific methods via specialized impl blocks:

```rust
// Heap-specific read operations
impl<'a> Page<'a, Heap> {
    pub fn tuple_bytes(&self, slot: SlotId) -> Option<&'a [u8]> {
        let lp = self.line_ptr(slot)?;
        if !lp.is_live() {
            return None;
        }
        self.record_space.get_bytes(lp)
    }

    pub fn tuple_ref(&self, slot: SlotId) -> Option<TupleRef<'a>> {
        let lp = self.line_ptr(slot)?;
        match lp.state() {
            LineState::Free => Some(TupleRef::Free),
            LineState::Live => Some(TupleRef::Live(HeapTuple::from_bytes(self.tuple_bytes(slot)?))),
            LineState::Dead => Some(TupleRef::Dead),
            LineState::Redirect => Some(TupleRef::Redirect(lp.offset() as usize)),
        }
    }
}

// BTree leaf-specific read operations
impl<'a> Page<'a, BTreeLeaf> {
    pub fn entry_bytes(&self, slot: SlotId) -> Option<&'a [u8]> {
        let lp = self.line_ptr(slot)?;
        self.record_space.get_bytes(lp)
    }

    pub fn find_slot(&self, key: &[u8]) -> Result<usize, usize> {
        // Binary search for key
        (0..self.slot_count()).binary_search_by(|&i| {
            let entry = self.entry_bytes(i).unwrap();
            // Compare keys
            todo!()
        })
    }
}

// Heap-specific mutation operations
impl<'a> PageParts<'a, Heap> {
    pub fn insert_tuple_fast(&mut self, bytes: &[u8]) -> SimpleDBResult<HeapInsert> {
        // Try freelist fast path
        if let Some(slot) = self.pop_free_slot() {
            let needed = bytes.len() as u16;
            let (lower, upper) = {
                let hdr = self.header.as_ref();
                (hdr.free_lower(), hdr.free_upper())
            };

            if upper - lower < needed {
                return Err("insufficient free space".into());
            }

            let new_upper = upper - needed;
            self.record_space.write_entry(new_upper as usize, bytes);
            self.line_ptrs.set(slot, LinePtr::new(new_upper, needed, LineState::Live));
            self.header.set_free_upper(new_upper);

            return Ok(HeapInsert::Done(slot));
        }

        // Fall back to allocate_slot
        let slot = self.allocate_slot(bytes.len() as u16)?;
        Ok(HeapInsert::Reserved(ReservedSlot { slot_idx: slot }))
    }

    pub fn redirect_slot(&mut self, slot: SlotId, target: SlotId) -> SimpleDBResult<()> {
        let mut lp = self.line_ptrs.as_ref().get(slot);
        if !lp.is_live() {
            return Err("slot is not live".into());
        }
        lp.mark_redirect(target as u16);
        self.line_ptrs.set(slot, lp);
        Ok(())
    }

    fn pop_free_slot(&mut self) -> Option<SlotId> {
        if !self.header.as_ref().has_free_slot() {
            return None;
        }
        let free_idx = self.header.as_ref().free_head() as usize;
        let lp = self.line_ptrs.as_ref().get(free_idx);
        self.header.set_free_head(lp.offset());
        Some(free_idx)
    }

    fn push_to_freelist(&mut self, slot: SlotId) {
        let mut lp = self.line_ptrs.as_ref().get(slot);
        let next = self.header.as_ref().free_head();
        lp.set_offset(next);
        lp.set_length(0);
        self.line_ptrs.set(slot, lp);
        self.header.set_free_head(slot as u16);
    }
}

// BTree leaf-specific mutation operations
impl<'a> PageParts<'a, BTreeLeaf> {
    pub fn insert_ordered(&mut self, key: &[u8], value: &[u8]) -> SimpleDBResult<SlotId> {
        // Binary search to find insertion point
        let insert_pos = self.find_insert_position(key)?;

        let entry_size = (key.len() + value.len()) as u16;
        let slot = self.allocate_slot(entry_size)?;

        // Shift slots to maintain order
        if insert_pos < slot {
            self.line_ptrs.shift_right(insert_pos, slot);
        }

        // Write entry
        let offset = self.header.as_ref().free_upper();
        self.record_space.write_entry(offset as usize, &[key, value].concat());

        Ok(insert_pos)
    }

    fn find_insert_position(&self, key: &[u8]) -> SimpleDBResult<usize> {
        // Binary search implementation
        todo!()
    }
}
```

## Usage Examples

### Creating Pages

```rust
// From buffer pool guard
let guard = buffer_pool.pin(block_id)?;

// Read-only heap page
let page = HeapPageZeroCopy::new(guard.bytes())?;
let tuple = page.tuple_ref(0)?;

// Mutable heap page
let mut page = HeapPageZeroCopyMut::new(guard.bytes_mut())?;
let mut parts = page.split()?;
parts.insert_tuple_fast(&tuple_bytes)?;
drop(parts);
page.update_crc32();

// BTree leaf page
let page = BTreeLeafPageZeroCopy::new(bytes)?;
let entry = page.entry_bytes(slot)?;
```

### Method Resolution

```rust
// Generic method (available for all page types)
let count = page.slot_count();  // Calls impl<K: PageKind> Page<K>

// Type-specific method (only for Heap)
let heap_page = HeapPageZeroCopy::new(bytes)?;
let tuple = heap_page.tuple_ref(0)?;  // Calls impl Page<Heap>

// Won't compile for BTree
let btree_page = BTreeLeafPageZeroCopy::new(bytes)?;
// btree_page.tuple_ref(0)?;  // ERROR: method not found
```

## Migration Path

### Step 1: Define Markers and Extend PageKind

```rust
pub struct Heap;
pub struct BTreeLeaf;
pub struct BTreeInternal;

// Extend PageKind with associated types
pub trait PageKind {
    type HeaderRef<'a>: HeaderReader<'a>;
    type HeaderMut<'a>: HeaderHelpers;
    // ... existing methods ...
    fn is_slot_live(lp: &LinePtr) -> bool;
}

// Implement for each marker
impl PageKind for Heap { ... }
impl PageKind for BTreeLeaf { ... }
impl PageKind for BTreeInternal { ... }
```

### Step 2: Create Generic Structs

```rust
pub struct Page<'a, K: PageKind> { ... }
pub struct PageMut<'a, K: PageKind> { ... }
pub struct PageParts<'a, K: PageKind> { ... }

// Implement shared logic
impl<'a, K: PageKind> Page<'a, K> { ... }
impl<'a, K: PageKind> PageMut<'a, K> { ... }
impl<'a, K: PageKind> PageParts<'a, K> { ... }
```

### Step 3: Add Type Aliases

```rust
pub type HeapPageZeroCopy<'a> = Page<'a, Heap>;
pub type HeapPageZeroCopyMut<'a> = PageMut<'a, Heap>;
pub type HeapPageParts<'a> = PageParts<'a, Heap>;
// ... etc
```

### Step 4: Add Type-Specific Extensions

```rust
impl<'a> Page<'a, Heap> { ... }
impl<'a> PageParts<'a, Heap> { ... }
impl<'a> Page<'a, BTreeLeaf> { ... }
impl<'a> PageParts<'a, BTreeLeaf> { ... }
```

### Step 5: Remove Old Concrete Types

Delete:
- `HeapPageZeroCopy` struct definition
- `BTreeLeafPageZeroCopy` struct definition
- `BTreeInternalPageZeroCopy` struct definition
- etc.

Keep:
- Concrete header types (`HeapHeaderRef`, `BTreeLeafHeaderRef`, etc.)
- Type aliases

### Step 6: Update Call Sites

Most code should compile unchanged due to type aliases. Only update code that:
- Matches on page types structurally
- Uses fully qualified paths

## Benefits

### 1. Zero Code Duplication

Field definitions and common operations written once, specialized via markers.

### 2. Type Safety

Compiler ensures:
- Heap pages can't call BTree methods
- Each page type has the correct header type
- Type-specific operations only available for correct types

### 3. Zero Runtime Cost

Markers are zero-sized and eliminated at compile time. Generated code is identical to hand-written specializations.

### 4. Maintainability

Common logic changes in one place. New page types add marker + impl blocks, reusing all shared infrastructure.

### 5. Extensibility

New page types require:
- Define marker struct
- Implement `PageKind`
- Add type-specific methods

All generic infrastructure (allocation, compaction, validation) works immediately.

### 6. Backward Compatible

Type aliases preserve existing API. Migration is incremental.

## Scope and Limitations: Non-Slotted Pages

### The Assumption

The generic `Page<K>` abstraction assumes **all pages use slotted layout**:

```rust
struct Page<'a, K: PageKind> {
    header: K::HeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,  // ← Assumes slotted layout
    record_space: RecordSpace<'a>,     // ← Assumes slotted layout
    _marker: PhantomData<K>,
}
```

This works for:
- **Heap pages** - table rows with MVCC
- **BTree leaf pages** - index entries
- **BTree internal pages** - separator keys + child pointers
- **Overflow pages** (if they use slots for large tuple chunks)

### Pages That Don't Fit

Some page types have **fundamentally different structure**:

#### Meta Pages

```rust
// Current implementation (lines 1449-1482 in page.rs)
pub struct BTreeMetaPageView<'a> {
    header: BTreeMetaHeaderRef<'a>,
    // NO line pointers
    // NO record space
    // Just header fields: version, tree_height, root_block, first_free_block
}
```

Meta pages store tree metadata. There are no slots or records - just fixed header fields.

#### WAL Pages

From module docs: "Write-ahead log pages with boundary-pointer format"

WAL pages use a different layout (boundary pointers, not slots) for log records.

#### Free Pages

May be just zeros, a bitmap, or a simple free space map. No slotted structure.

### Why the Abstraction Breaks

If you try to force meta pages into the generic abstraction:

```rust
type MetaPage<'a> = Page<'a, Meta>;

// This expands to:
Page<'a, Meta> {
    header: BTreeMetaHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,  // ??? Meta has no slots!
    record_space: RecordSpace<'a>,     // ??? Meta has no records!
    _marker: PhantomData<Meta>,
}
```

**Problem**: Meta pages don't have line pointers or record space. The abstraction assumes structure that doesn't exist.

### Solution: Scope the Abstraction to Slotted Pages

**Recommendation**: Rename to be explicit about scope and keep non-slotted pages concrete.

```rust
// Rename to clarify scope
pub trait SlottedPageKind: Sized {
    type HeaderRef<'a>: HeaderReader<'a>;
    type HeaderMut<'a>: HeaderHelpers;
    const PAGE_TYPE: PageType;
    // ... slotted-specific behavior
}

// Generic abstraction ONLY for slotted pages
pub struct SlottedPage<'a, K: SlottedPageKind> {
    header: K::HeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: RecordSpace<'a>,
    _marker: PhantomData<K>,
}

pub struct SlottedPageMut<'a, K: SlottedPageKind> { ... }
pub struct SlottedPageParts<'a, K: SlottedPageKind> { ... }

// Markers implement SlottedPageKind
impl SlottedPageKind for Heap { ... }
impl SlottedPageKind for BTreeLeaf { ... }
impl SlottedPageKind for BTreeInternal { ... }

// Type aliases for slotted pages
pub type HeapPageZeroCopy<'a> = SlottedPage<'a, Heap>;
pub type BTreeLeafPageZeroCopy<'a> = SlottedPage<'a, BTreeLeaf>;
pub type BTreeInternalPageZeroCopy<'a> = SlottedPage<'a, BTreeInternal>;

// Non-slotted pages stay CONCRETE
pub struct MetaPage<'a> {
    header: BTreeMetaHeaderRef<'a>,
}

pub struct WalPage<'a> {
    header: WalHeaderRef<'a>,
    boundary_ptr: u32,
    records: &'a [u8],
}

pub struct FreePage<'a> {
    header: FreeHeaderRef<'a>,
    bitmap: &'a [u8],
}
```

### Alternative: Optional Fields (Not Recommended)

You could make fields optional:

```rust
struct Page<'a, K: PageKind> {
    header: K::HeaderRef<'a>,
    line_pointers: Option<LinePtrArray<'a>>,  // None for meta
    record_space: Option<RecordSpace<'a>>,     // None for meta
}
```

**Why this is bad:**
- Runtime overhead (`Option` checks)
- Uglifies common case (heap/btree must unwrap)
- Generic methods need to handle None cases
- Defeats zero-cost abstraction goal

### Alternative: Multiple Generic Hierarchies

```rust
// Slotted page hierarchy
struct SlottedPage<'a, K: SlottedPageKind> { ... }

// Simple header-only hierarchy
struct SimplePage<'a, K: SimplePageKind> {
    header: K::HeaderRef<'a>,
    body: &'a [u8],
}

// Log page hierarchy
struct LogPage<'a, K: LogPageKind> {
    header: K::HeaderRef<'a>,
    boundary_ptr: u32,
    records: &'a [u8],
}
```

**Tradeoff:**
- **Pro**: Each category gets appropriate abstraction
- **Con**: Multiple parallel abstractions add complexity

### Recommendation

**Use slotted abstraction for slotted pages, concrete types for others:**

```rust
// Generic abstraction (3+ types share structure)
SlottedPage<Heap>
SlottedPage<BTreeLeaf>
SlottedPage<BTreeInternal>
SlottedPage<Overflow>  // if overflow uses slots

// Concrete types (fundamentally different structure)
MetaPage
WalPage
FreePage
```

**Why this works:**

1. **Slotted pages genuinely share structure** - heap/btree/internal all have line pointers + record space
2. **Meta/WAL/Free are fundamentally different** - trying to unify them is forcing abstraction
3. **3+ slotted types justify generics** - significant duplication reduction
4. **Non-slotted pages are simpler** - just header, no complex slot management

### When to Use Each Approach

**Use generic SlottedPage when:**
- Page uses line pointer array + record space
- Shares allocation/compaction/slot management logic
- 3+ page types follow this pattern

**Use concrete types when:**
- Page has unique structure (meta, WAL, free)
- No line pointers or record space
- Only 1-2 pages of this type

**Don't force unification** - it's OK to have both slotted generics and concrete types coexisting.

### Updated Architecture

```
Slotted Pages (Generic):
  SlottedPage<Heap>
  SlottedPage<BTreeLeaf>
  SlottedPage<BTreeInternal>
  SlottedPage<Overflow>

Non-Slotted Pages (Concrete):
  MetaPage
  WalPage
  FreePage

Both coexist - use the right abstraction for each category.
```

The slotted abstraction is still highly valuable - it eliminates duplication for 3+ page types that genuinely share structure. Just don't try to force meta/WAL/free pages into it.

## Performance Notes

The compiler monomorphizes each `Page<K>` instantiation:
- `Page<Heap>` generates separate code from `Page<BTreeLeaf>`
- No vtables, no dynamic dispatch
- All method calls are statically dispatched
- Inlining and optimization work as if hand-written

Assembly output for `Page::<Heap>::slot_count()` is identical to the current `HeapPageZeroCopy::slot_count()`.

## Testing Strategy

1. **Unit tests for generic implementations**: Test `Page<Heap>`, `Page<BTreeLeaf>` separately
2. **Integration tests**: Ensure type aliases work correctly
3. **Compile tests**: Verify type-specific methods aren't available for wrong types
4. **Regression tests**: Existing tests should pass with type aliases

## Summary

This refactor:
- Eliminates field duplication across page types
- Centralizes common logic in generic implementations
- Preserves type safety and specialization via markers
- Has zero runtime cost (pure compile-time polymorphism)
- Maintains backward compatibility via type aliases
- Simplifies maintenance and extension

The key insight: **page types differ in behavior, not structure**. Parameterizing by zero-sized markers captures behavioral differences at compile time while sharing structural code.
