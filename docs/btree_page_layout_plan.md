# B-Tree Page Layout Refactor

## Summary
- Tracking Issue: [#61](https://github.com/redixhumayun/simpledb/issues/61)
- Heap and index pages currently share the same physical header (slot freelist, CRC seed, latch word, etc.).
- B-Tree logic still expects fixed-width slots plus metadata such as rightmost-child pointers and overflow links, which are being forced into `reserved` bytes.
- This document proposes splitting the on-disk header/slot layout per `PageKind`, so heap pages keep MVCC-specific machinery while B-Tree leaf/internal pages get lean headers that surface high-key/right-sibling data directly.

## Motivation
- **Impedance mismatch**: Index pages do not use MVCC tuple headers, redirects, or freelists, yet they pay for those bytes and semantics.
- **Missing metadata**: Rightmost child pointers, sibling links, and overflow chains have no explicit home in the current header, so invariants live in ad‑hoc fields.
- **Direct I/O readiness**: We still need deterministic 4 KB/8 KB page images for O_DIRECT; typed headers preserve that while keeping formats purpose-built.
- **Safety**: Typed views can reject miscast pages at runtime (e.g., `PageView<'_, BTreeLeafPage>::new` can verify the header signature before exposing methods).

## Design Principles
1. `PageBytes = Page<RawPage>` remains the canonical buffer payload.
2. Each `PageKind` owns:
   - A header struct with `write_to_bytes`/`read_from_bytes`.
   - Slot/tuple interpretation logic that fits that header.
3. `PageView<'_, K>` and `PageViewMut<'_, K>`:
   - Check the `page_type` byte.
   - Deserialize the `K` header into a typed struct.
   - Provide APIs around that typed representation (heap tuple iterator, B-Tree insert, etc.).
4. Serialization pipeline:
   - Guard mutates `Page<K>`.
   - `Page<K>::write_bytes` lays out the `K` header + payload into a contiguous buffer.
   - Buffer manager flushes the raw bytes; direct I/O simply writes the same buffer.

## Proposed Headers

### Heap Header
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::Heap` | discriminant checked by views |
| 1 | `reserved_flags: u8` | feature bits / future use |
| 2 | `slot_count: u16` | number of line pointers |
| 4 | `free_lower: u16` | end of slot directory (grows downward) |
| 6 | `free_upper: u16` | start of free heap space (grows upward) |
| 8 | `free_ptr: u32` | bump pointer for tuple allocation |
| 12 | `crc32: u32` | torn-write protection |
| 16 | `latch_word: u64` | seqlock/debug instrumentation |
| 24 | `free_head: u16` | freelist head (`0xFFFF` sentinel) |
| 26 | `reserved[6]` | MVCC/VACUUM metadata (future use) |

Following the fixed header, heap pages store a line-pointer array (4 bytes per slot) and a tuple heap that grows upward. MVCC metadata (xmin/xmax/nullmap) lives inside each tuple body; the header tracks slot reuse and free space only.

### B-Tree Leaf Header (example layout)
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::IndexLeaf` | matches dispatch |
| 1 | `level: u8` | leaf level (0) for sanity |
| 2 | `slot_count: u16` | number of entries |
| 4 | `high_key_len: u16` | length of optional high-key payload |
| 6 | `right_sibling_block: u32` | block number of next leaf / overflow |
| 10 | `overflow_block: u32` | optional overflow chain head |
| 14 | `crc32: u32` | for torn-write protection |
| 18 | `lsn: u64` | last-modified LSN |
| 26 | `reserved[6]` | future use / padding |

Line-pointer directory: we retain a pointer array (offset/length/state) so entries can be variable-length in the future. Unlike heap pages, B-tree delete semantics will immediately remove the pointer and compact payload bytes, keeping the directory dense and sorted for binary search. There is no long-lived FREE state or deferred garbage collection.

### B-Tree Internal Header
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::IndexInternal` | internal node discriminant |
| 1 | `level: u8` | tree level (> 0) |
| 2 | `slot_count: u16` | number of separator keys |
| 4 | `rightmost_child_block: u32` | downlink for keys greater than all entries |
| 8 | `high_key_len: u16` | optional high key payload for concurrent splits |
| 10 | `reserved[2]` | alignment / future use |
| 12 | `crc32: u32` | torn-write protection |
| 16 | `lsn: u64` | last-modified LSN |
| 24 | `reserved2[8]` | padding / future metadata |

Internal slots store `(key, child_block)` payloads; the header’s `rightmost_child_block` holds the extra downlink instead of embedding it in a dummy slot. Like leaves, internal pages keep a dense pointer directory with no freelist/redirect state.

### B-Tree Overflow Header
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::Overflow` | discriminant |
| 1 | `reserved_flags: u8` | future use |
| 2 | `slot_count: u16` | number of overflow entries |
| 4 | `key_len: u16` | length of duplicated key payload (0 if entries store full keys) |
| 6 | `next_overflow_block: u32` | next page in chain (`0xFFFF_FFFF` sentinel = none) |
| 10 | `crc32: u32` | torn-write protection |
| 14 | `lsn: u64` | last-modified LSN |
| 22 | `reserved[10]` | padding / future metadata |

Overflow pages optionally store the duplicated key bytes immediately after the header (length = `key_len`), followed by a dense pointer directory referencing the RID payloads. `slot_count` drives the directory size; there is no freelist because overflow entries are compacted after every insert/delete.

### B-Tree Meta Header
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::Meta` | identified as index metadata |
| 1 | `version: u8` | on-disk format version |
| 2 | `tree_height: u16` | current height (root level) |
| 4 | `root_block: u32` | block ID of current root page |
| 8 | `first_free_block: u32` | head of free-page list (`0xFFFF_FFFF` = none) |
| 12 | `reserved[8]` | optional stats (tuple/page counts, FSM pointer) |
| 20 | `crc32: u32` | torn-write protection |
| 24 | `lsn: u64` | last-modified LSN |

Each B-tree file reserves block 0 for this meta page. Structural changes (root splits, free list updates) update this header so reopen operations know where the root lives and which blocks can be recycled.

### Free Page Header
| Offset | Field | Notes |
| --- | --- | --- |
| 0 | `page_type = PageType::Free` | marks page as reusable |
| 1 | `reserved_flags: u8` | future use |
| 2 | `reserved: u16` | padding / alignment |
| 4 | `next_free_block: u32` | next page on the free list (`0xFFFF_FFFF` if none) |
| 8 | `crc32: u32` | optional torn-write protection |
| 12 | `lsn: u64` | last time the free list changed |
| 20 | `reserved2[12]` | padding |

Free pages carry no slot directory or payload; when a page is returned to the free list we zero the body and write this minimal header so the allocator can chain through the free list quickly.

### Serialization Helpers
- `struct BTreeLeafHeader` and `BTreeInternalHeader` implement `to_bytes` / `from_bytes`.
- `Page<BTreeLeafPage>::write_bytes` writes `BTreeLeafHeader` first, then the slot array, then fills the remaining bytes with zeros for deterministic images.
- `Page<BTreeLeafPage>::from_bytes` validates `page_type`, deserializes header, rebuilds slot metadata, and errors if invariants fail (e.g., slot_count * slot_size exceeds page size).

## Implementation Plan
1. **Header structs & serialization**: Introduce `BTreeLeafHeader` / `BTreeInternalHeader`, move existing reserved-byte hacks into explicit fields.
2. **Typed views update**: `BTreeLeafPageView` reads the new header, exposes helpers for `high_key`, `right_sibling`, etc.
3. **Record layout rewrite**: Replace heap-style `LinePtr` usage with a B-tree-specific pointer directory that keeps entries densely sorted and compacts payload bytes eagerly (required for variable-length keys).
4. **Single index file**:
   - Add a proper index catalog entry that records `{index_name, base table, file_name, root_block}`.
   - Allocate one storage file per index (`{index}.idx`) and format block 0 as the meta page; leaf/internal/overflow pages coexist in that file.
   - Update `BTreeIndex` to drop `{index}leaf` / `{index}internal` tables and always reference the shared file when reading/writing pages.
5. **Staged rollout**:
   - *Phase A*: Keep the existing `PageHeader`/`LinePtr` structure but add B-tree header “views” that reinterpret those bytes (no storage changes yet). Update all B-tree code to use the typed views instead of raw `reserved` fields.
   - *Phase B*: Move B-tree serialization/deserialization into per-kind helper structs while `PageBytes` still stores the shared fields; the views own all access.
   - *Phase C*: Redefine `Page<RawPage>` as a raw byte buffer and let each `PageKind` persist only the data it needs (line pointers for heap, pointer directory for B-tree, etc.). Because all callers already go through the typed views, this becomes a mechanical change.
6. **Direct I/O validation**: Ensure `write_bytes` still emits exactly `PAGE_SIZE_BYTES`, adjust CRC/Lsn placement accordingly, and run torn-write tests.

## Open Questions
- Backwards compatibility strategy for existing on-disk B-Tree files (one-time reindex vs. dual-format reader).
- Whether WAL records should contain full-page images during the transition.
- How to represent prefix-compressed keys or variable-length index entries within the pointer directory.

## Concurrency Considerations
- The current API acquires transactional locks and buffer latches together via `txn.pin_*`. To support Lehman–Yao style concurrency, we must split these concerns: logical locks (tuple/key-range) should be managed by the concurrency manager and held until commit, while buffer latches are short-lived `RwLock` guards released immediately after reading/writing the page bytes.
- The lock table today operates at page granularity, so even if latches were short-lived, logical X-locks would still serialize access on entire blocks. Moving to tuple/key-range locks is required before separation pays off.
- `BTreeLeaf` / `BTreeInternal` helpers currently grab their own guards per call, making it impossible to latch-couple parent/child or follow high-key/right-link chains. A concurrent design needs explicit descent APIs that return path context (parent latch, child latch, key range) so callers can release and reacquire latches in the Lehman–Yao pattern.
- We also need to add per-page metadata (high keys, right-links) so readers can detect concurrent splits and follow siblings instead of waiting for writers to propagate changes to the parent.

## Implementation Sketch

```rust
pub struct PageBytes {
    data: [u8; PAGE_SIZE_BYTES as usize],
}

impl PageBytes {
    pub fn page_type(&self) -> PageType {
        PageType::from(self.data[0])
    }
}

pub trait PageKind {
    const PAGE_TYPE: PageType;
    type Header: HeaderCodec;

    fn decode_header(bytes: &[u8]) -> Result<Self::Header, Box<dyn Error>>;
    fn encode_header(header: &Self::Header, dst: &mut [u8]);
}

pub struct HeapHeader {
    pub slot_count: u16,
    pub free_lower: u16,
    pub free_upper: u16,
    pub free_head: u16,
    pub crc32: u32,
    pub lsn: u64,
}

impl HeaderCodec for HeapHeader {
    fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            slot_count: u16::from_le_bytes(bytes[2..4].try_into()?),
            free_lower: u16::from_le_bytes(bytes[4..6].try_into()?),
            free_upper: u16::from_le_bytes(bytes[6..8].try_into()?),
            free_head: u16::from_le_bytes(bytes[24..26].try_into()?),
            crc32: u32::from_le_bytes(bytes[12..16].try_into()?),
            lsn: u64::from_le_bytes(bytes[16..24].try_into()?),
        })
    }

    fn write_bytes(&self, dst: &mut [u8]) {
        dst[0] = PageType::Heap as u8;
        dst[2..4].copy_from_slice(&self.slot_count.to_le_bytes());
        dst[4..6].copy_from_slice(&self.free_lower.to_le_bytes());
        dst[6..8].copy_from_slice(&self.free_upper.to_le_bytes());
        dst[24..26].copy_from_slice(&self.free_head.to_le_bytes());
        dst[12..16].copy_from_slice(&self.crc32.to_le_bytes());
        dst[16..24].copy_from_slice(&self.lsn.to_le_bytes());
    }
}

impl<'a> HeapPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>) -> Result<Self, Box<dyn Error>> {
        let bytes = guard.bytes();
        if PageBytes::page_type(bytes) != PageType::Heap {
            return Err("not a heap page".into());
        }
        let header = HeapHeader::from_bytes(bytes)?;
        let (_, rest) = bytes.split_at(HEAP_HEADER_LEN);
        let (slot_dir, heap_area) =
            rest.split_at(header.slot_count as usize * LINE_PTR_SIZE);

        Ok(Self {
            guard,
            header,
            slot_dir: LinePtrArray::from_bytes(slot_dir),
            heap_area,
        })
    }
}
```

B-Tree leaf/internal headers follow the same pattern: each defines a struct that knows how to decode/encode the first bytes of `PageBytes`, and the corresponding view reinterprets the remaining bytes as a pointer directory + payload with no `unsafe` casts.

### Migration Steps (breaking changes acceptable)
1. Replace `Page<RawPage>` in `BufferFrame` with `PageBytes` guarded by `RwLock`. Update `PageReadGuard` / `PageWriteGuard` to expose `[u8]` views instead of `Page<RawPage>`.
2. Introduce header structs (`HeapHeader`, `BTreeLeafHeader`, etc.) with `HeaderCodec` implementations and plug them into `PageKind`.
3. Refactor typed views (`HeapPageView{,Mut}`, `BTreeLeafPageView{,Mut}`, etc.) to validate `page_type`, decode the typed header, and build their slot directories from raw bytes.
4. Update formatting helpers (`format_as_heap`, `format_as_btree_leaf`, …) to write the new headers via the codec and zero the rest of the page.
5. Ensure `PageViewMut::write_bytes` (and any WAL/checkpoint serialization code) uses the header codec so flushed bytes align with the new format.
6. Switch B-tree storage to a single file per index (as outlined in Implementation Plan) so all page kinds share the same namespace.

### Scope / Impact
- **Buffer layer**: `BufferFrame`, guard structs, and serialization helpers change to operate on `PageBytes`. Mechanical but touches core components.
- **Typed views**: internal logic rewrites to use per-kind headers; public APIs (`row`, `insert_entry`, etc.) remain stable, so higher layers (RecordPage, TableScan, executor) stay untouched.
- **Formatting / catalog**: page-formatting functions and index creation code change to write/expect the new headers; removing `{index}leaf`/`{index}internal` tables simplifies the metadata layer.
- **Higher layers**: Record management, planner/executor nodes, and tests continue using the same view APIs, so no application-level rewrite is required. Once headers are in place, future work (lock/latch separation, concurrency, prefix compression) can build on top without reshaping the public interfaces.

## Next Steps: Textbook Internal Separator Layout (agreed design)

Invariants (left-closed, right-open): with separator keys `K0..Kk-1` and children `C0..Ck`:
- `C0` holds keys < `K0`
- For `1 <= i < k`: `Ci` holds keys >= `K{i-1}` and < `Ki`
- `Ck` holds keys >= `K{k-1}`

On-page mapping:
- Internal entry payload stays `(key, child_left)`. Entry `i` stores `Ki` and `child_left = Ci`.
- Header stores `rightmost_child_block = Ck`.
- Implicit children array: `[entry[0].child, entry[1].child, ..., entry[k-1].child, header.rightmost]`.
- No dummy min-key entry; empty node has `slot_count = 0` and `rightmost_child_block` set to its only child.

Split contract:
- Child split returns `SplitResult { sep_key, left_block, right_block }`.
- Parent inserts `sep_key` where it belongs; the separator’s *left* child is `left_block`; the child immediately to the right of the separator becomes `right_block`.

Insert rewiring inside an internal page:
- API: `insert_separator(key, right_child)` on `BTreeInternalPageZeroCopyMut` rewires children:
  - Insert separator key at position `i` (binary search).
  - Let `right_child` be the new child for the range >= key (from split sibling).
  - If inserting at end: set header.rightmost = `right_child`.
  - Else: swap `right_child` with the child currently at position `i` (the child to the right of the previous key), then shift subsequent entry.child values right by one; header.rightmost shifts if we insert before the end.
- Search becomes: first key > target -> descend to that entry.child; else header.rightmost.

Root/init:
- `format_as_btree_internal(level, rightmost_child)` sets header.rightmost and leaves `slot_count = 0`; no sentinel entry.

Planned code changes
- `src/page.rs`
  - Expose `rightmost_child_block` getters/setters through `BTreeInternalPageView{,Mut}`.
  - Update `format_as_btree_internal(level, rightmost_child)` signature and call sites.
  - Add `insert_separator(key, right_child)` (and helper to read/write child at position i and header rightmost) to maintain the implicit children array.
- `src/btree.rs`
  - Drop dummy min-entry on root creation; pass initial child via formatter.
  - Update `find_child_block` to textbook search (no sentinel).
  - Change leaf/internal split results to `SplitResult { sep_key, left_block, right_block }`; parent insert uses `insert_separator`.
  - Update tests to reflect zero-slot root, rightmost-child header, and search > last key.

## High Key + Right Sibling Plan (to be implemented)

- Header changes
  - Add `high_key_len: u16` and `high_key_off: u16` to leaf/internal headers (reuse reserved bytes; header size may grow).
  - Keep `right_sibling_block` (`u32::MAX` sentinel = none).

- Storage of high key
  - High key = exclusive upper bound for the page.
  - Store bytes at `high_key_off`, length `high_key_len`.
  - When setting a new high key: compact payload first (so free space is contiguous), compute `off = free_upper - len`, write bytes there, set `high_key_off/len`, then set `free_upper = off`.
  - Rightmost page: `high_key_len = 0`, `high_key_off = 0` (means +∞).

- Split wiring
  - `sep` = first key of right sibling after split.
  - Left page: `high_key = sep`, `right_sibling_block = new_sibling`.
  - Right page: `high_key = previous upper bound` (or +∞ if rightmost), `right_sibling_block = old right link`.
  - Parent already uses `sep` as separator; children array unchanged.

- Search/iteration usage
  - Add view helpers to decode high key; during search, if `high_key_len > 0` and `search_key >= high_key`, follow `right_sibling_block`.
  - Leaf iterator/range scan: on end of page, follow `right_sibling_block` when present.

- Tests needed
  - Split tests: left high key = separator, left right_sibling set, right high key = +∞ when rightmost.
  - Serialization round-trip of `high_key_len/off` and right_sibling.
  - Search test where `search_key == high_key` hops right and finds the key.
  - Iterator test that traverses across sibling link.

