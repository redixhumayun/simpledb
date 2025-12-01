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

## Proposed B-Tree Headers

### Leaf Header (example layout)
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

### Internal Header
Same structure except:
- `rightmost_child_block: u32` replaces `overflow_block`.
- Slots store `(key, child_block)` pairs; the rightmost child pointer lives outside the slot array like in PostgreSQL’s “downlink”.

### Serialization Helpers
- `struct BTreeLeafHeader` and `BTreeInternalHeader` implement `to_bytes` / `from_bytes`.
- `Page<BTreeLeafPage>::write_bytes` writes `BTreeLeafHeader` first, then the slot array, then fills the remaining bytes with zeros for deterministic images.
- `Page<BTreeLeafPage>::from_bytes` validates `page_type`, deserializes header, rebuilds slot metadata, and errors if invariants fail (e.g., slot_count * slot_size exceeds page size).

## Implementation Plan
1. **Header structs & serialization**: Introduce `BTreeLeafHeader` / `BTreeInternalHeader`, move existing reserved-byte hacks into explicit fields.
2. **Typed views update**: `BTreeLeafPageView` reads the new header, exposes helpers for `high_key`, `right_sibling`, etc.
3. **Record layout rewrite**: Replace heap-style `LinePtr` usage with a B-tree-specific pointer directory that keeps entries densely sorted and compacts payload bytes eagerly (required for variable-length keys).
4. **Staged rollout**:
   - *Phase A*: Keep the existing `PageHeader`/`LinePtr` structure but add B-tree header “views” that reinterpret those bytes (no storage changes yet). Update all B-tree code to use the typed views instead of raw `reserved` fields.
   - *Phase B*: Move B-tree serialization/deserialization into per-kind helper structs while `PageBytes` still stores the shared fields; the views own all access.
   - *Phase C*: Redefine `Page<RawPage>` as a raw byte buffer and let each `PageKind` persist only the data it needs (line pointers for heap, pointer directory for B-tree, etc.). Because all callers already go through the typed views, this becomes a mechanical change.
5. **Migration path**:
   - Write adapters that can load legacy heap-formatted B-Tree pages if encountered (for backward compatibility) or require full reformat via rebuild.
   - Update `BTreePage::format` to write the new headers.
6. **Direct I/O validation**: Ensure `write_bytes` still emits exactly `PAGE_SIZE_BYTES`, adjust CRC/Lsn placement accordingly, and run torn-write tests.

## Open Questions
- Backwards compatibility strategy for existing on-disk B-Tree files (one-time reindex vs. dual-format reader).
- Whether WAL records should contain full-page images during the transition.
- How to represent prefix-compressed keys or variable-length index entries within the pointer directory.
