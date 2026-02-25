# Variable-Length Heap Tuple Implementation Plan

## Goal
Replace fixed-size `slot_size` tuple allocation with dense packing: only actual bytes per value are stored, NULLs consume zero payload bytes, and field offsets are computed dynamically at read time.

---

## Phase 1 — Layout struct refactor [x]
Remove the precomputed static-offset machinery from `Layout` and fix every consumer that depended on it (BTree encode/decode, planner cost estimates).

**Changes:**
- Remove `offsets: HashMap<String, usize>` and `slot_size: usize` from `Layout`
- Remove `Layout::offset()`, `offset_with_index()`, `field_length()`
- Add `Layout::max_encoded_size() -> usize` — worst-case payload bytes (INT=4, STRING=4+declared_max); used by BTree capacity checks and planner `records_per_block`
- Fix `BTreeLeafEntry::encode/decode` — 3-field schema, compute field positions directly without `layout.offset()`
- Fix `BTreeInternalEntry::encode/decode` — same
- Fix `BTreeLeafPage::is_full` (`page.rs:6089`) and internal page check (`page.rs:6636`) to use `max_encoded_size()`
- Fix planner `records_per_block` uses (`main.rs:206, 2932`) to use `max_encoded_size()`

**State at end of phase:** BTree tests pass. Heap tests broken (nothing writes or reads tuples correctly yet).

---

## Phase 2 — Dense encode/decode at tuple layer [x]
Implement the dense payload format end-to-end at the `HeapTuple`/`LogicalRow` level.

**Changes:**
- Add `Layout::encode_payload(values: &[Constant]) -> Vec<u8>`:
  - Byte 0..ceil(n/8): null bitmap (bit i set → field i is NULL)
  - For each non-NULL field in schema order: INT → 4 LE bytes; STRING → 4-byte LE length + actual UTF-8 bytes
  - NULL fields write nothing
- Add `Layout::decode_field(payload: &[u8], field_name: &str) -> Option<Constant>`:
  - Read null bitmap, return `None` if null
  - Scan fields 0..field_idx: skip NULLs (0 bytes), advance by 4 for INT, advance by 4+actual_len for STRING
  - Decode target field at computed offset
- Rewrite `LogicalRow::get_column` to call `Layout::decode_field`
- Add `HeapPageViewMut::insert_row_values(values: &[Constant]) -> SimpleDBResult<SlotId>`:
  - Calls `layout.encode_payload(values)` to get dense bytes
  - Builds tuple: `[HEAP_TUPLE_HEADER | nullmap_ptr=0 | encoded_payload]`
  - Inserts via existing `insert_tuple()`, sets WAL record
  - Returns `SlotId`
- Keep `insert_row_mut` alive temporarily (delete in Phase 3) — it's still called by `RecordPage::insert()`

**State at end of phase:** heap insert+read round-trips correctly in dense format. Unit tests for encode/decode added.

---

## Phase 3 — Insert API wiring (Scan trait + call sites) [x]
Thread the dense insert path from `HeapPageViewMut` up through `RecordPage`, `TableScan`, and the `Scan` trait. Update all call sites.

**Changes:**
- `RecordPage::insert_with_values(values: &[Constant])` — promote from `#[cfg(test)]` to public; calls `insert_row_values`
- Add `fn insert_values(&mut self, values: &[Constant]) -> Result<(), Box<dyn Error>>` to the `Scan` trait
- `TableScan::insert_values` delegates to `RecordPage::insert_with_values`; propagates block-full logic (same as existing `insert()`)
- Implement stub `insert_values` on all other `Scan` implementors (`SelectScan`, `ProjectScan`, `ProductScan`, `UnionScan`, etc.) — most should delegate or return an error
- Update all ~20 call sites from `scan.insert()` + `scan.set_int/set_string` → `scan.insert_values(&[...])`
- Remove `Scan::insert()` from trait and all implementors
- Remove `RecordPage::insert()`
- Remove `HeapPageViewMut::insert_row_mut` (now dead)

**State at end of phase:** all insert-path tests pass. `set_int`/`set_string` still work (only used by update path now).

---

## Phase 4 — Update path [x]
Fix `set_int`/`set_string` to work correctly with dense-encoded tuples.

**Changes:**
- `RecordPage::set_int(slot, field, value)` and `set_string` → new implementation:
  1. Read all current field values from the slot via `LogicalRow` (dynamic decode)
  2. Apply the change to the in-memory vec
  3. Re-encode densely via `layout.encode_payload()`
  4. Call `HeapPageViewMut::update_tuple(slot, &new_bytes)` which handles size growth via redirect
- `LogicalRowMut::set_column` can be removed or reduced (only path was `RecordPage::set_*`)
- Verify WAL before/after images are captured correctly by `update_tuple`

**State at end of phase:** all tests pass (insert + update + read all working in dense format).

---

## Phase 5 — Cleanup, tests, docs [x]
Remove dead code, add the acceptance-criteria tests, update documentation.

**Changes:**
- Remove `HeapPageView::column_page_offset` and `HeapPageViewMut::column_page_offset` — static offsets are gone, WAL doesn't need these
- Remove `LogicalRowMut` if fully dead, or strip to bare minimum
- Add unit test: insert rows with mixed-length strings, assert free space after is larger than with `max_encoded_size()` allocation
- Add unit test: NULL fields consume zero payload bytes
- Update `docs/record_management.md` — mark "True Variable-Length Field Implementation (TODO)" section as implemented, add final format spec

**State at end of phase:** PR ready. All tests pass. Acceptance criteria met.

---

## Phase 6 — Fix scan double-processing of redirected tuples [x]

**Problem:** `update_tuple` redirects a grown tuple by appending a new live slot at the end of the page's slot array. A forward scan (`execute_modify`) walking slots sequentially will encounter that new slot later in the same pass and apply the update expression a second time to the same logical row. Wrong results for expressions like `SET name = name || 'x'`; wrong update count for all growing updates.

**Root cause:** `RecordPage::next_valid_slot` recreates `HeapIterator` from slot 0 on every `scan.next()` call. The iterator is not persistent — `current_slot` is not maintained between calls, causing O(n²) scan and re-evaluation of `page.slot_count()` each time (which now includes redirect-appended slots).

**Fix:** Give `TableScan` a persistent `HeapIterator` per page. `HeapIterator` is restructured to:
- Hold a `BufferHandle` (keeps the page pinned in the buffer pool for the duration of the page scan)
- Hold `current_slot` and `max_slot` (captured once at block-entry time from the page's slot count)
- On each `next()` call: acquire `frame.read_page()` directly (bypassing `pin_read_guard` overhead — no repeated slock, pin, or buffer lookup), read the line pointer at `current_slot`, release the read guard, advance

`max_slot` is captured when `TableScan` moves to a new block. Redirect-appended slots always land at indices >= `max_slot` and are never visited. `RecordPage::next_valid_slot` is deleted.

**Why not hold `PageReadGuard` in `HeapIterator`:** The write path (`set_string` etc.) acquires a write lock on the same page between `next()` calls. Holding the read guard across iterations would deadlock.

**Why not `HeapIterator<'a>` with borrowed bytes:** `PageReadGuard<'a>` has `'a` tied to the borrow of `Arc<Transaction>`. `TableScan` owns `Arc<Transaction>`, so storing a guard that borrows from it would be self-referential. Propagating `'a` to `TableScan<'a>` would require `TableScan` to borrow rather than own `Transaction` — a broader architectural change.
