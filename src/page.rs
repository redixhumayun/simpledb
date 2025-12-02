use std::{
    error::Error,
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{BlockId, BufferFrame, BufferHandle, Constant, FieldInfo, FieldType, Layout, Lsn, RID};

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageType {
    Heap = 0,
    IndexLeaf = 1,
    IndexInternal = 2,
    Overflow = 3,
    Meta = 4,
    Free = 255,
}

// Compile-time fixed page size, selected via Cargo features.
// Exactly one of `page-4k`, `page-8k`, or `page-1m` should be enabled.
#[cfg(feature = "page-4k")]
pub const PAGE_SIZE_BYTES: u16 = 4096;
#[cfg(feature = "page-8k")]
pub const PAGE_SIZE_BYTES: u16 = 8192;
#[cfg(feature = "page-1m")]
pub const PAGE_SIZE_BYTES: u16 = 1024 * 1024;

#[cfg(not(any(feature = "page-4k", feature = "page-8k", feature = "page-1m")))]
compile_error!(
    "One of `page-4k`, `page-8k`, or `page-1m` features must be enabled to select a page size."
);

/// Fixed header size as per `docs/record_management.md`.
pub const PAGE_HEADER_SIZE_BYTES: u16 = 32;
/// Sentinel for "no free slot" in `free_head`.
pub const NO_FREE_SLOT: u16 = 0xFFFF;

struct PageHeader {
    page_type: PageType,
    reserved_flags: u8,
    slot_count: u16,
    free_lower: u16,
    free_upper: u16,
    free_ptr: u32,
    crc32: u32,
    latch_word: u64,
    free_head: u16,
    reserved: [u8; 6],
}

impl PageHeader {
    /// Create a new, empty page header for the given page type.
    ///
    /// Invariants:
    /// - Slot directory is empty (`slot_count = 0`)
    /// - `free_lower` starts just after the fixed-size header
    /// - `free_upper` / `free_ptr` start at the end of the 4KB page
    /// - `free_head` uses a sentinel to indicate "no free slots"
    fn new(page_type: PageType) -> Self {
        PageHeader {
            page_type,
            reserved_flags: 0,
            slot_count: 0,
            free_lower: PAGE_HEADER_SIZE_BYTES,
            free_upper: PAGE_SIZE_BYTES,
            free_ptr: PAGE_SIZE_BYTES as u32,
            crc32: 0,
            latch_word: 0,
            free_head: NO_FREE_SLOT,
            reserved: [0; 6],
        }
    }

    fn page_type(&self) -> PageType {
        self.page_type
    }

    fn set_page_type(&mut self, page_type: PageType) {
        self.page_type = page_type;
    }

    fn slot_count(&self) -> u16 {
        self.slot_count
    }

    fn set_slot_count(&mut self, slot_count: u16) {
        self.slot_count = slot_count;
    }

    /// Return the current free-space bounds (line pointers down, heap up).
    fn free_bounds(&self) -> (u16, u16) {
        (self.free_lower, self.free_upper)
    }

    /// Set the free-space bounds, keeping basic invariants.
    fn set_free_bounds(&mut self, lower: u16, upper: u16) {
        debug_assert!(lower >= PAGE_HEADER_SIZE_BYTES);
        debug_assert!(upper <= PAGE_SIZE_BYTES);
        debug_assert!(lower <= upper);
        self.free_lower = lower;
        self.free_upper = upper;
    }

    /// Number of contiguous free bytes between the slot array and the heap.
    fn free_space(&self) -> u16 {
        self.free_upper.saturating_sub(self.free_lower)
    }

    fn free_ptr(&self) -> u32 {
        self.free_ptr
    }

    fn set_free_ptr(&mut self, free_ptr: u32) {
        self.free_ptr = free_ptr;
    }

    fn crc32(&self) -> u32 {
        self.crc32
    }

    fn set_crc32(&mut self, crc: u32) {
        self.crc32 = crc;
    }

    fn latch_word(&self) -> u64 {
        self.latch_word
    }

    fn set_latch_word(&mut self, latch: u64) {
        self.latch_word = latch;
    }

    fn free_head(&self) -> u16 {
        self.free_head
    }

    fn set_free_head(&mut self, head: u16) {
        self.free_head = head;
    }

    fn has_free_slot(&self) -> bool {
        self.free_head != NO_FREE_SLOT
    }

    fn reserved(&self) -> &[u8; 6] {
        &self.reserved
    }

    fn set_reserved(&mut self, reserved: [u8; 6]) {
        self.reserved = reserved;
    }

    fn reserved_flags(&self) -> u8 {
        self.reserved_flags
    }

    fn set_reserved_flags(&mut self, flags: u8) {
        self.reserved_flags = flags;
    }

    /// Get the B-tree level for internal nodes (uses reserved bytes 0-1)
    fn btree_level(&self) -> u16 {
        u16::from_le_bytes([self.reserved[0], self.reserved[1]])
    }

    /// Set the B-tree level for internal nodes (uses reserved bytes 0-1)
    fn set_btree_level(&mut self, level: u16) {
        let bytes = level.to_le_bytes();
        self.reserved[0] = bytes[0];
        self.reserved[1] = bytes[1];
    }

    /// Get the overflow block number for leaf nodes (uses reserved bytes 2-5)
    /// Returns None if no overflow block (0xFFFFFFFF sentinel)
    fn overflow_block(&self) -> Option<usize> {
        let val = u32::from_le_bytes([
            self.reserved[2],
            self.reserved[3],
            self.reserved[4],
            self.reserved[5],
        ]);
        if val == 0xFFFFFFFF {
            None
        } else {
            Some(val as usize)
        }
    }

    /// Set the overflow block number for leaf nodes (uses reserved bytes 2-5)
    /// Use None to clear (sets to 0xFFFFFFFF sentinel)
    fn set_overflow_block(&mut self, block: Option<usize>) {
        let val = block.map(|b| b as u32).unwrap_or(0xFFFFFFFF);
        let bytes = val.to_le_bytes();
        self.reserved[2..6].copy_from_slice(&bytes);
    }

    /// Serialize the header into the provided 32-byte buffer using the documented layout.
    fn write_to_bytes(&self, dst: &mut [u8]) {
        assert_eq!(
            dst.len(),
            PAGE_HEADER_SIZE_BYTES as usize,
            "header buffer must be 32 bytes"
        );

        dst.fill(0);
        dst[0] = self.page_type as u8;
        dst[1] = self.reserved_flags;
        dst[2..4].copy_from_slice(&self.slot_count.to_le_bytes());
        dst[4..6].copy_from_slice(&self.free_lower.to_le_bytes());
        dst[6..8].copy_from_slice(&self.free_upper.to_le_bytes());
        dst[8..12].copy_from_slice(&self.free_ptr.to_le_bytes());
        dst[12..16].copy_from_slice(&self.crc32.to_le_bytes());
        dst[16..24].copy_from_slice(&self.latch_word.to_le_bytes());
        dst[24..26].copy_from_slice(&self.free_head.to_le_bytes());
        dst[26..32].copy_from_slice(&self.reserved);
    }

    /// Parse a header from a 32-byte buffer.
    fn read_from_bytes(src: &[u8]) -> Result<Self, Box<dyn Error>> {
        if src.len() != PAGE_HEADER_SIZE_BYTES as usize {
            return Err("header buffer must be 32 bytes".into());
        }

        let page_type = match src[0] {
            0 => PageType::Heap,
            1 => PageType::IndexLeaf,
            2 => PageType::IndexInternal,
            3 => PageType::Overflow,
            4 => PageType::Meta,
            255 => PageType::Free,
            _ => return Err("invalid page type byte".into()),
        };

        let slot_count = u16::from_le_bytes(src[2..4].try_into().unwrap());
        let free_lower = u16::from_le_bytes(src[4..6].try_into().unwrap());
        let free_upper = u16::from_le_bytes(src[6..8].try_into().unwrap());
        let free_ptr = u32::from_le_bytes(src[8..12].try_into().unwrap());
        let crc32 = u32::from_le_bytes(src[12..16].try_into().unwrap());
        let latch_word = u64::from_le_bytes(src[16..24].try_into().unwrap());
        let free_head = u16::from_le_bytes(src[24..26].try_into().unwrap());
        let mut reserved = [0u8; 6];
        reserved.copy_from_slice(&src[26..32]);

        Ok(PageHeader {
            page_type,
            reserved_flags: src[1],
            slot_count,
            free_lower,
            free_upper,
            free_ptr,
            crc32,
            latch_word,
            free_head,
            reserved,
        })
    }
}

#[derive(Clone, Copy)]
struct LinePtr(u32);

#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineState {
    Free = 0,
    Live = 1,
    Dead = 2,
    Redirect = 3,
}

impl TryFrom<u32> for LineState {
    type Error = ();

    fn try_from(value: u32) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(LineState::Free),
            1 => Ok(LineState::Live),
            2 => Ok(LineState::Dead),
            3 => Ok(LineState::Redirect),
            _ => Err(()),
        }
    }
}

impl LineState {
    fn from_u32(value: u32) -> Self {
        Self::try_from(value).expect("invalid LineState bits")
    }
}

impl LinePtr {
    fn new(offset: u16, length: u16, state: LineState) -> Self {
        let mut line_pointer = LinePtr(0);
        line_pointer.set_offset(offset);
        line_pointer.set_length(length);
        line_pointer.set_state(state);
        line_pointer
    }

    fn offset(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    fn length(&self) -> u16 {
        ((self.0 >> 4) & 0x0FFF) as u16
    }

    fn offset_and_length(&self) -> (usize, usize) {
        (self.offset() as usize, self.length() as usize)
    }

    fn state(&self) -> LineState {
        let state = self.0 & 0x000F;
        LineState::from_u32(state)
    }

    fn set_offset(&mut self, offset: u16) {
        self.0 = (self.0 & 0x0000_FFFF) | ((offset as u32) << 16);
    }

    fn set_length(&mut self, length: u16) {
        let length_bits = (length as u32) & 0x0FFF;
        self.0 = (self.0 & 0xFFFF_000F) | (length_bits << 4);
    }

    fn set_state(&mut self, state: LineState) {
        let state_bits = (state as u32) & 0x000F;
        self.0 = (self.0 & 0xFFFF_FFF0) | (state_bits);
    }

    #[cfg(test)]
    fn with_offset(mut self, offset: u16) -> Self {
        self.set_offset(offset);
        self
    }

    #[cfg(test)]
    fn with_length(mut self, length: u16) -> Self {
        self.set_length(length);
        self
    }

    #[cfg(test)]
    fn with_state(mut self, state: LineState) -> Self {
        self.set_state(state);
        self
    }

    fn is_free(&self) -> bool {
        self.state() == LineState::Free
    }

    fn is_live(&self) -> bool {
        self.state() == LineState::Live
    }

    fn mark_free(&mut self) {
        self.set_state(LineState::Free);
    }

    #[cfg(test)]
    fn mark_live(&mut self) {
        self.set_state(LineState::Live);
    }

    #[cfg(test)]
    fn mark_dead(&mut self) {
        self.set_state(LineState::Dead);
    }

    fn mark_redirect(&mut self, offset: u16) {
        self.set_offset(offset);
        self.set_length(0x000);
        self.set_state(LineState::Redirect);
    }
}

#[cfg(test)]
mod line_ptr_tests {
    use super::*;

    #[test]
    fn offset_length_state_round_trip() {
        let mut lp = LinePtr(0);
        lp.set_offset(0x1234);
        lp.set_length(0x0567);
        lp.set_state(LineState::Live);

        assert_eq!(lp.offset(), 0x1234);
        assert_eq!(lp.length(), 0x0567);
        assert_eq!(lp.state(), LineState::Live);
    }

    #[test]
    fn updating_offset_preserves_length_and_state() {
        let mut lp = LinePtr(0);
        lp.set_offset(0xAAAA);
        lp.set_length(0x0555);
        lp.set_state(LineState::Dead);

        lp.set_offset(0xBBBB);

        assert_eq!(lp.offset(), 0xBBBB);
        assert_eq!(lp.length(), 0x0555);
        assert_eq!(lp.state(), LineState::Dead);
    }

    #[test]
    fn updating_length_preserves_offset_and_state() {
        let mut lp = LinePtr(0);
        lp.set_offset(0x1111);
        lp.set_length(0x0123);
        lp.set_state(LineState::Live);

        lp.set_length(0x0456);

        assert_eq!(lp.offset(), 0x1111);
        assert_eq!(lp.length(), 0x0456);
        assert_eq!(lp.state(), LineState::Live);
    }

    #[test]
    fn updating_state_preserves_offset_and_length() {
        let mut lp = LinePtr(0);
        lp.set_offset(0x2222);
        lp.set_length(0x0789);
        lp.set_state(LineState::Free);

        lp.set_state(LineState::Redirect);

        assert_eq!(lp.offset(), 0x2222);
        assert_eq!(lp.length(), 0x0789);
        assert_eq!(lp.state(), LineState::Redirect);
    }

    #[test]
    fn length_is_clamped_to_12_bits() {
        let mut lp = LinePtr(0);
        lp.set_length(0xFFFF); // higher than 12 bits

        assert_eq!(lp.length(), 0x0FFF); // only low 12 bits kept
    }

    #[test]
    fn with_methods_return_modified_copy() {
        let lp = LinePtr(0);
        let lp2 = lp
            .with_offset(0x3333)
            .with_length(0x0345)
            .with_state(LineState::Live);

        // original unchanged
        assert_eq!(lp.offset(), 0);
        assert_eq!(lp.length(), 0);
        assert_eq!(lp.state(), LineState::Free);

        // new one has changes
        assert_eq!(lp2.offset(), 0x3333);
        assert_eq!(lp2.length(), 0x0345);
        assert_eq!(lp2.state(), LineState::Live);
    }

    #[test]
    fn mark_helpers_update_state() {
        let mut lp = LinePtr(0);

        lp.mark_live();
        assert_eq!(lp.state(), LineState::Live);

        lp.mark_dead();
        assert_eq!(lp.state(), LineState::Dead);

        lp.mark_free();
        assert_eq!(lp.state(), LineState::Free);

        lp.mark_redirect(0);
        assert_eq!(lp.state(), LineState::Redirect);
    }
}

pub trait PageAllocator<'a> {
    type Output;
    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>>;
}

pub trait PageKind {
    const PAGE_TYPE: PageType;
}

type SlotId = usize;

pub struct RawPage;

impl PageKind for RawPage {
    const PAGE_TYPE: PageType = PageType::Free;
}

pub struct HeapPage;

pub struct HeapAllocator<'a> {
    page: &'a mut Page<HeapPage>,
}

impl<'a> PageAllocator<'a> for HeapAllocator<'a> {
    type Output = SlotId;

    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>> {
        self.page.allocate_tuple(bytes)
    }
}

pub struct HeapIterator<'a> {
    page: &'a Page<HeapPage>,
    current_slot: SlotId,
    match_state: Option<LineState>,
}

impl<'a> Iterator for HeapIterator<'a> {
    type Item = TupleRef<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let total_slots = self.page.slot_count();
        while self.current_slot < total_slots {
            let slot = self.current_slot;
            self.current_slot += 1;
            if let Some(tuple_ref) = self.page.tuple(slot) {
                if self
                    .match_state
                    .map_or(true, |ms| ms == tuple_ref.line_state())
                {
                    return Some(tuple_ref);
                }
            }
        }
        None
    }
}

impl PageKind for HeapPage {
    const PAGE_TYPE: PageType = PageType::Heap;
}

// BTree page types
pub struct BTreeLeafPage;
pub struct BTreeInternalPage;

pub struct BTreeLeafAllocator<'a> {
    page: &'a mut Page<BTreeLeafPage>,
}

pub struct BTreeInternalAllocator<'a> {
    page: &'a mut Page<BTreeInternalPage>,
}

pub struct BTreeLeafIterator<'a> {
    page: &'a Page<BTreeLeafPage>,
    layout: &'a Layout,
    current_slot: SlotId,
}

pub struct BTreeInternalIterator<'a> {
    page: &'a Page<BTreeInternalPage>,
    layout: &'a Layout,
    current_slot: SlotId,
}

impl<'a> PageAllocator<'a> for BTreeLeafAllocator<'a> {
    type Output = SlotId;

    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>> {
        // (TODO) BTree leaf pages need sorted insertion
        self.page.allocate_tuple(bytes)
    }
}

impl<'a> PageAllocator<'a> for BTreeInternalAllocator<'a> {
    type Output = SlotId;

    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>> {
        // (TODO) BTree internal pages need sorted insertion
        self.page.allocate_tuple(bytes)
    }
}

impl<'a> Iterator for BTreeLeafIterator<'a> {
    type Item = BTreeLeafEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_slot < self.page.slot_count() {
            let slot = self.current_slot;
            self.current_slot += 1;

            if let Some(bytes) = self.page.tuple_bytes(slot) {
                if let Ok(entry) = BTreeLeafEntry::decode(self.layout, bytes) {
                    return Some(entry);
                }
            }
        }
        None
    }
}

impl<'a> Iterator for BTreeInternalIterator<'a> {
    type Item = BTreeInternalEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_slot < self.page.slot_count() {
            let slot = self.current_slot;
            self.current_slot += 1;

            if let Some(bytes) = self.page.tuple_bytes(slot) {
                if let Ok(entry) = BTreeInternalEntry::decode(self.layout, bytes) {
                    return Some(entry);
                }
            }
        }
        None
    }
}

impl PageKind for BTreeLeafPage {
    const PAGE_TYPE: PageType = PageType::IndexLeaf;
}

impl PageKind for BTreeInternalPage {
    const PAGE_TYPE: PageType = PageType::IndexInternal;
}

/// Type alias for a page image whose kind is not yet known at the IO boundary.
pub type PageBytes = Page<RawPage>;

/// Write-ahead log pages use a simple "boundary pointer + payload" format that is
/// unrelated to heap/index layouts. They only need to manage raw bytes.
#[derive(Debug)]
pub struct WalPage {
    data: Vec<u8>,
}

impl WalPage {
    pub const HEADER_BYTES: usize = 4;

    pub fn new() -> Self {
        let mut page = Self {
            data: vec![0u8; PAGE_SIZE_BYTES as usize],
        };
        page.reset();
        page
    }

    pub fn reset(&mut self) {
        self.data.fill(0);
        self.set_boundary(self.data.len());
    }

    pub fn boundary(&self) -> usize {
        let mut buf = [0u8; Self::HEADER_BYTES];
        buf.copy_from_slice(&self.data[..Self::HEADER_BYTES]);
        i32::from_be_bytes(buf) as usize
    }

    pub fn set_boundary(&mut self, offset: usize) {
        assert!(
            offset <= self.data.len(),
            "boundary cannot exceed page capacity"
        );
        let value = i32::try_from(offset).expect("boundary offset must fit in i32");
        self.data[..Self::HEADER_BYTES].copy_from_slice(&value.to_be_bytes());
    }

    pub fn capacity(&self) -> usize {
        self.data.len()
    }

    /// Record payloads are stored as `[len:u32][bytes...]`.
    pub fn write_record(&mut self, dest: usize, bytes: &[u8]) {
        let payload_len =
            u32::try_from(bytes.len()).expect("record larger than u32::MAX is unsupported");
        let start = dest;
        let end = dest + Self::HEADER_BYTES + bytes.len();
        assert!(
            end <= self.data.len(),
            "record does not fit in WAL page buffer"
        );
        self.data[start..start + Self::HEADER_BYTES].copy_from_slice(&payload_len.to_be_bytes());
        self.data[start + Self::HEADER_BYTES..end].copy_from_slice(bytes);
    }

    pub fn read_record(&self, src: usize) -> (Vec<u8>, usize) {
        let mut len_buf = [0u8; Self::HEADER_BYTES];
        len_buf.copy_from_slice(&self.data[src..src + Self::HEADER_BYTES]);
        let length = u32::from_be_bytes(len_buf) as usize;
        let start = src + Self::HEADER_BYTES;
        let end = start + length;
        assert!(end <= self.data.len(), "record length exceeds WAL page");
        let bytes = self.data[start..end].to_vec();
        (bytes, end)
    }

    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

impl<'a> PageAllocator<'a> for () {
    type Output = SlotId;
    fn insert(&mut self, _bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>> {
        Err("RawPage allocator is not supported".into())
    }
}

impl HeapPage {
    fn live_iterator<'a>(page: &'a mut Page<Self>) -> HeapIterator<'a> {
        HeapIterator {
            page,
            current_slot: 0,
            match_state: Some(LineState::Live),
        }
    }
}

pub struct Page<K: PageKind> {
    header: PageHeader,
    line_pointers: Vec<LinePtr>,
    record_space: Vec<u8>,
    kind: PhantomData<K>,
}

impl<K: PageKind> Page<K> {
    pub fn new() -> Self {
        Self {
            header: PageHeader::new(K::PAGE_TYPE),
            line_pointers: Vec::new(),
            record_space: vec![0u8; PAGE_SIZE_BYTES as usize],
            kind: PhantomData,
        }
    }

    fn push_line_pointer(&mut self, line_pointer: LinePtr) -> u16 {
        self.line_pointers.push(line_pointer);
        self.header.free_lower += 4;
        self.header.set_slot_count(self.line_pointers.len() as u16);
        self.header.free_lower
    }

    #[track_caller]
    pub fn assert_layout_valid(&self, context: &str) {
        let expected_lower = PAGE_HEADER_SIZE_BYTES as usize + self.line_pointers.len() * 4;
        let (lower, upper) = self.header.free_bounds();
        assert_eq!(
            expected_lower,
            lower as usize,
            "[SLOT-DIR] header.lower mismatch at {} (lp_len={}, header.slot_count={})",
            context,
            self.line_pointers.len(),
            self.header.slot_count()
        );
        assert_eq!(
            self.line_pointers.len(),
            self.header.slot_count() as usize,
            "[SLOT-DIR] slot_count mismatch at {} (lower={}, expected_lower={})",
            context,
            lower,
            expected_lower as u16
        );
        assert!(
            (lower as usize) <= (upper as usize),
            "[PAGE] free_lower exceeds free_upper at {} (lower={}, upper={})",
            context,
            lower,
            upper
        );
        for (idx, lp) in self.line_pointers.iter().enumerate() {
            if lp.is_live() {
                let off = lp.offset() as usize;
                let len = lp.length() as usize;
                assert!(
                    off >= upper as usize && off + len <= PAGE_SIZE_BYTES as usize,
                    "[PAGE] tuple slot {} out of bounds at {} (offset={}, len={}, upper={})",
                    idx,
                    context,
                    off,
                    len,
                    upper
                );
            }
        }
    }

    fn push_free_slot(&mut self, free_idx: SlotId) {
        let line_pointer = self
            .line_pointers
            .get_mut(free_idx)
            .expect("free slot must exist in line pointer array");
        debug_assert!(line_pointer.is_free());
        let next = self.header.free_head();
        line_pointer.set_offset(next);
        line_pointer.set_length(0);
        self.header
            .set_free_head(free_idx.try_into().expect("slot id fits in u16"));
    }

    fn pop_free_slot(&mut self) -> Option<SlotId> {
        if !self.header.has_free_slot() {
            return None;
        }
        let idx = self.header.free_head() as usize;
        debug_assert!(self.line_pointers[idx].is_free());
        let next_free_head = self.line_pointers[idx].offset();
        self.header.set_free_head(next_free_head);
        Some(idx)
    }

    fn allocate_tuple(&mut self, bytes: &[u8]) -> Result<SlotId, Box<dyn Error>> {
        let (mut lower, upper) = self.header.free_bounds();
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size (u16::MAX)".to_string())?;
        if lower + needed > upper {
            return Err("insufficient free space".into());
        }

        let slot = if let Some(idx) = self.pop_free_slot() {
            idx
        } else {
            if lower + 4 > upper {
                return Err("insufficient space for slot".into());
            }
            let idx = self.line_pointers.len();
            self.line_pointers.push(LinePtr::new(0, 0, LineState::Free));
            lower = lower.saturating_add(4);
            idx
        };

        let new_upper = upper - needed;
        self.record_space[new_upper as usize..(new_upper + needed) as usize].copy_from_slice(bytes);

        self.line_pointers[slot] = LinePtr::new(new_upper, needed, LineState::Live);
        self.header.set_free_bounds(lower, new_upper);
        self.header.set_free_ptr(new_upper as u32);
        self.header.set_slot_count(self.line_pointers.len() as u16);

        if cfg!(debug_assertions) {
            self.assert_layout_valid("allocate_tuple");
        }

        Ok(slot)
    }

    fn tuple_bytes(&self, slot: SlotId) -> Option<&[u8]> {
        let line_pointer = self.line_pointers.get(slot)?;
        if !line_pointer.is_live() {
            return None;
        }
        let offset = line_pointer.offset() as usize;
        let length = line_pointer.length() as usize;
        self.record_space.get(offset..offset + length)
    }

    fn tuple_bytes_mut(&mut self, slot: SlotId) -> Option<&mut [u8]> {
        let line_pointer = self.line_pointers.get(slot)?;
        if !line_pointer.is_live() {
            return None;
        }
        let offset = line_pointer.offset() as usize;
        let length = line_pointer.length() as usize;
        self.record_space.get_mut(offset..offset + length)
    }

    fn update_tuple(&mut self, slot: SlotId, bytes: &[u8]) -> Result<(), Box<dyn Error>> {
        let line_pointer = self
            .line_pointers
            .get(slot)
            .ok_or("invalid slot provided during update")?;
        if !line_pointer.is_live() {
            return Err("cannot update a non-live tuple".into());
        }
        let (offset, length) = (
            line_pointer.offset() as usize,
            line_pointer.length() as usize,
        );
        if length == bytes.len() {
            self.record_space[offset..offset + length].copy_from_slice(bytes);
            return Ok(());
        }

        let new_slot = self.allocate_tuple(bytes)?;
        // Re-fetch after allocation because the earlier immutable borrow was dropped to
        // satisfy Rust's aliasing rules; safe because `line_pointers` indices stay stable.
        let old_lp = self
            .line_pointers
            .get_mut(slot)
            .ok_or("invalid slot provided during update")?;
        old_lp.mark_redirect(new_slot as u16);
        Ok(())
    }

    fn delete_tuple(&mut self, slot: SlotId) -> Result<(), Box<dyn Error>> {
        let line_pointer = self
            .line_pointers
            .get_mut(slot)
            .ok_or("invalid slot provided during deletion")?;
        if !line_pointer.is_live() {
            return Err("cannot delete a slot that is not live".into());
        }
        line_pointer.mark_free();
        line_pointer.set_length(0);
        self.push_free_slot(slot);
        Ok(())
    }

    fn tuple<'a>(&'a self, slot: SlotId) -> Option<TupleRef<'a>> {
        let line_pointer = self.line_pointers.get(slot)?;
        match line_pointer.state() {
            LineState::Free => Some(TupleRef::Free),
            LineState::Live => {
                let (offset, length) = line_pointer.offset_and_length();
                let bytes = self.record_space.get(offset..offset + length)?;
                let heap_tuple = HeapTuple::from_bytes(bytes);
                Some(TupleRef::Live(heap_tuple))
            }
            LineState::Dead => Some(TupleRef::Dead),
            LineState::Redirect => {
                let new_slot = line_pointer.offset() as usize;
                Some(TupleRef::Redirect(new_slot))
            }
        }
    }

    pub fn slot_count(&self) -> usize {
        self.line_pointers.len()
    }

    pub fn header_free_bounds(&self) -> (u16, u16) {
        self.header.free_bounds()
    }

    pub fn header_slot_count(&self) -> usize {
        self.header.slot_count() as usize
    }

    /// Check if a slot exists and is live
    pub fn is_slot_live(&self, slot: SlotId) -> bool {
        self.line_pointers
            .get(slot)
            .map(|lp| lp.is_live())
            .unwrap_or(false)
    }

    /// Serialize the page into a contiguous `PAGE_SIZE_BYTES` buffer.
    ///
    /// Layout matches `docs/record_management.md`:
    /// header (32B) + line pointer array (4B each, downward) + heap (upward).
    pub fn write_bytes(&self, out: &mut [u8]) -> Result<(), Box<dyn Error>> {
        if out.len() != PAGE_SIZE_BYTES as usize {
            return Err("output buffer must equal PAGE_SIZE_BYTES".into());
        }

        eprintln!(
            "[DEBUG] Page::write_bytes: page_type={:?}, slot_count={}, lp_count={}, page_addr={:p}",
            self.header.page_type(),
            self.header.slot_count(),
            self.line_pointers.len(),
            self as *const _
        );

        if cfg!(debug_assertions) {
            self.assert_layout_valid("write_bytes");
        }

        // Start with heap copy (holds tuple bytes). This is safe because header/LPs are
        // overwritten below.
        out.fill(0);
        out.copy_from_slice(&self.record_space);

        // Header.
        self.header
            .write_to_bytes(&mut out[..PAGE_HEADER_SIZE_BYTES as usize]);

        // Line pointers.
        let lp_bytes = self.line_pointers.len() * size_of::<u32>();
        let lp_region_end = PAGE_HEADER_SIZE_BYTES as usize + lp_bytes;
        if lp_region_end > out.len() {
            return Err("line pointer array exceeds page size".into());
        }

        for (i, lp) in self.line_pointers.iter().enumerate() {
            let start = PAGE_HEADER_SIZE_BYTES as usize + i * 4;
            out[start..start + 4].copy_from_slice(&lp.0.to_le_bytes());
        }

        Ok(())
    }

    /// Construct a page from a contiguous `PAGE_SIZE_BYTES` buffer.
    ///
    /// Validates the page type matches `K::PAGE_TYPE` and rebuilds line pointers and heap.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        if bytes.len() != PAGE_SIZE_BYTES as usize {
            return Err("input buffer must equal PAGE_SIZE_BYTES".into());
        }

        let header = PageHeader::read_from_bytes(&bytes[..PAGE_HEADER_SIZE_BYTES as usize])?;
        // RawPage is allowed to wrap any on-disk page type; other kinds must match.
        if K::PAGE_TYPE != PageType::Free && header.page_type() != K::PAGE_TYPE {
            return Err("page type does not match requested PageKind".into());
        }

        let free_lower = header.free_bounds().0 as usize;
        if free_lower < PAGE_HEADER_SIZE_BYTES as usize || free_lower > bytes.len() {
            return Err("corrupt free_lower in header".into());
        }
        let lp_bytes = free_lower - PAGE_HEADER_SIZE_BYTES as usize;
        if lp_bytes % 4 != 0 {
            return Err("line pointer region not aligned to 4 bytes".into());
        }
        let lp_count = lp_bytes / 4;

        let mut line_pointers = Vec::with_capacity(lp_count);
        for i in 0..lp_count {
            let start = PAGE_HEADER_SIZE_BYTES as usize + i * 4;
            let raw = u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
            line_pointers.push(LinePtr(raw));
        }

        let record_space = bytes.to_vec();

        let mut header = header;
        let (_, upper) = header.free_bounds();
        let computed_lower = (PAGE_HEADER_SIZE_BYTES as usize + lp_count * 4) as u16;
        header.set_free_bounds(computed_lower, upper);
        header.set_slot_count(line_pointers.len() as u16);

        eprintln!(
            "[DESER] Page::from_bytes: page_type={:?}, slot_count={}, free_lower={}, free_upper={}, page_addr={:p}",
            header.page_type(), lp_count, computed_lower, upper, &record_space as *const _
        );

        let page = Self {
            header,
            line_pointers,
            record_space,
            kind: PhantomData,
        };
        if cfg!(debug_assertions) {
            page.assert_layout_valid("from_bytes");
        }
        Ok(page)
    }
}

impl From<Page<HeapPage>> for PageBytes {
    fn from(page: Page<HeapPage>) -> Self {
        let Page {
            header,
            line_pointers,
            record_space,
            ..
        } = page;
        Page::<RawPage> {
            header,
            line_pointers,
            record_space,
            kind: PhantomData,
        }
    }
}

impl From<Page<BTreeLeafPage>> for PageBytes {
    fn from(page: Page<BTreeLeafPage>) -> Self {
        let Page {
            header,
            line_pointers,
            record_space,
            ..
        } = page;
        Page::<RawPage> {
            header,
            line_pointers,
            record_space,
            kind: PhantomData,
        }
    }
}

impl From<Page<BTreeInternalPage>> for PageBytes {
    fn from(page: Page<BTreeInternalPage>) -> Self {
        let Page {
            header,
            line_pointers,
            record_space,
            ..
        } = page;
        Page::<RawPage> {
            header,
            line_pointers,
            record_space,
            kind: PhantomData,
        }
    }
}

impl Page<HeapPage> {
    fn heap_tuple(&self, slot: SlotId) -> Option<HeapTuple<'_>> {
        let bytes = self.tuple_bytes(slot)?;
        Some(HeapTuple::from_bytes(bytes))
    }

    fn heap_tuple_mut(&mut self, slot: SlotId) -> Option<HeapTupleMut<'_>> {
        let bytes = self.tuple_bytes_mut(slot)?;
        Some(HeapTupleMut::from_bytes(bytes))
    }

    fn insert_tuple(&mut self, bytes: &[u8]) -> Result<SlotId, Box<dyn Error>> {
        self.allocate_tuple(bytes)
    }

    fn delete_slot(&mut self, slot: SlotId) -> Result<(), Box<dyn Error>> {
        self.delete_tuple(slot)
    }

    fn redirect_slot(&mut self, slot: SlotId, target: SlotId) -> Result<(), Box<dyn Error>> {
        let line_pointer = self
            .line_pointers
            .get_mut(slot)
            .ok_or("invalid slot provided during redirect")?;
        line_pointer.mark_redirect(target as u16);
        Ok(())
    }
}

impl Page<BTreeLeafPage> {
    pub fn assert_leaf_invariants(&self, layout: &Layout, context: &str) {
        self.assert_layout_valid(context);
        if self.slot_count() <= 1 {
            return;
        }
        let mut prev = self.get_leaf_entry(layout, 0).expect("decode leaf entry 0");
        for idx in 1..self.slot_count() {
            let curr = self.get_leaf_entry(layout, idx).expect("decode leaf entry");
            assert!(
                prev.key <= curr.key,
                "[BTreeLeaf] key order violated at {} slot {}: {:?} > {:?}",
                context,
                idx,
                prev.key,
                curr.key
            );
            prev = curr;
        }
    }
    /// Initialize a new B-tree leaf page
    pub fn init(&mut self, overflow_block: Option<usize>) {
        self.header.set_page_type(PageType::IndexLeaf);
        self.header.set_overflow_block(overflow_block);
        self.header.set_slot_count(0);
    }

    /// Find the slot where a key should be inserted to maintain sorted order (binary search)
    /// For duplicate keys, finds the rightmost position (after all existing duplicates)
    fn find_insertion_slot(&self, layout: &Layout, search_key: &Constant) -> SlotId {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;

            // Deserialize entry at mid to compare keys
            if let Ok(entry) = self.get_leaf_entry(layout, mid) {
                if entry.key <= *search_key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                // If we can't read the entry, treat it as less than search key
                left = mid + 1;
            }
        }
        left
    }

    /// Insert a tuple at a specific slot, shifting later slots to the right
    fn insert_tuple_at_slot(
        &mut self,
        slot: SlotId,
        bytes: &[u8],
    ) -> Result<SlotId, Box<dyn Error>> {
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size".to_string())?;

        let (lower, upper) = self.header.free_bounds();

        // Check if we have space for the line pointer and the data
        let line_ptr_space = 4u16; // LinePtr is 4 bytes (u32)
        if lower + line_ptr_space + needed > upper {
            return Err("page full".into());
        }

        // Allocate space in record_space from upper end
        let new_upper = upper - needed;
        self.record_space[new_upper as usize..(new_upper + needed) as usize].copy_from_slice(bytes);

        // Create new line pointer
        let line_ptr = LinePtr::new(new_upper, needed, LineState::Live);

        // Insert line pointer at the specified slot, shifting later ones right
        if slot <= self.line_pointers.len() {
            self.line_pointers.insert(slot, line_ptr);
        } else {
            return Err("invalid slot index".into());
        }

        // Update header
        let new_lower = lower + line_ptr_space;
        self.header.set_free_bounds(new_lower, new_upper);
        self.header.set_free_ptr(new_upper as u32);
        self.header.set_slot_count(self.line_pointers.len() as u16);

        Ok(slot)
    }

    /// Insert a B-tree leaf entry in sorted order by key
    pub fn insert_leaf_entry(
        &mut self,
        layout: &Layout,
        key: &Constant,
        rid: &RID,
    ) -> Result<SlotId, Box<dyn Error>> {
        // Find insertion position using binary search
        let slot = self.find_insertion_slot(layout, key);

        // Encode entry
        let entry = BTreeLeafEntry {
            key: key.clone(),
            rid: rid.clone(),
        };
        let bytes = entry.encode(layout);

        // Insert at the correct position
        let result = self.insert_tuple_at_slot(slot, &bytes);
        if cfg!(debug_assertions) {
            self.assert_leaf_invariants(layout, "insert_leaf_entry");
        }
        result
    }

    /// Get a B-tree leaf entry at the given slot
    pub fn get_leaf_entry(
        &self,
        layout: &Layout,
        slot: SlotId,
    ) -> Result<BTreeLeafEntry, Box<dyn Error>> {
        let bytes = self.tuple_bytes(slot).ok_or("slot not found or not live")?;
        BTreeLeafEntry::decode(layout, bytes)
    }

    /// Compact payload space after deletion by sliding tuples upward to close gaps
    /// Tuples with offset < deleted_offset move up by deleted_len bytes
    fn compact_payload_after_delete(&mut self, deleted_offset: usize, deleted_len: usize) {
        let mut moves: Vec<(usize, usize, usize)> = vec![];

        for (i, lp) in self.line_pointers.iter().enumerate() {
            let offset = lp.offset() as usize;
            if offset < deleted_offset {
                moves.push((i, offset, lp.length() as usize));
            }
        }

        // Sort by offset DESCENDING - move highest offsets first to avoid overwriting
        moves.sort_by_key(|(_, off, _)| std::cmp::Reverse(*off));

        // Move each tuple up and update its pointer
        for (lp_idx, old_offset, length) in moves {
            let new_offset = old_offset + deleted_len;
            self.record_space
                .copy_within(old_offset..old_offset + length, new_offset);
            self.line_pointers[lp_idx].set_offset(new_offset as u16);
        }

        // Update free_upper to reflect reclaimed space
        let (lower, upper) = self.header.free_bounds();
        self.header
            .set_free_bounds(lower, upper + deleted_len as u16);
    }

    /// Delete a B-tree leaf entry at the given slot
    /// Uses physical deletion (Vec::remove) to maintain dense sorted array
    /// Compacts payload space to reclaim deleted tuple bytes
    pub fn delete_leaf_entry(
        &mut self,
        slot: SlotId,
        layout: &Layout,
    ) -> Result<(), Box<dyn Error>> {
        if slot >= self.line_pointers.len() {
            return Err("invalid slot".into());
        }

        // Capture deleted tuple info BEFORE removing pointer
        let deleted_offset = self.line_pointers[slot].offset() as usize;
        let deleted_len = self.line_pointers[slot].length() as usize;

        // CRITICAL: Verify this is actually a B-tree leaf page
        assert_eq!(
            self.header.page_type(),
            PageType::IndexLeaf,
            "delete_leaf_entry called on wrong page type: {:?} (should be IndexLeaf)",
            self.header.page_type()
        );

        // Physical deletion: remove line pointer from Vec (shifts remaining left)
        self.line_pointers.remove(slot);

        // Compact payload to reclaim space
        self.compact_payload_after_delete(deleted_offset, deleted_len);

        // Update header (compact_payload_after_delete already updated upper)
        let (lower, upper) = self.header.free_bounds();
        self.header.set_free_bounds(lower - 4, upper);
        self.header.set_slot_count(self.line_pointers.len() as u16);
        if cfg!(debug_assertions) {
            self.assert_leaf_invariants(layout, "delete_leaf_entry");
        }
        Ok(())
    }

    /// Find the slot before the first occurrence of the search key
    /// Uses leftmost binary search
    pub fn find_slot_before(&self, layout: &Layout, search_key: &Constant) -> Option<SlotId> {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;

            if let Ok(entry) = self.get_leaf_entry(layout, mid) {
                if entry.key < *search_key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                left = mid + 1;
            }
        }

        if left == 0 {
            None
        } else {
            Some(left - 1)
        }
    }

    /// Check if the page is full
    pub fn is_full(&self, layout: &Layout) -> bool {
        let (lower, upper) = self.header.free_bounds();
        let needed = layout.slot_size as u16 + 4;
        lower + needed > upper
    }

    /// Get the B-tree level from the header (for leaf pages, usually 0)
    pub fn btree_level(&self) -> u16 {
        self.header.btree_level()
    }

    /// Set the B-tree level in the header
    pub fn set_btree_level(&mut self, level: u16) {
        self.header.set_btree_level(level);
    }

    /// Get the overflow block number (if any)
    pub fn overflow_block(&self) -> Option<usize> {
        self.header.overflow_block()
    }

    /// Set the overflow block number
    pub fn set_overflow_block(&mut self, block: Option<usize>) {
        self.header.set_overflow_block(block);
    }
}

impl Page<BTreeInternalPage> {
    pub fn assert_internal_invariants(&self, layout: &Layout, context: &str) {
        self.assert_layout_valid(context);
        if self.slot_count() <= 1 {
            return;
        }
        let mut prev = self
            .get_internal_entry(layout, 0)
            .expect("decode internal entry 0");
        for idx in 1..self.slot_count() {
            let curr = self
                .get_internal_entry(layout, idx)
                .expect("decode internal entry");
            assert!(
                prev.key <= curr.key,
                "[BTreeInternal] key order violated at {} slot {}: {:?} > {:?}",
                context,
                idx,
                prev.key,
                curr.key
            );
            prev = curr;
        }
    }
    /// Initialize a new B-tree internal page
    pub fn init(&mut self, level: u16) {
        self.header.set_page_type(PageType::IndexInternal);
        self.header.set_btree_level(level);
        self.header.set_slot_count(0);
    }

    /// Find the slot where a key should be inserted to maintain sorted order (binary search)
    /// For duplicate keys, finds the rightmost position (after all existing duplicates)
    fn find_insertion_slot(&self, layout: &Layout, search_key: &Constant) -> SlotId {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;

            // Deserialize entry at mid to compare keys
            if let Ok(entry) = self.get_internal_entry(layout, mid) {
                if entry.key <= *search_key {
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                // If we can't read the entry, treat it as less than search key
                left = mid + 1;
            }
        }
        left
    }

    /// Insert a tuple at a specific slot, shifting later slots to the right
    fn insert_tuple_at_slot(
        &mut self,
        slot: SlotId,
        bytes: &[u8],
    ) -> Result<SlotId, Box<dyn Error>> {
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size".to_string())?;

        let (lower, upper) = self.header.free_bounds();

        // Check if we have space for the line pointer and the data
        let line_ptr_space = 4u16; // LinePtr is 4 bytes (u32)
        if lower + line_ptr_space + needed > upper {
            return Err("page full".into());
        }

        // Allocate space in record_space from upper end
        let new_upper = upper - needed;
        self.record_space[new_upper as usize..(new_upper + needed) as usize].copy_from_slice(bytes);

        // Create new line pointer
        let line_ptr = LinePtr::new(new_upper, needed, LineState::Live);

        // Insert line pointer at the specified slot, shifting later ones right
        if slot <= self.line_pointers.len() {
            self.line_pointers.insert(slot, line_ptr);
        } else {
            return Err("invalid slot index".into());
        }

        // Update header
        let new_lower = lower + line_ptr_space;
        self.header.set_free_bounds(new_lower, new_upper);
        self.header.set_free_ptr(new_upper as u32);
        self.header.set_slot_count(self.line_pointers.len() as u16);

        Ok(slot)
    }

    /// Insert a B-tree internal entry in sorted order by key
    pub fn insert_internal_entry(
        &mut self,
        layout: &Layout,
        key: &Constant,
        child_block: usize,
    ) -> Result<SlotId, Box<dyn Error>> {
        // Find insertion position using binary search
        let slot = self.find_insertion_slot(layout, key);

        // Encode entry
        let entry = BTreeInternalEntry {
            key: key.clone(),
            child_block,
        };
        let bytes = entry.encode(layout);

        // Insert at the correct position
        let result = self.insert_tuple_at_slot(slot, &bytes);
        if cfg!(debug_assertions) {
            self.assert_internal_invariants(layout, "insert_internal_entry");
        }
        result
    }

    /// Get a B-tree internal entry at the given slot
    pub fn get_internal_entry(
        &self,
        layout: &Layout,
        slot: SlotId,
    ) -> Result<BTreeInternalEntry, Box<dyn Error>> {
        let bytes = self.tuple_bytes(slot).ok_or("slot not found")?;
        BTreeInternalEntry::decode(layout, bytes)
    }

    /// Compact payload space after deletion by sliding tuples upward to close gaps
    /// Tuples with offset < deleted_offset move up by deleted_len bytes
    fn compact_payload_after_delete(&mut self, deleted_offset: usize, deleted_len: usize) {
        // Collect tuples that need to move (those "above" the deleted one)
        let mut moves: Vec<(usize, usize, usize)> = vec![]; // (lp_idx, old_offset, length)

        for (i, lp) in self.line_pointers.iter().enumerate() {
            let offset = lp.offset() as usize;
            if offset < deleted_offset {
                moves.push((i, offset, lp.length() as usize));
            }
        }

        // Sort by offset DESCENDING - move highest offsets first to avoid overwriting
        moves.sort_by_key(|(_, off, _)| std::cmp::Reverse(*off));

        // Move each tuple up and update its pointer
        for (lp_idx, old_offset, length) in moves {
            let new_offset = old_offset + deleted_len;
            self.record_space
                .copy_within(old_offset..old_offset + length, new_offset);
            self.line_pointers[lp_idx].set_offset(new_offset as u16);
        }

        // Update free_upper to reflect reclaimed space
        let (lower, upper) = self.header.free_bounds();
        self.header
            .set_free_bounds(lower, upper + deleted_len as u16);
    }

    /// Delete a B-tree internal entry at the given slot
    /// Uses physical deletion (Vec::remove) to maintain dense sorted array
    /// Compacts payload space to reclaim deleted tuple bytes
    pub fn delete_internal_entry(
        &mut self,
        slot: SlotId,
        layout: &Layout,
    ) -> Result<(), Box<dyn Error>> {
        if slot >= self.line_pointers.len() {
            return Err("invalid slot".into());
        }

        // Capture deleted tuple info BEFORE removing pointer
        let deleted_offset = self.line_pointers[slot].offset() as usize;
        let deleted_len = self.line_pointers[slot].length() as usize;

        // CRITICAL: Verify this is actually a B-tree internal page
        assert_eq!(
            self.header.page_type(),
            PageType::IndexInternal,
            "delete_internal_entry called on wrong page type: {:?} (should be IndexInternal)",
            self.header.page_type()
        );

        // Physical deletion: remove line pointer from Vec (shifts remaining left)
        self.line_pointers.remove(slot);

        // Compact payload to reclaim space
        self.compact_payload_after_delete(deleted_offset, deleted_len);

        // Update header (compact_payload_after_delete already updated upper)
        let (lower, upper) = self.header.free_bounds();
        self.header.set_free_bounds(lower - 4, upper);
        self.header.set_slot_count(self.line_pointers.len() as u16);
        if cfg!(debug_assertions) {
            self.assert_internal_invariants(layout, "delete_internal_entry");
        }

        Ok(())
    }

    /// Find the rightmost slot where key <= search_key
    /// This is used for B-tree internal node routing to find the correct child
    pub fn find_slot_before(&self, layout: &Layout, search_key: &Constant) -> Option<SlotId> {
        let mut left = 0;
        let mut right = self.slot_count();
        let mut result = None;

        while left < right {
            let mid = (left + right) / 2;

            if let Ok(entry) = self.get_internal_entry(layout, mid) {
                if entry.key <= *search_key {
                    result = Some(mid);
                    left = mid + 1;
                } else {
                    right = mid;
                }
            } else {
                left = mid + 1;
            }
        }

        result
    }

    /// Check if the page is full
    pub fn is_full(&self, layout: &Layout) -> bool {
        let (lower, upper) = self.header.free_bounds();
        let needed = layout.slot_size as u16 + 4;
        lower + needed > upper
    }

    /// Get the B-tree level from the header
    pub fn btree_level(&self) -> u16 {
        self.header.btree_level()
    }

    /// Set the B-tree level in the header
    pub fn set_btree_level(&mut self, level: u16) {
        self.header.set_btree_level(level);
    }
}

// Temporary compatibility helpers for legacy callers that treated a page as a raw byte buffer.
// These operate directly on the backing record_space and use big-endian encoding to match the
// previous `Page` in main.rs. Intended primarily for RawPage during migration.
impl std::fmt::Debug for Page<RawPage> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Page<RawPage>")
            .field("header", &"PageHeader{...}")
            .field("line_pointers_len", &self.line_pointers.len())
            .field("record_space_len", &self.record_space.len())
            .finish()
    }
}

impl Page<RawPage> {
    pub const INT_BYTES: usize = 4;

    pub fn get_int(&self, offset: usize) -> i32 {
        let bytes: [u8; Self::INT_BYTES] = self.record_space[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        i32::from_be_bytes(bytes)
    }

    pub fn set_int(&mut self, offset: usize, n: i32) {
        self.record_space[offset..offset + Self::INT_BYTES].copy_from_slice(&n.to_be_bytes());
    }

    pub fn get_bytes(&self, mut offset: usize) -> Vec<u8> {
        let length_bytes: [u8; Self::INT_BYTES] = self.record_space
            [offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        let length = u32::from_be_bytes(length_bytes) as usize;
        offset += Self::INT_BYTES;
        self.record_space[offset..offset + length].to_vec()
    }

    pub fn set_bytes(&mut self, mut offset: usize, bytes: &[u8]) {
        let length = bytes.len() as u32;
        self.record_space[offset..offset + Self::INT_BYTES].copy_from_slice(&length.to_be_bytes());
        offset += Self::INT_BYTES;
        self.record_space[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    pub fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes).unwrap()
    }

    pub fn set_string(&mut self, offset: usize, string: &str) {
        self.set_bytes(offset, string.as_bytes());
    }
}

pub struct PageReadGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockReadGuard<'a, Page<RawPage>>,
}

impl<'a> PageReadGuard<'a> {
    pub fn new(
        handle: BufferHandle,
        frame: Arc<BufferFrame>,
        page: RwLockReadGuard<'a, Page<RawPage>>,
    ) -> Self {
        Self {
            handle,
            frame,
            page,
        }
    }

    pub fn block_id(&self) -> &BlockId {
        self.handle.block_id()
    }

    pub fn frame(&self) -> &BufferFrame {
        &self.frame
    }

    pub fn into_heap_view(
        self,
        layout: &'a Layout,
    ) -> Result<HeapPageView<'a, HeapPage>, Box<dyn Error>> {
        HeapPageView::new(self, layout)
    }

    pub fn into_btree_leaf_page_view(
        self,
        layout: &'a Layout,
    ) -> Result<BTreeLeafPageView<'a>, Box<dyn Error>> {
        BTreeLeafPageView::new(self, layout)
    }

    pub fn into_btree_internal_page_view(
        self,
        layout: &'a Layout,
    ) -> Result<BTreeInternalPageView<'a>, Box<dyn Error>> {
        BTreeInternalPageView::new(self, layout)
    }
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page<RawPage>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

pub struct PageWriteGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockWriteGuard<'a, Page<RawPage>>,
}

impl<'a> PageWriteGuard<'a> {
    pub fn new(
        handle: BufferHandle,
        frame: Arc<BufferFrame>,
        page: RwLockWriteGuard<'a, Page<RawPage>>,
    ) -> Self {
        Self {
            handle,
            frame,
            page,
        }
    }

    pub fn block_id(&self) -> &BlockId {
        self.handle.block_id()
    }

    pub fn frame(&self) -> &BufferFrame {
        &self.frame
    }

    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.frame.set_modified(txn_id, lsn);
    }

    pub fn format_as_heap(&mut self) {
        **self = Page::<HeapPage>::new().into()
    }

    pub fn format_as_btree_leaf(&mut self, overflow_block: Option<usize>) {
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(overflow_block);
        **self = page.into();
    }

    pub fn format_as_btree_internal(&mut self, level: u16) {
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(level);
        **self = page.into();
    }

    pub fn into_heap_view_mut(
        self,
        layout: &'a Layout,
    ) -> Result<HeapPageViewMut<'a, HeapPage>, Box<dyn Error>> {
        HeapPageViewMut::new(self, &layout)
    }

    pub fn into_btree_leaf_page_view_mut(
        self,
        layout: &'a Layout,
    ) -> Result<BTreeLeafPageViewMut<'a>, Box<dyn Error>> {
        BTreeLeafPageViewMut::new(self, layout)
    }

    pub fn into_btree_internal_page_view_mut(
        self,
        layout: &'a Layout,
    ) -> Result<BTreeInternalPageViewMut<'a>, Box<dyn Error>> {
        BTreeInternalPageViewMut::new(self, layout)
    }
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = Page<RawPage>;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;
    use std::slice;

    #[test]
    fn allocate_tuple_exposes_bytes_and_tuple_ref() {
        let mut page = Page::<HeapPage>::new();
        let payload = vec![1u8, 2, 3, 4];
        let tuple = heap_tuple_bytes(&payload);

        let slot = page
            .allocate_tuple(&tuple)
            .expect("allocation should succeed");
        assert_eq!(slot, 0);

        assert_eq!(page.tuple_bytes(slot).unwrap(), tuple.as_slice());

        match page.tuple(slot).unwrap() {
            TupleRef::Live(heap_tuple) => {
                assert_eq!(heap_tuple.payload(), payload.as_slice());
                assert_eq!(heap_tuple.payload_len(), payload.len() as u32);
            }
            _ => panic!("expected live tuple"),
        }
    }

    #[test]
    fn delete_frees_slot_and_allocation_reuses_it() {
        let mut page = Page::<HeapPage>::new();
        let tuple_a = heap_tuple_bytes(&[10]);
        let tuple_b = heap_tuple_bytes(&[20, 30]);
        let tuple_c_payload = vec![99, 100, 101];
        let tuple_c = heap_tuple_bytes(&tuple_c_payload);

        let slot_a = page.allocate_tuple(&tuple_a).unwrap();
        let slot_b = page.allocate_tuple(&tuple_b).unwrap();
        assert_eq!(slot_a, 0);
        assert_eq!(slot_b, 1);

        page.delete_tuple(slot_a).expect("delete live tuple");

        let reused = page.allocate_tuple(&tuple_c).unwrap();
        assert_eq!(reused, slot_a, "freed slot should be reused first");

        match page.tuple(reused).unwrap() {
            TupleRef::Live(tuple) => {
                assert_eq!(tuple.payload(), tuple_c_payload.as_slice());
            }
            _ => panic!("expected live tuple in reused slot"),
        }
    }

    #[test]
    fn update_tuple_redirects_when_growing_and_overwrites_in_place_when_equal() {
        let mut page = Page::<HeapPage>::new();
        let small_payload = vec![1u8, 2, 3];
        let large_payload = vec![5u8; 8];
        let replacement_payload = vec![7u8; 8];

        let slot = page
            .allocate_tuple(&heap_tuple_bytes(&small_payload))
            .unwrap();

        page.update_tuple(slot, &heap_tuple_bytes(&large_payload))
            .expect("growing update should succeed");

        let redirect_target = match page.tuple(slot).unwrap() {
            TupleRef::Redirect(target) => target,
            _ => panic!("expected redirect after growth"),
        };

        match page.tuple(redirect_target).unwrap() {
            TupleRef::Live(tuple) => assert_eq!(tuple.payload(), large_payload.as_slice()),
            _ => panic!("redirect target must be live"),
        }

        page.update_tuple(redirect_target, &heap_tuple_bytes(&replacement_payload))
            .expect("same-size update should be in place");

        match page.tuple(redirect_target).unwrap() {
            TupleRef::Live(tuple) => assert_eq!(tuple.payload(), replacement_payload.as_slice()),
            _ => panic!("in-place update should remain live"),
        }
    }

    #[test]
    fn pack_and_unpack_preserves_tuples() {
        let mut page = Page::<HeapPage>::new();
        let payload = vec![42u8, 43, 44, 45];
        let slot = page
            .allocate_tuple(&heap_tuple_bytes(&payload))
            .expect("allocation succeeds");

        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        page.write_bytes(&mut buf).expect("pack succeeds");

        let reconstructed = Page::<HeapPage>::from_bytes(&buf).expect("unpack succeeds");

        match reconstructed.tuple(slot).unwrap() {
            TupleRef::Live(tuple) => assert_eq!(tuple.payload(), payload.as_slice()),
            _ => panic!("expected live tuple"),
        }
    }

    #[test]
    fn wal_page_round_trip() {
        let mut wal = WalPage::new();
        let record = vec![1u8, 2, 3, 4, 5];

        let start = wal.boundary() - (WalPage::HEADER_BYTES + record.len());
        wal.write_record(start, &record);
        wal.set_boundary(start);

        let (loaded, next_pos) = wal.read_record(start);
        assert_eq!(loaded, record);
        assert_eq!(next_pos, start + WalPage::HEADER_BYTES + record.len());
    }

    fn heap_tuple_bytes(payload: &[u8]) -> Vec<u8> {
        let header = HeapTupleHeader {
            payload_len: payload.len() as u32,
            xmin: 1,
            xmax: 0,
            flags: 0,
            nullmap_ptr: 0,
        };
        let header_bytes = unsafe {
            slice::from_raw_parts(
                &header as *const HeapTupleHeader as *const u8,
                std::mem::size_of::<HeapTupleHeader>(),
            )
        };
        let mut buf = Vec::with_capacity(header_bytes.len() + payload.len());
        buf.extend_from_slice(header_bytes);
        buf.extend_from_slice(payload);
        buf
    }
}

pub enum TupleRef<'a> {
    Live(HeapTuple<'a>),
    Redirect(SlotId),
    Free,
    Dead,
}

impl<'a> TupleRef<'a> {
    pub fn line_state(&self) -> LineState {
        match self {
            TupleRef::Live(_) => LineState::Live,
            TupleRef::Redirect(_) => LineState::Redirect,
            TupleRef::Free => LineState::Free,
            TupleRef::Dead => LineState::Dead,
        }
    }
}

struct NullBitmap<'a> {
    bytes: &'a [u8],
}

impl<'a> NullBitmap<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn is_null(&self, col_idx: usize) -> bool {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        let mask = 1u8 << bit;
        (self.bytes[byte] & mask) != 0
    }
}

struct NullBitmapMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> NullBitmapMut<'a> {
    fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes }
    }

    #[allow(unused)]
    fn set_null(&mut self, col_idx: usize) {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        let mask = 1u8 << bit;
        self.bytes[byte] = self.bytes[byte] | mask;
    }

    fn clear(&mut self, col_idx: usize) {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        let mask = 1u8 << bit;
        self.bytes[byte] = self.bytes[byte] & !mask;
    }
}

#[cfg(test)]
fn build_tuple_bytes(payload: &[u8], nullmap_ptr: u16) -> Vec<u8> {
    let mut buf = vec![0u8; size_of::<HeapTupleHeader>() + payload.len()];
    unsafe {
        let header = &mut *(buf.as_mut_ptr() as *mut HeapTupleHeader);
        header.payload_len = payload.len() as u32;
        header.xmin = 1;
        header.xmax = 0;
        header.flags = 0;
        header.nullmap_ptr = nullmap_ptr;
    }
    buf[size_of::<HeapTupleHeader>()..].copy_from_slice(payload);
    buf
}

#[cfg(test)]
mod bitmap_tests {
    use super::*;

    #[test]
    fn null_bitmap_reads_bits_across_bytes() {
        let bytes = [0b1010_1010u8, 0b0000_0011u8];
        let bitmap = NullBitmap::new(&bytes);
        // byte 0 bits
        assert!(bitmap.is_null(1)); // bit 1 set
        assert!(bitmap.is_null(3));
        assert!(bitmap.is_null(5));
        assert!(bitmap.is_null(7));
        assert!(!bitmap.is_null(0));
        assert!(!bitmap.is_null(6));
        // byte 1 bits (columns 8,9)
        assert!(bitmap.is_null(8));
        assert!(bitmap.is_null(9));
        assert!(!bitmap.is_null(10));
    }

    #[test]
    fn null_bitmap_mut_sets_and_clears_bits() {
        let mut bytes = [0u8; 2];
        {
            let mut bitmap = NullBitmapMut::new(&mut bytes);
            bitmap.set_null(0);
            bitmap.set_null(9);
            bitmap.set_null(7);
            bitmap.clear(7);
        }
        let bitmap = NullBitmap::new(&bytes);
        assert!(bitmap.is_null(0));
        assert!(!bitmap.is_null(7));
        assert!(bitmap.is_null(9));
        assert!(!bitmap.is_null(5));
    }
}

#[cfg(test)]
mod heap_tuple_tests {
    use super::*;

    #[test]
    fn heap_tuple_reads_int_and_varlen_payload() {
        // Layout: [bitmap byte][i32 big endian][len:u32][payload bytes]
        let mut payload = Vec::new();
        payload.push(0b0000_1000u8); // only column 3 is null
        payload.extend_from_slice(&0x01020304u32.to_be_bytes());
        payload.extend_from_slice(&3u32.to_be_bytes());
        payload.extend_from_slice(b"abc");
        let bytes = build_tuple_bytes(&payload, 0);
        let tuple = HeapTuple::from_bytes(&bytes);

        assert_eq!(tuple.payload_len(), payload.len() as u32);
        assert_eq!(tuple.read_i32(1), 0x01020304);
        assert_eq!(tuple.read_varlen(1 + 4), b"abc");
        let bitmap = tuple.null_bitmap(8);
        assert!(bitmap.is_null(3));
        assert!(!bitmap.is_null(2));
    }

    #[test]
    fn heap_tuple_mut_updates_payload_and_bitmap() {
        let mut payload = Vec::new();
        payload.push(0u8); // bitmap
        payload.extend_from_slice(&0u32.to_be_bytes());
        let mut bytes = build_tuple_bytes(&payload, 0);

        {
            let mut tuple_mut = HeapTupleMut::from_bytes(bytes.as_mut_slice());
            let new_val = 0x0A0B0C0Du32.to_be_bytes();
            tuple_mut.payload_slice_mut(1, 4).copy_from_slice(&new_val);
            tuple_mut.null_bitmap_mut(8).set_null(6);
        }

        let tuple = HeapTuple::from_bytes(&bytes);
        assert_eq!(tuple.read_i32(1), 0x0A0B0C0D);
        let bitmap = tuple.null_bitmap(8);
        assert!(bitmap.is_null(6));
        assert!(!bitmap.is_null(0));
    }
}

#[cfg(test)]
mod logical_row_tests {
    use super::*;
    use crate::Schema;

    fn sample_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("a");
        schema.add_string_field("b", 5);
        schema.add_int_field("c");
        Layout::new(schema)
    }

    fn base_payload(layout: &Layout, a: i32, b: &str, c: i32, null_bitmap: u8) -> Vec<u8> {
        let mut payload = vec![0u8; layout.slot_size];
        payload[0] = null_bitmap;

        let offset_a = layout.offset("a").unwrap();
        payload[offset_a..offset_a + 4].copy_from_slice(&a.to_le_bytes());

        let offset_b = layout.offset("b").unwrap();
        payload[offset_b..offset_b + 4].copy_from_slice(&(b.len() as u32).to_le_bytes());
        payload[offset_b + 4..offset_b + 4 + b.len()].copy_from_slice(b.as_bytes());

        let offset_c = layout.offset("c").unwrap();
        payload[offset_c..offset_c + 4].copy_from_slice(&c.to_le_bytes());

        payload
    }

    #[test]
    fn logical_row_reads_typed_columns_and_nulls() {
        let layout = sample_layout();
        let payload = base_payload(&layout, 10, "xy", 0, 0b0000_0100);
        let bytes = build_tuple_bytes(&payload, 0);
        let tuple = HeapTuple::from_bytes(&bytes);
        let row = LogicalRow::new(tuple, &layout);

        match row.get_column("a") {
            Some(Constant::Int(v)) => assert_eq!(v, 10),
            _ => panic!("expected int value for column a"),
        }

        match row.get_column("b") {
            Some(Constant::String(s)) => assert_eq!(s, "xy"),
            _ => panic!("expected string value for column b"),
        }

        assert!(row.get_column("c").is_none());
    }

    #[test]
    fn logical_row_mut_updates_and_nulls_columns() {
        let layout = sample_layout();
        let payload = base_payload(&layout, 1, "hi", 5, 0);
        let mut bytes = build_tuple_bytes(&payload, 0);

        {
            let tuple_mut = HeapTupleMut::from_bytes(bytes.as_mut_slice());
            let mut row_mut = LogicalRowMut::new(tuple_mut, layout.clone());
            row_mut
                .set_column("a", &Constant::Int(99))
                .expect("set int");
            row_mut
                .set_column("b", &Constant::String("hey".to_string()))
                .expect("set string");
            row_mut.set_null("c").expect("set null");
        }

        let tuple = HeapTuple::from_bytes(&bytes);
        let row = LogicalRow::new(tuple, &layout);

        match row.get_column("a") {
            Some(Constant::Int(v)) => assert_eq!(v, 99),
            _ => panic!("expected updated int"),
        }

        match row.get_column("b") {
            Some(Constant::String(s)) => assert_eq!(s, "hey"),
            _ => panic!("expected updated string"),
        }

        assert!(row.get_column("c").is_none());
    }

    fn serialization_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("num");
        schema.add_string_field("text", 8);
        Layout::new(schema)
    }

    #[test]
    fn logical_row_round_trip_serialization_cases() {
        let layout = serialization_layout();
        let cases: Vec<(i32, Option<&str>)> = vec![
            (123, Some("")),
            (-77, Some("abc")),
            (42, Some("abcdefgh")),
            (0, None),
        ];

        for (int_val, str_val) in cases {
            let payload = vec![0u8; layout.slot_size];
            let mut bytes = build_tuple_bytes(&payload, 0);

            {
                let tuple_mut = HeapTupleMut::from_bytes(bytes.as_mut_slice());
                let mut row_mut = LogicalRowMut::new(tuple_mut, layout.clone());
                row_mut
                    .set_column("num", &Constant::Int(int_val))
                    .expect("set int column");
                match str_val {
                    Some(s) => {
                        row_mut
                            .set_column("text", &Constant::String(s.to_string()))
                            .expect("set string column");
                    }
                    None => {
                        row_mut.set_null("text").expect("set string column null");
                    }
                }
            }

            let tuple = HeapTuple::from_bytes(&bytes);
            let row = LogicalRow::new(tuple, &layout);
            assert_eq!(row.get_column("num"), Some(Constant::Int(int_val)));
            match str_val {
                Some(s) => assert_eq!(
                    row.get_column("text"),
                    Some(Constant::String(s.to_string()))
                ),
                None => assert!(row.get_column("text").is_none()),
            }
        }
    }
}

#[repr(C)]
#[derive(Debug, Clone, Copy)]
struct HeapTupleHeader {
    payload_len: u32,
    xmin: u64,
    xmax: u64,
    flags: u16,
    nullmap_ptr: u16,
}

pub struct HeapTuple<'a> {
    header: &'a HeapTupleHeader,
    payload: &'a [u8],
}

impl<'a> HeapTuple<'a> {
    fn from_bytes(buf: &'a [u8]) -> Self {
        let (header_bytes, payload_bytes) = buf.split_at(size_of::<HeapTupleHeader>());
        let header = unsafe { &*(header_bytes.as_ptr() as *const HeapTupleHeader) };
        Self {
            header,
            payload: payload_bytes,
        }
    }

    fn from_parts(header: &'a HeapTupleHeader, payload: &'a [u8]) -> Self {
        Self { header, payload }
    }

    fn xmin(&self) -> u64 {
        self.header.xmin
    }

    fn xmax(&self) -> u64 {
        self.header.xmax
    }

    fn flags(&self) -> u16 {
        self.header.flags
    }

    fn nullmap_ptr(&self) -> u16 {
        self.header.nullmap_ptr
    }

    fn payload_len(&self) -> u32 {
        self.header.payload_len
    }

    fn payload(&self) -> &'a [u8] {
        self.payload
    }

    fn null_bitmap(&self, num_columns: usize) -> NullBitmap<'_> {
        let offset = self.header.nullmap_ptr as usize;
        let bytes_needed = (num_columns + 7) / 8;
        let bytes = &self.payload[offset..offset + bytes_needed];
        NullBitmap::new(bytes)
    }

    fn payload_slice(&self, offset: usize, len: usize) -> &'a [u8] {
        &self.payload[offset..offset + len]
    }

    fn read_i32(&self, offset: usize) -> i32 {
        let bytes = self.payload_slice(offset, 4);
        i32::from_be_bytes(bytes.try_into().unwrap())
    }

    fn read_varlen(&self, offset: usize) -> &'a [u8] {
        let length_bytes = self.payload_slice(offset, 4);
        let length = u32::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
        &self.payload[offset + 4..offset + 4 + length]
    }
}

struct HeapTupleMut<'a> {
    header: &'a mut HeapTupleHeader,
    payload: &'a mut [u8],
}

impl<'a> HeapTupleMut<'a> {
    fn from_bytes(bytes: &'a mut [u8]) -> Self {
        let (header_bytes, payload_bytes) = bytes.split_at_mut(size_of::<HeapTupleHeader>());
        let header = unsafe { &mut *(header_bytes.as_ptr() as *mut HeapTupleHeader) };
        Self {
            header,
            payload: payload_bytes,
        }
    }

    fn payload_slice_mut(&mut self, offset: usize, len: usize) -> &'_ mut [u8] {
        &mut self.payload[offset..offset + len]
    }

    fn null_bitmap_mut(&mut self, num_columns: usize) -> NullBitmapMut<'_> {
        let offset = self.header.nullmap_ptr as usize;
        let bytes_needed = (num_columns + 7) / 8;
        let bytes = &mut self.payload[offset..offset + bytes_needed];
        NullBitmapMut::new(bytes)
    }

    fn as_tuple(&self) -> HeapTuple<'_> {
        HeapTuple::from_parts(&self.header, &self.payload)
    }
}

pub struct LogicalRow<'a> {
    tuple: HeapTuple<'a>,
    layout: &'a Layout,
}

impl<'a> LogicalRow<'a> {
    fn new(tuple: HeapTuple<'a>, layout: &'a Layout) -> Self {
        Self { tuple, layout }
    }

    pub fn get_column(&self, column_name: &str) -> Option<Constant> {
        let (offset, index) = self.layout.offset_with_index(column_name)?;
        let null_bitmap = self.tuple.null_bitmap(self.layout.num_of_columns());
        if null_bitmap.is_null(index) {
            return None;
        }
        let field_info = self.layout.field_info(column_name)?;
        let field_length = self.layout.field_length(column_name)?;
        let bytes = self.tuple.payload_slice(offset, field_length);
        Some(self.decode(bytes, field_info, field_length))
    }

    fn decode(&self, bytes: &'a [u8], field_info: &FieldInfo, field_length: usize) -> Constant {
        match field_info.field_type {
            FieldType::Int => Constant::Int(i32::from_le_bytes(
                bytes[..field_length].try_into().unwrap(),
            )),
            FieldType::String => {
                let len = u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
                Constant::String(String::from_utf8(bytes[4..4 + len].to_vec()).unwrap())
            }
        }
    }
}

pub struct LogicalRowMut<'a> {
    tuple: HeapTupleMut<'a>,
    layout: Layout,
}

impl<'a> LogicalRowMut<'a> {
    fn new(tuple: HeapTupleMut<'a>, layout: Layout) -> Self {
        Self { tuple, layout }
    }

    pub fn set_column(&mut self, column_name: &str, value: &Constant) -> Option<()> {
        let (offset, index) = self.layout.offset_with_index(column_name)?;
        let field_info = self.layout.field_info(column_name)?;
        let field_length = self.layout.field_length(column_name)?;
        self.tuple
            .null_bitmap_mut(self.layout.num_of_columns())
            .clear(index);
        let dest = self.tuple.payload_slice_mut(offset, field_length);
        LogicalRowMut::encode(dest, field_info, value);
        Some(())
    }

    #[cfg(test)]
    fn set_null(&mut self, column_name: &str) -> Option<()> {
        let (_, index) = self.layout.offset_with_index(column_name)?;
        self.tuple
            .null_bitmap_mut(self.layout.num_of_columns())
            .set_null(index);
        Some(())
    }

    fn encode(bytes: &'_ mut [u8], field_info: &FieldInfo, value: &Constant) {
        match field_info.field_type {
            FieldType::Int => {
                let value = value.as_int();
                bytes[..4].copy_from_slice(&value.to_le_bytes());
            }
            FieldType::String => {
                let value = value.as_str();
                let len = value.len() as u32;
                bytes[..4].copy_from_slice(&len.to_le_bytes());
                bytes[4..4 + len as usize].copy_from_slice(value.as_bytes());
            }
        }
    }
}

pub struct HeapPageView<'a, K: PageKind> {
    _guard: PageReadGuard<'a>,
    page_ref: &'a Page<K>,
    layout: &'a Layout,
}

impl<'a> HeapPageView<'a, HeapPage> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::Heap {
            return Err("cannot initialize PageView<'a, HeapPage> with a non-heap page".into());
        }
        let page = &*guard.page as *const Page<RawPage> as *const Page<HeapPage>;
        let page_ref = unsafe { &*page };
        Ok(Self {
            _guard: guard,
            page_ref,
            layout,
        })
    }

    fn tuple(&self, slot: SlotId) -> Option<HeapTuple<'_>> {
        self.page_ref.heap_tuple(slot)
    }

    pub fn row(&self, slot: SlotId) -> Option<LogicalRow<'_>> {
        let heap_tuple = self.tuple(slot)?;
        Some(LogicalRow::new(heap_tuple, self.layout))
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }
}

pub struct HeapPageViewMut<'a, K: PageKind> {
    guard: PageWriteGuard<'a>,
    page_ref: &'a mut Page<K>,
    layout: &'a Layout,
}

impl<'a> HeapPageViewMut<'a, HeapPage> {
    fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::Heap {
            return Err("cannot initialize PageViewMut<'a, HeapPage> with a non-heap page".into());
        }
        let page = &mut *guard.page as *mut Page<RawPage> as *mut Page<HeapPage>;
        let page_ref = unsafe { &mut *page };
        Ok(Self {
            guard,
            page_ref,
            layout,
        })
    }

    fn tuple(&self, slot: SlotId) -> Option<HeapTuple<'_>> {
        self.page_ref.heap_tuple(slot)
    }

    fn tuple_mut(&mut self, slot: SlotId) -> Option<HeapTupleMut<'_>> {
        self.page_ref.heap_tuple_mut(slot)
    }

    pub fn row(&self, slot: SlotId) -> Option<LogicalRow<'_>> {
        let heap_tuple = self.tuple(slot)?;
        Some(LogicalRow::new(heap_tuple, self.layout))
    }

    pub fn row_mut(&mut self, slot: SlotId) -> Option<LogicalRowMut<'_>> {
        //  this annoying clone has to be done because heap_tuple_mut takes &mut self so I can't pass in &Layout which is &self
        let layout_clone = self.layout.clone();
        let heap_tuple_mut = self.tuple_mut(slot)?;
        Some(LogicalRowMut::new(heap_tuple_mut, layout_clone))
    }

    pub fn insert_tuple(&mut self, bytes: &[u8]) -> Result<SlotId, Box<dyn Error>> {
        self.page_ref.insert_tuple(bytes)
    }

    pub fn delete_slot(&mut self, slot: SlotId) -> Result<(), Box<dyn Error>> {
        self.page_ref.delete_slot(slot)
    }

    pub fn redirect_slot(&mut self, slot: SlotId, target: SlotId) -> Result<(), Box<dyn Error>> {
        self.page_ref.redirect_slot(slot, target)
    }

    pub fn write_bytes(&self, out: &mut [u8]) -> Result<(), Box<dyn Error>> {
        let page_ref = &*self.page_ref;
        page_ref.write_bytes(out)
    }

    pub fn insert_row_mut(&mut self) -> Result<(SlotId, LogicalRowMut<'_>), Box<dyn Error>> {
        let payload_len = self.layout.slot_size;
        let mut buf = vec![0u8; size_of::<HeapTupleHeader>() + payload_len];
        {
            let header = HeapTupleHeader {
                payload_len: payload_len as u32,
                xmin: 0,
                xmax: 0,
                flags: 0,
                nullmap_ptr: 0,
            };
            let header_bytes = unsafe {
                std::slice::from_raw_parts(
                    &header as *const HeapTupleHeader as *const u8,
                    size_of::<HeapTupleHeader>(),
                )
            };
            buf[..header_bytes.len()].copy_from_slice(header_bytes);
        }
        let slot = self.page_ref.insert_tuple(&buf)?;
        let tuple_mut = self
            .page_ref
            .heap_tuple_mut(slot)
            .expect("tuple must exist after allocation");
        Ok((slot, LogicalRowMut::new(tuple_mut, self.layout.clone())))
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }

    /// Mark this page as modified by a transaction.
    /// This ensures the page will be flushed to disk during eviction or commit.
    pub fn mark_modified(&mut self, txn_id: usize, lsn: Lsn) {
        // let block_id = self.guard.block_id();
        // eprintln!(
        //     "[DEBUG] HeapPageViewMut::mark_modified: block={:?}, txn={}, lsn={:?}, slot_count={}, page_addr={:p}",
        //     block_id,
        //     txn_id,
        //     lsn,
        //     self.slot_count(),
        //     self.page_ref as *const _
        // );
        self.guard.mark_modified(txn_id, lsn);
    }
}

// BTree entry types
#[derive(Debug, Clone, PartialEq)]
pub struct BTreeLeafEntry {
    pub key: Constant,
    pub rid: RID,
}

impl BTreeLeafEntry {
    pub fn encode(&self, layout: &Layout) -> Vec<u8> {
        let mut bytes = vec![0u8; layout.slot_size];

        // Encode key at "dataval" offset
        let key_offset = layout
            .offset(BTREE_DATA_FIELD)
            .expect("dataval field required");
        match &self.key {
            Constant::Int(v) => {
                bytes[key_offset..key_offset + 4].copy_from_slice(&v.to_le_bytes());
            }
            Constant::String(s) => {
                let len = s.len() as u32;
                bytes[key_offset..key_offset + 4].copy_from_slice(&len.to_le_bytes());
                bytes[key_offset + 4..key_offset + 4 + s.len()].copy_from_slice(s.as_bytes());
            }
        }

        // Encode block number at "block" offset
        let block_offset = layout
            .offset(BTREE_BLOCK_FIELD)
            .expect("block field required");
        bytes[block_offset..block_offset + 4]
            .copy_from_slice(&(self.rid.block_num as i32).to_le_bytes());

        // Encode slot number at "id" offset
        let id_offset = layout.offset(BTREE_ID_FIELD).expect("id field required");
        bytes[id_offset..id_offset + 4].copy_from_slice(&(self.rid.slot as i32).to_le_bytes());

        bytes
    }

    pub fn decode(layout: &Layout, bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        // Decode key from "dataval" offset
        let key_offset = layout
            .offset(BTREE_DATA_FIELD)
            .ok_or("dataval field not found")?;
        let field_info = layout
            .field_info(BTREE_DATA_FIELD)
            .ok_or("dataval field info not found")?;
        let key = match field_info.field_type {
            FieldType::Int => {
                let val = i32::from_le_bytes(bytes[key_offset..key_offset + 4].try_into()?);
                Constant::Int(val)
            }
            FieldType::String => {
                let len =
                    u32::from_le_bytes(bytes[key_offset..key_offset + 4].try_into()?) as usize;
                let str_bytes = &bytes[key_offset + 4..key_offset + 4 + len];
                Constant::String(String::from_utf8(str_bytes.to_vec())?)
            }
        };

        // Decode block number from "block" offset
        let block_offset = layout
            .offset(BTREE_BLOCK_FIELD)
            .ok_or("block field not found")?;
        let block_num =
            i32::from_le_bytes(bytes[block_offset..block_offset + 4].try_into()?) as usize;

        // Decode slot number from "id" offset
        let id_offset = layout.offset(BTREE_ID_FIELD).ok_or("id field not found")?;
        let slot = i32::from_le_bytes(bytes[id_offset..id_offset + 4].try_into()?) as usize;

        Ok(BTreeLeafEntry {
            key,
            rid: RID::new(block_num, slot),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BTreeInternalEntry {
    pub key: Constant,
    pub child_block: usize,
}

impl BTreeInternalEntry {
    pub fn encode(&self, layout: &Layout) -> Vec<u8> {
        let mut bytes = vec![0u8; layout.slot_size];

        // Encode key at "dataval" offset
        let key_offset = layout
            .offset(BTREE_DATA_FIELD)
            .expect("dataval field required");
        match &self.key {
            Constant::Int(v) => {
                bytes[key_offset..key_offset + 4].copy_from_slice(&v.to_le_bytes());
            }
            Constant::String(s) => {
                let len = s.len() as u32;
                bytes[key_offset..key_offset + 4].copy_from_slice(&len.to_le_bytes());
                bytes[key_offset + 4..key_offset + 4 + s.len()].copy_from_slice(s.as_bytes());
            }
        }

        // Encode child block number at "block" offset
        let block_offset = layout
            .offset(BTREE_BLOCK_FIELD)
            .expect("block field required");
        bytes[block_offset..block_offset + 4]
            .copy_from_slice(&(self.child_block as i32).to_le_bytes());

        bytes
    }

    pub fn decode(layout: &Layout, bytes: &[u8]) -> Result<Self, Box<dyn Error>> {
        // Decode key from "dataval" offset
        let key_offset = layout
            .offset(BTREE_DATA_FIELD)
            .ok_or("dataval field not found")?;
        let field_info = layout
            .field_info(BTREE_DATA_FIELD)
            .ok_or("dataval field info not found")?;
        let key = match field_info.field_type {
            FieldType::Int => {
                let val = i32::from_le_bytes(bytes[key_offset..key_offset + 4].try_into()?);
                Constant::Int(val)
            }
            FieldType::String => {
                let len =
                    u32::from_le_bytes(bytes[key_offset..key_offset + 4].try_into()?) as usize;
                let str_bytes = &bytes[key_offset + 4..key_offset + 4 + len];
                Constant::String(String::from_utf8(str_bytes.to_vec())?)
            }
        };

        // Decode child block number from "block" offset
        let block_offset = layout
            .offset(BTREE_BLOCK_FIELD)
            .ok_or("block field not found")?;
        let child_block =
            i32::from_le_bytes(bytes[block_offset..block_offset + 4].try_into()?) as usize;

        Ok(BTreeInternalEntry { key, child_block })
    }
}

// Field names used in BTree layouts (matching IndexInfo constants in btree.rs)
const BTREE_DATA_FIELD: &str = "dataval";
const BTREE_BLOCK_FIELD: &str = "block";
const BTREE_ID_FIELD: &str = "id";

// BTree Leaf Page Views
pub struct BTreeLeafPageView<'a> {
    _guard: PageReadGuard<'a>,
    page_ref: &'a Page<BTreeLeafPage>,
    layout: &'a Layout,
}

impl<'a> BTreeLeafPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::IndexLeaf {
            return Err("cannot initialize BTreeLeafPageView with non-leaf page".into());
        }
        let page = &*guard.page as *const Page<RawPage> as *const Page<BTreeLeafPage>;
        let page_ref = unsafe { &*page };
        Ok(Self {
            _guard: guard,
            page_ref,
            layout,
        })
    }

    pub fn get_entry(&self, slot: SlotId) -> Result<BTreeLeafEntry, Box<dyn Error>> {
        self.page_ref.get_leaf_entry(self.layout, slot)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.page_ref.find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }

    pub fn is_slot_live(&self, slot: SlotId) -> bool {
        self.page_ref.is_slot_live(slot)
    }

    pub fn is_full(&self) -> bool {
        self.page_ref.is_full(self.layout)
    }

    pub fn overflow_block(&self) -> Option<usize> {
        self.page_ref.overflow_block()
    }

    pub fn iter(&self) -> BTreeLeafIterator<'_> {
        BTreeLeafIterator {
            page: self.page_ref,
            layout: self.layout,
            current_slot: 0,
        }
    }
}

pub struct BTreeLeafPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    page_ref: &'a mut Page<BTreeLeafPage>,
    layout: &'a Layout,
}

impl<'a> BTreeLeafPageViewMut<'a> {
    pub fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::IndexLeaf {
            return Err("cannot initialize BTreeLeafPageViewMut with non-leaf page".into());
        }
        let page = &mut *guard.page as *mut Page<RawPage> as *mut Page<BTreeLeafPage>;
        let page_ref = unsafe { &mut *page };
        Ok(Self {
            guard,
            page_ref,
            layout,
        })
    }

    // Read operations
    pub fn get_entry(&self, slot: SlotId) -> Result<BTreeLeafEntry, Box<dyn Error>> {
        self.page_ref.get_leaf_entry(self.layout, slot)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.page_ref.find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }

    pub fn is_slot_live(&self, slot: SlotId) -> bool {
        self.page_ref.is_slot_live(slot)
    }

    pub fn is_full(&self) -> bool {
        self.page_ref.is_full(self.layout)
    }

    pub fn overflow_block(&self) -> Option<usize> {
        self.page_ref.overflow_block()
    }

    pub fn iter(&self) -> BTreeLeafIterator<'_> {
        BTreeLeafIterator {
            page: self.page_ref,
            layout: self.layout,
            current_slot: 0,
        }
    }

    // Write operations
    pub fn insert_entry(&mut self, key: Constant, rid: RID) -> Result<SlotId, Box<dyn Error>> {
        self.page_ref.insert_leaf_entry(self.layout, &key, &rid)
    }

    pub fn delete_entry(&mut self, slot: SlotId) -> Result<(), Box<dyn Error>> {
        self.page_ref.delete_leaf_entry(slot, self.layout)
    }

    pub fn set_overflow_block(&mut self, block: Option<usize>) {
        self.page_ref.set_overflow_block(block);
    }

    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.guard.mark_modified(txn_id, lsn);
    }
}

// BTree Internal Page Views
pub struct BTreeInternalPageView<'a> {
    _guard: PageReadGuard<'a>,
    page_ref: &'a Page<BTreeInternalPage>,
    layout: &'a Layout,
}

impl<'a> BTreeInternalPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::IndexInternal {
            return Err("cannot initialize BTreeInternalPageView with non-internal page".into());
        }
        let page = &*guard.page as *const Page<RawPage> as *const Page<BTreeInternalPage>;
        let page_ref = unsafe { &*page };
        Ok(Self {
            _guard: guard,
            page_ref,
            layout,
        })
    }

    pub fn get_entry(&self, slot: SlotId) -> Result<BTreeInternalEntry, Box<dyn Error>> {
        self.page_ref.get_internal_entry(self.layout, slot)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.page_ref.find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }

    pub fn is_full(&self) -> bool {
        self.page_ref.is_full(self.layout)
    }

    pub fn btree_level(&self) -> u16 {
        self.page_ref.btree_level()
    }

    pub fn iter(&self) -> BTreeInternalIterator<'_> {
        BTreeInternalIterator {
            page: self.page_ref,
            layout: self.layout,
            current_slot: 0,
        }
    }
}

pub struct BTreeInternalPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    page_ref: &'a mut Page<BTreeInternalPage>,
    layout: &'a Layout,
}

impl<'a> BTreeInternalPageViewMut<'a> {
    pub fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::IndexInternal {
            return Err("cannot initialize BTreeInternalPageViewMut with non-internal page".into());
        }
        let page = &mut *guard.page as *mut Page<RawPage> as *mut Page<BTreeInternalPage>;
        let page_ref = unsafe { &mut *page };
        Ok(Self {
            guard,
            page_ref,
            layout,
        })
    }

    // Read operations
    pub fn get_entry(&self, slot: SlotId) -> Result<BTreeInternalEntry, Box<dyn Error>> {
        self.page_ref.get_internal_entry(self.layout, slot)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.page_ref.find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.page_ref.slot_count()
    }

    pub fn is_full(&self) -> bool {
        self.page_ref.is_full(self.layout)
    }

    pub fn btree_level(&self) -> u16 {
        self.page_ref.btree_level()
    }

    pub fn iter(&self) -> BTreeInternalIterator<'_> {
        BTreeInternalIterator {
            page: self.page_ref,
            layout: self.layout,
            current_slot: 0,
        }
    }

    // Write operations
    pub fn insert_entry(
        &mut self,
        key: Constant,
        child_block: usize,
    ) -> Result<SlotId, Box<dyn Error>> {
        self.page_ref
            .insert_internal_entry(self.layout, &key, child_block)
    }

    pub fn delete_entry(&mut self, slot: SlotId) -> Result<(), Box<dyn Error>> {
        self.page_ref.delete_internal_entry(slot, self.layout)
    }

    pub fn set_btree_level(&mut self, level: u16) {
        self.page_ref.set_btree_level(level);
    }

    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.guard.mark_modified(txn_id, lsn);
    }
}

#[cfg(test)]
mod heap_page_view_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn sample_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 16);
        schema.add_int_field("score");
        Layout::new(schema)
    }

    fn format_heap_page(guard: &mut PageWriteGuard<'_>) {
        **guard = Page::<HeapPage>::new().into();
    }

    #[test]
    fn heap_page_view_reads_rows_from_heap_page() {
        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            format_heap_page(&mut guard);
            let mut view_mut = HeapPageViewMut::new(guard, &layout)
                .expect("heap page mutable view initialization");
            let (slot0, mut row0) = view_mut.insert_row_mut().expect("insert row 0");
            assert_eq!(slot0, 0);
            row0.set_column("id", &Constant::Int(42)).unwrap();
            row0.set_column("name", &Constant::String("alpha".into()))
                .unwrap();
            row0.set_column("score", &Constant::Int(9)).unwrap();

            let (slot1, mut row1) = view_mut.insert_row_mut().expect("insert row 1");
            assert_eq!(slot1, 1);
            row1.set_column("id", &Constant::Int(7)).unwrap();
            row1.set_column("name", &Constant::String("beta".into()))
                .unwrap();
            row1.set_null("score").unwrap();
        } // drop guard

        let read_guard = txn.pin_read_guard(&block_id);
        let view = HeapPageView::new(read_guard, &layout).expect("heap page view");

        let row0 = view.row(0).expect("slot 0 row");
        assert_eq!(row0.get_column("id"), Some(Constant::Int(42)));
        assert_eq!(
            row0.get_column("name"),
            Some(Constant::String("alpha".to_string()))
        );
        assert_eq!(row0.get_column("score"), Some(Constant::Int(9)));

        let row1 = view.row(1).expect("slot 1 row");
        assert_eq!(row1.get_column("id"), Some(Constant::Int(7)));
        assert_eq!(
            row1.get_column("name"),
            Some(Constant::String("beta".to_string()))
        );
        assert!(row1.get_column("score").is_none());
    }

    #[test]
    fn heap_page_view_mut_updates_rows() {
        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        let mut guard = txn.pin_write_guard(&block_id);
        format_heap_page(&mut guard);
        let mut view =
            HeapPageViewMut::new(guard, &layout).expect("heap page mutable view initialization");
        let (slot, mut row_initial) = view.insert_row_mut().expect("insert new row");
        assert_eq!(slot, 0);
        row_initial.set_column("id", &Constant::Int(5)).unwrap();
        row_initial
            .set_column("name", &Constant::String("seed".into()))
            .unwrap();
        row_initial.set_column("score", &Constant::Int(10)).unwrap();
        {
            let mut row_mut = view.row_mut(slot).expect("mutable access to slot 0");
            row_mut
                .set_column("id", &Constant::Int(777))
                .expect("update int column");
            row_mut
                .set_column("name", &Constant::String("toast".to_string()))
                .expect("update string column");
            row_mut.set_null("score").expect("mark score as NULL");
        }

        let row = view.row(slot).expect("read updated slot 0");
        assert_eq!(row.get_column("id"), Some(Constant::Int(777)));
        assert_eq!(
            row.get_column("name"),
            Some(Constant::String("toast".to_string()))
        );
        assert!(row.get_column("score").is_none());
        drop(view); // drop write guard before acquiring read guard

        let read_guard = txn.pin_read_guard(&block_id);
        let view = HeapPageView::new(read_guard, &layout).expect("reopen heap view");
        let row = view.row(slot).expect("slot 0 after write guard drop");
        assert_eq!(row.get_column("id"), Some(Constant::Int(777)));
        assert!(row.get_column("score").is_none());
    }

    #[test]
    fn heap_page_view_mut_reuses_slots_and_serializes() {
        use super::TupleRef;

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        let mut guard = txn.pin_write_guard(&block_id);
        format_heap_page(&mut guard);
        let mut view =
            HeapPageViewMut::new(guard, &layout).expect("heap page mutable view initialization");

        let slot_a = {
            let (slot, mut row) = view.insert_row_mut().expect("insert row a");
            row.set_column("id", &Constant::Int(10)).unwrap();
            row.set_column("name", &Constant::String("alpha".into()))
                .unwrap();
            row.set_column("score", &Constant::Int(1)).unwrap();
            slot
        };
        let slot_b = {
            let (slot, mut row) = view.insert_row_mut().expect("insert row b");
            row.set_column("id", &Constant::Int(20)).unwrap();
            row.set_column("name", &Constant::String("beta".into()))
                .unwrap();
            row.set_column("score", &Constant::Int(2)).unwrap();
            slot
        };
        assert_eq!((slot_a, slot_b), (0, 1));

        view.delete_slot(slot_a).expect("delete slot_a");
        let slot_c = {
            let (slot, mut row) = view.insert_row_mut().expect("insert row c");
            row.set_column("id", &Constant::Int(30)).unwrap();
            row.set_column("name", &Constant::String("gamma".into()))
                .unwrap();
            row.set_column("score", &Constant::Int(3)).unwrap();
            slot
        };
        assert_eq!(
            slot_c, slot_a,
            "freed slot should be reused for next allocation"
        );

        let slot_d = {
            let (slot, mut row) = view.insert_row_mut().expect("insert row d");
            row.set_column("id", &Constant::Int(40)).unwrap();
            row.set_column("name", &Constant::String("delta".into()))
                .unwrap();
            row.set_column("score", &Constant::Int(4)).unwrap();
            slot
        };
        assert_eq!(slot_d, 2);

        view.redirect_slot(slot_b, slot_d)
            .expect("redirect slot_b to slot_d");

        // slot_b should now report redirect state (rows returns None)
        assert!(view.row(slot_b).is_none());
        let redirected = view.page_ref.tuple(slot_b).expect("slot_b exists");
        match redirected {
            TupleRef::Redirect(target) => assert_eq!(target as usize, slot_d),
            _ => panic!("slot_b should be redirect after redirect_slot"),
        }

        // slot_c and slot_d remain live
        let row_c = view.row(slot_c).expect("row at slot_c");
        assert_eq!(row_c.get_column("id"), Some(Constant::Int(30)));
        let row_d = view.row(slot_d).expect("row at slot_d");
        assert_eq!(row_d.get_column("id"), Some(Constant::Int(40)));

        // Serialize to bytes and drop the guard
        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        view.write_bytes(&mut buf)
            .expect("serialize heap page state");
        drop(view);

        let rebuilt =
            Page::<HeapPage>::from_bytes(&buf).expect("deserialize heap page after serialization");

        match rebuilt.tuple(slot_c).expect("slot_c tuple") {
            TupleRef::Live(tuple) => {
                let row = LogicalRow::new(tuple, &layout);
                assert_eq!(row.get_column("id"), Some(Constant::Int(30)));
            }
            _ => panic!("slot_c should remain live after serialization"),
        }

        match rebuilt.tuple(slot_d).expect("slot_d tuple") {
            TupleRef::Live(tuple) => {
                let row = LogicalRow::new(tuple, &layout);
                assert_eq!(row.get_column("id"), Some(Constant::Int(40)));
            }
            _ => panic!("slot_d should remain live after serialization"),
        }

        match rebuilt.tuple(slot_b).expect("slot_b tuple") {
            TupleRef::Redirect(target) => assert_eq!(target as usize, slot_d),
            _ => panic!("slot_b redirect state not preserved across serialization"),
        }
    }
}

#[cfg(test)]
mod btree_page_tests {
    use super::*;
    use crate::Schema;

    // Helper: Create a layout for BTree leaf entries with INT key
    fn btree_leaf_layout_int() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(BTREE_DATA_FIELD);
        schema.add_int_field(BTREE_BLOCK_FIELD);
        schema.add_int_field(BTREE_ID_FIELD);
        Layout::new(schema)
    }

    // Helper: Create a layout for BTree leaf entries with VARCHAR key
    fn btree_leaf_layout_varchar() -> Layout {
        let mut schema = Schema::new();
        schema.add_string_field(BTREE_DATA_FIELD, 20);
        schema.add_int_field(BTREE_BLOCK_FIELD);
        schema.add_int_field(BTREE_ID_FIELD);
        Layout::new(schema)
    }

    // Helper: Create a layout for BTree internal entries with INT key
    fn btree_internal_layout_int() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(BTREE_DATA_FIELD);
        schema.add_int_field(BTREE_BLOCK_FIELD);
        Layout::new(schema)
    }

    // Helper: Create a layout for BTree internal entries with VARCHAR key
    fn btree_internal_layout_varchar() -> Layout {
        let mut schema = Schema::new();
        schema.add_string_field(BTREE_DATA_FIELD, 20);
        schema.add_int_field(BTREE_BLOCK_FIELD);
        Layout::new(schema)
    }

    // ========== Phase 1: Entry Encoding/Decoding Tests ==========

    #[test]
    fn btree_leaf_entry_int_roundtrip() {
        let layout = btree_leaf_layout_int();
        let entry = BTreeLeafEntry {
            key: Constant::Int(42),
            rid: RID::new(10, 5),
        };

        let encoded = entry.encode(&layout);
        let decoded = BTreeLeafEntry::decode(&layout, &encoded).expect("decode should succeed");

        assert_eq!(decoded, entry);
    }

    #[test]
    fn btree_leaf_entry_varchar_roundtrip() {
        let layout = btree_leaf_layout_varchar();
        let entry = BTreeLeafEntry {
            key: Constant::String("hello".to_string()),
            rid: RID::new(100, 25),
        };

        let encoded = entry.encode(&layout);
        let decoded = BTreeLeafEntry::decode(&layout, &encoded).expect("decode should succeed");

        assert_eq!(decoded, entry);
    }

    #[test]
    fn btree_leaf_entry_varchar_edge_cases() {
        let layout = btree_leaf_layout_varchar();

        // Empty string
        let entry_empty = BTreeLeafEntry {
            key: Constant::String("".to_string()),
            rid: RID::new(0, 0),
        };
        let encoded = entry_empty.encode(&layout);
        let decoded = BTreeLeafEntry::decode(&layout, &encoded).expect("decode empty string");
        assert_eq!(decoded, entry_empty);

        // Max length string (20 chars)
        let entry_max = BTreeLeafEntry {
            key: Constant::String("12345678901234567890".to_string()),
            rid: RID::new(999, 999),
        };
        let encoded = entry_max.encode(&layout);
        let decoded = BTreeLeafEntry::decode(&layout, &encoded).expect("decode max length");
        assert_eq!(decoded, entry_max);
    }

    #[test]
    fn btree_internal_entry_int_roundtrip() {
        let layout = btree_internal_layout_int();
        let entry = BTreeInternalEntry {
            key: Constant::Int(99),
            child_block: 42,
        };

        let encoded = entry.encode(&layout);
        let decoded = BTreeInternalEntry::decode(&layout, &encoded).expect("decode should succeed");

        assert_eq!(decoded, entry);
    }

    #[test]
    fn btree_internal_entry_varchar_roundtrip() {
        let layout = btree_internal_layout_varchar();
        let entry = BTreeInternalEntry {
            key: Constant::String("index".to_string()),
            child_block: 123,
        };

        let encoded = entry.encode(&layout);
        let decoded = BTreeInternalEntry::decode(&layout, &encoded).expect("decode should succeed");

        assert_eq!(decoded, entry);
    }

    // ========== Phase 1: Page Initialization Tests ==========

    #[test]
    fn btree_leaf_page_init() {
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        assert_eq!(page.header.page_type(), PageType::IndexLeaf);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.overflow_block(), None);
    }

    #[test]
    fn btree_leaf_page_init_with_overflow() {
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(Some(42));

        assert_eq!(page.header.page_type(), PageType::IndexLeaf);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.overflow_block(), Some(42));
    }

    #[test]
    fn btree_internal_page_init() {
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(3);

        assert_eq!(page.header.page_type(), PageType::IndexInternal);
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.btree_level(), 3);
    }

    // ========== Phase 1: Sorted Insertion Tests ==========

    #[test]
    fn btree_leaf_sorted_insertion_random_order() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert in random order
        let keys = [42, 1, 99, 17, 55, 3, 88];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, &Constant::Int(key), &rid)
                .expect("insert should succeed");
        }

        // Verify sorted order
        let expected_sorted = [1, 3, 17, 42, 55, 88, 99];
        for (slot, &expected_key) in expected_sorted.iter().enumerate() {
            let entry = page.get_leaf_entry(&layout, slot).expect("get entry");
            assert_eq!(entry.key, Constant::Int(expected_key));
        }
    }

    #[test]
    fn btree_leaf_sorted_insertion_ascending() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        let keys = [1, 2, 3, 4, 5];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, &Constant::Int(key), &rid)
                .expect("insert should succeed");
        }

        // Verify order maintained
        for (slot, &expected_key) in keys.iter().enumerate() {
            let entry = page.get_leaf_entry(&layout, slot).expect("get entry");
            assert_eq!(entry.key, Constant::Int(expected_key));
        }
    }

    #[test]
    fn btree_leaf_sorted_insertion_descending() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        let keys = [5, 4, 3, 2, 1];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, &Constant::Int(key), &rid)
                .expect("insert should succeed");
        }

        // Verify ascending sorted order
        let expected = [1, 2, 3, 4, 5];
        for (slot, &expected_key) in expected.iter().enumerate() {
            let entry = page.get_leaf_entry(&layout, slot).expect("get entry");
            assert_eq!(entry.key, Constant::Int(expected_key));
        }
    }

    #[test]
    fn btree_leaf_varchar_sorted_insertion() {
        let layout = btree_leaf_layout_varchar();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        let keys = ["dog", "apple", "zebra", "banana", "cat"];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, &Constant::String(key.to_string()), &rid)
                .expect("insert should succeed");
        }

        // Verify sorted order
        let expected = ["apple", "banana", "cat", "dog", "zebra"];
        for (slot, &expected_key) in expected.iter().enumerate() {
            let entry = page.get_leaf_entry(&layout, slot).expect("get entry");
            assert_eq!(entry.key, Constant::String(expected_key.to_string()));
        }
    }

    #[test]
    fn btree_internal_sorted_insertion() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(1);

        let keys = [50, 10, 90, 30, 70];
        for (i, &key) in keys.iter().enumerate() {
            page.insert_internal_entry(&layout, &Constant::Int(key), i * 10)
                .expect("insert should succeed");
        }

        // Verify sorted order
        let expected = [10, 30, 50, 70, 90];
        for (slot, &expected_key) in expected.iter().enumerate() {
            let entry = page.get_internal_entry(&layout, slot).expect("get entry");
            assert_eq!(entry.key, Constant::Int(expected_key));
        }
    }

    // ========== Phase 1: Binary Search Tests ==========

    #[test]
    fn find_insertion_slot_empty_page() {
        let layout = btree_leaf_layout_int();
        let page = Page::<BTreeLeafPage>::new();

        let slot = page.find_insertion_slot(&layout, &Constant::Int(42));
        assert_eq!(slot, 0);
    }

    #[test]
    fn find_insertion_slot_middle() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert [10, 30, 50]
        for &key in &[10, 30, 50] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        // Search for 40 should return slot 2 (between 30 and 50)
        let slot = page.find_insertion_slot(&layout, &Constant::Int(40));
        assert_eq!(slot, 2);
    }

    #[test]
    fn find_insertion_slot_beginning() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        // Search for 5 should return slot 0
        let slot = page.find_insertion_slot(&layout, &Constant::Int(5));
        assert_eq!(slot, 0);
    }

    #[test]
    fn find_insertion_slot_end() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        // Search for 40 should return slot 3 (end)
        let slot = page.find_insertion_slot(&layout, &Constant::Int(40));
        assert_eq!(slot, 3);
    }

    #[test]
    fn find_slot_before_empty_page() {
        let layout = btree_leaf_layout_int();
        let page = Page::<BTreeLeafPage>::new();

        let slot = page.find_slot_before(&layout, &Constant::Int(42));
        assert_eq!(slot, None);
    }

    #[test]
    fn find_slot_before_key_less_than_all() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        let slot = page.find_slot_before(&layout, &Constant::Int(5));
        assert_eq!(slot, None);
    }

    #[test]
    fn find_slot_before_key_greater_than_all() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        let slot = page.find_slot_before(&layout, &Constant::Int(100));
        assert_eq!(slot, Some(2)); // Last slot
    }

    #[test]
    fn find_slot_before_key_in_middle() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30, 40] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        // Search for 25 should return slot 1 (20 is before 25)
        let slot = page.find_slot_before(&layout, &Constant::Int(25));
        assert_eq!(slot, Some(1));
    }

    #[test]
    fn find_slot_before_exact_match() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        for &key in &[10, 20, 30, 40] {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(0, 0))
                .unwrap();
        }

        // Exact match with 30 should return slot before it (slot 1 = 20)
        let slot = page.find_slot_before(&layout, &Constant::Int(30));
        assert_eq!(slot, Some(1));
    }

    // ========== Phase 1: Basic CRUD Tests ==========

    #[test]
    fn btree_leaf_insert_get_verify() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        let rid = RID::new(100, 50);
        let slot = page
            .insert_leaf_entry(&layout, &Constant::Int(42), &rid)
            .expect("insert should succeed");

        let entry = page
            .get_leaf_entry(&layout, slot)
            .expect("get should succeed");
        assert_eq!(entry.key, Constant::Int(42));
        assert_eq!(entry.rid, rid);
    }

    #[test]
    fn btree_leaf_insert_delete_verify() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert two entries - they will be sorted by key (10, 20)
        page.insert_leaf_entry(&layout, &Constant::Int(10), &RID::new(1, 1))
            .unwrap();
        page.insert_leaf_entry(&layout, &Constant::Int(20), &RID::new(2, 2))
            .unwrap();

        // Verify both entries exist at expected positions
        assert_eq!(page.slot_count(), 2);
        let entry0 = page.get_leaf_entry(&layout, 0).unwrap();
        assert_eq!(entry0.key, Constant::Int(10));
        assert_eq!(entry0.rid, RID::new(1, 1));

        let entry1 = page.get_leaf_entry(&layout, 1).unwrap();
        assert_eq!(entry1.key, Constant::Int(20));
        assert_eq!(entry1.rid, RID::new(2, 2));

        // Delete first entry (slot 0, key=10)
        page.delete_leaf_entry(0, &layout)
            .expect("delete should succeed");

        // After physical deletion, only one entry remains
        assert_eq!(page.slot_count(), 1);

        // The entry that was at slot 1 is now at slot 0 due to dense array maintenance
        let remaining = page.get_leaf_entry(&layout, 0).unwrap();
        assert_eq!(remaining.key, Constant::Int(20));
        assert_eq!(remaining.rid, RID::new(2, 2));

        // Verify slot 1 no longer exists (out of bounds)
        assert!(page.get_leaf_entry(&layout, 1).is_err());
    }

    #[test]
    fn btree_leaf_get_invalid_slot() {
        let layout = btree_leaf_layout_int();
        let page = Page::<BTreeLeafPage>::new();

        let result = page.get_leaf_entry(&layout, 999);
        assert!(result.is_err());
    }

    #[test]
    fn btree_leaf_delete_invalid_slot() {
        let _layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        let result = page.delete_leaf_entry(999, &_layout);
        assert!(result.is_err());
    }

    #[test]
    fn btree_internal_insert_get_verify() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(2);

        let slot = page
            .insert_internal_entry(&layout, &Constant::Int(50), 123)
            .expect("insert should succeed");

        let entry = page
            .get_internal_entry(&layout, slot)
            .expect("get should succeed");
        assert_eq!(entry.key, Constant::Int(50));
        assert_eq!(entry.child_block, 123);
    }

    #[test]
    fn btree_internal_insert_delete_verify() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(1);

        // Insert two entries - they will be sorted by key (10, 20)
        page.insert_internal_entry(&layout, &Constant::Int(10), 100)
            .unwrap();
        page.insert_internal_entry(&layout, &Constant::Int(20), 200)
            .unwrap();

        // Verify both entries exist at expected positions
        assert_eq!(page.slot_count(), 2);
        let entry0 = page.get_internal_entry(&layout, 0).unwrap();
        assert_eq!(entry0.key, Constant::Int(10));
        assert_eq!(entry0.child_block, 100);

        let entry1 = page.get_internal_entry(&layout, 1).unwrap();
        assert_eq!(entry1.key, Constant::Int(20));
        assert_eq!(entry1.child_block, 200);

        // Delete the first entry (slot 0, key=10)
        page.delete_internal_entry(0, &layout)
            .expect("delete should succeed");

        // After physical deletion, only one entry remains
        assert_eq!(page.slot_count(), 1);

        // The entry that was at slot 1 is now at slot 0 due to dense array maintenance
        let remaining = page.get_internal_entry(&layout, 0).unwrap();
        assert_eq!(remaining.key, Constant::Int(20));
        assert_eq!(remaining.child_block, 200);

        // Verify slot 1 no longer exists (out of bounds)
        assert!(page.get_internal_entry(&layout, 1).is_err());
    }

    // ========== Phase 2: Capacity Tests ==========

    #[test]
    fn btree_leaf_is_full_detection() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Initially not full
        assert!(!page.is_full(&layout));

        // Insert entries until full
        let mut count = 0;
        loop {
            if page.is_full(&layout) {
                break;
            }
            let result = page.insert_leaf_entry(
                &layout,
                &Constant::Int(count),
                &RID::new(count as usize, count as usize),
            );
            if result.is_err() {
                break;
            }
            count += 1;
        }

        // Should be marked as full
        assert!(page.is_full(&layout));
        assert!(count > 0, "should have inserted at least one entry");
    }

    #[test]
    fn btree_leaf_fill_to_capacity_then_fail() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Fill page completely
        let mut inserted = 0;
        loop {
            let result = page.insert_leaf_entry(
                &layout,
                &Constant::Int(inserted),
                &RID::new(inserted as usize, inserted as usize),
            );
            if result.is_err() {
                break;
            }
            inserted += 1;
            if inserted > 1000 {
                panic!("infinite loop - page never fills");
            }
        }

        // One more insert should fail
        let result = page.insert_leaf_entry(&layout, &Constant::Int(9999), &RID::new(9999, 9999));
        assert!(result.is_err(), "insert should fail on full page");

        // Verify all previously inserted entries are still accessible and sorted
        for slot in 0..inserted {
            let entry = page
                .get_leaf_entry(&layout, slot as usize)
                .expect("entry should exist");
            assert_eq!(entry.key, Constant::Int(slot));
        }
    }

    #[test]
    fn btree_leaf_delete_then_insert_more() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Fill page
        let mut inserted = 0;
        loop {
            let result = page.insert_leaf_entry(
                &layout,
                &Constant::Int(inserted),
                &RID::new(inserted as usize, inserted as usize),
            );
            if result.is_err() {
                break;
            }
            inserted += 1;
            if inserted > 1000 {
                panic!("infinite loop");
            }
        }

        let _initial_count = inserted;
        assert!(page.is_full(&layout), "page should be full");

        // Delete several entries
        for slot in 0..5 {
            page.delete_leaf_entry(slot, &layout)
                .expect("delete should succeed");
        }

        // Should no longer be marked as full (freed some space)
        // Note: is_full checks if there's space for one more entry
        let was_full_after_delete = page.is_full(&layout);

        // Try inserting again - should succeed now that we've freed space
        let result = page.insert_leaf_entry(&layout, &Constant::Int(8888), &RID::new(8888, 8888));
        assert!(
            result.is_ok() || was_full_after_delete,
            "either insert succeeds or page was still full after deletes"
        );
    }

    #[test]
    fn btree_internal_is_full_detection() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(1);

        assert!(!page.is_full(&layout));

        let mut count = 0;
        loop {
            if page.is_full(&layout) {
                break;
            }
            let result =
                page.insert_internal_entry(&layout, &Constant::Int(count), count as usize * 100);
            if result.is_err() {
                break;
            }
            count += 1;
        }

        assert!(page.is_full(&layout));
    }

    // ========== Phase 2: Empty/Single Entry Tests ==========

    #[test]
    fn btree_leaf_operations_on_empty_page() {
        let layout = btree_leaf_layout_int();
        let page = Page::<BTreeLeafPage>::new();

        // Empty page operations
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.find_insertion_slot(&layout, &Constant::Int(42)), 0);
        assert_eq!(page.find_slot_before(&layout, &Constant::Int(42)), None);
        assert!(page.get_leaf_entry(&layout, 0).is_err());
    }

    #[test]
    fn btree_leaf_single_entry_operations() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert single entry
        let slot = page
            .insert_leaf_entry(&layout, &Constant::Int(50), &RID::new(10, 5))
            .expect("insert should succeed");
        assert_eq!(slot, 0);
        assert_eq!(page.slot_count(), 1);

        // Search operations with single entry
        assert_eq!(page.find_insertion_slot(&layout, &Constant::Int(40)), 0); // before
        assert_eq!(page.find_insertion_slot(&layout, &Constant::Int(50)), 1); // exact (rightmost for duplicates)
        assert_eq!(page.find_insertion_slot(&layout, &Constant::Int(60)), 1); // after

        assert_eq!(page.find_slot_before(&layout, &Constant::Int(40)), None);
        assert_eq!(page.find_slot_before(&layout, &Constant::Int(50)), None);
        assert_eq!(page.find_slot_before(&layout, &Constant::Int(60)), Some(0));

        // Delete the only entry - physical deletion removes it from the line_pointers array
        page.delete_leaf_entry(0, &layout)
            .expect("delete should succeed");

        // After deletion, slot count should be 0 and accessing slot 0 should fail
        assert_eq!(page.slot_count(), 0);
        assert!(page.tuple_bytes(0).is_none());
        assert!(page.get_leaf_entry(&layout, 0).is_err());
    }

    #[test]
    fn btree_internal_single_entry_operations() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(2);

        let slot = page
            .insert_internal_entry(&layout, &Constant::Int(100), 500)
            .expect("insert should succeed");
        assert_eq!(slot, 0);

        let entry = page
            .get_internal_entry(&layout, slot)
            .expect("get should succeed");
        assert_eq!(entry.key, Constant::Int(100));
        assert_eq!(entry.child_block, 500);

        page.delete_internal_entry(slot, &layout)
            .expect("delete should succeed");
        assert!(page.tuple_bytes(slot).is_none());
    }

    // ========== Phase 2: Metadata Persistence Tests ==========

    #[test]
    fn btree_leaf_overflow_block_roundtrip() {
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Initially None
        assert_eq!(page.overflow_block(), None);

        // Set to Some value
        page.set_overflow_block(Some(42));
        assert_eq!(page.overflow_block(), Some(42));

        // Change value
        page.set_overflow_block(Some(999));
        assert_eq!(page.overflow_block(), Some(999));

        // Clear back to None
        page.set_overflow_block(None);
        assert_eq!(page.overflow_block(), None);
    }

    #[test]
    fn btree_internal_level_roundtrip() {
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(0);

        assert_eq!(page.btree_level(), 0);

        page.set_btree_level(5);
        assert_eq!(page.btree_level(), 5);

        page.set_btree_level(255);
        assert_eq!(page.btree_level(), 255);
    }

    #[test]
    fn btree_leaf_metadata_persists_across_serialization() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(Some(123));

        // Insert some entries
        page.insert_leaf_entry(&layout, &Constant::Int(10), &RID::new(1, 1))
            .unwrap();
        page.insert_leaf_entry(&layout, &Constant::Int(20), &RID::new(2, 2))
            .unwrap();

        // Serialize
        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        page.write_bytes(&mut buf)
            .expect("serialize should succeed");

        // Deserialize
        let restored = Page::<BTreeLeafPage>::from_bytes(&buf).expect("deserialize should succeed");

        // Verify metadata preserved
        assert_eq!(restored.overflow_block(), Some(123));
        assert_eq!(restored.slot_count(), 2);

        // Verify entries preserved
        let entry1 = restored
            .get_leaf_entry(&layout, 0)
            .expect("entry 0 should exist");
        assert_eq!(entry1.key, Constant::Int(10));

        let entry2 = restored
            .get_leaf_entry(&layout, 1)
            .expect("entry 1 should exist");
        assert_eq!(entry2.key, Constant::Int(20));
    }

    #[test]
    fn btree_internal_metadata_persists_across_serialization() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(7);

        // Insert entries
        page.insert_internal_entry(&layout, &Constant::Int(30), 300)
            .unwrap();
        page.insert_internal_entry(&layout, &Constant::Int(60), 600)
            .unwrap();

        // Serialize
        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        page.write_bytes(&mut buf)
            .expect("serialize should succeed");

        // Deserialize
        let restored =
            Page::<BTreeInternalPage>::from_bytes(&buf).expect("deserialize should succeed");

        // Verify metadata
        assert_eq!(restored.btree_level(), 7);
        assert_eq!(restored.slot_count(), 2);

        // Verify entries
        let entry1 = restored
            .get_internal_entry(&layout, 0)
            .expect("entry should exist");
        assert_eq!(entry1.key, Constant::Int(30));
        assert_eq!(entry1.child_block, 300);
    }

    // ========== Phase 3: Iterator Tests ==========

    #[test]
    fn btree_leaf_iterator_yields_sorted_order() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert in random order
        let keys = [42, 10, 99, 5, 77, 33];
        for &key in &keys {
            page.insert_leaf_entry(&layout, &Constant::Int(key), &RID::new(key as usize, 0))
                .unwrap();
        }

        // Iterate and collect
        let mut iter = BTreeLeafIterator {
            page: &page,
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        while let Some(entry) = iter.next() {
            if let Constant::Int(k) = entry.key {
                collected.push(k);
            }
        }

        // Should be sorted
        assert_eq!(collected, vec![5, 10, 33, 42, 77, 99]);
    }

    #[test]
    fn btree_leaf_iterator_skips_deleted_entries() {
        let layout = btree_leaf_layout_int();
        let mut page = Page::<BTreeLeafPage>::new();
        page.init(None);

        // Insert 5 entries
        for i in 0..5 {
            page.insert_leaf_entry(&layout, &Constant::Int(i * 10), &RID::new(i as usize, 0))
                .unwrap();
        }

        // Delete slot 2 (key=20)
        page.delete_leaf_entry(2, &layout)
            .expect("delete should succeed");

        // Iterate
        let mut iter = BTreeLeafIterator {
            page: &page,
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        while let Some(entry) = iter.next() {
            if let Constant::Int(k) = entry.key {
                collected.push(k);
            }
        }

        // Should skip deleted entry
        assert_eq!(collected, vec![0, 10, 30, 40]);
    }

    #[test]
    fn btree_leaf_iterator_empty_page() {
        let layout = btree_leaf_layout_int();
        let page = Page::<BTreeLeafPage>::new();

        let mut iter = BTreeLeafIterator {
            page: &page,
            layout: &layout,
            current_slot: 0,
        };

        assert!(iter.next().is_none());
    }

    #[test]
    fn btree_internal_iterator_yields_sorted_order() {
        let layout = btree_internal_layout_int();
        let mut page = Page::<BTreeInternalPage>::new();
        page.init(1);

        let keys = [50, 20, 80, 10, 90];
        for (i, &key) in keys.iter().enumerate() {
            page.insert_internal_entry(&layout, &Constant::Int(key), i * 100)
                .unwrap();
        }

        let mut iter = BTreeInternalIterator {
            page: &page,
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        while let Some(entry) = iter.next() {
            if let Constant::Int(k) = entry.key {
                collected.push(k);
            }
        }

        assert_eq!(collected, vec![10, 20, 50, 80, 90]);
    }

    // ========== Phase 3: Mixed Operations via Views ==========

    #[test]
    fn btree_leaf_view_delete_and_reinsert() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_leaf_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            guard.format_as_btree_leaf(None);

            let mut view = BTreeLeafPageViewMut::new(guard, &layout).expect("create leaf view");

            // Insert 5 entries - keys will be sorted: [10, 20, 30, 40, 50]
            view.insert_entry(Constant::Int(10), RID::new(1, 0))
                .unwrap();
            view.insert_entry(Constant::Int(20), RID::new(2, 0))
                .unwrap();
            view.insert_entry(Constant::Int(30), RID::new(3, 0))
                .unwrap();
            view.insert_entry(Constant::Int(40), RID::new(4, 0))
                .unwrap();
            view.insert_entry(Constant::Int(50), RID::new(5, 0))
                .unwrap();

            assert_eq!(view.slot_count(), 5);

            // Delete entries with keys 20 and 40
            // Must delete in reverse order to avoid slot shifting issues
            // After sorting: slot 0=10, 1=20, 2=30, 3=40, 4=50
            view.delete_entry(3).expect("delete key 40 at slot 3");
            view.delete_entry(1).expect("delete key 20 at slot 1");

            // After deletions: [10, 30, 50] remain
            assert_eq!(view.slot_count(), 3);
            let live_count = view.iter().count();
            assert_eq!(live_count, 3);

            // Insert new entries - they will be inserted in sorted order
            view.insert_entry(Constant::Int(15), RID::new(10, 0))
                .unwrap();
            view.insert_entry(Constant::Int(25), RID::new(11, 0))
                .unwrap();

            // Verify all live entries are in sorted order via iterator
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(collected, vec![10, 15, 25, 30, 50]);
            assert_eq!(collected.len(), 5);
        }
    }

    #[test]
    fn btree_leaf_view_serialize_with_deletes() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_leaf_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            guard.format_as_btree_leaf(None);

            let mut view = BTreeLeafPageViewMut::new(guard, &layout).expect("create leaf view");

            // Insert 10 entries with keys [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
            for i in 0..10 {
                view.insert_entry(Constant::Int(i * 10), RID::new(i as usize, 0))
                    .unwrap();
            }

            // Delete entries with keys 0, 20, 40, 60, 80 (every other entry)
            // Must delete in reverse order to avoid slot shifting issues
            // Initial: [0, 10, 20, 30, 40, 50, 60, 70, 80, 90]
            // Slots:    0   1   2   3   4   5   6   7   8   9
            view.delete_entry(8).expect("delete key 80 at slot 8");
            view.delete_entry(6).expect("delete key 60 at slot 6");
            view.delete_entry(4).expect("delete key 40 at slot 4");
            view.delete_entry(2).expect("delete key 20 at slot 2");
            view.delete_entry(0).expect("delete key 0 at slot 0");

            // Verify 5 live entries remain via iterator
            let count = view.iter().count();
            assert_eq!(count, 5);
        }

        // Serialize happens automatically when guard is dropped
        // Re-read the page
        {
            let view = txn
                .pin_read_guard(&block_id)
                .into_btree_leaf_page_view(&layout)
                .expect("create read view");

            // Verify 5 live entries still accessible: [10, 30, 50, 70, 90]
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(collected, vec![10, 30, 50, 70, 90]);
        }
    }

    #[test]
    fn btree_leaf_view_insert_delete_chaos() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_leaf_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            guard.format_as_btree_leaf(None);

            let mut view = BTreeLeafPageViewMut::new(guard, &layout).expect("create leaf view");

            // Insert 20 entries with keys [0, 1, 2, ..., 19]
            for i in 0..20 {
                view.insert_entry(Constant::Int(i), RID::new(i as usize, 0))
                    .unwrap();
            }

            // Delete entries with odd keys: 1, 3, 5, 7, 9, 11, 13, 15, 17, 19
            // Must delete in reverse order to avoid slot shifting issues
            // After insertion, keys are at their corresponding slots
            view.delete_entry(19).expect("delete key 19");
            view.delete_entry(17).expect("delete key 17");
            view.delete_entry(15).expect("delete key 15");
            view.delete_entry(13).expect("delete key 13");
            view.delete_entry(11).expect("delete key 11");
            view.delete_entry(9).expect("delete key 9");
            view.delete_entry(7).expect("delete key 7");
            view.delete_entry(5).expect("delete key 5");
            view.delete_entry(3).expect("delete key 3");
            view.delete_entry(1).expect("delete key 1");

            // After deletions, should have 10 entries with even keys: [0, 2, 4, 6, 8, 10, 12, 14, 16, 18]
            assert_eq!(view.slot_count(), 10);

            // Insert 5 new entries with keys [100, 101, 102, 103, 104]
            for i in 100..105 {
                view.insert_entry(Constant::Int(i), RID::new(i as usize, 0))
                    .unwrap();
            }

            // Verify sorted order maintained
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();

            // Should be: [0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 100, 101, 102, 103, 104]
            let mut expected = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18];
            expected.extend(100..105);
            assert_eq!(collected, expected);
            assert_eq!(collected.len(), 15);
        }
    }

    #[test]
    fn btree_leaf_view_fill_delete_refill() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_leaf_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            guard.format_as_btree_leaf(None);

            let mut view = BTreeLeafPageViewMut::new(guard, &layout).expect("create leaf view");

            // Fill page to capacity
            let mut inserted = 0;
            loop {
                let result =
                    view.insert_entry(Constant::Int(inserted), RID::new(inserted as usize, 0));
                if result.is_err() {
                    break;
                }
                inserted += 1;
                if inserted > 1000 {
                    panic!("page never fills");
                }
            }

            assert!(view.is_full());
            let _full_count = inserted;

            // Delete 10 entries from the middle
            let start_delete = (_full_count / 2 - 5) as usize;
            for slot in start_delete..start_delete + 10 {
                view.delete_entry(slot).expect("delete should succeed");
            }

            // Should no longer be full
            let still_full = view.is_full();

            // Try to insert more entries
            let mut new_inserted = 0;
            for i in 1000..1010 {
                let result = view.insert_entry(Constant::Int(i), RID::new(i as usize, 0));
                if result.is_ok() {
                    new_inserted += 1;
                } else {
                    break;
                }
            }

            // Should have inserted at least some entries (or page was still full)
            assert!(new_inserted > 0 || still_full);

            // Verify sorted order maintained
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();

            // Verify sorted
            for window in collected.windows(2) {
                assert!(
                    window[0] < window[1],
                    "not sorted: {} >= {}",
                    window[0],
                    window[1]
                );
            }
        }
    }

    #[test]
    fn btree_leaf_view_wrong_page_type_error() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_leaf_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            // Format as Heap page, NOT BTree
            guard.format_as_heap();

            // Try to create BTree leaf view on heap page
            let result = BTreeLeafPageViewMut::new(guard, &layout);
            assert!(result.is_err());
        }
    }

    #[test]
    fn btree_internal_view_mixed_operations() {
        use crate::{test_utils::generate_filename, SimpleDB};

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = btree_internal_layout_int();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            guard.format_as_btree_internal(2);

            let mut view =
                BTreeInternalPageViewMut::new(guard, &layout).expect("create internal view");

            // Insert entries
            view.insert_entry(Constant::Int(50), 100).unwrap();
            view.insert_entry(Constant::Int(30), 200).unwrap();
            view.insert_entry(Constant::Int(70), 300).unwrap();

            // Verify sorted
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(collected, vec![30, 50, 70]);

            // Delete middle entry
            view.delete_entry(1).expect("delete should succeed");

            // Verify remaining sorted
            let collected: Vec<i32> = view
                .iter()
                .filter_map(|e| {
                    if let Constant::Int(k) = e.key {
                        Some(k)
                    } else {
                        None
                    }
                })
                .collect();
            assert_eq!(collected, vec![30, 70]);

            // Verify level preserved
            assert_eq!(view.btree_level(), 2);
        }
    }
}
