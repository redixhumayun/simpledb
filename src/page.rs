use std::{
    error::Error,
    marker::PhantomData,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{BlockId, BufferFrame, BufferHandle, Constant, FieldInfo, FieldType, Layout};

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
enum LineState {
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

    fn with_offset(mut self, offset: u16) -> Self {
        self.set_offset(offset);
        self
    }

    fn with_length(mut self, length: u16) -> Self {
        self.set_length(length);
        self
    }

    fn with_state(mut self, state: LineState) -> Self {
        self.set_state(state);
        self
    }

    fn is_free(&self) -> bool {
        self.state() == LineState::Free
    }

    fn is_dead(&self) -> bool {
        self.state() == LineState::Dead
    }

    fn is_live(&self) -> bool {
        self.state() == LineState::Live
    }

    fn mark_free(&mut self) {
        self.set_state(LineState::Free);
    }

    fn mark_live(&mut self) {
        self.set_state(LineState::Live);
    }

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
    /// Whether this page kind uses the structured header/line-pointer layout.
    /// RawPage keeps the legacy “flat byte array” semantics for legacy callers like B-tree
    /// while they’re still being migrated to the new page API.
    /// TODO: Once B-tree pages have typed views, remove this escape hatch.
    const HAS_HEADER: bool = true;
    type Alloc<'a>: PageAllocator<'a>
    where
        Self: 'a;
    type Iter<'a>: Iterator
    where
        Self: 'a;

    fn allocator<'a>(page: &'a mut Page<Self>) -> Self::Alloc<'a>
    where
        Self: Sized;

    fn iterator<'a>(page: &'a mut Page<Self>) -> Self::Iter<'a>
    where
        Self: Sized;
}

type SlotId = usize;

pub struct HeapPage;
/// Raw page kind used to hold an on-disk image without enforcing a specific PageType.
/// Useful at the IO boundary (FileManager/LogManager) where the page kind is not yet known.
pub struct RawPage;

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

    type Alloc<'a> = HeapAllocator<'a>;

    type Iter<'a> = HeapIterator<'a>;

    fn allocator<'a>(page: &'a mut Page<Self>) -> Self::Alloc<'a>
    where
        Self: Sized,
    {
        HeapAllocator { page }
    }

    fn iterator<'a>(page: &'a mut Page<Self>) -> Self::Iter<'a>
    where
        Self: Sized,
    {
        HeapIterator {
            page,
            current_slot: 0,
            match_state: None,
        }
    }
}

impl PageKind for RawPage {
    const PAGE_TYPE: PageType = PageType::Free;
    const HAS_HEADER: bool = false;

    type Alloc<'a> = ();

    type Iter<'a> = std::iter::Empty<()>;

    fn allocator<'a>(_page: &'a mut Page<Self>) -> Self::Alloc<'a>
    where
        Self: Sized,
    {
        ()
    }

    fn iterator<'a>(_page: &'a mut Page<Self>) -> Self::Iter<'a>
    where
        Self: Sized,
    {
        std::iter::empty()
    }
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
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size (u16::MAX)".to_string())?;

        let (mut lower, upper) = self.header.free_bounds();
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

        if lower + needed > upper {
            return Err("insufficient free space".into());
        }

        let new_upper = upper - needed;
        self.record_space[new_upper as usize..(new_upper + needed) as usize].copy_from_slice(bytes);

        self.line_pointers[slot] = LinePtr::new(new_upper, needed, LineState::Live);
        self.header.set_free_bounds(lower, new_upper);
        self.header.set_free_ptr(new_upper as u32);
        self.header.set_slot_count(self.line_pointers.len() as u16);

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

    fn slot_count(&self) -> usize {
        self.line_pointers.len()
    }

    /// Serialize the page into a contiguous `PAGE_SIZE_BYTES` buffer.
    ///
    /// Layout matches `docs/record_management.md`:
    /// header (32B) + line pointer array (4B each, downward) + heap (upward).
    pub fn write_bytes(&self, out: &mut [u8]) -> Result<(), Box<dyn Error>> {
        if out.len() != PAGE_SIZE_BYTES as usize {
            return Err("output buffer must equal PAGE_SIZE_BYTES".into());
        }

        if !K::HAS_HEADER {
            out.copy_from_slice(&self.record_space);
            return Ok(());
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

        if !K::HAS_HEADER {
            return Ok(Self {
                header: PageHeader::new(K::PAGE_TYPE),
                line_pointers: Vec::new(),
                record_space: bytes.to_vec(),
                kind: PhantomData,
            });
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

        Ok(Self {
            header,
            line_pointers,
            record_space,
            kind: PhantomData,
        })
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
    ) -> Result<PageView<'a, HeapPage>, Box<dyn Error>> {
        PageView::new(self, layout)
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

    pub fn into_heap_view_mut(
        self,
        layout: &'a Layout,
    ) -> Result<PageViewMut<'a, HeapPage>, Box<dyn Error>> {
        PageViewMut::new(self, &layout)
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
    fn heap_iterator_filters_by_line_state() {
        let mut page = Page::<HeapPage>::new();
        let payload_a = vec![1u8];
        let payload_b = vec![2u8];
        let payload_c = vec![3u8];
        let grown_payload = vec![9u8, 9, 9, 9];

        let _slot_a = page.allocate_tuple(&heap_tuple_bytes(&payload_a)).unwrap();
        let slot_b = page.allocate_tuple(&heap_tuple_bytes(&payload_b)).unwrap();
        let slot_c = page.allocate_tuple(&heap_tuple_bytes(&payload_c)).unwrap();

        page.update_tuple(slot_b, &heap_tuple_bytes(&grown_payload))
            .unwrap();
        page.delete_tuple(slot_c).unwrap();

        let redirect_target = match page.tuple(slot_b).unwrap() {
            TupleRef::Redirect(target) => target,
            _ => panic!("expected redirect state for slot_b"),
        };

        // live iterator sees slot_a and the redirect target of slot_b
        {
            let mut iter = HeapPage::live_iterator(&mut page);
            let mut seen = Vec::new();
            while let Some(TupleRef::Live(tuple)) = iter.next() {
                seen.push(tuple.payload().to_vec());
            }
            assert_eq!(seen, vec![payload_a.clone(), grown_payload.clone()]);
        }

        // default iterator reports every state in slot order
        {
            let mut iter = HeapPage::iterator(&mut page);
            let mut states = Vec::new();
            while let Some(tref) = iter.next() {
                states.push(match tref {
                    TupleRef::Live(tuple) if tuple.payload() == payload_a => "live_a",
                    TupleRef::Redirect(target) if target == redirect_target => "redirect",
                    TupleRef::Free => "free",
                    TupleRef::Live(tuple) if tuple.payload() == grown_payload => "live_grown",
                    _ => "other",
                });
            }
            assert_eq!(states, vec!["live_a", "redirect", "free", "live_grown"]);
        }
    }

    #[test]
    fn heap_allocator_and_iterator_round_trip() {
        let mut page = Page::<HeapPage>::new();

        // Use the high-level allocator API (via PageKind)
        {
            let mut alloc = HeapPage::allocator(&mut page);
            let payload1 = vec![1u8, 2, 3];
            let payload2 = vec![4u8, 5, 6];

            let tuple1 = heap_tuple_bytes(&payload1);
            let tuple2 = heap_tuple_bytes(&payload2);

            let slot1 = alloc.insert(&tuple1).expect("first insert");
            let slot2 = alloc.insert(&tuple2).expect("second insert");

            assert_ne!(slot1, slot2, "slots should be distinct");
        } // alloc drops; page keeps the tuples

        // Use the high-level iterator API (via PageKind)
        let mut iter = HeapPage::iterator(&mut page);
        let mut seen = Vec::new();
        while let Some(tref) = iter.next() {
            if let TupleRef::Live(tuple) = tref {
                seen.push(tuple.payload().to_vec());
            }
        }

        // Order is insertion order for a fresh page
        assert_eq!(seen, vec![vec![1u8, 2, 3], vec![4u8, 5, 6]]);
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

struct HeapTuple<'a> {
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

    fn as_row(&self) -> LogicalRow<'_> {
        LogicalRow::new(self.tuple.as_tuple(), &self.layout)
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

pub struct PageView<'a, K: PageKind> {
    guard: PageReadGuard<'a>,
    page_ref: &'a Page<K>,
    layout: &'a Layout,
}

impl<'a> PageView<'a, HeapPage> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> Result<Self, Box<dyn Error>> {
        if guard.page.header.page_type() != PageType::Heap {
            return Err("cannot initialize PageView<'a, HeapPage> with a non-heap page".into());
        }
        let page = &*guard.page as *const Page<RawPage> as *const Page<HeapPage>;
        let page_ref = unsafe { &*page };
        Ok(Self {
            guard,
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

pub struct PageViewMut<'a, K: PageKind> {
    guard: PageWriteGuard<'a>,
    page_ref: &'a mut Page<K>,
    layout: &'a Layout,
}

impl<'a> PageViewMut<'a, HeapPage> {
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
}

#[cfg(test)]
mod page_view_tests {
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
    fn page_view_reads_rows_from_heap_page() {
        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        {
            let mut guard = txn.pin_write_guard(&block_id);
            format_heap_page(&mut guard);
            let mut view_mut =
                PageViewMut::new(guard, &layout).expect("heap page mutable view initialization");
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
        let view = PageView::new(read_guard, &layout).expect("heap page view");

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
    fn page_view_mut_updates_rows() {
        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        let mut guard = txn.pin_write_guard(&block_id);
        format_heap_page(&mut guard);
        let mut view =
            PageViewMut::new(guard, &layout).expect("heap page mutable view initialization");
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
        let view = PageView::new(read_guard, &layout).expect("reopen heap view");
        let row = view.row(slot).expect("slot 0 after write guard drop");
        assert_eq!(row.get_column("id"), Some(Constant::Int(777)));
        assert!(row.get_column("score").is_none());
    }

    #[test]
    fn page_view_mut_reuses_slots_and_serializes() {
        use super::TupleRef;

        let (db, _dir) = SimpleDB::new_for_test(2, 1000);
        let txn = db.new_tx();
        let filename = generate_filename();
        let block_id = txn.append(&filename);
        let layout = sample_layout();

        let mut guard = txn.pin_write_guard(&block_id);
        format_heap_page(&mut guard);
        let mut view =
            PageViewMut::new(guard, &layout).expect("heap page mutable view initialization");

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
