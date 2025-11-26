use std::{error::Error, marker::PhantomData, mem::size_of, sync::Arc};

use crate::{BlockId, BufferHandle, Schema, Transaction};

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

struct HeapPage;

struct HeapAllocator<'a> {
    page: &'a mut Page<HeapPage>,
}

impl<'a> PageAllocator<'a> for HeapAllocator<'a> {
    type Output = SlotId;

    fn insert(&mut self, bytes: &[u8]) -> Result<Self::Output, Box<dyn Error>> {
        self.page.allocate_tuple(bytes)
    }
}

struct HeapIterator<'a> {
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
    fn new() -> Self {
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

enum TupleRef<'a> {
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
}

struct RecordPage {
    txn: Arc<Transaction>,
    handle: BufferHandle,
    layout: Layout,
}

impl RecordPage {
    fn new(txn: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = txn.pin(&block_id);
        Self {
            txn,
            handle,
            layout,
        }
    }
}

struct Layout {
    schema: Schema,
}
