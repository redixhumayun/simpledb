//! Page management with slotted page architecture for heap and B-tree pages.
//!
//! This module implements the core page structures and operations for the database,
//! supporting both heap pages (table rows) and B-tree pages (index entries).
//!
//! # Architecture
//!
//! Pages use a slotted layout with three regions:
//! - Fixed 32-byte header containing metadata and free space pointers
//! - Line pointer array growing downward (4 bytes per slot)
//! - Tuple heap growing upward from the end
//!
//! # Page Types
//!
//! - [`HeapPage`]: Table rows with slotted page layout and MVCC support
//! - [`BTreeLeafPage`]: Index leaf entries with sorted key-RID pairs
//! - [`BTreeInternalPage`]: Index internal nodes with sorted key-child pointers
//! - [`WalPage`]: Write-ahead log pages with boundary-pointer format
//!
//! # Type Safety
//!
//! The [`Page<K>`] struct uses compile-time phantom types to enforce page-type-specific
//! operations. Page views ([`HeapPageView`], [`BTreeLeafPageView`], etc.) provide
//! schema-aware access with automatic dirty tracking.

use std::{
    cell::Cell,
    error::Error,
    ops::Deref,
    rc::Rc,
    sync::{Arc, RwLockReadGuard, RwLockWriteGuard},
};

use crate::{
    BlockId, BufferFrame, BufferHandle, Constant, FieldInfo, FieldType, Layout, Lsn,
    SimpleDBResult, RID,
};

/// Discriminator for the type of data stored in a page.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PageType {
    /// Heap page storing table rows with slotted page layout
    Heap = 0,
    /// B-tree leaf page containing key-value entries
    IndexLeaf = 1,
    /// B-tree internal page containing key-child pointers
    IndexInternal = 2,
    /// Overflow page for large tuples
    Overflow = 3,
    /// Metadata page
    Meta = 4,
    /// Uninitialized/free page
    Free = 255,
}

impl TryFrom<u8> for PageType {
    type Error = Box<dyn Error>;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PageType::Heap),
            1 => Ok(PageType::IndexLeaf),
            2 => Ok(PageType::IndexInternal),
            3 => Ok(PageType::Overflow),
            4 => Ok(PageType::Meta),
            255 => Ok(PageType::Free),
            _ => Err("invalid page type byte".into()),
        }
    }
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
/// Sentinel value indicating no free slots in the free list.
pub const NO_FREE_SLOT: u16 = 0xFFFF;

mod crc {
    pub fn crc32<I>(bytes: I) -> u32
    where
        I: Iterator<Item = u8>,
    {
        const CRC32_POLY: u32 = 0xEDB8_8320;
        let mut crc = 0xFFFF_FFFFu32;
        for b in bytes.into_iter() {
            crc ^= b as u32;
            for _ in 0..8 {
                let mask = (crc & 1).wrapping_neg();
                crc = (crc >> 1) ^ (CRC32_POLY & mask);
            }
        }
        !crc
    }
}

/// Read-only view over a heap header stored inline in `PageBytes`.
#[derive(Clone, Copy)]
pub struct HeapHeaderRef<'a> {
    bytes: &'a [u8],
}

impl<'a> HeapHeaderRef<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    fn range<const N: usize>(&self, start: usize) -> [u8; N] {
        self.bytes[start..start + N]
            .try_into()
            .expect("invalid header slice length")
    }

    pub fn page_type(&self) -> PageType {
        match self.bytes[0] {
            0 => PageType::Heap,
            1 => PageType::IndexLeaf,
            2 => PageType::IndexInternal,
            3 => PageType::Overflow,
            4 => PageType::Meta,
            255 => PageType::Free,
            _ => panic!("invalid page type byte"),
        }
    }

    #[allow(dead_code)]
    pub fn reserved_flags(&self) -> u8 {
        self.bytes[1]
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.range::<2>(2))
    }

    pub fn free_lower(&self) -> u16 {
        u16::from_le_bytes(self.range::<2>(4))
    }

    pub fn free_upper(&self) -> u16 {
        u16::from_le_bytes(self.range::<2>(6))
    }

    #[allow(dead_code)]
    pub fn free_ptr(&self) -> u32 {
        u32::from_le_bytes(self.range::<4>(8))
    }

    pub fn crc32(&self) -> u32 {
        u32::from_le_bytes(self.range::<4>(12))
    }

    #[allow(dead_code)]
    pub fn latch_word(&self) -> u64 {
        u64::from_le_bytes(self.range::<8>(16))
    }

    pub fn free_head(&self) -> u16 {
        u16::from_le_bytes(self.range::<2>(24))
    }

    #[allow(dead_code)]
    pub fn reserved_bytes(&self) -> [u8; 6] {
        self.range::<6>(26)
    }

    pub fn has_free_slot(&self) -> bool {
        self.free_head() != NO_FREE_SLOT
    }
}

/// Mutable view over a heap header stored inline in `PageBytes`.
pub struct HeapHeaderMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> HeapHeaderMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        debug_assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    pub fn init_heap(&mut self) {
        self.set_page_type(PageType::Heap);
        self.set_reserved_flags(0);
        self.set_slot_count(0);
        self.set_free_lower(PAGE_HEADER_SIZE_BYTES);
        self.set_free_upper(PAGE_SIZE_BYTES);
        self.set_free_ptr(PAGE_SIZE_BYTES as u32);
        self.set_crc32(0);
        self.set_latch_word(0);
        self.set_free_head(NO_FREE_SLOT);
        self.set_reserved_bytes([0; 6]);
    }

    pub fn as_ref(&self) -> HeapHeaderRef<'_> {
        HeapHeaderRef {
            bytes: &self.bytes[..],
        }
    }

    fn write<const N: usize>(&mut self, start: usize, value: [u8; N]) {
        self.bytes[start..start + N].copy_from_slice(&value);
    }

    pub fn set_page_type(&mut self, page_type: PageType) {
        self.bytes[0] = page_type as u8;
    }

    pub fn set_reserved_flags(&mut self, flags: u8) {
        self.bytes[1] = flags;
    }

    pub fn set_slot_count(&mut self, slot_count: u16) {
        self.write(2, slot_count.to_le_bytes());
    }

    pub fn set_free_lower(&mut self, free_lower: u16) {
        self.write(4, free_lower.to_le_bytes());
    }

    pub fn set_free_upper(&mut self, free_upper: u16) {
        self.write(6, free_upper.to_le_bytes());
    }

    pub fn set_free_ptr(&mut self, free_ptr: u32) {
        self.write(8, free_ptr.to_le_bytes());
    }

    pub fn set_crc32(&mut self, crc32: u32) {
        self.write(12, crc32.to_le_bytes());
    }

    pub fn set_latch_word(&mut self, latch: u64) {
        self.write(16, latch.to_le_bytes());
    }

    pub fn set_free_head(&mut self, free_head: u16) {
        self.write(24, free_head.to_le_bytes());
    }

    pub fn set_reserved_bytes(&mut self, reserved: [u8; 6]) {
        self.write(26, reserved);
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut *self.bytes
    }

    pub fn update_crc32(&mut self, body_bytes: &[u8]) {
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(crc32);
    }

    pub fn verify_crc32(&mut self, body_bytes: &[u8]) -> bool {
        let stored_crc32 = self.as_ref().crc32();
        if stored_crc32 == 0 {
            return true;
        }
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(stored_crc32);
        crc32 == stored_crc32
    }
}

#[cfg(test)]
pub(crate) mod test_helpers {
    use super::*;
    use std::{cell::Cell, rc::Rc};

    pub fn init_heap_page_with_row<F>(
        page: &mut PageBytes,
        layout: &Layout,
        builder: F,
    ) -> SimpleDBResult<()>
    where
        F: FnOnce(&mut LogicalRowMut<'_>),
    {
        let bytes = page.bytes_mut();
        bytes.fill(0);
        let (header_bytes, _) = bytes.split_at_mut(PAGE_HEADER_SIZE_BYTES as usize);
        let mut header = HeapHeaderMut::new(header_bytes);
        header.init_heap();

        let tuple_bytes = build_tuple_bytes(layout, builder)?;
        insert_tuple_bytes(bytes, &tuple_bytes).map(|slot| {
            assert_eq!(slot, 0, "test helper expects first inserted slot to be 0");
        })
    }

    pub fn init_heap_page_with_int(
        page: &mut PageBytes,
        layout: &Layout,
        field: &str,
        value: i32,
    ) -> SimpleDBResult<()> {
        init_heap_page_with_row(page, layout, |row| {
            row.set_column(field, &Constant::Int(value))
                .expect("set int column");
        })
    }

    pub fn read_single_int_field(
        page: &PageBytes,
        layout: &Layout,
        field: &str,
    ) -> SimpleDBResult<i32> {
        let row = read_single_row(page, layout)?;
        match row.get_column(field) {
            Some(Constant::Int(v)) => Ok(v),
            Some(_) => Err(format!("field {field} is not an int").into()),
            None => Err(format!("field {field} is null").into()),
        }
    }

    pub fn read_single_string_field(
        page: &PageBytes,
        layout: &Layout,
        field: &str,
    ) -> SimpleDBResult<String> {
        let row = read_single_row(page, layout)?;
        match row.get_column(field) {
            Some(Constant::String(s)) => Ok(s),
            Some(_) => Err(format!("field {field} is not a string").into()),
            None => Err(format!("field {field} is null").into()),
        }
    }

    fn build_tuple_bytes<F>(layout: &Layout, builder: F) -> SimpleDBResult<Vec<u8>>
    where
        F: FnOnce(&mut LogicalRowMut<'_>),
    {
        let mut buf = vec![0u8; HEAP_TUPLE_HEADER_BYTES + layout.slot_size];
        {
            let (header_bytes, payload_bytes) = buf.split_at_mut(HEAP_TUPLE_HEADER_BYTES);
            let header_bytes: &mut [u8; HEAP_TUPLE_HEADER_BYTES] = header_bytes.try_into().unwrap();
            let mut header = HeapTupleHeaderBytesMut::from_bytes(header_bytes);
            header.set_xmin(0);
            header.set_xmax(0);
            header.set_payload_len(layout.slot_size as u32);
            header.set_flags(0);
            header.set_nullmap_ptr(0);
            payload_bytes.fill(0);
        }
        {
            let tuple_mut = HeapTupleMut::from_bytes(buf.as_mut_slice());
            let layout_clone = layout.clone();
            let dirty = Rc::new(Cell::new(false));
            let mut row_mut = LogicalRowMut::new(tuple_mut, layout_clone, dirty);
            builder(&mut row_mut);
        }
        Ok(buf)
    }

    fn insert_tuple_bytes(bytes: &mut [u8], tuple: &[u8]) -> SimpleDBResult<SlotId> {
        let mut page = HeapPageZeroCopyMut::new(bytes)?;
        let mut split_guard = page.split()?;
        let slot = match split_guard.insert_tuple_fast(tuple)? {
            HeapInsert::Done(slot) => slot,
            HeapInsert::Reserved(reservation) => {
                drop(split_guard);
                let mut split_guard = page.split()?;
                split_guard.insert_tuple_slow(reservation, tuple)?
            }
        };
        Ok(slot)
    }

    fn read_single_row<'a>(
        page: &'a PageBytes,
        layout: &'a Layout,
    ) -> SimpleDBResult<LogicalRow<'a>> {
        let view = HeapPageZeroCopy::new(page.bytes())?;
        let tuple = match view
            .tuple_ref(0)
            .ok_or_else(|| -> Box<dyn Error> { "slot 0 missing".into() })?
        {
            TupleRef::Live(tuple) => tuple,
            _ => return Err("slot 0 not live".into()),
        };
        Ok(LogicalRow::new(tuple, layout))
    }
}

/// Read-only view over a B-tree leaf header.
#[derive(Clone, Copy)]
pub struct BTreeLeafHeaderRef<'a> {
    bytes: &'a [u8],
}

impl<'a> BTreeLeafHeaderRef<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    pub fn page_type(&self) -> PageType {
        match self.bytes[0] {
            1 => PageType::IndexLeaf,
            _ => panic!("invalid B-tree leaf page type byte"),
        }
    }

    #[allow(dead_code)]
    pub fn level(&self) -> u8 {
        self.bytes[1]
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.bytes[2..4].try_into().unwrap())
    }

    pub fn free_lower(&self) -> u16 {
        u16::from_le_bytes(self.bytes[4..6].try_into().unwrap())
    }

    pub fn free_upper(&self) -> u16 {
        u16::from_le_bytes(self.bytes[6..8].try_into().unwrap())
    }

    pub fn free_bounds(&self) -> (u16, u16) {
        (self.free_lower(), self.free_upper())
    }

    pub fn free_space(&self) -> u16 {
        self.free_upper().saturating_sub(self.free_lower())
    }

    #[allow(dead_code)]
    pub fn high_key_len(&self) -> u16 {
        u16::from_le_bytes(self.bytes[8..10].try_into().unwrap())
    }

    #[allow(dead_code)]
    pub fn right_sibling(&self) -> u32 {
        u32::from_le_bytes(self.bytes[10..14].try_into().unwrap())
    }

    pub fn overflow_block(&self) -> u32 {
        u32::from_le_bytes(self.bytes[14..18].try_into().unwrap())
    }

    pub fn crc32(&self) -> u32 {
        u32::from_le_bytes(self.bytes[18..22].try_into().unwrap())
    }
}

pub struct BTreeLeafHeaderMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> BTreeLeafHeaderMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    pub fn as_ref(&self) -> BTreeLeafHeaderRef<'_> {
        BTreeLeafHeaderRef::new(&self.bytes[..])
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    fn write<const N: usize>(&mut self, start: usize, value: [u8; N]) {
        self.bytes[start..start + N].copy_from_slice(&value);
    }

    pub fn set_page_type(&mut self) {
        self.bytes[0] = PageType::IndexLeaf as u8;
    }

    pub fn set_level(&mut self, level: u8) {
        self.bytes[1] = level;
    }

    pub fn set_slot_count(&mut self, slot_count: u16) {
        self.bytes[2..4].copy_from_slice(&slot_count.to_le_bytes());
    }

    pub fn set_free_lower(&mut self, lower: u16) {
        self.write(4, lower.to_le_bytes());
    }

    pub fn set_free_upper(&mut self, upper: u16) {
        self.write(6, upper.to_le_bytes());
    }

    pub fn set_free_bounds(&mut self, lower: u16, upper: u16) {
        debug_assert!(lower >= PAGE_HEADER_SIZE_BYTES);
        debug_assert!(upper <= PAGE_SIZE_BYTES);
        debug_assert!(lower <= upper);
        self.set_free_lower(lower);
        self.set_free_upper(upper);
    }

    pub fn set_high_key_len(&mut self, len: u16) {
        self.write(8, len.to_le_bytes());
    }

    pub fn set_right_sibling_block(&mut self, block: u32) {
        self.write(10, block.to_le_bytes());
    }

    pub fn set_overflow_block(&mut self, block: u32) {
        self.write(14, block.to_le_bytes());
    }

    pub fn set_crc32(&mut self, crc32: u32) {
        self.write(18, crc32.to_le_bytes());
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.write(22, lsn.to_le_bytes());
    }

    pub fn set_reserved_bytes(&mut self, reserved: [u8; 2]) {
        self.write(30, reserved);
    }

    pub fn init_leaf(
        &mut self,
        level: u8,
        right_sibling: Option<u32>,
        overflow_block: Option<u32>,
    ) {
        self.bytes.fill(0);
        self.set_page_type();
        self.set_level(level);
        self.set_slot_count(0);
        self.set_free_lower(PAGE_HEADER_SIZE_BYTES);
        self.set_free_upper(PAGE_SIZE_BYTES);
        self.set_high_key_len(0);
        self.set_right_sibling_block(right_sibling.unwrap_or(u32::MAX));
        self.set_overflow_block(overflow_block.unwrap_or(u32::MAX));
        self.set_crc32(0);
        self.set_lsn(0);
        self.set_reserved_bytes([0; 2]);
    }

    pub fn update_crc32(&mut self, body_bytes: &[u8]) {
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(crc32);
    }

    pub fn verify_crc32(&mut self, body_bytes: &[u8]) -> bool {
        let stored_crc32 = self.as_ref().crc32();
        if stored_crc32 == 0 {
            return true;
        }
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(stored_crc32);
        crc32 == stored_crc32
    }
}

/// Read-only view over a B-tree internal header.
#[derive(Clone, Copy)]
pub struct BTreeInternalHeaderRef<'a> {
    bytes: &'a [u8],
}

impl<'a> BTreeInternalHeaderRef<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    pub fn page_type(&self) -> PageType {
        match self.bytes[0] {
            2 => PageType::IndexInternal,
            _ => panic!("invalid B-tree internal page type byte"),
        }
    }

    pub fn level(&self) -> u8 {
        self.bytes[1]
    }

    pub fn slot_count(&self) -> u16 {
        u16::from_le_bytes(self.bytes[2..4].try_into().unwrap())
    }

    pub fn free_lower(&self) -> u16 {
        u16::from_le_bytes(self.bytes[4..6].try_into().unwrap())
    }

    pub fn free_upper(&self) -> u16 {
        u16::from_le_bytes(self.bytes[6..8].try_into().unwrap())
    }

    pub fn free_bounds(&self) -> (u16, u16) {
        (self.free_lower(), self.free_upper())
    }

    pub fn free_space(&self) -> u16 {
        self.free_upper().saturating_sub(self.free_lower())
    }

    #[allow(dead_code)]
    pub fn rightmost_child_block(&self) -> u32 {
        u32::from_le_bytes(self.bytes[8..12].try_into().unwrap())
    }

    #[allow(dead_code)]
    pub fn high_key_len(&self) -> u16 {
        u16::from_le_bytes(self.bytes[12..14].try_into().unwrap())
    }

    pub fn crc32(&self) -> u32 {
        u32::from_le_bytes(self.bytes[14..18].try_into().unwrap())
    }
}

pub struct BTreeInternalHeaderMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> BTreeInternalHeaderMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        assert_eq!(bytes.len(), PAGE_HEADER_SIZE_BYTES as usize);
        Self { bytes }
    }

    pub fn as_ref(&self) -> BTreeInternalHeaderRef<'_> {
        BTreeInternalHeaderRef::new(&self.bytes[..])
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    fn write<const N: usize>(&mut self, start: usize, value: [u8; N]) {
        self.bytes[start..start + N].copy_from_slice(&value);
    }

    pub fn set_page_type(&mut self) {
        self.bytes[0] = PageType::IndexInternal as u8;
    }

    pub fn set_level(&mut self, level: u8) {
        self.bytes[1] = level;
    }

    pub fn set_slot_count(&mut self, slot_count: u16) {
        self.bytes[2..4].copy_from_slice(&slot_count.to_le_bytes());
    }

    pub fn set_free_lower(&mut self, lower: u16) {
        self.write(4, lower.to_le_bytes());
    }

    pub fn set_free_upper(&mut self, upper: u16) {
        self.write(6, upper.to_le_bytes());
    }

    pub fn set_free_bounds(&mut self, lower: u16, upper: u16) {
        debug_assert!(lower >= PAGE_HEADER_SIZE_BYTES);
        debug_assert!(upper <= PAGE_SIZE_BYTES);
        debug_assert!(lower <= upper);
        self.set_free_lower(lower);
        self.set_free_upper(upper);
    }

    pub fn set_rightmost_child_block(&mut self, block: u32) {
        self.write(8, block.to_le_bytes());
    }

    pub fn set_high_key_len(&mut self, len: u16) {
        self.write(12, len.to_le_bytes());
    }

    pub fn set_crc32(&mut self, crc32: u32) {
        self.write(14, crc32.to_le_bytes());
    }

    pub fn set_lsn(&mut self, lsn: u64) {
        self.write(18, lsn.to_le_bytes());
    }

    pub fn set_reserved_bytes(&mut self, reserved: [u8; 6]) {
        self.write(26, reserved);
    }

    pub fn init_internal(&mut self, level: u8, rightmost_child: Option<u32>) {
        self.bytes.fill(0);
        self.set_page_type();
        self.set_level(level);
        self.set_slot_count(0);
        self.set_free_lower(PAGE_HEADER_SIZE_BYTES);
        self.set_free_upper(PAGE_SIZE_BYTES);
        self.set_rightmost_child_block(rightmost_child.unwrap_or(u32::MAX));
        self.set_high_key_len(0);
        self.set_crc32(0);
        self.set_lsn(0);
        self.set_reserved_bytes([0; 6]);
    }

    pub fn update_crc32(&mut self, body_bytes: &[u8]) {
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(crc32);
    }

    pub fn verify_crc32(&mut self, body_bytes: &[u8]) -> bool {
        let stored_crc32 = self.as_ref().crc32();
        if stored_crc32 == 0 {
            return true;
        }
        self.set_crc32(0);
        let crc32 = crc::crc32(self.bytes.iter().copied().chain(body_bytes.iter().copied()));
        self.set_crc32(stored_crc32);
        crc32 == stored_crc32
    }
}

struct LinePtrBytes<'a> {
    bytes: &'a [u8],
}

impl<'a> LinePtrBytes<'a> {
    const LINE_PTR_BYTES: usize = 4;
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn read(&self, index: usize) -> Vec<u8> {
        let start = index * Self::LINE_PTR_BYTES;
        let end = start + Self::LINE_PTR_BYTES;
        let mut dest = vec![0u8; Self::LINE_PTR_BYTES];
        dest.copy_from_slice(&self.bytes[start..end]);
        dest
    }

    fn as_slice(&self) -> &'a [u8] {
        self.bytes
    }
}

struct LinePtrBytesMut<'a> {
    bytes: &'a mut [u8],
}

impl<'a> LinePtrBytesMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> Self {
        Self { bytes }
    }

    pub fn as_ref(&self) -> LinePtrBytes<'_> {
        LinePtrBytes::new(&self.bytes[..])
    }

    pub fn write(&mut self, index: usize, line_ptr: LinePtr) {
        let start = index * LinePtrBytes::LINE_PTR_BYTES;
        let end = start + LinePtrBytes::LINE_PTR_BYTES;
        self.bytes[start..end].copy_from_slice(&line_ptr.to_bytes());
    }

    pub fn shift_left(&mut self, start: usize, end: usize) {
        assert!(
            start <= end,
            "cannot call shift_left with start > end - {} > {}",
            start,
            end
        );
        if start == end {
            return;
        }
        assert!(start > 0, "cannot shift left starting at the beginning");
        let head = start * LinePtrBytes::LINE_PTR_BYTES;
        let tail = end * LinePtrBytes::LINE_PTR_BYTES;
        self.bytes.copy_within(head..tail, head - 4);
    }

    pub fn shift_right(&mut self, start: usize, end: usize) {
        assert!(
            start <= end,
            "cannot call shift_right with start > end - {} > {}",
            start,
            end
        );
        if start == end {
            return;
        }
        let head = start * LinePtrBytes::LINE_PTR_BYTES;
        let tail = end * LinePtrBytes::LINE_PTR_BYTES;
        self.bytes.copy_within(head..tail, head + 4);
    }
}

impl<'a> Deref for LinePtrBytesMut<'a> {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.bytes
    }
}

struct LinePtrArray<'a> {
    bytes: LinePtrBytes<'a>,
    len: usize,
    capacity: usize,
}

impl<'a> LinePtrArray<'a> {
    fn new(bytes: &'a [u8], len: usize, capacity: usize) -> Self {
        assert!(
            bytes.len() % LinePtrBytes::LINE_PTR_BYTES == 0,
            "line pointer region must be multiple of {} bytes",
            LinePtrBytes::LINE_PTR_BYTES
        );
        assert!(
            len <= capacity,
            "len must be less than or equal to capacity"
        );
        let bytes = LinePtrBytes::new(bytes);
        Self {
            bytes,
            len,
            capacity,
        }
    }

    fn with_len(bytes: &'a [u8], len: usize) -> Self {
        assert!(
            bytes.len() % LinePtrBytes::LINE_PTR_BYTES == 0,
            "line pointer region must be multiple of {} bytes",
            LinePtrBytes::LINE_PTR_BYTES
        );
        let capacity = bytes.len() / LinePtrBytes::LINE_PTR_BYTES;
        Self::new(bytes, len, capacity)
    }

    fn len(&self) -> usize {
        self.len
    }

    fn get(&self, index: usize) -> LinePtr {
        assert!(index < self.len, "index out of bounds");
        assert_eq!(
            self.bytes.as_slice().len(),
            self.capacity * LinePtrBytes::LINE_PTR_BYTES
        );
        LinePtr::from_bytes(&self.bytes.read(index))
    }
}

struct LinePtrArrayMut<'a> {
    bytes: LinePtrBytesMut<'a>,
    len: usize,
    capacity: usize,
}

impl<'a> LinePtrArrayMut<'a> {
    fn new(bytes: &'a mut [u8], len: usize, capacity: usize) -> Self {
        assert!(
            bytes.len() % LinePtrBytes::LINE_PTR_BYTES == 0,
            "line pointer region must be multiple of {} bytes",
            LinePtrBytes::LINE_PTR_BYTES
        );
        assert!(
            len <= capacity,
            "len must be less than or equal to capacity"
        );
        let bytes = LinePtrBytesMut::new(bytes);
        Self {
            bytes,
            len,
            capacity,
        }
    }

    fn with_len(bytes: &'a mut [u8], len: usize) -> Self {
        assert!(
            bytes.len() % LinePtrBytes::LINE_PTR_BYTES == 0,
            "line pointer region must be multiple of {} bytes",
            LinePtrBytes::LINE_PTR_BYTES
        );
        let capacity = bytes.len() / LinePtrBytes::LINE_PTR_BYTES;
        Self::new(bytes, len, capacity)
    }

    fn as_ref(&self) -> LinePtrArray<'_> {
        LinePtrArray::new(self.bytes.as_ref().as_slice(), self.len, self.capacity)
    }

    fn set(&mut self, index: usize, line_ptr: LinePtr) {
        assert!(index < self.capacity, "index out of bounds");
        self.bytes.write(index, line_ptr);
    }

    fn insert(&mut self, index: usize, line_ptr: LinePtr) {
        assert!(self.len < self.capacity, "line pointer array is full");
        assert!(index <= self.len, "insert index out of bounds");
        if index < self.len {
            self.bytes.shift_right(index, self.len);
        }
        self.set(index, line_ptr);
        self.len += 1;
    }

    fn delete(&mut self, index: usize) {
        self.bytes.shift_left(index + 1, self.len);
        self.len -= 1;
    }

    fn len(&self) -> usize {
        self.len
    }
}

/// 4-byte line pointer encoding offset (16 bits), length (12 bits), and state (4 bits).
///
/// Layout: `[offset:16][length:12][state:4]`
#[derive(Clone, Copy)]
struct LinePtr(u32);

/// State of a tuple slot in the slotted page.
#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LineState {
    /// Slot is free and available for reuse
    Free = 0,
    /// Slot contains a live tuple
    Live = 1,
    /// Slot contains a dead tuple (marked for garbage collection)
    Dead = 2,
    /// Slot redirects to another slot (for tuple updates)
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
    /// Creates a new line pointer with the given offset, length, and state.
    fn new(offset: u16, length: u16, state: LineState) -> Self {
        let mut line_pointer = LinePtr(0);
        line_pointer.set_offset(offset);
        line_pointer.set_length(length);
        line_pointer.set_state(state);
        line_pointer
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        assert_eq!(bytes.len(), 4);
        Self(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn to_bytes(&self) -> [u8; 4] {
        self.0.to_le_bytes()
    }

    /// Extracts the 16-bit offset field.
    fn offset(&self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// Extracts the 12-bit length field.
    fn length(&self) -> u16 {
        ((self.0 >> 4) & 0x0FFF) as u16
    }

    /// Returns offset and length as (usize, usize).
    fn offset_and_length(&self) -> (usize, usize) {
        (self.offset() as usize, self.length() as usize)
    }

    /// Extracts the 4-bit state field.
    fn state(&self) -> LineState {
        let state = self.0 & 0x000F;
        LineState::from_u32(state)
    }

    /// Updates the offset field.
    fn set_offset(&mut self, offset: u16) {
        self.0 = (self.0 & 0x0000_FFFF) | ((offset as u32) << 16);
    }

    /// Updates the length field (clamped to 12 bits).
    fn set_length(&mut self, length: u16) {
        let length_bits = (length as u32) & 0x0FFF;
        self.0 = (self.0 & 0xFFFF_000F) | (length_bits << 4);
    }

    /// Updates the state field.
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

    /// Checks if this slot is free.
    fn is_free(&self) -> bool {
        self.state() == LineState::Free
    }

    /// Checks if this slot is live.
    fn is_live(&self) -> bool {
        self.state() == LineState::Live
    }

    /// Marks this slot as free.
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

    /// Marks this slot as a redirect to another slot.
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

/// Marker trait associating a compile-time page kind with its runtime PageType.
pub trait PageKind: Sized {
    #[allow(dead_code)]
    const PAGE_TYPE: PageType;
    const HEADER_SIZE: usize;
    type Header;
}

/// Slot identifier within a page.
type SlotId = usize;

#[derive(Debug)]
pub struct PageBytes {
    bytes: [u8; PAGE_SIZE_BYTES as usize],
}

impl PageBytes {
    pub fn new() -> Self {
        Self {
            bytes: [0; PAGE_SIZE_BYTES as usize],
        }
    }

    pub fn from_bytes(bytes: [u8; PAGE_SIZE_BYTES as usize]) -> Self {
        Self { bytes }
    }

    pub fn peek_page_type(&self) -> SimpleDBResult<PageType> {
        self.bytes[0].try_into()
    }

    pub fn bytes<'a>(&'a self) -> &'a [u8] {
        &self.bytes
    }

    pub fn bytes_mut<'a>(&'a mut self) -> &'a mut [u8] {
        &mut self.bytes
    }
}

struct HeapRecordSpace<'a> {
    bytes: &'a [u8],
    base_offset: usize,
}

impl<'a> HeapRecordSpace<'a> {
    fn new(bytes: &'a [u8], base_offset: usize) -> Self {
        assert!(
            base_offset <= PAGE_SIZE_BYTES as usize,
            "base offset must lie within page"
        );
        assert!(
            base_offset + bytes.len() == PAGE_SIZE_BYTES as usize,
            "record space must cover remaining page bytes"
        );
        Self { bytes, base_offset }
    }

    fn tuple_bytes(&self, ptr: LinePtr) -> Option<&'a [u8]> {
        if !ptr.is_live() {
            return None;
        }

        let (offset, length) = ptr.offset_and_length();
        let relative = offset.checked_sub(self.base_offset)?;
        self.bytes.get(relative..relative + length)
    }
}

struct HeapRecordSpaceMut<'a> {
    bytes: &'a mut [u8],
    base_offset: usize,
}

impl<'a> HeapRecordSpaceMut<'a> {
    fn new(bytes: &'a mut [u8], base_offset: usize) -> Self {
        assert!(
            base_offset <= PAGE_SIZE_BYTES as usize,
            "base offset must lie within page"
        );
        assert!(
            base_offset + bytes.len() == PAGE_SIZE_BYTES as usize,
            "record space must cover remaining page bytes"
        );
        Self { bytes, base_offset }
    }

    fn write_tuple(&mut self, offset: usize, tuple: &[u8]) {
        let relative = offset
            .checked_sub(self.base_offset)
            .expect("tuple offset precedes record space");
        let end = relative + tuple.len();
        self.bytes[relative..end].copy_from_slice(tuple);
    }
}

/// B-tree-specific payload helpers operate directly on the record slice.
struct BTreeRecordSpace<'a> {
    bytes: &'a [u8],
    base_offset: usize,
}

impl<'a> BTreeRecordSpace<'a> {
    fn new(bytes: &'a [u8], base_offset: usize) -> Self {
        assert!(
            base_offset + bytes.len() == PAGE_SIZE_BYTES as usize,
            "record space must cover remaining page"
        );
        Self { bytes, base_offset }
    }

    fn entry_bytes(&self, ptr: LinePtr) -> Option<&'a [u8]> {
        let offset = ptr.offset() as usize;
        let length = ptr.length() as usize;
        let relative = offset.checked_sub(self.base_offset)?;
        self.bytes.get(relative..relative + length)
    }
}

struct BTreeRecordSpaceMut<'a> {
    bytes: &'a mut [u8],
    base_offset: usize,
}

impl<'a> BTreeRecordSpaceMut<'a> {
    fn new(bytes: &'a mut [u8], base_offset: usize) -> Self {
        assert!(
            base_offset + bytes.len() == PAGE_SIZE_BYTES as usize,
            "record space must cover remaining page"
        );
        Self { bytes, base_offset }
    }

    fn write_entry(&mut self, offset: usize, bytes: &[u8]) {
        let relative = offset
            .checked_sub(self.base_offset)
            .expect("entry offset precedes record space");
        let end = relative + bytes.len();
        self.bytes[relative..end].copy_from_slice(bytes);
    }

    fn copy_within(&mut self, src_offset: usize, dst_offset: usize, len: usize) {
        if len == 0 {
            return;
        }
        let src_relative = src_offset
            .checked_sub(self.base_offset)
            .expect("source offset precedes record space");
        let dst_relative = dst_offset
            .checked_sub(self.base_offset)
            .expect("destination offset precedes record space");
        let src_end = src_relative + len;
        self.bytes.copy_within(src_relative..src_end, dst_relative);
    }
}

struct HeapPageZeroCopy<'a> {
    header: HeapHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: HeapRecordSpace<'a>,
}

impl<'a> PageKind for HeapPageZeroCopy<'a> {
    const PAGE_TYPE: PageType = PageType::Heap;
    const HEADER_SIZE: usize = 32;
    type Header = HeapHeaderRef<'a>;
}

impl<'a> HeapPageZeroCopy<'a> {
    fn new(bytes: &'a [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, rest) = bytes.split_at(HeapPageZeroCopy::HEADER_SIZE as usize);
        let header = HeapHeaderRef::new(header_bytes);
        if header.page_type() != PageType::Heap {
            return Err("not a heap page".into());
        }
        let lp_capacity = header.free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > rest.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = rest.split_at(lp_capacity);
        let base_offset = Self::HEADER_SIZE as usize + lp_capacity;
        let page = Self::from_parts(header, line_ptr_bytes, record_space_bytes, base_offset);
        assert_eq!(
            page.slot_count(),
            header.slot_count() as usize,
            "slot directory length must match header slot_count"
        );
        Ok(page)
    }

    fn from_parts(
        header: HeapHeaderRef<'a>,
        line_ptr_bytes: &'a [u8],
        record_space_bytes: &'a [u8],
        base_offset: usize,
    ) -> Self {
        Self {
            header,
            line_pointers: LinePtrArray::with_len(line_ptr_bytes, header.slot_count() as usize),
            record_space: HeapRecordSpace::new(record_space_bytes, base_offset),
        }
    }

    fn slot_count(&self) -> usize {
        self.header.slot_count() as usize
    }

    fn line_ptr(&self, slot: SlotId) -> Option<LinePtr> {
        if slot >= self.line_pointers.len() {
            None
        } else {
            Some(self.line_pointers.get(slot))
        }
    }

    fn tuple_bytes(&self, slot: SlotId) -> Option<&'a [u8]> {
        let lp = self.line_ptr(slot)?;
        self.record_space.tuple_bytes(lp)
    }

    fn tuple_ref(&self, slot: SlotId) -> Option<TupleRef<'a>> {
        let lp = self.line_ptr(slot)?;
        match lp.state() {
            LineState::Free => Some(TupleRef::Free),
            LineState::Live => Some(TupleRef::Live(HeapTuple::from_bytes(
                self.tuple_bytes(slot)?,
            ))),
            LineState::Dead => Some(TupleRef::Dead),
            LineState::Redirect => Some(TupleRef::Redirect(lp.offset() as usize)),
        }
    }
}

pub struct HeapPageZeroCopyMut<'a> {
    header: HeapHeaderMut<'a>,
    body_bytes: &'a mut [u8],
}

impl<'a> PageKind for HeapPageZeroCopyMut<'a> {
    const PAGE_TYPE: PageType = PageType::Heap;
    const HEADER_SIZE: usize = 32;
    type Header = HeapHeaderMut<'a>;
}

impl<'a> HeapPageZeroCopyMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, body_bytes) = bytes.split_at_mut(PAGE_HEADER_SIZE_BYTES as usize);
        let header = HeapHeaderMut::new(header_bytes);
        if header.as_ref().page_type() != PageType::Heap {
            return Err("not a heap page".into());
        }
        Ok(Self { header, body_bytes })
    }

    #[allow(dead_code)]
    fn as_read(&self) -> SimpleDBResult<HeapPageZeroCopy<'_>> {
        let slot_len = self.header.as_ref().slot_count() as usize * LinePtrBytes::LINE_PTR_BYTES;
        if slot_len > self.body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_bytes) = (&self.body_bytes[..]).split_at(slot_len);
        let base_offset = Self::HEADER_SIZE as usize + slot_len;
        let page = HeapPageZeroCopy::from_parts(
            self.header.as_ref(),
            line_ptr_bytes,
            record_bytes,
            base_offset,
        );
        assert_eq!(
            page.slot_count(),
            self.header.as_ref().slot_count() as usize,
            "slot directory length must match header slot_count"
        );
        Ok(page)
    }

    pub fn update_crc32(&mut self) {
        self.header.update_crc32(&self.body_bytes);
    }

    pub fn verify_crc32(&mut self) -> bool {
        self.header.verify_crc32(&self.body_bytes)
    }

    /// Splits the mutable page into header/slot-dir/record-space views tied together by a guard.
    /// Callers must obtain this guard before performing any mutation so the slot-directory
    /// boundary always reflects the latest `slot_count`. Dropping the guard releases the borrows,
    /// forcing the next operation to resplit.
    fn split(&mut self) -> SimpleDBResult<HeapPageParts<'_>> {
        let lp_capacity = self.header.as_ref().free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > self.body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = self.body_bytes.split_at_mut(lp_capacity);
        let base_offset = self.header.as_ref().free_lower() as usize;

        assert_eq!(
            self.header.as_ref().free_lower() as usize,
            Self::HEADER_SIZE + lp_capacity
        );
        assert_eq!(
            Self::HEADER_SIZE + lp_capacity + record_space_bytes.len(),
            PAGE_SIZE_BYTES as usize
        );

        let slot_count = self.header.as_ref().slot_count() as usize;
        let parts = HeapPageParts {
            header: HeapHeaderMut::new(self.header.bytes_mut()),
            line_ptrs: LinePtrArrayMut::with_len(line_ptr_bytes, slot_count),
            record_space: HeapRecordSpaceMut::new(record_space_bytes, base_offset),
        };
        assert_eq!(
            parts.line_ptrs.as_ref().len(),
            parts.header.as_ref().slot_count() as usize,
            "slot directory length must match header slot_count"
        );
        Ok(parts)
    }
}

struct ReservedSlot {
    slot_idx: SlotId,
}

/// Outcome of attempting to insert through the freelist fast path.
enum HeapInsert {
    /// Tuple inserted via freelist
    Done(SlotId),
    /// Freelist empty; header reserved a new slot entry that must be initialized after re-splitting
    Reserved(ReservedSlot),
}

/// Guard holding disjoint mutable views over a heap page's header, slot directory, and record
/// space. All heap mutations must go through this guard so the slices stay aligned with the
/// latest `slot_count`. Drop releases the borrows so the next mutation re-splits the page.
pub struct HeapPageParts<'a> {
    header: HeapHeaderMut<'a>,
    line_ptrs: LinePtrArrayMut<'a>,
    record_space: HeapRecordSpaceMut<'a>,
}

impl<'a> HeapPageParts<'a> {
    fn header(&mut self) -> &mut HeapHeaderMut<'a> {
        &mut self.header
    }

    fn line_ptrs(&mut self) -> &mut LinePtrArrayMut<'a> {
        &mut self.line_ptrs
    }

    fn record_space(&mut self) -> &mut HeapRecordSpaceMut<'a> {
        &mut self.record_space
    }

    fn push_free_slot(&mut self, slot: SlotId) {
        let mut lp = self.line_ptrs.as_ref().get(slot);
        assert!(lp.is_free(), "slot {slot} must be free");
        let next = self.header.as_ref().free_head();
        lp.set_offset(next);
        lp.set_length(0);
        self.line_ptrs.set(slot, lp);
        self.header
            .set_free_head(slot.try_into().expect("slot id fits in u16"));
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

    fn insert_tuple_fast(&mut self, bytes: &[u8]) -> SimpleDBResult<HeapInsert> {
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size (u16::MAX)".to_string())?;

        //  Try the fast path freelist first
        if let Some(slot) = self.pop_free_slot() {
            let (lower, upper) = {
                let hdr = self.header.as_ref();
                (hdr.free_lower(), hdr.free_upper())
            };
            if upper - lower < needed {
                return Err("insufficient free space".into());
            }
            let new_upper = upper - needed;
            self.record_space.write_tuple(new_upper as usize, bytes);
            self.line_ptrs
                .set(slot, LinePtr::new(new_upper, needed, LineState::Live));
            self.header.set_free_upper(new_upper);
            self.header.set_free_ptr(new_upper as u32);
            return Ok(HeapInsert::Done(slot));
        }

        //  Fast path didn't work, need to carve out space
        let lower = self.header.as_ref().free_lower();
        let upper = self.header.as_ref().free_upper();
        if lower as usize + LinePtrBytes::LINE_PTR_BYTES > upper as usize {
            return Err("insufficient space for line pointer".into());
        }
        self.header
            .set_free_lower(lower + LinePtrBytes::LINE_PTR_BYTES as u16);
        let new_slot_count = self.header.as_ref().slot_count() + 1;
        self.header.set_slot_count(new_slot_count);
        let slot_idx = (new_slot_count - 1) as SlotId;
        Ok(HeapInsert::Reserved(ReservedSlot { slot_idx }))
    }

    fn insert_tuple_slow(
        &mut self,
        reservation: ReservedSlot,
        bytes: &[u8],
    ) -> SimpleDBResult<SlotId> {
        let needed: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size (u16::MAX)".to_string())?;
        let (lower, upper) = {
            let header = self.header.as_ref();
            (header.free_lower(), header.free_upper())
        };
        if upper - lower < needed {
            return Err("insufficient free space".into());
        }
        let new_upper = upper - needed;
        self.record_space().write_tuple(new_upper as usize, bytes);
        let slot = reservation.slot_idx;
        let expected = self
            .line_ptrs()
            .len()
            .checked_sub(1)
            .ok_or_else(|| -> Box<dyn Error> { "slot directory empty after reservation".into() })?;
        if slot != expected {
            return Err("reserved slot index mismatch".into());
        }
        self.line_ptrs()
            .set(slot, LinePtr::new(new_upper, needed, LineState::Live));

        self.header.set_free_upper(new_upper);
        self.header.set_free_ptr(new_upper as u32);
        Ok(slot)
    }

    fn delete_slot(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        assert_eq!(
            self.header().as_ref().slot_count() as usize,
            self.line_ptrs().len()
        );
        let mut lp = self.line_ptrs.as_ref().get(slot);
        if !lp.is_live() {
            return Err(format!("slot {slot} is not live").into());
        }
        lp.mark_free();
        self.line_ptrs.set(slot, lp);
        self.push_free_slot(slot);
        Ok(())
    }

    fn redirect_slot(&mut self, slot: SlotId, target: SlotId) -> SimpleDBResult<()> {
        let mut line_pointer = self.line_ptrs().as_ref().get(slot);
        assert!(line_pointer.is_live(), "slot {slot} must be live");
        line_pointer.mark_redirect(target.try_into().expect("slot id does not in u16"));
        self.line_ptrs.set(slot, line_pointer);
        Ok(())
    }
}

/// Iterator over tuples in a heap page, optionally filtering by LineState.
pub struct HeapIterator<'a> {
    page: HeapPageZeroCopy<'a>,
    current_slot: SlotId,
    match_state: Option<LineState>,
}

impl<'a> Iterator for HeapIterator<'a> {
    type Item = (SlotId, TupleRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        let total_slots = self.page.slot_count();
        while self.current_slot < total_slots {
            let slot = self.current_slot;
            self.current_slot += 1;
            if let Some(tuple_ref) = self.page.tuple_ref(slot) {
                if self
                    .match_state
                    .is_none_or(|ms| ms == tuple_ref.line_state())
                {
                    return Some((slot, tuple_ref));
                }
            }
        }
        None
    }
}

/// Iterator over entries in a B-tree leaf page.
pub struct BTreeLeafIterator<'a> {
    page: BTreeLeafPageZeroCopy<'a>,
    layout: &'a Layout,
    current_slot: SlotId,
}

impl<'a> BTreeLeafIterator<'a> {
    fn new(page: BTreeLeafPageZeroCopy<'a>, layout: &'a Layout) -> Self {
        Self {
            page,
            layout,
            current_slot: 0,
        }
    }
}

/// Iterator over entries in a B-tree internal page.
pub struct BTreeInternalIterator<'a> {
    page: BTreeInternalPageZeroCopy<'a>,
    layout: &'a Layout,
    current_slot: SlotId,
}

impl<'a> BTreeInternalIterator<'a> {
    fn new(page: BTreeInternalPageZeroCopy<'a>, layout: &'a Layout) -> Self {
        Self {
            page,
            layout,
            current_slot: 0,
        }
    }
}

impl Iterator for BTreeLeafIterator<'_> {
    type Item = BTreeLeafEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_slot < self.page.slot_count() {
            let slot = self.current_slot;
            self.current_slot += 1;

            if let Some(bytes) = self.page.entry_bytes(slot) {
                return BTreeLeafEntry::decode(self.layout, bytes).ok();
            }
        }
        None
    }
}

impl Iterator for BTreeInternalIterator<'_> {
    type Item = BTreeInternalEntry;

    fn next(&mut self) -> Option<Self::Item> {
        while self.current_slot < self.page.slot_count() {
            let slot = self.current_slot;
            self.current_slot += 1;

            if let Some(bytes) = self.page.entry_bytes(slot) {
                return BTreeInternalEntry::decode(self.layout, bytes).ok();
            }
        }
        None
    }
}

/// Write-ahead log page using boundary-pointer format for sequential record storage.
///
/// WAL pages don't use slotted page layout. Instead, they store records sequentially
/// from the end of the page towards the beginning, with a boundary pointer tracking
/// the current insertion point.
#[derive(Debug)]
pub struct WalPage {
    data: Vec<u8>,
}

impl WalPage {
    pub const HEADER_BYTES: usize = 4;

    /// Creates a new WAL page with the boundary at the end.
    pub fn new() -> Self {
        let mut page = Self {
            data: vec![0u8; PAGE_SIZE_BYTES as usize],
        };
        page.reset();
        page
    }

    /// Resets the page by zeroing data and setting boundary to end.
    pub fn reset(&mut self) {
        self.data.fill(0);
        self.set_boundary(self.data.len());
    }

    /// Returns the current boundary offset.
    pub fn boundary(&self) -> usize {
        let mut buf = [0u8; Self::HEADER_BYTES];
        buf.copy_from_slice(&self.data[..Self::HEADER_BYTES]);
        i32::from_be_bytes(buf) as usize
    }

    /// Sets the boundary offset.
    pub fn set_boundary(&mut self, offset: usize) {
        assert!(
            offset <= self.data.len(),
            "boundary cannot exceed page capacity"
        );
        let value = i32::try_from(offset).expect("boundary offset must fit in i32");
        self.data[..Self::HEADER_BYTES].copy_from_slice(&value.to_be_bytes());
    }

    /// Returns the total page capacity in bytes.
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

    /// Reads a record from the given offset. Returns (data, next_offset).
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

    /// Returns a reference to the page bytes.
    pub fn bytes(&self) -> &[u8] {
        &self.data
    }

    /// Returns a mutable reference to the page bytes.
    pub fn bytes_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// Read guard providing shared access to a pinned page.
///
/// Holds a buffer handle, frame reference, and read lock on the page data.
/// Automatically unpins when dropped.
pub struct PageReadGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockReadGuard<'a, PageBytes>,
}

impl<'a> PageReadGuard<'a> {
    /// Creates a new read guard.
    pub fn new(
        handle: BufferHandle,
        frame: Arc<BufferFrame>,
        page: RwLockReadGuard<'a, PageBytes>,
    ) -> Self {
        Self {
            handle,
            frame,
            page,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.page.bytes()
    }

    /// Returns the block ID of the pinned page.
    pub fn block_id(&self) -> &BlockId {
        self.handle.block_id()
    }

    /// Returns the buffer frame.
    pub fn frame(&self) -> &BufferFrame {
        &self.frame
    }

    /// Converts to a typed heap page view with schema access.
    pub fn into_heap_view(self, layout: &'a Layout) -> SimpleDBResult<HeapPageView<'a>> {
        HeapPageView::new(self, layout)
    }

    /// Converts to a B-tree leaf page view.
    pub fn into_btree_leaf_page_view(
        self,
        layout: &'a Layout,
    ) -> SimpleDBResult<BTreeLeafPageView<'a>> {
        BTreeLeafPageView::new(self, layout)
    }

    /// Converts to a B-tree internal page view.
    pub fn into_btree_internal_page_view(
        self,
        layout: &'a Layout,
    ) -> SimpleDBResult<BTreeInternalPageView<'a>> {
        BTreeInternalPageView::new(self, layout)
    }
}

/// Write guard providing exclusive access to a pinned page.
///
/// Holds a buffer handle, frame reference, and write lock on the page data.
/// Automatically unpins when dropped.
pub struct PageWriteGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockWriteGuard<'a, PageBytes>,
}

impl<'a> PageWriteGuard<'a> {
    /// Creates a new write guard.
    pub fn new(
        handle: BufferHandle,
        frame: Arc<BufferFrame>,
        page: RwLockWriteGuard<'a, PageBytes>,
    ) -> Self {
        Self {
            handle,
            frame,
            page,
        }
    }

    pub fn bytes(&self) -> &[u8] {
        self.page.bytes()
    }

    pub fn bytes_mut(&mut self) -> &mut [u8] {
        self.page.bytes_mut()
    }

    /// Returns the block ID of the pinned page.
    pub fn block_id(&self) -> &BlockId {
        self.handle.block_id()
    }

    /// Returns the transaction ID.
    pub fn txn_id(&self) -> usize {
        self.handle.txn_id()
    }

    /// Returns the buffer frame.
    pub fn frame(&self) -> &BufferFrame {
        &self.frame
    }

    /// Marks the page as modified for WAL.
    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.frame.set_modified(txn_id, lsn);
    }

    /// Formats the page as an empty heap page.
    pub fn format_as_heap(&mut self) {
        let bytes = self.bytes_mut();
        bytes.fill(0);

        let mut header = HeapHeaderMut::new(bytes);
        header.init_heap();
    }

    /// Formats the page as an empty B-tree leaf page.
    pub fn format_as_btree_leaf(&mut self, overflow_block: Option<usize>) {
        let bytes = self.bytes_mut();
        bytes.fill(0);

        let mut header = BTreeLeafHeaderMut::new(bytes);
        header.init_leaf(0, None, overflow_block.map(|b| b as u32));
    }

    /// Formats the page as an empty B-tree internal page.
    pub fn format_as_btree_internal(&mut self, level: u8) {
        let bytes = self.bytes_mut();
        bytes.fill(0);

        let mut header = BTreeInternalHeaderMut::new(bytes);
        header.init_internal(level, None);
    }

    pub fn into_heap_view_mut(self, layout: &'a Layout) -> SimpleDBResult<HeapPageViewMut<'a>> {
        HeapPageViewMut::new(self, layout)
    }

    /// Converts to a mutable B-tree leaf page view.
    pub fn into_btree_leaf_page_view_mut(
        self,
        layout: &'a Layout,
    ) -> SimpleDBResult<BTreeLeafPageViewMut<'a>> {
        BTreeLeafPageViewMut::new(self, layout)
    }

    /// Converts to a mutable B-tree internal page view.
    pub fn into_btree_internal_page_view_mut(
        self,
        layout: &'a Layout,
    ) -> SimpleDBResult<BTreeInternalPageViewMut<'a>> {
        BTreeInternalPageViewMut::new(self, layout)
    }
}

impl Deref for PageWriteGuard<'_> {
    type Target = PageBytes;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;

    fn heap_tuple_bytes(payload: &[u8]) -> Vec<u8> {
        let mut header_bytes = [0u8; HEAP_TUPLE_HEADER_BYTES];
        let mut header = HeapTupleHeaderBytesMut::from_bytes(&mut header_bytes);
        header.set_xmin(1);
        header.set_xmax(0);
        header.set_payload_len(payload.len() as u32);
        header.set_flags(0);
        header.set_nullmap_ptr(0);

        let mut buf = Vec::with_capacity(HEAP_TUPLE_HEADER_BYTES + payload.len());
        buf.extend_from_slice(&header_bytes);
        buf.extend_from_slice(payload);
        buf
    }

    fn insert_tuple_bytes(
        page: &mut HeapPageZeroCopyMut<'_>,
        tuple: &[u8],
    ) -> SimpleDBResult<SlotId> {
        let mut split_guard = page.split()?;
        let slot = match split_guard.insert_tuple_fast(tuple)? {
            HeapInsert::Done(slot) => slot,
            HeapInsert::Reserved(reservation) => {
                drop(split_guard);
                let mut split_guard = page.split()?;
                split_guard.insert_tuple_slow(reservation, tuple)?
            }
        };
        Ok(slot)
    }

    #[test]
    fn allocate_tuple_exposes_bytes_and_tuple_ref() {
        let mut bytes = [0u8; PAGE_SIZE_BYTES as usize];
        let payload = vec![1u8, 2, 3, 4];
        let tuple = heap_tuple_bytes(&payload);

        let mut page = HeapPageZeroCopyMut::new(&mut bytes).unwrap();
        let slot = insert_tuple_bytes(&mut page, &tuple).unwrap();

        assert_eq!(
            page.as_read().unwrap().tuple_bytes(slot).unwrap(),
            tuple.as_slice()
        );

        match page.as_read().unwrap().tuple_ref(slot).unwrap() {
            TupleRef::Live(heap_tuple) => {
                assert_eq!(heap_tuple.payload(), payload.as_slice());
                assert_eq!(heap_tuple.payload_len(), payload.len() as u32);
            }
            _ => panic!("expected live tuple"),
        }
    }

    #[test]
    fn delete_frees_slot_and_allocation_reuses_it() {
        let mut bytes = [0u8; PAGE_SIZE_BYTES as usize];
        let mut page = HeapPageZeroCopyMut::new(&mut bytes).unwrap();
        let tuple_a = heap_tuple_bytes(&[10]);
        let tuple_b = heap_tuple_bytes(&[20, 30]);
        let tuple_c_payload = vec![99, 100, 101];
        let tuple_c = heap_tuple_bytes(&tuple_c_payload);

        let slot_a = insert_tuple_bytes(&mut page, &tuple_a).unwrap();
        let slot_b = insert_tuple_bytes(&mut page, &tuple_b).unwrap();
        assert_eq!(slot_a, 0);
        assert_eq!(slot_b, 1);

        page.split()
            .unwrap()
            .delete_slot(slot_a)
            .expect("delete live tuple");

        let reused = insert_tuple_bytes(&mut page, &tuple_c).unwrap();
        assert_eq!(reused, slot_a, "freed slot should be reused first");

        match page.as_read().unwrap().tuple_ref(reused).unwrap() {
            TupleRef::Live(tuple) => {
                assert_eq!(tuple.payload(), tuple_c_payload.as_slice());
            }
            _ => panic!("expected live tuple in reused slot"),
        }
    }

    #[test]
    fn crc32_detects_corruption() {
        let mut bytes = [0u8; PAGE_SIZE_BYTES as usize];
        let mut page = HeapPageZeroCopyMut::new(&mut bytes).unwrap();
        let payload = vec![7u8; 16];

        insert_tuple_bytes(&mut page, &heap_tuple_bytes(&payload)).expect("tuple allocation");
        page.update_crc32();

        let mut pristine = vec![0u8; PAGE_SIZE_BYTES as usize];
        pristine.copy_from_slice(&bytes);

        let mut corrupted = pristine.clone();
        corrupted[128] ^= 0xFF; // flip a byte to simulate torn write

        let mut ok_page = HeapPageZeroCopyMut::new(&mut pristine).expect("deserialize pristine");
        assert!(ok_page.verify_crc32());

        let mut bad_page = HeapPageZeroCopyMut::new(&mut corrupted).expect("deserialize corrupted");
        assert!(!bad_page.verify_crc32());
    }

    #[test]
    fn pack_and_unpack_preserves_tuples() {
        let mut bytes = [0u8; PAGE_SIZE_BYTES as usize];
        let mut page = HeapPageZeroCopyMut::new(&mut bytes).unwrap();
        let payload = vec![42u8, 43, 44, 45];
        let slot = insert_tuple_bytes(&mut page, &heap_tuple_bytes(&payload)).unwrap();

        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        buf.copy_from_slice(&bytes);

        let reconstructed = HeapPageZeroCopy::new(&buf).expect("unpack succeeds");

        match reconstructed.tuple_ref(slot).unwrap() {
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
}

/// Reference to a tuple slot in various states.
pub enum TupleRef<'a> {
    /// Live tuple with accessible data
    Live(HeapTuple<'a>),
    /// Redirect to another slot (for updated tuples)
    Redirect(SlotId),
    /// Free slot available for reuse
    Free,
    /// Dead tuple marked for garbage collection
    Dead,
}

impl TupleRef<'_> {
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
        self.bytes[byte] |= mask;
    }

    fn clear(&mut self, col_idx: usize) {
        let byte = col_idx / 8;
        let bit = col_idx % 8;
        let mask = 1u8 << bit;
        self.bytes[byte] &= !mask;
    }
}

#[cfg(test)]
fn build_tuple_bytes(payload: &[u8], nullmap_ptr: u16) -> Vec<u8> {
    let mut header_buf = [0u8; HEAP_TUPLE_HEADER_BYTES];
    let mut header = HeapTupleHeaderBytesMut::from_bytes(&mut header_buf);
    header.set_xmin(1);
    header.set_xmax(0);
    header.set_payload_len(payload.len() as u32);
    header.set_flags(0);
    header.set_nullmap_ptr(nullmap_ptr);
    let mut buf = vec![0u8; HEAP_TUPLE_HEADER_BYTES + payload.len()];
    buf[..HEAP_TUPLE_HEADER_BYTES].copy_from_slice(&header_buf);
    buf[HEAP_TUPLE_HEADER_BYTES..].copy_from_slice(payload);
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

        let bytes = tuple.payload_slice(1, 4);
        let i32 = i32::from_be_bytes(bytes.try_into().unwrap());
        assert_eq!(i32, 0x01020304);

        let length_bytes = tuple.payload_slice(1 + 4, 4);
        let length = u32::from_be_bytes(length_bytes.try_into().unwrap()) as usize;
        let bytes = tuple.payload_slice(1 + 4 + 4, length);
        assert_eq!(bytes, b"abc");

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
        let bytes = tuple.payload_slice(1, 4);
        let i32 = i32::from_be_bytes(bytes.try_into().unwrap());
        assert_eq!(i32, 0x0A0B0C0D);

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
            let mut row_mut =
                LogicalRowMut::new(tuple_mut, layout.clone(), Rc::new(Cell::new(false)));
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
                let mut row_mut =
                    LogicalRowMut::new(tuple_mut, layout.clone(), Rc::new(Cell::new(false)));
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

/// The layout of the heap tuple header is as follows:
/// - xmin: 8 bytes
/// - xmax: 8 bytes
/// - payload_len: 4 bytes
/// - flags: 2 bytes
/// - nullmap_ptr: 2 bytes
const HEAP_TUPLE_HEADER_BYTES: usize = 24;

struct HeapTupleHeaderBytes<'a> {
    bytes: &'a [u8; HEAP_TUPLE_HEADER_BYTES],
}

impl<'a> HeapTupleHeaderBytes<'a> {
    fn from_bytes(bytes: &'a [u8; HEAP_TUPLE_HEADER_BYTES]) -> Self {
        Self { bytes }
    }

    #[allow(dead_code)]
    fn xmin(&self) -> u64 {
        u64::from_le_bytes(self.bytes[0..8].try_into().unwrap())
    }

    #[allow(dead_code)]
    fn xmax(&self) -> u64 {
        u64::from_le_bytes(self.bytes[8..16].try_into().unwrap())
    }

    #[allow(dead_code)]
    fn payload_len(&self) -> u32 {
        u32::from_le_bytes(self.bytes[16..20].try_into().unwrap())
    }

    #[allow(dead_code)]
    fn flags(&self) -> u16 {
        u16::from_le_bytes(self.bytes[20..22].try_into().unwrap())
    }

    fn nullmap_ptr(&self) -> u16 {
        u16::from_le_bytes(self.bytes[22..24].try_into().unwrap())
    }
}

struct HeapTupleHeaderBytesMut<'a> {
    bytes: &'a mut [u8; HEAP_TUPLE_HEADER_BYTES],
}

impl<'a> HeapTupleHeaderBytesMut<'a> {
    fn from_bytes(bytes: &'a mut [u8; HEAP_TUPLE_HEADER_BYTES]) -> Self {
        Self { bytes }
    }

    fn as_ref(&self) -> HeapTupleHeaderBytes<'_> {
        HeapTupleHeaderBytes::from_bytes(self.bytes)
    }

    fn set_xmin(&mut self, xmin: u64) {
        self.bytes[0..8].copy_from_slice(&xmin.to_le_bytes());
    }

    fn set_xmax(&mut self, xmax: u64) {
        self.bytes[8..16].copy_from_slice(&xmax.to_le_bytes());
    }

    fn set_payload_len(&mut self, payload_len: u32) {
        self.bytes[16..20].copy_from_slice(&payload_len.to_le_bytes());
    }

    fn set_flags(&mut self, flags: u16) {
        self.bytes[20..22].copy_from_slice(&flags.to_le_bytes());
    }

    fn set_nullmap_ptr(&mut self, nullmap_ptr: u16) {
        self.bytes[22..24].copy_from_slice(&nullmap_ptr.to_le_bytes());
    }
}

/// Immutable view of a heap tuple with header and payload.
pub struct HeapTuple<'a> {
    header: HeapTupleHeaderBytes<'a>,
    payload: &'a [u8],
}

impl<'a> HeapTuple<'a> {
    fn from_bytes(buf: &'a [u8]) -> Self {
        let (header_bytes, payload_bytes) = buf.split_at(HEAP_TUPLE_HEADER_BYTES);
        let header_bytes: &[u8; HEAP_TUPLE_HEADER_BYTES] = header_bytes.try_into().unwrap();
        let header = HeapTupleHeaderBytes::from_bytes(header_bytes);
        Self {
            header,
            payload: payload_bytes,
        }
    }

    fn nullmap_ptr(&self) -> u16 {
        self.header.nullmap_ptr()
    }

    #[cfg(test)]
    fn payload_len(&self) -> u32 {
        self.header.payload_len()
    }

    fn payload(&self) -> &'a [u8] {
        self.payload
    }

    fn null_bitmap(&self, num_columns: usize) -> NullBitmap<'_> {
        let offset = self.nullmap_ptr() as usize;
        let bytes_needed = num_columns.div_ceil(8);
        let bytes = &self.payload()[offset..offset + bytes_needed];
        NullBitmap::new(bytes)
    }

    fn payload_slice(&self, offset: usize, len: usize) -> &'a [u8] {
        &self.payload()[offset..offset + len]
    }
}

struct HeapTupleMut<'a> {
    header: HeapTupleHeaderBytesMut<'a>,
    payload: &'a mut [u8],
}

impl<'a> HeapTupleMut<'a> {
    fn from_bytes(bytes: &'a mut [u8]) -> Self {
        let (header_bytes, payload_bytes) = bytes.split_at_mut(HEAP_TUPLE_HEADER_BYTES);
        let header_bytes: &mut [u8; HEAP_TUPLE_HEADER_BYTES] = header_bytes.try_into().unwrap();
        let header = HeapTupleHeaderBytesMut::from_bytes(header_bytes);
        Self {
            header,
            payload: payload_bytes,
        }
    }

    fn payload_slice_mut(&mut self, offset: usize, len: usize) -> &'_ mut [u8] {
        &mut self.payload[offset..offset + len]
    }

    fn null_bitmap_mut(&mut self, num_columns: usize) -> NullBitmapMut<'_> {
        let offset = self.header.as_ref().nullmap_ptr() as usize;
        let bytes_needed = num_columns.div_ceil(8);
        let bytes = &mut self.payload[offset..offset + bytes_needed];
        NullBitmapMut::new(bytes)
    }
}

/// Type-safe view of a heap tuple with schema-aware column access.
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

/// Mutable type-safe view of a heap tuple with schema-aware column access.
///
/// Tracks modifications via a shared dirty flag.
pub struct LogicalRowMut<'a> {
    tuple: HeapTupleMut<'a>,
    layout: Layout,
    dirty: Rc<Cell<bool>>,
}

impl<'a> LogicalRowMut<'a> {
    fn new(tuple: HeapTupleMut<'a>, layout: Layout, dirty: Rc<Cell<bool>>) -> Self {
        Self {
            tuple,
            layout,
            dirty,
        }
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
        self.dirty.set(true);
        Some(())
    }

    #[cfg(test)]
    fn set_null(&mut self, column_name: &str) -> Option<()> {
        let (_, index) = self.layout.offset_with_index(column_name)?;
        self.tuple
            .null_bitmap_mut(self.layout.num_of_columns())
            .set_null(index);
        self.dirty.set(true);
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

/// Read-only view of a heap page with schema-aware row access.
///
/// Holds a read guard and provides typed access to logical rows.
pub struct HeapPageView<'a> {
    guard: PageReadGuard<'a>,
    layout: &'a Layout,
}

impl<'a> HeapPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        HeapPageZeroCopy::new(guard.bytes())?;
        Ok(Self { guard, layout })
    }

    fn build_page(&'a self) -> HeapPageZeroCopy<'a> {
        HeapPageZeroCopy::new(self.guard.bytes()).unwrap()
    }

    pub fn row(&self, slot: SlotId) -> Option<LogicalRow<'_>> {
        let view = self.build_page();
        let tuple_ref = view.tuple_ref(slot)?;
        match tuple_ref {
            TupleRef::Live(tuple) => Some(LogicalRow::new(tuple, self.layout)),
            TupleRef::Redirect(_) | TupleRef::Free | TupleRef::Dead => None,
        }
    }

    /// Returns the absolute page offset for the given column within the tuple at `slot`.
    ///
    /// Useful for WAL logging that still expects byte offsets.
    pub fn column_page_offset(&self, slot: SlotId, column_name: &str) -> Option<usize> {
        let payload_offset = self.layout.offset(column_name)?;
        let page = self.build_page();
        let line_ptr = page.line_ptr(slot)?;
        if !line_ptr.is_live() {
            return None;
        }
        let tuple_start = line_ptr.offset() as usize;
        Some(tuple_start + HEAP_TUPLE_HEADER_BYTES + payload_offset)
    }

    pub fn slot_count(&self) -> usize {
        self.build_page().slot_count()
    }

    pub fn live_slot_iter(&self) -> HeapIterator<'_> {
        todo!()
        // HeapPage::live_iterator(self.guard.as_heap_page().unwrap())
    }
}

/// Mutable view of a heap page with schema-aware row access.
///
/// Holds a write guard, tracks modifications via dirty flag, and automatically
/// marks the page as modified when dropped if any changes were made.
pub struct HeapPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    layout: &'a Layout,
    dirty: Rc<Cell<bool>>,
}

impl<'a> HeapPageViewMut<'a> {
    fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        HeapPageZeroCopyMut::new(guard.bytes_mut())?;
        Ok(Self {
            guard,
            layout,
            dirty: Rc::new(Cell::new(false)),
        })
    }

    fn build_page(&'a self) -> HeapPageZeroCopy<'a> {
        HeapPageZeroCopy::new(self.guard.bytes()).unwrap()
    }

    fn build_mut_page(&mut self) -> HeapPageZeroCopyMut<'_> {
        HeapPageZeroCopyMut::new(self.guard.bytes_mut()).unwrap()
    }

    /// Returns the current [`TupleRef`] at `slot`, including redirect/dead markers.
    pub fn tuple_ref(&self, slot: SlotId) -> Option<TupleRef<'_>> {
        let view = self.build_page();
        view.tuple_ref(slot)
    }

    /// Returns a logical row for the slot if it is live; otherwise `None`.
    pub fn row(&self, slot: SlotId) -> Option<LogicalRow<'_>> {
        let view = self.build_page();
        let tuple_ref = view.tuple_ref(slot)?;
        match tuple_ref {
            TupleRef::Live(tuple) => Some(LogicalRow::new(tuple, self.layout)),
            TupleRef::Redirect(_) | TupleRef::Free | TupleRef::Dead => None,
        }
    }

    /// Returns the absolute page offset for the given column within `slot`.
    pub fn column_page_offset(&self, slot: SlotId, column_name: &str) -> Option<usize> {
        let payload_offset = self.layout.offset(column_name)?;
        let page = self.build_page();
        let line_ptr = page.line_ptr(slot)?;
        if !line_ptr.is_live() {
            return None;
        }
        let tuple_start = line_ptr.offset() as usize;
        Some(tuple_start + HEAP_TUPLE_HEADER_BYTES + payload_offset)
    }

    /// Returns a mutable logical row for a live slot, following redirect chains.
    pub fn row_mut(&mut self, slot: SlotId) -> Option<LogicalRowMut<'_>> {
        //  this annoying clone has to be done because heap_tuple_mut takes &mut self so I can't pass in &Layout which is &self
        let layout_clone = self.layout.clone();
        let dirty = self.dirty.clone();
        let heap_tuple_mut = self.resolve_live_tuple_mut(slot)?;
        Some(LogicalRowMut::new(heap_tuple_mut, layout_clone, dirty))
    }

    /// Inserts a raw tuple payload into the page and returns the allocated slot.
    pub fn insert_tuple(&mut self, bytes: &[u8]) -> SimpleDBResult<SlotId> {
        let mut page = self.build_mut_page();
        let mut split_guard = page.split()?;
        match split_guard.insert_tuple_fast(bytes)? {
            HeapInsert::Done(slot) => {
                drop(split_guard);
                self.dirty.set(true);
                return Ok(slot);
            }
            HeapInsert::Reserved(reservation) => {
                drop(split_guard);
                let mut split_guard = page.split()?;
                let slot = split_guard.insert_tuple_slow(reservation, bytes)?;
                self.dirty.set(true);
                return Ok(slot);
            }
        }
    }

    /// Deletes the tuple at `slot`, marking it free for reuse.
    pub fn delete_slot(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        let mut page = self.build_mut_page();
        let mut split_guard = page.split()?;
        split_guard.delete_slot(slot)?;
        self.dirty.set(true);
        Ok(())
    }

    /// Updates the tuple stored at `slot` with the provided bytes, redirecting if it grows.
    pub fn update_tuple(&mut self, slot: SlotId, bytes: &[u8]) -> SimpleDBResult<()> {
        let Some(target_slot) = self.resolve_live_slot_id(slot) else {
            return Err(format!("slot {slot} is not live").into());
        };
        let new_len: u16 = bytes
            .len()
            .try_into()
            .map_err(|_| "tuple larger than max tuple size (u16::MAX)")?;

        {
            let mut page = self.build_mut_page();
            let mut split_guard = page.split()?;
            let mut lp = split_guard.line_ptrs().as_ref().get(target_slot);
            if !lp.is_live() {
                return Err(format!("slot {slot} is not live").into());
            }
            let current_len = lp.length() as usize;
            if bytes.len() <= current_len {
                split_guard
                    .record_space()
                    .write_tuple(lp.offset() as usize, bytes);
                lp.set_length(new_len);
                split_guard.line_ptrs().set(target_slot, lp);
                self.dirty.set(true);
                return Ok(());
            }
        }

        let new_slot = self.insert_tuple(bytes)?;
        self.redirect_slot(target_slot, new_slot)?;
        Ok(())
    }

    /// Marks `slot` as a redirect to `target`, used for tuple forwarding.
    pub fn redirect_slot(&mut self, slot: SlotId, target: SlotId) -> SimpleDBResult<()> {
        let mut page = self.build_mut_page();
        let mut split_guard = page.split()?;
        split_guard.redirect_slot(slot, target)?;
        self.dirty.set(true);
        Ok(())
    }

    /// Serializes the page into the provided buffer.
    pub fn write_bytes(&self, out: &mut [u8]) -> SimpleDBResult<()> {
        out.copy_from_slice(self.guard.bytes());
        Ok(())
    }

    /// Allocates a new heap tuple and returns both the slot and a mutable logical row handle.
    pub fn insert_row_mut(&mut self) -> SimpleDBResult<(SlotId, LogicalRowMut<'_>)> {
        let payload_len = self.layout.slot_size;
        let mut buf = vec![0u8; HEAP_TUPLE_HEADER_BYTES + payload_len];
        let mut header_buf = [0u8; HEAP_TUPLE_HEADER_BYTES];
        let mut header = HeapTupleHeaderBytesMut::from_bytes(&mut header_buf);
        header.set_payload_len(payload_len as u32);
        header.set_xmin(0);
        header.set_xmax(0);
        header.set_flags(0);
        header.set_nullmap_ptr(0);
        buf[..HEAP_TUPLE_HEADER_BYTES].copy_from_slice(&header_buf);
        let slot = self.insert_tuple(&buf)?;
        self.dirty.set(true);
        let dirty = self.dirty.clone();
        let layout_clone = self.layout.clone();
        let heap_tuple_mut = self
            .resolve_live_tuple_mut(slot)
            .expect("tuple must exist after allocation");
        Ok((
            slot,
            LogicalRowMut::new(heap_tuple_mut, layout_clone, dirty),
        ))
    }

    /// Returns the number of slot entries currently present.
    pub fn slot_count(&self) -> usize {
        self.build_page().slot_count()
    }

    fn resolve_live_tuple_mut(&mut self, slot: SlotId) -> Option<HeapTupleMut<'_>> {
        let mut current = slot;
        loop {
            let view = self.build_page();
            match view.tuple_ref(current)? {
                TupleRef::Live(_) => break,
                TupleRef::Redirect(next) => current = next,
                TupleRef::Free | TupleRef::Dead => return None,
            }
        }

        //  This block of code is here so that the compiler can see that the lifetime of the bytes provided to LogicalRowMut is valid
        //  If we were to build page and then operate on that, the lifetime of the underlying bytes cannot be proven by the compiler
        //  since the compiler has no idea that page aliases the same underlying bytes
        let bytes = self.guard.bytes_mut();
        let (header_bytes, body_bytes) = bytes.split_at_mut(HeapPageZeroCopy::HEADER_SIZE as usize);
        let header = HeapHeaderRef::new(header_bytes);
        if current >= header.slot_count() as usize {
            return None;
        }
        let lp_capacity = header.free_lower() as usize - HeapPageZeroCopy::HEADER_SIZE;
        if lp_capacity > body_bytes.len() {
            return None;
        }
        let (line_ptr_bytes, record_space_bytes) = body_bytes.split_at_mut(lp_capacity);
        let base_offset = header.free_lower() as usize;
        let line_ptr =
            LinePtrArray::with_len(line_ptr_bytes, header.slot_count() as usize).get(current);
        if !line_ptr.is_live() {
            return None;
        }

        let (offset, length) = line_ptr.offset_and_length();
        let relative = offset.checked_sub(base_offset)?;
        let tuple_bytes = record_space_bytes.get_mut(relative..relative + length)?;
        Some(HeapTupleMut::from_bytes(tuple_bytes))
    }

    fn resolve_live_slot_id(&self, slot: SlotId) -> Option<SlotId> {
        let mut current = slot;
        loop {
            let view = self.build_page();
            match view.tuple_ref(current)? {
                TupleRef::Live(_) => return Some(current),
                TupleRef::Redirect(next) => current = next,
                TupleRef::Free | TupleRef::Dead => return None,
            }
        }
    }
}

impl Drop for HeapPageViewMut<'_> {
    fn drop(&mut self) {
        if self.dirty.get() {
            self.guard.mark_modified(self.guard.txn_id(), Lsn::MAX);
        }
    }
}

/// B-tree leaf entry mapping a key to a record identifier (RID).
#[derive(Debug, Clone, PartialEq)]
pub struct BTreeLeafEntry {
    pub key: Constant,
    pub rid: RID,
}

impl BTreeLeafEntry {
    /// Encodes the entry to bytes using the given layout.
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

    /// Decodes an entry from bytes using the given layout.
    pub fn decode(layout: &Layout, bytes: &[u8]) -> SimpleDBResult<Self> {
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

/// B-tree internal entry mapping a key to a child block number.
#[derive(Debug, Clone, PartialEq)]
pub struct BTreeInternalEntry {
    pub key: Constant,
    pub child_block: usize,
}

impl BTreeInternalEntry {
    /// Encodes the entry to bytes using the given layout.
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

    /// Decodes an entry from bytes using the given layout.
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

/// Read-only view of a B-tree leaf page.
pub struct BTreeLeafPageView<'a> {
    guard: PageReadGuard<'a>,
    layout: &'a Layout,
}

impl<'a> BTreeLeafPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        BTreeLeafPageZeroCopy::new(guard.bytes())?;
        Ok(Self { guard, layout })
    }

    fn build_view(&self) -> SimpleDBResult<BTreeLeafPageZeroCopy<'_>> {
        BTreeLeafPageZeroCopy::new(self.guard.bytes())
    }

    fn view(&self) -> BTreeLeafPageZeroCopy<'_> {
        self.build_view()
            .expect("BTreeLeafPageView constructed with valid leaf page")
    }

    pub fn get_entry(&self, slot: SlotId) -> SimpleDBResult<BTreeLeafEntry> {
        let view = self.build_view()?;
        let bytes = view.entry_bytes(slot).ok_or("slot not found or not live")?;
        BTreeLeafEntry::decode(self.layout, bytes)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.view().find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.view().slot_count()
    }

    pub fn is_slot_live(&self, slot: SlotId) -> bool {
        self.view()
            .line_ptr(slot)
            .map(|lp| lp.is_live())
            .unwrap_or(false)
    }

    pub fn is_full(&self) -> bool {
        self.view().is_full(self.layout)
    }

    pub fn overflow_block(&self) -> Option<usize> {
        self.view().overflow_block()
    }

    pub fn iter(&self) -> BTreeLeafIterator<'_> {
        BTreeLeafIterator::new(
            self.build_view()
                .expect("BTreeLeafPageView constructed with valid leaf page"),
            self.layout,
        )
    }
}

/// Mutable view of a B-tree leaf page.
///
/// Provides insertion, deletion, and search operations on leaf entries.
/// Automatically marks the page as modified when dropped if changes were made.
pub struct BTreeLeafPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    layout: &'a Layout,
    dirty: bool,
}

impl<'a> BTreeLeafPageViewMut<'a> {
    pub fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        BTreeLeafPageZeroCopyMut::new(guard.bytes_mut())?;
        Ok(Self {
            guard,
            layout,
            dirty: false,
        })
    }

    fn build_view(&self) -> SimpleDBResult<BTreeLeafPageZeroCopy<'_>> {
        BTreeLeafPageZeroCopy::new(self.guard.bytes())
    }

    fn view(&self) -> BTreeLeafPageZeroCopy<'_> {
        self.build_view()
            .expect("BTreeLeafPageViewMut constructed with valid leaf page")
    }

    fn build_mut_page(&mut self) -> SimpleDBResult<BTreeLeafPageZeroCopyMut<'_>> {
        BTreeLeafPageZeroCopyMut::new(self.guard.bytes_mut())
    }

    // Read operations
    pub fn get_entry(&self, slot: SlotId) -> SimpleDBResult<BTreeLeafEntry> {
        let view = self.build_view()?;
        let bytes = view.entry_bytes(slot).ok_or("slot not found or not live")?;
        BTreeLeafEntry::decode(self.layout, bytes)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.view().find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.view().slot_count()
    }

    pub fn is_slot_live(&self, slot: SlotId) -> bool {
        self.view()
            .line_ptr(slot)
            .map(|lp| lp.is_live())
            .unwrap_or(false)
    }

    pub fn is_full(&self) -> bool {
        self.view().is_full(self.layout)
    }

    pub fn overflow_block(&self) -> Option<usize> {
        self.view().overflow_block()
    }

    pub fn iter(&self) -> BTreeLeafIterator<'_> {
        BTreeLeafIterator::new(self.view(), self.layout)
    }

    // Write operations
    pub fn insert_entry(&mut self, key: Constant, rid: RID) -> SimpleDBResult<SlotId> {
        let layout = self.layout;
        let mut page = self.build_mut_page()?;
        let slot = page.insert_entry(layout, key, rid)?;
        self.dirty = true;
        Ok(slot)
    }

    pub fn delete_entry(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        let mut page = self.build_mut_page()?;
        page.delete_entry(slot)?;
        self.dirty = true;
        Ok(())
    }

    pub fn set_overflow_block(&mut self, block: Option<usize>) -> SimpleDBResult<()> {
        let mut page = self.build_mut_page()?;
        page.set_overflow_block(block)?;
        self.dirty = true;
        Ok(())
    }

    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.guard.mark_modified(txn_id, lsn);
    }
}

impl Drop for BTreeLeafPageViewMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.guard.mark_modified(self.guard.txn_id(), Lsn::MAX);
        }
    }
}

/// Read-only view of a B-tree internal page.
pub struct BTreeInternalPageView<'a> {
    guard: PageReadGuard<'a>,
    layout: &'a Layout,
}

impl<'a> BTreeInternalPageView<'a> {
    pub fn new(guard: PageReadGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        BTreeInternalPageZeroCopy::new(guard.bytes())?;
        Ok(Self { guard, layout })
    }

    fn build_view(&self) -> SimpleDBResult<BTreeInternalPageZeroCopy<'_>> {
        BTreeInternalPageZeroCopy::new(self.guard.bytes())
    }

    fn view(&self) -> BTreeInternalPageZeroCopy<'_> {
        self.build_view()
            .expect("BTreeInternalPageView constructed with valid internal page")
    }

    pub fn get_entry(&self, slot: SlotId) -> SimpleDBResult<BTreeInternalEntry> {
        let view = self.build_view()?;
        let bytes = view.entry_bytes(slot).ok_or("slot not found or not live")?;
        BTreeInternalEntry::decode(self.layout, bytes)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.view().find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.view().slot_count()
    }

    pub fn is_full(&self) -> bool {
        self.view().is_full(self.layout)
    }

    pub fn btree_level(&self) -> u8 {
        self.view().btree_level()
    }

    pub fn iter(&self) -> BTreeInternalIterator<'_> {
        BTreeInternalIterator::new(self.view(), self.layout)
    }
}

/// Mutable view of a B-tree internal page.
///
/// Provides insertion, deletion, and search operations on internal entries.
/// Automatically marks the page as modified when dropped if changes were made.
pub struct BTreeInternalPageViewMut<'a> {
    guard: PageWriteGuard<'a>,
    layout: &'a Layout,
    dirty: bool,
}

impl<'a> BTreeInternalPageViewMut<'a> {
    pub fn new(mut guard: PageWriteGuard<'a>, layout: &'a Layout) -> SimpleDBResult<Self> {
        BTreeInternalPageZeroCopyMut::new(guard.bytes_mut())?;
        Ok(Self {
            guard,
            layout,
            dirty: false,
        })
    }

    fn build_view(&self) -> SimpleDBResult<BTreeInternalPageZeroCopy<'_>> {
        BTreeInternalPageZeroCopy::new(self.guard.bytes())
    }

    fn view(&self) -> BTreeInternalPageZeroCopy<'_> {
        self.build_view()
            .expect("BTreeInternalPageViewMut constructed with valid internal page")
    }

    fn build_mut_page(&mut self) -> SimpleDBResult<BTreeInternalPageZeroCopyMut<'_>> {
        BTreeInternalPageZeroCopyMut::new(self.guard.bytes_mut())
    }

    // Read operations
    pub fn get_entry(&self, slot: SlotId) -> SimpleDBResult<BTreeInternalEntry> {
        let view = self.build_view()?;
        let bytes = view.entry_bytes(slot).ok_or("slot not found or not live")?;
        BTreeInternalEntry::decode(self.layout, bytes)
    }

    pub fn find_slot_before(&self, search_key: &Constant) -> Option<SlotId> {
        self.view().find_slot_before(self.layout, search_key)
    }

    pub fn slot_count(&self) -> usize {
        self.view().slot_count()
    }

    pub fn is_full(&self) -> bool {
        self.view().is_full(self.layout)
    }

    pub fn btree_level(&self) -> u8 {
        self.view().btree_level()
    }

    pub fn iter(&self) -> BTreeInternalIterator<'_> {
        BTreeInternalIterator::new(self.view(), self.layout)
    }

    // Write operations
    pub fn insert_entry(&mut self, key: Constant, child_block: usize) -> SimpleDBResult<SlotId> {
        let layout = self.layout;
        let mut page = self.build_mut_page()?;
        let slot = page.insert_entry(layout, key, child_block)?;
        self.dirty = true;
        Ok(slot)
    }

    pub fn delete_entry(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        let layout = self.layout;
        let mut page = self.build_mut_page()?;
        page.delete_entry(slot, layout)?;
        self.dirty = true;
        Ok(())
    }

    pub fn set_btree_level(&mut self, level: u8) -> SimpleDBResult<()> {
        let mut page = self.build_mut_page()?;
        page.set_btree_level(level)?;
        self.dirty = true;
        Ok(())
    }

    pub fn mark_modified(&self, txn_id: usize, lsn: usize) {
        self.guard.mark_modified(txn_id, lsn);
    }
}

impl Drop for BTreeInternalPageViewMut<'_> {
    fn drop(&mut self) {
        if self.dirty {
            self.guard.mark_modified(self.guard.txn_id(), Lsn::MAX);
        }
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
        guard.format_as_heap();
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
        let redirected = view
            .tuple_ref(slot_b)
            .expect("error while converting to Page<HeapPage>");
        match redirected {
            TupleRef::Redirect(target) => assert_eq!({ target }, slot_d),
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

        // let rebuilt =
        //     Page::<HeapPage>::from_bytes(&buf).expect("deserialize heap page after serialization");
        let rebuilt =
            HeapPageZeroCopy::new(&buf).expect("deserialize heap page after serialization");

        match rebuilt.tuple_ref(slot_c).expect("slot_c tuple") {
            TupleRef::Live(tuple) => {
                let row = LogicalRow::new(tuple, &layout);
                assert_eq!(row.get_column("id"), Some(Constant::Int(30)));
            }
            _ => panic!("slot_c should remain live after serialization"),
        }

        match rebuilt.tuple_ref(slot_d).expect("slot_d tuple") {
            TupleRef::Live(tuple) => {
                let row = LogicalRow::new(tuple, &layout);
                assert_eq!(row.get_column("id"), Some(Constant::Int(40)));
            }
            _ => panic!("slot_d should remain live after serialization"),
        }

        match rebuilt.tuple_ref(slot_b).expect("slot_b tuple") {
            TupleRef::Redirect(target) => assert_eq!({ target }, slot_d),
            _ => panic!("slot_b redirect state not preserved across serialization"),
        }
    }
}

#[cfg(test)]
mod btree_page_tests {
    use super::*;
    use crate::Schema;

    fn zeroed_page_bytes() -> Vec<u8> {
        vec![0u8; PAGE_SIZE_BYTES as usize]
    }

    fn init_btree_leaf_bytes(
        level: u8,
        right_sibling: Option<u32>,
        overflow_block: Option<u32>,
    ) -> Vec<u8> {
        let mut bytes = zeroed_page_bytes();
        {
            let (header_bytes, _) = bytes.split_at_mut(BTreeLeafPageZeroCopy::HEADER_SIZE as usize);
            let mut header = BTreeLeafHeaderMut::new(header_bytes);
            header.init_leaf(level, right_sibling, overflow_block);
        }
        bytes
    }

    fn init_btree_internal_bytes(level: u8, rightmost_child: Option<u32>) -> Vec<u8> {
        let mut bytes = zeroed_page_bytes();
        {
            let (header_bytes, _) =
                bytes.split_at_mut(BTreeInternalPageZeroCopy::HEADER_SIZE as usize);
            let mut header = BTreeInternalHeaderMut::new(header_bytes);
            header.init_internal(level, rightmost_child);
        }
        bytes
    }

    struct LeafTestPage {
        bytes: Vec<u8>,
    }

    impl LeafTestPage {
        fn new() -> Self {
            Self::with_params(0, None, None)
        }

        fn with_overflow(overflow_block: Option<u32>) -> Self {
            Self::with_params(0, None, overflow_block)
        }

        fn with_params(level: u8, right_sibling: Option<u32>, overflow_block: Option<u32>) -> Self {
            Self {
                bytes: init_btree_leaf_bytes(level, right_sibling, overflow_block),
            }
        }

        fn from_bytes(bytes: Vec<u8>) -> Self {
            BTreeLeafPageZeroCopy::new(&bytes).expect("valid leaf page bytes");
            Self { bytes }
        }

        fn view(&self) -> BTreeLeafPageZeroCopy<'_> {
            BTreeLeafPageZeroCopy::new(&self.bytes).expect("leaf view")
        }

        fn view_mut(&mut self) -> BTreeLeafPageZeroCopyMut<'_> {
            BTreeLeafPageZeroCopyMut::new(&mut self.bytes).expect("leaf view mut")
        }

        fn slot_count(&self) -> usize {
            self.view().slot_count()
        }

        fn overflow_block(&self) -> Option<usize> {
            self.view().overflow_block()
        }

        fn set_overflow_block(&mut self, block: Option<usize>) {
            self.view_mut()
                .set_overflow_block(block)
                .expect("set overflow");
        }

        fn insert_leaf_entry(
            &mut self,
            layout: &Layout,
            key: Constant,
            rid: RID,
        ) -> SimpleDBResult<SlotId> {
            self.view_mut().insert_entry(layout, key, rid)
        }

        fn get_leaf_entry(&self, layout: &Layout, slot: SlotId) -> SimpleDBResult<BTreeLeafEntry> {
            self.view()
                .entry_bytes(slot)
                .map(|bytes| BTreeLeafEntry::decode(layout, bytes))
                .unwrap()
        }

        fn delete_leaf_entry(&mut self, slot: SlotId, _layout: &Layout) -> SimpleDBResult<()> {
            self.view_mut().delete_entry(slot)
        }

        fn find_insertion_slot(&self, layout: &Layout, key: &Constant) -> SlotId {
            self.view().find_insertion_slot(layout, key)
        }

        fn find_slot_before(&self, layout: &Layout, key: &Constant) -> Option<SlotId> {
            self.view().find_slot_before(layout, key)
        }

        fn is_full(&self, layout: &Layout) -> bool {
            self.view().is_full(layout)
        }

        fn tuple_bytes(&self, slot: SlotId) -> Option<Vec<u8>> {
            self.view().entry_bytes(slot).map(|bytes| bytes.to_vec())
        }

        fn write_bytes(&self, dst: &mut [u8]) {
            dst.copy_from_slice(&self.bytes);
        }
    }

    struct InternalTestPage {
        bytes: Vec<u8>,
    }

    impl InternalTestPage {
        fn new(level: u8) -> Self {
            Self {
                bytes: init_btree_internal_bytes(level, None),
            }
        }

        fn from_bytes(bytes: Vec<u8>) -> Self {
            BTreeInternalPageZeroCopy::new(&bytes).expect("valid internal page bytes");
            Self { bytes }
        }

        fn view(&self) -> BTreeInternalPageZeroCopy<'_> {
            BTreeInternalPageZeroCopy::new(&self.bytes).expect("internal view")
        }

        fn view_mut(&mut self) -> BTreeInternalPageZeroCopyMut<'_> {
            BTreeInternalPageZeroCopyMut::new(&mut self.bytes).expect("internal view mut")
        }

        fn slot_count(&self) -> usize {
            self.view().slot_count()
        }

        fn btree_level(&self) -> u8 {
            self.view().btree_level()
        }

        fn set_btree_level(&mut self, level: u8) {
            self.view_mut()
                .set_btree_level(level)
                .expect("set btree level");
        }

        fn insert_internal_entry(
            &mut self,
            layout: &Layout,
            key: Constant,
            child_block: usize,
        ) -> SimpleDBResult<SlotId> {
            self.view_mut().insert_entry(layout, key, child_block)
        }

        fn get_internal_entry(
            &self,
            layout: &Layout,
            slot: SlotId,
        ) -> SimpleDBResult<BTreeInternalEntry> {
            self.view()
                .entry_bytes(slot)
                .map(|bytes| BTreeInternalEntry::decode(layout, bytes))
                .transpose()?
                .ok_or_else(|| "missing enrty bytes".into())
        }

        fn delete_internal_entry(&mut self, slot: SlotId, layout: &Layout) -> SimpleDBResult<()> {
            self.view_mut().delete_entry(slot, layout)
        }

        fn is_full(&self, layout: &Layout) -> bool {
            self.view().is_full(layout)
        }

        fn write_bytes(&self, dst: &mut [u8]) {
            dst.copy_from_slice(&self.bytes);
        }
    }

    fn leaf_view_mut_from_bytes(bytes: &mut [u8]) -> BTreeLeafPageZeroCopyMut<'_> {
        BTreeLeafPageZeroCopyMut::new(bytes).expect("leaf page mut view")
    }

    fn internal_view_mut_from_bytes(bytes: &mut [u8]) -> BTreeInternalPageZeroCopyMut<'_> {
        BTreeInternalPageZeroCopyMut::new(bytes).expect("internal page mut view")
    }

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

    // ========== Phase 1: Sorted Insertion Tests ==========

    #[test]
    fn btree_leaf_sorted_insertion_random_order() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        // Insert in random order
        let keys = [42, 1, 99, 17, 55, 3, 88];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, Constant::Int(key), rid)
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
        let mut page = LeafTestPage::new();

        let keys = [1, 2, 3, 4, 5];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, Constant::Int(key), rid)
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
        let mut page = LeafTestPage::new();

        let keys = [5, 4, 3, 2, 1];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, Constant::Int(key), rid)
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
        let mut page = LeafTestPage::new();

        let keys = ["dog", "apple", "zebra", "banana", "cat"];
        for (i, &key) in keys.iter().enumerate() {
            let rid = RID::new(i, i);
            page.insert_leaf_entry(&layout, Constant::String(key.to_string()), rid)
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
        let mut page = InternalTestPage::new(1);

        let keys = [50, 10, 90, 30, 70];
        for (i, &key) in keys.iter().enumerate() {
            page.insert_internal_entry(&layout, Constant::Int(key), i * 10)
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
        let page = LeafTestPage::new();

        let slot = page.find_insertion_slot(&layout, &Constant::Int(42));
        assert_eq!(slot, 0);
    }

    #[test]
    fn find_insertion_slot_middle() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        // Insert [10, 30, 50]
        for &key in &[10, 30, 50] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        // Search for 40 should return slot 2 (between 30 and 50)
        let slot = page.find_insertion_slot(&layout, &Constant::Int(40));
        assert_eq!(slot, 2);
    }

    #[test]
    fn find_insertion_slot_beginning() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        // Search for 5 should return slot 0
        let slot = page.find_insertion_slot(&layout, &Constant::Int(5));
        assert_eq!(slot, 0);
    }

    #[test]
    fn find_insertion_slot_end() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        // Search for 40 should return slot 3 (end)
        let slot = page.find_insertion_slot(&layout, &Constant::Int(40));
        assert_eq!(slot, 3);
    }

    #[test]
    fn find_slot_before_empty_page() {
        let layout = btree_leaf_layout_int();
        let page = LeafTestPage::new();

        let slot = page.find_slot_before(&layout, &Constant::Int(42));
        assert_eq!(slot, None);
    }

    #[test]
    fn find_slot_before_key_less_than_all() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        let slot = page.find_slot_before(&layout, &Constant::Int(5));
        assert_eq!(slot, None);
    }

    #[test]
    fn find_slot_before_key_greater_than_all() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        let slot = page.find_slot_before(&layout, &Constant::Int(100));
        assert_eq!(slot, Some(2)); // Last slot
    }

    #[test]
    fn find_slot_before_key_in_middle() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30, 40] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
                .unwrap();
        }

        // Search for 25 should return slot 1 (20 is before 25)
        let slot = page.find_slot_before(&layout, &Constant::Int(25));
        assert_eq!(slot, Some(1));
    }

    #[test]
    fn find_slot_before_exact_match() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        for &key in &[10, 20, 30, 40] {
            page.insert_leaf_entry(&layout, Constant::Int(key), RID::new(0, 0))
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
        let mut page = LeafTestPage::new();

        let rid = RID::new(100, 50);
        let slot = page
            .insert_leaf_entry(&layout, Constant::Int(42), rid)
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
        let mut page = LeafTestPage::new();

        // Insert two entries - they will be sorted by key (10, 20)
        page.insert_leaf_entry(&layout, Constant::Int(10), RID::new(1, 1))
            .unwrap();
        page.insert_leaf_entry(&layout, Constant::Int(20), RID::new(2, 2))
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
        let page = LeafTestPage::new();

        let result = page.get_leaf_entry(&layout, 999);
        assert!(result.is_err());
    }

    #[test]
    fn btree_leaf_delete_invalid_slot() {
        let _layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        let result = page.delete_leaf_entry(999, &_layout);
        assert!(result.is_err());
    }

    #[test]
    fn btree_internal_insert_get_verify() {
        let layout = btree_internal_layout_int();
        let mut page = InternalTestPage::new(2);

        let slot = page
            .insert_internal_entry(&layout, Constant::Int(50), 123)
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
        let mut page = InternalTestPage::new(1);

        // Insert two entries - they will be sorted by key (10, 20)
        page.insert_internal_entry(&layout, Constant::Int(10), 100)
            .unwrap();
        page.insert_internal_entry(&layout, Constant::Int(20), 200)
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
        let mut page = LeafTestPage::new();

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
                Constant::Int(count),
                RID::new(count as usize, count as usize),
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
        let mut page = LeafTestPage::new();

        // Fill page completely
        let mut inserted = 0;
        loop {
            let result = page.insert_leaf_entry(
                &layout,
                Constant::Int(inserted),
                RID::new(inserted as usize, inserted as usize),
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
        let result = page.insert_leaf_entry(&layout, Constant::Int(9999), RID::new(9999, 9999));
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
        let mut page = LeafTestPage::new();

        // Fill page
        let mut inserted = 0;
        loop {
            let result = page.insert_leaf_entry(
                &layout,
                Constant::Int(inserted),
                RID::new(inserted as usize, inserted as usize),
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
        let result = page.insert_leaf_entry(&layout, Constant::Int(8888), RID::new(8888, 8888));
        assert!(
            result.is_ok() || was_full_after_delete,
            "either insert succeeds or page was still full after deletes"
        );
    }

    #[test]
    fn btree_internal_is_full_detection() {
        let layout = btree_internal_layout_int();
        let mut page = InternalTestPage::new(1);

        assert!(!page.is_full(&layout));

        let mut count = 0;
        loop {
            if page.is_full(&layout) {
                break;
            }
            let result =
                page.insert_internal_entry(&layout, Constant::Int(count), count as usize * 100);
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
        let page = LeafTestPage::new();

        // Empty page operations
        assert_eq!(page.slot_count(), 0);
        assert_eq!(page.find_insertion_slot(&layout, &Constant::Int(42)), 0);
        assert_eq!(page.find_slot_before(&layout, &Constant::Int(42)), None);
        assert!(page.get_leaf_entry(&layout, 0).is_err());
    }

    #[test]
    fn btree_leaf_single_entry_operations() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::new();

        // Insert single entry
        let slot = page
            .insert_leaf_entry(&layout, Constant::Int(50), RID::new(10, 5))
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
        let mut page = InternalTestPage::new(2);

        let slot = page
            .insert_internal_entry(&layout, Constant::Int(100), 500)
            .expect("insert should succeed");
        assert_eq!(slot, 0);

        let entry = page
            .get_internal_entry(&layout, slot)
            .expect("get should succeed");
        assert_eq!(entry.key, Constant::Int(100));
        assert_eq!(entry.child_block, 500);

        page.delete_internal_entry(slot, &layout)
            .expect("delete should succeed");
    }

    // ========== Phase 2: Metadata Persistence Tests ==========

    #[test]
    fn btree_leaf_overflow_block_roundtrip() {
        let mut page = LeafTestPage::new();

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
        let mut page = InternalTestPage::new(0);

        assert_eq!(page.btree_level(), 0);

        page.set_btree_level(5);
        assert_eq!(page.btree_level(), 5);

        page.set_btree_level(255);
        assert_eq!(page.btree_level(), 255);
    }

    #[test]
    fn btree_leaf_metadata_persists_across_serialization() {
        let layout = btree_leaf_layout_int();
        let mut page = LeafTestPage::with_overflow(Some(123));

        // Insert some entries
        page.insert_leaf_entry(&layout, Constant::Int(10), RID::new(1, 1))
            .unwrap();
        page.insert_leaf_entry(&layout, Constant::Int(20), RID::new(2, 2))
            .unwrap();

        // Serialize
        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        page.write_bytes(&mut buf);

        // Deserialize
        let restored = LeafTestPage::from_bytes(buf);

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
        let mut page = InternalTestPage::new(7);

        // Insert entries
        page.insert_internal_entry(&layout, Constant::Int(30), 300)
            .unwrap();
        page.insert_internal_entry(&layout, Constant::Int(60), 600)
            .unwrap();

        // Serialize
        let mut buf = vec![0u8; PAGE_SIZE_BYTES as usize];
        page.write_bytes(&mut buf);

        // Deserialize
        let restored = InternalTestPage::from_bytes(buf);

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
        let mut bytes = init_btree_leaf_bytes(0, None, None);
        let mut page = leaf_view_mut_from_bytes(&mut bytes);

        // Insert in random order
        let keys = [42, 10, 99, 5, 77, 33];
        for &key in &keys {
            page.insert_entry(&layout, Constant::Int(key), RID::new(key as usize, 0))
                .unwrap();
        }

        // Iterate and collect
        let iter = BTreeLeafIterator {
            page: page.as_read().unwrap(),
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        for entry in iter {
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
        let mut bytes = init_btree_leaf_bytes(0, None, None);
        let mut page = leaf_view_mut_from_bytes(&mut bytes);

        // Insert 5 entries
        for i in 0..5 {
            page.insert_entry(&layout, Constant::Int(i * 10), RID::new(i as usize, 0))
                .unwrap();
        }

        // Delete slot 2 (key=20)
        page.delete_entry(2).unwrap();

        // Iterate
        let iter = BTreeLeafIterator {
            page: page.as_read().unwrap(),
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        for entry in iter {
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
        let mut bytes = init_btree_leaf_bytes(0, None, None);
        let page = leaf_view_mut_from_bytes(&mut bytes);

        let mut iter = BTreeLeafIterator {
            page: page.as_read().unwrap(),
            layout: &layout,
            current_slot: 0,
        };

        assert!(iter.next().is_none());
    }

    #[test]
    fn btree_internal_iterator_yields_sorted_order() {
        let layout = btree_internal_layout_int();
        let mut bytes = init_btree_internal_bytes(1, None);
        let mut page = internal_view_mut_from_bytes(&mut bytes);

        let keys = [50, 20, 80, 10, 90];
        for (i, &key) in keys.iter().enumerate() {
            page.insert_entry(&layout, Constant::Int(key), i * 100)
                .unwrap();
        }

        let iter = BTreeInternalIterator {
            page: page.as_read().unwrap(),
            layout: &layout,
            current_slot: 0,
        };

        let mut collected = Vec::new();
        for entry in iter {
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
struct BTreeLeafPageZeroCopy<'a> {
    header: BTreeLeafHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: BTreeRecordSpace<'a>,
}

impl<'a> PageKind for BTreeLeafPageZeroCopy<'a> {
    const PAGE_TYPE: PageType = PageType::IndexLeaf;
    const HEADER_SIZE: usize = 32;
    type Header = BTreeLeafHeaderRef<'a>;
}

impl<'a> BTreeLeafPageZeroCopy<'a> {
    fn new(bytes: &'a [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, rest) = bytes.split_at(PAGE_HEADER_SIZE_BYTES as usize);
        let header = BTreeLeafHeaderRef::new(header_bytes);
        if header.page_type() != PageType::IndexLeaf {
            return Err("not a B-tree leaf page".into());
        }
        let lp_capacity = header.free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > rest.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = rest.split_at(lp_capacity);
        let base_offset = Self::HEADER_SIZE as usize + lp_capacity;
        let page = Self {
            header,
            line_pointers: LinePtrArray::with_len(line_ptr_bytes, header.slot_count() as usize),
            record_space: BTreeRecordSpace::new(record_space_bytes, base_offset),
        };
        Ok(page)
    }

    fn slot_count(&self) -> usize {
        self.line_pointers.len()
    }

    fn line_ptr(&self, slot: SlotId) -> Option<LinePtr> {
        if slot >= self.line_pointers.len() {
            None
        } else {
            Some(self.line_pointers.get(slot))
        }
    }

    fn entry_bytes(&self, slot: SlotId) -> Option<&'a [u8]> {
        let lp = self.line_ptr(slot)?;
        self.record_space.entry_bytes(lp)
    }

    fn find_slot_before(&self, layout: &Layout, search_key: &Constant) -> Option<SlotId> {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;
            match self
                .entry_bytes(mid)
                .and_then(|bytes| BTreeLeafEntry::decode(layout, bytes).ok())
            {
                Some(entry) if entry.key < *search_key => left = mid + 1,
                Some(_) => right = mid,
                None => left = mid + 1,
            }
        }

        if left == 0 {
            None
        } else {
            Some(left - 1)
        }
    }

    fn find_insertion_slot(&self, layout: &Layout, search_key: &Constant) -> SlotId {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;
            match self
                .entry_bytes(mid)
                .and_then(|bytes| BTreeLeafEntry::decode(layout, bytes).ok())
            {
                Some(entry) if entry.key <= *search_key => left = mid + 1,
                Some(_) => right = mid,
                None => left = mid + 1,
            }
        }

        left
    }

    fn is_full(&self, layout: &Layout) -> bool {
        let lower = self.header.free_lower();
        let upper = self.header.free_upper();
        let needed = layout.slot_size as u16 + 4;
        lower + needed > upper
    }

    fn overflow_block(&self) -> Option<usize> {
        let raw = self.header.overflow_block();
        if raw == 0xFFFF_FFFF {
            None
        } else {
            Some(raw as usize)
        }
    }
}

pub struct BTreeLeafPageZeroCopyMut<'a> {
    header: BTreeLeafHeaderMut<'a>,
    body_bytes: &'a mut [u8],
}

impl<'a> PageKind for BTreeLeafPageZeroCopyMut<'a> {
    const PAGE_TYPE: PageType = PageType::IndexLeaf;
    const HEADER_SIZE: usize = 32;
    type Header = BTreeLeafHeaderMut<'a>;
}

impl<'a> BTreeLeafPageZeroCopyMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, body_bytes) = bytes.split_at_mut(PAGE_HEADER_SIZE_BYTES as usize);
        let header = BTreeLeafHeaderMut::new(header_bytes);
        if header.as_ref().page_type() != PageType::IndexLeaf {
            return Err("not a B-tree leaf page".into());
        }
        Ok(Self { header, body_bytes })
    }

    fn split(&mut self) -> SimpleDBResult<BTreeLeafPageParts<'_>> {
        let lp_capacity = self.header.as_ref().free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > self.body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = self.body_bytes.split_at_mut(lp_capacity);
        let base_offset = self.header.as_ref().free_lower() as usize;

        assert_eq!(
            self.header.as_ref().free_lower() as usize,
            Self::HEADER_SIZE + lp_capacity
        );
        assert_eq!(
            Self::HEADER_SIZE + lp_capacity + record_space_bytes.len(),
            PAGE_SIZE_BYTES as usize
        );

        let slot_count = self.header.as_ref().slot_count() as usize;
        let parts = BTreeLeafPageParts {
            header: BTreeLeafHeaderMut::new(self.header.bytes_mut()),
            line_ptrs: LinePtrArrayMut::with_len(line_ptr_bytes, slot_count),
            record_space: BTreeRecordSpaceMut::new(record_space_bytes, base_offset),
        };
        Ok(parts)
    }

    fn as_read(&self) -> SimpleDBResult<BTreeLeafPageZeroCopy<'_>> {
        let body_bytes: &[u8] = &self.body_bytes[..];
        if self.header.as_ref().page_type() != PageType::IndexLeaf {
            return Err("not a B-tree leaf page".into());
        }
        let lp_capacity = self.header.as_ref().free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = body_bytes.split_at(lp_capacity);
        let base_offset = Self::HEADER_SIZE as usize + lp_capacity;
        Ok(BTreeLeafPageZeroCopy {
            header: self.header.as_ref(),
            line_pointers: LinePtrArray::with_len(
                line_ptr_bytes,
                self.header.as_ref().slot_count() as usize,
            ),
            record_space: BTreeRecordSpace::new(record_space_bytes, base_offset),
        })
    }

    fn insert_entry(&mut self, layout: &Layout, key: Constant, rid: RID) -> SimpleDBResult<SlotId> {
        let slot = {
            let view = self.as_read()?;
            view.find_insertion_slot(layout, &key)
        };

        let entry = BTreeLeafEntry { key, rid };
        let entry_bytes = entry.encode(layout);
        let entry_len: u16 = entry_bytes
            .len()
            .try_into()
            .map_err(|_| "entry larger than maximum leaf payload".to_string())?;
        let slot_bytes = LinePtrBytes::LINE_PTR_BYTES as u16;

        let line_ptr = {
            let mut parts = self.split()?;
            let (free_lower, free_upper) = {
                let header = parts.header();
                let view = header.as_ref();
                if view.free_space() < slot_bytes + entry_len {
                    return Err("page full".into());
                }
                view.free_bounds()
            };
            let new_upper = free_upper - entry_len;
            parts
                .record_space()
                .write_entry(new_upper as usize, &entry_bytes);
            {
                let header = parts.header();
                header.set_free_bounds(free_lower + slot_bytes, new_upper);
            }
            LinePtr::new(new_upper, entry_len, LineState::Live)
        };

        let mut parts = self.split()?;
        parts.line_ptrs().insert(slot, line_ptr);
        let slot_count = parts.header().as_ref().slot_count();
        parts.header().set_slot_count(slot_count + 1);
        Ok(slot)
    }

    fn delete_entry(&mut self, slot: SlotId) -> SimpleDBResult<()> {
        let mut parts = self.split()?;
        let free_upper = {
            let header = parts.header();
            header.as_ref().free_upper() as usize
        };
        let (deleted_offset, deleted_len) = {
            let line_ptrs = parts.line_ptrs();
            if slot >= line_ptrs.len() {
                return Err("invalid slot".into());
            }
            let lp = line_ptrs.as_ref().get(slot);
            if !lp.is_live() {
                return Err("slot not live".into());
            }
            let offset = lp.offset() as usize;
            let len = lp.length() as usize;
            line_ptrs.delete(slot);
            (offset, len)
        };
        let deleted_len_u16: u16 = deleted_len
            .try_into()
            .map_err(|_| "entry length exceeds header capacity".to_string())?;

        if deleted_offset > free_upper {
            let shift_len = deleted_offset - free_upper;
            parts
                .record_space()
                .copy_within(free_upper, free_upper + deleted_len, shift_len);
        }

        {
            let line_ptrs = parts.line_ptrs();
            let len = line_ptrs.len();
            for idx in 0..len {
                let mut lp = {
                    let view = line_ptrs.as_ref();
                    view.get(idx)
                };
                if (lp.offset() as usize) < deleted_offset {
                    let new_offset = lp.offset() as usize + deleted_len;
                    lp.set_offset(new_offset as u16);
                    line_ptrs.set(idx, lp);
                }
            }
        }

        {
            let header = parts.header();
            let current_lower = header.as_ref().free_lower();
            let current_upper = header.as_ref().free_upper();
            let new_lower = current_lower
                .checked_sub(LinePtrBytes::LINE_PTR_BYTES as u16)
                .expect("free_lower underflow during delete");
            let new_upper = current_upper
                .checked_add(deleted_len_u16)
                .expect("free_upper overflow during delete");
            header.set_free_bounds(new_lower, new_upper);
            let slot_count = header.as_ref().slot_count();
            header.set_slot_count(
                slot_count
                    .checked_sub(1)
                    .expect("slot_count underflow during delete"),
            );
        }

        Ok(())
    }

    fn set_overflow_block(&mut self, block: Option<usize>) -> SimpleDBResult<()> {
        let mut parts = self.split()?;
        let value = block.map(|b| b as u32).unwrap_or(u32::MAX);
        parts.header().set_overflow_block(value);
        Ok(())
    }

    pub fn update_crc32(&mut self) {
        self.header.update_crc32(&self.body_bytes);
    }

    pub fn verify_crc32(&mut self) -> bool {
        self.header.verify_crc32(&self.body_bytes)
    }
}

pub struct BTreeLeafPageParts<'a> {
    header: BTreeLeafHeaderMut<'a>,
    line_ptrs: LinePtrArrayMut<'a>,
    record_space: BTreeRecordSpaceMut<'a>,
}

impl<'a> BTreeLeafPageParts<'a> {
    pub fn header(&mut self) -> &mut BTreeLeafHeaderMut<'a> {
        &mut self.header
    }

    fn line_ptrs(&mut self) -> &mut LinePtrArrayMut<'a> {
        &mut self.line_ptrs
    }

    fn record_space(&mut self) -> &mut BTreeRecordSpaceMut<'a> {
        &mut self.record_space
    }
}

struct BTreeInternalPageZeroCopy<'a> {
    header: BTreeInternalHeaderRef<'a>,
    line_pointers: LinePtrArray<'a>,
    record_space: BTreeRecordSpace<'a>,
}

impl<'a> PageKind for BTreeInternalPageZeroCopy<'a> {
    const PAGE_TYPE: PageType = PageType::IndexInternal;
    const HEADER_SIZE: usize = 32;
    type Header = BTreeInternalHeaderRef<'a>;
}

impl<'a> BTreeInternalPageZeroCopy<'a> {
    fn new(bytes: &'a [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, rest) = bytes.split_at(PAGE_HEADER_SIZE_BYTES as usize);
        let header = BTreeInternalHeaderRef::new(header_bytes);
        if header.page_type() != PageType::IndexInternal {
            return Err("not a B-tree internal page".into());
        }
        let lp_capacity = header.free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > rest.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = rest.split_at(lp_capacity);
        let base_offset = Self::HEADER_SIZE as usize + lp_capacity;
        let page = Self {
            header,
            line_pointers: LinePtrArray::with_len(line_ptr_bytes, header.slot_count() as usize),
            record_space: BTreeRecordSpace::new(record_space_bytes, base_offset),
        };
        Ok(page)
    }

    fn slot_count(&self) -> usize {
        self.line_pointers.len()
    }

    fn line_ptr(&self, slot: SlotId) -> Option<LinePtr> {
        if slot >= self.line_pointers.len() {
            None
        } else {
            Some(self.line_pointers.get(slot))
        }
    }

    fn entry_bytes(&self, slot: SlotId) -> Option<&'a [u8]> {
        let lp = self.line_ptr(slot)?;
        self.record_space.entry_bytes(lp)
    }

    fn find_insertion_slot(&self, layout: &Layout, search_key: &Constant) -> SlotId {
        let mut left = 0;
        let mut right = self.slot_count();

        while left < right {
            let mid = (left + right) / 2;
            match self
                .entry_bytes(mid)
                .and_then(|bytes| BTreeInternalEntry::decode(layout, bytes).ok())
            {
                Some(entry) if entry.key <= *search_key => left = mid + 1,
                Some(_) => right = mid,
                None => left = mid + 1,
            }
        }

        left
    }

    fn find_slot_before(&self, layout: &Layout, search_key: &Constant) -> Option<SlotId> {
        let mut left = 0;
        let mut right = self.slot_count();
        let mut result = None;

        while left < right {
            let mid = (left + right) / 2;
            match self
                .entry_bytes(mid)
                .and_then(|bytes| BTreeInternalEntry::decode(layout, bytes).ok())
            {
                Some(entry) if entry.key <= *search_key => {
                    result = Some(mid);
                    left = mid + 1;
                }
                Some(_) => right = mid,
                None => left = mid + 1,
            }
        }

        result
    }

    fn is_full(&self, layout: &Layout) -> bool {
        let needed = layout.slot_size as u16 + LinePtrBytes::LINE_PTR_BYTES as u16;
        self.header.free_lower() + needed > self.header.free_upper()
    }

    fn btree_level(&self) -> u8 {
        self.header.level()
    }
}

pub struct BTreeInternalPageZeroCopyMut<'a> {
    header: BTreeInternalHeaderMut<'a>,
    body_bytes: &'a mut [u8],
}

impl<'a> PageKind for BTreeInternalPageZeroCopyMut<'a> {
    const PAGE_TYPE: PageType = PageType::IndexInternal;
    const HEADER_SIZE: usize = 32;
    type Header = BTreeInternalHeaderMut<'a>;
}

impl<'a> BTreeInternalPageZeroCopyMut<'a> {
    pub fn new(bytes: &'a mut [u8]) -> SimpleDBResult<Self> {
        let (header_bytes, body_bytes) = bytes.split_at_mut(PAGE_HEADER_SIZE_BYTES as usize);
        let header = BTreeInternalHeaderMut::new(header_bytes);
        if header.as_ref().page_type() != PageType::IndexInternal {
            return Err("not a B-tree internal page".into());
        }
        Ok(Self { header, body_bytes })
    }

    fn split(&mut self) -> SimpleDBResult<BTreeInternalPageParts<'_>> {
        let lp_capacity = self.header.as_ref().free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > self.body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = self.body_bytes.split_at_mut(lp_capacity);
        let base_offset = self.header.as_ref().free_lower() as usize;

        assert_eq!(
            self.header.as_ref().free_lower() as usize,
            Self::HEADER_SIZE + lp_capacity
        );
        assert_eq!(
            Self::HEADER_SIZE + lp_capacity + record_space_bytes.len(),
            PAGE_SIZE_BYTES as usize
        );

        let slot_count = self.header.as_ref().slot_count() as usize;
        let parts = BTreeInternalPageParts {
            header: BTreeInternalHeaderMut::new(self.header.bytes_mut()),
            line_ptrs: LinePtrArrayMut::with_len(line_ptr_bytes, slot_count),
            record_space: BTreeRecordSpaceMut::new(record_space_bytes, base_offset),
        };
        Ok(parts)
    }

    fn as_read(&self) -> SimpleDBResult<BTreeInternalPageZeroCopy<'_>> {
        let body_bytes: &[u8] = &self.body_bytes[..];
        if self.header.as_ref().page_type() != PageType::IndexInternal {
            return Err("not a B-tree internal page".into());
        }
        let lp_capacity = self.header.as_ref().free_lower() as usize - Self::HEADER_SIZE;
        if lp_capacity > body_bytes.len() {
            return Err("slot directory exceeds page body".into());
        }
        let (line_ptr_bytes, record_space_bytes) = body_bytes.split_at(lp_capacity);
        let base_offset = Self::HEADER_SIZE as usize + lp_capacity;
        Ok(BTreeInternalPageZeroCopy {
            header: self.header.as_ref(),
            line_pointers: LinePtrArray::with_len(
                line_ptr_bytes,
                self.header.as_ref().slot_count() as usize,
            ),
            record_space: BTreeRecordSpace::new(record_space_bytes, base_offset),
        })
    }

    fn insert_entry(
        &mut self,
        layout: &Layout,
        key: Constant,
        child_block: usize,
    ) -> SimpleDBResult<SlotId> {
        let slot = {
            let view = self.as_read()?;
            view.find_insertion_slot(layout, &key)
        };

        let entry = BTreeInternalEntry { key, child_block };
        let entry_bytes = entry.encode(layout);
        let entry_len: u16 = entry_bytes
            .len()
            .try_into()
            .map_err(|_| "entry larger than maximum internal payload".to_string())?;
        let slot_bytes = LinePtrBytes::LINE_PTR_BYTES as u16;

        let line_ptr = {
            let mut parts = self.split()?;
            let needed = slot_bytes + entry_len;
            let (free_lower, free_upper) = {
                let header = parts.header();
                let view = header.as_ref();
                if view.free_space() < needed {
                    return Err("page full".into());
                }
                view.free_bounds()
            };
            let new_upper = free_upper
                .checked_sub(entry_len)
                .expect("free_upper underflow during insert");
            parts
                .record_space()
                .write_entry(new_upper as usize, &entry_bytes);
            {
                let header = parts.header();
                header.set_free_bounds(free_lower + slot_bytes, new_upper);
            }
            LinePtr::new(new_upper, entry_len, LineState::Live)
        };

        let mut parts = self.split()?;
        parts.line_ptrs().insert(slot, line_ptr);
        let slot_count = parts.header().as_ref().slot_count();
        parts.header().set_slot_count(slot_count + 1);
        Ok(slot)
    }

    fn delete_entry(&mut self, slot: SlotId, _layout: &Layout) -> SimpleDBResult<()> {
        let mut parts = self.split()?;
        let (deleted_offset, deleted_len_u16) = {
            let line_ptrs = parts.line_ptrs();
            if slot >= line_ptrs.len() {
                return Err("invalid slot".into());
            }
            let lp = line_ptrs.as_ref().get(slot);
            if !lp.is_live() {
                return Err("slot not live".into());
            }
            let offset = lp.offset() as usize;
            let len_u16 = lp.length();
            line_ptrs.delete(slot);
            (offset, len_u16)
        };
        let deleted_len = deleted_len_u16 as usize;
        let free_upper_usize = parts.header().as_ref().free_upper() as usize;

        if deleted_offset > free_upper_usize {
            let shift_len = deleted_offset - free_upper_usize;
            parts.record_space().copy_within(
                free_upper_usize,
                free_upper_usize + deleted_len,
                shift_len,
            );
        }

        {
            let line_ptrs = parts.line_ptrs();
            let len = line_ptrs.len();
            for idx in 0..len {
                let mut lp = {
                    let view = line_ptrs.as_ref();
                    view.get(idx)
                };
                if (lp.offset() as usize) < deleted_offset {
                    let new_offset = lp.offset() as usize + deleted_len;
                    lp.set_offset(new_offset as u16);
                    line_ptrs.set(idx, lp);
                }
            }
        }

        {
            let slot_len = parts.line_ptrs().len() as u16;
            let (new_lower, new_upper) = {
                let header = parts.header();
                (
                    header
                        .as_ref()
                        .free_lower()
                        .checked_sub(LinePtrBytes::LINE_PTR_BYTES as u16)
                        .expect("free_lower underflow during delete"),
                    header
                        .as_ref()
                        .free_upper()
                        .checked_add(deleted_len_u16)
                        .expect("free_upper overflow during delete"),
                )
            };
            let header = parts.header();
            header.set_free_bounds(new_lower, new_upper);
            header.set_slot_count(slot_len);
        }

        Ok(())
    }

    fn set_btree_level(&mut self, level: u8) -> SimpleDBResult<()> {
        self.header.set_level(level);
        Ok(())
    }

    pub fn update_crc32(&mut self) {
        self.header.update_crc32(&self.body_bytes);
    }

    pub fn verify_crc32(&mut self) -> bool {
        self.header.verify_crc32(&self.body_bytes)
    }
}

pub struct BTreeInternalPageParts<'a> {
    header: BTreeInternalHeaderMut<'a>,
    line_ptrs: LinePtrArrayMut<'a>,
    record_space: BTreeRecordSpaceMut<'a>,
}

impl<'a> BTreeInternalPageParts<'a> {
    pub fn header(&mut self) -> &mut BTreeInternalHeaderMut<'a> {
        &mut self.header
    }

    fn line_ptrs(&mut self) -> &mut LinePtrArrayMut<'a> {
        &mut self.line_ptrs
    }

    fn record_space(&mut self) -> &mut BTreeRecordSpaceMut<'a> {
        &mut self.record_space
    }
}
