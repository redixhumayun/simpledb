#[repr(u8)]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PageType {
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
enum LineState {
    Free = 0,
    Live = 1,
    Dead = 2,
    Redirect = 3,
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

    fn state(&self) -> u8 {
        (self.0 & 0x000F) as u8
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

    fn mark_free(&mut self) {
        self.set_state(LineState::Free);
    }

    fn mark_live(&mut self) {
        self.set_state(LineState::Live);
    }

    fn mark_dead(&mut self) {
        self.set_state(LineState::Dead);
    }

    fn mark_redirect(&mut self) {
        self.set_state(LineState::Redirect);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offset_length_state_round_trip() {
        let mut lp = LinePtr(0);
        lp.set_offset(0x1234);
        lp.set_length(0x0567);
        lp.set_state(LineState::Live);

        assert_eq!(lp.offset(), 0x1234);
        assert_eq!(lp.length(), 0x0567);
        assert_eq!(lp.state(), LineState::Live as u8);
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
        assert_eq!(lp.state(), LineState::Dead as u8);
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
        assert_eq!(lp.state(), LineState::Live as u8);
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
        assert_eq!(lp.state(), LineState::Redirect as u8);
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
        assert_eq!(lp.state(), 0);

        // new one has changes
        assert_eq!(lp2.offset(), 0x3333);
        assert_eq!(lp2.length(), 0x0345);
        assert_eq!(lp2.state(), LineState::Live as u8);
    }

    #[test]
    fn mark_helpers_update_state() {
        let mut lp = LinePtr(0);

        lp.mark_live();
        assert_eq!(lp.state(), LineState::Live as u8);

        lp.mark_dead();
        assert_eq!(lp.state(), LineState::Dead as u8);

        lp.mark_free();
        assert_eq!(lp.state(), LineState::Free as u8);

        lp.mark_redirect();
        assert_eq!(lp.state(), LineState::Redirect as u8);
    }
}

struct Page {
    header: PageHeader,
    line_pointers: Vec<LinePtr>,
    record_space: Vec<u8>,
}

impl Page {
    fn new(page_type: PageType) -> Self {
        Self {
            header: PageHeader::new(page_type),
            line_pointers: Vec::new(),
            record_space: Vec::new(),
        }
    }
}
