use std::{error::Error, sync::Arc};

#[cfg(test)]
use crate::page::BTreeInternalPageView;
#[cfg(test)]
use crate::page::BTreeLeafEntry;
use crate::{
    debug,
    page::{BTreeInternalHeaderRef, BTreeMetaPageView, BTreeMetaPageViewMut, PageType},
    BlockId, Constant, Index, IndexInfo, Layout, LogRecord, Lsn, Schema, SimpleDBResult,
    Transaction, RID,
};

mod split_gate {
    use std::sync::{Arc, Condvar, Mutex};

    struct SplitGateInner {
        readers: u32,
        writer: bool,
    }

    pub struct SplitGate {
        state: Mutex<SplitGateInner>,
        cond: Condvar,
    }

    pub struct SplitGateReadGuard(Arc<SplitGate>);
    pub struct SplitGateWriteGuard(Arc<SplitGate>);

    impl Default for SplitGate {
        fn default() -> Self {
            Self::new()
        }
    }

    impl SplitGate {
        pub fn new() -> Self {
            Self {
                state: Mutex::new(SplitGateInner {
                    readers: 0,
                    writer: false,
                }),
                cond: Condvar::new(),
            }
        }

        pub fn acquire_shared(self: &Arc<Self>) -> SplitGateReadGuard {
            let mut s = self.state.lock().unwrap();
            while s.writer {
                s = self.cond.wait(s).unwrap();
            }
            s.readers += 1;
            SplitGateReadGuard(Arc::clone(self))
        }

        pub fn acquire_exclusive(self: &Arc<Self>) -> SplitGateWriteGuard {
            let mut s = self.state.lock().unwrap();
            while s.readers > 0 || s.writer {
                s = self.cond.wait(s).unwrap();
            }
            s.writer = true;
            SplitGateWriteGuard(Arc::clone(self))
        }
    }

    impl Drop for SplitGateReadGuard {
        fn drop(&mut self) {
            let mut s = self.0.state.lock().unwrap();
            s.readers -= 1;
            if s.readers == 0 {
                self.0.cond.notify_all();
            }
        }
    }

    impl Drop for SplitGateWriteGuard {
        fn drop(&mut self) {
            let mut s = self.0.state.lock().unwrap();
            s.writer = false;
            self.0.cond.notify_all();
        }
    }
}

pub use split_gate::SplitGate;
pub use split_gate::SplitGateReadGuard;

enum WriteState {
    Start,
    DescendFast,
    NeedSlowPin(BlockId),
    AcquireSplitGate,
    DescendUnderSplitGate,
    NeedSlowPinUnderSplitGate(BlockId),
    Done,
}

enum ReadRequest {
    Point { key: Constant },
    Range { low: Constant, high: Constant },
}

enum ReadState {
    Start,
    RetryFromRoot,
    DescendFast,
    NeedSlowPin(BlockId),
    Ready { cursor: traversal::ScanCursor },
}

/// Separator promoted from a child split.
#[derive(Debug, Clone)]
struct SplitResult {
    sep_key: Constant,
    left_block: usize,
    right_block: usize,
}

/// Result of allocating a block from the free list or appending a new block.
/// Contains the block ID and an optional LSN from BTreePageAppend (if appended).
#[derive(Clone)]
struct AllocatedBlock {
    block_id: BlockId,
    append_lsn: Option<Lsn>,
}

mod free_list {
    use super::*;

    pub(crate) struct IndexFreeList;

    impl IndexFreeList {
        const NO_FREE_BLOCK: u32 = u32::MAX;
        const FREE_NEXT_OFFSET: usize = 4;

        #[cfg(test)]
        pub(crate) fn no_free_block() -> u32 {
            Self::NO_FREE_BLOCK
        }

        fn meta_block_id(file_name: &str) -> BlockId {
            BlockId::new(file_name.to_string(), 0)
        }

        fn read_next_free_block(bytes: &[u8]) -> SimpleDBResult<u32> {
            if bytes.len() < Self::FREE_NEXT_OFFSET + 4 {
                return Err("page too small for free-list header".into());
            }
            if PageType::try_from(bytes[0])? != PageType::Free {
                return Err("expected free page while popping free list".into());
            }
            Ok(u32::from_le_bytes(
                bytes[Self::FREE_NEXT_OFFSET..Self::FREE_NEXT_OFFSET + 4]
                    .try_into()
                    .expect("free next pointer slice is exactly 4 bytes"),
            ))
        }

        pub(crate) fn allocate(
            txn: &Arc<Transaction>,
            file_name: &str,
        ) -> SimpleDBResult<AllocatedBlock> {
            let meta_block = Self::meta_block_id(file_name);
            let tx_id = txn.id();

            let meta_guard = txn.pin_write_guard(&meta_block)?;
            let mut meta_view = BTreeMetaPageViewMut::new(meta_guard)?;
            let free_head = meta_view.first_free_block();
            if free_head == Self::NO_FREE_BLOCK {
                // No free blocks, append new block to file
                meta_view.update_crc32();
                drop(meta_view);
                let block_id = txn.append(file_name);
                let append_lsn = crate::LogRecord::BTreePageAppend {
                    txnum: tx_id,
                    meta_block_id: meta_block,
                    block_id: block_id.clone(),
                }
                .write_log_record(&txn.log_manager())?;
                return Ok(AllocatedBlock {
                    block_id,
                    append_lsn: Some(append_lsn),
                });
            }

            // Read next free pointer from the block we're about to allocate
            let free_block = BlockId::new(file_name.to_string(), free_head as usize);
            let next_free = {
                let free_guard = txn.pin_write_guard(&free_block)?;
                Self::read_next_free_block(free_guard.bytes())?
            };

            // Emit WAL record before mutation
            let record = LogRecord::BTreeFreeListPop {
                txnum: tx_id,
                meta_block_id: meta_block.clone(),
                block_id: free_block.clone(),
                old_head: free_head,
                new_head: next_free,
                old_block_next: next_free,
            };
            let lsn = record.write_log_record(&txn.log_manager())?;

            // Perform mutation
            meta_view.set_first_free_block(next_free);
            meta_view.update_crc32();
            drop(meta_view);

            // Mark meta page with actual LSN
            let meta_guard = txn.pin_write_guard(&meta_block)?;
            meta_guard.mark_modified(tx_id, lsn);

            Ok(AllocatedBlock {
                block_id: free_block,
                append_lsn: None,
            })
        }

        #[cfg(test)]
        pub(crate) fn deallocate(
            txn: &Arc<Transaction>,
            file_name: &str,
            block_num: usize,
        ) -> SimpleDBResult<()> {
            if block_num == 0 {
                return Err("cannot deallocate meta block".into());
            }

            let meta_block = Self::meta_block_id(file_name);
            let tx_id = txn.id();

            let meta_guard = txn.pin_write_guard(&meta_block)?;
            let mut meta_view = BTreeMetaPageViewMut::new(meta_guard)?;
            let old_head = meta_view.first_free_block();
            let new_head = block_num as u32;

            let target_block = BlockId::new(file_name.to_string(), block_num);

            // Emit WAL record before mutation
            let record = LogRecord::BTreeFreeListPush {
                txnum: tx_id,
                meta_block_id: meta_block.clone(),
                block_id: target_block.clone(),
                old_head,
                new_head,
            };
            let lsn = record.write_log_record(&txn.log_manager())?;

            // Mark target block as free and link it to the free list
            let mut target_guard = txn.pin_write_guard(&target_block)?;
            let bytes = target_guard.bytes_mut();
            bytes.fill(0); // Clear the page
            bytes[0] = PageType::Free as u8; // Set page type discriminator
                                             // Write next_free pointer at offset 4 (points to old free-list head)
            bytes[Self::FREE_NEXT_OFFSET..Self::FREE_NEXT_OFFSET + 4]
                .copy_from_slice(&old_head.to_le_bytes());
            target_guard.mark_modified(tx_id, lsn);

            meta_view.set_first_free_block(new_head);
            meta_view.update_crc32();
            drop(meta_view);

            // Mark meta page with actual LSN
            let meta_guard = txn.pin_write_guard(&meta_block)?;
            meta_guard.mark_modified(tx_id, lsn);

            Ok(())
        }
    }
}

#[cfg(test)]
use free_list::IndexFreeList;

const BTREE_HEADER_BYTES: usize = 32;

mod split_wal {
    use super::{BTreeInternalHeaderRef, SimpleDBResult, BTREE_HEADER_BYTES};

    fn decode_optional_block(raw: u32) -> Option<usize> {
        if raw == u32::MAX {
            None
        } else {
            Some(raw as usize)
        }
    }

    pub(crate) fn read_internal_split_state(
        page_bytes: &[u8],
    ) -> SimpleDBResult<(Option<Vec<u8>>, Option<usize>)> {
        let header = BTreeInternalHeaderRef::new(
            page_bytes
                .get(..BTREE_HEADER_BYTES)
                .ok_or("internal page header too small")?,
        );
        let hk = if header.high_key_len() == 0 {
            None
        } else {
            let len = header.high_key_len() as usize;
            let off = header.high_key_off() as usize;
            Some(
                page_bytes
                    .get(off..off + len)
                    .ok_or("internal high key out of bounds")?
                    .to_vec(),
            )
        };
        Ok((hk, decode_optional_block(header.rightmost_child_block())))
    }
}

pub struct BTreeIndex {
    txn: Arc<Transaction>,
    index_name: String,
    indexed_table_id: u32,
    index_lock_table_id: u32,
    index_file_name: String,
    internal_layout: Layout,
    leaf_layout: Layout,
    read_cursor: Option<traversal::ScanCursor>,
    meta_block: BlockId,
    root_block: BlockId,
    tree_height: u16,
    structure_version: u64,
    split_gate: Arc<SplitGate>,
    scan_gate_guard: Option<SplitGateReadGuard>,
}

impl std::fmt::Display for BTreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BTreeIndex({})", self.index_name)
    }
}

impl BTreeIndex {
    const INDEX_LOCK_NAMESPACE_PREFIX: u32 = 0x4000_0000;
    const TABLE_ID_NAMESPACE_MASK: u32 = 0x3fff_ffff;

    pub(crate) const fn index_lock_table_id_for(indexed_table_id: u32) -> u32 {
        Self::INDEX_LOCK_NAMESPACE_PREFIX | (indexed_table_id & Self::TABLE_ID_NAMESPACE_MASK)
    }

    pub fn new(
        txn: Arc<Transaction>,
        index_name: &str,
        leaf_layout: Layout,
        indexed_table_id: u32,
        split_gate: Arc<SplitGate>,
    ) -> Result<Self, Box<dyn Error>> {
        let index_file_name = format!("{index_name}.idx");
        let index_lock_table_id = Self::index_lock_table_id_for(indexed_table_id);
        let meta_block = BlockId::new(index_file_name.clone(), 0);
        let mut internal_schema = Schema::new();
        internal_schema.add_from_schema(IndexInfo::BLOCK_NUM_FIELD, &leaf_layout.schema)?;
        internal_schema.add_from_schema(IndexInfo::DATA_FIELD, &leaf_layout.schema)?;
        let internal_layout = Layout::new(internal_schema.clone());

        // Bootstrap single-file index if missing.
        let (root_block, tree_height, structure_version) = if txn.size(&index_file_name) == 0 {
            // Block 0: meta
            let meta_id = txn.append(&index_file_name);
            assert_eq!(meta_id.block_num, 0);
            let append_lsn = crate::LogRecord::BTreePageAppend {
                txnum: txn.id(),
                meta_block_id: meta_id.clone(),
                block_id: meta_id.clone(),
            }
            .write_log_record(&txn.log_manager())?;
            {
                let mut guard = txn.pin_write_guard(&meta_id)?;
                guard.mark_modified(txn.id(), append_lsn);
                guard.format_as_btree_meta(1, 1, 1, u32::MAX)?;
            }

            // Block 1: root internal (level 0 -> children are leaves)
            let root_id = txn.append(&index_file_name);
            assert_eq!(root_id.block_num, 1);
            let append_lsn = crate::LogRecord::BTreePageAppend {
                txnum: txn.id(),
                meta_block_id: meta_id.clone(),
                block_id: root_id.clone(),
            }
            .write_log_record(&txn.log_manager())?;
            {
                let mut guard = txn.pin_write_guard(&root_id)?;
                guard.mark_modified(txn.id(), append_lsn);
                // rightmost child will point to first leaf (block 2)
                guard.format_as_btree_internal(0, Some(2))?;
            }

            // Block 2: first leaf
            let leaf_id = txn.append(&index_file_name);
            assert_eq!(leaf_id.block_num, 2);
            let append_lsn = crate::LogRecord::BTreePageAppend {
                txnum: txn.id(),
                meta_block_id: meta_id.clone(),
                block_id: leaf_id.clone(),
            }
            .write_log_record(&txn.log_manager())?;
            {
                let mut guard = txn.pin_write_guard(&leaf_id)?;
                guard.mark_modified(txn.id(), append_lsn);
                guard.format_as_btree_leaf(None)?;
            }
            (root_id, 1, 0)
        } else {
            // Load meta
            let guard = txn.pin_read_guard(&meta_block)?;
            let meta_view = BTreeMetaPageView::new(guard)?;
            (
                meta_view.root_block_id(&index_file_name),
                meta_view.tree_height(),
                meta_view.structure_version(),
            )
        };

        Ok(Self {
            txn,
            index_name: index_name.to_string(),
            indexed_table_id,
            index_lock_table_id,
            index_file_name,
            internal_layout,
            leaf_layout,
            meta_block,
            root_block,
            read_cursor: None,
            tree_height,
            structure_version,
            split_gate,
            scan_gate_guard: None,
        })
    }

    pub fn search_cost(num_of_blocks: usize, records_per_block: usize) -> usize {
        (1 + num_of_blocks.ilog(records_per_block))
            .try_into()
            .unwrap()
    }

    /// Returns the name of this index
    pub fn index_name(&self) -> &str {
        &self.index_name
    }

    pub fn indexed_table_id(&self) -> u32 {
        self.indexed_table_id
    }

    fn read_meta_state(&self) -> Result<(BlockId, u16, u64), Box<dyn Error>> {
        let guard = self.txn.pin_read_guard(&self.meta_block)?;
        let meta_view = BTreeMetaPageView::new(guard)?;
        Ok((
            meta_view.root_block_id(&self.index_file_name),
            meta_view.tree_height(),
            meta_view.structure_version(),
        ))
    }

    fn refresh_cached_meta(&mut self) -> Result<(BlockId, u16, u64), Box<dyn Error>> {
        let (root_block, tree_height, structure_version) = self.read_meta_state()?;
        self.root_block = root_block.clone();
        self.tree_height = tree_height;
        self.structure_version = structure_version;
        Ok((root_block, tree_height, structure_version))
    }

    fn update_meta(&mut self, lsn: Lsn, structure_version: u64) -> Result<(), Box<dyn Error>> {
        let guard = self.txn.pin_write_guard(&self.meta_block)?;
        guard.mark_modified(self.txn.id(), lsn);
        let mut view = BTreeMetaPageViewMut::new(guard)?;
        view.set_tree_height(self.tree_height);
        view.set_root_block(self.root_block.block_num as u32);
        view.set_structure_version(structure_version);
        view.update_crc32();
        self.structure_version = structure_version;
        Ok(())
    }

    fn apply_root_update(
        &mut self,
        old_root_block: usize,
        new_root_block: usize,
        old_tree_height: u16,
        new_tree_height: u16,
        old_structure_version: u64,
        new_structure_version: u64,
    ) -> Result<(), Box<dyn Error>> {
        let record = LogRecord::BTreeRootUpdate {
            txnum: self.txn.id(),
            meta_block_id: self.meta_block.clone(),
            old_root_block: old_root_block as u32,
            new_root_block: new_root_block as u32,
            old_tree_height,
            new_tree_height,
            old_structure_version,
            new_structure_version,
        };
        let lsn = record.write_log_record(&self.txn.log_manager())?;

        self.root_block = BlockId::new(self.index_file_name.clone(), new_root_block);
        self.tree_height = new_tree_height;
        self.update_meta(lsn, new_structure_version)
    }

    fn apply_insert_no_split<'a>(
        &self,
        txn: &'a Arc<Transaction>,
        mut ctx: traversal::WriteCtx<'a>,
        data_val: &Constant,
        data_rid: &RID,
        leaf_layout: &'a Layout,
        index_file_name: &'a str,
    ) -> Result<(), Box<dyn Error>> {
        let split = structural::apply_leaf_insert(
            &mut ctx,
            txn,
            leaf_layout,
            index_file_name,
            data_val.clone(),
            *data_rid,
        )?;
        assert!(
            split.is_none(),
            "leaf split on no-split insert path; leaf_needs_split precheck was wrong"
        );
        Ok(())
    }

    fn apply_insert_with_split<'a>(
        &mut self,
        txn: &'a Arc<Transaction>,
        mut ctx: traversal::WriteCtx<'a>,
        data_val: &Constant,
        data_rid: &RID,
        internal_layout: &'a Layout,
        leaf_layout: &'a Layout,
        index_file_name: &'a str,
    ) -> Result<(), Box<dyn Error>> {
        let (root_level_saved, leaf_split, root_split) = {
            let root_level_saved = ctx.root_level;
            let leaf_split = structural::apply_leaf_insert(
                &mut ctx,
                txn,
                leaf_layout,
                index_file_name,
                data_val.clone(),
                *data_rid,
            )?;
            let root_split = match leaf_split.clone() {
                None => None,
                Some(split) => {
                    debug!("Insert in index caused a leaf split");
                    structural::propagate_split_up(
                        &mut ctx,
                        txn,
                        internal_layout,
                        index_file_name,
                        split,
                    )?
                }
            };
            (root_level_saved, leaf_split, root_split)
        };

        if leaf_split.is_some() {
            let old_root_block = self.root_block.block_num;
            let old_tree_height = self.tree_height;
            let old_structure_version = self.structure_version;
            let new_structure_version = old_structure_version.wrapping_add(1);

            if let Some(root_split) = root_split {
                debug!("Insert in index caused a root split");
                let new_root_block = structural::maybe_make_new_root(
                    txn,
                    root_level_saved,
                    root_split,
                    index_file_name,
                    internal_layout,
                )?;
                self.apply_root_update(
                    old_root_block,
                    new_root_block.block_num,
                    old_tree_height,
                    old_tree_height.saturating_add(1),
                    old_structure_version,
                    new_structure_version,
                )?;
            } else {
                self.apply_root_update(
                    old_root_block,
                    old_root_block,
                    old_tree_height,
                    old_tree_height,
                    old_structure_version,
                    new_structure_version,
                )?;
            }
        }
        Ok(())
    }

    fn build_scan_cursor(
        request: &ReadRequest,
        cursor: traversal::ReadCursor,
    ) -> traversal::ScanCursor {
        match request {
            ReadRequest::Point { .. } => traversal::ScanCursor::Point(cursor),
            ReadRequest::Range { low, high } => traversal::ScanCursor::Range(
                traversal::RangeCursor::from_read_cursor(cursor, low.clone(), high.clone()),
            ),
        }
    }

    fn read_search_key(request: &ReadRequest) -> &Constant {
        match request {
            ReadRequest::Point { key } => key,
            ReadRequest::Range { low, .. } => low,
        }
    }

    fn begin_read(&mut self, request: ReadRequest) -> Result<(), Box<dyn Error>> {
        self.read_cursor = None;
        self.scan_gate_guard = None;

        let txn = Arc::clone(&self.txn);
        let internal_layout = self.internal_layout.clone();
        let leaf_layout = self.leaf_layout.clone();
        let index_file_name = self.index_file_name.clone();
        let mut root_block = self.root_block.clone();
        let mut state = ReadState::Start;

        loop {
            state = match state {
                ReadState::Start => {
                    self.scan_gate_guard = Some(self.split_gate.acquire_shared());
                    ReadState::RetryFromRoot
                }
                ReadState::RetryFromRoot => {
                    (root_block, _, _) = self.refresh_cached_meta()?;
                    ReadState::DescendFast
                }
                ReadState::DescendFast => {
                    let outcome = traversal::try_descend_read(
                        &txn,
                        &root_block,
                        &internal_layout,
                        &leaf_layout,
                        &index_file_name,
                        Self::read_search_key(&request),
                    )?;
                    match outcome {
                        traversal::ReadTraverseOutcome::Ready(cursor) => ReadState::Ready {
                            cursor: Self::build_scan_cursor(&request, cursor),
                        },
                        traversal::ReadTraverseOutcome::NeedSlowPin(block) => {
                            ReadState::NeedSlowPin(block)
                        }
                    }
                }
                ReadState::NeedSlowPin(block) => {
                    txn.pin_read_guard(&block)?;
                    ReadState::RetryFromRoot
                }
                ReadState::Ready { cursor } => {
                    self.read_cursor = Some(cursor);
                    return Ok(());
                }
            };
        }
    }

    fn execute_insert(
        &mut self,
        data_val: &Constant,
        data_rid: &RID,
    ) -> Result<(), Box<dyn Error>> {
        let txn = Arc::clone(&self.txn);
        let mut state = WriteState::Start;
        let mut split_gate_guard: Option<split_gate::SplitGateWriteGuard> = None;
        let internal_layout = self.internal_layout.clone();
        let leaf_layout = self.leaf_layout.clone();
        let index_file_name = self.index_file_name.clone();

        loop {
            state = match state {
                WriteState::Start => WriteState::DescendFast,
                WriteState::DescendFast => {
                    let ctx =
                        self.begin_write(&txn, &internal_layout, &index_file_name, data_val)?;
                    if traversal::leaf_needs_split(&ctx, &leaf_layout, data_val)? {
                        WriteState::AcquireSplitGate
                    } else {
                        self.apply_insert_no_split(
                            &txn,
                            ctx,
                            data_val,
                            data_rid,
                            &leaf_layout,
                            &index_file_name,
                        )?;
                        WriteState::Done
                    }
                }
                WriteState::NeedSlowPin(_) => {
                    unreachable!("begin_write consumes non-escalated slow-pin retries")
                }
                WriteState::AcquireSplitGate => {
                    self.scan_gate_guard = None;
                    split_gate_guard = Some(self.split_gate.acquire_exclusive());
                    WriteState::DescendUnderSplitGate
                }
                WriteState::DescendUnderSplitGate => {
                    let (root_block, _, _) = self.refresh_cached_meta()?;
                    let outcome = traversal::try_descend_write_fast(
                        &txn,
                        &root_block,
                        &internal_layout,
                        &index_file_name,
                        data_val,
                    )?;
                    match outcome {
                        traversal::WriteTraverseOutcome::Ready(ctx) => {
                            if traversal::leaf_needs_split(&ctx, &leaf_layout, data_val)? {
                                self.apply_insert_with_split(
                                    &txn,
                                    ctx,
                                    data_val,
                                    data_rid,
                                    &internal_layout,
                                    &leaf_layout,
                                    &index_file_name,
                                )?;
                                split_gate_guard.take();
                                WriteState::Done
                            } else {
                                split_gate_guard.take();
                                WriteState::DescendFast
                            }
                        }
                        traversal::WriteTraverseOutcome::NeedSlowPin(block) => {
                            WriteState::NeedSlowPinUnderSplitGate(block)
                        }
                    }
                }
                WriteState::NeedSlowPinUnderSplitGate(block) => {
                    split_gate_guard.take();
                    txn.pin_write_guard(&block)?;
                    WriteState::AcquireSplitGate
                }
                WriteState::Done => return Ok(()),
            };
        }
    }

    fn begin_write<'a>(
        &mut self,
        txn: &'a Arc<Transaction>,
        internal_layout: &'a Layout,
        index_file_name: &'a str,
        search_key: &Constant,
    ) -> Result<traversal::WriteCtx<'a>, Box<dyn Error>> {
        let mut state = WriteState::Start;

        loop {
            state = match state {
                WriteState::Start => WriteState::DescendFast,
                WriteState::DescendFast => {
                    let (root_block, _, _) = self.refresh_cached_meta()?;
                    match traversal::try_descend_write_fast(
                        txn,
                        &root_block,
                        internal_layout,
                        index_file_name,
                        search_key,
                    )? {
                        traversal::WriteTraverseOutcome::Ready(ctx) => return Ok(ctx),
                        traversal::WriteTraverseOutcome::NeedSlowPin(block) => {
                            WriteState::NeedSlowPin(block)
                        }
                    }
                }
                WriteState::NeedSlowPin(block) => {
                    txn.pin_write_guard(&block)?;
                    WriteState::DescendFast
                }
                WriteState::AcquireSplitGate
                | WriteState::DescendUnderSplitGate
                | WriteState::NeedSlowPinUnderSplitGate(_)
                | WriteState::Done => {
                    unreachable!("begin_write only handles non-escalated descent")
                }
            };
        }
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, search_key: &Constant) {
        self.txn
            .lock_in_order(vec![
                crate::OrderedLockRequest::Table {
                    table_id: self.index_lock_table_id,
                    mode: crate::TableLockMode::IS,
                },
                crate::OrderedLockRequest::IndexKey {
                    index_id: self.index_lock_table_id,
                    key: search_key.clone(),
                    mode: crate::IndexLockMode::S,
                },
            ])
            .expect("failed to acquire ordered point-scan locks");
        self.begin_read(ReadRequest::Point {
            key: search_key.clone(),
        })
        .expect("point read orchestration failed");
    }

    fn before_range(&mut self, low: &Constant, high: &Constant) {
        self.txn
            .lock_in_order(vec![
                crate::OrderedLockRequest::Table {
                    table_id: self.index_lock_table_id,
                    mode: crate::TableLockMode::IS,
                },
                crate::OrderedLockRequest::IndexRange {
                    index_id: self.index_lock_table_id,
                    low: crate::IndexBound::Key(low.clone()),
                    high: crate::IndexBound::Key(high.clone()),
                    mode: crate::IndexLockMode::S,
                },
            ])
            .expect("failed to acquire ordered range-scan locks");
        self.begin_read(ReadRequest::Range {
            low: low.clone(),
            high: high.clone(),
        })
        .expect("range read orchestration failed");
    }

    fn next(&mut self) -> bool {
        let cursor = self
            .read_cursor
            .as_mut()
            .expect("ReadCursor not initialized, did you forget to call before_first?");
        cursor
            .next(&self.txn, &self.leaf_layout, &self.index_file_name)
            .expect("scan next failed")
    }

    fn get_data_rid(&self) -> RID {
        self.read_cursor
            .as_ref()
            .unwrap()
            .get_data_rid(&self.txn, &self.leaf_layout)
            .unwrap()
    }

    fn insert(&mut self, data_val: &Constant, data_rid: &RID) {
        self.txn
            .lock_in_order(vec![
                crate::OrderedLockRequest::Table {
                    table_id: self.index_lock_table_id,
                    mode: crate::TableLockMode::IX,
                },
                crate::OrderedLockRequest::IndexKey {
                    index_id: self.index_lock_table_id,
                    key: data_val.clone(),
                    mode: crate::IndexLockMode::X,
                },
            ])
            .expect("failed to acquire ordered insert locks");
        debug!(
            "Inserting value {:?} for rid {:?} into index",
            data_val, data_rid
        );
        self.execute_insert(data_val, data_rid)
            .expect("insert orchestration failed");
    }

    fn delete(&mut self, data_val: &Constant, data_rid: &RID) {
        self.txn
            .lock_in_order(vec![
                crate::OrderedLockRequest::Table {
                    table_id: self.index_lock_table_id,
                    mode: crate::TableLockMode::IX,
                },
                crate::OrderedLockRequest::IndexKey {
                    index_id: self.index_lock_table_id,
                    key: data_val.clone(),
                    mode: crate::IndexLockMode::X,
                },
            ])
            .expect("failed to acquire ordered delete locks");
        let txn = Arc::clone(&self.txn);
        let internal_layout = self.internal_layout.clone();
        let index_file_name = self.index_file_name.clone();
        let mut ctx = self
            .begin_write(&txn, &internal_layout, &index_file_name, data_val)
            .expect("begin write descent");
        structural::apply_leaf_delete(
            &mut ctx,
            &txn,
            &self.leaf_layout,
            &self.index_file_name,
            data_val,
            *data_rid,
        )
        .unwrap();
    }
}

/// Range iterator that walks leaf pages via right-sibling links.
#[cfg(test)]
pub struct BTreeRangeIter<'a> {
    txn: Arc<Transaction>,
    layout: &'a Layout,
    file_name: &'a str,
    current_block: Option<BlockId>,
    current_slot: Option<usize>,
    lower: &'a Constant,
    upper: Option<&'a Constant>,
}

#[cfg(test)]
impl<'a> BTreeRangeIter<'a> {
    /// Start at the leaf/block/slot computed by caller; `start_slot` is typically
    /// `find_slot_before(lower)` result (or None to start at first live slot).
    pub fn new(
        txn: Arc<Transaction>,
        layout: &'a Layout,
        file_name: &'a str,
        start_block: BlockId,
        start_slot: Option<usize>,
        lower: &'a Constant,
        upper: Option<&'a Constant>,
    ) -> Self {
        Self {
            txn,
            layout,
            file_name,
            current_block: Some(start_block),
            current_slot: start_slot,
            lower,
            upper,
        }
    }
}

#[cfg(test)]
impl<'a> Iterator for BTreeRangeIter<'a> {
    type Item = BTreeLeafEntry;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let block = self.current_block.clone()?;
            let guard = self.txn.pin_read_guard(&block).ok()?;
            let view = guard.into_btree_leaf_page_view(self.layout).ok()?;

            // Hop right if we’re past this page’s high key
            if let Some(hk) = view.high_key() {
                if *self.lower >= hk {
                    let rsib = view.right_sibling_block()?;
                    self.current_block = Some(BlockId::new(self.file_name.to_string(), rsib));
                    self.current_slot = None;
                    continue;
                }
            }

            let slot_start = match self.current_slot {
                Some(s) => s,
                None => view
                    .find_slot_before(self.lower)
                    .map(|s| s + 1)
                    .unwrap_or(0),
            };

            for slot in slot_start..view.slot_count() {
                if let Ok(entry) = view.get_entry(slot) {
                    if entry.key < *self.lower {
                        continue;
                    }
                    if let Some(up) = self.upper {
                        if entry.key >= *up {
                            self.current_block = None;
                            return None;
                        }
                    }
                    self.current_slot = Some(slot + 1);
                    return Some(entry);
                }
            }

            // end of page: follow sibling
            let rsib = view.right_sibling_block()?;
            self.current_block = Some(BlockId::new(self.file_name.to_string(), rsib));
            self.current_slot = None;
        }
    }
}

mod traversal {
    use std::sync::Arc;

    use crate::{
        page::{BTreeInternalPageViewMut, PageWriteGuard},
        BlockId, Constant, Layout, SimpleDBResult, Transaction, RID,
    };

    pub(super) enum ReadTraverseOutcome {
        Ready(ReadCursor),
        NeedSlowPin(BlockId),
    }

    pub(super) struct ReadCursor {
        pub(super) leaf_block: BlockId,
        pub(super) current_slot: Option<usize>,
        pub(super) search_key: Constant,
    }

    impl ReadCursor {
        /// Advance to the next live entry equal to `search_key`.
        ///
        /// This walks forward from the current slot and follows overflow pages for duplicate
        /// keys when needed. Returns `true` when positioned on a matching entry and `false`
        /// once the duplicate chain is exhausted.
        pub(super) fn next_matching(
            &mut self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
            file_name: &str,
        ) -> SimpleDBResult<bool> {
            // Advance slot once before the search loop.
            self.current_slot = Some(self.current_slot.map(|s| s + 1).unwrap_or(0));

            loop {
                let (found, overflow_blk) = {
                    let guard = txn.pin_read_guard(&self.leaf_block)?;
                    let view = guard.into_btree_leaf_page_view(leaf_layout)?;

                    // Skip dead slots.
                    while self.current_slot.unwrap() < view.slot_count()
                        && !view.is_slot_live(self.current_slot.unwrap())
                    {
                        *self.current_slot.as_mut().unwrap() += 1;
                    }
                    let slot = self.current_slot.unwrap();

                    if slot < view.slot_count() && view.get_entry(slot)?.key == self.search_key {
                        (true, None)
                    } else {
                        // Check overflow: first live key must match search_key.
                        let mut first_live = 0;
                        while first_live < view.slot_count() && !view.is_slot_live(first_live) {
                            first_live += 1;
                        }
                        let overflow = if first_live < view.slot_count()
                            && view.get_entry(first_live)?.key == self.search_key
                        {
                            view.overflow_block()
                        } else {
                            None
                        };
                        (false, overflow)
                    }
                };

                if found {
                    return Ok(true);
                }
                if let Some(ovf_blk) = overflow_blk {
                    // Switch to overflow block; slot 0 will be read on next loop iteration.
                    self.leaf_block = BlockId::new(file_name.to_string(), ovf_blk);
                    self.current_slot = Some(0);
                } else {
                    return Ok(false);
                }
            }
        }

        /// Read the RID at the cursor's current slot.
        pub(super) fn get_data_rid(
            &self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
        ) -> SimpleDBResult<RID> {
            let slot = self.current_slot.expect("no current slot in ReadCursor");
            let guard = txn.pin_read_guard(&self.leaf_block)?;
            let view = guard.into_btree_leaf_page_view(leaf_layout)?;
            Ok(view.get_entry(slot)?.rid)
        }
    }

    pub(super) struct RangeCursor {
        pub(super) leaf_block: BlockId,
        pub(super) current_slot: Option<usize>,
        pub(super) lower_bound: Constant,
        pub(super) upper_bound: Constant,
    }

    impl RangeCursor {
        /// Convert a point-positioned leaf cursor into a bounded range cursor.
        ///
        /// The lower bound position is reused; only the cursor semantics change.
        pub(super) fn from_read_cursor(
            cursor: ReadCursor,
            lower_bound: Constant,
            upper_bound: Constant,
        ) -> Self {
            Self {
                leaf_block: cursor.leaf_block,
                current_slot: cursor.current_slot,
                lower_bound,
                upper_bound,
            }
        }

        /// Advance to the next live entry within `[lower_bound, upper_bound)`.
        ///
        /// This may hop right through sibling leaves when the current page is entirely below
        /// the lower bound or when the scan reaches the end of the page. Returns `false`
        /// once the upper bound is reached or the sibling chain ends.
        pub(super) fn next_in_range(
            &mut self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
            file_name: &str,
        ) -> SimpleDBResult<bool> {
            self.current_slot = Some(self.current_slot.map(|s| s + 1).unwrap_or(0));

            loop {
                let guard = txn.pin_read_guard(&self.leaf_block)?;
                let view = guard.into_btree_leaf_page_view(leaf_layout)?;

                if let Some(hk) = view.high_key() {
                    if self.lower_bound >= hk {
                        let Some(rsib) = view.right_sibling_block() else {
                            return Ok(false);
                        };
                        self.leaf_block = BlockId::new(file_name.to_string(), rsib);
                        self.current_slot = None;
                        continue;
                    }
                }

                let slot_start = self.current_slot.unwrap_or_else(|| {
                    view.find_slot_before(&self.lower_bound)
                        .map(|s| s + 1)
                        .unwrap_or(0)
                });

                for slot in slot_start..view.slot_count() {
                    if !view.is_slot_live(slot) {
                        continue;
                    }
                    let entry = view.get_entry(slot)?;
                    if entry.key < self.lower_bound {
                        continue;
                    }
                    if entry.key >= self.upper_bound {
                        return Ok(false);
                    }
                    self.current_slot = Some(slot);
                    return Ok(true);
                }

                let Some(rsib) = view.right_sibling_block() else {
                    return Ok(false);
                };
                self.leaf_block = BlockId::new(file_name.to_string(), rsib);
                self.current_slot = None;
            }
        }

        /// Read the RID at the range cursor's current slot.
        pub(super) fn get_data_rid(
            &self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
        ) -> SimpleDBResult<RID> {
            let slot = self.current_slot.expect("no current slot in RangeCursor");
            let guard = txn.pin_read_guard(&self.leaf_block)?;
            let view = guard.into_btree_leaf_page_view(leaf_layout)?;
            Ok(view.get_entry(slot)?.rid)
        }
    }

    pub(super) struct WriteCtx<'a> {
        pub(super) leaf_guard: Option<PageWriteGuard<'a>>,
        pub(super) leaf_block_id: BlockId,
        /// Ancestor write-latches root→parent order; last = direct parent of leaf.
        pub(super) ancestor_views: Vec<(BlockId, BTreeInternalPageViewMut<'a>)>,
        /// B-tree level of the root at descent time (for new-root allocation).
        pub(super) root_level: u8,
    }

    pub(super) enum ScanCursor {
        Point(ReadCursor),
        Range(RangeCursor),
    }

    pub(super) enum WriteTraverseOutcome<'a> {
        Ready(WriteCtx<'a>),
        NeedSlowPin(BlockId),
    }

    /// Choose the child pointer to follow from one internal page.
    ///
    /// This does the internal-node search only; it does not pin or descend into the
    /// child. If `search_key` is >= every separator, this returns the rightmost child.
    fn find_child_block(
        search_key: &Constant,
        file_name: &str,
        slot_count: usize,
        mut entry_at: impl FnMut(usize) -> SimpleDBResult<(Constant, usize)>,
        rightmost_child_block: Option<usize>,
    ) -> SimpleDBResult<BlockId> {
        let mut left = 0;
        let mut right = slot_count;
        while left < right {
            let mid = (left + right) / 2;
            let (key_mid, _) = entry_at(mid)?;
            if key_mid > *search_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        if left < slot_count {
            let (_, block_num) = entry_at(left)?;
            Ok(BlockId::new(file_name.to_string(), block_num))
        } else {
            let block_num = rightmost_child_block.ok_or("missing rightmost child")?;
            Ok(BlockId::new(file_name.to_string(), block_num))
        }
    }

    /// Position a read cursor on the leaf that should contain `search_key`.
    ///
    /// The starting leaf is already known. This may hop right through sibling leaves
    /// while `search_key` is beyond the current page's high key. Sibling hops use
    /// fast pins; if a sibling is not resident the function returns `NeedSlowPin`
    /// so the caller can pin outside latch scope and restart from the root.
    fn try_find_initial_cursor(
        txn: &Arc<Transaction>,
        leaf_block: BlockId,
        leaf_layout: &Layout,
        file_name: &str,
        search_key: &Constant,
    ) -> SimpleDBResult<ReadTraverseOutcome> {
        let first_guard = txn.pin_read_guard(&leaf_block)?;
        let mut current_block = leaf_block;
        let mut current_view = first_guard.into_btree_leaf_page_view(leaf_layout)?;

        loop {
            let should_hop = if let Some(hk) = current_view.high_key() {
                *search_key >= hk
            } else {
                false
            };

            if !should_hop {
                let current_slot = current_view.find_slot_before(search_key);
                return Ok(ReadTraverseOutcome::Ready(ReadCursor {
                    leaf_block: current_block,
                    current_slot,
                    search_key: search_key.clone(),
                }));
            }

            let Some(rsib) = current_view.right_sibling_block() else {
                let current_slot = current_view.find_slot_before(search_key);
                return Ok(ReadTraverseOutcome::Ready(ReadCursor {
                    leaf_block: current_block,
                    current_slot,
                    search_key: search_key.clone(),
                }));
            };

            let next_block = BlockId::new(file_name.to_string(), rsib);
            let Some(next_guard) = txn.pin_read_guard_fast(&next_block)? else {
                return Ok(ReadTraverseOutcome::NeedSlowPin(next_block));
            };
            let next_view = next_guard.into_btree_leaf_page_view(leaf_layout)?;
            current_block = next_block;
            current_view = next_view;
        }
    }

    /// Descend the B-tree with fast (non-blocking) pin attempts for internal nodes.
    /// At leaf level, uses slow pin (no latches held at that point).
    /// Returns `Ready` with a positioned cursor, or `NeedSlowPin(block)` if an
    /// internal page was not resident (all latches released).
    pub(super) fn try_descend_read(
        txn: &Arc<Transaction>,
        root_block: &BlockId,
        internal_layout: &Layout,
        leaf_layout: &Layout,
        file_name: &str,
        search_key: &Constant,
    ) -> SimpleDBResult<ReadTraverseOutcome> {
        let guard = match txn.pin_read_guard_fast(root_block)? {
            Some(g) => g,
            None => return Ok(ReadTraverseOutcome::NeedSlowPin(root_block.clone())),
        };
        let mut current_view = guard.into_btree_internal_page_view(internal_layout)?;

        loop {
            let child_block = find_child_block(
                search_key,
                file_name,
                current_view.slot_count(),
                |slot| {
                    let entry = current_view.get_entry(slot)?;
                    Ok((entry.key, entry.child_block))
                },
                current_view.rightmost_child_block(),
            )?;
            let is_leaf_level = current_view.btree_level() == 0;

            if is_leaf_level {
                // No parent latch held after releasing current_view. Leaf/sibling hops
                // must still use fast resident pins and restart on miss.
                return try_find_initial_cursor(
                    txn,
                    child_block,
                    leaf_layout,
                    file_name,
                    search_key,
                );
            }

            let child_guard = match txn.pin_read_guard_fast(&child_block)? {
                Some(g) => g,
                None => return Ok(ReadTraverseOutcome::NeedSlowPin(child_block)),
            };
            let child_view = child_guard.into_btree_internal_page_view(internal_layout)?;
            current_view = child_view;
        }
    }

    /// Check whether inserting `search_key` into the descended leaf would force a split.
    ///
    /// This is separate from descent so the outer write state machine can decide whether
    /// it needs to switch from the fast path into the split-gated structural path.
    pub(super) fn leaf_needs_split(
        ctx: &WriteCtx<'_>,
        leaf_layout: &Layout,
        search_key: &Constant,
    ) -> SimpleDBResult<bool> {
        let guard = ctx.leaf_guard.as_ref().expect("leaf guard present");
        crate::page::BTreeLeafPageView::bytes_insert_requires_split(
            guard.bytes(),
            leaf_layout,
            search_key,
        )
    }

    /// Descend with write-latch crabbing using blocking pins.
    ///
    /// Safe ancestors are released once the child proves it cannot split, so the returned
    /// context holds only the leaf and the unsafe ancestor chain needed for propagation.
    #[allow(dead_code)]
    pub(super) fn descend_write<'a>(
        txn: &'a Arc<Transaction>,
        root_block: &BlockId,
        internal_layout: &'a Layout,
        file_name: &str,
        search_key: &Constant,
    ) -> SimpleDBResult<WriteCtx<'a>> {
        let root_guard = txn.pin_write_guard(root_block)?;
        let root_view = root_guard.into_btree_internal_page_view_mut(internal_layout)?;
        let root_level = root_view.btree_level();

        let mut ancestor_views: Vec<(BlockId, BTreeInternalPageViewMut<'a>)> = Vec::new();
        let mut current_block = root_block.clone();
        let mut current_view = root_view;

        loop {
            let child_block = find_child_block(
                search_key,
                file_name,
                current_view.slot_count(),
                |slot| {
                    let entry = current_view.get_entry(slot)?;
                    Ok((entry.key, entry.child_block))
                },
                current_view.rightmost_child_block(),
            )?;
            let level = current_view.btree_level();

            if level == 0 {
                let leaf_guard = txn.pin_write_guard(&child_block)?;
                ancestor_views.push((current_block, current_view));
                return Ok(WriteCtx {
                    leaf_guard: Some(leaf_guard),
                    leaf_block_id: child_block,
                    ancestor_views,
                    root_level,
                });
            }

            let child_guard = txn.pin_write_guard(&child_block)?;
            let child_view = child_guard.into_btree_internal_page_view_mut(internal_layout)?;

            if !child_view.is_full() {
                ancestor_views.clear();
            } else {
                ancestor_views.push((current_block, current_view));
            }

            current_block = child_block;
            current_view = child_view;
        }
    }

    /// Descend with write-latch crabbing using only fast resident pins.
    ///
    /// On a miss, all held latches are released and `NeedSlowPin(block)` is returned so the
    /// caller can pin outside latch scope and restart from the root.
    pub(super) fn try_descend_write_fast<'a>(
        txn: &'a Arc<Transaction>,
        root_block: &BlockId,
        internal_layout: &'a Layout,
        file_name: &str,
        search_key: &Constant,
    ) -> SimpleDBResult<WriteTraverseOutcome<'a>> {
        let root_guard = match txn.pin_write_guard_fast(root_block)? {
            Some(g) => g,
            None => return Ok(WriteTraverseOutcome::NeedSlowPin(root_block.clone())),
        };
        let root_view = root_guard.into_btree_internal_page_view_mut(internal_layout)?;
        let root_level = root_view.btree_level();

        let mut ancestor_views: Vec<(BlockId, BTreeInternalPageViewMut<'a>)> = Vec::new();
        let mut current_block = root_block.clone();
        let mut current_view = root_view;

        loop {
            let child_block = find_child_block(
                search_key,
                file_name,
                current_view.slot_count(),
                |slot| {
                    let entry = current_view.get_entry(slot)?;
                    Ok((entry.key, entry.child_block))
                },
                current_view.rightmost_child_block(),
            )?;
            let level = current_view.btree_level();

            if level == 0 {
                let leaf_guard = match txn.pin_write_guard_fast(&child_block)? {
                    Some(g) => g,
                    None => {
                        ancestor_views.clear();
                        return Ok(WriteTraverseOutcome::NeedSlowPin(child_block));
                    }
                };
                ancestor_views.push((current_block, current_view));
                return Ok(WriteTraverseOutcome::Ready(WriteCtx {
                    leaf_guard: Some(leaf_guard),
                    leaf_block_id: child_block,
                    ancestor_views,
                    root_level,
                }));
            }

            let child_guard = match txn.pin_write_guard_fast(&child_block)? {
                Some(g) => g,
                None => {
                    ancestor_views.clear();
                    return Ok(WriteTraverseOutcome::NeedSlowPin(child_block));
                }
            };
            let child_view = child_guard.into_btree_internal_page_view_mut(internal_layout)?;

            if !child_view.is_full() {
                ancestor_views.clear();
            } else {
                ancestor_views.push((current_block, current_view));
            }

            current_block = child_block;
            current_view = child_view;
        }
    }

    impl ScanCursor {
        /// Advance whichever scan cursor variant is active.
        pub(super) fn next(
            &mut self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
            file_name: &str,
        ) -> SimpleDBResult<bool> {
            match self {
                ScanCursor::Point(cursor) => cursor.next_matching(txn, leaf_layout, file_name),
                ScanCursor::Range(cursor) => cursor.next_in_range(txn, leaf_layout, file_name),
            }
        }

        /// Read the RID at the active scan cursor position.
        pub(super) fn get_data_rid(
            &self,
            txn: &Arc<Transaction>,
            leaf_layout: &Layout,
        ) -> SimpleDBResult<RID> {
            match self {
                ScanCursor::Point(cursor) => cursor.get_data_rid(txn, leaf_layout),
                ScanCursor::Range(cursor) => cursor.get_data_rid(txn, leaf_layout),
            }
        }
    }
}

mod structural {
    use std::sync::Arc;

    use crate::{
        page::{BTreeInternalPageViewMut, BTreeLeafPageViewMut},
        BlockId, Constant, Layout, LogRecord, SimpleDBResult, Transaction, RID,
    };

    use super::{free_list::IndexFreeList, split_wal, traversal::WriteCtx, SplitResult};

    /// Split a leaf page using an already-held mutable view (no re-acquisition).
    /// Returns the BlockId of the newly created right sibling.
    fn split_leaf_inplace<'a>(
        orig_view: &mut BTreeLeafPageViewMut<'a>,
        orig_block_id: &BlockId,
        txn: &'a Arc<Transaction>,
        leaf_layout: &'a Layout,
        file_name: &str,
        split_slot: usize,
        overflow_block: Option<usize>,
    ) -> SimpleDBResult<BlockId> {
        let txn_id = txn.id();

        // Capture WAL split state from view before any mutations.
        let old_left_high_key: Option<Vec<u8>> = orig_view
            .high_key()
            .map(|c| -> SimpleDBResult<Vec<u8>> {
                c.try_into().map_err(|e: Box<dyn std::error::Error>| e)
            })
            .transpose()?;
        let old_left_right_sibling = orig_view.right_sibling_block();
        let old_left_overflow = orig_view.overflow_block();

        // Allocate new leaf page.
        let allocated = IndexFreeList::allocate(txn, file_name)?;
        let mut new_guard = txn.pin_write_guard(&allocated.block_id)?;
        if let Some(append_lsn) = allocated.append_lsn {
            new_guard.mark_modified(txn_id, append_lsn);
        }
        new_guard.format_as_btree_leaf(overflow_block)?;
        let mut new_view = new_guard.into_btree_leaf_page_view_mut(leaf_layout)?;

        // Emit WAL split record.
        let split_lsn = LogRecord::BTreePageSplit {
            txnum: txn_id,
            left_block_id: orig_block_id.clone(),
            right_block_id: allocated.block_id.clone(),
            is_leaf: true,
            old_left_high_key,
            old_left_right_sibling,
            old_left_overflow,
            old_left_rightmost_child: None,
        }
        .write_log_record(&txn.log_manager())?;
        orig_view.update_page_lsn(split_lsn);
        new_view.update_page_lsn(split_lsn);

        // Preserve old right-sibling pointer before mutating orig.
        let old_right = orig_view.right_sibling_block();

        // Move entries [split_slot, end) to new page.
        while split_slot < orig_view.slot_count() {
            let entry = orig_view.get_entry(split_slot)?.clone();
            new_view.insert_entry(entry.key, entry.rid)?;
            orig_view.delete_entry(split_slot)?;
        }

        // Set high keys and sibling links.
        let sep_key = new_view.get_entry(0)?.key.clone();
        let sep_bytes: Vec<u8> = sep_key.try_into()?;
        orig_view.set_high_key(&sep_bytes)?;
        orig_view.set_right_sibling_block(Some(allocated.block_id.block_num))?;
        new_view.set_right_sibling_block(old_right)?;
        new_view.clear_high_key()?;

        Ok(allocated.block_id)
    }

    /// Split an internal page using an already-held mutable view (no re-acquisition).
    /// Returns (new_block_id, separator_key_to_push_up).
    #[allow(dead_code)]
    fn split_internal_inplace<'a>(
        orig_view: &mut BTreeInternalPageViewMut<'a>,
        orig_block_id: &BlockId,
        txn: &'a Arc<Transaction>,
        internal_layout: &'a Layout,
        file_name: &str,
    ) -> SimpleDBResult<(BlockId, Constant)> {
        let txn_id = txn.id();
        let split_slot = orig_view.slot_count() / 2;

        // Read WAL split state from raw bytes before any mutations.
        let (old_left_high_key, old_left_rightmost_child) =
            split_wal::read_internal_split_state(orig_view.bytes())?;

        // Allocate new internal page.
        let allocated = IndexFreeList::allocate(txn, file_name)?;
        let mut new_guard = txn.pin_write_guard(&allocated.block_id)?;
        if let Some(append_lsn) = allocated.append_lsn {
            new_guard.mark_modified(txn_id, append_lsn);
        }
        new_guard.format_as_btree_internal(orig_view.btree_level(), None)?;
        let mut new_view = new_guard.into_btree_internal_page_view_mut(internal_layout)?;

        // Emit WAL split record.
        let split_lsn = LogRecord::BTreePageSplit {
            txnum: txn_id,
            left_block_id: orig_block_id.clone(),
            right_block_id: allocated.block_id.clone(),
            is_leaf: false,
            old_left_high_key,
            old_left_right_sibling: None,
            old_left_overflow: None,
            old_left_rightmost_child,
        }
        .write_log_record(&txn.log_manager())?;
        orig_view.update_page_lsn(split_lsn);
        new_view.update_page_lsn(split_lsn);

        // Snapshot children array C0..Ck.
        let orig_slot_count = orig_view.slot_count();
        let mut children = Vec::with_capacity(orig_slot_count + 1);
        for i in 0..orig_slot_count {
            children.push(orig_view.get_entry(i)?.child_block);
        }
        children.push(
            orig_view
                .rightmost_child_block()
                .ok_or("missing rightmost child")?,
        );

        let (left_children, right_children) = children.split_at(split_slot);

        // Collect entries [split_slot, end) to move to right page.
        let mut moved = Vec::new();
        for rel_idx in split_slot..orig_slot_count {
            let entry = orig_view.get_entry(rel_idx)?;
            let rc = right_children
                .get(rel_idx - split_slot + 1)
                .copied()
                .ok_or("right child missing for moved entry")?;
            moved.push((entry.key.clone(), rc));
        }

        // Delete moved entries from original.
        for _ in split_slot..orig_slot_count {
            orig_view.delete_entry(split_slot)?;
        }

        // Fix up child pointers.
        if let Some(&last_left) = left_children.last() {
            orig_view.set_rightmost_child_block(last_left)?;
        }
        if let Some(&last_right) = right_children.last() {
            new_view.set_rightmost_child_block(last_right)?;
        }
        for (k, rc) in moved {
            new_view.insert_entry(k, rc)?;
        }

        // Set high keys: left gets separator, right gets +∞ sentinel.
        let sep_key = new_view.get_entry(0)?.key.clone();
        let sep_bytes: Vec<u8> = sep_key.clone().try_into()?;
        orig_view.set_high_key(&sep_bytes)?;
        new_view.clear_high_key()?;

        Ok((allocated.block_id, sep_key))
    }

    fn collect_internal_keys_children(
        view: &BTreeInternalPageViewMut<'_>,
    ) -> SimpleDBResult<(Vec<Constant>, Vec<usize>)> {
        let slot_count = view.slot_count();
        let mut keys = Vec::with_capacity(slot_count);
        let mut children = Vec::with_capacity(slot_count + 1);

        for i in 0..slot_count {
            let entry = view.get_entry(i)?;
            keys.push(entry.key);
            children.push(entry.child_block);
        }
        children.push(
            view.rightmost_child_block()
                .ok_or("missing rightmost child")?,
        );

        Ok((keys, children))
    }

    fn upper_bound_slot(keys: &[Constant], search_key: &Constant) -> usize {
        let mut left = 0usize;
        let mut right = keys.len();
        while left < right {
            let mid = (left + right) / 2;
            if keys[mid] > *search_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        left
    }

    fn rebuild_internal_page(
        view: &mut BTreeInternalPageViewMut<'_>,
        keys: &[Constant],
        children: &[usize],
        high_key: Option<&Constant>,
    ) -> SimpleDBResult<()> {
        assert_eq!(children.len(), keys.len() + 1);

        for slot in (0..view.slot_count()).rev() {
            view.delete_entry(slot)?;
        }

        view.set_rightmost_child_block(children[0])?;
        for (idx, key) in keys.iter().enumerate() {
            view.insert_entry(key.clone(), children[idx + 1])?;
        }

        if let Some(high_key) = high_key {
            let high_key_bytes: Vec<u8> = high_key.clone().try_into()?;
            view.set_high_key(&high_key_bytes)?;
        } else {
            view.clear_high_key()?;
        }

        Ok(())
    }

    fn split_internal_with_incoming<'a>(
        orig_view: &mut BTreeInternalPageViewMut<'a>,
        orig_block_id: &BlockId,
        txn: &'a Arc<Transaction>,
        internal_layout: &'a Layout,
        file_name: &str,
        incoming_key: Constant,
        incoming_right_child: usize,
    ) -> SimpleDBResult<(BlockId, Constant)> {
        let (mut keys, mut children) = collect_internal_keys_children(orig_view)?;
        let slot = upper_bound_slot(&keys, &incoming_key);
        keys.insert(slot, incoming_key);
        children.insert(slot + 1, incoming_right_child);

        let split_slot = keys.len() / 2;
        let left_keys = keys[..split_slot].to_vec();
        let right_keys = keys[split_slot..].to_vec();
        let left_children = children[..split_slot + 1].to_vec();
        let right_children = children[split_slot..].to_vec();
        let sep_key = right_keys
            .first()
            .cloned()
            .ok_or("internal split requires at least one right-side key")?;

        let txn_id = txn.id();
        let (old_left_high_key, old_left_rightmost_child) =
            split_wal::read_internal_split_state(orig_view.bytes())?;

        let allocated = IndexFreeList::allocate(txn, file_name)?;
        let mut new_guard = txn.pin_write_guard(&allocated.block_id)?;
        if let Some(append_lsn) = allocated.append_lsn {
            new_guard.mark_modified(txn_id, append_lsn);
        }
        new_guard.format_as_btree_internal(orig_view.btree_level(), Some(right_children[0]))?;
        let mut new_view = new_guard.into_btree_internal_page_view_mut(internal_layout)?;

        let split_lsn = LogRecord::BTreePageSplit {
            txnum: txn_id,
            left_block_id: orig_block_id.clone(),
            right_block_id: allocated.block_id.clone(),
            is_leaf: false,
            old_left_high_key,
            old_left_right_sibling: None,
            old_left_overflow: None,
            old_left_rightmost_child,
        }
        .write_log_record(&txn.log_manager())?;
        orig_view.update_page_lsn(split_lsn);
        new_view.update_page_lsn(split_lsn);

        rebuild_internal_page(orig_view, &left_keys, &left_children, Some(&sep_key))?;
        rebuild_internal_page(&mut new_view, &right_keys, &right_children, None)?;

        Ok((allocated.block_id, sep_key))
    }

    /// Insert (search_key, rid) into the leaf held in ctx.
    /// Returns Some(SplitResult) when the leaf split and a separator must propagate up.
    pub(super) fn apply_leaf_insert<'a>(
        ctx: &mut WriteCtx<'a>,
        txn: &'a Arc<Transaction>,
        leaf_layout: &'a Layout,
        file_name: &str,
        search_key: Constant,
        rid: RID,
    ) -> SimpleDBResult<Option<SplitResult>> {
        let leaf_guard = ctx
            .leaf_guard
            .take()
            .expect("leaf_guard missing in WriteCtx");
        let leaf_block_id = ctx.leaf_block_id.clone();
        let mut leaf_view = leaf_guard.into_btree_leaf_page_view_mut(leaf_layout)?;

        // Check overflow + smaller-key case: inserting a record smaller than the first key
        // on a page that already has an overflow chain forces a split at slot 0.
        if let Some(overflow_block) = leaf_view.overflow_block() {
            if leaf_view.slot_count() > 0 {
                let first_key = leaf_view.get_entry(0)?.key.clone();
                if first_key > search_key {
                    let new_block_id = split_leaf_inplace(
                        &mut leaf_view,
                        &leaf_block_id,
                        txn,
                        leaf_layout,
                        file_name,
                        0,
                        Some(overflow_block),
                    )?;
                    leaf_view.set_overflow_block(None)?;
                    leaf_view.insert_entry(search_key, rid)?;
                    return Ok(Some(SplitResult {
                        sep_key: first_key,
                        left_block: leaf_block_id.block_num,
                        right_block: new_block_id.block_num,
                    }));
                }
            }
        }

        // Normal insert.
        leaf_view.insert_entry(search_key.clone(), rid)?;
        if !leaf_view.is_full() {
            return Ok(None);
        }

        // Page is full after insert; determine split strategy.
        let first_key = leaf_view.get_entry(0)?.key.clone();
        let last_key = leaf_view.get_entry(leaf_view.slot_count() - 1)?.key.clone();

        if first_key == last_key {
            // All identical keys: create an overflow page.
            let new_block_id = split_leaf_inplace(
                &mut leaf_view,
                &leaf_block_id,
                txn,
                leaf_layout,
                file_name,
                1,
                None,
            )?;
            leaf_view.set_overflow_block(Some(new_block_id.block_num))?;
            return Ok(None); // Overflow split: no separator to propagate.
        }

        // Find the split point, keeping identical keys on the same side.
        let mut split_point = leaf_view.slot_count() / 2;
        let mut split_record = leaf_view.get_entry(split_point)?.key.clone();
        if split_record == first_key {
            while leaf_view.get_entry(split_point)?.key == first_key {
                split_point += 1;
            }
            split_record = leaf_view.get_entry(split_point)?.key.clone();
        } else {
            while split_point > 0 && leaf_view.get_entry(split_point - 1)?.key == split_record {
                split_point -= 1;
            }
        }

        let new_block_id = split_leaf_inplace(
            &mut leaf_view,
            &leaf_block_id,
            txn,
            leaf_layout,
            file_name,
            split_point,
            None,
        )?;
        Ok(Some(SplitResult {
            sep_key: split_record,
            left_block: leaf_block_id.block_num,
            right_block: new_block_id.block_num,
        }))
    }

    /// Insert (search_key, rid) into the leaf without splitting.
    /// Caller must have verified the leaf has space (leaf_is_full returned false).
    #[allow(dead_code)]
    pub(super) fn apply_leaf_insert_no_split<'a>(
        ctx: &mut WriteCtx<'a>,
        leaf_layout: &'a Layout,
        search_key: Constant,
        rid: RID,
    ) -> SimpleDBResult<()> {
        let leaf_guard = ctx
            .leaf_guard
            .take()
            .expect("leaf_guard missing in WriteCtx");
        ctx.ancestor_views.clear(); // no split, don't need ancestors
        let mut leaf_view = leaf_guard.into_btree_leaf_page_view_mut(leaf_layout)?;
        leaf_view.insert_entry(search_key, rid)?;
        Ok(())
    }

    /// Propagate a leaf split up the ancestor stack.
    /// Returns None when the split was absorbed; Some(SplitResult) when the root itself split.
    pub(super) fn propagate_split_up<'a>(
        ctx: &mut WriteCtx<'a>,
        txn: &'a Arc<Transaction>,
        internal_layout: &'a Layout,
        file_name: &str,
        mut split: SplitResult,
    ) -> SimpleDBResult<Option<SplitResult>> {
        while let Some((ancestor_block, mut ancestor_view)) = ctx.ancestor_views.pop() {
            if !ancestor_view.is_full() {
                ancestor_view.insert_entry(split.sep_key.clone(), split.right_block)?;
                // Ancestor absorbed the separator; release remaining latches.
                ctx.ancestor_views.clear();
                return Ok(None);
            }

            let (new_block_id, sep_key) = split_internal_with_incoming(
                &mut ancestor_view,
                &ancestor_block,
                txn,
                internal_layout,
                file_name,
                split.sep_key.clone(),
                split.right_block,
            )?;
            split = SplitResult {
                sep_key,
                left_block: ancestor_block.block_num,
                right_block: new_block_id.block_num,
            };
            // ancestor_view dropped here, releasing its write-latch.
        }
        // All ancestors exhausted: the root itself split.
        Ok(Some(split))
    }

    /// Allocate a new root page above the old root after a root split.
    pub(super) fn maybe_make_new_root(
        txn: &Arc<Transaction>,
        old_root_level: u8,
        split: SplitResult,
        file_name: &str,
        internal_layout: &Layout,
    ) -> SimpleDBResult<BlockId> {
        let allocated = IndexFreeList::allocate(txn, file_name)?;
        let mut guard = txn.pin_write_guard(&allocated.block_id)?;
        if let Some(append_lsn) = allocated.append_lsn {
            guard.mark_modified(txn.id(), append_lsn);
        }
        guard.format_as_btree_internal(old_root_level + 1, Some(split.left_block))?;
        let mut view = guard.into_btree_internal_page_view_mut(internal_layout)?;
        view.insert_entry(split.sep_key, split.right_block)?;
        Ok(allocated.block_id)
    }

    /// Delete the entry (search_key, rid) from the leaf held in ctx.
    /// Follows overflow chains as needed; does not require ancestor latches.
    pub(super) fn apply_leaf_delete<'a>(
        ctx: &mut WriteCtx<'a>,
        txn: &'a Arc<Transaction>,
        leaf_layout: &'a Layout,
        file_name: &str,
        search_key: &Constant,
        rid: RID,
    ) -> SimpleDBResult<()> {
        // Delete doesn't need ancestors; release them.
        ctx.ancestor_views.clear();
        let leaf_guard = ctx
            .leaf_guard
            .take()
            .expect("leaf_guard missing in WriteCtx");

        // Scan the initial leaf page (already write-latched) for the target entry.
        let overflow_from_initial = {
            let mut leaf_view = leaf_guard.into_btree_leaf_page_view_mut(leaf_layout)?;
            let start_slot = leaf_view
                .find_slot_before(search_key)
                .map(|s| s + 1)
                .unwrap_or(0);
            for slot in start_slot..leaf_view.slot_count() {
                if !leaf_view.is_slot_live(slot) {
                    continue;
                }
                let entry = leaf_view.get_entry(slot)?;
                if entry.key != *search_key {
                    break;
                }
                if entry.rid == rid {
                    leaf_view.delete_entry(slot)?;
                    return Ok(());
                }
            }
            // Entry not in this page; check if we should follow the overflow chain.
            let first_live_key = (0..leaf_view.slot_count())
                .find(|&s| leaf_view.is_slot_live(s))
                .and_then(|s| leaf_view.get_entry(s).ok().map(|e| e.key));
            if first_live_key.as_ref() == Some(search_key) {
                leaf_view.overflow_block()
            } else {
                None
            }
            // leaf_view (and the leaf write-latch) is released here.
        };

        // Follow overflow chain using release-before-next pattern.
        let mut current_block = match overflow_from_initial {
            Some(n) => BlockId::new(file_name.to_string(), n),
            None => return Err("RID not found in BTreeLeaf".into()),
        };
        loop {
            let next_overflow = {
                let guard = txn.pin_write_guard(&current_block)?;
                let mut view = guard.into_btree_leaf_page_view_mut(leaf_layout)?;
                let mut found = false;
                for slot in 0..view.slot_count() {
                    if !view.is_slot_live(slot) {
                        continue;
                    }
                    let entry = view.get_entry(slot)?;
                    if entry.rid == rid {
                        view.delete_entry(slot)?;
                        found = true;
                        break;
                    }
                }
                if found {
                    return Ok(());
                }
                view.overflow_block()
                // guard and view released here.
            };
            current_block = match next_overflow {
                Some(n) => BlockId::new(file_name.to_string(), n),
                None => return Err("RID not found in BTreeLeaf".into()),
            };
        }
    }
}

#[cfg(test)]
mod btree_index_tests {
    use super::*;
    use crate::{
        test_utils::{generate_filename, generate_random_number},
        Schema, SimpleDB, TestDir, WalMode,
    };
    const TEST_INDEXED_TABLE_ID: u32 = 7;

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("dataval");
        schema.add_int_field("block");
        schema.add_int_field("id");
        Layout::new(schema)
    }

    fn setup_index(db: &SimpleDB) -> BTreeIndex {
        let tx = db.new_tx();
        let layout = create_test_layout();
        let index_name = generate_filename();
        BTreeIndex::new(
            Arc::clone(&tx),
            &index_name,
            layout,
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap()
    }

    /// Insert ascending keys until the leaf file reaches `target_blocks` size.
    /// Returns the next key that would be inserted after completion.
    fn insert_until_leaf_blocks(
        index: &mut BTreeIndex,
        target_blocks: usize,
        mut next_key: i32,
    ) -> i32 {
        let cap = 10_000;
        while index.txn.size(&index.index_file_name).saturating_sub(2) < target_blocks {
            index.insert(&Constant::Int(next_key), &RID::new(1, next_key as usize));
            next_key += 1;
            assert!(
                next_key < cap,
                "insert_until_leaf_blocks exceeded safety cap without reaching {} blocks",
                target_blocks
            );
        }
        next_key
    }

    fn descend_read_with_restart(
        index: &mut BTreeIndex,
        search_key: &Constant,
    ) -> traversal::ReadCursor {
        loop {
            let (root_block, _, _) = index.refresh_cached_meta().expect("read index meta");
            match traversal::try_descend_read(
                &index.txn,
                &root_block,
                &index.internal_layout,
                &index.leaf_layout,
                &index.index_file_name,
                search_key,
            )
            .expect("restart-oriented read descent")
            {
                traversal::ReadTraverseOutcome::Ready(cursor) => return cursor,
                traversal::ReadTraverseOutcome::NeedSlowPin(block) => {
                    index
                        .txn
                        .pin_read_guard(&block)
                        .expect("slow pin for restart");
                }
            }
        }
    }

    #[test]
    fn test_simple_insert_and_search() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        // Insert some values
        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.insert(&Constant::Int(20), &RID::new(1, 2));
        index.insert(&Constant::Int(30), &RID::new(1, 3));

        // Search for inserted values
        index.before_first(&Constant::Int(20));
        assert!(index.next());
        assert_eq!(index.get_data_rid(), RID::new(1, 2));

        index.before_first(&Constant::Int(10));
        assert!(index.next());
        assert_eq!(index.get_data_rid(), RID::new(1, 1));
    }

    #[test]
    fn test_range_scan_returns_only_keys_in_half_open_interval() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.insert(&Constant::Int(20), &RID::new(1, 2));
        index.insert(&Constant::Int(25), &RID::new(1, 3));
        index.insert(&Constant::Int(30), &RID::new(1, 4));

        index.before_range(&Constant::Int(20), &Constant::Int(30));

        let mut found = Vec::new();
        while index.next() {
            found.push(index.get_data_rid());
        }

        assert_eq!(found, vec![RID::new(1, 2), RID::new(1, 3)]);
    }

    /// Combined test replacing:
    /// - test_btree_index_construction
    /// - test_duplicate_keys
    /// - test_delete
    #[test]
    fn test_btree_comprehensive_operations() {
        // Test construction (was test_btree_index_construction)
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let index = setup_index(&db);

        let guard = index.txn.pin_read_guard(&index.root_block).unwrap();
        let view = BTreeInternalPageView::new(guard, &index.internal_layout).unwrap();
        assert_eq!(view.slot_count(), 0);
        assert_eq!(view.rightmost_child_block(), Some(2));

        // Test duplicate keys (was test_duplicate_keys)
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.insert(&Constant::Int(10), &RID::new(1, 2));
        index.insert(&Constant::Int(10), &RID::new(1, 3));

        index.before_first(&Constant::Int(10));
        let mut found_rids = Vec::new();
        while index.next() {
            found_rids.push(index.get_data_rid());
        }

        assert_eq!(found_rids.len(), 3);
        assert!(found_rids.contains(&RID::new(1, 1)));
        assert!(found_rids.contains(&RID::new(1, 2)));
        assert!(found_rids.contains(&RID::new(1, 3)));

        // Test delete (was test_delete)
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.delete(&Constant::Int(10), &RID::new(1, 1));

        index.before_first(&Constant::Int(10));
        assert!(!index.next());

        index.insert(&Constant::Int(20), &RID::new(1, 1));
        index.insert(&Constant::Int(20), &RID::new(1, 2));
        index.delete(&Constant::Int(20), &RID::new(1, 1));

        index.before_first(&Constant::Int(20));
        assert!(index.next());
        assert_eq!(index.get_data_rid(), RID::new(1, 2));
        assert!(!index.next());
    }

    #[test]
    fn test_single_file_bootstrap_layout() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let index = setup_index(&db);

        // File should contain meta + root + first leaf
        assert_eq!(index.txn.size(&index.index_file_name), 3);

        // Meta assertions
        {
            let guard = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 0))
                .unwrap();
            let meta = BTreeMetaPageView::new(guard).expect("meta page view");
            assert_eq!(meta.version(), 1);
            assert_eq!(meta.tree_height(), 1);
            assert_eq!(meta.root_block(), 1);
            assert_eq!(meta.first_free_block(), u32::MAX);
            assert_eq!(meta.structure_version(), 0);
        }

        // Root internal assertions
        {
            let guard = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 1))
                .unwrap();
            let view = guard
                .into_btree_internal_page_view(&index.internal_layout)
                .expect("root internal view");
            assert_eq!(view.slot_count(), 0);
            assert_eq!(view.btree_level(), 0);
            assert_eq!(view.rightmost_child_block(), Some(2));
        }

        // First leaf assertions
        {
            let guard = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 2))
                .unwrap();
            let view = guard
                .into_btree_leaf_page_view(&index.leaf_layout)
                .expect("leaf view");
            assert_eq!(view.slot_count(), 0);
            assert_eq!(view.right_sibling_block(), None);
            assert_eq!(view.overflow_block(), None);
            assert_eq!(view.high_key(), None);
        }
    }

    #[test]
    fn test_btree_split() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        // Insert enough values to force splits
        let _ = insert_until_leaf_blocks(&mut index, 2, 0);

        // Verify we can still find values after splits
        for i in 0..24 {
            index.before_first(&Constant::Int(i));
            assert!(index.next());
            assert_eq!(index.get_data_rid(), RID::new(1, i as usize));
        }

        // Meta should reflect current root and height
        {
            let guard = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 0))
                .unwrap();
            let meta = BTreeMetaPageView::new(guard).expect("meta page view");
            assert_eq!(meta.root_block() as usize, index.root_block.block_num);
            assert_eq!(meta.tree_height(), index.tree_height);
            assert_eq!(meta.structure_version(), index.structure_version);
            assert!(meta.structure_version() > 0);
        }
    }

    #[test]
    fn test_before_first_on_height_two_tree() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        let mut next_key = 0_i32;
        let cap = 200_000_i32;
        while index.tree_height < 2 && next_key < cap {
            index.insert(&Constant::Int(next_key), &RID::new(1, next_key as usize));
            next_key += 1;
        }
        assert!(
            index.tree_height >= 2,
            "failed to grow index to height >= 2 within cap={cap}, reached height={}",
            index.tree_height
        );

        let lookup_key = next_key / 2;
        index.before_first(&Constant::Int(lookup_key));
        assert!(index.next(), "expected to find key {lookup_key}");
        assert_eq!(
            index.get_data_rid(),
            RID::new(1, lookup_key as usize),
            "lookup should land on the matching RID for key {lookup_key}"
        );
    }

    #[test]
    fn test_range_iterator_across_siblings() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        // Force multiple splits (aim for 4 leaf blocks)
        let next_key = insert_until_leaf_blocks(&mut index, 4, 0);

        // Unbounded range
        let lower = Constant::Int(0);
        let start = {
            let cursor = descend_read_with_restart(&mut index, &lower);
            (cursor.leaf_block, cursor.current_slot)
        };
        let iter = BTreeRangeIter::new(
            Arc::clone(&index.txn),
            &index.leaf_layout,
            &index.index_file_name,
            start.0.clone(),
            start.1,
            &lower,
            None,
        );
        let collected: Vec<i32> = iter
            .map(|e| match e.key {
                Constant::Int(v) => v,
                _ => panic!("expected int keys"),
            })
            .collect();
        let expected: Vec<i32> = (0..next_key).collect();
        assert_eq!(collected, expected);

        // Bounded range [10,50)
        let lower_b = Constant::Int(10);
        let upper_b = Constant::Int(50);
        let start_b = {
            let cursor = descend_read_with_restart(&mut index, &lower_b);
            (cursor.leaf_block, cursor.current_slot)
        };
        let iter_b = BTreeRangeIter::new(
            Arc::clone(&index.txn),
            &index.leaf_layout,
            &index.index_file_name,
            start_b.0,
            start_b.1,
            &lower_b,
            Some(&upper_b),
        );
        let collected_b: Vec<i32> = iter_b
            .map(|e| match e.key {
                Constant::Int(v) => v,
                _ => panic!("expected int keys"),
            })
            .collect();
        assert!(collected_b.iter().all(|&v| (10..50).contains(&v)));
        let mut sorted_b = collected_b.clone();
        sorted_b.sort();
        assert_eq!(collected_b, sorted_b);
    }

    #[test]
    fn test_leaf_split_headers_and_sibling_chain() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let mut index = setup_index(&db);

        // Grow until first split (two leaf pages exist)
        let mut i = 0;
        while index.txn.size(&index.index_file_name).saturating_sub(2) < 2 && i < 1000 {
            index.insert(&Constant::Int(i as i32), &RID::new(1, i));
            i += 1;
        }
        assert!(
            index.txn.size(&index.index_file_name).saturating_sub(2) >= 2,
            "expected at least one split to create a second leaf"
        );

        // Read both leaves
        let left_id = BlockId::new(index.index_file_name.clone(), 2);
        {
            let left_view = index
                .txn
                .pin_read_guard(&left_id)
                .unwrap()
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let rsib = left_view
                .right_sibling_block()
                .expect("left page should link to split sibling");
            let right_id = BlockId::new(index.index_file_name.clone(), rsib);
            let right_view = index
                .txn
                .pin_read_guard(&right_id)
                .unwrap()
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let right_first = right_view.get_entry(0).unwrap().key;

            assert_eq!(rsib, 3, "first split should append sibling at block 3");
            assert_eq!(
                left_view.high_key(),
                Some(right_first.clone()),
                "left high key should equal right's first key"
            );
            assert_eq!(left_view.right_sibling_block(), Some(rsib));
            assert_eq!(right_view.high_key(), None);
            assert_eq!(right_view.right_sibling_block(), None);
        }

        // Continue inserting until a second split creates a third leaf
        while index.txn.size(&index.index_file_name).saturating_sub(2) < 3 && i < 2000 {
            index.insert(&Constant::Int(i as i32), &RID::new(1, i));
            i += 1;
        }
        assert!(
            index.txn.size(&index.index_file_name).saturating_sub(2) >= 3,
            "expected a second split to create a third leaf"
        );

        {
            // Follow sibling pointers from block 0 to gather the chain
            let mut blocks = vec![2usize];
            let mut current = 2usize;
            while blocks.len() < 5 {
                let view = index
                    .txn
                    .pin_read_guard(&BlockId::new(index.index_file_name.clone(), current))
                    .unwrap()
                    .into_btree_leaf_page_view(&index.leaf_layout)
                    .unwrap();
                if let Some(rs) = view.right_sibling_block() {
                    blocks.push(rs);
                    current = rs;
                } else {
                    break;
                }
            }
            assert_eq!(
                blocks.len(),
                3,
                "expected two splits to yield a chain of three leaves, got {:?}",
                blocks
            );

            let l0 = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), blocks[0]))
                .unwrap()
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let l1 = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), blocks[1]))
                .unwrap()
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let l2 = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), blocks[2]))
                .unwrap()
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();

            let l1_first = l1.get_entry(0).unwrap().key;
            let l2_first = l2.get_entry(0).unwrap().key;

            assert_eq!(
                l0.high_key(),
                Some(l1_first.clone()),
                "leftmost high key should match l1 first key"
            );
            assert_eq!(l0.right_sibling_block(), Some(blocks[1]));
            assert_eq!(
                l1.high_key(),
                Some(l2_first.clone()),
                "middle high key should match l2 first key"
            );
            assert_eq!(l1.right_sibling_block(), Some(blocks[2]));
            assert_eq!(l2.high_key(), None);
            assert_eq!(l2.right_sibling_block(), None);

            // All pages should have at least one entry, ensuring splits distributed records
            assert!(l0.slot_count() > 0);
            assert!(l1.slot_count() > 0);
            assert!(l2.slot_count() > 0);
        }
    }

    #[test]
    fn test_free_list_push_pop_behavior() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let index = setup_index(&db);

        // Append a spare block, then push it onto the free list.
        let spare = index.txn.append(&index.index_file_name);
        IndexFreeList::deallocate(&index.txn, &index.index_file_name, spare.block_num).unwrap();

        {
            let guard = index.txn.pin_read_guard(&index.meta_block).unwrap();
            let meta = BTreeMetaPageView::new(guard).unwrap();
            assert_eq!(meta.first_free_block(), spare.block_num as u32);
        }

        // Pop from free list; allocator should return the same block.
        let reused = IndexFreeList::allocate(&index.txn, &index.index_file_name).unwrap();
        assert_eq!(reused.block_id.block_num, spare.block_num);

        {
            let guard = index.txn.pin_read_guard(&index.meta_block).unwrap();
            let meta = BTreeMetaPageView::new(guard).unwrap();
            assert_eq!(meta.first_free_block(), IndexFreeList::no_free_block());
        }
    }

    #[test]
    #[ignore]
    fn recovery_undoes_uncommitted_btree_split_cascade() {
        let dir = TestDir::new(format!(
            "/tmp/recovery_test/split_cascade_{}",
            generate_random_number()
        ));
        let layout = create_test_layout();
        let index_name = generate_filename();

        let (committed_root_block, committed_tree_height, index_file_name) = {
            let db = SimpleDB::new(&dir, 8, true, 5000);

            // Transaction 1: establish committed baseline.
            let t1 = db.new_tx();
            let mut idx = BTreeIndex::new(
                Arc::clone(&t1),
                &index_name,
                layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            for key in 0..120 {
                idx.insert(&Constant::Int(key), &RID::new(1, key as usize));
            }
            let committed_root_block = idx.root_block.block_num as u32;
            let committed_tree_height = idx.tree_height;
            let index_file_name = idx.index_file_name.clone();
            t1.commit().unwrap();

            // Transaction 2: force many uncommitted splits/cascades.
            let t2 = db.new_tx();
            let mut idx2 = BTreeIndex::new(
                Arc::clone(&t2),
                &index_name,
                layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            let blocks_before = idx2.txn.size(&idx2.index_file_name);
            for key in 1000..1600 {
                idx2.insert(&Constant::Int(key), &RID::new(2, key as usize));
            }
            let blocks_after = idx2.txn.size(&idx2.index_file_name);
            assert!(
                blocks_after > blocks_before,
                "expected uncommitted inserts to trigger split allocations"
            );
            // No commit: simulate crash.
            (committed_root_block, committed_tree_height, index_file_name)
        };

        // Recover in fresh DB process view.
        let db = SimpleDB::new(&dir, 8, false, 5000);
        let recovery_tx = db.new_tx();
        recovery_tx.recover().unwrap();

        let verify_tx = db.new_tx();
        let mut verify_index = BTreeIndex::new(
            Arc::clone(&verify_tx),
            &index_name,
            layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();

        // Baseline committed keys must remain queryable.
        for key in 0..120 {
            verify_index.before_first(&Constant::Int(key));
            assert!(verify_index.next(), "committed key {key} should remain");
            assert_eq!(verify_index.get_data_rid(), RID::new(1, key as usize));
        }

        // Uncommitted keys must be absent after recovery undo.
        for key in 1000..1600 {
            verify_index.before_first(&Constant::Int(key));
            assert!(
                !verify_index.next(),
                "uncommitted key {key} should be undone by recovery"
            );
        }

        // Meta state should match committed baseline.
        let guard = verify_tx
            .pin_read_guard(&BlockId::new(index_file_name, 0))
            .unwrap();
        let meta = BTreeMetaPageView::new(guard).unwrap();
        assert_eq!(meta.root_block(), committed_root_block);
        assert_eq!(meta.tree_height(), committed_tree_height);
    }

    #[test]
    fn rollback_btree_split_cascade_restores_tree_and_reuses_freelist() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let layout = create_test_layout();
        let index_name = generate_filename();

        // Transaction 1: committed baseline with enough keys to form a non-trivial tree.
        let t1 = db.new_tx();
        let mut baseline_idx = BTreeIndex::new(
            Arc::clone(&t1),
            &index_name,
            layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        for key in 0..120 {
            baseline_idx.insert(&Constant::Int(key), &RID::new(1, key as usize));
        }
        let baseline_root_block = baseline_idx.root_block.block_num as u32;
        let baseline_tree_height = baseline_idx.tree_height;
        let index_file_name = baseline_idx.index_file_name.clone();
        t1.commit().unwrap();

        // Transaction 2: uncommitted heavy insert workload causing split cascades.
        let t2 = db.new_tx();
        let mut idx2 = BTreeIndex::new(
            Arc::clone(&t2),
            &index_name,
            layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        let blocks_before = idx2.txn.size(&idx2.index_file_name);
        for key in 1000..1600 {
            idx2.insert(&Constant::Int(key), &RID::new(2, key as usize));
        }
        let blocks_after = idx2.txn.size(&idx2.index_file_name);
        assert!(
            blocks_after > blocks_before,
            "expected uncommitted inserts to allocate split pages"
        );
        t2.rollback().unwrap();

        // Transaction 3: verify logical state restored to baseline.
        let t3 = db.new_tx();
        let mut verify_idx = BTreeIndex::new(
            Arc::clone(&t3),
            &index_name,
            layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        for key in 0..120 {
            verify_idx.before_first(&Constant::Int(key));
            assert!(
                verify_idx.next(),
                "committed key {key} should remain after rollback"
            );
            assert_eq!(verify_idx.get_data_rid(), RID::new(1, key as usize));
        }
        for key in 1000..1600 {
            verify_idx.before_first(&Constant::Int(key));
            assert!(
                !verify_idx.next(),
                "rolled-back key {key} should be absent after rollback"
            );
        }

        // Meta should match committed baseline.
        let meta_guard = t3
            .pin_read_guard(&BlockId::new(index_file_name.clone(), 0))
            .unwrap();
        let meta = BTreeMetaPageView::new(meta_guard).unwrap();
        assert_eq!(meta.root_block(), baseline_root_block);
        assert_eq!(meta.tree_height(), baseline_tree_height);
        let free_head = meta.first_free_block();
        assert_ne!(
            free_head,
            IndexFreeList::no_free_block(),
            "rollback should reclaim split-allocated pages into free list"
        );
        drop(meta);

        // Free-list pop should reuse reclaimed page (head).
        let reused = IndexFreeList::allocate(&t3, &index_file_name).unwrap();
        assert_eq!(reused.block_id.block_num, free_head as usize);
        assert!(
            reused.block_id.block_num >= blocks_before && reused.block_id.block_num < blocks_after,
            "reused block should come from rollback-reclaimed split allocation range"
        );
    }

    // -------------------------------------------------------------------------
    // Concurrent correctness tests
    // -------------------------------------------------------------------------

    /// 4 reader threads each do 50 point-lookups on 200 pre-committed keys.
    /// Verifies that concurrent S-lock holders do not interfere.
    #[test]
    fn test_concurrent_reads() {
        use std::sync::Barrier;
        use std::thread;

        const READERS: usize = 4;
        const LOOKUPS_PER_READER: usize = 50;
        const PRELOAD: i32 = 200;

        let (db, _dir) = SimpleDB::new_for_test(64, 5000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();
        let split_gate = Arc::new(SplitGate::new());

        // Pre-populate with PRELOAD keys in a single committed transaction.
        {
            let setup_tx = db.new_tx();
            let mut idx = BTreeIndex::new(
                Arc::clone(&setup_tx),
                &index_name,
                leaf_layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::clone(&split_gate),
            )
            .unwrap();
            for k in 0..PRELOAD {
                idx.insert(&Constant::Int(k), &RID::new(1, k as usize));
            }
            setup_tx.commit().unwrap();
        }

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let index_name = Arc::new(index_name);
        let leaf_layout = Arc::new(leaf_layout);
        let split_gate = Arc::clone(&split_gate);
        let barrier = Arc::new(Barrier::new(READERS));

        let mut handles = vec![];
        for reader_id in 0..READERS {
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            let iname = Arc::clone(&index_name);
            let ilayout = Arc::clone(&leaf_layout);
            let gate = Arc::clone(&split_gate);
            let bar = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                bar.wait();
                let mut all_found = true;
                for i in 0..LOOKUPS_PER_READER {
                    let key = ((reader_id * LOOKUPS_PER_READER + i) % PRELOAD as usize) as i32;
                    let txn = Arc::new(Transaction::new(
                        Arc::clone(&fm),
                        Arc::clone(&lm),
                        Arc::clone(&bm),
                        Arc::clone(&lt),
                    ));
                    let mut idx = BTreeIndex::new(
                        Arc::clone(&txn),
                        &iname,
                        (*ilayout).clone(),
                        TEST_INDEXED_TABLE_ID,
                        Arc::clone(&gate),
                    )
                    .unwrap();
                    idx.before_first(&Constant::Int(key));
                    if !idx.next() {
                        all_found = false;
                    }
                    txn.commit().unwrap();
                }
                all_found
            }));
        }

        for h in handles {
            assert!(h.join().unwrap(), "reader thread missed a key");
        }
    }

    /// 4 writer threads each insert 100 keys in disjoint ranges.
    /// After joining, verify all 400 keys are present and the tree is structurally valid.
    #[test]
    fn test_concurrent_writes_disjoint_ranges() {
        use std::sync::Barrier;
        use std::thread;

        const WRITERS: usize = 4;
        const KEYS_PER_WRITER: usize = 100;

        let (db, _dir) = SimpleDB::new_for_test(64, 5000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();

        // Bootstrap the index file (no pre-inserted keys).
        {
            let setup_tx = db.new_tx();
            let _ = BTreeIndex::new(
                Arc::clone(&setup_tx),
                &index_name,
                leaf_layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            setup_tx.commit().unwrap();
        }

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let index_name = Arc::new(index_name);
        let leaf_layout = Arc::new(leaf_layout);
        let barrier = Arc::new(Barrier::new(WRITERS));

        let mut handles = vec![];
        for writer_id in 0..WRITERS {
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            let iname = Arc::clone(&index_name);
            let ilayout = Arc::clone(&leaf_layout);
            let bar = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                bar.wait();
                let base = (writer_id * KEYS_PER_WRITER) as i32;
                for k in base..base + KEYS_PER_WRITER as i32 {
                    let txn = Arc::new(Transaction::new(
                        Arc::clone(&fm),
                        Arc::clone(&lm),
                        Arc::clone(&bm),
                        Arc::clone(&lt),
                    ));
                    let mut idx = BTreeIndex::new(
                        Arc::clone(&txn),
                        &iname,
                        (*ilayout).clone(),
                        TEST_INDEXED_TABLE_ID,
                        Arc::new(SplitGate::new()),
                    )
                    .unwrap();
                    idx.insert(&Constant::Int(k), &RID::new(1, k as usize));
                    txn.commit().unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        // Verify all 400 keys are present and tree is structurally sound.
        let val_txn = Arc::new(Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ));
        let mut val_idx = BTreeIndex::new(
            Arc::clone(&val_txn),
            &index_name,
            (*leaf_layout).clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        let total = WRITERS * KEYS_PER_WRITER;
        for k in 0..total as i32 {
            val_idx.before_first(&Constant::Int(k));
            assert!(
                val_idx.next(),
                "key {k} missing after concurrent disjoint writes"
            );
        }
        super::validator::validate_btree_integrity(&val_txn, &val_idx).unwrap();
        val_txn.commit().unwrap();
    }

    /// 4 writer threads insert interleaved keys (thread i: i, i+4, i+8, …) to force
    /// splits under contention on shared leaf pages.
    #[test]
    fn test_concurrent_split_stress() {
        use std::sync::Barrier;
        use std::thread;

        const WRITERS: usize = 4;
        const KEYS_PER_WRITER: usize = 150;

        let (db, _dir) = SimpleDB::new_for_test(64, 5000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();

        {
            let setup_tx = db.new_tx();
            let _ = BTreeIndex::new(
                Arc::clone(&setup_tx),
                &index_name,
                leaf_layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            setup_tx.commit().unwrap();
        }

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let index_name = Arc::new(index_name);
        let leaf_layout = Arc::new(leaf_layout);
        let barrier = Arc::new(Barrier::new(WRITERS));

        let mut handles = vec![];
        for writer_id in 0..WRITERS {
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            let iname = Arc::clone(&index_name);
            let ilayout = Arc::clone(&leaf_layout);
            let bar = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                bar.wait();
                // Thread i inserts keys: i, i+4, i+8, …
                for j in 0..KEYS_PER_WRITER {
                    let k = (writer_id + j * WRITERS) as i32;
                    let txn = Arc::new(Transaction::new(
                        Arc::clone(&fm),
                        Arc::clone(&lm),
                        Arc::clone(&bm),
                        Arc::clone(&lt),
                    ));
                    let mut idx = BTreeIndex::new(
                        Arc::clone(&txn),
                        &iname,
                        (*ilayout).clone(),
                        TEST_INDEXED_TABLE_ID,
                        Arc::new(SplitGate::new()),
                    )
                    .unwrap();
                    idx.insert(&Constant::Int(k), &RID::new(1, k as usize));
                    txn.commit().unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        let total = WRITERS * KEYS_PER_WRITER; // 600 keys: 0..600
        let val_txn = Arc::new(Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ));
        let mut val_idx = BTreeIndex::new(
            Arc::clone(&val_txn),
            &index_name,
            (*leaf_layout).clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        for k in 0..total as i32 {
            val_idx.before_first(&Constant::Int(k));
            assert!(val_idx.next(), "key {k} missing after split stress");
        }
        super::validator::validate_btree_integrity(&val_txn, &val_idx).unwrap();
        val_txn.commit().unwrap();
    }

    /// 1 writer thread inserts new keys while 3 reader threads lookup pre-committed keys.
    /// Readers must always find the pre-committed keys (no dirty-read or lost-read).
    #[test]
    fn test_concurrent_read_write() {
        use std::sync::{
            atomic::{AtomicUsize, Ordering},
            Barrier,
        };
        use std::thread;

        const PRELOAD: i32 = 100;
        const WRITER_KEYS: i32 = 100; // inserts PRELOAD..PRELOAD+WRITER_KEYS
        const READERS: usize = 3;
        const LOOKUPS_PER_READER: usize = 30;

        let (db, _dir) = SimpleDB::new_for_test(64, 5000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();

        {
            let setup_tx = db.new_tx();
            let mut idx = BTreeIndex::new(
                Arc::clone(&setup_tx),
                &index_name,
                leaf_layout.clone(),
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            for k in 0..PRELOAD {
                idx.insert(&Constant::Int(k), &RID::new(1, k as usize));
            }
            setup_tx.commit().unwrap();
        }

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let index_name = Arc::new(index_name);
        let leaf_layout = Arc::new(leaf_layout);

        let barrier = Arc::new(Barrier::new(1 + READERS));
        let missed = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        // Writer thread
        {
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            let iname = Arc::clone(&index_name);
            let ilayout = Arc::clone(&leaf_layout);
            let bar = Arc::clone(&barrier);

            handles.push(thread::spawn(move || {
                bar.wait();
                for k in PRELOAD..PRELOAD + WRITER_KEYS {
                    let txn = Arc::new(Transaction::new(
                        Arc::clone(&fm),
                        Arc::clone(&lm),
                        Arc::clone(&bm),
                        Arc::clone(&lt),
                    ));
                    let mut idx = BTreeIndex::new(
                        Arc::clone(&txn),
                        &iname,
                        (*ilayout).clone(),
                        TEST_INDEXED_TABLE_ID,
                        Arc::new(SplitGate::new()),
                    )
                    .unwrap();
                    idx.insert(&Constant::Int(k), &RID::new(2, k as usize));
                    txn.commit().unwrap();
                }
            }));
        }

        // Reader threads
        for reader_id in 0..READERS {
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            let iname = Arc::clone(&index_name);
            let ilayout = Arc::clone(&leaf_layout);
            let bar = Arc::clone(&barrier);
            let missed_ref = Arc::clone(&missed);

            handles.push(thread::spawn(move || {
                bar.wait();
                for i in 0..LOOKUPS_PER_READER {
                    let k = ((reader_id * LOOKUPS_PER_READER + i) % PRELOAD as usize) as i32;
                    let txn = Arc::new(Transaction::new(
                        Arc::clone(&fm),
                        Arc::clone(&lm),
                        Arc::clone(&bm),
                        Arc::clone(&lt),
                    ));
                    let mut idx = BTreeIndex::new(
                        Arc::clone(&txn),
                        &iname,
                        (*ilayout).clone(),
                        TEST_INDEXED_TABLE_ID,
                        Arc::new(SplitGate::new()),
                    )
                    .unwrap();
                    idx.before_first(&Constant::Int(k));
                    if !idx.next() {
                        missed_ref.fetch_add(1, Ordering::Relaxed);
                    }
                    txn.commit().unwrap();
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(
            missed.load(Ordering::Relaxed),
            0,
            "readers missed pre-committed keys during concurrent writes"
        );

        let val_txn = Arc::new(Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ));
        let val_idx = BTreeIndex::new(
            Arc::clone(&val_txn),
            &index_name,
            (*leaf_layout).clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        super::validator::validate_btree_integrity(&val_txn, &val_idx).unwrap();
        val_txn.commit().unwrap();
    }

    #[test]
    fn test_range_lock_blocks_overlapping_insert() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let (db, _dir) = SimpleDB::new_for_test(64, 2_000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();

        let setup_tx = db.new_tx();
        let _idx = BTreeIndex::new(
            Arc::clone(&setup_tx),
            &index_name,
            leaf_layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        setup_tx.commit().unwrap();

        let holder_tx = db.new_tx();
        let index_lock_table_id = BTreeIndex::index_lock_table_id_for(TEST_INDEXED_TABLE_ID);
        holder_tx.lock_table_is(index_lock_table_id).unwrap();
        holder_tx
            .lock_index_range_s(index_lock_table_id, Constant::Int(20), Constant::Int(30))
            .unwrap();

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let (started_tx, started_rx) = mpsc::channel();
        let (done_tx, done_rx) = mpsc::channel();
        let index_name_thread = index_name.clone();
        let leaf_layout_thread = leaf_layout.clone();

        let handle = thread::spawn(move || {
            let txn = Arc::new(Transaction::new(
                file_manager,
                log_manager,
                buffer_manager,
                lock_table,
            ));
            let mut idx = BTreeIndex::new(
                Arc::clone(&txn),
                &index_name_thread,
                leaf_layout_thread,
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            started_tx.send(()).unwrap();
            idx.insert(&Constant::Int(25), &RID::new(9, 25));
            txn.commit().unwrap();
            done_tx.send(()).unwrap();
        });

        started_rx.recv().unwrap();
        assert!(
            done_rx.recv_timeout(Duration::from_millis(100)).is_err(),
            "overlapping insert should block while range-S lock is held"
        );

        holder_tx.commit().unwrap();
        done_rx
            .recv_timeout(Duration::from_secs(2))
            .expect("insert should complete after range lock is released");
        handle.join().unwrap();

        let verify_tx = db.new_tx();
        let mut verify_idx = BTreeIndex::new(
            Arc::clone(&verify_tx),
            &index_name,
            leaf_layout,
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        verify_idx.before_first(&Constant::Int(25));
        assert!(verify_idx.next());
        assert_eq!(verify_idx.get_data_rid(), RID::new(9, 25));
        verify_tx.commit().unwrap();
    }

    #[test]
    fn test_range_lock_allows_disjoint_insert() {
        use std::sync::mpsc;
        use std::thread;
        use std::time::Duration;

        let (db, _dir) = SimpleDB::new_for_test(64, 2_000);
        db.set_wal_mode(WalMode::UnsafeNoWal);
        let index_name = generate_filename();
        let leaf_layout = create_test_layout();

        let setup_tx = db.new_tx();
        let _idx = BTreeIndex::new(
            Arc::clone(&setup_tx),
            &index_name,
            leaf_layout.clone(),
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        setup_tx.commit().unwrap();

        let holder_tx = db.new_tx();
        let index_lock_table_id = BTreeIndex::index_lock_table_id_for(TEST_INDEXED_TABLE_ID);
        holder_tx.lock_table_is(index_lock_table_id).unwrap();
        holder_tx
            .lock_index_range_s(index_lock_table_id, Constant::Int(20), Constant::Int(30))
            .unwrap();

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = db.log_manager();
        let buffer_manager = db.buffer_manager();
        let lock_table = db.lock_table();
        let (done_tx, done_rx) = mpsc::channel();
        let index_name_thread = index_name.clone();
        let leaf_layout_thread = leaf_layout.clone();

        let handle = thread::spawn(move || {
            let txn = Arc::new(Transaction::new(
                file_manager,
                log_manager,
                buffer_manager,
                lock_table,
            ));
            let mut idx = BTreeIndex::new(
                Arc::clone(&txn),
                &index_name_thread,
                leaf_layout_thread,
                TEST_INDEXED_TABLE_ID,
                Arc::new(SplitGate::new()),
            )
            .unwrap();
            idx.insert(&Constant::Int(40), &RID::new(9, 40));
            txn.commit().unwrap();
            done_tx.send(()).unwrap();
        });

        done_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("disjoint insert should not block on non-overlapping range lock");
        handle.join().unwrap();
        holder_tx.commit().unwrap();

        let verify_tx = db.new_tx();
        let mut verify_idx = BTreeIndex::new(
            Arc::clone(&verify_tx),
            &index_name,
            leaf_layout,
            TEST_INDEXED_TABLE_ID,
            Arc::new(SplitGate::new()),
        )
        .unwrap();
        verify_idx.before_first(&Constant::Int(40));
        assert!(verify_idx.next());
        assert_eq!(verify_idx.get_data_rid(), RID::new(9, 40));
        verify_tx.commit().unwrap();
    }
}

/// Structural integrity validator for a B-tree index.
///
/// Exposes `validate_btree_integrity` for use in tests across the crate.
/// Keep `collect_leaf_blocks` private to this module — callers only need the top-level validator.
#[cfg(test)]
pub mod validator {
    use super::*;

    /// Walk one internal page, verify key ordering, and return children in left-to-right order.
    /// For level-0 pages the children are leaf block numbers; for higher levels, recurse.
    pub(super) fn collect_leaf_blocks(
        txn: &Arc<Transaction>,
        internal_layout: &Layout,
        file_name: &str,
        block_id: &BlockId,
    ) -> SimpleDBResult<Vec<usize>> {
        let guard = txn.pin_read_guard(block_id)?;
        let view = guard.into_btree_internal_page_view(internal_layout)?;
        let level = view.btree_level();
        let slot_count = view.slot_count();

        // Verify keys are sorted strictly ascending within the page.
        for i in 1..slot_count {
            let prev = view.get_entry(i - 1)?.key.clone();
            let curr = view.get_entry(i)?.key.clone();
            if prev >= curr {
                return Err(format!(
                    "Internal page block {}: keys out of order at slot {}: {:?} >= {:?}",
                    block_id.block_num, i, prev, curr
                )
                .into());
            }
        }

        // Collect child block numbers in traversal order:
        // entry[0].child_block, entry[1].child_block, ..., entry[n-1].child_block, rightmost
        let mut children = Vec::with_capacity(slot_count + 1);
        for i in 0..slot_count {
            children.push(view.get_entry(i)?.child_block);
        }
        let rightmost = view.rightmost_child_block().ok_or_else(|| {
            format!(
                "Internal page block {} missing rightmost child",
                block_id.block_num
            )
        })?;
        children.push(rightmost);
        drop(view);

        if level == 0 {
            Ok(children)
        } else {
            let mut leaves = Vec::new();
            for child_num in children {
                let child_id = BlockId::new(file_name.to_string(), child_num);
                let child_leaves = collect_leaf_blocks(txn, internal_layout, file_name, &child_id)?;
                leaves.extend(child_leaves);
            }
            Ok(leaves)
        }
    }

    /// Verify structural invariants of a B-tree index.
    ///
    /// Phase 1: Walk internal pages top-down to collect leaf block numbers in order.
    /// Phase 2: Walk the leaf sibling chain and verify:
    ///   - Keys within each page are sorted ascending.
    ///   - All keys on a page are < the page's high_key (if set).
    ///   - The high_key of page N equals the first key of page N+1 (separator invariant).
    ///   - The sibling chain visits exactly the same blocks as Phase 1 collected.
    pub fn validate_btree_integrity(
        txn: &Arc<Transaction>,
        index: &BTreeIndex,
    ) -> SimpleDBResult<()> {
        // Phase 1
        let leaf_blocks = collect_leaf_blocks(
            txn,
            &index.internal_layout,
            &index.index_file_name,
            &index.root_block,
        )?;
        if leaf_blocks.is_empty() {
            return Err("B-tree has no leaf blocks".into());
        }

        // Phase 2
        let mut sibling_chain: Vec<usize> = Vec::new();
        let mut expected_first_key: Option<Constant> = None;
        let mut global_prev_key: Option<Constant> = None;
        let mut current_block = leaf_blocks[0];

        loop {
            let block_id = BlockId::new(index.index_file_name.clone(), current_block);
            let guard = txn.pin_read_guard(&block_id)?;
            let view = guard.into_btree_leaf_page_view(&index.leaf_layout)?;

            sibling_chain.push(current_block);
            let slot_count = view.slot_count();
            let high_key = view.high_key();
            let rsib = view.right_sibling_block();

            // Check first key matches the previous page's high_key.
            if let Some(ref expected) = expected_first_key {
                if slot_count > 0 {
                    let first = view.get_entry(0)?.key.clone();
                    if first != *expected {
                        return Err(format!(
                            "Leaf block {}: first key ({:?}) != expected separator ({:?})",
                            current_block, first, expected
                        )
                        .into());
                    }
                }
            }

            // Verify keys within page are sorted ascending.
            for i in 1..slot_count {
                let prev = view.get_entry(i - 1)?.key.clone();
                let curr = view.get_entry(i)?.key.clone();
                if prev > curr {
                    return Err(format!(
                        "Leaf block {}: keys out of order at slot {}: {:?} > {:?}",
                        current_block, i, prev, curr
                    )
                    .into());
                }
            }

            // Verify all keys are strictly below the high_key.
            if let Some(ref hk) = high_key {
                for i in 0..slot_count {
                    let k = view.get_entry(i)?.key.clone();
                    if k >= *hk {
                        return Err(format!(
                            "Leaf block {}: key at slot {} ({:?}) >= high_key ({:?})",
                            current_block, i, k, hk
                        )
                        .into());
                    }
                }
            }

            // Verify global ordering: first key of this page >= last key of previous page.
            if let Some(ref prev_k) = global_prev_key {
                if slot_count > 0 {
                    let first = view.get_entry(0)?.key.clone();
                    if first < *prev_k {
                        return Err(format!(
                            "Leaf block {}: first key ({:?}) < prev page last key ({:?})",
                            current_block, first, prev_k
                        )
                        .into());
                    }
                }
            }

            if slot_count > 0 {
                global_prev_key = Some(view.get_entry(slot_count - 1)?.key.clone());
            }
            expected_first_key = high_key;

            match rsib {
                Some(rsib_block) => current_block = rsib_block,
                None => break,
            }
        }

        if sibling_chain != leaf_blocks {
            return Err(format!(
                "Sibling chain {:?} != internal-page leaf list {:?}",
                sibling_chain, leaf_blocks
            )
            .into());
        }

        Ok(())
    }
}
