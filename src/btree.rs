use std::{error::Error, sync::Arc};

#[cfg(test)]
use crate::page::BTreeLeafEntry;
use crate::{
    debug,
    page::{
        BTreeInternalEntry, BTreeInternalPageView, BTreeInternalPageViewMut, BTreeMetaPageView,
        BTreeMetaPageViewMut,
    },
    BlockId, Constant, Index, IndexInfo, Layout, Lsn, Schema, Transaction, RID,
};

/// Separator promoted from a child split.
#[derive(Debug, Clone)]
struct SplitResult {
    sep_key: Constant,
    left_block: usize,
    right_block: usize,
}

pub struct BTreeIndex {
    txn: Arc<Transaction>,
    index_name: String,
    index_file_name: String,
    internal_layout: Layout,
    leaf_layout: Layout,
    leaf: Option<BTreeLeaf>,
    meta_block: BlockId,
    root_block: BlockId,
    tree_height: u16,
}

impl std::fmt::Display for BTreeIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BTreeIndex({})", self.index_name)
    }
}

impl BTreeIndex {
    pub fn new(
        txn: Arc<Transaction>,
        index_name: &str,
        leaf_layout: Layout,
    ) -> Result<Self, Box<dyn Error>> {
        let index_file_name = format!("{index_name}.idx");
        let meta_block = BlockId::new(index_file_name.clone(), 0);
        let mut internal_schema = Schema::new();
        internal_schema.add_from_schema(IndexInfo::BLOCK_NUM_FIELD, &leaf_layout.schema)?;
        internal_schema.add_from_schema(IndexInfo::DATA_FIELD, &leaf_layout.schema)?;
        let internal_layout = Layout::new(internal_schema.clone());

        // Bootstrap single-file index if missing.
        let (root_block, tree_height) = if txn.size(&index_file_name) == 0 {
            // Block 0: meta
            let meta_id = txn.append(&index_file_name);
            assert_eq!(meta_id.block_num, 0);
            {
                let mut guard = txn.pin_write_guard(&meta_id);
                guard.format_as_btree_meta(1, 1, 1, u32::MAX);
                guard.mark_modified(txn.id(), Lsn::MAX);
            }

            // Block 1: root internal (level 0 -> children are leaves)
            let root_id = txn.append(&index_file_name);
            assert_eq!(root_id.block_num, 1);
            {
                let mut guard = txn.pin_write_guard(&root_id);
                // rightmost child will point to first leaf (block 2)
                guard.format_as_btree_internal(0, Some(2));
                guard.mark_modified(txn.id(), Lsn::MAX);
            }

            // Block 2: first leaf
            let leaf_id = txn.append(&index_file_name);
            assert_eq!(leaf_id.block_num, 2);
            {
                let mut guard = txn.pin_write_guard(&leaf_id);
                guard.format_as_btree_leaf(None);
                guard.mark_modified(txn.id(), Lsn::MAX);
            }
            (root_id, 1)
        } else {
            // Load meta
            let guard = txn.pin_read_guard(&meta_block);
            let meta_view = BTreeMetaPageView::new(guard)?;
            let root_blk = meta_view.root_block() as usize;
            let height = meta_view.tree_height();
            let root_block = BlockId::new(index_file_name.clone(), root_blk);
            (root_block, height)
        };

        Ok(Self {
            txn,
            index_name: index_name.to_string(),
            index_file_name,
            internal_layout,
            leaf_layout,
            meta_block,
            root_block,
            leaf: None,
            tree_height,
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

    fn update_meta(&mut self) -> Result<(), Box<dyn Error>> {
        let guard = self.txn.pin_write_guard(&self.meta_block);
        guard.mark_modified(self.txn.id(), Lsn::MAX);
        let mut view = BTreeMetaPageViewMut::new(guard)?;
        view.set_tree_height(self.tree_height);
        view.set_root_block(self.root_block.block_num as u32);
        view.update_crc32();
        Ok(())
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
            let guard = self.txn.pin_read_guard(&block);
            let view = guard.into_btree_leaf_page_view(&self.layout).ok()?;

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
                    if let Some(ref up) = self.upper {
                        if entry.key >= **up {
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

impl Index for BTreeIndex {
    fn before_first(&mut self, search_key: &Constant) {
        let mut root = BTreeInternal::new(
            Arc::clone(&self.txn),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.index_file_name.clone(),
        );
        let leaf_block_num = root.search(search_key).unwrap();
        let leaf_block_id = BlockId::new(self.index_file_name.clone(), leaf_block_num);
        self.leaf = Some(
            BTreeLeaf::new(
                Arc::clone(&self.txn),
                leaf_block_id.clone(),
                self.leaf_layout.clone(),
                search_key.clone(),
                leaf_block_id.filename,
            )
            .unwrap(),
        );
    }

    fn next(&mut self) -> bool {
        self.leaf
            .as_mut()
            .expect("Leaf not initialized, did you forget to call before_first?")
            .next()
            .expect("Next failed")
            .is_some()
    }

    fn get_data_rid(&self) -> RID {
        self.leaf.as_ref().unwrap().get_data_rid().unwrap()
    }

    fn insert(&mut self, data_val: &Constant, data_rid: &RID) {
        debug!(
            "Inserting value {:?} for rid {:?} into index",
            data_val, data_rid
        );
        self.before_first(data_val);
        let split = self.leaf.as_mut().unwrap().insert(*data_rid).unwrap();
        if split.is_none() {
            return;
        }
        debug!("Insert in index caused a split");
        let split = split.unwrap();
        let root = BTreeInternal::new(
            Arc::clone(&self.txn),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.index_file_name.clone(),
        );
        let root_split = root
            .insert_entry(BTreeInternalEntry {
                key: split.sep_key.clone(),
                child_block: split.right_block,
            })
            .unwrap();
        if root_split.is_none() {
            return;
        }
        debug!("Insert in index caused a root split");
        let root_split = root_split.unwrap();
        root.make_new_root(root_split).unwrap();
        self.tree_height = self.tree_height.saturating_add(1);
        self.update_meta().unwrap();
    }

    fn delete(&mut self, data_val: &Constant, data_rid: &RID) {
        self.before_first(data_val);
        self.leaf.as_mut().unwrap().delete(*data_rid).unwrap();
        //  TODO: Should the leaf be set to None here?
        self.leaf = None;
    }
}

#[cfg(test)]
mod btree_index_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

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
        BTreeIndex::new(Arc::clone(&tx), &index_name, layout).unwrap()
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

    /// Combined test replacing:
    /// - test_btree_index_construction
    /// - test_duplicate_keys
    /// - test_delete
    #[test]
    fn test_btree_comprehensive_operations() {
        // Test construction (was test_btree_index_construction)
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let index = setup_index(&db);

        let root = BTreeInternal::new(
            Arc::clone(&index.txn),
            index.root_block.clone(),
            index.internal_layout.clone(),
            index.index_file_name.clone(),
        );
        let guard = index.txn.pin_read_guard(&root.block_id);
        let view = BTreeInternalPageView::new(guard, &root.layout).unwrap();
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
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 0));
            let meta = BTreeMetaPageView::new(guard).expect("meta page view");
            assert_eq!(meta.version(), 1);
            assert_eq!(meta.tree_height(), 1);
            assert_eq!(meta.root_block(), 1);
            assert_eq!(meta.first_free_block(), u32::MAX);
        }

        // Root internal assertions
        {
            let guard = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 1));
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
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 2));
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
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), 0));
            let meta = BTreeMetaPageView::new(guard).expect("meta page view");
            assert_eq!(meta.root_block() as usize, index.root_block.block_num);
            assert_eq!(meta.tree_height(), index.tree_height);
        }
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
            let mut root = BTreeInternal::new(
                Arc::clone(&index.txn),
                index.root_block.clone(),
                index.internal_layout.clone(),
                index.index_file_name.clone(),
            );
            let blk = root.search(&lower).unwrap();
            let block_id = BlockId::new(index.index_file_name.clone(), blk);
            let slot = {
                let guard = index.txn.pin_read_guard(&block_id);
                let view = guard.into_btree_leaf_page_view(&index.leaf_layout).unwrap();
                view.find_slot_before(&lower)
            };
            (block_id, slot)
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
            let mut root = BTreeInternal::new(
                Arc::clone(&index.txn),
                index.root_block.clone(),
                index.internal_layout.clone(),
                index.index_file_name.clone(),
            );
            let blk = root.search(&lower_b).unwrap();
            let block_id = BlockId::new(index.index_file_name.clone(), blk);
            let slot = {
                let guard = index.txn.pin_read_guard(&block_id);
                let view = guard.into_btree_leaf_page_view(&index.leaf_layout).unwrap();
                view.find_slot_before(&lower_b)
            };
            (block_id, slot)
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
        assert!(collected_b.iter().all(|&v| v >= 10 && v < 50));
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
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let rsib = left_view
                .right_sibling_block()
                .expect("left page should link to split sibling");
            let right_id = BlockId::new(index.index_file_name.clone(), rsib);
            let right_view = index
                .txn
                .pin_read_guard(&right_id)
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
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let l1 = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), blocks[1]))
                .into_btree_leaf_page_view(&index.leaf_layout)
                .unwrap();
            let l2 = index
                .txn
                .pin_read_guard(&BlockId::new(index.index_file_name.clone(), blocks[2]))
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
}

/// The general format of the BTreePage
///
/// The format of the record slot for the leaf page
/// +-------------+---------------+--------------+
/// | key         | block number  | slot number  |
/// +-------------+---------------+--------------+
///
/// The format of the record slot for the internal page
/// +-------------+------------------+
/// | key         | child block num  |
/// +-------------+------------------+
struct BTreeInternal {
    txn: Arc<Transaction>,
    block_id: BlockId,
    layout: Layout,
    file_name: String,
}

impl std::fmt::Display for BTreeInternal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreeInternal Debug ===")?;
        writeln!(f, "Block ID: {:?}", self.block_id)?;
        writeln!(f, "File Name: {}", self.file_name)?;
        Ok(())
    }
}

impl BTreeInternal {
    fn new(txn: Arc<Transaction>, block_id: BlockId, layout: Layout, file_name: String) -> Self {
        Self {
            txn,
            block_id,
            layout,
            file_name,
        }
    }

    /// Helper method to split an internal page by moving entries from [split_slot..] onwards
    fn split_page(&self, split_slot: usize) -> Result<BlockId, Box<dyn Error>> {
        let txn_id = self.txn.id();
        let orig_guard = self.txn.pin_write_guard(&self.block_id);
        let mut orig_view = BTreeInternalPageViewMut::new(orig_guard, &self.layout)?;

        let new_block_id = self.txn.append(&self.file_name);
        let mut new_guard = self.txn.pin_write_guard(&new_block_id);
        // rightmost will be set after we compute child partitions
        new_guard.format_as_btree_internal(orig_view.btree_level(), None);
        new_guard.mark_modified(txn_id, Lsn::MAX);
        let mut new_view = BTreeInternalPageViewMut::new(new_guard, &self.layout)?;

        // Snapshot children array C0..Ck
        let orig_slot_count = orig_view.slot_count();
        let mut children = Vec::with_capacity(orig_slot_count + 1);
        for i in 0..orig_slot_count {
            let child = orig_view.get_entry(i)?.child_block;
            children.push(child);
        }
        children.push(
            orig_view
                .rightmost_child_block()
                .ok_or("missing rightmost child")?,
        );

        let (left_children, right_children) = children.split_at(split_slot);

        // Collect entries to move with corresponding right_child pointers
        let mut moved = Vec::new();
        for rel_idx in split_slot..orig_slot_count {
            let entry = orig_view.get_entry(rel_idx)?;
            let rc = right_children
                .get(rel_idx - split_slot + 1)
                .copied()
                .ok_or("right child missing for moved entry")?;
            moved.push((entry.key.clone(), rc));
        }

        // Delete moved entries from original page
        for _ in split_slot..orig_slot_count {
            orig_view.delete_entry(split_slot)?;
        }

        // Set left page rightmost child
        if let Some(&last_left) = left_children.last() {
            orig_view.set_rightmost_child_block(last_left)?;
        }

        // Set right page rightmost and insert moved entries
        if let Some(&last_right) = right_children.last() {
            new_view.set_rightmost_child_block(last_right)?;
        }
        for (k, right_child) in moved.into_iter() {
            new_view.insert_entry(k, right_child)?;
        }

        // Set high keys: left gets separator (first key of right), right = +∞ sentinel
        let sep_key = new_view.get_entry(0)?.key.clone();
        let sep_bytes: Vec<u8> = sep_key.try_into()?;
        orig_view.set_high_key(&sep_bytes)?;
        new_view.clear_high_key()?;

        Ok(new_block_id)
    }

    /// This method will search for a given key in the [BTreeInternal] node
    /// It will loop until it finds the terminal internal node and then return the block
    /// number of the leaf node that contains the key
    fn search(&mut self, search_key: &Constant) -> Result<usize, Box<dyn Error>> {
        let mut child_block = self.find_child_block(search_key)?;
        let mut guard = self.txn.pin_read_guard(&self.block_id);
        let mut view = guard.into_btree_internal_page_view(&self.layout)?;
        while view.btree_level() != 0 {
            child_block = self.find_child_block(search_key)?;
            guard = self.txn.pin_read_guard(&child_block);
            view = guard.into_btree_internal_page_view(&self.layout)?;
        }
        Ok(child_block.block_num)
    }

    /// Create a new root above current root after a split.
    fn make_new_root(&self, split: SplitResult) -> Result<(), Box<dyn Error>> {
        // read current level
        let level = {
            let guard = self.txn.pin_read_guard(&self.block_id);
            let view = guard.into_btree_internal_page_view(&self.layout)?;
            view.btree_level()
        };

        // Reformat current page as empty internal at level+1, pointing rightmost to LEFT child for upcoming insert.
        let mut guard = self.txn.pin_write_guard(&self.block_id);
        guard.format_as_btree_internal(level + 1, Some(split.left_block));
        {
            let mut view = BTreeInternalPageViewMut::new(guard, &self.layout)?;
            // insert separator with right child = split.right_block
            view.insert_entry(split.sep_key, split.right_block)?;
        }
        Ok(())
    }

    /// Insert a separator into this internal node, returning an optional split to bubble up.
    /// This is the public entry point used by callers and tests; it delegates to the
    /// new split-aware flow.
    fn insert_entry(
        &self,
        entry: BTreeInternalEntry,
    ) -> Result<Option<SplitResult>, Box<dyn Error>> {
        let guard = self.txn.pin_read_guard(&self.block_id);
        let view = BTreeInternalPageView::new(guard, &self.layout)?;
        if view.btree_level() == 0 {
            drop(view);
            return self.insert_internal_node_entry(entry);
        }

        let child_block = self.find_child_block(&entry.key)?;
        let child_internal_node = BTreeInternal::new(
            Arc::clone(&self.txn),
            child_block,
            self.layout.clone(),
            self.file_name.clone(),
        );
        let new_entry = child_internal_node.insert_entry(entry)?;
        match new_entry {
            Some(split) => self.insert_internal_node_entry(BTreeInternalEntry {
                key: split.sep_key,
                child_block: split.right_block,
            }),
            None => Ok(None),
        }
    }

    /// This method will insert a new entry into the [BTreeInternal] node
    /// It will find the appropriate slot for the new entry
    /// If the page is full, it will split the page and return the new entry
    fn insert_internal_node_entry(
        &self,
        entry: BTreeInternalEntry,
    ) -> Result<Option<SplitResult>, Box<dyn Error>> {
        let split_point_opt = {
            let guard = self.txn.pin_write_guard(&self.block_id);
            let mut view = BTreeInternalPageViewMut::new(guard, &self.layout)?;
            view.insert_entry(entry.key, entry.child_block)?;
            if view.is_full() {
                Some(view.slot_count() / 2)
            } else {
                None
            }
        };
        let Some(split_point) = split_point_opt else {
            return Ok(None);
        };

        let new_block_id = self.split_page(split_point)?;

        let guard = self.txn.pin_read_guard(&new_block_id);
        let right_view = guard.into_btree_internal_page_view(&self.layout)?;
        let sep_key = right_view.get_entry(0)?.key.clone();

        Ok(Some(SplitResult {
            sep_key,
            left_block: self.block_id.block_num,
            right_block: new_block_id.block_num,
        }))
    }

    /// This method will find the child block for a given search key in a [BTreeInternal] node
    /// It uses textbook separator search: first key > search_key => take that entry's child; otherwise take header.rightmost_child.
    fn find_child_block(&self, search_key: &Constant) -> Result<BlockId, Box<dyn Error>> {
        let guard = self.txn.pin_read_guard(&self.block_id);
        let view = BTreeInternalPageView::new(guard, &self.layout)?;
        let mut left = 0;
        let mut right = view.slot_count();
        while left < right {
            let mid = (left + right) / 2;
            let key_mid = view.get_entry(mid)?.key;
            if key_mid > *search_key {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        if left < view.slot_count() {
            let block_num = view.get_entry(left)?.child_block;
            Ok(BlockId::new(self.file_name.clone(), block_num))
        } else {
            let block_num = view
                .rightmost_child_block()
                .ok_or("missing rightmost child")?;
            Ok(BlockId::new(self.file_name.clone(), block_num))
        }
    }
}

#[cfg(test)]
mod btree_internal_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    fn setup_internal_node(db: &SimpleDB) -> (Arc<Transaction>, BTreeInternal) {
        let tx = db.new_tx();
        let filename = generate_filename();
        let block = tx.append(&filename);
        let dummy_child = tx.append(&filename);
        let layout = create_test_layout();

        // Format the page as internal node
        let mut guard = tx.pin_write_guard(&block);
        guard.format_as_btree_internal(0, Some(dummy_child.block_num));
        guard.mark_modified(tx.id(), Lsn::MAX);
        drop(guard);

        let internal = BTreeInternal::new(Arc::clone(&tx), block, layout, filename);
        (tx, internal)
    }

    #[test]
    fn test_search_simple_path() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (txn, internal) = setup_internal_node(&db);

        // Insert some entries to create a simple path
        {
            let guard = txn.pin_write_guard(&internal.block_id);
            let mut view = BTreeInternalPageViewMut::new(guard, &internal.layout).unwrap();
            view.insert_entry(Constant::Int(10), 2).unwrap();
            view.insert_entry(Constant::Int(20), 3).unwrap();
            view.insert_entry(Constant::Int(30), 4).unwrap();
        }

        // Search for a value - should return correct child block
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.block_num, 2); // Should return block 2 since 15 < 20

        let result = internal.find_child_block(&Constant::Int(25)).unwrap();
        assert_eq!(result.block_num, 3); // Should return block 3 since 20 < 25 < 30
    }

    #[test]
    fn test_insert_with_split() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (_, internal) = setup_internal_node(&db);

        let mut block_num = 0;
        let mut split_entry = None;

        while split_entry.is_none() {
            let entry = BTreeInternalEntry {
                key: Constant::Int(block_num),
                child_block: block_num as usize,
            };
            let res = internal.insert_entry(entry).unwrap();
            if res.is_some() {
                split_entry = res;
            }
            block_num += 1;
        }
        let split_entry = split_entry.unwrap();
        let mid_val = block_num / 2;
        assert_eq!(split_entry.sep_key, Constant::Int(mid_val));
    }

    #[test]
    fn test_make_new_root() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (txn, internal) = setup_internal_node(&db);

        // Setup initial entries
        {
            let guard = txn.pin_write_guard(&internal.block_id);
            let mut view = BTreeInternalPageViewMut::new(guard, &internal.layout).unwrap();
            view.insert_entry(Constant::Int(10), 2).unwrap();
            view.insert_entry(Constant::Int(20), 3).unwrap();
        }

        // Create a new entry that will be part of new root
        let split = SplitResult {
            sep_key: Constant::Int(30),
            left_block: internal.block_id.block_num,
            right_block: 4,
        };

        // Make new root
        internal.make_new_root(split).unwrap();

        // Verify root structure
        let guard = txn.pin_read_guard(&internal.block_id);
        let view = BTreeInternalPageView::new(guard, &internal.layout).unwrap();
        assert!(matches!(view.btree_level(), 1));
        assert_eq!(view.slot_count(), 1);

        // Separator points to left child
        assert_eq!(
            view.get_entry(0).unwrap().child_block,
            internal.block_id.block_num
        );
        // Rightmost points to right child
        assert_eq!(view.rightmost_child_block().unwrap(), 4);
    }

    #[test]
    fn test_insert_recursive_split() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (_, mut internal) = setup_internal_node(&db);

        let mut block_num = 0;
        let mut split_entry = None;
        while split_entry.is_none() {
            let entry = BTreeInternalEntry {
                key: Constant::Int(block_num),
                child_block: block_num as usize,
            };
            let res = internal.insert_entry(entry).unwrap();
            if res.is_some() {
                split_entry = res;
            }
            block_num += 1;
        }
        assert!(split_entry.is_some());
        let leaf_block_num = internal.search(&Constant::Int(3)).unwrap();
        assert!(leaf_block_num > 0);
    }

    #[test]
    fn test_edge_cases() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (txn, internal) = setup_internal_node(&db);

        // Test inserting duplicate keys
        {
            let guard = txn.pin_write_guard(&internal.block_id);
            let mut view = BTreeInternalPageViewMut::new(guard, &internal.layout).unwrap();
            view.insert_entry(Constant::Int(10), 1).unwrap();
            view.insert_entry(Constant::Int(10), 2).unwrap();
        }

        //  With rightmost insertion, duplicates are inserted after existing entries
        //  === BTreePage Debug ===
        //  Block: BlockId { filename: "test_file_...", block_num: 0 }
        //  Page Type: Internal(None)
        //  Record Count: 2
        //  Entries:
        //  Slot 0: Key=Int(10), Child Block=1  (inserted first)
        //  Slot 1: Key=Int(10), Child Block=2  (inserted second, rightmost)
        //  ====================
        // Search should return the rightmost child for duplicate key
        let result = internal.find_child_block(&Constant::Int(10)).unwrap();
        assert_eq!(result.block_num, 2);

        // Test searching for key less than all entries
        let result = internal.find_child_block(&Constant::Int(5)).unwrap();
        assert_eq!(result.block_num, 1); // First entry

        // Test searching for key greater than all entries
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.block_num, 2); // Rightmost entry
    }

    #[test]
    fn test_internal_split_preserves_child_invariants() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (txn, internal) = setup_internal_node(&db);

        // Insert entries until split occurs
        // Use predictable pattern: key=i*10, child=i*100
        let mut entries_inserted = 0;
        let split = loop {
            let key = Constant::Int(entries_inserted * 10);
            let child = (entries_inserted * 100) as usize;
            let entry = BTreeInternalEntry {
                key: key.clone(),
                child_block: child,
            };

            let split_result = internal.insert_entry(entry).unwrap();
            entries_inserted += 1;

            if let Some(s) = split_result {
                break s;
            }

            assert!(entries_inserted < 1000, "should split before 1000 entries");
        };

        // Verify the split result structure
        assert_eq!(split.left_block, internal.block_id.block_num);
        assert!(split.right_block > split.left_block);

        // Read left page and verify child invariants
        let left_guard = txn.pin_read_guard(&internal.block_id);
        let left_view = BTreeInternalPageView::new(left_guard, &internal.layout).unwrap();

        // Verify left page structure
        assert!(left_view.slot_count() > 0, "left page should have entries");
        let left_count = left_view.slot_count();

        // Check each entry in left page
        for i in 0..left_count {
            let entry = left_view.get_entry(i).unwrap();
            // Verify entry exists
            assert!(
                matches!(entry.key, Constant::Int(_)),
                "left entry {} should have int key",
                i
            );
        }

        // Verify left has rightmost child
        assert!(
            left_view.rightmost_child_block().is_some(),
            "left page must have rightmost child"
        );

        // Read right page and verify child invariants
        let right_block = BlockId::new(internal.file_name.clone(), split.right_block);
        let right_guard = txn.pin_read_guard(&right_block);
        let right_view = BTreeInternalPageView::new(right_guard, &internal.layout).unwrap();

        // Verify right page structure
        assert!(
            right_view.slot_count() > 0,
            "right page should have entries"
        );
        let right_count = right_view.slot_count();

        // Verify first key of right page equals separator
        let right_first = right_view.get_entry(0).unwrap();
        assert_eq!(
            right_first.key, split.sep_key,
            "right page first key should equal separator"
        );

        // Check each entry in right page
        for i in 0..right_count {
            let entry = right_view.get_entry(i).unwrap();
            assert!(
                matches!(entry.key, Constant::Int(_)),
                "right entry {} should have int key",
                i
            );
        }

        // Verify right has rightmost child
        assert!(
            right_view.rightmost_child_block().is_some(),
            "right page must have rightmost child"
        );

        // Verify pages have reasonable distribution
        assert!(
            left_count > 0 && right_count > 0,
            "both pages should have entries after split"
        );

        // Verify total entry count is preserved (all entries plus separator should equal original)
        // Note: The separator IS the first key of right page, not a separate entry
        let total_entries_after = left_count + right_count;
        assert_eq!(
            total_entries_after, entries_inserted as usize,
            "total entry count should be preserved (separator is first key of right page)"
        );

        // Verify separator is a valid key from the original set
        if let Constant::Int(sep_val) = split.sep_key {
            assert_eq!(
                sep_val % 10,
                0,
                "separator should be a multiple of 10 from our test data"
            );
        } else {
            panic!("separator should be an int");
        }
    }
}

/// The [BTreeLeaf] struct. This is the page that contains all the actual pointers to [RID] in the heap tables
/// It can have an overflow pointer to an overflow page, but overflow pages are special in that they have entries with the same value for the dataval field
/// A [BTreeLeaf] that has an overflow page must have its first entry have the same dataval as all entries in the overflow block
/// Main Page:          Overflow Block:
/// [K5, K6]  ------->  [K5, K5, K5, K5]
struct BTreeLeaf {
    txn: Arc<Transaction>,
    layout: Layout,
    search_key: Constant,
    current_block_id: BlockId,
    current_slot: Option<usize>,
    file_name: String,
}

impl std::fmt::Display for BTreeLeaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreeLeaf Debug ===")?;
        writeln!(f, "Search Key: {:?}", self.search_key)?;
        writeln!(f, "Current Slot: {:?}", self.current_slot)?;
        writeln!(f, "File Name: {}", self.file_name)?;
        writeln!(f, "Current Block: {:?}", self.current_block_id)?;
        Ok(())
    }
}

impl BTreeLeaf {
    /// Helper method to split a leaf page by moving entries from split_slot onwards to a new page
    /// Returns the BlockId of the newly created page
    fn split_page(
        &self,
        split_slot: usize,
        overflow_block: Option<usize>,
    ) -> Result<BlockId, Box<dyn Error>> {
        let txn_id = self.txn.id();
        let orig_guard = self.txn.pin_write_guard(&self.current_block_id);
        let mut orig_view = orig_guard.into_btree_leaf_page_view_mut(&self.layout)?;

        let new_block_id = self.txn.append(&self.file_name);
        let mut new_guard = self.txn.pin_write_guard(&new_block_id);
        new_guard.format_as_btree_leaf(overflow_block);
        new_guard.mark_modified(txn_id, Lsn::MAX);
        let mut new_view = new_guard.into_btree_leaf_page_view_mut(&self.layout)?;

        // Preserve old right sibling so we can re-chain after the split.
        let old_right = orig_view.right_sibling_block();

        // Move entries split_slot..end to new page
        while split_slot < orig_view.slot_count() {
            let entry = orig_view.get_entry(split_slot)?.clone();
            new_view.insert_entry(entry.key.clone(), entry.rid)?;
            orig_view.delete_entry(split_slot)?;
        }

        // Set high keys and right sibling
        let sep_key = new_view.get_entry(0)?.key.clone();
        let sep_bytes: Vec<u8> = sep_key.try_into()?;
        // Left high key = separator, right link = new page (reuse existing guard)
        orig_view.set_high_key(&sep_bytes)?;
        orig_view.set_right_sibling_block(Some(new_block_id.block_num))?;
        new_view.set_right_sibling_block(old_right)?;
        new_view.clear_high_key()?;

        Ok(new_block_id)
    }

    /// Creates a new [BTreeLeaf] with the given transaction, block ID, layout, search key and filename
    /// The page is initialized with an appropriate slot based on the search key position
    fn new(
        txn: Arc<Transaction>,
        block_id: BlockId,
        layout: Layout,
        search_key: Constant,
        file_name: String,
    ) -> Result<Self, Box<dyn Error>> {
        // Calculate initial slot using a temporary guard
        let current_slot = {
            let guard = txn.pin_read_guard(&block_id);
            let view = guard.into_btree_leaf_page_view(&layout)?;
            view.find_slot_before(&search_key)
        };

        let mut leaf = Self {
            txn,
            layout,
            search_key,
            current_block_id: block_id,
            current_slot,
            file_name,
        };
        leaf.hop_right_if_needed()?;
        Ok(leaf)
    }

    /// Follow right siblings while search_key is >= this page's high key.
    fn hop_right_if_needed(&mut self) -> Result<(), Box<dyn Error>> {
        loop {
            let guard = self.txn.pin_read_guard(&self.current_block_id);
            let view = guard.into_btree_leaf_page_view(&self.layout)?;
            let Some(hk) = view.high_key() else {
                break;
            };
            if self.search_key < hk {
                break;
            }
            let Some(rsib) = view.right_sibling_block() else {
                break;
            };
            // move to sibling and recompute slot
            self.current_block_id = BlockId::new(self.file_name.clone(), rsib);
            let guard = self.txn.pin_read_guard(&self.current_block_id);
            let view = guard.into_btree_leaf_page_view(&self.layout)?;
            self.current_slot = view.find_slot_before(&self.search_key);
            continue;
        }
        Ok(())
    }

    /// Advances to the next record that matches the search key
    /// If we've reached the end of the current page, attempts to follow the overflow chain
    /// Returns Some(()) if a matching record is found, None otherwise
    fn next(&mut self) -> Result<Option<()>, Box<dyn Error>> {
        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };

        let (at_end, key_matches) = {
            let guard = self.txn.pin_read_guard(&self.current_block_id);
            let view = guard.into_btree_leaf_page_view(&self.layout)?;

            // Skip over any dead slots to find next live entry
            while self.current_slot.unwrap() < view.slot_count() {
                if view.is_slot_live(self.current_slot.unwrap()) {
                    break;
                }
                self.current_slot = Some(self.current_slot.unwrap() + 1);
            }

            let at_end = self.current_slot.unwrap() >= view.slot_count();
            let key_matches = if !at_end {
                view.get_entry(self.current_slot.unwrap())?.key == self.search_key
            } else {
                false
            };
            (at_end, key_matches)
        };

        if at_end {
            self.try_overflow()
        } else if key_matches {
            Ok(Some(()))
        } else {
            self.try_overflow()
        }
    }

    /// Deletes the record with the specified RID from this leaf page or its overflow chain
    /// Returns Ok(()) if the record was found and deleted, error otherwise
    /// Requires that current_slot is initialized
    fn delete(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        while (self.next()?).is_some() {
            let guard = self.txn.pin_write_guard(&self.current_block_id);
            let mut view = guard.into_btree_leaf_page_view_mut(&self.layout)?;
            let slot = self.current_slot.unwrap();

            if view.get_entry(slot)?.rid == rid {
                view.delete_entry(slot)?;
                return Ok(());
            }
        }
        Err("RID not found in BTreeLeaf".into())
    }

    /// This method will attempt to insert an entry into a [BTreeLeaf] page
    /// If the leaf page has an overflow page, and the new entry is smaller than the first key, split the page
    /// If the page splits, return the [InternalNodeEntry] identifier to the new page
    fn insert(&mut self, rid: RID) -> Result<Option<SplitResult>, Box<dyn Error>> {
        //  If this page has an overflow page, and the key being inserted is less than the first key force a split
        //  This is done to ensure that overflow pages are linked to a page with the first key the same as entries in overflow pages
        debug!("Inserting rid {:?} into BTreeLeaf", rid);

        // Check for overflow + smaller key case
        {
            let guard = self.txn.pin_read_guard(&self.current_block_id);
            let view = guard.into_btree_leaf_page_view(&self.layout)?;

            if let Some(overflow_block) = view.overflow_block() {
                let first_key = view.get_entry(0)?.key;
                if first_key > self.search_key {
                    debug!("Inserting a record smaller than the first record into a page full of identical records");
                    drop(view); // Drops view and guard

                    // Split at 0, preserving current overflow
                    let new_block_id = self.split_page(0, Some(overflow_block))?;

                    // Clear overflow on current page and insert new entry
                    let guard = self.txn.pin_write_guard(&self.current_block_id);
                    let mut view = guard.into_btree_leaf_page_view_mut(&self.layout)?;
                    view.set_overflow_block(None)?;
                    view.insert_entry(self.search_key.clone(), rid)?;

                    self.current_slot = Some(0);

                    return Ok(Some(SplitResult {
                        sep_key: first_key,
                        left_block: self.current_block_id.block_num,
                        right_block: new_block_id.block_num,
                    }));
                }
            }
        }

        // Normal insert
        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };

        {
            let guard = self.txn.pin_write_guard(&self.current_block_id);
            let mut view = guard.into_btree_leaf_page_view_mut(&self.layout)?;
            view.insert_entry(self.search_key.clone(), rid)?;

            if !view.is_full() {
                debug!("Done inserting rid {:?} into BTreeLeaf", rid);
                return Ok(None);
            }
        }

        //  The leaf needs to be split. There are two cases to handle here
        //
        //  The page is full of identical keys
        //  1. Create an overflow block and move all keys except the first key there
        //  2. Link the current page to the overflow block
        //
        //  The page is not full of identical keys
        //  1. Find the split point
        //  2. Move the split point
        //
        //  Moving the split point
        //  If the split key is identical to the first key, move it right because all identical keys need to stay together
        //  If the split key is not identical to the first key, move it left until the the first instance of the split key is found
        debug!("Splitting BTreeLeaf");

        let guard = self.txn.pin_read_guard(&self.current_block_id);
        let view = guard.into_btree_leaf_page_view(&self.layout)?;

        let first_key = view.get_entry(0)?.key;
        let last_key = view.get_entry(view.slot_count() - 1)?.key;

        if first_key == last_key {
            debug!("The first key and last key are identical, so moving everything except first record into overflow page");
            drop(view); // Drops view and guard

            let new_block_id = self.split_page(1, None)?;

            // Set overflow on current page
            let guard = self.txn.pin_write_guard(&self.current_block_id);
            let mut view = guard.into_btree_leaf_page_view_mut(&self.layout)?;
            view.set_overflow_block(Some(new_block_id.block_num))?;

            debug!("Done splitting BTreeLeaf");
            return Ok(None);
        }

        debug!("Finding the split point");
        let mut split_point = view.slot_count() / 2;
        debug!("The split point {}", split_point);
        let mut split_record = view.get_entry(split_point)?.key;

        if split_record == first_key {
            debug!("Moving split point to the right");
            while view.get_entry(split_point)?.key == first_key {
                split_point += 1;
            }
            split_record = view.get_entry(split_point)?.key;
        } else {
            debug!("Moving split point to the left");
            while view.get_entry(split_point - 1)?.key == split_record {
                split_point -= 1;
            }
        }

        debug!("Splitting at {}", split_point);
        drop(view); // Drops view and guard

        let new_block_id = self.split_page(split_point, None)?;

        Ok(Some(SplitResult {
            sep_key: split_record,
            left_block: self.current_block_id.block_num,
            right_block: new_block_id.block_num,
        }))
    }

    /// This method will check to see if an overflow page is present for this block
    /// An overflow page for a specific page will contain entries that are the same as the first key of the current page
    /// If no overflow page can be found, just return. Otherwise swap out the current contents for the overflow contents
    fn try_overflow(&mut self) -> Result<Option<()>, Box<dyn Error>> {
        let guard = self.txn.pin_read_guard(&self.current_block_id);
        let view = guard.into_btree_leaf_page_view(&self.layout)?;

        // Find first live slot
        let mut first_live_slot = 0;
        while first_live_slot < view.slot_count() {
            if view.is_slot_live(first_live_slot) {
                break;
            }
            first_live_slot += 1;
        }

        if first_live_slot >= view.slot_count() {
            return Ok(None);
        }

        // Get first live entry - any error here is a real error (corruption, etc.)
        let first_key = view.get_entry(first_live_slot)?.key;

        if first_key != self.search_key {
            return Ok(None);
        }

        let Some(overflow_block_num) = view.overflow_block() else {
            return Ok(None);
        };

        // Switch to overflow page
        self.current_block_id = BlockId::new(self.file_name.clone(), overflow_block_num);
        self.current_slot = None;
        Ok(Some(()))
    }

    fn get_data_rid(&self) -> Result<RID, Box<dyn Error>> {
        let slot = self
            .current_slot
            .expect("Current slot not set in BTreeLeaf::get_data_rid");

        let guard = self.txn.pin_read_guard(&self.current_block_id);
        let view = guard.into_btree_leaf_page_view(&self.layout)?;
        let entry = view.get_entry(slot)?;
        Ok(entry.rid)
    }
}

#[cfg(test)]
mod btree_leaf_tests {
    use super::*;
    use crate::{
        test_utils::{generate_filename, generate_random_number},
        Schema, SimpleDB, TestDir,
    };

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    fn setup_leaf(db: &SimpleDB, search_key: Constant) -> (Arc<Transaction>, BTreeLeaf) {
        let txn = db.new_tx();
        let filename = generate_filename();
        let block = txn.append(&filename);
        let layout = create_test_layout();

        // Format the page as a leaf using new page format
        {
            let mut guard = txn.pin_write_guard(&block);
            guard.format_as_btree_leaf(None);
            guard.mark_modified(txn.id(), Lsn::MAX);
        }

        let leaf = BTreeLeaf::new(Arc::clone(&txn), block, layout, search_key, filename).unwrap();

        (txn, leaf)
    }

    #[test]
    fn test_insert_no_split() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Insert should succeed without splitting
        assert!(leaf.insert(RID::new(1, 1)).unwrap().is_none());

        // Verify the record was inserted
        let guard = leaf.txn.pin_read_guard(&leaf.current_block_id);
        let view = guard.into_btree_leaf_page_view(&leaf.layout).unwrap();
        assert_eq!(view.slot_count(), 1);
        assert_eq!(view.get_entry(0).unwrap().key, Constant::Int(10));
    }

    #[test]
    fn test_insert_with_split_different_keys() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Fill the page with different keys
        let mut counter = 0;
        let mut split_result = None;
        while split_result.is_none() {
            leaf.search_key = Constant::Int(counter);
            let res = leaf.insert(RID::new(1, counter as usize)).unwrap();
            if res.is_some() {
                split_result = res;
            }
            counter += 1;
        }

        // Verify split occurred
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.right_block, 1); // new sibling block id
        assert_eq!(entry.sep_key, Constant::Int(counter / 2)); // Middle key
    }

    #[test]
    fn test_insert_with_overflow_same_keys() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10));

        let mut counter = 0;
        loop {
            leaf.insert(RID::new(1, counter)).unwrap();
            {
                let guard = leaf.txn.pin_read_guard(&leaf.current_block_id);
                let view = guard.into_btree_leaf_page_view(&leaf.layout).unwrap();
                if view.overflow_block().is_some() {
                    break;
                }
            }
            counter += 1;
        }

        //  verify both the leaf and the overflow page have the same first key
        {
            let guard = leaf.txn.pin_read_guard(&leaf.current_block_id);
            let view = guard.into_btree_leaf_page_view(&leaf.layout).unwrap();
            assert_eq!(view.get_entry(0).unwrap().key, Constant::Int(10));
        }
        assert!(leaf.try_overflow().unwrap().is_some());
        {
            let guard = leaf.txn.pin_read_guard(&leaf.current_block_id);
            let view = guard.into_btree_leaf_page_view(&leaf.layout).unwrap();
            assert_eq!(view.get_entry(0).unwrap().key, Constant::Int(10));
        }
    }

    #[test]
    fn test_insert_edge_cases() {
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);

        let mut counter = 0;
        let mut split_result = None;

        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10));

        while split_result.is_none() {
            leaf.search_key = Constant::Int(if counter % 2 == 0 { 10 } else { 20 });
            let res = leaf.insert(RID::new(1, counter)).unwrap();
            if res.is_some() {
                split_result = res;
            }
            counter += 1;
        }

        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.sep_key, Constant::Int(20));
    }

    #[test]
    fn test_btree_leaf_rollback_mixed_operations() {
        // Test rollback of B-tree leaf insert operations
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let layout = create_test_layout();
        let filename = generate_filename();

        // Transaction 1: Insert initial entries and commit
        let t1 = db.new_tx();
        let block = t1.append(&filename);
        {
            let mut guard = t1.pin_write_guard(&block);
            guard.format_as_btree_leaf(None);
            guard.mark_modified(t1.id(), crate::Lsn::MAX);
        }
        {
            let guard = t1.pin_write_guard(&block);
            let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
            view.insert_entry(Constant::Int(10), RID::new(1, 1))
                .unwrap();
            view.insert_entry(Constant::Int(20), RID::new(1, 2))
                .unwrap();
        }
        t1.commit().unwrap();

        // Transaction 2: Mixed insert/delete operations that will be rolled back
        let t2 = db.new_tx();
        let slot_to_delete = {
            let guard = t2.pin_read_guard(&block);
            let view = guard.into_btree_leaf_page_view(&layout).unwrap();
            // Find slot for key=20
            (0..view.slot_count())
                .find(|&slot| {
                    view.is_slot_live(slot)
                        && view
                            .get_entry(slot)
                            .ok()
                            .map(|e| e.key == Constant::Int(20))
                            .unwrap_or(false)
                })
                .unwrap()
        };
        {
            let guard = t2.pin_write_guard(&block);
            let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
            // Delete existing entry (should be undone)
            view.delete_entry(slot_to_delete).unwrap();
            // Insert new entries (should be undone)
            view.insert_entry(Constant::Int(15), RID::new(1, 3))
                .unwrap();
            view.insert_entry(Constant::Int(25), RID::new(1, 4))
                .unwrap();
        }
        // Rollback - t2 operations should be undone
        t2.rollback().unwrap();

        // Transaction 3: Verify committed entries remain, rolled-back entries are gone
        let t3 = db.new_tx();
        let guard = t3.pin_read_guard(&block);
        let view = guard.into_btree_leaf_page_view(&layout).unwrap();

        // Collect live entries sorted by key
        let mut live_keys: Vec<_> = (0..view.slot_count())
            .filter(|&slot| view.is_slot_live(slot))
            .filter_map(|slot| view.get_entry(slot).ok())
            .map(|e| e.key)
            .collect();
        live_keys.sort();

        // Should only have original committed entries
        assert_eq!(live_keys, vec![Constant::Int(10), Constant::Int(20)]);
    }

    #[test]
    fn test_btree_leaf_recovery_mixed_operations() {
        // Test recovery undoes uncommitted B-tree operations after crash
        let dir = TestDir::new(format!("/tmp/recovery_test/{}", generate_random_number()));
        let layout = create_test_layout();
        let filename = generate_filename();

        // Simulate crash by ending process scope without commit/rollback, then reopening DB.
        let block = {
            let db = SimpleDB::new(&dir, 8, true, 5000);

            // Transaction 1: Insert initial entries and commit
            let t1 = db.new_tx();
            let block = t1.append(&filename);
            {
                let mut guard = t1.pin_write_guard(&block);
                guard.format_as_btree_leaf(None);
                guard.mark_modified(t1.id(), crate::Lsn::MAX);
            }
            {
                let guard = t1.pin_write_guard(&block);
                let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
                view.insert_entry(Constant::Int(100), RID::new(1, 1))
                    .unwrap();
                view.insert_entry(Constant::Int(200), RID::new(1, 2))
                    .unwrap();
            }
            t1.commit().unwrap();

            // Transaction 2: Committed operation
            let t2 = db.new_tx();
            {
                let guard = t2.pin_write_guard(&block);
                let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
                view.insert_entry(Constant::Int(150), RID::new(1, 3))
                    .unwrap();
            }
            t2.commit().unwrap();

            // Transaction 3: Uncommitted operations (simulates crash)
            let t3 = db.new_tx();
            {
                let guard = t3.pin_write_guard(&block);
                let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
                // Insert new entries without committing
                view.insert_entry(Constant::Int(175), RID::new(1, 4))
                    .unwrap();
                view.insert_entry(Constant::Int(250), RID::new(1, 5))
                    .unwrap();
            }
            block
        };

        // Recover in a fresh DB instance (new lock table)
        let db = SimpleDB::new(&dir, 8, false, 5000);
        let recovery_tx = db.new_tx();
        recovery_tx.recover().unwrap();

        // Transaction 4: Verify only committed state remains
        let t4 = db.new_tx();
        let guard = t4.pin_read_guard(&block);
        let view = guard.into_btree_leaf_page_view(&layout).unwrap();

        // Collect live entries sorted by key
        let mut live_keys: Vec<_> = (0..view.slot_count())
            .filter(|&slot| view.is_slot_live(slot))
            .filter_map(|slot| view.get_entry(slot).ok())
            .map(|e| e.key)
            .collect();
        live_keys.sort();

        // Should have only committed entries: t1's 2 + t2's 1 (t3's undone)
        assert_eq!(
            live_keys,
            vec![Constant::Int(100), Constant::Int(150), Constant::Int(200)]
        );
    }

    #[test]
    fn test_btree_leaf_rollback_delete() {
        // Test rollback of a single delete operation
        let (db, _dir) = SimpleDB::new_for_test(8, 5000);
        let layout = create_test_layout();
        let filename = generate_filename();

        // Transaction 1: Insert entries and commit
        let t1 = db.new_tx();
        let block = t1.append(&filename);
        {
            let mut guard = t1.pin_write_guard(&block);
            guard.format_as_btree_leaf(None);
            guard.mark_modified(t1.id(), crate::Lsn::MAX);
        }
        let slot_to_delete;
        {
            let guard = t1.pin_write_guard(&block);
            let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
            view.insert_entry(Constant::Int(10), RID::new(1, 1))
                .unwrap();
            slot_to_delete = view
                .insert_entry(Constant::Int(20), RID::new(1, 2))
                .unwrap();
            view.insert_entry(Constant::Int(30), RID::new(1, 3))
                .unwrap();
        }
        t1.commit().unwrap();

        // Transaction 2: Delete middle entry and rollback
        let t2 = db.new_tx();
        println!("Deleting slot {}", slot_to_delete);
        {
            let guard = t2.pin_write_guard(&block);
            let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
            view.delete_entry(slot_to_delete).unwrap();
        }

        println!("Rolling back transaction");
        t2.rollback().unwrap();

        // Transaction 3: Verify all entries still present
        let t3 = db.new_tx();
        let guard = t3.pin_read_guard(&block);
        let view = guard.into_btree_leaf_page_view(&layout).unwrap();

        println!("\nAfter rollback:");
        println!("  Slot count: {}", view.slot_count());
        let mut live_keys: Vec<_> = (0..view.slot_count())
            .filter(|&slot| {
                let is_live = view.is_slot_live(slot);
                println!("    Slot {} live: {}", slot, is_live);
                is_live
            })
            .filter_map(|slot| {
                let entry = view.get_entry(slot).ok();
                if let Some(ref e) = entry {
                    println!("      key={:?}", e.key);
                }
                entry
            })
            .map(|e| e.key)
            .collect();
        live_keys.sort();

        println!("\nLive keys: {:?}", live_keys);

        // All three entries should still be live after rollback
        assert_eq!(
            live_keys,
            vec![Constant::Int(10), Constant::Int(20), Constant::Int(30)]
        );
    }

    #[test]
    fn test_btree_leaf_recovery_delete() {
        // Test recovery undoes uncommitted delete operation
        let dir = TestDir::new(format!("/tmp/recovery_test/{}", generate_random_number()));
        let layout = create_test_layout();
        let filename = generate_filename();

        // Transaction 1: Insert entries and commit
        let block = {
            let db = SimpleDB::new(&dir, 8, true, 5000);
            let t1 = db.new_tx();
            let block = t1.append(&filename);
            {
                let mut guard = t1.pin_write_guard(&block);
                guard.format_as_btree_leaf(None);
            }
            let slot_to_delete = {
                let guard = t1.pin_write_guard(&block);
                let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
                view.insert_entry(Constant::Int(100), RID::new(1, 1))
                    .unwrap();
                let slot_to_delete = view
                    .insert_entry(Constant::Int(200), RID::new(1, 2))
                    .unwrap();
                view.insert_entry(Constant::Int(300), RID::new(1, 3))
                    .unwrap();
                slot_to_delete
            };
            t1.commit().unwrap();

            let t2 = db.new_tx();
            {
                let guard = t2.pin_write_guard(&block);
                let mut view = guard.into_btree_leaf_page_view_mut(&layout).unwrap();
                view.delete_entry(slot_to_delete).unwrap();
            }
            // No commit - simulate crash
            block
        };

        // Recover - should undo the delete (new DB instance resets lock table)
        let db = SimpleDB::new(&dir, 8, false, 5000);
        let recovery_tx = db.new_tx();
        recovery_tx.recover().unwrap();

        // Transaction 3: Verify all entries still present
        let t3 = db.new_tx();
        let guard = t3.pin_read_guard(&block);
        let view = guard.into_btree_leaf_page_view(&layout).unwrap();

        let mut live_keys: Vec<_> = (0..view.slot_count())
            .filter(|&slot| view.is_slot_live(slot))
            .filter_map(|slot| view.get_entry(slot).ok())
            .map(|e| e.key)
            .collect();
        live_keys.sort();

        // All three entries should be restored after recovery
        assert_eq!(
            live_keys,
            vec![Constant::Int(100), Constant::Int(200), Constant::Int(300)]
        );
    }
}
