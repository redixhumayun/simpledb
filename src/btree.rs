use std::{error::Error, sync::Arc};

use crate::{
    debug, BlockId, BufferHandle, Constant, FieldType, Index, IndexInfo, Layout, Schema,
    Transaction, RID,
};

pub struct BTreeIndex {
    txn: Arc<Transaction>,
    index_name: String,
    internal_layout: Layout,
    leaf_layout: Layout,
    leaf_table_name: String,
    leaf: Option<BTreeLeaf>,
    root_block: BlockId,
}

impl BTreeIndex {
    pub fn new(
        txn: Arc<Transaction>,
        index_name: &str,
        leaf_layout: Layout,
    ) -> Result<Self, Box<dyn Error>> {
        //  Create the leaf file with the schema provided if it does not exist
        let leaf_table_name = format!("{index_name}leaf");
        if txn.size(&leaf_table_name) == 0 {
            let block_id = txn.append(&leaf_table_name);
            let btree_page = BTreePage::new(Arc::clone(&txn), block_id, leaf_layout.clone());
            btree_page.format(PageType::Leaf(None))?;
        }

        //  Create the internal file with the schema required if it does not exist
        let internal_table_name = format!("{index_name}internal");
        let mut internal_schema = Schema::new();
        internal_schema.add_from_schema(IndexInfo::BLOCK_NUM_FIELD, &leaf_layout.schema)?;
        internal_schema.add_from_schema(IndexInfo::DATA_FIELD, &leaf_layout.schema)?;
        let internal_layout = Layout::new(internal_schema.clone());
        if txn.size(&internal_table_name) == 0 {
            let block_id = txn.append(&internal_table_name);
            let internal_page = BTreePage::new(Arc::clone(&txn), block_id, internal_layout.clone());
            internal_page.format(PageType::Internal(None))?;
            //  insert initial entry
            let field_type = internal_schema
                .info
                .get(IndexInfo::DATA_FIELD)
                .unwrap()
                .field_type;
            let min_val = match field_type {
                FieldType::Int => Constant::Int(i32::MIN),
                FieldType::String => Constant::String("".to_string()),
            };
            internal_page.insert_internal(0, min_val, 0)?;
        }
        Ok(Self {
            txn,
            index_name: index_name.to_string(),
            internal_layout,
            leaf_layout,
            leaf_table_name,
            leaf: None,
            root_block: BlockId::new(internal_table_name, 0),
        })
    }

    fn search_cost(num_of_blocks: usize, records_per_block: usize) -> usize {
        (1 + num_of_blocks.ilog(records_per_block))
            .try_into()
            .unwrap()
    }
}

impl Index for BTreeIndex {
    fn before_first(&mut self, search_key: &Constant) {
        let mut root = BTreeInternal::new(
            Arc::clone(&self.txn),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.root_block.filename.clone(),
        );
        let leaf_block_num = root.search(search_key).unwrap();
        let leaf_block_id = BlockId::new(self.leaf_table_name.clone(), leaf_block_num);
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
        let int_node_id = self.leaf.as_mut().unwrap().insert(*data_rid).unwrap();
        if int_node_id.is_none() {
            return;
        }
        debug!("Insert in index caused a split");
        let int_node_id = int_node_id.unwrap();
        let root = BTreeInternal::new(
            Arc::clone(&self.txn),
            self.root_block.clone(),
            self.internal_layout.clone(),
            self.root_block.filename.clone(),
        );
        let root_split_entry = root.insert_entry(int_node_id).unwrap();
        if root_split_entry.is_none() {
            return;
        }
        debug!("Insert in index caused a root split");
        let root_split_entry = root_split_entry.unwrap();
        root.make_new_root(root_split_entry).unwrap();
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
        let tx = Arc::new(db.new_tx());
        let layout = create_test_layout();
        let index_name = generate_filename();
        BTreeIndex::new(Arc::clone(&tx), &index_name, layout).unwrap()
    }

    #[test]
    fn test_btree_index_construction() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let index = setup_index(&db);

        // Verify internal node file exists with minimum value entry
        let root = BTreeInternal::new(
            Arc::clone(&index.txn),
            index.root_block.clone(),
            index.internal_layout.clone(),
            index.root_block.filename.clone(),
        );
        assert_eq!(root.contents.get_number_of_recs().unwrap(), 1);
        assert_eq!(
            root.contents.get_data_value(0).unwrap(),
            Constant::Int(i32::MIN)
        );
    }

    #[test]
    fn test_simple_insert_and_search() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
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
    fn test_duplicate_keys() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let mut index = setup_index(&db);

        // Insert duplicate keys
        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.insert(&Constant::Int(10), &RID::new(1, 2));
        index.insert(&Constant::Int(10), &RID::new(1, 3));

        // Search and verify all duplicates are found
        index.before_first(&Constant::Int(10));

        let mut found_rids = Vec::new();
        while index.next() {
            found_rids.push(index.get_data_rid());
        }

        assert_eq!(found_rids.len(), 3);
        assert!(found_rids.contains(&RID::new(1, 1)));
        assert!(found_rids.contains(&RID::new(1, 2)));
        assert!(found_rids.contains(&RID::new(1, 3)));
    }

    #[test]
    fn test_delete() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let mut index = setup_index(&db);

        // Insert and then delete a value
        index.insert(&Constant::Int(10), &RID::new(1, 1));
        index.delete(&Constant::Int(10), &RID::new(1, 1));

        // Verify value is gone
        index.before_first(&Constant::Int(10));
        assert!(!index.next());

        // Insert multiple values and delete one
        index.insert(&Constant::Int(20), &RID::new(1, 1));
        index.insert(&Constant::Int(20), &RID::new(1, 2));
        index.delete(&Constant::Int(20), &RID::new(1, 1));

        // Verify only one remains
        index.before_first(&Constant::Int(20));
        assert!(index.next());
        assert_eq!(index.get_data_rid(), RID::new(1, 2));
        assert!(!index.next());
    }

    #[test]
    fn test_btree_split() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let mut index = setup_index(&db);

        // Insert enough values to force splits
        for i in 0..24 {
            index.insert(&Constant::Int(i), &RID::new(1, i as usize));
        }

        // Verify we can still find values after splits
        for i in 0..24 {
            index.before_first(&Constant::Int(i));
            assert!(index.next());
            assert_eq!(index.get_data_rid(), RID::new(1, i as usize));
        }
    }
}

struct BTreeInternal {
    txn: Arc<Transaction>,
    block_id: BlockId,
    layout: Layout,
    contents: BTreePage,
    file_name: String,
}

impl std::fmt::Display for BTreeInternal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreeInternal Debug ===")?;
        writeln!(f, "Block ID: {:?}", self.block_id)?;
        writeln!(f, "File Name: {}", self.file_name)?;
        writeln!(f, "\nContents:")?;
        write!(f, "{}", self.contents)?;
        Ok(())
    }
}

impl BTreeInternal {
    fn new(txn: Arc<Transaction>, block_id: BlockId, layout: Layout, file_name: String) -> Self {
        let contents = BTreePage::new(Arc::clone(&txn), block_id.clone(), layout.clone());
        Self {
            txn,
            block_id,
            layout,
            contents,
            file_name,
        }
    }

    /// This method will search for a given key in the [BTreeInternal] node
    /// It will find the child block that contains the key
    /// It will return the block ID of the child block
    fn search(&mut self, search_key: &Constant) -> Result<usize, Box<dyn Error>> {
        let mut child_block = self.find_child_block(search_key)?;
        while !matches!(self.contents.get_flag()?, PageType::Internal(None)) {
            self.contents = BTreePage::new(
                Arc::clone(&self.txn),
                child_block.clone(),
                self.layout.clone(),
            );
            child_block = self.find_child_block(search_key)?;
        }
        Ok(child_block.block_num)
    }

    /// This method will create a new root for the BTree
    /// It will take the entry that needs to be inserted after the split, move its existing
    /// entries into a new block and then insert both the newly created block with its old entries and the new block
    /// This is done so that the root is always at block 0 of the file
    fn make_new_root(&self, entry: InternalNodeEntry) -> Result<(), Box<dyn Error>> {
        let first_value = self.contents.get_data_value(0)?;
        let page_type = self.contents.get_flag()?;
        let level = match page_type {
            PageType::Internal(None) => 0,
            PageType::Internal(Some(n)) => n,
            _ => panic!("Invalid page type for new root"),
        };
        let new_block_id = self.contents.split(0, page_type)?;
        let new_block_entry = InternalNodeEntry {
            dataval: first_value,
            block_num: new_block_id.block_num,
        };
        self.insert_entry(new_block_entry)?;
        self.insert_entry(entry)?;
        self.contents
            .set_flag(PageType::Internal(Some(level + 1)))?;
        Ok(())
    }

    /// This method will insert a new entry into the [BTreeInternal] node
    /// It works in conjunction with [BTreeInternal::insert_internal_node_entry] to do the insertion
    /// This method will find the correct child block to insert it into and the [BTreeInternal::insert_internal_node_entry] will do the actual
    /// insertion into the specific block
    fn insert_entry(
        &self,
        entry: InternalNodeEntry,
    ) -> Result<Option<InternalNodeEntry>, Box<dyn Error>> {
        if matches!(self.contents.get_flag()?, PageType::Internal(None)) {
            return self.insert_internal_node_entry(entry);
        }
        let child_block = self.find_child_block(&entry.dataval)?;
        let child_internal_node = BTreeInternal::new(
            Arc::clone(&self.txn),
            child_block,
            self.layout.clone(),
            self.file_name.clone(),
        );
        let new_entry = child_internal_node.insert_entry(entry)?;
        match new_entry {
            Some(entry) => self.insert_internal_node_entry(entry),
            None => Ok(None),
        }
    }

    /// This method will insert a new entry into the [BTreeInternal] node
    /// It will find the appropriate slot for the new entry
    /// If the page is full, it will split the page and return the new entry
    fn insert_internal_node_entry(
        &self,
        entry: InternalNodeEntry,
    ) -> Result<Option<InternalNodeEntry>, Box<dyn Error>> {
        let slot = match self.contents.find_slot_before(&entry.dataval)? {
            Some(slot) => slot + 1, //  move to the insertion point
            None => 0,              //  the insertion point is at the first slot
        };
        self.contents
            .insert_internal(slot, entry.dataval, entry.block_num)?;

        if !self.contents.is_full()? {
            return Ok(None);
        }

        let page_type = self.contents.get_flag()?;
        let split_point = self.contents.get_number_of_recs()? / 2;
        let split_record = self.contents.get_data_value(split_point)?;
        let new_block_id = self.contents.split(split_point, page_type)?;
        Ok(Some(InternalNodeEntry {
            dataval: split_record,
            block_num: new_block_id.block_num,
        }))
    }

    /// This method will find the child block for a given search key in a [BTreeInternal] node
    /// It will search for the rightmost slot before the search key
    /// If the search key is found in the slot, it will return the next slot
    fn find_child_block(&self, search_key: &Constant) -> Result<BlockId, Box<dyn Error>> {
        let mut slot = (self.contents.find_slot_before(search_key)?).unwrap_or(0);
        if self.contents.get_data_value(slot + 1)? == *search_key {
            slot += 1;
        }
        let block_num = self.contents.get_child_block_num(slot)?;
        Ok(BlockId::new(self.file_name.clone(), block_num))
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
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();
        let filename = generate_filename();

        // Format the page as internal node
        let page = BTreePage::new(Arc::clone(&tx), block.clone(), layout.clone());
        page.format(PageType::Internal(None)).unwrap();

        let internal = BTreeInternal::new(Arc::clone(&tx), block, layout, filename);
        (tx, internal)
    }

    #[test]
    fn test_search_simple_path() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (_, internal) = setup_internal_node(&db);

        // Insert some entries to create a simple path
        internal
            .contents
            .insert_internal(0, Constant::Int(10), 2)
            .unwrap();
        internal
            .contents
            .insert_internal(1, Constant::Int(20), 3)
            .unwrap();
        internal
            .contents
            .insert_internal(2, Constant::Int(30), 4)
            .unwrap();

        // Search for a value - should return correct child block
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.block_num, 2); // Should return block 2 since 15 < 20

        let result = internal.find_child_block(&Constant::Int(25)).unwrap();
        assert_eq!(result.block_num, 3); // Should return block 3 since 20 < 25 < 30
    }

    #[test]
    fn test_insert_with_split() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (_, internal) = setup_internal_node(&db);

        // Fill the node until just before splitting
        let mut block_num = 0;
        while !internal.contents.is_one_off_full().unwrap() {
            let entry = InternalNodeEntry {
                dataval: Constant::Int(block_num),
                block_num: block_num as usize,
            };
            internal.insert_entry(entry).unwrap();
            block_num += 1;
        }

        // Insert one more entry to force split
        let entry = InternalNodeEntry {
            dataval: Constant::Int(block_num),
            block_num: block_num as usize,
        };

        let split_result = internal.insert_entry(entry).unwrap();
        assert!(split_result.is_some());

        let split_entry = split_result.unwrap();
        assert!(split_entry.block_num > 0); // Should be a new block number

        // Verify middle key was chosen for split
        let mid_val = (block_num + 1) / 2;
        assert_eq!(split_entry.dataval, Constant::Int(mid_val));
    }

    #[test]
    fn test_make_new_root() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (_, internal) = setup_internal_node(&db);

        // Setup initial entries
        internal
            .contents
            .insert_internal(0, Constant::Int(10), 2)
            .unwrap();
        internal
            .contents
            .insert_internal(1, Constant::Int(20), 3)
            .unwrap();

        // Create a new entry that will be part of new root
        let new_entry = InternalNodeEntry {
            dataval: Constant::Int(30),
            block_num: 4,
        };

        // Make new root
        internal.make_new_root(new_entry).unwrap();

        // Verify root structure
        assert!(matches!(
            internal.contents.get_flag().unwrap(),
            PageType::Internal(Some(1))
        ));
        assert_eq!(internal.contents.get_number_of_recs().unwrap(), 2);

        // First entry should point to block with original entries
        assert!(internal.contents.get_child_block_num(0).unwrap() > 0);
        // Second entry should be our new entry
        assert_eq!(internal.contents.get_child_block_num(1).unwrap(), 4);
    }

    #[test]
    fn test_insert_recursive_split() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (_, mut internal) = setup_internal_node(&db);

        // Create a multi-level tree by filling and splitting nodes
        let mut value = 1;
        while !internal.contents.is_one_off_full().unwrap() {
            let entry = InternalNodeEntry {
                dataval: Constant::Int(value),
                block_num: value as usize,
            };
            internal.insert_entry(entry).unwrap();
            value += 1;
        }

        // Insert one more to force recursive split
        let entry = InternalNodeEntry {
            dataval: Constant::Int(value),
            block_num: value as usize,
        };

        let split_result = internal.insert_entry(entry).unwrap();
        assert!(split_result.is_some());

        // Verify the split maintained tree properties
        let leaf_block_num = internal.search(&Constant::Int(3)).unwrap();
        assert!(leaf_block_num > 0);
    }

    #[test]
    fn test_edge_cases() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (_, internal) = setup_internal_node(&db);

        // Test inserting duplicate keys
        internal
            .insert_entry(InternalNodeEntry {
                dataval: Constant::Int(10),
                block_num: 1,
            })
            .unwrap();
        internal
            .insert_entry(InternalNodeEntry {
                dataval: Constant::Int(10),
                block_num: 2,
            })
            .unwrap();

        println!("the page contents {}", internal.contents);

        //  NOTE: It looks like the numbers are reversed here in the sense that the block numbers asserted are backwards
        //  but they are correct because the insertion into the node results in a page that looks like this where block 2
        //  is in slot 0
        //  === BTreePage Debug ===
        //  Block: BlockId { filename: "test_file_1746190249550660000_ThreadId(2)", block_num: 0 }
        //  Page Type: Internal(None)
        //  Record Count: 2
        //  Entries:
        //  Slot 0: Key=Int(10), Child Block=2
        //  Slot 1: Key=Int(10), Child Block=1
        //  ====================
        // Search should return the rightmost child for duplicate key
        let result = internal.find_child_block(&Constant::Int(10)).unwrap();
        assert_eq!(result.block_num, 1);

        // Test searching for key less than all entries
        let result = internal.find_child_block(&Constant::Int(5)).unwrap();
        assert_eq!(result.block_num, 2);

        // Test searching for key greater than all entries
        let result = internal.find_child_block(&Constant::Int(15)).unwrap();
        assert_eq!(result.block_num, 1);
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
    contents: BTreePage,
    current_slot: Option<usize>,
    file_name: String,
}

impl std::fmt::Display for BTreeLeaf {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreeLeaf Debug ===")?;
        writeln!(f, "Search Key: {:?}", self.search_key)?;
        writeln!(f, "Current Slot: {:?}", self.current_slot)?;
        writeln!(f, "File Name: {}", self.file_name)?;
        writeln!(f, "\nContents:")?;
        write!(f, "{}", self.contents)?;
        Ok(())
    }
}

impl BTreeLeaf {
    /// Creates a new [BTreeLeaf] with the given transaction, block ID, layout, search key and filename
    /// The page is initialized with an appropriate slot based on the search key position
    fn new(
        txn: Arc<Transaction>,
        block_id: BlockId,
        layout: Layout,
        search_key: Constant,
        file_name: String,
    ) -> Result<Self, Box<dyn Error>> {
        let contents = BTreePage::new(Arc::clone(&txn), block_id, layout.clone());
        let current_slot = contents.find_slot_before(&search_key)?;
        Ok(Self {
            txn,
            layout,
            search_key,
            contents,
            current_slot,
            file_name,
        })
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
        if self.current_slot.unwrap() >= self.contents.get_number_of_recs()? {
            self.try_overflow()
        } else if self.contents.get_data_value(self.current_slot.unwrap())? == self.search_key {
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
            if self.contents.get_rid(self.current_slot.unwrap())? == rid {
                self.contents.delete(self.current_slot.unwrap())?;
                return Ok(());
            }
        }
        Err("RID not found in BTreeLeaf".into())
    }

    /// This method will attempt to insert an entry into a [BTreeLeaf] page
    /// If the leaf page has an overflow page, and the new entry is smaller than the first key, split the page
    /// If the page splits, return the [InternalNodeEntry] identifier to the new page
    fn insert(&mut self, rid: RID) -> Result<Option<InternalNodeEntry>, Box<dyn Error>> {
        //  If this page has an overflow page, and the key being inserted is less than the first key force a split
        //  This is done to ensure that overflow pages are linked to a page with the first key the same as entries in overflow pages
        debug!("Inserting rid {:?} into BTreeLeaf", rid);
        if matches!(self.contents.get_flag()?, PageType::Leaf(Some(_)))
            && self.contents.get_data_value(0)? > self.search_key
        {
            debug!("Inserting a record smaller than the first record into a page full of identical records");
            let first_entry = self.contents.get_data_value(0)?;
            let new_block_id = self.contents.split(0, self.contents.get_flag()?)?;
            self.current_slot = Some(0);
            self.contents.set_flag(PageType::Leaf(None))?;
            self.contents.insert_leaf(0, self.search_key.clone(), rid)?;
            return Ok(Some(InternalNodeEntry {
                dataval: first_entry,
                block_num: new_block_id.block_num,
            }));
        }

        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };
        self.contents
            .insert_leaf(self.current_slot.unwrap(), self.search_key.clone(), rid)?;
        if !self.contents.is_full()? {
            debug!("Done inserting rid {:?} into BTreeLeaf", rid);
            return Ok(None);
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
        debug!("State of BTreeLeaf before split {}", self.contents);
        let first_key = self.contents.get_data_value(0)?;
        let last_key = self
            .contents
            .get_data_value(self.contents.get_number_of_recs()? - 1)?;
        if first_key == last_key {
            debug!("The first key and last key are identical, so moving everything except first record into overflow page");
            let new_block_id = self.contents.split(1, self.contents.get_flag()?)?;
            self.contents
                .set_flag(PageType::Leaf(Some(new_block_id.block_num)))?;
            debug!("Done splitting BTreeLeaf");
            return Ok(None);
        }

        debug!("Finding the split point");
        let mut split_point = self.contents.get_number_of_recs()? / 2;
        debug!("The split point {}", split_point);
        let mut split_record = self.contents.get_data_value(split_point)?;
        if split_record == first_key {
            debug!("Moving split point to the right");
            while self.contents.get_data_value(split_point)? == first_key {
                split_point += 1;
            }
            split_record = self.contents.get_data_value(split_point)?;
        } else {
            debug!("Moving split point to the left");
            while self.contents.get_data_value(split_point - 1)? == split_record {
                split_point -= 1;
            }
        }
        debug!("Splitting at {}", split_point);
        let new_block_id = self.contents.split(split_point, PageType::Leaf(None))?;

        Ok(Some(InternalNodeEntry {
            dataval: split_record,
            block_num: new_block_id.block_num,
        }))
    }

    /// This method will check to see if an overflow page is present for this block
    /// An overflow page for a specific page will contain entries that are the same as the first key of the current page
    /// If no overflow page can be found, just return. Otherwise swap out the current contents for the overflow contents
    fn try_overflow(&mut self) -> Result<Option<()>, Box<dyn Error>> {
        let first_key = self.contents.get_data_value(0)?;

        if first_key != self.search_key
            || !matches!(self.contents.get_flag()?, PageType::Leaf(Some(_)))
        {
            return Ok(None);
        }

        let PageType::Leaf(Some(overflow_block_num)) = self.contents.get_flag()? else {
            return Ok(None);
        };

        let overflow_contents = BTreePage::new(
            Arc::clone(&self.txn),
            BlockId::new(self.file_name.clone(), overflow_block_num),
            self.layout.clone(),
        );
        self.contents = overflow_contents;
        Ok(Some(()))
    }

    fn get_data_rid(&self) -> Result<RID, Box<dyn Error>> {
        self.contents.get_rid(
            self.current_slot
                .expect("Current slot not set in BTreeLeaf::get_data_rid"),
        )
    }
}

#[cfg(test)]
mod btree_leaf_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    fn setup_leaf(db: &SimpleDB, search_key: Constant) -> (Arc<Transaction>, BTreeLeaf) {
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        // Format the page as a leaf
        let page = BTreePage::new(Arc::clone(&tx), block.clone(), layout.clone());
        page.format(PageType::Leaf(None)).unwrap();

        let leaf = BTreeLeaf::new(
            Arc::clone(&tx),
            block,
            layout,
            search_key,
            generate_filename(),
        )
        .unwrap();

        (tx, leaf)
    }

    #[test]
    fn test_insert_no_split() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (tx, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Insert should succeed without splitting
        assert!(leaf.insert(RID::new(1, 1)).unwrap().is_none());

        // Verify the record was inserted
        assert_eq!(leaf.contents.get_number_of_recs().unwrap(), 1);
        assert_eq!(leaf.contents.get_data_value(0).unwrap(), Constant::Int(10));
    }

    #[test]
    fn test_insert_with_split_different_keys() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (tx, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Fill the page with different keys
        let mut slot = 0;
        // let mut split_result = None;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.search_key = Constant::Int(slot);
            leaf.insert(RID::new(1, slot as usize)).unwrap();
            slot += 1;
        }

        let split_result = leaf.insert(RID::new(1, slot as usize)).unwrap();

        // Verify split occurred
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.block_num, 1); //  this is a new file that has just added a new block
        assert_eq!(entry.dataval, Constant::Int((slot + 1) / 2)); // Middle key. Adding 1 to slot because slot is 0-based
    }

    #[test]
    fn test_insert_with_overflow_same_keys() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (tx, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Fill the page with same key
        let mut slot = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.insert(RID::new(1, slot)).unwrap();
            slot += 1;
        }

        // Insert one more record with same key to force overflow
        let split_result = leaf.insert(RID::new(1, slot)).unwrap();

        // Verify overflow block was created
        assert!(split_result.is_none()); //  overflow block returns None
        let PageType::Leaf(Some(overflow_num)) = leaf.contents.get_flag().unwrap() else {
            panic!("Expected overflow block");
        };

        // Verify first key matches in both pages
        assert_eq!(leaf.contents.get_data_value(0).unwrap(), Constant::Int(10));
    }

    #[test]
    fn test_insert_with_existing_overflow() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let (tx, mut leaf) = setup_leaf(&db, Constant::Int(5));

        // Create a page with overflow block containing key 10
        leaf.search_key = Constant::Int(10);
        let mut slot = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.insert(RID::new(1, slot)).unwrap();
            slot += 1;
        }
        leaf.insert(RID::new(1, slot)).unwrap(); // Create overflow with split

        // Try to insert key 5 (less than 10) which will force another split
        leaf.search_key = Constant::Int(5);
        let split_result = leaf.insert(RID::new(2, 1)).unwrap();

        // Verify page was split
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.dataval, Constant::Int(10));
    }

    #[test]
    fn test_insert_edge_cases() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);

        // Test case 1: Insert when split point equals first key
        let (_, mut leaf) = setup_leaf(&db, Constant::Int(10));
        // Fill page with alternating 10s and 20s
        let mut counter = 0;
        while !leaf.contents.is_one_off_full().unwrap() {
            leaf.search_key = Constant::Int(if counter % 2 == 0 { 10 } else { 20 });
            leaf.insert(RID::new(1, counter)).unwrap();
            counter += 1;
        }

        // Force a split - should move split point right until after all 10s
        leaf.search_key = Constant::Int(15);
        let split_result = leaf.insert(RID::new(1, 10)).unwrap();
        assert!(split_result.is_some());
        let entry = split_result.unwrap();
        assert_eq!(entry.dataval, Constant::Int(20)); // First non-10 value
    }
}

struct InternalNodeEntry {
    dataval: Constant,
    block_num: usize,
}

/// The general format of the BTreePage
/// +--------------------+----------------------+----------------------+
/// | flag (4 bytes)     | record count (4B)    | record slots [...]   |
/// +--------------------+----------------------+----------------------+
///     ^ offset 0            ^ offset 4              ^ offset 8
///
/// The format of the record slot for the leaf page
/// +-------------+---------------+--------------+
/// | dataval     | block number  | slot number  |
/// +-------------+---------------+--------------+
///
/// The format of the record slot for the internal page
/// +-------------+------------------+
/// | dataval     | child block num  |
/// +-------------+------------------+

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum PageType {
    Internal(Option<usize>),
    Leaf(Option<usize>),
}

impl From<i32> for PageType {
    fn from(value: i32) -> Self {
        const TYPE_MASK: i32 = 1 << 31;
        const VALUE_MASK: i32 = !(1 << 31);
        let is_internal = value & TYPE_MASK == 0;
        if is_internal {
            if value == 0 {
                PageType::Internal(None)
            } else {
                PageType::Internal(Some((value & VALUE_MASK) as usize))
            }
        } else {
            let val = value & VALUE_MASK;
            if val == 0 {
                PageType::Leaf(None)
            } else {
                PageType::Leaf(Some(val as usize))
            }
        }
    }
}

impl From<PageType> for i32 {
    fn from(value: PageType) -> Self {
        const TYPE_MASK: i32 = 1 << 31;
        match value {
            PageType::Internal(None) => 0,
            PageType::Internal(Some(n)) => n as i32,
            PageType::Leaf(None) => TYPE_MASK,
            PageType::Leaf(Some(n)) => TYPE_MASK | (n as i32),
        }
    }
}

struct BTreePage {
    txn: Arc<Transaction>,
    handle: BufferHandle,
    layout: Layout,
}

impl BTreePage {
    const INT_BYTES: usize = 4;

    // Column name constants
    // const DATA_VAL_COLUMN: &'static str = "dataval";
    // const BLOCK_NUM_COLUMN: &'static str = "block";
    // const SLOT_NUM_COLUMN: &'static str = "id";

    /// Creates a new [BTreePage] by pinning the specified block and initializing it with the given layout
    fn new(txn: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = txn.pin(&block_id);
        Self {
            txn,
            handle,
            layout,
        }
    }

    /// Finds the rightmost slot position before where the search key should be inserted
    /// Returns None if the search key belongs at the start of the page
    /// Returns Some(pos) where pos is the index of the rightmost record less than search_key
    fn find_slot_before(&self, search_key: &Constant) -> Result<Option<usize>, Box<dyn Error>> {
        let mut current_slot = 0;
        while current_slot < self.get_number_of_recs()?
            && self.get_data_value(current_slot)? < *search_key
        {
            current_slot += 1;
        }
        if current_slot == 0 {
            Ok(None)
        } else {
            Ok(Some(current_slot - 1))
        }
    }

    /// Returns true if adding two more records would exceed the block size
    /// Used primarily for testing to detect splits before they occur
    fn is_one_off_full(&self) -> Result<bool, Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 2) > self.txn.block_size())
    }

    /// Returns true if adding one more record would exceed the block size
    fn is_full(&self) -> Result<bool, Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 1) > self.txn.block_size())
    }

    /// This method splits the existing [BTreePage] and moves the records from [slot..]
    /// into a new page and then returns the [BlockId] of the new page
    /// The current page continues to be the same, but with fewer records
    fn split(&self, slot: usize, page_type: PageType) -> Result<BlockId, Box<dyn Error>> {
        //  construct a new block, a new btree page and then pin the buffer
        debug!(
            "Splitting the btree page for block num {} at slot {}",
            self.handle.block_id().block_num,
            slot
        );
        let block_id = self.txn.append(&self.handle.block_id().filename);
        let new_btree_page =
            BTreePage::new(Arc::clone(&self.txn), block_id.clone(), self.layout.clone());
        new_btree_page.format(page_type)?;

        //  set the metadata on the new page
        new_btree_page.set_flag(page_type)?;

        //  move the records from [slot..] to the new page
        let mut dest_slot = 0;
        while slot < self.get_number_of_recs()? {
            new_btree_page.insert(dest_slot)?;
            for field in &self.layout.schema.fields {
                new_btree_page.set_value(dest_slot, field, self.get_value(slot, field)?)?;
            }
            self.delete(slot)?;
            dest_slot += 1;
        }

        Ok(block_id)
    }

    /// Formats a new page by initializing its flag and record count
    /// Sets all record slots to their zero values based on field types
    fn format(&self, page_type: PageType) -> Result<(), Box<dyn Error>> {
        self.txn
            .set_int(self.handle.block_id(), 0, page_type.into(), true)?;
        self.txn
            .set_int(self.handle.block_id(), Self::INT_BYTES, 0, true)?;
        let record_size = self.layout.slot_size;
        for i in ((2 * Self::INT_BYTES)..self.txn.block_size()).step_by(record_size) {
            for field in &self.layout.schema.fields {
                let field_type = self.layout.schema.info.get(field).unwrap().field_type;
                match field_type {
                    FieldType::Int => {
                        self.txn.set_int(self.handle.block_id(), i, 0, false)?;
                    }
                    FieldType::String => {
                        self.txn.set_string(self.handle.block_id(), i, "", false)?;
                    }
                }
            }
        }
        Ok(())
    }

    /// Retrieves the page type flag from the header
    fn get_flag(&self) -> Result<PageType, Box<dyn Error>> {
        self.txn
            .get_int(self.handle.block_id(), 0)
            .map(PageType::from)
    }

    /// Updates the page type flag in the header
    fn set_flag(&self, value: PageType) -> Result<(), Box<dyn Error>> {
        self.txn
            .set_int(self.handle.block_id(), 0, value.into(), true)
    }

    /// Gets the data value at the specified slot
    fn get_data_value(&self, slot: usize) -> Result<Constant, Box<dyn Error>> {
        let value = self.get_value(slot, IndexInfo::DATA_FIELD)?;
        Ok(value)
    }

    /// Gets the child block number at the specified slot (for internal nodes)
    fn get_child_block_num(&self, slot: usize) -> Result<usize, Box<dyn Error>> {
        let block_num = self.get_int(slot, IndexInfo::BLOCK_NUM_FIELD)? as usize;
        Ok(block_num)
    }

    /// Gets the RID stored at the specified slot (for leaf nodes)
    fn get_rid(&self, slot: usize) -> Result<RID, Box<dyn Error>> {
        let block_num = self.get_int(slot, IndexInfo::BLOCK_NUM_FIELD)? as usize;
        let slot_num = self.get_int(slot, IndexInfo::ID_FIELD)? as usize;
        Ok(RID::new(block_num, slot_num))
    }

    /// Inserts a directory entry at the specified slot (for internal nodes)
    /// Directory entries contain a data value and child block number
    fn insert_internal(
        &self,
        slot: usize,
        value: Constant,
        block_num: usize,
    ) -> Result<(), Box<dyn Error>> {
        self.insert(slot)?;
        self.set_value(slot, IndexInfo::DATA_FIELD, value)?;
        self.set_int(slot, IndexInfo::BLOCK_NUM_FIELD, block_num as i32)?;
        Ok(())
    }

    /// Inserts a leaf entry at the specified slot
    /// Leaf entries contain a data value and RID pointing to the actual record
    fn insert_leaf(&self, slot: usize, value: Constant, rid: RID) -> Result<(), Box<dyn Error>> {
        self.insert(slot)?;
        self.set_value(slot, IndexInfo::DATA_FIELD, value)?;
        self.set_int(slot, IndexInfo::BLOCK_NUM_FIELD, rid.block_num as i32)?;
        self.set_int(slot, IndexInfo::ID_FIELD, rid.slot as i32)?;
        Ok(())
    }

    /// Inserts space for a new record at the specified slot
    /// Shifts all following records right by one position
    fn insert(&self, slot: usize) -> Result<(), Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        for i in (slot..current_records).rev() {
            //  move records over by one to the right
            self.copy_record(i, i + 1)?;
        }
        self.set_number_of_recs(current_records + 1)?;
        Ok(())
    }

    /// Deletes the record at the specified slot
    /// Shifts all following records left by one position
    fn delete(&self, slot: usize) -> Result<(), Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        for i in slot + 1..current_records {
            self.copy_record(i, i - 1)?;
        }
        self.set_number_of_recs(current_records - 1)?;
        Ok(())
    }

    /// Copies all fields from one record slot to another
    fn copy_record(&self, from: usize, to: usize) -> Result<(), Box<dyn Error>> {
        for field in &self.layout.schema.fields {
            self.set_value(to, field, self.get_value(from, field)?)?;
        }
        Ok(())
    }

    /// Gets the number of records currently stored in the page
    fn get_number_of_recs(&self) -> Result<usize, Box<dyn Error>> {
        self.txn
            .get_int(self.handle.block_id(), Self::INT_BYTES)
            .map(|v| v as usize)
    }

    /// Updates the number of records stored in the page
    fn set_number_of_recs(&self, num: usize) -> Result<(), Box<dyn Error>> {
        self.txn
            .set_int(self.handle.block_id(), Self::INT_BYTES, num as i32, true)
    }

    fn get_int(&self, slot: usize, field_name: &str) -> Result<i32, Box<dyn Error>> {
        self.txn.get_int(
            self.handle.block_id(),
            self.slot_pos(slot) + self.layout.offset(field_name).unwrap(),
        )
    }

    fn set_int(&self, slot: usize, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        self.txn.set_int(
            self.handle.block_id(),
            self.field_position(slot, field_name),
            value,
            true,
        )
    }

    fn get_string(&self, slot: usize, field_name: &str) -> Result<String, Box<dyn Error>> {
        self.txn.get_string(
            self.handle.block_id(),
            self.slot_pos(slot) + self.layout.offset(field_name).unwrap(),
        )
    }

    fn set_string(
        &self,
        slot: usize,
        field_name: &str,
        value: String,
    ) -> Result<(), Box<dyn Error>> {
        self.txn.set_string(
            self.handle.block_id(),
            self.field_position(slot, field_name),
            &value,
            true,
        )
    }

    fn get_value(&self, slot: usize, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        let field_type = self
            .layout
            .schema
            .info
            .get(field_name)
            .ok_or_else(|| format!("Field {field_name} not found in schema"))?
            .field_type;
        match field_type {
            FieldType::Int => {
                let value = self.get_int(slot, field_name)?;
                Ok(Constant::Int(value))
            }
            FieldType::String => {
                let value = self.get_string(slot, field_name)?;
                Ok(Constant::String(value))
            }
        }
    }

    fn set_value(
        &self,
        slot: usize,
        field_name: &str,
        value: Constant,
    ) -> Result<(), Box<dyn Error>> {
        // Get field type from schema
        let expected_type = self
            .layout
            .schema
            .info
            .get(field_name)
            .ok_or_else(|| format!("Field {field_name} not found in schema"))?
            .field_type;

        // Check if value type matches schema
        match (expected_type, &value) {
            (FieldType::Int, Constant::Int(v)) => self.set_int(slot, field_name, *v),
            (FieldType::String, Constant::String(v)) => {
                self.set_string(slot, field_name, v.clone())
            }
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("Type mismatch: expected {expected_type:?} but got {value:?}"),
            ))),
        }
    }

    /// Calculates the byte position of a field within a record slot
    fn field_position(&self, slot: usize, field_name: &str) -> usize {
        self.slot_pos(slot) + self.layout.offset(field_name).unwrap()
    }

    /// Calculates the starting byte position of a record slot
    fn slot_pos(&self, slot: usize) -> usize {
        Self::INT_BYTES + Self::INT_BYTES + slot * self.layout.slot_size
    }
}

impl std::fmt::Display for BTreePage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "\n=== BTreePage Debug ===")?;
        writeln!(f, "Block: {:?}", self.handle.block_id())?;
        match self.get_flag() {
            Ok(flag) => writeln!(f, "Page Type: {flag:?}")?,
            Err(e) => writeln!(f, "Error getting flag: {e}")?,
        }

        match self.get_number_of_recs() {
            Ok(count) => {
                writeln!(f, "Record Count: {count}")?;
                writeln!(f, "Entries:")?;
                match self.get_flag() {
                    Ok(PageType::Internal(_)) => {
                        for slot in 0..count {
                            if let (Ok(key), Ok(child)) =
                                (self.get_data_value(slot), self.get_child_block_num(slot))
                            {
                                writeln!(f, "Slot {slot}: Key={key:?}, Child Block={child}")?;
                            }
                        }
                    }
                    Ok(PageType::Leaf(_)) => {
                        for slot in 0..count {
                            if let (Ok(key), Ok(rid)) =
                                (self.get_data_value(slot), self.get_rid(slot))
                            {
                                writeln!(
                                    f,
                                    "Slot {}: Key={:?}, RID=(block={}, slot={})",
                                    slot, key, rid.block_num, rid.slot
                                )?;
                            }
                        }
                    }
                    Err(e) => writeln!(f, "Error getting page type: {e}")?,
                }
            }
            Err(e) => writeln!(f, "Error getting record count: {e}")?,
        }
        writeln!(f, "====================")
    }
}

#[cfg(test)]
mod btree_page_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(IndexInfo::DATA_FIELD);
        schema.add_int_field(IndexInfo::BLOCK_NUM_FIELD);
        schema.add_int_field(IndexInfo::ID_FIELD);
        Layout::new(schema)
    }

    #[test]
    fn test_btree_page_format() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf(None)).unwrap();

        assert_eq!(page.get_flag().unwrap(), PageType::Leaf(None));
        assert_eq!(page.get_number_of_recs().unwrap(), 0);
    }

    #[test]
    fn test_leaf_insert_and_delete() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf(None)).unwrap();

        // Insert a record
        let rid = RID::new(1, 1);
        page.insert_leaf(0, Constant::Int(10), rid).unwrap();

        // Verify record
        assert_eq!(page.get_number_of_recs().unwrap(), 1);
        assert_eq!(page.get_data_value(0).unwrap(), Constant::Int(10));
        assert_eq!(page.get_rid(0).unwrap(), rid);

        // Delete record
        page.delete(0).unwrap();
        assert_eq!(page.get_number_of_recs().unwrap(), 0);
    }

    #[test]
    fn test_page_split() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block.clone(), layout.clone());
        page.format(PageType::Leaf(None)).unwrap();

        // Insert records until full
        let mut slot = 0;
        while !page.is_full().unwrap() {
            page.insert_leaf(slot, Constant::Int(slot as i32), RID::new(1, slot))
                .unwrap();
            slot += 1;
        }

        // Split the page
        let split_point = slot / 2;
        let new_block = page.split(split_point, PageType::Leaf(None)).unwrap();

        // Verify original page
        assert_eq!(page.get_number_of_recs().unwrap(), split_point);

        // Verify new page
        let new_page = BTreePage::new(Arc::clone(&tx), new_block, layout);
        assert_eq!(new_page.get_number_of_recs().unwrap(), slot - split_point);
    }

    #[test]
    fn test_type_safety() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf(None)).unwrap();

        // Try to insert wrong type
        let result = page.set_value(0, "dataval", Constant::String("wrong type".to_string()));
        assert!(result.is_err());
    }

    #[test]
    fn test_internal_node_operations() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Internal(None)).unwrap();

        // Insert internal entry
        page.insert_internal(0, Constant::Int(10), 2).unwrap();

        // Verify entry
        assert_eq!(page.get_data_value(0).unwrap(), Constant::Int(10));
        assert_eq!(page.get_child_block_num(0).unwrap(), 2);
    }

    #[test]
    fn test_find_slot_before() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8, 5000);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf(None)).unwrap();

        page.insert_leaf(0, Constant::Int(10), RID::new(1, 1))
            .unwrap();
        page.insert_leaf(1, Constant::Int(20), RID::new(1, 2))
            .unwrap();
        page.insert_leaf(2, Constant::Int(30), RID::new(1, 3))
            .unwrap();

        assert_eq!(
            page.find_slot_before(&Constant::Int(15)).unwrap().unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(&Constant::Int(20)).unwrap().unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(&Constant::Int(25)).unwrap().unwrap(),
            1
        );
    }
}
