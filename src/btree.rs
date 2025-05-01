use std::{error::Error, sync::Arc};

use crate::{debug, BlockId, Constant, FieldType, Layout, Transaction, RID};

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

impl BTreeLeaf {
    fn new(
        txn: Arc<Transaction>,
        block_id: BlockId,
        layout: Layout,
        search_key: Constant,
        file_name: String,
    ) -> Result<Self, Box<dyn Error>> {
        let contents = BTreePage::new(Arc::clone(&txn), block_id, layout.clone());
        let current_slot = contents.find_slot_before(search_key.clone())?;
        Ok(Self {
            txn,
            layout,
            search_key,
            contents,
            current_slot,
            file_name,
        })
    }

    fn next(&mut self) -> Result<Option<()>, Box<dyn Error>> {
        self.current_slot = {
            match self.current_slot {
                Some(slot) => Some(slot + 1),
                None => Some(0),
            }
        };
        if self.current_slot.unwrap() >= self.contents.get_number_of_recs()? {
            return self.try_overflow();
        } else if self.contents.get_data_value(self.current_slot.unwrap())? == self.search_key {
            return Ok(Some(()));
        } else {
            return self.try_overflow();
        }
    }

    fn delete(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        assert!(self.current_slot.is_some());
        while let Some(_) = self.next()? {
            if self.contents.get_rid(self.current_slot.unwrap())? == rid {
                self.contents.delete(self.current_slot.unwrap())?;
                return Ok(());
            }
        }
        return Err("RID not found in BTreeLeaf".into());
    }

    /// This method will attempt to insert an entry into a [BTreeLeaf] page
    /// If the leaf page has an overflow page, and the new entry is smaller than the first key, split the page
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
}

#[cfg(test)]
mod btree_leaf_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(BTreePage::DATA_VAL_COLUMN);
        schema.add_int_field(BTreePage::BLOCK_NUM_COLUMN);
        schema.add_int_field(BTreePage::SLOT_NUM_COLUMN);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
        let (tx, mut leaf) = setup_leaf(&db, Constant::Int(10));

        // Insert should succeed without splitting
        assert!(leaf.insert(RID::new(1, 1)).unwrap().is_none());

        // Verify the record was inserted
        assert_eq!(leaf.contents.get_number_of_recs().unwrap(), 1);
        assert_eq!(leaf.contents.get_data_value(0).unwrap(), Constant::Int(10));
    }

    #[test]
    fn test_insert_with_split_different_keys() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);

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
    Internal,
    Leaf(Option<usize>),
}

impl From<i32> for PageType {
    fn from(value: i32) -> Self {
        match value {
            0 => PageType::Internal,
            -1 => PageType::Leaf(None),
            n if n > 0 => PageType::Leaf(Some(n as usize)),
            _ => panic!("Invalid page type for value: {}", value),
        }
    }
}

impl From<PageType> for i32 {
    fn from(value: PageType) -> Self {
        match value {
            PageType::Internal => 0,
            PageType::Leaf(None) => -1,
            PageType::Leaf(Some(n)) => n as i32,
        }
    }
}

struct BTreePage {
    txn: Arc<Transaction>,
    block_id: BlockId,
    layout: Layout,
}

impl BTreePage {
    const INT_BYTES: usize = 4;

    // Column name constants
    const DATA_VAL_COLUMN: &'static str = "dataval";
    const BLOCK_NUM_COLUMN: &'static str = "block";
    const SLOT_NUM_COLUMN: &'static str = "id";

    fn new(txn: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        txn.pin(&block_id);
        Self {
            txn,
            block_id,
            layout,
        }
    }

    fn find_slot_before(&self, search_key: Constant) -> Result<Option<usize>, Box<dyn Error>> {
        let mut current_slot = 0;
        while current_slot < self.get_number_of_recs()?
            && self.get_data_value(current_slot)? < search_key
        {
            current_slot += 1;
        }
        if current_slot == 0 {
            return Ok(None);
        } else {
            return Ok(Some(current_slot - 1));
        }
    }

    /// Helper method for tests since just checking [BTreePage::is_full] leads to issues because
    /// the [BTreeLeaf::insert] does that check itself and splits the page before the test can do anything
    fn is_one_off_full(&self) -> Result<bool, Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 2) > self.txn.block_size())
    }

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
            self.block_id.block_num, slot
        );
        let block_id = self.txn.append(&self.block_id.filename);
        let new_btree_page =
            BTreePage::new(Arc::clone(&self.txn), block_id.clone(), self.layout.clone());
        new_btree_page.format(page_type)?;

        //  set the metadata on the new page
        new_btree_page.set_flag(page_type)?;

        //  move the records from [slot..] to the new page
        let current_records = self.get_number_of_recs()?;
        let mut new_slot = 0;
        for i in slot..current_records {
            new_btree_page.insert(new_slot)?;
            for field in &self.layout.schema.fields {
                new_btree_page.set_value(new_slot, field, self.get_value(i, field)?)?;
            }
            self.delete(i)?;
            new_slot += 1;
        }

        Ok(block_id)
    }

    fn format(&self, page_type: PageType) -> Result<(), Box<dyn Error>> {
        self.txn
            .set_int(&self.block_id, 0, page_type.into(), true)?;
        self.txn.set_int(&self.block_id, Self::INT_BYTES, 0, true)?;
        let record_size = self.layout.slot_size;
        for i in ((2 * Self::INT_BYTES)..self.txn.block_size()).step_by(record_size) {
            for field in &self.layout.schema.fields {
                let field_type = self.layout.schema.info.get(field).unwrap().field_type;
                match field_type {
                    FieldType::INT => {
                        self.txn.set_int(&self.block_id, i, 0, false)?;
                    }
                    FieldType::STRING => {
                        self.txn.set_string(&self.block_id, i, "", false)?;
                    }
                }
            }
        }
        Ok(())
    }

    fn get_flag(&self) -> Result<PageType, Box<dyn Error>> {
        self.txn.get_int(&self.block_id, 0).map(PageType::from)
    }

    fn set_flag(&self, value: PageType) -> Result<(), Box<dyn Error>> {
        self.txn.set_int(&self.block_id, 0, value.into(), true)
    }

    fn get_data_value(&self, slot: usize) -> Result<Constant, Box<dyn Error>> {
        let value = self.get_value(slot, Self::DATA_VAL_COLUMN)?;
        Ok(value)
    }

    fn get_child_block_num(&self, slot: usize) -> Result<usize, Box<dyn Error>> {
        let block_num = self.get_int(slot, Self::BLOCK_NUM_COLUMN)? as usize;
        Ok(block_num)
    }

    fn get_rid(&self, slot: usize) -> Result<RID, Box<dyn Error>> {
        let block_num = self.get_int(slot, Self::BLOCK_NUM_COLUMN)? as usize;
        let slot_num = self.get_int(slot, Self::SLOT_NUM_COLUMN)? as usize;
        Ok(RID::new(block_num, slot_num))
    }

    fn insert_dir(
        &self,
        slot: usize,
        value: Constant,
        block_num: usize,
    ) -> Result<(), Box<dyn Error>> {
        self.insert(slot)?;
        self.set_value(slot, Self::DATA_VAL_COLUMN, value)?;
        self.set_int(slot, Self::BLOCK_NUM_COLUMN, block_num as i32)?;
        Ok(())
    }

    fn insert_leaf(&self, slot: usize, value: Constant, rid: RID) -> Result<(), Box<dyn Error>> {
        self.insert(slot)?;
        self.set_value(slot, Self::DATA_VAL_COLUMN, value)?;
        self.set_int(slot, Self::BLOCK_NUM_COLUMN, rid.block_num as i32)?;
        self.set_int(slot, Self::SLOT_NUM_COLUMN, rid.slot as i32)?;
        Ok(())
    }

    fn insert(&self, slot: usize) -> Result<(), Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        for i in (slot..current_records).rev() {
            //  move records over by one to the right
            self.copy_record(i, i + 1)?;
        }
        self.set_number_of_recs(current_records + 1)?;
        Ok(())
    }

    fn delete(&self, slot: usize) -> Result<(), Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        for i in slot + 1..current_records {
            self.copy_record(i, i - 1)?;
        }
        self.set_number_of_recs(current_records - 1)?;
        Ok(())
    }

    fn copy_record(&self, from: usize, to: usize) -> Result<(), Box<dyn Error>> {
        for field in &self.layout.schema.fields {
            self.set_value(to, field, self.get_value(from, field)?)?;
        }
        Ok(())
    }

    fn get_number_of_recs(&self) -> Result<usize, Box<dyn Error>> {
        self.txn
            .get_int(&self.block_id, Self::INT_BYTES)
            .map(|v| v as usize)
    }

    fn set_number_of_recs(&self, num: usize) -> Result<(), Box<dyn Error>> {
        self.txn
            .set_int(&self.block_id, Self::INT_BYTES, num as i32, true)
    }

    fn get_int(&self, slot: usize, field_name: &str) -> Result<i32, Box<dyn Error>> {
        self.txn.get_int(
            &self.block_id,
            self.slot_pos(slot) + self.layout.offset(field_name).unwrap(),
        )
    }

    fn set_int(&self, slot: usize, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        self.txn.set_int(
            &self.block_id,
            self.field_position(slot, field_name),
            value,
            true,
        )
    }

    fn get_string(&self, slot: usize, field_name: &str) -> Result<String, Box<dyn Error>> {
        self.txn.get_string(
            &self.block_id,
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
            &self.block_id,
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
            .ok_or_else(|| format!("Field {} not found in schema", field_name))?
            .field_type;
        match field_type {
            FieldType::INT => {
                let value = self.get_int(slot, field_name)?;
                Ok(Constant::Int(value))
            }
            FieldType::STRING => {
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
            .ok_or_else(|| format!("Field {} not found in schema", field_name))?
            .field_type;

        // Check if value type matches schema
        match (expected_type, &value) {
            (FieldType::INT, Constant::Int(v)) => self.set_int(slot, field_name, *v),
            (FieldType::STRING, Constant::String(v)) => {
                self.set_string(slot, field_name, v.clone())
            }
            _ => Err(Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Type mismatch: expected {:?} but got {:?}",
                    expected_type, value
                ),
            ))),
        }
    }

    fn field_position(&self, slot: usize, field_name: &str) -> usize {
        self.slot_pos(slot) + self.layout.offset(field_name).unwrap()
    }

    fn slot_pos(&self, slot: usize) -> usize {
        Self::INT_BYTES + Self::INT_BYTES + slot * self.layout.slot_size
    }

    fn close(&self) {
        self.txn.unpin(&self.block_id);
    }
}

impl Drop for BTreePage {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod btree_page_tests {
    use super::*;
    use crate::{test_utils::generate_filename, Schema, SimpleDB};

    fn create_test_layout() -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field(BTreePage::DATA_VAL_COLUMN);
        schema.add_int_field(BTreePage::BLOCK_NUM_COLUMN);
        schema.add_int_field(BTreePage::SLOT_NUM_COLUMN);
        Layout::new(schema)
    }

    #[test]
    fn test_btree_page_format() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(Arc::clone(&tx), block, layout);
        page.format(PageType::Internal).unwrap();

        // Insert directory entry
        page.insert_dir(0, Constant::Int(10), 2).unwrap();

        // Verify entry
        assert_eq!(page.get_data_value(0).unwrap(), Constant::Int(10));
        assert_eq!(page.get_child_block_num(0).unwrap(), 2);
    }

    #[test]
    fn test_find_slot_before() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
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
            page.find_slot_before(Constant::Int(15)).unwrap().unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(Constant::Int(20)).unwrap().unwrap(),
            0
        );
        assert_eq!(
            page.find_slot_before(Constant::Int(25)).unwrap().unwrap(),
            1
        );
    }
}
