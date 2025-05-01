use std::{error::Error, sync::Arc};

use crate::{BlockId, Constant, FieldType, Layout, Transaction, RID};

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
    Leaf,
    Overflow(usize),
}

impl From<i32> for PageType {
    fn from(value: i32) -> Self {
        match value {
            0 => PageType::Internal,
            -1 => PageType::Leaf,
            n if n > 0 => PageType::Overflow(n as usize),
            _ => panic!("Invalid page type for value: {}", value),
        }
    }
}

impl From<PageType> for i32 {
    fn from(value: PageType) -> Self {
        match value {
            PageType::Internal => 0,
            PageType::Leaf => -1,
            PageType::Overflow(n) => n as i32,
        }
    }
}

struct BTreePage {
    page_type: PageType,
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

    fn new(page_type: PageType, txn: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        txn.pin(&block_id);
        Self {
            page_type,
            txn,
            block_id,
            layout,
        }
    }

    fn find_slot_before(&self, search_key: Constant) -> Result<usize, Box<dyn Error>> {
        let mut current_slot = 0;
        while current_slot < self.get_number_of_recs()?
            && self.get_data_value(current_slot)? < search_key
        {
            current_slot += 1;
        }
        Ok(current_slot - 1)
    }

    fn is_full(&self) -> Result<bool, Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 1) > self.txn.block_size())
    }

    fn split(&self, slot: usize, page_type: PageType) -> Result<BlockId, Box<dyn Error>> {
        //  construct a new block, a new btree page and then pin the buffer
        let block_id = self.txn.append(&self.block_id.filename);
        let new_btree_page = BTreePage::new(
            page_type,
            Arc::clone(&self.txn),
            block_id.clone(),
            self.layout.clone(),
        );

        //  set the metadata on the new page
        new_btree_page.set_flag(page_type)?;

        //  move the records from [slot..) to the new page
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
        matches!(self.page_type, PageType::Internal);
        let block_num = self.get_int(slot, Self::BLOCK_NUM_COLUMN)? as usize;
        Ok(block_num)
    }

    fn get_rid(&self, slot: usize) -> Result<RID, Box<dyn Error>> {
        matches!(self.page_type, PageType::Leaf);
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
        matches!(self.page_type, PageType::Internal);
        self.insert(slot)?;
        self.set_value(slot, Self::DATA_VAL_COLUMN, value)?;
        self.set_int(slot, Self::BLOCK_NUM_COLUMN, block_num as i32)?;
        Ok(())
    }

    fn insert_leaf(&self, slot: usize, value: Constant, rid: RID) -> Result<(), Box<dyn Error>> {
        matches!(self.page_type, PageType::Leaf);
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

        let page = BTreePage::new(PageType::Leaf, Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf).unwrap();

        assert_eq!(page.get_flag().unwrap(), PageType::Leaf);
        assert_eq!(page.get_number_of_recs().unwrap(), 0);
    }

    #[test]
    fn test_leaf_insert_and_delete() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(PageType::Leaf, Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf).unwrap();

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

        let page = BTreePage::new(
            PageType::Leaf,
            Arc::clone(&tx),
            block.clone(),
            layout.clone(),
        );
        page.format(PageType::Leaf).unwrap();

        // Insert records until full
        let mut slot = 0;
        while !page.is_full().unwrap() {
            page.insert_leaf(slot, Constant::Int(slot as i32), RID::new(1, slot))
                .unwrap();
            slot += 1;
        }

        // Split the page
        let split_point = slot / 2;
        let new_block = page.split(split_point, PageType::Leaf).unwrap();

        // Verify original page
        assert_eq!(page.get_number_of_recs().unwrap(), split_point);

        // Verify new page
        let new_page = BTreePage::new(PageType::Leaf, Arc::clone(&tx), new_block, layout);
        assert_eq!(new_page.get_number_of_recs().unwrap(), slot - split_point);
    }

    #[test]
    fn test_type_safety() {
        let (db, _dir) = SimpleDB::new_for_test(400, 8);
        let tx = Arc::new(db.new_tx());
        let block = tx.append(&generate_filename());
        let layout = create_test_layout();

        let page = BTreePage::new(PageType::Leaf, Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf).unwrap();

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

        let page = BTreePage::new(PageType::Internal, Arc::clone(&tx), block, layout);
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

        let page = BTreePage::new(PageType::Leaf, Arc::clone(&tx), block, layout);
        page.format(PageType::Leaf).unwrap();

        page.insert_leaf(0, Constant::Int(10), RID::new(1, 1))
            .unwrap();
        page.insert_leaf(1, Constant::Int(20), RID::new(1, 2))
            .unwrap();
        page.insert_leaf(2, Constant::Int(30), RID::new(1, 3))
            .unwrap();

        assert_eq!(page.find_slot_before(Constant::Int(15)).unwrap(), 0);
        assert_eq!(page.find_slot_before(Constant::Int(20)).unwrap(), 0);
        assert_eq!(page.find_slot_before(Constant::Int(25)).unwrap(), 1);
    }
}
