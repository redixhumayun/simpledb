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

#[derive(Clone, Copy)]
enum PageType {
    Internal = 0,
    Leaf = 1,
}

impl From<i32> for PageType {
    fn from(value: i32) -> Self {
        match value {
            0 => PageType::Internal,
            1 => PageType::Leaf,
            _ => panic!("Invalid page type for value: {}", value),
        }
    }
}

impl From<PageType> for i32 {
    fn from(value: PageType) -> Self {
        value as i32
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
    fn new(page_type: PageType, txn: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        txn.pin(&block_id);
        Self {
            page_type,
            txn,
            block_id,
            layout,
        }
    }

    fn is_full(&self) -> Result<bool, Box<dyn Error>> {
        let current_records = self.get_number_of_recs()?;
        Ok(self.slot_pos(current_records + 1) > self.txn.block_size())
    }

    fn split(&self, slot: usize, page_type: PageType) -> Result<(), Box<dyn Error>> {
        //  construct a new block, a new btree page and then pin the buffer
        let block_id = self.txn.append(&self.block_id.filename);
        let new_btree_page = BTreePage::new(
            page_type,
            Arc::clone(&self.txn),
            block_id,
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
        //  close the new page
        new_btree_page.close();
        Ok(())
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
        let value = self.get_value(slot, "dataval")?;
        Ok(value)
    }

    fn get_child_block_num(&self, slot: usize) -> Result<usize, Box<dyn Error>> {
        matches!(self.page_type, PageType::Internal);
        let block_num = self.get_int(slot, "block")? as usize;
        Ok(block_num)
    }

    fn get_rid(&self, slot: usize) -> Result<RID, Box<dyn Error>> {
        matches!(self.page_type, PageType::Leaf);
        let block_num = self.get_int(slot, "block")? as usize;
        let slot_num = self.get_int(slot, "id")? as usize;
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
        self.set_value(slot, "dataval", value)?;
        self.set_int(slot, "block", block_num as i32)?;
        Ok(())
    }

    fn insert_leaf(&self, slot: usize, value: Constant, rid: RID) -> Result<(), Box<dyn Error>> {
        matches!(self.page_type, PageType::Leaf);
        self.insert(slot)?;
        self.set_value(slot, "dataval", value)?;
        self.set_int(slot, "block", rid.block_num as i32)?;
        self.set_int(slot, "id", rid.slot as i32)?;
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
