#![allow(clippy::arc_with_non_send_sync)]

use std::{
    any::Any,
    cell::{Cell, RefCell},
    cmp::Ordering,
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt::Display,
    fs::{self, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read, Seek, Write},
    ops::{Deref, DerefMut},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize},
        Arc, Condvar, Mutex, MutexGuard, OnceLock, RwLock, RwLockReadGuard, RwLockWriteGuard, Weak,
    },
    time::{Duration, Instant},
};
pub mod test_utils;
pub use btree::BTreeIndex;
use parser::{
    CreateIndexData, CreateTableData, CreateViewData, DeleteData, InsertData, ModifyData, Parser,
    QueryData,
};

pub use test_utils::TestDir;

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
use crate::intrusive_dll::IntrusiveNode;
pub mod benchmark_framework;
mod btree;
#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
mod intrusive_dll;
mod page;
mod parser;
mod replacement;

use replacement::PolicyState;

type Lsn = usize;
type SimpleDBResult<T> = Result<T, Box<dyn Error>>;

//  Shared filesystem trait object used across the database components
type SharedFS = Arc<Mutex<Box<dyn FileSystemInterface + Send + 'static>>>;

/// The database struct
pub struct SimpleDB {
    db_directory: PathBuf,
    pub file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<BufferManager>,
    metadata_manager: Arc<MetadataManager>,
    pub planner: Arc<Planner>,
    lock_table: Arc<LockTable>,
}

impl SimpleDB {
    const LOG_FILE: &str = "simpledb.log";

    pub fn new<P: AsRef<Path>>(
        path: P,
        num_buffers: usize,
        clean: bool,
        lock_timeout_ms: u64,
    ) -> Self {
        let file_manager: SharedFS = Arc::new(Mutex::new(Box::new(
            FileManager::new(&path, clean).unwrap(),
        )));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            Self::LOG_FILE,
        )));
        let buffer_manager = Arc::new(BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        ));
        let lock_table = Arc::new(LockTable::new(lock_timeout_ms));
        let txn = Arc::new(Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
            Arc::clone(&lock_table),
        ));
        let metadata_manager = Arc::new(MetadataManager::new(clean, Arc::clone(&txn)));
        let query_planner = BasicQueryPlanner::new(Arc::clone(&metadata_manager));
        let _update_planner = BasicUpdatePlanner::new(Arc::clone(&metadata_manager));
        let index_update_planner = IndexUpdatePlanner::new(Arc::clone(&metadata_manager));
        let planner = Arc::new(Planner::new(
            Box::new(query_planner),
            Box::new(index_update_planner),
        ));
        txn.commit().unwrap();
        Self {
            db_directory: path.as_ref().to_path_buf(),
            log_manager,
            file_manager,
            buffer_manager,
            metadata_manager,
            planner,
            lock_table,
        }
    }

    pub fn new_tx(&self) -> Arc<Transaction> {
        Arc::new(Transaction::new(
            Arc::clone(&self.file_manager),
            Arc::clone(&self.log_manager),
            Arc::clone(&self.buffer_manager),
            Arc::clone(&self.lock_table),
        ))
    }

    pub fn new_for_test(num_buffers: usize, lock_timeout_ms: u64) -> (Self, TestDir) {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let test_dir = TestDir::new(format!("/tmp/test_db_{timestamp}_{thread_id:?}"));
        let db = Self::new(&test_dir, num_buffers, true, lock_timeout_ms);
        (db, test_dir)
    }

    pub fn buffer_manager(&self) -> Arc<BufferManager> {
        Arc::clone(&self.buffer_manager)
    }

    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        Arc::clone(&self.log_manager)
    }

    pub fn db_directory(&self) -> &Path {
        &self.db_directory
    }

    pub fn metadata_manager(&self) -> Arc<MetadataManager> {
        Arc::clone(&self.metadata_manager)
    }
}

pub struct MultiBufferProductPlan {
    lhs: Arc<dyn Plan>,
    rhs: Arc<dyn Plan>,
    txn: Arc<Transaction>,
    schema: Schema,
}

impl MultiBufferProductPlan {
    pub fn new(
        lhs: Arc<dyn Plan>,
        rhs: Arc<dyn Plan>,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Self> {
        let mut schema = Schema::new();
        schema.add_all_from_schema(&lhs.schema())?;
        schema.add_all_from_schema(&rhs.schema())?;
        let lhs = Arc::new(MaterializePlan::new(lhs, Arc::clone(&txn)));
        Ok(Self {
            txn,
            lhs,
            rhs,
            schema,
        })
    }

    pub fn create_temp_table(&self, plan: &Arc<dyn Plan>) -> SimpleDBResult<TempTable> {
        let temp_table = TempTable::new(Arc::clone(&self.txn), plan.schema());
        let mut source_scan = plan.open();
        let mut table_scan = temp_table.open();
        while let Some(result) = source_scan.next() {
            result?;
            table_scan.insert()?;
            for field in plan.schema().fields {
                table_scan.set_value(&field, source_scan.get_value(&field)?)?;
            }
        }
        Ok(temp_table)
    }
}

impl Plan for MultiBufferProductPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let scan_1 = self.lhs.open();
        let table_scan: TableScan = *(scan_1 as Box<dyn Any>)
            .downcast()
            .expect("Failed to downcast to TableScan");
        let scan_2 = self.create_temp_table(&self.rhs).unwrap();
        let scan = MultiBufferProductScan::new(
            Arc::clone(&self.txn),
            table_scan,
            &scan_2.table_name,
            scan_2.layout,
        );
        Box::new(scan)
    }

    fn blocks_accessed(&self) -> usize {
        let available_buffs = self.txn.available_buffs();
        //  TODO: This is copied over from [MaterializePlan::blocks_accessed] because there is no way
        //  to pass ownership to MaterializePlan right now of self.rhs
        let num_blocks = {
            let layout = Layout::new(self.rhs.schema());
            let records_per_block = self.txn.block_size() / layout.slot_size;
            self.rhs.records_output() / records_per_block
        };
        let num_chunks = num_blocks / available_buffs;
        self.rhs.blocks_accessed() + (self.lhs.blocks_accessed() * num_chunks)
    }

    fn records_output(&self) -> usize {
        self.lhs.records_output() * self.rhs.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.lhs.schema().fields.contains(&field_name.to_string()) {
            self.lhs.distinct_values(field_name)
        } else {
            self.rhs.distinct_values(field_name)
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        println!("{}MultiBufferProductPlan", " ".repeat(indent));
        self.lhs.print_plan_internal(indent + 2);
        self.rhs.print_plan_internal(indent + 2);
    }
}

#[cfg(test)]
mod multi_buffer_product_plan_tests {
    use std::sync::Arc;

    use crate::{MultiBufferProductPlan, Plan, SimpleDB, TablePlan, Transaction};

    fn setup_emp_dept(db: &SimpleDB, txn: Arc<Transaction>) {
        db.planner
            .execute_update(
                "create table emp(emp_id int, name varchar(10))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();
        db.planner
            .execute_update(
                "create table dept(dept_id int, dept_name varchar(10))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();
    }

    fn insert_emp(db: &SimpleDB, txn: Arc<Transaction>, n: usize) {
        for i in 0..n {
            db.planner
                .execute_update(
                    format!("insert into emp(emp_id, name) values ({i}, 'emp{i}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }
    }

    fn insert_dept(db: &SimpleDB, txn: Arc<Transaction>, n: usize) {
        for i in 0..n {
            db.planner
                .execute_update(
                    format!("insert into dept(dept_id, dept_name) values ({i}, 'dept{i}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }
    }

    fn build_plan(db: &SimpleDB, txn: Arc<Transaction>) -> MultiBufferProductPlan {
        let lhs = Arc::new(TablePlan::new(
            "emp",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let rhs = Arc::new(TablePlan::new(
            "dept",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        MultiBufferProductPlan::new(lhs, rhs, Arc::clone(&txn)).unwrap()
    }

    #[test]
    fn test_mbp_plan_basic_count() {
        let (db, _td) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        setup_emp_dept(&db, Arc::clone(&txn));
        insert_emp(&db, Arc::clone(&txn), 5);
        insert_dept(&db, Arc::clone(&txn), 30);

        let mbp = build_plan(&db, Arc::clone(&txn));
        let scan = mbp.open();

        let mut count = 0;
        for res in scan {
            res.unwrap();
            count += 1;
        }
        assert_eq!(count, 150, "5 x 30 combinations expected");
    }

    #[test]
    fn test_mbp_plan_empty_tables() {
        let (db, _td) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        setup_emp_dept(&db, Arc::clone(&txn));
        // no inserts

        let mbp = build_plan(&db, Arc::clone(&txn));
        let scan = mbp.open();

        let mut count = 0;
        for res in scan {
            res.unwrap();
            count += 1;
        }
        assert_eq!(count, 0, "No combinations for empty tables");
    }

    #[test]
    fn test_mbp_plan_before_first() {
        let (db, _td) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        setup_emp_dept(&db, Arc::clone(&txn));
        insert_emp(&db, Arc::clone(&txn), 5);
        insert_dept(&db, Arc::clone(&txn), 30);

        let mbp = build_plan(&db, Arc::clone(&txn));
        let mut scan = mbp.open();

        // consume a few
        scan.next();
        scan.next();

        scan.before_first().unwrap();

        let mut count = 0;
        for res in scan {
            res.unwrap();
            count += 1;
        }
        assert_eq!(count, 150, "Should re-iterate all combinations");
    }

    #[test]
    fn test_mbp_plan_schema_union() {
        let (db, _td) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        setup_emp_dept(&db, Arc::clone(&txn));

        let mbp = build_plan(&db, Arc::clone(&txn));
        let schema = mbp.schema();

        assert!(schema.fields.contains(&"emp_id".to_string()));
        assert!(schema.fields.contains(&"name".to_string()));
        assert!(schema.fields.contains(&"dept_id".to_string()));
        assert!(schema.fields.contains(&"dept_name".to_string()));
    }
}

pub struct MultiBufferProductScan<S1>
where
    S1: Scan + Clone,
{
    txn: Arc<Transaction>,
    s1: S1,
    s2: Option<ChunkScan>,
    product_scan: Option<ProductScan<S1, ChunkScan>>,
    chunk_size: usize,
    table_name: String,
    file_name: String,
    layout: Layout,
    next_start_block_num: usize,
}

impl<S1> MultiBufferProductScan<S1>
where
    S1: Scan + Clone,
{
    pub fn new(txn: Arc<Transaction>, s1: S1, table_name: &str, layout: Layout) -> Self {
        debug!("Creating multi buffer product scan for {}.tbl", table_name);
        let file_name = format!("{table_name}.tbl");
        let available_buffers = txn.available_buffs();
        let rhs_file_size = txn.size(&file_name);
        let chunk_size = best_factor(available_buffers, rhs_file_size);
        debug!(
            "The chunk size for the multi buffer product plan is {}",
            chunk_size
        );

        let mut scan = MultiBufferProductScan {
            txn,
            s1,
            s2: None,
            product_scan: None,
            chunk_size,
            table_name: table_name.to_string(),
            file_name: file_name.to_string(),
            layout: layout.clone(),
            next_start_block_num: 0,
        };
        scan.before_first().unwrap();
        scan.load_next_set_of_chunks();
        scan
    }

    pub fn load_next_set_of_chunks(&mut self) -> bool {
        if self.next_start_block_num >= self.txn.size(&self.file_name) {
            return false;
        }
        let new_last_block_num = std::cmp::min(
            self.next_start_block_num + self.chunk_size - 1,
            self.txn.size(&self.file_name) - 1,
        );

        //  Drop all old values to make room in the buffer pool
        self.product_scan.take();
        self.s2.take();

        self.s1.before_first().unwrap();
        let chunk_scan = ChunkScan::new(
            Arc::clone(&self.txn),
            self.layout.clone(),
            &self.table_name,
            self.next_start_block_num,
            new_last_block_num,
        );
        self.s2 = Some(chunk_scan);
        self.product_scan = Some(ProductScan::new(
            self.s1.clone(),
            self.s2.as_ref().unwrap().clone(),
        ));
        self.next_start_block_num = new_last_block_num + 1;
        true
    }
}

impl<S1> Iterator for MultiBufferProductScan<S1>
where
    S1: Scan + Clone,
{
    type Item = SimpleDBResult<()>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on MultiBufferProductScan");
        loop {
            if let Some(prod_scan) = self.product_scan.as_mut() {
                match prod_scan.next() {
                    Some(result) => match result {
                        Ok(_) => return Some(Ok(())),
                        Err(e) => return Some(Err(e)),
                    },
                    None => {
                        if !self.load_next_set_of_chunks() {
                            return None;
                        }
                    }
                }
            } else {
                return None;
            }
        }
    }
}

impl<S1> Scan for MultiBufferProductScan<S1>
where
    S1: Scan + Clone,
{
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.next_start_block_num = 0;
        self.load_next_set_of_chunks();
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        self.product_scan.as_ref().unwrap().get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        self.product_scan.as_ref().unwrap().get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        self.product_scan.as_ref().unwrap().get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        self.product_scan.as_ref().unwrap().has_field(field_name)
    }
}

impl<S1> UpdateScan for MultiBufferProductScan<S1>
where
    S1: UpdateScan + Clone + 'static,
{
    fn set_int(&self, _field_name: &str, _value: i32) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_string(&self, _field_name: &str, _value: String) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_value(&self, _field_name: &str, _value: Constant) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        unimplemented!()
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod multi_buffer_product_scan_tests {
    use super::*;

    fn create_test_tables(db: &SimpleDB, txn: Arc<Transaction>) -> (Layout, Layout) {
        // Create schema for first table (employees)
        let mut schema1 = Schema::new();
        schema1.add_int_field("emp_id");
        schema1.add_string_field("name", 10);
        let layout1 = Layout::new(schema1.clone());
        db.metadata_manager
            .create_table("emp", schema1, Arc::clone(&txn));

        // Create schema for second table (departments)
        let mut schema2 = Schema::new();
        schema2.add_int_field("dept_id");
        schema2.add_string_field("dept_name", 10);
        let layout2 = Layout::new(schema2.clone());
        db.metadata_manager
            .create_table("dept", schema2, Arc::clone(&txn));

        (layout1, layout2)
    }

    fn insert_test_records(
        emp_scan: &mut TableScan,
        emp_size: usize,
        dept_scan: &mut TableScan,
        dept_size: usize,
    ) -> Result<(), Box<dyn Error>> {
        // Insert employee records
        for i in 0..emp_size {
            emp_scan.insert()?;
            emp_scan.set_int("emp_id", i as i32)?;
            emp_scan.set_string("name", format!("emp{i}"))?;
        }

        // Insert department records
        for i in 0..dept_size {
            dept_scan.insert()?;
            dept_scan.set_int("dept_id", i as i32)?;
            dept_scan.set_string("dept_name", format!("dept{i}"))?;
        }

        Ok(())
    }

    #[test]
    fn test_multi_buffer_product_basic() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let (emp_layout, dept_layout) = create_test_tables(&db, Arc::clone(&txn));

        // Insert test records
        let mut emp_scan = TableScan::new(Arc::clone(&txn), emp_layout.clone(), "emp");
        let mut dept_scan = TableScan::new(Arc::clone(&txn), dept_layout.clone(), "dept");
        insert_test_records(&mut emp_scan, 5, &mut dept_scan, 30)?;
        drop(emp_scan);
        drop(dept_scan);

        // Create MultiBufferProductScan
        let emp_scan = TableScan::new(Arc::clone(&txn), emp_layout, "emp");
        let mbp_scan = MultiBufferProductScan::new(Arc::clone(&txn), emp_scan, "dept", dept_layout);

        // Count total combinations (should be 500 * 3000 = 1,500,000)
        let mut count = 0;
        for result in mbp_scan {
            result?;
            count += 1;
        }

        assert_eq!(
            count, 150,
            "Should produce 150 combinations (5 employees * 30 departments)"
        );
        Ok(())
    }

    #[test]
    fn test_multi_buffer_product_empty_tables() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let (emp_layout, dept_layout) = create_test_tables(&db, Arc::clone(&txn));

        // Create empty scans
        let emp_scan = TableScan::new(Arc::clone(&txn), emp_layout, "emp");
        let mbp_scan = MultiBufferProductScan::new(Arc::clone(&txn), emp_scan, "dept", dept_layout);

        let mut count = 0;
        for result in mbp_scan {
            result?;
            count += 1;
        }

        assert_eq!(count, 0, "Should produce no combinations with empty tables");
        Ok(())
    }

    #[test]
    fn test_multi_buffer_product_field_access() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let (emp_layout, dept_layout) = create_test_tables(&db, Arc::clone(&txn));

        // Insert test records
        let mut emp_scan = TableScan::new(Arc::clone(&txn), emp_layout.clone(), "emp");
        let mut dept_scan = TableScan::new(Arc::clone(&txn), dept_layout.clone(), "dept");
        insert_test_records(&mut emp_scan, 5, &mut dept_scan, 30)?;
        drop(emp_scan);
        drop(dept_scan);

        // Create MultiBufferProductScan
        let emp_scan = TableScan::new(Arc::clone(&txn), emp_layout, "emp");
        let mut mbp_scan =
            MultiBufferProductScan::new(Arc::clone(&txn), emp_scan, "dept", dept_layout);

        // Test first combination
        if let Some(result) = mbp_scan.next() {
            result?;
            let emp_id = mbp_scan.get_int("emp_id")?;
            let name = mbp_scan.get_string("name")?;
            let dept_id = mbp_scan.get_int("dept_id")?;
            let dept_name = mbp_scan.get_string("dept_name")?;

            assert_eq!(emp_id, 0, "First employee ID should be 0");
            assert_eq!(name, "emp0", "First employee name should be emp0");
            assert_eq!(dept_id, 0, "First department ID should be 0");
            assert_eq!(dept_name, "dept0", "First department name should be dept0");
        }

        Ok(())
    }

    #[test]
    fn test_multi_buffer_product_before_first() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let (emp_layout, dept_layout) = create_test_tables(&db, Arc::clone(&txn));

        // Insert test records
        let mut emp_scan = TableScan::new(Arc::clone(&txn), emp_layout.clone(), "emp");
        let mut dept_scan = TableScan::new(Arc::clone(&txn), dept_layout.clone(), "dept");
        insert_test_records(&mut emp_scan, 5, &mut dept_scan, 30)?;
        drop(emp_scan);
        drop(dept_scan);

        // Create MultiBufferProductScan
        let emp_scan = TableScan::new(Arc::clone(&txn), emp_layout, "emp");
        let mut mbp_scan =
            MultiBufferProductScan::new(Arc::clone(&txn), emp_scan, "dept", dept_layout);

        // Read some records
        mbp_scan.next();
        mbp_scan.next();

        // Reset to beginning
        mbp_scan.before_first()?;

        // Count all combinations again
        let mut count = 0;
        for result in mbp_scan {
            result?;
            count += 1;
        }

        assert_eq!(
            count, 150,
            "Should read all combinations after before_first"
        );
        Ok(())
    }
}

#[derive(Clone)]
pub struct ChunkScan {
    txn: Arc<Transaction>,
    layout: Layout,
    file_name: String,
    table_name: String,
    first_block_num: usize,
    last_block_num: usize,
    current_block_num: usize,
    current_record_page: Option<usize>,
    current_slot: Option<usize>,
    buffer_list: Vec<RecordPage>,
}

impl ChunkScan {
    pub fn txn(&self) -> &Arc<Transaction> {
        &self.txn
    }

    pub fn file_name(&self) -> &str {
        &self.file_name
    }

    pub fn last_block_num(&self) -> usize {
        self.last_block_num
    }

    pub fn current_block_num(&self) -> usize {
        self.current_block_num
    }

    pub fn new(
        txn: Arc<Transaction>,
        layout: Layout,
        table_name: &str,
        first_block_num: usize,
        last_block_num: usize,
    ) -> Self {
        assert!(
            first_block_num <= last_block_num,
            "{first_block_num} is not less than or equal to {last_block_num}"
        );
        debug!(
            "Creating chunk scan for {}.tbl for blocks from {} to {}",
            table_name, first_block_num, last_block_num
        );
        let file_name = format!("{table_name}.tbl");
        let mut buffer_list = Vec::new();
        for block_num in first_block_num..=last_block_num {
            let block_id = BlockId::new(file_name.to_string(), block_num);
            let record_page = RecordPage::new(Arc::clone(&txn), block_id, layout.clone());
            buffer_list.push(record_page);
        }

        let mut scan = Self {
            txn,
            layout,
            file_name: file_name.to_string(),
            table_name: table_name.to_string(),
            first_block_num,
            last_block_num,
            current_block_num: first_block_num,
            current_record_page: None,
            current_slot: None,
            buffer_list,
        };
        scan.move_to_block(first_block_num);
        scan
    }

    pub fn move_to_block(&mut self, block_num: usize) {
        let offset = block_num - self.first_block_num;
        debug!(
            "Moving chunk scan to block {} which is offset {}",
            block_num, offset
        );
        self.current_record_page = Some(offset);
        self.current_slot = None;
    }
}
impl Scan for ChunkScan {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.move_to_block(self.first_block_num);
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        let record_page = &self.buffer_list[self.current_record_page.ok_or_else(|| {
            format!(
                "No record page number in ChunkScan set when calling get_int for {}",
                self.table_name
            )
        })?];
        Ok(self
            .current_slot
            .ok_or_else(|| {
                format!(
                    "No current slot set in ChunkScan when calling get_int for {}",
                    self.table_name
                )
            })
            .map(|slot| record_page.get_int(slot, field_name))?)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        let record_page = &self.buffer_list[self.current_record_page.ok_or_else(|| {
            format!(
                "No record page number set in ChunkScan when calling get_string for {}",
                self.table_name
            )
        })?];
        Ok(self
            .current_slot
            .ok_or_else(|| {
                format!(
                    "No current slot set in ChunkScan when calling get_string for {}",
                    self.table_name
                )
            })
            .map(|slot| record_page.get_string(slot, field_name))?)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        match self.layout.schema.info.get(field_name).unwrap().field_type {
            FieldType::Int => Ok(Constant::Int(self.get_int(field_name)?)),
            FieldType::String => Ok(Constant::String(self.get_string(field_name)?)),
        }
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self.layout.schema.fields.contains(&field_name.to_string()))
    }
}

impl Iterator for ChunkScan {
    type Item = SimpleDBResult<()>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on ChunkScan for {}", self.table_name);
        assert!(!self.buffer_list.is_empty());
        loop {
            if let Some(record_page_idx) = &self.current_record_page {
                let record_page = &self.buffer_list[*record_page_idx];
                let next_slot = match self.current_slot {
                    None => record_page.iter_used_slots().next(),
                    Some(slot) => record_page.iter_used_slots().find(|s| *s > slot),
                };

                //  There are still slots to iterate in the current record page
                if let Some(slot) = next_slot {
                    self.current_slot = Some(slot);
                    return Some(Ok(()));
                }

                //  There are no more slots in the current record page. Check if there are more record pages
                if *record_page_idx < self.buffer_list.len() - 1 {
                    self.current_record_page = Some(*record_page_idx + 1);
                    self.current_slot = None;
                    continue;
                }
            }

            //  There are no more record pages left
            return None;
        }
    }
}

impl UpdateScan for ChunkScan {
    fn set_int(&self, _field_name: &str, _value: i32) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_string(&self, _field_name: &str, _value: String) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_value(&self, _field_name: &str, _value: Constant) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        unimplemented!()
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }
}

#[cfg(test)]
mod chunk_scan_tests {
    use super::*;

    fn create_test_table(db: &SimpleDB, txn: Arc<Transaction>) -> Layout {
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 10);

        let layout = Layout::new(schema.clone());
        db.metadata_manager
            .create_table("test_table", schema, Arc::clone(&txn));
        layout
    }

    fn insert_test_records(table_scan: &mut TableScan, count: usize) -> Result<(), Box<dyn Error>> {
        for i in 0..count {
            table_scan.insert()?;
            table_scan.set_int("id", i as i32)?;
            table_scan.set_string("name", format!("name{i}"))?;
        }
        Ok(())
    }

    #[test]
    fn test_chunk_scan_basic() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let layout = create_test_table(&db, Arc::clone(&txn));

        // Insert some test records using TableScan
        let mut table_scan = TableScan::new(Arc::clone(&txn), layout.clone(), "test_table");
        insert_test_records(&mut table_scan, 10)?;
        drop(table_scan);

        // Test ChunkScan over all blocks
        let mut chunk_scan = ChunkScan::new(
            Arc::clone(&txn),
            layout.clone(),
            "test_table",
            0,
            2, // Assuming records spread across first 3 blocks
        );

        let mut count = 0;
        let mut last_id = -1;

        while let Some(result) = chunk_scan.next() {
            result?;
            let id = chunk_scan.get_int("id")?;
            let name = chunk_scan.get_string("name")?;

            assert!(id > last_id, "Records should be read in order");
            assert_eq!(name, format!("name{id}"));

            last_id = id;
            count += 1;
        }

        assert_eq!(count, 10, "Should have read all 10 records");
        Ok(())
    }

    #[test]
    fn test_chunk_scan_basic_multiple_blocks() -> SimpleDBResult<()> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let layout = create_test_table(&db, Arc::clone(&txn));

        // Insert some test records using TableScan
        let mut table_scan = TableScan::new(Arc::clone(&txn), layout.clone(), "test_table");
        insert_test_records(&mut table_scan, 100)?;
        drop(table_scan);

        // Test ChunkScan over all blocks
        let mut chunk_scan = ChunkScan::new(
            Arc::clone(&txn),
            layout.clone(),
            "test_table",
            0,
            6, // Each block holds 18 records, 100 / 18 ≈ 6
        );

        let mut count = 0;
        let mut last_id = -1;

        while let Some(result) = chunk_scan.next() {
            result?;
            let id = chunk_scan.get_int("id")?;
            let name = chunk_scan.get_string("name")?;

            assert!(id > last_id, "Records should be read in order");
            assert_eq!(name, format!("name{id}"));

            last_id = id;
            count += 1;
        }

        assert_eq!(count, 100, "Should have read all 100 records");
        Ok(())
    }

    #[test]
    fn test_chunk_scan_partial_blocks() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let layout = create_test_table(&db, Arc::clone(&txn));

        // Insert test records; 300 ensures multiple 4KB blocks
        {
            let mut table_scan = TableScan::new(Arc::clone(&txn), layout.clone(), "test_table");
            insert_test_records(&mut table_scan, 300)?;
        }

        // Test ChunkScan over middle blocks only
        let mut chunk_scan = ChunkScan::new(
            Arc::clone(&txn),
            layout.clone(),
            "test_table",
            1, // Start from second block
            2, // End at third block
        );

        let mut records = Vec::new();
        while let Some(result) = chunk_scan.next() {
            result?;
            let id = chunk_scan.get_int("id")?;
            records.push(id);
        }

        assert!(
            !records.is_empty(),
            "Should have found some records in middle blocks"
        );
        assert!(
            records.windows(2).all(|w| w[0] < w[1]),
            "Records should be in order"
        );
        Ok(())
    }

    #[test]
    fn test_chunk_scan_empty_blocks() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let layout = create_test_table(&db, Arc::clone(&txn));

        // Create empty table
        {
            TableScan::new(Arc::clone(&txn), layout.clone(), "test_table");
        }

        // Test ChunkScan over empty blocks
        let chunk_scan = ChunkScan::new(Arc::clone(&txn), layout.clone(), "test_table", 0, 1);

        let mut count = 0;
        for result in chunk_scan {
            result?;
            count += 1;
        }

        assert_eq!(count, 0, "Should not find any records in empty blocks");
        Ok(())
    }

    #[test]
    fn test_chunk_scan_before_first() -> Result<(), Box<dyn Error>> {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let layout = create_test_table(&db, Arc::clone(&txn));

        // Insert test records
        let mut table_scan = TableScan::new(Arc::clone(&txn), layout.clone(), "test_table");
        insert_test_records(&mut table_scan, 5)?;
        drop(table_scan);

        // Test before_first functionality
        let mut chunk_scan = ChunkScan::new(Arc::clone(&txn), layout.clone(), "test_table", 0, 1);

        // Read some records
        chunk_scan.next();
        chunk_scan.next();

        // Call before_first
        chunk_scan.before_first()?;

        // Should start from beginning again
        let mut ids = Vec::new();
        while let Some(result) = chunk_scan.next() {
            result?;
            ids.push(chunk_scan.get_int("id")?);
        }
        assert_eq!(ids.len(), 5, "Should read all records after before_first");
        assert_eq!(
            ids,
            (0..5).collect::<Vec<i32>>(),
            "Should read records in order from start"
        );
        Ok(())
    }
}

/// This function finds the best root for doing a multibuffer mergejoin
/// We are trying to find the number of buffers to reserve and how many blocks
/// of the input record to read
/// This is a root because the cost of the merge side of merge join is logarithmic
pub fn best_root(available_buffers: usize, num_of_blocks: usize) -> usize {
    let buffers = available_buffers - 2; //  reserve some buffers
    if buffers <= 1 {
        return buffers;
    }
    let mut k = usize::MAX;
    let mut root = 1;
    while k > buffers {
        root += 1;
        k = num_of_blocks.pow(1 / root);
    }
    k
}

/// This function finds the best factor for doing a multibuffer productjoin
/// We are trying to find the number of buffers to reserve and how many blocks
/// of the input record to read
/// This is a factor because the cost of the productscan is linear
pub fn best_factor(available_buffers: usize, num_of_blocks: usize) -> usize {
    let buffers = available_buffers - 2; // reserve some buffers
    if buffers <= 1 {
        return buffers;
    }
    let mut k = num_of_blocks;
    let mut factor = 1;
    while k > buffers {
        factor += 1;
        k = num_of_blocks / factor;
    }
    k
}

pub struct MergeJoinPlan {
    plan_1: Arc<dyn Plan>,
    plan_2: Arc<dyn Plan>,
    field_name_1: String,
    field_name_2: String,
    txn: Arc<Transaction>,
    schema: Schema,
}

impl MergeJoinPlan {
    pub fn txn(&self) -> &Arc<Transaction> {
        &self.txn
    }

    pub fn new(
        plan_1: Arc<dyn Plan>,
        plan_2: Arc<dyn Plan>,
        txn: Arc<Transaction>,
        field_name_1: String,
        field_name_2: String,
    ) -> Result<Self, Box<dyn Error>> {
        let mut schema = Schema::new();
        schema.add_all_from_schema(&plan_1.schema())?;
        schema.add_all_from_schema(&plan_2.schema())?;
        Ok(Self {
            plan_1,
            plan_2,
            field_name_1,
            field_name_2,
            txn,
            schema,
        })
    }
}

impl Plan for MergeJoinPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let scan_1 = self.plan_1.open();
        let scan_2 = self.plan_2.open();
        let sort_scan_2: SortScan = *(scan_2 as Box<dyn Any>)
            .downcast()
            .expect("Failed to downcast");
        let scan = MergeJoinScan::new(
            scan_1,
            sort_scan_2,
            self.field_name_1.clone(),
            self.field_name_2.clone(),
        );
        Box::new(scan)
    }

    fn blocks_accessed(&self) -> usize {
        let blocks_1 = self.plan_1.blocks_accessed();
        let blocks_2 = self.plan_2.blocks_accessed();
        blocks_1 + blocks_2
    }

    fn records_output(&self) -> usize {
        let max_vals = std::cmp::max(
            self.distinct_values(&self.field_name_1),
            self.distinct_values(&self.field_name_2),
        );
        (self.plan_1.records_output() * self.plan_2.records_output()) / max_vals
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.plan_1.schema().fields.contains(&self.field_name_1) {
            self.plan_1.distinct_values(field_name)
        } else if self.plan_2.schema().fields.contains(&self.field_name_2) {
            self.plan_2.distinct_values(field_name)
        } else {
            0
        }
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        let indent_str = " ".repeat(indent);
        println!("{indent_str}MergeJoinPlan");
        println!("{}  Field 1: {}", indent_str, self.field_name_1);
        println!("{}  Field 2: {}", indent_str, self.field_name_2);
        println!("{indent_str}  Plan 1:");
        self.plan_1.print_plan_internal(indent + 2);
        println!("{indent_str}  Plan 2:");
        self.plan_2.print_plan_internal(indent + 2);
    }
}

#[cfg(test)]
mod merge_join_plan_tests {
    use std::sync::Arc;

    use crate::{MergeJoinPlan, Plan, SimpleDB, SortPlan, TablePlan};

    #[test]
    fn test_merge_join_plan_with_real_tables() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the tables using SQL
        let sql1 = "create table employees(id int, name varchar(20))".to_string();
        db.planner.execute_update(sql1, Arc::clone(&txn)).unwrap();

        let sql2 = "create table departments(depid int, deptname varchar(20))".to_string();
        db.planner.execute_update(sql2, Arc::clone(&txn)).unwrap();

        // Insert test data
        let employees = vec![(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")];
        let departments = vec![(2, "Engineering"), (3, "Sales"), (5, "Marketing")];

        for (id, name) in &employees {
            let sql = format!("insert into employees(id, name) values ({id}, '{name}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        for (id, dept) in &departments {
            let sql = format!("insert into departments(depid, deptname) values ({id}, '{dept}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Create table plans
        let plan1 = Arc::new(TablePlan::new(
            "employees",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));

        let plan2 = Arc::new(TablePlan::new(
            "departments",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));

        // Create sort plans
        let sort_plan1 = Arc::new(SortPlan::new(
            plan1,
            Arc::clone(&txn),
            vec!["id".to_string()],
        ));

        let sort_plan2 = Arc::new(SortPlan::new(
            plan2,
            Arc::clone(&txn),
            vec!["depid".to_string()],
        ));

        // Create merge join plan
        let merge_join_plan = MergeJoinPlan::new(
            sort_plan1,
            sort_plan2,
            Arc::clone(&txn),
            "id".to_string(),
            "depid".to_string(),
        )
        .unwrap();

        // Open the plan and test
        let mut scan = merge_join_plan.open();

        let mut results = Vec::new();
        while let Some(result) = scan.next() {
            assert!(result.is_ok());
            let id = scan.get_int("id").unwrap();
            let name = scan.get_string("name").unwrap();
            let dept = scan.get_string("deptname").unwrap();
            results.push((id, name, dept));
        }

        assert_eq!(results.len(), 2, "Should find 2 matching records");

        // Sort results for consistent comparison
        results.sort_by(|a, b| a.0.cmp(&b.0));

        // Expected matches: Bob-Engineering, Charlie-Sales
        assert_eq!(
            results[0],
            (2, "Bob".to_string(), "Engineering".to_string())
        );
        assert_eq!(results[1], (3, "Charlie".to_string(), "Sales".to_string()));
    }
}

pub enum MergeJoinScanState {
    BeforeFirst,
    SeekMatch,
    InGroup(Constant),
}

pub struct MergeJoinScan<S>
where
    S: Scan,
{
    scan_1: S,
    scan_2: SortScan,
    field_name_1: String,
    field_name_2: String,
    scan_state: MergeJoinScanState,
    at_new_group: bool,
}

impl<S> MergeJoinScan<S>
where
    S: Scan,
{
    pub fn at_new_group(&self) -> bool {
        self.at_new_group
    }

    pub fn new(scan_1: S, scan_2: SortScan, field_name_1: String, field_name_2: String) -> Self {
        Self {
            scan_1,
            scan_2,
            field_name_1,
            field_name_2,
            scan_state: MergeJoinScanState::BeforeFirst,
            at_new_group: false,
        }
    }
}

impl<S> Iterator for MergeJoinScan<S>
where
    S: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match &self.scan_state {
                MergeJoinScanState::BeforeFirst => match (self.scan_1.next(), self.scan_2.next()) {
                    (None, None) | (None, Some(Ok(_))) | (Some(Ok(_)), None) => return None,
                    (None, Some(Err(_e))) | (Some(Err(_e)), None) => return None,
                    (Some(Ok(_)), Some(Err(e))) | (Some(Err(e)), Some(Ok(_))) => {
                        return Some(Err(e))
                    }
                    (Some(Err(e1)), Some(Err(_e2))) => return Some(Err(e1)),
                    (Some(Ok(_)), Some(Ok(_))) => {
                        self.scan_state = MergeJoinScanState::SeekMatch;
                        continue;
                    }
                },
                MergeJoinScanState::SeekMatch => {
                    let value_1 = self.scan_1.get_value(&self.field_name_1).unwrap();
                    let value_2 = self.scan_2.get_value(&self.field_name_2).unwrap();
                    match value_1.cmp(&value_2) {
                        Ordering::Less => match self.scan_1.next() {
                            Some(Ok(_)) => {
                                continue;
                            }
                            Some(Err(e)) => return Some(Err(e)),
                            None => return None,
                        },
                        Ordering::Greater => match self.scan_2.next() {
                            Some(Ok(_)) => {
                                continue;
                            }
                            Some(Err(e)) => return Some(Err(e)),
                            None => return None,
                        },
                        Ordering::Equal => {
                            self.scan_2.save_position().unwrap();
                            self.scan_state = MergeJoinScanState::InGroup(value_2);
                            return Some(Ok(()));
                        }
                    }
                }
                MergeJoinScanState::InGroup(join_value) => match self.scan_2.next() {
                    Some(Ok(_)) => {
                        let value_2 = self.scan_2.get_value(&self.field_name_2).unwrap();
                        if value_2 == *join_value {
                            return Some(Ok(()));
                        } else {
                            match self.scan_1.next() {
                                Some(Ok(_)) => {
                                    let value_1 =
                                        self.scan_1.get_value(&self.field_name_1).unwrap();
                                    if value_1 == *join_value {
                                        self.scan_2.restore_position().unwrap();
                                        return Some(Ok(()));
                                    } else {
                                        self.scan_state = MergeJoinScanState::SeekMatch;
                                        continue;
                                    }
                                }
                                Some(Err(e)) => return Some(Err(e)),
                                None => return None,
                            }
                        }
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => return None,
                },
            }
        }
    }
}

impl<S> Scan for MergeJoinScan<S>
where
    S: Scan,
{
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan_1.before_first()?;
        self.scan_2.before_first()?;
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        if self.scan_1.has_field(field_name)? {
            return self.scan_1.get_int(field_name);
        } else if self.scan_2.has_field(field_name)? {
            return self.scan_2.get_int(field_name);
        }
        Err(format!("Field {field_name} not found").into())
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if self.scan_1.has_field(field_name)? {
            return self.scan_1.get_string(field_name);
        } else if self.scan_2.has_field(field_name)? {
            return self.scan_2.get_string(field_name);
        }
        Err(format!("Field {field_name} not found").into())
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if self.scan_1.has_field(field_name)? {
            return self.scan_1.get_value(field_name);
        } else if self.scan_2.has_field(field_name)? {
            return self.scan_2.get_value(field_name);
        }
        Err(format!("Field {field_name} not found").into())
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        if self.scan_1.has_field(field_name)? || self.scan_2.has_field(field_name)? {
            return Ok(true);
        }
        Err(format!("Field {field_name} not found").into())
    }
}

impl<S> UpdateScan for MergeJoinScan<S>
where
    S: Scan + 'static,
{
    fn set_int(&self, _field_name: &str, _value: i32) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_string(&self, _field_name: &str, _value: String) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn set_value(&self, _field_name: &str, _value: Constant) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        unimplemented!()
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }
}
#[cfg(test)]
mod merge_join_scan_tests {

    use std::sync::Arc;

    use crate::{
        Layout, MergeJoinScan, RecordComparator, Scan, Schema, SimpleDB, SortScan, TempTable,
        UpdateScan,
    };

    #[test]
    fn test_basic_merge_join() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        schema1.add_string_field("name", 10);
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        schema2.add_string_field("dept", 10);
        let layout2 = Layout::new(schema2);

        // Create temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert sorted test data into first table
        {
            let mut scan = temp_table1.open();
            for i in [1, 2, 3, 5, 7] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        // Insert sorted test data into second table
        {
            let mut scan = temp_table2.open();
            for i in [2, 3, 5, 7, 9] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("dept", format!("dept{i}")).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let mut merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join
        let mut join_count = 0;
        let expected_ids = vec![2, 3, 5, 7];
        let mut matched_ids = Vec::new();

        while let Some(result) = merge_join_scan.next() {
            assert!(result.is_ok(), "Join should succeed");
            let id1 = merge_join_scan.get_int("id").unwrap();
            let name = merge_join_scan.get_string("name").unwrap();
            let dept = merge_join_scan.get_string("dept").unwrap();

            assert_eq!(format!("name{id1}"), name);
            assert_eq!(format!("dept{id1}"), dept);

            matched_ids.push(id1);
            join_count += 1;
        }

        assert_eq!(join_count, 4, "Should find all matching records");
        assert_eq!(matched_ids, expected_ids, "Should match expected IDs");
    }

    #[test]
    fn test_merge_join_no_matches() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        schema1.add_string_field("name", 10);
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        schema2.add_string_field("dept", 10);
        let layout2 = Layout::new(schema2);

        // Create temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert non-overlapping data
        {
            let mut scan = temp_table1.open();
            for i in [1, 3, 5, 7, 9] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        {
            let mut scan = temp_table2.open();
            for i in [2, 4, 6, 8, 10] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("dept", format!("dept{i}")).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join - should find no matches
        let mut join_count = 0;
        for result in merge_join_scan {
            assert!(result.is_ok());
            join_count += 1;
        }

        assert_eq!(join_count, 0, "Should find no matching records");
    }

    #[test]
    fn test_merge_join_duplicate_values() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        schema1.add_string_field("name", 10);
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        schema2.add_string_field("dept", 10);
        let layout2 = Layout::new(schema2);

        // Create temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert data with duplicates
        {
            let mut scan = temp_table1.open();
            // Insert id=5 twice
            for i in [1, 3, 5, 5, 7] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        {
            let mut scan = temp_table2.open();
            // Insert id=5 three times
            for i in [2, 5, 5, 5, 8] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("dept", format!("dept{i}")).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let mut merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join - should find 2*3=6 matches for id=5
        let mut join_count = 0;
        while let Some(result) = merge_join_scan.next() {
            assert!(result.is_ok());
            let id = merge_join_scan.get_int("id").unwrap();
            assert_eq!(id, 5, "Only id=5 should match");
            join_count += 1;
        }

        assert_eq!(join_count, 6, "Should find 2*3=6 matching records for id=5");
    }

    #[test]
    fn test_merge_join_empty_tables() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        let layout2 = Layout::new(schema2);

        // Create empty temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join - should find no matches
        let mut join_count = 0;
        for _ in merge_join_scan {
            join_count += 1;
        }

        assert_eq!(join_count, 0, "Should find no matches with empty tables");
    }

    #[test]
    fn test_merge_join_one_empty_table() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        let layout2 = Layout::new(schema2);

        // Create temp tables - one empty, one with data
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert data into only the second table
        {
            let mut scan = temp_table2.open();
            for i in 1..5 {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join - should find no matches
        let mut join_count = 0;
        for _ in merge_join_scan {
            join_count += 1;
        }

        assert_eq!(join_count, 0, "Should find no matches with one empty table");
    }

    #[test]
    fn test_merge_join_single_record_match() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        schema1.add_string_field("name", 10);
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        schema2.add_string_field("dept", 10);
        let layout2 = Layout::new(schema2);

        // Create temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert data with just one matching record
        {
            let mut scan = temp_table1.open();
            for i in [1, 3, 5, 7] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        {
            let mut scan = temp_table2.open();
            for i in [5, 8, 10] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("dept", format!("dept{i}")).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let mut merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // Test the join - should find exactly one match
        let mut join_count = 0;
        while let Some(result) = merge_join_scan.next() {
            assert!(result.is_ok());
            let id = merge_join_scan.get_int("id").unwrap();
            let name = merge_join_scan.get_string("name").unwrap();
            let dept = merge_join_scan.get_string("dept").unwrap();

            assert_eq!(id, 5);
            assert_eq!(name, "name5");
            assert_eq!(dept, "dept5");

            join_count += 1;
        }

        assert_eq!(join_count, 1, "Should find exactly one matching record");
    }

    #[test]
    fn test_merge_join_before_first() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("id");
        let layout1 = Layout::new(schema1);

        let mut schema2 = Schema::new();
        schema2.add_int_field("id");
        let layout2 = Layout::new(schema2);

        // Create temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout1.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout2.schema.clone());

        // Insert matching data
        {
            let mut scan = temp_table1.open();
            for i in 1..4 {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
            }
        }

        {
            let mut scan = temp_table2.open();
            for i in 1..4 {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
            }
        }

        // Create SortScans
        let record_comparator1 = RecordComparator::new(vec!["id".to_string()]);
        let record_comparator2 = RecordComparator::new(vec!["id".to_string()]);
        let sort_scan1 = SortScan::new(vec![temp_table1], record_comparator1);
        let sort_scan2 = SortScan::new(vec![temp_table2], record_comparator2);

        // Create MergeJoinScan
        let mut merge_join_scan =
            MergeJoinScan::new(sort_scan1, sort_scan2, "id".to_string(), "id".to_string());

        // First read all records
        let mut first_pass_count = 0;
        for _ in merge_join_scan.by_ref() {
            first_pass_count += 1;
        }

        assert_eq!(first_pass_count, 3, "Should find 3 matches in first pass");

        // Reset and read again
        merge_join_scan.before_first().unwrap();

        // Second pass should get the same results
        let mut second_pass_count = 0;
        for _ in merge_join_scan {
            second_pass_count += 1;
        }

        assert_eq!(second_pass_count, 3, "Should find 3 matches after reset");
        assert_eq!(
            first_pass_count, second_pass_count,
            "Both passes should return the same number of records"
        );
    }
}

pub struct SortPlan {
    source_plan: Arc<dyn Plan>,
    txn: Arc<Transaction>,
    schema: Schema,
    record_comparator: RecordComparator,
}

impl SortPlan {
    pub fn new(source_plan: Arc<dyn Plan>, txn: Arc<Transaction>, field_list: Vec<String>) -> Self {
        let schema = source_plan.schema();
        let record_comparator = RecordComparator::new(field_list);
        Self {
            source_plan,
            txn,
            schema,
            record_comparator,
        }
    }

    fn copy<Source, Dest>(
        &self,
        source: &Source,
        destination: &mut Dest,
    ) -> Result<(), Box<dyn Error>>
    where
        Source: Scan,
        Dest: UpdateScan,
    {
        destination.insert()?;
        for field in &self.schema.fields {
            let value = source.get_value(field)?;
            destination.set_value(field, value)?;
        }
        Ok(())
    }

    pub fn split_into_runs(
        &self,
        mut source_scan: Box<dyn UpdateScan>,
    ) -> Result<Vec<TempTable>, Box<dyn Error>> {
        let mut temp_tables: Vec<TempTable> = Vec::new();
        source_scan.before_first()?;
        let current_temp_table = TempTable::new(Arc::clone(&self.txn), self.source_plan.schema());
        let mut current_scan = current_temp_table.open();
        temp_tables.push(current_temp_table);

        //  Copy over first record as is
        match source_scan.next() {
            Some(Ok(_)) => self.copy(&source_scan, &mut current_scan)?,
            Some(Err(e)) => return Err(e),
            None => {
                return Ok(temp_tables);
            }
        };

        //  Loop over the current scan and keep adding records
        //  Split into a new temp table when the invariant is brokern
        loop {
            match source_scan.next() {
                Some(Ok(_)) => {
                    match self.record_comparator.compare(&current_scan, &source_scan) {
                        Ok(ordering) => match ordering {
                            Ordering::Greater => {
                                let new_temp_table = TempTable::new(
                                    Arc::clone(&self.txn),
                                    self.source_plan.schema(),
                                );
                                current_scan = new_temp_table.open();
                                temp_tables.push(new_temp_table);
                                self.copy(&source_scan, &mut current_scan)?;
                            }
                            Ordering::Equal | Ordering::Less => {
                                self.copy(&source_scan, &mut current_scan)?;
                            }
                        },
                        Err(_) => return Err("Error comparing records".into()),
                    };
                }
                Some(Err(e)) => return Err(e),
                None => {
                    break;
                }
            };
        }
        Ok(temp_tables)
    }

    pub fn do_merge_iters(
        &self,
        mut temp_tables: Vec<TempTable>,
    ) -> Result<Vec<TempTable>, Box<dyn Error>> {
        if temp_tables.len() <= 2 {
            return Ok(temp_tables);
        }
        while temp_tables.len() > 2 {
            let temp_table_1 = temp_tables.remove(0);
            let temp_table_2 = temp_tables.remove(0);
            let sorted_temp_table = self.merge(temp_table_1, temp_table_2)?;
            temp_tables.push(sorted_temp_table);
        }
        Ok(temp_tables)
    }

    pub fn merge(
        &self,
        table_1: TempTable,
        table_2: TempTable,
    ) -> Result<TempTable, Box<dyn Error>> {
        let mut scan_1 = Some(table_1.open());
        let mut scan_2 = Some(table_2.open());
        let temp_table = TempTable::new(Arc::clone(&self.txn), self.source_plan.schema());
        let mut current_scan = temp_table.open();

        enum MergeState {
            DoCompare, //  compare the two scan values at this point
            First,     //  copy over value from scan_1 and call next() on it
            Second,    //  copy over value from scan_2 and call next() on it
            Done,      //  break out of loop
        }

        let mut merge_state = MergeState::DoCompare;

        //  Do the initial next() call and handle situations where either scan is empty
        if let Some(inner_scan_1) = scan_1.as_mut() {
            match inner_scan_1.next() {
                Some(Ok(_)) => (),
                Some(Err(e)) => {
                    return Err(e);
                }
                None => {
                    scan_1 = None;
                    merge_state = MergeState::Done;
                }
            }
        }
        if let Some(inner_scan_2) = scan_2.as_mut() {
            match inner_scan_2.next() {
                Some(Ok(_)) => (),
                Some(Err(e)) => {
                    return Err(e);
                }
                None => {
                    scan_2 = None;
                    merge_state = MergeState::Done;
                }
            }
        }

        loop {
            match merge_state {
                MergeState::DoCompare => {
                    if let (Some(inner_scan_1), Some(inner_scan_2)) =
                        (scan_1.as_mut(), scan_2.as_mut())
                    {
                        match self.record_comparator.compare(inner_scan_1, inner_scan_2) {
                            Ok(ordering) => match ordering {
                                Ordering::Less => {
                                    merge_state = MergeState::First;
                                }
                                Ordering::Equal => {
                                    merge_state = MergeState::First;
                                }
                                Ordering::Greater => {
                                    merge_state = MergeState::Second;
                                }
                            },
                            Err(e) => return Err(e),
                        };
                    }
                }
                MergeState::First => {
                    let Some(inner_scan_1) = scan_1.as_mut() else {
                        return Err("Scan 1 is None during MergeState::First".into());
                    };
                    self.copy(inner_scan_1, &mut current_scan)?;
                    match inner_scan_1.next() {
                        Some(Ok(_)) => {
                            merge_state = MergeState::DoCompare;
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                        None => {
                            scan_1 = None;
                            merge_state = MergeState::Done;
                        }
                    }
                }
                MergeState::Second => {
                    let Some(inner_scan_2) = scan_2.as_mut() else {
                        return Err("Scan 2 is None during MergeState::Second".into());
                    };
                    self.copy(inner_scan_2, &mut current_scan)?;
                    match inner_scan_2.next() {
                        Some(Ok(_)) => {
                            merge_state = MergeState::DoCompare;
                            continue;
                        }
                        Some(Err(e)) => {
                            return Err(e);
                        }
                        None => {
                            scan_2 = None;
                            merge_state = MergeState::Done;
                        }
                    }
                }
                MergeState::Done => {
                    break;
                }
            }
        }

        //  Either one of the scans is still valid or both are invalid
        if let Some(inner_scan_1) = scan_1.as_mut() {
            self.copy(inner_scan_1, &mut current_scan)?;
            loop {
                match inner_scan_1.next() {
                    Some(Ok(_)) => {
                        self.copy(inner_scan_1, &mut current_scan)?;
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
        }

        if let Some(inner_scan_2) = scan_2.as_mut() {
            self.copy(inner_scan_2, &mut current_scan)?;
            loop {
                match inner_scan_2.next() {
                    Some(Ok(_)) => {
                        self.copy(inner_scan_2, &mut current_scan)?;
                    }
                    Some(Err(e)) => return Err(e),
                    None => break,
                }
            }
        }

        //  Close the scans
        Ok(temp_table)
    }
}

impl Plan for SortPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let source_scan = self.source_plan.open();
        let runs = self.split_into_runs(source_scan).unwrap();
        let merged_runs = self.do_merge_iters(runs).unwrap();
        Box::new(SortScan::new(merged_runs, self.record_comparator.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        //  TODO: This is incorrect, it should be using MaterializePlan::blocks_accessed()
        //  however, that requires clone on the Plan trait
        // let materialize_plan =
        //     MaterializePlan::new((*self.source_plan).clone(), Arc::clone(&self.txn));
        // materialize_plan.blocks_accessed()
        self.source_plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.source_plan.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.source_plan.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.source_plan.schema()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ SortPlan");
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!(
            "{}├─ Schema: {:?}",
            prefix,
            self.source_plan.schema().fields
        );
        println!("{prefix}├─ Source Plan:");
        self.source_plan.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

#[cfg(test)]
mod sort_plan_tests {
    use crate::{Plan, SimpleDB, SortPlan, TablePlan};
    use std::sync::Arc;

    #[test]
    fn test_basic_sort() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the table using SQL
        let sql = "create table numbers(id int, value int)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert unsorted test data
        let test_data = vec![(5, 50), (3, 30), (1, 10), (4, 40), (2, 20)];

        for (id, value) in &test_data {
            let sql = format!("insert into numbers(id, value) values ({id}, {value})");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Create source plan
        let table_plan = Arc::new(TablePlan::new(
            "numbers",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));

        // Create sort plan sorting by id
        let sort_plan = SortPlan::new(table_plan, Arc::clone(&txn), vec!["id".to_string()]);

        // Open the sort scan
        let mut sort_scan = sort_plan.open();

        // Verify records come back in sorted order
        let mut prev_id = None;
        let mut count = 0;

        while let Some(result) = sort_scan.next() {
            assert!(result.is_ok());
            let curr_id = sort_scan.get_int("id").unwrap();

            if let Some(prev) = prev_id {
                assert!(curr_id > prev, "Records should be in ascending order");
            }

            count += 1;
            prev_id = Some(curr_id);
        }

        assert_eq!(count, test_data.len(), "Should have retrieved all records");
    }

    #[test]
    fn test_sort_with_duplicates() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the table
        let sql = "create table students_sort(grade int, name varchar(20))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert test data with duplicate grades
        let test_data = vec![
            (85, "Alice"),
            (90, "Bob"),
            (85, "Charlie"),
            (95, "David"),
            (90, "Eve"),
        ];

        for (grade, name) in &test_data {
            let sql = format!("insert into students_sort(grade, name) values ({grade}, '{name}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Create sort plan sorting by grade
        let table_plan = Arc::new(TablePlan::new(
            "students_sort",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let sort_plan = SortPlan::new(table_plan, Arc::clone(&txn), vec!["grade".to_string()]);

        // Open the sort scan
        let mut sort_scan = sort_plan.open();

        // Verify records come back in sorted order
        let mut prev_grade = None;
        let mut count = 0;
        let mut grade_counts = std::collections::HashMap::new();

        while let Some(result) = sort_scan.next() {
            assert!(result.is_ok());
            let curr_grade = sort_scan.get_int("grade").unwrap();

            if let Some(prev) = prev_grade {
                assert!(curr_grade >= prev, "Records should be in ascending order");
            }

            *grade_counts.entry(curr_grade).or_insert(0) += 1;
            count += 1;
            prev_grade = Some(curr_grade);
        }

        assert_eq!(count, test_data.len(), "Should have retrieved all records");
        assert_eq!(
            *grade_counts.get(&85).unwrap(),
            2,
            "Should have 2 records with grade 85"
        );
        assert_eq!(
            *grade_counts.get(&90).unwrap(),
            2,
            "Should have 2 records with grade 90"
        );
    }

    #[test]
    fn test_multi_field_sort() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the table
        let sql = "create table employees(dept int, salary int, name varchar(20))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert test data
        let test_data = vec![
            (1, 50000, "Alice"),
            (2, 60000, "Bob"),
            (1, 55000, "Charlie"),
            (2, 55000, "David"),
            (1, 60000, "Eve"),
        ];

        for (dept, salary, name) in &test_data {
            let sql = format!(
                "insert into employees(dept, salary, name) values ({dept}, {salary}, '{name}')"
            );
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Create sort plan sorting by dept and salary
        let table_plan = Arc::new(TablePlan::new(
            "employees",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let sort_plan = SortPlan::new(
            table_plan,
            Arc::clone(&txn),
            vec!["dept".to_string(), "salary".to_string()],
        );

        // Open the sort scan
        let mut sort_scan = sort_plan.open();

        // Verify records come back in sorted order
        let mut prev_dept = None;
        let mut prev_salary = None;
        let mut count = 0;

        while let Some(result) = sort_scan.next() {
            assert!(result.is_ok());
            let curr_dept = sort_scan.get_int("dept").unwrap();
            let curr_salary = sort_scan.get_int("salary").unwrap();

            if let (Some(pd), Some(ps)) = (prev_dept, prev_salary) {
                assert!(
                    curr_dept > pd || (curr_dept == pd && curr_salary >= ps),
                    "Records should be sorted by dept then salary"
                );
            }

            count += 1;
            prev_dept = Some(curr_dept);
            prev_salary = Some(curr_salary);
        }

        assert_eq!(count, test_data.len(), "Should have retrieved all records");
    }

    #[test]
    fn test_sort_empty_table() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create empty table
        let sql = "create table empty_table(id int, value int)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Create sort plan
        let table_plan = Arc::new(TablePlan::new(
            "empty_table",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let sort_plan = SortPlan::new(table_plan, Arc::clone(&txn), vec!["id".to_string()]);

        // Open the sort scan
        let sort_scan = sort_plan.open();

        // Verify no records are returned
        let mut count = 0;
        for result in sort_scan {
            assert!(result.is_ok());
            count += 1;
        }

        assert_eq!(count, 0, "Should have no records in empty table");
    }
}

#[derive(Clone, Copy)]
pub enum SortScanState {
    BeforeFirst,
    OnFirst,
    OnSecond,
    OnlyFirst,
    OnlySecond,
    Done,
}

pub struct SortScan {
    s1: TableScan,
    s2: Option<TableScan>,
    current_scan: SortScanState,
    record_comparator: RecordComparator,
    saved_rids: [Option<RID>; 2],
}

impl SortScan {
    pub fn new(mut runs: Vec<TempTable>, record_comparator: RecordComparator) -> Self {
        assert!(runs.len() <= 2);
        let s1 = runs.remove(0).open();
        let s2 = runs.pop().map(|t| t.open());
        Self {
            s1,
            s2,
            current_scan: SortScanState::BeforeFirst,
            record_comparator,
            saved_rids: [None, None],
        }
    }

    pub fn set_current_scan(&mut self) -> Result<(), Box<dyn Error>> {
        match self
            .record_comparator
            .compare(&self.s1, self.s2.as_ref().unwrap())
        {
            Ok(ordering) => match ordering {
                Ordering::Less => {
                    self.current_scan = SortScanState::OnFirst;
                    Ok(())
                }
                Ordering::Equal => {
                    self.current_scan = SortScanState::OnFirst;
                    Ok(())
                }
                Ordering::Greater => {
                    self.current_scan = SortScanState::OnSecond;
                    Ok(())
                }
            },
            Err(e) => {
                self.current_scan = SortScanState::Done;
                Err(format!("Error in SortScan while comparing records: {e}").into())
            }
        }
    }

    pub fn save_position(&mut self) -> Result<(), Box<dyn Error>> {
        let rid_1 = self.s1.get_rid()?;
        let rid_2 = self.s2.as_ref().map(|s| s.get_rid()).transpose()?;
        self.saved_rids[0] = Some(rid_1);
        self.saved_rids[1] = rid_2;
        Ok(())
    }

    pub fn restore_position(&mut self) -> Result<(), Box<dyn Error>> {
        let rid_1 = self.saved_rids[0]
            .ok_or_else(|| "Error getting saved RID from first scan".to_string())?;
        self.s1.move_to_row_id(rid_1);
        match (self.s2.as_mut(), self.saved_rids[1]) {
            (None, None) => (),
            (None, Some(_)) => return Err("Second scan is not defined in SortScan".into()),
            (Some(_), None) => return Err("Second RID is not defined in SortScan".into()),
            (Some(s2), Some(rid)) => {
                s2.move_to_row_id(rid);
            }
        }
        Ok(())
    }
}

impl Iterator for SortScan {
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.current_scan {
            SortScanState::BeforeFirst => {
                match (self.s1.next(), self.s2.as_mut().and_then(|s| s.next())) {
                    (None, None) => {
                        self.current_scan = SortScanState::Done;
                        None
                    }
                    (Some(Ok(_)), None) => {
                        self.current_scan = SortScanState::OnlyFirst;
                        Some(Ok(()))
                    }
                    (None, Some(Ok(_))) => {
                        self.current_scan = SortScanState::OnlySecond;
                        Some(Ok(()))
                    }
                    (Some(Err(e)), _) | (_, Some(Err(e))) => {
                        self.current_scan = SortScanState::Done;
                        Some(Err(e))
                    }
                    (Some(_), Some(_)) => match self.set_current_scan() {
                        Ok(_) => Some(Ok(())),
                        Err(e) => Some(Err(e)),
                    },
                }
            }
            SortScanState::OnFirst => match self.s1.next() {
                Some(Ok(_)) => {
                    return match self.set_current_scan() {
                        Ok(_) => Some(Ok(())),
                        Err(e) => Some(Err(e)),
                    };
                }
                Some(Err(e)) => Some(Err(e)),
                None => {
                    self.current_scan = SortScanState::OnlySecond;
                    Some(Ok(()))
                }
            },
            SortScanState::OnlyFirst => match self.s1.next() {
                Some(Ok(_)) => Some(Ok(())),
                Some(Err(e)) => Some(Err(e)),
                None => {
                    self.current_scan = SortScanState::Done;
                    None
                }
            },
            SortScanState::OnSecond => match self.s2.as_mut().unwrap().next() {
                Some(Ok(_)) => {
                    return match self.set_current_scan() {
                        Ok(_) => Some(Ok(())),
                        Err(e) => Some(Err(e)),
                    };
                }
                Some(Err(e)) => Some(Err(e)),
                None => {
                    self.s2 = None;
                    self.current_scan = SortScanState::OnlyFirst;
                    Some(Ok(()))
                }
            },
            SortScanState::OnlySecond => match self.s2.as_mut().unwrap().next() {
                Some(Ok(_)) => Some(Ok(())),
                Some(Err(e)) => Some(Err(e)),
                None => {
                    self.current_scan = SortScanState::Done;
                    None
                }
            },
            SortScanState::Done => None,
        }
    }
}

impl Scan for SortScan {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.current_scan = SortScanState::BeforeFirst;
        self.s1.before_first()?;
        if let Some(s2) = &mut self.s2 {
            s2.before_first()?;
        }
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        match self.current_scan {
            SortScanState::OnFirst | SortScanState::OnlyFirst => self.s1.get_int(field_name),
            SortScanState::OnSecond | SortScanState::OnlySecond => {
                self.s2.as_ref().unwrap().get_int(field_name)
            }
            _ => Err("No current record".into()),
        }
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        match self.current_scan {
            SortScanState::OnFirst | SortScanState::OnlyFirst => self.s1.get_string(field_name),
            SortScanState::OnSecond | SortScanState::OnlySecond => {
                self.s2.as_ref().unwrap().get_string(field_name)
            }
            _ => Err("No current record".into()),
        }
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        match self.current_scan {
            SortScanState::OnFirst | SortScanState::OnlyFirst => self.s1.get_value(field_name),
            SortScanState::OnSecond | SortScanState::OnlySecond => {
                self.s2.as_ref().unwrap().get_value(field_name)
            }
            _ => Err("No current record".into()),
        }
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        match self.current_scan {
            SortScanState::OnFirst | SortScanState::OnlyFirst => self.s1.has_field(field_name),
            SortScanState::OnSecond | SortScanState::OnlySecond => {
                self.s2.as_ref().unwrap().has_field(field_name)
            }
            _ => Err("No current record".into()),
        }
    }
}

impl UpdateScan for SortScan {
    fn set_int(&self, _field_name: &str, _value: i32) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn set_string(&self, _field_name: &str, _value: String) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn set_value(&self, _field_name: &str, _value: Constant) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        todo!()
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

#[cfg(test)]
mod sort_scan_tests {
    use std::sync::Arc;

    use crate::{
        Layout, RecordComparator, Scan, Schema, SimpleDB, SortScan, TempTable, UpdateScan,
    };

    #[test]
    fn test_sort_scan_basic() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create schema and layout
        let mut schema = Schema::new();
        schema.add_int_field("id");
        schema.add_string_field("name", 10);
        let layout = Layout::new(schema);

        // Create two temp tables with test data
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout.schema.clone());

        // Insert test data into first temp table
        {
            let mut scan = temp_table1.open();
            for i in [1, 3, 5] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        // Insert test data into second temp table
        {
            let mut scan = temp_table2.open();
            for i in [2, 4, 6] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
                scan.set_string("name", format!("name{i}")).unwrap();
            }
        }

        // Create record comparator for sorting on id field
        let record_comparator = RecordComparator::new(vec!["id".to_string()]);

        // Create sort scan
        let mut sort_scan = SortScan::new(vec![temp_table1, temp_table2], record_comparator);

        // Verify records come back in sorted order
        let mut prev_id = None;
        let mut count = 0;

        while let Some(result) = sort_scan.next() {
            assert!(result.is_ok());
            let curr_id = sort_scan.get_int("id").unwrap();

            if let Some(prev) = prev_id {
                assert!(
                    curr_id > prev,
                    "Records should be in ascending order which is not upheld for {curr_id} and {prev}"
                );
            }

            count += 1;
            prev_id = Some(curr_id);
        }

        assert_eq!(count, 6, "Should have retrieved all records");

        txn.commit().unwrap();
    }

    #[test]
    fn test_sort_scan_single_table() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("id");
        let layout = Layout::new(schema);

        // Create single temp table with unsorted data
        let temp_table = TempTable::new(Arc::clone(&txn), layout.schema.clone());

        {
            let mut scan = temp_table.open();
            for i in [1, 2, 3, 4, 5] {
                scan.insert().unwrap();
                scan.set_int("id", i).unwrap();
            }
        }

        let record_comparator = RecordComparator::new(vec!["id".to_string()]);
        let mut sort_scan = SortScan::new(vec![temp_table], record_comparator);

        let mut prev_id = None;
        let mut count = 0;

        while let Some(result) = sort_scan.next() {
            assert!(result.is_ok());
            let curr_id = sort_scan.get_int("id").unwrap();

            if let Some(prev) = prev_id {
                assert!(curr_id > prev);
            }

            count += 1;
            prev_id = Some(curr_id);
        }

        assert_eq!(count, 5);

        drop(sort_scan);
        txn.commit().unwrap();
    }

    #[test]
    fn test_sort_scan_empty_tables() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("id");
        let layout = Layout::new(schema);

        // Create empty temp tables
        let temp_table1 = TempTable::new(Arc::clone(&txn), layout.schema.clone());
        let temp_table2 = TempTable::new(Arc::clone(&txn), layout.schema.clone());

        let record_comparator = RecordComparator::new(vec!["id".to_string()]);
        let mut sort_scan = SortScan::new(vec![temp_table1, temp_table2], record_comparator);

        let mut count = 0;
        for result in sort_scan.by_ref() {
            assert!(result.is_ok());
            count += 1;
        }

        assert_eq!(count, 0, "No records should be returned for empty tables");

        drop(sort_scan);
        txn.commit().unwrap();
    }
}

#[derive(Clone)]
pub struct RecordComparator {
    field_list: Vec<String>,
}

impl RecordComparator {
    pub fn new(field_list: Vec<String>) -> Self {
        Self { field_list }
    }

    fn compare<S1: Scan, S2: Scan>(&self, s1: &S1, s2: &S2) -> Result<Ordering, Box<dyn Error>> {
        for field in &self.field_list {
            let value_1 = s1.get_value(field)?;
            let value_2 = s2.get_value(field)?;
            let cmp = value_1.cmp(&value_2);
            if cmp != std::cmp::Ordering::Equal {
                return Ok(cmp);
            }
        }
        Ok(Ordering::Equal)
    }
}

pub struct MaterializePlan {
    source_plan: Arc<dyn Plan>,
    txn: Arc<Transaction>,
}

impl MaterializePlan {
    pub fn new(source_plan: Arc<dyn Plan>, txn: Arc<Transaction>) -> Self {
        Self { source_plan, txn }
    }
}

impl Plan for MaterializePlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let mut source_scan = self.source_plan.open();
        println!("The schema retrieved {:?}", self.source_plan.schema());
        let temp_table = TempTable::new(Arc::clone(&self.txn), self.source_plan.schema());
        let mut temp_table_scan = temp_table.open();
        while let Some(result) = source_scan.next() {
            if result.is_err() {
                panic!("Error while materializing the plan");
            }
            temp_table_scan.insert().unwrap();
            for field in self.source_plan.schema().fields {
                temp_table_scan
                    .set_value(&field, source_scan.get_value(&field).unwrap())
                    .unwrap();
            }
        }
        temp_table_scan.before_first().unwrap();
        Box::new(temp_table_scan)
    }

    fn blocks_accessed(&self) -> usize {
        let layout = Layout::new(self.source_plan.schema());
        let rpb = self.txn.block_size() / layout.slot_size;
        self.source_plan.records_output() / rpb
    }

    fn records_output(&self) -> usize {
        self.source_plan.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.source_plan.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.source_plan.schema()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ MaterializePlan");
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!(
            "{}├─ Schema: {:?}",
            prefix,
            self.source_plan.schema().fields
        );
        println!("{prefix}├─ Source Plan:");
        self.source_plan.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

#[cfg(test)]
mod materialize_plan_tests {
    use crate::{MaterializePlan, Plan, Scan, SimpleDB, TablePlan};
    use std::sync::Arc;

    #[test]
    fn test_materialize_plan() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the source table using SQL
        let sql = "create table source_table(A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert test data using SQL
        let test_data = [
            (1, "first"),
            (2, "second"),
            (3, "third"),
            (4, "fourth"),
            (5, "fifth"),
        ];

        for (a_val, b_val) in test_data.iter() {
            let sql = format!("insert into source_table(A, B) values ({a_val}, '{b_val}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }
        println!("DONE INSERTING DATA");

        // Create source plan
        let table_plan = TablePlan::new(
            "source_table",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        );

        // Create materialize plan
        let materialize_plan = MaterializePlan::new(Arc::new(table_plan), Arc::clone(&txn));

        // Open the materialized scan
        let mut materialized_scan = materialize_plan.open();

        // Verify all records were materialized correctly
        let mut count = 0;
        while let Some(result) = materialized_scan.next() {
            assert!(result.is_ok());
            let a_val = materialized_scan.get_int("a").unwrap();
            let b_val = materialized_scan.get_string("b").unwrap();

            // Verify against original data
            assert_eq!(b_val, test_data[count].1);
            assert_eq!(a_val, test_data[count].0);
            count += 1;
        }

        assert_eq!(count, test_data.len(), "All records should be materialized");

        // Test that the materialized data persists after source is modified
        let sql = "delete from source_table".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Verify materialized data still exists
        materialized_scan.before_first().unwrap();
        let mut count = 0;
        while let Some(result) = materialized_scan.next() {
            assert!(result.is_ok());
            let a_val = materialized_scan.get_int("a").unwrap();
            let b_val = materialized_scan.get_string("b").unwrap();

            // Verify against original data
            assert_eq!(b_val, test_data[count].1);
            assert_eq!(a_val, test_data[count].0);
            count += 1;
        }

        assert_eq!(count, test_data.len(), "Materialized data should persist");

        // Test schema matches
        let materialized_schema = materialize_plan.schema();
        assert_eq!(materialized_schema.fields.len(), 2);
        assert!(materialized_schema.fields.contains(&"a".to_string()));
        assert!(materialized_schema.fields.contains(&"b".to_string()));
    }

    #[test]
    fn test_materialize_plan_empty_source() {
        // Create test database
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create empty table using SQL
        let sql = "create table empty_table(A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Create source plan
        let sql = "select A, B from empty_table".to_string();
        let source_plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();

        // Create materialize plan
        let materialize_plan = MaterializePlan::new(source_plan, Arc::clone(&txn));

        // Open the materialized scan
        let materialized_scan = materialize_plan.open();

        // Verify no records exist
        let mut count = 0;
        for result in materialized_scan {
            assert!(result.is_ok());
            count += 1;
        }

        assert_eq!(
            count, 0,
            "No records should be materialized from empty source"
        );
    }
}

pub static TEMP_TABLE_ID_GENERATOR: AtomicUsize = AtomicUsize::new(0);

#[derive(Clone)]
pub struct TempTable {
    txn: Arc<Transaction>,
    table_name: String,
    layout: Layout,
}

impl TempTable {
    pub fn new(txn: Arc<Transaction>, schema: Schema) -> Self {
        let table_id = TEMP_TABLE_ID_GENERATOR.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let table_name = format!("TempTable{table_id}");
        let layout = Layout::new(schema);
        Self {
            txn,
            table_name,
            layout,
        }
    }

    pub fn open(&self) -> TableScan {
        TableScan::new(Arc::clone(&self.txn), self.layout.clone(), &self.table_name)
    }
}

pub struct Planner {
    query_planner: Box<dyn QueryPlanner>,
    update_planner: Box<dyn UpdatePlanner>,
}

impl Planner {
    pub fn new(
        query_planner: Box<dyn QueryPlanner>,
        update_planner: Box<dyn UpdatePlanner>,
    ) -> Self {
        Self {
            query_planner,
            update_planner,
        }
    }

    pub fn create_query_plan(
        &self,
        query: String,
        txn: Arc<Transaction>,
    ) -> Result<Arc<dyn Plan>, Box<dyn Error>> {
        let mut parser = Parser::new(&query);
        let query_data = parser.query()?;
        //  TODO: Verify the query. How?
        self.query_planner.create_plan(query_data, txn)
    }

    pub fn execute_update(
        &self,
        command: String,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let mut parser = Parser::new(&command);
        match parser.update_command()? {
            parser::SQLStatement::CreateTable(create_table_data) => self
                .update_planner
                .execute_create_table(create_table_data, Arc::clone(&txn)),
            parser::SQLStatement::CreateView(create_view_data) => self
                .update_planner
                .execute_create_view(create_view_data, Arc::clone(&txn)),
            parser::SQLStatement::CreateIndex(create_index_data) => self
                .update_planner
                .execute_create_index(create_index_data, Arc::clone(&txn)),
            parser::SQLStatement::Insert(insert_data) => self
                .update_planner
                .execute_insert(insert_data, Arc::clone(&txn)),
            parser::SQLStatement::Delete(delete_data) => self
                .update_planner
                .execute_delete(delete_data, Arc::clone(&txn)),
            parser::SQLStatement::Modify(modify_data) => self
                .update_planner
                .execute_modify(modify_data, Arc::clone(&txn)),
        }
    }
}

#[cfg(test)]
mod planner_tests {
    use std::sync::Arc;

    use crate::{Constant, Index, Plan, SimpleDB, TablePlan};

    #[test]
    fn test_planner_single_table() {
        //  Create the table T1
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        let sql = "create table T1(A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        //  Insert the records into the table T1
        let count = 200;
        dbg!("inserting records", count);
        for i in 0..count {
            let sql = format!("insert into T1(A, B) values ({i}, 'string{i}')");
            println!("the sql {sql:?}");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        //  Read the records back and make sure they exist
        dbg!("reading records back");
        let sql = "select B from T1 where A>10".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        let mut scan = plan.open();
        let mut retrieved_count = 0;
        while let Some(_) = scan.next() {
            scan.get_string("b").unwrap();
            retrieved_count += 1;
        }
        assert_eq!(retrieved_count, 189);
    }

    #[test]
    fn test_planner_multi_table() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        //  Create table T1
        dbg!("Creating table T1");
        let sql = "create table T1(A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        //  Insert records in T1
        let count = 200;
        dbg!("Inserting records in T1", count);
        for i in 0..count {
            let sql = format!("insert into T1(A, B) values ({i}, 'string{i}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        //  Create table T2
        dbg!("Creating table T2");
        let sql = "create table T2(C int, D varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        //  Insert records into T2
        dbg!("Inserting records in T2", count);
        for i in (0..count).rev() {
            let sql = format!("insert into T2(C, D) values ({i}, 'string{i}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        //  Join across T1 and T2 on A=C
        dbg!("Joining T1 and T2");
        let sql = "select B,D from T1,T2 where A=C".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        plan.print_plan_internal(0);
        dbg!("Reading records in join");
        let mut scan = plan.open();
        let mut read_count = 0;
        while let Some(_) = scan.next() {
            let lhs = scan.get_string("b").unwrap();
            let rhs = scan.get_string("d").unwrap();
            assert_eq!(lhs, rhs);
            read_count += 1;
        }
        assert_eq!(read_count, 200);
    }

    #[test]
    fn test_planner_single_table_delete() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        //  Creating the table t1
        dbg!("Creating table t1");
        let sql = "create table t1 (A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        //  Insert records into t1
        let count = 200;
        dbg!("Inserting {} records into t1", count);
        for i in 0..count {
            let sql = format!("insert into t1(A, B) values ({i}, 'string{i}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        //  Read the records in t1 back
        dbg!("Reading all the records back");
        let sql = "select A, B from t1".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        let mut scan = plan.open();
        let mut read_count = 0;
        while let Some(_) = scan.next() {
            scan.get_int("a").unwrap();
            scan.get_string("b").unwrap();
            read_count += 1;
        }
        assert_eq!(read_count, count);

        //  Delete some records in t1 and then read back remaining
        dbg!("Deleting some records in t1");
        let sql = "delete from t1 where A < 100".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        let sql = "select A, B from t1".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        let mut scan = plan.open();
        let mut read_count = 0;
        while let Some(_) = scan.next() {
            let a = scan.get_int("a").unwrap();
            scan.get_string("b").unwrap();
            assert!(a >= 100);
            read_count += 1;
        }
        assert_eq!(read_count, count - 100);
    }

    #[test]
    fn test_planner_single_table_modify() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        //  Create the table t1
        dbg!("Creating table t1");
        let sql = "create table t1 (A int, B varchar(10))".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        //  Insert records into t1
        let count = 200;
        dbg!("Inserting {} records into t1", count);
        for i in 0..count {
            let sql = format!("insert into t1(A, B) values ({i}, 'string{i}')");
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        //  Modify some records in t1 and then read back remaining
        dbg!("Modifying some records in t1");
        let sql = "update t1 set B='modified' where A < 100".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        let sql = "select A, B from t1 where A < 100".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        let mut scan = plan.open();
        let mut read_count = 0;
        while let Some(_) = scan.next() {
            scan.get_int("a").unwrap();
            assert_eq!(scan.get_string("b").unwrap(), "modified");
            read_count += 1;
        }
        assert_eq!(read_count, 100);
    }

    #[test]
    fn test_planner_index_retrieval() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        //  Create the student table
        let sql = "create table student(sid int, sname varchar(10), majorid int, gradyear int)"
            .to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Create an index on majorid
        let sql = "create index idx_major on student (majorid)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert some test records
        let students = vec![
            (1, "joe", 10, 2021),
            (2, "amy", 20, 2020),
            (3, "max", 20, 2022),
            (4, "bob", 20, 2020),
            (5, "sue", 30, 2021),
        ];

        for (sid, sname, majorid, gradyear) in students {
            let sql = format!(
                "insert into student(sid, sname, majorid, gradyear) values ({sid}, '{sname}', {majorid}, {gradyear})"
            );
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Get the index info
        let indexes = db
            .metadata_manager
            .get_index_info("student", Arc::clone(&txn));
        let major_index = indexes.get("majorid").expect("Index not found");

        // Open table scan for student table
        let table_plan = TablePlan::new(
            "student",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        );
        let mut table_scan = table_plan.open();

        // Open the index
        let mut index = major_index.open();

        // Find all students with majorid = 20
        let target_major = Constant::Int(20);
        index.before_first(&target_major);

        let mut found_students = Vec::new();
        while index.next() {
            let rid = index.get_data_rid();
            table_scan.move_to_rid(rid).unwrap();
            found_students.push(table_scan.get_string("sname").unwrap());
        }

        assert_eq!(found_students.len(), 3);

        // Sort for consistent comparison
        found_students.sort();

        // We should find amy, bob, and max
        let mut expected = vec!["amy", "bob", "max"];
        expected.sort();

        assert_eq!(found_students, expected);

        //  TODO: I'm leaving this commented out in here as a reminder to implement the RAII guard feature
        //  If I uncomment this, this test fails because of double release of pinned buffers
        // txn.commit().unwrap();
    }

    #[test]
    fn test_planner_index_updates() {
        // Setup database with test data
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create the student table
        let sql = "create table student_alt(sid int, sname varchar(10), majorid int, gradyear int)"
            .to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Create indexes on all fields
        let sql = "create index idx_sid_alt on student_alt(sid)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        let sql = "create index idx_major_alt on student_alt(majorid)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        let sql = "create index idx_year_alt on student_alt(gradyear)".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Insert initial test data
        let students = vec![
            (1, "joe", 10, 2021),
            (2, "amy", 20, 2020),
            (3, "max", 20, 2022),
        ];

        for (sid, sname, majorid, gradyear) in students {
            let sql = format!(
                "insert into student_alt(sid, sname, majorid, gradyear) values ({sid}, '{sname}', {majorid}, {gradyear})"
            );
            db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();
        }

        // Task 1: Insert Sam using index-update planner
        let sql =
            "insert into student_alt(sid, sname, majorid, gradyear) values (11, 'sam', 30, 2023)"
                .to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Task 2: Delete Joe's record using index-update planner
        let sql = "delete from student_alt where sname = 'joe'".to_string();
        db.planner.execute_update(sql, Arc::clone(&txn)).unwrap();

        // Verify the updates through a query
        let sql = "select sname, sid from student_alt".to_string();
        let plan = db.planner.create_query_plan(sql, Arc::clone(&txn)).unwrap();
        let mut scan = plan.open();

        let mut results = Vec::new();
        while let Some(_) = scan.next() {
            let name = scan.get_string("sname").unwrap();
            let id = scan.get_int("sid").unwrap();
            results.push((name, id));
        }

        // Sort results for consistent comparison
        results.sort_by(|a, b| a.0.cmp(&b.0));

        let mut expected = vec![
            ("amy".to_string(), 2),
            ("max".to_string(), 3),
            ("sam".to_string(), 11),
        ];
        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(results, expected);

        // Verify that indexes are correct by checking majorid index
        let indexes = db
            .metadata_manager
            .get_index_info("student_alt", Arc::clone(&txn));
        let major_index = indexes.get("majorid").expect("Index not found");
        let mut index = major_index.open();

        // Check for major=30 (Sam's major)
        let target_major = Constant::Int(30);
        index.before_first(&target_major);

        let mut found = false;
        while index.next() {
            let rid = index.get_data_rid();
            let mut table_scan = TablePlan::new(
                "student_alt",
                Arc::clone(&txn),
                Arc::clone(&db.metadata_manager),
            )
            .open();
            table_scan.move_to_rid(rid).unwrap();
            if table_scan.get_string("sname").unwrap() == "sam" {
                found = true;
                break;
            }
        }

        assert!(found, "Sam's record not found in majorid index");

        // txn.commit().unwrap();
    }
}

struct IndexUpdatePlanner {
    metadata_manager: Arc<MetadataManager>,
}

impl IndexUpdatePlanner {
    pub fn new(metadata_manager: Arc<MetadataManager>) -> Self {
        Self { metadata_manager }
    }
}

impl UpdatePlanner for IndexUpdatePlanner {
    fn execute_insert(
        &self,
        data: InsertData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let indexes = self
            .metadata_manager
            .get_index_info(&data.table_name, Arc::clone(&txn));
        let plan = TablePlan::new(&data.table_name, txn, Arc::clone(&self.metadata_manager));
        let mut scan = plan.open();
        scan.insert()?;

        for (field, value) in data.fields.iter().zip(data.values) {
            scan.set_value(field, value.clone())?;
            if let Some(ii) = indexes.get(field) {
                let mut index = ii.open();
                index.insert(&value, &scan.get_rid()?);
            }
        }
        Ok(1)
    }

    fn execute_delete(
        &self,
        data: DeleteData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let indexes = self
            .metadata_manager
            .get_index_info(&data.table_name, Arc::clone(&txn));
        let plan = Arc::new(TablePlan::new(
            &data.table_name,
            Arc::clone(&txn),
            Arc::clone(&self.metadata_manager),
        ));
        let plan = SelectPlan::new(plan, data.predicate);
        let mut scan = plan.open();
        let mut rows_deleted = 0;

        while let Some(_) = scan.next() {
            let rid = scan.get_rid()?;
            for field in indexes.keys() {
                let mut index = indexes.get(field).unwrap().open();
                index.delete(&scan.get_value(field)?, &rid);
            }
            scan.delete()?;
            rows_deleted += 1;
        }

        Ok(rows_deleted)
    }

    fn execute_modify(
        &self,
        data: ModifyData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let indexes = self
            .metadata_manager
            .get_index_info(&data.table_name, Arc::clone(&txn));
        let plan = Arc::new(TablePlan::new(
            &data.table_name,
            Arc::clone(&txn),
            Arc::clone(&self.metadata_manager),
        ));
        let plan = SelectPlan::new(plan, data.predicate);
        let mut scan = plan.open();
        let mut update_count = 0;

        while let Some(_) = scan.next() {
            let old_value = scan.get_value(&data.field_name)?;
            let new_value = data.new_value.evaluate(&scan)?;
            scan.set_value(&data.field_name, new_value.clone())?;
            if let Some(ii) = indexes.get(&data.field_name) {
                let mut index = ii.open();
                index.delete(&old_value, &scan.get_rid()?);
                index.insert(&new_value, &scan.get_rid()?);
            }
            update_count += 1;
        }
        Ok(update_count)
    }

    fn execute_create_table(
        &self,
        data: CreateTableData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager
            .create_table(&data.table_name, data.schema, Arc::clone(&txn));
        Ok(0)
    }

    fn execute_create_view(
        &self,
        data: CreateViewData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager.create_view(
            &data.view_name,
            &data.query_data.to_sql(),
            Arc::clone(&txn),
        );
        Ok(0)
    }

    fn execute_create_index(
        &self,
        data: CreateIndexData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager.create_index(
            &data.table_name,
            &data.index_name,
            &data.field_name,
            Arc::clone(&txn),
        );
        Ok(0)
    }
}

struct BasicUpdatePlanner {
    metadata_manager: Arc<MetadataManager>,
}

impl BasicUpdatePlanner {
    pub fn new(metadata_manager: Arc<MetadataManager>) -> Self {
        Self { metadata_manager }
    }
}

impl UpdatePlanner for BasicUpdatePlanner {
    fn execute_insert(
        &self,
        data: InsertData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let plan = TablePlan::new(&data.table_name, txn, Arc::clone(&self.metadata_manager));
        let mut scan = plan.open();
        scan.insert()?;
        for (field, value) in data.fields.iter().zip(data.values) {
            scan.set_value(field, value)?;
        }
        Ok(1)
    }

    fn execute_delete(
        &self,
        data: DeleteData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let plan = Arc::new(TablePlan::new(
            &data.table_name,
            Arc::clone(&txn),
            Arc::clone(&self.metadata_manager),
        ));
        let plan = SelectPlan::new(plan, data.predicate);
        let mut scan = plan.open();
        let mut rows_deleted = 0;
        while let Some(_) = scan.next() {
            scan.delete()?;
            rows_deleted += 1;
        }
        Ok(rows_deleted)
    }

    fn execute_modify(
        &self,
        data: ModifyData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        let plan = Arc::new(TablePlan::new(
            &data.table_name,
            Arc::clone(&txn),
            Arc::clone(&self.metadata_manager),
        ));
        let plan = SelectPlan::new(plan, data.predicate);
        let mut scan = plan.open();
        let mut update_count = 0;
        while let Some(_) = scan.next() {
            let value = data.new_value.evaluate(&scan)?;
            scan.set_value(&data.field_name, value)?;
            update_count += 1;
        }
        Ok(update_count)
    }

    fn execute_create_table(
        &self,
        data: CreateTableData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager
            .create_table(&data.table_name, data.schema, Arc::clone(&txn));
        Ok(0)
    }

    fn execute_create_view(
        &self,
        data: CreateViewData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager.create_view(
            &data.view_name,
            &data.query_data.to_sql(),
            Arc::clone(&txn),
        );
        Ok(0)
    }

    fn execute_create_index(
        &self,
        data: CreateIndexData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>> {
        self.metadata_manager.create_index(
            &data.index_name,
            &data.table_name,
            &data.field_name,
            Arc::clone(&txn),
        );
        Ok(0)
    }
}

pub trait UpdatePlanner {
    fn execute_insert(
        &self,
        data: InsertData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
    fn execute_delete(
        &self,
        data: DeleteData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
    fn execute_modify(
        &self,
        data: ModifyData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
    fn execute_create_table(
        &self,
        data: CreateTableData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
    fn execute_create_view(
        &self,
        data: CreateViewData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
    fn execute_create_index(
        &self,
        data: CreateIndexData,
        txn: Arc<Transaction>,
    ) -> Result<usize, Box<dyn Error>>;
}

/// This is the "physical" portion of the optimizer. It decides on the physical implementation of a node to use
/// For instance, whether to use an [IndexSelectPlan] or a regular [SelectPlan] depending on the availability of certain resources
pub struct TablePlanner {
    table_name: String,
    predicate: Predicate,
    txn: Arc<Transaction>,
    metadata_manager: Arc<MetadataManager>,
    schema: Schema,
    indexes: HashMap<String, IndexInfo>,
    plan: Arc<dyn Plan>,
}

impl TablePlanner {
    pub fn table_name(&self) -> &str {
        &self.table_name
    }

    pub fn metadata_manager(&self) -> &Arc<MetadataManager> {
        &self.metadata_manager
    }

    pub fn new(
        table_name: String,
        predicate: Predicate,
        txn: Arc<Transaction>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        let plan = Arc::new(TablePlan::new(
            table_name.as_str(),
            Arc::clone(&txn),
            Arc::clone(&metadata_manager),
        ));
        let indexes = metadata_manager.get_index_info(&table_name, Arc::clone(&txn));
        Self {
            table_name,
            predicate,
            txn: Arc::clone(&txn),
            metadata_manager,
            schema: plan.schema(),
            indexes,
            plan,
        }
    }

    /// Create a [SelectPlan] for this table using the available predicate. This applies heuristic 6
    /// First, try to apply indexes and create an [IndexSelectPlan]
    /// Next, push down predicates and create a [SelectPlan] with the sub-predicate
    fn make_select_plan(&self) -> Arc<dyn Plan> {
        let new_plan = self.make_index_select_plan(Arc::clone(&self.plan));
        self.add_select_predicate(new_plan)
    }

    /// Create a join plan with the other_plan provided in the function signature. Uses heuristic 7
    /// Check if the [Predicate] will allow joining these two tables. If not, return [None]
    /// If possible, construct an [IndexJoinPlan]. If predicate does not apply, return [ProductPlan]
    /// with a join predicate on top
    fn make_join_plan(&self, other_plan: &Arc<dyn Plan>) -> SimpleDBResult<Option<Arc<dyn Plan>>> {
        let mut unioned_schema = Schema::new();
        unioned_schema.add_all_from_schema(&self.schema)?;
        unioned_schema.add_all_from_schema(&other_plan.schema())?;
        let sub_pred = self.predicate.sub_predicate_for_join(
            &self.schema,
            &other_plan.schema(),
            &unioned_schema,
        );
        if sub_pred.is_empty() {
            return Ok(None);
        }
        let plan = self
            .make_index_join_plan(Arc::clone(&self.plan), Arc::clone(other_plan))?
            .map(Ok)
            .unwrap_or_else(|| self.make_product_join_plan(Arc::clone(other_plan)))?;
        Ok(Some(plan))
    }

    /// Construct a [MultiBufferProductPlan] with the provided plan
    fn make_product_plan(&self, other_plan: Arc<dyn Plan>) -> SimpleDBResult<Arc<dyn Plan>> {
        let filtered_plan = self.add_select_predicate(Arc::clone(&self.plan));
        Ok(Arc::new(MultiBufferProductPlan::new(
            other_plan,
            filtered_plan,
            Arc::clone(&self.txn),
        )?))
    }

    /// Takes the plan and the plan to join with and tries to construct an [IndexJoinPlan]
    /// The conditions for the [IndexJoinPlan] are that:
    /// 1. There needs to exist an index on the field
    /// 2. A subpredicate of the [Predicate] should contain the field
    /// 3. The other plan's schema must also contain the field
    fn make_index_join_plan(
        &self,
        plan: Arc<dyn Plan>,
        other_plan: Arc<dyn Plan>,
    ) -> SimpleDBResult<Option<Arc<dyn Plan>>> {
        let plan_schema = &self.schema;
        let other_plan_schema = &other_plan.schema();
        for field in self.indexes.keys() {
            if let Some(lhs_field) = self.predicate.equates_with_field(field) {
                if plan_schema.fields.contains(field)
                    && other_plan_schema.fields.contains(&lhs_field)
                {
                    let index_info = self.indexes.get(field).cloned().unwrap();
                    let plan =
                        IndexJoinPlan::new(Arc::clone(&other_plan), plan, index_info, lhs_field)
                            .map(Arc::new)
                            .map(|p| self.add_select_predicate(p))
                            .and_then(|p| self.add_join_predicate(p, &other_plan))?;
                    return Ok(Some(plan));
                }
            }
        }
        Ok(None)
    }

    /// This function constructs a [MultiBufferProductPlan] and then adds two plans on top
    /// 1. [SelectPlan] with [Predicate] filtering for this table
    /// 2. [SelectPlan] with [Predicate] filtering for the cross-table join condition
    fn make_product_join_plan(&self, other_plan: Arc<dyn Plan>) -> SimpleDBResult<Arc<dyn Plan>> {
        self.make_product_plan(Arc::clone(&other_plan))
            .and_then(|p| self.add_join_predicate(p, &other_plan))
    }

    /// Loop through fields which have indexes and for the first field which has an index
    /// and is part of the predicate in the form F=c, construct an [IndexSelectPlan]
    /// Using the first index is a pedgagogical simplification
    /// TODO: Fix this to use the most selective index and to use more than one index
    fn make_index_select_plan(&self, plan: Arc<dyn Plan>) -> Arc<dyn Plan> {
        for field in self.indexes.keys() {
            match self.predicate.equates_with_constant(field) {
                Some(value) => {
                    return Arc::new(IndexSelectPlan::new(
                        plan,
                        self.indexes.get(field).unwrap().clone(),
                        value,
                    ));
                }
                None => continue,
            }
        }
        plan
    }

    /// This function will take the [Predicate] and construct a subset of the [Predicate]
    /// which applies to the schema of the table associated with this plan
    /// It will then use that [Predicate] to construct a [SelectPlan]
    /// If no part of the predicate applies, it will return the original plan
    fn add_select_predicate(&self, plan: Arc<dyn Plan>) -> Arc<dyn Plan> {
        let sub_pred = self.predicate.sub_predicate_for_select(&self.schema);
        if sub_pred.is_empty() {
            return plan;
        }
        Arc::new(SelectPlan::new(plan, sub_pred))
    }

    /// This function will take the [Predicate] and construct a subset of the [Predicate]
    /// which applies to the union of the schemas for this table and the joining table
    /// It will then use that [Predicate] to construct a [SelectPlan]
    /// If no part of the predicate applies, it will return the original plan
    fn add_join_predicate(
        &self,
        plan: Arc<dyn Plan>,
        other_plan: &Arc<dyn Plan>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        let mut unioned_schema = Schema::new();
        unioned_schema.add_all_from_schema(&self.schema)?;
        unioned_schema.add_all_from_schema(&other_plan.schema())?;
        let sub_pred = self.predicate.sub_predicate_for_join(
            &self.schema,
            &other_plan.schema(),
            &unioned_schema,
        );
        if sub_pred.is_empty() {
            return Ok(plan);
        }
        Ok(Arc::new(SelectPlan::new(plan, sub_pred)))
    }
}

/// This struct applies a bunch of heuristics on a query to construct a logical query tree
/// It performs logical optimizations while depending on [TablePlanner] to perform physical optimizations
/// Both these optimizations happen in lockstep
pub struct HeuristicQueryPlanner {
    table_planners: Vec<TablePlanner>,
    metadata_manager: Arc<MetadataManager>,
}

impl HeuristicQueryPlanner {
    pub fn new(metadata_manager: Arc<MetadataManager>) -> Self {
        Self {
            table_planners: Vec::new(),
            metadata_manager,
        }
    }

    /// The entry point function which will go through a list of heuristics and attempt to apply them to the provided query
    /// It constructs a left-deep query tree by applying the following heuristics
    /// 1. Heuristic 5a - Choose the table producing the smallest output when deciding join order
    /// 2. Heuristic 4 - Join only previously chosen tables and try to produce smallest possible output
    /// 3. Heuristic 8 - Apply projection nodes to reduce the output, especially at the output of materialized nodes
    pub fn create_plan_internal(
        &mut self,
        query_data: QueryData,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        //  Construct all instances of [TablePlanner]
        for table_name in query_data.tables {
            let table_planner = TablePlanner::new(
                table_name.clone(),
                query_data.predicate.clone(),
                Arc::clone(&txn),
                Arc::clone(&self.metadata_manager),
            );
            self.table_planners.push(table_planner);
        }

        //  Find the table producing the smallest output first
        let mut current_plan = self.get_lowest_select_plan()?;

        //  Find the lowest cost join plan, and failing that find the lowest cost product plan and failing that just error out
        while !self.table_planners.is_empty() {
            match self.get_lowest_join_plan(&current_plan) {
                Ok(new_plan) => {
                    current_plan = new_plan;
                }
                Err(e) => match self.get_lowest_product_plan(current_plan) {
                    Ok(new_plan) => {
                        current_plan = new_plan;
                    }
                    Err(_) => return Err(e),
                },
            }
        }

        Ok(Arc::new(ProjectPlan::new(
            current_plan,
            query_data.fields.iter().map(String::as_str).collect(),
        )?))
    }

    /// Find the [SelectPlan] with the lowest record output
    /// This will apply heuristic 5a
    pub fn get_lowest_select_plan(&mut self) -> SimpleDBResult<Arc<dyn Plan>> {
        let (idx, plan) = self
            .table_planners
            .iter()
            .enumerate()
            .map(|(idx, tp)| (idx, tp.make_select_plan()))
            .min_by_key(|(_, p)| p.records_output())
            .ok_or("there were no plans provided to the get_lowest_select_plan function")?;
        self.table_planners.remove(idx);
        Ok(plan)
    }

    /// Find the right table to construct a join plan with and construct the best join plan
    /// Choose the table to join with current_plan by minimizing the current output
    /// This will apply heuristic 4
    pub fn get_lowest_join_plan(
        &mut self,
        current_plan: &Arc<dyn Plan>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        let candidates: Vec<(usize, Arc<dyn Plan>)> = self
            .table_planners
            .iter()
            .enumerate()
            .map(|(idx, tp)| Ok((idx, tp.make_join_plan(current_plan)?)))
            .collect::<SimpleDBResult<Vec<_>>>()?
            .into_iter()
            .filter_map(|(idx, opt)| opt.map(|p| (idx, p)))
            .collect();
        let (idx, plan) = candidates
            .into_iter()
            .min_by_key(|(_, p)| p.records_output())
            .ok_or("could not construct any join plans")?;
        self.table_planners.remove(idx);
        Ok(plan)
    }

    /// Find the right table to construct a product plan with
    /// Choose the table to do a product with current_plan by minimizing the output
    pub fn get_lowest_product_plan(
        &mut self,
        current_plan: Arc<dyn Plan>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        let (idx, plan) = self
            .table_planners
            .iter()
            .enumerate()
            .map(|(idx, tp)| {
                tp.make_product_plan(Arc::clone(&current_plan))
                    .map(|p| (idx, p))
            })
            .collect::<SimpleDBResult<Vec<_>>>()?
            .into_iter()
            .min_by_key(|(_, p)| p.records_output())
            .ok_or("could not construct any product plan")?;
        self.table_planners.remove(idx);
        Ok(plan)
    }
}

impl QueryPlanner for HeuristicQueryPlanner {
    fn create_plan(
        &self,
        query_data: QueryData,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        let mut hp = HeuristicQueryPlanner::new(Arc::clone(&self.metadata_manager));
        hp.create_plan_internal(query_data, txn)
    }
}

struct BasicQueryPlanner {
    metadata_manager: Arc<MetadataManager>,
}

impl BasicQueryPlanner {
    pub fn new(metadata_manager: Arc<MetadataManager>) -> Self {
        Self { metadata_manager }
    }
}

impl QueryPlanner for BasicQueryPlanner {
    /// Every query plan follows the same pattern:
    /// 1. Create a TablePlan for every table
    /// 2. Create a join of all tables
    /// 3. Create a selection with the predicate
    /// 4. Create a projection of the required columns
    fn create_plan(
        &self,
        query_data: QueryData,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Arc<dyn Plan>> {
        let mut plans = Vec::new();

        // 1. Create the table plans
        for table in query_data.tables {
            plans.push(Arc::new(TablePlan::new(
                &table,
                Arc::clone(&txn),
                Arc::clone(&self.metadata_manager),
            )));
        }

        // 2. Create the product plan for joins
        let mut plan: Arc<dyn Plan> = plans.remove(0);
        for next_plan in plans {
            plan = Arc::new(ProductPlan::new(plan, next_plan)?);
        }

        //  3. Create the selection with the predicate
        plan = Arc::new(SelectPlan::new(plan, query_data.predicate));

        //  4. Create the projection
        plan = Arc::new(ProjectPlan::new(
            plan,
            query_data.fields.iter().map(AsRef::as_ref).collect(),
        )?);
        Ok(plan)
    }
}

#[cfg(test)]
mod basic_query_planner_tests {
    use super::*;

    fn setup_db() -> (SimpleDB, Arc<Transaction>, test_utils::TestDir) {
        let (db, dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        (db, txn, dir)
    }

    fn basic_planner(db: &SimpleDB) -> Planner {
        Planner::new(
            Box::new(BasicQueryPlanner::new(Arc::clone(&db.metadata_manager))),
            Box::new(IndexUpdatePlanner::new(Arc::clone(&db.metadata_manager))),
        )
    }

    fn exec(planner: &Planner, txn: Arc<Transaction>, sql: &str) {
        planner.execute_update(sql.to_string(), txn).unwrap();
    }

    fn fetch_rows(
        planner: &Planner,
        txn: Arc<Transaction>,
        sql: &str,
        fields: &[&str],
    ) -> Vec<Vec<Constant>> {
        let plan = planner.create_query_plan(sql.to_string(), txn).unwrap();
        let mut scan = plan.open();
        let mut out = Vec::new();
        scan.before_first().unwrap();
        while let Some(Ok(())) = scan.next() {
            let mut row = Vec::new();
            for &f in fields {
                row.push(scan.get_value(f).unwrap());
            }
            out.push(row);
        }
        out.sort();
        out
    }

    #[test]
    fn basic_single_table_select_and_project() {
        let (db, txn, _dir) = setup_db();
        let planner = basic_planner(&db);

        exec(
            &planner,
            Arc::clone(&txn),
            "create table t(a int, b varchar(10))",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (1,'x')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (2,'y')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (3,'z')",
        );

        let rows = fetch_rows(
            &planner,
            Arc::clone(&txn),
            "select b from t where a = 2",
            &["b"],
        );

        assert_eq!(rows, vec![vec![Constant::String("y".into())]]);
    }

    #[test]
    fn basic_two_table_product_then_filter() {
        let (db, txn, _dir) = setup_db();
        let planner = basic_planner(&db);

        exec(
            &planner,
            Arc::clone(&txn),
            "create table e(id int, name varchar(20))",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "create table d(depid int, dept varchar(20))",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into e(id,name) values (1,'a')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into e(id,name) values (2,'b')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (2,'x')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (3,'y')",
        );

        // Basic planner builds product + select
        let rows = fetch_rows(
            &planner,
            Arc::clone(&txn),
            "select name, dept from e, d where id = depid",
            &["name", "dept"],
        );

        assert_eq!(
            rows,
            vec![vec![
                Constant::String("b".into()),
                Constant::String("x".into())
            ]]
        );
    }

    #[test]
    fn basic_single_table_or_predicate() {
        let (db, txn, _dir) = setup_db();
        let planner = basic_planner(&db);

        exec(
            &planner,
            Arc::clone(&txn),
            "create table t(a int, b varchar(10))",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (1,'x')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (2,'y')",
        );
        exec(
            &planner,
            Arc::clone(&txn),
            "insert into t(a,b) values (3,'z')",
        );

        let rows = fetch_rows(
            &planner,
            Arc::clone(&txn),
            "select b from t where a = 2 or a = 3",
            &["b"],
        );

        assert_eq!(
            rows,
            vec![
                vec![Constant::String("y".into())],
                vec![Constant::String("z".into())]
            ]
        );
    }
}

#[cfg(test)]
mod heuristic_equivalence_tests {
    use super::*;

    fn setup_db() -> (SimpleDB, Arc<Transaction>, test_utils::TestDir) {
        let (db, dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        (db, txn, dir)
    }

    fn basic_planner(db: &SimpleDB) -> Planner {
        Planner::new(
            Box::new(BasicQueryPlanner::new(Arc::clone(&db.metadata_manager))),
            Box::new(IndexUpdatePlanner::new(Arc::clone(&db.metadata_manager))),
        )
    }

    fn heuristic_planner(db: &SimpleDB) -> Planner {
        Planner::new(
            Box::new(HeuristicQueryPlanner::new(Arc::clone(&db.metadata_manager))),
            Box::new(IndexUpdatePlanner::new(Arc::clone(&db.metadata_manager))),
        )
    }

    fn exec(planner: &Planner, txn: Arc<Transaction>, sql: &str) {
        planner.execute_update(sql.to_string(), txn).unwrap();
    }

    fn fetch_rows(
        planner: &Planner,
        txn: Arc<Transaction>,
        sql: &str,
        fields: &[&str],
    ) -> Vec<Vec<Constant>> {
        let plan = planner.create_query_plan(sql.to_string(), txn).unwrap();
        let mut scan = plan.open();
        let mut out = Vec::new();
        scan.before_first().unwrap();
        while let Some(Ok(())) = scan.next() {
            let mut row = Vec::new();
            for &f in fields {
                row.push(scan.get_value(f).unwrap());
            }
            out.push(row);
        }
        out.sort();
        out
    }

    fn assert_equivalent(db: &SimpleDB, txn: Arc<Transaction>, sql: &str, fields: &[&str]) {
        let bp = basic_planner(db);
        let hp = heuristic_planner(db);
        let left = fetch_rows(&bp, Arc::clone(&txn), sql, fields);
        let right = fetch_rows(&hp, Arc::clone(&txn), sql, fields);
        assert_eq!(left, right);
    }

    #[test]
    fn eq_single_table_or_select() {
        let (db, txn, _dir) = setup_db();
        let p = basic_planner(&db);

        exec(&p, Arc::clone(&txn), "create table t(a int, b varchar(10))");
        exec(&p, Arc::clone(&txn), "insert into t(a,b) values (1,'x')");
        exec(&p, Arc::clone(&txn), "insert into t(a,b) values (2,'y')");
        exec(&p, Arc::clone(&txn), "insert into t(a,b) values (3,'z')");

        assert_equivalent(
            &db,
            Arc::clone(&txn),
            "select b from t where a = 2 or a = 3",
            &["b"],
        );
    }

    #[test]
    fn eq_two_table_join_without_index() {
        let (db, txn, _dir) = setup_db();
        let p = basic_planner(&db);

        exec(
            &p,
            Arc::clone(&txn),
            "create table e(id int, name varchar(20))",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "create table d(depid int, dept varchar(20))",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into e(id,name) values (1,'a')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into e(id,name) values (2,'b')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (2,'x')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (3,'y')",
        );

        assert_equivalent(
            &db,
            Arc::clone(&txn),
            "select name, dept from e, d where id = depid",
            &["name", "dept"],
        );
    }

    #[test]
    fn eq_two_table_join_with_index() {
        let (db, txn, _dir) = setup_db();
        let p = basic_planner(&db);

        exec(
            &p,
            Arc::clone(&txn),
            "create table e(id int, name varchar(20))",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "create table d(depid int, dept varchar(20))",
        );
        exec(&p, Arc::clone(&txn), "create index idx_d_depid on d(depid)");
        exec(
            &p,
            Arc::clone(&txn),
            "insert into e(id,name) values (1,'a')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into e(id,name) values (2,'b')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (2,'x')",
        );
        exec(
            &p,
            Arc::clone(&txn),
            "insert into d(depid,dept) values (3,'y')",
        );

        assert_equivalent(
            &db,
            Arc::clone(&txn),
            "select name, dept from e, d where id = depid",
            &["name", "dept"],
        );
    }
}

#[cfg(test)]
mod heuristic_efficiency_tests {
    use super::*;

    fn setup_db() -> (SimpleDB, Arc<Transaction>, test_utils::TestDir) {
        let (db, dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();
        (db, txn, dir)
    }

    fn basic_planner(db: &SimpleDB) -> Planner {
        Planner::new(
            Box::new(BasicQueryPlanner::new(Arc::clone(&db.metadata_manager))),
            Box::new(IndexUpdatePlanner::new(Arc::clone(&db.metadata_manager))),
        )
    }

    fn heuristic_planner(db: &SimpleDB) -> Planner {
        Planner::new(
            Box::new(HeuristicQueryPlanner::new(Arc::clone(&db.metadata_manager))),
            Box::new(IndexUpdatePlanner::new(Arc::clone(&db.metadata_manager))),
        )
    }

    fn exec(planner: &Planner, txn: Arc<Transaction>, sql: &str) {
        planner.execute_update(sql.to_string(), txn).unwrap();
    }

    fn plan_costs(planner: &Planner, txn: Arc<Transaction>, sql: &str) -> (usize, usize) {
        let plan = planner.create_query_plan(sql.to_string(), txn).unwrap();
        (plan.blocks_accessed(), plan.records_output())
    }

    #[test]
    fn efficiency_student_enroll_query() {
        let (db, txn, _dir) = setup_db();
        let bp = basic_planner(&db);
        let hp = heuristic_planner(&db);

        exec(
            &bp,
            Arc::clone(&txn),
            "create table student(sid int, name varchar(20), gradyear int)",
        );
        exec(
            &bp,
            Arc::clone(&txn),
            "create table enroll(sid2 int, cid int)",
        );

        exec(
            &bp,
            Arc::clone(&txn),
            "create index idx_student_sid on student(sid)",
        );
        exec(
            &bp,
            Arc::clone(&txn),
            "create index idx_enroll_sid2 on enroll(sid2)",
        );

        exec(
            &bp,
            Arc::clone(&txn),
            "insert into student(sid,name,gradyear) values (1,'John',2025)",
        );
        exec(
            &bp,
            Arc::clone(&txn),
            "insert into student(sid,name,gradyear) values (2,'Alice',2024)",
        );

        for i in 0..200 {
            let sid = if i % 2 == 0 { 1 } else { 2 };
            exec(
                &bp,
                Arc::clone(&txn),
                &format!("insert into enroll(sid2,cid) values ({}, {})", sid, 2025),
            );
        }

        let sql = "select name from student, enroll \
                   where sid = sid2 and name = 'john' and gradyear = cid";

        let (b_blocks, b_rows) = plan_costs(&bp, Arc::clone(&txn), sql);
        let (h_blocks, h_rows) = plan_costs(&hp, Arc::clone(&txn), sql);

        let b_plan = bp
            .create_query_plan(sql.to_string(), Arc::clone(&txn))
            .unwrap();
        println!("Basic plan:");
        b_plan.print_plan(0);
        let h_plan = hp
            .create_query_plan(sql.to_string(), Arc::clone(&txn))
            .unwrap();
        println!("Heuristic plan:");
        h_plan.print_plan(0);

        assert_eq!(b_rows, h_rows);
        println!("h_blocks: {h_blocks}, b_blocks: {b_blocks}");
        // Allow modest slack because cost model can differ with page size; heuristic should not be drastically worse.
        assert!(
            h_blocks <= b_blocks + 2,
            "heuristic blocks {h_blocks} > basic blocks {b_blocks} (with slack)"
        );
    }
}

pub trait QueryPlanner {
    fn create_plan(
        &self,
        query_data: QueryData,
        txn: Arc<Transaction>,
    ) -> SimpleDBResult<Arc<dyn Plan>>;
}

struct ProductPlan {
    plan_1: Arc<dyn Plan>,
    plan_2: Arc<dyn Plan>,
    schema: Schema,
}

impl ProductPlan {
    pub fn new(plan_1: Arc<dyn Plan>, plan_2: Arc<dyn Plan>) -> Result<Self, Box<dyn Error>> {
        let mut schema = Schema::new();
        schema.add_all_from_schema(&plan_1.schema())?;
        schema.add_all_from_schema(&plan_2.schema())?;
        Ok(Self {
            plan_1,
            plan_2,
            schema,
        })
    }
}

impl Plan for ProductPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let scan_1 = self.plan_1.open();
        let scan_2 = self.plan_2.open();
        Box::new(ProductScan::new(scan_1, scan_2))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan_1.blocks_accessed() + self.plan_1.records_output() * self.plan_2.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan_1.records_output() * self.plan_2.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self
            .plan_1
            .schema()
            .fields
            .contains(&field_name.to_string())
        {
            return self.plan_1.distinct_values(field_name);
        } else if self
            .plan_2
            .schema()
            .fields
            .contains(&field_name.to_string())
        {
            return self.plan_2.distinct_values(field_name);
        }
        0
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ ProductPlan");
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{}├─ Schema: {:?}", prefix, self.schema.fields);
        println!("{prefix}├─ Left Plan:");
        self.plan_1.print_plan(indent + 1);
        println!("{prefix}├─ Right Plan:");
        self.plan_2.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

struct ProjectPlan {
    plan: Arc<dyn Plan>,
    schema: Schema,
}

impl ProjectPlan {
    pub fn new(plan: Arc<dyn Plan>, fields_list: Vec<&str>) -> Result<Self, Box<dyn Error>> {
        let mut schema = Schema::new();

        // Handle wildcard expansion
        let expanded_fields: Vec<String> = if fields_list.contains(&"*") {
            plan.schema().fields.clone()
        } else {
            fields_list.iter().map(|s| s.to_string()).collect()
        };

        for field in &expanded_fields {
            schema.add_from_schema(field, &plan.schema())?;
        }
        Ok(Self { plan, schema })
    }
}

impl Plan for ProjectPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let scan = self.plan.open();
        Box::new(ProjectScan::new(scan, self.schema.fields.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.plan.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ ProjectPlan");
        println!("{}├─ Fields: {:?}", prefix, self.schema.fields);
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{prefix}├─ Child Plan:");
        self.plan.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

pub struct IndexSelectPlan {
    plan: Arc<dyn Plan>,
    ii: IndexInfo,
    value: Constant,
}

impl IndexSelectPlan {
    pub fn new(plan: Arc<dyn Plan>, ii: IndexInfo, value: Constant) -> Self {
        Self { plan, ii, value }
    }
}

impl Plan for IndexSelectPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let scan = self.plan.open();
        let scan: TableScan = *(scan as Box<dyn Any>)
            .downcast()
            .expect("Failed to downcast to TableScan");
        Box::new(IndexSelectScan::new(
            scan,
            self.ii.open(),
            self.value.clone(),
        ))
    }

    fn blocks_accessed(&self) -> usize {
        self.ii.blocks_accessed() + self.records_output()
    }

    fn records_output(&self) -> usize {
        self.ii.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.ii.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ IndexSelectPlan");
        println!("{}├─ Index: {}", prefix, self.ii.index_name);
        println!("{}├─ Search Value: {:?}", prefix, self.value);
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{prefix}├─ Child Plan:");
        self.plan.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

struct SelectPlan {
    plan: Arc<dyn Plan>,
    predicate: Predicate,
}

impl SelectPlan {
    pub fn new(plan: Arc<dyn Plan>, predicate: Predicate) -> Self {
        Self { plan, predicate }
    }
}

impl Plan for SelectPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        Box::new(SelectScan::new(self.plan.open(), self.predicate.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan.records_output() / self.predicate.reduction_factor(&self.plan)
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.predicate.equates_with_constant(field_name).is_some() {
            1
        } else if let Some(field_name_2) = self.predicate.equates_with_field(field_name) {
            std::cmp::min(
                self.plan.distinct_values(field_name),
                self.plan.distinct_values(&field_name_2),
            )
        } else {
            self.plan.distinct_values(field_name)
        }
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ SelectPlan");
        println!("{}├─ Predicate: {}", prefix, self.predicate.to_sql());
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{prefix}├─ Child Plan:");
        self.plan.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

struct TablePlan {
    table_name: String,
    txn: Arc<Transaction>,
    layout: Layout,
    stat_info: StatInfo,
}

impl TablePlan {
    pub fn new(
        table_name: &str,
        txn: Arc<Transaction>,
        metadata_manager: Arc<MetadataManager>,
    ) -> Self {
        let layout = metadata_manager.get_layout(table_name, Arc::clone(&txn));
        let stat_info =
            metadata_manager.get_stat_info(table_name, layout.clone(), Arc::clone(&txn));
        Self {
            table_name: table_name.to_string(),
            txn,
            layout,
            stat_info,
        }
    }
}

impl Plan for TablePlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        Box::new(TableScan::new(
            Arc::clone(&self.txn),
            self.layout.clone(),
            &self.table_name,
        ))
    }

    fn blocks_accessed(&self) -> usize {
        self.stat_info.num_blocks
    }

    fn records_output(&self) -> usize {
        self.stat_info.num_records
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        self.stat_info.distinct_values(field_name)
    }

    fn schema(&self) -> Schema {
        self.layout.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ TablePlan");
        println!("{}├─ Table: {}", prefix, self.table_name);
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{}├─ Schema: {:?}", prefix, self.schema().fields);
        println!("{prefix}╰─");
    }
}

pub trait Plan {
    fn open(&self) -> Box<dyn UpdateScan>;
    fn blocks_accessed(&self) -> usize;
    fn records_output(&self) -> usize;
    fn distinct_values(&self, field_name: &str) -> usize;
    fn schema(&self) -> Schema;
    fn print_plan(&self, indent: usize) {
        self.print_plan_internal(indent);
    }
    fn print_plan_internal(&self, indent: usize);
}

#[cfg(test)]
mod plan_test_single_table {
    use std::sync::Arc;

    use crate::{Plan, Predicate, ProjectPlan, SelectPlan, SimpleDB, TablePlan, Term};

    fn print_stats<T>(plan: &T, type_of_plan: &str)
    where
        T: Plan,
    {
        println!("Here are the stats for plan {type_of_plan}");
        println!("B(p) -> {}", plan.blocks_accessed());
        println!("R(p) -> {}", plan.records_output());
    }

    #[test]
    fn plan_test_single_table() {
        //  This is a test for the SQL query
        //  SELECT sname, majorid, gradyear
        //  FROM student
        //  WHERE majorid = 10 AND gradyear = 2020;
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);

        //  the table plan
        let table = TablePlan::new("student", db.new_tx(), Arc::clone(&db.metadata_manager));
        print_stats(&table, "table");

        //  the select plan
        let term_1 = Term::new(
            crate::Expression::FieldName("majorid".to_string()),
            crate::Expression::Constant(crate::Constant::Int(10)),
        );
        let term_2 = Term::new(
            crate::Expression::FieldName("gradyear".to_string()),
            crate::Expression::Constant(crate::Constant::Int(10)),
        );
        let predicate = Predicate::new(vec![term_1, term_2]);
        let select = SelectPlan::new(Arc::new(table), predicate);
        print_stats(&select, "select");

        //  the project plan
        let project = ProjectPlan::new(Arc::new(select), vec!["sname", "majorid", "gradyear"]);
        assert!(project.is_err());

        //  This will never run in the test, but that's okay for now. This test is mostly a sanity check to see that things compose together
        if let Ok(project) = project {
            // open the plan and initiate the scan now
            let mut scan = project.open();
            while let Some(_) = scan.next() {
                println!(
                    "sid {}, sname {}, majorid {}, gradyear {}",
                    scan.get_int("sid").unwrap(),
                    scan.get_string("sname").unwrap(),
                    scan.get_int("majorid").unwrap(),
                    scan.get_int("gradyear").unwrap()
                );
            }
        }
    }
}

impl Scan for Box<dyn Scan> {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        (**self).before_first()
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        (**self).get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        (**self).get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        (**self).get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        (**self).has_field(field_name)
    }
}

impl Scan for Box<dyn UpdateScan> {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        (**self).before_first()
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        (**self).get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        (**self).get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        (**self).get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        (**self).has_field(field_name)
    }
}

impl UpdateScan for Box<dyn UpdateScan> {
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        (**self).set_int(field_name, value)
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        (**self).set_string(field_name, value)
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        (**self).set_value(field_name, value)
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        (**self).insert()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        (**self).delete()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        (**self).get_rid()
    }

    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        (**self).move_to_rid(rid)
    }
}

struct ProductScan<S1, S2>
where
    S1: Scan,
    S2: Scan,
{
    s1: S1,
    s2: S2,
}

impl<S1, S2> ProductScan<S1, S2>
where
    S1: Scan,
    S2: Scan,
{
    pub fn new(s1: S1, s2: S2) -> Self {
        let mut scan = Self { s1, s2 };
        scan.before_first().unwrap();
        scan
    }
}

impl<S1, S2> Iterator for ProductScan<S1, S2>
where
    S1: Scan,
    S2: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on ProductScan");
        match self.s2.next() {
            Some(result) => match result {
                Ok(_) => Some(Ok(())),
                Err(e) => Some(Err(e)),
            },
            //  s2 cannot be advanced
            None => match self.s1.next() {
                //  advance s1, reset s2 and then return
                Some(result) => match result {
                    Ok(_) => {
                        self.s2.before_first().unwrap();
                        self.s2.next();
                        Some(Ok(()))
                    }
                    Err(e) => Some(Err(e)),
                },
                None => None,
            },
        }
    }
}

impl<S1, S2> Scan for ProductScan<S1, S2>
where
    S1: Scan,
    S2: Scan,
{
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.s1.before_first()?;
        self.s1.next();
        self.s2.before_first()
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.get_int(field_name);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.get_int(field_name);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.get_string(field_name);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.get_string(field_name);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.get_value(field_name);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.get_value(field_name);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return Ok(true);
        }
        if self.s2.has_field(field_name)? {
            return Ok(true);
        }
        Ok(false)
    }
}

impl<S1, S2> UpdateScan for ProductScan<S1, S2>
where
    S1: UpdateScan + 'static,
    S2: UpdateScan + 'static,
{
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.set_int(field_name, value);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.set_int(field_name, value);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.set_string(field_name, value);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.set_string(field_name, value);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.set_value(field_name, value);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.set_value(field_name, value);
        }
        Err(format!("Field {field_name} not found in ProductScan").into())
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        panic!("Insert not supported in ProductScan");
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        panic!("Delete not supported in ProductScan");
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        panic!("Get RID not supported in ProductScan");
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        panic!("Move to RID not supported in ProductScan");
    }
}

#[cfg(test)]
mod product_scan_tests {
    use super::UpdateScan;
    use std::sync::Arc;

    use crate::{
        Layout, Predicate, ProductScan, ProjectScan, Scan, Schema, SelectScan, SimpleDB, TableScan,
        Term,
    };

    #[test]
    fn product_scan_test() {
        let (test_db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = test_db.new_tx();
        let mut schema1 = Schema::new();
        schema1.add_int_field("A");
        schema1.add_string_field("B", 10);
        let layout1 = Layout::new(schema1);
        let mut schema2 = Schema::new();
        schema2.add_int_field("C");
        schema2.add_string_field("D", 10);
        let layout2 = Layout::new(schema2);

        //  open scanners for both schemas and insert them
        {
            let mut scan1 = TableScan::new(Arc::clone(&txn), layout1.clone(), "T1");
            let mut scan2 = TableScan::new(Arc::clone(&txn), layout2.clone(), "T2");
            for i in 0..50 {
                scan1.insert().unwrap();
                scan1.set_int("A", i).unwrap();
                scan1.set_string("B", format!("string{i}")).unwrap();
                scan2.insert().unwrap();
                scan2.set_int("C", i).unwrap();
                scan2.set_string("D", format!("string{i}")).unwrap();
            }
        }

        //  create a product scan for both tables and retrieve B and D where A = C
        {
            let scan1 = TableScan::new(Arc::clone(&txn), layout1.clone(), "T1");
            let scan2 = TableScan::new(Arc::clone(&txn), layout2.clone(), "T2");
            let product_scan = ProductScan::new(scan1, scan2);
            let term = Term::new(
                crate::Expression::FieldName("A".to_string()),
                crate::Expression::FieldName("C".to_string()),
            );
            let predicate = Predicate::new(vec![term]);
            let select_scan = SelectScan::new(product_scan, predicate);
            let mut project_scan =
                ProjectScan::new(select_scan, vec!["B".to_string(), "D".to_string()]);
            // project_scan.before_first().unwrap();
            while let Some(_) = project_scan.next() {
                let lhs = project_scan.get_string("B").unwrap();
                let rhs = project_scan.get_string("D").unwrap();
                assert_eq!(lhs, rhs);
            }
        }
        txn.commit().unwrap();
    }
}

struct ProjectScan<S>
where
    S: Scan,
{
    scan: S,
    _field_list: Vec<String>,
}

impl<S> ProjectScan<S>
where
    S: Scan,
{
    pub fn new(scan: S, field_list: Vec<String>) -> Self {
        Self {
            scan,
            _field_list: field_list,
        }
    }
}

impl<S> Iterator for ProjectScan<S>
where
    S: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on ProjectScan");
        self.scan.next()
    }
}

impl<S> Scan for ProjectScan<S>
where
    S: Scan,
{
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {field_name} not found in ProjectScan").into());
        }
        self.scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {field_name} not found in ProjectScan").into());
        }
        self.scan.get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {field_name} not found in ProjectScan").into());
        }
        self.scan.get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        self.scan.has_field(field_name)
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.before_first()
    }
}

impl<S> UpdateScan for ProjectScan<S>
where
    S: UpdateScan + 'static,
{
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        self.scan.set_int(field_name, value)
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        self.scan.set_string(field_name, value)
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        self.scan.set_value(field_name, value)
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.insert()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.delete()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        self.scan.move_to_rid(rid)
    }
}

impl<S> Drop for ProjectScan<S>
where
    S: Scan,
{
    fn drop(&mut self) {
        //  no-op because no resources to clean up
    }
}

#[cfg(test)]
mod project_scan_tests {
    use super::UpdateScan;
    use std::sync::Arc;

    use crate::{
        test_utils::generate_random_number, Constant, Layout, Predicate, ProjectScan, Scan, Schema,
        SelectScan, SimpleDB, TableScan, Term,
    };

    #[test]
    fn project_scan_test() {
        let (test_db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = test_db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        let mut inserted_count = 0;
        //  insertion block
        {
            let mut scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            for i in 0..50 {
                if i % 10 == 0 {
                    dbg!("Inserting number {}", 10);
                    scan.insert().unwrap();
                    scan.set_int("A", 10).unwrap();
                    scan.set_string("B", format!("string{}", 10)).unwrap();
                    inserted_count += 1;
                    continue;
                }

                let number = (generate_random_number() % 9) + 1; //  generate number in the range of 1-9
                dbg!("Inserting number {}", number);
                scan.insert().unwrap();
                scan.set_int("A", number.try_into().unwrap()).unwrap();
                scan.set_string("B", format!("string{number}")).unwrap();
                inserted_count += 1;
            }
            dbg!("Inserted count {}", inserted_count);
        }

        //  selection and projection block
        {
            let mut projected_count = 0;
            let scan = TableScan::new(Arc::clone(&txn), layout, "T");
            let constant = Constant::Int(10);
            let term = Term::new(
                crate::Expression::FieldName("A".to_string()),
                crate::Expression::Constant(constant),
            );
            let predicate = Predicate::new(vec![term]);
            let select_scan = SelectScan::new(scan, predicate);
            let mut projection_scan = ProjectScan::new(select_scan, vec!["B".to_string()]);
            while let Some(_) = projection_scan.next() {
                assert_eq!(projection_scan.get_int("A").unwrap(), 10);
                assert_eq!(
                    projection_scan.get_string("B").unwrap(),
                    format!("string{}", 10)
                );
                projected_count += 1;
            }
            assert_eq!(projected_count, 5);
        }
        txn.commit().unwrap();
    }
}

pub struct IndexJoinPlan {
    plan_1: Arc<dyn Plan>,
    plan_2: Arc<dyn Plan>,
    index_info: IndexInfo,
    schema: Schema,
    join_field: String,
}

impl IndexJoinPlan {
    pub fn new(
        plan_1: Arc<dyn Plan>,
        plan_2: Arc<dyn Plan>,
        index_info: IndexInfo,
        join_field: String,
    ) -> Result<Self, Box<dyn Error>> {
        let mut schema = Schema::new();
        schema.add_all_from_schema(&plan_1.schema())?;
        schema.add_all_from_schema(&plan_2.schema())?;
        Ok(Self {
            plan_1,
            plan_2,
            index_info,
            schema,
            join_field,
        })
    }
}

impl Plan for IndexJoinPlan {
    fn open(&self) -> Box<dyn UpdateScan> {
        let lhs = self.plan_1.open();
        let scan = self.plan_2.open();
        let scan: TableScan = *(scan as Box<dyn Any>)
            .downcast()
            .expect("Failed to downcast to TableScan in IndexJoinPlan");
        let idx = self.index_info.open();
        Box::new(IndexJoinScan::new(lhs, idx, scan, self.join_field.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan_1.blocks_accessed()
            + (self.plan_2.records_output() * self.index_info.blocks_accessed())
            + self.records_output()
    }

    fn records_output(&self) -> usize {
        self.plan_1.records_output() * self.index_info.records_output()
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self
            .plan_1
            .schema()
            .fields
            .contains(&field_name.to_string())
        {
            return self.plan_1.distinct_values(field_name);
        }
        if self
            .plan_2
            .schema()
            .fields
            .contains(&field_name.to_string())
        {
            return self.plan_2.distinct_values(field_name);
        }
        panic!("Field {field_name} not found in IndexJoinPlan");
    }

    fn schema(&self) -> Schema {
        self.schema.clone()
    }

    fn print_plan_internal(&self, indent: usize) {
        let prefix = "  ".repeat(indent);
        println!("{prefix}╭─ IndexJoinPlan");
        println!("{}├─ Blocks: {}", prefix, self.blocks_accessed());
        println!("{}├─ Records: {}", prefix, self.records_output());
        println!("{}├─ Index: {}", prefix, self.index_info);
        println!("{}├─ JoinField: {}", prefix, self.join_field);
        println!("{prefix}├─ Left Plan:");
        self.plan_1.print_plan(indent + 1);
        println!("{prefix}├─ Right Plan:");
        self.plan_2.print_plan(indent + 1);
        println!("{prefix}╰─");
    }
}

#[cfg(test)]
mod index_join_plan_tests {
    use std::sync::Arc;

    use crate::{IndexJoinPlan, Plan, SimpleDB, TablePlan};

    #[test]
    fn test_index_join_plan_with_real_tables() {
        // Setup DB
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        // Create tables
        db.planner
            .execute_update(
                "create table employees(id int, name varchar(20))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();
        db.planner
            .execute_update(
                "create table departments(depid int, deptname varchar(20))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();

        // Create index on departments.depid (join target)
        db.planner
            .execute_update(
                "create index idx_depid on departments(depid)".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();

        // Insert data
        for (id, name) in &[(1, "Alice"), (2, "Bob"), (3, "Charlie"), (4, "David")] {
            db.planner
                .execute_update(
                    format!("insert into employees(id, name) values ({id}, '{name}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }
        for (id, dept) in &[(2, "Engineering"), (3, "Sales"), (5, "Marketing")] {
            db.planner
                .execute_update(
                    format!("insert into departments(depid, deptname) values ({id}, '{dept}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }

        // Build plans
        let lhs = Arc::new(TablePlan::new(
            "employees",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let rhs = Arc::new(TablePlan::new(
            "departments",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));

        // Get IndexInfo for departments.depid
        let idx_info = db
            .metadata_manager
            .get_index_info("departments", Arc::clone(&txn))
            .remove("depid")
            .expect("expected index on departments.depid");

        // Create plan: join employees.id = departments.depid using index on departments.depid
        let plan = IndexJoinPlan::new(lhs, rhs, idx_info, "id".to_string()).unwrap();

        // Execute
        let mut scan = plan.open();
        let mut results = Vec::new();
        while let Some(res) = scan.next() {
            assert!(res.is_ok());
            let id = scan.get_int("id").unwrap();
            let name = scan.get_string("name").unwrap();
            let dept = scan.get_string("deptname").unwrap();
            results.push((id, name, dept));
        }

        // Expect Bob-Engineering, Charlie-Sales
        results.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0],
            (2, "Bob".to_string(), "Engineering".to_string())
        );
        assert_eq!(results[1], (3, "Charlie".to_string(), "Sales".to_string()));
    }

    #[test]
    fn test_index_join_plan_no_matches() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = db.new_tx();

        db.planner
            .execute_update(
                "create table t1(a int, b varchar(10))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();
        db.planner
            .execute_update(
                "create table t2(c int, d varchar(10))".to_string(),
                Arc::clone(&txn),
            )
            .unwrap();
        db.planner
            .execute_update("create index idx_c on t2(c)".to_string(), Arc::clone(&txn))
            .unwrap();

        for i in 0..5 {
            db.planner
                .execute_update(
                    format!("insert into t1(a, b) values ({i}, 'x{i}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }
        for i in 100..105 {
            db.planner
                .execute_update(
                    format!("insert into t2(c, d) values ({i}, 'y{i}')"),
                    Arc::clone(&txn),
                )
                .unwrap();
        }

        let lhs = Arc::new(TablePlan::new(
            "t1",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let rhs = Arc::new(TablePlan::new(
            "t2",
            Arc::clone(&txn),
            Arc::clone(&db.metadata_manager),
        ));
        let idx_info = db
            .metadata_manager
            .get_index_info("t2", Arc::clone(&txn))
            .remove("c")
            .expect("expected index on t2.c");

        let plan = IndexJoinPlan::new(lhs, rhs, idx_info, "a".to_string()).unwrap();
        let scan = plan.open();
        let mut count = 0;
        for res in scan {
            assert!(res.is_ok());
            count += 1;
        }
        assert_eq!(count, 0);
    }
}

enum IndexJoinScanState {
    Init,
    Probe,
    Advance,
    Done,
}

pub struct IndexJoinScan<S, I>
where
    S: Scan,
    I: Index,
{
    lhs: S,
    rhs: TableScan,
    index: I,
    join_field: String,
    state: IndexJoinScanState,
}

impl<S, I> IndexJoinScan<S, I>
where
    S: Scan,
    I: Index,
{
    pub fn new(lhs: S, index: I, rhs: TableScan, join_field: String) -> Self {
        Self {
            lhs,
            rhs,
            index,
            join_field,
            state: IndexJoinScanState::Init,
        }
    }

    pub fn reset_index(&mut self) -> Result<(), Box<dyn Error>> {
        self.index
            .before_first(&self.lhs.get_value(&self.join_field)?);
        Ok(())
    }
}

impl<S, I> Iterator for IndexJoinScan<S, I>
where
    S: Scan,
    I: Index,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on IndexJoinScan");

        loop {
            match self.state {
                IndexJoinScanState::Init => {
                    if let Err(e) = self.lhs.before_first() {
                        return Some(Err(e));
                    }
                    match self.lhs.next() {
                        Some(Ok(())) => {
                            if let Err(e) = self.reset_index() {
                                return Some(Err(e));
                            };
                            self.state = IndexJoinScanState::Probe;
                            continue;
                        }
                        Some(Err(e)) => {
                            return Some(Err(e));
                        }
                        None => {
                            self.state = IndexJoinScanState::Done;
                            continue;
                        }
                    }
                }
                IndexJoinScanState::Probe => {
                    if self.index.next() {
                        let rid = self.index.get_data_rid();
                        self.rhs.move_to_row_id(rid);
                        return Some(Ok(()));
                    } else {
                        self.state = IndexJoinScanState::Advance;
                    }
                }
                IndexJoinScanState::Advance => match self.lhs.next() {
                    Some(Ok(())) => {
                        if let Err(e) = self.reset_index() {
                            return Some(Err(e));
                        }
                        self.state = IndexJoinScanState::Probe;
                        continue;
                    }
                    Some(Err(e)) => return Some(Err(e)),
                    None => {
                        self.state = IndexJoinScanState::Done;
                        continue;
                    }
                },
                IndexJoinScanState::Done => return None,
            }
        }
    }
}

impl<S, I> Scan for IndexJoinScan<S, I>
where
    S: Scan,
    I: Index,
{
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.state = IndexJoinScanState::Init;
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.get_int(field_name);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.get_int(field_name);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.get_string(field_name);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.get_string(field_name);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.get_value(field_name);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.get_value(field_name);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        if self.rhs.has_field(field_name)? {
            return Ok(true);
        }
        if self.lhs.has_field(field_name)? {
            return Ok(true);
        }
        Ok(false)
    }
}

impl<S, I> UpdateScan for IndexJoinScan<S, I>
where
    S: UpdateScan + 'static,
    I: Index + 'static,
{
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.set_int(field_name, value);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.set_int(field_name, value);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.set_string(field_name, value);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.set_string(field_name, value);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        if self.lhs.has_field(field_name)? {
            return self.lhs.set_value(field_name, value);
        }
        if self.rhs.has_field(field_name)? {
            return self.rhs.set_value(field_name, value);
        }
        Err(format!("Field {field_name} not found in IndexJoinScan").into())
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        panic!("Insert not supported in IndexJoinScan");
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        panic!("Delete not supported in IndexJoinScan");
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        panic!("Get RID not supported in IndexJoinScan");
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        panic!("Move to RID not supported in IndexJoinScan");
    }
}

#[cfg(test)]
mod index_join_scan_tests {
    use super::UpdateScan;
    use std::sync::Arc;

    use crate::{
        Constant, Index, IndexInfo, IndexJoinScan, Layout, Scan, Schema, SimpleDB, StatInfo,
        TableScan,
    };

    #[test]
    fn index_join_scan_test() {
        let (simple_db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = simple_db.new_tx();

        // Create schemas for both tables
        let mut schema1 = Schema::new();
        schema1.add_int_field("A");
        schema1.add_string_field("B", 10);
        let layout1 = Layout::new(schema1.clone());

        let mut schema2 = Schema::new();
        schema2.add_int_field("C");
        schema2.add_string_field("D", 10);
        let layout2 = Layout::new(schema2.clone());

        // Create index info for join field
        let index_info = IndexInfo::new(
            "test_index",
            "C",
            Arc::clone(&txn),
            schema2,
            StatInfo::new(0, 0),
        );

        // Insert data into both tables
        let mut inserted_count = 0;
        {
            // First table
            let mut scan1 = TableScan::new(Arc::clone(&txn), layout1.clone(), "T1");
            // Second table with index
            let mut scan2 = TableScan::new(Arc::clone(&txn), layout2.clone(), "T2");

            for i in 0..50 {
                // Insert into first table
                scan1.insert().unwrap();
                scan1.set_int("A", i).unwrap();
                scan1.set_string("B", format!("string{i}")).unwrap();

                // Insert into second table with matching values
                scan2.insert().unwrap();
                scan2.set_int("C", i).unwrap();
                scan2.set_string("D", format!("string{i}")).unwrap();

                // Create index entry
                let mut index = index_info.open();
                index.insert(&Constant::Int(i), &scan2.get_rid().unwrap());

                inserted_count += 1;
            }
            dbg!("Inserted {} records in each table", inserted_count);
        }

        // Test the index join
        {
            let mut join_count = 0;
            let scan1 = TableScan::new(Arc::clone(&txn), layout1.clone(), "T1");
            let scan2 = TableScan::new(Arc::clone(&txn), layout2.clone(), "T2");
            let index = index_info.open();

            let mut index_join_scan = IndexJoinScan::new(scan1, index, scan2, "A".to_string());
            index_join_scan.before_first().unwrap();

            while let Some(Ok(())) = index_join_scan.next() {
                // Verify join condition A = C
                let a_val = index_join_scan.get_int("A").unwrap();
                let c_val = index_join_scan.get_int("C").unwrap();
                assert_eq!(a_val, c_val);

                // Verify corresponding strings match
                let b_val = index_join_scan.get_string("B").unwrap();
                let d_val = index_join_scan.get_string("D").unwrap();
                assert_eq!(b_val, d_val);

                join_count += 1;
            }

            // Should find all matches
            assert_eq!(join_count, inserted_count);
        }

        txn.commit().unwrap();
    }
}

pub struct IndexSelectScan<I>
where
    I: Index,
{
    scan: TableScan,
    index: I,
    value: Constant,
}

impl<I> IndexSelectScan<I>
where
    I: Index,
{
    pub fn new(scan: TableScan, index: I, value: Constant) -> Self {
        Self { scan, index, value }
    }
}

impl<I> Iterator for IndexSelectScan<I>
where
    I: Index,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        let result = self.index.next();
        if result {
            let rid = self.index.get_data_rid();
            self.scan.move_to_row_id(rid);
            return Some(Ok(()));
        }
        None
    }
}

impl<I> Scan for IndexSelectScan<I>
where
    I: Index,
{
    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.index.before_first(&self.value);
        Ok(())
    }

    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        self.scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        self.scan.get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        self.scan.get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        self.scan.has_field(field_name)
    }
}

impl<I> UpdateScan for IndexSelectScan<I>
where
    I: Index + 'static,
{
    fn set_int(&self, _field_name: &str, _value: i32) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }

    fn set_string(&self, _field_name: &str, _value: String) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }

    fn set_value(&self, _field_name: &str, _value: Constant) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        unreachable!()
    }

    fn move_to_rid(&mut self, _rid: RID) -> Result<(), Box<dyn Error>> {
        unreachable!()
    }
}

#[cfg(test)]
mod index_select_scan_tests {
    use super::UpdateScan;
    use std::sync::Arc;

    use crate::{
        test_utils::generate_random_number, Constant, Index, IndexInfo, IndexSelectScan, Layout,
        Scan, Schema, SimpleDB, StatInfo, TableScan,
    };

    #[test]
    fn index_select_scan_test() {
        let (simple_db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = simple_db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema.clone());

        let mut inserted_count = 0;
        let index_info = IndexInfo::new(
            "test_index",
            "A",
            Arc::clone(&txn),
            schema,
            StatInfo::new(0, 0),
        );
        //  insertion block
        {
            let mut scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            for i in 0..50 {
                if i % 10 == 0 {
                    dbg!("Inserting number {}", 10);
                    scan.insert().unwrap();
                    scan.set_int("A", 10).unwrap();
                    scan.set_string("B", format!("string{}", 10)).unwrap();
                    dbg!("Inserting the index entry when value is 10");
                    let mut index = index_info.open();
                    index.insert(&Constant::Int(10), &scan.get_rid().unwrap());
                    inserted_count += 1;
                    continue;
                }

                let number = (generate_random_number() % 9) + 1; //  generate number in the range of 1-9
                dbg!("Inserting number {} into table", number);
                scan.insert().unwrap();
                scan.set_int("A", number.try_into().unwrap()).unwrap();
                scan.set_string("B", format!("string{number}")).unwrap();
                dbg!("Inserting the index entry");
                let mut index = index_info.open();
                index.insert(
                    &Constant::Int(number.try_into().unwrap()),
                    &scan.get_rid().unwrap(),
                );
                inserted_count += 1;
            }
            dbg!("Inserted count {}", inserted_count);
        }

        //  read block via index
        {
            let mut selection_count = 0;
            let scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            let value = Constant::Int(10);
            let index = index_info.open();
            let mut index_select_scan = IndexSelectScan::new(scan, index, value);
            index_select_scan.before_first().unwrap();
            while let Some(Ok(())) = index_select_scan.next() {
                assert_eq!(index_select_scan.get_int("A").unwrap(), 10);
                selection_count += 1;
            }
            assert_eq!(selection_count, 5);
        }
        txn.commit().unwrap();
    }
}

struct SelectScan<S>
where
    S: Scan,
{
    scan: S,
    predicate: Predicate,
}

impl<S> SelectScan<S>
where
    S: Scan,
{
    pub fn new(scan: S, predicate: Predicate) -> Self {
        Self { scan, predicate }
    }
}

impl<S> Iterator for SelectScan<S>
where
    S: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on SelectScan");
        while let Some(result) = self.scan.next() {
            match result {
                Ok(_) => match self.predicate.is_satisfied(&self.scan) {
                    Ok(true) => return Some(Ok(())),
                    Ok(false) => continue,
                    Err(e) => return Some(Err(e)),
                },
                Err(e) => return Some(Err(e)),
            }
        }
        None
    }
}

impl<S> Scan for SelectScan<S>
where
    S: Scan,
{
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        self.scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        self.scan.get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        self.scan.get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        self.scan.has_field(field_name)
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.before_first()
    }
}

impl<S> UpdateScan for SelectScan<S>
where
    S: UpdateScan + 'static,
{
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        self.scan.set_int(field_name, value)
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        self.scan.set_string(field_name, value)
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        self.scan.set_value(field_name, value)
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.insert()
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.delete()
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        self.scan.get_rid()
    }

    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        self.scan.move_to_rid(rid)
    }
}

#[cfg(test)]
mod select_scan_tests {
    use super::UpdateScan;
    use std::sync::Arc;

    use crate::{
        test_utils::generate_random_number, Constant, Layout, Predicate, Scan, Schema, SelectScan,
        SimpleDB, TableScan, Term,
    };

    #[test]
    fn select_scan_test() {
        let (simple_db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let txn = simple_db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        let mut inserted_count = 0;
        //  insertion block
        {
            let mut scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            for i in 0..50 {
                if i % 10 == 0 {
                    dbg!("Inserting number {}", 10);
                    scan.insert().unwrap();
                    scan.set_int("A", 10).unwrap();
                    scan.set_string("B", format!("string{}", 10)).unwrap();
                    inserted_count += 1;
                    continue;
                }

                let number = (generate_random_number() % 9) + 1; //  generate number in the range of 1-9
                dbg!("Inserting number {}", number);
                scan.insert().unwrap();
                scan.set_int("A", number.try_into().unwrap()).unwrap();
                scan.set_string("B", format!("string{number}")).unwrap();
                inserted_count += 1;
            }
            dbg!("Inserted count {}", inserted_count);
        }

        //  selection block
        {
            let mut selection_count = 0;
            let scan = TableScan::new(Arc::clone(&txn), layout, "T");
            let constant = Constant::Int(10);
            let term = Term::new(
                crate::Expression::FieldName("A".to_string()),
                crate::Expression::Constant(constant),
            );
            let predicate = Predicate::new(vec![term]);
            let mut select_scan = SelectScan::new(scan, predicate);
            while let Some(result) = select_scan.next() {
                assert!(result.is_ok());
                assert!(select_scan.get_int("A").unwrap() == 10);
                selection_count += 1;
            }
            assert_eq!(selection_count, 5);
        }
        txn.commit().unwrap();
    }
}

#[derive(Debug, Clone)]
pub struct Predicate {
    root: PredicateNode,
}

#[derive(Clone, Debug)]
enum PredicateNode {
    Empty,
    Term(Term),
    Composite {
        op: BooleanConnective,
        operands: Vec<PredicateNode>,
    },
}

#[derive(Clone, Debug)]
enum BooleanConnective {
    And,
    Or,
    Not,
}

impl Predicate {
    pub fn new(terms: Vec<Term>) -> Self {
        match terms.len() {
            0 => Self {
                root: PredicateNode::Empty,
            },
            1 => Self {
                root: PredicateNode::Term(terms[0].clone()),
            },
            _ => Self {
                root: PredicateNode::Composite {
                    op: BooleanConnective::And,
                    operands: terms.into_iter().map(PredicateNode::Term).collect(),
                },
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        matches!(self.root, PredicateNode::Empty)
    }

    fn or(predicates: Vec<Predicate>) -> Self {
        Self {
            root: PredicateNode::Composite {
                op: BooleanConnective::Or,
                operands: predicates.iter().map(|p| p.root.clone()).collect(),
            },
        }
    }

    fn and(predicates: Vec<Predicate>) -> Self {
        Self {
            root: PredicateNode::Composite {
                op: BooleanConnective::And,
                operands: predicates.iter().map(|p| p.root.clone()).collect(),
            },
        }
    }

    fn not(predicate: Predicate) -> Self {
        Self {
            root: PredicateNode::Composite {
                op: BooleanConnective::Not,
                operands: vec![predicate.root],
            },
        }
    }

    fn is_satisfied<S>(&self, scan: &S) -> Result<bool, Box<dyn Error>>
    where
        S: Scan,
    {
        Predicate::evaluate_node(&self.root, scan)
    }

    fn evaluate_node<S>(node: &PredicateNode, scan: &S) -> Result<bool, Box<dyn Error>>
    where
        S: Scan,
    {
        match node {
            //  terminal condition for recursion
            PredicateNode::Empty => Ok(true),
            PredicateNode::Term(term) => term.is_satisfied(scan),
            PredicateNode::Composite { op, operands } => match op {
                BooleanConnective::And => {
                    for operand in operands {
                        if !Predicate::evaluate_node(operand, scan)? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                BooleanConnective::Or => {
                    for operand in operands {
                        if Predicate::evaluate_node(operand, scan)? {
                            return Ok(true);
                        }
                    }
                    Ok(false)
                }
                BooleanConnective::Not => {
                    if operands.len() != 1 {
                        return Err("NOT operator must have exactly one operand".into());
                    }
                    return Ok(!Predicate::evaluate_node(&operands[0], scan)?);
                }
            },
        }
    }

    fn reduction_factor(&self, plan: &Arc<dyn Plan>) -> usize {
        Predicate::evaluate_reduction_factor(&self.root, plan)
    }

    fn evaluate_reduction_factor(node: &PredicateNode, plan: &Arc<dyn Plan>) -> usize {
        match node {
            PredicateNode::Empty => 1,
            PredicateNode::Term(term) => term.reduction_factor(plan),
            PredicateNode::Composite { op, operands } => {
                let mut factor = 1;
                for operand in operands {
                    factor *= Predicate::evaluate_reduction_factor(operand, plan);
                }
                match op {
                    BooleanConnective::And => factor,
                    BooleanConnective::Or => factor,
                    BooleanConnective::Not => factor,
                }
            }
        }
    }

    fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        Predicate::evaluate_equates_with_constant(&self.root, field_name)
    }

    fn evaluate_equates_with_constant(node: &PredicateNode, field_name: &str) -> Option<Constant> {
        match node {
            PredicateNode::Empty => None,
            PredicateNode::Term(term) => term.equates_with_constant(field_name),
            PredicateNode::Composite { op: _, operands } => {
                for operand in operands {
                    if let Some(val) =
                        Predicate::evaluate_equates_with_constant(operand, field_name)
                    {
                        return Some(val);
                    }
                }
                None
            }
        }
    }

    /// Return the name of the other field if this predicate contains an equality
    /// between `field_name` and another field.
    ///
    /// Behavior:
    /// - Traverses the predicate tree and finds a term of the form `field_name = X`
    ///   or `X = field_name`, where `X` is another field name.
    /// - Returns `Some(other_field_name)` for the first match encountered
    ///   (depth-first), otherwise `None` if no such equality exists.
    /// - Non-equality comparisons and equalities with constants are ignored.
    fn equates_with_field(&self, field_name: &str) -> Option<String> {
        Predicate::evaluate_equates_with_field(&self.root, field_name)
    }

    /// Helper used by [equates_with_field] to recursively search the predicate tree
    /// for an equality between `field_name` and another field.
    ///
    /// Behavior:
    /// - `PredicateNode::Term`: delegate to `Term::equates_with_field`.
    /// - `PredicateNode::Composite`: recursively visit operands in order and
    ///   return the first match found.
    /// - `PredicateNode::Empty`: returns `None`.
    fn evaluate_equates_with_field(node: &PredicateNode, field_name: &str) -> Option<String> {
        match node {
            PredicateNode::Empty => None,
            PredicateNode::Term(term) => term.equates_with_field(field_name),
            PredicateNode::Composite { op: _, operands } => {
                for operand in operands {
                    if let Some(field) = Predicate::evaluate_equates_with_field(operand, field_name)
                    {
                        return Some(field);
                    }
                }
                None
            }
        }
    }

    /// Construct a sub-predicate which will apply to the union of the two schemas provided
    /// but will not apply to either individually. This is done to avoid redundant predicates
    /// which would have already been applied by individual select sub-predicates on a specific relation
    pub fn sub_predicate_for_join(
        &self,
        schema_1: &Schema,
        schema_2: &Schema,
        unioned_schema: &Schema,
    ) -> Predicate {
        let term_ok = |term: &Term| {
            !term.applies_to(schema_1)
                && !term.applies_to(schema_2)
                && term.applies_to(unioned_schema)
        };
        Predicate {
            root: Predicate::filter_node(&self.root, &term_ok),
        }
    }

    /// This function will take in a schema and evaluate which parts of this predicate apply
    /// to that schema. It will construct and return a new sub-predicate
    pub fn sub_predicate_for_select(&self, schema: &Schema) -> Predicate {
        let term_ok = |term: &Term| term.applies_to(schema);
        Predicate {
            root: Predicate::filter_node(&self.root, &term_ok),
        }
    }

    /// Determines whether the [PredicateNode] fully applies to the given schema
    /// Used to determine whether the [BooleanConnective::Or] and [BooleanConnective::Not] can be applied to the schema
    fn node_applies_to<F>(node: &PredicateNode, term_ok: &F) -> bool
    where
        F: Fn(&Term) -> bool,
    {
        match node {
            PredicateNode::Empty => false,
            PredicateNode::Term(term) => term_ok(term),
            PredicateNode::Composite { op, operands } => match op {
                BooleanConnective::Not => {
                    assert!(operands.len() == 1);
                    Predicate::node_applies_to(operands.first().unwrap(), term_ok)
                }
                _ => operands
                    .iter()
                    .all(|node| Predicate::node_applies_to(node, term_ok)),
            },
        }
    }

    /// Recursively decide whether this node is valid to keep. The following rules are applied:
    /// 1. If [Term] is encountered, check if it applies on this [Schema]
    /// 2. If [BooleanConnective::And] is encountered, keep parts of conjunct that apply
    /// 3. If [BooleanConnective::Or] is encountered, all parts of disjunct must apply
    /// 4. If [BooleanConnective::Not] is encountered, the sole term must apply
    ///    Technically, it is not required for the [BooleanConnective::Or] disjuncts to apply. Take, for instance,
    ///    P = (R.a = 1 ∧ S.b = 2) ∨ (R.c = 3), pushing to R
    ///    (R.a = 1 ∧ S.b = 2) filters to R.a = 1
    ///    (R.c = 3) stays R.c = 3
    ///    OR as (R.a = 1) ∨ (R.c = 3)
    ///    This would lead to a partial application of the disjuncts but would not be incorrect
    ///    However, the same is not true for NOT because
    ///    P = NOT(R.a = 1 ∧ S.b = 2), pushing to R.
    ///    Inner on R partially applies: (R.a = 1 ∧ S.b = 2) ⇒ R.a = 1
    ///    Consider a row r with a=1 and some s with b≠2
    ///    Pushed-down filter NOT(a=1) removes r before join, losing valid results.
    ///    TODO: An alternative is to rewrite the rules using De Morgan's Laws
    fn filter_node<F>(node: &PredicateNode, term_ok: &F) -> PredicateNode
    where
        F: Fn(&Term) -> bool,
    {
        match node {
            PredicateNode::Empty => PredicateNode::Empty,
            PredicateNode::Term(term) => {
                if term_ok(term) {
                    PredicateNode::Term(term.clone())
                } else {
                    PredicateNode::Empty
                }
            }
            PredicateNode::Composite { op, operands } => match op {
                BooleanConnective::And => {
                    let kept: Vec<PredicateNode> = operands
                        .iter()
                        .map(|node| Predicate::filter_node(node, term_ok))
                        .filter(|node| !matches!(node, PredicateNode::Empty))
                        .collect();
                    match kept.is_empty() {
                        true => PredicateNode::Empty,
                        false => PredicateNode::Composite {
                            op: BooleanConnective::And,
                            operands: kept,
                        },
                    }
                }
                BooleanConnective::Or => {
                    if operands
                        .iter()
                        .all(|node| Predicate::node_applies_to(node, term_ok))
                    {
                        let kept: Vec<PredicateNode> = operands
                            .iter()
                            .map(|node| Predicate::filter_node(node, term_ok))
                            .collect();
                        PredicateNode::Composite {
                            op: BooleanConnective::Or,
                            operands: kept,
                        }
                    } else {
                        PredicateNode::Empty
                    }
                }
                BooleanConnective::Not => {
                    assert!(operands.len() == 1);
                    let inner = &operands[0];
                    if Predicate::node_applies_to(inner, term_ok) {
                        let kept = Predicate::filter_node(inner, term_ok);
                        return PredicateNode::Composite {
                            op: BooleanConnective::Not,
                            operands: vec![kept],
                        };
                    }
                    PredicateNode::Empty
                }
            },
        }
    }

    fn to_sql(&self) -> String {
        Predicate::node_to_sql(&self.root)
    }

    fn node_to_sql(node: &PredicateNode) -> String {
        match node {
            PredicateNode::Empty => String::new(),
            PredicateNode::Term(term) => term.to_sql(),
            PredicateNode::Composite { op, operands } => {
                let op_str = match op {
                    BooleanConnective::And => "AND",
                    BooleanConnective::Or => "OR",
                    BooleanConnective::Not => "NOT",
                };
                let terms: Vec<String> = operands.iter().map(Predicate::node_to_sql).collect();
                match op {
                    BooleanConnective::Not => format!("{}({})", op_str, terms.join("")),
                    _ => terms.join(op_str),
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub struct Term {
    lhs: Expression,
    rhs: Expression,
    comparison_op: ComparisonOp,
}

#[derive(Clone, Debug)]
enum ComparisonOp {
    Equal,
    LessThan,
    GreaterThan,
    LessThanOrEqual,
    GreaterThanOrEqual,
    NotEqual,
}

impl Term {
    pub fn new(lhs: Expression, rhs: Expression) -> Self {
        Self {
            lhs,
            rhs,
            comparison_op: ComparisonOp::Equal,
        }
    }

    fn new_with_op(lhs: Expression, rhs: Expression, comparison_op: ComparisonOp) -> Self {
        Self {
            lhs,
            rhs,
            comparison_op,
        }
    }

    fn is_satisfied<S>(&self, scan: &S) -> Result<bool, Box<dyn Error>>
    where
        S: Scan,
    {
        let lhs = self.lhs.evaluate(scan)?;
        let rhs = self.rhs.evaluate(scan)?;

        match self.comparison_op {
            ComparisonOp::Equal => Ok(lhs == rhs),
            ComparisonOp::LessThan => Ok(lhs < rhs),
            ComparisonOp::GreaterThan => Ok(lhs > rhs),
            ComparisonOp::LessThanOrEqual => Ok(lhs <= rhs),
            ComparisonOp::GreaterThanOrEqual => Ok(lhs >= rhs),
            ComparisonOp::NotEqual => Ok(lhs != rhs),
        }
    }

    /// Calculates the reduction factor for the term
    fn reduction_factor(&self, plan: &Arc<dyn Plan>) -> usize {
        if self.lhs.is_field_name() && self.rhs.is_field_name() {
            let lhs_field = self.lhs.get_field_name().unwrap();
            let rhs_field = self.rhs.get_field_name().unwrap();
            return std::cmp::max(
                plan.distinct_values(lhs_field),
                plan.distinct_values(rhs_field),
            );
        }

        if self.lhs.is_field_name() {
            let lhs_field = self.lhs.get_field_name().unwrap();
            return plan.distinct_values(lhs_field);
        }

        if self.rhs.is_field_name() {
            let rhs_field = self.rhs.get_field_name().unwrap();
            return plan.distinct_values(rhs_field);
        }

        if self.lhs.get_constant_value().unwrap() == self.rhs.get_constant_value().unwrap() {
            return 1;
        }

        usize::MAX
    }

    /// Checks if the term equates with a constant value of the form "F=c"
    /// where F is the specified field and c is some constant
    fn equates_with_constant(&self, field_name: &str) -> Option<Constant> {
        if self.lhs.is_field_name()
            && (self.lhs.get_field_name().unwrap() == field_name)
            && !self.rhs.is_field_name()
        {
            return self.rhs.get_constant_value().cloned();
        } else if self.rhs.is_field_name()
            && (self.rhs.get_field_name().unwrap() == field_name)
            && !self.lhs.is_field_name()
        {
            return self.lhs.get_constant_value().cloned();
        }
        None
    }

    /// Checks if the term equates with a field name of the form "F=G"
    /// where F is the specified field and G is some other field
    fn equates_with_field(&self, field_name: &str) -> Option<String> {
        if self.lhs.is_field_name()
            && (self.lhs.get_field_name().unwrap() == field_name)
            && self.rhs.is_field_name()
        {
            return self.rhs.get_field_name().cloned();
        } else if self.rhs.is_field_name()
            && (self.rhs.get_field_name().unwrap() == field_name)
            && self.lhs.is_field_name()
        {
            return self.lhs.get_field_name().cloned();
        }
        None
    }

    /// Check that both sides of this expression apply to the provided [Schema]
    fn applies_to(&self, schema: &Schema) -> bool {
        self.lhs.applies_to(schema) && self.rhs.applies_to(schema)
    }

    fn to_sql(&self) -> String {
        let lhs_sql = self.lhs.to_sql();
        let rhs_sql = self.rhs.to_sql();
        let op_str = match self.comparison_op {
            ComparisonOp::Equal => "=",
            ComparisonOp::LessThan => "<",
            ComparisonOp::GreaterThan => ">",
            ComparisonOp::LessThanOrEqual => "<=",
            ComparisonOp::GreaterThanOrEqual => ">=",
            ComparisonOp::NotEqual => "<>",
        };
        format!("{lhs_sql} {op_str} {rhs_sql}")
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Expression {
    Constant(Constant),
    FieldName(String),
    BinaryOp {
        operator: BinaryOperator,
        left: Box<Expression>,
        right: Box<Expression>,
    },
}

impl Expression {
    fn evaluate<S: Scan>(&self, scan: &S) -> Result<Constant, Box<dyn Error>> {
        match self {
            Expression::Constant(constant) => Ok(constant.clone()),
            Expression::FieldName(field_name) => scan.get_value(field_name),
            Expression::BinaryOp {
                operator,
                left,
                right,
            } => {
                let left_val = left.evaluate(scan)?;
                let right_val = right.evaluate(scan)?;

                let left_int = match left_val {
                    Constant::Int(value) => value,
                    _ => return Err("Left operand must be an integer".into()),
                };
                let right_int = match right_val {
                    Constant::Int(value) => value,
                    _ => return Err("Right operand must be an integer".into()),
                };

                let result = match operator {
                    BinaryOperator::Add => left_int + right_int,
                    BinaryOperator::Subtract => left_int - right_int,
                    BinaryOperator::Divide => left_int / right_int,
                    BinaryOperator::Multiply => left_int * right_int,
                    BinaryOperator::Modulo => left_int % right_int,
                };

                Ok(Constant::Int(result))
            }
        }
    }

    /// Check if this expression applies to the given schema
    /// If the expression is of type [Expression::Constant] the schema will always apply
    fn applies_to(&self, schema: &Schema) -> bool {
        match self {
            Expression::Constant(_) => true,
            Expression::FieldName(field_name) => schema.fields.contains(field_name),
            _ => panic!("applies_to called for something that doesn't make sense"),
        }
    }

    fn is_field_name(&self) -> bool {
        matches!(self, Expression::FieldName(_))
    }

    fn get_field_name(&self) -> Option<&String> {
        match self {
            Expression::FieldName(name) => Some(name),
            _ => None,
        }
    }

    fn get_constant_value(&self) -> Option<&Constant> {
        match self {
            Expression::Constant(value) => Some(value),
            _ => None,
        }
    }

    fn to_sql(&self) -> String {
        match self {
            Expression::Constant(constant) => match constant {
                Constant::Int(value) => value.to_string(),
                Constant::String(string) => string.to_string(),
            },
            Expression::FieldName(field_name) => field_name.clone(),
            Expression::BinaryOp {
                operator,
                left,
                right,
            } => {
                let op_str = match operator {
                    BinaryOperator::Add => "+",
                    BinaryOperator::Subtract => "-",
                    BinaryOperator::Multiply => "*",
                    BinaryOperator::Divide => "/",
                    BinaryOperator::Modulo => "%",
                };
                format!("({} {} {})", left.to_sql(), op_str, right.to_sql())
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BinaryOperator {
    Add,
    Subtract,
    Divide,
    Multiply,
    Modulo,
}

// Metadata helpers are constructed once at boot and stay read-only during steady state.
pub struct MetadataManager {
    table_manager: Arc<TableManager>,
    view_manager: Arc<ViewManager>,
    index_manager: Arc<IndexManager>,
    stat_manager: Arc<StatManager>,
}

impl MetadataManager {
    pub fn new(is_new: bool, txn: Arc<Transaction>) -> Self {
        let table_manager = Arc::new(TableManager::new(is_new, Arc::clone(&txn)));
        let view_manager = Arc::new(ViewManager::new(
            is_new,
            Arc::clone(&table_manager),
            Arc::clone(&txn),
        ));
        let stat_manager = Arc::new(StatManager::new(Arc::clone(&table_manager)));
        let index_manager = Arc::new(IndexManager::new(
            is_new,
            Arc::clone(&table_manager),
            Arc::clone(&stat_manager),
            txn,
        ));
        Self {
            table_manager,
            view_manager,
            index_manager,
            stat_manager,
        }
    }

    fn create_table(&self, table_name: &str, schema: Schema, txn: Arc<Transaction>) {
        self.table_manager.create_table(table_name, &schema, txn);
    }

    pub fn get_layout(&self, table_name: &str, txn: Arc<Transaction>) -> Layout {
        self.table_manager.get_layout(table_name, txn)
    }

    fn create_view(&self, view_name: &str, view_def: &str, txn: Arc<Transaction>) {
        self.view_manager.create_view(view_name, view_def, txn);
    }

    pub fn get_view_def(&self, view_name: &str, txn: Arc<Transaction>) -> Option<String> {
        self.view_manager.get_view(view_name, txn)
    }

    fn create_index(
        &self,
        table_name: &str,
        index_name: &str,
        field_name: &str,
        txn: Arc<Transaction>,
    ) {
        debug!(
            "Creating index {} on table {} for field {}",
            index_name, table_name, field_name
        );
        self.index_manager
            .create_index(index_name, table_name, field_name, txn);
    }

    pub fn get_index_info(
        &self,
        table_name: &str,
        txn: Arc<Transaction>,
    ) -> HashMap<String, IndexInfo> {
        debug!("Fetching indices for table {}", table_name);
        self.index_manager.get_index_info(table_name, txn)
    }

    pub fn get_stat_info(
        &self,
        table_name: &str,
        layout: Layout,
        txn: Arc<Transaction>,
    ) -> StatInfo {
        self.stat_manager.get_stat_info(table_name, layout, txn)
    }

    pub fn get_table_names(&self, txn: &Arc<Transaction>) -> Result<Vec<String>, Box<dyn Error>> {
        self.table_manager.get_table_names(Arc::clone(txn))
    }
}

#[cfg(test)]
mod metadata_manager_tests {
    use super::UpdateScan;
    use crate::{
        test_utils::generate_random_number, FieldType, MetadataManager, Schema, SimpleDB,
        TableScan, Transaction,
    };
    use std::sync::Arc;

    #[test]
    fn test_metadata_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let tx = db.new_tx();
        let mdm = MetadataManager::new(true, Arc::clone(&tx));

        // Part 1: Table Metadata
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);

        let table_name = "MyTable";
        mdm.create_table(table_name, schema.clone(), Arc::clone(&tx));
        let layout = mdm.get_layout(table_name, Arc::clone(&tx));

        println!("MyTable has slot size {}", layout.slot_size);
        // Verify slot size
        let expected_slot_size = 4 + 4 + (4 + 9); // header + int + (string length prefix + chars)
        assert_eq!(layout.slot_size, expected_slot_size);

        // Verify schema fields
        println!("Its fields are:");
        for field in &layout.schema.fields {
            let field_info = layout.schema.info.get(field).unwrap();
            let type_str = match field_info.field_type {
                FieldType::Int => "int".to_string(),
                FieldType::String => format!("varchar({})", field_info.length),
            };
            println!("{field}: {type_str}");

            // Assert field properties
            match field.as_str() {
                "A" => assert_eq!(field_info.field_type, FieldType::Int),
                "B" => {
                    assert_eq!(field_info.field_type, FieldType::String);
                    assert_eq!(field_info.length, 9);
                }
                _ => panic!("Unexpected field: {field}"),
            }
        }

        // Part 2: Statistics Metadata
        {
            let mut table_scan = TableScan::new(Arc::clone(&tx), layout.clone(), table_name);
            for _ in 0..50 {
                table_scan.insert().unwrap();
                let n = (generate_random_number() % 50) + 1;
                table_scan.set_int("A", n as i32).unwrap();
                table_scan.set_string("B", format!("rec{n}")).unwrap();
            }

            let stat_info = mdm.get_stat_info(table_name, layout.clone(), Arc::clone(&tx));
            println!("B(MyTable) = {}", stat_info.num_blocks);
            println!("R(MyTable) = {}", stat_info.num_blocks);
            println!("V(MyTable,A) = {}", stat_info.distinct_values("A"));
            println!("V(MyTable,B) = {}", stat_info.distinct_values("B"));

            // Add assertions for statistics
            assert!(stat_info.num_blocks > 0);
            assert_eq!(stat_info.num_records, 50);
            assert!(stat_info.distinct_values("A") <= 50);
            assert!(stat_info.distinct_values("B") <= 50);
        }

        // Part 3: View Metadata
        let view_def = "select B from MyTable where A = 1";
        mdm.create_view("viewA", view_def, Arc::clone(&tx));
        let retrieved_view = mdm.get_view_def("viewA", Arc::clone(&tx));
        println!("View def = {retrieved_view:?}");
        assert_eq!(retrieved_view, Some(view_def.to_string()));

        // Part 4: Index Metadata
        mdm.create_index(table_name, "indexA", "A", Arc::clone(&tx));
        mdm.create_index(table_name, "indexB", "B", Arc::clone(&tx));

        let idx_map = mdm.get_index_info(table_name, Arc::clone(&tx));

        // Verify index A
        let idx_a = idx_map.get("A").expect("Index A not found");
        println!("B(indexA) = {}", idx_a.blocks_accessed());
        println!("R(indexA) = {}", idx_a.records_output());
        println!("V(indexA,A) = {}", idx_a.distinct_values("A"));
        println!("V(indexA,B) = {}", idx_a.distinct_values("B"));

        assert_eq!(idx_a.records_output(), 2);
        assert!(idx_a.distinct_values("A") == 1); //  we have an index on A

        // Verify index B
        let idx_b = idx_map.get("B").expect("Index B not found");
        println!("B(indexB) = {}", idx_b.blocks_accessed());
        println!("R(indexB) = {}", idx_b.records_output());
        println!("V(indexB,A) = {}", idx_b.distinct_values("A"));
        println!("V(indexB,B) = {}", idx_b.distinct_values("B"));

        assert_eq!(idx_b.records_output(), 2);
        assert!(idx_b.distinct_values("B") == 1); //  we have an index on B

        tx.commit().unwrap();
    }

    #[test]
    fn stat_manager_concurrent_access() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let setup_txn = db.new_tx();
        let mdm = Arc::new(MetadataManager::new(true, Arc::clone(&setup_txn)));

        let table_name = "stat_concurrent";
        let mut schema = Schema::new();
        schema.add_int_field("val");
        mdm.create_table(table_name, schema.clone(), Arc::clone(&setup_txn));

        let layout = mdm.get_layout(table_name, Arc::clone(&setup_txn));
        {
            let mut table_scan = TableScan::new(Arc::clone(&setup_txn), layout.clone(), table_name);
            for i in 0..20 {
                table_scan.insert().unwrap();
                table_scan.set_int("val", i).unwrap();
            }
        }
        setup_txn.commit().unwrap();

        let layout_txn = db.new_tx();
        let layout = mdm.get_layout(table_name, Arc::clone(&layout_txn));
        layout_txn.commit().unwrap();

        // Prime the cache so concurrent readers hit the fast path
        let prime_txn = db.new_tx();
        mdm.get_stat_info(table_name, layout.clone(), Arc::clone(&prime_txn));
        prime_txn.commit().unwrap();

        let file_manager = Arc::clone(&db.file_manager);
        let log_manager = Arc::clone(&db.log_manager);
        let buffer_manager = Arc::clone(&db.buffer_manager);
        let lock_table = Arc::clone(&db.lock_table);

        let mut handles = Vec::new();
        for _ in 0..8 {
            let mdm_clone = Arc::clone(&mdm);
            let layout_clone = layout.clone();
            let fm = Arc::clone(&file_manager);
            let lm = Arc::clone(&log_manager);
            let bm = Arc::clone(&buffer_manager);
            let lt = Arc::clone(&lock_table);
            handles.push(std::thread::spawn(move || {
                let txn = Arc::new(Transaction::new(fm, lm, bm, lt));
                for _ in 0..10 {
                    let stats =
                        mdm_clone.get_stat_info(table_name, layout_clone.clone(), Arc::clone(&txn));
                    assert_eq!(stats.num_records, 20);
                }
                txn.commit().unwrap();
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }
    }
}

struct IndexManager {
    layout: Layout,
    table_manager: Arc<TableManager>,
    stat_manager: Arc<StatManager>,
}

impl IndexManager {
    const INDEX_CAT_TBL_NAME: &str = "index_cat";
    const INDEX_COL_NAME: &str = "index_name";
    const TABLE_COL_NAME: &str = "table_name";
    const TABLE_FIELD_NAME: &str = "field_name";

    pub fn new(
        is_new: bool,
        table_manager: Arc<TableManager>,
        stat_manager: Arc<StatManager>,
        txn: Arc<Transaction>,
    ) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema.add_string_field(Self::INDEX_COL_NAME, TableManager::MAX_NAME_LENGTH);
            schema.add_string_field(Self::TABLE_COL_NAME, TableManager::MAX_NAME_LENGTH);
            schema.add_string_field(Self::TABLE_FIELD_NAME, TableManager::MAX_NAME_LENGTH);
            table_manager.create_table(Self::INDEX_CAT_TBL_NAME, &schema, Arc::clone(&txn));
        }
        let layout = table_manager.get_layout(Self::INDEX_CAT_TBL_NAME, txn);
        Self {
            layout,
            table_manager,
            stat_manager,
        }
    }

    fn create_index(
        &self,
        index_name: &str,
        table_name: &str,
        field_name: &str,
        txn: Arc<Transaction>,
    ) {
        let mut table_scan = TableScan::new(txn, self.layout.clone(), Self::INDEX_CAT_TBL_NAME);
        table_scan.insert().unwrap();
        table_scan
            .set_string(Self::INDEX_COL_NAME, index_name.to_string())
            .unwrap();
        table_scan
            .set_string(Self::TABLE_COL_NAME, table_name.to_string())
            .unwrap();
        table_scan
            .set_string(Self::TABLE_FIELD_NAME, field_name.to_string())
            .unwrap();
    }

    fn get_index_info(
        &self,
        table_name: &str,
        txn: Arc<Transaction>,
    ) -> HashMap<String, IndexInfo> {
        let mut hash_map = HashMap::new();
        let mut table_scan = TableScan::new(
            Arc::clone(&txn),
            self.layout.clone(),
            Self::INDEX_CAT_TBL_NAME,
        );
        while table_scan.next().is_some() {
            if table_scan.get_string(Self::TABLE_COL_NAME).unwrap() == table_name {
                let field_name = table_scan.get_string(Self::TABLE_FIELD_NAME).unwrap();
                let index_name = table_scan.get_string(Self::INDEX_COL_NAME).unwrap();
                let layout = self.table_manager.get_layout(table_name, Arc::clone(&txn));
                let stat_info =
                    self.stat_manager
                        .get_stat_info(table_name, layout.clone(), Arc::clone(&txn));
                let index_info = IndexInfo::new(
                    &index_name,
                    &field_name,
                    Arc::clone(&txn),
                    layout.schema,
                    stat_info,
                );
                hash_map.insert(field_name, index_info);
            }
        }
        hash_map
    }
}

#[derive(Debug, Clone)]
pub struct IndexInfo {
    index_name: String,
    field_name: String,
    txn: Arc<Transaction>,
    table_schema: Schema,
    index_layout: Layout,
    stat_info: StatInfo,
}

impl std::fmt::Display for IndexInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "IndexInfo(name={}, field={})",
            self.index_name, self.field_name
        )
    }
}

impl IndexInfo {
    const BLOCK_NUM_FIELD: &str = "block"; //   the block number
    const ID_FIELD: &str = "id"; //  the record id (slot number)
    const DATA_FIELD: &str = "dataval"; //  the data field
    pub fn new(
        index_name: &str,
        field_name: &str,
        txn: Arc<Transaction>,
        table_schema: Schema,
        stat_info: StatInfo,
    ) -> Self {
        //  Construct the schema for the index
        let mut schema = Schema::new();
        schema.add_int_field(Self::BLOCK_NUM_FIELD);
        schema.add_int_field(Self::ID_FIELD);
        match table_schema.info.get(field_name).unwrap().field_type {
            FieldType::Int => {
                schema.add_int_field(Self::DATA_FIELD);
            }
            FieldType::String => {
                let field_length = table_schema.info.get(field_name).unwrap().length;
                schema.add_string_field(Self::DATA_FIELD, field_length);
            }
        };
        let index_layout = Layout::new(schema);
        Self {
            index_name: index_name.to_string(),
            field_name: field_name.to_string(),
            txn,
            table_schema,
            index_layout,
            stat_info,
        }
    }

    pub fn open(&self) -> impl Index {
        // HashIndex::new(
        //     Arc::clone(&self.txn),
        //     &self.index_name,
        //     self.index_layout.clone(),
        // )
        BTreeIndex::new(
            Arc::clone(&self.txn),
            &self.index_name,
            self.index_layout.clone(),
        )
        .unwrap()
    }

    /// This function returns the number of blocks that would need to be searched for this index on a specific field
    pub fn blocks_accessed(&self) -> usize {
        let records_per_block = self.txn.block_size() / self.index_layout.slot_size;
        let num_blocks = self.stat_info.num_records / records_per_block;
        HashIndex::search_cost(num_blocks)
    }

    /// This function returns the number of records that we would expect to get when using this index on a specific field
    pub fn records_output(&self) -> usize {
        self.stat_info.num_records / self.stat_info.distinct_values(&self.field_name)
    }

    /// This function returns the number of distinct values for a specific field in this index
    pub fn distinct_values(&self, field_name: &str) -> usize {
        if self.field_name == field_name {
            1
        } else {
            self.stat_info.distinct_values(&self.field_name)
        }
    }

    /// Returns the table schema for this index
    pub fn table_schema(&self) -> &Schema {
        &self.table_schema
    }

    /// Returns statistics for this index
    pub fn stat_info(&self) -> &StatInfo {
        &self.stat_info
    }
}

pub struct HashIndex {
    txn: Arc<Transaction>,
    index_name: String,
    layout: Layout,
    search_key: Option<Constant>,
    table_scan: Option<TableScan>,
}

impl HashIndex {
    const NUM_BUCKETS: usize = 100;

    pub fn new(txn: Arc<Transaction>, index_name: &str, layout: Layout) -> Self {
        Self {
            txn,
            index_name: index_name.to_string(),
            layout,
            search_key: None,
            table_scan: None,
        }
    }

    pub fn search_cost(num_blocks: usize) -> usize {
        num_blocks / Self::NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&mut self, search_key: &Constant) {
        self.search_key = Some(search_key.clone());
        let mut hasher = DefaultHasher::new();
        search_key.hash(&mut hasher);
        let hash = hasher.finish() as usize;
        let bucket = hash % Self::NUM_BUCKETS;
        let table_name = format!("{}_{}", self.index_name, bucket);
        let table_scan = TableScan::new(Arc::clone(&self.txn), self.layout.clone(), &table_name);
        self.table_scan = Some(table_scan);
    }

    fn next(&mut self) -> bool {
        let table_scan = self.table_scan.as_mut().unwrap();
        while table_scan.next().is_some() {
            if table_scan.get_value(IndexInfo::DATA_FIELD).unwrap()
                == *self.search_key.as_ref().unwrap()
            {
                return true;
            }
        }
        false
    }

    fn get_data_rid(&self) -> RID {
        let table_scan = self.table_scan.as_ref().unwrap();
        let block_num = table_scan.get_int(IndexInfo::BLOCK_NUM_FIELD).unwrap();
        let id = table_scan.get_int(IndexInfo::ID_FIELD).unwrap();
        RID {
            block_num: block_num as usize,
            slot: id as usize,
        }
    }

    fn insert(&mut self, data_val: &Constant, data_rid: &RID) {
        self.before_first(data_val);
        let table_scan = self.table_scan.as_mut().unwrap();
        table_scan.insert().unwrap();
        table_scan
            .set_int(IndexInfo::BLOCK_NUM_FIELD, data_rid.block_num as i32)
            .unwrap();
        table_scan
            .set_int(IndexInfo::ID_FIELD, data_rid.slot as i32)
            .unwrap();
        table_scan
            .set_value(IndexInfo::DATA_FIELD, data_val.clone())
            .unwrap();
    }

    fn delete(&mut self, data_val: &Constant, data_rid: &RID) {
        self.before_first(data_val);
        while self.next() {
            if *data_rid == self.get_data_rid() {
                self.table_scan.as_mut().unwrap().delete().unwrap();
                return;
            }
        }
    }
}

/// Interface for traversing and modifying an index
pub trait Index {
    /// Position the index before the first record having the specified search key
    fn before_first(&mut self, search_key: &Constant);

    /// Move to the next record having the search key specified in before_first
    /// Returns false if there are no more index records with that search key
    fn next(&mut self) -> bool;

    /// Get the RID stored in the current index record
    fn get_data_rid(&self) -> RID;

    /// Insert an index record with the specified value and RID
    fn insert(&mut self, data_val: &Constant, data_rid: &RID);

    /// Delete the index record with the specified value and RID
    fn delete(&mut self, data_val: &Constant, data_rid: &RID);
}

struct StatState {
    table_stats: HashMap<String, StatInfo>,
    num_calls: usize,
}

struct StatManager {
    table_manager: Arc<TableManager>,
    state: Mutex<StatState>,
}

impl StatManager {
    pub fn new(table_manager: Arc<TableManager>) -> Self {
        Self {
            table_manager,
            state: Mutex::new(StatState {
                table_stats: HashMap::new(),
                num_calls: 0,
            }),
        }
    }

    /// Returns the statistics for a given table
    /// Refreshes all stats for all tables based on a counter
    fn get_stat_info(&self, table_name: &str, layout: Layout, txn: Arc<Transaction>) -> StatInfo {
        debug!("getting stat info for {}", table_name);
        {
            let mut state = self.state.lock().unwrap();
            state.num_calls += 1;
            if state.num_calls > 100 {
                self.refresh_stats_inner(&mut state, Arc::clone(&txn));
            }

            if let Some(stats) = state.table_stats.get(table_name) {
                debug!("found table stats {:?}", stats);
                return stats.clone();
            }
        }

        debug!("going to calculate table stats");
        let table_stats = self.calculate_table_stats(table_name, layout, Arc::clone(&txn));
        let mut state = self.state.lock().unwrap();
        debug!("table stats {:?}", table_stats);
        state
            .table_stats
            .entry(table_name.to_string())
            .or_insert_with(|| table_stats.clone())
            .clone()
    }

    /// Refreshes the statistics for all tables in the database
    fn refresh_stats_inner(&self, state: &mut StatState, txn: Arc<Transaction>) {
        state.table_stats.clear();
        let table_catalog_layout = self
            .table_manager
            .get_layout(TableManager::TABLE_CAT_TABLE_NAME, Arc::clone(&txn));
        let mut table_scan = TableScan::new(
            Arc::clone(&txn),
            table_catalog_layout,
            TableManager::TABLE_CAT_TABLE_NAME,
        );
        while table_scan.next().is_some() {
            let table_name = table_scan.get_string(TableManager::TABLE_NAME_COL).unwrap();
            let layout = self.table_manager.get_layout(&table_name, Arc::clone(&txn));
            let table_stats = self.calculate_table_stats(&table_name, layout, Arc::clone(&txn));
            state.table_stats.insert(table_name, table_stats);
        }
        state.num_calls = 0;
    }

    /// Calculates the [`StatInfo`] for a given table
    fn calculate_table_stats(
        &self,
        table_name: &str,
        layout: Layout,
        txn: Arc<Transaction>,
    ) -> StatInfo {
        debug!("calculating table stats for {}", table_name);
        let mut table_scan = TableScan::new(txn, layout, table_name);
        let mut num_rec = 0;
        let mut num_blocks = 0;
        while table_scan.next().is_some() {
            num_rec += 1;
            num_blocks = table_scan
                .record_page
                .as_ref()
                .unwrap()
                .handle
                .block_id()
                .block_num
                + 1;
        }
        StatInfo {
            num_blocks,
            num_records: num_rec,
        }
    }
}

#[derive(Clone, Debug)]
pub struct StatInfo {
    pub num_blocks: usize,
    pub num_records: usize,
}

impl StatInfo {
    pub fn new(num_block: usize, num_records: usize) -> Self {
        Self {
            num_blocks: num_block,
            num_records,
        }
    }

    fn distinct_values(&self, _field_name: &str) -> usize {
        1 + (self.num_records / 3)
    }
}

struct ViewManager {
    table_manager: Arc<TableManager>,
}

impl ViewManager {
    const VIEW_DEF_MAX_LENGTH: usize = 100;
    const VIEW_MANAGER_TABLE_NAME: &str = "view_catalog";
    const VIEW_NAME_COL: &str = "view_name";
    const VIEW_DEF_COL: &str = "view_col";

    pub fn new(is_new: bool, table_manager: Arc<TableManager>, txn: Arc<Transaction>) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema.add_string_field(Self::VIEW_NAME_COL, TableManager::MAX_NAME_LENGTH);
            schema.add_string_field(Self::VIEW_DEF_COL, Self::VIEW_DEF_MAX_LENGTH);
            table_manager.create_table(Self::VIEW_MANAGER_TABLE_NAME, &schema, txn);
        }
        ViewManager { table_manager }
    }

    /// Creates a new view in the view catalog
    fn create_view(&self, view_name: &str, view_def: &str, txn: Arc<Transaction>) {
        let layout = self
            .table_manager
            .get_layout(Self::VIEW_MANAGER_TABLE_NAME, Arc::clone(&txn));
        let mut table_scan = TableScan::new(txn, layout, Self::VIEW_MANAGER_TABLE_NAME);
        table_scan.insert().unwrap();
        table_scan
            .set_string(Self::VIEW_NAME_COL, view_name.to_string())
            .unwrap();
        table_scan
            .set_string(Self::VIEW_DEF_COL, view_def.to_string())
            .unwrap();
    }

    /// Returns the view definition for a given view name
    pub fn get_view(&self, view_name: &str, txn: Arc<Transaction>) -> Option<String> {
        let layout = self
            .table_manager
            .get_layout(Self::VIEW_MANAGER_TABLE_NAME, Arc::clone(&txn));
        let mut table_scan = TableScan::new(txn, layout, Self::VIEW_MANAGER_TABLE_NAME);
        while let Some(_) = table_scan.next() {
            if view_name == table_scan.get_string(Self::VIEW_NAME_COL).unwrap() {
                return Some(table_scan.get_string(Self::VIEW_DEF_COL).unwrap());
            }
        }
        None
    }
}

struct TableManager {
    table_catalog_layout: Layout,
    field_catalog_layout: Layout,
}

impl TableManager {
    const MAX_NAME_LENGTH: usize = 16; //  the max length for a table name (TODO: Do other databases use variable name lengths for tables?)
    const TABLE_CAT_TABLE_NAME: &str = "table_catalog";
    const FIELD_CAT_TABLE_NAME: &str = "field_catalog";

    // Table catalog columns
    const TABLE_NAME_COL: &str = "table_name";
    const SLOT_SIZE_COL: &str = "slot_size";

    // Field catalog columns
    const FIELD_NAME_COL: &str = "field_name";
    const FIELD_TYPE_COL: &str = "type";
    const FIELD_LENGTH_COL: &str = "length";
    const FIELD_OFFSET_COL: &str = "offset";

    pub fn new(is_new: bool, tx: Arc<Transaction>) -> Self {
        //  Create the table catalog layout
        let mut table_cat_schema = Schema::new();
        table_cat_schema.add_string_field(Self::TABLE_NAME_COL, Self::MAX_NAME_LENGTH);
        table_cat_schema.add_int_field(Self::SLOT_SIZE_COL);
        let table_cat_layout = Layout::new(table_cat_schema.clone());

        //  Create the field catalog layout
        let mut field_cat_schema = Schema::new();
        field_cat_schema.add_string_field(Self::TABLE_NAME_COL, Self::MAX_NAME_LENGTH);
        field_cat_schema.add_string_field(Self::FIELD_NAME_COL, Self::MAX_NAME_LENGTH);
        field_cat_schema.add_int_field(Self::FIELD_TYPE_COL);
        field_cat_schema.add_int_field(Self::FIELD_LENGTH_COL);
        field_cat_schema.add_int_field(Self::FIELD_OFFSET_COL);
        let field_cat_layout = Layout::new(field_cat_schema.clone());

        let table_mgr = Self {
            table_catalog_layout: table_cat_layout,
            field_catalog_layout: field_cat_layout,
        };

        if is_new {
            //  Create both tables
            table_mgr.create_table(
                Self::TABLE_CAT_TABLE_NAME,
                &table_cat_schema,
                Arc::clone(&tx),
            );
            table_mgr.create_table(Self::FIELD_CAT_TABLE_NAME, &field_cat_schema, tx);
        }

        table_mgr
    }

    /// This method will accept a [`Schema`] for a table that is being created as part of a txn and
    /// create the relevant metadata in the catalog tables
    fn create_table(&self, table_name: &str, schema: &Schema, tx: Arc<Transaction>) {
        let layout = Layout::new(schema.clone());

        //  insert the record for the table name and slot size
        {
            let mut table_scan = TableScan::new(
                Arc::clone(&tx),
                self.table_catalog_layout.clone(),
                Self::TABLE_CAT_TABLE_NAME,
            );
            table_scan.insert().unwrap();
            table_scan
                .set_string(Self::TABLE_NAME_COL, table_name.to_string())
                .unwrap();
            table_scan
                .set_int(Self::SLOT_SIZE_COL, layout.slot_size as i32)
                .unwrap();
        }

        // insert the records for the fields into the field catalog table
        {
            let mut table_scan = TableScan::new(
                tx,
                self.field_catalog_layout.clone(),
                Self::FIELD_CAT_TABLE_NAME,
            );
            for field in &schema.fields {
                table_scan.insert().unwrap();
                table_scan
                    .set_string(Self::TABLE_NAME_COL, table_name.to_string())
                    .unwrap();
                table_scan
                    .set_string(Self::FIELD_NAME_COL, field.to_string())
                    .unwrap();
                let field_info = schema.info.get(field).unwrap();
                table_scan
                    .set_int(Self::FIELD_TYPE_COL, field_info.field_type as i32)
                    .unwrap();
                table_scan
                    .set_int(Self::FIELD_LENGTH_COL, field_info.length as i32)
                    .unwrap();
                table_scan
                    .set_int(Self::FIELD_OFFSET_COL, layout.offset(field).unwrap() as i32)
                    .unwrap();
            }
        }
    }

    /// Return the physical [`Layout`] for a specific table defined in the table catalog metadata
    fn get_layout(&self, table_name: &str, tx: Arc<Transaction>) -> Layout {
        //  Get the slot size of the table
        // let slot_size = {
        //     let mut table_scan = TableScan::new(
        //         Arc::clone(&tx),
        //         self.table_catalog_layout.clone(),
        //         Self::TABLE_CAT_TABLE_NAME,
        //     );
        //     let mut slot_size = None;
        //     while let Some(_) = table_scan.next() {
        //         if table_name == table_scan.get_string(Self::TABLE_NAME_COL).unwrap() {
        //             slot_size = Some(table_scan.get_int(Self::SLOT_SIZE_COL));
        //         }
        //     }
        //     slot_size
        // };

        //  Construct the schema from the table so the layout can be created
        let schema = {
            let mut table_scan = TableScan::new(
                Arc::clone(&tx),
                self.field_catalog_layout.clone(),
                Self::FIELD_CAT_TABLE_NAME,
            );
            let mut schema = Schema::new();
            while let Some(_) = table_scan.next() {
                if table_name == table_scan.get_string(Self::TABLE_NAME_COL).unwrap() {
                    let field_name = table_scan.get_string(Self::FIELD_NAME_COL).unwrap();
                    let field_type: FieldType =
                        table_scan.get_int(Self::FIELD_TYPE_COL).unwrap().into();
                    let field_length = table_scan.get_int(Self::FIELD_LENGTH_COL).unwrap() as usize;
                    schema.add_field(&field_name, field_type, field_length);
                }
            }
            schema
        };
        Layout::new(schema)
    }

    /// Return a list of all table names in the database
    fn get_table_names(&self, tx: Arc<Transaction>) -> Result<Vec<String>, Box<dyn Error>> {
        let mut table_scan = TableScan::new(
            tx,
            self.table_catalog_layout.clone(),
            Self::TABLE_CAT_TABLE_NAME,
        );
        let mut tables = Vec::new();
        while let Some(_) = table_scan.next() {
            let table_name = table_scan.get_string(Self::TABLE_NAME_COL)?;
            // Skip internal catalog tables
            if table_name != Self::TABLE_CAT_TABLE_NAME && table_name != Self::FIELD_CAT_TABLE_NAME
            {
                tables.push(table_name);
            }
        }
        Ok(tables)
    }
}

#[cfg(test)]
mod table_manager_tests {
    use std::sync::Arc;

    use crate::{FieldType, Schema, SimpleDB, TableManager};

    #[test]
    fn test_table_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let tx = db.new_tx();
        let table_manager = TableManager::new(true, Arc::clone(&tx));

        // Create schema
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 9);

        // Create table and get its layout
        let table_name = "MyTable";
        table_manager.create_table(table_name, &schema, Arc::clone(&tx));
        let layout = table_manager.get_layout(table_name, Arc::clone(&tx));

        // Verify slot size
        println!("MyTable has slot size {}", layout.slot_size);
        // Assert slot size matches expected (calculate expected size based on schema)
        let expected_slot_size = 4 + // header
                            4 + // int field
                            (4 + 9); // string field (length prefix + chars)
        assert_eq!(layout.slot_size, expected_slot_size);

        // Verify schema fields
        println!("Its fields are:");
        for field in &layout.schema.fields {
            let field_info = layout.schema.info.get(field).unwrap();
            let type_str = match field_info.field_type {
                FieldType::Int => "int".to_string(),
                FieldType::String => format!("varchar({})", field_info.length),
            };
            println!("{field}: {type_str}");

            // Assert field properties
            match field.as_str() {
                "A" => {
                    assert_eq!(field_info.field_type, FieldType::Int);
                }
                "B" => {
                    assert_eq!(field_info.field_type, FieldType::String);
                    assert_eq!(field_info.length, 9);
                }
                _ => panic!("Unexpected field: {field}"),
            }
        }

        // Verify field count
        assert_eq!(layout.schema.fields.len(), 2);

        // Verify field offsets
        assert_eq!(layout.offset("A").unwrap(), 4); // First field after slot header
        assert_eq!(layout.offset("B").unwrap(), 8); // After int field

        tx.commit().unwrap();
    }
}

#[derive(Clone)]
pub struct TableScan {
    txn: Arc<Transaction>,
    layout: Layout,
    file_name: String,
    record_page: Option<RecordPage>,
    current_slot: Option<usize>,
    table_name: String,
}

impl TableScan {
    pub fn new(txn: Arc<Transaction>, layout: Layout, table_name: &str) -> Self {
        debug!("Creating table scan for {}", table_name);
        let file_name = format!("{table_name}.tbl");
        let mut scan = Self {
            txn,
            layout,
            file_name: file_name.to_string(),
            record_page: None,
            current_slot: None,
            table_name: table_name.to_string(),
        };

        if scan.txn.size(&file_name) == 0 {
            debug!(
                "TableScan for {} is empty, allocating new block",
                table_name
            );
            scan.move_to_new_block();
        } else {
            debug!(
                "TableScan for {} is not empty, moving to block 0",
                table_name
            );
            scan.move_to_block(0);
        }
        scan
    }

    /// Moves the [`RecordPage`] on this [`TableScan`] to a specific block number
    pub fn move_to_block(&mut self, block_num: usize) {
        let block_id = BlockId::new(self.file_name.clone(), block_num);
        let record_page = RecordPage::new(Arc::clone(&self.txn), block_id, self.layout.clone());
        self.current_slot = None;
        self.record_page = Some(record_page);
    }

    /// Allocates a new [`BlockId`] to the underlying file and moves the [`RecordPage`] there
    fn move_to_new_block(&mut self) {
        let block = self.txn.append(&self.file_name);
        let record_page = RecordPage::new(Arc::clone(&self.txn), block, self.layout.clone());
        record_page.format();
        self.current_slot = None;
        self.record_page = Some(record_page);
    }

    /// Checks if the [`TableScan`] is at the last block in the file
    fn at_last_block(&self) -> bool {
        self.record_page
            .as_ref()
            .unwrap()
            .handle
            .block_id()
            .block_num
            == self.txn.size(&self.file_name) - 1
    }

    /// Moves the [`RecordPage`] to the start of the file
    pub fn move_to_start(&mut self) {
        self.move_to_block(0);
    }

    pub fn move_to_row_id(&mut self, row_id: RID) {
        let block_id = BlockId::new(self.file_name.clone(), row_id.block_num);
        self.record_page = Some(RecordPage::new(
            Arc::clone(&self.txn),
            block_id,
            self.layout.clone(),
        ));
        self.current_slot = Some(row_id.slot);
    }
}

/// An iterator over the records in the table
impl Iterator for TableScan {
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        debug!("Calling next on TableScan for {}", self.table_name);
        loop {
            //  Check if there is a record page currently
            if let Some(record_page) = &self.record_page {
                let next_slot = match self.current_slot {
                    None => record_page.iter_used_slots().next(),
                    Some(slot) => record_page.iter_used_slots().find(|s| *s > slot),
                };

                if let Some(slot) = next_slot {
                    self.current_slot = Some(slot);
                    return Some(Ok(()));
                }
            }

            if self.at_last_block() {
                return None;
            }
            self.move_to_block(
                self.record_page
                    .as_ref()
                    .unwrap()
                    .handle
                    .block_id()
                    .block_num
                    + 1,
            );
        }
    }
}

impl Scan for TableScan {
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        Ok(self
            .record_page
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "No record page set when calling get_int for {}",
                    self.table_name
                )
            })
            .and_then(|page| {
                self.current_slot
                    .ok_or_else(|| {
                        format!(
                            "No current slot set when calling get_int for {}",
                            self.table_name
                        )
                    })
                    .map(|slot| page.get_int(slot, field_name))
            })?)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        Ok(self
            .record_page
            .as_ref()
            .ok_or_else(|| {
                format!(
                    "No record page set when calling get_string for {}",
                    self.table_name
                )
            })
            .and_then(|page| {
                self.current_slot
                    .ok_or_else(|| {
                        format!(
                            "No current slot set when calling get_string for {}",
                            self.table_name
                        )
                    })
                    .map(|slot| page.get_string(slot, field_name))
            })?)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        match self.layout.schema.info.get(field_name).unwrap().field_type {
            FieldType::Int => Ok(Constant::Int(self.get_int(field_name)?)),
            FieldType::String => Ok(Constant::String(self.get_string(field_name)?)),
        }
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self.layout.schema.fields.contains(&field_name.to_string()))
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.move_to_block(0);
        Ok(())
    }
}

impl UpdateScan for TableScan {
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>> {
        self.record_page.as_ref().unwrap().set_int(
            *self.current_slot.as_ref().unwrap(),
            field_name,
            value,
        );
        Ok(())
    }

    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>> {
        self.record_page.as_ref().unwrap().set_string(
            *self.current_slot.as_ref().unwrap(),
            field_name,
            &value,
        );
        Ok(())
    }

    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>> {
        match self.layout.schema.info.get(field_name).unwrap().field_type {
            FieldType::Int => self.set_int(field_name, value.as_int())?,
            FieldType::String => self.set_string(field_name, value.as_str().to_string())?,
        }
        Ok(())
    }

    fn insert(&mut self) -> Result<(), Box<dyn Error>> {
        let mut iterations = 0;
        loop {
            //  sanity check in case i runs into an infinite loop
            iterations += 1;
            assert!(
                iterations <= 10000,
                "Table scan insert failed for {iterations} iterations"
            );
            match self
                .record_page
                .as_ref()
                .unwrap()
                .insert_after(self.current_slot)
            {
                Ok(slot) => {
                    self.current_slot = Some(slot);
                    break;
                }
                Err(_) => {
                    if self.at_last_block() {
                        self.move_to_new_block();
                    } else {
                        self.move_to_block(
                            self.record_page
                                .as_ref()
                                .unwrap()
                                .handle
                                .block_id()
                                .block_num
                                + 1,
                        );
                    }
                    continue;
                }
            }
        }
        Ok(())
    }

    fn delete(&mut self) -> Result<(), Box<dyn Error>> {
        self.record_page
            .as_ref()
            .unwrap()
            .delete(*self.current_slot.as_ref().unwrap());
        Ok(())
    }

    fn get_rid(&self) -> Result<RID, Box<dyn Error>> {
        Ok(RID::new(
            self.record_page
                .as_ref()
                .unwrap()
                .handle
                .block_id()
                .block_num,
            *self.current_slot.as_ref().unwrap(),
        ))
    }

    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>> {
        let block_id = BlockId::new(self.file_name.clone(), rid.block_num);
        self.record_page = Some(RecordPage::new(
            Arc::clone(&self.txn),
            block_id,
            self.layout.clone(),
        ));
        self.current_slot = Some(rid.slot);
        Ok(())
    }
}

pub trait UpdateScan: Scan + Any {
    fn set_int(&self, field_name: &str, value: i32) -> Result<(), Box<dyn Error>>;
    fn set_string(&self, field_name: &str, value: String) -> Result<(), Box<dyn Error>>;
    fn set_value(&self, field_name: &str, value: Constant) -> Result<(), Box<dyn Error>>;
    fn insert(&mut self) -> Result<(), Box<dyn Error>>;
    fn delete(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_rid(&self) -> Result<RID, Box<dyn Error>>;
    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>>;
}

pub trait Scan: Iterator<Item = Result<(), Box<dyn Error>>> {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>>;
    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>>;
    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>>;
    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>>;
}

#[cfg(test)]
mod table_scan_tests {
    use super::UpdateScan;

    use crate::{test_utils::generate_random_number, Layout, Scan, Schema, SimpleDB, TableScan};

    #[test]
    fn table_scan_test() {
        let (db, _test_dir) = SimpleDB::new_for_test(4, 5000);
        let txn = db.new_tx();

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        dbg!("Inserting a bunch of records into the table");
        let mut inserted_count = 0;
        let mut table_scan = TableScan::new(txn, layout, "table");
        for _ in 0..100 {
            table_scan.insert().unwrap();
            let number = (generate_random_number() % 100) + 1;
            table_scan.set_int("A", number as i32).unwrap();
            table_scan.set_string("B", format!("rec{number}")).unwrap();
            dbg!(format!("Inserting number {}", number));
            inserted_count += 1;
        }
        dbg!(format!("Inserted {} records", inserted_count));

        dbg!("Deleting a bunch of records");
        dbg!(format!(
            "The table scan is at {:?}",
            table_scan.record_page.as_ref().unwrap().handle.block_id()
        ));
        let mut deleted_count = 0;
        table_scan.move_to_start();
        while let Some(_) = table_scan.next() {
            let number = table_scan.get_int("A").unwrap();
            dbg!(format!("The number retrieved {}", number));
            if number < 25 {
                deleted_count += 1;
                table_scan.delete().unwrap();
            }
        }
        dbg!(format!("Deleted {} records", deleted_count));

        dbg!("Finding remaining records");
        let mut remaining_count = 0;
        table_scan.move_to_start();
        while let Some(_) = table_scan.next() {
            table_scan.get_int("A").unwrap();
            table_scan.get_string("B").unwrap();
            remaining_count += 1;
        }
        dbg!(format!("Found {} remaining records", remaining_count));
        assert_eq!(remaining_count + deleted_count, inserted_count);
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum Constant {
    Int(i32),
    String(String),
}

impl Constant {
    fn as_int(&self) -> i32 {
        match self {
            Constant::Int(value) => *value,
            _ => panic!("Expected an integer constant"),
        }
    }

    fn as_str(&self) -> &str {
        match self {
            Constant::String(value) => value,
            _ => panic!("Expected a string constant"),
        }
    }
}

#[derive(PartialEq, Eq, Debug, Clone, Copy)]
pub struct RID {
    block_num: usize,
    slot: usize,
}

impl RID {
    pub fn new(block_num: usize, slot: usize) -> Self {
        Self { block_num, slot }
    }
}

struct RecordPageIterator<'a> {
    record_page: &'a RecordPage,
    current_slot: Option<usize>,
    presence: SlotPresence,
}

impl<'a> RecordPageIterator<'a> {
    pub fn new(record_page: &'a RecordPage, presence: SlotPresence) -> Self {
        Self {
            record_page,
            current_slot: None,
            presence,
        }
    }
}

impl Iterator for RecordPageIterator<'_> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let slot = match self.current_slot {
                None => 0,
                Some(slot) => slot + 1,
            };
            if !self.record_page.is_valid_slot(slot) {
                break;
            }

            self.current_slot = Some(slot);

            let slot_value = self
                .record_page
                .tx
                .get_int(
                    self.record_page.handle.block_id(),
                    self.record_page.offset(slot),
                )
                .unwrap();

            if slot_value == self.presence as i32 {
                return Some(slot);
            }
        }
        None
    }
}

#[derive(Clone, Copy)]
enum SlotPresence {
    Empty,
    Used,
}

#[derive(Clone)]
struct RecordPage {
    tx: Arc<Transaction>,
    handle: BufferHandle,
    layout: Layout,
}

impl RecordPage {
    /// Creates a new RecordPage with the given transaction, block ID, and layout.
    /// Pins the block in memory.
    pub fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        let handle = tx.pin(&block_id);
        Self { tx, handle, layout }
    }

    /// Retrieves an integer value from the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn get_int(&self, slot: usize, field_name: &str) -> i32 {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx.get_int(self.handle.block_id(), offset).unwrap()
    }

    /// Retrieves a string value from the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn get_string(&self, slot: usize, field_name: &str) -> String {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx.get_string(self.handle.block_id(), offset).unwrap()
    }

    /// Sets an integer value in the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn set_int(&self, slot: usize, field_name: &str, value: i32) {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx
            .set_int(self.handle.block_id(), offset, value, true)
            .unwrap();
    }

    /// Sets a string value in the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn set_string(&self, slot: usize, field_name: &str, value: &str) {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx
            .set_string(self.handle.block_id(), offset, value, true)
            .unwrap();
    }

    /// Marks a slot as used and returns its slot number.
    #[cfg(test)]
    fn insert(&self, slot: usize) -> usize {
        self.set_flag(slot, SlotPresence::Used);
        slot
    }

    /// Finds the next empty slot after the given slot, marks it as used, and returns its number.
    fn insert_after(&self, slot: Option<usize>) -> Result<usize, Box<dyn Error>> {
        let new_slot = match slot {
            None => self
                .iter_empty_slots()
                .next()
                .ok_or("no empty slots available in this record page")?,
            Some(current_slot) => self
                .iter_empty_slots()
                .find(|s| *s > current_slot)
                .ok_or("no empty slots available in this record page")?,
        };
        self.set_flag(new_slot, SlotPresence::Used);
        Ok(new_slot)
    }

    /// Sets the presence flag (EMPTY or USED) for a given slot.
    fn set_flag(&self, slot: usize, flag: SlotPresence) {
        self.tx
            .set_int(self.handle.block_id(), self.offset(slot), flag as i32, true)
            .unwrap();
    }

    /// Marks a slot as empty, effectively deleting its record.
    fn delete(&self, slot: usize) {
        self.set_flag(slot, SlotPresence::Empty);
    }

    /// Calculates the byte offset for a given slot based on the layout's slot size.
    fn offset(&self, slot: usize) -> usize {
        slot * self.layout.slot_size
    }

    /// Checks if a slot number is valid within the block's size.
    fn is_valid_slot(&self, slot: usize) -> bool {
        self.offset(slot + 1) <= self.tx.block_size()
    }

    /// Initializes all slots in the block with empty flags and default values.
    /// For each field in the schema, sets integers to 0 and strings to empty.
    fn format(&self) {
        let mut current_slot = 0;
        while self.is_valid_slot(current_slot) {
            self.tx
                .set_int(
                    self.handle.block_id(),
                    self.offset(current_slot),
                    SlotPresence::Empty as i32,
                    false,
                )
                .unwrap();
            let schema = &self.layout.schema;
            for field in &schema.fields {
                let field_pos = self.offset(current_slot) + self.layout.offset(field).unwrap();
                match schema.info.get(field).unwrap().field_type {
                    FieldType::Int => self
                        .tx
                        .set_int(self.handle.block_id(), field_pos, 0, false)
                        .unwrap(),
                    FieldType::String => self
                        .tx
                        .set_string(self.handle.block_id(), field_pos, "", false)
                        .unwrap(),
                }
            }
            current_slot += 1;
        }
    }

    /// Returns an iterator over empty slots in the record page.
    fn iter_empty_slots(&self) -> RecordPageIterator<'_> {
        RecordPageIterator {
            record_page: self,
            current_slot: None,
            presence: SlotPresence::Empty,
        }
    }

    /// Returns an iterator over used slots in the record page.
    fn iter_used_slots(&self) -> RecordPageIterator<'_> {
        RecordPageIterator::new(self, SlotPresence::Used)
    }
}

#[cfg(test)]
mod record_page_tests {

    use crate::{test_utils::generate_random_number, Layout, RecordPage, Schema, SimpleDB};

    #[test]
    fn record_page_test() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = db.new_tx();

        //  Set up the test
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);
        for field in &layout.schema.fields {
            let offset = layout.offset(field).unwrap();
            if field == "A" {
                assert_eq!(offset, 4);
            }
            if field == "B" {
                assert_eq!(offset, 8);
            }
        }
        let block_id = txn.append("test_file");
        let record_page = RecordPage::new(txn, block_id, layout);
        record_page.format();

        //  Create a bunch of records
        let record_iter = record_page.iter_empty_slots();
        let mut inserted_count = 0;
        for slot in record_iter {
            let number = (generate_random_number() % 100) + 1;

            record_page.set_int(slot, "A", number as i32);
            record_page.set_string(slot, "B", &format!("rec{number}"));
            inserted_count += 1;
            record_page.insert(slot);
        }

        //  Delete all records with a value less than 25
        let record_iter = record_page.iter_used_slots();
        let mut deleted_count = 0;
        for slot in record_iter {
            let a = record_page.get_int(slot, "A");
            println!("value of a {}", a);
            if a < 25 {
                deleted_count += 1;
                record_page.delete(slot);
            }
        }
        println!("{deleted_count} values were deleted");

        //  Check that the correct number of records are left
        let record_iter = record_page.iter_used_slots();
        let mut remaining_count = 0;
        for slot in record_iter {
            let a = record_page.get_int(slot, "A");
            assert!(a >= 25);
            remaining_count += 1;
        }

        assert_eq!(remaining_count + deleted_count, inserted_count);
    }
}

#[derive(Clone, Debug)]
pub struct Layout {
    pub schema: Schema,
    offsets: HashMap<String, usize>, //  map the field name to the offset
    pub slot_size: usize,
}

impl Layout {
    pub fn new(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut offset = Page::INT_BYTES;
        for field in schema.fields.iter() {
            let field_info = schema.info.get(field).unwrap();
            offsets.insert(field.clone(), offset);

            match field_info.field_type {
                FieldType::Int => offset += field_info.length,
                FieldType::String => offset += Page::INT_BYTES + field_info.length,
            }
        }
        Self {
            schema,
            offsets,
            slot_size: offset,
        }
    }

    /// Get the offset of a field in a record
    fn offset(&self, field: &str) -> Option<usize> {
        self.offsets.get(field).copied()
    }
}

#[cfg(test)]
mod layout_tests {
    use crate::{Layout, Schema};

    #[test]
    fn layout_test() {
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);
        for field in layout.schema.fields.iter() {
            let offset = layout.offset(field).unwrap();
            if field == "A" {
                assert_eq!(offset, 4);
            }
            if field == "B" {
                assert_eq!(offset, 8);
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum FieldType {
    Int = 0,
    String = 1,
}

impl From<i32> for FieldType {
    fn from(value: i32) -> Self {
        match value {
            0 => FieldType::Int,
            1 => FieldType::String,
            _ => panic!("Invalid field type"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct FieldInfo {
    pub field_type: FieldType,
    pub length: usize,
}

#[derive(Clone, Debug)]
pub struct Schema {
    pub fields: Vec<String>,
    pub info: HashMap<String, FieldInfo>,
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl Schema {
    pub fn new() -> Self {
        Schema {
            fields: Vec::new(),
            info: HashMap::new(),
        }
    }

    fn add_field(&mut self, field_name: &str, field_type: FieldType, length: usize) {
        self.fields.push(field_name.to_string());
        self.info
            .entry(field_name.to_string())
            .and_modify(|entry| *entry = FieldInfo { field_type, length })
            .or_insert_with(|| FieldInfo { field_type, length });
    }

    fn add_int_field(&mut self, field_name: &str) {
        self.add_field(field_name, FieldType::Int, Page::INT_BYTES);
    }

    fn add_string_field(&mut self, field_name: &str, length: usize) {
        self.add_field(field_name, FieldType::String, length);
    }

    fn add_from_schema(&mut self, field_name: &str, schema: &Schema) -> Result<(), Box<dyn Error>> {
        let (field_type, field_length) = schema
            .info
            .get(field_name)
            .map(|info| (info.field_type, info.length))
            .ok_or_else(|| {
                format!("Field {field_name} not found in schema while looking for type")
            })?;

        self.add_field(field_name, field_type, field_length);
        Ok(())
    }

    fn add_all_from_schema(&mut self, schema: &Schema) -> Result<(), Box<dyn Error>> {
        for field_name in schema.fields.iter() {
            self.add_from_schema(field_name, schema)?;
        }
        Ok(())
    }
}

/// A handle representing a pinned buffer
/// The buffer is automatically unpinned when the handle is dropped
///
/// This uses RAII semantics to ensure that manual unpinning is not required which will reduce programmer error as well
///
/// # Example
/// ```ignore
/// let handle = txn.pin(&block_id);
/// let value = txn.get_int(handle.block_id(), offset)?;
/// //  handle will drop after scope end and automatically unpin
/// ```
pub struct BufferHandle {
    block_id: BlockId,
    txn: Arc<Transaction>,
}

impl BufferHandle {
    pub fn new(block_id: BlockId, txn: Arc<Transaction>) -> Self {
        txn.pin_internal(&block_id);
        BufferHandle { block_id, txn }
    }

    pub fn block_id(&self) -> &BlockId {
        &self.block_id
    }
}

impl Clone for BufferHandle {
    fn clone(&self) -> Self {
        self.txn.pin_internal(&self.block_id);
        Self {
            block_id: self.block_id.clone(),
            txn: Arc::clone(&self.txn),
        }
    }
}

impl Drop for BufferHandle {
    fn drop(&mut self) {
        self.txn.unpin_internal(&self.block_id);
    }
}

pub struct PageReadGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockReadGuard<'a, Page>,
}

impl<'a> PageReadGuard<'a> {
    fn new(handle: BufferHandle, frame: Arc<BufferFrame>, page: RwLockReadGuard<'a, Page>) -> Self {
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
}

impl<'a> Deref for PageReadGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

pub struct PageWriteGuard<'a> {
    handle: BufferHandle,
    frame: Arc<BufferFrame>,
    page: RwLockWriteGuard<'a, Page>,
}

impl<'a> PageWriteGuard<'a> {
    fn new(
        handle: BufferHandle,
        frame: Arc<BufferFrame>,
        page: RwLockWriteGuard<'a, Page>,
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
}

impl<'a> Deref for PageWriteGuard<'a> {
    type Target = Page;

    fn deref(&self) -> &Self::Target {
        &self.page
    }
}

impl<'a> DerefMut for PageWriteGuard<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.page
    }
}

trait TransactionOperations {
    fn pin(&self, block_id: &BlockId);
    fn unpin(&self, block_id: &BlockId);
    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool);
    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool);
}

impl TransactionOperations for Arc<Transaction> {
    fn pin(&self, block_id: &BlockId) {
        Transaction::pin_internal(self, block_id);
    }

    fn unpin(&self, block_id: &BlockId) {
        Transaction::unpin_internal(self, block_id);
    }

    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool) {
        Transaction::set_int(self, block_id, offset, val, log).unwrap();
    }

    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool) {
        Transaction::set_string(self, block_id, offset, val, log).unwrap();
    }
}

type TransactionID = u64;

/// The timestamp oracle which will generate unique timestamps for each transaction
/// in a monotonically increasing fashion
struct TxIdGenerator {
    next_id: AtomicU64,
}

impl TxIdGenerator {
    fn next_id(&self) -> TransactionID {
        self.next_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }
}

static TX_ID_GENERATOR: OnceLock<TxIdGenerator> = OnceLock::new();

#[derive(Debug)]
pub struct Transaction {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<BufferManager>,
    recovery_manager: RecoveryManager,
    concurrency_manager: ConcurrencyManager,
    buffer_list: BufferList,
    tx_id: TransactionID,
}

impl Transaction {
    const TXN_SLEEP_TIMEOUT: u64 = 100; //  time the txn will sleep for

    /// Returns the transaction sleep timeout in milliseconds
    pub fn sleep_timeout() -> u64 {
        Self::TXN_SLEEP_TIMEOUT
    }

    pub fn new(
        file_manager: SharedFS,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<BufferManager>,
        lock_table: Arc<LockTable>,
    ) -> Self {
        let generator = TX_ID_GENERATOR.get_or_init(|| TxIdGenerator {
            next_id: AtomicU64::new(0),
        });
        let tx_id = generator.next_id();
        Self {
            tx_id,
            recovery_manager: RecoveryManager::new(
                tx_id as usize,
                Arc::clone(&log_manager),
                Arc::clone(&buffer_manager),
            ),
            buffer_list: BufferList::new(Arc::clone(&buffer_manager)),
            buffer_manager,
            log_manager,
            concurrency_manager: ConcurrencyManager::new(tx_id, lock_table),
            file_manager,
        }
    }

    /// Commit this transaction
    /// This will write all data associated with this transaction out to disk and append a [`LogRecord::Commit`] to the WAL
    /// It will release all locks that are currently held by this transaction
    /// It will also handle meta operations like unpinning buffers
    pub fn commit(&self) -> Result<(), Box<dyn Error>> {
        self.recovery_manager.commit();
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Rollback this transaction
    /// This will undo all operations performed by this transaction and append a [`LogRecord::Rollback`] to the WAL
    /// It will also handle meta operations like unpinning buffers
    pub fn rollback(self: &Arc<Self>) -> Result<(), Box<dyn Error>> {
        self.recovery_manager.rollback(self).unwrap();
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Recover the database on start-up or after a crash
    pub fn recover(self: &Arc<Self>) -> Result<(), Box<dyn Error>> {
        self.recovery_manager.recover(self).unwrap();
        self.concurrency_manager.release()?;
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// The public pin method which will return a [`BufferHandle`] for RAII semantics
    pub fn pin(self: &Arc<Self>, block_id: &BlockId) -> BufferHandle {
        BufferHandle::new(block_id.clone(), Arc::clone(self))
    }

    pub fn pin_read_guard(self: &Arc<Self>, block_id: &BlockId) -> PageReadGuard<'_> {
        let handle = self.pin(block_id);
        let frame = self.buffer_list.get_buffer(block_id).unwrap();
        let frame_clone = Arc::clone(&frame);
        let raw = Arc::into_raw(frame_clone);
        let page = unsafe { (&*raw).read_page() };
        let frame_for_guard = unsafe { Arc::from_raw(raw) };
        PageReadGuard::new(handle, frame_for_guard, page)
    }

    pub fn pin_write_guard(self: &Arc<Self>, block_id: &BlockId) -> PageWriteGuard<'_> {
        let handle = self.pin(block_id);
        let frame = self.buffer_list.get_buffer(block_id).unwrap();
        let frame_clone = Arc::clone(&frame);
        let raw = Arc::into_raw(frame_clone);
        let page = unsafe { (&*raw).write_page() };
        let frame_for_guard = unsafe { Arc::from_raw(raw) };
        PageWriteGuard::new(handle, frame_for_guard, page)
    }

    pub fn get_int(
        self: &Arc<Self>,
        block_id: &BlockId,
        offset: usize,
    ) -> Result<i32, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let page = self.pin_read_guard(block_id);
        Ok(page.get_int(offset))
    }

    pub fn get_string(
        self: &Arc<Self>,
        block_id: &BlockId,
        offset: usize,
    ) -> Result<String, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let page = self.pin_read_guard(block_id);
        Ok(page.get_string(offset))
    }

    pub fn set_int(
        self: &Arc<Self>,
        block_id: &BlockId,
        offset: usize,
        value: i32,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let mut page = self.pin_write_guard(block_id);
        let old_value = page.get_int(offset);
        let lsn = if log {
            self.recovery_manager.set_int(block_id, offset, old_value)?
        } else {
            Lsn::MAX
        };
        page.set_int(offset, value);
        page.mark_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    pub fn set_string(
        self: &Arc<Self>,
        block_id: &BlockId,
        offset: usize,
        value: &str,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let mut page = self.pin_write_guard(block_id);
        let old_value = page.get_string(offset);
        let lsn = if log {
            self.recovery_manager
                .set_string(block_id, offset, old_value)?
        } else {
            Lsn::MAX
        };
        page.set_string(offset, value);
        page.mark_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    /// Pin this [`BlockId`] to be used in this transaction
    /// This should not be used anywhere outside of the following modules - [`Transaction`], [`RecoveryManager`]
    /// It does not provide RAII semantics. Requires an explicit call to [`Transaction::unpin_internal`] after
    fn pin_internal(&self, block_id: &BlockId) {
        self.buffer_list.pin(block_id);
    }

    /// Unpin this [`BlockId`] since it is no longer needed by this transaction
    fn unpin_internal(&self, block_id: &BlockId) {
        self.buffer_list.unpin(block_id);
    }

    /// Get the available buffers for this transaction
    pub fn available_buffs(&self) -> usize {
        self.buffer_manager.available()
    }

    /// Get the log manager for monitoring purposes
    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        Arc::clone(&self.log_manager)
    }

    /// Get the buffer manager for monitoring purposes
    pub fn buffer_manager(&self) -> Arc<BufferManager> {
        Arc::clone(&self.buffer_manager)
    }

    /// Get the size of this file in blocks
    fn size(&self, file_name: &str) -> usize {
        self.file_manager
            .lock()
            .unwrap()
            .length(file_name.to_string())
    }

    /// Append a block to the file
    fn append(&self, file_name: &str) -> BlockId {
        self.file_manager
            .lock()
            .unwrap()
            .append(file_name.to_string())
    }

    /// Get the block size
    pub fn block_size(&self) -> usize {
        self.file_manager.lock().unwrap().block_size()
    }
}
#[cfg(test)]
mod transaction_tests {
    use std::{error::Error, sync::Arc, thread::JoinHandle, time::Duration};

    use crate::{
        test_utils::{generate_filename, generate_random_number, TestDir},
        BlockId, BufferHandle, SimpleDB, Transaction,
    };

    #[test]
    fn test_transaction_single_threaded() {
        let file = generate_filename();
        
        let (test_db, _test_dir) = SimpleDB::new_for_test(3, 5000);

        //  Start a transaction t1 that will set an int and a string
        let t1 = test_db.new_tx();
        let block_id = BlockId::new(file.to_string(), 1);
        t1.pin_internal(&block_id);
        t1.set_int(&block_id, 80, 1, false).unwrap();
        t1.set_string(&block_id, 40, "one", false).unwrap();
        t1.commit().unwrap();

        //  Start a transaction t2 that should see the results of the previously committed transaction t1
        //  Set new values in this transaction
        let t2 = test_db.new_tx();
        t2.pin_internal(&block_id);
        assert_eq!(t2.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(t2.get_string(&block_id, 40).unwrap(), "one");
        t2.set_int(&block_id, 80, 2, true).unwrap();
        t2.set_string(&block_id, 40, "two", true).unwrap();
        t2.commit().unwrap();

        //  Start a transaction t3 which should see the results of t2
        //  Set new values for t3 but roll it back instead of committing
        let t3 = test_db.new_tx();
        t3.pin_internal(&block_id);
        assert_eq!(t3.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t3.get_string(&block_id, 40).unwrap(), "two");
        t3.set_int(&block_id, 80, 3, true).unwrap();
        t3.set_string(&block_id, 40, "three", true).unwrap();
        t3.rollback().unwrap();

        //  Start a transaction t4 which should see the result of t2 since t3 rolled back
        //  This will be a read only transaction that commits
        let t4 = test_db.new_tx();
        t4.pin_internal(&block_id);
        assert_eq!(t4.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t4.get_string(&block_id, 40).unwrap(), "two");
        t4.commit().unwrap();
    }

    #[test]
    fn test_transaction_multi_threaded_single_reader_single_writer() {
        let file = generate_filename();
        
        let (test_db, _test_dir) = SimpleDB::new_for_test(10, 5000);
        let block_id = BlockId::new(file.to_string(), 1);

        let fm1 = Arc::clone(&test_db.file_manager);
        let lm1 = Arc::clone(&test_db.log_manager);
        let bm1 = Arc::clone(&test_db.buffer_manager);
        let lt1 = Arc::clone(&test_db.lock_table);
        let bid1 = block_id.clone();

        let fm2 = Arc::clone(&test_db.file_manager);
        let lm2 = Arc::clone(&test_db.log_manager);
        let bm2 = Arc::clone(&test_db.buffer_manager);
        let lt2 = Arc::clone(&test_db.lock_table);
        let bid2 = block_id.clone();

        //  Create a read only transasction
        let t1 = std::thread::spawn(move || {
            let txn = Arc::new(Transaction::new(fm1, lm1, bm1, lt1));
            txn.pin_internal(&bid1);
            txn.get_int(&bid1, 80).unwrap();
            txn.get_string(&bid1, 40).unwrap();
            txn.commit().unwrap();
        });

        //  Create a write only transaction
        let t2 = std::thread::spawn(move || {
            let txn = Arc::new(Transaction::new(fm2, lm2, bm2, lt2));
            txn.pin_internal(&bid2.clone());
            txn.set_int(&bid2, 80, 1, false).unwrap();
            txn.set_string(&bid2, 40, "Hello", false).unwrap();
            txn.commit().unwrap();
        });
        t1.join().unwrap();
        t2.join().unwrap();

        //  Create a final read-only transaction that will read the written values
        let txn = Arc::new(Transaction::new(
            test_db.file_manager,
            test_db.log_manager,
            test_db.buffer_manager,
            test_db.lock_table,
        ));
        txn.pin_internal(&block_id);
        assert_eq!(txn.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(txn.get_string(&block_id, 40).unwrap(), "Hello");
    }

    #[test]
    fn test_transaction_multi_threaded_multiple_readers_single_writer() {
        let file = generate_filename();
        
        let (test_db, _test_dir) = SimpleDB::new_for_test(10, 5000);
        let block_id = BlockId::new(file.to_string(), 1);

        // Initialize data before spawning threads
        let init_txn = Arc::new(Transaction::new(
            test_db.file_manager.clone(),
            test_db.log_manager.clone(),
            test_db.buffer_manager.clone(),
            test_db.lock_table.clone(),
        ));
        init_txn.pin_internal(&block_id);
        init_txn.set_int(&block_id, 80, 0, false).unwrap();
        init_txn
            .set_string(&block_id, 40, "initial", false)
            .unwrap();
        init_txn.commit().unwrap();

        let reader_threads = 10;
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..reader_threads {
            let fm = Arc::clone(&test_db.file_manager);
            let lm = Arc::clone(&test_db.log_manager);
            let bm = Arc::clone(&test_db.buffer_manager);
            let lt = Arc::clone(&test_db.lock_table);
            let bid = block_id.clone();

            handles.push(std::thread::spawn(move || {
                let txn = Arc::new(Transaction::new(fm, lm, bm, lt));
                txn.pin_internal(&bid);

                // Verify we read a valid state (either initial or final)
                let val = txn.get_int(&bid, 80).unwrap();
                assert!(val == 0 || val == 42, "Read invalid int value: {}", val);

                let s = txn.get_string(&bid, 40).unwrap();
                assert!(
                    s == "initial" || s == "final",
                    "Read invalid string value: {}",
                    s
                );

                txn.commit().unwrap();
            }));
        }

        let txn = Arc::new(Transaction::new(
            test_db.file_manager.clone(),
            test_db.log_manager.clone(),
            test_db.buffer_manager.clone(),
            test_db.lock_table.clone(),
        ));
        txn.pin_internal(&block_id);
        txn.set_int(&block_id, 80, 42, false).unwrap();
        txn.set_string(&block_id, 40, "final", false).unwrap();
        txn.commit().unwrap();

        handles
            .into_iter()
            .for_each(|handle| handle.join().unwrap());

        // Verify final state
        let final_txn = Arc::new(Transaction::new(
            test_db.file_manager.clone(),
            test_db.log_manager.clone(),
            test_db.buffer_manager.clone(),
            test_db.lock_table.clone(),
        ));
        final_txn.pin_internal(&block_id);
        assert_eq!(final_txn.get_int(&block_id, 80).unwrap(), 42);
        assert_eq!(final_txn.get_string(&block_id, 40).unwrap(), "final");
        final_txn.commit().unwrap();
    }

    #[test]
    fn test_transaction_rollback() {
        let file = generate_filename();
        let (test_db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let block_id = BlockId::new(file.clone(), 1);

        // Setup initial state
        let t1 = Arc::new(Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
            Arc::clone(&test_db.lock_table),
        ));
        t1.pin_internal(&block_id);
        t1.set_int(&block_id, 80, 100, true).unwrap();
        t1.set_string(&block_id, 40, "initial", true).unwrap();
        t1.commit().unwrap();

        // Start transaction that will modify multiple values but fail midway
        let t2 = Arc::new(Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
            Arc::clone(&test_db.lock_table),
        ));
        t2.pin_internal(&block_id);
        t2.set_int(&block_id, 80, 200, true).unwrap();
        t2.set_string(&block_id, 40, "modified", true).unwrap();
        // Simulate failure by rolling back
        t2.rollback().unwrap();

        // Verify that none of t2's changes persisted
        let t3 = Arc::new(Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
            Arc::clone(&test_db.lock_table),
        ));
        t3.pin_internal(&block_id);
        assert_eq!(t3.get_int(&block_id, 80).unwrap(), 100);
        assert_eq!(t3.get_string(&block_id, 40).unwrap(), "initial");
    }

    /// Tests that concurrent read-modify-write transactions can succeed via retry logic under high lock contention.
    ///
    /// Scenario: 5 threads simultaneously try to read-increment-write the same location. All acquire SLOCK,
    /// then all attempt to upgrade to XLOCK, causing lock contention. The test validates that the retry
    /// mechanism (with rollback on timeout) eventually allows all transactions to complete serially.
    ///
    /// Final value should be `num_of_txns` (5), confirming all increments were applied atomically.
    #[test]
    fn test_transaction_isolation_with_concurrent_writes() {
        let file = generate_filename();
        let (test_db, _test_dir) = SimpleDB::new_for_test(3, 500);
        let block_id = BlockId::new(file.clone(), 1);
        let num_of_txns = 2;
        let max_retry_count = 150;

        // Initialize data
        let t1 = Arc::new(Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
            Arc::clone(&test_db.lock_table),
        ));
        t1.pin_internal(&block_id);
        t1.set_int(&block_id, 80, 0, true).unwrap();
        t1.commit().unwrap();

        // Create channel to track operations
        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn transactions that will increment the value
        let mut handles = vec![];
        for _ in 0..num_of_txns {
            let fm = Arc::clone(&test_db.file_manager);
            let lm = Arc::clone(&test_db.log_manager);
            let bm = Arc::clone(&test_db.buffer_manager);
            let lt = Arc::clone(&test_db.lock_table);
            let bid = block_id.clone();
            let tx = tx.clone();

            handles.push(std::thread::spawn(move || {
                let mut retry_count = 0;
                let txn = Arc::new(Transaction::new(
                    fm.clone(),
                    lm.clone(),
                    bm.clone(),
                    lt.clone(),
                ));
                loop {
                    if retry_count > max_retry_count {
                        panic!("Too many retries");
                    }
                    txn.pin_internal(&bid);

                    // Try to perform the increment
                    match (|| -> Result<(), Box<dyn Error>> {
                        let val = txn.get_int(&bid, 80)?;

                        // Short sleep to increase chance of conflicts
                        std::thread::sleep(Duration::from_millis(10));

                        txn.set_int(&bid, 80, val + 1, true)?;
                        txn.commit()?;
                        tx.send(format!(
                            "Transaction {} successfully incremented from {} to {}",
                            txn.tx_id,
                            val,
                            val + 1
                        ))
                        .unwrap();
                        Ok(())
                    })() {
                        Ok(_) => break, // Success, exit loop
                        Err(e) => {
                            // If lock timeout, retry
                            if e.to_string().contains("Timeout") {
                                retry_count += 1;
                                txn.rollback().unwrap();
                                tx.send(format!(
                                    "Transaction {} retrying after timeout",
                                    txn.tx_id
                                ))
                                .unwrap();
                                // Add deterministic jitter so retries don't re-enter lock queue in sync
                                let jitter_ms = 25 + ((txn.tx_id + retry_count as u64) % 10) * 5;
                                std::thread::sleep(Duration::from_millis(jitter_ms));
                                continue;
                            }
                            // Other errors should fail the test
                            panic!("Transaction failed: {e}");
                        }
                    }
                }
            }));
        }

        // Collect and log all operations
        let mut successful_increments = 0;
        let mut operations = vec![];

        loop {
            match rx.recv_timeout(Duration::from_secs(60)) {
                Ok(msg) => {
                    if msg.contains("successfully incremented") {
                        successful_increments += 1;
                    }
                    operations.push(msg);

                    if successful_increments == num_of_txns {
                        break;
                    }
                }
                Err(_) => {
                    // Print operations for debugging
                    println!("Operations so far: {operations:?}");
                    panic!("Test timed out with {successful_increments} successful increments");
                }
            }
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final value
        let t_final = Arc::new(Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
            Arc::clone(&test_db.lock_table),
        ));
        t_final.pin_internal(&block_id);
        assert!(t_final.get_int(&block_id, 80).unwrap() == num_of_txns);
    }

    #[test]
    fn test_transaction_durability() {
        let file = generate_filename();
        let dir = TestDir::new(format!("/tmp/recovery_test/{}", generate_random_number()));

        //  Phase 1: Create and populate database and then drop it
        {
            let db = SimpleDB::new(&dir, 3, true, 100);
            let t1 = Arc::new(Transaction::new(
                Arc::clone(&db.file_manager),
                Arc::clone(&db.log_manager),
                Arc::clone(&db.buffer_manager),
                Arc::clone(&db.lock_table),
            ));
            let block_id = BlockId::new(file.clone(), 1);
            t1.pin_internal(&block_id);
            t1.set_int(&block_id, 80, 100, true).unwrap();
            t1.commit().unwrap();
        }

        //  Phase 2: Recover and verify
        {
            let db = SimpleDB::new(&dir, 3, false, 100);
            let t2 = Arc::new(Transaction::new(
                Arc::clone(&db.file_manager),
                Arc::clone(&db.log_manager),
                Arc::clone(&db.buffer_manager),
                Arc::clone(&db.lock_table),
            ));
            t2.recover().unwrap();

            let block_id = BlockId::new(file.clone(), 1);
            t2.pin_internal(&block_id);
            assert_eq!(t2.get_int(&block_id, 80).unwrap(), 100);
        }
    }

    #[test]
    fn test_buffer_handle_raii_basic_pin_unpin() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = db.new_tx();
        let block_id = BlockId::new("test".to_string(), 0);

        {
            let _handle = BufferHandle::new(block_id.clone(), Arc::clone(&txn));

            // Verify pin count = 1
            let buffer = txn.buffer_list.get_buffer(&block_id).unwrap();
            assert_eq!(buffer.pin_count(), 1);

            #[cfg(debug_assertions)]
            txn.buffer_list.assert_pin_invariant(&block_id, 1);
        }

        // After handle is dropped, buffer should be unpinned
        assert!(txn.buffer_list.get_buffer(&block_id).is_none());

        #[cfg(debug_assertions)]
        db.buffer_manager.assert_buffer_count_invariant();
    }

    #[test]
    fn test_buffer_handle_clone_increments_pins() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = db.new_tx();
        let block_id = BlockId::new("test".to_string(), 0);

        let handle1 = BufferHandle::new(block_id.clone(), Arc::clone(&txn));

        #[cfg(debug_assertions)]
        txn.buffer_list.assert_pin_invariant(&block_id, 1);

        let handle2 = handle1.clone();

        // Both handles should keep block pinned - pin count should be 2
        let buffer = txn.buffer_list.get_buffer(&block_id).unwrap();
        assert_eq!(buffer.pin_count(), 2);

        #[cfg(debug_assertions)]
        txn.buffer_list.assert_pin_invariant(&block_id, 2);

        drop(handle1);

        // After dropping one handle, pin count should be 1
        let buffer = txn.buffer_list.get_buffer(&block_id).unwrap();
        assert_eq!(buffer.pin_count(), 1);

        #[cfg(debug_assertions)]
        txn.buffer_list.assert_pin_invariant(&block_id, 1);

        drop(handle2);

        // After dropping both handles, buffer should be unpinned
        assert!(txn.buffer_list.get_buffer(&block_id).is_none());

        #[cfg(debug_assertions)]
        db.buffer_manager.assert_buffer_count_invariant();
    }

    #[test]
    fn test_no_buffer_leaks_after_commit() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = db.new_tx();
        let block_id = BlockId::new("test".to_string(), 0);

        let _handle = BufferHandle::new(block_id.clone(), Arc::clone(&txn));

        txn.commit().unwrap();

        // All buffers should be unpinned after commit
        // Even though handle still exists
        assert_eq!(db.buffer_manager.available(), 3);

        #[cfg(debug_assertions)]
        db.buffer_manager.assert_buffer_count_invariant();
    }

    #[test]
    fn test_handle_drop_after_commit_is_safe() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let txn = db.new_tx();
        let block_id = BlockId::new("test".to_string(), 0);
        let handle = BufferHandle::new(block_id.clone(), Arc::clone(&txn));

        // Commit unpins everything and sets committed flag
        txn.commit().unwrap();

        // Handle still exists - this should NOT panic
        drop(handle); // Should be no-op (committed flag prevents double-unpin)

        // Verify no crash and all buffers available
        assert_eq!(db.buffer_manager.available(), 3);
    }

    #[test]
    fn test_multiple_handles_after_commit() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);

        // Check initial available buffers
        let initial_available = db.buffer_manager.available();

        let txn = db.new_tx();
        let block_id = BlockId::new("test".to_string(), 0);

        let handle1 = BufferHandle::new(block_id.clone(), Arc::clone(&txn));
        let handle2 = handle1.clone();
        let handle3 = handle2.clone();

        txn.commit().unwrap();

        // All three handles should drop safely (no-op after commit)
        drop(handle1);
        drop(handle2);
        drop(handle3);

        // Drop the transaction Arc as well
        drop(txn);

        // Verify all buffers available (should match initial available)
        assert_eq!(db.buffer_manager.available(), initial_available);
    }
}

#[derive(Debug)]
struct LockState {
    readers: HashSet<TransactionID>, //  keep track of which transaction id's have a reader lock here
    writer: Option<TransactionID>,   //  keep track of the transaction writing to a specific block
    upgrade_requests: VecDeque<TransactionID>, //  keep track of upgrade requests to prevent writer starvation
}

/// Global struct used by all transactions to keep track of locks
#[derive(Debug)]
pub struct LockTable {
    lock_table: Mutex<HashMap<BlockId, LockState>>,
    cond_var: Condvar,
    timeout: u64,
}

impl LockTable {
    pub fn new(timeout: u64) -> Self {
        Self {
            lock_table: Mutex::new(HashMap::new()),
            cond_var: Condvar::new(),
            timeout,
        }
    }

    /// Acquire a shared lock on a [`BlockId`] for a [`Transaction`]
    fn acquire_shared_lock(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        lock_table_guard
            .entry(block_id.clone())
            .or_insert(LockState {
                readers: vec![tx_id].into_iter().collect(),
                writer: None,
                upgrade_requests: VecDeque::new(),
            });

        //  Do an early return if the txn already has an SLock on this block
        if lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id)
        {
            return Ok(());
        }

        //  Loop until either
        //  1. There are no more writers or pending writers on this block
        //  2. The timeout expires
        let deadline = Instant::now() + Duration::from_millis(self.timeout);
        loop {
            let state = lock_table_guard.get_mut(block_id).unwrap();
            let should_wait = state.writer.is_some() || !state.upgrade_requests.is_empty();

            if !should_wait {
                break;
            }

            lock_table_guard = self.cond_var.wait(lock_table_guard).unwrap();

            if Instant::now() >= deadline {
                return Err("Timeout while waiting for shared lock".into());
            }
        }
        lock_table_guard
            .get_mut(block_id)
            .unwrap()
            .readers
            .insert(tx_id);
        Ok(())
    }

    /// Acquire an exclusive lock on a [`BlockId`] for a [`Transaction`]
    fn acquire_write_lock(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        lock_table_guard
            .entry(block_id.clone())
            .or_insert(LockState {
                readers: HashSet::from_iter(vec![tx_id]),
                writer: Some(tx_id),
                upgrade_requests: VecDeque::new(),
            });

        //  Do an early return if this txn already has an xlock on the buffer
        if lock_table_guard.get(block_id).unwrap().writer == Some(tx_id) {
            return Ok(());
        }

        //  Maintain the invariant that any transaction that wants an xlock must first have an slock
        assert!(lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id), "Transaction {tx_id} failed to have an slock before attempting to acquire xlock on block id {block_id:?}");

        lock_table_guard
            .get_mut(block_id)
            .unwrap()
            .upgrade_requests
            .push_back(tx_id);
        let deadline = Instant::now() + Duration::from_millis(self.timeout);
        loop {
            let state = lock_table_guard.get_mut(block_id).unwrap();
            let should_wait = state.readers.len() > 1
                || state.writer.is_some()
                || state
                    .upgrade_requests
                    .front()
                    .is_some_and(|id| *id != tx_id);

            if !should_wait {
                break;
            }

            let timeout = deadline.saturating_duration_since(Instant::now());
            if timeout.is_zero() {
                return Err("Timeout while waiting for write lock".into());
            }
            let (guard, timeout_reached) = self
                .cond_var
                .wait_timeout(lock_table_guard, timeout)
                .unwrap();
            lock_table_guard = guard;
            if timeout_reached.timed_out() {
                return Err(
                    "Timeout while waiting for write lock and timeout exceeded after woken up"
                        .into(),
                );
            }
        }
        let state = lock_table_guard.get_mut(block_id).unwrap();
        assert_eq!(state.readers.len(), 1);
        assert!(state.readers.contains(&tx_id));
        assert!(state
            .upgrade_requests
            .front()
            .is_some_and(|id| *id == tx_id));
        state.writer = Some(tx_id);
        state.readers.remove(&tx_id);
        state.upgrade_requests.pop_front();
        Ok(())
    }

    /// Release all locks on a specific [`BlockId`] that were acquired by a [`Transaction`]
    fn release_locks(
        &self,
        tx_id: TransactionID,
        block_id: &BlockId,
    ) -> Result<(), Box<dyn Error>> {
        let mut lock_table_guard = self.lock_table.lock().unwrap();
        if let Some(state) = lock_table_guard.get_mut(block_id) {
            state.readers.remove(&tx_id);
            if let Some(writer_tx_id) = state.writer {
                if writer_tx_id == tx_id {
                    state.writer = None;
                }
            }
            state.upgrade_requests.retain(|&id| id != tx_id);
        }
        self.cond_var.notify_all();
        Ok(())
    }
}

#[cfg(test)]
mod lock_table_tests {
    use std::{sync::Arc, time::Duration};

    use crate::{test_utils::generate_filename, BlockId, LockTable};

    #[test]
    fn test_basic_shared_lock() {
        let filename = generate_filename();
        let lock_table = Arc::new(LockTable::new(10_000));
        let block_id = BlockId::new(filename, 1);

        // Should be able to acquire shared lock
        lock_table.acquire_shared_lock(1, &block_id).unwrap();

        // Another transaction should also be able to acquire shared lock
        lock_table.acquire_shared_lock(2, &block_id).unwrap();

        // Release locks
        lock_table.release_locks(1, &block_id).unwrap();
        lock_table.release_locks(2, &block_id).unwrap();
    }

    #[test]
    fn test_basic_exclusive_lock() {
        let filename = generate_filename();
        let lock_table = Arc::new(LockTable::new(1)); //  extremely short timeout of 1ms
        let block_id = BlockId::new(filename, 1);

        // Should be able to acquire exclusive lock
        lock_table.acquire_write_lock(1, &block_id).unwrap();

        let lt_1 = Arc::clone(&lock_table);
        let bid_1 = block_id.clone();

        //  Another transaction should not be able to acquire any locks
        let _ = std::thread::spawn(move || {
            lt_1.acquire_shared_lock(2, &bid_1).unwrap_err();
        });

        // Release lock after a timeout of making sure t2 panics
        std::thread::sleep(Duration::from_millis(5));
        lock_table.release_locks(1, &block_id).unwrap();
    }

    #[test]
    fn test_read_write_interleaving() {
        let lock_table = Arc::new(LockTable::new(1000)); //  timeout of 1sec
        let block_id = BlockId::new(generate_filename(), 1);

        //  reader thread
        let lt_1 = Arc::clone(&lock_table);
        let bid_1 = block_id.clone();
        std::thread::spawn(move || {
            let readers = 10;
            for i in 0..readers {
                lt_1.acquire_shared_lock(i, &bid_1).unwrap();
                std::thread::sleep(Duration::from_millis(super::Transaction::sleep_timeout()));
                lt_1.release_locks(i, &bid_1).unwrap();
            }
        });

        //  writer thread
        let lt_2 = Arc::clone(&lock_table);
        let bid_2 = block_id.clone();
        std::thread::spawn(move || {
            let count = 10;
            let mut iterations = 0;
            loop {
                if iterations == count {
                    break;
                }

                lt_2.acquire_shared_lock(12, &bid_2).unwrap();
                lt_2.acquire_write_lock(12, &bid_2).unwrap();
                lt_2.release_locks(12, &bid_2).unwrap();

                iterations += 1;
            }
        });
    }

    #[test]
    fn test_lock_upgrade() {
        let lock_table = Arc::new(LockTable::new(1000));
        let block_id = BlockId::new(generate_filename(), 1);
        let (tx, rx) = std::sync::mpsc::channel::<String>();

        //  T1 acquires shared lock
        lock_table.acquire_shared_lock(1, &block_id).unwrap();

        //  T2 acquires shared lock
        lock_table.acquire_shared_lock(2, &block_id).unwrap();

        //  T1 requests an upgrade
        let lt1 = Arc::clone(&lock_table);
        let bid1 = block_id.clone();
        std::thread::spawn(move || {
            tx.send("Acquiring write lock".to_string()).unwrap();
            lt1.acquire_write_lock(1, &bid1).unwrap();
            tx.send("Acquired write lock".to_string()).unwrap();
        });

        //  Wait for T1 to start acquiring write lock and release T2's lock
        assert!(rx.recv().unwrap() == *"Acquiring write lock");
        lock_table.release_locks(2, &block_id).unwrap();
        assert!(rx.recv().unwrap() == *"Acquired write lock");
    }
}

#[derive(Debug)]
enum LockType {
    Shared,
    Exclusive,
}

#[derive(Debug)]
struct ConcurrencyManager {
    lock_table: Arc<LockTable>,
    locks: RefCell<HashMap<BlockId, LockType>>,
    tx_id: TransactionID,
}
impl ConcurrencyManager {
    pub fn new(tx_id: TransactionID, lock_table: Arc<LockTable>) -> Self {
        Self {
            lock_table,
            locks: RefCell::new(HashMap::new()),
            tx_id,
        }
    }

    /// Acquire a shared lock on a [`BlockId`] for the associated [`Transaction`]
    fn slock(&self, block_id: &BlockId) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        if locks.contains_key(block_id) {
            return Ok(());
        }
        self.lock_table.acquire_shared_lock(self.tx_id, block_id)?;
        locks.insert(block_id.clone(), LockType::Shared);
        Ok(())
    }

    /// Acquire an exclusive lock on a [`BlockId`] for the associated [`Transaction`]
    /// It will first check to see if there is already a [`LockType`] available on the [`BlockId`]
    /// If there is none, it will first attempt to acquire a [`LockType::Shared`] and then a [`LockType::Exclusive`]
    fn xlock(&self, block_id: &BlockId) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        match locks.get(block_id) {
            Some(lock) => match lock {
                LockType::Shared => {
                    self.lock_table.acquire_write_lock(self.tx_id, block_id)?;
                    locks.insert(block_id.clone(), LockType::Exclusive).unwrap();
                }
                LockType::Exclusive => return Ok(()),
            },
            None => {
                //  drop the value here so no overlapping borrows
                drop(locks);
                self.slock(block_id)?;
                self.lock_table.acquire_write_lock(self.tx_id, block_id)?;

                //  re-acquire the borrow mut here
                let mut locks = self.locks.borrow_mut();
                locks.insert(block_id.clone(), LockType::Exclusive);
            }
        }
        Ok(())
    }

    /// Release all locks associated with a [`Transaction`]
    fn release(&self) -> Result<(), Box<dyn Error>> {
        let mut locks = self.locks.borrow_mut();
        for block in locks.keys() {
            self.lock_table.release_locks(self.tx_id, block)?;
        }
        locks.clear();
        Ok(())
    }
}

/// The container for the recovery manager - a [`Transaction`] uses a unique instance of this to
/// manage writing records to WAL and handling recovery & rollback
#[derive(Debug)]
struct RecoveryManager {
    tx_num: usize,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<BufferManager>,
}

impl RecoveryManager {
    pub fn new(
        tx_num: usize,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<BufferManager>,
    ) -> Self {
        Self {
            tx_num,
            log_manager,
            buffer_manager,
        }
    }

    /// Commit the [`Transaction`]
    /// It flushes all the buffers associated with this transaction
    /// It creates and writes a new [`LogRecord::Commit`] record to the WAL
    /// It then forces a flush on the WAL to ensure logs are committed
    fn commit(&self) {
        self.buffer_manager.flush_all(self.tx_num);
        let record = LogRecord::Commit(self.tx_num);
        let lsn = record
            .write_log_record(Arc::clone(&self.log_manager))
            .unwrap();
        self.log_manager.lock().unwrap().flush_lsn(lsn);
    }

    /// Rollback the [`Transaction`] associated with this [`RecoveryManager`] instance
    /// Iterate over the WAL records in reverse order and undo any modifications done for this [`Transaction`]
    /// Flush all data associated with this transaction
    /// Create, write and flush a [`LogRecord::Checkpoint`] record
    fn rollback(&self, tx: &dyn TransactionOperations) -> Result<(), Box<dyn Error>> {
        //  Perform the actual rollback by reading the files from WAL and undoing all changes made by this txn
        let log_iter = self.log_manager.lock().unwrap().iterator();
        for log in log_iter {
            let record = LogRecord::from_bytes(log)?;
            if record.get_tx_num() != self.tx_num {
                continue;
            }
            if let LogRecord::Start(_) = record {
                return Ok(());
            }
            record.undo(tx);
        }
        //  Flush all data associated with this transaction
        self.buffer_manager.flush_all(self.tx_num);
        //  Write a checkpoint record and flush it
        let checkpoint_record = LogRecord::Checkpoint;
        let lsn = checkpoint_record.write_log_record(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        Ok(())
    }

    /// Recover the database from the last [`LogRecord::Checkpoint`]
    /// Find all the incomplete transactions and undo their operations
    /// Write a quiescent [`LogRecord::Checkpoint`] to the log and flush it
    fn recover(&self, tx: &dyn TransactionOperations) -> Result<(), Box<dyn Error>> {
        //  Iterate over the WAL records in reverse order and add any that don't have a COMMIT to unfinished txns
        let log_iter = self.log_manager.lock().unwrap().iterator();
        let mut finished_txns: Vec<usize> = Vec::new();
        for log in log_iter {
            let record = LogRecord::from_bytes(log)?;
            match record {
                LogRecord::Checkpoint => return Ok(()),
                LogRecord::Commit(_) | LogRecord::Rollback(_) => {
                    finished_txns.push(record.get_tx_num());
                }
                _ => {
                    if !finished_txns.contains(&record.get_tx_num()) {
                        record.undo(tx);
                    }
                }
            }
        }
        //  Flush all data associated with this transaction
        self.buffer_manager.flush_all(self.tx_num);
        //  Write a checkpoint record and flush it
        let checkpoint_record = LogRecord::Checkpoint;
        let lsn = checkpoint_record.write_log_record(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        Ok(())
    }

    /// Write the [`LogRecord`] to set the value of an integer in a [`Buffer`]
    fn set_int(
        &self,
        block_id: &BlockId,
        offset: usize,
        old_value: i32,
    ) -> Result<Lsn, Box<dyn Error>> {
        let record = LogRecord::SetInt {
            txnum: self.tx_num,
            block_id: block_id.clone(),
            offset,
            old_val: old_value,
        };
        record.write_log_record(Arc::clone(&self.log_manager))
    }

    /// Write the [`LogRecord`] to set the value of a String in a [`Buffer`]
    fn set_string(
        &self,
        block_id: &BlockId,
        offset: usize,
        old_value: String,
    ) -> Result<Lsn, Box<dyn Error>> {
        let record = LogRecord::SetString {
            txnum: self.tx_num,
            block_id: block_id.clone(),
            offset,
            old_val: old_value,
        };
        record.write_log_record(Arc::clone(&self.log_manager))
    }
}

#[cfg(test)]
mod recovery_manager_tests {
    use std::sync::{Arc, Mutex};

    use crate::{BlockId, LogRecord, RecoveryManager, SimpleDB, TransactionOperations};

    struct MockTransaction {
        modified_ints: Mutex<Vec<(BlockId, usize, i32)>>,
        modified_strings: Mutex<Vec<(BlockId, usize, String)>>,
    }

    impl MockTransaction {
        pub fn new() -> Self {
            Self {
                modified_ints: Mutex::new(Vec::new()),
                modified_strings: Mutex::new(Vec::new()),
            }
        }

        fn verify_int_was_reset(
            &self,
            block_id: &BlockId,
            offset: usize,
            expected_val: i32,
        ) -> bool {
            self.modified_ints
                .lock()
                .unwrap()
                .iter()
                .any(|(b, o, v)| b == block_id && *o == offset && *v == expected_val)
        }

        fn verify_string_was_reset(
            &self,
            block_id: &BlockId,
            offset: usize,
            expected_val: String,
        ) -> bool {
            self.modified_strings
                .lock()
                .unwrap()
                .iter()
                .any(|(b, o, v)| b == block_id && *o == offset && *v == expected_val)
        }
    }

    impl TransactionOperations for MockTransaction {
        fn pin(&self, block_id: &BlockId) {
            dbg!("Pinning block {:?}", block_id);
        }

        fn unpin(&self, block_id: &BlockId) {
            dbg!("Unpinning block {:?}", block_id);
        }

        fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, _log: bool) {
            dbg!(
                "Setting int at block {:?} offset {} to {}",
                block_id,
                offset,
                val
            );
            self.modified_ints
                .lock()
                .unwrap()
                .push((block_id.clone(), offset, val));
        }

        fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, _log: bool) {
            dbg!(
                "Setting string at block {:?} offset {} to {}",
                block_id,
                offset,
                val
            );
            self.modified_strings
                .lock()
                .unwrap()
                .push((block_id.clone(), offset, val.to_string()));
        }
    }

    #[test]
    fn test_rollback_with_int() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);

        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mock_tx = MockTransaction::new();
        let test_block = BlockId::new("test.txt".to_string(), 1);

        // Write some log records that will need to be rolled back
        let set_int_record = LogRecord::SetInt {
            txnum: 1,
            block_id: test_block.clone(),
            offset: 0,
            old_val: 100, // Original value before modification
        };
        set_int_record
            .write_log_record(Arc::clone(&db.log_manager))
            .unwrap();

        // Perform rollback
        recovery_manager.rollback(&mock_tx).unwrap();

        // Verify that the value was reset to the original value
        assert!(mock_tx.verify_int_was_reset(&test_block, 0, 100));
        assert_eq!(
            mock_tx.modified_ints.lock().unwrap().len(),
            1,
            "Should have exactly one modification"
        );
    }

    #[test]
    fn test_rollback_with_string() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mock_tx = MockTransaction::new();
        let test_block = BlockId::new("test.txt".to_string(), 1);

        //  Write some log records that will need to be rolled back
        let set_string_record = LogRecord::SetString {
            txnum: 1,
            block_id: test_block.clone(),
            offset: 0,
            old_val: "Hello World".to_string(),
        };
        set_string_record
            .write_log_record(Arc::clone(&db.log_manager))
            .unwrap();

        //   Perform rollback
        recovery_manager.rollback(&mock_tx).unwrap();

        //  Verify that the value was reset to the original value
        assert!(mock_tx.verify_string_was_reset(&test_block, 0, "Hello World".to_string()));
        assert_eq!(
            mock_tx.modified_strings.lock().unwrap().len(),
            1,
            "Should have exactly one modification"
        );
    }
}

/// The container for all the different types of log records that are written to the WAL
#[derive(Clone)]
enum LogRecord {
    Start(usize),
    Commit(usize),
    Rollback(usize),
    Checkpoint,
    SetInt {
        txnum: usize,
        block_id: BlockId,
        offset: usize,
        old_val: i32,
    },
    SetString {
        txnum: usize,
        block_id: BlockId,
        offset: usize,
        old_val: String,
    },
}

impl Display for LogRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LogRecord::Start(txnum) => write!(f, "Start({txnum})"),
            LogRecord::Commit(txnum) => write!(f, "Commit({txnum})"),
            LogRecord::Rollback(txnum) => write!(f, "Rollback({txnum})"),
            LogRecord::Checkpoint => write!(f, "Checkpoint"),
            LogRecord::SetInt {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetInt(txnum: {txnum}, block_id: {block_id:?}, offset: {offset}, old_val: {old_val})"
            ),
            LogRecord::SetString {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetString(txnum: {txnum}, block_id: {block_id:?}, offset: {offset}, old_val: {old_val})"
            ),
        }
    }
}

impl TryInto<Vec<u8>> for &LogRecord {
    type Error = Box<dyn Error>;

    fn try_into(self) -> Result<Vec<u8>, Self::Error> {
        let size = self.calculate_size();
        let int_value = self.discriminant();
        let mut page = Page::new(size);
        let mut pos = 0;
        page.set_int(pos, int_value as i32);
        pos += 4;
        match self {
            LogRecord::Start(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Commit(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Rollback(txnum) => {
                page.set_int(pos, *txnum as i32);
            }
            LogRecord::Checkpoint => {}
            LogRecord::SetInt {
                txnum,
                block_id,
                offset,
                old_val,
            } => {
                page.set_int(pos, *txnum as i32);
                pos += 4;
                page.set_string(pos, &block_id.filename);
                pos += 4 + block_id.filename.len();
                page.set_int(pos, block_id.block_num as i32);
                pos += 4;
                page.set_int(pos, *offset as i32);
                pos += 4;
                page.set_int(pos, *old_val);
            }
            LogRecord::SetString {
                txnum,
                block_id,
                offset,
                old_val,
            } => {
                page.set_int(pos, *txnum as i32);
                pos += 4;
                page.set_string(pos, &block_id.filename);
                pos += 4 + block_id.filename.len();
                page.set_int(pos, block_id.block_num as i32);
                pos += 4;
                page.set_int(pos, *offset as i32);
                pos += 4;
                page.set_string(pos, old_val);
            }
        }
        Ok(page.contents)
    }
}

impl TryFrom<Vec<u8>> for LogRecord {
    type Error = Box<dyn Error>;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        let page = Page::from_bytes(value);
        let mut pos = 0;
        let discriminant = page.get_int(pos);
        pos += 4;

        match discriminant {
            0 => Ok(LogRecord::Start(page.get_int(pos) as usize)),
            1 => Ok(LogRecord::Commit(page.get_int(pos) as usize)),
            2 => Ok(LogRecord::Rollback(page.get_int(pos) as usize)),
            3 => Ok(LogRecord::Checkpoint),
            4 => {
                let txnum = page.get_int(pos) as usize;
                pos += 4;
                let filename = page.get_string(pos);
                pos += 4 + filename.len();
                let block_num = page.get_int(pos) as usize;
                pos += 4;
                let offset = page.get_int(pos) as usize;
                pos += 4;
                let old_val = page.get_int(pos);

                Ok(LogRecord::SetInt {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                })
            }
            5 => {
                let txnum = page.get_int(pos) as usize;
                pos += 4;
                let filename = page.get_string(pos);
                pos += 4 + filename.len();
                let block_num = page.get_int(pos) as usize;
                pos += 4;
                let offset = page.get_int(pos) as usize;
                pos += 4;
                let old_val = page.get_string(pos);

                Ok(LogRecord::SetString {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                })
            }
            _ => Err("Invalid log record type".into()),
        }
    }
}

impl LogRecord {
    // Size constants for different components
    const DISCRIMINANT_SIZE: usize = Page::INT_BYTES;
    const TXNUM_SIZE: usize = Page::INT_BYTES;
    const OFFSET_SIZE: usize = Page::INT_BYTES;
    const BLOCK_NUM_SIZE: usize = Page::INT_BYTES;
    const STR_LEN_SIZE: usize = Page::INT_BYTES;

    fn calculate_size(&self) -> usize {
        let base_size = Self::DISCRIMINANT_SIZE; // Every record has a discriminant
        match self {
            LogRecord::Start(_) | LogRecord::Commit(_) | LogRecord::Rollback(_) => {
                base_size + Self::TXNUM_SIZE
            }
            LogRecord::Checkpoint => base_size,
            LogRecord::SetInt { block_id, .. } => {
                base_size
                    + Self::TXNUM_SIZE
                    + Self::STR_LEN_SIZE
                    + block_id.filename.len()
                    + Self::BLOCK_NUM_SIZE
                    + Self::OFFSET_SIZE
                    + Self::TXNUM_SIZE // NOTE: old_val size (be careful of this changing)
            }
            LogRecord::SetString {
                block_id, old_val, ..
            } => {
                base_size
                    + Self::TXNUM_SIZE
                    + Self::STR_LEN_SIZE
                    + block_id.filename.len()
                    + Self::BLOCK_NUM_SIZE
                    + Self::OFFSET_SIZE
                    + Self::STR_LEN_SIZE
                    + old_val.len()
            }
        }
    }

    /// Get the discriminant value for the log record
    fn discriminant(&self) -> u32 {
        match self {
            LogRecord::Start(_) => 0,
            LogRecord::Commit(_) => 1,
            LogRecord::Rollback(_) => 2,
            LogRecord::Checkpoint => 3,
            LogRecord::SetInt { .. } => 4,
            LogRecord::SetString { .. } => 5,
        }
    }

    /// Get the transaction number associated with this log record
    /// Will panic for certain log records
    fn get_tx_num(&self) -> usize {
        match self {
            LogRecord::Start(txnum) => *txnum,
            LogRecord::Commit(txnum) => *txnum,
            LogRecord::Checkpoint => usize::MAX, //  dummy value
            LogRecord::Rollback(txnum) => *txnum,
            LogRecord::SetInt { txnum, .. } => *txnum,
            LogRecord::SetString { txnum, .. } => *txnum,
        }
    }

    /// Undo the operation performed by this log record
    /// This is used by the [`RecoveryManager`] when performing a recovery
    fn undo(&self, tx: &dyn TransactionOperations) {
        match self {
            LogRecord::Start(_) => (),    //  no-op
            LogRecord::Commit(_) => (),   //  no-op
            LogRecord::Rollback(_) => (), //  no-op
            LogRecord::Checkpoint => (),  //  no-op
            LogRecord::SetInt {
                block_id,
                offset,
                old_val,
                ..
            } => {
                tx.pin(block_id);
                tx.set_int(block_id, *offset, *old_val, false);
                tx.unpin(block_id);
            }
            LogRecord::SetString {
                block_id,
                offset,
                old_val,
                ..
            } => {
                tx.pin(block_id);
                tx.set_string(block_id, *offset, old_val, false);
                tx.unpin(block_id);
            }
        }
    }

    /// Serialize the log record to bytes and write it to the log file
    fn write_log_record(&self, log_manager: Arc<Mutex<LogManager>>) -> Result<Lsn, Box<dyn Error>> {
        let bytes: Vec<u8> = self.try_into()?;
        Ok(log_manager.lock().unwrap().append(bytes))
    }

    /// Read the bytes from the log file and deserialize them into a [`LogRecord`]
    fn from_bytes(bytes: Vec<u8>) -> Result<LogRecord, Box<dyn Error>> {
        let result: LogRecord = bytes.try_into()?;
        Ok(result)
    }
}

/// Wrapper for the value contained in the hash map of the [`BufferList`]
#[derive(Debug)]
struct HashMapValue {
    buffer: Arc<BufferFrame>,
    count: usize,
}

/// A wrapper to maintain the list of [`Buffer`] being used by the [`Transaction`]
/// It uses the [`BufferManager`] internally to maintain metadata
#[derive(Debug)]
struct BufferList {
    /// Maps block ID's to buffers and their pin contents
    buffers: RefCell<HashMap<BlockId, HashMapValue>>,
    /// Shared buffer manager for pinning/unpinning operations
    buffer_manager: Arc<BufferManager>,
    /// Tracks whether the transaction has been committed
    txn_committed: Cell<bool>,
}

impl BufferList {
    pub fn new(buffer_manager: Arc<BufferManager>) -> Self {
        Self {
            buffers: RefCell::new(HashMap::new()),
            buffer_manager,
            txn_committed: Cell::new(false),
        }
    }

    /// Get the buffer associated with the provided block_id
    fn get_buffer(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
        self.buffers
            .borrow()
            .get(block_id)
            .map(|v| Arc::clone(&v.buffer))
    }

    /// Pin the buffer associated with the provided [`BlockId`]
    fn pin(&self, block_id: &BlockId) {
        let buffer = self.buffer_manager.pin(block_id).unwrap();
        self.buffers
            .borrow_mut()
            .entry(block_id.clone())
            .and_modify(|v| v.count += 1)
            .or_insert(HashMapValue { buffer, count: 1 });
    }

    /// Unpin the buffer associated with the provided [`BlockId`]
    fn unpin(&self, block_id: &BlockId) {
        // If transaction has committed/rolled back, BufferHandles may outlive the transaction
        // In that case, unpin is a no-op since unpin_all() already handled it
        if self.txn_committed.get() {
            return;
        }

        // Runtime assertion: ensure we're unpinning a block that was actually pinned
        if !self.buffers.borrow().contains_key(block_id) {
            panic!(
                "INVARIANT VIOLATION: Unpinning {block_id:?} that was never pinned or already fully unpinned"
            );
        }
        let buffer = Arc::clone(&self.buffers.borrow().get(block_id).unwrap().buffer);
        self.buffer_manager.unpin(buffer);
        let should_remove = {
            let mut buffers = self.buffers.borrow_mut();
            let v = buffers.get_mut(block_id).unwrap();
            v.count -= 1;
            v.count == 0
        };
        if should_remove {
            self.buffers.borrow_mut().remove(block_id);
        }
    }

    /// Unpin all the buffers in this [`BufferList`]
    fn unpin_all(&self) {
        let mut buffer_guard = self.buffers.borrow_mut();
        let buffers = buffer_guard.values();
        for value in buffers {
            for _ in 0..value.count {
                self.buffer_manager.unpin(Arc::clone(&value.buffer));
            }
        }
        buffer_guard.clear();

        // Mark as committed so subsequent BufferHandle drops become no-ops
        self.txn_committed.set(true);
    }

    /// Debug assertion to verify pin count invariants hold for this transaction
    ///
    /// Verifies:
    /// 1. BufferList count matches expected number of live handles for this transaction
    /// 2. BufferManager pin count >= BufferList count (other transactions may have pins too)
    #[cfg(debug_assertions)]
    #[cfg(test)]
    fn assert_pin_invariant(&self, block_id: &BlockId, expected_handles: usize) {
        let buffer_list_count = self
            .buffers
            .borrow()
            .get(block_id)
            .map(|v| v.count)
            .unwrap_or(0);

        // Invariant 1: This transaction's BufferList count should match expected handles
        assert_eq!(
            expected_handles, buffer_list_count,
            "Handle count mismatch for {block_id:?}: expected={expected_handles}, actual={buffer_list_count}"
        );

        // Invariant 2: BufferManager total pins >= this transaction's pins
        // (Other transactions may have pinned the same buffer)
        if let Some(buffer) = self.get_buffer(block_id) {
            let buffer_manager_count = buffer.pin_count();

            assert!(
                buffer_manager_count >= buffer_list_count,
                "Pin count invariant violated for {block_id:?}: BufferManager pins ({buffer_manager_count}) < BufferList count ({buffer_list_count}) for this transaction"
            );
        }
    }
}

#[cfg(test)]
mod buffer_list_tests {
    use std::sync::{Arc, Mutex};

    use crate::{test_utils::TestDir, BlockId, BufferList, BufferManager, FileManager, LogManager};

    #[test]
    fn test_buffer_list_functionality() {
        let dir = TestDir::new("buffer_list_tests");
        let file_manager: super::SharedFS = Arc::new(Mutex::new(Box::new(
            FileManager::new(&dir, true).unwrap(),
        )));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            "buffer_list_tests_log_file",
        )));
        let buffer_manager = Arc::new(BufferManager::new(file_manager, log_manager, 4));
        let buffer_list = BufferList::new(buffer_manager);

        //  check that there are no buffers in the buffer list initially
        let block_id = BlockId {
            filename: "testfile".to_string(),
            block_num: 1,
        };
        assert!(buffer_list.get_buffer(&block_id).is_none());

        //  pinning a buffer and then attempting to fetch it should return the correct one
        buffer_list.pin(&block_id);
        assert!(buffer_list.get_buffer(&block_id).is_some());

        //  unpinning all buffers will empty the buffer list
        buffer_list.unpin_all();
        assert!(buffer_list.buffers.borrow().is_empty());
    }
}

#[derive(Debug)]
pub(crate) struct FrameMeta {
    pub(crate) block_id: Option<BlockId>,
    pub(crate) pins: usize,
    pub(crate) txn: Option<usize>,
    pub(crate) lsn: Option<Lsn>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) prev_idx: Option<usize>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) next_idx: Option<usize>,
    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub(crate) index: usize,
    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub(crate) ref_bit: bool,
}

impl FrameMeta {
    fn new(index: usize) -> Self {
        #[cfg(not(any(feature = "replacement_lru", feature = "replacement_sieve")))]
        let _ = index;
        Self {
            block_id: None,
            pins: 0,
            txn: None,
            lsn: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            prev_idx: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            next_idx: None,
            #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
            index,
            #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
            ref_bit: false,
        }
    }

    fn pin(&mut self) -> bool {
        let was_zero = self.pins == 0;
        self.pins += 1;
        was_zero
    }

    fn unpin(&mut self) -> bool {
        assert!(self.pins > 0, "FrameMeta::unpin on zero pins");
        self.pins -= 1;
        self.pins == 0
    }

    fn reset_pins(&mut self) {
        self.pins = 0;
    }
}

#[derive(Debug)]
pub struct BufferFrame {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    page: RwLock<Page>,
    meta: Mutex<FrameMeta>,
}

impl BufferFrame {
    pub fn new(file_manager: SharedFS, log_manager: Arc<Mutex<LogManager>>, index: usize) -> Self {
        let size = file_manager.lock().unwrap().block_size();
        #[cfg(feature = "replacement_clock")]
        let _ = index; // Suppress unused warning when only clock is enabled
        Self {
            file_manager,
            log_manager,
            page: RwLock::new(Page::new(size)),
            meta: Mutex::new(FrameMeta::new(index)),
        }
    }

    pub(crate) fn lock_meta(&self) -> MutexGuard<'_, FrameMeta> {
        self.meta.lock().unwrap()
    }

    pub fn block_id_owned(&self) -> Option<BlockId> {
        self.lock_meta().block_id.clone()
    }

    pub fn pin_count(&self) -> usize {
        self.lock_meta().pins
    }

    #[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
    pub fn replacement_index(&self) -> usize {
        self.lock_meta().index
    }

    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn ref_bit(&self) -> bool {
        self.lock_meta().ref_bit
    }

    #[cfg(any(feature = "replacement_clock", feature = "replacement_sieve"))]
    pub fn set_ref_bit(&self, value: bool) {
        self.lock_meta().ref_bit = value;
    }

    pub fn read_page(&self) -> RwLockReadGuard<'_, Page> {
        self.page.read().unwrap()
    }

    pub fn write_page(&self) -> RwLockWriteGuard<'_, Page> {
        self.page.write().unwrap()
    }

    /// Mark that this buffer has been modified and set associated metadata for the modifying transaction
    fn set_modified(&self, txn_num: usize, lsn: usize) {
        let mut meta = self.lock_meta();
        meta.txn = Some(txn_num);
        meta.lsn = Some(lsn);
    }

    /// Check whether the buffer is pinned in memory
    fn is_pinned(&self) -> bool {
        self.lock_meta().pins > 0
    }

    fn flush_locked(&self, meta: &mut FrameMeta) {
        if let (Some(block_id), Some(lsn)) = (meta.block_id.clone(), meta.lsn) {
            self.log_manager.lock().unwrap().flush_lsn(lsn);
            let mut page_guard = self.page.write().unwrap();
            self.file_manager
                .lock()
                .unwrap()
                .write(&block_id, &mut *page_guard);
            meta.txn = None;
            meta.lsn = None;
        }
    }

    /// Modify this buffer to hold the contents of a different block
    /// This requires flushing the existing page contents, if any, to disk if dirty
    fn assign_to_block_locked(&self, meta: &mut FrameMeta, block_id: &BlockId) {
        self.flush_locked(meta);
        meta.block_id = Some(block_id.clone());
        let mut page_guard = self.page.write().unwrap();
        self.file_manager
            .lock()
            .unwrap()
            .read(block_id, &mut *page_guard);
        meta.reset_pins();
        meta.txn = None;
        meta.lsn = None;
    }
}

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
impl IntrusiveNode for FrameMeta {
    fn prev(&self) -> Option<usize> {
        self.prev_idx
    }

    fn set_prev(&mut self, prev: Option<usize>) {
        self.prev_idx = prev
    }

    fn next(&self) -> Option<usize> {
        self.next_idx
    }

    fn set_next(&mut self, next: Option<usize>) {
        self.next_idx = next
    }
}

#[cfg(any(feature = "replacement_lru", feature = "replacement_sieve"))]
impl<'a> IntrusiveNode for MutexGuard<'a, FrameMeta> {
    fn prev(&self) -> Option<usize> {
        self.prev_idx
    }

    fn set_prev(&mut self, prev: Option<usize>) {
        self.prev_idx = prev;
    }

    fn next(&self) -> Option<usize> {
        self.next_idx
    }

    fn set_next(&mut self, next: Option<usize>) {
        self.next_idx = next;
    }
}

/// Statistics for buffer pool performance tracking
#[derive(Debug)]
pub struct BufferStats {
    pub hits: AtomicUsize,
    pub misses: AtomicUsize,
}

impl Default for BufferStats {
    fn default() -> Self {
        Self::new()
    }
}

impl BufferStats {
    pub fn new() -> Self {
        Self {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    pub fn get(&self) -> (usize, usize) {
        (
            self.hits.load(std::sync::atomic::Ordering::Relaxed),
            self.misses.load(std::sync::atomic::Ordering::Relaxed),
        )
    }

    pub fn reset(&self) {
        self.hits.store(0, std::sync::atomic::Ordering::Relaxed);
        self.misses.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    pub fn hit_rate(&self) -> f64 {
        let (hits, misses) = self.get();
        let total = hits + misses;
        if total == 0 {
            0.0
        } else {
            (hits as f64 / total as f64) * 100.0
        }
    }
}

struct LatchTableGuard<'a> {
    latch_table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    block_id: BlockId,
    latch: Arc<Mutex<()>>,
}

impl<'a> LatchTableGuard<'a> {
    pub fn new(table: &'a Mutex<HashMap<BlockId, Arc<Mutex<()>>>>, block_id: &BlockId) -> Self {
        let latch = {
            let mut guard = table.lock().unwrap();
            let block_latch_ptr = guard
                .entry(block_id.clone())
                .or_insert_with(|| Arc::new(Mutex::new(())));
            Arc::clone(block_latch_ptr)
        };
        Self {
            latch_table: table,
            block_id: block_id.clone(),
            latch,
        }
    }

    fn lock(&'a self) -> MutexGuard<'a, ()> {
        self.latch.lock().unwrap()
    }
}

impl Drop for LatchTableGuard<'_> {
    fn drop(&mut self) {
        let mut block_latch_table_guard = self.latch_table.lock().unwrap();
        if let Some(ptr) = block_latch_table_guard.get(&self.block_id) {
            if Arc::strong_count(ptr) == 2 {
                block_latch_table_guard.remove(&self.block_id);
            }
        }
    }
}

#[derive(Debug)]
pub struct BufferManager {
    file_manager: SharedFS,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<BufferFrame>>,
    num_available: Mutex<usize>,
    cond: Condvar,
    stats: OnceLock<Arc<BufferStats>>,
    latch_table: Mutex<HashMap<BlockId, Arc<Mutex<()>>>>,
    resident_table: Mutex<HashMap<BlockId, Weak<BufferFrame>>>,
    policy: PolicyState,
}

impl BufferManager {
    const MAX_TIME: u64 = 10; //  10 seconds
    pub fn new(
        file_manager: SharedFS,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        let buffer_pool: Vec<Arc<BufferFrame>> = (0..num_buffers)
            .map(|index| {
                Arc::new(BufferFrame::new(
                    Arc::clone(&file_manager),
                    Arc::clone(&log_manager),
                    index,
                ))
            })
            .collect();
        let policy = PolicyState::new(&buffer_pool);

        Self {
            file_manager,
            log_manager,
            buffer_pool,
            num_available: Mutex::new(num_buffers),
            cond: Condvar::new(),
            stats: OnceLock::new(),
            latch_table: Mutex::new(HashMap::new()),
            resident_table: Mutex::new(HashMap::new()),
            policy,
        }
    }

    /// Enable statistics collection for benchmarking purposes.
    ///
    /// Idempotent - calling multiple times has no effect after first call.
    /// Zero-cost when not enabled. Thread-safe.
    pub fn enable_stats(&self) {
        let _ = self.stats.set(Arc::new(BufferStats::new()));
    }

    /// Get current buffer pool statistics as (hits, misses) tuple.
    ///
    /// Returns `None` if stats not enabled via `enable_stats()`.
    /// Use this for simple stat queries; use `stats()` for access to full `BufferStats` API.
    pub fn get_stats(&self) -> Option<(usize, usize)> {
        self.stats.get().map(|s| s.get())
    }

    /// Get reference to `BufferStats` struct for advanced queries (e.g., `hit_rate()`).
    ///
    /// Returns `None` if stats not enabled. Prefer `get_stats()` for simple (hits, misses) access.
    pub fn stats(&self) -> Option<&Arc<BufferStats>> {
        self.stats.get()
    }

    /// Reset hit/miss counters to zero.
    ///
    /// No-op if stats not enabled. Useful for isolating measurements between benchmark phases.
    pub fn reset_stats(&self) {
        if let Some(stats) = self.stats.get() {
            stats.reset();
        }
    }

    /// Returns the number of unpinned buffers, that is buffers with no pages pinned to them
    pub fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    /// Get the file manager for monitoring purposes
    pub fn file_manager(&self) -> SharedFS {
        Arc::clone(&self.file_manager)
    }

    /// Get the log manager for monitoring purposes
    pub fn log_manager(&self) -> Arc<Mutex<LogManager>> {
        Arc::clone(&self.log_manager)
    }

    /// Flushes the dirty buffers modified by this specific transaction
    fn flush_all(&self, txn_num: usize) {
        for buffer in &self.buffer_pool {
            let mut meta = buffer.lock_meta();
            if matches!(meta.txn, Some(t) if t == txn_num) {
                buffer.flush_locked(&mut meta);
            }
        }
    }

    /// Depends on the [`BufferManager::try_to_pin`] method to get a [`BufferFrame`] back
    /// This method will not perform any metadata operations on the buffer
    pub fn pin(&self, block_id: &BlockId) -> Result<Arc<BufferFrame>, Box<dyn Error>> {
        let start = Instant::now();
        loop {
            if let Some(buffer) = self.try_to_pin(block_id) {
                return Ok(buffer);
            }

            let mut avail = self.num_available.lock().unwrap();
            while *avail == 0 {
                let elapsed = start.elapsed();
                if elapsed >= Duration::from_secs(Self::MAX_TIME) {
                    return Err("Timed out waiting for buffer".into());
                }
                let timeout = Duration::from_secs(Self::MAX_TIME) - elapsed;
                let (guard, wait_res) = self.cond.wait_timeout(avail, timeout).unwrap();
                avail = guard;
                if wait_res.timed_out() {
                    return Err("Timed out waiting for buffer".into());
                }
            }
            drop(avail);
        }
    }

    /// The function has two clear paths laid out - hit path and miss path
    /// The hit path will attempt to find the buffer in the resident table and increase the pin count if found
    /// The miss path will call [`BufferManager::choose_unpinned_frame`] which will return the first unpinned
    /// frame.
    /// If all frames are occupied, return to the top level loop
    ///
    /// # ABA Race at Buffer Pool Layer
    ///
    /// There is a TOCTOU race between `find_existing_buffer()` releasing the lock and
    /// re-acquiring it here. A buffer can be evicted and reloaded with the SAME BlockId
    /// but DIFFERENT content (e.g., another transaction committed changes to disk, then
    /// the block was reloaded). The check below detects BlockId changes but not content
    /// changes for the same BlockId.
    ///
    /// **Why this is safe**: Transaction-level locks (via LockTable) prevent isolation
    /// violations. Transactions acquire slock/xlock on BlockId before calling pin(), and
    /// hold locks until commit/rollback (Strict 2PL). Even if a buffer is evicted and
    /// uncommitted data flushed to disk, other transactions cannot read it until locks
    /// are released. The buffer pool provides physical consistency; LockTable provides
    /// logical isolation.
    ///
    /// **Non-transactional access** (catalog reads, WAL, recovery) bypasses LockTable
    /// and could observe stale data from this race, but these operations are designed
    /// to handle such cases.
    fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<BufferFrame>> {
        //  Wrap the latch table in a guard which will prune it appropriately
        let latch_table_guard = LatchTableGuard::new(&self.latch_table, block_id);
        #[allow(unused_variables)]
        let block_latch = latch_table_guard.lock();

        //  check the resident table for the associated frame
        let frame_ptr = {
            let mut resident_guard = self.resident_table.lock().unwrap();
            match resident_guard.get(block_id) {
                Some(weak_frame_ptr) => match weak_frame_ptr.upgrade() {
                    Some(frame_ptr) => Some(frame_ptr),
                    None => {
                        //  this is a dangling pointer, clean it up and go back to top level loop
                        resident_guard.remove(block_id);
                        return None;
                    }
                },
                None => None,
            }
        };

        //  fast hit path, found the frame in the resident table
        if let Some(frame_ptr) = frame_ptr {
            {
                let mut meta_guard = self.record_hit(&frame_ptr, block_id)?;
                let was_unpinned = meta_guard.pin();
                if was_unpinned {
                    *self.num_available.lock().unwrap() -= 1;
                }
                if let Some(stats) = self.stats.get() {
                    stats
                        .hits
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
            return Some(frame_ptr);
        }

        //  slow miss path, did not find frame in resident table
        if let Some(stats) = self.stats.get() {
            stats
                .misses
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }

        let (tail_idx, mut meta_guard) = match self.evict_frame() {
            Some((idx, guard)) => (idx, guard),
            None => return None,
        };

        if let Some(old) = meta_guard.block_id.clone() {
            self.resident_table.lock().unwrap().remove(&old);
        }
        let frame = Arc::clone(&self.buffer_pool[tail_idx]);
        frame.assign_to_block_locked(&mut meta_guard, block_id);
        let became_pinned = meta_guard.pin();
        debug_assert!(became_pinned, "newly assigned frame must have zero pins");
        drop(meta_guard);

        self.policy.on_frame_assigned(&self.buffer_pool, tail_idx);

        self.resident_table
            .lock()
            .unwrap()
            .insert(block_id.clone(), Arc::downgrade(&frame));
        *self.num_available.lock().unwrap() -= 1;
        Some(frame)
    }

    /// Decrement the pin count for the provided buffer
    /// If all of the pins have been removed, managed metadata & notify waiting threads
    pub fn unpin(&self, frame: Arc<BufferFrame>) {
        let mut meta = frame.lock_meta();
        let became_unpinned = meta.unpin();
        if became_unpinned {
            *self.num_available.lock().unwrap() += 1;
            self.cond.notify_all();
        }
    }

    fn evict_frame(&self) -> Option<(usize, MutexGuard<'_, FrameMeta>)> {
        self.policy.evict_frame(&self.buffer_pool)
    }

    fn record_hit<'a>(
        &'a self,
        frame_ptr: &'a Arc<BufferFrame>,
        block_id: &BlockId,
    ) -> Option<MutexGuard<'a, FrameMeta>> {
        self.policy
            .record_hit(&self.buffer_pool, frame_ptr, block_id, &self.resident_table)
    }

    /// Debug assertion to verify buffer count invariants
    /// Checks that available + pinned buffers = pool size
    #[cfg(debug_assertions)]
    pub fn assert_buffer_count_invariant(&self) {
        let available = *self.num_available.lock().unwrap();

        // Count buffers with at least one pin
        let num_pinned_buffers: usize = self
            .buffer_pool
            .iter()
            .filter(|buf| buf.is_pinned())
            .count();

        assert_eq!(
            available + num_pinned_buffers,
            self.buffer_pool.len(),
            "Buffer count invariant violated: available={}, pinned_buffers={}, total={}",
            available,
            num_pinned_buffers,
            self.buffer_pool.len()
        );
    }
}

#[cfg(test)]
mod buffer_manager_tests {
    use crate::{BlockId, Page, SimpleDB};

    /// This test will assert that when the buffer pool swaps out a page from the buffer pool, it properly flushes those contents to disk
    /// and can then correctly read them back later
    #[test]
    fn test_buffer_replacement() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let buffer_manager = db.buffer_manager;

        //  Initialize the file with enough data
        let block_id = BlockId::new("testfile".to_string(), 1);
        let mut page = Page::new(400);
        page.set_int(80, 1);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);

        let buffer_manager_guard = &buffer_manager;

        //  Create a buffer for block 1 and modify it
        let buffer_1 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        {
            let mut page = buffer_1.write_page();
            page.set_int(80, 100);
        }
        buffer_1.set_modified(1, 0);
        buffer_manager_guard.unpin(buffer_1);

        //  force buffer replacement by pinning 3 new blocks
        let buffer_2 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 2))
            .unwrap();
        buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 3))
            .unwrap();
        buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 4))
            .unwrap();

        //  remove one of the buffers so block 1 can be read back in
        buffer_manager_guard.unpin(buffer_2);

        //  Read block 1 back from disk and verify it is the same
        let buffer_2 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        let page = buffer_2.read_page();
        assert_eq!(page.get_int(80), 100);
        drop(page);
        assert_eq!(buffer_manager.latch_table.lock().unwrap().len(), 0);
    }

    /// Concurrent stress test: multiple threads hammering same small working set
    /// Tests for:
    /// 1. Concurrent eviction races (multiple threads evicting/pinning same buffer slots)
    /// 2. Pin count correctness (concurrent pin/unpin on same BlockId)
    /// 3. Stats counter accuracy (AtomicUsize under concurrent updates)
    /// 4. No panics/deadlocks under high contention
    #[test]
    fn test_concurrent_buffer_pool_stress() {
        use std::thread;

        // Small buffer pool (4 buffers) with small working set (6 blocks)
        // Forces evictions and contention
        let (db, _test_dir) = SimpleDB::new_for_test(4, 5000);
        db.buffer_manager.enable_stats();

        let num_blocks = 6;
        let num_threads = 8;
        let ops_per_thread = 100;

        // Pre-create blocks on disk
        for i in 0..num_blocks {
            let block_id = BlockId::new("stressfile".to_string(), i);
            let mut page = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
            page.set_int(0, i as i32);
            db.file_manager.lock().unwrap().write(&block_id, &mut page);
        }

        // Spawn threads that all hammer the same small working set
        let handles: Vec<_> = (0..num_threads)
            .map(|thread_id| {
                let buffer_manager = db.buffer_manager.clone();
                thread::spawn(move || {
                    for op in 0..ops_per_thread {
                        // Each thread accesses all blocks in round-robin
                        // This creates maximum contention on buffer slots
                        let block_num = (thread_id + op) % num_blocks;
                        let block_id = BlockId::new("stressfile".to_string(), block_num);

                        // Pin block
                        let buffer = buffer_manager.pin(&block_id).unwrap();

                        // Verify we got the right block
                        assert_eq!(buffer.block_id_owned().unwrap(), block_id);
                        {
                            let page = buffer.read_page();
                            assert_eq!(page.get_int(0), block_num as i32);
                        }

                        // Unpin immediately to maximize churn
                        buffer_manager.unpin(buffer);
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in handles {
            handle.join().expect("Thread panicked during stress test");
        }

        // Verify stats counters are consistent with deterministic lower bounds
        // Note: Total may exceed num_threads * ops_per_thread because pin() can call
        // try_to_pin() multiple times (retries when waiting for buffers)
        if let Some(stats) = db.buffer_manager.stats() {
            let (hits, misses) = stats.get();
            let total = hits + misses;
            let min_expected = num_threads * ops_per_thread;

            assert!(
                total >= min_expected,
                "Stats counter sanity check failed: got {total} total accesses (hits={hits}, misses={misses}), expected at least {min_expected}"
            );

            // Misses: Must miss on first access to each unique block (6 blocks)
            assert!(
                misses >= num_blocks,
                "Expected at least {num_blocks} misses (cold start for {num_blocks} blocks), got {misses}"
            );

            // Hits: With 4 buffers for 6 blocks, even with thrashing, expect some hits
            // Conservative lower bound: ~12% hit rate under worst-case thrashing
            let min_hits = 100;
            assert!(
                hits >= min_hits,
                "Expected at least {min_hits} hits under contention (got {hits}), possible correctness issue"
            );

            // Verify buffer pool is consistent (all buffers unpinned)
            let available = db.buffer_manager.available();
            assert_eq!(
                available, 4,
                "Buffer pool inconsistent: expected 4 available buffers, got {available}"
            );
        }
    }
}

#[derive(Debug)]
pub struct LogManager {
    file_manager: SharedFS,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: usize,
    last_saved_lsn: usize,
}

impl LogManager {
    pub fn new(file_manager: SharedFS, log_file: &str) -> Self {
        let bytes = vec![0; file_manager.lock().unwrap().block_size()];
        let mut log_page = Page::from_bytes(bytes);
        let log_size = file_manager.lock().unwrap().length(log_file.to_string());
        let current_block = if log_size == 0 {
            LogManager::append_new_block(&file_manager, log_file, &mut log_page)
        } else {
            let block = BlockId {
                filename: log_file.to_string(),
                block_num: log_size - 1,
            };
            file_manager.lock().unwrap().read(&block, &mut log_page);
            block
        };
        Self {
            file_manager,
            log_file: log_file.to_string(),
            log_page,
            current_block,
            latest_lsn: 0,
            last_saved_lsn: 0,
        }
    }

    /// Determine if this Lsn has been flushed to disk, and flush it if it hasn't
    pub fn flush_lsn(&mut self, lsn: Lsn) {
        if self.last_saved_lsn >= lsn {
            return;
        }
        self.flush_to_disk();
    }

    /// Write the bytes from log_page to disk for the current_block
    /// Update the last_saved_lsn before returning
    fn flush_to_disk(&mut self) {
        self.file_manager
            .lock()
            .unwrap()
            .write(&self.current_block, &mut self.log_page);

        self.file_manager.lock().unwrap().sync(&self.log_file);
        self.file_manager.lock().unwrap().sync_directory();

        self.last_saved_lsn = self.latest_lsn;
    }

    /// Write the log_record to the log page
    /// First, check if there is enough space
    pub fn append(&mut self, log_record: Vec<u8>) -> Lsn {
        let mut boundary = self.log_page.get_int(0) as usize;
        let bytes_needed = log_record.len() + Page::INT_BYTES;
        if boundary.saturating_sub(bytes_needed) < Page::INT_BYTES {
            self.flush_to_disk();
            self.current_block = LogManager::append_new_block(
                &self.file_manager,
                &self.log_file,
                &mut self.log_page,
            );
            boundary = self.log_page.get_int(0) as usize;
        }

        let record_pos = boundary - bytes_needed;
        self.log_page.set_bytes(record_pos, &log_record);
        self.log_page.set_int(0, record_pos as i32);
        self.latest_lsn += 1;
        self.latest_lsn
    }

    /// Append a new block to the file maintained by the log manager
    /// This involves initializing a new block, writing a boundary pointer to it and writing the block to disk
    fn append_new_block(file_manager: &SharedFS, log_file: &str, log_page: &mut Page) -> BlockId {
        let block_id = file_manager.lock().unwrap().append(log_file.to_string());
        log_page.set_int(
            0,
            file_manager
                .lock()
                .unwrap()
                .block_size()
                .try_into()
                .unwrap(),
        );
        file_manager.lock().unwrap().write(&block_id, log_page);
        block_id
    }

    fn iterator(&mut self) -> LogIterator {
        self.flush_to_disk();
        LogIterator::new(
            Arc::clone(&self.file_manager),
            BlockId::new(self.log_file.clone(), self.current_block.block_num),
        )
    }
}

pub struct LogIterator {
    file_manager: SharedFS,
    current_block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    pub fn new(file_manager: SharedFS, current_block: BlockId) -> Self {
        let block_size = file_manager.lock().unwrap().block_size();
        let mut page = Page::new(block_size);
        file_manager.lock().unwrap().read(&current_block, &mut page);
        let boundary = page.get_int(0) as usize;

        Self {
            file_manager,
            current_block,
            page,
            current_pos: boundary,
            boundary,
        }
    }

    pub fn move_to_block(&mut self) {
        self.file_manager
            .lock()
            .unwrap()
            .read(&self.current_block, &mut self.page);
        self.boundary = self.page.get_int(0) as usize;
        self.current_pos = self.boundary;
    }
}

impl Iterator for LogIterator {
    type Item = Vec<u8>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_pos >= self.file_manager.lock().unwrap().block_size() {
            if self.current_block.block_num == 0 {
                return None; //  no more blocks
            }
            self.current_block = BlockId {
                filename: self.current_block.filename.to_string(),
                block_num: self.current_block.block_num - 1,
            };
            self.move_to_block();
        }
        //  Read the record
        let record = self.page.get_bytes(self.current_pos);
        self.current_pos += Page::INT_BYTES + record.len();
        Some(record)
    }
}

impl IntoIterator for LogManager {
    type Item = Vec<u8>;
    type IntoIter = LogIterator;

    fn into_iter(mut self) -> Self::IntoIter {
        self.iterator()
    }
}

#[cfg(test)]
mod log_manager_tests {
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    use crate::{LogManager, Page, SimpleDB};

    fn create_log_record(s: &str, n: usize) -> Vec<u8> {
        let string_bytes = s.as_bytes();
        let total_size = Page::INT_BYTES + string_bytes.len() + Page::INT_BYTES;
        let mut record = Vec::with_capacity(total_size);

        record
            .write_all(&(string_bytes.len() as i32).to_be_bytes())
            .unwrap();
        record.write_all(string_bytes).unwrap();
        record.write_all(&n.to_be_bytes()).unwrap();
        record
    }

    fn create_log_records(log_manager: Arc<Mutex<LogManager>>, start: usize, end: usize) {
        dbg!("creating records");
        for i in start..=end {
            let record = create_log_record(&format!("record{i}"), i + 100);
            let lsn = log_manager.lock().unwrap().append(record);
            print!("{lsn} ");
        }
        println!();
    }

    /// Print the log records in the log file
    /// This accepts a counter and uses that counter to decide when to break because the metadata manager writes some logs
    /// into the log file and that complicates reading back logs for now
    fn print_log_records(log_manager: Arc<Mutex<LogManager>>, message: &str, count: usize) {
        dbg!("Message: ", &message);
        let iter = log_manager.lock().unwrap().iterator();
        let mut counter = 0;

        for record in iter {
            let length = i32::from_be_bytes(record[..4].try_into().unwrap());
            let string = String::from_utf8(record[4..4 + length as usize].to_vec()).unwrap();
            let n = usize::from_be_bytes(record[4 + length as usize..].try_into().unwrap());
            dbg!("String: ", &string, "Int: ", &n);
            counter += 1;
            if counter == count {
                break;
            }
        }
    }

    #[test]
    fn test_log_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(3, 5000);
        let log_manager = db.log_manager;

        create_log_records(Arc::clone(&log_manager), 1, 35);
        print_log_records(
            Arc::clone(&log_manager),
            "The log file now has these records:",
            35,
        );
        create_log_records(Arc::clone(&log_manager), 36, 70);
        log_manager.lock().unwrap().flush_lsn(65);
        print_log_records(log_manager, "The log file now has these records:", 35);
    }
}

/// The block id container that contains a specific block number for a specific file
#[derive(Debug, Eq, PartialEq, Clone, Hash)]
pub struct BlockId {
    filename: String,
    block_num: usize,
}

impl BlockId {
    pub fn new(filename: String, block_num: usize) -> Self {
        Self {
            filename,
            block_num,
        }
    }
}

/// The page struct that contains the contents of a page
#[derive(Debug)]
pub struct Page {
    pub contents: Vec<u8>,
}

impl Page {
    const INT_BYTES: usize = 4;

    pub fn new(blocksize: usize) -> Self {
        Self {
            contents: vec![0; blocksize],
        }
    }

    /// Create a new page from the given bytes
    fn from_bytes(bytes: Vec<u8>) -> Self {
        Self { contents: bytes }
    }

    /// Get an integer from the page at the given offset
    fn get_int(&self, offset: usize) -> i32 {
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        i32::from_be_bytes(bytes)
    }

    /// Set an integer at the given offset
    pub fn set_int(&mut self, offset: usize, n: i32) {
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&n.to_be_bytes());
    }

    /// Get a slice of bytes from the page at the given offset. Read the length and then the bytes
    fn get_bytes(&self, mut offset: usize) -> Vec<u8> {
        let _length_bytes = &self.contents[offset..offset + Self::INT_BYTES];
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        let length = u32::from_be_bytes(bytes) as usize;
        offset += Self::INT_BYTES;
        self.contents[offset..offset + length].to_vec()
    }

    /// Set a slice of bytes at the given offset. Write the length and then the bytes
    fn set_bytes(&mut self, mut offset: usize, bytes: &[u8]) {
        let length = bytes.len() as u32;
        let _length_bytes = length.to_be_bytes();
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&length.to_be_bytes());
        offset += Self::INT_BYTES;
        self.contents[offset..offset + bytes.len()].copy_from_slice(bytes);
    }

    /// Get a string from the page at the given offset
    fn get_string(&self, offset: usize) -> String {
        let bytes = self.get_bytes(offset);
        String::from_utf8(bytes).unwrap()
    }

    /// Set a string at the given offset
    fn set_string(&mut self, offset: usize, string: &str) {
        self.set_bytes(offset, string.as_bytes());
    }
}

#[cfg(test)]
mod page_tests {
    use super::*;
    #[test]
    fn test_page_int_operations() {
        let mut page = Page::new(4096);
        page.set_int(100, 4000);
        assert_eq!(page.get_int(100), 4000);

        page.set_int(200, -67890);
        assert_eq!(page.get_int(200), -67890);

        page.set_int(200, 1);
        assert_eq!(page.get_int(200), 1);
    }

    #[test]
    fn test_page_string_operations() {
        let mut page = Page::new(4096);
        page.set_string(100, "Hello");
        assert_eq!(page.get_string(100), "Hello");

        page.set_string(200, "World");
        assert_eq!(page.get_string(200), "World");
    }
}

/// Trait defining the file system interface for database operations
pub trait FileSystemInterface: std::fmt::Debug {
    fn block_size(&self) -> usize;
    fn length(&mut self, filename: String) -> usize;
    fn read(&mut self, block_id: &BlockId, page: &mut Page);
    fn write(&mut self, block_id: &BlockId, page: &mut Page);
    fn append(&mut self, filename: String) -> BlockId;
    fn sync(&mut self, filename: &str);
    fn sync_directory(&mut self);
}

/// The file manager struct that manages the files in the database
#[derive(Debug)]
struct FileManager {
    db_directory: PathBuf,
    open_files: HashMap<String, File>,
    directory_fd: File,
}

impl FileManager {
    fn new<P>(db_directory: &P, clean: bool) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let db_path = db_directory.as_ref().to_path_buf();
        fs::create_dir_all(&db_path)?;

        if clean {
            for entry in fs::read_dir(&db_path)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    fs::remove_file(entry.path())?;
                }
            }
        }

        let directory_fd = File::open(&db_path)?;

        Ok(Self {
            db_directory: db_path,
            open_files: HashMap::new(),
            directory_fd,
        })
    }

    /// Get the file handle for the file with the given filename or create it if it doesn't exist
    fn get_file(&mut self, filename: &str) -> File {
        let full_path = self.db_directory.join(filename);
        let full_path_str = full_path.to_string_lossy().to_string();
        self.open_files
            .entry(full_path_str)
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(full_path)
                    .expect("Failed to open file")
            })
            .try_clone()
            .unwrap()
    }
}
impl FileSystemInterface for FileManager {
    fn block_size(&self) -> usize {
        crate::page::PAGE_SIZE_BYTES as usize
    }

    fn length(&mut self, filename: String) -> usize {
        let file = self.get_file(&filename);
        let metadata = file.metadata().unwrap();
        (metadata.len() as usize) / (crate::page::PAGE_SIZE_BYTES as usize)
    }

    fn read(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * crate::page::PAGE_SIZE_BYTES as usize) as u64,
        ))
        .unwrap();
        match file.read_exact(&mut page.contents) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                page.contents = vec![0; crate::page::PAGE_SIZE_BYTES as usize];
            }
            Err(e) => panic!("Failed to read from file {e}"),
        }
    }

    fn write(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * crate::page::PAGE_SIZE_BYTES as usize) as u64,
        ))
        .unwrap();
        file.write_all(&page.contents).unwrap();
    }

    /// Append a new empty block to the file
    fn append(&mut self, filename: String) -> BlockId {
        let new_blk_num = self.length(filename.clone());
        let block_id = BlockId::new(filename.clone(), new_blk_num);
        let buffer = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
        let mut file = self.get_file(&filename);
        file.seek(io::SeekFrom::Start(
            (new_blk_num * crate::page::PAGE_SIZE_BYTES as usize) as u64,
        ))
            .unwrap();
        file.write_all(&buffer.contents).unwrap();
        block_id
    }

    /// Sync the file with the disk to ensure durability
    fn sync(&mut self, filename: &str) {
        let file = self.get_file(filename);
        file.sync_all().unwrap();
    }

    /// Sync the directory with the disk to ensure durability
    fn sync_directory(&mut self) {
        self.directory_fd.sync_all().unwrap();
    }
}

#[cfg(test)]
mod mock_file_manager {
    use super::*;
    use std::collections::HashMap;

    #[derive(Debug, Clone)]
    struct MockBlock {
        data: Vec<u8>,
        synced: bool,
    }

    #[derive(Debug, Clone)]
    struct MockFile {
        blocks: Vec<MockBlock>,
        file_synced: bool,
    }

    #[derive(Debug)]
    pub struct MockFileManager {
        files: HashMap<String, MockFile>,
        directory_synced: bool,
        crashed: bool,
    }

    impl MockFileManager {
        pub fn new() -> Self {
            Self {
                files: HashMap::new(),
                directory_synced: false,
                crashed: false,
            }
        }

        /// Simulate a system crash - discards all unsynced data
        /// Files with unsynced directory entries disappear entirely
        pub fn simulate_crash(&mut self) {
            if !self.directory_synced {
                self.files.clear();
            } else {
                for (_filename, file) in self.files.iter_mut() {
                    file.blocks.retain(|block| block.synced);
                    file.file_synced = false; // Reset sync state after crash
                }
            }

            self.crashed = true;
        }

        pub fn restore_from_crash(&mut self) {
            self.crashed = false;
        }

        fn ensure_file_exists(&mut self, filename: &str) {
            if !self.files.contains_key(filename) {
                self.files.insert(
                    filename.to_string(),
                    MockFile {
                        blocks: Vec::new(),
                        file_synced: false,
                    },
                );
                self.directory_synced = false;
            }
        }

        fn ensure_block_exists(&mut self, filename: &str, block_num: usize) {
            self.ensure_file_exists(filename);
            let file = self.files.get_mut(filename).unwrap();

            while file.blocks.len() <= block_num {
                file.blocks.push(MockBlock {
                    data: vec![0; crate::page::PAGE_SIZE_BYTES as usize],
                    synced: false,
                });
            }
        }
    }

    impl FileSystemInterface for MockFileManager {
        fn block_size(&self) -> usize {
            crate::page::PAGE_SIZE_BYTES as usize
        }

        fn length(&mut self, filename: String) -> usize {
            self.files
                .get(&filename)
                .map_or(0, |file| file.blocks.len())
        }

        fn read(&mut self, block_id: &BlockId, page: &mut Page) {
            if self.crashed {
                panic!("Cannot read from crashed file system");
            }

            if !self.files.contains_key(&block_id.filename) {
                page.contents.fill(0);
                return;
            }

            self.ensure_block_exists(&block_id.filename, block_id.block_num);
            let file = self.files.get(&block_id.filename).unwrap();

            if block_id.block_num < file.blocks.len() {
                let block = &file.blocks[block_id.block_num];
                page.contents.copy_from_slice(&block.data);
            } else {
                page.contents.fill(0);
            }
        }

        fn write(&mut self, block_id: &BlockId, page: &mut Page) {
            if self.crashed {
                panic!("Cannot write to crashed file system");
            }

            self.ensure_block_exists(&block_id.filename, block_id.block_num);
            let file = self.files.get_mut(&block_id.filename).unwrap();

            file.blocks[block_id.block_num] = MockBlock {
                data: page.contents.clone(),
                synced: false, // Write only goes to buffer, not synced
            };
        }

        fn append(&mut self, filename: String) -> BlockId {
            if self.crashed {
                panic!("Cannot append to crashed file system");
            }

            self.ensure_file_exists(&filename);
            let file = self.files.get_mut(&filename).unwrap();
            let block_num = file.blocks.len();

            file.blocks.push(MockBlock {
                data: vec![0; crate::page::PAGE_SIZE_BYTES as usize],
                synced: false,
            });

            BlockId::new(filename, block_num)
        }

        fn sync(&mut self, filename: &str) {
            if self.crashed {
                panic!("Cannot sync crashed file system");
            }

            if let Some(file) = self.files.get_mut(filename) {
                for block in file.blocks.iter_mut() {
                    block.synced = true;
                }
                file.file_synced = true;
            }
        }

        fn sync_directory(&mut self) {
            if self.crashed {
                panic!("Cannot sync crashed file manager");
            }
            self.directory_synced = true;
        }
    }
}

#[cfg(test)]
mod file_manager_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{test_utils::TestDir, FileManager, FileSystemInterface};

    fn setup() -> (TestDir, FileManager) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let dir = TestDir::new(format!("/tmp/test_db_{timestamp}_{thread_id:?}"));
        let file_manger = FileManager::new(&dir, true).unwrap();
        (dir, file_manger)
    }

    #[test]
    fn test_file_creation() {
        let (_temp_dir, mut file_manager) = setup();

        let filename = "test_file";
        file_manager.get_file(filename);

        let full_path = file_manager.db_directory.join(filename);
        let full_path_str = full_path.to_string_lossy().to_string();
        assert!(file_manager.open_files.contains_key(&full_path_str));
    }

    #[test]
    fn test_append_and_length() {
        let (_temp_dir, mut file_manager) = setup();

        let filename = "testfile".to_string();
        assert_eq!(file_manager.length(filename.clone()), 0);

        let block_id = file_manager.append(filename.clone());
        assert_eq!(block_id.block_num, 0);
        assert_eq!(file_manager.length(filename.clone()), 1);

        let block_id_2 = file_manager.append(filename.clone());
        assert_eq!(block_id_2.block_num, 1);
        assert_eq!(file_manager.length(filename), 2);
    }
}

#[cfg(test)]
mod durability_tests {
    use super::*;
    use mock_file_manager::MockFileManager;

    #[test]
    fn test_mock_filesystem_demonstrates_durability_flaw() {
        let mut mock_fs = MockFileManager::new();
        let block_id = BlockId::new("test_file".to_string(), 42);
        let mut page = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
        page.set_int(0, 42);
        page.set_string(4, "durability");

        // Phase 1: Write data without sync and simulate a crash which will discard all unsynced data
        mock_fs.write(&block_id, &mut page);

        mock_fs.simulate_crash();
        mock_fs.restore_from_crash();

        // Phase 2: Try to read data after crash. Data cannot be recovered.
        let mut read_page = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
        mock_fs.read(&block_id, &mut read_page);

        let recovered_int = read_page.get_int(0);
        let recovered_string = read_page.get_string(4);

        assert_eq!(
            recovered_int, 0,
            "Data lost: int should be 0 after crash without sync"
        );
        assert_eq!(
            recovered_string, "",
            "Data lost: string should be empty after crash without sync"
        );
    }

    #[test]
    fn test_mock_filesystem_with_sync_preserves_data() {
        let mut mock_fs = MockFileManager::new();
        let block_id = BlockId::new("test_file".to_string(), 42);
        let mut page = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
        page.set_int(0, 42);
        page.set_string(4, "durability");

        // Phase 1: Write data AND sync
        mock_fs.write(&block_id, &mut page);
        mock_fs.sync("test_file");
        mock_fs.sync_directory();

        mock_fs.simulate_crash();
        mock_fs.restore_from_crash();

        // Phase 2: Read data after crash
        let mut read_page = Page::new(crate::page::PAGE_SIZE_BYTES as usize);
        mock_fs.read(&block_id, &mut read_page);

        let recovered_int = read_page.get_int(0);
        let recovered_string = read_page.get_string(4);

        assert_eq!(
            recovered_int, 42,
            "Data preserved: int should survive crash with sync"
        );
        assert_eq!(
            recovered_string, "durability",
            "Data preserved: string should survive crash with sync"
        );
    }
}

// Orphaned main function removed - CLI binary is in src/bin/simpledb-cli.rs

#[cfg(test)]
mod offset_smoke_tests {
    use super::*;

    #[test]
    fn prints_buffer_manager_num_available_offset() {
        let (db, _test_dir) = SimpleDB::new_for_test(8, 5000);
        let bm = Arc::clone(&db.buffer_manager);

        println!(
            "num_available offset: 0x{:X}", core::mem::offset_of!(BufferManager, num_available)
        );

        // Get pointer to the actual BufferManager inside the Arc
        let bm_ptr = Arc::as_ptr(&bm);
        let bm_addr = bm_ptr as usize;

        // Calculate num_available address
        let num_avail_addr = unsafe { std::ptr::addr_of!((*bm_ptr).num_available) as usize };

        println!("BufferManager base: 0x{:X}", bm_addr);
        println!("num_available addr: 0x{:X}", num_avail_addr);
        println!("Offset: 0x{:X}", num_avail_addr - bm_addr);

        // Lock and check data address
        {
            let guard = bm.num_available.lock().unwrap();
            let data_addr = &*guard as *const usize as usize;
            println!("Data inside mutex:  0x{:X}", data_addr);
            println!(
                "Data offset from BufferManager: 0x{:X}",
                data_addr - bm_addr
            );
            println!(
                "Data offset from num_available: 0x{:X}",
                data_addr - num_avail_addr
            );
        }
    }
}
