#![allow(dead_code)]
#![allow(unused_variables)]

use std::{
    cell::RefCell,
    collections::{HashMap, HashSet, VecDeque},
    error::Error,
    fmt::Display,
    fs::{self, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{self, Read, Seek, Write},
    ops::Deref,
    path::{Path, PathBuf},
    sync::{atomic::AtomicU64, Arc, Condvar, Mutex, OnceLock},
    time::{Duration, Instant},
};
mod test_utils;
#[cfg(test)]
use test_utils::TestDir;
mod parser;

type LSN = usize;

/// The database struct
struct SimpleDB {
    db_directory: PathBuf,
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    metadata_manager: Arc<MetadataManager>,
}

impl SimpleDB {
    const LOG_FILE: &str = "simpledb.log";

    fn new<P: AsRef<Path>>(path: P, block_size: usize, num_buffers: usize, clean: bool) -> Self {
        let file_manager = Arc::new(Mutex::new(
            FileManager::new(&path, block_size, clean).unwrap(),
        ));
        let joined_path = path.as_ref().join(Self::LOG_FILE);
        let log_path = joined_path.to_str().unwrap();
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            log_path,
        )));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            num_buffers,
        )));
        let txn = Arc::new(Transaction::new(
            Arc::clone(&file_manager),
            Arc::clone(&log_manager),
            Arc::clone(&buffer_manager),
        ));
        let metadata_manager = Arc::new(MetadataManager::new(clean, Arc::clone(&txn)));
        txn.commit().unwrap();
        Self {
            db_directory: path.as_ref().to_path_buf(),
            log_manager,
            file_manager,
            buffer_manager,
            metadata_manager,
        }
    }

    fn new_tx(&self) -> Transaction {
        Transaction::new(
            Arc::clone(&self.file_manager),
            Arc::clone(&self.log_manager),
            Arc::clone(&self.buffer_manager),
        )
    }

    #[cfg(test)]
    fn new_for_test(block_size: usize, num_buffers: usize) -> (Self, TestDir) {
        use std::time::{SystemTime, UNIX_EPOCH};

        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let test_dir = TestDir::new(format!("/tmp/test_db_{}_{:?}", timestamp, thread_id));
        let db = Self::new(&test_dir, block_size, num_buffers, true);
        (db, test_dir)
    }
}

struct ProductPlan {
    plan_1: Box<dyn Plan>,
    plan_2: Box<dyn Plan>,
    schema: Schema,
}

impl ProductPlan {
    fn new(plan_1: Box<dyn Plan>, plan_2: Box<dyn Plan>) -> Result<Self, Box<dyn Error>> {
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
    fn open(&self) -> Box<dyn Scan> {
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
}

struct ProjectPlan {
    plan: Box<dyn Plan>,
    schema: Schema,
}

impl ProjectPlan {
    fn new(plan: Box<dyn Plan>, fields_list: Vec<&str>) -> Result<Self, Box<dyn Error>> {
        let mut schema = Schema::new();
        for field in fields_list {
            schema.add_from_schema(field, &plan.schema())?;
        }
        Ok(Self { plan, schema })
    }
}

impl Plan for ProjectPlan {
    fn open(&self) -> Box<dyn Scan> {
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
}

struct SelectPlan {
    plan: Box<dyn Plan>,
    predicate: Predicate,
}

impl SelectPlan {
    fn new(plan: Box<dyn Plan>, predicate: Predicate) -> Self {
        Self { plan, predicate }
    }
}

impl Plan for SelectPlan {
    fn open(&self) -> Box<dyn Scan> {
        Box::new(SelectScan::new(self.plan.open(), self.predicate.clone()))
    }

    fn blocks_accessed(&self) -> usize {
        self.plan.blocks_accessed()
    }

    fn records_output(&self) -> usize {
        self.plan.records_output() / self.predicate.reduction_factor(&self.plan)
    }

    fn distinct_values(&self, field_name: &str) -> usize {
        if self.predicate.equates_with_constant(field_name) {
            return 1;
        } else if let Some(field_name_2) = self.predicate.equates_with_field(field_name) {
            return std::cmp::min(
                self.plan.distinct_values(field_name),
                self.plan.distinct_values(&field_name_2),
            );
        } else {
            return self.plan.distinct_values(field_name);
        }
    }

    fn schema(&self) -> Schema {
        self.plan.schema()
    }
}

struct TablePlan {
    table_name: String,
    txn: Arc<Transaction>,
    layout: Layout,
    stat_info: StatInfo,
}

impl TablePlan {
    fn new(
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
    fn open(&self) -> Box<dyn Scan> {
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
}

trait Plan {
    fn open(&self) -> Box<dyn Scan>;
    fn blocks_accessed(&self) -> usize;
    fn records_output(&self) -> usize;
    fn distinct_values(&self, field_name: &str) -> usize;
    fn schema(&self) -> Schema;
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
        let (db, test_dir) = SimpleDB::new_for_test(400, 3);

        //  the table plan
        let table = TablePlan::new(
            "student",
            Arc::new(db.new_tx()),
            Arc::clone(&db.metadata_manager),
        );
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
        let select = SelectPlan::new(Box::new(table), predicate);
        print_stats(&select, "select");

        //  the project plan
        let project = ProjectPlan::new(Box::new(select), vec!["sname", "majorid", "gradyear"]);
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

    fn close(&self) {
        (**self).close()
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
    fn new(s1: S1, s2: S2) -> Self {
        Self { s1, s2 }
    }
}

impl<S1, S2> Iterator for ProductScan<S1, S2>
where
    S1: Scan,
    S2: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.s2.next() {
            Some(result) => match result {
                Ok(_) => {
                    return Some(Ok(()));
                }
                Err(e) => return Some(Err(e)),
            },
            //  s2 cannot be advanced
            None => match self.s1.next() {
                //  advance s1, reset s2 and then return
                Some(result) => match result {
                    Ok(_) => {
                        self.s2.before_first().unwrap();
                        self.s2.next();
                        return Some(Ok(()));
                    }
                    Err(e) => return Some(Err(e)),
                },
                None => return None,
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
        Err(format!("Field {} not found in ProductScan", field_name).into())
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.get_string(field_name);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.get_string(field_name);
        }
        Err(format!("Field {} not found in ProductScan", field_name).into())
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if self.s1.has_field(field_name)? {
            return self.s1.get_value(field_name);
        }
        if self.s2.has_field(field_name)? {
            return self.s2.get_value(field_name);
        }
        Err(format!("Field {} not found in ProductScan", field_name).into())
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

    fn close(&self) {
        //  no-op because no resources to clean up
    }
}

#[cfg(test)]
mod product_scan_tests {
    use std::sync::Arc;

    use crate::{
        Layout, Predicate, ProductScan, ProjectScan, Scan, Schema, SelectScan, SimpleDB, TableScan,
        Term,
    };

    #[test]
    fn product_scan_test() {
        let (test_db, test_dir) = SimpleDB::new_for_test(400, 3);
        let txn = Arc::new(test_db.new_tx());
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
                scan1.insert();
                scan1.set_int("A", i as i32);
                scan1.set_string("B", &format!("string{}", i));
                scan2.insert();
                scan2.set_int("C", i as i32);
                scan2.set_string("D", &format!("string{}", i));
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
            project_scan.before_first().unwrap();
            while let Some(_) = project_scan.next() {
                let lhs = project_scan.get_string("B").unwrap();
                let rhs = project_scan.get_string("D").unwrap();
                println!("{}, {}", lhs, rhs);
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
    field_list: Vec<String>,
}

impl<S> ProjectScan<S>
where
    S: Scan,
{
    fn new(scan: S, field_list: Vec<String>) -> Self {
        Self { scan, field_list }
    }
}

impl<S> Iterator for ProjectScan<S>
where
    S: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.scan.next()
    }
}

impl<S> Scan for ProjectScan<S>
where
    S: Scan,
{
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {} not found in ProjectScan", field_name).into());
        }
        self.scan.get_int(field_name)
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {} not found in ProjectScan", field_name).into());
        }
        self.scan.get_string(field_name)
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        if !self.has_field(field_name)? {
            return Err(format!("Field {} not found in ProjectScan", field_name).into());
        }
        self.scan.get_value(field_name)
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        self.scan.has_field(field_name)
    }

    fn close(&self) {
        //  no-op because no resources to clean up
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.before_first()
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
    use std::sync::Arc;

    use crate::{
        test_utils::generate_random_number, Constant, Layout, Predicate, ProjectScan, Scan, Schema,
        SelectScan, SimpleDB, TableScan, Term,
    };

    #[test]
    fn project_scan_test() {
        let (test_db, test_dir) = SimpleDB::new_for_test(400, 3);
        let txn = Arc::new(test_db.new_tx());

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        let mut inserted_count = 0;
        let mut inserted_count_10 = 0;
        //  insertion block
        {
            let mut scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            for i in 0..50 {
                if i % 10 == 0 {
                    dbg!("Inserting number {}", 10);
                    scan.insert();
                    scan.set_int("A", 10);
                    scan.set_string("B", &format!("string{}", 10));
                    inserted_count += 1;
                    inserted_count_10 += 1;
                    continue;
                }

                let number = (generate_random_number() % 9) + 1; //  generate number in the range of 1-9
                dbg!("Inserting number {}", number);
                scan.insert();
                scan.set_int("A", number.try_into().unwrap());
                scan.set_string("B", &format!("string{}", number));
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
    fn new(scan: S, predicate: Predicate) -> Self {
        Self { scan, predicate }
    }
}

impl<S> Iterator for SelectScan<S>
where
    S: Scan,
{
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
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

    fn close(&self) {
        //  no-op because no resources to clean up
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.scan.before_first()
    }
}

impl<S> UpdateScan for SelectScan<S>
where
    S: UpdateScan,
{
    fn set_int(&self, field_name: &str) -> Result<(), Box<dyn Error>> {
        self.scan.set_int(field_name)
    }

    fn set_string(&self, field_name: &str) -> Result<(), Box<dyn Error>> {
        self.scan.set_string(field_name)
    }

    fn set_value(&self, field_name: &str) -> Result<(), Box<dyn Error>> {
        self.scan.set_value(field_name)
    }

    fn insert(&self) -> Result<(), Box<dyn Error>> {
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

impl<S> Drop for SelectScan<S>
where
    S: Scan,
{
    fn drop(&mut self) {
        //  no-op because no resources to cleanup
    }
}

#[cfg(test)]
mod select_scan_tests {
    use std::sync::Arc;

    use crate::{
        test_utils::generate_random_number, ComparisonOp, Constant, Expression, Layout, Predicate,
        Scan, Schema, SelectScan, SimpleDB, TableScan, Term,
    };

    #[test]
    fn select_scan_test() {
        let (simple_db, test_dir) = SimpleDB::new_for_test(400, 8);
        let txn = Arc::new(simple_db.new_tx());

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        let mut inserted_count = 0;
        let mut inserted_count_10 = 0;
        //  insertion block
        {
            let mut scan = TableScan::new(Arc::clone(&txn), layout.clone(), "T");
            for i in 0..50 {
                if i % 10 == 0 {
                    dbg!("Inserting number {}", 10);
                    scan.insert();
                    scan.set_int("A", 10);
                    scan.set_string("B", &format!("string{}", 10));
                    inserted_count += 1;
                    inserted_count_10 += 1;
                    continue;
                }

                let number = (generate_random_number() % 9) + 1; //  generate number in the range of 1-9
                dbg!("Inserting number {}", number);
                scan.insert();
                scan.set_int("A", number.try_into().unwrap());
                scan.set_string("B", &format!("string{}", number));
                inserted_count += 1;
            }
            dbg!("Inserted count {}", inserted_count);
        }

        let age_gt_30 = Term::new_with_op(
            Expression::FieldName("age".to_string()),
            Expression::Constant(Constant::Int(30)),
            ComparisonOp::GreaterThan,
        );
        let name_eq_john = Term::new_with_op(
            Expression::FieldName("name".to_string()),
            Expression::Constant(Constant::String("John".to_string())),
            ComparisonOp::Equal,
        );

        let dept_eq_eng = Term::new_with_op(
            Expression::FieldName("dept".to_string()),
            Expression::Constant(Constant::String("Engineering".to_string())),
            ComparisonOp::Equal,
        );

        let name_or_dept = Predicate::or(vec![
            Predicate::new(vec![name_eq_john]),
            Predicate::new(vec![dept_eq_eng]),
        ]);

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
struct Predicate {
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
    fn new(terms: Vec<Term>) -> Self {
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
        return self.evaluate_node(&self.root, scan);
    }

    fn evaluate_node<S>(&self, node: &PredicateNode, scan: &S) -> Result<bool, Box<dyn Error>>
    where
        S: Scan,
    {
        match node {
            //  terminal condition for recursion
            PredicateNode::Empty => Ok(true),
            PredicateNode::Term(term) => term.is_satisfied(scan),
            PredicateNode::Composite { op, operands } => {
                match op {
                    BooleanConnective::And => {
                        for operand in operands {
                            if !self.evaluate_node(operand, scan)? {
                                return Ok(false);
                            }
                        }
                        return Ok(true);
                    }
                    BooleanConnective::Or => {
                        for operand in operands {
                            if self.evaluate_node(operand, scan)? {
                                return Ok(true);
                            }
                        }
                        return Ok(false);
                    }
                    BooleanConnective::Not => {
                        if operands.len() != 1 {
                            return Err("NOT operator must have exactly one operand".into());
                        }
                        return Ok(!self.evaluate_node(&operands[0], scan)?);
                    }
                };
            }
        }
    }

    fn reduction_factor(&self, plan: &Box<dyn Plan>) -> usize {
        self.evaluate_reduction_factor(&self.root, plan)
    }

    fn evaluate_reduction_factor(&self, node: &PredicateNode, plan: &Box<dyn Plan>) -> usize {
        match node {
            PredicateNode::Empty => 1,
            PredicateNode::Term(term) => term.reduction_factor(plan),
            PredicateNode::Composite { op, operands } => {
                let mut factor = 1;
                for operand in operands {
                    factor *= self.evaluate_reduction_factor(operand, plan);
                }
                match op {
                    BooleanConnective::And => factor,
                    BooleanConnective::Or => factor,
                    BooleanConnective::Not => factor,
                }
            }
        }
    }

    fn equates_with_constant(&self, field_name: &str) -> bool {
        self.evaluate_equates_with_constant(&self.root, field_name)
    }

    fn evaluate_equates_with_constant(&self, node: &PredicateNode, field_name: &str) -> bool {
        match node {
            PredicateNode::Empty => false,
            PredicateNode::Term(term) => term.equates_with_constant(field_name),
            PredicateNode::Composite { op, operands } => {
                for operand in operands {
                    if self.evaluate_equates_with_constant(operand, field_name) {
                        return true;
                    }
                }
                return false;
            }
        }
    }

    fn equates_with_field(&self, field_name: &str) -> Option<String> {
        self.evaluate_equates_with_field(&self.root, field_name)
    }

    fn evaluate_equates_with_field(
        &self,
        node: &PredicateNode,
        field_name: &str,
    ) -> Option<String> {
        match node {
            PredicateNode::Empty => None,
            PredicateNode::Term(term) => term.equates_with_field(field_name),
            PredicateNode::Composite { op, operands } => {
                for operand in operands {
                    if let Some(field) = self.evaluate_equates_with_field(operand, field_name) {
                        return Some(field);
                    }
                }
                None
            }
        }
    }
}

#[derive(Clone, Debug)]
struct Term {
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
    fn new(lhs: Expression, rhs: Expression) -> Self {
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
    fn reduction_factor(&self, plan: &Box<dyn Plan>) -> usize {
        if self.lhs.is_field_name() && self.rhs.is_field_name() {
            let lhs_field = self.lhs.get_field_name().unwrap();
            let rhs_field = self.rhs.get_field_name().unwrap();
            return std::cmp::max(
                plan.distinct_values(&lhs_field),
                plan.distinct_values(&rhs_field),
            );
        }

        if self.lhs.is_field_name() {
            let lhs_field = self.lhs.get_field_name().unwrap();
            return plan.distinct_values(&lhs_field);
        }

        if self.rhs.is_field_name() {
            let rhs_field = self.rhs.get_field_name().unwrap();
            return plan.distinct_values(&rhs_field);
        }

        if self.lhs.get_constant_value().unwrap() == self.rhs.get_constant_value().unwrap() {
            return 1;
        }

        usize::MAX
    }

    /// Checks if the term equates with a constant value of the form "F=c"
    /// where F is the specified field and c is some constant
    fn equates_with_constant(&self, field_name: &str) -> bool {
        if self.lhs.is_field_name()
            && (self.lhs.get_field_name().unwrap() == field_name)
            && !self.rhs.is_field_name()
        {
            return true;
        } else if self.rhs.is_field_name()
            && (self.rhs.get_field_name().unwrap() == field_name)
            && !self.lhs.is_field_name()
        {
            return true;
        }
        return false;
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
        return None;
    }
}

#[derive(Clone, Debug)]
enum Expression {
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

    fn applies_to(&self, schema: &Schema) -> Result<bool, Box<dyn Error>> {
        match self {
            Expression::Constant(_) => Ok(true),
            Expression::FieldName(field_name) => Ok(schema.fields.contains(field_name)),
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
}

#[derive(Clone, Debug)]
enum BinaryOperator {
    Add,
    Subtract,
    Divide,
    Multiply,
    Modulo,
}

struct MetadataManager {
    table_manager: Arc<TableManager>,
    view_manager: Arc<ViewManager>,
    index_manager: Arc<IndexManager>,
    stat_manager: Arc<Mutex<StatManager>>,
}

impl MetadataManager {
    fn new(is_new: bool, txn: Arc<Transaction>) -> Self {
        let table_manager = Arc::new(TableManager::new(is_new, Arc::clone(&txn)));
        let view_manager = Arc::new(ViewManager::new(
            is_new,
            Arc::clone(&table_manager),
            Arc::clone(&txn),
        ));
        let stat_manager = Arc::new(Mutex::new(StatManager::new(Arc::clone(&table_manager))));
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

    fn get_layout(&self, table_name: &str, txn: Arc<Transaction>) -> Layout {
        self.table_manager.get_layout(table_name, txn)
    }

    fn create_view(&self, view_name: &str, view_def: &str, txn: Arc<Transaction>) {
        self.view_manager.create_view(view_name, view_def, txn);
    }

    fn get_view_def(&self, view_name: &str, txn: Arc<Transaction>) -> Option<String> {
        self.view_manager.get_view(view_name, txn)
    }

    fn create_index(
        &self,
        table_name: &str,
        index_name: &str,
        field_name: &str,
        txn: Arc<Transaction>,
    ) {
        println!(
            "Creating index {} on table {} for field {}",
            index_name, table_name, field_name
        );
        self.index_manager
            .create_index(index_name, table_name, field_name, txn);
    }

    fn get_index_info(
        &self,
        table_name: &str,
        txn: Arc<Transaction>,
    ) -> HashMap<String, IndexInfo> {
        println!("Fetching indices for table {}", table_name);
        self.index_manager.get_index_info(table_name, txn)
    }

    fn get_stat_info(&self, table_name: &str, layout: Layout, txn: Arc<Transaction>) -> StatInfo {
        self.stat_manager
            .lock()
            .unwrap()
            .get_stat_info(table_name, layout, txn)
    }
}

#[cfg(test)]
mod metadata_manager_tests {
    use crate::{
        test_utils::generate_random_number, FieldType, MetadataManager, Schema, SimpleDB, TableScan,
    };
    use std::sync::Arc;

    #[test]
    fn test_metadata_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 8);
        let tx = Arc::new(db.new_tx());
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
                FieldType::INT => "int".to_string(),
                FieldType::STRING => format!("varchar({})", field_info.length),
            };
            println!("{}: {}", field, type_str);

            // Assert field properties
            match field.as_str() {
                "A" => assert_eq!(field_info.field_type, FieldType::INT),
                "B" => {
                    assert_eq!(field_info.field_type, FieldType::STRING);
                    assert_eq!(field_info.length, 9);
                }
                _ => panic!("Unexpected field: {}", field),
            }
        }

        // Part 2: Statistics Metadata
        {
            let mut table_scan = TableScan::new(Arc::clone(&tx), layout.clone(), table_name);
            for _ in 0..50 {
                table_scan.insert();
                let n = (generate_random_number() % 50) + 1;
                table_scan.set_int("A", n as i32);
                table_scan.set_string("B", &format!("rec{}", n));
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
        println!("View def = {:?}", retrieved_view);
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

        // assert!(idx_a.blocks_accessed() >= 0); //  TODO: is there a better way to assert this?
        assert_eq!(idx_a.records_output(), 2);
        assert!(idx_a.distinct_values("A") == 1); //  we have an index on A

        // Verify index B
        let idx_b = idx_map.get("B").expect("Index B not found");
        println!("B(indexB) = {}", idx_b.blocks_accessed());
        println!("R(indexB) = {}", idx_b.records_output());
        println!("V(indexB,A) = {}", idx_b.distinct_values("A"));
        println!("V(indexB,B) = {}", idx_b.distinct_values("B"));

        // assert!(idx_b.blocks_accessed() >= 0); //  TODO: Is there a better way to assert this?
        assert_eq!(idx_b.records_output(), 2);
        assert!(idx_b.distinct_values("B") == 1); //  we have an index on B

        tx.commit().unwrap();
    }
}

struct IndexManager {
    layout: Layout,
    table_manager: Arc<TableManager>,
    stat_manager: Arc<Mutex<StatManager>>,
}

impl IndexManager {
    const INDEX_CAT_TBL_NAME: &str = "index_cat";
    const INDEX_COL_NAME: &str = "index_name";
    const TABLE_COL_NAME: &str = "table_name";
    const TABLE_FIELD_NAME: &str = "field_name";

    fn new(
        is_new: bool,
        table_manager: Arc<TableManager>,
        stat_manager: Arc<Mutex<StatManager>>,
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
        table_scan.insert();
        table_scan.set_string(Self::INDEX_COL_NAME, index_name);
        table_scan.set_string(Self::TABLE_COL_NAME, table_name);
        table_scan.set_string(Self::TABLE_FIELD_NAME, field_name);
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
                let stat_info = self.stat_manager.lock().unwrap().get_stat_info(
                    table_name,
                    layout.clone(),
                    Arc::clone(&txn),
                );
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

struct IndexInfo {
    index_name: String,
    field_name: String,
    txn: Arc<Transaction>,
    table_schema: Schema,
    index_layout: Layout,
    stat_info: StatInfo,
}

impl IndexInfo {
    const BLOCK_NUM_FIELD: &str = "block_num"; //   the block number
    const ID_FIELD: &str = "id"; //  the record id (slot number)
    const DATA_FIELD: &str = "dataval"; //  the data field
    fn new(
        index_name: &str,
        field_name: &str,
        txn: Arc<Transaction>,
        table_schema: Schema,
        stat_info: StatInfo,
    ) -> Self {
        let mut schema = Schema::new();
        schema.add_int_field(Self::BLOCK_NUM_FIELD);
        schema.add_int_field(Self::ID_FIELD);
        match table_schema.info.get(field_name).unwrap().field_type {
            FieldType::INT => {
                schema.add_int_field(field_name);
            }
            FieldType::STRING => {
                schema.add_string_field(
                    field_name,
                    table_schema.info.get(field_name).unwrap().length,
                );
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

    /// This function returns the number of blocks that would need to be searched for this index on a specific field
    fn blocks_accessed(&self) -> usize {
        let records_per_block = self.txn.block_size() / self.index_layout.slot_size;
        let num_blocks = self.stat_info.num_records / records_per_block;
        HashIndex::search_cost(num_blocks)
    }

    /// This function returns the number of records that we would expect to get when using this index on a specific field
    fn records_output(&self) -> usize {
        self.stat_info.num_records / self.stat_info.distinct_values(&self.field_name)
    }

    /// This function returns the number of distinct values for a specific field in this index
    fn distinct_values(&self, field_name: &str) -> usize {
        if self.field_name == field_name {
            1
        } else {
            self.stat_info.distinct_values(&self.field_name)
        }
    }
}

struct HashIndex {
    txn: Arc<Transaction>,
    index_name: String,
    layout: Layout,
    search_key: Option<Constant>,
    table_scan: Option<TableScan>,
}

impl HashIndex {
    const NUM_BUCKETS: usize = 100;

    fn new(txn: Arc<Transaction>, index_name: &str, layout: Layout) -> Self {
        Self {
            txn,
            index_name: index_name.to_string(),
            layout,
            search_key: None,
            table_scan: None,
        }
    }

    fn search_cost(num_blocks: usize) -> usize {
        num_blocks / HashIndex::NUM_BUCKETS
    }
}

impl Index for HashIndex {
    fn before_first(&mut self, search_key: &Constant) {
        self.close();
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
        return false;
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
        table_scan.insert();
        table_scan.set_int(IndexInfo::BLOCK_NUM_FIELD, data_rid.block_num as i32);
        table_scan.set_int(IndexInfo::ID_FIELD, data_rid.slot as i32);
        table_scan.set_value(IndexInfo::DATA_FIELD, data_val.clone());
    }

    fn delete(&mut self, data_val: &Constant, data_rid: &RID) {
        self.before_first(data_val);
        while self.next() {
            if *data_rid == self.get_data_rid() {
                self.table_scan.as_ref().unwrap().delete();
                return;
            }
        }
    }

    fn close(&mut self) {
        self.table_scan.as_ref().and_then(|ts| Some(ts.close()));
    }
}

/// Interface for traversing and modifying an index
trait Index {
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

    /// Close the index and release any resources
    fn close(&mut self);
}

struct StatManager {
    table_manager: Arc<TableManager>,
    table_stats: HashMap<String, StatInfo>,
    num_calls: usize,
}

impl StatManager {
    fn new(table_manager: Arc<TableManager>) -> Self {
        Self {
            table_manager,
            table_stats: HashMap::new(),
            num_calls: 0,
        }
    }

    /// Returns the statistics for a given table
    /// Refreshes all stats for all tables based on a counter
    fn get_stat_info(
        &mut self,
        table_name: &str,
        layout: Layout,
        txn: Arc<Transaction>,
    ) -> StatInfo {
        println!("getting stat info for {}", table_name);
        self.num_calls += 1;
        if self.num_calls > 100 {
            self.refresh_stats(Arc::clone(&txn));
        }

        if let Some(stats) = self.table_stats.get(table_name) {
            println!("found table stats {:?}", stats);
            stats.clone()
        } else {
            println!("going to calculate table stats");
            let table_stats = self.calculate_table_stats(table_name, layout, txn);
            println!("table stats {:?}", table_stats);
            self.table_stats
                .insert(table_name.to_string(), table_stats.clone());
            table_stats
        }
    }

    /// Refreshes the statistics for all tables in the database
    fn refresh_stats(&mut self, txn: Arc<Transaction>) {
        self.table_stats.clear();
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
            self.table_stats.insert(table_name, table_stats);
        }
    }

    /// Calculates the [`StatInfo`] for a given table
    fn calculate_table_stats(
        &self,
        table_name: &str,
        layout: Layout,
        txn: Arc<Transaction>,
    ) -> StatInfo {
        println!("calculating table stats for {}", table_name);
        let mut table_scan = TableScan::new(txn, layout, table_name);
        let mut num_rec = 0;
        let mut num_blocks = 0;
        while table_scan.next().is_some() {
            num_rec += 1;
            num_blocks = table_scan.record_page.as_ref().unwrap().block_id.block_num + 1;
        }
        StatInfo {
            num_blocks,
            num_records: num_rec,
        }
    }
}

#[derive(Clone, Debug)]
struct StatInfo {
    num_blocks: usize,
    num_records: usize,
}

impl StatInfo {
    fn new(num_block: usize, num_records: usize) -> Self {
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

    fn new(is_new: bool, table_manager: Arc<TableManager>, txn: Arc<Transaction>) -> Self {
        if is_new {
            let mut schema = Schema::new();
            schema.add_string_field(Self::VIEW_NAME_COL, TableManager::MAX_NAME_LENGTH);
            schema.add_string_field(Self::VIEW_DEF_COL, Self::VIEW_DEF_MAX_LENGTH);
            table_manager.create_table(Self::VIEW_MANAGER_TABLE_NAME, &schema, txn);
        }
        let view_manager = ViewManager { table_manager };
        view_manager
    }

    /// Creates a new view in the view catalog
    fn create_view(&self, view_name: &str, view_def: &str, txn: Arc<Transaction>) {
        let layout = self
            .table_manager
            .get_layout(Self::VIEW_MANAGER_TABLE_NAME, Arc::clone(&txn));
        let mut table_scan = TableScan::new(txn, layout, Self::VIEW_MANAGER_TABLE_NAME);
        table_scan.insert();
        table_scan.set_string(Self::VIEW_NAME_COL, view_name);
        table_scan.set_string(Self::VIEW_DEF_COL, view_def);
    }

    /// Returns the view definition for a given view name
    fn get_view(&self, view_name: &str, txn: Arc<Transaction>) -> Option<String> {
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

    fn new(is_new: bool, tx: Arc<Transaction>) -> Self {
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
            table_scan.insert();
            table_scan.set_string(Self::TABLE_NAME_COL, table_name);
            table_scan.set_int(Self::SLOT_SIZE_COL, layout.slot_size as i32);
        }

        // insert the records for the fields into the field catalog table
        {
            let mut table_scan = TableScan::new(
                tx,
                self.field_catalog_layout.clone(),
                Self::FIELD_CAT_TABLE_NAME,
            );
            for field in &schema.fields {
                table_scan.insert();
                table_scan.set_string(Self::TABLE_NAME_COL, table_name);
                table_scan.set_string(Self::FIELD_NAME_COL, field);
                let field_info = schema.info.get(field).unwrap();
                table_scan.set_int(Self::FIELD_TYPE_COL, field_info.field_type as i32);
                table_scan.set_int(Self::FIELD_LENGTH_COL, field_info.length as i32);
                table_scan.set_int(Self::FIELD_OFFSET_COL, layout.offset(field).unwrap() as i32);
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
}

#[cfg(test)]
mod table_manager_tests {
    use std::sync::Arc;

    use crate::{FieldType, Schema, SimpleDB, TableManager};

    #[test]
    fn test_table_manager() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 8);
        let tx = Arc::new(db.new_tx());
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
                FieldType::INT => "int".to_string(),
                FieldType::STRING => format!("varchar({})", field_info.length),
            };
            println!("{}: {}", field, type_str);

            // Assert field properties
            match field.as_str() {
                "A" => {
                    assert_eq!(field_info.field_type, FieldType::INT);
                }
                "B" => {
                    assert_eq!(field_info.field_type, FieldType::STRING);
                    assert_eq!(field_info.length, 9);
                }
                _ => panic!("Unexpected field: {}", field),
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

struct TableScan {
    txn: Arc<Transaction>,
    layout: Layout,
    file_name: String,
    record_page: Option<RecordPage>,
    current_slot: Option<usize>,
}

impl TableScan {
    fn new(txn: Arc<Transaction>, layout: Layout, table_name: &str) -> Self {
        let db_dir = {
            let fm = txn.file_manager.lock().unwrap();
            fm.db_directory.clone()
        };
        let path = db_dir.join(format!("{}.tbl", table_name));
        let file_name = path.to_str().unwrap();
        let mut scan = Self {
            txn,
            layout,
            file_name: file_name.to_string(),
            record_page: None,
            current_slot: None,
        };

        if scan.txn.size(&file_name) == 0 {
            scan.move_to_new_block();
        } else {
            scan.move_to_block(0);
        }
        scan
    }

    /// Moves the [`RecordPage`] on this [`TableScan`] to a specific block number
    fn move_to_block(&mut self, block_num: usize) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), block_num);
        let record_page = RecordPage::new(Arc::clone(&self.txn), block_id, self.layout.clone());
        self.current_slot = None;
        self.record_page = Some(record_page);
    }

    /// Allocates a new [`BlockId`] to the underlying file and moves the [`RecordPage`] there
    fn move_to_new_block(&mut self) {
        self.close();
        let block = self.txn.append(&self.file_name);
        let record_page = RecordPage::new(Arc::clone(&self.txn), block, self.layout.clone());
        record_page.format();
        self.current_slot = None;
        self.record_page = Some(record_page);
    }

    /// Checks if the [`TableScan`] is at the last block in the file
    fn at_last_block(&self) -> bool {
        self.record_page.as_ref().unwrap().block_id.block_num == self.txn.size(&self.file_name) - 1
    }

    /// Moves the [`RecordPage`] to the start of the file
    fn move_to_start(&mut self) {
        self.move_to_block(0);
    }

    /// Sets the integer value of a field in the current slot
    fn set_int(&self, field_name: &str, value: i32) {
        self.record_page.as_ref().unwrap().set_int(
            *self.current_slot.as_ref().unwrap(),
            field_name,
            value,
        )
    }

    /// Sets the string value of a field in the current slot
    fn set_string(&self, field_name: &str, value: &str) {
        self.record_page.as_ref().unwrap().set_string(
            *self.current_slot.as_ref().unwrap(),
            field_name,
            value,
        );
    }

    /// Tries to insert a new record into the table
    fn insert(&mut self) {
        let mut iterations = 0;
        loop {
            //  sanity check in case i runs into an infinite loop
            iterations += 1;
            assert!(
                iterations <= 10000,
                "Table scan insert failed for {} iterations",
                iterations
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
                            self.record_page.as_ref().unwrap().block_id.block_num + 1,
                        );
                    }
                    continue;
                }
            }
        }
    }

    /// Deletes the record pointed to by current slot from the table
    fn delete(&self) {
        self.record_page
            .as_ref()
            .unwrap()
            .delete(*self.current_slot.as_ref().unwrap());
    }

    fn move_to_row_id(&mut self, row_id: RID) {
        self.close();
        let block_id = BlockId::new(self.file_name.clone(), row_id.block_num);
        self.record_page = Some(RecordPage::new(
            Arc::clone(&self.txn),
            block_id,
            self.layout.clone(),
        ));
        self.current_slot = Some(row_id.slot);
    }

    fn get_row_id(&self) -> RID {
        RID::new(
            self.record_page.as_ref().unwrap().block_id.block_num,
            *self.current_slot.as_ref().unwrap(),
        )
    }

    fn set_value(&self, field_name: &str, value: Constant) {
        match self.layout.schema.info.get(field_name).unwrap().field_type {
            FieldType::INT => self.set_int(field_name, value.as_int()),
            FieldType::STRING => self.set_string(field_name, value.as_str()),
        }
    }
}

impl Drop for TableScan {
    fn drop(&mut self) {
        self.close();
    }
}

/// An iterator over the records in the table
impl Iterator for TableScan {
    type Item = Result<(), Box<dyn Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            //  Check if there is a record page currently
            if let Some(record_page) = &self.record_page {
                let next_slot = match self.current_slot {
                    None => record_page.iter_used_slots().next(),
                    Some(slot) => record_page
                        .iter_used_slots()
                        .skip_while(|s| *s <= slot)
                        .next(),
                };

                if let Some(slot) = next_slot {
                    self.current_slot = Some(slot);
                    return Some(Ok(()));
                }
            }

            if self.at_last_block() {
                return None;
            }
            self.move_to_block(self.record_page.as_ref().unwrap().block_id.block_num + 1);
        }
    }
}

impl Scan for TableScan {
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>> {
        Ok(self
            .record_page
            .as_ref()
            .unwrap()
            .get_int(*self.current_slot.as_ref().unwrap(), field_name))
    }

    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>> {
        Ok(self
            .record_page
            .as_ref()
            .unwrap()
            .get_string(*self.current_slot.as_ref().unwrap(), field_name))
    }

    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>> {
        match self.layout.schema.info.get(field_name).unwrap().field_type {
            FieldType::INT => Ok(Constant::Int(self.get_int(field_name)?)),
            FieldType::STRING => Ok(Constant::String(self.get_string(field_name)?)),
        }
    }

    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>> {
        Ok(self.layout.schema.fields.contains(&field_name.to_string()))
    }

    fn close(&self) {
        if let Some(record_page) = &self.record_page {
            self.txn.unpin(&record_page.block_id);
        }
    }

    fn before_first(&mut self) -> Result<(), Box<dyn Error>> {
        self.move_to_block(0);
        Ok(())
    }
}

trait UpdateScan: Scan {
    fn set_int(&self, field_name: &str) -> Result<(), Box<dyn Error>>;
    fn set_string(&self, field_name: &str) -> Result<(), Box<dyn Error>>;
    fn set_value(&self, field_name: &str) -> Result<(), Box<dyn Error>>;
    fn insert(&self) -> Result<(), Box<dyn Error>>;
    fn delete(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_rid(&self) -> Result<RID, Box<dyn Error>>;
    fn move_to_rid(&mut self, rid: RID) -> Result<(), Box<dyn Error>>;
}

trait Scan: Iterator<Item = Result<(), Box<dyn Error>>> {
    fn before_first(&mut self) -> Result<(), Box<dyn Error>>;
    fn get_int(&self, field_name: &str) -> Result<i32, Box<dyn Error>>;
    fn get_string(&self, field_name: &str) -> Result<String, Box<dyn Error>>;
    fn get_value(&self, field_name: &str) -> Result<Constant, Box<dyn Error>>;
    fn has_field(&self, field_name: &str) -> Result<bool, Box<dyn Error>>;
    fn close(&self);
}

#[cfg(test)]
mod table_scan_tests {
    use std::sync::Arc;

    use crate::{test_utils::generate_random_number, Layout, Scan, Schema, SimpleDB, TableScan};

    #[test]
    fn table_scan_test() {
        let (db, test_dir) = SimpleDB::new_for_test(400, 4);
        let txn = Arc::new(db.new_tx());

        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);

        dbg!("Inserting a bunch of records into the table");
        let mut inserted_count = 0;
        let mut table_scan = TableScan::new(txn, layout, "table");
        for i in 0..100 {
            table_scan.insert();
            let number = (generate_random_number() % 100) + 1;
            table_scan.set_int("A", number as i32);
            table_scan.set_string("B", &format!("rec{}", number));
            dbg!(format!("Inserting number {}", number));
            inserted_count += 1;
        }
        dbg!(format!("Inserted {} records", inserted_count));

        dbg!("Deleting a bunch of records");
        dbg!(format!(
            "The table scan is at {:?}",
            table_scan.record_page.as_ref().unwrap().block_id
        ));
        let mut deleted_count = 0;
        table_scan.move_to_start();
        while let Some(_) = table_scan.next() {
            let number = table_scan.get_int("A").unwrap();
            dbg!(format!("The number retrieved {}", number));
            if number < 25 {
                deleted_count += 1;
                table_scan.delete();
            }
        }
        dbg!(format!("Deleted {} records", deleted_count));

        dbg!("Finding remaining records");
        let mut remaining_count = 0;
        table_scan.move_to_start();
        while let Some(_) = table_scan.next() {
            let number = table_scan.get_int("A").unwrap();
            let string = table_scan.get_string("B");
            remaining_count += 1;
        }
        dbg!(format!("Found {} remaining records", remaining_count));
        assert_eq!(remaining_count + deleted_count, inserted_count);
    }
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
enum Constant {
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

#[derive(PartialEq, Eq)]
struct RID {
    block_num: usize,
    slot: usize,
}

impl RID {
    fn new(block_num: usize, slot: usize) -> Self {
        Self { block_num, slot }
    }
}

struct RecordPageIterator<'a> {
    record_page: &'a RecordPage,
    current_slot: Option<usize>,
    presence: SlotPresence,
}

impl<'a> RecordPageIterator<'a> {
    fn new(record_page: &'a RecordPage, presence: SlotPresence) -> Self {
        Self {
            record_page,
            current_slot: None,
            presence,
        }
    }
}

impl<'a> Iterator for RecordPageIterator<'a> {
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
                .get_int(&self.record_page.block_id, self.record_page.offset(slot))
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
    EMPTY,
    USED,
}

struct RecordPage {
    tx: Arc<Transaction>,
    block_id: BlockId,
    layout: Layout,
}

impl RecordPage {
    /// Creates a new RecordPage with the given transaction, block ID, and layout.
    /// Pins the block in memory.
    fn new(tx: Arc<Transaction>, block_id: BlockId, layout: Layout) -> Self {
        tx.pin(&block_id);
        Self {
            tx,
            block_id,
            layout,
        }
    }

    /// Retrieves an integer value from the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn get_int(&self, slot: usize, field_name: &str) -> i32 {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx.get_int(&self.block_id, offset).unwrap()
    }

    /// Retrieves a string value from the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn get_string(&self, slot: usize, field_name: &str) -> String {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx.get_string(&self.block_id, offset).unwrap()
    }

    /// Sets an integer value in the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn set_int(&self, slot: usize, field_name: &str, value: i32) {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx
            .set_int(&self.block_id, offset, value, true)
            .unwrap();
    }

    /// Sets a string value in the specified slot and field.
    /// The offset is calculated using the slot number and field layout.
    fn set_string(&self, slot: usize, field_name: &str, value: &str) {
        let offset = self.offset(slot) + self.layout.offset(field_name).unwrap();
        self.tx
            .set_string(&self.block_id, offset, value, true)
            .unwrap();
    }

    /// Marks a slot as used and returns its slot number.
    fn insert(&self, slot: usize) -> usize {
        self.set_flag(slot, SlotPresence::USED);
        slot
    }

    /// Finds the next empty slot after the given slot, marks it as used, and returns its number.
    fn insert_after(&self, slot: Option<usize>) -> Result<usize, Box<dyn Error>> {
        let new_slot = match slot {
            None => self
                .iter_empty_slots()
                .next()
                .ok_or_else(|| "no empty slots available in this record page")?,
            Some(current_slot) => self
                .iter_empty_slots()
                .skip_while(|s| *s <= current_slot)
                .next()
                .ok_or_else(|| "no empty slots available in this record page")?,
        };
        self.set_flag(new_slot, SlotPresence::USED);
        Ok(new_slot)
    }

    /// Returns the next [`SlotPresence::USED`] slot after the slot passed in
    fn search_after(&self, slot: usize) -> Result<usize, Box<dyn Error>> {
        let next_slot = self
            .iter_used_slots()
            .skip_while(|s| *s <= slot)
            .next()
            .unwrap();
        Ok(next_slot)
    }

    /// Sets the presence flag (EMPTY or USED) for a given slot.
    fn set_flag(&self, slot: usize, flag: SlotPresence) {
        self.tx
            .set_int(&self.block_id, self.offset(slot), flag as i32, true)
            .unwrap();
    }

    /// Marks a slot as empty, effectively deleting its record.
    fn delete(&self, slot: usize) {
        self.set_flag(slot, SlotPresence::EMPTY);
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
                    &self.block_id,
                    self.offset(current_slot),
                    SlotPresence::EMPTY as i32,
                    false,
                )
                .unwrap();
            let schema = &self.layout.schema;
            for field in &schema.fields {
                let field_pos = self.offset(current_slot) + self.layout.offset(&field).unwrap();
                match schema.info.get(field).unwrap().field_type {
                    FieldType::INT => self
                        .tx
                        .set_int(&self.block_id, field_pos, 0, false)
                        .unwrap(),
                    FieldType::STRING => self
                        .tx
                        .set_string(&self.block_id, field_pos, "", false)
                        .unwrap(),
                }
            }
            current_slot += 1;
        }
    }

    /// Returns an iterator over empty slots in the record page.
    fn iter_empty_slots(&self) -> RecordPageIterator {
        RecordPageIterator {
            record_page: self,
            current_slot: None,
            presence: SlotPresence::EMPTY,
        }
    }

    /// Returns an iterator over used slots in the record page.
    fn iter_used_slots(&self) -> RecordPageIterator {
        RecordPageIterator {
            record_page: self,
            current_slot: None,
            presence: SlotPresence::USED,
        }
    }
}

#[cfg(test)]
mod record_page_tests {
    use std::sync::Arc;

    use crate::{test_utils::generate_random_number, Layout, RecordPage, Schema, SimpleDB};

    #[test]
    fn record_page_test() {
        let (db, test_dir) = SimpleDB::new_for_test(400, 3);
        let txn = Arc::new(db.new_tx());

        //  Set up the test
        let mut schema = Schema::new();
        schema.add_int_field("A");
        schema.add_string_field("B", 10);
        let layout = Layout::new(schema);
        for field in &layout.schema.fields {
            let offset = layout.offset(&field).unwrap();
            if field == "A" {
                assert_eq!(offset, 4);
            }
            if field == "B" {
                assert_eq!(offset, 8);
            }
        }
        let block_id = txn.append("test_file");
        txn.pin(&block_id);
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
            println!("value of a {a}");
            let b = record_page.get_string(slot, "B");
            if a < 25 {
                deleted_count += 1;
                record_page.delete(slot);
            }
        }
        println!("{} values were deleted", deleted_count);

        //  Check that the correct number of records are left
        let record_iter = record_page.iter_used_slots();
        let mut remaining_count = 0;
        for slot in record_iter {
            let a = record_page.get_int(slot, "A");
            let b = record_page.get_string(slot, "B");
            assert!(a >= 25);
            remaining_count += 1;
        }

        assert_eq!(remaining_count + deleted_count, inserted_count);
    }
}

#[derive(Clone, Debug)]
struct Layout {
    schema: Schema,
    offsets: HashMap<String, usize>, //  map the field name to the offset
    slot_size: usize,
}

impl Layout {
    fn new(schema: Schema) -> Self {
        let mut offsets = HashMap::new();
        let mut offset = Page::INT_BYTES;
        for field in schema.fields.iter() {
            let field_info = schema.info.get(field).unwrap();
            offsets.insert(field.clone(), offset);

            match field_info.field_type {
                FieldType::INT => offset += field_info.length,
                FieldType::STRING => offset += Page::INT_BYTES + field_info.length,
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
            let offset = layout.offset(&field).unwrap();
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
enum FieldType {
    INT = 0,
    STRING = 1,
}

impl From<i32> for FieldType {
    fn from(value: i32) -> Self {
        match value {
            0 => FieldType::INT,
            1 => FieldType::STRING,
            _ => panic!("Invalid field type"),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct FieldInfo {
    field_type: FieldType,
    length: usize,
}

#[derive(Clone, Debug)]
struct Schema {
    fields: Vec<String>,
    info: HashMap<String, FieldInfo>,
}

impl Schema {
    fn new() -> Self {
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
        self.add_field(field_name, FieldType::INT, Page::INT_BYTES);
    }

    fn add_string_field(&mut self, field_name: &str, length: usize) {
        self.add_field(field_name, FieldType::STRING, length);
    }

    fn add_from_schema(&mut self, field_name: &str, schema: &Schema) -> Result<(), Box<dyn Error>> {
        let field_type = schema
            .info
            .get(field_name)
            .and_then(|info| Some(info.field_type))
            .ok_or_else(|| {
                format!(
                    "Field {} not found in schema while looking for type",
                    field_name
                )
            })?;
        // .unwrap();
        let field_length = schema
            .info
            .get(field_name)
            .and_then(|info| Some(info.length))
            .ok_or_else(|| {
                format!(
                    "Field {} not found in schema while looking for length",
                    field_name
                )
            })?;
        // .unwrap();
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

trait TransactionOperations {
    fn pin(&self, block_id: &BlockId);
    fn unpin(&self, block_id: &BlockId);
    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool);
    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool);
}

impl TransactionOperations for Transaction {
    fn pin(&self, block_id: &BlockId) {
        Transaction::pin(&self, block_id);
    }

    fn unpin(&self, block_id: &BlockId) {
        Transaction::unpin(&self, block_id);
    }

    fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool) {
        Transaction::set_int(&self, block_id, offset, val, log).unwrap();
    }

    fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool) {
        Transaction::set_string(&self, block_id, offset, val, log).unwrap();
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

struct Transaction {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
    recovery_manager: RecoveryManager,
    concurrency_manager: ConcurrencyManager,
    buffer_list: BufferList,
    tx_id: TransactionID,
}

impl Transaction {
    const TXN_SLEEP_TIMEOUT: u64 = 100; //  time the txn will sleep for
    fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
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
            concurrency_manager: ConcurrencyManager::new(tx_id, Self::TXN_SLEEP_TIMEOUT),
            file_manager,
        }
    }

    /// Commit this transaction
    /// This will write all data associated with this transaction out to disk and append a [`LogRecord::Commit`] to the WAL
    /// It will release all locks that are currently held by this transaction
    /// It will also handle meta operations like unpinning buffers
    fn commit(&self) -> Result<(), Box<dyn Error>> {
        //  Commit all data associated with this txn
        self.recovery_manager.commit();
        //  Release all locks associated with this txn
        self.concurrency_manager.release()?;
        //  unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Rollback this transaction
    /// This will undo all operations performed by this transaction and append a [`LogRecord::Rollback`] to the WAL
    /// It will also handle meta operations like unpinning buffers
    fn rollback(&self) -> Result<(), Box<dyn Error>> {
        //  Rollback all data associated with this txn
        self.recovery_manager.rollback(self).unwrap();
        //  Release all locks associated with this txn
        self.concurrency_manager.release()?;
        //  unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Recover the database on start-up or after a crash
    fn recover(&self) -> Result<(), Box<dyn Error>> {
        //  Perform a database recovery
        self.recovery_manager.recover(self).unwrap();
        //  TODO: Release all locks associated with this transaction
        self.concurrency_manager.release()?;
        //  Unpin all buffers and release metadata
        self.buffer_list.unpin_all();
        Ok(())
    }

    /// Pin this [`BlockId`] to be used in this transaction
    fn pin(&self, block_id: &BlockId) {
        self.buffer_list.pin(block_id);
    }

    /// Unpin this [`BlockId`] since it is no longer needed by this transaction
    fn unpin(&self, block_id: &BlockId) {
        self.buffer_list.unpin(block_id);
    }

    /// Get an integer value in a [`Buffer`] associated with this transaction
    fn get_int(&self, block_id: &BlockId, offset: usize) -> Result<i32, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let guard = buffer.lock().unwrap();
        Ok(guard.contents.get_int(offset))
    }

    /// Set an integer value in a [`Buffer`] associated with this transaction
    fn set_int(
        &self,
        block_id: &BlockId,
        offset: usize,
        value: i32,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let lsn = {
            if log {
                //  The LSN returned from writing to the WAL
                self.recovery_manager
                    .set_int(buffer.lock().unwrap().deref(), offset, value)
                    .unwrap()
            } else {
                //  The default LSN when no WAL write occurs
                LSN::MAX
            }
        };
        let mut guard = buffer.lock().unwrap();
        guard.contents.set_int(offset, value);
        guard.set_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    /// Get a string value in a [`Buffer`] associated with this transaction
    fn get_string(&self, block_id: &BlockId, offset: usize) -> Result<String, Box<dyn Error>> {
        self.concurrency_manager.slock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let guard = buffer.lock().unwrap();
        Ok(guard.contents.get_string(offset))
    }

    /// Set a string value in a [`Buffer`] associated with this transaction
    fn set_string(
        &self,
        block_id: &BlockId,
        offset: usize,
        value: &str,
        log: bool,
    ) -> Result<(), Box<dyn Error>> {
        self.concurrency_manager.xlock(block_id)?;
        let buffer = self.buffer_list.get_buffer(block_id).unwrap();
        let lsn: usize = {
            if log {
                self.recovery_manager
                    .set_string(buffer.lock().unwrap().deref(), offset, value)
                    .unwrap()
            } else {
                LSN::MAX
            }
        };
        let mut guard = buffer.lock().unwrap();
        guard.contents.set_string(offset, value);
        guard.set_modified(self.tx_id as usize, lsn);
        Ok(())
    }

    /// Get the available buffers for this transaction
    fn available_buffs(&self) -> usize {
        self.buffer_manager.lock().unwrap().available()
    }

    /// Get the size of this file in blocks
    fn size(&self, file_name: &str) -> usize {
        //  TODO: Insert a shared lock here to ensure the length read is accurate
        self.file_manager
            .lock()
            .unwrap()
            .length(file_name.to_string())
    }

    /// Append a block to the file
    fn append(&self, file_name: &str) -> BlockId {
        //  TODO: Insert a write lock here to ensure this write is safe
        self.file_manager
            .lock()
            .unwrap()
            .append(file_name.to_string())
    }

    /// Get the block size
    fn block_size(&self) -> usize {
        self.file_manager.lock().unwrap().blocksize
    }
}

#[cfg(test)]
mod transaction_tests {
    use std::{error::Error, sync::Arc, thread::JoinHandle, time::Duration};

    use crate::{
        test_utils::{generate_filename, generate_random_number, TestDir},
        BlockId, SimpleDB, Transaction,
    };

    #[test]
    fn test_transaction_single_threaded() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 3);

        //  Start a transaction t1 that will set an int and a string
        let t1 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        let block_id = BlockId::new(file.to_string(), 1);
        t1.pin(&block_id);
        t1.set_int(&block_id, 80, 1, false).unwrap();
        t1.set_string(&block_id, 40, "one", false).unwrap();
        t1.commit().unwrap();

        //  Start a transaction t2 that should see the results of the previously committed transaction t1
        //  Set new values in this transaction
        let t2 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t2.pin(&block_id);
        assert_eq!(t2.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(t2.get_string(&block_id, 40).unwrap(), "one");
        t2.set_int(&block_id, 80, 2, true).unwrap();
        t2.set_string(&block_id, 40, "two", true).unwrap();
        t2.commit().unwrap();

        //  Start a transaction t3 which should see the results of t2
        //  Set new values for t3 but roll it back instead of committing
        let t3 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t3.pin(&block_id);
        assert_eq!(t3.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t3.get_string(&block_id, 40).unwrap(), "two");
        t3.set_int(&block_id, 80, 3, true).unwrap();
        t3.set_string(&block_id, 40, "three", true).unwrap();
        t3.rollback().unwrap();

        //  Start a transaction t4 which should see the result of t2 since t3 rolled back
        //  This will be a read only transaction that commits
        let t4 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t4.pin(&block_id);
        assert_eq!(t4.get_int(&block_id, 80).unwrap(), 2);
        assert_eq!(t4.get_string(&block_id, 40).unwrap(), "two");
        t4.commit().unwrap();
    }

    #[test]
    fn test_transaction_multi_threaded_single_reader_single_writer() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 10);
        let block_id = BlockId::new(file.to_string(), 1);

        let fm1 = Arc::clone(&test_db.file_manager);
        let lm1 = Arc::clone(&test_db.log_manager);
        let bm1 = Arc::clone(&test_db.buffer_manager);
        let bid1 = block_id.clone();

        let fm2 = Arc::clone(&test_db.file_manager);
        let lm2 = Arc::clone(&test_db.log_manager);
        let bm2 = Arc::clone(&test_db.buffer_manager);
        let bid2 = block_id.clone();

        //  Create a read only transasction
        let t1 = std::thread::spawn(move || {
            let txn = Transaction::new(fm1, lm1, bm1);
            txn.pin(&bid1);
            txn.get_int(&bid1, 80).unwrap();
            txn.get_string(&bid1, 40).unwrap();
            txn.commit().unwrap();
        });

        //  Create a write only transaction
        let t2 = std::thread::spawn(move || {
            let txn = Transaction::new(fm2, lm2, bm2);
            txn.pin(&bid2.clone());
            txn.set_int(&bid2, 80, 1, false).unwrap();
            txn.set_string(&bid2, 40, "Hello", false).unwrap();
            txn.commit().unwrap();
        });
        t1.join().unwrap();
        t2.join().unwrap();

        //  Create a final read-only transaction that will read the written values
        let txn = Transaction::new(
            test_db.file_manager,
            test_db.log_manager,
            test_db.buffer_manager,
        );
        txn.pin(&block_id);
        assert_eq!(txn.get_int(&block_id, 80).unwrap(), 1);
        assert_eq!(txn.get_string(&block_id, 40).unwrap(), "Hello");
    }

    #[test]
    fn test_transaction_multi_threaded_multiple_readers_single_writer() {
        let file = generate_filename();
        let block_size = 512;
        let (test_db, test_dir) = SimpleDB::new_for_test(block_size, 10);
        let block_id = BlockId::new(file.to_string(), 1);

        let reader_threads = 10;
        let mut handles: Vec<JoinHandle<()>> = Vec::new();
        for _ in 0..reader_threads {
            let fm = Arc::clone(&test_db.file_manager);
            let lm = Arc::clone(&test_db.log_manager);
            let bm = Arc::clone(&test_db.buffer_manager);
            let bid = block_id.clone();

            handles.push(std::thread::spawn(move || {
                let txn = Transaction::new(fm, lm, bm);
                txn.pin(&bid);
                txn.get_int(&bid, 80).unwrap();
                txn.get_string(&bid, 40).unwrap();
                txn.commit().unwrap();
            }));
        }

        let txn = Transaction::new(
            test_db.file_manager.clone(),
            test_db.log_manager.clone(),
            test_db.buffer_manager.clone(),
        );
        txn.pin(&block_id);
        txn.set_int(&block_id, 80, 1, false).unwrap();
        txn.set_string(&block_id, 40, "Hello", false).unwrap();
        txn.commit().unwrap();

        handles
            .into_iter()
            .for_each(|handle| handle.join().unwrap());
    }

    #[test]
    fn test_transaction_rollback() {
        let file = generate_filename();
        let (test_db, test_dir) = SimpleDB::new_for_test(512, 3);
        let block_id = BlockId::new(file.clone(), 1);

        // Setup initial state
        let t1 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t1.pin(&block_id);
        t1.set_int(&block_id, 80, 100, true).unwrap();
        t1.set_string(&block_id, 40, "initial", true).unwrap();
        t1.commit().unwrap();

        // Start transaction that will modify multiple values but fail midway
        let t2 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t2.pin(&block_id);
        t2.set_int(&block_id, 80, 200, true).unwrap();
        t2.set_string(&block_id, 40, "modified", true).unwrap();
        // Simulate failure by rolling back
        t2.rollback().unwrap();

        // Verify that none of t2's changes persisted
        let t3 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t3.pin(&block_id);
        assert_eq!(t3.get_int(&block_id, 80).unwrap(), 100);
        assert_eq!(t3.get_string(&block_id, 40).unwrap(), "initial");
    }

    /// This test is actually a little bit of a scam. It does concurrent writes but doesn't verify what the final counter is
    /// because the transaction isolation level allows lost writes since all threads will read the same value initially and then overwrite each other's answer
    /// This test is purely about ensuring that all transactions succeed in a multi-threaded scenario
    #[test]
    fn test_transaction_isolation_with_concurrent_writes() {
        let file = generate_filename();
        let (test_db, test_dir) = SimpleDB::new_for_test(512, 3);
        let block_id = BlockId::new(file.clone(), 1);
        let num_of_txns = 5;
        let max_retry_count = 50;

        // Initialize data
        let t1 = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t1.pin(&block_id);
        t1.set_int(&block_id, 80, 0, true).unwrap();
        t1.commit().unwrap();

        // Create channel to track operations
        let (tx, rx) = std::sync::mpsc::channel();

        // Spawn transactions that will increment the value
        let mut handles = vec![];
        for i in 0..num_of_txns {
            let fm = Arc::clone(&test_db.file_manager);
            let lm = Arc::clone(&test_db.log_manager);
            let bm = Arc::clone(&test_db.buffer_manager);
            let bid = block_id.clone();
            let tx = tx.clone();

            handles.push(std::thread::spawn(move || {
                let mut retry_count = 0;
                let txn = Transaction::new(fm.clone(), lm.clone(), bm.clone());
                loop {
                    if retry_count > max_retry_count {
                        panic!("Too many retries");
                    }
                    txn.pin(&bid);

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
                                std::thread::sleep(Duration::from_millis(50));
                                continue;
                            }
                            // Other errors should fail the test
                            panic!("Transaction failed: {}", e);
                        }
                    }
                }
            }));
        }

        // Collect and log all operations
        let mut successful_increments = 0;
        let mut operations = vec![];

        loop {
            match rx.recv_timeout(Duration::from_secs(5)) {
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
                    println!("Operations so far: {:?}", operations);
                    panic!(
                        "Test timed out with {} successful increments",
                        successful_increments
                    );
                }
            }
        }

        // Wait for all threads
        for handle in handles {
            handle.join().unwrap();
        }

        // Verify final value
        let t_final = Transaction::new(
            Arc::clone(&test_db.file_manager),
            Arc::clone(&test_db.log_manager),
            Arc::clone(&test_db.buffer_manager),
        );
        t_final.pin(&block_id);
        assert!(t_final.get_int(&block_id, 80).unwrap() == num_of_txns);
    }

    #[test]
    fn test_transaction_durability() {
        let file = generate_filename();
        let dir = TestDir::new(format!("/tmp/recovery_test/{}", generate_random_number()));

        //  Phase 1: Create and populate database and then drop it
        {
            let db = SimpleDB::new(&dir, 512, 3, true);
            let t1 = Transaction::new(
                Arc::clone(&db.file_manager),
                Arc::clone(&db.log_manager),
                Arc::clone(&db.buffer_manager),
            );
            let block_id = BlockId::new(file.clone(), 1);
            t1.pin(&block_id);
            t1.set_int(&block_id, 80, 100, true).unwrap();
            t1.commit().unwrap();
        }

        //  Phase 2: Recover and verify
        {
            let db = SimpleDB::new(&dir, 512, 3, false);
            let t2 = Transaction::new(
                Arc::clone(&db.file_manager),
                Arc::clone(&db.log_manager),
                Arc::clone(&db.buffer_manager),
            );
            t2.recover().unwrap();

            let block_id = BlockId::new(file.clone(), 1);
            t2.pin(&block_id);
            assert_eq!(t2.get_int(&block_id, 80).unwrap(), 100);
        }
    }
}

struct LockState {
    readers: HashSet<TransactionID>, //  keep track of which transaction id's have a reader lock here
    writer: Option<TransactionID>,   //  keep track of the transaction writing to a specific block
    upgrade_requests: VecDeque<TransactionID>, //  keep track of upgrade requests to prevent writer starvation
}

/// Global struct used by all transactions to keep track of locks
struct LockTable {
    lock_table: Mutex<HashMap<BlockId, LockState>>,
    cond_var: Condvar,
    timeout: u64,
}

impl LockTable {
    fn new(timeout: u64) -> Self {
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
        if lock_table_guard
            .get(block_id)
            .unwrap()
            .writer
            .map_or(false, |id| id == tx_id)
        {
            return Ok(());
        }

        //  Maintain the invariant that any transaction that wants an xlock must first have an slock
        assert!(lock_table_guard
            .get(block_id)
            .unwrap()
            .readers
            .contains(&tx_id), "Transaction {} failed to have an slock before attempting to acquire xlock on block id {:?}", tx_id, block_id);

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
        return Ok(());
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
        let handle = std::thread::spawn(move || {
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
                std::thread::sleep(Duration::from_millis(100));
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
        let j1 = std::thread::spawn(move || {
            tx.send("Acquiring write lock".to_string()).unwrap();
            lt1.acquire_write_lock(1, &bid1).unwrap();
            tx.send("Acquired write lock".to_string()).unwrap();
        });

        //  Wait for T1 to start acquiring write lock and release T2's lock
        assert!(rx.recv().unwrap() == "Acquiring write lock".to_string());
        lock_table.release_locks(2, &block_id).unwrap();
        assert!(rx.recv().unwrap() == "Acquired write lock".to_string());
    }
}

/// The static instance of the lock table
static LOCK_TABLE_GENERATOR: OnceLock<Arc<LockTable>> = OnceLock::new();

enum LockType {
    Shared,
    Exclusive,
}

struct ConcurrencyManager {
    lock_table: Arc<LockTable>,
    locks: RefCell<HashMap<BlockId, LockType>>,
    tx_id: TransactionID,
}

impl ConcurrencyManager {
    fn new(tx_id: TransactionID, timeout: u64) -> Self {
        Self {
            lock_table: LOCK_TABLE_GENERATOR
                .get_or_init(|| Arc::new(LockTable::new(timeout)))
                .clone(),
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
        return Ok(());
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
struct RecoveryManager {
    tx_num: usize,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl RecoveryManager {
    fn new(
        tx_num: usize,
        log_manager: Arc<Mutex<LogManager>>,
        buffer_manager: Arc<Mutex<BufferManager>>,
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
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
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
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
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
        self.buffer_manager.lock().unwrap().flush_all(self.tx_num);
        //  Write a checkpoint record and flush it
        let checkpoint_record = LogRecord::Checkpoint;
        let lsn = checkpoint_record.write_log_record(Arc::clone(&self.log_manager))?;
        self.log_manager.lock().unwrap().flush_lsn(lsn);
        Ok(())
    }

    /// Write the [`LogRecord`] to set the value of an integer in a [`Buffer`]
    fn set_int(
        &self,
        buffer: &Buffer,
        offset: usize,
        _new_value: i32,
    ) -> Result<LSN, Box<dyn Error>> {
        let old_value = buffer.contents.get_int(offset);
        let block_id = buffer.block_id.clone().unwrap();
        let record = LogRecord::SetInt {
            txnum: self.tx_num,
            block_id,
            offset,
            old_val: old_value,
        };
        record.write_log_record(Arc::clone(&self.log_manager))
    }

    /// Write the [`LogRecord`] to set the value of a String in a [`Buffer`]
    fn set_string(
        &self,
        buffer: &Buffer,
        offset: usize,
        _new_value: &str,
    ) -> Result<LSN, Box<dyn Error>> {
        let old_value = buffer.contents.get_string(offset);
        let block_id = buffer.block_id.clone().unwrap();
        let record = LogRecord::SetString {
            txnum: self.tx_num,
            block_id,
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
        pinned_blocks: Vec<BlockId>,
        modified_ints: Mutex<Vec<(BlockId, usize, i32)>>,
        modified_strings: Mutex<Vec<(BlockId, usize, String)>>,
    }

    impl MockTransaction {
        fn new() -> Self {
            Self {
                pinned_blocks: Vec::new(),
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

        fn set_int(&self, block_id: &BlockId, offset: usize, val: i32, log: bool) {
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

        fn set_string(&self, block_id: &BlockId, offset: usize, val: &str, log: bool) {
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
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3);

        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mut mock_tx = MockTransaction::new();
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
        recovery_manager.rollback(&mut mock_tx).unwrap();

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
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3);
        let recovery_manager = RecoveryManager::new(
            1,
            Arc::clone(&db.log_manager),
            Arc::clone(&db.buffer_manager),
        );

        let mut mock_tx = MockTransaction::new();
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
        recovery_manager.rollback(&mut mock_tx).unwrap();

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
            LogRecord::Start(txnum) => write!(f, "Start({})", txnum),
            LogRecord::Commit(txnum) => write!(f, "Commit({})", txnum),
            LogRecord::Rollback(txnum) => write!(f, "Rollback({})", txnum),
            LogRecord::Checkpoint => write!(f, "Checkpoint"),
            LogRecord::SetInt {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetInt(txnum: {}, block_id: {:?}, offset: {}, old_val: {})",
                txnum, block_id, offset, old_val
            ),
            LogRecord::SetString {
                txnum,
                block_id,
                offset,
                old_val,
            } => write!(
                f,
                "SetString(txnum: {}, block_id: {:?}, offset: {}, old_val: {})",
                txnum, block_id, offset, old_val
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

                return Ok(LogRecord::SetInt {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                });
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

                return Ok(LogRecord::SetString {
                    txnum,
                    block_id: BlockId::new(filename, block_num),
                    offset,
                    old_val,
                });
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
    fn write_log_record(&self, log_manager: Arc<Mutex<LogManager>>) -> Result<LSN, Box<dyn Error>> {
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
struct HashMapValue {
    buffer: Arc<Mutex<Buffer>>,
    count: usize,
}

/// A wrapper to maintain the list of [`Buffer`] being used by the [`Transaction`]
/// It uses the [`BufferManager`] internally to maintain metadata
struct BufferList {
    buffers: RefCell<HashMap<BlockId, HashMapValue>>,
    buffer_manager: Arc<Mutex<BufferManager>>,
}

impl BufferList {
    fn new(buffer_manager: Arc<Mutex<BufferManager>>) -> Self {
        Self {
            buffers: RefCell::new(HashMap::new()),
            buffer_manager,
        }
    }

    /// Get the buffer associated with the provided block_id
    fn get_buffer(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        self.buffers
            .borrow()
            .get(block_id)
            .and_then(|v| Some(Arc::clone(&v.buffer)))
    }

    /// Pin the buffer associated with the provided [`BlockId`]
    fn pin(&self, block_id: &BlockId) {
        let buffer = self.buffer_manager.lock().unwrap().pin(block_id).unwrap();
        self.buffers
            .borrow_mut()
            .entry(block_id.clone())
            .and_modify(|v| v.count += 1)
            .or_insert(HashMapValue { buffer, count: 1 });
    }

    /// Unpin the buffer associated with the provided [`BlockId`]
    fn unpin(&self, block_id: &BlockId) {
        assert!(self.buffers.borrow().contains_key(block_id));
        let buffer = Arc::clone(&self.buffers.borrow().get(block_id).unwrap().buffer);
        self.buffer_manager.lock().unwrap().unpin(buffer);
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
        for buffer in buffers {
            self.buffer_manager
                .lock()
                .unwrap()
                .unpin(Arc::clone(&buffer.buffer));
        }
        buffer_guard.clear();
    }
}

#[cfg(test)]
mod buffer_list_tests {
    use std::sync::{Arc, Mutex};

    use crate::{test_utils::TestDir, BlockId, BufferList, BufferManager, FileManager, LogManager};

    #[test]
    fn test_buffer_list_functionality() {
        let dir = TestDir::new("buffer_list_tests");
        let file_manager = Arc::new(Mutex::new(FileManager::new(&dir, 400, true).unwrap()));
        let log_manager = Arc::new(Mutex::new(LogManager::new(
            Arc::clone(&file_manager),
            "buffer_list_tests_log_file",
        )));
        let buffer_manager = Arc::new(Mutex::new(BufferManager::new(file_manager, log_manager, 4)));
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

struct Buffer {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    contents: Page,
    block_id: Option<BlockId>,
    pins: usize,
    txn: Option<usize>,
    lsn: Option<LSN>,
}

impl Buffer {
    fn new(file_manager: Arc<Mutex<FileManager>>, log_manager: Arc<Mutex<LogManager>>) -> Self {
        let size = file_manager.lock().unwrap().blocksize;
        Self {
            file_manager,
            log_manager,
            contents: Page::new(size),
            block_id: None,
            pins: 0,
            txn: None,
            lsn: None,
        }
    }

    /// Mark that this buffer has been modified and set associated metadata for the modifying transaction
    fn set_modified(&mut self, txn_num: usize, lsn: usize) {
        self.txn = Some(txn_num);
        self.lsn = Some(lsn);
    }

    /// Check whether the buffer is pinned in memory
    fn is_pinned(&self) -> bool {
        self.pins > 0
    }

    /// Modify this buffer to hold the contents of a different block
    /// This requires flushing the existing page contents, if any, to disk if dirty
    fn assign_to_block(&mut self, block_id: &BlockId) {
        self.flush();
        self.block_id = Some(block_id.clone());
        self.file_manager
            .lock()
            .unwrap()
            .read(self.block_id.as_ref().unwrap(), &mut self.contents);
        self.reset_pins();
    }

    /// Write the current buffer contents to disk if dirty
    fn flush(&mut self) {
        if let Some(_) = &self.txn {
            self.log_manager
                .lock()
                .unwrap()
                .flush_lsn(self.lsn.unwrap());
            self.file_manager
                .lock()
                .unwrap()
                .write(self.block_id.as_ref().unwrap(), &mut self.contents);
        }
    }

    /// Increment the pin count for this buffer
    fn pin(&mut self) {
        self.pins += 1;
    }

    /// Decrement the pin count for this buffer
    fn unpin(&mut self) {
        assert!(self.pins > 0); //  sanity check to know that it will not become negative
        self.pins -= 1;
    }

    /// Reset the pin count for this buffer
    fn reset_pins(&mut self) {
        self.pins = 0;
    }
}

struct BufferManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_manager: Arc<Mutex<LogManager>>,
    buffer_pool: Vec<Arc<Mutex<Buffer>>>,
    num_available: Mutex<usize>,
    cond: Condvar,
}

impl BufferManager {
    const MAX_TIME: u64 = 10; //  10 seconds
    fn new(
        file_manager: Arc<Mutex<FileManager>>,
        log_manager: Arc<Mutex<LogManager>>,
        num_buffers: usize,
    ) -> Self {
        let buffer_pool = (0..num_buffers)
            .map(|_| {
                Arc::new(Mutex::new(Buffer::new(
                    Arc::clone(&file_manager),
                    Arc::clone(&log_manager),
                )))
            })
            .collect();
        Self {
            file_manager,
            log_manager,
            buffer_pool,
            num_available: Mutex::new(num_buffers),
            cond: Condvar::new(),
        }
    }

    /// Returns the number of unpinned buffers, that is buffers with no pages pinned to them
    fn available(&self) -> usize {
        *self.num_available.lock().unwrap()
    }

    /// Flushes the dirty buffers modified by this specific transaction
    fn flush_all(&mut self, txn_num: usize) {
        for buffer in &mut self.buffer_pool {
            let mut buffer = buffer.lock().unwrap();
            if buffer.txn.is_some() && *buffer.txn.as_ref().unwrap() == txn_num {
                buffer.flush();
            }
        }
    }

    /// Pin the buffer associated with the provided block_id
    /// It depends on [`BufferManager::try_to_pin`] to get a buffer back
    /// Once the buffer has been retrieved, it will handle metadata operations
    fn pin(&self, block_id: &BlockId) -> Result<Arc<Mutex<Buffer>>, Box<dyn Error>> {
        let start = Instant::now();
        let mut num_available = self.num_available.lock().unwrap();
        loop {
            match self.try_to_pin(block_id) {
                Some(buffer) => {
                    {
                        let mut buffer_guard = buffer.lock().unwrap();
                        if !buffer_guard.is_pinned() {
                            *num_available -= 1;
                        }
                        buffer_guard.pin();
                    }
                    return Ok(buffer);
                }
                None => {
                    num_available = self.cond.wait(num_available).unwrap();
                    if start.elapsed() > Duration::from_secs(Self::MAX_TIME) {
                        return Err("Timed out waiting for buffer".into());
                    }
                }
            }
        }
    }

    /// Find a buffer to pin this block to
    /// First check to see if there is an existing buffer for this block
    /// If not, try to find an unpinned buffer
    /// If both cases above fail, return None
    /// Update matadata for the assigned buffer before returning
    fn try_to_pin(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        let buffer = match self.find_existing_buffer(block_id) {
            Some(buffer) => buffer,
            None => match self.choose_unpinned_buffer() {
                Some(buffer) => {
                    buffer.lock().unwrap().assign_to_block(block_id);
                    buffer
                }
                None => return None,
            },
        };
        return Some(buffer);
    }

    /// Decrement the pin count for the provided buffer
    /// If all of the pins have been removed, managed metadata & notify waiting threads
    fn unpin(&self, buffer: Arc<Mutex<Buffer>>) {
        let mut buffer_guard = buffer.lock().unwrap();
        buffer_guard.unpin();
        if !buffer_guard.is_pinned() {
            *self.num_available.lock().unwrap() += 1;
            self.cond.notify_all();
        }
    }

    /// Look for a buffer associated with this specific [`BlockId`]
    fn find_existing_buffer(&self, block_id: &BlockId) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let buffer_guard = buffer.lock().unwrap();
            if buffer_guard.block_id.is_some()
                && buffer_guard.block_id.as_ref().unwrap() == block_id
            {
                return Some(Arc::clone(&buffer));
            }
        }
        None
    }

    /// Try to find an unpinned buffer and return pointer to that, if present
    fn choose_unpinned_buffer(&self) -> Option<Arc<Mutex<Buffer>>> {
        for buffer in &self.buffer_pool {
            let buffer_guard = buffer.lock().unwrap();
            if !buffer_guard.is_pinned() {
                return Some(Arc::clone(&buffer));
            }
        }
        None
    }
}

#[cfg(test)]
mod buffer_manager_tests {
    use crate::{BlockId, Page, SimpleDB};

    /// This test will assert that when the buffer pool swaps out a page from the buffer pool, it properly flushes those contents to disk
    /// and can then correctly read them back later
    #[test]
    fn test_buffer_replacement() {
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3); // use 3 buffer slots
        let buffer_manager = db.buffer_manager;

        //  Initialize the file with enough data
        let block_id = BlockId::new("testfile".to_string(), 1);
        let mut page = Page::new(400);
        page.set_int(80, 1);
        db.file_manager.lock().unwrap().write(&block_id, &mut page);

        let buffer_manager_guard = buffer_manager.lock().unwrap();

        //  Create a buffer for block 1 and modify it
        let buffer_1 = buffer_manager_guard
            .pin(&BlockId::new("testfile".to_string(), 1))
            .unwrap();
        buffer_1.lock().unwrap().contents.set_int(80, 100);
        buffer_1.lock().unwrap().set_modified(1, 0);
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
        assert_eq!(buffer_2.lock().unwrap().contents.get_int(80), 100);
    }
}

struct LogManager {
    file_manager: Arc<Mutex<FileManager>>,
    log_file: String,
    log_page: Page,
    current_block: BlockId,
    latest_lsn: usize,
    last_saved_lsn: usize,
}

impl LogManager {
    fn new(file_manager: Arc<Mutex<FileManager>>, log_file: &str) -> Self {
        let bytes = vec![0; file_manager.lock().unwrap().blocksize];
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

    /// Determine if this LSN has been flushed to disk, and flush it if it hasn't
    fn flush_lsn(&mut self, lsn: LSN) {
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
        self.last_saved_lsn = self.latest_lsn;
    }

    /// Write the log_record to the log page
    /// First, check if there is enough space
    fn append(&mut self, log_record: Vec<u8>) -> LSN {
        let mut boundary = self.log_page.get_int(0) as usize;
        let bytes_needed = log_record.len() + Page::INT_BYTES;
        if boundary.saturating_sub(bytes_needed) < Page::INT_BYTES {
            self.flush_to_disk();
            self.current_block = LogManager::append_new_block(
                &mut self.file_manager,
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
    fn append_new_block(
        file_manager: &Arc<Mutex<FileManager>>,
        log_file: &str,
        log_page: &mut Page,
    ) -> BlockId {
        let block_id = file_manager.lock().unwrap().append(log_file.to_string());
        log_page.set_int(
            0,
            file_manager.lock().unwrap().blocksize.try_into().unwrap(),
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

struct LogIterator {
    file_manager: Arc<Mutex<FileManager>>,
    current_block: BlockId,
    page: Page,
    current_pos: usize,
    boundary: usize,
}

impl LogIterator {
    fn new(file_manager: Arc<Mutex<FileManager>>, current_block: BlockId) -> Self {
        let block_size = file_manager.lock().unwrap().blocksize;
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

    fn move_to_block(&mut self) {
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
        if self.current_pos >= self.file_manager.lock().unwrap().blocksize {
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
        record.write_all(&string_bytes).unwrap();
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
        println!("");
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
        let (db, _test_dir) = SimpleDB::new_for_test(400, 3);
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
struct BlockId {
    filename: String,
    block_num: usize,
}

impl BlockId {
    fn new(filename: String, block_num: usize) -> Self {
        Self {
            filename,
            block_num,
        }
    }
}

/// The page struct that contains the contents of a page
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
    fn set_int(&mut self, offset: usize, n: i32) {
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&n.to_be_bytes());
    }

    /// Get the raw bytes from the page at the given offset for the given length
    fn get_raw_bytes(&self, offset: usize, length: usize) -> Vec<u8> {
        let bytes = &self.contents[offset..offset + length];
        bytes.to_vec()
    }

    /// Get a slice of bytes from the page at the given offset. Read the length and then the bytes
    fn get_bytes(&self, mut offset: usize) -> Vec<u8> {
        let length_bytes = &self.contents[offset..offset + Self::INT_BYTES];
        let bytes: [u8; Self::INT_BYTES] = self.contents[offset..offset + Self::INT_BYTES]
            .try_into()
            .unwrap();
        let length = u32::from_be_bytes(bytes) as usize;
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + length].to_vec()
    }

    /// Set a slice of bytes at the given offset. Write the length and then the bytes
    fn set_bytes(&mut self, mut offset: usize, bytes: &[u8]) {
        let length = bytes.len() as u32;
        let length_bytes = length.to_be_bytes();
        self.contents[offset..offset + Self::INT_BYTES].copy_from_slice(&length.to_be_bytes());
        offset = offset + Self::INT_BYTES;
        self.contents[offset..offset + bytes.len()].copy_from_slice(&bytes);
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

/// The file manager struct that manages the files in the database
struct FileManager {
    db_directory: PathBuf,
    blocksize: usize,
    open_files: HashMap<String, File>,
}

impl FileManager {
    fn new<P>(db_directory: &P, blocksize: usize, clean: bool) -> io::Result<Self>
    where
        P: AsRef<Path>,
    {
        let db_path = db_directory.as_ref().to_path_buf();
        fs::create_dir_all(&db_path)?;

        if clean {
            //  remove all existing files in the directory
            for entry in fs::read_dir(&db_path)? {
                let entry = entry?;
                if entry.file_type()?.is_file() {
                    fs::remove_file(entry.path())?;
                }
            }
        }

        Ok(Self {
            db_directory: db_path,
            blocksize,
            open_files: HashMap::new(),
        })
    }

    /// Get the length of the file in blocks
    fn length(&mut self, filename: String) -> usize {
        let file = self.get_file(&filename);
        let len = file.metadata().unwrap().len() as usize;
        len / self.blocksize
    }

    /// Read the block provided by the block_id into the provided page
    fn read(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        match file.read_exact(&mut page.contents) {
            Ok(_) => (),
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                page.contents = vec![0; self.blocksize];
            }
            Err(e) => panic!("Failed to read from file {}", e),
        }
    }

    /// Write the page to the block provided by the block_id
    fn write(&mut self, block_id: &BlockId, page: &mut Page) {
        let mut file = self.get_file(&block_id.filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize) as u64,
        ))
        .unwrap();
        file.write(&page.contents).unwrap();
    }

    /// Append a new, empty block to the file and return
    fn append(&mut self, filename: String) -> BlockId {
        let new_blk_num = self.length(filename.clone());
        let block_id = BlockId::new(filename.clone(), new_blk_num);
        let buffer = Page::new(self.blocksize);
        let mut file = self.get_file(&filename);
        file.seek(io::SeekFrom::Start(
            (block_id.block_num * self.blocksize).try_into().unwrap(),
        ))
        .unwrap();
        file.write(&buffer.contents).unwrap();
        block_id
    }

    /// Get the file handle for the file with the given filename or create it if it doesn't exist
    fn get_file(&mut self, filename: &str) -> File {
        self.open_files
            .entry(filename.to_string())
            .or_insert_with(|| {
                OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .open(self.db_directory.join(filename))
                    .expect("Failed to open file")
            })
            .try_clone()
            .unwrap()
    }
}

#[cfg(test)]
mod file_manager_tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::{test_utils::TestDir, FileManager};

    fn setup() -> (TestDir, FileManager) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis();
        let thread_id = std::thread::current().id();
        let dir = TestDir::new(format!("/tmp/test_db_{}_{:?}", timestamp, thread_id));
        let file_manger = FileManager::new(&dir, 400, true).unwrap();
        (dir, file_manger)
    }

    #[test]
    fn test_file_creation() {
        let (_temp_dir, mut file_manager) = setup();

        let filename = "test_file";
        file_manager.get_file(filename);

        assert!(file_manager.open_files.contains_key(filename));
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

fn main() {}
