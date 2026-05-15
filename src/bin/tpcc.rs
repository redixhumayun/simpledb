#![allow(clippy::arc_with_non_send_sync)]

use simpledb::{Constant, ExecutionContext, LockError, Scan, SimpleDB};
use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

struct Config {
    warehouses: i32,
    items: i32,
    customers_per_district: i32,
    duration: Duration,
    db_path: Option<String>,
    buffers: usize,
    lock_timeout_ms: u64,
    skip_load: bool,
}

fn parse_args() -> Config {
    let mut warehouses = 1i32;
    let mut items = 10_000i32;
    let mut cpd = 300i32; // customers per district
    let mut duration_secs = 60u64;
    let mut db_path: Option<String> = None;
    let mut buffers = 256usize;
    let mut lock_timeout_ms = 5000u64;
    let mut skip_load = false;

    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut i = 0;
    while i < args.len() {
        match args[i].as_str() {
            "--warehouses" => {
                warehouses = args[i + 1].parse().expect("--warehouses <N>");
                i += 2;
            }
            "--items" => {
                items = args[i + 1].parse().expect("--items <N>");
                i += 2;
            }
            "--customers-per-district" | "--cpd" => {
                cpd = args[i + 1].parse().expect("--cpd <N>");
                i += 2;
            }
            "--duration" => {
                duration_secs = args[i + 1].parse().expect("--duration <secs>");
                i += 2;
            }
            "--db" => {
                db_path = Some(args[i + 1].clone());
                i += 2;
            }
            "--buffers" => {
                buffers = args[i + 1].parse().expect("--buffers <N>");
                i += 2;
            }
            "--lock-timeout" => {
                lock_timeout_ms = args[i + 1].parse().expect("--lock-timeout <ms>");
                i += 2;
            }
            "--skip-load" => {
                skip_load = true;
                i += 1;
            }
            "--help" | "-h" => {
                eprintln!(
                    "tpcc [options]\n\
                     --warehouses N       number of warehouses (default 1)\n\
                     --items N            item count (default 10000; spec=100000)\n\
                     --cpd N              customers per district (default 300; spec=3000)\n\
                     --duration N         benchmark duration in seconds (default 60)\n\
                     --db PATH            database directory (default: temp dir)\n\
                     --buffers N          buffer pool size (default 256)\n\
                     --lock-timeout N     lock timeout in ms (default 5000)\n\
                     --skip-load          skip data loading phase"
                );
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown flag: {other}");
                std::process::exit(1);
            }
        }
    }

    Config {
        warehouses,
        items,
        customers_per_district: cpd,
        duration: Duration::from_secs(duration_secs),
        db_path,
        buffers,
        lock_timeout_ms,
        skip_load,
    }
}

// ---------------------------------------------------------------------------
// Random number generator (xorshift64)
// ---------------------------------------------------------------------------

struct Rng(u64);

impl Rng {
    fn new() -> Self {
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64;
        Self(seed | 1)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn uniform(&mut self, lo: i32, hi: i32) -> i32 {
        debug_assert!(lo <= hi);
        let range = (hi - lo + 1) as u64;
        (self.next_u64() % range) as i32 + lo
    }

    fn uniform_f64(&mut self, lo: f64, hi: f64) -> f64 {
        let r = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        lo + r * (hi - lo)
    }

    fn astring(&mut self, min_len: usize, max_len: usize) -> String {
        let len = self.uniform(min_len as i32, max_len as i32) as usize;
        const CHARS: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        (0..len)
            .map(|_| CHARS[(self.next_u64() % CHARS.len() as u64) as usize] as char)
            .collect()
    }

    fn nstring(&mut self, min_len: usize, max_len: usize) -> String {
        let len = self.uniform(min_len as i32, max_len as i32) as usize;
        (0..len)
            .map(|_| ((self.next_u64() % 10) as u8 + b'0') as char)
            .collect()
    }
}

const LAST_NAME_SYLLABLES: [&str; 10] = [
    "BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING",
];

fn make_last_name(n: usize) -> String {
    format!(
        "{}{}{}",
        LAST_NAME_SYLLABLES[n / 100],
        LAST_NAME_SYLLABLES[(n / 10) % 10],
        LAST_NAME_SYLLABLES[n % 10]
    )
}

fn now_secs() -> i32 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i32
}

fn sql_str(s: &str) -> String {
    format!("'{}'", s.replace('\'', "''"))
}

fn fmt_f(v: f64) -> String {
    format!("{:.6}", v)
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn run_query(
    db: &SimpleDB,
    txn: &Arc<simpledb::Transaction>,
    sql: &str,
    fields: &[&str],
) -> Result<Vec<Vec<Constant>>> {
    let plan = db
        .planner
        .create_query_plan(sql.to_string(), Arc::clone(txn))?;
    let ctx = ExecutionContext::new(Arc::clone(txn));
    let mut scan = plan.open(&ctx);
    scan.before_first()?;
    let mut rows = Vec::new();
    while let Some(r) = scan.next() {
        r?;
        let mut row = Vec::with_capacity(fields.len());
        for &f in fields {
            row.push(scan.get_value(f)?);
        }
        rows.push(row);
    }
    Ok(rows)
}

fn exec_update(db: &SimpleDB, txn: &Arc<simpledb::Transaction>, sql: &str) -> Result<usize> {
    #[allow(clippy::needless_question_mark)]
    Ok(db
        .planner
        .execute_update(sql.to_string(), Arc::clone(txn))?)
}

#[allow(clippy::borrowed_box)]
fn is_lock_error(e: &Box<dyn Error>) -> bool {
    e.downcast_ref::<LockError>().is_some()
}

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

const CREATE_TABLES: &[&str] = &[
    "CREATE TABLE warehouse (\
        w_id int, w_name varchar(10), \
        w_street_1 varchar(20), w_street_2 varchar(20), \
        w_city varchar(20), w_state varchar(2), w_zip varchar(9), \
        w_tax float, w_ytd float)",
    "CREATE TABLE district (\
        d_w_id int, d_id int, d_name varchar(10), \
        d_street_1 varchar(20), d_street_2 varchar(20), \
        d_city varchar(20), d_state varchar(2), d_zip varchar(9), \
        d_tax float, d_ytd float, d_next_o_id int)",
    // c_data varchar(500): heap is dense-encoded, so only actual string bytes are stored.
    // max_encoded_size() is large but that only affects btree index capacity estimates.
    "CREATE TABLE customer (\
        c_w_id int, c_d_id int, c_id int, \
        c_first varchar(16), c_middle varchar(2), c_last varchar(16), \
        c_street_1 varchar(20), c_street_2 varchar(20), \
        c_city varchar(20), c_state varchar(2), c_zip varchar(9), \
        c_phone varchar(16), c_since int, \
        c_credit varchar(2), c_credit_lim float, \
        c_discount float, c_balance float, c_ytd_payment float, \
        c_payment_cnt int, c_delivery_cnt int, c_data varchar(500))",
    "CREATE TABLE history (\
        h_c_id int, h_c_d_id int, h_c_w_id int, \
        h_d_id int, h_w_id int, h_date int, \
        h_amount float, h_data varchar(24))",
    "CREATE TABLE new_order (\
        no_o_id int, no_d_id int, no_w_id int)",
    // Renamed from ORDER (SQL keyword) to orders.
    "CREATE TABLE orders (\
        o_id int, o_d_id int, o_w_id int, o_c_id int, \
        o_entry_d int, o_carrier_id int, o_ol_cnt int, o_all_local int)",
    "CREATE TABLE order_line (\
        ol_o_id int, ol_d_id int, ol_w_id int, ol_number int, \
        ol_i_id int, ol_supply_w_id int, ol_delivery_d int, \
        ol_quantity int, ol_amount float, ol_dist_info varchar(24))",
    "CREATE TABLE item (\
        i_id int, i_im_id int, i_name varchar(24), i_price float, i_data varchar(50))",
    "CREATE TABLE stock (\
        s_i_id int, s_w_id int, s_quantity int, \
        s_dist_01 varchar(24), s_dist_02 varchar(24), s_dist_03 varchar(24), \
        s_dist_04 varchar(24), s_dist_05 varchar(24), s_dist_06 varchar(24), \
        s_dist_07 varchar(24), s_dist_08 varchar(24), s_dist_09 varchar(24), \
        s_dist_10 varchar(24), \
        s_ytd int, s_order_cnt int, s_remote_cnt int, s_data varchar(50))",
];

fn load_schema(db: &SimpleDB) -> Result<()> {
    for sql in CREATE_TABLES {
        let txn = db.new_tx();
        exec_update(db, &txn, sql)?;
        txn.write_session().commit()?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Data loading
// ---------------------------------------------------------------------------

fn load_items(db: &SimpleDB, rng: &mut Rng, item_count: i32) -> Result<()> {
    eprintln!("  loading {} items...", item_count);
    // Batch into chunks to limit transaction size
    const BATCH: i32 = 500;
    let mut i_id = 1i32;
    while i_id <= item_count {
        let txn = db.new_tx();
        let end = (i_id + BATCH - 1).min(item_count);
        for id in i_id..=end {
            let i_im_id = rng.uniform(1, 10000);
            let name = rng.astring(14, 24);
            let price = rng.uniform_f64(1.0, 100.0);
            // ~10% of items are "original" (contain ORIGINAL in data)
            let data = if rng.uniform(1, 10) == 1 {
                let mut s = rng.astring(26, 50);
                let pos = rng.uniform(0, (s.len() as i32) - 8).max(0) as usize;
                s.replace_range(pos..pos, "ORIGINAL");
                s.truncate(50);
                s
            } else {
                rng.astring(26, 50)
            };
            let sql = format!(
                "INSERT INTO item (i_id, i_im_id, i_name, i_price, i_data) \
                 VALUES ({id}, {i_im_id}, {}, {}, {})",
                sql_str(&name),
                fmt_f(price),
                sql_str(&data)
            );
            exec_update(db, &txn, &sql)?;
        }
        txn.write_session().commit()?;
        i_id = end + 1;
    }
    Ok(())
}

fn load_warehouse(db: &SimpleDB, rng: &mut Rng, w_id: i32) -> Result<()> {
    let txn = db.new_tx();
    let name = rng.astring(6, 10);
    let street1 = rng.astring(10, 20);
    let street2 = rng.astring(10, 20);
    let city = rng.astring(10, 20);
    let state = rng.astring(2, 2);
    let zip = format!("{:04}11111", rng.uniform(0, 9999));
    let tax = rng.uniform_f64(0.0, 0.2);
    let ytd = 300_000.0f64;
    let sql = format!(
        "INSERT INTO warehouse \
         (w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) \
         VALUES ({w_id}, {}, {}, {}, {}, {}, {}, {}, {})",
        sql_str(&name),
        sql_str(&street1),
        sql_str(&street2),
        sql_str(&city),
        sql_str(&state),
        sql_str(&zip),
        fmt_f(tax),
        fmt_f(ytd)
    );
    exec_update(db, &txn, &sql)?;
    txn.write_session().commit()?;
    Ok(())
}

fn load_district(db: &SimpleDB, rng: &mut Rng, w_id: i32, d_id: i32, cpd: i32) -> Result<()> {
    let txn = db.new_tx();
    let name = rng.astring(6, 10);
    let street1 = rng.astring(10, 20);
    let street2 = rng.astring(10, 20);
    let city = rng.astring(10, 20);
    let state = rng.astring(2, 2);
    let zip = format!("{:04}11111", rng.uniform(0, 9999));
    let tax = rng.uniform_f64(0.0, 0.2);
    let ytd = 30_000.0f64;
    // d_next_o_id starts at cpd + 1 (orders 1..=cpd are pre-loaded)
    let next_o_id = cpd + 1;
    let sql = format!(
        "INSERT INTO district \
         (d_w_id, d_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, \
          d_tax, d_ytd, d_next_o_id) \
         VALUES ({w_id}, {d_id}, {}, {}, {}, {}, {}, {}, {}, {}, {next_o_id})",
        sql_str(&name),
        sql_str(&street1),
        sql_str(&street2),
        sql_str(&city),
        sql_str(&state),
        sql_str(&zip),
        fmt_f(tax),
        fmt_f(ytd)
    );
    exec_update(db, &txn, &sql)?;
    txn.write_session().commit()?;
    Ok(())
}

fn load_customers(db: &SimpleDB, rng: &mut Rng, w_id: i32, d_id: i32, cpd: i32) -> Result<()> {
    const BATCH: i32 = 200;
    let mut c_id = 1i32;
    while c_id <= cpd {
        let txn = db.new_tx();
        let end = (c_id + BATCH - 1).min(cpd);
        for id in c_id..=end {
            let first = rng.astring(8, 16);
            let middle = "OE";
            // Last name: first 1000 customers get deterministic names, rest random
            let last = if id <= 1000 {
                make_last_name((id - 1) as usize)
            } else {
                make_last_name(rng.uniform(0, 999) as usize)
            };
            let street1 = rng.astring(10, 20);
            let street2 = rng.astring(10, 20);
            let city = rng.astring(10, 20);
            let state = rng.astring(2, 2);
            let zip = format!("{:04}11111", rng.uniform(0, 9999));
            let phone = rng.nstring(16, 16);
            let since = now_secs();
            // 10% bad credit
            let credit = if rng.uniform(1, 10) == 1 { "BC" } else { "GC" };
            let credit_lim = 50_000.0f64;
            let discount = rng.uniform_f64(0.0, 0.5);
            let balance = -10.0f64;
            let ytd_payment = 10.0f64;
            // Fixed at 500 chars so Payment's c_data update never grows the row.
            // update_tuple can only redirect within the same page (UPDATE_SLACK=4 bytes),
            // so a growing string would panic if the page has no free space.
            let data = rng.astring(500, 500);

            let sql = format!(
                "INSERT INTO customer \
                 (c_w_id, c_d_id, c_id, c_first, c_middle, c_last, \
                  c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, \
                  c_credit, c_credit_lim, c_discount, c_balance, c_ytd_payment, \
                  c_payment_cnt, c_delivery_cnt, c_data) \
                 VALUES ({w_id}, {d_id}, {id}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {since}, \
                         {}, {}, {}, {}, {}, 1, 0, {})",
                sql_str(&first),
                sql_str(middle),
                sql_str(&last),
                sql_str(&street1),
                sql_str(&street2),
                sql_str(&city),
                sql_str(&state),
                sql_str(&zip),
                sql_str(&phone),
                sql_str(credit),
                fmt_f(credit_lim),
                fmt_f(discount),
                fmt_f(balance),
                fmt_f(ytd_payment),
                sql_str(&data)
            );
            exec_update(db, &txn, &sql)?;
        }
        txn.write_session().commit()?;
        c_id = end + 1;
    }
    Ok(())
}

fn load_orders(
    db: &SimpleDB,
    rng: &mut Rng,
    w_id: i32,
    d_id: i32,
    cpd: i32,
    item_count: i32,
) -> Result<()> {
    // Generate cpd orders, shuffled customer IDs so each customer has exactly one order.
    let mut cids: Vec<i32> = (1..=cpd).collect();
    // Fisher-Yates shuffle
    for i in (1..cids.len()).rev() {
        let j = (rng.next_u64() % (i as u64 + 1)) as usize;
        cids.swap(i, j);
    }

    let entry_d = now_secs();
    const BATCH: i32 = 100;
    let mut o_id = 1i32;

    while o_id <= cpd {
        let txn = db.new_tx();
        let end = (o_id + BATCH - 1).min(cpd);

        for oid in o_id..=end {
            let c_id = cids[(oid - 1) as usize];
            let ol_cnt = rng.uniform(5, 15);
            // Last 10% of orders (2101..=3000 for cpd=3000) go into new_order
            let carrier_id = if oid <= (cpd * 7 / 10) {
                rng.uniform(1, 10)
            } else {
                0 // 0 = NULL sentinel: order not yet delivered
            };

            // Insert order
            let sql = format!(
                "INSERT INTO orders \
                 (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) \
                 VALUES ({oid}, {d_id}, {w_id}, {c_id}, {entry_d}, {carrier_id}, {ol_cnt}, 1)"
            );
            exec_update(db, &txn, &sql)?;

            // Undelivered orders go into new_order
            if carrier_id == 0 {
                let sql = format!(
                    "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) \
                     VALUES ({oid}, {d_id}, {w_id})"
                );
                exec_update(db, &txn, &sql)?;
            }

            // Insert order lines
            let delivery_d = if carrier_id != 0 { entry_d } else { 0 };
            for ol_num in 1..=ol_cnt {
                let i_id = rng.uniform(1, item_count);
                let quantity = 5i32;
                // Price will be re-read on delivery; use 0 for pre-loaded undelivered lines
                let amount = if carrier_id != 0 {
                    rng.uniform_f64(0.01, 9_999.99)
                } else {
                    0.0
                };
                let dist_info = rng.astring(24, 24);
                let sql = format!(
                    "INSERT INTO order_line \
                     (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
                      ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) \
                     VALUES ({oid}, {d_id}, {w_id}, {ol_num}, {i_id}, {w_id}, \
                             {delivery_d}, {quantity}, {}, {})",
                    fmt_f(amount),
                    sql_str(&dist_info)
                );
                exec_update(db, &txn, &sql)?;
            }
        }
        txn.write_session().commit()?;
        o_id = end + 1;
    }
    Ok(())
}

fn load_stock(db: &SimpleDB, rng: &mut Rng, w_id: i32, item_count: i32) -> Result<()> {
    eprintln!("  loading {} stock rows for w_id={}...", item_count, w_id);
    const BATCH: i32 = 200;
    let mut s_id = 1i32;
    while s_id <= item_count {
        let txn = db.new_tx();
        let end = (s_id + BATCH - 1).min(item_count);
        for id in s_id..=end {
            let qty = rng.uniform(10, 100);
            let dists: Vec<String> = (0..10).map(|_| sql_str(&rng.astring(24, 24))).collect();
            let data = if rng.uniform(1, 10) == 1 {
                let mut s = rng.astring(26, 50);
                let pos = rng.uniform(0, (s.len() as i32) - 8).max(0) as usize;
                s.replace_range(pos..pos, "ORIGINAL");
                s.truncate(50);
                s
            } else {
                rng.astring(26, 50)
            };
            let sql = format!(
                "INSERT INTO stock \
                 (s_i_id, s_w_id, s_quantity, \
                  s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, \
                  s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, \
                  s_ytd, s_order_cnt, s_remote_cnt, s_data) \
                 VALUES ({id}, {w_id}, {qty}, \
                         {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, \
                         0, 0, 0, {})",
                dists[0],
                dists[1],
                dists[2],
                dists[3],
                dists[4],
                dists[5],
                dists[6],
                dists[7],
                dists[8],
                dists[9],
                sql_str(&data)
            );
            exec_update(db, &txn, &sql)?;
        }
        txn.write_session().commit()?;
        s_id = end + 1;
    }
    Ok(())
}

fn create_indexes(db: &SimpleDB) -> Result<()> {
    // Single-field indexes only (composite indexes are issue #113).
    let indexes = [
        "CREATE INDEX wh_idx ON warehouse (w_id)",
        "CREATE INDEX dist_wid_idx ON district (d_w_id)",
        "CREATE INDEX cust_id_idx ON customer (c_id)",
        "CREATE INDEX cust_last_idx ON customer (c_last)",
        "CREATE INDEX cust_wid_idx ON customer (c_w_id)",
        "CREATE INDEX ord_wid_idx ON orders (o_w_id)",
        "CREATE INDEX ord_cid_idx ON orders (o_c_id)",
        "CREATE INDEX no_wid_idx ON new_order (no_w_id)",
        "CREATE INDEX ol_oid_idx ON order_line (ol_o_id)",
        "CREATE INDEX ol_wid_idx ON order_line (ol_w_id)",
        "CREATE INDEX stock_iid_idx ON stock (s_i_id)",
        "CREATE INDEX stock_wid_idx ON stock (s_w_id)",
        "CREATE INDEX item_id_idx ON item (i_id)",
    ];
    for sql in &indexes {
        let txn = db.new_tx();
        exec_update(db, &txn, sql)?;
        txn.write_session().commit()?;
    }
    Ok(())
}

fn load_data(db: &SimpleDB, cfg: &Config) -> Result<()> {
    let rng = &mut Rng::new();

    // Indexes must be created BEFORE loading data: SimpleDB's CREATE INDEX only
    // registers the index in the catalog and does not backfill existing rows.
    // The IndexUpdatePlanner then maintains the btree on every subsequent INSERT.
    eprintln!("Creating indexes...");
    create_indexes(db)?;

    eprintln!("Loading items...");
    load_items(db, rng, cfg.items)?;

    for w_id in 1..=cfg.warehouses {
        eprintln!("Loading warehouse {}...", w_id);
        load_warehouse(db, rng, w_id)?;

        for d_id in 1..=10 {
            load_district(db, rng, w_id, d_id, cfg.customers_per_district)?;
            load_customers(db, rng, w_id, d_id, cfg.customers_per_district)?;
            load_orders(db, rng, w_id, d_id, cfg.customers_per_district, cfg.items)?;
        }
        load_stock(db, rng, w_id, cfg.items)?;
    }

    eprintln!("Load complete.");
    Ok(())
}

// ---------------------------------------------------------------------------
// Transaction implementations
// ---------------------------------------------------------------------------

// Returns Ok(true) if the transaction committed, Ok(false) if it rolled back
// for a business reason (e.g. item not found), Err on infrastructure failure.
type TxnOutcome = Result<bool>;

fn do_new_order(db: &SimpleDB, rng: &mut Rng, cfg: &Config) -> TxnOutcome {
    let w_id = rng.uniform(1, cfg.warehouses);
    let d_id = rng.uniform(1, 10);
    let c_id = rng.uniform(1, cfg.customers_per_district);
    let ol_cnt = rng.uniform(5, 15);

    let txn = db.new_tx();
    let outcome = (|| -> Result<bool> {
        // 1. Warehouse tax
        let sql = format!("SELECT w_tax FROM warehouse WHERE w_id = {w_id}");
        let rows = run_query(db, &txn, &sql, &["w_tax"])?;
        let w_tax = rows[0][0].as_float();

        // 2. District tax + next order ID
        let sql = format!(
            "SELECT d_tax, d_next_o_id FROM district WHERE d_w_id = {w_id} AND d_id = {d_id}"
        );
        let rows = run_query(db, &txn, &sql, &["d_tax", "d_next_o_id"])?;
        let d_tax = rows[0][0].as_float();
        let o_id = rows[0][1].as_int();

        // 3. Increment d_next_o_id
        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE district SET d_next_o_id = {} WHERE d_w_id = {w_id} AND d_id = {d_id}",
                o_id + 1
            ),
        )?;

        // 4. Customer discount
        let sql = format!(
            "SELECT c_discount, c_last, c_credit FROM customer \
             WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
        );
        let rows = run_query(db, &txn, &sql, &["c_discount", "c_last", "c_credit"])?;
        let c_discount = rows[0][0].as_float();

        // 5. Insert order header (carrier_id = 0 = not-yet-assigned)
        let entry_d = now_secs();
        exec_update(
            db,
            &txn,
            &format!(
                "INSERT INTO orders \
                 (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) \
                 VALUES ({o_id}, {d_id}, {w_id}, {c_id}, {entry_d}, 0, {ol_cnt}, 1)"
            ),
        )?;

        // 6. Insert new_order
        exec_update(
            db,
            &txn,
            &format!(
                "INSERT INTO new_order (no_o_id, no_d_id, no_w_id) \
                 VALUES ({o_id}, {d_id}, {w_id})"
            ),
        )?;

        // 7. Order lines
        let mut all_local = true;
        for ol_num in 1..=ol_cnt {
            let i_id = rng.uniform(1, cfg.items);
            let supply_w_id = if cfg.warehouses > 1 && rng.uniform(1, 100) == 1 {
                all_local = false;
                let mut remote = rng.uniform(1, cfg.warehouses);
                if remote == w_id {
                    remote = if w_id < cfg.warehouses {
                        w_id + 1
                    } else {
                        w_id - 1
                    };
                }
                remote
            } else {
                w_id
            };
            let quantity = rng.uniform(1, 10);

            // Item lookup
            let sql = format!("SELECT i_price, i_name, i_data FROM item WHERE i_id = {i_id}");
            let rows = run_query(db, &txn, &sql, &["i_price", "i_name", "i_data"])?;
            if rows.is_empty() {
                // TPC-C §2.4.2.2: 1% chance of invalid item causes rollback
                txn.write_session().rollback()?;
                return Ok(false);
            }
            let i_price = rows[0][0].as_float();

            // Stock lookup: grab the per-district info string + counters
            let dist_col = format!("s_dist_{:02}", d_id);
            let sql = format!(
                "SELECT s_quantity, {dist_col}, s_ytd, s_order_cnt, s_remote_cnt \
                 FROM stock WHERE s_i_id = {i_id} AND s_w_id = {supply_w_id}"
            );
            let rows = run_query(
                db,
                &txn,
                &sql,
                &[
                    "s_quantity",
                    &dist_col,
                    "s_ytd",
                    "s_order_cnt",
                    "s_remote_cnt",
                ],
            )?;
            let s_qty = rows[0][0].as_int();
            let ol_dist_info = rows[0][1].as_str().to_string();
            let s_ytd = rows[0][2].as_int();
            let s_order_cnt = rows[0][3].as_int();
            let s_remote_cnt = rows[0][4].as_int();

            // Update stock: adjust quantity, keep above 10
            let new_qty = if s_qty - quantity >= 10 {
                s_qty - quantity
            } else {
                s_qty - quantity + 91
            };
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE stock SET s_quantity = {new_qty} \
                     WHERE s_i_id = {i_id} AND s_w_id = {supply_w_id}"
                ),
            )?;
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE stock SET s_ytd = {} \
                     WHERE s_i_id = {i_id} AND s_w_id = {supply_w_id}",
                    s_ytd + quantity
                ),
            )?;
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE stock SET s_order_cnt = {} \
                     WHERE s_i_id = {i_id} AND s_w_id = {supply_w_id}",
                    s_order_cnt + 1
                ),
            )?;
            if !all_local {
                exec_update(
                    db,
                    &txn,
                    &format!(
                        "UPDATE stock SET s_remote_cnt = {} \
                         WHERE s_i_id = {i_id} AND s_w_id = {supply_w_id}",
                        s_remote_cnt + 1
                    ),
                )?;
            }

            let ol_amount = quantity as f64 * i_price * (1.0 + w_tax + d_tax) * (1.0 - c_discount);
            exec_update(
                db,
                &txn,
                &format!(
                    "INSERT INTO order_line \
                     (ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, \
                      ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) \
                     VALUES ({o_id}, {d_id}, {w_id}, {ol_num}, {i_id}, {supply_w_id}, \
                             0, {quantity}, {}, {})",
                    fmt_f(ol_amount),
                    sql_str(&ol_dist_info)
                ),
            )?;
        }

        // Correct o_all_local if any remote warehouse was used
        if !all_local {
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE orders SET o_all_local = 0 \
                     WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {o_id}"
                ),
            )?;
        }

        txn.write_session().commit()?;
        Ok(true)
    })();

    if outcome.is_err() {
        let _ = txn.write_session().rollback();
    }
    outcome
}

fn do_payment(db: &SimpleDB, rng: &mut Rng, cfg: &Config) -> TxnOutcome {
    let w_id = rng.uniform(1, cfg.warehouses);
    let d_id = rng.uniform(1, 10);
    let h_amount = rng.uniform_f64(1.0, 5000.0);

    // 60% lookup by c_id, 40% by c_last
    let by_id = rng.uniform(1, 10) <= 6;
    let c_id_input = if by_id {
        rng.uniform(1, cfg.customers_per_district)
    } else {
        0
    };
    let c_last_input = if !by_id {
        make_last_name(rng.uniform(0, 999) as usize)
    } else {
        String::new()
    };

    let txn = db.new_tx();
    let outcome = (|| -> Result<bool> {
        // 1. Get and update warehouse YTD
        let sql = format!(
            "SELECT w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_ytd \
             FROM warehouse WHERE w_id = {w_id}"
        );
        let rows = run_query(
            db,
            &txn,
            &sql,
            &[
                "w_name",
                "w_street_1",
                "w_street_2",
                "w_city",
                "w_state",
                "w_zip",
                "w_ytd",
            ],
        )?;
        let w_ytd = rows[0][6].as_float();
        let w_name = rows[0][0].as_str().to_string();
        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE warehouse SET w_ytd = {} WHERE w_id = {w_id}",
                fmt_f(w_ytd + h_amount)
            ),
        )?;

        // 2. Get and update district YTD
        let sql =
            format!("SELECT d_name, d_ytd FROM district WHERE d_w_id = {w_id} AND d_id = {d_id}");
        let rows = run_query(db, &txn, &sql, &["d_name", "d_ytd"])?;
        let d_ytd = rows[0][1].as_float();
        let d_name = rows[0][0].as_str().to_string();
        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE district SET d_ytd = {} WHERE d_w_id = {w_id} AND d_id = {d_id}",
                fmt_f(d_ytd + h_amount)
            ),
        )?;

        // 3. Resolve customer
        let c_id = if by_id {
            c_id_input
        } else {
            // ORDER BY c_first, pick middle row
            let sql = format!(
                "SELECT c_id FROM customer \
                 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = {} \
                 ORDER BY c_first",
                sql_str(&c_last_input)
            );
            let rows = run_query(db, &txn, &sql, &["c_id"])?;
            if rows.is_empty() {
                txn.write_session().rollback()?;
                return Ok(false);
            }
            let mid = rows.len() / 2;
            rows[mid][0].as_int()
        };

        let sql = format!(
            "SELECT c_balance, c_ytd_payment, c_payment_cnt, c_credit, c_data \
             FROM customer WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
        );
        let rows = run_query(
            db,
            &txn,
            &sql,
            &[
                "c_balance",
                "c_ytd_payment",
                "c_payment_cnt",
                "c_credit",
                "c_data",
            ],
        )?;
        let c_balance = rows[0][0].as_float();
        let c_ytd = rows[0][1].as_float();
        let c_pmt_cnt = rows[0][2].as_int();
        let c_credit = rows[0][3].as_str().to_string();
        let c_data_old = rows[0][4].as_str().to_string();

        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE customer SET c_balance = {} WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                fmt_f(c_balance - h_amount)
            ),
        )?;
        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE customer SET c_ytd_payment = {} WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                fmt_f(c_ytd + h_amount)
            ),
        )?;
        exec_update(
            db,
            &txn,
            &format!(
                "UPDATE customer SET c_payment_cnt = {} WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                c_pmt_cnt + 1
            ),
        )?;

        if c_credit == "BC" {
            // Bad credit: prepend new info to c_data, keep at most 500 chars
            let new_info = format!("{c_id} {d_id} {w_id} {d_id} {w_id} {:.2} ", h_amount);
            let mut new_data = new_info + &c_data_old;
            new_data.truncate(500);
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE customer SET c_data = {} WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                    sql_str(&new_data)
                ),
            )?;
        }

        // 4. Insert history
        let h_data = format!(
            "{:<10}{:<10}",
            &w_name[..w_name.len().min(10)],
            &d_name[..d_name.len().min(10)]
        );
        exec_update(
            db,
            &txn,
            &format!(
                "INSERT INTO history \
                 (h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) \
                 VALUES ({c_id}, {d_id}, {w_id}, {d_id}, {w_id}, {}, {}, {})",
                now_secs(),
                fmt_f(h_amount),
                sql_str(&h_data)
            ),
        )?;

        txn.write_session().commit()?;
        Ok(true)
    })();

    if outcome.is_err() {
        let _ = txn.write_session().rollback();
    }
    outcome
}

fn do_order_status(db: &SimpleDB, rng: &mut Rng, cfg: &Config) -> TxnOutcome {
    let w_id = rng.uniform(1, cfg.warehouses);
    let d_id = rng.uniform(1, 10);
    let by_id = rng.uniform(1, 10) <= 6;
    let c_id_input = if by_id {
        rng.uniform(1, cfg.customers_per_district)
    } else {
        0
    };
    let c_last_input = if !by_id {
        make_last_name(rng.uniform(0, 999) as usize)
    } else {
        String::new()
    };

    let txn = db.new_tx();
    let outcome = (|| -> Result<bool> {
        // Resolve customer
        let c_id = if by_id {
            c_id_input
        } else {
            let sql = format!(
                "SELECT c_id FROM customer \
                 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_last = {} \
                 ORDER BY c_first",
                sql_str(&c_last_input)
            );
            let rows = run_query(db, &txn, &sql, &["c_id"])?;
            if rows.is_empty() {
                txn.write_session().rollback()?;
                return Ok(false);
            }
            let mid = rows.len() / 2;
            rows[mid][0].as_int()
        };

        // Get customer balance
        let sql = format!(
            "SELECT c_balance, c_first, c_middle, c_last FROM customer \
             WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
        );
        let _rows = run_query(
            db,
            &txn,
            &sql,
            &["c_balance", "c_first", "c_middle", "c_last"],
        )?;

        // Get most recent order using MAX aggregate
        let sql = format!(
            "SELECT max(o_id) FROM orders WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_c_id = {c_id}"
        );
        let rows = run_query(db, &txn, &sql, &["max_o_id"])?;
        if rows.is_empty() {
            txn.write_session().commit()?;
            return Ok(true);
        }
        let o_id = rows[0][0].as_int();

        // Get order details
        let sql = format!(
            "SELECT o_entry_d, o_carrier_id, o_ol_cnt FROM orders \
             WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {o_id}"
        );
        let _rows = run_query(db, &txn, &sql, &["o_entry_d", "o_carrier_id", "o_ol_cnt"])?;

        // Get order lines
        let sql = format!(
            "SELECT ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d \
             FROM order_line \
             WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {o_id} \
             ORDER BY ol_number"
        );
        let _rows = run_query(
            db,
            &txn,
            &sql,
            &[
                "ol_i_id",
                "ol_supply_w_id",
                "ol_quantity",
                "ol_amount",
                "ol_delivery_d",
            ],
        )?;

        txn.write_session().commit()?;
        Ok(true)
    })();

    if outcome.is_err() {
        let _ = txn.write_session().rollback();
    }
    outcome
}

fn do_delivery(db: &SimpleDB, rng: &mut Rng, cfg: &Config) -> TxnOutcome {
    let w_id = rng.uniform(1, cfg.warehouses);
    let carrier_id = rng.uniform(1, 10);
    let delivery_d = now_secs();

    let txn = db.new_tx();
    let outcome = (|| -> Result<bool> {
        for d_id in 1..=10 {
            // Find the oldest undelivered order
            let sql = format!(
                "SELECT min(no_o_id) FROM new_order WHERE no_w_id = {w_id} AND no_d_id = {d_id}"
            );
            let rows = run_query(db, &txn, &sql, &["min_no_o_id"])?;
            if rows.is_empty() {
                continue;
            }
            let o_id = rows[0][0].as_int();
            if o_id == 0 {
                // MIN on empty set returns 0 (default int); skip
                continue;
            }

            // Delete from new_order
            exec_update(
                db,
                &txn,
                &format!(
                    "DELETE FROM new_order \
                     WHERE no_w_id = {w_id} AND no_d_id = {d_id} AND no_o_id = {o_id}"
                ),
            )?;

            // Get order customer and line count
            let sql = format!(
                "SELECT o_c_id, o_ol_cnt FROM orders \
                 WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {o_id}"
            );
            let rows = run_query(db, &txn, &sql, &["o_c_id", "o_ol_cnt"])?;
            if rows.is_empty() {
                continue;
            }
            let c_id = rows[0][0].as_int();

            // Assign carrier
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE orders SET o_carrier_id = {carrier_id} \
                     WHERE o_w_id = {w_id} AND o_d_id = {d_id} AND o_id = {o_id}"
                ),
            )?;

            // Mark all order lines delivered
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE order_line SET ol_delivery_d = {delivery_d} \
                     WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {o_id}"
                ),
            )?;

            // Sum order line amounts
            let sql = format!(
                "SELECT sum(ol_amount) FROM order_line \
                 WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} AND ol_o_id = {o_id}"
            );
            let rows = run_query(db, &txn, &sql, &["sum_ol_amount"])?;
            let ol_total = if rows.is_empty() {
                0.0f64
            } else {
                rows[0][0].as_float()
            };

            // Update customer balance and delivery count
            let sql = format!(
                "SELECT c_balance, c_delivery_cnt FROM customer \
                 WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}"
            );
            let rows = run_query(db, &txn, &sql, &["c_balance", "c_delivery_cnt"])?;
            let c_balance = rows[0][0].as_float();
            let c_del_cnt = rows[0][1].as_int();

            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE customer SET c_balance = {} \
                     WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                    fmt_f(c_balance + ol_total)
                ),
            )?;
            exec_update(
                db,
                &txn,
                &format!(
                    "UPDATE customer SET c_delivery_cnt = {} \
                     WHERE c_w_id = {w_id} AND c_d_id = {d_id} AND c_id = {c_id}",
                    c_del_cnt + 1
                ),
            )?;
        }

        txn.write_session().commit()?;
        Ok(true)
    })();

    if outcome.is_err() {
        let _ = txn.write_session().rollback();
    }
    outcome
}

fn do_stock_level(db: &SimpleDB, rng: &mut Rng, cfg: &Config) -> TxnOutcome {
    let w_id = rng.uniform(1, cfg.warehouses);
    let d_id = rng.uniform(1, 10);
    let threshold = rng.uniform(10, 20);

    let txn = db.new_tx();
    let outcome = (|| -> Result<bool> {
        // Get next order ID
        let sql =
            format!("SELECT d_next_o_id FROM district WHERE d_w_id = {w_id} AND d_id = {d_id}");
        let rows = run_query(db, &txn, &sql, &["d_next_o_id"])?;
        let next_o_id = rows[0][0].as_int();
        let low_o_id = next_o_id - 20;

        // Collect distinct item IDs from the last 20 orders
        let sql = format!(
            "SELECT ol_i_id FROM order_line \
             WHERE ol_w_id = {w_id} AND ol_d_id = {d_id} \
               AND ol_o_id >= {low_o_id} AND ol_o_id < {next_o_id}"
        );
        let rows = run_query(db, &txn, &sql, &["ol_i_id"])?;
        let item_ids: HashSet<i32> = rows.iter().map(|r| r[0].as_int()).collect();

        // Count items below threshold (driver-side: no subquery support)
        let mut low_stock_count = 0usize;
        for i_id in item_ids {
            let sql =
                format!("SELECT s_quantity FROM stock WHERE s_i_id = {i_id} AND s_w_id = {w_id}");
            let rows = run_query(db, &txn, &sql, &["s_quantity"])?;
            if !rows.is_empty() && rows[0][0].as_int() < threshold {
                low_stock_count += 1;
            }
        }

        let _ = low_stock_count; // result available for reporting

        txn.write_session().commit()?;
        Ok(true)
    })();

    if outcome.is_err() {
        let _ = txn.write_session().rollback();
    }
    outcome
}

// ---------------------------------------------------------------------------
// Driver loop
// ---------------------------------------------------------------------------

struct Stats {
    attempts: [u64; 5],
    commits: [u64; 5],
    rollbacks: [u64; 5],
    retries: [u64; 5],
    errors: [u64; 5],
}

const TXN_NAMES: [&str; 5] = [
    "NewOrder",
    "Payment",
    "OrderStatus",
    "Delivery",
    "StockLevel",
];

impl Stats {
    fn new() -> Self {
        Self {
            attempts: [0; 5],
            commits: [0; 5],
            rollbacks: [0; 5],
            retries: [0; 5],
            errors: [0; 5],
        }
    }

    fn report(&self, elapsed: Duration) {
        let secs = elapsed.as_secs_f64();
        let new_orders = self.commits[0];
        let tpmc = new_orders as f64 / secs * 60.0;

        println!("\n=== TPC-C Results ===");
        println!("Duration:  {:.1}s", secs);
        println!("tpmC:      {:.1}  ({} NewOrder commits)", tpmc, new_orders);
        println!();
        println!(
            "{:<14} {:>8} {:>8} {:>8} {:>8} {:>8}",
            "Transaction", "Attempts", "Commits", "Rollbacks", "Retries", "Errors"
        );
        println!("{}", "-".repeat(62));
        for (i, name) in TXN_NAMES.iter().enumerate() {
            println!(
                "{:<14} {:>8} {:>8} {:>8} {:>8} {:>8}",
                name,
                self.attempts[i],
                self.commits[i],
                self.rollbacks[i],
                self.retries[i],
                self.errors[i]
            );
        }
    }
}

fn run_driver(db: &SimpleDB, cfg: &Config) {
    let rng = &mut Rng::new();
    let mut stats = Stats::new();
    // Mix weights: 45/43/4/4/4
    let weights = [45u32, 43, 4, 4, 4];
    let total_weight: u32 = weights.iter().sum();

    let start = Instant::now();
    const MAX_RETRIES: usize = 3;

    eprintln!("Running benchmark for {}s...", cfg.duration.as_secs());

    while start.elapsed() < cfg.duration {
        // Select transaction type by weight
        let pick = rng.uniform(0, (total_weight - 1) as i32) as u32;
        let mut txn_type = 4usize;
        let mut acc = 0u32;
        for (i, &w) in weights.iter().enumerate() {
            acc += w;
            if pick < acc {
                txn_type = i;
                break;
            }
        }

        stats.attempts[txn_type] += 1;

        let mut last_err: Option<Box<dyn Error>> = None;
        let mut committed = false;
        let mut rolled_back = false;

        for _attempt in 0..=MAX_RETRIES {
            let result = match txn_type {
                0 => do_new_order(db, rng, cfg),
                1 => do_payment(db, rng, cfg),
                2 => do_order_status(db, rng, cfg),
                3 => do_delivery(db, rng, cfg),
                _ => do_stock_level(db, rng, cfg),
            };

            match result {
                Ok(true) => {
                    committed = true;
                    break;
                }
                Ok(false) => {
                    rolled_back = true;
                    break;
                }
                Err(e) if is_lock_error(&e) => {
                    stats.retries[txn_type] += 1;
                    last_err = Some(e);
                    // brief pause before retry to let conflicting transaction finish
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(e) => {
                    last_err = Some(e);
                    break;
                }
            }
        }

        if committed {
            stats.commits[txn_type] += 1;
        } else if rolled_back {
            stats.rollbacks[txn_type] += 1;
        } else {
            stats.errors[txn_type] += 1;
            if let Some(e) = last_err {
                eprintln!("ERROR in {}: {e}", TXN_NAMES[txn_type]);
            }
        }
    }

    stats.report(start.elapsed());
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

fn main() {
    let cfg = parse_args();

    if cfg.skip_load && cfg.db_path.is_none() {
        eprintln!("error: --skip-load requires --db <path>");
        std::process::exit(1);
    }

    let (db_path, _tmp_dir) = match &cfg.db_path {
        Some(p) => (p.clone(), None),
        None => {
            let ts = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let path = format!("/tmp/tpcc_db_{ts}");
            (path, Some(()))
        }
    };

    let clean = !cfg.skip_load;
    eprintln!(
        "Database path: {db_path}  (clean={clean}, buffers={}, warehouses={}, items={}, cpd={})",
        cfg.buffers, cfg.warehouses, cfg.items, cfg.customers_per_district
    );

    let db = SimpleDB::new(&db_path, cfg.buffers, clean, cfg.lock_timeout_ms);

    if !cfg.skip_load {
        let load_start = Instant::now();
        if let Err(e) = load_schema(&db) {
            eprintln!("Schema creation failed: {e}");
            std::process::exit(1);
        }
        if let Err(e) = load_data(&db, &cfg) {
            eprintln!("Data load failed: {e}");
            std::process::exit(1);
        }
        eprintln!("Load took {:.1}s", load_start.elapsed().as_secs_f64());
    }

    run_driver(&db, &cfg);

    // Clean up temp directory if we created one
    if cfg.db_path.is_none() {
        let _ = std::fs::remove_dir_all(&db_path);
    }
}
