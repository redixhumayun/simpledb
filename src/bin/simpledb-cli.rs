#![allow(clippy::arc_with_non_send_sync)]

use simpledb::{BTreeIndex, Constant, FieldType, SimpleDB, Transaction};
use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    println!("SimpleDB CLI v0.1.0");
    println!("Type 'help' for commands, 'quit' to exit");
    println!();

    // Initialize database
    let db = SimpleDB::new("./simpledb-data", 8, false, 100);

    // Main REPL loop
    loop {
        // Print prompt
        print!("simpledb> ");
        io::stdout().flush()?;

        // Read user input
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        // Handle special commands
        match input {
            "quit" | "exit" => {
                println!("Goodbye!");
                break;
            }
            "help" => {
                show_help();
                continue;
            }
            "show database" | "SHOW DATABASE" => {
                show_database_info(&db);
                continue;
            }
            "show tables" | "SHOW TABLES" => {
                match show_tables(&db) {
                    Ok(result) => println!("{result}"),
                    Err(e) => println!("Error: {e}"),
                }
                continue;
            }
            "show buffers" | "SHOW BUFFERS" => {
                show_buffers(&db);
                continue;
            }
            "recover" | "RECOVER" => {
                match recover_database(&db) {
                    Ok(result) => println!("{result}"),
                    Err(e) => println!("Error: {e}"),
                }
                continue;
            }
            "" => continue, // Empty input
            _ => {
                // Check for DESCRIBE <table> command
                let input_lower = input.to_lowercase();
                if input_lower.starts_with("describe ") || input_lower.starts_with("desc ") {
                    let table_name = if input_lower.starts_with("describe ") {
                        input[9..].trim()
                    } else {
                        input[5..].trim()
                    };
                    match describe_table(&db, table_name) {
                        Ok(result) => println!("{result}"),
                        Err(e) => println!("Error: {e}"),
                    }
                    continue;
                }
            }
        }

        // Execute SQL command
        match execute_sql(&db, input) {
            Ok(result) => println!("{result}"),
            Err(e) => println!("Error: {e}"),
        }
    }

    Ok(())
}

fn show_help() {
    println!("SimpleDB CLI Commands:");
    println!("  help                - Show this help message");
    println!("  quit/exit           - Exit the CLI");
    println!("  SHOW DATABASE       - Display database information");
    println!("  SHOW TABLES         - List all tables");
    println!("  SHOW BUFFERS        - Display buffer pool statistics");
    println!("  DESCRIBE <table>    - Show table schema and statistics");
    println!("  RECOVER             - Recover database from log");
    println!();
    println!("Supported SQL:");
    println!("  CREATE TABLE table_name(field_name type, ...)");
    println!("  INSERT INTO table_name(field1, field2) VALUES (val1, val2)");
    println!("  SELECT field1, field2 FROM table_name WHERE condition");
    println!("  UPDATE table_name SET field=value WHERE condition");
    println!("  DELETE FROM table_name WHERE condition");
    println!();
    println!("Example:");
    println!("  CREATE TABLE students(id int, name varchar(50))");
    println!("  INSERT INTO students(id, name) VALUES (1, 'Alice')");
    println!("  SELECT * FROM students");
    println!("  DESCRIBE students");
}

fn show_database_info(db: &SimpleDB) {
    println!("Database Information:");
    println!("  Directory: {}", db.db_directory().display());
    println!("  File Manager: Active");
    println!("  Buffer Manager: Active");
    println!("  Log Manager: Active");
}

fn show_tables(db: &SimpleDB) -> Result<String, Box<dyn Error>> {
    let txn = db.new_tx();

    let tables = match db.metadata_manager().get_table_names(&txn) {
        Ok(t) => t,
        Err(e) => {
            txn.rollback()?;
            return Err(e);
        }
    };

    txn.commit()?;

    if tables.is_empty() {
        Ok("No tables found.".to_string())
    } else {
        let mut result = String::from("Tables:\n");
        for table in tables {
            result.push_str(&format!("  - {}\n", table));
        }
        Ok(result)
    }
}

fn show_buffers(db: &SimpleDB) {
    let available = db.buffer_manager().available();
    println!("Buffer Pool Information:");
    println!("  Available buffers: {}", available);
}

fn recover_database(db: &SimpleDB) -> Result<String, Box<dyn Error>> {
    let txn = db.new_tx();

    match txn.recover() {
        Ok(_) => {
            txn.commit()?;
            Ok("Database recovery completed successfully.".to_string())
        }
        Err(e) => {
            txn.rollback()?;
            Err(e)
        }
    }
}

fn describe_table(db: &SimpleDB, table_name: &str) -> Result<String, Box<dyn Error>> {
    let txn = db.new_tx();
    let layout = db
        .metadata_manager()
        .get_layout(table_name, Arc::clone(&txn));
    let stat_info =
        db.metadata_manager()
            .get_stat_info(table_name, layout.clone(), Arc::clone(&txn));
    let indexes = db
        .metadata_manager()
        .get_index_info(table_name, Arc::clone(&txn));

    // Capture block_size before committing to avoid creating new transactions in the index loop
    let block_size = txn.block_size();

    txn.commit()?;

    let mut result = format!("Table: {}\n", table_name);
    result.push_str(&format!("Slot Size: {} bytes\n", layout.slot_size));
    result.push_str(&format!(
        "Statistics: {} blocks, {} records\n",
        stat_info.num_blocks, stat_info.num_records
    ));
    result.push_str("\nFields:\n");
    result.push_str(&format!("{:<20} {:<15}\n", "Name", "Type"));
    result.push_str(&format!("{}\n", "-".repeat(35)));

    for field in &layout.schema.fields {
        let field_info = &layout.schema.info[field];
        let type_str = match field_info.field_type {
            FieldType::Int => "int".to_string(),
            FieldType::String => format!("varchar({})", field_info.length),
        };
        result.push_str(&format!("{:<20} {:<15}\n", field, type_str));
    }

    // Show index information
    if !indexes.is_empty() {
        result.push_str("\nIndexes:\n");
        for (field_name, index_info) in indexes {
            // Use the accessor methods to get comprehensive index information
            let idx_schema = index_info.table_schema();
            let idx_stats = index_info.stat_info();
            let blocks = index_info.blocks_accessed();
            let records = index_info.records_output();
            let distinct = index_info.distinct_values(&field_name);

            // Calculate BTree search cost for comparison
            let records_per_block = if layout.slot_size > 0 {
                block_size / layout.slot_size
            } else {
                1
            };
            let btree_cost = BTreeIndex::search_cost(blocks, records_per_block);

            result.push_str(&format!(
                "  - {}: {} fields, {} records, {} blocks, {} output records, {} distinct values, BTree cost: {}\n",
                field_name,
                idx_schema.fields.len(),
                idx_stats.num_records,
                blocks,
                records,
                distinct,
                btree_cost
            ));
        }
    }

    Ok(result)
}

fn execute_sql(db: &SimpleDB, sql: &str) -> Result<String, Box<dyn Error>> {
    let txn = db.new_tx();

    // Determine if this is a query or update command
    let sql_lower = sql.to_lowercase();

    let result = if sql_lower.trim_start().starts_with("select") {
        // Handle SELECT queries
        execute_query(db, sql, Arc::clone(&txn))
    } else {
        // Handle UPDATE commands (CREATE, INSERT, UPDATE, DELETE)
        execute_update(db, sql, Arc::clone(&txn))
    };

    //  commit the txn or rollback on error
    match result {
        Ok(_) => {
            txn.commit()?;
            result
        }
        Err(e) => {
            // Rollback on error
            txn.rollback()?;
            Err(e)
        }
    }
}

fn execute_query(
    db: &SimpleDB,
    sql: &str,
    txn: Arc<Transaction>,
) -> Result<String, Box<dyn Error>> {
    let plan = db.planner.create_query_plan(sql.to_string(), txn)?;
    let mut scan = plan.open();

    let mut result = String::new();
    let mut row_count = 0;

    // Get the schema to know what fields to display
    let schema = plan.schema();
    let fields: Vec<String> = schema.fields.to_vec();

    // Print header
    if !fields.is_empty() {
        result.push_str(&format!("{}\n", fields.join(" | ")));
        result.push_str(&format!(
            "{}\n",
            fields
                .iter()
                .map(|f| "-".repeat(f.len()))
                .collect::<Vec<_>>()
                .join("-|-")
        ));
    }

    // Print rows
    while let Some(scan_result) = scan.next() {
        scan_result?; // Handle scan errors

        let mut row_values = Vec::new();
        for field in &fields {
            let value = scan.get_value(field)?;
            row_values.push(format_value(&value));
        }

        if !row_values.is_empty() {
            result.push_str(&format!("{}\n", row_values.join(" | ")));
        }
        row_count += 1;
    }

    if row_count == 0 {
        result.push_str("No results found.\n");
    } else {
        result.push_str(&format!("\n{row_count} row(s) returned.\n"));
    }

    Ok(result)
}

fn execute_update(
    db: &SimpleDB,
    sql: &str,
    txn: Arc<Transaction>,
) -> Result<String, Box<dyn Error>> {
    let affected_rows = db.planner.execute_update(sql.to_string(), txn)?;
    Ok(format!("{affected_rows} row(s) affected."))
}

fn format_value(value: &Constant) -> String {
    match value {
        Constant::Int(i) => i.to_string(),
        Constant::String(s) => s.clone(),
    }
}
