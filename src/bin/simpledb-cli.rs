#![allow(clippy::arc_with_non_send_sync)]

use simpledb::{Constant, SimpleDB, Transaction};
use std::error::Error;
use std::io::{self, Write};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn Error>> {
    println!("SimpleDB CLI v0.1.0");
    println!("Type 'help' for commands, 'quit' to exit");
    println!();

    // Initialize database
    let db = SimpleDB::new("./simpledb-data", 1024, 8, false, 100);

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
            "" => continue, // Empty input
            _ => {}
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
}

fn show_database_info(db: &SimpleDB) {
    println!("Database Information:");
    println!("  Directory: {}", db.db_directory().display());
    println!("  File Manager: Active");
    println!("  Buffer Manager: Active");
    println!("  Log Manager: Active");
}

fn show_tables(db: &SimpleDB) -> Result<String, Box<dyn Error>> {
    let txn = Arc::new(db.new_tx());
    let tables = db.metadata_manager().get_table_names(&txn)?;
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

fn execute_sql(db: &SimpleDB, sql: &str) -> Result<String, Box<dyn Error>> {
    let txn = Arc::new(db.new_tx());

    // Determine if this is a query or update command
    let sql_lower = sql.to_lowercase();

    let result = if sql_lower.trim_start().starts_with("select") {
        // Handle SELECT queries
        execute_query(db, sql, Arc::clone(&txn))
    } else {
        // Handle UPDATE commands (CREATE, INSERT, UPDATE, DELETE)
        execute_update(db, sql, Arc::clone(&txn))
    };

    //  commit the txn
    match result {
        Ok(_) => {
            txn.commit()?;
            result
        }
        Err(e) => Err(e),
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
