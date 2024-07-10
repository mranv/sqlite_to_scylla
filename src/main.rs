use rusqlite::{params, Connection, Result};
use scylla::{
    Session, SessionBuilder, IntoTypedRows, QueryResult, SessionPager, QueryPager, FromRow,
    error::QueryError,
};
use std::error::Error;
use futures_util::stream::StreamExt;
use tokio::stream::Stream;

// Struct to hold table schema information
#[derive(Debug)]
struct TableSchema {
    table_name: String,
    columns: Vec<(String, String)>,
}

// Function to retrieve schema information from SQLite
fn fetch_sqlite_schema(db_path: &str) -> Result<Vec<TableSchema>> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let tables_iter = stmt.query_map([], |row| {
        Ok(row.get(0)?)
    })?;

    let mut tables = Vec::new();
    for table_name in tables_iter {
        let table_name: String = table_name?;
        let mut stmt_columns = conn.prepare(&format!("PRAGMA table_info('{}')", table_name))?;
        let columns_iter = stmt_columns.query_map([], |row| {
            Ok((row.get(1)?, row.get(2)?))
        })?;

        let mut columns = Vec::new();
        for column in columns_iter {
            columns.push(column?);
        }

        tables.push(TableSchema {
            table_name,
            columns,
        });
    }
    Ok(tables)
}

// Function to create tables in ScyllaDB based on SQLite schema
async fn create_scylla_tables(session: &Session, schema: &[TableSchema]) -> Result<(), QueryError> {
    for table in schema {
        let mut create_query = format!("CREATE TABLE IF NOT EXISTS {} (", table.table_name);
        for (i, (column_name, column_type)) in table.columns.iter().enumerate() {
            if i > 0 {
                create_query.push_str(", ");
            }
            create_query.push_str(&format!("{} {}", column_name, column_type));
        }
        create_query.push_str(")");

        session.query(create_query, ()).await?;
        println!("Created table {} in ScyllaDB", table.table_name);
    }
    Ok(())
}

// Function to migrate data from SQLite to ScyllaDB
async fn migrate_data(session: &Session, schema: &[TableSchema], db_path: &str) -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(db_path)?;
    for table in schema {
        let query = format!("SELECT * FROM {}", table.table_name);
        let mut stmt = conn.prepare(&query)?;
        let rows = stmt.query_map([], |row| {
            let mut values: Vec<String> = Vec::new();
            for i in 0..row.column_count() {
                values.push(row.get::<usize, String>(i)?);
            }
            Ok(values)
        })?;

        let mut batch = session.start_batch();
        for row in rows {
            let row = row?;
            let mut insert_query = format!("INSERT INTO {} (", table.table_name);
            for (i, (column_name, _)) in table.columns.iter().enumerate() {
                if i > 0 {
                    insert_query.push_str(", ");
                }
                insert_query.push_str(column_name);
            }
            insert_query.push_str(") VALUES (");
            for (i, value) in row.iter().enumerate() {
                if i > 0 {
                    insert_query.push_str(", ");
                }
                insert_query.push_str(value);
            }
            insert_query.push_str(")");
            batch.query(insert_query, ()).await?;
        }
        batch.execute().await?;
        println!("Data migrated for table {} to ScyllaDB", table.table_name);
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Step 1: SQLite database path
    let sqlite_db_path = "/home/mranv/Downloads/chinook.db";
    
    // Step 2: Connect to SQLite and fetch schema
    let schema = fetch_sqlite_schema(sqlite_db_path)?;

    // Step 3: Connect to ScyllaDB
    let scylla_node = "127.0.0.1:9042";
    let session = SessionBuilder::new()
        .known_node(scylla_node)
        .build()
        .await?;

    // Step 4: Create tables in ScyllaDB
    create_scylla_tables(&session, &schema).await?;

    // Step 5: Migrate data from SQLite to ScyllaDB
    migrate_data(&session, &schema, sqlite_db_path).await?;

    println!("Migration completed successfully");

    Ok(())
}
