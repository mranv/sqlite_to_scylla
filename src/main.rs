use rusqlite::{params, Connection, Result};
use serde::{Deserialize, Serialize};
use scylla::{Session, SessionBuilder};
use tokio;

#[derive(Debug, Serialize, Deserialize)]
struct SampleData {
    id: i32,
    name: String,
    value: String,
}

fn read_sqlite_data(db_path: &str) -> Result<Vec<SampleData>> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare("SELECT id, name, value FROM sample_table")?;
    let sample_iter = stmt.query_map([], |row| {
        Ok(SampleData {
            id: row.get(0)?,
            name: row.get(1)?,
            value: row.get(2)?,
        })
    })?;

    let mut samples = Vec::new();
    for sample in sample_iter {
        samples.push(sample?);
    }
    Ok(samples)
}

async fn connect_to_scylla(node: &str) -> Result<Session, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(node)
        .build()
        .await?;
    Ok(session)
}

async fn insert_into_scylla(session: &Session, data: &[SampleData]) -> Result<(), Box<dyn std::error::Error>> {
    for record in data {
        session
            .query(
                "INSERT INTO keyspace_name.table_name (id, name, value) VALUES (?, ?, ?)",
                (record.id, &record.name, &record.value),
            )
            .await?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Read data from SQLite
    let sqlite_db_path = "/home/mranv/Downloads/chinook.db";
    let data = read_sqlite_data(sqlite_db_path)?;
    println!("Read {} records from SQLite", data.len());

    // Step 2: Connect to ScyllaDB
    let scylla_node = "127.0.0.1:9042";
    let session = connect_to_scylla(scylla_node).await?;
    println!("Connected to ScyllaDB");

    // Step 3: Insert data into ScyllaDB
    insert_into_scylla(&session, &data).await?;
    println!("Data migrated to ScyllaDB successfully");

    Ok(())
}
