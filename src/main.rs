use rusqlite::{Connection, Result};
use serde::{Deserialize, Serialize};
use scylla::{Session, SessionBuilder};
use tokio;

#[derive(Debug, Serialize, Deserialize)]
struct Album {
    album_id: i32,
    title: String,
    artist_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Artist {
    artist_id: i32,
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Customer {
    customer_id: i32,
    first_name: String,
    last_name: String,
    company: Option<String>,
    address: Option<String>,
    city: Option<String>,
    state: Option<String>,
    country: Option<String>,
    postal_code: Option<String>,
    phone: Option<String>,
    fax: Option<String>,
    email: String,
    support_rep_id: Option<i32>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Employee {
    employee_id: i32,
    last_name: String,
    first_name: String,
    title: Option<String>,
    reports_to: Option<i32>,
    birth_date: Option<String>,
    hire_date: Option<String>,
    address: Option<String>,
    city: Option<String>,
    state: Option<String>,
    country: Option<String>,
    postal_code: Option<String>,
    phone: Option<String>,
    fax: Option<String>,
    email: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Genre {
    genre_id: i32,
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Invoice {
    invoice_id: i32,
    customer_id: i32,
    invoice_date: String,
    billing_address: Option<String>,
    billing_city: Option<String>,
    billing_state: Option<String>,
    billing_country: Option<String>,
    billing_postal_code: Option<String>,
    total: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct InvoiceItem {
    invoice_line_id: i32,
    invoice_id: i32,
    track_id: i32,
    unit_price: f64,
    quantity: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct MediaType {
    media_type_id: i32,
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct Playlist {
    playlist_id: i32,
    name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct PlaylistTrack {
    playlist_id: i32,
    track_id: i32,
}

#[derive(Debug, Serialize, Deserialize)]
struct Track {
    track_id: i32,
    name: String,
    album_id: Option<i32>,
    media_type_id: i32,
    genre_id: Option<i32>,
    composer: Option<String>,
    milliseconds: i32,
    bytes: Option<i32>,
    unit_price: f64,
}

fn read_sqlite_data<T>(db_path: &str, query: &str) -> Result<Vec<T>>
where
    T: for<'de> Deserialize<'de>,
{
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare(query)?;
    let rows = stmt.query_map([], |row| {
        let json: String = row.get(0)?;
        serde_json::from_str(&json).map_err(|e| rusqlite::Error::UserFunctionError(Box::new(e)))
    })?;

    let mut records = Vec::new();
    for record in rows {
        records.push(record?);
    }
    Ok(records)
}

async fn connect_to_scylla(node: &str) -> Result<Session, Box<dyn std::error::Error>> {
    let session = SessionBuilder::new()
        .known_node(node)
        .build()
        .await?;
    Ok(session)
}

async fn create_tables(session: &Session) -> Result<(), Box<dyn std::error::Error>> {
    let create_keyspace = "CREATE KEYSPACE IF NOT EXISTS music WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1};";
    session.query(create_keyspace, &[]).await?;

    let create_statements = vec![
        "CREATE TABLE IF NOT EXISTS music.albums (album_id int PRIMARY KEY, title text, artist_id int);",
        "CREATE TABLE IF NOT EXISTS music.artists (artist_id int PRIMARY KEY, name text);",
        "CREATE TABLE IF NOT EXISTS music.customers (customer_id int PRIMARY KEY, first_name text, last_name text, company text, address text, city text, state text, country text, postal_code text, phone text, fax text, email text, support_rep_id int);",
        "CREATE TABLE IF NOT EXISTS music.employees (employee_id int PRIMARY KEY, last_name text, first_name text, title text, reports_to int, birth_date timestamp, hire_date timestamp, address text, city text, state text, country text, postal_code text, phone text, fax text, email text);",
        "CREATE TABLE IF NOT EXISTS music.genres (genre_id int PRIMARY KEY, name text);",
        "CREATE TABLE IF NOT EXISTS music.invoices (invoice_id int PRIMARY KEY, customer_id int, invoice_date timestamp, billing_address text, billing_city text, billing_state text, billing_country text, billing_postal_code text, total decimal);",
        "CREATE TABLE IF NOT EXISTS music.invoice_items (invoice_line_id int PRIMARY KEY, invoice_id int, track_id int, unit_price decimal, quantity int);",
        "CREATE TABLE IF NOT EXISTS music.media_types (media_type_id int PRIMARY KEY, name text);",
        "CREATE TABLE IF NOT EXISTS music.playlists (playlist_id int PRIMARY KEY, name text);",
        "CREATE TABLE IF NOT EXISTS music.playlist_track (playlist_id int, track_id int, PRIMARY KEY (playlist_id, track_id));",
        "CREATE TABLE IF NOT EXISTS music.tracks (track_id int PRIMARY KEY, name text, album_id int, media_type_id int, genre_id int, composer text, milliseconds int, bytes int, unit_price decimal);",
    ];

    for statement in create_statements {
        session.query(statement, &[]).await?;
    }

    Ok(())
}

async fn insert_album_into_scylla(
    session: &Session,
    album: &Album,
    insert_query: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    session.query(
        insert_query,
        (
            album.album_id,
            album.title.clone(),
            album.artist_id,
        ),
    ).await?;
    Ok(())
}

async fn insert_artist_into_scylla(
    session: &Session,
    artist: &Artist,
    insert_query: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    session.query(
        insert_query,
        (
            artist.artist_id,
            artist.name.clone(),
        ),
    ).await?;
    Ok(())
}

// Implement similar functions for other tables like Customer, Employee, Genre, etc.

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let sqlite_db_path = "path/to/your/sqlite.db";

    let albums: Vec<Album> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('album_id', AlbumId, 'title', Title, 'artist_id', ArtistId) FROM albums",
    )?;
    let artists: Vec<Artist> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('artist_id', ArtistId, 'name', Name) FROM artists",
    )?;
    // Repeat for other tables like customers, employees, genres, etc.

    let scylla_node = "127.0.0.1:9042";
    let session = connect_to_scylla(scylla_node).await?;
    println!("Connected to ScyllaDB");

    create_tables(&session).await?;
    println!("ScyllaDB tables created");

    for album in &albums {
        insert_album_into_scylla(
            &session,
            album,
            "INSERT INTO music.albums (album_id, title, artist_id) VALUES (?, ?, ?)",
        ).await?;
    }

    for artist in &artists {
        insert_artist_into_scylla(
            &session,
            artist,
            "INSERT INTO music.artists (artist_id, name) VALUES (?, ?)",
        ).await?;
    }

    // Repeat for other tables

    println!("Data migrated to ScyllaDB successfully");

    Ok(())
}
