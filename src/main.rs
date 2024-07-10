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

async fn insert_into_scylla<T>(
    session: &Session,
    data: &[T],
    insert_query: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    T: Serialize,
{
    for record in data {
        let json = serde_json::to_value(record)?;
        session.query(insert_query, &json).await?;
    }
    Ok(())
}

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
    let customers: Vec<Customer> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('customer_id', CustomerId, 'first_name', FirstName, 'last_name', LastName, 'company', Company, 'address', Address, 'city', City, 'state', State, 'country', Country, 'postal_code', PostalCode, 'phone', Phone, 'fax', Fax, 'email', Email, 'support_rep_id', SupportRepId) FROM customers",
    )?;
    let employees: Vec<Employee> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('employee_id', EmployeeId, 'last_name', LastName, 'first_name', FirstName, 'title', Title, 'reports_to', ReportsTo, 'birth_date', BirthDate, 'hire_date', HireDate, 'address', Address, 'city', City, 'state', State, 'country', Country, 'postal_code', PostalCode, 'phone', Phone, 'fax', Fax, 'email', Email) FROM employees",
    )?;
    let genres: Vec<Genre> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('genre_id', GenreId, 'name', Name) FROM genres",
    )?;
    let invoices: Vec<Invoice> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('invoice_id', InvoiceId, 'customer_id', CustomerId, 'invoice_date', InvoiceDate, 'billing_address', BillingAddress, 'billing_city', BillingCity, 'billing_state', BillingState, 'billing_country', BillingCountry, 'billing_postal_code', BillingPostalCode, 'total', Total) FROM invoices",
    )?;
    let invoice_items: Vec<InvoiceItem> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('invoice_line_id', InvoiceLineId, 'invoice_id', InvoiceId, 'track_id', TrackId, 'unit_price', UnitPrice, 'quantity', Quantity) FROM invoice_items",
    )?;
    let media_types: Vec<MediaType> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('media_type_id', MediaTypeId, 'name', Name) FROM media_types",
    )?;
    let playlists: Vec<Playlist> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('playlist_id', PlaylistId, 'name', Name) FROM playlists",
    )?;
    let playlist_tracks: Vec<PlaylistTrack> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('playlist_id', PlaylistId, 'track_id', TrackId) FROM playlist_track",
    )?;
    let tracks: Vec<Track> = read_sqlite_data(
        sqlite_db_path,
        "SELECT json_object('track_id', TrackId, 'name', Name, 'album_id', AlbumId, 'media_type_id', MediaTypeId, 'genre_id', GenreId, 'composer', Composer, 'milliseconds', Milliseconds, 'bytes', Bytes, 'unit_price', UnitPrice) FROM tracks",
    )?;

    let scylla_node = "127.0.0.1:9042";
    let session = connect_to_scylla(scylla_node).await?;
    println!("Connected to ScyllaDB");

    create_tables(&session).await?;
    println!("ScyllaDB tables created");

    insert_into_scylla(
        &session,
        &albums,
        "INSERT INTO music.albums (album_id, title, artist_id) VALUES (?, ?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &artists,
        "INSERT INTO music.artists (artist_id, name) VALUES (?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &customers,
        "INSERT INTO music.customers (customer_id, first_name, last_name, company, address, city, state, country, postal_code, phone, fax, email, support_rep_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &employees,
        "INSERT INTO music.employees (employee_id, last_name, first_name, title, reports_to, birth_date, hire_date, address, city, state, country, postal_code, phone, fax, email) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &genres,
        "INSERT INTO music.genres (genre_id, name) VALUES (?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &invoices,
        "INSERT INTO music.invoices (invoice_id, customer_id, invoice_date, billing_address, billing_city, billing_state, billing_country, billing_postal_code, total) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &invoice_items,
        "INSERT INTO music.invoice_items (invoice_line_id, invoice_id, track_id, unit_price, quantity) VALUES (?, ?, ?, ?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &media_types,
        "INSERT INTO music.media_types (media_type_id, name) VALUES (?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &playlists,
        "INSERT INTO music.playlists (playlist_id, name) VALUES (?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &playlist_tracks,
        "INSERT INTO music.playlist_track (playlist_id, track_id) VALUES (?, ?)",
    )
    .await?;
    insert_into_scylla(
        &session,
        &tracks,
        "INSERT INTO music.tracks (track_id, name, album_id, media_type_id, genre_id, composer, milliseconds, bytes, unit_price) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .await?;

    println!("Data migrated to ScyllaDB successfully");

    Ok(())
}
