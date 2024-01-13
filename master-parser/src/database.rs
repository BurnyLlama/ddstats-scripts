use sqlx::{migrate::MigrateDatabase, sqlite::SqlitePoolOptions, Sqlite, SqlitePool};
use std::error::Error;

const DB_URL: &str = "../db/master.db";

// pub fn insert_snapshot(date_entry: &mut DateEntry, conn: &Connection) {
//     let time = Instant::now();

//     //conn.execute_batch("BEGIN TRANSACTION;").unwrap();
//     let mut stmt = conn.prepare("INSERT INTO record_snapshot (date, location, gametype, map, name, clan, country, skin_name, skin_color_body, skin_color_feet, afk, team, time) VALUES (?1 ,?2 ,?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13);").unwrap();
//     for (key, time) in date_entry.snapshot.iter() {
//         let params = (
//             date_entry.date.format("%Y-%m-%d").to_string(),
//             key.location.clone(),
//             key.game_type.clone(),
//             key.map.clone(),
//             key.name.clone(),
//             key.clan.clone(),
//             key.country,
//             key.skin_name.clone(),
//             key.skin_color_body,
//             key.skin_color_feet,
//             key.afk,
//             key.team,
//             *time,
//         );

//         let _ = stmt.execute(params);
//     }
//     //conn.execute_batch("COMMIT TRANSACTION;").unwrap();

//     // mark day as processed
//     conn.execute(
//         "INSERT INTO processed (date) VALUES (?1)",
//         params![date_entry.date.format("%Y-%m-%d").to_string()],
//     )
//     .unwrap();

//     let duration = time.elapsed();
//     println!(
//         "{:?} - Inserting {} took: {:?}",
//         thread::current().id(),
//         date_entry.date,
//         duration
//     );
// }

// pub fn process_day(date_entry: &mut DateEntry) -> Result<(), Box<dyn Error>> {
//     let resp = ureq::get(&format!(
//         "https://ddnet.org/stats/master/{}.tar.zstd",
//         date_entry.date.format("%Y-%m-%d")
//     ))
//     .call()?;
//     let decoder = zstd::stream::Decoder::new(resp.into_reader())?;

//     let mut archive = Archive::new(decoder);

//     let time = Instant::now();
//     for entry in archive.entries()? {
//         let entry = entry.unwrap();

//         let data: ServerList = match simd_json::from_reader(entry) {
//             Ok(data) => data,
//             Err(err) => {
//                 println!("{:?}", err);
//                 continue;
//             }
//         };

//         for server in data.servers.iter() {
//             for client in server.info.clients.iter() {
//                 Client::process(client, server, &mut date_entry.snapshot)
//             }
//         }
//     }
//     let duration = time.elapsed();
//     println!(
//         "{:?} - Parsing {} took: {:?}",
//         thread::current().id(),
//         date_entry.date,
//         duration
//     );
//     Ok(())
// }

pub struct DatabaseHandler {
    pub pool: SqlitePool,
}

impl DatabaseHandler {
    pub async fn create() -> Result<Self, Box<dyn Error>> {
        if !Sqlite::database_exists(DB_URL).await.unwrap_or(false) {
            println!("Creating database at {}", DB_URL);
            Sqlite::create_database(DB_URL).await?;
        }

        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect(DB_URL)
            .await?;

        let row: (i32,) = sqlx::query_as("SELECT $1")
            .bind(150_i32)
            .fetch_one(&pool)
            .await?;

        assert_eq!(row.0, 150_i32);

        Ok(DatabaseHandler { pool })
    }

    pub async fn setup(&self) -> Result<(), Box<dyn Error>> {
        // Set pragmas
        sqlx::query("PRAGMA journal_mode = OFF; PRAGMA synchronous = 0; PRAGMA cache_size = 1000000; PRAGMA locking_mode = EXCLUSIVE; PRAGMA temp_store = MEMORY;")
            .execute(&self.pool)
            .await?;

        // Create all tables.
        sqlx::query(
            "
            CREATE TABLE IF NOT EXISTS record_snapshot (
                date TEXT NOT NULL,
                location CHAR(32) NOT NULL,
                gametype CHAR(32) NOT NULL,
                map CHAR(128) NOT NULL,
                name CHAR(32) NOT NULL,
                clan CHAR(32) NOT NULL,
                country INTEGER NOT NULL,
                skin_name CHAR(32),
                skin_color_body INTEGER,
                skin_color_feet INTEGER,
                afk INTEGER,
                team INTEGER,
                time INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS processed (date TEXT NOT NULL);",
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
#[cfg(test)]

mod tests {
    use super::*;

    #[test]
    fn test_connect_database() {
        async fn test() {
            let db = match DatabaseHandler::create().await {
                Ok(db) => db,
                Err(err) => panic!("Could not connect to database!\n{}", err),
            };

            match db.setup().await {
                Ok(_) => (),
                Err(err) => panic!("Could not set up the database!\n{}", err),
            };
        }

        tokio_test::block_on(test());
    }
}
