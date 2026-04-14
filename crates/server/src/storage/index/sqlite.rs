use std::path::Path;
use std::sync::Arc;

use rusqlite::Connection;
use tokio::sync::Mutex;

use super::super::error::StorageError;
use super::xorb_metadata::{XorbChunkMetadata, XorbMetadataIndex};
use super::{ChunkIndex, ChunkLocation, FileIndex};

fn map_sqlite_error(e: rusqlite::Error) -> StorageError {
    StorageError::ObjectStore(format!("sqlite error: {e}"))
}

// ---------------------------------------------------------------------------
// SqliteFileIndex
// ---------------------------------------------------------------------------

pub struct SqliteFileIndex {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteFileIndex {
    pub async fn new(db_path: &Path) -> Result<Self, StorageError> {
        let path = db_path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, StorageError> {
            let conn = Connection::open(&path).map_err(map_sqlite_error)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 CREATE TABLE IF NOT EXISTS files (
                     file_hash TEXT PRIMARY KEY,
                     shard_hash TEXT NOT NULL
                 );",
            )
            .map_err(map_sqlite_error)?;
            Ok(conn)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
        .map_err(|e: StorageError| e)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl FileIndex for SqliteFileIndex {
    async fn get(&self, file_hash: &str) -> Result<Option<String>, StorageError> {
        let conn = self.conn.clone();
        let hash = file_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached("SELECT shard_hash FROM files WHERE file_hash = ?1")
                .map_err(map_sqlite_error)?;
            match stmt.query_row(rusqlite::params![hash], |row| row.get::<_, String>(0)) {
                Ok(shard_hash) => Ok(Some(shard_hash)),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
                Err(e) => Err(map_sqlite_error(e)),
            }
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn put(&self, file_hash: &str, shard_hash: &str) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        let fh = file_hash.to_string();
        let sh = shard_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT OR REPLACE INTO files (file_hash, shard_hash) VALUES (?1, ?2)",
                rusqlite::params![fh, sh],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn put_batch(&self, entries: &[(String, String)]) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute_batch("BEGIN")
                .map_err(map_sqlite_error)?;
            {
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT OR REPLACE INTO files (file_hash, shard_hash) VALUES (?1, ?2)",
                    )
                    .map_err(map_sqlite_error)?;
                for (fh, sh) in &entries {
                    stmt.execute(rusqlite::params![fh, sh])
                        .map_err(map_sqlite_error)?;
                }
            }
            conn.execute_batch("COMMIT")
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn list_all(&self) -> Result<Vec<(String, String)>, StorageError> {
        let conn = self.conn.clone();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached("SELECT file_hash, shard_hash FROM files ORDER BY file_hash")
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map([], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                })
                .map_err(map_sqlite_error)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(map_sqlite_error)?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }
}

// ---------------------------------------------------------------------------
// SqliteChunkIndex
// ---------------------------------------------------------------------------

pub struct SqliteChunkIndex {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteChunkIndex {
    pub async fn new(db_path: &Path) -> Result<Self, StorageError> {
        let path = db_path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, StorageError> {
            let conn = Connection::open(&path).map_err(map_sqlite_error)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 CREATE TABLE IF NOT EXISTS chunks (
                     chunk_hash TEXT NOT NULL,
                     xorb_hash TEXT NOT NULL,
                     chunk_index INTEGER NOT NULL,
                     PRIMARY KEY (chunk_hash, xorb_hash, chunk_index)
                 );
                 CREATE INDEX IF NOT EXISTS idx_chunks_xorb ON chunks(xorb_hash);",
            )
            .map_err(map_sqlite_error)?;
            Ok(conn)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
        .map_err(|e: StorageError| e)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl ChunkIndex for SqliteChunkIndex {
    async fn get(&self, chunk_hash: &str) -> Result<Vec<ChunkLocation>, StorageError> {
        let conn = self.conn.clone();
        let hash = chunk_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached(
                    "SELECT xorb_hash, chunk_index FROM chunks WHERE chunk_hash = ?1",
                )
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(rusqlite::params![hash], |row| {
                    Ok(ChunkLocation {
                        xorb_hash: row.get::<_, String>(0)?,
                        chunk_index: row.get::<_, u32>(1)?,
                    })
                })
                .map_err(map_sqlite_error)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(map_sqlite_error)?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn put(
        &self,
        chunk_hash: &str,
        location: ChunkLocation,
    ) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        let ch = chunk_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute(
                "INSERT OR IGNORE INTO chunks (chunk_hash, xorb_hash, chunk_index) VALUES (?1, ?2, ?3)",
                rusqlite::params![ch, location.xorb_hash, location.chunk_index],
            )
            .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn put_batch(
        &self,
        entries: &[(String, ChunkLocation)],
    ) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        let entries = entries.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute_batch("BEGIN")
                .map_err(map_sqlite_error)?;
            {
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT OR IGNORE INTO chunks (chunk_hash, xorb_hash, chunk_index) VALUES (?1, ?2, ?3)",
                    )
                    .map_err(map_sqlite_error)?;
                for (ch, loc) in &entries {
                    stmt.execute(rusqlite::params![ch, loc.xorb_hash, loc.chunk_index])
                        .map_err(map_sqlite_error)?;
                }
            }
            conn.execute_batch("COMMIT")
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn get_by_xorb(
        &self,
        xorb_hash: &str,
    ) -> Result<Vec<(String, u32)>, StorageError> {
        let conn = self.conn.clone();
        let hash = xorb_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached(
                    "SELECT chunk_hash, chunk_index FROM chunks WHERE xorb_hash = ?1 ORDER BY chunk_index",
                )
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(rusqlite::params![hash], |row| {
                    Ok((row.get::<_, String>(0)?, row.get::<_, u32>(1)?))
                })
                .map_err(map_sqlite_error)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(map_sqlite_error)?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }
}

// ---------------------------------------------------------------------------
// SqliteXorbMetadataIndex
// ---------------------------------------------------------------------------

pub struct SqliteXorbMetadataIndex {
    conn: Arc<Mutex<Connection>>,
}

impl SqliteXorbMetadataIndex {
    pub async fn new(db_path: &Path) -> Result<Self, StorageError> {
        let path = db_path.to_path_buf();
        let conn = tokio::task::spawn_blocking(move || -> Result<Connection, StorageError> {
            let conn = Connection::open(&path).map_err(map_sqlite_error)?;
            conn.execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA synchronous=NORMAL;
                 CREATE TABLE IF NOT EXISTS xorb_metadata (
                     xorb_hash TEXT NOT NULL,
                     chunk_index INTEGER NOT NULL,
                     chunk_hash TEXT NOT NULL,
                     compressed_offset_start INTEGER NOT NULL,
                     compressed_offset_end INTEGER NOT NULL,
                     uncompressed_offset INTEGER NOT NULL,
                     uncompressed_size INTEGER NOT NULL,
                     PRIMARY KEY (xorb_hash, chunk_index)
                 );",
            )
            .map_err(map_sqlite_error)?;
            Ok(conn)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
        .map_err(|e: StorageError| e)?;

        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }
}

impl XorbMetadataIndex for SqliteXorbMetadataIndex {
    async fn get(
        &self,
        xorb_hash: &str,
    ) -> Result<Vec<XorbChunkMetadata>, StorageError> {
        let conn = self.conn.clone();
        let hash = xorb_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached(
                    "SELECT chunk_index, chunk_hash, compressed_offset_start, compressed_offset_end, \
                     uncompressed_offset, uncompressed_size \
                     FROM xorb_metadata WHERE xorb_hash = ?1 ORDER BY chunk_index",
                )
                .map_err(map_sqlite_error)?;
            let rows = stmt
                .query_map(rusqlite::params![hash], |row| {
                    Ok(XorbChunkMetadata {
                        chunk_index: row.get(0)?,
                        chunk_hash: row.get(1)?,
                        compressed_offset_start: row.get(2)?,
                        compressed_offset_end: row.get(3)?,
                        uncompressed_offset: row.get(4)?,
                        uncompressed_size: row.get(5)?,
                    })
                })
                .map_err(map_sqlite_error)?;
            let mut results = Vec::new();
            for row in rows {
                results.push(row.map_err(map_sqlite_error)?);
            }
            Ok(results)
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn put(
        &self,
        xorb_hash: &str,
        metadata: &[XorbChunkMetadata],
    ) -> Result<(), StorageError> {
        let conn = self.conn.clone();
        let hash = xorb_hash.to_string();
        let metadata = metadata.to_vec();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            conn.execute_batch("BEGIN")
                .map_err(map_sqlite_error)?;
            {
                let mut stmt = conn
                    .prepare_cached(
                        "INSERT OR REPLACE INTO xorb_metadata \
                         (xorb_hash, chunk_index, chunk_hash, compressed_offset_start, \
                          compressed_offset_end, uncompressed_offset, uncompressed_size) \
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                    )
                    .map_err(map_sqlite_error)?;
                for m in &metadata {
                    stmt.execute(rusqlite::params![
                        hash,
                        m.chunk_index,
                        m.chunk_hash,
                        m.compressed_offset_start,
                        m.compressed_offset_end,
                        m.uncompressed_offset,
                        m.uncompressed_size,
                    ])
                    .map_err(map_sqlite_error)?;
                }
            }
            conn.execute_batch("COMMIT")
                .map_err(map_sqlite_error)?;
            Ok(())
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }

    async fn exists(&self, xorb_hash: &str) -> Result<bool, StorageError> {
        let conn = self.conn.clone();
        let hash = xorb_hash.to_string();
        tokio::task::spawn_blocking(move || {
            let conn = conn.blocking_lock();
            let mut stmt = conn
                .prepare_cached("SELECT 1 FROM xorb_metadata WHERE xorb_hash = ?1 LIMIT 1")
                .map_err(map_sqlite_error)?;
            match stmt.query_row(rusqlite::params![hash], |_row| Ok(())) {
                Ok(()) => Ok(true),
                Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
                Err(e) => Err(map_sqlite_error(e)),
            }
        })
        .await
        .map_err(|e| StorageError::ObjectStore(format!("spawn_blocking: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HASH_A: &str = "a1b2c3d4e5f60708091011121314151617181920212223242526272829303132";
    const HASH_B: &str = "b1b2c3d4e5f60708091011121314151617181920212223242526272829303132";
    const HASH_C: &str = "c1b2c3d4e5f60708091011121314151617181920212223242526272829303132";
    const HASH_D: &str = "d1b2c3d4e5f60708091011121314151617181920212223242526272829303132";

    // ----- SqliteFileIndex tests -----

    #[tokio::test]
    async fn test_sqlite_file_index_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteFileIndex::new(&db).await.unwrap();

        index.put(HASH_A, HASH_B).await.unwrap();
        let result = index.get(HASH_A).await.unwrap();
        assert_eq!(result, Some(HASH_B.to_string()));

        // Unknown key returns None
        let none = index.get(HASH_C).await.unwrap();
        assert_eq!(none, None);
    }

    #[tokio::test]
    async fn test_sqlite_file_index_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteFileIndex::new(&db).await.unwrap();

        let entries = vec![
            (HASH_A.to_string(), HASH_B.to_string()),
            (HASH_C.to_string(), HASH_D.to_string()),
        ];
        index.put_batch(&entries).await.unwrap();

        let all = index.list_all().await.unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0], (HASH_A.to_string(), HASH_B.to_string()));
        assert_eq!(all[1], (HASH_C.to_string(), HASH_D.to_string()));
    }

    #[tokio::test]
    async fn test_sqlite_file_index_overwrite() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteFileIndex::new(&db).await.unwrap();

        index.put(HASH_A, HASH_B).await.unwrap();
        index.put(HASH_A, HASH_C).await.unwrap();

        let result = index.get(HASH_A).await.unwrap();
        assert_eq!(result, Some(HASH_C.to_string()));
    }

    // ----- SqliteChunkIndex tests -----

    #[tokio::test]
    async fn test_sqlite_chunk_index_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteChunkIndex::new(&db).await.unwrap();

        let loc = ChunkLocation {
            xorb_hash: HASH_B.to_string(),
            chunk_index: 5,
        };
        index.put(HASH_A, loc.clone()).await.unwrap();

        let result = index.get(HASH_A).await.unwrap();
        assert_eq!(result, vec![loc]);

        // Unknown returns empty
        let empty = index.get(HASH_C).await.unwrap();
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_sqlite_chunk_index_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteChunkIndex::new(&db).await.unwrap();

        let entries = vec![
            (
                HASH_A.to_string(),
                ChunkLocation {
                    xorb_hash: HASH_B.to_string(),
                    chunk_index: 0,
                },
            ),
            (
                HASH_A.to_string(),
                ChunkLocation {
                    xorb_hash: HASH_C.to_string(),
                    chunk_index: 1,
                },
            ),
        ];
        index.put_batch(&entries).await.unwrap();

        let result = index.get(HASH_A).await.unwrap();
        assert_eq!(result.len(), 2);
    }

    #[tokio::test]
    async fn test_sqlite_chunk_index_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteChunkIndex::new(&db).await.unwrap();

        let loc = ChunkLocation {
            xorb_hash: HASH_B.to_string(),
            chunk_index: 0,
        };
        index.put(HASH_A, loc.clone()).await.unwrap();
        index.put(HASH_A, loc.clone()).await.unwrap();

        let result = index.get(HASH_A).await.unwrap();
        assert_eq!(result.len(), 1);
    }

    #[tokio::test]
    async fn test_sqlite_chunk_index_get_by_xorb() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteChunkIndex::new(&db).await.unwrap();

        // Two chunks in the same xorb
        let entries = vec![
            (
                HASH_A.to_string(),
                ChunkLocation {
                    xorb_hash: HASH_D.to_string(),
                    chunk_index: 0,
                },
            ),
            (
                HASH_B.to_string(),
                ChunkLocation {
                    xorb_hash: HASH_D.to_string(),
                    chunk_index: 1,
                },
            ),
            // Different xorb — should not appear
            (
                HASH_C.to_string(),
                ChunkLocation {
                    xorb_hash: HASH_A.to_string(),
                    chunk_index: 0,
                },
            ),
        ];
        index.put_batch(&entries).await.unwrap();

        let result = index.get_by_xorb(HASH_D).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0], (HASH_A.to_string(), 0));
        assert_eq!(result[1], (HASH_B.to_string(), 1));
    }

    // ----- SqliteXorbMetadataIndex tests -----

    #[tokio::test]
    async fn test_sqlite_xorb_metadata_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteXorbMetadataIndex::new(&db).await.unwrap();

        let metadata = vec![
            XorbChunkMetadata {
                chunk_index: 0,
                chunk_hash: HASH_A.to_string(),
                compressed_offset_start: 0,
                compressed_offset_end: 100,
                uncompressed_offset: 0,
                uncompressed_size: 200,
            },
            XorbChunkMetadata {
                chunk_index: 1,
                chunk_hash: HASH_B.to_string(),
                compressed_offset_start: 100,
                compressed_offset_end: 250,
                uncompressed_offset: 200,
                uncompressed_size: 300,
            },
        ];

        assert!(!index.exists(HASH_C).await.unwrap());

        index.put(HASH_C, &metadata).await.unwrap();

        assert!(index.exists(HASH_C).await.unwrap());

        let result = index.get(HASH_C).await.unwrap();
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].chunk_index, 0);
        assert_eq!(result[0].chunk_hash, HASH_A);
        assert_eq!(result[0].compressed_offset_end, 100);
        assert_eq!(result[1].chunk_index, 1);
        assert_eq!(result[1].uncompressed_size, 300);
    }

    #[tokio::test]
    async fn test_sqlite_xorb_metadata_not_exists() {
        let dir = tempfile::tempdir().unwrap();
        let db = dir.path().join("index.db");
        let index = SqliteXorbMetadataIndex::new(&db).await.unwrap();

        assert!(!index.exists(HASH_A).await.unwrap());
        let result = index.get(HASH_A).await.unwrap();
        assert!(result.is_empty());
    }
}
