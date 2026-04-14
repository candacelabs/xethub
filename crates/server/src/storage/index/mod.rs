pub mod chunk_index;
pub mod dispatch;
pub mod file_index;
pub mod sqlite;
pub mod xorb_metadata;

use serde::{Deserialize, Serialize};

use super::error::StorageError;

pub use chunk_index::FilesystemChunkIndex;
pub use dispatch::{ChunkIndexDispatch, FileIndexDispatch, XorbMetadataIndexDispatch};
pub use file_index::FilesystemFileIndex;
pub use sqlite::{SqliteChunkIndex, SqliteFileIndex, SqliteXorbMetadataIndex};
pub use xorb_metadata::{NoopXorbMetadataIndex, XorbChunkMetadata, XorbMetadataIndex};

/// A location where a chunk can be found: which xorb and at what index within it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkLocation {
    pub xorb_hash: String,
    pub chunk_index: u32,
}

/// Index mapping file hashes to shard hashes.
///
/// Used for file reconstruction lookups: given a file hash, find the shard
/// that describes how to reconstruct it.
pub trait FileIndex: Send + Sync {
    /// Look up which shard contains the file's reconstruction info.
    /// Returns `None` if the file is not indexed.
    fn get(
        &self,
        file_hash: &str,
    ) -> impl Future<Output = Result<Option<String>, StorageError>> + Send;

    /// Record that `file_hash` can be reconstructed from `shard_hash`.
    fn put(
        &self,
        file_hash: &str,
        shard_hash: &str,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Bulk insert file→shard mappings. Default: loops over put().
    fn put_batch(
        &self,
        entries: &[(String, String)],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// List all file index entries as (file_hash, shard_hash) pairs.
    fn list_all(&self) -> impl Future<Output = Result<Vec<(String, String)>, StorageError>> + Send;
}

/// Index mapping chunk hashes to their locations in xorbs.
///
/// Used for global deduplication: given a chunk hash, find which xorb(s)
/// already contain that chunk data.
pub trait ChunkIndex: Send + Sync {
    /// Look up all known locations for a chunk.
    /// Returns an empty `Vec` if the chunk is not indexed.
    fn get(
        &self,
        chunk_hash: &str,
    ) -> impl Future<Output = Result<Vec<ChunkLocation>, StorageError>> + Send;

    /// Record that `chunk_hash` exists at `location`.
    /// Deduplicates: adding the same location twice is a no-op.
    fn put(
        &self,
        chunk_hash: &str,
        location: ChunkLocation,
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Bulk insert chunk→location mappings. Default: loops over put().
    fn put_batch(
        &self,
        entries: &[(String, ChunkLocation)],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    /// Reverse lookup: all chunks in a given xorb, sorted by chunk_index.
    fn get_by_xorb(
        &self,
        xorb_hash: &str,
    ) -> impl Future<Output = Result<Vec<(String, u32)>, StorageError>> + Send;
}
