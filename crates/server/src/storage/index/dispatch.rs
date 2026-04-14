use super::super::error::StorageError;
use super::chunk_index::FilesystemChunkIndex;
use super::file_index::FilesystemFileIndex;
use super::sqlite::{SqliteChunkIndex, SqliteFileIndex, SqliteXorbMetadataIndex};
use super::xorb_metadata::{NoopXorbMetadataIndex, XorbChunkMetadata, XorbMetadataIndex};
use super::{ChunkIndex, ChunkLocation, FileIndex};

// ---------------------------------------------------------------------------
// FileIndexDispatch
// ---------------------------------------------------------------------------

pub enum FileIndexDispatch {
    Filesystem(FilesystemFileIndex),
    Sqlite(SqliteFileIndex),
}

impl FileIndex for FileIndexDispatch {
    async fn get(&self, file_hash: &str) -> Result<Option<String>, StorageError> {
        match self {
            Self::Filesystem(i) => i.get(file_hash).await,
            Self::Sqlite(i) => i.get(file_hash).await,
        }
    }

    async fn put(&self, file_hash: &str, shard_hash: &str) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(i) => i.put(file_hash, shard_hash).await,
            Self::Sqlite(i) => i.put(file_hash, shard_hash).await,
        }
    }

    async fn put_batch(&self, entries: &[(String, String)]) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(i) => i.put_batch(entries).await,
            Self::Sqlite(i) => i.put_batch(entries).await,
        }
    }

    async fn list_all(&self) -> Result<Vec<(String, String)>, StorageError> {
        match self {
            Self::Filesystem(i) => i.list_all().await,
            Self::Sqlite(i) => i.list_all().await,
        }
    }
}

// ---------------------------------------------------------------------------
// ChunkIndexDispatch
// ---------------------------------------------------------------------------

pub enum ChunkIndexDispatch {
    Filesystem(FilesystemChunkIndex),
    Sqlite(SqliteChunkIndex),
}

impl ChunkIndex for ChunkIndexDispatch {
    async fn get(&self, chunk_hash: &str) -> Result<Vec<ChunkLocation>, StorageError> {
        match self {
            Self::Filesystem(i) => i.get(chunk_hash).await,
            Self::Sqlite(i) => i.get(chunk_hash).await,
        }
    }

    async fn put(
        &self,
        chunk_hash: &str,
        location: ChunkLocation,
    ) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(i) => i.put(chunk_hash, location).await,
            Self::Sqlite(i) => i.put(chunk_hash, location).await,
        }
    }

    async fn put_batch(
        &self,
        entries: &[(String, ChunkLocation)],
    ) -> Result<(), StorageError> {
        match self {
            Self::Filesystem(i) => i.put_batch(entries).await,
            Self::Sqlite(i) => i.put_batch(entries).await,
        }
    }

    async fn get_by_xorb(
        &self,
        xorb_hash: &str,
    ) -> Result<Vec<(String, u32)>, StorageError> {
        match self {
            Self::Filesystem(i) => i.get_by_xorb(xorb_hash).await,
            Self::Sqlite(i) => i.get_by_xorb(xorb_hash).await,
        }
    }
}

// ---------------------------------------------------------------------------
// XorbMetadataIndexDispatch
// ---------------------------------------------------------------------------

pub enum XorbMetadataIndexDispatch {
    Noop(NoopXorbMetadataIndex),
    Sqlite(SqliteXorbMetadataIndex),
}

impl XorbMetadataIndex for XorbMetadataIndexDispatch {
    async fn get(
        &self,
        xorb_hash: &str,
    ) -> Result<Vec<XorbChunkMetadata>, StorageError> {
        match self {
            Self::Noop(i) => i.get(xorb_hash).await,
            Self::Sqlite(i) => i.get(xorb_hash).await,
        }
    }

    async fn put(
        &self,
        xorb_hash: &str,
        metadata: &[XorbChunkMetadata],
    ) -> Result<(), StorageError> {
        match self {
            Self::Noop(i) => i.put(xorb_hash, metadata).await,
            Self::Sqlite(i) => i.put(xorb_hash, metadata).await,
        }
    }

    async fn exists(&self, xorb_hash: &str) -> Result<bool, StorageError> {
        match self {
            Self::Noop(i) => i.exists(xorb_hash).await,
            Self::Sqlite(i) => i.exists(xorb_hash).await,
        }
    }
}
