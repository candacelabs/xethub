use serde::{Deserialize, Serialize};

use super::super::error::StorageError;

/// Metadata for a single chunk within a xorb.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XorbChunkMetadata {
    pub chunk_index: u32,
    pub chunk_hash: String,
    pub compressed_offset_start: u64,
    pub compressed_offset_end: u64,
    pub uncompressed_offset: u64,
    pub uncompressed_size: u64,
}

/// Index for xorb chunk metadata (byte offsets, sizes).
/// Used by reconstruction (byte range → presigned URL) and dedup (chunk hash lookup).
pub trait XorbMetadataIndex: Send + Sync {
    fn get(
        &self,
        xorb_hash: &str,
    ) -> impl Future<Output = Result<Vec<XorbChunkMetadata>, StorageError>> + Send;

    fn put(
        &self,
        xorb_hash: &str,
        metadata: &[XorbChunkMetadata],
    ) -> impl Future<Output = Result<(), StorageError>> + Send;

    fn exists(
        &self,
        xorb_hash: &str,
    ) -> impl Future<Output = Result<bool, StorageError>> + Send;
}

/// No-op implementation for filesystem mode (falls back to parsing xorbs from storage).
pub struct NoopXorbMetadataIndex;

impl XorbMetadataIndex for NoopXorbMetadataIndex {
    async fn get(&self, _xorb_hash: &str) -> Result<Vec<XorbChunkMetadata>, StorageError> {
        Ok(Vec::new())
    }

    async fn put(
        &self,
        _xorb_hash: &str,
        _metadata: &[XorbChunkMetadata],
    ) -> Result<(), StorageError> {
        Ok(())
    }

    async fn exists(&self, _xorb_hash: &str) -> Result<bool, StorageError> {
        Ok(false)
    }
}
