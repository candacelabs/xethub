# Scope Decisions

Answers to Phase 0 clarifying questions, recorded for future sessions.

## Q1: Index Dispatch Pattern

**Decision:** Create `FileIndexDispatch` and `ChunkIndexDispatch` enums mirroring the existing `StorageDispatch` pattern. Two variants each: `Filesystem` and `Sqlite`. `AppState` holds `Arc<FileIndexDispatch>`, `Arc<ChunkIndexDispatch>`, and `Arc<XorbMetadataIndexDispatch>`. Config key: `storage.index_backend = "sqlite" | "filesystem"`, default `"sqlite"`.

**Why:** The existing RPITIT traits (`FileIndex`, `ChunkIndex`) can't be used as `dyn` trait objects. The enum dispatch pattern is already established in `StorageDispatch` and keeps everything concrete + zero-cost.

## Q2: ChunkIndex Trait Extension

**Decision:** Both (c). Add `get_by_xorb(&self, xorb_hash: &str) → Vec<(String, u32)>` to the `ChunkIndex` trait (filesystem impl does a full scan for backward compat). AND add a separate `XorbMetadataIndex` trait for the `xorb_metadata` table with byte offset data.

**Why:** Dedup handler needs both: chunk hashes from the xorb (via `get_by_xorb` or xorb_metadata) and byte offset metadata (via `XorbMetadataIndex`). Separation keeps concerns clean.

## Q3: Presigned URL Mechanics

**Decision:** Store `Option<Arc<dyn object_store::signer::Signer>>` alongside the `dyn ObjectStore` in `ObjectStoreBackend`. Populated when backend is S3/MinIO. If signer is `None`, fall back to server-proxied URLs.

**Why:** The `Signer` trait is only implemented by concrete `AmazonS3`, not `dyn ObjectStore`. This gives clean degradation for non-S3 backends.

## Q4: Presigned URL Expiry

**Decision:** Default 1 hour (3600 seconds). Configurable via `storage.presigned_url_expiry_seconds`.

## Q5: Rebuild-Index Binary

**Decision:** Subcommand on existing binary. `openxet-server serve` (default) and `openxet-server rebuild-index`. Shares all parsing/storage code. Also add `openxet-server generate-token`.

## Q6: LFS Agent Auth

**Decision:** Long-lived token via env var (`OPENXET_TOKEN`) for v1. Add `openxet-server generate-token --scope write --repo "*"` subcommand. No login endpoint needed.

## Q7: xorb_metadata Table Schema

**Decision:** Store both compressed and uncompressed offsets, plus chunk_hash for reverse lookup:

```sql
xorb_metadata(
  xorb_hash TEXT,
  chunk_index INTEGER,
  chunk_hash TEXT,
  compressed_offset_start INTEGER,
  compressed_offset_end INTEGER,
  uncompressed_offset INTEGER,
  uncompressed_size INTEGER,
  PRIMARY KEY(xorb_hash, chunk_index)
)
```

## Explicit Exclusions

- No cloud-specific services (AWS Lambda, DynamoDB, RDS, etc.)
- No Kubernetes, Helm, or container orchestration
- No multi-user auth flows, OAuth, OIDC
- No login endpoint or session management
- No React frontend changes
- No changes to the hashing, chunking, or cas_types crates (consumers only)
- No migration from filesystem to SQLite (rebuild-index handles cold start)
