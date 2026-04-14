use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Parser;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tracing_subscriber::EnvFilter;

use openxet_server::config::{AppConfig, Cli, Command};
use openxet_server::routes::build_router;
use openxet_server::state::{AppState, UploadSession};
use openxet_server::storage::{
    ChunkIndexDispatch, FileIndexDispatch, FilesystemChunkIndex, FilesystemFileIndex,
    NoopXorbMetadataIndex, SqliteChunkIndex, SqliteFileIndex, SqliteXorbMetadataIndex,
    XorbMetadataIndexDispatch, build_storage,
};

const UPLOAD_SESSION_TTL: Duration = Duration::from_secs(30 * 60);
const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_target(true)
        .with_current_span(true)
        .init();

    let cli = Cli::parse();
    let config = AppConfig::load(&cli)?;

    match cli.command {
        Some(Command::GenerateToken { scope, repo, expiry }) => {
            generate_token(&config, &scope, &repo, expiry)
        }
        Some(Command::RebuildIndex { .. }) => {
            tracing::info!("rebuild-index not yet implemented");
            Ok(())
        }
        Some(Command::Serve { .. }) | None => run_server(config).await,
    }
}

fn generate_token(config: &AppConfig, scope: &str, repo: &str, expiry: u64) -> Result<()> {
    use openxet_server::auth::jwt::{Claims, Scope, create_token};

    let scope = match scope {
        "read" => Scope::Read,
        "write" => Scope::Write,
        other => anyhow::bail!("invalid scope: {other} (must be 'read' or 'write')"),
    };

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    let claims = Claims {
        scope,
        repo: repo.to_string(),
        exp: (now + expiry) as usize,
    };

    let token = create_token(&config.auth.secret, &claims)?;
    println!("{token}");
    Ok(())
}

async fn run_server(config: AppConfig) -> Result<()> {
    tracing::info!(
        host = %config.server.host,
        port = config.server.port,
        data_dir = %config.storage.data_dir.display(),
        index_backend = %config.storage.index_backend,
        "starting openxet-server"
    );

    let data_dir = config.data_dir();
    let storage = Arc::new(build_storage(&config.storage).await?);

    let (file_index, chunk_index, xorb_metadata_index) =
        match config.storage.index_backend.as_str() {
            "sqlite" => {
                let db_path = data_dir.join("index.db");
                tracing::info!(db_path = %db_path.display(), "initializing SQLite indexes");
                let fi = FileIndexDispatch::Sqlite(SqliteFileIndex::new(&db_path).await?);
                let ci = ChunkIndexDispatch::Sqlite(SqliteChunkIndex::new(&db_path).await?);
                let xm = XorbMetadataIndexDispatch::Sqlite(
                    SqliteXorbMetadataIndex::new(&db_path).await?,
                );
                (Arc::new(fi), Arc::new(ci), Arc::new(xm))
            }
            "filesystem" | _ => {
                tracing::info!("initializing filesystem indexes");
                let fi =
                    FileIndexDispatch::Filesystem(FilesystemFileIndex::new(data_dir).await?);
                let ci =
                    ChunkIndexDispatch::Filesystem(FilesystemChunkIndex::new(data_dir).await?);
                let xm = XorbMetadataIndexDispatch::Noop(NoopXorbMetadataIndex);
                (Arc::new(fi), Arc::new(ci), Arc::new(xm))
            }
        };

    let uploads_dir = data_dir.join("uploads").join("tmp");
    tokio::fs::create_dir_all(&uploads_dir).await?;

    let upload_sessions = Arc::new(Mutex::new(HashMap::new()));

    let state = AppState {
        storage,
        file_index,
        chunk_index,
        xorb_metadata_index,
        config: Arc::new(config.clone()),
        upload_sessions: upload_sessions.clone(),
    };

    tokio::spawn(cleanup_expired_sessions(upload_sessions));

    let app = build_router(state);

    let addr = format!("{}:{}", config.server.host, config.server.port);
    let listener = TcpListener::bind(&addr).await?;
    tracing::info!("listening on {addr}");

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

async fn cleanup_expired_sessions(sessions: Arc<Mutex<HashMap<String, UploadSession>>>) {
    loop {
        tokio::time::sleep(CLEANUP_INTERVAL).await;
        let now = Instant::now();
        let mut map = sessions.lock().await;
        let expired: Vec<String> = map
            .iter()
            .filter(|(_, s)| now.duration_since(s.created_at) > UPLOAD_SESSION_TTL)
            .map(|(id, _)| id.clone())
            .collect();
        for id in &expired {
            if let Some(session) = map.remove(id) {
                let _ = tokio::fs::remove_file(&session.temp_path).await;
                tracing::info!(session_id = %id, "cleaned up expired upload session");
            }
        }
    }
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl+c");
    tracing::info!("shutdown signal received");
}
