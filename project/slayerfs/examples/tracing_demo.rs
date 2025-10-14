//! Tracing Demo
//!
//! Demonstrates tracing functionality and cache hit rates
//!
//! Run with:
//!   RUST_LOG=slayerfs=debug cargo run --example tracing_demo

use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::meta::create_meta_store_from_url;
use slayerfs::vfs::fs::VFS;
use std::error::Error;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "slayerfs=info".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("=== SlayerFS Tracing Demo ===");

    // Setup
    let tmp = tempfile::tempdir()?;
    let layout = ChunkLayout::default();
    let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
    let store = ObjectBlockStore::new(client);
    let meta = create_meta_store_from_url("sqlite::memory:").await?;
    let fs = VFS::new(layout, store, meta).await?;

    tracing::info!("VFS initialized successfully");

    // Create directory structure
    tracing::info!("Creating directory structure...");
    fs.mkdir_p("/data/logs").await?;
    fs.mkdir_p("/data/cache").await?;
    fs.mkdir_p("/config").await?;

    // Create files
    tracing::info!("Creating files...");
    for i in 1..=5 {
        let path = format!("/data/logs/log_{:03}.txt", i);
        fs.create_file(&path).await?;
        tracing::info!(path = %path, "File created");
    }

    // Write data
    tracing::info!("Writing data to files...");
    for i in 1..=5 {
        let path = format!("/data/logs/log_{:03}.txt", i);
        let data = format!("Log entry {} - This is test data\n", i);
        fs.write(&path, 0, data.as_bytes()).await?;
    }

    // Read data multiple times to show cache hits
    tracing::info!("Reading files (first time - cache miss expected)...");
    for i in 1..=5 {
        let path = format!("/data/logs/log_{:03}.txt", i);
        let data = fs.read(&path, 0, 1024).await?;
        tracing::debug!(path = %path, bytes = data.len(), "Read completed");
    }

    tracing::info!("Reading files again (cache hits expected)...");
    for i in 1..=5 {
        let path = format!("/data/logs/log_{:03}.txt", i);
        let data = fs.read(&path, 0, 1024).await?;
        tracing::debug!(path = %path, bytes = data.len(), "Read completed");
    }

    // List directory
    tracing::info!("Listing /data/logs directory...");
    let entries = fs.readdir("/data/logs").await?;
    for entry in entries {
        tracing::info!(name = %entry.name, ino = entry.ino, kind = ?entry.kind, "Directory entry");
    }

    // Stat files
    tracing::info!("Getting file stats...");
    for i in 1..=5 {
        let path = format!("/data/logs/log_{:03}.txt", i);
        if let Some(attr) = fs.stat(&path).await {
            tracing::info!(
                path = %path,
                ino = attr.ino,
                size = attr.size,
                "File stat"
            );
        }
    }

    tracing::info!("=== Demo completed successfully ===");
    Ok(())
}
