//! Batch Operations Demo
//!
//! Demonstrates batch metadata operations and their performance benefits
//!
//! Run with:
//!   RUST_LOG=slayerfs=info cargo run --example batch_demo

use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::meta::create_meta_store_from_url;
use slayerfs::vfs::fs::VFS;
use std::error::Error;
use std::time::Instant;
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

    tracing::info!("=== SlayerFS Batch Operations Demo ===");

    // Setup
    let tmp = tempfile::tempdir()?;
    let layout = ChunkLayout::default();
    let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
    let store = ObjectBlockStore::new(client);
    let meta = create_meta_store_from_url("sqlite::memory:").await?;
    let fs = VFS::new(layout, store, meta).await?;

    // Create many files to demonstrate batch performance
    tracing::info!("Creating 100 test files...");
    let start = Instant::now();
    fs.mkdir_p("/test").await?;
    for i in 1..=100 {
        let path = format!("/test/file_{:03}.txt", i);
        fs.create_file(&path).await?;
        fs.write(&path, 0, format!("Content of file {}\n", i).as_bytes())
            .await?;
    }
    let create_time = start.elapsed();
    tracing::info!(duration_ms = ?create_time.as_millis(), "File creation completed");

    // List directory - this will use batch getattr via readdirplus
    tracing::info!("Listing directory (using readdirplus with batch operations)...");
    let start = Instant::now();
    let entries = fs.readdir("/test").await?;
    let readdir_time = start.elapsed();
    tracing::info!(
        files = entries.len(),
        duration_ms = ?readdir_time.as_millis(),
        "Directory listing completed"
    );

    // Read all files to show cache effects
    tracing::info!("Reading all files (first pass - expect cache misses)...");
    let start = Instant::now();
    for i in 1..=100 {
        let path = format!("/test/file_{:03}.txt", i);
        let _ = fs.read(&path, 0, 1024).await?;
    }
    let first_read_time = start.elapsed();
    tracing::info!(duration_ms = ?first_read_time.as_millis(), "First read pass completed");

    tracing::info!("Reading all files again (second pass - expect cache hits)...");
    let start = Instant::now();
    for i in 1..=100 {
        let path = format!("/test/file_{:03}.txt", i);
        let _ = fs.read(&path, 0, 1024).await?;
    }
    let second_read_time = start.elapsed();
    tracing::info!(duration_ms = ?second_read_time.as_millis(), "Second read pass completed");

    // Performance summary
    tracing::info!("=== Performance Summary ===");
    tracing::info!("File creation (100 files): {:?}", create_time);
    tracing::info!("Directory listing: {:?}", readdir_time);
    tracing::info!("First read pass: {:?}", first_read_time);
    tracing::info!(
        "Second read pass: {:?} (cache speedup: {:.2}x)",
        second_read_time,
        first_read_time.as_millis() as f64 / second_read_time.as_millis() as f64
    );

    tracing::info!("=== Demo completed successfully ===");
    Ok(())
}
