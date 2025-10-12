use clap::Parser;
use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::fuse::mount_v2::mount_vfs_v2;
use slayerfs::meta::config::{Config, DatabaseConfig, DatabaseType};
use slayerfs::meta::database_store::DatabaseMetaStore;
use slayerfs::meta::store_v2::MetaStoreV2;
use slayerfs::vfs::fs_v2::VfsV2;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// SQLite database file path (e.g., /tmp/slayerfs.db)
    #[arg(short, long)]
    db: PathBuf,

    /// Mount point path
    #[arg(short, long, default_value = "/tmp/mount")]
    mount: PathBuf,

    /// Backend storage path
    #[arg(short, long, default_value = "/tmp/storage")]
    storage: PathBuf,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("This demo only works on Linux (requires FUSE support).");
        eprintln!("If you're on Windows, please run under WSL/WSL2 or Linux host.");
        std::process::exit(2);
    }

    #[cfg(target_os = "linux")]
    {
        let args = Args::parse();
        let program_name = std::env::args()
            .next()
            .unwrap_or_else(|| "persistence_demo".to_string());

        let db_path = args.db;
        let mount_point = args.mount;
        let backend_storage = args.storage;

        println!("=== SlayerFS Persistence Demo ===");
        println!("Database: {}", db_path.display());
        println!("Data storage: {}", backend_storage.display());
        println!("Mount point: {}", mount_point.display());
        println!();

        // Create directories if they don't exist
        std::fs::create_dir_all(&backend_storage)?;
        std::fs::create_dir_all(&mount_point)?;

        // Prepare object backend
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(&backend_storage));
        let store = ObjectBlockStore::new(client);

        // Create SQLite metadata store
        let sqlite_url = format!("sqlite://{}?mode=rwc", db_path.display());
        println!("Initializing SQLite metadata store...");
        println!("SQLite URL: {}", sqlite_url);

        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite {
                    url: sqlite_url.clone(),
                },
            },
        };

        let meta = DatabaseMetaStore::from_config(config)
            .await
            .map_err(|e| format!("Failed to initialize metadata store: {}", e))?;

        println!("Verifying metadata storage status...");
        let root_ino = meta.root_ino();
        let root_entries = meta
            .readdir(root_ino)
            .await
            .map_err(|e| format!("readdir failed: {}", e))?;

        println!("Root directory contains {} entries", root_entries.len());
        if !root_entries.is_empty() {
            println!("Existing entries:");
            for entry in &root_entries {
                println!(
                    "  - {} (inode: {}, type: {:?})",
                    entry.name, entry.ino, entry.kind
                );
            }
            println!("✅ Detected existing data - persistence is working!");
        } else {
            println!("This is a new filesystem");
        }

        println!("Creating VFS instance...");
        let vfs = Arc::new(
            VfsV2::new(layout, store, meta)
                .await
                .expect("Failed to create VFS"),
        );
        println!("VFS instance created successfully");

        // Get current user uid/gid
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        println!("Mounting filesystem...");

        // Check if mount point is empty
        if let Ok(entries) = std::fs::read_dir(&mount_point) {
            let count = entries.count();
            if count > 0 {
                eprintln!(
                    "Warning: Mount point {} is not empty (has {} entries)",
                    mount_point.display(),
                    count
                );
                eprintln!("This may indicate the filesystem is already mounted.");
                eprintln!("Try: fusermount -u {}", mount_point.display());
                return Err("Mount point not empty".into());
            }
        }

        let handle = mount_vfs_v2(vfs, &mount_point, uid, gid)
            .await
            .map_err(|e| format!("Failed to mount filesystem: {}", e))?;

        println!(
            "✅ SlayerFS successfully mounted at: {}",
            mount_point.display()
        );

        println!();
        println!("=== How to test persistence ===");
        println!("1. In another terminal, create some files:");
        println!("   echo 'hello world' > {}/test.txt", mount_point.display());
        println!("   mkdir {}/testdir", mount_point.display());
        println!("   ls -la {}", mount_point.display());
        println!();
        println!("2. Press Ctrl+C to stop this program");
        println!();
        println!("3. Restart the program with the SAME database:");
        println!(
            "   {} --db {} --mount {} --storage {}",
            program_name,
            db_path.display(),
            mount_point.display(),
            backend_storage.display()
        );
        println!();
        println!("4. Your files should still be there!");
        println!();
        println!("Press Ctrl+C to exit and unmount...");

        signal::ctrl_c().await?;
        println!("\n🔄 Unmounting filesystem...");

        handle.unmount().await?;
        println!("✅ Filesystem unmounted successfully");
        println!();
        println!("💡 Tip: Your data is persisted in:");
        println!("   Database: {}", db_path.display());
        println!("   Storage: {}", backend_storage.display());
        println!();
        println!("Run the same command again to verify persistence!");

        Ok(())
    }

    #[cfg(not(target_os = "linux"))]
    {
        eprintln!("This demo requires Linux with FUSE support");
        std::process::exit(2);
    }
}
