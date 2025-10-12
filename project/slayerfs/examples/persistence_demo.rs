use clap::Parser;
use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::fuse::mount::mount_vfs_v2;
use slayerfs::meta::config::{Config, DatabaseType};
use slayerfs::meta::database_store::DatabaseMetaStore;
use slayerfs::meta::etcd_store::EtcdMetaStore;
use slayerfs::meta::store::MetaStore;
use slayerfs::vfs::fs::Vfs;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Configuration file path (e.g. sqlite.yml, etcd.yml, pg.yml)
    #[arg(short, long)]
    config: PathBuf,

    /// Mount point path
    #[arg(short, long, default_value = "/tmp/mount")]
    mount: PathBuf,

    /// Backend storage path
    #[arg(short, long, default_value = "/tmp/storage")]
    storage: PathBuf,
}

/// Process config file and adjust SQLite path to absolute path
fn process_config_for_backend(
    config_content: &str,
    meta_dir: &std::path::Path,
) -> Result<String, Box<dyn std::error::Error>> {
    let config: serde_yaml::Value = serde_yaml::from_str(config_content)?;

    if let Some(database) = config.get("database") {
        if let Some(db_type) = database.get("type").and_then(|t| t.as_str()) {
            match db_type {
                "sqlite" => {
                    let db_path = meta_dir.join("metadata.db");
                    let sqlite_url = format!("sqlite://{}?mode=rwc", db_path.display());

                    let processed_config = format!(
                        r#"database:
  type: sqlite
  url: "{}"
"#,
                        sqlite_url
                    );

                    println!("📁 SQLite database path: {}", db_path.display());
                    Ok(processed_config)
                }
                "postgres" => {
                    println!("🐘 Using PostgreSQL database backend");
                    Ok(config_content.to_string())
                }
                "etcd" => {
                    println!("🔑 Using etcd distributed backend");
                    Ok(config_content.to_string())
                }
                _ => Err(format!("Unsupported database type: {}", db_type).into()),
            }
        } else {
            Err("Missing database.type field in config file".into())
        }
    } else {
        Err("Missing database configuration in config file".into())
    }
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

        let config_file = args.config;
        let mount_point = args.mount;
        let backend_storage = args.storage;

        println!("=== SlayerFS Persistence Demo (Vfs) ===");
        println!("📋 Config file: {}", config_file.display());
        println!("💾 Data storage: {}", backend_storage.display());
        println!("📂 Mount point: {}", mount_point.display());
        println!();

        // Check if config file exists
        if !config_file.exists() {
            eprintln!(
                "❌ Error: Config file {} does not exist",
                config_file.display()
            );
            eprintln!();
            eprintln!("Please create a config file or use existing ones:");
            eprintln!("  sqlite.yml   # SQLite database backend");
            eprintln!("  etcd.yml     # etcd distributed backend");
            eprintln!("  pg.yml       # PostgreSQL backend");
            std::process::exit(1);
        }

        // Create directories
        std::fs::create_dir_all(&mount_point).map_err(|e| {
            format!(
                "Cannot create mount point directory {}: {}",
                mount_point.display(),
                e
            )
        })?;
        std::fs::create_dir_all(&backend_storage).map_err(|e| {
            format!(
                "Cannot create storage directory {}: {}",
                backend_storage.display(),
                e
            )
        })?;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = std::fs::Permissions::from_mode(0o755);
            std::fs::set_permissions(&mount_point, permissions)?;
        }

        println!("🚀 Starting SlayerFS...");
        println!("Backend storage location: {}", backend_storage.display());

        // Prepare object backend
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(&backend_storage));
        let store = ObjectBlockStore::new(client);

        // Process config file
        println!("📖 Reading config file: {}", config_file.display());
        let config_content = std::fs::read_to_string(&config_file)
            .map_err(|e| format!("Cannot read config file: {}", e))?;

        let meta_config_dir = backend_storage.join(".slayerfs");
        std::fs::create_dir_all(&meta_config_dir)?;

        let target_config_path = meta_config_dir.join("slayerfs.yml");
        let processed_config = process_config_for_backend(&config_content, &meta_config_dir)?;
        std::fs::write(&target_config_path, processed_config)?;

        // Initialize metadata store and mount
        println!("🔧 Initializing metadata storage...");
        let config = Config::from_file(&target_config_path)
            .map_err(|e| format!("Failed to load config file: {}", e))?;

        // Get current user uid/gid
        let uid = unsafe { libc::getuid() };
        let gid = unsafe { libc::getgid() };

        // Match on database type and create appropriate VFS
        match &config.database.db_config {
            DatabaseType::Sqlite { .. } | DatabaseType::Postgres { .. } => {
                run_with_database_backend(
                    config,
                    layout,
                    store,
                    &mount_point,
                    uid,
                    gid,
                    &program_name,
                    &config_file,
                    &backend_storage,
                )
                .await?;
            }
            DatabaseType::Etcd { .. } => {
                run_with_etcd_backend(
                    config,
                    layout,
                    store,
                    &mount_point,
                    uid,
                    gid,
                    &program_name,
                    &config_file,
                    &backend_storage,
                )
                .await?;
            }
        }

        Ok(())
    }
}

/// Run with DatabaseMetaStore (SQLite or PostgreSQL)
async fn run_with_database_backend(
    config: Config,
    layout: ChunkLayout,
    store: ObjectBlockStore<LocalFsBackend>,
    mount_point: &PathBuf,
    uid: u32,
    gid: u32,
    program_name: &str,
    config_file: &PathBuf,
    backend_storage: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let meta = DatabaseMetaStore::from_config(config)
        .await
        .map_err(|e| format!("Failed to initialize metadata storage: {}", e))?;

    println!("🔍 Verifying metadata storage status...");
    let root_ino = meta.root_ino();
    let root_entries = meta
        .readdir(root_ino)
        .await
        .map_err(|e| format!("readdir failed: {}", e))?;

    print_storage_status(&root_entries);

    println!("🔨 Creating VFS instance...");
    let vfs = Arc::new(
        Vfs::new(layout, store, meta)
            .await
            .expect("Failed to create VFS"),
    );
    println!("✅ VFS instance created successfully");

    mount_and_wait(
        vfs,
        mount_point,
        uid,
        gid,
        program_name,
        config_file,
        backend_storage,
    )
    .await
}

/// Run with EtcdMetaStore
async fn run_with_etcd_backend(
    config: Config,
    layout: ChunkLayout,
    store: ObjectBlockStore<LocalFsBackend>,
    mount_point: &PathBuf,
    uid: u32,
    gid: u32,
    program_name: &str,
    config_file: &PathBuf,
    backend_storage: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let meta = EtcdMetaStore::from_config(config)
        .await
        .map_err(|e| format!("Failed to initialize metadata storage: {}", e))?;

    println!("🔍 Verifying metadata storage status...");
    let root_ino = meta.root_ino();
    let root_entries = meta
        .readdir(root_ino)
        .await
        .map_err(|e| format!("readdir failed: {}", e))?;

    print_storage_status(&root_entries);

    println!("🔨 Creating VFS instance...");
    let vfs = Arc::new(
        Vfs::new(layout, store, meta)
            .await
            .expect("Failed to create VFS"),
    );
    println!("✅ VFS instance created successfully");

    mount_and_wait(
        vfs,
        mount_point,
        uid,
        gid,
        program_name,
        config_file,
        backend_storage,
    )
    .await
}

/// Print storage status
fn print_storage_status(root_entries: &[slayerfs::meta::store::DirEntry]) {
    println!("Root directory contains {} entries", root_entries.len());
    if !root_entries.is_empty() {
        println!("📁 Existing entries:");
        for entry in root_entries {
            println!(
                "  - {} (inode: {}, type: {:?})",
                entry.name, entry.ino, entry.kind
            );
        }
        println!("✅ Detected existing data - persistence is working!");
    } else {
        println!("🆕 This is a new filesystem");
    }
}

/// Mount filesystem and wait for signal
async fn mount_and_wait<S, M>(
    vfs: Arc<Vfs<S, M>>,
    mount_point: &PathBuf,
    uid: u32,
    gid: u32,
    program_name: &str,
    config_file: &PathBuf,
    backend_storage: &PathBuf,
) -> Result<(), Box<dyn std::error::Error>>
where
    S: slayerfs::chuck::store::BlockStore + Send + Sync + 'static,
    M: MetaStore + Send + Sync + 'static,
{
    println!("🔗 Mounting filesystem...");

    // Check if mount point is a directory
    if std::fs::metadata(mount_point)
        .map(|m| !m.is_dir())
        .unwrap_or(false)
    {
        return Err(format!("Mount point {} is not a directory", mount_point.display()).into());
    }

    // Check if mount point is empty
    if let Ok(entries) = std::fs::read_dir(mount_point) {
        let count = entries.count();
        if count > 0 {
            eprintln!(
                "⚠️  Warning: Mount point {} is not empty (has {} entries)",
                mount_point.display(),
                count
            );
            eprintln!("This may indicate the filesystem is already mounted.");
            eprintln!("Try: fusermount -u {}", mount_point.display());
            return Err("Mount point not empty".into());
        }
    }

    let handle = mount_vfs_v2(vfs, mount_point, uid, gid)
        .await
        .map_err(|e| format!("Failed to mount filesystem: {}", e))?;

    println!();
    println!(
        "✅ SlayerFS successfully mounted at: {}",
        mount_point.display()
    );
    println!();
    println!("=== How to test the filesystem ===");
    println!("In another terminal, try:");
    println!("  ls -la {}", mount_point.display());
    println!("  echo 'hello world' > {}/test.txt", mount_point.display());
    println!("  cat {}/test.txt", mount_point.display());
    println!("  mkdir {}/testdir", mount_point.display());
    println!();
    println!("=== How to test persistence ===");
    println!("1. Create some files and directories");
    println!("2. Press Ctrl+C to stop the program");
    println!("3. Start the program again with same parameters:");
    println!(
        "   {} --config {} --mount {} --storage {}",
        program_name,
        config_file.display(),
        mount_point.display(),
        backend_storage.display()
    );
    println!("4. Check if your data is still there!");
    println!();
    println!("=== Backend switching test ===");
    println!("You can test different backends by using different config files:");
    println!(
        "  {} --config sqlite.yml --mount {} --storage {}",
        program_name,
        mount_point.display(),
        backend_storage.display()
    );
    println!(
        "  {} --config etcd.yml --mount {} --storage {}",
        program_name,
        mount_point.display(),
        backend_storage.display()
    );
    println!(
        "  {} --config pg.yml --mount {} --storage {}",
        program_name,
        mount_point.display(),
        backend_storage.display()
    );
    println!();
    println!("Press Ctrl+C to exit and unmount...");

    signal::ctrl_c().await?;
    println!("\n🔄 Unmounting filesystem...");

    handle.unmount().await?;
    println!("✅ Filesystem unmounted successfully");
    println!();
    println!("💡 Tips:");
    println!("  - Your data is persisted in the metadata backend");
    println!("  - Re-run the same command to verify persistence");
    println!("  - Try different config files to test multi-backend support");
    println!();
    println!("Restart command:");
    println!(
        "  {} --config {} --mount {} --storage {}",
        program_name,
        config_file.display(),
        mount_point.display(),
        backend_storage.display()
    );

    Ok(())
}
