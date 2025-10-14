use clap::Parser;
use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::fuse::mount::mount_vfs_unprivileged;
use slayerfs::meta::MetaStore;
use slayerfs::meta::types::Inode;
use slayerfs::vfs::fs::VFS;
use std::path::PathBuf;
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about = "SlayerFS universal config-driven demo - supports all backends", long_about = None)]
struct Args {
    /// Configuration file path (supports sqlite.yml, pg.yml, etcd.yml, or custom gRPC config)
    #[arg(short, long)]
    config: PathBuf,

    /// Mount point path
    #[arg(short, long, default_value = "/tmp/mount")]
    mount: PathBuf,

    /// Backend storage path (for data chunks)
    #[arg(short, long, default_value = "/tmp/slayerfs-data")]
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
            .unwrap_or_else(|| "config_driven_demo".to_string());

        let config_file = args.config;
        let mount_point = args.mount;
        let backend_storage = args.storage;

        println!("=== SlayerFS Config-Driven Universal Demo ===");
        println!("Configuration: {}", config_file.display());
        println!("Data storage: {}", backend_storage.display());
        println!("Mount point: {}", mount_point.display());
        println!();

        // 检查配置文件是否存在
        if !config_file.exists() {
            eprintln!(
                "❌ Error: Config file {} does not exist",
                config_file.display()
            );
            eprintln!();
            eprintln!("📋 Available configuration examples:");
            eprintln!("  sqlite.yml      - Local SQLite database");
            eprintln!("  pg.yml          - PostgreSQL database");
            eprintln!("  etcd.yml        - Distributed Etcd cluster");
            eprintln!("  grpc.yml        - Remote gRPC MetaServer");
            eprintln!();
            eprintln!("📝 Example gRPC config (grpc.yml):");
            eprintln!(
                r#"metadata:
  backend_type: grpc
  endpoint: "http://localhost:50051"
  timeout_secs: 30
  tls: false

cache:
  attr_capacity: 10000
  attr_ttl_secs: 60
  dentry_capacity: 50000
  dentry_ttl_secs: 120

logging:
  level: info

storage:
  block_size: 4194304
  chunk_size: 67108864"#
            );
            eprintln!();
            std::process::exit(1);
        }

        // 创建必要的目录
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

        println!("Starting SlayerFS...");
        println!("Data chunks location: {}", backend_storage.display());

        // 创建数据存储层
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(&backend_storage));
        let store = ObjectBlockStore::new(client);

        // 读取配置文件
        println!("Loading configuration from {}...", config_file.display());
        let config = slayerfs::meta::config::Config::from_file(&config_file)
            .map_err(|e| format!("Failed to load config file: {}", e))?;

        // 打印配置信息
        println!();
        println!("📊 Configuration Summary:");
        println!("─────────────────────────────────────────────");

        use slayerfs::meta::config::{DatabaseType, MetadataBackend};
        match &config.metadata.backend {
            MetadataBackend::Database { config: db_config } => {
                match &db_config.db_config {
                    DatabaseType::Sqlite { url } => {
                        println!("Backend Type: SQLite Database");
                        println!("  Storage: Local file");
                        println!("  Path: {}", url);
                    }
                    DatabaseType::Postgres { url } => {
                        println!("Backend Type: PostgreSQL Database");
                        // 隐藏密码
                        let masked_url = if url.contains('@') {
                            let parts: Vec<&str> = url.split('@').collect();
                            format!("***@{}", parts.get(1).unwrap_or(&""))
                        } else {
                            url.to_string()
                        };
                        println!("  Connection: {}", masked_url);
                    }
                    DatabaseType::Etcd { urls } => {
                        println!("Backend Type: Etcd Cluster");
                        println!("  Endpoints: {}", urls.join(", "));
                        println!("  Mode: Distributed");
                    }
                }
            }
            MetadataBackend::Grpc {
                endpoint,
                timeout_secs,
                tls,
            } => {
                println!("Backend Type: gRPC Remote");
                println!("  MetaServer: {}", endpoint);
                println!("  Timeout: {}s", timeout_secs);
                println!("  TLS: {}", if *tls { "enabled" } else { "disabled" });
                println!("  Mode: Client-Server");
            }
        }

        println!();
        println!("Cache Configuration:");
        println!(
            "  Attr Cache: {} entries, {} sec TTL",
            config.cache.attr_cache_size, config.cache.attr_cache_ttl_secs
        );
        println!(
            "  Dentry Cache: {} entries, {} sec TTL",
            config.cache.dentry_cache_size, config.cache.dentry_cache_ttl_secs
        );
        println!("─────────────────────────────────────────────");
        println!();

        // 使用工厂创建元数据存储
        println!("Connecting to metadata backend...");
        let meta = slayerfs::meta::factory::MetaStoreFactory::create_from_config(config)
            .await
            .map_err(|e| format!("Failed to initialize metadata backend: {}", e))?;

        println!("✅ Connected to metadata backend successfully!");

        // 初始化元数据存储
        println!("Initializing metadata storage...");
        meta.initialize()
            .await
            .map_err(|e| format!("Failed to initialize metadata storage: {}", e))?;

        // 验证元数据状态
        println!("Verifying metadata storage status...");
        let root_entries = meta
            .readdir(Inode(1))
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

        // 创建 VFS
        println!("Creating VFS instance...");
        let fs = VFS::new(layout, store, meta).await.expect("create VFS");
        println!("VFS instance created successfully");

        // 挂载文件系统
        println!("Mounting filesystem...");

        if std::fs::metadata(&mount_point)
            .map(|m| !m.is_dir())
            .unwrap_or(false)
        {
            return Err(format!("Mount point {} is not a directory", mount_point.display()).into());
        }

        if let Ok(entries) = std::fs::read_dir(&mount_point) {
            let count = entries.count();
            if count > 0 {
                eprintln!(
                    "Warning: Mount point {} is not empty, may already be mounted",
                    mount_point.display()
                );
                eprintln!("Please unmount first or use an empty directory");
                eprintln!("Try: fusermount -u {}", mount_point.display());
                return Err("Mount point not empty".into());
            }
        }

        let handle = mount_vfs_unprivileged(fs, &mount_point)
            .await
            .map_err(|e| format!("Failed to mount filesystem: {}", e))?;

        println!(
            "✅ SlayerFS successfully mounted at: {}",
            mount_point.display()
        );
        println!(
            "Mount point permissions: {:?}",
            std::fs::metadata(&mount_point)?.permissions()
        );

        println!();
        println!("================================================");
        println!("🎯 You can now test file operations in another terminal:");
        println!("================================================");
        println!("  ls -la {}", mount_point.display());
        println!(
            "  echo 'Hello SlayerFS!' > {}/test.txt",
            mount_point.display()
        );
        println!("  cat {}/test.txt", mount_point.display());
        println!("  mkdir {}/testdir", mount_point.display());
        println!(
            "  dd if=/dev/zero of={}/large.bin bs=1M count=100",
            mount_point.display()
        );
        println!();
        println!("================================================");
        println!("🔄 Persistence testing:");
        println!("================================================");
        println!("  1. Create some files and directories in the mount point");
        println!("  2. Press Ctrl+C to stop this program");
        println!("  3. Start the program again with the same config:");
        println!(
            "     {} --config {} --mount {} --storage {}",
            program_name,
            config_file.display(),
            mount_point.display(),
            backend_storage.display()
        );
        println!("  4. Check if your data is still there!");
        println!();
        println!("================================================");
        println!("🔀 Backend switching test:");
        println!("================================================");
        println!("  SQLite (local):");
        println!(
            "    {} --config sqlite.yml --mount /tmp/mount1 --storage /tmp/storage1",
            program_name
        );
        println!();
        println!("  PostgreSQL (centralized):");
        println!(
            "    {} --config pg.yml --mount /tmp/mount2 --storage /tmp/storage2",
            program_name
        );
        println!();
        println!("  Etcd (distributed):");
        println!(
            "    {} --config etcd.yml --mount /tmp/mount3 --storage /tmp/storage3",
            program_name
        );
        println!();
        println!("  gRPC (client-server):");
        println!(
            "    {} --config grpc.yml --mount /tmp/mount4 --storage /tmp/storage4",
            program_name
        );
        println!();
        println!("================================================");
        println!("📊 Architecture:");
        println!("================================================");
        println!("  Config File → MetaStoreFactory → Backend");
        println!("  - sqlite.yml  → DatabaseMetaStore → SQLite");
        println!("  - pg.yml      → DatabaseMetaStore → PostgreSQL");
        println!("  - etcd.yml    → EtcdMetaStore → Etcd Cluster");
        println!("  - grpc.yml    → RemoteMetaStore → MetaServer");
        println!();
        println!("Press Ctrl+C to exit and unmount filesystem...");

        signal::ctrl_c().await?;
        println!("\n🛑 Received shutdown signal, unmounting filesystem...");

        handle.unmount().await?;
        println!("✅ Filesystem unmounted successfully");
        println!();
        println!("💡 Tips:");
        println!("  - Simply change the config file to switch backends");
        println!("  - All backends share the same VFS and FUSE interface");
        println!(
            "  - Restart: {} --config {} --mount {} --storage {}",
            program_name,
            config_file.display(),
            mount_point.display(),
            backend_storage.display()
        );

        Ok(())
    }
}
