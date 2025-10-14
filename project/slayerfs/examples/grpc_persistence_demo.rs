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
#[command(author, version, about = "SlayerFS FUSE mount demo with remote gRPC MetaServer", long_about = None)]
struct Args {
    /// gRPC MetaServer endpoint (e.g., http://localhost:50051)
    #[arg(short, long, default_value = "http://localhost:50051")]
    endpoint: String,

    /// Mount point path
    #[arg(short, long, default_value = "/tmp/mount")]
    mount: PathBuf,

    /// Backend storage path (for data chunks)
    #[arg(short, long, default_value = "/tmp/slayerfs-grpc-data")]
    storage: PathBuf,

    /// Connection timeout in seconds
    #[arg(short, long, default_value = "30")]
    timeout: u64,
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
            .unwrap_or_else(|| "grpc_persistence_demo".to_string());

        let endpoint = args.endpoint;
        let mount_point = args.mount;
        let backend_storage = args.storage;
        let timeout_secs = args.timeout;

        println!("=== SlayerFS gRPC Client Demo ===");
        println!("gRPC MetaServer: {}", endpoint);
        println!("Data storage: {}", backend_storage.display());
        println!("Mount point: {}", mount_point.display());
        println!("Timeout: {}s", timeout_secs);
        println!();

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
        println!("Backend storage location: {}", backend_storage.display());

        // 创建数据存储层
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(&backend_storage));
        let store = ObjectBlockStore::new(client);

        println!("Connecting to gRPC MetaServer at {}...", endpoint);

        // 创建 gRPC 配置
        use slayerfs::meta::config::{Config, MetadataBackend, MetadataConfig};

        let config = Config {
            metadata: MetadataConfig {
                backend: MetadataBackend::Grpc {
                    endpoint: endpoint.clone(),
                    timeout_secs,
                    tls: false,
                },
            },
            cache: Default::default(),
            logging: Default::default(),
            storage: Default::default(),
        };

        // 使用工厂创建元数据存储
        use slayerfs::meta::factory::MetaStoreFactory;
        let meta = MetaStoreFactory::create_from_config(config)
            .await
            .map_err(|e| format!("Failed to connect to MetaServer: {}", e))?;

        println!("Connected to MetaServer successfully!");

        // 初始化元数据存储
        println!("Initializing metadata storage...");
        meta.initialize()
            .await
            .map_err(|e| format!("Failed to initialize metadata storage: {}", e))?;

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
            println!("✅ Detected existing data - remote persistence is working!");
        } else {
            println!("This is a new filesystem");
        }

        println!("Creating VFS instance...");
        let fs = VFS::new(layout, store, meta).await.expect("create VFS");
        println!("VFS instance created successfully");

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
            "  echo 'Hello from gRPC!' > {}/test.txt",
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
        println!("🔄 Remote persistence testing:");
        println!("================================================");
        println!("  1. Create some files and directories in the mount point");
        println!("  2. Press Ctrl+C to stop this client");
        println!("  3. Start another client (even from a different machine):");
        println!(
            "     {} --endpoint {} --mount /tmp/mount2 --storage /tmp/storage2",
            program_name, endpoint
        );
        println!("  4. Check if your data is shared across clients!");
        println!("  5. Multiple clients can mount simultaneously and see each other's changes");
        println!();
        println!("================================================");
        println!("🌐 Multi-client testing:");
        println!("================================================");
        println!("  Terminal 1:");
        println!(
            "    {} --endpoint {} --mount /tmp/mount1 --storage /tmp/storage1",
            program_name, endpoint
        );
        println!();
        println!("  Terminal 2:");
        println!(
            "    {} --endpoint {} --mount /tmp/mount2 --storage /tmp/storage2",
            program_name, endpoint
        );
        println!();
        println!("  Terminal 3 - Test shared access:");
        println!("    echo 'from client 1' > /tmp/mount1/shared.txt");
        println!("    cat /tmp/mount2/shared.txt  # Should see 'from client 1'");
        println!();
        println!("================================================");
        println!("📊 Architecture:");
        println!("================================================");
        println!("  This Client (FUSE) <--gRPC--> MetaServer <--> Database");
        println!("  Data: {} (local)", backend_storage.display());
        println!("  Metadata: {} (remote)", endpoint);
        println!();
        println!("Press Ctrl+C to exit and unmount filesystem...");

        signal::ctrl_c().await?;
        println!("\n🛑 Received shutdown signal, unmounting filesystem...");

        handle.unmount().await?;
        println!("✅ Filesystem unmounted successfully");
        println!();
        println!("💡 Tips:");
        println!("  - Data is stored remotely on the MetaServer");
        println!("  - Multiple clients can access the same filesystem simultaneously");
        println!(
            "  - Restart command: {} --endpoint {} --mount {} --storage {}",
            program_name,
            endpoint,
            mount_point.display(),
            backend_storage.display()
        );
        println!("  - Check MetaServer logs for connection details");

        Ok(())
    }
}
