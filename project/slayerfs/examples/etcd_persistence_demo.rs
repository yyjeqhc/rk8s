use clap::Parser;
use slayerfs::cadapter::client::ObjectClient;
use slayerfs::cadapter::localfs::LocalFsBackend;
use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::chuck::store::ObjectBlockStore;
use slayerfs::fuse::mount::mount_vfs_unprivileged;
use slayerfs::meta::MetaStore;
use slayerfs::meta::config::{
    Config, DatabaseConfig, DatabaseType, MetadataBackend, MetadataConfig,
};
use slayerfs::meta::factory::MetaStoreFactory;
use slayerfs::meta::types::Inode;
use slayerfs::vfs::fs::VFS;
use std::path::PathBuf;
use tokio::signal;

#[derive(Parser)]
#[command(author, version, about = "SlayerFS FUSE mount demo with Etcd backend", long_about = None)]
struct Args {
    /// Etcd endpoints (comma-separated, e.g., http://localhost:2379,http://localhost:2380)
    #[arg(short, long, default_value = "http://localhost:2379")]
    etcd_endpoints: String,

    /// Mount point path
    #[arg(short, long, default_value = "/tmp/mount")]
    mount: PathBuf,

    /// Backend storage path (for data chunks)
    #[arg(short, long, default_value = "/tmp/slayerfs-etcd-data")]
    storage: PathBuf,

    /// Etcd key prefix
    #[arg(short, long, default_value = "/slayerfs")]
    prefix: String,
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
            .unwrap_or_else(|| "etcd_persistence_demo".to_string());

        let etcd_endpoints: Vec<String> = args
            .etcd_endpoints
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mount_point = args.mount;
        let backend_storage = args.storage;
        let etcd_prefix = args.prefix;

        println!("=== SlayerFS Etcd Backend Demo ===");
        println!("Etcd endpoints: {:?}", etcd_endpoints);
        println!("Etcd prefix: {}", etcd_prefix);
        println!("Data storage: {}", backend_storage.display());
        println!("Mount point: {}", mount_point.display());
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

        println!("Starting SlayerFS with Etcd backend...");
        println!("Backend storage location: {}", backend_storage.display());

        // 创建数据存储层
        let layout = ChunkLayout::default();
        let client = ObjectClient::new(LocalFsBackend::new(&backend_storage));
        let store = ObjectBlockStore::new(client);

        println!("Connecting to Etcd cluster...");

        // 创建 Etcd 配置
        let config = Config {
            metadata: MetadataConfig {
                backend: MetadataBackend::Database {
                    config: DatabaseConfig {
                        db_config: DatabaseType::Etcd {
                            urls: etcd_endpoints.clone(),
                        },
                    },
                },
            },
            cache: Default::default(),
            logging: Default::default(),
            storage: Default::default(),
        };

        // 创建元数据存储
        println!("Initializing Etcd metadata store...");
        let meta = MetaStoreFactory::create_from_config(config)
            .await
            .map_err(|e| format!("Failed to create Etcd metadata store: {}", e))?;

        println!("✅ Connected to Etcd successfully!");

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
            println!("✅ Detected existing data - Etcd persistence is working!");
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
            "  echo 'Hello from Etcd!' > {}/test.txt",
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
        println!("🔄 Distributed persistence testing:");
        println!("================================================");
        println!("  1. Create some files and directories in the mount point");
        println!("  2. Press Ctrl+C to stop this client");
        println!("  3. Start another client from ANY machine:");
        println!(
            "     {} --etcd-endpoints {} --mount /tmp/mount2 --storage /tmp/storage2",
            program_name, args.etcd_endpoints
        );
        println!("  4. Your data is stored in Etcd and accessible from anywhere!");
        println!("  5. Multiple clients can mount simultaneously across different hosts");
        println!();
        println!("================================================");
        println!("🌐 Multi-node cluster testing:");
        println!("================================================");
        println!("  Node 1 (192.168.1.10):");
        println!(
            "    {} --etcd-endpoints {} --mount /mnt/slayerfs --storage /data/node1",
            program_name, args.etcd_endpoints
        );
        println!();
        println!("  Node 2 (192.168.1.11):");
        println!(
            "    {} --etcd-endpoints {} --mount /mnt/slayerfs --storage /data/node2",
            program_name, args.etcd_endpoints
        );
        println!();
        println!("  Node 3 (192.168.1.12):");
        println!(
            "    {} --etcd-endpoints {} --mount /mnt/slayerfs --storage /data/node3",
            program_name, args.etcd_endpoints
        );
        println!();
        println!("  Test distributed access:");
        println!("    # On Node 1");
        println!("    echo 'from node 1' > /mnt/slayerfs/shared.txt");
        println!();
        println!("    # On Node 2");
        println!("    cat /mnt/slayerfs/shared.txt  # Should see 'from node 1'");
        println!();
        println!("    # On Node 3");
        println!("    ls -la /mnt/slayerfs/  # Should see shared.txt");
        println!();
        println!("================================================");
        println!("🔍 Etcd inspection:");
        println!("================================================");
        println!("  # View all SlayerFS keys in Etcd");
        println!("  etcdctl get --prefix {}/", etcd_prefix);
        println!();
        println!("  # Count total keys");
        println!(
            "  etcdctl get --prefix {}/ --keys-only | wc -l",
            etcd_prefix
        );
        println!();
        println!("  # Watch for changes in real-time");
        println!("  etcdctl watch --prefix {}/", etcd_prefix);
        println!();
        println!("================================================");
        println!("📊 Architecture:");
        println!("================================================");
        println!("  Client A (FUSE) ──┐");
        println!("  Client B (FUSE) ──┼──> Etcd Cluster (Distributed Metadata)");
        println!("  Client C (FUSE) ──┘");
        println!();
        println!(
            "  Data: Local to each client ({}/)",
            backend_storage.display()
        );
        println!("  Metadata: Shared in Etcd ({})", etcd_prefix);
        println!();
        println!("================================================");
        println!("⚡ Benefits of Etcd backend:");
        println!("================================================");
        println!("  ✅ Distributed consensus (Raft protocol)");
        println!("  ✅ High availability with multi-node cluster");
        println!("  ✅ Automatic leader election and failover");
        println!("  ✅ Watch API for real-time updates");
        println!("  ✅ Strong consistency guarantees");
        println!("  ✅ Cross-datacenter replication possible");
        println!();
        println!("Press Ctrl+C to exit and unmount filesystem...");

        signal::ctrl_c().await?;
        println!("\n🛑 Received shutdown signal, unmounting filesystem...");

        handle.unmount().await?;
        println!("✅ Filesystem unmounted successfully");
        println!();
        println!("💡 Tips:");
        println!(
            "  - Metadata is stored in Etcd cluster at prefix: {}",
            etcd_prefix
        );
        println!("  - Data chunks remain local for performance");
        println!("  - Use Etcd's watch feature to monitor filesystem changes");
        println!(
            "  - Restart command: {} --etcd-endpoints {} --mount {} --storage {}",
            program_name,
            args.etcd_endpoints,
            mount_point.display(),
            backend_storage.display()
        );
        println!("  - Check Etcd cluster health: etcdctl endpoint health");

        Ok(())
    }
}
