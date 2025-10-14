use std::sync::Arc;
use tokio::signal;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber::{EnvFilter, fmt, prelude::*};

use slayerfs::meta::factory::MetaStoreFactory;
use slayerfs::meta::{MetaServer, config::Config};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化 tracing
    tracing_subscriber::registry()
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .with(fmt::layer().with_target(true).with_thread_ids(true))
        .init();

    info!("Starting SlayerFS MetaServer");

    // 加载配置
    let config_path =
        std::env::var("SLAYERFS_CONFIG").unwrap_or_else(|_| "slayerfs.yml".to_string());

    let config = Config::from_file(&config_path)?;
    info!("Loaded configuration from {}", config_path);

    // 创建 MetaStore
    let store = MetaStoreFactory::create_from_config(config).await?;
    info!("Created metadata store");

    // 初始化 MetaStore
    store.initialize().await?;
    info!("Initialized metadata store");

    // 创建 gRPC server
    let meta_server = MetaServer::new(Arc::clone(&store));
    let svc = meta_server.into_service();

    // 获取监听地址
    let addr = std::env::var("SLAYERFS_LISTEN")
        .unwrap_or_else(|_| "0.0.0.0:50051".to_string())
        .parse()?;

    info!("MetaServer listening on {}", addr);

    // 启动 gRPC server
    Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, async {
            signal::ctrl_c().await.expect("Failed to listen for Ctrl+C");
            info!("Received shutdown signal");
        })
        .await?;

    info!("MetaServer shutdown complete");
    Ok(())
}
