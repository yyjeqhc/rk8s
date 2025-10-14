mod cadapter;
mod chuck;
mod daemon;
mod fuse;
mod meta;
mod vfs;

use crate::vfs::demo;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Initialize tracing
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "slayerfs=info,warn".into()),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    tracing::info!("SlayerFS starting...");

    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("demo-localfs") => {
            let dir = match args.next() {
                Some(p) => p,
                None => {
                    tracing::error!("Missing directory argument");
                    eprintln!("Usage: slayerfs demo-localfs <dir>");
                    std::process::exit(2);
                }
            };
            match demo::e2e_localfs_demo(dir).await {
                Ok(()) => {
                    tracing::info!("demo-localfs completed successfully");
                    println!("demo-localfs: OK");
                }
                Err(e) => {
                    tracing::error!(error = %e, "demo-localfs failed");
                    eprintln!("demo-localfs failed: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            tracing::info!("No command specified, showing help");
            println!("Hello, I'm SlayerFS!\nUsage:\n  slayerfs demo-localfs <dir>");
        }
    }
}
