use std::time::Instant;

use anyhow::Result;
use xline_client::{Client, ClientOptions};

/// Long-running KV benchmark demonstrating that KV operations bypass H3Channel.
///
/// KV put/get/delete go through `curp_client.propose()` → CURP QuicChannel,
/// NOT through H3Channel. This benchmark measures per-request KV latency in a
/// long-running client to show that H3 session caching is not on the hot path
/// for KV workloads.
///
/// Usage:
///   RUST_LOG=xlinerpc=debug cargo run --example client_kv_benchmark -- --requests 100
///
/// Env vars:
///   RUST_LOG=xlinerpc=debug    Enable debug logging
///
/// Requires a running 3-node QUIC cluster with /etc/hosts entries
/// and fixtures/ca.crt available.
#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let requests: usize = std::env::args()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|w| w[0] == "--requests")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(100);

    let curp_members = [
        "https://server0:2379",
        "https://server1:2381",
        "https://server2:2383",
    ];

    let ca_cert_pem = include_bytes!("../../../fixtures/ca.crt").to_vec();
    let options = ClientOptions::default().with_quic_peer_ca_cert(ca_cert_pem);

    let client = Client::connect(curp_members, options).await?;
    let kv = client.kv_client();

    kv.put("warmup", "0", None).await?;
    let _ = kv.range("warmup", None).await?;

    let mut latencies = Vec::with_capacity(requests);

    for i in 0..requests {
        let key = format!("bench-{i}");
        let value = format!("value-{i}");

        let start = Instant::now();
        kv.put(key.clone(), value.clone(), None).await?;
        let resp = kv.range(key.clone(), None).await?;
        let elapsed = start.elapsed();

        assert_eq!(resp.kvs.len(), 1);
        assert_eq!(resp.kvs[0].value, value.as_bytes());
        latencies.push(elapsed);
    }

    for i in 0..requests {
        let key = format!("bench-{i}");
        let _ = kv.delete(key, None).await?;
    }

    latencies.sort();
    let total: std::time::Duration = latencies.iter().sum();
    let avg = total / latencies.len() as u32;
    let p50 = latencies[latencies.len() / 2];
    let p95 = latencies[(latencies.len() * 95) / 100];
    let p99 = latencies[(latencies.len() * 99) / 100];
    let min = latencies[0];
    let max = latencies[latencies.len() - 1];

    println!("--- KV Benchmark (CURP path, not H3Channel) ---");
    println!("requests:    {requests}");
    println!("total:       {:.3}s", total.as_secs_f64());
    println!("avg:         {:.3}ms", avg.as_secs_f64() * 1000.0);
    println!("p50:         {:.3}ms", p50.as_secs_f64() * 1000.0);
    println!("p95:         {:.3}ms", p95.as_secs_f64() * 1000.0);
    println!("p99:         {:.3}ms", p99.as_secs_f64() * 1000.0);
    println!("min:         {:.3}ms", min.as_secs_f64() * 1000.0);
    println!("max:         {:.3}ms", max.as_secs_f64() * 1000.0);
    println!(
        "throughput:  {:.1} req/s",
        requests as f64 / total.as_secs_f64()
    );

    Ok(())
}
