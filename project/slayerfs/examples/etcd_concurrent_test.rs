/// Etcd 分布式锁并发测试示例（概念验证）
/// 
/// 这个文件展示了如何为 etcd 分布式锁实现编写并发测试
/// 
/// ⚠️ 注意：这是一个概念性示例，实际的测试应该：
/// 1. 在 tests/ 目录中创建集成测试
/// 2. 使用 testcontainers 或 Docker Compose 启动 etcd
/// 3. 使用 #[tokio::test] 宏
/// 
/// ## 测试场景概览
/// 
/// ### 场景 1: 并发创建同名文件
/// 
/// **目的**: 验证 `atomic_create_with_check()` 的原子性
/// 
/// **步骤**:
/// ```rust
/// // 两个客户端同时创建 "test.txt"
/// let (result_a, result_b) = tokio::join!(
///     client_a.create_file(parent_ino, "test.txt".to_string(), 0o644),
///     client_b.create_file(parent_ino, "test.txt".to_string(), 0o644),
/// );
/// 
/// // 验证: 只有一个成功
/// assert!(result_a.is_ok() ^ result_b.is_ok());
/// 
/// // 验证: 没有孤儿 inode
/// let entries = client_a.readdir(parent_ino).await.unwrap();
/// assert_eq!(entries.len(), 1);
/// ```
/// 
/// **预期结果**:
/// - ✅ 一个成功 (create_revision == 0 的条件满足)
/// - ✅ 一个失败 (AlreadyExists 错误)
/// - ✅ 目录中只有一个文件
/// - ✅ 所有相关 key (forward, reverse) 都存在
/// 
/// ---
/// 
/// ### 场景 2: 并发创建不同文件
/// 
/// **目的**: 验证 CAS 重试机制
/// 
/// **步骤**:
/// ```rust
/// // 10 个客户端同时在同一个目录创建不同文件
/// let tasks: Vec<_> = (0..10).map(|i| {
///     let name = format!("file{}.txt", i);
///     client.create_file(parent_ino, name, 0o644)
/// }).collect();
/// 
/// let results = futures::future::join_all(tasks).await;
/// 
/// // 验证: 全部成功
/// assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 10);
/// ```
/// 
/// **预期结果**:
/// - ✅ 所有文件创建成功
/// - ✅ 父目录 children 包含所有 10 个文件
/// - ✅ CAS 自动重试解决 mod_revision 冲突
/// 
/// ---
/// 
/// ### 场景 3: 并发删除
/// 
/// **目的**: 验证 `atomic_delete_with_check()` 的原子性
/// 
/// **步骤**:
/// ```rust
/// // 创建文件
/// let ino = client_a.create_file(parent, "test.txt".to_string(), 0o644).await.unwrap();
/// 
/// // 两个客户端同时删除
/// let (result_a, result_b) = tokio::join!(
///     client_a.unlink(parent, "test.txt"),
///     client_b.unlink(parent, "test.txt"),
/// );
/// 
/// // 验证: 一个成功，一个失败
/// let successes = [result_a.is_ok(), result_b.is_ok()].iter().filter(|&&x| x).count();
/// assert_eq!(successes, 1);
/// ```
/// 
/// **预期结果**:
/// - ✅ 一个成功 (create_revision > 0 的条件满足)
/// - ✅ 一个失败 (NotFound 错误)
/// - ✅ 所有相关 key 都被删除 (forward, reverse, children)
/// 
/// ---
/// 
/// ### 场景 4: 并发 rename 到相同目标
/// 
/// **目的**: 验证 `atomic_rename()` 的双重检查
/// 
/// **步骤**:
/// ```rust
/// // 创建两个文件
/// client_a.create_file(parent, "file1.txt".to_string(), 0o644).await.unwrap();
/// client_b.create_file(parent, "file2.txt".to_string(), 0o644).await.unwrap();
/// 
/// // 同时 rename 到相同目标
/// let (result_a, result_b) = tokio::join!(
///     client_a.rename(parent, "file1.txt", parent, "target.txt"),
///     client_b.rename(parent, "file2.txt", parent, "target.txt"),
/// );
/// 
/// // 验证: 只有一个成功
/// assert!(result_a.is_ok() ^ result_b.is_ok());
/// ```
/// 
/// **预期结果**:
/// - ✅ 一个成功 (源存在 AND 目标不存在)
/// - ✅ 一个失败 (目标已存在)
/// - ✅ 目录中有 `target.txt` + 失败的源文件
/// 
/// ---
/// 
/// ### 场景 5: CAS 重试压力测试
/// 
/// **目的**: 测试极限并发下的重试机制
/// 
/// **步骤**:
/// ```rust
/// // 50 个客户端同时创建文件
/// let tasks: Vec<_> = (0..50).map(|i| {
///     client.create_file(parent, format!("file{}.txt", i), 0o644)
/// }).collect();
/// 
/// let results = futures::future::join_all(tasks).await;
/// 
/// // 验证: 全部成功
/// assert_eq!(results.iter().filter(|r| r.is_ok()).count(), 50);
/// ```
/// 
/// **预期性能**:
/// - ✅ 平均重试次数 < 3
/// - ✅ 最大重试次数 < 10
/// - ✅ 成功率 > 99%
/// 
/// ---
/// 
/// ### 场景 6: Watch 缓存失效测试
/// 
/// **目的**: 验证 Watch Worker 的实时缓存失效
/// 
/// **步骤**:
/// ```rust
/// // 启动 Watch Worker
/// let (tx, mut rx) = mpsc::channel(100);
/// let watch_worker = EtcdWatchWorker::new(etcd_client.clone(), tx);
/// watch_worker.start().await;
/// 
/// // 客户端 A 创建文件
/// client_a.create_file(parent, "test.txt".to_string(), 0o644).await.unwrap();
/// 
/// // 等待 Watch 事件
/// let event = tokio::time::timeout(Duration::from_millis(500), rx.recv())
///     .await
///     .expect("Timeout")
///     .expect("Channel closed");
/// 
/// // 验证: 收到失效事件
/// match event {
///     CacheInvalidationEvent::InvalidateParentChildren(ino) => {
///         assert_eq!(ino, parent);
///     },
///     _ => panic!("Unexpected event: {:?}", event),
/// }
/// ```
/// 
/// **预期结果**:
/// - ✅ Watch Worker 监听到 PUT 事件
/// - ✅ 解析为 `InvalidateParentChildren(parent_ino)`
/// - ✅ 延迟 < 100ms
/// 
/// ---
/// 
/// ## 实际测试实现建议
/// 
/// ### 1. 目录结构
/// 
/// ```
/// slayerfs/
/// ├── src/
/// │   └── meta/
/// │       └── stores/
/// │           ├── etcd_store.rs
/// │           ├── etcd_txn_helper.rs
/// │           └── etcd_watch.rs
/// ├── tests/
/// │   ├── etcd_concurrent_tests.rs  ← 集成测试
/// │   └── docker-compose.yml        ← etcd 测试环境
/// └── Cargo.toml
/// ```
/// 
/// ### 2. 使用 testcontainers
/// 
/// ```rust
/// use testcontainers::{clients::Cli, images::generic::GenericImage};
/// 
/// #[tokio::test]
/// async fn test_concurrent_create() {
///     // 启动 etcd 容器
///     let docker = Cli::default();
///     let etcd_image = GenericImage::new("quay.io/coreos/etcd", "v3.5.0")
///         .with_exposed_port(2379)
///         .with_env_var("ETCD_LISTEN_CLIENT_URLS", "http://0.0.0.0:2379")
///         .with_env_var("ETCD_ADVERTISE_CLIENT_URLS", "http://0.0.0.0:2379");
///     
///     let etcd_container = docker.run(etcd_image);
///     let etcd_port = etcd_container.get_host_port_ipv4(2379);
///     
///     // 创建客户端
///     let config = MetaClientConfig {
///         backend: MetaBackend::Etcd {
///             endpoints: vec![format!("http://127.0.0.1:{}", etcd_port)],
///         },
///         cache_config: None,
///     };
///     
///     let client = MetaClient::new(config).await.unwrap();
///     
///     // ... 测试逻辑 ...
/// }
/// ```
/// 
/// ### 3. 性能基准测试
/// 
/// ```rust
/// use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId};
/// 
/// fn bench_concurrent_create(c: &mut Criterion) {
///     let mut group = c.benchmark_group("etcd_concurrent");
///     
///     for concurrency in [1, 5, 10, 50] {
///         group.bench_with_input(
///             BenchmarkId::new("create", concurrency),
///             &concurrency,
///             |b, &concurrency| {
///                 b.to_async(Runtime::new().unwrap()).iter(|| async {
///                     // 创建文件测试
///                 });
///             },
///         );
///     }
/// }
/// 
/// criterion_group!(benches, bench_concurrent_create);
/// criterion_main!(benches);
/// ```
/// 
/// ---
/// 
/// ## 监控指标
/// 
/// 建议在测试中收集以下指标：
/// 
/// ```rust
/// struct TestMetrics {
///     total_operations: u64,
///     successful_operations: u64,
///     failed_operations: u64,
///     avg_retry_count: f64,
///     max_retry_count: u64,
///     avg_latency_ms: f64,
///     p99_latency_ms: f64,
/// }
/// ```
/// 
/// ---
/// 
/// ## 总结
/// 
/// 这个示例文件提供了测试 etcd 分布式锁的完整蓝图。实际实现时：
/// 
/// 1. ✅ 使用 `testcontainers` 启动隔离的 etcd 实例
/// 2. ✅ 使用 `#[tokio::test]` 编写异步测试
/// 3. ✅ 使用 `criterion` 进行性能基准测试
/// 4. ✅ 收集重试次数、延迟等指标
/// 5. ✅ 测试各种边界情况（网络分区、超时等）
/// 
/// 关键点:
/// - 原子性: 并发操作只有一个成功
/// - 一致性: 没有孤儿 inode 或数据丢失
/// - 重试机制: CAS 冲突自动重试
/// - 性能: 低开销，高并发下重试次数合理

/// 主函数：打印使用说明
fn main() {
    println!("\n╔══════════════════════════════════════════════════════════════╗");
    println!("║  Etcd 分布式锁并发测试概念说明                            ║");
    println!("╚══════════════════════════════════════════════════════════════╝\n");
    println!("这不是一个可执行的测试示例，而是一份详细的测试设计文档。\n");
    println!("请参考上面的注释了解如何实现真正的并发测试。\n");
    println!("关键点:");
    println!("  1. 使用 testcontainers 启动隔离的 etcd 实例");
    println!("  2. 使用 #[tokio::test] 编写异步测试");
    println!("  3. 测试原子性、一致性、重试机制");
    println!("  4. 收集性能指标（重试次数、延迟等）\n");
    println!("实际测试文件应该放在 tests/ 目录下。\n");
}
