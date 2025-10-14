//! ID Generator abstraction for stateless metadata servers
//!
//! Provides different strategies for generating unique inode numbers
//! in a distributed environment.

use crate::meta::store::MetaError;
use async_trait::async_trait;
use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

/// ID generator trait for stateless inode allocation
#[async_trait]
pub trait IdGenerator: Send + Sync {
    /// Generate next unique ID
    async fn next_id(&self) -> Result<i64, MetaError>;

    /// Initialize the generator (e.g., create sequences, set initial values)
    async fn initialize(&self) -> Result<(), MetaError> {
        Ok(())
    }
}

/// PostgreSQL sequence-based ID generator (recommended for production)
///
/// Uses PostgreSQL's native sequence for efficient, distributed ID generation.
/// This is the most reliable option for multi-instance deployments.
pub struct PostgresIdGenerator {
    db: DatabaseConnection,
    sequence_name: String,
}

impl PostgresIdGenerator {
    pub fn new(db: DatabaseConnection) -> Self {
        Self {
            db,
            sequence_name: "slayerfs_inode_seq".to_string(),
        }
    }

    pub fn with_sequence_name(mut self, name: String) -> Self {
        self.sequence_name = name;
        self
    }
}

#[async_trait]
impl IdGenerator for PostgresIdGenerator {
    async fn initialize(&self) -> Result<(), MetaError> {
        // Create sequence if not exists (starting from 2, as 1 is reserved for root)
        let sql = format!(
            "CREATE SEQUENCE IF NOT EXISTS {} START 2 INCREMENT 1",
            self.sequence_name
        );

        self.db
            .execute(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .map_err(MetaError::Database)?;

        Ok(())
    }

    async fn next_id(&self) -> Result<i64, MetaError> {
        let sql = format!("SELECT nextval('{}')", self.sequence_name);

        let result = self
            .db
            .query_one(Statement::from_string(DatabaseBackend::Postgres, sql))
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::Internal("Failed to generate ID".to_string()))?;

        let id: i64 = result
            .try_get("", "nextval")
            .map_err(|e| MetaError::Internal(format!("Failed to extract ID: {}", e)))?;

        Ok(id)
    }
}

/// SQLite auto-increment based ID generator
///
/// For SQLite, we use a dedicated counter table with auto-increment.
/// This is suitable for single-node deployments.
pub struct SqliteIdGenerator {
    db: DatabaseConnection,
}

impl SqliteIdGenerator {
    pub fn new(db: DatabaseConnection) -> Self {
        Self { db }
    }
}

#[async_trait]
impl IdGenerator for SqliteIdGenerator {
    async fn initialize(&self) -> Result<(), MetaError> {
        // Create counter table if not exists
        let sql = r#"
            CREATE TABLE IF NOT EXISTS inode_counter (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at INTEGER DEFAULT (strftime('%s', 'now'))
            )
        "#;

        self.db
            .execute(Statement::from_string(DatabaseBackend::Sqlite, sql))
            .await
            .map_err(MetaError::Database)?;

        // Insert initial value if table is empty
        let count_sql = "SELECT COUNT(*) as cnt FROM inode_counter";
        let count = self
            .db
            .query_one(Statement::from_string(
                DatabaseBackend::Sqlite,
                count_sql,
            ))
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::Internal("Failed to query counter".to_string()))?;

        let cnt: i32 = count
            .try_get("", "cnt")
            .map_err(|e| MetaError::Internal(format!("Failed to get count: {}", e)))?;

        if cnt == 0 {
            // Insert initial row (id will be 1, but we'll skip to 2 for root)
            let insert_sql = "INSERT INTO inode_counter DEFAULT VALUES";
            self.db
                .execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    insert_sql,
                ))
                .await
                .map_err(MetaError::Database)?;
        }

        Ok(())
    }

    async fn next_id(&self) -> Result<i64, MetaError> {
        // Insert a new row and get its ID
        let insert_sql = "INSERT INTO inode_counter DEFAULT VALUES";
        let result = self
            .db
            .execute(Statement::from_string(
                DatabaseBackend::Sqlite,
                insert_sql,
            ))
            .await
            .map_err(MetaError::Database)?;

        // Get the last inserted ID
        let id = result.last_insert_id() as i64;

        // Ensure we don't return 1 (reserved for root)
        if id <= 1 {
            return self.next_id().await;
        }

        Ok(id)
    }
}

/// Etcd-based distributed ID generator using CAS (Compare-And-Swap)
///
/// Uses etcd's transaction API for atomic counter increment.
/// Suitable for distributed deployments with etcd cluster.
pub struct EtcdIdGenerator {
    client: etcd_client::Client,
    key: String,
}

impl EtcdIdGenerator {
    pub fn new(client: etcd_client::Client) -> Self {
        Self {
            client,
            key: "slayerfs/inode_counter".to_string(),
        }
    }

    pub fn with_key(mut self, key: String) -> Self {
        self.key = key;
        self
    }
}

#[async_trait]
impl IdGenerator for EtcdIdGenerator {
    async fn initialize(&self) -> Result<(), MetaError> {
        let mut client = self.client.clone();

        // Check if key exists
        match client.get(self.key.clone(), None).await {
            Ok(resp) if resp.kvs().is_empty() => {
                // Initialize with value 2 (1 is reserved for root)
                client
                    .put(self.key.clone(), "2", None)
                    .await
                    .map_err(|e| MetaError::Config(format!("Failed to initialize counter: {}", e)))?;
            }
            Ok(_) => {
                // Already initialized
            }
            Err(e) => {
                return Err(MetaError::Config(format!(
                    "Failed to check counter: {}",
                    e
                )));
            }
        }

        Ok(())
    }

    async fn next_id(&self) -> Result<i64, MetaError> {
        use etcd_client::{Compare, CompareOp, Txn, TxnOp};

        let mut client = self.client.clone();

        // Retry loop for CAS operation
        for attempt in 0..20 {
            // Get current value
            let resp = client
                .get(self.key.clone(), None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to get counter: {}", e)))?;

            let (current_id, mod_revision) = if let Some(kv) = resp.kvs().first() {
                let id = String::from_utf8_lossy(kv.value())
                    .parse::<i64>()
                    .map_err(|e| MetaError::Internal(format!("Invalid counter value: {}", e)))?;
                (id, kv.mod_revision())
            } else {
                // Counter not found, initialize it
                client
                    .put(self.key.clone(), "2", None)
                    .await
                    .map_err(|e| MetaError::Internal(format!("Failed to initialize: {}", e)))?;
                return Ok(2);
            };

            let next_id = current_id + 1;

            // CAS: only update if mod_revision hasn't changed
            let cmp = Compare::mod_revision(self.key.clone(), CompareOp::Equal, mod_revision);
            let put_op = TxnOp::put(self.key.clone(), next_id.to_string(), None);
            let txn = Txn::new().when([cmp]).and_then([put_op]);

            match client.txn(txn).await {
                Ok(txn_resp) if txn_resp.succeeded() => {
                    return Ok(next_id);
                }
                Ok(_) => {
                    // CAS failed, retry with exponential backoff
                    if attempt < 19 {
                        let backoff_ms = 2u64.pow(attempt.min(10));
                        tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
                        continue;
                    } else {
                        return Err(MetaError::Internal(
                            "Failed to generate ID after max retries".to_string(),
                        ));
                    }
                }
                Err(e) => {
                    return Err(MetaError::Internal(format!(
                        "Transaction failed: {}",
                        e
                    )));
                }
            }
        }

        Err(MetaError::Internal(
            "Failed to generate ID: max retries exceeded".to_string(),
        ))
    }
}

/// In-memory atomic counter (for testing only)
///
/// NOT suitable for production as it doesn't persist across restarts
/// and doesn't work in distributed scenarios.
pub struct AtomicIdGenerator {
    counter: Arc<AtomicI64>,
}

impl AtomicIdGenerator {
    pub fn new(start: i64) -> Self {
        Self {
            counter: Arc::new(AtomicI64::new(start)),
        }
    }
}

#[async_trait]
impl IdGenerator for AtomicIdGenerator {
    async fn next_id(&self) -> Result<i64, MetaError> {
        Ok(self.counter.fetch_add(1, Ordering::SeqCst))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_atomic_generator() {
        let generator = AtomicIdGenerator::new(100);

        let id1 = generator.next_id().await.unwrap();
        let id2 = generator.next_id().await.unwrap();
        let id3 = generator.next_id().await.unwrap();

        assert_eq!(id1, 100);
        assert_eq!(id2, 101);
        assert_eq!(id3, 102);
    }

    #[tokio::test]
    async fn test_atomic_generator_concurrent() {
        let generator = Arc::new(AtomicIdGenerator::new(1));
        let mut handles = vec![];

        for _ in 0..10 {
            let generator_clone = generator.clone();
            let handle = tokio::spawn(async move {
                let mut ids = vec![];
                for _ in 0..100 {
                    ids.push(generator_clone.next_id().await.unwrap());
                }
                ids
            });
            handles.push(handle);
        }

        let mut all_ids = vec![];
        for handle in handles {
            all_ids.extend(handle.await.unwrap());
        }

        all_ids.sort();
        all_ids.dedup();

        // Should have 1000 unique IDs
        assert_eq!(all_ids.len(), 1000);
    }
}
