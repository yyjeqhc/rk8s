//! Etcd Transaction Operations Implementation
//!
//! Provides etcd-specific implementation of the TransactionOps trait.
//! Uses etcd's MVCC (Multi-Version Concurrency Control) for atomic operations.
//!
//! ## Implementation Strategy
//!
//! - **Optimistic Locking**: Uses mod_revision for CAS (Compare-And-Swap)
//! - **Automatic Retries**: Handles version conflicts with exponential backoff
//! - **Atomic Transactions**: Leverages etcd's native transaction support

use crate::meta::store::MetaError;
use crate::meta::stores::transaction::TransactionOps;
use async_trait::async_trait;
use etcd_client::{Client as EtcdClient, Compare, CompareOp, Txn, TxnOp};
use log::{debug, warn};
use std::collections::HashSet;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Etcd-based transaction operations implementation
///
/// This struct wraps an etcd client and provides atomic operations
/// using etcd's native MVCC transaction support.
pub struct EtcdTransactionOps {
    client: Arc<Mutex<EtcdClient>>,
}

impl EtcdTransactionOps {
    /// Create a new EtcdTransactionOps instance
    pub fn new(client: Arc<Mutex<EtcdClient>>) -> Self {
        Self { client }
    }

    /// Get a mutable reference to the underlying etcd client
    pub async fn client(&self) -> tokio::sync::MutexGuard<'_, EtcdClient> {
        self.client.lock().await
    }
}

#[async_trait]
impl TransactionOps for EtcdTransactionOps {
    async fn update_parent_children_cas<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<(), MetaError>
    where
        F: Fn(&mut Vec<String>) + Send + 'static,
    {
        let mut client = self.client.lock().await;

        for retry in 0..max_retries {
            // Step 1: Read current value and mod_revision
            let resp = client
                .get(key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to get key for CAS: {}", e)))?;

            let (mut children_set, mod_revision) = if let Some(kv) = resp.kvs().first() {
                let children: crate::meta::entities::etcd::EtcdDirChildren =
                    serde_json::from_slice(kv.value()).map_err(|e| {
                        MetaError::Internal(format!("Failed to parse children: {}", e))
                    })?;
                // Convert HashSet to Vec for updater
                let mut children_vec: Vec<String> = children.children.into_iter().collect();
                (children_vec, kv.mod_revision())
            } else {
                // No existing entry - create new with revision 0
                (Vec::new(), 0)
            };

            // Step 2: Apply update function
            let old_size = children_set.len();
            updater(&mut children_set);
            let new_size = children_set.len();

            debug!(
                "CAS update children for {}: {} -> {} entries (retry {})",
                key, old_size, new_size, retry
            );

            // Step 3: Prepare new value (convert Vec back to HashSet)
            let children_hashset: HashSet<String> = children_set.into_iter().collect();

            // Extract parent inode from key (format: "c:123")
            let parent_ino = key
                .strip_prefix("c:")
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| {
                    MetaError::Internal(format!("Invalid children key format: {}", key))
                })?;

            let new_children = crate::meta::entities::etcd::EtcdDirChildren {
                inode: parent_ino,
                children: children_hashset,
            };
            let new_json = serde_json::to_string(&new_children)
                .map_err(|e| MetaError::Internal(format!("Failed to serialize children: {}", e)))?;

            // Step 4: CAS transaction
            let txn = if mod_revision == 0 {
                Txn::new()
                    .when([Compare::create_revision(key, CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(key, new_json, None)])
            } else {
                Txn::new()
                    .when([Compare::mod_revision(key, CompareOp::Equal, mod_revision)])
                    .and_then([TxnOp::put(key, new_json, None)])
            };

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| MetaError::Internal(format!("CAS transaction failed: {}", e)))?;

            if txn_resp.succeeded() {
                debug!("CAS update succeeded for {}", key);
                return Ok(());
            } else if retry < max_retries - 1 {
                warn!("CAS conflict for {} (retry {}/{})", key, retry, max_retries);
                continue;
            } else {
                return Err(MetaError::Internal(format!(
                    "CAS max retries exceeded for {}",
                    key
                )));
            }
        }

        Err(MetaError::Internal(
            "CAS update failed: unreachable".to_string(),
        ))
    }

    async fn atomic_create_with_check(
        &self,
        check_key: &str,
        entries: &[(&str, &str)],
    ) -> Result<(), MetaError> {
        let mut client = self.client.lock().await;

        // Build transaction: check + puts
        let mut txn = Txn::new().when([Compare::create_revision(check_key, CompareOp::Equal, 0)]);

        let mut ops = Vec::new();
        for (key, value) in entries {
            ops.push(TxnOp::put(*key, *value, None));
        }
        txn = txn.and_then(ops);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic create transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic create succeeded for check_key: {}", check_key);
            Ok(())
        } else {
            Err(MetaError::AlreadyExists {
                parent: 0, // Will be filled by caller
                name: check_key.to_string(),
            })
        }
    }

    async fn atomic_delete_with_check(
        &self,
        check_key: &str,
        keys: &[&str],
    ) -> Result<(), MetaError> {
        let mut client = self.client.lock().await;

        // Build transaction: check + deletes
        let mut txn =
            Txn::new().when([Compare::create_revision(check_key, CompareOp::NotEqual, 0)]);

        let mut ops = Vec::new();
        for key in keys {
            ops.push(TxnOp::delete(*key, None));
        }
        txn = txn.and_then(ops);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic delete transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic delete succeeded for check_key: {}", check_key);
            Ok(())
        } else {
            Err(MetaError::NotFound(0)) // Will be filled by caller
        }
    }

    async fn atomic_rename(
        &self,
        source_key: &str,
        target_key: &str,
        source_value: &str,
        target_value: &str,
    ) -> Result<(), MetaError> {
        let mut client = self.client.lock().await;

        // Transaction: source exists AND target doesn't exist
        let txn = Txn::new()
            .when([
                Compare::create_revision(source_key, CompareOp::NotEqual, 0),
                Compare::create_revision(target_key, CompareOp::Equal, 0),
            ])
            .and_then([
                TxnOp::put(target_key, target_value, None),
                TxnOp::delete(source_key, None),
            ]);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic rename transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic rename succeeded: {} -> {}", source_key, target_key);
            Ok(())
        } else {
            // Check which condition failed
            let source_resp = client
                .get(source_key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to check source key: {}", e)))?;

            if source_resp.kvs().is_empty() {
                Err(MetaError::NotFound(0))
            } else {
                Err(MetaError::AlreadyExists {
                    parent: 0,
                    name: target_key.to_string(),
                })
            }
        }
    }

    async fn cas_update<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<i64, MetaError>
    where
        F: Fn(&str, i64) -> Result<String, MetaError> + Send + 'static,
    {
        let mut client = self.client.lock().await;

        for retry in 0..max_retries {
            // Step 1: Read current value and version
            let resp = client
                .get(key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to get key for CAS: {}", e)))?;

            let (current_value, mod_revision) = if let Some(kv) = resp.kvs().first() {
                (
                    String::from_utf8_lossy(kv.value()).to_string(),
                    kv.mod_revision(),
                )
            } else {
                return Err(MetaError::NotFound(0));
            };

            // Step 2: Apply updater function
            let new_value = updater(&current_value, mod_revision)?;

            // Step 3: CAS update
            let txn = Txn::new()
                .when([Compare::mod_revision(key, CompareOp::Equal, mod_revision)])
                .and_then([TxnOp::put(key, new_value, None)]);

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| MetaError::Internal(format!("CAS transaction failed: {}", e)))?;

            if txn_resp.succeeded() {
                // Get new version
                let resp = client.get(key, None).await.map_err(|e| {
                    MetaError::Internal(format!("Failed to get updated key: {}", e))
                })?;

                if let Some(kv) = resp.kvs().first() {
                    debug!("CAS update succeeded for key: {}", key);
                    return Ok(kv.mod_revision());
                } else {
                    return Err(MetaError::Internal(
                        "Key disappeared after CAS update".to_string(),
                    ));
                }
            } else if retry < max_retries - 1 {
                warn!("CAS conflict for {} (retry {}/{})", key, retry, max_retries);
                continue;
            } else {
                return Err(MetaError::Internal(format!(
                    "CAS max retries exceeded for {}",
                    key
                )));
            }
        }

        Err(MetaError::Internal(
            "CAS update failed: unreachable".to_string(),
        ))
    }

    fn backend_name(&self) -> &'static str {
        "etcd"
    }

    fn supports_native_transactions(&self) -> bool {
        true
    }
}

// ============================================================================
// Legacy helper functions (deprecated, kept for backward compatibility)
// These will be removed in future versions - use EtcdTransactionOps instead
// ============================================================================

/// Helper for atomic parent children update with CAS (Compare-And-Swap)
///
/// # Strategy
///
/// Uses etcd's mod_revision to implement optimistic locking:
/// 1. Read current children set + mod_revision
/// 2. Apply update function
/// 3. CAS update: only succeed if mod_revision unchanged
/// 4. Retry on conflict (up to max_retries)
///
/// # Arguments
///
/// * `client` - Etcd client
/// * `parent_ino` - Parent directory inode
/// * `update_fn` - Function to modify children set
/// * `max_retries` - Maximum retry attempts (default: 10)
pub async fn update_parent_children_cas<F>(
    client: &mut EtcdClient,
    parent_ino: i64,
    update_fn: F,
    max_retries: usize,
) -> Result<(), MetaError>
where
    F: Fn(&mut HashSet<String>),
{
    let key = format!("c:{}", parent_ino);

    for retry in 0..max_retries {
        // Step 1: Read current value and mod_revision
        let resp = client
            .get(key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Failed to get children for CAS: {}", e)))?;

        let (mut children_set, mod_revision) = if let Some(kv) = resp.kvs().first() {
            let children: crate::meta::entities::etcd::EtcdDirChildren =
                serde_json::from_slice(kv.value())
                    .map_err(|e| MetaError::Internal(format!("Failed to parse children: {}", e)))?;
            (children.children, kv.mod_revision())
        } else {
            // No existing entry - create new with revision 0
            (HashSet::new(), 0)
        };

        // Step 2: Apply update function
        let old_size = children_set.len();
        update_fn(&mut children_set);
        let new_size = children_set.len();

        debug!(
            "CAS update children for parent {}: {} -> {} entries (retry {})",
            parent_ino, old_size, new_size, retry
        );

        // Step 3: Prepare new value
        let new_children = crate::meta::entities::etcd::EtcdDirChildren {
            inode: parent_ino,
            children: children_set,
        };
        let new_json = serde_json::to_string(&new_children)
            .map_err(|e| MetaError::Internal(format!("Failed to serialize children: {}", e)))?;

        // Step 4: CAS transaction
        let txn = if mod_revision == 0 {
            // First time creation - ensure key doesn't exist
            Txn::new()
                .when([Compare::create_revision(key.clone(), CompareOp::Equal, 0)])
                .and_then([TxnOp::put(key.clone(), new_json, None)])
        } else {
            // Update existing - ensure mod_revision unchanged
            Txn::new()
                .when([Compare::mod_revision(
                    key.clone(),
                    CompareOp::Equal,
                    mod_revision,
                )])
                .and_then([TxnOp::put(key.clone(), new_json, None)])
        };

        let txn_resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("CAS transaction failed: {}", e)))?;

        if txn_resp.succeeded() {
            debug!("CAS update succeeded for parent {}", parent_ino);
            return Ok(());
        } else {
            // CAS failed - retry
            if retry < max_retries - 1 {
                warn!(
                    "CAS conflict for parent {} (retry {}/{})",
                    parent_ino, retry, max_retries
                );
                continue;
            } else {
                return Err(MetaError::Internal(format!(
                    "CAS max retries exceeded for parent {}",
                    parent_ino
                )));
            }
        }
    }

    Err(MetaError::Internal(
        "CAS update failed: unreachable".to_string(),
    ))
}

/// Execute atomic create operation with existence check
///
/// # Strategy
///
/// Use etcd transaction to atomically:
/// 1. Check that forward key doesn't exist (create_revision == 0)
/// 2. Create all related keys (forward, reverse, children if dir)
/// 3. Update parent's children set with CAS
///
/// # Arguments
///
/// * `client` - Etcd client
/// * `forward_key` - Forward index key (f:parent:name)
/// * `operations` - Additional put operations to execute atomically
///
/// # Returns
///
/// * `Ok(true)` - Creation succeeded
/// * `Ok(false)` - Entry already exists (conflict)
/// * `Err(_)` - Transaction failed
pub async fn atomic_create_with_check(
    client: &mut EtcdClient,
    forward_key: String,
    operations: Vec<(String, String)>, // (key, value) pairs
) -> Result<bool, MetaError> {
    // Build transaction: IF forward_key not exists THEN put all keys
    let mut txn_ops = vec![TxnOp::put(
        forward_key.clone(),
        operations
            .iter()
            .find(|(k, _)| k == &forward_key)
            .map(|(_, v)| v.clone())
            .unwrap_or_default(),
        None,
    )];

    for (key, value) in operations {
        if key != forward_key {
            txn_ops.push(TxnOp::put(key, value, None));
        }
    }

    let txn = Txn::new()
        .when([Compare::create_revision(
            forward_key.clone(),
            CompareOp::Equal,
            0,
        )])
        .and_then(txn_ops);

    let resp = client
        .txn(txn)
        .await
        .map_err(|e| MetaError::Internal(format!("Atomic create transaction failed: {}", e)))?;

    Ok(resp.succeeded())
}

/// Execute atomic delete with existence check
///
/// # Strategy
///
/// Use etcd transaction to atomically:
/// 1. Check that key exists (create_revision > 0)
/// 2. Delete all related keys
///
/// # Arguments
///
/// * `client` - Etcd client
/// * `check_key` - Key to check for existence
/// * `delete_keys` - Keys to delete atomically
pub async fn atomic_delete_with_check(
    client: &mut EtcdClient,
    check_key: String,
    delete_keys: Vec<String>,
) -> Result<bool, MetaError> {
    let mut txn_ops = Vec::new();
    for key in delete_keys {
        txn_ops.push(TxnOp::delete(key, None));
    }

    let txn = Txn::new()
        .when([Compare::create_revision(check_key, CompareOp::Greater, 0)])
        .and_then(txn_ops);

    let resp = client
        .txn(txn)
        .await
        .map_err(|e| MetaError::Internal(format!("Atomic delete transaction failed: {}", e)))?;

    Ok(resp.succeeded())
}

/// Execute atomic rename operation
///
/// # Strategy
///
/// Use etcd transaction to atomically:
/// 1. Check old forward key exists
/// 2. Check new forward key doesn't exist
/// 3. Create new forward key
/// 4. Update reverse index
/// 5. Delete old forward key
///
/// # Returns
///
/// * `Ok(())` - Rename succeeded
/// * `Err(AlreadyExists)` - Target already exists
/// * `Err(NotFound)` - Source doesn't exist
pub async fn atomic_rename(
    client: &mut EtcdClient,
    old_forward_key: String,
    new_forward_key: String,
    operations: Vec<(String, String)>, // (key, value) for new entries
    delete_keys: Vec<String>,
) -> Result<(), MetaError> {
    // Build put operations
    let mut txn_ops = Vec::new();
    for (key, value) in operations {
        txn_ops.push(TxnOp::put(key, value, None));
    }

    // Build delete operations
    for key in delete_keys {
        txn_ops.push(TxnOp::delete(key, None));
    }

    // Transaction: old exists AND new doesn't exist
    let txn = Txn::new()
        .when([
            Compare::create_revision(old_forward_key.clone(), CompareOp::Greater, 0),
            Compare::create_revision(new_forward_key.clone(), CompareOp::Equal, 0),
        ])
        .and_then(txn_ops);

    let resp = client
        .txn(txn)
        .await
        .map_err(|e| MetaError::Internal(format!("Atomic rename transaction failed: {}", e)))?;

    if !resp.succeeded() {
        // Check which condition failed
        // Try to read old key
        let old_exists = client
            .get(old_forward_key, None)
            .await
            .ok()
            .and_then(|r| r.kvs().first().cloned())
            .is_some();

        let new_exists = client
            .get(new_forward_key.clone(), None)
            .await
            .ok()
            .and_then(|r| r.kvs().first().cloned())
            .is_some();

        if !old_exists {
            return Err(MetaError::NotFound(0)); // Source doesn't exist
        }

        if new_exists {
            // Extract parent and name from new_forward_key (format: f:parent:name)
            let parts: Vec<&str> = new_forward_key.split(':').collect();
            if parts.len() >= 3 {
                if let Ok(parent) = parts[1].parse::<i64>() {
                    let name = parts[2..].join(":");
                    return Err(MetaError::AlreadyExists { parent, name });
                }
            }
            return Err(MetaError::Internal("Target already exists".to_string()));
        }

        return Err(MetaError::Internal("Rename transaction failed".to_string()));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: These tests require a running etcd instance
    // Run with: cargo test --test etcd_integration -- --ignored

    #[tokio::test]
    #[ignore]
    async fn test_cas_update_children() {
        let mut client = EtcdClient::connect(["localhost:2379"], None).await.unwrap();

        // Test adding child
        update_parent_children_cas(
            &mut client,
            1,
            |children| {
                children.insert("test.txt".to_string());
            },
            10,
        )
        .await
        .unwrap();

        // Verify
        let resp = client.get("c:1", None).await.unwrap();
        assert!(resp.kvs().first().is_some());
    }

    #[tokio::test]
    #[ignore]
    async fn test_atomic_create() {
        let mut client = EtcdClient::connect(["localhost:2379"], None).await.unwrap();

        let forward_key = "f:1:test_atomic.txt".to_string();

        // First create should succeed
        let succeeded = atomic_create_with_check(
            &mut client,
            forward_key.clone(),
            vec![(forward_key.clone(), "test_value".to_string())],
        )
        .await
        .unwrap();
        assert!(succeeded);

        // Second create should fail (already exists)
        let succeeded = atomic_create_with_check(
            &mut client,
            forward_key.clone(),
            vec![(forward_key.clone(), "test_value".to_string())],
        )
        .await
        .unwrap();
        assert!(!succeeded);

        // Cleanup
        client.delete(forward_key, None).await.unwrap();
    }
}
