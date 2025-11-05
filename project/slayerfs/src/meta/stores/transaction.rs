//! Transaction Operations Abstraction Layer
//!
//! This module provides a unified abstraction for atomic operations across different storage backends.
//! It supports both transaction-native backends (etcd, PostgreSQL, MySQL) and non-transactional
//! backends (simple KV stores) through application-level locking.
//!
//! ## Architecture
//!
//! - **TransactionOps trait**: Defines atomic operations interface
//! - **Backend implementations**:
//!   - `EtcdTransactionOps`: Uses etcd's MVCC transactions (optimistic locking)
//!   - `DatabaseTransactionOps`: Uses SQL database transactions (ACID)
//!   - `LockBasedTransactionOps`: Fallback using application-level distributed locks
//!
//! ## Usage Example
//!
//! ```rust
//! let tx_ops = EtcdTransactionOps::new(client);
//! tx_ops.atomic_create_with_check(
//!     &check_key,
//!     &[(&key1, &value1), (&key2, &value2)]
//! ).await?;
//! ```

use crate::meta::store::MetaError;
use async_trait::async_trait;

/// Result of a CAS (Compare-And-Swap) operation
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CasResult<T> {
    /// Operation succeeded with the new value and version
    Success { value: T, version: i64 },
    /// Operation failed due to version mismatch (concurrent modification detected)
    VersionMismatch { current_version: i64 },
    /// Key not found (for update operations)
    NotFound,
}

/// Atomic transaction operations interface
///
/// This trait abstracts the atomic operations needed for filesystem metadata management.
/// Different backends can implement this trait using their native transaction mechanisms
/// or fallback to application-level locking.
#[async_trait]
pub trait TransactionOps: Send + Sync {
    /// Atomically update a parent's children set using CAS
    ///
    /// # Arguments
    /// * `key` - The key storing the children set (JSON array)
    /// * `updater` - Function to modify the children set
    /// * `max_retries` - Maximum number of retry attempts on version conflict
    ///
    /// # Returns
    /// * `Ok(())` - Update succeeded
    /// * `Err(MetaError)` - Operation failed (e.g., max retries exceeded)
    async fn update_parent_children_cas<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<(), MetaError>
    where
        F: Fn(&mut Vec<String>) + Send + 'static;

    /// Atomically create multiple entries if a check key does NOT exist
    ///
    /// # Arguments
    /// * `check_key` - Key to verify non-existence (e.g., child forward index)
    /// * `entries` - Key-value pairs to create atomically
    ///
    /// # Returns
    /// * `Ok(())` - All entries created successfully
    /// * `Err(MetaError::AlreadyExists)` - Check key already exists
    async fn atomic_create_with_check(
        &self,
        check_key: &str,
        entries: &[(&str, &str)],
    ) -> Result<(), MetaError>;

    /// Atomically delete multiple entries if a check key EXISTS
    ///
    /// # Arguments
    /// * `check_key` - Key to verify existence (e.g., child forward index)
    /// * `keys` - Keys to delete atomically
    ///
    /// # Returns
    /// * `Ok(())` - All keys deleted successfully
    /// * `Err(MetaError::NotFound)` - Check key doesn't exist
    async fn atomic_delete_with_check(
        &self,
        check_key: &str,
        keys: &[&str],
    ) -> Result<(), MetaError>;

    /// Atomically rename an entry (move from source to target)
    ///
    /// # Arguments
    /// * `source_key` - Source key (must exist)
    /// * `target_key` - Target key (must NOT exist)
    /// * `source_value` - New value for source after rename
    /// * `target_value` - Value to write to target
    ///
    /// # Returns
    /// * `Ok(())` - Rename succeeded
    /// * `Err(MetaError::NotFound)` - Source doesn't exist
    /// * `Err(MetaError::AlreadyExists)` - Target already exists
    async fn atomic_rename(
        &self,
        source_key: &str,
        target_key: &str,
        source_value: &str,
        target_value: &str,
    ) -> Result<(), MetaError>;

    /// Generic CAS update for a single key
    ///
    /// This is useful for operations like set_file_size, set_permissions, etc.
    /// where you need to atomically update a single field with concurrency protection.
    ///
    /// # Arguments
    /// * `key` - The key to update
    /// * `updater` - Function to modify the value (receives current value and version)
    /// * `max_retries` - Maximum number of retry attempts on version conflict
    ///
    /// # Returns
    /// * `Ok(new_version)` - Update succeeded, returns new version
    /// * `Err(MetaError)` - Operation failed
    async fn cas_update<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<i64, MetaError>
    where
        F: Fn(&str, i64) -> Result<String, MetaError> + Send + 'static;

    /// Get backend name for debugging/logging
    fn backend_name(&self) -> &'static str;

    /// Check if backend supports native transactions
    fn supports_native_transactions(&self) -> bool;
}

/// Transaction operation metrics
#[derive(Debug, Clone, Default)]
pub struct TransactionMetrics {
    /// Total number of CAS retry attempts
    pub total_retries: u64,
    /// Number of operations that hit max retries
    pub max_retries_hit: u64,
    /// Average retries per operation
    pub avg_retries: f64,
}

impl TransactionMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record_retry(&mut self, retries: usize, max_retries: usize) {
        self.total_retries += retries as u64;
        if retries >= max_retries {
            self.max_retries_hit += 1;
        }
        // Update running average (simplified)
        let total_ops = self.total_retries + 1;
        self.avg_retries = self.total_retries as f64 / total_ops as f64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cas_result() {
        let success: CasResult<String> = CasResult::Success {
            value: "test".to_string(),
            version: 42,
        };
        assert!(matches!(success, CasResult::Success { .. }));

        let mismatch: CasResult<String> = CasResult::VersionMismatch {
            current_version: 10,
        };
        assert!(matches!(mismatch, CasResult::VersionMismatch { .. }));
    }

    #[test]
    fn test_metrics() {
        let mut metrics = TransactionMetrics::new();
        metrics.record_retry(3, 10);
        assert_eq!(metrics.total_retries, 3);
        assert_eq!(metrics.max_retries_hit, 0);

        metrics.record_retry(10, 10);
        assert_eq!(metrics.max_retries_hit, 1);
    }
}
