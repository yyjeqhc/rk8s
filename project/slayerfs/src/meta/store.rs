//! Metadata store abstract interface
//!
//! Defines unified interface for filesystem metadata operations
use crate::meta::entities::content_meta::EntryType;
use async_trait::async_trait;

/// File type enumeration
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileType {
    File,
    Dir,
}

impl From<EntryType> for FileType {
    fn from(entry_type: EntryType) -> Self {
        match entry_type {
            EntryType::File => FileType::File,
            EntryType::Directory => FileType::Dir,
        }
    }
}

/// File attributes
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct FileAttr {
    pub ino: i64,
    pub size: u64,
    pub kind: FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
    pub atime: i64,
    pub mtime: i64,
    pub ctime: i64,
    pub nlink: u32,
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub ino: i64,
    pub kind: FileType,
}

/// Transaction operations for atomic metadata updates
///
/// This trait provides low-level atomic operations for metadata storage backends
/// that support transactions (etcd, PostgreSQL, MySQL, etc.).
#[async_trait]
pub trait TransactionOps: Send + Sync {
    /// Update a list value using Compare-And-Swap (CAS)
    ///
    /// # Arguments
    ///
    /// * `key` - The key identifying the list to update
    /// * `updater` - Function to modify the list in-place
    /// * `max_retries` - Maximum retry attempts on CAS conflicts
    ///
    /// # Implementation Notes
    ///
    /// - **Etcd/TiKV**: Read JSON → deserialize to Vec → apply updater → serialize → CAS write
    /// - **PostgreSQL/MySQL**: SELECT FOR UPDATE → modify JSONB column → UPDATE with version check
    /// - **SQLite**: BEGIN IMMEDIATE → modify → COMMIT (serializable isolation)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Update succeeded within max_retries
    /// * `Err(MetaError::Internal)` - All retries exhausted (concurrent modification)
    async fn cas_update_list<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<(), MetaError>
    where
        F: Fn(&mut Vec<String>) + Send + 'static;

    /// Atomically create multiple entries if check_key does NOT exist
    ///
    /// # Arguments
    ///
    /// * `check_key` - Key to check for non-existence (e.g., forward index key)
    /// * `entries` - List of (key, value) pairs to create atomically
    ///
    /// # Semantics
    ///
    /// Equivalent to: `if not exists(check_key) { create(entries) }`
    ///
    /// # Implementation Notes
    ///
    /// - **Etcd**: Transaction with Compare(check_key, CompareOp::Equal, "") + batch Put
    /// - **SQL**: `INSERT ... WHERE NOT EXISTS (SELECT 1 FROM ... WHERE key = check_key)`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Creation succeeded (check passed)
    /// * `Err(MetaError::AlreadyExists)` - check_key already exists
    async fn create_if_not_exists(
        &self,
        check_key: &str,
        entries: &[(&str, &str)],
    ) -> Result<(), MetaError>;

    /// Atomically delete multiple entries if check_key exists
    ///
    /// # Arguments
    ///
    /// * `check_key` - Key to check for existence (e.g., forward index key)
    /// * `keys` - List of keys to delete atomically
    ///
    /// # Semantics
    ///
    /// Equivalent to: `if exists(check_key) { delete(keys) }`
    ///
    /// # Implementation Notes
    ///
    /// - **Etcd**: Transaction with Compare(check_key, CompareOp::NotEqual, "") + batch Delete
    /// - **SQL**: `DELETE FROM ... WHERE key IN (...) AND EXISTS (SELECT 1 FROM ... WHERE key = check_key)`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Deletion succeeded (check passed)
    /// * `Err(MetaError::NotFound)` - check_key doesn't exist
    async fn delete_if_exists(&self, check_key: &str, keys: &[&str]) -> Result<(), MetaError>;

    /// Atomically rename an entry (move from source to target)
    ///
    /// # Arguments
    ///
    /// * `source_key` - Key of the source entry (must exist)
    /// * `target_key` - Key of the target entry (must NOT exist)
    /// * `source_value` - Expected value at source_key (for verification)
    /// * `target_value` - New value to write at target_key
    ///
    /// # Semantics
    ///
    /// Equivalent to: `if exists(source_key) && not exists(target_key) { delete(source_key); create(target_key) }`
    ///
    /// # Implementation Notes
    ///
    /// - **Etcd**: Transaction with Compare(source exists) AND Compare(target not exists) + Delete + Put
    /// - **SQL**: `UPDATE ... SET key = target_key WHERE key = source_key AND NOT EXISTS (SELECT 1 FROM ... WHERE key = target_key)`
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Rename succeeded
    /// * `Err(MetaError::NotFound)` - source_key doesn't exist
    /// * `Err(MetaError::AlreadyExists)` - target_key already exists
    async fn rename_atomic(
        &self,
        source_key: &str,
        target_key: &str,
        source_value: &str,
        target_value: &str,
    ) -> Result<(), MetaError>;

    /// Generic CAS update for a single scalar value (reserved for future use)
    ///
    /// # Arguments
    ///
    /// * `key` - The key to update
    /// * `updater` - Function that takes (current_value, current_version) and returns new_value
    /// * `max_retries` - Maximum retry attempts on CAS conflicts
    ///
    /// # Implementation Notes
    ///
    /// This is a lower-level primitive for implementing custom CAS logic.
    /// Most use cases should prefer higher-level methods like `cas_update_list`.
    ///
    /// # Returns
    ///
    /// * `Ok(new_version)` - Update succeeded, returns new version number
    /// * `Err(MetaError::Internal)` - All retries exhausted
    #[allow(dead_code)]
    async fn cas_update_value<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<i64, MetaError>
    where
        F: Fn(&str, i64) -> Result<String, MetaError> + Send + 'static;
}

/// Metadata operation errors
#[derive(Debug, thiserror::Error)]
#[allow(dead_code)]
pub enum MetaError {
    #[error("Entry not found: {0}")]
    NotFound(i64),

    #[error("Parent directory not found: {0}")]
    ParentNotFound(i64),

    #[error("Entry already exists: {name} in parent {parent}")]
    AlreadyExists { parent: i64, name: String },

    #[error("Not a directory: {0}")]
    NotDirectory(i64),

    #[error("Directory not empty: {0}")]
    DirectoryNotEmpty(i64),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Operation not supported: {0}")]
    NotSupported(String),

    #[error("Not implemented")]
    NotImplemented,

    #[error("Internal error: {0}")]
    Internal(String),

    #[error("Database error: {0}")]
    Database(#[from] sea_orm::DbErr),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("Config error: {0}")]
    Config(String),
}

/// Metadata store abstract interface
#[async_trait]
#[auto_impl::auto_impl(&, Arc)]
#[allow(dead_code)]
pub trait MetaStore: Send + Sync {
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError>;

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError>;

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError>;

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError>;

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError>;

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError>;

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError>;

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError>;

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError>;

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError>;

    /// get the node's parent inode
    async fn get_parent(&self, ino: i64) -> Result<Option<i64>, MetaError>;

    /// get the node's name in its parent directory
    async fn get_name(&self, ino: i64) -> Result<Option<String>, MetaError>;

    /// get the inode's full path (from the root directory)
    async fn get_path(&self, ino: i64) -> Result<Option<String>, MetaError>;

    fn root_ino(&self) -> i64;

    async fn initialize(&self) -> Result<(), MetaError>;

    /// Allow downcasting to concrete types
    fn as_any(&self) -> &dyn std::any::Any;
}
