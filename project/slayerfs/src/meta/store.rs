//! Metadata store abstract interface
//!
//! Defines unified interface for filesystem metadata operations
use crate::vfs::fs::FileType;
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
    pub blocks: u64,
    pub blksize: u32,
    pub rdev: u32,
    /// Version number for optimistic locking (0 means versioning not supported)
    pub version: u64,
}

impl FileAttr {
    /// Create a new FileAttr with default blocks/blksize/rdev values
    pub fn new(
        ino: i64,
        size: u64,
        kind: FileType,
        mode: u32,
        uid: u32,
        gid: u32,
        atime: i64,
        mtime: i64,
        ctime: i64,
        nlink: u32,
    ) -> Self {
        let blocks = if kind == FileType::Dir {
            8
        } else {
            (size + 511) / 512
        };

        Self {
            ino,
            size,
            kind,
            mode,
            uid,
            gid,
            atime,
            mtime,
            ctime,
            nlink,
            blocks,
            blksize: 4096,
            rdev: 0,
            version: 0,
        }
    }
}

/// Directory entry
#[derive(Debug, Clone)]
pub struct DirEntry {
    pub name: String,
    pub ino: i64,
    pub kind: FileType,
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

    #[error("Version conflict: expected {expected}, got {actual}")]
    Conflict { expected: u64, actual: u64 },

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

    #[error("Invalid configuration: {0}")]
    InvalidConfig(String),

    #[error("RPC error: {0}")]
    RpcError(String),

    #[error("Connection error: {0}")]
    ConnectionError(String),
}

use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use async_trait::async_trait;

/// MetaStore V2 trait
///
/// All operations are inode-based. Path resolution is handled by the VFS layer.
/// Implementations must ensure atomicity and consistency.
#[async_trait]
#[auto_impl::auto_impl(&, Arc)]
pub trait MetaStore: Send + Sync {
    // ==================== Query Operations ====================

    /// Get file attributes for a given inode
    async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError>;

    /// Get attributes for multiple inodes (batch operation)
    async fn getattr_batch(&self, inos: &[Inode]) -> Result<Vec<(Inode, FileAttr)>, MetaError> {
        let mut results = Vec::with_capacity(inos.len());
        for &ino in inos {
            if let Ok(attr) = self.getattr(ino).await {
                results.push((ino, attr));
            }
        }
        Ok(results)
    }

    /// Look up a directory entry by name
    async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError>;

    /// Read directory entries
    async fn readdir(&self, ino: Inode) -> Result<Vec<DirEntry>, MetaError>;

    /// Read directory entries with attributes (optimization)
    async fn readdirplus(&self, ino: Inode) -> Result<Vec<(DirEntry, FileAttr)>, MetaError> {
        let entries = self.readdir(ino).await?;
        let inos: Vec<Inode> = entries.iter().map(|e| Inode(e.ino)).collect();
        let attrs = self.getattr_batch(&inos).await?;

        let attr_map: std::collections::HashMap<Inode, FileAttr> = attrs.into_iter().collect();

        let mut results = Vec::with_capacity(entries.len());
        for entry in entries {
            if let Some(attr) = attr_map.get(&Inode(entry.ino)) {
                results.push((entry, attr.clone()));
            }
        }

        Ok(results)
    }

    // ==================== Creation Operations ====================

    /// Create a new file or directory
    ///
    /// This operation is atomic: either fully succeeds or fails with no side effects.
    async fn create(&self, params: CreateParams) -> Result<(Inode, FileAttr), MetaError>;

    // ==================== Modification Operations ====================

    /// Update file attributes
    ///
    /// Only attributes specified in the mask are updated.
    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> Result<FileAttr, MetaError>;

    /// Rename/move a file or directory
    ///
    /// This operation is atomic.
    async fn rename(
        &self,
        old_parent: Inode,
        old_name: &str,
        new_parent: Inode,
        new_name: String,
    ) -> Result<(), MetaError>;

    // ==================== Deletion Operations ====================

    /// Remove a file
    async fn unlink(&self, parent: Inode, name: &str) -> Result<(), MetaError>;

    /// Remove a directory (must be empty)
    async fn rmdir(&self, parent: Inode, name: &str) -> Result<(), MetaError>;

    // ==================== System Operations ====================

    /// Initialize the metadata store
    async fn initialize(&self) -> Result<(), MetaError>;

    /// Get the root directory inode
    fn root_ino(&self) -> Inode {
        Inode::ROOT
    }
}
