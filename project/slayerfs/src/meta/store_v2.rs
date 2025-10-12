//! MetaStore trait V2 - Modern interface for filesystem metadata operations
//!
//! This is a redesigned interface that:
//! - Uses strong-typed Inode instead of raw i64
//! - Provides clear, atomic operations
//! - Separates concerns (inode operations only, no path resolution)
//! - Supports batch operations for performance

use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use async_trait::async_trait;

/// MetaStore V2 trait
///
/// All operations are inode-based. Path resolution is handled by the VFS layer.
/// Implementations must ensure atomicity and consistency.
#[async_trait]
pub trait MetaStoreV2: Send + Sync {
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

        let attr_map: std::collections::HashMap<Inode, FileAttr> =
            attrs.into_iter().collect();

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

#[cfg(test)]
pub mod trait_tests {
    use super::*;
    use crate::vfs::fs::FileType;

    /// Test basic operations on any MetaStoreV2 implementation
    pub async fn test_basic_operations<M: MetaStoreV2>(store: M) {
        // 1. Initialize
        store.initialize().await.unwrap();
        assert_eq!(store.root_ino(), Inode::ROOT);

        // 2. Create a file
        let params = CreateParams::file(Inode::ROOT, "test.txt".into(), 1000, 1000);
        let (ino, attr) = store.create(params).await.unwrap();
        assert_eq!(attr.kind, FileType::File);
        assert!(ino != Inode::ROOT);

        // 3. Lookup the file
        let found = store.lookup(Inode::ROOT, "test.txt").await.unwrap();
        assert_eq!(found, ino);

        // 4. Get attributes
        let attr2 = store.getattr(ino).await.unwrap();
        assert_eq!(attr2.ino, ino.as_i64());

        // 5. List directory
        let entries = store.readdir(Inode::ROOT).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "test.txt");
    }

    /// Test directory operations
    pub async fn test_directory_operations<M: MetaStoreV2>(store: M) {
        store.initialize().await.unwrap();

        // 1. Create a directory
        let params = CreateParams::dir(Inode::ROOT, "dir1".into(), 1000, 1000);
        let (dir_ino, attr) = store.create(params).await.unwrap();
        assert_eq!(attr.kind, FileType::Dir);

        // 2. Create a file in the directory
        let params = CreateParams::file(dir_ino, "file.txt".into(), 1000, 1000);
        let (file_ino, _) = store.create(params).await.unwrap();

        // 3. Lookup file
        let found = store.lookup(dir_ino, "file.txt").await.unwrap();
        assert_eq!(found, file_ino);

        // 4. Delete file first
        store.unlink(dir_ino, "file.txt").await.unwrap();

        // 5. Delete directory
        store.rmdir(Inode::ROOT, "dir1").await.unwrap();
    }

    /// Test error conditions
    pub async fn test_error_conditions<M: MetaStoreV2>(store: M) {
        store.initialize().await.unwrap();

        // 1. Lookup non-existent file
        let result = store.lookup(Inode::ROOT, "nonexistent").await;
        assert!(result.is_err());

        // 2. Create duplicate file
        let params = CreateParams::file(Inode::ROOT, "dup.txt".into(), 1000, 1000);
        store.create(params.clone()).await.unwrap();

        let result = store.create(params).await;
        assert!(matches!(result, Err(MetaError::AlreadyExists { .. })));
    }

    /// Test rename operations
    pub async fn test_rename<M: MetaStoreV2>(store: M) {
        store.initialize().await.unwrap();

        // 1. Create a file
        let params = CreateParams::file(Inode::ROOT, "old.txt".into(), 1000, 1000);
        let (ino, _) = store.create(params).await.unwrap();

        // 2. Rename in same directory
        store
            .rename(Inode::ROOT, "old.txt", Inode::ROOT, "new.txt".into())
            .await
            .unwrap();

        // 3. Verify old name is gone
        let result = store.lookup(Inode::ROOT, "old.txt").await;
        assert!(result.is_err());

        // 4. Verify new name exists
        let found = store.lookup(Inode::ROOT, "new.txt").await.unwrap();
        assert_eq!(found, ino);
    }

    /// Test batch operations
    pub async fn test_batch_operations<M: MetaStoreV2>(store: M) {
        store.initialize().await.unwrap();

        // Create multiple files
        let mut inos = Vec::new();
        for i in 0..5 {
            let params = CreateParams::file(
                Inode::ROOT,
                format!("file{}.txt", i),
                1000,
                1000,
            );
            let (ino, _) = store.create(params).await.unwrap();
            inos.push(ino);
        }

        // Batch getattr
        let results = store.getattr_batch(&inos).await.unwrap();
        assert_eq!(results.len(), 5);

        // Test readdirplus
        let results = store.readdirplus(Inode::ROOT).await.unwrap();
        assert_eq!(results.len(), 5);

        for (entry, attr) in results {
            assert_eq!(entry.ino, attr.ino);
            assert!(entry.name.starts_with("file"));
        }
    }
}
