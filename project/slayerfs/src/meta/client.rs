//! Metadata client with caching
//!
//! Provides a caching layer on top of MetaStore backend,
//! reducing round-trip queries and improving performance.

use crate::meta::cache::{CacheConfig, MetaCache};
use crate::meta::store::{DirEntry, FileAttr, MetaError, MetaStore};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use std::sync::Arc;

/// Metadata client with caching
///
/// Acts as an intermediary between VFS and MetaStore, providing:
/// - Transparent caching with TTL
/// - Batch operations
/// - Cache invalidation
/// - Optimistic concurrency control
pub struct MetaClient {
    /// Backend metadata store
    store: Arc<dyn MetaStore>,
    
    /// Metadata cache
    cache: Arc<MetaCache>,
}

impl MetaClient {
    /// Create new metadata client with default cache configuration
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self::with_config(store, CacheConfig::default())
    }

    /// Create new metadata client with custom cache configuration
    pub fn with_config(store: Arc<dyn MetaStore>, cache_config: CacheConfig) -> Self {
        Self {
            store,
            cache: Arc::new(MetaCache::new(cache_config)),
        }
    }

    /// Get the underlying MetaStore (for direct access if needed)
    pub fn store(&self) -> &Arc<dyn MetaStore> {
        &self.store
    }

    /// Get the cache instance
    pub fn cache(&self) -> &Arc<MetaCache> {
        &self.cache
    }

    // ==================== Query Operations ====================

    /// Get file attributes with caching
    pub async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError> {
        // Check cache first
        if let Some(attr) = self.cache.get_attr(ino) {
            return Ok(attr);
        }

        // Cache miss, query backend
        let attr = self.store.getattr(ino).await?;

        // Update cache
        self.cache.put_attr(ino, attr.clone());

        Ok(attr)
    }

    /// Get attributes for multiple inodes (batch operation)
    pub async fn getattr_batch(&self, inos: &[Inode]) -> Result<Vec<(Inode, FileAttr)>, MetaError> {
        let mut results = Vec::with_capacity(inos.len());
        let mut cache_misses = Vec::new();

        // Check cache first
        for &ino in inos {
            if let Some(attr) = self.cache.get_attr(ino) {
                results.push((ino, attr));
            } else {
                cache_misses.push(ino);
            }
        }

        // Batch query for cache misses
        if !cache_misses.is_empty() {
            let backend_results = self.store.getattr_batch(&cache_misses).await?;

            for (ino, attr) in &backend_results {
                self.cache.put_attr(*ino, attr.clone());
            }

            results.extend(backend_results);
        }

        Ok(results)
    }

    /// Look up a directory entry by name with caching
    pub async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError> {
        // Check dentry cache
        if let Some(child) = self.cache.get_dentry(parent, name) {
            return Ok(child);
        }

        // Check negative cache
        let neg_key = format!("{}:{}", parent.0, name);
        if self.cache.is_negative(&neg_key) {
            return Err(MetaError::NotFound(parent.0));
        }

        // Cache miss, query backend
        match self.store.lookup(parent, name).await {
            Ok(child) => {
                // Update caches
                self.cache.put_dentry(parent, name, child);
                self.cache.remove_negative(&neg_key);
                Ok(child)
            }
            Err(e) => {
                // Add to negative cache for NotFound errors
                if matches!(e, MetaError::NotFound(_)) {
                    self.cache.put_negative(&neg_key);
                }
                Err(e)
            }
        }
    }

    /// Read directory entries with caching
    pub async fn readdir(&self, ino: Inode) -> Result<Vec<DirEntry>, MetaError> {
        // For readdir, we don't cache the full list to avoid stale data
        // But we do update the dentry cache for each entry
        let entries = self.store.readdir(ino).await?;

        // Update dentry cache
        for entry in &entries {
            self.cache.put_dentry(ino, &entry.name, Inode(entry.ino));
        }

        Ok(entries)
    }

    /// Read directory entries with attributes (optimized)
    pub async fn readdirplus(&self, ino: Inode) -> Result<Vec<(DirEntry, FileAttr)>, MetaError> {
        let results = self.store.readdirplus(ino).await?;

        // Update both dentry and attr caches
        for (entry, attr) in &results {
            self.cache.put_dentry(ino, &entry.name, Inode(entry.ino));
            self.cache.put_attr(Inode(entry.ino), attr.clone());
        }

        Ok(results)
    }

    // ==================== Creation Operations ====================

    /// Create a new file or directory
    pub async fn create(&self, params: CreateParams) -> Result<(Inode, FileAttr), MetaError> {
        let (ino, attr) = self.store.create(params.clone()).await?;

        // Update caches
        self.cache.put_attr(ino, attr.clone());
        self.cache.put_dentry(params.parent, &params.name, ino);
        
        // Remove from negative cache if present
        let neg_key = format!("{}:{}", params.parent.0, params.name);
        self.cache.remove_negative(&neg_key);

        // Invalidate parent directory
        self.cache.invalidate_dir(params.parent);

        Ok((ino, attr))
    }

    // ==================== Modification Operations ====================

    /// Update file attributes
    pub async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> Result<FileAttr, MetaError> {
        let attr = self.store.setattr(ino, mask).await?;

        // Update cache with new attributes
        self.cache.put_attr(ino, attr.clone());

        Ok(attr)
    }

    /// Rename/move a file or directory
    pub async fn rename(
        &self,
        old_parent: Inode,
        old_name: &str,
        new_parent: Inode,
        new_name: String,
    ) -> Result<(), MetaError> {
        self.store
            .rename(old_parent, old_name, new_parent, new_name.clone())
            .await?;

        // Invalidate affected caches
        self.cache.invalidate_dentry(old_parent, old_name);
        self.cache.invalidate_dir(old_parent);
        self.cache.invalidate_dir(new_parent);

        // Remove old negative cache entry
        let old_neg_key = format!("{}:{}", old_parent.0, old_name);
        self.cache.remove_negative(&old_neg_key);

        // Note: We don't add new dentry to cache here, let it be populated on lookup

        Ok(())
    }

    // ==================== Deletion Operations ====================

    /// Remove a file
    pub async fn unlink(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        // Get the inode before unlinking (for cache invalidation)
        let child_ino = self.lookup(parent, name).await?;

        self.store.unlink(parent, name).await?;

        // Invalidate caches
        self.cache.invalidate_attr(child_ino);
        self.cache.invalidate_dentry(parent, name);
        self.cache.invalidate_dir(parent);

        // Add to negative cache
        let neg_key = format!("{}:{}", parent.0, name);
        self.cache.put_negative(&neg_key);

        Ok(())
    }

    /// Remove a directory (must be empty)
    pub async fn rmdir(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        // Get the inode before removing (for cache invalidation)
        let child_ino = self.lookup(parent, name).await?;

        self.store.rmdir(parent, name).await?;

        // Invalidate caches
        self.cache.invalidate_attr(child_ino);
        self.cache.invalidate_dentry(parent, name);
        self.cache.invalidate_dir(parent);
        self.cache.invalidate_dir(child_ino); // Clear any cached children

        // Add to negative cache
        let neg_key = format!("{}:{}", parent.0, name);
        self.cache.put_negative(&neg_key);

        Ok(())
    }

    // ==================== Path Resolution ====================

    /// Resolve a path to an inode
    ///
    /// This is a convenience method that handles path parsing and
    /// sequential lookups with caching.
    pub async fn resolve_path(&self, path: &str) -> Result<Inode, MetaError> {
        // Check path cache first
        if let Some(ino) = self.cache.get_path(path) {
            return Ok(ino);
        }

        // Special case for root
        if path == "/" {
            let root = self.root_ino();
            self.cache.put_path(path, root);
            return Ok(root);
        }

        // Parse and resolve path components
        let components: Vec<&str> = path
            .trim_start_matches('/')
            .split('/')
            .filter(|s| !s.is_empty())
            .collect();

        let mut current = self.root_ino();

        for component in components {
            current = self.lookup(current, component).await?;
        }

        // Cache the full path resolution
        self.cache.put_path(path, current);

        Ok(current)
    }

    // ==================== System Operations ====================

    /// Initialize the metadata store
    pub async fn initialize(&self) -> Result<(), MetaError> {
        self.store.initialize().await
    }

    /// Get the root directory inode
    pub fn root_ino(&self) -> Inode {
        self.store.root_ino()
    }

    // ==================== Advanced Operations ====================

    /// Prefetch directory contents (load children and their attributes)
    ///
    /// This is useful for operations that will access many entries,
    /// reducing subsequent lookup latency.
    pub async fn prefetch_directory(&self, parent: Inode) -> Result<(), MetaError> {
        let results = self.readdirplus(parent).await?;

        // Results are already cached by readdirplus
        log::debug!(
            "Prefetched {} entries for directory {}",
            results.len(),
            parent.0
        );

        Ok(())
    }

    /// Batch lookup multiple names in the same directory
    pub async fn lookup_batch(
        &self,
        parent: Inode,
        names: &[&str],
    ) -> Result<Vec<(String, Inode)>, MetaError> {
        let mut results = Vec::with_capacity(names.len());
        let mut cache_misses = Vec::new();

        // Check cache first
        for &name in names {
            if let Some(child) = self.cache.get_dentry(parent, name) {
                results.push((name.to_string(), child));
            } else {
                cache_misses.push(name);
            }
        }

        // For cache misses, do individual lookups
        // (Backend doesn't have batch lookup yet)
        for &name in &cache_misses {
            match self.lookup(parent, name).await {
                Ok(child) => {
                    results.push((name.to_string(), child));
                }
                Err(_) => {
                    // Skip errors, just don't include in results
                }
            }
        }

        Ok(results)
    }

    /// Clear all caches (useful for testing or forced refresh)
    pub fn clear_cache(&self) {
        self.cache.clear_all();
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> crate::meta::cache::CacheStats {
        self.cache.stats()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::meta::create_meta_store_from_url;
    use crate::vfs::fs::FileType;

    #[tokio::test]
    async fn test_meta_client_basic() {
        let store = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap();
        let client = MetaClient::new(store);

        client.initialize().await.unwrap();

        // Test root
        let root = client.root_ino();
        assert_eq!(root.0, 1);

        // Test create directory
        let params = CreateParams::dir(root, "test_dir".to_string(), 1000, 1000);
        let (dir_ino, dir_attr) = client.create(params).await.unwrap();
        assert_eq!(dir_attr.kind, FileType::Dir);

        // Test lookup (should hit cache)
        let found_ino = client.lookup(root, "test_dir").await.unwrap();
        assert_eq!(found_ino, dir_ino);

        // Test getattr (should hit cache)
        let attr = client.getattr(dir_ino).await.unwrap();
        assert_eq!(attr.kind, FileType::Dir);
    }

    #[tokio::test]
    async fn test_meta_client_negative_cache() {
        let store = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap();
        let client = MetaClient::new(store);

        client.initialize().await.unwrap();
        let root = client.root_ino();

        // Lookup non-existent entry
        let result = client.lookup(root, "nonexistent").await;
        assert!(result.is_err());

        // Second lookup should hit negative cache
        let result2 = client.lookup(root, "nonexistent").await;
        assert!(result2.is_err());
    }

    #[tokio::test]
    async fn test_meta_client_resolve_path() {
        let store = create_meta_store_from_url("sqlite::memory:")
            .await
            .unwrap();
        let client = MetaClient::new(store);

        client.initialize().await.unwrap();
        let root = client.root_ino();

        // Create /a/b/c
        let params_a = CreateParams::dir(root, "a".to_string(), 1000, 1000);
        let (ino_a, _) = client.create(params_a).await.unwrap();

        let params_b = CreateParams::dir(ino_a, "b".to_string(), 1000, 1000);
        let (ino_b, _) = client.create(params_b).await.unwrap();

        let params_c = CreateParams::dir(ino_b, "c".to_string(), 1000, 1000);
        let (ino_c, _) = client.create(params_c).await.unwrap();

        // Test path resolution
        let resolved = client.resolve_path("/a/b/c").await.unwrap();
        assert_eq!(resolved, ino_c);

        // Second resolution should hit path cache
        let resolved2 = client.resolve_path("/a/b/c").await.unwrap();
        assert_eq!(resolved2, ino_c);
    }
}

