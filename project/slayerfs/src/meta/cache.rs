//! Metadata cache for MetaClient
//!
//! Implements LRU cache with TTL support for file attributes,
//! directory entries, and path lookups.

use crate::meta::store::FileAttr;
use crate::meta::types::Inode;
use lru::LruCache;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Configuration for metadata cache
#[derive(Debug, Clone)]
pub struct CacheConfig {
    /// Maximum number of file attributes to cache
    pub attr_capacity: usize,
    /// Maximum number of directory entries to cache
    pub dentry_capacity: usize,
    /// Maximum number of path lookups to cache
    pub path_capacity: usize,
    /// Maximum number of negative entries to cache
    pub negative_capacity: usize,
    /// TTL for attribute cache entries
    pub attr_ttl: Duration,
    /// TTL for directory entry cache
    pub dentry_ttl: Duration,
    /// TTL for path cache
    pub path_ttl: Duration,
    /// TTL for negative cache (shorter to detect new files quickly)
    pub negative_ttl: Duration,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            attr_capacity: 10000,
            dentry_capacity: 10000,
            path_capacity: 5000,
            negative_capacity: 1000,
            attr_ttl: Duration::from_secs(60),    // 1 minute
            dentry_ttl: Duration::from_secs(60),  // 1 minute
            path_ttl: Duration::from_secs(30),    // 30 seconds
            negative_ttl: Duration::from_secs(5), // 5 seconds
        }
    }
}

/// Cached entry with expiration
#[derive(Debug, Clone)]
struct CachedEntry<T> {
    value: T,
    expire_at: Instant,
    version: u64,
}

impl<T> CachedEntry<T> {
    fn new(value: T, ttl: Duration, version: u64) -> Self {
        Self {
            value,
            expire_at: Instant::now() + ttl,
            version,
        }
    }

    fn is_expired(&self) -> bool {
        Instant::now() > self.expire_at
    }

    fn is_valid(&self, expected_version: Option<u64>) -> bool {
        if self.is_expired() {
            return false;
        }

        if let Some(expected) = expected_version {
            if self.version != expected {
                return false;
            }
        }

        true
    }
}

/// Metadata cache with LRU eviction and TTL
pub struct MetaCache {
    config: CacheConfig,

    /// File attribute cache: Inode -> FileAttr
    attr_cache: Arc<Mutex<LruCache<Inode, CachedEntry<FileAttr>>>>,

    /// Directory entry cache: (parent_inode, name) -> child_inode
    dentry_cache: Arc<Mutex<LruCache<(Inode, String), CachedEntry<Inode>>>>,

    /// Path cache: path -> inode (for fast path resolution)
    path_cache: Arc<Mutex<LruCache<String, CachedEntry<Inode>>>>,

    /// Negative cache: path or (parent, name) -> expiration time
    negative_cache: Arc<Mutex<LruCache<String, Instant>>>,
}

impl MetaCache {
    pub fn new(config: CacheConfig) -> Self {
        Self {
            attr_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(config.attr_capacity).unwrap(),
            ))),
            dentry_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(config.dentry_capacity).unwrap(),
            ))),
            path_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(config.path_capacity).unwrap(),
            ))),
            negative_cache: Arc::new(Mutex::new(LruCache::new(
                NonZeroUsize::new(config.negative_capacity).unwrap(),
            ))),
            config,
        }
    }

    // ==================== Attribute Cache ====================

    /// Get cached file attributes
    pub fn get_attr(&self, ino: Inode) -> Option<FileAttr> {
        let mut cache = self.attr_cache.lock().unwrap();

        if let Some(entry) = cache.get(&ino) {
            if entry.is_valid(None) {
                return Some(entry.value.clone());
            } else {
                // Expired, remove it
                cache.pop(&ino);
            }
        }

        None
    }

    /// Get cached attributes with version check
    pub fn get_attr_versioned(&self, ino: Inode, expected_version: u64) -> Option<FileAttr> {
        let mut cache = self.attr_cache.lock().unwrap();

        if let Some(entry) = cache.get(&ino) {
            if entry.is_valid(Some(expected_version)) {
                return Some(entry.value.clone());
            } else {
                cache.pop(&ino);
            }
        }

        None
    }

    /// Put file attributes into cache
    pub fn put_attr(&self, ino: Inode, attr: FileAttr) {
        let mut cache = self.attr_cache.lock().unwrap();
        let version = attr.version;
        let entry = CachedEntry::new(attr, self.config.attr_ttl, version);
        cache.put(ino, entry);
    }

    /// Invalidate specific inode's attributes
    pub fn invalidate_attr(&self, ino: Inode) {
        let mut cache = self.attr_cache.lock().unwrap();
        cache.pop(&ino);
    }

    /// Batch invalidate attributes
    pub fn invalidate_attrs(&self, inos: &[Inode]) {
        let mut cache = self.attr_cache.lock().unwrap();
        for &ino in inos {
            cache.pop(&ino);
        }
    }

    // ==================== Directory Entry Cache ====================

    /// Get cached directory entry
    pub fn get_dentry(&self, parent: Inode, name: &str) -> Option<Inode> {
        let mut cache = self.dentry_cache.lock().unwrap();
        let key = (parent, name.to_string());

        if let Some(entry) = cache.get(&key) {
            if entry.is_valid(None) {
                return Some(entry.value);
            } else {
                cache.pop(&key);
            }
        }

        None
    }

    /// Put directory entry into cache
    pub fn put_dentry(&self, parent: Inode, name: &str, child: Inode) {
        let mut cache = self.dentry_cache.lock().unwrap();
        let key = (parent, name.to_string());
        let entry = CachedEntry::new(child, self.config.dentry_ttl, 0);
        cache.put(key, entry);
    }

    /// Invalidate all entries in a directory
    pub fn invalidate_dir(&self, parent: Inode) {
        let mut cache = self.dentry_cache.lock().unwrap();

        // Remove all entries with matching parent
        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|((p, _), _)| *p == parent)
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    /// Invalidate specific directory entry
    pub fn invalidate_dentry(&self, parent: Inode, name: &str) {
        let mut cache = self.dentry_cache.lock().unwrap();
        let key = (parent, name.to_string());
        cache.pop(&key);
    }

    // ==================== Path Cache ====================

    /// Get cached path lookup result
    pub fn get_path(&self, path: &str) -> Option<Inode> {
        let mut cache = self.path_cache.lock().unwrap();
        let key = path.to_string();

        if let Some(entry) = cache.get(&key) {
            if entry.is_valid(None) {
                return Some(entry.value);
            } else {
                cache.pop(&key);
            }
        }

        None
    }

    /// Put path lookup result into cache
    pub fn put_path(&self, path: &str, ino: Inode) {
        let mut cache = self.path_cache.lock().unwrap();
        let key = path.to_string();
        let entry = CachedEntry::new(ino, self.config.path_ttl, 0);
        cache.put(key, entry);
    }

    /// Invalidate path cache entry
    pub fn invalidate_path(&self, path: &str) {
        let mut cache = self.path_cache.lock().unwrap();
        cache.pop(&path.to_string());
    }

    /// Invalidate all paths starting with prefix
    pub fn invalidate_path_prefix(&self, prefix: &str) {
        let mut cache = self.path_cache.lock().unwrap();

        let keys_to_remove: Vec<_> = cache
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();

        for key in keys_to_remove {
            cache.pop(&key);
        }
    }

    // ==================== Negative Cache ====================

    /// Check if entry is in negative cache (known to not exist)
    pub fn is_negative(&self, key: &str) -> bool {
        let mut cache = self.negative_cache.lock().unwrap();

        if let Some(&expire_at) = cache.get(&key.to_string()) {
            if Instant::now() < expire_at {
                return true;
            } else {
                cache.pop(&key.to_string());
            }
        }

        false
    }

    /// Add entry to negative cache
    pub fn put_negative(&self, key: &str) {
        let mut cache = self.negative_cache.lock().unwrap();
        let expire_at = Instant::now() + self.config.negative_ttl;
        cache.put(key.to_string(), expire_at);
    }

    /// Remove entry from negative cache
    pub fn remove_negative(&self, key: &str) {
        let mut cache = self.negative_cache.lock().unwrap();
        cache.pop(&key.to_string());
    }

    // ==================== Cache Management ====================

    /// Clear all caches
    pub fn clear_all(&self) {
        self.attr_cache.lock().unwrap().clear();
        self.dentry_cache.lock().unwrap().clear();
        self.path_cache.lock().unwrap().clear();
        self.negative_cache.lock().unwrap().clear();
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            attr_size: self.attr_cache.lock().unwrap().len(),
            attr_capacity: self.config.attr_capacity,
            dentry_size: self.dentry_cache.lock().unwrap().len(),
            dentry_capacity: self.config.dentry_capacity,
            path_size: self.path_cache.lock().unwrap().len(),
            path_capacity: self.config.path_capacity,
            negative_size: self.negative_cache.lock().unwrap().len(),
            negative_capacity: self.config.negative_capacity,
        }
    }
}

/// Cache statistics
#[derive(Debug, Clone)]
pub struct CacheStats {
    pub attr_size: usize,
    pub attr_capacity: usize,
    pub dentry_size: usize,
    pub dentry_capacity: usize,
    pub path_size: usize,
    pub path_capacity: usize,
    pub negative_size: usize,
    pub negative_capacity: usize,
}

impl std::fmt::Display for CacheStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Cache Stats:\n\
             - Attr: {}/{} ({:.1}%)\n\
             - Dentry: {}/{} ({:.1}%)\n\
             - Path: {}/{} ({:.1}%)\n\
             - Negative: {}/{} ({:.1}%)",
            self.attr_size,
            self.attr_capacity,
            self.attr_size as f64 / self.attr_capacity as f64 * 100.0,
            self.dentry_size,
            self.dentry_capacity,
            self.dentry_size as f64 / self.dentry_capacity as f64 * 100.0,
            self.path_size,
            self.path_capacity,
            self.path_size as f64 / self.path_capacity as f64 * 100.0,
            self.negative_size,
            self.negative_capacity,
            self.negative_size as f64 / self.negative_capacity as f64 * 100.0,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vfs::fs::FileType;

    fn make_test_attr(ino: i64, version: u64) -> FileAttr {
        FileAttr {
            ino,
            size: 0,
            kind: FileType::File,
            mode: 0o644,
            uid: 1000,
            gid: 1000,
            atime: 0,
            mtime: 0,
            ctime: 0,
            nlink: 1,
            blocks: 0,
            blksize: 4096,
            rdev: 0,
            version,
        }
    }

    #[test]
    fn test_attr_cache() {
        let cache = MetaCache::new(CacheConfig::default());
        let ino = Inode(100);
        let attr = make_test_attr(100, 1);

        // Cache miss
        assert!(cache.get_attr(ino).is_none());

        // Put and get
        cache.put_attr(ino, attr.clone());
        assert!(cache.get_attr(ino).is_some());

        // Invalidate
        cache.invalidate_attr(ino);
        assert!(cache.get_attr(ino).is_none());
    }

    #[test]
    fn test_dentry_cache() {
        let cache = MetaCache::new(CacheConfig::default());
        let parent = Inode(1);
        let child = Inode(100);

        assert!(cache.get_dentry(parent, "test.txt").is_none());

        cache.put_dentry(parent, "test.txt", child);
        assert_eq!(cache.get_dentry(parent, "test.txt"), Some(child));

        cache.invalidate_dentry(parent, "test.txt");
        assert!(cache.get_dentry(parent, "test.txt").is_none());
    }

    #[test]
    fn test_negative_cache() {
        let cache = MetaCache::new(CacheConfig::default());
        let key = "parent:1:name:nonexistent";

        assert!(!cache.is_negative(key));

        cache.put_negative(key);
        assert!(cache.is_negative(key));

        cache.remove_negative(key);
        assert!(!cache.is_negative(key));
    }

    #[tokio::test]
    async fn test_attr_ttl_expiration() {
        let mut config = CacheConfig::default();
        config.attr_ttl = Duration::from_millis(100);
        let cache = MetaCache::new(config);

        let ino = Inode(100);
        let attr = make_test_attr(100, 1);

        cache.put_attr(ino, attr);
        assert!(cache.get_attr(ino).is_some());

        // Wait for expiration
        tokio::time::sleep(Duration::from_millis(150)).await;
        assert!(cache.get_attr(ino).is_none());
    }

    #[test]
    fn test_version_check() {
        let cache = MetaCache::new(CacheConfig::default());
        let ino = Inode(100);
        let attr = make_test_attr(100, 5);

        cache.put_attr(ino, attr);

        // Valid version
        assert!(cache.get_attr_versioned(ino, 5).is_some());

        // Invalid version
        assert!(cache.get_attr_versioned(ino, 4).is_none());
        assert!(cache.get_attr_versioned(ino, 6).is_none());
    }

    #[test]
    fn test_invalidate_dir() {
        let cache = MetaCache::new(CacheConfig::default());
        let parent = Inode(1);

        cache.put_dentry(parent, "file1.txt", Inode(100));
        cache.put_dentry(parent, "file2.txt", Inode(101));
        cache.put_dentry(parent, "file3.txt", Inode(102));

        assert!(cache.get_dentry(parent, "file1.txt").is_some());
        assert!(cache.get_dentry(parent, "file2.txt").is_some());

        cache.invalidate_dir(parent);

        assert!(cache.get_dentry(parent, "file1.txt").is_none());
        assert!(cache.get_dentry(parent, "file2.txt").is_none());
        assert!(cache.get_dentry(parent, "file3.txt").is_none());
    }
}
