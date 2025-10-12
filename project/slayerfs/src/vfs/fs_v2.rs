//! VFS V2: Modern filesystem interface using MetaStoreV2
//!
//! This module provides a clean, type-safe VFS layer built on top of MetaStoreV2.
//! It manages the in-memory namespace cache and coordinates between metadata and
//! block storage for file operations.

use crate::chuck::chunk::ChunkLayout;
use crate::chuck::reader::ChunkReader;
use crate::chuck::store::BlockStore;
use crate::chuck::util::split_file_range_into_chunks;
use crate::chuck::writer::ChunkWriter;
use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::store_v2::MetaStoreV2;
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::vfs::fs::FileType;
use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::Mutex;

/// VFS operation errors
#[derive(Debug, thiserror::Error)]
pub enum VfsError {
    #[error("Path not found: {0}")]
    PathNotFound(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("Not a directory: {0}")]
    NotDirectory(String),

    #[error("Not a file: {0}")]
    NotFile(String),

    #[error("Already exists: {0}")]
    AlreadyExists(String),

    #[error("Directory not empty: {0}")]
    DirectoryNotEmpty(String),

    #[error("Metadata error: {0}")]
    Meta(#[from] MetaError),

    #[error("IO error: {0}")]
    Io(String),
}

/// Result type for VFS operations
pub type VfsResult<T> = Result<T, VfsError>;

/// In-memory namespace node for path resolution
///
/// VNode maintains the directory tree structure in memory, caching the
/// parent-child relationships. The actual metadata (size, timestamps, etc.)
/// is stored in MetaStore.
struct VNode {
    /// Node type: file or directory
    kind: FileType,
    /// Base name of this node (not full path)
    name: String,
    /// Parent inode (None only for root)
    parent: Option<Inode>,
    /// Directory children: name -> inode (empty for files)
    children: HashMap<String, Inode>,
}

impl VNode {
    fn dir(name: String, parent: Option<Inode>) -> Self {
        Self {
            kind: FileType::Dir,
            name,
            parent,
            children: HashMap::new(),
        }
    }

    fn file(name: String, parent: Option<Inode>) -> Self {
        Self {
            kind: FileType::File,
            name,
            parent,
            children: HashMap::new(),
        }
    }
}

/// In-memory namespace for path-to-inode mapping
///
/// Namespace caches the directory structure to enable efficient path
/// lookups without querying MetaStore for every path component.
struct Namespace {
    /// inode -> VNode mapping
    nodes: HashMap<Inode, VNode>,
    /// Canonical path -> inode mapping (for fast path lookup)
    lookup: HashMap<String, Inode>,
}

impl Namespace {
    fn new(root: Inode) -> Self {
        let mut nodes = HashMap::new();
        let mut lookup = HashMap::new();

        nodes.insert(root, VNode::dir("".into(), None));
        lookup.insert("/".into(), root);

        Self { nodes, lookup }
    }

    /// Insert a node and update path lookup
    fn insert_node(&mut self, ino: Inode, node: VNode, path: &str) {
        self.nodes.insert(ino, node);
        self.lookup.insert(path.to_string(), ino);
    }

    /// Remove a node and its path lookup
    fn remove_node(&mut self, ino: Inode, path: &str) {
        self.nodes.remove(&ino);
        self.lookup.remove(path);
    }

    /// Build absolute path for an inode
    fn build_path(&self, ino: Inode, root: Inode) -> Option<String> {
        if ino == root {
            return Some("/".into());
        }

        let mut parts = Vec::new();
        let mut cur = ino;

        loop {
            let node = self.nodes.get(&cur)?;
            if node.parent.is_none() {
                break;
            }
            parts.push(node.name.clone());
            cur = node.parent?;
        }

        if parts.is_empty() {
            return Some("/".into());
        }

        parts.reverse();
        Some(format!("/{}", parts.join("/")))
    }
}

/// Filesystem V2 interface
///
/// FileSystemV2 provides a complete filesystem abstraction with:
/// - Path-based operations (create, mkdir, read, write, etc.)
/// - Inode-based operations (for FUSE compatibility)
/// - Efficient namespace caching
/// - Integrated chunk storage
#[async_trait]
pub trait FileSystemV2: Send + Sync {
    // === Path-based operations ===

    /// Create a directory at the given path
    async fn mkdir(&self, path: &str, mode: u32, uid: u32, gid: u32) -> VfsResult<Inode>;

    /// Create a directory and all parent directories (like mkdir -p)
    async fn mkdir_p(&self, path: &str) -> VfsResult<Inode>;

    /// Create a file at the given path
    async fn create(&self, path: &str, mode: u32, uid: u32, gid: u32) -> VfsResult<Inode>;

    /// Remove a file
    async fn unlink(&self, path: &str) -> VfsResult<()>;

    /// Remove an empty directory
    async fn rmdir(&self, path: &str) -> VfsResult<()>;

    /// Rename/move a file or directory
    async fn rename(&self, old_path: &str, new_path: &str) -> VfsResult<()>;

    /// Get file attributes by path
    async fn stat(&self, path: &str) -> VfsResult<FileAttr>;

    /// List directory contents by path
    async fn readdir(&self, path: &str) -> VfsResult<Vec<DirEntry>>;

    /// Write data to a file
    async fn write(&self, path: &str, offset: u64, data: &[u8]) -> VfsResult<usize>;

    /// Read data from a file
    async fn read(&self, path: &str, offset: u64, len: usize) -> VfsResult<Vec<u8>>;

    // === Inode-based operations (for FUSE) ===

    /// Get file attributes by inode
    async fn getattr(&self, ino: Inode) -> VfsResult<FileAttr>;

    /// List directory contents by inode
    async fn readdir_ino(&self, ino: Inode) -> VfsResult<Vec<DirEntry>>;

    /// Write data to a file by inode
    async fn write_ino(&self, ino: Inode, offset: u64, data: &[u8]) -> VfsResult<usize>;

    /// Read data from a file by inode
    async fn read_ino(&self, ino: Inode, offset: u64, len: usize) -> VfsResult<Vec<u8>>;

    /// Set file attributes
    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> VfsResult<FileAttr>;

    /// Lookup a name in a directory
    async fn lookup(&self, parent: Inode, name: &str) -> VfsResult<Inode>;

    // === Utility operations ===

    /// Get root inode
    fn root_ino(&self) -> Inode;

    /// Get parent inode (returns root for root)
    fn parent_of(&self, ino: Inode) -> Option<Inode>;

    /// Get absolute path for an inode
    fn path_of(&self, ino: Inode) -> Option<String>;
}

/// VFS V2 implementation
///
/// This is the main VFS implementation that combines:
/// - MetaStoreV2 for metadata operations
/// - BlockStore for chunk data
/// - In-memory namespace for path caching
pub struct VfsV2<S: BlockStore, M: MetaStoreV2> {
    /// Chunk layout configuration
    layout: ChunkLayout,
    /// Block storage for chunk data
    store: tokio::sync::Mutex<S>,
    /// Metadata store
    meta: M,
    /// Base offset for chunk ID calculation
    base: i64,
    /// In-memory namespace cache
    ns: Mutex<Namespace>,
    /// Root inode
    root: Inode,
}

impl<S: BlockStore, M: MetaStoreV2> VfsV2<S, M> {
    /// Create a new VFS V2 instance
    ///
    /// This will initialize the metadata store and load the directory tree
    /// from persistent storage into the in-memory namespace cache.
    pub async fn new(layout: ChunkLayout, store: S, meta: M) -> VfsResult<Self> {
        // Initialize metadata store
        meta.initialize().await?;

        let root = meta.root_ino();
        let ns = Namespace::new(root);

        // Chunk ID base offset to avoid conflicts
        let base = 1_000_000_000i64;

        let vfs = Self {
            layout,
            store: tokio::sync::Mutex::new(store),
            meta,
            base,
            ns: Mutex::new(ns),
            root,
        };

        // Load existing directory tree
        vfs.load_tree_from_meta().await?;

        Ok(vfs)
    }

    /// Load directory tree from MetaStore into namespace cache
    ///
    /// This is called during VFS initialization to rebuild the in-memory
    /// namespace from persistent storage.
    async fn load_tree_from_meta(&self) -> VfsResult<()> {
        let mut queue = vec![("/".to_string(), self.root)];

        while let Some((path, ino)) = queue.pop() {
            let entries = self.meta.readdir(ino).await?;

            let mut ns = self.ns.lock().unwrap();

            // Ensure parent node exists
            if !ns.nodes.contains_key(&ino) {
                ns.nodes.insert(ino, VNode::dir("".to_string(), None));
            }

            // Update parent's children map
            if let Some(parent) = ns.nodes.get_mut(&ino) {
                parent.children.clear();
                for entry in &entries {
                    parent.children.insert(entry.name.clone(), Inode(entry.ino));
                }
            }

            // Insert child nodes and queue directories
            for entry in entries {
                let child_ino = Inode(entry.ino);
                let child_path = if path == "/" {
                    format!("/{}", entry.name)
                } else {
                    format!("{}/{}", path, entry.name)
                };

                let node = match entry.kind {
                    FileType::Dir => VNode::dir(entry.name.clone(), Some(ino)),
                    FileType::File => VNode::file(entry.name.clone(), Some(ino)),
                };

                ns.insert_node(child_ino, node, &child_path);

                // Queue directories for recursive loading
                if entry.kind == FileType::Dir {
                    queue.push((child_path, child_ino));
                }
            }
        }

        Ok(())
    }

    /// Resolve a path to an inode
    ///
    /// Returns None if the path doesn't exist or is invalid.
    fn resolve_path(&self, path: &str) -> Option<Inode> {
        let ns = self.ns.lock().unwrap();
        ns.lookup.get(path).copied()
    }

    /// Parse path into (parent_path, parent_inode, basename)
    ///
    /// For "/foo/bar", returns ("/foo", parent_ino, "bar")
    /// For "/foo", returns ("/", root_ino, "foo")
    fn parse_path(&self, path: &str) -> VfsResult<(String, Inode, String)> {
        if path.is_empty() || !path.starts_with('/') {
            return Err(VfsError::InvalidPath(path.to_string()));
        }

        if path == "/" {
            return Err(VfsError::InvalidPath(
                "Cannot parse root path".to_string(),
            ));
        }

        // Find the last '/' to split parent and basename
        let last_slash = path.rfind('/').unwrap(); // Must exist since path starts with '/'
        
        let (parent_path, basename) = if last_slash == 0 {
            // Path like "/foo" -> parent is "/", basename is "foo"
            ("/".to_string(), path[1..].to_string())
        } else {
            // Path like "/foo/bar" -> parent is "/foo", basename is "bar"
            (path[..last_slash].to_string(), path[last_slash + 1..].to_string())
        };

        let parent_ino = self
            .resolve_path(&parent_path)
            .ok_or_else(|| VfsError::PathNotFound(parent_path.clone()))?;

        Ok((parent_path, parent_ino, basename))
    }

    /// Calculate chunk ID from file inode and chunk index
    fn chunk_id(&self, ino: Inode, chunk_idx: u64) -> i64 {
        self.base + ino.as_i64() * 1000 + chunk_idx as i64
    }

    /// Write data to chunks
    async fn write_chunks(
        &self,
        ino: Inode,
        offset: u64,
        data: &[u8],
    ) -> VfsResult<usize> {
        let spans = split_file_range_into_chunks(self.layout, offset, data.len());
        let mut total_written = 0;
        let mut data_offset = 0;

        for span in spans {
            let chunk_id = self.chunk_id(ino, span.chunk_index);
            let chunk_data = &data[data_offset..data_offset + span.len];

            let mut store = self.store.lock().await;
            let mut writer = ChunkWriter::new(self.layout, chunk_id, &mut *store);
            writer.write(span.offset_in_chunk, chunk_data).await;
            
            total_written += span.len;
            data_offset += span.len;
        }

        Ok(total_written)
    }

    /// Read data from chunks
    async fn read_chunks(
        &self,
        ino: Inode,
        offset: u64,
        len: usize,
    ) -> VfsResult<Vec<u8>> {
        let spans = split_file_range_into_chunks(self.layout, offset, len);
        let mut result = Vec::with_capacity(len);

        for span in spans {
            let chunk_id = self.chunk_id(ino, span.chunk_index);

            let store = self.store.lock().await;
            let reader = ChunkReader::new(self.layout, chunk_id, &*store);
            let chunk_data = reader.read(span.offset_in_chunk, span.len).await;
            result.extend_from_slice(&chunk_data);
        }

        Ok(result)
    }
}

#[async_trait]
impl<S: BlockStore + Send + Sync, M: MetaStoreV2> FileSystemV2 for VfsV2<S, M> {
    async fn mkdir(&self, path: &str, mode: u32, uid: u32, gid: u32) -> VfsResult<Inode> {
        let (_parent_path, parent_ino, basename) = self.parse_path(path)?;

        let mut params = CreateParams::dir(parent_ino, basename.clone(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            let node = VNode::dir(basename.clone(), Some(parent_ino));
            ns.insert_node(ino, node, path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.insert(basename, ino);
            }
        }

        Ok(ino)
    }

    async fn mkdir_p(&self, path: &str) -> VfsResult<Inode> {
        if path == "/" {
            return Ok(self.root);
        }

        // Check if already exists
        if let Some(ino) = self.resolve_path(path) {
            return Ok(ino);
        }

        // Split path and create parent first
        let parts: Vec<&str> = path.trim_matches('/').split('/').collect();
        let mut current_path = String::new();

        for part in &parts[..parts.len() - 1] {
            current_path.push('/');
            current_path.push_str(part);

            if self.resolve_path(&current_path).is_none() {
                self.mkdir(&current_path, 0o755, 0, 0).await?;
            }
        }

        // Create final directory
        self.mkdir(path, 0o755, 0, 0).await
    }

    async fn create(&self, path: &str, mode: u32, uid: u32, gid: u32) -> VfsResult<Inode> {
        let (_parent_path, parent_ino, basename) = self.parse_path(path)?;

        let mut params = CreateParams::file(parent_ino, basename.clone(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            let node = VNode::file(basename.clone(), Some(parent_ino));
            ns.insert_node(ino, node, path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.insert(basename, ino);
            }
        }

        Ok(ino)
    }

    async fn unlink(&self, path: &str) -> VfsResult<()> {
        let (_parent_path, parent_ino, basename) = self.parse_path(path)?;
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;

        self.meta.unlink(parent_ino, &basename).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            ns.remove_node(ino, path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.remove(&basename);
            }
        }

        Ok(())
    }

    async fn rmdir(&self, path: &str) -> VfsResult<()> {
        let (_parent_path, parent_ino, basename) = self.parse_path(path)?;
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;

        self.meta.rmdir(parent_ino, &basename).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            ns.remove_node(ino, path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.remove(&basename);
            }
        }

        Ok(())
    }

    async fn rename(&self, old_path: &str, new_path: &str) -> VfsResult<()> {
        let (_old_parent_path, old_parent, old_name) = self.parse_path(old_path)?;
        let (_new_parent_path, new_parent, new_name) = self.parse_path(new_path)?;

        let ino = self
            .resolve_path(old_path)
            .ok_or_else(|| VfsError::PathNotFound(old_path.to_string()))?;

        self.meta
            .rename(old_parent, &old_name, new_parent, new_name.clone())
            .await?;

        // Update namespace - need to recursively update all child paths
        {
            let mut ns = self.ns.lock().unwrap();

            // Collect all descendants to update their paths
            let mut to_update = vec![(ino, old_path.to_string(), new_path.to_string())];
            let mut i = 0;
            
            while i < to_update.len() {
                let (current_ino, old_p, new_p) = to_update[i].clone();
                
                // Find children of current node
                if let Some(node) = ns.nodes.get(&current_ino) {
                    for (child_name, &child_ino) in &node.children {
                        let child_old_path = if old_p == "/" {
                            format!("/{}", child_name)
                        } else {
                            format!("{}/{}", old_p, child_name)
                        };
                        let child_new_path = if new_p == "/" {
                            format!("/{}", child_name)
                        } else {
                            format!("{}/{}", new_p, child_name)
                        };
                        to_update.push((child_ino, child_old_path, child_new_path));
                    }
                }
                i += 1;
            }

            // Now update all paths in reverse order (children first)
            for (update_ino, old_p, new_p) in to_update.iter().rev() {
                ns.lookup.remove(old_p);
                ns.lookup.insert(new_p.clone(), *update_ino);
            }

            // Update node parent and name for the renamed item
            if let Some(node) = ns.nodes.get_mut(&ino) {
                node.name = new_name.clone();
                node.parent = Some(new_parent);
            }

            // Update old parent's children
            if let Some(parent) = ns.nodes.get_mut(&old_parent) {
                parent.children.remove(&old_name);
            }

            // Update new parent's children
            if let Some(parent) = ns.nodes.get_mut(&new_parent) {
                parent.children.insert(new_name, ino);
            }
        }

        Ok(())
    }

    async fn stat(&self, path: &str) -> VfsResult<FileAttr> {
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;
        self.getattr(ino).await
    }

    async fn readdir(&self, path: &str) -> VfsResult<Vec<DirEntry>> {
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;
        self.readdir_ino(ino).await
    }

    async fn write(&self, path: &str, offset: u64, data: &[u8]) -> VfsResult<usize> {
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;
        self.write_ino(ino, offset, data).await
    }

    async fn read(&self, path: &str, offset: u64, len: usize) -> VfsResult<Vec<u8>> {
        let ino = self
            .resolve_path(path)
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))?;
        self.read_ino(ino, offset, len).await
    }

    async fn getattr(&self, ino: Inode) -> VfsResult<FileAttr> {
        let attr = self.meta.getattr(ino).await?;
        Ok(attr)
    }

    async fn readdir_ino(&self, ino: Inode) -> VfsResult<Vec<DirEntry>> {
        let entries = self.meta.readdir(ino).await?;
        Ok(entries)
    }

    async fn write_ino(&self, ino: Inode, offset: u64, data: &[u8]) -> VfsResult<usize> {
        // Write chunks
        let written = self.write_chunks(ino, offset, data).await?;

        // Update file size if needed
        let new_size = offset + written as u64;
        let attr = self.meta.getattr(ino).await?;

        if new_size > attr.size {
            let mask = SetAttrMask::size(new_size);
            self.meta.setattr(ino, mask).await?;
        }

        Ok(written)
    }

    async fn read_ino(&self, ino: Inode, offset: u64, len: usize) -> VfsResult<Vec<u8>> {
        // Get file size
        let attr = self.meta.getattr(ino).await?;

        // Clamp read length to file size
        let actual_len = if offset >= attr.size {
            0
        } else {
            len.min((attr.size - offset) as usize)
        };

        if actual_len == 0 {
            return Ok(Vec::new());
        }

        self.read_chunks(ino, offset, actual_len).await
    }

    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> VfsResult<FileAttr> {
        let attr = self.meta.setattr(ino, mask).await?;
        Ok(attr)
    }

    async fn lookup(&self, parent: Inode, name: &str) -> VfsResult<Inode> {
        let ino = self.meta.lookup(parent, name).await?;
        Ok(ino)
    }

    fn root_ino(&self) -> Inode {
        self.root
    }

    fn parent_of(&self, ino: Inode) -> Option<Inode> {
        let ns = self.ns.lock().unwrap();
        let node = ns.nodes.get(&ino)?;
        Some(node.parent.unwrap_or(self.root))
    }

    fn path_of(&self, ino: Inode) -> Option<String> {
        let ns = self.ns.lock().unwrap();
        ns.build_path(ino, self.root)
    }
}

#[cfg(test)]
mod tests {
    // Tests will be added in the test module
}
