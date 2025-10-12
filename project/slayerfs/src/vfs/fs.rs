//! FUSE/SDK 友好的简化 VFS：基于路径的 create/mkdir/read/write/readdir/stat。

use crate::chuck::chunk::ChunkLayout;
use crate::chuck::reader::ChunkReader;
use crate::chuck::store::BlockStore;
use crate::chuck::util::{ChunkSpan, split_file_range_into_chunks};
use crate::chuck::writer::ChunkWriter;
use crate::meta::MetaStore;
use crate::meta::entities::content_meta::EntryType;
use rfuse3::raw::Filesystem;
use std::collections::HashMap;
use std::sync::Mutex;

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

#[derive(Clone, Debug)]
pub struct VfsFileAttr {
    pub ino: i64,
    pub size: u64,
    pub kind: FileType,
}

#[derive(Clone, Debug)]
pub struct VfsFileType {
    pub name: String,
    pub ino: i64,
    pub kind: FileType,
}

use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use async_trait::async_trait;

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

    #[error("Operation error: {0}")]
    Operation(String),
}

impl From<String> for VfsError {
    fn from(s: String) -> Self {
        VfsError::Operation(s)
    }
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

pub struct Vfs<S: BlockStore, M: MetaStore> {
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

impl<S: BlockStore, M: MetaStore> Vfs<S, M> {
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
            return Err(VfsError::InvalidPath("Cannot parse root path".to_string()));
        }

        // Find the last '/' to split parent and basename
        let last_slash = path.rfind('/').unwrap(); // Must exist since path starts with '/'

        let (parent_path, basename) = if last_slash == 0 {
            // Path like "/foo" -> parent is "/", basename is "foo"
            ("/".to_string(), path[1..].to_string())
        } else {
            // Path like "/foo/bar" -> parent is "/foo", basename is "bar"
            (
                path[..last_slash].to_string(),
                path[last_slash + 1..].to_string(),
            )
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

    /// Normalize path (remove empty segments, ensure leading slash)
    fn norm_path(p: &str) -> String {
        if p.is_empty() {
            return "/".into();
        }
        let parts: Vec<&str> = p.split('/').filter(|s| !s.is_empty()).collect();
        let mut out = String::from("/");
        out.push_str(&parts.join("/"));
        if out.is_empty() { "/".into() } else { out }
    }

    /// Split path into (parent_directory, filename)
    fn split_dir_file(path: &str) -> (String, String) {
        let n = path.rfind('/').unwrap_or(0);
        if n == 0 {
            ("/".into(), path[1..].into())
        } else {
            (path[..n].into(), path[n + 1..].into())
        }
    }

    /// Write data to chunks
    async fn write_chunks(&self, ino: Inode, offset: u64, data: &[u8]) -> VfsResult<usize> {
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
    async fn read_chunks(&self, ino: Inode, offset: u64, len: usize) -> VfsResult<Vec<u8>> {
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

    // ========================================================================
    // Public API methods for FUSE adapter
    // ========================================================================

    /// Get child inode by parent inode and name
    pub fn child_of(&self, parent: i64, name: &str) -> Option<Inode> {
        let parent_ino = Inode(parent);
        let ns = self.ns.lock().unwrap();
        let parent_node = ns.nodes.get(&parent_ino)?;
        parent_node.children.get(name).copied()
    }

    /// Get file attributes by inode (async version for FUSE)
    pub async fn stat_ino(&self, ino: i64) -> Option<VfsFileAttr> {
        let inode = Inode(ino);
        let attr = self.meta.getattr(inode).await.ok()?;
        Some(VfsFileAttr {
            ino: attr.ino,
            size: attr.size,
            kind: attr.kind,
        })
    }

    /// Read data by inode
    pub async fn read_ino(&self, ino: i64, offset: u64, len: usize) -> Result<Vec<u8>, VfsError> {
        let inode = Inode(ino);
        // Get file size
        let attr = self.meta.getattr(inode).await?;

        // Clamp read length to file size
        let actual_len = if offset >= attr.size {
            0
        } else {
            len.min((attr.size - offset) as usize)
        };

        if actual_len == 0 {
            return Ok(Vec::new());
        }

        self.read_chunks(inode, offset, actual_len).await
    }

    /// Write data by inode  
    pub async fn write_ino(&self, ino: i64, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        let inode = Inode(ino);
        // Write chunks
        let written = self.write_chunks(inode, offset, data).await?;

        // Update file size if needed
        let new_size = offset + written as u64;
        let attr = self.meta.getattr(inode).await?;

        if new_size > attr.size {
            let mask = crate::meta::types::SetAttrMask::size(new_size);
            self.meta.setattr(inode, mask).await?;
        }

        Ok(written)
    }

    /// Get absolute path for an inode
    pub fn path_of(&self, ino: i64) -> Option<String> {
        let inode = Inode(ino);
        let ns = self.ns.lock().unwrap();
        ns.build_path(inode, self.root)
    }

    /// Get parent inode
    pub fn parent_of(&self, ino: i64) -> Option<i64> {
        let inode = Inode(ino);
        let ns = self.ns.lock().unwrap();
        let node = ns.nodes.get(&inode)?;
        Some(node.parent.unwrap_or(self.root).0)
    }

    /// Get root inode value
    pub fn root_ino(&self) -> i64 {
        self.root.0
    }

    /// List directory entries by inode
    pub async fn readdir_ino(&self, ino: i64) -> Result<Vec<VfsFileType>, VfsError> {
        let inode = Inode(ino);
        let entries = self.meta.readdir(inode).await?;
        Ok(entries
            .into_iter()
            .map(|e| VfsFileType {
                name: e.name,
                ino: e.ino,
                kind: e.kind,
            })
            .collect())
    }

    /// Truncate file by inode
    pub async fn truncate_ino(&self, ino: i64, size: u64) -> Result<(), VfsError> {
        let inode = Inode(ino);
        let mask = crate::meta::types::SetAttrMask::size(size);
        self.meta.setattr(inode, mask).await?;
        Ok(())
    }

    /// Lookup child by parent inode and name
    pub async fn lookup(&self, parent: i64, name: &str) -> Result<i64, VfsError> {
        let parent_ino = Inode(parent);
        let child_ino = self.meta.lookup(parent_ino, name).await?;
        Ok(child_ino.0)
    }

    /// Get file attributes by inode (returns full FileAttr)
    pub async fn getattr(&self, ino: i64) -> Result<crate::meta::store::FileAttr, VfsError> {
        let inode = Inode(ino);
        let attr = self.meta.getattr(inode).await?;
        Ok(attr)
    }

    /// Create directory by parent inode and name
    pub async fn mkdir_ino(
        &self,
        parent: i64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<i64, VfsError> {
        let parent_ino = Inode(parent);
        let parent_path = self
            .path_of(parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", parent)))?;

        let full_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };

        let mut params =
            crate::meta::types::CreateParams::dir(parent_ino, name.to_string(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            let node = VNode::dir(name.to_string(), Some(parent_ino));
            ns.insert_node(ino, node, &full_path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.insert(name.to_string(), ino);
            }
        }

        Ok(ino.0)
    }

    /// Create file by parent inode and name
    pub async fn create_ino(
        &self,
        parent: i64,
        name: &str,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<i64, VfsError> {
        let parent_ino = Inode(parent);
        let parent_path = self
            .path_of(parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", parent)))?;

        let full_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };

        let mut params =
            crate::meta::types::CreateParams::file(parent_ino, name.to_string(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            let node = VNode::file(name.to_string(), Some(parent_ino));
            ns.insert_node(ino, node, &full_path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.insert(name.to_string(), ino);
            }
        }

        Ok(ino.0)
    }

    /// Delete file by parent inode and name
    pub async fn unlink_ino(&self, parent: i64, name: &str) -> Result<(), VfsError> {
        let parent_ino = Inode(parent);
        let parent_path = self
            .path_of(parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", parent)))?;

        let full_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };

        let ino = self
            .resolve_path(&full_path)
            .ok_or_else(|| VfsError::PathNotFound(full_path.clone()))?;

        self.meta.unlink(parent_ino, name).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            ns.remove_node(ino, &full_path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.remove(name);
            }
        }

        Ok(())
    }

    /// Delete directory by parent inode and name
    pub async fn rmdir_ino(&self, parent: i64, name: &str) -> Result<(), VfsError> {
        let parent_ino = Inode(parent);
        let parent_path = self
            .path_of(parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", parent)))?;

        let full_path = if parent_path == "/" {
            format!("/{}", name)
        } else {
            format!("{}/{}", parent_path, name)
        };

        let ino = self
            .resolve_path(&full_path)
            .ok_or_else(|| VfsError::PathNotFound(full_path.clone()))?;

        self.meta.rmdir(parent_ino, name).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();
            ns.remove_node(ino, &full_path);

            // Update parent's children
            if let Some(parent) = ns.nodes.get_mut(&parent_ino) {
                parent.children.remove(name);
            }
        }

        Ok(())
    }

    /// Rename by inode references
    pub async fn rename_ino(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: &str,
    ) -> Result<(), VfsError> {
        let old_parent_ino = Inode(old_parent);
        let new_parent_ino = Inode(new_parent);

        let old_parent_path = self
            .path_of(old_parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", old_parent)))?;
        let old_path = if old_parent_path == "/" {
            format!("/{}", old_name)
        } else {
            format!("{}/{}", old_parent_path, old_name)
        };

        let new_parent_path = self
            .path_of(new_parent)
            .ok_or_else(|| VfsError::PathNotFound(format!("inode {}", new_parent)))?;
        let new_path = if new_parent_path == "/" {
            format!("/{}", new_name)
        } else {
            format!("{}/{}", new_parent_path, new_name)
        };

        let ino = self
            .resolve_path(&old_path)
            .ok_or_else(|| VfsError::PathNotFound(old_path.clone()))?;

        self.meta
            .rename(
                old_parent_ino,
                old_name,
                new_parent_ino,
                new_name.to_string(),
            )
            .await?;

        // Update namespace - need to recursively update all child paths
        {
            let mut ns = self.ns.lock().unwrap();

            // Collect all descendants to update their paths
            let mut to_update = vec![(ino, old_path.clone(), new_path.clone())];
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
                node.name = new_name.to_string();
                node.parent = Some(new_parent_ino);
            }

            // Update old parent's children
            if let Some(parent) = ns.nodes.get_mut(&old_parent_ino) {
                parent.children.remove(old_name);
            }

            // Update new parent's children
            if let Some(parent) = ns.nodes.get_mut(&new_parent_ino) {
                parent.children.insert(new_name.to_string(), ino);
            }
        }

        Ok(())
    }

    /// Set file attributes by inode
    pub async fn setattr_ino(
        &self,
        ino: i64,
        mask: crate::meta::types::SetAttrMask,
    ) -> Result<crate::meta::store::FileAttr, VfsError> {
        let inode = Inode(ino);
        let attr = self.meta.setattr(inode, mask).await?;
        Ok(attr)
    }

    // ========== Path-based convenience methods (from old_fs.rs) ==========

    /// Create directory recursively (like mkdir -p)
    pub async fn mkdir_p(&self, path: &str) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        if &path == "/" {
            return Ok(self.root.0);
        }
        if let Some(ino) = self.resolve_path(&path) {
            return Ok(ino.0);
        }

        // Create each segment
        let mut cur_ino = self.root;
        let mut cur_path = String::from("/");

        for part in path.trim_start_matches('/').split('/') {
            if part.is_empty() {
                continue;
            }
            if cur_path != "/" {
                cur_path.push('/');
            }
            cur_path.push_str(part);

            if let Some(ino) = self.resolve_path(&cur_path) {
                // Verify it's a directory
                if let Ok(attr) = self.getattr(ino.0).await {
                    if attr.kind != FileType::Dir {
                        return Err(VfsError::NotDirectory(path.clone()));
                    }
                }
                cur_ino = ino;
                continue;
            }

            // Create new directory
            let ino = self.mkdir_ino(cur_ino.0, part, 0o755, 0, 0).await?;
            cur_ino = Inode(ino);
        }
        Ok(cur_ino.0)
    }

    /// Create file (parent directories created if missing)
    pub async fn create_file(&self, path: &str) -> Result<i64, VfsError> {
        let path = Self::norm_path(path);
        let (dir, name) = Self::split_dir_file(&path);

        let dir_ino = self.mkdir_p(&dir).await?;

        // Check if directory is actually a directory
        if let Ok(attr) = self.getattr(dir_ino).await {
            if attr.kind != FileType::Dir {
                return Err(VfsError::NotDirectory(path.clone()));
            }
        }

        // Check if file already exists
        if let Some(child_ino) = self.child_of(dir_ino, &name) {
            if let Ok(attr) = self.getattr(child_ino.0).await {
                return if attr.kind == FileType::Dir {
                    Err(VfsError::NotFile(path.clone()))
                } else {
                    Ok(child_ino.0)
                };
            }
        }

        // Create new file
        let ino = self.create_ino(dir_ino, &name, 0o644, 0, 0).await?;
        Ok(ino)
    }

    /// Get file attributes by path
    pub async fn stat(&self, path: &str) -> Option<crate::meta::store::FileAttr> {
        let path = Self::norm_path(path);
        let ino = self.resolve_path(&path)?;
        self.getattr(ino.0).await.ok()
    }

    /// List directory by path
    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        let path = Self::norm_path(path);
        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound(path.clone()))?;

        // Check if it's a directory
        {
            let ns = self.ns.lock().unwrap();
            if let Some(vnode) = ns.nodes.get(&ino) {
                if vnode.kind != FileType::Dir {
                    return Err(VfsError::NotDirectory(path.clone()));
                }

                // If children already loaded in namespace, use them
                if !vnode.children.is_empty() {
                    let mut entries = Vec::new();
                    for (name, &child_ino) in &vnode.children {
                        if let Some(child_node) = ns.nodes.get(&child_ino) {
                            entries.push(DirEntry {
                                name: name.clone(),
                                ino: child_ino.0,
                                kind: child_node.kind,
                            });
                        }
                    }
                    return Ok(entries);
                }
            }
        }

        // Load from meta store
        let meta_entries = self.meta.readdir(ino).await?;

        // Update namespace
        {
            let mut ns = self.ns.lock().unwrap();

            if let Some(vnode) = ns.nodes.get_mut(&ino) {
                vnode.children.clear();

                for entry in &meta_entries {
                    vnode.children.insert(entry.name.clone(), Inode(entry.ino));
                }
            }

            for entry in &meta_entries {
                let child_path = if path == "/" {
                    format!("/{}", entry.name)
                } else {
                    format!("{}/{}", path, entry.name)
                };

                let kind = entry.kind;

                ns.nodes.insert(
                    Inode(entry.ino),
                    match kind {
                        FileType::Dir => VNode::dir(entry.name.clone(), Some(ino)),
                        FileType::File => VNode::file(entry.name.clone(), Some(ino)),
                    },
                );
                ns.lookup.insert(child_path, Inode(entry.ino));
            }
        }

        Ok(meta_entries)
    }

    /// Check if path exists
    pub fn exists(&self, path: &str) -> bool {
        let path = Self::norm_path(path);
        self.ns.lock().unwrap().lookup.contains_key(&path)
    }

    /// Delete file by path
    pub async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;

        let (parent, kind) = {
            let ns = self.ns.lock().unwrap();
            let vnode = ns
                .nodes
                .get(&ino)
                .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;
            (
                vnode
                    .parent
                    .ok_or_else(|| VfsError::Operation("orphan".to_string()))?,
                vnode.kind,
            )
        };

        if kind != FileType::File {
            return Err(VfsError::NotFile(path.clone()));
        }

        let name = {
            let ns = self.ns.lock().unwrap();
            ns.nodes
                .get(&ino)
                .map(|v| v.name.clone())
                .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?
        };

        self.unlink_ino(parent.0, &name).await;

        Ok(())
    }

    /// Delete empty directory by path
    pub async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        if path == "/" {
            return Err(VfsError::Operation("cannot remove root".to_string()));
        }

        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;

        let (parent, kind, has_children) = {
            let ns = self.ns.lock().unwrap();
            let vnode = ns
                .nodes
                .get(&ino)
                .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;
            (
                vnode
                    .parent
                    .ok_or_else(|| VfsError::Operation("orphan".to_string()))?,
                vnode.kind,
                !vnode.children.is_empty(),
            )
        };

        if kind != FileType::Dir {
            return Err(VfsError::NotDirectory(path.clone()));
        }
        if has_children {
            return Err(VfsError::DirectoryNotEmpty(path.clone()));
        }

        let name = {
            let ns = self.ns.lock().unwrap();
            ns.nodes
                .get(&ino)
                .map(|v| v.name.clone())
                .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?
        };

        self.rmdir_ino(parent.0, &name).await;

        Ok(())
    }

    /// Rename file by path
    pub async fn rename_file(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);
        let (new_dir, new_name) = Self::split_dir_file(&new);

        if self.exists(&new) {
            return Err(VfsError::AlreadyExists(new.clone()));
        }

        let ino = self
            .resolve_path(&old)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;

        // Create missing parent directories
        self.mkdir_p(&new_dir).await?;
        let new_dir_ino = self
            .resolve_path(&new_dir)
            .ok_or_else(|| VfsError::PathNotFound("parent not found".to_string()))?;

        let (old_parent, old_name, kind) = {
            let ns = self.ns.lock().unwrap();
            let vnode = ns
                .nodes
                .get(&ino)
                .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;
            if vnode.kind != FileType::File {
                return Err(VfsError::NotFile(old.clone()));
            }
            (
                vnode
                    .parent
                    .ok_or_else(|| VfsError::Operation("orphan".to_string()))?,
                vnode.name.clone(),
                vnode.kind,
            )
        };

        self.rename_ino(old_parent.0, &old_name, new_dir_ino.0, &new_name)
            .await;

        Ok(())
    }

    /// Truncate/extend file size by path
    pub async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;

        self.truncate_ino(ino.0, size).await
    }

    /// Write to file by path
    pub async fn write(&self, path: &str, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        let path = Self::norm_path(path);
        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;

        let spans: Vec<ChunkSpan> = split_file_range_into_chunks(self.layout, offset, data.len());
        let mut cursor = 0usize;

        for sp in spans {
            let cid = self.chunk_id(ino, sp.chunk_index);
            let mut guard = self.store.lock().await;
            let mut w = ChunkWriter::new(self.layout, cid, &mut *guard);
            let take = sp.len;
            let buf = &data[cursor..cursor + take];
            let _slice = w.write(sp.offset_in_chunk, buf).await;
            cursor += take;
        }

        // Update size
        let new_size = offset + data.len() as u64;
        let mask = crate::meta::types::SetAttrMask::size(new_size);
        self.meta.setattr(ino, mask).await;

        Ok(data.len())
    }

    /// Read from file by path
    pub async fn read(&self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, VfsError> {
        let path = Self::norm_path(path);
        let ino = self
            .resolve_path(&path)
            .ok_or_else(|| VfsError::PathNotFound("not found".to_string()))?;
        self.read_ino(ino.0, offset, len).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chuck::store::ObjectBlockStore;
    use crate::meta::create_meta_store_from_url;

    #[tokio::test]
    async fn test_fs_mkdir_create_write_read_readdir() {
        let layout = ChunkLayout::default();
        let tmp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new(client);

        let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let fs = Vfs::new(layout, store, meta).await.unwrap();

        fs.mkdir_p("/a/b").await.expect("mkdir_p");
        fs.create_file("/a/b/hello.txt").await.expect("create");
        let data_len = layout.block_size as usize + (layout.block_size / 2) as usize;
        let mut data = vec![0u8; data_len];
        for (i, b) in data.iter_mut().enumerate().take(data_len) {
            *b = (i % 251) as u8;
        }
        fs.write("/a/b/hello.txt", (layout.block_size / 2) as u64, &data)
            .await
            .expect("write");
        let out = fs
            .read("/a/b/hello.txt", (layout.block_size / 2) as u64, data_len)
            .await
            .expect("read");
        assert_eq!(out, data);

        let entries = fs.readdir("/a/b").await.expect("readdir");
        assert!(
            entries
                .iter()
                .any(|e| e.name == "hello.txt" && e.kind == FileType::File)
        );

        let stat = fs.stat("/a/b/hello.txt").await.unwrap();
        assert_eq!(stat.kind, FileType::File);
        assert!(stat.size >= data_len as u64);
    }

    #[tokio::test]
    async fn test_fs_unlink_rmdir_rename_truncate() {
        let layout = ChunkLayout::default();
        let tmp = tempfile::tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new(client);

        let meta = create_meta_store_from_url("sqlite::memory:").await.unwrap();
        let fs = Vfs::new(layout, store, meta).await.unwrap();

        fs.mkdir_p("/a/b").await.unwrap();
        fs.create_file("/a/b/t.txt").await.unwrap();
        assert!(fs.exists("/a/b/t.txt"));

        // rename file
        fs.rename_file("/a/b/t.txt", "/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/t.txt") && fs.exists("/a/b/u.txt"));

        // truncate
        fs.truncate("/a/b/u.txt", layout.block_size as u64 * 2)
            .await
            .unwrap();
        let st = fs.stat("/a/b/u.txt").await.unwrap();
        assert!(st.size >= (layout.block_size * 2) as u64);

        // unlink and rmdir
        fs.unlink("/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/u.txt"));
        // dir empty then rmdir
        fs.rmdir("/a/b").await.unwrap();
        assert!(!fs.exists("/a/b"));
    }
}
