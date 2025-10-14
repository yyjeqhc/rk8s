//! FUSE/SDK 友好的简化 VFS：基于路径的 create/mkdir/read/write/readdir/stat。
//!
//! 重构后的 VFS 不再维护内存 Namespace，而是直接使用 MetaClient 的缓存能力。

use crate::chuck::chunk::ChunkLayout;
use crate::chuck::reader::ChunkReader;
use crate::chuck::store::BlockStore;
use crate::chuck::util::{ChunkSpan, split_file_range_into_chunks};
use crate::chuck::writer::ChunkWriter;
use crate::meta::client::MetaClient;
use crate::meta::MetaStore;
use crate::meta::entities::content_meta::EntryType;
use std::sync::Arc;

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
pub struct VFSFileAttr {
    pub ino: i64,
    pub size: u64,
    pub kind: FileType,
}

#[derive(Clone, Debug)]
pub struct VFSFileType {
    pub name: String,
    pub ino: i64,
    pub kind: FileType,
}

use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};

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
pub type VFSResult<T> = Result<T, VfsError>;

/// VFS: Virtual File System layer
///
/// 重构后的 VFS 不再维护内存目录树（Namespace），而是直接使用 MetaClient。
/// MetaClient 内部有多级缓存（attr、dentry、path），性能不会下降。
///
/// 优势：
/// - 简化代码：移除 500+ 行的 Namespace 管理代码
/// - 统一缓存：所有元数据访问都经过 MetaClient 的缓存
/// - 一致性强：不需要同步内存状态和持久化状态
pub struct VFS<S: BlockStore> {
    /// Chunk layout configuration
    layout: ChunkLayout,
    /// Block storage for chunk data
    store: tokio::sync::Mutex<S>,
    /// Metadata client (with caching)
    meta: MetaClient,
    /// Base offset for chunk ID calculation
    base: i64,
    /// Root inode
    root: Inode,
}

impl<S: BlockStore> VFS<S> {
    /// Create a new VFS instance
    ///
    /// 重构后的版本不再加载整个目录树到内存，而是依赖 MetaClient 的缓存。
    pub async fn new<M: MetaStore + 'static>(
        layout: ChunkLayout,
        store: S,
        meta: M,
    ) -> VFSResult<Self> {
        // Wrap MetaStore in MetaClient for caching
        let meta_client = MetaClient::new(Arc::new(meta));

        Self::with_meta_client(layout, store, meta_client).await
    }

    /// Create VFS with custom MetaClient (for advanced caching configuration)
    pub async fn with_meta_client(
        layout: ChunkLayout,
        store: S,
        meta_client: MetaClient,
    ) -> VFSResult<Self> {
        meta_client.initialize().await?;
        let root = meta_client.root_ino();
        let base = 1_000_000_000i64;

        Ok(Self {
            layout,
            store: tokio::sync::Mutex::new(store),
            meta: meta_client,
            base,
            root,
        })
    }

    /// Resolve a path to an inode by walking through each component
    ///
    /// 使用 MetaClient 的 lookup 方法逐级解析路径，会自动利用 dentry 缓存。
    async fn resolve_path(&self, path: &str) -> VFSResult<Inode> {
        if path == "/" {
            return Ok(self.root);
        }

        let mut current_ino = self.root;
        let parts: Vec<&str> = path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).collect();

        for part in parts {
            // Use MetaClient.lookup which has caching
            current_ino = self.meta
                .lookup(current_ino, part)
                .await
                .map_err(|e| VfsError::PathNotFound(format!("{}: {}", path, e)))?;
        }

        Ok(current_ino)
    }

    /// Parse path into (parent_path, parent_inode, basename)
    ///
    /// For "/foo/bar", returns ("/foo", parent_ino, "bar")
    /// For "/foo", returns ("/", root_ino, "foo")
    async fn parse_path(&self, path: &str) -> VFSResult<(String, Inode, String)> {
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
            .await?;

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
    async fn write_chunks(&self, ino: Inode, offset: u64, data: &[u8]) -> VFSResult<usize> {
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
    async fn read_chunks(&self, ino: Inode, offset: u64, len: usize) -> VFSResult<Vec<u8>> {
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
    ///
    /// 重构后直接使用 MetaClient.lookup，利用其 dentry 缓存
    pub async fn child_of(&self, parent: i64, name: &str) -> Option<Inode> {
        let parent_ino = Inode(parent);
        self.meta.lookup(parent_ino, name).await.ok()
    }

    /// Get file attributes by inode (async version for FUSE)
    pub async fn stat_ino(&self, ino: i64) -> Option<VFSFileAttr> {
        let inode = Inode(ino);
        let attr = self.meta.getattr(inode).await.ok()?;
        Some(VFSFileAttr {
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

    /// Get absolute path for an inode (REMOVED - use path tracking in caller)
    ///
    /// 注意：重构后移除了内存 Namespace，此方法已不可用。
    /// 如果需要路径信息，请在调用方维护路径状态。
    #[deprecated(note = "Path tracking removed from VFS. Track paths in FUSE layer.")]
    pub fn path_of(&self, _ino: i64) -> Option<String> {
        None
    }

    /// Get parent inode (DEPRECATED - use getattr to get parent info if needed)
    ///
    /// 注意：重构后移除了内存 Namespace，此方法不再高效。
    #[deprecated(note = "Use metadata store methods instead.")]
    pub fn parent_of(&self, _ino: i64) -> Option<i64> {
        None
    }

    /// Get root inode value
    pub fn root_ino(&self) -> i64 {
        self.root.0
    }

    /// List directory entries by inode
    pub async fn readdir_ino(&self, ino: i64) -> Result<Vec<VFSFileType>, VfsError> {
        let inode = Inode(ino);
        let entries = self.meta.readdir(inode).await?;
        Ok(entries
            .into_iter()
            .map(|e| VFSFileType {
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

        let mut params =
            crate::meta::types::CreateParams::dir(parent_ino, name.to_string(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // No need to update namespace - MetaClient caching handles everything

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

        let mut params =
            crate::meta::types::CreateParams::file(parent_ino, name.to_string(), uid, gid);
        params.mode = mode;

        let (ino, _attr) = self.meta.create(params).await?;

        // No need to update namespace - MetaClient caching handles everything

        Ok(ino.0)
    }

    /// Delete file by parent inode and name
    pub async fn unlink_ino(&self, parent: i64, name: &str) -> Result<(), VfsError> {
        let parent_ino = Inode(parent);
        self.meta.unlink(parent_ino, name).await?;

        // No need to update namespace - MetaClient cache invalidation handles it

        Ok(())
    }

    /// Delete directory by parent inode and name
    pub async fn rmdir_ino(&self, parent: i64, name: &str) -> Result<(), VfsError> {
        let parent_ino = Inode(parent);
        self.meta.rmdir(parent_ino, name).await?;

        // No need to update namespace - MetaClient cache invalidation handles it

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

        self.meta
            .rename(
                old_parent_ino,
                old_name,
                new_parent_ino,
                new_name.to_string(),
            )
            .await?;

        // No need to update namespace - MetaClient cache invalidation handles everything

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
        
        // Try to resolve the full path first
        if let Ok(ino) = self.resolve_path(&path).await {
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

            if let Ok(ino) = self.resolve_path(&cur_path).await {
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
        if let Some(child_ino) = self.child_of(dir_ino, &name).await {
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
        let ino = self.resolve_path(&path).await.ok()?;
        self.getattr(ino.0).await.ok()
    }

    /// List directory by path
    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        let path = Self::norm_path(path);
        let ino = self.resolve_path(&path).await?;

        // Check if it's a directory
        let attr = self.meta.getattr(ino).await?;
        if attr.kind != FileType::Dir {
            return Err(VfsError::NotDirectory(path.clone()));
        }

        // Load from meta store (uses MetaClient's cache)
        let meta_entries = self.meta.readdir(ino).await?;

        Ok(meta_entries)
    }

    /// Check if path exists
    pub async fn exists(&self, path: &str) -> bool {
        let path = Self::norm_path(path);
        self.resolve_path(&path).await.is_ok()
    }

    /// Delete file by path
    pub async fn unlink(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let (parent_path, parent_ino, name) = self.parse_path(&path).await?;

        // Verify it's a file
        let ino = self.resolve_path(&path).await?;
        let attr = self.meta.getattr(ino).await?;
        if attr.kind != FileType::File {
            return Err(VfsError::NotFile(path.clone()));
        }

        self.unlink_ino(parent_ino.0, &name).await?;

        Ok(())
    }

    /// Delete empty directory by path
    pub async fn rmdir(&self, path: &str) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        if path == "/" {
            return Err(VfsError::Operation("cannot remove root".to_string()));
        }

        let (parent_path, parent_ino, name) = self.parse_path(&path).await?;

        // Verify it's a directory
        let ino = self.resolve_path(&path).await?;
        let attr = self.meta.getattr(ino).await?;
        if attr.kind != FileType::Dir {
            return Err(VfsError::NotDirectory(path.clone()));
        }

        // Check if empty
        let entries = self.meta.readdir(ino).await?;
        if !entries.is_empty() {
            return Err(VfsError::DirectoryNotEmpty(path.clone()));
        }

        self.rmdir_ino(parent_ino.0, &name).await?;

        Ok(())
    }

    /// Rename file by path
    pub async fn rename_file(&self, old: &str, new: &str) -> Result<(), VfsError> {
        let old = Self::norm_path(old);
        let new = Self::norm_path(new);

        if self.exists(&new).await {
            return Err(VfsError::AlreadyExists(new.clone()));
        }

        let (new_dir, new_name) = Self::split_dir_file(&new);

        // Verify old path exists and is a file
        let ino = self.resolve_path(&old).await?;
        let attr = self.meta.getattr(ino).await?;
        if attr.kind != FileType::File {
            return Err(VfsError::NotFile(old.clone()));
        }

        // Get old parent
        let (_old_parent_path, old_parent_ino, old_name) = self.parse_path(&old).await?;

        // Create missing parent directories for new path
        self.mkdir_p(&new_dir).await?;
        let new_dir_ino = self.resolve_path(&new_dir).await?;

        self.rename_ino(old_parent_ino.0, &old_name, new_dir_ino.0, &new_name)
            .await?;

        Ok(())
    }

    /// Truncate/extend file size by path
    pub async fn truncate(&self, path: &str, size: u64) -> Result<(), VfsError> {
        let path = Self::norm_path(path);
        let ino = self.resolve_path(&path).await?;

        self.truncate_ino(ino.0, size).await
    }

    /// Write to file by path
    pub async fn write(&self, path: &str, offset: u64, data: &[u8]) -> Result<usize, VfsError> {
        let path = Self::norm_path(path);
        let ino = self.resolve_path(&path).await?;

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
        let ino = self.resolve_path(&path).await?;
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
        let fs = VFS::new(layout, store, meta).await.unwrap();

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
        let fs = VFS::new(layout, store, meta).await.unwrap();

        fs.mkdir_p("/a/b").await.unwrap();
        fs.create_file("/a/b/t.txt").await.unwrap();
        assert!(fs.exists("/a/b/t.txt").await);

        // rename file
        fs.rename_file("/a/b/t.txt", "/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/t.txt").await && fs.exists("/a/b/u.txt").await);

        // truncate
        fs.truncate("/a/b/u.txt", layout.block_size as u64 * 2)
            .await
            .unwrap();
        let st = fs.stat("/a/b/u.txt").await.unwrap();
        assert!(st.size >= (layout.block_size * 2) as u64);

        // unlink and rmdir
        fs.unlink("/a/b/u.txt").await.unwrap();
        assert!(!fs.exists("/a/b/u.txt").await);
        // dir empty then rmdir
        fs.rmdir("/a/b").await.unwrap();
        assert!(!fs.exists("/a/b").await);
    }
}
