//! SDK 接口：提供面向应用/SDK 的简化文件系统 API（参考 JuiceFS 风格）。
//!
//! 设计目标：
//! - 路径级接口：mkdir_p/create/read/write/readdir/stat
//! - 可插拔后端：复用 Fs 上的 BlockStore + MetaStore
//! - 提供 LocalFs 的便捷构造

use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chuck::chunk::ChunkLayout;
use crate::chuck::store::{BlockStore, ObjectBlockStore};
use crate::meta::config::{Config, DatabaseConfig, DatabaseType};
use crate::meta::database_store::DatabaseMetaStore;
use crate::meta::store::DirEntry;
use crate::meta::{MetaStore, create_meta_store_from_url};
use crate::vfs::fs::{VFS, VfsError};
use std::path::Path;
use std::sync::Arc;

pub struct Client<S: BlockStore, M: MetaStore> {
    vfs: Arc<VFS<S, M>>,
}

impl<S: BlockStore + Send + Sync, M: MetaStore + Send + Sync> Client<S, M> {
    /// Create a new client from a vfs instance
    pub fn new(vfs: Arc<VFS<S, M>>) -> Self {
        Self { vfs }
    }

    /// Create a directory (including all parent directories)
    pub async fn mkdir_p(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.mkdir_p(path).await?;
        Ok(())
    }

    /// Create a file
    pub async fn create(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.create_file(path).await?;
        Ok(())
    }

    /// Write data at a specific offset
    pub async fn write_at(&mut self, path: &str, offset: u64, data: &[u8]) -> Result<(), VfsError> {
        self.vfs.write(path, offset, data).await?;
        Ok(())
    }

    /// Read data from a specific offset
    pub async fn read_at(
        &mut self,
        path: &str,
        offset: u64,
        len: usize,
    ) -> Result<Vec<u8>, VfsError> {
        self.vfs.read(path, offset, len).await
    }

    /// Remove a file
    pub async fn remove(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.unlink(path).await
    }

    /// Remove a file (alias for remove)
    pub async fn unlink(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.unlink(path).await
    }

    /// Remove an empty directory
    pub async fn rmdir(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.rmdir(path).await
    }

    /// Get file attributes
    pub async fn stat(&self, path: &str) -> Result<crate::meta::store::FileAttr, VfsError> {
        self.vfs
            .stat(path)
            .await
            .ok_or_else(|| VfsError::PathNotFound(path.to_string()))
    }

    /// Truncate file to specified size
    pub async fn truncate(&mut self, path: &str, size: u64) -> Result<(), VfsError> {
        self.vfs.truncate(path, size).await
    }

    /// List directory contents
    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        self.vfs.readdir(path).await
    }

    /// Rename a file or directory
    pub async fn rename(&mut self, old_path: &str, new_path: &str) -> Result<(), VfsError> {
        self.vfs.rename_file(old_path, new_path).await
    }
}

/// Local filesystem client (convenience type)
pub type LocalClient = Client<ObjectBlockStore<LocalFsBackend>, DatabaseMetaStore>;

impl LocalClient {
    /// Create a new local client with in-memory SQLite backend
    pub async fn new_local<P: AsRef<Path>>(root: P, layout: ChunkLayout) -> Self {
        let client = ObjectClient::new(LocalFsBackend::new(root));
        let store = ObjectBlockStore::new(client);

        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite {
                    url: "sqlite::memory:".to_string(),
                },
            },
        };
        let meta = DatabaseMetaStore::from_config(config)
            .await
            .expect("Failed to create meta store");

        let vfs = Arc::new(
            VFS::new(layout, store, meta)
                .await
                .expect("Failed to create vfs"),
        );
        Client::new(vfs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_sdk_local_basic() {
        let layout = ChunkLayout::default();
        let tmp = tempdir().unwrap();
        let mut cli = LocalClient::new_local(tmp.path(), layout).await;

        cli.mkdir_p("/a/b").await.unwrap();
        cli.create("/a/b/hello.txt").await.unwrap();

        let half = (layout.block_size / 2) as usize;
        let len = layout.block_size as usize + half;
        let mut data = vec![0u8; len];
        for (i, b) in data.iter_mut().enumerate().take(len) {
            *b = (i % 251) as u8;
        }
        cli.write_at("/a/b/hello.txt", half as u64, &data)
            .await
            .unwrap();

        let out = cli
            .read_at("/a/b/hello.txt", half as u64, len)
            .await
            .unwrap();
        assert_eq!(out, data);

        let ent = cli.readdir("/a/b").await.unwrap();
        assert!(ent.iter().any(|e| e.name == "hello.txt"));

        let st = cli.stat("/a/b/hello.txt").await.unwrap();
        assert!(st.size >= len as u64);
    }

    #[tokio::test]
    async fn test_sdk_local_ops_extras() {
        let layout = ChunkLayout::default();
        let tmp = tempdir().unwrap();
        let mut cli = LocalClient::new_local(tmp.path(), layout).await;

        cli.mkdir_p("/x/y").await.unwrap();
        cli.create("/x/y/a.txt").await.unwrap();
        cli.rename("/x/y/a.txt", "/x/y/b.txt").await.unwrap();
        cli.truncate("/x/y/b.txt", (layout.block_size * 2) as u64)
            .await
            .unwrap();
        let st = cli.stat("/x/y/b.txt").await.unwrap();
        assert!(st.size >= (layout.block_size * 2) as u64);
        cli.unlink("/x/y/b.txt").await.unwrap();
        // 目录空了，允许删除
        cli.rmdir("/x/y").await.unwrap();
    }
}
