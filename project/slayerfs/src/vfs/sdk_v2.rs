//! SDK V2: Client wrapper for VfsV2
//!
//! Provides a simplified client interface for application-level file operations
//! using VfsV2 backend.

use crate::cadapter::client::ObjectClient;
use crate::cadapter::localfs::LocalFsBackend;
use crate::chuck::chunk::ChunkLayout;
use crate::chuck::store::{BlockStore, ObjectBlockStore};
use crate::meta::config::{Config, DatabaseConfig, DatabaseType};
use crate::meta::database_store::DatabaseMetaStore;
use crate::meta::store::DirEntry;
use crate::meta::store_v2::MetaStoreV2;
use crate::vfs::fs_v2::{FileSystemV2, VfsError, VfsV2};
use std::path::Path;
use std::sync::Arc;

/// Client for VFS V2 operations
///
/// Provides a high-level interface for file system operations.
pub struct ClientV2<S: BlockStore, M: MetaStoreV2> {
    vfs: Arc<VfsV2<S, M>>,
}

impl<S: BlockStore + Send + Sync, M: MetaStoreV2 + Send + Sync> ClientV2<S, M> {
    /// Create a new client from a VfsV2 instance
    pub fn new(vfs: Arc<VfsV2<S, M>>) -> Self {
        Self { vfs }
    }

    /// Create a directory (including all parent directories)
    pub async fn mkdir_p(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.mkdir_p(path).await?;
        Ok(())
    }

    /// Create a file
    pub async fn create(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.create(path, 0o644, 1000, 1000).await?;
        Ok(())
    }

    /// Write data at a specific offset
    pub async fn write_at(&mut self, path: &str, offset: u64, data: &[u8]) -> Result<(), VfsError> {
        self.vfs.write(path, offset, data).await?;
        Ok(())
    }

    /// Read data from a specific offset
    pub async fn read_at(&mut self, path: &str, offset: u64, len: usize) -> Result<Vec<u8>, VfsError> {
        self.vfs.read(path, offset, len).await
    }

    /// Remove a file
    pub async fn remove(&mut self, path: &str) -> Result<(), VfsError> {
        self.vfs.unlink(path).await
    }

    /// List directory contents
    pub async fn readdir(&self, path: &str) -> Result<Vec<DirEntry>, VfsError> {
        self.vfs.readdir(path).await
    }

    /// Rename a file or directory
    pub async fn rename(&mut self, old_path: &str, new_path: &str) -> Result<(), VfsError> {
        self.vfs.rename(old_path, new_path).await
    }
}

/// Local filesystem client (convenience type)
pub type LocalClientV2 = ClientV2<ObjectBlockStore<LocalFsBackend>, DatabaseMetaStore>;

impl LocalClientV2 {
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
            VfsV2::new(layout, store, meta)
                .await
                .expect("Failed to create VFS"),
        );
        ClientV2::new(vfs)
    }
}
