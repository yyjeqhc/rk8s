//! FUSE mounting utilities for VfsV2
//!
//! This module provides helper functions to mount VfsV2 instances as FUSE filesystems.
//! It supports unprivileged mounting on Linux using user namespaces.

use crate::chuck::store::BlockStore;
use crate::fuse::adapter_v2::FuseAdapterV2;
use crate::meta::store_v2::MetaStoreV2;
use crate::vfs::fs_v2::VfsV2;
use rfuse3::raw::Session;
use rfuse3::MountOptions;
use std::path::Path;
use std::sync::Arc;

/// Mount a VfsV2 instance as a FUSE filesystem.
///
/// # Arguments
/// * `vfs` - The VFS instance to mount
/// * `mount_point` - Path to the mount point
/// * `uid` - User ID for ownership
/// * `gid` - Group ID for ownership
/// * `mount_options` - Additional FUSE mount options
///
/// # Returns
/// A Session that keeps the filesystem mounted. The filesystem will be unmounted
/// when the Session is dropped.
///
/// # Example
/// ```no_run
/// use slayerfs::vfs::fs_v2::VfsV2;
/// use slayerfs::fuse::mount_v2::mount_vfs_v2;
/// use slayerfs::meta::store::database_store::DatabaseMetaStore;
/// use slayerfs::chuck::store::ObjectBlockStore;
/// use std::sync::Arc;
///
/// # async fn example() -> anyhow::Result<()> {
/// let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;
/// let block_store = ObjectBlockStore::new(/* ... */);
/// let vfs = Arc::new(VfsV2::new(block_store, meta_store));
///
/// let session = mount_vfs_v2(
///     vfs,
///     "/mnt/slayerfs",
///     1000, // uid
///     1000, // gid
///     &[],
/// ).await?;
///
/// // Filesystem is now mounted and accessible
/// // ...
///
/// // Filesystem unmounts when session is dropped
/// drop(session);
/// # Ok(())
/// # }
/// ```
pub async fn mount_vfs_v2<S, M, P>(
    vfs: Arc<VfsV2<S, M>>,
    mount_point: P,
    uid: u32,
    gid: u32,
    mount_options: &[MountOptions],
) -> anyhow::Result<Session>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStoreV2 + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let adapter = FuseAdapterV2::new(vfs, uid, gid);
    let mut session = Session::new(mount_options).await?;
    session.mount_with_unprivileged(adapter, mount_point.as_ref()).await?;
    Ok(session)
}

/// Mount a VfsV2 instance with default options for unprivileged mounting.
///
/// This is a convenience wrapper around `mount_vfs_v2` that sets up common
/// mount options suitable for unprivileged user access.
///
/// # Arguments
/// * `vfs` - The VFS instance to mount
/// * `mount_point` - Path to the mount point
/// * `uid` - User ID for ownership
/// * `gid` - Group ID for ownership
///
/// # Example
/// ```no_run
/// use slayerfs::vfs::fs_v2::VfsV2;
/// use slayerfs::fuse::mount_v2::mount_vfs_v2_unprivileged;
/// use slayerfs::meta::store::database_store::DatabaseMetaStore;
/// use slayerfs::chuck::store::ObjectBlockStore;
/// use std::sync::Arc;
///
/// # async fn example() -> anyhow::Result<()> {
/// let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;
/// let block_store = ObjectBlockStore::new(/* ... */);
/// let vfs = Arc::new(VfsV2::new(block_store, meta_store));
///
/// let session = mount_vfs_v2_unprivileged(
///     vfs,
///     "/mnt/slayerfs",
///     1000, // uid
///     1000, // gid
/// ).await?;
///
/// // Filesystem is now mounted
/// # Ok(())
/// # }
/// ```
pub async fn mount_vfs_v2_unprivileged<S, M, P>(
    vfs: Arc<VfsV2<S, M>>,
    mount_point: P,
    uid: u32,
    gid: u32,
) -> anyhow::Result<Session>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStoreV2 + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let mount_options = vec![
        MountOptions::AllowOther,
        MountOptions::DefaultPermissions,
        MountOptions::FSName("slayerfs".to_string()),
    ];

    mount_vfs_v2(vfs, mount_point, uid, gid, &mount_options).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chuck::chunk::ChunkLayout;
    use crate::chuck::store::ObjectBlockStore;
    use crate::meta::store::database_store::DatabaseMetaStore;
    use std::fs;
    use tempfile::TempDir;
    use tokio::time::{sleep, Duration};

    /// This test is disabled by default as it requires:
    /// 1. Linux OS with FUSE support
    /// 2. User namespace support
    /// 3. Proper permissions
    ///
    /// To run: SLAYERFS_FUSE_TEST=1 cargo test --lib mount_v2_tests
    #[tokio::test]
    #[ignore]
    async fn test_mount_and_basic_operations() -> anyhow::Result<()> {
        // Skip if not explicitly enabled
        if std::env::var("SLAYERFS_FUSE_TEST").is_err() {
            println!("Skipping FUSE mount test (set SLAYERFS_FUSE_TEST=1 to enable)");
            return Ok(());
        }

        // Create temporary directories
        let temp_storage = TempDir::new()?;
        let temp_mount = TempDir::new()?;

        // Initialize backend storage
        let backend = LocalFsBackend::new(temp_storage.path().to_str().unwrap())?;
        let client = Arc::new(ObjectClient::new(backend));
        let layout = ChunkLayout::default();
        let block_store = ObjectBlockStore::new(client, layout);

        // Initialize metadata store
        let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;

        // Create VFS
        let vfs = Arc::new(VfsV2::new(block_store, meta_store));

        // Mount the filesystem
        let session = mount_vfs_v2_unprivileged(
            vfs.clone(),
            temp_mount.path(),
            1000,
            1000,
        )
        .await?;

        // Give FUSE some time to initialize
        sleep(Duration::from_millis(100)).await;

        // Test basic operations
        let test_dir = temp_mount.path().join("test_dir");
        fs::create_dir(&test_dir)?;
        assert!(test_dir.exists());

        let test_file = test_dir.join("test.txt");
        fs::write(&test_file, b"Hello, FUSE!")?;
        assert!(test_file.exists());

        let content = fs::read(&test_file)?;
        assert_eq!(content, b"Hello, FUSE!");

        // Cleanup
        fs::remove_file(&test_file)?;
        fs::remove_dir(&test_dir)?;

        // Unmount
        drop(session);

        Ok(())
    }
}
