//! FUSE mounting utilities for Vfs
//!
//! This module provides helper functions to mount Vfs instances as FUSE filesystems.
//! It supports unprivileged mounting on Linux using user namespaces.

use crate::chuck::store::BlockStore;
use crate::fuse::adapter::FuseAdapter;
use crate::meta::store::MetaStore;
use crate::vfs::fs::Vfs;
use rfuse3::MountOptions;
use rfuse3::raw::MountHandle;
use std::path::Path;
use std::sync::Arc;

/// Build default mount options for SlayerFS V2.
fn default_mount_options() -> MountOptions {
    let mut mo = MountOptions::default();
    mo.fs_name("slayerfs_v2");
    mo
}

/// Mount a Vfs instance as a FUSE filesystem.
///
/// # Arguments
/// * `vfs` - The VFS instance to mount
/// * `mount_point` - Path to the mount point
/// * `uid` - User ID for ownership
/// * `gid` - Group ID for ownership
///
/// # Returns
/// A MountHandle that keeps the filesystem mounted. The filesystem will be unmounted
/// when the MountHandle is dropped.
///
/// # Example
/// ```no_run
/// use slayerfs::vfs::fs_v2::Vfs;
/// use slayerfs::fuse::mount_v2::mount_vfs_v2;
/// use slayerfs::meta::store::database_store::DatabaseMetaStore;
/// use slayerfs::chuck::store::ObjectBlockStore;
/// use std::sync::Arc;
///
/// # async fn example() -> anyhow::Result<()> {
/// let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;
/// let block_store = ObjectBlockStore::new(/* ... */);
/// let vfs = Arc::new(Vfs::new(block_store, meta_store));
///
/// let handle = mount_vfs_v2(
///     vfs,
///     "/mnt/slayerfs",
///     1000, // uid
///     1000, // gid
/// ).await?;
///
/// // Filesystem is now mounted and accessible
/// // ...
///
/// // Filesystem unmounts when handle is dropped
/// drop(handle);
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "linux")]
pub async fn mount_vfs_v2<S, M, P>(
    vfs: Arc<Vfs<S, M>>,
    mount_point: P,
    uid: u32,
    gid: u32,
) -> std::io::Result<MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let adapter = FuseAdapter::new(vfs, uid, gid);
    let opts = default_mount_options();
    let session = rfuse3::raw::Session::new(opts);
    session.mount_with_unprivileged(adapter, mount_point).await
}

/// Mount a Vfs instance with custom mount options.
///
/// # Arguments
/// * `vfs` - The VFS instance to mount
/// * `mount_point` - Path to the mount point
/// * `uid` - User ID for ownership
/// * `gid` - Group ID for ownership
/// * `mount_options` - Custom FUSE mount options
///
/// # Example
/// ```no_run
/// use slayerfs::vfs::fs_v2::Vfs;
/// use slayerfs::fuse::mount_v2::mount_vfs_with_options;
/// use slayerfs::meta::store::database_store::DatabaseMetaStore;
/// use slayerfs::chuck::store::ObjectBlockStore;
/// use rfuse3::MountOptions;
/// use std::sync::Arc;
///
/// # async fn example() -> anyhow::Result<()> {
/// let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;
/// let block_store = ObjectBlockStore::new(/* ... */);
/// let vfs = Arc::new(Vfs::new(block_store, meta_store));
///
/// let mut opts = MountOptions::default();
/// opts.fs_name("my_slayerfs");
/// opts.force_readdir_plus(true);
///
/// let handle = mount_vfs_with_options(
///     vfs,
///     "/mnt/slayerfs",
///     1000,
///     1000,
///     opts,
/// ).await?;
/// # Ok(())
/// # }
/// ```
#[cfg(target_os = "linux")]
pub async fn mount_vfs_with_options<S, M, P>(
    vfs: Arc<Vfs<S, M>>,
    mount_point: P,
    uid: u32,
    gid: u32,
    mount_options: MountOptions,
) -> std::io::Result<MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + Send + Sync + 'static,
    P: AsRef<Path>,
{
    let adapter = FuseAdapter::new(vfs, uid, gid);
    let session = rfuse3::raw::Session::new(mount_options);
    session.mount_with_unprivileged(adapter, mount_point).await
}

/// Fallback stub for non-Linux targets.
#[cfg(not(target_os = "linux"))]
pub async fn mount_vfs_v2<S, M, P>(
    _vfs: Arc<Vfs<S, M>>,
    _mount_point: P,
    _uid: u32,
    _gid: u32,
) -> std::io::Result<MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + Send + Sync + 'static,
    P: AsRef<Path>,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FUSE mount is only supported on Linux in this build",
    ))
}

/// Fallback stub for non-Linux targets.
#[cfg(not(target_os = "linux"))]
pub async fn mount_vfs_with_options<S, M, P>(
    _vfs: Arc<Vfs<S, M>>,
    _mount_point: P,
    _uid: u32,
    _gid: u32,
    _mount_options: MountOptions,
) -> std::io::Result<MountHandle>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStore + Send + Sync + 'static,
    P: AsRef<Path>,
{
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FUSE mount is only supported on Linux in this build",
    ))
}
