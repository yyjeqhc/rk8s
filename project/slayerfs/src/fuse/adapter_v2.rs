//! FUSE adapter for VfsV2
//!
//! This module implements the rfuse3::Filesystem trait for VfsV2,
//! enabling the VFS to be mounted as a FUSE filesystem.
//!
//! # Example
//! ```no_run
//! use slayerfs::vfs::fs_v2::VfsV2;
//! use slayerfs::fuse::adapter_v2::FuseAdapterV2;
//! # use slayerfs::meta::store::database_store::DatabaseMetaStore;
//! # use slayerfs::chuck::store::ObjectBlockStore;
//! # use std::sync::Arc;
//!
//! # async fn example() -> anyhow::Result<()> {
//! let meta_store = DatabaseMetaStore::new("sqlite::memory:").await?;
//! let block_store = ObjectBlockStore::new(/* ... */);
//! let vfs = Arc::new(VfsV2::new(block_store, meta_store));
//! let adapter = FuseAdapterV2::new(vfs, 1000, 1000);
//! # Ok(())
//! # }
//! ```

use crate::chuck::store::BlockStore;
use crate::meta::store::FileAttr as VfsFileAttr;
use crate::meta::store_v2::MetaStoreV2;
use crate::meta::types::Inode;
use crate::vfs::fs::{FileType as VfsFileType};
use crate::vfs::fs_v2::{FileSystemV2, VfsV2};
use bytes::Bytes;
use futures_util::stream;
use futures_util::Stream;
use rfuse3::raw::prelude::*;
use rfuse3::{FileType as FuseFileType, Result as FuseResult, SetAttr, Timestamp};
use std::ffi::{OsStr, OsString};
use std::num::NonZeroU32;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

/// FUSE adapter for VfsV2
///
/// This struct implements the `rfuse3::Filesystem` trait to enable mounting
/// a `VfsV2` instance as a FUSE filesystem.
pub struct FuseAdapterV2<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStoreV2 + Send + Sync + 'static,
{
    vfs: Arc<VfsV2<S, M>>,
    uid: u32,
    gid: u32,
}

impl<S, M> FuseAdapterV2<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStoreV2 + Send + Sync + 'static,
{
    /// Create a new FUSE adapter
    ///
    /// # Arguments
    /// * `vfs` - The VFS instance to adapt
    /// * `uid` - User ID for file ownership
    /// * `gid` - Group ID for file ownership
    pub fn new(vfs: Arc<VfsV2<S, M>>, uid: u32, gid: u32) -> Self {
        Self { vfs, uid, gid }
    }

    /// Convert VFS FileAttr to FUSE FileAttr
    fn to_fuse_attr(&self, vattr: VfsFileAttr) -> rfuse3::raw::reply::FileAttr {
        let now = Timestamp::from(SystemTime::now());
        let perm = match vattr.kind {
            VfsFileType::Dir => 0o755,
            VfsFileType::File => 0o644,
        } as u16;
        let blocks = vattr.size.div_ceil(512);

        rfuse3::raw::reply::FileAttr {
            ino: vattr.ino as u64,
            size: vattr.size,
            blocks,
            atime: now,
            mtime: now,
            ctime: now,
            #[cfg(target_os = "macos")]
            crtime: now,
            kind: vfs_kind_to_fuse(vattr.kind),
            perm,
            nlink: 1,
            uid: self.uid,
            gid: self.gid,
            rdev: 0,
            #[cfg(target_os = "macos")]
            flags: 0,
            blksize: 4096,
        }
    }
}

impl<S, M> Filesystem for FuseAdapterV2<S, M>
where
    S: BlockStore + Send + Sync + 'static,
    M: MetaStoreV2 + Send + Sync + 'static,
{
    type DirEntryStream<'a> = Pin<Box<dyn Stream<Item = FuseResult<DirectoryEntry>> + Send + 'a>>
    where
        Self: 'a;
    type DirEntryPlusStream<'a> = Pin<Box<dyn Stream<Item = FuseResult<DirectoryEntryPlus>> + Send + 'a>>
    where
        Self: 'a;

    async fn init(&self, _req: Request) -> FuseResult<ReplyInit> {
        let max_write = NonZeroU32::new(1024 * 1024).unwrap(); // 1 MiB
        Ok(ReplyInit { max_write })
    }

    async fn destroy(&self, _req: Request) {}

    async fn lookup(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<ReplyEntry> {
        let name = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        // Use path-based lookup for simplicity
        let child_ino = self.vfs.lookup(parent_ino, name).await.map_err(|_| libc::ENOENT)?;
        let attr = self.vfs.getattr(child_ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: self.to_fuse_attr(attr),
            generation: 0,
        })
    }

    async fn getattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        _flags: u32,
    ) -> FuseResult<ReplyAttr> {
        let ino = Inode(inode as i64);
        let attr = self.vfs.getattr(ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: self.to_fuse_attr(attr),
        })
    }

    async fn setattr(
        &self,
        _req: Request,
        inode: u64,
        _fh: Option<u64>,
        set_attr: SetAttr,
    ) -> FuseResult<ReplyAttr> {
        let ino = Inode(inode as i64);

        // Only support truncate for now
        if let Some(size) = set_attr.size {
            self.vfs.truncate(ino, size).await.map_err(|_| libc::EIO)?;
        }

        let attr = self.vfs.getattr(ino).await.map_err(|_| libc::ENOENT)?;
        Ok(ReplyAttr {
            ttl: Duration::from_secs(1),
            attr: self.to_fuse_attr(attr),
        })
    }

    async fn mkdir(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> FuseResult<ReplyEntry> {
        let name = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        let child_ino = self.vfs.mkdir(parent_ino, name).await.map_err(|_| libc::EIO)?;
        let attr = self.vfs.getattr(child_ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyEntry {
            ttl: Duration::from_secs(1),
            attr: self.to_fuse_attr(attr),
            generation: 0,
        })
    }

    async fn create(
        &self,
        _req: Request,
        parent: u64,
        name: &OsStr,
        _mode: u32,
        _flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let name = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        let child_ino = self.vfs.create(parent_ino, name).await.map_err(|_| libc::EIO)?;
        let attr = self.vfs.getattr(child_ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyCreated {
            ttl: Duration::from_secs(1),
            attr: self.to_fuse_attr(attr),
            generation: 0,
            fh: 0, // Stateless
            flags: 0,
        })
    }

    async fn unlink(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        let name = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        self.vfs.unlink(parent_ino, name).await.map_err(|_| libc::EIO)
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        let name = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        self.vfs.rmdir(parent_ino, name).await.map_err(|_| libc::EIO)
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: u64,
        origin_name: &OsStr,
        parent: u64,
        name: &OsStr,
    ) -> FuseResult<()> {
        let origin_name = origin_name.to_str().ok_or(libc::EINVAL)?;
        let new_name = name.to_str().ok_or(libc::EINVAL)?;
        let origin_parent_ino = Inode(origin_parent as i64);
        let new_parent_ino = Inode(parent as i64);

        self.vfs
            .rename(origin_parent_ino, origin_name, new_parent_ino, new_name)
            .await
            .map_err(|_| libc::EIO)
    }

    async fn open(&self, _req: Request, inode: u64, _flags: u32) -> FuseResult<ReplyOpen> {
        // Verify file exists
        let ino = Inode(inode as i64);
        let _ = self.vfs.getattr(ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyOpen {
            fh: 0, // Stateless
            flags: 0,
        })
    }

    async fn read(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        size: u32,
    ) -> FuseResult<ReplyData> {
        let ino = Inode(inode as i64);

        let data = self
            .vfs
            .read(ino, offset, size as usize)
            .await
            .map_err(|_| libc::EIO)?;

        Ok(ReplyData {
            data: Bytes::from(data),
        })
    }

    async fn write(
        &self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: u64,
        data: &[u8],
        _write_flags: u32,
        _flags: u32,
    ) -> FuseResult<ReplyWrite> {
        let ino = Inode(inode as i64);

        let written = self
            .vfs
            .write(ino, offset, data)
            .await
            .map_err(|_| libc::EIO)?;

        Ok(ReplyWrite {
            written: written as u32,
        })
    }

    async fn release(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
        _lock_owner: u64,
        _flush: bool,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn opendir(&self, _req: Request, inode: u64, _flags: u32) -> FuseResult<ReplyOpen> {
        // Verify directory exists
        let ino = Inode(inode as i64);
        let _ = self.vfs.getattr(ino).await.map_err(|_| libc::ENOENT)?;

        Ok(ReplyOpen {
            fh: 0,
            flags: 0,
        })
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        let ino = Inode(inode as i64);

        let entries = self.vfs.readdir(ino).await.map_err(|_| libc::ENOTDIR)?;

        let mut all_entries = Vec::with_capacity(entries.len() + 2);

        // Add "." entry
        all_entries.push(DirectoryEntry {
            inode: inode,
            kind: FuseFileType::Directory,
            name: OsString::from("."),
            offset: 1,
        });

        // Add ".." entry (for simplicity, use root for parent)
        all_entries.push(DirectoryEntry {
            inode: 1,
            kind: FuseFileType::Directory,
            name: OsString::from(".."),
            offset: 2,
        });

        // Add actual entries
        for (i, entry) in entries.iter().enumerate() {
            all_entries.push(DirectoryEntry {
                inode: entry.ino as u64,
                kind: vfs_kind_to_fuse(entry.kind),
                name: OsString::from(&entry.name),
                offset: (i as i64) + 3,
            });
        }

        // Filter by offset
        let start = if offset <= 0 { 0 } else { offset as usize };
        let slice = if start >= all_entries.len() {
            Vec::new()
        } else {
            all_entries[start..].to_vec()
        };

        let stream = stream::iter(slice.into_iter().map(Ok));
        let boxed: Self::DirEntryStream<'a> = Box::pin(stream);

        Ok(ReplyDirectory { entries: boxed })
    }

    async fn releasedir(
        &self,
        _req: Request,
        _inode: u64,
        _fh: u64,
        _flags: u32,
    ) -> FuseResult<()> {
        Ok(())
    }

    async fn statfs(&self, _req: Request, _inode: u64) -> FuseResult<ReplyStatFs> {
        Ok(ReplyStatFs {
            blocks: 0,
            bfree: 0,
            bavail: 0,
            files: 0,
            ffree: u64::MAX,
            bsize: 4096,
            namelen: 255,
            frsize: 4096,
        })
    }
}

// Helper to convert VFS file type to FUSE file type
fn vfs_kind_to_fuse(kind: VfsFileType) -> FuseFileType {
    match kind {
        VfsFileType::Dir => FuseFileType::Directory,
        VfsFileType::File => FuseFileType::RegularFile,
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    // Tests will be added as integration tests due to async complexity
}
