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
            self.vfs.truncate_ino(ino, size).await.map_err(|_| libc::EIO)?;
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
        mode: u32,
        _umask: u32,
    ) -> FuseResult<ReplyEntry> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        let child_ino = self.vfs.mkdir_ino(parent_ino, name_str, mode, self.uid, self.gid)
            .await
            .map_err(|_| libc::EIO)?;
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
        mode: u32,
        _flags: u32,
    ) -> FuseResult<ReplyCreated> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        let child_ino = self.vfs.create_ino(parent_ino, name_str, mode, self.uid, self.gid)
            .await
            .map_err(|_| libc::EIO)?;
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
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        self.vfs.unlink_ino(parent_ino, name_str)
            .await
            .map_err(|_| libc::EIO.into())
    }

    async fn rmdir(&self, _req: Request, parent: u64, name: &OsStr) -> FuseResult<()> {
        let name_str = name.to_str().ok_or(libc::EINVAL)?;
        let parent_ino = Inode(parent as i64);

        self.vfs.rmdir_ino(parent_ino, name_str)
            .await
            .map_err(|_| libc::EIO.into())
    }

    async fn rename(
        &self,
        _req: Request,
        origin_parent: u64,
        origin_name: &OsStr,
        parent: u64,
        name: &OsStr,
    ) -> FuseResult<()> {
        let origin_name_str = origin_name.to_str().ok_or(libc::EINVAL)?;
        let new_name_str = name.to_str().ok_or(libc::EINVAL)?;
        let origin_parent_ino = Inode(origin_parent as i64);
        let new_parent_ino = Inode(parent as i64);

        self.vfs
            .rename_ino(origin_parent_ino, origin_name_str, new_parent_ino, new_name_str)
            .await
            .map_err(|_| libc::EIO.into())
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
            .read_ino(ino, offset, size as usize)
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
            .write_ino(ino, offset, data)
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
        eprintln!("[FUSE] opendir called for inode {}", inode);
        // Verify directory exists
        let ino = Inode(inode as i64);
        match self.vfs.getattr(ino).await {
            Ok(attr) => {
                eprintln!("[FUSE] opendir: inode {} found, type: {:?}", inode, attr.kind);
                Ok(ReplyOpen {
                    fh: 0,
                    flags: 0,
                })
            }
            Err(e) => {
                eprintln!("[FUSE] opendir: inode {} not found: {:?}", inode, e);
                Err(libc::ENOENT.into())
            }
        }
    }

    async fn readdir<'a>(
        &'a self,
        _req: Request,
        inode: u64,
        _fh: u64,
        offset: i64,
    ) -> FuseResult<ReplyDirectory<Self::DirEntryStream<'a>>> {
        eprintln!("========================================");
        eprintln!("[FUSE] readdir CALLED! inode={}, offset={}", inode, offset);
        eprintln!("========================================");
        let ino = Inode(inode as i64);

        // Read directory entries from VFS
        let entries = match self.vfs.readdir_ino(ino).await {
            Ok(entries) => {
                eprintln!("[FUSE] readdir_ino succeeded, got {} entries", entries.len());
                entries
            }
            Err(e) => {
                eprintln!("[FUSE] readdir_ino failed for inode {}: {:?}", inode, e);
                return Err(libc::EIO.into());
            }
        };

        let mut all_entries = Vec::with_capacity(entries.len() + 2);

        // Add "." entry (offset 1)
        if offset <= 0 {
            all_entries.push(DirectoryEntry {
                inode: inode,
                kind: FuseFileType::Directory,
                name: OsString::from("."),
                offset: 1,
            });
        }

        // Add ".." entry (offset 2)
        if offset <= 1 {
            all_entries.push(DirectoryEntry {
                inode: 1,
                kind: FuseFileType::Directory,
                name: OsString::from(".."),
                offset: 2,
            });
        }

        // Add actual entries (offset 3+)
        for (i, entry) in entries.iter().enumerate() {
            let entry_offset = (i as i64) + 3;
            if offset < entry_offset {
                all_entries.push(DirectoryEntry {
                    inode: entry.ino as u64,
                    kind: vfs_kind_to_fuse(entry.kind),
                    name: OsString::from(&entry.name),
                    offset: entry_offset,
                });
            }
        }

        eprintln!("[FUSE] readdir returning {} entries (offset {})", all_entries.len(), offset);
        let stream = stream::iter(all_entries.into_iter().map(Ok));
        let boxed: Self::DirEntryStream<'a> = Box::pin(stream);

        Ok(ReplyDirectory { entries: boxed })
    }

    async fn readdirplus<'a>(
        &'a self,
        _req: Request,
        parent: u64,
        _fh: u64,
        offset: u64,
        _lock_owner: u64,
    ) -> FuseResult<ReplyDirectoryPlus<Self::DirEntryPlusStream<'a>>> {
        eprintln!("========================================");
        eprintln!("[FUSE] readdirplus CALLED! parent={}, offset={}", parent, offset);
        eprintln!("========================================");
        
        let parent_ino = Inode(parent as i64);

        // Read directory entries from VFS
        let entries = match self.vfs.readdir_ino(parent_ino).await {
            Ok(entries) => {
                eprintln!("[FUSE] readdirplus: got {} entries", entries.len());
                entries
            }
            Err(e) => {
                eprintln!("[FUSE] readdirplus failed: {:?}", e);
                return Err(libc::EIO.into());
            }
        };

        let mut all_entries = Vec::with_capacity(entries.len() + 2);

        // Add "." entry (offset 1)
        if offset <= 0 {
            let dot_attr = match self.vfs.getattr(parent_ino).await {
                Ok(attr) => self.to_fuse_attr(attr),
                Err(_) => {
                    return Err(libc::EIO.into());
                }
            };
            all_entries.push(DirectoryEntryPlus {
                inode: parent,
                generation: 0,
                kind: FuseFileType::Directory,
                name: OsString::from("."),
                offset: 1,
                attr: dot_attr,
                entry_ttl: std::time::Duration::from_secs(1),
                attr_ttl: std::time::Duration::from_secs(1),
            });
        }

        // Add ".." entry (offset 2) - for simplicity, use root
        if offset <= 1 {
            let dotdot_ino = Inode(1);
            let dotdot_attr = match self.vfs.getattr(dotdot_ino).await {
                Ok(attr) => self.to_fuse_attr(attr),
                Err(_) => {
                    return Err(libc::EIO.into());
                }
            };
            all_entries.push(DirectoryEntryPlus {
                inode: 1,
                generation: 0,
                kind: FuseFileType::Directory,
                name: OsString::from(".."),
                offset: 2,
                attr: dotdot_attr,
                entry_ttl: std::time::Duration::from_secs(1),
                attr_ttl: std::time::Duration::from_secs(1),
            });
        }

        // Add actual entries (offset 3+)
        for (i, entry) in entries.iter().enumerate() {
            let entry_offset = (i as i64) + 3;
            if offset < entry_offset as u64 {
                let entry_ino = Inode(entry.ino);
                let attr = match self.vfs.getattr(entry_ino).await {
                    Ok(attr) => self.to_fuse_attr(attr),
                    Err(_) => continue, // Skip entries we can't get attrs for
                };
                
                all_entries.push(DirectoryEntryPlus {
                    inode: entry.ino as u64,
                    generation: 0,
                    kind: vfs_kind_to_fuse(entry.kind),
                    name: OsString::from(&entry.name),
                    offset: entry_offset,
                    attr,
                    entry_ttl: std::time::Duration::from_secs(1),
                    attr_ttl: std::time::Duration::from_secs(1),
                });
            }
        }

        eprintln!("[FUSE] readdirplus returning {} entries", all_entries.len());
        let stream = stream::iter(all_entries.into_iter().map(Ok));
        let boxed: Self::DirEntryPlusStream<'a> = Box::pin(stream);

        Ok(ReplyDirectoryPlus { entries: boxed })
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
