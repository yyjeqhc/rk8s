//! DatabaseMetaStore implementation of MetaStoreV2 trait
//!
//! This module provides the V2 interface implementation for DatabaseMetaStore,
//! using strong-typed Inode and structured parameters.

use crate::meta::database_store::DatabaseMetaStore;
use crate::meta::entities::*;
use crate::meta::error::MetaErrorHelper;
use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::store_v2::MetaStoreV2;
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::meta::Permission;
use crate::vfs::fs::FileType;
use async_trait::async_trait;
use chrono::Utc;
use sea_orm::*;

#[async_trait]
impl MetaStoreV2 for DatabaseMetaStore {
    async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError> {
        // Try file_meta first
        if let Some(file) = FileMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
        {
            let permission = file.permission();
            return Ok(FileAttr {
                ino: ino.as_i64(),
                size: file.size as u64,
                kind: FileType::File,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: file.access_time,
                mtime: file.modify_time,
                ctime: file.create_time,
                nlink: file.nlink as u32,
            });
        }

        // Try access_meta (directory)
        if let Some(dir) = AccessMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
        {
            let permission = dir.permission();
            return Ok(FileAttr {
                ino: ino.as_i64(),
                size: 4096, // Directory size
                kind: FileType::Dir,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: dir.access_time,
                mtime: dir.modify_time,
                ctime: dir.create_time,
                nlink: dir.nlink as u32,
            });
        }

        Err(MetaError::not_found(ino))
    }

    async fn getattr_batch(&self, inos: &[Inode]) -> Result<Vec<(Inode, FileAttr)>, MetaError> {
        if inos.is_empty() {
            return Ok(Vec::new());
        }

        let mut results = Vec::with_capacity(inos.len());
        let ino_i64s: Vec<i64> = inos.iter().map(|i| i.as_i64()).collect();

        // Batch query files
        let files = FileMeta::find()
            .filter(file_meta::Column::Inode.is_in(ino_i64s.clone()))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        for file in files {
            let permission = file.permission();
            let attr = FileAttr {
                ino: file.inode,
                size: file.size as u64,
                kind: FileType::File,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: file.access_time,
                mtime: file.modify_time,
                ctime: file.create_time,
                nlink: file.nlink as u32,
            };
            results.push((Inode(file.inode), attr));
        }

        // Batch query directories
        let dirs = AccessMeta::find()
            .filter(access_meta::Column::Inode.is_in(ino_i64s))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        for dir in dirs {
            let permission = dir.permission();
            let attr = FileAttr {
                ino: dir.inode,
                size: 4096,
                kind: FileType::Dir,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: dir.access_time,
                mtime: dir.modify_time,
                ctime: dir.create_time,
                nlink: dir.nlink as u32,
            };
            results.push((Inode(dir.inode), attr));
        }

        Ok(results)
    }

    async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError> {
        let content = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(name))
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::not_found(parent))?;

        Ok(Inode(content.inode))
    }

    async fn readdir(&self, ino: Inode) -> Result<Vec<DirEntry>, MetaError> {
        // Verify it's a directory
        if AccessMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
            .is_none()
        {
            return Err(MetaError::not_directory(ino));
        }

        let contents = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(ino.as_i64()))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        let entries = contents
            .into_iter()
            .map(|c| DirEntry {
                ino: c.inode,
                name: c.entry_name,
                kind: match c.entry_type {
                    EntryType::File => FileType::File,
                    EntryType::Directory => FileType::Dir,
                },
            })
            .collect();

        Ok(entries)
    }

    async fn readdirplus(&self, ino: Inode) -> Result<Vec<(DirEntry, FileAttr)>, MetaError> {
        // Verify it's a directory
        if AccessMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
            .is_none()
        {
            return Err(MetaError::not_directory(ino));
        }

        // Get all directory entries
        let contents = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(ino.as_i64()))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        if contents.is_empty() {
            return Ok(Vec::new());
        }

        // Collect all inodes
        let inos: Vec<Inode> = contents.iter().map(|c| Inode(c.inode)).collect();

        // Batch get attributes
        let attrs = self.getattr_batch(&inos).await?;
        let attr_map: std::collections::HashMap<i64, FileAttr> =
            attrs.into_iter().map(|(ino, attr)| (ino.as_i64(), attr)).collect();

        // Combine results
        let mut results = Vec::with_capacity(contents.len());
        for content in contents {
            let entry = DirEntry {
                ino: content.inode,
                name: content.entry_name,
                kind: match content.entry_type {
                    EntryType::File => FileType::File,
                    EntryType::Directory => FileType::Dir,
                },
            };

            if let Some(attr) = attr_map.get(&content.inode) {
                results.push((entry, attr.clone()));
            }
        }

        Ok(results)
    }

    async fn create(&self, params: CreateParams) -> Result<(Inode, FileAttr), MetaError> {
        // Start transaction
        let txn = self.db.begin().await.map_err(MetaError::Database)?;

        // 1. Verify parent exists and is a directory
        let parent_exists = AccessMeta::find_by_id(params.parent.as_i64())
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
            .is_some();

        if !parent_exists {
            return Err(MetaError::parent_not_found(params.parent));
        }

        // 2. Check for duplicate name
        let existing = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(params.parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(&params.name))
            .one(&txn)
            .await
            .map_err(MetaError::Database)?;

        if existing.is_some() {
            return Err(MetaError::already_exists(params.parent, params.name));
        }

        // 3. Allocate new inode
        let ino = Inode(self.generate_id());
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // 4. Create metadata based on type
        let permission = Permission::new(params.mode, params.uid, params.gid);

        match params.kind {
            FileType::File => {
                let file = file_meta::ActiveModel {
                    inode: Set(ino.as_i64()),
                    size: Set(0),
                    permission: Set(permission),
                    access_time: Set(now),
                    modify_time: Set(now),
                    create_time: Set(now),
                    nlink: Set(1),
                };
                file.insert(&txn).await.map_err(MetaError::Database)?;
            }
            FileType::Dir => {
                let dir_perm = Permission::new(params.mode | 0o40000, params.uid, params.gid);
                let dir = access_meta::ActiveModel {
                    inode: Set(ino.as_i64()),
                    permission: Set(dir_perm),
                    access_time: Set(now),
                    modify_time: Set(now),
                    create_time: Set(now),
                    nlink: Set(2),
                };
                dir.insert(&txn).await.map_err(MetaError::Database)?;
            }
        }

        // 5. Add directory entry
        let content = content_meta::ActiveModel {
            inode: Set(ino.as_i64()),
            parent_inode: Set(params.parent.as_i64()),
            entry_name: Set(params.name),
            entry_type: Set(match params.kind {
                FileType::File => EntryType::File,
                FileType::Dir => EntryType::Directory,
            }),
        };
        content.insert(&txn).await.map_err(MetaError::Database)?;

        // 6. Commit transaction
        txn.commit().await.map_err(MetaError::Database)?;

        // 7. Get and return attributes
        let attr = self.getattr(ino).await?;
        Ok((ino, attr))
    }

    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> Result<FileAttr, MetaError> {
        if mask.is_empty() {
            return self.getattr(ino).await;
        }

        let txn = self.db.begin().await.map_err(MetaError::Database)?;

        // Try to update file_meta
        if let Some(file) = FileMeta::find_by_id(ino.as_i64())
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
        {
            let mut active: file_meta::ActiveModel = file.into();

            if let Some(size) = mask.size {
                active.size = Set(size as i64);
            }
            if let Some(mode) = mask.mode {
                let mut perm = active.permission.clone().unwrap();
                perm.mode = mode;
                active.permission = Set(perm);
            }
            if let Some(uid) = mask.uid {
                let mut perm = active.permission.clone().unwrap();
                perm.uid = uid;
                active.permission = Set(perm);
            }
            if let Some(gid) = mask.gid {
                let mut perm = active.permission.clone().unwrap();
                perm.gid = gid;
                active.permission = Set(perm);
            }
            if let Some(atime) = mask.atime {
                active.access_time = Set(atime);
            }
            if let Some(mtime) = mask.mtime {
                active.modify_time = Set(mtime);
            }

            active.update(&txn).await.map_err(MetaError::Database)?;
            txn.commit().await.map_err(MetaError::Database)?;
            return self.getattr(ino).await;
        }

        // Try to update access_meta (directory)
        if let Some(dir) = AccessMeta::find_by_id(ino.as_i64())
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
        {
            let mut active: access_meta::ActiveModel = dir.into();

            if let Some(mode) = mask.mode {
                let mut perm = active.permission.clone().unwrap();
                perm.mode = mode | 0o40000; // Keep directory flag
                active.permission = Set(perm);
            }
            if let Some(uid) = mask.uid {
                let mut perm = active.permission.clone().unwrap();
                perm.uid = uid;
                active.permission = Set(perm);
            }
            if let Some(gid) = mask.gid {
                let mut perm = active.permission.clone().unwrap();
                perm.gid = gid;
                active.permission = Set(perm);
            }
            if let Some(atime) = mask.atime {
                active.access_time = Set(atime);
            }
            if let Some(mtime) = mask.mtime {
                active.modify_time = Set(mtime);
            }

            active.update(&txn).await.map_err(MetaError::Database)?;
            txn.commit().await.map_err(MetaError::Database)?;
            return self.getattr(ino).await;
        }

        Err(MetaError::not_found(ino))
    }

    async fn rename(
        &self,
        old_parent: Inode,
        old_name: &str,
        new_parent: Inode,
        new_name: String,
    ) -> Result<(), MetaError> {
        let txn = self.db.begin().await.map_err(MetaError::Database)?;

        // 1. Find old entry
        let old_entry = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(old_parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(old_name))
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::not_found(old_parent))?;

        // 2. Check if new parent exists
        let new_parent_exists = AccessMeta::find_by_id(new_parent.as_i64())
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
            .is_some();

        if !new_parent_exists {
            return Err(MetaError::parent_not_found(new_parent));
        }

        // 3. Check if target exists (for overwrite semantics)
        let existing = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(new_parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(&new_name))
            .one(&txn)
            .await
            .map_err(MetaError::Database)?;

        if let Some(existing_entry) = existing {
            // If target exists and is a directory, check if empty
            if existing_entry.entry_type == EntryType::Directory {
                let children = ContentMeta::find()
                    .filter(content_meta::Column::ParentInode.eq(existing_entry.inode))
                    .count(&txn)
                    .await
                    .map_err(MetaError::Database)?;

                if children > 0 {
                    return Err(MetaError::directory_not_empty(Inode(existing_entry.inode)));
                }
            }

            // Delete existing entry
            ContentMeta::delete_by_id(existing_entry.inode)
                .exec(&txn)
                .await
                .map_err(MetaError::Database)?;
        }

        // 4. Delete old directory entry
        let entry_inode = old_entry.inode;
        let entry_type = old_entry.entry_type.clone();
        let old_model: content_meta::ActiveModel = old_entry.into();
        old_model.delete(&txn).await.map_err(MetaError::Database)?;

        // 5. Create new directory entry
        let new_content = content_meta::ActiveModel {
            inode: Set(entry_inode),
            parent_inode: Set(new_parent.as_i64()),
            entry_name: Set(new_name),
            entry_type: Set(entry_type),
        };
        new_content.insert(&txn).await.map_err(MetaError::Database)?;

        // 6. Commit transaction
        txn.commit().await.map_err(MetaError::Database)?;

        Ok(())
    }

    async fn unlink(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        let txn = self.db.begin().await.map_err(MetaError::Database)?;

        // 1. Find the entry
        let entry = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(name))
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::not_found(parent))?;

        // 2. Verify it's not a directory
        if entry.entry_type == EntryType::Directory {
            return Err(MetaError::Internal("Cannot unlink directory, use rmdir".into()));
        }

        // 3. Delete directory entry
        let entry_model: content_meta::ActiveModel = entry.clone().into();
        entry_model.delete(&txn).await.map_err(MetaError::Database)?;

        // 4. Decrement nlink and possibly delete file metadata
        if let Some(file) = FileMeta::find_by_id(entry.inode)
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
        {
            let mut active: file_meta::ActiveModel = file.into();
            let nlink = active.nlink.clone().unwrap();

            if nlink <= 1 {
                // Last link, delete the file metadata
                active.delete(&txn).await.map_err(MetaError::Database)?;
            } else {
                // Decrement nlink
                active.nlink = Set(nlink - 1);
                active.update(&txn).await.map_err(MetaError::Database)?;
            }
        }

        txn.commit().await.map_err(MetaError::Database)?;
        Ok(())
    }

    async fn rmdir(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        let txn = self.db.begin().await.map_err(MetaError::Database)?;

        // 1. Find the entry
        let entry = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(name))
            .one(&txn)
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| MetaError::not_found(parent))?;

        // 2. Verify it's a directory
        if entry.entry_type != EntryType::Directory {
            return Err(MetaError::not_directory(Inode(entry.inode)));
        }

        // 3. Check if directory is empty
        let children = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(entry.inode))
            .count(&txn)
            .await
            .map_err(MetaError::Database)?;

        if children > 0 {
            return Err(MetaError::directory_not_empty(Inode(entry.inode)));
        }

        // 4. Delete directory entry
        let entry_model: content_meta::ActiveModel = entry.clone().into();
        entry_model.delete(&txn).await.map_err(MetaError::Database)?;

        // 5. Delete access_meta
        AccessMeta::delete_by_id(entry.inode)
            .exec(&txn)
            .await
            .map_err(MetaError::Database)?;

        txn.commit().await.map_err(MetaError::Database)?;
        Ok(())
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        // Already initialized in DatabaseMetaStore::new()
        Ok(())
    }

    fn root_ino(&self) -> Inode {
        Inode::ROOT
    }
}
