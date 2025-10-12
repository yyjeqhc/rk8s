//! EtcdMetaStore implementation of MetaStoreV2 trait
//!
//! This module provides the V2 interface implementation for EtcdMetaStore,
//! using strong-typed Inode and structured parameters.

use crate::meta::entities::etcd::*;
use crate::meta::error::MetaErrorHelper;
use crate::meta::etcd_store::EtcdMetaStore;
use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::store_v2::MetaStoreV2;
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::meta::Permission;
use crate::vfs::fs::FileType;
use async_trait::async_trait;
use chrono::Utc;
use std::collections::HashSet;

#[async_trait]
impl MetaStoreV2 for EtcdMetaStore {
    async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError> {
        // Special case for root
        if ino == Inode::ROOT {
            let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
            return Ok(FileAttr {
                ino: 1,
                size: 4096,
                kind: FileType::Dir,
                mode: 0o755,
                uid: 0,
                gid: 0,
                atime: now,
                mtime: now,
                ctime: now,
                nlink: 2,
            });
        }

        let reverse_key = Self::etcd_reverse_key(ino.as_i64());
        let mut client = self.client.clone();

        let resp = client
            .get(reverse_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let entry_info: EtcdEntryInfo = serde_json::from_slice(kv.value())?;
            let permission = entry_info.permission();

            if entry_info.is_file {
                Ok(FileAttr {
                    ino: ino.as_i64(),
                    size: entry_info.size.unwrap_or(0) as u64,
                    kind: FileType::File,
                    mode: permission.mode,
                    uid: permission.uid,
                    gid: permission.gid,
                    atime: entry_info.access_time,
                    mtime: entry_info.modify_time,
                    ctime: entry_info.create_time,
                    nlink: entry_info.nlink as u32,
                })
            } else {
                Ok(FileAttr {
                    ino: ino.as_i64(),
                    size: 4096,
                    kind: FileType::Dir,
                    mode: permission.mode,
                    uid: permission.uid,
                    gid: permission.gid,
                    atime: entry_info.access_time,
                    mtime: entry_info.modify_time,
                    ctime: entry_info.create_time,
                    nlink: entry_info.nlink as u32,
                })
            }
        } else {
            Err(MetaError::not_found(ino))
        }
    }

    async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError> {
        let forward_key = Self::etcd_forward_key(parent.as_i64(), name);
        let mut client = self.client.clone();

        let resp = client
            .get(forward_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let forward_entry: EtcdForwardEntry = serde_json::from_slice(kv.value())?;
            Ok(Inode(forward_entry.inode))
        } else {
            Err(MetaError::not_found(parent))
        }
    }

    async fn readdir(&self, ino: Inode) -> Result<Vec<DirEntry>, MetaError> {
        let children_key = Self::etcd_children_key(ino.as_i64());
        let mut client = self.client.clone();

        let resp = client
            .get(children_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let dir_children: EtcdDirChildren = serde_json::from_slice(kv.value())?;

            if dir_children.children.is_empty() {
                return Ok(Vec::new());
            }

            let mut entries = Vec::new();

            for child_name in &dir_children.children {
                let forward_key = Self::etcd_forward_key(ino.as_i64(), child_name);
                if let Ok(forward_resp) = client.get(forward_key, None).await {
                    if let Some(forward_kv) = forward_resp.kvs().first() {
                        let forward_entry: EtcdForwardEntry =
                            serde_json::from_slice(forward_kv.value())?;

                        entries.push(DirEntry {
                            ino: forward_entry.inode,
                            name: child_name.clone(),
                            kind: if forward_entry.is_file {
                                FileType::File
                            } else {
                                FileType::Dir
                            },
                        });
                    }
                }
            }

            Ok(entries)
        } else {
            Ok(Vec::new())
        }
    }

    async fn create(&self, params: CreateParams) -> Result<(Inode, FileAttr), MetaError> {
        let mut client = self.client.clone();

        // 1. Verify parent exists
        let parent_children_key = Self::etcd_children_key(params.parent.as_i64());
        let parent_resp = client
            .get(parent_children_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if parent_resp.kvs().is_empty() {
            return Err(MetaError::parent_not_found(params.parent));
        }

        // 2. Check for duplicate name
        let forward_key = Self::etcd_forward_key(params.parent.as_i64(), &params.name);
        let existing = client.get(forward_key.clone(), None).await;

        if let Ok(resp) = existing {
            if !resp.kvs().is_empty() {
                return Err(MetaError::already_exists(params.parent, params.name));
            }
        }

        // 3. Allocate new inode (using a simple counter approach)
        // In production, you'd want a distributed counter
        let ino = Inode(Utc::now().timestamp_millis());
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        // 4. Create entry info
        let is_file = matches!(params.kind, FileType::File);
        let permission = Permission::new(
            if is_file {
                params.mode
            } else {
                params.mode | 0o40000
            },
            params.uid,
            params.gid,
        );

        let entry_info = EtcdEntryInfo {
            is_file,
            size: if is_file { Some(0) } else { None },
            version: Some(1),
            access_time: now,
            modify_time: now,
            create_time: now,
            permission: permission.clone(),
            nlink: if is_file { 1 } else { 2 },
        };

        // 5. Store reverse entry
        let reverse_key = Self::etcd_reverse_key(ino.as_i64());
        let entry_json = serde_json::to_string(&entry_info)?;
        client
            .put(reverse_key, entry_json, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 6. Store forward entry
        let forward_entry = EtcdForwardEntry {
            parent_inode: params.parent.as_i64(),
            name: params.name.clone(),
            inode: ino.as_i64(),
            is_file,
        };
        let forward_json = serde_json::to_string(&forward_entry)?;
        client
            .put(forward_key, forward_json, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 7. Update parent's children list
        let mut dir_children: EtcdDirChildren = if let Some(kv) = parent_resp.kvs().first() {
            serde_json::from_slice(kv.value())?
        } else {
            EtcdDirChildren {
                inode: params.parent.as_i64(),
                children: HashSet::new(),
            }
        };

        dir_children.children.insert(params.name.clone());
        let children_json = serde_json::to_string(&dir_children)?;
        client
            .put(parent_children_key, children_json, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 8. Initialize children set for directories
        if !is_file {
            let children_key = Self::etcd_children_key(ino.as_i64());
            let empty_children = EtcdDirChildren {
                inode: ino.as_i64(),
                children: HashSet::new(),
            };
            let empty_json = serde_json::to_string(&empty_children)?;
            client
                .put(children_key, empty_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;
        }

        // 9. Return attributes
        let attr = FileAttr {
            ino: ino.as_i64(),
            size: if is_file { 0 } else { 4096 },
            kind: params.kind,
            mode: permission.mode,
            uid: permission.uid,
            gid: permission.gid,
            atime: now,
            mtime: now,
            ctime: now,
            nlink: if is_file { 1 } else { 2 },
        };

        Ok((ino, attr))
    }

    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> Result<FileAttr, MetaError> {
        if mask.is_empty() {
            return self.getattr(ino).await;
        }

        let mut client = self.client.clone();
        let reverse_key = Self::etcd_reverse_key(ino.as_i64());

        // Get current entry
        let resp = client
            .get(reverse_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = resp.kvs().first() {
            let mut entry_info: EtcdEntryInfo = serde_json::from_slice(kv.value())?;

            // Apply updates
            if let Some(size) = mask.size {
                entry_info.size = Some(size as i64);
            }
            if let Some(mode) = mask.mode {
                entry_info.permission.mode = if entry_info.is_file {
                    mode
                } else {
                    mode | 0o40000
                };
            }
            if let Some(uid) = mask.uid {
                entry_info.permission.uid = uid;
            }
            if let Some(gid) = mask.gid {
                entry_info.permission.gid = gid;
            }
            if let Some(atime) = mask.atime {
                entry_info.access_time = atime;
            }
            if let Some(mtime) = mask.mtime {
                entry_info.modify_time = mtime;
            }

            // Store updated entry
            let entry_json = serde_json::to_string(&entry_info)?;
            client
                .put(reverse_key, entry_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

            // Return updated attributes
            self.getattr(ino).await
        } else {
            Err(MetaError::not_found(ino))
        }
    }

    async fn rename(
        &self,
        old_parent: Inode,
        old_name: &str,
        new_parent: Inode,
        new_name: String,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();

        // 1. Get the inode of the entry to rename
        let old_forward_key = Self::etcd_forward_key(old_parent.as_i64(), old_name);
        let old_resp = client
            .get(old_forward_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        let forward_entry: EtcdForwardEntry = if let Some(kv) = old_resp.kvs().first() {
            serde_json::from_slice(kv.value())?
        } else {
            return Err(MetaError::not_found(old_parent));
        };

        // 2. Verify new parent exists
        let new_parent_key = Self::etcd_children_key(new_parent.as_i64());
        let new_parent_resp = client
            .get(new_parent_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if new_parent_resp.kvs().is_empty() {
            return Err(MetaError::parent_not_found(new_parent));
        }

        // 3. Remove from old parent's children
        let old_parent_key = Self::etcd_children_key(old_parent.as_i64());
        let old_parent_resp = client
            .get(old_parent_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = old_parent_resp.kvs().first() {
            let mut old_children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
            old_children.children.remove(old_name);
            let children_json = serde_json::to_string(&old_children)?;
            client
                .put(old_parent_key, children_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;
        }

        // 4. Add to new parent's children
        if let Some(kv) = new_parent_resp.kvs().first() {
            let mut new_children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
            new_children.children.insert(new_name.clone());
            let children_json = serde_json::to_string(&new_children)?;
            client
                .put(new_parent_key, children_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;
        }

        // 5. Delete old forward entry
        client
            .delete(old_forward_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 6. Create new forward entry
        let new_forward_key = Self::etcd_forward_key(new_parent.as_i64(), &new_name);
        let forward_json = serde_json::to_string(&forward_entry)?;
        client
            .put(new_forward_key, forward_json, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        Ok(())
    }

    async fn unlink(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        let mut client = self.client.clone();

        // 1. Get forward entry
        let forward_key = Self::etcd_forward_key(parent.as_i64(), name);
        let resp = client
            .get(forward_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        let forward_entry: EtcdForwardEntry = if let Some(kv) = resp.kvs().first() {
            serde_json::from_slice(kv.value())?
        } else {
            return Err(MetaError::not_found(parent));
        };

        // 2. Verify it's a file
        if !forward_entry.is_file {
            return Err(MetaError::Internal("Cannot unlink directory, use rmdir".into()));
        }

        // 3. Remove from parent's children
        let parent_children_key = Self::etcd_children_key(parent.as_i64());
        let parent_resp = client
            .get(parent_children_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = parent_resp.kvs().first() {
            let mut children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
            children.children.remove(name);
            let children_json = serde_json::to_string(&children)?;
            client
                .put(parent_children_key, children_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;
        }

        // 4. Delete forward entry
        client
            .delete(forward_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 5. Delete reverse entry (metadata)
        let reverse_key = Self::etcd_reverse_key(forward_entry.inode);
        client
            .delete(reverse_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        Ok(())
    }

    async fn rmdir(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        let mut client = self.client.clone();

        // 1. Get forward entry
        let forward_key = Self::etcd_forward_key(parent.as_i64(), name);
        let resp = client
            .get(forward_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        let forward_entry: EtcdForwardEntry = if let Some(kv) = resp.kvs().first() {
            serde_json::from_slice(kv.value())?
        } else {
            return Err(MetaError::not_found(parent));
        };

        // 2. Verify it's a directory
        if forward_entry.is_file {
            return Err(MetaError::not_directory(Inode(forward_entry.inode)));
        }

        // 3. Check if directory is empty
        let children_key = Self::etcd_children_key(forward_entry.inode);
        let children_resp = client
            .get(children_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = children_resp.kvs().first() {
            let children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
            if !children.children.is_empty() {
                return Err(MetaError::directory_not_empty(Inode(forward_entry.inode)));
            }
        }

        // 4. Remove from parent's children
        let parent_children_key = Self::etcd_children_key(parent.as_i64());
        let parent_resp = client
            .get(parent_children_key.clone(), None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        if let Some(kv) = parent_resp.kvs().first() {
            let mut parent_children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
            parent_children.children.remove(name);
            let parent_json = serde_json::to_string(&parent_children)?;
            client
                .put(parent_children_key, parent_json, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;
        }

        // 5. Delete directory's children list
        client
            .delete(children_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 6. Delete forward entry
        client
            .delete(forward_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        // 7. Delete reverse entry
        let reverse_key = Self::etcd_reverse_key(forward_entry.inode);
        client
            .delete(reverse_key, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Etcd error: {}", e)))?;

        Ok(())
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        // Already initialized in EtcdMetaStore::new()
        Ok(())
    }

    fn root_ino(&self) -> Inode {
        Inode::ROOT
    }
}
