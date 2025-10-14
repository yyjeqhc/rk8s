//! Etcd-based metadata store implementation
//!
//! Uses Etcd/etcd as the backend for metadata storage

use crate::meta::entities::etcd::*;
use crate::meta::error::MetaErrorHelper;
use crate::meta::id_generator::{EtcdIdGenerator, IdGenerator};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};

use crate::meta::Permission;
use crate::meta::config::{Config, DatabaseType};
use crate::meta::entities::*;
use crate::meta::store::{DirEntry, FileAttr, MetaError, MetaStore};
use crate::vfs::fs::FileType;
use async_trait::async_trait;
use chrono::Utc;
use etcd_client::Client as EtcdClient;
use log::{error, info};
use serde_json;
use std::collections::HashSet;
use std::path::Path;
use std::sync::Arc;

/// Etcd-based metadata store (stateless)
pub struct EtcdMetaStore {
    pub(crate) client: EtcdClient,
    pub(crate) _config: Config,
    pub(crate) id_gen: Arc<dyn IdGenerator>,
}
#[allow(dead_code)]
impl EtcdMetaStore {
    /// Etcd helper method: generate forward index key (parent_inode, name)
    pub(crate) fn etcd_forward_key(parent_inode: i64, name: &str) -> String {
        format!("f:{}:{}", parent_inode, name)
    }

    /// Etcd helper method: generate reverse index key for inode
    pub(crate) fn etcd_reverse_key(ino: i64) -> String {
        format!("r:{}", ino)
    }

    /// Etcd helper method: generate directory children key
    pub(crate) fn etcd_children_key(inode: i64) -> String {
        format!("c:{}", inode)
    }

    /// Create or open an etcd metadata store
    pub async fn new(backend_path: &Path) -> Result<Self, MetaError> {
        let _config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;

        info!("Initializing EtcdMetaStore");
        info!("Backend path: {}", backend_path.display());

        let client = Self::create_client(&_config).await?;

        // Create ID generator
        let id_gen = Arc::new(EtcdIdGenerator::new(client.clone()));
        id_gen.initialize().await?;

        let store = Self {
            client,
            _config,
            id_gen,
        };
        store.init_root_directory().await?;

        info!("EtcdMetaStore initialized successfully");
        Ok(store)
    }

    /// Create from existing config
    pub async fn from_config(_config: Config) -> Result<Self, MetaError> {
        info!("Initializing EtcdMetaStore from config");

        let client = Self::create_client(&_config).await?;

        // Create ID generator
        let id_gen = Arc::new(EtcdIdGenerator::new(client.clone()));
        id_gen.initialize().await?;

        let store = Self {
            client,
            _config,
            id_gen,
        };
        store.init_root_directory().await?;

        info!("EtcdMetaStore initialized successfully");
        Ok(store)
    }

    /// Create etcd client
    async fn create_client(config: &Config) -> Result<EtcdClient, MetaError> {
        let db_config = config
            .database()
            .ok_or_else(|| MetaError::Config("Database backend not configured".to_string()))?;

        match &db_config.db_config {
            DatabaseType::Etcd { urls } => {
                info!("Connecting to Etcd cluster: {:?}", urls);
                let client = EtcdClient::connect(urls, None)
                    .await
                    .map_err(|e| MetaError::Config(format!("Failed to connect to Etcd: {}", e)))?;
                Ok(client)
            }
            DatabaseType::Sqlite { .. } | DatabaseType::Postgres { .. } => {
                Err(MetaError::Config(
                    "SQL database backend not supported by EtcdMetaStore. Use DatabaseMetaStore instead."
                        .to_string(),
                ))
            }
        }
    }

    /// Initialize root directory
    async fn init_root_directory(&self) -> Result<(), MetaError> {
        let children_key = Self::etcd_children_key(1);
        let mut client = self.client.clone();

        if let Ok(resp) = client.get(children_key.clone(), None).await
            && !resp.kvs().is_empty()
        {
            info!("Root directory already initialized for Etcd backend");
            return Ok(());
        }

        let root_children = EtcdDirChildren {
            inode: 1,
            children: HashSet::new(),
        };

        let children_json = serde_json::to_string(&root_children)?;
        client
            .put(children_key, children_json, None)
            .await
            .map_err(|e| {
                MetaError::Config(format!(
                    "Failed to initialize root directory in Etcd: {}",
                    e
                ))
            })?;

        info!("Root directory initialized for Etcd backend");
        Ok(())
    }

    /// Get directory access metadata
    async fn get_access_meta(&self, inode: i64) -> Result<Option<AccessMetaModel>, MetaError> {
        if inode == 1 {
            let now = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);
            return Ok(Some(AccessMetaModel {
                inode: 1,
                permission: Permission::new(0o40755, 0, 0),
                access_time: now,
                modify_time: now,
                create_time: now,
                nlink: 2,
            }));
        }

        let reverse_key = Self::etcd_reverse_key(inode);
        let mut client = self.client.clone();

        match client.get(reverse_key, None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let entry_info: EtcdEntryInfo = serde_json::from_slice(kv.value())?;
                    if !entry_info.is_file {
                        let permission = entry_info.permission().clone();
                        let access_meta = AccessMetaModel::from_permission(
                            inode,
                            permission,
                            entry_info.access_time,
                            entry_info.modify_time,
                            entry_info.create_time,
                            entry_info.nlink as i32,
                        );
                        return Ok(Some(access_meta));
                    }
                }
                Ok(None)
            }
            Err(_) => Ok(None),
        }
    }

    /// Get directory content metadata
    async fn get_content_meta(
        &self,
        parent_inode: i64,
    ) -> Result<Option<Vec<ContentMetaModel>>, MetaError> {
        let children_key = Self::etcd_children_key(parent_inode);
        let mut client = self.client.clone();

        match client.get(children_key, None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let dir_children: EtcdDirChildren = serde_json::from_slice(kv.value())?;

                    if dir_children.children.is_empty() {
                        return Ok(None);
                    }

                    let mut content_list = Vec::new();

                    for child_name in &dir_children.children {
                        let forward_key = Self::etcd_forward_key(parent_inode, child_name);
                        if let Ok(forward_resp) = client.get(forward_key, None).await
                            && let Some(forward_kv) = forward_resp.kvs().first()
                        {
                            let forward_entry: EtcdForwardEntry =
                                serde_json::from_slice(forward_kv.value())?;

                            let entry_type = if forward_entry.is_file {
                                EntryType::File
                            } else {
                                EntryType::Directory
                            };

                            content_list.push(ContentMetaModel {
                                inode: forward_entry.inode,
                                parent_inode,
                                entry_name: child_name.clone(),
                                entry_type,
                            });
                        }
                    }

                    if content_list.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some(content_list))
                    }
                } else {
                    Ok(None)
                }
            }
            Err(_) => Ok(None),
        }
    }

    /// Get file metadata
    async fn get_file_meta(&self, inode: i64) -> Result<Option<FileMetaModel>, MetaError> {
        let reverse_key = Self::etcd_reverse_key(inode);
        let mut client = self.client.clone();

        match client.get(reverse_key.clone(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let entry_info: EtcdEntryInfo = serde_json::from_slice(kv.value())?;

                    if entry_info.is_file {
                        let permission = entry_info.permission().clone();
                        let file_meta = FileMetaModel::from_permission(
                            inode,
                            entry_info.size.unwrap_or(0),
                            permission,
                            entry_info.access_time,
                            entry_info.modify_time,
                            entry_info.create_time,
                            entry_info.nlink as i32,
                        );
                        Ok(Some(file_meta))
                    } else {
                        Ok(None)
                    }
                } else {
                    Ok(None)
                }
            }
            Err(e) => {
                error!("Failed to get file meta from Etcd: {}", e);
                Ok(None)
            }
        }
    }

    /// Create a new directory
    async fn create_directory(&self, parent_inode: i64, name: String) -> Result<i64, MetaError> {
        if self.get_access_meta(parent_inode).await?.is_none() {
            return Err(MetaError::ParentNotFound(parent_inode));
        }

        if let Some(contents) = self.get_content_meta(parent_inode).await? {
            for content in contents {
                if content.entry_name == name {
                    return Err(MetaError::AlreadyExists {
                        parent: parent_inode,
                        name,
                    });
                }
            }
        }

        let inode = self.generate_id().await?;
        let mut client = self.client.clone();

        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let dir_permission = Permission::new(0o40755, 0, 0);
        let entry_info = EtcdEntryInfo {
            is_file: false,
            size: None,
            version: None,
            permission: dir_permission,
            access_time: now,
            modify_time: now,
            create_time: now,
            nlink: 2,
        };

        let forward_key = Self::etcd_forward_key(parent_inode, &name);
        let forward_entry = EtcdForwardEntry {
            parent_inode,
            name: name.clone(),
            inode,
            is_file: false,
        };
        let forward_json = serde_json::to_string(&forward_entry)?;

        let reverse_key = Self::etcd_reverse_key(inode);
        let reverse_json = serde_json::to_string(&entry_info)?;

        let children_key = Self::etcd_children_key(inode);
        let children = EtcdDirChildren {
            inode,
            children: HashSet::new(),
        };
        let children_json = serde_json::to_string(&children)?;

        let parent_children_key = Self::etcd_children_key(parent_inode);
        let parent_children = match client.get(parent_children_key.clone(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let mut children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
                    children.children.insert(name.clone());
                    children
                } else {
                    let mut children = EtcdDirChildren {
                        inode: parent_inode,
                        children: HashSet::new(),
                    };
                    children.children.insert(name.clone());
                    children
                }
            }
            Err(e) => {
                error!("Failed to get parent directory children: {}", e);
                return Err(MetaError::Config(format!(
                    "Failed to get parent directory children: {}",
                    e
                )));
            }
        };
        let parent_children_json = serde_json::to_string(&parent_children)?;

        client
            .put(forward_key, forward_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to create forward index: {}", e)))?;
        client
            .put(reverse_key, reverse_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to create reverse index: {}", e)))?;
        client
            .put(children_key, children_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to create children index: {}", e)))?;
        client
            .put(parent_children_key, parent_children_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to update parent children: {}", e)))?;

        Ok(inode)
    }

    /// Create a new file
    async fn create_file_internal(
        &self,
        parent_inode: i64,
        name: String,
    ) -> Result<i64, MetaError> {
        if self.get_access_meta(parent_inode).await?.is_none() {
            return Err(MetaError::ParentNotFound(parent_inode));
        }

        if let Some(contents) = self.get_content_meta(parent_inode).await? {
            for content in contents {
                if content.entry_name == name {
                    return Err(MetaError::AlreadyExists {
                        parent: parent_inode,
                        name,
                    });
                }
            }
        }

        let inode = self.generate_id().await?;
        let mut client = self.client.clone();

        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let file_permission = Permission::new(0o644, 0, 0);
        let entry_info = EtcdEntryInfo {
            is_file: true,
            size: Some(0),
            version: Some(0),
            permission: file_permission,
            access_time: now,
            modify_time: now,
            create_time: now,
            nlink: 1,
        };

        let forward_key = Self::etcd_forward_key(parent_inode, &name);
        let forward_entry = EtcdForwardEntry {
            parent_inode,
            name: name.clone(),
            inode,
            is_file: true,
        };
        let forward_json = serde_json::to_string(&forward_entry)?;

        let reverse_key = Self::etcd_reverse_key(inode);
        let reverse_json = serde_json::to_string(&entry_info)?;

        let parent_children_key = Self::etcd_children_key(parent_inode);
        let parent_children = match client.get(parent_children_key.clone(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let mut children: EtcdDirChildren = serde_json::from_slice(kv.value())?;
                    children.children.insert(name.clone());
                    children
                } else {
                    let mut children = EtcdDirChildren {
                        inode: parent_inode,
                        children: HashSet::new(),
                    };
                    children.children.insert(name.clone());
                    children
                }
            }
            Err(e) => {
                error!("Failed to get parent directory children: {}", e);
                return Err(MetaError::Config(format!(
                    "Failed to get parent directory children: {}",
                    e
                )));
            }
        };
        let parent_children_json = serde_json::to_string(&parent_children)?;

        client
            .put(forward_key, forward_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to create forward index: {}", e)))?;
        client
            .put(reverse_key, reverse_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to create reverse index: {}", e)))?;
        client
            .put(parent_children_key, parent_children_json, None)
            .await
            .map_err(|e| MetaError::Config(format!("Failed to update parent children: {}", e)))?;

        Ok(inode)
    }

    /// Generate unique ID using the injected ID generator
    async fn generate_id(&self) -> Result<i64, MetaError> {
        self.id_gen.next_id().await
    }
}

#[async_trait]
impl MetaStore for EtcdMetaStore {
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
                blocks: 8,
                blksize: 4096,
                rdev: 0,
                version: 0,
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
            let version = entry_info.version.unwrap_or(0) as u64;

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
                    blocks: (entry_info.size.unwrap_or(0) as u64 + 511) / 512,
                    blksize: 4096,
                    rdev: 0,
                    version,
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
                    blocks: 8,
                    blksize: 4096,
                    rdev: 0,
                    version,
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

        // 3. Allocate new inode using ID generator
        let ino = Inode(self.generate_id().await?);
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
            blocks: if is_file { 0 } else { 8 },
            blksize: 4096,
            rdev: 0,
            version: 1, // Initial version
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
            return Err(MetaError::Internal(
                "Cannot unlink directory, use rmdir".into(),
            ));
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
