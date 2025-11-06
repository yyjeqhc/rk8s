//! Etcd-based metadata store implementation
//!
//! Uses Etcd/etcd as the backend for metadata storage

use crate::meta::Permission;
use crate::meta::config::{Config, DatabaseType};
use crate::meta::entities::etcd::*;
use crate::meta::entities::*;
use crate::meta::store::{DirEntry, FileAttr, MetaError, MetaStore, TransactionOps};
use crate::vfs::fs::FileType;
use async_trait::async_trait;
use chrono::Utc;
use etcd_client::{Client as EtcdClient, Compare, CompareOp, Txn, TxnOp};
use log::{debug, error, info, warn};
use serde::de::DeserializeOwned;
use serde_json;
use std::collections::HashSet;
use std::path::Path;

/// Etcd-based metadata store with distributed locking
///
/// # Concurrency Model
///
/// Uses etcd transactions (CAS - Compare-And-Swap) to ensure atomic operations
/// across multiple clients in a distributed environment.
///
/// - **create_file/mkdir**: Atomic existence check + multi-key creation
/// - **unlink/rmdir**: Atomic existence check + multi-key deletion  
/// - **rename**: Atomic source exists + target doesn't exist + update
/// - **parent_children update**: CAS with mod_revision retry loop
///
/// # Key Prefixes
///
/// - `f:{parent}:{name}` - Forward index: (parent, name) → inode
/// - `r:{inode}` - Reverse index: inode → metadata
/// - `c:{inode}` - Children index: inode → children set
/// - `slayerfs:next_inode_id` - Global inode counter
pub struct EtcdMetaStore {
    client: EtcdClient,
    _config: Config,
}
#[allow(dead_code)]
impl EtcdMetaStore {
    /// Etcd helper method: generate forward index key (parent_inode, name)
    fn etcd_forward_key(parent_inode: i64, name: &str) -> String {
        format!("f:{}:{}", parent_inode, name)
    }

    /// Etcd helper method: generate reverse index key for inode
    fn etcd_reverse_key(ino: i64) -> String {
        format!("r:{}", ino)
    }

    /// Etcd helper method: generate directory children key
    fn etcd_children_key(inode: i64) -> String {
        format!("c:{}", inode)
    }

    /// Create or open an etcd metadata store
    pub async fn new(backend_path: &Path) -> Result<Self, MetaError> {
        let _config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;

        info!("Initializing EtcdMetaStore");
        info!("Backend path: {}", backend_path.display());

        let client = Self::create_client(&_config).await?;
        let store = Self { client, _config };
        store.init_root_directory().await?;

        info!("EtcdMetaStore initialized successfully");
        Ok(store)
    }

    /// Create from existing config
    pub async fn from_config(_config: Config) -> Result<Self, MetaError> {
        info!("Initializing EtcdMetaStore from config");

        let client = Self::create_client(&_config).await?;
        let store = Self { client, _config };
        store.init_root_directory().await?;

        info!("EtcdMetaStore initialized successfully");
        Ok(store)
    }

    /// Create etcd client
    async fn create_client(config: &Config) -> Result<EtcdClient, MetaError> {
        match &config.database.db_config {
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

    /// Helper: get key from etcd and deserialize JSON into T.
    ///
    /// Strict variant: returns Err(MetaError::Internal) when etcd client returns error.
    async fn etcd_get_json<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, MetaError> {
        let mut client = self.client.clone();
        match client.get(key.to_string(), None).await {
            Ok(resp) => {
                if let Some(kv) = resp.kvs().first() {
                    let obj: T = serde_json::from_slice(kv.value()).map_err(|e| {
                        MetaError::Internal(format!("Failed to parse {}: {}", key, e))
                    })?;
                    Ok(Some(obj))
                } else {
                    Ok(None)
                }
            }
            Err(e) => Err(MetaError::Internal(format!(
                "Failed to get key {}: {}",
                key, e
            ))),
        }
    }

    /// Lenient variant: on etcd client error, log and return Ok(None).
    async fn etcd_get_json_lenient<T: DeserializeOwned>(
        &self,
        key: &str,
    ) -> Result<Option<T>, MetaError> {
        match self.etcd_get_json::<T>(key).await {
            Ok(v) => Ok(v),
            Err(e) => {
                error!("Etcd get failed for {}: {}", key, e);
                Ok(None)
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
        // lenient: if etcd client fails, treat as not found (caller expects Option)
        if let Ok(Some(entry_info)) = self
            .etcd_get_json_lenient::<EtcdEntryInfo>(&reverse_key)
            .await
            && !entry_info.is_file
        {
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
        Ok(None)
    }

    /// Get directory content metadata
    async fn get_content_meta(
        &self,
        parent_inode: i64,
    ) -> Result<Option<Vec<ContentMetaModel>>, MetaError> {
        let children_key = Self::etcd_children_key(parent_inode);
        // strict read of children list
        let dir_children_opt = self
            .etcd_get_json_lenient::<EtcdDirChildren>(&children_key)
            .await?;
        let dir_children = match dir_children_opt {
            Some(dc) => dc,
            None => return Ok(None),
        };

        if dir_children.children.is_empty() {
            return Ok(None);
        }

        let mut content_list = Vec::new();
        for child_name in &dir_children.children {
            let forward_key = Self::etcd_forward_key(parent_inode, child_name);
            if let Ok(Some(forward_entry)) = self
                .etcd_get_json_lenient::<EtcdForwardEntry>(&forward_key)
                .await
            {
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
    }

    /// Get file metadata
    async fn get_file_meta(&self, inode: i64) -> Result<Option<FileMetaModel>, MetaError> {
        let reverse_key = Self::etcd_reverse_key(inode);
        if let Ok(Some(entry_info)) = self
            .etcd_get_json_lenient::<EtcdEntryInfo>(&reverse_key)
            .await
            && entry_info.is_file
        {
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
            return Ok(Some(file_meta));
        }
        Ok(None)
    }

    /// Create a new directory with distributed locking
    ///
    /// # Concurrency Strategy
    ///
    /// Uses etcd transaction to atomically:
    /// 1. Check forward key doesn't exist (f:parent:name)
    /// 2. Create forward index, reverse index, and children index
    /// 3. Update parent's children set with CAS retry
    ///
    /// # Prevents
    ///
    /// - Duplicate directories (concurrent creates with same name)
    /// - Orphaned inodes (partial creation failures)
    async fn create_directory(&self, parent_inode: i64, name: String) -> Result<i64, MetaError> {
        // Step 1: Verify parent exists
        if self.get_access_meta(parent_inode).await?.is_none() {
            return Err(MetaError::ParentNotFound(parent_inode));
        }

        // Step 2: Generate inode (already uses CAS)
        let inode = self.generate_id().await?;

        // Step 3: Prepare all data
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
            parent_inode,
            entry_name: name.clone(),
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

        // Step 4: Atomic transaction - create all keys only if forward key doesn't exist
        info!(
            "Creating directory with transaction: parent={}, name={}, inode={}",
            parent_inode, name, inode
        );

        let operations = vec![
            (forward_key.as_str(), forward_json.as_str()),
            (reverse_key.as_str(), reverse_json.as_str()),
            (children_key.as_str(), children_json.as_str()),
        ];

        // Step 4: Atomic transaction - create all keys only if forward key doesn't exist
        match self
            .atomic_create_with_check(&forward_key, &operations)
            .await
        {
            Ok(()) => {}
            Err(MetaError::AlreadyExists { .. }) => {
                warn!(
                    "Directory creation failed - already exists: parent={}, name={}",
                    parent_inode, name
                );
                return Err(MetaError::AlreadyExists {
                    parent: parent_inode,
                    name,
                });
            }
            Err(e) => return Err(e),
        }

        // Step 5: Update parent's children set with CAS
        let children_key = format!("c:{}", parent_inode);
        let name_clone = name.clone();
        self.update_parent_children_cas(
            &children_key,
            move |children| {
                children.push(name_clone.clone());
            },
            10,
        )
        .await?;

        info!(
            "Directory created successfully: parent={}, name={}, inode={}",
            parent_inode, name, inode
        );

        Ok(inode)
    }

    /// Create a new file with distributed locking
    ///
    /// # Concurrency Strategy
    ///
    /// Uses etcd transaction to atomically:
    /// 1. Check forward key doesn't exist (f:parent:name)
    /// 2. Create forward index and reverse index
    /// 3. Update parent's children set with CAS retry
    ///
    /// # Prevents
    ///
    /// - Duplicate files (concurrent creates with same name)
    /// - Orphaned inodes (partial creation failures)
    async fn create_file_internal(
        &self,
        parent_inode: i64,
        name: String,
    ) -> Result<i64, MetaError> {
        // Step 1: Verify parent exists
        if self.get_access_meta(parent_inode).await?.is_none() {
            return Err(MetaError::ParentNotFound(parent_inode));
        }

        // Step 2: Generate inode (already uses CAS)
        let inode = self.generate_id().await?;

        // Step 3: Prepare all data
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
            parent_inode,
            entry_name: name.clone(),
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

        // Step 4: Atomic transaction - create keys only if forward key doesn't exist
        info!(
            "Creating file with transaction: parent={}, name={}, inode={}",
            parent_inode, name, inode
        );

        let operations = vec![
            (forward_key.as_str(), forward_json.as_str()),
            (reverse_key.as_str(), reverse_json.as_str()),
        ];

        // Step 4: Atomic transaction - create keys only if forward key doesn't exist
        match self
            .atomic_create_with_check(&forward_key, &operations)
            .await
        {
            Ok(()) => {}
            Err(MetaError::AlreadyExists { .. }) => {
                warn!(
                    "File creation failed - already exists: parent={}, name={}",
                    parent_inode, name
                );
                return Err(MetaError::AlreadyExists {
                    parent: parent_inode,
                    name,
                });
            }
            Err(e) => return Err(e),
        }

        // Step 5: Update parent's children set with CAS
        let children_key = format!("c:{}", parent_inode);
        let name_clone = name.clone();
        self.update_parent_children_cas(
            &children_key,
            move |children| {
                children.push(name_clone.clone());
            },
            10,
        )
        .await?;

        info!(
            "File created successfully: parent={}, name={}, inode={}",
            parent_inode, name, inode
        );

        Ok(inode)
    }

    /// Generate unique ID using Etcd atomic counter
    /// Uses compare-and-swap to ensure atomicity in distributed environment
    async fn generate_id(&self) -> Result<i64, MetaError> {
        let mut client = self.client.clone();
        let counter_key = "slayerfs:next_inode_id";

        // Retry loop for CAS operation
        // TODO: think about how to keep in sync with remote
        for retry in 0..10 {
            match client.get(counter_key, None).await {
                Ok(resp) => {
                    let (current_id, mod_revision) = if let Some(kv) = resp.kvs().first() {
                        let id = String::from_utf8_lossy(kv.value())
                            .parse::<i64>()
                            .unwrap_or(1);
                        (id, kv.mod_revision())
                    } else {
                        // First time initialization
                        if let Err(e) = client.put(counter_key, "2", None).await {
                            error!("Failed to initialize ID counter: {}", e);
                            return Err(MetaError::Config(format!(
                                "Failed to initialize ID counter: {}",
                                e
                            )));
                        }
                        return Ok(2);
                    };

                    let next_id = current_id + 1;

                    // Use transaction for atomic compare-and-swap
                    use etcd_client::{Compare, CompareOp, Txn, TxnOp};

                    let cmp = Compare::mod_revision(counter_key, CompareOp::Equal, mod_revision);
                    let put_op = TxnOp::put(counter_key, next_id.to_string(), None);
                    let txn = Txn::new().when([cmp]).and_then([put_op]);

                    match client.txn(txn).await {
                        Ok(txn_resp) => {
                            if txn_resp.succeeded() {
                                // CAS succeeded, return the new ID
                                return Ok(next_id);
                            } else {
                                // CAS failed, retry
                                if retry < 9 {
                                    continue;
                                } else {
                                    return Err(MetaError::Config(
                                        "Failed to generate ID after max retries".to_string(),
                                    ));
                                }
                            }
                        }
                        Err(e) => {
                            error!("Failed to execute transaction: {}", e);
                            return Err(MetaError::Config(format!(
                                "Failed to execute ID generation transaction: {}",
                                e
                            )));
                        }
                    }
                }
                Err(e) => {
                    error!("Failed to get ID counter: {}", e);
                    return Err(MetaError::Config(format!(
                        "Failed to get ID counter: {}",
                        e
                    )));
                }
            }
        }

        Err(MetaError::Config(
            "Failed to generate ID: max retries exceeded".to_string(),
        ))
    }
}

#[async_trait]
impl MetaStore for EtcdMetaStore {
    async fn stat(&self, ino: i64) -> Result<Option<FileAttr>, MetaError> {
        if let Ok(Some(file_meta)) = self.get_file_meta(ino).await {
            let permission = file_meta.permission();
            return Ok(Some(FileAttr {
                ino: file_meta.inode,
                size: file_meta.size as u64,
                kind: FileType::File,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: file_meta.access_time,
                mtime: file_meta.modify_time,
                ctime: file_meta.create_time,
                nlink: file_meta.nlink as u32,
            }));
        }

        if let Ok(Some(access_meta)) = self.get_access_meta(ino).await {
            let permission = access_meta.permission();
            return Ok(Some(FileAttr {
                ino: access_meta.inode,
                size: 4096,
                kind: FileType::Dir,
                mode: permission.mode,
                uid: permission.uid,
                gid: permission.gid,
                atime: access_meta.access_time,
                mtime: access_meta.modify_time,
                ctime: access_meta.create_time,
                nlink: access_meta.nlink as u32,
            }));
        }

        Ok(None)
    }

    async fn lookup(&self, parent: i64, name: &str) -> Result<Option<i64>, MetaError> {
        let contents = match self.get_content_meta(parent).await? {
            Some(contents) => contents,
            None => return Ok(None),
        };

        for content in contents {
            if content.entry_name == name {
                return Ok(Some(content.inode));
            }
        }

        Ok(None)
    }

    async fn lookup_path(&self, path: &str) -> Result<Option<(i64, FileType)>, MetaError> {
        if path == "/" {
            return Ok(Some((1, FileType::Dir)));
        }

        let parts: Vec<&str> = path
            .trim_matches('/')
            .split('/')
            .filter(|p| !p.is_empty())
            .collect();
        let mut current_inode = 1i64;

        for (index, part) in parts.iter().enumerate() {
            let contents = self.get_content_meta(current_inode).await?;

            let found_entry = match contents {
                Some(entries) => entries.into_iter().find(|entry| entry.entry_name == *part),
                None => return Ok(None),
            };

            match found_entry {
                Some(entry) => match entry.entry_type {
                    EntryType::Directory => {
                        current_inode = entry.inode;
                    }
                    EntryType::File => {
                        if index == parts.len() - 1 {
                            return Ok(Some((entry.inode, FileType::File)));
                        } else {
                            return Ok(None);
                        }
                    }
                },
                None => return Ok(None),
            }
        }

        Ok(Some((current_inode, FileType::Dir)))
    }

    async fn readdir(&self, ino: i64) -> Result<Vec<DirEntry>, MetaError> {
        let access_meta = self
            .get_access_meta(ino)
            .await?
            .ok_or(MetaError::NotFound(ino))?;

        let permission = access_meta.permission();
        if !permission.is_directory() {
            return Err(MetaError::NotDirectory(ino));
        }

        let contents = match self.get_content_meta(ino).await? {
            Some(contents) => contents,
            None => return Ok(Vec::new()),
        };

        let mut entries = Vec::new();
        for content in contents {
            let kind = match content.entry_type {
                EntryType::File => FileType::File,
                EntryType::Directory => FileType::Dir,
            };
            entries.push(DirEntry {
                name: content.entry_name,
                ino: content.inode,
                kind,
            });
        }

        Ok(entries)
    }

    async fn mkdir(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_directory(parent, name).await
    }

    async fn rmdir(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        // Step 1: Read forward entry to get child inode
        let forward_key = Self::etcd_forward_key(parent, name);
        let forward_entry: EtcdForwardEntry =
            match self.etcd_get_json::<EtcdForwardEntry>(&forward_key).await? {
                Some(fe) => fe,
                None => return Err(MetaError::NotFound(parent)),
            };

        let child_ino = forward_entry.inode;

        if forward_entry.is_file {
            return Err(MetaError::Internal("Not a directory".to_string()));
        }

        // Step 2: Check directory is empty
        let children_key = Self::etcd_children_key(child_ino);
        if let Some(children) = self.etcd_get_json::<EtcdDirChildren>(&children_key).await?
            && !children.children.is_empty()
        {
            return Err(MetaError::DirectoryNotEmpty(child_ino));
        }

        // Step 3: Atomic delete - check forward key exists, then delete all related keys
        info!(
            "Deleting directory with transaction: parent={}, name={}, inode={}",
            parent, name, child_ino
        );

        let reverse_key = Self::etcd_reverse_key(child_ino);
        let children_key = Self::etcd_children_key(child_ino);
        let delete_keys = vec![
            forward_key.as_str(),
            reverse_key.as_str(),
            children_key.as_str(),
        ];

        // Step 3: Atomic transaction - delete only if forward key exists
        match self
            .atomic_delete_with_check(&forward_key, &delete_keys)
            .await
        {
            Ok(()) => {}
            Err(MetaError::NotFound(_)) => {
                warn!(
                    "Directory deletion failed - not found: parent={}, name={}",
                    parent, name
                );
                return Err(MetaError::NotFound(parent));
            }
            Err(e) => return Err(e),
        }

        // Step 4: Update parent's children set with CAS
        let parent_children_key = format!("c:{}", parent);
        let name_clone = name.to_string();
        self.update_parent_children_cas(
            &parent_children_key,
            move |children| {
                children.retain(|c| c != &name_clone);
            },
            10,
        )
        .await?;

        info!(
            "Directory deleted successfully: parent={}, name={}, inode={}",
            parent, name, child_ino
        );

        Ok(())
    }

    async fn create_file(&self, parent: i64, name: String) -> Result<i64, MetaError> {
        self.create_file_internal(parent, name).await
    }

    async fn unlink(&self, parent: i64, name: &str) -> Result<(), MetaError> {
        // Step 1: Read forward entry to get file inode
        let forward_key = Self::etcd_forward_key(parent, name);
        let forward_entry: EtcdForwardEntry =
            match self.etcd_get_json::<EtcdForwardEntry>(&forward_key).await? {
                Some(fe) => fe,
                None => return Err(MetaError::NotFound(parent)),
            };

        let file_ino = forward_entry.inode;

        if !forward_entry.is_file {
            return Err(MetaError::Internal("Is a directory".to_string()));
        }

        // Step 2: Atomic delete - check forward key exists, then delete all related keys
        info!(
            "Deleting file with transaction: parent={}, name={}, inode={}",
            parent, name, file_ino
        );

        let reverse_key = Self::etcd_reverse_key(file_ino);
        let delete_keys = vec![forward_key.as_str(), reverse_key.as_str()];

        match self
            .atomic_delete_with_check(&forward_key, &delete_keys)
            .await
        {
            Ok(()) => {}
            Err(MetaError::NotFound(_)) => {
                warn!(
                    "File deletion failed - not found: parent={}, name={}",
                    parent, name
                );
                return Err(MetaError::NotFound(parent));
            }
            Err(e) => return Err(e),
        }

        // Step 3: Update parent's children set with CAS
        let parent_children_key = format!("c:{}", parent);
        let name_owned = name.to_string();
        self.update_parent_children_cas(
            &parent_children_key,
            move |children| {
                children.retain(|c| c != &name_owned);
            },
            10,
        )
        .await?;

        info!(
            "File deleted successfully: parent={}, name={}, inode={}",
            parent, name, file_ino
        );

        Ok(())
    }

    async fn rename(
        &self,
        old_parent: i64,
        old_name: &str,
        new_parent: i64,
        new_name: String,
    ) -> Result<(), MetaError> {
        // Step 1: Read entry information
        let old_forward_key = Self::etcd_forward_key(old_parent, old_name);
        let forward_entry = self
            .etcd_get_json::<EtcdForwardEntry>(&old_forward_key)
            .await?
            .ok_or(MetaError::NotFound(old_parent))?;

        let entry_ino = forward_entry.inode;
        let is_file = forward_entry.is_file;

        // Step 2: Read and update reverse index
        let reverse_key = Self::etcd_reverse_key(entry_ino);
        let mut entry_info = self
            .etcd_get_json::<EtcdEntryInfo>(&reverse_key)
            .await?
            .ok_or(MetaError::NotFound(entry_ino))?;

        entry_info.parent_inode = new_parent;
        entry_info.entry_name = new_name.clone();
        entry_info.modify_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let updated_reverse_json = serde_json::to_string(&entry_info)?;

        // Step 3: Prepare new forward index
        let new_forward_key = Self::etcd_forward_key(new_parent, &new_name);
        let new_forward_entry = EtcdForwardEntry {
            parent_inode: new_parent,
            name: new_name.clone(),
            inode: entry_ino,
            is_file,
        };
        let new_forward_json = serde_json::to_string(&new_forward_entry)?;

        // Step 4: Atomic rename transaction
        info!(
            "Renaming with transaction: {} (parent={}) -> {} (parent={}), inode={}",
            old_name, old_parent, new_name, new_parent, entry_ino
        );

        // Step 4: Atomic rename - old exists AND new doesn't exist
        self.atomic_rename(&old_forward_key, &new_forward_key, "", &new_forward_json)
            .await?;

        // Update reverse index separately
        let mut client = self.client.clone();
        client
            .put(reverse_key, updated_reverse_json, None)
            .await
            .map_err(|e| MetaError::Internal(format!("Failed to update reverse index: {}", e)))?;

        // Step 5: Update old parent's children with CAS
        if old_parent != new_parent || old_name != new_name {
            let old_parent_children_key = format!("c:{}", old_parent);
            let old_name_owned = old_name.to_string();
            self.update_parent_children_cas(
                &old_parent_children_key,
                move |children| {
                    children.retain(|c| c != &old_name_owned);
                },
                10,
            )
            .await?;
        }

        // Step 6: Update new parent's children with CAS
        if old_parent != new_parent {
            let new_parent_children_key = format!("c:{}", new_parent);
            let new_name_clone = new_name.clone();
            self.update_parent_children_cas(
                &new_parent_children_key,
                move |children| {
                    children.push(new_name_clone.clone());
                },
                10,
            )
            .await?;
        } else if old_name != new_name {
            let parent_children_key = format!("c:{}", new_parent);
            let old_name_owned = old_name.to_string();
            let new_name_clone = new_name.clone();
            self.update_parent_children_cas(
                &parent_children_key,
                move |children| {
                    children.retain(|c| c != &old_name_owned);
                    children.push(new_name_clone.clone());
                },
                10,
            )
            .await?;
        }

        info!(
            "Rename completed successfully: {} -> {}, inode={}",
            old_name, new_name, entry_ino
        );

        Ok(())
    }

    async fn set_file_size(&self, ino: i64, size: u64) -> Result<(), MetaError> {
        let reverse_key = Self::etcd_reverse_key(ino);

        let mut entry_info = self
            .etcd_get_json::<EtcdEntryInfo>(&reverse_key)
            .await?
            .ok_or(MetaError::NotFound(ino))?;

        if !entry_info.is_file {
            return Err(MetaError::Internal(
                "Cannot set size for directory".to_string(),
            ));
        }

        entry_info.size = Some(size as i64);
        entry_info.modify_time = chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0);

        let updated_json = serde_json::to_string(&entry_info)
            .map_err(|e| MetaError::Internal(format!("Failed to serialize entry info: {}", e)))?;

        let mut client = self.client.clone();
        client
            .put(reverse_key, updated_json, None)
            .await
            .map_err(|e| {
                MetaError::Internal(format!("Failed to update file size in Etcd: {}", e))
            })?;

        Ok(())
    }

    async fn get_parent(&self, ino: i64) -> Result<Option<i64>, MetaError> {
        if ino == 1 {
            return Ok(None);
        }

        let reverse_key = Self::etcd_reverse_key(ino);
        if let Some(entry_info) = self.etcd_get_json::<EtcdEntryInfo>(&reverse_key).await? {
            Ok(Some(entry_info.parent_inode))
        } else {
            Ok(None)
        }
    }

    async fn get_name(&self, ino: i64) -> Result<Option<String>, MetaError> {
        if ino == 1 {
            return Ok(Some("/".to_string()));
        }

        let reverse_key = Self::etcd_reverse_key(ino);
        if let Some(entry_info) = self.etcd_get_json::<EtcdEntryInfo>(&reverse_key).await? {
            Ok(Some(entry_info.entry_name))
        } else {
            Ok(None)
        }
    }

    async fn get_path(&self, ino: i64) -> Result<Option<String>, MetaError> {
        if ino == 1 {
            return Ok(Some("/".to_string()));
        }

        let mut path_parts = Vec::new();
        let mut current_ino = ino;

        loop {
            let reverse_key = Self::etcd_reverse_key(current_ino);

            let entry_info = match self.etcd_get_json::<EtcdEntryInfo>(&reverse_key).await? {
                Some(info) => info,
                None => return Ok(None),
            };

            path_parts.push(entry_info.entry_name);

            let parent = entry_info.parent_inode;
            if parent == 1 {
                break;
            }

            current_ino = parent;
        }

        path_parts.reverse();
        let path = format!("/{}", path_parts.join("/"));
        Ok(Some(path))
    }

    fn root_ino(&self) -> i64 {
        1
    }

    async fn initialize(&self) -> Result<(), MetaError> {
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl EtcdMetaStore {
    /// Get a clone of the etcd client (for Watch Worker)
    pub fn get_client(&self) -> EtcdClient {
        self.client.clone()
    }
}

#[async_trait]
impl TransactionOps for EtcdMetaStore {
    async fn update_parent_children_cas<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<(), MetaError>
    where
        F: Fn(&mut Vec<String>) + Send + 'static,
    {
        let mut client = self.client.clone();
        for retry in 0..max_retries {
            let resp = client
                .get(key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to get key for CAS: {}", e)))?;

            let (mut children_set, mod_revision) = if let Some(kv) = resp.kvs().first() {
                let children: EtcdDirChildren = serde_json::from_slice(kv.value())
                    .map_err(|e| MetaError::Internal(format!("Failed to parse children: {}", e)))?;
                let children_vec: Vec<String> = children.children.into_iter().collect();
                (children_vec, kv.mod_revision())
            } else {
                (Vec::new(), 0)
            };

            let old_size = children_set.len();
            updater(&mut children_set);
            let new_size = children_set.len();

            debug!(
                "CAS update children for {}: {} -> {} entries (retry {})",
                key, old_size, new_size, retry
            );

            let children_hashset: HashSet<String> = children_set.into_iter().collect();
            let parent_ino = key
                .strip_prefix("c:")
                .and_then(|s| s.parse::<i64>().ok())
                .ok_or_else(|| {
                    MetaError::Internal(format!("Invalid children key format: {}", key))
                })?;

            let new_children = EtcdDirChildren {
                inode: parent_ino,
                children: children_hashset,
            };
            let new_json = serde_json::to_string(&new_children)
                .map_err(|e| MetaError::Internal(format!("Failed to serialize children: {}", e)))?;

            let txn = if mod_revision == 0 {
                Txn::new()
                    .when([Compare::create_revision(key, CompareOp::Equal, 0)])
                    .and_then([TxnOp::put(key, new_json, None)])
            } else {
                Txn::new()
                    .when([Compare::mod_revision(key, CompareOp::Equal, mod_revision)])
                    .and_then([TxnOp::put(key, new_json, None)])
            };

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| MetaError::Internal(format!("CAS transaction failed: {}", e)))?;

            if txn_resp.succeeded() {
                debug!("CAS update succeeded for {}", key);
                return Ok(());
            } else if retry < max_retries - 1 {
                warn!("CAS conflict for {} (retry {}/{})", key, retry, max_retries);
                continue;
            } else {
                return Err(MetaError::Internal(format!(
                    "CAS max retries exceeded for {}",
                    key
                )));
            }
        }
        Err(MetaError::Internal(
            "CAS update failed: unreachable".to_string(),
        ))
    }

    async fn atomic_create_with_check(
        &self,
        check_key: &str,
        entries: &[(&str, &str)],
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        let mut txn = Txn::new().when([Compare::create_revision(check_key, CompareOp::Equal, 0)]);
        let mut ops = Vec::new();
        for (key, value) in entries {
            ops.push(TxnOp::put(*key, *value, None));
        }
        txn = txn.and_then(ops);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic create transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic create succeeded for check_key: {}", check_key);
            Ok(())
        } else {
            Err(MetaError::AlreadyExists {
                parent: 0,
                name: check_key.to_string(),
            })
        }
    }

    async fn atomic_delete_with_check(
        &self,
        check_key: &str,
        keys: &[&str],
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        let mut txn =
            Txn::new().when([Compare::create_revision(check_key, CompareOp::NotEqual, 0)]);
        let mut ops = Vec::new();
        for key in keys {
            ops.push(TxnOp::delete(*key, None));
        }
        txn = txn.and_then(ops);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic delete transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic delete succeeded for check_key: {}", check_key);
            Ok(())
        } else {
            Err(MetaError::NotFound(0))
        }
    }

    async fn atomic_rename(
        &self,
        source_key: &str,
        target_key: &str,
        _source_value: &str,
        target_value: &str,
    ) -> Result<(), MetaError> {
        let mut client = self.client.clone();
        let txn = Txn::new()
            .when([
                Compare::create_revision(source_key, CompareOp::NotEqual, 0),
                Compare::create_revision(target_key, CompareOp::Equal, 0),
            ])
            .and_then([
                TxnOp::put(target_key, target_value, None),
                TxnOp::delete(source_key, None),
            ]);

        let resp = client
            .txn(txn)
            .await
            .map_err(|e| MetaError::Internal(format!("Atomic rename transaction failed: {}", e)))?;

        if resp.succeeded() {
            debug!("Atomic rename succeeded: {} -> {}", source_key, target_key);
            Ok(())
        } else {
            let source_resp = client
                .get(source_key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to check source key: {}", e)))?;

            if source_resp.kvs().is_empty() {
                Err(MetaError::NotFound(0))
            } else {
                Err(MetaError::AlreadyExists {
                    parent: 0,
                    name: target_key.to_string(),
                })
            }
        }
    }

    async fn cas_update<F>(
        &self,
        key: &str,
        updater: F,
        max_retries: usize,
    ) -> Result<i64, MetaError>
    where
        F: Fn(&str, i64) -> Result<String, MetaError> + Send + 'static,
    {
        let mut client = self.client.clone();
        for retry in 0..max_retries {
            let resp = client
                .get(key, None)
                .await
                .map_err(|e| MetaError::Internal(format!("Failed to get key for CAS: {}", e)))?;

            let (current_value, mod_revision) = if let Some(kv) = resp.kvs().first() {
                (
                    String::from_utf8_lossy(kv.value()).to_string(),
                    kv.mod_revision(),
                )
            } else {
                return Err(MetaError::NotFound(0));
            };

            let new_value = updater(&current_value, mod_revision)?;

            let txn = Txn::new()
                .when([Compare::mod_revision(key, CompareOp::Equal, mod_revision)])
                .and_then([TxnOp::put(key, new_value, None)]);

            let txn_resp = client
                .txn(txn)
                .await
                .map_err(|e| MetaError::Internal(format!("CAS transaction failed: {}", e)))?;

            if txn_resp.succeeded() {
                let resp = client.get(key, None).await.map_err(|e| {
                    MetaError::Internal(format!("Failed to get updated key: {}", e))
                })?;

                if let Some(kv) = resp.kvs().first() {
                    debug!("CAS update succeeded for key: {}", key);
                    return Ok(kv.mod_revision());
                } else {
                    return Err(MetaError::Internal(
                        "Key disappeared after CAS update".to_string(),
                    ));
                }
            } else if retry < max_retries - 1 {
                warn!("CAS conflict for {} (retry {}/{})", key, retry, max_retries);
                continue;
            } else {
                return Err(MetaError::Internal(format!(
                    "CAS max retries exceeded for {}",
                    key
                )));
            }
        }
        Err(MetaError::Internal(
            "CAS update failed: unreachable".to_string(),
        ))
    }
}
