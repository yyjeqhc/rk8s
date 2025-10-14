//! Database-based metadata store implementation
//!
//! Supports SQLite and PostgreSQL backends via SeaORM

//! DatabaseMetaStore implementation of MetaStoreV2 trait
//!
//! This module provides the V2 interface implementation for DatabaseMetaStore,
//! using strong-typed Inode and structured parameters.

use crate::meta::Permission;
use crate::meta::entities::*;
use crate::meta::error::MetaErrorHelper;
use crate::meta::id_generator::{IdGenerator, PostgresIdGenerator, SqliteIdGenerator};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::vfs::fs::FileType;
use chrono::Utc;
use sea_orm::*;

use crate::meta::config::{Config, DatabaseType};
use crate::meta::store::{DirEntry, FileAttr, MetaError, MetaStore};
use async_trait::async_trait;
use log::info;
use std::path::Path;
use std::sync::Arc;

/// Database-based metadata store (stateless)
pub struct DatabaseMetaStore {
    pub(crate) db: DatabaseConnection,
    pub(crate) _config: Config,
    pub(crate) id_gen: Arc<dyn IdGenerator>,
}

impl DatabaseMetaStore {
    /// Create or open a database metadata store
    #[allow(dead_code)]
    pub async fn new(backend_path: &Path) -> Result<Self, MetaError> {
        let _config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;

        info!("Initializing DatabaseMetaStore");
        info!("Backend path: {}", backend_path.display());
        info!("Database type: {}", _config.database.db_type_str());

        let db = Self::create_connection(&_config).await?;
        Self::init_schema(&db).await?;

        // Create appropriate ID generator based on database type
        let id_gen: Arc<dyn IdGenerator> = match &_config.database.db_config {
            DatabaseType::Sqlite { .. } => {
                let generator = SqliteIdGenerator::new(db.clone());
                generator.initialize().await?;
                Arc::new(generator)
            }
            DatabaseType::Postgres { .. } => {
                let generator = PostgresIdGenerator::new(db.clone());
                generator.initialize().await?;
                Arc::new(generator)
            }
            DatabaseType::Etcd { .. } => {
                return Err(MetaError::Config(
                    "Etcd backend not supported by DatabaseMetaStore".to_string(),
                ));
            }
        };

        let store = Self {
            db,
            _config,
            id_gen,
        };
        store.init_root_directory().await?;

        info!("DatabaseMetaStore initialized successfully");
        Ok(store)
    }

    /// Create from existing config
    pub async fn from_config(_config: Config) -> Result<Self, MetaError> {
        info!("Initializing DatabaseMetaStore from config");
        info!("Database type: {}", _config.database.db_type_str());

        let db = Self::create_connection(&_config).await?;
        Self::init_schema(&db).await?;

        // Create appropriate ID generator based on database type
        let id_gen: Arc<dyn IdGenerator> = match &_config.database.db_config {
            DatabaseType::Sqlite { .. } => {
                let generator = SqliteIdGenerator::new(db.clone());
                generator.initialize().await?;
                Arc::new(generator)
            }
            DatabaseType::Postgres { .. } => {
                let generator = PostgresIdGenerator::new(db.clone());
                generator.initialize().await?;
                Arc::new(generator)
            }
            DatabaseType::Etcd { .. } => {
                return Err(MetaError::Config(
                    "Etcd backend not supported by DatabaseMetaStore".to_string(),
                ));
            }
        };

        let store = Self {
            db,
            _config,
            id_gen,
        };
        store.init_root_directory().await?;

        info!("DatabaseMetaStore initialized successfully");
        Ok(store)
    }

    /// Create database connection
    async fn create_connection(config: &Config) -> Result<DatabaseConnection, MetaError> {
        match &config.database.db_config {
            DatabaseType::Sqlite { url } => {
                info!("Connecting to SQLite: {}", url);
                
                // For SQLite, use serialized access with proper settings
                let mut opts = ConnectOptions::new(url.clone());
                opts.max_connections(10)  // Allow multiple connections for better concurrency
                    .min_connections(2)
                    .connect_timeout(std::time::Duration::from_secs(30))
                    .acquire_timeout(std::time::Duration::from_secs(60))  // Longer acquire timeout
                    .idle_timeout(std::time::Duration::from_secs(600))
                    .max_lifetime(std::time::Duration::from_secs(1800))
                    .sqlx_logging(false);  // Disable verbose logging
                
                let db = Database::connect(opts).await?;
                
                // Enable WAL mode for better concurrency
                db.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "PRAGMA journal_mode=WAL;".to_string(),
                ))
                .await
                .map_err(MetaError::Database)?;
                
                // Set busy timeout to 10 seconds
                db.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "PRAGMA busy_timeout=10000;".to_string(),
                ))
                .await
                .map_err(MetaError::Database)?;
                
                // Enable synchronous=NORMAL for better performance with WAL
                db.execute(Statement::from_string(
                    DatabaseBackend::Sqlite,
                    "PRAGMA synchronous=NORMAL;".to_string(),
                ))
                .await
                .map_err(MetaError::Database)?;
                
                Ok(db)
            }
            DatabaseType::Postgres { url } => {
                info!("Connecting to PostgreSQL: {}", url);
                let mut opts = ConnectOptions::new(url.clone());
                opts.max_connections(100)
                    .min_connections(5)
                    .connect_timeout(std::time::Duration::from_secs(30))
                    .acquire_timeout(std::time::Duration::from_secs(30))
                    .idle_timeout(std::time::Duration::from_secs(600))
                    .max_lifetime(std::time::Duration::from_secs(1800))
                    .sqlx_logging(false);
                let db = Database::connect(opts).await?;
                Ok(db)
            }
            DatabaseType::Etcd { .. } => Err(MetaError::Config(
                "Etcd backend not supported by DatabaseMetaStore. Use EtcdMetaStore instead."
                    .to_string(),
            )),
        }
    }

    /// Initialize database schema
    async fn init_schema(db: &DatabaseConnection) -> Result<(), MetaError> {
        let builder = db.get_database_backend();
        let schema = Schema::new(builder);

        let stmts = vec![
            schema
                .create_table_from_entity(AccessMeta)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(ContentMeta)
                .if_not_exists()
                .to_owned(),
            schema
                .create_table_from_entity(FileMeta)
                .if_not_exists()
                .to_owned(),
        ];

        for (i, stmt) in stmts.iter().enumerate() {
            let sql = builder.build(stmt);
            db.execute(sql).await.map_err(|e| {
                eprintln!("Failed to execute statement {}: {}", i + 1, e);
                MetaError::Database(e)
            })?;
        }

        info!("Database schema initialized successfully");
        Ok(())
    }

    /// Initialize root directory
    async fn init_root_directory(&self) -> Result<(), MetaError> {
        // Check if root directory exists
        if (self.get_access_meta(1).await?).is_some() {
            return Ok(());
        }

        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let root_permission = Permission::new(0o40755, 0, 0); // 目录权限：0o40000 (目录标志) + 0o755 (权限)
        let root_dir = access_meta::ActiveModel {
            inode: Set(1),
            permission: Set(root_permission),
            access_time: Set(now),
            modify_time: Set(now),
            create_time: Set(now),
            nlink: Set(2),
        };

        root_dir
            .insert(&self.db)
            .await
            .map_err(MetaError::Database)?;
        info!("Root directory initialized");

        Ok(())
    }

    /// Get directory access metadata
    async fn get_access_meta(&self, inode: i64) -> Result<Option<AccessMetaModel>, MetaError> {
        AccessMeta::find_by_id(inode)
            .one(&self.db)
            .await
            .map_err(|e| MetaError::Internal(format!("Database error: {}", e)))
    }

    /// Get directory content metadata
    async fn get_content_meta(
        &self,
        parent_inode: i64,
    ) -> Result<Option<Vec<ContentMetaModel>>, MetaError> {
        let contents = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(parent_inode))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        if contents.is_empty() {
            Ok(None)
        } else {
            Ok(Some(contents))
        }
    }

    /// Get file metadata
    async fn get_file_meta(&self, inode: i64) -> Result<Option<FileMetaModel>, MetaError> {
        FileMeta::find_by_id(inode)
            .one(&self.db)
            .await
            .map_err(MetaError::Database)
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

        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let dir_permission = Permission::new(0o40755, 0, 0); // 目录权限：0o40000 (目录标志) + 0o755 (权限)
        let access_meta = access_meta::ActiveModel {
            inode: Set(inode),
            permission: Set(dir_permission),
            access_time: Set(now),
            modify_time: Set(now),
            create_time: Set(now),
            nlink: Set(2),
        };

        access_meta
            .insert(&self.db)
            .await
            .map_err(MetaError::Database)?;

        let content_meta = content_meta::ActiveModel {
            inode: Set(inode),
            parent_inode: Set(parent_inode),
            entry_name: Set(name),
            entry_type: Set(EntryType::Directory),
        };

        content_meta
            .insert(&self.db)
            .await
            .map_err(MetaError::Database)?;

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

        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);
        let file_permission = Permission::new(0o644, 0, 0);
        let file_meta = file_meta::ActiveModel {
            inode: Set(inode),
            size: Set(0),
            permission: Set(file_permission),
            access_time: Set(now),
            modify_time: Set(now),
            create_time: Set(now),
            nlink: Set(1),
        };

        file_meta
            .insert(&self.db)
            .await
            .map_err(MetaError::Database)?;

        let content_meta = content_meta::ActiveModel {
            inode: Set(inode),
            parent_inode: Set(parent_inode),
            entry_name: Set(name),
            entry_type: Set(EntryType::File),
        };

        content_meta
            .insert(&self.db)
            .await
            .map_err(MetaError::Database)?;

        Ok(inode)
    }

    /// Generate unique ID using the injected ID generator
    async fn generate_id(&self) -> Result<i64, MetaError> {
        self.id_gen.next_id().await
    }
}

#[async_trait]
impl MetaStore for DatabaseMetaStore {
    #[tracing::instrument(skip(self), fields(ino = %ino.as_i64()))]
    async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError> {
        // Try file_meta first
        if let Some(file) = FileMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
        {
            let permission = file.permission();
            tracing::debug!(ino = ino.as_i64(), size = file.size, kind = "file", "getattr found file");
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
                version: 0, // TODO: Implement versioning in database schema
            });
        }

        // Try access_meta (directory)
        if let Some(dir) = AccessMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
        {
            let permission = dir.permission();
            tracing::debug!(ino = ino.as_i64(), kind = "directory", "getattr found directory");
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
                version: 0, // TODO: Implement versioning in database schema
            });
        }

        tracing::warn!(ino = ino.as_i64(), "getattr: inode not found");
        Err(MetaError::not_found(ino))
    }

    #[tracing::instrument(skip(self), fields(count = inos.len()))]
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

        tracing::debug!(files_found = files.len(), "batch query: files");
        
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
                version: 0, // TODO: Implement versioning
            };
            results.push((Inode(file.inode), attr));
        }

        // Batch query directories
        let dirs = AccessMeta::find()
            .filter(access_meta::Column::Inode.is_in(ino_i64s))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        tracing::debug!(dirs_found = dirs.len(), "batch query: directories");

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
                version: 0, // TODO: Implement versioning
            };
            results.push((Inode(dir.inode), attr));
        }

        tracing::info!(
            requested = inos.len(),
            found = results.len(),
            "getattr_batch completed"
        );


        Ok(results)
    }

    #[tracing::instrument(skip(self), fields(parent = %parent.as_i64(), name = %name))]
    async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError> {
        let content = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(parent.as_i64()))
            .filter(content_meta::Column::EntryName.eq(name))
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
            .ok_or_else(|| {
                tracing::debug!(parent = parent.as_i64(), name = %name, "lookup: entry not found");
                MetaError::not_found(parent)
            })?;

        tracing::debug!(parent = parent.as_i64(), name = %name, ino = content.inode, "lookup: found");
        Ok(Inode(content.inode))
    }

    #[tracing::instrument(skip(self), fields(ino = %ino.as_i64()))]
    async fn readdir(&self, ino: Inode) -> Result<Vec<DirEntry>, MetaError> {
        // Verify it's a directory
        if AccessMeta::find_by_id(ino.as_i64())
            .one(&self.db)
            .await
            .map_err(MetaError::Database)?
            .is_none()
        {
            tracing::warn!(ino = ino.as_i64(), "readdir: not a directory");
            return Err(MetaError::not_directory(ino));
        }

        let contents = ContentMeta::find()
            .filter(content_meta::Column::ParentInode.eq(ino.as_i64()))
            .all(&self.db)
            .await
            .map_err(MetaError::Database)?;

        tracing::debug!(ino = ino.as_i64(), entries = contents.len(), "readdir: completed");
        
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
        let attr_map: std::collections::HashMap<i64, FileAttr> = attrs
            .into_iter()
            .map(|(ino, attr)| (ino.as_i64(), attr))
            .collect();

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

    #[tracing::instrument(skip(self), fields(
        parent = %params.parent.as_i64(),
        name = %params.name,
        kind = ?params.kind,
        mode = format!("{:o}", params.mode)
    ))]
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
            tracing::warn!(parent = params.parent.as_i64(), "create: parent not found");
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
            tracing::warn!(parent = params.parent.as_i64(), name = %params.name, "create: already exists");
            return Err(MetaError::already_exists(params.parent, params.name));
        }

        // 3. Allocate new inode
        let ino = Inode(self.generate_id().await?);
        let now = Utc::now().timestamp_nanos_opt().unwrap_or(0);

        tracing::debug!(ino = ino.as_i64(), "create: allocated new inode");

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
            entry_name: Set(params.name.clone()),
            entry_type: Set(match params.kind {
                FileType::File => EntryType::File,
                FileType::Dir => EntryType::Directory,
            }),
        };
        content.insert(&txn).await.map_err(MetaError::Database)?;

        // 6. Commit transaction
        txn.commit().await.map_err(MetaError::Database)?;

        tracing::info!(
            ino = ino.as_i64(),
            parent = params.parent.as_i64(),
            name = %params.name,
            kind = ?params.kind,
            "create: completed"
        );

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
        new_content
            .insert(&txn)
            .await
            .map_err(MetaError::Database)?;

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
            return Err(MetaError::Internal(
                "Cannot unlink directory, use rmdir".into(),
            ));
        }

        // 3. Delete directory entry
        let entry_model: content_meta::ActiveModel = entry.clone().into();
        entry_model
            .delete(&txn)
            .await
            .map_err(MetaError::Database)?;

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
        entry_model
            .delete(&txn)
            .await
            .map_err(MetaError::Database)?;

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
