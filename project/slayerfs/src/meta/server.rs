use std::sync::Arc;
use tonic::{Request, Response, Status};
use tracing::{debug, error, instrument};

use crate::meta::MetaStore;
use crate::meta::store::{DirEntry, FileAttr};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::vfs::fs::FileType;

// Generated proto code
pub mod proto {
    tonic::include_proto!("slayerfs.meta");
}

use proto::{
    DirEntry as ProtoDirEntry, FileAttr as ProtoFileAttr,
    meta_service_server::{MetaService, MetaServiceServer},
    *,
};

/// gRPC MetaServer 实现
///
/// 这是一个无状态的 gRPC 服务，将请求转发到底层的 MetaStore
/// 可以是 DatabaseMetaStore 或 EtcdMetaStore
pub struct MetaServer {
    store: Arc<dyn MetaStore>,
}

impl MetaServer {
    pub fn new(store: Arc<dyn MetaStore>) -> Self {
        Self { store }
    }

    pub fn into_service(self) -> MetaServiceServer<Self> {
        MetaServiceServer::new(self)
    }
}

#[tonic::async_trait]
impl MetaService for MetaServer {
    #[instrument(skip(self))]
    async fn initialize(
        &self,
        _request: Request<InitializeRequest>,
    ) -> Result<Response<InitializeResponse>, Status> {
        debug!("Initializing metadata store");

        self.store.initialize().await.map_err(|e| {
            error!("Failed to initialize: {}", e);
            Status::internal(format!("Initialize failed: {}", e))
        })?;

        Ok(Response::new(InitializeResponse { success: true }))
    }

    #[instrument(skip(self))]
    async fn get_root_ino(
        &self,
        _request: Request<GetRootInoRequest>,
    ) -> Result<Response<GetRootInoResponse>, Status> {
        debug!("Getting root inode");

        let root_ino = self.store.root_ino();

        Ok(Response::new(GetRootInoResponse {
            ino: root_ino.as_i64(),
        }))
    }

    #[instrument(skip(self), fields(ino = %request.get_ref().ino))]
    async fn get_attr(
        &self,
        request: Request<GetAttrRequest>,
    ) -> Result<Response<GetAttrResponse>, Status> {
        let ino = request.into_inner().ino;
        debug!("Getting attributes for inode {}", ino);

        let attr = self.store.getattr(Inode(ino as i64)).await.map_err(|e| {
            error!("Failed to get attr for {}: {}", ino, e);
            Status::not_found(format!("Inode {} not found: {}", ino, e))
        })?;

        Ok(Response::new(GetAttrResponse {
            attr: Some(convert_attr_to_proto(&attr)),
        }))
    }

    #[instrument(skip(self), fields(count = %request.get_ref().inos.len()))]
    async fn get_attr_batch(
        &self,
        request: Request<GetAttrBatchRequest>,
    ) -> Result<Response<GetAttrBatchResponse>, Status> {
        let inos = request.into_inner().inos;
        debug!("Getting attributes for {} inodes", inos.len());

        let ino_list: Vec<Inode> = inos.iter().map(|&i| Inode(i as i64)).collect();

        let attrs = self.store.getattr_batch(&ino_list).await.map_err(|e| {
            error!("Failed to get attrs batch: {}", e);
            Status::internal(format!("Batch getattr failed: {}", e))
        })?;

        let proto_attrs = attrs
            .iter()
            .map(|(ino, attr)| AttrPair {
                ino: ino.as_i64(),
                attr: Some(convert_attr_to_proto(attr)),
            })
            .collect();

        Ok(Response::new(GetAttrBatchResponse { attrs: proto_attrs }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().parent, name = %request.get_ref().name))]
    async fn lookup(
        &self,
        request: Request<LookupRequest>,
    ) -> Result<Response<LookupResponse>, Status> {
        let req = request.into_inner();
        debug!("Looking up {} in parent {}", req.name, req.parent);

        let ino = self
            .store
            .lookup(Inode(req.parent as i64), &req.name)
            .await
            .map_err(|e| {
                error!("Lookup failed for {}/{}: {}", req.parent, req.name, e);
                Status::not_found(format!("Lookup failed: {}", e))
            })?;

        Ok(Response::new(LookupResponse { ino: ino.as_i64() }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().ino))]
    async fn readdir(
        &self,
        request: Request<ReaddirRequest>,
    ) -> Result<Response<ReaddirResponse>, Status> {
        let req = request.into_inner();
        debug!("Reading directory {}", req.ino);

        let entries = self
            .store
            .readdir(Inode(req.ino as i64))
            .await
            .map_err(|e| {
                error!("Readdir failed for {}: {}", req.ino, e);
                Status::internal(format!("Readdir failed: {}", e))
            })?;

        let proto_entries = entries.iter().map(convert_entry_to_proto).collect();

        Ok(Response::new(ReaddirResponse {
            entries: proto_entries,
        }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().ino))]
    async fn readdir_plus(
        &self,
        request: Request<ReaddirPlusRequest>,
    ) -> Result<Response<ReaddirPlusResponse>, Status> {
        let req = request.into_inner();
        debug!("Reading directory+ {}", req.ino);

        let entries = self
            .store
            .readdirplus(Inode(req.ino as i64))
            .await
            .map_err(|e| {
                error!("ReaddirPlus failed for {}: {}", req.ino, e);
                Status::internal(format!("ReaddirPlus failed: {}", e))
            })?;

        let proto_entries = entries
            .iter()
            .map(|(entry, attr)| DirEntryPlus {
                entry: Some(convert_entry_to_proto(entry)),
                attr: Some(convert_attr_to_proto(attr)),
            })
            .collect();

        Ok(Response::new(ReaddirPlusResponse {
            entries: proto_entries,
        }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().parent, name = %request.get_ref().name))]
    async fn create(
        &self,
        request: Request<CreateRequest>,
    ) -> Result<Response<CreateResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "Creating {} in parent {} with mode {:o}",
            req.name, req.parent, req.mode
        );

        let params = CreateParams {
            parent: Inode(req.parent as i64),
            name: req.name,
            mode: req.mode,
            uid: req.uid,
            gid: req.gid,
            kind: convert_file_type_from_proto(req.kind),
        };

        let (ino, attr) = self.store.create(params).await.map_err(|e| {
            error!("Create failed: {}", e);
            Status::internal(format!("Create failed: {}", e))
        })?;

        Ok(Response::new(CreateResponse {
            ino: ino.as_i64(),
            attr: Some(convert_attr_to_proto(&attr)),
        }))
    }

    #[instrument(skip(self), fields(ino = %request.get_ref().ino))]
    async fn set_attr(
        &self,
        request: Request<SetAttrRequest>,
    ) -> Result<Response<SetAttrResponse>, Status> {
        let req = request.into_inner();
        debug!("Setting attributes for inode {}", req.ino);

        let mask = SetAttrMask {
            mode: req.mode,
            uid: req.uid,
            gid: req.gid,
            size: req.size,
            atime: req.atime,
            mtime: req.mtime,
        };

        let attr = self
            .store
            .setattr(Inode(req.ino as i64), mask)
            .await
            .map_err(|e| {
                error!("SetAttr failed for {}: {}", req.ino, e);
                Status::internal(format!("SetAttr failed: {}", e))
            })?;

        Ok(Response::new(SetAttrResponse {
            attr: Some(convert_attr_to_proto(&attr)),
        }))
    }

    #[instrument(skip(self), fields(old_parent = %request.get_ref().old_parent, old_name = %request.get_ref().old_name, new_parent = %request.get_ref().new_parent, new_name = %request.get_ref().new_name))]
    async fn rename(
        &self,
        request: Request<RenameRequest>,
    ) -> Result<Response<RenameResponse>, Status> {
        let req = request.into_inner();
        debug!(
            "Renaming {}/{} to {}/{}",
            req.old_parent, req.old_name, req.new_parent, req.new_name
        );

        self.store
            .rename(
                Inode(req.old_parent as i64),
                &req.old_name,
                Inode(req.new_parent as i64),
                req.new_name,
            )
            .await
            .map_err(|e| {
                error!("Rename failed: {}", e);
                Status::internal(format!("Rename failed: {}", e))
            })?;

        Ok(Response::new(RenameResponse { success: true }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().parent, name = %request.get_ref().name))]
    async fn unlink(
        &self,
        request: Request<UnlinkRequest>,
    ) -> Result<Response<UnlinkResponse>, Status> {
        let req = request.into_inner();
        debug!("Unlinking {}/{}", req.parent, req.name);

        self.store
            .unlink(Inode(req.parent as i64), &req.name)
            .await
            .map_err(|e| {
                error!("Unlink failed for {}/{}: {}", req.parent, req.name, e);
                Status::internal(format!("Unlink failed: {}", e))
            })?;

        Ok(Response::new(UnlinkResponse { success: true }))
    }

    #[instrument(skip(self), fields(parent = %request.get_ref().parent, name = %request.get_ref().name))]
    async fn rmdir(
        &self,
        request: Request<RmdirRequest>,
    ) -> Result<Response<RmdirResponse>, Status> {
        let req = request.into_inner();
        debug!("Removing directory {}/{}", req.parent, req.name);

        self.store
            .rmdir(Inode(req.parent as i64), &req.name)
            .await
            .map_err(|e| {
                error!("Rmdir failed for {}/{}: {}", req.parent, req.name, e);
                Status::internal(format!("Rmdir failed: {}", e))
            })?;

        Ok(Response::new(RmdirResponse { success: true }))
    }
}

// 类型转换辅助函数

fn convert_attr_to_proto(attr: &FileAttr) -> ProtoFileAttr {
    ProtoFileAttr {
        ino: attr.ino,
        size: attr.size,
        blocks: attr.blocks,
        atime: attr.atime,
        mtime: attr.mtime,
        ctime: attr.ctime,
        mode: attr.mode,
        nlink: attr.nlink,
        uid: attr.uid,
        gid: attr.gid,
        rdev: attr.rdev,
        blksize: attr.blksize,
        kind: convert_file_type_to_proto(attr.kind),
        version: attr.version,
    }
}

fn convert_entry_to_proto(entry: &DirEntry) -> ProtoDirEntry {
    ProtoDirEntry {
        ino: entry.ino,
        name: entry.name.clone(),
        kind: convert_file_type_to_proto(entry.kind),
    }
}

fn convert_file_type_to_proto(kind: FileType) -> i32 {
    match kind {
        FileType::File => 1,
        FileType::Dir => 2,
    }
}

fn convert_file_type_from_proto(file_type: i32) -> FileType {
    match file_type {
        2 => FileType::Dir,
        _ => FileType::File, // 默认为文件类型
    }
}
