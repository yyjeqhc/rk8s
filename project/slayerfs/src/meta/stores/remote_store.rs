use async_trait::async_trait;
use std::time::Duration;
use tonic::transport::{Channel, Uri};
use tracing::{debug, error, instrument};

use crate::meta::MetaStore;
use crate::meta::proto;
use crate::meta::store::{DirEntry, FileAttr, MetaError};
use crate::meta::types::{CreateParams, Inode, SetAttrMask};
use crate::vfs::fs::FileType;

use proto::{
    DirEntry as ProtoDirEntry, FileAttr as ProtoFileAttr, meta_service_client::MetaServiceClient, *,
};

/// gRPC 客户端实现 MetaStore trait
///
/// 这是一个远程元数据存储客户端，通过 gRPC 连接到 MetaServer
pub struct RemoteMetaStore {
    client: MetaServiceClient<Channel>,
    endpoint: String,
}

impl RemoteMetaStore {
    /// 创建新的远程元数据存储客户端
    pub async fn new(endpoint: &str, timeout: Option<Duration>) -> Result<Self, MetaError> {
        debug!("Connecting to MetaServer at {}", endpoint);

        let uri: Uri = endpoint
            .parse()
            .map_err(|e| MetaError::InvalidConfig(format!("Invalid endpoint: {}", e)))?;

        let mut channel = Channel::builder(uri);

        if let Some(timeout_duration) = timeout {
            channel = channel.timeout(timeout_duration);
        }

        let channel = channel
            .connect()
            .await
            .map_err(|e| MetaError::ConnectionError(format!("Failed to connect: {}", e)))?;

        let client = MetaServiceClient::new(channel);

        Ok(Self {
            client,
            endpoint: endpoint.to_string(),
        })
    }

    /// 从配置创建远程元数据存储
    pub async fn from_config(config: &crate::meta::config::Config) -> Result<Self, MetaError> {
        use crate::meta::config::MetadataBackend;

        match &config.metadata.backend {
            MetadataBackend::Grpc {
                endpoint,
                timeout_secs,
                tls: _,
            } => {
                let timeout_duration = if *timeout_secs > 0 {
                    Some(Duration::from_secs(*timeout_secs))
                } else {
                    None
                };
                Self::new(endpoint, timeout_duration).await
            }
            _ => Err(MetaError::InvalidConfig(
                "Expected gRPC backend configuration".to_string(),
            )),
        }
    }
}

#[async_trait]
impl MetaStore for RemoteMetaStore {
    #[instrument(skip(self))]
    async fn initialize(&self) -> Result<(), MetaError> {
        debug!("Initializing remote metadata store");

        let mut client = self.client.clone();
        let request = tonic::Request::new(InitializeRequest {});

        client.initialize(request).await.map_err(|e| {
            error!("Initialize RPC failed: {}", e);
            MetaError::RpcError(format!("Initialize failed: {}", e))
        })?;

        Ok(())
    }

    fn root_ino(&self) -> Inode {
        Inode::ROOT
    }

    #[instrument(skip(self))]
    async fn getattr(&self, ino: Inode) -> Result<FileAttr, MetaError> {
        debug!("Getting attributes for inode {} from remote", ino.as_i64());

        let mut client = self.client.clone();
        let request = tonic::Request::new(GetAttrRequest { ino: ino.as_i64() });

        let response = client.get_attr(request).await.map_err(|e| {
            error!("GetAttr RPC failed for {}: {}", ino.as_i64(), e);
            MetaError::RpcError(format!("GetAttr failed: {}", e))
        })?;

        let proto_attr = response
            .into_inner()
            .attr
            .ok_or_else(|| MetaError::NotFound(ino.as_i64()))?;

        Ok(convert_attr_from_proto(&proto_attr))
    }

    #[instrument(skip(self))]
    async fn getattr_batch(&self, inos: &[Inode]) -> Result<Vec<(Inode, FileAttr)>, MetaError> {
        debug!("Getting attributes for {} inodes from remote", inos.len());

        let mut client = self.client.clone();
        let request = tonic::Request::new(GetAttrBatchRequest {
            inos: inos.iter().map(|i| i.as_i64()).collect(),
        });

        let response = client.get_attr_batch(request).await.map_err(|e| {
            error!("GetAttrBatch RPC failed: {}", e);
            MetaError::RpcError(format!("GetAttrBatch failed: {}", e))
        })?;

        let attrs: Vec<(Inode, FileAttr)> = response
            .into_inner()
            .attrs
            .iter()
            .filter_map(|attr_pair| {
                attr_pair
                    .attr
                    .as_ref()
                    .map(|proto_attr| (Inode(attr_pair.ino), convert_attr_from_proto(proto_attr)))
            })
            .collect();

        Ok(attrs)
    }

    #[instrument(skip(self))]
    async fn lookup(&self, parent: Inode, name: &str) -> Result<Inode, MetaError> {
        debug!(
            "Looking up {} in parent {} on remote",
            name,
            parent.as_i64()
        );

        let mut client = self.client.clone();
        let request = tonic::Request::new(LookupRequest {
            parent: parent.as_i64(),
            name: name.to_string(),
        });

        let response = client.lookup(request).await.map_err(|e| {
            error!("Lookup RPC failed for {}/{}: {}", parent.as_i64(), name, e);
            MetaError::RpcError(format!("Lookup failed: {}", e))
        })?;

        let ino = response.into_inner().ino;
        Ok(Inode(ino))
    }

    #[instrument(skip(self))]
    async fn readdir(&self, parent: Inode) -> Result<Vec<DirEntry>, MetaError> {
        debug!("Reading directory {} from remote", parent.as_i64());

        let mut client = self.client.clone();
        let request = tonic::Request::new(ReaddirRequest {
            ino: parent.as_i64(),
        });

        let response = client.readdir(request).await.map_err(|e| {
            error!("Readdir RPC failed for {}: {}", parent.as_i64(), e);
            MetaError::RpcError(format!("Readdir failed: {}", e))
        })?;

        let entries = response
            .into_inner()
            .entries
            .iter()
            .map(convert_entry_from_proto)
            .collect();

        Ok(entries)
    }

    #[instrument(skip(self))]
    async fn readdirplus(&self, parent: Inode) -> Result<Vec<(DirEntry, FileAttr)>, MetaError> {
        debug!("Reading directory+ {} from remote", parent.as_i64());

        let mut client = self.client.clone();
        let request = tonic::Request::new(ReaddirPlusRequest {
            ino: parent.as_i64(),
        });

        let response = client.readdir_plus(request).await.map_err(|e| {
            error!("ReaddirPlus RPC failed for {}: {}", parent.as_i64(), e);
            MetaError::RpcError(format!("ReaddirPlus failed: {}", e))
        })?;

        let entries = response
            .into_inner()
            .entries
            .iter()
            .filter_map(|e| {
                let entry = e.entry.as_ref()?;
                let attr = e.attr.as_ref()?;
                Some((
                    convert_entry_from_proto(entry),
                    convert_attr_from_proto(attr),
                ))
            })
            .collect();

        Ok(entries)
    }

    #[instrument(skip(self))]
    async fn create(&self, params: CreateParams) -> Result<(Inode, FileAttr), MetaError> {
        debug!(
            "Creating {} in parent {} on remote with mode {:o}",
            params.name,
            params.parent.as_i64(),
            params.mode
        );

        let mut client = self.client.clone();
        let request = tonic::Request::new(CreateRequest {
            parent: params.parent.as_i64(),
            name: params.name,
            mode: params.mode,
            uid: params.uid,
            gid: params.gid,
            rdev: 0, // proto has rdev but CreateParams doesn't, use default
            kind: convert_file_type_to_proto(params.kind),
        });

        let response = client.create(request).await.map_err(|e| {
            error!("Create RPC failed: {}", e);
            MetaError::RpcError(format!("Create failed: {}", e))
        })?;

        let inner = response.into_inner();
        let proto_attr = inner
            .attr
            .ok_or_else(|| MetaError::Internal("Create returned no attributes".to_string()))?;

        let attr = convert_attr_from_proto(&proto_attr);
        Ok((Inode(inner.ino), attr))
    }

    #[instrument(skip(self))]
    async fn setattr(&self, ino: Inode, mask: SetAttrMask) -> Result<FileAttr, MetaError> {
        debug!("Setting attributes for inode {} on remote", ino.as_i64());

        let mut client = self.client.clone();
        let request = tonic::Request::new(SetAttrRequest {
            ino: ino.as_i64(),
            mode: mask.mode,
            uid: mask.uid,
            gid: mask.gid,
            size: mask.size,
            atime: mask.atime,
            mtime: mask.mtime,
        });

        let response = client.set_attr(request).await.map_err(|e| {
            error!("SetAttr RPC failed for {}: {}", ino.as_i64(), e);
            MetaError::RpcError(format!("SetAttr failed: {}", e))
        })?;

        let proto_attr = response
            .into_inner()
            .attr
            .ok_or_else(|| MetaError::Internal("SetAttr returned no attributes".to_string()))?;

        Ok(convert_attr_from_proto(&proto_attr))
    }

    #[instrument(skip(self))]
    async fn rename(
        &self,
        old_parent: Inode,
        old_name: &str,
        new_parent: Inode,
        new_name: String,
    ) -> Result<(), MetaError> {
        debug!(
            "Renaming {}/{} to {}/{} on remote",
            old_parent.as_i64(),
            old_name,
            new_parent.as_i64(),
            new_name
        );

        let mut client = self.client.clone();
        let request = tonic::Request::new(RenameRequest {
            old_parent: old_parent.as_i64(),
            old_name: old_name.to_string(),
            new_parent: new_parent.as_i64(),
            new_name,
        });

        client.rename(request).await.map_err(|e| {
            error!("Rename RPC failed: {}", e);
            MetaError::RpcError(format!("Rename failed: {}", e))
        })?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn unlink(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        debug!("Unlinking {}/{} on remote", parent.as_i64(), name);

        let mut client = self.client.clone();
        let request = tonic::Request::new(UnlinkRequest {
            parent: parent.as_i64(),
            name: name.to_string(),
        });

        client.unlink(request).await.map_err(|e| {
            error!("Unlink RPC failed for {}/{}: {}", parent.as_i64(), name, e);
            MetaError::RpcError(format!("Unlink failed: {}", e))
        })?;

        Ok(())
    }

    #[instrument(skip(self))]
    async fn rmdir(&self, parent: Inode, name: &str) -> Result<(), MetaError> {
        debug!("Removing directory {}/{} on remote", parent.as_i64(), name);

        let mut client = self.client.clone();
        let request = tonic::Request::new(RmdirRequest {
            parent: parent.as_i64(),
            name: name.to_string(),
        });

        client.rmdir(request).await.map_err(|e| {
            error!("Rmdir RPC failed for {}/{}: {}", parent.as_i64(), name, e);
            MetaError::RpcError(format!("Rmdir failed: {}", e))
        })?;

        Ok(())
    }
}

// 类型转换辅助函数

fn convert_attr_from_proto(proto: &ProtoFileAttr) -> FileAttr {
    FileAttr {
        ino: proto.ino as i64,
        size: proto.size,
        blocks: proto.blocks,
        atime: proto.atime,
        mtime: proto.mtime,
        ctime: proto.ctime,
        mode: proto.mode,
        nlink: proto.nlink,
        uid: proto.uid,
        gid: proto.gid,
        rdev: proto.rdev,
        blksize: proto.blksize,
        kind: convert_file_type_from_proto(proto.kind),
        version: proto.version,
    }
}

fn convert_entry_from_proto(proto: &ProtoDirEntry) -> DirEntry {
    DirEntry {
        ino: proto.ino as i64,
        name: proto.name.clone(),
        kind: convert_file_type_from_proto(proto.kind),
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
