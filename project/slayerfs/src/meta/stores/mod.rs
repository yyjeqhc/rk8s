//! Metadata Store Implementations
//!
//! This module contains all concrete implementations of the MetaStore trait.
//! Each store provides a different backend for metadata persistence:
//!
//! - `DatabaseStore`: SQL databases (PostgreSQL, SQLite)
//! - `EtcdStore`: Distributed etcd cluster
//! - `RemoteStore`: gRPC client to remote MetaServer
//! - `CacheStore`: Pure in-memory cache (defined in parent module)

pub mod database_store;
pub mod etcd_store;
pub mod remote_store;

// Re-export main types for convenience
pub use database_store::DatabaseMetaStore;
pub use etcd_store::EtcdMetaStore;
pub use remote_store::RemoteMetaStore;
