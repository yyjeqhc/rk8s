//! Metadata client and schema
//!
//! Responsibilities:
//! - Provide a metadata client with caching layer (MetaClient)
//! - Support multiple backend stores (Database, Etcd, Remote gRPC)
//! - Handle ID generation for stateless servers
//! - Expose safe, atomic operations for inode lifecycle management
//!
//! Submodules:
//! - `client`: Metadata client with caching and batch operations
//! - `cache`: LRU cache with TTL for metadata
//! - `id_generator`: Stateless ID generation strategies
//! - `migrations`: DB migration helpers
//! - `stores`: All MetaStore implementations (database, etcd, remote)
//! - `factory`: Factory for creating MetaStore and MetaClient
//! - `store`: MetaStore trait definition
//! - `types`: Core types (Inode, CreateParams, etc.)
//! - `proto`: Auto-generated gRPC protocol definitions

pub mod cache;
pub mod client;
pub mod config;
pub mod entities;
pub mod error;
pub mod factory;
pub mod id_generator;
pub mod migrations;
pub mod permission;
pub mod proto;
pub mod server;
pub mod store;
pub mod stores;
pub mod types;

// Primary exports
pub use client::MetaClient;
pub use factory::{create_meta_client, create_meta_store_from_url};
pub use permission::Permission;
pub use server::MetaServer;
pub use store::MetaStore;

// Re-export store implementations
pub use stores::{DatabaseMetaStore, EtcdMetaStore, RemoteMetaStore};
