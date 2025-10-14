//! Metadata client and schema
//!
//! Responsibilities:
//! - Provide a metadata client with caching layer (MetaClient)
//! - Support multiple backend stores (Database, Etcd)
//! - Handle ID generation for stateless servers
//! - Expose safe, atomic operations for inode lifecycle management
//!
//! Submodules:
//! - `client`: Metadata client with caching and batch operations
//! - `cache`: LRU cache with TTL for metadata
//! - `id_generator`: Stateless ID generation strategies
//! - `migrations`: DB migration helpers
//! - `database_store`: Database-based metadata store (SQLite/PostgreSQL)
//! - `etcd_store`: Etcd-based metadata store
//! - `factory`: Factory for creating MetaStore and MetaClient
//! - `store`: MetaStore trait definition
//! - `types`: Core types (Inode, CreateParams, etc.)

pub mod cache;
pub mod client;
pub mod config;
pub mod database_store;
pub mod entities;
pub mod error;
pub mod etcd_store;
pub mod factory;
pub mod id_generator;
pub mod migrations;
pub mod permission;
pub mod store;
pub mod types;

// Primary exports
pub use client::MetaClient;
pub use factory::{create_meta_client, create_meta_store_from_url};
pub use permission::Permission;
pub use store::MetaStore;
