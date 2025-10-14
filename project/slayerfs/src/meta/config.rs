//! SlayerFS configuration management
//!
//! Comprehensive configuration supporting database, cache, logging, and gRPC

use serde::{Deserialize, Serialize};
use std::path::Path;
use std::time::Duration;
use thiserror::Error;

/// SlayerFS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Metadata backend configuration
    #[serde(default)]
    pub metadata: MetadataConfig,

    /// Cache configuration
    #[serde(default)]
    pub cache: CacheConfig,

    /// Logging configuration
    #[serde(default)]
    pub logging: LoggingConfig,

    /// Storage configuration
    #[serde(default)]
    pub storage: StorageConfig,
}

/// Metadata configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetadataConfig {
    /// Backend type and connection
    #[serde(flatten)]
    pub backend: MetadataBackend,
}

/// Metadata backend configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "backend_type")]
pub enum MetadataBackend {
    /// Local database (SQLite/PostgreSQL)
    #[serde(rename = "database")]
    Database {
        #[serde(flatten)]
        config: DatabaseConfig,
    },

    /// Remote gRPC metadata server
    #[serde(rename = "grpc")]
    Grpc {
        /// Server endpoint (e.g., "localhost:9000")
        endpoint: String,

        /// Connection timeout in seconds
        #[serde(default = "default_grpc_timeout")]
        timeout_secs: u64,

        /// Enable TLS
        #[serde(default)]
        tls: bool,
    },
}

fn default_grpc_timeout() -> u64 {
    30
}

/// Cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    /// Enable caching
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Attribute cache size
    #[serde(default = "default_attr_cache_size")]
    pub attr_cache_size: usize,

    /// Attribute cache TTL in seconds
    #[serde(default = "default_attr_cache_ttl")]
    pub attr_cache_ttl_secs: u64,

    /// Dentry cache size
    #[serde(default = "default_dentry_cache_size")]
    pub dentry_cache_size: usize,

    /// Dentry cache TTL in seconds
    #[serde(default = "default_dentry_cache_ttl")]
    pub dentry_cache_ttl_secs: u64,

    /// Negative cache size
    #[serde(default = "default_negative_cache_size")]
    pub negative_cache_size: usize,

    /// Negative cache TTL in seconds
    #[serde(default = "default_negative_cache_ttl")]
    pub negative_cache_ttl_secs: u64,
}

fn default_true() -> bool {
    true
}
fn default_attr_cache_size() -> usize {
    10000
}
fn default_attr_cache_ttl() -> u64 {
    60
}
fn default_dentry_cache_size() -> usize {
    10000
}
fn default_dentry_cache_ttl() -> u64 {
    60
}
fn default_negative_cache_size() -> usize {
    5000
}
fn default_negative_cache_ttl() -> u64 {
    30
}

/// Logging configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error
    #[serde(default = "default_log_level")]
    pub level: String,

    /// Log format: text, json
    #[serde(default = "default_log_format")]
    pub format: String,

    /// Log to file path (optional)
    pub file: Option<String>,
}

fn default_log_level() -> String {
    "info".to_string()
}
fn default_log_format() -> String {
    "text".to_string()
}

/// Storage configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageConfig {
    /// Chunk size in bytes
    #[serde(default = "default_chunk_size")]
    pub chunk_size: u32,

    /// Block size in bytes
    #[serde(default = "default_block_size")]
    pub block_size: u32,
}

fn default_chunk_size() -> u32 {
    64 * 1024 * 1024
} // 64MB
fn default_block_size() -> u32 {
    4 * 1024
} // 4KB

impl Default for Config {
    fn default() -> Self {
        Self {
            metadata: MetadataConfig::default(),
            cache: CacheConfig::default(),
            logging: LoggingConfig::default(),
            storage: StorageConfig::default(),
        }
    }
}

impl Default for MetadataConfig {
    fn default() -> Self {
        Self {
            backend: MetadataBackend::Database {
                config: DatabaseConfig::default(),
            },
        }
    }
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            attr_cache_size: 10000,
            attr_cache_ttl_secs: 60,
            dentry_cache_size: 10000,
            dentry_cache_ttl_secs: 60,
            negative_cache_size: 5000,
            negative_cache_ttl_secs: 30,
        }
    }
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: "info".to_string(),
            format: "text".to_string(),
            file: None,
        }
    }
}

impl Default for StorageConfig {
    fn default() -> Self {
        Self {
            chunk_size: 64 * 1024 * 1024,
            block_size: 4 * 1024,
        }
    }
}

// ========== Legacy compatibility ==========

/// Database configuration (kept for backward compatibility)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    #[serde(flatten)]
    pub db_config: DatabaseType,
}

/// Database type enumeration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum DatabaseType {
    #[serde(rename = "sqlite")]
    Sqlite {
        #[serde(default = "default_sqlite_url")]
        url: String,
    },
    #[serde(rename = "postgres")]
    Postgres { url: String },
    #[serde(rename = "etcd")]
    Etcd { urls: Vec<String> },
}

fn default_sqlite_url() -> String {
    "sqlite:///tmp/slayerfs/metadata.db".to_string()
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            db_config: DatabaseType::Sqlite {
                url: default_sqlite_url(),
            },
        }
    }
}

impl Config {
    /// Load configuration from YAML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self, ConfigError> {
        let content = std::fs::read_to_string(path.as_ref()).map_err(ConfigError::IoError)?;

        let config: Config =
            serde_yaml::from_str(&content).map_err(|e| ConfigError::ParseError(e.to_string()))?;

        Ok(config)
    }

    /// Load configuration from environment variables
    pub fn from_env() -> Result<Self, ConfigError> {
        let mut config = Self::default();

        // Metadata backend
        if let Ok(endpoint) = std::env::var("SLAYERFS_META_ENDPOINT") {
            config.metadata.backend = MetadataBackend::Grpc {
                endpoint,
                timeout_secs: std::env::var("SLAYERFS_META_TIMEOUT")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(30),
                tls: std::env::var("SLAYERFS_META_TLS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(false),
            };
        } else if let Ok(db_url) = std::env::var("SLAYERFS_DATABASE_URL") {
            let db_type = if db_url.starts_with("postgres") {
                DatabaseType::Postgres { url: db_url }
            } else {
                DatabaseType::Sqlite { url: db_url }
            };
            config.metadata.backend = MetadataBackend::Database {
                config: DatabaseConfig { db_config: db_type },
            };
        }

        // Cache configuration
        if let Ok(size) = std::env::var("SLAYERFS_CACHE_SIZE") {
            if let Ok(size) = size.parse() {
                config.cache.attr_cache_size = size;
                config.cache.dentry_cache_size = size;
            }
        }

        if let Ok(ttl) = std::env::var("SLAYERFS_CACHE_TTL") {
            if let Ok(ttl) = ttl.parse() {
                config.cache.attr_cache_ttl_secs = ttl;
                config.cache.dentry_cache_ttl_secs = ttl;
            }
        }

        // Logging configuration
        if let Ok(level) = std::env::var("SLAYERFS_LOG_LEVEL") {
            config.logging.level = level;
        }

        if let Ok(format) = std::env::var("SLAYERFS_LOG_FORMAT") {
            config.logging.format = format;
        }

        Ok(config)
    }

    /// Load configuration from path, fallback to default paths
    pub fn from_path(backend_path: &Path) -> Result<Self, ConfigError> {
        let config_file = backend_path.join("slayerfs.yml");
        if config_file.exists() {
            return Self::from_file(&config_file);
        }

        Self::from_default_path()
    }

    /// Load configuration from default paths
    pub fn from_default_path() -> Result<Self, ConfigError> {
        let possible_paths = [
            "slayerfs.yml",
            "slayerfs.yaml",
            "config.yml",
            "config.yaml",
            "/etc/slayerfs/config.yml",
        ];

        for path in &possible_paths {
            if std::path::Path::new(path).exists() {
                return Self::from_file(path);
            }
        }

        // Try environment variables as fallback
        Self::from_env().or(Err(ConfigError::ConfigNotFound))
    }

    /// Get database config (for backward compatibility)
    pub fn database(&self) -> Option<&DatabaseConfig> {
        match &self.metadata.backend {
            MetadataBackend::Database { config } => Some(config),
            _ => None,
        }
    }
}

impl DatabaseConfig {
    /// Get database type string
    pub fn db_type_str(&self) -> &'static str {
        match &self.db_config {
            DatabaseType::Sqlite { .. } => "sqlite",
            DatabaseType::Postgres { .. } => "postgres",
            DatabaseType::Etcd { .. } => "etcd",
        }
    }
}

/// Configuration error types
#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    IoError(std::io::Error),

    #[error("Parse error: {0}")]
    ParseError(String),

    #[error("Config file not found in default locations")]
    ConfigNotFound,
}
