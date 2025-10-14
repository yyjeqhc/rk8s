//! Metadata store factory
//!
//! Creates appropriate MetaStore implementation and MetaClient based on configuration

use std::path::Path;
use std::sync::Arc;

use crate::meta::cache::CacheConfig;
use crate::meta::client::MetaClient;
use crate::meta::config::{Config, DatabaseType};
use crate::meta::database_store::DatabaseMetaStore;
use crate::meta::etcd_store::EtcdMetaStore;
use crate::meta::store::{MetaError, MetaStore};

/// Factory for creating MetaStore instances
pub struct MetaStoreFactory;

impl MetaStoreFactory {
    /// Create MetaStore from path
    #[allow(dead_code)]
    pub async fn create_from_path(backend_path: &Path) -> Result<Arc<dyn MetaStore>, MetaError> {
        let config =
            Config::from_path(backend_path).map_err(|e| MetaError::Config(e.to_string()))?;
        Self::create_from_config(config).await
    }

    /// Create MetaStore from config
    pub async fn create_from_config(config: Config) -> Result<Arc<dyn MetaStore>, MetaError> {
        match &config.database.db_config {
            DatabaseType::Sqlite { .. } | DatabaseType::Postgres { .. } => {
                let store = DatabaseMetaStore::from_config(config).await?;
                Ok(Arc::new(store))
            }
            DatabaseType::Etcd { .. } => {
                let store = EtcdMetaStore::from_config(config).await?;
                Ok(Arc::new(store))
            }
        }
    }

    /// Create MetaStore from URL (simplified interface)
    pub async fn create_from_url(url: &str) -> Result<Arc<dyn MetaStore>, MetaError> {
        let config = Self::config_from_url(url)?;
        Self::create_from_config(config).await
    }

    /// Parse URL to config
    fn config_from_url(url: &str) -> Result<Config, MetaError> {
        use crate::meta::config::{DatabaseConfig, DatabaseType};

        let db_config = if url.starts_with("sqlite:") {
            DatabaseType::Sqlite {
                url: url.to_string(),
            }
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            DatabaseType::Postgres {
                url: url.to_string(),
            }
        } else if url.starts_with("etcd://")
            || url.starts_with("http://")
            || url.starts_with("https://")
        {
            // For etcd, support comma-separated URLs
            let urls: Vec<String> = if url.contains(',') {
                url.split(',').map(|s| s.trim().to_string()).collect()
            } else {
                vec![url.to_string()]
            };
            DatabaseType::Etcd { urls }
        } else {
            return Err(MetaError::Config(format!(
                "Unsupported URL scheme: {}",
                url
            )));
        };

        Ok(Config {
            database: DatabaseConfig { db_config },
        })
    }
}

/// Factory for creating MetaClient instances
pub struct MetaClientFactory;

impl MetaClientFactory {
    /// Create MetaClient from URL with default cache config
    pub async fn create_from_url(url: &str) -> Result<MetaClient, MetaError> {
        let store = MetaStoreFactory::create_from_url(url).await?;
        Ok(MetaClient::new(store))
    }

    /// Create MetaClient from URL with custom cache config
    pub async fn create_from_url_with_config(
        url: &str,
        cache_config: CacheConfig,
    ) -> Result<MetaClient, MetaError> {
        let store = MetaStoreFactory::create_from_url(url).await?;
        Ok(MetaClient::with_config(store, cache_config))
    }

    /// Create MetaClient from config
    pub async fn create_from_config(config: Config) -> Result<MetaClient, MetaError> {
        let store = MetaStoreFactory::create_from_config(config).await?;
        Ok(MetaClient::new(store))
    }

    /// Create MetaClient from config with custom cache config
    pub async fn create_from_config_with_cache(
        config: Config,
        cache_config: CacheConfig,
    ) -> Result<MetaClient, MetaError> {
        let store = MetaStoreFactory::create_from_config(config).await?;
        Ok(MetaClient::with_config(store, cache_config))
    }
}

/// Convenience function to create MetaStore from path
#[allow(dead_code)]
pub async fn create_meta_store(backend_path: &Path) -> Result<Arc<dyn MetaStore>, MetaError> {
    MetaStoreFactory::create_from_path(backend_path).await
}

/// Convenience function to create MetaStore from URL
pub async fn create_meta_store_from_url(url: &str) -> Result<Arc<dyn MetaStore>, MetaError> {
    MetaStoreFactory::create_from_url(url).await
}

/// Convenience function to create MetaClient from URL
pub async fn create_meta_client(url: &str) -> Result<MetaClient, MetaError> {
    MetaClientFactory::create_from_url(url).await
}

/// Convenience function to create MetaClient from URL with custom cache config
pub async fn create_meta_client_with_cache(
    url: &str,
    cache_config: CacheConfig,
) -> Result<MetaClient, MetaError> {
    MetaClientFactory::create_from_url_with_config(url, cache_config).await
}
