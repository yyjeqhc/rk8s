//! Tests for MetaStoreV2 implementations
//!
//! This module tests both DatabaseMetaStore and EtcdMetaStore implementations
//! of the MetaStoreV2 trait.

#[cfg(test)]
mod database_tests {
    use crate::meta::config::{Config, DatabaseConfig, DatabaseType};
    use crate::meta::database_store::DatabaseMetaStore;
    use crate::meta::store_v2::trait_tests;

    async fn setup_database_store() -> DatabaseMetaStore {
        // Use in-memory SQLite database for tests to avoid permission issues
        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite {
                    url: "sqlite::memory:".to_string(),
                },
            },
        };

        DatabaseMetaStore::from_config(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_database_basic_operations() {
        let store = setup_database_store().await;
        trait_tests::test_basic_operations(store).await;
    }

    #[tokio::test]
    async fn test_database_directory_operations() {
        let store = setup_database_store().await;
        trait_tests::test_directory_operations(store).await;
    }

    #[tokio::test]
    async fn test_database_error_conditions() {
        let store = setup_database_store().await;
        trait_tests::test_error_conditions(store).await;
    }

    #[tokio::test]
    async fn test_database_rename() {
        let store = setup_database_store().await;
        trait_tests::test_rename(store).await;
    }

    #[tokio::test]
    async fn test_database_batch_operations() {
        let store = setup_database_store().await;
        trait_tests::test_batch_operations(store).await;
    }
}

// Note: Etcd tests require a running Etcd cluster
// Uncomment and configure when testing with Etcd
/*
#[cfg(test)]
mod etcd_tests {
    use crate::meta::config::{Config, DatabaseConfig, DatabaseType};
    use crate::meta::etcd_store::EtcdMetaStore;
    use crate::meta::store_v2::trait_tests;

    async fn setup_etcd_store() -> EtcdMetaStore {
        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Etcd {
                    urls: vec!["http://localhost:2379".to_string()],
                },
            },
        };

        EtcdMetaStore::from_config(config).await.unwrap()
    }

    #[tokio::test]
    async fn test_etcd_basic_operations() {
        let store = setup_etcd_store().await;
        trait_tests::test_basic_operations(store).await;
    }

    #[tokio::test]
    async fn test_etcd_directory_operations() {
        let store = setup_etcd_store().await;
        trait_tests::test_directory_operations(store).await;
    }

    #[tokio::test]
    async fn test_etcd_error_conditions() {
        let store = setup_etcd_store().await;
        trait_tests::test_error_conditions(store).await;
    }

    #[tokio::test]
    async fn test_etcd_rename() {
        let store = setup_etcd_store().await;
        trait_tests::test_rename(store).await;
    }

    #[tokio::test]
    async fn test_etcd_batch_operations() {
        let store = setup_etcd_store().await;
        trait_tests::test_batch_operations(store).await;
    }
}
*/
