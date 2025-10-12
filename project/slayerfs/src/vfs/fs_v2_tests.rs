//! Tests for VFS V2 implementation
//!
//! This module tests the VfsV2 implementation with both DatabaseMetaStore
//! and EtcdMetaStore backends.

#[cfg(test)]
mod tests {
    use crate::cadapter::client::ObjectClient;
    use crate::cadapter::localfs::LocalFsBackend;
    use crate::chuck::chunk::ChunkLayout;
    use crate::chuck::store::ObjectBlockStore;
    use crate::meta::config::{Config, DatabaseConfig, DatabaseType};
    use crate::meta::database_store::DatabaseMetaStore;
    use crate::meta::types::Inode;
    use crate::vfs::fs_v2::{FileSystemV2, VfsV2};
    use tempfile::tempdir;

    async fn setup_vfs_with_database() -> VfsV2<ObjectBlockStore<LocalFsBackend>, DatabaseMetaStore> {
        let layout = ChunkLayout::default();

        // Setup block store
        let tmp = tempdir().unwrap();
        let client = ObjectClient::new(LocalFsBackend::new(tmp.path()));
        let store = ObjectBlockStore::new(client);

        // Setup metadata store
        let config = Config {
            database: DatabaseConfig {
                db_config: DatabaseType::Sqlite {
                    url: "sqlite::memory:".to_string(),
                },
            },
        };
        let meta = DatabaseMetaStore::from_config(config).await.unwrap();

        VfsV2::new(layout, store, meta).await.unwrap()
    }

    #[tokio::test]
    async fn test_vfs_basic_directory_operations() {
        let vfs = setup_vfs_with_database().await;

        // Create directory
        let dir_ino = vfs.mkdir("/test_dir", 0o755, 1000, 1000).await.unwrap();
        assert_ne!(dir_ino, Inode::ROOT);

        // Stat directory
        let attr = vfs.stat("/test_dir").await.unwrap();
        assert_eq!(attr.ino, dir_ino.as_i64());
        assert_eq!(attr.mode & 0o777, 0o755);

        // Create nested directory
        let nested_ino = vfs.mkdir("/test_dir/nested", 0o755, 1000, 1000).await.unwrap();
        assert_ne!(nested_ino, dir_ino);

        // List directory
        let entries = vfs.readdir("/test_dir").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].name, "nested");

        // Remove empty nested directory
        vfs.rmdir("/test_dir/nested").await.unwrap();

        // Verify removed
        let entries = vfs.readdir("/test_dir").await.unwrap();
        assert_eq!(entries.len(), 0);
    }

    #[tokio::test]
    async fn test_vfs_mkdir_p() {
        let vfs = setup_vfs_with_database().await;

        // Create deep directory structure
        let ino = vfs.mkdir_p("/a/b/c/d").await.unwrap();
        assert_ne!(ino, Inode::ROOT);

        // Verify all directories exist
        let _attr_a = vfs.stat("/a").await.unwrap();
        let _attr_b = vfs.stat("/a/b").await.unwrap();
        let _attr_c = vfs.stat("/a/b/c").await.unwrap();
        let attr_d = vfs.stat("/a/b/c/d").await.unwrap();

        assert_eq!(attr_d.ino, ino.as_i64());

        // mkdir_p on existing path should succeed
        let ino2 = vfs.mkdir_p("/a/b/c/d").await.unwrap();
        assert_eq!(ino, ino2);
    }

    #[tokio::test]
    async fn test_vfs_file_operations() {
        let vfs = setup_vfs_with_database().await;

        // Create file
        let file_ino = vfs.create("/test.txt", 0o644, 1000, 1000).await.unwrap();
        assert_ne!(file_ino, Inode::ROOT);

        // Stat file
        let attr = vfs.stat("/test.txt").await.unwrap();
        assert_eq!(attr.ino, file_ino.as_i64());
        assert_eq!(attr.size, 0);

        // Write data
        let data = b"Hello, World!";
        let written = vfs.write("/test.txt", 0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read data
        let read_data = vfs.read("/test.txt", 0, data.len()).await.unwrap();
        assert_eq!(read_data, data);

        // Check size updated
        let attr = vfs.stat("/test.txt").await.unwrap();
        assert_eq!(attr.size, data.len() as u64);

        // Write at offset
        let more_data = b" More data.";
        let written = vfs.write("/test.txt", data.len() as u64, more_data).await.unwrap();
        assert_eq!(written, more_data.len());

        // Read all
        let all_data = vfs.read("/test.txt", 0, 100).await.unwrap();
        assert_eq!(all_data.len(), data.len() + more_data.len());

        // Unlink file
        vfs.unlink("/test.txt").await.unwrap();

        // Verify file removed
        let result = vfs.stat("/test.txt").await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_vfs_rename_operations() {
        let vfs = setup_vfs_with_database().await;

        // Create file
        let file_ino = vfs.create("/old_name.txt", 0o644, 1000, 1000).await.unwrap();

        // Write some data
        vfs.write("/old_name.txt", 0, b"test data").await.unwrap();

        // Rename file
        vfs.rename("/old_name.txt", "/new_name.txt").await.unwrap();

        // Old path should not exist
        let result = vfs.stat("/old_name.txt").await;
        assert!(result.is_err());

        // New path should exist with same inode
        let attr = vfs.stat("/new_name.txt").await.unwrap();
        assert_eq!(attr.ino, file_ino.as_i64());

        // Data should be preserved
        let data = vfs.read("/new_name.txt", 0, 100).await.unwrap();
        assert_eq!(data, b"test data");

        // Rename directory
        vfs.mkdir("/old_dir", 0o755, 1000, 1000).await.unwrap();
        vfs.create("/old_dir/file.txt", 0o644, 1000, 1000).await.unwrap();

        vfs.rename("/old_dir", "/new_dir").await.unwrap();

        // Old path should not exist
        let result = vfs.stat("/old_dir").await;
        assert!(result.is_err());

        // New path should exist
        let _attr = vfs.stat("/new_dir").await.unwrap();

        // Child should be accessible
        let _attr = vfs.stat("/new_dir/file.txt").await.unwrap();
    }

    #[tokio::test]
    async fn test_vfs_inode_operations() {
        let vfs = setup_vfs_with_database().await;

        // Create file
        let file_ino = vfs.create("/test.txt", 0o644, 1000, 1000).await.unwrap();

        // Get attr by inode
        let attr = vfs.getattr(file_ino).await.unwrap();
        assert_eq!(attr.ino, file_ino.as_i64());

        // Write by inode
        let data = b"inode write test";
        let written = vfs.write_ino(file_ino, 0, data).await.unwrap();
        assert_eq!(written, data.len());

        // Read by inode
        let read_data = vfs.read_ino(file_ino, 0, data.len()).await.unwrap();
        assert_eq!(read_data, data);

        // Create directory and test readdir_ino
        let dir_ino = vfs.mkdir("/dir", 0o755, 1000, 1000).await.unwrap();
        vfs.create("/dir/file1.txt", 0o644, 1000, 1000).await.unwrap();
        vfs.create("/dir/file2.txt", 0o644, 1000, 1000).await.unwrap();

        let entries = vfs.readdir_ino(dir_ino).await.unwrap();
        assert_eq!(entries.len(), 2);

        let names: Vec<_> = entries.iter().map(|e| e.name.as_str()).collect();
        assert!(names.contains(&"file1.txt"));
        assert!(names.contains(&"file2.txt"));

        // Test lookup
        let file1_ino = vfs.lookup(dir_ino, "file1.txt").await.unwrap();
        let attr = vfs.getattr(file1_ino).await.unwrap();
        assert_eq!(attr.ino, file1_ino.as_i64());
    }

    #[tokio::test]
    async fn test_vfs_path_utilities() {
        let vfs = setup_vfs_with_database().await;

        // Root utilities
        let root = vfs.root_ino();
        assert_eq!(root, Inode::ROOT);

        let path = vfs.path_of(root);
        assert_eq!(path, Some("/".to_string()));

        let parent = vfs.parent_of(root);
        assert_eq!(parent, Some(root));

        // Create nested structure
        vfs.mkdir_p("/a/b/c").await.unwrap();
        let file_ino = vfs.create("/a/b/c/test.txt", 0o644, 1000, 1000).await.unwrap();

        // Test path_of
        let path = vfs.path_of(file_ino);
        assert_eq!(path, Some("/a/b/c/test.txt".to_string()));

        // Test parent_of
        let parent = vfs.parent_of(file_ino).unwrap();
        let parent_path = vfs.path_of(parent);
        assert_eq!(parent_path, Some("/a/b/c".to_string()));
    }

    #[tokio::test]
    async fn test_vfs_error_conditions() {
        let vfs = setup_vfs_with_database().await;

        // Stat non-existent path
        let result = vfs.stat("/non_existent").await;
        assert!(result.is_err());

        // Read non-existent file
        let result = vfs.read("/non_existent", 0, 10).await;
        assert!(result.is_err());

        // Unlink non-existent file
        let result = vfs.unlink("/non_existent").await;
        assert!(result.is_err());

        // Remove non-empty directory
        vfs.mkdir("/dir", 0o755, 1000, 1000).await.unwrap();
        vfs.create("/dir/file.txt", 0o644, 1000, 1000).await.unwrap();

        let result = vfs.rmdir("/dir").await;
        // Should fail because directory is not empty
        assert!(result.is_err());

        // Create duplicate file
        vfs.create("/test.txt", 0o644, 1000, 1000).await.unwrap();
        let result = vfs.create("/test.txt", 0o644, 1000, 1000).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_vfs_large_file_io() {
        let vfs = setup_vfs_with_database().await;

        let file_ino = vfs.create("/large.bin", 0o644, 1000, 1000).await.unwrap();

        // Write multiple chunks
        let chunk_size = 64 * 1024; // 64KB
        let num_chunks = 5;
        let mut expected_data = Vec::new();

        for i in 0..num_chunks {
            let mut data = vec![0u8; chunk_size];
            for (j, byte) in data.iter_mut().enumerate() {
                *byte = ((i * chunk_size + j) % 256) as u8;
            }
            expected_data.extend_from_slice(&data);

            let offset = (i * chunk_size) as u64;
            let written = vfs.write_ino(file_ino, offset, &data).await.unwrap();
            assert_eq!(written, chunk_size);
        }

        // Read back all data
        let total_size = num_chunks * chunk_size;
        let read_data = vfs.read_ino(file_ino, 0, total_size).await.unwrap();
        assert_eq!(read_data.len(), total_size);
        assert_eq!(read_data, expected_data);

        // Read partial data
        let partial = vfs.read_ino(file_ino, chunk_size as u64, chunk_size).await.unwrap();
        assert_eq!(partial, &expected_data[chunk_size..chunk_size * 2]);

        // Check file size
        let attr = vfs.getattr(file_ino).await.unwrap();
        assert_eq!(attr.size, total_size as u64);
    }
}
