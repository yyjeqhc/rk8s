//! Error types for metadata operations (V2)
//!
//! This module extends the existing MetaError with errno conversion capability.

use crate::meta::store::MetaError;
use crate::meta::types::Inode;

/// Extension trait for MetaError to add errno conversion
pub trait MetaErrorExt {
    /// Convert error to POSIX errno code for FUSE
    fn to_errno(&self) -> i32;
}

impl MetaErrorExt for MetaError {
    fn to_errno(&self) -> i32 {
        match self {
            MetaError::NotFound(_) | MetaError::ParentNotFound(_) => libc::ENOENT,
            MetaError::AlreadyExists { .. } => libc::EEXIST,
            MetaError::NotDirectory(_) => libc::ENOTDIR,
            MetaError::DirectoryNotEmpty(_) => libc::ENOTEMPTY,
            MetaError::InvalidPath(_) => libc::EINVAL,
            MetaError::NotSupported(_) => libc::ENOTSUP,
            MetaError::NotImplemented => libc::ENOSYS,
            MetaError::Io(err) => err.raw_os_error().unwrap_or(libc::EIO),
            MetaError::Database(_) | MetaError::Serialization(_) | MetaError::Internal(_) => {
                libc::EIO
            }
            MetaError::Config(_) => libc::EINVAL,
        }
    }
}

/// Helper functions for creating MetaError from Inode
pub trait MetaErrorHelper {
    fn not_found(ino: Inode) -> Self;
    fn parent_not_found(ino: Inode) -> Self;
    fn already_exists(parent: Inode, name: impl Into<String>) -> Self;
    fn not_directory(ino: Inode) -> Self;
    fn directory_not_empty(ino: Inode) -> Self;
}

impl MetaErrorHelper for MetaError {
    fn not_found(ino: Inode) -> Self {
        MetaError::NotFound(ino.as_i64())
    }

    fn parent_not_found(ino: Inode) -> Self {
        MetaError::ParentNotFound(ino.as_i64())
    }

    fn already_exists(parent: Inode, name: impl Into<String>) -> Self {
        MetaError::AlreadyExists {
            parent: parent.as_i64(),
            name: name.into(),
        }
    }

    fn not_directory(ino: Inode) -> Self {
        MetaError::NotDirectory(ino.as_i64())
    }

    fn directory_not_empty(ino: Inode) -> Self {
        MetaError::DirectoryNotEmpty(ino.as_i64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_errno_conversion() {
        let err = MetaError::not_found(Inode(1));
        assert_eq!(err.to_errno(), libc::ENOENT);

        let err = MetaError::already_exists(Inode::ROOT, "test");
        assert_eq!(err.to_errno(), libc::EEXIST);

        let err = MetaError::not_directory(Inode(1));
        assert_eq!(err.to_errno(), libc::ENOTDIR);

        let err = MetaError::directory_not_empty(Inode(1));
        assert_eq!(err.to_errno(), libc::ENOTEMPTY);
    }
}
