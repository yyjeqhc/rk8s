//! Core types for metadata operations (V2)
//!
//! This module defines strong-typed wrappers and operation parameters for the new MetaStore trait.
//! It reuses existing FileAttr, DirEntry from the old store where appropriate.

use serde::{Deserialize, Serialize};
use std::fmt;

/// Inode number (strong type wrapper)
///
/// Provides type safety to prevent mixing inode numbers with other i64 values.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Inode(pub i64);

impl Inode {
    /// Root directory inode (always 1)
    pub const ROOT: Inode = Inode(1);

    #[inline]
    pub fn as_i64(self) -> i64 {
        self.0
    }

    #[inline]
    pub fn new(value: i64) -> Self {
        Inode(value)
    }
}

impl fmt::Display for Inode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ino:{}", self.0)
    }
}

impl From<i64> for Inode {
    fn from(value: i64) -> Self {
        Inode(value)
    }
}

impl From<Inode> for i64 {
    fn from(ino: Inode) -> Self {
        ino.0
    }
}

/// Parameters for creating a new file or directory
#[derive(Debug, Clone)]
pub struct CreateParams {
    pub parent: Inode,
    pub name: String,
    pub kind: crate::vfs::fs::FileType,
    pub mode: u32,
    pub uid: u32,
    pub gid: u32,
}

impl CreateParams {
    pub fn dir(parent: Inode, name: String, uid: u32, gid: u32) -> Self {
        Self {
            parent,
            name,
            kind: crate::vfs::fs::FileType::Dir,
            mode: 0o755,
            uid,
            gid,
        }
    }

    pub fn file(parent: Inode, name: String, uid: u32, gid: u32) -> Self {
        Self {
            parent,
            name,
            kind: crate::vfs::fs::FileType::File,
            mode: 0o644,
            uid,
            gid,
        }
    }

    pub fn with_mode(mut self, mode: u32) -> Self {
        self.mode = mode & 0o7777;
        self
    }
}

/// Attribute update mask
#[derive(Debug, Clone, Default)]
pub struct SetAttrMask {
    pub size: Option<u64>,
    pub mode: Option<u32>,
    pub uid: Option<u32>,
    pub gid: Option<u32>,
    pub atime: Option<i64>,
    pub mtime: Option<i64>,
}

impl SetAttrMask {
    pub fn size(size: u64) -> Self {
        Self {
            size: Some(size),
            ..Default::default()
        }
    }

    pub fn mode(mode: u32) -> Self {
        Self {
            mode: Some(mode),
            ..Default::default()
        }
    }

    pub fn owner(uid: u32, gid: u32) -> Self {
        Self {
            uid: Some(uid),
            gid: Some(gid),
            ..Default::default()
        }
    }

    pub fn is_empty(&self) -> bool {
        self.size.is_none()
            && self.mode.is_none()
            && self.uid.is_none()
            && self.gid.is_none()
            && self.atime.is_none()
            && self.mtime.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inode_basic() {
        let ino = Inode(100);
        assert_eq!(ino.as_i64(), 100);
        assert_eq!(format!("{}", ino), "ino:100");
    }

    #[test]
    fn test_inode_root() {
        assert_eq!(Inode::ROOT.as_i64(), 1);
        assert_eq!(Inode::ROOT, Inode(1));
    }

    #[test]
    fn test_create_params() {
        let params = CreateParams::file(Inode::ROOT, "test.txt".into(), 1000, 1000);
        assert_eq!(params.parent, Inode::ROOT);
        assert_eq!(params.name, "test.txt");
        assert_eq!(params.mode, 0o644);

        let params = CreateParams::dir(Inode::ROOT, "dir".into(), 1000, 1000);
        assert_eq!(params.mode, 0o755);
    }

    #[test]
    fn test_setattr_mask() {
        let mask = SetAttrMask::size(100);
        assert_eq!(mask.size, Some(100));
        assert!(mask.mode.is_none());
        assert!(!mask.is_empty());

        let mask = SetAttrMask::default();
        assert!(mask.is_empty());
    }
}
