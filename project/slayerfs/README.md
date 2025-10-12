
<div align="center">
	<img src="doc/icon.png" alt="SlayerFS icon" width="96" height="96" />
</div>

<h1 align="center">SlayerFS</h1>
<p align="center"><strong>High-performance Rust &amp; Layers-aware Distributed Filesystem</strong></p>
<p align="center"><a href="README.md"><b>English</b></a> | <a href="README_CN.md">中文</a></p>

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/language-Rust-orange.svg)](https://www.rust-lang.org/)

> **📢 Important**: SlayerFS V2 is now available with improved architecture and full FUSE support!  
> See [MIGRATION_V2.md](MIGRATION_V2.md) for details. All examples have been migrated to V2.

## ✨ Project Overview

SlayerFS is a Rust-based distributed filesystem for container and AI scenarios. It uses a chunk/block layout and integrates with object storage backends (LocalFS implemented; S3/Rustfs reserved) to provide path-based read/write, directory operations, truncate, and other basic capabilities, making it easy to integrate with SDKs and FUSE.

Core idea: decouple compute from storage. Applications use POSIX-like interfaces to access data, while the scheduler/cache layers decide where the data lives and how it’s accessed.

## 🖼 Architecture

<div align="center">
	<img src="doc/SlayerFS.png" alt="SlayerFS architecture" width="1280" />
</div>

Components overview:
- chuck: ChunkLayout, ChunkReader/Writer. Maps file offsets to chunk/block and handles cross-block IO and zero-filling holes.
- cadapter: Object backend abstraction and implementations (LocalFs implemented; S3/Rustfs placeholders).
- meta: In-memory metadata + transactions (InMemoryMetaStore). Tracks size and slice, supports commit/rollback.
- vfs: Path-based simplified VFS (mkdir_p/create/read/write/readdir/stat/unlink/rmdir/rename/truncate).
- sdk: App-facing lightweight client wrapper (with LocalClient convenience).

## 🚀 Quick Start

### Requirements

- Rust: >= 1.75.0
- Operating system: Linux (Ubuntu 20.04+, CentOS 8+)

### V2 Examples (Recommended)

**Mount with SQLite backend:**
```bash
cargo run --example mount_local -- --data /tmp/data --mount /tmp/mnt
```

**Persistence demo (supports SQLite/PostgreSQL/etcd):**
```bash
cargo run --example persistence_demo -- \
  --config sqlite.yml \
  --storage /tmp/storage \
  --mount /tmp/mnt
```

**SDK usage:**
```bash
cargo run --example sdk_demo
```

**S3 demo:**
```bash
cargo run --example s3_demo
```

### Legacy V1 Demo (Deprecated)

```bash
cargo run -q --bin sdk_demo -- /tmp/slayerfs-objroot
```
The demo will:
- Create nested directories/files, perform cross-block/chunk writes and read verification
- Do rename, truncate (shrink/extend), readdir and unlink/rmdir
- Print expected error scenarios and finally output "sdk demo: OK"

> **Note**: V1 API is deprecated. Please use V2 examples for new projects.

---

## 🌟 Features

### V2 Features (Current)
- ✅ **Inode-based VFS**: Clean separation between path and inode operations
- ✅ **Full FUSE support**: Including `readdirplus` for modern `ls` commands  
- ✅ **Multiple metadata backends**: SQLite, PostgreSQL, etcd
- ✅ **Improved performance**: Optimized inode-based operations
- ✅ **Better SDK**: Type-safe `ClientV2` and `LocalClientV2`

See [MIGRATION_V2.md](MIGRATION_V2.md) for V2 API details.

### V1 Features (Legacy - Deprecated)

### Path-based VFS
- mkdir_p/create/read/write/readdir/stat/exists/unlink/rmdir/rename/truncate
- Single mutex to protect the namespace (avoid multi-lock deadlocks); avoid awaiting under lock on hot paths

### Chunked IO with zero-fill
- 64MiB chunk + 4MiB block (default, configurable)
- Write path splits by block; read path returns zeros for holes

### Object-backed BlockStore
- LocalFs implemented (for tests/examples); S3/Rustfs placeholders

### Metadata with txn
- InMemoryMetaStore: alloc_inode, record_slice, update_size (truncate shrink works)
- Transaction commit/rollback tests are in place

More: see `doc/sdk.md` and inline rustdoc.

---

## 📚 Documentation
- **V2 Migration Guide**: [MIGRATION_V2.md](MIGRATION_V2.md) - Complete guide for V2 API
- **Architecture**: [doc/arch.md](doc/arch.md) - System design and components
- **Metadata**: [doc/meta.md](doc/meta.md) - MetaStore implementation details
- **SDK Guide**: [doc/sdk.md](doc/sdk.md) - SDK usage and examples

---


## 🤝 Contributing

Issues and PRs are welcome to improve architecture, implementation, and docs.


