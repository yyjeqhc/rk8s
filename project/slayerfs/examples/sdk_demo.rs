// Example program: demonstrate vfs::sdk_v2::ClientV2 usage without FUSE

use slayerfs::chuck::chunk::ChunkLayout;
use slayerfs::vfs::sdk::LocalClient;
use std::path::PathBuf;

#[tokio::main(flavor = "current_thread")]
async fn main() {
    // Resolve object root directory from argv or default to a temp location
    let mut args = std::env::args().skip(1);
    let root: PathBuf = if let Some(p) = args.next() {
        PathBuf::from(p)
    } else {
        std::env::temp_dir().join("slayerfs-sdk-demo")
    };

    let layout = ChunkLayout::default();
    let mut cli = LocalClient::new_local(&root, layout).await;

    // Prepare paths
    let dir = "/demo/ns";
    let file_a = "/demo/ns/a.txt";
    let file_b = "/demo/ns/b.txt";
    let file_a2 = "/demo/renamed/a_renamed.txt";

    // Ensure directory and files
    cli.mkdir_p(dir).await.expect("mkdir_p");
    cli.create(file_a).await.expect("create a");
    cli.create(file_b).await.expect("create b");

    // A) Write cross-block content to a.txt starting at half block
    let half = (layout.block_size / 2) as usize;
    let len = layout.block_size as usize + half; // 1.5 blocks
    let mut data = vec![0u8; len];
    for (i, b) in data.iter_mut().enumerate().take(len) {
        *b = (i % 251) as u8;
    }
    cli.write_at(file_a, half as u64, &data)
        .await
        .expect("write A half+block");

    // B) Overwrite prefix of a.txt at offset 0
    let prefix: Vec<u8> = (0..128u8).collect();
    cli.write_at(file_a, 0, &prefix)
        .await
        .expect("overwrite prefix");

    // C) Cross-chunk write: near chunk boundary
    let near_end = layout.chunk_size - 10;
    let cross = vec![7u8; 100]; // crosses into next chunk
    cli.write_at(file_a, near_end, &cross)
        .await
        .expect("cross-chunk write");

    // D) Sparse write in b.txt leaving holes
    let off_b = (layout.block_size as u64) * 2 + 123; // after 2 blocks + 123 bytes
    let data_b = vec![9u8; 1024];
    cli.write_at(file_b, off_b, &data_b)
        .await
        .expect("sparse write B");

    // Reads and validations
    let out_a = cli
        .read_at(file_a, half as u64, len)
        .await
        .expect("read A segment");
    assert_eq!(out_a, data, "A segment readback");
    let out_prefix = cli
        .read_at(file_a, 0, prefix.len())
        .await
        .expect("read prefix");
    assert_eq!(out_prefix, prefix, "prefix overwritten should match");
    // Read across chunk boundary: should match what we wrote
    let out_cross = cli
        .read_at(file_a, near_end, cross.len())
        .await
        .expect("read cross-chunk");
    assert_eq!(out_cross, cross);

    // Zero-fill checks on B: read a window that partially overlaps the hole and data
    let read_start = off_b - 200; // before the written region
    let read_len = 200 + 1024 + 200; // include before and after
    let out_b = cli
        .read_at(file_b, read_start, read_len as usize)
        .await
        .expect("read around hole");
    // Expect first 200 bytes to be zero, next 1024 to be 9, last 200 to be zero
    assert!(out_b[..200].iter().all(|&b| b == 0));
    assert!(out_b[200..(200 + 1024)].iter().all(|&b| b == 9));
    assert!(out_b[(200 + 1024)..].iter().all(|&b| b == 0));

    // Read zero length returns empty
    let empty = cli.read_at(file_a, 0, 0).await.expect("read zero");
    assert!(empty.is_empty());

    // Directory listing before rename
    let ents = cli.readdir(dir).await.expect("readdir before rename");
    println!(
        "{} => {}",
        dir,
        ents.iter()
            .map(|e| e.name.clone())
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Cross-directory rename (auto-create parent)
    cli.rename(file_a, file_a2)
        .await
        .expect("rename across dirs");
    println!("✓ Renamed {} to {}", file_a, file_a2);

    // Read data back from renamed file
    let out_a_renamed = cli
        .read_at(file_a2, half as u64, len)
        .await
        .expect("read renamed file");
    assert_eq!(out_a_renamed, data, "renamed file readback");
    println!("✓ Read data from renamed file successfully");

    // Cleanup: remove files
    cli.remove(file_b).await.expect("remove b");
    cli.remove(file_a2).await.expect("remove a2");
    println!("✓ Removed test files");

    println!("\n🎉 sdk_v2 demo: All operations completed successfully!");
}
