fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Configure tonic-build to generate gRPC code
    // The generated code will be placed in OUT_DIR (target/debug/build/.../out/)
    // This is the recommended approach - DO NOT output to src/ directory
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // Do NOT use .out_dir() - let it use the default OUT_DIR
        .compile_protos(&["proto/meta.proto"], &["proto"])?;

    // Tell cargo to rerun this build script if the proto file changes
    println!("cargo:rerun-if-changed=proto/meta.proto");

    Ok(())
}
