fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Retrieve the OUT_DIR environment variable, where build scripts should place generated files
    let out_dir = std::env::var("OUT_DIR")?;

    // Compile the protobuf files with prost_build
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(
            &[
                "solana/dex_block_message.proto",
                "solana/token_block_message.proto",
                "solana/block_message.proto",
                "solana/parsed_idl_block_message.proto"
            ],
            &["."]
        )?;

    Ok(())
}
