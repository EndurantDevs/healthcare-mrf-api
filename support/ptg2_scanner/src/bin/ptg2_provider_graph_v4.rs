use ptg2_scanner::provider_graph_v4::{
    compile_provider_graph_v4_manifest, extract_provider_graph_v4_npi_scope,
    ProviderGraphV4Manifest, ProviderGraphV4NpiScopeManifest,
};
use std::env;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Write};
use std::path::PathBuf;

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os();
    let program = arguments
        .next()
        .and_then(|value| value.into_string().ok())
        .unwrap_or_else(|| "ptg2_provider_graph_v4".to_owned());
    let first = arguments.next().ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {program} [--extract-npi-scope] <manifest.json>"),
        )
    })?;
    let (extract_scope, manifest_path) = if first == "--extract-npi-scope" {
        (
            true,
            arguments.next().map(PathBuf::from).ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("usage: {program} --extract-npi-scope <manifest.json>"),
                )
            })?,
        )
    } else {
        (false, PathBuf::from(first))
    };
    if arguments.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {program} [--extract-npi-scope] <manifest.json>"),
        )
        .into());
    }
    let mut output = BufWriter::new(io::stdout().lock());
    if extract_scope {
        let manifest: ProviderGraphV4NpiScopeManifest =
            serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
        let summary = extract_provider_graph_v4_npi_scope(&manifest.shards, manifest.output_path)?;
        serde_json::to_writer(&mut output, &summary)?;
    } else {
        let manifest: ProviderGraphV4Manifest =
            serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
        let summary = compile_provider_graph_v4_manifest(manifest)?;
        serde_json::to_writer(&mut output, &summary)?;
    }
    output.write_all(b"\n")?;
    output.flush()?;
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("PTG2_PROVIDER_GRAPH_V4_ERROR\t{error}");
        std::process::exit(1);
    }
}
