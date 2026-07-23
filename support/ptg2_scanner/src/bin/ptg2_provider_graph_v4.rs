use ptg2_scanner::provider_graph_v4::{
    compile_provider_graph_v4_manifest, ProviderGraphV4Manifest,
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
    let manifest_path = arguments.next().map(PathBuf::from).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {program} <manifest.json>"),
        )
    })?;
    if arguments.next().is_some() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("usage: {program} <manifest.json>"),
        )
        .into());
    }
    let manifest: ProviderGraphV4Manifest =
        serde_json::from_reader(BufReader::new(File::open(&manifest_path)?))?;
    let summary = compile_provider_graph_v4_manifest(manifest)?;
    let mut output = BufWriter::new(io::stdout().lock());
    serde_json::to_writer(&mut output, &summary)?;
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
