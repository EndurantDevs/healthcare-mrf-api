//! Bounded UHC retained-file semantic COPY encoder.

use ptg2_scanner::uhc_retained::UHCVerifiedReplayRequest;
use ptg2_scanner::uhc_semantic::{
    encode_admitted_ranges_to_copy, AdmittedSemanticLineage, SemanticMemoryBudget,
    UhcCollectionKind, UhcSemanticReplaySource,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::env;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

struct Arguments {
    input: PathBuf,
    manifest: PathBuf,
    output: PathBuf,
    artifact_sha256: String,
    artifact_byte_count: u64,
    manifest_sha256: String,
    range_set_sha256: String,
    record_count: u64,
    range_count: usize,
    source_file_id: String,
    source_binding_id: String,
    collection_kind: UhcCollectionKind,
    budget: SemanticMemoryBudget,
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn parse_u64(value: String, field: &str) -> io::Result<u64> {
    match value.parse() {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid(format!("{field} must be an unsigned integer"))),
    }
}

fn parse_usize(value: String, field: &str) -> io::Result<usize> {
    match value.parse() {
        Ok(value) => Ok(value),
        Err(_) => Err(invalid(format!("{field} must be an unsigned integer"))),
    }
}

fn take_required(fields: &mut BTreeMap<String, String>, flag: &str) -> io::Result<String> {
    match fields.remove(flag) {
        Some(value) => Ok(value),
        None => Err(invalid(format!("{flag} is required"))),
    }
}

fn take_optional_usize(
    fields: &mut BTreeMap<String, String>,
    flag: &str,
    default: usize,
) -> io::Result<usize> {
    match fields.remove(flag) {
        Some(value) => parse_usize(value, flag),
        None => Ok(default),
    }
}

fn parse_arguments() -> io::Result<Arguments> {
    let mut values = env::args().skip(1);
    let mut fields = BTreeMap::new();
    while let Some(flag) = values.next() {
        let Some(value) = values.next() else {
            return Err(invalid(format!("missing value for {flag}")));
        };
        if fields.insert(flag.clone(), value).is_some() {
            return Err(invalid(format!("duplicate argument: {flag}")));
        }
    }
    let input = PathBuf::from(take_required(&mut fields, "--input")?);
    let manifest = PathBuf::from(take_required(&mut fields, "--manifest")?);
    let output = PathBuf::from(take_required(&mut fields, "--output")?);
    let artifact_sha256 = take_required(&mut fields, "--artifact-sha256")?;
    let artifact_byte_count = parse_u64(
        take_required(&mut fields, "--artifact-byte-count")?,
        "--artifact-byte-count",
    )?;
    let manifest_sha256 = take_required(&mut fields, "--manifest-sha256")?;
    let range_set_sha256 = take_required(&mut fields, "--range-set-sha256")?;
    let record_count = parse_u64(
        take_required(&mut fields, "--record-count")?,
        "--record-count",
    )?;
    let range_count = parse_usize(
        take_required(&mut fields, "--range-count")?,
        "--range-count",
    )?;
    let source_file_id = take_required(&mut fields, "--source-file-id")?;
    let source_binding_id = take_required(&mut fields, "--source-binding-id")?;
    let collection_kind = match take_required(&mut fields, "--collection-kind")?.as_str() {
        "provider_membership" => UhcCollectionKind::ProviderMembership,
        "plan_reference" => UhcCollectionKind::PlanReference,
        _ => return Err(invalid("--collection-kind is unsupported".to_owned())),
    };
    let defaults = SemanticMemoryBudget::default();
    let budget = SemanticMemoryBudget {
        worker_count: take_optional_usize(&mut fields, "--workers", defaults.worker_count)?,
        per_worker_bytes: take_optional_usize(
            &mut fields,
            "--per-worker-memory-bytes",
            defaults.per_worker_bytes,
        )?,
        total_bytes: take_optional_usize(
            &mut fields,
            "--total-memory-bytes",
            defaults.total_bytes,
        )?,
        max_record_bytes: take_optional_usize(
            &mut fields,
            "--max-record-bytes",
            defaults.max_record_bytes,
        )?,
        evidence_buffer_bytes: take_optional_usize(
            &mut fields,
            "--evidence-buffer-bytes",
            defaults.evidence_buffer_bytes,
        )?,
    };
    if let Some(flag) = fields.keys().next() {
        return Err(invalid(format!("unknown argument: {flag}")));
    }
    Ok(Arguments {
        input,
        manifest,
        output,
        artifact_sha256,
        artifact_byte_count,
        manifest_sha256,
        range_set_sha256,
        record_count,
        range_count,
        source_file_id,
        source_binding_id,
        collection_kind,
        budget,
    })
}

fn file_sha256(path: &Path) -> io::Result<String> {
    let file = File::open(path)?;
    let mut reader = BufReader::with_capacity(1024 * 1024, file);
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut digest = Sha256::new();
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    let bytes = digest.finalize();
    let mut encoded = String::with_capacity(bytes.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in bytes {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(encoded)
}

fn encoded_report(report: impl serde::Serialize, encoder_sha256: &str) -> io::Result<String> {
    let mut value = match serde_json::to_value(report) {
        Ok(value) => value,
        Err(error) => return Err(io::Error::other(error.to_string())),
    };
    let Some(object) = value.as_object_mut() else {
        return Err(io::Error::other("UHC semantic report is not an object"));
    };
    object.insert(
        "encoder_sha256".to_owned(),
        Value::String(encoder_sha256.to_owned()),
    );
    Ok(serde_json::to_string(&value).expect("JSON value serialization is infallible"))
}

fn run() -> io::Result<()> {
    let arguments = parse_arguments()?;
    if arguments.output != Path::new("-") && arguments.output.exists() {
        return Err(invalid("UHC semantic output already exists".to_owned()));
    }
    let encoder_sha256 = file_sha256(&env::current_exe()?)?;
    let source = UhcSemanticReplaySource::open(
        &UHCVerifiedReplayRequest {
            raw_path: arguments.input,
            manifest_path: arguments.manifest,
            expected_artifact_sha256: arguments.artifact_sha256.clone(),
            expected_artifact_byte_count: arguments.artifact_byte_count,
            expected_manifest_sha256: arguments.manifest_sha256.clone(),
            expected_range_set_sha256: arguments.range_set_sha256.clone(),
            expected_record_count: arguments.record_count,
            expected_range_count: arguments.range_count,
        },
        AdmittedSemanticLineage {
            artifact_sha256: arguments.artifact_sha256,
            manifest_sha256: arguments.manifest_sha256,
            range_set_sha256: arguments.range_set_sha256,
            source_file_id: arguments.source_file_id,
            source_binding_id: arguments.source_binding_id,
            collection_kind: arguments.collection_kind,
        },
    )?;
    if arguments.output == Path::new("-") {
        let stdout = io::stdout();
        let report = encode_admitted_ranges_to_copy(&source, stdout, &arguments.budget)?;
        eprintln!("{}", encoded_report(report, &encoder_sha256)?);
        return Ok(());
    }

    let file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&arguments.output)?;
    let mut output = BufWriter::with_capacity(4 * 1024 * 1024, file);
    let report = encode_admitted_ranges_to_copy(&source, &mut output, &arguments.budget)?;
    output.flush()?;
    let file = match output.into_inner() {
        Ok(file) => file,
        Err(error) => return Err(error.into_error()),
    };
    file.sync_all()?;
    println!("{}", encoded_report(report, &encoder_sha256)?);
    Ok(())
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct SerializationFailure;

    impl serde::Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("injected report failure"))
        }
    }

    #[test]
    fn report_encoding_rejects_serialization_failure_and_non_object_values() {
        assert!(encoded_report(SerializationFailure, "encoder")
            .unwrap_err()
            .to_string()
            .contains("injected report failure"));
        assert!(encoded_report((), "encoder")
            .unwrap_err()
            .to_string()
            .contains("not an object"));
    }
}
