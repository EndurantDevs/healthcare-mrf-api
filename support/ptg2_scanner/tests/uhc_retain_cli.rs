use ptg2_scanner::uhc_retained::{retain_uhc_artifact, UHCRetainRequest};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::process::Command;

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(bytes);
    let mut encoded = String::with_capacity(64);
    for byte in digest {
        encoded.push(char::from(HEX[usize::from(byte >> 4)]));
        encoded.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    encoded
}

fn run_retain_cli(
    source: &Path,
    retained: &Path,
    sha256: &str,
    byte_count: usize,
    ranges: usize,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args([
            "--uhc-retain",
            source.to_str().expect("UTF-8 source path"),
            retained.to_str().expect("UTF-8 retained path"),
            sha256,
            &byte_count.to_string(),
            &ranges.to_string(),
        ])
        .output()
        .expect("run ptg2_scanner --uhc-retain")
}

fn summary_from_success(completed: std::process::Output) -> Value {
    assert!(
        completed.status.success(),
        "CLI failed: {}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let stdout = String::from_utf8(completed.stdout).expect("UTF-8 CLI summary");
    assert_eq!(stdout.lines().count(), 1);
    serde_json::from_str(stdout.trim_end()).expect("JSON CLI summary")
}

#[test]
fn uhc_retain_cli_emits_one_machine_readable_summary() {
    let fixture = br#"[
{"id":1,"addresses":[{"city":"Phoenix"}],"negative":-42,"decimal":3.125,"empty":null,"escaped":"provider-\u2603"},
{"id":2,"addresses":[{"city":"Mesa"}]},
{"id":3,"addresses":[{"city":"Tempe"}]},
{"id":4,"addresses":[{"city":"Tucson"}]}
]"#;
    let directory = tempfile::tempdir().expect("temporary test root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    fs::write(&source, fixture).expect("write UHC fixture");
    fs::create_dir(&retained).expect("create retained root");
    let sha256 = sha256_hex(fixture);

    let summary = summary_from_success(run_retain_cli(
        &source,
        &retained,
        &sha256,
        fixture.len(),
        4,
    ));
    assert_eq!(summary["record_kind"], "uhc_retained_summary");
    assert_eq!(
        summary["contract_id"],
        "healthporta.uhc.retained-json-ranges.v2"
    );
    assert_eq!(summary["contract_version"], 2);
    assert_eq!(
        summary["canonicalization_id"],
        "json-object-remove-crlf-append-lf.v1"
    );
    assert_eq!(summary["raw_artifact_sha256"], sha256);
    assert_eq!(summary["raw_artifact_byte_count"], fixture.len() as u64);
    assert_eq!(summary["record_count"], 4);
    assert_eq!(summary["range_count"], 4);
    assert_eq!(summary["raw_reused"], false);
    assert_eq!(summary["manifest_reused"], false);
    assert!(summary["timings_seconds"]["total"]
        .as_f64()
        .is_some_and(|seconds| seconds > 0.0));
    for field in ["raw_artifact_path", "manifest_path"] {
        assert!(
            fs::metadata(summary[field].as_str().expect("artifact path"))
                .expect("published artifact")
                .is_file()
        );
    }
}

#[test]
fn uhc_retain_cli_reuses_verified_artifacts_without_the_source() {
    let fixture = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#;
    let directory = tempfile::tempdir().expect("temporary test root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    fs::write(&source, fixture).expect("write UHC fixture");
    fs::create_dir(&retained).expect("create retained root");
    let sha256 = sha256_hex(fixture);
    let request = UHCRetainRequest {
        source_path: source.clone(),
        output_root: retained.clone(),
        expected_sha256: sha256,
        expected_byte_count: fixture.len() as u64,
        range_count: 4,
    };

    retain_uhc_artifact(&request).expect("initial retained admission");
    fs::remove_file(&source).expect("remove original source");
    let reused = retain_uhc_artifact(&request).expect("reuse retained admission");
    assert_eq!(reused.record_count, 4);
    assert!(reused.raw_reused);
    assert!(reused.manifest_reused);
}

#[test]
fn uhc_retain_cli_exercises_verified_large_fanout_fallback() {
    let fixture = (0..9)
        .map(|ordinal| format!(r#"{{"id":{ordinal}}}"#))
        .collect::<Vec<_>>()
        .join(",");
    let fixture = format!("[{fixture}]");
    let fixture = fixture.as_bytes();
    let directory = tempfile::tempdir().expect("temporary test root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    fs::write(&source, fixture).expect("write UHC fixture");
    fs::create_dir(&retained).expect("create retained root");
    let sha256 = sha256_hex(fixture);

    let summary = retain_uhc_artifact(&UHCRetainRequest {
        source_path: source,
        output_root: retained,
        expected_sha256: sha256,
        expected_byte_count: fixture.len() as u64,
        range_count: 9,
    })
    .expect("retain nine-range fixture");
    assert_eq!(summary.record_count, 9);
    assert_eq!(summary.range_count, 9);
    assert!(summary.timings_seconds.range_verification > 0.0);
}
