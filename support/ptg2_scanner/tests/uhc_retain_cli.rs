use ptg2_scanner::uhc_retained::{retain_uhc_artifact, UHCRetainRequest};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;
use std::process::Command;

fn request(source: &Path, retained: &Path, fixture: &[u8], ranges: usize) -> UHCRetainRequest {
    UHCRetainRequest {
        source_path: source.to_path_buf(),
        output_root: retained.to_path_buf(),
        expected_sha256: sha256_hex(fixture),
        expected_byte_count: fixture.len() as u64,
        range_count: ranges,
    }
}

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

#[test]
fn uhc_retain_rejects_request_and_cli_contract_violations() {
    let directory = tempfile::tempdir().expect("temporary validation root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    let fixture = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#;
    fs::write(&source, fixture).expect("write validation fixture");
    fs::create_dir(&retained).expect("create retained root");

    for (sha256, byte_count, range_count, expected) in [
        ("bad".to_owned(), fixture.len() as u64, 4, "64 lowercase"),
        ("A".repeat(64), fixture.len() as u64, 4, "64 lowercase"),
        ("0".repeat(64), 0, 4, "1..=i64::MAX"),
        ("0".repeat(64), i64::MAX as u64 + 1, 4, "1..=i64::MAX"),
        ("0".repeat(64), fixture.len() as u64, 3, "4..=256"),
        ("0".repeat(64), fixture.len() as u64, 257, "4..=256"),
    ] {
        let error = retain_uhc_artifact(&UHCRetainRequest {
            source_path: source.clone(),
            output_root: retained.clone(),
            expected_sha256: sha256,
            expected_byte_count: byte_count,
            range_count,
        })
        .expect_err("invalid retain request must fail");
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    for arguments in [
        vec!["--uhc-retain"],
        vec!["--uhc-retain", "one", "two", "three", "bad", "4"],
        vec!["--uhc-retain", "one", "two", "three", "4", "bad"],
    ] {
        let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
            .args(arguments)
            .output()
            .expect("run invalid retain CLI");
        assert!(!completed.status.success());
    }
}

#[test]
fn uhc_retain_rejects_malformed_json_framing_and_digest_mismatch() {
    let malformed: &[&[u8]] = &[
        b"{}",
        b"[1]",
        b"[null]",
        b"[true]",
        b"[{}",
        b"[{}] trailing",
        b"[{},]",
        b"[,{}]",
        b"[{\"id\":1} {\"id\":2}]",
        b"[{\"id\":\"unterminated}]",
        b"[{\"id\":1}] [{\"id\":2}]",
        b"[]",
    ];
    for (ordinal, fixture) in malformed.iter().enumerate() {
        let directory = tempfile::tempdir().expect("temporary malformed root");
        let source = directory.path().join(format!("source-{ordinal}.json"));
        let retained = directory.path().join("retained");
        fs::write(&source, fixture).expect("write malformed fixture");
        fs::create_dir(&retained).expect("create retained root");
        assert!(
            retain_uhc_artifact(&request(&source, &retained, fixture, 4)).is_err(),
            "malformed fixture {ordinal} was accepted"
        );
    }

    let fixture = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#;
    let directory = tempfile::tempdir().expect("temporary digest root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    fs::write(&source, fixture).expect("write digest fixture");
    fs::create_dir(&retained).expect("create retained root");

    let mut wrong_length = request(&source, &retained, fixture, 4);
    wrong_length.expected_byte_count += 1;
    assert!(retain_uhc_artifact(&wrong_length)
        .expect_err("byte-count mismatch must fail")
        .to_string()
        .contains("byte count changed"));

    let mut wrong_digest = request(&source, &retained, fixture, 4);
    wrong_digest.expected_sha256 = "0".repeat(64);
    assert!(retain_uhc_artifact(&wrong_digest)
        .expect_err("digest mismatch must fail")
        .to_string()
        .contains("SHA-256"));
}

#[cfg(unix)]
#[test]
fn uhc_retain_rejects_unsafe_paths_and_corrupt_reused_artifacts() {
    use std::os::unix::fs::symlink;

    let fixture = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#;

    let missing_root = tempfile::tempdir().expect("temporary missing-root fixture");
    let source = missing_root.path().join("source.json");
    fs::write(&source, fixture).expect("write source");
    let missing = missing_root.path().join("missing");
    assert!(retain_uhc_artifact(&request(&source, &missing, fixture, 4)).is_err());

    let file_root = tempfile::tempdir().expect("temporary file-root fixture");
    let source = file_root.path().join("source.json");
    let retained_file = file_root.path().join("retained-file");
    fs::write(&source, fixture).expect("write source");
    fs::write(&retained_file, b"not a directory").expect("write retained file");
    assert!(retain_uhc_artifact(&request(&source, &retained_file, fixture, 4)).is_err());

    let symlink_root = tempfile::tempdir().expect("temporary symlink-root fixture");
    let source = symlink_root.path().join("source.json");
    let real_root = symlink_root.path().join("real-retained");
    let retained_link = symlink_root.path().join("retained-link");
    fs::write(&source, fixture).expect("write source");
    fs::create_dir(&real_root).expect("create real retained root");
    symlink(&real_root, &retained_link).expect("link retained root");
    assert!(retain_uhc_artifact(&request(&source, &retained_link, fixture, 4)).is_err());

    let source_link_root = tempfile::tempdir().expect("temporary source-link fixture");
    let source = source_link_root.path().join("source.json");
    let source_link = source_link_root.path().join("source-link.json");
    let retained = source_link_root.path().join("retained");
    fs::write(&source, fixture).expect("write source");
    symlink(&source, &source_link).expect("link source");
    fs::create_dir(&retained).expect("create retained root");
    assert!(retain_uhc_artifact(&request(&source_link, &retained, fixture, 4)).is_err());

    let corrupt_manifest_root = tempfile::tempdir().expect("temporary corrupt-manifest fixture");
    let source = corrupt_manifest_root.path().join("source.json");
    let retained = corrupt_manifest_root.path().join("retained");
    fs::write(&source, fixture).expect("write source");
    fs::create_dir(&retained).expect("create retained root");
    let valid = retain_uhc_artifact(&request(&source, &retained, fixture, 4))
        .expect("publish retained fixture");
    fs::write(&valid.manifest_path, b"not-json").expect("corrupt retained manifest");
    assert!(retain_uhc_artifact(&request(&source, &retained, fixture, 4)).is_err());

    let corrupt_raw_root = tempfile::tempdir().expect("temporary corrupt-raw fixture");
    let source = corrupt_raw_root.path().join("source.json");
    let retained = corrupt_raw_root.path().join("retained");
    fs::write(&source, fixture).expect("write source");
    fs::create_dir(&retained).expect("create retained root");
    let valid = retain_uhc_artifact(&request(&source, &retained, fixture, 4))
        .expect("publish retained fixture");
    let mut corrupted = fixture.to_vec();
    corrupted[2] ^= 1;
    fs::write(&valid.raw_artifact_path, corrupted).expect("corrupt retained raw artifact");
    assert!(retain_uhc_artifact(&request(&source, &retained, fixture, 4)).is_err());
}
