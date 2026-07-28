use ptg2_scanner::uhc_retained::{
    retain_uhc_artifact, UHCRetainRequest, UHCRetainSummary, UHCRetainedManifest,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const PROVIDER_FIXTURE: &[u8] = br#"[
{"type":"INDIVIDUAL","npi":"1003821380","name":{"first":"Ada","middle":null,"last":"Lovelace"},"facility_name":null,"facility_type":null,"gender":"F","accepting":"accepting","addresses":[{"address":"1 Main St","city":"Chicago","state":"IL","zip":"60601","phone":"3125551212"}],"plans":[{"plan_id_type":"HIOS-PLAN-ID","plan_id":"12345IL0010001","years":[2026],"network_tier":"PREFERRED"}],"specialty":["Family Medicine"],"last_updated_on":"2026-07-01"},
{"type":"INDIVIDUAL","npi":"1003821380","name":{"first":"Ada","middle":null,"last":"Lovelace"},"facility_name":null,"facility_type":null,"gender":"F","accepting":"accepting","addresses":[{"address":"1 Main St","city":"Chicago","state":"IL","zip":"60601","phone":"3125551212"}],"plans":[{"plan_id_type":"HIOS-PLAN-ID","plan_id":"12345IL0010001","years":[2026],"network_tier":"PREFERRED"}],"specialty":["Family Medicine"],"last_updated_on":"2026-07-01"},
{"type":"INDIVIDUAL","npi":"1003821380","name":{"first":"Ada","middle":null,"last":"Lovelace"},"facility_name":null,"facility_type":null,"gender":"F","accepting":"accepting","addresses":[{"address":"1 Main St","city":"Chicago","state":"IL","zip":"60601","phone":"3125551212"}],"plans":[{"plan_id_type":"HIOS-PLAN-ID","plan_id":"12345IL0010001","years":[2026],"network_tier":"PREFERRED"}],"specialty":["Family Medicine"],"last_updated_on":"2026-07-01"},
{"type":"INDIVIDUAL","npi":"1003821380","name":{"first":"Ada","middle":null,"last":"Lovelace"},"facility_name":null,"facility_type":null,"gender":"F","accepting":"accepting","addresses":[{"address":"1 Main St","city":"Chicago","state":"IL","zip":"60601","phone":"3125551212"}],"plans":[{"plan_id_type":"HIOS-PLAN-ID","plan_id":"12345IL0010001","years":[2026],"network_tier":"PREFERRED"}],"specialty":["Family Medicine"],"last_updated_on":"2026-07-01"}
]"#;

struct RetainedFixture {
    _directory: tempfile::TempDir,
    summary: UHCRetainSummary,
    manifest: UHCRetainedManifest,
    output: PathBuf,
}

fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    Sha256::digest(bytes)
        .iter()
        .flat_map(|byte| {
            [
                char::from(HEX[usize::from(byte >> 4)]),
                char::from(HEX[usize::from(byte & 0x0f)]),
            ]
        })
        .collect()
}

fn retained_fixture() -> RetainedFixture {
    let directory = tempfile::tempdir().expect("temporary semantic CLI root");
    let source = directory.path().join("source.json");
    let retained = directory.path().join("retained");
    fs::write(&source, PROVIDER_FIXTURE).expect("write semantic CLI fixture");
    fs::create_dir(&retained).expect("create retained root");
    let summary = retain_uhc_artifact(&UHCRetainRequest {
        source_path: source,
        output_root: retained,
        expected_sha256: sha256_hex(PROVIDER_FIXTURE),
        expected_byte_count: PROVIDER_FIXTURE.len() as u64,
        range_count: 4,
    })
    .expect("retain semantic CLI fixture");
    let manifest =
        serde_json::from_slice(&fs::read(&summary.manifest_path).expect("read retained manifest"))
            .expect("decode retained manifest");
    let output = directory.path().join("semantic.copy");
    RetainedFixture {
        _directory: directory,
        summary,
        manifest,
        output,
    }
}

fn semantic_arguments(fixture: &RetainedFixture, output: &Path) -> Vec<String> {
    vec![
        "--input".to_owned(),
        fixture.summary.raw_artifact_path.clone(),
        "--manifest".to_owned(),
        fixture.summary.manifest_path.clone(),
        "--output".to_owned(),
        output.to_str().expect("UTF-8 output path").to_owned(),
        "--artifact-sha256".to_owned(),
        fixture.summary.raw_artifact_sha256.clone(),
        "--artifact-byte-count".to_owned(),
        fixture.summary.raw_artifact_byte_count.to_string(),
        "--manifest-sha256".to_owned(),
        fixture.summary.manifest_sha256.clone(),
        "--range-set-sha256".to_owned(),
        fixture.manifest.range_set_sha256.clone(),
        "--record-count".to_owned(),
        fixture.summary.record_count.to_string(),
        "--range-count".to_owned(),
        fixture.summary.range_count.to_string(),
        "--source-file-id".to_owned(),
        fixture.summary.raw_artifact_sha256.clone(),
        "--source-binding-id".to_owned(),
        "synthetic/provider-membership".to_owned(),
        "--collection-kind".to_owned(),
        "provider_membership".to_owned(),
        "--workers".to_owned(),
        "2".to_owned(),
        "--per-worker-memory-bytes".to_owned(),
        (4 * 1024 * 1024).to_string(),
        "--total-memory-bytes".to_owned(),
        (10 * 1024 * 1024).to_string(),
        "--max-record-bytes".to_owned(),
        (16 * 1024).to_string(),
        "--evidence-buffer-bytes".to_owned(),
        (64 * 1024).to_string(),
    ]
}

fn run_semantic(arguments: &[String]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_uhc_semantic_facts"))
        .args(arguments)
        .output()
        .expect("run UHC semantic facts CLI")
}

fn assert_failed(output: Output, expected: &str) {
    assert_eq!(output.status.code(), Some(2));
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(expected),
        "stderr did not contain {expected:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn semantic_cli_streams_or_publishes_copy_with_the_same_sealed_report() {
    let fixture = retained_fixture();
    let streamed = run_semantic(&semantic_arguments(&fixture, Path::new("-")));
    assert!(
        streamed.status.success(),
        "streaming CLI failed: {}",
        String::from_utf8_lossy(&streamed.stderr)
    );
    assert!(streamed.stdout.starts_with(b"PGCOPY\n\xff\r\n\0"));
    let streamed_report: Value =
        serde_json::from_slice(&streamed.stderr).expect("streamed JSON report");
    assert_eq!(streamed_report["fact_count"], 4);
    assert_eq!(streamed_report["evidence_count"], 4);
    assert_eq!(
        streamed_report["encoder_sha256"]
            .as_str()
            .expect("encoder digest")
            .len(),
        64
    );

    let published = run_semantic(&semantic_arguments(&fixture, &fixture.output));
    assert!(
        published.status.success(),
        "file CLI failed: {}",
        String::from_utf8_lossy(&published.stderr)
    );
    assert!(published.stderr.is_empty());
    assert!(fs::read(&fixture.output)
        .expect("published semantic COPY")
        .starts_with(b"PGCOPY\n\xff\r\n\0"));
    let published_report: Value =
        serde_json::from_slice(&published.stdout).expect("published JSON report");
    assert_eq!(
        published_report["fact_set_sha256"],
        streamed_report["fact_set_sha256"]
    );
    assert_eq!(
        published_report["record_identity_set_sha256"],
        streamed_report["record_identity_set_sha256"]
    );

    assert_failed(
        run_semantic(&semantic_arguments(&fixture, &fixture.output)),
        "output already exists",
    );
}

#[test]
fn semantic_cli_rejects_incomplete_duplicate_unknown_and_invalid_arguments() {
    assert_failed(run_semantic(&[]), "--input is required");
    assert_failed(
        run_semantic(&["--input".to_owned()]),
        "missing value for --input",
    );
    assert_failed(
        run_semantic(&[
            "--input".to_owned(),
            "first".to_owned(),
            "--input".to_owned(),
            "second".to_owned(),
        ]),
        "duplicate argument: --input",
    );

    let fixture = retained_fixture();
    let valid = semantic_arguments(&fixture, Path::new("-"));
    for required in [
        "--input",
        "--manifest",
        "--output",
        "--artifact-sha256",
        "--artifact-byte-count",
        "--manifest-sha256",
        "--range-set-sha256",
        "--record-count",
        "--range-count",
        "--source-file-id",
        "--source-binding-id",
        "--collection-kind",
    ] {
        let mut incomplete = valid.clone();
        let index = incomplete
            .iter()
            .position(|value| value == required)
            .expect("required flag");
        incomplete.drain(index..=index + 1);
        assert_failed(
            run_semantic(&incomplete),
            &format!("{required} is required"),
        );
    }

    let mut unknown = valid.clone();
    unknown.extend(["--future-flag".to_owned(), "value".to_owned()]);
    assert_failed(run_semantic(&unknown), "unknown argument: --future-flag");

    let mut unsupported = valid.clone();
    let kind = unsupported
        .iter()
        .position(|value| value == "--collection-kind")
        .expect("collection kind flag");
    unsupported[kind + 1] = "commercial".to_owned();
    assert_failed(
        run_semantic(&unsupported),
        "--collection-kind is unsupported",
    );

    let mut bad_count = valid;
    let count = bad_count
        .iter()
        .position(|value| value == "--record-count")
        .expect("record count flag");
    bad_count[count + 1] = "four".to_owned();
    assert_failed(
        run_semantic(&bad_count),
        "--record-count must be an unsigned integer",
    );

    let mut bad_workers = semantic_arguments(&fixture, Path::new("-"));
    let workers = bad_workers
        .iter()
        .position(|value| value == "--workers")
        .expect("workers flag");
    bad_workers[workers + 1] = "many".to_owned();
    assert_failed(
        run_semantic(&bad_workers),
        "--workers must be an unsigned integer",
    );
    for (flag, expected) in [
        (
            "--artifact-byte-count",
            "--artifact-byte-count must be an unsigned integer",
        ),
        ("--range-count", "--range-count must be an unsigned integer"),
        (
            "--per-worker-memory-bytes",
            "--per-worker-memory-bytes must be an unsigned integer",
        ),
        (
            "--total-memory-bytes",
            "--total-memory-bytes must be an unsigned integer",
        ),
        (
            "--max-record-bytes",
            "--max-record-bytes must be an unsigned integer",
        ),
        (
            "--evidence-buffer-bytes",
            "--evidence-buffer-bytes must be an unsigned integer",
        ),
    ] {
        let mut invalid_number = semantic_arguments(&fixture, Path::new("-"));
        let index = invalid_number
            .iter()
            .position(|value| value == flag)
            .expect("numeric flag");
        invalid_number[index + 1] = "invalid".to_owned();
        assert_failed(run_semantic(&invalid_number), expected);
    }

    let mut defaults_and_plan_kind = semantic_arguments(&fixture, Path::new("-"));
    for flag in [
        "--workers",
        "--per-worker-memory-bytes",
        "--total-memory-bytes",
        "--max-record-bytes",
        "--evidence-buffer-bytes",
    ] {
        let index = defaults_and_plan_kind
            .iter()
            .position(|value| value == flag)
            .expect("optional budget flag");
        defaults_and_plan_kind.drain(index..=index + 1);
    }
    let kind = defaults_and_plan_kind
        .iter()
        .position(|value| value == "--collection-kind")
        .expect("collection kind flag");
    defaults_and_plan_kind[kind + 1] = "plan_reference".to_owned();
    assert_failed(
        run_semantic(&defaults_and_plan_kind),
        "invalid retained UHC plan JSON",
    );

    let missing_parent = fixture
        ._directory
        .path()
        .join("missing")
        .join("semantic.copy");
    assert_failed(
        run_semantic(&semantic_arguments(&fixture, &missing_parent)),
        "No such file or directory",
    );

    let mut missing_input = semantic_arguments(&fixture, Path::new("-"));
    let input = missing_input
        .iter()
        .position(|value| value == "--input")
        .expect("input flag");
    missing_input[input + 1] = fixture
        ._directory
        .path()
        .join("missing-input.json")
        .to_str()
        .expect("UTF-8 missing input")
        .to_owned();
    assert_failed(run_semantic(&missing_input), "No such file or directory");

    let mut missing_manifest = semantic_arguments(&fixture, Path::new("-"));
    let manifest = missing_manifest
        .iter()
        .position(|value| value == "--manifest")
        .expect("manifest flag");
    missing_manifest[manifest + 1] = fixture
        ._directory
        .path()
        .join("missing-manifest.json")
        .to_str()
        .expect("UTF-8 missing manifest")
        .to_owned();
    assert_failed(run_semantic(&missing_manifest), "No such file or directory");
}
