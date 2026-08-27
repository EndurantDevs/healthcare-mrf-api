use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const RAW_MRF: &[u8] = include_bytes!("fixtures/compact_v4_mrf.json");

fn sha256_hex(payload: &[u8]) -> String {
    lower_hex(&Sha256::digest(payload))
}

fn lower_hex(payload: &[u8]) -> String {
    payload.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn invalid_price_expectation(
    raw_source_sha256: &str,
    entries: &[(u64, u64, u64, &str)],
    emptied_rate_count: u64,
) -> serde_json::Value {
    let mut source_digest = Sha256::new();
    source_digest.update(b"PTG2_INVALID_PRICE_EXCLUSION_SOURCE_V1\0");
    let mut entries = entries.to_vec();
    entries.sort_unstable_by_key(|entry| (entry.0, entry.1, entry.2));
    let entries = entries
        .iter()
        .map(
            |&(object_ordinal, rate_ordinal, price_ordinal, invalid_value)| {
                let mut value_digest = Sha256::new();
                value_digest.update(b"PTG2_INVALID_PRICE_EXCLUSION_VALUE_V1\0");
                value_digest.update(invalid_value.as_bytes());
                let value_digest = value_digest.finalize();
                source_digest.update(object_ordinal.to_be_bytes());
                source_digest.update(rate_ordinal.to_be_bytes());
                source_digest.update(price_ordinal.to_be_bytes());
                source_digest.update(value_digest);
                serde_json::json!({
                    "object_ordinal": object_ordinal,
                    "rate_ordinal": rate_ordinal,
                    "price_ordinal": price_ordinal,
                    "invalid_value_sha256": lower_hex(&value_digest),
                })
            },
        )
        .collect::<Vec<_>>();
    serde_json::json!({
        "contract": "ptg2_invalid_price_exclusion_source_v1",
        "reason": "invalid_iso_calendar_date",
        "raw_source_sha256": raw_source_sha256,
        "excluded_price_count": entries.len(),
        "emptied_rate_count": emptied_rate_count,
        "entries": entries,
        "sha256": lower_hex(&source_digest.finalize()),
    })
}

fn compact_exclusion_command(
    source: &std::path::Path,
    output: &std::path::Path,
    serving: &std::path::Path,
    witness_scratch: &std::path::Path,
    raw_source_sha256: &str,
    expectation: &serde_json::Value,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"));
    command
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "44".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", raw_source_sha256)
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", witness_scratch)
        .env(
            "HLTHPRT_PTG2_INVALID_PRICE_EXCLUSION_JSON",
            expectation.to_string(),
        )
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
            output.join("price-atom.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "1")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "1")
        .env("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false");
    command
}

fn framed_payload(stdout: &[u8], target: &str) -> serde_json::Value {
    let mut offset = 0usize;
    while offset < stdout.len() {
        let header_end = stdout[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|relative| offset + relative)
            .expect("framed output header");
        let header = std::str::from_utf8(&stdout[offset..header_end]).expect("UTF-8 header");
        let (kind, length) = header
            .split_once('\t')
            .expect("framed output kind and length");
        let length: usize = length.parse().expect("framed output length");
        let payload_start = header_end + 1;
        let payload_end = payload_start + length;
        if kind == target {
            return serde_json::from_slice(&stdout[payload_start..payload_end])
                .expect("JSON frame payload");
        }
        offset = payload_end + usize::from(stdout.get(payload_end) == Some(&b'\n'));
    }
    panic!("missing {target} frame")
}

fn source_witness_header(path: &std::path::Path) -> serde_json::Value {
    let bundle = fs::read(path).expect("read source witness bundle");
    assert_eq!(&bundle[..8], b"PTG2SW03");
    let header_length = u32::from_be_bytes(bundle[8..12].try_into().unwrap()) as usize;
    serde_json::from_slice(&bundle[12..12 + header_length]).expect("source witness header")
}

#[test]
fn legacy_compact_scan_exercises_direct_provider_and_price_projection() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");
    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    fixture["in_network"][0]["ignored_nested_extension"] =
        serde_json::json!({"items": [{"value": 1}]});
    fixture["provider_references"][0]["provider_groups"]
        .as_array_mut()
        .expect("provider groups")
        .push(serde_json::json!({
            "tin": {"type": "ein", "value": "111223333"},
            "npi": [1234567893]
        }));
    let mixed_rate = serde_json::json!({
        "provider_references": [7],
        "provider_groups": [{
            "tin": {"type": "ein", "value": "444556666"},
            "npi": [1234567894]
        }],
        "negotiated_prices": [{
            "negotiated_type": "negotiated",
            "negotiated_rate": 101
        }]
    });
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    for _ in 0..256 {
        rates.push(mixed_rate.clone());
    }
    fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
        .push(serde_json::json!({
            "billing_code_type": " ",
            "billing_code": " ",
            "negotiated_rates": [mixed_rate]
        }));
    for record in fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
    {
        record
            .as_object_mut()
            .expect("in-network object")
            .remove("negotiation_arrangement");
    }
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "11".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&raw))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "1")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "1")
        .env("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false")
        .output()
        .expect("run legacy compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    assert!(!completed.stdout.is_empty());
    let stdout = String::from_utf8(completed.stdout).expect("UTF-8 scanner output");
    assert!(stdout.lines().any(|line| {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|payload| payload["work_queue_blocked_sends"].as_u64())
            .is_some_and(|blocked| blocked > 0)
    }));
    assert!(!fs::read_dir(serving)
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());
}

#[test]
fn legacy_compact_scan_rejects_invalid_expiration_without_terminal_outputs() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");
    let baseline = serving.join("ptg2-v3-serving-baseline.ready");
    fs::write(&baseline, b"baseline").expect("seed serving baseline");

    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    let mut valid_rate = rates[0].clone();
    valid_rate["negotiated_prices"][0]["expiration_date"] = serde_json::json!("2028-02-29");
    let mut invalid_rate = valid_rate.clone();
    invalid_rate["negotiated_prices"][0]["expiration_date"] = serde_json::json!("2027-02-30");
    rates.clear();
    rates.extend([valid_rate, invalid_rate]);
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "33".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&raw))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
            output.join("price-atom.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "1")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "1")
        .env("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false")
        .output()
        .expect("run legacy compact scanner");

    assert!(!completed.status.success());
    assert!(String::from_utf8_lossy(&completed.stderr)
        .contains("expiration_date must be an exact ISO calendar date"));
    for terminal_kind in [
        b"manifest_price_atom_copy_file\t".as_slice(),
        b"scanner_summary\t".as_slice(),
        b"source_audit_witness_file\t".as_slice(),
        b"v3_serving_code_dictionary_file\t".as_slice(),
        b"v3_serving_run_partition_file\t".as_slice(),
    ] {
        assert!(!completed
            .stdout
            .windows(terminal_kind.len())
            .any(|window| window == terminal_kind));
    }
    assert_eq!(fs::read(&baseline).unwrap(), b"baseline");
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
    assert!(!fs::read_dir(&serving)
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            path != baseline
                && path.is_file()
                && (name.starts_with(".ptg2-v3-serving-")
                    || name.starts_with("ptg2-v3-serving-")
                    || name.starts_with("ptg2-v3-source-witness-"))
        }));
}

#[test]
fn legacy_compact_scan_applies_only_the_exact_invalid_price_exclusion() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");

    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    let rate = &mut fixture["in_network"][0]["negotiated_rates"][0];
    rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"},
        {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"},
        {"negotiated_type": "negotiated", "negotiated_rate": 12, "expiration_date": "2029-03-01"}
    ]);
    for record in fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
    {
        record
            .as_object_mut()
            .expect("in-network object")
            .remove("negotiation_arrangement");
    }
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");
    let raw_source_sha256 = sha256_hex(&raw);
    let expectation = invalid_price_expectation(&raw_source_sha256, &[(0, 0, 1, "2027-02-30")], 0);

    let completed = compact_exclusion_command(
        &source,
        &output,
        &serving,
        &witness_scratch,
        &raw_source_sha256,
        &expectation,
    )
    .output()
    .expect("run legacy compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let summary = framed_payload(&completed.stdout, "scanner_summary");
    assert_eq!(
        summary["invalid_price_exclusion"],
        serde_json::json!({
            "contract": "ptg2_invalid_price_exclusion_source_v1",
            "reason": "invalid_iso_calendar_date",
            "excluded_price_count": 1,
            "emptied_rate_count": 0,
            "sha256": expectation["sha256"],
        })
    );
    let price_copy = fs::read_dir(&output)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains("price-atom")
        })
        .expect("price atom output");
    let price_copy = fs::read_to_string(price_copy).expect("UTF-8 price atom COPY");
    assert!(price_copy.contains("2028-02-29"));
    assert!(price_copy.contains("2029-03-01"));
    assert!(!price_copy.contains("2027-02-30"));
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
}

#[test]
fn invalid_price_exclusion_mismatch_is_failure_atomic() {
    for case in [
        "extra_expected_price",
        "wrong_emptied_rate_count",
        "missing_in_network",
    ] {
        let temporary = tempfile::tempdir().expect("temporary fixture root");
        let source = temporary.path().join("rates.json");
        let output = temporary.path().join("output");
        let serving = output.join("serving");
        let witness_scratch = output.join("witness-scratch");
        fs::create_dir(&output).expect("create output directory");
        fs::create_dir(&serving).expect("create serving directory");
        fs::create_dir(&witness_scratch).expect("create witness scratch directory");
        let output_baseline = output.join("baseline.keep");
        let serving_baseline = serving.join("ptg2-v3-serving-baseline.ready");
        fs::write(&output_baseline, b"output baseline").expect("seed output baseline");
        fs::write(&serving_baseline, b"serving baseline").expect("seed serving baseline");

        let mut fixture: serde_json::Value =
            serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
        if case == "missing_in_network" {
            fixture
                .as_object_mut()
                .expect("source object")
                .remove("in_network");
        } else {
            fixture["in_network"][0]["negotiated_rates"][0]["negotiated_prices"] = serde_json::json!([
                {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"},
                {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"},
                {"negotiated_type": "negotiated", "negotiated_rate": 12, "expiration_date": "2029-03-01"}
            ]);
            for record in fixture["in_network"]
                .as_array_mut()
                .expect("in-network records")
            {
                record
                    .as_object_mut()
                    .expect("in-network object")
                    .remove("negotiation_arrangement");
            }
        }
        let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
        fs::write(&source, &raw).expect("write compact source fixture");
        let raw_source_sha256 = sha256_hex(&raw);
        let expectation = match case {
            "extra_expected_price" => invalid_price_expectation(
                &raw_source_sha256,
                &[(0, 0, 1, "2027-02-30"), (0, 0, 3, "2027-02-31")],
                0,
            ),
            "wrong_emptied_rate_count" => {
                invalid_price_expectation(&raw_source_sha256, &[(0, 0, 1, "2027-02-30")], 1)
            }
            "missing_in_network" => {
                invalid_price_expectation(&raw_source_sha256, &[(0, 0, 0, "2027-02-30")], 0)
            }
            _ => unreachable!(),
        };
        let completed = compact_exclusion_command(
            &source,
            &output,
            &serving,
            &witness_scratch,
            &raw_source_sha256,
            &expectation,
        )
        .env("HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES", "1")
        .env("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "1")
        .output()
        .expect("run mismatched exclusion scanner");

        assert!(!completed.status.success(), "{case}");
        assert!(
            String::from_utf8_lossy(&completed.stderr)
                .contains("observed invalid price exclusions do not match the exact expectation"),
            "{case}: {}",
            String::from_utf8_lossy(&completed.stderr),
        );
        assert!(String::from_utf8_lossy(&completed.stderr).contains("PTG2_SCANNER_PROGRESS"));
        assert!(completed
            .stdout
            .windows(b"scanner_config\t".len())
            .any(|window| window == b"scanner_config\t"));
        for terminal_marker in [
            b"_copy_file\t".as_slice(),
            b"v3_serving_run_partition_file\t".as_slice(),
            b"v3_serving_code_dictionary_file\t".as_slice(),
            b"scanner_summary\t".as_slice(),
            b"source_audit_witness_file\t".as_slice(),
        ] {
            assert!(
                !completed
                    .stdout
                    .windows(terminal_marker.len())
                    .any(|window| window == terminal_marker),
                "{case}: {}",
                String::from_utf8_lossy(&completed.stdout),
            );
        }
        assert_eq!(fs::read(&output_baseline).unwrap(), b"output baseline");
        assert_eq!(fs::read(&serving_baseline).unwrap(), b"serving baseline");
        assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
        let mut output_entries = fs::read_dir(&output)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>();
        output_entries.sort();
        assert_eq!(
            output_entries,
            ["baseline.keep", "serving", "witness-scratch"]
                .map(std::ffi::OsString::from)
                .to_vec(),
            "{case}",
        );
        assert_eq!(
            fs::read_dir(&serving)
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect::<Vec<_>>(),
            vec![serving_baseline],
            "{case}",
        );
    }
}

#[test]
fn all_invalid_price_exclusion_records_one_unqueryable_rate() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");

    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
        .truncate(1);
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    let mut valid_rate = rates[0].clone();
    valid_rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"}
    ]);
    let mut excluded_rate = rates[0].clone();
    excluded_rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"}
    ]);
    rates.clear();
    rates.extend([valid_rate, excluded_rate]);
    fixture["in_network"][0]
        .as_object_mut()
        .expect("in-network object")
        .remove("negotiation_arrangement");
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");
    let raw_source_sha256 = sha256_hex(&raw);
    let expectation = invalid_price_expectation(&raw_source_sha256, &[(0, 1, 0, "2027-02-30")], 1);

    let completed = compact_exclusion_command(
        &source,
        &output,
        &serving,
        &witness_scratch,
        &raw_source_sha256,
        &expectation,
    )
    .env("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "1")
    .output()
    .expect("run all-invalid exclusion scanner");
    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let summary = framed_payload(&completed.stdout, "scanner_summary");
    assert_eq!(
        summary["invalid_price_exclusion"]["excluded_price_count"],
        1
    );
    assert_eq!(summary["invalid_price_exclusion"]["emptied_rate_count"], 1);
    assert_eq!(
        framed_payload(&completed.stdout, "v3_serving_run_partition_file")["row_count"],
        1,
    );
    let witness = framed_payload(&completed.stdout, "source_audit_witness_file");
    let witness_header = source_witness_header(std::path::Path::new(
        witness["path"].as_str().expect("witness path"),
    ));
    assert_eq!(
        witness_header["rate_occurrence"]["emitted_rate_row_count"],
        2,
    );
    assert_eq!(
        witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
        1,
    );
    let price_copy = fs::read_dir(&output)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("price-atom"))
        })
        .expect("price atom output");
    let price_copy = fs::read_to_string(price_copy).expect("UTF-8 price atom COPY");
    assert_eq!(price_copy.lines().count(), 1);
    assert!(price_copy.contains("2028-02-29"));
    assert!(!price_copy.contains("2027-02-30"));
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
}

#[cfg(unix)]
#[test]
fn gzip_scan_indexes_and_reorders_in_network_before_provider_references() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json.gz");
    let rapidgzip = temporary.path().join("rapidgzip");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");

    let fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact fixture");
    let reordered = serde_json::to_vec(&fixture).expect("serialize reordered fixture");
    let in_network = reordered
        .windows(b"\"in_network\"".len())
        .position(|window| window == b"\"in_network\"")
        .expect("in-network field");
    let provider_references = reordered
        .windows(b"\"provider_references\"".len())
        .position(|window| window == b"\"provider_references\"")
        .expect("provider-references field");
    assert!(in_network < provider_references);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&reordered).expect("compress fixture");
    let compressed = encoder.finish().expect("finish compressed fixture");
    fs::write(&source, &compressed).expect("write compressed fixture");
    fs::write(
        &rapidgzip,
        br#"#!/bin/sh
index=
ranges=
input=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --export-index) index="$2"; shift 2 ;;
    --import-index|--index-format|-P) shift 2 ;;
    --ranges) ranges="$2"; shift 2 ;;
    -d|-c|--verify) shift ;;
    *) input="$1"; shift ;;
  esac
done
if [ -n "$index" ]; then
  printf x > "$index"
fi
if [ -n "$ranges" ]; then
  count=${ranges%@*}
  skip=${ranges#*@}
  sleep 4
  gzip -dc "$input" | dd bs=1 skip="$skip" count="$count" 2>/dev/null
else
  gzip -dc "$input"
fi
"#,
    )
    .expect("write rapidgzip stand-in");
    fs::set_permissions(&rapidgzip, fs::Permissions::from_mode(0o700))
        .expect("make rapidgzip stand-in executable");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "22".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&compressed))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "2")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN", &rapidgzip)
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS", "2")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_INDEX_THREADS", "2")
        .env("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "2")
        .output()
        .expect("run indexed compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let stderr = String::from_utf8_lossy(&completed.stderr);
    assert!(
        stderr.lines().any(|line| {
            line.contains("progress_basis=indexed_objects")
                && line.contains("indexed_objects_completed=0")
                && line.contains("done=false")
        }),
        "{stderr}"
    );
    assert!(!completed.stdout.is_empty());
    assert!(!fs::read_dir(serving)
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());
}
