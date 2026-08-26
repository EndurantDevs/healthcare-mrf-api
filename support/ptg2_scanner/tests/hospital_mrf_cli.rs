use flate2::write::GzEncoder;
use flate2::Compression;
use serde_json::json;
use std::collections::BTreeMap;
use std::fs;
use std::io::{Cursor, Write};
use std::path::Path;
use std::process::Command;
use zip::write::SimpleFileOptions;
use zip::{CompressionMethod, ZipWriter};

const ATTESTATION_TEXT: &str = "To the best of its knowledge and belief, this hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date in the file. This hospital has included all payer-specific negotiated charges in dollars that can be expressed as a dollar amount. For payer-specific negotiated charges that cannot be expressed as a dollar amount in the machine-readable file or not knowable in advance, the hospital attests that the payer-specific negotiated charge is based on a contractual algorithm, percentage or formula that precludes the provision of a dollar amount and has provided all necessary information available to the hospital for the public to be able to derive the dollar amount, including, but not limited to, the specific fee schedule or components referenced in such percentage, algorithm or formula.";
const VERSION_ID: &str = "hospital-cli-fixture";
const COPY_KINDS: [&str; 11] = [
    "mrf",
    "location",
    "npi",
    "license",
    "contract_provision",
    "service",
    "code",
    "charge",
    "payer_charge",
    "modifier",
    "modifier_payer",
];

fn json_fixture() -> Vec<u8> {
    serde_json::to_vec(&json!({
        "hospital_name": "CLI Hospital",
        "last_updated_on": "2026-08-25",
        "version": "3.0.0",
        "location_name": ["Main"],
        "hospital_address": ["1 Main Street"],
        "type_2_npi": ["1234567890"],
        "license_information": {"license_number": "A-1", "state": "CA"},
        "attestation": {
            "attestation": ATTESTATION_TEXT,
            "confirm_attestation": true,
            "attester_name": "Alex Attester"
        },
        "standard_charge_information": [{
            "description": "MRI",
            "code_information": [{"code": "70551", "type": "CPT"}],
            "standard_charges": [{
                "setting": "outpatient",
                "billing_class": "facility",
                "gross_charge": 12,
                "minimum": 10,
                "maximum": 10,
                "payers_information": [{
                    "payer_name": "Payer",
                    "plan_name": "Plan",
                    "standard_charge_dollar": 10,
                    "methodology": "fee schedule"
                }]
            }]
        }],
        "modifier_information": [{
            "code": "25",
            "description": "Professional component",
            "setting": "outpatient",
            "modifier_payer_information": [{
                "payer_name": "Payer",
                "plan_name": "Plan",
                "description": "Contract note"
            }]
        }]
    }))
    .unwrap()
}

fn csv_fixture(is_tall: bool) -> Vec<u8> {
    let mut data_headers = vec![
        "description",
        "code|1",
        "code|1|type",
        "modifiers",
        "setting",
        "billing_class",
        "drug_unit_of_measurement",
        "drug_type_of_measurement",
        "standard_charge|gross",
        "standard_charge|discounted_cash",
        "standard_charge|min",
        "standard_charge|max",
        "additional_generic_notes",
    ];
    let mut data_values = vec![
        "MRI",
        "70551",
        "CPT",
        "",
        "outpatient",
        "facility",
        "",
        "",
        "12",
        "",
        "8",
        "12",
        "Generic note",
    ];
    if is_tall {
        data_headers.extend([
            "payer_name",
            "plan_name",
            "standard_charge|negotiated_dollar",
            "standard_charge|negotiated_percentage",
            "standard_charge|negotiated_algorithm",
            "median_amount",
            "10th_percentile",
            "90th_percentile",
            "count",
            "standard_charge|methodology",
        ]);
        data_values.resize(data_headers.len(), "");
    } else {
        data_headers.extend([
            "standard_charge|Payer|Plan|negotiated_dollar",
            "standard_charge|Payer|Plan|negotiated_percentage",
            "standard_charge|Payer|Plan|negotiated_algorithm",
            "median_amount|Payer|Plan",
            "10th_percentile|Payer|Plan",
            "90th_percentile|Payer|Plan",
            "count|Payer|Plan",
            "standard_charge|Payer|Plan|methodology",
            "additional_payer_notes|Payer|Plan",
        ]);
        data_values.extend([
            "10",
            "",
            "",
            "10",
            "8",
            "12",
            "11",
            "fee schedule",
            "Contract note",
        ]);
    }

    let mut general_headers = vec![
        "hospital_name",
        "last_updated_on",
        "version",
        "location_name",
        "hospital_address",
        "license_number|CA",
        "type_2_npi",
        ATTESTATION_TEXT,
        "attester_name",
        "financial_aid_policy",
        "general_contract_provisions",
    ];
    let mut general_values = vec![
        "CLI Hospital",
        "2026-08-25",
        "3.0.0",
        "Main",
        "1 Main Street",
        "A-1",
        "1234567890",
        "true",
        "Alex Attester",
        "",
        "",
    ];
    general_headers.resize(data_headers.len(), "");
    general_values.resize(data_headers.len(), "");

    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .from_writer(Vec::new());
    writer.write_record(general_headers).unwrap();
    writer.write_record(general_values).unwrap();
    writer.write_record(data_headers).unwrap();
    writer.write_record(data_values).unwrap();
    writer.into_inner().unwrap()
}

fn gzip_bytes(payload: &[u8]) -> Vec<u8> {
    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(payload).unwrap();
    encoder.finish().unwrap()
}

fn zip_bytes(payload: &[u8]) -> Vec<u8> {
    let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
    writer
        .start_file(
            "prices.json",
            SimpleFileOptions::default().compression_method(CompressionMethod::Deflated),
        )
        .unwrap();
    writer.write_all(payload).unwrap();
    writer.finish().unwrap().into_inner()
}

fn run_case(
    root: &Path,
    label: &str,
    format: &str,
    extension: &str,
    payload: &[u8],
) -> BTreeMap<String, Vec<u8>> {
    let input = root.join(format!("{label}.{extension}"));
    let output = root.join(label);
    fs::write(&input, payload).unwrap();
    fs::create_dir(&output).unwrap();
    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args([
            "--hospital-mrf-copy",
            format,
            VERSION_ID,
            input.to_str().unwrap(),
            output.to_str().unwrap(),
            "2097152",
            "1048576",
        ])
        .env("HLTHPRT_HOSPITAL_MRF_MAX_FANOUT_ROWS", "128")
        .output()
        .unwrap();
    assert!(
        completed.status.success(),
        "{label} failed: {}",
        String::from_utf8_lossy(&completed.stderr)
    );
    let summary: serde_json::Value = serde_json::from_slice(&completed.stdout).unwrap();
    assert_eq!(summary["contract"], "hospital-mrf-copy-v3");
    assert_eq!(summary["version_id"], VERSION_ID);
    assert_eq!(summary["format"], format);
    assert_eq!(summary["max_fanout_rows"], 128);
    assert_eq!(summary["max_decompressed_bytes"], 2_097_152);
    assert_eq!(summary["max_output_bytes"], 1_048_576);
    assert_eq!(
        summary["artifacts"].as_array().unwrap().len(),
        COPY_KINDS.len()
    );
    assert!(summary["compressed_input_bytes"].as_u64().unwrap() > 0);
    assert!(fs::read_dir(&output).unwrap().all(|entry| !entry
        .unwrap()
        .file_name()
        .to_string_lossy()
        .starts_with('.')));
    COPY_KINDS
        .into_iter()
        .map(|kind| {
            (
                kind.to_owned(),
                fs::read(output.join(format!("{kind}.copy"))).unwrap(),
            )
        })
        .collect()
}

fn assert_cli_error(args: &[String], max_fanout_rows: &str, expected: &str) {
    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .arg("--hospital-mrf-copy")
        .args(args)
        .env("HLTHPRT_HOSPITAL_MRF_MAX_FANOUT_ROWS", max_fanout_rows)
        .output()
        .unwrap();
    assert!(!completed.status.success());
    assert!(
        String::from_utf8_lossy(&completed.stderr).contains(expected),
        "expected {expected:?} in stderr: {}",
        String::from_utf8_lossy(&completed.stderr)
    );
}

#[test]
fn production_binary_imports_json_tall_wide_gzip_and_one_member_zip() {
    let temporary = tempfile::tempdir().unwrap();
    let json = json_fixture();
    let expected = run_case(temporary.path(), "json", "json", "json", &json);
    let packed_output = temporary.path().join("packed");
    fs::create_dir(&packed_output).unwrap();
    let packed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args([
            "--hospital-mrf-copy",
            "json",
            VERSION_ID,
            temporary.path().join("json.json").to_str().unwrap(),
            packed_output.to_str().unwrap(),
            "2097152",
            "1048576",
            "packed",
        ])
        .env("HLTHPRT_HOSPITAL_MRF_MAX_FANOUT_ROWS", "128")
        .output()
        .unwrap();
    assert!(
        packed.status.success(),
        "packed import failed: {}",
        String::from_utf8_lossy(&packed.stderr)
    );
    let packed_summary: serde_json::Value = serde_json::from_slice(&packed.stdout).unwrap();
    assert_eq!(packed_summary["contract"], "hospital-mrf-copy-v3-packed-v1");
    assert_eq!(packed_summary["artifacts"].as_array().unwrap().len(), 10);
    assert_eq!(packed_summary["root"]["fact_count"], 1);
    let tall = run_case(
        temporary.path(),
        "tall",
        "csv-tall",
        "csv",
        &csv_fixture(true),
    );
    let wide = run_case(
        temporary.path(),
        "wide",
        "csv-wide",
        "csv",
        &csv_fixture(false),
    );
    for rows in [&tall, &wide] {
        assert!(!rows["mrf"].is_empty());
        assert!(!rows["service"].is_empty());
        assert!(!rows["charge"].is_empty());
    }
    let wide_charge = String::from_utf8(wide["charge"].clone()).unwrap();
    assert_eq!(
        wide_charge.trim_end().split('\t').nth(9),
        Some("Generic note")
    );
    let wide_payer = String::from_utf8(wide["payer_charge"].clone()).unwrap();
    let payer_fields = wide_payer.trim_end().split('\t').collect::<Vec<_>>();
    assert_eq!(
        &payer_fields[4..15],
        &[
            "Payer",
            "Plan",
            "10",
            "\\N",
            "\\N",
            "10",
            "8",
            "12",
            "11",
            "fee schedule",
            "Contract note"
        ]
    );
    assert_eq!(
        run_case(
            temporary.path(),
            "gzip",
            "json",
            "json.gz",
            &gzip_bytes(&json),
        ),
        expected
    );
    assert_eq!(
        run_case(temporary.path(), "zip", "json", "zip", &zip_bytes(&json),),
        expected
    );

    assert_cli_error(&[], "128", "usage: ptg2_scanner --hospital-mrf-copy");
    let input = temporary.path().join("json.json");
    let unused_output = temporary.path().join("unused-error-output");
    let base_args = vec![
        "json".to_owned(),
        VERSION_ID.to_owned(),
        input.display().to_string(),
        unused_output.display().to_string(),
        "2097152".to_owned(),
        "1048576".to_owned(),
    ];

    let mut invalid_format = base_args.clone();
    invalid_format[0] = "xml".to_owned();
    assert_cli_error(&invalid_format, "128", "format must be json");
    let mut invalid_decompressed_limit = base_args.clone();
    invalid_decompressed_limit[4] = "0".to_owned();
    assert_cli_error(
        &invalid_decompressed_limit,
        "128",
        "max_decompressed_bytes must be a positive integer",
    );
    let mut invalid_output_limit = base_args.clone();
    invalid_output_limit[5] = "0".to_owned();
    assert_cli_error(
        &invalid_output_limit,
        "128",
        "max_output_bytes must be a positive integer",
    );
    assert_cli_error(&base_args, "0", "must be a positive integer");

    let mut oversized_version = base_args.clone();
    oversized_version[1] = "v".repeat(65);
    assert_cli_error(
        &oversized_version,
        "128",
        "version_id exceeds 64 UTF-8 bytes",
    );

    let output_file = temporary.path().join("output-file");
    fs::write(&output_file, b"not a directory").unwrap();
    let mut output_file_args = base_args.clone();
    output_file_args[3] = output_file.display().to_string();
    assert_cli_error(
        &output_file_args,
        "128",
        "output path must be an existing non-symlink directory",
    );

    let occupied_output = temporary.path().join("occupied-output");
    fs::create_dir(&occupied_output).unwrap();
    fs::write(occupied_output.join("mrf.copy"), b"occupied").unwrap();
    let mut occupied_output_args = base_args;
    occupied_output_args[3] = occupied_output.display().to_string();
    assert_cli_error(&occupied_output_args, "128", "output already exists");
}
