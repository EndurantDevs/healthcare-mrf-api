    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use serde_json::json;
    use std::io::Cursor;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    const VERSION_ID: &str = "fixture-version";
    const TEST_MAX_DECOMPRESSED_BYTES: u64 = 2 * 1024 * 1024;
    const TEST_MAX_OUTPUT_BYTES: u64 = 1024 * 1024;

    fn fixture_json() -> Vec<u8> {
        serde_json::to_vec(&json!({
            "hospital_name": "North, Hospital",
            "last_updated_on": "2026-04-01",
            "version": "3.0.0",
            "location_name": ["Main, Campus"],
            "hospital_address": ["1 Main Street,\nSuite 2"],
            "type_2_npi": ["1234567890"],
            "license_information": {"license_number": "A-1", "state": "CA"},
            "attestation": {
                "attestation": ATTESTATION_TEXT,
                "confirm_attestation": true,
                "attester_name": "Alex Attester"
            },
            "financial_aid_policy": "Policy,\nline",
            "general_contract_provisions": [{
                "provisions": "Aggregate,\nterms"
            }],
            "standard_charge_information": [{
                "description": "MRI,\nbrain",
                "code_information": [{"code": "70551", "type": "CPT"}],
                "standard_charges": [{
                    "setting": "outpatient",
                    "billing_class": "facility",
                    "modifier_code": ["26", "TC"],
                    "gross_charge": 12.34,
                    "discounted_cash": 10.5,
                    "minimum": 8.001,
                    "maximum": 9.999,
                    "payers_information": [{
                        "payer_name": "Payer, Inc.",
                        "plan_name": "Plan A",
                        "standard_charge_dollar": 9.125,
                        "methodology": "fee schedule"
                    }]
                }]
            }]
        }))
        .unwrap()
    }

    fn general_rows(width: usize) -> (Vec<String>, Vec<String>) {
        let mut headers = vec![
            "hospital_name".to_owned(),
            "last_updated_on".to_owned(),
            "version".to_owned(),
            "location_name".to_owned(),
            "hospital_address".to_owned(),
            "license_number | CA".to_owned(),
            "type_2_npi".to_owned(),
            ATTESTATION_TEXT.to_owned(),
            "attester_name".to_owned(),
            "financial_aid_policy".to_owned(),
            "general_contract_provisions".to_owned(),
        ];
        let mut values = vec![
            "North, Hospital".to_owned(),
            "2026-04-01".to_owned(),
            "3.0.0".to_owned(),
            "Main, Campus".to_owned(),
            "1 Main Street,\nSuite 2".to_owned(),
            "A-1".to_owned(),
            "1234567890".to_owned(),
            "TRUE".to_owned(),
            "Alex Attester".to_owned(),
            "Policy,\nline".to_owned(),
            "Aggregate,\nterms".to_owned(),
        ];
        headers.resize(width, String::new());
        values.resize(width, String::new());
        (headers, values)
    }

    fn csv_bytes(headers: Vec<&str>, row: Vec<&str>) -> Vec<u8> {
        let (general_headers, general_values) = general_rows(headers.len());
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        writer.write_record(general_headers).unwrap();
        writer.write_record(general_values).unwrap();
        writer.write_record(headers).unwrap();
        writer.write_record(row).unwrap();
        writer.into_inner().unwrap()
    }

    fn fixture_tall_csv() -> Vec<u8> {
        csv_bytes(
            vec![
                "description",
                "code | 1",
                "code | 1 | type",
                "modifiers",
                "setting",
                "billing_class",
                "drug_unit_of_measurement",
                "drug_type_of_measurement",
                "standard_charge | gross",
                "standard_charge | discounted_cash",
                "payer_name",
                "plan_name",
                "standard_charge | negotiated_dollar",
                "standard_charge | negotiated_percentage",
                "standard_charge | negotiated_algorithm",
                "median_amount",
                "10th_percentile",
                "90th_percentile",
                "count",
                "standard_charge | methodology",
                "standard_charge | min",
                "standard_charge | max",
                "additional_generic_notes",
            ],
            vec![
                "MRI,\nbrain",
                "70551",
                "CPT",
                "26 | TC",
                "OUTPATIENT",
                "FACILITY",
                "",
                "",
                "12.3400",
                "10.500",
                "Payer, Inc.",
                "Plan A",
                "9.1250",
                "",
                "",
                "",
                "",
                "",
                "",
                "Fee Schedule",
                "8.0010",
                "9.9990",
                "",
            ],
        )
    }

    fn fixture_wide_csv() -> Vec<u8> {
        csv_bytes(
            vec![
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
                "standard_charge|Payer, Inc.|Plan A|negotiated_dollar",
                "standard_charge|Payer, Inc.|Plan A|negotiated_percentage",
                "standard_charge|Payer, Inc.|Plan A|negotiated_algorithm",
                "median_amount|Payer, Inc.|Plan A",
                "10th_percentile|Payer, Inc.|Plan A",
                "90th_percentile|Payer, Inc.|Plan A",
                "count|Payer, Inc.|Plan A",
                "standard_charge|Payer, Inc.|Plan A|methodology",
                "additional_payer_notes|Payer, Inc.|Plan A",
                "standard_charge|min",
                "standard_charge|max",
                "additional_generic_notes",
            ],
            vec![
                "MRI,\nbrain",
                "70551",
                "CPT",
                "26|TC",
                "outpatient",
                "facility",
                "",
                "",
                "12.34",
                "10.5",
                "9.125",
                "",
                "",
                "",
                "",
                "",
                "",
                "fee schedule",
                "",
                "8.001",
                "9.999",
                "",
            ],
        )
    }

    fn append_csv_row(payload: &[u8], values: &[(&str, &str)]) -> Vec<u8> {
        let mut reader = ReaderBuilder::new().has_headers(false).from_reader(payload);
        let records = reader.records().collect::<Result<Vec<_>, _>>().unwrap();
        let headers = &records[2];
        let mut row = vec![String::new(); headers.len()];
        for (header, value) in values {
            let index = headers
                .iter()
                .position(|candidate| candidate == *header)
                .expect("missing fixture header");
            row[index] = (*value).to_owned();
        }
        let mut writer = csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(Vec::new());
        for record in records {
            writer.write_record(&record).unwrap();
        }
        writer.write_record(row).unwrap();
        writer.into_inner().unwrap()
    }

    fn run_fixture(format: InputFormat, payload: &[u8], gzip: bool) -> BTreeMap<String, Vec<u8>> {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory
            .path()
            .join(if gzip { "input.gz" } else { "input.mrf" });
        if gzip {
            let file = File::create(&input_path).unwrap();
            let mut encoder = GzEncoder::new(file, Compression::default());
            encoder.write_all(payload).unwrap();
            encoder.finish().unwrap();
        } else {
            fs::write(&input_path, payload).unwrap();
        }
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let summary = import_hospital_mrf(
            format,
            VERSION_ID,
            &input_path,
            &output_directory,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap();
        assert_eq!(summary.version_id, VERSION_ID);
        assert_eq!(summary.contract, "hospital-mrf-copy-v3");
        assert_eq!(
            summary.max_decompressed_bytes,
            DEFAULT_MAX_DECOMPRESSED_BYTES
        );
        assert_eq!(summary.max_output_bytes, TEST_MAX_OUTPUT_BYTES);
        assert_eq!(summary.artifacts.len(), CopyKind::ALL.len());
        assert!(summary
            .artifacts
            .iter()
            .all(|artifact| artifact.path.ends_with(&format!("{}.copy", artifact.kind))));
        CopyKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind.name().to_owned(),
                    fs::read(output_directory.join(format!("{}.copy", kind.name()))).unwrap(),
                )
            })
            .collect()
    }

    fn zip_bytes(entries: &[(&str, &[u8])], method: CompressionMethod) -> Vec<u8> {
        let mut writer = ZipWriter::new(Cursor::new(Vec::new()));
        let options = SimpleFileOptions::default().compression_method(method);
        for (name, payload) in entries {
            if name.ends_with('/') {
                writer.add_directory(*name, options).unwrap();
            } else {
                writer.start_file(*name, options).unwrap();
                writer.write_all(payload).unwrap();
            }
        }
        writer.finish().unwrap().into_inner()
    }

    fn zip_field(bytes: &mut [u8], signature: &[u8; 4], offset: usize, value: u32) {
        let start = bytes
            .windows(signature.len())
            .position(|window| window == signature)
            .unwrap()
            + offset;
        bytes[start..start + 4].copy_from_slice(&value.to_le_bytes());
    }

    fn assert_zip_import_error(bytes: &[u8], max_decompressed_bytes: u64, expected: &str) {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("invalid.zip");
        fs::write(&input_path, bytes).unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let error = import_hospital_mrf_with_limits(
            InputFormat::Json,
            VERSION_ID,
            &input_path,
            &output_directory,
            DEFAULT_MAX_FANOUT_ROWS,
            max_decompressed_bytes,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
        assert_eq!(fs::read_dir(output_directory).unwrap().count(), 0);
    }

    fn run_zip_fixture(
        format: InputFormat,
        payload: &[u8],
        method: CompressionMethod,
    ) -> BTreeMap<String, Vec<u8>> {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("input.bin");
        fs::write(
            &input_path,
            zip_bytes(&[("folder/", b""), ("prices.mrf", payload)], method),
        )
        .unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let summary = import_hospital_mrf(
            format,
            VERSION_ID,
            &input_path,
            &output_directory,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap();
        assert_eq!(
            summary.compressed_input_bytes,
            input_path.metadata().unwrap().len()
        );
        CopyKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind.name().to_owned(),
                    fs::read(output_directory.join(format!("{}.copy", kind.name()))).unwrap(),
                )
            })
            .collect()
    }

    fn assert_import_error(
        format: InputFormat,
        payload: &[u8],
        max_fanout_rows: usize,
        expected: &str,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("invalid.mrf");
        fs::write(&input_path, payload).unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let error = import_hospital_mrf_with_limits(
            format,
            VERSION_ID,
            &input_path,
            &output_directory,
            max_fanout_rows,
            TEST_MAX_DECOMPRESSED_BYTES,
            TEST_MAX_OUTPUT_BYTES,
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
        assert_eq!(fs::read_dir(output_directory).unwrap().count(), 0);
    }

    fn assert_payload_limit_error(
        format: InputFormat,
        payload: &[u8],
        max_input_value_bytes: u64,
        expected: &str,
    ) {
        let directory = tempfile::tempdir().unwrap();
        let output_directory = directory.path().join("output");
        fs::create_dir(&output_directory).unwrap();
        let error = parse_hospital_payload_with_limits(
            format,
            Cursor::new(payload),
            VERSION_ID,
            &output_directory,
            HospitalMrfLimits {
                max_fanout_rows: DEFAULT_MAX_FANOUT_ROWS,
                max_decompressed_bytes: payload.len() as u64 + 1,
                max_output_bytes: TEST_MAX_OUTPUT_BYTES,
                max_input_value_bytes,
            },
        )
        .unwrap_err();
        assert!(
            error.to_string().contains(expected),
            "expected {expected:?} in {error}"
        );
        assert_eq!(fs::read_dir(output_directory).unwrap().count(), 0);
    }
