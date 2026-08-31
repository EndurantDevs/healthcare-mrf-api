    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use serde_json::json;
    use std::io::Cursor;
    use zip::write::SimpleFileOptions;
    use zip::{CompressionMethod, ZipWriter};

    const VERSION_ID: &str = "fixture-version";
    const TEST_MAX_DECOMPRESSED_BYTES: u64 = 2 * 1024 * 1024;
    const TEST_MAX_OUTPUT_BYTES: u64 = 1024 * 1024;
    // The zip crate supports LZMA decompression but not compression, so this is
    // a single-member ZIP_LZMA archive containing the JSON fixture below.
    const LZMA_JSON_ZIP_HEX: &str = concat!(
        "504b03043f0002000e00ab2c1d5d9b43bbca9b030000080700000b0000007072696365732e6a736f6e090405005d0000",
        "8000003d88890734343f5c43a628b219bee05b500e05cc8fbd2e749fb10217991c781ee89d5cf71683e44129163883a1",
        "d76efe763809a032542011ff308358aab23851180d71cd7ccd6a2548aa0a7ee70ce2ddb1ed08da782bbe1deeb6b68b0a",
        "066991046631766fa383cdee991244e19e387df5b4935ae87cf4409a6e518dc6a2e47b7e04394ff5c7b4d996b1aaf27e",
        "cb276310e43307981fd485eeba35e02aaf00fb5f82953a4848fc57cce381e8b0cc0898f81c5a3142cf3d13a3b17e4346",
        "7e6c694c04129fcf1f71ddff3a276bb8d1efb69f79522479f80e15504a923be34bca4ab658a4514893c315a1bd5e31e6",
        "b4512d2f7c5cecd8f94d2d14f7513cec571818b527276f679595f2d481994d88abdaa48a3dbb16f7f74f286f8317de02",
        "f1f9529be4f93f207fc866989808e60a79faaeb050cadb76970874390c84c96fbdaa0e150a8b63423a31dca6a25f39bd",
        "52f470e2f4f78eee9488a9195a594bef694d7593e24498d723c458701e34ed2ee1fdd737fadd10016717ea8db5b848b3",
        "61676ab945f6ecdaa437aa07397e9382b251cb1a0018c65e91efd837e93dd9728bfc2d4c9cc10f9ee868b21c1e7921a6",
        "ab3ec4a6ffae0082fa6a00bacedb859dd4e44070270b5238ac67a99d6057bf58319bd856f5a12a022c5a758102a26cf1",
        "33707930b3aefa6f33b1abb29416554828d52dba6a2336e76cf745696e47b09c15e0952bdafa0fbec6efb6f515d508f5",
        "a2db39a42d278e70bafe58112c048f94720cdff552d8c4f45341fee337fab7950228ac33b990008311225dcbbd599235",
        "f65195ee364cc8bc836130dd9a8865049a83b7d8c18f5ee80c4174b9a702a5ee73ccca746ff4cd5419898edfb84ab97c",
        "713ec43a9160821eaef86097c8805c8711458a8f8b1a1c29e431ac7a4fb1af65db1d35c6aaa5d7f073259a457757c7ce",
        "a00c9f35cebf42f3fb55a98c0445075b5244fb912929a95ca2633218c50cc846336e9bb10e6129b514c36fa5304339a8",
        "1bdaa2bc89e67424c1eeae9759dde8c305e9080d13bc1b115a79e1f9d1a6916984634380bbbfb59e511468c6021619bb",
        "f9751c876d8264f7cd7dea0fe3d70726c99c78224b37fd9ae1ed38bb102b12a4d1a19690eb842d3f65eae7359aca0e63",
        "607a7419a6295b10e38913479f67ec228e37ddb56612b4b52511050342f2d413e36ac032b4a33aa1c4022d9a7c040b96",
        "82ef4cfdf1d67219eece41a6638000b64f54e19f5a356af83155f6303f229883d255b46b717799445ec8f91fc62739ff",
        "4ce2a100504b01023f033f0002000e00ab2c1d5d9b43bbca9b030000080700000b000000000000000000000080010000",
        "00007072696365732e6a736f6e504b0506000000000100010039000000c40300000000",
    );

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

    fn fixture_v2_json(version: &str) -> Vec<u8> {
        serde_json::to_vec(&json!({
            "hospital_name": "North, Hospital",
            "last_updated_on": "2025-04-01",
            "version": version,
            "hospital_location": ["Main, Campus"],
            "hospital_address": ["1 Main Street"],
            "license_information": {"license_number": "A-1", "state": "CA"},
            "affirmation": {
                "affirmation": "To the best of its knowledge and belief, the hospital has included all applicable standard charge information in accordance with the requirements of 45 CFR 180.50, and the information encoded is true, accurate, and complete as of the date indicated.",
                "confirm_affirmation": true
            },
            "standard_charge_information": [{
                "description": "Drug",
                "code_information": [{"code": "0001", "type": "NDC"}],
                "drug_information": {"unit": "2.50", "type": "ML"},
                "standard_charges": [{
                    "setting": "outpatient",
                    "payers_information": [{
                        "payer_name": "Payer, Inc.",
                        "plan_name": "Plan A",
                        "standard_charge_percentage": 80,
                        "estimated_amount": 9.125,
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
        run_fixture_with_summary(format, payload, gzip).0
    }

    fn run_fixture_with_summary(
        format: InputFormat,
        payload: &[u8],
        gzip: bool,
    ) -> (BTreeMap<String, Vec<u8>>, HospitalMrfSummary) {
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
        assert_eq!(summary.contract, "hospital-mrf-copy-v2-v3-v2");
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
        let rows = CopyKind::ALL
            .into_iter()
            .map(|kind| {
                (
                    kind.name().to_owned(),
                    fs::read(output_directory.join(format!("{}.copy", kind.name()))).unwrap(),
                )
            })
            .collect();
        (rows, summary)
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
        run_zip_archive(
            format,
            &zip_bytes(&[("folder/", b""), ("prices.mrf", payload)], method),
        )
    }

    fn run_zip_archive(format: InputFormat, archive: &[u8]) -> BTreeMap<String, Vec<u8>> {
        let directory = tempfile::tempdir().unwrap();
        let input_path = directory.path().join("input.bin");
        fs::write(&input_path, archive).unwrap();
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
