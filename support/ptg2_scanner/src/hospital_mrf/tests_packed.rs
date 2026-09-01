fn import_packed(
    format: InputFormat,
    payload: &[u8],
    max_output_bytes: u64,
) -> (tempfile::TempDir, HospitalMrfSummary) {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input");
    fs::write(&input_path, payload).unwrap();
    let output_directory = directory.path().join("output");
    fs::create_dir(&output_directory).unwrap();
    let summary = import_hospital_mrf_with_output_mode(
        format,
        VERSION_ID,
        &input_path,
        &output_directory,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            max_output_bytes,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap();
    (directory, summary)
}

fn import_packed_json(
    payload: &[u8],
    max_output_bytes: u64,
) -> (tempfile::TempDir, HospitalMrfSummary) {
    import_packed(InputFormat::Json, payload, max_output_bytes)
}

#[test]
fn packed_v2_facts_keep_estimated_and_comparison_amounts_distinct() {
    let mut payload: serde_json::Value =
        serde_json::from_slice(&fixture_v2_json("2.2.1")).unwrap();
    let charge = &mut payload["standard_charge_information"][0]["standard_charges"][0];
    charge["minimum"] = json!(8);
    charge["maximum"] = json!(10);
    charge["payers_information"][0]["standard_charge_dollar"] = json!(8.5);
    let payload = serde_json::to_vec(&payload).unwrap();
    let (directory, summary) = import_packed_json(&payload, TEST_MAX_OUTPUT_BYTES);
    assert_eq!(summary.schema_version, "2.2.1");
    assert_eq!(summary.contract, "hospital-mrf-copy-v2-v3-packed-v5");
    let payloads = super::packed_output_tests::payloads(
        &directory.path().join("output/fact_block.copy"),
    );
    let facts = crate::hospital_price_block::decode_fact_block(
        &payloads[0],
        None,
        None,
        0,
        crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
    )
    .unwrap();
    assert_eq!(facts[0].estimated_amount.as_deref(), Some("9.125"));
    assert_eq!(facts[0].comparison_amount.as_deref(), Some("8.5"));

    let zipped_directory = tempfile::tempdir().unwrap();
    let input_path = zipped_directory.path().join("input.zip");
    fs::write(
        &input_path,
        zip_bytes(&[("prices.json", &payload)], CompressionMethod::Deflated),
    )
    .unwrap();
    let output_directory = zipped_directory.path().join("output");
    fs::create_dir(&output_directory).unwrap();
    let zipped = import_hospital_mrf_with_output_mode(
        InputFormat::Json,
        VERSION_ID,
        &input_path,
        &output_directory,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            TEST_MAX_OUTPUT_BYTES,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap();
    assert_eq!(zipped.schema_version, "2.2.1");
    assert_eq!(zipped.contract, "hospital-mrf-copy-v2-v3-packed-v5");
    assert_eq!(
        summary
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>(),
        zipped
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>()
    );
}

#[test]
fn packed_csv_v2_tall_and_wide_keep_estimated_facts_identical() {
    let (tall_directory, tall) = import_packed(
        InputFormat::TallCsv,
        &fixture_v2_csv(InputFormat::TallCsv, "2.0.0"),
        TEST_MAX_OUTPUT_BYTES,
    );
    let (_, wide) = import_packed(
        InputFormat::WideCsv,
        &fixture_v2_csv(InputFormat::WideCsv, "2.0.0"),
        TEST_MAX_OUTPUT_BYTES,
    );
    assert_eq!(tall.schema_version, "2.0.0");
    assert_eq!(wide.schema_version, "2.0.0");
    assert_eq!(
        tall.artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>(),
        wide.artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>()
    );
    let payloads = super::packed_output_tests::payloads(
        &tall_directory.path().join("output/fact_block.copy"),
    );
    let facts = crate::hospital_price_block::decode_fact_block(
        &payloads[0],
        None,
        None,
        0,
        crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
    )
    .unwrap();
    assert_eq!(facts[0].estimated_amount.as_deref(), Some("9.125"));
    assert_eq!(facts[0].comparison_amount.as_deref(), Some("9.125"));
}

fn assert_packed_nul_rejected(payload: &serde_json::Value) {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input.json");
    fs::write(&input_path, serde_json::to_vec(payload).unwrap()).unwrap();
    let output_directory = directory.path().join("output");
    fs::create_dir(&output_directory).unwrap();
    let error = import_hospital_mrf_with_output_mode(
        InputFormat::Json,
        VERSION_ID,
        &input_path,
        &output_directory,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            TEST_MAX_OUTPUT_BYTES,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap_err();
    assert!(error.to_string().contains("contains NUL"));
    assert_eq!(fs::read_dir(output_directory).unwrap().count(), 0);
}

#[test]
fn legacy_summary_contract_tracks_transitional_metadata() {
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input.json");
    fs::write(&input_path, fixture_json()).unwrap();
    let output_directory = directory.path().join("output");
    fs::create_dir(&output_directory).unwrap();
    let summary = import_hospital_mrf(
        InputFormat::Json,
        VERSION_ID,
        &input_path,
        &output_directory,
        TEST_MAX_OUTPUT_BYTES,
    )
    .unwrap();
    let value = serde_json::to_value(summary).unwrap();
    assert_eq!(value["contract"], "hospital-mrf-copy-v2-v3-v2");
    assert_eq!(value["schema_revision"], HOSPITAL_MRF_SCHEMA_REVISION);
    assert_eq!(value["artifacts"].as_array().unwrap().len(), 11);
    assert!(value.get("root").is_none());
}

#[test]
fn packed_json_tall_and_wide_are_identical() {
    let (_, json) = import_packed(
        InputFormat::Json,
        &fixture_json(),
        TEST_MAX_OUTPUT_BYTES,
    );
    for (format, payload) in [
        (InputFormat::TallCsv, fixture_tall_csv()),
        (InputFormat::WideCsv, fixture_wide_csv()),
    ] {
        let (_, csv) = import_packed(format, &payload, TEST_MAX_OUTPUT_BYTES);
        assert_eq!(
            json.artifacts
                .iter()
                .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                .collect::<Vec<_>>(),
            csv.artifacts
                .iter()
                .map(|artifact| (artifact.kind, artifact.rows, &artifact.sha256))
                .collect::<Vec<_>>()
        );
        assert_eq!(
            serde_json::to_value(json.root.as_ref().unwrap()).unwrap(),
            serde_json::to_value(csv.root.as_ref().unwrap()).unwrap()
        );
    }
}

#[test]
fn packed_mode_rejects_nul_in_each_packed_row_kind() {
    for pointer in [
        "/standard_charge_information/0/description",
        "/standard_charge_information/0/code_information/0/code",
        "/standard_charge_information/0/standard_charges/0/payers_information/0/payer_name",
    ] {
        let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
        *payload.pointer_mut(pointer).unwrap() = json!("invalid\0text");
        assert_packed_nul_rejected(&payload);
    }
    let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    payload["standard_charge_information"][0]["standard_charges"][0]
        ["additional_generic_notes"] = json!("invalid\0note");
    assert_packed_nul_rejected(&payload);
}

#[test]
fn packed_mode_emits_ordered_artifacts_root_and_shared_budget() {
    let payload = fixture_json();
    let (directory, summary) = import_packed_json(&payload, TEST_MAX_OUTPUT_BYTES);
    assert_eq!(summary.contract, "hospital-mrf-copy-v2-v3-packed-v5");
    assert_eq!(summary.schema_revision, HOSPITAL_MRF_PACKED_SCHEMA_REVISION);
    assert_eq!(
        summary
            .artifacts
            .iter()
            .map(|artifact| artifact.kind)
            .collect::<Vec<_>>(),
        vec![
            "mrf",
            "location",
            "npi",
            "license",
            "contract_provision",
            "modifier",
            "modifier_payer",
            "service_block",
            "fact_block",
            "selector_page",
        ]
    );
    let root = summary.root.as_ref().unwrap();
    assert_eq!(
        (root.service_count, root.charge_count, root.fact_count),
        (1, 1, 1)
    );
    assert_eq!(
        (
            root.code_selector_key_count,
            root.payer_plan_selector_key_count,
            root.code_selector_ref_count,
            root.payer_plan_selector_ref_count,
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(
        (
            root.service_block_count,
            root.fact_block_count,
            root.code_selector_block_count,
            root.payer_plan_selector_block_count,
        ),
        (1, 1, 1, 1)
    );
    assert_eq!(root.peak_scratch_bytes, root.selector_spool_bytes * 3);
    let output_directory = directory.path().join("output");
    for kind in ["service", "code", "charge", "payer_charge"] {
        assert!(!output_directory.join(format!("{kind}.copy")).exists());
    }

    let retained_bytes = summary
        .artifacts
        .iter()
        .map(|artifact| artifact.bytes)
        .sum::<u64>();
    assert!(retained_bytes - 1 > summary.artifacts.iter().map(|a| a.bytes).max().unwrap());
    let failed = tempfile::tempdir().unwrap();
    let input_path = failed.path().join("input.json");
    fs::write(&input_path, payload).unwrap();
    let failed_output = failed.path().join("output");
    fs::create_dir(&failed_output).unwrap();
    let error = import_hospital_mrf_with_output_mode(
        InputFormat::Json,
        VERSION_ID,
        &input_path,
        &failed_output,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            retained_bytes - 1,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("COPY output exceeds configured limit"));
    assert_eq!(fs::read_dir(failed_output).unwrap().count(), 0);
}

#[test]
fn packed_selector_spool_deduplicates_codes_per_charge() {
    let mut payload: serde_json::Value = serde_json::from_slice(&fixture_json()).unwrap();
    payload["standard_charge_information"][0]["code_information"]
        .as_array_mut()
        .unwrap()
        .push(json!({"code": "70551", "type": "CPT"}));
    payload["standard_charge_information"][0]["code_information"]
        .as_array_mut()
        .unwrap()
        .push(json!({"code": "70551", "type": "HCPCS"}));
    let (_, summary) = import_packed_json(
        &serde_json::to_vec(&payload).unwrap(),
        TEST_MAX_OUTPUT_BYTES,
    );
    let root = summary.root.unwrap();

    assert_eq!(root.code_selector_key_count, 2);
    assert_eq!(root.code_selector_ref_count, 2);
    assert_eq!(root.payer_plan_selector_ref_count, 1);
    assert_eq!(root.code_selector_page_count, 2);
    assert_eq!(root.code_selector_block_count, 1);
    assert_eq!(root.selector_spool_bytes, 3 * SELECTOR_SPOOL_RECORD_BYTES as u64);
    assert_eq!(root.peak_scratch_bytes, root.selector_spool_bytes * 3);
}

#[test]
fn selector_key_memory_is_bounded_before_retaining_a_new_key() {
    let directory = tempfile::tempdir().unwrap();
    let mut builder = PackedOutputBuilder::create(
        directory.path(),
        VERSION_ID,
        Arc::new(AtomicU64::new(0)),
        1024,
    )
    .unwrap();
    for suffix in ["a", "b"] {
        let key = crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
            code_type: "CPT".to_owned(),
            code: format!("{suffix}{}", "x".repeat(99)),
        };
        builder.selector_key_ordinal(key).unwrap();
    }
    let retained_bytes = builder.selector_key_memory_bytes;
    let error = builder
        .selector_key_ordinal(
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
                code_type: "CPT".to_owned(),
                code: format!("c{}", "x".repeat(99)),
            },
        )
        .unwrap_err();

    assert!(error.to_string().contains("selector key memory exceeds 1024 bytes"));
    assert_eq!(builder.selector_keys.len(), 2);
    assert_eq!(builder.selector_key_memory_bytes, retained_bytes);
}

#[cfg(unix)]
#[test]
fn output_collisions_preserve_unowned_symlinks() {
    use std::os::unix::fs::symlink;

    for occupied_name in [
        "mrf.copy",
        "service_block.copy",
        ".selector_refs.sorted.partial",
    ] {
        let directory = tempfile::tempdir().unwrap();
        let occupied = directory.path().join(occupied_name);
        symlink(directory.path().join("missing"), &occupied).unwrap();
        assert!(CopyOutputs::create(
            directory.path(),
            VERSION_ID,
            TEST_MAX_OUTPUT_BYTES,
            HospitalMrfOutputMode::Packed,
        )
        .is_err());
        assert!(fs::symlink_metadata(&occupied)
            .unwrap()
            .file_type()
            .is_symlink());
    }

    let directory = tempfile::tempdir().unwrap();
    let outputs = CopyOutputs::create(
        directory.path(),
        VERSION_ID,
        TEST_MAX_OUTPUT_BYTES,
        HospitalMrfOutputMode::Legacy,
    )
    .unwrap();
    let occupied = directory.path().join("mrf.copy");
    symlink(directory.path().join("missing"), &occupied).unwrap();
    assert!(outputs
        .finish(HOSPITAL_MRF_SCHEMA_VERSION.to_owned())
        .is_err());
    assert!(fs::symlink_metadata(&occupied)
        .unwrap()
        .file_type()
        .is_symlink());

    let directory = tempfile::tempdir().unwrap();
    let mut sink = PackedSink::create(
        directory.path(),
        "service_block",
        VERSION_ID,
        Arc::new(AtomicU64::new(0)),
        TEST_MAX_OUTPUT_BYTES,
    )
    .unwrap();
    let occupied = directory.path().join("service_block.copy");
    symlink(directory.path().join("missing"), &occupied).unwrap();
    assert!(sink.finish().is_err());
    drop(sink);
    assert!(fs::symlink_metadata(&occupied)
        .unwrap()
        .file_type()
        .is_symlink());
}

#[test]
fn packed_zip_matches_plain_and_late_finish_failure_cleans_everything() {
    let payload = fixture_json();
    let (_, plain) = import_packed_json(&payload, TEST_MAX_OUTPUT_BYTES);
    let directory = tempfile::tempdir().unwrap();
    let input_path = directory.path().join("input.zip");
    fs::write(
        &input_path,
        zip_bytes(&[("prices.json", &payload)], CompressionMethod::Deflated),
    )
    .unwrap();
    let output_directory = directory.path().join("output");
    fs::create_dir(&output_directory).unwrap();
    let zipped = import_hospital_mrf_with_output_mode(
        InputFormat::Json,
        VERSION_ID,
        &input_path,
        &output_directory,
        HospitalMrfLimits::new(
            DEFAULT_MAX_FANOUT_ROWS,
            TEST_MAX_DECOMPRESSED_BYTES,
            TEST_MAX_OUTPUT_BYTES,
        ),
        HospitalMrfOutputMode::Packed,
    )
    .unwrap();
    assert_eq!(
        plain
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>(),
        zipped
            .artifacts
            .iter()
            .map(|artifact| (&artifact.kind, &artifact.sha256))
            .collect::<Vec<_>>()
    );

    let cleanup = tempfile::tempdir().unwrap();
    let mut outputs = CopyOutputs::create(
        cleanup.path(),
        VERSION_ID,
        TEST_MAX_OUTPUT_BYTES,
        HospitalMrfOutputMode::Packed,
    )
    .unwrap();
    drop(
        outputs.sinks[CopyKind::Location as usize]
            .as_mut()
            .unwrap()
            .writer
            .take(),
    );
    assert!(outputs
        .finish(HOSPITAL_MRF_SCHEMA_VERSION.to_owned())
        .is_err());
    assert_eq!(fs::read_dir(cleanup.path()).unwrap().count(), 0);
}
