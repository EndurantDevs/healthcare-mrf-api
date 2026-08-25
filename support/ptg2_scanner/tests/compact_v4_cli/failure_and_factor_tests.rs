#[test]
fn compact_cli_reuses_byte_identical_raw_inline_groups_before_deserialization() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    fs::create_dir(&output).expect("create output directory");
    let raw = br#"{
      "reporting_entity_name":"V4 raw cache fixture",
      "provider_references":[],
      "in_network":[{
        "billing_code_type":"CPT",
        "billing_code":"70553",
        "negotiation_arrangement":"ffs",
        "negotiated_rates":[
          {"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}],"negotiated_prices":[{"negotiated_rate":100}]},
          {"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}],"negotiated_prices":[{"negotiated_rate":125}]}
        ]
      }]
    }"#;
    fs::write(&source, raw).expect("write repeated inline-group fixture");

    let completed = run_compact_v4(&source, &output);
    assert!(
        completed.status.success(),
        "scanner failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let records = String::from_utf8(completed.stdout).expect("UTF-8 scanner output");
    let summary = records
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .find(|payload| {
            payload
                .get("provider_graph_v4_inline_transform_cache_transforms")
                .is_some()
        })
        .unwrap_or_else(|| panic!("parallel scanner summary in:\n{records}"));
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_transforms"],
        1
    );
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_misses"],
        1
    );
    assert_eq!(summary["provider_graph_v4_inline_transform_cache_hits"], 1);
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_entries"],
        1
    );
    assert!(
        summary["provider_graph_v4_inline_transform_cache_estimated_bytes"]
            .as_u64()
            .zip(summary["provider_graph_v4_inline_transform_cache_max_bytes"].as_u64())
            .is_some_and(|(estimated, maximum)| estimated <= maximum)
    );
}

#[test]
fn compact_cli_reports_provider_reference_worker_failures() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    fs::create_dir(&output).expect("create output directory");
    fs::write(
        &source,
        br#"{
          "provider_references":[{"provider_group_id":7,"provider_groups":"invalid"}],
          "in_network":[]
        }"#,
    )
    .expect("write malformed provider reference fixture");

    let v2_path = output.join("provider-group-tax-identity-v2.ptg2tax");
    let completed = compact_v4_command(&source, &output)
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH",
            &v2_path,
        )
        .output()
        .expect("run compact V4 scanner");
    assert!(!completed.status.success());
    let stderr = String::from_utf8_lossy(&completed.stderr);
    assert!(stderr.contains("PTG2_SCANNER_WORKER_FAILED"), "{stderr}");
    assert!(stderr.contains("provider_ref_error"), "{stderr}");
    assert!(framed_json_records_named(
        &completed.stdout,
        "manifest_provider_group_tax_identity_sidecar_file"
    )
    .is_empty());
    assert!(framed_json_records_named(
        &completed.stdout,
        "manifest_provider_group_tax_identity_v2_sidecar_file"
    )
    .is_empty());
    for path in [
        output.join("provider-group-tax-identity.ptg2tax"),
        v2_path,
        output.join("provider-group-tax-identity.ptg2tax.building"),
        output.join("provider-group-tax-identity-v2.ptg2tax.building"),
    ] {
        assert!(!path.exists());
    }
}

#[test]
fn compact_cli_reports_primary_producer_failures() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    fs::create_dir(&output).expect("create output directory");
    fs::write(
        &source,
        br#"{
          "provider_references":[{
            "provider_group_id":7,
            "provider_groups":[{
              "tin":{"type":"ein","value":"111223333"},
              "npi":[1234567890]
            }]
          }],
          "in_network":[{
            "billing_code_type":"CPT",
            "billing_code":"99213",
            "negotiated_rates":[}
        }"#,
    )
    .expect("write malformed rate fixture");

    let v2_path = output.join("provider-group-tax-identity-v2.ptg2tax");
    let completed = compact_v4_command(&source, &output)
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH",
            &v2_path,
        )
        .output()
        .expect("run compact V4 scanner");
    assert!(!completed.status.success());
    let stderr = String::from_utf8_lossy(&completed.stderr);
    assert!(stderr.contains("PTG2_SCANNER_PRIMARY_FAILED"), "{stderr}");
    assert!(stderr.contains("producer_error"), "{stderr}");
    assert!(framed_json_records_named(
        &completed.stdout,
        "manifest_provider_group_tax_identity_sidecar_file"
    )
    .is_empty());
    assert!(framed_json_records_named(
        &completed.stdout,
        "manifest_provider_group_tax_identity_v2_sidecar_file"
    )
    .is_empty());
    for path in [
        output.join("provider-group-tax-identity.ptg2tax"),
        v2_path,
        output.join("provider-group-tax-identity.ptg2tax.building"),
        output.join("provider-group-tax-identity-v2.ptg2tax.building"),
    ] {
        assert!(!path.exists());
    }
}

#[test]
fn compact_cli_reports_fail_closed_provider_and_sidecar_boundaries() {
    fn run_case<F>(root: &Path, name: &str, source_bytes: &[u8], configure: F, expected: &str)
    where
        F: FnOnce(&mut Command),
    {
        let case_root = root.join(name);
        let source = case_root.join("rates.json");
        let output = case_root.join("output");
        fs::create_dir_all(&output).expect("create case output directory");
        fs::write(&source, source_bytes).expect("write fail-closed fixture");
        let mut command = compact_v4_command(&source, &output);
        configure(&mut command);
        let completed = command.output().expect("run fail-closed compact scanner");
        assert!(!completed.status.success(), "{name} unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&completed.stderr);
        assert!(
            stderr.contains(expected),
            "{name} stderr did not contain {expected:?}: {stderr}"
        );
    }

    let temporary = tempfile::tempdir().expect("temporary fail-closed fixture root");
    let root = temporary.path();
    let no_overrides = |_: &mut Command| {};
    run_case(
        root,
        "missing-groups",
        br#"{"provider_references":[{"provider_group_id":7}],"in_network":[]}"#,
        no_overrides,
        "provider_groups must be an array",
    );
    run_case(
        root,
        "non-object-group",
        br#"{"provider_references":[{"provider_group_id":7,"provider_groups":[7]}],"in_network":[]}"#,
        no_overrides,
        "provider_groups elements must be JSON objects",
    );
    run_case(
        root,
        "missing-group-id",
        br#"{"provider_references":[{"provider_groups":[{"tin":{"type":"ein","value":"111223333"},"npi":[1234567890]}]}],"in_network":[]}"#,
        no_overrides,
        "provider reference is missing provider_group_id",
    );

    run_case(
        root,
        "non-array-inline-groups",
        br#"{"provider_references":[],"in_network":[{"billing_code_type":"CPT","billing_code":"99213","negotiated_rates":[{"provider_groups":{},"negotiated_prices":[{"negotiated_rate":125}]}]}]}"#,
        no_overrides,
        "expected JSON value type Array",
    );

    let blocked_parent = root.join("sidecar-parent-is-a-file");
    fs::write(&blocked_parent, b"not a directory").expect("write blocked sidecar parent");
    run_case(
        root,
        "blocked-sidecar-parent",
        RAW_MRF,
        |command| {
            command.env(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH",
                blocked_parent.join("provider-set-component.ptg2sc"),
            );
        },
        "tax identity collision coordinate could not be resolved",
    );
}

#[test]
fn compact_cli_emits_exact_v4_factors_and_source_witnesses() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    fs::create_dir(&output).expect("create output directory");
    fs::write(&source, RAW_MRF).expect("write MRF fixture");

    let completed = run_compact_v4(&source, &output);
    assert!(
        completed.status.success(),
        "scanner failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let records = String::from_utf8(completed.stdout).expect("UTF-8 scanner output");
    let payloads = records
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let summary = payloads
        .iter()
        .find(|payload| {
            payload
                .get("provider_graph_v4_factor_cache_entries")
                .is_some()
        })
        .unwrap_or_else(|| panic!("parallel scanner summary in:\n{records}"));
    assert_eq!(summary["top_level_byte_scan_selected"], true);
    assert_eq!(summary["provider_graph_v4_factor_mode"], true);
    assert_eq!(summary["provider_graph_v4_factor_cache_entries"], 3);
    assert_eq!(summary["provider_graph_v4_npi_union_attempts"], 3);
    assert_eq!(summary["provider_graph_v4_flat_group_union_attempts"], 3);
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_transforms"],
        1
    );
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_misses"],
        1
    );
    assert_eq!(
        summary["provider_graph_v4_inline_transform_cache_entries"],
        1
    );
    assert!(
        summary["provider_graph_v4_inline_transform_cache_estimated_bytes"]
            .as_u64()
            .zip(summary["provider_graph_v4_inline_transform_cache_max_bytes"].as_u64())
            .is_some_and(|(estimated, maximum)| estimated <= maximum)
    );
    assert!(summary["provider_graph_v4_reference_only_rates"]
        .as_u64()
        .is_some_and(|value| value >= 2));
    assert_eq!(summary["provider_graph_v4_inline_only_rates"], 1);

    for name in [
        "provider-set-component.ptg2sc",
        "provider-component-group.ptg2sc",
    ] {
        assert!(
            fs::metadata(output.join(name))
                .expect("factor sidecar")
                .len()
                > 0
        );
    }
    let witness = payloads
        .iter()
        .find(|payload| payload["contract"] == "ptg2_v3_source_witness_v3")
        .expect("source witness summary");
    assert_eq!(witness["provider_population_count"], 2);
    assert_eq!(witness["queryable_occurrence_population_count"], 9);
    assert_eq!(witness["occurrence_witness_count"], 9);
    assert!(
        fs::metadata(witness["path"].as_str().expect("witness path"))
            .expect("witness bundle")
            .len()
            > 0
    );

    let manifest = output.join("scanner-manifest.json");
    write_finalizer_manifest(&payloads, &manifest);
    let price_key_map = output.join("price-key-map.copy");
    let price_key_count = write_price_key_map(&payloads, &price_key_map);
    let finalizer_output = output.join("finalized");
    let finalized = run_v3_finalizer(
        &finalizer_output,
        &manifest,
        &price_key_map,
        price_key_count,
    );
    assert!(
        finalized.status.success(),
        "finalizer failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&finalized.stderr),
        String::from_utf8_lossy(&finalized.stdout),
    );
    let finalizer_payloads = String::from_utf8(finalized.stdout)
        .expect("UTF-8 finalizer output")
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let finalizer_summary = finalizer_payloads
        .iter()
        .find(|payload| payload["format"] == "ptg2_v3_direct_finalizer_v3")
        .expect("V3 finalizer summary");
    assert_eq!(
        finalizer_summary["preservation"]["all_source_occurrences_preserved"],
        true
    );
    let provider_code_bitmap_candidate_bytes =
        finalizer_summary["identity_maps"]["provider_code_bitmap_candidate_bytes"].as_u64();
    let provider_code_bitmap_charged_bytes = finalizer_summary["identity_maps"]
        ["provider_code_bitmap_charged_bytes"]
        .as_u64()
        .expect("provider-code bitmap charge");
    let expected_provider_code_mode = if provider_code_bitmap_charged_bytes > 0
        || provider_code_bitmap_candidate_bytes == Some(0)
    {
        "provider_major_bitmap_v1"
    } else {
        "pair_spool_sort_v1"
    };
    assert_eq!(
        finalizer_summary["identity_maps"]["provider_code_bitmap_planned_mode"],
        expected_provider_code_mode
    );
    assert_eq!(finalizer_summary["rate_schedule_observe"]["enabled"], true);
    assert!(
        finalizer_summary
            .pointer("/blocks/assigned_encoder/provider_set_codes/storage/compressed_records")
            .and_then(Value::as_u64)
            .is_some_and(|count| count > 0),
        "{finalizer_summary:#}"
    );
    for name in [
        "summary.json",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
    ] {
        assert!(finalizer_output.join(name).is_file(), "missing {name}");
    }
}
