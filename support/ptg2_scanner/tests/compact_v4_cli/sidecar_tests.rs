#[test]
fn compact_cli_active_byte_engine_emits_paired_tax_identity_sidecars() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let provider_references = r#""provider_references":[
      {"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":["1000000491"]}]},
      {"provider_group_id":8,"provider_groups":[{"tin":{"type":"npi","value":"2999999990"},"npi":[2999999990]}]}
    ]"#;
    let in_network = r#""in_network":[{
            "billing_code_type":"CPT",
            "billing_code":"99213",
            "negotiation_arrangement":"ffs",
            "negotiated_rates":[
              {"provider_references":[7,8],"negotiated_prices":[{"negotiated_rate":100}]},
              {"provider_groups":[{"tin":{"type":"npi","value":"1000000491"},"npi":[1000000491]}],"negotiated_prices":[{"negotiated_rate":125}]}
            ]
          }]"#;
    let cases = [
        (
            "source-order",
            format!("{{{provider_references},{in_network}}}"),
        ),
        (
            "reordered",
            format!("{{{in_network},{provider_references}}}"),
        ),
    ];
    let mut expected_v1 = None;
    let mut expected_v2 = None;
    for (label, raw_fixture) in cases {
        let case_root = temporary.path().join(label);
        let source = case_root.join("rates.json");
        let output = case_root.join("output");
        fs::create_dir_all(&output).expect("create output directory");
        fs::write(&source, raw_fixture).expect("write paired tax identity fixture");
        let v2_path = output.join("provider-group-tax-identity-v2.ptg2tax");
        let completed = compact_v4_command(&source, &output)
            .env(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH",
                &v2_path,
            )
            .output()
            .expect("run active byte-parallel scanner");
        assert!(
            completed.status.success(),
            "{label} scanner failed: {}\nstdout:\n{}",
            String::from_utf8_lossy(&completed.stderr),
            String::from_utf8_lossy(&completed.stdout),
        );

        let stdout = String::from_utf8_lossy(&completed.stdout);
        assert!(stdout.contains("\"top_level_byte_scan_selected\":true"));
        if label == "reordered" {
            assert!(
                stdout.contains("parallel_top_level_bytes_plain_range_reorder")
                    || stdout.contains("parallel_top_level_bytes_indexed_reorder")
            );
        }
        assert!(stdout.contains("manifest_provider_group_tax_identity_sidecar_file"));
        assert!(stdout.contains("manifest_provider_group_tax_identity_v2_sidecar_file"));
        for raw_identity in ["123456789", "1000000491", "2999999990"] {
            assert!(!stdout.contains(raw_identity));
            assert!(!String::from_utf8_lossy(&completed.stderr).contains(raw_identity));
        }

        let v1_path = output.join("provider-group-tax-identity.ptg2tax");
        let v1_bytes = fs::read(&v1_path).expect("read v1 sidecar");
        if let Some(expected) = expected_v1.as_ref() {
            assert_eq!(&v1_bytes, expected);
        } else {
            expected_v1 = Some(v1_bytes.clone());
        }
        let v2_bytes = fs::read(&v2_path).expect("read v2 sidecar");
        if let Some(expected) = expected_v2.as_ref() {
            assert_eq!(&v2_bytes, expected);
        } else {
            expected_v2 = Some(v2_bytes.clone());
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            for artifact in [&v1_path, &v2_path] {
                assert_eq!(
                    fs::metadata(artifact)
                        .expect("read sidecar permissions")
                        .permissions()
                        .mode()
                        & 0o777,
                    0o600
                );
            }
        }
        let mut validator = TaxIdentitySidecarV2StreamValidator::new(
            fs::File::open(&v2_path).expect("open v2 sidecar"),
            3,
        )
        .expect("validate v2 sidecar header");
        let mut states = Vec::new();
        while let Some(record) = validator.next_record().expect("validate v2 record") {
            states.push(record.state());
        }
        states.sort_unstable_by_key(|state| *state as u8);
        assert_eq!(
            states,
            vec![
                TaxIdentityStateV2::MatchedEin,
                TaxIdentityStateV2::MatchedNpi,
                TaxIdentityStateV2::MatchedNpi,
            ]
        );
        assert_eq!(validator.records_validated(), 3);
        let v1_events = framed_json_records_named(
            &completed.stdout,
            "manifest_provider_group_tax_identity_sidecar_file",
        );
        let v2_events = framed_json_records_named(
            &completed.stdout,
            "manifest_provider_group_tax_identity_v2_sidecar_file",
        );
        assert_eq!(v1_events.len(), 1);
        assert_eq!(v2_events.len(), 1);
        assert_eq!(v1_events[0]["path"], v1_path.display().to_string());
        assert_eq!(v1_events[0]["bytes"], v1_bytes.len() as u64);
        assert_eq!(v1_events[0]["sha256"], file_sha256(&v1_path));
        assert_eq!(v1_events[0]["row_count"], 3);
        assert_eq!(v1_events[0]["provider_group_count"], 3);
        assert_eq!(v1_events[0]["matched_ein_count"], 1);
        assert_eq!(v1_events[0]["unsupported_type_count"], 2);
        assert_eq!(v2_events[0]["path"], v2_path.display().to_string());
        assert_eq!(v2_events[0]["bytes"], v2_bytes.len() as u64);
        assert_eq!(v2_events[0]["sha256"], file_sha256(&v2_path));
        assert_eq!(v2_events[0]["row_count"], 3);
        assert_eq!(v2_events[0]["provider_group_count"], 3);
        assert_eq!(v2_events[0]["matched_ein_count"], 1);
        assert_eq!(v2_events[0]["matched_npi_count"], 2);
        assert_eq!(v2_events[0]["missing_count"], 0);
        assert_eq!(v2_events[0]["malformed_count"], 0);
        assert_eq!(v2_events[0]["unsupported_type_count"], 0);
        assert!(!output
            .join("provider-group-tax-identity.ptg2tax.building")
            .exists());
        assert!(!output
            .join("provider-group-tax-identity-v2.ptg2tax.building")
            .exists());
        assert!(fs::read_dir(&output)
            .expect("read output directory")
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".ptg2-tax-identity-")));
    }
}

#[cfg(unix)]
#[test]
fn compact_cli_rejects_tax_identity_aliases_before_worker_outputs() {
    use std::os::unix::fs::symlink;

    let temporary = tempfile::tempdir().expect("temporary path-collision fixture root");
    let raw_fixture = br#"{
      "provider_references":[
        {"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1000000491]}]}
      ],
      "in_network":[{
        "billing_code_type":"CPT",
        "billing_code":"99213",
        "negotiation_arrangement":"ffs",
        "negotiated_rates":[
          {"provider_references":[7],"negotiated_prices":[{"negotiated_rate":100}]}
        ]
      }]
    }"#;
    for case in [
        "v1-raw-input",
        "v1-token-secret",
        "v1-manifest-final",
        "v2-raw-input",
        "v2-token-secret",
        "v2-manifest-symlink-parent",
        "price-worker",
        "price-worker-rotation",
        "provider-reference-worker",
    ] {
        let case_root = temporary.path().join(case);
        let source = case_root.join("rates.json");
        let output = case_root.join("output");
        fs::create_dir_all(&output).expect("create collision output directory");
        fs::write(&source, raw_fixture).expect("write collision fixture");
        let mut command = compact_v4_command(&source, &output);
        let secret = output.join("tin-token-secret.bin");
        let candidate = match case {
            "v1-raw-input" | "v2-raw-input" => source.clone(),
            "v1-token-secret" | "v2-token-secret" => secret.clone(),
            "v1-manifest-final" => output.join("provider-set-component.ptg2sc"),
            "v2-manifest-symlink-parent" => {
                let output_alias = case_root.join("output-alias");
                symlink(&output, &output_alias).expect("create output directory alias");
                output_alias.join("provider-set-component.ptg2sc")
            }
            "price-worker" => output.join("price-set-summary.copy.worker0000"),
            "price-worker-rotation" => {
                output.join("price-set-summary.copy.worker0000.part000000.ready")
            }
            "provider-reference-worker" => {
                let provider_group_base = output.join("provider-group-member.copy");
                command.env(
                    "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
                    &provider_group_base,
                );
                output.join("provider-group-member.copy.provider_refs.worker0000")
            }
            _ => unreachable!(),
        };
        if case.starts_with("v1-") {
            command.env(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_SIDECAR_PATH",
                &candidate,
            );
        } else {
            command.env(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH",
                &candidate,
            );
        }
        let source_before = fs::read(&source).expect("read source sentinel");
        let secret_before = fs::read(&secret).expect("read secret sentinel");
        let completed = command.output().expect("run collision scanner");
        assert!(!completed.status.success(), "{case} unexpectedly succeeded");
        let stderr = String::from_utf8_lossy(&completed.stderr);
        assert!(
            stderr.contains("configured tax identity output")
                || stderr.contains("configured output paths"),
            "{case} unexpected stderr: {stderr}"
        );
        for raw_identity in ["123456789", "1000000491"] {
            assert!(!stderr.contains(raw_identity));
            assert!(!String::from_utf8_lossy(&completed.stdout).contains(raw_identity));
        }
        assert_eq!(fs::read(&source).expect("reread source"), source_before);
        assert_eq!(fs::read(&secret).expect("reread secret"), secret_before);
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
        assert!(!output.join("provider-group-tax-identity.ptg2tax").exists());
        assert!(!output
            .join("provider-group-tax-identity.ptg2tax.building")
            .exists());
        assert!(fs::read_dir(&output)
            .expect("read collision output directory")
            .filter_map(Result::ok)
            .all(|entry| !entry
                .file_name()
                .to_string_lossy()
                .starts_with(".ptg2-tax-identity-")));
    }
}
