use super::*;

#[test]
fn skipped_inline_provider_quarantine_covers_raw_and_parsed_rates() {
    let parsed_rate = RateLite {
        provider_refs: Vec::new(),
        provider_groups: vec![json!({"npi": [-7, "malformed"]})],
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: Vec::new(),
        prepared_price_set: None,
    };
    let dedupe = SharedDedupe::new(1);
    record_skipped_inline_provider_quarantine(&parsed_rate, &dedupe, false).unwrap();

    let raw_rate = RateLite {
        provider_groups: Vec::new(),
        provider_groups_raw: Some(raw_provider_groups(
            r#"[{"npi":[]},{"npi":[-8,"also-malformed"]}]"#,
        )),
        ..parsed_rate.clone()
    };
    record_skipped_inline_provider_quarantine(&raw_rate, &dedupe, true).unwrap();

    let invalid_raw_rate = RateLite {
        provider_groups_raw: Some(raw_provider_groups("null")),
        ..raw_rate.clone()
    };
    assert!(record_skipped_inline_provider_quarantine(&invalid_raw_rate, &dedupe, true).is_err());

    let empty_rate = RateLite {
        provider_groups_raw: None,
        ..raw_rate
    };
    record_skipped_inline_provider_quarantine(&empty_rate, &dedupe, false).unwrap();

    let oversized_rate = RateLite {
        provider_groups: vec![json!({"npi": ["x".repeat(129)]})],
        ..parsed_rate.clone()
    };
    assert!(record_skipped_inline_provider_quarantine(&oversized_rate, &dedupe, false).is_err());
    assert!(record_skipped_inline_provider_quarantine(&oversized_rate, &dedupe, true).is_err());

    let full_numeric = SharedDedupe::new(1);
    fill_provider_identifier_quarantine(&full_numeric);
    let extra_numeric_rate = RateLite {
        provider_groups: vec![json!({"npi": [-1025]})],
        ..parsed_rate.clone()
    };
    assert!(
        record_skipped_inline_provider_quarantine(&extra_numeric_rate, &full_numeric, false,)
            .is_err()
    );

    let full_text = SharedDedupe::new(1);
    fill_provider_identifier_quarantine(&full_text);
    let extra_text_rate = RateLite {
        provider_groups: vec![json!({"npi": ["new-malformed"]})],
        ..parsed_rate.clone()
    };
    assert!(
        record_skipped_inline_provider_quarantine(&extra_text_rate, &full_text, false,).is_err()
    );

    let provider_group = json!({
        "tin": {"type": "ein", "value": "123456789"},
        "npi": [1234567890_i64, "new-malformed"],
    });
    let capped_rate = RateLite {
        provider_groups: vec![provider_group],
        ..parsed_rate.clone()
    };
    let capped_dedupe = SharedDedupe::new(1);
    fill_provider_identifier_quarantine(&capped_dedupe);
    let mut capped_sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    assert!(provider_entry_view_for_worker_rate(
        &HashMap::new(),
        &capped_rate,
        &mut capped_sinks,
        &capped_dedupe,
    )
    .is_err());

    let capped_v4_dedupe = v4_test_shared_dedupe(1);
    fill_provider_identifier_quarantine(&capped_v4_dedupe);
    let mut capped_v4_sinks =
        DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    assert!(resolve_v4_inline_provider_transform(
        &capped_rate,
        &mut capped_v4_sinks,
        &capped_v4_dedupe,
        &V4InlineProviderTransformSharedCache::new(0),
    )
    .is_err());

    let quarantine = dedupe
        .provider_identifier_quarantine()
        .unwrap()
        .payload()
        .unwrap();
    assert_eq!(
        quarantine["contract"],
        "ptg2_provider_identifier_quarantine_v2"
    );
    assert_eq!(quarantine["occurrence_count"], 4);
    assert_eq!(quarantine["distinct_value_count"], 4);
}
#[test]
fn strict_provider_definition_rejects_malformed_tin_and_network_metadata() {
    for invalid_tin in [
        Value::Null,
        json!(true),
        json!([]),
        json!({}),
        json!({"type": "ein"}),
        json!({"value": "123456789"}),
        json!({"type": 7, "value": "123456789"}),
        json!({"type": "ein", "value": 123456789}),
        json!({"type": " ", "value": "123456789"}),
        json!({"type": "ein", "value": " "}),
        json!({"type": "ein", "value": "123456789", "business_name": 7}),
    ] {
        let mut provider_ref = valid_provider_reference();
        provider_ref["provider_groups"][0]["tin"] = invalid_tin;
        assert!(provider_ref_definition(&provider_ref).is_err());
    }

    for invalid_network_name in [
        Value::Null,
        json!(true),
        json!(7),
        json!({}),
        json!([["network"]]),
        json!(["network", 7]),
    ] {
        let mut provider_ref = valid_provider_reference();
        provider_ref["network_name"] = invalid_network_name;
        assert!(provider_ref_definition(&provider_ref).is_err());
    }

    let mut scalar_provider_ref = valid_provider_reference();
    scalar_provider_ref["network_name"] = json!(" network one ");
    let (_, scalar_entry) = provider_ref_definition(&scalar_provider_ref).unwrap();
    assert_eq!(scalar_entry.network_names, vec!["network one"]);

    let mut singleton_array_provider_ref = valid_provider_reference();
    singleton_array_provider_ref["network_name"] = json!([" network one "]);
    let (_, singleton_array_entry) =
        provider_ref_definition(&singleton_array_provider_ref).unwrap();
    assert_eq!(scalar_entry.entry_hash, singleton_array_entry.entry_hash);

    let mut provider_ref = valid_provider_reference();
    provider_ref["network_name"] = json!(["network one", "network two"]);
    provider_ref_definition(&provider_ref).unwrap();
}
#[test]
fn v4_provider_definition_classifies_tin_without_rejecting_pricing() {
    for unavailable_tin in [
        Value::Null,
        json!(true),
        json!([]),
        json!({}),
        json!({"type": "ein"}),
        json!({"value": "123456789"}),
        json!({"type": 7, "value": "123456789"}),
        json!({"type": "ein", "value": 123456789}),
        json!({"type": " ", "value": "123456789"}),
        json!({"type": "ein", "value": " "}),
        json!({"type": "ein", "value": "01💥2345678"}),
        json!({"type": "ein", "value": "123456789", "business_name": 7}),
    ] {
        let mut provider_ref = valid_provider_reference();
        provider_ref["provider_groups"][0]["tin"] = unavailable_tin;
        provider_ref_definition_audited(&provider_ref, true).unwrap();
    }

    let mut provider_ref = valid_provider_reference();
    provider_ref["network_name"] = json!(["network", 7]);
    assert!(provider_ref_definition_audited(&provider_ref, true).is_err());
}
#[test]
fn strict_rate_parser_rejects_invalid_provider_reference_scalar_types() {
    for invalid in [r#""7""#, "true", "{}", "[]", "null", "7.5"] {
        let raw = format!(
            r#"{{"provider_references":[{invalid}],"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
        );
        let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
    }
}
#[test]
fn strict_rate_parser_rejects_non_string_network_names() {
    for invalid in ["true", "12", "{}", r#"["network", 12]"#] {
        let raw = format!(
            r#"{{"provider_references":[7],"network_name":{invalid},"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
        );
        assert!(read_rate_lite_from_reader(raw.as_bytes()).is_err(), "{raw}");
    }
    for valid in ["null", r#""network""#, r#"["network one","network two"]"#] {
        let raw = format!(
            r#"{{"provider_references":[7],"network_name":{valid},"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
        );
        assert!(read_rate_lite_from_reader(raw.as_bytes()).is_ok(), "{raw}");
    }
}
#[test]
fn strict_rate_parser_rejects_missing_or_empty_provider_membership() {
    for raw in [
        r#"{"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
        r#"{"provider_references":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
        r#"{"provider_groups":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
        r#"{"provider_references":[],"provider_groups":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
    ] {
        let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        assert!(error.to_string().contains("provider"), "{error}");
        assert!(read_rate_lite_bytes(raw.as_bytes()).is_err(), "{raw}");
    }
}
#[test]
fn strict_rate_parser_rejects_missing_or_empty_negotiated_prices() {
    for raw in [
        r#"{"provider_references":[7]}"#,
        r#"{"provider_references":[7],"negotiated_prices":[]}"#,
    ] {
        let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        assert!(error.to_string().contains("negotiated price"), "{error}");
        assert!(read_rate_lite_bytes(raw.as_bytes()).is_err(), "{raw}");
    }
}
#[test]
fn repeated_provider_group_ids_union_split_fragments_canonically() {
    let provider_ref = valid_provider_reference();
    let (key, mut entry) = provider_ref_definition(&provider_ref).unwrap();
    entry.source_locators.push(ProviderSourceLocator {
        shard: 1,
        offset: 20,
        length: 2,
    });
    let mut provider_map = HashMap::new();
    insert_provider_definition(&mut provider_map, key.clone(), entry.clone()).unwrap();
    insert_provider_definition(&mut provider_map, key.clone(), entry.clone()).unwrap();
    assert_eq!(provider_map.len(), 1);

    let mut split_ref = provider_ref.clone();
    split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
    let mut split_entry = build_provider_entry(&split_ref).unwrap();
    split_entry.source_locators.push(ProviderSourceLocator {
        shard: 0,
        offset: 10,
        length: 1,
    });
    let expected = combine_provider_entries(entry.clone(), split_entry.clone());
    insert_provider_definition(&mut provider_map, key.clone(), split_entry.clone()).unwrap();
    assert_eq!(provider_map.len(), 1);
    assert_eq!(provider_map.get(&key), Some(&expected));
    assert_eq!(provider_map[&key].provider_count, 2);
    assert_eq!(provider_map[&key].npi, vec![1234567890, 2222222222]);
    assert_eq!(provider_map[&key].source_locators, expected.source_locators);

    let mut conflicting_ref = provider_ref.clone();
    conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
    let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();
    let error =
        insert_provider_definition(&mut provider_map, key.clone(), conflicting_entry).unwrap_err();
    assert!(error
        .to_string()
        .contains("conflicting provider_group_id definition: 7"));
    assert_eq!(provider_map.get(&key), Some(&expected));

    let mut left = HashMap::new();
    left.insert(key.clone(), entry.clone());
    let mut right = HashMap::new();
    right.insert(key.clone(), entry.clone());
    let merged = merge_provider_maps_pairwise(vec![(0, left), (1, right)]).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged.get(&key), Some(&entry));

    let mut right = HashMap::new();
    right.insert(key.clone(), entry.clone());
    let merged = merge_provider_maps_pairwise(vec![(0, HashMap::new()), (1, right)]).unwrap();
    assert_eq!(merged.len(), 1);
    assert_eq!(merged.get(&key), Some(&entry));

    let mut left = HashMap::new();
    left.insert(key.clone(), entry.clone());
    let mut right = HashMap::new();
    right.insert(key, split_entry);
    let merged = merge_provider_maps_pairwise(vec![(0, left), (1, right)]).unwrap();
    assert_eq!(merged.values().next(), Some(&expected));
}
#[test]
fn distinct_scope_conflicts_preserve_same_scope_unions_deterministically() {
    let provider_ref = valid_provider_reference();
    let (key, entry) = provider_ref_definition(&provider_ref).unwrap();
    let mut split_ref = provider_ref.clone();
    split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
    let split_entry = build_provider_entry(&split_ref).unwrap();
    let expected_scope = combine_provider_entries(entry.clone(), split_entry.clone());
    let mut conflicting_ref = provider_ref.clone();
    conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
    let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();

    let mut forward = ProviderDefinitions::default();
    forward.insert(key.clone(), entry.clone()).unwrap();
    forward.insert(key.clone(), split_entry.clone()).unwrap();
    forward
        .insert(key.clone(), conflicting_entry.clone())
        .unwrap();
    let (forward_map, forward_conflicts) = forward.clone().into_materialized().unwrap();
    assert!(!forward_map.contains_key(&key));
    assert_eq!(forward_conflicts.definition_count, 2);
    assert_eq!(
        forward_conflicts.definitions_by_key[&key].get(&expected_scope.provider_group_scope_hash),
        Some(&expected_scope)
    );

    let mut reverse = ProviderDefinitions::default();
    reverse
        .insert(key.clone(), conflicting_entry.clone())
        .unwrap();
    reverse.insert(key.clone(), split_entry.clone()).unwrap();
    reverse.insert(key.clone(), entry.clone()).unwrap();
    let (reverse_map, reverse_conflicts) = reverse.into_materialized().unwrap();
    assert_eq!(reverse_conflicts, forward_conflicts);

    let forward_payload = provider_identifier_quarantine_payload(
        &forward_map,
        ProviderIdentifierQuarantine::default(),
        &forward_conflicts,
        &[0x11; 32],
    )
    .unwrap();
    let reverse_payload = provider_identifier_quarantine_payload(
        &reverse_map,
        ProviderIdentifierQuarantine::default(),
        &reverse_conflicts,
        &[0x11; 32],
    )
    .unwrap();
    assert_eq!(forward_payload, reverse_payload);
    assert_eq!(
        forward_payload["contract"],
        "ptg2_provider_identifier_quarantine_v2"
    );
    assert_eq!(forward_payload["provider_group_conflict_count"], 1);
    assert_eq!(
        forward_payload["provider_group_conflicting_definition_count"],
        2
    );

    let mut left = ProviderDefinitions::default();
    left.insert(key.clone(), entry).unwrap();
    let mut right = ProviderDefinitions::default();
    right.insert(key.clone(), split_entry).unwrap();
    right.insert(key.clone(), conflicting_entry).unwrap();
    let merged = merge_provider_definitions_pairwise(vec![(0, left), (1, right)]).unwrap();
    let (_merged_map, merged_conflicts) = merged.into_materialized().unwrap();
    assert_eq!(merged_conflicts, forward_conflicts);

    let referenced_rate = RateLite {
        provider_refs: vec![key.clone()],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: Vec::new(),
        prepared_price_set: None,
    };
    assert!(validate_unreferenced_provider_group_conflicts(&[], &forward_conflicts.keys()).is_ok());
    let error =
        validate_unreferenced_provider_group_conflicts(&[referenced_rate], &HashSet::from([key]))
            .unwrap_err();
    assert!(error
        .to_string()
        .contains("referenced conflicting provider_group_id definition: 7"));
}
#[test]
fn provider_conflict_identity_is_scoped_to_the_raw_source() {
    let key = ProviderRefKey::from("7");

    assert_eq!(
        provider_ref_key_sha256(&[0x11; 32], &key),
        provider_ref_key_sha256(&[0x11; 32], &key),
    );
    assert_ne!(
        provider_ref_key_sha256(&[0x11; 32], &key),
        provider_ref_key_sha256(&[0x22; 32], &key),
    );
}
#[test]
fn provider_definition_parts_dedupe_before_scope_union_across_orders_and_workers() {
    let mut once_ref = valid_provider_reference();
    once_ref["provider_groups"][0]["npi"] = json!([1234567890_i64, -1_i64]);
    let (key, mut once) = provider_ref_definition(&once_ref).unwrap();
    let mut duplicate = once.clone();
    once.source_locators.push(ProviderSourceLocator {
        shard: 1,
        offset: 20,
        length: 2,
    });
    duplicate.source_locators.push(ProviderSourceLocator {
        shard: 0,
        offset: 30,
        length: 2,
    });

    let mut twice_ref = once_ref.clone();
    twice_ref["provider_groups"][0]["npi"] = json!([1234567890_i64, -1_i64, -1_i64]);
    let mut twice = build_provider_entry(&twice_ref).unwrap();
    twice.source_locators.push(ProviderSourceLocator {
        shard: 0,
        offset: 40,
        length: 3,
    });

    let mut conflicting_ref = once_ref.clone();
    conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
    conflicting_ref["provider_groups"][0]["npi"] = json!([2222222222_i64, -2_i64, "bad"]);
    let mut conflicting = build_provider_entry(&conflicting_ref).unwrap();
    conflicting.source_locators.push(ProviderSourceLocator {
        shard: 1,
        offset: 50,
        length: 4,
    });

    let mut forward = ProviderDefinitions::default();
    for entry in [
        once.clone(),
        duplicate.clone(),
        twice.clone(),
        conflicting.clone(),
    ] {
        forward.insert(key.clone(), entry).unwrap();
    }
    let (forward_map, forward_conflicts) = forward.into_materialized().unwrap();
    assert!(forward_map.is_empty());
    let retained_scope =
        &forward_conflicts.definitions_by_key[&key][&once.provider_group_scope_hash];
    assert_eq!(retained_scope.source_locators.len(), 2);
    let expected_payload = provider_identifier_quarantine_payload(
        &forward_map,
        ProviderIdentifierQuarantine::default(),
        &forward_conflicts,
        &[0x11; 32],
    )
    .unwrap();
    assert_eq!(expected_payload["occurrence_count"], 5);
    assert_eq!(expected_payload["distinct_value_count"], 3);
    assert_eq!(expected_payload["entries"][0]["value"], "-2");
    assert_eq!(expected_payload["entries"][0]["occurrence_count"], 1);
    assert_eq!(expected_payload["entries"][1]["value"], "-1");
    assert_eq!(expected_payload["entries"][1]["occurrence_count"], 3);
    assert_eq!(expected_payload["entries"][2]["kind"], "string");
    assert_eq!(expected_payload["entries"][2]["occurrence_count"], 1);

    let mut reverse = ProviderDefinitions::default();
    for entry in [
        conflicting.clone(),
        twice.clone(),
        duplicate.clone(),
        once.clone(),
    ] {
        reverse.insert(key.clone(), entry).unwrap();
    }
    let (reverse_map, reverse_conflicts) = reverse.into_materialized().unwrap();
    assert_eq!(
        provider_identifier_quarantine_payload(
            &reverse_map,
            ProviderIdentifierQuarantine::default(),
            &reverse_conflicts,
            &[0x11; 32],
        )
        .unwrap(),
        expected_payload
    );

    let mut left = ProviderDefinitions::default();
    left.insert(key.clone(), once).unwrap();
    left.insert(key.clone(), twice).unwrap();
    let mut right = ProviderDefinitions::default();
    right.insert(key.clone(), duplicate).unwrap();
    right.insert(key, conflicting).unwrap();
    let merged = merge_provider_definitions_pairwise(vec![(0, left), (1, right)]).unwrap();
    let (merged_map, merged_conflicts) = merged.into_materialized().unwrap();
    assert_eq!(
        provider_identifier_quarantine_payload(
            &merged_map,
            ProviderIdentifierQuarantine::default(),
            &merged_conflicts,
            &[0x11; 32],
        )
        .unwrap(),
        expected_payload
    );
}
#[test]
fn retained_provider_definition_replay_verifies_source_identity() {
    let provider_ref = valid_provider_reference();
    let raw_provider = serde_json::to_vec(&provider_ref).unwrap();
    let (key, mut entry) = provider_ref_definition(&provider_ref).unwrap();
    let source_witness = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
    source_witness.configure_provider_spools(1).unwrap();
    entry.source_locators.push(
        source_witness
            .store_provider_source(0, &raw_provider)
            .unwrap(),
    );
    source_witness.seal_provider_sources().unwrap();
    let dedupe = SharedDedupe::new(1);
    let context = ProviderRefBatchContext {
        dedupe: &dedupe,
        source_witness: &source_witness,
        manifest_sidecars: None,
        allow_empty_npi_tin_only: false,
        defer_definition_outputs: false,
    };

    emit_provider_definition_outputs(
        &HashMap::from([(key.clone(), entry.clone())]),
        1,
        &CopyPathConfig::default(),
        0,
        context,
    )
    .unwrap();

    let mut drifted_entry = entry;
    drifted_entry.npi.push(2_222_222_222);
    drifted_entry.provider_count = 2;
    let drift_dedupe = SharedDedupe::new(1);
    let drift_sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
    let drift_context = ProviderRefBatchContext {
        dedupe: &drift_dedupe,
        source_witness: &source_witness,
        manifest_sidecars: Some(&drift_sidecars),
        allow_empty_npi_tin_only: false,
        defer_definition_outputs: false,
    };
    let error = emit_provider_definition_outputs(
        &HashMap::from([(key, drifted_entry)]),
        1,
        &CopyPathConfig::default(),
        0,
        drift_context,
    )
    .err()
    .expect("source identity drift must fail closed");
    assert!(error.to_string().contains("differs from its source spool"));
    assert!(lock_manifest_sidecars(&drift_sidecars)
        .provider_component_group
        .is_empty());
}
#[test]
fn retained_provider_replay_validates_every_definition_before_any_output() {
    let directory = tempfile::tempdir().unwrap();
    let first_provider = valid_provider_reference();
    let mut second_provider = valid_provider_reference();
    second_provider["provider_group_id"] = json!(8);
    second_provider["provider_groups"][0]["npi"] = json!([2222222222_i64]);
    let first_raw = serde_json::to_vec(&first_provider).unwrap();
    let second_raw = serde_json::to_vec(&second_provider).unwrap();
    let source_witness = SourceWitnessCollector::new(&"ef".repeat(32)).unwrap();
    source_witness.configure_provider_spools(1).unwrap();
    let (first_key, mut first_entry) = provider_ref_definition(&first_provider).unwrap();
    first_entry
        .source_locators
        .push(source_witness.store_provider_source(0, &first_raw).unwrap());
    let (second_key, mut second_entry) = provider_ref_definition(&second_provider).unwrap();
    second_entry.source_locators.push(
        source_witness
            .store_provider_source(0, &second_raw)
            .unwrap(),
    );
    assert!(first_entry.source_locators[0].offset < second_entry.source_locators[0].offset);
    source_witness.seal_provider_sources().unwrap();
    second_entry.npi.push(3_333_333_333);
    second_entry.provider_count += 1;

    let dedupe = SharedDedupe::new(1);
    let sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
    let context = ProviderRefBatchContext {
        dedupe: &dedupe,
        source_witness: &source_witness,
        manifest_sidecars: Some(&sidecars),
        allow_empty_npi_tin_only: false,
        defer_definition_outputs: false,
    };
    let base_path = directory.path().join("provider-group-member.copy");
    let copy_paths = CopyPathConfig {
        manifest_provider_group_member: Some(base_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let worker_path = copy_paths
        .for_worker(0)
        .manifest_provider_group_member
        .unwrap();

    let error = emit_provider_definition_outputs(
        &HashMap::from([(first_key, first_entry), (second_key, second_entry)]),
        1,
        &copy_paths,
        0,
        context,
    )
    .err()
    .expect("later source identity drift must fail closed");

    assert!(error.to_string().contains("differs from its source spool"));
    assert!(!Path::new(&worker_path).exists());
    assert!(lock_manifest_sidecars(&sidecars)
        .provider_component_group
        .is_empty());
    let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(summary["provider_group_attempted"], 0);
    assert_eq!(summary["provider_group_member_attempted"], 0);
}
#[test]
fn retained_split_provider_replay_records_the_aggregate_component() {
    let provider_ref = valid_provider_reference();
    let mut split_ref = provider_ref.clone();
    split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
    let source_witness = SourceWitnessCollector::new(&"cd".repeat(32)).unwrap();
    source_witness.configure_provider_spools(1).unwrap();
    let mut definitions = ProviderDefinitions::default();
    for value in [&provider_ref, &split_ref] {
        let raw_provider = serde_json::to_vec(value).unwrap();
        let (key, mut entry) = provider_ref_definition(value).unwrap();
        entry.source_locators.push(
            source_witness
                .store_provider_source(0, &raw_provider)
                .unwrap(),
        );
        definitions.insert(key, entry).unwrap();
    }
    let (provider_map, conflicts) = definitions.into_materialized().unwrap();
    assert!(conflicts.definitions_by_key.is_empty());
    source_witness.seal_provider_sources().unwrap();
    let expected_entry = provider_map.values().next().unwrap();
    let expected_component_id = provider_component_global_id_from_hash(expected_entry.entry_hash);
    let expected_group_ids = expected_entry
        .provider_group_hashes
        .iter()
        .map(|hash| provider_group_global_id_from_hash(*hash))
        .collect::<Vec<_>>();
    let dedupe = SharedDedupe::new(1);
    let sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
    let context = ProviderRefBatchContext {
        dedupe: &dedupe,
        source_witness: &source_witness,
        manifest_sidecars: Some(&sidecars),
        allow_empty_npi_tin_only: false,
        defer_definition_outputs: false,
    };

    emit_provider_definition_outputs(&provider_map, 1, &CopyPathConfig::default(), 0, context)
        .unwrap();

    assert_eq!(
        lock_manifest_sidecars(&sidecars)
            .provider_component_group
            .get(&expected_component_id),
        Some(&expected_group_ids)
    );
}
#[test]
fn provider_quarantine_counts_only_globally_deduped_definitions() {
    let definition = json!({
        "provider_group_id": 7,
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890_i64, -7_i64, "1447744750`"]
        }]
    });
    let (key, entry) = provider_ref_definition(&definition).unwrap();
    let mut provider_map = merge_provider_maps_pairwise(vec![
        (0, HashMap::from([(key.clone(), entry.clone())])),
        (1, HashMap::from([(key.clone(), entry.clone())])),
    ])
    .unwrap();
    let dedupe = SharedDedupe::new(1);

    record_and_clear_provider_identifier_quarantine(&mut provider_map, &dedupe).unwrap();

    let quarantine = dedupe
        .provider_identifier_quarantine()
        .unwrap()
        .payload()
        .unwrap();
    assert_eq!(quarantine["occurrence_count"], 2);
    assert_eq!(quarantine["distinct_value_count"], 2);
    assert!(provider_map[&key].quarantined_npi.is_empty());
    assert!(provider_map[&key].quarantined_npi_text.is_empty());

    let mut invalid_numeric_entry = entry.clone();
    invalid_numeric_entry.quarantined_npi = vec![0];
    assert!(record_and_clear_provider_identifier_quarantine(
        &mut HashMap::from([(
            ProviderRefKey::from("invalid-numeric"),
            invalid_numeric_entry,
        )]),
        &SharedDedupe::new(1),
    )
    .is_err());

    let mut invalid_text_entry = entry.clone();
    invalid_text_entry.quarantined_npi_text = vec!["x".repeat(129)];
    assert!(record_and_clear_provider_identifier_quarantine(
        &mut HashMap::from([(ProviderRefKey::from("invalid-text"), invalid_text_entry)]),
        &SharedDedupe::new(1),
    )
    .is_err());

    let mut split_entry = entry.clone();
    split_entry
        .quarantined_npi_text
        .push("1447744750`".to_string());
    let mut split_map = merge_provider_maps_pairwise(vec![
        (0, HashMap::from([(key.clone(), entry)])),
        (1, HashMap::from([(key.clone(), split_entry)])),
    ])
    .unwrap();
    let split_dedupe = SharedDedupe::new(1);
    record_and_clear_provider_identifier_quarantine(&mut split_map, &split_dedupe).unwrap();
    let split_quarantine = split_dedupe
        .provider_identifier_quarantine()
        .unwrap()
        .payload()
        .unwrap();
    assert_eq!(split_quarantine["occurrence_count"], 5);
    assert_eq!(split_quarantine["distinct_value_count"], 2);
    assert!(split_map[&key].quarantined_npi.is_empty());
    assert!(split_map[&key].quarantined_npi_text.is_empty());
}
#[test]
fn raw_provider_reference_worker_accepts_identical_group_ids() {
    let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = SharedDedupe::new(1);
    let mut provider_map = HashMap::new();
    let raw_ref = serde_json::to_vec(&valid_provider_reference()).unwrap();
    let mut raw_refs = RawRateChunk::with_capacity(2, raw_ref.len() * 2);
    for _ in 0..2 {
        let start = raw_refs.byte_len();
        raw_refs.bytes.extend_from_slice(&raw_ref);
        raw_refs.push_current_value_span(start);
    }

    let processed =
        process_provider_ref_raw_batch(&raw_refs, &mut provider_map, &mut sinks, &dedupe, false)
            .unwrap();

    assert_eq!(processed, 2);
    assert_eq!(provider_map.len(), 1);
}
#[test]
fn raw_provider_reference_worker_preserves_empty_npi_group_without_edges() {
    let mut provider_ref = valid_provider_reference();
    provider_ref["provider_groups"][0]["npi"] = json!([]);
    let raw_ref = serde_json::to_vec(&provider_ref).unwrap();
    let mut raw_refs = RawRateChunk::with_capacity(1, raw_ref.len());
    raw_refs.bytes.extend_from_slice(&raw_ref);
    raw_refs.push_current_value_span(0);

    let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = v4_test_shared_dedupe(2);
    let mut provider_map = HashMap::new();
    let processed =
        process_provider_ref_raw_batch(&raw_refs, &mut provider_map, &mut sinks, &dedupe, true)
            .unwrap();

    assert_eq!(processed, 1);
    assert_eq!(provider_map.len(), 1);
    let entry = provider_map.values().next().unwrap();
    assert_eq!(entry.provider_count, 0);
    assert!(entry.npi.is_empty());
    let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(dedupe.empty_npi_tin_only_normalization_count(), 1);
    assert_eq!(summary["provider_group_attempted"], 1);
    assert_eq!(summary["provider_group_member_attempted"], 0);
}
#[test]
fn preloaded_provider_definitions_are_idempotent_and_conflicts_fail_closed() {
    let provider_ref = valid_provider_reference();
    let (key, entry) = provider_ref_definition(&provider_ref).unwrap();
    let mut provider_map = HashMap::new();
    provider_map.insert(key.clone(), entry.clone());
    validate_preloaded_provider_definition(&provider_map, &key, &entry).unwrap();
    validate_preloaded_provider_definition(&provider_map, &key, &entry).unwrap();

    let mut conflicting_ref = provider_ref.clone();
    conflicting_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
    let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();

    let error = validate_preloaded_provider_definition(&provider_map, &key, &conflicting_entry)
        .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error
        .to_string()
        .contains("provider_group_id definition changed after preflight: 7"));
}
#[test]
fn procedure_and_v3_code_identity_normalize_arrangement_consistently() {
    let first = json!({
        "billing_code_type": " cpt ",
        "billing_code_type_version": "2026",
        "billing_code": " 99213 ",
        "negotiation_arrangement": "ffs",
        "name": "First display name",
        "description": "First description"
    });
    let same_identity = json!({
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "FFS",
        "name": "Different display name",
        "description": "Different description"
    });
    let bundled = json!({
        "billing_code_type": "CPT",
        "billing_code_type_version": "2026",
        "billing_code": "99213",
        "negotiation_arrangement": "bundle"
    });

    let first_payload = procedure_identity_payload(&first);
    let same_payload = procedure_identity_payload(&same_identity);
    let bundled_payload = procedure_identity_payload(&bundled);

    assert_eq!(first_payload, same_payload);
    assert_eq!(
        procedure_global_id(&first_payload),
        procedure_global_id(&same_payload)
    );
    assert_ne!(
        procedure_global_id(&first_payload),
        procedure_global_id(&bundled_payload)
    );

    let drg_alias = json!({
        "billing_code_type": "MS-DRG",
        "billing_code": "7",
        "negotiation_arrangement": "ffs"
    });
    let drg_canonical = json!({
        "billing_code_type": "MS_DRG",
        "billing_code": "007",
        "negotiation_arrangement": "FFS"
    });
    assert_eq!(
        procedure_identity_payload(&drg_alias),
        procedure_identity_payload(&drg_canonical)
    );
    assert_eq!(
        procedure_global_id(&procedure_identity_payload(&drg_alias)),
        procedure_global_id(&procedure_identity_payload(&drg_canonical))
    );

    let scope = [0x44; COVERAGE_SCOPE_ID_BYTES];
    let first_arrangement = normalize_code(first.get("negotiation_arrangement"));
    let same_arrangement = normalize_code(same_identity.get("negotiation_arrangement"));
    let bundled_arrangement = normalize_code(bundled.get("negotiation_arrangement"));
    let first_code_id = natural_lean_code_identity(
        &scope,
        Some("CPT"),
        Some("99213"),
        first_arrangement.as_deref(),
        None,
        None,
        None,
    );
    assert_eq!(
        first_code_id,
        natural_lean_code_identity(
            &scope,
            Some("CPT"),
            Some("99213"),
            same_arrangement.as_deref(),
            None,
            None,
            None,
        )
    );
    assert_ne!(
        first_code_id,
        natural_lean_code_identity(
            &scope,
            Some("CPT"),
            Some("99213"),
            bundled_arrangement.as_deref(),
            None,
            None,
            None,
        )
    );
}
#[test]
fn provider_set_identity_depends_on_groups_not_reference_packaging() {
    let group_a = json!({
        "tin": {"type": "ein", "value": "111111111"},
        "npi": [1111111111]
    });
    let group_b = json!({
        "tin": {"type": "ein", "value": "222222222"},
        "npi": [2222222222_i64]
    });
    let entry_a = build_provider_entry(&json!({"provider_groups": [group_a.clone()]})).unwrap();
    let entry_b = build_provider_entry(&json!({"provider_groups": [group_b.clone()]})).unwrap();
    let combined = build_provider_entry(&json!({"provider_groups": [group_a, group_b]})).unwrap();
    let mut provider_map = HashMap::new();
    provider_map.insert(ProviderRefKey::from("a"), entry_a);
    provider_map.insert(ProviderRefKey::from("b"), entry_b);
    let separate = provider_set_from_ref_keys(
        &provider_map,
        &[ProviderRefKey::from("a"), ProviderRefKey::from("b")],
    )
    .unwrap()
    .unwrap();

    assert_eq!(
        separate.provider_group_hashes,
        combined.provider_group_hashes
    );
    assert_eq!(
        provider_set_global_id_from_group_hashes_and_network_names(
            &separate.provider_group_hashes,
            &separate.network_names,
        ),
        provider_set_global_id_from_group_hashes_and_network_names(
            &combined.provider_group_hashes,
            &combined.network_names,
        )
    );
    assert_eq!(
        hash_i64_list("provider_set", &separate.provider_group_hashes),
        hash_i64_list("provider_set", &combined.provider_group_hashes)
    );
}
#[test]
fn raw_provider_reference_batch_retains_npis_for_exact_counts() {
    let paths = CopyPathConfig {
        compact: None,
        manifest_serving: None,
        manifest_lean_serving: None,
        v3_serving_run_directory: None,
        v3_coverage_scope_id: None,
        manifest_provider_forward_sidecar: None,
        manifest_provider_inverted_sidecar: None,
        manifest_provider_set_component_sidecar: None,
        manifest_provider_component_group_sidecar: None,
        manifest_provider_group_tax_identity_sidecar: None,
        manifest_provider_group_tax_identity_v2_sidecar: None,
        manifest_provider_npi_sidecar: None,
        manifest_price_forward_sidecar: None,
        manifest_price_atom: None,
        manifest_price_set_atom: None,
        manifest_price_set_summary: None,
        manifest_provider_group_member: None,
        manifest_code_count: None,
        manifest_provider_set_dictionary: None,
        procedure: None,
        price_code_set: None,
        price_atom: None,
        price_set_entry: None,
        provider_set: None,
        provider_set_component: None,
        provider_set_entry: None,
        provider_entry_component: None,
        provider_group_member: None,
        manifest_only: true,
    };
    let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let dedupe = SharedDedupe::new(2);
    let mut provider_map = HashMap::new();
    let raw_ref_values = [
        br#"{"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890,1234567891]}]}"#.to_vec(),
        br#"{"provider_group_id":8,"provider_groups":[{"tin":{"type":"npi","value":"9876543210"},"npi":[2222222222]}]}"#.to_vec(),
        br#"{"provider_group_id":121591448686103182592848195376305442061,"provider_groups":[{"tin":{"type":"ein","value":"462560124"},"npi":[1265502504]}]}"#.to_vec(),
    ];
    let mut raw_refs = RawRateChunk::with_capacity(raw_ref_values.len(), 1024);
    for raw_ref in raw_ref_values {
        let start = raw_refs.byte_len();
        raw_refs.bytes.extend_from_slice(&raw_ref);
        raw_refs.push_current_value_span(start);
    }

    let processed =
        process_provider_ref_raw_batch(&raw_refs, &mut provider_map, &mut sinks, &dedupe, false)
            .unwrap();

    assert_eq!(processed, 3);
    assert_eq!(provider_map.len(), 3);
    let key_7 = ProviderRefKey::from("7");
    let key_8 = ProviderRefKey::from("8");
    let wide_key = ProviderRefKey::from("121591448686103182592848195376305442061");
    assert!(provider_map.contains_key(&key_7));
    assert!(provider_map.contains_key(&key_8));
    assert!(provider_map.contains_key(&wide_key));
    assert_eq!(provider_map[&key_7].provider_count, 2);
    assert_eq!(provider_map[&key_8].provider_count, 1);
    assert_eq!(provider_map[&wide_key].provider_count, 1);
    assert_eq!(provider_map[&key_7].npi, vec![1234567890, 1234567891]);
    assert!(!provider_map[&key_7].provider_group_hashes.is_empty());
}
#[test]
fn worker_handles_mixed_top_level_references_and_inline_provider_groups() {
    assert_worker_handles_mixed_referenced_and_inline_rates(false);
}
#[test]
fn v4_inline_empty_npi_rate_preserves_price_and_group_without_npi_edges() {
    let raw_rate = br#"{
        "provider_groups":[{
            "tin":{"type":"ein","value":"444444444"},
            "npi":[]
        }],
        "negotiated_prices":[{"negotiated_rate":103.00}]
    }"#;
    assert!(read_rate_lite_bytes_profiled_with_policy(raw_rate, false).is_err());
    let (rate, _typed) = read_rate_lite_bytes_profiled_with_policy(raw_rate, true).unwrap();
    let rate = rate.unwrap();
    let price_set = rate_price_set(&rate).unwrap();
    assert_eq!(price_set.atoms.len(), 1);
    assert_eq!(price_set.atoms[0].negotiated_rate, "103");

    let mut dictionary_sinks =
        DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = v4_test_shared_dedupe(2);
    let mut provider_cache = ProviderSetScopeCache::with_v4_factor_mode(true);
    let provider_map = HashMap::new();
    let resolved = resolve_worker_provider(
        &provider_map,
        &rate,
        &mut dictionary_sinks,
        &dedupe,
        &mut provider_cache,
        &test_compact_context(),
    )
    .unwrap()
    .unwrap();

    assert!(resolved.is_v4_factor());
    assert_eq!(resolved.provider_count(), 0);
    assert_eq!(resolved.emitted_npis().len(), 0);
    let (component_hash, group_hashes) = resolved.inline_component().unwrap();
    assert_ne!(component_hash, 0);
    assert_eq!(group_hashes.len(), 1);
    assert_eq!(
        group_hashes[0],
        provider_group_hash(&resolved.inline_provider_groups()[0]["tin"], &[], &[], &[])
    );
    assert_eq!(dedupe.empty_npi_tin_only_normalization_count(), 1);
    let dedupe_summary = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(dedupe_summary["provider_group_attempted"], 1);
    assert_eq!(dedupe_summary["provider_group_unique"], 1);
    assert_eq!(dedupe_summary["provider_group_member_attempted"], 0);
}
#[test]
fn mixed_valid_and_dangling_provider_refs_fail_serial_and_worker_paths() {
    let provider_ref = json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890, "bad`"]
        }]
    });
    let mut provider_map = HashMap::new();
    provider_map.insert(
        ProviderRefKey::from("valid-ref"),
        build_provider_entry(&provider_ref).unwrap(),
    );
    let rates = vec![RateLite {
        provider_refs: vec![
            ProviderRefKey::from("valid-ref"),
            ProviderRefKey::from("dangling-ref"),
        ],
        provider_groups: provider_ref["provider_groups"].as_array().unwrap().clone(),
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: vec![test_price_lite("100.00")],
        prepared_price_set: None,
    }];
    let procedure = json!({"billing_code_type": "CPT", "billing_code": "99213"});
    let paths = CopyPathConfig::default();

    let mut serial_writer = Vec::new();
    let mut serial_compact_copy_writer = None;
    let mut serial_manifest_serving_copy_writer = None;
    let mut serial_dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let mut emitted_price_code_sets = HashSet::new();
    let mut emitted_price_atoms = HashSet::new();
    let mut emitted_price_sets = HashSet::new();
    let mut emitted_price_set_entries = HashSet::new();
    let mut emitted_provider_sets = HashSet::new();
    let mut emitted_provider_set_components = HashSet::new();
    let mut emitted_provider_set_entries = HashSet::new();
    let mut emitted_provider_entry_components = HashSet::new();
    let mut emitted_procedures = HashSet::new();
    let mut emitted_provider_group_members = HashSet::new();
    let mut provider_identifier_quarantine = ProviderIdentifierQuarantine::default();
    let mut serial_manifest_global_id_cache = ManifestGlobalIdCache::default();
    let context = test_compact_context();
    let mut outputs = LocalCompactOutputs {
        writer: &mut serial_writer,
        compact_copy_writer: &mut serial_compact_copy_writer,
        manifest_serving_copy_writer: &mut serial_manifest_serving_copy_writer,
        dictionary_copy_sinks: &mut serial_dictionary_copy_sinks,
        manifest_sidecars: None,
        record_price_forward_sidecar: false,
        suppress_legacy_row_output: false,
    };
    let mut serial_dedupe = LocalCompactDedupe {
        price_code_sets: &mut emitted_price_code_sets,
        price_atoms: &mut emitted_price_atoms,
        price_sets: &mut emitted_price_sets,
        price_set_entries: &mut emitted_price_set_entries,
        provider_sets: &mut emitted_provider_sets,
        provider_set_components: &mut emitted_provider_set_components,
        provider_set_entries: &mut emitted_provider_set_entries,
        provider_entry_components: &mut emitted_provider_entry_components,
        procedures: &mut emitted_procedures,
        provider_group_members: &mut emitted_provider_group_members,
        provider_identifier_quarantine: &mut provider_identifier_quarantine,
    };
    let mut batch = CompactRateBatch {
        provider_map: &provider_map,
        manifest_global_id_cache: &mut serial_manifest_global_id_cache,
        rates: &rates,
        procedure_value: &procedure,
        context: &context,
    };
    let serial_error =
        process_compact_rate_lites(&mut outputs, &mut serial_dedupe, &mut batch).unwrap_err();

    assert_eq!(serial_error.kind(), io::ErrorKind::InvalidData);
    assert!(serial_error.to_string().contains("dangling-ref"));
    assert!(!String::from_utf8(serial_writer)
        .unwrap()
        .contains("serving_rate_compact"));

    let mut worker_writer = Vec::new();
    let mut worker_compact_copy_writer = None;
    let mut worker_manifest_serving_copy_writer = None;
    let mut worker_dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let worker_dedupe = SharedDedupe::new(1);
    let mut worker_dedupe_cache = WorkerDedupeCache::new(16);
    let mut worker_provider_set_scope_cache = ProviderSetScopeCache::default();
    let mut worker_manifest_global_id_cache = ManifestGlobalIdCache::default();
    let mut worker_state = SharedCompactState {
        writer: &mut worker_writer,
        compact_copy_writer: &mut worker_compact_copy_writer,
        manifest_serving_copy_writer: &mut worker_manifest_serving_copy_writer,
        dictionary_copy_sinks: &mut worker_dictionary_copy_sinks,
        manifest_sidecars: None,
        record_price_forward_sidecar: false,
        suppress_legacy_row_output: false,
        provider_map: &provider_map,
        dedupe: &worker_dedupe,
        worker_dedupe_cache: &mut worker_dedupe_cache,
        provider_set_scope_cache: &mut worker_provider_set_scope_cache,
        manifest_global_id_cache: &mut worker_manifest_global_id_cache,
        context: &context,
    };
    let worker_error = process_compact_rate_lites_worker_with_grouping(
        &mut worker_state,
        &rates,
        &procedure,
        false,
    )
    .unwrap_err();

    assert_eq!(worker_error.kind(), io::ErrorKind::InvalidData);
    assert!(worker_error.to_string().contains("dangling-ref"));
    assert!(!String::from_utf8(worker_writer)
        .unwrap()
        .contains("serving_rate_compact"));
}
#[test]
fn reversed_top_level_order_imports_referenced_rates_end_to_end() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-reversed-top-level-order-success-{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&base);
    let input_path = base.join("input.json");
    let serving_path = base.join("serving.copy");
    let serving_run_directory = base.join("serving-runs");
    let price_atom_path = base.join("price-atom.copy");
    let provider_member_path = base.join("provider-group-member.copy");
    write_reversed_provider_reference_fixture(&input_path, &[7]);
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
        TestEnvVar::set(
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
            serving_path.to_str().unwrap(),
        ),
        TestEnvVar::set(
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
            price_atom_path.to_str().unwrap(),
        ),
        TestEnvVar::set(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
            provider_member_path.to_str().unwrap(),
        ),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    scan_compact_struson(&input_path).unwrap();

    let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum::<u64>();
    let code_dictionary_path = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains("-codes.v4") && name.ends_with(".ready"))
        })
        .unwrap();
    let code_dictionary = read_code_dictionary(code_dictionary_path).unwrap();
    let price_atom_rows = read_worker_copy_text(&price_atom_path).unwrap();
    let provider_member_rows = read_worker_copy_text(&provider_member_path).unwrap();
    assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
    assert_eq!(code_dictionary.len(), 1);
    assert_eq!(
        code_dictionary[0].negotiation_arrangement.as_deref(),
        Some("FFS")
    );
    assert!(!serving_path.exists());
    assert_eq!(price_atom_rows.lines().count(), 1);
    assert_eq!(
        price_atom_rows.lines().next().unwrap().split('\t').nth(2),
        Some("123.45")
    );
    assert_eq!(provider_member_rows.lines().count(), 1);
    assert!(provider_member_rows.ends_with("\t1234567890\n"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn scanner_utility_boundaries_are_explicit() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let input_path = Path::new("/tmp/scanner-utility-boundaries.json");
    let _progress_env = [
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "1"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "1"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "invalid"),
    ];

    let integer_bytes = [0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0];
    let mut integer_cursor = Cursor::new(&integer_bytes);
    assert_eq!(read_i32_be(&mut integer_cursor).unwrap(), 7);
    assert_eq!(
        to_io_error(<[u8; 2]>::try_from(&[1u8][..]).unwrap_err()).kind(),
        io::ErrorKind::InvalidData,
    );
    assert_eq!(
        to_io_error("invalid".parse::<u64>().unwrap_err()).kind(),
        io::ErrorKind::InvalidData,
    );
    assert_eq!(
        to_io_error(u8::try_from(256u16).unwrap_err()).kind(),
        io::ErrorKind::InvalidData,
    );
    assert_eq!(
        to_io_error(io::Error::other("coverage")).kind(),
        io::ErrorKind::InvalidData,
    );
    assert_eq!(
        indexed_range_producers_requested().unwrap_err().kind(),
        io::ErrorKind::InvalidInput,
    );
    let mut array_reader = BufferedJsonByteReader::new(&b"]"[..]);
    assert!(!next_array_value(&mut array_reader, &mut true).unwrap());
    let mut wrapped_reader =
        WrappedIndexedRangeReader::new(Box::new(Cursor::new(b"[]".to_vec())), 2, b"", b"");
    drain_reader_to_eof(&mut wrapped_reader).unwrap();
    emit_plain_reorder_progress(input_path, 2, 1, 1, Instant::now(), false);
}
#[test]
fn reversed_top_level_order_quarantines_dangling_rate_end_to_end() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-reversed-top-level-order-dangling-{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&base);
    let input_path = base.join("input.json");
    let serving_path = base.join("serving.copy");
    let serving_run_directory = base.join("serving-runs");
    write_reversed_provider_reference_fixture(&input_path, &[7, 999]);
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
        TestEnvVar::set(
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
            serving_path.to_str().unwrap(),
        ),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    scan_compact_struson(&input_path).unwrap();

    let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum::<u64>();
    let witness_path = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("ptg2-v3-source-witness-"))
        })
        .unwrap();
    let witness_header = source_witness_header(&std::fs::read(witness_path).unwrap());

    assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
    assert_eq!(
        witness_header["rate_occurrence"]["emitted_rate_row_count"],
        2
    );
    assert_eq!(
        witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
        1,
    );
    assert!(!serving_path.exists());
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn late_procedure_fields_fail_before_partial_rates_can_publish() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base =
        std::env::temp_dir().join(format!("ptg2-late-procedure-field-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&base);
    let input_path = base.join("input.json");
    let serving_path = base.join("serving.copy");
    let serving_run_directory = base.join("serving-runs");
    std::fs::write(
        &input_path,
        r#"{
            "provider_references":[{
                "provider_group_id":7,
                "provider_groups":[{
                    "tin":{"type":"ein","value":"123456789"},
                    "npi":[1234567890]
                }]
            }],
            "in_network":[{
                "negotiated_rates":[{
                    "provider_references":[7],
                    "negotiated_prices":[{
                        "negotiated_type":"negotiated",
                        "negotiated_rate":123.45
                    }]
                }],
                "billing_code_type":"CPT",
                "billing_code":"99213",
                "negotiation_arrangement":"ffs"
            }]
        }"#,
    )
    .unwrap();
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1"),
        TestEnvVar::set(
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
            serving_path.to_str().unwrap(),
        ),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    let error = scan_compact_struson(&input_path).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error
        .to_string()
        .contains("before billing_code_type and billing_code"));
    assert!(!serving_path.exists());
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_price_set_atom_sink_emits_global_id_pairs() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-manifest-price-set-atom-{}.copy",
        std::process::id()
    ));
    let paths = CopyPathConfig {
        compact: None,
        manifest_serving: None,
        manifest_lean_serving: None,
        v3_serving_run_directory: None,
        v3_coverage_scope_id: None,
        manifest_provider_forward_sidecar: None,
        manifest_provider_inverted_sidecar: None,
        manifest_provider_set_component_sidecar: None,
        manifest_provider_component_group_sidecar: None,
        manifest_provider_group_tax_identity_sidecar: None,
        manifest_provider_group_tax_identity_v2_sidecar: None,
        manifest_provider_npi_sidecar: None,
        manifest_price_forward_sidecar: None,
        manifest_price_atom: None,
        manifest_price_set_atom: Some(base.to_string_lossy().to_string()),
        manifest_price_set_summary: None,
        manifest_provider_group_member: None,
        manifest_code_count: None,
        manifest_provider_set_dictionary: None,
        procedure: None,
        price_code_set: None,
        price_atom: None,
        price_set_entry: None,
        provider_set: None,
        provider_set_component: None,
        provider_set_entry: None,
        provider_entry_component: None,
        provider_group_member: None,
        manifest_only: true,
    };
    let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let price_set = price_lite_set(&[PriceLite {
        negotiated_type: Some("negotiated".to_string()),
        negotiated_rate: "123.45".to_string(),
        expiration_date: Some("2026-12-31".to_string()),
        service_code: vec!["11".to_string()],
        billing_class: Some("professional".to_string()),
        setting: None,
        billing_code_modifier: vec![],
        additional_information: None,
    }])
    .unwrap();

    sinks.write_manifest_price_set_atoms(&price_set).unwrap();
    sinks
        .write_price_atoms(&price_set.atoms, &mut HashSet::new(), &mut HashSet::new())
        .unwrap();
    sinks.write_price_code_set("unused", &[]).unwrap();
    sinks
        .write_price_set_entries(GlobalId128([0; GLOBAL_ID_BYTES]), &[], &mut HashSet::new())
        .unwrap();
    sinks
        .write_provider_set_entries("set", &[11], &mut HashSet::new())
        .unwrap();
    sinks
        .write_provider_set_components("set", &[12], &mut HashSet::new())
        .unwrap();
    sinks
        .write_provider_entry_components(11, &[12], &mut HashSet::new())
        .unwrap();
    sinks
        .write_provider_group_members(&Value::Null, &mut HashSet::new())
        .unwrap();
    let events = sinks.finish_silent().unwrap();
    let body = std::fs::read_to_string(&base).unwrap();
    let expected_price_set_id = price_set_global_id(&price_set).to_hex();
    let expected_atom_id = price_atom_global_id(&price_set.atoms[0]).to_hex();

    assert_eq!(events.len(), 1);
    assert_eq!(events[0].record_kind, "manifest_price_set_atom_copy_file");
    assert_eq!(events[0].row_count, 1);
    assert_eq!(
        body,
        format!("{expected_price_set_id}\t{expected_atom_id}\n")
    );
    let _ = std::fs::remove_file(base);
}
#[test]
fn manifest_price_set_summary_sink_emits_exact_minimum() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-manifest-price-set-summary-{}.copy",
        std::process::id()
    ));
    let paths = CopyPathConfig {
        manifest_price_set_summary: Some(base.to_string_lossy().to_string()),
        manifest_only: true,
        ..CopyPathConfig::default()
    };
    let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let prices = ["10", "2", "-2", "-10", "0.0000000000000000001"]
        .into_iter()
        .map(|negotiated_rate| PriceLite {
            negotiated_type: Some("negotiated".to_string()),
            negotiated_rate: negotiated_rate.to_string(),
            expiration_date: None,
            service_code: vec![],
            billing_class: None,
            setting: None,
            billing_code_modifier: vec![],
            additional_information: None,
        })
        .collect::<Vec<_>>();
    let price_set = price_lite_set(&prices).unwrap();

    assert_eq!(price_set.minimum_negotiated_rate(), "-10");
    sinks.write_manifest_price_set_summary(&price_set).unwrap();
    let events = sinks.finish_silent().unwrap();
    let expected_price_set_id = price_set_global_id(&price_set).to_hex();

    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].record_kind,
        "manifest_price_set_summary_copy_file"
    );
    assert_eq!(events[0].row_count, 1);
    assert_eq!(
        std::fs::read_to_string(&base).unwrap(),
        format!("{expected_price_set_id}\t-10\n")
    );
    let _ = std::fs::remove_file(base);
}
#[test]
fn raw_worker_parsers_reject_invalid_utf8_values() {
    let mut raw_rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100,"additional_information":"A"#.to_vec();
    raw_rate.push(0xff);
    raw_rate.extend_from_slice(br#"B"}]}"#);
    assert!(read_rate_lite_bytes(&raw_rate).is_err());

    let paths = CopyPathConfig {
        compact: None,
        manifest_serving: None,
        manifest_lean_serving: None,
        v3_serving_run_directory: None,
        v3_coverage_scope_id: None,
        manifest_provider_forward_sidecar: None,
        manifest_provider_inverted_sidecar: None,
        manifest_provider_set_component_sidecar: None,
        manifest_provider_component_group_sidecar: None,
        manifest_provider_group_tax_identity_sidecar: None,
        manifest_provider_group_tax_identity_v2_sidecar: None,
        manifest_provider_npi_sidecar: None,
        manifest_price_forward_sidecar: None,
        manifest_price_atom: None,
        manifest_price_set_atom: None,
        manifest_price_set_summary: None,
        manifest_provider_group_member: None,
        manifest_code_count: None,
        manifest_provider_set_dictionary: None,
        procedure: None,
        price_code_set: None,
        price_atom: None,
        price_set_entry: None,
        provider_set: None,
        provider_set_component: None,
        provider_set_entry: None,
        provider_entry_component: None,
        provider_group_member: None,
        manifest_only: true,
    };
    let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let dedupe = SharedDedupe::new(1);
    let mut provider_map = HashMap::new();
    let mut raw_ref = br#"{"provider_group_id":9,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890],"bad":"A"#.to_vec();
    raw_ref.push(0xff);
    raw_ref.extend_from_slice(br#"B"}]}"#);
    let mut raw_refs = RawRateChunk::with_capacity(1, raw_ref.len());
    let start = raw_refs.byte_len();
    raw_refs.bytes.extend_from_slice(&raw_ref);
    raw_refs.push_current_value_span(start);

    let error =
        process_provider_ref_raw_batch(&raw_refs, &mut provider_map, &mut sinks, &dedupe, false)
            .unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(provider_map.is_empty());
}
#[test]
fn normal_order_byte_scan_rejects_duplicate_rate_array_suffix() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    assert_normal_order_suffix_rejected(
        "duplicate-array",
        r#", "in_network":[]}"#,
        true,
        "duplicate PTG top-level array",
    );
}
#[test]
fn normal_order_byte_scan_rejects_malformed_json_suffix() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    assert_normal_order_suffix_rejected("malformed-json", r#", "invalid":not_json}"#, true, "JSON");
}
#[test]
fn normal_order_parallel_byte_scan_accepts_identical_provider_definitions() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-normal-identical-provider-definitions-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let input_path = base.join("input.json");
    let serving_path = base.join("serving.copy");
    let serving_run_directory = base.join("serving-runs");
    let provider_member_path = base.join("provider-group-member.copy");
    let provider_reference = valid_provider_reference();
    std::fs::write(
        &input_path,
        format!(
            r#"{{
                "provider_references":[{provider_reference},{provider_reference}],
                "in_network":[{{
                    "billing_code_type":"CPT",
                    "billing_code":"99213",
                    "negotiation_arrangement":"ffs",
                    "negotiated_rates":[{{
                        "provider_references":[7],
                        "negotiated_prices":[{{
                            "negotiated_type":"negotiated",
                            "negotiated_rate":123.45
                        }}]
                    }}]
                }}]
            }}"#
        ),
    )
    .unwrap();
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORK_QUEUE", "8"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS", "1"),
        TestEnvVar::set("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "2"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true"),
        TestEnvVar::set(
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
            serving_path.to_str().unwrap(),
        ),
        TestEnvVar::set(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
            provider_member_path.to_str().unwrap(),
        ),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    scan_compact_struson(&input_path).unwrap();

    let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum::<u64>();
    let provider_member_rows = read_worker_copy_text(&provider_member_path).unwrap();
    assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
    assert_eq!(provider_member_rows.lines().count(), 1);
    assert!(provider_member_rows.ends_with("\t1234567890\n"));
    assert!(!serving_path.exists());
    std::fs::remove_dir_all(base).unwrap();
}
#[test]
fn normal_order_parallel_byte_scan_quarantines_empty_code_rows() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base =
        std::env::temp_dir().join(format!("ptg2-normal-producer-error-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let input_path = base.join("input.json");
    let serving_path = base.join("serving.copy");
    let serving_run_directory = base.join("serving-runs");
    let provider_reference = valid_provider_reference();
    std::fs::write(
        &input_path,
        format!(
            r#"{{
                "provider_references":[{provider_reference}],
                "in_network":[
                    {{
                        "billing_code_type":"CPT",
                        "billing_code":"99213",
                        "negotiation_arrangement":"ffs",
                        "negotiated_rates":[{{
                            "provider_references":[7],
                            "negotiated_prices":[{{
                                "negotiated_type":"negotiated",
                                "negotiated_rate":123.45
                            }}]
                        }}]
                    }},
                    {{
                        "billing_code_type":"CPT",
                        "billing_code":"",
                        "negotiation_arrangement":"ffs",
                        "negotiated_rates":[{{
                            "provider_references":[7],
                            "negotiated_prices":[{{
                                "negotiated_type":"negotiated",
                                "negotiated_rate":999.99
                            }}]
                        }}]
                    }}
                ]
            }}"#
        ),
    )
    .unwrap();
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_WORK_QUEUE", "8"),
        TestEnvVar::set("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "2"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true"),
        TestEnvVar::set(
            "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
            serving_path.to_str().unwrap(),
        ),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    scan_compact_struson(&input_path).unwrap();

    let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
        .map(|entry| entry.metadata().unwrap().len())
        .sum::<u64>();
    let witness_path = std::fs::read_dir(&serving_run_directory)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("ptg2-v3-source-witness-"))
        })
        .unwrap();
    let witness_header = source_witness_header(&std::fs::read(witness_path).unwrap());

    assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
    assert_eq!(
        witness_header["rate_occurrence"]["emitted_rate_row_count"],
        2
    );
    assert_eq!(
        witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
        1,
    );
    assert!(!serving_path.exists());
    std::fs::remove_dir_all(base).unwrap();
}
#[test]
fn compact_byte_scan_rejects_invalid_utf8_outside_captured_values() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let base = std::env::temp_dir().join(format!(
        "ptg2-byte-scan-invalid-utf8-{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&base);
    let input_path = base.join("input.json");
    let serving_run_directory = base.join("serving-runs");
    let provider_ref = serde_json::to_vec(&valid_provider_reference()).unwrap();
    let mut payload = br#"{"provider_references":["#.to_vec();
    payload.extend_from_slice(&provider_ref);
    payload.extend_from_slice(br#"],"ignored":"A"#);
    payload.push(0xff);
    payload.extend_from_slice(br#"B","in_network":[]}"#);
    std::fs::write(&input_path, payload).unwrap();
    let _strict_env = strict_scan_env(&serving_run_directory);
    let _env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
        TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
    ];

    let error = scan_compact_struson(&input_path).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("invalid UTF-8"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn capture_value_bytes_into_reuses_scratch_buffer() {
    let input = br#" {"a":[1,{"b":"c"}]} "tail" "#;
    let mut reader = BufferedJsonByteReader::new(&input[..]);
    let mut scratch = Vec::with_capacity(64);

    reader.capture_value_bytes_into(&mut scratch).unwrap();
    let retained_capacity = scratch.capacity();
    assert_eq!(scratch, br#"{"a":[1,{"b":"c"}]}"#);

    reader.capture_value_bytes_into(&mut scratch).unwrap();

    assert_eq!(scratch, br#""tail""#);
    assert_eq!(scratch.capacity(), retained_capacity);

    let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
    let mut deep_reader = BufferedJsonByteReader::new(deep.as_bytes());
    deep_reader.capture_value_bytes_into(&mut scratch).unwrap();
    assert_eq!(scratch, deep.as_bytes());

    let mut mismatched_reader = BufferedJsonByteReader::new(&b"x"[..]);
    let mismatch = mismatched_reader.expect_byte(b'y').unwrap_err();
    assert_eq!(mismatch.kind(), io::ErrorKind::InvalidData);
    assert!(mismatch.to_string().contains("expected JSON byte"));

    let mut empty_reader = BufferedJsonByteReader::new(&b""[..]);
    let missing = empty_reader.expect_byte(b'y').unwrap_err();
    assert_eq!(missing.kind(), io::ErrorKind::UnexpectedEof);
}
#[test]
fn fused_array_object_capture_handles_compact_and_whitespace_json() {
    let compact = br#"[{"a":1},{"b":[2,{"c":3}]}]"#;
    let whitespace = br#" [
        { "a": 1 },
        { "b": [2, {"c": 3}] }
    ] "#;

    let compact_values = capture_array_objects(compact)
        .unwrap()
        .into_iter()
        .map(|raw| serde_json::from_slice::<Value>(&raw).unwrap())
        .collect::<Vec<_>>();
    let whitespace_values = capture_array_objects(whitespace)
        .unwrap()
        .into_iter()
        .map(|raw| serde_json::from_slice::<Value>(&raw).unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        compact_values,
        vec![json!({"a": 1}), json!({"b": [2, {"c": 3}]})]
    );
    assert_eq!(whitespace_values, compact_values);
    assert!(capture_array_objects(b"[]").unwrap().is_empty());
}
#[test]
fn fused_array_object_capture_preserves_escaped_quotes_and_delimiters() {
    let input =
        br#"[{"message":"escaped quote: \" and slash: \\ and delimiters: } ] {"},{"tail":2}]"#;

    let objects = capture_array_objects(input).unwrap();

    assert_eq!(objects.len(), 2);
    assert_eq!(
        serde_json::from_slice::<Value>(&objects[0]).unwrap(),
        json!({"message": "escaped quote: \" and slash: \\ and delimiters: } ] {"})
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&objects[1]).unwrap(),
        json!({"tail": 2})
    );
}
#[test]
fn fused_array_object_capture_rejects_malformed_delimiters_and_eof() {
    fn capture_error(input: &[u8]) -> io::Error {
        let mut reader = BufferedJsonByteReader::new(input);
        let mut captured = Vec::new();
        let mut first = true;
        reader.expect_byte(b'[').unwrap();
        loop {
            match reader.capture_next_array_object_bytes_append(&mut captured, &mut first) {
                Ok(true) => {}
                Ok(false) => panic!("malformed input unexpectedly completed"),
                Err(error) => return error,
            }
        }
    }

    for input in [
        br#"[{"a":1} {"b":2}]"#.as_slice(),
        br#"[{"a":1},,{"b":2}]"#.as_slice(),
        br#"[{"a":1},]"#.as_slice(),
        br#"[null]"#.as_slice(),
    ] {
        let error = capture_error(input);
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{input:?}");
    }

    let missing_array_end = capture_error(br#"[{"a":1}"#);
    assert_eq!(missing_array_end.kind(), io::ErrorKind::UnexpectedEof);
    let unterminated_object = capture_error(br#"[{"message":"unterminated}"#);
    assert_eq!(unterminated_object.kind(), io::ErrorKind::UnexpectedEof);
}
#[test]
fn capture_nested_values_handles_fragmented_escapes_and_delimiters() {
    struct FragmentedReader<'a> {
        bytes: &'a [u8],
        offset: usize,
    }

    impl Read for FragmentedReader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let Some(byte) = self.bytes.get(self.offset).copied() else {
                return Ok(0);
            };
            buffer[0] = byte;
            self.offset += 1;
            Ok(1)
        }
    }

    let first = br#"{"message":"escaped quote: \" and slash: \\ and delimiters: } ]","nested":[{"value":"x"}]}"#;
    let second = br#"[1,{"value":"tail"}]"#;
    let mut input = Vec::new();
    input.extend_from_slice(first);
    input.push(b' ');
    input.extend_from_slice(second);
    let fragmented = FragmentedReader {
        bytes: &input,
        offset: 0,
    };
    let mut reader = BufferedJsonByteReader::new(fragmented);
    let mut captured = Vec::new();

    reader.capture_value_bytes_into(&mut captured).unwrap();
    assert_eq!(captured, first);
    reader.capture_value_bytes_into(&mut captured).unwrap();
    assert_eq!(captured, second);

    let fragmented = FragmentedReader {
        bytes: &input,
        offset: 0,
    };
    let mut reader = BufferedJsonByteReader::new(fragmented);
    captured.clear();
    reader.capture_object_bytes_append(&mut captured).unwrap();
    assert_eq!(captured, first);

    let direct: &[u8] = first;
    let mut direct_reader = BufferedJsonByteReader::new(direct);
    captured.clear();
    direct_reader
        .capture_object_bytes_append(&mut captured)
        .unwrap();
    assert_eq!(captured, first);
}
#[test]
fn provider_source_capture_rejects_the_byte_past_the_record_limit() {
    validate_provider_source_capture_append(PROVIDER_SOURCE_RECORD_MAX_BYTES - 1, 0).unwrap();
    let error =
        validate_provider_source_capture_append(PROVIDER_SOURCE_RECORD_MAX_BYTES, 0).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error
        .to_string()
        .contains("provider source record is 67108865 bytes"));
    assert!(validate_provider_source_capture_append(0, 1).is_err());
}
#[test]
fn raw_byte_scan_reuses_final_chunk_at_next_object_boundary() {
    let rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":100}]}"#;
    let object = format!(
        r#"{{"billing_code_type":"CPT","billing_code":"99213","negotiated_rates":[{}]}}"#,
        std::str::from_utf8(rate).unwrap()
    );
    let payload = format!("{object}\n{object}");
    let mut reader = BufferedJsonByteReader::new(payload.as_bytes());
    let (tx, rx) = bounded::<WorkerJob>(4);
    let (_event_tx, event_rx) = bounded::<CopyFileEvent>(4);
    let mut writer = Vec::new();
    let mut producer_blocked_micros = 0u128;
    let mut stats = RawChunkStats::default();
    let mut copy_file_event_gate = CopyFileEventGate::passthrough();
    let recycle_tx = stats.enable_recycling(2);

    for object_ordinal in 0..2 {
        let mut enqueue_io = InNetworkEnqueueIo {
            tx: &tx,
            event_rx: &event_rx,
            writer: &mut writer,
            copy_file_event_gate: &mut copy_file_event_gate,
            cancelled: None,
            producer_blocked_micros: &mut producer_blocked_micros,
            raw_chunk_stats: &mut stats,
        };
        let rate_count = enqueue_in_network_raw_byte_scan(
            &mut reader,
            &mut enqueue_io,
            InNetworkEnqueueOptions {
                chunk_size: 8,
                raw_chunk_byte_limit: 4 * 1024,
                parse_in_workers: true,
                object_ordinal,
            },
        )
        .unwrap();
        assert_eq!(rate_count, 1);

        let mut raw_rates = match rx.recv().unwrap() {
            WorkerJob::RawRates { raw_rates, .. } => raw_rates,
            WorkerJob::Rates { .. } => panic!("raw byte scan emitted parsed rates"),
        };
        let captured = raw_rates
            .iter_with_coordinates()
            .map(|(coordinate, raw)| (coordinate, raw.to_vec()))
            .collect::<Vec<_>>();
        assert_eq!(
            captured,
            vec![(
                SourceWitnessCoordinate::new(object_ordinal, 0),
                rate.to_vec()
            )]
        );
        stats.queue_bytes.finish_receive(raw_rates.byte_len());
        if object_ordinal == 0 {
            raw_rates.clear_for_recycle();
            assert!(recycle_tx.try_send(raw_rates).is_ok());
        }
    }

    assert_eq!(stats.chunk_count, 2);
    assert_eq!(stats.buffer_allocations, 1);
    assert_eq!(stats.buffer_reuses, 1);
}
#[test]
fn capture_nested_values_rejects_mismatched_delimiters() {
    let mut reader = BufferedJsonByteReader::new(br#"{"items":[1,2}}"#.as_slice());
    let mut captured = Vec::new();

    let error = reader.capture_value_bytes_into(&mut captured).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("mismatched JSON delimiter"));
}
#[test]
#[ignore = "manual byte-framing throughput probe"]
fn benchmark_nested_value_capture_throughput() {
    fn capture(input: &[u8], mode: &str) -> (Duration, usize, usize) {
        let mut reader = BufferedJsonByteReader::new(input);
        let mut captured = Vec::with_capacity(input.len());
        let mut first = true;
        let mut captured_values = 0usize;
        reader.expect_byte(b'[').unwrap();
        let started_at = Instant::now();
        loop {
            if mode == "fused" {
                if !reader
                    .capture_next_array_object_bytes_append(&mut captured, &mut first)
                    .unwrap()
                {
                    break;
                }
            } else {
                if !next_array_value(&mut reader, &mut first).unwrap() {
                    break;
                }
                if mode == "object" {
                    reader.capture_object_bytes_append(&mut captured).unwrap();
                } else {
                    reader.capture_value_bytes_append(&mut captured).unwrap();
                }
            }
            captured_values += 1;
        }
        (started_at.elapsed(), captured_values, captured.len())
    }

    const TARGET_BYTES: usize = 128 * 1024 * 1024;
    const RATE: &[u8] = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":123.45,"service_code":["11"],"billing_class":"professional","additional_information":"quoted \"text\" and delimiters } ]"}]}"#;
    serde_json::from_slice::<Value>(RATE).unwrap();
    let rate_count = TARGET_BYTES / (RATE.len() + 1);
    let mut input = Vec::with_capacity(rate_count * (RATE.len() + 1) + 2);
    input.push(b'[');
    for index in 0..rate_count {
        if index > 0 {
            input.push(b',');
        }
        input.extend_from_slice(RATE);
    }
    input.push(b']');

    let (baseline_elapsed, baseline_values, baseline_bytes) = capture(&input, "generic");
    let (fast_elapsed, fast_values, fast_bytes) = capture(&input, "object");
    let (fused_elapsed, fused_values, fused_bytes) = capture(&input, "fused");
    let baseline_mib_per_second =
        input.len() as f64 / (1024.0 * 1024.0) / baseline_elapsed.as_secs_f64();
    let fast_mib_per_second = input.len() as f64 / (1024.0 * 1024.0) / fast_elapsed.as_secs_f64();
    let fused_mib_per_second = input.len() as f64 / (1024.0 * 1024.0) / fused_elapsed.as_secs_f64();

    eprintln!(
        "captured {} values / {:.1} MiB: baseline {:.3}s ({:.1} MiB/s), object fast path {:.3}s ({:.1} MiB/s), fused object path {:.3}s ({:.1} MiB/s), fused speedup {:.3}x",
        fused_values,
        input.len() as f64 / (1024.0 * 1024.0),
        baseline_elapsed.as_secs_f64(),
        baseline_mib_per_second,
        fast_elapsed.as_secs_f64(),
        fast_mib_per_second,
        fused_elapsed.as_secs_f64(),
        fused_mib_per_second,
        fast_elapsed.as_secs_f64() / fused_elapsed.as_secs_f64(),
    );
    assert_eq!(baseline_values, rate_count);
    assert_eq!(fast_values, rate_count);
    assert_eq!(fused_values, rate_count);
    assert_eq!(baseline_bytes, rate_count * RATE.len());
    assert_eq!(fast_bytes, baseline_bytes);
    assert_eq!(fused_bytes, baseline_bytes);
}
#[test]
fn raw_rate_chunk_iter_returns_contiguous_spans() {
    let mut chunk = RawRateChunk::with_capacity(2, 64);
    let first = chunk.byte_len();
    chunk.bytes.extend_from_slice(br#"{"a":1}"#);
    chunk.push_current_value_span(first);
    let second = chunk.byte_len();
    chunk.bytes.extend_from_slice(br#"{"b":[2,3]}"#);
    chunk.push_current_value_span(second);

    let raw_values: Vec<&[u8]> = chunk.iter().collect();

    assert_eq!(chunk.len(), 2);
    assert_eq!(chunk.byte_len(), br#"{"a":1}{"b":[2,3]}"#.len());
    assert_eq!(
        raw_values,
        vec![br#"{"a":1}"#.as_slice(), br#"{"b":[2,3]}"#.as_slice()]
    );
}
#[test]
fn provider_entry_compact_provider_set_hash_matches_json_payload_hash() {
    let provider_ref = json!({
        "provider_groups": [
            {
                "tin": {"type": "ein", "value": "12-3456789"},
                "npi": [1234567891, 1234567890, 1234567890]
            },
            {
                "tin": {"type": "npi", "value": " 9876543210 "},
                "npi": [2222222222_i64, 1111111111, 1234567891]
            }
        ]
    });

    let entry = build_provider_entry(&provider_ref).unwrap();
    let mut group_payloads: Vec<Value> = Vec::new();
    for group in provider_ref
        .get("provider_groups")
        .and_then(Value::as_array)
        .unwrap()
    {
        let tin = group.get("tin").unwrap_or(&Value::Null);
        let npi = strict_npi_list(group.get("npi")).unwrap();
        let group_hash = provider_group_hash(tin, &npi, &[], &[]);
        group_payloads.push(json!({
            "provider_group_hash": group_hash,
            "tin_type": normalize_tin_type(tin.get("type")),
            "tin_value": normalize_tin_value(tin.get("value")),
            "npi": npi,
        }));
    }
    group_payloads.sort_by_cached_key(ptg2_scanner::hashing::canonical_json);
    let expected = make_checksum(vec![json!("provider_set"), Value::Array(group_payloads)]);

    assert_eq!(entry.entry_hash, expected);
    assert_eq!(entry.provider_count, 4);
    assert_eq!(
        entry.npi,
        vec![1111111111, 1234567890, 1234567891, 2222222222]
    );
}
#[test]
fn provider_entry_view_borrows_single_refs_and_owns_combined_refs() {
    let provider_ref_a = json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890]
        }]
    });
    let provider_ref_b = json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "987654321"},
            "npi": [1234567891]
        }]
    });
    let mut provider_map = HashMap::new();
    provider_map.insert(
        ProviderRefKey::from("1"),
        build_provider_entry(&provider_ref_a).unwrap(),
    );
    provider_map.insert(
        ProviderRefKey::from("2"),
        build_provider_entry(&provider_ref_b).unwrap(),
    );

    let single = provider_entry_view_from_ref_keys(&provider_map, &[ProviderRefKey::from("1")])
        .unwrap()
        .expect("single ref should resolve");
    assert!(matches!(single, ProviderEntryView::Borrowed(_)));

    let combined = provider_entry_view_from_ref_keys(
        &provider_map,
        &[ProviderRefKey::from("1"), ProviderRefKey::from("2")],
    )
    .unwrap()
    .expect("combined refs should resolve");
    assert!(matches!(combined, ProviderEntryView::Owned(_)));
    assert_eq!(combined.provider_count(), 2);
}
#[test]
fn shared_block_hash_matches_fixed_python_vector() {
    let hash = shared_v3_block_hash(1, "by_code_grouped", "none", &[1, 2, 3]).unwrap();
    assert_eq!(
        sha256_hex(&hash),
        "4ce3f60a45772e30f3055b5f385024010c45863a0e9091c9c55aabc9482f603e"
    );
}
#[test]
fn shared_block_parallel_preparation_is_byte_identical_for_mixed_codecs_and_workers() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let _compression_level =
        TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV, "6");
    let _minimum_bytes =
        TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV, "128");
    let _minimum_savings = TestEnvVar::set(
        PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV,
        "2",
    );
    let records = vec![
        SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            block_key: 1,
            fragment_no: 0,
            entry_count: 3,
            payload: vec![7; 8192],
        },
        SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
            block_key: 2,
            fragment_no: 0,
            entry_count: 1,
            payload: deterministic_incompressible_payload(8192, 0x1234_5678_9abc_def0),
        },
        SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
            block_key: 3,
            fragment_no: 0,
            entry_count: 1,
            payload: vec![42; 64],
        },
        SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
            block_key: 4,
            fragment_no: 0,
            entry_count: 0,
            payload: Vec::new(),
        },
        SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND,
            block_key: 5,
            fragment_no: 1,
            entry_count: 5,
            payload: vec![9; 32 * 1024],
        },
    ];
    let limits = SharedBlockPreparationBatchLimits {
        maximum_raw_bytes: 12 * 1024,
        maximum_records: 3,
    };
    let serial = render_shared_block_preparation_test_copy(&records, None, limits);
    let serial_summary = serial.shared_block_summary(Path::new("blocks.copy"));
    let serial_records = read_test_shared_binary_records(serial.inner.clone());
    assert!(serial_records
        .iter()
        .any(|record| record.compression == "zlib"));
    assert!(serial_records
        .iter()
        .any(|record| record.compression == "none"));

    for workers in [1, 4, 8] {
        let parallel = render_shared_block_preparation_test_copy(&records, Some(workers), limits);
        assert_eq!(parallel.inner, serial.inner, "worker count {workers}");
        assert_eq!(
            parallel.shared_block_summary(Path::new("blocks.copy")),
            serial_summary,
            "worker count {workers}",
        );
        if workers == 1 {
            assert_eq!(
                parallel
                    .shared_block_preparation_metrics
                    .parallel_batch_count,
                0,
            );
        } else {
            assert!(
                parallel
                    .shared_block_preparation_metrics
                    .parallel_batch_count
                    > 0,
            );
        }
    }
}
#[test]
fn shared_block_parallel_preparation_is_byte_identical_with_compression_disabled() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let records = (0..17)
        .map(|index| SharedBlockPreparationTestRecord {
            kind: if index % 2 == 0 {
                PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND
            } else {
                PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
            },
            block_key: index,
            fragment_no: index as usize % 3,
            entry_count: index as usize + 1,
            payload: if index % 2 == 0 {
                vec![index as u8; 4096]
            } else {
                deterministic_incompressible_payload(3072, index as u64 + 1)
            },
        })
        .collect::<Vec<_>>();
    let limits = SharedBlockPreparationBatchLimits {
        maximum_raw_bytes: 16 * 1024,
        maximum_records: 4,
    };
    let serial = render_shared_block_preparation_test_copy(&records, None, limits);
    let serial_summary = serial.shared_block_summary(Path::new("blocks.copy"));
    assert!(read_test_shared_binary_records(serial.inner.clone())
        .iter()
        .all(|record| record.compression == "none"));
    for workers in [1, 4, 8] {
        let parallel = render_shared_block_preparation_test_copy(&records, Some(workers), limits);
        assert_eq!(parallel.inner, serial.inner, "worker count {workers}");
        assert_eq!(
            parallel.shared_block_summary(Path::new("blocks.copy")),
            serial_summary,
            "worker count {workers}",
        );
    }
}
#[test]
fn shared_block_preparation_batch_respects_record_and_raw_byte_bounds() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let limits = SharedBlockPreparationBatchLimits {
        maximum_raw_bytes: 64 * 1024,
        maximum_records: 5,
    };
    let mut records = (0..37)
        .map(|index| SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            block_key: index,
            fragment_no: 0,
            entry_count: 1,
            payload: vec![index as u8; 4096 + (index as usize % 3) * 1024],
        })
        .collect::<Vec<_>>();
    records.push(SharedBlockPreparationTestRecord {
        kind: PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
        block_key: 100,
        fragment_no: 0,
        entry_count: 1,
        payload: vec![1; 128 * 1024],
    });
    let writer = render_shared_block_preparation_test_copy(&records, Some(8), limits);
    let metrics = &writer.shared_block_preparation_metrics;
    assert!(metrics.peak_batch_records <= limits.maximum_records);
    assert!(
        metrics.peak_batch_raw_bytes
            <= limits
                .maximum_raw_bytes
                .max(metrics.maximum_record_raw_bytes)
    );
    assert_eq!(metrics.record_count, records.len() as u64);
    assert!(writer.pending_shared_blocks.is_empty());
    assert!(writer.recycled_shared_block_capacity_bytes <= limits.maximum_raw_bytes);
}
#[test]
fn shared_block_parallel_preparation_propagates_validation_and_writer_cancellation() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let limits = SharedBlockPreparationBatchLimits {
        maximum_raw_bytes: 1024 * 1024,
        maximum_records: 16,
    };
    let mut writer = CountingWriter::with_shared_block_preparation_batch(
        FailAfterWriter {
            remaining_bytes: 48,
            bytes: Vec::new(),
        },
        limits,
    )
    .unwrap();
    write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
        .unwrap();
    write_serving_binary_copy_record_with_i64_key_and_stats(
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
        1,
        0,
        1,
        &vec![7; 8192],
    )
    .unwrap();
    let pending_before_validation_error = writer.pending_shared_blocks.len();
    let validation_error = write_serving_binary_copy_record_with_i64_key_and_stats(
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
        -1,
        0,
        1,
        &[1, 2, 3],
    )
    .unwrap_err();
    assert_eq!(validation_error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        writer.pending_shared_blocks.len(),
        pending_before_validation_error
    );

    let pool = rayon::ThreadPoolBuilder::new()
        .num_threads(4)
        .build()
        .unwrap();
    let cancellation_error = pool
        .install(|| {
            write_serving_binary_copy_trailer(
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
            )
        })
        .unwrap_err();
    assert_eq!(cancellation_error.kind(), io::ErrorKind::BrokenPipe);
    assert!(writer.pending_shared_blocks.is_empty());
    assert_eq!(writer.shared_blocks.row_count, 0);
}
#[test]
fn shared_block_fail_after_writer_preserves_complete_copy_on_sufficient_capacity() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let mut writer = CountingWriter::with_shared_block_preparation_batch(
        FailAfterWriter {
            remaining_bytes: 4096,
            bytes: Vec::new(),
        },
        SharedBlockPreparationBatchLimits {
            maximum_raw_bytes: 4096,
            maximum_records: 2,
        },
    )
    .unwrap();

    write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
        .unwrap();
    write_serving_binary_copy_record_with_i64_key_and_stats(
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
        7,
        0,
        2,
        &[3, 1, 4, 1, 5, 9],
    )
    .unwrap();
    write_serving_binary_copy_trailer(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
        .unwrap();
    writer.flush().unwrap();

    assert_eq!(&writer.inner.bytes[..11], b"PGCOPY\n\xff\r\n\0");
    assert_eq!(
        &writer.inner.bytes[writer.inner.bytes.len() - 2..],
        &[0xff, 0xff]
    );
    assert_eq!(writer.shared_blocks.row_count, 1);
    assert!(writer.pending_shared_blocks.is_empty());
}
#[test]
fn shared_block_writer_summary_matches_reread_oracle() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("shared-blocks.copy");
    let file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&path)
        .unwrap();
    let mut writer = CountingWriter::new(BufWriter::new(file));
    write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
        .unwrap();
    let mut stats = ServingBinaryV3BlockStats::default();
    write_serving_binary_v3_logical_block(
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        32,
        ServingBinaryV3LogicalBlock {
            artifact_kind: PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
            block_key: 7,
            entry_count: 3,
            payload: &[42; 96],
        },
        &mut stats,
    )
    .unwrap();
    let compressible_payload = vec![7u8; 8192];
    write_serving_binary_v3_logical_block(
        &mut writer,
        ServingBinaryTargetCopyFormat::SharedBinary,
        64 * 1024,
        ServingBinaryV3LogicalBlock {
            artifact_kind: PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
            block_key: 8,
            entry_count: 1,
            payload: &compressible_payload,
        },
        &mut stats,
    )
    .unwrap();
    write_serving_binary_copy_trailer(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
        .unwrap();
    sync_counting_writer(&mut writer, ScratchDurability::Durable).unwrap();

    assert_eq!(
        writer.shared_block_summary(&path),
        summarize_shared_block_copy(&path).unwrap(),
    );
}
#[test]
fn code_dictionary_support_digest_covers_all_ten_fields() {
    let support_digest = |code_key: i32, code: NaturalLeanCode, rate_count: u64| {
        let mut copy_row = Vec::new();
        let mut row_digest = Sha256::new();
        row_digest.update(V3_FINALIZER_CODE_ROW_HASH_DOMAIN);
        write_v3_code_dictionary_copy_row(
            &mut copy_row,
            code_key,
            &code,
            rate_count,
            &mut row_digest,
        )
        .unwrap();
        assert_eq!(&copy_row[..2], &10i16.to_be_bytes());
        v3_support_digest(&row_digest.finalize().into(), 1, &[0u8; 32], 0).unwrap()
    };
    let base = NaturalLeanCode {
        code_id: [1u8; 16],
        coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
        reported_code_system: None,
        reported_code: Some("99213".to_owned()),
        negotiation_arrangement: Some("FFS".to_owned()),
        billing_code_type_version: Some("2026".to_owned()),
        name: Some("Source name".to_owned()),
        description: Some("Source description".to_owned()),
    };
    let digests = BTreeSet::from([
        support_digest(0, base.clone(), 1),
        support_digest(1, base.clone(), 1),
        support_digest(
            0,
            NaturalLeanCode {
                code_id: [2u8; 16],
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                coverage_scope_id: [0x22; COVERAGE_SCOPE_ID_BYTES],
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                reported_code_system: Some(String::new()),
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                reported_code: None,
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                negotiation_arrangement: Some("BUNDLE".to_owned()),
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                billing_code_type_version: Some("2025".to_owned()),
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                name: Some("Different name".to_owned()),
                ..base.clone()
            },
            1,
        ),
        support_digest(
            0,
            NaturalLeanCode {
                description: Some("Different description".to_owned()),
                ..base.clone()
            },
            1,
        ),
        support_digest(0, base, 2),
    ]);
    assert_eq!(digests.len(), 11);
}
#[test]
fn direct_v3_finalizer_is_deterministic_across_scratch_durability() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
    let _provider_sort = TestEnvVar::set(
        PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
        "8",
    );
    let base =
        std::env::temp_dir().join(format!("ptg2-direct-v3-finalizer-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let provider_a = prefixed_test_id(1, 1);
    let provider_b = prefixed_test_id(1, 2);
    let price_a = prefixed_test_id(2, 1);
    let price_b = prefixed_test_id(2, 2);
    let scope_a = [0x11; COVERAGE_SCOPE_ID_BYTES];
    let scope_b = [0x22; COVERAGE_SCOPE_ID_BYTES];
    let row_a = V3FinalizerTestRow {
        coverage_scope_id: scope_a,
        code_system: Some("CPT"),
        code: Some("99213"),
        negotiation_arrangement: Some("FFS"),
        provider_id: provider_a,
        price_id: price_a,
        provider_count: 2,
    };
    let row_b = V3FinalizerTestRow {
        coverage_scope_id: scope_a,
        code_system: Some("CPT"),
        code: Some("99213"),
        negotiation_arrangement: Some("FFS"),
        provider_id: provider_b,
        price_id: price_b,
        provider_count: 3,
    };
    let row_c = V3FinalizerTestRow {
        coverage_scope_id: scope_b,
        code_system: None,
        code: Some("A100"),
        negotiation_arrangement: None,
        provider_id: provider_a,
        price_id: price_b,
        provider_count: 2,
    };
    let row_d = V3FinalizerTestRow {
        coverage_scope_id: scope_a,
        code_system: Some("CPT"),
        code: Some("99213"),
        negotiation_arrangement: Some("BUNDLE"),
        provider_id: provider_a,
        price_id: price_b,
        provider_count: 2,
    };
    let manifest_a = write_v3_finalizer_test_manifest_with_source(
        &base,
        "input-a",
        &[row_b.clone(), row_a.clone()],
        0,
        2,
    );
    let manifest_b = write_v3_finalizer_test_manifest_with_source(
        &base,
        "input-b",
        &[row_c.clone(), row_d.clone(), row_a.clone()],
        1,
        2,
    );
    let price_ids_in_key_order = [price_b, price_a];
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "authoritative", &price_ids_in_key_order);
    let source_rows = vec![row_a.clone(), row_b, row_c, row_d, row_a.clone()];
    let output_a = base.join("output-a");
    let summary_a = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output_a.clone(),
        manifest_paths: vec![manifest_a.clone(), manifest_b.clone()],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
        workers: 2,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input: price_key_map_input.clone(),
        price_key_map_row_count: price_ids_in_key_order.len() as u64,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();
    let output_b = base.join("output-b");
    let summary_b = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output_b.clone(),
        manifest_paths: vec![manifest_b, manifest_a],
        scratch_durability: ScratchDurability::Ephemeral,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
        workers: 2,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: price_ids_in_key_order.len() as u64,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();

    for file_name in [
        "audit_candidates.bin",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
    ] {
        assert_eq!(
            std::fs::read(output_a.join(file_name)).unwrap(),
            std::fs::read(output_b.join(file_name)).unwrap(),
            "{file_name} changed with input order or scratch durability"
        );
    }
    let durable_sync = &summary_a["scratch_durability"];
    let ephemeral_sync = &summary_b["scratch_durability"];
    assert_eq!(durable_sync["policy"], "durable");
    assert_eq!(
        durable_sync["crash_recovery"],
        "synced_files_before_atomic_directory_publish_v1"
    );
    assert_eq!(ephemeral_sync["policy"], "ephemeral");
    assert_eq!(
        ephemeral_sync["crash_recovery"],
        "caller_discards_and_rebuilds_uncommitted_attempt_v1"
    );
    for category in [
        "assigned_final_runs",
        "price_copy_output",
        "serving_copy_output",
    ] {
        let durable = &durable_sync["categories"][category];
        let ephemeral = &ephemeral_sync["categories"][category];
        assert!(durable["sync_calls"].as_u64().unwrap() > 0);
        assert!(durable["sync_bytes"].as_u64().unwrap() > 0);
        assert_eq!(durable["skipped_sync_calls"], 0);
        assert_eq!(durable["skipped_sync_bytes"], 0);
        assert_eq!(ephemeral["sync_calls"], 0);
        assert_eq!(ephemeral["sync_bytes"], 0);
        assert_eq!(ephemeral["sync_seconds"], 0.0);
        assert_eq!(ephemeral["sync_max_seconds"], 0.0);
        assert!(ephemeral["skipped_sync_calls"].as_u64().unwrap() > 0);
        assert_eq!(
            durable["sync_bytes"], ephemeral["skipped_sync_bytes"],
            "{category} measured different logical bytes by policy"
        );
    }
    assert_eq!(
        summary_a["dictionaries"]["support_digest"],
        summary_b["dictionaries"]["support_digest"]
    );
    assert_eq!(summary_a["audit_candidates"], summary_b["audit_candidates"]);
    assert_eq!(
        summary_a["audit_candidates"]["record_format"],
        "ptg2_v3_audit_candidates_v2"
    );
    assert_eq!(summary_a["audit_candidates"]["record_bytes"], 20);
    assert_eq!(summary_a["audit_candidates"]["row_count"], 5);
    assert_eq!(summary_a["audit_candidates"]["maximum_rows"], 4096);
    assert_eq!(summary_a["audit_candidates"]["source_row_count"], 5);
    assert_eq!(summary_a["source"]["record_count"], 5);
    assert_eq!(summary_a["source_key_bytes"], 1);
    assert_eq!(summary_a["tagged_record_bytes"], 53);
    assert_eq!(summary_a["source"]["source_key_bytes"], 1);
    assert_eq!(summary_a["source"]["tagged_record_bytes"], 53);
    assert_eq!(summary_a["source_identity_scan"]["bytes_read"], 0);
    assert_eq!(summary_a["source_identity_scan"]["passes"], 0);
    assert_eq!(
        summary_a["source_identity_scan"]["strategy"],
        "assignment_integrated_exact_coverage_v1"
    );
    assert_eq!(
        summary_a["source_identity_scan"]["tagged_partition_sort"],
        false
    );
    assert_eq!(
        summary_a["price_identity_assignment"]["source_identity_sort"],
        false
    );
    assert_eq!(summary_a["preservation"]["sorted_records"], 5);
    assert_eq!(summary_a["preservation"]["staged_records"], 5);
    assert_eq!(summary_a["preservation"]["assigned_records"], 5);
    assert_eq!(summary_a["preservation"]["encoded_records"], 5);
    assert_eq!(summary_a["preservation"]["distinct_serving_records"], 5);
    assert_eq!(summary_a["preservation"]["duplicate_serving_records"], 0);
    assert!(
        summary_a["preservation"]["all_source_occurrences_preserved"]
            .as_bool()
            .unwrap()
    );
    assert_eq!(summary_a["dense_keys"]["code"]["count"], 3);
    assert_eq!(summary_a["dense_keys"]["provider_set"]["count"], 2);
    assert_eq!(summary_a["dense_keys"]["price"]["count"], 2);
    assert_eq!(
        summary_a["dense_keys"]["price"]["ordering"],
        "minimum_negotiated_rate_then_global_id_128_v1"
    );
    assert_eq!(
        summary_a["price_key_map"]["dense_price_ordering"],
        "minimum_negotiated_rate_then_global_id_128_v1"
    );
    assert_eq!(summary_a["price_key_map"]["source_ids_exact_match"], true);
    assert_eq!(
        summary_a["price_key_map"]["dictionary_external_sort"],
        false
    );
    assert_eq!(
        summary_a["price_key_map"]["dictionary_strategy"],
        "single_pass_map_and_shared_blocks_v1"
    );
    assert_eq!(
        summary_a["price_key_map"]["input_ordering"],
        "price_key_dense_ascending_v1"
    );
    assert_eq!(summary_a["price_key_map"]["input_passes"], 1);
    assert_eq!(summary_a["price_key_map"]["fixed_map_scratch_files"], 0);
    assert_eq!(summary_a["dictionaries"]["code"]["field_count"], 10);
    assert_eq!(
        summary_a["dictionaries"]["code"]["fields"],
        json!([
            "code_key",
            "code_global_id_128",
            "coverage_scope_id",
            "reported_code_system",
            "reported_code",
            "negotiation_arrangement",
            "billing_code_type_version",
            "source_name",
            "source_description",
            "rate_count"
        ])
    );
    assert_eq!(summary_a["dictionaries"]["code"]["rate_count_total"], 5);
    let code_rows = read_v3_code_dictionary_rows(&output_a.join("code_dictionary.copy"));
    let code_a = natural_lean_code_identity(
        &scope_a,
        Some("CPT"),
        Some("99213"),
        Some("FFS"),
        None,
        None,
        None,
    );
    let code_a_bundle = natural_lean_code_identity(
        &scope_a,
        Some("CPT"),
        Some("99213"),
        Some("BUNDLE"),
        None,
        None,
        None,
    );
    let code_b = natural_lean_code_identity(&scope_b, None, Some("A100"), None, None, None, None);
    assert_ne!(code_a, code_a_bundle);
    assert_eq!(code_rows.len(), 3);
    assert_eq!(code_rows[&code_a].rate_count, 3);
    assert_eq!(code_rows[&code_a].coverage_scope_id, scope_a);
    assert_eq!(
        code_rows[&code_a].reported_code_system.as_deref(),
        Some("CPT")
    );
    assert_eq!(code_rows[&code_a].reported_code.as_deref(), Some("99213"));
    assert_eq!(
        code_rows[&code_a].negotiation_arrangement.as_deref(),
        Some("FFS")
    );
    assert_eq!(code_rows[&code_a_bundle].rate_count, 1);
    assert_eq!(
        code_rows[&code_a_bundle].negotiation_arrangement.as_deref(),
        Some("BUNDLE")
    );
    assert_eq!(code_rows[&code_b].coverage_scope_id, scope_b);
    assert_eq!(code_rows[&code_b].negotiation_arrangement, None);
    assert_eq!(code_rows[&code_b].rate_count, 1);
    assert_eq!(
        code_rows.values().map(|row| row.rate_count).sum::<u64>(),
        summary_a["preservation"]["source_records"]
            .as_u64()
            .unwrap()
    );
    assert!(summary_a["preservation"]["assigned_equals_encoded"]
        .as_bool()
        .unwrap());
    assert!(
        summary_a["partition_assignment_sorts"]["aggregate"]["chunk_count"]
            .as_u64()
            .unwrap()
            > 1
    );

    let (reference_serving, reference_price) = reference_v3_assigned_records(
        &source_rows,
        &[0, 0, 1, 1, 1],
        &output_a,
        &price_ids_in_key_order,
    );
    let shared_serving = read_test_shared_binary_records(
        std::fs::read(output_a.join("shared_serving_blocks.copy")).unwrap(),
    );
    let shared_price = read_test_shared_binary_records(
        std::fs::read(output_a.join("shared_price_dictionary_blocks.copy")).unwrap(),
    );
    assert_eq!(shared_serving, reference_serving);
    assert_eq!(shared_price, reference_price);
    let audit_candidates =
        ptg2_scanner::v3_runs::read_audit_candidate_file(output_a.join("audit_candidates.bin"))
            .unwrap();
    assert_eq!(audit_candidates.len(), 5);
    assert!(audit_candidates.windows(2).all(|pair| pair[0] != pair[1]));
    let provider_code_record = shared_serving
        .iter()
        .find(|record| record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND)
        .unwrap();
    assert!(!decode_test_provider_block(
        &logical_test_payload(
            &shared_serving,
            PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
            provider_code_record.block_key,
        ),
        provider_code_record.block_key,
    )
    .is_empty());
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn direct_v3_finalizer_is_byte_identical_with_1_8_and_16_workers() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-worker-parity-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let coverage_scope_id = [0x73; COVERAGE_SCOPE_ID_BYTES];
    let partition_count = 64usize;
    let mut rows_by_partition = BTreeMap::new();
    for candidate in 0..10_000usize {
        let code: &'static str = Box::leak(format!("{:05}", 10_000 + candidate).into_boxed_str());
        let code_fields = NaturalLeanCodeFields {
            coverage_scope_id: &coverage_scope_id,
            reported_code_system: Some("CPT"),
            reported_code: Some(code),
            negotiation_arrangement: Some("FFS"),
            billing_code_type_version: None,
            name: None,
            description: None,
        };
        let record = ServingRunRecord {
            code_id: code_fields.identity(),
            provider_set_id: prefixed_test_id(1, candidate as u8),
            price_set_id: prefixed_test_id(2, 1),
            provider_count: candidate as u32 + 1,
        };
        let partition = partition_for_record(&record, partition_count).unwrap();
        rows_by_partition
            .entry(partition)
            .or_insert(V3FinalizerTestRow {
                coverage_scope_id,
                code_system: Some("CPT"),
                code: Some(code),
                negotiation_arrangement: Some("FFS"),
                provider_id: record.provider_set_id,
                price_id: record.price_set_id,
                provider_count: record.provider_count,
            });
        if rows_by_partition.len() == 16 {
            break;
        }
    }
    assert_eq!(rows_by_partition.len(), 16);
    let rows = rows_by_partition.into_values().collect::<Vec<_>>();
    let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
        &base,
        "worker-parity",
        &rows,
        0,
        1,
        partition_count,
    );
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "worker-parity", &[prefixed_test_id(2, 1)]);
    let file_names = [
        "audit_candidates.bin",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
    ];
    let mut baseline_files = None;
    let mut baseline_support_digest = None;
    for workers in [1usize, 8, 16] {
        let output = base.join(format!("output-{workers}"));
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output.clone(),
            manifest_paths: vec![manifest.clone()],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(workers, 4),
            workers,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input: price_key_map_input.clone(),
            price_key_map_row_count: 1,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();
        assert_eq!(summary["configured_workers"], workers);
        assert_eq!(summary["workers"], workers);
        let files = file_names
            .iter()
            .map(|file_name| (*file_name, std::fs::read(output.join(file_name)).unwrap()))
            .collect::<BTreeMap<_, _>>();
        let support_digest = summary["dictionaries"]["support_digest"].clone();
        if let Some(expected) = baseline_files.as_ref() {
            assert_eq!(&files, expected, "V3 output changed with {workers} workers");
            assert_eq!(
                Some(&support_digest),
                baseline_support_digest.as_ref(),
                "support digest changed with {workers} workers"
            );
        } else {
            baseline_files = Some(files);
            baseline_support_digest = Some(support_digest);
        }
    }
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn direct_v3_finalizer_matches_reference_on_randomized_duplicate_heavy_rows() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-randomized-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let codes = [
        "10001", "10002", "10003", "10004", "20001", "20002", "20003", "20004", "30001", "30002",
        "30003", "30004", "40001", "40002", "40003", "40004",
    ];
    let mut state = 0x9e37_79b9_7f4a_7c15u64;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state
    };
    let mut rows = Vec::new();
    for index in 0..600usize {
        let code_index = next() as usize % codes.len();
        let provider_index = next() as usize % 24;
        let price_index = next() as usize % 17;
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x71; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some(codes[code_index]),
            negotiation_arrangement: Some(if code_index.is_multiple_of(2) {
                "FFS"
            } else {
                "BUNDLE"
            }),
            provider_id: prefixed_test_id(0x3100, provider_index as u8 + 1),
            price_id: prefixed_test_id(0x4200, price_index as u8 + 1),
            provider_count: provider_index as u32 + 1,
        };
        rows.push(row.clone());
        if index.is_multiple_of(2) {
            rows.push(row.clone());
        }
        if index.is_multiple_of(7) {
            rows.push(row);
        }
    }
    let manifest = write_v3_finalizer_test_manifest(&base, "randomized", &rows);
    let price_ids_in_key_order = (0..17usize)
        .rev()
        .map(|index| prefixed_test_id(0x4200, index as u8 + 1))
        .collect::<Vec<_>>();
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "randomized", &price_ids_in_key_order);
    let output = base.join("output");
    let summary = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output.clone(),
        manifest_paths: vec![manifest],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(4, 13),
        workers: 4,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: price_ids_in_key_order.len() as u64,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();

    let (reference_serving, reference_price) = reference_v3_assigned_records(
        &rows,
        &vec![0; rows.len()],
        &output,
        &price_ids_in_key_order,
    );
    assert_eq!(
        read_test_shared_binary_records(
            std::fs::read(output.join("shared_serving_blocks.copy")).unwrap()
        ),
        reference_serving
    );
    assert_eq!(
        read_test_shared_binary_records(
            std::fs::read(output.join("shared_price_dictionary_blocks.copy")).unwrap()
        ),
        reference_price
    );
    assert_eq!(summary["preservation"]["source_records"], rows.len() as u64);
    assert!(
        summary["preservation"]["duplicate_serving_records"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(
        summary["partition_assignment_sorts"]["strategy"],
        "immutable_dense_maps_bounded_direct_runs_v2"
    );
    assert_eq!(
        summary["partition_assignment_sorts"]["global_assignment_cascade"],
        false
    );
    assert!(summary.get("dense_assignment_sorts").is_none());
    assert!(
        summary["scratch_io"]["total_bytes_written"]
            .as_u64()
            .unwrap()
            > 0
    );
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn strict_v3_source_has_no_global_assignment_cascade_artifacts() {
    let source = include_str!("../../main.rs");
    for obsolete in [
        ["assigned", "-provider.unsorted"].concat(),
        ["assigned", "-price.unsorted"].concat(),
        ["join_v3_", "provider_keys"].concat(),
        ["join_v3_", "price_keys"].concat(),
    ] {
        assert!(
            !source.contains(&obsolete),
            "obsolete V3 cascade artifact: {obsolete}"
        );
    }
}
#[test]
#[ignore = "focused release-mode V3 finalizer benchmark"]
fn benchmark_v3_finalizer_250k_duplicate_heavy_rows() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let base = std::env::temp_dir().join(format!(
        "ptg2-v3-finalizer-benchmark-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let codes = [
        "10001", "10002", "10003", "10004", "20001", "20002", "20003", "20004", "30001", "30002",
        "30003", "30004", "40001", "40002", "40003", "40004",
    ];
    let rows = (0..250_000usize)
        .map(|index| {
            let duplicate_bucket = index / 2;
            let code_index = duplicate_bucket % codes.len();
            let provider_index = duplicate_bucket % 64;
            let price_index = duplicate_bucket % 32;
            V3FinalizerTestRow {
                coverage_scope_id: [0x73; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some(codes[code_index]),
                negotiation_arrangement: Some("FFS"),
                provider_id: prefixed_test_id(0x5100, provider_index as u8 + 1),
                price_id: prefixed_test_id(0x6200, price_index as u8 + 1),
                provider_count: provider_index as u32 + 1,
            }
        })
        .collect::<Vec<_>>();
    let manifest = write_v3_finalizer_test_manifest(&base, "benchmark", &rows);
    let price_ids = (0..32usize)
        .rev()
        .map(|index| prefixed_test_id(0x6200, index as u8 + 1))
        .collect::<Vec<_>>();
    let price_key_map_input = write_v3_finalizer_test_price_key_map(&base, "benchmark", &price_ids);
    let output = base.join("output");
    let started_at = Instant::now();
    let summary = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output,
        manifest_paths: vec![manifest],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(8, 50_000),
        workers: 8,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: price_ids.len() as u64,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();
    eprintln!(
        "{}",
        json!({
            "benchmark": "v3_finalizer_250k_duplicate_heavy_rows",
            "elapsed_seconds": started_at.elapsed().as_secs_f64(),
            "scratch_io": summary["scratch_io"],
            "timings": summary["timings"],
        })
    );
    assert_eq!(summary["preservation"]["encoded_records"], 250_000);
    let _ = std::fs::remove_dir_all(base);
}
#[test]
#[ignore = "focused release-mode V3 finalizer 64-partition/two-run benchmark"]
fn benchmark_v3_finalizer_10m_64_partitions_two_runs() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
    let base = std::env::temp_dir().join(format!(
        "ptg2-v3-finalizer-scaling-benchmark-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row_count = 10_000_000usize;
    let provider_cardinality = 100_000usize;
    let price_cardinality = 200_000usize;
    let partition_count = 64usize;
    let manifest = write_v3_finalizer_benchmark_manifest(
        &base,
        "scaling",
        row_count,
        provider_cardinality,
        price_cardinality,
        partition_count,
    );
    let price_ids = (0..price_cardinality)
        .map(|index| indexed_test_id(0x6200, index as u64 + 1))
        .collect::<Vec<_>>();
    let price_key_map_input = write_v3_finalizer_test_price_key_map(&base, "scaling", &price_ids);
    let output = base.join("output");
    let started_at = Instant::now();
    let summary = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output,
        manifest_paths: vec![manifest],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(16, 100_000),
        workers: 16,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: price_cardinality as u64,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();
    eprintln!(
        "{}",
        json!({
            "benchmark": "v3_finalizer_10m_64_partitions_two_runs",
            "elapsed_seconds": started_at.elapsed().as_secs_f64(),
            "row_count": row_count,
            "provider_cardinality": provider_cardinality,
            "price_cardinality": price_cardinality,
            "partition_count": partition_count,
            "resource_configuration": summary["resource_configuration"],
            "scratch_io": summary["scratch_io"],
            "timings": summary["timings"],
            "serving_storage": summary["blocks"]["serving"]["stored_payload_bytes"],
            "price_storage": summary["blocks"]["price_dictionary"]["stored_payload_bytes"],
        })
    );
    assert_eq!(summary["preservation"]["source_records"], row_count as u64);
    assert_eq!(summary["preservation"]["encoded_records"], row_count as u64);
    assert!(summary["preservation"]["all_source_occurrences_preserved"]
        .as_bool()
        .unwrap());
    assert_eq!(
        summary["price_key_map"]["row_count"],
        price_cardinality as u64
    );
    assert_eq!(summary["partition_count"], partition_count as u64);
    assert_eq!(summary["source_identity_scan"]["passes"], 0);
    assert_eq!(summary["source_identity_scan"]["bytes_read"], 0);
    assert_eq!(summary["timings"]["identity_scan_seconds"], 0.0);
    let assignment = &summary["partition_assignment_sorts"];
    assert_eq!(
        assignment["strategy"],
        "immutable_dense_maps_bounded_direct_runs_v2"
    );
    assert_eq!(assignment["unsorted_materialization"], false);
    assert_eq!(assignment["final_sorted_materialization"], false);
    assert_eq!(assignment["aggregate"]["chunk_count"], 128);
    assert_eq!(assignment["aggregate"]["final_copy_bytes"], 0);
    let partitions = assignment["partitions"].as_array().unwrap();
    assert_eq!(partitions.len(), partition_count);
    for (partition, summary) in partitions.iter().enumerate() {
        assert_eq!(summary["partition"], partition as u64);
        assert_eq!(summary["row_count"], 156_250);
        assert_eq!(summary["sort"]["chunk_count"], 2);
        assert_eq!(summary["sort"]["final_copy_bytes"], 0);
    }
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn direct_v3_finalizer_preserves_dense_source_provenance_and_multiplicity() {
    let _env_lock = scanner_env_lock().lock().unwrap();
    let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-provenance-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x44; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99213"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 2,
    };
    let source_zero = write_v3_finalizer_test_manifest_with_source(
        &base,
        "source-zero",
        &[row.clone(), row.clone()],
        0,
        2,
    );
    let source_one =
        write_v3_finalizer_test_manifest_with_source(&base, "source-one", &[row], 1, 2);
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "provenance", &[prefixed_test_id(2, 1)]);
    let output = base.join("output");
    let summary = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: output.clone(),
        manifest_paths: vec![source_one, source_zero],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
        workers: 2,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: 1,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();

    assert_eq!(summary["format"], "ptg2_v3_direct_finalizer_v3");
    assert_eq!(summary["storage_generation"], "shared_blocks_v3");
    assert_eq!(summary["cold_lookup_contract"], "ptg_v3_cold_v2");
    assert_eq!(summary["shared_block_layout"], "dense_shared_blocks_v3");
    assert_eq!(
        summary["resource_configuration"],
        json!({
            "contract": V3_FINALIZER_RESOURCE_CONTRACT,
            "workers": 2,
            "identity_map_max_bytes": V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            "total_sort_memory_bytes": v3_finalizer_test_sort_memory_bytes(2, 1),
            "sort_memory_scope": V3_FINALIZER_SORT_MEMORY_SCOPE,
        })
    );
    assert_eq!(summary["source_count"], 2);
    assert_eq!(summary["source_key_bits"], 1);
    assert_eq!(summary["source_key_bytes"], 1);
    assert_eq!(summary["tagged_record_bytes"], 53);
    assert_eq!(summary["source"]["record_count"], 3);
    assert_eq!(summary["source"]["source_key_bytes"], 1);
    assert_eq!(summary["source"]["tagged_record_bytes"], 53);
    assert_eq!(summary["source_identity_scan"]["bytes_read"], 0);
    assert_eq!(summary["source_identity_scan"]["passes"], 0);
    assert_eq!(
        summary["source_identity_scan"]["tagged_partition_sort"],
        false
    );
    assert_eq!(
        summary["source"]["record_counts_by_source"],
        json!({"0": 2, "1": 1})
    );
    assert_eq!(summary["preservation"]["distinct_serving_records"], 2);
    assert_eq!(summary["preservation"]["duplicate_serving_records"], 1);
    assert_eq!(summary["preservation"]["encoded_records"], 3);
    assert!(summary["preservation"]["all_source_occurrences_preserved"]
        .as_bool()
        .unwrap());
    assert_eq!(summary["audit_candidates"]["source_key_included"], true);
    assert_eq!(summary["audit_candidates"]["source_count"], 2);
    assert_eq!(summary["audit_candidates"]["source_key_bits"], 1);
    assert_eq!(
        summary["audit_candidates"]["record_counts_by_source"],
        json!({"0": 2, "1": 1})
    );

    let audit =
        ptg2_scanner::v3_runs::read_audit_candidate_file(output.join("audit_candidates.bin"))
            .unwrap();
    assert_eq!(
        audit.iter().map(|row| row.source_key).collect::<Vec<_>>(),
        vec![0, 0, 1]
    );

    let records = read_test_shared_binary_records(
        std::fs::read(output.join("shared_serving_blocks.copy")).unwrap(),
    );
    let provider_shard =
        logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND, 0);
    let mut cursor = 0usize;
    assert_eq!(
        provider_shard[cursor],
        PTG2_SERVING_BINARY_V3_GROUPED_FORMAT_VERSION
    );
    cursor += 1;
    assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 2);
    assert_eq!(provider_shard[cursor], 1);
    cursor += 1;
    assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 0);
    assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 3);
    for _ in 0..3 {
        assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 0);
    }
    assert_eq!(&provider_shard[cursor..], &[0b0000_0100]);

    let forward = logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 0);
    cursor = 0;
    assert_eq!(forward[cursor], PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION);
    cursor += 1;
    assert_eq!(test_read_uvarint(&forward, &mut cursor), 2);
    assert_eq!(forward[cursor], 1);
    cursor += 1;
    assert_eq!(test_read_uvarint(&forward, &mut cursor), 3);
    for _ in 0..3 {
        assert_eq!(test_read_uvarint(&forward, &mut cursor), 0);
        assert_eq!(test_read_uvarint(&forward, &mut cursor), 2);
        assert_eq!(test_read_uvarint(&forward, &mut cursor), 0);
    }
    assert_eq!(&forward[cursor..], &[0b0000_0100]);

    let reverse = logical_test_payload(&records, PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND, 0);
    cursor = 0;
    assert_eq!(reverse[cursor], PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION);
    cursor += 1;
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 2);
    assert_eq!(reverse[cursor], 1);
    cursor += 1;
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 1);
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 2);
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 3);
    assert_eq!(test_read_uvarint(&reverse, &mut cursor), 3);
    for _ in 0..3 {
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
    }
    assert_eq!(&reverse[cursor..], &[0b0000_0100]);
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_missing_out_of_range_and_non_dense_source_metadata() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-source-metadata-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let valid_path = write_v3_finalizer_test_manifest(
        &base,
        "valid-source",
        &[V3FinalizerTestRow {
            coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 2,
        }],
    );
    let valid: Value = serde_json::from_slice(&std::fs::read(&valid_path).unwrap()).unwrap();
    let cases = [
        ("missing-key", None, Some(json!(1))),
        ("missing-count", Some(json!(0)), None),
        ("string-key", Some(json!("0")), Some(json!(1))),
        ("out-of-range", Some(json!(1)), Some(json!(1))),
        ("non-dense", Some(json!(0)), Some(json!(2))),
        ("zero-count", Some(json!(0)), Some(json!(0))),
    ];
    for (label, source_key, source_count) in cases {
        let mut manifest = valid.clone();
        let entry = manifest["serving_run_partition_files"][0]
            .as_object_mut()
            .unwrap();
        match source_key {
            Some(value) => {
                entry.insert("source_key".to_owned(), value);
            }
            None => {
                entry.remove("source_key");
            }
        }
        match source_count {
            Some(value) => {
                entry.insert("source_count".to_owned(), value);
            }
            None => {
                entry.remove("source_count");
            }
        }
        let path = base.join(format!("{label}.json"));
        std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        let error = load_v3_finalizer_inputs(&[path]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}");
    }
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_invalid_duplicate_and_non_deterministic_source_identities() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-source-identities-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x64; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99221"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let source_zero = write_v3_finalizer_test_manifest_with_source(
        &base,
        "identity-zero",
        std::slice::from_ref(&row),
        0,
        2,
    );
    let source_one = write_v3_finalizer_test_manifest_with_source(
        &base,
        "identity-one",
        std::slice::from_ref(&row),
        1,
        2,
    );
    let zero_payload: Value =
        serde_json::from_slice(&std::fs::read(&source_zero).unwrap()).unwrap();
    let one_payload: Value = serde_json::from_slice(&std::fs::read(&source_one).unwrap()).unwrap();
    let zero_identity = zero_payload["source_run_contracts"][0]["source_identity"].clone();
    let one_identity = one_payload["source_run_contracts"][0]["source_identity"].clone();

    let mut duplicate_one = one_payload.clone();
    set_v3_finalizer_test_source_identity(&mut duplicate_one, zero_identity.clone());
    let duplicate_one_path = base.join("duplicate-one.json");
    std::fs::write(
        &duplicate_one_path,
        serde_json::to_vec(&duplicate_one).unwrap(),
    )
    .unwrap();
    let error = load_v3_finalizer_inputs(&[source_zero.clone(), duplicate_one_path]).unwrap_err();
    assert!(error
        .to_string()
        .contains("source identities must be unique"));

    let mut swapped_zero = zero_payload.clone();
    let mut swapped_one = one_payload.clone();
    set_v3_finalizer_test_source_identity(&mut swapped_zero, one_identity);
    set_v3_finalizer_test_source_identity(&mut swapped_one, zero_identity);
    let swapped_zero_path = base.join("swapped-zero.json");
    let swapped_one_path = base.join("swapped-one.json");
    std::fs::write(
        &swapped_zero_path,
        serde_json::to_vec(&swapped_zero).unwrap(),
    )
    .unwrap();
    std::fs::write(&swapped_one_path, serde_json::to_vec(&swapped_one).unwrap()).unwrap();
    let error = load_v3_finalizer_inputs(&[swapped_zero_path, swapped_one_path]).unwrap_err();
    assert!(error
        .to_string()
        .contains("source keys do not match deterministic identity ordering"));

    for (label, identity) in [
        (
            "unicode-source-type",
            json!({
                "source_type": "in_netwörk",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": "00".repeat(32),
            }),
        ),
        (
            "unsupported-identity-kind",
            json!({
                "source_type": "in_network",
                "identity_kind": "sha256",
                "identity_sha256": "00".repeat(32),
            }),
        ),
    ] {
        let mut payload = zero_payload.clone();
        set_v3_finalizer_test_source_identity(&mut payload, identity);
        let path = base.join(format!("{label}.json"));
        std::fs::write(&path, serde_json::to_vec(&payload).unwrap()).unwrap();
        let error = load_v3_finalizer_inputs(&[path]).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}");
    }
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_authenticates_run_content_on_both_scans() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-run-authentication-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let provider_a = prefixed_test_id(1, 1);
    let provider_b = prefixed_test_id(1, 2);
    let price_id = prefixed_test_id(2, 1);
    let rows = [
        V3FinalizerTestRow {
            coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider_a,
            price_id,
            provider_count: 2,
        },
        V3FinalizerTestRow {
            coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider_b,
            price_id,
            provider_count: 2,
        },
    ];
    let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
        &base,
        "authenticated",
        &rows,
        0,
        1,
        1,
    );
    let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
    let (code_dictionary, _, _) = load_v3_code_dictionary(&inputs.code_dictionaries).unwrap();
    let code_map = build_v3_code_identity_map(&code_dictionary).unwrap();
    let code_partition_ranges =
        build_v3_code_partition_ranges(&code_dictionary, inputs.partition_count).unwrap();
    let mut price_map = DenseIdentityMap::with_capacity(1).unwrap();
    price_map
        .insert(
            price_id,
            DenseIdentityValue {
                key: 0,
                auxiliary: 0,
            },
        )
        .unwrap();
    let combined_price_seen_words = Mutex::new(vec![0u64; 1]);
    let prepared = prepare_v3_partition(
        0,
        inputs.partitions,
        &V3PartitionScanContext {
            partition_count: 1,
            price_key_count: 1,
            work_root: &base,
            code_map: &code_map,
            code_partition_ranges: &code_partition_ranges,
            price_map: &price_map,
            combined_price_seen_words: &combined_price_seen_words,
            provider_sort_record_limit: 2,
        },
    )
    .unwrap();
    let mut provider_map = DenseIdentityMap::with_capacity(2).unwrap();
    provider_map
        .insert(
            provider_a,
            DenseIdentityValue {
                key: 0,
                auxiliary: 2,
            },
        )
        .unwrap();
    provider_map
        .insert(
            provider_b,
            DenseIdentityValue {
                key: 1,
                auxiliary: 2,
            },
        )
        .unwrap();

    let run_path = &prepared.inputs[0].path;
    let mut encoded = std::fs::read(run_path).unwrap();
    let second_provider_offset = SERVING_RUN_RECORD_BYTES + GLOBAL_ID_BYTES;
    let first_provider = encoded[GLOBAL_ID_BYTES..2 * GLOBAL_ID_BYTES].to_vec();
    let second_provider =
        encoded[second_provider_offset..second_provider_offset + GLOBAL_ID_BYTES].to_vec();
    encoded[GLOBAL_ID_BYTES..2 * GLOBAL_ID_BYTES].copy_from_slice(&second_provider);
    encoded[second_provider_offset..second_provider_offset + GLOBAL_ID_BYTES]
        .copy_from_slice(&first_provider);
    std::fs::write(run_path, encoded).unwrap();

    let identity_mismatch_work = base.join("identity-mismatch");
    std::fs::create_dir_all(&identity_mismatch_work).unwrap();
    let identity_mismatch_seen = Mutex::new(vec![0u64; 1]);
    let identity_error = prepare_v3_partition(
        0,
        prepared.inputs.clone(),
        &V3PartitionScanContext {
            partition_count: 1,
            price_key_count: 1,
            work_root: &identity_mismatch_work,
            code_map: &code_map,
            code_partition_ranges: &code_partition_ranges,
            price_map: &price_map,
            combined_price_seen_words: &identity_mismatch_seen,
            provider_sort_record_limit: 2,
        },
    )
    .unwrap_err();
    assert!(identity_error
        .to_string()
        .contains("digest mismatch during identity scan"));

    let combined_provider_seen_words = Mutex::new(vec![0u64; 1]);
    let combined_assignment_price_seen_words = Mutex::new(vec![0u64; 1]);
    let error = assign_v3_partition(
        0,
        &prepared.inputs,
        &V3AssignmentContext {
            partition_count: 1,
            work_root: &base.join("assignment"),
            code_map: &code_map,
            code_partition_ranges: &code_partition_ranges,
            provider_map: &provider_map,
            provider_key_count: 2,
            price_key_count: 1,
            price_map: &price_map,
            combined_provider_seen_words: &combined_provider_seen_words,
            combined_price_seen_words: &combined_assignment_price_seen_words,
            assigned_record_limit: 2,
            scratch_durability: ScratchDurability::Durable,
        },
    )
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("digest mismatch during assignment scan"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_authenticates_code_dictionary_before_parsing() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-code-authentication-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x56; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99214"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-auth", &[row]);
    let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
    let dictionary_path = &inputs.code_dictionaries[0].path;
    let mut encoded = std::fs::read(dictionary_path).unwrap();
    encoded[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    std::fs::write(dictionary_path, encoded).unwrap();

    let error = load_v3_code_dictionary(&inputs.code_dictionaries).unwrap_err();
    assert!(error.to_string().contains("content digest mismatch"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_provider_metadata_requires_canonical_network_names() {
    let canonical_values = vec![
        "Alpha".to_owned(),
        "B\"eta".to_owned(),
        "C\\name".to_owned(),
        "Line\tName".to_owned(),
    ];
    let canonical = pg_text_array_copy_field(&canonical_values);
    validate_v3_provider_network_names_copy_field(canonical.as_bytes()).unwrap();
    validate_v3_provider_network_names_copy_field(b"{}").unwrap();

    for malformed in [
        br"\N".as_slice(),
        br"{a}".as_slice(),
        br#"{"b","a"}"#.as_slice(),
        br#"{"a","a"}"#.as_slice(),
        br#"{"a",}"#.as_slice(),
        br#"{"a"}junk"#.as_slice(),
    ] {
        let error = validate_v3_provider_network_names_copy_field(malformed).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
#[test]
fn v3_provider_metadata_authenticates_exact_bytes_rows_and_shape() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-provider-metadata-authentication-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let provider_id = GlobalId128(prefixed_test_id(1, 1)).to_hex();
    let valid_payload = format!("{provider_id}\t1\t{{}}\n").into_bytes();
    let input_for = |path: &Path, payload: &[u8], row_count: u64| {
        let sha256: [u8; 32] = Sha256::digest(payload).into();
        V3FinalizerProviderMetadataInput {
            path: path.to_path_buf(),
            source_key: 0,
            row_count,
            bytes: payload.len() as u64,
            sha256,
        }
    };

    let tampered_path = base.join("tampered.copy");
    std::fs::write(&tampered_path, &valid_payload).unwrap();
    let tampered_input = input_for(&tampered_path, &valid_payload, 1);
    let mut tampered_payload = valid_payload.clone();
    tampered_payload[provider_id.len() + 1] = b'2';
    assert_eq!(tampered_payload.len(), valid_payload.len());
    std::fs::write(&tampered_path, tampered_payload).unwrap();
    let error = stage_v3_provider_metadata(
        &[tampered_input],
        &base.join("tampered.unsorted"),
        &base.join("tampered.sorted"),
        &base.join("tampered-work"),
        1,
    )
    .unwrap_err();
    assert!(error.to_string().contains("content changed"));

    let malformed_path = base.join("malformed.copy");
    let malformed_payload = format!("{provider_id}\t1\t{{}}\textra\n").into_bytes();
    std::fs::write(&malformed_path, &malformed_payload).unwrap();
    let error = stage_v3_provider_metadata(
        &[input_for(&malformed_path, &malformed_payload, 1)],
        &base.join("malformed.unsorted"),
        &base.join("malformed.sorted"),
        &base.join("malformed-work"),
        1,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exactly three fields"));

    let row_count_path = base.join("row-count.copy");
    std::fs::write(&row_count_path, &valid_payload).unwrap();
    let error = stage_v3_provider_metadata(
        &[input_for(&row_count_path, &valid_payload, 2)],
        &base.join("row-count.unsorted"),
        &base.join("row-count.sorted"),
        &base.join("row-count-work"),
        1,
    )
    .unwrap_err();
    assert!(error.to_string().contains("row count mismatch"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_provider_metadata_bound_to_another_source_run() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-provider-metadata-source-contract-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let manifest = write_v3_finalizer_test_manifest(
        &base,
        "provider-source-contract",
        &[V3FinalizerTestRow {
            coverage_scope_id: [0x58; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99215"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        }],
    );
    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    payload["provider_set_metadata_files"][0]["source_run_contract_sha256"] =
        json!("00".repeat(32));
    complete_v3_finalizer_test_manifest_contracts(&mut payload);
    std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

    let error = load_v3_finalizer_inputs(&[manifest]).unwrap_err();
    assert!(error
        .to_string()
        .contains("bound to another source-run contract"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_accepts_multiple_dictionary_and_provider_metadata_shards_for_one_source() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-code-shards-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x60; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99218"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-shards", &[row]);
    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let first_entry = payload["serving_run_code_dictionary_files"][0].clone();
    let first_path = PathBuf::from(first_entry["path"].as_str().unwrap());
    let second_path = first_path.with_file_name("dictionary-shard-2.ready");
    std::fs::copy(&first_path, &second_path).unwrap();
    let mut second_entry = first_entry;
    second_entry["path"] = json!(second_path);
    payload["serving_run_code_dictionary_files"]
        .as_array_mut()
        .unwrap()
        .push(second_entry);
    let first_metadata = payload["provider_set_metadata_files"][0].clone();
    let first_metadata_path = PathBuf::from(first_metadata["path"].as_str().unwrap());
    let second_metadata_path =
        first_metadata_path.with_file_name("0-provider-metadata-shard.ready");
    std::fs::copy(&first_metadata_path, &second_metadata_path).unwrap();
    let mut second_metadata = first_metadata;
    second_metadata["path"] = json!(second_metadata_path);
    payload["provider_set_metadata_files"]
        .as_array_mut()
        .unwrap()
        .push(second_metadata);
    complete_v3_finalizer_test_manifest_contracts(&mut payload);
    std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

    let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
    assert_eq!(inputs.code_dictionaries.len(), 2);
    assert_eq!(inputs.provider_metadata[0].path, second_metadata_path);
    assert_eq!(inputs.provider_metadata[1].path, first_metadata_path);
    let (codes, source_rows, source_bytes) =
        load_v3_code_dictionary(&inputs.code_dictionaries).unwrap();
    assert_eq!(codes.len(), 1);
    assert_eq!(source_rows, 2);
    assert_eq!(source_bytes, first_path.metadata().unwrap().len() * 2);
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_coherent_serving_manifest_subset() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-serving-subset-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x61; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99219"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "serving-subset", &[row]);
    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    payload["serving_run_partition_files"] = json!([]);
    complete_v3_finalizer_test_manifest_contracts(&mut payload);
    std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

    let error = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap_err();
    assert!(error
        .to_string()
        .contains("files do not match complete source contract"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_dictionary_array_that_omits_declared_shard() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-dictionary-subset-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99220"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-subset", &[row]);
    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    let first_entry = payload["serving_run_code_dictionary_files"][0].clone();
    let first_path = PathBuf::from(first_entry["path"].as_str().unwrap());
    let second_path = first_path.with_file_name("dictionary-subset-shard-2.ready");
    std::fs::copy(&first_path, &second_path).unwrap();
    let mut second_entry = first_entry;
    second_entry["path"] = json!(second_path);
    payload["serving_run_code_dictionary_files"]
        .as_array_mut()
        .unwrap()
        .push(second_entry);
    complete_v3_finalizer_test_manifest_contracts(&mut payload);

    payload["serving_run_code_dictionary_files"]
        .as_array_mut()
        .unwrap()
        .pop();
    let remaining = payload["serving_run_code_dictionary_files"]
        .as_array()
        .unwrap()
        .clone();
    payload["expected_code_dictionary_files"] = json!(remaining.len());
    payload["expected_code_dictionary_rows"] = json!(remaining
        .iter()
        .map(|entry| entry["row_count"].as_u64().unwrap())
        .sum::<u64>());
    payload["expected_code_dictionary_bytes"] = json!(remaining
        .iter()
        .map(|entry| entry["bytes"].as_u64().unwrap())
        .sum::<u64>());
    let entry_contracts = remaining
        .iter()
        .map(|entry| {
            json!({
                "source_key": entry["source_key"],
                "row_count": entry["row_count"],
                "bytes": entry["bytes"],
                "sha256": entry["sha256"],
                "source_run_contract_sha256": entry["source_run_contract_sha256"],
                "code_dictionary_contract_sha256": entry["code_dictionary_contract_sha256"],
            })
        })
        .collect::<Vec<_>>();
    payload["code_dictionary_contract_set_sha256"] = json!(test_json_sha256(&json!({
        "code_dictionary_contracts": entry_contracts,
    })));
    std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

    let error = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap_err();
    assert!(error
        .to_string()
        .contains("files do not match complete source contract"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_dictionary_source_contract_mismatch() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-code-contract-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x57; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99215"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "contract-auth", &[row]);
    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    payload["serving_run_code_dictionary_files"][0]["source_run_contract_sha256"] =
        json!("00".repeat(32));
    let mismatched = base.join("contract-mismatch.json");
    std::fs::write(&mismatched, serde_json::to_vec(&payload).unwrap()).unwrap();
    let error = load_v3_finalizer_inputs(&[mismatched]).unwrap_err();
    assert!(error
        .to_string()
        .contains("code dictionary source contract does not match"));

    let mut payload: Value = serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
    payload["source_run_contracts"][0]["row_count"] = json!(2);
    let corrupt = base.join("corrupt-source-contract.json");
    std::fs::write(&corrupt, serde_json::to_vec(&payload).unwrap()).unwrap();
    let error = load_v3_finalizer_inputs(&[corrupt]).unwrap_err();
    assert!(error
        .to_string()
        .contains("source-run contract set digest mismatch"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_rejects_dictionary_memory_before_loading_it() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-code-memory-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x58; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99216"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-memory", &[row]);
    let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
    let required = v3_code_dictionary_memory_estimate(&inputs.code_dictionaries).unwrap();
    let dictionary_path = &inputs.code_dictionaries[0].path;
    let mut encoded = std::fs::read(dictionary_path).unwrap();
    encoded[..8].copy_from_slice(&u64::MAX.to_be_bytes());
    std::fs::write(dictionary_path, encoded).unwrap();
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "memory", &[prefixed_test_id(2, 1)]);

    let error = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: base.join("output"),
        manifest_paths: vec![manifest],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 1),
        workers: 1,
        identity_map_max_bytes: required - 1,
        price_key_map_input,
        price_key_map_row_count: 1,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap_err();
    assert!(error
        .to_string()
        .contains("code dictionary resident state requires"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_counts_only_nonempty_partition_workers() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-sparse-workers-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let row = V3FinalizerTestRow {
        coverage_scope_id: [0x59; COVERAGE_SCOPE_ID_BYTES],
        code_system: Some("CPT"),
        code: Some("99217"),
        negotiation_arrangement: Some("FFS"),
        provider_id: prefixed_test_id(1, 1),
        price_id: prefixed_test_id(2, 1),
        provider_count: 1,
    };
    let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
        &base,
        "sparse-workers",
        &[row],
        0,
        1,
        128,
    );
    let price_key_map_input =
        write_v3_finalizer_test_price_key_map(&base, "sparse", &[prefixed_test_id(2, 1)]);
    let summary = finalize_v3_runs(&V3FinalizerOptions {
        output_directory: base.join("output"),
        manifest_paths: vec![manifest],
        scratch_durability: ScratchDurability::Durable,
        total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 2),
        workers: 16,
        identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
        price_key_map_input,
        price_key_map_row_count: 1,
        price_membership_inputs: Vec::new(),
        price_atom_inputs: Vec::new(),
    })
    .unwrap();
    assert_eq!(summary["configured_workers"], 16);
    assert_eq!(summary["workers"], 1);
    assert_eq!(
        summary["sort_memory_reserved_overhead_bytes"],
        V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
    );
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_requires_one_existing_price_key_map_input() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-direct-v3-finalizer-price-map-cli-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let map_path = base.join("price-map.copy");
    std::fs::write(&map_path, pg_binary_copy_rows(&[])).unwrap();
    let output = base.join("output").display().to_string();
    let manifest = base.join("manifest.json").display().to_string();

    let missing = parse_v3_finalizer_options(&[output.clone(), manifest.clone()]).unwrap_err();
    assert!(missing
        .to_string()
        .contains("requires --price-key-map-input"));

    let duplicate = parse_v3_finalizer_options(&[
        output.clone(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        manifest.clone(),
    ])
    .unwrap_err();
    assert!(duplicate.to_string().contains("only once"));

    let missing_count = parse_v3_finalizer_options(&[
        output.clone(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        manifest.clone(),
    ])
    .unwrap_err();
    assert!(missing_count
        .to_string()
        .contains("requires --price-key-map-row-count"));

    for invalid_count in ["0", "4294967297"] {
        let invalid = parse_v3_finalizer_options(&[
            output.clone(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-row-count".to_owned(),
            invalid_count.to_owned(),
            manifest.clone(),
        ])
        .unwrap_err();
        assert!(invalid.to_string().contains("must be between 1 and"));
    }

    let duplicate_count = parse_v3_finalizer_options(&[
        output.clone(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        "--price-key-map-row-count".to_owned(),
        "1".to_owned(),
        "--price-key-map-row-count".to_owned(),
        "1".to_owned(),
        manifest.clone(),
    ])
    .unwrap_err();
    assert!(duplicate_count.to_string().contains("only once"));

    let options = parse_v3_finalizer_options(&[
        output,
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        "--price-key-map-row-count".to_owned(),
        "1".to_owned(),
        "--workers".to_owned(),
        "2".to_owned(),
        "--identity-map-max-bytes".to_owned(),
        (64 * 1024 * 1024usize).to_string(),
        "--total-sort-memory-bytes".to_owned(),
        (32 * 1024 * 1024usize).to_string(),
        manifest.clone(),
    ])
    .unwrap();
    assert_eq!(options.price_key_map_input, map_path);
    assert_eq!(options.price_key_map_row_count, 1);
    assert_eq!(options.workers, 2);
    assert_eq!(options.identity_map_max_bytes, 64 * 1024 * 1024);
    assert_eq!(options.total_sort_memory_bytes, 32 * 1024 * 1024);
    assert_eq!(options.scratch_durability, ScratchDurability::Durable);

    let explicit_ephemeral = parse_v3_finalizer_options(&[
        base.join("ephemeral-output").display().to_string(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        "--price-key-map-row-count".to_owned(),
        "1".to_owned(),
        "--workers".to_owned(),
        "2".to_owned(),
        "--identity-map-max-bytes".to_owned(),
        (64 * 1024 * 1024usize).to_string(),
        "--total-sort-memory-bytes".to_owned(),
        (32 * 1024 * 1024usize).to_string(),
        "--scratch-durability".to_owned(),
        "ephemeral".to_owned(),
        manifest.clone(),
    ])
    .unwrap();
    assert_eq!(
        explicit_ephemeral.scratch_durability,
        ScratchDurability::Ephemeral
    );

    let missing_durability = parse_v3_finalizer_options(&[
        base.join("missing-durability-output").display().to_string(),
        "--scratch-durability".to_owned(),
    ])
    .unwrap_err();
    assert!(missing_durability
        .to_string()
        .contains(v3_finalizer_usage()));

    for durability_arguments in [
        vec!["--scratch-durability", "unsafe"],
        vec![
            "--scratch-durability",
            "ephemeral",
            "--scratch-durability",
            "durable",
        ],
    ] {
        let mut arguments = vec![
            base.join("invalid-durability-output").display().to_string(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-row-count".to_owned(),
            "1".to_owned(),
            "--workers".to_owned(),
            "2".to_owned(),
            "--identity-map-max-bytes".to_owned(),
            (64 * 1024 * 1024usize).to_string(),
            "--total-sort-memory-bytes".to_owned(),
            (32 * 1024 * 1024usize).to_string(),
        ];
        arguments.extend(durability_arguments.into_iter().map(str::to_owned));
        arguments.push(manifest.clone());
        let error = parse_v3_finalizer_options(&arguments).unwrap_err();
        assert!(
            error.to_string().contains("durability") || error.to_string().contains("only once")
        );
    }

    let legacy = parse_v3_finalizer_options(&[
        base.join("legacy-output").display().to_string(),
        "--price-key-map-input".to_owned(),
        map_path.display().to_string(),
        "--memory-records".to_owned(),
        "1000".to_owned(),
        manifest,
    ])
    .unwrap_err();
    assert!(legacy.to_string().contains("process-wide"));
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v3_finalizer_sort_memory_budget_is_process_wide() {
    let total = v3_finalizer_test_sort_memory_bytes(4, 2);
    let budget = v3_sort_memory_budget(total, 4).unwrap();
    assert_eq!(budget.active_workers, 4);
    assert_eq!(
        budget.reserved_overhead_bytes,
        4 * V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
    );
    assert_eq!(budget.payload_bytes, 160);
    assert_eq!(budget.bytes_per_active_worker, 40);
    assert_eq!(
        budget.single_sort_payload_bytes,
        total - V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
    );
    assert_eq!(budget.assigned_records_per_worker, 2);
    assert!(v3_sort_memory_budget(
        4 * V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER - 1,
        4
    )
    .unwrap_err()
    .to_string()
    .contains("reserve"));
}
