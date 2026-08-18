use super::*;

fn row(first: &str, second: Option<&str>, strict_bits: i32) -> ArchiveRow {
    let raw = RawAddress {
        first: Some(first.to_string()),
        second: second.map(str::to_string),
        city: Some("Dallas".to_string()),
        state: Some("TX".to_string()),
        postal: Some("75001".to_string()),
        country: Some("US".to_string()),
    };
    let canonical = canonicalize_address(
        raw.first.as_deref(),
        raw.second.as_deref(),
        raw.city.as_deref(),
        raw.state.as_deref(),
        raw.postal.as_deref(),
        raw.country.as_deref(),
    );
    build_archive_row(ArchiveInput {
        key: canonical.address_key,
        identity: canonical.identity_key,
        precision: Some("street".to_string()),
        raw,
        strict_bits,
        merged: false,
        stored_state: Some("TX".to_string()),
        stored_zip: Some("75001".to_string()),
    })
}

fn write_archive(path: &Path, rows: &[ArchiveRow]) {
    let mut writer = BufWriter::new(File::create(path).unwrap());
    for row in rows {
        let strict_bits = row.strict_bits.to_string();
        write_copy_fields(
            &mut writer,
            &[
                pg_text_copy_field(row.key.as_deref()),
                pg_text_copy_field(row.identity.as_deref()),
                pg_text_copy_field(row.precision.as_deref()),
                pg_text_copy_field(row.raw.first.as_deref()),
                pg_text_copy_field(row.raw.second.as_deref()),
                pg_text_copy_field(row.raw.city.as_deref()),
                pg_text_copy_field(row.raw.state.as_deref()),
                pg_text_copy_field(row.raw.postal.as_deref()),
                pg_text_copy_field(row.raw.country.as_deref()),
                pg_text_copy_field(Some(&strict_bits)),
                pg_text_copy_field(row.merged.then_some("merged")),
                pg_text_copy_field(row.stored_state.as_deref()),
                pg_text_copy_field(row.stored_zip.as_deref()),
            ],
        )
        .unwrap();
    }
    writer.flush().unwrap();
}

#[test]
fn native_scanner_writes_the_reviewed_candidate_and_receipt() {
    let run_id = "00000000-0000-0000-0000-000000000001";
    let source = row("4007 Clarksville Pike 301", None, 1);
    let target = row("4007 Clarksville Pike", Some("Suite 301"), 6);
    let source_key = source.key.clone().unwrap();
    let target_key = target.key.clone().unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let archive_path = temporary.path().join("archive.copy");
    let membership_path = temporary.path().join("memberships.copy");
    let aliases_path = temporary.path().join("aliases.copy");
    let config_path = temporary.path().join("config.json");
    let output_path = temporary.path().join("candidates.copy");
    let summary_path = temporary.path().join("summary.json");
    write_archive(&archive_path, &[source, target]);
    std::fs::write(
        &membership_path,
        format!("1234567893\t{}\n1234567893\t{}\n", source_key, target_key,),
    )
    .unwrap();
    std::fs::write(&aliases_path, "").unwrap();
    std::fs::write(
        &config_path,
        format!(
            r#"{{"run_id":"{run_id}","state_code":null,"zip_prefix":null,"retry_shadow_run_id":null}}"#,
        ),
    )
    .unwrap();

    derive_evidence_alias_candidates(
        &archive_path,
        &membership_path,
        &aliases_path,
        &config_path,
        &output_path,
        &summary_path,
    )
    .unwrap();

    let candidates = std::fs::read_to_string(&output_path).unwrap();
    let fields = copy_fields(candidates.trim_end(), 14, "candidate").unwrap();
    assert_eq!(fields[0].as_deref(), Some(run_id));
    assert_eq!(fields[8].as_deref(), Some("eligible"));
    assert_eq!(fields[9].as_deref(), Some("pending"));
    assert_eq!(fields[10].as_deref(), Some("candidate_confirmed_bare_unit"));
    assert_eq!(fields[11].as_deref(), Some("exact"));
    assert_eq!(fields[12].as_deref(), Some("1234567893"));
    let summary: serde_json::Value =
        serde_json::from_reader(File::open(summary_path).unwrap()).unwrap();
    assert_eq!(summary["contract"], ADDRESS_EVIDENCE_ALIAS_NATIVE_CONTRACT);
    assert_eq!(summary["archive_rows"], 2);
    assert_eq!(summary["membership_rows"], 2);
    assert_eq!(summary["visible_memberships"], 2);
    assert_eq!(summary["candidate_rows"], 1);
    assert_eq!(summary["output_sha256"], sha256_file(&output_path).unwrap());
}

#[test]
fn native_scanner_exercises_optional_and_fail_closed_paths() {
    assert_eq!(invalid("fixture").kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        invalid("fixture".to_string()).kind(),
        io::ErrorKind::InvalidData
    );
    assert!(copy_fields("one", 2, "fixture").is_err());
    let temporary = tempfile::tempdir().unwrap();
    let malformed_archive = temporary.path().join("malformed-archive.copy");
    let mut malformed_fields = vec!["\\N"; 13];
    malformed_fields[9] = "not-an-integer";
    std::fs::write(
        &malformed_archive,
        format!("{}\n", malformed_fields.join("\t")),
    )
    .unwrap();
    assert!(parse_archive(&malformed_archive).is_err());
    let aliases_path = temporary.path().join("aliases.copy");
    std::fs::write(&aliases_path, "\\N\ttarget\t\\N\n").unwrap();
    assert!(load_aliases(&aliases_path).is_err());
    std::fs::write(&aliases_path, "source\t\\N\t\\N\n").unwrap();
    assert!(load_aliases(&aliases_path).is_err());

    let original = row("10 N Main St", None, 1);
    let rebuilt = build_archive_row(ArchiveInput {
        key: original.key.clone(),
        identity: original.identity.clone(),
        precision: None,
        raw: original.raw.clone(),
        strict_bits: original.strict_bits,
        merged: false,
        stored_state: original.stored_state.clone(),
        stored_zip: original.stored_zip.clone(),
    });
    assert_eq!(
        identity_precision(rebuilt.identity.as_deref()),
        Some("street")
    );
    let config = RunConfig {
        run_id: "00000000-0000-0000-0000-000000000002".to_string(),
        state_code: Some("TX".to_string()),
        zip_prefix: Some("750".to_string()),
        retry_shadow_run_id: Some("00000000-0000-0000-0000-000000000003".to_string()),
    };
    let key = rebuilt.key.clone().unwrap();
    let memberships_path = temporary.path().join("memberships.copy");
    std::fs::write(&memberships_path, format!("1234567893\t{key}\n")).unwrap();
    let memberships = load_memberships(
        &memberships_path,
        &[rebuilt],
        &HashMap::from([(key.clone(), 0)]),
        &config,
        &HashMap::new(),
    )
    .unwrap();
    assert_eq!(memberships.rows, vec![(1234567893, 0)]);

    let source = row("20 Main", None, 1);
    let first_target = row("20 Main St", None, 6);
    let second_target = row("20 Main Ave", None, 6);
    assert!(topology_allows(
        &source,
        &first_target,
        &config,
        &HashMap::from([(
            source.key.clone().unwrap(),
            config.retry_shadow_run_id.clone(),
        )]),
        &HashSet::new(),
    ));
    let marker_rows = vec![original];
    let completion = marker_rows[0].marker_features.completion.clone();
    let markers = marker_set(&marker_rows, &[0], completion.as_deref());
    assert_eq!(markers.direction_count, 1);
    assert_eq!(markers.suffix_count, 1);
    assert!(markers.direction.is_some());
    assert!(markers.suffix.is_some());
    assert!(!marker_conflict(&marker_rows[0].features, markers));

    let output_path = temporary.path().join("sorted-candidates.copy");
    let candidate_rows = vec![source, first_target, second_target];
    let preferred = vec![
        PreferredPair {
            source: 0,
            target: 1,
            rule: "terminal_suffix_omission",
            evidence_npi: 1234567893,
            evidence_npi_count: 1,
            marker_conflict: false,
        },
        PreferredPair {
            source: 0,
            target: 2,
            rule: "terminal_suffix_omission",
            evidence_npi: 1234567893,
            evidence_npi_count: 1,
            marker_conflict: false,
        },
    ];
    let global_targets = HashMap::from([(0, HashSet::from([1, 2]))]);
    assert_eq!(
        write_candidates(
            &output_path,
            &config.run_id,
            &candidate_rows,
            preferred,
            &global_targets,
        )
        .unwrap(),
        2
    );

    let mut direction_source = row("902 7th Street North", Some("Suite 4"), 1);
    let direction_target = row("902 N 7TH ST", Some("Ste 4"), 6);
    direction_source.raw.first = None;
    assert_eq!(
        match_pair(&direction_source, &direction_target)
            .unwrap()
            .effective_first,
        ""
    );
    let invalid_config = temporary.path().join("invalid.json");
    std::fs::write(&invalid_config, "{").unwrap();
    assert!(derive_evidence_alias_candidates(
        &invalid_config,
        &invalid_config,
        &invalid_config,
        &invalid_config,
        &output_path,
        &output_path,
    )
    .is_err());
}

#[test]
fn native_matcher_covers_the_five_reviewed_exact_rules() {
    let cases = [
        (
            "4007 Clarksville Pike 301",
            None,
            "4007 Clarksville Pike",
            Some("Suite 301"),
            "candidate_confirmed_bare_unit",
        ),
        (
            "3009 North Ballas Road Suite: 141A",
            None,
            "3009 N Ballas Rd",
            Some("Ste 141A"),
            "unit_designator_punctuation",
        ),
        (
            "7108 DE SOTO AVE",
            Some("105 C"),
            "7108 De Soto Avenue unit 105c",
            None,
            "candidate_confirmed_spaced_unit",
        ),
        (
            "902 7th Street North",
            Some("Suite 4"),
            "902 N 7TH ST",
            Some("Ste 4"),
            "direction_relocation",
        ),
        (
            "15101 Glenwood",
            Some("Suite B"),
            "15101 Glenwood Ave",
            Some("Ste B"),
            "terminal_suffix_omission",
        ),
    ];
    for (source_first, source_second, target_first, target_second, expected) in cases {
        let source = row(source_first, source_second, 1);
        let target = row(target_first, target_second, 6);
        assert_eq!(match_pair(&source, &target).unwrap().rule, expected);
    }
    assert_eq!(
        match_pair(
            &row("3009 North Ballas Road", Some("Suite: 141A"), 1),
            &row("3009 N Ballas Rd", Some("Ste 141A"), 6),
        )
        .unwrap()
        .rule,
        "unit_designator_punctuation"
    );
}

#[test]
fn native_matcher_preserves_route_and_unit_negatives() {
    assert!(match_pair(
        &row("123 US Highway 64", None, 1),
        &row("123 US Highway", Some("Suite 64"), 6),
    )
    .is_none());
    assert!(match_pair(
        &row("902 7th Street North", Some("Suite 4"), 1),
        &row("902 N 7TH ST", Some("Ste 5"), 6),
    )
    .is_none());
}

#[test]
fn bare_unit_does_not_treat_tab_second_line_as_postgres_blank() {
    assert!(match_pair(
        &row("4007 Clarksville Pike 301", Some("\t"), 1),
        &row("4007 Clarksville Pike", Some("Suite 301"), 6),
    )
    .is_none());
}

#[test]
fn bare_unit_does_not_treat_nbsp_second_line_as_postgres_blank() {
    assert!(match_pair(
        &row("4007 Clarksville Pike 301", Some("\u{00a0}"), 1),
        &row("4007 Clarksville Pike", Some("Suite 301"), 6),
    )
    .is_none());
}

#[test]
fn completion_keeps_a_lone_suffix_after_direction_removal() {
    let empty = address_evidence_features(None, None);
    assert!(empty.street.is_none());
    assert!(empty.suffix.is_none());
    assert!(empty.completion.is_none());
    assert_eq!(
        address_evidence_features(Some("N St"), None)
            .completion
            .as_deref(),
        Some("st")
    );
    assert_eq!(
        address_evidence_features(Some("St"), None)
            .completion
            .as_deref(),
        Some("st")
    );
}

#[test]
fn marker_set_excludes_postgres_null_completion_bucket() {
    assert_eq!(
        address_evidence_features(None, Some("Main"))
            .completion
            .as_deref(),
        Some("main")
    );
    assert_eq!(
        address_evidence_features(Some(""), Some("Main"))
            .completion
            .as_deref(),
        Some("main")
    );

    let mut rows = vec![row("N Main St", None, 1), row("S Main Ave", None, 1)];
    for row in &mut rows {
        row.marker_features.completion = None;
    }
    assert_eq!(rows[0].marker_features.completion, None);
    assert_eq!(rows[1].marker_features.completion, None);
    let markers = marker_set(&rows, &[0, 1], None);

    assert_eq!(markers.direction_count, 0);
    assert_eq!(markers.suffix_count, 0);
    assert!(markers.direction.is_none());
    assert!(markers.suffix.is_none());
}

#[test]
fn active_skip_count_does_not_require_street_precision() {
    let mut archived = row("10 Main St", None, 1);
    archived.precision = Some("premise".to_string());
    archived.visible_valid = false;
    let key = archived.key.clone().unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let memberships_path = temporary.path().join("memberships.copy");
    std::fs::write(&memberships_path, format!("1234567893\t{key}\n")).unwrap();
    let key_index = HashMap::from([(key.clone(), 0)]);
    let active_sources = HashMap::from([(key, None)]);

    let memberships = load_memberships(
        &memberships_path,
        &[archived],
        &key_index,
        &RunConfig {
            run_id: "00000000-0000-0000-0000-000000000001".to_string(),
            state_code: None,
            zip_prefix: None,
            retry_shadow_run_id: None,
        },
        &active_sources,
    )
    .unwrap();

    assert_eq!(memberships.source_count, 0);
    assert_eq!(memberships.active_skipped, 1);
    assert!(memberships.rows.is_empty());
}
