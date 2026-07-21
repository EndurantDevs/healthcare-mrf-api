// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};

    const FIXTURE: &[u8] = br#"[
  {"id":1,"text":"brace } and bracket [ inside a string"},
  {"id":2,"nested":{"items":[1,2,{"ok":true}]}},
  {"id":3,"huge":1234567890123456789012345678901234567890},
  {"id":4,"escaped":"quote: \" and slash: \\"},
  {"id":5,"empty":null},
  {"id":6,"unicode":"provider-\u2603"},
  {"id":7,"array":[{"a":1},{"b":2}]},
  {"id":8,"active":false}
]"#;

    struct Fixture {
        _directory: tempfile::TempDir,
        source: PathBuf,
        output: PathBuf,
        sha256: String,
        byte_count: u64,
    }

    impl Fixture {
        fn new(payload: &[u8]) -> Self {
            let directory = tempfile::tempdir().expect("temporary fixture root");
            let source = directory.path().join("source.json");
            let output = directory.path().join("retained");
            fs::write(&source, payload).expect("write source fixture");
            fs::create_dir(&output).expect("create retained root");
            Self {
                _directory: directory,
                source,
                output,
                sha256: sha256_hex(&Sha256::digest(payload)),
                byte_count: payload.len() as u64,
            }
        }

        fn request(&self, range_count: usize) -> UHCRetainRequest {
            UHCRetainRequest {
                source_path: self.source.clone(),
                output_root: self.output.clone(),
                expected_sha256: self.sha256.clone(),
                expected_byte_count: self.byte_count,
                range_count,
            }
        }

        fn raw_path(&self) -> PathBuf {
            self.output.join(raw_file_name(&self.sha256))
        }

        fn manifest_path(&self, range_count: usize) -> PathBuf {
            self.output
                .join(manifest_file_name(&self.sha256, range_count))
        }
    }

    fn retained_files(root: &Path) -> Vec<String> {
        let mut files = fs::read_dir(root)
            .expect("read retained root")
            .map(|entry| {
                entry
                    .expect("retained entry")
                    .file_name()
                    .into_string()
                    .expect("UTF-8 retained file name")
            })
            .collect::<Vec<_>>();
        files.sort();
        files
    }

    #[test]
    fn retains_one_raw_artifact_and_deterministic_logical_ranges() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
        assert!(!summary.raw_reused);
        assert!(!summary.manifest_reused);
        assert_eq!(summary.record_count, 8);
        assert_eq!(summary.range_count, 4);

        let raw = fs::read(fixture.raw_path()).expect("read retained raw");
        assert_eq!(raw, FIXTURE);
        let manifest_bytes = fs::read(fixture.manifest_path(4)).expect("read manifest");
        let manifest = parse_strict_manifest(&manifest_bytes).expect("strict manifest");
        assert_eq!(manifest.raw_artifact.record_count, 8);
        assert_eq!(manifest.ranges.len(), 4);
        assert_eq!(
            manifest
                .ranges
                .iter()
                .map(|range| range.record_count)
                .collect::<Vec<_>>(),
            vec![2, 2, 2, 2]
        );

        for range in &manifest.ranges {
            let bytes = &raw[range.raw_byte_start as usize..range.raw_byte_end as usize];
            assert_eq!(sha256_hex(&Sha256::digest(bytes)), range.raw_sha256);
            let mut framer =
                JsonObjectFramer::fragment(range.raw_byte_start, MAX_PROVIDER_RECORD_BYTES);
            let mut canonical = Vec::new();
            framer
                .feed(bytes, |record, _, _| {
                    canonical.extend(
                        record
                            .iter()
                            .copied()
                            .filter(|byte| !matches!(*byte, b'\r' | b'\n')),
                    );
                    canonical.push(b'\n');
                    Ok(())
                })
                .expect("frame retained range");
            framer.finish().expect("complete retained range");
            assert_eq!(
                sha256_hex(&Sha256::digest(&canonical)),
                range.canonical_sha256
            );
            assert_eq!(canonical.len() as u64, range.canonical_byte_count);
        }

        let files = retained_files(&fixture.output);
        assert_eq!(files.len(), 2);
        assert!(files.iter().any(|name| name.ends_with(".json")));
        assert!(!files.iter().any(|name| name.ends_with(".ndjson")));
        assert!(!files.iter().any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn rejects_trailing_comma_and_cleans_every_temporary_file() {
        let fixture = Fixture::new(br#"[{"id":1},{"id":2},{"id":3},{"id":4},]"#);
        let error = retain_uhc_artifact(&fixture.request(4)).expect_err("trailing comma rejected");
        assert!(error.to_string().contains("JSON"));
        assert!(retained_files(&fixture.output).is_empty());
    }

    #[test]
    fn strict_json_rejects_duplicates_extensions_and_invalid_utf8() {
        for invalid in [
            br#"{"a":1,"a":2}"#.as_slice(),
            br#"{"nested":{"a":1,"a":2}}"#.as_slice(),
            br#"{"number":NaN}"#.as_slice(),
            br#"{"number":Infinity}"#.as_slice(),
            br#"{"number":-Infinity}"#.as_slice(),
            br#"[]"#.as_slice(),
            br#"{"ok":true} trailing"#.as_slice(),
            b"{\"bad\":\"\xff\"}".as_slice(),
        ] {
            assert!(
                validate_strict_json_object(invalid).is_err(),
                "accepted invalid JSON: {:?}",
                String::from_utf8_lossy(invalid)
            );
        }
        validate_strict_json_object(
            br#"{"huge":12345678901234567890123456789012345678901234567890}"#,
        )
        .expect("arbitrary precision JSON number remains valid");
        validate_strict_json_object(
            br#"{"negative":-42,"decimal":3.125,"null":null,"escaped":"provider-\u2603"}"#,
        )
        .expect("every supported JSON scalar remains valid");
    }

    #[test]
    fn cli_rejects_incomplete_and_non_numeric_arguments() {
        assert_eq!(
            run_uhc_retain_cli(&[])
                .expect_err("missing arguments")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        let mut arguments = vec![
            "source.json".to_owned(),
            "retained".to_owned(),
            "0".repeat(64),
            "not-a-byte-count".to_owned(),
            "4".to_owned(),
        ];
        assert_eq!(
            run_uhc_retain_cli(&arguments)
                .expect_err("non-numeric byte count")
                .kind(),
            io::ErrorKind::InvalidInput
        );

        arguments[3] = "1".to_owned();
        arguments[4] = "not-a-range-count".to_owned();
        assert_eq!(
            run_uhc_retain_cli(&arguments)
                .expect_err("non-numeric range count")
                .kind(),
            io::ErrorKind::InvalidInput
        );
        assert!(some_or_invalid_data::<u64>(None, "missing proof").is_err());
        for build_id in ["", "control\nbyte", "provider-☃"] {
            assert!(validate_build_id(build_id).is_err());
        }
        assert!(validate_build_id(&"x".repeat(MAX_BUILD_ID_BYTES + 1)).is_err());
        assert!(checked_i64_domain(i64::MAX as u64 + 1, "byte_count").is_err());

        let fixture = Fixture::new(FIXTURE);
        let mut request = fixture.request(4);
        request.expected_byte_count = 0;
        assert!(request.validate().is_err());
        request.expected_byte_count = i64::MAX as u64 + 1;
        assert!(request.validate().is_err());

        let source_file = File::open(&fixture.source).expect("open source fixture");
        let source_identity = FileIdentity::from_file(&source_file).expect("source identity");
        assert!(require_stable_regular_file(
            &source_file,
            source_identity,
            fixture.byte_count + 1,
            "source",
        )
        .is_err());
        let directory_file = File::open(&fixture.output).expect("open retained directory");
        let directory_identity =
            FileIdentity::from_file(&directory_file).expect("directory identity");
        assert!(require_stable_regular_file(
            &directory_file,
            directory_identity,
            0,
            "source",
        )
        .is_err());
        assert!(open_regular_nofollow(&fixture.output, "source").is_err());
        let relative_root = RootDirectory::open(Path::new(".")).expect("open relative root");
        assert!(relative_root.path.is_absolute());
    }

    #[test]
    fn framing_is_chunk_independent_and_enforces_object_size() {
        let payload = br#"[{"value":"escaped \\\" quote and } brace"},{"nested":[{}]}]"#;
        let mut records = Vec::new();
        let mut framer = JsonObjectFramer::array(true, 1024);
        for byte in payload {
            framer
                .feed(std::slice::from_ref(byte), |record, start, end| {
                    records.push((record.to_vec(), start, end));
                    Ok(())
                })
                .expect("single-byte framing");
        }
        framer.finish().expect("complete single-byte framing");
        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].0,
            br#"{"value":"escaped \\\" quote and } brace"}"#
        );

        let mut limited = JsonObjectFramer::array(true, 4);
        assert!(limited
            .feed(br#"[{"too":"large"}]"#, |_, _, _| Ok(()))
            .is_err());
    }

    #[test]
    fn trailing_padding_and_large_range_fanout_use_verified_fallback() {
        let mut padded = br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#.to_vec();
        padded.extend(std::iter::repeat_n(b' ', 16 * 1024));
        let padded_fixture = Fixture::new(&padded);
        let padded_summary =
            retain_uhc_artifact(&padded_fixture.request(4)).expect("retain padded fixture");
        assert_eq!(padded_summary.record_count, 4);
        assert!(padded_summary.timings_seconds.range_verification > 0.0);
        let padded_manifest = parse_strict_manifest(
            &fs::read(padded_fixture.manifest_path(4)).expect("padded manifest"),
        )
        .expect("parse padded manifest");
        assert_eq!(padded_manifest.ranges.len(), 4);
        assert!(padded_manifest
            .ranges
            .iter()
            .all(|range| range.record_count == 1));

        let records = (0..9)
            .map(|ordinal| format!(r#"{{"id":{ordinal}}}"#))
            .collect::<Vec<_>>()
            .join(",");
        let wide_fixture = Fixture::new(format!("[{records}]").as_bytes());
        let wide_summary =
            retain_uhc_artifact(&wide_fixture.request(9)).expect("retain nine ranges");
        assert_eq!(wide_summary.range_count, 9);
        assert!(wide_summary.timings_seconds.range_verification > 0.0);
    }

    #[test]
    fn rejects_non_objects_trailing_content_and_impossible_range_counts() {
        for payload in [
            br#"[1,2,3,4]"#.as_slice(),
            br#"[{"id":1},{"id":2},{"id":3},{"id":4}] trailing"#.as_slice(),
        ] {
            let fixture = Fixture::new(payload);
            assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
            assert!(retained_files(&fixture.output).is_empty());
        }

        let fixture = Fixture::new(br#"[{"id":1},{"id":2},{"id":3},{"id":4}]"#);
        assert!(retain_uhc_artifact(&fixture.request(3)).is_err());
        assert!(retain_uhc_artifact(&fixture.request(5)).is_err());
        let mut unsafe_request = fixture.request(4);
        unsafe_request.expected_sha256 = "A".repeat(64);
        assert!(retain_uhc_artifact(&unsafe_request).is_err());
    }

    #[test]
    fn exact_existing_artifacts_are_reused_without_the_source() {
        let fixture = Fixture::new(FIXTURE);
        let first = retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        assert!(!first.raw_reused);
        fs::remove_file(&fixture.source).expect("remove original source");
        let second = retain_uhc_artifact(&fixture.request(4)).expect("reuse retained artifacts");
        assert!(second.raw_reused);
        assert!(second.manifest_reused);
    }

    #[test]
    fn preserves_a_valid_manifest_from_a_different_producer_build() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        let manifest_path = fixture.manifest_path(4);
        let mut manifest = parse_strict_manifest(&fs::read(&manifest_path).expect("manifest"))
            .expect("parse manifest");
        manifest.producer_build_id = "ptg2_scanner/preseeded-test-producer".to_owned();
        let preseeded = encode_manifest(&manifest).expect("encode preseeded manifest");
        fs::write(&manifest_path, &preseeded).expect("preseed existing valid manifest");
        let before_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("preseeded manifest metadata"),
        );

        let summary = retain_uhc_artifact(&fixture.request(4)).expect("reuse preseeded manifest");
        let after_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("reused manifest metadata"),
        );
        assert!(summary.raw_reused);
        assert!(summary.manifest_reused);
        assert_eq!(
            summary.producer_build_id,
            "ptg2_scanner/preseeded-test-producer"
        );
        assert_eq!(summary.verifier_build_id, current_build_id());
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&manifest_path).expect("reused bytes"), preseeded);
    }

    #[test]
    fn mismatching_existing_manifest_fails_closed_without_replacement() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("initial retain");
        let manifest_path = fixture.manifest_path(4);
        let mut manifest = parse_strict_manifest(&fs::read(&manifest_path).expect("manifest"))
            .expect("parse manifest");
        manifest.ranges[0].canonical_sha256 = "0".repeat(64);
        let poisoned = encode_manifest(&manifest).expect("encode poisoned manifest");
        fs::write(&manifest_path, &poisoned).expect("poison existing manifest");
        let before_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("poisoned manifest metadata"),
        );

        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        let after_identity = FileIdentity::from_metadata(
            &fs::metadata(&manifest_path).expect("unchanged manifest metadata"),
        );
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&manifest_path).expect("unchanged bytes"), poisoned);
        assert!(!retained_files(&fixture.output)
            .iter()
            .any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn mismatching_existing_raw_fails_closed_without_replacement() {
        let fixture = Fixture::new(FIXTURE);
        let raw_path = fixture.raw_path();
        let mut poisoned = FIXTURE.to_vec();
        let digit = poisoned
            .iter_mut()
            .find(|byte| **byte == b'8')
            .expect("fixture contains a digit to poison");
        *digit = b'9';
        fs::write(&raw_path, &poisoned).expect("preseed mismatching raw");
        let before_identity =
            FileIdentity::from_metadata(&fs::metadata(&raw_path).expect("raw metadata"));

        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        let after_identity =
            FileIdentity::from_metadata(&fs::metadata(&raw_path).expect("raw metadata"));
        assert!(same_inode(before_identity, after_identity));
        assert_eq!(fs::read(&raw_path).expect("unchanged raw"), poisoned);
        assert_eq!(
            retained_files(&fixture.output),
            vec![raw_file_name(&fixture.sha256)]
        );
    }

    #[test]
    fn rejects_symlink_roots_sources_and_final_artifacts() {
        let fixture = Fixture::new(FIXTURE);
        let linked_root = fixture._directory.path().join("linked-root");
        symlink(&fixture.output, &linked_root).expect("link output root");
        let mut linked_root_request = fixture.request(4);
        linked_root_request.output_root = linked_root;
        assert!(retain_uhc_artifact(&linked_root_request).is_err());

        let source_target = fixture._directory.path().join("source-target.json");
        fs::write(&source_target, FIXTURE).expect("write source target");
        let linked_source = fixture._directory.path().join("linked-source.json");
        symlink(&source_target, &linked_source).expect("link source");
        let mut linked_source_request = fixture.request(4);
        linked_source_request.source_path = linked_source;
        assert!(retain_uhc_artifact(&linked_source_request).is_err());

        let raw_link_target = fixture._directory.path().join("raw-link-target.json");
        fs::write(&raw_link_target, FIXTURE).expect("write raw link target");
        symlink(&raw_link_target, fixture.raw_path()).expect("link final raw");
        assert!(retain_uhc_artifact(&fixture.request(4)).is_err());
        assert!(fs::symlink_metadata(fixture.raw_path())
            .expect("raw symlink metadata")
            .file_type()
            .is_symlink());
    }

    #[test]
    fn rejects_aliased_or_group_writable_retained_artifacts() {
        let linked_fixture = Fixture::new(FIXTURE);
        fs::write(linked_fixture.raw_path(), FIXTURE).expect("preseed raw artifact");
        let alias = linked_fixture._directory.path().join("raw-alias.json");
        fs::hard_link(linked_fixture.raw_path(), &alias).expect("hard-link retained raw");
        assert!(retain_uhc_artifact(&linked_fixture.request(4)).is_err());
        assert_eq!(fs::metadata(&alias).expect("alias metadata").nlink(), 2);

        let writable_fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&writable_fixture.request(4)).expect("initial retain");
        let manifest_path = writable_fixture.manifest_path(4);
        let mut permissions = fs::metadata(&manifest_path)
            .expect("manifest metadata")
            .permissions();
        permissions.set_mode(0o620);
        fs::set_permissions(&manifest_path, permissions).expect("make manifest group writable");
        assert!(retain_uhc_artifact(&writable_fixture.request(4)).is_err());
        assert_eq!(
            fs::metadata(&manifest_path)
                .expect("manifest metadata")
                .mode()
                & 0o777,
            0o620
        );
    }

    #[test]
    fn concurrent_admission_never_clobbers_final_artifacts() {
        let fixture = Fixture::new(FIXTURE);
        let request = Arc::new(fixture.request(4));
        let first_request = Arc::clone(&request);
        let second_request = Arc::clone(&request);
        let first = thread::spawn(move || retain_uhc_artifact(&first_request));
        let second = thread::spawn(move || retain_uhc_artifact(&second_request));
        let first = first
            .join()
            .expect("first admission thread")
            .expect("first admission");
        let second = second
            .join()
            .expect("second admission thread")
            .expect("second admission");
        assert_eq!(first.manifest_sha256, second.manifest_sha256);
        assert!(first.raw_reused || second.raw_reused);
        assert!(first.manifest_reused || second.manifest_reused);
        assert_eq!(retained_files(&fixture.output).len(), 2);
        assert!(!retained_files(&fixture.output)
            .iter()
            .any(|name| name.ends_with(".partial")));
    }

    #[test]
    fn range_set_hash_has_a_stable_golden_value() {
        let fixture = Fixture::new(FIXTURE);
        retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
        let manifest = parse_strict_manifest(
            &fs::read(fixture.manifest_path(4)).expect("read golden manifest"),
        )
        .expect("parse golden manifest");
        assert_eq!(
            manifest.range_set_sha256,
            "0cd8eb9ccef2be7d4abb442b48d51f56e76fb11670ce662568cba4dc6ee15bb8"
        );
    }
}
