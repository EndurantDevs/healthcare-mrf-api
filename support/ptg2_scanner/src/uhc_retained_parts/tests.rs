// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::{symlink, PermissionsExt};
    use std::sync::atomic::AtomicBool;

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

    fn descriptor_device_inode(descriptor: RawFd) -> io::Result<(libc::dev_t, libc::ino_t)> {
        let mut status = std::mem::MaybeUninit::<libc::stat>::uninit();
        if unsafe { libc::fstat(descriptor, status.as_mut_ptr()) } == -1 {
            return Err(io::Error::last_os_error());
        }
        let status = unsafe { status.assume_init() };
        Ok((status.st_dev, status.st_ino))
    }

    fn assert_descriptor_closed(
        descriptor: RawFd,
        original_device_inode: (libc::dev_t, libc::ino_t),
    ) {
        match descriptor_device_inode(descriptor) {
            Ok(device_inode) => assert_ne!(
                device_inode, original_device_inode,
                "the temporary file descriptor remained open",
            ),
            Err(error) => assert!(
                matches!(error.raw_os_error(), Some(libc::EBADF)),
                "unexpected closed descriptor error: {error}",
            ),
        }
    }

    fn assert_no_open_descriptor_for_identity(identity: (libc::dev_t, libc::ino_t)) {
        for entry in fs::read_dir("/dev/fd").expect("read process descriptor directory") {
            let Ok(descriptor) = entry
                .expect("read process descriptor entry")
                .file_name()
                .to_string_lossy()
                .parse::<RawFd>()
            else {
                continue;
            };
            if let Ok(observed_identity) = descriptor_device_inode(descriptor) {
                assert_ne!(
                    observed_identity, identity,
                    "a temporary-file descriptor clone remained open",
                );
            }
        }
    }

    fn probe_close_before_unlink(
        temporary: &mut RootTemporaryFile,
        temporary_path: PathBuf,
    ) -> Arc<AtomicBool> {
        let temporary_file = temporary.file();
        let descriptor = temporary_file.as_raw_fd();
        let original_device_inode =
            descriptor_device_inode(descriptor).expect("temporary descriptor identity");
        let observed = Arc::new(AtomicBool::new(false));
        let probe_observed = Arc::clone(&observed);
        temporary.pre_unlink_probe = Some(Box::new(move || {
            assert_descriptor_closed(descriptor, original_device_inode);
            assert_no_open_descriptor_for_identity(original_device_inode);
            assert!(temporary_path.exists());
            probe_observed.store(true, Ordering::SeqCst);
        }));
        observed
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
    fn closes_temporary_descriptors_before_publication_cleanup_returns() {
        let fixture = Fixture::new(FIXTURE);
        let root = RootDirectory::open(&fixture.output).expect("open retained root");

        let mut published = root.create_temporary("published").expect("temporary file");
        published
            .file_mut()
            .write_all(b"published")
            .expect("write publication candidate");
        published
            .file()
            .sync_all()
            .expect("sync publication candidate");
        let published_name = published.name.clone();
        let published_path = fixture.output.join(&published_name);
        let published_probe = probe_close_before_unlink(&mut published, published_path.clone());
        assert!(published
            .publish_noclobber("published.json")
            .expect("publish candidate"));
        assert!(published_probe.load(Ordering::SeqCst));
        assert!(!published_path.exists());
        assert_eq!(
            FileIdentity::from_metadata(
                &fs::metadata(fixture.output.join("published.json"))
                    .expect("published file metadata"),
            )
            .link_count,
            1,
        );

        fs::write(fixture.output.join("incumbent.json"), b"incumbent")
            .expect("write incumbent");
        let mut collision = root.create_temporary("collision").expect("temporary file");
        collision
            .file_mut()
            .write_all(b"candidate")
            .expect("write collision candidate");
        collision
            .file()
            .sync_all()
            .expect("sync collision candidate");
        let collision_name = collision.name.clone();
        let collision_path = fixture.output.join(&collision_name);
        let collision_probe = probe_close_before_unlink(&mut collision, collision_path.clone());
        assert!(!collision
            .publish_noclobber("incumbent.json")
            .expect("resolve publication collision"));
        assert!(collision_probe.load(Ordering::SeqCst));
        assert!(!collision_path.exists());
        assert_eq!(
            fs::read(fixture.output.join("incumbent.json")).expect("read incumbent"),
            b"incumbent",
        );

        let mut link_failure = root.create_temporary("link-failure").expect("temporary file");
        let link_failure_path = fixture.output.join(&link_failure.name);
        let link_failure_probe =
            probe_close_before_unlink(&mut link_failure, link_failure_path.clone());
        assert!(link_failure.publish_noclobber("missing/child").is_err());
        assert!(link_failure_probe.load(Ordering::SeqCst));
        assert!(!link_failure_path.exists());

        let mut abandoned = root.create_temporary("abandoned").expect("temporary file");
        let abandoned_path = fixture.output.join(&abandoned.name);
        let abandoned_probe =
            probe_close_before_unlink(&mut abandoned, abandoned_path.clone());
        drop(abandoned);
        assert!(abandoned_probe.load(Ordering::SeqCst));
        assert!(!abandoned_path.exists());

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
            br#"{"array":[}"#.as_slice(),
            br#"{"missing":}"#.as_slice(),
            br#"{"unfinished":1,"#.as_slice(),
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
    fn framing_overflow_and_mismatched_delimiters_fail_closed() {
        let mut record_overflow = JsonObjectFramer::fragment(0, usize::MAX);
        record_overflow.in_record = true;
        record_overflow.record_size = usize::MAX;
        assert!(record_overflow
            .append_record_byte(b'x')
            .unwrap_err()
            .to_string()
            .contains("byte count overflowed"));

        let mut offset_overflow = JsonObjectFramer::array(true, 1024);
        offset_overflow.absolute_offset = u64::MAX;
        assert!(offset_overflow
            .feed(b" ", |_, _, _| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("source offset overflowed"));

        let mut leading_space = JsonObjectFramer::array(true, 1024);
        leading_space
            .feed(b" \n[]", |_, _, _| Ok(()))
            .expect("leading JSON whitespace");
        leading_space.finish().expect("empty array is complete");

        let mut depth_overflow = JsonObjectFramer::fragment(0, 1024);
        depth_overflow.in_record = true;
        depth_overflow.depth = i64::MAX;
        assert!(depth_overflow
            .feed(b"{", |_, _, _| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("nesting overflowed"));

        let mut negative_depth = JsonObjectFramer::fragment(0, 1024);
        negative_depth.in_record = true;
        negative_depth.depth = 0;
        assert!(negative_depth
            .feed(b"}", |_, _, _| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("frame is invalid"));

        let mut non_object_close = JsonObjectFramer::fragment(0, 1024);
        non_object_close.in_record = true;
        non_object_close.depth = 1;
        assert!(non_object_close
            .feed(b"]", |_, _, _| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("must be JSON objects"));
    }

    #[test]
    fn descriptor_roots_and_partition_arithmetic_reject_drift() {
        let fixture = Fixture::new(FIXTURE);
        assert!(RootDirectory::open(&fixture.source).is_err());
        assert!(c_string("bad\0name", "test value").is_err());

        let root = RootDirectory::open(&fixture.output).expect("open retained root");
        let mut wrong_descriptor_identity = root.identity;
        wrong_descriptor_identity.inode = wrong_descriptor_identity.inode.wrapping_add(1);
        let drifted_descriptor = RootDirectory {
            supplied_path: root.supplied_path.clone(),
            path: root.path.clone(),
            directory: root.directory.try_clone().expect("clone root descriptor"),
            identity: wrong_descriptor_identity,
        };
        assert!(drifted_descriptor
            .verify_path_identity()
            .unwrap_err()
            .to_string()
            .contains("identity changed"));

        let other = tempfile::tempdir().expect("alternate retained root");
        let drifted_path = RootDirectory {
            supplied_path: other.path().to_path_buf(),
            path: other.path().to_path_buf(),
            directory: root.directory.try_clone().expect("clone root descriptor"),
            identity: root.identity,
        };
        assert!(drifted_path
            .verify_path_identity()
            .unwrap_err()
            .to_string()
            .contains("path changed"));

        assert!(ceil_partition_boundary(2, usize::MAX, 1)
            .unwrap_err()
            .to_string()
            .contains("exceeds u64"));
        let mut offsets = tempfile::tempfile().expect("partition offsets");
        for value in [0u64, 2, 3, 5, 6, 8, 9, 11] {
            offsets.write_all(&value.to_be_bytes()).expect("write offset");
        }
        assert!(build_range_boundaries(&offsets, 4, 5)
            .unwrap_err()
            .to_string()
            .contains("records for 5 ranges"));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn forced_identity_rejects_failed_and_incomplete_statx_results() {
        let incomplete = unsafe { std::mem::zeroed::<libc::statx>() };
        assert!(FileIdentity::from_statx_result(-1, &incomplete).is_err());
        assert!(FileIdentity::from_statx_result(0, &incomplete)
            .unwrap_err()
            .to_string()
            .contains("identity is incomplete"));
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
    fn accepts_retained_file_after_delayed_publication_link_settles() {
        assert_eq!(PUBLICATION_LINK_RETRIES, 1_501);
        assert_eq!(PUBLICATION_LINK_RETRY_DELAY, Duration::from_millis(50));
        let fixture = Fixture::new(FIXTURE);
        let raw_path = fixture.raw_path();
        fs::write(&raw_path, FIXTURE).expect("preseed raw artifact");
        let alias = fixture._directory.path().join("transient-publication-link.json");
        fs::hard_link(&raw_path, &alias).expect("link retained raw during publication");
        let alias_for_removal = alias.clone();
        let remover = thread::spawn(move || {
            // Exercise convergence beyond the former five-second retry budget.
            thread::sleep(Duration::from_millis(16_000));
            fs::remove_file(alias_for_removal).expect("settle publication link");
        });

        let root = RootDirectory::open(&fixture.output).expect("open retained root");
        let raw_name = raw_file_name(&fixture.sha256);
        let retained_file = root
            .open_existing_regular(&raw_name)
            .expect("open settled retained file")
            .expect("retained file exists");
        remover.join().expect("publication link remover");

        assert_eq!(
            FileIdentity::from_file(&retained_file)
                .expect("settled retained identity")
                .link_count,
            1
        );
        assert!(!alias.exists());
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

    include!("publication_tests.rs");
    include!("../../tests/unit/retained_raw_publication.rs");
    include!("publication_margin_tests.rs");

    #[test]
    fn verified_replay_visits_descriptor_bound_records_without_whole_file_read() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain replay fixture");
        let manifest_bytes = fs::read(&summary.manifest_path).expect("read replay manifest");
        let manifest = parse_strict_manifest(&manifest_bytes).expect("parse replay manifest");
        let source = open_verified_uhc_replay(&UHCVerifiedReplayRequest {
            raw_path: PathBuf::from(&summary.raw_artifact_path),
            manifest_path: PathBuf::from(&summary.manifest_path),
            expected_artifact_sha256: summary.raw_artifact_sha256.clone(),
            expected_artifact_byte_count: summary.raw_artifact_byte_count,
            expected_manifest_sha256: summary.manifest_sha256.clone(),
            expected_range_set_sha256: manifest.range_set_sha256.clone(),
            expected_record_count: summary.record_count,
            expected_range_count: summary.range_count as usize,
        })
        .expect("open verified replay");

        let mut ordinals = Vec::new();
        for ordinal in 0..source.manifest().ranges.len() {
            source
                .visit_verified_range_records(ordinal, |record_ordinal, record| {
                    let value: serde_json::Value =
                        serde_json::from_slice(record).expect("decode replayed object");
                    assert!(value.is_object());
                    ordinals.push(record_ordinal);
                    Ok(())
                })
                .expect("visit verified range");
        }
        assert_eq!(ordinals, (0..summary.record_count).collect::<Vec<_>>());
        assert_eq!(source.manifest_sha256(), summary.manifest_sha256);
    }

    #[test]
    fn verified_replay_rejects_path_replacement_after_open() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain replay fixture");
        let manifest_bytes = fs::read(&summary.manifest_path).expect("read replay manifest");
        let manifest = parse_strict_manifest(&manifest_bytes).expect("parse replay manifest");
        let raw_path = PathBuf::from(&summary.raw_artifact_path);
        let source = open_verified_uhc_replay(&UHCVerifiedReplayRequest {
            raw_path: raw_path.clone(),
            manifest_path: PathBuf::from(&summary.manifest_path),
            expected_artifact_sha256: summary.raw_artifact_sha256,
            expected_artifact_byte_count: summary.raw_artifact_byte_count,
            expected_manifest_sha256: summary.manifest_sha256,
            expected_range_set_sha256: manifest.range_set_sha256,
            expected_record_count: summary.record_count,
            expected_range_count: summary.range_count as usize,
        })
        .expect("open verified replay");
        let mut visitor = |_ordinal, _record: &[u8]| Ok(());
        source
            .visit_verified_range_records(0, &mut visitor)
            .expect("visit verified range before path replacement");
        let moved_path = raw_path.with_extension("moved");
        fs::rename(&raw_path, &moved_path).expect("move admitted raw path");
        fs::write(&raw_path, FIXTURE).expect("replace admitted raw path");

        let error = source
            .visit_verified_range_records(0, &mut visitor)
            .expect_err("replaced path must fail");
        assert!(error
            .to_string()
            .contains("identity or permissions changed"));
    }

    #[test]
    fn streaming_ranges_workers_and_verifiers_reject_internal_boundary_drift() {
        let mut record_end_overflow = StreamingRangeBoundary::new(0, u64::MAX, 0);
        assert!(record_end_overflow.add_record(1).is_err());
        let mut record_count_overflow = StreamingRangeBoundary::new(0, 0, 0);
        record_count_overflow.record_count = u64::MAX;
        assert!(record_count_overflow.add_record(1).is_err());
        assert!(StreamingRangeBoundary::new(0, 0, 0).finish().is_err());

        let worker_input = Arc::new(tempfile::tempfile().expect("worker input"));
        let (sender, receiver) = sync_channel(1);
        drop(sender);
        assert!(run_range_worker(Arc::clone(&worker_input), receiver)
            .expect("closed worker")
            .is_none());

        let (sender, receiver) = sync_channel(1);
        sender
            .send(RangeWorkerMessage::Finish(RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 0,
                raw_byte_end: 1,
                record_start: 0,
                record_end: 1,
            }))
            .expect("send incomplete boundary");
        assert!(run_range_worker(Arc::clone(&worker_input), receiver)
            .unwrap_err()
            .to_string()
            .contains("proof is incomplete"));

        let (sender, receiver) = sync_channel(1);
        sender
            .send(RangeWorkerMessage::Finish(RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 0,
                raw_byte_end: 1,
                record_start: 2,
                record_end: 1,
            }))
            .expect("send underflow boundary");
        assert!(run_range_worker(Arc::clone(&worker_input), receiver)
            .unwrap_err()
            .to_string()
            .contains("underflowed"));

        let mut workers =
            ConcurrentRangeWorkers::spawn(Arc::clone(&worker_input), 1).expect("spawn worker");
        assert!(workers.send_record(1, b"{}").is_err());
        workers.pending[0].byte_count = usize::MAX;
        assert!(workers.send_record(0, b"{}").is_err());
        workers.pending[0] = PendingRecordBatch::default();
        assert!(workers.flush(1).is_err());
        let (stopped_sender, stopped_receiver) = sync_channel(1);
        drop(stopped_receiver);
        workers.senders[0] = stopped_sender;
        workers.pending[0].records.push(b"{}".to_vec());
        assert!(workers
            .flush(0)
            .unwrap_err()
            .to_string()
            .contains("stopped unexpectedly"));
        drop(workers);

        let empty_offsets = tempfile::tempfile().expect("empty offsets");
        assert!(build_range_boundaries(&empty_offsets, u64::MAX, 4)
            .unwrap_err()
            .to_string()
            .contains("offset spool size overflowed"));

        let mut invalid_offsets = tempfile::tempfile().expect("invalid offsets");
        for value in [5u64, 4, 10, 20, 21, 30, 31, 40] {
            invalid_offsets
                .write_all(&value.to_be_bytes())
                .expect("write offset");
        }
        assert!(build_range_boundaries(&invalid_offsets, 4, 4)
            .unwrap_err()
            .to_string()
            .contains("invalid byte boundaries"));

        let mut overlapping_offsets = tempfile::tempfile().expect("overlap offsets");
        for value in [0u64, 10, 9, 20, 21, 30, 31, 40] {
            overlapping_offsets
                .write_all(&value.to_be_bytes())
                .expect("write overlap offset");
        }
        assert!(build_range_boundaries(&overlapping_offsets, 4, 4)
            .unwrap_err()
            .to_string()
            .contains("not ordered and disjoint"));

        let short_input = tempfile::tempfile().expect("short input");
        assert!(hash_raw_range(
            &short_input,
            RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 2,
                raw_byte_end: 1,
                record_start: 0,
                record_end: 1,
            },
        )
        .is_err());
        assert!(hash_raw_range(
            &short_input,
            RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 1,
                raw_byte_end: 1,
                record_start: 0,
                record_end: 1,
            },
        )
        .is_err());
        assert!(hash_raw_range(
            &short_input,
            RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 0,
                raw_byte_end: 1,
                record_start: 0,
                record_end: 1,
            },
        )
        .unwrap_err()
        .to_string()
        .contains("ended before"));

        let mut one_record = tempfile::tempfile().expect("one record");
        one_record.write_all(b"{}").expect("write record");
        assert!(verify_raw_range(
            &one_record,
            RawRangeBoundary {
                range_ordinal: 0,
                raw_byte_start: 0,
                raw_byte_end: 2,
                record_start: 0,
                record_end: 2,
            },
        )
        .unwrap_err()
        .to_string()
        .contains("record count changed"));

        let extra_fixture = Fixture::new(FIXTURE);
        let input = File::open(&extra_fixture.source).expect("open extra-byte fixture");
        let range_input = Arc::new(
            File::open(&extra_fixture.source).expect("open extra-byte range fixture"),
        );
        let mut offsets = tempfile::tempfile().expect("extra-byte offsets");
        assert!(scan_raw_and_build_ranges(
            &input,
            range_input,
            None,
            &mut offsets,
            &parse_sha256_hex(&extra_fixture.sha256).expect("fixture digest"),
            extra_fixture.byte_count - 1,
            4,
        )
        .unwrap_err()
        .to_string()
        .contains("exceeds its expected byte count"));
    }

    fn replay_request(
        summary: &UHCRetainSummary,
        manifest: &UHCRetainedManifest,
    ) -> UHCVerifiedReplayRequest {
        UHCVerifiedReplayRequest {
            raw_path: PathBuf::from(&summary.raw_artifact_path),
            manifest_path: PathBuf::from(&summary.manifest_path),
            expected_artifact_sha256: summary.raw_artifact_sha256.clone(),
            expected_artifact_byte_count: summary.raw_artifact_byte_count,
            expected_manifest_sha256: summary.manifest_sha256.clone(),
            expected_range_set_sha256: manifest.range_set_sha256.clone(),
            expected_record_count: summary.record_count,
            expected_range_count: summary.range_count as usize,
        }
    }

    #[test]
    fn replay_request_manifest_range_and_descriptor_proofs_fail_independently() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain replay fixture");
        let manifest = parse_strict_manifest(
            &fs::read(&summary.manifest_path).expect("read replay manifest"),
        )
        .expect("parse replay manifest");

        let mut invalid_counts = replay_request(&summary, &manifest);
        invalid_counts.expected_record_count = 0;
        assert!(open_verified_uhc_replay(&invalid_counts)
            .err()
            .expect("invalid replay counts")
            .to_string()
            .contains("expected counts are invalid"));

        let mut wrong_manifest_hash = replay_request(&summary, &manifest);
        wrong_manifest_hash.expected_manifest_sha256 = "0".repeat(64);
        assert!(open_verified_uhc_replay(&wrong_manifest_hash)
            .err()
            .expect("manifest digest mismatch")
            .to_string()
            .contains("manifest SHA-256 does not match"));

        let mut wrong_identity = replay_request(&summary, &manifest);
        wrong_identity.expected_record_count += 1;
        assert!(open_verified_uhc_replay(&wrong_identity)
            .err()
            .expect("manifest identity mismatch")
            .to_string()
            .contains("manifest identity does not match"));

        let mut changed_manifest = manifest.clone();
        changed_manifest.ranges[0].raw_sha256 = "f".repeat(64);
        let changed_bytes = encode_manifest(&changed_manifest).expect("encode changed manifest");
        let changed_path = fixture._directory.path().join("changed.manifest.json");
        fs::write(&changed_path, &changed_bytes).expect("write changed manifest");
        let mut permissions = fs::metadata(&changed_path)
            .expect("changed manifest metadata")
            .permissions();
        permissions.set_mode(0o400);
        fs::set_permissions(&changed_path, permissions).expect("seal changed manifest");
        let mut wrong_range_set = replay_request(&summary, &changed_manifest);
        wrong_range_set.manifest_path = changed_path;
        wrong_range_set.expected_manifest_sha256 =
            sha256_hex(&Sha256::digest(&changed_bytes));
        assert!(open_verified_uhc_replay(&wrong_range_set)
            .err()
            .expect("range-set mismatch")
            .to_string()
            .contains("range-set proof does not match"));

        let mut invalid_sequence_manifest = manifest.clone();
        invalid_sequence_manifest.ranges[0].record_count = 0;
        let invalid_sequence_bytes =
            encode_manifest(&invalid_sequence_manifest).expect("encode invalid sequence manifest");
        let invalid_sequence_path = fixture
            ._directory
            .path()
            .join("invalid-sequence.manifest.json");
        fs::write(&invalid_sequence_path, &invalid_sequence_bytes)
            .expect("write invalid sequence manifest");
        let mut invalid_sequence_permissions = fs::metadata(&invalid_sequence_path)
            .expect("invalid sequence manifest metadata")
            .permissions();
        invalid_sequence_permissions.set_mode(0o400);
        fs::set_permissions(&invalid_sequence_path, invalid_sequence_permissions)
            .expect("seal invalid sequence manifest");
        let mut invalid_sequence_request = replay_request(&summary, &invalid_sequence_manifest);
        invalid_sequence_request.manifest_path = invalid_sequence_path;
        invalid_sequence_request.expected_manifest_sha256 =
            sha256_hex(&Sha256::digest(&invalid_sequence_bytes));
        assert!(open_verified_uhc_replay(&invalid_sequence_request)
            .err()
            .expect("invalid range sequence")
            .to_string()
            .contains("invalid logical range"));

        let wrong_raw_path = fixture.output.join("wrong-identity.raw.json");
        fs::copy(&summary.raw_artifact_path, &wrong_raw_path).expect("copy wrong-identity raw");
        let mut wrong_raw_permissions = fs::metadata(&wrong_raw_path)
            .expect("wrong-identity raw metadata")
            .permissions();
        wrong_raw_permissions.set_mode(0o400);
        fs::set_permissions(&wrong_raw_path, wrong_raw_permissions)
            .expect("seal wrong-identity raw");
        let wrong_raw_digest = [0u8; SHA256_BYTES];
        let wrong_raw_sha256 = sha256_hex(&wrong_raw_digest);
        let mut wrong_raw_manifest = manifest.clone();
        wrong_raw_manifest.raw_artifact.file_name = "wrong-identity.raw.json".to_owned();
        wrong_raw_manifest.raw_artifact.sha256 = wrong_raw_sha256.clone();
        wrong_raw_manifest.range_set_sha256 = range_set_sha256(
            &wrong_raw_digest,
            summary.raw_artifact_byte_count,
            summary.record_count,
            &wrong_raw_manifest.ranges,
        )
        .expect("wrong-identity range-set proof");
        let wrong_raw_manifest_bytes =
            encode_manifest(&wrong_raw_manifest).expect("encode wrong-identity manifest");
        let wrong_raw_manifest_path = fixture
            ._directory
            .path()
            .join("wrong-identity.manifest.json");
        fs::write(&wrong_raw_manifest_path, &wrong_raw_manifest_bytes)
            .expect("write wrong-identity manifest");
        let mut wrong_manifest_permissions = fs::metadata(&wrong_raw_manifest_path)
            .expect("wrong-identity manifest metadata")
            .permissions();
        wrong_manifest_permissions.set_mode(0o400);
        fs::set_permissions(&wrong_raw_manifest_path, wrong_manifest_permissions)
            .expect("seal wrong-identity manifest");
        let wrong_raw_request = UHCVerifiedReplayRequest {
            raw_path: wrong_raw_path,
            manifest_path: wrong_raw_manifest_path,
            expected_artifact_sha256: wrong_raw_sha256,
            expected_artifact_byte_count: summary.raw_artifact_byte_count,
            expected_manifest_sha256: sha256_hex(&Sha256::digest(&wrong_raw_manifest_bytes)),
            expected_range_set_sha256: wrong_raw_manifest.range_set_sha256,
            expected_record_count: summary.record_count,
            expected_range_count: summary.range_count as usize,
        };
        assert!(open_verified_uhc_replay(&wrong_raw_request)
            .err()
            .expect("wrong raw whole-file digest")
            .to_string()
            .contains("SHA-256 does not match"));

        let mut out_of_bounds =
            open_verified_uhc_replay(&replay_request(&summary, &manifest)).expect("open replay");
        assert!(out_of_bounds
            .visit_verified_range_records(99, |_ordinal, _record| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("out of bounds"));
        out_of_bounds.manifest.ranges[0].record_count += 1;
        assert!(out_of_bounds
            .visit_verified_range_records(0, |_ordinal, _record| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("proof does not match"));

        let raw_path = PathBuf::from(&summary.raw_artifact_path);
        let mut writable = fs::metadata(&raw_path)
            .expect("raw metadata")
            .permissions();
        writable.set_mode(0o600);
        fs::set_permissions(&raw_path, writable).expect("make raw writable");
        fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(&raw_path)
            .expect("truncate raw");
        let mut sealed = fs::metadata(&raw_path)
            .expect("truncated raw metadata")
            .permissions();
        sealed.set_mode(0o400);
        fs::set_permissions(&raw_path, sealed).expect("reseal raw");
        out_of_bounds.raw_identity =
            FileIdentity::from_file(&out_of_bounds.raw_file).expect("refresh raw identity");
        assert!(out_of_bounds
            .visit_verified_range_records(0, |_ordinal, _record| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("ended before its boundary"));
    }

    #[test]
    fn replay_propagates_visitors_and_rechecks_both_descriptor_paths() {
        let visitor_fixture = Fixture::new(FIXTURE);
        let visitor_summary =
            retain_uhc_artifact(&visitor_fixture.request(4)).expect("retain visitor fixture");
        let visitor_manifest = parse_strict_manifest(
            &fs::read(&visitor_summary.manifest_path).expect("read visitor manifest"),
        )
        .expect("parse visitor manifest");
        let mut visitor_source =
            open_verified_uhc_replay(&replay_request(&visitor_summary, &visitor_manifest))
                .expect("open visitor replay");
        assert!(visitor_source
            .visit_verified_range_records(0, |_ordinal, _record| {
                Err(io::Error::other("injected visitor failure"))
            })
            .unwrap_err()
            .to_string()
            .contains("injected visitor failure"));

        visitor_source.manifest.ranges[0].record_start = u64::MAX;
        assert!(visitor_source
            .visit_verified_range_records(0, |_ordinal, _record| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("occurrence ordinal overflowed"));

        let precheck_fixture = Fixture::new(FIXTURE);
        let precheck_summary =
            retain_uhc_artifact(&precheck_fixture.request(4)).expect("retain precheck fixture");
        let precheck_manifest = parse_strict_manifest(
            &fs::read(&precheck_summary.manifest_path).expect("read precheck manifest"),
        )
        .expect("parse precheck manifest");
        let precheck_source =
            open_verified_uhc_replay(&replay_request(&precheck_summary, &precheck_manifest))
                .expect("open precheck replay");
        let manifest_path = PathBuf::from(&precheck_summary.manifest_path);
        let moved_manifest = manifest_path.with_extension("moved");
        fs::rename(&manifest_path, &moved_manifest).expect("move replay manifest");
        fs::write(&manifest_path, b"replacement").expect("replace replay manifest");
        assert!(precheck_source
            .visit_verified_range_records(0, |_ordinal, _record| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("identity or permissions changed"));

        let postcheck_fixture = Fixture::new(FIXTURE);
        let postcheck_summary =
            retain_uhc_artifact(&postcheck_fixture.request(4)).expect("retain postcheck fixture");
        let postcheck_manifest = parse_strict_manifest(
            &fs::read(&postcheck_summary.manifest_path).expect("read postcheck manifest"),
        )
        .expect("parse postcheck manifest");
        let postcheck_source =
            open_verified_uhc_replay(&replay_request(&postcheck_summary, &postcheck_manifest))
                .expect("open postcheck replay");
        let postcheck_manifest_path = PathBuf::from(&postcheck_summary.manifest_path);
        let moved_postcheck = postcheck_manifest_path.with_extension("moved");
        let mut replaced = false;
        assert!(postcheck_source
            .visit_verified_range_records(0, |_ordinal, _record| {
                if !replaced {
                    fs::rename(&postcheck_manifest_path, &moved_postcheck)?;
                    fs::write(&postcheck_manifest_path, b"replacement")?;
                    replaced = true;
                }
                Ok(())
            })
            .unwrap_err()
            .to_string()
            .contains("identity or permissions changed"));

        let raw_postcheck_fixture = Fixture::new(FIXTURE);
        let raw_postcheck_summary = retain_uhc_artifact(&raw_postcheck_fixture.request(4))
            .expect("retain raw postcheck fixture");
        let raw_postcheck_manifest = parse_strict_manifest(
            &fs::read(&raw_postcheck_summary.manifest_path)
                .expect("read raw postcheck manifest"),
        )
        .expect("parse raw postcheck manifest");
        let raw_postcheck_source = open_verified_uhc_replay(&replay_request(
            &raw_postcheck_summary,
            &raw_postcheck_manifest,
        ))
        .expect("open raw postcheck replay");
        let raw_postcheck_path = PathBuf::from(&raw_postcheck_summary.raw_artifact_path);
        let moved_raw_postcheck = raw_postcheck_path.with_extension("moved");
        let mut raw_replaced = false;
        assert!(raw_postcheck_source
            .visit_verified_range_records(0, |_ordinal, _record| {
                if !raw_replaced {
                    fs::rename(&raw_postcheck_path, &moved_raw_postcheck)?;
                    fs::write(&raw_postcheck_path, b"replacement")?;
                    raw_replaced = true;
                }
                Ok(())
            })
            .unwrap_err()
            .to_string()
            .contains("identity or permissions changed"));
    }

    #[test]
    fn verification_replay_filesystem_and_worker_fault_boundaries_are_explicit() {
        let two_bytes = Fixture::new(b"{}");
        let input = File::open(&two_bytes.source).expect("open two-byte fixture");
        let boundary = RawRangeBoundary {
            range_ordinal: 0,
            raw_byte_start: 0,
            raw_byte_end: 2,
            record_start: 0,
            record_end: 1,
        };
        assert!(verify_raw_range(
            &input,
            RawRangeBoundary {
                raw_byte_start: 2,
                raw_byte_end: 1,
                ..boundary
            },
        )
        .unwrap_err()
        .to_string()
        .contains("byte count underflowed"));
        assert!(verify_raw_range(
            &input,
            RawRangeBoundary {
                record_start: 2,
                record_end: 1,
                ..boundary
            },
        )
        .unwrap_err()
        .to_string()
        .contains("record count underflowed"));
        assert!(verify_raw_range(
            &input,
            RawRangeBoundary {
                raw_byte_end: 3,
                ..boundary
            },
        )
        .unwrap_err()
        .to_string()
        .contains("ended before"));
        assert!(verify_raw_range(
            &input,
            RawRangeBoundary {
                range_ordinal: i64::MAX as u64 + 1,
                ..boundary
            },
        )
        .unwrap_err()
        .to_string()
        .contains("signed 64-bit"));
        assert!(verify_ranges_parallel(
            Arc::new(input.try_clone().expect("clone verification input")),
            vec![RawRangeBoundary {
                raw_byte_end: 3,
                ..boundary
            }],
        )
        .is_err());

        let root = RootDirectory::open(&two_bytes.output).expect("open filesystem test root");
        fs::create_dir(two_bytes.output.join("directory")).expect("create final directory");
        assert!(root
            .open_existing_regular("directory")
            .unwrap_err()
            .to_string()
            .contains("not a regular file"));
        root.unlink("already-absent").expect("absent unlink is idempotent");
        let temporary = root.create_temporary("link-failure").expect("temporary file");
        assert!(temporary.publish_noclobber("missing/child").is_err());
        let original_permissions = fs::metadata(&two_bytes.output)
            .expect("filesystem root metadata")
            .permissions();
        let mut read_only_permissions = original_permissions.clone();
        read_only_permissions.set_mode(0o500);
        fs::set_permissions(&two_bytes.output, read_only_permissions)
            .expect("make filesystem root read-only");
        let create_failure = root.create_temporary("permission-failure");
        fs::set_permissions(&two_bytes.output, original_permissions)
            .expect("restore filesystem root permissions");
        assert!(matches!(
            create_failure,
            Err(ref error) if error.kind() == io::ErrorKind::PermissionDenied
        ));

        let (sender, receiver) = sync_channel(1);
        let mut automatic_flush = ConcurrentRangeWorkers {
            senders: vec![sender],
            pending: vec![PendingRecordBatch::default()],
            handles: Vec::new(),
        };
        automatic_flush
            .send_record(0, &vec![b'x'; RANGE_RECORD_BATCH_BYTES])
            .expect("full record batch is flushed");
        assert!(automatic_flush.pending[0].records.is_empty());
        assert!(matches!(
            receiver.recv().expect("automatic flush message"),
            RangeWorkerMessage::Records(_)
        ));

        let truncated = Fixture::new(b"");
        let truncated_input = File::open(&truncated.source).expect("open truncated input");
        let mut offsets = tempfile::tempfile().expect("truncated offsets");
        assert!(scan_raw_and_build_ranges(
            &truncated_input,
            Arc::new(truncated_input.try_clone().expect("clone truncated input")),
            None,
            &mut offsets,
            &[0u8; SHA256_BYTES],
            1,
            4,
        )
        .unwrap_err()
        .to_string()
        .contains("ended before"));

        let empty_array = Fixture::new(b"[]");
        let empty_array_input =
            File::open(&empty_array.source).expect("open empty-array input");
        let mut empty_offsets = tempfile::tempfile().expect("empty-array offsets");
        assert!(scan_raw_and_build_ranges(
            &empty_array_input,
            Arc::new(
                empty_array_input
                    .try_clone()
                    .expect("clone empty-array input"),
            ),
            None,
            &mut empty_offsets,
            &Sha256::digest(b"[]").into(),
            2,
            4,
        )
        .unwrap_err()
        .to_string()
        .contains("contains no records"));

        #[cfg(target_os = "linux")]
        {
            let retained = Fixture::new(FIXTURE);
            let summary =
                retain_uhc_artifact(&retained.request(4)).expect("retain replay fixture");
            let manifest = parse_strict_manifest(
                &fs::read(&summary.manifest_path).expect("read replay manifest"),
            )
            .expect("parse replay manifest");
            let invalid_name = retained.output.join(
                <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(vec![0xff]),
            );
            fs::copy(&summary.raw_artifact_path, &invalid_name).expect("copy invalid-name raw");
            let mut invalid_name_permissions = fs::metadata(&invalid_name)
                .expect("invalid-name raw metadata")
                .permissions();
            invalid_name_permissions.set_mode(0o400);
            fs::set_permissions(&invalid_name, invalid_name_permissions)
                .expect("seal invalid-name raw");
            let mut invalid_name_request = replay_request(&summary, &manifest);
            invalid_name_request.raw_path = invalid_name;
            assert!(open_verified_uhc_replay(&invalid_name_request)
                .err()
                .expect("non-UTF-8 raw file name rejected")
                .to_string()
                .contains("file name is invalid"));
        }
    }

    #[test]
    fn every_manifest_range_sequence_coordinate_fails_independently() {
        let fixture = Fixture::new(FIXTURE);
        let summary = retain_uhc_artifact(&fixture.request(4)).expect("retain sequence fixture");
        let manifest = parse_strict_manifest(
            &fs::read(&summary.manifest_path).expect("read sequence manifest"),
        )
        .expect("parse sequence manifest");
        let ranges = manifest.ranges;

        assert!(validate_range_sequence(&ranges, summary.record_count, ranges.len() + 1).is_err());
        for mutation in 0..10 {
            let mut malformed = ranges.clone();
            match mutation {
                0 => malformed[0].range_ordinal += 1,
                1 => malformed[0].record_start += 1,
                2 => malformed[0].record_end = 0,
                3 => malformed[0].record_count += 1,
                4 => malformed[0].record_count = 0,
                5 => malformed[0].raw_byte_end = malformed[0].raw_byte_start,
                6 => malformed[0].raw_byte_count += 1,
                7 => malformed[0].raw_byte_count = 0,
                8 => malformed[0].canonical_byte_count = 0,
                9 => malformed[0].raw_sha256 = "invalid".to_owned(),
                _ => unreachable!(),
            }
            assert!(validate_range_sequence(&malformed, summary.record_count, malformed.len())
                .is_err());
        }
        let mut bad_canonical_sha = ranges.clone();
        bad_canonical_sha[0].canonical_sha256 = "invalid".to_owned();
        assert!(validate_range_sequence(
            &bad_canonical_sha,
            summary.record_count,
            bad_canonical_sha.len(),
        )
        .is_err());
        let mut overlapping = ranges.clone();
        overlapping[1].raw_byte_start = overlapping[0].raw_byte_end;
        overlapping[1].raw_byte_count =
            overlapping[1].raw_byte_end - overlapping[1].raw_byte_start;
        assert!(validate_range_sequence(
            &overlapping,
            summary.record_count,
            overlapping.len(),
        )
        .is_err());
        assert!(validate_range_sequence(&ranges, summary.record_count + 1, ranges.len()).is_err());
    }
}
