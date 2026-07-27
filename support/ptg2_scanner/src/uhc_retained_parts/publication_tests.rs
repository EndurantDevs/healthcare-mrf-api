// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[test]
fn manifest_admission_rejects_size_encoding_and_proof_drift() {
    assert!(parse_strict_manifest(b"{")
        .expect_err("invalid JSON manifest rejected")
        .to_string()
        .contains("manifest is invalid"));
    assert!(parse_strict_manifest(b"{}")
        .expect_err("incomplete manifest rejected")
        .to_string()
        .contains("manifest is invalid"));

    let empty = Fixture::new(b"");
    let empty_file = File::open(&empty.source).expect("open empty manifest fixture");
    assert!(read_bounded_stable_file(&empty_file, 1, "manifest")
        .expect_err("empty manifest rejected")
        .to_string()
        .contains("invalid byte count"));

    let oversized = Fixture::new(b"{}");
    let oversized_file = File::open(&oversized.source).expect("open oversized manifest fixture");
    assert!(read_bounded_stable_file(&oversized_file, 1, "manifest")
        .expect_err("oversized manifest rejected")
        .to_string()
        .contains("invalid byte count"));

    let retained = Fixture::new(FIXTURE);
    retain_uhc_artifact(&retained.request(4)).expect("retain fixture");
    let manifest_bytes = fs::read(retained.manifest_path(4)).expect("read canonical manifest");
    let mut oversized_manifest =
        parse_strict_manifest(&manifest_bytes).expect("parse oversized manifest fixture");
    oversized_manifest.producer_build_id = "x".repeat(MAX_MANIFEST_BYTES as usize);
    assert!(encode_manifest(&oversized_manifest)
        .expect_err("oversized encoded manifest rejected")
        .to_string()
        .contains("exceeds its byte limit"));

    let mut noncanonical = manifest_bytes.clone();
    noncanonical.push(b'\n');
    assert!(parse_strict_manifest(&noncanonical)
        .expect_err("noncanonical manifest rejected")
        .to_string()
        .contains("deterministic canonical encoding"));

    let mut manifest = parse_strict_manifest(&manifest_bytes).expect("parse canonical manifest");
    manifest.range_set_sha256 = "0".repeat(64);
    let expected_raw = manifest.raw_artifact.clone();
    let expected_ranges = manifest.ranges.clone();
    let expected_range_set_sha256 = manifest.range_set_sha256.clone();
    assert!(validate_existing_manifest(
        &manifest,
        &expected_raw,
        &expected_ranges,
        &expected_range_set_sha256,
    )
    .expect_err("mismatched range-set proof rejected")
    .to_string()
    .contains("invalid range-set proof"));

    let mut invalid_raw_sha = parse_strict_manifest(&manifest_bytes).expect("parse manifest");
    invalid_raw_sha.raw_artifact.sha256 = "x".repeat(64);
    let expected_raw = invalid_raw_sha.raw_artifact.clone();
    assert!(validate_existing_manifest(
        &invalid_raw_sha,
        &expected_raw,
        &invalid_raw_sha.ranges,
        &invalid_raw_sha.range_set_sha256,
    )
    .expect_err("invalid raw SHA rejected")
    .to_string()
    .contains("raw SHA-256 is invalid"));

    let mut invalid_sequence = parse_strict_manifest(&manifest_bytes).expect("parse manifest");
    invalid_sequence.ranges[0].range_ordinal += 1;
    let expected_raw = invalid_sequence.raw_artifact.clone();
    let expected_ranges = invalid_sequence.ranges.clone();
    assert!(validate_existing_manifest(
        &invalid_sequence,
        &expected_raw,
        &expected_ranges,
        &invalid_sequence.range_set_sha256,
    )
    .expect_err("invalid logical range sequence rejected")
    .to_string()
    .contains("invalid logical range"));

    let non_utf8_path = PathBuf::from(
        <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(vec![0xff]),
    );
    assert!(path_text(&non_utf8_path, "manifest")
        .expect_err("non-UTF-8 path rejected")
        .to_string()
        .contains("path is not UTF-8"));
}

#[test]
fn bounded_reader_reports_truncation_and_post_read_identity_drift() {
    let truncated = Fixture::new(b"truncated");
    let truncated_file = File::open(&truncated.source).expect("open truncation fixture");
    let truncated_identity =
        FileIdentity::from_file(&truncated_file).expect("truncation fixture identity");
    OpenOptions::new()
        .write(true)
        .open(&truncated.source)
        .expect("open truncation writer")
        .set_len(0)
        .expect("truncate fixture after identity capture");
    assert!(read_bounded_file_from_identity(
        &truncated_file,
        truncated_identity,
        truncated_identity.byte_count,
        "manifest",
    )
    .expect_err("truncated file rejected")
    .to_string()
    .contains("ended unexpectedly"));

    let changed = Fixture::new(b"changed");
    let changed_file = File::open(&changed.source).expect("open changed fixture");
    let changed_identity =
        FileIdentity::from_file(&changed_file).expect("changed fixture identity");
    let mut changed_writer = OpenOptions::new()
        .append(true)
        .open(&changed.source)
        .expect("open changed writer");
    changed_writer
        .write_all(b"!")
        .expect("change fixture after identity capture");
    changed_writer.sync_all().expect("sync changed fixture");
    assert!(read_bounded_file_from_identity(
        &changed_file,
        changed_identity,
        changed_identity.byte_count,
        "manifest",
    )
    .expect_err("post-read identity drift rejected")
    .to_string()
    .contains("changed while it was read"));
}

#[test]
fn candidate_publish_collision_and_post_link_checks_are_deterministic() {
    let retained = Fixture::new(FIXTURE);
    retain_uhc_artifact(&retained.request(4)).expect("retain manifest fixture");
    let candidate = parse_strict_manifest(
        &fs::read(retained.manifest_path(4)).expect("read manifest fixture"),
    )
    .expect("parse manifest fixture");
    let expected_raw = candidate.raw_artifact.clone();
    let expected_ranges = candidate.ranges.clone();
    let expected_range_set_sha256 = candidate.range_set_sha256.clone();
    let manifest_name = manifest_file_name(&retained.sha256, 4);

    let retained_root = RootDirectory::open(&retained.output).expect("open retained root");
    let collision = publish_manifest_candidate(
        &retained_root,
        &manifest_name,
        &candidate,
        &expected_raw,
        &expected_ranges,
        &expected_range_set_sha256,
    )
    .expect("verify deterministic publication collision");
    assert!(collision.reused);

    let fresh = Fixture::new(FIXTURE);
    let fresh_root = RootDirectory::open(&fresh.output).expect("open fresh root");
    let publication = publish_manifest_candidate(
        &fresh_root,
        &manifest_name,
        &candidate,
        &expected_raw,
        &expected_ranges,
        &expected_range_set_sha256,
    )
    .expect("publish deterministic manifest fixture");
    assert!(!publication.reused);

    let final_file = fresh_root
        .open_existing_regular(&manifest_name)
        .expect("open final manifest")
        .expect("published manifest exists");
    let final_identity = FileIdentity::from_file(&final_file).expect("final manifest identity");
    let unrelated_file = File::open(&fresh.source).expect("open unrelated inode");
    let unrelated_identity =
        FileIdentity::from_file(&unrelated_file).expect("unrelated file identity");
    let inode_error = verify_new_manifest_publication(
        &final_file,
        unrelated_identity,
        &candidate.producer_build_id,
        publication.encoded.clone(),
    )
    .err()
    .expect("post-link inode substitution rejected");
    assert!(inode_error.to_string().contains("inode changed"));
    let byte_error = verify_new_manifest_publication(
        &final_file,
        final_identity,
        &candidate.producer_build_id,
        b"{}\n".to_vec(),
    )
    .err()
    .expect("post-link byte substitution rejected");
    assert!(byte_error.to_string().contains("bytes changed"));
}

#[test]
fn manifest_reverification_rejects_missing_replaced_and_changed_files() {
    let fixture = Fixture::new(FIXTURE);
    let root = RootDirectory::open(&fixture.output).expect("open retained root");
    let source_file = File::open(&fixture.source).expect("open source fixture");
    let source_identity = FileIdentity::from_file(&source_file).expect("source identity");
    let unavailable = ManifestPublication {
        producer_build_id: "test-build".to_owned(),
        encoded: b"{}\n".to_vec(),
        reused: true,
        final_identity: source_identity,
    };
    assert!(
        reverify_manifest_path(&root, "missing.manifest.json", &unavailable)
            .expect_err("missing manifest rejected")
            .to_string()
            .contains("disappeared")
    );

    let manifest_name = "present.manifest.json";
    let manifest_path = fixture.output.join(manifest_name);
    fs::write(&manifest_path, b"{}\n").expect("write manifest fixture");
    assert!(reverify_manifest_path(&root, manifest_name, &unavailable)
        .expect_err("replacement manifest rejected")
        .to_string()
        .contains("identity changed"));

    let manifest_file = File::open(&manifest_path).expect("open manifest fixture");
    let manifest_identity = FileIdentity::from_file(&manifest_file).expect("manifest identity");
    let changed = ManifestPublication {
        final_identity: manifest_identity,
        encoded: b"{\"expected\":true}\n".to_vec(),
        ..unavailable
    };
    assert!(reverify_manifest_path(&root, manifest_name, &changed)
        .expect_err("changed manifest rejected")
        .to_string()
        .contains("bytes changed"));
}

#[test]
fn existing_manifest_rejects_drift_in_every_identity_coordinate() {
    let fixture = Fixture::new(FIXTURE);
    retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
    let manifest = parse_strict_manifest(
        &fs::read(fixture.manifest_path(4)).expect("read canonical manifest"),
    )
    .expect("parse canonical manifest");
    let expected_raw = manifest.raw_artifact.clone();
    let expected_ranges = manifest.ranges.clone();
    let expected_range_set_sha256 = manifest.range_set_sha256.clone();

    let mut drifts = Vec::new();
    let mut contract_id = manifest.clone();
    contract_id.contract_id.push_str("-other");
    drifts.push(contract_id);
    let mut contract_version = manifest.clone();
    contract_version.contract_version += 1;
    drifts.push(contract_version);
    let mut canonicalization = manifest.clone();
    canonicalization.canonicalization_id.push_str("-other");
    drifts.push(canonicalization);
    let mut raw_artifact = manifest.clone();
    raw_artifact.raw_artifact.byte_count += 1;
    drifts.push(raw_artifact);
    let mut range_count = manifest.clone();
    range_count.range_count += 1;
    drifts.push(range_count);
    let mut ranges = manifest.clone();
    ranges.ranges[0].canonical_byte_count += 1;
    drifts.push(ranges);
    let mut range_set = manifest;
    range_set.range_set_sha256 = "f".repeat(64);
    drifts.push(range_set);

    for drift in drifts {
        assert!(validate_existing_manifest(
            &drift,
            &expected_raw,
            &expected_ranges,
            &expected_range_set_sha256,
        )
        .expect_err("identity-coordinate drift rejected")
        .to_string()
        .contains("does not match"));
    }
}

#[test]
fn raw_reverification_and_path_admission_fail_closed() {
    let fixture = Fixture::new(FIXTURE);
    let source_file = File::open(&fixture.source).expect("open source fixture");
    let source_identity = FileIdentity::from_file(&source_file).expect("source identity");
    let expected_sha256 =
        parse_sha256_hex(&fixture.request(4).expected_sha256).expect("fixture SHA-256");

    verify_whole_file_sha256(
        &source_file,
        source_identity,
        &expected_sha256,
        source_identity.byte_count,
        "raw artifact",
    )
    .expect("whole-file proof");
    verify_existing_raw_file(
        &source_file,
        &expected_sha256,
        source_identity.byte_count,
    )
    .expect("existing-file proof");

    let wrong_sha256 = [0u8; SHA256_BYTES];
    assert!(verify_existing_raw_file(
        &source_file,
        &wrong_sha256,
        source_identity.byte_count,
    )
    .expect_err("wrong raw SHA rejected")
    .to_string()
    .contains("SHA-256 does not match"));
    assert!(c_string("unsafe\0path", "retained path")
        .expect_err("interior NUL rejected")
        .to_string()
        .contains("NUL byte"));
}

#[test]
fn strict_value_proof_and_cli_coordinates_reject_ambiguous_inputs() {
    use serde::de::value::Error as ValueError;

    let value_expectation = <ValueError as de::Error>::invalid_type(
        de::Unexpected::Unit,
        &StrictValueVisitor,
    );
    assert!(value_expectation.to_string().contains("strict JSON value"));
    let object_expectation = <ValueError as de::Error>::invalid_type(
        de::Unexpected::Unit,
        &StrictObjectVisitor,
    );
    assert!(object_expectation.to_string().contains("JSON object"));
    assert!(validate_strict_json_object(b"[]")
        .expect_err("non-object record rejected")
        .to_string()
        .contains("JSON object"));
    assert!(validate_strict_json_object(b"{} trailing")
        .expect_err("trailing record content rejected")
        .to_string()
        .contains("invalid JSON"));

    assert!(StrictValueVisitor.visit_bool::<ValueError>(true).is_ok());
    assert!(StrictValueVisitor
        .visit_bool::<serde_json::Error>(true)
        .is_ok());
    assert!(StrictValueVisitor.visit_i64::<ValueError>(-1).is_ok());
    assert!(StrictValueVisitor.visit_u64::<ValueError>(1).is_ok());
    assert!(StrictValueVisitor.visit_f64::<ValueError>(1.5).is_ok());
    assert!(StrictValueVisitor
        .visit_f64::<serde_json::Error>(1.5)
        .is_ok());
    assert!(StrictValueVisitor.visit_str::<ValueError>("text").is_ok());
    assert!(StrictValueVisitor
        .visit_borrowed_str::<ValueError>("borrowed")
        .is_ok());
    assert!(StrictValueVisitor
        .visit_string::<ValueError>("owned".to_owned())
        .is_ok());
    assert!(StrictValueVisitor.visit_unit::<ValueError>().is_ok());

    let fixture = Fixture::new(FIXTURE);
    retain_uhc_artifact(&fixture.request(4)).expect("retain fixture");
    let manifest = parse_strict_manifest(
        &fs::read(fixture.manifest_path(4)).expect("read canonical manifest"),
    )
    .expect("parse canonical manifest");
    let raw_sha256 =
        parse_sha256_hex(&manifest.raw_artifact.sha256).expect("raw artifact SHA-256");
    for (raw_range_sha256, canonical_range_sha256) in [
        ("x".repeat(64), manifest.ranges[0].canonical_sha256.clone()),
        (manifest.ranges[0].raw_sha256.clone(), "x".repeat(64)),
    ] {
        let mut ranges = manifest.ranges.clone();
        ranges[0].raw_sha256 = raw_range_sha256;
        ranges[0].canonical_sha256 = canonical_range_sha256;
        assert!(range_set_sha256(
            &raw_sha256,
            manifest.raw_artifact.byte_count,
            manifest.raw_artifact.record_count,
            &ranges,
        )
        .is_err());
    }

    let invalid_byte_count = vec![
        "source".to_owned(),
        "output".to_owned(),
        "0".repeat(64),
        "not-a-count".to_owned(),
        "4".to_owned(),
    ];
    assert!(run_uhc_retain_cli(&invalid_byte_count).is_err());
    let mut invalid_range_count = invalid_byte_count;
    invalid_range_count[3] = "1".to_owned();
    invalid_range_count[4] = "not-a-count".to_owned();
    assert!(run_uhc_retain_cli(&invalid_range_count).is_err());
}

#[test]
fn concurrent_range_sender_failure_is_explicit_at_flush_and_finish() {
    let failed_workers = || {
        let (sender, receiver) = sync_channel(1);
        drop(receiver);
        ConcurrentRangeWorkers {
            senders: vec![sender],
            pending: vec![PendingRecordBatch::default()],
            handles: Vec::new(),
        }
    };

    let mut flushing = failed_workers();
    flushing.pending[0].records.push(b"{}".to_vec());
    flushing.pending[0].byte_count = 2;
    assert!(flushing
        .flush(0)
        .expect_err("stopped range worker rejects records")
        .to_string()
        .contains("stopped unexpectedly"));

    let mut finishing = failed_workers();
    assert!(finishing
        .finish_range(RawRangeBoundary {
            range_ordinal: 0,
            raw_byte_start: 0,
            raw_byte_end: 2,
            record_start: 0,
            record_end: 1,
        })
        .expect_err("stopped range worker rejects its terminal boundary")
        .to_string()
        .contains("stopped unexpectedly"));
}

#[test]
fn range_and_filesystem_admission_guards_reject_boundary_drift() {
    let fixture = Fixture::new(FIXTURE);
    retain_uhc_artifact(&fixture.request(4)).expect("retain manifest fixture");
    let manifest = parse_strict_manifest(
        &fs::read(fixture.manifest_path(4)).expect("read manifest fixture"),
    )
    .expect("parse manifest fixture");
    assert!(validate_range_sequence(&manifest.ranges, 8, 5)
        .expect_err("range-list length mismatch rejected")
        .to_string()
        .contains("range count"));
    let mut overlapping = manifest.ranges.clone();
    overlapping[1].raw_byte_start = overlapping[0].raw_byte_end;
    overlapping[1].raw_byte_count =
        overlapping[1].raw_byte_end - overlapping[1].raw_byte_start;
    assert!(validate_range_sequence(&overlapping, 8, 4)
        .expect_err("overlapping raw ranges rejected")
        .to_string()
        .contains("overlap"));
    assert!(validate_range_sequence(&manifest.ranges, 9, 4)
        .expect_err("incomplete record coverage rejected")
        .to_string()
        .contains("cover every record"));
    let directory = File::open(&fixture.output).expect("open directory fixture");
    let directory_identity = FileIdentity::from_file(&directory).expect("directory identity");
    assert!(require_stable_regular_file(&directory, directory_identity, 0, "fixture")
        .expect_err("directory rejected as regular file")
        .to_string()
        .contains("not a regular file"));
    assert!(open_regular_nofollow(&fixture.output, "fixture")
        .expect_err("directory rejected by no-follow opener")
        .to_string()
        .contains("not a regular file"));
    let root = RootDirectory::open(&fixture.output).expect("open retained root");
    fs::create_dir(fixture.output.join("nested")).expect("create nested directory");
    assert!(root
        .open_existing_regular("nested")
        .expect_err("nested directory rejected")
        .to_string()
        .contains("not a regular file"));
    let writable = fixture.output.join("writable");
    fs::write(&writable, b"fixture").expect("write writable fixture");
    fs::set_permissions(&writable, fs::Permissions::from_mode(0o622))
        .expect("set unsafe fixture mode");
    assert!(root.open_existing_regular("writable").is_err());
    root.unlink("already-absent").expect("missing unlink is idempotent");
    let moved = fixture.output.with_extension("moved");
    fs::rename(&fixture.output, &moved).expect("move admitted root");
    fs::create_dir(&fixture.output).expect("replace admitted root path");
    assert!(root.verify_path_identity().is_err());
}
