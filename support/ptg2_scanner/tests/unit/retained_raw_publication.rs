// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
// Private white-box tests included only by the retained-artifact unit-test module.

fn raw_publication_fixture() -> (
    Fixture,
    Arc<RootDirectory>,
    String,
    [u8; SHA256_BYTES],
) {
    let fixture = Fixture::new(FIXTURE);
    let root = RootDirectory::open(&fixture.output).expect("open retained root");
    let raw_name = raw_file_name(&fixture.sha256);
    let expected_sha256 = parse_sha256_hex(&fixture.sha256).expect("decode fixture SHA-256");
    (fixture, root, raw_name, expected_sha256)
}

fn raw_publication_candidate(root: &Arc<RootDirectory>, label: &str) -> RootTemporaryFile {
    let mut candidate = root.create_temporary(label).expect("raw candidate");
    candidate
        .file_mut()
        .write_all(FIXTURE)
        .expect("write raw candidate");
    candidate.file().sync_all().expect("sync raw candidate");
    candidate
}

#[test]
fn raw_candidate_publication_collision_deterministically_reuses_incumbent() {
    let (fixture, root, raw_name, expected_sha256) = raw_publication_fixture();
    let candidate = raw_publication_candidate(&root, "raw-collision");
    let candidate_path = fixture.output.join(&candidate.name);

    let winner = raw_publication_candidate(&root, "raw-winner");
    assert!(winner
        .publish_noclobber(&raw_name)
        .expect("publish concurrent incumbent"));
    root.sync().expect("sync concurrent publication");
    let incumbent = root
        .open_existing_regular(&raw_name)
        .expect("open incumbent raw")
        .expect("incumbent raw exists");
    let incumbent_identity = FileIdentity::from_file(&incumbent).expect("incumbent identity");

    let (reused, authoritative_identity) = publish_or_verify_raw(
        &root,
        &raw_name,
        candidate,
        &expected_sha256,
        fixture.byte_count,
    )
    .expect("verify deterministic raw publication collision");

    assert!(reused);
    assert_eq!(authoritative_identity, incumbent_identity);
    assert_eq!(fs::read(fixture.raw_path()).expect("read incumbent"), FIXTURE);
    assert!(!candidate_path.exists());
    assert_eq!(retained_files(&fixture.output), vec![raw_name]);
}

#[test]
fn raw_candidate_publication_rejects_disappearing_new_link() {
    let (fixture, root, raw_name, expected_sha256) = raw_publication_fixture();
    let mut candidate = raw_publication_candidate(&root, "raw-new-link");
    let candidate_path = fixture.output.join(&candidate.name);
    let probe_root = Arc::clone(&root);
    let probe_name = raw_name.clone();
    candidate.pre_unlink_probe = Some(Box::new(move || {
        probe_root
            .unlink(&probe_name)
            .expect("remove newly published link");
    }));

    let error = publish_or_verify_raw(
        &root,
        &raw_name,
        candidate,
        &expected_sha256,
        fixture.byte_count,
    )
    .expect_err("missing new publication must fail closed");

    assert!(error.to_string().contains("published retained"));
    assert!(!candidate_path.exists());
    assert!(retained_files(&fixture.output).is_empty());
}

#[test]
fn raw_candidate_publication_rejects_disappearing_incumbent() {
    let (fixture, root, raw_name, expected_sha256) = raw_publication_fixture();
    let winner = raw_publication_candidate(&root, "raw-winner");
    assert!(winner
        .publish_noclobber(&raw_name)
        .expect("publish concurrent incumbent"));
    root.sync().expect("sync concurrent publication");

    let mut candidate = raw_publication_candidate(&root, "raw-collision");
    let candidate_path = fixture.output.join(&candidate.name);
    let probe_root = Arc::clone(&root);
    let probe_name = raw_name.clone();
    candidate.pre_unlink_probe = Some(Box::new(move || {
        probe_root
            .unlink(&probe_name)
            .expect("remove concurrent incumbent");
    }));

    let error = publish_or_verify_raw(
        &root,
        &raw_name,
        candidate,
        &expected_sha256,
        fixture.byte_count,
    )
    .expect_err("missing incumbent must fail closed");

    assert!(error.to_string().contains("concurrently published"));
    assert!(!candidate_path.exists());
    assert!(retained_files(&fixture.output).is_empty());
}

#[test]
fn raw_candidate_publication_rejects_new_link_byte_drift() {
    let (fixture, root, raw_name, expected_sha256) = raw_publication_fixture();
    let mut candidate = raw_publication_candidate(&root, "raw-byte-drift");
    let candidate_path = fixture.output.join(&candidate.name);
    let probe_path = fixture.raw_path();
    candidate.pre_unlink_probe = Some(Box::new(move || {
        let final_file = fs::OpenOptions::new()
            .write(true)
            .open(&probe_path)
            .expect("open newly published raw");
        final_file
            .set_len((FIXTURE.len() - 1) as u64)
            .expect("truncate newly published raw");
        final_file.sync_all().expect("sync truncated raw");
    }));

    let error = publish_or_verify_raw(
        &root,
        &raw_name,
        candidate,
        &expected_sha256,
        fixture.byte_count,
    )
    .expect_err("changed publication must fail closed");

    assert!(error.to_string().contains("inode changed unexpectedly"));
    assert!(!candidate_path.exists());
    assert_eq!(retained_files(&fixture.output), vec![raw_name]);
    assert_eq!(
        fs::metadata(fixture.raw_path())
            .expect("changed raw metadata")
            .len(),
        fixture.byte_count - 1
    );
}
