use super::*;
use std::io::{Seek, SeekFrom, Write};

fn matched_fixture() -> BundleFixture {
    BundleFixture::new(vec![vec![(
        TaxIdentityStateV2::MatchedEin,
        Some(token(0x31)),
    )]])
}

fn assert_private_failure(
    fixture: &BundleFixture,
    descriptors: &[crate::tax_identity_sidecar_bundle::TaxIdentitySidecarV2ArtifactDescriptor],
    suffix: &str,
    expected: &str,
) {
    let scratch = fixture.scratch_root(suffix);
    let error = audit_tax_identity_sidecar_bundle(
        &fixture.checkpoint,
        descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
    )
    .unwrap_err();
    let message = error.to_string();
    assert_eq!(message, expected);
    assert!(!message.contains(fixture.temporary.path().to_string_lossy().as_ref()));
    assert!(!message.contains(&encode_hex(&token(0x31))));
    assert!(directory_is_empty(&scratch));
}

#[cfg(unix)]
#[test]
fn symlink_source_replacement_is_not_followed() {
    use std::os::unix::fs::symlink;

    let fixture = matched_fixture();
    let source = &fixture.descriptors[0].path;
    let target = fixture.temporary.path().join("held-source-v2");
    fs::rename(source, &target).unwrap();
    symlink(&target, source).unwrap();

    assert_private_failure(
        &fixture,
        &fixture.descriptors,
        "source-symlink",
        artifacts::ARTIFACT_UNAVAILABLE,
    );
}

#[cfg(unix)]
#[test]
fn fifo_source_replacement_is_nonblocking_and_rejected() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::time::{Duration, Instant};

    let fixture = matched_fixture();
    let source = &fixture.descriptors[0].path;
    fs::remove_file(source).unwrap();
    let encoded = CString::new(source.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);

    let started = Instant::now();
    assert_private_failure(
        &fixture,
        &fixture.descriptors,
        "source-fifo",
        artifacts::ARTIFACT_UNAVAILABLE,
    );
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn preexisting_truncation_and_append_are_rejected_at_declared_bounds() {
    type SourceMutation = fn(&std::path::Path);
    let cases: [(&str, SourceMutation); 2] = [
        ("source-truncated", |path: &std::path::Path| {
            fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .set_len(1)
                .unwrap()
        }),
        ("source-appended", |path: &std::path::Path| {
            fs::OpenOptions::new()
                .append(true)
                .open(path)
                .unwrap()
                .write_all(b"extra")
                .unwrap()
        }),
    ];
    for (suffix, mutate) in cases {
        let fixture = matched_fixture();
        mutate(&fixture.descriptors[0].path);
        assert_private_failure(
            &fixture,
            &fixture.descriptors,
            suffix,
            artifacts::ARTIFACT_CONTENT_MISMATCH,
        );
    }
}

#[test]
fn in_place_content_tamper_during_authentication_is_rejected() {
    let fixture = matched_fixture();
    let source = fixture.descriptors[0].path.clone();
    let first_hash_end = fixture.descriptors[0].metadata.byte_count;
    let scratch = fixture.scratch_root("source-content-tamper");
    let mut mutated = false;

    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
        |event| {
            if !mutated
                && event.phase == TaxIdentityCollisionAuditPhase::Authenticate
                && event.completed == first_hash_end
            {
                let mut file = fs::OpenOptions::new().write(true).open(&source)?;
                file.seek(SeekFrom::Start(first_hash_end - 1))?;
                file.write_all(&[0x7f])?;
                file.flush()?;
                mutated = true;
            }
            Ok(())
        },
    )
    .unwrap_err();

    assert!(mutated);
    assert_eq!(error.to_string(), artifacts::ARTIFACT_AUTHENTICATION_FAILED);
    assert!(directory_is_empty(&scratch));
}

#[test]
fn held_source_fd_is_reauthenticated_after_record_consumption() {
    let fixture = matched_fixture();
    let source = fixture.descriptors[0].path.clone();
    let scratch = fixture.scratch_root("source-post-read-reauthentication");
    let mut tampered = false;

    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
        |event| {
            if !tampered && event.phase == TaxIdentityCollisionAuditPhase::Scan {
                let mut file = fs::OpenOptions::new().write(true).open(&source)?;
                file.seek(SeekFrom::Start(0))?;
                file.write_all(&[0x7f])?;
                file.flush()?;
                tampered = true;
            }
            Ok(())
        },
    )
    .unwrap_err();

    assert!(tampered);
    assert_eq!(error.to_string(), artifacts::ARTIFACT_AUTHENTICATION_FAILED);
    assert!(directory_is_empty(&scratch));
}

#[test]
fn atomic_path_replacement_during_authentication_is_rejected() {
    let fixture = matched_fixture();
    let source = fixture.descriptors[0].path.clone();
    let original = fs::read(&source).unwrap();
    let first_hash_end = fixture.descriptors[0].metadata.byte_count;
    let scratch = fixture.scratch_root("source-path-replacement");
    let mut replaced = false;

    let error = audit_tax_identity_sidecar_bundle_with_progress(
        &fixture.checkpoint,
        &fixture.descriptors,
        &config(scratch.clone(), 1_000_000, 2, 6),
        |event| {
            if !replaced
                && event.phase == TaxIdentityCollisionAuditPhase::Authenticate
                && event.completed == first_hash_end
            {
                fs::remove_file(&source)?;
                fs::write(&source, &original)?;
                replaced = true;
            }
            Ok(())
        },
    )
    .unwrap_err();

    assert!(replaced);
    assert_eq!(error.to_string(), artifacts::ARTIFACT_AUTHENTICATION_FAILED);
    assert!(directory_is_empty(&scratch));
}

#[cfg(unix)]
#[test]
fn distinct_paths_to_one_physical_source_are_rejected() {
    let fixture = BundleFixture::new(vec![
        vec![(TaxIdentityStateV2::MatchedEin, Some(token(0x41)))],
        vec![(TaxIdentityStateV2::MatchedNpi, Some(token(0x42)))],
    ]);
    let mut descriptors = fixture.descriptors.clone();
    let alias = fixture.temporary.path().join("physical-alias-v2");
    fs::hard_link(&descriptors[0].path, &alias).unwrap();
    descriptors[1].path = alias;

    assert_private_failure(
        &fixture,
        &descriptors,
        "source-hardlink",
        artifacts::ARTIFACT_CONTENT_MISMATCH,
    );
}

#[test]
fn duplicate_descriptor_path_is_rejected_before_scanning() {
    let fixture = BundleFixture::new(vec![
        vec![(TaxIdentityStateV2::MatchedEin, Some(token(0x51)))],
        vec![(TaxIdentityStateV2::MatchedNpi, Some(token(0x52)))],
    ]);
    let mut descriptors = fixture.descriptors.clone();
    descriptors[1].path = descriptors[0].path.clone();

    assert_private_failure(
        &fixture,
        &descriptors,
        "source-duplicate-path",
        artifacts::ARTIFACT_SET_MISMATCH,
    );
}
