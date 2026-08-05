use super::PairFixture;
use crate::tax_identity_sidecar_bundle::files::{
    hash_open_file, ARTIFACT_SIZE_MISMATCH, HASH_BUFFER_BYTES, UNAVAILABLE_ARTIFACT,
};
use std::fs::{self, File};
use std::io::Seek;

#[cfg(unix)]
#[test]
fn symlink_artifact_replacement_is_not_followed() {
    use std::os::unix::fs::symlink;

    let fixture = PairFixture::new();
    let target = fixture._temporary.path().join("real-v2.sidecar");
    fs::rename(&fixture.v2.path, &target).unwrap();
    symlink(&target, &fixture.v2.path).unwrap();

    let error = fixture.validate().unwrap_err();
    assert_eq!(error.to_string(), UNAVAILABLE_ARTIFACT);
}

#[cfg(unix)]
#[test]
fn fifo_artifact_replacement_is_nonblocking_and_rejected() {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::OpenOptionsExt;
    use std::sync::mpsc;
    use std::thread;
    use std::time::{Duration, Instant};

    let fixture = PairFixture::new();
    fs::remove_file(&fixture.v2.path).unwrap();
    let fifo_path = CString::new(fixture.v2.path.as_os_str().as_bytes()).unwrap();
    let result = unsafe { libc::mkfifo(fifo_path.as_ptr(), 0o600) };
    assert_eq!(result, 0);

    let (cancel_fallback, fallback_cancelled) = mpsc::channel();
    let fallback_path = fixture.v2.path.clone();
    let fallback = thread::spawn(move || {
        if fallback_cancelled
            .recv_timeout(Duration::from_secs(2))
            .is_ok()
        {
            return false;
        }
        let _ = std::fs::OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(fallback_path);
        true
    });
    let started = Instant::now();
    let result = fixture.validate();
    let elapsed = started.elapsed();
    let _ = cancel_fallback.send(());
    let fallback_ran = fallback.join().unwrap();

    let error = result.unwrap_err();
    assert!(
        !fallback_ran,
        "FIFO validation waited for its fallback writer"
    );
    assert!(elapsed < Duration::from_secs(2));
    assert_eq!(error.to_string(), UNAVAILABLE_ARTIFACT);
}

#[test]
fn hashing_stops_at_the_declared_boundary_and_rejects_extra_bytes() {
    let temporary = tempfile::tempdir().unwrap();
    let path = temporary.path().join("oversized.sidecar");
    fs::write(&path, vec![0x5a; HASH_BUFFER_BYTES * 2]).unwrap();
    let mut file = File::open(path).unwrap();

    let error = hash_open_file(&mut file, 7).unwrap_err();

    assert_eq!(error.to_string(), ARTIFACT_SIZE_MISMATCH);
    assert_eq!(file.stream_position().unwrap(), 8);
}

#[cfg(unix)]
#[test]
fn descriptor_path_replacement_with_identical_bytes_is_rejected() {
    let fixture = PairFixture::new();
    let path = fixture.v2.path.clone();
    let detached = fixture._temporary.path().join("detached-v2.sidecar");
    let bytes = fs::read(&path).unwrap();

    let error = super::validate_tax_identity_sidecar_shard_with_progress(
        "synthetic-a",
        fixture.v1(),
        &fixture.v2,
        &super::VecUniverse(fixture.groups.clone()),
        |ordinal| {
            if ordinal == 1 {
                fs::rename(&path, &detached)?;
                fs::write(&path, &bytes)?;
            }
            Ok(())
        },
    )
    .unwrap_err();

    assert_eq!(error.to_string(), super::ARTIFACT_DIGEST_MISMATCH);
    assert!(!error.to_string().contains(path.to_string_lossy().as_ref()));
}
