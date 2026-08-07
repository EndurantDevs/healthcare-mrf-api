#![cfg(unix)]

use super::*;
use std::ffi::CString;
use std::fs;
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::time::{Duration, Instant};

fn sealed_run(root: &Path, bytes: &[u8]) -> (PrivateScratch, ScratchRun, std::path::PathBuf) {
    let scratch = PrivateScratch::create(root, bytes.len() as u64 * 2, 0).unwrap();
    let (pending, mut file) = scratch
        .create_run(ScratchRunKind::Initial, bytes.len() as u64)
        .unwrap();
    file.write_all(bytes).unwrap();
    file.flush().unwrap();
    let run = scratch
        .seal_run(pending, &mut file, &mut || Ok(()))
        .unwrap();
    let path = root.join(&scratch.directory_name).join(&run.name);
    (scratch, run, path)
}

fn assert_tamper_rejected(mutator: impl FnOnce(&Path)) {
    let root = tempfile::tempdir().unwrap();
    let (scratch, run, path) = sealed_run(root.path(), b"sealed-collision-audit-run");
    mutator(&path);

    let error = scratch.open_run(&run, &mut || Ok(())).unwrap_err();

    assert_eq!(error.to_string(), SCRATCH_UNAVAILABLE);
    drop(scratch);
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());
}

#[test]
fn sealed_run_detects_mutation_append_truncation_and_mode_drift() {
    assert_tamper_rejected(|path| {
        let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
        file.write_all(b"changed").unwrap();
        file.flush().unwrap();
    });
    assert_tamper_rejected(|path| {
        fs::OpenOptions::new()
            .append(true)
            .open(path)
            .unwrap()
            .write_all(b"extra")
            .unwrap();
    });
    assert_tamper_rejected(|path| {
        fs::OpenOptions::new()
            .write(true)
            .open(path)
            .unwrap()
            .set_len(1)
            .unwrap();
    });
    assert_tamper_rejected(|path| {
        fs::set_permissions(path, fs::Permissions::from_mode(0o640)).unwrap();
    });
}

#[test]
fn sealed_run_rejects_regular_symlink_and_fifo_name_replacement_nonblocking() {
    assert_tamper_rejected(|path| {
        fs::remove_file(path).unwrap();
        fs::write(path, b"replacement").unwrap();
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).unwrap();
    });
    assert_tamper_rejected(|path| {
        let target = path.with_extension("target");
        fs::write(&target, b"target").unwrap();
        fs::remove_file(path).unwrap();
        symlink(&target, path).unwrap();
        fs::remove_file(target).unwrap();
    });

    let started = Instant::now();
    assert_tamper_rejected(|path| {
        fs::remove_file(path).unwrap();
        let encoded = CString::new(path.as_os_str().as_bytes()).unwrap();
        assert_eq!(unsafe { libc::mkfifo(encoded.as_ptr(), 0o600) }, 0);
    });
    assert!(started.elapsed() < Duration::from_secs(2));
}

#[test]
fn sealed_run_rejects_an_added_hard_link() {
    let root = tempfile::tempdir().unwrap();
    let (scratch, run, path) = sealed_run(root.path(), b"hard-link-sensitive-run");
    let external_link = root.path().join("external-link");
    fs::hard_link(&path, &external_link).unwrap();

    let error = scratch.open_run(&run, &mut || Ok(())).unwrap_err();

    assert_eq!(error.to_string(), SCRATCH_UNAVAILABLE);
    fs::remove_file(external_link).unwrap();
    drop(scratch);
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());
}

#[test]
fn scratch_hashing_is_cancellable_during_seal_open_and_remove() {
    let root = tempfile::tempdir().unwrap();
    let scratch = PrivateScratch::create(root.path(), (HASH_BUFFER_BYTES * 4) as u64, 0).unwrap();
    let bytes = vec![0x5a; HASH_BUFFER_BYTES * 2];
    let (pending, mut file) = scratch
        .create_run(ScratchRunKind::Initial, bytes.len() as u64)
        .unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();
    let seal_error = scratch
        .seal_run(pending, &mut file, &mut || {
            Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"))
        })
        .unwrap_err();
    assert_eq!(seal_error.kind(), io::ErrorKind::Interrupted);
    drop(scratch);
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());

    for operation in ["open", "remove"] {
        let root = tempfile::tempdir().unwrap();
        let (scratch, run, _) = sealed_run(root.path(), &bytes);
        let mut cancel = || Err(io::Error::new(io::ErrorKind::Interrupted, "cancelled"));
        let error = if operation == "open" {
            scratch.open_run(&run, &mut cancel).unwrap_err()
        } else {
            scratch.remove_run(&run, &mut cancel).unwrap_err()
        };
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        drop(scratch);
        assert!(fs::read_dir(root.path()).unwrap().next().is_none());
    }
}

#[test]
fn held_run_fd_is_reauthenticated_after_consumption() {
    let root = tempfile::tempdir().unwrap();
    let (scratch, run, path) = sealed_run(root.path(), b"post-read-reauthentication");
    let mut held = scratch.open_run(&run, &mut || Ok(())).unwrap();
    fs::OpenOptions::new()
        .append(true)
        .open(path)
        .unwrap()
        .write_all(b"tamper")
        .unwrap();

    let error = scratch
        .reauthenticate_run(&mut held, &run, &mut || Ok(()))
        .unwrap_err();

    assert_eq!(error.to_string(), SCRATCH_UNAVAILABLE);
    drop(held);
    drop(scratch);
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());
}

#[test]
fn randomized_directory_names_ignore_predictable_precreation() {
    let root = tempfile::tempdir().unwrap();
    for sequence in 0..MAX_NAME_ATTEMPTS {
        fs::create_dir(root.path().join(format!(
            ".ptg2-tax-collision-{}-{sequence:016x}",
            std::process::id()
        )))
        .unwrap();
    }

    let scratch = PrivateScratch::create(root.path(), 0, 0).unwrap();
    assert!(scratch.directory_name.starts_with(".ptg2-tax-collision-"));
    assert_eq!(scratch.directory_name.len(), 52);
    let metadata = scratch.directory.metadata().unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o700);
    drop(scratch);

    for entry in fs::read_dir(root.path()).unwrap() {
        fs::remove_dir(entry.unwrap().path()).unwrap();
    }
    assert!(fs::read_dir(root.path()).unwrap().next().is_none());
}
