use super::{
    ARTIFACT_CONTENT_MISMATCH, ARTIFACT_UNAVAILABLE, ARTIFACT_VERIFICATION_FAILED,
    HASH_BUFFER_BYTES,
};
use crate::tax_identity_sidecar_bundle::collision_audit::sort::AuditProgressCallback;
use crate::tax_identity_sidecar_bundle::collision_audit::{
    invalid_data, TaxIdentityCollisionAuditPhase,
};
use crate::tax_identity_sidecar_bundle::digests::checked_add;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

pub(super) fn reauthenticate_source_path(
    path: &Path,
    expected_identity: &FileIdentity,
    expected_digest: [u8; 32],
    progress_base: u64,
    progress_total: u64,
    progress: &mut AuditProgressCallback<'_>,
) -> io::Result<()> {
    let mut reopened = match open_source_artifact(path) {
        Ok(file) => file,
        Err(_) => return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED)),
    };
    let reopened_identity_before = match file_identity(&reopened) {
        Ok(identity) => identity,
        Err(_) => return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED)),
    };
    let reopened_digest = hash_open_file(
        &mut reopened,
        expected_identity.byte_count,
        progress_base,
        progress_total,
        progress,
    )?;
    let reopened_identity_after = match file_identity(&reopened) {
        Ok(identity) => identity,
        Err(_) => return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED)),
    };
    drop(reopened);
    let final_path_file = match open_source_artifact(path) {
        Ok(file) => file,
        Err(_) => return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED)),
    };
    let final_path_identity = match file_identity(&final_path_file) {
        Ok(identity) => identity,
        Err(_) => return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED)),
    };
    if reopened_identity_before != *expected_identity
        || reopened_identity_after != reopened_identity_before
        || final_path_identity != *expected_identity
        || reopened_digest != expected_digest
    {
        return Err(invalid_data(ARTIFACT_VERIFICATION_FAILED));
    }
    Ok(())
}

pub(super) fn hash_open_file(
    file: &mut File,
    expected_byte_count: u64,
    progress_base: u64,
    progress_total: u64,
    progress: &mut AuditProgressCallback<'_>,
) -> io::Result<[u8; 32]> {
    if file.seek(SeekFrom::Start(0)).is_err() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    let mut observed = 0u64;
    while observed < expected_byte_count {
        let requested =
            match usize::try_from((expected_byte_count - observed).min(HASH_BUFFER_BYTES as u64)) {
                Ok(requested) => requested,
                Err(_) => return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH)),
            };
        let read = match file.read(&mut buffer[..requested]) {
            Ok(read) => read,
            Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
        };
        if read == 0 {
            return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
        }
        hasher.update(&buffer[..read]);
        observed = checked_add(observed, read as u64)?;
        progress(
            TaxIdentityCollisionAuditPhase::VerifySource,
            checked_add(progress_base, observed)?,
            progress_total,
        )?;
    }
    let mut trailing = [0u8; 1];
    let trailing_count = match file.read(&mut trailing) {
        Ok(count) => count,
        Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
    };
    if trailing_count != 0 {
        return Err(invalid_data(ARTIFACT_CONTENT_MISMATCH));
    }
    if file.seek(SeekFrom::Start(0)).is_err() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    Ok(hasher.finalize().into())
}

pub(super) fn open_source_artifact(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = match options.open(path) {
        Ok(file) => file,
        Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
    };
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
    };
    if !metadata.is_file() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    Ok(file)
}

#[derive(Clone, Eq, Hash, PartialEq)]
pub(super) struct FileIdentity {
    pub(super) byte_count: u64,
    #[cfg(unix)]
    device: u64,
    #[cfg(unix)]
    inode: u64,
    #[cfg(unix)]
    modified_seconds: i64,
    #[cfg(unix)]
    modified_nanoseconds: i64,
    #[cfg(unix)]
    changed_seconds: i64,
    #[cfg(unix)]
    changed_nanoseconds: i64,
    #[cfg(not(unix))]
    modified: Option<std::time::SystemTime>,
}

#[cfg(unix)]
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub(super) struct PhysicalFileIdentity {
    device: u64,
    inode: u64,
}

#[cfg(not(unix))]
#[derive(Eq, Hash, PartialEq)]
pub(super) struct PhysicalFileIdentity;

#[cfg(unix)]
pub(super) fn physical_file_identity(file: &File) -> io::Result<PhysicalFileIdentity> {
    use std::os::unix::fs::MetadataExt;

    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
    };
    if !metadata.is_file() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    Ok(PhysicalFileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
    })
}

#[cfg(not(unix))]
pub(super) fn physical_file_identity(_file: &File) -> io::Result<PhysicalFileIdentity> {
    Err(invalid_data(ARTIFACT_VERIFICATION_FAILED))
}

pub(super) fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = match file.metadata() {
        Ok(metadata) => metadata,
        Err(_) => return Err(invalid_data(ARTIFACT_UNAVAILABLE)),
    };
    if !metadata.is_file() {
        return Err(invalid_data(ARTIFACT_UNAVAILABLE));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        Ok(FileIdentity {
            byte_count: metadata.len(),
            device: metadata.dev(),
            inode: metadata.ino(),
            modified_seconds: metadata.mtime(),
            modified_nanoseconds: metadata.mtime_nsec(),
            changed_seconds: metadata.ctime(),
            changed_nanoseconds: metadata.ctime_nsec(),
        })
    }
    #[cfg(not(unix))]
    {
        Ok(FileIdentity {
            byte_count: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
}
