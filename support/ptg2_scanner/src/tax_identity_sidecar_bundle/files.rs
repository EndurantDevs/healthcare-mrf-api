use super::invalid_data;
use sha2::{Digest, Sha256};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

pub(super) const HASH_BUFFER_BYTES: usize = 64 * 1024;
pub(super) const UNAVAILABLE_ARTIFACT: &str = "PTG tax identity sidecar artifact is unavailable";
pub(super) const ARTIFACT_SIZE_MISMATCH: &str =
    "PTG tax identity sidecar artifact size does not match";
pub(super) const ARTIFACT_DIGEST_MISMATCH: &str =
    "PTG tax identity sidecar artifact digest does not match";

#[derive(Clone, Eq, PartialEq)]
pub(super) struct FileIdentity {
    byte_count: u64,
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

pub(super) fn open_authentic_artifact(
    path: &Path,
    expected_byte_count: u64,
    expected_digest: [u8; 32],
) -> io::Result<(File, [u8; 32], FileIdentity)> {
    let mut file = open_read_only_artifact(path)?;
    let metadata = file
        .metadata()
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?;
    if !metadata.is_file() {
        return Err(invalid_data(UNAVAILABLE_ARTIFACT));
    }
    let identity = file_identity(&file)?;
    if identity.byte_count != expected_byte_count {
        return Err(invalid_data(ARTIFACT_SIZE_MISMATCH));
    }
    let observed_digest = hash_open_file(&mut file, expected_byte_count)?;
    if file_identity(&file)? != identity || observed_digest != expected_digest {
        return Err(invalid_data(ARTIFACT_DIGEST_MISMATCH));
    }
    Ok((file, observed_digest, identity))
}

pub(super) fn reauthenticate_artifact(
    file: &mut File,
    expected_identity: &FileIdentity,
    expected_byte_count: u64,
    expected_digest: [u8; 32],
) -> io::Result<()> {
    if expected_identity.byte_count != expected_byte_count
        || &file_identity(file)? != expected_identity
    {
        return Err(invalid_data(ARTIFACT_DIGEST_MISMATCH));
    }
    let observed_digest = hash_open_file(file, expected_byte_count)?;
    if &file_identity(file)? != expected_identity || observed_digest != expected_digest {
        return Err(invalid_data(ARTIFACT_DIGEST_MISMATCH));
    }
    Ok(())
}

pub(super) fn hash_open_file(file: &mut File, expected_byte_count: u64) -> io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    let mut remaining = expected_byte_count;
    while remaining != 0 {
        let requested = usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64))
            .map_err(|_| invalid_data(ARTIFACT_SIZE_MISMATCH))?;
        let read = file
            .read(&mut buffer[..requested])
            .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?;
        if read == 0 {
            return Err(invalid_data(ARTIFACT_SIZE_MISMATCH));
        }
        hasher.update(&buffer[..read]);
        remaining -= read as u64;
    }
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?
        != 0
    {
        return Err(invalid_data(ARTIFACT_SIZE_MISMATCH));
    }
    let observed_digest: [u8; 32] = hasher.finalize().into();
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?;
    Ok(observed_digest)
}

fn open_read_only_artifact(path: &Path) -> io::Result<File> {
    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;

        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    options
        .open(path)
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))
}

fn file_identity(file: &File) -> io::Result<FileIdentity> {
    let metadata = file
        .metadata()
        .map_err(|_| invalid_data(UNAVAILABLE_ARTIFACT))?;
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
