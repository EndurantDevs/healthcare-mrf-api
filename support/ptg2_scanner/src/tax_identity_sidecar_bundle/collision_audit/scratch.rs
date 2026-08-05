use super::invalid_data;
use sha2::{Digest, Sha256};
use std::cell::RefCell;
use std::collections::HashSet;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

const SCRATCH_UNAVAILABLE: &str = "PTG tax identity collision audit scratch is unavailable";
const SCRATCH_LIMIT_EXCEEDED: &str = "PTG tax identity collision audit scratch byte limit exceeded";
const SCRATCH_ACCOUNTING_INVALID: &str =
    "PTG tax identity collision audit scratch accounting is invalid";
const SCRATCH_CAPACITY_INSUFFICIENT: &str =
    "PTG tax identity collision audit scratch capacity is insufficient";
const MAX_NAME_ATTEMPTS: usize = 64;
const HASH_BUFFER_BYTES: usize = 64 * 1024;
static NEXT_SCRATCH_ID: AtomicU64 = AtomicU64::new(0);

#[cfg(test)]
mod budget_guard_tests;
#[cfg(test)]
mod file_tests;
mod platform;

#[derive(Clone, Copy)]
pub(super) enum ScratchRunKind {
    Initial,
    Merge,
}

pub(super) type ScratchPollCallback<'a> = dyn FnMut() -> io::Result<()> + 'a;

impl ScratchRunKind {
    const fn label(self) -> &'static str {
        match self {
            Self::Initial => "initial",
            Self::Merge => "merge",
        }
    }
}

pub(super) struct PendingScratchRun {
    name: String,
    byte_count: u64,
}

#[derive(Clone, Eq, PartialEq)]
pub(super) struct ScratchRun {
    name: String,
    byte_count: u64,
    identity: ScratchFileIdentity,
    sha256: [u8; 32],
}

impl ScratchRun {
    pub(super) const fn byte_count(&self) -> u64 {
        self.byte_count
    }
}

impl fmt::Debug for ScratchRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ScratchRun")
            .field("name", &"<redacted>")
            .field("byte_count", &self.byte_count)
            .field("identity", &"<redacted>")
            .field("sha256", &"<redacted>")
            .finish()
    }
}

#[derive(Clone, Eq, PartialEq)]
struct ScratchFileIdentity {
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

pub(super) struct ScratchBudget {
    limit: u64,
    used: u64,
    peak: u64,
}

impl ScratchBudget {
    pub(super) const fn new(limit: u64) -> Self {
        Self {
            limit,
            used: 0,
            peak: 0,
        }
    }

    pub(super) fn reserve(&mut self, byte_count: u64) -> io::Result<()> {
        let projected = self
            .used
            .checked_add(byte_count)
            .ok_or_else(|| invalid_data(SCRATCH_ACCOUNTING_INVALID))?;
        if projected > self.limit {
            return Err(invalid_data(SCRATCH_LIMIT_EXCEEDED));
        }
        self.used = projected;
        self.peak = self.peak.max(projected);
        Ok(())
    }

    pub(super) fn release(&mut self, byte_count: u64) -> io::Result<()> {
        self.used = self
            .used
            .checked_sub(byte_count)
            .ok_or_else(|| invalid_data(SCRATCH_ACCOUNTING_INVALID))?;
        Ok(())
    }

    pub(super) const fn used(&self) -> u64 {
        self.used
    }

    pub(super) const fn peak(&self) -> u64 {
        self.peak
    }
}

pub(super) struct PrivateScratch {
    #[cfg(unix)]
    root_directory: File,
    #[cfg(unix)]
    directory: File,
    #[cfg(unix)]
    directory_name: String,
    #[cfg(not(unix))]
    directory: tempfile::TempDir,
    tracked_names: RefCell<HashSet<String>>,
}

impl fmt::Debug for PrivateScratch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrivateScratch")
            .field("directory", &"<redacted>")
            .field("tracked_file_count", &self.tracked_names.borrow().len())
            .finish()
    }
}

impl PrivateScratch {
    pub(super) fn create(
        root: &Path,
        required_bytes: u64,
        minimum_free_bytes: u64,
    ) -> io::Result<Self> {
        let scratch = Self::create_inner(root)?;
        let required_capacity = required_bytes
            .checked_add(minimum_free_bytes)
            .ok_or_else(|| invalid_data(SCRATCH_CAPACITY_INSUFFICIENT))?;
        if scratch.available_bytes()? < required_capacity {
            return Err(invalid_data(SCRATCH_CAPACITY_INSUFFICIENT));
        }
        Ok(scratch)
    }

    pub(super) fn create_run(
        &self,
        kind: ScratchRunKind,
        byte_count: u64,
    ) -> io::Result<(PendingScratchRun, File)> {
        for _ in 0..MAX_NAME_ATTEMPTS {
            let sequence = NEXT_SCRATCH_ID.fetch_add(1, Ordering::Relaxed);
            let name = format!(".tax-collision-{}-{sequence:016x}.run", kind.label());
            validate_run_name(&name)?;
            match self.create_new_file(&name) {
                Ok(file) => {
                    self.tracked_names.borrow_mut().insert(name.clone());
                    if self.secure_created_file(&file).is_err() {
                        let _ = self.unlink(&name);
                        self.tracked_names.borrow_mut().remove(&name);
                        return Err(invalid_data(SCRATCH_UNAVAILABLE));
                    }
                    return Ok((PendingScratchRun { name, byte_count }, file));
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(_) => return Err(invalid_data(SCRATCH_UNAVAILABLE)),
            }
        }
        Err(invalid_data(SCRATCH_UNAVAILABLE))
    }

    pub(super) fn seal_run(
        &self,
        pending: PendingScratchRun,
        file: &mut File,
        poll: &mut ScratchPollCallback<'_>,
    ) -> io::Result<ScratchRun> {
        validate_run_name(&pending.name)?;
        if !self.tracked_names.borrow().contains(&pending.name) {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        let identity = file_identity(file)?;
        if identity.byte_count != pending.byte_count {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        let sha256 = hash_open_file(file, pending.byte_count, poll)?;
        if file_identity(file)? != identity {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(ScratchRun {
            name: pending.name,
            byte_count: pending.byte_count,
            identity,
            sha256,
        })
    }

    pub(super) fn open_run(
        &self,
        run: &ScratchRun,
        poll: &mut ScratchPollCallback<'_>,
    ) -> io::Result<File> {
        validate_run_name(&run.name)?;
        if !self.tracked_names.borrow().contains(&run.name) {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        let mut file = self.open_existing_file(&run.name)?;
        self.reauthenticate_run(&mut file, run, poll)?;
        Ok(file)
    }

    pub(super) fn reauthenticate_run(
        &self,
        file: &mut File,
        run: &ScratchRun,
        poll: &mut ScratchPollCallback<'_>,
    ) -> io::Result<()> {
        if !self.tracked_names.borrow().contains(&run.name)
            || file_identity(file)? != run.identity
            || hash_open_file(file, run.byte_count, poll)? != run.sha256
            || file_identity(file)? != run.identity
        {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(())
    }

    pub(super) fn remove_run(
        &self,
        run: &ScratchRun,
        poll: &mut ScratchPollCallback<'_>,
    ) -> io::Result<()> {
        drop(self.open_run(run, poll)?);
        self.unlink(&run.name)?;
        self.tracked_names.borrow_mut().remove(&run.name);
        Ok(())
    }
}

fn validate_run_name(name: &str) -> io::Result<()> {
    if name.is_empty()
        || name.len() > 96
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
    {
        return Err(invalid_data(SCRATCH_UNAVAILABLE));
    }
    Ok(())
}

fn hash_open_file(
    file: &mut File,
    expected_byte_count: u64,
    poll: &mut ScratchPollCallback<'_>,
) -> io::Result<[u8; 32]> {
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
    let mut hasher = Sha256::new();
    let mut buffer = [0u8; HASH_BUFFER_BYTES];
    let mut remaining = expected_byte_count;
    while remaining != 0 {
        let requested = usize::try_from(remaining.min(HASH_BUFFER_BYTES as u64))
            .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
        let read = file
            .read(&mut buffer[..requested])
            .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
        if read == 0 {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        hasher.update(&buffer[..read]);
        remaining = remaining
            .checked_sub(read as u64)
            .ok_or_else(|| invalid_data(SCRATCH_UNAVAILABLE))?;
        poll()?;
    }
    let mut trailing = [0u8; 1];
    if file
        .read(&mut trailing)
        .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?
        != 0
    {
        return Err(invalid_data(SCRATCH_UNAVAILABLE));
    }
    file.seek(SeekFrom::Start(0))
        .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
    Ok(hasher.finalize().into())
}

fn file_identity(file: &File) -> io::Result<ScratchFileIdentity> {
    let metadata = file
        .metadata()
        .map_err(|_| invalid_data(SCRATCH_UNAVAILABLE))?;
    if !metadata.is_file() {
        return Err(invalid_data(SCRATCH_UNAVAILABLE));
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;

        if metadata.mode() & 0o777 != 0o600
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.nlink() != 1
        {
            return Err(invalid_data(SCRATCH_UNAVAILABLE));
        }
        Ok(ScratchFileIdentity {
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
        Ok(ScratchFileIdentity {
            byte_count: metadata.len(),
            modified: metadata.modified().ok(),
        })
    }
}
