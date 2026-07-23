use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

const SOURCE_WITNESS_SPOOL_MAX_BYTES_ENV: &str = "HLTHPRT_PTG2_SOURCE_WITNESS_SPOOL_MAX_BYTES";
const SOURCE_WITNESS_SCRATCH_DIR_ENV: &str = "HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR";
const DEFAULT_SOURCE_WITNESS_SPOOL_MAX_BYTES: u64 = 128 * 1024 * 1024 * 1024;

pub struct SourceWitnessScratchBudget {
    byte_limit: u64,
    used_bytes: AtomicU64,
    scratch_root: PathBuf,
}

impl SourceWitnessScratchBudget {
    pub fn from_env() -> io::Result<Arc<Self>> {
        let configured_limit = std::env::var(SOURCE_WITNESS_SPOOL_MAX_BYTES_ENV).ok();
        let byte_limit = parse_scratch_byte_limit(configured_limit.as_deref())?;
        let configured_scratch_root = std::env::var_os(SOURCE_WITNESS_SCRATCH_DIR_ENV)
            .filter(|value| !value.is_empty())
            .map(PathBuf::from);
        #[cfg(not(test))]
        let scratch_root = match configured_scratch_root {
            Some(scratch_root) => scratch_root,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "source witness scratch directory is required",
                ));
            }
        };
        #[cfg(test)]
        let scratch_root = configured_scratch_root.unwrap_or_else(std::env::temp_dir);
        Self::new(byte_limit, scratch_root)
    }

    fn new(byte_limit: u64, scratch_root: PathBuf) -> io::Result<Arc<Self>> {
        if byte_limit == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "source witness spool byte limit must be positive",
            ));
        }
        let scratch_metadata = std::fs::symlink_metadata(&scratch_root).map_err(|error| {
            io::Error::new(
                error.kind(),
                "source witness scratch directory is unavailable",
            )
        })?;
        if !scratch_metadata.is_dir() || scratch_metadata.file_type().is_symlink() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "source witness scratch directory is invalid",
            ));
        }
        Ok(Arc::new(Self {
            byte_limit,
            used_bytes: AtomicU64::new(0),
            scratch_root,
        }))
    }

    pub(crate) fn reserve(&self, byte_count: u64) -> io::Result<()> {
        self.used_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                current
                    .checked_add(byte_count)
                    .filter(|projected| *projected <= self.byte_limit)
            })
            .map(|_| ())
            .map_err(|current| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "source witness spool byte limit exceeded: used={current}, requested={byte_count}, limit={}",
                        self.byte_limit,
                    ),
                )
            })
    }

    pub(crate) fn release(&self, byte_count: u64) {
        self.used_bytes.fetch_sub(byte_count, Ordering::AcqRel);
    }

    pub fn used_bytes(&self) -> u64 {
        self.used_bytes.load(Ordering::Acquire)
    }

    pub const fn byte_limit(&self) -> u64 {
        self.byte_limit
    }

    pub fn scratch_root(&self) -> &Path {
        &self.scratch_root
    }
}

fn parse_scratch_byte_limit(value: Option<&str>) -> io::Result<u64> {
    value
        .map(str::parse::<u64>)
        .transpose()
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "source witness spool byte limit is invalid",
            )
        })
        .map(|value| value.unwrap_or(DEFAULT_SOURCE_WITNESS_SPOOL_MAX_BYTES))
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct ProviderSourceLocator {
    pub shard: u16,
    pub offset: u64,
    pub length: u32,
}

pub type RateSourceLocator = ProviderSourceLocator;

struct ProviderSpoolWriter {
    stream: BufWriter<File>,
    offset: u64,
}

pub struct ProviderSourceSpools {
    _directory: tempfile::TempDir,
    paths: Vec<std::path::PathBuf>,
    writers: Vec<Mutex<ProviderSpoolWriter>>,
    readers: OnceLock<Vec<Mutex<File>>>,
    sealed: AtomicBool,
    scratch_budget: Arc<SourceWitnessScratchBudget>,
}

pub type RateSourceSpools = ProviderSourceSpools;

impl ProviderSourceSpools {
    #[cfg(test)]
    pub fn new(shard_count: usize) -> io::Result<Self> {
        Self::new_with_budget(
            shard_count,
            "ptg2-source-witness-provider-",
            SourceWitnessScratchBudget::from_env()?,
        )
    }

    pub fn new_provider_with_budget(
        shard_count: usize,
        scratch_budget: Arc<SourceWitnessScratchBudget>,
    ) -> io::Result<Self> {
        Self::new_with_budget(shard_count, "ptg2-source-witness-provider-", scratch_budget)
    }

    pub fn new_rate_with_budget(
        shard_count: usize,
        scratch_budget: Arc<SourceWitnessScratchBudget>,
    ) -> io::Result<Self> {
        Self::new_with_budget(shard_count, "ptg2-source-witness-rate-", scratch_budget)
    }

    fn new_with_budget(
        shard_count: usize,
        prefix: &str,
        scratch_budget: Arc<SourceWitnessScratchBudget>,
    ) -> io::Result<Self> {
        if shard_count == 0 || shard_count > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "provider source spool shard count is invalid",
            ));
        }
        let directory = tempfile::Builder::new()
            .prefix(prefix)
            .tempdir_in(scratch_budget.scratch_root())?;
        let mut paths = Vec::with_capacity(shard_count);
        let mut writers = Vec::with_capacity(shard_count);
        for shard in 0..shard_count {
            let path = directory.path().join(format!("provider-{shard:04}.jsonl"));
            let file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            paths.push(path);
            writers.push(Mutex::new(ProviderSpoolWriter {
                stream: BufWriter::new(file),
                offset: 0,
            }));
        }
        Ok(Self {
            _directory: directory,
            paths,
            writers,
            readers: OnceLock::new(),
            sealed: AtomicBool::new(false),
            scratch_budget,
        })
    }

    pub fn shard_count(&self) -> usize {
        self.writers.len()
    }

    pub fn append(&self, shard: usize, raw_provider: &[u8]) -> io::Result<ProviderSourceLocator> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "provider source spools are already sealed",
            ));
        }
        let length = match u32::try_from(raw_provider.len()) {
            Ok(length) => length,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "provider source record exceeds the locator width",
                ));
            }
        };
        let Some(writer) = self.writers.get(shard) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "provider source spool shard is out of range",
            ));
        };
        let mut writer = match writer.lock() {
            Ok(writer) => writer,
            Err(_) => {
                return Err(io::Error::other("provider source spool mutex is poisoned"));
            }
        };
        let next_offset = match writer.offset.checked_add(u64::from(length)) {
            Some(next_offset) => next_offset,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "source witness spool offset overflow",
                ));
            }
        };
        self.scratch_budget.reserve(u64::from(length))?;
        let offset = writer.offset;
        if let Err(error) = writer.stream.write_all(raw_provider) {
            self.scratch_budget.release(u64::from(length));
            return Err(error);
        }
        writer.offset = next_offset;
        Ok(ProviderSourceLocator {
            shard: u16::try_from(shard).expect("validated provider spool shard"),
            offset,
            length,
        })
    }

    pub fn seal(&self) -> io::Result<()> {
        if self.sealed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        for writer in &self.writers {
            let mut writer = match writer.lock() {
                Ok(writer) => writer,
                Err(_) => {
                    return Err(io::Error::other("provider source spool mutex is poisoned"));
                }
            };
            writer.stream.flush()?;
            writer.stream.get_ref().sync_all()?;
        }
        let readers = self
            .paths
            .iter()
            .map(|path| File::open(path).map(Mutex::new))
            .collect::<io::Result<Vec<_>>>()?;
        match self.readers.set(readers) {
            Ok(()) => Ok(()),
            Err(_) => Err(io::Error::other(
                "provider source spool readers already initialized",
            )),
        }
    }

    #[cfg(test)]
    pub fn read(&self, locator: ProviderSourceLocator) -> io::Result<Vec<u8>> {
        self.read_bounded(locator, u32::MAX as usize)
    }

    pub fn read_bounded(
        &self,
        locator: ProviderSourceLocator,
        maximum_bytes: usize,
    ) -> io::Result<Vec<u8>> {
        if !self.sealed.load(Ordering::Acquire) {
            return Err(io::Error::other("provider source spools are not sealed"));
        }
        if locator.length as usize > maximum_bytes {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "source witness evidence is {} bytes, exceeding the fail-closed {maximum_bytes}-byte decoded evidence limit",
                    locator.length,
                ),
            ));
        }
        let Some(readers) = self.readers.get() else {
            return Err(io::Error::other(
                "provider source spool readers are unavailable",
            ));
        };
        let Some(reader) = readers.get(locator.shard as usize) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "provider source locator shard is out of range",
            ));
        };
        let mut reader = match reader.lock() {
            Ok(reader) => reader,
            Err(_) => {
                return Err(io::Error::other(
                    "provider source spool reader mutex is poisoned",
                ));
            }
        };
        reader.seek(SeekFrom::Start(locator.offset))?;
        let mut raw_provider = vec![0u8; locator.length as usize];
        reader.read_exact(&mut raw_provider)?;
        Ok(raw_provider)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_spool_round_trips_records_across_shards() {
        let spools = ProviderSourceSpools::new(2).unwrap();
        let first = spools.append(0, br#"{"provider_group_id":1}"#).unwrap();
        let second = spools.append(1, br#"{"provider_group_id":2}"#).unwrap();
        spools.seal().unwrap();

        assert_eq!(spools.read(first).unwrap(), br#"{"provider_group_id":1}"#);
        assert_eq!(spools.read(second).unwrap(), br#"{"provider_group_id":2}"#);
    }

    #[test]
    fn bounded_read_rejects_before_allocating_the_locator_length() {
        let spools = ProviderSourceSpools::new(1).unwrap();
        let locator = spools.append(0, b"oversized").unwrap();
        spools.seal().unwrap();

        let error = spools.read_bounded(locator, 4).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("fail-closed"));
    }

    #[test]
    fn provider_and_rate_spools_share_one_hard_scratch_budget() {
        let scratch_root = tempfile::tempdir().unwrap();
        let budget = Arc::new(SourceWitnessScratchBudget {
            byte_limit: 5,
            used_bytes: AtomicU64::new(0),
            scratch_root: scratch_root.path().to_path_buf(),
        });
        let provider =
            ProviderSourceSpools::new_provider_with_budget(1, Arc::clone(&budget)).unwrap();
        let rate = RateSourceSpools::new_rate_with_budget(1, Arc::clone(&budget)).unwrap();

        provider.append(0, b"abc").unwrap();
        let error = rate.append(0, b"def").unwrap_err();

        assert_eq!(budget.used_bytes(), 3);
        assert!(error.to_string().contains("spool byte limit exceeded"));
    }

    #[test]
    fn scratch_configuration_and_append_failures_release_reserved_bytes() {
        assert_eq!(
            parse_scratch_byte_limit(None).unwrap(),
            DEFAULT_SOURCE_WITNESS_SPOOL_MAX_BYTES,
        );
        assert_eq!(parse_scratch_byte_limit(Some("7")).unwrap(), 7);
        assert!(parse_scratch_byte_limit(Some("invalid")).is_err());

        let scratch_root = tempfile::tempdir().unwrap();
        assert!(SourceWitnessScratchBudget::new(0, scratch_root.path().to_path_buf()).is_err());
        assert!(SourceWitnessScratchBudget::new(1, scratch_root.path().join("missing"),).is_err());
        let ordinary_file = scratch_root.path().join("ordinary-file");
        std::fs::write(&ordinary_file, b"not a directory").unwrap();
        assert!(SourceWitnessScratchBudget::new(1, ordinary_file.clone()).is_err());

        let budget =
            SourceWitnessScratchBudget::new(16, scratch_root.path().to_path_buf()).unwrap();
        let spools =
            ProviderSourceSpools::new_provider_with_budget(1, Arc::clone(&budget)).unwrap();
        {
            let mut writer = spools.writers[0].lock().unwrap();
            writer.offset = u64::MAX;
        }
        assert!(spools.append(0, b"x").is_err());
        assert_eq!(budget.used_bytes(), 0);

        {
            let mut writer = spools.writers[0].lock().unwrap();
            writer.offset = 0;
            writer.stream = BufWriter::with_capacity(0, File::open(&ordinary_file).unwrap());
        }
        assert!(spools.append(0, b"x").is_err());
        assert_eq!(budget.used_bytes(), 0);
    }
}
