use std::fs::{File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProviderSourceLocator {
    pub shard: u16,
    pub offset: u64,
    pub length: u32,
}

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
}

impl ProviderSourceSpools {
    pub fn new(shard_count: usize) -> io::Result<Self> {
        if shard_count == 0 || shard_count > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "provider source spool shard count is invalid",
            ));
        }
        let directory = tempfile::Builder::new()
            .prefix("ptg2-source-witness-provider-")
            .tempdir()?;
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
        })
    }

    pub fn append(&self, shard: usize, raw_provider: &[u8]) -> io::Result<ProviderSourceLocator> {
        if self.sealed.load(Ordering::Acquire) {
            return Err(io::Error::other(
                "provider source spools are already sealed",
            ));
        }
        let length = u32::try_from(raw_provider.len()).map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "provider source record exceeds the locator width",
            )
        })?;
        let writer = self.writers.get(shard).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "provider source spool shard is out of range",
            )
        })?;
        let mut writer = writer
            .lock()
            .map_err(|_| io::Error::other("provider source spool mutex is poisoned"))?;
        let offset = writer.offset;
        writer.stream.write_all(raw_provider)?;
        writer.offset = writer.offset.saturating_add(raw_provider.len() as u64);
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
            let mut writer = writer
                .lock()
                .map_err(|_| io::Error::other("provider source spool mutex is poisoned"))?;
            writer.stream.flush()?;
            writer.stream.get_ref().sync_all()?;
        }
        let readers = self
            .paths
            .iter()
            .map(|path| File::open(path).map(Mutex::new))
            .collect::<io::Result<Vec<_>>>()?;
        self.readers
            .set(readers)
            .map_err(|_| io::Error::other("provider source spool readers already initialized"))
    }

    pub fn read(&self, locator: ProviderSourceLocator) -> io::Result<Vec<u8>> {
        if !self.sealed.load(Ordering::Acquire) {
            return Err(io::Error::other("provider source spools are not sealed"));
        }
        let readers = self
            .readers
            .get()
            .ok_or_else(|| io::Error::other("provider source spool readers are unavailable"))?;
        let reader = readers.get(locator.shard as usize).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                "provider source locator shard is out of range",
            )
        })?;
        let mut reader = reader
            .lock()
            .map_err(|_| io::Error::other("provider source spool reader mutex is poisoned"))?;
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
}
