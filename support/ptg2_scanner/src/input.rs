//! Input readers for compressed and plain TiC artifacts.

use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

struct CountingReader<R: Read> {
    inner: R,
    bytes_read: Arc<AtomicU64>,
}

impl<R: Read> CountingReader<R> {
    fn new(inner: R, bytes_read: Arc<AtomicU64>) -> Self {
        Self { inner, bytes_read }
    }
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        if read > 0 {
            self.bytes_read.fetch_add(read as u64, Ordering::Relaxed);
        }
        Ok(read)
    }
}

fn is_gzip(path: &Path) -> io::Result<bool> {
    if path
        .extension()
        .and_then(|value| value.to_str())
        .map(|value| value.eq_ignore_ascii_case("gz"))
        .unwrap_or(false)
    {
        return Ok(true);
    }
    let mut fp = File::open(path)?;
    let mut header = [0u8; 2];
    let read = fp.read(&mut header)?;
    Ok(read == 2 && header == [0x1f, 0x8b])
}

pub fn open_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
) -> io::Result<Box<dyn Read>> {
    let fp = File::open(path)?;
    if is_gzip(path)? {
        let compressed_reader = CountingReader::new(BufReader::new(fp), compressed_bytes_read);
        Ok(Box::new(MultiGzDecoder::new(compressed_reader)))
    } else {
        Ok(Box::new(CountingReader::new(
            BufReader::new(fp),
            compressed_bytes_read,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::Write;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ptg2_scanner_input_test_{nanos}_{suffix}"))
    }

    #[test]
    fn open_reader_reads_plain_files_and_counts_bytes() {
        let path = temp_path("plain.json");
        std::fs::write(&path, b"{\"ok\":true}").expect("write plain test file");
        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader = open_reader(&path, Arc::clone(&bytes_read)).expect("open plain reader");
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read plain file");
        assert_eq!(text, "{\"ok\":true}");
        assert_eq!(bytes_read.load(Ordering::Relaxed), 11);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn open_reader_decompresses_gzip_files_and_counts_compressed_bytes() {
        let path = temp_path("compressed.json.gz");
        let file = File::create(&path).expect("create gzip test file");
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder
            .write_all(b"{\"ok\":true}")
            .expect("write gzip payload");
        encoder.finish().expect("finish gzip payload");

        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader = open_reader(&path, Arc::clone(&bytes_read)).expect("open gzip reader");
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read gzip file");
        assert_eq!(text, "{\"ok\":true}");
        assert!(bytes_read.load(Ordering::Relaxed) > 0);
        std::fs::remove_file(path).ok();
    }
}
