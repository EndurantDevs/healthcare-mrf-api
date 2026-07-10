//! Input readers for compressed and plain TiC artifacts.

use crate::config::READ_BUF_SIZE;
use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{self, BufReader, Read};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

mod rapidgzip;

pub use rapidgzip::{open_full_scan_json_reader, open_full_scan_reader, RapidgzipConfig};

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

struct LossyUtf8Reader<R: Read> {
    inner: R,
    pending: Vec<u8>,
    output: Vec<u8>,
    output_pos: usize,
    eof: bool,
}

impl<R: Read> LossyUtf8Reader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            pending: Vec::with_capacity(4),
            output: Vec::new(),
            output_pos: 0,
            eof: false,
        }
    }

    fn append_valid_utf8_lossy(&mut self, bytes: &[u8], eof: bool) {
        let mut pos = 0usize;
        while pos < bytes.len() {
            match std::str::from_utf8(&bytes[pos..]) {
                Ok(valid) => {
                    self.output.extend_from_slice(valid.as_bytes());
                    self.pending.clear();
                    return;
                }
                Err(error) => {
                    let valid_up_to = error.valid_up_to();
                    if valid_up_to > 0 {
                        self.output
                            .extend_from_slice(&bytes[pos..pos + valid_up_to]);
                        pos += valid_up_to;
                    }
                    match error.error_len() {
                        Some(error_len) => {
                            self.output.extend_from_slice(
                                char::REPLACEMENT_CHARACTER
                                    .encode_utf8(&mut [0; 4])
                                    .as_bytes(),
                            );
                            pos += error_len;
                        }
                        None => {
                            self.pending.clear();
                            if eof {
                                let text = String::from_utf8_lossy(&bytes[pos..]);
                                self.output.extend_from_slice(text.as_bytes());
                            } else {
                                self.pending.extend_from_slice(&bytes[pos..]);
                            }
                            return;
                        }
                    }
                }
            }
        }
        self.pending.clear();
    }

    fn fill_output(&mut self) -> io::Result<bool> {
        self.output.clear();
        self.output_pos = 0;
        let mut raw = [0u8; 8192];
        while self.output.is_empty() && !self.eof {
            let read = self.inner.read(&mut raw)?;
            if read == 0 {
                self.eof = true;
                if !self.pending.is_empty() {
                    let pending = std::mem::take(&mut self.pending);
                    self.append_valid_utf8_lossy(&pending, true);
                }
                break;
            }
            if self.pending.is_empty() {
                self.append_valid_utf8_lossy(&raw[..read], false);
            } else {
                let mut bytes = Vec::with_capacity(self.pending.len() + read);
                bytes.extend_from_slice(&self.pending);
                bytes.extend_from_slice(&raw[..read]);
                self.append_valid_utf8_lossy(&bytes, false);
            }
        }
        Ok(!self.output.is_empty())
    }
}

impl<R: Read> Read for LossyUtf8Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        if self.output_pos >= self.output.len() && !self.fill_output()? {
            return Ok(0);
        }
        let available = &self.output[self.output_pos..];
        let count = available.len().min(buf.len());
        buf[..count].copy_from_slice(&available[..count]);
        self.output_pos += count;
        Ok(count)
    }
}

pub(super) fn is_gzip(path: &Path) -> io::Result<bool> {
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
        let compressed_reader = CountingReader::new(
            BufReader::with_capacity(READ_BUF_SIZE, fp),
            compressed_bytes_read,
        );
        Ok(Box::new(MultiGzDecoder::new(compressed_reader)))
    } else {
        Ok(Box::new(CountingReader::new(
            BufReader::with_capacity(READ_BUF_SIZE, fp),
            compressed_bytes_read,
        )))
    }
}

pub fn open_json_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
) -> io::Result<Box<dyn Read>> {
    Ok(lossy_utf8_reader(open_reader(path, compressed_bytes_read)?))
}

pub fn lossy_utf8_reader<R: Read + 'static>(inner: R) -> Box<dyn Read> {
    Box::new(LossyUtf8Reader::new(inner))
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{write::GzEncoder, Compression};
    use std::io::{Cursor, Write};
    use std::time::{SystemTime, UNIX_EPOCH};

    fn temp_path(suffix: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("ptg2_scanner_input_test_{nanos}_{suffix}"))
    }

    fn write_gzip(path: &Path, payload: &[u8]) {
        let file = File::create(path).expect("create gzip test file");
        let mut encoder = GzEncoder::new(file, Compression::default());
        encoder.write_all(payload).expect("write gzip payload");
        encoder.finish().expect("finish gzip payload");
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
        write_gzip(&path, b"{\"ok\":true}");

        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader = open_reader(&path, Arc::clone(&bytes_read)).expect("open gzip reader");
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read gzip file");
        assert_eq!(text, "{\"ok\":true}");
        assert!(bytes_read.load(Ordering::Relaxed) > 0);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn open_json_reader_replaces_invalid_utf8_without_loading_file() {
        let path = temp_path("invalid_utf8.json");
        std::fs::write(&path, b"{\"name\":\"A\xffB\"}").expect("write invalid utf8 test file");
        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader =
            open_json_reader(&path, Arc::clone(&bytes_read)).expect("open json reader");
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read json file");
        assert_eq!(text, "{\"name\":\"A\u{FFFD}B\"}");
        assert_eq!(bytes_read.load(Ordering::Relaxed), 14);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn lossy_utf8_reader_preserves_multibyte_sequences_across_reads() {
        struct OneByteReader(Cursor<Vec<u8>>);

        impl Read for OneByteReader {
            fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
                if buf.is_empty() {
                    return Ok(0);
                }
                let mut one = [0u8; 1];
                let read = self.0.read(&mut one)?;
                if read > 0 {
                    buf[0] = one[0];
                }
                Ok(read)
            }
        }

        let mut reader =
            LossyUtf8Reader::new(OneByteReader(Cursor::new("AéB".as_bytes().to_vec())));
        let mut text = String::new();
        reader
            .read_to_string(&mut text)
            .expect("read lossy utf8 stream");
        assert_eq!(text, "AéB");
    }
}
