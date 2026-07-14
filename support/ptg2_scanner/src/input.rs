//! Input readers for compressed and plain TiC artifacts.

use crate::config::READ_BUF_SIZE;
use flate2::read::MultiGzDecoder;
use std::fs::File;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::path::Path;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

mod rapidgzip;

pub use rapidgzip::{
    open_full_scan_json_reader, open_full_scan_reader, open_full_scan_reader_exporting_index,
    open_indexed_ranges_reader, RapidgzipConfig,
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

struct StrictUtf8Reader<R: Read> {
    inner: R,
    pending: Vec<u8>,
    output: Vec<u8>,
    output_pos: usize,
    eof: bool,
    checked_bom: bool,
}

impl<R: Read> StrictUtf8Reader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            pending: Vec::with_capacity(4),
            output: Vec::new(),
            output_pos: 0,
            eof: false,
            checked_bom: false,
        }
    }

    fn preserving_bom(inner: R) -> Self {
        Self {
            inner,
            pending: Vec::with_capacity(4),
            output: Vec::new(),
            output_pos: 0,
            eof: false,
            checked_bom: true,
        }
    }

    fn append_initial_bytes(&mut self, bytes: &[u8], eof: bool) -> io::Result<()> {
        self.checked_bom = true;
        let json_bytes = bytes.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(bytes);
        self.append_valid_utf8(json_bytes, eof)
    }

    fn append_valid_utf8(&mut self, bytes: &[u8], eof: bool) -> io::Result<()> {
        match std::str::from_utf8(bytes) {
            Ok(valid) => {
                self.output.extend_from_slice(valid.as_bytes());
                self.pending.clear();
                Ok(())
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                self.output.extend_from_slice(&bytes[..valid_up_to]);
                if error.error_len().is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PTG JSON contains invalid UTF-8",
                    ));
                }
                self.pending.clear();
                if eof {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PTG JSON ends with incomplete UTF-8",
                    ));
                }
                self.pending.extend_from_slice(&bytes[valid_up_to..]);
                Ok(())
            }
        }
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
                    if self.checked_bom {
                        self.append_valid_utf8(&pending, true)?;
                    } else {
                        self.append_initial_bytes(&pending, true)?;
                    }
                }
                break;
            }
            if !self.checked_bom {
                let mut initial_bytes = std::mem::take(&mut self.pending);
                initial_bytes.extend_from_slice(&raw[..read]);
                if initial_bytes.len() < 3 {
                    self.pending = initial_bytes;
                    continue;
                }
                self.append_initial_bytes(&initial_bytes, false)?;
                continue;
            }
            if self.pending.is_empty() {
                self.append_valid_utf8(&raw[..read], false)?;
            } else {
                let mut bytes = Vec::with_capacity(self.pending.len() + read);
                bytes.extend_from_slice(&self.pending);
                bytes.extend_from_slice(&raw[..read]);
                self.append_valid_utf8(&bytes, false)?;
            }
        }
        Ok(!self.output.is_empty())
    }
}

impl<R: Read> Read for StrictUtf8Reader<R> {
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

pub fn is_gzip(path: &Path) -> io::Result<bool> {
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
    Ok(strict_utf8_reader(open_reader(
        path,
        compressed_bytes_read,
    )?))
}

pub fn open_plain_range_json_reader(
    path: &Path,
    offset: u64,
    length: u64,
    bytes_read: Arc<AtomicU64>,
) -> io::Result<Box<dyn Read>> {
    if is_gzip(path)? {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "plain JSON range reads do not support gzip input",
        ));
    }
    let mut file = File::open(path)?;
    let file_bytes = file.metadata()?.len();
    let end = offset.checked_add(length).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "plain JSON range overflows u64",
        )
    })?;
    if length == 0 || end > file_bytes {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("plain JSON range {offset}+{length} exceeds {file_bytes} bytes"),
        ));
    }
    file.seek(SeekFrom::Start(offset))?;
    let reader = CountingReader::new(
        BufReader::with_capacity(READ_BUF_SIZE, file.take(length)),
        bytes_read,
    );
    Ok(strict_utf8_reader_preserving_bom(reader))
}

pub fn strict_utf8_reader<R: Read + 'static>(inner: R) -> Box<dyn Read> {
    Box::new(StrictUtf8Reader::new(inner))
}

pub fn strict_utf8_reader_preserving_bom<R: Read + 'static>(inner: R) -> Box<dyn Read> {
    Box::new(StrictUtf8Reader::preserving_bom(inner))
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
    fn open_json_reader_rejects_invalid_utf8_without_loading_file() {
        let path = temp_path("invalid_utf8.json");
        std::fs::write(&path, b"{\"name\":\"A\xffB\"}").expect("write invalid utf8 test file");
        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader =
            open_json_reader(&path, Arc::clone(&bytes_read)).expect("open json reader");
        let mut text = String::new();
        let error = reader.read_to_string(&mut text).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert_eq!(bytes_read.load(Ordering::Relaxed), 14);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn open_json_reader_strips_utf8_bom() {
        let path = temp_path("bom.json");
        std::fs::write(&path, b"\xEF\xBB\xBF{\"ok\":true}").expect("write bom test file");
        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader =
            open_json_reader(&path, Arc::clone(&bytes_read)).expect("open json reader");
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read json file");
        assert_eq!(text, "{\"ok\":true}");
        assert_eq!(bytes_read.load(Ordering::Relaxed), 14);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn strict_utf8_reader_strips_bom_split_across_reads() {
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

        let payload = b"\xEF\xBB\xBF{\"ok\":true}".to_vec();
        let mut reader = strict_utf8_reader(OneByteReader(Cursor::new(payload)));
        let mut text = String::new();
        reader.read_to_string(&mut text).expect("read split bom");
        assert_eq!(text, "{\"ok\":true}");
    }

    #[test]
    fn strict_utf8_reader_preserves_multibyte_sequences_across_reads() {
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
            StrictUtf8Reader::new(OneByteReader(Cursor::new("AéB".as_bytes().to_vec())));
        let mut text = String::new();
        reader
            .read_to_string(&mut text)
            .expect("read strict utf8 stream");
        assert_eq!(text, "AéB");
    }

    #[test]
    fn strict_utf8_reader_rejects_invalid_sequences() {
        let mut reader = strict_utf8_reader(Cursor::new(vec![b'A', 0xff, b'B']));
        let mut text = String::new();
        let error = reader.read_to_string(&mut text).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn strict_utf8_reader_rejects_incomplete_sequences_at_eof() {
        let mut reader = strict_utf8_reader(Cursor::new(vec![b'A', 0xc3]));
        let mut text = String::new();
        let error = reader.read_to_string(&mut text).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn strict_utf8_reader_can_preserve_bom_for_offset_sensitive_scans() {
        let payload = b"\xEF\xBB\xBF{\"ok\":true}".to_vec();
        let mut reader = strict_utf8_reader_preserving_bom(Cursor::new(payload.clone()));
        let mut output = Vec::new();
        reader.read_to_end(&mut output).expect("read valid UTF-8");
        assert_eq!(output, payload);
    }
}
