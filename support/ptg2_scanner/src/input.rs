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

// Valid input is read and validated directly in the caller's buffer. These
// fixed-size buffers are only used while resolving the BOM or a split codepoint.
struct StrictUtf8Reader<R: Read> {
    inner: R,
    pending: [u8; 4],
    pending_len: usize,
    output: [u8; 4],
    output_pos: usize,
    output_len: usize,
    eof: bool,
    checked_bom: bool,
}

impl<R: Read> StrictUtf8Reader<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            pending: [0; 4],
            pending_len: 0,
            output: [0; 4],
            output_pos: 0,
            output_len: 0,
            eof: false,
            checked_bom: false,
        }
    }

    fn preserving_bom(inner: R) -> Self {
        Self {
            inner,
            pending: [0; 4],
            pending_len: 0,
            output: [0; 4],
            output_pos: 0,
            output_len: 0,
            eof: false,
            checked_bom: true,
        }
    }

    fn stage_valid_utf8(&mut self, bytes: &[u8], eof: bool) -> io::Result<()> {
        debug_assert!(bytes.len() <= self.output.len());
        self.output_pos = 0;
        self.output_len = 0;
        self.pending_len = 0;
        match std::str::from_utf8(bytes) {
            Ok(_) => {
                self.output[..bytes.len()].copy_from_slice(bytes);
                self.output_len = bytes.len();
                Ok(())
            }
            Err(error) => {
                let valid_up_to = error.valid_up_to();
                if error.error_len().is_some() {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PTG JSON contains invalid UTF-8",
                    ));
                }
                if eof {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PTG JSON ends with incomplete UTF-8",
                    ));
                }
                self.output[..valid_up_to].copy_from_slice(&bytes[..valid_up_to]);
                self.output_len = valid_up_to;
                let pending = &bytes[valid_up_to..];
                self.pending[..pending.len()].copy_from_slice(pending);
                self.pending_len = pending.len();
                Ok(())
            }
        }
    }

    fn initialize_bom(&mut self) -> io::Result<()> {
        while self.pending_len < 3 && !self.eof {
            let read = self.inner.read(&mut self.pending[self.pending_len..3])?;
            if read == 0 {
                self.eof = true;
            } else {
                self.pending_len += read;
            }
        }
        self.checked_bom = true;
        let mut initial = [0; 3];
        let initial_len = self.pending_len;
        initial[..initial_len].copy_from_slice(&self.pending[..initial_len]);
        self.pending_len = 0;
        let json_bytes = initial[..initial_len]
            .strip_prefix(b"\xEF\xBB\xBF")
            .unwrap_or(&initial[..initial_len]);
        self.stage_valid_utf8(json_bytes, self.eof)
    }

    fn complete_pending(&mut self) -> io::Result<()> {
        while self.pending_len > 0 {
            let read = self.inner.read(&mut self.pending[self.pending_len..])?;
            if read == 0 {
                self.eof = true;
                self.pending_len = 0;
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "PTG JSON ends with incomplete UTF-8",
                ));
            }
            self.pending_len += read;
            let mut staged = [0; 4];
            let staged_len = self.pending_len;
            staged[..staged_len].copy_from_slice(&self.pending[..staged_len]);
            self.stage_valid_utf8(&staged[..staged_len], false)?;
            if self.output_len > 0 {
                return Ok(());
            }
        }
        Ok(())
    }
}

impl<R: Read> Read for StrictUtf8Reader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }
        loop {
            if self.output_pos < self.output_len {
                let count = (self.output_len - self.output_pos).min(buf.len());
                buf[..count]
                    .copy_from_slice(&self.output[self.output_pos..self.output_pos + count]);
                self.output_pos += count;
                return Ok(count);
            }
            if !self.checked_bom {
                self.initialize_bom()?;
                continue;
            }
            if self.pending_len > 0 {
                self.complete_pending()?;
                continue;
            }
            if self.eof {
                return Ok(0);
            }
            let read = self.inner.read(buf)?;
            if read == 0 {
                self.eof = true;
                return Ok(0);
            }
            match std::str::from_utf8(&buf[..read]) {
                Ok(_) => return Ok(read),
                Err(error) if error.error_len().is_some() => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "PTG JSON contains invalid UTF-8",
                    ));
                }
                Err(error) => {
                    let valid_up_to = error.valid_up_to();
                    let pending = &buf[valid_up_to..read];
                    self.pending[..pending.len()].copy_from_slice(pending);
                    self.pending_len = pending.len();
                    if valid_up_to > 0 {
                        return Ok(valid_up_to);
                    }
                }
            }
        }
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

    struct ChunkedReader {
        inner: Cursor<Vec<u8>>,
        max_chunk: usize,
    }

    impl ChunkedReader {
        fn new(bytes: &[u8], max_chunk: usize) -> Self {
            Self {
                inner: Cursor::new(bytes.to_vec()),
                max_chunk,
            }
        }
    }

    impl Read for ChunkedReader {
        fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
            let limit = buf.len().min(self.max_chunk);
            self.inner.read(&mut buf[..limit])
        }
    }

    fn read_strict_chunks(
        payload: &[u8],
        inner_chunk: usize,
        caller_chunk: usize,
        preserve_bom: bool,
    ) -> io::Result<Vec<u8>> {
        let inner = ChunkedReader::new(payload, inner_chunk);
        let mut reader = if preserve_bom {
            StrictUtf8Reader::preserving_bom(inner)
        } else {
            StrictUtf8Reader::new(inner)
        };
        let mut buf = vec![0; caller_chunk];
        let mut output = Vec::new();
        loop {
            let read = reader.read(&mut buf)?;
            if read == 0 {
                return Ok(output);
            }
            output.extend_from_slice(&buf[..read]);
        }
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
    fn open_json_reader_validates_gzip_utf8_and_strips_bom() {
        let path = temp_path("valid_utf8.json.gz");
        let payload = "\u{feff}{\"name\":\"Café 😀\"}".as_bytes();
        write_gzip(&path, payload);

        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader =
            open_json_reader(&path, Arc::clone(&bytes_read)).expect("open gzip json reader");
        let mut text = String::new();
        reader
            .read_to_string(&mut text)
            .expect("read valid gzip UTF-8");
        assert_eq!(text, "{\"name\":\"Café 😀\"}");
        assert!(bytes_read.load(Ordering::Relaxed) > 0);
        std::fs::remove_file(path).ok();
    }

    #[test]
    fn open_json_reader_rejects_invalid_utf8_in_gzip() {
        let path = temp_path("invalid_utf8.json.gz");
        write_gzip(&path, b"{\"name\":\"A\xFFB\"}");

        let bytes_read = Arc::new(AtomicU64::new(0));
        let mut reader =
            open_json_reader(&path, Arc::clone(&bytes_read)).expect("open gzip json reader");
        let mut text = String::new();
        let error = reader.read_to_string(&mut text).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(bytes_read.load(Ordering::Relaxed) > 0);
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

    #[test]
    fn strict_utf8_reader_handles_all_small_input_and_output_boundaries() {
        let payload = "\u{feff}Aé中😀Z".as_bytes();
        let stripped = "Aé中😀Z".as_bytes();
        for inner_chunk in 1..=9 {
            for caller_chunk in 1..=11 {
                let output = read_strict_chunks(payload, inner_chunk, caller_chunk, false)
                    .expect("read strict UTF-8 with stripped BOM");
                assert_eq!(output, stripped);

                let output = read_strict_chunks(payload, inner_chunk, caller_chunk, true)
                    .expect("read strict UTF-8 with preserved BOM");
                assert_eq!(output, payload);
            }
        }
    }

    #[test]
    fn strict_utf8_reader_handles_short_inputs_and_bom_only() {
        for (payload, expected) in [
            (&b""[..], &b""[..]),
            (&b"A"[..], &b"A"[..]),
            (&b"AB"[..], &b"AB"[..]),
            (&b"\xEF\xBB\xBF"[..], &b""[..]),
            (&b"\xEF\xBB\xBFA"[..], &b"A"[..]),
        ] {
            for inner_chunk in 1..=4 {
                for caller_chunk in 1..=4 {
                    let output = read_strict_chunks(payload, inner_chunk, caller_chunk, false)
                        .expect("read short strict UTF-8 input");
                    assert_eq!(output, expected);
                }
            }
        }
    }

    #[test]
    fn strict_utf8_reader_rejects_invalid_and_incomplete_utf8_at_all_boundaries() {
        let invalid_inputs: &[&[u8]] = &[
            b"AB\xC3(C",
            b"AB\xF0\x28\x8C\xBC",
            b"AB\xF0\x9F\x98",
            b"\xEF",
            b"\xEF\xBB",
        ];
        for payload in invalid_inputs {
            for inner_chunk in 1..=6 {
                for caller_chunk in 1..=7 {
                    let error = read_strict_chunks(payload, inner_chunk, caller_chunk, false)
                        .expect_err("reject malformed strict UTF-8 input");
                    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
                }
            }
        }
    }

    #[test]
    fn strict_utf8_reader_only_strips_a_leading_bom() {
        let payload = "ABC\u{feff}DEF".as_bytes();
        for inner_chunk in 1..=5 {
            for caller_chunk in 1..=6 {
                let output = read_strict_chunks(payload, inner_chunk, caller_chunk, false)
                    .expect("read UTF-8 containing an interior BOM");
                assert_eq!(output, payload);
            }
        }
    }
}
