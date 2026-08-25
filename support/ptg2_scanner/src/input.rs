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

include!("input_tests.rs");
