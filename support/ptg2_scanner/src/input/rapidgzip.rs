//! Optional external rapidgzip reader used by the full scanner pass.

use super::{is_gzip, open_reader, strict_utf8_reader};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_STDERR_BYTES: usize = 16 * 1024;
// CI and atomic executable replacement can delay an ETXTBSY release beyond one scheduler tick.
const SPAWN_BUSY_RETRIES: usize = 40;
const SPAWN_BUSY_RETRY_DELAY: Duration = Duration::from_millis(25);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RapidgzipConfig {
    pub enabled: bool,
    pub executable: PathBuf,
    pub decoder_threads: usize,
}

impl Default for RapidgzipConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            executable: PathBuf::from("rapidgzip"),
            decoder_threads: 1,
        }
    }
}

#[derive(Default)]
struct StderrCapture {
    bytes: Vec<u8>,
    truncated: bool,
}

impl StderrCapture {
    fn message(&self) -> String {
        let mut message = match std::str::from_utf8(&self.bytes) {
            Ok(message) => message.trim().to_owned(),
            Err(_) => "rapidgzip emitted non-UTF-8 stderr".to_owned(),
        };
        if self.truncated {
            if !message.is_empty() {
                message.push(' ');
            }
            message.push_str("[stderr truncated]");
        }
        message
    }
}

fn drain_stderr(mut stderr: ChildStderr) -> io::Result<StderrCapture> {
    let mut capture = StderrCapture {
        bytes: Vec::with_capacity(MAX_STDERR_BYTES),
        truncated: false,
    };
    let mut buffer = [0u8; 8192];
    loop {
        let read = stderr.read(&mut buffer)?;
        if read == 0 {
            return Ok(capture);
        }
        let retained = (MAX_STDERR_BYTES - capture.bytes.len()).min(read);
        capture.bytes.extend_from_slice(&buffer[..retained]);
        capture.truncated |= retained < read;
    }
}

enum ReaderState {
    Reading,
    Complete,
    Failed {
        kind: io::ErrorKind,
        message: String,
    },
}

struct RapidgzipReader {
    path: PathBuf,
    stdout: Option<ChildStdout>,
    child: Option<Child>,
    stderr_drain: Option<JoinHandle<io::Result<StderrCapture>>>,
    compressed_bytes_read: Arc<AtomicU64>,
    compressed_total: u64,
    state: ReaderState,
}

#[derive(Default)]
struct RapidgzipReadOptions<'a> {
    export_index: Option<&'a Path>,
    import_index: Option<&'a Path>,
    index_format: Option<&'a str>,
    ranges: Option<&'a str>,
}

impl RapidgzipReader {
    fn error(&mut self, error: io::Error) -> io::Error {
        let kind = error.kind();
        let message = error.to_string();
        self.state = ReaderState::Failed {
            kind,
            message: message.clone(),
        };
        io::Error::new(kind, message)
    }

    fn join_stderr(&mut self) -> io::Result<StderrCapture> {
        let Some(stderr_drain) = self.stderr_drain.take() else {
            return Ok(StderrCapture::default());
        };
        stderr_drain
            .join()
            .map_err(|_| io::Error::other("rapidgzip stderr drain thread panicked"))?
    }

    fn kill_and_wait(&mut self) {
        self.stdout.take();
        if let Some(mut child) = self.child.take() {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = self.join_stderr();
    }

    fn finish(&mut self) -> io::Result<usize> {
        self.stdout.take();
        let status = match self.child.as_mut() {
            Some(child) => child.wait(),
            None => Err(io::Error::other("rapidgzip child process is unavailable")),
        };
        match status {
            Ok(status) => {
                self.child.take();
                let stderr = self.join_stderr().map_err(|error| self.error(error))?;
                if !status.success() {
                    let stderr_message = stderr.message();
                    let suffix = if stderr_message.is_empty() {
                        String::new()
                    } else {
                        format!(": {stderr_message}")
                    };
                    return Err(self.error(io::Error::other(format!(
                        "rapidgzip failed for {} with {status}{suffix}",
                        self.path.display()
                    ))));
                }
                self.compressed_bytes_read
                    .store(self.compressed_total, Ordering::Relaxed);
                self.state = ReaderState::Complete;
                Ok(0)
            }
            Err(error) => {
                self.kill_and_wait();
                Err(self.error(io::Error::new(
                    error.kind(),
                    format!(
                        "failed waiting for rapidgzip for {}: {error}",
                        self.path.display()
                    ),
                )))
            }
        }
    }
}

impl Read for RapidgzipReader {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        match &self.state {
            ReaderState::Complete => return Ok(0),
            ReaderState::Failed { kind, message } => {
                return Err(io::Error::new(*kind, message.clone()));
            }
            ReaderState::Reading => {}
        }
        if buffer.is_empty() {
            return Ok(0);
        }
        let read_result = match self.stdout.as_mut() {
            Some(stdout) => stdout.read(buffer),
            None => Err(io::Error::other("rapidgzip stdout is unavailable")),
        };
        match read_result {
            Ok(0) => self.finish(),
            Ok(read) => Ok(read),
            Err(error) => {
                self.kill_and_wait();
                Err(self.error(error))
            }
        }
    }
}

impl Drop for RapidgzipReader {
    fn drop(&mut self) {
        if self.child.is_some() || self.stderr_drain.is_some() {
            self.kill_and_wait();
        }
    }
}

fn stop_spawned_child(mut child: Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn spawn_rapidgzip(
    path: &Path,
    config: &RapidgzipConfig,
    options: RapidgzipReadOptions<'_>,
) -> io::Result<Child> {
    let mut command = Command::new(&config.executable);
    command
        .arg("-d")
        .arg("-c")
        .arg("-P")
        .arg(config.decoder_threads.to_string())
        .arg("--verify");
    if let Some(index_path) = options.export_index {
        command.arg("--export-index").arg(index_path);
    }
    if let Some(index_format) = options.index_format {
        command.arg("--index-format").arg(index_format);
    }
    if let Some(index_path) = options.import_index {
        command.arg("--import-index").arg(index_path);
    }
    if let Some(ranges) = options.ranges {
        command.arg("--ranges").arg(ranges);
    }
    command
        .arg(path)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    for retry_no in 0..=SPAWN_BUSY_RETRIES {
        match command.spawn() {
            Ok(child) => return Ok(child),
            Err(error)
                if error.kind() == io::ErrorKind::ExecutableFileBusy
                    && retry_no < SPAWN_BUSY_RETRIES =>
            {
                thread::sleep(SPAWN_BUSY_RETRY_DELAY);
            }
            Err(error) => {
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "failed to spawn rapidgzip executable {}: {error}",
                        config.executable.display()
                    ),
                ));
            }
        }
    }
    unreachable!("rapidgzip spawn retry loop always returns")
}

fn open_rapidgzip_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
    config: &RapidgzipConfig,
    options: RapidgzipReadOptions<'_>,
) -> io::Result<Box<dyn Read>> {
    if config.decoder_threads == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "rapidgzip decoder_threads must be greater than zero",
        ));
    }

    let compressed_total = path.metadata()?.len();
    let mut child = spawn_rapidgzip(path, config, options)?;
    let stdout = match child.stdout.take() {
        Some(stdout) => stdout,
        None => {
            stop_spawned_child(child);
            return Err(io::Error::other("rapidgzip stdout pipe is unavailable"));
        }
    };
    let stderr = match child.stderr.take() {
        Some(stderr) => stderr,
        None => {
            drop(stdout);
            stop_spawned_child(child);
            return Err(io::Error::other("rapidgzip stderr pipe is unavailable"));
        }
    };
    let child_id = child.id();
    let stderr_drain = match thread::Builder::new()
        .name(format!("rapidgzip-stderr-{child_id}"))
        .spawn(move || drain_stderr(stderr))
    {
        Ok(handle) => handle,
        Err(error) => {
            drop(stdout);
            stop_spawned_child(child);
            return Err(io::Error::new(
                error.kind(),
                format!("failed to start rapidgzip stderr drain: {error}"),
            ));
        }
    };

    Ok(Box::new(RapidgzipReader {
        path: path.to_path_buf(),
        stdout: Some(stdout),
        child: Some(child),
        stderr_drain: Some(stderr_drain),
        compressed_bytes_read,
        compressed_total,
        state: ReaderState::Reading,
    }))
}

pub fn open_full_scan_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
    rapidgzip: &RapidgzipConfig,
) -> io::Result<Box<dyn Read>> {
    if !rapidgzip.enabled || !is_gzip(path)? {
        return open_reader(path, compressed_bytes_read);
    }
    open_rapidgzip_reader(
        path,
        compressed_bytes_read,
        rapidgzip,
        RapidgzipReadOptions::default(),
    )
}

pub fn open_full_scan_reader_exporting_index(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
    rapidgzip: &RapidgzipConfig,
    index_path: &Path,
) -> io::Result<Box<dyn Read>> {
    if !rapidgzip.enabled || !is_gzip(path)? {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "indexed scans require rapidgzip and gzip input",
        ));
    }
    open_rapidgzip_reader(
        path,
        compressed_bytes_read,
        rapidgzip,
        RapidgzipReadOptions {
            export_index: Some(index_path),
            index_format: Some("gztool"),
            ..RapidgzipReadOptions::default()
        },
    )
}

pub fn open_indexed_ranges_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
    rapidgzip: &RapidgzipConfig,
    index_path: &Path,
    ranges: &str,
) -> io::Result<Box<dyn Read>> {
    if !rapidgzip.enabled || !is_gzip(path)? {
        return Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "indexed range scans require rapidgzip and gzip input",
        ));
    }
    open_rapidgzip_reader(
        path,
        compressed_bytes_read,
        rapidgzip,
        RapidgzipReadOptions {
            import_index: Some(index_path),
            ranges: Some(ranges),
            ..RapidgzipReadOptions::default()
        },
    )
}

pub fn open_full_scan_json_reader(
    path: &Path,
    compressed_bytes_read: Arc<AtomicU64>,
    rapidgzip: &RapidgzipConfig,
) -> io::Result<Box<dyn Read>> {
    Ok(strict_utf8_reader(open_full_scan_reader(
        path,
        compressed_bytes_read,
        rapidgzip,
    )?))
}
