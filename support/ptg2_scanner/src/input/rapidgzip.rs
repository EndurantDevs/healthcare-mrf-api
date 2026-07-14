//! Optional external rapidgzip reader used by the full scanner pass.

use super::{is_gzip, open_reader, strict_utf8_reader};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

const MAX_STDERR_BYTES: usize = 16 * 1024;
// CI and atomic executable replacement can delay an ETXTBSY release beyond one scheduler tick.
const SPAWN_BUSY_RETRIES: usize = 40;
const SPAWN_BUSY_RETRY_DELAY: Duration = Duration::from_millis(25);
const CANCELLATION_POLL_INTERVAL: Duration = Duration::from_millis(5);

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

impl RapidgzipConfig {
    pub fn open_indexed_ranges_reader_cancellable(
        &self,
        path: &Path,
        compressed_bytes_read: Arc<AtomicU64>,
        index_path: &Path,
        ranges: &str,
        cancelled: Arc<AtomicBool>,
    ) -> io::Result<Box<dyn Read>> {
        if cancelled.load(Ordering::Acquire) {
            return Err(indexed_range_cancellation_error(path, None));
        }
        if !self.enabled || !is_gzip(path)? {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "indexed range scans require rapidgzip and gzip input",
            ));
        }
        let reader = open_rapidgzip_reader(
            path,
            compressed_bytes_read,
            self,
            RapidgzipReadOptions {
                import_index: Some(index_path),
                ranges: Some(ranges),
                ..RapidgzipReadOptions::default()
            },
            Some(Arc::clone(&cancelled)),
        );
        match reader {
            Err(error) if cancelled.load(Ordering::Acquire) => {
                Err(indexed_range_cancellation_error(path, Some(error)))
            }
            result => result,
        }
    }
}

fn indexed_range_cancellation_error(path: &Path, cause: Option<io::Error>) -> io::Error {
    let suffix = cause.map(|error| format!(": {error}")).unwrap_or_default();
    io::Error::new(
        io::ErrorKind::Interrupted,
        format!(
            "rapidgzip indexed range cancelled before reader startup completed for {}{suffix}",
            path.display()
        ),
    )
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
    cancellation_watchdog: Option<CancellationWatchdog>,
    cancelled: Option<Arc<AtomicBool>>,
    state: ReaderState,
}

struct CancellationWatchdog {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl CancellationWatchdog {
    fn start(process_group_id: u32, cancelled: Arc<AtomicBool>) -> io::Result<Self> {
        let stop = Arc::new(AtomicBool::new(false));
        let thread_stop = Arc::clone(&stop);
        let handle = thread::Builder::new()
            .name(format!("rapidgzip-cancel-{process_group_id}"))
            .spawn(move || {
                while !thread_stop.load(Ordering::Acquire) {
                    if cancelled.load(Ordering::Acquire) {
                        let _ = terminate_process_group(process_group_id);
                        return;
                    }
                    thread::park_timeout(CANCELLATION_POLL_INTERVAL);
                }
            })?;
        Ok(Self {
            stop,
            handle: Some(handle),
        })
    }

    fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(handle) = self.handle.take() {
            handle.thread().unpark();
            let _ = handle.join();
        }
    }
}

impl Drop for CancellationWatchdog {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Default)]
struct RapidgzipReadOptions<'a> {
    export_index: Option<&'a Path>,
    import_index: Option<&'a Path>,
    index_format: Option<&'a str>,
    ranges: Option<&'a str>,
}

impl RapidgzipReader {
    fn was_cancelled(&self) -> bool {
        self.cancelled
            .as_ref()
            .is_some_and(|cancelled| cancelled.load(Ordering::Acquire))
    }

    fn stop_cancellation_watchdog(&mut self) {
        if let Some(mut watchdog) = self.cancellation_watchdog.take() {
            watchdog.stop();
        }
    }

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
            let _ = terminate_process_group(child.id());
            let _ = child.kill();
            let _ = child.wait();
        }
        self.stop_cancellation_watchdog();
        let _ = self.join_stderr();
    }

    fn finish(&mut self) -> io::Result<usize> {
        self.stdout.take();
        let cancelled_before_wait = self.was_cancelled();
        let status = match self.child.as_mut() {
            Some(child) => child.wait(),
            None => Err(io::Error::other("rapidgzip child process is unavailable")),
        };
        let was_cancelled = cancelled_before_wait || self.was_cancelled();
        match status {
            Ok(status) => {
                self.child.take();
                self.stop_cancellation_watchdog();
                let stderr = self.join_stderr().map_err(|error| self.error(error))?;
                if !status.success() {
                    let stderr_message = stderr.message();
                    let suffix = if stderr_message.is_empty() {
                        String::new()
                    } else {
                        format!(": {stderr_message}")
                    };
                    if was_cancelled {
                        return Err(self.error(io::Error::new(
                            io::ErrorKind::Interrupted,
                            format!(
                                "rapidgzip cancelled for {} with {status}{suffix}",
                                self.path.display()
                            ),
                        )));
                    }
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
                if was_cancelled {
                    return Err(self.error(io::Error::new(
                        io::ErrorKind::Interrupted,
                        format!(
                            "rapidgzip cancelled while waiting for {}: {error}",
                            self.path.display()
                        ),
                    )));
                }
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
    let _ = terminate_process_group(child.id());
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(unix)]
fn configure_process_group(command: &mut Command) {
    use std::os::unix::process::CommandExt;

    command.process_group(0);
}

#[cfg(not(unix))]
fn configure_process_group(_command: &mut Command) {}

#[cfg(unix)]
fn terminate_process_group(process_group_id: u32) -> io::Result<()> {
    unsafe extern "C" {
        #[link_name = "killpg"]
        fn kill_process_group(process_group: i32, signal: i32) -> i32;
    }

    const SIGKILL: i32 = 9;
    let process_group = i32::try_from(process_group_id).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("rapidgzip process id {process_group_id} exceeds i32"),
        )
    })?;
    // SAFETY: the child is spawned as leader of its own process group and killpg does not retain
    // the integer arguments. Killing the group also terminates helper descendants inherited by it.
    if unsafe { kill_process_group(process_group, SIGKILL) } == 0 {
        return Ok(());
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound || error.raw_os_error() == Some(3) {
        return Ok(());
    }
    Err(error)
}

#[cfg(not(unix))]
fn terminate_process_group(_process_group_id: u32) -> io::Result<()> {
    Ok(())
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
    configure_process_group(&mut command);

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
    cancelled: Option<Arc<AtomicBool>>,
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
    let cancellation_watchdog = match cancelled.as_ref() {
        Some(cancelled) => match CancellationWatchdog::start(child_id, Arc::clone(cancelled)) {
            Ok(watchdog) => Some(watchdog),
            Err(error) => {
                drop(stdout);
                stop_spawned_child(child);
                let _ = stderr_drain.join();
                return Err(io::Error::new(
                    error.kind(),
                    format!("failed to start rapidgzip cancellation watchdog: {error}"),
                ));
            }
        },
        None => None,
    };

    Ok(Box::new(RapidgzipReader {
        path: path.to_path_buf(),
        stdout: Some(stdout),
        child: Some(child),
        stderr_drain: Some(stderr_drain),
        compressed_bytes_read,
        compressed_total,
        cancellation_watchdog,
        cancelled,
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
        None,
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
        None,
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
        None,
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
