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
