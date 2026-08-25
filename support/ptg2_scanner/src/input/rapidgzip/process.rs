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
