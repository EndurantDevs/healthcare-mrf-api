use flate2::{write::GzEncoder, Compression};
use ptg2_scanner::input::{
    open_full_scan_reader, open_full_scan_reader_exporting_index, RapidgzipConfig,
};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_path(suffix: &str) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("ptg2_scanner_rapidgzip_test_{nanos}_{suffix}"))
}

fn write_gzip(path: &Path, payload: &[u8]) {
    let file = File::create(path).expect("create gzip test file");
    let mut encoder = GzEncoder::new(file, Compression::default());
    encoder.write_all(payload).expect("write gzip payload");
    encoder.finish().expect("finish gzip payload");
}

#[cfg(unix)]
fn write_executable(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;

    let staging_path = path.with_extension("staging");
    {
        let mut staging_file =
            File::create(&staging_path).expect("create fake rapidgzip staging file");
        staging_file
            .write_all(script.as_bytes())
            .expect("write fake rapidgzip executable");
    }
    let mut permissions = staging_path
        .metadata()
        .expect("stat fake rapidgzip executable")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(&staging_path, permissions)
        .expect("make fake rapidgzip executable runnable");
    std::fs::rename(staging_path, path).expect("publish fake rapidgzip executable");
}

#[cfg(unix)]
fn enabled_rapidgzip(executable: &Path, decoder_threads: usize) -> RapidgzipConfig {
    RapidgzipConfig {
        enabled: true,
        executable: executable.to_path_buf(),
        decoder_threads,
    }
}

#[cfg(unix)]
#[test]
fn invalid_configuration_and_spawn_failures_are_reported() {
    let input_path = temp_path("invalid-config.json.gz");
    let index_path = temp_path("invalid-config.index");
    write_gzip(&input_path, b"unused");

    let zero_threads = enabled_rapidgzip(Path::new("/does/not/matter"), 0);
    let zero_thread_error =
        open_full_scan_reader(&input_path, Arc::new(AtomicU64::new(0)), &zero_threads)
            .err()
            .expect("zero decoder threads must fail");
    assert_eq!(zero_thread_error.kind(), std::io::ErrorKind::InvalidInput);

    let missing_executable = enabled_rapidgzip(&temp_path("missing-executable"), 1);
    let spawn_error = open_full_scan_reader_exporting_index(
        &input_path,
        Arc::new(AtomicU64::new(0)),
        &missing_executable,
        &index_path,
    )
    .err()
    .expect("missing rapidgzip executable must fail");
    assert_eq!(spawn_error.kind(), std::io::ErrorKind::NotFound);
    assert!(spawn_error
        .to_string()
        .contains("failed to spawn rapidgzip"));

    std::fs::remove_file(input_path).ok();
}

#[cfg(unix)]
#[test]
fn cancellable_indexed_reader_interrupts_the_process_group() {
    let input_path = temp_path("cancel.json.gz");
    let executable_path = temp_path("cancel.sh");
    let index_path = temp_path("cancel.index");
    write_gzip(&input_path, b"unused");
    write_executable(
        &executable_path,
        "#!/bin/sh\nprintf started\nsleep 0.1\nexit 9\n",
    );

    let cancelled = Arc::new(AtomicBool::new(false));
    let config = enabled_rapidgzip(&executable_path, 1);
    let mut reader = config
        .open_indexed_ranges_reader_cancellable(
            &input_path,
            Arc::new(AtomicU64::new(0)),
            &index_path,
            "0@1",
            Arc::clone(&cancelled),
        )
        .expect("open cancellable indexed reader");
    let mut empty = [];
    assert_eq!(reader.read(&mut empty).expect("empty read"), 0);
    cancelled.store(true, Ordering::Release);

    let mut buffer = [0u8; 64];
    let error = loop {
        match reader.read(&mut buffer) {
            Ok(0) => panic!("cancelled reader reached clean EOF"),
            Ok(_) => {}
            Err(error) => break error,
        }
    };
    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(error.to_string().contains("rapidgzip cancelled"));

    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}

#[cfg(unix)]
#[test]
fn cancellable_indexed_reader_rejects_pre_cancelled_work() {
    let input_path = temp_path("pre-cancel.json.gz");
    let index_path = temp_path("pre-cancel.index");
    let config = enabled_rapidgzip(Path::new("/does/not/matter"), 1);
    let error = config
        .open_indexed_ranges_reader_cancellable(
            &input_path,
            Arc::new(AtomicU64::new(0)),
            &index_path,
            "0@1",
            Arc::new(AtomicBool::new(true)),
        )
        .err()
        .expect("pre-cancelled indexed work must fail before opening input");

    assert_eq!(error.kind(), std::io::ErrorKind::Interrupted);
    assert!(error
        .to_string()
        .contains("before reader startup completed"));
}

#[cfg(unix)]
#[test]
fn non_utf8_stderr_has_a_stable_diagnostic() {
    let input_path = temp_path("non-utf8-stderr.json.gz");
    let executable_path = temp_path("non-utf8-stderr.sh");
    write_gzip(&input_path, b"unused");
    write_executable(&executable_path, "#!/bin/sh\nprintf '\\377' >&2\nexit 9\n");

    let config = enabled_rapidgzip(&executable_path, 1);
    let mut reader = open_full_scan_reader(&input_path, Arc::new(AtomicU64::new(0)), &config)
        .expect("open failing decoder");
    let error = reader
        .read_to_end(&mut Vec::new())
        .expect_err("non-UTF-8 diagnostic process must fail");
    assert!(error
        .to_string()
        .contains("rapidgzip emitted non-UTF-8 stderr"));

    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}
