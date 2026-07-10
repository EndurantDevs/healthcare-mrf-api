use flate2::{write::GzEncoder, Compression};
use ptg2_scanner::input::{open_full_scan_reader, RapidgzipConfig};
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{
    atomic::{AtomicU64, Ordering},
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
fn shell_quote(path: &Path) -> String {
    format!("'{}'", path.to_string_lossy().replace('\'', "'\"'\"'"))
}

#[cfg(unix)]
fn write_executable(path: &Path, script: &str) {
    use std::os::unix::fs::PermissionsExt;

    std::fs::write(path, script).expect("write fake rapidgzip executable");
    let mut permissions = path
        .metadata()
        .expect("stat fake rapidgzip executable")
        .permissions();
    permissions.set_mode(0o700);
    std::fs::set_permissions(path, permissions).expect("make fake rapidgzip executable runnable");
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
fn full_scan_reader_returns_exact_rapidgzip_stdout() {
    let input_path = temp_path("output.json.gz");
    let executable_path = temp_path("output.sh");
    write_gzip(&input_path, b"internal decoder output");
    write_executable(
        &executable_path,
        "#!/bin/sh\nprintf '%s' 'external decoder output'\n",
    );

    let compressed_total = input_path.metadata().expect("stat gzip input").len();
    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = enabled_rapidgzip(&executable_path, 3);
    let mut reader = open_full_scan_reader(&input_path, Arc::clone(&bytes_read), &config)
        .expect("open rapidgzip reader");
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .expect("read rapidgzip stdout");

    assert_eq!(output, "external decoder output");
    assert_eq!(bytes_read.load(Ordering::Relaxed), compressed_total);
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}

#[cfg(unix)]
#[test]
fn full_scan_reader_surfaces_nonzero_exit_and_bounded_stderr() {
    let input_path = temp_path("failure.json.gz");
    let executable_path = temp_path("failure.sh");
    write_gzip(&input_path, b"internal decoder must not be used");
    write_executable(
        &executable_path,
        "#!/bin/sh\n\
         printf '%s' 'partial output'\n\
         printf 'decoder exploded\\n' >&2\n\
         i=0\n\
         while [ \"$i\" -lt 3000 ]; do\n\
           printf '0123456789' >&2\n\
           i=$((i + 1))\n\
         done\n\
         exit 23\n",
    );

    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = enabled_rapidgzip(&executable_path, 2);
    let mut reader = open_full_scan_reader(&input_path, Arc::clone(&bytes_read), &config)
        .expect("spawn fake rapidgzip");
    let mut output = String::new();
    let error = reader
        .read_to_string(&mut output)
        .expect_err("nonzero rapidgzip exit must fail the read");

    let message = error.to_string();
    assert_eq!(output, "partial output");
    assert!(message.contains("23"), "unexpected error: {message}");
    assert!(
        message.contains("decoder exploded"),
        "unexpected error: {message}"
    );
    assert!(
        message.contains("[stderr truncated]"),
        "unexpected error: {message}"
    );
    assert!(message.len() <= 17 * 1024);
    assert_eq!(bytes_read.load(Ordering::Relaxed), 0);
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}

#[cfg(unix)]
#[test]
fn full_scan_reader_reaps_rapidgzip_when_dropped_early() {
    let input_path = temp_path("drop.json.gz");
    let executable_path = temp_path("drop.sh");
    let pid_path = temp_path("drop.pid");
    write_gzip(&input_path, b"unused");
    write_executable(
        &executable_path,
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$$\" > {}\nwhile :; do\n  printf 'abcdefghijklmnopqrstuvwxyz'\ndone\n",
            shell_quote(&pid_path)
        ),
    );

    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = enabled_rapidgzip(&executable_path, 1);
    let mut reader =
        open_full_scan_reader(&input_path, bytes_read, &config).expect("spawn fake rapidgzip");
    let mut byte = [0u8; 1];
    reader.read_exact(&mut byte).expect("read child output");
    let pid: u32 = std::fs::read_to_string(&pid_path)
        .expect("read child pid")
        .trim()
        .parse()
        .expect("parse child pid");
    drop(reader);

    let process_is_alive = Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("run kill -0")
        .success();
    assert!(!process_is_alive, "rapidgzip process {pid} survived drop");
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
    std::fs::remove_file(pid_path).ok();
}

#[cfg(unix)]
#[test]
fn full_scan_reader_uses_internal_decoder_when_disabled() {
    let input_path = temp_path("disabled.json.gz");
    let executable_path = temp_path("disabled.sh");
    let invoked_path = temp_path("disabled.invoked");
    write_gzip(&input_path, b"internal decoder output");
    write_executable(
        &executable_path,
        &format!(
            "#!/bin/sh\nprintf invoked > {}\nexit 99\n",
            shell_quote(&invoked_path)
        ),
    );

    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = RapidgzipConfig {
        enabled: false,
        executable: executable_path.clone(),
        decoder_threads: 0,
    };
    let mut reader = open_full_scan_reader(&input_path, bytes_read, &config)
        .expect("open disabled rapidgzip fallback");
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .expect("read internal decoder output");

    assert_eq!(output, "internal decoder output");
    assert!(!invoked_path.exists());
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}

#[cfg(unix)]
#[test]
fn full_scan_reader_constructs_expected_rapidgzip_arguments() {
    let input_path = temp_path("args.json.gz");
    let executable_path = temp_path("args.sh");
    let arguments_path = temp_path("args.txt");
    write_gzip(&input_path, b"unused");
    write_executable(
        &executable_path,
        &format!(
            "#!/bin/sh\nfor argument in \"$@\"; do\n  printf '%s\\n' \"$argument\"\ndone > {}\nprintf arguments-ok\n",
            shell_quote(&arguments_path)
        ),
    );

    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = enabled_rapidgzip(&executable_path, 7);
    let mut reader =
        open_full_scan_reader(&input_path, bytes_read, &config).expect("spawn fake rapidgzip");
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .expect("read fake rapidgzip output");

    let arguments = std::fs::read_to_string(&arguments_path).expect("read rapidgzip arguments");
    assert_eq!(output, "arguments-ok");
    assert_eq!(
        arguments.lines().collect::<Vec<_>>(),
        vec![
            "-d",
            "-c",
            "-P",
            "7",
            "--verify",
            input_path.to_str().expect("utf8 test input path"),
        ]
    );
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
    std::fs::remove_file(arguments_path).ok();
}

#[cfg(unix)]
#[test]
fn full_scan_reader_only_uses_rapidgzip_for_gzip_inputs() {
    let input_path = temp_path("plain.json");
    let executable_path = temp_path("plain.sh");
    let invoked_path = temp_path("plain.invoked");
    std::fs::write(&input_path, b"plain input").expect("write plain input");
    write_executable(
        &executable_path,
        &format!(
            "#!/bin/sh\nprintf invoked > {}\nprintf external\n",
            shell_quote(&invoked_path)
        ),
    );

    let bytes_read = Arc::new(AtomicU64::new(0));
    let config = enabled_rapidgzip(&executable_path, 4);
    let mut reader = open_full_scan_reader(&input_path, bytes_read, &config)
        .expect("open plain full-scan reader");
    let mut output = String::new();
    reader
        .read_to_string(&mut output)
        .expect("read plain input");

    assert_eq!(output, "plain input");
    assert!(!invoked_path.exists());
    std::fs::remove_file(input_path).ok();
    std::fs::remove_file(executable_path).ok();
}
