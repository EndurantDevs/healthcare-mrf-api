#[cfg(all(test, unix))]
mod tests {
    use super::{
        configure_process_group, indexed_range_cancellation_error, open_full_scan_json_reader,
        open_full_scan_reader_exporting_index, open_indexed_ranges_reader, stop_spawned_child,
        terminate_process_group, RapidgzipConfig, StderrCapture,
    };
    use std::fs;
    use std::io::{self, Read, Write};
    use std::process::Command;
    use std::sync::{
        atomic::{AtomicBool, AtomicU64},
        Arc,
    };

    #[test]
    fn spawned_child_cleanup_reaps_its_process_group() {
        let mut command = Command::new("sh");
        command.args(["-c", "sleep 60"]);
        configure_process_group(&mut command);
        let child = command.spawn().expect("spawn isolated cleanup fixture");

        stop_spawned_child(child);
    }

    #[test]
    fn process_group_cleanup_rejects_unrepresentable_ids() {
        let error = terminate_process_group(u32::MAX)
            .expect_err("process group identifiers must fit signed C integers");

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("exceeds i32"));
    }

    #[test]
    fn disabled_indexed_readers_fail_closed_while_full_json_falls_back() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("provider-file.json");
        let index_path = directory.path().join("provider-file.index");
        fs::write(&path, b"[]").unwrap();
        let compressed_bytes = Arc::new(AtomicU64::new(0));
        let config = RapidgzipConfig::default();

        let mut reader = open_full_scan_json_reader(&path, Arc::clone(&compressed_bytes), &config)
            .expect("plain JSON uses the strict fallback reader");
        let mut payload = String::new();
        reader.read_to_string(&mut payload).unwrap();
        assert_eq!(payload, "[]");

        let Err(export_error) = open_full_scan_reader_exporting_index(
            &path,
            Arc::clone(&compressed_bytes),
            &config,
            &index_path,
        ) else {
            panic!("an indexed scan cannot silently use the fallback reader");
        };
        assert_eq!(export_error.kind(), io::ErrorKind::Unsupported);
        let Err(range_error) =
            open_indexed_ranges_reader(&path, compressed_bytes, &config, &index_path, "0:2")
        else {
            panic!("an indexed range cannot silently use the fallback reader");
        };
        assert_eq!(range_error.kind(), io::ErrorKind::Unsupported);
    }

    #[test]
    fn cancellation_and_stderr_errors_preserve_bounded_context() {
        let cause = io::Error::other("spawn denied");
        let error =
            indexed_range_cancellation_error(std::path::Path::new("fixture.gz"), Some(cause));
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert!(error.to_string().contains("fixture.gz: spawn denied"));

        let capture = StderrCapture {
            bytes: vec![0xff],
            truncated: true,
        };
        assert_eq!(
            capture.message(),
            "rapidgzip emitted non-UTF-8 stderr [stderr truncated]"
        );
    }

    #[test]
    fn cancellable_reader_reaps_successful_decoder_and_watchdog() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("provider-file.json.gz");
        let index_path = directory.path().join("provider-file.index");
        let executable = directory.path().join("rapidgzip");
        let mut encoder = flate2::write::GzEncoder::new(
            fs::File::create(&path).unwrap(),
            flate2::Compression::default(),
        );
        encoder.write_all(b"[]").unwrap();
        encoder.finish().unwrap();
        fs::write(&executable, b"#!/bin/sh\nprintf '[]'\n").unwrap();
        let mut permissions = executable.metadata().unwrap().permissions();
        permissions.set_mode(0o700);
        fs::set_permissions(&executable, permissions).unwrap();

        let config = RapidgzipConfig {
            enabled: true,
            executable,
            decoder_threads: 1,
        };
        let mut reader = config
            .open_indexed_ranges_reader_cancellable(
                &path,
                Arc::new(AtomicU64::new(0)),
                &index_path,
                "0@2",
                Arc::new(AtomicBool::new(false)),
            )
            .expect("start a cancellable indexed reader");
        let mut payload = String::new();
        reader.read_to_string(&mut payload).unwrap();
        assert_eq!(payload, "[]");
    }
}
