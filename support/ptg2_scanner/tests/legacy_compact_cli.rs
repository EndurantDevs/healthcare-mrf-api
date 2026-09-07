use flate2::write::GzEncoder;
use flate2::Compression;
use sha2::{Digest, Sha256};
use std::fs;
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::process::Command;

const RAW_MRF: &[u8] = include_bytes!("fixtures/compact_v4_mrf.json");

fn sha256_hex(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

#[test]
fn legacy_compact_scan_exercises_direct_provider_and_price_projection() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");
    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    fixture["in_network"][0]["ignored_nested_extension"] =
        serde_json::json!({"items": [{"value": 1}]});
    fixture["provider_references"][0]["provider_groups"]
        .as_array_mut()
        .expect("provider groups")
        .push(serde_json::json!({
            "tin": {"type": "ein", "value": "111223333"},
            "npi": [1234567893]
        }));
    let mixed_rate = serde_json::json!({
        "provider_references": [7],
        "provider_groups": [{
            "tin": {"type": "ein", "value": "444556666"},
            "npi": [1234567894]
        }],
        "negotiated_prices": [{
            "negotiated_type": "negotiated",
            "negotiated_rate": 101
        }]
    });
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    for _ in 0..256 {
        rates.push(mixed_rate.clone());
    }
    fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
        .push(serde_json::json!({
            "billing_code_type": " ",
            "billing_code": " ",
            "negotiated_rates": [mixed_rate]
        }));
    for record in fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
    {
        record
            .as_object_mut()
            .expect("in-network object")
            .remove("negotiation_arrangement");
    }
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "11".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&raw))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "1")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "1")
        .env("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false")
        .output()
        .expect("run legacy compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    assert!(!completed.stdout.is_empty());
    let stdout = String::from_utf8(completed.stdout).expect("UTF-8 scanner output");
    assert!(stdout.lines().any(|line| {
        serde_json::from_str::<serde_json::Value>(line)
            .ok()
            .and_then(|payload| payload["work_queue_blocked_sends"].as_u64())
            .is_some_and(|blocked| blocked > 0)
    }));
    assert!(!fs::read_dir(serving)
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());
}

#[test]
fn legacy_compact_scan_rejects_invalid_expiration_without_terminal_outputs() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");
    let baseline = serving.join("ptg2-v3-serving-baseline.ready");
    fs::write(&baseline, b"baseline").expect("seed serving baseline");

    let mut fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    let mut valid_rate = rates[0].clone();
    valid_rate["negotiated_prices"][0]["expiration_date"] = serde_json::json!("2028-02-29");
    let mut invalid_rate = valid_rate.clone();
    invalid_rate["negotiated_prices"][0]["expiration_date"] = serde_json::json!("2027-02-30");
    rates.clear();
    rates.extend([valid_rate, invalid_rate]);
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "33".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&raw))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
            output.join("price-atom.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "1")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "1")
        .env("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false")
        .output()
        .expect("run legacy compact scanner");

    assert!(!completed.status.success());
    assert!(String::from_utf8_lossy(&completed.stderr)
        .contains("expiration_date must be an exact ISO calendar date"));
    for terminal_kind in [
        b"manifest_price_atom_copy_file\t".as_slice(),
        b"scanner_summary\t".as_slice(),
        b"source_audit_witness_file\t".as_slice(),
        b"v3_serving_code_dictionary_file\t".as_slice(),
        b"v3_serving_run_partition_file\t".as_slice(),
    ] {
        assert!(!completed
            .stdout
            .windows(terminal_kind.len())
            .any(|window| window == terminal_kind));
    }
    assert_eq!(fs::read(&baseline).unwrap(), b"baseline");
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
    assert!(!fs::read_dir(&serving)
        .unwrap()
        .filter_map(Result::ok)
        .any(|entry| {
            let path = entry.path();
            let name = entry.file_name();
            let name = name.to_string_lossy();
            path != baseline
                && path.is_file()
                && (name.starts_with(".ptg2-v3-serving-")
                    || name.starts_with("ptg2-v3-serving-")
                    || name.starts_with("ptg2-v3-source-witness-"))
        }));
}

#[cfg(unix)]
#[test]
fn gzip_scan_indexes_and_reorders_in_network_before_provider_references() {
    use std::os::unix::process::CommandExt;
    use std::process::{Child, Stdio};
    use std::time::{Duration, Instant};

    struct ScannerChild {
        child: Child,
        reaped: bool,
        release_paths: [std::path::PathBuf; 2],
    }

    impl Drop for ScannerChild {
        fn drop(&mut self) {
            if !self.reaped {
                // Let the scanner finish and reap its independently grouped helpers.
                for release in &self.release_paths {
                    let _ = fs::write(release, b"release");
                }
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    match self.child.try_wait() {
                        Ok(Some(_)) => {
                            self.reaped = true;
                            break;
                        }
                        Ok(None) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_millis(10));
                        }
                        _ => break,
                    }
                }
                if !self.reaped {
                    // Only this unreaped scanner owns the test-created group.
                    unsafe { libc::kill(-(self.child.id() as libc::pid_t), libc::SIGKILL) };
                    let _ = self.child.wait();
                }
            }
            let mut cleanup_errors = Vec::new();
            for release in &self.release_paths {
                let pid_path = release.with_extension("pid");
                let record = match fs::read_to_string(&pid_path) {
                    Ok(record) => record,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                    Err(error) => {
                        cleanup_errors.push(format!("{}: {error}", pid_path.display()));
                        continue;
                    }
                };
                let Ok(pid) = record.trim().parse::<libc::pid_t>() else {
                    cleanup_errors.push(format!("invalid helper PID in {}", pid_path.display()));
                    continue;
                };
                let deadline = Instant::now() + Duration::from_secs(2);
                loop {
                    // Observe only: a recorded PID could have been reused after exit.
                    if pid > 0
                        && unsafe { libc::kill(-pid, 0) } == -1
                        && std::io::Error::last_os_error().raw_os_error() == Some(libc::ESRCH)
                    {
                        break;
                    }
                    if Instant::now() >= deadline {
                        cleanup_errors.push(format!("helper process group {pid} is not absent"));
                        break;
                    }
                    std::thread::sleep(Duration::from_millis(10));
                }
            }
            if !cleanup_errors.is_empty() {
                let errors = cleanup_errors.join("; ");
                if std::thread::panicking() {
                    eprintln!("scanner fixture cleanup failed: {errors}");
                } else {
                    panic!("scanner fixture cleanup failed: {errors}");
                }
            }
        }
    }

    fn wait_for_progress(stderr_path: &std::path::Path, completed: u64) -> String {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let stderr = fs::read(stderr_path).expect("read scanner progress");
            let stderr = String::from_utf8_lossy(&stderr);
            if let Some(line) = stderr.lines().find(|line| {
                line.contains("progress_basis=indexed_objects")
                    && line.contains(&format!("\tindexed_objects_completed={completed}\t"))
                    && line.contains("\tindexed_objects_total=2\t")
                    && line.ends_with("\tdone=false")
            }) {
                return line.to_string();
            }
            assert!(
                Instant::now() < deadline,
                "missing progress {completed}/2: {stderr}"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json.gz");
    let rapidgzip = temporary.path().join("rapidgzip");
    let output = temporary.path().join("output");
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir(&output).expect("create output directory");
    fs::create_dir(&serving).expect("create serving directory");
    fs::create_dir(&witness_scratch).expect("create witness scratch directory");

    let fixture: serde_json::Value =
        serde_json::from_slice(RAW_MRF).expect("parse compact fixture");
    let reordered = serde_json::to_vec(&fixture).expect("serialize reordered fixture");
    let range_offsets: Vec<usize> = fixture["in_network"]
        .as_array()
        .expect("in-network objects")
        .iter()
        .map(|object| {
            let encoded = serde_json::to_vec(object).expect("encode range object");
            reordered
                .windows(encoded.len())
                .position(|window| window == encoded)
                .expect("range object offset")
        })
        .collect();
    assert_eq!(range_offsets.len(), 2);
    let first_release = temporary.path().join("release-first-range");
    let second_release = temporary.path().join("release-second-range");
    let stdout_path = temporary.path().join("scanner.stdout");
    let stderr_path = temporary.path().join("scanner.stderr");
    let in_network = reordered
        .windows(b"\"in_network\"".len())
        .position(|window| window == b"\"in_network\"")
        .expect("in-network field");
    let provider_references = reordered
        .windows(b"\"provider_references\"".len())
        .position(|window| window == b"\"provider_references\"")
        .expect("provider-references field");
    assert!(in_network < provider_references);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
    encoder.write_all(&reordered).expect("compress fixture");
    let compressed = encoder.finish().expect("finish compressed fixture");
    fs::write(&source, &compressed).expect("write compressed fixture");
    fs::write(
        &rapidgzip,
        br#"#!/bin/sh
index=
ranges=
input=
while [ "$#" -gt 0 ]; do
  case "$1" in
    --export-index) index="$2"; shift 2 ;;
    --import-index|--index-format|-P) shift 2 ;;
    --ranges) ranges="$2"; shift 2 ;;
    -d|-c|--verify) shift ;;
    *) input="$1"; shift ;;
  esac
done
if [ -n "$index" ]; then
  printf x > "$index"
fi
if [ -n "$ranges" ]; then
  count=${ranges%@*}
  skip=${ranges#*@}
  release=
  case "$skip" in
    "$TEST_FIRST_RANGE_OFFSET") release="$TEST_FIRST_RANGE_RELEASE" ;;
    "$TEST_SECOND_RANGE_OFFSET") release="$TEST_SECOND_RANGE_RELEASE" ;;
  esac
  scanner_pid=$PPID
  if [ -n "$release" ]; then
    printf '%s\n' "$$" > "$release.pid"
  fi
  attempts=2000
  while [ -n "$release" ] && [ ! -f "$release" ]; do
    kill -0 "$scanner_pid" 2>/dev/null || exit 125
    [ "$attempts" -gt 0 ] || exit 124
    attempts=$((attempts - 1))
    sleep 0.01
  done
  gzip -dc "$input" | dd bs=1 skip="$skip" count="$count" 2>/dev/null
else
  gzip -dc "$input"
fi
"#,
    )
    .expect("write rapidgzip stand-in");
    fs::set_permissions(&rapidgzip, fs::Permissions::from_mode(0o700))
        .expect("make rapidgzip stand-in executable");

    let child = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "22".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(&compressed))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "false")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env("HLTHPRT_PTG2_RUST_WORKERS", "2")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_BIN", &rapidgzip)
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_THREADS", "2")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_INDEX_THREADS", "2")
        .env("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "2")
        .env("TEST_FIRST_RANGE_OFFSET", range_offsets[0].to_string())
        .env("TEST_SECOND_RANGE_OFFSET", range_offsets[1].to_string())
        .env("TEST_FIRST_RANGE_RELEASE", &first_release)
        .env("TEST_SECOND_RANGE_RELEASE", &second_release)
        .stdout(Stdio::from(
            fs::File::create(&stdout_path).expect("create stdout"),
        ))
        .stderr(Stdio::from(
            fs::File::create(&stderr_path).expect("create stderr"),
        ))
        .process_group(0)
        .spawn()
        .expect("run indexed compact scanner");
    let mut scanner = ScannerChild {
        child,
        reaped: false,
        release_paths: [first_release.clone(), second_release.clone()],
    };

    wait_for_progress(&stderr_path, 0);
    fs::write(&first_release, b"release").expect("release first indexed range");
    let partial_progress = wait_for_progress(&stderr_path, 1);
    let eta = partial_progress
        .split('\t')
        .find_map(|field| field.strip_prefix("eta_seconds="))
        .expect("partial indexed progress has ETA")
        .parse::<f64>()
        .expect("partial indexed progress has numeric ETA");
    assert!(eta.is_finite() && eta >= 0.0, "{partial_progress}");
    fs::write(&second_release, b"release").expect("release second indexed range");
    let deadline = Instant::now() + Duration::from_secs(10);
    let status = loop {
        if let Some(status) = scanner.child.try_wait().expect("inspect scanner status") {
            scanner.reaped = true;
            break status;
        }
        assert!(Instant::now() < deadline, "indexed scanner did not finish");
        std::thread::sleep(Duration::from_millis(10));
    };
    let stderr = fs::read(&stderr_path).expect("read scanner stderr");
    let stdout = fs::read(&stdout_path).expect("read scanner stdout");

    assert!(
        status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&stderr),
        String::from_utf8_lossy(&stdout),
    );
    let stderr = String::from_utf8_lossy(&stderr);
    assert!(
        stderr.lines().any(|line| {
            line.contains("progress_basis=indexed_objects")
                && line.contains("indexed_objects_completed=0")
                && line.contains("done=false")
        }),
        "{stderr}"
    );
    assert!(
        stderr.lines().any(|line| {
            line.contains("progress_basis=indexed_objects")
                && line.contains("\tindexed_objects_completed=2\t")
                && line.contains("\tindexed_objects_total=2\t")
                && line.ends_with("\tdone=true")
        }),
        "{stderr}"
    );
    assert!(!stdout.is_empty());
    assert!(!fs::read_dir(serving)
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());
}
