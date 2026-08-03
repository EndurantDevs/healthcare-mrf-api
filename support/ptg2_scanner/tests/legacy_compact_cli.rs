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

#[cfg(unix)]
#[test]
fn gzip_scan_indexes_and_reorders_in_network_before_provider_references() {
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
  sleep 4
  gzip -dc "$input" | dd bs=1 skip="$skip" count="$count" 2>/dev/null
else
  gzip -dc "$input"
fi
"#,
    )
    .expect("write rapidgzip stand-in");
    fs::set_permissions(&rapidgzip, fs::Permissions::from_mode(0o700))
        .expect("make rapidgzip stand-in executable");

    let completed = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
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
        .output()
        .expect("run indexed compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let stderr = String::from_utf8_lossy(&completed.stderr);
    assert!(
        stderr.lines().any(|line| {
            line.contains("progress_basis=indexed_objects")
                && line.contains("indexed_objects_completed=0")
                && line.contains("done=false")
        }),
        "{stderr}"
    );
    assert!(!completed.stdout.is_empty());
    assert!(!fs::read_dir(serving)
        .unwrap()
        .collect::<Vec<_>>()
        .is_empty());
}
