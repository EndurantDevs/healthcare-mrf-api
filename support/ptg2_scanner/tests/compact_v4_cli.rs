use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

const RAW_MRF: &[u8] = include_bytes!("fixtures/compact_v4_mrf.json");

fn sha256_hex(payload: &[u8]) -> String {
    Sha256::digest(payload)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn file_sha256(path: &Path) -> String {
    sha256_hex(&fs::read(path).expect("read artifact for digest"))
}

fn json_sha256(value: &Value) -> String {
    sha256_hex(&serde_json::to_vec(value).expect("encode digest payload"))
}

fn artifact_path(event: &Value) -> PathBuf {
    PathBuf::from(event["path"].as_str().expect("artifact path"))
}

fn scanner_artifacts<'a>(payloads: &'a [Value], name: &str) -> Vec<&'a Value> {
    let mut events = payloads
        .iter()
        .filter(|payload| {
            payload["path"]
                .as_str()
                .is_some_and(|path| path.contains(name))
        })
        .collect::<Vec<_>>();
    events.sort_by_key(|event| event["path"].as_str().expect("artifact path"));
    events
}

fn append_pg_binary_field(payload: &mut Vec<u8>, field: &[u8]) {
    payload.extend_from_slice(&(field.len() as i32).to_be_bytes());
    payload.extend_from_slice(field);
}

fn decode_global_id(value: &str) -> [u8; 16] {
    assert_eq!(value.len(), 32, "global ID must contain 32 hex digits");
    let mut decoded = [0u8; 16];
    for (index, destination) in decoded.iter_mut().enumerate() {
        *destination = u8::from_str_radix(&value[index * 2..index * 2 + 2], 16)
            .expect("lowercase global ID hex");
    }
    decoded
}

fn write_price_key_map(payloads: &[Value], path: &Path) -> usize {
    let mut price_sets = BTreeMap::<String, f64>::new();
    for event in scanner_artifacts(payloads, "price-set-summary.copy") {
        let contents = fs::read_to_string(artifact_path(event)).expect("price-set summary");
        for line in contents.lines().filter(|line| !line.is_empty()) {
            let mut fields = line.split('\t');
            let global_id = fields.next().expect("price-set global ID").to_owned();
            let minimum_rate = fields
                .next()
                .expect("minimum negotiated rate")
                .parse::<f64>()
                .expect("numeric minimum negotiated rate");
            price_sets
                .entry(global_id)
                .and_modify(|existing| *existing = existing.min(minimum_rate))
                .or_insert(minimum_rate);
        }
    }
    assert!(
        !price_sets.is_empty(),
        "scanner must emit price-set summaries"
    );
    let mut price_sets = price_sets.into_iter().collect::<Vec<_>>();
    price_sets.sort_by(|(left_id, left_rate), (right_id, right_rate)| {
        left_rate
            .total_cmp(right_rate)
            .then_with(|| left_id.cmp(right_id))
    });

    let mut payload = Vec::new();
    payload.extend_from_slice(b"PGCOPY\n\xff\r\n\0");
    payload.extend_from_slice(&0i32.to_be_bytes());
    payload.extend_from_slice(&0i32.to_be_bytes());
    for (price_key, (global_id, _minimum_rate)) in price_sets.iter().enumerate() {
        payload.extend_from_slice(&2i16.to_be_bytes());
        append_pg_binary_field(&mut payload, &decode_global_id(global_id));
        append_pg_binary_field(&mut payload, &(price_key as i64).to_be_bytes());
    }
    payload.extend_from_slice(&(-1i16).to_be_bytes());
    fs::write(path, payload).expect("write authoritative price-key map");
    price_sets.len()
}

fn write_finalizer_manifest(payloads: &[Value], path: &Path) {
    let source_identity = json!({
        "source_type": "in_network",
        "identity_kind": "raw_container_sha256_v1",
        "identity_sha256": sha256_hex(RAW_MRF),
    });
    let mut serving_events = payloads
        .iter()
        .filter(|payload| payload["format"] == "ptg2_v3_serving_run")
        .collect::<Vec<_>>();
    serving_events.sort_by_key(|event| {
        (
            event["partition"].as_u64().expect("serving partition"),
            event["path"].as_str().expect("serving path"),
        )
    });
    assert!(!serving_events.is_empty(), "scanner must emit serving runs");
    let partition_count = serving_events[0]["partition_count"]
        .as_u64()
        .expect("partition count") as usize;
    let mut partition_rows = vec![0u64; partition_count];
    let mut source_files = serving_events
        .iter()
        .map(|event| {
            let partition = event["partition"].as_u64().expect("serving partition") as usize;
            let row_count = event["row_count"].as_u64().expect("serving rows");
            partition_rows[partition] += row_count;
            json!({
                "partition": partition,
                "row_count": row_count,
                "bytes": event["bytes"],
                "sha256": event["sha256"],
            })
        })
        .collect::<Vec<_>>();
    source_files.sort_by_key(|entry| {
        (
            entry["partition"].as_u64().expect("source partition"),
            entry["sha256"].as_str().expect("source digest").to_owned(),
            entry["row_count"].as_u64().expect("source rows"),
            entry["bytes"].as_u64().expect("source bytes"),
        )
    });
    let source_contract_body = json!({
        "version": 1,
        "source_identity": source_identity,
        "partition_count": partition_count,
        "partition_rows": partition_rows,
        "file_count": source_files.len(),
        "row_count": source_files.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
        "byte_count": source_files.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
        "files": source_files,
    });
    let source_contract_sha256 = json_sha256(&source_contract_body);
    let mut source_contract = source_contract_body.as_object().unwrap().clone();
    source_contract.insert("source_key".to_owned(), json!(0));
    source_contract.insert("contract_sha256".to_owned(), json!(source_contract_sha256));
    let source_contracts = vec![Value::Object(source_contract)];

    let mut code_events = payloads
        .iter()
        .filter(|payload| payload["format"] == "ptg2_v3_serving_code_dictionary")
        .collect::<Vec<_>>();
    code_events.sort_by_key(|event| event["path"].as_str().expect("code dictionary path"));
    assert!(
        !code_events.is_empty(),
        "scanner must emit code dictionaries"
    );
    let mut code_files = code_events
        .iter()
        .map(|event| {
            json!({
                "row_count": event["row_count"],
                "bytes": event["bytes"],
                "sha256": event["sha256"],
            })
        })
        .collect::<Vec<_>>();
    code_files.sort_by_key(|entry| {
        (
            entry["sha256"].as_str().expect("code digest").to_owned(),
            entry["row_count"].as_u64().expect("code rows"),
            entry["bytes"].as_u64().expect("code bytes"),
        )
    });
    let code_source_contract_body = json!({
        "version": 1,
        "source_identity": source_identity,
        "source_run_contract_sha256": source_contract_sha256,
        "file_count": code_files.len(),
        "row_count": code_files.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
        "byte_count": code_files.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
        "files": code_files,
    });
    let code_source_contract_sha256 = json_sha256(&code_source_contract_body);
    let mut code_source_contract = code_source_contract_body.as_object().unwrap().clone();
    code_source_contract.insert("source_key".to_owned(), json!(0));
    code_source_contract.insert(
        "contract_sha256".to_owned(),
        json!(code_source_contract_sha256),
    );
    let code_source_contracts = vec![Value::Object(code_source_contract)];

    let serving_entries = serving_events
        .iter()
        .map(|event| {
            json!({
                "path": event["path"],
                "partition": event["partition"],
                "partition_count": event["partition_count"],
                "source_key": 0,
                "source_count": 1,
                "row_count": event["row_count"],
                "bytes": event["bytes"],
                "sha256": event["sha256"],
                "format": event["format"],
                "version": event["version"],
            })
        })
        .collect::<Vec<_>>();
    let code_entries = code_events
        .iter()
        .map(|event| {
            json!({
                "path": event["path"],
                "source_key": 0,
                "source_count": 1,
                "source_run_contract_sha256": source_contract_sha256,
                "code_dictionary_contract_sha256": code_source_contract_sha256,
                "row_count": event["row_count"],
                "bytes": event["bytes"],
                "sha256": event["sha256"],
                "format": event["format"],
                "version": event["version"],
            })
        })
        .collect::<Vec<_>>();

    let provider_events = scanner_artifacts(payloads, "provider-set-metadata.copy");
    assert!(
        !provider_events.is_empty(),
        "scanner must emit provider metadata"
    );
    let provider_entries = provider_events
        .iter()
        .map(|event| {
            let artifact = artifact_path(event);
            json!({
                "path": artifact,
                "source_key": 0,
                "source_count": 1,
                "physical_source_identity": source_identity,
                "source_run_contract_sha256": source_contract_sha256,
                "row_count": event["row_count"],
                "bytes": event["bytes"],
                "sha256": file_sha256(&artifact),
                "format": "ptg2_v3_provider_set_metadata_copy",
                "version": 1,
            })
        })
        .collect::<Vec<_>>();
    let code_contracts = code_entries
        .iter()
        .map(|entry| {
            json!({
                "source_key": entry["source_key"],
                "row_count": entry["row_count"],
                "bytes": entry["bytes"],
                "sha256": entry["sha256"],
                "source_run_contract_sha256": entry["source_run_contract_sha256"],
                "code_dictionary_contract_sha256": entry["code_dictionary_contract_sha256"],
            })
        })
        .collect::<Vec<_>>();
    let provider_contracts = provider_entries
        .iter()
        .map(|entry| {
            json!({
                "source_key": entry["source_key"],
                "row_count": entry["row_count"],
                "bytes": entry["bytes"],
                "sha256": entry["sha256"],
                "source_run_contract_sha256": entry["source_run_contract_sha256"],
            })
        })
        .collect::<Vec<_>>();
    let manifest = json!({
        "source_count": 1,
        "source_run_contracts": source_contracts,
        "source_run_contract_set_sha256": json_sha256(&json!({"source_run_contracts": source_contracts})),
        "code_dictionary_source_contracts": code_source_contracts,
        "code_dictionary_source_contract_set_sha256": json_sha256(&json!({"code_dictionary_source_contracts": code_source_contracts})),
        "serving_run_partition_files": serving_entries,
        "serving_run_code_dictionary_files": code_entries,
        "provider_set_metadata_files": provider_entries,
        "expected_serving_run_files": serving_entries.len(),
        "expected_serving_run_rows": serving_entries.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
        "expected_serving_run_bytes": serving_entries.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
        "expected_code_dictionary_files": code_entries.len(),
        "expected_code_dictionary_rows": code_entries.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
        "expected_code_dictionary_bytes": code_entries.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
        "code_dictionary_contract_set_sha256": json_sha256(&json!({"code_dictionary_contracts": code_contracts})),
        "expected_provider_set_metadata_files": provider_entries.len(),
        "expected_provider_set_metadata_rows": provider_entries.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
        "expected_provider_set_metadata_bytes": provider_entries.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
        "provider_set_metadata_contract_set_sha256": json_sha256(&json!({"provider_set_metadata_contracts": provider_contracts})),
    });
    fs::write(path, serde_json::to_vec(&manifest).unwrap()).expect("write finalizer manifest");
}

fn run_v3_finalizer(
    output: &Path,
    manifest: &Path,
    price_key_map: &Path,
    price_key_count: usize,
) -> Output {
    Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .arg("--finalize-v3-runs")
        .arg(output)
        .args(["--price-key-map-input", price_key_map.to_str().unwrap()])
        .args(["--price-key-map-row-count", &price_key_count.to_string()])
        .args(["--workers", "2"])
        .args(["--identity-map-max-bytes", "268435456"])
        .args(["--total-sort-memory-bytes", "33554432"])
        .args(["--scratch-durability", "ephemeral"])
        .arg(manifest)
        .env("HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION", "none")
        .env("HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES", "65536")
        .env("HLTHPRT_PTG2_RATE_SCHEDULE_OBSERVE", "true")
        .output()
        .expect("run V3 finalizer")
}

fn run_compact_v4(source: &Path, output: &Path) -> Output {
    let serving = output.join("serving");
    let witness_scratch = output.join("witness-scratch");
    fs::create_dir_all(&serving).expect("create serving directory");
    fs::create_dir_all(&witness_scratch).expect("create witness scratch directory");
    Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"))
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", &serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "11".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", sha256_hex(RAW_MRF))
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", &witness_scratch)
        .env("HLTHPRT_PTG2_RUST_GROUP_NEGOTIATED_RATE_CHUNKS", "false")
        .env("HLTHPRT_PTG2_PROVIDER_GRAPH_V4", "true")
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH",
            output.join("provider-set-component.ptg2sc"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_COMPONENT_GROUP_SIDECAR_PATH",
            output.join("provider-component-group.ptg2sc"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
            output.join("provider-set-metadata.copy"),
        )
        .env(
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH",
            output.join("price-set-summary.copy"),
        )
        .env("HLTHPRT_PTG2_MANIFEST_SIDECAR_SPILL", "true")
        .env("HLTHPRT_PTG2_RUST_WORKERS", "2")
        .env("HLTHPRT_PTG2_RUST_WORK_QUEUE", "2")
        .env("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true")
        .env("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true")
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false")
        .env("HLTHPRT_PTG2_COMPACT_SNAPSHOT_ID", "snapshot-v4-test")
        .env("HLTHPRT_PTG2_COMPACT_PLAN_ID", "plan-v4-test")
        .env("HLTHPRT_PTG2_COMPACT_PLAN_MONTH_ID", "2026-07")
        .env(
            "HLTHPRT_PTG2_COMPACT_SOURCE_TRACE_SET_HASH",
            "trace-v4-test",
        )
        .env("HLTHPRT_PTG2_COMPACT_CONFIDENCE_CODE", "coverage-test")
        .output()
        .expect("run compact V4 scanner")
}

#[test]
fn compact_cli_emits_exact_v4_factors_and_source_witnesses() {
    let temporary = tempfile::tempdir().expect("temporary fixture root");
    let source = temporary.path().join("rates.json");
    let output = temporary.path().join("output");
    fs::create_dir(&output).expect("create output directory");
    fs::write(&source, RAW_MRF).expect("write MRF fixture");

    let completed = run_compact_v4(&source, &output);
    assert!(
        completed.status.success(),
        "scanner failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let records = String::from_utf8(completed.stdout).expect("UTF-8 scanner output");
    let payloads = records
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let summary = payloads
        .iter()
        .find(|payload| {
            payload
                .get("provider_graph_v4_factor_cache_entries")
                .is_some()
        })
        .unwrap_or_else(|| panic!("parallel scanner summary in:\n{records}"));
    assert_eq!(summary["top_level_byte_scan_selected"], true);
    assert_eq!(summary["provider_graph_v4_factor_mode"], true);
    assert_eq!(summary["provider_graph_v4_factor_cache_entries"], 3);
    assert_eq!(summary["provider_graph_v4_npi_union_attempts"], 3);
    assert_eq!(summary["provider_graph_v4_flat_group_union_attempts"], 3);
    assert!(summary["provider_graph_v4_reference_only_rates"]
        .as_u64()
        .is_some_and(|value| value >= 2));
    assert_eq!(summary["provider_graph_v4_inline_only_rates"], 1);

    for name in [
        "provider-set-component.ptg2sc",
        "provider-component-group.ptg2sc",
    ] {
        assert!(
            fs::metadata(output.join(name))
                .expect("factor sidecar")
                .len()
                > 0
        );
    }
    let witness = payloads
        .iter()
        .find(|payload| payload["contract"] == "ptg2_v3_source_witness_v3")
        .expect("source witness summary");
    assert_eq!(witness["provider_population_count"], 2);
    assert_eq!(witness["queryable_occurrence_population_count"], 9);
    assert_eq!(witness["occurrence_witness_count"], 9);
    assert!(
        fs::metadata(witness["path"].as_str().expect("witness path"))
            .expect("witness bundle")
            .len()
            > 0
    );

    let manifest = output.join("scanner-manifest.json");
    write_finalizer_manifest(&payloads, &manifest);
    let price_key_map = output.join("price-key-map.copy");
    let price_key_count = write_price_key_map(&payloads, &price_key_map);
    let finalizer_output = output.join("finalized");
    let finalized = run_v3_finalizer(
        &finalizer_output,
        &manifest,
        &price_key_map,
        price_key_count,
    );
    assert!(
        finalized.status.success(),
        "finalizer failed: {}\nstdout:\n{}",
        String::from_utf8_lossy(&finalized.stderr),
        String::from_utf8_lossy(&finalized.stdout),
    );
    let finalizer_payloads = String::from_utf8(finalized.stdout)
        .expect("UTF-8 finalizer output")
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .collect::<Vec<_>>();
    let finalizer_summary = finalizer_payloads
        .iter()
        .find(|payload| payload["format"] == "ptg2_v3_direct_finalizer_v3")
        .expect("V3 finalizer summary");
    assert_eq!(
        finalizer_summary["preservation"]["all_source_occurrences_preserved"],
        true
    );
    assert_eq!(finalizer_summary["rate_schedule_observe"]["enabled"], true);
    for name in [
        "summary.json",
        "shared_serving_blocks.copy",
        "shared_price_dictionary_blocks.copy",
        "code_dictionary.copy",
        "provider_set_dictionary.copy",
    ] {
        assert!(finalizer_output.join(name).is_file(), "missing {name}");
    }
}
