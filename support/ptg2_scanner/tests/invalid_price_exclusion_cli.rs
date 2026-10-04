use flate2::read::ZlibDecoder;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Cursor, Read};
use std::process::Command;

const RAW_MRF: &[u8] = include_bytes!("fixtures/compact_v4_mrf.json");

fn sha256_hex(payload: &[u8]) -> String {
    lower_hex(&Sha256::digest(payload))
}
fn lower_hex(payload: &[u8]) -> String {
    payload.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn invalid_price_expectation(
    raw_source_sha256: &str,
    entries: &[(u64, u64, u64, &str)],
    emptied_rate_count: u64,
) -> serde_json::Value {
    let mut source_digest = Sha256::new();
    source_digest.update(b"PTG2_INVALID_PRICE_EXCLUSION_SOURCE_V1\0");
    let mut entries = entries.to_vec();
    entries.sort_unstable_by_key(|entry| (entry.0, entry.1, entry.2));
    let entries = entries
        .iter()
        .map(
            |&(object_ordinal, rate_ordinal, price_ordinal, invalid_value)| {
                let mut value_digest = Sha256::new();
                value_digest.update(b"PTG2_INVALID_PRICE_EXCLUSION_VALUE_V1\0");
                value_digest.update(invalid_value.as_bytes());
                let value_digest = value_digest.finalize();
                source_digest.update(object_ordinal.to_be_bytes());
                source_digest.update(rate_ordinal.to_be_bytes());
                source_digest.update(price_ordinal.to_be_bytes());
                source_digest.update(value_digest);
                serde_json::json!({
                    "object_ordinal": object_ordinal,
                    "rate_ordinal": rate_ordinal,
                    "price_ordinal": price_ordinal,
                    "invalid_value_sha256": lower_hex(&value_digest),
                })
            },
        )
        .collect::<Vec<_>>();
    serde_json::json!({
        "contract": "ptg2_invalid_price_exclusion_source_v1",
        "reason": "invalid_iso_calendar_date",
        "raw_source_sha256": raw_source_sha256,
        "excluded_price_count": entries.len(),
        "emptied_rate_count": emptied_rate_count,
        "entries": entries,
        "sha256": lower_hex(&source_digest.finalize()),
    })
}

fn compact_exclusion_command(
    source: &std::path::Path,
    output: &std::path::Path,
    serving: &std::path::Path,
    witness_scratch: &std::path::Path,
    raw_source_sha256: &str,
    expectation: &serde_json::Value,
) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ptg2_scanner"));
    command
        .args(["--compact-serving", source.to_str().expect("UTF-8 source")])
        .env("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
        .env("HLTHPRT_PTG2_V3_SERVING_RUN_DIR", serving)
        .env("HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID", "44".repeat(32))
        .env("HLTHPRT_PTG2_RAW_SOURCE_SHA256", raw_source_sha256)
        .env("HLTHPRT_PTG2_SOURCE_WITNESS_SCRATCH_DIR", witness_scratch)
        .env(
            "HLTHPRT_PTG2_INVALID_PRICE_EXCLUSION_JSON",
            expectation.to_string(),
        )
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
        .env("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false");
    command
}

fn framed_payload(stdout: &[u8], target: &str) -> serde_json::Value {
    let mut offset = 0usize;
    while offset < stdout.len() {
        let header_end = stdout[offset..]
            .iter()
            .position(|byte| *byte == b'\n')
            .map(|relative| offset + relative)
            .expect("framed output header");
        let header = std::str::from_utf8(&stdout[offset..header_end]).expect("UTF-8 header");
        let (kind, length) = header
            .split_once('\t')
            .expect("framed output kind and length");
        let length: usize = length.parse().expect("framed output length");
        let payload_start = header_end + 1;
        let payload_end = payload_start + length;
        if kind == target {
            return serde_json::from_slice(&stdout[payload_start..payload_end])
                .expect("JSON frame payload");
        }
        offset = payload_end + usize::from(stdout.get(payload_end) == Some(&b'\n'));
    }
    panic!("missing {target} frame")
}

fn witness_array<const N: usize>(cursor: &mut Cursor<&[u8]>) -> [u8; N] {
    let mut value = [0; N];
    cursor
        .read_exact(&mut value)
        .expect("complete witness field");
    value
}

fn witness_u32(cursor: &mut Cursor<&[u8]>) -> usize {
    u32::from_be_bytes(witness_array(cursor)) as usize
}

fn witness_slice<'a>(cursor: &mut Cursor<&'a [u8]>, length: usize) -> &'a [u8] {
    let start = cursor.position() as usize;
    let end = start.checked_add(length).expect("witness field length");
    let value = cursor
        .get_ref()
        .get(start..end)
        .expect("complete witness field");
    cursor.set_position(end as u64);
    value
}

fn decoded_witness_frame(cursor: &mut Cursor<&[u8]>, raw_limit: usize, overhead: usize) -> Vec<u8> {
    let length = witness_u32(cursor);
    let framed_length = length
        .checked_add(overhead)
        .expect("stored witness frame length");
    assert!(length > 0 && framed_length <= 8 * 1024 * 1024);
    let mut decoder = ZlibDecoder::new(witness_slice(cursor, length));
    let mut raw = Vec::new();
    (&mut decoder)
        .take(raw_limit as u64 + 1)
        .read_to_end(&mut raw)
        .expect("valid witness zlib frame");
    assert!(raw.len() <= raw_limit);
    assert_eq!(decoder.total_in(), length as u64);
    raw
}

fn source_witness_header(witness: &serde_json::Value) -> serde_json::Value {
    let bundle = fs::read(witness["path"].as_str().expect("witness path"))
        .expect("read source witness bundle");
    assert_eq!(witness["format_version"], 4);
    assert_eq!(witness["byte_count"], bundle.len());
    assert!(bundle.len() <= 512 * 1024 * 1024);
    assert_eq!(witness["sha256"], sha256_hex(&bundle));
    let mut cursor = Cursor::new(bundle.as_slice());
    assert_eq!(&witness_array::<8>(&mut cursor), b"PTG2SW04");
    let header_length = witness_u32(&mut cursor);
    assert!((1..=8 * 1024 * 1024 - 4).contains(&header_length));
    let header: serde_json::Value =
        serde_json::from_slice(witness_slice(&mut cursor, header_length))
            .expect("source witness header");
    assert_eq!(header["format_version"], 4);
    assert_eq!(header["raw_source_sha256"], witness["raw_source_sha256"]);
    assert_eq!(header["evidence_encoding"], "fixed_byte_fragments_v1");
    assert_eq!(header["fragment_byte_count"], 4096);

    let fragment_count = witness_u32(&mut cursor);
    assert!((1..=153072).contains(&fragment_count));
    assert_eq!(header["fragment_count"], fragment_count);
    let mut fragments = BTreeMap::new();
    for _ in 0..fragment_count {
        let digest = witness_array::<32>(&mut cursor);
        assert!(fragments
            .last_key_value()
            .is_none_or(|(previous, _)| previous < &digest));
        let raw_length = witness_u32(&mut cursor);
        assert!((1..=4096).contains(&raw_length));
        let raw = decoded_witness_frame(&mut cursor, raw_length, 40);
        assert_eq!(raw.len(), raw_length);
        assert_eq!(<[u8; 32]>::from(Sha256::digest(&raw)), digest);
        fragments.insert(digest, raw);
    }
    assert!(fragments.values().map(Vec::len).sum::<usize>() <= 512 * 1024 * 1024);
    let recipe_count = witness_u32(&mut cursor);
    assert!((1..=22000).contains(&recipe_count));
    let mut recipes = BTreeSet::new();
    let mut used_fragments = BTreeSet::new();
    let mut reference_count = 0usize;
    let mut reconstructed_bytes = 0usize;
    for _ in 0..recipe_count {
        let digest = witness_array::<32>(&mut cursor);
        assert!(recipes.last().is_none_or(|previous| previous < &digest));
        let raw_length = witness_u32(&mut cursor);
        assert!((1..=64 * 1024 * 1024).contains(&raw_length));
        let count = witness_u32(&mut cursor);
        assert_eq!(count, raw_length.div_ceil(4096));
        let mut raw_digest = Sha256::new();
        for index in 0..count {
            let fragment_digest = witness_array::<32>(&mut cursor);
            let raw = fragments
                .get(&fragment_digest)
                .expect("known recipe fragment");
            assert_eq!(raw.len(), 4096.min(raw_length - index * 4096));
            raw_digest.update(raw);
            used_fragments.insert(fragment_digest);
        }
        assert_eq!(<[u8; 32]>::from(raw_digest.finalize()), digest);
        recipes.insert(digest);
        reference_count += count;
        reconstructed_bytes += raw_length;
    }
    assert_eq!(used_fragments.len(), fragments.len());
    assert!(reference_count <= 16777216);
    assert!(reconstructed_bytes as u64 <= 64 * 1024 * 1024 * 1024);
    assert_eq!(header["recipe_reference_count"], reference_count);
    assert_eq!(header["evidence_reconstructed_bytes"], reconstructed_bytes);

    let record_count = witness_u32(&mut cursor);
    assert!((1..=11000).contains(&record_count));
    assert_eq!(witness["row_count"], record_count);
    let recipe_hex = recipes
        .iter()
        .map(|digest| lower_hex(digest))
        .collect::<BTreeSet<_>>();
    let mut used_recipes = BTreeSet::new();
    let mut rate_count = 0usize;
    let mut provider_count = 0usize;
    for _ in 0..record_count {
        let raw = decoded_witness_frame(&mut cursor, 64 * 1024 * 1024, 4);
        let mut record = Cursor::new(raw.as_slice());
        assert_eq!(&witness_array::<8>(&mut record), b"PTG2SWR2");
        let metadata_length = witness_u32(&mut record);
        let metadata: serde_json::Value =
            serde_json::from_slice(witness_slice(&mut record, metadata_length))
                .expect("witness record metadata");
        assert_eq!(metadata["contract"], "ptg2_v3_source_witness_record_v2");
        assert_eq!(witness_u32(&mut record), 0);
        assert_eq!(witness_u32(&mut record), 0);
        assert_eq!(record.position(), raw.len() as u64);
        for field in ["raw_sha256", "linked_provider_sha256"] {
            if field == "raw_sha256" || !metadata[field].is_null() {
                let digest = metadata[field].as_str().expect("source token digest");
                assert!(recipe_hex.contains(digest));
                used_recipes.insert(digest.to_owned());
            }
        }
        match metadata["kind"].as_str().expect("witness kind") {
            "rate_occurrence" => {
                assert_eq!(
                    provider_count, 0,
                    "rate witnesses precede provider witnesses"
                );
                rate_count += 1;
            }
            "provider_reference" => provider_count += 1,
            kind => panic!("unexpected witness kind {kind}"),
        }
    }
    assert_eq!(used_recipes, recipe_hex);
    assert_eq!(cursor.position(), bundle.len() as u64);
    assert_eq!(header["rate_occurrence"]["selected_count"], rate_count);
    assert_eq!(
        header["provider_reference"]["selected_count"],
        provider_count
    );
    assert_eq!(witness["occurrence_witness_count"], rate_count);
    assert_eq!(witness["provider_witness_count"], provider_count);
    assert_eq!(
        header["rate_occurrence"]["population_count"],
        witness["queryable_occurrence_population_count"]
    );
    assert_eq!(
        header["provider_reference"]["population_count"],
        witness["provider_population_count"]
    );
    assert_eq!(
        rate_count as u64,
        witness["queryable_occurrence_population_count"]
            .as_u64()
            .unwrap()
            .min(10000)
    );
    assert_eq!(
        provider_count as u64,
        witness["provider_population_count"]
            .as_u64()
            .unwrap()
            .min(1000)
    );
    header
}

#[test]
fn legacy_compact_scan_applies_only_the_exact_invalid_price_exclusion() {
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
    let rate = &mut fixture["in_network"][0]["negotiated_rates"][0];
    rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"},
        {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"},
        {"negotiated_type": "negotiated", "negotiated_rate": 12, "expiration_date": "2029-03-01"}
    ]);
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
    let raw_source_sha256 = sha256_hex(&raw);
    let expectation = invalid_price_expectation(&raw_source_sha256, &[(0, 0, 1, "2027-02-30")], 0);

    let completed = compact_exclusion_command(
        &source,
        &output,
        &serving,
        &witness_scratch,
        &raw_source_sha256,
        &expectation,
    )
    .output()
    .expect("run legacy compact scanner");

    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let summary = framed_payload(&completed.stdout, "scanner_summary");
    assert_eq!(
        summary["invalid_price_exclusion"],
        serde_json::json!({
            "contract": "ptg2_invalid_price_exclusion_source_v1",
            "reason": "invalid_iso_calendar_date",
            "excluded_price_count": 1,
            "emptied_rate_count": 0,
            "sha256": expectation["sha256"],
        })
    );
    let price_copy = fs::read_dir(&output)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .unwrap()
                .to_string_lossy()
                .contains("price-atom")
        })
        .expect("price atom output");
    let price_copy = fs::read_to_string(price_copy).expect("UTF-8 price atom COPY");
    assert!(price_copy.contains("2028-02-29"));
    assert!(price_copy.contains("2029-03-01"));
    assert!(!price_copy.contains("2027-02-30"));
    source_witness_header(&framed_payload(
        &completed.stdout,
        "source_audit_witness_file",
    ));
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
}

#[test]
fn invalid_price_exclusion_mismatch_is_failure_atomic() {
    for case in [
        "extra_expected_price",
        "wrong_emptied_rate_count",
        "missing_in_network",
    ] {
        let temporary = tempfile::tempdir().expect("temporary fixture root");
        let source = temporary.path().join("rates.json");
        let output = temporary.path().join("output");
        let serving = output.join("serving");
        let witness_scratch = output.join("witness-scratch");
        fs::create_dir(&output).expect("create output directory");
        fs::create_dir(&serving).expect("create serving directory");
        fs::create_dir(&witness_scratch).expect("create witness scratch directory");
        let output_baseline = output.join("baseline.keep");
        let serving_baseline = serving.join("ptg2-v3-serving-baseline.ready");
        fs::write(&output_baseline, b"output baseline").expect("seed output baseline");
        fs::write(&serving_baseline, b"serving baseline").expect("seed serving baseline");

        let mut fixture: serde_json::Value =
            serde_json::from_slice(RAW_MRF).expect("parse compact source fixture");
        if case == "missing_in_network" {
            fixture
                .as_object_mut()
                .expect("source object")
                .remove("in_network");
        } else {
            fixture["in_network"][0]["negotiated_rates"][0]["negotiated_prices"] = serde_json::json!([
                {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"},
                {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"},
                {"negotiated_type": "negotiated", "negotiated_rate": 12, "expiration_date": "2029-03-01"}
            ]);
            for record in fixture["in_network"]
                .as_array_mut()
                .expect("in-network records")
            {
                record
                    .as_object_mut()
                    .expect("in-network object")
                    .remove("negotiation_arrangement");
            }
        }
        let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
        fs::write(&source, &raw).expect("write compact source fixture");
        let raw_source_sha256 = sha256_hex(&raw);
        let expectation = match case {
            "extra_expected_price" => invalid_price_expectation(
                &raw_source_sha256,
                &[(0, 0, 1, "2027-02-30"), (0, 0, 3, "2027-02-31")],
                0,
            ),
            "wrong_emptied_rate_count" => {
                invalid_price_expectation(&raw_source_sha256, &[(0, 0, 1, "2027-02-30")], 1)
            }
            "missing_in_network" => {
                invalid_price_expectation(&raw_source_sha256, &[(0, 0, 0, "2027-02-30")], 0)
            }
            _ => unreachable!(),
        };
        let completed = compact_exclusion_command(
            &source,
            &output,
            &serving,
            &witness_scratch,
            &raw_source_sha256,
            &expectation,
        )
        .env("HLTHPRT_PTG2_COMPACT_SERVING_COPY_ROTATE_BYTES", "1")
        .env("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "1")
        .output()
        .expect("run mismatched exclusion scanner");

        assert!(!completed.status.success(), "{case}");
        assert!(
            String::from_utf8_lossy(&completed.stderr)
                .contains("observed invalid price exclusions do not match the exact expectation"),
            "{case}: {}",
            String::from_utf8_lossy(&completed.stderr),
        );
        assert!(String::from_utf8_lossy(&completed.stderr).contains("PTG2_SCANNER_PROGRESS"));
        assert!(completed
            .stdout
            .windows(b"scanner_config\t".len())
            .any(|window| window == b"scanner_config\t"));
        for terminal_marker in [
            b"_copy_file\t".as_slice(),
            b"v3_serving_run_partition_file\t".as_slice(),
            b"v3_serving_code_dictionary_file\t".as_slice(),
            b"scanner_summary\t".as_slice(),
            b"source_audit_witness_file\t".as_slice(),
        ] {
            assert!(
                !completed
                    .stdout
                    .windows(terminal_marker.len())
                    .any(|window| window == terminal_marker),
                "{case}: {}",
                String::from_utf8_lossy(&completed.stdout),
            );
        }
        assert_eq!(fs::read(&output_baseline).unwrap(), b"output baseline");
        assert_eq!(fs::read(&serving_baseline).unwrap(), b"serving baseline");
        assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
        let mut output_entries = fs::read_dir(&output)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.file_name())
            .collect::<Vec<_>>();
        output_entries.sort();
        assert_eq!(
            output_entries,
            ["baseline.keep", "serving", "witness-scratch"]
                .map(std::ffi::OsString::from)
                .to_vec(),
            "{case}",
        );
        assert_eq!(
            fs::read_dir(&serving)
                .unwrap()
                .filter_map(Result::ok)
                .map(|entry| entry.path())
                .collect::<Vec<_>>(),
            vec![serving_baseline],
            "{case}",
        );
    }
}

#[test]
fn all_invalid_price_exclusion_records_one_unqueryable_rate() {
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
    fixture["in_network"]
        .as_array_mut()
        .expect("in-network records")
        .truncate(1);
    let rates = fixture["in_network"][0]["negotiated_rates"]
        .as_array_mut()
        .expect("negotiated rates");
    let mut valid_rate = rates[0].clone();
    valid_rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 10, "expiration_date": "2028-02-29"}
    ]);
    let mut excluded_rate = rates[0].clone();
    excluded_rate["negotiated_prices"] = serde_json::json!([
        {"negotiated_type": "negotiated", "negotiated_rate": 11, "expiration_date": "2027-02-30"}
    ]);
    rates.clear();
    rates.extend([valid_rate, excluded_rate]);
    fixture["in_network"][0]
        .as_object_mut()
        .expect("in-network object")
        .remove("negotiation_arrangement");
    let raw = serde_json::to_vec(&fixture).expect("encode compact source fixture");
    fs::write(&source, &raw).expect("write compact source fixture");
    let raw_source_sha256 = sha256_hex(&raw);
    let expectation = invalid_price_expectation(&raw_source_sha256, &[(0, 1, 0, "2027-02-30")], 1);

    let completed = compact_exclusion_command(
        &source,
        &output,
        &serving,
        &witness_scratch,
        &raw_source_sha256,
        &expectation,
    )
    .env("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "1")
    .output()
    .expect("run all-invalid exclusion scanner");
    assert!(
        completed.status.success(),
        "scanner failed:\n{}\nstdout:\n{}",
        String::from_utf8_lossy(&completed.stderr),
        String::from_utf8_lossy(&completed.stdout),
    );
    let summary = framed_payload(&completed.stdout, "scanner_summary");
    assert_eq!(
        summary["invalid_price_exclusion"]["excluded_price_count"],
        1
    );
    assert_eq!(summary["invalid_price_exclusion"]["emptied_rate_count"], 1);
    assert_eq!(
        framed_payload(&completed.stdout, "v3_serving_run_partition_file")["row_count"],
        1,
    );
    let witness = framed_payload(&completed.stdout, "source_audit_witness_file");
    let witness_header = source_witness_header(&witness);
    assert_eq!(
        witness_header["rate_occurrence"]["emitted_rate_row_count"],
        2,
    );
    assert_eq!(
        witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
        1,
    );
    let price_copy = fs::read_dir(&output)
        .unwrap()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .find(|path| {
            path.file_name()
                .is_some_and(|name| name.to_string_lossy().contains("price-atom"))
        })
        .expect("price atom output");
    let price_copy = fs::read_to_string(price_copy).expect("UTF-8 price atom COPY");
    assert_eq!(price_copy.lines().count(), 1);
    assert!(price_copy.contains("2028-02-29"));
    assert!(!price_copy.contains("2027-02-30"));
    assert!(fs::read_dir(&witness_scratch).unwrap().next().is_none());
}
