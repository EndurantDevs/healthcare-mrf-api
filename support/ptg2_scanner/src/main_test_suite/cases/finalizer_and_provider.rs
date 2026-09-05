use super::*;

#[test]
fn v4_finalizer_pack_emits_exact_atomic_native_artifacts() {
    let directory = tempfile::tempdir().unwrap();
    let (price_rows, serving_rows, price, serving) =
        v4_finalizer_pack_success_fixture(directory.path());
    let manifest = v4_finalizer_pack_test_manifest(directory.path(), &price, &serving);
    let output = directory.path().join("packed");
    let summary = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: output.clone(),
        manifest_path: manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .unwrap();

    assert_eq!(summary["format"], V4_FINALIZER_PACK_FORMAT);
    assert_eq!(summary["coordinates_per_pack"], 2);
    assert_eq!(summary["canonical_mapping_count"], 7);
    assert_eq!(summary["canonical_byte_count"], 559);
    assert_eq!(
        summary["canonical_mapping_digest"],
        "0f7bd15dd6890eff5f0d3e83f6be32fe7522374676533f37743a4420df63aab8"
    );
    assert_eq!(
        summary["target_identity_digest"],
        "d226609a59a4cd1259d044d5b2a8cb1f8e72cca1cf455d6b85cdbcd70efc6230"
    );
    assert_eq!(
        summary["map_digest"],
        "7deec5a7085aac5d976587ede6bf1645ea760f6aabbbdbc664b27482bf44a0a6"
    );
    assert_eq!(summary["target_block_count"], 6);
    assert!(summary["elapsed_seconds"].as_f64().unwrap().is_finite());
    assert!(summary["elapsed_seconds"].as_f64().unwrap() >= 0.0);
    assert_eq!(summary["lanes"][0]["name"], "price_dictionary");
    assert_eq!(summary["lanes"][0]["coordinate_count"], 2);
    assert_eq!(summary["lanes"][0]["target_block_count"], 1);
    assert_eq!(summary["lanes"][0]["target_stored_byte_count"], 10);
    assert_eq!(summary["lanes"][0]["map_pack_count"], 1);
    assert_eq!(summary["lanes"][1]["name"], "serving");
    assert_eq!(summary["lanes"][1]["coordinate_count"], 5);
    assert_eq!(summary["lanes"][1]["target_block_count"], 5);
    assert_eq!(summary["lanes"][1]["target_stored_byte_count"], 15);
    assert_eq!(summary["lanes"][1]["map_pack_count"], 5);

    let mut canonical_digest = Sha256::new();
    canonical_digest.update(V4_FINALIZER_PACK_MAPPING_HASH_DOMAIN);
    let mut canonical_bytes = 0usize;
    for row in price_rows.iter().chain(&serving_rows) {
        let block_hash = shared_v3_block_hash(
            PTG2_V3_SHARED_BLOCK_FORMAT_VERSION,
            row.object_kind,
            "none",
            &row.payload,
        )
        .unwrap();
        let mut record = Vec::new();
        update_sha256_length_prefixed(&mut canonical_digest, row.object_kind.as_bytes()).unwrap();
        record.extend_from_slice(&row.block_key.to_be_bytes());
        record.extend_from_slice(&(row.fragment_no as u32).to_be_bytes());
        record.extend_from_slice(&(row.entry_count as u64).to_be_bytes());
        record.extend_from_slice(&block_hash);
        canonical_digest.update(&record);
        canonical_bytes += 4 + row.object_kind.len() + record.len();
    }
    assert_eq!(
        summary["canonical_mapping_digest"],
        sha256_hex(&canonical_digest.finalize())
    );
    assert_eq!(summary["canonical_byte_count"], canonical_bytes);

    let mut unique_hashes = price
        .5
        .iter()
        .chain(&serving.5)
        .copied()
        .collect::<Vec<_>>();
    unique_hashes.sort_unstable();
    unique_hashes.dedup();
    let mut target_digest = Sha256::new();
    target_digest.update(V4_FINALIZER_PACK_TARGET_HASH_DOMAIN);
    for block_hash in unique_hashes {
        target_digest.update(block_hash);
    }
    assert_eq!(
        summary["target_identity_digest"],
        sha256_hex(&target_digest.finalize())
    );

    let price_targets =
        v4_finalizer_pack_copy_rows(&output.join("price_dictionary/target_blocks.copy"), 10);
    assert_eq!(price_targets.len(), 1);
    let price_map_blocks =
        v4_finalizer_pack_copy_rows(&output.join("price_dictionary/map_blocks.copy"), 10);
    assert_eq!(price_map_blocks.len(), 1);
    let map_payload = &price_map_blocks[0][9];
    assert_eq!(&map_payload[..8], V4_FINALIZER_MAP_MAGIC);
    assert_eq!(
        u16::from_be_bytes(map_payload[8..10].try_into().unwrap()),
        1
    );
    assert_eq!(
        u32::from_be_bytes(map_payload[12..16].try_into().unwrap()),
        2
    );
    assert_eq!(map_payload.len(), 80 + 2 * 52);
    assert_eq!(
        price_map_blocks[0][0],
        shared_v3_block_hash(
            PTG2_V3_SHARED_BLOCK_FORMAT_VERSION,
            V4_FINALIZER_MAP_BLOCK_KIND,
            "none",
            map_payload,
        )
        .unwrap()
    );
    for lane in ["price_dictionary", "serving"] {
        for artifact in ["target_blocks", "map_blocks", "map_packs"] {
            let receipt = &summary["lanes"]
                .as_array()
                .unwrap()
                .iter()
                .find(|value| value["name"] == lane)
                .unwrap()[artifact];
            let path = PathBuf::from(receipt["path"].as_str().unwrap());
            assert_eq!(path.metadata().unwrap().len(), receipt["byte_count"]);
            assert_eq!(sha256_hex(&sha256_file(&path).unwrap()), receipt["sha256"]);
        }
    }
    let persisted_summary: Value =
        serde_json::from_reader(File::open(output.join("summary.json")).unwrap()).unwrap();
    assert_eq!(persisted_summary, summary);
    let mut output_names = std::fs::read_dir(&output)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect::<Vec<_>>();
    output_names.sort();
    assert_eq!(
        output_names,
        ["price_dictionary", "serving", "summary.json"]
            .map(std::ffi::OsString::from)
            .to_vec()
    );
}
#[test]
fn v4_finalizer_pack_geometry_covers_255_256_257_and_boundary_duplicates() {
    let directory = tempfile::tempdir().unwrap();
    let (_, serving_rows, _, serving) = v4_finalizer_pack_success_fixture(directory.path());
    for coordinate_count in [255usize, 256, 257] {
        let price_rows = (0..coordinate_count)
            .map(|block_key| V4FinalizerPackTestRow {
                object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
                block_key: block_key as i64,
                fragment_no: 0,
                entry_count: 1,
                payload: b"geometry".to_vec(),
                block_hash: None,
            })
            .collect::<Vec<_>>();
        let price = v4_finalizer_pack_test_copy(
            directory.path(),
            &format!("geometry-{coordinate_count}.copy"),
            &price_rows,
        );
        let manifest =
            v4_finalizer_pack_test_manifest_with_size(directory.path(), &price, &serving, 256);
        let output = directory
            .path()
            .join(format!("geometry-{coordinate_count}"));
        let summary = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
            output_directory: output.clone(),
            manifest_path: manifest,
            identity_map_max_bytes: 1024 * 1024,
        })
        .unwrap();

        let expected_pack_count = coordinate_count.div_ceil(256);
        assert_eq!(summary["lanes"][0]["map_pack_count"], expected_pack_count);
        let packs =
            v4_finalizer_pack_copy_rows(&output.join("price_dictionary/map_packs.copy"), 10);
        assert_eq!(packs.len(), expected_pack_count);
        for (pack_no, pack) in packs.iter().enumerate() {
            let first = pack_no * 256;
            let last = (first + 255).min(coordinate_count - 1);
            assert_eq!(
                i32::from_be_bytes(pack[1][..].try_into().unwrap()),
                pack_no as i32
            );
            assert_eq!(
                i64::from_be_bytes(pack[2][..].try_into().unwrap()),
                first as i64
            );
            assert_eq!(
                i64::from_be_bytes(pack[4][..].try_into().unwrap()),
                last as i64
            );
            assert_eq!(
                i32::from_be_bytes(pack[6][..].try_into().unwrap()),
                (last - first + 1) as i32
            );
        }

        let mut expected_digest = Sha256::new();
        expected_digest.update(V4_FINALIZER_PACK_MAPPING_HASH_DOMAIN);
        let mut expected_bytes = 0usize;
        for row in price_rows.iter().chain(&serving_rows) {
            let block_hash = shared_v3_block_hash(
                PTG2_V3_SHARED_BLOCK_FORMAT_VERSION,
                row.object_kind,
                "none",
                &row.payload,
            )
            .unwrap();
            let record_bytes = 4 + row.object_kind.len() + 8 + 4 + 8 + block_hash.len();
            update_sha256_length_prefixed(&mut expected_digest, row.object_kind.as_bytes())
                .unwrap();
            expected_digest.update(row.block_key.to_be_bytes());
            expected_digest.update((row.fragment_no as u32).to_be_bytes());
            expected_digest.update((row.entry_count as u64).to_be_bytes());
            expected_digest.update(block_hash);
            expected_bytes += record_bytes;
        }
        assert_eq!(
            summary["canonical_mapping_count"],
            coordinate_count + serving_rows.len()
        );
        assert_eq!(summary["canonical_byte_count"], expected_bytes);
        assert_eq!(
            summary["canonical_mapping_digest"],
            sha256_hex(&expected_digest.finalize())
        );
    }

    let mut duplicate_rows = (0..257)
        .map(|block_key| V4FinalizerPackTestRow {
            object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
            block_key,
            fragment_no: 0,
            entry_count: 1,
            payload: b"duplicate-boundary".to_vec(),
            block_hash: None,
        })
        .collect::<Vec<_>>();
    duplicate_rows[256].block_key = 255;
    let duplicate =
        v4_finalizer_pack_test_copy(directory.path(), "duplicate-boundary.copy", &duplicate_rows);
    let manifest =
        v4_finalizer_pack_test_manifest_with_size(directory.path(), &duplicate, &serving, 256);
    let output = directory.path().join("duplicate-boundary");
    let error = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: output.clone(),
        manifest_path: manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .unwrap_err();
    assert!(error.to_string().contains("not strictly ordered"));
    assert!(!output.exists());
    assert!(!std::fs::read_dir(directory.path())
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".ptg2-finalizer-")));
}
#[test]
fn v4_finalizer_pack_rejects_conflicts_caps_and_source_drift_without_residue() {
    let directory = tempfile::tempdir().unwrap();
    let (_, _, _, serving) = v4_finalizer_pack_success_fixture(directory.path());
    let shared_hash = shared_v3_block_hash(
        PTG2_V3_SHARED_BLOCK_FORMAT_VERSION,
        V4_FINALIZER_PACKED_OBJECT_KINDS[0],
        "none",
        b"conflict",
    )
    .unwrap();
    let conflict_rows = [
        V4FinalizerPackTestRow {
            object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
            block_key: 0,
            fragment_no: 0,
            entry_count: 1,
            payload: b"conflict".to_vec(),
            block_hash: Some(shared_hash),
        },
        V4FinalizerPackTestRow {
            object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
            block_key: 1,
            fragment_no: 0,
            entry_count: 2,
            payload: b"conflict".to_vec(),
            block_hash: Some(shared_hash),
        },
    ];
    let conflict = v4_finalizer_pack_test_copy(directory.path(), "conflict.copy", &conflict_rows);
    let conflict_manifest = v4_finalizer_pack_test_manifest(directory.path(), &conflict, &serving);
    let conflict_output = directory.path().join("conflict-output");
    let error = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: conflict_output.clone(),
        manifest_path: conflict_manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .unwrap_err();
    assert!(error.to_string().contains("conflicting metadata"));
    assert!(!conflict_output.exists());

    let unique_rows = [
        V4FinalizerPackTestRow {
            block_key: 0,
            entry_count: 1,
            payload: b"one".to_vec(),
            block_hash: None,
            ..conflict_rows[0].clone()
        },
        V4FinalizerPackTestRow {
            block_key: 1,
            entry_count: 1,
            payload: b"two".to_vec(),
            block_hash: None,
            ..conflict_rows[0].clone()
        },
    ];
    let capped = v4_finalizer_pack_test_copy(directory.path(), "capped.copy", &unique_rows);
    let capped_manifest = v4_finalizer_pack_test_manifest(directory.path(), &capped, &serving);
    let capped_output = directory.path().join("capped-output");
    let error = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: capped_output.clone(),
        manifest_path: capped_manifest,
        identity_map_max_bytes: V4_FINALIZER_TARGET_IDENTITY_BYTES_PER_ENTRY,
    })
    .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert!(!capped_output.exists());

    let aggregate_price =
        v4_finalizer_pack_test_copy(directory.path(), "aggregate.copy", &unique_rows[..1]);
    let aggregate_manifest =
        v4_finalizer_pack_test_manifest(directory.path(), &aggregate_price, &serving);
    let mut aggregate_manifest_value: Value =
        serde_json::from_reader(File::open(&aggregate_manifest).unwrap()).unwrap();
    aggregate_manifest_value["lanes"][0]["row_count"] = json!(2);
    std::fs::write(
        &aggregate_manifest,
        serde_json::to_vec(&aggregate_manifest_value).unwrap(),
    )
    .unwrap();
    let aggregate_output = directory.path().join("aggregate-output");
    let error = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: aggregate_output.clone(),
        manifest_path: aggregate_manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .unwrap_err();
    assert!(error.to_string().contains("source aggregates changed"));
    assert!(!aggregate_output.exists());

    let drift_price =
        v4_finalizer_pack_test_copy(directory.path(), "drift.copy", &unique_rows[..1]);
    let drift_manifest = v4_finalizer_pack_test_manifest(directory.path(), &drift_price, &serving);
    let mut drift_bytes = std::fs::read(&drift_price.0).unwrap();
    let last_payload = drift_bytes.len() - 3;
    drift_bytes[last_payload] ^= 1;
    std::fs::write(&drift_price.0, drift_bytes).unwrap();
    let drift_output = directory.path().join("drift-output");
    let error = pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: drift_output.clone(),
        manifest_path: drift_manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .unwrap_err();
    assert!(error.to_string().contains("source content changed"));
    assert!(!drift_output.exists());
    assert!(!std::fs::read_dir(directory.path())
        .unwrap()
        .any(|entry| entry
            .unwrap()
            .file_name()
            .to_string_lossy()
            .contains(".ptg2-finalizer-")));
}
#[test]
fn v4_finalizer_pack_cli_requires_one_manifest_and_explicit_memory_cap() {
    let parse = |values: &[&str]| {
        parse_v4_finalizer_pack_options(
            &values
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>(),
        )
    };
    for invalid in [
        vec![],
        vec!["--identity-map-max-bytes"],
        vec!["/tmp/packed", "--identity-map-max-bytes"],
        vec!["/tmp/packed", "--identity-map-max-bytes", "invalid"],
        vec!["/tmp/packed", "--unknown", "/tmp/manifest.json"],
        vec![
            "/tmp/packed",
            "--identity-map-max-bytes",
            "160",
            "--identity-map-max-bytes",
            "160",
            "/tmp/manifest.json",
        ],
        vec![
            "/tmp/packed",
            "--identity-map-max-bytes",
            "160",
            "/tmp/one.json",
            "/tmp/two.json",
        ],
        vec!["/tmp/packed", "--identity-map-max-bytes", "160"],
        vec!["/tmp/packed", "/tmp/manifest.json"],
    ] {
        assert!(parse(&invalid).is_err(), "accepted {invalid:?}");
    }
    let output = "/tmp/packed".to_owned();
    let manifest = "/tmp/manifest.json".to_owned();
    let options = parse_v4_finalizer_pack_options(&[
        output,
        "--identity-map-max-bytes".to_owned(),
        "160".to_owned(),
        manifest,
    ])
    .unwrap();
    assert_eq!(options.identity_map_max_bytes, 160);
    assert!(parse_v4_finalizer_pack_options(&[
        "/tmp/a".to_owned(),
        "--identity-map-max-bytes".to_owned(),
        "159".to_owned(),
        "/tmp/b".to_owned(),
    ])
    .is_err());
    assert!(run_v4_finalizer_pack(&[]).is_err());
}
#[test]
fn v4_finalizer_pack_manifest_admission_rejects_every_contract_boundary() {
    let directory = tempfile::tempdir().unwrap();
    let (_, _, price, serving) = v4_finalizer_pack_success_fixture(directory.path());
    let manifest_path = v4_finalizer_pack_test_manifest(directory.path(), &price, &serving);
    let base: Value = serde_json::from_reader(File::open(&manifest_path).unwrap()).unwrap();
    let reject = |name: &str, manifest: &Value| {
        let path = directory.path().join(format!("invalid-{name}.json"));
        std::fs::write(&path, serde_json::to_vec(manifest).unwrap()).unwrap();
        assert!(
            load_v4_finalizer_pack_manifest(&path).is_err(),
            "accepted {name}"
        );
    };
    for (name, pointer, replacement) in [
        ("contract", "/contract", json!("other")),
        ("coordinates", "/coordinates_per_pack", json!(0)),
        ("lane-count", "/lanes", json!([])),
        ("blank-lane", "/lanes/0/name", json!("")),
        ("duplicate-lane", "/lanes/1/name", json!("price_dictionary")),
        ("missing-lane", "/lanes/0/name", json!("other")),
        ("object-kinds", "/lanes/0/object_kinds", json!([])),
        ("byte-count", "/lanes/0/byte_count", json!(0)),
        ("sha-length", "/lanes/0/sha256", json!("bad")),
    ] {
        let mut manifest = base.clone();
        *manifest.pointer_mut(pointer).unwrap() = replacement;
        reject(name, &manifest);
    }
    let mut same_source = base.clone();
    same_source["lanes"][1]["path"] = same_source["lanes"][0]["path"].clone();
    same_source["lanes"][1]["byte_count"] = same_source["lanes"][0]["byte_count"].clone();
    reject("same-source", &same_source);

    let missing_manifest = directory.path().join("missing-manifest.json");
    assert!(load_v4_finalizer_pack_manifest(&missing_manifest).is_err());
    let malformed_manifest = directory.path().join("malformed-manifest.json");
    std::fs::write(&malformed_manifest, b"{").unwrap();
    assert!(load_v4_finalizer_pack_manifest(&malformed_manifest).is_err());
    let mut missing_lane_file = base;
    missing_lane_file["lanes"][0]["path"] = json!(directory.path().join("missing.copy"));
    reject("missing-lane-file", &missing_lane_file);
    assert!(decode_v4_finalizer_pack_sha256(&"G".repeat(64), "test").is_err());
    assert!(decode_v4_finalizer_pack_sha256(&"0G".repeat(32), "test").is_err());
}
#[test]
fn v4_finalizer_pack_source_rows_fail_closed_at_each_wire_boundary() {
    let row = V4FinalizerPackTestRow {
        object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
        block_key: 0,
        fragment_no: 0,
        entry_count: 1,
        payload: vec![1],
        block_hash: None,
    };
    let valid = v4_finalizer_pack_test_fields(&row);
    for (name, index, replacement) in [
        ("null-hash", 0, None),
        ("hash-width", 0, Some(vec![0; 31])),
        ("format-width", 1, Some(vec![0])),
        ("format-version", 1, Some(1i16.to_be_bytes().to_vec())),
        ("empty-kind", 2, Some(vec![])),
        ("long-kind", 2, Some(vec![b'x'; 65])),
        ("unknown-kind", 2, Some(b"other".to_vec())),
        ("block-key-width", 3, Some(1i32.to_be_bytes().to_vec())),
        (
            "negative-block-key",
            3,
            Some((-1i64).to_be_bytes().to_vec()),
        ),
        ("fragment-width", 4, Some(vec![0; 3])),
        ("negative-fragment", 4, Some((-1i32).to_be_bytes().to_vec())),
        (
            "negative-entry-count",
            5,
            Some((-1i64).to_be_bytes().to_vec()),
        ),
        ("codec", 6, Some(b"gzip".to_vec())),
        (
            "negative-raw-bytes",
            7,
            Some((-1i64).to_be_bytes().to_vec()),
        ),
        (
            "negative-stored-bytes",
            8,
            Some((-1i64).to_be_bytes().to_vec()),
        ),
        ("payload-length", 9, Some(vec![1, 2])),
    ] {
        let mut fields = valid.clone();
        fields[index] = replacement;
        assert!(
            parse_v4_finalizer_source_row(&fields).is_err(),
            "accepted {name}"
        );
    }
    let mut zlib = valid.clone();
    zlib[6] = Some(b"zlib".to_vec());
    assert_eq!(parse_v4_finalizer_source_row(&zlib).unwrap().codec_index, 1);

    let parsed = parse_v4_finalizer_source_row(&valid).unwrap();
    let reference = V4FinalizerMapReference {
        block_key: parsed.block_key,
        fragment_no: parsed.fragment_no,
        entry_count: parsed.entry_count,
        block_hash: parsed.block_hash,
        raw_byte_count: parsed.raw_byte_count,
    };
    assert!(encode_v4_finalizer_map_pack("", &[reference]).is_err());
    assert!(encode_v4_finalizer_map_pack(parsed.object_kind, &[]).is_err());
    assert!(encode_v4_finalizer_map_pack(&"x".repeat(65), &[reference]).is_err());
    assert!(encode_v4_finalizer_map_pack(
        parsed.object_kind,
        &vec![reference; V4_FINALIZER_PACK_MAX_COORDINATES + 1],
    )
    .is_err());
    let mut count = u64::MAX;
    assert!(add_v4_finalizer_count(&mut count, 1, "test").is_err());
}
#[test]
fn v4_finalizer_pack_internal_overflow_and_state_guards_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("authenticated-source");
    std::fs::write(&source_path, b"x").unwrap();
    let mut authenticated =
        V4FinalizerPackAuthenticatingReader::new(File::open(&source_path).unwrap());
    authenticated.byte_count = u64::MAX;
    assert!(authenticated.read(&mut [0u8; 1]).is_err());
    const INVALID_KINDS: [&str; 1] = ["unsupported"];
    let invalid_root = directory.path().join("invalid-kind");
    std::fs::create_dir(&invalid_root).unwrap();
    let invalid_source = ValidatedV4FinalizerPackLane {
        name: "invalid",
        path: source_path.clone(),
        byte_count: 1,
        sha256: [0; 32],
        row_count: 0,
        stored_payload_bytes: 0,
        object_kinds: &INVALID_KINDS,
    };
    assert!(V4FinalizerPackLaneWriter::new(
        invalid_source,
        2,
        &invalid_root,
        &directory.path().join("invalid-final"),
    )
    .is_err());
    assert!(V4FinalizerPackArtifactWriter::new(
        directory.path(),
        directory.path().join("invalid-artifact"),
    )
    .is_err());
    assert!(V4FinalizerCanonicalSegment::new(directory.path().to_path_buf()).is_err());

    let readonly_artifact = |capacity| V4FinalizerPackArtifactWriter {
        final_path: directory.path().join(format!("readonly-{capacity}")),
        writer: CountingWriter::new(BufWriter::with_capacity(
            capacity,
            File::open(&source_path).unwrap(),
        )),
        row_count: 0,
    };
    assert!(readonly_artifact(0).finish().is_err());
    assert!(readonly_artifact(8192).finish().is_err());

    let price_row = v4_finalizer_pack_test_source_row(0);
    let serving_row = v4_finalizer_pack_test_source_row(1);
    let (mut writer, _) =
        v4_finalizer_pack_test_lane_writer(directory.path(), "state-missing", 0, 2);
    assert!(writer.add_mapping(&serving_row).is_err());
    assert!(writer.flush_kind(1).is_err());

    for target_counter in 0..3 {
        let (mut writer, _) = v4_finalizer_pack_test_lane_writer(
            directory.path(),
            &format!("target-overflow-{target_counter}"),
            0,
            2,
        );
        match target_counter {
            0 => writer.target_blocks.row_count = u64::MAX,
            1 => writer.target_block_count = u64::MAX,
            _ => writer.target_stored_byte_count = u64::MAX,
        }
        assert!(writer.write_target(&price_row).is_err());
    }

    for (name, counter) in [
        ("coordinate-overflow", 0usize),
        ("entry-overflow", 1),
        ("logical-overflow", 2),
        ("stored-overflow", 3),
        ("source-row-overflow", 4),
        ("source-byte-overflow", 5),
    ] {
        let (mut writer, _) = v4_finalizer_pack_test_lane_writer(directory.path(), name, 0, 2);
        match counter {
            0 => writer.coordinate_count = u64::MAX,
            1 => writer.entry_count = u64::MAX,
            2 => writer.logical_byte_count = u64::MAX,
            3 => writer.stored_byte_count = u64::MAX,
            4 => writer.source_row_count = u64::MAX,
            _ => writer.source_stored_payload_bytes = u64::MAX,
        }
        assert!(writer.add_mapping(&price_row).is_err(), "accepted {name}");
    }

    let reference = V4FinalizerMapReference {
        block_key: 0,
        fragment_no: 0,
        entry_count: 1,
        block_hash: price_row.block_hash,
        raw_byte_count: 1,
    };
    let (mut writer, _) = v4_finalizer_pack_test_lane_writer(directory.path(), "pack-number", 0, 2);
    let state = writer.kind_states.get_mut(&0).unwrap();
    state.references.push(reference);
    state.next_pack_no = u32::MAX;
    assert!(writer.flush_kind(0).is_err());

    let (mut writer, _) =
        v4_finalizer_pack_test_lane_writer(directory.path(), "pack-number-i32", 0, 2);
    let state = writer.kind_states.get_mut(&0).unwrap();
    state.references.push(reference);
    state.next_pack_no = i32::MAX as u32 + 1;
    assert!(writer.flush_kind(0).is_err());

    let (mut writer, _) =
        v4_finalizer_pack_test_lane_writer(directory.path(), "map-block-key", 0, 2);
    writer
        .kind_states
        .get_mut(&0)
        .unwrap()
        .references
        .push(reference);
    writer.map_block_count = i64::MAX as u64 + 1;
    assert!(writer.flush_kind(0).is_err());

    for failing_artifact in ["target", "map-block", "map-pack"] {
        let (mut writer, staged) = v4_finalizer_pack_test_lane_writer(
            directory.path(),
            &format!("io-{failing_artifact}"),
            0,
            2,
        );
        if failing_artifact == "target" {
            writer.target_blocks.writer = CountingWriter::new(BufWriter::with_capacity(
                0,
                File::open(&source_path).unwrap(),
            ));
            assert!(writer.write_target(&price_row).is_err());
            continue;
        }
        writer.add_mapping(&price_row).unwrap();
        let failing = CountingWriter::new(BufWriter::with_capacity(
            if failing_artifact == "map-block" {
                0
            } else {
                8192
            },
            File::open(&source_path).unwrap(),
        ));
        if failing_artifact == "map-block" {
            writer.map_blocks.writer = failing;
        } else {
            writer.map_packs.writer = failing;
        }
        assert!(writer.finish(&staged).is_err());
    }

    for aggregate in ["entry-i64", "logical-i64"] {
        let (mut writer, _) = v4_finalizer_pack_test_lane_writer(directory.path(), aggregate, 0, 4);
        let mut overflowing = reference;
        if aggregate == "entry-i64" {
            overflowing.entry_count = i64::MAX;
        } else {
            overflowing.raw_byte_count = i64::MAX;
        }
        writer.kind_states.get_mut(&0).unwrap().references = vec![overflowing; 2];
        assert!(writer.flush_kind(0).is_err());
    }

    let (mut writer, _) =
        v4_finalizer_pack_test_lane_writer(directory.path(), "pack-logical", 0, 4);
    writer.kind_states.get_mut(&0).unwrap().references = vec![
        V4FinalizerMapReference {
            raw_byte_count: i64::MAX,
            ..reference
        };
        3
    ];
    assert!(writer.flush_kind(0).is_err());

    let (writer, staged) = v4_finalizer_pack_test_lane_writer(directory.path(), "empty-kind", 0, 2);
    assert!(writer.finish(&staged).is_err());

    let digest_sets = vec![
        vec![(0, [0; 32])],
        (1..6).map(|index| (index, [index as u8; 32])).collect(),
    ];
    let summaries = vec![
        json!({"coordinate_count": 1, "target_block_count": 1}),
        json!({"coordinate_count": 2, "target_block_count": 1}),
    ];
    assert!(validate_v4_finalizer_pack_aggregates(&digest_sets, &summaries, 3, 2).is_ok());

    let mut invalid_index = digest_sets.clone();
    invalid_index[1][0].0 = 6;
    assert!(validate_v4_finalizer_pack_aggregates(&invalid_index, &summaries, 3, 2).is_err());
    let mut duplicate = digest_sets.clone();
    duplicate[1][0].0 = 0;
    assert!(validate_v4_finalizer_pack_aggregates(&duplicate, &summaries, 3, 2).is_err());
    let mut incomplete = digest_sets.clone();
    incomplete[1].pop();
    assert!(validate_v4_finalizer_pack_aggregates(&incomplete, &summaries, 3, 2).is_err());
    assert!(validate_v4_finalizer_pack_aggregates(&digest_sets, &summaries, 4, 2).is_err());
    assert!(validate_v4_finalizer_pack_aggregates(&digest_sets, &summaries, 3, 3).is_err());
    let overflowing = vec![
        json!({"coordinate_count": u64::MAX, "target_block_count": 0}),
        json!({"coordinate_count": 1, "target_block_count": 0}),
    ];
    assert!(validate_v4_finalizer_pack_aggregates(&digest_sets, &overflowing, 0, 0).is_err());
}
#[test]
fn v4_finalizer_pack_rejects_copy_framing_and_canonical_overflow() {
    let directory = tempfile::tempdir().unwrap();
    for framing in ["header", "trailing", "truncated-header", "truncated-row"] {
        let root = directory.path().join(framing);
        std::fs::create_dir(&root).unwrap();
        let (_, _, mut price, serving) = v4_finalizer_pack_success_fixture(&root);
        let mut bytes = std::fs::read(&price.0).unwrap();
        match framing {
            "header" => bytes[0] ^= 1,
            "trailing" => bytes.push(0),
            "truncated-header" => bytes.truncate(10),
            "truncated-row" => {
                bytes.pop();
            }
            _ => unreachable!(),
        }
        price.1 = bytes.len() as u64;
        price.2 = sha256_hex(&Sha256::digest(&bytes));
        std::fs::write(&price.0, bytes).unwrap();
        let manifest = v4_finalizer_pack_test_manifest(&root, &price, &serving);
        let output = root.join("output");
        assert!(pack_v4_finalizer_copies(&V4FinalizerPackOptions {
            output_directory: output.clone(),
            manifest_path: manifest,
            identity_map_max_bytes: 1024 * 1024,
        })
        .is_err());
        assert!(!output.exists());
    }

    let empty_root = directory.path().join("empty");
    std::fs::create_dir(&empty_root).unwrap();
    let (_, _, _, serving) = v4_finalizer_pack_success_fixture(&empty_root);
    let empty_price = v4_finalizer_pack_test_copy(&empty_root, "empty.copy", &[]);
    let manifest = v4_finalizer_pack_test_manifest(&empty_root, &empty_price, &serving);
    assert!(pack_v4_finalizer_copies(&V4FinalizerPackOptions {
        output_directory: empty_root.join("output"),
        manifest_path: manifest,
        identity_map_max_bytes: 1024 * 1024,
    })
    .is_err());

    for overflow in ["rows", "bytes"] {
        let root = directory.path().join(format!("canonical-{overflow}"));
        std::fs::create_dir(&root).unwrap();
        let mut first = V4FinalizerCanonicalSegment::new(root.join("first")).unwrap();
        let mut second = V4FinalizerCanonicalSegment::new(root.join("second")).unwrap();
        if overflow == "rows" {
            first.row_count = u64::MAX;
            second.row_count = 1;
        } else {
            first.writer.byte_count = u64::MAX;
            second.writer.byte_count = 1;
        }
        assert!(finish_v4_finalizer_canonical_segments(vec![first, second], &root).is_err());
    }

    let canonical_path = directory.path().join("canonical-row");
    let mut segment = V4FinalizerCanonicalSegment::new(canonical_path).unwrap();
    segment.row_count = u64::MAX;
    assert!(segment
        .write_mapping(&v4_finalizer_pack_test_source_row(0))
        .is_err());

    let missing_segment_root = directory.path().join("missing-segment");
    std::fs::create_dir(&missing_segment_root).unwrap();
    let missing_segment_path = missing_segment_root.join("segment");
    let missing_segment = V4FinalizerCanonicalSegment::new(missing_segment_path.clone()).unwrap();
    std::fs::remove_file(missing_segment_path).unwrap();
    assert!(
        finish_v4_finalizer_canonical_segments(vec![missing_segment], &missing_segment_root,)
            .is_err()
    );

    let nonempty_root = directory.path().join("nonempty-canonical");
    std::fs::create_dir(&nonempty_root).unwrap();
    std::fs::write(nonempty_root.join("residue"), b"x").unwrap();
    assert!(finish_v4_finalizer_canonical_segments(Vec::new(), &nonempty_root).is_err());
}
#[test]
fn indexed_range_and_cli_usage_guards_are_explicit() {
    use std::os::unix::ffi::OsStringExt;

    let _guard = scanner_env_lock().lock().unwrap();
    {
        let _unset = TestEnvVar::remove("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS");
        assert_eq!(
            indexed_range_producers_requested().unwrap(),
            DEFAULT_INDEXED_RANGE_PRODUCERS
        );
    }
    {
        let _valid = TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "2");
        assert_eq!(indexed_range_producers_requested().unwrap(), 2);
    }
    {
        let _invalid = TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "0");
        assert!(indexed_range_producers_requested().is_err());
    }
    {
        let _invalid = TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "not-a-number");
        assert!(indexed_range_producers_requested().is_err());
    }

    assert!(strict_v3_price_copy_stdio_usage().contains("<24|32>"));
    assert_eq!(
        shared_graph_summary_path(Path::new("/tmp/shared-graph"), "fixture").unwrap(),
        "/tmp/shared-graph"
    );
    let non_utf8 = PathBuf::from(std::ffi::OsString::from_vec(vec![0xff]));
    assert!(shared_graph_summary_path(&non_utf8, "fixture").is_err());

    let progress = IndexedProducerProgress::default();
    progress.record_object(3, 128);
    emit_indexed_range_progress(
        Path::new("/tmp/indexed-progress"),
        2,
        &progress,
        Instant::now(),
        true,
    );
    assert!(run_shared_graph_converter(&[]).is_err());
    assert!(run_v3_finalizer(&[]).is_err());
}
#[test]
fn compact_scan_entrypoints_reject_startup_boundaries_before_work() {
    let _guard = scanner_env_lock().lock().unwrap();
    let _factor_mode = TestEnvVar::remove(PROVIDER_GRAPH_V4_ENV);
    let directory = tempfile::tempdir().unwrap();
    let _strict_env = strict_scan_env(directory.path());
    let missing = directory.path().join("missing.json.gz");
    let scan_error = scan_compact(&missing).unwrap_err();
    assert_eq!(scan_error.kind(), io::ErrorKind::NotFound);

    let indexed_error = build_indexed_top_level_reorder(
        &missing,
        &RapidgzipConfig::default(),
        Arc::new(AtomicU64::new(0)),
        1,
        1,
    )
    .err()
    .expect("missing scanner input rejected");
    assert_eq!(indexed_error.kind(), io::ErrorKind::Unsupported);
    assert_eq!(
        indexed_error.to_string(),
        "indexed scans require rapidgzip and gzip input"
    );
}
#[test]
fn strict_compact_scan_rejects_disabled_parallel_routes() {
    let _guard = scanner_env_lock().lock().unwrap();
    let _factor_mode = TestEnvVar::remove(PROVIDER_GRAPH_V4_ENV);
    let temporary = tempfile::tempdir().unwrap();
    let input_path = temporary.path().join("input.json");
    std::fs::write(
        &input_path,
        br#"{"provider_references":[],"in_network":[]}"#,
    )
    .unwrap();

    for (name, parse_in_workers, top_level_scan, provider_ref_workers, expected) in [
        (
            "inline-rate-parsing",
            "false",
            "true",
            "true",
            "strict V3 source attestation requires worker-side raw rate parsing",
        ),
        (
            "top-level-scan-disabled",
            "true",
            "false",
            "true",
            "strict V3 source attestation requires the parallel top-level byte scanner; provider_reference_order=before_in_network",
        ),
        (
            "provider-ref-workers-disabled",
            "true",
            "true",
            "false",
            "strict V3 source attestation requires the parallel top-level byte scanner; provider_reference_order=before_in_network",
        ),
    ] {
        let serving_run_directory = temporary.path().join(name);
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _route_env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", parse_in_workers),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", top_level_scan),
            TestEnvVar::set(
                "HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS",
                provider_ref_workers,
            ),
        ];

        let error = scan_compact_struson(&input_path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidInput, "{name}");
        assert_eq!(error.to_string(), expected, "{name}");
    }

    let reversed_input_path = temporary.path().join("reversed-input.json.gz");
    let mut reversed_encoder = flate2::write::GzEncoder::new(
        File::create(&reversed_input_path).unwrap(),
        Compression::default(),
    );
    reversed_encoder
        .write_all(br#"{"in_network":[],"provider_references":[]}"#)
        .unwrap();
    reversed_encoder.finish().unwrap();
    let reversed_serving_run_directory = temporary.path().join("reversed-order-disabled");
    let _reversed_strict_env = strict_scan_env(&reversed_serving_run_directory);
    let _reversed_route_env = [
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
        TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
    ];
    let error = scan_compact_struson(&reversed_input_path).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "strict V3 source attestation requires the parallel top-level byte scanner; provider_reference_order=after_in_network"
    );

    let _parse_in_workers = TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true");
    let error = scan_compact_struson_inner(
        &input_path,
        CopyPathConfig::default(),
        Arc::new(SourceWitnessCollector::new(&"00".repeat(32)).unwrap()),
        None,
    )
    .expect_err("later source identity drift must fail closed");
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "strict V3 source attestation requires configured file outputs"
    );
}
#[test]
fn pre_cancelled_indexed_producer_never_opens_its_source() {
    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.json.gz");
    let (worker_tx, _worker_rx) = unbounded();
    let (_recycle_tx, recycle_rx) = unbounded();
    let cancelled = Arc::new(AtomicBool::new(true));
    let error = produce_indexed_in_network_range(IndexedRangeProducerConfig {
        path: missing.clone(),
        rapidgzip_config: RapidgzipConfig::default(),
        index_path: missing.with_extension("index"),
        range_id: 0,
        range: IndexedInNetworkRange {
            offset: 0,
            length: 1,
            object_count: 1,
        },
        object_ordinal_base: 0,
        tx: worker_tx,
        cancelled,
        queue_bytes: Arc::new(QueueByteMetrics::default()),
        recycle_rx,
        progress: Arc::new(IndexedProducerProgress::default()),
        enqueue_options: InNetworkEnqueueOptions {
            chunk_size: 1,
            raw_chunk_byte_limit: 1,
            parse_in_workers: false,
            object_ordinal: 0,
        },
    })
    .err()
    .expect("pre-cancelled indexed producer rejected");
    assert_eq!(error.kind(), io::ErrorKind::Interrupted);
}
#[test]
fn compact_worker_failure_precedes_peer_cancellation() {
    for peer_kind in [io::ErrorKind::Interrupted, io::ErrorKind::BrokenPipe] {
        let error = compact_pipeline_error(
            Some(io::Error::other("primary worker failure")),
            Some(io::Error::new(peer_kind, "peer rapidgzip producer stopped")),
        )
        .unwrap();
        assert_eq!(error.to_string(), "primary worker failure");
    }
}
#[test]
fn raw_chunk_blocked_send_count_saturates() {
    let mut chunk_stats = RawChunkStats::default();
    chunk_stats.record_queue_blocked();
    assert_eq!(chunk_stats.queue_blocked_sends, 1);
    chunk_stats.queue_blocked_sends = u64::MAX;
    chunk_stats.record_queue_blocked();
    assert_eq!(chunk_stats.queue_blocked_sends, u64::MAX);
}
#[test]
fn recycled_rate_vector_retains_requested_capacity() {
    let mut rate_values = Vec::<RateLite>::with_capacity(4);
    let taken_values = take_vec_replacing_with_capacity(&mut rate_values, 2);
    assert!(taken_values.capacity() >= 4);
    assert!(rate_values.capacity() >= 2);
}
#[test]
fn compact_producer_failure_precedes_secondary_worker_failure() {
    let error = compact_pipeline_error(
        Some(io::Error::new(
            io::ErrorKind::BrokenPipe,
            "worker observed closed producer channel",
        )),
        Some(io::Error::new(
            io::ErrorKind::InvalidData,
            "primary rapidgzip producer failure",
        )),
    )
    .unwrap();
    assert_eq!(error.to_string(), "primary rapidgzip producer failure");
}
#[test]
fn wrapped_indexed_range_reader_validates_exact_range_before_suffix() {
    let mut reader = WrappedIndexedRangeReader::new(
        Box::new(Cursor::new(b"[]".to_vec())),
        2,
        br#"{"provider_references":"#,
        br#","in_network":[]}"#,
    );
    let mut output = String::new();

    reader.read_to_string(&mut output).unwrap();

    assert_eq!(output, r#"{"provider_references":[],"in_network":[]}"#);

    let nested = br#"{"items":[1,{"value":"two"}]}"#;
    let wrapped_nested = WrappedIndexedRangeReader::new(
        Box::new(Cursor::new(nested.to_vec())),
        nested.len() as u64,
        b"",
        b"",
    );
    let mut byte_reader = BufferedJsonByteReader::new(wrapped_nested);
    let mut captured = Vec::new();
    byte_reader
        .capture_value_bytes_into(&mut captured)
        .expect("capture nested indexed range");
    assert_eq!(captured, nested);

    let string_body = br#"indexed \"range"#;
    let wrapped_string = WrappedIndexedRangeReader::new(
        Box::new(Cursor::new(string_body.to_vec())),
        string_body.len() as u64,
        b"\"",
        b"\"",
    );
    let mut byte_reader = BufferedJsonByteReader::new(wrapped_string);
    captured.clear();
    byte_reader
        .capture_value_bytes_into(&mut captured)
        .expect("capture string across indexed range stages");
    assert_eq!(captured, br#""indexed \"range""#);
}
#[test]
fn wrapped_indexed_range_reader_rejects_short_or_extra_ranges() {
    let mut short_reader =
        WrappedIndexedRangeReader::new(Box::new(Cursor::new(b"[".to_vec())), 2, b"", b"");
    let short_error = io::read_to_string(&mut short_reader).unwrap_err();
    assert_eq!(short_error.kind(), io::ErrorKind::UnexpectedEof);

    let mut extra_reader =
        WrappedIndexedRangeReader::new(Box::new(Cursor::new(b"[]x".to_vec())), 2, b"", b"");
    let extra_error = io::read_to_string(&mut extra_reader).unwrap_err();
    assert_eq!(extra_error.kind(), io::ErrorKind::InvalidData);
}
#[test]
fn wrapped_indexed_range_reader_surfaces_late_range_failure() {
    let mut reader = WrappedIndexedRangeReader::new(
        Box::new(LateErrorReader {
            bytes: Cursor::new(b"[]".to_vec()),
            emitted_error: false,
        }),
        2,
        b"",
        b"",
    );

    let error = io::read_to_string(&mut reader).unwrap_err();

    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(error.to_string().contains("late indexed range failure"));
}
#[test]
fn indexed_ranges_are_bounded_and_preserve_object_coverage() {
    let mut ranges = Vec::new();
    for object_index in 0..24u64 {
        retain_coalesced_indexed_range(
            &mut ranges,
            TopLevelArrayRange {
                offset: 1 + object_index * 11,
                length: 10,
            },
            4,
        );
        assert!(ranges.len() <= 4);
    }

    validate_indexed_in_network_ranges(
        &ranges,
        TopLevelArrayRange {
            offset: 0,
            length: 266,
        },
        24,
    )
    .unwrap();
    assert_eq!(ranges.len(), 4);
    assert_eq!(
        ranges.iter().map(|range| range.object_count).sum::<u64>(),
        24
    );
    assert!(ranges.windows(2).all(|pair| pair[0].end() < pair[1].offset));
}
#[test]
fn indexed_decoder_budget_is_split_deterministically() {
    assert_eq!(
        indexed_range_decoder_thread_allocation(4, 4),
        vec![1, 1, 1, 1]
    );
    assert_eq!(
        indexed_range_decoder_thread_allocation(6, 4),
        vec![2, 2, 1, 1]
    );
    assert_eq!(indexed_range_decoder_thread_allocation(4, 1), vec![4]);
    assert!(indexed_range_decoder_thread_allocation(4, 0).is_empty());
}
#[test]
fn temporary_rapidgzip_index_is_private() {
    use std::os::unix::fs::PermissionsExt;

    let index = TemporaryRapidgzipIndex::new().unwrap();
    let directory_mode = index
        ._directory
        .path()
        .metadata()
        .unwrap()
        .permissions()
        .mode()
        & 0o777;
    assert_eq!(directory_mode, 0o700);

    File::create(&index.path).unwrap();
    index.harden_file_permissions().unwrap();
    let index_mode = index.path.metadata().unwrap().permissions().mode() & 0o777;
    assert_eq!(index_mode, 0o600);
}
#[test]
fn indexed_range_metrics_and_wrapped_parser_preserve_exact_values() {
    let index = TemporaryRapidgzipIndex::new().unwrap();
    assert_eq!(index.byte_len(), 0);
    std::fs::write(&index.path, b"index").unwrap();
    assert_eq!(index.byte_len(), 5);

    let range = IndexedInNetworkRange {
        offset: 11,
        length: 22,
        object_count: 3,
    };
    assert_eq!(
        range.payload(7, 2),
        json!({
            "range_id": 7,
            "offset": 11,
            "length": 22,
            "object_count": 3,
            "decoder_threads": 2,
        })
    );

    let mut raw_chunk_stats = RawChunkStats::default();
    raw_chunk_stats.record(2, 64);
    raw_chunk_stats.record_queue_depth(3);
    raw_chunk_stats.record_queue_blocked();
    let output = IndexedRangeProducerOutput {
        range_id: 7,
        range,
        decoder_threads: 2,
        elapsed_seconds: 0.25,
        rate_count: 2,
        blocked_micros: 1_500_000,
        compressed_bytes: 32,
        raw_chunk_stats,
    };
    assert_eq!(
        output.payload(),
        json!({
            "range_id": 7,
            "offset": 11,
            "length": 22,
            "object_count": 3,
            "decoder_threads": 2,
            "elapsed_seconds": 0.25,
            "rate_count": 2,
            "blocked_seconds": 1.5,
            "raw_chunk_count": 1,
            "raw_chunk_total_bytes": 64,
            "raw_chunk_max_bytes": 64,
            "raw_chunk_max_rates": 2,
            "queue_high_water": 3,
            "queue_blocked_sends": 1,
            "compressed_bytes": 32,
        })
    );

    let input = br#"[{"name":"one"},{"name":"two"}]"#;
    let wrapped = WrappedIndexedRangeReader::new(
        Box::new(Cursor::new(input.to_vec())),
        input.len() as u64,
        b"",
        b"",
    );
    let mut reader = BufferedJsonByteReader::new(wrapped);
    reader.expect_byte(b'[').unwrap();
    let mut first = true;
    let mut captured = Vec::new();

    assert!(reader
        .capture_next_array_object_bytes_append(&mut captured, &mut first)
        .unwrap());
    assert_eq!(captured, br#"{"name":"one"}"#);

    captured.clear();
    assert!(reader
        .capture_next_array_object_bytes_append(&mut captured, &mut first)
        .unwrap());
    assert_eq!(captured, br#"{"name":"two"}"#);

    captured.clear();
    assert!(!reader
        .capture_next_array_object_bytes_append(&mut captured, &mut first)
        .unwrap());

    let invalid_money = b"1e999999999";
    let wrapped = WrappedIndexedRangeReader::new(
        Box::new(Cursor::new(invalid_money.to_vec())),
        invalid_money.len() as u64,
        b"",
        b"",
    );
    let mut reader = JsonStreamReader::new(wrapped);
    assert!(strict_money_number_from_reader(&mut reader).is_err());
}
#[test]
fn serving_binary_compression_requires_minimum_savings() {
    let _lock = scanner_env_lock().lock().unwrap();
    let _minimum_savings = TestEnvVar::set(
        PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV,
        "2",
    );
    let minimum_savings_pct = ServingBinaryCompressionOptions::configured().minimum_savings_pct;

    assert!(!serving_binary_compression_is_worthwhile(
        1_000_000,
        993_200,
        minimum_savings_pct,
    ));
    assert!(serving_binary_compression_is_worthwhile(
        1_000_000,
        970_000,
        minimum_savings_pct,
    ));
}
#[test]
fn price_lite_set_caches_canonical_v3_atom_and_set_ids() {
    let first = test_price_lite("100.00");
    let mut second = test_price_lite("250.50");
    second.service_code = vec!["22".to_string(), "11".to_string()];
    second.billing_code_modifier = vec!["GT".to_string()];
    let prices = vec![first.clone(), second.clone(), first.clone()];

    let price_set = price_lite_set(&prices).unwrap();
    let expected_atom_ids = prices
        .iter()
        .map(|price| {
            GlobalId128::from_price_atom_parts(PriceAtomParts {
                negotiated_type: price.negotiated_type.as_deref(),
                negotiated_rate: Some(&price.negotiated_rate),
                expiration_date: price.expiration_date.as_deref(),
                service_code: &price.service_code,
                billing_class: price.billing_class.as_deref(),
                setting: price.setting.as_deref(),
                billing_code_modifier: &price.billing_code_modifier,
                additional_information: price.additional_information.as_deref(),
            })
        })
        .collect::<Vec<_>>();
    let mut expected_sorted_atom_ids = expected_atom_ids.clone();
    expected_sorted_atom_ids.sort_unstable();

    assert_eq!(
        price_set
            .atoms
            .iter()
            .map(price_atom_global_id)
            .collect::<Vec<_>>(),
        expected_atom_ids
    );
    assert_eq!(price_set.atom_ids, expected_sorted_atom_ids);
    assert_eq!(
        price_set_global_id(&price_set),
        price_set_global_id_from_atom_ids(&expected_atom_ids)
    );
}
#[test]
fn price_set_identity_is_order_independent_and_multiplicity_sensitive() {
    let first = test_price_lite("100.00");
    let second = test_price_lite("250.50");
    let ordered = price_lite_set(&[first.clone(), second.clone()]).unwrap();
    let reordered = price_lite_set(&[second, first.clone()]).unwrap();
    let repeated = price_lite_set(&[first.clone(), first]).unwrap();

    assert_eq!(ordered.global_id, reordered.global_id);
    assert_eq!(ordered.atom_ids, reordered.atom_ids);
    assert_ne!(ordered.global_id, repeated.global_id);
    assert_eq!(repeated.atom_ids[0], repeated.atom_ids[1]);
}
#[test]
fn provider_set_scope_cache_preserves_canonical_identity_and_checks_collisions() {
    let context = test_compact_context();
    let rate = RateLite {
        provider_refs: vec!["7".into()],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: vec!["A network".to_string(), "Shared".to_string()],
        prices: vec![test_price_lite("100.00")],
        prepared_price_set: None,
    };
    let provider_entry = ProviderEntryView::Owned(ProviderEntry {
        entry_hash: 17,
        provider_group_scope_hash: 0,
        provider_count: 1,
        provider_group_hashes: vec![101],
        npi: vec![1234567890],
        quarantined_npi: Vec::new(),
        quarantined_npi_text: Vec::new(),
        network_names: vec!["Shared".to_string(), "Z network".to_string()],
        source_locators: Vec::new(),
    });
    let expected_network_names =
        rate_network_names(&rate, provider_entry.network_names(), &context);
    let expected_hash = provider_set_scope_hash(
        provider_entry.provider_group_hashes(),
        &expected_network_names,
    );
    let mut cache = ProviderSetScopeCache::default();

    for _ in 0..2 {
        let cached = cache.resolve(&provider_entry, &rate, &context);
        assert_eq!(cached.network_names, expected_network_names);
        assert_eq!(cached.provider_set_hash, expected_hash);
        assert_eq!(
            cached.provider_set_global_id,
            provider_set_global_id_from_group_hashes_and_network_names(
                provider_entry.provider_group_hashes(),
                &expected_network_names,
            )
        );
    }
    assert_eq!(cache.buckets[&17].len(), 1);

    let colliding_entry = ProviderEntryView::Owned(ProviderEntry {
        entry_hash: 17,
        provider_group_scope_hash: 0,
        provider_count: 1,
        provider_group_hashes: vec![202],
        npi: vec![1234567891],
        quarantined_npi: Vec::new(),
        quarantined_npi_text: Vec::new(),
        network_names: Vec::new(),
        source_locators: Vec::new(),
    });
    let colliding_scope = cache.resolve(&colliding_entry, &rate, &context);
    assert_ne!(colliding_scope.provider_set_hash, expected_hash);
    assert_eq!(cache.buckets[&17].len(), 2);
}
#[test]
fn worker_dedupe_cache_preserves_global_counts_across_hits_and_resets() {
    let dedupe = SharedDedupe::new(1);
    let mut cache = WorkerDedupeCache::new(2);
    let price_a = GlobalId128([1; 16]);
    let price_b = GlobalId128([2; 16]);
    let price_c = GlobalId128([3; 16]);
    let provider_a = GlobalId128([4; 16]);
    let mut provider_b_bytes = [4; 16];
    provider_b_bytes[15] = 5;
    let provider_b = GlobalId128(provider_b_bytes);
    let provider_c = GlobalId128([6; 16]);

    assert!(cache.insert_price_set(&dedupe, price_a));
    assert!(!cache.insert_price_set(&dedupe, price_a));

    assert!(cache.insert_price_atom(&dedupe, price_a));
    assert!(!cache.insert_price_atom(&dedupe, price_a));
    assert!(cache.insert_price_atom(&dedupe, price_b));
    assert!(cache.insert_price_atom(&dedupe, price_c));
    assert!(!cache.insert_price_atom(&dedupe, price_a));
    assert!(cache.insert_price_set(&dedupe, price_b));
    assert!(cache.insert_price_set(&dedupe, price_c));
    assert!(!cache.insert_price_set(&dedupe, price_a));

    assert!(cache.insert_provider_set(&dedupe, provider_a));
    assert!(!cache.insert_provider_set(&dedupe, provider_a));
    assert!(cache.insert_provider_set(&dedupe, provider_b));
    assert!(cache.insert_provider_set(&dedupe, provider_c));
    assert!(!cache.insert_provider_set(&dedupe, provider_a));

    cache.flush_duplicate_counts(&dedupe);

    let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(summary["price_set_attempted"], 5);
    assert_eq!(summary["price_set_unique"], 3);
    assert_eq!(summary["price_set_duplicate"], 2);
    assert_eq!(summary["price_atom_attempted"], 5);
    assert_eq!(summary["price_atom_unique"], 3);
    assert_eq!(summary["price_atom_duplicate"], 2);
    assert_eq!(summary["provider_set_attempted"], 5);
    assert_eq!(summary["provider_set_unique"], 3);
    assert_eq!(summary["provider_set_duplicate"], 2);
    assert_eq!(cache.price_set_hits, 1);
    assert_eq!(cache.price_atom_hits, 1);
    assert_eq!(cache.provider_set_hits, 1);
    assert_eq!(cache.price_set_resets, 1);
    assert_eq!(cache.price_atom_resets, 1);
    assert_eq!(cache.provider_set_resets, 1);
}
#[test]
fn v3_emission_captures_every_atomic_price_provider_occurrence() {
    let directory = tempfile::tempdir().unwrap();
    let raw_provider_one = br#"{"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}]}"#;
    let raw_provider_two = br#"{"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567891]}]}"#;
    let raw_rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100,"service_code":["11"]},{"negotiated_type":"negotiated","negotiated_rate":200,"service_code":["22"]}]}"#;
    let source_witness = Arc::new(SourceWitnessCollector::new(&"ab".repeat(32)).unwrap());
    source_witness.configure_provider_spools(1).unwrap();
    source_witness.configure_rate_spools(1).unwrap();
    let source_locator_one = source_witness
        .store_provider_source(0, raw_provider_one)
        .unwrap();
    let source_locator_two = source_witness
        .store_provider_source(0, raw_provider_two)
        .unwrap();
    source_witness.seal_provider_sources().unwrap();

    let provider_value_one: Value = serde_json::from_slice(raw_provider_one).unwrap();
    let provider_value_two: Value = serde_json::from_slice(raw_provider_two).unwrap();
    let mut provider_entry_one = build_provider_entry(&provider_value_one).unwrap();
    provider_entry_one.source_locators.push(source_locator_one);
    let mut provider_entry_two = build_provider_entry(&provider_value_two).unwrap();
    provider_entry_two.source_locators.push(source_locator_two);
    let mut provider_map = HashMap::new();
    insert_provider_definition(
        &mut provider_map,
        ProviderRefKey::from("7"),
        provider_entry_one,
    )
    .unwrap();
    insert_provider_definition(
        &mut provider_map,
        ProviderRefKey::from("7"),
        provider_entry_two,
    )
    .unwrap();
    let rate = read_rate_lite_bytes(raw_rate).unwrap().unwrap();
    let procedure = json!({
        "billing_code_type": "CPT",
        "billing_code": "99213",
        "negotiation_arrangement": "ffs"
    });
    let context = CompactContext {
        snapshot_id: "snapshot-test".to_string(),
        plan_id: "plan-test".to_string(),
        plan_month_id: "2026-07".to_string(),
        source_trace_set_hash: "trace-test".to_string(),
        confidence_code: "test".to_string(),
        source_witness: Arc::clone(&source_witness),
        invalid_price_exclusion: None,
    };
    let paths = CopyPathConfig::default();
    let mut writer = io::sink();
    let mut compact_copy_writer = None;
    let mut manifest_serving_copy_writer = Some(
        V3ServingRunSink::new(
            directory.path().to_str().unwrap(),
            "witness-test",
            [0; COVERAGE_SCOPE_ID_BYTES],
        )
        .unwrap(),
    );
    let mut dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
    let dedupe = SharedDedupe::new(1);
    let mut worker_dedupe_cache = WorkerDedupeCache::new(16);
    let mut provider_set_scope_cache = ProviderSetScopeCache::default();
    let mut manifest_global_id_cache = ManifestGlobalIdCache::default();
    let rates = [rate];
    let source_inputs = [SourceRateWitnessInput {
        coordinate: SourceWitnessCoordinate::new(9, 17),
        raw_rate,
    }];
    let mut state = SharedCompactState {
        writer: &mut writer,
        compact_copy_writer: &mut compact_copy_writer,
        manifest_serving_copy_writer: &mut manifest_serving_copy_writer,
        dictionary_copy_sinks: &mut dictionary_copy_sinks,
        manifest_sidecars: None,
        record_price_forward_sidecar: false,
        suppress_legacy_row_output: true,
        provider_map: &provider_map,
        dedupe: &dedupe,
        worker_dedupe_cache: &mut worker_dedupe_cache,
        provider_set_scope_cache: &mut provider_set_scope_cache,
        manifest_global_id_cache: &mut manifest_global_id_cache,
        context: &context,
    };

    process_compact_rate_lites_worker_with_source(&mut state, &rates, &procedure, &source_inputs)
        .unwrap();

    let invalid_inline_rate = RateLite {
        provider_refs: vec![ProviderRefKey::from("missing")],
        provider_groups: vec![json!({"npi": ["x".repeat(129)]})],
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: Vec::new(),
        prepared_price_set: None,
    };
    assert!(process_compact_rate_lites_worker(
        &mut state,
        std::slice::from_ref(&invalid_inline_rate),
        &json!({}),
    )
    .is_err());
    let invalid_source_inputs = [SourceRateWitnessInput {
        coordinate: SourceWitnessCoordinate::new(19, 23),
        raw_rate: br#"{}"#,
    }];
    assert!(process_compact_rate_lites_worker_with_source(
        &mut state,
        &[invalid_inline_rate],
        &procedure,
        &invalid_source_inputs,
    )
    .is_err());

    manifest_serving_copy_writer
        .take()
        .unwrap()
        .finish_silent()
        .unwrap();
    dictionary_copy_sinks.finish_silent().unwrap();
    let summary = source_witness.write_bundle(directory.path()).unwrap();
    assert_eq!(summary["queryable_occurrence_population_count"], 4);
    assert_eq!(summary["occurrence_witness_count"], 4);

    let bundle = std::fs::read(summary["path"].as_str().unwrap()).unwrap();
    let mut coordinates = source_witness_record_metadata(&bundle)
        .into_iter()
        .filter(|metadata| metadata["kind"] == "rate_occurrence")
        .map(|metadata| {
            (
                metadata["coordinate"]["price_ordinal"].as_u64().unwrap(),
                metadata["coordinate"]["provider_ordinal"].as_u64().unwrap(),
                metadata["provider_evidence"]["npi_ordinal"]
                    .as_u64()
                    .unwrap(),
            )
        })
        .collect::<Vec<_>>();
    coordinates.sort_unstable();
    assert_eq!(
        coordinates,
        vec![(0, 0, 0), (0, 1, 0), (1, 0, 0), (1, 1, 0)]
    );
}
#[test]
fn top_level_range_scan_rejects_invalid_json_outside_replayed_arrays() {
    let payload = br#"{
        "in_network":[{"billing_code":"10000","negotiated_rates":[]}],
        "invalid":not_json,
        "provider_references":[{"provider_group_id":1,"provider_groups":[]}]
    }"#;

    let error = scan_compact_top_level_array_ranges(Box::new(Cursor::new(payload.to_vec())), 2)
        .unwrap_err();

    assert!(error.to_string().contains("JSON"), "{error}");
}
#[test]
fn top_level_range_scan_preserves_bom_and_multibyte_byte_offsets() {
    struct OneByteReader(Cursor<Vec<u8>>);

    impl Read for OneByteReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            if buffer.is_empty() {
                return Ok(0);
            }
            let mut byte = [0u8; 1];
            let read = self.0.read(&mut byte)?;
            if read > 0 {
                buffer[0] = byte[0];
            }
            Ok(read)
        }
    }

    let mut payload = b"\xef\xbb\xbf".to_vec();
    payload.extend_from_slice(
        "{\"label\":\"é\",\"in_network\":[{\"billing_code\":\"10000\",\"negotiated_rates\":[]}],\"provider_references\":[{\"provider_group_id\":1,\"provider_groups\":[]}]}"
            .as_bytes(),
    );

    let scan = scan_compact_top_level_array_ranges(
        Box::new(OneByteReader(Cursor::new(payload.clone()))),
        2,
    )
    .unwrap()
    .unwrap();
    let provider_range = &payload[scan.provider_references.offset as usize
        ..scan
            .provider_references
            .offset
            .saturating_add(scan.provider_references.length) as usize];
    let in_network_range = &payload[scan.in_network.offset as usize
        ..scan
            .in_network
            .offset
            .saturating_add(scan.in_network.length) as usize];

    assert!(serde_json::from_slice::<Value>(provider_range).is_ok());
    assert!(serde_json::from_slice::<Value>(in_network_range).is_ok());
    assert_eq!(scan.in_network_object_count, 1);
}
#[test]
fn rate_network_names_are_only_exact_rate_and_provider_reference_values() {
    let context = test_compact_context();
    let rate = RateLite {
        provider_refs: Vec::new(),
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: vec![" Rate Network ".to_string(), "Shared".to_string()],
        prices: Vec::new(),
        prepared_price_set: None,
    };
    assert_eq!(
        rate_network_names(
            &rate,
            &["Provider Network".to_string(), "Shared".to_string()],
            &context,
        ),
        vec![
            "Provider Network".to_string(),
            "Rate Network".to_string(),
            "Shared".to_string(),
        ]
    );
    assert!(rate_network_names(
        &RateLite {
            provider_refs: Vec::new(),
            provider_groups: Vec::new(),
            provider_groups_raw: None,
            network_names: Vec::new(),
            prices: Vec::new(),
            prepared_price_set: None,
        },
        &[],
        &context,
    )
    .is_empty());
}
#[test]
fn strict_v3_config_rejects_negotiated_rate_grouping() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory =
        std::env::temp_dir().join(format!("ptg2-v3-grouping-config-{}", std::process::id()));
    let _arch = TestEnvVar::set("HLTHPRT_PTG2_SNAPSHOT_ARCH", REQUIRED_SNAPSHOT_ARCH);
    let _directory = TestEnvVar::set(
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR",
        directory.to_str().unwrap(),
    );
    let _scope = TestEnvVar::set(
        V3_COVERAGE_SCOPE_ID_ENV,
        &"11".repeat(COVERAGE_SCOPE_ID_BYTES),
    );
    let _grouping = TestEnvVar::set(GROUP_NEGOTIATED_RATE_CHUNKS_ENV, "true");

    let error = CopyPathConfig::from_env().err().unwrap();

    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(error
        .to_string()
        .contains("must be false for strict V3 exact source multiplicity"));
}
#[test]
fn strict_v3_config_requires_valid_coverage_scope_env() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory =
        std::env::temp_dir().join(format!("ptg2-v3-scope-config-{}", std::process::id()));
    let _arch = TestEnvVar::set("HLTHPRT_PTG2_SNAPSHOT_ARCH", REQUIRED_SNAPSHOT_ARCH);
    let _directory = TestEnvVar::set(
        "HLTHPRT_PTG2_V3_SERVING_RUN_DIR",
        directory.to_str().unwrap(),
    );
    let _grouping = TestEnvVar::set(GROUP_NEGOTIATED_RATE_CHUNKS_ENV, "false");
    let valid = "ab".repeat(COVERAGE_SCOPE_ID_BYTES);
    let scope = TestEnvVar::set(V3_COVERAGE_SCOPE_ID_ENV, &valid);
    assert_eq!(
        CopyPathConfig::from_env().unwrap().v3_coverage_scope_id,
        Some([0xab; COVERAGE_SCOPE_ID_BYTES])
    );
    drop(scope);

    for invalid in [
        "ab".repeat(COVERAGE_SCOPE_ID_BYTES - 1),
        "AB".repeat(COVERAGE_SCOPE_ID_BYTES),
        "gg".repeat(COVERAGE_SCOPE_ID_BYTES),
    ] {
        let scope = TestEnvVar::set(V3_COVERAGE_SCOPE_ID_ENV, &invalid);
        let error = CopyPathConfig::from_env().err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        drop(scope);
    }
}
#[test]
fn strict_v3_config_keeps_provider_set_dictionary_path() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory = std::env::temp_dir().join(format!(
        "ptg2-v3-provider-set-config-{}",
        std::process::id()
    ));
    let metadata_path = directory.join("provider-set-metadata.copy");
    let _strict_env = strict_scan_env(&directory);
    let _metadata = TestEnvVar::set(
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_DICTIONARY_COPY_PATH",
        metadata_path.to_str().unwrap(),
    );

    assert_eq!(
        CopyPathConfig::from_env()
            .unwrap()
            .manifest_provider_set_dictionary,
        Some(metadata_path.display().to_string())
    );
}
#[test]
fn strict_v4_factor_config_uses_main_flag_and_requires_paired_paths() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory =
        std::env::temp_dir().join(format!("ptg2-v4-factor-config-{}", std::process::id()));
    let _strict_env = strict_scan_env(&directory);
    let _main_v4 = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "true");
    let _stale_factor_toggle = TestEnvVar::set("HLTHPRT_PTG2_PROVIDER_GRAPH_V4_FACTORS", "false");
    let _set_path_absent =
        TestEnvVar::remove("HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH");
    let _component_path_absent =
        TestEnvVar::remove("HLTHPRT_PTG2_MANIFEST_PROVIDER_COMPONENT_GROUP_SIDECAR_PATH");
    let _tax_identity_v2_path_absent =
        TestEnvVar::remove(PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH_ENV);

    let missing_paths_error = CopyPathConfig::from_env().err().unwrap();
    assert!(missing_paths_error
        .to_string()
        .contains("requires both V4 provider factor sidecar paths"));

    let set_path = directory.join("set-component.ptg2sc");
    let _set_path = TestEnvVar::set(
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH",
        set_path.to_str().unwrap(),
    );
    let missing_component_error = CopyPathConfig::from_env().err().unwrap();
    assert!(missing_component_error
        .to_string()
        .contains("outputs require both set-to-component and component-to-group"));

    let component_path = directory.join("component-group.ptg2sc");
    let _component_path = TestEnvVar::set(
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_COMPONENT_GROUP_SIDECAR_PATH",
        component_path.to_str().unwrap(),
    );
    let tax_identity_path = directory.join("provider-group-tax-identity.ptg2tax");
    let _tax_identity_path = TestEnvVar::set(
        "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_SIDECAR_PATH",
        tax_identity_path.to_str().unwrap(),
    );
    let tax_identity_v2_path = directory.join("provider-group-tax-identity-v2.ptg2tax");
    let _tax_identity_v2_path = TestEnvVar::set(
        PROVIDER_GROUP_TAX_IDENTITY_V2_SIDECAR_PATH_ENV,
        tax_identity_v2_path.to_str().unwrap(),
    );
    let config = CopyPathConfig::from_env().unwrap();
    assert!(configured_v4_factor_mode(&config).unwrap());
    assert_eq!(
        config.manifest_provider_group_tax_identity_v2_sidecar,
        Some(tax_identity_v2_path.display().to_string())
    );
}
#[test]
fn v4_tax_secret_failure_precedes_sidecar_output_and_hides_its_path() {
    let _lock = scanner_env_lock().lock().unwrap();
    let directory = std::env::temp_dir().join(format!("ptg2-v4-tax-secret-{}", std::process::id()));
    let output_path = directory.join("tax-identities.ptg2tax");
    let secret_path = directory.join("secret.bin");
    let _ = std::fs::create_dir_all(&directory);
    let _policy = TestEnvVar::set(
        "HLTHPRT_PTG2_TIN_TOKEN_POLICY_ID",
        "ptg-tin-hmac-sha256-v1:test-1",
    );
    let _missing_secret = TestEnvVar::remove("HLTHPRT_PTG2_TIN_TOKEN_SECRET_FILE");

    let missing = configured_shared_dedupe(2, false, true, false)
        .err()
        .unwrap();
    assert_eq!(missing.kind(), io::ErrorKind::InvalidInput);
    assert!(!output_path.exists());

    std::fs::write(&secret_path, [9u8; 31]).unwrap();
    let _secret = TestEnvVar::set(
        "HLTHPRT_PTG2_TIN_TOKEN_SECRET_FILE",
        secret_path.to_str().unwrap(),
    );
    let malformed = configured_shared_dedupe(2, false, true, false)
        .err()
        .unwrap();
    assert_eq!(malformed.kind(), io::ErrorKind::InvalidInput);
    assert!(!malformed
        .to_string()
        .contains(secret_path.to_str().unwrap()));
    assert!(!output_path.exists());

    std::fs::write(&secret_path, [9u8; 33]).unwrap();
    let oversized = configured_shared_dedupe(2, false, true, false)
        .err()
        .unwrap();
    assert_eq!(oversized.kind(), io::ErrorKind::InvalidInput);
    assert!(!oversized
        .to_string()
        .contains(secret_path.to_str().unwrap()));
    assert!(!output_path.exists());

    std::fs::write(&secret_path, [9u8; 32]).unwrap();
    let v1 = configured_shared_dedupe(2, false, true, false).unwrap();
    let mut paired_visits = 0;
    let mut accept_pair = |_, _, _| {
        paired_visits += 1;
        Ok(())
    };
    assert!(v1
        .visit_provider_group_tax_identity_pairs(&mut accept_pair)
        .is_err());
    let paired = configured_shared_dedupe(2, false, true, true).unwrap();
    paired
        .insert_provider_group_with_tax_identity(1, None)
        .unwrap();
    assert!(paired
        .visit_provider_group_tax_identity_pairs(&mut accept_pair)
        .is_ok());
    assert_eq!(paired_visits, 1);
    assert_eq!(
        configured_shared_dedupe(2, false, false, true)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(!output_path.exists());

    let _ = std::fs::remove_dir_all(directory);
}
#[test]
fn worker_copy_paths_suffix_every_worker_owned_output() {
    let coverage_scope_id = [7_u8; COVERAGE_SCOPE_ID_BYTES];
    let paths = CopyPathConfig {
        compact: Some("compact.copy".to_string()),
        manifest_serving: Some("manifest-serving.copy".to_string()),
        manifest_lean_serving: Some("manifest-lean-serving.copy".to_string()),
        v3_serving_run_directory: Some("serving-runs".to_string()),
        v3_coverage_scope_id: Some(coverage_scope_id),
        manifest_provider_forward_sidecar: Some("provider-forward.copy".to_string()),
        manifest_provider_inverted_sidecar: Some("provider-inverted.copy".to_string()),
        manifest_provider_set_component_sidecar: Some("provider-set-component.sidecar".to_string()),
        manifest_provider_component_group_sidecar: Some(
            "provider-component-group.sidecar".to_string(),
        ),
        manifest_provider_group_tax_identity_sidecar: Some(
            "provider-group-tax-identity.sidecar".to_string(),
        ),
        manifest_provider_group_tax_identity_v2_sidecar: Some(
            "provider-group-tax-identity-v2.sidecar".to_string(),
        ),
        manifest_provider_npi_sidecar: Some("provider-npi.copy".to_string()),
        manifest_price_forward_sidecar: Some("price-forward.copy".to_string()),
        manifest_price_atom: Some("manifest-price-atom.copy".to_string()),
        manifest_price_set_atom: Some("manifest-price-set-atom.copy".to_string()),
        manifest_price_set_summary: Some("manifest-price-set-summary.copy".to_string()),
        manifest_provider_group_member: Some("manifest-provider-group-member.copy".to_string()),
        manifest_code_count: Some("manifest-code-count.copy".to_string()),
        manifest_provider_set_dictionary: Some("manifest-provider-set-dictionary.copy".to_string()),
        procedure: Some("procedure.copy".to_string()),
        price_code_set: Some("price-code-set.copy".to_string()),
        price_atom: Some("price-atom.copy".to_string()),
        price_set_entry: Some("price-set-entry.copy".to_string()),
        provider_set: Some("provider-set.copy".to_string()),
        provider_set_component: Some("provider-set-component.copy".to_string()),
        provider_set_entry: Some("provider-set-entry.copy".to_string()),
        provider_entry_component: Some("provider-entry-component.copy".to_string()),
        provider_group_member: Some("provider-group-member.copy".to_string()),
        manifest_only: true,
    };

    assert!(paths.has_file_paths());
    assert!(paths.has_manifest_sidecar_paths());

    let worker_paths = paths.for_worker(12);
    let suffix = ".worker0012";
    for (original, worker) in [
        (&paths.compact, &worker_paths.compact),
        (&paths.manifest_serving, &worker_paths.manifest_serving),
        (
            &paths.manifest_lean_serving,
            &worker_paths.manifest_lean_serving,
        ),
        (
            &paths.manifest_price_atom,
            &worker_paths.manifest_price_atom,
        ),
        (
            &paths.manifest_price_set_atom,
            &worker_paths.manifest_price_set_atom,
        ),
        (
            &paths.manifest_price_set_summary,
            &worker_paths.manifest_price_set_summary,
        ),
        (
            &paths.manifest_provider_group_member,
            &worker_paths.manifest_provider_group_member,
        ),
        (
            &paths.manifest_code_count,
            &worker_paths.manifest_code_count,
        ),
        (
            &paths.manifest_provider_set_dictionary,
            &worker_paths.manifest_provider_set_dictionary,
        ),
        (&paths.procedure, &worker_paths.procedure),
        (&paths.price_code_set, &worker_paths.price_code_set),
        (&paths.price_atom, &worker_paths.price_atom),
        (&paths.price_set_entry, &worker_paths.price_set_entry),
        (&paths.provider_set, &worker_paths.provider_set),
        (
            &paths.provider_set_component,
            &worker_paths.provider_set_component,
        ),
        (&paths.provider_set_entry, &worker_paths.provider_set_entry),
        (
            &paths.provider_entry_component,
            &worker_paths.provider_entry_component,
        ),
        (
            &paths.provider_group_member,
            &worker_paths.provider_group_member,
        ),
    ] {
        assert_eq!(
            worker.as_deref(),
            Some(format!("{}{suffix}", original.as_deref().unwrap()).as_str())
        );
    }
    assert_eq!(
        worker_paths.v3_serving_run_directory,
        paths.v3_serving_run_directory
    );
    assert_eq!(worker_paths.v3_coverage_scope_id, Some(coverage_scope_id));
    assert_eq!(
        worker_paths.manifest_provider_forward_sidecar,
        paths.manifest_provider_forward_sidecar
    );
    assert_eq!(
        worker_paths.manifest_provider_inverted_sidecar,
        paths.manifest_provider_inverted_sidecar
    );
    assert_eq!(
        worker_paths.manifest_provider_group_tax_identity_sidecar,
        paths.manifest_provider_group_tax_identity_sidecar
    );
    assert_eq!(
        worker_paths.manifest_provider_group_tax_identity_v2_sidecar,
        paths.manifest_provider_group_tax_identity_v2_sidecar
    );
    assert_eq!(
        worker_paths.manifest_provider_npi_sidecar,
        paths.manifest_provider_npi_sidecar
    );
    assert_eq!(
        worker_paths.manifest_price_forward_sidecar,
        paths.manifest_price_forward_sidecar
    );
    assert!(worker_paths.manifest_only);

    let empty = CopyPathConfig::default();
    assert!(!empty.for_worker(12).has_file_paths());
    assert!(!empty.for_provider_refs().has_file_paths());

    let relative = normalized_absolute_lexical_path("./child/../leaf").unwrap();
    assert_eq!(relative, env::current_dir().unwrap().join("leaf"));
    assert_eq!(
        normalized_absolute_lexical_path("/../leaf").unwrap(),
        PathBuf::from("/leaf")
    );
}
#[test]
fn failed_scan_guard_removes_only_new_serving_run_files() {
    let base = std::env::temp_dir().join(format!("ptg2-serving-run-guard-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    std::fs::create_dir_all(&base).unwrap();
    let existing = base.join("ptg2-v3-serving-existing.ready");
    let unrelated = base.join("unrelated.ready");
    std::fs::write(&existing, b"existing").unwrap();
    let paths = CopyPathConfig {
        v3_serving_run_directory: Some(base.display().to_string()),
        ..CopyPathConfig::default()
    };

    {
        let _guard = ServingRunScanGuard::from_config(&paths);
        std::fs::write(base.join("ptg2-v3-serving-new.ready"), b"ready").unwrap();
        std::fs::write(base.join(".ptg2-v3-serving-new.partial"), b"partial").unwrap();
        std::fs::write(&unrelated, b"unrelated").unwrap();
    }

    assert_eq!(std::fs::read(&existing).unwrap(), b"existing");
    assert_eq!(std::fs::read(&unrelated).unwrap(), b"unrelated");
    assert!(!base.join("ptg2-v3-serving-new.ready").exists());
    assert!(!base.join(".ptg2-v3-serving-new.partial").exists());
    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn manifest_only_disables_high_cardinality_v2_dictionary_sinks() {
    let provider_set_entry_path = std::env::temp_dir().join(format!(
        "ptg2-provider-set-entry-{}.copy",
        std::process::id()
    ));
    let paths = CopyPathConfig {
        compact: None,
        manifest_serving: None,
        manifest_lean_serving: None,
        v3_serving_run_directory: None,
        v3_coverage_scope_id: None,
        manifest_provider_forward_sidecar: None,
        manifest_provider_inverted_sidecar: None,
        manifest_provider_set_component_sidecar: None,
        manifest_provider_component_group_sidecar: None,
        manifest_provider_group_tax_identity_sidecar: None,
        manifest_provider_group_tax_identity_v2_sidecar: None,
        manifest_provider_npi_sidecar: None,
        manifest_price_forward_sidecar: None,
        manifest_price_atom: None,
        manifest_price_set_atom: None,
        manifest_price_set_summary: None,
        manifest_provider_group_member: None,
        manifest_code_count: None,
        manifest_provider_set_dictionary: None,
        procedure: None,
        price_code_set: None,
        price_atom: None,
        price_set_entry: Some("unused-price-set-entry.copy".to_string()),
        provider_set: None,
        provider_set_component: Some("unused-provider-set-component.copy".to_string()),
        provider_set_entry: Some(provider_set_entry_path.to_string_lossy().to_string()),
        provider_entry_component: None,
        provider_group_member: None,
        manifest_only: true,
    };

    let sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();

    assert!(sinks.price_set_entry.is_none());
    assert!(sinks.provider_set_component.is_none());
    assert!(sinks.provider_set_entry.is_some());
    drop(sinks);
    let _ = std::fs::remove_file(provider_set_entry_path);
}
#[test]
fn provider_reference_copy_sinks_are_suffix_isolated() {
    let base = std::env::temp_dir().join(format!(
        "ptg2-provider-ref-copy-test-{}",
        std::process::id()
    ));
    let _ = std::fs::create_dir_all(&base);
    let manifest_member_path = base.join("manifest-provider-group-member.copy");
    let member_path = base.join("provider-group-member.copy");
    let paths = CopyPathConfig {
        compact: Some(
            base.join("unused-compact.copy")
                .to_string_lossy()
                .to_string(),
        ),
        manifest_serving: Some(
            base.join("unused-serving.copy")
                .to_string_lossy()
                .to_string(),
        ),
        manifest_lean_serving: None,
        v3_serving_run_directory: None,
        v3_coverage_scope_id: None,
        manifest_provider_forward_sidecar: None,
        manifest_provider_inverted_sidecar: None,
        manifest_provider_set_component_sidecar: None,
        manifest_provider_component_group_sidecar: None,
        manifest_provider_group_tax_identity_sidecar: Some(
            base.join("provider-group-tax-identity.sidecar")
                .to_string_lossy()
                .to_string(),
        ),
        manifest_provider_group_tax_identity_v2_sidecar: Some(
            base.join("provider-group-tax-identity-v2.sidecar")
                .to_string_lossy()
                .to_string(),
        ),
        manifest_provider_npi_sidecar: None,
        manifest_price_forward_sidecar: None,
        manifest_price_atom: Some(base.join("unused-price.copy").to_string_lossy().to_string()),
        manifest_price_set_atom: None,
        manifest_price_set_summary: None,
        manifest_provider_group_member: Some(manifest_member_path.to_string_lossy().to_string()),
        manifest_code_count: None,
        manifest_provider_set_dictionary: None,
        procedure: Some(
            base.join("unused-procedure.copy")
                .to_string_lossy()
                .to_string(),
        ),
        price_code_set: None,
        price_atom: None,
        price_set_entry: None,
        provider_set: None,
        provider_set_component: None,
        provider_set_entry: None,
        provider_entry_component: None,
        provider_group_member: Some(member_path.to_string_lossy().to_string()),
        manifest_only: true,
    };

    let provider_ref_paths = paths.for_provider_refs();
    assert_eq!(
        provider_ref_paths.manifest_provider_group_tax_identity_sidecar,
        paths.manifest_provider_group_tax_identity_sidecar
    );
    assert_eq!(
        provider_ref_paths.manifest_provider_group_tax_identity_v2_sidecar,
        paths.manifest_provider_group_tax_identity_v2_sidecar
    );
    let mut sinks = DictionaryCopySinks::from_paths(&provider_ref_paths, 0).unwrap();
    let dedupe = SharedDedupe::new(1);
    let provider_ref = json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890, 1234567891]
        }]
    });

    sinks
        .write_provider_group_members_shared(&provider_ref, &dedupe, false)
        .unwrap();
    sinks
        .write_provider_group_members_shared(&provider_ref, &dedupe, false)
        .unwrap();
    let events = sinks.finish_silent().unwrap();

    assert_eq!(events.len(), 2);
    assert!(events.iter().all(|event| {
        event.path.ends_with(".provider_refs")
            && event.row_count == 2
            && matches!(
                event.record_kind.as_str(),
                "manifest_provider_group_member_copy_file" | "provider_group_member_copy_file"
            )
    }));
    assert!(manifest_member_path
        .with_extension("copy.provider_refs")
        .exists());
    assert!(member_path.with_extension("copy.provider_refs").exists());
    assert!(!manifest_member_path.exists());
    assert!(!member_path.exists());
    let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(summary["provider_group_attempted"], 2);
    assert_eq!(summary["provider_group_unique"], 1);
    assert_eq!(summary["provider_group_duplicate"], 1);
    assert_eq!(summary["provider_group_member_attempted"], 2);
    assert_eq!(summary["provider_group_member_unique"], 2);
    assert_eq!(summary["provider_group_member_duplicate"], 0);

    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v4_tax_identity_sidecar_is_token_only_complete_and_deterministic() {
    fn build_artifact(path: &Path, worker_count: usize, reverse: bool) -> (Vec<u8>, String) {
        let policy =
            TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:test-1".to_string(), [7u8; 32])
                .unwrap();
        let dedupe = SharedDedupe::new_with_v4_tax_identity(worker_count, false, policy);
        let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        let mut provider_refs = vec![
            json!({"provider_groups": [{
                "tin": {"type": "ein", "value": "01💥2345678"},
                "npi": [1234567890]
            }]}),
            json!({"provider_groups": [{
                "tin": {"type": " EIN ", "value": "012345678"},
                "npi": [1234567890]
            }]}),
            json!({"provider_groups": [{
                "npi": []
            }]}),
            json!({"provider_groups": [{
                "tin": {"type": "other", "value": "opaque"},
                "npi": [1234567891]
            }]}),
        ];
        if reverse {
            provider_refs.reverse();
        }
        for provider_ref in provider_refs {
            sinks
                .write_provider_group_members_shared(&provider_ref, &dedupe, true)
                .unwrap();
        }
        let paths = CopyPathConfig {
            manifest_provider_group_tax_identity_sidecar: Some(path.to_string_lossy().to_string()),
            ..CopyPathConfig::default()
        };
        let mut event = Vec::new();
        emit_provider_group_tax_identity_sidecar(&mut event, &paths, &dedupe).unwrap();
        (
            std::fs::read(path).unwrap(),
            String::from_utf8(event).unwrap(),
        )
    }

    let base =
        std::env::temp_dir().join(format!("ptg2-provider-tax-identity-{}", std::process::id()));
    let _ = std::fs::create_dir_all(&base);
    let first_path = base.join("first.ptg2tax");
    let second_path = base.join("second.ptg2tax");
    let (first, first_event) = build_artifact(&first_path, 1, false);
    let (second, _second_event) = build_artifact(&second_path, 8, true);

    assert_eq!(first, second);
    assert_eq!(&first[..8], PROVIDER_GROUP_TAX_IDENTITY_MAGIC);
    assert_eq!(
        u16::from_le_bytes(first[8..10].try_into().unwrap()),
        PROVIDER_GROUP_TAX_IDENTITY_VERSION
    );
    assert_eq!(
        u16::from_le_bytes(first[10..12].try_into().unwrap()),
        PROVIDER_GROUP_TAX_IDENTITY_RECORD_BYTES
    );
    let policy_length = first[12] as usize;
    let records = &first[13 + policy_length..];
    assert_eq!(
        records.len(),
        PROVIDER_GROUP_TAX_IDENTITY_RECORD_BYTES as usize * 3
    );
    let rows = records
        .chunks_exact(PROVIDER_GROUP_TAX_IDENTITY_RECORD_BYTES as usize)
        .collect::<Vec<_>>();
    assert!(rows.windows(2).all(|pair| pair[0][..16] < pair[1][..16]));
    assert_eq!(
        rows.iter()
            .filter(|row| row[16] == TaxIdentityState::MatchedEin as u8)
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row[16] == TaxIdentityState::Missing as u8)
            .count(),
        1
    );
    assert_eq!(
        rows.iter()
            .filter(|row| row[16] == TaxIdentityState::UnsupportedType as u8)
            .count(),
        1
    );
    for row in rows {
        let token = &row[17..];
        if row[16] == TaxIdentityState::MatchedEin as u8 {
            assert!(token.iter().any(|byte| *byte != 0));
            assert_eq!(&token[..16], &token[16..32]);
        } else {
            assert!(token.iter().all(|byte| *byte == 0));
        }
    }
    assert!(first_event.contains(r#""row_count":3"#));
    assert!(first_event.contains(r#""matched_ein_count":1"#));
    assert!(first_event.contains(r#""missing_count":1"#));
    assert!(first_event.contains(r#""unsupported_type_count":1"#));
    assert!(!first
        .windows("012345678".len())
        .any(|part| part == b"012345678"));
    assert!(!first.windows("opaque".len()).any(|part| part == b"opaque"));
    assert!(!first_event.contains("012345678"));
    assert!(!first_event.contains("opaque"));

    let _ = std::fs::remove_dir_all(base);
}
#[test]
fn v2_tax_identity_sidecar_is_opt_in_deterministic_private_and_v1_compatible() {
    let directory = tempfile::tempdir().unwrap();
    let v1_path = directory.path().join("v1-only.ptg2tax");
    let v1_paths = CopyPathConfig {
        manifest_provider_group_tax_identity_sidecar: Some(v1_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let v1_dedupe = synthetic_v2_tax_identity_dedupe(2, false, false);
    let mut direct_v1_event = Vec::new();
    emit_provider_group_tax_identity_sidecar(&mut direct_v1_event, &v1_paths, &v1_dedupe).unwrap();
    let direct_v1_bytes = std::fs::read(&v1_path).unwrap();
    let paired_v1_dedupe = synthetic_v2_tax_identity_dedupe(8, true, true);
    let mut paired_v1_event = Vec::new();
    emit_provider_group_tax_identity_sidecar(&mut paired_v1_event, &v1_paths, &paired_v1_dedupe)
        .unwrap();
    assert_eq!(std::fs::read(&v1_path).unwrap(), direct_v1_bytes);
    assert_eq!(paired_v1_event, direct_v1_event);
    let mut delegated_v1_event = Vec::new();
    emit_provider_group_tax_identity_sidecars(&mut delegated_v1_event, &v1_paths, &v1_dedupe)
        .unwrap();
    assert_eq!(delegated_v1_event, direct_v1_event);
    assert_eq!(std::fs::read(&v1_path).unwrap(), direct_v1_bytes);
    let delegated_v1_text = String::from_utf8(delegated_v1_event.clone()).unwrap();
    assert_eq!(
        delegated_v1_text
            .matches("manifest_provider_group_tax_identity_sidecar_file")
            .count(),
        1
    );
    assert!(!delegated_v1_text.contains("tax_identity_v2_sidecar_file"));
    let mut absent_v2_event = Vec::new();
    emit_provider_group_tax_identity_v2_sidecar(
        &mut absent_v2_event,
        &CopyPathConfig::default(),
        &v1_dedupe,
    )
    .unwrap();
    assert!(absent_v2_event.is_empty());

    let (paired_v1, first_v2, first_events) =
        emit_synthetic_v2_tax_identity_artifacts(directory.path(), "first", 1, false);
    let (reversed_v1, reversed_v2, _reversed_events) =
        emit_synthetic_v2_tax_identity_artifacts(directory.path(), "reversed", 8, true);
    assert_eq!(paired_v1, direct_v1_bytes);
    assert_eq!(reversed_v1, direct_v1_bytes);
    assert_eq!(reversed_v2, first_v2);
    let frozen_v2_sha256 = "c7a3b0b0bbae41ed968bda60a91a2d03717f0f225e1130b3565bfab9a619a204";
    assert_eq!(first_v2.len(), 368);
    assert_eq!(sha256_hex(&Sha256::digest(&first_v2)), frozen_v2_sha256);
    assert!(!directory.path().join("first-v1.ptg2tax.building").exists());
    assert!(!directory.path().join("first-v2.ptg2tax.building").exists());

    let mut validator =
        ptg2_scanner::tax_identity_sidecar_v2::TaxIdentitySidecarV2StreamValidator::new(
            Cursor::new(&first_v2),
            5,
        )
        .unwrap();
    assert_eq!(
        validator.header().policy_id(),
        "ptg-tin-hmac-sha256-v1:test-v2"
    );
    let mut state_codes = Vec::new();
    while let Some(record) = validator.next_record().unwrap() {
        state_codes.push(record.state() as u8);
    }
    state_codes.sort_unstable();
    assert_eq!(state_codes, vec![1, 2, 3, 4, 5]);
    assert_eq!(validator.records_validated(), 5);

    let mut pair_validator =
        ptg2_scanner::tax_identity_sidecar_pair::TaxIdentitySidecarPairValidator::new(
            Cursor::new(paired_v1.clone()),
            Cursor::new(first_v2.clone()),
            5,
        )
        .unwrap();
    assert_eq!(pair_validator.policy_id(), "ptg-tin-hmac-sha256-v1:test-v2");
    let mut paired_state_codes = Vec::new();
    while let Some(record) = pair_validator.next_record().unwrap() {
        assert_eq!(
            record.v1().provider_group_global_id(),
            record.v2().provider_group_global_id()
        );
        paired_state_codes.push(record.v2().state() as u8);
    }
    paired_state_codes.sort_unstable();
    assert_eq!(paired_state_codes, vec![1, 2, 3, 4, 5]);
    assert_eq!(pair_validator.records_validated(), 5);
    let pair_summary = pair_validator.validated_summary().unwrap();
    assert_eq!(pair_summary.row_count(), 5);
    assert_eq!(pair_summary.matched_ein_count(), 1);
    assert_eq!(pair_summary.matched_npi_count(), 1);
    assert_eq!(pair_summary.missing_count(), 1);
    assert_eq!(pair_summary.malformed_count(), 1);
    assert_eq!(pair_summary.unsupported_type_count(), 1);

    let events = String::from_utf8(first_events).unwrap();
    let v1_event_offset = events
        .find("manifest_provider_group_tax_identity_sidecar_file")
        .unwrap();
    let v2_event_offset = events
        .find("manifest_provider_group_tax_identity_v2_sidecar_file")
        .unwrap();
    assert!(v1_event_offset < v2_event_offset);
    let v2_frame = &events[v2_event_offset..];
    let v2_header_end = v2_frame.find('\n').unwrap();
    let v2_payload_bytes = v2_frame[..v2_header_end]
        .rsplit_once('\t')
        .unwrap()
        .1
        .parse::<usize>()
        .unwrap();
    let v2_payload_start = v2_header_end + 1;
    let v2_metadata: Value =
        serde_json::from_str(&v2_frame[v2_payload_start..v2_payload_start + v2_payload_bytes])
            .unwrap();
    let expected_v2_metadata = json!({
        "path": directory.path().join("first-v2.ptg2tax").display().to_string(),
        "bytes": 368,
        "row_count": 5,
        "provider_group_count": 5,
        "matched_ein_count": 1,
        "matched_npi_count": 1,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 1,
        "format": "ptg2_provider_group_tax_identity_v2",
        "version": 2,
        "record_bytes": 65,
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test-v2",
        "normalization_contract":
            "ein_ascii_digits_or_2_7_hyphen_and_npi_10_ascii_digits_cms_80840_luhn_v2",
        "token_message_contract":
            "healthporta_ptg_tin_v1_nul_u16be_type_length_type_u16be_value_length_value",
        "hmac_contract": "hmac_sha256_ptg_tin_v1",
        "tin_id_128_contract": "first_16_bytes(tin_hmac_sha256)",
        "full_hmac_authority_contract":
            "tin_hmac_sha256_full_32_bytes_authoritative",
        "sha256": frozen_v2_sha256,
        "final": true,
    });
    assert_eq!(v2_metadata, expected_v2_metadata);
    let expected_v2_payload = serde_json::to_vec(&expected_v2_metadata).unwrap();
    let mut expected_v2_frame = format!(
        "manifest_provider_group_tax_identity_v2_sidecar_file\t{}\n",
        expected_v2_payload.len()
    )
    .into_bytes();
    expected_v2_frame.extend_from_slice(&expected_v2_payload);
    expected_v2_frame.push(b'\n');
    assert_eq!(v2_frame.as_bytes(), expected_v2_frame);
    for private in [
        "12-3456789",
        "123456789",
        "1000000491",
        "1000000492",
        "private-unsupported-marker",
        "Private Synthetic Practice One",
        "Private Synthetic Practice Two",
    ] {
        assert!(!events.contains(private));
        assert!(!paired_v1
            .windows(private.len())
            .any(|window| window == private.as_bytes()));
        assert!(!first_v2
            .windows(private.len())
            .any(|window| window == private.as_bytes()));
    }
}
#[test]
fn tax_identity_writers_preserve_symlink_and_hardlink_scratch_sentinels() {
    use std::os::unix::fs::symlink;

    let directory = tempfile::tempdir().unwrap();
    let dedupe = synthetic_v2_tax_identity_dedupe(2, true, false);
    for version in ["v1", "v2"] {
        for relation_kind in ["symlink", "hardlink"] {
            let label = format!("{version}-{relation_kind}");
            let sentinel = directory.path().join(format!("{label}-sentinel"));
            let output = directory.path().join(format!("{label}.ptg2tax"));
            let legacy_scratch = PathBuf::from(format!("{}.building", output.display()));
            let sentinel_bytes = format!("{label}-sentinel-bytes").into_bytes();
            std::fs::write(&sentinel, &sentinel_bytes).unwrap();
            if relation_kind == "symlink" {
                symlink(&sentinel, &legacy_scratch).unwrap();
            } else {
                std::fs::hard_link(&sentinel, &legacy_scratch).unwrap();
            }
            let paths = if version == "v1" {
                CopyPathConfig {
                    manifest_provider_group_tax_identity_sidecar: Some(
                        output.display().to_string(),
                    ),
                    ..CopyPathConfig::default()
                }
            } else {
                CopyPathConfig {
                    manifest_provider_group_tax_identity_v2_sidecar: Some(
                        output.display().to_string(),
                    ),
                    ..CopyPathConfig::default()
                }
            };
            let mut events = Vec::new();
            let error = if version == "v1" {
                emit_provider_group_tax_identity_sidecar(&mut events, &paths, &dedupe).unwrap_err()
            } else {
                emit_provider_group_tax_identity_v2_sidecar(&mut events, &paths, &dedupe)
                    .unwrap_err()
            };
            assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
            assert!(events.is_empty());
            assert!(!output.exists());
            assert_eq!(std::fs::read(&sentinel).unwrap(), sentinel_bytes);
            assert!(std::fs::symlink_metadata(&legacy_scratch).is_ok());
            assert!(!error
                .to_string()
                .contains(directory.path().to_str().unwrap()));
            for private in ["12-3456789", "1000000491", "Private Synthetic Practice One"] {
                assert!(!error.to_string().contains(private));
            }
            std::fs::remove_file(&legacy_scratch).unwrap();
        }
    }
    assert!(std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .all(|entry| !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".ptg2-tax-identity-")));
}
#[test]
fn tax_identity_private_scratch_rejects_post_open_link_and_path_replacement() {
    use std::os::unix::fs::{symlink, MetadataExt, PermissionsExt};

    let directory = tempfile::tempdir().unwrap();
    let output = directory.path().join("hardlink.ptg2tax");
    let (scratch, file) = TaxIdentitySidecarScratch::create(&output).unwrap();
    assert_eq!(
        std::fs::metadata(scratch.directory.path())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert_eq!(
        std::fs::metadata(&scratch.artifact_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
    let hardlink = scratch.directory.path().join("artifact-hardlink");
    std::fs::hard_link(&scratch.artifact_path, &hardlink).unwrap();
    let mut writer = TaxIdentityArtifactWriter::new(file);
    writer.write_all(b"hardlink-scratch").unwrap();
    let error = writer.finish(&scratch).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        std::fs::metadata(&scratch.artifact_path).unwrap().nlink(),
        2
    );
    drop(scratch);
    assert!(!output.exists());

    let output = directory.path().join("pathname.ptg2tax");
    let sentinel = directory.path().join("pathname-sentinel");
    std::fs::write(&sentinel, b"pathname-sentinel").unwrap();
    let (scratch, file) = TaxIdentitySidecarScratch::create(&output).unwrap();
    std::fs::remove_file(&scratch.artifact_path).unwrap();
    symlink(&sentinel, &scratch.artifact_path).unwrap();
    let mut writer = TaxIdentityArtifactWriter::new(file);
    writer.write_all(b"unlinked-owned-scratch").unwrap();
    let error = writer.finish(&scratch).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert_eq!(std::fs::read(&sentinel).unwrap(), b"pathname-sentinel");
    drop(scratch);
    assert!(!output.exists());
    assert!(std::fs::read_dir(directory.path())
        .unwrap()
        .filter_map(Result::ok)
        .all(|entry| !entry
            .file_name()
            .to_string_lossy()
            .starts_with(".ptg2-tax-identity-")));
}
#[test]
fn v2_tax_identity_failure_emits_no_buffered_success_frame() {
    let directory = tempfile::tempdir().unwrap();
    let v1_path = directory.path().join("orphan-v1.ptg2tax");
    let v2_path = directory.path().join("v2-final-directory");
    std::fs::create_dir(&v2_path).unwrap();
    let paths = CopyPathConfig {
        manifest_provider_group_tax_identity_sidecar: Some(v1_path.display().to_string()),
        manifest_provider_group_tax_identity_v2_sidecar: Some(v2_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let dedupe = synthetic_v2_tax_identity_dedupe(1, true, false);
    let mut events = Vec::new();

    let error =
        emit_provider_group_tax_identity_sidecars(&mut events, &paths, &dedupe).unwrap_err();

    assert!(events.is_empty());
    assert!(v1_path.is_file());
    assert!(v2_path.is_dir());
    assert!(!PathBuf::from(format!("{}.building", v2_path.display())).exists());
    for private in ["12-3456789", "1000000491", "Private Synthetic Practice One"] {
        assert!(!error.to_string().contains(private));
    }

    let v2_only_path = directory.path().join("v2-only.ptg2tax");
    let v2_only_paths = CopyPathConfig {
        manifest_provider_group_tax_identity_v2_sidecar: Some(v2_only_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let mut v2_only_events = Vec::new();
    let v2_only_error =
        emit_provider_group_tax_identity_sidecars(&mut v2_only_events, &v2_only_paths, &dedupe)
            .unwrap_err();
    assert_eq!(v2_only_error.kind(), io::ErrorKind::InvalidInput);
    assert!(v2_only_events.is_empty());
    assert!(!v2_only_path.exists());
    assert!(!PathBuf::from(format!("{}.building", v2_only_path.display())).exists());
}
#[test]
fn v2_tax_identity_writer_helpers_fail_closed() {
    let directory = tempfile::tempdir().unwrap();
    let v2_path = directory.path().join("unconfigured-v2.ptg2tax");
    let paths = CopyPathConfig {
        manifest_provider_group_tax_identity_v2_sidecar: Some(v2_path.display().to_string()),
        ..CopyPathConfig::default()
    };
    let mut events = Vec::new();
    let unconfigured = SharedDedupe::new(1);
    let error = emit_provider_group_tax_identity_v2_sidecar(&mut events, &paths, &unconfigured)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::Other);
    assert!(events.is_empty());
    assert!(!v2_path.exists());
    assert!(!PathBuf::from(format!("{}.building", v2_path.display())).exists());

    let lexical_parent = directory.path().join("lexical-parent");
    assert_eq!(
        normalized_absolute_lexical_path_value(
            &lexical_parent.join("discarded").join("..").join("artifact")
        )
        .unwrap(),
        lexical_parent.join("artifact")
    );
    assert_eq!(
        publication_coordinate(Path::new("/")).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let missing_publication_name = TaxIdentitySidecarScratch::create(Path::new("/"))
        .err()
        .unwrap();
    assert_eq!(missing_publication_name.kind(), io::ErrorKind::InvalidInput);

    let unresolved_collision_path = directory
        .path()
        .join("missing-collision-parent")
        .join("artifact");
    let unresolved_collision = RuntimeCollisionPath::new(&unresolved_collision_path)
        .err()
        .unwrap();
    assert_eq!(unresolved_collision.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        unresolved_collision.to_string(),
        "tax identity collision coordinate could not be resolved"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::symlink;

        let symlink_loop = directory.path().join("collision-symlink-loop");
        symlink(&symlink_loop, &symlink_loop).unwrap();
        let loop_collision = RuntimeCollisionPath::new(&symlink_loop).err().unwrap();
        assert_eq!(loop_collision.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            loop_collision.to_string(),
            "tax identity collision coordinate could not be resolved"
        );
        std::fs::remove_file(symlink_loop).unwrap();
    }

    let overflow_path = directory.path().join("overflow.ptg2tax");
    let (overflow_scratch, overflow_file) =
        TaxIdentitySidecarScratch::create(&overflow_path).unwrap();
    let mut overflow_writer = TaxIdentityArtifactWriter::new(overflow_file);
    overflow_writer.flush().unwrap();
    overflow_writer.byte_count = u64::MAX;
    assert_eq!(
        overflow_writer.write(b"x").unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    drop(overflow_writer);
    drop(overflow_scratch);
    assert!(!overflow_path.exists());

    let mut maximum = u64::MAX;
    assert_eq!(
        checked_tax_identity_v2_increment(&mut maximum)
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(
        checked_tax_identity_v2_total([u64::MAX, 1, 0, 0, 0])
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );

    let mut previous = None;
    validate_tax_identity_v2_group_order(&mut previous, [2; 16]).unwrap();
    assert!(validate_tax_identity_v2_group_order(&mut previous, [2; 16]).is_err());
    previous = Some([2; 16]);
    assert!(validate_tax_identity_v2_group_order(&mut previous, [1; 16]).is_err());

    let invalid_v1 = TaxIdentityObservation {
        state: TaxIdentityState::MatchedEin,
        tin_hmac_sha256: Some([1; 32]),
    };
    let invalid_v2 = TaxIdentityObservationV2 {
        state: TaxIdentityStateV2::MatchedNpi,
        tin_hmac_sha256: Some([1; 32]),
    };
    assert!(validate_tax_identity_v1_v2_transition(invalid_v1, invalid_v2).is_err());
}
#[test]
fn provider_entry_always_retains_npis_for_exact_cross_reference_counts() {
    let provider_ref = json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890, 1234567891]
        }]
    });

    let entry = build_provider_entry(&provider_ref).unwrap();

    assert_eq!(entry.provider_count, 2);
    assert_eq!(entry.npi, vec![1234567890, 1234567891]);
}
#[test]
fn provider_entry_count_uses_distinct_npi_union() {
    let provider_ref = json!({
        "provider_groups": [
            {
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890, 1234567891]
            },
            {
                "tin": {"type": "ein", "value": "987654321"},
                "npi": [1234567890, 1234567892]
            }
        ]
    });

    let entry = build_provider_entry(&provider_ref).unwrap();

    assert_eq!(entry.provider_count, 3);
    assert_eq!(entry.npi, vec![1234567890, 1234567891, 1234567892]);
}
#[test]
fn provider_set_count_deduplicates_overlapping_npis_across_references() {
    let mut provider_map = HashMap::new();
    provider_map.insert(
        ProviderRefKey::from("first"),
        build_provider_entry(&json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890, 1234567891]
            }]
        }))
        .unwrap(),
    );
    provider_map.insert(
        ProviderRefKey::from("second"),
        build_provider_entry(&json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "987654321"},
                "npi": [1234567891, 1234567892]
            }]
        }))
        .unwrap(),
    );

    let entry = provider_set_from_ref_keys(
        &provider_map,
        &[
            ProviderRefKey::from("first"),
            ProviderRefKey::from("second"),
        ],
    )
    .unwrap()
    .unwrap();

    assert_eq!(entry.provider_count, 3);
    assert_eq!(entry.npi, vec![1234567890, 1234567891, 1234567892]);
}
#[test]
fn provider_set_count_unions_npis_even_when_entry_hashes_match() {
    let mut provider_map = HashMap::new();
    provider_map.insert(
        ProviderRefKey::from("first"),
        ProviderEntry {
            entry_hash: 42,
            provider_group_scope_hash: 0,
            provider_count: 2,
            provider_group_hashes: vec![101],
            npi: vec![1234567890, 1234567891],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: Vec::new(),
            source_locators: Vec::new(),
        },
    );
    provider_map.insert(
        ProviderRefKey::from("second"),
        ProviderEntry {
            entry_hash: 42,
            provider_group_scope_hash: 0,
            provider_count: 2,
            provider_group_hashes: vec![202],
            npi: vec![1234567891, 1234567892],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: Vec::new(),
            source_locators: Vec::new(),
        },
    );

    let entry = provider_set_from_ref_keys(
        &provider_map,
        &[
            ProviderRefKey::from("first"),
            ProviderRefKey::from("second"),
        ],
    )
    .unwrap()
    .unwrap();

    assert_eq!(entry.provider_count, 3);
    assert_eq!(entry.npi, vec![1234567890, 1234567891, 1234567892]);
    assert_eq!(entry.provider_group_hashes, vec![101, 202]);
}
#[test]
fn v4_factor_cache_preserves_legacy_identity_for_equivalent_compositions() {
    let mut provider_map = HashMap::new();
    provider_map.insert(
        ProviderRefKey::from("a"),
        ProviderEntry {
            entry_hash: 11,
            provider_group_scope_hash: 0,
            provider_count: 2,
            provider_group_hashes: vec![101],
            npi: vec![1_000_000_001, 1_000_000_002],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: vec!["shared".to_string()],
            source_locators: Vec::new(),
        },
    );
    provider_map.insert(
        ProviderRefKey::from("b"),
        ProviderEntry {
            entry_hash: 12,
            provider_group_scope_hash: 0,
            provider_count: 2,
            provider_group_hashes: vec![202],
            npi: vec![1_000_000_002, 1_000_000_003],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: vec!["shared".to_string()],
            source_locators: Vec::new(),
        },
    );
    provider_map.insert(
        ProviderRefKey::from("c"),
        ProviderEntry {
            entry_hash: 13,
            provider_group_scope_hash: 0,
            provider_count: 3,
            provider_group_hashes: vec![101, 202],
            npi: vec![1_000_000_001, 1_000_000_002, 1_000_000_003],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: vec!["shared".to_string()],
            source_locators: Vec::new(),
        },
    );
    let context = test_compact_context();
    let rate_ab = RateLite {
        provider_refs: vec![ProviderRefKey::from("b"), ProviderRefKey::from("a")],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: vec!["rate".to_string()],
        prices: vec![test_price_lite("100.00")],
        prepared_price_set: None,
    };
    let rate_c = RateLite {
        provider_refs: vec![ProviderRefKey::from("c")],
        ..rate_ab.clone()
    };
    let legacy_ab = provider_set_from_ref_keys(&provider_map, &rate_ab.provider_refs)
        .unwrap()
        .unwrap();
    let expected_networks = rate_network_names(&rate_ab, &legacy_ab.network_names, &context);
    let expected_hash =
        provider_set_scope_hash(&legacy_ab.provider_group_hashes, &expected_networks);
    let expected_global = provider_set_global_id_from_group_hashes_and_network_names(
        &legacy_ab.provider_group_hashes,
        &expected_networks,
    );

    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let mut cache = V4ProviderSetFactorCache::new(true, Arc::clone(&shared));
    let factored_ab = cache
        .resolve(
            &provider_map,
            &rate_ab.provider_refs,
            None,
            &rate_ab,
            &context,
        )
        .unwrap()
        .unwrap();
    let factored_c = cache
        .resolve(
            &provider_map,
            &rate_c.provider_refs,
            None,
            &rate_c,
            &context,
        )
        .unwrap()
        .unwrap();
    assert_eq!(factored_ab.provider_set_hash, expected_hash);
    assert_eq!(factored_ab.provider_set_global_id, expected_global);
    assert_eq!(factored_c.provider_set_global_id, expected_global);
    assert_eq!(factored_ab.provider_count, 3);
    assert_eq!(factored_c.provider_count, 3);
    assert_eq!(
        HashSet::from([
            factored_ab.provider_set_global_id,
            factored_c.provider_set_global_id,
        ])
        .len(),
        1
    );

    let before = cache.metrics;
    let repeated = cache
        .resolve(
            &provider_map,
            &rate_ab.provider_refs,
            None,
            &rate_ab,
            &context,
        )
        .unwrap()
        .unwrap();
    assert_eq!(repeated.provider_set_global_id, expected_global);
    assert_eq!(
        cache.metrics.flat_group_union_attempts,
        before.flat_group_union_attempts
    );
    assert_eq!(cache.metrics.npi_union_attempts, before.npi_union_attempts);
    assert_eq!(cache.metrics.flat_group_union_attempts, 2);
    assert_eq!(cache.metrics.npi_union_attempts, 2);
    assert_eq!(cache.metrics.cache_hits, 1);
    assert_eq!(cache.metrics.cache_misses, 2);
    assert_eq!(cache.metrics.cache_resets, 0);

    let source = EmittedProviderNpis::Factor {
        provider_count: factored_ab.provider_count as u64,
        inline_npis: &[],
    };
    assert_eq!(
        source.nth(0, &rate_ab, &provider_map).unwrap(),
        1_000_000_001
    );
    assert_eq!(
        source.nth(2, &rate_ab, &provider_map).unwrap(),
        1_000_000_003
    );
    let (entries, estimated_bytes, max_bytes) = shared.snapshot();
    assert_eq!(entries, 2);
    assert!(estimated_bytes > 0);
    assert!(estimated_bytes < 4096);
    assert_eq!(max_bytes, 1024 * 1024);
}
#[test]
fn v4_factor_resolution_advances_semantic_progress_before_object_completion() {
    let group_hashes = (1..=65_537).collect::<Vec<_>>();
    let npis = (1_000_000_000..1_000_065_537).collect::<Vec<_>>();
    let key = ProviderRefKey::from("wide");
    let provider_map = HashMap::from([(
        key.clone(),
        ProviderEntry {
            entry_hash: 91,
            provider_group_scope_hash: 0,
            provider_count: npis.len() as i64,
            provider_group_hashes: group_hashes,
            npi: npis,
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: Vec::new(),
            source_locators: Vec::new(),
        },
    )]);
    let rate = RateLite {
        provider_refs: vec![key],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: vec![test_price_lite("1")],
        prepared_price_set: None,
    };
    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let progress = shared.semantic_progress_if_enabled().unwrap();
    let before = progress.snapshot();
    let mut cache = V4ProviderSetFactorCache::new(true, shared);

    cache
        .resolve(
            &provider_map,
            &rate.provider_refs,
            None,
            &rate,
            &test_compact_context(),
        )
        .unwrap()
        .unwrap();

    let after = progress.snapshot();
    assert_eq!(
        before,
        ptg2_scanner::progress::ScannerSemanticSnapshot::default()
    );
    assert!(after.semantic_work_completed > before.semantic_work_completed);
    assert!(after.provider_group_union_visits >= 65_537 * 2);
    assert!(after.provider_npi_union_visits >= 65_537);
    assert_eq!(after.in_network_objects_completed, 0);
}
#[test]
fn v4_factor_cache_fails_admission_without_retaining_expanded_unions() {
    let provider_map = HashMap::from([(
        ProviderRefKey::from("wide"),
        ProviderEntry {
            entry_hash: 91,
            provider_group_scope_hash: 0,
            provider_count: 4,
            provider_group_hashes: vec![1, 2, 3, 4],
            npi: vec![1_000_000_001, 1_000_000_002, 1_000_000_003, 1_000_000_004],
            quarantined_npi: Vec::new(),
            quarantined_npi_text: Vec::new(),
            network_names: Vec::new(),
            source_locators: Vec::new(),
        },
    )]);
    let rate = RateLite {
        provider_refs: vec![ProviderRefKey::from("wide")],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: Vec::new(),
        prices: vec![test_price_lite("1")],
        prepared_price_set: None,
    };
    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1));
    let mut cache = V4ProviderSetFactorCache::new(true, Arc::clone(&shared));
    let error = cache
        .resolve(
            &provider_map,
            &rate.provider_refs,
            None,
            &rate,
            &test_compact_context(),
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::OutOfMemory);
    assert_eq!(shared.snapshot(), (0, 0, 1));
    assert!(cache.buckets.is_empty());
}
#[test]
fn v4_factor_cache_covers_source_ordinals_and_shared_cache_paths() {
    let key = ProviderRefKey::from("component");
    let component = ProviderEntry {
        entry_hash: 71,
        provider_group_scope_hash: 0,
        provider_count: 2,
        provider_group_hashes: vec![101, 202],
        npi: vec![1_000_000_001, 1_000_000_002],
        quarantined_npi: Vec::new(),
        quarantined_npi_text: Vec::new(),
        network_names: vec!["component-network".to_string()],
        source_locators: Vec::new(),
    };
    let inline = ProviderEntry {
        entry_hash: 72,
        provider_group_scope_hash: 0,
        provider_count: 2,
        provider_group_hashes: vec![202, 303],
        npi: vec![1_000_000_002, 1_000_000_003],
        quarantined_npi: Vec::new(),
        quarantined_npi_text: Vec::new(),
        network_names: vec!["inline-network".to_string()],
        source_locators: Vec::new(),
    };
    let provider_map = HashMap::from([(key.clone(), component)]);
    let rate = RateLite {
        provider_refs: vec![key.clone()],
        provider_groups: Vec::new(),
        provider_groups_raw: None,
        network_names: vec!["rate-network".to_string()],
        prices: vec![test_price_lite("1")],
        prepared_price_set: None,
    };
    let context = test_compact_context();

    let exact_error = EmittedProviderNpis::Exact(&[1_000_000_001])
        .nth(1, &rate, &provider_map)
        .unwrap_err();
    assert!(exact_error
        .to_string()
        .contains("outside the exact provider set"));

    let missing_rate = RateLite {
        provider_refs: vec![ProviderRefKey::from("missing")],
        ..rate.clone()
    };
    let missing_error = EmittedProviderNpis::Factor {
        provider_count: 1,
        inline_npis: &[],
    }
    .nth(0, &missing_rate, &provider_map)
    .unwrap_err();
    assert!(missing_error
        .to_string()
        .contains("unresolved provider reference"));

    let ordinal_error = EmittedProviderNpis::Factor {
        provider_count: 2,
        inline_npis: &[],
    }
    .nth(2, &rate, &provider_map)
    .unwrap_err();
    assert!(ordinal_error
        .to_string()
        .contains("outside the factored provider set"));
    assert_eq!(sorted_unique_i64_nth(&[&[1, 2]], 3), None);

    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let mut first = V4ProviderSetFactorCache::new(true, Arc::clone(&shared));
    let first_entry = first
        .resolve(&provider_map, &rate.provider_refs, None, &rate, &context)
        .unwrap()
        .unwrap();
    let mut second = V4ProviderSetFactorCache::new(true, Arc::clone(&shared));
    let shared_entry = second
        .resolve(&provider_map, &rate.provider_refs, None, &rate, &context)
        .unwrap()
        .unwrap();
    assert!(Arc::ptr_eq(&first_entry, &shared_entry));
    assert_eq!(second.metrics.cache_misses, 1);
    assert_eq!(second.metrics.flat_group_union_attempts, 0);

    let mixed = second
        .resolve(
            &provider_map,
            &rate.provider_refs,
            Some(&inline),
            &rate,
            &context,
        )
        .unwrap()
        .unwrap();
    assert_eq!(mixed.provider_count, 3);
    assert_eq!(second.metrics.mixed_rates, 1);

    let inline_rate = RateLite {
        provider_refs: Vec::new(),
        ..rate.clone()
    };
    assert!(second
        .resolve(&provider_map, &[], Some(&inline), &inline_rate, &context)
        .unwrap()
        .is_some());
    assert_eq!(second.metrics.inline_only_rates, 1);
    assert!(second
        .resolve(&provider_map, &[], None, &inline_rate, &context)
        .unwrap()
        .is_none());

    let unresolved_error = second
        .resolve(
            &provider_map,
            &missing_rate.provider_refs,
            None,
            &missing_rate,
            &context,
        )
        .unwrap_err();
    assert!(unresolved_error
        .to_string()
        .contains("unresolved provider reference"));
}
#[test]
fn v4_factor_shared_cache_configuration_validates_byte_limit() {
    const NAME: &str = "HLTHPRT_PTG2_V4_FACTOR_CACHE_MAX_BYTES";
    let _lock = scanner_env_lock().lock().unwrap();

    {
        let _unset = TestEnvVar::remove(NAME);
        assert_eq!(
            V4ProviderSetFactorSharedCache::configured()
                .unwrap()
                .snapshot()
                .2,
            DEFAULT_V4_PROVIDER_FACTOR_CACHE_MAX_BYTES as u64
        );
    }
    {
        let _configured = TestEnvVar::set(NAME, "4096");
        assert_eq!(
            V4ProviderSetFactorSharedCache::configured()
                .unwrap()
                .snapshot()
                .2,
            4096
        );
    }
    for invalid in ["0", "not-a-byte-count"] {
        let configured = TestEnvVar::set(NAME, invalid);
        let error = V4ProviderSetFactorSharedCache::configured().err().unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error.to_string().contains("positive integer byte count"));
        drop(configured);
    }
}
#[test]
fn v3_factor_mode_omits_v4_semantic_reporter_counters_and_sidecar_instrumentation() {
    const FACTOR_CACHE_NAME: &str = "HLTHPRT_PTG2_V4_FACTOR_CACHE_MAX_BYTES";
    const INLINE_CACHE_NAME: &str = "HLTHPRT_PTG2_V4_INLINE_TRANSFORM_CACHE_MAX_BYTES";
    let _lock = scanner_env_lock().lock().unwrap();
    let _factor_cache = TestEnvVar::remove(FACTOR_CACHE_NAME);
    let invalid_inline_cache = TestEnvVar::set(INLINE_CACHE_NAME, "invalid-for-v4");

    let v3_cache = V4ProviderSetFactorSharedCache::configured_for_mode(false).unwrap();
    assert!(v3_cache.semantic_progress_if_enabled().is_none());

    let compressed_bytes_read = Arc::new(AtomicU64::new(0));
    let reporter = start_semantic_progress_reporter(
        Path::new("synthetic-v3.mrf.json"),
        0,
        &compressed_bytes_read,
        v3_cache.semantic_progress_if_enabled().as_ref(),
        Instant::now(),
    )
    .unwrap();
    assert!(reporter.is_none());

    let v3_finalize_progress =
        ManifestFinalizeProgress::new(v3_cache.semantic_progress_if_enabled());
    assert!(v3_finalize_progress.semantic_progress.is_none());

    drop(invalid_inline_cache);
    let _inline_cache = TestEnvVar::remove(INLINE_CACHE_NAME);
    let v4_cache = V4ProviderSetFactorSharedCache::configured_for_mode(true).unwrap();
    let v4_progress = v4_cache.semantic_progress_if_enabled();
    assert!(v4_progress.is_some());
    let reporter = start_semantic_progress_reporter(
        Path::new("synthetic-v4.mrf.json"),
        0,
        &compressed_bytes_read,
        v4_progress.as_ref(),
        Instant::now(),
    )
    .unwrap();
    assert!(reporter.is_some());
    drop(reporter);

    let v4_finalize_progress = ManifestFinalizeProgress::new(v4_progress);
    assert!(v4_finalize_progress.semantic_progress.is_some());
}
#[test]
fn reference_extreme_inline_cache_is_exact_bounded_and_shared_across_workers() {
    let uncached_single = run_reference_extreme_inline_cache_case(1, 0);
    let uncached_parallel = run_reference_extreme_inline_cache_case(4, 0);
    let cached_single = run_reference_extreme_inline_cache_case(1, 16 * 1024 * 1024);
    let cached_parallel = run_reference_extreme_inline_cache_case(4, 16 * 1024 * 1024);

    for candidate in [&uncached_parallel, &cached_single, &cached_parallel] {
        assert_eq!(candidate.output_sha256, uncached_single.output_sha256);
        assert_eq!(candidate.dedupe, uncached_single.dedupe);
        assert_eq!(candidate.quarantine, uncached_single.quarantine);
        assert_eq!(
            candidate.empty_npi_normalizations,
            uncached_single.empty_npi_normalizations
        );
        assert!(candidate.cache.estimated_bytes <= candidate.cache.max_estimated_bytes);
        assert!(candidate.cache.peak_estimated_bytes <= candidate.cache.max_estimated_bytes);
    }

    // Two raw group attempts per rate, with exactly two unique groups:
    // (12_500 - 2) / 12_500 = 99.984% duplicates.
    assert_eq!(uncached_single.dedupe["provider_group_attempted"], 12_500);
    assert_eq!(uncached_single.dedupe["provider_group_unique"], 2);
    assert_eq!(uncached_single.dedupe["provider_group_duplicate"], 12_498);
    assert_eq!(uncached_single.dedupe["provider_group_member_attempted"], 1);
    assert_eq!(uncached_single.quarantine["occurrence_count"], 12_500);
    assert_eq!(uncached_single.quarantine["distinct_value_count"], 2);
    assert_eq!(uncached_single.empty_npi_normalizations, 6_250);

    assert_eq!(cached_single.cache.transforms, 1);
    assert_eq!(cached_single.cache.misses, 1);
    assert_eq!(cached_single.cache.hits, 6_249);
    assert_eq!(cached_parallel.cache.transforms, 1);
    assert_eq!(cached_parallel.cache.misses, 1);
    assert_eq!(cached_parallel.cache.hits, 6_249);
    assert_eq!(uncached_single.cache.transforms, 6_250);
    assert_eq!(uncached_single.cache.bypasses, 6_250);
}
#[test]
fn v4_production_parse_probes_raw_cache_before_deserialization_and_retains_arc() {
    let raw_provider_groups = r#"[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}]"#;
    let first_rate = parse_v4_inline_rate(raw_provider_groups);
    let second_rate = parse_v4_inline_rate(raw_provider_groups);
    assert!(first_rate.provider_groups.is_empty());
    assert_eq!(
        first_rate.provider_groups_raw.as_deref().map(RawValue::get),
        Some(raw_provider_groups)
    );

    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let mut scope_cache = ProviderSetScopeCache::configured(Arc::clone(&shared), true);
    let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = v4_test_shared_dedupe(1);
    let provider_map = HashMap::new();
    let context = test_compact_context();
    let first = resolve_worker_provider(
        &provider_map,
        &first_rate,
        &mut sinks,
        &dedupe,
        &mut scope_cache,
        &context,
    )
    .unwrap()
    .unwrap();
    let second = resolve_worker_provider(
        &provider_map,
        &second_rate,
        &mut sinks,
        &dedupe,
        &mut scope_cache,
        &context,
    )
    .unwrap()
    .unwrap();
    let (
        ResolvedWorkerProvider::V4Factor {
            inline_transform: Some(first_transform),
            ..
        },
        ResolvedWorkerProvider::V4Factor {
            inline_transform: Some(second_transform),
            ..
        },
    ) = (&first, &second)
    else {
        panic!("V4 inline rates must retain their cached transforms");
    };
    assert!(Arc::ptr_eq(first_transform, second_transform));
    assert_eq!(first.inline_provider_groups().len(), 1);
    assert_eq!(
        first.inline_component(),
        second.inline_component(),
        "cache hits must preserve exact normalized factors"
    );
    let cache = shared.inline_transforms.snapshot();
    assert_eq!(cache.transforms, 1);
    assert_eq!(cache.misses, 1);
    assert_eq!(cache.hits, 1);
    assert!(cache.estimated_bytes >= raw_provider_groups.len() as u64);
    assert!(cache.estimated_bytes <= cache.max_estimated_bytes);
    let audit = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(audit["provider_group_attempted"], 2);
    assert_eq!(audit["provider_group_unique"], 1);
    assert_eq!(audit["provider_group_duplicate"], 1);
}
#[test]
fn v4_raw_spelling_mismatch_misses_but_preserves_semantic_output() {
    let first_rate =
        parse_v4_inline_rate(r#"[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}]"#);
    let second_rate = parse_v4_inline_rate(
        r#"[ { "npi" : [1234567890], "tin" : { "value" : "123456789", "type" : "ein" } } ]"#,
    );
    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let mut scope_cache = ProviderSetScopeCache::configured(Arc::clone(&shared), true);
    let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = v4_test_shared_dedupe(1);
    let provider_map = HashMap::new();
    let context = test_compact_context();
    let first = resolve_worker_provider(
        &provider_map,
        &first_rate,
        &mut sinks,
        &dedupe,
        &mut scope_cache,
        &context,
    )
    .unwrap()
    .unwrap();
    let second = resolve_worker_provider(
        &provider_map,
        &second_rate,
        &mut sinks,
        &dedupe,
        &mut scope_cache,
        &context,
    )
    .unwrap()
    .unwrap();

    assert_eq!(first.inline_component(), second.inline_component());
    assert_eq!(
        first.factor_entry().unwrap().provider_set_global_id,
        second.factor_entry().unwrap().provider_set_global_id
    );
    let cache = shared.inline_transforms.snapshot();
    assert_eq!(cache.hits, 0);
    assert_eq!(cache.misses, 2);
    assert_eq!(cache.transforms, 2);
    let audit = dedupe_summary_payload(&dedupe, &HashMap::new());
    assert_eq!(audit["provider_group_attempted"], 2);
    assert_eq!(audit["provider_group_unique"], 1);
    assert_eq!(audit["provider_group_duplicate"], 1);
}
#[test]
fn v4_inline_cache_full_compares_collision_buckets_and_eviction_only_changes_speed() {
    let groups_a = reference_extreme_inline_provider_groups();
    let mut groups_b = reference_extreme_inline_provider_groups();
    groups_b[0]["tin"]["value"] = json!("111111111");
    let (entry_a, normalization_a, attempts_a) = audited_inline_transform(&groups_a);
    let (entry_b, normalization_b, attempts_b) = audited_inline_transform(&groups_b);
    assert_ne!(entry_a.entry_hash, entry_b.entry_hash);
    let raw_a = serde_json::to_vec(&groups_a).unwrap();
    let raw_b = serde_json::to_vec(&groups_b).unwrap();
    let key_kind = V4InlineProviderTransformCacheKeyKind::ExactRawJson;
    let estimated_a = estimated_v4_inline_provider_transform_bytes(&V4InlineProviderTransform {
        cache_key_kind: key_kind,
        cache_key: Arc::from(raw_a.as_slice()),
        provider_groups: groups_a.clone().into(),
        entry: entry_a.clone(),
        empty_npi_tin_only_normalization_count: normalization_a,
        provider_group_attempts: attempts_a,
    });
    let estimated_b = estimated_v4_inline_provider_transform_bytes(&V4InlineProviderTransform {
        cache_key_kind: key_kind,
        cache_key: Arc::from(raw_b.as_slice()),
        provider_groups: groups_b.clone().into(),
        entry: entry_b.clone(),
        empty_npi_tin_only_normalization_count: normalization_b,
        provider_group_attempts: attempts_b,
    });

    let collision_cache = V4InlineProviderTransformSharedCache::new_with_shards(1024 * 1024, 1);
    let (first_a, hit) = collision_cache
        .resolve_or_insert(7, key_kind, &raw_a, || {
            Ok((
                groups_a.clone(),
                entry_a.clone(),
                normalization_a,
                attempts_a,
            ))
        })
        .unwrap();
    assert!(!hit);
    let (first_b, hit) = collision_cache
        .resolve_or_insert(7, key_kind, &raw_b, || {
            Ok((
                groups_b.clone(),
                entry_b.clone(),
                normalization_b,
                attempts_b,
            ))
        })
        .unwrap();
    assert!(!hit);
    let (again_a, hit) = collision_cache
        .resolve_or_insert(7, key_kind, &raw_a, || {
            panic!("full value comparison must find the first collision entry")
        })
        .unwrap();
    assert!(hit);
    let (again_b, hit) = collision_cache
        .resolve_or_insert(7, key_kind, &raw_b, || {
            panic!("full value comparison must find the second collision entry")
        })
        .unwrap();
    assert!(hit);
    assert_eq!(first_a.entry, again_a.entry);
    assert_eq!(first_b.entry, again_b.entry);
    assert_ne!(again_a.entry.entry_hash, again_b.entry.entry_hash);
    assert_eq!(collision_cache.snapshot().transforms, 2);

    let one_entry_limit = estimated_a.max(estimated_b);
    let eviction_cache = V4InlineProviderTransformSharedCache::new_with_shards(one_entry_limit, 1);
    let (eviction_a, _) = eviction_cache
        .resolve_or_insert(9, key_kind, &raw_a, || {
            Ok((
                groups_a.clone(),
                entry_a.clone(),
                normalization_a,
                attempts_a,
            ))
        })
        .unwrap();
    let (eviction_b, _) = eviction_cache
        .resolve_or_insert(9, key_kind, &raw_b, || {
            Ok((
                groups_b.clone(),
                entry_b.clone(),
                normalization_b,
                attempts_b,
            ))
        })
        .unwrap();
    let (eviction_a_again, hit) = eviction_cache
        .resolve_or_insert(9, key_kind, &raw_a, || {
            Ok((
                groups_a.clone(),
                entry_a.clone(),
                normalization_a,
                attempts_a,
            ))
        })
        .unwrap();
    assert!(!hit);
    assert_eq!(eviction_a.entry, eviction_a_again.entry);
    assert_eq!(eviction_b.entry, entry_b);
    let snapshot = eviction_cache.snapshot();
    assert!(snapshot.evictions >= 2);
    assert!(snapshot.evicted_entries >= 2);
    assert_eq!(snapshot.entries, 1);
    assert!(snapshot.estimated_bytes <= snapshot.max_estimated_bytes);
    assert!(snapshot.peak_estimated_bytes <= snapshot.max_estimated_bytes);
}
#[test]
fn v4_inline_transform_cache_configuration_accepts_zero_and_rejects_invalid_values() {
    const NAME: &str = "HLTHPRT_PTG2_V4_INLINE_TRANSFORM_CACHE_MAX_BYTES";
    let _lock = scanner_env_lock().lock().unwrap();

    {
        let _unset = TestEnvVar::remove(NAME);
        assert_eq!(
            V4InlineProviderTransformSharedCache::configured()
                .unwrap()
                .snapshot()
                .max_estimated_bytes,
            DEFAULT_V4_INLINE_PROVIDER_TRANSFORM_CACHE_MAX_BYTES as u64
        );
    }
    {
        let _disabled = TestEnvVar::set(NAME, "0");
        assert_eq!(
            V4InlineProviderTransformSharedCache::configured()
                .unwrap()
                .snapshot()
                .max_estimated_bytes,
            0
        );
    }
    {
        let _invalid = TestEnvVar::set(NAME, "not-a-byte-count");
        let error = V4InlineProviderTransformSharedCache::configured()
            .err()
            .unwrap();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(error
            .to_string()
            .contains("non-negative integer byte count"));
        assert_eq!(
            V4ProviderSetFactorSharedCache::configured_for_mode(false)
                .unwrap()
                .inline_transforms
                .snapshot()
                .max_estimated_bytes,
            0
        );
    }
}
#[test]
fn legacy_inline_resolution_does_not_touch_v4_transform_cache() {
    let raw_rate = br#"{"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}],"negotiated_prices":[{"negotiated_rate":1}]}"#;
    let (rate, typed) = read_rate_lite_bytes_profiled_with_policy(raw_rate, false).unwrap();
    let rate = rate.expect("legacy inline rate");
    assert!(typed);
    assert!(rate.provider_groups_raw.is_none());
    assert_eq!(rate.provider_groups.len(), 1);
    let expected = audited_inline_transform(&rate.provider_groups).0;
    let shared = Arc::new(V4ProviderSetFactorSharedCache::new(1024 * 1024));
    let mut scope_cache = ProviderSetScopeCache::configured(Arc::clone(&shared), false);
    let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
    let dedupe = SharedDedupe::new(1);
    let provider_map = HashMap::new();
    let resolved = resolve_worker_provider(
        &provider_map,
        &rate,
        &mut sinks,
        &dedupe,
        &mut scope_cache,
        &test_compact_context(),
    )
    .unwrap()
    .unwrap();

    assert!(!resolved.is_v4_factor());
    assert_eq!(
        resolved.legacy_entry().unwrap().entry_hash(),
        expected.entry_hash
    );
    assert_eq!(
        shared.inline_transforms.snapshot(),
        V4InlineProviderTransformCacheSnapshot {
            max_estimated_bytes: DEFAULT_V4_INLINE_PROVIDER_TRANSFORM_CACHE_MAX_BYTES as u64,
            ..V4InlineProviderTransformCacheSnapshot::default()
        }
    );
}
#[test]
fn tin_only_provider_group_has_no_npi_members() {
    let entry = build_provider_entry(&json!({
        "provider_groups": [{
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [0]
        }]
    }))
    .unwrap();

    assert_eq!(entry.provider_count, 0);
    assert!(entry.npi.is_empty());
    assert_eq!(entry.provider_group_hashes.len(), 1);
}
#[test]
fn strict_price_parser_rejects_non_numeric_negotiated_rate_types() {
    for invalid in [r#""12.5""#, "true", "{}", "[]"] {
        let raw = format!(r#"{{"negotiated_rate":{invalid}}}"#);
        let mut reader = JsonStreamReader::new(raw.as_bytes());
        let error = read_price_lite_struson(&mut reader).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{invalid}");
    }

    for omitted_or_null in ["{}", r#"{"negotiated_rate":null}"#] {
        let mut reader = JsonStreamReader::new(omitted_or_null.as_bytes());
        let error = read_price_lite_struson(&mut reader).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
#[test]
fn typed_rate_parser_matches_strict_streaming_parser() {
    let fixtures = [
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":" negotiated ","negotiated_rate":1.2500e2,"expiration_date":" 2026-12-31 ","service_code":[" 11 ","11"],"billing_class":" professional ","setting":null,"billing_code_modifier":[" 26 ","26"],"additional_information":" note "}],"network_name":" alpha "}"#.as_slice(),
        br#"{"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}],"negotiated_prices":[{"negotiated_rate":0.000001}],"network_names":["beta"," beta "]}"#.as_slice(),
        br#"{"ignored":{"nested":[1,2,3]},"provider_references":[9007199254740993],"negotiated_prices":[{"negotiated_rate":10,"service_code":[],"billing_code_modifier":[]}]}"#.as_slice(),
        br#"{"provider_references":[1e2],"negotiated_prices":[{"negotiated_rate":-0.0}],"network_name":null,"network_names":null}"#.as_slice(),
    ];

    for raw in fixtures {
        assert_eq!(
            read_rate_lite_bytes_typed(raw, false).unwrap(),
            read_rate_lite_bytes_streaming(raw, false).unwrap()
        );
    }
}
#[test]
fn expiration_date_requires_an_exact_gregorian_calendar_date() {
    for (raw, expected) in [
        (None, None),
        (Some("   "), None),
        (Some(" 2028-02-29 "), Some("2028-02-29")),
        (Some("1900-02-28"), Some("1900-02-28")),
        (Some("9999-12-31"), Some("9999-12-31")),
    ] {
        assert_eq!(
            strict_optional_iso_calendar_date(raw.map(str::to_owned)).unwrap(),
            expected.map(str::to_owned),
        );
    }
    for invalid in [
        "0000-01-01",
        "1900-02-29",
        "2027-02-29",
        "2027-02-30",
        "2027-00-01",
        "2027-13-01",
        "2027-01-00",
        "2027-1-01",
        "+001-01-01",
        "2027-+1-01",
        "2027-01-+1",
        "2027-01-01T00:00:00Z",
    ] {
        let error = strict_optional_iso_calendar_date(Some(invalid.to_owned())).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{invalid}");
    }

    let valid = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.5,"expiration_date":"2028-02-29"}]}"#;
    let invalid = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.5,"expiration_date":"2027-02-30"},{"negotiated_rate":13.5,"expiration_date":"2028-02-29"}]}"#;
    for allow_empty_npi_tin_only in [false, true] {
        let (parsed, typed) =
            read_rate_lite_bytes_profiled_with_policy(valid, allow_empty_npi_tin_only).unwrap();
        assert!(typed);
        assert_eq!(
            parsed.unwrap().prepared_price_set.unwrap().atoms[0]
                .expiration_date
                .as_deref(),
            Some("2028-02-29"),
        );
        for error in [
            read_rate_lite_bytes_typed(invalid, allow_empty_npi_tin_only).unwrap_err(),
            read_rate_lite_bytes_streaming(invalid, allow_empty_npi_tin_only).unwrap_err(),
            read_rate_lite_bytes_profiled_with_policy(invalid, allow_empty_npi_tin_only)
                .unwrap_err(),
        ] {
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains("exact ISO calendar date"));
        }
    }
}
#[test]
fn exact_invalid_price_exclusion_preserves_original_price_ordinals() {
    let coordinate = SourceWitnessCoordinate::new(4, 2);
    let observation = InvalidPriceExclusionObservation {
        coordinate,
        price_ordinal: 1,
        invalid_value_sha256: invalid_price_value_sha256("2027-02-30"),
    };
    let expected = vec![observation];
    let policy = InvalidPriceExclusionPolicy {
        sha256: invalid_price_exclusion_source_sha256(&expected),
        expected,
        emptied_rate_count: 0,
    };
    let raw = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":10,"expiration_date":"2028-02-29"},{"negotiated_rate":11,"expiration_date":"2027-02-30"},{"negotiated_rate":12,"expiration_date":"2029-03-01"}]}"#;

    for allow_empty_npi_tin_only in [false, true] {
        for parsed in [
            read_rate_lite_bytes_typed_with_exclusion(
                raw,
                allow_empty_npi_tin_only,
                coordinate,
                Some(&policy),
            )
            .unwrap(),
            read_rate_lite_bytes_streaming_with_exclusion(
                raw,
                allow_empty_npi_tin_only,
                coordinate,
                Some(&policy),
            )
            .unwrap(),
        ] {
            assert_eq!(parsed.exclusions, vec![observation]);
            assert!(!parsed.emptied_rate);
            assert_eq!(
                parsed
                    .rate
                    .prepared_price_set
                    .unwrap()
                    .atoms
                    .iter()
                    .map(|atom| atom.source_ordinal)
                    .collect::<Vec<_>>(),
                vec![0, 2],
            );
        }
    }
    assert_eq!(
        policy.validate_observed(vec![observation], 0).unwrap()["excluded_price_count"],
        1,
    );
    assert!(read_rate_lite_bytes_profiled_with_exclusion(
        raw,
        false,
        SourceWitnessCoordinate::new(4, 3),
        Some(&policy),
    )
    .is_err());
}
#[test]
fn all_invalid_price_exclusion_is_exact_across_parser_paths() {
    let coordinate = SourceWitnessCoordinate::new(4, 2);
    let expected = vec![
        InvalidPriceExclusionObservation {
            coordinate,
            price_ordinal: 0,
            invalid_value_sha256: invalid_price_value_sha256("2027-02-30"),
        },
        InvalidPriceExclusionObservation {
            coordinate,
            price_ordinal: 1,
            invalid_value_sha256: invalid_price_value_sha256("2027-02-31"),
        },
    ];
    let policy = InvalidPriceExclusionPolicy {
        sha256: invalid_price_exclusion_source_sha256(&expected),
        expected: expected.clone(),
        emptied_rate_count: 1,
    };
    let raw = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":10,"expiration_date":"2027-02-30"},{"negotiated_rate":11,"expiration_date":"2027-02-31"}]}"#;
    let fallback_raw = br#"{"provider_references":[7],"network_name":"one","network_name":"two","negotiated_prices":[{"negotiated_rate":10,"expiration_date":"2027-02-30"},{"negotiated_rate":11,"expiration_date":"2027-02-31"}]}"#;

    for parsed in [
        read_rate_lite_bytes_typed_with_exclusion(raw, false, coordinate, Some(&policy)).unwrap(),
        read_rate_lite_bytes_typed_with_exclusion(raw, true, coordinate, Some(&policy)).unwrap(),
        read_rate_lite_bytes_streaming_with_exclusion(raw, false, coordinate, Some(&policy))
            .unwrap(),
        read_rate_lite_bytes_streaming_with_exclusion(raw, true, coordinate, Some(&policy))
            .unwrap(),
    ] {
        assert_eq!(parsed.exclusions, expected);
        assert!(parsed.emptied_rate);
        assert!(parsed.rate.prepared_price_set.is_none());
    }
    let (fallback, typed) = read_rate_lite_bytes_profiled_with_exclusion(
        fallback_raw,
        false,
        coordinate,
        Some(&policy),
    )
    .unwrap();
    assert!(!typed);
    assert_eq!(fallback.exclusions, expected);
    assert!(fallback.emptied_rate);
    assert!(fallback.rate.prepared_price_set.is_none());
    assert_eq!(
        policy.validate_observed(expected, 1).unwrap()["emptied_rate_count"],
        1,
    );
}
#[test]
fn v4_typed_rate_parser_retains_raw_inline_groups_without_deserializing_them() {
    let raw_provider_groups =
        br#"[ { "tin": { "type": "ein", "value": "123456789" }, "npi": [1234567890] } ]"#;
    let raw = br#"{"provider_groups":[ { "tin": { "type": "ein", "value": "123456789" }, "npi": [1234567890] } ],"negotiated_prices":[{"negotiated_rate":12.50}]}"#;

    let rate = read_rate_lite_bytes_typed(raw, true)
        .unwrap()
        .expect("typed inline rate");

    assert!(rate.provider_groups.is_empty());
    assert_eq!(
        rate.provider_groups_raw
            .as_deref()
            .map(RawValue::get)
            .map(str::as_bytes),
        Some(raw_provider_groups.as_slice())
    );
}
#[test]
fn profiled_rate_parser_preserves_streaming_parser_contract() {
    let fixtures = [
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_references":[7],"provider_groups":[{"npi":[1234567890],"tin":{"type":"ein","value":"123456789"}}],"negotiated_prices":[{"negotiated_rate":12.50}],"network_name":"one","network_names":["two"]}"#.as_slice(),
        br#"{"provider_references":[1e2],"negotiated_prices":[{"negotiated_rate":1.2e10,"negotiated_type":" ffs ","service_code":[" 11 ","22"],"billing_code_modifier":[" 26 "]}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.50,"additional_information":"escaped \\ value \" text","billing_class":"\u00e9"}]}"#.as_slice(),
        br#"{"provider_references":[7],"provider_references":[8],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.50,"negotiated_rate":13.50}]}"#.as_slice(),
        br#"{"provider_references":[],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[]}"#.as_slice(),
        br#"{"provider_references":["7"],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_references":[7.5],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":"12.50"}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":null}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.50,"service_code":null}]}"#.as_slice(),
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":12.50}],"network_name":12}"#.as_slice(),
        br#"{"provider_groups":[{"npi":[0]}],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
        br#"{"provider_groups":[{"npi":["1234567890"]}],"negotiated_prices":[{"negotiated_rate":12.50}]}"#.as_slice(),
    ];

    for raw in fixtures {
        let profiled = read_rate_lite_bytes_profiled(raw);
        let streaming = read_rate_lite_bytes_streaming(raw, false);
        match (profiled, streaming) {
            (Ok((profiled, _)), Ok(streaming)) => assert_eq!(profiled, streaming),
            (Err(profiled), Err(streaming)) => {
                assert_eq!(profiled.kind(), streaming.kind());
                assert_eq!(profiled.to_string(), streaming.to_string());
            }
            (profiled, streaming) => {
                panic!("parser contract diverged: profiled={profiled:?} streaming={streaming:?}")
            }
        }
    }
}
#[test]
#[ignore = "manual release parser throughput probe"]
fn benchmark_typed_rate_parser_against_streaming_parser() {
    let raw = std::hint::black_box(
        br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":125.25,"expiration_date":"2026-12-31","service_code":["11"],"billing_class":"professional","billing_code_modifier":["26"]},{"negotiated_type":"negotiated","negotiated_rate":225.50,"expiration_date":"2026-12-31","service_code":["22"],"billing_class":"professional","billing_code_modifier":[]}],"network_name":"network"}"#,
    );
    const ITERATIONS: usize = 1_000_000;

    let started_at = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(read_rate_lite_bytes_streaming(raw, false).unwrap());
    }
    let streaming_seconds = started_at.elapsed().as_secs_f64();

    let started_at = Instant::now();
    for _ in 0..ITERATIONS {
        std::hint::black_box(read_rate_lite_bytes_typed(raw, false).unwrap());
    }
    let typed_seconds = started_at.elapsed().as_secs_f64();

    eprintln!(
        "parser_benchmark iterations={ITERATIONS} streaming_seconds={streaming_seconds:.6} typed_seconds={typed_seconds:.6} speedup={:.3}",
        streaming_seconds / typed_seconds
    );
}
#[test]
fn strict_price_parser_requires_nullable_fields_to_be_strings_or_null() {
    for field_name in [
        "negotiated_type",
        "expiration_date",
        "billing_class",
        "setting",
        "additional_information",
    ] {
        for invalid in ["12", "true", "{}", "[]"] {
            let raw = price_json(field_name, invalid);
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            let error = read_price_lite_struson(&mut reader).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        }
        for valid in ["null", r#"" value ""#] {
            let raw = price_json(field_name, valid);
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            assert!(read_price_lite_struson(&mut reader).unwrap().is_some());
        }
    }
}
#[test]
fn strict_v3_procedure_requires_string_code_fields() {
    let valid = Map::from_iter([
        ("billing_code_type".to_owned(), json!(" CPT ")),
        ("billing_code".to_owned(), json!(" 99213 ")),
        ("negotiation_arrangement".to_owned(), Value::Null),
    ]);
    validate_procedure_for_rate_dispatch(&valid).unwrap();
    assert!(procedure_has_queryable_code(&valid));

    for field_name in ["billing_code_type", "billing_code"] {
        for invalid in [Value::Null, json!(12), json!(true)] {
            let mut procedure = valid.clone();
            procedure.insert(field_name.to_owned(), invalid);
            assert!(validate_procedure_for_rate_dispatch(&procedure).is_err());
        }
        let mut unqueryable = valid.clone();
        unqueryable.insert(field_name.to_owned(), json!(" "));
        validate_procedure_for_rate_dispatch(&unqueryable).unwrap();
        assert!(!procedure_has_queryable_code(&unqueryable));
        let mut missing = valid.clone();
        missing.remove(field_name);
        assert!(validate_procedure_for_rate_dispatch(&missing).is_err());
    }
    for invalid in [json!(12), json!(true), json!({}), json!([])] {
        let mut procedure = valid.clone();
        procedure.insert("negotiation_arrangement".to_owned(), invalid);
        assert!(validate_procedure_for_rate_dispatch(&procedure).is_err());
    }
    for field_name in ["billing_code_type_version", "name", "description"] {
        for invalid in [json!(12), json!(true), json!({}), json!([])] {
            let mut procedure = valid.clone();
            procedure.insert(field_name.to_owned(), invalid);
            assert!(validate_procedure_for_rate_dispatch(&procedure).is_err());
        }
        for valid_value in [Value::Null, json!(" value ")] {
            let mut procedure = valid.clone();
            procedure.insert(field_name.to_owned(), valid_value);
            validate_procedure_for_rate_dispatch(&procedure).unwrap();
        }
    }
}
#[test]
fn producer_failure_capture_cancels_workers_and_preserves_source_error() {
    let cancelled = AtomicBool::new(false);
    let source_error = io::Error::new(
        io::ErrorKind::InvalidData,
        "billing_code must be a non-empty JSON string",
    );

    let captured = capture_producer_error(&cancelled, Err(source_error)).unwrap();

    assert!(cancelled.load(Ordering::Acquire));
    assert_eq!(captured.kind(), io::ErrorKind::InvalidData);
    assert_eq!(
        captured.to_string(),
        "billing_code must be a non-empty JSON string"
    );

    let not_cancelled = AtomicBool::new(false);
    assert!(capture_producer_error(&not_cancelled, Ok(())).is_none());
    assert!(!not_cancelled.load(Ordering::Acquire));
}
#[test]
fn primary_producer_diagnostic_excludes_peer_cancellation() {
    let source_error = io::Error::new(
        io::ErrorKind::InvalidData,
        "billing_code must be a non-empty JSON string",
    );
    let diagnostic = primary_producer_failure_diagnostic(&source_error).unwrap();
    assert!(diagnostic.starts_with("PTG2_SCANNER_PRIMARY_FAILED\t"));
    assert!(diagnostic.contains("billing_code must be a non-empty JSON string"));

    for peer_kind in [io::ErrorKind::Interrupted, io::ErrorKind::BrokenPipe] {
        let peer_error = io::Error::new(peer_kind, "peer worker stopped");
        assert!(primary_producer_failure_diagnostic(&peer_error).is_none());
    }
}
#[test]
fn scanner_failure_and_hash_helpers_cover_terminal_edges() {
    let payload: &(dyn Any + Send + 'static) = &"worker panic";
    assert_eq!(panic_payload_message(payload), "worker panic");
    let owned_payload = "owned worker panic".to_string();
    assert_eq!(panic_payload_message(&owned_payload), "owned worker panic");
    assert_eq!(panic_payload_message(&7_u32), "non-string panic payload");
    assert_eq!(compressed_mib_per_second(1024, 0.0), 0.0);
    log_worker_failure(0, "test", "expected diagnostic");

    let codes = vec!["11".to_string(), "22".to_string()];
    assert_eq!(
        price_code_set_hash(&codes),
        hash_string_list("price_code_set", &codes)
    );

    let missing_input =
        std::env::temp_dir().join(format!("ptg2-scanner-missing-input-{}", std::process::id()));
    assert!(scan(&missing_input, &[]).is_err());
}
#[test]
fn manifest_global_id_cache_reuses_canonical_provider_set_identity() {
    let groups = [7_i64, 3_i64];
    let networks = ["Network Alpha".to_string(), "Network Beta".to_string()];
    let expected = provider_set_global_id_from_group_hashes_and_network_names(&groups, &networks);
    let mut cache = ManifestGlobalIdCache::default();
    assert_eq!(
        cache.provider_set_id("provider-set", &groups, &networks),
        expected
    );
    assert_eq!(cache.provider_set_id("provider-set", &[], &[]), expected);
}
#[test]
fn source_provenance_bitpacking_matches_width_boundaries() {
    let cases = [
        (1, vec![0], Vec::new()),
        (2, vec![0, 1], vec![0b0000_0010]),
        (256, vec![0, 255], vec![0, 255]),
        (257, vec![0, 256], vec![0, 0, 0b0000_0010]),
    ];
    for (source_count, keys, expected) in cases {
        let bits = source_key_bits(source_count).unwrap();
        assert_eq!(
            tagged_serving_run_record_bytes(source_count).unwrap(),
            SERVING_RUN_RECORD_BYTES + usize::from(source_key_bytes(source_count).unwrap()),
        );
        assert_eq!(
            encode_source_key_vector(&keys, source_count, bits).unwrap(),
            expected
        );
    }
    assert!(encode_source_key_vector(&[2], 2, 1).is_err());
    assert!(encode_source_key_vector(&[0], 2, 8).is_err());
}
#[test]
fn strict_price_parser_requires_string_arrays_for_tic_code_lists() {
    for field_name in ["service_code", "billing_code_modifier"] {
        for invalid in [r#""11""#, "true", "12", "{}", "null"] {
            let raw = price_json(field_name, invalid);
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            let error = read_price_lite_struson(&mut reader).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        }
        for invalid_element in ["true", "12", "{}", "[]", "null"] {
            let raw = price_json(field_name, &format!(r#"["11",{invalid_element}]"#));
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            let error = read_price_lite_struson(&mut reader).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        }
    }

    let raw = r#"{"negotiated_rate":12.5,"service_code":[" 22 ","11"],"billing_code_modifier":["tc, 26","26"]}"#;
    let mut reader = JsonStreamReader::new(raw.as_bytes());
    let price = read_price_lite_struson(&mut reader).unwrap().unwrap();
    assert_eq!(price.service_code, vec!["11", "22"]);
    assert_eq!(price.billing_code_modifier, vec!["26", "TC"]);
}
#[test]
fn strict_provider_definition_rejects_invalid_id_and_npi_types() {
    for invalid_id in [
        json!("7"),
        json!(true),
        json!({}),
        json!([]),
        Value::Null,
        json!(7.5),
    ] {
        let mut provider_ref = valid_provider_reference();
        provider_ref["provider_group_id"] = invalid_id;
        let error = provider_ref_definition(&provider_ref).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    for invalid_npi in [
        json!(1234567890_i64),
        json!([true]),
        json!([{}]),
        json!([[]]),
        json!([null]),
        json!([1234567890.5]),
    ] {
        let mut provider_ref = valid_provider_reference();
        provider_ref["provider_groups"][0]["npi"] = invalid_npi;
        let error = provider_ref_definition(&provider_ref).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
#[test]
fn provider_definition_accepts_exact_string_npis() {
    let numeric = valid_provider_reference();
    let mut string = numeric.clone();
    string["provider_groups"][0]["npi"] = json!(["1234567890"]);

    assert_eq!(
        provider_ref_definition(&string).unwrap(),
        provider_ref_definition(&numeric).unwrap(),
    );
    assert_eq!(
        npi_source_coordinate(string["provider_groups"].as_array().unwrap(), 1234567890,),
        Some((0, 0)),
    );

    let mut string_zero = numeric.clone();
    string_zero["provider_groups"][0]["npi"] = json!(["0"]);
    let mut numeric_zero = numeric;
    numeric_zero["provider_groups"][0]["npi"] = json!([0]);
    assert_eq!(
        provider_ref_definition(&string_zero).unwrap(),
        provider_ref_definition(&numeric_zero).unwrap(),
    );
}
#[test]
fn v3_mode_rejects_empty_npi_array_while_v4_normalizes_it() {
    let mut empty_npi = valid_provider_reference();
    empty_npi["provider_groups"][0]["npi"] = json!([]);
    let mut zero_marker = valid_provider_reference();
    zero_marker["provider_groups"][0]["npi"] = json!([0]);

    assert!(build_provider_entry(&empty_npi).is_err());
    let (empty_entry, empty_normalizations) =
        build_provider_entry_audited(&empty_npi, true).unwrap();
    let (zero_entry, zero_normalizations) =
        build_provider_entry_audited(&zero_marker, true).unwrap();

    assert_eq!(empty_normalizations, 1);
    assert_eq!(zero_normalizations, 0);
    assert_eq!(empty_entry.entry_hash, zero_entry.entry_hash);
    assert_eq!(
        empty_entry.provider_group_hashes,
        zero_entry.provider_group_hashes
    );
    assert_eq!(empty_entry.provider_count, 0);
    assert!(empty_entry.npi.is_empty());
    assert!(empty_entry.quarantined_npi.is_empty());
    assert_eq!(
        empty_npi_tin_only_normalization_payload(empty_normalizations),
        json!({
            "contract": "ptg2_v4_empty_npi_tin_only_normalization_v1",
            "source_shape": "empty_array",
            "canonical_equivalent": "zero_marker",
            "occurrence_count": 1,
            "emitted_npi_edge_count": 0,
            "sha256": "db13782e1535049353cacb9fd1a2f6943a7c461cc09596d23e631741728a0216",
        }),
    );
    let v3_summary = json!({"factor_mode": false});
    assert_eq!(
        scanner_summary_with_v4_empty_npi_audit(v3_summary.clone(), false, 1),
        v3_summary,
    );
    assert_eq!(
        scanner_summary_with_v4_empty_npi_audit(json!({"factor_mode": true}), true, 1)
            ["empty_npi_tin_only_normalization"]["occurrence_count"],
        1,
    );
}
#[test]
fn malformed_integer_npi_is_quarantined_without_losing_valid_membership() {
    let mut mixed = valid_provider_reference();
    mixed["provider_groups"][0]["npi"] = json!([1234567890_i64, 123456789_i64, 123456789_i64]);
    let mixed_entry = build_provider_entry(&mixed).unwrap();
    assert_eq!(mixed_entry.provider_count, 1);
    assert_eq!(mixed_entry.npi, vec![1234567890]);
    assert_eq!(mixed_entry.quarantined_npi, vec![123456789, 123456789]);

    let valid_entry = build_provider_entry(&valid_provider_reference()).unwrap();
    assert_ne!(mixed_entry.entry_hash, valid_entry.entry_hash);

    let provider_map = HashMap::from([(ProviderRefKey::from("7"), mixed_entry)]);
    let payload = provider_identifier_quarantine_payload(
        &provider_map,
        ProviderIdentifierQuarantine::default(),
        &ProviderGroupDefinitionConflicts::default(),
        &[0x11; 32],
    )
    .unwrap();
    assert_eq!(payload["occurrence_count"], 2);
    assert_eq!(payload["distinct_value_count"], 1);
    assert_eq!(payload["entries"][0]["value"], "123456789");
    assert_eq!(payload["entries"][0]["occurrence_count"], 2);
    assert_eq!(
        payload["sha256"],
        "6b01033baec61d1e9d4738f0f12cf2f48cefbd6a801fd0bd4a9b76d1b570624b"
    );
}
#[test]
fn malformed_string_npi_is_quarantined_without_losing_valid_membership() {
    let malformed = "1447744750`";
    let mut mixed = valid_provider_reference();
    mixed["provider_groups"][0]["npi"] =
        json!([1234567890_i64, 123456789_i64, malformed, malformed]);
    let mixed_entry = build_provider_entry(&mixed).unwrap();
    assert_eq!(mixed_entry.provider_count, 1);
    assert_eq!(mixed_entry.npi, vec![1234567890]);
    assert_eq!(mixed_entry.quarantined_npi, vec![123456789]);
    assert_eq!(
        mixed_entry.quarantined_npi_text,
        vec![malformed.to_string(), malformed.to_string()]
    );

    let valid_entry = build_provider_entry(&valid_provider_reference()).unwrap();
    assert_ne!(mixed_entry.entry_hash, valid_entry.entry_hash);

    let provider_map = HashMap::from([(ProviderRefKey::from("7"), mixed_entry)]);
    let payload = provider_identifier_quarantine_payload(
        &provider_map,
        ProviderIdentifierQuarantine::default(),
        &ProviderGroupDefinitionConflicts::default(),
        &[0x11; 32],
    )
    .unwrap();
    assert_eq!(
        payload["contract"],
        "ptg2_provider_identifier_quarantine_v2"
    );
    assert_eq!(payload["occurrence_count"], 3);
    assert_eq!(payload["distinct_value_count"], 2);
    assert_eq!(payload["provider_group_conflict_count"], 0);
    assert_eq!(payload["provider_group_conflicting_definition_count"], 0);
    assert_eq!(payload["provider_group_definition_conflicts"], json!([]));
    assert_eq!(payload["entries"][0]["kind"], "integer");
    assert_eq!(payload["entries"][0]["value"], "123456789");
    assert_eq!(payload["entries"][1]["kind"], "string");
    assert_eq!(payload["entries"][1]["byte_length"], 11);
    assert_eq!(payload["entries"][1]["occurrence_count"], 2);
    assert_eq!(
        payload["entries"][1]["value_sha256"],
        "27e0d2def7d3bfb8c0538e8af4def83d193d1a59bcdf96c2d1e5ea67e7c766a3"
    );

    let mut oversized_entry = provider_map.values().next().unwrap().clone();
    oversized_entry.quarantined_npi_text = vec!["x".repeat(129)];
    assert!(provider_identifier_quarantine_payload(
        &HashMap::from([(ProviderRefKey::from("oversized"), oversized_entry)]),
        ProviderIdentifierQuarantine::default(),
        &ProviderGroupDefinitionConflicts::default(),
        &[0x11; 32],
    )
    .is_err());
}
