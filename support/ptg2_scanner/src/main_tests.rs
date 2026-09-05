mod tests {
    use super::*;
    use ptg2_scanner::manifest::GLOBAL_ID_BYTES;
    use std::collections::BTreeSet;
    use std::sync::OnceLock;

    struct TestEnvVar {
        name: &'static str,
        previous: Option<String>,
    }

    impl TestEnvVar {
        fn set(name: &'static str, value: &str) -> Self {
            let previous = std::env::var(name).ok();
            std::env::set_var(name, value);
            Self { name, previous }
        }

        fn remove(name: &'static str) -> Self {
            let previous = std::env::var(name).ok();
            std::env::remove_var(name);
            Self { name, previous }
        }
    }

    impl Drop for TestEnvVar {
        fn drop(&mut self) {
            match self.previous.as_deref() {
                Some(value) => std::env::set_var(self.name, value),
                None => std::env::remove_var(self.name),
            }
        }
    }

    fn scanner_env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[derive(Clone)]
    struct V4FinalizerPackTestRow {
        object_kind: &'static str,
        block_key: i64,
        fragment_no: i32,
        entry_count: i64,
        payload: Vec<u8>,
        block_hash: Option<[u8; 32]>,
    }

    type V4FinalizerPackTestInput = (PathBuf, u64, String, u64, u64, Vec<[u8; 32]>);
    type V4FinalizerPackSuccessFixture = (
        Vec<V4FinalizerPackTestRow>,
        Vec<V4FinalizerPackTestRow>,
        V4FinalizerPackTestInput,
        V4FinalizerPackTestInput,
    );

    fn v4_finalizer_pack_test_fields(row: &V4FinalizerPackTestRow) -> Vec<Option<Vec<u8>>> {
        let block_hash = row.block_hash.unwrap_or_else(|| {
            shared_v3_block_hash(
                PTG2_V3_SHARED_BLOCK_FORMAT_VERSION,
                row.object_kind,
                "none",
                &row.payload,
            )
            .unwrap()
        });
        vec![
            Some(block_hash.to_vec()),
            Some(PTG2_V3_SHARED_BLOCK_FORMAT_VERSION.to_be_bytes().to_vec()),
            Some(row.object_kind.as_bytes().to_vec()),
            Some(row.block_key.to_be_bytes().to_vec()),
            Some(row.fragment_no.to_be_bytes().to_vec()),
            Some(row.entry_count.to_be_bytes().to_vec()),
            Some(b"none".to_vec()),
            Some((row.payload.len() as i64).to_be_bytes().to_vec()),
            Some((row.payload.len() as i64).to_be_bytes().to_vec()),
            Some(row.payload.clone()),
        ]
    }

    fn v4_finalizer_pack_test_copy(
        directory: &Path,
        name: &str,
        rows: &[V4FinalizerPackTestRow],
    ) -> V4FinalizerPackTestInput {
        let mut hashes = Vec::with_capacity(rows.len());
        let copy_rows = rows
            .iter()
            .map(|row| {
                let fields = v4_finalizer_pack_test_fields(row);
                hashes.push(fields[0].as_deref().unwrap().try_into().unwrap());
                fields
            })
            .collect::<Vec<_>>();
        let payload = pg_binary_copy_rows(&copy_rows);
        let path = directory.join(name);
        std::fs::write(&path, &payload).unwrap();
        let digest = Sha256::digest(&payload);
        (
            path,
            payload.len() as u64,
            sha256_hex(&digest),
            rows.len() as u64,
            rows.iter().map(|row| row.payload.len() as u64).sum(),
            hashes,
        )
    }

    fn v4_finalizer_pack_test_manifest_with_size(
        directory: &Path,
        price: &V4FinalizerPackTestInput,
        serving: &V4FinalizerPackTestInput,
        coordinates_per_pack: usize,
    ) -> PathBuf {
        let path = directory.join("pack-manifest.json");
        std::fs::write(
            &path,
            serde_json::to_vec(&json!({
                "contract": V4_FINALIZER_PACK_INPUT_CONTRACT,
                "coordinates_per_pack": coordinates_per_pack,
                "lanes": [
                    {
                        "name": "price_dictionary",
                        "path": price.0,
                        "byte_count": price.1,
                        "sha256": price.2,
                        "row_count": price.3,
                        "stored_payload_bytes": price.4,
                        "object_kinds": V4_FINALIZER_PRICE_DICTIONARY_KINDS,
                    },
                    {
                        "name": "serving",
                        "path": serving.0,
                        "byte_count": serving.1,
                        "sha256": serving.2,
                        "row_count": serving.3,
                        "stored_payload_bytes": serving.4,
                        "object_kinds": V4_FINALIZER_SERVING_KINDS,
                    },
                ],
            }))
            .unwrap(),
        )
        .unwrap();
        path
    }

    fn v4_finalizer_pack_test_manifest(
        directory: &Path,
        price: &V4FinalizerPackTestInput,
        serving: &V4FinalizerPackTestInput,
    ) -> PathBuf {
        v4_finalizer_pack_test_manifest_with_size(directory, price, serving, 2)
    }

    fn v4_finalizer_pack_test_source_row(object_kind_index: usize) -> V4FinalizerSourceRow {
        let test_row = V4FinalizerPackTestRow {
            object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[object_kind_index],
            block_key: 0,
            fragment_no: 0,
            entry_count: 1,
            payload: vec![1],
            block_hash: None,
        };
        parse_v4_finalizer_source_row(&v4_finalizer_pack_test_fields(&test_row)).unwrap()
    }

    fn v4_finalizer_pack_test_lane_writer(
        directory: &Path,
        name: &str,
        lane_index: usize,
        coordinates_per_pack: usize,
    ) -> (V4FinalizerPackLaneWriter, PathBuf) {
        let root = directory.join(name);
        std::fs::create_dir(&root).unwrap();
        let (_, _, price, serving) = v4_finalizer_pack_success_fixture(&root);
        let manifest = v4_finalizer_pack_test_manifest(&root, &price, &serving);
        let (_, mut sources) = load_v4_finalizer_pack_manifest(&manifest).unwrap();
        let source = sources.remove(lane_index);
        let staged = root.join("staged");
        std::fs::create_dir(&staged).unwrap();
        (
            V4FinalizerPackLaneWriter::new(
                source,
                coordinates_per_pack,
                &staged,
                &root.join("final"),
            )
            .unwrap(),
            staged,
        )
    }

    fn v4_finalizer_pack_success_fixture(directory: &Path) -> V4FinalizerPackSuccessFixture {
        let price_rows = vec![
            V4FinalizerPackTestRow {
                object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
                block_key: 0,
                fragment_no: 0,
                entry_count: 3,
                payload: b"same-price".to_vec(),
                block_hash: None,
            },
            V4FinalizerPackTestRow {
                object_kind: V4_FINALIZER_PACKED_OBJECT_KINDS[0],
                block_key: 1,
                fragment_no: 0,
                entry_count: 3,
                payload: b"same-price".to_vec(),
                block_hash: None,
            },
        ];
        let serving_rows = V4_FINALIZER_SERVING_KINDS
            .iter()
            .enumerate()
            .map(|(index, object_kind)| V4FinalizerPackTestRow {
                object_kind,
                block_key: index as i64,
                fragment_no: 0,
                entry_count: index as i64 + 1,
                payload: vec![index as u8 + 1; index + 1],
                block_hash: None,
            })
            .collect::<Vec<_>>();
        let price = v4_finalizer_pack_test_copy(
            directory,
            "shared_price_dictionary_blocks.copy",
            &price_rows,
        );
        let serving =
            v4_finalizer_pack_test_copy(directory, "shared_serving_blocks.copy", &serving_rows);
        (price_rows, serving_rows, price, serving)
    }

    fn v4_finalizer_pack_copy_rows(path: &Path, expected_fields: i16) -> Vec<Vec<Vec<u8>>> {
        let mut reader = BufReader::new(File::open(path).unwrap());
        read_pg_binary_copy_header(&mut reader).unwrap();
        let mut rows = Vec::new();
        while let Some(fields) =
            read_pg_binary_copy_row(&mut reader, expected_fields, "packed test output").unwrap()
        {
            rows.push(
                fields
                    .into_iter()
                    .map(|field| field.expect("packed output fields are non-null"))
                    .collect(),
            );
        }
        assert_eq!(reader.read(&mut [0u8; 1]).unwrap(), 0);
        rows
    }

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
            update_sha256_length_prefixed(&mut canonical_digest, row.object_kind.as_bytes())
                .unwrap();
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
        let duplicate = v4_finalizer_pack_test_copy(
            directory.path(),
            "duplicate-boundary.copy",
            &duplicate_rows,
        );
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
        let conflict =
            v4_finalizer_pack_test_copy(directory.path(), "conflict.copy", &conflict_rows);
        let conflict_manifest =
            v4_finalizer_pack_test_manifest(directory.path(), &conflict, &serving);
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
        let drift_manifest =
            v4_finalizer_pack_test_manifest(directory.path(), &drift_price, &serving);
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
        let (mut writer, _) =
            v4_finalizer_pack_test_lane_writer(directory.path(), "pack-number", 0, 2);
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
            let (mut writer, _) =
                v4_finalizer_pack_test_lane_writer(directory.path(), aggregate, 0, 4);
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

        let (writer, staged) =
            v4_finalizer_pack_test_lane_writer(directory.path(), "empty-kind", 0, 2);
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
        let missing_segment =
            V4FinalizerCanonicalSegment::new(missing_segment_path.clone()).unwrap();
        std::fs::remove_file(missing_segment_path).unwrap();
        assert!(finish_v4_finalizer_canonical_segments(
            vec![missing_segment],
            &missing_segment_root,
        )
        .is_err());

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
            let _invalid =
                TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "not-a-number");
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

    struct LateErrorReader {
        bytes: Cursor<Vec<u8>>,
        emitted_error: bool,
    }

    impl Read for LateErrorReader {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let read = self.bytes.read(buffer)?;
            if read > 0 {
                return Ok(read);
            }
            if !self.emitted_error {
                self.emitted_error = true;
                return Err(io::Error::other("late indexed range failure"));
            }
            Ok(0)
        }
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

    #[cfg(unix)]
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

    fn write_reversed_provider_reference_fixture(path: &Path, referenced_ids: &[i64]) {
        let negotiated_rates = referenced_ids
            .iter()
            .map(|referenced_id| {
                json!({
                    "provider_references": [referenced_id],
                    "negotiated_prices": [{
                        "negotiated_type": "negotiated",
                        "negotiated_rate": 123.45,
                        "expiration_date": "2026-12-31",
                        "service_code": ["11"],
                        "billing_class": "professional"
                    }]
                })
            })
            .collect::<Vec<_>>();
        let in_network = json!([{
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiation_arrangement": " fFs ",
            "name": "Office visit",
            "negotiated_rates": negotiated_rates
        }]);
        let provider_references = json!([{
            "provider_group_id": 7,
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890_i64]
            }]
        }]);
        let payload = format!(
            "{{\"in_network\":{in_network},\"provider_references\":{provider_references}}}"
        );
        std::fs::write(path, payload).unwrap();
    }

    fn read_worker_copy_text(base_path: &Path) -> io::Result<String> {
        let parent = base_path.parent().ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidInput, "COPY path has no parent")
        })?;
        let base_name = base_path
            .file_name()
            .and_then(|value| value.to_str())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "COPY path is not UTF-8"))?;
        let mut shard_paths = std::fs::read_dir(parent)?
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .filter(|path| {
                path.file_name()
                    .and_then(|value| value.to_str())
                    .is_some_and(|name| {
                        name == base_name || name.starts_with(&format!("{base_name}."))
                    })
            })
            .collect::<Vec<_>>();
        shard_paths.sort_unstable();
        if shard_paths.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("no COPY shards found for {}", base_path.display()),
            ));
        }
        let mut output = String::new();
        for shard_path in shard_paths {
            output.push_str(&std::fs::read_to_string(shard_path)?);
        }
        Ok(output)
    }

    fn test_price_lite(negotiated_rate: &str) -> PriceLite {
        PriceLite {
            negotiated_type: Some("negotiated".to_string()),
            negotiated_rate: negotiated_rate.to_string(),
            expiration_date: Some("2026-12-31".to_string()),
            service_code: vec!["11".to_string()],
            billing_class: Some("professional".to_string()),
            setting: None,
            billing_code_modifier: Vec::new(),
            additional_information: None,
        }
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

    fn test_compact_context() -> CompactContext {
        CompactContext {
            snapshot_id: "snapshot-test".to_string(),
            plan_id: "plan-test".to_string(),
            plan_month_id: "2026-07".to_string(),
            source_trace_set_hash: "trace-test".to_string(),
            confidence_code: "test".to_string(),
            source_witness: Arc::new(SourceWitnessCollector::new(&"00".repeat(32)).unwrap()),
            invalid_price_exclusion: None,
        }
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

    fn source_witness_record_metadata(bundle: &[u8]) -> Vec<Value> {
        assert_eq!(&bundle[..8], b"PTG2SW03");
        let header_length = u32::from_be_bytes(bundle[8..12].try_into().unwrap()) as usize;
        let mut offset = 12 + header_length;
        let evidence_count =
            u32::from_be_bytes(bundle[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        for _ in 0..evidence_count {
            offset += 32;
            let _raw_length =
                u32::from_be_bytes(bundle[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let compressed_length =
                u32::from_be_bytes(bundle[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4 + compressed_length;
        }
        let record_count =
            u32::from_be_bytes(bundle[offset..offset + 4].try_into().unwrap()) as usize;
        offset += 4;
        let mut metadata = Vec::with_capacity(record_count);
        for _ in 0..record_count {
            let compressed_length =
                u32::from_be_bytes(bundle[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            let compressed_end = offset + compressed_length;
            let mut decoded = Vec::new();
            ZlibDecoder::new(&bundle[offset..compressed_end])
                .read_to_end(&mut decoded)
                .unwrap();
            assert_eq!(&decoded[..8], b"PTG2SWR2");
            let metadata_length = u32::from_be_bytes(decoded[8..12].try_into().unwrap()) as usize;
            metadata.push(serde_json::from_slice(&decoded[12..12 + metadata_length]).unwrap());
            offset = compressed_end;
        }
        assert_eq!(offset, bundle.len());
        metadata
    }

    fn source_witness_header(bundle: &[u8]) -> Value {
        assert_eq!(&bundle[..8], b"PTG2SW03");
        let header_length = u32::from_be_bytes(bundle[8..12].try_into().unwrap()) as usize;
        serde_json::from_slice(&bundle[12..12 + header_length]).unwrap()
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

        process_compact_rate_lites_worker_with_source(
            &mut state,
            &rates,
            &procedure,
            &source_inputs,
        )
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

    fn strict_scan_env(serving_run_directory: &Path) -> [TestEnvVar; 5] {
        [
            TestEnvVar::set("HLTHPRT_PTG2_SNAPSHOT_ARCH", REQUIRED_SNAPSHOT_ARCH),
            TestEnvVar::set(
                "HLTHPRT_PTG2_V3_SERVING_RUN_DIR",
                serving_run_directory.to_str().unwrap(),
            ),
            TestEnvVar::set(
                V3_COVERAGE_SCOPE_ID_ENV,
                &"11".repeat(COVERAGE_SCOPE_ID_BYTES),
            ),
            TestEnvVar::set(V3_RAW_SOURCE_SHA256_ENV, &"22".repeat(32)),
            TestEnvVar::set(GROUP_NEGOTIATED_RATE_CHUNKS_ENV, "false"),
        ]
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
        let _stale_factor_toggle =
            TestEnvVar::set("HLTHPRT_PTG2_PROVIDER_GRAPH_V4_FACTORS", "false");
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
        let directory =
            std::env::temp_dir().join(format!("ptg2-v4-tax-secret-{}", std::process::id()));
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
            manifest_provider_set_component_sidecar: Some(
                "provider-set-component.sidecar".to_string(),
            ),
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
            manifest_provider_set_dictionary: Some(
                "manifest-provider-set-dictionary.copy".to_string(),
            ),
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
        let base =
            std::env::temp_dir().join(format!("ptg2-serving-run-guard-{}", std::process::id()));
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
            manifest_provider_group_member: Some(
                manifest_member_path.to_string_lossy().to_string(),
            ),
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
                manifest_provider_group_tax_identity_sidecar: Some(
                    path.to_string_lossy().to_string(),
                ),
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

    fn synthetic_v2_tax_identity_dedupe(
        worker_count: usize,
        paired: bool,
        reverse: bool,
    ) -> SharedDedupe {
        let policy =
            TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:test-v2".to_string(), [8u8; 32])
                .unwrap();
        let dedupe = if paired {
            SharedDedupe::new_with_v4_paired_tax_identity(worker_count, false, policy)
        } else {
            SharedDedupe::new_with_v4_tax_identity(worker_count, false, policy)
        };
        let mut rows = vec![
            (
                10,
                Some(json!({
                    "type": "ein",
                    "value": "12-3456789",
                    "business_name": "Private Synthetic Practice One"
                })),
            ),
            (
                10,
                Some(json!({
                    "type": " EIN ",
                    "value": "123456789",
                    "business_name": "Private Synthetic Practice Two"
                })),
            ),
            (11, Some(json!({"type": "npi", "value": "1000000491"}))),
            (12, Some(json!({"type": "npi", "value": "1000000492"}))),
            (13, None),
            (
                14,
                Some(json!({
                    "type": "other",
                    "value": "private-unsupported-marker"
                })),
            ),
        ];
        if reverse {
            rows.reverse();
        }
        for (group_hash, tin) in rows {
            dedupe
                .insert_provider_group_with_tax_identity(group_hash, tin.as_ref())
                .unwrap();
        }
        dedupe
    }

    fn emit_synthetic_v2_tax_identity_artifacts(
        directory: &Path,
        label: &str,
        worker_count: usize,
        reverse: bool,
    ) -> (Vec<u8>, Vec<u8>, Vec<u8>) {
        let dedupe = synthetic_v2_tax_identity_dedupe(worker_count, true, reverse);
        let v1_path = directory.join(format!("{label}-v1.ptg2tax"));
        let v2_path = directory.join(format!("{label}-v2.ptg2tax"));
        let paths = CopyPathConfig {
            manifest_provider_group_tax_identity_sidecar: Some(v1_path.display().to_string()),
            manifest_provider_group_tax_identity_v2_sidecar: Some(v2_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let mut events = Vec::new();
        emit_provider_group_tax_identity_sidecars(&mut events, &paths, &dedupe).unwrap();
        (
            std::fs::read(v1_path).unwrap(),
            std::fs::read(v2_path).unwrap(),
            events,
        )
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
        emit_provider_group_tax_identity_sidecar(&mut direct_v1_event, &v1_paths, &v1_dedupe)
            .unwrap();
        let direct_v1_bytes = std::fs::read(&v1_path).unwrap();
        let paired_v1_dedupe = synthetic_v2_tax_identity_dedupe(8, true, true);
        let mut paired_v1_event = Vec::new();
        emit_provider_group_tax_identity_sidecar(
            &mut paired_v1_event,
            &v1_paths,
            &paired_v1_dedupe,
        )
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

    #[cfg(unix)]
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
                    emit_provider_group_tax_identity_sidecar(&mut events, &paths, &dedupe)
                        .unwrap_err()
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

    #[cfg(unix)]
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
            manifest_provider_group_tax_identity_v2_sidecar: Some(
                v2_only_path.display().to_string(),
            ),
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

    fn reference_extreme_inline_provider_groups() -> Vec<Value> {
        vec![
            json!({
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890i64, -7i64, "1447744750`"],
            }),
            json!({
                "tin": {"type": "ein", "value": "987654321"},
                "npi": [],
            }),
        ]
    }

    fn raw_provider_groups(value: &str) -> Box<RawValue> {
        RawValue::from_string(value.to_owned()).expect("valid raw provider_groups JSON")
    }

    fn v4_test_shared_dedupe(worker_count: usize) -> SharedDedupe {
        let policy =
            TinTokenPolicy::from_secret("ptg-tin-hmac-sha256-v1:test-1".to_string(), [7u8; 32])
                .unwrap();
        SharedDedupe::new_with_v4_tax_identity(worker_count, false, policy)
    }

    fn reference_extreme_inline_rate() -> RateLite {
        RateLite {
            provider_refs: Vec::new(),
            provider_groups: Vec::new(),
            provider_groups_raw: Some(raw_provider_groups(
                r#"[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890,-7,"1447744750`"]},{"tin":{"type":"ein","value":"987654321"},"npi":[]}]"#,
            )),
            network_names: Vec::new(),
            prices: vec![test_price_lite("1")],
            prepared_price_set: None,
        }
    }

    fn audited_inline_transform(groups: &[Value]) -> (ProviderEntry, u64, u64) {
        let provider_ref = json!({"provider_groups": groups});
        let (entry, normalization_count) =
            build_provider_entry_audited(&provider_ref, true).unwrap();
        (entry, normalization_count, groups.len() as u64)
    }

    struct ReferenceExtremeInlineCacheRun {
        output_sha256: [u8; 32],
        dedupe: Value,
        quarantine: Value,
        empty_npi_normalizations: u64,
        cache: V4InlineProviderTransformCacheSnapshot,
    }

    fn run_reference_extreme_inline_cache_case(
        worker_count: usize,
        cache_max_bytes: u64,
    ) -> ReferenceExtremeInlineCacheRun {
        const RATE_ATTEMPTS: usize = 6_250;
        let rate = Arc::new(reference_extreme_inline_rate());
        let cache = Arc::new(V4InlineProviderTransformSharedCache::new(cache_max_bytes));
        let dedupe = Arc::new(v4_test_shared_dedupe(worker_count));
        let mut handles = Vec::new();
        for worker_id in 0..worker_count {
            let rate = Arc::clone(&rate);
            let cache = Arc::clone(&cache);
            let dedupe = Arc::clone(&dedupe);
            handles.push(thread::spawn(move || {
                let mut sinks =
                    DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
                let mut outputs = Vec::new();
                for ordinal in (worker_id..RATE_ATTEMPTS).step_by(worker_count) {
                    let transform = resolve_v4_inline_provider_transform(
                        &rate,
                        &mut sinks,
                        &dedupe,
                        &cache,
                    )
                    .unwrap();
                    outputs.push((
                        ordinal,
                        serde_json::to_vec(&json!({
                            "entry_hash": transform.entry.entry_hash,
                            "provider_count": transform.entry.provider_count,
                            "provider_group_hashes": transform.entry.provider_group_hashes,
                            "npi": transform.entry.npi,
                            "quarantined_npi": transform.entry.quarantined_npi,
                            "quarantined_npi_text": transform.entry.quarantined_npi_text,
                            "empty_npi_tin_only_normalization_count": transform.empty_npi_tin_only_normalization_count,
                        }))
                        .unwrap(),
                    ));
                }
                outputs
            }));
        }
        let mut outputs = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect::<Vec<_>>();
        outputs.sort_by_key(|(ordinal, _)| *ordinal);
        let mut output_digest = Sha256::new();
        for (ordinal, payload) in outputs {
            output_digest.update(ordinal.to_le_bytes());
            output_digest.update((payload.len() as u64).to_le_bytes());
            output_digest.update(payload);
        }
        ReferenceExtremeInlineCacheRun {
            output_sha256: output_digest.finalize().into(),
            dedupe: dedupe_summary_payload(&dedupe, &HashMap::new()),
            quarantine: dedupe
                .provider_identifier_quarantine()
                .unwrap()
                .payload()
                .unwrap(),
            empty_npi_normalizations: dedupe.empty_npi_tin_only_normalization_count(),
            cache: cache.snapshot(),
        }
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

    fn parse_v4_inline_rate(raw_provider_groups: &str) -> RateLite {
        let raw_rate = format!(
            r#"{{"provider_groups":{raw_provider_groups},"negotiated_prices":[{{"negotiated_rate":1}}]}}"#
        );
        let (rate, typed) =
            read_rate_lite_bytes_profiled_with_policy(raw_rate.as_bytes(), true).unwrap();
        assert!(typed, "V4 production parser should retain the raw array");
        rate.expect("V4 inline rate")
    }

    #[test]
    fn v4_production_parse_probes_raw_cache_before_deserialization_and_retains_arc() {
        let raw_provider_groups =
            r#"[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}]"#;
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
        let first_rate = parse_v4_inline_rate(
            r#"[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890]}]"#,
        );
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
        let estimated_a =
            estimated_v4_inline_provider_transform_bytes(&V4InlineProviderTransform {
                cache_key_kind: key_kind,
                cache_key: Arc::from(raw_a.as_slice()),
                provider_groups: groups_a.clone().into(),
                entry: entry_a.clone(),
                empty_npi_tin_only_normalization_count: normalization_a,
                provider_group_attempts: attempts_a,
            });
        let estimated_b =
            estimated_v4_inline_provider_transform_bytes(&V4InlineProviderTransform {
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
        let eviction_cache =
            V4InlineProviderTransformSharedCache::new_with_shards(one_entry_limit, 1);
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

    fn price_json(field_name: &str, field_value: &str) -> String {
        format!(r#"{{"negotiated_rate":12.5,"{field_name}":{field_value}}}"#)
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
            read_rate_lite_bytes_typed_with_exclusion(raw, false, coordinate, Some(&policy))
                .unwrap(),
            read_rate_lite_bytes_typed_with_exclusion(raw, true, coordinate, Some(&policy))
                .unwrap(),
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
                    panic!(
                        "parser contract diverged: profiled={profiled:?} streaming={streaming:?}"
                    )
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
        let expected =
            provider_set_global_id_from_group_hashes_and_network_names(&groups, &networks);
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

    fn valid_provider_reference() -> Value {
        json!({
            "provider_group_id": 7,
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890_i64]
            }]
        })
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

    fn fill_provider_identifier_quarantine(dedupe: &SharedDedupe) {
        let values = (1..=1024)
            .map(|value| -i64::from(value))
            .collect::<Vec<_>>();
        dedupe
            .record_quarantined_provider_identifiers(&values)
            .unwrap();
    }

    #[test]
    fn skipped_inline_provider_quarantine_covers_raw_and_parsed_rates() {
        let parsed_rate = RateLite {
            provider_refs: Vec::new(),
            provider_groups: vec![json!({"npi": [-7, "malformed"]})],
            provider_groups_raw: None,
            network_names: Vec::new(),
            prices: Vec::new(),
            prepared_price_set: None,
        };
        let dedupe = SharedDedupe::new(1);
        record_skipped_inline_provider_quarantine(&parsed_rate, &dedupe, false).unwrap();

        let raw_rate = RateLite {
            provider_groups: Vec::new(),
            provider_groups_raw: Some(raw_provider_groups(
                r#"[{"npi":[]},{"npi":[-8,"also-malformed"]}]"#,
            )),
            ..parsed_rate.clone()
        };
        record_skipped_inline_provider_quarantine(&raw_rate, &dedupe, true).unwrap();

        let invalid_raw_rate = RateLite {
            provider_groups_raw: Some(raw_provider_groups("null")),
            ..raw_rate.clone()
        };
        assert!(
            record_skipped_inline_provider_quarantine(&invalid_raw_rate, &dedupe, true).is_err()
        );

        let empty_rate = RateLite {
            provider_groups_raw: None,
            ..raw_rate
        };
        record_skipped_inline_provider_quarantine(&empty_rate, &dedupe, false).unwrap();

        let oversized_rate = RateLite {
            provider_groups: vec![json!({"npi": ["x".repeat(129)]})],
            ..parsed_rate.clone()
        };
        assert!(
            record_skipped_inline_provider_quarantine(&oversized_rate, &dedupe, false).is_err()
        );
        assert!(record_skipped_inline_provider_quarantine(&oversized_rate, &dedupe, true).is_err());

        let full_numeric = SharedDedupe::new(1);
        fill_provider_identifier_quarantine(&full_numeric);
        let extra_numeric_rate = RateLite {
            provider_groups: vec![json!({"npi": [-1025]})],
            ..parsed_rate.clone()
        };
        assert!(record_skipped_inline_provider_quarantine(
            &extra_numeric_rate,
            &full_numeric,
            false,
        )
        .is_err());

        let full_text = SharedDedupe::new(1);
        fill_provider_identifier_quarantine(&full_text);
        let extra_text_rate = RateLite {
            provider_groups: vec![json!({"npi": ["new-malformed"]})],
            ..parsed_rate.clone()
        };
        assert!(
            record_skipped_inline_provider_quarantine(&extra_text_rate, &full_text, false,)
                .is_err()
        );

        let provider_group = json!({
            "tin": {"type": "ein", "value": "123456789"},
            "npi": [1234567890_i64, "new-malformed"],
        });
        let capped_rate = RateLite {
            provider_groups: vec![provider_group],
            ..parsed_rate.clone()
        };
        let capped_dedupe = SharedDedupe::new(1);
        fill_provider_identifier_quarantine(&capped_dedupe);
        let mut capped_sinks =
            DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        assert!(provider_entry_view_for_worker_rate(
            &HashMap::new(),
            &capped_rate,
            &mut capped_sinks,
            &capped_dedupe,
        )
        .is_err());

        let capped_v4_dedupe = v4_test_shared_dedupe(1);
        fill_provider_identifier_quarantine(&capped_v4_dedupe);
        let mut capped_v4_sinks =
            DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        assert!(resolve_v4_inline_provider_transform(
            &capped_rate,
            &mut capped_v4_sinks,
            &capped_v4_dedupe,
            &V4InlineProviderTransformSharedCache::new(0),
        )
        .is_err());

        let quarantine = dedupe
            .provider_identifier_quarantine()
            .unwrap()
            .payload()
            .unwrap();
        assert_eq!(
            quarantine["contract"],
            "ptg2_provider_identifier_quarantine_v2"
        );
        assert_eq!(quarantine["occurrence_count"], 4);
        assert_eq!(quarantine["distinct_value_count"], 4);
    }

    #[test]
    fn strict_provider_definition_rejects_malformed_tin_and_network_metadata() {
        for invalid_tin in [
            Value::Null,
            json!(true),
            json!([]),
            json!({}),
            json!({"type": "ein"}),
            json!({"value": "123456789"}),
            json!({"type": 7, "value": "123456789"}),
            json!({"type": "ein", "value": 123456789}),
            json!({"type": " ", "value": "123456789"}),
            json!({"type": "ein", "value": " "}),
            json!({"type": "ein", "value": "123456789", "business_name": 7}),
        ] {
            let mut provider_ref = valid_provider_reference();
            provider_ref["provider_groups"][0]["tin"] = invalid_tin;
            assert!(provider_ref_definition(&provider_ref).is_err());
        }

        for invalid_network_name in [
            Value::Null,
            json!(true),
            json!(7),
            json!({}),
            json!([["network"]]),
            json!(["network", 7]),
        ] {
            let mut provider_ref = valid_provider_reference();
            provider_ref["network_name"] = invalid_network_name;
            assert!(provider_ref_definition(&provider_ref).is_err());
        }

        let mut scalar_provider_ref = valid_provider_reference();
        scalar_provider_ref["network_name"] = json!(" network one ");
        let (_, scalar_entry) = provider_ref_definition(&scalar_provider_ref).unwrap();
        assert_eq!(scalar_entry.network_names, vec!["network one"]);

        let mut singleton_array_provider_ref = valid_provider_reference();
        singleton_array_provider_ref["network_name"] = json!([" network one "]);
        let (_, singleton_array_entry) =
            provider_ref_definition(&singleton_array_provider_ref).unwrap();
        assert_eq!(scalar_entry.entry_hash, singleton_array_entry.entry_hash);

        let mut provider_ref = valid_provider_reference();
        provider_ref["network_name"] = json!(["network one", "network two"]);
        provider_ref_definition(&provider_ref).unwrap();
    }

    #[test]
    fn v4_provider_definition_classifies_tin_without_rejecting_pricing() {
        for unavailable_tin in [
            Value::Null,
            json!(true),
            json!([]),
            json!({}),
            json!({"type": "ein"}),
            json!({"value": "123456789"}),
            json!({"type": 7, "value": "123456789"}),
            json!({"type": "ein", "value": 123456789}),
            json!({"type": " ", "value": "123456789"}),
            json!({"type": "ein", "value": " "}),
            json!({"type": "ein", "value": "01💥2345678"}),
            json!({"type": "ein", "value": "123456789", "business_name": 7}),
        ] {
            let mut provider_ref = valid_provider_reference();
            provider_ref["provider_groups"][0]["tin"] = unavailable_tin;
            provider_ref_definition_audited(&provider_ref, true).unwrap();
        }

        let mut provider_ref = valid_provider_reference();
        provider_ref["network_name"] = json!(["network", 7]);
        assert!(provider_ref_definition_audited(&provider_ref, true).is_err());
    }

    #[test]
    fn strict_rate_parser_rejects_invalid_provider_reference_scalar_types() {
        for invalid in [r#""7""#, "true", "{}", "[]", "null", "7.5"] {
            let raw = format!(
                r#"{{"provider_references":[{invalid}],"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
            );
            let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
        }
    }

    #[test]
    fn strict_rate_parser_rejects_non_string_network_names() {
        for invalid in ["true", "12", "{}", r#"["network", 12]"#] {
            let raw = format!(
                r#"{{"provider_references":[7],"network_name":{invalid},"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
            );
            assert!(read_rate_lite_from_reader(raw.as_bytes()).is_err(), "{raw}");
        }
        for valid in ["null", r#""network""#, r#"["network one","network two"]"#] {
            let raw = format!(
                r#"{{"provider_references":[7],"network_name":{valid},"negotiated_prices":[{{"negotiated_rate":12.5}}]}}"#
            );
            assert!(read_rate_lite_from_reader(raw.as_bytes()).is_ok(), "{raw}");
        }
    }

    #[test]
    fn strict_rate_parser_rejects_missing_or_empty_provider_membership() {
        for raw in [
            r#"{"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
            r#"{"provider_references":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
            r#"{"provider_groups":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
            r#"{"provider_references":[],"provider_groups":[],"negotiated_prices":[{"negotiated_rate":12.5}]}"#,
        ] {
            let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
            assert!(error.to_string().contains("provider"), "{error}");
            assert!(read_rate_lite_bytes(raw.as_bytes()).is_err(), "{raw}");
        }
    }

    #[test]
    fn strict_rate_parser_rejects_missing_or_empty_negotiated_prices() {
        for raw in [
            r#"{"provider_references":[7]}"#,
            r#"{"provider_references":[7],"negotiated_prices":[]}"#,
        ] {
            let error = read_rate_lite_from_reader(raw.as_bytes()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{raw}");
            assert!(error.to_string().contains("negotiated price"), "{error}");
            assert!(read_rate_lite_bytes(raw.as_bytes()).is_err(), "{raw}");
        }
    }

    #[test]
    fn repeated_provider_group_ids_union_split_fragments_canonically() {
        let provider_ref = valid_provider_reference();
        let (key, mut entry) = provider_ref_definition(&provider_ref).unwrap();
        entry.source_locators.push(ProviderSourceLocator {
            shard: 1,
            offset: 20,
            length: 2,
        });
        let mut provider_map = HashMap::new();
        insert_provider_definition(&mut provider_map, key.clone(), entry.clone()).unwrap();
        insert_provider_definition(&mut provider_map, key.clone(), entry.clone()).unwrap();
        assert_eq!(provider_map.len(), 1);

        let mut split_ref = provider_ref.clone();
        split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
        let mut split_entry = build_provider_entry(&split_ref).unwrap();
        split_entry.source_locators.push(ProviderSourceLocator {
            shard: 0,
            offset: 10,
            length: 1,
        });
        let expected = combine_provider_entries(entry.clone(), split_entry.clone());
        insert_provider_definition(&mut provider_map, key.clone(), split_entry.clone()).unwrap();
        assert_eq!(provider_map.len(), 1);
        assert_eq!(provider_map.get(&key), Some(&expected));
        assert_eq!(provider_map[&key].provider_count, 2);
        assert_eq!(provider_map[&key].npi, vec![1234567890, 2222222222]);
        assert_eq!(provider_map[&key].source_locators, expected.source_locators);

        let mut conflicting_ref = provider_ref.clone();
        conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
        let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();
        let error = insert_provider_definition(&mut provider_map, key.clone(), conflicting_entry)
            .unwrap_err();
        assert!(error
            .to_string()
            .contains("conflicting provider_group_id definition: 7"));
        assert_eq!(provider_map.get(&key), Some(&expected));

        let mut left = HashMap::new();
        left.insert(key.clone(), entry.clone());
        let mut right = HashMap::new();
        right.insert(key.clone(), entry.clone());
        let merged = merge_provider_maps_pairwise(vec![(0, left), (1, right)]).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged.get(&key), Some(&entry));

        let mut right = HashMap::new();
        right.insert(key.clone(), entry.clone());
        let merged = merge_provider_maps_pairwise(vec![(0, HashMap::new()), (1, right)]).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged.get(&key), Some(&entry));

        let mut left = HashMap::new();
        left.insert(key.clone(), entry.clone());
        let mut right = HashMap::new();
        right.insert(key, split_entry);
        let merged = merge_provider_maps_pairwise(vec![(0, left), (1, right)]).unwrap();
        assert_eq!(merged.values().next(), Some(&expected));
    }

    #[test]
    fn distinct_scope_conflicts_preserve_same_scope_unions_deterministically() {
        let provider_ref = valid_provider_reference();
        let (key, entry) = provider_ref_definition(&provider_ref).unwrap();
        let mut split_ref = provider_ref.clone();
        split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
        let split_entry = build_provider_entry(&split_ref).unwrap();
        let expected_scope = combine_provider_entries(entry.clone(), split_entry.clone());
        let mut conflicting_ref = provider_ref.clone();
        conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
        let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();

        let mut forward = ProviderDefinitions::default();
        forward.insert(key.clone(), entry.clone()).unwrap();
        forward.insert(key.clone(), split_entry.clone()).unwrap();
        forward
            .insert(key.clone(), conflicting_entry.clone())
            .unwrap();
        let (forward_map, forward_conflicts) = forward.clone().into_materialized().unwrap();
        assert!(!forward_map.contains_key(&key));
        assert_eq!(forward_conflicts.definition_count, 2);
        assert_eq!(
            forward_conflicts.definitions_by_key[&key]
                .get(&expected_scope.provider_group_scope_hash),
            Some(&expected_scope)
        );

        let mut reverse = ProviderDefinitions::default();
        reverse
            .insert(key.clone(), conflicting_entry.clone())
            .unwrap();
        reverse.insert(key.clone(), split_entry.clone()).unwrap();
        reverse.insert(key.clone(), entry.clone()).unwrap();
        let (reverse_map, reverse_conflicts) = reverse.into_materialized().unwrap();
        assert_eq!(reverse_conflicts, forward_conflicts);

        let forward_payload = provider_identifier_quarantine_payload(
            &forward_map,
            ProviderIdentifierQuarantine::default(),
            &forward_conflicts,
            &[0x11; 32],
        )
        .unwrap();
        let reverse_payload = provider_identifier_quarantine_payload(
            &reverse_map,
            ProviderIdentifierQuarantine::default(),
            &reverse_conflicts,
            &[0x11; 32],
        )
        .unwrap();
        assert_eq!(forward_payload, reverse_payload);
        assert_eq!(
            forward_payload["contract"],
            "ptg2_provider_identifier_quarantine_v2"
        );
        assert_eq!(forward_payload["provider_group_conflict_count"], 1);
        assert_eq!(
            forward_payload["provider_group_conflicting_definition_count"],
            2
        );

        let mut left = ProviderDefinitions::default();
        left.insert(key.clone(), entry).unwrap();
        let mut right = ProviderDefinitions::default();
        right.insert(key.clone(), split_entry).unwrap();
        right.insert(key.clone(), conflicting_entry).unwrap();
        let merged = merge_provider_definitions_pairwise(vec![(0, left), (1, right)]).unwrap();
        let (_merged_map, merged_conflicts) = merged.into_materialized().unwrap();
        assert_eq!(merged_conflicts, forward_conflicts);

        let referenced_rate = RateLite {
            provider_refs: vec![key.clone()],
            provider_groups: Vec::new(),
            provider_groups_raw: None,
            network_names: Vec::new(),
            prices: Vec::new(),
            prepared_price_set: None,
        };
        assert!(
            validate_unreferenced_provider_group_conflicts(&[], &forward_conflicts.keys()).is_ok()
        );
        let error = validate_unreferenced_provider_group_conflicts(
            &[referenced_rate],
            &HashSet::from([key]),
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("referenced conflicting provider_group_id definition: 7"));
    }

    #[test]
    fn provider_conflict_identity_is_scoped_to_the_raw_source() {
        let key = ProviderRefKey::from("7");

        assert_eq!(
            provider_ref_key_sha256(&[0x11; 32], &key),
            provider_ref_key_sha256(&[0x11; 32], &key),
        );
        assert_ne!(
            provider_ref_key_sha256(&[0x11; 32], &key),
            provider_ref_key_sha256(&[0x22; 32], &key),
        );
    }

    #[test]
    fn provider_definition_parts_dedupe_before_scope_union_across_orders_and_workers() {
        let mut once_ref = valid_provider_reference();
        once_ref["provider_groups"][0]["npi"] = json!([1234567890_i64, -1_i64]);
        let (key, mut once) = provider_ref_definition(&once_ref).unwrap();
        let mut duplicate = once.clone();
        once.source_locators.push(ProviderSourceLocator {
            shard: 1,
            offset: 20,
            length: 2,
        });
        duplicate.source_locators.push(ProviderSourceLocator {
            shard: 0,
            offset: 30,
            length: 2,
        });

        let mut twice_ref = once_ref.clone();
        twice_ref["provider_groups"][0]["npi"] = json!([1234567890_i64, -1_i64, -1_i64]);
        let mut twice = build_provider_entry(&twice_ref).unwrap();
        twice.source_locators.push(ProviderSourceLocator {
            shard: 0,
            offset: 40,
            length: 3,
        });

        let mut conflicting_ref = once_ref.clone();
        conflicting_ref["provider_groups"][0]["tin"]["value"] = json!("987654321");
        conflicting_ref["provider_groups"][0]["npi"] = json!([2222222222_i64, -2_i64, "bad"]);
        let mut conflicting = build_provider_entry(&conflicting_ref).unwrap();
        conflicting.source_locators.push(ProviderSourceLocator {
            shard: 1,
            offset: 50,
            length: 4,
        });

        let mut forward = ProviderDefinitions::default();
        for entry in [
            once.clone(),
            duplicate.clone(),
            twice.clone(),
            conflicting.clone(),
        ] {
            forward.insert(key.clone(), entry).unwrap();
        }
        let (forward_map, forward_conflicts) = forward.into_materialized().unwrap();
        assert!(forward_map.is_empty());
        let retained_scope =
            &forward_conflicts.definitions_by_key[&key][&once.provider_group_scope_hash];
        assert_eq!(retained_scope.source_locators.len(), 2);
        let expected_payload = provider_identifier_quarantine_payload(
            &forward_map,
            ProviderIdentifierQuarantine::default(),
            &forward_conflicts,
            &[0x11; 32],
        )
        .unwrap();
        assert_eq!(expected_payload["occurrence_count"], 5);
        assert_eq!(expected_payload["distinct_value_count"], 3);
        assert_eq!(expected_payload["entries"][0]["value"], "-2");
        assert_eq!(expected_payload["entries"][0]["occurrence_count"], 1);
        assert_eq!(expected_payload["entries"][1]["value"], "-1");
        assert_eq!(expected_payload["entries"][1]["occurrence_count"], 3);
        assert_eq!(expected_payload["entries"][2]["kind"], "string");
        assert_eq!(expected_payload["entries"][2]["occurrence_count"], 1);

        let mut reverse = ProviderDefinitions::default();
        for entry in [
            conflicting.clone(),
            twice.clone(),
            duplicate.clone(),
            once.clone(),
        ] {
            reverse.insert(key.clone(), entry).unwrap();
        }
        let (reverse_map, reverse_conflicts) = reverse.into_materialized().unwrap();
        assert_eq!(
            provider_identifier_quarantine_payload(
                &reverse_map,
                ProviderIdentifierQuarantine::default(),
                &reverse_conflicts,
                &[0x11; 32],
            )
            .unwrap(),
            expected_payload
        );

        let mut left = ProviderDefinitions::default();
        left.insert(key.clone(), once).unwrap();
        left.insert(key.clone(), twice).unwrap();
        let mut right = ProviderDefinitions::default();
        right.insert(key.clone(), duplicate).unwrap();
        right.insert(key, conflicting).unwrap();
        let merged = merge_provider_definitions_pairwise(vec![(0, left), (1, right)]).unwrap();
        let (merged_map, merged_conflicts) = merged.into_materialized().unwrap();
        assert_eq!(
            provider_identifier_quarantine_payload(
                &merged_map,
                ProviderIdentifierQuarantine::default(),
                &merged_conflicts,
                &[0x11; 32],
            )
            .unwrap(),
            expected_payload
        );
    }

    #[test]
    fn retained_provider_definition_replay_verifies_source_identity() {
        let provider_ref = valid_provider_reference();
        let raw_provider = serde_json::to_vec(&provider_ref).unwrap();
        let (key, mut entry) = provider_ref_definition(&provider_ref).unwrap();
        let source_witness = SourceWitnessCollector::new(&"ab".repeat(32)).unwrap();
        source_witness.configure_provider_spools(1).unwrap();
        entry.source_locators.push(
            source_witness
                .store_provider_source(0, &raw_provider)
                .unwrap(),
        );
        source_witness.seal_provider_sources().unwrap();
        let dedupe = SharedDedupe::new(1);
        let context = ProviderRefBatchContext {
            dedupe: &dedupe,
            source_witness: &source_witness,
            manifest_sidecars: None,
            allow_empty_npi_tin_only: false,
            defer_definition_outputs: false,
        };

        emit_provider_definition_outputs(
            &HashMap::from([(key.clone(), entry.clone())]),
            1,
            &CopyPathConfig::default(),
            0,
            context,
        )
        .unwrap();

        let mut drifted_entry = entry;
        drifted_entry.npi.push(2_222_222_222);
        drifted_entry.provider_count = 2;
        let drift_dedupe = SharedDedupe::new(1);
        let drift_sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
        let drift_context = ProviderRefBatchContext {
            dedupe: &drift_dedupe,
            source_witness: &source_witness,
            manifest_sidecars: Some(&drift_sidecars),
            allow_empty_npi_tin_only: false,
            defer_definition_outputs: false,
        };
        let error = emit_provider_definition_outputs(
            &HashMap::from([(key, drifted_entry)]),
            1,
            &CopyPathConfig::default(),
            0,
            drift_context,
        )
        .err()
        .expect("source identity drift must fail closed");
        assert!(error.to_string().contains("differs from its source spool"));
        assert!(lock_manifest_sidecars(&drift_sidecars)
            .provider_component_group
            .is_empty());
    }

    #[test]
    fn retained_provider_replay_validates_every_definition_before_any_output() {
        let directory = tempfile::tempdir().unwrap();
        let first_provider = valid_provider_reference();
        let mut second_provider = valid_provider_reference();
        second_provider["provider_group_id"] = json!(8);
        second_provider["provider_groups"][0]["npi"] = json!([2222222222_i64]);
        let first_raw = serde_json::to_vec(&first_provider).unwrap();
        let second_raw = serde_json::to_vec(&second_provider).unwrap();
        let source_witness = SourceWitnessCollector::new(&"ef".repeat(32)).unwrap();
        source_witness.configure_provider_spools(1).unwrap();
        let (first_key, mut first_entry) = provider_ref_definition(&first_provider).unwrap();
        first_entry
            .source_locators
            .push(source_witness.store_provider_source(0, &first_raw).unwrap());
        let (second_key, mut second_entry) = provider_ref_definition(&second_provider).unwrap();
        second_entry.source_locators.push(
            source_witness
                .store_provider_source(0, &second_raw)
                .unwrap(),
        );
        assert!(first_entry.source_locators[0].offset < second_entry.source_locators[0].offset);
        source_witness.seal_provider_sources().unwrap();
        second_entry.npi.push(3_333_333_333);
        second_entry.provider_count += 1;

        let dedupe = SharedDedupe::new(1);
        let sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
        let context = ProviderRefBatchContext {
            dedupe: &dedupe,
            source_witness: &source_witness,
            manifest_sidecars: Some(&sidecars),
            allow_empty_npi_tin_only: false,
            defer_definition_outputs: false,
        };
        let base_path = directory.path().join("provider-group-member.copy");
        let copy_paths = CopyPathConfig {
            manifest_provider_group_member: Some(base_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let worker_path = copy_paths
            .for_worker(0)
            .manifest_provider_group_member
            .unwrap();

        let error = emit_provider_definition_outputs(
            &HashMap::from([(first_key, first_entry), (second_key, second_entry)]),
            1,
            &copy_paths,
            0,
            context,
        )
        .err()
        .expect("later source identity drift must fail closed");

        assert!(error.to_string().contains("differs from its source spool"));
        assert!(!Path::new(&worker_path).exists());
        assert!(lock_manifest_sidecars(&sidecars)
            .provider_component_group
            .is_empty());
        let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(summary["provider_group_attempted"], 0);
        assert_eq!(summary["provider_group_member_attempted"], 0);
    }

    #[test]
    fn retained_split_provider_replay_records_the_aggregate_component() {
        let provider_ref = valid_provider_reference();
        let mut split_ref = provider_ref.clone();
        split_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
        let source_witness = SourceWitnessCollector::new(&"cd".repeat(32)).unwrap();
        source_witness.configure_provider_spools(1).unwrap();
        let mut definitions = ProviderDefinitions::default();
        for value in [&provider_ref, &split_ref] {
            let raw_provider = serde_json::to_vec(value).unwrap();
            let (key, mut entry) = provider_ref_definition(value).unwrap();
            entry.source_locators.push(
                source_witness
                    .store_provider_source(0, &raw_provider)
                    .unwrap(),
            );
            definitions.insert(key, entry).unwrap();
        }
        let (provider_map, conflicts) = definitions.into_materialized().unwrap();
        assert!(conflicts.definitions_by_key.is_empty());
        source_witness.seal_provider_sources().unwrap();
        let expected_entry = provider_map.values().next().unwrap();
        let expected_component_id =
            provider_component_global_id_from_hash(expected_entry.entry_hash);
        let expected_group_ids = expected_entry
            .provider_group_hashes
            .iter()
            .map(|hash| provider_group_global_id_from_hash(*hash))
            .collect::<Vec<_>>();
        let dedupe = SharedDedupe::new(1);
        let sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
        let context = ProviderRefBatchContext {
            dedupe: &dedupe,
            source_witness: &source_witness,
            manifest_sidecars: Some(&sidecars),
            allow_empty_npi_tin_only: false,
            defer_definition_outputs: false,
        };

        emit_provider_definition_outputs(&provider_map, 1, &CopyPathConfig::default(), 0, context)
            .unwrap();

        assert_eq!(
            lock_manifest_sidecars(&sidecars)
                .provider_component_group
                .get(&expected_component_id),
            Some(&expected_group_ids)
        );
    }

    #[test]
    fn provider_quarantine_counts_only_globally_deduped_definitions() {
        let definition = json!({
            "provider_group_id": 7,
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890_i64, -7_i64, "1447744750`"]
            }]
        });
        let (key, entry) = provider_ref_definition(&definition).unwrap();
        let mut provider_map = merge_provider_maps_pairwise(vec![
            (0, HashMap::from([(key.clone(), entry.clone())])),
            (1, HashMap::from([(key.clone(), entry.clone())])),
        ])
        .unwrap();
        let dedupe = SharedDedupe::new(1);

        record_and_clear_provider_identifier_quarantine(&mut provider_map, &dedupe).unwrap();

        let quarantine = dedupe
            .provider_identifier_quarantine()
            .unwrap()
            .payload()
            .unwrap();
        assert_eq!(quarantine["occurrence_count"], 2);
        assert_eq!(quarantine["distinct_value_count"], 2);
        assert!(provider_map[&key].quarantined_npi.is_empty());
        assert!(provider_map[&key].quarantined_npi_text.is_empty());

        let mut invalid_numeric_entry = entry.clone();
        invalid_numeric_entry.quarantined_npi = vec![0];
        assert!(record_and_clear_provider_identifier_quarantine(
            &mut HashMap::from([(
                ProviderRefKey::from("invalid-numeric"),
                invalid_numeric_entry,
            )]),
            &SharedDedupe::new(1),
        )
        .is_err());

        let mut invalid_text_entry = entry.clone();
        invalid_text_entry.quarantined_npi_text = vec!["x".repeat(129)];
        assert!(record_and_clear_provider_identifier_quarantine(
            &mut HashMap::from([(ProviderRefKey::from("invalid-text"), invalid_text_entry)]),
            &SharedDedupe::new(1),
        )
        .is_err());

        let mut split_entry = entry.clone();
        split_entry
            .quarantined_npi_text
            .push("1447744750`".to_string());
        let mut split_map = merge_provider_maps_pairwise(vec![
            (0, HashMap::from([(key.clone(), entry)])),
            (1, HashMap::from([(key.clone(), split_entry)])),
        ])
        .unwrap();
        let split_dedupe = SharedDedupe::new(1);
        record_and_clear_provider_identifier_quarantine(&mut split_map, &split_dedupe).unwrap();
        let split_quarantine = split_dedupe
            .provider_identifier_quarantine()
            .unwrap()
            .payload()
            .unwrap();
        assert_eq!(split_quarantine["occurrence_count"], 5);
        assert_eq!(split_quarantine["distinct_value_count"], 2);
        assert!(split_map[&key].quarantined_npi.is_empty());
        assert!(split_map[&key].quarantined_npi_text.is_empty());
    }

    #[test]
    fn raw_provider_reference_worker_accepts_identical_group_ids() {
        let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        let dedupe = SharedDedupe::new(1);
        let mut provider_map = HashMap::new();
        let raw_ref = serde_json::to_vec(&valid_provider_reference()).unwrap();
        let mut raw_refs = RawRateChunk::with_capacity(2, raw_ref.len() * 2);
        for _ in 0..2 {
            let start = raw_refs.byte_len();
            raw_refs.bytes.extend_from_slice(&raw_ref);
            raw_refs.push_current_value_span(start);
        }

        let processed = process_provider_ref_raw_batch(
            &raw_refs,
            &mut provider_map,
            &mut sinks,
            &dedupe,
            false,
        )
        .unwrap();

        assert_eq!(processed, 2);
        assert_eq!(provider_map.len(), 1);
    }

    #[test]
    fn raw_provider_reference_worker_preserves_empty_npi_group_without_edges() {
        let mut provider_ref = valid_provider_reference();
        provider_ref["provider_groups"][0]["npi"] = json!([]);
        let raw_ref = serde_json::to_vec(&provider_ref).unwrap();
        let mut raw_refs = RawRateChunk::with_capacity(1, raw_ref.len());
        raw_refs.bytes.extend_from_slice(&raw_ref);
        raw_refs.push_current_value_span(0);

        let mut sinks = DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        let dedupe = v4_test_shared_dedupe(2);
        let mut provider_map = HashMap::new();
        let processed =
            process_provider_ref_raw_batch(&raw_refs, &mut provider_map, &mut sinks, &dedupe, true)
                .unwrap();

        assert_eq!(processed, 1);
        assert_eq!(provider_map.len(), 1);
        let entry = provider_map.values().next().unwrap();
        assert_eq!(entry.provider_count, 0);
        assert!(entry.npi.is_empty());
        let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(dedupe.empty_npi_tin_only_normalization_count(), 1);
        assert_eq!(summary["provider_group_attempted"], 1);
        assert_eq!(summary["provider_group_member_attempted"], 0);
    }

    #[test]
    fn preloaded_provider_definitions_are_idempotent_and_conflicts_fail_closed() {
        let provider_ref = valid_provider_reference();
        let (key, entry) = provider_ref_definition(&provider_ref).unwrap();
        let mut provider_map = HashMap::new();
        provider_map.insert(key.clone(), entry.clone());
        validate_preloaded_provider_definition(&provider_map, &key, &entry).unwrap();
        validate_preloaded_provider_definition(&provider_map, &key, &entry).unwrap();

        let mut conflicting_ref = provider_ref.clone();
        conflicting_ref["provider_groups"][0]["npi"] = json!([2222222222_i64]);
        let conflicting_entry = build_provider_entry(&conflicting_ref).unwrap();

        let error = validate_preloaded_provider_definition(&provider_map, &key, &conflicting_entry)
            .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("provider_group_id definition changed after preflight: 7"));
    }

    #[test]
    fn procedure_and_v3_code_identity_normalize_arrangement_consistently() {
        let first = json!({
            "billing_code_type": " cpt ",
            "billing_code_type_version": "2026",
            "billing_code": " 99213 ",
            "negotiation_arrangement": "ffs",
            "name": "First display name",
            "description": "First description"
        });
        let same_identity = json!({
            "billing_code_type": "CPT",
            "billing_code_type_version": "2026",
            "billing_code": "99213",
            "negotiation_arrangement": "FFS",
            "name": "Different display name",
            "description": "Different description"
        });
        let bundled = json!({
            "billing_code_type": "CPT",
            "billing_code_type_version": "2026",
            "billing_code": "99213",
            "negotiation_arrangement": "bundle"
        });

        let first_payload = procedure_identity_payload(&first);
        let same_payload = procedure_identity_payload(&same_identity);
        let bundled_payload = procedure_identity_payload(&bundled);

        assert_eq!(first_payload, same_payload);
        assert_eq!(
            procedure_global_id(&first_payload),
            procedure_global_id(&same_payload)
        );
        assert_ne!(
            procedure_global_id(&first_payload),
            procedure_global_id(&bundled_payload)
        );

        let drg_alias = json!({
            "billing_code_type": "MS-DRG",
            "billing_code": "7",
            "negotiation_arrangement": "ffs"
        });
        let drg_canonical = json!({
            "billing_code_type": "MS_DRG",
            "billing_code": "007",
            "negotiation_arrangement": "FFS"
        });
        assert_eq!(
            procedure_identity_payload(&drg_alias),
            procedure_identity_payload(&drg_canonical)
        );
        assert_eq!(
            procedure_global_id(&procedure_identity_payload(&drg_alias)),
            procedure_global_id(&procedure_identity_payload(&drg_canonical))
        );

        let scope = [0x44; COVERAGE_SCOPE_ID_BYTES];
        let first_arrangement = normalize_code(first.get("negotiation_arrangement"));
        let same_arrangement = normalize_code(same_identity.get("negotiation_arrangement"));
        let bundled_arrangement = normalize_code(bundled.get("negotiation_arrangement"));
        let first_code_id = natural_lean_code_identity(
            &scope,
            Some("CPT"),
            Some("99213"),
            first_arrangement.as_deref(),
            None,
            None,
            None,
        );
        assert_eq!(
            first_code_id,
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                same_arrangement.as_deref(),
                None,
                None,
                None,
            )
        );
        assert_ne!(
            first_code_id,
            natural_lean_code_identity(
                &scope,
                Some("CPT"),
                Some("99213"),
                bundled_arrangement.as_deref(),
                None,
                None,
                None,
            )
        );
    }

    #[test]
    fn provider_set_identity_depends_on_groups_not_reference_packaging() {
        let group_a = json!({
            "tin": {"type": "ein", "value": "111111111"},
            "npi": [1111111111]
        });
        let group_b = json!({
            "tin": {"type": "ein", "value": "222222222"},
            "npi": [2222222222_i64]
        });
        let entry_a = build_provider_entry(&json!({"provider_groups": [group_a.clone()]})).unwrap();
        let entry_b = build_provider_entry(&json!({"provider_groups": [group_b.clone()]})).unwrap();
        let combined =
            build_provider_entry(&json!({"provider_groups": [group_a, group_b]})).unwrap();
        let mut provider_map = HashMap::new();
        provider_map.insert(ProviderRefKey::from("a"), entry_a);
        provider_map.insert(ProviderRefKey::from("b"), entry_b);
        let separate = provider_set_from_ref_keys(
            &provider_map,
            &[ProviderRefKey::from("a"), ProviderRefKey::from("b")],
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            separate.provider_group_hashes,
            combined.provider_group_hashes
        );
        assert_eq!(
            provider_set_global_id_from_group_hashes_and_network_names(
                &separate.provider_group_hashes,
                &separate.network_names,
            ),
            provider_set_global_id_from_group_hashes_and_network_names(
                &combined.provider_group_hashes,
                &combined.network_names,
            )
        );
        assert_eq!(
            hash_i64_list("provider_set", &separate.provider_group_hashes),
            hash_i64_list("provider_set", &combined.provider_group_hashes)
        );
    }

    #[test]
    fn raw_provider_reference_batch_retains_npis_for_exact_counts() {
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
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let dedupe = SharedDedupe::new(2);
        let mut provider_map = HashMap::new();
        let raw_ref_values = [
            br#"{"provider_group_id":7,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890,1234567891]}]}"#.to_vec(),
            br#"{"provider_group_id":8,"provider_groups":[{"tin":{"type":"npi","value":"9876543210"},"npi":[2222222222]}]}"#.to_vec(),
            br#"{"provider_group_id":121591448686103182592848195376305442061,"provider_groups":[{"tin":{"type":"ein","value":"462560124"},"npi":[1265502504]}]}"#.to_vec(),
        ];
        let mut raw_refs = RawRateChunk::with_capacity(raw_ref_values.len(), 1024);
        for raw_ref in raw_ref_values {
            let start = raw_refs.byte_len();
            raw_refs.bytes.extend_from_slice(&raw_ref);
            raw_refs.push_current_value_span(start);
        }

        let processed = process_provider_ref_raw_batch(
            &raw_refs,
            &mut provider_map,
            &mut sinks,
            &dedupe,
            false,
        )
        .unwrap();

        assert_eq!(processed, 3);
        assert_eq!(provider_map.len(), 3);
        let key_7 = ProviderRefKey::from("7");
        let key_8 = ProviderRefKey::from("8");
        let wide_key = ProviderRefKey::from("121591448686103182592848195376305442061");
        assert!(provider_map.contains_key(&key_7));
        assert!(provider_map.contains_key(&key_8));
        assert!(provider_map.contains_key(&wide_key));
        assert_eq!(provider_map[&key_7].provider_count, 2);
        assert_eq!(provider_map[&key_8].provider_count, 1);
        assert_eq!(provider_map[&wide_key].provider_count, 1);
        assert_eq!(provider_map[&key_7].npi, vec![1234567890, 1234567891]);
        assert!(!provider_map[&key_7].provider_group_hashes.is_empty());
    }

    fn assert_worker_handles_mixed_referenced_and_inline_rates(group_rates: bool) {
        let mode = if group_rates { "grouped" } else { "ungrouped" };
        let base = std::env::temp_dir().join(format!(
            "ptg2-worker-inline-provider-groups-{}-{mode}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let compact_path = base.join("serving.copy");
        let member_path = base.join("provider-group-member.copy");
        let manifest_member_path = base.join("manifest-provider-group-member.copy");
        let paths = CopyPathConfig {
            provider_group_member: Some(member_path.display().to_string()),
            manifest_provider_group_member: Some(manifest_member_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let referenced_provider = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "111111111"},
                "npi": [1111111111]
            }]
        });
        let inline_group = json!({
            "tin": {"type": "ein", "value": "222222222"},
            "npi": [2222222222_i64, 3333333333_i64]
        });
        let mut provider_map = HashMap::new();
        provider_map.insert(
            ProviderRefKey::from("top-level-ref"),
            build_provider_entry(&referenced_provider).unwrap(),
        );
        let rates = vec![
            RateLite {
                provider_refs: vec![ProviderRefKey::from("top-level-ref")],
                provider_groups: Vec::new(),
                provider_groups_raw: None,
                network_names: Vec::new(),
                prices: vec![test_price_lite("100.00")],
                prepared_price_set: None,
            },
            RateLite {
                provider_refs: Vec::new(),
                provider_groups: vec![inline_group.clone()],
                provider_groups_raw: None,
                network_names: Vec::new(),
                prices: vec![test_price_lite("101.00")],
                prepared_price_set: None,
            },
            RateLite {
                provider_refs: vec![ProviderRefKey::from("top-level-ref")],
                provider_groups: vec![inline_group.clone()],
                provider_groups_raw: None,
                network_names: Vec::new(),
                prices: vec![test_price_lite("102.00")],
                prepared_price_set: None,
            },
            RateLite {
                provider_refs: Vec::new(),
                provider_groups: vec![inline_group.clone()],
                provider_groups_raw: None,
                network_names: Vec::new(),
                prices: vec![test_price_lite("101.00")],
                prepared_price_set: None,
            },
        ];
        let procedure = json!({
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "name": "Office visit"
        });
        let mut writer = Vec::new();
        let mut compact_copy_writer =
            Some(CompactCopySink::new_file(compact_path.display().to_string(), 0).unwrap());
        let mut manifest_serving_copy_writer = None;
        let mut dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let manifest_sidecars = Arc::new(Mutex::new(ManifestSidecarCollector::default()));
        let dedupe = SharedDedupe::new(2);
        let mut worker_dedupe_cache = WorkerDedupeCache::new(16);
        let mut provider_set_scope_cache = ProviderSetScopeCache::default();
        let mut manifest_global_id_cache = ManifestGlobalIdCache::default();
        let context = test_compact_context();

        {
            let mut state = SharedCompactState {
                writer: &mut writer,
                compact_copy_writer: &mut compact_copy_writer,
                manifest_serving_copy_writer: &mut manifest_serving_copy_writer,
                dictionary_copy_sinks: &mut dictionary_copy_sinks,
                manifest_sidecars: Some(Arc::clone(&manifest_sidecars)),
                record_price_forward_sidecar: false,
                suppress_legacy_row_output: false,
                provider_map: &provider_map,
                dedupe: &dedupe,
                worker_dedupe_cache: &mut worker_dedupe_cache,
                provider_set_scope_cache: &mut provider_set_scope_cache,
                manifest_global_id_cache: &mut manifest_global_id_cache,
                context: &context,
            };
            process_compact_rate_lites_worker_with_grouping(
                &mut state,
                &rates,
                &procedure,
                group_rates,
            )
            .unwrap();
        }

        let sink = compact_copy_writer.take().unwrap();
        sink.finish(&mut writer).unwrap();
        dictionary_copy_sinks.finish_silent().unwrap();

        assert_eq!(
            std::fs::read_to_string(&compact_path)
                .unwrap()
                .lines()
                .count(),
            3
        );
        let inline_tin = inline_group.get("tin").unwrap();
        let inline_npis = strict_npi_list(inline_group.get("npi")).unwrap();
        let inline_group_hash = provider_group_hash(inline_tin, &inline_npis, &[], &[]);
        let member_rows = std::fs::read_to_string(&member_path).unwrap();
        assert_eq!(member_rows.lines().count(), 2);
        assert!(member_rows
            .lines()
            .all(|line| line.starts_with(&format!("{inline_group_hash}\t"))));
        let inline_group_id = provider_group_global_id_from_hash(inline_group_hash);
        let manifest_member_rows = std::fs::read_to_string(&manifest_member_path).unwrap();
        assert_eq!(manifest_member_rows.lines().count(), 2);
        assert!(manifest_member_rows
            .lines()
            .all(|line| line.starts_with(&format!("{}\t", inline_group_id.to_hex()))));

        let summary = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(summary["provider_group_attempted"], 3);
        assert_eq!(summary["provider_group_unique"], 1);
        assert_eq!(summary["provider_group_duplicate"], 2);
        assert_eq!(summary["provider_group_member_unique"], 2);

        let mut sidecars = manifest_sidecars.lock().unwrap();
        let provider_forward_entries = sidecars.provider_forward_entries().unwrap();
        assert!(provider_forward_entries
            .iter()
            .any(|entry| entry.members.contains(&inline_group_id)));
        let referenced_group = referenced_provider["provider_groups"][0].clone();
        let referenced_group_id = provider_group_global_id_from_hash(provider_group_hash(
            referenced_group.get("tin").unwrap(),
            &strict_npi_list(referenced_group.get("npi")).unwrap(),
            &[],
            &[],
        ));
        assert!(provider_forward_entries.iter().any(|entry| {
            entry.members.contains(&inline_group_id) && entry.members.contains(&referenced_group_id)
        }));
        let provider_npi_entries = sidecars.provider_npi_entries().unwrap();
        for npi in inline_npis {
            assert!(provider_npi_entries
                .iter()
                .any(|entry| entry.members.contains(&npi_member_id(npi))));
        }
        assert!(provider_npi_entries
            .iter()
            .all(|entry| !entry.members.contains(&npi_member_id(0))));
        assert!(sidecars.price_forward_entries().unwrap().is_empty());
        drop(sidecars);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn worker_handles_mixed_top_level_references_and_inline_provider_groups() {
        assert_worker_handles_mixed_referenced_and_inline_rates(false);
    }

    #[test]
    fn v4_inline_empty_npi_rate_preserves_price_and_group_without_npi_edges() {
        let raw_rate = br#"{
            "provider_groups":[{
                "tin":{"type":"ein","value":"444444444"},
                "npi":[]
            }],
            "negotiated_prices":[{"negotiated_rate":103.00}]
        }"#;
        assert!(read_rate_lite_bytes_profiled_with_policy(raw_rate, false).is_err());
        let (rate, _typed) = read_rate_lite_bytes_profiled_with_policy(raw_rate, true).unwrap();
        let rate = rate.unwrap();
        let price_set = rate_price_set(&rate).unwrap();
        assert_eq!(price_set.atoms.len(), 1);
        assert_eq!(price_set.atoms[0].negotiated_rate, "103");

        let mut dictionary_sinks =
            DictionaryCopySinks::from_paths(&CopyPathConfig::default(), 0).unwrap();
        let dedupe = v4_test_shared_dedupe(2);
        let mut provider_cache = ProviderSetScopeCache::with_v4_factor_mode(true);
        let provider_map = HashMap::new();
        let resolved = resolve_worker_provider(
            &provider_map,
            &rate,
            &mut dictionary_sinks,
            &dedupe,
            &mut provider_cache,
            &test_compact_context(),
        )
        .unwrap()
        .unwrap();

        assert!(resolved.is_v4_factor());
        assert_eq!(resolved.provider_count(), 0);
        assert_eq!(resolved.emitted_npis().len(), 0);
        let (component_hash, group_hashes) = resolved.inline_component().unwrap();
        assert_ne!(component_hash, 0);
        assert_eq!(group_hashes.len(), 1);
        assert_eq!(
            group_hashes[0],
            provider_group_hash(&resolved.inline_provider_groups()[0]["tin"], &[], &[], &[])
        );
        assert_eq!(dedupe.empty_npi_tin_only_normalization_count(), 1);
        let dedupe_summary = dedupe_summary_payload(&dedupe, &HashMap::new());
        assert_eq!(dedupe_summary["provider_group_attempted"], 1);
        assert_eq!(dedupe_summary["provider_group_unique"], 1);
        assert_eq!(dedupe_summary["provider_group_member_attempted"], 0);
    }

    #[test]
    fn mixed_valid_and_dangling_provider_refs_fail_serial_and_worker_paths() {
        let provider_ref = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890, "bad`"]
            }]
        });
        let mut provider_map = HashMap::new();
        provider_map.insert(
            ProviderRefKey::from("valid-ref"),
            build_provider_entry(&provider_ref).unwrap(),
        );
        let rates = vec![RateLite {
            provider_refs: vec![
                ProviderRefKey::from("valid-ref"),
                ProviderRefKey::from("dangling-ref"),
            ],
            provider_groups: provider_ref["provider_groups"].as_array().unwrap().clone(),
            provider_groups_raw: None,
            network_names: Vec::new(),
            prices: vec![test_price_lite("100.00")],
            prepared_price_set: None,
        }];
        let procedure = json!({"billing_code_type": "CPT", "billing_code": "99213"});
        let paths = CopyPathConfig::default();

        let mut serial_writer = Vec::new();
        let mut serial_compact_copy_writer = None;
        let mut serial_manifest_serving_copy_writer = None;
        let mut serial_dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let mut emitted_price_code_sets = HashSet::new();
        let mut emitted_price_atoms = HashSet::new();
        let mut emitted_price_sets = HashSet::new();
        let mut emitted_price_set_entries = HashSet::new();
        let mut emitted_provider_sets = HashSet::new();
        let mut emitted_provider_set_components = HashSet::new();
        let mut emitted_provider_set_entries = HashSet::new();
        let mut emitted_provider_entry_components = HashSet::new();
        let mut emitted_procedures = HashSet::new();
        let mut emitted_provider_group_members = HashSet::new();
        let mut provider_identifier_quarantine = ProviderIdentifierQuarantine::default();
        let mut serial_manifest_global_id_cache = ManifestGlobalIdCache::default();
        let context = test_compact_context();
        let mut outputs = LocalCompactOutputs {
            writer: &mut serial_writer,
            compact_copy_writer: &mut serial_compact_copy_writer,
            manifest_serving_copy_writer: &mut serial_manifest_serving_copy_writer,
            dictionary_copy_sinks: &mut serial_dictionary_copy_sinks,
            manifest_sidecars: None,
            record_price_forward_sidecar: false,
            suppress_legacy_row_output: false,
        };
        let mut serial_dedupe = LocalCompactDedupe {
            price_code_sets: &mut emitted_price_code_sets,
            price_atoms: &mut emitted_price_atoms,
            price_sets: &mut emitted_price_sets,
            price_set_entries: &mut emitted_price_set_entries,
            provider_sets: &mut emitted_provider_sets,
            provider_set_components: &mut emitted_provider_set_components,
            provider_set_entries: &mut emitted_provider_set_entries,
            provider_entry_components: &mut emitted_provider_entry_components,
            procedures: &mut emitted_procedures,
            provider_group_members: &mut emitted_provider_group_members,
            provider_identifier_quarantine: &mut provider_identifier_quarantine,
        };
        let mut batch = CompactRateBatch {
            provider_map: &provider_map,
            manifest_global_id_cache: &mut serial_manifest_global_id_cache,
            rates: &rates,
            procedure_value: &procedure,
            context: &context,
        };
        let serial_error =
            process_compact_rate_lites(&mut outputs, &mut serial_dedupe, &mut batch).unwrap_err();

        assert_eq!(serial_error.kind(), io::ErrorKind::InvalidData);
        assert!(serial_error.to_string().contains("dangling-ref"));
        assert!(!String::from_utf8(serial_writer)
            .unwrap()
            .contains("serving_rate_compact"));

        let mut worker_writer = Vec::new();
        let mut worker_compact_copy_writer = None;
        let mut worker_manifest_serving_copy_writer = None;
        let mut worker_dictionary_copy_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let worker_dedupe = SharedDedupe::new(1);
        let mut worker_dedupe_cache = WorkerDedupeCache::new(16);
        let mut worker_provider_set_scope_cache = ProviderSetScopeCache::default();
        let mut worker_manifest_global_id_cache = ManifestGlobalIdCache::default();
        let mut worker_state = SharedCompactState {
            writer: &mut worker_writer,
            compact_copy_writer: &mut worker_compact_copy_writer,
            manifest_serving_copy_writer: &mut worker_manifest_serving_copy_writer,
            dictionary_copy_sinks: &mut worker_dictionary_copy_sinks,
            manifest_sidecars: None,
            record_price_forward_sidecar: false,
            suppress_legacy_row_output: false,
            provider_map: &provider_map,
            dedupe: &worker_dedupe,
            worker_dedupe_cache: &mut worker_dedupe_cache,
            provider_set_scope_cache: &mut worker_provider_set_scope_cache,
            manifest_global_id_cache: &mut worker_manifest_global_id_cache,
            context: &context,
        };
        let worker_error = process_compact_rate_lites_worker_with_grouping(
            &mut worker_state,
            &rates,
            &procedure,
            false,
        )
        .unwrap_err();

        assert_eq!(worker_error.kind(), io::ErrorKind::InvalidData);
        assert!(worker_error.to_string().contains("dangling-ref"));
        assert!(!String::from_utf8(worker_writer)
            .unwrap()
            .contains("serving_rate_compact"));
    }

    #[test]
    fn reversed_top_level_order_imports_referenced_rates_end_to_end() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-reversed-top-level-order-success-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_path = base.join("input.json");
        let serving_path = base.join("serving.copy");
        let serving_run_directory = base.join("serving-runs");
        let price_atom_path = base.join("price-atom.copy");
        let provider_member_path = base.join("provider-group-member.copy");
        write_reversed_provider_reference_fixture(&input_path, &[7]);
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
                serving_path.to_str().unwrap(),
            ),
            TestEnvVar::set(
                "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH",
                price_atom_path.to_str().unwrap(),
            ),
            TestEnvVar::set(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
                provider_member_path.to_str().unwrap(),
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        scan_compact_struson(&input_path).unwrap();

        let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>();
        let code_dictionary_path = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.contains("-codes.v4") && name.ends_with(".ready"))
            })
            .unwrap();
        let code_dictionary = read_code_dictionary(code_dictionary_path).unwrap();
        let price_atom_rows = read_worker_copy_text(&price_atom_path).unwrap();
        let provider_member_rows = read_worker_copy_text(&provider_member_path).unwrap();
        assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(code_dictionary.len(), 1);
        assert_eq!(
            code_dictionary[0].negotiation_arrangement.as_deref(),
            Some("FFS")
        );
        assert!(!serving_path.exists());
        assert_eq!(price_atom_rows.lines().count(), 1);
        assert_eq!(
            price_atom_rows.lines().next().unwrap().split('\t').nth(2),
            Some("123.45")
        );
        assert_eq!(provider_member_rows.lines().count(), 1);
        assert!(provider_member_rows.ends_with("\t1234567890\n"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn scanner_utility_boundaries_are_explicit() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let input_path = Path::new("/tmp/scanner-utility-boundaries.json");
        let _progress_env = [
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "1"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "1"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_INDEXED_RANGE_PRODUCERS", "invalid"),
        ];

        let integer_bytes = [0, 0, 0, 7, 0, 0, 0, 0, 0, 0, 0, 0];
        let mut integer_cursor = Cursor::new(&integer_bytes);
        assert_eq!(read_i32_be(&mut integer_cursor).unwrap(), 7);
        assert_eq!(
            to_io_error(<[u8; 2]>::try_from(&[1u8][..]).unwrap_err()).kind(),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(
            to_io_error("invalid".parse::<u64>().unwrap_err()).kind(),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(
            to_io_error(u8::try_from(256u16).unwrap_err()).kind(),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(
            to_io_error(io::Error::other("coverage")).kind(),
            io::ErrorKind::InvalidData,
        );
        assert_eq!(
            indexed_range_producers_requested().unwrap_err().kind(),
            io::ErrorKind::InvalidInput,
        );
        let mut array_reader = BufferedJsonByteReader::new(&b"]"[..]);
        assert!(!next_array_value(&mut array_reader, &mut true).unwrap());
        let mut wrapped_reader =
            WrappedIndexedRangeReader::new(Box::new(Cursor::new(b"[]".to_vec())), 2, b"", b"");
        drain_reader_to_eof(&mut wrapped_reader).unwrap();
        emit_plain_reorder_progress(input_path, 2, 1, 1, Instant::now(), false);
    }

    #[test]
    fn reversed_top_level_order_quarantines_dangling_rate_end_to_end() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-reversed-top-level-order-dangling-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_path = base.join("input.json");
        let serving_path = base.join("serving.copy");
        let serving_run_directory = base.join("serving-runs");
        write_reversed_provider_reference_fixture(&input_path, &[7, 999]);
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
                serving_path.to_str().unwrap(),
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        scan_compact_struson(&input_path).unwrap();

        let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>();
        let witness_path = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("ptg2-v3-source-witness-"))
            })
            .unwrap();
        let witness_header = source_witness_header(&std::fs::read(witness_path).unwrap());

        assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(
            witness_header["rate_occurrence"]["emitted_rate_row_count"],
            2
        );
        assert_eq!(
            witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
            1,
        );
        assert!(!serving_path.exists());
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn late_procedure_fields_fail_before_partial_rates_can_publish() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base =
            std::env::temp_dir().join(format!("ptg2-late-procedure-field-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_path = base.join("input.json");
        let serving_path = base.join("serving.copy");
        let serving_run_directory = base.join("serving-runs");
        std::fs::write(
            &input_path,
            r#"{
                "provider_references":[{
                    "provider_group_id":7,
                    "provider_groups":[{
                        "tin":{"type":"ein","value":"123456789"},
                        "npi":[1234567890]
                    }]
                }],
                "in_network":[{
                    "negotiated_rates":[{
                        "provider_references":[7],
                        "negotiated_prices":[{
                            "negotiated_type":"negotiated",
                            "negotiated_rate":123.45
                        }]
                    }],
                    "billing_code_type":"CPT",
                    "billing_code":"99213",
                    "negotiation_arrangement":"ffs"
                }]
            }"#,
        )
        .unwrap();
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_SPLIT_NEGOTIATED_RATES", "1"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
                serving_path.to_str().unwrap(),
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        let error = scan_compact_struson(&input_path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("before billing_code_type and billing_code"));
        assert!(!serving_path.exists());
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_price_set_atom_sink_emits_global_id_pairs() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-manifest-price-set-atom-{}.copy",
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
            manifest_price_set_atom: Some(base.to_string_lossy().to_string()),
            manifest_price_set_summary: None,
            manifest_provider_group_member: None,
            manifest_code_count: None,
            manifest_provider_set_dictionary: None,
            procedure: None,
            price_code_set: None,
            price_atom: None,
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let price_set = price_lite_set(&[PriceLite {
            negotiated_type: Some("negotiated".to_string()),
            negotiated_rate: "123.45".to_string(),
            expiration_date: Some("2026-12-31".to_string()),
            service_code: vec!["11".to_string()],
            billing_class: Some("professional".to_string()),
            setting: None,
            billing_code_modifier: vec![],
            additional_information: None,
        }])
        .unwrap();

        sinks.write_manifest_price_set_atoms(&price_set).unwrap();
        sinks
            .write_price_atoms(&price_set.atoms, &mut HashSet::new(), &mut HashSet::new())
            .unwrap();
        sinks.write_price_code_set("unused", &[]).unwrap();
        sinks
            .write_price_set_entries(GlobalId128([0; GLOBAL_ID_BYTES]), &[], &mut HashSet::new())
            .unwrap();
        sinks
            .write_provider_set_entries("set", &[11], &mut HashSet::new())
            .unwrap();
        sinks
            .write_provider_set_components("set", &[12], &mut HashSet::new())
            .unwrap();
        sinks
            .write_provider_entry_components(11, &[12], &mut HashSet::new())
            .unwrap();
        sinks
            .write_provider_group_members(&Value::Null, &mut HashSet::new())
            .unwrap();
        let events = sinks.finish_silent().unwrap();
        let body = std::fs::read_to_string(&base).unwrap();
        let expected_price_set_id = price_set_global_id(&price_set).to_hex();
        let expected_atom_id = price_atom_global_id(&price_set.atoms[0]).to_hex();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].record_kind, "manifest_price_set_atom_copy_file");
        assert_eq!(events[0].row_count, 1);
        assert_eq!(
            body,
            format!("{expected_price_set_id}\t{expected_atom_id}\n")
        );
        let _ = std::fs::remove_file(base);
    }

    #[test]
    fn manifest_price_set_summary_sink_emits_exact_minimum() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-manifest-price-set-summary-{}.copy",
            std::process::id()
        ));
        let paths = CopyPathConfig {
            manifest_price_set_summary: Some(base.to_string_lossy().to_string()),
            manifest_only: true,
            ..CopyPathConfig::default()
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let prices = ["10", "2", "-2", "-10", "0.0000000000000000001"]
            .into_iter()
            .map(|negotiated_rate| PriceLite {
                negotiated_type: Some("negotiated".to_string()),
                negotiated_rate: negotiated_rate.to_string(),
                expiration_date: None,
                service_code: vec![],
                billing_class: None,
                setting: None,
                billing_code_modifier: vec![],
                additional_information: None,
            })
            .collect::<Vec<_>>();
        let price_set = price_lite_set(&prices).unwrap();

        assert_eq!(price_set.minimum_negotiated_rate(), "-10");
        sinks.write_manifest_price_set_summary(&price_set).unwrap();
        let events = sinks.finish_silent().unwrap();
        let expected_price_set_id = price_set_global_id(&price_set).to_hex();

        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].record_kind,
            "manifest_price_set_summary_copy_file"
        );
        assert_eq!(events[0].row_count, 1);
        assert_eq!(
            std::fs::read_to_string(&base).unwrap(),
            format!("{expected_price_set_id}\t-10\n")
        );
        let _ = std::fs::remove_file(base);
    }

    #[test]
    fn raw_worker_parsers_reject_invalid_utf8_values() {
        let mut raw_rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100,"additional_information":"A"#.to_vec();
        raw_rate.push(0xff);
        raw_rate.extend_from_slice(br#"B"}]}"#);
        assert!(read_rate_lite_bytes(&raw_rate).is_err());

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
            price_set_entry: None,
            provider_set: None,
            provider_set_component: None,
            provider_set_entry: None,
            provider_entry_component: None,
            provider_group_member: None,
            manifest_only: true,
        };
        let mut sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let dedupe = SharedDedupe::new(1);
        let mut provider_map = HashMap::new();
        let mut raw_ref = br#"{"provider_group_id":9,"provider_groups":[{"tin":{"type":"ein","value":"123456789"},"npi":[1234567890],"bad":"A"#.to_vec();
        raw_ref.push(0xff);
        raw_ref.extend_from_slice(br#"B"}]}"#);
        let mut raw_refs = RawRateChunk::with_capacity(1, raw_ref.len());
        let start = raw_refs.byte_len();
        raw_refs.bytes.extend_from_slice(&raw_ref);
        raw_refs.push_current_value_span(start);

        let error = process_provider_ref_raw_batch(
            &raw_refs,
            &mut provider_map,
            &mut sinks,
            &dedupe,
            false,
        )
        .unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(provider_map.is_empty());
    }

    fn assert_normal_order_suffix_rejected(
        suffix_label: &str,
        suffix: &str,
        parse_in_workers: bool,
        expected_error: &str,
    ) {
        let base = std::env::temp_dir().join(format!(
            "ptg2-normal-suffix-{suffix_label}-{}-{}",
            parse_in_workers,
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let input_path = base.join("input.json");
        let serving_run_directory = base.join("serving-runs");
        let provider_reference = valid_provider_reference();
        std::fs::write(
            &input_path,
            format!(r#"{{"provider_references":[{provider_reference}],"in_network":[]{suffix}"#,),
        )
        .unwrap();
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS",
                if parse_in_workers { "true" } else { "false" },
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        let error = scan_compact_struson(&input_path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains(expected_error), "{error}");
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn normal_order_byte_scan_rejects_duplicate_rate_array_suffix() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        assert_normal_order_suffix_rejected(
            "duplicate-array",
            r#", "in_network":[]}"#,
            true,
            "duplicate PTG top-level array",
        );
    }

    #[test]
    fn normal_order_byte_scan_rejects_malformed_json_suffix() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        assert_normal_order_suffix_rejected(
            "malformed-json",
            r#", "invalid":not_json}"#,
            true,
            "JSON",
        );
    }

    #[test]
    fn normal_order_parallel_byte_scan_accepts_identical_provider_definitions() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-normal-identical-provider-definitions-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let input_path = base.join("input.json");
        let serving_path = base.join("serving.copy");
        let serving_run_directory = base.join("serving-runs");
        let provider_member_path = base.join("provider-group-member.copy");
        let provider_reference = valid_provider_reference();
        std::fs::write(
            &input_path,
            format!(
                r#"{{
                    "provider_references":[{provider_reference},{provider_reference}],
                    "in_network":[{{
                        "billing_code_type":"CPT",
                        "billing_code":"99213",
                        "negotiation_arrangement":"ffs",
                        "negotiated_rates":[{{
                            "provider_references":[7],
                            "negotiated_prices":[{{
                                "negotiated_type":"negotiated",
                                "negotiated_rate":123.45
                            }}]
                        }}]
                    }}]
                }}"#
            ),
        )
        .unwrap();
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORK_QUEUE", "8"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REF_CHUNK_ITEMS", "1"),
            TestEnvVar::set("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "2"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
                serving_path.to_str().unwrap(),
            ),
            TestEnvVar::set(
                "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH",
                provider_member_path.to_str().unwrap(),
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        scan_compact_struson(&input_path).unwrap();

        let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>();
        let provider_member_rows = read_worker_copy_text(&provider_member_path).unwrap();
        assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(provider_member_rows.lines().count(), 1);
        assert!(provider_member_rows.ends_with("\t1234567890\n"));
        assert!(!serving_path.exists());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn normal_order_parallel_byte_scan_quarantines_empty_code_rows() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base =
            std::env::temp_dir().join(format!("ptg2-normal-producer-error-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let input_path = base.join("input.json");
        let serving_path = base.join("serving.copy");
        let serving_run_directory = base.join("serving-runs");
        let provider_reference = valid_provider_reference();
        std::fs::write(
            &input_path,
            format!(
                r#"{{
                    "provider_references":[{provider_reference}],
                    "in_network":[
                        {{
                            "billing_code_type":"CPT",
                            "billing_code":"99213",
                            "negotiation_arrangement":"ffs",
                            "negotiated_rates":[{{
                                "provider_references":[7],
                                "negotiated_prices":[{{
                                    "negotiated_type":"negotiated",
                                    "negotiated_rate":123.45
                                }}]
                            }}]
                        }},
                        {{
                            "billing_code_type":"CPT",
                            "billing_code":"",
                            "negotiation_arrangement":"ffs",
                            "negotiated_rates":[{{
                                "provider_references":[7],
                                "negotiated_prices":[{{
                                    "negotiated_type":"negotiated",
                                    "negotiated_rate":999.99
                                }}]
                            }}]
                        }}
                    ]
                }}"#
            ),
        )
        .unwrap();
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORKERS", "2"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_WORK_QUEUE", "8"),
            TestEnvVar::set("HLTHPRT_PTG2_V3_SERVING_RUN_PARTITIONS", "2"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PARSE_IN_WORKERS", "true"),
            TestEnvVar::set(
                "HLTHPRT_PTG2_COMPACT_SERVING_COPY_PATH",
                serving_path.to_str().unwrap(),
            ),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        scan_compact_struson(&input_path).unwrap();

        let serving_run_bytes = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().contains("-partition-"))
            .map(|entry| entry.metadata().unwrap().len())
            .sum::<u64>();
        let witness_path = std::fs::read_dir(&serving_run_directory)
            .unwrap()
            .filter_map(Result::ok)
            .map(|entry| entry.path())
            .find(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with("ptg2-v3-source-witness-"))
            })
            .unwrap();
        let witness_header = source_witness_header(&std::fs::read(witness_path).unwrap());

        assert_eq!(serving_run_bytes, SERVING_RUN_RECORD_BYTES as u64);
        assert_eq!(
            witness_header["rate_occurrence"]["emitted_rate_row_count"],
            2
        );
        assert_eq!(
            witness_header["rate_occurrence"]["unqueryable_rate_row_count"],
            1,
        );
        assert!(!serving_path.exists());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn compact_byte_scan_rejects_invalid_utf8_outside_captured_values() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-byte-scan-invalid-utf8-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_path = base.join("input.json");
        let serving_run_directory = base.join("serving-runs");
        let provider_ref = serde_json::to_vec(&valid_provider_reference()).unwrap();
        let mut payload = br#"{"provider_references":["#.to_vec();
        payload.extend_from_slice(&provider_ref);
        payload.extend_from_slice(br#"],"ignored":"A"#);
        payload.push(0xff);
        payload.extend_from_slice(br#"B","in_network":[]}"#);
        std::fs::write(&input_path, payload).unwrap();
        let _strict_env = strict_scan_env(&serving_run_directory);
        let _env = [
            TestEnvVar::set("HLTHPRT_PTG2_RUST_TOP_LEVEL_BYTE_SCAN", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_PROVIDER_REFS_IN_WORKERS", "true"),
            TestEnvVar::set("HLTHPRT_PTG2_RUST_RAPIDGZIP_ENABLED", "false"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_BYTES", "0"),
            TestEnvVar::set("HLTHPRT_PTG2_SCANNER_PROGRESS_OBJECTS", "0"),
        ];

        let error = scan_compact_struson(&input_path).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("invalid UTF-8"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn capture_value_bytes_into_reuses_scratch_buffer() {
        let input = br#" {"a":[1,{"b":"c"}]} "tail" "#;
        let mut reader = BufferedJsonByteReader::new(&input[..]);
        let mut scratch = Vec::with_capacity(64);

        reader.capture_value_bytes_into(&mut scratch).unwrap();
        let retained_capacity = scratch.capacity();
        assert_eq!(scratch, br#"{"a":[1,{"b":"c"}]}"#);

        reader.capture_value_bytes_into(&mut scratch).unwrap();

        assert_eq!(scratch, br#""tail""#);
        assert_eq!(scratch.capacity(), retained_capacity);

        let deep = format!("{}0{}", "[".repeat(65), "]".repeat(65));
        let mut deep_reader = BufferedJsonByteReader::new(deep.as_bytes());
        deep_reader.capture_value_bytes_into(&mut scratch).unwrap();
        assert_eq!(scratch, deep.as_bytes());

        let mut mismatched_reader = BufferedJsonByteReader::new(&b"x"[..]);
        let mismatch = mismatched_reader.expect_byte(b'y').unwrap_err();
        assert_eq!(mismatch.kind(), io::ErrorKind::InvalidData);
        assert!(mismatch.to_string().contains("expected JSON byte"));

        let mut empty_reader = BufferedJsonByteReader::new(&b""[..]);
        let missing = empty_reader.expect_byte(b'y').unwrap_err();
        assert_eq!(missing.kind(), io::ErrorKind::UnexpectedEof);
    }

    fn capture_array_objects(input: &[u8]) -> io::Result<Vec<Vec<u8>>> {
        let mut reader = BufferedJsonByteReader::new(input);
        let mut captured = Vec::new();
        let mut objects = Vec::new();
        let mut first = true;
        reader.expect_byte(b'[')?;
        loop {
            let start = captured.len();
            if !reader.capture_next_array_object_bytes_append(&mut captured, &mut first)? {
                break;
            }
            objects.push(captured[start..].to_vec());
        }
        Ok(objects)
    }

    #[test]
    fn fused_array_object_capture_handles_compact_and_whitespace_json() {
        let compact = br#"[{"a":1},{"b":[2,{"c":3}]}]"#;
        let whitespace = br#" [
            { "a": 1 },
            { "b": [2, {"c": 3}] }
        ] "#;

        let compact_values = capture_array_objects(compact)
            .unwrap()
            .into_iter()
            .map(|raw| serde_json::from_slice::<Value>(&raw).unwrap())
            .collect::<Vec<_>>();
        let whitespace_values = capture_array_objects(whitespace)
            .unwrap()
            .into_iter()
            .map(|raw| serde_json::from_slice::<Value>(&raw).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(
            compact_values,
            vec![json!({"a": 1}), json!({"b": [2, {"c": 3}]})]
        );
        assert_eq!(whitespace_values, compact_values);
        assert!(capture_array_objects(b"[]").unwrap().is_empty());
    }

    #[test]
    fn fused_array_object_capture_preserves_escaped_quotes_and_delimiters() {
        let input =
            br#"[{"message":"escaped quote: \" and slash: \\ and delimiters: } ] {"},{"tail":2}]"#;

        let objects = capture_array_objects(input).unwrap();

        assert_eq!(objects.len(), 2);
        assert_eq!(
            serde_json::from_slice::<Value>(&objects[0]).unwrap(),
            json!({"message": "escaped quote: \" and slash: \\ and delimiters: } ] {"})
        );
        assert_eq!(
            serde_json::from_slice::<Value>(&objects[1]).unwrap(),
            json!({"tail": 2})
        );
    }

    #[test]
    fn fused_array_object_capture_rejects_malformed_delimiters_and_eof() {
        fn capture_error(input: &[u8]) -> io::Error {
            let mut reader = BufferedJsonByteReader::new(input);
            let mut captured = Vec::new();
            let mut first = true;
            reader.expect_byte(b'[').unwrap();
            loop {
                match reader.capture_next_array_object_bytes_append(&mut captured, &mut first) {
                    Ok(true) => {}
                    Ok(false) => panic!("malformed input unexpectedly completed"),
                    Err(error) => return error,
                }
            }
        }

        for input in [
            br#"[{"a":1} {"b":2}]"#.as_slice(),
            br#"[{"a":1},,{"b":2}]"#.as_slice(),
            br#"[{"a":1},]"#.as_slice(),
            br#"[null]"#.as_slice(),
        ] {
            let error = capture_error(input);
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{input:?}");
        }

        let missing_array_end = capture_error(br#"[{"a":1}"#);
        assert_eq!(missing_array_end.kind(), io::ErrorKind::UnexpectedEof);
        let unterminated_object = capture_error(br#"[{"message":"unterminated}"#);
        assert_eq!(unterminated_object.kind(), io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn capture_nested_values_handles_fragmented_escapes_and_delimiters() {
        struct FragmentedReader<'a> {
            bytes: &'a [u8],
            offset: usize,
        }

        impl Read for FragmentedReader<'_> {
            fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
                let Some(byte) = self.bytes.get(self.offset).copied() else {
                    return Ok(0);
                };
                buffer[0] = byte;
                self.offset += 1;
                Ok(1)
            }
        }

        let first = br#"{"message":"escaped quote: \" and slash: \\ and delimiters: } ]","nested":[{"value":"x"}]}"#;
        let second = br#"[1,{"value":"tail"}]"#;
        let mut input = Vec::new();
        input.extend_from_slice(first);
        input.push(b' ');
        input.extend_from_slice(second);
        let fragmented = FragmentedReader {
            bytes: &input,
            offset: 0,
        };
        let mut reader = BufferedJsonByteReader::new(fragmented);
        let mut captured = Vec::new();

        reader.capture_value_bytes_into(&mut captured).unwrap();
        assert_eq!(captured, first);
        reader.capture_value_bytes_into(&mut captured).unwrap();
        assert_eq!(captured, second);

        let fragmented = FragmentedReader {
            bytes: &input,
            offset: 0,
        };
        let mut reader = BufferedJsonByteReader::new(fragmented);
        captured.clear();
        reader.capture_object_bytes_append(&mut captured).unwrap();
        assert_eq!(captured, first);

        let direct: &[u8] = first;
        let mut direct_reader = BufferedJsonByteReader::new(direct);
        captured.clear();
        direct_reader
            .capture_object_bytes_append(&mut captured)
            .unwrap();
        assert_eq!(captured, first);
    }

    #[test]
    fn provider_source_capture_rejects_the_byte_past_the_record_limit() {
        validate_provider_source_capture_append(PROVIDER_SOURCE_RECORD_MAX_BYTES - 1, 0).unwrap();
        let error = validate_provider_source_capture_append(PROVIDER_SOURCE_RECORD_MAX_BYTES, 0)
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error
            .to_string()
            .contains("provider source record is 67108865 bytes"));
        assert!(validate_provider_source_capture_append(0, 1).is_err());
    }

    #[test]
    fn raw_byte_scan_reuses_final_chunk_at_next_object_boundary() {
        let rate = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_rate":100}]}"#;
        let object = format!(
            r#"{{"billing_code_type":"CPT","billing_code":"99213","negotiated_rates":[{}]}}"#,
            std::str::from_utf8(rate).unwrap()
        );
        let payload = format!("{object}\n{object}");
        let mut reader = BufferedJsonByteReader::new(payload.as_bytes());
        let (tx, rx) = bounded::<WorkerJob>(4);
        let (_event_tx, event_rx) = bounded::<CopyFileEvent>(4);
        let mut writer = Vec::new();
        let mut producer_blocked_micros = 0u128;
        let mut stats = RawChunkStats::default();
        let mut copy_file_event_gate = CopyFileEventGate::passthrough();
        let recycle_tx = stats.enable_recycling(2);

        for object_ordinal in 0..2 {
            let mut enqueue_io = InNetworkEnqueueIo {
                tx: &tx,
                event_rx: &event_rx,
                writer: &mut writer,
                copy_file_event_gate: &mut copy_file_event_gate,
                cancelled: None,
                producer_blocked_micros: &mut producer_blocked_micros,
                raw_chunk_stats: &mut stats,
            };
            let rate_count = enqueue_in_network_raw_byte_scan(
                &mut reader,
                &mut enqueue_io,
                InNetworkEnqueueOptions {
                    chunk_size: 8,
                    raw_chunk_byte_limit: 4 * 1024,
                    parse_in_workers: true,
                    object_ordinal,
                },
            )
            .unwrap();
            assert_eq!(rate_count, 1);

            let mut raw_rates = match rx.recv().unwrap() {
                WorkerJob::RawRates { raw_rates, .. } => raw_rates,
                WorkerJob::Rates { .. } => panic!("raw byte scan emitted parsed rates"),
            };
            let captured = raw_rates
                .iter_with_coordinates()
                .map(|(coordinate, raw)| (coordinate, raw.to_vec()))
                .collect::<Vec<_>>();
            assert_eq!(
                captured,
                vec![(
                    SourceWitnessCoordinate::new(object_ordinal, 0),
                    rate.to_vec()
                )]
            );
            stats.queue_bytes.finish_receive(raw_rates.byte_len());
            if object_ordinal == 0 {
                raw_rates.clear_for_recycle();
                assert!(recycle_tx.try_send(raw_rates).is_ok());
            }
        }

        assert_eq!(stats.chunk_count, 2);
        assert_eq!(stats.buffer_allocations, 1);
        assert_eq!(stats.buffer_reuses, 1);
    }

    #[test]
    fn capture_nested_values_rejects_mismatched_delimiters() {
        let mut reader = BufferedJsonByteReader::new(br#"{"items":[1,2}}"#.as_slice());
        let mut captured = Vec::new();

        let error = reader.capture_value_bytes_into(&mut captured).unwrap_err();

        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("mismatched JSON delimiter"));
    }

    #[test]
    #[ignore = "manual byte-framing throughput probe"]
    fn benchmark_nested_value_capture_throughput() {
        fn capture(input: &[u8], mode: &str) -> (Duration, usize, usize) {
            let mut reader = BufferedJsonByteReader::new(input);
            let mut captured = Vec::with_capacity(input.len());
            let mut first = true;
            let mut captured_values = 0usize;
            reader.expect_byte(b'[').unwrap();
            let started_at = Instant::now();
            loop {
                if mode == "fused" {
                    if !reader
                        .capture_next_array_object_bytes_append(&mut captured, &mut first)
                        .unwrap()
                    {
                        break;
                    }
                } else {
                    if !next_array_value(&mut reader, &mut first).unwrap() {
                        break;
                    }
                    if mode == "object" {
                        reader.capture_object_bytes_append(&mut captured).unwrap();
                    } else {
                        reader.capture_value_bytes_append(&mut captured).unwrap();
                    }
                }
                captured_values += 1;
            }
            (started_at.elapsed(), captured_values, captured.len())
        }

        const TARGET_BYTES: usize = 128 * 1024 * 1024;
        const RATE: &[u8] = br#"{"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":123.45,"service_code":["11"],"billing_class":"professional","additional_information":"quoted \"text\" and delimiters } ]"}]}"#;
        serde_json::from_slice::<Value>(RATE).unwrap();
        let rate_count = TARGET_BYTES / (RATE.len() + 1);
        let mut input = Vec::with_capacity(rate_count * (RATE.len() + 1) + 2);
        input.push(b'[');
        for index in 0..rate_count {
            if index > 0 {
                input.push(b',');
            }
            input.extend_from_slice(RATE);
        }
        input.push(b']');

        let (baseline_elapsed, baseline_values, baseline_bytes) = capture(&input, "generic");
        let (fast_elapsed, fast_values, fast_bytes) = capture(&input, "object");
        let (fused_elapsed, fused_values, fused_bytes) = capture(&input, "fused");
        let baseline_mib_per_second =
            input.len() as f64 / (1024.0 * 1024.0) / baseline_elapsed.as_secs_f64();
        let fast_mib_per_second =
            input.len() as f64 / (1024.0 * 1024.0) / fast_elapsed.as_secs_f64();
        let fused_mib_per_second =
            input.len() as f64 / (1024.0 * 1024.0) / fused_elapsed.as_secs_f64();

        eprintln!(
            "captured {} values / {:.1} MiB: baseline {:.3}s ({:.1} MiB/s), object fast path {:.3}s ({:.1} MiB/s), fused object path {:.3}s ({:.1} MiB/s), fused speedup {:.3}x",
            fused_values,
            input.len() as f64 / (1024.0 * 1024.0),
            baseline_elapsed.as_secs_f64(),
            baseline_mib_per_second,
            fast_elapsed.as_secs_f64(),
            fast_mib_per_second,
            fused_elapsed.as_secs_f64(),
            fused_mib_per_second,
            fast_elapsed.as_secs_f64() / fused_elapsed.as_secs_f64(),
        );
        assert_eq!(baseline_values, rate_count);
        assert_eq!(fast_values, rate_count);
        assert_eq!(fused_values, rate_count);
        assert_eq!(baseline_bytes, rate_count * RATE.len());
        assert_eq!(fast_bytes, baseline_bytes);
        assert_eq!(fused_bytes, baseline_bytes);
    }

    #[test]
    fn raw_rate_chunk_iter_returns_contiguous_spans() {
        let mut chunk = RawRateChunk::with_capacity(2, 64);
        let first = chunk.byte_len();
        chunk.bytes.extend_from_slice(br#"{"a":1}"#);
        chunk.push_current_value_span(first);
        let second = chunk.byte_len();
        chunk.bytes.extend_from_slice(br#"{"b":[2,3]}"#);
        chunk.push_current_value_span(second);

        let raw_values: Vec<&[u8]> = chunk.iter().collect();

        assert_eq!(chunk.len(), 2);
        assert_eq!(chunk.byte_len(), br#"{"a":1}{"b":[2,3]}"#.len());
        assert_eq!(
            raw_values,
            vec![br#"{"a":1}"#.as_slice(), br#"{"b":[2,3]}"#.as_slice()]
        );
    }

    #[test]
    fn provider_entry_compact_provider_set_hash_matches_json_payload_hash() {
        let provider_ref = json!({
            "provider_groups": [
                {
                    "tin": {"type": "ein", "value": "12-3456789"},
                    "npi": [1234567891, 1234567890, 1234567890]
                },
                {
                    "tin": {"type": "npi", "value": " 9876543210 "},
                    "npi": [2222222222_i64, 1111111111, 1234567891]
                }
            ]
        });

        let entry = build_provider_entry(&provider_ref).unwrap();
        let mut group_payloads: Vec<Value> = Vec::new();
        for group in provider_ref
            .get("provider_groups")
            .and_then(Value::as_array)
            .unwrap()
        {
            let tin = group.get("tin").unwrap_or(&Value::Null);
            let npi = strict_npi_list(group.get("npi")).unwrap();
            let group_hash = provider_group_hash(tin, &npi, &[], &[]);
            group_payloads.push(json!({
                "provider_group_hash": group_hash,
                "tin_type": normalize_tin_type(tin.get("type")),
                "tin_value": normalize_tin_value(tin.get("value")),
                "npi": npi,
            }));
        }
        group_payloads.sort_by_cached_key(ptg2_scanner::hashing::canonical_json);
        let expected = make_checksum(vec![json!("provider_set"), Value::Array(group_payloads)]);

        assert_eq!(entry.entry_hash, expected);
        assert_eq!(entry.provider_count, 4);
        assert_eq!(
            entry.npi,
            vec![1111111111, 1234567890, 1234567891, 2222222222]
        );
    }

    #[test]
    fn provider_entry_view_borrows_single_refs_and_owns_combined_refs() {
        let provider_ref_a = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "123456789"},
                "npi": [1234567890]
            }]
        });
        let provider_ref_b = json!({
            "provider_groups": [{
                "tin": {"type": "ein", "value": "987654321"},
                "npi": [1234567891]
            }]
        });
        let mut provider_map = HashMap::new();
        provider_map.insert(
            ProviderRefKey::from("1"),
            build_provider_entry(&provider_ref_a).unwrap(),
        );
        provider_map.insert(
            ProviderRefKey::from("2"),
            build_provider_entry(&provider_ref_b).unwrap(),
        );

        let single = provider_entry_view_from_ref_keys(&provider_map, &[ProviderRefKey::from("1")])
            .unwrap()
            .expect("single ref should resolve");
        assert!(matches!(single, ProviderEntryView::Borrowed(_)));

        let combined = provider_entry_view_from_ref_keys(
            &provider_map,
            &[ProviderRefKey::from("1"), ProviderRefKey::from("2")],
        )
        .unwrap()
        .expect("combined refs should resolve");
        assert!(matches!(combined, ProviderEntryView::Owned(_)));
        assert_eq!(combined.provider_count(), 2);
    }

    fn test_price_id(last_byte: u8) -> [u8; GLOBAL_ID_BYTES] {
        let mut out = [0u8; GLOBAL_ID_BYTES];
        out[GLOBAL_ID_BYTES - 1] = last_byte;
        out
    }

    fn append_pg_binary_field(payload: &mut Vec<u8>, field: &[u8]) {
        payload.extend_from_slice(&(field.len() as i32).to_be_bytes());
        payload.extend_from_slice(field);
    }

    fn append_pg_binary_optional_field(payload: &mut Vec<u8>, field: Option<&[u8]>) {
        match field {
            Some(field) => append_pg_binary_field(payload, field),
            None => payload.extend_from_slice(&(-1i32).to_be_bytes()),
        }
    }

    fn pg_binary_copy_rows(rows: &[Vec<Option<Vec<u8>>>]) -> Vec<u8> {
        let mut payload = Vec::new();
        write_pg_binary_copy_header(&mut payload).unwrap();
        for row in rows {
            payload.extend_from_slice(&(row.len() as i16).to_be_bytes());
            for field in row {
                append_pg_binary_optional_field(&mut payload, field.as_deref());
            }
        }
        write_pg_binary_copy_trailer(&mut payload).unwrap();
        payload
    }

    fn pg_i32_field(value: i32) -> Option<Vec<u8>> {
        Some(value.to_be_bytes().to_vec())
    }

    fn pg_i64_field(value: i64) -> Option<Vec<u8>> {
        Some(value.to_be_bytes().to_vec())
    }

    fn write_v3_finalizer_test_price_key_map(
        base: &Path,
        label: &str,
        price_ids_in_key_order: &[[u8; GLOBAL_ID_BYTES]],
    ) -> PathBuf {
        let rows = price_ids_in_key_order
            .iter()
            .enumerate()
            .map(|(price_key, price_set_id)| (*price_set_id, price_key as i64))
            .collect::<Vec<_>>();
        let copy_rows = rows
            .into_iter()
            .map(|(price_set_id, price_key)| {
                vec![Some(price_set_id.to_vec()), pg_i64_field(price_key)]
            })
            .collect::<Vec<_>>();
        let path = base.join(format!("{label}-price-key-map.copy"));
        std::fs::write(&path, pg_binary_copy_rows(&copy_rows)).unwrap();
        path
    }

    fn pg_binary_numeric(
        negative: bool,
        weight: i16,
        display_scale: i16,
        digits: &[u16],
    ) -> Vec<u8> {
        let mut payload = Vec::with_capacity(8 + digits.len() * 2);
        payload.extend_from_slice(&(digits.len() as i16).to_be_bytes());
        payload.extend_from_slice(&weight.to_be_bytes());
        payload.extend_from_slice(&(if negative { 0x4000u16 } else { 0u16 }).to_be_bytes());
        payload.extend_from_slice(&display_scale.to_be_bytes());
        for digit in digits {
            payload.extend_from_slice(&digit.to_be_bytes());
        }
        payload
    }

    fn pg_v3_price_atom_row(
        atom_key: i32,
        negotiated_rate: Option<Vec<u8>>,
        attribute_keys: [Option<i32>; PTG2_SERVING_BINARY_PRICE_ATOM_V3_ATTRIBUTE_COUNT],
    ) -> Vec<Option<Vec<u8>>> {
        let mut row = vec![pg_i32_field(atom_key), negotiated_rate];
        row.extend(
            attribute_keys
                .into_iter()
                .map(|value| value.map(|key| key.to_be_bytes().to_vec())),
        );
        row
    }

    #[derive(Debug, Eq, PartialEq)]
    struct TestServingBinaryRecord {
        kind: String,
        block_key: i64,
        block_no: i32,
        entry_count: i32,
        payload: Vec<u8>,
        compression: String,
        raw_payload_bytes: i32,
    }

    fn read_test_shared_binary_records(payload: Vec<u8>) -> Vec<TestServingBinaryRecord> {
        let mut reader = Cursor::new(payload);
        read_pg_binary_copy_header(&mut reader).unwrap();
        let mut records = Vec::new();
        while let Some(fields) =
            read_pg_binary_copy_row(&mut reader, 10, "test shared output").unwrap()
        {
            let format_version = pg_binary_i16(
                required_pg_binary_field(&fields, 1, "format_version").unwrap(),
                "format_version",
            )
            .unwrap();
            assert_eq!(format_version, PTG2_V3_SHARED_BLOCK_FORMAT_VERSION);
            let kind =
                std::str::from_utf8(required_pg_binary_field(&fields, 2, "object_kind").unwrap())
                    .unwrap()
                    .to_owned();
            let block_key = pg_binary_nonnegative_i64(
                required_pg_binary_field(&fields, 3, "block_key").unwrap(),
                "block_key",
            )
            .unwrap();
            let block_no = pg_binary_nonnegative_i32(
                required_pg_binary_field(&fields, 4, "fragment_no").unwrap(),
                "fragment_no",
            )
            .unwrap();
            let entry_count = i32::try_from(
                pg_binary_u64(
                    required_pg_binary_field(&fields, 5, "entry_count").unwrap(),
                    "entry_count",
                )
                .unwrap(),
            )
            .unwrap();
            let compression =
                std::str::from_utf8(required_pg_binary_field(&fields, 6, "codec").unwrap())
                    .unwrap()
                    .to_owned();
            let raw_payload_bytes = i32::try_from(
                pg_binary_u64(
                    required_pg_binary_field(&fields, 7, "raw_byte_count").unwrap(),
                    "raw_byte_count",
                )
                .unwrap(),
            )
            .unwrap();
            let stored_payload = required_pg_binary_field(&fields, 9, "payload")
                .unwrap()
                .to_vec();
            let expected_hash =
                shared_v3_block_hash(format_version, &kind, &compression, &stored_payload).unwrap();
            assert_eq!(
                required_pg_binary_field(&fields, 0, "block_hash").unwrap(),
                expected_hash
            );
            let decoded_payload = if compression == "zlib" {
                let mut decoder = ZlibDecoder::new(stored_payload.as_slice());
                let mut decoded = Vec::new();
                decoder.read_to_end(&mut decoded).unwrap();
                decoded
            } else {
                assert_eq!(compression, "none");
                stored_payload
            };
            records.push(TestServingBinaryRecord {
                kind,
                block_key,
                block_no,
                entry_count,
                payload: decoded_payload,
                compression,
                raw_payload_bytes,
            });
        }
        assert_eq!(reader.position(), reader.get_ref().len() as u64);
        records
    }

    fn test_read_uvarint(payload: &[u8], cursor: &mut usize) -> u64 {
        let mut value = 0u64;
        let mut shift = 0u32;
        loop {
            let byte = payload[*cursor];
            *cursor += 1;
            value |= u64::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }

    fn decode_test_provider_block(payload: &[u8], block_key: i64) -> BTreeMap<i64, Vec<u64>> {
        let mut cursor = 0usize;
        let provider_count = test_read_uvarint(payload, &mut cursor);
        let block_start = block_key * PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_BLOCK_SPAN;
        let mut code_keys_by_provider = BTreeMap::new();
        for _ in 0..provider_count {
            let provider_set_key = block_start + test_read_uvarint(payload, &mut cursor) as i64;
            let code_bytes = test_read_uvarint(payload, &mut cursor) as usize;
            let code_end = cursor + code_bytes;
            code_keys_by_provider.insert(
                provider_set_key,
                ptg2_serving_binary_v3::decode_provider_code_set(&payload[cursor..code_end])
                    .unwrap(),
            );
            cursor = code_end;
        }
        assert_eq!(cursor, payload.len());
        code_keys_by_provider
    }

    fn logical_test_payload(
        records: &[TestServingBinaryRecord],
        kind: &str,
        block_key: i64,
    ) -> Vec<u8> {
        let mut fragments = records
            .iter()
            .filter(|record| record.kind == kind && record.block_key == block_key)
            .collect::<Vec<_>>();
        fragments.sort_unstable_by_key(|record| record.block_no);
        fragments
            .into_iter()
            .flat_map(|record| record.payload.iter().copied())
            .collect()
    }

    fn decode_test_source_key_vector(
        payload: &[u8],
        cursor: &mut usize,
        occurrence_count: usize,
        source_bits: u8,
    ) -> Vec<u32> {
        let byte_count = occurrence_count
            .checked_mul(usize::from(source_bits))
            .unwrap()
            .div_ceil(8);
        let source_end = cursor.checked_add(byte_count).unwrap();
        let source_payload = &payload[*cursor..source_end];
        let mut source_keys = Vec::with_capacity(occurrence_count);
        let mut bit_offset = 0usize;
        for _ in 0..occurrence_count {
            let mut source_key = 0u32;
            for source_bit in 0..source_bits {
                if source_payload[bit_offset / 8] & (1u8 << (bit_offset % 8)) != 0 {
                    source_key |= 1u32 << source_bit;
                }
                bit_offset += 1;
            }
            source_keys.push(source_key);
        }
        *cursor = source_end;
        source_keys
    }

    fn decode_test_by_code_provider_shard_fragment(
        record: &TestServingBinaryRecord,
    ) -> Vec<(i32, Vec<(u32, u32)>)> {
        assert_eq!(record.kind, PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND);
        let mut cursor = 0usize;
        assert_eq!(
            record.payload[cursor],
            PTG2_SERVING_BINARY_V3_GROUPED_FORMAT_VERSION
        );
        cursor += 1;
        let source_count = test_read_uvarint(&record.payload, &mut cursor);
        let source_bits = record.payload[cursor];
        cursor += 1;
        assert_eq!(source_bits, source_key_bits(source_count).unwrap());
        let mut previous_provider_set_key = 0i32;
        let mut groups = Vec::with_capacity(record.entry_count as usize);
        for _ in 0..record.entry_count {
            let provider_delta =
                i32::try_from(test_read_uvarint(&record.payload, &mut cursor)).unwrap();
            let provider_set_key = previous_provider_set_key
                .checked_add(provider_delta)
                .unwrap();
            let occurrence_count =
                usize::try_from(test_read_uvarint(&record.payload, &mut cursor)).unwrap();
            let price_keys = (0..occurrence_count)
                .map(|_| u32::try_from(test_read_uvarint(&record.payload, &mut cursor)).unwrap())
                .collect::<Vec<_>>();
            let source_keys = decode_test_source_key_vector(
                &record.payload,
                &mut cursor,
                occurrence_count,
                source_bits,
            );
            assert!(source_keys
                .iter()
                .all(|source_key| u64::from(*source_key) < source_count));
            groups.push((
                provider_set_key,
                price_keys.into_iter().zip(source_keys).collect(),
            ));
            previous_provider_set_key = provider_set_key;
        }
        assert_eq!(cursor, record.payload.len());
        groups
    }

    fn prefixed_test_id(prefix: u16, last_byte: u8) -> [u8; GLOBAL_ID_BYTES] {
        let mut value = [0u8; GLOBAL_ID_BYTES];
        value[..2].copy_from_slice(&prefix.to_be_bytes());
        value[GLOBAL_ID_BYTES - 1] = last_byte;
        value
    }

    fn indexed_test_id(domain: u64, index: u64) -> [u8; GLOBAL_ID_BYTES] {
        let mut value = [0u8; GLOBAL_ID_BYTES];
        value[..8].copy_from_slice(&domain.to_be_bytes());
        value[8..].copy_from_slice(&index.to_be_bytes());
        value
    }

    fn test_file_sha256(path: &Path) -> String {
        let mut reader = BufReader::new(File::open(path).unwrap());
        let mut digest = Sha256::new();
        let mut buffer = [0u8; 1024 * 1024];
        loop {
            let read = reader.read(&mut buffer).unwrap();
            if read == 0 {
                break;
            }
            digest.update(&buffer[..read]);
        }
        sha256_hex(&digest.finalize())
    }

    fn test_json_sha256(value: &Value) -> String {
        sha256_hex(&Sha256::digest(serde_json::to_vec(value).unwrap()))
    }

    fn v3_finalizer_test_source_contract(
        output: &ptg2_scanner::v3_runs::ServingRunOutput,
        _label: &str,
        source_key: u32,
    ) -> (Value, String) {
        let partition_count = output
            .partition_files
            .first()
            .map(|file| file.partition_count)
            .unwrap_or(1);
        let mut partition_rows = vec![0u64; partition_count];
        let mut files = output
            .partition_files
            .iter()
            .map(|file| {
                partition_rows[file.partition_index] =
                    partition_rows[file.partition_index].saturating_add(file.record_count);
                json!({
                    "partition": file.partition_index,
                    "row_count": file.record_count,
                    "bytes": file.bytes,
                    "sha256": test_file_sha256(&file.path),
                })
            })
            .collect::<Vec<_>>();
        files.sort_by_key(|entry| {
            (
                entry["partition"].as_u64().unwrap(),
                entry["sha256"].as_str().unwrap().to_owned(),
                entry["row_count"].as_u64().unwrap(),
                entry["bytes"].as_u64().unwrap(),
            )
        });
        let contract = json!({
            "version": 1,
            "source_identity": {
                "source_type": "in_network",
                "identity_kind": "logical_json_sha256_v1",
                "identity_sha256": format!("{source_key:064x}"),
            },
            "partition_count": partition_count,
            "partition_rows": partition_rows,
            "file_count": output.partition_files.len(),
            "row_count": output.partition_files.iter().map(|file| file.record_count).sum::<u64>(),
            "byte_count": output.partition_files.iter().map(|file| file.bytes).sum::<u64>(),
            "files": files,
        });
        let contract_sha256 = test_json_sha256(&contract);
        let mut entry = contract.as_object().unwrap().clone();
        entry.insert("source_key".to_owned(), json!(source_key));
        entry.insert("contract_sha256".to_owned(), json!(contract_sha256.clone()));
        (Value::Object(entry), contract_sha256)
    }

    fn complete_v3_finalizer_test_manifest_contracts(manifest: &mut Value) {
        let source_contracts = manifest["source_run_contracts"].as_array().unwrap().clone();
        let serving_entries = manifest["serving_run_partition_files"]
            .as_array()
            .unwrap()
            .clone();
        let dictionary_entries = manifest["serving_run_code_dictionary_files"]
            .as_array()
            .unwrap()
            .clone();
        let provider_metadata_entries = manifest["provider_set_metadata_files"]
            .as_array()
            .unwrap()
            .clone();
        let source_count = serving_entries
            .first()
            .and_then(|entry| entry["source_count"].as_u64())
            .or_else(|| manifest["source_count"].as_u64())
            .unwrap();
        let mut dictionary_source_contracts = Vec::with_capacity(source_contracts.len());
        for source_contract in &source_contracts {
            let source_key = source_contract["source_key"].as_u64().unwrap();
            let mut files = dictionary_entries
                .iter()
                .filter(|entry| entry["source_key"].as_u64() == Some(source_key))
                .map(|entry| {
                    json!({
                        "row_count": entry["row_count"],
                        "bytes": entry["bytes"],
                        "sha256": entry["sha256"],
                    })
                })
                .collect::<Vec<_>>();
            files.sort_by_key(|entry| {
                (
                    entry["sha256"].as_str().unwrap().to_owned(),
                    entry["row_count"].as_u64().unwrap(),
                    entry["bytes"].as_u64().unwrap(),
                )
            });
            let contract = json!({
                "version": 1,
                "source_identity": source_contract["source_identity"],
                "source_run_contract_sha256": source_contract["contract_sha256"],
                "file_count": files.len(),
                "row_count": files.iter().map(|entry| entry["row_count"].as_u64().unwrap()).sum::<u64>(),
                "byte_count": files.iter().map(|entry| entry["bytes"].as_u64().unwrap()).sum::<u64>(),
                "files": files,
            });
            let contract_sha256 = test_json_sha256(&contract);
            for entry in manifest["serving_run_code_dictionary_files"]
                .as_array_mut()
                .unwrap()
                .iter_mut()
                .filter(|entry| entry["source_key"].as_u64() == Some(source_key))
            {
                entry["code_dictionary_contract_sha256"] = json!(contract_sha256);
            }
            let mut contract_entry = contract.as_object().unwrap().clone();
            contract_entry.insert("source_key".to_owned(), json!(source_key));
            contract_entry.insert("contract_sha256".to_owned(), json!(contract_sha256));
            dictionary_source_contracts.push(Value::Object(contract_entry));
        }
        dictionary_source_contracts.sort_by_key(|entry| entry["source_key"].as_u64().unwrap());
        let dictionary_entries = manifest["serving_run_code_dictionary_files"]
            .as_array()
            .unwrap()
            .clone();
        let dictionary_contracts = dictionary_entries
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
        let provider_metadata_contracts = provider_metadata_entries
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
        manifest["source_count"] = json!(source_count);
        manifest["source_run_contract_set_sha256"] = json!(test_json_sha256(&json!({
            "source_run_contracts": source_contracts,
        })));
        manifest["code_dictionary_source_contracts"] = json!(dictionary_source_contracts.clone());
        manifest["code_dictionary_source_contract_set_sha256"] = json!(test_json_sha256(&json!({
            "code_dictionary_source_contracts": dictionary_source_contracts,
        })));
        manifest["expected_serving_run_files"] = json!(serving_entries.len());
        manifest["expected_serving_run_rows"] = json!(serving_entries
            .iter()
            .map(|entry| entry["row_count"].as_u64().unwrap())
            .sum::<u64>());
        manifest["expected_serving_run_bytes"] = json!(serving_entries
            .iter()
            .map(|entry| entry["bytes"].as_u64().unwrap())
            .sum::<u64>());
        manifest["expected_code_dictionary_files"] = json!(dictionary_entries.len());
        manifest["expected_code_dictionary_rows"] = json!(dictionary_entries
            .iter()
            .map(|entry| entry["row_count"].as_u64().unwrap())
            .sum::<u64>());
        manifest["expected_code_dictionary_bytes"] = json!(dictionary_entries
            .iter()
            .map(|entry| entry["bytes"].as_u64().unwrap())
            .sum::<u64>());
        manifest["code_dictionary_contract_set_sha256"] = json!(test_json_sha256(&json!({
            "code_dictionary_contracts": dictionary_contracts,
        })));
        manifest["expected_provider_set_metadata_files"] = json!(provider_metadata_entries.len());
        manifest["expected_provider_set_metadata_rows"] = json!(provider_metadata_entries
            .iter()
            .map(|entry| entry["row_count"].as_u64().unwrap())
            .sum::<u64>());
        manifest["expected_provider_set_metadata_bytes"] = json!(provider_metadata_entries
            .iter()
            .map(|entry| entry["bytes"].as_u64().unwrap())
            .sum::<u64>());
        manifest["provider_set_metadata_contract_set_sha256"] = json!(test_json_sha256(&json!({
            "provider_set_metadata_contracts": provider_metadata_contracts,
        })));
    }

    fn set_v3_finalizer_test_source_identity(manifest: &mut Value, source_identity: Value) {
        manifest["source_run_contracts"][0]["source_identity"] = source_identity.clone();
        let contract_sha256 = sha256_hex(
            &v3_manifest_source_contract_sha256(&manifest["source_run_contracts"][0]).unwrap(),
        );
        manifest["source_run_contracts"][0]["contract_sha256"] = json!(contract_sha256.clone());
        for entry in manifest["serving_run_code_dictionary_files"]
            .as_array_mut()
            .unwrap()
        {
            entry["source_run_contract_sha256"] = json!(contract_sha256);
        }
        for entry in manifest["provider_set_metadata_files"]
            .as_array_mut()
            .unwrap()
        {
            entry["physical_source_identity"] = source_identity.clone();
            entry["source_run_contract_sha256"] = json!(contract_sha256);
        }
        complete_v3_finalizer_test_manifest_contracts(manifest);
    }

    fn write_v3_finalizer_test_provider_metadata(
        base: &Path,
        label: &str,
        provider_counts: &BTreeMap<[u8; GLOBAL_ID_BYTES], u32>,
        source_key: u32,
        source_count: u64,
        physical_source_identity: &Value,
        source_run_contract_sha256: &str,
    ) -> Value {
        assert!(!provider_counts.is_empty());
        let path = base.join(format!("{label}-provider-set-metadata.copy.ready"));
        let mut payload = Vec::new();
        for (provider_id, provider_count) in provider_counts {
            writeln!(
                &mut payload,
                "{}\t{}\t{{}}",
                GlobalId128(*provider_id).to_hex(),
                provider_count,
            )
            .unwrap();
        }
        std::fs::write(&path, &payload).unwrap();
        let sha256 = test_file_sha256(&path);
        json!({
            "path": path,
            "source_key": source_key,
            "source_count": source_count,
            "physical_source_identity": physical_source_identity,
            "source_run_contract_sha256": source_run_contract_sha256,
            "row_count": provider_counts.len(),
            "bytes": payload.len(),
            "sha256": sha256,
            "format": V3_PROVIDER_SET_METADATA_FORMAT,
            "version": V3_PROVIDER_SET_METADATA_VERSION,
        })
    }

    #[derive(Clone)]
    struct V3FinalizerTestRow {
        coverage_scope_id: [u8; COVERAGE_SCOPE_ID_BYTES],
        code_system: Option<&'static str>,
        code: Option<&'static str>,
        negotiation_arrangement: Option<&'static str>,
        provider_id: [u8; 16],
        price_id: [u8; 16],
        provider_count: u32,
    }

    fn write_v3_finalizer_test_manifest(
        base: &Path,
        label: &str,
        rows: &[V3FinalizerTestRow],
    ) -> PathBuf {
        write_v3_finalizer_test_manifest_with_source(base, label, rows, 0, 1)
    }

    fn write_v3_finalizer_test_manifest_with_source(
        base: &Path,
        label: &str,
        rows: &[V3FinalizerTestRow],
        source_key: u32,
        source_count: u64,
    ) -> PathBuf {
        write_v3_finalizer_test_manifest_with_source_and_partitions(
            base,
            label,
            rows,
            source_key,
            source_count,
            4,
        )
    }

    fn write_v3_finalizer_test_manifest_with_source_and_partitions(
        base: &Path,
        label: &str,
        rows: &[V3FinalizerTestRow],
        source_key: u32,
        source_count: u64,
        partition_count: usize,
    ) -> PathBuf {
        let run_directory = base.join(format!("{label}-runs"));
        let mut writer = ServingRunPartitionWriter::with_buffer_capacity(
            &run_directory,
            partition_count,
            label,
            SERVING_RUN_RECORD_BYTES,
        )
        .unwrap();
        for row in rows {
            let code_fields = NaturalLeanCodeFields {
                coverage_scope_id: &row.coverage_scope_id,
                reported_code_system: row.code_system,
                reported_code: row.code,
                negotiation_arrangement: row.negotiation_arrangement,
                billing_code_type_version: None,
                name: None,
                description: None,
            };
            let record = ServingRunRecord {
                code_id: code_fields.identity(),
                provider_set_id: row.provider_id,
                price_set_id: row.price_id,
                provider_count: row.provider_count,
            };
            writer
                .write_natural_lean_record(&record, code_fields)
                .unwrap();
        }
        let output = writer.finish().unwrap();
        let (source_contract, source_run_contract_sha256) =
            v3_finalizer_test_source_contract(&output, label, source_key);
        let mut provider_counts = BTreeMap::new();
        for row in rows {
            assert!(provider_counts
                .insert(row.provider_id, row.provider_count)
                .is_none_or(|existing| existing == row.provider_count));
        }
        let provider_metadata = write_v3_finalizer_test_provider_metadata(
            base,
            label,
            &provider_counts,
            source_key,
            source_count,
            &source_contract["source_identity"],
            &source_run_contract_sha256,
        );
        let manifest_path = base.join(format!("{label}.json"));
        let mut manifest = json!({
            "source_run_contracts": [source_contract],
            "serving_run_partition_files": output.partition_files.iter().map(|file| json!({
                "path": file.path,
                "partition": file.partition_index,
                "partition_count": file.partition_count,
                "source_key": source_key,
                "source_count": source_count,
                "row_count": file.record_count,
                "bytes": file.bytes,
                "sha256": test_file_sha256(&file.path),
                "format": SERVING_RUN_FORMAT,
                "version": SERVING_RUN_FORMAT_VERSION,
            })).collect::<Vec<_>>(),
            "serving_run_code_dictionary_files": output.code_dictionary_file.iter().map(|file| json!({
                "path": file.path,
                "source_key": source_key,
                "source_count": source_count,
                "source_run_contract_sha256": source_run_contract_sha256,
                "row_count": file.record_count,
                "bytes": file.bytes,
                "sha256": test_file_sha256(&file.path),
                "format": CODE_DICTIONARY_FORMAT,
                "version": CODE_DICTIONARY_FORMAT_VERSION,
            })).collect::<Vec<_>>(),
            "provider_set_metadata_files": [provider_metadata],
        });
        complete_v3_finalizer_test_manifest_contracts(&mut manifest);
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        manifest_path
    }

    fn v3_finalizer_benchmark_codes_by_partition(
        coverage_scope_id: &[u8; COVERAGE_SCOPE_ID_BYTES],
        partition_count: usize,
    ) -> Vec<String> {
        let mut codes = vec![None; partition_count];
        let mut remaining = partition_count;
        for candidate in 0..1_000_000u64 {
            let code = format!("B{candidate:08}");
            let code_fields = NaturalLeanCodeFields {
                coverage_scope_id,
                reported_code_system: Some("CPT"),
                reported_code: Some(&code),
                negotiation_arrangement: Some("FFS"),
                billing_code_type_version: None,
                name: None,
                description: None,
            };
            let partition = ptg2_scanner::v3_runs::partition_for_code_id(
                &code_fields.identity(),
                partition_count,
            )
            .unwrap();
            if codes[partition].is_none() {
                codes[partition] = Some(code);
                remaining -= 1;
                if remaining == 0 {
                    break;
                }
            }
        }
        assert_eq!(remaining, 0, "failed to cover every benchmark partition");
        codes
            .into_iter()
            .map(|code| code.expect("benchmark partition code"))
            .collect()
    }

    fn write_v3_finalizer_benchmark_manifest(
        base: &Path,
        label: &str,
        row_count: usize,
        provider_cardinality: usize,
        price_cardinality: usize,
        partition_count: usize,
    ) -> PathBuf {
        let coverage_scope_id = [0x75; COVERAGE_SCOPE_ID_BYTES];
        let codes = v3_finalizer_benchmark_codes_by_partition(&coverage_scope_id, partition_count);
        let run_directory = base.join(format!("{label}-runs"));
        let mut writer = ServingRunPartitionWriter::with_buffer_capacity(
            &run_directory,
            partition_count,
            label,
            SERVING_RUN_RECORD_BYTES,
        )
        .unwrap();
        let mut provider_counts = BTreeMap::new();
        for index in 0..row_count {
            let unique_bucket = index / 2;
            let provider_index = unique_bucket % provider_cardinality;
            let provider_cycle = unique_bucket / provider_cardinality;
            let price_index = (provider_index
                .wrapping_mul(7_919)
                .wrapping_add(provider_cycle.wrapping_mul(104_729)))
                % price_cardinality;
            let code_fields = NaturalLeanCodeFields {
                coverage_scope_id: &coverage_scope_id,
                reported_code_system: Some("CPT"),
                reported_code: Some(&codes[unique_bucket % codes.len()]),
                negotiation_arrangement: Some("FFS"),
                billing_code_type_version: None,
                name: None,
                description: None,
            };
            let provider_id = indexed_test_id(0x5100, provider_index as u64 + 1);
            let provider_count = (provider_index % 200 + 1) as u32;
            provider_counts.insert(provider_id, provider_count);
            writer
                .write_natural_lean_record(
                    &ServingRunRecord {
                        code_id: code_fields.identity(),
                        provider_set_id: provider_id,
                        price_set_id: indexed_test_id(0x6200, price_index as u64 + 1),
                        provider_count,
                    },
                    code_fields,
                )
                .unwrap();
        }
        let output = writer.finish().unwrap();
        let (source_contract, source_run_contract_sha256) =
            v3_finalizer_test_source_contract(&output, label, 0);
        let provider_metadata = write_v3_finalizer_test_provider_metadata(
            base,
            label,
            &provider_counts,
            0,
            1,
            &source_contract["source_identity"],
            &source_run_contract_sha256,
        );
        let manifest_path = base.join(format!("{label}.json"));
        let mut manifest = json!({
            "source_run_contracts": [source_contract],
            "serving_run_partition_files": output.partition_files.iter().map(|file| json!({
                "path": file.path,
                "partition": file.partition_index,
                "partition_count": file.partition_count,
                "source_key": 0,
                "source_count": 1,
                "row_count": file.record_count,
                "bytes": file.bytes,
                "sha256": test_file_sha256(&file.path),
                "format": SERVING_RUN_FORMAT,
                "version": SERVING_RUN_FORMAT_VERSION,
            })).collect::<Vec<_>>(),
            "serving_run_code_dictionary_files": output.code_dictionary_file.iter().map(|file| json!({
                "path": file.path,
                "source_key": 0,
                "source_count": 1,
                "source_run_contract_sha256": source_run_contract_sha256,
                "row_count": file.record_count,
                "bytes": file.bytes,
                "sha256": test_file_sha256(&file.path),
                "format": CODE_DICTIONARY_FORMAT,
                "version": CODE_DICTIONARY_FORMAT_VERSION,
            })).collect::<Vec<_>>(),
            "provider_set_metadata_files": [provider_metadata],
        });
        complete_v3_finalizer_test_manifest_contracts(&mut manifest);
        std::fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();
        manifest_path
    }

    #[derive(Debug, Eq, PartialEq)]
    struct V3FinalizerCodeCopyRow {
        code_key: i32,
        coverage_scope_id: [u8; COVERAGE_SCOPE_ID_BYTES],
        reported_code_system: Option<String>,
        reported_code: Option<String>,
        negotiation_arrangement: Option<String>,
        billing_code_type_version: Option<String>,
        name: Option<String>,
        description: Option<String>,
        rate_count: u64,
    }

    fn pg_binary_optional_test_text(field: Option<&[u8]>) -> Option<String> {
        field.map(|value| std::str::from_utf8(value).unwrap().to_owned())
    }

    fn read_v3_code_dictionary_rows(path: &Path) -> BTreeMap<[u8; 16], V3FinalizerCodeCopyRow> {
        let mut reader = BufReader::new(File::open(path).unwrap());
        read_pg_binary_copy_header(&mut reader).unwrap();
        let mut rows = BTreeMap::new();
        while let Some(fields) =
            read_pg_binary_copy_row(&mut reader, 10, "code dictionary").unwrap()
        {
            let code_key = pg_binary_i32(
                required_pg_binary_field(&fields, 0, "code_key").unwrap(),
                "code_key",
            )
            .unwrap();
            let id: [u8; 16] = required_pg_binary_field(&fields, 1, "code_id")
                .unwrap()
                .try_into()
                .unwrap();
            let coverage_scope_id: [u8; COVERAGE_SCOPE_ID_BYTES] =
                required_pg_binary_field(&fields, 2, "coverage_scope_id")
                    .unwrap()
                    .try_into()
                    .unwrap();
            let reported_code_system = pg_binary_optional_test_text(fields[3].as_deref());
            let reported_code = pg_binary_optional_test_text(fields[4].as_deref());
            let negotiation_arrangement = pg_binary_optional_test_text(fields[5].as_deref());
            let billing_code_type_version = pg_binary_optional_test_text(fields[6].as_deref());
            let name = pg_binary_optional_test_text(fields[7].as_deref());
            let description = pg_binary_optional_test_text(fields[8].as_deref());
            let rate_count = pg_binary_u64(
                required_pg_binary_field(&fields, 9, "rate_count").unwrap(),
                "rate_count",
            )
            .unwrap();
            rows.insert(
                id,
                V3FinalizerCodeCopyRow {
                    code_key,
                    coverage_scope_id,
                    reported_code_system,
                    reported_code,
                    negotiation_arrangement,
                    billing_code_type_version,
                    name,
                    description,
                    rate_count,
                },
            );
        }
        rows
    }

    fn read_v3_provider_dictionary_keys(path: &Path) -> BTreeMap<[u8; 16], (i32, u32)> {
        let mut reader = BufReader::new(File::open(path).unwrap());
        read_pg_binary_copy_header(&mut reader).unwrap();
        let mut keys = BTreeMap::new();
        while let Some(fields) =
            read_pg_binary_copy_row(&mut reader, 3, "provider dictionary").unwrap()
        {
            let key = pg_binary_i32(
                required_pg_binary_field(&fields, 0, "provider_key").unwrap(),
                "provider_key",
            )
            .unwrap();
            let id: [u8; 16] = required_pg_binary_field(&fields, 1, "provider_id")
                .unwrap()
                .try_into()
                .unwrap();
            let count = u32::try_from(
                pg_binary_u64(
                    required_pg_binary_field(&fields, 2, "provider_count").unwrap(),
                    "provider_count",
                )
                .unwrap(),
            )
            .unwrap();
            keys.insert(id, (key, count));
        }
        keys
    }

    fn reference_v3_assigned_records(
        rows: &[V3FinalizerTestRow],
        source_keys: &[u32],
        output_directory: &Path,
        price_ids_in_key_order: &[[u8; GLOBAL_ID_BYTES]],
    ) -> (Vec<TestServingBinaryRecord>, Vec<TestServingBinaryRecord>) {
        assert_eq!(source_keys.len(), rows.len());
        let source_count = source_keys
            .iter()
            .copied()
            .max()
            .map(u64::from)
            .unwrap_or(0)
            .saturating_add(1);
        let code_rows =
            read_v3_code_dictionary_rows(&output_directory.join("code_dictionary.copy"));
        let provider_keys = read_v3_provider_dictionary_keys(
            &output_directory.join("provider_set_dictionary.copy"),
        );
        let source_price_ids = rows.iter().map(|row| row.price_id).collect::<BTreeSet<_>>();
        assert_eq!(
            source_price_ids,
            price_ids_in_key_order
                .iter()
                .copied()
                .collect::<BTreeSet<_>>()
        );
        let price_keys = price_ids_in_key_order
            .iter()
            .enumerate()
            .map(|(key, id)| (*id, key as i64))
            .collect::<BTreeMap<_, _>>();
        let mut assigned_rows = rows
            .iter()
            .zip(source_keys)
            .map(|(row, source_key)| {
                let code_id = natural_lean_code_identity(
                    &row.coverage_scope_id,
                    row.code_system,
                    row.code,
                    row.negotiation_arrangement,
                    None,
                    None,
                    None,
                );
                let (provider_key, provider_count) = provider_keys[&row.provider_id];
                vec![
                    pg_i32_field(code_rows[&code_id].code_key),
                    pg_i32_field(provider_key),
                    pg_i64_field(i64::from(provider_count)),
                    pg_i64_field(price_keys[&row.price_id]),
                    pg_i64_field(i64::from(*source_key)),
                ]
            })
            .collect::<Vec<_>>();
        assigned_rows.sort();
        let assigned_source = pg_binary_copy_rows(&assigned_rows);
        let mut assigned_output = CountingWriter::new(Vec::new());
        write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
            &mut Cursor::new(assigned_source),
            &mut assigned_output,
            ServingBinaryTargetCopyFormat::SharedBinary,
            AssignedV3EncoderOptions {
                grouped_payload_bytes: serving_binary_block_bytes(),
                hot_payload_bytes: V3_FINALIZER_HOT_BLOCK_BYTES,
                provider_code_sort_chunk_bytes: serving_binary_v3_provider_code_sort_chunk_bytes(),
                provider_set_count: None,
                provider_code_count: None,
                provider_code_bitmap_max_bytes: 0,
                rate_schedule_observe: false,
                source: SourceEncoding {
                    count: source_count,
                    key_bits: source_key_bits(source_count).unwrap(),
                    tagged_codec: TaggedServingRunCodec::new(
                        source_count,
                        source_key_bytes(source_count).unwrap(),
                    )
                    .unwrap(),
                },
            },
            true,
        )
        .unwrap();

        let price_rows = price_ids_in_key_order
            .iter()
            .enumerate()
            .map(|(key, id)| vec![pg_i64_field(key as i64), Some(id.to_vec())])
            .collect::<Vec<_>>();
        let price_source = pg_binary_copy_rows(&price_rows);
        let mut price_output = CountingWriter::new(Vec::new());
        write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
            &mut Cursor::new(price_source),
            &mut price_output,
            ServingBinaryTargetCopyFormat::SharedBinary,
            V3_FINALIZER_HOT_BLOCK_BYTES,
        )
        .unwrap();
        let normalize_raw_bytes = |mut records: Vec<TestServingBinaryRecord>| {
            for record in &mut records {
                if record.compression == "none" {
                    record.raw_payload_bytes = record.payload.len() as i32;
                }
            }
            records
        };
        (
            normalize_raw_bytes(read_test_shared_binary_records(assigned_output.inner)),
            normalize_raw_bytes(read_test_shared_binary_records(price_output.inner)),
        )
    }

    #[test]
    fn shared_block_hash_matches_fixed_python_vector() {
        let hash = shared_v3_block_hash(1, "by_code_grouped", "none", &[1, 2, 3]).unwrap();
        assert_eq!(
            sha256_hex(&hash),
            "4ce3f60a45772e30f3055b5f385024010c45863a0e9091c9c55aabc9482f603e"
        );
    }

    #[derive(Clone)]
    struct SharedBlockPreparationTestRecord {
        kind: &'static str,
        block_key: i64,
        fragment_no: usize,
        entry_count: usize,
        payload: Vec<u8>,
    }

    fn deterministic_incompressible_payload(bytes: usize, mut state: u64) -> Vec<u8> {
        (0..bytes)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as u8
            })
            .collect()
    }

    fn render_shared_block_preparation_test_copy(
        records: &[SharedBlockPreparationTestRecord],
        workers: Option<usize>,
        limits: SharedBlockPreparationBatchLimits,
    ) -> CountingWriter<Vec<u8>> {
        let mut writer = match workers {
            Some(_) => {
                CountingWriter::with_shared_block_preparation_batch(Vec::new(), limits).unwrap()
            }
            None => CountingWriter::new(Vec::new()),
        };
        let mut encode = || -> io::Result<()> {
            write_serving_binary_copy_header(
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
            )?;
            for record in records {
                write_serving_binary_copy_record_with_i64_key_and_stats(
                    &mut writer,
                    ServingBinaryTargetCopyFormat::SharedBinary,
                    record.kind,
                    record.block_key,
                    record.fragment_no,
                    record.entry_count,
                    &record.payload,
                )?;
            }
            write_serving_binary_copy_trailer(
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
            )
        };
        match workers {
            Some(workers) => rayon::ThreadPoolBuilder::new()
                .num_threads(workers)
                .build()
                .unwrap()
                .install(encode)
                .unwrap(),
            None => encode().unwrap(),
        }
        writer
    }

    #[test]
    fn shared_block_parallel_preparation_is_byte_identical_for_mixed_codecs_and_workers() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let _compression_level =
            TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_LEVEL_ENV, "6");
        let _minimum_bytes =
            TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_BYTES_ENV, "128");
        let _minimum_savings = TestEnvVar::set(
            PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_MIN_SAVINGS_PCT_ENV,
            "2",
        );
        let records = vec![
            SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
                block_key: 1,
                fragment_no: 0,
                entry_count: 3,
                payload: vec![7; 8192],
            },
            SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
                block_key: 2,
                fragment_no: 0,
                entry_count: 1,
                payload: deterministic_incompressible_payload(8192, 0x1234_5678_9abc_def0),
            },
            SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
                block_key: 3,
                fragment_no: 0,
                entry_count: 1,
                payload: vec![42; 64],
            },
            SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
                block_key: 4,
                fragment_no: 0,
                entry_count: 0,
                payload: Vec::new(),
            },
            SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND,
                block_key: 5,
                fragment_no: 1,
                entry_count: 5,
                payload: vec![9; 32 * 1024],
            },
        ];
        let limits = SharedBlockPreparationBatchLimits {
            maximum_raw_bytes: 12 * 1024,
            maximum_records: 3,
        };
        let serial = render_shared_block_preparation_test_copy(&records, None, limits);
        let serial_summary = serial.shared_block_summary(Path::new("blocks.copy"));
        let serial_records = read_test_shared_binary_records(serial.inner.clone());
        assert!(serial_records
            .iter()
            .any(|record| record.compression == "zlib"));
        assert!(serial_records
            .iter()
            .any(|record| record.compression == "none"));

        for workers in [1, 4, 8] {
            let parallel =
                render_shared_block_preparation_test_copy(&records, Some(workers), limits);
            assert_eq!(parallel.inner, serial.inner, "worker count {workers}");
            assert_eq!(
                parallel.shared_block_summary(Path::new("blocks.copy")),
                serial_summary,
                "worker count {workers}",
            );
            if workers == 1 {
                assert_eq!(
                    parallel
                        .shared_block_preparation_metrics
                        .parallel_batch_count,
                    0,
                );
            } else {
                assert!(
                    parallel
                        .shared_block_preparation_metrics
                        .parallel_batch_count
                        > 0,
                );
            }
        }
    }

    #[test]
    fn shared_block_parallel_preparation_is_byte_identical_with_compression_disabled() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let records = (0..17)
            .map(|index| SharedBlockPreparationTestRecord {
                kind: if index % 2 == 0 {
                    PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND
                } else {
                    PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
                },
                block_key: index,
                fragment_no: index as usize % 3,
                entry_count: index as usize + 1,
                payload: if index % 2 == 0 {
                    vec![index as u8; 4096]
                } else {
                    deterministic_incompressible_payload(3072, index as u64 + 1)
                },
            })
            .collect::<Vec<_>>();
        let limits = SharedBlockPreparationBatchLimits {
            maximum_raw_bytes: 16 * 1024,
            maximum_records: 4,
        };
        let serial = render_shared_block_preparation_test_copy(&records, None, limits);
        let serial_summary = serial.shared_block_summary(Path::new("blocks.copy"));
        assert!(read_test_shared_binary_records(serial.inner.clone())
            .iter()
            .all(|record| record.compression == "none"));
        for workers in [1, 4, 8] {
            let parallel =
                render_shared_block_preparation_test_copy(&records, Some(workers), limits);
            assert_eq!(parallel.inner, serial.inner, "worker count {workers}");
            assert_eq!(
                parallel.shared_block_summary(Path::new("blocks.copy")),
                serial_summary,
                "worker count {workers}",
            );
        }
    }

    #[test]
    fn shared_block_preparation_batch_respects_record_and_raw_byte_bounds() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let limits = SharedBlockPreparationBatchLimits {
            maximum_raw_bytes: 64 * 1024,
            maximum_records: 5,
        };
        let mut records = (0..37)
            .map(|index| SharedBlockPreparationTestRecord {
                kind: PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
                block_key: index,
                fragment_no: 0,
                entry_count: 1,
                payload: vec![index as u8; 4096 + (index as usize % 3) * 1024],
            })
            .collect::<Vec<_>>();
        records.push(SharedBlockPreparationTestRecord {
            kind: PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
            block_key: 100,
            fragment_no: 0,
            entry_count: 1,
            payload: vec![1; 128 * 1024],
        });
        let writer = render_shared_block_preparation_test_copy(&records, Some(8), limits);
        let metrics = &writer.shared_block_preparation_metrics;
        assert!(metrics.peak_batch_records <= limits.maximum_records);
        assert!(
            metrics.peak_batch_raw_bytes
                <= limits
                    .maximum_raw_bytes
                    .max(metrics.maximum_record_raw_bytes)
        );
        assert_eq!(metrics.record_count, records.len() as u64);
        assert!(writer.pending_shared_blocks.is_empty());
        assert!(writer.recycled_shared_block_capacity_bytes <= limits.maximum_raw_bytes);
    }

    struct FailAfterWriter {
        remaining_bytes: usize,
        bytes: Vec<u8>,
    }

    impl Write for FailAfterWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            if self.remaining_bytes == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected shared-block writer cancellation",
                ));
            }
            let written = bytes.len().min(self.remaining_bytes);
            self.bytes.extend_from_slice(&bytes[..written]);
            self.remaining_bytes -= written;
            Ok(written)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn shared_block_parallel_preparation_propagates_validation_and_writer_cancellation() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let limits = SharedBlockPreparationBatchLimits {
            maximum_raw_bytes: 1024 * 1024,
            maximum_records: 16,
        };
        let mut writer = CountingWriter::with_shared_block_preparation_batch(
            FailAfterWriter {
                remaining_bytes: 48,
                bytes: Vec::new(),
            },
            limits,
        )
        .unwrap();
        write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
            .unwrap();
        write_serving_binary_copy_record_with_i64_key_and_stats(
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            1,
            0,
            1,
            &vec![7; 8192],
        )
        .unwrap();
        let pending_before_validation_error = writer.pending_shared_blocks.len();
        let validation_error = write_serving_binary_copy_record_with_i64_key_and_stats(
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            -1,
            0,
            1,
            &[1, 2, 3],
        )
        .unwrap_err();
        assert_eq!(validation_error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            writer.pending_shared_blocks.len(),
            pending_before_validation_error
        );

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(4)
            .build()
            .unwrap();
        let cancellation_error = pool
            .install(|| {
                write_serving_binary_copy_trailer(
                    &mut writer,
                    ServingBinaryTargetCopyFormat::SharedBinary,
                )
            })
            .unwrap_err();
        assert_eq!(cancellation_error.kind(), io::ErrorKind::BrokenPipe);
        assert!(writer.pending_shared_blocks.is_empty());
        assert_eq!(writer.shared_blocks.row_count, 0);
    }

    #[test]
    fn shared_block_fail_after_writer_preserves_complete_copy_on_sufficient_capacity() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let mut writer = CountingWriter::with_shared_block_preparation_batch(
            FailAfterWriter {
                remaining_bytes: 4096,
                bytes: Vec::new(),
            },
            SharedBlockPreparationBatchLimits {
                maximum_raw_bytes: 4096,
                maximum_records: 2,
            },
        )
        .unwrap();

        write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
            .unwrap();
        write_serving_binary_copy_record_with_i64_key_and_stats(
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
            7,
            0,
            2,
            &[3, 1, 4, 1, 5, 9],
        )
        .unwrap();
        write_serving_binary_copy_trailer(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
            .unwrap();
        writer.flush().unwrap();

        assert_eq!(&writer.inner.bytes[..11], b"PGCOPY\n\xff\r\n\0");
        assert_eq!(
            &writer.inner.bytes[writer.inner.bytes.len() - 2..],
            &[0xff, 0xff]
        );
        assert_eq!(writer.shared_blocks.row_count, 1);
        assert!(writer.pending_shared_blocks.is_empty());
    }

    #[test]
    fn shared_block_writer_summary_matches_reread_oracle() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("shared-blocks.copy");
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .unwrap();
        let mut writer = CountingWriter::new(BufWriter::new(file));
        write_serving_binary_copy_header(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
            .unwrap();
        let mut stats = ServingBinaryV3BlockStats::default();
        write_serving_binary_v3_logical_block(
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            32,
            ServingBinaryV3LogicalBlock {
                artifact_kind: PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
                block_key: 7,
                entry_count: 3,
                payload: &[42; 96],
            },
            &mut stats,
        )
        .unwrap();
        let compressible_payload = vec![7u8; 8192];
        write_serving_binary_v3_logical_block(
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            64 * 1024,
            ServingBinaryV3LogicalBlock {
                artifact_kind: PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
                block_key: 8,
                entry_count: 1,
                payload: &compressible_payload,
            },
            &mut stats,
        )
        .unwrap();
        write_serving_binary_copy_trailer(&mut writer, ServingBinaryTargetCopyFormat::SharedBinary)
            .unwrap();
        sync_counting_writer(&mut writer, ScratchDurability::Durable).unwrap();

        assert_eq!(
            writer.shared_block_summary(&path),
            summarize_shared_block_copy(&path).unwrap(),
        );
    }

    #[test]
    fn code_dictionary_support_digest_covers_all_ten_fields() {
        let support_digest = |code_key: i32, code: NaturalLeanCode, rate_count: u64| {
            let mut copy_row = Vec::new();
            let mut row_digest = Sha256::new();
            row_digest.update(V3_FINALIZER_CODE_ROW_HASH_DOMAIN);
            write_v3_code_dictionary_copy_row(
                &mut copy_row,
                code_key,
                &code,
                rate_count,
                &mut row_digest,
            )
            .unwrap();
            assert_eq!(&copy_row[..2], &10i16.to_be_bytes());
            v3_support_digest(&row_digest.finalize().into(), 1, &[0u8; 32], 0).unwrap()
        };
        let base = NaturalLeanCode {
            code_id: [1u8; 16],
            coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
            reported_code_system: None,
            reported_code: Some("99213".to_owned()),
            negotiation_arrangement: Some("FFS".to_owned()),
            billing_code_type_version: Some("2026".to_owned()),
            name: Some("Source name".to_owned()),
            description: Some("Source description".to_owned()),
        };
        let digests = BTreeSet::from([
            support_digest(0, base.clone(), 1),
            support_digest(1, base.clone(), 1),
            support_digest(
                0,
                NaturalLeanCode {
                    code_id: [2u8; 16],
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    coverage_scope_id: [0x22; COVERAGE_SCOPE_ID_BYTES],
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    reported_code_system: Some(String::new()),
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    reported_code: None,
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    negotiation_arrangement: Some("BUNDLE".to_owned()),
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    billing_code_type_version: Some("2025".to_owned()),
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    name: Some("Different name".to_owned()),
                    ..base.clone()
                },
                1,
            ),
            support_digest(
                0,
                NaturalLeanCode {
                    description: Some("Different description".to_owned()),
                    ..base.clone()
                },
                1,
            ),
            support_digest(0, base, 2),
        ]);
        assert_eq!(digests.len(), 11);
    }

    #[test]
    fn direct_v3_finalizer_is_deterministic_across_scratch_durability() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
        let _provider_sort = TestEnvVar::set(
            PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
            "8",
        );
        let base =
            std::env::temp_dir().join(format!("ptg2-direct-v3-finalizer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let provider_a = prefixed_test_id(1, 1);
        let provider_b = prefixed_test_id(1, 2);
        let price_a = prefixed_test_id(2, 1);
        let price_b = prefixed_test_id(2, 2);
        let scope_a = [0x11; COVERAGE_SCOPE_ID_BYTES];
        let scope_b = [0x22; COVERAGE_SCOPE_ID_BYTES];
        let row_a = V3FinalizerTestRow {
            coverage_scope_id: scope_a,
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider_a,
            price_id: price_a,
            provider_count: 2,
        };
        let row_b = V3FinalizerTestRow {
            coverage_scope_id: scope_a,
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: provider_b,
            price_id: price_b,
            provider_count: 3,
        };
        let row_c = V3FinalizerTestRow {
            coverage_scope_id: scope_b,
            code_system: None,
            code: Some("A100"),
            negotiation_arrangement: None,
            provider_id: provider_a,
            price_id: price_b,
            provider_count: 2,
        };
        let row_d = V3FinalizerTestRow {
            coverage_scope_id: scope_a,
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("BUNDLE"),
            provider_id: provider_a,
            price_id: price_b,
            provider_count: 2,
        };
        let manifest_a = write_v3_finalizer_test_manifest_with_source(
            &base,
            "input-a",
            &[row_b.clone(), row_a.clone()],
            0,
            2,
        );
        let manifest_b = write_v3_finalizer_test_manifest_with_source(
            &base,
            "input-b",
            &[row_c.clone(), row_d.clone(), row_a.clone()],
            1,
            2,
        );
        let price_ids_in_key_order = [price_b, price_a];
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "authoritative", &price_ids_in_key_order);
        let source_rows = vec![row_a.clone(), row_b, row_c, row_d, row_a.clone()];
        let output_a = base.join("output-a");
        let summary_a = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output_a.clone(),
            manifest_paths: vec![manifest_a.clone(), manifest_b.clone()],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
            workers: 2,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input: price_key_map_input.clone(),
            price_key_map_row_count: price_ids_in_key_order.len() as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();
        let output_b = base.join("output-b");
        let summary_b = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output_b.clone(),
            manifest_paths: vec![manifest_b, manifest_a],
            scratch_durability: ScratchDurability::Ephemeral,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
            workers: 2,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: price_ids_in_key_order.len() as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();

        for file_name in [
            "audit_candidates.bin",
            "shared_serving_blocks.copy",
            "shared_price_dictionary_blocks.copy",
            "code_dictionary.copy",
            "provider_set_dictionary.copy",
        ] {
            assert_eq!(
                std::fs::read(output_a.join(file_name)).unwrap(),
                std::fs::read(output_b.join(file_name)).unwrap(),
                "{file_name} changed with input order or scratch durability"
            );
        }
        let durable_sync = &summary_a["scratch_durability"];
        let ephemeral_sync = &summary_b["scratch_durability"];
        assert_eq!(durable_sync["policy"], "durable");
        assert_eq!(
            durable_sync["crash_recovery"],
            "synced_files_before_atomic_directory_publish_v1"
        );
        assert_eq!(ephemeral_sync["policy"], "ephemeral");
        assert_eq!(
            ephemeral_sync["crash_recovery"],
            "caller_discards_and_rebuilds_uncommitted_attempt_v1"
        );
        for category in [
            "assigned_final_runs",
            "price_copy_output",
            "serving_copy_output",
        ] {
            let durable = &durable_sync["categories"][category];
            let ephemeral = &ephemeral_sync["categories"][category];
            assert!(durable["sync_calls"].as_u64().unwrap() > 0);
            assert!(durable["sync_bytes"].as_u64().unwrap() > 0);
            assert_eq!(durable["skipped_sync_calls"], 0);
            assert_eq!(durable["skipped_sync_bytes"], 0);
            assert_eq!(ephemeral["sync_calls"], 0);
            assert_eq!(ephemeral["sync_bytes"], 0);
            assert_eq!(ephemeral["sync_seconds"], 0.0);
            assert_eq!(ephemeral["sync_max_seconds"], 0.0);
            assert!(ephemeral["skipped_sync_calls"].as_u64().unwrap() > 0);
            assert_eq!(
                durable["sync_bytes"], ephemeral["skipped_sync_bytes"],
                "{category} measured different logical bytes by policy"
            );
        }
        assert_eq!(
            summary_a["dictionaries"]["support_digest"],
            summary_b["dictionaries"]["support_digest"]
        );
        assert_eq!(summary_a["audit_candidates"], summary_b["audit_candidates"]);
        assert_eq!(
            summary_a["audit_candidates"]["record_format"],
            "ptg2_v3_audit_candidates_v2"
        );
        assert_eq!(summary_a["audit_candidates"]["record_bytes"], 20);
        assert_eq!(summary_a["audit_candidates"]["row_count"], 5);
        assert_eq!(summary_a["audit_candidates"]["maximum_rows"], 4096);
        assert_eq!(summary_a["audit_candidates"]["source_row_count"], 5);
        assert_eq!(summary_a["source"]["record_count"], 5);
        assert_eq!(summary_a["source_key_bytes"], 1);
        assert_eq!(summary_a["tagged_record_bytes"], 53);
        assert_eq!(summary_a["source"]["source_key_bytes"], 1);
        assert_eq!(summary_a["source"]["tagged_record_bytes"], 53);
        assert_eq!(summary_a["source_identity_scan"]["bytes_read"], 0);
        assert_eq!(summary_a["source_identity_scan"]["passes"], 0);
        assert_eq!(
            summary_a["source_identity_scan"]["strategy"],
            "assignment_integrated_exact_coverage_v1"
        );
        assert_eq!(
            summary_a["source_identity_scan"]["tagged_partition_sort"],
            false
        );
        assert_eq!(
            summary_a["price_identity_assignment"]["source_identity_sort"],
            false
        );
        assert_eq!(summary_a["preservation"]["sorted_records"], 5);
        assert_eq!(summary_a["preservation"]["staged_records"], 5);
        assert_eq!(summary_a["preservation"]["assigned_records"], 5);
        assert_eq!(summary_a["preservation"]["encoded_records"], 5);
        assert_eq!(summary_a["preservation"]["distinct_serving_records"], 5);
        assert_eq!(summary_a["preservation"]["duplicate_serving_records"], 0);
        assert!(
            summary_a["preservation"]["all_source_occurrences_preserved"]
                .as_bool()
                .unwrap()
        );
        assert_eq!(summary_a["dense_keys"]["code"]["count"], 3);
        assert_eq!(summary_a["dense_keys"]["provider_set"]["count"], 2);
        assert_eq!(summary_a["dense_keys"]["price"]["count"], 2);
        assert_eq!(
            summary_a["dense_keys"]["price"]["ordering"],
            "minimum_negotiated_rate_then_global_id_128_v1"
        );
        assert_eq!(
            summary_a["price_key_map"]["dense_price_ordering"],
            "minimum_negotiated_rate_then_global_id_128_v1"
        );
        assert_eq!(summary_a["price_key_map"]["source_ids_exact_match"], true);
        assert_eq!(
            summary_a["price_key_map"]["dictionary_external_sort"],
            false
        );
        assert_eq!(
            summary_a["price_key_map"]["dictionary_strategy"],
            "single_pass_map_and_shared_blocks_v1"
        );
        assert_eq!(
            summary_a["price_key_map"]["input_ordering"],
            "price_key_dense_ascending_v1"
        );
        assert_eq!(summary_a["price_key_map"]["input_passes"], 1);
        assert_eq!(summary_a["price_key_map"]["fixed_map_scratch_files"], 0);
        assert_eq!(summary_a["dictionaries"]["code"]["field_count"], 10);
        assert_eq!(
            summary_a["dictionaries"]["code"]["fields"],
            json!([
                "code_key",
                "code_global_id_128",
                "coverage_scope_id",
                "reported_code_system",
                "reported_code",
                "negotiation_arrangement",
                "billing_code_type_version",
                "source_name",
                "source_description",
                "rate_count"
            ])
        );
        assert_eq!(summary_a["dictionaries"]["code"]["rate_count_total"], 5);
        let code_rows = read_v3_code_dictionary_rows(&output_a.join("code_dictionary.copy"));
        let code_a = natural_lean_code_identity(
            &scope_a,
            Some("CPT"),
            Some("99213"),
            Some("FFS"),
            None,
            None,
            None,
        );
        let code_a_bundle = natural_lean_code_identity(
            &scope_a,
            Some("CPT"),
            Some("99213"),
            Some("BUNDLE"),
            None,
            None,
            None,
        );
        let code_b =
            natural_lean_code_identity(&scope_b, None, Some("A100"), None, None, None, None);
        assert_ne!(code_a, code_a_bundle);
        assert_eq!(code_rows.len(), 3);
        assert_eq!(code_rows[&code_a].rate_count, 3);
        assert_eq!(code_rows[&code_a].coverage_scope_id, scope_a);
        assert_eq!(
            code_rows[&code_a].reported_code_system.as_deref(),
            Some("CPT")
        );
        assert_eq!(code_rows[&code_a].reported_code.as_deref(), Some("99213"));
        assert_eq!(
            code_rows[&code_a].negotiation_arrangement.as_deref(),
            Some("FFS")
        );
        assert_eq!(code_rows[&code_a_bundle].rate_count, 1);
        assert_eq!(
            code_rows[&code_a_bundle].negotiation_arrangement.as_deref(),
            Some("BUNDLE")
        );
        assert_eq!(code_rows[&code_b].coverage_scope_id, scope_b);
        assert_eq!(code_rows[&code_b].negotiation_arrangement, None);
        assert_eq!(code_rows[&code_b].rate_count, 1);
        assert_eq!(
            code_rows.values().map(|row| row.rate_count).sum::<u64>(),
            summary_a["preservation"]["source_records"]
                .as_u64()
                .unwrap()
        );
        assert!(summary_a["preservation"]["assigned_equals_encoded"]
            .as_bool()
            .unwrap());
        assert!(
            summary_a["partition_assignment_sorts"]["aggregate"]["chunk_count"]
                .as_u64()
                .unwrap()
                > 1
        );

        let (reference_serving, reference_price) = reference_v3_assigned_records(
            &source_rows,
            &[0, 0, 1, 1, 1],
            &output_a,
            &price_ids_in_key_order,
        );
        let shared_serving = read_test_shared_binary_records(
            std::fs::read(output_a.join("shared_serving_blocks.copy")).unwrap(),
        );
        let shared_price = read_test_shared_binary_records(
            std::fs::read(output_a.join("shared_price_dictionary_blocks.copy")).unwrap(),
        );
        assert_eq!(shared_serving, reference_serving);
        assert_eq!(shared_price, reference_price);
        let audit_candidates =
            ptg2_scanner::v3_runs::read_audit_candidate_file(output_a.join("audit_candidates.bin"))
                .unwrap();
        assert_eq!(audit_candidates.len(), 5);
        assert!(audit_candidates.windows(2).all(|pair| pair[0] != pair[1]));
        let provider_code_record = shared_serving
            .iter()
            .find(|record| record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND)
            .unwrap();
        assert!(!decode_test_provider_block(
            &logical_test_payload(
                &shared_serving,
                PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
                provider_code_record.block_key,
            ),
            provider_code_record.block_key,
        )
        .is_empty());
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn direct_v3_finalizer_is_byte_identical_with_1_8_and_16_workers() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-worker-parity-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let coverage_scope_id = [0x73; COVERAGE_SCOPE_ID_BYTES];
        let partition_count = 64usize;
        let mut rows_by_partition = BTreeMap::new();
        for candidate in 0..10_000usize {
            let code: &'static str =
                Box::leak(format!("{:05}", 10_000 + candidate).into_boxed_str());
            let code_fields = NaturalLeanCodeFields {
                coverage_scope_id: &coverage_scope_id,
                reported_code_system: Some("CPT"),
                reported_code: Some(code),
                negotiation_arrangement: Some("FFS"),
                billing_code_type_version: None,
                name: None,
                description: None,
            };
            let record = ServingRunRecord {
                code_id: code_fields.identity(),
                provider_set_id: prefixed_test_id(1, candidate as u8),
                price_set_id: prefixed_test_id(2, 1),
                provider_count: candidate as u32 + 1,
            };
            let partition = partition_for_record(&record, partition_count).unwrap();
            rows_by_partition
                .entry(partition)
                .or_insert(V3FinalizerTestRow {
                    coverage_scope_id,
                    code_system: Some("CPT"),
                    code: Some(code),
                    negotiation_arrangement: Some("FFS"),
                    provider_id: record.provider_set_id,
                    price_id: record.price_set_id,
                    provider_count: record.provider_count,
                });
            if rows_by_partition.len() == 16 {
                break;
            }
        }
        assert_eq!(rows_by_partition.len(), 16);
        let rows = rows_by_partition.into_values().collect::<Vec<_>>();
        let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
            &base,
            "worker-parity",
            &rows,
            0,
            1,
            partition_count,
        );
        let price_key_map_input = write_v3_finalizer_test_price_key_map(
            &base,
            "worker-parity",
            &[prefixed_test_id(2, 1)],
        );
        let file_names = [
            "audit_candidates.bin",
            "shared_serving_blocks.copy",
            "shared_price_dictionary_blocks.copy",
            "code_dictionary.copy",
            "provider_set_dictionary.copy",
        ];
        let mut baseline_files = None;
        let mut baseline_support_digest = None;
        for workers in [1usize, 8, 16] {
            let output = base.join(format!("output-{workers}"));
            let summary = finalize_v3_runs(&V3FinalizerOptions {
                output_directory: output.clone(),
                manifest_paths: vec![manifest.clone()],
                scratch_durability: ScratchDurability::Durable,
                total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(workers, 4),
                workers,
                identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
                price_key_map_input: price_key_map_input.clone(),
                price_key_map_row_count: 1,
                price_membership_inputs: Vec::new(),
                price_atom_inputs: Vec::new(),
            })
            .unwrap();
            assert_eq!(summary["configured_workers"], workers);
            assert_eq!(summary["workers"], workers);
            let files = file_names
                .iter()
                .map(|file_name| (*file_name, std::fs::read(output.join(file_name)).unwrap()))
                .collect::<BTreeMap<_, _>>();
            let support_digest = summary["dictionaries"]["support_digest"].clone();
            if let Some(expected) = baseline_files.as_ref() {
                assert_eq!(&files, expected, "V3 output changed with {workers} workers");
                assert_eq!(
                    Some(&support_digest),
                    baseline_support_digest.as_ref(),
                    "support digest changed with {workers} workers"
                );
            } else {
                baseline_files = Some(files);
                baseline_support_digest = Some(support_digest);
            }
        }
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn direct_v3_finalizer_matches_reference_on_randomized_duplicate_heavy_rows() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let _block_bytes = TestEnvVar::set(PTG2_SERVING_BINARY_BLOCK_BYTES_ENV, "65536");
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-randomized-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let codes = [
            "10001", "10002", "10003", "10004", "20001", "20002", "20003", "20004", "30001",
            "30002", "30003", "30004", "40001", "40002", "40003", "40004",
        ];
        let mut state = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = || {
            state = state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            state
        };
        let mut rows = Vec::new();
        for index in 0..600usize {
            let code_index = next() as usize % codes.len();
            let provider_index = next() as usize % 24;
            let price_index = next() as usize % 17;
            let row = V3FinalizerTestRow {
                coverage_scope_id: [0x71; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some(codes[code_index]),
                negotiation_arrangement: Some(if code_index.is_multiple_of(2) {
                    "FFS"
                } else {
                    "BUNDLE"
                }),
                provider_id: prefixed_test_id(0x3100, provider_index as u8 + 1),
                price_id: prefixed_test_id(0x4200, price_index as u8 + 1),
                provider_count: provider_index as u32 + 1,
            };
            rows.push(row.clone());
            if index.is_multiple_of(2) {
                rows.push(row.clone());
            }
            if index.is_multiple_of(7) {
                rows.push(row);
            }
        }
        let manifest = write_v3_finalizer_test_manifest(&base, "randomized", &rows);
        let price_ids_in_key_order = (0..17usize)
            .rev()
            .map(|index| prefixed_test_id(0x4200, index as u8 + 1))
            .collect::<Vec<_>>();
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "randomized", &price_ids_in_key_order);
        let output = base.join("output");
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output.clone(),
            manifest_paths: vec![manifest],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(4, 13),
            workers: 4,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: price_ids_in_key_order.len() as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();

        let (reference_serving, reference_price) = reference_v3_assigned_records(
            &rows,
            &vec![0; rows.len()],
            &output,
            &price_ids_in_key_order,
        );
        assert_eq!(
            read_test_shared_binary_records(
                std::fs::read(output.join("shared_serving_blocks.copy")).unwrap()
            ),
            reference_serving
        );
        assert_eq!(
            read_test_shared_binary_records(
                std::fs::read(output.join("shared_price_dictionary_blocks.copy")).unwrap()
            ),
            reference_price
        );
        assert_eq!(summary["preservation"]["source_records"], rows.len() as u64);
        assert!(
            summary["preservation"]["duplicate_serving_records"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(
            summary["partition_assignment_sorts"]["strategy"],
            "immutable_dense_maps_bounded_direct_runs_v2"
        );
        assert_eq!(
            summary["partition_assignment_sorts"]["global_assignment_cascade"],
            false
        );
        assert!(summary.get("dense_assignment_sorts").is_none());
        assert!(
            summary["scratch_io"]["total_bytes_written"]
                .as_u64()
                .unwrap()
                > 0
        );
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn strict_v3_source_has_no_global_assignment_cascade_artifacts() {
        let source = include_str!("main.rs");
        for obsolete in [
            ["assigned", "-provider.unsorted"].concat(),
            ["assigned", "-price.unsorted"].concat(),
            ["join_v3_", "provider_keys"].concat(),
            ["join_v3_", "price_keys"].concat(),
        ] {
            assert!(
                !source.contains(&obsolete),
                "obsolete V3 cascade artifact: {obsolete}"
            );
        }
    }

    #[test]
    #[ignore = "focused release-mode V3 finalizer benchmark"]
    fn benchmark_v3_finalizer_250k_duplicate_heavy_rows() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let base = std::env::temp_dir().join(format!(
            "ptg2-v3-finalizer-benchmark-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let codes = [
            "10001", "10002", "10003", "10004", "20001", "20002", "20003", "20004", "30001",
            "30002", "30003", "30004", "40001", "40002", "40003", "40004",
        ];
        let rows = (0..250_000usize)
            .map(|index| {
                let duplicate_bucket = index / 2;
                let code_index = duplicate_bucket % codes.len();
                let provider_index = duplicate_bucket % 64;
                let price_index = duplicate_bucket % 32;
                V3FinalizerTestRow {
                    coverage_scope_id: [0x73; COVERAGE_SCOPE_ID_BYTES],
                    code_system: Some("CPT"),
                    code: Some(codes[code_index]),
                    negotiation_arrangement: Some("FFS"),
                    provider_id: prefixed_test_id(0x5100, provider_index as u8 + 1),
                    price_id: prefixed_test_id(0x6200, price_index as u8 + 1),
                    provider_count: provider_index as u32 + 1,
                }
            })
            .collect::<Vec<_>>();
        let manifest = write_v3_finalizer_test_manifest(&base, "benchmark", &rows);
        let price_ids = (0..32usize)
            .rev()
            .map(|index| prefixed_test_id(0x6200, index as u8 + 1))
            .collect::<Vec<_>>();
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "benchmark", &price_ids);
        let output = base.join("output");
        let started_at = Instant::now();
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output,
            manifest_paths: vec![manifest],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(8, 50_000),
            workers: 8,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: price_ids.len() as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();
        eprintln!(
            "{}",
            json!({
                "benchmark": "v3_finalizer_250k_duplicate_heavy_rows",
                "elapsed_seconds": started_at.elapsed().as_secs_f64(),
                "scratch_io": summary["scratch_io"],
                "timings": summary["timings"],
            })
        );
        assert_eq!(summary["preservation"]["encoded_records"], 250_000);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    #[ignore = "focused release-mode V3 finalizer 64-partition/two-run benchmark"]
    fn benchmark_v3_finalizer_10m_64_partitions_two_runs() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let base = std::env::temp_dir().join(format!(
            "ptg2-v3-finalizer-scaling-benchmark-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row_count = 10_000_000usize;
        let provider_cardinality = 100_000usize;
        let price_cardinality = 200_000usize;
        let partition_count = 64usize;
        let manifest = write_v3_finalizer_benchmark_manifest(
            &base,
            "scaling",
            row_count,
            provider_cardinality,
            price_cardinality,
            partition_count,
        );
        let price_ids = (0..price_cardinality)
            .map(|index| indexed_test_id(0x6200, index as u64 + 1))
            .collect::<Vec<_>>();
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "scaling", &price_ids);
        let output = base.join("output");
        let started_at = Instant::now();
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output,
            manifest_paths: vec![manifest],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(16, 100_000),
            workers: 16,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: price_cardinality as u64,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();
        eprintln!(
            "{}",
            json!({
                "benchmark": "v3_finalizer_10m_64_partitions_two_runs",
                "elapsed_seconds": started_at.elapsed().as_secs_f64(),
                "row_count": row_count,
                "provider_cardinality": provider_cardinality,
                "price_cardinality": price_cardinality,
                "partition_count": partition_count,
                "resource_configuration": summary["resource_configuration"],
                "scratch_io": summary["scratch_io"],
                "timings": summary["timings"],
                "serving_storage": summary["blocks"]["serving"]["stored_payload_bytes"],
                "price_storage": summary["blocks"]["price_dictionary"]["stored_payload_bytes"],
            })
        );
        assert_eq!(summary["preservation"]["source_records"], row_count as u64);
        assert_eq!(summary["preservation"]["encoded_records"], row_count as u64);
        assert!(summary["preservation"]["all_source_occurrences_preserved"]
            .as_bool()
            .unwrap());
        assert_eq!(
            summary["price_key_map"]["row_count"],
            price_cardinality as u64
        );
        assert_eq!(summary["partition_count"], partition_count as u64);
        assert_eq!(summary["source_identity_scan"]["passes"], 0);
        assert_eq!(summary["source_identity_scan"]["bytes_read"], 0);
        assert_eq!(summary["timings"]["identity_scan_seconds"], 0.0);
        let assignment = &summary["partition_assignment_sorts"];
        assert_eq!(
            assignment["strategy"],
            "immutable_dense_maps_bounded_direct_runs_v2"
        );
        assert_eq!(assignment["unsorted_materialization"], false);
        assert_eq!(assignment["final_sorted_materialization"], false);
        assert_eq!(assignment["aggregate"]["chunk_count"], 128);
        assert_eq!(assignment["aggregate"]["final_copy_bytes"], 0);
        let partitions = assignment["partitions"].as_array().unwrap();
        assert_eq!(partitions.len(), partition_count);
        for (partition, summary) in partitions.iter().enumerate() {
            assert_eq!(summary["partition"], partition as u64);
            assert_eq!(summary["row_count"], 156_250);
            assert_eq!(summary["sort"]["chunk_count"], 2);
            assert_eq!(summary["sort"]["final_copy_bytes"], 0);
        }
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn direct_v3_finalizer_preserves_dense_source_provenance_and_multiplicity() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "none");
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-provenance-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x44; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99213"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 2,
        };
        let source_zero = write_v3_finalizer_test_manifest_with_source(
            &base,
            "source-zero",
            &[row.clone(), row.clone()],
            0,
            2,
        );
        let source_one =
            write_v3_finalizer_test_manifest_with_source(&base, "source-one", &[row], 1, 2);
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "provenance", &[prefixed_test_id(2, 1)]);
        let output = base.join("output");
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output.clone(),
            manifest_paths: vec![source_one, source_zero],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
            workers: 2,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: 1,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();

        assert_eq!(summary["format"], "ptg2_v3_direct_finalizer_v3");
        assert_eq!(summary["storage_generation"], "shared_blocks_v3");
        assert_eq!(summary["cold_lookup_contract"], "ptg_v3_cold_v2");
        assert_eq!(summary["shared_block_layout"], "dense_shared_blocks_v3");
        assert_eq!(
            summary["resource_configuration"],
            json!({
                "contract": V3_FINALIZER_RESOURCE_CONTRACT,
                "workers": 2,
                "identity_map_max_bytes": V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
                "total_sort_memory_bytes": v3_finalizer_test_sort_memory_bytes(2, 1),
                "sort_memory_scope": V3_FINALIZER_SORT_MEMORY_SCOPE,
            })
        );
        assert_eq!(summary["source_count"], 2);
        assert_eq!(summary["source_key_bits"], 1);
        assert_eq!(summary["source_key_bytes"], 1);
        assert_eq!(summary["tagged_record_bytes"], 53);
        assert_eq!(summary["source"]["record_count"], 3);
        assert_eq!(summary["source"]["source_key_bytes"], 1);
        assert_eq!(summary["source"]["tagged_record_bytes"], 53);
        assert_eq!(summary["source_identity_scan"]["bytes_read"], 0);
        assert_eq!(summary["source_identity_scan"]["passes"], 0);
        assert_eq!(
            summary["source_identity_scan"]["tagged_partition_sort"],
            false
        );
        assert_eq!(
            summary["source"]["record_counts_by_source"],
            json!({"0": 2, "1": 1})
        );
        assert_eq!(summary["preservation"]["distinct_serving_records"], 2);
        assert_eq!(summary["preservation"]["duplicate_serving_records"], 1);
        assert_eq!(summary["preservation"]["encoded_records"], 3);
        assert!(summary["preservation"]["all_source_occurrences_preserved"]
            .as_bool()
            .unwrap());
        assert_eq!(summary["audit_candidates"]["source_key_included"], true);
        assert_eq!(summary["audit_candidates"]["source_count"], 2);
        assert_eq!(summary["audit_candidates"]["source_key_bits"], 1);
        assert_eq!(
            summary["audit_candidates"]["record_counts_by_source"],
            json!({"0": 2, "1": 1})
        );

        let audit =
            ptg2_scanner::v3_runs::read_audit_candidate_file(output.join("audit_candidates.bin"))
                .unwrap();
        assert_eq!(
            audit.iter().map(|row| row.source_key).collect::<Vec<_>>(),
            vec![0, 0, 1]
        );

        let records = read_test_shared_binary_records(
            std::fs::read(output.join("shared_serving_blocks.copy")).unwrap(),
        );
        let provider_shard =
            logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND, 0);
        let mut cursor = 0usize;
        assert_eq!(
            provider_shard[cursor],
            PTG2_SERVING_BINARY_V3_GROUPED_FORMAT_VERSION
        );
        cursor += 1;
        assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 2);
        assert_eq!(provider_shard[cursor], 1);
        cursor += 1;
        assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 0);
        assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 3);
        for _ in 0..3 {
            assert_eq!(test_read_uvarint(&provider_shard, &mut cursor), 0);
        }
        assert_eq!(&provider_shard[cursor..], &[0b0000_0100]);

        let forward =
            logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 0);
        cursor = 0;
        assert_eq!(forward[cursor], PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION);
        cursor += 1;
        assert_eq!(test_read_uvarint(&forward, &mut cursor), 2);
        assert_eq!(forward[cursor], 1);
        cursor += 1;
        assert_eq!(test_read_uvarint(&forward, &mut cursor), 3);
        for _ in 0..3 {
            assert_eq!(test_read_uvarint(&forward, &mut cursor), 0);
            assert_eq!(test_read_uvarint(&forward, &mut cursor), 2);
            assert_eq!(test_read_uvarint(&forward, &mut cursor), 0);
        }
        assert_eq!(&forward[cursor..], &[0b0000_0100]);

        let reverse =
            logical_test_payload(&records, PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND, 0);
        cursor = 0;
        assert_eq!(reverse[cursor], PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION);
        cursor += 1;
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 2);
        assert_eq!(reverse[cursor], 1);
        cursor += 1;
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 1);
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 2);
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 3);
        assert_eq!(test_read_uvarint(&reverse, &mut cursor), 3);
        for _ in 0..3 {
            assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
            assert_eq!(test_read_uvarint(&reverse, &mut cursor), 0);
        }
        assert_eq!(&reverse[cursor..], &[0b0000_0100]);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_missing_out_of_range_and_non_dense_source_metadata() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-source-metadata-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let valid_path = write_v3_finalizer_test_manifest(
            &base,
            "valid-source",
            &[V3FinalizerTestRow {
                coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: prefixed_test_id(1, 1),
                price_id: prefixed_test_id(2, 1),
                provider_count: 2,
            }],
        );
        let valid: Value = serde_json::from_slice(&std::fs::read(&valid_path).unwrap()).unwrap();
        let cases = [
            ("missing-key", None, Some(json!(1))),
            ("missing-count", Some(json!(0)), None),
            ("string-key", Some(json!("0")), Some(json!(1))),
            ("out-of-range", Some(json!(1)), Some(json!(1))),
            ("non-dense", Some(json!(0)), Some(json!(2))),
            ("zero-count", Some(json!(0)), Some(json!(0))),
        ];
        for (label, source_key, source_count) in cases {
            let mut manifest = valid.clone();
            let entry = manifest["serving_run_partition_files"][0]
                .as_object_mut()
                .unwrap();
            match source_key {
                Some(value) => {
                    entry.insert("source_key".to_owned(), value);
                }
                None => {
                    entry.remove("source_key");
                }
            }
            match source_count {
                Some(value) => {
                    entry.insert("source_count".to_owned(), value);
                }
                None => {
                    entry.remove("source_count");
                }
            }
            let path = base.join(format!("{label}.json"));
            std::fs::write(&path, serde_json::to_vec(&manifest).unwrap()).unwrap();
            let error = load_v3_finalizer_inputs(&[path]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}");
        }
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_invalid_duplicate_and_non_deterministic_source_identities() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-source-identities-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x64; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99221"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let source_zero = write_v3_finalizer_test_manifest_with_source(
            &base,
            "identity-zero",
            std::slice::from_ref(&row),
            0,
            2,
        );
        let source_one = write_v3_finalizer_test_manifest_with_source(
            &base,
            "identity-one",
            std::slice::from_ref(&row),
            1,
            2,
        );
        let zero_payload: Value =
            serde_json::from_slice(&std::fs::read(&source_zero).unwrap()).unwrap();
        let one_payload: Value =
            serde_json::from_slice(&std::fs::read(&source_one).unwrap()).unwrap();
        let zero_identity = zero_payload["source_run_contracts"][0]["source_identity"].clone();
        let one_identity = one_payload["source_run_contracts"][0]["source_identity"].clone();

        let mut duplicate_one = one_payload.clone();
        set_v3_finalizer_test_source_identity(&mut duplicate_one, zero_identity.clone());
        let duplicate_one_path = base.join("duplicate-one.json");
        std::fs::write(
            &duplicate_one_path,
            serde_json::to_vec(&duplicate_one).unwrap(),
        )
        .unwrap();
        let error =
            load_v3_finalizer_inputs(&[source_zero.clone(), duplicate_one_path]).unwrap_err();
        assert!(error
            .to_string()
            .contains("source identities must be unique"));

        let mut swapped_zero = zero_payload.clone();
        let mut swapped_one = one_payload.clone();
        set_v3_finalizer_test_source_identity(&mut swapped_zero, one_identity);
        set_v3_finalizer_test_source_identity(&mut swapped_one, zero_identity);
        let swapped_zero_path = base.join("swapped-zero.json");
        let swapped_one_path = base.join("swapped-one.json");
        std::fs::write(
            &swapped_zero_path,
            serde_json::to_vec(&swapped_zero).unwrap(),
        )
        .unwrap();
        std::fs::write(&swapped_one_path, serde_json::to_vec(&swapped_one).unwrap()).unwrap();
        let error = load_v3_finalizer_inputs(&[swapped_zero_path, swapped_one_path]).unwrap_err();
        assert!(error
            .to_string()
            .contains("source keys do not match deterministic identity ordering"));

        for (label, identity) in [
            (
                "unicode-source-type",
                json!({
                    "source_type": "in_netwörk",
                    "identity_kind": "logical_json_sha256_v1",
                    "identity_sha256": "00".repeat(32),
                }),
            ),
            (
                "unsupported-identity-kind",
                json!({
                    "source_type": "in_network",
                    "identity_kind": "sha256",
                    "identity_sha256": "00".repeat(32),
                }),
            ),
        ] {
            let mut payload = zero_payload.clone();
            set_v3_finalizer_test_source_identity(&mut payload, identity);
            let path = base.join(format!("{label}.json"));
            std::fs::write(&path, serde_json::to_vec(&payload).unwrap()).unwrap();
            let error = load_v3_finalizer_inputs(&[path]).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{label}");
        }
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_authenticates_run_content_on_both_scans() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-run-authentication-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let provider_a = prefixed_test_id(1, 1);
        let provider_b = prefixed_test_id(1, 2);
        let price_id = prefixed_test_id(2, 1);
        let rows = [
            V3FinalizerTestRow {
                coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: provider_a,
                price_id,
                provider_count: 2,
            },
            V3FinalizerTestRow {
                coverage_scope_id: [0x55; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: provider_b,
                price_id,
                provider_count: 2,
            },
        ];
        let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
            &base,
            "authenticated",
            &rows,
            0,
            1,
            1,
        );
        let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
        let (code_dictionary, _, _) = load_v3_code_dictionary(&inputs.code_dictionaries).unwrap();
        let code_map = build_v3_code_identity_map(&code_dictionary).unwrap();
        let code_partition_ranges =
            build_v3_code_partition_ranges(&code_dictionary, inputs.partition_count).unwrap();
        let mut price_map = DenseIdentityMap::with_capacity(1).unwrap();
        price_map
            .insert(
                price_id,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 0,
                },
            )
            .unwrap();
        let combined_price_seen_words = Mutex::new(vec![0u64; 1]);
        let prepared = prepare_v3_partition(
            0,
            inputs.partitions,
            &V3PartitionScanContext {
                partition_count: 1,
                price_key_count: 1,
                work_root: &base,
                code_map: &code_map,
                code_partition_ranges: &code_partition_ranges,
                price_map: &price_map,
                combined_price_seen_words: &combined_price_seen_words,
                provider_sort_record_limit: 2,
            },
        )
        .unwrap();
        let mut provider_map = DenseIdentityMap::with_capacity(2).unwrap();
        provider_map
            .insert(
                provider_a,
                DenseIdentityValue {
                    key: 0,
                    auxiliary: 2,
                },
            )
            .unwrap();
        provider_map
            .insert(
                provider_b,
                DenseIdentityValue {
                    key: 1,
                    auxiliary: 2,
                },
            )
            .unwrap();

        let run_path = &prepared.inputs[0].path;
        let mut encoded = std::fs::read(run_path).unwrap();
        let second_provider_offset = SERVING_RUN_RECORD_BYTES + GLOBAL_ID_BYTES;
        let first_provider = encoded[GLOBAL_ID_BYTES..2 * GLOBAL_ID_BYTES].to_vec();
        let second_provider =
            encoded[second_provider_offset..second_provider_offset + GLOBAL_ID_BYTES].to_vec();
        encoded[GLOBAL_ID_BYTES..2 * GLOBAL_ID_BYTES].copy_from_slice(&second_provider);
        encoded[second_provider_offset..second_provider_offset + GLOBAL_ID_BYTES]
            .copy_from_slice(&first_provider);
        std::fs::write(run_path, encoded).unwrap();

        let identity_mismatch_work = base.join("identity-mismatch");
        std::fs::create_dir_all(&identity_mismatch_work).unwrap();
        let identity_mismatch_seen = Mutex::new(vec![0u64; 1]);
        let identity_error = prepare_v3_partition(
            0,
            prepared.inputs.clone(),
            &V3PartitionScanContext {
                partition_count: 1,
                price_key_count: 1,
                work_root: &identity_mismatch_work,
                code_map: &code_map,
                code_partition_ranges: &code_partition_ranges,
                price_map: &price_map,
                combined_price_seen_words: &identity_mismatch_seen,
                provider_sort_record_limit: 2,
            },
        )
        .unwrap_err();
        assert!(identity_error
            .to_string()
            .contains("digest mismatch during identity scan"));

        let combined_provider_seen_words = Mutex::new(vec![0u64; 1]);
        let combined_assignment_price_seen_words = Mutex::new(vec![0u64; 1]);
        let error = assign_v3_partition(
            0,
            &prepared.inputs,
            &V3AssignmentContext {
                partition_count: 1,
                work_root: &base.join("assignment"),
                code_map: &code_map,
                code_partition_ranges: &code_partition_ranges,
                provider_map: &provider_map,
                provider_key_count: 2,
                price_key_count: 1,
                price_map: &price_map,
                combined_provider_seen_words: &combined_provider_seen_words,
                combined_price_seen_words: &combined_assignment_price_seen_words,
                assigned_record_limit: 2,
                scratch_durability: ScratchDurability::Durable,
            },
        )
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("digest mismatch during assignment scan"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_authenticates_code_dictionary_before_parsing() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-code-authentication-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x56; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99214"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-auth", &[row]);
        let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
        let dictionary_path = &inputs.code_dictionaries[0].path;
        let mut encoded = std::fs::read(dictionary_path).unwrap();
        encoded[..8].copy_from_slice(&u64::MAX.to_be_bytes());
        std::fs::write(dictionary_path, encoded).unwrap();

        let error = load_v3_code_dictionary(&inputs.code_dictionaries).unwrap_err();
        assert!(error.to_string().contains("content digest mismatch"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_provider_metadata_requires_canonical_network_names() {
        let canonical_values = vec![
            "Alpha".to_owned(),
            "B\"eta".to_owned(),
            "C\\name".to_owned(),
            "Line\tName".to_owned(),
        ];
        let canonical = pg_text_array_copy_field(&canonical_values);
        validate_v3_provider_network_names_copy_field(canonical.as_bytes()).unwrap();
        validate_v3_provider_network_names_copy_field(b"{}").unwrap();

        for malformed in [
            br"\N".as_slice(),
            br"{a}".as_slice(),
            br#"{"b","a"}"#.as_slice(),
            br#"{"a","a"}"#.as_slice(),
            br#"{"a",}"#.as_slice(),
            br#"{"a"}junk"#.as_slice(),
        ] {
            let error = validate_v3_provider_network_names_copy_field(malformed).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }
    }

    #[test]
    fn v3_provider_metadata_authenticates_exact_bytes_rows_and_shape() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-provider-metadata-authentication-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let provider_id = GlobalId128(prefixed_test_id(1, 1)).to_hex();
        let valid_payload = format!("{provider_id}\t1\t{{}}\n").into_bytes();
        let input_for = |path: &Path, payload: &[u8], row_count: u64| {
            let sha256: [u8; 32] = Sha256::digest(payload).into();
            V3FinalizerProviderMetadataInput {
                path: path.to_path_buf(),
                source_key: 0,
                row_count,
                bytes: payload.len() as u64,
                sha256,
            }
        };

        let tampered_path = base.join("tampered.copy");
        std::fs::write(&tampered_path, &valid_payload).unwrap();
        let tampered_input = input_for(&tampered_path, &valid_payload, 1);
        let mut tampered_payload = valid_payload.clone();
        tampered_payload[provider_id.len() + 1] = b'2';
        assert_eq!(tampered_payload.len(), valid_payload.len());
        std::fs::write(&tampered_path, tampered_payload).unwrap();
        let error = stage_v3_provider_metadata(
            &[tampered_input],
            &base.join("tampered.unsorted"),
            &base.join("tampered.sorted"),
            &base.join("tampered-work"),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("content changed"));

        let malformed_path = base.join("malformed.copy");
        let malformed_payload = format!("{provider_id}\t1\t{{}}\textra\n").into_bytes();
        std::fs::write(&malformed_path, &malformed_payload).unwrap();
        let error = stage_v3_provider_metadata(
            &[input_for(&malformed_path, &malformed_payload, 1)],
            &base.join("malformed.unsorted"),
            &base.join("malformed.sorted"),
            &base.join("malformed-work"),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("exactly three fields"));

        let row_count_path = base.join("row-count.copy");
        std::fs::write(&row_count_path, &valid_payload).unwrap();
        let error = stage_v3_provider_metadata(
            &[input_for(&row_count_path, &valid_payload, 2)],
            &base.join("row-count.unsorted"),
            &base.join("row-count.sorted"),
            &base.join("row-count-work"),
            1,
        )
        .unwrap_err();
        assert!(error.to_string().contains("row count mismatch"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_provider_metadata_bound_to_another_source_run() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-provider-metadata-source-contract-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let manifest = write_v3_finalizer_test_manifest(
            &base,
            "provider-source-contract",
            &[V3FinalizerTestRow {
                coverage_scope_id: [0x58; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99215"),
                negotiation_arrangement: Some("FFS"),
                provider_id: prefixed_test_id(1, 1),
                price_id: prefixed_test_id(2, 1),
                provider_count: 1,
            }],
        );
        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        payload["provider_set_metadata_files"][0]["source_run_contract_sha256"] =
            json!("00".repeat(32));
        complete_v3_finalizer_test_manifest_contracts(&mut payload);
        std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

        let error = load_v3_finalizer_inputs(&[manifest]).unwrap_err();
        assert!(error
            .to_string()
            .contains("bound to another source-run contract"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_accepts_multiple_dictionary_and_provider_metadata_shards_for_one_source() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-code-shards-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x60; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99218"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-shards", &[row]);
        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        let first_entry = payload["serving_run_code_dictionary_files"][0].clone();
        let first_path = PathBuf::from(first_entry["path"].as_str().unwrap());
        let second_path = first_path.with_file_name("dictionary-shard-2.ready");
        std::fs::copy(&first_path, &second_path).unwrap();
        let mut second_entry = first_entry;
        second_entry["path"] = json!(second_path);
        payload["serving_run_code_dictionary_files"]
            .as_array_mut()
            .unwrap()
            .push(second_entry);
        let first_metadata = payload["provider_set_metadata_files"][0].clone();
        let first_metadata_path = PathBuf::from(first_metadata["path"].as_str().unwrap());
        let second_metadata_path =
            first_metadata_path.with_file_name("0-provider-metadata-shard.ready");
        std::fs::copy(&first_metadata_path, &second_metadata_path).unwrap();
        let mut second_metadata = first_metadata;
        second_metadata["path"] = json!(second_metadata_path);
        payload["provider_set_metadata_files"]
            .as_array_mut()
            .unwrap()
            .push(second_metadata);
        complete_v3_finalizer_test_manifest_contracts(&mut payload);
        std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

        let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
        assert_eq!(inputs.code_dictionaries.len(), 2);
        assert_eq!(inputs.provider_metadata[0].path, second_metadata_path);
        assert_eq!(inputs.provider_metadata[1].path, first_metadata_path);
        let (codes, source_rows, source_bytes) =
            load_v3_code_dictionary(&inputs.code_dictionaries).unwrap();
        assert_eq!(codes.len(), 1);
        assert_eq!(source_rows, 2);
        assert_eq!(source_bytes, first_path.metadata().unwrap().len() * 2);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_coherent_serving_manifest_subset() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-serving-subset-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x61; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99219"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "serving-subset", &[row]);
        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        payload["serving_run_partition_files"] = json!([]);
        complete_v3_finalizer_test_manifest_contracts(&mut payload);
        std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

        let error = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap_err();
        assert!(error
            .to_string()
            .contains("files do not match complete source contract"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_dictionary_array_that_omits_declared_shard() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-dictionary-subset-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99220"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-subset", &[row]);
        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        let first_entry = payload["serving_run_code_dictionary_files"][0].clone();
        let first_path = PathBuf::from(first_entry["path"].as_str().unwrap());
        let second_path = first_path.with_file_name("dictionary-subset-shard-2.ready");
        std::fs::copy(&first_path, &second_path).unwrap();
        let mut second_entry = first_entry;
        second_entry["path"] = json!(second_path);
        payload["serving_run_code_dictionary_files"]
            .as_array_mut()
            .unwrap()
            .push(second_entry);
        complete_v3_finalizer_test_manifest_contracts(&mut payload);

        payload["serving_run_code_dictionary_files"]
            .as_array_mut()
            .unwrap()
            .pop();
        let remaining = payload["serving_run_code_dictionary_files"]
            .as_array()
            .unwrap()
            .clone();
        payload["expected_code_dictionary_files"] = json!(remaining.len());
        payload["expected_code_dictionary_rows"] = json!(remaining
            .iter()
            .map(|entry| entry["row_count"].as_u64().unwrap())
            .sum::<u64>());
        payload["expected_code_dictionary_bytes"] = json!(remaining
            .iter()
            .map(|entry| entry["bytes"].as_u64().unwrap())
            .sum::<u64>());
        let entry_contracts = remaining
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
        payload["code_dictionary_contract_set_sha256"] = json!(test_json_sha256(&json!({
            "code_dictionary_contracts": entry_contracts,
        })));
        std::fs::write(&manifest, serde_json::to_vec(&payload).unwrap()).unwrap();

        let error = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap_err();
        assert!(error
            .to_string()
            .contains("files do not match complete source contract"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_dictionary_source_contract_mismatch() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-code-contract-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x57; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99215"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "contract-auth", &[row]);
        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        payload["serving_run_code_dictionary_files"][0]["source_run_contract_sha256"] =
            json!("00".repeat(32));
        let mismatched = base.join("contract-mismatch.json");
        std::fs::write(&mismatched, serde_json::to_vec(&payload).unwrap()).unwrap();
        let error = load_v3_finalizer_inputs(&[mismatched]).unwrap_err();
        assert!(error
            .to_string()
            .contains("code dictionary source contract does not match"));

        let mut payload: Value =
            serde_json::from_slice(&std::fs::read(&manifest).unwrap()).unwrap();
        payload["source_run_contracts"][0]["row_count"] = json!(2);
        let corrupt = base.join("corrupt-source-contract.json");
        std::fs::write(&corrupt, serde_json::to_vec(&payload).unwrap()).unwrap();
        let error = load_v3_finalizer_inputs(&[corrupt]).unwrap_err();
        assert!(error
            .to_string()
            .contains("source-run contract set digest mismatch"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_rejects_dictionary_memory_before_loading_it() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-code-memory-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x58; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99216"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest(&base, "dictionary-memory", &[row]);
        let inputs = load_v3_finalizer_inputs(std::slice::from_ref(&manifest)).unwrap();
        let required = v3_code_dictionary_memory_estimate(&inputs.code_dictionaries).unwrap();
        let dictionary_path = &inputs.code_dictionaries[0].path;
        let mut encoded = std::fs::read(dictionary_path).unwrap();
        encoded[..8].copy_from_slice(&u64::MAX.to_be_bytes());
        std::fs::write(dictionary_path, encoded).unwrap();
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "memory", &[prefixed_test_id(2, 1)]);

        let error = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: base.join("output"),
            manifest_paths: vec![manifest],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 1),
            workers: 1,
            identity_map_max_bytes: required - 1,
            price_key_map_input,
            price_key_map_row_count: 1,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap_err();
        assert!(error
            .to_string()
            .contains("code dictionary resident state requires"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_counts_only_nonempty_partition_workers() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-sparse-workers-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let row = V3FinalizerTestRow {
            coverage_scope_id: [0x59; COVERAGE_SCOPE_ID_BYTES],
            code_system: Some("CPT"),
            code: Some("99217"),
            negotiation_arrangement: Some("FFS"),
            provider_id: prefixed_test_id(1, 1),
            price_id: prefixed_test_id(2, 1),
            provider_count: 1,
        };
        let manifest = write_v3_finalizer_test_manifest_with_source_and_partitions(
            &base,
            "sparse-workers",
            &[row],
            0,
            1,
            128,
        );
        let price_key_map_input =
            write_v3_finalizer_test_price_key_map(&base, "sparse", &[prefixed_test_id(2, 1)]);
        let summary = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: base.join("output"),
            manifest_paths: vec![manifest],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 2),
            workers: 16,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: 1,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap();
        assert_eq!(summary["configured_workers"], 16);
        assert_eq!(summary["workers"], 1);
        assert_eq!(
            summary["sort_memory_reserved_overhead_bytes"],
            V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
        );
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_requires_one_existing_price_key_map_input() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-price-map-cli-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let map_path = base.join("price-map.copy");
        std::fs::write(&map_path, pg_binary_copy_rows(&[])).unwrap();
        let output = base.join("output").display().to_string();
        let manifest = base.join("manifest.json").display().to_string();

        let missing = parse_v3_finalizer_options(&[output.clone(), manifest.clone()]).unwrap_err();
        assert!(missing
            .to_string()
            .contains("requires --price-key-map-input"));

        let duplicate = parse_v3_finalizer_options(&[
            output.clone(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            manifest.clone(),
        ])
        .unwrap_err();
        assert!(duplicate.to_string().contains("only once"));

        let missing_count = parse_v3_finalizer_options(&[
            output.clone(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            manifest.clone(),
        ])
        .unwrap_err();
        assert!(missing_count
            .to_string()
            .contains("requires --price-key-map-row-count"));

        for invalid_count in ["0", "4294967297"] {
            let invalid = parse_v3_finalizer_options(&[
                output.clone(),
                "--price-key-map-input".to_owned(),
                map_path.display().to_string(),
                "--price-key-map-row-count".to_owned(),
                invalid_count.to_owned(),
                manifest.clone(),
            ])
            .unwrap_err();
            assert!(invalid.to_string().contains("must be between 1 and"));
        }

        let duplicate_count = parse_v3_finalizer_options(&[
            output.clone(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-row-count".to_owned(),
            "1".to_owned(),
            "--price-key-map-row-count".to_owned(),
            "1".to_owned(),
            manifest.clone(),
        ])
        .unwrap_err();
        assert!(duplicate_count.to_string().contains("only once"));

        let options = parse_v3_finalizer_options(&[
            output,
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-row-count".to_owned(),
            "1".to_owned(),
            "--workers".to_owned(),
            "2".to_owned(),
            "--identity-map-max-bytes".to_owned(),
            (64 * 1024 * 1024usize).to_string(),
            "--total-sort-memory-bytes".to_owned(),
            (32 * 1024 * 1024usize).to_string(),
            manifest.clone(),
        ])
        .unwrap();
        assert_eq!(options.price_key_map_input, map_path);
        assert_eq!(options.price_key_map_row_count, 1);
        assert_eq!(options.workers, 2);
        assert_eq!(options.identity_map_max_bytes, 64 * 1024 * 1024);
        assert_eq!(options.total_sort_memory_bytes, 32 * 1024 * 1024);
        assert_eq!(options.scratch_durability, ScratchDurability::Durable);

        let explicit_ephemeral = parse_v3_finalizer_options(&[
            base.join("ephemeral-output").display().to_string(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--price-key-map-row-count".to_owned(),
            "1".to_owned(),
            "--workers".to_owned(),
            "2".to_owned(),
            "--identity-map-max-bytes".to_owned(),
            (64 * 1024 * 1024usize).to_string(),
            "--total-sort-memory-bytes".to_owned(),
            (32 * 1024 * 1024usize).to_string(),
            "--scratch-durability".to_owned(),
            "ephemeral".to_owned(),
            manifest.clone(),
        ])
        .unwrap();
        assert_eq!(
            explicit_ephemeral.scratch_durability,
            ScratchDurability::Ephemeral
        );

        let missing_durability = parse_v3_finalizer_options(&[
            base.join("missing-durability-output").display().to_string(),
            "--scratch-durability".to_owned(),
        ])
        .unwrap_err();
        assert!(missing_durability
            .to_string()
            .contains(v3_finalizer_usage()));

        for durability_arguments in [
            vec!["--scratch-durability", "unsafe"],
            vec![
                "--scratch-durability",
                "ephemeral",
                "--scratch-durability",
                "durable",
            ],
        ] {
            let mut arguments = vec![
                base.join("invalid-durability-output").display().to_string(),
                "--price-key-map-input".to_owned(),
                map_path.display().to_string(),
                "--price-key-map-row-count".to_owned(),
                "1".to_owned(),
                "--workers".to_owned(),
                "2".to_owned(),
                "--identity-map-max-bytes".to_owned(),
                (64 * 1024 * 1024usize).to_string(),
                "--total-sort-memory-bytes".to_owned(),
                (32 * 1024 * 1024usize).to_string(),
            ];
            arguments.extend(durability_arguments.into_iter().map(str::to_owned));
            arguments.push(manifest.clone());
            let error = parse_v3_finalizer_options(&arguments).unwrap_err();
            assert!(
                error.to_string().contains("durability") || error.to_string().contains("only once")
            );
        }

        let legacy = parse_v3_finalizer_options(&[
            base.join("legacy-output").display().to_string(),
            "--price-key-map-input".to_owned(),
            map_path.display().to_string(),
            "--memory-records".to_owned(),
            "1000".to_owned(),
            manifest,
        ])
        .unwrap_err();
        assert!(legacy.to_string().contains("process-wide"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_finalizer_sort_memory_budget_is_process_wide() {
        let total = v3_finalizer_test_sort_memory_bytes(4, 2);
        let budget = v3_sort_memory_budget(total, 4).unwrap();
        assert_eq!(budget.active_workers, 4);
        assert_eq!(
            budget.reserved_overhead_bytes,
            4 * V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
        );
        assert_eq!(budget.payload_bytes, 160);
        assert_eq!(budget.bytes_per_active_worker, 40);
        assert_eq!(
            budget.single_sort_payload_bytes,
            total - V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER
        );
        assert_eq!(budget.assigned_records_per_worker, 2);
        assert!(v3_sort_memory_budget(
            4 * V3_FINALIZER_SORT_OVERHEAD_BYTES_PER_ACTIVE_WORKER - 1,
            4
        )
        .unwrap_err()
        .to_string()
        .contains("reserve"));
    }

    #[test]
    fn v3_price_key_map_rejects_malformed_and_noncontiguous_rows() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-price-map-validation-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let id_a = prefixed_test_id(2, 1);
        let id_b = prefixed_test_id(2, 2);

        #[derive(Debug)]
        struct ValidatedPriceMap {
            mapped_a: Option<DenseIdentityValue>,
            mapped_b: Option<DenseIdentityValue>,
            summary: Value,
            output: Vec<u8>,
        }

        let validate = |label: &str,
                        rows: Vec<Vec<Option<Vec<u8>>>>,
                        expected_rows: u64|
         -> io::Result<ValidatedPriceMap> {
            let input = base.join(format!("{label}.copy"));
            std::fs::write(&input, pg_binary_copy_rows(&rows))?;
            let mut writer = CountingWriter::new(Vec::new());
            let (map, stage, summary) =
                load_v3_price_key_map_and_write_dictionary(&input, expected_rows, &mut writer)?;
            assert_eq!(stage.row_count, expected_rows);
            Ok(ValidatedPriceMap {
                mapped_a: map.get(&id_a),
                mapped_b: map.get(&id_b),
                summary,
                output: writer.inner,
            })
        };

        let valid = validate(
            "valid-key-order-with-nonmonotonic-ids",
            vec![
                vec![Some(id_b.to_vec()), pg_i64_field(0)],
                vec![Some(id_a.to_vec()), pg_i64_field(1)],
            ],
            2,
        )
        .unwrap();
        assert_eq!(valid.mapped_b.unwrap().key, 0);
        assert_eq!(valid.mapped_a.unwrap().key, 1);
        assert_eq!(valid.summary["price_set_count"], 2);

        let mut fixed_input = Vec::new();
        for (price_key, price_set_id) in [id_b, id_a].iter().enumerate() {
            fixed_input.extend_from_slice(&(price_key as u32).to_be_bytes());
            fixed_input.extend_from_slice(price_set_id);
        }
        let mut fixed_reader = Cursor::new(fixed_input);
        let mut fixed_writer = CountingWriter::new(Vec::new());
        let fixed_summary = write_serving_binary_v3_price_dictionary_copy_from_fixed_reader(
            &mut fixed_reader,
            &mut fixed_writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            V3_FINALIZER_HOT_BLOCK_BYTES,
        )
        .unwrap();
        assert_eq!(valid.output, fixed_writer.inner);
        assert_eq!(valid.summary["storage"], fixed_summary["storage"]);

        let malformed = validate(
            "malformed-int4",
            vec![vec![Some(id_a.to_vec()), pg_i32_field(0)]],
            1,
        )
        .unwrap_err();
        assert!(malformed.to_string().contains("must be int8"));

        let swapped = validate(
            "swapped-keys",
            vec![
                vec![Some(id_a.to_vec()), pg_i64_field(1)],
                vec![Some(id_b.to_vec()), pg_i64_field(0)],
            ],
            2,
        )
        .unwrap_err();
        assert!(swapped.to_string().contains("expected 0, got 1"));

        let gap = validate(
            "noncontiguous-keys",
            vec![
                vec![Some(id_a.to_vec()), pg_i64_field(0)],
                vec![Some(id_b.to_vec()), pg_i64_field(2)],
            ],
            2,
        )
        .unwrap_err();
        assert!(gap.to_string().contains("expected 1, got 2"));

        let duplicate_key = validate(
            "duplicate-keys",
            vec![
                vec![Some(id_a.to_vec()), pg_i64_field(0)],
                vec![Some(id_b.to_vec()), pg_i64_field(0)],
            ],
            2,
        )
        .unwrap_err();
        assert!(duplicate_key.to_string().contains("expected 1, got 0"));

        let duplicate_id = validate(
            "duplicate-id",
            vec![
                vec![Some(id_a.to_vec()), pg_i64_field(0)],
                vec![Some(id_a.to_vec()), pg_i64_field(1)],
            ],
            2,
        )
        .unwrap_err();
        assert!(duplicate_id
            .to_string()
            .contains("duplicate identity in immutable dense map"));

        let zero_id = validate(
            "zero-id",
            vec![vec![Some(vec![0; GLOBAL_ID_BYTES]), pg_i64_field(0)]],
            1,
        )
        .unwrap_err();
        assert!(zero_id.to_string().contains("zero global identity"));

        let too_many = validate(
            "more-than-declared",
            vec![
                vec![Some(id_a.to_vec()), pg_i64_field(0)],
                vec![Some(id_b.to_vec()), pg_i64_field(1)],
            ],
            1,
        )
        .unwrap_err();
        assert!(too_many
            .to_string()
            .contains("exceeds its declared row count 1"));

        let too_few = validate(
            "fewer-than-declared",
            vec![vec![Some(id_a.to_vec()), pg_i64_field(0)]],
            2,
        )
        .unwrap_err();
        assert!(too_few
            .to_string()
            .contains("row count mismatch: expected 2, got 1"));

        let trailing_path = base.join("trailing.copy");
        let mut trailing_copy = pg_binary_copy_rows(&[vec![Some(id_a.to_vec()), pg_i64_field(0)]]);
        trailing_copy.push(0);
        std::fs::write(&trailing_path, trailing_copy).unwrap();
        let trailing_error = load_v3_price_key_map_and_write_dictionary(
            &trailing_path,
            1,
            &mut CountingWriter::new(Vec::new()),
        )
        .err()
        .expect("trailing COPY bytes must fail");
        assert!(trailing_error
            .to_string()
            .contains("bytes after its COPY trailer"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn v3_price_key_map_requires_exact_source_id_coverage() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-price-map-coverage-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let id_a = prefixed_test_id(2, 1);
        let id_b = prefixed_test_id(2, 2);
        let id_c = prefixed_test_id(2, 3);
        let rows = [
            V3FinalizerTestRow {
                coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: prefixed_test_id(1, 1),
                price_id: id_a,
                provider_count: 1,
            },
            V3FinalizerTestRow {
                coverage_scope_id: [0x62; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: prefixed_test_id(1, 1),
                price_id: id_b,
                provider_count: 1,
            },
        ];
        let manifest = write_v3_finalizer_test_manifest(&base, "coverage", &rows);
        let finalize =
            |label: &str, ids_in_key_order: &[[u8; GLOBAL_ID_BYTES]]| -> io::Result<Value> {
                let price_key_map_input =
                    write_v3_finalizer_test_price_key_map(&base, label, ids_in_key_order);
                finalize_v3_runs(&V3FinalizerOptions {
                    output_directory: base.join(format!("{label}-output")),
                    manifest_paths: vec![manifest.clone()],
                    scratch_durability: ScratchDurability::Durable,
                    total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(1, 2),
                    workers: 1,
                    identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
                    price_key_map_input,
                    price_key_map_row_count: ids_in_key_order.len() as u64,
                    price_membership_inputs: Vec::new(),
                    price_atom_inputs: Vec::new(),
                })
            };

        let exact = finalize("exact", &[id_b, id_a]).unwrap();
        assert_eq!(exact["dense_keys"]["price"]["count"], 2);
        assert_eq!(exact["price_key_map"]["source_ids_exact_match"], true);

        let missing = finalize("missing", &[id_a]).unwrap_err();
        assert!(missing
            .to_string()
            .contains("assigned price identity is absent"));
        assert!(!base.join("missing-output").exists());

        let extra = finalize("extra", &[id_b, id_a, id_c]).unwrap_err();
        assert!(extra
            .to_string()
            .contains("authoritative price-key map contains unused price key 2"));
        assert!(!base.join("extra-output").exists());
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn direct_v3_finalizer_provider_conflict_leaves_no_output() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let base = std::env::temp_dir().join(format!(
            "ptg2-direct-v3-finalizer-conflict-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();
        let provider = prefixed_test_id(1, 1);
        let manifest_a = write_v3_finalizer_test_manifest_with_source(
            &base,
            "conflict-a",
            &[V3FinalizerTestRow {
                coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("99213"),
                negotiation_arrangement: Some("FFS"),
                provider_id: provider,
                price_id: prefixed_test_id(2, 1),
                provider_count: 2,
            }],
            0,
            2,
        );
        let manifest_b = write_v3_finalizer_test_manifest_with_source(
            &base,
            "conflict-b",
            &[V3FinalizerTestRow {
                coverage_scope_id: [0x11; COVERAGE_SCOPE_ID_BYTES],
                code_system: Some("CPT"),
                code: Some("A100"),
                negotiation_arrangement: Some("FFS"),
                provider_id: provider,
                price_id: prefixed_test_id(2, 2),
                provider_count: 3,
            }],
            1,
            2,
        );
        let price_key_map_input = write_v3_finalizer_test_price_key_map(
            &base,
            "conflict",
            &[prefixed_test_id(2, 1), prefixed_test_id(2, 2)],
        );
        let output = base.join("output");
        let error = finalize_v3_runs(&V3FinalizerOptions {
            output_directory: output.clone(),
            manifest_paths: vec![manifest_a, manifest_b],
            scratch_durability: ScratchDurability::Durable,
            total_sort_memory_bytes: v3_finalizer_test_sort_memory_bytes(2, 1),
            workers: 2,
            identity_map_max_bytes: V3_FINALIZER_DEFAULT_IDENTITY_MAP_MAX_BYTES,
            price_key_map_input,
            price_key_map_row_count: 2,
            price_membership_inputs: Vec::new(),
            price_atom_inputs: Vec::new(),
        })
        .unwrap_err();
        assert!(error.to_string().contains("provider_count"));
        assert!(!output.exists());
        assert!(std::fs::read_dir(&base).unwrap().all(|entry| {
            !entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .contains("ptg2-finalizer")
        }));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn serving_binary_v3_provider_shard_block_keys_are_collision_free_and_i64_safe() {
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(0, 0).unwrap(),
            0
        );
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(0, 1023).unwrap(),
            0
        );
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(0, 1024).unwrap(),
            0
        );
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(0, 8191).unwrap(),
            0
        );
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(0, 8192).unwrap(),
            1
        );
        assert_eq!(
            serving_binary_by_code_provider_shard_block_key(1, 0).unwrap(),
            1i64 << 31
        );
        assert!(
            serving_binary_by_code_provider_shard_block_key(0, i32::MAX).unwrap()
                < serving_binary_by_code_provider_shard_block_key(1, 0).unwrap()
        );
        let maximum_key =
            serving_binary_by_code_provider_shard_block_key(i32::MAX, i32::MAX).unwrap();
        assert_eq!(
            maximum_key,
            (i64::from(i32::MAX) << 31)
                | (i64::from(i32::MAX) / PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN)
        );
        assert!(maximum_key < (1i64 << 62));
        assert!(serving_binary_by_code_provider_shard_block_key(-1, 0).is_err());
        assert!(serving_binary_by_code_provider_shard_block_key(0, -1).is_err());
    }

    fn assigned_fixed_record(row: AssignedV3Row) -> [u8; V3_FINALIZER_ASSIGNED_BYTES] {
        let mut record = [0u8; V3_FINALIZER_ASSIGNED_BYTES];
        record[0..4].copy_from_slice(&row.code_key.to_be_bytes());
        record[4..8].copy_from_slice(&row.provider_set_key.to_be_bytes());
        record[8..12].copy_from_slice(&row.price_key.to_be_bytes());
        record[12..16].copy_from_slice(&row.source_key.to_be_bytes());
        record[16..20].copy_from_slice(&u32::try_from(row.provider_count).unwrap().to_be_bytes());
        record
    }

    fn write_assigned_fixed_records(path: &Path, rows: &[AssignedV3Row]) {
        let mut writer = BufWriter::new(File::create(path).unwrap());
        for row in rows {
            writer.write_all(&assigned_fixed_record(*row)).unwrap();
        }
        writer.flush().unwrap();
    }

    #[test]
    fn serving_binary_v3_fixed_assigned_stream_matches_pg_binary_exactly() {
        let directory = tempfile::tempdir().unwrap();
        let first_path = directory.path().join("assigned-000.bin");
        let second_path = directory.path().join("assigned-001.bin");
        let rows = [
            AssignedV3Row {
                code_key: 1,
                provider_set_key: 2,
                provider_count: 3,
                price_key: 0,
                source_key: 0,
            },
            AssignedV3Row {
                code_key: 1,
                provider_set_key: 2,
                provider_count: 3,
                price_key: 1,
                source_key: 1,
            },
            AssignedV3Row {
                code_key: 2,
                provider_set_key: 4,
                provider_count: 7,
                price_key: 2,
                source_key: 2,
            },
        ];
        write_assigned_fixed_records(&first_path, &rows[..2]);
        write_assigned_fixed_records(&second_path, &rows[2..]);
        let options = AssignedV3EncoderOptions {
            grouped_payload_bytes: 1024,
            hot_payload_bytes: 1024,
            provider_code_sort_chunk_bytes: 1024,
            provider_set_count: None,
            provider_code_count: None,
            provider_code_bitmap_max_bytes: 0,
            rate_schedule_observe: false,
            source: SourceEncoding {
                count: 3,
                key_bits: 2,
                tagged_codec: TaggedServingRunCodec::new(3, 1).unwrap(),
            },
        };

        let pg_rows = rows
            .iter()
            .map(|row| {
                vec![
                    pg_i32_field(row.code_key),
                    pg_i32_field(row.provider_set_key),
                    pg_i64_field(row.provider_count as i64),
                    pg_i64_field(i64::from(row.price_key)),
                    pg_i64_field(i64::from(row.source_key)),
                ]
            })
            .collect::<Vec<_>>();
        let mut pg_reader = Cursor::new(pg_binary_copy_rows(&pg_rows));
        let mut pg_output = CountingWriter::new(Vec::new());
        let pg_summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                &mut pg_reader,
                &mut pg_output,
                ServingBinaryTargetCopyFormat::SharedBinary,
                options,
                true,
            )
            .unwrap();

        let mut fixed_reader =
            AssignedFixedRecordStream::new_many(vec![first_path, second_path], rows.len() as u64)
                .unwrap();
        let mut fixed_output = CountingWriter::new(Vec::new());
        let fixed_summary = write_serving_binary_v3_assigned_rows_copy_with_provenance(
            &mut fixed_reader,
            &mut fixed_output,
            ServingBinaryTargetCopyFormat::SharedBinary,
            options,
        )
        .unwrap();

        assert_eq!(fixed_output.inner, pg_output.inner);
        assert_eq!(fixed_summary["row_count"], pg_summary["row_count"]);
        assert_eq!(
            fixed_summary["copy_record_count"],
            pg_summary["copy_record_count"]
        );
        assert_eq!(fixed_summary["source_copy_format"], "assigned_fixed_v1");
        assert_eq!(fixed_reader.distinct_record_count(), rows.len() as u64);
        assert_eq!(fixed_reader.duplicate_record_count(), 0);
        assert!(!fixed_reader.audit_candidates().unwrap().is_empty());
    }

    #[test]
    fn serving_binary_v3_reports_exact_observe_only_rate_schedule_reuse() {
        let rows = [
            AssignedV3Row {
                code_key: 1,
                provider_set_key: 0,
                provider_count: 1,
                price_key: 2,
                source_key: 0,
            },
            AssignedV3Row {
                code_key: 1,
                provider_set_key: 1,
                provider_count: 1,
                price_key: 2,
                source_key: 0,
            },
            AssignedV3Row {
                code_key: 2,
                provider_set_key: 0,
                provider_count: 1,
                price_key: 3,
                source_key: 1,
            },
            AssignedV3Row {
                code_key: 2,
                provider_set_key: 1,
                provider_count: 1,
                price_key: 3,
                source_key: 1,
            },
        ];
        let pg_rows = rows
            .iter()
            .map(|row| {
                vec![
                    pg_i32_field(row.code_key),
                    pg_i32_field(row.provider_set_key),
                    pg_i64_field(row.provider_count as i64),
                    pg_i64_field(i64::from(row.price_key)),
                    pg_i64_field(i64::from(row.source_key)),
                ]
            })
            .collect::<Vec<_>>();
        let mut reader = Cursor::new(pg_binary_copy_rows(&pg_rows));
        let mut writer = CountingWriter::new(Vec::new());
        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                AssignedV3EncoderOptions {
                    grouped_payload_bytes: 1024,
                    hot_payload_bytes: 1024,
                    provider_code_sort_chunk_bytes: 1024,
                    provider_set_count: Some(2),
                    provider_code_count: Some(3),
                    provider_code_bitmap_max_bytes: 1024,
                    rate_schedule_observe: true,
                    source: SourceEncoding {
                        count: 2,
                        key_bits: 1,
                        tagged_codec: TaggedServingRunCodec::new(2, 1).unwrap(),
                    },
                },
                true,
            )
            .unwrap();

        assert_eq!(summary["rate_schedule_observe"]["enabled"], true);
        assert_eq!(
            summary["rate_schedule_observe"]["representation_effect"],
            "observe_only_no_serving_change"
        );
        assert_eq!(summary["rate_schedule_observe"]["provider_set_count_s"], 2);
        assert_eq!(
            summary["rate_schedule_observe"]["distinct_schedule_count_k"],
            1
        );
        assert_eq!(
            summary["rate_schedule_observe"]["rate_occurrence_count_r"],
            4
        );
        assert_eq!(
            summary["rate_schedule_observe"]["unique_schedule_occurrence_count_u"],
            2
        );
        assert_eq!(
            summary["rate_schedule_observe"]["distinct_schedule_code_incidence_count_i"],
            2
        );
        assert_eq!(
            summary["rate_schedule_observe"]["weighted_reuse_r_over_u"],
            2.0
        );
        assert_eq!(
            summary["rate_schedule_observe"]["occurrence_external_sort"],
            false
        );
        assert_eq!(summary["rate_schedule_observe"]["scratch_bytes_written"], 0);
    }

    #[test]
    fn assigned_fixed_stream_rejects_unordered_and_partial_records() {
        let directory = tempfile::tempdir().unwrap();
        let unordered_path = directory.path().join("unordered.bin");
        let later = AssignedV3Row {
            code_key: 2,
            provider_set_key: 0,
            provider_count: 1,
            price_key: 0,
            source_key: 0,
        };
        let earlier = AssignedV3Row {
            code_key: 1,
            ..later
        };
        write_assigned_fixed_records(&unordered_path, &[later, earlier]);
        let mut unordered = AssignedFixedRecordStream::new_many(vec![unordered_path], 2).unwrap();
        let error = unordered.next_row().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("input is not sorted"));

        let partial_path = directory.path().join("partial.bin");
        std::fs::write(&partial_path, [0u8; V3_FINALIZER_ASSIGNED_BYTES - 1]).unwrap();
        let mut partial = AssignedFixedRecordStream::new_many(vec![partial_path], 1).unwrap();
        let error = partial.next_row().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        assert!(error.to_string().contains("not aligned to 20 bytes"));
    }

    #[test]
    fn serving_binary_direct_group_append_matches_temporary_encoder_bytes() {
        let reference = |provider_set_key: i32,
                         previous_provider_set_key: i32,
                         occurrence_count: usize,
                         price_payload: &[u8],
                         source_payload: &[u8]| {
            let mut encoded = Vec::new();
            write_uvarint_to_vec(
                &mut encoded,
                (provider_set_key - previous_provider_set_key) as u64,
            );
            write_uvarint_to_vec(&mut encoded, occurrence_count as u64);
            encoded.extend_from_slice(price_payload);
            encoded.extend_from_slice(source_payload);
            encoded
        };
        let cases = [
            (0, 0, 1, vec![0], vec![]),
            (127, 0, 127, vec![0x7f, 0x80, 0x01], vec![0xaa; 16]),
            (128, 127, 128, vec![0x80, 0x01], vec![0x55; 17]),
            (128, 0, 1, vec![0], vec![]),
            (16_384, 128, 16_384, vec![0xff; 257], vec![0x11; 33]),
        ];
        for (provider_set_key, previous_provider_set_key, occurrence_count, prices, sources) in
            cases
        {
            let expected = reference(
                provider_set_key,
                previous_provider_set_key,
                occurrence_count,
                &prices,
                &sources,
            );
            let layout = serving_by_code_group_layout(
                provider_set_key,
                previous_provider_set_key,
                occurrence_count,
                &prices,
                &sources,
            )
            .unwrap();
            let mut actual = vec![0xde, 0xad];
            append_serving_by_code_group(&mut actual, layout, occurrence_count, &prices, &sources);
            assert_eq!(&actual[2..], expected);
            assert_eq!(layout.encoded_bytes, expected.len());
        }

        let continuing = serving_by_code_group_layout(128, 127, 1, &[0], &[]).unwrap();
        let fragment_reset = serving_by_code_group_layout(128, 0, 1, &[0], &[]).unwrap();
        assert_eq!(continuing.provider_delta, 1);
        assert_eq!(fragment_reset.provider_delta, 128);
        assert_eq!(fragment_reset.encoded_bytes, continuing.encoded_bytes + 1);
    }

    #[test]
    fn serving_binary_dense_provider_projections_match_sparse_copy_exactly() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let input = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(0),
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i64_field(0),
            ],
            vec![
                pg_i32_field(0),
                pg_i32_field(1),
                pg_i32_field(3),
                pg_i64_field(1),
            ],
            vec![
                pg_i32_field(1),
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i64_field(1),
            ],
            vec![
                pg_i32_field(1),
                pg_i32_field(2),
                pg_i32_field(1),
                pg_i64_field(2),
            ],
            vec![
                pg_i32_field(2),
                pg_i32_field(2),
                pg_i32_field(1),
                pg_i64_field(3),
            ],
        ]);
        let encode = |provider_set_count| {
            let mut reader = Cursor::new(input.clone());
            let mut writer = CountingWriter::new(Vec::new());
            let summary =
                write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                    &mut reader,
                    &mut writer,
                    ServingBinaryTargetCopyFormat::SharedBinary,
                    AssignedV3EncoderOptions {
                        grouped_payload_bytes: 1024,
                        hot_payload_bytes: 1024,
                        provider_code_sort_chunk_bytes: 1024,
                        provider_set_count,
                        provider_code_count: None,
                        provider_code_bitmap_max_bytes: 0,
                        rate_schedule_observe: false,
                        source: SourceEncoding {
                            count: 1,
                            key_bits: 0,
                            tagged_codec: TaggedServingRunCodec::new(1, 0).unwrap(),
                        },
                    },
                    false,
                )
                .unwrap();
            let block_summary = writer.shared_block_summary(Path::new("blocks.copy"));
            (writer.inner, block_summary, summary)
        };
        let (sparse_copy, sparse_blocks, sparse_summary) = encode(None);
        let (dense_copy, dense_blocks, dense_summary) = encode(Some(3));
        assert_eq!(dense_copy, sparse_copy);
        assert_eq!(dense_blocks, sparse_blocks);
        assert_eq!(
            sparse_summary["provider_set_page"]["provider_projection"]["storage_kind"],
            "sparse_unbounded_v1",
        );
        assert_eq!(
            dense_summary["provider_set_page"]["provider_projection"]["storage_kind"],
            "dense_vec_v1",
        );
        assert_eq!(
            dense_summary["provider_set_page"]["provider_projection"]["entry_count"],
            3,
        );
    }

    #[test]
    fn dense_provider_projection_bounds_and_charge_are_explicit() {
        let dense_entry_bytes = std::mem::size_of::<Option<ServingBinaryV3ProviderProjection>>()
            + PTG2_SERVING_BINARY_V3_PAGE_ROWS * std::mem::size_of::<(i32, u32, u32)>();
        assert!(dense_entry_bytes <= V3_FINALIZER_PROVIDER_PROJECTION_MAX_BYTES_PER_ENTRY);
        assert_eq!(
            v3_finalizer_provider_projection_max_bytes(68_587).unwrap(),
            68_587 * V3_FINALIZER_PROVIDER_PROJECTION_MAX_BYTES_PER_ENTRY,
        );
        assert!(v3_finalizer_provider_projection_max_bytes(usize::MAX).is_err());

        let mut dense = ServingBinaryV3ProviderProjections::new(Some(2));
        let error = dense.push(2, 1, 0, 0, 0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        dense.push(0, 1, 0, 0, 0).unwrap();
        let error = dense.validate_complete().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let mut sparse = ServingBinaryV3ProviderProjections::new(None);
        sparse.push(100, 1, 0, 0, 0).unwrap();
        assert_eq!(sparse.len(), 1);
        sparse.validate_complete().unwrap();
    }

    #[test]
    fn provider_code_bitmap_matches_external_sort_copy_exactly() {
        let _env_lock = scanner_env_lock().lock().unwrap();
        let _compression = TestEnvVar::set(PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION_ENV, "zlib");
        let input = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i32_field(7),
                pg_i64_field(0),
            ],
            vec![
                pg_i32_field(63),
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i64_field(1),
            ],
            vec![
                pg_i32_field(63),
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i64_field(2),
            ],
            vec![
                pg_i32_field(63),
                pg_i32_field(2),
                pg_i32_field(7),
                pg_i64_field(3),
            ],
            vec![
                pg_i32_field(64),
                pg_i32_field(1),
                pg_i32_field(5),
                pg_i64_field(4),
            ],
            vec![
                pg_i32_field(127),
                pg_i32_field(0),
                pg_i32_field(2),
                pg_i64_field(5),
            ],
            vec![
                pg_i32_field(127),
                pg_i32_field(1),
                pg_i32_field(5),
                pg_i64_field(6),
            ],
            vec![
                pg_i32_field(127),
                pg_i32_field(2),
                pg_i32_field(7),
                pg_i64_field(7),
            ],
        ]);
        let encode = |bitmap_maximum_bytes| {
            let mut reader = Cursor::new(input.clone());
            let mut writer = CountingWriter::new(Vec::new());
            let summary =
                write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                    &mut reader,
                    &mut writer,
                    ServingBinaryTargetCopyFormat::SharedBinary,
                    AssignedV3EncoderOptions {
                        grouped_payload_bytes: 1024,
                        hot_payload_bytes: 1024,
                        provider_code_sort_chunk_bytes: PROVIDER_CODE_PAIR_RECORD_BYTES,
                        provider_set_count: Some(3),
                        provider_code_count: Some(128),
                        provider_code_bitmap_max_bytes: bitmap_maximum_bytes,
                        rate_schedule_observe: false,
                        source: SourceEncoding {
                            count: 1,
                            key_bits: 0,
                            tagged_codec: TaggedServingRunCodec::new(1, 0).unwrap(),
                        },
                    },
                    false,
                )
                .unwrap();
            let block_summary = writer.shared_block_summary(Path::new("blocks.copy"));
            (writer.inner, block_summary, summary)
        };

        let (sorted_copy, sorted_blocks, sorted_summary) = encode(0);
        let (bitmap_copy, bitmap_blocks, bitmap_summary) = encode(48);
        assert_eq!(bitmap_copy, sorted_copy);
        assert_eq!(bitmap_blocks, sorted_blocks);
        assert_eq!(
            sorted_summary["provider_set_codes"]["index_mode"],
            "pair_spool_sort_v1",
        );
        assert_eq!(
            sorted_summary["provider_set_codes"]["bitmap_fallback_reason"],
            "memory_cap_exceeded",
        );
        assert_eq!(
            sorted_summary["provider_set_codes"]["bitmap_candidate_bytes"],
            48,
        );
        assert_eq!(
            sorted_summary["provider_set_codes"]["bitmap_charged_bytes"],
            0,
        );
        assert_eq!(sorted_summary["provider_set_codes"]["external_sort"], true);
        assert!(
            sorted_summary["provider_set_codes"]["sort_scratch_bytes_read"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["index_mode"],
            "provider_major_bitmap_v1",
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["bitmap_candidate_bytes"],
            48,
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["bitmap_charged_bytes"],
            48,
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["sort_scratch_bytes_read"],
            0,
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["sort_scratch_bytes_written"],
            0,
        );
        assert_eq!(bitmap_summary["provider_set_codes"]["external_sort"], false);
        assert_eq!(
            bitmap_summary["provider_set_codes"]["spool_input_pair_count"],
            7,
        );
        assert_eq!(
            bitmap_summary["provider_set_codes"]["spool_unique_pair_count"],
            7,
        );
    }

    #[test]
    fn provider_code_bitmap_layout_bounds_and_fallback_are_explicit() {
        let high_cardinality_layout = provider_code_bitmap_layout_bytes(68_587, 18_807).unwrap();
        assert_eq!(high_cardinality_layout, (294, 161_316_624),);
        assert!(high_cardinality_layout.1 <= V3_FINALIZER_PROVIDER_CODE_BITMAP_MAX_BYTES);
        assert!(provider_code_bitmap_layout_bytes(1, usize::MAX).is_err());
        assert!(provider_code_bitmap_layout_bytes(usize::MAX, 64).is_err());

        let candidate_bytes = provider_code_bitmap_layout_bytes(2, 65).unwrap().1;
        let capped =
            ServingBinaryV3ProviderCodeIndex::new(Some(2), Some(65), candidate_bytes - 1).unwrap();
        assert!(matches!(
            capped,
            ServingBinaryV3ProviderCodeIndex::Spool {
                bitmap_candidate_bytes: Some(bytes),
                bitmap_fallback_reason: "memory_cap_exceeded",
                ..
            } if bytes == candidate_bytes
        ));

        let mut bitmap =
            ServingBinaryV3ProviderCodeIndex::new(Some(2), Some(65), candidate_bytes).unwrap();
        bitmap.push(0, 64).unwrap();
        let error = bitmap.push(2, 0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let error = bitmap.push(0, 65).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        let error = bitmap.push(-1, 0).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn serving_binary_v3_provider_shards_follow_code_and_shard_transitions_and_summary() {
        let input = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(1),
                pg_i32_field(8191),
                pg_i32_field(2),
                pg_i64_field(0),
            ],
            vec![
                pg_i32_field(1),
                pg_i32_field(8192),
                pg_i32_field(3),
                pg_i64_field(1),
            ],
            vec![
                pg_i32_field(2),
                pg_i32_field(8192),
                pg_i32_field(3),
                pg_i64_field(2),
            ],
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let shard_records = records
            .iter()
            .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
            .collect::<Vec<_>>();
        assert_eq!(shard_records.len(), 3);
        assert_eq!(
            shard_records
                .iter()
                .map(|record| (record.block_key, record.block_no, record.entry_count))
                .collect::<Vec<_>>(),
            vec![
                ((1i64 << 31), 0, 1),
                ((1i64 << 31) | 1, 0, 1),
                ((2i64 << 31) | 1, 0, 1),
            ]
        );
        assert_eq!(
            summary["artifact_kind"],
            PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND
        );
        assert_eq!(summary["provider_shard_span"], 8192);
        assert_eq!(summary["block_count"], 3);
        assert_eq!(summary["by_code_copy_record_count"], 3);
        assert_eq!(summary["by_code_provider_shard"]["block_count"], 3);
        assert_eq!(summary["by_code_provider_shard"]["copy_record_count"], 3);
        assert_eq!(
            summary["emitted_artifact_kinds"],
            json!([
                PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND,
                PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND,
                PTG2_SERVING_BINARY_PROVIDER_COUNT_DICTIONARY_KIND,
                PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND,
                PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND,
            ])
        );
        assert!(!records
            .iter()
            .any(|record| record.kind == "by_code_grouped_v2"));
        assert!(!serde_json::to_string(&summary)
            .unwrap()
            .contains("by_code_grouped_v2"));
    }

    #[test]
    fn serving_binary_v3_provider_shard_fragments_reset_headers_and_provider_deltas() {
        let input = pg_binary_copy_rows(
            &[126, 127, 128]
                .into_iter()
                .map(|provider_set_key| {
                    vec![
                        pg_i32_field(4),
                        pg_i32_field(provider_set_key),
                        pg_i32_field(1),
                        pg_i64_field(i64::from(provider_set_key - 126)),
                    ]
                })
                .collect::<Vec<_>>(),
        );
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_limits(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                8,
                1024,
            )
            .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let shard_records = records
            .iter()
            .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
            .collect::<Vec<_>>();
        assert_eq!(shard_records.len(), 3);
        assert_eq!(
            shard_records
                .iter()
                .map(|record| record.block_no)
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        for (record, expected_provider_set_key) in shard_records.iter().zip([126, 127, 128]) {
            assert_eq!(record.block_key, 4i64 << 31);
            assert_eq!(record.entry_count, 1);
            assert_eq!(
                &record.payload[..3],
                &[PTG2_SERVING_BINARY_V3_GROUPED_FORMAT_VERSION, 1, 0]
            );
            assert_eq!(
                decode_test_by_code_provider_shard_fragment(record),
                vec![(
                    expected_provider_set_key,
                    vec![((expected_provider_set_key - 126) as u32, 0)]
                )]
            );
        }
        assert_eq!(summary["block_count"], 1);
        assert_eq!(summary["by_code_copy_record_count"], 3);
    }

    #[test]
    fn serving_binary_v3_provider_shard_continuations_bound_duplicate_occurrences() {
        let input = pg_binary_copy_rows(
            &(0..8)
                .map(|_| {
                    vec![
                        pg_i32_field(4),
                        pg_i32_field(5),
                        pg_i32_field(1),
                        pg_i64_field(0),
                    ]
                })
                .collect::<Vec<_>>(),
        );
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_limits(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                8,
                1024,
            )
            .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let shard_records = records
            .iter()
            .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
            .collect::<Vec<_>>();
        assert_eq!(shard_records.len(), 3);
        assert!(shard_records.iter().all(|record| record.payload.len() <= 8));
        assert_eq!(
            shard_records
                .iter()
                .flat_map(|record| decode_test_by_code_provider_shard_fragment(record))
                .flat_map(|(provider_set_key, occurrences)| {
                    occurrences
                        .into_iter()
                        .map(move |occurrence| (provider_set_key, occurrence))
                })
                .collect::<Vec<_>>(),
            vec![(5, (0, 0)); 8]
        );
        assert_eq!(summary["group_count"], 1);
        assert_eq!(
            summary["by_code_provider_shard"]["occurrence_chunk_count"],
            3
        );
        assert_eq!(
            summary["by_code_provider_shard"]["maximum_occurrences_buffered"],
            3
        );
    }

    #[test]
    fn serving_binary_v3_provider_shards_decode_exact_rows_with_source_provenance() {
        let source = SourceEncoding {
            count: 3,
            key_bits: 2,
            tagged_codec: TaggedServingRunCodec::new(3, 1).unwrap(),
        };
        let expected_rows = vec![
            (7, 8191, 2, 0),
            (7, 8191, 3, 2),
            (7, 8192, 5, 1),
            (8, 8192, 6, 2),
        ];
        let input = pg_binary_copy_rows(
            &expected_rows
                .iter()
                .map(|(code_key, provider_set_key, price_key, source_key)| {
                    vec![
                        pg_i32_field(*code_key),
                        pg_i32_field(*provider_set_key),
                        pg_i32_field(4),
                        pg_i64_field(i64::from(*price_key)),
                        pg_i64_field(i64::from(*source_key)),
                    ]
                })
                .collect::<Vec<_>>(),
        );
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary =
            write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader_with_provenance(
                &mut reader,
                &mut writer,
                ServingBinaryTargetCopyFormat::SharedBinary,
                AssignedV3EncoderOptions {
                    grouped_payload_bytes: 1024,
                    hot_payload_bytes: 1024,
                    provider_code_sort_chunk_bytes: 1024,
                    provider_set_count: None,
                    provider_code_count: None,
                    provider_code_bitmap_max_bytes: 0,
                    rate_schedule_observe: false,
                    source,
                },
                true,
            )
            .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let mut decoded_rows = Vec::new();
        for record in records
            .iter()
            .filter(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_KIND)
        {
            let code_key = i32::try_from(record.block_key >> 31).unwrap();
            let shard_no = record.block_key & ((1i64 << 31) - 1);
            for (provider_set_key, occurrences) in
                decode_test_by_code_provider_shard_fragment(record)
            {
                assert_eq!(
                    i64::from(provider_set_key) / PTG2_SERVING_BINARY_BY_CODE_PROVIDER_SHARD_SPAN,
                    shard_no
                );
                decoded_rows.extend(occurrences.into_iter().map(|(price_key, source_key)| {
                    (code_key, provider_set_key, price_key, source_key)
                }));
            }
        }
        assert_eq!(decoded_rows, expected_rows);
        assert_eq!(summary["row_count"], 4);
        assert_eq!(summary["group_count"], 3);
        assert_eq!(summary["block_count"], 3);
        assert_eq!(summary["source_count"], 3);
        assert_eq!(summary["source_key_bits"], 2);
    }

    #[test]
    fn serving_binary_v3_provider_codes_match_golden_and_dedupe_pairs() {
        let input = pg_binary_copy_rows(&[
            vec![pg_i32_field(4), pg_i32_field(1)],
            vec![pg_i32_field(4), pg_i32_field(1)],
            vec![pg_i32_field(4), pg_i32_field(2)],
            vec![pg_i32_field(1024), pg_i32_field(65_536)],
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            64 * 1024,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);

        assert_eq!(records.len(), 2);
        assert!(records.iter().all(|record| {
            record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
                && record.block_no == 0
                && record.entry_count == 1
        }));
        assert_eq!(records[0].block_key, 0);
        assert_eq!(records[1].block_key, 1);
        assert_eq!(records[0].payload, [1, 4, 8, 1, 1, 0, 1, 2, 2, 1, 1]);
        assert_eq!(records[1].payload, [1, 0, 7, 1, 1, 1, 1, 1, 1, 0]);
        assert_eq!(
            decode_test_provider_block(&records[0].payload, 0),
            BTreeMap::from([(4, vec![1, 2])])
        );
        assert_eq!(
            decode_test_provider_block(&records[1].payload, 1),
            BTreeMap::from([(1024, vec![65_536])])
        );
        assert_eq!(summary["format"], PTG2_SERVING_BINARY_V3_FORMAT);
        assert_eq!(
            summary["artifact_kind"],
            PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND
        );
        assert_eq!(summary["row_count"], 4);
        assert_eq!(summary["pair_count"], 3);
        assert_eq!(summary["duplicate_pair_count"], 1);
        assert_eq!(summary["provider_set_count"], 2);
        assert_eq!(summary["block_span"], 1024);
        assert_eq!(summary["block_count"], 2);
        assert_eq!(summary["storage"]["entry_count"], 2);
        assert_eq!(
            summary["target_copy_format"],
            "postgres_binary_shared_blocks"
        );
    }

    #[test]
    fn serving_binary_v3_provider_codes_reject_order_and_copy_corruption() {
        let unordered = pg_binary_copy_rows(&[
            vec![pg_i32_field(2), pg_i32_field(8)],
            vec![pg_i32_field(2), pg_i32_field(7)],
        ]);
        let mut reader = Cursor::new(unordered);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be ordered"));

        let mut missing_trailer = Vec::new();
        write_pg_binary_copy_header(&mut missing_trailer).unwrap();
        missing_trailer.extend_from_slice(&2i16.to_be_bytes());
        append_pg_binary_field(&mut missing_trailer, &1i32.to_be_bytes());
        append_pg_binary_field(&mut missing_trailer, &2i32.to_be_bytes());
        let mut reader = Cursor::new(missing_trailer);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(error.to_string().contains("missing its trailer"));

        let wrong_shape = pg_binary_copy_rows(&[vec![pg_i32_field(1)]]);
        let mut reader = Cursor::new(wrong_shape);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_provider_codes_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must have 2 fields"));
    }

    #[test]
    fn serving_binary_v3_page_projections_cap_and_report_dense_rows() {
        let assigned_rows = (0..100)
            .map(|price_key| {
                vec![
                    pg_i32_field(7),
                    pg_i32_field(3),
                    pg_i32_field(10),
                    pg_i64_field(price_key),
                ]
            })
            .collect::<Vec<_>>();
        let input = pg_binary_copy_rows(&assigned_rows);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            serving_binary_block_bytes(),
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);

        assert_eq!(summary["by_code_page"]["row_count"], 64);
        assert_eq!(summary["by_code_page"]["code_count"], 1);
        assert_eq!(summary["provider_set_page"]["row_count"], 64);
        assert_eq!(summary["provider_set_page"]["source_row_count"], 100);
        assert_eq!(summary["provider_set_page"]["provider_set_count"], 1);
        assert_eq!(summary["provider_set_page"]["block_span"], 1);
        assert_eq!(
            summary["provider_set_page"]["truncated_provider_set_count"],
            1
        );

        let forward_payload =
            logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 7);
        let mut cursor = 0usize;
        assert_eq!(
            forward_payload[cursor],
            PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION
        );
        cursor += 1;
        assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 1);
        assert_eq!(forward_payload[cursor], 0);
        cursor += 1;
        assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 64);
        for expected_price_key in 0..64 {
            assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 3);
            assert_eq!(test_read_uvarint(&forward_payload, &mut cursor), 10);
            assert_eq!(
                test_read_uvarint(&forward_payload, &mut cursor),
                expected_price_key
            );
        }
        assert_eq!(cursor, forward_payload.len());

        let provider_payload =
            logical_test_payload(&records, PTG2_SERVING_BINARY_PROVIDER_SET_PAGE_V3_KIND, 3);
        cursor = 0;
        assert_eq!(
            provider_payload[cursor],
            PTG2_SERVING_BINARY_PAGE_FORMAT_VERSION
        );
        cursor += 1;
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 1);
        assert_eq!(provider_payload[cursor], 0);
        cursor += 1;
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 1);
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 0);
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 10);
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 100);
        assert_eq!(test_read_uvarint(&provider_payload, &mut cursor), 64);
        for expected_price_key in 0..64 {
            let expected_code_delta = if expected_price_key == 0 { 7 } else { 0 };
            assert_eq!(
                test_read_uvarint(&provider_payload, &mut cursor),
                expected_code_delta
            );
            assert_eq!(
                test_read_uvarint(&provider_payload, &mut cursor),
                expected_price_key
            );
        }
        assert_eq!(cursor, provider_payload.len());
    }

    #[test]
    fn serving_binary_v3_forward_page_ranks_price_before_provider_count() {
        let mut assigned_rows = vec![vec![
            pg_i32_field(7),
            pg_i32_field(0),
            pg_i32_field(10_000),
            pg_i64_field(64),
        ]];
        assigned_rows.extend((0..64).map(|price_key| {
            vec![
                pg_i32_field(7),
                pg_i32_field(price_key + 1),
                pg_i32_field(1),
                pg_i64_field(i64::from(price_key)),
            ]
        }));
        let mut reader = Cursor::new(pg_binary_copy_rows(&assigned_rows));
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            serving_binary_block_bytes(),
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let payload =
            logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_PRICE_PAGE_V4_KIND, 7);
        let mut cursor = 0usize;
        assert_eq!(payload[cursor], 4);
        cursor += 1;
        assert_eq!(test_read_uvarint(&payload, &mut cursor), 1);
        assert_eq!(payload[cursor], 0);
        cursor += 1;
        assert_eq!(test_read_uvarint(&payload, &mut cursor), 64);
        for expected_price_key in 0..64u64 {
            assert_eq!(
                test_read_uvarint(&payload, &mut cursor),
                expected_price_key + 1
            );
            assert_eq!(test_read_uvarint(&payload, &mut cursor), 1);
            assert_eq!(test_read_uvarint(&payload, &mut cursor), expected_price_key);
        }
        assert_eq!(cursor, payload.len());
        assert_eq!(
            summary["by_code_page"]["artifact_kind"],
            "by_code_price_page_v4"
        );
        assert_eq!(summary["by_code_page"]["payload_format_version"], 4);
        assert_eq!(
            summary["by_code_page"]["ranking"],
            json!([
                "price_key",
                "provider_set_key",
                "source_key",
                "provider_count"
            ])
        );
    }

    #[test]
    fn serving_binary_v3_assigned_forward_externally_sorts_provider_codes() {
        let _lock = scanner_env_lock().lock().unwrap();
        let _sort_chunk = TestEnvVar::set(
            PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
            "16",
        );
        let input = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(1),
                pg_i32_field(10),
                pg_i32_field(2),
                pg_i64_field(0),
            ],
            vec![
                pg_i32_field(1),
                pg_i32_field(12),
                pg_i32_field(3),
                pg_i64_field(1),
            ],
            vec![
                pg_i32_field(2),
                pg_i32_field(10),
                pg_i32_field(2),
                pg_i64_field(2),
            ],
            vec![
                pg_i32_field(2),
                pg_i32_field(12),
                pg_i32_field(3),
                pg_i64_field(3),
            ],
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let provider_record = records
            .iter()
            .find(|record| record.kind == PTG2_SERVING_BINARY_PROVIDER_SET_CODES_V3_KIND)
            .unwrap();

        assert_eq!(
            decode_test_provider_block(&provider_record.payload, provider_record.block_key),
            BTreeMap::from([(10, vec![1, 2]), (12, vec![1, 2])])
        );
        assert_eq!(summary["provider_set_codes"]["external_sort"], true);
        assert_eq!(summary["provider_set_codes"]["sort_chunk_count"], 2);
        assert_eq!(summary["provider_set_codes"]["spool_bytes"], 32);
        assert_eq!(summary["provider_set_codes"]["spool_unique_pair_count"], 4);
    }

    #[test]
    fn serving_binary_v3_provider_code_sort_stays_bounded_beyond_one_merge_fan_in() {
        let _lock = scanner_env_lock().lock().unwrap();
        let _sort_chunk = TestEnvVar::set(
            PTG2_SERVING_BINARY_V3_PROVIDER_CODE_SORT_CHUNK_BYTES_ENV,
            "8",
        );
        let rows = (1..=130)
            .map(|provider_set_key| {
                vec![
                    pg_i32_field(1),
                    pg_i32_field(provider_set_key),
                    pg_i32_field(1),
                    pg_i64_field(i64::from(provider_set_key - 1)),
                ]
            })
            .collect::<Vec<_>>();
        let mut reader = Cursor::new(pg_binary_copy_rows(&rows));
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap();
        let provider_codes = &summary["provider_set_codes"];

        assert_eq!(provider_codes["external_sort"], true);
        assert_eq!(provider_codes["spool_input_pair_count"], 130);
        assert_eq!(provider_codes["spool_unique_pair_count"], 130);
        assert_eq!(provider_codes["sort_chunk_count"], 130);
        assert_eq!(provider_codes["merge_pass_count"], 2);
        assert_eq!(provider_codes["maximum_merge_fan_in"], 64);
        assert_eq!(provider_codes["sort_scratch_bytes_read"], 4_144);
        assert_eq!(provider_codes["sort_scratch_bytes_written"], 4_144);
        assert!(
            provider_codes["block_spool_scratch_bytes_read"]
                .as_u64()
                .unwrap()
                > 0
        );
        assert_eq!(
            provider_codes["scratch_bytes_read"].as_u64().unwrap(),
            provider_codes["sort_scratch_bytes_read"].as_u64().unwrap()
                + provider_codes["block_spool_scratch_bytes_read"]
                    .as_u64()
                    .unwrap()
        );
        assert_eq!(
            provider_codes["scratch_bytes_written"].as_u64().unwrap(),
            provider_codes["sort_scratch_bytes_written"]
                .as_u64()
                .unwrap()
                + provider_codes["block_spool_scratch_bytes_written"]
                    .as_u64()
                    .unwrap()
        );
    }

    #[test]
    fn provider_code_merge_pass_count_includes_eager_fan_in_boundaries() {
        assert_eq!(provider_code_merge_pass_count(0), 0);
        assert_eq!(provider_code_merge_pass_count(1), 1);
        assert_eq!(provider_code_merge_pass_count(63), 1);
        assert_eq!(provider_code_merge_pass_count(64), 2);
        assert_eq!(provider_code_merge_pass_count(65), 2);
        assert_eq!(provider_code_merge_pass_count(4_032), 2);
        assert_eq!(provider_code_merge_pass_count(4_095), 3);
        assert_eq!(provider_code_merge_pass_count(4_096), 3);
    }

    #[test]
    fn v3_finalizer_encoder_workspace_contract_is_exact_and_checked() {
        let block_bytes = 64 * 1024;
        let code_count = 17usize;
        let active_workers = 8usize;
        assert_eq!(
            v3_finalizer_encoder_workspace_max_bytes(block_bytes, code_count, active_workers,)
                .unwrap(),
            block_bytes * V3_FINALIZER_ENCODER_BLOCK_BUFFER_MULTIPLIER
                + code_count * V3_FINALIZER_ENCODER_PROVIDER_CODE_BYTES_PER_CODE
                + V3_FINALIZER_ENCODER_FIXED_WORKSPACE_BYTES
                + V3_FINALIZER_SHARED_BLOCK_BATCH_MAX_RAW_BYTES
                    * V3_FINALIZER_SHARED_BLOCK_BATCH_MEMORY_MULTIPLIER
                + active_workers * V3_FINALIZER_SHARED_BLOCK_COMPRESSION_WORKSPACE_BYTES_PER_WORKER
        );
        assert!(v3_finalizer_encoder_workspace_max_bytes(usize::MAX, 1, 1).is_err());
        assert!(v3_finalizer_encoder_workspace_max_bytes(1, usize::MAX, 1).is_err());
        assert!(v3_finalizer_encoder_workspace_max_bytes(1, 1, usize::MAX).is_err());
    }

    #[test]
    fn serving_binary_v3_assigned_forward_rejects_order_and_count_changes() {
        let unordered = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(2),
                pg_i32_field(1),
                pg_i32_field(3),
                pg_i32_field(0),
            ],
            vec![
                pg_i32_field(1),
                pg_i32_field(1),
                pg_i32_field(3),
                pg_i32_field(0),
            ],
        ]);
        let mut reader = Cursor::new(unordered);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must be ordered"));

        let changed_count = pg_binary_copy_rows(&[
            vec![
                pg_i32_field(1),
                pg_i32_field(4),
                pg_i32_field(2),
                pg_i32_field(0),
            ],
            vec![
                pg_i32_field(2),
                pg_i32_field(4),
                pg_i32_field(3),
                pg_i32_field(1),
            ],
        ]);
        let mut reader = Cursor::new(changed_count);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_assigned_by_code_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("provider_count changed"));
    }

    #[test]
    fn serving_binary_v3_price_dictionary_streams_dense_aligned_fragments() {
        let price_ids = [
            test_price_id(0xa1),
            test_price_id(0xa2),
            test_price_id(0xa3),
        ];
        let input = pg_binary_copy_rows(
            &price_ids
                .iter()
                .enumerate()
                .map(|(price_key, price_id)| {
                    vec![pg_i64_field(price_key as i64), Some(price_id.to_vec())]
                })
                .collect::<Vec<_>>(),
        );
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            32,
        )
        .unwrap();
        let pg_binary_output = writer.inner;
        let records = read_test_shared_binary_records(pg_binary_output.clone());
        let expected_payload = price_ids.into_iter().flatten().collect::<Vec<_>>();

        let mut fixed_input = Vec::new();
        for (price_key, price_id) in price_ids.iter().enumerate() {
            fixed_input.extend_from_slice(&(price_key as u32).to_be_bytes());
            fixed_input.extend_from_slice(price_id);
        }
        let mut fixed_reader = Cursor::new(fixed_input);
        let mut fixed_writer = CountingWriter::new(Vec::new());
        let fixed_summary = write_serving_binary_v3_price_dictionary_copy_from_fixed_reader(
            &mut fixed_reader,
            &mut fixed_writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            32,
        )
        .unwrap();

        assert_eq!(records.len(), 2);
        assert_eq!(fixed_writer.inner, pg_binary_output);
        assert_eq!(fixed_summary["price_set_count"], summary["price_set_count"]);
        assert_eq!(fixed_summary["storage"], summary["storage"]);
        assert!(records
            .iter()
            .all(|record| record.kind == PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND));
        assert_eq!(records[0].block_key, 0);
        assert_eq!(records[0].block_no, 0);
        assert_eq!(records[0].entry_count, 2);
        assert_eq!(records[0].payload.len(), 32);
        assert_eq!(records[1].block_no, 1);
        assert_eq!(records[1].entry_count, 1);
        assert_eq!(records[1].payload.len(), 16);
        assert_eq!(
            logical_test_payload(&records, PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND, 0,),
            expected_payload
        );
        assert_eq!(
            summary["encoder_kind"],
            PTG2_SERVING_BINARY_PRICE_DICTIONARY_V3_ENCODER_KIND
        );
        assert_eq!(
            summary["artifact_kind"],
            PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND
        );
        assert_eq!(summary["price_set_count"], 3);
        assert_eq!(summary["copy_record_count"], 2);
        assert_eq!(summary["storage"]["entry_count"], 3);
    }

    #[test]
    fn serving_binary_v3_price_dictionary_rejects_dense_gaps_and_emits_empty_artifact() {
        let gap = pg_binary_copy_rows(&[
            vec![pg_i32_field(0), Some(test_price_id(0xa1).to_vec())],
            vec![pg_i32_field(2), Some(test_price_id(0xa2).to_vec())],
        ]);
        let mut reader = Cursor::new(gap);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expected 1, got 2"));

        let text_id = pg_binary_copy_rows(&[vec![
            pg_i32_field(0),
            Some(b"00000000000000000000000000000001".to_vec()),
        ]]);
        let mut reader = Cursor::new(text_id);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap_err();
        assert!(error.to_string().contains("must contain 16 bytes"));

        let mut reader = Cursor::new(pg_binary_copy_rows(&[]));
        let mut writer = CountingWriter::new(Vec::new());
        let summary = write_serving_binary_v3_price_dictionary_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, PTG2_SERVING_BINARY_BY_CODE_DICTIONARY_KIND);
        assert_eq!(records[0].entry_count, 0);
        assert!(records[0].payload.is_empty());
        assert_eq!(summary["price_set_count"], 0);
        assert_eq!(summary["copy_record_count"], 1);
    }

    #[test]
    fn serving_binary_v3_memberships_fragment_and_span_price_keys() {
        let input = pg_binary_copy_rows(&[
            vec![pg_i32_field(0), pg_i32_field(1)],
            vec![pg_i32_field(0), pg_i32_field(2)],
            vec![pg_i32_field(512), pg_i32_field(7)],
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            8,
            24,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let block_zero_records = records
            .iter()
            .filter(|record| record.block_key == 0)
            .collect::<Vec<_>>();

        assert!(block_zero_records.len() > 1);
        assert_eq!(block_zero_records[0].entry_count, 1);
        for (block_number, record) in block_zero_records.iter().enumerate() {
            assert_eq!(record.block_no, block_number as i32);
            if block_number > 0 {
                assert_eq!(record.entry_count, 0);
            }
        }
        assert!(records.iter().all(|record| record.payload.len() <= 8));
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
                &records,
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
                0,
            ))
            .unwrap(),
            BTreeMap::from([(0, vec![1, 2])])
        );
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
                &records,
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
                1,
            ))
            .unwrap(),
            BTreeMap::from([(512, vec![7])])
        );
        assert_eq!(summary["atom_key_bits"], 24);
        assert_eq!(summary["atom_key_bytes"], 3);
        assert_eq!(summary["block_span"], 512);
        assert_eq!(summary["block_count"], 2);
        assert_eq!(summary["price_set_count"], 2);
        assert_eq!(summary["atom_reference_count"], 3);
        assert!(summary["copy_record_count"].as_u64().unwrap() > 2);
    }

    #[test]
    fn deferred_v3_atom_streams_emit_shared_block_staging_copy() {
        let target_format = ServingBinaryTargetCopyFormat::SharedBinary;
        let membership_input = pg_binary_copy_rows(&[vec![pg_i32_field(0), pg_i32_field(7)]]);
        let mut membership_reader = Cursor::new(membership_input);
        let mut membership_writer = CountingWriter::new(Vec::new());
        write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
            &mut membership_reader,
            &mut membership_writer,
            target_format,
            V3_FINALIZER_HOT_BLOCK_BYTES,
            24,
        )
        .unwrap();
        let membership_records = read_test_shared_binary_records(membership_writer.inner);
        assert_eq!(membership_records.len(), 1);
        assert_eq!(
            membership_records[0].kind,
            PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND
        );

        let atom_input = pg_binary_copy_rows(&[pg_v3_price_atom_row(0, None, [None; 7])]);
        let mut atom_reader = Cursor::new(atom_input);
        let mut atom_writer = CountingWriter::new(Vec::new());
        write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut atom_reader,
            &mut atom_writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            V3_FINALIZER_HOT_BLOCK_BYTES,
            24,
        )
        .unwrap();
        let atom_records = read_test_shared_binary_records(atom_writer.inner);
        assert_eq!(atom_records.len(), 1);
        assert_eq!(
            atom_records[0].kind,
            PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND
        );
    }

    #[test]
    fn serving_binary_v3_memberships_enforce_width_and_strict_pairs() {
        let wide_atom = pg_binary_copy_rows(&[vec![pg_i32_field(0), pg_i64_field(1 << 24)]]);
        let mut reader = Cursor::new(wide_atom.clone());
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not fit in 24 bits"));

        let mut reader = Cursor::new(wide_atom);
        let mut writer = CountingWriter::new(Vec::new());
        let summary = write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            32,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        assert_eq!(summary["atom_key_bytes"], 4);
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
                &records,
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
                0,
            ))
            .unwrap(),
            BTreeMap::from([(0, vec![1 << 24])])
        );

        let duplicate = pg_binary_copy_rows(&[
            vec![pg_i32_field(0), pg_i32_field(1)],
            vec![pg_i32_field(0), pg_i32_field(1)],
        ]);
        let mut reader = Cursor::new(duplicate);
        let mut writer = CountingWriter::new(Vec::new());
        write_serving_binary_v3_memberships_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_memberships(&logical_test_payload(
                &records,
                PTG2_SERVING_BINARY_PRICE_SET_ATOM_MEMBERSHIPS_V3_KIND,
                0,
            ))
            .unwrap(),
            BTreeMap::from([(0, vec![1, 1])])
        );
        assert!(serving_binary_v3_validate_atom_key(0, 16).is_err());
        assert!(serving_binary_v3_validate_atom_key(-1, 24).is_err());
        assert_eq!(serving_binary_v3_atom_key_bits("24").unwrap(), 24);
        assert_eq!(serving_binary_v3_atom_key_bits("32").unwrap(), 32);
        assert!(serving_binary_v3_atom_key_bits("3").is_err());
        assert!(serving_binary_i32_count(usize::MAX, "count").is_err());
        assert!(serving_binary_v3_block_key(i64::MAX, 1, "key").is_err());

        let _env_guard = scanner_env_lock().lock().unwrap();
        {
            let _atom_bits = TestEnvVar::set(PTG2_SERVING_BINARY_V3_ATOM_KEY_BITS_ENV, "32");
            assert_eq!(serving_binary_v3_configured_atom_key_bits(&[]).unwrap(), 32);
        }
        let _atom_bits = TestEnvVar::remove(PTG2_SERVING_BINARY_V3_ATOM_KEY_BITS_ENV);
        let _atom_count = TestEnvVar::set(PTG2_SERVING_BINARY_V3_ATOM_COUNT_ENV, "not-a-count");
        assert!(serving_binary_v3_configured_atom_key_bits(&[]).is_err());
    }

    #[test]
    fn serving_binary_v3_price_atoms_decode_numeric_and_text_goldens() {
        let input = pg_binary_copy_rows(&[
            pg_v3_price_atom_row(
                0,
                Some(pg_binary_numeric(false, 0, 2, &[15, 2500])),
                [Some(0), None, Some(2), Some(3), Some(4), Some(5), None],
            ),
            pg_v3_price_atom_row(
                1,
                Some(b"20.50".to_vec()),
                [
                    Some(1),
                    Some(8),
                    Some(13),
                    None,
                    Some(21),
                    Some(34),
                    Some(55),
                ],
            ),
        ]);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            64 * 1024,
            24,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);
        let atoms = ptg2_serving_binary_v3::decode_price_atoms(&logical_test_payload(
            &records,
            PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND,
            0,
        ))
        .unwrap();

        assert_eq!(atoms.len(), 2);
        assert_eq!(atoms[0].negotiated_rate.as_deref(), Some("15.25"));
        assert_eq!(atoms[1].negotiated_rate.as_deref(), Some("20.50"));
        assert_eq!(
            atoms[0].attribute_keys,
            vec![Some(0), None, Some(2), Some(3), Some(4), Some(5), None]
        );
        assert_eq!(
            summary["artifact_kind"],
            PTG2_SERVING_BINARY_PRICE_ATOMS_V3_KIND
        );
        assert_eq!(summary["atom_count"], 2);
        assert_eq!(summary["attribute_count"], 7);
        assert_eq!(summary["block_span"], 512);
        assert_eq!(
            pg_binary_numeric_text(&pg_binary_numeric(false, -1, 2, &[100])).unwrap(),
            "0.01"
        );
        assert_eq!(
            pg_binary_numeric_text(&pg_binary_numeric(true, 1, 0, &[1, 2])).unwrap(),
            "-10002"
        );
    }

    #[test]
    fn serving_binary_v3_price_atoms_use_dense_512_atom_spans() {
        let rows = (0..=512)
            .map(|atom_key| pg_v3_price_atom_row(atom_key, None, [None; 7]))
            .collect::<Vec<_>>();
        let input = pg_binary_copy_rows(&rows);
        let mut reader = Cursor::new(input);
        let mut writer = CountingWriter::new(Vec::new());

        let summary = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            64 * 1024,
            24,
        )
        .unwrap();
        let records = read_test_shared_binary_records(writer.inner);

        assert_eq!(summary["atom_count"], 513);
        assert_eq!(summary["block_count"], 2);
        assert_eq!(
            records
                .iter()
                .map(|record| record.block_key)
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(records[0].entry_count, 512);
        assert_eq!(records[1].entry_count, 1);
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_atoms(&records[0].payload)
                .unwrap()
                .len(),
            512
        );
        assert_eq!(
            ptg2_serving_binary_v3::decode_price_atoms(&records[1].payload)
                .unwrap()
                .len(),
            1
        );
    }

    #[test]
    fn serving_binary_v3_price_atoms_reject_gaps_null_keys_and_corrupt_numeric() {
        let dense_gap = pg_binary_copy_rows(&[pg_v3_price_atom_row(1, None, [None; 7])]);
        let mut reader = Cursor::new(dense_gap);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap_err();
        assert!(error.to_string().contains("expected 0, got 1"));

        let mut null_atom_row = pg_v3_price_atom_row(0, None, [None; 7]);
        null_atom_row[0] = None;
        let mut reader = Cursor::new(pg_binary_copy_rows(&[null_atom_row]));
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap_err();
        assert!(error.to_string().contains("atom_key cannot be NULL"));

        let corrupt_numeric =
            pg_binary_copy_rows(&[pg_v3_price_atom_row(0, Some(vec![0; 7]), [None; 7])]);
        let mut reader = Cursor::new(corrupt_numeric);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap_err();
        assert!(error.to_string().contains("invalid byte count"));

        let negative_attribute = pg_binary_copy_rows(&[pg_v3_price_atom_row(
            0,
            None,
            [Some(-1), None, None, None, None, None, None],
        )]);
        let mut reader = Cursor::new(negative_attribute);
        let mut writer = CountingWriter::new(Vec::new());
        let error = write_serving_binary_v3_price_atoms_copy_from_pg_binary_reader(
            &mut reader,
            &mut writer,
            ServingBinaryTargetCopyFormat::SharedBinary,
            1024,
            24,
        )
        .unwrap_err();
        assert!(error.to_string().contains("cannot be negative"));
    }

    #[test]
    fn manifest_sidecar_collector_sorts_and_merges_members() {
        let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
        let mut collector = ManifestSidecarCollector::default();

        collector
            .record_provider_set(
                provider_set_id,
                &[20, 10, 20],
                &[91, 92],
                &[1003002106, 1003007311],
            )
            .unwrap();

        let entries = collector.provider_forward_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].owner, provider_set_id);
        assert_eq!(entries[0].members.len(), 2);
        assert!(entries[0].members[0] < entries[0].members[1]);
        let inverted = collector.provider_inverted_entries().unwrap();
        assert_eq!(inverted.len(), 2);
        assert!(inverted
            .iter()
            .all(|entry| entry.members == vec![provider_set_id]));
        let provider_npi = collector.provider_npi_entries().unwrap();
        assert_eq!(provider_npi.len(), 1);
        assert_eq!(provider_npi[0].owner, provider_set_id);
        assert_eq!(provider_npi[0].members.len(), 2);
    }

    #[test]
    fn manifest_sidecar_collector_can_spill_members_before_sidecar_write() {
        let provider_set_id = GlobalId128([5; GLOBAL_ID_BYTES]);
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::all().unwrap()),
            ..ManifestSidecarCollector::default()
        };

        collector
            .record_provider_set(
                provider_set_id,
                &[20, 10, 20],
                &[91, 92],
                &[1003002106, 1003007311],
            )
            .unwrap();

        assert!(collector.provider_forward.is_empty());
        assert!(collector.provider_inverted.is_empty());
        assert!(collector.provider_npi.is_empty());
        let entries = collector.provider_forward_entries().unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].owner, provider_set_id);
        assert_eq!(entries[0].members.len(), 2);
        let inverted = collector.provider_inverted_entries().unwrap();
        assert_eq!(inverted.len(), 2);
        assert!(inverted
            .iter()
            .all(|entry| entry.members == vec![provider_set_id]));
        assert_eq!(collector.provider_npi_entries().unwrap().len(), 1);
        assert!(collector.price_forward_entries().unwrap().is_empty());
    }

    #[test]
    fn manifest_sidecar_spools_only_open_configured_artifacts() {
        let paths = CopyPathConfig {
            manifest_provider_forward_sidecar: Some("provider-forward.ptg2sc".to_string()),
            ..CopyPathConfig::default()
        };

        let spools = ManifestSidecarSpools::for_paths(&paths).unwrap();
        let import = ManifestSidecarCollector::for_import(&paths).unwrap();

        assert!(spools.provider_forward.is_some());
        assert!(spools.provider_inverted.is_none());
        assert!(spools.provider_npi.is_none());
        assert!(spools.price_forward.is_none());
        assert!(import.spools.is_some());
    }

    #[test]
    fn configured_manifest_sidecars_write_independent_spools_in_parallel() {
        let base =
            std::env::temp_dir().join(format!("ptg2-parallel-sidecar-test-{}", std::process::id()));
        let forward_path = base.with_extension("forward.ptg2sc");
        let inverted_path = base.with_extension("inverted.ptg2sc");
        let paths = CopyPathConfig {
            manifest_provider_forward_sidecar: Some(forward_path.display().to_string()),
            manifest_provider_inverted_sidecar: Some(inverted_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::for_paths(&paths).unwrap()),
            ..ManifestSidecarCollector::default()
        };
        collector
            .record_provider_set(
                GlobalId128([5; GLOBAL_ID_BYTES]),
                &[20, 10, 20],
                &[91, 92],
                &[1003002106, 1003007311],
            )
            .unwrap();

        let semantic_progress = Arc::new(ScannerSemanticProgress::default());
        let results = configured_spooled_manifest_sidecars(
            &paths,
            &mut collector,
            Some(Arc::clone(&semantic_progress)),
        )
        .unwrap()
        .unwrap();

        assert_eq!(results.len(), 2);
        assert_eq!(
            results[0].record_kind,
            "manifest_provider_forward_sidecar_file"
        );
        assert_eq!(results[0].entry_count, 1);
        assert_eq!(
            results[1].record_kind,
            "manifest_provider_inverted_sidecar_file"
        );
        assert_eq!(results[1].entry_count, 2);
        assert_eq!(&std::fs::read(&forward_path).unwrap()[..8], b"PTG2MNDS");
        assert_eq!(&std::fs::read(&inverted_path).unwrap()[..8], b"PTG2MNDS");
        let progress = semantic_progress.snapshot();
        assert_eq!(progress.scan_finalize_jobs_started, 2);
        assert_eq!(progress.scan_finalize_jobs_completed, 2);
        assert!(progress.scan_finalize_bytes_processed > 0);
        assert!(progress.scan_finalize_pairs_processed > 0);
        assert_eq!(progress.scan_finalize_chunks_sorted, 2);
        let _ = std::fs::remove_file(forward_path);
        let _ = std::fs::remove_file(inverted_path);
    }

    #[test]
    fn configured_manifest_sidecars_recreate_attempt_output_directory() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-sidecar-parent-test-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        let output_path = base.join("nested/provider-forward.ptg2sc");
        let paths = CopyPathConfig {
            manifest_provider_forward_sidecar: Some(output_path.display().to_string()),
            ..CopyPathConfig::default()
        };
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::for_paths(&paths).unwrap()),
            ..ManifestSidecarCollector::default()
        };
        collector
            .record_provider_set(
                GlobalId128([5; GLOBAL_ID_BYTES]),
                &[20],
                &[91],
                &[1003002106],
            )
            .unwrap();

        let results = configured_spooled_manifest_sidecars(&paths, &mut collector, None)
            .unwrap()
            .unwrap();

        assert_eq!(results.len(), 1);
        assert_eq!(&std::fs::read(&output_path).unwrap()[..8], b"PTG2MNDS");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn external_manifest_sidecar_sort_recreates_attempt_output_directory() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-external-sidecar-parent-test-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        let output_path = base.join("nested/provider-forward.ptg2sc");
        let mut spool = ManifestPairSpool::new("external_parent_recovery").unwrap();
        let owner = GlobalId128([5; GLOBAL_ID_BYTES]);
        spool
            .push(owner, GlobalId128([6; GLOBAL_ID_BYTES]))
            .unwrap();
        spool
            .push(owner, GlobalId128([7; GLOBAL_ID_BYTES]))
            .unwrap();

        spool
            .write_dense_sidecar_with_chunk_bytes(
                &output_path.display().to_string(),
                MANIFEST_PAIR_RECORD_BYTES,
            )
            .unwrap();

        assert_eq!(&std::fs::read(&output_path).unwrap()[..8], b"PTG2MNDS");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_sidecar_spool_failure_names_source_and_output_paths() {
        let output_path = std::env::temp_dir().join(format!(
            "ptg2-missing-spool-output-{}-{:?}.ptg2sc",
            std::process::id(),
            thread::current().id()
        ));
        let spool = ManifestPairSpool::new("missing_spool_diagnostic").unwrap();
        let spool_path = spool.path.clone();
        std::fs::remove_file(&spool_path).unwrap();

        let error = match write_manifest_sidecar_job(
            ManifestSidecarWriteJob {
                order: 0,
                record_kind: "manifest_provider_forward_sidecar_file",
                path: output_path.display().to_string(),
                spool,
            },
            None,
        ) {
            Ok(_) => panic!("missing manifest spool unexpectedly finalized"),
            Err(error) => error,
        };
        let message = error.to_string();

        assert!(message.contains("manifest_provider_forward_sidecar_file"));
        assert!(message.contains(&spool_path.display().to_string()));
        assert!(message.contains(&output_path.display().to_string()));
        let _ = std::fs::remove_file(output_path);
    }

    #[test]
    fn manifest_dense_sidecar_external_sort_matches_in_memory_format() {
        let output_prefix = std::env::temp_dir().join(format!(
            "ptg2-dense-sidecar-sort-test-{}",
            std::process::id()
        ));
        let in_memory_path = output_prefix.with_extension("memory.ptg2sc");
        let external_path = output_prefix.with_extension("external.ptg2sc");
        let owner_a = GlobalId128([1; GLOBAL_ID_BYTES]);
        let owner_b = GlobalId128([2; GLOBAL_ID_BYTES]);
        let member_a = GlobalId128([7; GLOBAL_ID_BYTES]);
        let member_b = GlobalId128([8; GLOBAL_ID_BYTES]);
        let member_c = GlobalId128([9; GLOBAL_ID_BYTES]);
        let pairs = [
            (owner_b, member_c),
            (owner_a, member_b),
            (owner_a, member_a),
            (owner_b, member_a),
            (owner_a, member_b),
            (owner_b, member_b),
        ];
        let mut in_memory_spool = ManifestPairSpool::new("dense_parity_memory").unwrap();
        let mut external_spool = ManifestPairSpool::new("dense_parity_external").unwrap();
        for (owner, member) in pairs {
            in_memory_spool.push(owner, member).unwrap();
            external_spool.push(owner, member).unwrap();
        }

        let in_memory_metrics = in_memory_spool
            .write_dense_sidecar_with_chunk_bytes(in_memory_path.to_str().unwrap(), usize::MAX)
            .unwrap();
        let semantic_progress = Arc::new(ScannerSemanticProgress::default());
        let external_metrics = external_spool
            .write_dense_sidecar_with_chunk_bytes_and_progress(
                external_path.to_str().unwrap(),
                MANIFEST_PAIR_RECORD_BYTES * 2,
                Some(Arc::clone(&semantic_progress)),
            )
            .unwrap();

        assert_eq!(external_metrics, in_memory_metrics);
        assert_eq!(external_metrics, (2, 5));
        assert_eq!(
            std::fs::read(&external_path).unwrap(),
            std::fs::read(&in_memory_path).unwrap()
        );
        let progress = semantic_progress.snapshot();
        assert_eq!(progress.scan_finalize_jobs_started, 1);
        assert_eq!(progress.scan_finalize_jobs_completed, 1);
        assert!(progress.scan_finalize_bytes_processed > 0);
        assert!(progress.scan_finalize_pairs_processed > pairs.len() as u64);
        assert_eq!(progress.scan_finalize_chunks_sorted, 3);
        assert_eq!(progress.scan_finalize_chunks_merged, 3);
        assert!(progress.scan_finalize_sort_comparisons > 0);
        let _ = std::fs::remove_file(in_memory_path);
        let _ = std::fs::remove_file(external_path);
    }

    #[test]
    fn manifest_standard_sidecar_is_sorted_deduplicated_and_handles_empty_input() {
        let temporary = tempfile::tempdir().unwrap();
        let populated_path = temporary.path().join("populated.ptg2sc");
        let empty_path = temporary.path().join("empty.ptg2sc");
        let owner_a = GlobalId128([1; GLOBAL_ID_BYTES]);
        let owner_b = GlobalId128([2; GLOBAL_ID_BYTES]);
        let member_a = GlobalId128([7; GLOBAL_ID_BYTES]);
        let member_b = GlobalId128([8; GLOBAL_ID_BYTES]);
        let mut populated = ManifestPairSpool::new("standard_populated").unwrap();
        for pair in [
            (owner_b, member_b),
            (owner_a, member_b),
            (owner_a, member_a),
            (owner_a, member_b),
        ] {
            populated.push(pair.0, pair.1).unwrap();
        }
        assert_eq!(
            populated
                .write_standard_sidecar(populated_path.to_str().unwrap())
                .unwrap(),
            (2, 3)
        );
        let bytes = std::fs::read(&populated_path).unwrap();
        assert_eq!(&bytes[..8], b"PTG2MNSC");
        assert_eq!(u32::from_le_bytes(bytes[8..12].try_into().unwrap()), 1);
        assert_eq!(u64::from_le_bytes(bytes[12..20].try_into().unwrap()), 2);
        assert_eq!(bytes.len(), 20 + 2 * 28 + 3 * GLOBAL_ID_BYTES);

        let mut empty = ManifestPairSpool::new("standard_empty").unwrap();
        assert_eq!(
            empty
                .write_standard_sidecar(empty_path.to_str().unwrap())
                .unwrap(),
            (0, 0)
        );
        assert_eq!(std::fs::read(&empty_path).unwrap().len(), 20);
    }

    #[test]
    fn manifest_copy_merge_dedupes_by_kind_key() {
        let base = std::env::temp_dir().join(format!("ptg2-merge-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        std::fs::write(&input_a, b"b\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\t\\N\n").unwrap();
        std::fs::write(&input_b, b"a\t1\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n").unwrap();

        merge_manifest_copy_files(
            "manifest_serving",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, "a\t1\nb\t2\nmanifest\t1\t2\t3\t4\t5\t6\t7\ttrace\n");
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_dedupes_lean_serving_by_full_row() {
        let base =
            std::env::temp_dir().join(format!("ptg2-lean-merge-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        let row_a = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-a\n";
        let row_b = "plan\tCPT\t29888\tprovider-set\t2\tprice-set-b\n";
        std::fs::write(&input_a, format!("{row_b}{row_b}")).unwrap();
        std::fs::write(&input_b, row_a).unwrap();

        merge_manifest_copy_files(
            "manifest_lean_serving",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, format!("{row_a}{row_b}"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_dedupes_price_set_atoms_by_full_pair() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-price-set-atom-merge-test-{}",
            std::process::id()
        ));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let output = base.join("out.copy");
        let atom_a = "price-set-a\tatom-a\n";
        let atom_b = "price-set-a\tatom-b\n";
        std::fs::write(&input_a, format!("{atom_b}{atom_b}")).unwrap();
        std::fs::write(&input_b, atom_a).unwrap();

        merge_manifest_copy_files(
            "price_set_atom",
            &output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        let merged = std::fs::read_to_string(&output).unwrap();
        assert_eq!(merged, format!("{atom_a}{atom_b}"));
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn manifest_copy_merge_parallel_chunk_sort_matches_serial_output() {
        let base =
            std::env::temp_dir().join(format!("ptg2-merge-parallel-test-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&base);
        let input_a = base.join("a.copy");
        let input_b = base.join("b.copy");
        let serial_output = base.join("serial.copy");
        let parallel_output = base.join("parallel.copy");
        let payload_a = "a".repeat(700_000);
        let payload_b = "b".repeat(700_000);
        let payload_c = "c".repeat(700_000);
        std::fs::write(
            &input_a,
            format!(
                "g2\t200\t{payload_a}\n\
                 g1\t100\t{payload_b}\n\
                 g3\t300\t{payload_c}\n\
                 g1\t100\t{payload_b}\n"
            ),
        )
        .unwrap();
        std::fs::write(
            &input_b,
            format!(
                "g4\t400\t{payload_a}\n\
                 g2\t200\t{payload_a}\n\
                 g5\t500\t{payload_c}\n"
            ),
        )
        .unwrap();

        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");
        merge_manifest_copy_files(
            "provider_group_member",
            &serial_output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();

        std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS", "2");
        std::env::set_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES", "1");
        merge_manifest_copy_files(
            "provider_group_member",
            &parallel_output,
            &[
                input_a.to_string_lossy().to_string(),
                input_b.to_string_lossy().to_string(),
            ],
        )
        .unwrap();
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_SORT_WORKERS");
        std::env::remove_var("HLTHPRT_PTG2_MANIFEST_MERGE_CHUNK_BYTES");

        let serial = std::fs::read_to_string(&serial_output).unwrap();
        let parallel = std::fs::read_to_string(&parallel_output).unwrap();
        assert_eq!(parallel, serial);
        assert_eq!(parallel.lines().count(), 5);
        let _ = std::fs::remove_dir_all(base);
    }

    #[test]
    fn raw_rate_chunks_flush_when_byte_limit_is_reached() {
        let payload = br#"{
            "billing_code_type": "CPT",
            "billing_code": "99213",
            "negotiated_rates": [
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":100}]},
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":101}]},
                {"provider_references":[7],"negotiated_prices":[{"negotiated_type":"negotiated","negotiated_rate":102}]}
            ]
        }"#;
        let mut reader = JsonStreamReader::new(&payload[..]);
        let (tx, rx) = bounded::<WorkerJob>(10);
        let (_event_tx, event_rx) = bounded::<CopyFileEvent>(10);
        let mut writer = Vec::new();
        let mut producer_blocked_micros = 0u128;
        let mut stats = RawChunkStats::default();
        let mut copy_file_event_gate = CopyFileEventGate::passthrough();

        let mut enqueue_io = InNetworkEnqueueIo {
            tx: &tx,
            event_rx: &event_rx,
            writer: &mut writer,
            copy_file_event_gate: &mut copy_file_event_gate,
            cancelled: None,
            producer_blocked_micros: &mut producer_blocked_micros,
            raw_chunk_stats: &mut stats,
        };
        let rate_count = enqueue_in_network_struson(
            &mut reader,
            &mut enqueue_io,
            InNetworkEnqueueOptions {
                chunk_size: 100,
                raw_chunk_byte_limit: 1,
                parse_in_workers: true,
                object_ordinal: 0,
            },
        )
        .unwrap();

        drop(tx);
        let jobs: Vec<_> = rx.try_iter().collect();
        stats.merge_from(&RawChunkStats::default());
        assert_eq!(rate_count, 3);
        assert_eq!(stats.chunk_count, 3);
        assert_eq!(stats.max_rates, 1);
        assert!(stats.max_bytes > 0);
        assert!(jobs.iter().all(|job| matches!(
            job,
            WorkerJob::RawRates { raw_rates, .. } if raw_rates.len() == 1
        )));
    }

    #[test]
    fn shared_graph_cli_manifest_parser_preserves_complete_artifact_metadata() {
        let artifact = |name: &str| {
            json!({
                "path": format!("/tmp/{name}.sidecar"),
                "metadata": {
                    "record_format": "format-v1",
                    "sha256": "a".repeat(64),
                    "byte_count": 123,
                    "owner_count": 4,
                    "member_count": 7,
                    "member_global_count": 9,
                    "name": name,
                    "source_shard_id": "source-1",
                    "shard_id": "source-1",
                },
            })
        };
        let shard = shared_graph_shard_from_json(
            &json!({
                "shard_id": "source-1",
                "group_npi": artifact("provider_group_npi"),
                "npi_group": artifact("provider_npi_group"),
                "group_provider_set": artifact("provider_inverted"),
                "provider_set_group": artifact("provider_forward"),
            }),
            0,
        )
        .unwrap();

        assert_eq!(shard.shard_id, "source-1");
        assert_eq!(shard.group_npi.metadata.member_global_count, Some(9));
        assert_eq!(
            shard.group_provider_set.metadata.name.as_deref(),
            Some("provider_inverted")
        );
        assert_eq!(
            shard.provider_set_group.metadata.source_shard_id.as_deref(),
            Some("source-1")
        );
    }

    #[test]
    fn v4_manifest_factor_collectors_cover_spooled_and_in_memory_directions() {
        let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
        let atom = GlobalId128([9; GLOBAL_ID_BYTES]);
        let price_set = PriceSetLite {
            global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
            atoms: Vec::new(),
            atom_ids: vec![atom],
        };

        let mut in_memory = ManifestSidecarCollector::default();
        in_memory
            .record_provider_set(provider_set, &[2, 1, 1], &[4, 3, 3], &[1_234_567_890, 0])
            .unwrap();
        in_memory.record_provider_component(3, &[2, 1, 1]).unwrap();
        in_memory.record_provider_component(4, &[2]).unwrap();
        in_memory.record_price_set(&price_set).unwrap();
        assert_eq!(in_memory.provider_forward_entries().unwrap().len(), 1);
        assert_eq!(in_memory.provider_inverted_entries().unwrap().len(), 2);
        assert_eq!(in_memory.provider_set_component_entries().unwrap().len(), 1);
        assert_eq!(
            in_memory.provider_component_group_entries().unwrap().len(),
            2
        );
        assert_eq!(in_memory.provider_npi_entries().unwrap().len(), 1);
        assert_eq!(in_memory.price_forward_entries().unwrap().len(), 1);
        assert!(in_memory
            .write_spooled_standard_sidecar("unknown", "unused", false)
            .unwrap()
            .is_none());

        let factor_paths = CopyPathConfig {
            manifest_provider_set_component_sidecar: Some("set-component".to_owned()),
            manifest_provider_component_group_sidecar: Some("component-group".to_owned()),
            ..CopyPathConfig::default()
        };
        let mut spooled = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::for_paths(&factor_paths).unwrap()),
            ..ManifestSidecarCollector::default()
        };
        spooled
            .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
            .unwrap();
        spooled.record_provider_component(3, &[2, 1]).unwrap();
        assert!(spooled.provider_set_component_entries().unwrap().len() == 1);
        assert!(spooled.provider_component_group_entries().unwrap().len() == 1);
    }

    #[test]
    fn direct_spooled_sidecars_and_dictionary_outputs_finalize_exactly() {
        let temporary = tempfile::tempdir().unwrap();
        let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
        let price_set = PriceSetLite {
            global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
            atoms: Vec::new(),
            atom_ids: vec![GlobalId128([9; GLOBAL_ID_BYTES])],
        };
        let mut collector = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::all().unwrap()),
            ..ManifestSidecarCollector::default()
        };
        collector
            .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
            .unwrap();
        collector.record_provider_component(3, &[2, 1]).unwrap();
        collector.record_price_set(&price_set).unwrap();

        for (name, dense) in [
            ("provider_forward", false),
            ("provider_inverted", false),
            ("provider_set_component", false),
            ("provider_component_group", false),
            ("provider_npi", true),
            ("price_forward", false),
        ] {
            let path = temporary.path().join(format!("{name}.ptg2sc"));
            assert!(collector
                .write_spooled_standard_sidecar(name, path.to_str().unwrap(), dense)
                .unwrap()
                .is_some());
        }
        assert!(collector
            .write_spooled_standard_sidecar(
                "unknown",
                temporary.path().join("unknown").to_str().unwrap(),
                false,
            )
            .unwrap()
            .is_none());

        let path = |name: &str| temporary.path().join(name).display().to_string();
        let paths = CopyPathConfig {
            manifest_price_atom: Some(path("manifest-price-atom.copy")),
            manifest_price_set_atom: Some(path("manifest-price-set-atom.copy")),
            manifest_price_set_summary: Some(path("manifest-price-set-summary.copy")),
            manifest_provider_group_member: Some(path("manifest-provider-member.copy")),
            manifest_code_count: Some(path("manifest-code-count.copy")),
            manifest_provider_set_dictionary: Some(path("manifest-provider-set.copy")),
            procedure: Some(path("procedure.copy")),
            price_code_set: Some(path("price-code-set.copy")),
            price_atom: Some(path("price-atom.copy")),
            price_set_entry: Some(path("price-set-entry.copy")),
            provider_set: Some(path("provider-set.copy")),
            provider_set_component: Some(path("provider-set-component.copy")),
            provider_set_entry: Some(path("provider-set-entry.copy")),
            provider_entry_component: Some(path("provider-entry-component.copy")),
            provider_group_member: Some(path("provider-group-member.copy")),
            ..CopyPathConfig::default()
        };
        let sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        let stdout = io::stdout();
        let mut events = BufWriter::new(stdout.lock());
        sinks.finish(&mut events).unwrap();

        let mut closed_sinks = DictionaryCopySinks::from_paths(&paths, 0).unwrap();
        closed_sinks.price_code_set.as_mut().unwrap().writer = None;
        closed_sinks.price_set_entry.as_mut().unwrap().writer = None;
        closed_sinks.provider_set.as_mut().unwrap().writer = None;
        closed_sinks.manifest_code_count.as_mut().unwrap().writer = None;
        closed_sinks.provider_set_entry.as_mut().unwrap().writer = None;

        assert_eq!(
            closed_sinks
                .write_price_code_set("price-code-set", &["12345".to_owned()])
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
        let mut emitted_price_set_entries = HashSet::new();
        assert_eq!(
            closed_sinks
                .write_price_set_entries(
                    GlobalId128([2; GLOBAL_ID_BYTES]),
                    &[GlobalId128([3; GLOBAL_ID_BYTES])],
                    &mut emitted_price_set_entries,
                )
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
        assert_eq!(
            closed_sinks
                .write_provider_set("provider-set", 1, &[1])
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
        assert_eq!(
            closed_sinks
                .write_manifest_code_count("plan", Some("CPT"), Some("12345"), 1)
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
        let mut emitted_provider_set_entries = HashSet::new();
        assert_eq!(
            closed_sinks
                .write_provider_set_entries(
                    "provider-set",
                    &[1],
                    &mut emitted_provider_set_entries,
                )
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe,
        );
    }

    #[test]
    fn v4_provider_membership_sidecars_preserve_exact_edges_and_reject_bad_rows() {
        let temporary = tempfile::tempdir().unwrap();
        let group_a = GlobalId128([5; GLOBAL_ID_BYTES]).to_hex();
        let group_b = GlobalId128([6; GLOBAL_ID_BYTES]).to_hex();
        let input_a = temporary.path().join("membership-a.copy");
        let input_b = temporary.path().join("membership-b.copy");
        std::fs::write(
            &input_a,
            format!(
                "{group_a}\t1234567890\r\n\
                 {group_b}\t0\n\
                 {group_a}\t1234567890\textra\n"
            ),
        )
        .unwrap();
        std::fs::write(&input_b, format!("{group_b}\t2222222222\n")).unwrap();
        let group_npi = temporary.path().join("group-npi.sidecar");
        let npi_group = temporary.path().join("npi-group.sidecar");
        let npi_scope = temporary.path().join("npi-scope.copy");
        write_provider_membership_sidecars(
            &group_npi,
            &npi_group,
            &npi_scope,
            &[
                temporary.path().join("missing.copy").display().to_string(),
                input_a.display().to_string(),
                input_b.display().to_string(),
            ],
        )
        .unwrap();

        assert_eq!(&std::fs::read(&group_npi).unwrap()[..8], b"PTG2MNDS");
        assert_eq!(&std::fs::read(&npi_group).unwrap()[..8], b"PTG2MNDS");
        let scope = std::fs::read(&npi_scope).unwrap();
        let copy_header = [b"PGCOPY\n\xff\r\n\0".as_slice(), &[0u8; 8]].concat();
        assert_eq!(&scope[..copy_header.len()], copy_header);
        assert_eq!(scope.len(), copy_header.len() + 2 * 14 + 2);
        assert_eq!(
            i64::from_be_bytes(scope[25..33].try_into().unwrap()),
            1_234_567_890,
        );
        assert_eq!(
            i64::from_be_bytes(scope[39..47].try_into().unwrap()),
            2_222_222_222,
        );
        assert_eq!(&scope[47..], &(-1i16).to_be_bytes());

        let tampered_npi_group = temporary.path().join("tampered-npi-group.sidecar");
        let tampered_scope = temporary.path().join("tampered-scope.copy");
        let mut tampered = std::fs::read(&npi_group).unwrap();
        tampered[20..28].copy_from_slice(&99u64.to_le_bytes());
        std::fs::write(&tampered_npi_group, tampered).unwrap();
        assert!(write_provider_npi_scope_from_dense_sidecar(
            &tampered_npi_group,
            &tampered_scope,
            2,
            2,
            2,
        )
        .is_err());
        assert!(!tampered_scope.exists());

        let existing_scope = temporary.path().join("existing-scope.copy");
        std::fs::write(&existing_scope, b"caller-owned").unwrap();
        assert!(
            write_provider_npi_scope_from_dense_sidecar(&npi_group, &existing_scope, 2, 2, 2,)
                .is_err()
        );
        assert_eq!(std::fs::read(&existing_scope).unwrap(), b"caller-owned");

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let target = temporary.path().join("scope-link-target.copy");
            let link = temporary.path().join("scope-link.copy");
            symlink(&target, &link).unwrap();
            assert!(
                write_provider_npi_scope_from_dense_sidecar(&npi_group, &link, 2, 2, 2,).is_err()
            );
            assert!(std::fs::symlink_metadata(&link)
                .unwrap()
                .file_type()
                .is_symlink());
            assert!(!target.exists());
        }

        assert!(global_id_from_hex_bytes(b"short").is_err());
        assert!(global_id_from_hex_bytes(&[b'g'; GLOBAL_ID_BYTES * 2]).is_err());
        let mut invalid_low = [b'0'; GLOBAL_ID_BYTES * 2];
        invalid_low[1] = b'g';
        assert!(global_id_from_hex_bytes(&invalid_low).is_err());

        for (name, row) in [
            ("missing-group", b"\t1234567890\n".as_slice()),
            (
                "invalid-group",
                b"0000000000000000000000000000000g\t1234567890\n".as_slice(),
            ),
            (
                "invalid-npi",
                b"05050505050505050505050505050505\tnot-an-npi\n".as_slice(),
            ),
            (
                "short-npi",
                b"05050505050505050505050505050505\t123456789\n".as_slice(),
            ),
            (
                "plus-npi",
                b"05050505050505050505050505050505\t+1234567890\n".as_slice(),
            ),
            (
                "leading-zero-eleven",
                b"05050505050505050505050505050505\t01234567890\n".as_slice(),
            ),
            (
                "plus-zero",
                b"05050505050505050505050505050505\t+0\n".as_slice(),
            ),
            (
                "minus-zero",
                b"05050505050505050505050505050505\t-0\n".as_slice(),
            ),
            (
                "multiple-zero",
                b"05050505050505050505050505050505\t00\n".as_slice(),
            ),
            (
                "ten-zero",
                b"05050505050505050505050505050505\t0000000000\n".as_slice(),
            ),
            (
                "leading-zero-ten",
                b"05050505050505050505050505050505\t0123456789\n".as_slice(),
            ),
            (
                "invalid-utf8",
                b"05050505050505050505050505050505\t\xff\n".as_slice(),
            ),
        ] {
            let input = temporary.path().join(format!("{name}.copy"));
            std::fs::write(&input, row).unwrap();
            assert!(write_provider_membership_sidecars(
                &temporary.path().join(format!("{name}-group.sidecar")),
                &temporary.path().join(format!("{name}-npi.sidecar")),
                &temporary.path().join(format!("{name}-scope.copy")),
                &[input.display().to_string()],
            )
            .is_err());
        }
    }

    #[test]
    fn provider_membership_npi_parser_accepts_only_exact_wire_values() {
        assert_eq!(parse_provider_membership_npi(b"0").unwrap(), None);
        assert_eq!(
            parse_provider_membership_npi(b"1234567890").unwrap(),
            Some(1_234_567_890),
        );
        assert_eq!(
            parse_provider_membership_npi(b"9999999999").unwrap(),
            Some(9_999_999_999),
        );
        for value in [
            b"".as_slice(),
            b"+0",
            b"-0",
            b"00",
            b"0000000000",
            b"0123456789",
            b"+1234567890",
            b"01234567890",
            b"123456789",
            b"123456789a",
            b"\xff234567890",
        ] {
            assert!(parse_provider_membership_npi(value).is_err(), "{value:?}",);
        }
    }

    #[test]
    fn configured_manifest_sidecar_emission_covers_memory_and_spooled_outputs() {
        let base = std::env::temp_dir().join(format!(
            "ptg2-sidecar-emission-test-{}-{:?}",
            std::process::id(),
            thread::current().id()
        ));
        std::fs::create_dir_all(&base).unwrap();
        let paths_for = |directory: &Path| CopyPathConfig {
            manifest_provider_forward_sidecar: Some(
                directory
                    .join("provider-forward.ptg2sc")
                    .display()
                    .to_string(),
            ),
            manifest_provider_inverted_sidecar: Some(
                directory
                    .join("provider-inverted.ptg2sc")
                    .display()
                    .to_string(),
            ),
            manifest_provider_set_component_sidecar: Some(
                directory.join("set-component.ptg2sc").display().to_string(),
            ),
            manifest_provider_component_group_sidecar: Some(
                directory
                    .join("component-group.ptg2sc")
                    .display()
                    .to_string(),
            ),
            manifest_provider_npi_sidecar: Some(
                directory.join("provider-npi.ptg2sc").display().to_string(),
            ),
            manifest_price_forward_sidecar: Some(
                directory.join("price-forward.ptg2sc").display().to_string(),
            ),
            ..CopyPathConfig::default()
        };
        let provider_set = GlobalId128([1; GLOBAL_ID_BYTES]);
        let price_set = PriceSetLite {
            global_id: GlobalId128([8; GLOBAL_ID_BYTES]),
            atoms: Vec::new(),
            atom_ids: vec![GlobalId128([9; GLOBAL_ID_BYTES])],
        };

        let memory_directory = base.join("memory");
        let memory_paths = paths_for(&memory_directory);
        let mut in_memory = ManifestSidecarCollector::default();
        in_memory
            .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
            .unwrap();
        in_memory.record_provider_component(3, &[2, 1]).unwrap();
        in_memory.record_price_set(&price_set).unwrap();
        let mut memory_events = Vec::new();
        emit_configured_manifest_sidecars(
            &mut memory_events,
            &memory_paths,
            Some(&mut in_memory),
            None,
        )
        .unwrap();
        assert_eq!(
            String::from_utf8(memory_events).unwrap().lines().count(),
            12
        );

        let spool_directory = base.join("spooled");
        let spool_paths = paths_for(&spool_directory);
        let mut spooled = ManifestSidecarCollector {
            spools: Some(ManifestSidecarSpools::for_paths(&spool_paths).unwrap()),
            ..ManifestSidecarCollector::default()
        };
        spooled
            .record_provider_set(provider_set, &[2, 1], &[4, 3], &[1_234_567_890])
            .unwrap();
        spooled.record_provider_component(3, &[2, 1]).unwrap();
        spooled.record_price_set(&price_set).unwrap();
        let mut spool_events = Vec::new();
        emit_configured_manifest_sidecars(
            &mut spool_events,
            &spool_paths,
            Some(&mut spooled),
            None,
        )
        .unwrap();
        assert_eq!(String::from_utf8(spool_events).unwrap().lines().count(), 12);

        let mut no_events = Vec::new();
        emit_configured_manifest_sidecars(&mut no_events, &CopyPathConfig::default(), None, None)
            .unwrap();
        assert!(no_events.is_empty());
        std::fs::remove_dir_all(base).unwrap();
    }

    #[test]
    fn v4_identity_and_projection_helpers_preserve_exact_boundaries() {
        let first_definition = json!({
            "provider_group_id": 7,
            "network_name": [" network-b ", "network-a", "network-a"],
            "provider_groups": [{
                "tin": {"type": "ein", "value": "12-3456789"},
                "npi": [1234567890, 1234567891]
            }]
        });
        let second_definition = json!({
            "provider_group_id": 8,
            "network_name": ["network-c"],
            "provider_groups": [{
                "tin": {"type": "ein", "value": "98-7654321"},
                "npi": [1234567891, 1234567892]
            }]
        });
        let (first_key, first_entry) = provider_ref_definition(&first_definition).unwrap();
        let (second_key, second_entry) = provider_ref_definition(&second_definition).unwrap();
        assert_eq!(ProviderRefKey::from("7"), first_key);
        assert_eq!(ProviderRefKey::from("8".to_string()), second_key);

        let rebuilt = build_provider_entry(&first_definition).unwrap();
        assert_eq!(rebuilt, first_entry);
        let mut provider_map = HashMap::new();
        insert_provider_definition(&mut provider_map, first_key.clone(), first_entry.clone())
            .unwrap();
        insert_provider_definition(&mut provider_map, second_key.clone(), second_entry.clone())
            .unwrap();
        insert_provider_definition(&mut provider_map, first_key.clone(), first_entry.clone())
            .unwrap();
        validate_preloaded_provider_definition(&provider_map, &first_key, &first_entry).unwrap();
        assert!(
            validate_preloaded_provider_definition(&provider_map, &first_key, &second_entry)
                .is_err()
        );
        assert!(validate_preloaded_provider_definition(
            &provider_map,
            &ProviderRefKey::from("missing"),
            &first_entry,
        )
        .is_err());

        let borrowed =
            provider_entry_view_from_ref_keys(&provider_map, std::slice::from_ref(&first_key))
                .unwrap()
                .unwrap();
        assert_eq!(borrowed.entry_hash(), first_entry.entry_hash);
        assert_eq!(borrowed.provider_count(), 2);
        assert_eq!(
            borrowed.provider_group_hashes(),
            first_entry.provider_group_hashes
        );
        assert_eq!(borrowed.npi(), first_entry.npi);
        assert_eq!(borrowed.network_names(), first_entry.network_names);

        let owned = provider_entry_view_from_ref_keys(
            &provider_map,
            &[first_key.clone(), second_key.clone()],
        )
        .unwrap()
        .unwrap();
        assert_eq!(owned.provider_count(), 3);
        assert_eq!(owned.npi(), &[1234567890, 1234567891, 1234567892]);
        assert_eq!(
            provider_set_from_ref_keys(&provider_map, &[]).unwrap(),
            None
        );
        assert!(
            provider_set_from_ref_keys(&provider_map, &[ProviderRefKey::from("unknown")]).is_err()
        );

        let combined = combine_provider_entries(first_entry.clone(), second_entry.clone());
        assert_eq!(combined.provider_count, 3);
        assert_eq!(combined.npi, vec![1234567890, 1234567891, 1234567892]);
        let first_group_payload = provider_group_payload_canonical_json(
            first_entry.provider_group_hashes[0],
            "ein",
            "123456789",
            &first_entry.npi,
            &[9, 8, 9],
            &[],
        );
        let second_group_payload = provider_group_payload_canonical_json(
            second_entry.provider_group_hashes[0],
            "ein",
            "987654321",
            &second_entry.npi,
            &[],
            &[],
        );
        assert_eq!(
            provider_set_checksum_from_group_payloads(vec![
                first_group_payload.clone(),
                second_group_payload.clone(),
            ]),
            provider_set_checksum_from_group_payloads(vec![
                second_group_payload,
                first_group_payload,
            ])
        );
        assert_eq!(
            provider_set_scope_hash(&[1, 2], &["a".to_string(), "b".to_string()]),
            provider_set_scope_hash(&[1, 2], &["a".to_string(), "b".to_string()])
        );
        assert_ne!(npi_member_id(1234567890), npi_member_id(1234567891));

        let prices = vec![
            PriceLite {
                negotiated_type: Some("negotiated".to_string()),
                negotiated_rate: "12.50".to_string(),
                expiration_date: Some("2027-01-01".to_string()),
                service_code: vec!["11".to_string()],
                billing_class: Some("professional".to_string()),
                setting: Some("outpatient".to_string()),
                billing_code_modifier: vec!["26".to_string()],
                additional_information: Some("contract".to_string()),
            },
            PriceLite {
                negotiated_type: Some("fee schedule".to_string()),
                negotiated_rate: "9.75".to_string(),
                expiration_date: None,
                service_code: vec!["12".to_string()],
                billing_class: None,
                setting: None,
                billing_code_modifier: Vec::new(),
                additional_information: None,
            },
        ];
        let first_atom = price_atom_from_lite(&prices[0], 0);
        assert_eq!(first_atom.global_id, price_atom_global_id(&first_atom));
        assert_eq!(
            price_code_set_hash(&first_atom.service_code),
            price_code_set_hash(&["11".to_string()])
        );
        let price_set = price_lite_set(&prices).unwrap();
        assert_eq!(price_set.minimum_negotiated_rate(), "9.75");
        assert_eq!(price_set.global_id, price_set_global_id(&price_set));
        let rate = RateLite {
            provider_refs: vec![first_key],
            provider_groups: Vec::new(),
            provider_groups_raw: None,
            network_names: Vec::new(),
            prices: Vec::new(),
            prepared_price_set: Some(price_set.clone()),
        };
        assert_eq!(rate, rate.clone());
        assert_eq!(rate.price_count(), 2);
        assert!(!rate.has_inline_provider_groups());
        assert_eq!(rate_price_set(&rate).unwrap().into_owned(), price_set);
        let grouped: GroupedPriceSet = (
            price_set,
            HashSet::from([first_entry.entry_hash]),
            HashSet::from([first_entry.provider_group_hashes[0]]),
            HashSet::from([1234567890]),
            1,
            HashSet::from(["network-a".to_string()]),
            BTreeMap::from([(
                first_entry.entry_hash,
                first_entry.provider_group_hashes.clone(),
            )]),
        )
            .into();
        assert_eq!(grouped.provider_count, 1);

        let default_cache = ProviderSetScopeCache::default();
        assert!(!default_cache.v4_factor_mode());
        assert!(ProviderSetScopeCache::with_v4_factor_mode(true).v4_factor_mode());
    }

    #[test]
    fn manifest_merge_ordering_and_empty_merge_cleanup_are_exact() {
        let pair_one = [1u8; MANIFEST_PAIR_RECORD_BYTES];
        let pair_two = [2u8; MANIFEST_PAIR_RECORD_BYTES];
        let first_pair = ManifestPairMergeItem {
            pair: pair_one,
            reader_index: 0,
        };
        let same_pair = ManifestPairMergeItem {
            pair: pair_one,
            reader_index: 0,
        };
        let later_pair = ManifestPairMergeItem {
            pair: pair_two,
            reader_index: 1,
        };
        assert!(first_pair == same_pair);
        assert!(first_pair != later_pair);
        assert_eq!(
            first_pair.partial_cmp(&later_pair),
            Some(first_pair.cmp(&later_pair))
        );

        let first_line = ManifestMergeItem {
            key: b"a".to_vec(),
            line: b"first\n".to_vec(),
            reader_index: 0,
        };
        let same_line = ManifestMergeItem {
            key: b"a".to_vec(),
            line: b"first\n".to_vec(),
            reader_index: 0,
        };
        let later_line = ManifestMergeItem {
            key: b"b".to_vec(),
            line: b"second\n".to_vec(),
            reader_index: 1,
        };
        assert!(first_line == same_line);
        assert!(first_line != later_line);
        assert_eq!(
            first_line.partial_cmp(&later_line),
            Some(first_line.cmp(&later_line))
        );

        let temporary = tempfile::tempdir().unwrap();
        let source = temporary.path().join("pairs.bin");
        let mut tracked_files = ManifestPairTemporaryFiles::default();
        let mut progress = ManifestFinalizeProgress::new(None);
        let merged =
            merge_sorted_pair_chunks(&source, &[], &mut tracked_files, &mut progress).unwrap();
        assert_eq!(merged.entry_count, 0);
        assert_eq!(merged.member_count, 0);
        assert!(merged.member_ids.is_empty());
        assert!(merged.path.is_file());
        let merged_path = merged.path.clone();
        drop(tracked_files);
        assert!(!merged_path.exists());

        assert_eq!(provider_code_merge_pass_count(0), 0);
        assert_eq!(provider_code_merge_pass_count(1), 1);
        assert!(
            provider_code_merge_pass_count(SERVING_BINARY_V3_PROVIDER_CODE_MERGE_FAN_IN as u64) > 1
        );
    }

    #[test]
    fn scanner_failure_and_binary_value_helpers_fail_closed() {
        let diagnostic =
            primary_producer_failure_diagnostic(&io::Error::other("upstream contract failed"))
                .unwrap();
        assert!(diagnostic.contains("producer_error"));
        for kind in [io::ErrorKind::Interrupted, io::ErrorKind::BrokenPipe] {
            assert!(primary_producer_failure_diagnostic(&io::Error::new(kind, "peer")).is_none());
        }
        assert_eq!(panic_payload_message(&"borrowed panic"), "borrowed panic");
        assert_eq!(
            panic_payload_message(&"owned panic".to_string()),
            "owned panic"
        );
        assert_eq!(panic_payload_message(&17usize), "non-string panic payload");
        log_worker_failure(7, "contract", "expected test diagnostic");

        assert_eq!(decimal_text_field(b"-12.50"), Some("-12.50"));
        assert_eq!(decimal_text_field(b"+7"), Some("+7"));
        for invalid in [b"".as_slice(), b"+", b"1.2.3", b"NaN", b"\xff"] {
            assert_eq!(decimal_text_field(invalid), None);
        }
        assert_eq!(pg_binary_u64(&7i32.to_be_bytes(), "value").unwrap(), 7);
        assert_eq!(pg_binary_u64(&9i64.to_be_bytes(), "value").unwrap(), 9);
        assert!(pg_binary_u64(&(-1i32).to_be_bytes(), "value").is_err());
        assert!(pg_binary_u64(&(-1i64).to_be_bytes(), "value").is_err());
        assert!(pg_binary_u64(&[0; 3], "value").is_err());
        assert_eq!(
            pg_binary_nonnegative_i64(&7i32.to_be_bytes(), "value").unwrap(),
            7
        );
        assert!(pg_binary_nonnegative_i64(&u64::MAX.to_be_bytes(), "value").is_err());
        assert!(
            pg_binary_nonnegative_i32(&(i64::from(i32::MAX) + 1).to_be_bytes(), "value",).is_err()
        );
        assert_eq!(
            pg_binary_optional_nonnegative_i64(None, "value").unwrap(),
            None
        );
        assert_eq!(
            pg_binary_optional_nonnegative_i64(Some(&7i32.to_be_bytes()), "value").unwrap(),
            Some(7)
        );
        assert_eq!(pg_binary_negotiated_rate(b"12.50").unwrap(), "12.50");
        assert!(pg_binary_numeric_text(&[0; 7]).is_err());
    }

    #[test]
    fn pg_binary_copy_header_accepts_extensions_and_rejects_invalid_boundaries() {
        let mut with_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
        with_extension.extend_from_slice(&0i32.to_be_bytes());
        with_extension.extend_from_slice(&2i32.to_be_bytes());
        with_extension.extend_from_slice(b"ok");
        read_pg_binary_copy_header(&mut Cursor::new(with_extension)).unwrap();

        let invalid_signature = read_pg_binary_copy_header(&mut Cursor::new(b"not-pg-copy\n"));
        assert!(invalid_signature
            .unwrap_err()
            .to_string()
            .contains("invalid header"));

        let mut negative_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
        negative_extension.extend_from_slice(&0i32.to_be_bytes());
        negative_extension.extend_from_slice(&(-1i32).to_be_bytes());
        assert!(
            read_pg_binary_copy_header(&mut Cursor::new(negative_extension))
                .unwrap_err()
                .to_string()
                .contains("negative extension length")
        );

        let mut truncated_extension = b"PGCOPY\n\xff\r\n\0".to_vec();
        truncated_extension.extend_from_slice(&0i32.to_be_bytes());
        truncated_extension.extend_from_slice(&2i32.to_be_bytes());
        truncated_extension.push(b'o');
        assert_eq!(
            read_pg_binary_copy_header(&mut Cursor::new(truncated_extension))
                .unwrap_err()
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }

    #[test]
    fn pg_binary_copy_rows_reject_truncated_and_invalid_boundaries() {
        let partial_field = read_exact_optional(&mut Cursor::new(vec![b'x']), &mut [0_u8; 2]);
        assert_eq!(
            partial_field.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );

        let missing_trailer =
            read_pg_binary_copy_row(&mut Cursor::new(Vec::<u8>::new()), 1, "test");
        assert_eq!(
            missing_trailer.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );

        let wrong_field_count =
            read_pg_binary_copy_row(&mut Cursor::new(2i16.to_be_bytes().to_vec()), 1, "test");
        assert!(wrong_field_count
            .unwrap_err()
            .to_string()
            .contains("must have 1 fields, got 2"));

        let mut invalid_length = 1i16.to_be_bytes().to_vec();
        invalid_length.extend_from_slice(&(-2i32).to_be_bytes());
        assert!(
            read_pg_binary_copy_row(&mut Cursor::new(invalid_length), 1, "test")
                .unwrap_err()
                .to_string()
                .contains("invalid length")
        );

        assert_eq!(
            read_pg_binary_copy_row(&mut Cursor::new((-1i16).to_be_bytes().to_vec()), 1, "test")
                .unwrap(),
            None
        );
    }

    #[test]
    fn worker_copy_reader_orders_shards_and_refuses_absent_output() {
        let temporary = tempfile::tempdir().unwrap();
        let copy_path = temporary.path().join("provider-members.copy");
        std::fs::write(
            temporary.path().join("provider-members.copy.0001"),
            b"second\n",
        )
        .unwrap();
        std::fs::write(
            temporary.path().join("provider-members.copy.0000"),
            b"first\n",
        )
        .unwrap();

        assert_eq!(
            read_worker_copy_text(&copy_path).unwrap(),
            "first\nsecond\n"
        );
        let absent = temporary.path().join("absent.copy");
        let error = read_worker_copy_text(&absent).unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn benchmark_fixture_helpers_build_small_partition_complete_manifest() {
        let temporary = tempfile::tempdir().unwrap();
        let coverage_scope_id = [0x75; COVERAGE_SCOPE_ID_BYTES];
        let codes = v3_finalizer_benchmark_codes_by_partition(&coverage_scope_id, 2);
        assert_eq!(codes.len(), 2);
        assert_ne!(codes[0], codes[1]);

        let manifest_path =
            write_v3_finalizer_benchmark_manifest(temporary.path(), "small", 8, 2, 3, 2);
        let manifest: Value =
            serde_json::from_slice(&std::fs::read(manifest_path).unwrap()).unwrap();
        assert_eq!(manifest["source_count"], 1);
        assert_eq!(manifest["expected_serving_run_rows"], 8);
        assert_eq!(
            manifest["serving_run_partition_files"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
    }

    #[test]
    fn witness_failure_code_distinguishes_payload_limits() {
        for message in [
            "source witness payload budget exceeded",
            "source witness intermediate budget exceeded",
            "source witness fail-closed limit",
            "source witness spool byte limit exceeded",
        ] {
            assert_eq!(
                scanner_failure_code(&io::Error::new(io::ErrorKind::InvalidData, message)),
                "witness_payload_limit",
            );
        }
        assert_eq!(
            scanner_failure_code(&io::Error::other("source witness parse failed")),
            "scanner_failure",
        );
        assert_eq!(
            scanner_failure_code(&io::Error::other("unrelated")),
            "scanner_failure",
        );
    }

    #[test]
    fn shared_graph_manifest_admission_rejects_every_incomplete_shape() {
        assert!(shared_graph_required_object(&Value::Null, "value").is_err());

        let mut object = Map::new();
        assert!(shared_graph_required_string(&object, "text", "value").is_err());
        object.insert("text".to_owned(), json!(1));
        assert!(shared_graph_required_string(&object, "text", "value").is_err());
        object.insert("text".to_owned(), json!(""));
        assert!(shared_graph_required_string(&object, "text", "value").is_err());
        object.insert("text".to_owned(), json!("ok"));
        assert_eq!(
            shared_graph_required_string(&object, "text", "value").unwrap(),
            "ok"
        );

        assert!(shared_graph_required_u64(&object, "count", "value").is_err());
        object.insert("count".to_owned(), json!(-1));
        assert!(shared_graph_required_u64(&object, "count", "value").is_err());
        object.insert("count".to_owned(), json!(4));
        assert_eq!(
            shared_graph_required_u64(&object, "count", "value").unwrap(),
            4
        );

        assert_eq!(
            shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
            None
        );
        object.insert("optional_count".to_owned(), Value::Null);
        assert_eq!(
            shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
            None
        );
        object.insert("optional_count".to_owned(), json!("four"));
        assert!(shared_graph_optional_u64(&object, "optional_count", "value").is_err());
        object.insert("optional_count".to_owned(), json!(4));
        assert_eq!(
            shared_graph_optional_u64(&object, "optional_count", "value").unwrap(),
            Some(4)
        );

        assert_eq!(
            shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
            None
        );
        object.insert("optional_text".to_owned(), Value::Null);
        assert_eq!(
            shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
            None
        );
        object.insert("optional_text".to_owned(), json!(4));
        assert!(shared_graph_optional_string(&object, "optional_text", "value").is_err());
        object.insert("optional_text".to_owned(), json!("text"));
        assert_eq!(
            shared_graph_optional_string(&object, "optional_text", "value").unwrap(),
            Some("text".to_owned())
        );

        assert!(shared_graph_artifact_from_json(&Value::Null, "artifact").is_err());
        assert!(shared_graph_artifact_from_json(&json!({}), "artifact").is_err());
        assert!(shared_graph_artifact_from_json(&json!({"metadata": null}), "artifact").is_err());
        let artifact = json!({
            "path": "artifact.bin",
            "metadata": {
                "record_format": "test",
                "sha256": "0",
                "byte_count": 1,
                "owner_count": 1,
                "member_count": 1,
                "member_global_count": null,
                "name": "test",
                "source_shard_id": null,
                "shard_id": "shard"
            }
        });
        assert!(shared_graph_artifact_from_json(&artifact, "artifact").is_ok());
        assert!(shared_graph_shard_from_json(&Value::Null, 0).is_err());
        assert!(shared_graph_shard_from_json(&json!({}), 0).is_err());
        let shard = json!({
            "shard_id": "shard",
            "group_npi": artifact,
            "npi_group": artifact,
            "group_provider_set": artifact,
            "provider_set_group": artifact
        });
        assert!(shared_graph_shard_from_json(&shard, 0).is_ok());

        let invalid_utf8 = PathBuf::from(
            <std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(vec![0xff]),
        );
        assert!(shared_graph_summary_path(&invalid_utf8, "test").is_err());

        let directory = tempfile::tempdir().unwrap();
        assert!(run_shared_graph_converter(&[]).is_err());
        assert!(run_shared_graph_converter(&[directory
            .path()
            .join("missing")
            .to_string_lossy()
            .into_owned()])
        .is_err());
        for (name, manifest) in [
            ("invalid", b"not-json".as_slice()),
            ("null", b"null".as_slice()),
            ("missing-key-map", br#"{}"#.as_slice()),
            (
                "missing-output",
                br#"{"provider_set_key_map_path":"map"}"#.as_slice(),
            ),
            (
                "missing-shards",
                br#"{"provider_set_key_map_path":"map","output_directory":"out"}"#.as_slice(),
            ),
            (
                "empty-shards",
                br#"{"provider_set_key_map_path":"map","output_directory":"out","shards":[]}"#
                    .as_slice(),
            ),
        ] {
            let path = directory.path().join(format!("{name}.json"));
            std::fs::write(&path, manifest).unwrap();
            assert!(
                run_shared_graph_converter(&[path.to_string_lossy().into_owned()]).is_err(),
                "accepted invalid shared graph manifest {name}",
            );
        }
    }

    #[test]
    fn copy_path_presence_and_v4_pairing_are_independently_admitted() {
        let setters: &[fn(&mut CopyPathConfig)] = &[
            |config| config.compact = Some("path".to_owned()),
            |config| config.manifest_serving = Some("path".to_owned()),
            |config| config.manifest_lean_serving = Some("path".to_owned()),
            |config| config.v3_serving_run_directory = Some("path".to_owned()),
            |config| config.manifest_provider_forward_sidecar = Some("path".to_owned()),
            |config| config.manifest_provider_inverted_sidecar = Some("path".to_owned()),
            |config| config.manifest_provider_set_component_sidecar = Some("path".to_owned()),
            |config| config.manifest_provider_component_group_sidecar = Some("path".to_owned()),
            |config| config.manifest_provider_group_tax_identity_sidecar = Some("path".to_owned()),
            |config| {
                config.manifest_provider_group_tax_identity_v2_sidecar = Some("path".to_owned())
            },
            |config| config.manifest_provider_npi_sidecar = Some("path".to_owned()),
            |config| config.manifest_price_forward_sidecar = Some("path".to_owned()),
            |config| config.manifest_price_atom = Some("path".to_owned()),
            |config| config.manifest_price_set_atom = Some("path".to_owned()),
            |config| config.manifest_price_set_summary = Some("path".to_owned()),
            |config| config.manifest_provider_group_member = Some("path".to_owned()),
            |config| config.manifest_code_count = Some("path".to_owned()),
            |config| config.manifest_provider_set_dictionary = Some("path".to_owned()),
            |config| config.procedure = Some("path".to_owned()),
            |config| config.price_code_set = Some("path".to_owned()),
            |config| config.price_atom = Some("path".to_owned()),
            |config| config.price_set_entry = Some("path".to_owned()),
            |config| config.provider_set = Some("path".to_owned()),
            |config| config.provider_set_component = Some("path".to_owned()),
            |config| config.provider_set_entry = Some("path".to_owned()),
            |config| config.provider_entry_component = Some("path".to_owned()),
            |config| config.provider_group_member = Some("path".to_owned()),
        ];
        assert!(!CopyPathConfig::default().has_file_paths());
        for set_path in setters {
            let mut config = CopyPathConfig::default();
            set_path(&mut config);
            assert!(config.has_file_paths());
        }

        let _lock = scanner_env_lock().lock().unwrap();
        let _disabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "false");
        let mismatched_pair = CopyPathConfig {
            manifest_provider_set_component_sidecar: Some("set".to_owned()),
            ..CopyPathConfig::default()
        };
        assert!(configured_v4_factor_mode(&mismatched_pair).is_err());

        let disabled_tax = CopyPathConfig {
            manifest_provider_group_tax_identity_sidecar: Some("tax".to_owned()),
            ..CopyPathConfig::default()
        };
        assert!(configured_v4_factor_mode(&disabled_tax).is_err());

        let _enabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "true");
        let missing_tax = CopyPathConfig {
            manifest_provider_set_component_sidecar: Some("set".to_owned()),
            manifest_provider_component_group_sidecar: Some("component".to_owned()),
            ..CopyPathConfig::default()
        };
        assert!(configured_v4_factor_mode(&missing_tax).is_err());
    }

    #[test]
    fn v2_tax_identity_paths_require_v4_v1_and_distinct_final_and_temporary_paths() {
        let _lock = scanner_env_lock().lock().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path_text = |name: &str| directory.path().join(name).display().to_string();
        let paths = |v1: Option<String>, v2: Option<String>| CopyPathConfig {
            manifest_provider_set_component_sidecar: Some(path_text("set")),
            manifest_provider_component_group_sidecar: Some(path_text("component")),
            manifest_provider_group_tax_identity_sidecar: v1,
            manifest_provider_group_tax_identity_v2_sidecar: v2,
            ..CopyPathConfig::default()
        };

        let _disabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "false");
        assert!(
            configured_v4_factor_mode(&paths(Some(path_text("v1")), Some(path_text("v2")),))
                .is_err()
        );

        let _enabled = TestEnvVar::set(PROVIDER_GRAPH_V4_ENV, "true");
        assert!(configured_v4_factor_mode(&paths(None, Some(path_text("v2")))).is_err());
        assert!(
            configured_v4_factor_mode(&paths(Some(path_text("v1")), Some(path_text("v2")),))
                .unwrap()
        );

        for (v1, v2) in [
            (path_text("same"), path_text("same")),
            (path_text("cross.building"), path_text("cross")),
            (path_text("reverse"), path_text("reverse.building")),
            (
                path_text("unused/../lexical-alias"),
                path_text("lexical-alias"),
            ),
            (
                "/tmp/ptg2-root-aware-alias".to_string(),
                "/../tmp/ptg2-root-aware-alias".to_string(),
            ),
        ] {
            let error = configured_v4_factor_mode(&paths(Some(v1), Some(v2)))
                .err()
                .unwrap();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains("must differ"));
        }

        let final_alias = paths(Some(path_text("v1")), Some(path_text("set")));
        assert!(configured_v4_factor_mode(&final_alias).is_err());
        let v1_only_final_alias = paths(Some(path_text("set")), None);
        assert!(configured_v4_factor_mode(&v1_only_final_alias).is_err());
        let mut temporary_alias =
            paths(Some(path_text("v1")), Some(path_text("v2-temporary-alias")));
        temporary_alias.manifest_provider_component_group_sidecar =
            Some(path_text("v2-temporary-alias.building"));
        assert!(configured_v4_factor_mode(&temporary_alias).is_err());
    }

    #[test]
    fn tax_identity_runtime_paths_reject_inputs_workers_rotations_and_reserved_directories() {
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.json");
        let secret_path = directory.path().join("token-secret.bin");
        let price_base = directory.path().join("price-atom.copy");
        let provider_group_base = directory.path().join("provider-group-member.copy");
        let factor_sidecar = directory.path().join("provider-set-component.sidecar");
        let v1_path = directory.path().join("tax-v1.ptg2tax");
        let serving_directory = directory.path().join("serving");
        let witness_scratch = directory.path().join("witness-scratch");
        std::fs::write(&source_path, b"source-sentinel").unwrap();
        std::fs::write(&secret_path, b"secret-sentinel").unwrap();
        std::fs::create_dir(&serving_directory).unwrap();
        std::fs::create_dir(&witness_scratch).unwrap();

        let base_paths = CopyPathConfig {
            v3_serving_run_directory: Some(serving_directory.display().to_string()),
            manifest_provider_group_tax_identity_sidecar: Some(v1_path.display().to_string()),
            manifest_provider_set_component_sidecar: Some(factor_sidecar.display().to_string()),
            manifest_price_atom: Some(price_base.display().to_string()),
            manifest_provider_group_member: Some(provider_group_base.display().to_string()),
            ..CopyPathConfig::default()
        };
        let collision_cases = [
            (source_path.clone(), "raw_input"),
            (secret_path.clone(), "token_secret"),
            (
                PathBuf::from(format!("{}.worker0000", price_base.display())),
                "manifest_price_atom.worker0000",
            ),
            (
                PathBuf::from(format!(
                    "{}.worker0000.part000000.ready",
                    price_base.display()
                )),
                "manifest_price_atom.worker0000",
            ),
            (
                PathBuf::from(format!(
                    "{}.provider_refs.worker0000",
                    provider_group_base.display()
                )),
                "manifest_provider_group_member.provider_refs.worker0000",
            ),
            (
                factor_sidecar.clone(),
                "manifest_provider_set_component_sidecar.final",
            ),
            (
                serving_directory.join("tax-v2.ptg2tax"),
                "serving_run_directory",
            ),
            (
                witness_scratch.join("tax-v2.ptg2tax"),
                "source_witness_scratch",
            ),
        ];
        for (candidate, expected_conflict) in collision_cases {
            let mut paths = base_paths.clone();
            paths.manifest_provider_group_tax_identity_v2_sidecar =
                Some(candidate.display().to_string());
            let error = validate_runtime_tax_identity_output_paths(
                &paths,
                &source_path,
                &secret_path,
                Some(&witness_scratch),
                2,
                2,
            )
            .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
            assert!(error.to_string().contains(expected_conflict));
            assert!(!error
                .to_string()
                .contains(directory.path().to_str().unwrap()));
            assert_eq!(std::fs::read(&source_path).unwrap(), b"source-sentinel");
            assert_eq!(std::fs::read(&secret_path).unwrap(), b"secret-sentinel");
        }

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let source_hardlink = directory.path().join("source-hardlink");
            std::fs::hard_link(&source_path, &source_hardlink).unwrap();
            let mut hardlink_paths = base_paths.clone();
            hardlink_paths.manifest_provider_group_tax_identity_v2_sidecar =
                Some(source_hardlink.display().to_string());
            assert!(validate_runtime_tax_identity_output_paths(
                &hardlink_paths,
                &source_path,
                &secret_path,
                Some(&witness_scratch),
                1,
                1,
            )
            .is_err());

            let actual_parent = directory.path().join("canonical-parent");
            let alias_parent = directory.path().join("canonical-parent-alias");
            std::fs::create_dir(&actual_parent).unwrap();
            symlink(&actual_parent, &alias_parent).unwrap();
            let canonical_source = actual_parent.join("source.json");
            std::fs::write(&canonical_source, b"canonical-source").unwrap();
            let mut symlink_parent_paths = base_paths.clone();
            symlink_parent_paths.manifest_provider_group_tax_identity_v2_sidecar =
                Some(alias_parent.join("source.json").display().to_string());
            assert!(validate_runtime_tax_identity_output_paths(
                &symlink_parent_paths,
                &canonical_source,
                &secret_path,
                Some(&witness_scratch),
                1,
                1,
            )
            .is_err());
            assert_eq!(
                std::fs::read(&canonical_source).unwrap(),
                b"canonical-source"
            );

            let manifest_target = actual_parent.join("manifest.sidecar");
            let mut manifest_alias_paths = base_paths.clone();
            manifest_alias_paths.manifest_provider_set_component_sidecar =
                Some(manifest_target.display().to_string());
            manifest_alias_paths.manifest_provider_group_tax_identity_v2_sidecar =
                Some(alias_parent.join("manifest.sidecar").display().to_string());
            assert!(validate_runtime_tax_identity_output_paths(
                &manifest_alias_paths,
                &source_path,
                &secret_path,
                Some(&witness_scratch),
                1,
                1,
            )
            .is_err());

            for (reserved_name, reserved_root) in [
                ("serving_run_directory", &serving_directory),
                ("source_witness_scratch", &witness_scratch),
            ] {
                let outside_target = directory
                    .path()
                    .join(format!("{reserved_name}-outside-target"));
                std::fs::write(&outside_target, b"outside-target").unwrap();
                let reserved_alias = directory.path().join(format!("{reserved_name}-alias"));
                symlink(reserved_root, &reserved_alias).unwrap();
                let reserved_entry = reserved_root.join("outward-tax-link");
                symlink(&outside_target, &reserved_entry).unwrap();
                let mut reserved_symlink_paths = base_paths.clone();
                reserved_symlink_paths.manifest_provider_group_tax_identity_v2_sidecar = Some(
                    reserved_alias
                        .join("outward-tax-link")
                        .display()
                        .to_string(),
                );
                let error = validate_runtime_tax_identity_output_paths(
                    &reserved_symlink_paths,
                    &source_path,
                    &secret_path,
                    Some(&witness_scratch),
                    1,
                    1,
                )
                .unwrap_err();
                assert!(error.to_string().contains(reserved_name));
                assert_eq!(std::fs::read(&outside_target).unwrap(), b"outside-target");
            }
        }

        let mut valid_paths = base_paths;
        valid_paths.manifest_provider_group_tax_identity_v2_sidecar = Some(
            directory
                .path()
                .join("tax-v2.ptg2tax")
                .display()
                .to_string(),
        );
        validate_runtime_tax_identity_output_paths(
            &valid_paths,
            &source_path,
            &secret_path,
            Some(&witness_scratch),
            2,
            2,
        )
        .unwrap();
    }

    #[test]
    fn strict_copy_path_environment_reports_each_missing_root_coordinate() {
        let _lock = scanner_env_lock().lock().unwrap();
        let directory = tempfile::tempdir().unwrap();
        let _strict = strict_scan_env(directory.path());
        {
            let _missing = TestEnvVar::remove("HLTHPRT_PTG2_SNAPSHOT_ARCH");
            assert!(CopyPathConfig::from_env().is_err());
        }
        {
            let _invalid = TestEnvVar::set("HLTHPRT_PTG2_SNAPSHOT_ARCH", "x86_64");
            assert!(CopyPathConfig::from_env().is_err());
        }
        {
            let _missing = TestEnvVar::remove("HLTHPRT_PTG2_V3_SERVING_RUN_DIR");
            assert!(CopyPathConfig::from_env().is_err());
        }
        {
            let _missing = TestEnvVar::remove(V3_COVERAGE_SCOPE_ID_ENV);
            assert!(CopyPathConfig::from_env().is_err());
        }
    }

    #[test]
    fn bounded_worker_queue_delivery_covers_success_pressure_and_shutdown() {
        let empty_job = || WorkerJob::Rates {
            procedure: Map::new(),
            rates: Vec::new(),
        };
        let empty_batch = || RawRateChunk::with_capacity(0, 0);
        let (_event_tx, event_rx) = unbounded();
        let mut writer = Vec::new();
        let mut blocked_micros = 0;
        let mut stats = RawChunkStats::default();
        let mut copy_file_event_gate = CopyFileEventGate::passthrough();

        let (job_tx, job_rx) = bounded(1);
        {
            let mut send_test_job = |tx, cancelled, job| {
                let mut io_state = InNetworkEnqueueIo {
                    tx,
                    event_rx: &event_rx,
                    writer: &mut writer,
                    copy_file_event_gate: &mut copy_file_event_gate,
                    cancelled,
                    producer_blocked_micros: &mut blocked_micros,
                    raw_chunk_stats: &mut stats,
                };
                send_worker_job(&mut io_state, job)
            };
            send_test_job(&job_tx, None, empty_job()).unwrap();
            assert!(matches!(job_rx.recv().unwrap(), WorkerJob::Rates { .. }));

            let cancelled = AtomicBool::new(true);
            assert_eq!(
                send_test_job(&job_tx, Some(&cancelled), empty_job())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::Interrupted
            );

            let (stopped_job_tx, stopped_job_rx) = bounded(1);
            drop(stopped_job_rx);
            assert_eq!(
                send_test_job(&stopped_job_tx, None, empty_job())
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe
            );
        }

        super::bounded_queue_pressure::assert_worker_job_queue_pressure(
            &mut blocked_micros,
            &mut stats,
        );

        let (batch_tx, batch_rx) = bounded(1);
        send_provider_ref_batch(
            &batch_tx,
            &event_rx,
            &mut writer,
            &mut blocked_micros,
            &mut stats,
            &mut copy_file_event_gate,
            empty_batch(),
        )
        .unwrap();
        assert!(batch_rx.recv().unwrap().is_empty());

        let (stopped_batch_tx, stopped_batch_rx) = bounded(1);
        drop(stopped_batch_rx);
        assert_eq!(
            send_provider_ref_batch(
                &stopped_batch_tx,
                &event_rx,
                &mut writer,
                &mut blocked_micros,
                &mut stats,
                &mut copy_file_event_gate,
                empty_batch(),
            )
            .unwrap_err()
            .kind(),
            io::ErrorKind::BrokenPipe
        );

        super::bounded_queue_pressure::assert_provider_reference_queue_pressure(
            &mut blocked_micros,
            &mut stats,
        );

        let mut sink = io::sink();
        let event = super::bounded_queue_pressure::empty_copy_file_event();
        emit_copy_file_event(&mut sink, &event).unwrap();
        let (sink_event_tx, sink_event_rx) = unbounded();
        sink_event_tx.send(event).unwrap();
        drain_copy_file_events(&sink_event_rx, &mut sink, &mut copy_file_event_gate).unwrap();

        let (sink_job_tx, sink_job_rx) = bounded(1);
        let mut sink_io_state = InNetworkEnqueueIo {
            tx: &sink_job_tx,
            event_rx: &sink_event_rx,
            writer: &mut sink,
            copy_file_event_gate: &mut copy_file_event_gate,
            cancelled: None,
            producer_blocked_micros: &mut blocked_micros,
            raw_chunk_stats: &mut stats,
        };
        send_worker_job(&mut sink_io_state, empty_job()).unwrap();
        assert!(matches!(
            sink_job_rx.recv().unwrap(),
            WorkerJob::Rates { .. }
        ));
    }
}
