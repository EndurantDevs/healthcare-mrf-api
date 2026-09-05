mod tests {
    use super::super::*;
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

    #[cfg(unix)]
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

    #[cfg(unix)]
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

    fn parse_v4_inline_rate(raw_provider_groups: &str) -> RateLite {
        let raw_rate = format!(
            r#"{{"provider_groups":{raw_provider_groups},"negotiated_prices":[{{"negotiated_rate":1}}]}}"#
        );
        let (rate, typed) =
            read_rate_lite_bytes_profiled_with_policy(raw_rate.as_bytes(), true).unwrap();
        assert!(typed, "V4 production parser should retain the raw array");
        rate.expect("V4 inline rate")
    }

    fn price_json(field_name: &str, field_value: &str) -> String {
        format!(r#"{{"negotiated_rate":12.5,"{field_name}":{field_value}}}"#)
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

    fn fill_provider_identifier_quarantine(dedupe: &SharedDedupe) {
        let values = (1..=1024)
            .map(|value| -i64::from(value))
            .collect::<Vec<_>>();
        dedupe
            .record_quarantined_provider_identifiers(&values)
            .unwrap();
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

    mod finalizer_and_provider;
    mod scanner_and_v3_finalizer;
    mod serving_and_runtime;
}
