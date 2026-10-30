// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

fn assert_naic_copy_conflict_records(bytes: &[u8]) {
    let header = crate::network_membership_codec::COPY_HEADER;
    assert!(bytes.starts_with(header));
    let mut offset = header.len();
    for source_row in [2i32, 3] {
        assert_eq!(
            i16::from_be_bytes(bytes[offset..offset + 2].try_into().unwrap()),
            6
        );
        offset += 2;
        let mut fields = Vec::new();
        for _ in 0..6 {
            let size = i32::from_be_bytes(bytes[offset..offset + 4].try_into().unwrap()) as usize;
            offset += 4;
            fields.push(&bytes[offset..offset + size]);
            offset += size;
        }
        assert_eq!(fields[2], source_row.to_be_bytes());
        assert_eq!(fields[3], b"unresolved");
        assert_eq!(fields[4][0], 1);
        let row: Value = serde_json::from_slice(&fields[4][1..]).unwrap();
        assert_eq!(row["normalized_naic_company"], "00456");
        assert!(row["valid_from"].is_null() && row["valid_to"].is_null());
    }
    assert_eq!(&bytes[offset..], &(-1i16).to_be_bytes());
}

fn registry_bytes_digest(input: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn naic_python_edition(input: &[u8]) -> Vec<u8> {
    use crate::naic_company_registry::{NaicEdition, NaicMember, PARSER_VERSION};
    let member = NaicMember::GroupCompanies;
    serde_json::to_vec(&NaicEdition {
        snapshot_id: "01234567-89ab-cdef-8123-456789abcdef".to_owned(),
        edition_id: "synthetic-2026".to_owned(),
        reporting_year: 2026,
        artifact_sha256: "a".repeat(64),
        member,
        member_sha256: registry_bytes_digest(input),
        layout: member.layout().to_owned(),
        layout_pdf_sha256: "b".repeat(64),
        header_sha256: registry_bytes_digest(&serde_json::to_vec(member.headers()).unwrap()),
        parser_version: PARSER_VERSION.to_owned(),
    })
    .unwrap()
}

#[test]
fn python_naic_encoder_retains_conflict_frames() {
    Python::initialize();
    Python::attach(|py| {
        let input = b"GROUP NAME,GROUP CODE,COMPANY CODE,FEIN NUMBER,COMPANY STATUS,STATE OF DOMICILE,COMPANY NAME\nSynthetic Group,707,456,01-2345678,1,CA,Synthetic Company\nSynthetic Group,808,00456,01-2345678,1,CA,Synthetic Company\n";
        let edition = naic_python_edition(input);
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let encoded = module
            .getattr("encode_naic_company_registry_observations")
            .unwrap()
            .call1((PyBytes::new(py, input), PyBytes::new(py, &edition)))
            .unwrap();
        let (copy, count, metadata): (Py<PyBytes>, usize, Py<PyBytes>) = encoded.extract().unwrap();
        assert_eq!(
            registry_bytes_digest(copy.bind(py).as_bytes()),
            "ab05a0fbb67935d62a340d15a6e09e3747b589242d7e145a1f6a31b3d27afdf8"
        );
        assert_eq!(
            registry_bytes_digest(metadata.bind(py).as_bytes()),
            "534bdb2f68365df8d720218df43de84eafde6b38de0ef5d42bbad70f625cc6cb"
        );
        let parsed = module
            .getattr("parse_naic_company_registry_member")
            .unwrap()
            .call1((PyBytes::new(py, input), PyBytes::new(py, &edition)))
            .unwrap();
        let parsed: Py<PyBytes> = parsed.extract().unwrap();
        assert_eq!(
            registry_bytes_digest(parsed.bind(py).as_bytes()),
            "011a37f51d9a7e59afb65820cb37284c21a79c4bd9773f785e3ead3c7faa2cf3"
        );
        let metadata: Value = serde_json::from_slice(metadata.bind(py).as_bytes()).unwrap();
        assert_eq!(count, 2);
        assert_eq!(metadata["counts"]["unresolved_rows"], 2);
        assert_eq!(metadata["conflicts"][0]["field"], "GROUP CODE");
        assert_eq!(
            metadata["conflicts"][0]["source_rows"],
            serde_json::json!([2, 3])
        );
        assert_naic_copy_conflict_records(copy.bind(py).as_bytes());
        assert_registry_output_contract(&module);
    });
}

fn assert_registry_output_contract(module: &Bound<'_, PyModule>) {
    let py = module.py();
    let empty = PyBytes::new(py, b"[]");
    let classified = module
        .getattr("classify_registry_assertions")
        .unwrap()
        .call1((&empty, "group_inclusive"))
        .unwrap();
    let classified: Py<PyBytes> = classified.extract().unwrap();
    assert_eq!(classified.bind(py).as_bytes(), br#"{"policy":"group_inclusive","assertions":[],"reviews":[],"counts":{"input_rows":0,"accepted_rows":0,"duplicate_rows":0,"review_required_rows":0}}"#);
    let catalog = module
        .getattr("validate_network_catalog_batch")
        .unwrap()
        .call1((&empty,))
        .unwrap();
    let catalog: Py<PyBytes> = catalog.extract().unwrap();
    assert_eq!(catalog.bind(py).as_bytes(), b"[]");
    let catalog = module.getattr("validate_network_catalog_batch").unwrap();
    let error = catalog
        .call1((PyBytes::new(py, b"{\"networks\":false}"),))
        .unwrap_err();
    assert!(error.is_instance_of::<PyValueError>(py));
    assert_eq!(
        error.value(py).str().unwrap().to_str().unwrap(),
        r#"{"code":"invalid_json","row_index":0,"field":null}"#
    );
    let error = catalog.call1((42,)).unwrap_err();
    assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
    let error = module
        .getattr("classify_registry_assertions")
        .unwrap()
        .call1((PyBytes::new(py, b"{}"), "group_inclusive"))
        .unwrap_err();
    assert!(error.is_instance_of::<PyValueError>(py));
    assert_eq!(
        error.value(py).str().unwrap().to_str().unwrap(),
        r#"{"code":"invalid_json","row_index":null,"field":null}"#
    );
}

#[test]
fn python_registry_wrappers_refuse_invalid_inputs() {
    Python::initialize();
    Python::attach(|py| {
        let input = b"GROUP NAME,GROUP CODE,COMPANY CODE,FEIN NUMBER,COMPANY STATUS,STATE OF DOMICILE,COMPANY NAME\nSynthetic Group,707,456,01-2345678,1,CA,Synthetic Company\n";
        let valid = naic_python_edition(input);
        let mut invalid: Value = serde_json::from_slice(&valid).unwrap();
        invalid["layout"] = serde_json::json!("unsupported");
        let wrong_scope = serde_json::to_vec(&invalid).unwrap();
        let oversized = vec![b' '; 65_537];
        let malformed = b"{\"member\":";
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        for name in [
            "encode_naic_company_registry_observations",
            "parse_naic_company_registry_member",
        ] {
            let function = module.getattr(name).unwrap();
            for (edition, message) in [
                (
                    oversized.as_slice(),
                    "Company listing edition metadata exceeds byte limit",
                ),
                (
                    malformed.as_slice(),
                    "Company listing edition metadata is invalid",
                ),
                (
                    wrong_scope.as_slice(),
                    r#"{"code":"invalid_edition","source_row_number":null}"#,
                ),
            ] {
                let error = function
                    .call1((PyBytes::new(py, input), PyBytes::new(py, edition)))
                    .unwrap_err();
                assert!(error.is_instance_of::<PyValueError>(py));
                assert_eq!(error.value(py).str().unwrap().to_str().unwrap(), message);
            }
            let broken = b"not,a,valid,header";
            let error = function
                .call1((
                    PyBytes::new(py, broken),
                    PyBytes::new(py, &naic_python_edition(broken)),
                ))
                .unwrap_err();
            assert_eq!(
                error.value(py).str().unwrap().to_str().unwrap(),
                r#"{"code":"invalid_headers","source_row_number":1}"#
            );
            let error = function.call1((7, PyBytes::new(py, &valid))).unwrap_err();
            assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        }
    });
}

#[test]
fn python_business_ledger_bridge_preserves_snapshot_and_canonical_artifact() {
    Python::initialize();
    Python::attach(|py| {
        let snapshot = "34567890-3456-7890-8345-34567890abcd";
        let input = b"COMPANY_ALIAS,COMPANY_NAME,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME\nExample,Example Company,Plan,PPO,Carrier,Network\n";
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let encoded = module
            .getattr("encode_registry_business_target_ledger_artifact")
            .unwrap()
            .call1((PyBytes::new(py, input), snapshot))
            .unwrap();
        let (copy, descriptor): (Py<PyBytes>, Py<PyBytes>) = encoded.extract().unwrap();
        let copy = copy.bind(py).as_bytes();
        assert!(copy.starts_with(crate::network_membership_codec::COPY_HEADER));
        assert_eq!(i16::from_be_bytes(copy[19..21].try_into().unwrap()), 6);
        let mut offset = 21;
        let mut fields = Vec::new();
        for _ in 0..6 {
            let length = i32::from_be_bytes(copy[offset..offset + 4].try_into().unwrap());
            assert!(length >= 0);
            offset += 4;
            fields.push(&copy[offset..offset + length as usize]);
            offset += length as usize;
        }
        assert_eq!(&copy[offset..], &(-1i16).to_be_bytes());
        assert_eq!(fields[1], b"ledger:v3");
        assert_eq!(fields[2], &1i32.to_be_bytes());
        assert_eq!(fields[3], b"accepted");
        assert_eq!(fields[4][0], 1);
        assert_eq!(fields[5], b"\x01[]");
        let document: Value = serde_json::from_slice(&fields[4][1..]).unwrap();
        let descriptor: Value = serde_json::from_slice(descriptor.bind(py).as_bytes()).unwrap();
        assert_eq!(descriptor["snapshot_id"], snapshot);
        assert_eq!(descriptor["parser_version"], "registry-target-ledger-v3");
        assert_eq!(descriptor["source_sha256"], registry_bytes_digest(input));
        assert_eq!(
            descriptor["artifact_sha256"],
            registry_bytes_digest(&fields[4][1..])
        );
        assert_eq!(descriptor["source_rows"], 1);
        assert_eq!(descriptor["target_count"], 1);
        let retained = serde_json::json!({"snapshot_id":snapshot,"artifact_sha256":descriptor["artifact_sha256"],"document":document});
        let validated = module
            .getattr("validate_registry_required_target_ledger_artifact")
            .unwrap()
            .call1((PyBytes::new(py, &serde_json::to_vec(&retained).unwrap()),))
            .unwrap();
        let validated: Py<PyBytes> = validated.extract().unwrap();
        let validated: Value = serde_json::from_slice(validated.bind(py).as_bytes()).unwrap();
        assert_eq!(validated["target_count"], 1);
    });
}

#[test]
fn python_business_ledger_bridge_refuses_invalid_bytes_identity_and_types() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let encoder = module
            .getattr("encode_registry_business_target_ledger_artifact")
            .unwrap();
        let snapshot = "34567890-3456-7890-8345-34567890abcd";
        let valid = b"COMPANY_ALIAS,COMPANY_NAME,PLAN_NAME,PLAN_TYPE,PLAN_CARRIER,FIND_CARE_NETWORK_NAME\nExample,Company,Plan,PPO,Carrier,Network\n";
        for (input, identity, code) in [
            (b"".as_slice(), snapshot, "invalid_headers"),
            (b"\xff".as_slice(), snapshot, "invalid_utf8"),
            (valid.as_slice(), "invalid", "invalid_snapshot_id"),
        ] {
            let error = encoder
                .call1((PyBytes::new(py, input), identity))
                .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            assert_eq!(error.value(py).str().unwrap().to_str().unwrap(), code);
        }
        let error = encoder.call1((42, snapshot)).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        let error = encoder.call1((PyBytes::new(py, valid), 42)).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
    });
}

#[test]
fn python_graph_locator_bridge_preserves_coordinates_and_refuses_invalid_metadata() {
    Python::initialize();
    Python::attach(|py| {
        let metadata = serde_json::json!({
            "context": {"graph_identity": {"snapshot_key": 11, "layout_generation": "shared_blocks_v4",
                "layout_mapping_sha256": "01".repeat(32), "map_sha256": "02".repeat(32),
                "finalizer_map_sha256": "03".repeat(32), "source_assignments_sha256": "04".repeat(32)},
                "after_ordinal": 0, "last_ordinal": 1, "row_count": 1},
            "manifest": {"snapshot_key": 11, "relation": "group_npis_exact",
                "member_object_kind": "v4_group_npis_exact_members_v1", "locator_object_kind": "v4_group_npis_exact_locators_v1",
                "owner_base": 5, "owner_count": 1, "logical_member_count": 3, "vector_member_count": 3,
                "member_width": 4, "member_page_bytes": 12, "locator_page_bytes": 12, "locator_owner_span": 1}});
        let selected = [5u32.to_be_bytes(), 5u32.to_be_bytes()].concat();
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let locator = module
            .getattr("plan_registry_ptg_graph_locator_pages")
            .unwrap();
        let encoded: Py<PyBytes> = locator
            .call1((
                PyBytes::new(py, &serde_json::to_vec(&metadata).unwrap()),
                PyBytes::new(py, &selected),
            ))
            .unwrap()
            .extract()
            .unwrap();
        let actual: Value = serde_json::from_slice(encoded.bind(py).as_bytes()).unwrap();
        assert_eq!(
            actual,
            serde_json::json!({"owner_keys": [5], "coordinates": [["v4_group_npis_exact_locators_v1", 5, 0]]})
        );
        for invalid in [
            b"{}".to_vec(),
            vec![b' '; crate::registry_ptg_graph_python::MAX_LOCATOR_METADATA_BYTES + 1],
        ] {
            let error = locator
                .call1((PyBytes::new(py, &invalid), PyBytes::new(py, &selected)))
                .unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
        }
        let error = locator
            .call1((42, PyBytes::new(py, &selected)))
            .unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
        let mut malformed = metadata;
        malformed["map_packs"] = serde_json::json!(true);
        malformed["blocks"] = serde_json::json!([]);
        malformed["heavy"] = serde_json::json!([]);
        let error = module
            .getattr("plan_registry_ptg_graph_member_pages")
            .unwrap()
            .call1((
                PyBytes::new(py, &serde_json::to_vec(&malformed).unwrap()),
                PyBytes::new(py, &selected),
                pyo3::types::PyTuple::empty(py),
            ))
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        assert_eq!(
            error.value(py).str().unwrap().to_str().unwrap(),
            "registry_ptg_graph_metadata_invalid"
        );
    });
}

fn retained_python_input() -> Value {
    let rows: Vec<_> = [
        ("Location", "site-1", r#"{"resource_id": "site-1"}"#),
        ("Organization", "org-1", r#"{"name": "Network", "resource_id": "org-1"}"#),
        ("InsurancePlan", "plan-1", r#"{"network_refs": ["Organization/network-a"], "resource_id": "plan-1"}"#),
        ("Practitioner", "person-1", r#"{"ratio": 1.5, "resource_id": "person-1"}"#),
    ]
    .into_iter()
    .map(|(kind, id, payload)| serde_json::json!({
        "resource_type": kind, "resource_id": id, "payload_json": payload,
        "payload_hash": registry_bytes_digest(payload.as_bytes()),
        "acquired_resource_sha256": null,
        "site_id": if kind == "Location" { Some("01234567-89ab-cdef-8123-456789abcdef") } else { None },
    }))
    .collect();
    serde_json::json!({
        "source_id": "source-a", "release_id": "release-1", "observed_at_pg_micros": 1234,
        "hash_contract": "transport_bound_v1", "derive": true,
        "resource_types": ["Location", "Organization", "InsurancePlan", "Practitioner", "Medication"],
        "rows": rows,
    })
}

#[test]
fn python_retained_capture_preserves_copy_roots_families_and_fallback_accounting() {
    Python::initialize();
    Python::attach(|py| {
        let input = retained_python_input();
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let encoded = module
            .getattr("encode_retained_fhir_capture_batch")
            .unwrap()
            .call1((PyBytes::new(py, &serde_json::to_vec(&input).unwrap()),))
            .unwrap();
        let (bindings, evidence, plans, roots, families, count, fallback, owned):
            RetainedCaptureOutput = encoded.extract().unwrap();
        assert_eq!(count, 4);
        assert_eq!(fallback, [3]);
        let mut copy_bytes = 0;
        for (batches, columns, rows) in [(&bindings, 6i16, 1), (&evidence, 7, 2), (&plans, 11, 1)] {
            assert_eq!(batches.len(), 1);
            assert_eq!(batches[0].1, rows);
            let bytes = batches[0].0.bind(py).as_bytes();
            assert!(bytes.starts_with(crate::network_membership_codec::COPY_HEADER));
            assert_eq!(&bytes[19..21], &columns.to_be_bytes());
            assert_eq!(&bytes[bytes.len() - 2..], &(-1i16).to_be_bytes());
            copy_bytes += bytes.len();
        }
        let expected_roots: Vec<_> = input["rows"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| {
                serde_json::to_vec(&serde_json::json!([
                    row["resource_type"],
                    row["resource_id"],
                    row["payload_hash"]
                ]))
                .unwrap()
            })
            .collect();
        assert_eq!(roots.bind(py).as_bytes(), expected_roots.join(&b'\n'));
        assert_eq!(families.len(), 5);
        for (index, kind) in ["Location", "Organization", "InsurancePlan", "Practitioner"]
            .iter()
            .enumerate()
        {
            assert_eq!(families[*kind].1, 1);
            assert_eq!(families[*kind].0.bind(py).as_bytes(), expected_roots[index]);
        }
        assert_eq!(families["Medication"].1, 0);
        assert!(families["Medication"].0.bind(py).as_bytes().is_empty());
        assert_eq!(
            owned,
            copy_bytes
                + roots.bind(py).as_bytes().len()
                + families
                    .values()
                    .map(|(bytes, _)| bytes.bind(py).as_bytes().len())
                    .sum::<usize>()
                + std::mem::size_of::<usize>()
        );
    });
}

#[test]
fn python_retained_capture_errors_preserve_row_identity_without_payloads() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let encoder = module
            .getattr("encode_retained_fhir_capture_batch")
            .unwrap();
        let mut invalid = retained_python_input();
        invalid["rows"][1]["payload_hash"] = serde_json::json!("0".repeat(64));
        for (input, expected) in [
            (
                b"{}".to_vec(),
                serde_json::json!({"code":"invalid_input","row_index":null}),
            ),
            (
                serde_json::to_vec(&invalid).unwrap(),
                serde_json::json!({"code":"payload_hash","row_index":1}),
            ),
        ] {
            let error = encoder.call1((PyBytes::new(py, &input),)).unwrap_err();
            assert!(error.is_instance_of::<PyValueError>(py));
            let message = error.value(py).str().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(message.to_str().unwrap()).unwrap(),
                expected
            );
        }
        let error = encoder.call1((42,)).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
    });
}

#[test]
fn python_entity_rows_preserve_binary_copy_defaults_and_reject_bad_inputs() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let version: String = module
            .getattr("entity_address_rows_version")
            .unwrap()
            .call0()
            .unwrap()
            .extract()
            .unwrap();
        assert_eq!(version, "entity_address_rows.v1");
        let encoder = module.getattr("prepare_entity_address_rows").unwrap();
        let mut input = crate::network_membership_codec::COPY_HEADER.to_vec();
        input.extend_from_slice(&14i16.to_be_bytes());
        input.extend_from_slice(&8i32.to_be_bytes());
        input.extend_from_slice(&7i64.to_be_bytes());
        for _ in 0..13 {
            input.extend_from_slice(&(-1i32).to_be_bytes());
        }
        input.extend_from_slice(&(-1i16).to_be_bytes());
        let encoded = encoder.call1((PyBytes::new(py, &input),)).unwrap();
        let (copy, count): (Py<PyBytes>, usize) = encoded.extract().unwrap();
        assert_eq!(count, 1);
        let mut expected = crate::network_membership_codec::COPY_HEADER.to_vec();
        expected.extend_from_slice(&27i16.to_be_bytes());
        expected.extend_from_slice(&8i32.to_be_bytes());
        expected.extend_from_slice(&7i64.to_be_bytes());
        for field in 0..24 {
            match field {
                2 => expected.extend_from_slice(b"\0\0\0\x07primary"),
                9 | 22 => expected.extend_from_slice(b"\0\0\0\x02US"),
                _ => expected.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        expected.extend_from_slice(b"\0\0\0\x01\x01\0\0\0\x01\x01\xff\xff");
        assert_eq!(copy.bind(py).as_bytes(), expected);
        let error = encoder
            .call1((PyBytes::new(py, &input[..input.len() - 1]),))
            .unwrap_err();
        assert!(error.is_instance_of::<PyValueError>(py));
        assert_eq!(
            error.value(py).str().unwrap().to_str().unwrap(),
            "Address row COPY is truncated"
        );
        let error = encoder.call1((42,)).unwrap_err();
        assert!(error.is_instance_of::<pyo3::exceptions::PyTypeError>(py));
    });
}
