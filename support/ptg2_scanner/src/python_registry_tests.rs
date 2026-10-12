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
