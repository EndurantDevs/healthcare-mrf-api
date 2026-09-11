#[test]
fn python_hospital_payer_plan_keys_reject_partial_dictionary_iteration() {
    Python::initialize();
    Python::attach(|py| {
        let mut empty = std::iter::empty();
        assert!(hospital_price_dict_list(py, &mut empty).unwrap().is_empty());
        let mut failing = std::iter::once(Err(PyValueError::new_err("invalid synthetic dictionary key")));
        assert!(hospital_price_dict_list(py, &mut failing).unwrap_err().is_instance_of::<PyValueError>(py));
        let first = hospital_price_dict(py, &[
            ("payer_name", hospital_price_py_value(py, "Synthetic payer")),
        ]).unwrap();
        let mut partial = [Ok(first), Err(PyValueError::new_err("invalid synthetic dictionary key"))].into_iter();
        assert!(hospital_price_dict_list(py, &mut partial).unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn python_hospital_payer_plan_keys_validate_call_boundary_and_legacy_header() {
    use crate::hospital_price_selector_block::{
        encode_selector_page, HospitalPriceSelectorEntry, HospitalPriceSelectorKey,
    };
    use pyo3::exceptions::PyTypeError;
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let decode = module.getattr("hospital_price_decode_payer_plan_keys").unwrap();
        let key = HospitalPriceSelectorKey::PayerPlan {
            payer_name: "Synthetic payer".to_owned(), plan_name: Some("Synthetic plan".to_owned()),
        };
        let mut payload = encode_selector_page(key.kind(), 0, 3, &[
            HospitalPriceSelectorEntry { key, refs: vec![0] },
        ]).unwrap();
        payload[8..12].copy_from_slice(&1_u32.to_le_bytes());
        let kwargs = PyDict::new(py);
        kwargs.set_item("payload", PyBytes::new(py, &payload)).unwrap();
        let page = decode.call((), Some(&kwargs)).unwrap();
        assert_eq!(page.get_item("page_count").unwrap().extract::<u32>().unwrap(), 3);
        assert_eq!(page.get_item("page_index").unwrap().extract::<u32>().unwrap(), 0);
        assert_eq!(page.get_item("items").unwrap().len().unwrap(), 1);
        for error in [
            decode.call0().unwrap_err(),
            decode.call1((py.None(),)).unwrap_err(),
            decode.call1(("not bytes",)).unwrap_err(),
            decode.call1((PyBytes::new(py, &payload), 1)).unwrap_err(),
            decode.call((PyBytes::new(py, &payload),), Some(&kwargs)).unwrap_err(),
        ] {
            assert!(error.is_instance_of::<PyTypeError>(py));
        }
        kwargs.set_item("unexpected", 1).unwrap();
        assert!(decode.call((), Some(&kwargs)).unwrap_err().is_instance_of::<PyTypeError>(py));
        for (offset, invalid) in [(16, 4097_u32), (32, 4 * 1024 * 1024 + 1), (36, 4 * 1024 * 1024 + 65537)] {
            let mut corrupt = payload.clone();
            corrupt[offset..offset + 4].copy_from_slice(&invalid.to_le_bytes());
            assert!(decode.call1((PyBytes::new(py, &corrupt),)).unwrap_err().is_instance_of::<PyValueError>(py));
        }
        payload[40] ^= 1;
        assert!(decode.call1((PyBytes::new(py, &payload),)).unwrap_err().is_instance_of::<PyValueError>(py));
    });
}

#[test]
fn python_hospital_payer_plan_keys_preserve_maximum_page_in_digest_order() {
    use crate::hospital_price_selector_block::{
        encode_selector_page, selector_key_sha256, HospitalPriceSelectorEntry,
        HospitalPriceSelectorKey, HospitalPriceSelectorKind, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS,
    };
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let decode = module.getattr("hospital_price_decode_payer_plan_keys").unwrap();
        let mut entries = (0..HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS).map(|index| {
            HospitalPriceSelectorEntry {
                key: HospitalPriceSelectorKey::PayerPlan {
                    payer_name: format!("Synthetic payer {index:04}"),
                    plan_name: Some(format!("Synthetic plan {index:04}")),
                },
                refs: vec![index as u64],
            }
        }).collect::<Vec<_>>();
        let payload = encode_selector_page(HospitalPriceSelectorKind::PayerPlanToFact, 0, 1, &entries).unwrap();
        let page = decode.call1((PyBytes::new(py, &payload),)).unwrap();
        let items = page.get_item("items").unwrap();
        assert_eq!(items.len().unwrap(), HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS);
        entries.sort_unstable_by_key(|entry| selector_key_sha256(&entry.key));
        for (index, entry) in entries.iter().enumerate() {
            let item = items.get_item(index).unwrap();
            assert_eq!(item.get_item("key_sha256").unwrap().extract::<Vec<u8>>().unwrap(), selector_key_sha256(&entry.key));
            let HospitalPriceSelectorKey::PayerPlan { payer_name, plan_name } = &entry.key else { panic!("expected payer key"); };
            assert_eq!(item.get_item("payer_name").unwrap().extract::<String>().unwrap(), *payer_name);
            assert_eq!(item.get_item("plan_name").unwrap().extract::<Option<String>>().unwrap(), *plan_name);
        }
    });
}

#[test]
fn python_hospital_payer_plan_keys_keep_pairs_and_missing_plans() {
    use crate::hospital_price_selector_block::{
        encode_selector_page, selector_key_sha256, HospitalPriceSelectorEntry,
        HospitalPriceSelectorKey, HospitalPriceSelectorKind,
    };
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let decode = module.getattr("hospital_price_decode_payer_plan_keys").unwrap();
        let mut entries = vec![
            HospitalPriceSelectorEntry {
                key: HospitalPriceSelectorKey::PayerPlan {
                    payer_name: "Synthetic payer".to_owned(), plan_name: None,
                }, refs: vec![0, 2],
            },
            HospitalPriceSelectorEntry {
                key: HospitalPriceSelectorKey::PayerPlan {
                    payer_name: "Synthetic payer".to_owned(), plan_name: Some("Synthetic plan".to_owned()),
                }, refs: vec![1],
            },
        ];
        let payload = encode_selector_page(HospitalPriceSelectorKind::PayerPlanToFact, 0, 1, &entries).unwrap();
        let page = decode.call1((PyBytes::new(py, &payload),)).unwrap();
        let items = page.get_item("items").unwrap();
        assert_eq!(items.len().unwrap(), 2);
        entries.sort_unstable_by_key(|entry| selector_key_sha256(&entry.key));
        for (index, entry) in entries.iter().enumerate() {
            let item = items.get_item(index).unwrap();
            assert_eq!(item.get_item("key_sha256").unwrap().extract::<Vec<u8>>().unwrap(), selector_key_sha256(&entry.key));
            let HospitalPriceSelectorKey::PayerPlan { payer_name, plan_name } = &entry.key else { panic!("expected payer key"); };
            assert_eq!(item.get_item("payer_name").unwrap().extract::<String>().unwrap(), *payer_name);
            assert_eq!(item.get_item("plan_name").unwrap().extract::<Option<String>>().unwrap(), *plan_name);
        }
        assert!(decode.call1((PyBytes::new(py, b"invalid"),)).is_err());
        let continuation = encode_selector_page(HospitalPriceSelectorKind::PayerPlanToFact, 1, 2, &entries[..1]).unwrap();
        assert!(decode.call1((PyBytes::new(py, &continuation),)).is_err());
        let code = HospitalPriceSelectorEntry { key: HospitalPriceSelectorKey::Code {
            code_type: "CPT".to_owned(), code: "12345".to_owned(),
        }, refs: vec![0] };
        let wrong_kind = encode_selector_page(HospitalPriceSelectorKind::CodeToCharge, 0, 1, &[code]).unwrap();
        assert!(decode.call1((PyBytes::new(py, &wrong_kind),)).is_err());
    });
}

#[test]
fn python_hospital_price_selector_is_canonical_and_bounded() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let selector_sha256 = module.getattr("hospital_price_selector_sha256").unwrap();
        let decode_selector = module
            .getattr("hospital_price_decode_selector_page")
            .unwrap();
        let selector_key =
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
                code_type: "CPT".to_owned(),
                code: "12345".to_owned(),
            };
        let selector_payload = crate::hospital_price_selector_block::encode_selector_page(
            selector_key.kind(),
            0,
            1,
            &[crate::hospital_price_selector_block::HospitalPriceSelectorEntry {
                key: selector_key.clone(),
                refs: vec![1, 3, 5],
            }],
        )
        .unwrap();
        assert_eq!(
            selector_sha256
                .call1(("code", "CPT", "12345"))
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap(),
            crate::hospital_price_selector_block::selector_key_sha256(&selector_key),
        );
        let payer_key =
            crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
                payer_name: "Synthetic payer".to_owned(),
                plan_name: Some("Synthetic plan".to_owned()),
            };
        assert_eq!(
            selector_sha256
                .call1(("payer_plan", "Synthetic payer", "Synthetic plan"))
                .unwrap()
                .extract::<Vec<u8>>()
                .unwrap(),
            crate::hospital_price_selector_block::selector_key_sha256(&payer_key),
        );
        assert!(selector_sha256.call1(("invalid", "a", "b")).is_err());
        assert!(selector_sha256.call1(("code", "CPT", py.None())).is_err());
        let missing_key = crate::hospital_price_selector_block::HospitalPriceSelectorKey::PayerPlan {
            payer_name: "Synthetic payer".to_owned(), plan_name: None,
        };
        assert_eq!(selector_sha256.call1(("payer_plan", "Synthetic payer", py.None()))
            .unwrap().extract::<Vec<u8>>().unwrap(),
            crate::hospital_price_selector_block::selector_key_sha256(&missing_key));
        let missing_payload = crate::hospital_price_selector_block::encode_selector_page(
            missing_key.kind(), 0, 1,
            &[crate::hospital_price_selector_block::HospitalPriceSelectorEntry {
                key: missing_key, refs: vec![1, 3],
            }],
        ).unwrap();
        let missing_refs = decode_selector.call1((PyBytes::new(py, &missing_payload),
            "payer_plan", "Synthetic payer", py.None(), vec![(0_u64, 4_u64)], 10_usize))
            .unwrap();
        assert_eq!(missing_refs.get_item("refs").unwrap().extract::<Vec<u64>>().unwrap(), vec![1, 3]);

        let selector_page = decode_selector
            .call1((
                PyBytes::new(py, &selector_payload),
                "code",
                "CPT",
                "12345",
                vec![(2_u64, 6_u64)],
                2_usize,
            ))
            .unwrap();
        assert_eq!(
            selector_page
                .get_item("refs")
                .unwrap()
                .extract::<Vec<u64>>()
                .unwrap(),
            vec![3, 5],
        );
        assert_eq!(
            selector_page
                .get_item("ref_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            3,
        );
        assert_eq!(
            selector_page
                .get_item("row_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            1,
        );
        assert_eq!(
            selector_page
                .get_item("page_ref_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            3,
        );
        assert!(selector_page
            .get_item("found")
            .unwrap()
            .extract::<bool>()
            .unwrap());
        assert_eq!(
            selector_page
                .get_item("first_ref")
                .unwrap()
                .extract::<Option<u64>>()
                .unwrap(),
            Some(1),
        );
        for (ranges, max_refs) in [
            (vec![], 2_usize),
            (vec![(0, 1)], 0),
            (vec![(0, 1)], 10_002),
            (vec![(3, 3)], 2),
            (vec![(0, 4), (3, 6)], 2),
        ] {
            assert!(decode_selector
                .call1((
                    PyBytes::new(py, &selector_payload),
                    "code",
                    "CPT",
                    "12345",
                    ranges,
                    max_refs,
                ))
                .is_err());
        }
        let truncated_page = decode_selector
            .call1((
                PyBytes::new(py, &selector_payload),
                "code",
                "CPT",
                "12345",
                vec![(0_u64, 2_u64), (3_u64, 6_u64)],
                1_usize,
            ))
            .unwrap();
        assert_eq!(
            truncated_page
                .get_item("refs")
                .unwrap()
                .extract::<Vec<u64>>()
                .unwrap(),
            vec![1],
        );
        assert!(truncated_page
            .get_item("truncated")
            .unwrap()
            .extract::<bool>()
            .unwrap());
        assert!(decode_selector
            .call1((
                PyBytes::new(py, &selector_payload),
                "invalid",
                "CPT",
                "12345",
                vec![(0_u64, 6_u64)],
                2_usize,
            ))
            .is_err());
        assert!(decode_selector
            .call1((
                PyBytes::new(py, b"invalid"),
                "code",
                "CPT",
                "12345",
                vec![(0_u64, 6_u64)],
                2_usize,
            ))
            .is_err());
        let multiple_key_payload =
            crate::hospital_price_selector_block::encode_selector_page(
                selector_key.kind(),
                0,
                1,
                &[
                    crate::hospital_price_selector_block::HospitalPriceSelectorEntry {
                        key: selector_key.clone(),
                        refs: vec![1],
                    },
                    crate::hospital_price_selector_block::HospitalPriceSelectorEntry {
                        key: crate::hospital_price_selector_block::HospitalPriceSelectorKey::Code {
                            code_type: "CPT".to_owned(),
                            code: "67890".to_owned(),
                        },
                        refs: vec![2],
                    },
                ],
            )
            .unwrap();
        let multiple_key_page = decode_selector
            .call1((
                PyBytes::new(py, &multiple_key_payload),
                "code",
                "CPT",
                "12345",
                vec![(0_u64, 3_u64)],
                3_usize,
            ))
            .unwrap();
        assert_eq!(
            multiple_key_page
                .get_item("refs")
                .unwrap()
                .extract::<Vec<u64>>()
                .unwrap(),
            vec![1],
        );
        assert_eq!(
            multiple_key_page
                .get_item("row_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2,
        );
        assert_eq!(
            multiple_key_page
                .get_item("page_ref_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            2,
        );
        assert!(multiple_key_page
            .get_item("found")
            .unwrap()
            .extract::<bool>()
            .unwrap());

        let missing_key_page = decode_selector
            .call1((
                PyBytes::new(py, &multiple_key_payload),
                "code",
                "CPT",
                "absent",
                vec![(0_u64, 3_u64)],
                3_usize,
            ))
            .unwrap();
        assert!(!missing_key_page
            .get_item("found")
            .unwrap()
            .extract::<bool>()
            .unwrap());
        assert_eq!(
            missing_key_page
                .get_item("ref_count")
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            0,
        );
        assert_eq!(
            missing_key_page
                .get_item("first_ref")
                .unwrap()
                .extract::<Option<u64>>()
                .unwrap(),
            None,
        );
        assert!(missing_key_page
            .get_item("refs")
            .unwrap()
            .extract::<Vec<u64>>()
            .unwrap()
            .is_empty());
    });
}

#[test]
fn python_hospital_price_decoders_return_normalized_rows() {
    Python::initialize();
    Python::attach(|py| {
        let module = PyModule::new(py, "ptg2_address_canon").unwrap();
        ptg2_address_canon(&module).unwrap();
        let decode_services = module
            .getattr("hospital_price_decode_service_block")
            .unwrap();
        let decode_facts = module
            .getattr("hospital_price_decode_fact_block")
            .unwrap();
        let service_payload = crate::hospital_price_service_block::encode_service_block(&[
            crate::hospital_price_service_block::HospitalPriceServiceRow {
                service_ordinal: 0,
                description: "Synthetic service".to_owned(),
                drug_unit: None,
                drug_type: None,
                codes: vec![
                    crate::hospital_price_service_block::HospitalPriceServiceCode {
                        code_type: "CPT".to_owned(),
                        code: "12345".to_owned(),
                    },
                ],
                charges: vec![
                    crate::hospital_price_service_block::HospitalPriceChargeRow {
                        charge_key: 0,
                        charge_ordinal: 0,
                        setting: "outpatient".to_owned(),
                        billing_class: Some("facility".to_owned()),
                        modifier_codes: Vec::new(),
                        gross_charge: Some("100.00".to_owned()),
                        discounted_cash: Some("80.00".to_owned()),
                        minimum: Some("70.00".to_owned()),
                        maximum: Some("120.00".to_owned()),
                        additional_generic_notes: None,
                        first_fact_ordinal: 0,
                        fact_count: 1,
                    },
                ],
            },
        ])
        .unwrap();
        let services = decode_services
            .call1((PyBytes::new(py, &service_payload),))
            .unwrap();
        assert_eq!(services.len().unwrap(), 1);
        assert_eq!(
            services
                .get_item(0)
                .unwrap()
                .get_item("description")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "Synthetic service",
        );

        let fact_payload = crate::hospital_price_block::encode_fact_block(&[
            crate::hospital_price_block::HospitalPriceFactRow {
                charge_key: 0,
                payer_name: "Synthetic payer".to_owned(),
                plan_name: Some("Synthetic plan".to_owned()),
                negotiated_rate_term: Some("JAN 2026-MAY 2026".to_owned()),
                negotiated_dollar: Some("75.00".to_owned()),
                negotiated_percentage: None,
                negotiated_algorithm: None,
                estimated_amount: Some("74.00".to_owned()),
                methodology: "fee schedule".to_owned(),
                median_amount: None,
                percentile_10: None,
                percentile_90: None,
                allowed_count: None,
                additional_payer_notes: None,
                comparison_amount: Some("75.00".to_owned()),
            },
        ])
        .unwrap();
        let facts = decode_facts
            .call1((PyBytes::new(py, &fact_payload),))
            .unwrap();
        assert_eq!(facts.len().unwrap(), 1);
        assert_eq!(
            facts
                .get_item(0)
                .unwrap()
                .get_item("payer_name")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "Synthetic payer",
        );
        assert_eq!(
            facts
                .get_item(0)
                .unwrap()
                .get_item("negotiated_rate_term")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "JAN 2026-MAY 2026",
        );
        assert_eq!(
            facts
                .get_item(0)
                .unwrap()
                .get_item("estimated_amount")
                .unwrap()
                .extract::<String>()
                .unwrap(),
            "74.00",
        );
        assert!(decode_services
            .call1((PyBytes::new(py, b"invalid"),))
            .is_err());
        let mut missing_rows = crate::hospital_price_block::decode_fact_block(
            &fact_payload, None, None, 0, 10).unwrap();
        missing_rows[0].plan_name = None;
        let missing_payload = crate::hospital_price_block::encode_fact_block(&missing_rows).unwrap();
        let missing_facts = decode_facts.call1((PyBytes::new(py, &missing_payload),)).unwrap();
        assert!(missing_facts.get_item(0).unwrap().get_item("plan_name").unwrap().is_none());
        assert!(decode_facts
            .call1((PyBytes::new(py, b"invalid"),))
            .is_err());
    });
}
