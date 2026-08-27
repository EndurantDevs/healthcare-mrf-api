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
                plan_name: "Synthetic plan".to_owned(),
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
                plan_name: "Synthetic plan".to_owned(),
                negotiated_dollar: Some("75.00".to_owned()),
                negotiated_percentage: None,
                negotiated_algorithm: None,
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
        assert!(decode_services
            .call1((PyBytes::new(py, b"invalid"),))
            .is_err());
        assert!(decode_facts
            .call1((PyBytes::new(py, b"invalid"),))
            .is_err());
    });
}
