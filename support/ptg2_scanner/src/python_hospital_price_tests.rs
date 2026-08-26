#[test]
fn python_hospital_price_selector_is_canonical_and_bounded() {
    Python::initialize();
    Python::attach(|py| {
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
            hospital_price_selector_sha256(py, "code", "CPT", "12345")
                .unwrap()
                .as_bytes(),
            crate::hospital_price_selector_block::selector_key_sha256(&selector_key),
        );
        let selector_page = hospital_price_decode_selector_page(
            py,
            &PyBytes::new(py, &selector_payload),
            "code",
            "CPT",
            "12345",
            vec![(2, 6)],
            2,
        )
        .unwrap();
        assert_eq!(
            selector_page
                .get_item("refs")
                .unwrap()
                .unwrap()
                .extract::<Vec<u64>>()
                .unwrap(),
            vec![3, 5],
        );
        assert_eq!(
            selector_page
                .get_item("ref_count")
                .unwrap()
                .unwrap()
                .extract::<usize>()
                .unwrap(),
            3,
        );
        assert!(hospital_price_decode_selector_page(
            py,
            &PyBytes::new(py, &selector_payload),
            "code",
            "CPT",
            "12345",
            vec![(3, 3)],
            2,
        )
        .is_err());
    });
}

#[test]
fn python_hospital_price_decoders_return_normalized_rows() {
    Python::initialize();
    Python::attach(|py| {
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
        let services = hospital_price_decode_service_block(
            py,
            &PyBytes::new(py, &service_payload),
        )
        .unwrap();
        assert_eq!(services.len(), 1);
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
        let facts = hospital_price_decode_fact_block(py, &PyBytes::new(py, &fact_payload)).unwrap();
        assert_eq!(facts.len(), 1);
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
    });
}
