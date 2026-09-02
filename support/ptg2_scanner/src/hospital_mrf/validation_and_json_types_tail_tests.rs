mod validation_tail_tests {
    use super::*;

    #[test]
    fn validation_and_emit_error_tails_are_explicit() {
        let service = ServiceRow {
            description: "service".to_owned(),
            codes: vec![CodeRow {
                code_type: "CPT".to_owned(),
                code: "12345".to_owned(),
            }],
            drug_unit: None,
            drug_type: None,
        };
        let mut invalid_service = service.clone();
        invalid_service.description.clear();
        assert!(validate_service(invalid_service, false).is_err());
        let mut invalid_service = service.clone();
        invalid_service.codes[0].code.clear();
        assert!(validate_service(invalid_service, false).is_err());
        let mut invalid_service = service.clone();
        invalid_service.drug_type = Some("invalid".to_owned());
        assert!(validate_service(invalid_service, false).is_err());

        let charge = ChargeRow {
            setting: "outpatient".to_owned(),
            billing_class: None,
            modifier_codes: Vec::new(),
            gross_charge: Some("1".to_owned()),
            discounted_cash: None,
            minimum: Some("1".to_owned()),
            maximum: Some("1".to_owned()),
            additional_generic_notes: None,
        };
        let mut invalid_charge = charge;
        invalid_charge.modifier_codes.push(String::new());
        assert!(validate_charge(invalid_charge, &[], false).is_err());

        let payer = PayerChargeRow {
            payer_name: "payer".to_owned(),
            plan_name: "plan".to_owned(),
            negotiated_rate_term: None,
            standard_charge_dollar: Some("1".to_owned()),
            standard_charge_percentage: None,
            standard_charge_algorithm: None,
            estimated_amount: None,
            median_amount: None,
            percentile_10: None,
            percentile_90: None,
            allowed_count: None,
            methodology: "fee schedule".to_owned(),
            additional_payer_notes: None,
        };
        let mut invalid_payer = payer.clone();
        invalid_payer.payer_name.clear();
        assert!(validate_payer(invalid_payer, None, false).is_err());
        let mut invalid_payer = payer.clone();
        invalid_payer.plan_name.clear();
        assert!(validate_payer(invalid_payer, None, false).is_err());
        let mut derived_payer = payer;
        derived_payer.standard_charge_dollar = None;
        derived_payer.standard_charge_percentage = Some("10".to_owned());
        derived_payer.median_amount = Some("1".to_owned());
        derived_payer.percentile_10 = Some("1".to_owned());
        derived_payer.percentile_90 = Some("1".to_owned());
        derived_payer.allowed_count = Some("1".to_owned());
        assert!(validate_payer(derived_payer, None, false).is_ok());

        let previous = JSON_RETAINED_BYTES.with(|budget| budget.replace(Some(0)));
        assert!(serde_json::from_str::<FanoutVec<u8>>("[1]").is_err());
        JSON_RETAINED_BYTES.with(|budget| budget.set(previous));

        let output_directory = tempfile::tempdir().unwrap();
        let mut outputs = CopyOutputs::create(
            output_directory.path(),
            "version",
            1,
            HospitalMrfOutputMode::Legacy,
        )
        .unwrap();
        let mut oversized_service = service.clone();
        oversized_service.description = "x".repeat(16 * 1024);
        assert!(emit_service(&mut outputs, "version", 0, &oversized_service).is_err());

        let output_directory = tempfile::tempdir().unwrap();
        let mut outputs = CopyOutputs::create(
            output_directory.path(),
            "version",
            1,
            HospitalMrfOutputMode::Legacy,
        )
        .unwrap();
        let mut oversized_service = service;
        oversized_service.codes[0].code = "x".repeat(16 * 1024);
        assert!(emit_service(&mut outputs, "version", 0, &oversized_service).is_err());
    }
}
