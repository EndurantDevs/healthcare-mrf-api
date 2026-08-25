#[cfg(test)]
mod tests {
    use super::{
        canonical_modifier_list, canonical_text_list, compare_canonical_decimal_text, int_list,
        normalize_catalog_code, normalize_code, normalize_code_system, normalize_money_text,
        normalize_string, normalize_tin_type, normalize_tin_value, normalized_money_from_reader,
        normalized_scalar_from_reader, normalized_string_list_from_reader, npi_list,
        strict_integer, strict_integer_text, strict_money_number, strict_money_number_from_reader,
        strict_npi_list, strict_npi_partition, strict_npi_partition_allow_empty_tin_only,
        strict_string_array_from_reader, StrictNpiList,
    };
    use serde_json::{json, Value};
    use struson::reader::JsonStreamReader;

    #[test]
    fn normalizes_json_scalars_for_dictionary_keys() {
        assert_eq!(
            normalize_string(Some(&json!(" RC "))),
            Some("RC".to_string())
        );
        assert_eq!(normalize_string(Some(&json!("   "))), None);
        assert_eq!(normalize_string(Some(&json!(450))), Some("450".to_string()));
        assert_eq!(
            normalize_string(Some(&json!(true))),
            Some("true".to_string())
        );
        assert_eq!(normalize_code(Some(&json!(" rc "))), Some("RC".to_string()));
    }

    #[test]
    fn canonicalizes_external_code_systems_and_catalog_codes() {
        assert_eq!(
            normalize_code_system(Some(&json!(" ms-drg "))),
            Some("MS_DRG".to_string())
        );
        assert_eq!(
            normalize_code_system(Some(&json!("revenue_code"))),
            Some("RC".to_string())
        );
        assert_eq!(
            normalize_catalog_code(Some(&json!("7")), Some("MS_DRG")),
            Some("007".to_string())
        );
        assert_eq!(
            normalize_catalog_code(Some(&json!("450")), Some("RC")),
            Some("0450".to_string())
        );
        assert_eq!(
            normalize_catalog_code(Some(&json!("A12.34")), Some("ICD10CM")),
            Some("A1234".to_string())
        );
        assert_eq!(
            normalize_catalog_code(Some(&json!("custom")), Some("CPT")),
            Some("CUSTOM".to_string())
        );
    }

    #[test]
    fn normalizes_tin_and_integer_lists() {
        assert_eq!(normalize_tin_type(Some(&json!(" EIN "))), "ein");
        assert_eq!(normalize_tin_value(Some(&json!(" 12-34 ab "))), "1234AB");
        assert_eq!(int_list(Some(&json!(["2", 1, "bad", 2]))), vec![1, 2]);
        assert_eq!(int_list(Some(&json!("42"))), vec![42]);
    }

    #[test]
    fn normalizes_npi_lists_to_ten_digit_values() {
        assert_eq!(
            npi_list(Some(&json!([
                "123456789",
                "1234567890",
                1234567890.0,
                1.23456789e9,
                9_999_999_999i64,
                10_000_000_000i64,
                "bad"
            ]))),
            vec![1_234_567_890, 9_999_999_999]
        );
    }

    #[test]
    fn strict_provider_identifiers_reject_coercion_and_invalid_npis() {
        assert_eq!(strict_integer(&json!(7), "id").unwrap(), 7);
        assert_eq!(strict_integer(&json!(7.0), "id").unwrap(), 7);
        assert_eq!(
            strict_integer(&serde_json::from_str("7e0").unwrap(), "id").unwrap(),
            7
        );
        assert_eq!(
            strict_integer_text(&json!(121591448686103182592848195376305442061u128), "id").unwrap(),
            "121591448686103182592848195376305442061"
        );
        for invalid in [json!("7"), json!(true), json!({}), json!([]), json!(7.5)] {
            assert!(strict_integer(&invalid, "id").is_err());
        }
        assert!(strict_integer(&json!(u64::MAX), "id").is_err());
        let unbounded_integer =
            serde_json::from_str("121591448686103182592848195376305442061").unwrap();
        assert!(strict_integer(&unbounded_integer, "id").is_err());

        assert_eq!(
            strict_npi_list(Some(&json!([1234567890, 1234567890.0, "1234567890"]))).unwrap(),
            vec![1234567890]
        );
        assert_eq!(
            strict_npi_list(Some(&json!([0, "0"]))).unwrap(),
            Vec::<i64>::new()
        );
        assert_eq!(
            strict_npi_partition(Some(&json!([123456789, 1234567890, 123456789]))).unwrap(),
            StrictNpiList {
                valid: vec![1234567890],
                quarantined: vec![123456789, 123456789],
                empty_array_normalized: false,
            }
        );
        assert_eq!(
            strict_npi_partition(Some(&json!([0, 1234567890, 0]))).unwrap(),
            StrictNpiList {
                valid: vec![1234567890],
                quarantined: Vec::new(),
                empty_array_normalized: false,
            }
        );
        assert_eq!(
            strict_npi_partition(Some(&json!([0, 0]))).unwrap(),
            StrictNpiList::default(),
        );
        assert_eq!(
            strict_npi_partition_allow_empty_tin_only(Some(&json!([]))).unwrap(),
            StrictNpiList {
                empty_array_normalized: true,
                ..StrictNpiList::default()
            },
        );
        assert_eq!(
            strict_npi_partition_allow_empty_tin_only(Some(&json!([])))
                .unwrap()
                .valid,
            strict_npi_list(Some(&json!([0]))).unwrap(),
        );
        for invalid in [
            json!(1234567890_i64),
            json!([]),
            json!([true]),
            json!([{}]),
            json!([[]]),
        ] {
            assert!(strict_npi_list(Some(&invalid)).is_err());
        }
        for invalid_text in [
            " 1234567890",
            "1234567890 ",
            "0123456789",
            "+1234567890",
            "1e9",
            "1234567890.0",
            "123456789",
            "12345678901",
            "１２３４５６７８９０",
        ] {
            assert!(strict_npi_list(Some(&json!([invalid_text]))).is_err());
        }
        assert!(strict_npi_list(None).is_err());
    }

    #[test]
    fn strict_money_reader_accepts_only_canonicalizable_numbers() {
        for (raw, expected) in [("12.3400", "12.34"), ("1.2e2", "120")] {
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            assert_eq!(
                strict_money_number_from_reader(&mut reader).unwrap(),
                Some(expected.to_string())
            );
        }
        for raw in [r#""12.34""#, "true", "{}", "[]", "null", "1e999999999"] {
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            assert!(strict_money_number_from_reader(&mut reader).is_err());
        }
    }

    #[test]
    fn strict_money_value_preserves_arbitrary_precision_canonicalization() {
        for (raw, expected) in [
            ("12.3400", "12.34"),
            ("1.2e2", "120"),
            ("-0.000", "0"),
            (
                "123456789012345678901234567890.0001000",
                "123456789012345678901234567890.0001",
            ),
        ] {
            let value: Value = serde_json::from_str(raw).unwrap();
            assert_eq!(strict_money_number(&value).unwrap(), expected);
        }
    }

    #[test]
    fn canonical_decimal_comparison_matches_exact_numeric_order() {
        use std::cmp::Ordering::{Equal, Greater, Less};

        for (left, right, expected) in [
            ("0", "0", Equal),
            ("-1", "0", Less),
            ("0", "0.0001", Less),
            ("9.9", "10", Less),
            ("-10", "-9.9", Less),
            ("0.009999", "0.01", Less),
            ("1", "1.0000000000000000001", Less),
            ("-1", "-1.0000000000000000001", Greater),
            ("123.455999", "123.456", Less),
            ("100000000000000000000", "99999999999999999999", Greater),
        ] {
            assert_eq!(compare_canonical_decimal_text(left, right), expected);
            assert_eq!(
                compare_canonical_decimal_text(right, left),
                expected.reverse()
            );
        }
    }

    #[test]
    fn canonical_decimal_comparison_handles_huge_values_without_conversion() {
        let smaller_integer = format!("1{}", "0".repeat(131_070));
        let larger_integer = format!("1{}", "0".repeat(131_071));
        assert_eq!(
            compare_canonical_decimal_text(&smaller_integer, &larger_integer),
            std::cmp::Ordering::Less
        );
        assert_eq!(
            compare_canonical_decimal_text(
                &format!("-{smaller_integer}"),
                &format!("-{larger_integer}")
            ),
            std::cmp::Ordering::Greater
        );
        let smaller_fraction = format!("0.{}1", "0".repeat(16_000));
        let larger_fraction = format!("0.{}1", "0".repeat(15_999));
        assert_eq!(
            compare_canonical_decimal_text(&smaller_fraction, &larger_fraction),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn strict_string_array_reader_rejects_scalars_and_non_string_elements() {
        let mut reader = JsonStreamReader::new(br#"["11", " 22 "]"#.as_slice());
        assert_eq!(
            strict_string_array_from_reader(&mut reader, "service_code").unwrap(),
            vec!["11".to_string(), " 22 ".to_string()]
        );
        for raw in [r#""11""#, "true", "12", "{}", "null", r#"["11", 22]"#] {
            let mut reader = JsonStreamReader::new(raw.as_bytes());
            assert!(strict_string_array_from_reader(&mut reader, "service_code").is_err());
        }
    }

    #[test]
    fn normalizes_money_text_without_changing_integer_strings() {
        assert_eq!(
            normalize_money_text("10.5000".to_string()),
            Some("10.5".to_string())
        );
        assert_eq!(
            normalize_money_text("10.000".to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            normalize_money_text("10".to_string()),
            Some("10".to_string())
        );
        assert_eq!(
            normalize_money_text("1.2300e10".to_string()),
            Some("12300000000".to_string())
        );
        assert_eq!(
            normalize_money_text("-1.2500E-3".to_string()),
            Some("-0.00125".to_string())
        );
        assert_eq!(
            normalize_money_text("+001.2300e2".to_string()),
            Some("123".to_string())
        );
        assert_eq!(
            normalize_money_text("-0e100".to_string()),
            Some("0".to_string())
        );
        assert_eq!(
            normalize_money_text("1e64".to_string()),
            Some(format!("1{}", "0".repeat(64)))
        );
        assert_eq!(normalize_money_text("".to_string()), None);
    }

    #[test]
    fn normalizes_scalar_values_from_streaming_reader() {
        let mut reader = JsonStreamReader::new(br#" " value " "#.as_slice());
        assert_eq!(
            normalized_scalar_from_reader(&mut reader).unwrap(),
            Some("value".to_string())
        );

        let mut reader = JsonStreamReader::new(br#"true"#.as_slice());
        assert_eq!(
            normalized_scalar_from_reader(&mut reader).unwrap(),
            Some("true".to_string())
        );
    }

    #[test]
    fn normalizes_money_and_lists_from_streaming_reader() {
        let mut reader = JsonStreamReader::new(br#"10.5000"#.as_slice());
        assert_eq!(
            normalized_money_from_reader(&mut reader).unwrap(),
            Some("10.5".to_string())
        );

        let mut reader = JsonStreamReader::new(br#"[" 26 ","",42,{"skip":true}]"#.as_slice());
        assert_eq!(
            normalized_string_list_from_reader(&mut reader).unwrap(),
            vec!["26".to_string(), "42".to_string()]
        );
    }

    #[test]
    fn canonical_text_lists_trim_sort_dedupe_and_optionally_uppercase() {
        assert_eq!(
            canonical_text_list(
                vec![" b ".to_string(), "A".to_string(), "b".to_string()],
                true
            ),
            vec!["A".to_string(), "B".to_string()]
        );
        assert_eq!(
            canonical_text_list(
                vec![" b ".to_string(), "a".to_string(), "".to_string()],
                false
            ),
            vec!["a".to_string(), "b".to_string()]
        );
    }

    #[test]
    fn canonical_text_list_reuses_already_trimmed_owned_values() {
        let value = String::from("already-trimmed");
        let allocation = value.as_ptr();
        let canonical = canonical_text_list(vec![value], false);
        assert_eq!(canonical, ["already-trimmed"]);
        assert_eq!(canonical[0].as_ptr(), allocation);
    }

    #[test]
    fn canonical_modifier_lists_split_payer_joined_values() {
        assert_eq!(
            canonical_modifier_list(vec![
                " tc, 26 ".to_string(),
                "26".to_string(),
                ",,".to_string(),
            ]),
            vec!["26".to_string(), "TC".to_string()]
        );
    }
}
