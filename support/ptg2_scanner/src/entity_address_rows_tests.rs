use super::*;

#[test]
fn soft_street_keys_match_whole_word_sql_substitutions() {
    for (raw, expected) in [
        (None, ""),
        (Some(" ! \t "), ""),
        (
            Some("WEST east North south STREET avenue road boulevard drive lane highway"),
            "wensstaverdblvddrlnhwy",
        ),
        (
            Some("West WEST Westward 1West _west west_ west_street"),
            "wwwestward1westwestwestweststreet",
        ),
        (Some("Street/street.street-street's"), "ststststs"),
        (
            Some("west\u{a0}street\u{2000}avenue\u{202f}road"),
            "wstaverd",
        ),
        (
            Some("streetstreet ST_strEEt 9road road9"),
            "streetstreetststreet9roadroad9",
        ),
    ] {
        assert_eq!(street_soft_norm(raw), expected, "{raw:?}");
    }
}

#[test]
fn grouping_fields_preserve_raw_sql_whitespace_and_null_rules() {
    let fields = [
        Some(" 10 WEST\u{a0}STREET "),
        Some(" Apt_2 \t"),
        Some(" O'Fallon "),
        Some(" \tnew york\u{a0} "),
        Some(" 12-X3 "),
        Some(" \u{a0}u.s.\t "),
    ];
    assert_eq!(
        prepare_address_group_fields(fields),
        AddressGroupFields {
            street_key: "10wst".to_owned(),
            line1_norm: Some("10weststreet".to_owned()),
            line2_norm: Some("apt2".to_owned()),
            city_norm: Some("ofallon".to_owned()),
            state_code: Some("\tNEW YORK\u{a0}".to_owned()),
            zip5: Some("123".to_owned()),
            country_norm: Some("\u{a0}U.S.\t".to_owned()),
            keyable: true,
        }
    );
    let empty = prepare_address_group_fields([None; 6]);
    assert_eq!(empty, prepare_address_group_fields([Some(""); 6]));
    assert_eq!(empty.street_key, "");
    assert!(empty.line1_norm.is_none() && empty.line2_norm.is_none() && empty.city_norm.is_none());
    assert!(empty.state_code.is_none() && empty.zip5.is_none() && empty.country_norm.is_none());
    assert!(!empty.keyable);
    assert_eq!(prepare_address_group_fields([Some(" "); 6]), empty);
    assert_eq!(
        prepare_address_group_fields([Some("123456789"); 6])
            .zip5
            .as_deref(),
        Some("12345")
    );
}

#[test]
fn keyable_predicate_preserves_text_state_and_zip_sentinels() {
    let base = [
        Some("1 Main Street"),
        None,
        None,
        Some("NY"),
        Some("10001"),
        Some("US"),
    ];
    for value in [
        None,
        Some(""),
        Some(" "),
        Some("NULL"),
        Some("NoNe"),
        Some("n-a"),
        Some("n_a_n"),
        Some("Unknown"),
        Some("Unspecified"),
    ] {
        let mut fields = base;
        fields[0] = value;
        assert!(
            !prepare_address_group_fields(fields).keyable,
            "text {value:?}"
        );
        fields[2] = Some("!");
        assert!(
            prepare_address_group_fields(fields).keyable,
            "city {value:?}"
        );
    }
    for value in [
        None,
        Some(""),
        Some(" "),
        Some("NULL"),
        Some("none"),
        Some("n-a"),
        Some("nan"),
        Some("un"),
        Some("unknown"),
        Some("unspecified"),
        Some("xx"),
        Some("z.z"),
    ] {
        let mut fields = base;
        fields[3] = value;
        assert!(
            !prepare_address_group_fields(fields).keyable,
            "state {value:?}"
        );
    }
    for value in [
        None,
        Some(""),
        Some("ZIP"),
        Some("00000"),
        Some("99999"),
        Some("00000-1234"),
        Some("99999-1234"),
    ] {
        let mut fields = base;
        fields[4] = value;
        assert!(
            !prepare_address_group_fields(fields).keyable,
            "ZIP {value:?}"
        );
    }
}

#[test]
fn keyable_predicate_preserves_raw_punctuation_whitespace_and_country_rules() {
    let base = [Some("!"), None, None, Some("!?"), Some("1"), None];
    for country in [
        None,
        Some(""),
        Some(" "),
        Some("123"),
        Some("!?"),
        Some("u.s."),
        Some("U_s_A"),
        Some("United States"),
        Some("United States of America"),
    ] {
        let mut fields = base;
        fields[5] = country;
        assert!(
            prepare_address_group_fields(fields).keyable,
            "country {country:?}"
        );
    }
    for country in [Some("GB"), Some("NULL"), Some("unknown"), Some("USX")] {
        let mut fields = base;
        fields[5] = country;
        assert!(
            !prepare_address_group_fields(fields).keyable,
            "country {country:?}"
        );
    }
    for text in ["\t", "\u{a0}", "_", "UN", "XX", "0"] {
        let mut fields = base;
        fields[0] = Some(text);
        fields[3] = Some(text);
        assert_eq!(
            prepare_address_group_fields(fields).keyable,
            !matches!(text, "UN" | "XX"),
            "{text:?}"
        );
    }
    for postal in ["0000", "00001", "9999", "123456", "00-001"] {
        let mut fields = base;
        fields[4] = Some(postal);
        assert!(
            prepare_address_group_fields(fields).keyable,
            "ZIP {postal:?}"
        );
    }
    assert!(
        !prepare_address_group_fields([
            None,
            Some("1 Main Street"),
            None,
            Some("NY"),
            Some("10001"),
            None
        ])
        .keyable
    );
}

fn input(rows: &[(i64, [Option<&str>; 13])]) -> Vec<u8> {
    let mut output = COPY_HEADER.to_vec();
    for (ordinal, fields) in rows {
        output.extend_from_slice(&INPUT_WIDTH.to_be_bytes());
        append_field(&mut output, Some(&ordinal.to_be_bytes())).unwrap();
        for field in fields {
            append_field(&mut output, field.map(str::as_bytes)).unwrap();
        }
    }
    output.extend_from_slice(&(-1i16).to_be_bytes());
    output
}

#[test]
fn preserves_trim_null_empty_and_checksum_normalization_contracts() {
    let fields = [
        Some("  Example\t "),
        Some(" "),
        Some(" "),
        Some("v1"),
        Some(" 12 Main St. "),
        None,
        Some(" New York "),
        Some(" ny "),
        Some("10001-1234"),
        Some(" "),
        Some(" None "),
        Some("312.555.0102 x009"),
        Some("\t"),
    ];
    let result = prepare_fields(fields);
    assert!(result.case_safe && result.contact_safe);
    assert_eq!(
        result.text[..13],
        [
            Some("Example\t"),
            None,
            Some("primary"),
            Some("v1"),
            Some("12 Main St."),
            None,
            Some("New York"),
            Some("ny"),
            Some("10001-1234"),
            Some("US"),
            None,
            Some("312.555.0102 x009"),
            Some("\t"),
        ]
        .map(|value| value.map(str::to_owned))
    );
    assert_eq!(
        result.text[13..],
        [
            None,
            None,
            Some("3125550102"),
            Some("009"),
            Some("12mainst"),
            None,
            Some("newyork"),
            Some("NY"),
            Some("10001"),
            Some("US"),
            None,
        ]
        .map(|value| value.map(str::to_owned))
    );
}

#[test]
fn matches_sql_extension_rules_including_invalid_number_and_strict_suffix() {
    for (raw, number, extension) in [
        ("x42", None, Some("42")),
        ("312-555-0102ext.007", Some("3125550102"), Some("007")),
        ("312-555-0102 ext 0-07", None, None),
        ("312-555-0102 extension 12345678901234567", None, None),
        ("312-555-0102 ;EXT=05\t", Some("3125550102"), Some("05")),
        ("312-555-0102 x 001\r\n", Some("3125550102"), Some("001")),
        (
            "312-555-0102\x0bx\x0b007\x0b",
            Some("3125550102"),
            Some("007"),
        ),
        ("1x2", None, Some("2")),
    ] {
        let result = prepare_contact(Some(raw), None);
        assert_eq!(
            result,
            (number.map(str::to_owned), extension.map(str::to_owned)),
            "{raw}"
        );
    }
}

#[test]
fn preserves_sql_country_prefix_and_raw_phone_checksum_distinctions() {
    for (raw, country, expected) in [
        ("+1 312 555 0102", "U.S.A.", Some("3125550102")),
        ("3125550102", "UNITED STATES OF AMERICA", Some("3125550102")),
        ("+44 20 7946 0958", "GB", Some("442079460958")),
        ("\t+44 20 7946 0958", "GB", None),
        ("+44 20 7946 0958", "GB", Some("442079460958")),
        ("442079460958", "GB", None),
    ] {
        assert_eq!(
            prepare_contact(Some(raw), Some(country)).0.as_deref(),
            expected
        );
    }
    let mut row = [None; 13];
    row[10] = Some("3125550102 x007");
    let result = prepare_fields(row);
    assert_eq!(result.text[13].as_deref(), Some("3125550102"));
    assert_eq!(result.text[23].as_deref(), Some("3125550102007"));
}

#[test]
fn retains_unicode_text_and_marks_only_affected_components_for_sql() {
    let mut fields = [None; 13];
    fields[0] = Some(" Éxample ");
    fields[4] = Some(" K Street ");
    fields[10] = Some("3125550102 x7");
    let result = prepare_fields(fields);
    assert!(!result.case_safe && result.contact_safe);
    assert_eq!(result.text[0].as_deref(), Some("Éxample"));
    assert_eq!(result.text[4].as_deref(), Some("K Street"));
    assert_eq!(result.text[17], None);
    assert_eq!(result.text[13].as_deref(), Some("3125550102"));
    fields[4] = Some("Main Street");
    fields[10] = Some("3125550102\u{2003}x7");
    let result = prepare_fields(fields);
    assert!(result.case_safe && !result.contact_safe);
    assert_eq!(result.text[13..17], [None, None, None, None]);
}

#[test]
fn emits_exact_binary_copy_types_and_preserves_signed_ordinal_bytes() {
    let rows = [(1, [None; 13]), (i64::MAX, [Some("\t\\N\n"); 13])];
    let prepared = prepare_entity_address_rows(&input(&rows)).unwrap();
    assert_eq!(prepared.row_count, 2);
    let mut reader = CopyReader {
        input: &prepared.copy_bytes,
        offset: COPY_HEADER.len(),
    };
    for ordinal in [1i64, i64::MAX] {
        assert_eq!(reader.take(2).unwrap(), &OUTPUT_WIDTH.to_be_bytes());
        assert_eq!(reader.field().unwrap().unwrap(), ordinal.to_be_bytes());
        for _ in 0..24 {
            reader.field().unwrap();
        }
        assert_eq!(reader.field().unwrap(), Some([1u8].as_slice()));
        assert_eq!(reader.field().unwrap(), Some([1u8].as_slice()));
    }
    assert_eq!(reader.take(2).unwrap(), &(-1i16).to_be_bytes());
    assert_eq!(reader.offset, prepared.copy_bytes.len());
}

#[test]
fn rejects_bad_framing_limits_and_duplicate_ordinals_atomically() {
    let valid = input(&[(1, [None; 13])]);
    for length in 0..valid.len() {
        assert!(prepare_entity_address_rows(&valid[..length]).is_err());
    }
    let mut trailing = valid.clone();
    trailing.push(0);
    assert!(prepare_entity_address_rows(&trailing).is_err());
    let mut bad_header = valid.clone();
    bad_header[0] ^= 1;
    assert!(prepare_entity_address_rows(&bad_header).is_err());
    for ordinals in [[1, 1], [2, 1], [0, 1], [-1, 1]] {
        assert!(prepare_entity_address_rows(&input(&ordinals.map(|id| (id, [None; 13])))).is_err());
    }
    let oversized = vec![0; MAX_INPUT_BYTES + 1];
    assert!(prepare_entity_address_rows(&oversized).is_err());
    let too_many = vec![(1, [None; 13]); MAX_ROWS + 1];
    let mut increasing = too_many;
    for (index, row) in increasing.iter_mut().enumerate() {
        row.0 = index as i64 + 1;
    }
    assert!(prepare_entity_address_rows(&input(&increasing)).is_err());
    let mut invalid_utf8 = valid;
    invalid_utf8.splice(33..37, [0, 0, 0, 1, 0xff]);
    assert!(prepare_entity_address_rows(&invalid_utf8).is_err());
}
