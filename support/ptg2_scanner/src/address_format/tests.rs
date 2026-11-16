use super::*;

#[test]
fn entity_fields_use_original_inputs_before_shared_space_normalization() {
    let fields = [
        Some("100 WEST\u{a0}STREET"),
        Some(" _Suite_ 2 "),
        Some(" Example\u{a0}City "),
        Some(" \u{a0}ny\u{a0} "),
        Some("123"),
        Some(" \u{a0}us\u{a0} "),
    ];
    let prepared = prepare_address(fields).unwrap();
    assert_eq!(
        prepared.entity,
        crate::entity_address_rows::prepare_address_group_fields(fields)
    );
    assert_eq!(prepared.entity.street_key, "100wst");
    assert_eq!(
        prepared.entity.state_code.as_deref(),
        Some("\u{a0}NY\u{a0}")
    );
    assert_eq!(
        prepared.entity.country_norm.as_deref(),
        Some("\u{a0}US\u{a0}")
    );
    assert_eq!(prepared.entity.zip5.as_deref(), Some("123"));
    assert_eq!(prepared.canonical.state_code.as_deref(), Some("NY"));
    assert_eq!(prepared.canonical.country_code, "US");
    assert_eq!(prepared.canonical.zip5.as_deref(), Some("00123"));
}

#[test]
fn matches_portable_existing_sql_boundary_corpus() {
    let cases: serde_json::Value = serde_json::from_str(include_str!("golden.json")).unwrap();
    for case in cases.as_array().unwrap() {
        let inputs = case["input"].as_array().unwrap();
        let fields = std::array::from_fn(|index| inputs[index].as_str());
        let prepared = prepare_address(fields).unwrap();
        assert_eq!(
            prepared.formatted_address.as_deref(),
            case["formatted_address"].as_str(),
            "{fields:?}"
        );
        let [first, second, city, state, postal, country] = fields;
        assert_eq!(
            prepared.canonical,
            canonicalize_address(first, second, city, state, postal, country)
        );
    }
}

#[test]
fn preserves_null_and_portable_input_boundaries() {
    assert!(prepare_address([None; 6])
        .unwrap()
        .formatted_address
        .is_none());
    for unsupported in [
        "Caf\u{e9}",
        "\u{212a} Street",
        "\u{130}",
        "Caf\u{e9}\u{a0}Road",
        "Main\u{85}St",
        "Main\u{1680}St",
        "Main\u{2008}St",
        "Main\u{2028}St",
        "Main\u{2029}St",
        "Main\u{205f}St",
        "Main\u{3000}St",
        "Main\0St",
        "Main\u{7f}St",
    ] {
        for index in 0..6 {
            let mut fields = [None; 6];
            fields[index] = Some(unsupported);
            assert!(
                prepare_address(fields).is_none(),
                "{unsupported:?} at {index}"
            );
        }
    }
}

#[test]
fn renders_structured_address_golden_cases() {
    let cases = [
        (
            [
                Some("3800 S WHITNEY AVE"),
                Some("SUITE 301"),
                Some("INDEPENDENCE"),
                Some("MO"),
                Some("64055"),
                None,
            ],
            "3800 South Whitney Avenue, Suite 301, Independence, MO 64055",
        ),
        (
            [
                Some("4007 Clarksville Pike Suite 301"),
                Some("Ste 301"),
                Some("NASHVILLE"),
                Some("TN"),
                Some("37218"),
                Some("United States"),
            ],
            "4007 Clarksville Pike, Suite 301, Nashville, TN 37218",
        ),
        (
            [
                Some("123 NE 1ST ST NW"),
                None,
                Some("O'FALLON"),
                Some("MO"),
                Some("63366"),
                Some("USA"),
            ],
            "123 NE 1st Street NW, O'Fallon, MO 63366",
        ),
        (
            [
                Some("P.O. BOX #42"),
                None,
                Some("ST LOUIS"),
                Some("MO"),
                Some("63101"),
                Some("U.S."),
            ],
            "PO Box 42, St Louis, MO 63101",
        ),
        (
            [
                Some("100 MAIN ST STE MCDONALD"),
                None,
                Some("CITY"),
                Some("MO"),
                Some("64055"),
                Some("US"),
            ],
            "100 Main Street, Suite Mcdonald, City, MO 64055",
        ),
        (
            [
                Some("100 MAIN ST STE McDonald"),
                None,
                Some("CITY"),
                Some("MO"),
                Some("64055"),
                Some("US"),
            ],
            "100 Main Street, Suite McDonald, City, MO 64055",
        ),
        (
            [
                Some("100 MAIN ST"),
                None,
                Some("FT. WORTH,"),
                Some("TX,"),
                Some("76102,"),
                Some("US,"),
            ],
            "100 Main Street, Fort Worth, TX 76102",
        ),
        (
            [
                Some("STRASSE STE-301"),
                None,
                Some("BERLIN"),
                None,
                Some("10115"),
                Some("DE"),
            ],
            "Strasse Ste-301, Berlin, 10115, DE",
        ),
        (
            [
                Some(" \t100 Main\r\nStreet "),
                Some(" Suite   200 "),
                Some(" Springfield "),
                Some(" IL "),
                Some("627041234"),
                Some(" us "),
            ],
            "100 Main Street, Suite 200, Springfield, IL 62704-1234",
        ),
    ];
    for (fields, expected) in cases {
        assert_eq!(
            prepare_address(fields)
                .unwrap()
                .formatted_address
                .as_deref(),
            Some(expected),
            "{fields:?}"
        );
    }
}

#[test]
fn unit_and_direction_boundaries_keep_street_words() {
    for (first, second, expected) in [
        ("123 UNIT RD", None, "123 Unit Road, City, MO 64055"),
        ("1 BUILDING WAY", None, "1 Building Way, City, MO 64055"),
        (
            "100 MAIN ST BLDG A STE 2",
            None,
            "100 Main Street, Building A, Suite 2, City, MO 64055",
        ),
        (
            "100 MAIN ST STE-301",
            Some("STE 301"),
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 MAIN ST STE 301",
            Some("301"),
            "100 Main Street, Suite 301, City, MO 64055",
        ),
        (
            "100 N.E. MAIN ST",
            None,
            "100 NE Main Street, City, MO 64055",
        ),
        (
            "100 MAIN ST 2ND FLOOR",
            Some("FLOOR 2"),
            "100 Main Street, Floor 2, City, MO 64055",
        ),
    ] {
        let fields = [
            Some(first),
            second,
            Some("CITY"),
            Some("MO"),
            Some("64055"),
            Some("US"),
        ];
        assert_eq!(
            prepare_address(fields)
                .unwrap()
                .formatted_address
                .as_deref(),
            Some(expected),
            "{fields:?}"
        );
    }
}

#[test]
fn bounded_labels_keep_sql_null_semantics() {
    let long = "A".repeat(1025);
    let prepared = prepare_address([Some(&long), None, None, None, None, None]).unwrap();
    assert_eq!(prepared.formatted_address.unwrap().len(), MAX_LENGTH);
    assert_eq!(
        prepare_address([None, None, None, None, None, Some("GB")])
            .unwrap()
            .formatted_address
            .as_deref(),
        Some("GB")
    );
}

#[test]
fn every_ascii_space_collapses_before_rendering() {
    let fields = [
        Some("\t100\n\rMain\u{b}Street\u{c}"),
        Some("Suite\t\t2"),
        Some("CITY"),
        Some("MO"),
        Some("64055"),
        Some("US"),
    ];
    assert_eq!(
        prepare_address(fields)
            .unwrap()
            .formatted_address
            .as_deref(),
        Some("100 Main Street, Suite 2, City, MO 64055")
    );
}

#[test]
fn unmapped_suffix_tokens_preserve_following_periods() {
    for (first, second, expected) in [
        ("100 NE 25TH.1", None, "100 NE 25th.1, Example, NY 10001"),
        (
            "100 MAIN RD",
            Some("3 SW STE. 4"),
            "100 Main Road, 3 SW Ste. 4, Example, NY 10001",
        ),
    ] {
        let fields = [
            Some(first),
            second,
            Some("EXAMPLE"),
            Some("NY"),
            Some("10001"),
            Some("US"),
        ];
        assert_eq!(
            prepare_address(fields)
                .unwrap()
                .formatted_address
                .as_deref(),
            Some(expected)
        );
    }
}

#[test]
fn normalizes_shared_spaces_before_all_derived_fields() {
    let base = [
        "100 Main Street Suite 200 A",
        "200 A",
        "Example City",
        "NY",
        "10001 1234",
        "US",
    ];
    let expected = prepare_address(base.map(Some)).unwrap();
    for space in [
        '\t', '\n', '\u{b}', '\u{c}', '\r', ' ', '\u{a0}', '\u{2000}', '\u{2001}', '\u{2002}',
        '\u{2003}', '\u{2004}', '\u{2005}', '\u{2006}', '\u{2007}', '\u{2009}', '\u{200a}',
        '\u{202f}',
    ] {
        for index in 0..6 {
            let mut input = base.map(str::to_owned);
            input[index] = format!(
                " {space}{space}{} {space}{space} ",
                base[index].replace(' ', &format!(" {space}  "))
            );
            let actual =
                prepare_address(input.each_ref().map(|value| Some(value.as_str()))).unwrap();
            assert_eq!(
                actual.canonical,
                expected.canonical,
                "U+{:04X} in field {index}",
                u32::from(space)
            );
            assert_eq!(
                actual.formatted_address,
                expected.formatted_address,
                "U+{:04X} in field {index}",
                u32::from(space)
            );
        }
    }
}

#[test]
fn punctuation_before_tail_units_preserves_sql_v2_labels() {
    // SQL v2 also slices the original prefix using its punctuation-trimmed length.
    for (line, expected) in [
        (
            ",100 Main St Ste 5",
            "100 Main South, Suite 5, Example, TX 75001",
        ),
        (
            ";100 Main St Ste 5",
            "100 Main South, Suite 5, Example, TX 75001",
        ),
        (
            ":100 Main St Ste 5",
            "100 Main South, Suite 5, Example, TX 75001",
        ),
        (
            ",;:100 Main St Ste 5",
            "100 Main, Suite 5, Example, TX 75001",
        ),
        (
            ", 100 Main St, Ste 5",
            "100 Main, Suite 5, Example, TX 75001",
        ),
        (
            "100 Main St Ste 5",
            "100 Main Street, Suite 5, Example, TX 75001",
        ),
        (
            ",100 Main St Suite 5",
            ",100 Main St Suite 5, Example, TX 75001",
        ),
    ] {
        let prepared = prepare_address([
            Some(line),
            None,
            Some("Example"),
            Some("TX"),
            Some("75001"),
            Some("US"),
        ])
        .unwrap();
        assert_eq!(
            prepared.formatted_address.as_deref(),
            Some(expected),
            "{line}"
        );
    }
}
