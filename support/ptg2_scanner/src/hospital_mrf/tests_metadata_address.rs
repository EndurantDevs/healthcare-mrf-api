fn metadata_address_fixture(format: InputFormat, addresses: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let original = match format {
        InputFormat::TallCsv => fixture_tall_csv(),
        InputFormat::WideCsv => fixture_wide_csv(),
        _ => unreachable!(),
    };
    let mut records = csv_fixture_records(&original);
    records[1][0] = "Synthetic Hospital".to_owned();
    records[1][3] = "Main Campus|North Campus".to_owned();
    records[1][4] = addresses.join("|");
    records[1][9].clear();
    records[1][10].clear();
    let canonical = csv_fixture_bytes(&records);
    let quoted = |value: &str| format!("\"{}\"", value.replace('"', "\"\""));
    let address = quoted(&addresses.join("|"));
    let components = addresses
        .iter()
        .map(|value| quoted(value))
        .collect::<Vec<_>>()
        .join("|");
    let text = String::from_utf8(canonical.clone()).unwrap();
    assert_eq!(text.matches(&address).count(), 1);
    (
        canonical,
        text.replacen(&address, &components, 1).into_bytes(),
    )
}

fn metadata_address_replay(payload: &[u8]) -> Vec<u8> {
    let mut result = Vec::new();
    csv_metadata_address_reader(Cursor::new(payload), DEFAULT_MAX_FANOUT_ROWS)
        .unwrap()
        .read_to_end(&mut result)
        .unwrap();
    result
}

#[test]
fn metadata_address_components_preserve_contents_order_and_false() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        for addresses in [
            ["1 Main Street, Test City", "2 North Street, Test City"],
            [
                "1 \"Main\" Street, Test City",
                "2 \"North\" Street, Test City",
            ],
        ] {
            let (canonical, malformed) = metadata_address_fixture(format, &addresses);
            for confirmation in ["TRUE", "FALSE"] {
                let canonical = String::from_utf8(canonical.clone())
                    .unwrap()
                    .replace(",TRUE,", &format!(",{confirmation},"))
                    .into_bytes();
                let malformed = String::from_utf8(malformed.clone())
                    .unwrap()
                    .replace(",TRUE,", &format!(",{confirmation},"))
                    .into_bytes();
                assert_eq!(metadata_address_replay(&malformed), canonical);
                assert_eq!(
                    run_fixture(format, &malformed, false),
                    run_fixture(format, &canonical, false)
                );
                let (_, left) = import_packed(format, &malformed, TEST_MAX_OUTPUT_BYTES);
                let (_, right) = import_packed(format, &canonical, TEST_MAX_OUTPUT_BYTES);
                assert_eq!(
                    left.artifacts
                        .iter()
                        .map(|item| (item.kind, item.rows, item.bytes, &item.sha256))
                        .collect::<Vec<_>>(),
                    right
                        .artifacts
                        .iter()
                        .map(|item| (item.kind, item.rows, item.bytes, &item.sha256))
                        .collect::<Vec<_>>(),
                );
                assert_eq!(
                    serde_json::to_value(&left.root).unwrap(),
                    serde_json::to_value(&right.root).unwrap()
                );
            }
        }
    }
}

#[test]
fn metadata_address_ordinary_multiline_preamble_and_overcap_are_unchanged() {
    let (canonical, _) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, Test City", "2 North St, Test City"],
    );
    for mut payload in [canonical, fixture_tall_csv(), fixture_wide_csv()] {
        assert_eq!(metadata_address_replay(&payload), payload);
        payload.splice(..0, b"A preamble note\r\n\r\n".iter().copied());
        assert_eq!(metadata_address_replay(&payload), payload);
        let mut oversized_preamble = vec![b'x'; CSV_METADATA_ADDRESS_PREFIX_BYTES as usize + 1];
        oversized_preamble.push(b'\n');
        oversized_preamble.extend_from_slice(&payload);
        assert_eq!(
            metadata_address_replay(&oversized_preamble),
            oversized_preamble
        );
    }
    let mut records = csv_fixture_records(&fixture_tall_csv());
    records[1][4] = "1 Main Street,\nSuite 2|2 North Street,\nSuite 3".to_owned();
    records[1][9] = "A quoted policy\nwith a second line".to_owned();
    let multiline_metadata = csv_fixture_bytes(&records);
    assert_eq!(
        metadata_address_replay(&multiline_metadata),
        multiline_metadata
    );
    assert!(!run_fixture(InputFormat::TallCsv, &multiline_metadata, false)["mrf"].is_empty());
    records[1][9] = "p".repeat(CSV_METADATA_ADDRESS_PREFIX_BYTES as usize + 1);
    let oversized_metadata = csv_fixture_bytes(&records);
    assert_eq!(
        metadata_address_replay(&oversized_metadata),
        oversized_metadata
    );
    assert!(!run_fixture(InputFormat::TallCsv, &oversized_metadata, false)["mrf"].is_empty());
}

#[test]
fn metadata_address_bom_and_invalid_metadata_preserve_validation() {
    let (canonical, malformed) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, City", "2 North St, City"],
    );
    let mut bom = b"\xef\xbb\xbf".to_vec();
    bom.extend_from_slice(&malformed);
    assert_eq!(
        run_fixture(InputFormat::TallCsv, &bom, false),
        run_fixture(InputFormat::TallCsv, &canonical, false)
    );
    let raw = String::from_utf8(malformed).unwrap();
    for (from, to, error) in [
        (
            ",TRUE,",
            ",INVALID,",
            "attestation value must be true or false",
        ),
        (
            "2026-04-01",
            "2026-02-30",
            "last_updated_on contains an invalid day",
        ),
    ] {
        let invalid = raw.replacen(from, to, 1).into_bytes();
        assert_ne!(metadata_address_replay(&invalid), invalid);
        assert_import_error(
            InputFormat::TallCsv,
            &invalid,
            DEFAULT_MAX_FANOUT_ROWS,
            error,
        );
    }
}

#[test]
fn metadata_address_old_success_with_address_last_is_preserved() {
    let (canonical, _) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, City", "2 North St, City"],
    );
    let mut records = csv_fixture_records(&canonical);
    records[1][0] = "Synthetic, Hospital".to_owned();
    for row in &mut records[..2] {
        row.truncate(11);
        let address = row.remove(4);
        row.push(address);
    }
    let mut writer = csv::WriterBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_writer(Vec::new());
    for record in records {
        writer.write_record(record).unwrap();
    }
    let raw = String::from_utf8(writer.into_inner().unwrap())
        .unwrap()
        .replace(
            "\"1 Main St, City|2 North St, City\"",
            "\"1 Main St, City\"|\"2 North St, City\"",
        )
        .into_bytes();
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(raw.as_slice());
    let mut rows = reader.records();
    let headers = rows.next().unwrap().unwrap();
    let values = rows.next().unwrap().unwrap();
    assert!(values.len() > headers.len());
    assert!(align_csv_hospital_name(&headers, &values).is_none());
    parse_csv_metadata(&headers, &values, DEFAULT_MAX_FANOUT_ROWS)
        .unwrap()
        .0
        .validate(true)
        .unwrap();
    assert_eq!(metadata_address_replay(&raw), raw);
    let legacy = run_fixture(InputFormat::TallCsv, &raw, false);
    assert!(std::str::from_utf8(&legacy["mrf"])
        .unwrap()
        .contains("\tSynthetic, Hospital\t"));
}

#[test]
fn metadata_address_rejects_nonmatching_raw_grammar() {
    let cases: &[(&[u8], usize, usize)] = &[
        (b"x,\"a, b\"|\"c, d\",y\n", 3, 0),   // Wrong field.
        (b"x,\"a, b\"|\"c, d\",y,z\n", 3, 1), // Extra field.
        (b"x,\"a, b\"|\"c, d\"\n", 3, 1),     // Missing field.
        (b"x,\"a, b\"|c, d,y\n", 3, 1),       // Unquoted second component.
        (b"x,\"a, b\"|\"c, d,y\n", 3, 1),     // Unclosed second component.
        (b"x,\"a, b\"|\"c, d\"x,y\n", 3, 1),  // Trailing garbage.
        (b"x,\"a|b\"|\"c, d\",y\n", 3, 1),    // Ambiguous inner pipe.
        (b"x,\"\"|\"c, d\",y\n", 3, 1),       // Empty component.
        (b"x,\"a,\nb\"|\"c, d\",y\n", 3, 1),  // Multiline compatibility is out of scope.
        (b"x,\"a, b\"|\"c, d\",y", 3, 1),     // Incomplete bounded record.
    ];
    for (record, fields, address) in cases {
        assert!(
            csv_metadata_address_record(record, *fields, *address).is_none(),
            "{record:?}"
        );
    }
    let (_, malformed) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, City", "2 North St, City"],
    );
    let duplicate = String::from_utf8(malformed)
        .unwrap()
        .replacen("financial_aid_policy", "hospital_address", 1)
        .into_bytes();
    assert_eq!(metadata_address_replay(&duplicate), duplicate);
    assert_import_error(
        InputFormat::TallCsv,
        &duplicate,
        DEFAULT_MAX_FANOUT_ROWS,
        "duplicate general CSV header",
    );
}

#[test]
fn metadata_address_preserves_read_and_resource_limits() {
    struct OneByte<R>(R);
    impl<R: Read> Read for OneByte<R> {
        fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
            let count = buffer.len().min(1);
            self.0.read(&mut buffer[..count])
        }
    }
    let (canonical, malformed) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, City", "2 North St, City"],
    );
    let mut reader =
        csv_metadata_address_reader(OneByte(Cursor::new(&malformed)), DEFAULT_MAX_FANOUT_ROWS)
            .unwrap();
    assert_eq!(reader.read(&mut []).unwrap(), 0);
    let mut actual = Vec::new();
    reader.read_to_end(&mut actual).unwrap();
    assert_eq!(actual, canonical);
    assert_payload_limit_error(
        InputFormat::TallCsv,
        &malformed,
        10,
        "CSV record exceeds configured limit",
    );
    assert_import_error(
        InputFormat::TallCsv,
        &malformed,
        1,
        "fanout exceeds configured limit",
    );
    let mut bounded = csv_metadata_address_reader(
        BoundedDecompressedReader::new(Cursor::new(&malformed), malformed.len() as u64),
        DEFAULT_MAX_FANOUT_ROWS,
    )
    .unwrap();
    let mut decoded = Vec::new();
    bounded.read_to_end(&mut decoded).unwrap();
    assert_eq!(decoded, canonical);
    let error = csv_metadata_address_reader(
        BoundedDecompressedReader::new(Cursor::new(&malformed), malformed.len() as u64 - 1),
        DEFAULT_MAX_FANOUT_ROWS,
    )
    .err()
    .unwrap();
    assert!(error
        .to_string()
        .contains("decompressed data exceeds configured limit"));
}

#[test]
fn metadata_address_stream_offsets_preserve_crlf_eof_and_body_suffix() {
    let (canonical, malformed) = metadata_address_fixture(
        InputFormat::TallCsv,
        &["1 Main St, City", "2 North St, City"],
    );
    let canonical = String::from_utf8(canonical).unwrap().replace('\n', "\r\n");
    let malformed = String::from_utf8(malformed).unwrap().replace('\n', "\r\n");
    for ending in ["", "\r\n"] {
        let expected = format!("{}{ending}", canonical.trim_end_matches(['\r', '\n']));
        let source = format!("{}{ending}", malformed.trim_end_matches(['\r', '\n']));
        assert_eq!(
            metadata_address_replay(source.as_bytes()),
            expected.as_bytes()
        );
    }
    let suffix = "\"untouched,body\nrecord\",x\r\n".repeat(2048);
    let expected = format!("{canonical}{suffix}");
    let source = format!("{malformed}{suffix}");
    assert!(source.len() > CSV_METADATA_ADDRESS_PREFIX_BYTES as usize);
    assert_eq!(
        metadata_address_replay(source.as_bytes()),
        expected.as_bytes()
    );
    let truncated_detection = malformed.replacen(
        "Synthetic Hospital",
        &"H".repeat(CSV_METADATA_ADDRESS_PREFIX_BYTES as usize),
        1,
    );
    assert_eq!(
        metadata_address_replay(truncated_detection.as_bytes()),
        truncated_detection.as_bytes()
    );
    let no_data = malformed.lines().take(2).collect::<Vec<_>>().join("\r\n");
    assert_eq!(
        metadata_address_replay(no_data.as_bytes()),
        no_data.as_bytes()
    );
}

#[test]
fn metadata_repairs_do_not_cascade_across_two_malformed_fields() {
    for format in [InputFormat::TallCsv, InputFormat::WideCsv] {
        let (_, malformed_address) =
            metadata_address_fixture(format, &["1 Main St, City", "2 North St, City"]);
        let malformed_both = String::from_utf8(malformed_address)
            .unwrap()
            .replacen("Synthetic Hospital", "Synthetic, Hospital", 1)
            .into_bytes();
        assert_eq!(metadata_address_replay(&malformed_both), malformed_both);
        assert_import_error(
            format,
            &malformed_both,
            DEFAULT_MAX_FANOUT_ROWS,
            "unsupported CMS CSV version",
        );
    }
}
