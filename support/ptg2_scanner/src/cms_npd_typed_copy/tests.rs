// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

mod references;
mod resources;

use super::pg::{
    parse_date, parse_npi, parse_timestamptz, parse_uuid, Row, COPY_HEADER, COPY_TRAILER,
};
use super::{encoder_for, run_cli, stream, Summary};
use std::io::Cursor;

pub(super) type Field = Option<Vec<u8>>;

/// Test-only binary COPY reader: tuples of raw fields from a row stream without header.
pub(super) fn read_rows(bytes: &[u8]) -> Vec<Vec<Field>> {
    let mut at = 0;
    let mut rows = Vec::new();
    while at < bytes.len() {
        let mut take = |count: usize| {
            at += count;
            &bytes[at - count..at]
        };
        let field_count = i16::from_be_bytes(take(2).try_into().unwrap());
        let row = (0..field_count)
            .map(|_| {
                let length = i32::from_be_bytes(take(4).try_into().unwrap());
                (length >= 0).then(|| take(length as usize).to_vec())
            })
            .collect();
        rows.push(row);
    }
    rows
}

pub(super) fn text(field: &Field) -> Option<&str> {
    field
        .as_deref()
        .map(|bytes| std::str::from_utf8(bytes).unwrap())
}

pub(super) fn int8(field: &Field) -> Option<i64> {
    field
        .as_deref()
        .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
}

pub(super) fn int4(field: &Field) -> Option<i32> {
    field
        .as_deref()
        .map(|bytes| i32::from_be_bytes(bytes.try_into().unwrap()))
}

pub(super) fn boolean(field: &Field) -> Option<bool> {
    field.as_deref().map(|bytes| match bytes {
        [0] => false,
        [1] => true,
        other => panic!("bad bool {other:?}"),
    })
}

pub(super) fn float8(field: &Field) -> Option<f64> {
    field
        .as_deref()
        .map(|bytes| f64::from_bits(u64::from_be_bytes(bytes.try_into().unwrap())))
}

pub(super) fn uuid(field: &Field) -> Option<[u8; 16]> {
    field.as_deref().map(|bytes| bytes.try_into().unwrap())
}

pub(super) fn jsonb(field: &Field) -> Option<serde_json::Value> {
    field.as_deref().map(|bytes| {
        assert_eq!(bytes[0], 1, "jsonb version byte");
        serde_json::from_slice(&bytes[1..]).unwrap()
    })
}

/// Elements of a one-dimensional array without NULLs, checking its header and element oid.
pub(super) fn array(field: &Field, elem_oid: i32) -> Option<Vec<Vec<u8>>> {
    let bytes = field.as_deref()?;
    let word = |at: usize| i32::from_be_bytes(bytes[at..at + 4].try_into().unwrap());
    assert_eq!([word(0), word(4), word(8), word(16)], [1, 0, elem_oid, 1]);
    let mut at = 20;
    let elements = (0..word(12))
        .map(|_| {
            let length = word(at) as usize;
            at += 4 + length;
            bytes[at - length..at].to_vec()
        })
        .collect();
    assert_eq!(at, bytes.len());
    Some(elements)
}

pub(super) fn texts(field: &Field) -> Option<Vec<String>> {
    array(field, 25).map(|items| {
        items
            .into_iter()
            .map(|item| String::from_utf8(item).unwrap())
            .collect()
    })
}

pub(super) fn int8s(field: &Field) -> Option<Vec<i64>> {
    array(field, 20).map(|items| {
        items
            .iter()
            .map(|item| i64::from_be_bytes(item[..].try_into().unwrap()))
            .collect()
    })
}

pub(super) fn uuids(field: &Field) -> Option<Vec<[u8; 16]>> {
    array(field, 2950).map(|items| {
        items
            .iter()
            .map(|item| item[..].try_into().unwrap())
            .collect()
    })
}

pub(super) fn u(text: &str) -> [u8; 16] {
    parse_uuid(text).unwrap()
}

#[test]
fn cms_npd_typed_copy_date_epoch_math() {
    assert_eq!(parse_date("2000-01-01"), Ok(0));
    assert_eq!(parse_date("2000-02-29T00:00:00Z"), Ok(59));
    assert_eq!(parse_date("2024-02-29T00:00:00Z"), Ok(8825));
    assert_eq!(parse_date("1999-12-31T00:00:00Z"), Ok(-1));
    assert_eq!(parse_date("1987-06-15"), Ok(-4583));
    assert_eq!(parse_date("1970-01-01"), Ok(-10_957));
    for bad in [
        "2023-02-29",
        "1900-02-29",
        "2026-13-01",
        "2026-04-31",
        "2026-1-01",
        "2026-01-01Z",
        "x",
    ] {
        assert!(parse_date(bad).is_err(), "{bad}");
    }
}

#[test]
fn cms_npd_typed_copy_timestamptz_epoch_math() {
    assert_eq!(parse_timestamptz("2000-01-01T00:00:00Z"), Ok(0));
    assert_eq!(parse_timestamptz("2000-01-01T00:00:01.5Z"), Ok(1_500_000));
    assert_eq!(parse_timestamptz("1999-12-31T23:59:59.999999999Z"), Ok(-1));
    assert_eq!(
        parse_timestamptz("2000-02-29T12:00:00Z"),
        Ok((59 * 86_400 + 43_200) * 1_000_000)
    );
    assert_eq!(
        parse_timestamptz("2026-09-24T15:38:16.229996Z"),
        Ok(843_579_496_229_996)
    );
    for bad in [
        "2026-09-24T15:38:16",
        "2026-09-24 15:38:16Z",
        "2026-09-24T24:00:00Z",
        "2026-09-24T15:38:60Z",
        "2026-09-24T15:38:16.Z",
        "2026-09-24T15:38:16.1234567890Z",
        "2026-09-24T15:38:16.12a4Z",
        "2026-09-24T15:38:16+00:00",
    ] {
        assert!(parse_timestamptz(bad).is_err(), "{bad}");
    }
}

#[test]
fn cms_npd_typed_copy_uuid_and_npi_parsing() {
    let expected: [u8; 16] = [
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee,
        0xff,
    ];
    assert_eq!(
        parse_uuid("00112233-4455-6677-8899-aabbccddeeff"),
        Ok(expected)
    );
    assert_eq!(
        parse_uuid("00112233-4455-6677-8899-AABBCCDDEEFF"),
        Ok(expected)
    );
    for bad in [
        "00112233445566778899aabbccddeeff",
        "00112233-4455-6677-8899-aabbccddeefg",
        "0011-2233",
    ] {
        assert!(parse_uuid(bad).is_err(), "{bad}");
    }
    assert_eq!(parse_npi("0123456789"), Ok(123_456_789));
    for bad in ["123456789", "12345678901", "12345678a0", "+123456789"] {
        assert!(parse_npi(bad).is_err(), "{bad}");
    }
}

#[test]
fn cms_npd_typed_copy_field_encodings_byte_for_byte() {
    let mut out = Vec::new();
    let mut row = Row::start(&mut out, 6);
    row.uuid(Some(u("00112233-4455-6677-8899-aabbccddeeff")));
    row.text_array(&["ab", "c"]).unwrap();
    row.int8_array(&[7]);
    row.uuid_array(&[]);
    row.text(None).unwrap();
    row.jsonb(Some(b"[]"));
    let mut expected = vec![0, 6, 0, 0, 0, 16];
    expected.extend(u("00112233-4455-6677-8899-aabbccddeeff"));
    expected.extend([
        0, 0, 0, 31, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 25, 0, 0, 0, 2, 0, 0, 0, 1,
    ]);
    expected.extend([0, 0, 0, 2, b'a', b'b', 0, 0, 0, 1, b'c']);
    expected.extend([
        0, 0, 0, 32, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 0, 1, 0, 0, 0, 1,
    ]);
    expected.extend([0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 7]);
    expected.extend([0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]);
    expected.extend([0, 0, 0, 3, 1, b'[', b']']);
    assert_eq!(out, expected);
    assert!(Row::start(&mut Vec::new(), 1).text(Some("a\0b")).is_err());
}

fn location_line(index: usize) -> String {
    format!(
        r#"{{"resourceType":"Location","id":"Location-00000000-0000-4000-8000-{index:012}","meta":{{"lastUpdated":"2026-01-01T00:00:00Z"}},"name":"Site {index}","description":"Padding text so that the synthetic input spans several encode batches"}}"#
    )
}

fn run_stream(input: String) -> std::io::Result<(Summary, Vec<u8>)> {
    let (encoder, _) = encoder_for("Location").unwrap();
    stream::copy_stream(encoder, Cursor::new(input.into_bytes()), Vec::new(), None)
        .map(|(summary, main, _)| (summary, main))
}

#[test]
fn cms_npd_typed_copy_stream_keeps_order_across_batches() {
    let lines: Vec<String> = (0..12_000).map(location_line).collect();
    let input = lines.join("\n");
    let input_bytes = input.len() as u64;
    let (summary, main) = run_stream(input).unwrap();
    assert_eq!(
        summary,
        Summary {
            rows: 12_000,
            address_rows: 0,
            input_bytes
        }
    );
    assert!(main.starts_with(COPY_HEADER) && main.ends_with(COPY_TRAILER));
    let rows = read_rows(&main[COPY_HEADER.len()..main.len() - COPY_TRAILER.len()]);
    assert_eq!(rows.len(), 12_000);
    for (index, row) in rows.iter().enumerate() {
        assert_eq!(text(&row[1]), Some(format!("Site {index}").as_str()));
    }
}

#[test]
fn cms_npd_typed_copy_stream_reports_global_line_numbers() {
    let mut lines: Vec<String> = (0..12_000).map(location_line).collect();
    lines[9_876] = "{not json".to_owned();
    lines[11_000] = "{also not json".to_owned();
    let error = run_stream(lines.join("\n") + "\n").unwrap_err();
    assert!(
        error.to_string().contains("line 9877: invalid record"),
        "{error}"
    );

    let error = run_stream(format!("{}\n\n{}\n", location_line(1), location_line(2))).unwrap_err();
    assert!(error.to_string().contains("line 2:"), "{error}");
}

#[test]
fn cms_npd_typed_copy_failed_stream_writes_no_trailer() {
    let (encoder, _) = encoder_for("Location").unwrap();
    let output = tempfile::NamedTempFile::new().unwrap();
    let input = format!("{}\nnot json\n", location_line(1));
    let result = stream::copy_stream(
        encoder,
        Cursor::new(input.into_bytes()),
        output.reopen().unwrap(),
        None,
    );
    assert!(result.is_err());
    let written = std::fs::read(output.path()).unwrap();
    assert!(written.starts_with(COPY_HEADER) && !written.ends_with(COPY_TRAILER));
}

#[test]
fn cms_npd_typed_copy_cli_argument_errors() {
    let run = |arguments: &[&str]| {
        let arguments: Vec<String> = arguments
            .iter()
            .map(|argument| argument.to_string())
            .collect();
        run_cli(&arguments).unwrap_err().to_string()
    };
    assert!(run(&["Organization", "/dev/null"]).contains("needs exactly one address output path"));
    assert!(run(&["Practitioner", "/dev/null"]).contains("needs exactly one address output path"));
    assert!(run(&["Location", "/dev/null", "/dev/null"]).contains("takes no address output path"));
    assert!(run(&["Patient", "/dev/null"]).contains("unsupported resource type"));
    assert!(run(&[]).contains("usage:"));
}

#[test]
fn cms_npd_typed_copy_strings_borrow_from_the_line() {
    let parsed: Option<super::fhir::Str> = serde_json::from_slice(br#""plain""#).unwrap();
    assert!(matches!(
        parsed.unwrap().0,
        std::borrow::Cow::Borrowed("plain")
    ));
    let parsed: Vec<super::fhir::Str> = serde_json::from_slice(br#"["a\"b"]"#).unwrap();
    assert_eq!(&*parsed[0], "a\"b");
}

const TS: &str = "2026-09-24T15:38:16.229996Z";
const U1: &str = "11111111-1111-4111-8111-111111111111";

#[test]
fn cms_npd_typed_copy_rejects_bad_records() {
    let meta = format!(r#""meta":{{"lastUpdated":"{TS}"}}"#);
    let cases = [
        ("Location", "{not json".to_owned(), "invalid record"),
        (
            "Location",
            format!(r#"{{"resourceType":"Endpoint","id":"Location-{U1}",{meta}}}"#),
            "is not Location",
        ),
        (
            "Location",
            format!(r#"{{"resourceType":"Location","id":"Location-not-a-uuid",{meta}}}"#),
            "invalid uuid",
        ),
        (
            "Location",
            format!(r#"{{"resourceType":"Location","id":"{U1}",{meta}}}"#),
            "invalid Location id",
        ),
        (
            "Location",
            format!(
                r#"{{"resourceType":"Location","id":"Location-{U1}","meta":{{"lastUpdated":"yesterday"}}}}"#
            ),
            "invalid timestamp",
        ),
        (
            "Practitioner",
            format!(r#"{{"resourceType":"Practitioner","id":"Practitioner-123",{meta}}}"#),
            "invalid NPI",
        ),
        (
            "Location",
            format!(
                r#"{{"resourceType":"Location","id":"Location-{U1}",{meta},"managingOrganization":{{"reference":"Practitioner/Practitioner-1234567890"}}}}"#
            ),
            "invalid Organization reference",
        ),
        (
            "PractitionerRole",
            format!(
                r#"{{"resourceType":"PractitionerRole","id":"PractitionerRole-{U1}",{meta},"location":[{{"reference":"Location/Location-1234567890"}}]}}"#
            ),
            "invalid uuid",
        ),
        (
            "InsurancePlan",
            format!(
                r#"{{"resourceType":"InsurancePlan","id":"InsurancePlan-{U1}",{meta},"period":{{"start":"2026-02-30T00:00:00Z"}}}}"#
            ),
            "invalid date",
        ),
    ];
    for (resource_type, line, reason) in cases {
        let error = encode_line(resource_type, &line)
            .err()
            .unwrap_or_else(|| panic!("accepted {line}"));
        assert!(error.contains(reason), "{error} does not contain {reason}");
    }
}

fn encode_line(resource_type: &str, line: &str) -> Result<super::Output, String> {
    let (encoder, _) = encoder_for(resource_type).unwrap();
    let mut output = super::Output::default();
    encoder(line.as_bytes(), &mut output).map(|()| output)
}
