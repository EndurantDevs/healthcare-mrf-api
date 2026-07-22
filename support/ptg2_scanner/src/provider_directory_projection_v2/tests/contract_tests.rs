// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::super::canonical::test_python_stable_json;
use super::super::contracts::{
    PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC, PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES,
};
use super::super::encode::decode_provider_directory_projection_copy_spool;
use super::super::fhir_values::test_microdegrees;
use super::super::pg_copy::test_copy_columns;
use super::super::stdio::test_parse_arguments;
use super::support::{rich_spool, wire_bytes, PARTITION_ID, RECIPE_ID};
use serde_json::json;

#[test]
fn v2_arguments_are_strict_and_locator_free() {
    let valid = [RECIPE_ID, PARTITION_ID, "7", "ndjson"].map(str::to_owned);
    let (context, _framing) = test_parse_arguments(&valid).unwrap();
    assert_eq!(context.partition_ordinal, 7);

    for invalid in [
        ["short", PARTITION_ID, "7", "ndjson"],
        [RECIPE_ID, "UPPER", "7", "ndjson"],
        [RECIPE_ID, PARTITION_ID, "-1", "ndjson"],
        [RECIPE_ID, PARTITION_ID, "2147483648", "ndjson"],
        [RECIPE_ID, PARTITION_ID, "7", "url"],
    ] {
        assert!(test_parse_arguments(&invalid.map(str::to_owned)).is_err());
    }
}

#[test]
fn decimal_microdegrees_use_exact_ties_to_even_without_exponent_allocation() {
    for (number, expected) in [
        ("0.0000005", 0),
        ("0.0000015", 2),
        ("-0.0000005", 0),
        ("-0.0000015", -2),
        ("4.16e1", 41_600_000),
        ("-9.36000015e1", -93_600_002),
        ("1e-999999999", 0),
        ("0e999999999", 0),
    ] {
        assert_eq!(test_microdegrees(number, 180).unwrap(), expected);
    }
    assert!(test_microdegrees("90.0000000000000000000001", 90).is_err());
    assert!(test_microdegrees("1e999999999", 180).is_err());
}

#[test]
fn semantic_hash_json_matches_python_ascii_escaping() {
    let value = json!({"bmp": "Ž", "pair": "😀", "space": "\u{2003}x\u{2003}"});
    assert_eq!(
        test_python_stable_json(&value),
        br#"{"bmp":"\u017d","pair":"\ud83d\ude00","space":"\u2003x\u2003"}"#
    );
}

#[test]
fn copy_contract_has_the_exact_stage_columns_and_one_owned_output_buffer() {
    assert_eq!(PROVIDER_DIRECTORY_PROJECTION_COPY_MAGIC.len(), 12);
    assert_eq!(test_copy_columns().len(), 18);
    assert_eq!(test_copy_columns()[0], "physical_projection_id");
    assert_eq!(test_copy_columns()[17], "profile_evidence_json");
    assert_eq!(
        PROVIDER_DIRECTORY_PROJECTION_COPY_MAX_OWNED_IO_BYTES,
        161 * 1024 * 1024 + 32,
    );

    let spool = rich_spool();
    assert!(spool.header_bytes.len() < 1024 * 1024 + 32);
    assert_eq!(spool.wire_byte_count(), wire_bytes(&spool).len());
}

#[test]
fn v2_spool_round_trips_and_fails_closed_on_hash_or_terminal_changes() {
    let spool = rich_spool();
    let wire = wire_bytes(&spool);
    let decoded = decode_provider_directory_projection_copy_spool(&wire).unwrap();
    assert_eq!(decoded.summary.resource_count, 8);
    assert_eq!(decoded.copy_bytes, spool.copy_bytes);

    let mut corrupt_copy = wire.clone();
    let copy_start = spool.header_bytes.len();
    corrupt_copy[copy_start + 20] ^= 1;
    assert!(decode_provider_directory_projection_copy_spool(&corrupt_copy).is_err());

    let mut bad_terminal = wire;
    *bad_terminal.last_mut().unwrap() = 0;
    assert!(decode_provider_directory_projection_copy_spool(&bad_terminal).is_err());
}
