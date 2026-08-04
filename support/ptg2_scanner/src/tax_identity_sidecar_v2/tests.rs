use super::*;
use crate::tax_identity::TaxIdentityStateV2;
use std::io::{self, Cursor, Read};

const POLICY_ID: &str = "ptg-tin-hmac-sha256-v1:test-2";

fn header() -> TaxIdentitySidecarV2Header {
    TaxIdentitySidecarV2Header::new(POLICY_ID.to_owned()).unwrap()
}

fn raw_header(policy_id: &[u8]) -> Vec<u8> {
    assert!(policy_id.len() <= usize::from(u8::MAX));
    let mut encoded = Vec::with_capacity(FIXED_HEADER_BYTES + policy_id.len());
    encoded.extend_from_slice(TAX_IDENTITY_SIDECAR_V2_MAGIC);
    encoded.extend_from_slice(&TAX_IDENTITY_SIDECAR_V2_FORMAT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&(TAX_IDENTITY_SIDECAR_V2_RECORD_BYTES as u16).to_le_bytes());
    encoded.push(policy_id.len() as u8);
    encoded.extend_from_slice(policy_id);
    encoded
}

fn matched_record(
    provider_group_global_id: ProviderGroupGlobalId,
    state: TaxIdentityStateV2,
    hmac_byte: u8,
) -> TaxIdentitySidecarV2Record {
    let tin_hmac_sha256 = [hmac_byte; 32];
    let mut tin_id_128 = [0u8; 16];
    tin_id_128.copy_from_slice(&tin_hmac_sha256[..16]);
    TaxIdentitySidecarV2Record::new(provider_group_global_id, state, tin_id_128, tin_hmac_sha256)
        .unwrap()
}

fn unavailable_record(
    provider_group_global_id: ProviderGroupGlobalId,
    state: TaxIdentityStateV2,
) -> TaxIdentitySidecarV2Record {
    TaxIdentitySidecarV2Record::new(provider_group_global_id, state, [0; 16], [0; 32]).unwrap()
}

fn sidecar(records: &[TaxIdentitySidecarV2Record]) -> Vec<u8> {
    let mut encoded = header().encode();
    for record in records {
        encoded.extend_from_slice(&record.encode());
    }
    encoded
}

fn invalid_data<T>(result: io::Result<T>) -> io::Error {
    let error = match result {
        Ok(_) => panic!("expected fail-closed validation"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    error
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        encoded.push(DIGITS[usize::from(byte >> 4)] as char);
        encoded.push(DIGITS[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[test]
fn frozen_header_bytes_and_format_identity_are_stable() {
    assert_eq!(
        TAX_IDENTITY_SIDECAR_V2_FORMAT,
        "ptg2_provider_group_tax_identity_v2"
    );
    let encoded = header().encode();
    assert_eq!(
        hex(&encoded),
        concat!(
            "5054473254415832020041001d",
            "7074672d74696e2d686d61632d7368613235362d76313a746573742d32"
        )
    );
    assert_eq!(
        TaxIdentitySidecarV2Header::decode(&encoded)
            .unwrap()
            .policy_id(),
        POLICY_ID
    );
}

#[test]
fn header_accepts_the_strict_maximum_policy_id_only() {
    let maximum = format!("ptg-tin-hmac-sha256-v1:{}", "a".repeat(32));
    assert_eq!(maximum.len(), 55);
    let encoded = TaxIdentitySidecarV2Header::new(maximum.clone())
        .unwrap()
        .encode();
    assert_eq!(encoded[12], 55);
    assert_eq!(
        TaxIdentitySidecarV2Header::decode(&encoded)
            .unwrap()
            .policy_id(),
        maximum
    );

    let oversized = format!("{maximum}a");
    assert_eq!(
        TaxIdentitySidecarV2Header::new(oversized.clone())
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    invalid_data(TaxIdentitySidecarV2Header::decode(&raw_header(
        oversized.as_bytes(),
    )));
}

#[test]
fn header_rejects_magic_version_width_length_utf8_and_policy_errors() {
    let valid = header().encode();
    invalid_data(TaxIdentitySidecarV2Header::decode(&valid[..12]));
    for (index, value) in [(0, b'X'), (8, 3), (10, 64)] {
        let mut tampered = valid.clone();
        tampered[index] = value;
        invalid_data(TaxIdentitySidecarV2Header::decode(&tampered));
    }

    let mut short_declared = valid.clone();
    short_declared[12] -= 1;
    invalid_data(TaxIdentitySidecarV2Header::decode(&short_declared));
    let mut long_declared = valid.clone();
    long_declared[12] += 1;
    invalid_data(TaxIdentitySidecarV2Header::decode(&long_declared));
    let mut trailing = valid.clone();
    trailing.push(0);
    invalid_data(TaxIdentitySidecarV2Header::decode(&trailing));

    invalid_data(TaxIdentitySidecarV2Header::decode(&raw_header(&[])));
    invalid_data(TaxIdentitySidecarV2Header::decode(&raw_header(&[0xff])));
    invalid_data(TaxIdentitySidecarV2Header::decode(&raw_header(
        b"ptg-tin-hmac-sha256-v1:UPPER",
    )));
}

#[test]
fn stream_rejects_every_truncated_header() {
    let encoded = header().encode();
    for end in 0..encoded.len() {
        invalid_data(TaxIdentitySidecarV2StreamValidator::new(
            Cursor::new(&encoded[..end]),
            1,
        ));
    }
}

#[test]
fn frozen_matched_ein_record_bytes_are_stable() {
    let record = matched_record([0x11; 16], TaxIdentityStateV2::MatchedEin, 0x22);
    assert_eq!(record.provider_group_global_id(), &[0x11; 16]);
    assert_eq!(record.state(), TaxIdentityStateV2::MatchedEin);
    assert_eq!(record.tin_id_128(), &[0x22; 16]);
    assert_eq!(record.tin_hmac_sha256(), &[0x22; 32]);
    let encoded = record.encode();
    assert_eq!(encoded.len(), 65);
    assert_eq!(
        hex(&encoded),
        concat!(
            "11111111111111111111111111111111",
            "01",
            "22222222222222222222222222222222",
            "2222222222222222222222222222222222222222222222222222222222222222"
        )
    );
    assert_eq!(
        TaxIdentitySidecarV2Record::decode(&encoded).unwrap(),
        record
    );
}

#[test]
fn state_values_and_valid_token_shapes_are_frozen() {
    assert_eq!(TaxIdentityStateV2::MatchedEin as u8, 1);
    assert_eq!(TaxIdentityStateV2::Missing as u8, 2);
    assert_eq!(TaxIdentityStateV2::Malformed as u8, 3);
    assert_eq!(TaxIdentityStateV2::UnsupportedType as u8, 4);
    assert_eq!(TaxIdentityStateV2::MatchedNpi as u8, 5);

    let npi = matched_record([1; 16], TaxIdentityStateV2::MatchedNpi, 0x33);
    assert_eq!(
        TaxIdentitySidecarV2Record::decode(&npi.encode()).unwrap(),
        npi
    );
    for state in [
        TaxIdentityStateV2::Missing,
        TaxIdentityStateV2::Malformed,
        TaxIdentityStateV2::UnsupportedType,
    ] {
        let record = unavailable_record([state as u8; 16], state);
        assert_eq!(
            TaxIdentitySidecarV2Record::decode(&record.encode()).unwrap(),
            record
        );
    }
}

#[test]
fn record_constructor_rejects_noncanonical_token_shapes() {
    let matched_states = [
        TaxIdentityStateV2::MatchedEin,
        TaxIdentityStateV2::MatchedNpi,
    ];
    for state in matched_states {
        let zero = TaxIdentitySidecarV2Record::new([1; 16], state, [0; 16], [0; 32]);
        assert_eq!(zero.err().unwrap().kind(), io::ErrorKind::InvalidInput);
        let mismatch = TaxIdentitySidecarV2Record::new([1; 16], state, [2; 16], [3; 32]);
        assert_eq!(mismatch.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    }

    for state in [
        TaxIdentityStateV2::Missing,
        TaxIdentityStateV2::Malformed,
        TaxIdentityStateV2::UnsupportedType,
    ] {
        let locator = TaxIdentitySidecarV2Record::new([1; 16], state, [1; 16], [0; 32]);
        assert_eq!(locator.err().unwrap().kind(), io::ErrorKind::InvalidInput);
        let hmac = TaxIdentitySidecarV2Record::new([1; 16], state, [0; 16], [1; 32]);
        assert_eq!(hmac.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    }
}

#[test]
fn record_decoder_rejects_width_state_and_token_tampering() {
    let matched = matched_record([1; 16], TaxIdentityStateV2::MatchedEin, 0x44).encode();
    invalid_data(TaxIdentitySidecarV2Record::decode(&matched[..64]));
    let mut oversized = matched.to_vec();
    oversized.push(0);
    invalid_data(TaxIdentitySidecarV2Record::decode(&oversized));

    for state in [0, 6, u8::MAX] {
        let mut invalid_state = matched;
        invalid_state[16] = state;
        invalid_data(TaxIdentitySidecarV2Record::decode(&invalid_state));
    }
    let mut zero_hmac = matched;
    zero_hmac[17..].fill(0);
    invalid_data(TaxIdentitySidecarV2Record::decode(&zero_hmac));
    let mut mismatched_locator = matched;
    mismatched_locator[17] ^= 1;
    invalid_data(TaxIdentitySidecarV2Record::decode(&mismatched_locator));

    let unavailable = unavailable_record([2; 16], TaxIdentityStateV2::Missing).encode();
    for token_index in [17, 33, 64] {
        let mut nonzero = unavailable;
        nonzero[token_index] = 1;
        invalid_data(TaxIdentitySidecarV2Record::decode(&nonzero));
    }
}

#[test]
fn stream_accepts_empty_and_strictly_ordered_records() {
    let mut empty = TaxIdentitySidecarV2StreamValidator::new(Cursor::new(sidecar(&[])), 0).unwrap();
    assert_eq!(empty.header().policy_id(), POLICY_ID);
    assert_eq!(empty.validate_to_end().unwrap(), 0);
    assert!(empty.next_record().unwrap().is_none());

    let records = [
        unavailable_record([1; 16], TaxIdentityStateV2::Missing),
        matched_record([2; 16], TaxIdentityStateV2::MatchedNpi, 0x55),
    ];
    let mut validator = TaxIdentitySidecarV2StreamValidator::new(
        Cursor::new(sidecar(&records)),
        records.len() as u64,
    )
    .unwrap();
    assert_eq!(validator.validate_to_end().unwrap(), 2);
    assert_eq!(validator.records_validated(), 2);
}

#[test]
fn stream_orders_on_the_complete_group_id_and_rejects_duplicates() {
    let mut low = [7u8; 16];
    low[15] = 1;
    let mut high = low;
    high[15] = 2;
    let ordered = [
        unavailable_record(low, TaxIdentityStateV2::Missing),
        unavailable_record(high, TaxIdentityStateV2::Malformed),
    ];
    let mut valid =
        TaxIdentitySidecarV2StreamValidator::new(Cursor::new(sidecar(&ordered)), 2).unwrap();
    assert_eq!(valid.validate_to_end().unwrap(), 2);

    for invalid_records in [[ordered[0], ordered[0]], [ordered[1], ordered[0]]] {
        let mut validator =
            TaxIdentitySidecarV2StreamValidator::new(Cursor::new(sidecar(&invalid_records)), 2)
                .unwrap();
        assert!(validator.next_record().unwrap().is_some());
        assert!(invalid_data(validator.next_record())
            .to_string()
            .contains("strictly increasing"));
        assert!(invalid_data(validator.next_record())
            .to_string()
            .contains("poisoned"));
    }
}

#[test]
fn record_limit_plus_one_is_a_permanent_error() {
    let records = [
        unavailable_record([1; 16], TaxIdentityStateV2::Missing),
        unavailable_record([2; 16], TaxIdentityStateV2::Malformed),
    ];
    let mut validator =
        TaxIdentitySidecarV2StreamValidator::new(Cursor::new(sidecar(&records)), 1).unwrap();
    assert!(validator.next_record().unwrap().is_some());
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("limit exceeded"));
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("poisoned"));
    assert_eq!(validator.records_validated(), 1);
}

#[test]
fn every_partial_trailing_record_fails_closed_and_poisons() {
    let record = matched_record([9; 16], TaxIdentityStateV2::MatchedEin, 0x66).encode();
    for partial_length in 1..record.len() {
        let mut encoded = header().encode();
        encoded.extend_from_slice(&record[..partial_length]);
        let mut validator =
            TaxIdentitySidecarV2StreamValidator::new(Cursor::new(encoded), 1).unwrap();
        assert!(invalid_data(validator.validate_to_end())
            .to_string()
            .contains("truncated"));
        assert!(invalid_data(validator.next_record())
            .to_string()
            .contains("poisoned"));
    }
}

#[test]
fn invalid_stream_record_fails_closed_and_poisons() {
    let mut encoded = header().encode();
    let mut invalid_record = unavailable_record([1; 16], TaxIdentityStateV2::Missing).encode();
    invalid_record[16] = 0;
    encoded.extend_from_slice(&invalid_record);
    let mut validator = TaxIdentitySidecarV2StreamValidator::new(Cursor::new(encoded), 1).unwrap();
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("record is invalid"));
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("poisoned"));
}

struct OneByteInterruptedReader {
    inner: Cursor<Vec<u8>>,
    interrupt_next: bool,
}

impl Read for OneByteInterruptedReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        if self.interrupt_next {
            self.interrupt_next = false;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.interrupt_next = true;
        self.inner.read(&mut output[..1])
    }
}

struct FailingReader {
    bytes: Vec<u8>,
    position: usize,
    fail_at: usize,
}

impl Read for FailingReader {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if self.position >= self.fail_at {
            return Err(io::Error::other("synthetic reader failure"));
        }
        let available = self
            .fail_at
            .saturating_sub(self.position)
            .min(self.bytes.len().saturating_sub(self.position))
            .min(output.len());
        output[..available].copy_from_slice(&self.bytes[self.position..self.position + available]);
        self.position += available;
        Ok(available)
    }
}

#[test]
fn one_byte_and_interrupted_reads_preserve_stream_validation() {
    let records = [matched_record(
        [3; 16],
        TaxIdentityStateV2::MatchedNpi,
        0x77,
    )];
    let reader = OneByteInterruptedReader {
        inner: Cursor::new(sidecar(&records)),
        interrupt_next: true,
    };
    let mut validator = TaxIdentitySidecarV2StreamValidator::new(reader, 1).unwrap();
    assert_eq!(validator.validate_to_end().unwrap(), 1);
}

#[test]
fn underlying_header_and_record_io_errors_are_preserved_and_poison_the_stream() {
    let records = [unavailable_record([1; 16], TaxIdentityStateV2::Missing)];
    let encoded = sidecar(&records);
    let header_failure = FailingReader {
        bytes: encoded.clone(),
        position: 0,
        fail_at: 5,
    };
    let error = TaxIdentitySidecarV2StreamValidator::new(header_failure, 1)
        .err()
        .unwrap();
    assert_eq!(error.kind(), io::ErrorKind::Other);

    let header_bytes = header().encode().len();
    let record_failure = FailingReader {
        bytes: encoded,
        position: 0,
        fail_at: header_bytes + 10,
    };
    let mut validator = TaxIdentitySidecarV2StreamValidator::new(record_failure, 1).unwrap();
    assert_eq!(
        validator.next_record().err().unwrap().kind(),
        io::ErrorKind::Other
    );
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn debug_and_errors_do_not_echo_opaque_or_token_bytes() {
    let record = matched_record([0xab; 16], TaxIdentityStateV2::MatchedEin, 0xcd);
    let debug = format!("{record:?}");
    assert!(!debug.contains("abab"));
    assert!(!debug.contains("cdcd"));
    assert!(debug.contains("<opaque>"));
    assert!(debug.contains("<redacted>"));

    let header_debug = format!("{:?}", header());
    assert!(!header_debug.contains(POLICY_ID));
    let mut tampered = record.encode();
    tampered[17] ^= 1;
    let error = invalid_data(TaxIdentitySidecarV2Record::decode(&tampered)).to_string();
    assert!(!error.contains("abab"));
    assert!(!error.contains("cdcd"));
}
