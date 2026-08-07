use super::*;
use crate::tax_identity::TaxIdentityState;
use std::io::{self, Cursor, Read};

const POLICY_ID: &str = "ptg-tin-hmac-sha256-v1:test-1";

fn header() -> TaxIdentitySidecarV1Header {
    TaxIdentitySidecarV1Header::new(POLICY_ID.to_owned()).unwrap()
}

fn raw_header(policy_id: &[u8]) -> Vec<u8> {
    assert!(policy_id.len() <= usize::from(u8::MAX));
    let mut encoded = Vec::with_capacity(FIXED_HEADER_BYTES + policy_id.len());
    encoded.extend_from_slice(TAX_IDENTITY_SIDECAR_V1_MAGIC);
    encoded.extend_from_slice(&TAX_IDENTITY_SIDECAR_V1_FORMAT_VERSION.to_le_bytes());
    encoded.extend_from_slice(&(TAX_IDENTITY_SIDECAR_V1_RECORD_BYTES as u16).to_le_bytes());
    encoded.push(policy_id.len() as u8);
    encoded.extend_from_slice(policy_id);
    encoded
}

fn matched_record(
    provider_group_global_id: ProviderGroupGlobalId,
    hmac_byte: u8,
) -> TaxIdentitySidecarV1Record {
    let hmac = [hmac_byte; 32];
    TaxIdentitySidecarV1Record::new(
        provider_group_global_id,
        TaxIdentityState::MatchedEin,
        [hmac_byte; 16],
        hmac,
    )
    .unwrap()
}

fn unavailable_record(
    provider_group_global_id: ProviderGroupGlobalId,
    state: TaxIdentityState,
) -> TaxIdentitySidecarV1Record {
    TaxIdentitySidecarV1Record::new(provider_group_global_id, state, [0; 16], [0; 32]).unwrap()
}

fn sidecar(records: &[TaxIdentitySidecarV1Record]) -> Vec<u8> {
    let mut encoded = header().encode();
    for record in records {
        encoded.extend_from_slice(&record.encode());
    }
    encoded
}

fn invalid_data<T>(result: io::Result<T>) -> io::Error {
    let error = result.err().expect("expected fail-closed validation");
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
fn frozen_v1_header_and_matched_record_are_stable() {
    assert_eq!(
        TAX_IDENTITY_SIDECAR_V1_FORMAT,
        "ptg2_provider_group_tax_identity_v1"
    );
    let encoded_header = header().encode();
    assert_eq!(
        hex(&encoded_header),
        concat!(
            "5054473254415831010041001d",
            "7074672d74696e2d686d61632d7368613235362d76313a746573742d31"
        )
    );
    assert_eq!(
        TaxIdentitySidecarV1Header::decode(&encoded_header)
            .unwrap()
            .policy_id(),
        POLICY_ID
    );

    let record = matched_record([0x11; 16], 0x22);
    assert_eq!(record.provider_group_global_id(), &[0x11; 16]);
    assert_eq!(record.state(), TaxIdentityState::MatchedEin);
    assert_eq!(record.tin_id_128(), &[0x22; 16]);
    assert_eq!(record.tin_hmac_sha256(), &[0x22; 32]);
    assert_eq!(
        hex(&record.encode()),
        concat!(
            "11111111111111111111111111111111",
            "01",
            "22222222222222222222222222222222",
            "2222222222222222222222222222222222222222222222222222222222222222"
        )
    );
    assert_eq!(
        TaxIdentitySidecarV1Record::decode(&record.encode()).unwrap(),
        record
    );
}

#[test]
fn header_accepts_only_the_strict_policy_contract() {
    let maximum = format!("ptg-tin-hmac-sha256-v1:{}", "a".repeat(32));
    let encoded = TaxIdentitySidecarV1Header::new(maximum.clone())
        .unwrap()
        .encode();
    assert_eq!(encoded[12], 55);
    assert_eq!(
        TaxIdentitySidecarV1Header::decode(&encoded)
            .unwrap()
            .policy_id(),
        maximum
    );
    let oversized = format!("{maximum}a");
    assert_eq!(
        TaxIdentitySidecarV1Header::new(oversized.clone())
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    invalid_data(TaxIdentitySidecarV1Header::decode(&raw_header(
        oversized.as_bytes(),
    )));
    invalid_data(TaxIdentitySidecarV1Header::decode(&raw_header(&[])));
    invalid_data(TaxIdentitySidecarV1Header::decode(&raw_header(&[0xff])));
    invalid_data(TaxIdentitySidecarV1Header::decode(&raw_header(
        b"ptg-tin-hmac-sha256-v1:UPPER",
    )));
}

#[test]
fn header_rejects_every_framing_mutation_and_truncation() {
    let valid = header().encode();
    for end in 0..valid.len() {
        invalid_data(TaxIdentitySidecarV1StreamValidator::new(
            Cursor::new(&valid[..end]),
            1,
        ));
    }
    for (index, value) in [(0, b'X'), (8, 2), (10, 64)] {
        let mut tampered = valid.clone();
        tampered[index] = value;
        invalid_data(TaxIdentitySidecarV1Header::decode(&tampered));
    }
    let mut short_declared = valid.clone();
    short_declared[12] -= 1;
    invalid_data(TaxIdentitySidecarV1Header::decode(&short_declared));
    let mut long_declared = valid.clone();
    long_declared[12] += 1;
    invalid_data(TaxIdentitySidecarV1Header::decode(&long_declared));
    let mut trailing = valid;
    trailing.push(0);
    invalid_data(TaxIdentitySidecarV1Header::decode(&trailing));
}

#[test]
fn state_values_and_canonical_token_shapes_are_frozen() {
    assert_eq!(TaxIdentityState::MatchedEin as u8, 1);
    assert_eq!(TaxIdentityState::Missing as u8, 2);
    assert_eq!(TaxIdentityState::Malformed as u8, 3);
    assert_eq!(TaxIdentityState::UnsupportedType as u8, 4);

    for state in [
        TaxIdentityState::Missing,
        TaxIdentityState::Malformed,
        TaxIdentityState::UnsupportedType,
    ] {
        let record = unavailable_record([state as u8; 16], state);
        assert_eq!(
            TaxIdentitySidecarV1Record::decode(&record.encode()).unwrap(),
            record
        );
    }
}

#[test]
fn constructor_and_decoder_reject_every_noncanonical_record_shape() {
    for (locator, hmac) in [([0; 16], [0; 32]), ([2; 16], [3; 32])] {
        let result =
            TaxIdentitySidecarV1Record::new([1; 16], TaxIdentityState::MatchedEin, locator, hmac);
        assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidInput);
    }
    for state in [
        TaxIdentityState::Missing,
        TaxIdentityState::Malformed,
        TaxIdentityState::UnsupportedType,
    ] {
        for (locator, hmac) in [([1; 16], [0; 32]), ([0; 16], [1; 32])] {
            let result = TaxIdentitySidecarV1Record::new([1; 16], state, locator, hmac);
            assert_eq!(result.err().unwrap().kind(), io::ErrorKind::InvalidInput);
        }
    }

    let matched = matched_record([1; 16], 0x44).encode();
    invalid_data(TaxIdentitySidecarV1Record::decode(&matched[..64]));
    let mut oversized = matched.to_vec();
    oversized.push(0);
    invalid_data(TaxIdentitySidecarV1Record::decode(&oversized));
    for state in [0, 5, u8::MAX] {
        let mut invalid_state = matched;
        invalid_state[16] = state;
        invalid_data(TaxIdentitySidecarV1Record::decode(&invalid_state));
    }
    let mut zero_hmac = matched;
    zero_hmac[17..].fill(0);
    invalid_data(TaxIdentitySidecarV1Record::decode(&zero_hmac));
    let mut mismatched_locator = matched;
    mismatched_locator[17] ^= 1;
    invalid_data(TaxIdentitySidecarV1Record::decode(&mismatched_locator));

    let unavailable = unavailable_record([2; 16], TaxIdentityState::Missing).encode();
    for token_index in [17, 33, 64] {
        let mut nonzero = unavailable;
        nonzero[token_index] = 1;
        invalid_data(TaxIdentitySidecarV1Record::decode(&nonzero));
    }
}

#[test]
fn stream_accepts_empty_and_strict_full_group_order() {
    let mut empty = TaxIdentitySidecarV1StreamValidator::new(Cursor::new(sidecar(&[])), 0).unwrap();
    assert_eq!(empty.header().policy_id(), POLICY_ID);
    assert_eq!(empty.validate_to_end().unwrap(), 0);
    assert!(empty.next_record().unwrap().is_none());

    let mut low = [7u8; 16];
    low[15] = 1;
    let mut high = low;
    high[15] = 2;
    let records = [
        unavailable_record(low, TaxIdentityState::Missing),
        matched_record(high, 0x55),
    ];
    let mut validator =
        TaxIdentitySidecarV1StreamValidator::new(Cursor::new(sidecar(&records)), 2).unwrap();
    assert_eq!(validator.validate_to_end().unwrap(), 2);
    assert_eq!(validator.records_validated(), 2);

    for invalid_records in [[records[0], records[0]], [records[1], records[0]]] {
        let mut invalid =
            TaxIdentitySidecarV1StreamValidator::new(Cursor::new(sidecar(&invalid_records)), 2)
                .unwrap();
        assert!(invalid.next_record().unwrap().is_some());
        assert!(invalid_data(invalid.next_record())
            .to_string()
            .contains("strictly increasing"));
        assert!(invalid_data(invalid.next_record())
            .to_string()
            .contains("poisoned"));
    }
}

#[test]
fn stream_limit_truncation_and_invalid_record_fail_permanently() {
    let records = [
        unavailable_record([1; 16], TaxIdentityState::Missing),
        unavailable_record([2; 16], TaxIdentityState::Malformed),
    ];
    let mut limited =
        TaxIdentitySidecarV1StreamValidator::new(Cursor::new(sidecar(&records)), 1).unwrap();
    assert!(limited.next_record().unwrap().is_some());
    assert!(invalid_data(limited.next_record())
        .to_string()
        .contains("limit exceeded"));
    assert!(invalid_data(limited.next_record())
        .to_string()
        .contains("poisoned"));

    let record = matched_record([9; 16], 0x66).encode();
    for partial_length in 1..record.len() {
        let mut encoded = header().encode();
        encoded.extend_from_slice(&record[..partial_length]);
        let mut validator =
            TaxIdentitySidecarV1StreamValidator::new(Cursor::new(encoded), 1).unwrap();
        assert!(invalid_data(validator.validate_to_end())
            .to_string()
            .contains("truncated"));
        assert!(invalid_data(validator.next_record())
            .to_string()
            .contains("poisoned"));
    }

    let mut encoded = header().encode();
    let mut invalid_record = unavailable_record([1; 16], TaxIdentityState::Missing).encode();
    invalid_record[16] = 0;
    encoded.extend_from_slice(&invalid_record);
    let mut validator = TaxIdentitySidecarV1StreamValidator::new(Cursor::new(encoded), 1).unwrap();
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
fn short_interrupted_and_underlying_io_reads_preserve_failure_semantics() {
    let records = [matched_record([3; 16], 0x77)];
    let reader = OneByteInterruptedReader {
        inner: Cursor::new(sidecar(&records)),
        interrupt_next: true,
    };
    let mut validator = TaxIdentitySidecarV1StreamValidator::new(reader, 1).unwrap();
    assert_eq!(validator.validate_to_end().unwrap(), 1);

    let encoded = sidecar(&records);
    let header_failure = FailingReader {
        bytes: encoded.clone(),
        position: 0,
        fail_at: 5,
    };
    assert_eq!(
        TaxIdentitySidecarV1StreamValidator::new(header_failure, 1)
            .err()
            .unwrap()
            .kind(),
        io::ErrorKind::Other
    );
    let record_failure = FailingReader {
        bytes: encoded,
        position: 0,
        fail_at: header().encode().len() + 10,
    };
    let mut validator = TaxIdentitySidecarV1StreamValidator::new(record_failure, 1).unwrap();
    assert_eq!(
        validator.next_record().err().unwrap().kind(),
        io::ErrorKind::Other
    );
    assert!(invalid_data(validator.next_record())
        .to_string()
        .contains("poisoned"));
}

#[test]
fn debug_and_errors_never_echo_group_policy_or_token_bytes() {
    let record = matched_record([0xab; 16], 0xcd);
    let debug = format!("{record:?}");
    assert!(!debug.contains("abab"));
    assert!(!debug.contains("cdcd"));
    assert!(debug.contains("<opaque>"));
    assert!(debug.contains("<redacted>"));
    let header_debug = format!("{:?}", header());
    assert!(!header_debug.contains(POLICY_ID));

    let mut tampered = record.encode();
    tampered[17] ^= 1;
    let error = invalid_data(TaxIdentitySidecarV1Record::decode(&tampered)).to_string();
    assert!(!error.contains("abab"));
    assert!(!error.contains("cdcd"));
}
