use ptg2_scanner::v3_runs::{
    external_sort_partition_files, external_sort_tagged_partition_files, read_audit_candidate_file,
    read_code_dictionary, read_code_dictionary_exact, write_audit_candidate_file,
    AuditCandidateRecord, NaturalLeanCodeFields, ServingRunPartitionWriter, ServingRunRecord,
    TaggedServingRunCodec, TaggedServingRunRecord, AUDIT_CANDIDATE_MAX_RECORDS,
    AUDIT_CANDIDATE_RECORD_BYTES, CODE_DICTIONARY_FORMAT_VERSION, COVERAGE_SCOPE_ID_BYTES,
    SERVING_RUN_RECORD_BYTES,
};
use std::fs;
use std::io::{self, Cursor, Read};
use std::path::{Path, PathBuf};

fn serving_record(code: u8, provider: u8, price: u8) -> ServingRunRecord {
    ServingRunRecord {
        code_id: [code; 16],
        provider_set_id: [provider; 16],
        price_set_id: [price; 16],
        provider_count: 7,
    }
}

fn write_records(path: &Path, records: &[ServingRunRecord]) {
    let mut bytes = Vec::with_capacity(records.len() * SERVING_RUN_RECORD_BYTES);
    for record in records {
        bytes.extend_from_slice(&record.encode());
    }
    fs::write(path, bytes).unwrap();
}

struct InterruptOnce<R> {
    inner: R,
    interrupted: bool,
}

impl<R> InterruptOnce<R> {
    fn new(inner: R) -> Self {
        Self {
            inner,
            interrupted: false,
        }
    }
}

impl<R: Read> Read for InterruptOnce<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        if !self.interrupted {
            self.interrupted = true;
            return Err(io::Error::from(io::ErrorKind::Interrupted));
        }
        self.inner.read(buffer)
    }
}

struct AlwaysFails;

impl Read for AlwaysFails {
    fn read(&mut self, _buffer: &mut [u8]) -> io::Result<usize> {
        Err(io::Error::other("scripted read failure"))
    }
}

fn dictionary_bytes(version: u32, record_count: u64, body: &[u8]) -> Vec<u8> {
    let mut bytes = b"PTG2CDR4".to_vec();
    bytes.extend_from_slice(&version.to_be_bytes());
    bytes.extend_from_slice(&record_count.to_be_bytes());
    bytes.extend_from_slice(body);
    bytes
}

fn write_dictionary(root: &Path, name: &str, bytes: &[u8]) -> PathBuf {
    let path = root.join(name);
    fs::write(&path, bytes).unwrap();
    path
}

#[test]
fn audit_candidate_files_reject_invalid_cardinality_and_framing() {
    let root = tempfile::tempdir().unwrap();
    let record = AuditCandidateRecord {
        code_key: 1,
        provider_set_key: 2,
        price_key: 3,
        source_key: 4,
        provider_count: 5,
    };

    let mismatched = root.path().join("mismatched.audit");
    assert!(write_audit_candidate_file(&mismatched, 2, &[record]).is_err());

    let unaligned = root.path().join("unaligned.audit");
    fs::write(&unaligned, vec![0; AUDIT_CANDIDATE_RECORD_BYTES - 1]).unwrap();
    assert!(read_audit_candidate_file(&unaligned).is_err());

    let oversized = root.path().join("oversized.audit");
    fs::write(
        &oversized,
        vec![0; (AUDIT_CANDIDATE_MAX_RECORDS + 1) * AUDIT_CANDIDATE_RECORD_BYTES],
    )
    .unwrap();
    assert!(read_audit_candidate_file(&oversized).is_err());
}

#[test]
fn serving_record_readers_retry_interrupts_and_preserve_terminal_errors() {
    let record = serving_record(1, 2, 3);
    assert!(ServingRunRecord::decode(&[0; SERVING_RUN_RECORD_BYTES + 1]).is_err());

    let mut interrupted = InterruptOnce::new(Cursor::new(record.encode()));
    assert_eq!(
        ServingRunRecord::read_from(&mut interrupted).unwrap(),
        Some(record)
    );
    assert!(ServingRunRecord::read_from(&mut AlwaysFails).is_err());

    let codec = TaggedServingRunCodec::new(2, 1).unwrap();
    let tagged = TaggedServingRunRecord {
        record,
        source_key: 1,
    };
    let encoded = tagged.encode(codec).unwrap();
    let mut interrupted = InterruptOnce::new(Cursor::new(encoded));
    assert_eq!(
        TaggedServingRunRecord::read_from(&mut interrupted, codec).unwrap(),
        Some(tagged)
    );
    assert!(TaggedServingRunRecord::read_from(&mut AlwaysFails, codec).is_err());

    let invalid = TaggedServingRunRecord {
        record,
        source_key: 2,
    };
    assert!(invalid.encode(codec).is_err());
    assert!(invalid.write_to(&mut Vec::new(), codec).is_err());
}

#[test]
fn partition_writer_rejects_invalid_public_inputs() {
    let root = tempfile::tempdir().unwrap();
    assert!(ServingRunPartitionWriter::with_buffer_capacity(root.path(), 4, "writer", 0).is_err());
    assert!(ServingRunPartitionWriter::new(root.path().join("empty-token"), 4, "").is_err());
    drop(
        ServingRunPartitionWriter::new(root.path().join("sanitized-token"), 4, "bad token!")
            .unwrap(),
    );

    let coverage_scope_id = [7; COVERAGE_SCOPE_ID_BYTES];
    let fields = NaturalLeanCodeFields {
        coverage_scope_id: &coverage_scope_id,
        reported_code_system: Some("CPT"),
        reported_code: Some("70553"),
        negotiation_arrangement: Some("ffs"),
        billing_code_type_version: None,
        name: None,
        description: None,
    };
    let wrong_record = serving_record(9, 2, 3);
    let mut writer =
        ServingRunPartitionWriter::new(root.path().join("writer"), 4, "writer").unwrap();
    assert!(writer
        .write_natural_lean_record(&wrong_record, fields)
        .is_err());
    let prepared = writer.register_natural_lean_code(fields).unwrap();
    assert!(writer
        .write_prepared_natural_lean_record(&wrong_record, prepared)
        .is_err());
}

#[test]
fn code_dictionary_reader_rejects_each_authenticated_boundary() {
    let root = tempfile::tempdir().unwrap();
    let valid_empty = dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, 0, &[]);
    let valid_empty_path = write_dictionary(root.path(), "valid-empty.codes", &valid_empty);
    assert!(read_code_dictionary(&valid_empty_path).unwrap().is_empty());
    assert!(
        read_code_dictionary_exact(&valid_empty_path, 0, valid_empty.len() as u64 + 1).is_err()
    );
    assert!(read_code_dictionary_exact(&valid_empty_path, 1, valid_empty.len() as u64).is_err());

    let short = write_dictionary(root.path(), "short.codes", &[0; 19]);
    assert!(read_code_dictionary(short).is_err());

    let mut invalid_magic = valid_empty.clone();
    invalid_magic[0] = b'X';
    let invalid_magic = write_dictionary(root.path(), "invalid-magic.codes", &invalid_magic);
    assert!(read_code_dictionary(invalid_magic).is_err());

    let invalid_version = write_dictionary(
        root.path(),
        "invalid-version.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION + 1, 0, &[]),
    );
    assert!(read_code_dictionary(invalid_version).is_err());

    let overflow = write_dictionary(
        root.path(),
        "overflow-count.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, u64::MAX, &[]),
    );
    assert!(read_code_dictionary(overflow).is_err());

    let impossible = write_dictionary(
        root.path(),
        "impossible-count.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, 1, &[]),
    );
    assert!(read_code_dictionary(impossible).is_err());

    let mut mismatched_identity_body = vec![0; 16 + COVERAGE_SCOPE_ID_BYTES];
    mismatched_identity_body.extend_from_slice(&[0; 6]);
    let mismatched_identity = write_dictionary(
        root.path(),
        "mismatched-identity.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, 1, &mismatched_identity_body),
    );
    assert!(read_code_dictionary(mismatched_identity).is_err());

    let mut invalid_utf8_body = vec![0; 16 + COVERAGE_SCOPE_ID_BYTES];
    invalid_utf8_body.push(1);
    invalid_utf8_body.extend_from_slice(&1u32.to_be_bytes());
    invalid_utf8_body.push(0xff);
    invalid_utf8_body.extend_from_slice(&[0; 5]);
    let invalid_utf8 = write_dictionary(
        root.path(),
        "invalid-utf8.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, 1, &invalid_utf8_body),
    );
    assert!(read_code_dictionary(invalid_utf8).is_err());

    let mut invalid_tag_body = vec![0; 16 + COVERAGE_SCOPE_ID_BYTES];
    invalid_tag_body.push(2);
    invalid_tag_body.extend_from_slice(&[0; 5]);
    let invalid_tag = write_dictionary(
        root.path(),
        "invalid-tag.codes",
        &dictionary_bytes(CODE_DICTIONARY_FORMAT_VERSION, 1, &invalid_tag_body),
    );
    assert!(read_code_dictionary(invalid_tag).is_err());
}

#[test]
fn partition_sorts_reject_invalid_boundaries_and_flush_remainders() {
    let root = tempfile::tempdir().unwrap();
    let empty: Vec<PathBuf> = Vec::new();
    assert!(external_sort_partition_files(
        &empty,
        root.path().join("zero-limit.out"),
        root.path().join("zero-limit-tmp"),
        0,
        0,
        4,
    )
    .is_err());
    assert!(external_sort_partition_files(
        &empty,
        root.path().join("bad-partition.out"),
        root.path().join("bad-partition-tmp"),
        1,
        4,
        4,
    )
    .is_err());

    let unaligned = root.path().join("unaligned.run");
    fs::write(&unaligned, [0]).unwrap();
    assert!(external_sort_partition_files(
        &[unaligned],
        root.path().join("unaligned.out"),
        root.path().join("unaligned-tmp"),
        1,
        0,
        4,
    )
    .is_err());

    let wrong_partition = root.path().join("wrong-partition.run");
    write_records(&wrong_partition, &[serving_record(0x80, 2, 3)]);
    assert!(external_sort_partition_files(
        &[wrong_partition],
        root.path().join("wrong-partition.out"),
        root.path().join("wrong-partition-tmp"),
        1,
        0,
        4,
    )
    .is_err());

    let remainder = root.path().join("remainder.run");
    write_records(&remainder, &[serving_record(1, 2, 3)]);
    let stats = external_sort_partition_files(
        &[remainder],
        root.path().join("remainder.out"),
        root.path().join("remainder-tmp"),
        2,
        0,
        4,
    )
    .unwrap();
    assert_eq!(stats.input_records, 1);
}

#[test]
fn tagged_partition_sorts_reject_invalid_boundaries_and_flush_remainders() {
    let root = tempfile::tempdir().unwrap();
    let codec = TaggedServingRunCodec::new(2, 1).unwrap();
    let empty: Vec<(PathBuf, u32)> = Vec::new();
    assert!(external_sort_tagged_partition_files(
        &empty,
        root.path().join("zero-limit.out"),
        root.path().join("zero-limit-tmp"),
        0,
        0,
        4,
        codec,
    )
    .is_err());
    assert!(external_sort_tagged_partition_files(
        &empty,
        root.path().join("bad-partition.out"),
        root.path().join("bad-partition-tmp"),
        1,
        4,
        4,
        codec,
    )
    .is_err());

    let valid = root.path().join("valid.run");
    write_records(&valid, &[serving_record(1, 2, 3)]);
    assert!(external_sort_tagged_partition_files(
        &[(valid.clone(), 2)],
        root.path().join("bad-source.out"),
        root.path().join("bad-source-tmp"),
        1,
        0,
        4,
        codec,
    )
    .is_err());

    let unaligned = root.path().join("unaligned.run");
    fs::write(&unaligned, [0]).unwrap();
    assert!(external_sort_tagged_partition_files(
        &[(unaligned, 0)],
        root.path().join("unaligned.out"),
        root.path().join("unaligned-tmp"),
        1,
        0,
        4,
        codec,
    )
    .is_err());

    let wrong_partition = root.path().join("wrong-partition.run");
    write_records(&wrong_partition, &[serving_record(0x80, 2, 3)]);
    assert!(external_sort_tagged_partition_files(
        &[(wrong_partition, 0)],
        root.path().join("wrong-partition.out"),
        root.path().join("wrong-partition-tmp"),
        1,
        0,
        4,
        codec,
    )
    .is_err());

    let stats = external_sort_tagged_partition_files(
        &[(valid, 1)],
        root.path().join("remainder.out"),
        root.path().join("remainder-tmp"),
        2,
        0,
        4,
        codec,
    )
    .unwrap();
    assert_eq!(stats.input_records, 1);
}
