use super::*;

fn encoded(value: u8, type_tag: u8) -> [u8; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES] {
    let mut record = [value; TAX_IDENTITY_COLLISION_AUDIT_RECORD_BYTES];
    record[32] = type_tag;
    record
}

#[test]
fn record_debug_redacts_hmac_and_decode_rejects_invalid_fields() {
    let record = CollisionAuditRecord::decode(encoded(0x31, EIN_TYPE_TAG)).unwrap();
    let debug = format!("{record:?}");
    assert!(debug.contains("<redacted>"));
    assert!(!debug.contains("31313131"));

    assert_eq!(
        CollisionAuditRecord::decode(encoded(0, EIN_TYPE_TAG))
            .unwrap_err()
            .to_string(),
        INVALID_AUDIT_RECORD
    );
    assert_eq!(
        CollisionAuditRecord::decode(encoded(0x31, 0xff))
            .unwrap_err()
            .to_string(),
        INVALID_AUDIT_RECORD
    );
}

#[test]
fn accumulator_rejects_invalid_tags_count_mismatch_and_overflow() {
    let invalid = CollisionAuditRecord {
        full_hmac: [0x41; 32],
        type_tag: 0xff,
    };
    assert_eq!(
        CollisionAuditAccumulator::new(1)
            .observe(invalid)
            .unwrap_err()
            .to_string(),
        INVALID_AUDIT_RECORD
    );

    let valid = CollisionAuditRecord::decode(encoded(0x51, NPI_TYPE_TAG)).unwrap();
    let mut short = CollisionAuditAccumulator::new(2);
    short.observe(valid).unwrap();
    assert_eq!(
        short.finish().err().unwrap().to_string(),
        INVALID_AUDIT_RECORD
    );
    assert_eq!(
        CollisionAuditAccumulator::new(1)
            .finish()
            .err()
            .unwrap()
            .to_string(),
        INVALID_AUDIT_RECORD
    );
    assert_eq!(
        checked_increment(u64::MAX).unwrap_err().to_string(),
        INVALID_AUDIT_RECORD
    );
}
