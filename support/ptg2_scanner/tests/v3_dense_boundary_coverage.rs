use ptg2_scanner::v3_dense::{DenseIdentityMap, DenseIdentityValue};

#[test]
fn dense_identity_memory_estimate_rejects_count_and_slot_byte_overflow() {
    for expected_len in [usize::MAX, usize::MAX / 10] {
        let error = DenseIdentityMap::estimated_memory_bytes(expected_len)
            .expect_err("oversized dense identity map must fail before allocation");
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "identity map is too large");
    }
    let mut bounded_map = DenseIdentityMap::with_capacity(0).unwrap();
    bounded_map
        .insert([1; 16], DenseIdentityValue::default())
        .unwrap();
    assert_eq!(
        bounded_map.get(&[1; 16]),
        Some(DenseIdentityValue::default())
    );
    assert_eq!(bounded_map.get(&[9; 16]), None);
    let error = bounded_map
        .insert([2; 16], DenseIdentityValue::default())
        .expect_err("immutable dense map must enforce its bounded load factor");
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert_eq!(
        error.to_string(),
        "identity map exceeded its bounded load factor"
    );
}
