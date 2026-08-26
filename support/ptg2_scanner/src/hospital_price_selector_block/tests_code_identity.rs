#[test]
fn code_system_keeps_equal_code_values_distinct() {
    let block = encode_selector_page(
        HospitalPriceSelectorKind::CodeToCharge,
        0,
        1,
        &[
            typed_code("CPT", "12345", &[1]),
            typed_code("HCPCS", "12345", &[2]),
        ],
    )
    .unwrap();
    let page = decode_selector_page(&block).unwrap();

    assert_eq!(page.entries.len(), 2);
    assert_eq!(
        page.exact_refs(&HospitalPriceSelectorKey::Code {
            code_type: "CPT".to_owned(),
            code: "12345".to_owned(),
        }),
        Some(&[1][..])
    );
    assert_eq!(
        page.exact_refs(&HospitalPriceSelectorKey::Code {
            code_type: "HCPCS".to_owned(),
            code: "12345".to_owned(),
        }),
        Some(&[2][..])
    );
}
