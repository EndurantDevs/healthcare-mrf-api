#[cfg(test)]
mod tests {
    use super::*;

    fn code(key: &str, refs: &[u64]) -> HospitalPriceSelectorEntry {
        typed_code("CPT", key, refs)
    }

    fn typed_code(code_type: &str, key: &str, refs: &[u64]) -> HospitalPriceSelectorEntry {
        HospitalPriceSelectorEntry {
            key: HospitalPriceSelectorKey::Code {
                code_type: code_type.to_owned(),
                code: key.to_owned(),
            },
            refs: refs.to_vec(),
        }
    }

    fn payer_plan(payer: &str, plan: &str, refs: &[u64]) -> HospitalPriceSelectorEntry {
        HospitalPriceSelectorEntry {
            key: HospitalPriceSelectorKey::PayerPlan {
                payer_name: payer.to_owned(),
                plan_name: plan.to_owned(),
            },
            refs: refs.to_vec(),
        }
    }

    fn raw_from(block: &[u8]) -> Vec<u8> {
        decode_frame(block).unwrap().1
    }

    #[test]
    fn pages_round_trip_with_canonical_rows_refs_and_counts() {
        let block = encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            1,
            3,
            &[
                code("Z99", &[9, 3, 9]),
                code("A01", &[8, 2]),
                code("Z99", &[7, 3]),
            ],
        )
        .unwrap();
        let page = decode_selector_page(&block).unwrap();
        assert_eq!(page.kind, HospitalPriceSelectorKind::CodeToCharge);
        assert_eq!((page.page_index, page.page_count), (1, 3));
        assert_eq!((page.row_count(), page.ref_count()), (2, 5));
        assert_eq!(
            page.entries,
            vec![code("A01", &[2, 8]), code("Z99", &[3, 7, 9])]
        );
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::Code {
                code_type: "CPT".to_owned(),
                code: "Z99".to_owned(),
            }),
            Some(&[3, 7, 9][..])
        );
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::Code {
                code_type: "CPT".to_owned(),
                code: "missing".to_owned(),
            }),
            None
        );
        assert_eq!(header_u32(&block, 16) as usize, page.row_count());
        assert_eq!(header_u32(&block, 28) as usize, page.ref_count());
    }

    #[test]
    fn exact_composite_keys_keep_collision_adjacent_values_distinct() {
        let block = encode_selector_page(
            HospitalPriceSelectorKind::PayerPlanToFact,
            0,
            1,
            &[
                payer_plan("ab", "c", &[4]),
                payer_plan("a", "bc", &[3]),
                payer_plan("a\0", "bc", &[2]),
            ],
        )
        .unwrap();
        let page = decode_selector_page(&block).unwrap();
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::PayerPlan {
                payer_name: "ab".to_owned(),
                plan_name: "c".to_owned(),
            }),
            Some(&[4][..])
        );
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::PayerPlan {
                payer_name: "a".to_owned(),
                plan_name: "bc".to_owned(),
            }),
            Some(&[3][..])
        );
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::PayerPlan {
                payer_name: "a\0".to_owned(),
                plan_name: "bc".to_owned(),
            }),
            Some(&[2][..])
        );
        assert_eq!(
            page.exact_refs(&HospitalPriceSelectorKey::Code {
                code_type: "CPT".to_owned(),
                code: "a\0bc".to_owned(),
            }),
            None
        );
    }

    #[test]
    fn malformed_and_corrupt_pages_fail_closed() {
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            0,
            &[code("A", &[1])],
        )
        .is_err());
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[payer_plan("A", "P", &[1])],
        )
        .is_err());
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &[])],
        )
        .is_err());

        let valid = encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &[1, 2])],
        )
        .unwrap();
        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(decode_selector_page(&bad_magic).is_err());
        let mut bad_digest = valid.clone();
        bad_digest[40] ^= 1;
        assert!(decode_selector_page(&bad_digest).is_err());
        let mut bad_count = valid.clone();
        bad_count[28..32].copy_from_slice(&3u32.to_le_bytes());
        assert!(decode_selector_page(&bad_count).is_err());
        let mut bad_page = valid.clone();
        bad_page[20..24].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_selector_page(&bad_page).is_err());
        assert!(decode_selector_page(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_selector_page(&trailing).is_err());

        let mut raw = raw_from(&valid);
        raw[4] = 0xff;
        assert!(decode_selector_page(
            &frame_raw(HospitalPriceSelectorKind::CodeToCharge, 0, 1, 1, 2, &raw).unwrap()
        )
        .is_err());

        let mut raw = raw_from(&valid);
        raw[9..17].copy_from_slice(&2u64.to_le_bytes());
        raw[17..25].copy_from_slice(&1u64.to_le_bytes());
        assert!(decode_selector_page(
            &frame_raw(HospitalPriceSelectorKind::CodeToCharge, 0, 1, 1, 2, &raw).unwrap()
        )
        .is_err());

        let mut zlib_trailing = valid.clone();
        let compressed_len = header_u32(&zlib_trailing, 36);
        zlib_trailing[36..40].copy_from_slice(&(compressed_len + 1).to_le_bytes());
        zlib_trailing.push(0);
        assert!(decode_selector_page(&zlib_trailing).is_err());
    }

    #[test]
    fn encoder_and_frame_limits_fail_closed() {
        assert!(checked_add(usize::MAX, 1)
            .unwrap_err()
            .contains("raw length overflows"));

        for entries in [
            Vec::new(),
            vec![code("A", &[1]); HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS + 1],
        ] {
            assert!(encode_selector_page(
                HospitalPriceSelectorKind::CodeToCharge,
                0,
                1,
                &entries,
            )
            .unwrap_err()
            .contains("row count must be between 1 and 4096"));
        }

        assert!(encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code(
                &"x".repeat(HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1),
                &[1],
            )],
        )
        .unwrap_err()
        .contains("key component exceeds 1 MiB"));

        let oversized = "x".repeat(HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1);
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::PayerPlanToFact,
            0,
            1,
            &[payer_plan(&oversized, "plan", &[1])],
        )
        .is_err());
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::PayerPlanToFact,
            0,
            1,
            &[payer_plan("payer", &oversized, &[1])],
        )
        .is_err());

        let refs = vec![1; HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES / 8];
        assert!(encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &refs)],
        )
        .unwrap_err()
        .contains("input rows exceed the 4 MiB page limit"));

        for row_count in [0, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS + 1] {
            assert!(frame_raw(
                HospitalPriceSelectorKind::CodeToCharge,
                0,
                1,
                row_count,
                1,
                &[],
            )
            .unwrap_err()
            .contains("row count must be between 1 and 4096"));
        }
        assert!(frame_raw(
            HospitalPriceSelectorKind::CodeToCharge,
            1,
            1,
            1,
            1,
            &[],
        )
        .unwrap_err()
        .contains("page index or count is invalid"));
        assert!(frame_raw(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            1,
            1,
            &vec![0; HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES + 1],
        )
        .unwrap_err()
        .contains("raw payload exceeds 4 MiB"));
    }

    #[test]
    fn hostile_frame_headers_fail_closed() {
        let valid = encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &[1, 2])],
        )
        .unwrap();
        let corrupt_u32 = |offset: usize, value: u32| {
            let mut block = valid.clone();
            block[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            block
        };
        let mut cases = vec![
            ("empty header", Vec::new(), "header is truncated"),
            ("truncated header", valid[..71].to_vec(), "header is truncated"),
            (
                "version",
                corrupt_u32(8, 2),
                "version is unsupported",
            ),
            ("kind", corrupt_u32(12, 3), "kind is unsupported"),
            ("row count", corrupt_u32(16, 0), "row count is invalid"),
            (
                "row count maximum",
                corrupt_u32(16, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_ROWS as u32 + 1),
                "row count is invalid",
            ),
            (
                "page count",
                corrupt_u32(24, 0),
                "page index or count is invalid",
            ),
            (
                "reference count",
                corrupt_u32(28, 0),
                "reference count is invalid",
            ),
            (
                "reference count maximum",
                corrupt_u32(
                    28,
                    (HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES / 8) as u32 + 1,
                ),
                "reference count is invalid",
            ),
            (
                "raw length",
                corrupt_u32(32, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES as u32 + 1),
                "raw length exceeds 4 MiB",
            ),
            (
                "compressed length",
                corrupt_u32(
                    36,
                    HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_COMPRESSED_BYTES as u32 + 1,
                ),
                "compressed length exceeds the byte limit",
            ),
        ];

        let mut bad_zlib = valid.clone();
        bad_zlib[HOSPITAL_PRICE_SELECTOR_BLOCK_HEADER_BYTES] = 0;
        cases.push(("zlib", bad_zlib, "decompression failed"));

        let mut wrong_raw_len = valid.clone();
        let raw_len = header_u32(&wrong_raw_len, 32);
        wrong_raw_len[32..36].copy_from_slice(&(raw_len + 1).to_le_bytes());
        cases.push((
            "decompressed length",
            wrong_raw_len,
            "decompressed length does not match the header",
        ));

        for (name, block, expected) in cases {
            let error = decode_selector_page(&block).unwrap_err();
            assert!(error.contains(expected), "{name}: {error}");
        }
    }

    #[test]
    fn authenticated_raw_validation_fails_closed() {
        let valid = encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &[1, 2])],
        )
        .unwrap();
        let raw = raw_from(&valid);
        let frame = |raw: &[u8], row_count, ref_count| {
            frame_raw(
                HospitalPriceSelectorKind::CodeToCharge,
                0,
                1,
                row_count,
                ref_count,
                raw,
            )
            .unwrap()
        };
        let mut cases = vec![(
            "truncated key",
            frame(&[1, 0, 0, 0], 1, 2),
            "raw payload is truncated",
        )];
        cases.push((
            "truncated key length",
            frame(&[], 1, 1),
            "raw payload is truncated",
        ));
        cases.push((
            "truncated reference count",
            frame(&[1, 0, 0, 0, b'A'], 1, 1),
            "raw payload is truncated",
        ));
        cases.push((
            "truncated reference",
            frame(&[1, 0, 0, 0, b'A', 1, 0, 0, 0], 1, 1),
            "raw payload is truncated",
        ));

        let mut payer_cursor = SliceCursor::new(&[1, 0, 0, 0, b'A']);
        assert!(decode_key(
            HospitalPriceSelectorKind::PayerPlanToFact,
            &mut payer_cursor,
        )
        .is_err());
        assert!(decode_key(
            HospitalPriceSelectorKind::PayerPlanToFact,
            &mut SliceCursor::new(&[]),
        )
        .is_err());

        let mut oversized_key = raw.clone();
        oversized_key[..4].copy_from_slice(
            &(HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES as u32 + 1).to_le_bytes(),
        );
        cases.push((
            "oversized key",
            frame(&oversized_key, 1, 2),
            "key component exceeds 1 MiB",
        ));

        let mut zero_refs = raw.clone();
        zero_refs[12..16].copy_from_slice(&0u32.to_le_bytes());
        cases.push((
            "zero row references",
            frame(&zero_refs, 1, 2),
            "selector row reference count is invalid",
        ));

        let mut too_many_refs = raw.clone();
        too_many_refs[12..16].copy_from_slice(
            &((HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES / 8) as u32 + 1).to_le_bytes(),
        );
        cases.push((
            "too many row references",
            frame(&too_many_refs, 1, 2),
            "selector row reference count is invalid",
        ));

        cases.push((
            "references exceed header",
            frame(&raw, 1, 1),
            "reference count exceeds the header",
        ));

        let mut trailing_raw = raw.clone();
        trailing_raw.push(0);
        cases.push((
            "trailing raw byte",
            frame(&trailing_raw, 1, 2),
            "raw payload has trailing bytes",
        ));

        let two_rows = encode_selector_page(
            HospitalPriceSelectorKind::CodeToCharge,
            0,
            1,
            &[code("A", &[1]), code("B", &[2])],
        )
        .unwrap();
        let mut duplicate_key = raw_from(&two_rows);
        let second_code_offset = entry_raw_len(&code("A", &[1])).unwrap() + 4 + 3 + 4;
        duplicate_key[second_code_offset] = b'A';
        cases.push((
            "duplicate key",
            frame(&duplicate_key, 2, 2),
            "keys are not strictly sorted and unique",
        ));

        for (name, block, expected) in cases {
            let error = decode_selector_page(&block).unwrap_err();
            assert!(error.contains(expected), "{name}: {error}");
        }
    }

    include!("tests_code_identity.rs");
}
