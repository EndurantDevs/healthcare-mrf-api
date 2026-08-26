#[cfg(test)]
mod tests {
    use super::*;

    fn code(key: &str, refs: &[u64]) -> HospitalPriceSelectorEntry {
        HospitalPriceSelectorEntry {
            key: HospitalPriceSelectorKey::Code(key.to_owned()),
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
            page.exact_refs(&HospitalPriceSelectorKey::Code("Z99".to_owned())),
            Some(&[3, 7, 9][..])
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
            page.exact_refs(&HospitalPriceSelectorKey::Code("a\0bc".to_owned())),
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
}
