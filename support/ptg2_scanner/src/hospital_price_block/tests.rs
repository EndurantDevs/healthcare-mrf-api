#[cfg(test)]
mod tests {
    use super::*;

    fn row(service: u32, payer: &str, plan: &str) -> HospitalPriceFactRow {
        HospitalPriceFactRow {
            charge_key: service,
            payer_name: payer.to_owned(),
            plan_name: plan.to_owned(),
            negotiated_dollar: service.is_multiple_of(2).then(|| format!("{service}.00")),
            negotiated_percentage: (service % 2 == 1).then(|| "87.125".to_owned()),
            negotiated_algorithm: service.is_multiple_of(3).then(|| "fee less 3%".to_owned()),
            methodology: "fee schedule".to_owned(),
            median_amount: Some(format!("{service}.50")),
            percentile_10: Some(format!("{service}.00")),
            percentile_90: Some(format!("{service}.99")),
            allowed_count: Some("11".to_owned()),
            additional_payer_notes: service.is_multiple_of(2).then(|| "reviewed".to_owned()),
            comparison_amount: Some(format!("{service}.50")),
        }
    }

    fn raw_from(block: &[u8]) -> Vec<u8> {
        decode_frame(block).unwrap().1
    }

    fn lane_offset(raw: &[u8], lane: usize) -> usize {
        header_u32(raw, 4 + lane * 8) as usize
    }

    #[test]
    fn normalized_block_round_trips_and_filters_before_offset_and_limit() {
        let mut nullable = row(5, "C", "Null");
        nullable.median_amount = None;
        nullable.percentile_10 = None;
        nullable.percentile_90 = None;
        nullable.allowed_count = None;
        nullable.comparison_amount = None;
        let rows = vec![
            row(1, "A", "One"),
            row(2, "B", "Two"),
            row(3, "A", "One"),
            row(4, "A", "Three"),
            nullable,
        ];
        let block = encode_fact_block(&rows).unwrap();
        let raw = raw_from(&block);
        assert_eq!(header_u32(&block, 12) as usize, rows.len());
        assert_eq!(header_u32(&block, 16) as usize, raw.len());
        assert_eq!(
            block.len(),
            HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES + header_u32(&block, 20) as usize
        );
        assert_eq!(decode_fact_block(&block, None, None, 0, 512).unwrap(), rows);
        assert_eq!(
            decode_fact_block(&block, Some("A"), Some("One"), 1, 1).unwrap(),
            vec![rows[2].clone()]
        );
        assert_eq!(
            decode_fact_block(&block, Some("A"), None, 1, 2).unwrap(),
            vec![rows[2].clone(), rows[3].clone()]
        );
        assert!(decode_fact_block(&block, None, None, usize::MAX, 5)
            .unwrap()
            .is_empty());
        assert!(decode_fact_block(&block, None, None, 0, 0)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn exact_limits_and_decimal_lexemes_are_enforced() {
        let rows = vec![row(2, "A", "One"); HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS];
        assert_eq!(
            decode_fact_block(&encode_fact_block(&rows).unwrap(), None, None, 0, 512).unwrap(),
            rows
        );
        assert!(encode_fact_block(&[]).is_err());
        assert!(encode_fact_block(&vec![row(2, "A", "One"); 513]).is_err());
        let mut invalid_decimal = row(2, "A", "One");
        invalid_decimal.median_amount = Some("1e2".to_owned());
        assert!(encode_fact_block(&[invalid_decimal]).is_err());
        let mut oversized = row(2, "A", "One");
        oversized.additional_payer_notes =
            Some("x".repeat(HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES));
        assert!(encode_fact_block(&[oversized]).is_err());

        let compact = vec![row(2, "P", "One"); HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS];
        let raw = raw_from(&encode_fact_block(&compact).unwrap());
        let mut lanes = decode_lanes(&raw).unwrap().map(|lane| lane.to_vec());
        let mut payer_plan = Vec::new();
        put_u16(&mut payer_plan, 1);
        put_text(&mut payer_plan, &"P".repeat(256 * 1024)).unwrap();
        put_text(&mut payer_plan, "One").unwrap();
        lanes[PAYER_PLAN_DICTIONARY] = payer_plan;
        let block = frame_raw(
            &assemble_raw(&lanes).unwrap(),
            HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
        )
        .unwrap();
        assert!(
            decode_fact_block(&block, None, None, 0, HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,).is_err()
        );
        assert_eq!(
            decode_fact_block(&block, None, None, 0, 1).unwrap().len(),
            1
        );
    }

    #[test]
    fn corruption_truncation_trailing_utf8_bitmap_and_digest_fail_closed() {
        let valid = encode_fact_block(&[row(2, "A", "One")]).unwrap();
        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(decode_fact_block(&bad_magic, None, None, 0, 1).is_err());

        let mut bad_digest = valid.clone();
        bad_digest[24] ^= 1;
        assert!(decode_fact_block(&bad_digest, None, None, 0, 1).is_err());
        let mut bad_count = valid.clone();
        bad_count[12..16].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_fact_block(&bad_count, None, None, 0, 1).is_err());
        assert!(decode_fact_block(&valid[..valid.len() - 1], None, None, 0, 1).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_fact_block(&trailing, None, None, 0, 1).is_err());

        let mut raw = raw_from(&valid);
        raw[4..8].copy_from_slice(&0u32.to_le_bytes());
        assert!(decode_fact_block(&frame_raw(&raw, 1).unwrap(), None, None, 0, 1).is_err());

        let mut raw = raw_from(&valid);
        let payer = lane_offset(&raw, PAYER_PLAN_DICTIONARY) + 2 + 4;
        raw[payer] = 0xff;
        assert!(decode_fact_block(&frame_raw(&raw, 1).unwrap(), None, None, 0, 1).is_err());

        let mut raw = raw_from(&valid);
        let bitmap = lane_offset(&raw, ALGORITHM_IDS);
        raw[bitmap] |= 0x80;
        assert!(decode_fact_block(&frame_raw(&raw, 1).unwrap(), None, None, 0, 1).is_err());

        let mut raw = raw_from(&valid);
        let median = lane_offset(&raw, MEDIAN_AMOUNTS) + 1 + 4;
        raw[median] = b'x';
        assert!(decode_fact_block(&frame_raw(&raw, 1).unwrap(), None, None, 0, 1).is_err());

        let mut zlib_trailing = valid.clone();
        let compressed_len = header_u32(&zlib_trailing, 20);
        zlib_trailing[20..24].copy_from_slice(&(compressed_len + 1).to_le_bytes());
        zlib_trailing.push(0);
        assert!(decode_fact_block(&zlib_trailing, None, None, 0, 1).is_err());
    }
}
