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

    fn assert_lanes_rejected(lanes: &[Vec<u8>; LANE_COUNT], row_count: usize) {
        let raw = assemble_raw(lanes).unwrap();
        let block = frame_raw(&raw, row_count).unwrap();
        assert!(decode_fact_block(&block, None, None, 0, row_count).is_err());
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
        assert!(decode_fact_block(
            &block,
            None,
            None,
            0,
            HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
        )
        .unwrap_err()
        .contains("materialization limit"));
    }

    #[test]
    fn private_size_and_dictionary_guards_fail_closed() {
        for (value, valid) in [
            ("0", true),
            ("-0.25", true),
            ("", false),
            ("-", false),
            (".1", false),
            ("1.", false),
            ("1.2.3", false),
        ] {
            assert_eq!(valid_decimal(value), valid, "{value:?}");
        }
        assert!(put_decimal(&mut Vec::new(), "1e2").is_err());
        assert!(encode_optional_decimals([Some("1e2")], 1).is_err());

        assert!(SliceCursor::new(&[]).u16().is_err());
        assert!(SliceCursor::new(&[]).u32().is_err());
        assert!(SliceCursor::new(&[]).text().is_err());
        assert!(SliceCursor::new(&[]).decimal().is_err());

        let oversized = "x".repeat(HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES + 1);
        assert!(put_text(&mut Vec::new(), &oversized).is_err());
        assert!(frame_raw(oversized.as_bytes(), 1).is_err());
        assert!(TextDictionary {
            entries: vec![oversized.clone()],
            ids: HashMap::new(),
        }
        .encode()
        .is_err());
        for entry in [
            (oversized.clone(), "plan".to_owned()),
            ("payer".to_owned(), oversized.clone()),
        ] {
            assert!(PayerPlanDictionary {
                entries: vec![entry],
                ids: HashMap::new(),
            }
            .encode()
            .is_err());
        }

        let mut lanes: [Vec<u8>; LANE_COUNT] = std::array::from_fn(|_| Vec::new());
        lanes[0] = vec![0; HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES];
        assert!(assemble_raw(&lanes).is_err());

        let mut total = usize::MAX;
        add_preflight_bytes(&mut total, 1);
        assert_eq!(total, usize::MAX);
        let mut total = HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES;
        add_preflight_bytes(&mut total, 1);
        assert_eq!(total, HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES + 1);
        let mut text = TextDictionary {
            entries: vec!["occupied".to_owned(); HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS],
            ids: HashMap::new(),
        };
        assert!(text.intern("new").is_err());
        let mut payer_plan = PayerPlanDictionary {
            entries: vec![
                ("payer".to_owned(), "plan".to_owned());
                HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS
            ],
            ids: HashMap::new(),
        };
        assert!(payer_plan.intern("new payer", "new plan").is_err());
    }

    #[test]
    fn corruption_truncation_trailing_utf8_bitmap_and_digest_fail_closed() {
        let valid = encode_fact_block(&[row(2, "A", "One")]).unwrap();
        assert!(decode_fact_block(&[], None, None, 0, 1).is_err());
        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(decode_fact_block(&bad_magic, None, None, 0, 1).is_err());

        let mut bad_digest = valid.clone();
        bad_digest[24] ^= 1;
        assert!(decode_fact_block(&bad_digest, None, None, 0, 1).is_err());
        let mut bad_version = valid.clone();
        bad_version[8..12].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_fact_block(&bad_version, None, None, 0, 1).is_err());
        let mut bad_count = valid.clone();
        bad_count[12..16].copy_from_slice(&2u32.to_le_bytes());
        assert!(decode_fact_block(&bad_count, None, None, 0, 1).is_err());
        for count in [0, HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS as u32 + 1] {
            let mut invalid_count = valid.clone();
            invalid_count[12..16].copy_from_slice(&count.to_le_bytes());
            assert!(decode_fact_block(&invalid_count, None, None, 0, 1).is_err());
        }
        let mut bad_raw_len = valid.clone();
        bad_raw_len[16..20]
            .copy_from_slice(&((HOSPITAL_PRICE_FACT_BLOCK_MAX_RAW_BYTES + 1) as u32).to_le_bytes());
        assert!(decode_fact_block(&bad_raw_len, None, None, 0, 1).is_err());
        let mut bad_compressed_len = valid.clone();
        bad_compressed_len[20..24].copy_from_slice(
            &((HOSPITAL_PRICE_FACT_BLOCK_MAX_COMPRESSED_BYTES + 1) as u32).to_le_bytes(),
        );
        assert!(decode_fact_block(&bad_compressed_len, None, None, 0, 1).is_err());
        assert!(decode_fact_block(&valid[..valid.len() - 1], None, None, 0, 1).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_fact_block(&trailing, None, None, 0, 1).is_err());
        let mut invalid_zlib = valid.clone();
        invalid_zlib[HOSPITAL_PRICE_FACT_BLOCK_HEADER_BYTES..].fill(0);
        assert!(decode_fact_block(&invalid_zlib, None, None, 0, 1).is_err());
        let mut wrong_raw_len = valid.clone();
        let raw_len = header_u32(&wrong_raw_len, 16);
        wrong_raw_len[16..20].copy_from_slice(&(raw_len + 1).to_le_bytes());
        assert!(decode_fact_block(&wrong_raw_len, None, None, 0, 1).is_err());

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

    #[test]
    fn authenticated_lane_validation_matrix_fails_closed() {
        let valid = encode_fact_block(&[row(2, "A", "One")]).unwrap();
        let base = decode_lanes(&raw_from(&valid)).unwrap().map(<[u8]>::to_vec);

        assert!(decode_lanes(&[]).is_err());
        assert!(decode_payer_plan_dictionary(&[]).is_err());
        let mut bad_lane_count = raw_from(&valid);
        bad_lane_count[..4].copy_from_slice(&0u32.to_le_bytes());
        assert!(decode_lanes(&bad_lane_count).is_err());
        let mut raw_trailing = raw_from(&valid);
        raw_trailing.push(0);
        assert!(decode_lanes(&raw_trailing).is_err());

        let mut lane_directory = raw_from(&valid);
        lane_directory[8..12].copy_from_slice(&u32::MAX.to_le_bytes());
        let block = frame_raw(&lane_directory, 1).unwrap();
        assert!(decode_fact_block(&block, None, None, 0, 1).is_err());

        for (lane, bytes) in [
            (PAYER_PLAN_IDS, Vec::new()),
            (PAYER_PLAN_IDS, 1u16.to_le_bytes().to_vec()),
            (CHARGE_KEYS, Vec::new()),
            (ALGORITHM_DICTIONARY, vec![0, 0, 0]),
            (
                ALGORITHM_DICTIONARY,
                vec![1, 0, 4, 0, 0, 0, b'A'],
            ),
            (
                ALGORITHM_DICTIONARY,
                vec![1, 0, 1, 0, 0, 0, 0xff],
            ),
            (
                ALGORITHM_DICTIONARY,
                [vec![1, 0], u32::MAX.to_le_bytes().to_vec()].concat(),
            ),
            (METHODOLOGY_DICTIONARY, Vec::new()),
            (ALLOWED_COUNT_DICTIONARY, Vec::new()),
            (PAYER_NOTE_DICTIONARY, Vec::new()),
            (ALGORITHM_IDS, vec![1, 0, 0]),
            (ALGORITHM_IDS, vec![0, 0]),
            (ALGORITHM_IDS, vec![1]),
            (METHODOLOGY_IDS, 1u16.to_le_bytes().to_vec()),
            (ALLOWED_COUNT_IDS, vec![1]),
            (PAYER_NOTE_IDS, vec![1]),
            (NEGOTIATED_DOLLARS, Vec::new()),
            (NEGOTIATED_DOLLARS, vec![1, 1, 0, 0, 0, b'x']),
            (NEGOTIATED_DOLLARS, vec![0, 0]),
            (NEGOTIATED_PERCENTAGES, Vec::new()),
            (MEDIAN_AMOUNTS, Vec::new()),
            (PERCENTILE_10_AMOUNTS, Vec::new()),
            (PERCENTILE_90_AMOUNTS, Vec::new()),
            (COMPARISON_AMOUNTS, Vec::new()),
            (ALGORITHM_DICTIONARY, 513u16.to_le_bytes().to_vec()),
            (PAYER_PLAN_DICTIONARY, 513u16.to_le_bytes().to_vec()),
        ] {
            let mut lanes = base.clone();
            lanes[lane] = bytes;
            assert_lanes_rejected(&lanes, 1);
        }

        let mut duplicate_text = base.clone();
        duplicate_text[ALGORITHM_DICTIONARY] =
            vec![2, 0, 1, 0, 0, 0, b'A', 1, 0, 0, 0, b'A'];
        assert_lanes_rejected(&duplicate_text, 1);

        let mut duplicate_payer = base;
        duplicate_payer[PAYER_PLAN_DICTIONARY] = vec![
            2, 0, 1, 0, 0, 0, b'A', 3, 0, 0, 0, b'O', b'n', b'e', 1, 0, 0, 0, b'A', 3,
            0, 0, 0, b'O', b'n', b'e',
        ];
        assert_lanes_rejected(&duplicate_payer, 1);

        let mut payer_plan_truncated = decode_lanes(&raw_from(&valid))
            .unwrap()
            .map(<[u8]>::to_vec);
        payer_plan_truncated[PAYER_PLAN_DICTIONARY] = vec![1, 0, 1, 0, 0, 0, b'A'];
        assert_lanes_rejected(&payer_plan_truncated, 1);

        let mut payer_plan_trailing = decode_lanes(&raw_from(&valid))
            .unwrap()
            .map(<[u8]>::to_vec);
        payer_plan_trailing[PAYER_PLAN_DICTIONARY] =
            vec![1, 0, 1, 0, 0, 0, b'A', 3, 0, 0, 0, b'O', b'n', b'e', 0];
        assert_lanes_rejected(&payer_plan_trailing, 1);
    }
}
