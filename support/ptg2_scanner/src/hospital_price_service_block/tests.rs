#[cfg(test)]
mod tests {
    use super::*;

    fn charge(
        charge_key: u32,
        charge_ordinal: u64,
        first_fact_ordinal: u64,
        fact_count: u32,
    ) -> HospitalPriceChargeRow {
        HospitalPriceChargeRow {
            charge_key,
            charge_ordinal,
            setting: "inpatient".to_owned(),
            billing_class: Some("facility".to_owned()),
            modifier_codes: vec!["26".to_owned(), "TC".to_owned()],
            gross_charge: Some("100.00".to_owned()),
            discounted_cash: Some("80.50".to_owned()),
            minimum: Some("70.000".to_owned()),
            maximum: Some("130.25".to_owned()),
            additional_generic_notes: Some("case rate".to_owned()),
            first_fact_ordinal,
            fact_count,
        }
    }

    fn services() -> Vec<HospitalPriceServiceRow> {
        let mut null_charge = charge(8, 1, 12, 0);
        null_charge.billing_class = None;
        null_charge.modifier_codes.clear();
        null_charge.gross_charge = None;
        null_charge.minimum = None;
        null_charge.maximum = None;
        null_charge.additional_generic_notes = None;
        vec![
            HospitalPriceServiceRow {
                service_ordinal: 3,
                description: "multi-code service".to_owned(),
                drug_unit: Some("1.2300".to_owned()),
                drug_type: Some("GR".to_owned()),
                codes: vec![
                    HospitalPriceServiceCode {
                        code_type: "CPT".to_owned(),
                        code: "12345".to_owned(),
                    },
                    HospitalPriceServiceCode {
                        code_type: "HCPCS".to_owned(),
                        code: "A1234".to_owned(),
                    },
                ],
                charges: vec![charge(7, 0, 10, 2), null_charge],
            },
            HospitalPriceServiceRow {
                service_ordinal: 9,
                description: "non-drug service".to_owned(),
                drug_unit: None,
                drug_type: None,
                codes: vec![HospitalPriceServiceCode {
                    code_type: "MS-DRG".to_owned(),
                    code: "001".to_owned(),
                }],
                charges: vec![charge(21, 4, 12, 3)],
            },
        ]
    }

    fn raw_from(block: &[u8]) -> Vec<u8> {
        decode_frame(block).unwrap().2
    }

    fn assert_error<T>(result: HospitalPriceServiceBlockResult<T>, message: &str) {
        assert_eq!(result.err().as_deref(), Some(message));
    }

    fn assert_authenticated_raw_error(
        raw: &[u8],
        service_count: usize,
        charge_count: usize,
        message: &str,
    ) {
        assert_error(
            decode_service_block(&frame_raw(raw, service_count, charge_count).unwrap()),
            message,
        );
    }

    fn raw_u64(raw: &[u8], offset: usize) -> u64 {
        u64::from_le_bytes(raw[offset..offset + 8].try_into().unwrap())
    }

    #[test]
    fn multi_code_multi_charge_null_and_lexical_decimal_round_trip() {
        let expected = services();
        let block = encode_service_block(&expected).unwrap();
        assert_eq!(header_u32(&block, 12), 2);
        assert_eq!(header_u32(&block, 16), 3);
        assert_eq!(header_u32(&block, 20) as usize, raw_from(&block).len());
        assert_eq!(
            block.len(),
            HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES + header_u32(&block, 24) as usize
        );
        assert_eq!(decode_service_block(&block).unwrap(), expected);
        assert_eq!(
            encode_service_block(&decode_service_block(&block).unwrap()).unwrap(),
            block
        );
    }

    #[test]
    fn encoding_is_deterministic_and_rejects_noncanonical_ranges_and_limits() {
        let expected = services();
        assert_eq!(
            encode_service_block(&expected).unwrap(),
            encode_service_block(&expected).unwrap()
        );

        let mut gap = expected.clone();
        gap[1].charges[0].first_fact_ordinal += 1;
        assert!(encode_service_block(&gap).is_err());

        let mut duplicate_key = expected.clone();
        duplicate_key[1].charges[0].charge_key = 7;
        assert!(encode_service_block(&duplicate_key).is_err());

        let mut bad_decimal = expected.clone();
        bad_decimal[0].charges[0].gross_charge = Some("1e2".to_owned());
        assert!(encode_service_block(&bad_decimal).is_err());

        let mut oversized_raw = expected.clone();
        oversized_raw[0].charges[0].additional_generic_notes =
            Some("x".repeat(HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES));
        assert!(encode_service_block(&oversized_raw).is_err());

        let template = charge(0, 0, 0, 0);
        let charges = (0..513)
            .map(|ordinal| HospitalPriceChargeRow {
                charge_key: ordinal as u32,
                charge_ordinal: ordinal as u64,
                first_fact_ordinal: 0,
                ..template.clone()
            })
            .collect();
        let oversized = HospitalPriceServiceRow {
            service_ordinal: 0,
            description: "too many charges".to_owned(),
            drug_unit: None,
            drug_type: None,
            codes: vec![HospitalPriceServiceCode {
                code_type: "CPT".to_owned(),
                code: "1".to_owned(),
            }],
            charges,
        };
        assert!(encode_service_block(&[oversized]).is_err());

        let mut wide_ordinals = vec![expected[0].clone()];
        wide_ordinals[0].service_ordinal = u64::from(u32::MAX) + 1;
        wide_ordinals[0].charges[0].charge_ordinal = u64::from(u32::MAX) + 2;
        wide_ordinals[0].charges[1].charge_ordinal = u64::from(u32::MAX) + 3;
        wide_ordinals[0].charges[0].first_fact_ordinal = u64::from(u32::MAX) + 3;
        wide_ordinals[0].charges[1].first_fact_ordinal = u64::from(u32::MAX) + 5;
        assert_eq!(
            decode_service_block(&encode_service_block(&wide_ordinals).unwrap()).unwrap(),
            wide_ordinals
        );
    }

    include!("tests_validation_tail.rs");

    #[test]
    fn direct_cursor_guards_reject_impossible_public_states() {
        assert_error(
            frame_raw(
                &vec![0; HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES + 1],
                1,
                1,
            ),
            &invalid("raw payload exceeds 4 MiB"),
        );

        let mut cursor = Cursor::new(&[]);
        assert_error(
            cursor.reserve_decoded(usize::MAX, 2, "test"),
            &invalid("test decoded size overflows"),
        );

        let mut cursor = Cursor {
            bytes: &[],
            position: 0,
            decoded_bytes: 1,
        };
        assert_error(
            cursor.reserve_decoded(usize::MAX, 1, "test"),
            &invalid("test decoded size overflows"),
        );

        let mut cursor = Cursor::new(&[]);
        assert_error(
            cursor.reserve_decoded(
                HOSPITAL_PRICE_SERVICE_BLOCK_MAX_DECODED_BYTES + 1,
                1,
                "test",
            ),
            &invalid("decoded output exceeds 64 MiB"),
        );

        let mut cursor = Cursor {
            bytes: &[],
            position: usize::MAX,
            decoded_bytes: 0,
        };
        assert_error(cursor.take(1), &invalid("raw field length overflows"));

        let mut cursor = Cursor::new(&[]);
        assert_error(cursor.take(1), &invalid("raw payload is truncated"));

        assert_error(Cursor::new(&[]).u8(), &invalid("raw payload is truncated"));
        assert_error(Cursor::new(&[]).u32(), &invalid("raw payload is truncated"));
        assert_error(Cursor::new(&[]).u64(), &invalid("raw payload is truncated"));
        assert_error(
            Cursor::new(&[]).text("test"),
            &invalid("raw payload is truncated"),
        );
        assert_error(
            Cursor::new(&[1, 0, 0, 0]).text("test"),
            &invalid("raw payload is truncated"),
        );
        assert_error(
            Cursor::new(&[]).optional_text("test"),
            &invalid("raw payload is truncated"),
        );
        assert_error(
            Cursor::new(&[1]).optional_text("test"),
            &invalid("raw payload is truncated"),
        );
        assert_error(
            Cursor::new(&[1, 0, 0, 0, 0xff]).text("test"),
            &invalid("test contains invalid UTF-8"),
        );
        let mut cursor = Cursor {
            bytes: &[1, 0, 0, 0, b'x'],
            position: 0,
            decoded_bytes: HOSPITAL_PRICE_SERVICE_BLOCK_MAX_DECODED_BYTES,
        };
        assert_error(
            cursor.text("test"),
            &invalid("decoded output exceeds 64 MiB"),
        );

        let count = 1u32.to_le_bytes();
        let mut cursor = Cursor::new(&count);
        assert_error(
            cursor.bounded_count("test", 1),
            &invalid("test count exceeds the raw payload"),
        );
        assert_error(
            Cursor::new(&[]).bounded_count("test", 1),
            &invalid("raw payload is truncated"),
        );

        let cursor = Cursor::new(&[0]);
        assert_error(cursor.finish(), &invalid("raw payload has trailing bytes"));
    }

    #[test]
    fn corruption_truncation_trailing_utf8_decimal_and_zlib_tail_fail_closed() {
        let valid = encode_service_block(&services()).unwrap();

        let mut bad_magic = valid.clone();
        bad_magic[0] ^= 1;
        assert!(decode_service_block(&bad_magic).is_err());
        let mut bad_digest = valid.clone();
        bad_digest[28] ^= 1;
        assert!(decode_service_block(&bad_digest).is_err());
        let mut bad_count = valid.clone();
        bad_count[12..16].copy_from_slice(&3u32.to_le_bytes());
        assert!(decode_service_block(&bad_count).is_err());
        assert!(decode_service_block(&valid[..valid.len() - 1]).is_err());
        let mut trailing = valid.clone();
        trailing.push(0);
        assert!(decode_service_block(&trailing).is_err());

        let mut raw = raw_from(&valid);
        raw[20] = 0xff;
        assert!(decode_service_block(&frame_raw(&raw, 2, 3).unwrap()).is_err());

        let mut raw = raw_from(&valid);
        let drug_unit_tag = 20 + header_u32(&raw, 16) as usize;
        raw[drug_unit_tag] = 2;
        assert!(decode_service_block(&frame_raw(&raw, 2, 3).unwrap()).is_err());

        let mut raw = raw_from(&valid);
        let decimal = raw
            .windows(b"100.00".len())
            .position(|window| window == b"100.00")
            .unwrap();
        raw[decimal] = b'x';
        assert!(decode_service_block(&frame_raw(&raw, 2, 3).unwrap()).is_err());

        let mut zlib_trailing = valid.clone();
        let compressed_len = header_u32(&zlib_trailing, 24);
        zlib_trailing[24..28].copy_from_slice(&(compressed_len + 1).to_le_bytes());
        zlib_trailing.push(0);
        assert!(decode_service_block(&zlib_trailing).is_err());
    }

    #[test]
    fn authenticated_header_and_raw_corruption_matrix_fails_closed() {
        let valid = encode_service_block(&services()).unwrap();

        assert_error(decode_service_block(&[]), &invalid("header is truncated"));

        for (offset, value, message) in [
            (8, 2, "version is unsupported"),
            (12, 0, "service or charge count is invalid"),
            (12, 513, "service or charge count is invalid"),
            (16, 0, "service or charge count is invalid"),
            (16, 513, "service or charge count is invalid"),
            (12, 4, "service or charge count is invalid"),
            (
                20,
                HOSPITAL_PRICE_SERVICE_BLOCK_MAX_RAW_BYTES as u32 + 1,
                "raw length exceeds 4 MiB",
            ),
            (
                24,
                HOSPITAL_PRICE_SERVICE_BLOCK_MAX_COMPRESSED_BYTES as u32 + 1,
                "compressed length exceeds the byte limit",
            ),
            (
                20,
                header_u32(&valid, 20) + 1,
                "decompressed length does not match the header",
            ),
        ] {
            let mut block = valid.clone();
            block[offset..offset + 4].copy_from_slice(&value.to_le_bytes());
            assert_error(decode_service_block(&block), &invalid(message));
        }

        let mut corrupt_zlib = valid.clone();
        corrupt_zlib[HOSPITAL_PRICE_SERVICE_BLOCK_HEADER_BYTES] ^= 0xff;
        let error = decode_service_block(&corrupt_zlib).unwrap_err();
        assert!(
            error.starts_with(&invalid("decompression failed:")),
            "{error}"
        );

        let raw = raw_from(&valid);
        assert_eq!(header_u32(&raw, 56), 2);
        assert_eq!(header_u32(&raw, 94), 2);
        assert_eq!(header_u32(&raw, 136), 2);
        assert_eq!(header_u32(&raw, 213), 8);
        assert_eq!(raw_u64(&raw, 217), 1);
        assert_eq!(raw_u64(&raw, 261), 9);
        let mut cases = Vec::new();

        let mut changed = raw[..70].to_vec();
        changed[56..60].copy_from_slice(&1u32.to_le_bytes());
        changed[60..64].copy_from_slice(&20u32.to_le_bytes());
        assert_authenticated_raw_error(&changed, 1, 1, &invalid("raw payload is truncated"));

        let mut changed = raw[..74].to_vec();
        changed[56..60].copy_from_slice(&1u32.to_le_bytes());
        changed[67..71].copy_from_slice(&20u32.to_le_bytes());
        assert_authenticated_raw_error(&changed, 1, 1, &invalid("raw payload is truncated"));

        assert_authenticated_raw_error(
            &raw[..94],
            1,
            2,
            &invalid("raw payload is truncated"),
        );
        assert_authenticated_raw_error(
            &raw[..217],
            1,
            2,
            &invalid("raw payload is truncated"),
        );

        let mut changed = raw.clone();
        changed[110..114].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_authenticated_raw_error(&changed, 2, 3, &invalid("raw payload is truncated"));

        for (offset, field) in [
            (123, "billing class"),
            (163, "discounted cash"),
            (173, "minimum"),
            (184, "maximum"),
            (195, "additional generic notes"),
        ] {
            let mut changed = raw.clone();
            changed[offset] = 2;
            assert_authenticated_raw_error(
                &changed,
                2,
                3,
                &invalid(format!("{field} has an invalid presence tag")),
            );
        }

        let mut changed = raw.clone();
        changed[140..144].copy_from_slice(&u32::MAX.to_le_bytes());
        assert_authenticated_raw_error(&changed, 2, 3, &invalid("raw payload is truncated"));

        let mut changed = raw.clone();
        changed.truncate(changed.len() - 1);
        cases.push((changed, 2, 3, "raw payload is truncated"));

        let mut changed = raw.clone();
        changed[..8].copy_from_slice(&u64::MAX.to_le_bytes());
        cases.push((changed, 2, 3, "final-fact range overflows u64"));

        cases.push((Vec::new(), 1, 1, "raw payload is truncated"));

        let mut changed = raw.clone();
        changed[16..20].copy_from_slice(&0u32.to_le_bytes());
        cases.push((changed, 2, 3, "description must be non-empty"));

        let mut changed = raw.clone();
        changed[49] = 2;
        cases.push((changed, 2, 3, "drug type has an invalid presence tag"));

        let mut changed = raw.clone();
        changed[56..60].copy_from_slice(&0u32.to_le_bytes());
        cases.push((changed, 2, 3, "service must contain at least one code"));

        let mut changed = raw.clone();
        changed[56..60].copy_from_slice(&u32::MAX.to_le_bytes());
        cases.push((changed, 2, 3, "code count exceeds the raw payload"));

        let mut changed = raw.clone();
        changed[94..98].copy_from_slice(&0u32.to_le_bytes());
        cases.push((changed, 2, 3, "service charge count is invalid"));

        let mut changed = raw.clone();
        changed[136..140].copy_from_slice(&u32::MAX.to_le_bytes());
        cases.push((changed, 2, 3, "modifier count exceeds the raw payload"));

        let mut changed = raw.clone();
        changed.push(0);
        cases.push((changed, 2, 3, "raw payload has trailing bytes"));

        cases.push((
            raw.clone(),
            2,
            4,
            "decoded charge count does not match the header",
        ));

        let mut changed = raw.clone();
        changed.splice(38..49, [0]);
        cases.push((
            changed,
            2,
            3,
            "drug unit and type must be supplied together",
        ));

        let mut changed = raw.clone();
        changed[261..269].copy_from_slice(&3u64.to_le_bytes());
        cases.push((
            changed,
            2,
            3,
            "service ordinals must be strictly increasing",
        ));

        let mut changed = raw.clone();
        changed[217..225].copy_from_slice(&0u64.to_le_bytes());
        cases.push((
            changed,
            2,
            3,
            "charge ordinals must be strictly increasing within a service",
        ));

        let mut changed = raw.clone();
        changed[213..217].copy_from_slice(&7u32.to_le_bytes());
        cases.push((changed, 2, 3, "charge key is duplicated"));

        for (raw, service_count, charge_count, message) in cases {
            assert_authenticated_raw_error(&raw, service_count, charge_count, &invalid(message));
        }
    }
}
