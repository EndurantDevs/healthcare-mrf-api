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
}
