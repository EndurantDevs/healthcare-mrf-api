    #[test]
    fn selector_packing_flushes_at_256_rows() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        let mut row = service();
        row.codes = (0..257)
            .map(|index| CodeRow {
                code_type: "CPT".to_owned(),
                code: format!("boundary-{index}"),
            })
            .collect();
        output.service(0, &row).unwrap();
        output.charge(0, 0, &charge("100")).unwrap();

        let summary = output.finish().unwrap();
        assert_eq!(summary.root.code_selector_key_count, 257);
        assert_eq!(summary.root.code_selector_page_count, 257);
        assert_eq!(summary.root.code_selector_block_count, 2);
        let rows = copy_rows(&directory.path().join("selector_page.copy"));
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    field_i64(row, 2),
                    field_i64(row, 3),
                    field_i32(row, 4),
                    field_i32(row, 7),
                    field_i32(row, 8),
                ))
                .collect::<Vec<_>>(),
            [(0, 0, 256, 0, 1), (1, 256, 1, 0, 1)]
        );
    }

    #[test]
    fn selector_packing_flushes_before_the_raw_byte_limit() {
        use crate::hospital_price_selector_block::{
            HospitalPriceSelectorKey, HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES,
            HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES,
        };

        let component_bytes = HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES;
        let mut keys = vec![
            HospitalPriceSelectorKey::Code {
                code_type: "a".repeat(component_bytes),
                code: "b".repeat(component_bytes),
            },
            HospitalPriceSelectorKey::Code {
                code_type: "c".repeat(component_bytes),
                code: "d".repeat(component_bytes),
            },
        ];
        keys.sort_unstable_by_key(crate::hospital_price_selector_block::selector_key_sha256);
        let entry_bytes = (4 + component_bytes) * 2 + 4 + 8;
        assert!(entry_bytes <= HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES);
        assert!(entry_bytes * 2 > HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_RAW_BYTES);

        let directory = tempfile::tempdir().unwrap();
        let (block_counts, rows) =
            write_selector_test_rows(directory.path(), keys, vec![vec![0], vec![1]], &[1, 1]);
        assert_eq!(block_counts, [2, 0]);
        assert_eq!(
            rows.iter()
                .map(|row| (
                    field_i64(row, 2),
                    field_i64(row, 3),
                    field_i32(row, 4),
                    field_i32(row, 7),
                    field_i32(row, 8),
                ))
                .collect::<Vec<_>>(),
            [(0, 0, 1, 0, 1), (1, 1, 1, 0, 1)]
        );
    }
