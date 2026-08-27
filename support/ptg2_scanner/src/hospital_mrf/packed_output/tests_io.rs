    #[test]
    fn finish_rejects_every_counter_mismatch() {
        let directories = (0..6)
            .map(|_| tempfile::tempdir().unwrap())
            .collect::<Vec<_>>();

        let mut output = prepared_builder(directories[0].path());
        output.next_charge_key = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[1].path());
        output.next_fact_ordinal = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[2].path());
        output.next_fact_ordinal = 2;
        output.written_fact_count = 2;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[3].path());
        output.next_charge_key = 3;
        output.written_charge_count = 3;
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[4].path());
        output.selector_block_counts = [2, 0];
        assert!(output.finish().is_err());

        let mut output = prepared_builder(directories[5].path());
        output.selector_spool_bytes = u64::MAX;
        assert!(output.finish().is_err());
    }

    #[test]
    fn scratch_drop_removes_owned_sorted_output() {
        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fs::write(&output.selector_sorted_path, b"sorted").unwrap();
        output.selector_sorted_owned = true;
        let sorted_path = output.selector_sorted_path.clone();
        drop(output);
        assert!(!sorted_path.exists());
    }

    #[test]
    fn builder_creation_and_empty_segments_fail_cleanly() {
        for partial in [".fact_block.copy.partial", ".selector_page.copy.partial"] {
            let directory = tempfile::tempdir().unwrap();
            fs::write(directory.path().join(partial), b"occupied").unwrap();
            assert!(PackedOutputBuilder::create(
                directory.path(),
                "version-1",
                Arc::new(AtomicU64::new(0)),
                16 * 1024 * 1024,
            )
            .is_err());
        }
        let directory = tempfile::tempdir().unwrap();
        fs::write(
            directory.path().join(".selector_refs.partial"),
            b"occupied",
        )
        .unwrap();
        assert!(PackedOutputBuilder::create(
            directory.path(),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            16 * 1024 * 1024,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.finish_current_service_segment();
        output.service(0, &service()).unwrap();
        output.finish_current_service_segment();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_service = None;
        assert!(output.finish_current_charge().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output
            .write_service_rows(vec![packed_service_row(0, 1, "service".to_owned())])
            .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        fail_next_sink_write(&mut output.sinks[1]);
        assert!(output.write_fact_rows(0, &[packed_fact(0)]).is_err());
    }

    #[test]
    fn remaining_io_edges_fail_closed() {
        use crate::hospital_price_selector_block::HospitalPriceSelectorKey;

        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            if unsafe { libc::geteuid() } != 0 {
                let directory = tempfile::tempdir().unwrap();
                let locked = directory.path().join("locked");
                fs::create_dir(&locked).unwrap();
                fs::set_permissions(&locked, fs::Permissions::from_mode(0o000)).unwrap();
                let locked_result = PackedSink::create(
                    &locked,
                    "test",
                    "version-1",
                    Arc::new(AtomicU64::new(0)),
                    1024,
                );
                fs::set_permissions(&locked, fs::Permissions::from_mode(0o700)).unwrap();
                assert!(locked_result.is_err());
            }
        }

        let directory = tempfile::tempdir().unwrap();
        let mut sink = PackedSink::create(
            directory.path(),
            "test",
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .unwrap();
        fail_next_sink_write(&mut sink);
        assert!(sink.finish().is_err());

        #[cfg(unix)]
        {
            let directory = tempfile::tempdir().unwrap();
            let mut sink = PackedSink::create(
                directory.path(),
                "test",
                "version-1",
                Arc::new(AtomicU64::new(0)),
                1024,
            )
            .unwrap();
            let writer = sink.writer.take().unwrap();
            let mut inner = writer.into_inner().ok().unwrap();
            inner.file = OpenOptions::new().write(true).open("/dev/null").unwrap();
            sink.writer = Some(BufWriter::with_capacity(1, inner));
            assert!(sink.finish().is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        assert!(PackedOutputBuilder::create(
            &directory.path().join("missing"),
            "version-1",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());
        assert!(PackedOutputBuilder::create(
            directory.path(),
            " ",
            Arc::new(AtomicU64::new(0)),
            1024,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_service = None;
        assert!(output.service(1, &service()).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        assert!(output.service(1, &service()).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.current_charge.as_mut().unwrap().first_fact_ordinal = 1;
        assert!(output.charge(0, 1, &charge("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.max_output_bytes = 38;
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.fact_rows = vec![
            packed_fact(0);
            crate::hospital_price_block::HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS - 1
        ];
        output.next_fact_ordinal = output.fact_rows.len() as u64;
        fail_next_sink_write(&mut output.sinks[1]);
        assert!(output.payer(0, 0, &payer("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.service_charge_count =
            crate::hospital_price_service_block::HOSPITAL_PRICE_SERVICE_BLOCK_MAX_CHARGES - 1;
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output.charge(0, 1, &charge("1")).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output
            .write_fact_rows(1, &[packed_fact(140 * 1024), packed_fact(140 * 1024)])
            .is_err());

        for capacity in [0, 2, 6] {
            let directory = tempfile::tempdir().unwrap();
            let path = directory.path().join("read-only");
            fs::write(&path, b"x").unwrap();
            let mut output = builder(directory.path());
            output.selector_spool = Some(BufWriter::with_capacity(
                capacity,
                File::open(&path).unwrap(),
            ));
            assert!(output
                .write_selector_ref(selector_code("x"), 0)
                .is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("read-only");
        fs::write(&path, b"x").unwrap();
        let mut output = builder(directory.path());
        output.selector_spool = Some(BufWriter::with_capacity(16, File::open(&path).unwrap()));
        output
            .selector_spool
            .as_mut()
            .unwrap()
            .write_all(b"buffered")
            .unwrap();
        output.selector_spool_bytes = 8;
        assert!(output.finish_selector_pages().is_err());

        #[cfg(unix)]
        {
            let directory = tempfile::tempdir().unwrap();
            let mut output = builder(directory.path());
            output.selector_spool = Some(BufWriter::new(
                OpenOptions::new().write(true).open("/dev/null").unwrap(),
            ));
            output.selector_spool_bytes = 1;
            assert!(output.finish_selector_pages().is_err());
        }

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(output.write_selector_pages(&[]).is_err());
        fs::write(&output.selector_sorted_path, [1, 0]).unwrap();
        assert!(output.write_selector_pages(&[]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output
            .selector_key_ordinal(selector_code("x"))
            .unwrap();
        fs::write(&output.selector_sorted_path, selector_record(1, 0, 0)).unwrap();
        assert!(output.write_selector_pages(&[]).is_err());

        let oversized = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        assert!(selector_ref_capacity(&selector_code_with_type(
            oversized.clone(),
            "12345",
        ))
        .is_err());
        assert!(output
            .selector_key_ordinal(selector_code(oversized.clone()))
            .is_err());
        assert!(selector_ref_capacity(&HospitalPriceSelectorKey::PayerPlan {
            payer_name: oversized.clone(),
            plan_name: "plan".to_owned(),
        })
        .is_err());
        assert!(selector_ref_capacity(&HospitalPriceSelectorKey::PayerPlan {
            payer_name: "payer".to_owned(),
            plan_name: oversized.clone(),
        })
        .is_err());
        output.selector_keys = vec![selector_code(oversized)];
        fs::write(&output.selector_sorted_path, selector_record(1, 0, 0)).unwrap();
        assert!(output.write_selector_pages(&[1]).is_err());

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("selector");
        assert!(count_selector_pages(&path, &[], 0, 0).is_err());
        let oversized = "x".repeat(
            crate::hospital_price_selector_block::HOSPITAL_PRICE_SELECTOR_BLOCK_MAX_KEY_BYTES + 1,
        );
        fs::write(&path, selector_record(1, 0, 0)).unwrap();
        assert!(count_selector_pages(
            &path,
            &[selector_code(oversized)],
            1,
            0,
        )
        .is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fail_next_sink_write(&mut output.sinks[2]);
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fs::write(output.selector_sort_directory.join("keep"), b"x").unwrap();
        assert!(output.finish_selector_pages().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = builder(directory.path());
        output.service(0, &service()).unwrap();
        output.charge(0, 0, &charge("1")).unwrap();
        output.payer(0, 0, &payer("1")).unwrap();
        fail_next_sink_write(&mut output.sinks[0]);
        assert!(output.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        fail_next_sink_write(&mut output.sinks[1]);
        output.fact_rows.push(packed_fact(0));
        assert!(output.finish().is_err());

        let directory = tempfile::tempdir().unwrap();
        let mut output = prepared_builder(directory.path());
        output
            .write_selector_ref(selector_code("12345"), 99)
            .unwrap();
        assert!(output.finish().is_err());
    }
