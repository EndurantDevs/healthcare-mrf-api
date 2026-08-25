#[cfg(test)]
mod tests {
    use super::*;

    fn occurrence(provider: u32, code: u32, price: u32, source: u32) -> RateScheduleOccurrence {
        RateScheduleOccurrence {
            provider_set_key: provider,
            code_key: code,
            price_set_key: price,
            source_key: source,
        }
    }

    #[test]
    fn exact_reuse_metrics_and_projection_count_duplicate_multiplicity() {
        let mut observer = RateScheduleObserver::new(3).unwrap();
        for row in [
            occurrence(0, 1, 2, 0),
            occurrence(1, 1, 2, 0),
            occurrence(2, 1, 2, 0),
            occurrence(2, 1, 2, 0),
            occurrence(0, 1, 3, 0),
            occurrence(1, 1, 3, 0),
        ] {
            observer.observe(row).unwrap();
        }
        let summary = observer.finish().unwrap();

        assert_eq!(summary.provider_set_count_s, 3);
        assert_eq!(summary.distinct_schedule_count_k, 2);
        assert_eq!(summary.rate_occurrence_count_r, 6);
        assert_eq!(summary.unique_schedule_occurrence_count_u, 4);
        assert_eq!(summary.distinct_schedule_code_incidence_count_i, 2);
        assert_eq!(summary.weighted_reuse_r_over_u, Some(1.5));
        assert_eq!(summary.direct_schedule_body_bytes, 29);
        assert_eq!(summary.distinct_schedule_body_bytes, 18);
        assert_eq!(summary.direct_encoded_bytes, 85);
        assert_eq!(summary.factored_encoded_bytes, 158);
        assert!(!summary.factoring_reduces_encoded_bytes);
        assert_eq!(summary.factored_encoded_bytes_added, 73);
        assert_eq!(summary.direct_projected_physical_bytes, 8 * 1024);
        assert_eq!(summary.factored_projected_physical_bytes, 16 * 1024);
        assert!(!summary.occurrence_external_sort);
        assert_eq!(summary.scratch_bytes_written, 0);
        assert!(summary.charged_memory_upper_bound_bytes > summary.estimated_peak_resident_bytes);
    }

    #[test]
    fn digest_distinguishes_code_price_source_and_duplicate_multiplicity() {
        let variants = [
            vec![occurrence(0, 1, 2, 3)],
            vec![occurrence(0, 9, 2, 3)],
            vec![occurrence(0, 1, 8, 3)],
            vec![occurrence(0, 1, 2, 7)],
            vec![occurrence(0, 1, 2, 3), occurrence(0, 1, 2, 3)],
        ];
        let mut catalog_digests = Vec::new();
        for rows in variants {
            let mut observer = RateScheduleObserver::new(1).unwrap();
            for row in rows {
                observer.observe(row).unwrap();
            }
            catalog_digests.push(observer.finish().unwrap().schedule_catalog_digest_sha256);
        }
        catalog_digests.sort_unstable();
        catalog_digests.dedup();
        assert_eq!(catalog_digests.len(), 5);
    }

    #[test]
    fn projection_reports_a_win_only_when_schedule_reuse_pays_for_indirection() {
        let mut observer = RateScheduleObserver::new(100).unwrap();
        for provider_set_key in 0..100 {
            observer
                .observe(occurrence(provider_set_key, 1, 2, 0))
                .unwrap();
        }
        let summary = observer.finish().unwrap();
        assert_eq!(summary.distinct_schedule_count_k, 1);
        assert_eq!(summary.weighted_reuse_r_over_u, Some(100.0));
        assert!(summary.factoring_reduces_encoded_bytes);
        assert!(summary.factored_encoded_bytes_saved > 0);
        assert_eq!(summary.factored_encoded_bytes_added, 0);
    }

    #[test]
    fn memory_charge_is_deterministic_and_linear_in_provider_sets() {
        assert_eq!(
            rate_schedule_observe_memory_upper_bound_bytes(0).unwrap(),
            CHARGED_FIXED_BYTES
        );
        assert_eq!(
            rate_schedule_observe_memory_upper_bound_bytes(100).unwrap(),
            CHARGED_FIXED_BYTES + 100 * CHARGED_BYTES_PER_PROVIDER_SET
        );
    }

    #[test]
    fn equivalent_provider_schedules_share_one_digest_independent_of_interleave() {
        let mut observer = RateScheduleObserver::new(2).unwrap();
        for row in [
            occurrence(1, 1, 5, 0),
            occurrence(0, 1, 5, 0),
            occurrence(0, 2, 6, 1),
            occurrence(1, 2, 6, 1),
        ] {
            observer.observe(row).unwrap();
        }
        let summary = observer.finish().unwrap();
        assert_eq!(summary.distinct_schedule_count_k, 1);
        assert_eq!(summary.rate_occurrence_count_r, 4);
        assert_eq!(summary.unique_schedule_occurrence_count_u, 2);
        assert_eq!(summary.distinct_schedule_code_incidence_count_i, 2);
        assert_eq!(summary.weighted_reuse_r_over_u, Some(2.0));
    }

    #[test]
    fn rejects_out_of_order_or_incomplete_provider_streams() {
        let mut out_of_order = RateScheduleObserver::new(1).unwrap();
        out_of_order.observe(occurrence(0, 2, 1, 0)).unwrap();
        let error = out_of_order.observe(occurrence(0, 1, 1, 0)).unwrap_err();
        assert!(error.to_string().contains("must be ordered"));

        let mut incomplete = RateScheduleObserver::new(2).unwrap();
        incomplete.observe(occurrence(0, 1, 1, 0)).unwrap();
        let error = incomplete.finish().unwrap_err();
        assert!(error.to_string().contains("missing provider_set_key 1"));
    }

    #[test]
    fn empty_schedule_and_projection_boundaries_are_explicit() {
        let empty = RateScheduleObserver::new(0).unwrap().finish().unwrap();
        assert_eq!(empty.provider_set_count_s, 0);
        assert_eq!(empty.distinct_schedule_count_k, 0);
        assert_eq!(empty.weighted_reuse_r_over_u, None);
        assert_eq!(empty.direct_encoded_bytes, DIRECT_HEADER_BYTES);
        assert_eq!(empty.direct_projected_physical_bytes, POSTGRES_PAGE_BYTES);

        assert_eq!(uvarint_encoded_len(0), 1);
        assert_eq!(uvarint_encoded_len(0x7f), 1);
        assert_eq!(uvarint_encoded_len(0x80), 2);
        assert_eq!(uvarint_encoded_len(u64::MAX), 10);
        assert!(checked_add(u64::MAX, 1, "addition").is_err());
        assert!(checked_mul(u64::MAX, 2, "multiplication").is_err());
        assert!(checked_sum([u64::MAX, 1], "sum").is_err());
        assert!(round_up_page(u64::MAX).is_err());
        assert_eq!(usize_to_u64(7, "usize").unwrap(), 7);
        assert_eq!(hex_digest([0xab; 32]), "ab".repeat(32));

        if usize::BITS > 32 {
            assert!(RateScheduleObserver::new(u32::MAX as usize + 1)
                .err()
                .unwrap()
                .to_string()
                .contains("u32 projection"));
        }
        assert!(rate_schedule_observe_memory_upper_bound_bytes(usize::MAX).is_err());
    }

    #[test]
    fn observer_and_accumulator_overflow_paths_fail_closed() {
        let mut outside = RateScheduleObserver::new(1).unwrap();
        assert!(outside.observe(occurrence(1, 1, 1, 1)).is_err());

        let mut total_overflow = RateScheduleObserver::new(1).unwrap();
        total_overflow.rate_occurrence_count = u64::MAX;
        assert!(total_overflow.observe(occurrence(0, 1, 1, 1)).is_err());

        let tuple = ScheduleTuple {
            code_key: 1,
            price_set_key: 2,
            source_key: 3,
        };
        let mut duplicate_overflow = ScheduleAccumulator::new();
        duplicate_overflow.current_tuple = Some(tuple);
        duplicate_overflow.current_multiplicity = u64::MAX;
        assert!(duplicate_overflow.observe(tuple).is_err());

        let mut occurrence_overflow = ScheduleAccumulator::new();
        occurrence_overflow.occurrence_count = u64::MAX;
        assert!(occurrence_overflow.observe(tuple).is_err());

        let mut delta_underflow = ScheduleAccumulator::new();
        delta_underflow.current_tuple = Some(tuple);
        delta_underflow.current_multiplicity = 1;
        delta_underflow.previous_encoded_code = 2;
        assert!(delta_underflow.flush_current_tuple().is_err());

        let mut body_overflow = ScheduleAccumulator::new();
        body_overflow.current_tuple = Some(tuple);
        body_overflow.current_multiplicity = 1;
        body_overflow.body_record_bytes = u64::MAX;
        assert!(body_overflow.flush_current_tuple().is_err());

        let mut run_overflow = ScheduleAccumulator::new();
        run_overflow.current_tuple = Some(tuple);
        run_overflow.current_multiplicity = 1;
        run_overflow.run_count = u64::MAX;
        assert!(run_overflow.flush_current_tuple().is_err());

        let mut incidence_overflow = ScheduleAccumulator::new();
        incidence_overflow.code_incidence_count = u64::MAX;
        assert!(incidence_overflow.observe(tuple).is_err());

        let mut empty_flush = ScheduleAccumulator::new();
        empty_flush.flush_current_tuple().unwrap();

        let mut finished_body_overflow = ScheduleAccumulator::new();
        finished_body_overflow.body_record_bytes = u64::MAX;
        assert!(finished_body_overflow.finish().is_err());

        let mut first = ScheduleAccumulator::new();
        first.body_record_bytes = u64::MAX - 3;
        let second = ScheduleAccumulator::new();
        let direct_overflow = RateScheduleObserver {
            schedules_by_provider: vec![first, second],
            seen_provider: vec![true, true],
            rate_occurrence_count: 0,
        };
        assert!(direct_overflow.finish().is_err());
    }
}
