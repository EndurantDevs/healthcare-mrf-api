pub struct RateScheduleObserver {
    schedules_by_provider: Vec<ScheduleAccumulator>,
    seen_provider: Vec<bool>,
    rate_occurrence_count: u64,
}

impl RateScheduleObserver {
    pub fn new(provider_set_count: usize) -> io::Result<Self> {
        if provider_set_count > u32::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "rate-schedule provider-set count exceeds the u32 projection contract",
            ));
        }
        let mut schedules_by_provider = Vec::new();
        if let Err(error) = schedules_by_provider.try_reserve_exact(provider_set_count) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("unable to reserve rate-schedule provider accumulators: {error}"),
            ));
        }
        schedules_by_provider.resize_with(provider_set_count, ScheduleAccumulator::new);
        let mut seen_provider = Vec::new();
        if let Err(error) = seen_provider.try_reserve_exact(provider_set_count) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("unable to reserve rate-schedule coverage vector: {error}"),
            ));
        }
        seen_provider.resize(provider_set_count, false);
        Ok(Self {
            schedules_by_provider,
            seen_provider,
            rate_occurrence_count: 0,
        })
    }

    pub fn observe(&mut self, occurrence: RateScheduleOccurrence) -> io::Result<()> {
        let provider_index = occurrence.provider_set_key as usize;
        let provider_set_count = self.schedules_by_provider.len();
        let Some(provider_schedule) = self.schedules_by_provider.get_mut(provider_index) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "rate-schedule provider_set_key {} is outside charged provider-set count {}",
                    occurrence.provider_set_key, provider_set_count,
                ),
            ));
        };
        provider_schedule.observe(occurrence.schedule_tuple())?;
        self.seen_provider[provider_index] = true;
        self.rate_occurrence_count = checked_add(
            self.rate_occurrence_count,
            1,
            "rate-schedule total occurrence count",
        )?;
        Ok(())
    }

    pub fn finish(self) -> io::Result<RateScheduleObserveSummary> {
        if let Some(missing_provider) = self.seen_provider.iter().position(|seen| !*seen) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "rate-schedule observe input is missing provider_set_key {missing_provider}"
                ),
            ));
        }
        let provider_set_count = self.schedules_by_provider.len() as u64;
        let Some(provider_accumulator_bytes) = self
            .schedules_by_provider
            .capacity()
            .checked_mul(size_of::<ScheduleAccumulator>())
            .and_then(|value| value.checked_add(self.seen_provider.capacity().div_ceil(8)))
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rate-schedule accumulator byte count overflows",
            ));
        };
        let provider_accumulator_bytes = usize_to_u64(
            provider_accumulator_bytes,
            "rate-schedule accumulator byte count",
        )?;
        let mut schedule_classes = HashMap::<[u8; 32], ScheduleClass>::new();
        if let Err(error) = schedule_classes.try_reserve(provider_set_count as usize) {
            return Err(io::Error::new(
                io::ErrorKind::OutOfMemory,
                format!("unable to reserve rate-schedule digest index: {error}"),
            ));
        }
        let mut direct_schedule_body_bytes = 0u64;
        for provider_schedule in self.schedules_by_provider {
            let finished = provider_schedule.finish()?;
            direct_schedule_body_bytes = checked_add(
                direct_schedule_body_bytes,
                finished.body_bytes,
                "direct rate-schedule body byte count",
            )?;
            if let Some(existing) = schedule_classes.get(&finished.digest) {
                if *existing != ScheduleClass::from(finished) {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "rate-schedule SHA-256 collision has incompatible schedule accounting",
                    ));
                }
            } else {
                schedule_classes.insert(finished.digest, ScheduleClass::from(finished));
            }
        }

        let distinct_schedule_count = schedule_classes.len() as u64;
        let mut unique_schedule_occurrence_count = 0u64;
        let mut distinct_schedule_code_incidence_count = 0u64;
        let mut distinct_schedule_body_bytes = 0u64;
        for schedule_class in schedule_classes.values() {
            unique_schedule_occurrence_count = checked_add(
                unique_schedule_occurrence_count,
                schedule_class.occurrence_count,
                "unique rate-schedule occurrence count",
            )?;
            distinct_schedule_code_incidence_count = checked_add(
                distinct_schedule_code_incidence_count,
                schedule_class.code_incidence_count,
                "distinct rate-schedule code incidence count",
            )?;
            distinct_schedule_body_bytes = checked_add(
                distinct_schedule_body_bytes,
                schedule_class.body_bytes,
                "distinct rate-schedule body byte count",
            )?;
        }

        let mut catalog_digests = schedule_classes.keys().copied().collect::<Vec<_>>();
        catalog_digests.sort_unstable();
        let mut catalog_digest = Sha256::new();
        catalog_digest.update(SCHEDULE_CATALOG_DIGEST_DOMAIN);
        catalog_digest.update(distinct_schedule_count.to_be_bytes());
        for digest in &catalog_digests {
            catalog_digest.update(digest);
        }

        let direct_owner_locator_bytes = checked_mul(
            provider_set_count,
            PACKED_LOCATOR_BYTES,
            "direct rate-schedule locator bytes",
        )?;
        let direct_encoded_bytes = checked_sum(
            [
                DIRECT_HEADER_BYTES,
                direct_owner_locator_bytes,
                direct_schedule_body_bytes,
            ],
            "direct rate-schedule encoded bytes",
        )?;
        let factored_schedule_locator_bytes = checked_mul(
            distinct_schedule_count,
            PACKED_LOCATOR_BYTES,
            "factored rate-schedule locator bytes",
        )?;
        let factored_schedule_digest_bytes = checked_mul(
            distinct_schedule_count,
            SCHEDULE_DIGEST_BYTES,
            "factored rate-schedule digest bytes",
        )?;
        let factored_provider_schedule_map_bytes = checked_mul(
            provider_set_count,
            DENSE_SCHEDULE_KEY_BYTES,
            "factored provider-schedule map bytes",
        )?;
        let factored_schedule_artifact_bytes = checked_sum(
            [
                FACTORED_SCHEDULE_HEADER_BYTES,
                factored_schedule_locator_bytes,
                factored_schedule_digest_bytes,
                distinct_schedule_body_bytes,
            ],
            "factored rate-schedule dictionary bytes",
        )?;
        let factored_provider_map_artifact_bytes = checked_sum(
            [
                FACTORED_PROVIDER_MAP_HEADER_BYTES,
                factored_provider_schedule_map_bytes,
            ],
            "factored provider-schedule map bytes",
        )?;
        let factored_encoded_bytes = checked_add(
            factored_schedule_artifact_bytes,
            factored_provider_map_artifact_bytes,
            "factored rate-schedule encoded bytes",
        )?;
        let direct_projected_physical_bytes = round_up_page(direct_encoded_bytes)?;
        let factored_projected_physical_bytes = checked_add(
            round_up_page(factored_schedule_artifact_bytes)?,
            round_up_page(factored_provider_map_artifact_bytes)?,
            "factored rate-schedule projected physical bytes",
        )?;

        let Some(distinct_schedule_index_estimated_bytes) = schedule_classes
            .capacity()
            .checked_mul(size_of::<[u8; 32]>() + size_of::<ScheduleClass>() + 1)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rate-schedule digest index estimate overflows",
            ));
        };
        let distinct_schedule_index_estimated_bytes = usize_to_u64(
            distinct_schedule_index_estimated_bytes,
            "rate-schedule digest index estimate",
        )?;
        let Some(catalog_digest_vector_bytes) = catalog_digests
            .capacity()
            .checked_mul(size_of::<[u8; 32]>())
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rate-schedule catalog digest vector byte count overflows",
            ));
        };
        let catalog_digest_vector_bytes = usize_to_u64(
            catalog_digest_vector_bytes,
            "rate-schedule catalog digest vector byte count",
        )?;
        let estimated_peak_resident_bytes = checked_sum(
            [
                provider_accumulator_bytes,
                distinct_schedule_index_estimated_bytes,
                catalog_digest_vector_bytes,
            ],
            "rate-schedule estimated peak resident bytes",
        )?;
        let charged_memory_upper_bound_bytes = usize_to_u64(
            rate_schedule_observe_memory_upper_bound_bytes(provider_set_count as usize)?,
            "rate-schedule charged memory upper bound",
        )?;

        Ok(RateScheduleObserveSummary {
            enabled: true,
            format: "ptg2_rate_schedule_observe_v1",
            representation_effect: "observe_only_no_serving_change",
            identity_scope: "snapshot_relative_dense_keys_only",
            digest_contract: "sha256_domain_v1_over_ordered_rle(code_key_u32_be,price_set_key_u32_be,source_key_u32_be,duplicate_multiplicity_u64_be)_plus_exact_counts",
            encoded_projection_contract: "packed_schedule_projection_v1:direct(header20+locator12_per_provider+canonical_rle_bodies);factored(schedule_header20+digest32_and_locator12_per_schedule+unique_bodies+provider_map_header20+u32_schedule_key_per_provider)",
            physical_projection_contract: "postgres_8k_page_rounded_payload_floor_v1_excludes_heap_tuple_index_toast_wal_and_free_space_overhead",
            provider_set_count_s: provider_set_count,
            distinct_schedule_count_k: distinct_schedule_count,
            rate_occurrence_count_r: self.rate_occurrence_count,
            unique_schedule_occurrence_count_u: unique_schedule_occurrence_count,
            distinct_schedule_code_incidence_count_i: distinct_schedule_code_incidence_count,
            weighted_reuse_numerator_r: self.rate_occurrence_count,
            weighted_reuse_denominator_u: unique_schedule_occurrence_count,
            weighted_reuse_r_over_u: if unique_schedule_occurrence_count == 0 {
                None
            } else {
                Some(self.rate_occurrence_count as f64 / unique_schedule_occurrence_count as f64)
            },
            schedule_catalog_digest_sha256: hex_digest(catalog_digest.finalize().into()),
            schedule_catalog_digest_sort_count: distinct_schedule_count,
            catalog_digest_in_memory_sort: true,
            direct_schedule_body_bytes,
            distinct_schedule_body_bytes,
            direct_owner_locator_bytes,
            factored_schedule_locator_bytes,
            factored_schedule_digest_bytes,
            factored_provider_schedule_map_bytes,
            direct_encoded_bytes,
            factored_encoded_bytes,
            factoring_reduces_encoded_bytes: factored_encoded_bytes < direct_encoded_bytes,
            factored_encoded_bytes_saved: direct_encoded_bytes
                .saturating_sub(factored_encoded_bytes),
            factored_encoded_bytes_added: factored_encoded_bytes
                .saturating_sub(direct_encoded_bytes),
            direct_projected_physical_bytes,
            factored_projected_physical_bytes,
            postgres_page_bytes: POSTGRES_PAGE_BYTES,
            input_passes: 1,
            occurrence_external_sort: false,
            scratch_bytes_read: 0,
            scratch_bytes_written: 0,
            provider_accumulator_bytes,
            distinct_schedule_index_estimated_bytes,
            catalog_digest_vector_bytes,
            estimated_peak_resident_bytes,
            charged_memory_upper_bound_bytes,
            memory_accounting_contract: "fail_closed_512_bytes_per_provider_plus_1mib_fixed_charge_v1;observed_estimate_reports_provider_accumulator_and_bit_vector_capacities_plus_hashmap_key_value_control_estimate_plus_catalog_digest_vector",
        })
    }
}

fn uvarint_encoded_len(mut value: u64) -> u64 {
    let mut bytes = 1u64;
    while value >= 0x80 {
        value >>= 7;
        bytes += 1;
    }
    bytes
}

fn checked_add(left: u64, right: u64, field: &str) -> io::Result<u64> {
    match left.checked_add(right) {
        Some(value) => Ok(value),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} overflows u64"),
        )),
    }
}

fn checked_mul(left: u64, right: u64, field: &str) -> io::Result<u64> {
    match left.checked_mul(right) {
        Some(value) => Ok(value),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} overflows u64"),
        )),
    }
}

fn checked_sum<const N: usize>(values: [u64; N], field: &str) -> io::Result<u64> {
    values
        .into_iter()
        .try_fold(0u64, |sum, value| checked_add(sum, value, field))
}

fn round_up_page(value: u64) -> io::Result<u64> {
    let pages = match value.checked_add(POSTGRES_PAGE_BYTES - 1) {
        Some(value) => value / POSTGRES_PAGE_BYTES,
        None => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rate-schedule physical page projection overflows",
            ));
        }
    };
    checked_mul(
        pages,
        POSTGRES_PAGE_BYTES,
        "rate-schedule physical page projection",
    )
}

fn usize_to_u64(value: usize, field: &str) -> io::Result<u64> {
    match u64::try_from(value) {
        Ok(value) => Ok(value),
        Err(_) => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{field} exceeds u64"),
        )),
    }
}

fn hex_digest(digest: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(64);
    for byte in digest {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
