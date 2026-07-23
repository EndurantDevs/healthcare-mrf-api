//! Observe-only exact rate-schedule reuse counters.
//!
//! Assigned serving rows are already ordered by
//! `(code_key, provider_set_key, price_set_key, source_key)`.  Therefore each
//! provider set sees its own canonical tuple stream in sorted order.  Keeping
//! one bounded accumulator per charged provider set lets us measure exact
//! snapshot-local schedule reuse without another occurrence sort or scratch
//! file.  These counters do not select or publish a serving representation.

use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::io;
use std::mem::size_of;

const SCHEDULE_DIGEST_DOMAIN: &[u8] = b"PTG2_RATE_SCHEDULE\x01";
const SCHEDULE_CATALOG_DIGEST_DOMAIN: &[u8] = b"PTG2_RATE_SCHEDULE_CATALOG\x01";
const DIRECT_HEADER_BYTES: u64 = 20;
const FACTORED_SCHEDULE_HEADER_BYTES: u64 = 20;
const FACTORED_PROVIDER_MAP_HEADER_BYTES: u64 = 20;
const PACKED_LOCATOR_BYTES: u64 = 12;
const SCHEDULE_DIGEST_BYTES: u64 = 32;
const DENSE_SCHEDULE_KEY_BYTES: u64 = 4;
const POSTGRES_PAGE_BYTES: u64 = 8 * 1024;
const CHARGED_BYTES_PER_PROVIDER_SET: usize = 512;
const CHARGED_FIXED_BYTES: usize = 1024 * 1024;

/// Return the fail-closed resident-memory charge used before enabling observe mode.
///
/// The charge covers the SHA-256 accumulator, coverage bit, worst-case distinct
/// schedule index, and K-sized catalog digest vector with allocator headroom.
pub fn rate_schedule_observe_memory_upper_bound_bytes(
    provider_set_count: usize,
) -> io::Result<usize> {
    let Some(bytes) = provider_set_count
        .checked_mul(CHARGED_BYTES_PER_PROVIDER_SET)
        .and_then(|bytes| bytes.checked_add(CHARGED_FIXED_BYTES))
    else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "rate-schedule observe memory charge overflows usize",
        ));
    };
    Ok(bytes)
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RateScheduleOccurrence {
    pub provider_set_key: u32,
    pub code_key: u32,
    pub price_set_key: u32,
    pub source_key: u32,
}

impl RateScheduleOccurrence {
    fn schedule_tuple(self) -> ScheduleTuple {
        ScheduleTuple {
            code_key: self.code_key,
            price_set_key: self.price_set_key,
            source_key: self.source_key,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ScheduleTuple {
    code_key: u32,
    price_set_key: u32,
    source_key: u32,
}

struct ScheduleAccumulator {
    digest: Sha256,
    current_tuple: Option<ScheduleTuple>,
    current_multiplicity: u64,
    previous_encoded_code: u32,
    occurrence_count: u64,
    run_count: u64,
    code_incidence_count: u64,
    body_record_bytes: u64,
}

impl ScheduleAccumulator {
    fn new() -> Self {
        let mut digest = Sha256::new();
        digest.update(SCHEDULE_DIGEST_DOMAIN);
        Self {
            digest,
            current_tuple: None,
            current_multiplicity: 0,
            previous_encoded_code: 0,
            occurrence_count: 0,
            run_count: 0,
            code_incidence_count: 0,
            body_record_bytes: 0,
        }
    }

    fn observe(&mut self, schedule_tuple: ScheduleTuple) -> io::Result<()> {
        let mut starts_new_code = true;
        if let Some(current_tuple) = self.current_tuple {
            if schedule_tuple < current_tuple {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "rate-schedule tuples must be ordered within each provider set",
                ));
            }
            if schedule_tuple == current_tuple {
                self.current_multiplicity = checked_add(
                    self.current_multiplicity,
                    1,
                    "rate-schedule duplicate multiplicity",
                )?;
                self.occurrence_count =
                    checked_add(self.occurrence_count, 1, "rate-schedule occurrence count")?;
                return Ok(());
            }
            starts_new_code = current_tuple.code_key != schedule_tuple.code_key;
            self.flush_current_tuple()?;
        }

        if starts_new_code {
            self.code_incidence_count = checked_add(
                self.code_incidence_count,
                1,
                "rate-schedule code incidence count",
            )?;
        }
        self.current_tuple = Some(schedule_tuple);
        self.current_multiplicity = 1;
        self.occurrence_count =
            checked_add(self.occurrence_count, 1, "rate-schedule occurrence count")?;
        Ok(())
    }

    fn flush_current_tuple(&mut self) -> io::Result<()> {
        let Some(schedule_tuple) = self.current_tuple.take() else {
            return Ok(());
        };
        let Some(code_delta) = schedule_tuple
            .code_key
            .checked_sub(self.previous_encoded_code)
        else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "rate-schedule code delta underflowed",
            ));
        };
        // Four uvarints are at most 40 bytes in total, so this sum cannot
        // overflow usize on any supported target.
        let record_bytes = uvarint_encoded_len(u64::from(code_delta))
            + uvarint_encoded_len(u64::from(schedule_tuple.price_set_key))
            + uvarint_encoded_len(u64::from(schedule_tuple.source_key))
            + uvarint_encoded_len(self.current_multiplicity);
        self.body_record_bytes = checked_add(
            self.body_record_bytes,
            record_bytes,
            "rate-schedule body byte count",
        )?;
        self.run_count = checked_add(self.run_count, 1, "rate-schedule run count")?;

        // Fixed-width, tagged fields make this digest unambiguous.  Repeated
        // identical tuples are represented once with their exact multiplicity.
        let mut digest_record = [0u8; 21];
        digest_record[0] = 1;
        digest_record[1..5].copy_from_slice(&schedule_tuple.code_key.to_be_bytes());
        digest_record[5..9].copy_from_slice(&schedule_tuple.price_set_key.to_be_bytes());
        digest_record[9..13].copy_from_slice(&schedule_tuple.source_key.to_be_bytes());
        digest_record[13..21].copy_from_slice(&self.current_multiplicity.to_be_bytes());
        self.digest.update(digest_record);
        self.previous_encoded_code = schedule_tuple.code_key;
        self.current_multiplicity = 0;
        Ok(())
    }

    fn finish(mut self) -> io::Result<FinishedSchedule> {
        self.flush_current_tuple()?;
        // Three uvarints are at most 30 bytes in total.
        let header_bytes = uvarint_encoded_len(self.run_count)
            + uvarint_encoded_len(self.occurrence_count)
            + uvarint_encoded_len(self.code_incidence_count);
        let body_bytes = checked_add(
            header_bytes,
            self.body_record_bytes,
            "rate-schedule body byte count",
        )?;
        let mut digest_trailer = [0u8; 33];
        digest_trailer[0] = 2;
        digest_trailer[1..9].copy_from_slice(&self.run_count.to_be_bytes());
        digest_trailer[9..17].copy_from_slice(&self.occurrence_count.to_be_bytes());
        digest_trailer[17..25].copy_from_slice(&self.code_incidence_count.to_be_bytes());
        digest_trailer[25..33].copy_from_slice(&body_bytes.to_be_bytes());
        self.digest.update(digest_trailer);
        Ok(FinishedSchedule {
            digest: self.digest.finalize().into(),
            occurrence_count: self.occurrence_count,
            run_count: self.run_count,
            code_incidence_count: self.code_incidence_count,
            body_bytes,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FinishedSchedule {
    digest: [u8; 32],
    occurrence_count: u64,
    run_count: u64,
    code_incidence_count: u64,
    body_bytes: u64,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ScheduleClass {
    occurrence_count: u64,
    run_count: u64,
    code_incidence_count: u64,
    body_bytes: u64,
}

impl From<FinishedSchedule> for ScheduleClass {
    fn from(schedule: FinishedSchedule) -> Self {
        Self {
            occurrence_count: schedule.occurrence_count,
            run_count: schedule.run_count,
            code_incidence_count: schedule.code_incidence_count,
            body_bytes: schedule.body_bytes,
        }
    }
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct RateScheduleObserveSummary {
    pub enabled: bool,
    pub format: &'static str,
    pub representation_effect: &'static str,
    pub identity_scope: &'static str,
    pub digest_contract: &'static str,
    pub encoded_projection_contract: &'static str,
    pub physical_projection_contract: &'static str,
    pub provider_set_count_s: u64,
    pub distinct_schedule_count_k: u64,
    pub rate_occurrence_count_r: u64,
    pub unique_schedule_occurrence_count_u: u64,
    pub distinct_schedule_code_incidence_count_i: u64,
    pub weighted_reuse_numerator_r: u64,
    pub weighted_reuse_denominator_u: u64,
    pub weighted_reuse_r_over_u: Option<f64>,
    pub schedule_catalog_digest_sha256: String,
    pub schedule_catalog_digest_sort_count: u64,
    pub catalog_digest_in_memory_sort: bool,
    pub direct_schedule_body_bytes: u64,
    pub distinct_schedule_body_bytes: u64,
    pub direct_owner_locator_bytes: u64,
    pub factored_schedule_locator_bytes: u64,
    pub factored_schedule_digest_bytes: u64,
    pub factored_provider_schedule_map_bytes: u64,
    pub direct_encoded_bytes: u64,
    pub factored_encoded_bytes: u64,
    pub factoring_reduces_encoded_bytes: bool,
    pub factored_encoded_bytes_saved: u64,
    pub factored_encoded_bytes_added: u64,
    pub direct_projected_physical_bytes: u64,
    pub factored_projected_physical_bytes: u64,
    pub postgres_page_bytes: u64,
    pub input_passes: u8,
    pub occurrence_external_sort: bool,
    pub scratch_bytes_read: u64,
    pub scratch_bytes_written: u64,
    pub provider_accumulator_bytes: u64,
    pub distinct_schedule_index_estimated_bytes: u64,
    pub catalog_digest_vector_bytes: u64,
    pub estimated_peak_resident_bytes: u64,
    pub charged_memory_upper_bound_bytes: u64,
    pub memory_accounting_contract: &'static str,
}

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
