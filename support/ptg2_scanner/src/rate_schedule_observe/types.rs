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
