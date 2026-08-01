//! Bounded semantic decoding for already-admitted UHC retained artifacts.
//!
//! This module deliberately has no path or manifest reader.  The admission
//! layer owns stable file descriptors, manifest verification, and record
//! framing.  It implements AdmittedRangeSource and supplies one verified JSON
//! object at a time.  Semantic decoding therefore cannot accidentally reopen a
//! mutable path or allocate the complete retained artifact.

use crate::uhc_retained::{
    open_verified_uhc_replay, UHCVerifiedReplayRequest, UHCVerifiedRetainedSource,
};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use rayon::ThreadPoolBuilder;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::mem::size_of;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

pub const UHC_SEMANTIC_FACT_CONTRACT_ID: &str = "healthporta.uhc.semantic-facts.v3";
pub const UHC_SEMANTIC_FACT_CONTRACT_VERSION: u64 = 3;
pub const UHC_SEMANTIC_COPY_FORMAT_ID: &str = "postgres-copy-binary-uhc-fact-evidence-v2";
pub const UHC_SEMANTIC_SOURCE_ID: &str = "pdfhir_2754e999dd691175821ec26e";
pub const UHC_SEMANTIC_COPY_COLUMN_COUNT: i16 = 11;

const UHC_PROVIDER_QUARANTINE_CONTRACT_ID: &str = "healthporta.uhc.provider-quarantine.v1";
const UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM: &str = "invalid_npi_checksum";
const UHC_PROVIDER_QUARANTINE_MAX_COUNT: u64 = 32;
const UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR: u64 = 1_000_000;

const COPY_ROW_FACT_BLOCK: i16 = 1;
const COPY_ROW_NPI_EVIDENCE: i16 = 2;
const COPY_BUFFER_BYTES: usize = 1024 * 1024;
const QUARANTINE_IDENTITY_BUFFER_BYTES: usize = 8 * 1024;
const WORKER_FIXED_BYTES: usize = 2 * 1024 * 1024 + QUARANTINE_IDENTITY_BUFFER_BYTES;
const RECORD_EXPANSION_FACTOR: usize = 8;
const RECORD_FIXED_BYTES: usize = 256 * 1024;
const MAX_WORKERS: usize = 64;
const MIN_EVIDENCE_BUFFER_BYTES: usize = 64 * 1024;
const MAX_RANGE_COUNT: usize = 256;
const PENDING_QUARANTINE_IDENTITY_BYTES: usize = MAX_RANGE_COUNT * QUARANTINE_IDENTITY_BUFFER_BYTES;

fn invalid(message: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message.into())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

fn sha256(value: &[u8]) -> String {
    hex_digest(Sha256::digest(value).as_slice())
}

fn update_framed(digest: &mut Sha256, value: &[u8]) {
    digest.update((value.len() as u64).to_be_bytes());
    digest.update(value);
}

fn require_sha256(value: &str, field: &str) -> io::Result<()> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(invalid(format!("{field} must be a lowercase SHA-256")));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UhcCollectionKind {
    ProviderMembership,
    PlanReference,
}

impl UhcCollectionKind {
    pub fn fact_type(self) -> &'static str {
        match self {
            Self::ProviderMembership => "ProviderMembershipRecord",
            Self::PlanReference => "PlanReferenceRecord",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedSemanticLineage {
    pub artifact_sha256: String,
    pub manifest_sha256: String,
    pub range_set_sha256: String,
    pub source_file_id: String,
    pub source_binding_id: String,
    pub collection_kind: UhcCollectionKind,
}

impl AdmittedSemanticLineage {
    fn validate(&self) -> io::Result<()> {
        require_sha256(&self.artifact_sha256, "artifact SHA-256")?;
        require_sha256(&self.manifest_sha256, "manifest SHA-256")?;
        require_sha256(&self.range_set_sha256, "range-set SHA-256")?;
        require_sha256(&self.source_file_id, "source file ID")?;
        if self.source_binding_id.is_empty()
            || self.source_binding_id.len() > 256
            || self.source_binding_id != self.source_binding_id.trim()
            || self.source_binding_id.chars().any(char::is_control)
        {
            return Err(invalid("source binding ID is invalid"));
        }
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdmittedSemanticRange {
    pub range_ordinal: u64,
    pub record_start: u64,
    pub record_count: u64,
    pub raw_byte_count: u64,
    pub raw_sha256: String,
    pub canonical_sha256: String,
}

/// Stable record access supplied by the UHC retained-admission module.
///
/// Implementations must keep the admitted file descriptor and identity proof
/// alive for the complete visit.  They must verify the selected raw range and
/// call the visitor once for each framed JSON object in source order.
pub trait AdmittedRangeSource: Sync {
    fn lineage(&self) -> &AdmittedSemanticLineage;
    fn ranges(&self) -> &[AdmittedSemanticRange];
    fn visit_verified_records(
        &self,
        range: &AdmittedSemanticRange,
        visitor: &mut dyn FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()>;
}

pub struct UhcSemanticReplaySource {
    retained: UHCVerifiedRetainedSource,
    lineage: AdmittedSemanticLineage,
    ranges: Vec<AdmittedSemanticRange>,
}

impl UhcSemanticReplaySource {
    pub fn open(
        request: &UHCVerifiedReplayRequest,
        lineage: AdmittedSemanticLineage,
    ) -> io::Result<Self> {
        let retained = open_verified_uhc_replay(request)?;
        if lineage.artifact_sha256 != request.expected_artifact_sha256
            || lineage.manifest_sha256 != request.expected_manifest_sha256
            || lineage.range_set_sha256 != request.expected_range_set_sha256
        {
            return Err(invalid(
                "UHC semantic lineage does not match verified retained replay",
            ));
        }
        lineage.validate()?;
        let ranges = retained
            .manifest()
            .ranges
            .iter()
            .map(|range| AdmittedSemanticRange {
                range_ordinal: range.range_ordinal,
                record_start: range.record_start,
                record_count: range.record_count,
                raw_byte_count: range.raw_byte_count,
                raw_sha256: range.raw_sha256.clone(),
                canonical_sha256: range.canonical_sha256.clone(),
            })
            .collect();
        Ok(Self {
            retained,
            lineage,
            ranges,
        })
    }
}

impl AdmittedRangeSource for UhcSemanticReplaySource {
    fn lineage(&self) -> &AdmittedSemanticLineage {
        &self.lineage
    }

    fn ranges(&self) -> &[AdmittedSemanticRange] {
        &self.ranges
    }

    fn visit_verified_records(
        &self,
        range: &AdmittedSemanticRange,
        visitor: &mut dyn FnMut(u64, &[u8]) -> io::Result<()>,
    ) -> io::Result<()> {
        #[cfg(target_pointer_width = "64")]
        let ordinal = range.range_ordinal as usize;
        #[cfg(not(target_pointer_width = "64"))]
        let ordinal = usize::try_from(range.range_ordinal)
            .map_err(|_| invalid("UHC semantic range ordinal is too large"))?;
        if self.ranges.get(ordinal) != Some(range) {
            return Err(invalid(
                "UHC semantic range does not belong to retained replay",
            ));
        }
        self.retained.visit_verified_range_records(ordinal, visitor)
    }
}

#[derive(Clone, Debug)]
pub struct SemanticMemoryBudget {
    pub worker_count: usize,
    pub per_worker_bytes: usize,
    pub total_bytes: usize,
    pub max_record_bytes: usize,
    pub evidence_buffer_bytes: usize,
}

impl Default for SemanticMemoryBudget {
    fn default() -> Self {
        Self {
            worker_count: 4,
            per_worker_bytes: 64 * 1024 * 1024,
            total_bytes: 384 * 1024 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
            evidence_buffer_bytes: 8 * 1024 * 1024,
        }
    }
}

impl SemanticMemoryBudget {
    pub fn validate(&self) -> io::Result<()> {
        if !(1..=MAX_WORKERS).contains(&self.worker_count) {
            return Err(invalid("UHC semantic worker count must be in 1..=64"));
        }
        if self.max_record_bytes == 0 {
            return Err(invalid("UHC semantic max record bytes must be positive"));
        }
        if self.evidence_buffer_bytes < MIN_EVIDENCE_BUFFER_BYTES {
            return Err(invalid(
                "UHC semantic evidence buffer must be at least 64 KiB",
            ));
        }
        let record_reservation = record_reservation(self.max_record_bytes)?;
        let Some(worker_with_record) = WORKER_FIXED_BYTES.checked_add(record_reservation) else {
            return Err(invalid("UHC semantic worker budget overflowed"));
        };
        let Some(required_worker) = worker_with_record.checked_add(self.evidence_buffer_bytes)
        else {
            return Err(invalid("UHC semantic worker budget overflowed"));
        };
        if required_worker > self.per_worker_bytes {
            return Err(invalid(format!(
                "UHC semantic per-worker budget is too small: {required_worker} bytes required"
            )));
        }
        let Some(worker_total) = self.per_worker_bytes.checked_mul(self.worker_count) else {
            return Err(invalid("UHC semantic total budget overflowed"));
        };
        let Some(required_total) = worker_total
            .checked_add(COPY_BUFFER_BYTES)
            .and_then(|total| total.checked_add(PENDING_QUARANTINE_IDENTITY_BYTES))
        else {
            return Err(invalid("UHC semantic total budget overflowed"));
        };
        if required_total > self.total_bytes {
            return Err(invalid(format!(
                "UHC semantic total budget is too small: {required_total} bytes required"
            )));
        }
        Ok(())
    }
}

fn record_reservation(record_bytes: usize) -> io::Result<usize> {
    let Some(expanded) = record_bytes.checked_mul(RECORD_EXPANSION_FACTOR) else {
        return Err(invalid("UHC semantic record memory reservation overflowed"));
    };
    match expanded.checked_add(RECORD_FIXED_BYTES) {
        Some(reserved) => Ok(reserved),
        None => Err(invalid("UHC semantic record memory reservation overflowed")),
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderName {
    first: Option<String>,
    middle: Option<String>,
    last: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderAddress {
    address: Option<String>,
    city: Option<String>,
    state: Option<String>,
    zip: Option<String>,
    phone: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderPlan {
    plan_id_type: Option<String>,
    plan_id: Option<String>,
    years: Vec<u16>,
    network_tier: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProviderRecord {
    #[serde(rename = "type")]
    provider_type: String,
    npi: String,
    name: Option<ProviderName>,
    facility_name: Option<String>,
    facility_type: Option<Vec<String>>,
    gender: Option<String>,
    accepting: Option<String>,
    addresses: Vec<ProviderAddress>,
    plans: Vec<ProviderPlan>,
    specialty: Option<Vec<String>>,
    last_updated_on: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlanNetwork {
    network_tier: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FormularyEntry {
    drug_tier: Option<String>,
    mail_order: Option<bool>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PlanRecord {
    plan_id_type: Option<String>,
    plan_id: Option<String>,
    years: Vec<u16>,
    marketing_name: Option<String>,
    marketing_url: Option<String>,
    summary_url: Option<String>,
    formulary_url: Option<String>,
    plan_contact: Option<String>,
    network: Option<Vec<PlanNetwork>>,
    formulary: Option<Vec<FormularyEntry>>,
    last_updated_on: Option<String>,
}

fn clean(value: Option<&str>, upper: bool) -> Option<String> {
    let cleaned = value?.replace('\0', "");
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        None
    } else if upper {
        Some(trimmed.to_uppercase())
    } else {
        Some(trimmed.to_owned())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NpiValidity {
    Valid,
    ChecksumInvalid,
    Invalid,
}

fn npi_validity(value: &str) -> NpiValidity {
    if value.len() != 10 || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return NpiValidity::Invalid;
    }
    if !matches!(
        value.parse::<u64>(),
        Ok(parsed) if (1_000_000_000..=2_999_999_999).contains(&parsed)
    ) {
        return NpiValidity::Invalid;
    }
    let digits: Vec<u64> = value.bytes().map(|byte| u64::from(byte - b'0')).collect();
    let mut digit_sum = 24 + digits[9];
    for (index, digit) in digits[..9].iter().enumerate() {
        if index % 2 == 0 {
            let doubled = digit * 2;
            digit_sum += if doubled > 9 { doubled - 9 } else { doubled };
        } else {
            digit_sum += digit;
        }
    }
    if digit_sum.is_multiple_of(10) {
        NpiValidity::Valid
    } else {
        NpiValidity::ChecksumInvalid
    }
}

fn provider_quarantine_limit(provider_count: u64) -> u64 {
    let rate_limit = provider_count / UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR
        + u64::from(!provider_count.is_multiple_of(UHC_PROVIDER_QUARANTINE_RATE_DENOMINATOR));
    UHC_PROVIDER_QUARANTINE_MAX_COUNT.min(rate_limit)
}

fn accepting_code(value: Option<&str>) -> io::Result<Option<&'static str>> {
    let Some(value) = clean(value, true) else {
        return Ok(None);
    };
    let canonical = value
        .split(|character: char| character == '-' || character.is_whitespace())
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    match canonical.as_str() {
        "ACCEPTING" | "ACCEPTING_NEW_PATIENTS" | "YES" => Ok(Some("newpt")),
        "NOT_ACCEPTING" | "NO" | "CLOSED" => Ok(Some("nopt")),
        _ => Err(invalid("UHC accepting status is unsupported")),
    }
}

fn normalized_phone(value: Option<&str>) -> Option<String> {
    let digits: String = clean(value, false)?
        .bytes()
        .filter(|byte| byte.is_ascii_digit())
        .map(char::from)
        .collect();
    (digits.len() == 10).then_some(digits)
}

fn required_years(years: &[u16]) -> io::Result<()> {
    if years.is_empty() || years.iter().any(|year| !(2000..=2100).contains(year)) {
        return Err(invalid("UHC plan years are invalid"));
    }
    Ok(())
}

fn canonical_value_bytes<T: Serialize>(value: &T) -> io::Result<Vec<u8>> {
    let value = match serde_json::to_value(value) {
        Ok(value) => value,
        Err(error) => {
            return Err(invalid(format!(
                "cannot encode UHC semantic value: {error}"
            )))
        }
    };
    Ok(serde_json::to_vec(&value).expect("JSON value serialization is infallible"))
}

fn optional_signature<T: Serialize>(value: &Option<T>) -> io::Result<String> {
    match serde_json::to_string(value) {
        Ok(encoded) => Ok(encoded),
        Err(error) => Err(invalid(error.to_string())),
    }
}

fn sorted_signature<T: Serialize>(values: &Option<Vec<T>>) -> io::Result<String> {
    let mut encoded = match values {
        Some(values) => {
            let mut encoded = Vec::with_capacity(values.len());
            for value in values {
                match serde_json::to_string(value) {
                    Ok(value) => encoded.push(value),
                    Err(error) => return Err(invalid(error.to_string())),
                }
            }
            encoded
        }
        None => return Ok("null".to_owned()),
    };
    encoded.sort_unstable();
    Ok(serde_json::to_string(&encoded).expect("string vector serialization is infallible"))
}

fn address_set_signature(addresses: &[ProviderAddress]) -> io::Result<String> {
    let mut encoded = Vec::with_capacity(addresses.len());
    for address in addresses {
        match serde_json::to_string(address) {
            Ok(address) => encoded.push(address),
            Err(error) => return Err(invalid(error.to_string())),
        }
    }
    encoded.sort_unstable();
    Ok(serde_json::to_string(&encoded).expect("string vector serialization is infallible"))
}

fn fact_identity(
    source_file_id: &str,
    fact_type: &str,
    occurrence_ordinal: u64,
    payload_hash: &str,
) -> String {
    let occurrence = occurrence_ordinal.to_string();
    let mut identity_digest = Sha256::new();
    for part in [
        UHC_SEMANTIC_FACT_CONTRACT_ID,
        source_file_id,
        fact_type,
        occurrence.as_str(),
    ] {
        update_framed(&mut identity_digest, part.as_bytes());
    }
    let fact_id = format!(
        "uhcf-{}",
        &hex_digest(identity_digest.finalize().as_slice())[..48]
    );
    serde_json::to_string(&(fact_type, fact_id, payload_hash))
        .expect("UHC fact identity is serializable")
}

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize)]
pub struct UhcFileCounters {
    pub raw_provider_records: u64,
    pub raw_plan_records: u64,
    pub raw_individual_records: u64,
    pub raw_facility_records: u64,
    pub raw_address_rows: u64,
    pub raw_provider_plan_rows: u64,
    pub raw_formulary_entries: u64,
    pub named_facility_records: u64,
    pub facility_type_values: u64,
    pub dated_records: u64,
    pub accepting_newpt_records: u64,
    pub accepting_nopt_records: u64,
    pub accepting_null_records: u64,
    pub invalid_phone_count: u64,
    pub valid_phone_count: u64,
    pub multi_address_provider_records: u64,
    pub plan_year_rows: u64,
    pub invalid_npi_count: u64,
    pub invalid_npi_individual_records: u64,
    pub invalid_npi_facility_records: u64,
    pub invalid_npi_address_rows: u64,
    pub invalid_npi_provider_plan_rows: u64,
}

impl UhcFileCounters {
    fn merge(&mut self, incoming: &Self) {
        self.raw_provider_records += incoming.raw_provider_records;
        self.raw_plan_records += incoming.raw_plan_records;
        self.raw_individual_records += incoming.raw_individual_records;
        self.raw_facility_records += incoming.raw_facility_records;
        self.raw_address_rows += incoming.raw_address_rows;
        self.raw_provider_plan_rows += incoming.raw_provider_plan_rows;
        self.raw_formulary_entries += incoming.raw_formulary_entries;
        self.named_facility_records += incoming.named_facility_records;
        self.facility_type_values += incoming.facility_type_values;
        self.dated_records += incoming.dated_records;
        self.accepting_newpt_records += incoming.accepting_newpt_records;
        self.accepting_nopt_records += incoming.accepting_nopt_records;
        self.accepting_null_records += incoming.accepting_null_records;
        self.invalid_phone_count += incoming.invalid_phone_count;
        self.valid_phone_count += incoming.valid_phone_count;
        self.multi_address_provider_records += incoming.multi_address_provider_records;
        self.plan_year_rows += incoming.plan_year_rows;
        self.invalid_npi_count += incoming.invalid_npi_count;
        self.invalid_npi_individual_records += incoming.invalid_npi_individual_records;
        self.invalid_npi_facility_records += incoming.invalid_npi_facility_records;
        self.invalid_npi_address_rows += incoming.invalid_npi_address_rows;
        self.invalid_npi_provider_plan_rows += incoming.invalid_npi_provider_plan_rows;
    }
}

#[derive(Serialize)]
struct ProviderQuarantineFact<'a> {
    #[serde(rename = "_healthporta_quarantine")]
    quarantine: ProviderQuarantinePayload<'a>,
}

#[derive(Serialize)]
struct ProviderQuarantinePayload<'a> {
    contract_id: &'static str,
    reason: &'static str,
    source_file_id: &'a str,
    range_ordinal: u64,
    occurrence_ordinal: u64,
    record_sha256: String,
}

fn provider_quarantine_identity(
    source_file_id: &str,
    range_ordinal: u64,
    occurrence_ordinal: u64,
    record_sha256: &str,
) -> Vec<u8> {
    serde_json::to_vec(&(
        UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
        source_file_id,
        range_ordinal,
        occurrence_ordinal,
        UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
        record_sha256,
    ))
    .expect("UHC provider quarantine identity is serializable")
}

#[derive(Clone, Debug)]
struct EvidenceRow {
    occurrence_ordinal: u64,
    npi: String,
    provider_type: String,
    name: String,
    facility_name: String,
    facility_types: String,
    gender: String,
    accepting: String,
    address_sets: String,
    specialties: String,
    dates: String,
}

impl EvidenceRow {
    fn shrink(&mut self) {
        self.npi.shrink_to_fit();
        self.provider_type.shrink_to_fit();
        self.name.shrink_to_fit();
        self.facility_name.shrink_to_fit();
        self.facility_types.shrink_to_fit();
        self.gender.shrink_to_fit();
        self.accepting.shrink_to_fit();
        self.address_sets.shrink_to_fit();
        self.specialties.shrink_to_fit();
        self.dates.shrink_to_fit();
    }

    fn heap_bytes(&self) -> usize {
        self.npi.capacity()
            + self.provider_type.capacity()
            + self.name.capacity()
            + self.facility_name.capacity()
            + self.facility_types.capacity()
            + self.gender.capacity()
            + self.accepting.capacity()
            + self.address_sets.capacity()
            + self.specialties.capacity()
            + self.dates.capacity()
    }

    fn conflict_signature_pack(&self) -> Vec<u8> {
        let mut pack = Vec::with_capacity(9 * 32);
        for value in [
            self.accepting.as_bytes(),
            self.address_sets.as_bytes(),
            self.dates.as_bytes(),
            self.facility_name.as_bytes(),
            self.facility_types.as_bytes(),
            self.gender.as_bytes(),
            self.name.as_bytes(),
            self.provider_type.as_bytes(),
            self.specialties.as_bytes(),
        ] {
            pack.extend_from_slice(Sha256::digest(value).as_slice());
        }
        pack
    }

    fn identity_bytes(&self) -> Vec<u8> {
        let signature_pack = self.conflict_signature_pack();
        serde_json::to_vec(&(
            self.occurrence_ordinal,
            self.npi.as_str(),
            hex_digest(&signature_pack),
        ))
        .expect("UHC evidence identity is serializable")
    }
}

struct HashingWriter<W> {
    inner: W,
    digest: Sha256,
    bytes: u64,
}

impl<W> HashingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            digest: Sha256::new(),
            bytes: 0,
        }
    }

    fn finish(self) -> (W, String, u64) {
        (
            self.inner,
            hex_digest(self.digest.finalize().as_slice()),
            self.bytes,
        )
    }
}

impl<W: Write> Write for HashingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.digest.update(&buffer[..written]);
        let Some(bytes) = self.bytes.checked_add(written as u64) else {
            return Err(invalid("UHC semantic output byte count overflowed"));
        };
        self.bytes = bytes;
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FactBlockProof {
    pub range_ordinal: u64,
    pub record_start: u64,
    pub record_count: u64,
    pub fact_count: u64,
    pub compressed_bytes: u64,
    pub compressed_payload_sha256: String,
    pub semantic_block_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct EvidenceRangeProof {
    pub range_ordinal: u64,
    pub evidence_count: u64,
    pub run_count: u64,
    pub layout_sha256: String,
}

#[derive(Clone, Debug, Serialize)]
pub struct SemanticEncodeReport {
    pub contract_id: &'static str,
    pub contract_version: u64,
    pub copy_format_id: &'static str,
    pub source_id: &'static str,
    pub lineage: SemanticLineageReport,
    pub counters: UhcFileCounters,
    pub fact_count: u64,
    pub evidence_count: u64,
    pub quarantine_count: u64,
    pub quarantine_identity_set_sha256: String,
    pub fact_set_sha256: String,
    pub record_identity_set_sha256: String,
    pub evidence_identity_set_sha256: String,
    pub evidence_layout_set_sha256: String,
    pub fact_blocks: Vec<FactBlockProof>,
    pub evidence_ranges: Vec<EvidenceRangeProof>,
    pub evidence_run_count: u64,
    pub output_bytes: u64,
    pub output_sha256: String,
    pub copy_row_count: u64,
    pub worker_count: usize,
    pub per_worker_memory_budget_bytes: usize,
    pub total_memory_budget_bytes: usize,
    pub max_record_bytes: usize,
    pub evidence_buffer_bytes: usize,
    pub pending_quarantine_identity_budget_bytes: usize,
    pub peak_worker_reserved_bytes: usize,
    pub peak_pending_range_results: usize,
    pub range_cpu_seconds: f64,
    pub elapsed_seconds: f64,
}

#[derive(Clone, Debug, Serialize)]
pub struct SemanticLineageReport {
    pub artifact_sha256: String,
    pub manifest_sha256: String,
    pub range_set_sha256: String,
    pub source_file_id: String,
    pub source_binding_id: String,
    pub collection_kind: UhcCollectionKind,
}

impl From<&AdmittedSemanticLineage> for SemanticLineageReport {
    fn from(lineage: &AdmittedSemanticLineage) -> Self {
        Self {
            artifact_sha256: lineage.artifact_sha256.clone(),
            manifest_sha256: lineage.manifest_sha256.clone(),
            range_set_sha256: lineage.range_set_sha256.clone(),
            source_file_id: lineage.source_file_id.clone(),
            source_binding_id: lineage.source_binding_id.clone(),
            collection_kind: lineage.collection_kind,
        }
    }
}

struct RangeWorkResult {
    fact_payload: File,
    evidence_spool: File,
    fact_identity_spool: File,
    evidence_identity_spool: File,
    quarantine_identity_bytes: Vec<u8>,
    counters: UhcFileCounters,
    fact_block: FactBlockProof,
    evidence_range: EvidenceRangeProof,
    peak_reserved_bytes: usize,
    cpu_seconds: f64,
}

struct RangeWorker {
    source_file_id: String,
    collection_kind: UhcCollectionKind,
    range: AdmittedSemanticRange,
    budget: SemanticMemoryBudget,
    counters: UhcFileCounters,
    fact_payload: File,
    fact_encoder: Option<ZlibEncoder<HashingWriter<BufWriter<File>>>>,
    fact_identity_spool: BufWriter<File>,
    evidence_identity_spool: BufWriter<File>,
    quarantine_identity_bytes: Vec<u8>,
    evidence_spool: BufWriter<File>,
    evidence_rows: Vec<EvidenceRow>,
    evidence_rows_bytes: usize,
    evidence_run_ordinal: u64,
    evidence_count: u64,
    evidence_layout_digest: Sha256,
    semantic_block_digest: Sha256,
    semantic_fact_count: u64,
    fact_identity_count: u64,
    evidence_identity_count: u64,
    quarantine_identity_count: u64,
    peak_reserved_bytes: usize,
}

impl RangeWorker {
    fn new(
        source_file_id: &str,
        collection_kind: UhcCollectionKind,
        range: &AdmittedSemanticRange,
        budget: &SemanticMemoryBudget,
    ) -> io::Result<Self> {
        let fact_payload = tempfile::tempfile()?;
        let fact_writer = BufWriter::with_capacity(COPY_BUFFER_BYTES, fact_payload.try_clone()?);
        let fact_encoder = ZlibEncoder::new(HashingWriter::new(fact_writer), Compression::fast());
        Ok(Self {
            source_file_id: source_file_id.to_owned(),
            collection_kind,
            range: range.clone(),
            budget: budget.clone(),
            counters: UhcFileCounters::default(),
            fact_payload,
            fact_encoder: Some(fact_encoder),
            fact_identity_spool: BufWriter::with_capacity(COPY_BUFFER_BYTES, tempfile::tempfile()?),
            evidence_identity_spool: BufWriter::with_capacity(
                COPY_BUFFER_BYTES,
                tempfile::tempfile()?,
            ),
            quarantine_identity_bytes: Vec::with_capacity(QUARANTINE_IDENTITY_BUFFER_BYTES),
            evidence_spool: BufWriter::with_capacity(COPY_BUFFER_BYTES, tempfile::tempfile()?),
            evidence_rows: Vec::new(),
            evidence_rows_bytes: 0,
            evidence_run_ordinal: 0,
            evidence_count: 0,
            evidence_layout_digest: Sha256::new(),
            semantic_block_digest: Sha256::new(),
            semantic_fact_count: 0,
            fact_identity_count: 0,
            evidence_identity_count: 0,
            quarantine_identity_count: 0,
            peak_reserved_bytes: WORKER_FIXED_BYTES,
        })
    }

    fn observe_peak(&mut self, record_bytes: usize) -> io::Result<()> {
        let Some(mut reserved) = WORKER_FIXED_BYTES.checked_add(record_reservation(record_bytes)?)
        else {
            return Err(invalid("UHC semantic worker reservation overflowed"));
        };
        let Some(with_rows) = reserved.checked_add(self.evidence_rows_bytes) else {
            return Err(invalid("UHC semantic worker reservation overflowed"));
        };
        reserved = with_rows;
        let capacity_bytes = self.evidence_rows.capacity() * size_of::<EvidenceRow>();
        let Some(reserved) = reserved.checked_add(capacity_bytes) else {
            return Err(invalid("UHC semantic worker reservation overflowed"));
        };
        if reserved > self.budget.per_worker_bytes {
            return Err(invalid(format!(
                "UHC semantic worker exceeded its hard memory budget: {reserved} > {}",
                self.budget.per_worker_bytes
            )));
        }
        self.peak_reserved_bytes = self.peak_reserved_bytes.max(reserved);
        Ok(())
    }

    fn append_identity(
        writer: &mut BufWriter<File>,
        count: &mut u64,
        identity: &[u8],
    ) -> io::Result<()> {
        if *count != 0 {
            writer.write_all(b"\n")?;
        }
        writer.write_all(identity)?;
        *count += 1;
        Ok(())
    }

    fn append_fact(&mut self, ordinal: u64, payload: &[u8]) -> io::Result<()> {
        let payload_hash = sha256(payload);
        let identity = fact_identity(
            &self.source_file_id,
            self.collection_kind.fact_type(),
            ordinal,
            &payload_hash,
        );
        if self.semantic_fact_count != 0 {
            self.semantic_block_digest.update(b"\n");
        }
        self.semantic_block_digest.update(identity.as_bytes());
        Self::append_identity(
            &mut self.fact_identity_spool,
            &mut self.fact_identity_count,
            identity.as_bytes(),
        )?;
        let Some(encoder) = self.fact_encoder.as_mut() else {
            return Err(invalid("UHC semantic fact encoder is already sealed"));
        };
        encoder.write_all(payload)?;
        encoder.write_all(b"\n")?;
        self.semantic_fact_count += 1;
        Ok(())
    }

    fn append_provider_quarantine(&mut self, ordinal: u64, record_bytes: &[u8]) -> io::Result<()> {
        if self.quarantine_identity_count >= UHC_PROVIDER_QUARANTINE_MAX_COUNT {
            return Err(invalid(
                "UHC provider quarantine exceeds its bounded identity buffer",
            ));
        }
        let record_sha256 = sha256(record_bytes);
        let payload = canonical_value_bytes(&ProviderQuarantineFact {
            quarantine: ProviderQuarantinePayload {
                contract_id: UHC_PROVIDER_QUARANTINE_CONTRACT_ID,
                reason: UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM,
                source_file_id: &self.source_file_id,
                range_ordinal: self.range.range_ordinal,
                occurrence_ordinal: ordinal,
                record_sha256: record_sha256.clone(),
            },
        })?;
        if payload.len() > self.budget.max_record_bytes {
            return Err(invalid(
                "canonical UHC provider quarantine exceeds max record bytes",
            ));
        }
        let identity = provider_quarantine_identity(
            &self.source_file_id,
            self.range.range_ordinal,
            ordinal,
            &record_sha256,
        );
        let separator_bytes = usize::from(self.quarantine_identity_count != 0);
        let Some(required_bytes) = self
            .quarantine_identity_bytes
            .len()
            .checked_add(separator_bytes)
            .and_then(|value| value.checked_add(identity.len()))
        else {
            return Err(invalid("UHC provider quarantine identity bytes overflowed"));
        };
        if required_bytes > QUARANTINE_IDENTITY_BUFFER_BYTES {
            return Err(invalid(
                "UHC provider quarantine exceeds its bounded identity bytes",
            ));
        }
        if separator_bytes != 0 {
            self.quarantine_identity_bytes.push(b'\n');
        }
        self.quarantine_identity_bytes.extend_from_slice(&identity);
        self.quarantine_identity_count += 1;
        self.append_fact(ordinal, &payload)?;
        self.observe_peak(record_bytes.len())
    }

    fn push_evidence(&mut self, mut row: EvidenceRow, record_bytes: usize) -> io::Result<()> {
        row.shrink();
        let row_bytes = row.heap_bytes();
        let Some(row_reservation) = row_bytes.checked_add(size_of::<EvidenceRow>()) else {
            return Err(invalid("UHC semantic evidence row overflowed"));
        };
        if row_reservation > self.budget.evidence_buffer_bytes {
            return Err(invalid(format!(
                "one UHC NPI evidence row exceeds the hard buffer budget: {row_bytes} > {}",
                self.budget.evidence_buffer_bytes
            )));
        }
        loop {
            let next_capacity = if self.evidence_rows.len() == self.evidence_rows.capacity() {
                self.evidence_rows.capacity().saturating_mul(2).max(4)
            } else {
                self.evidence_rows.capacity()
            };
            let Some(capacity_bytes) = next_capacity.checked_mul(size_of::<EvidenceRow>()) else {
                return Err(invalid("UHC semantic evidence vector overflowed"));
            };
            let Some(projected_with_rows) = capacity_bytes.checked_add(self.evidence_rows_bytes)
            else {
                return Err(invalid("UHC semantic evidence vector overflowed"));
            };
            let Some(projected) = projected_with_rows.checked_add(row_bytes) else {
                return Err(invalid("UHC semantic evidence vector overflowed"));
            };
            if projected <= self.budget.evidence_buffer_bytes {
                if next_capacity > self.evidence_rows.capacity()
                    && self
                        .evidence_rows
                        .try_reserve_exact(next_capacity - self.evidence_rows.len())
                        .is_err()
                {
                    return Err(invalid("unable to reserve bounded UHC evidence memory"));
                }
                break;
            }
            if self.evidence_rows.is_empty() {
                self.evidence_rows.shrink_to_fit();
                if self.evidence_rows.try_reserve_exact(1).is_err() {
                    return Err(invalid("unable to reserve bounded UHC evidence memory"));
                }
                break;
            }
            self.flush_evidence_run()?;
            let post_flush_capacity = self
                .evidence_rows
                .capacity()
                .checked_mul(size_of::<EvidenceRow>());
            let post_flush_reservation = match post_flush_capacity {
                Some(capacity) => capacity.checked_add(row_bytes),
                None => None,
            };
            if post_flush_reservation.is_none()
                || post_flush_reservation > Some(self.budget.evidence_buffer_bytes)
            {
                self.evidence_rows.shrink_to_fit();
            }
        }
        self.evidence_rows_bytes += row_bytes;
        let identity = row.identity_bytes();
        Self::append_identity(
            &mut self.evidence_identity_spool,
            &mut self.evidence_identity_count,
            &identity,
        )?;
        self.evidence_rows.push(row);
        self.evidence_count += 1;
        self.observe_peak(record_bytes)
    }

    fn flush_evidence_run(&mut self) -> io::Result<()> {
        if self.evidence_rows.is_empty() {
            return Ok(());
        }
        self.evidence_rows.sort_unstable_by(|left, right| {
            (left.npi.as_str(), left.occurrence_ordinal)
                .cmp(&(right.npi.as_str(), right.occurrence_ordinal))
        });
        let mut run_digest = Sha256::new();
        let run_count = self.evidence_rows.len() as u64;
        for (index, row) in self.evidence_rows.iter().enumerate() {
            let identity = row.identity_bytes();
            if index != 0 {
                run_digest.update(b"\n");
            }
            run_digest.update(&identity);
            write_evidence_copy_row(
                &mut self.evidence_spool,
                self.range.range_ordinal,
                self.evidence_run_ordinal,
                row,
            )?;
        }
        let run_hash = hex_digest(run_digest.finalize().as_slice());
        let layout = serde_json::to_vec(&(
            self.range.range_ordinal,
            self.evidence_run_ordinal,
            run_count,
            run_hash,
        ))
        .expect("UHC evidence run identity is serializable");
        if self.evidence_run_ordinal != 0 {
            self.evidence_layout_digest.update(b"\n");
        }
        self.evidence_layout_digest.update(layout);
        self.evidence_run_ordinal += 1;
        self.evidence_rows.clear();
        self.evidence_rows_bytes = 0;
        Ok(())
    }

    fn process_provider(&mut self, ordinal: u64, bytes: &[u8]) -> io::Result<()> {
        let record: ProviderRecord = match serde_json::from_slice(bytes) {
            Ok(record) => record,
            Err(error) => {
                return Err(invalid(format!(
                    "invalid retained UHC provider JSON: {error}"
                )))
            }
        };
        let npi_validity = npi_validity(&record.npi);
        if npi_validity == NpiValidity::Invalid {
            return Err(invalid("UHC provider NPI is not structurally valid"));
        }
        let Some(provider_type) = clean(Some(&record.provider_type), true) else {
            return Err(invalid("UHC provider type is empty"));
        };
        if provider_type != "INDIVIDUAL" && provider_type != "FACILITY" {
            return Err(invalid("UHC provider type is unsupported"));
        }
        if record.addresses.is_empty() || record.plans.is_empty() {
            return Err(invalid("UHC provider addresses and plans must be nonempty"));
        }
        self.counters.raw_provider_records += 1;
        self.counters.raw_address_rows += record.addresses.len() as u64;
        self.counters.multi_address_provider_records += u64::from(record.addresses.len() > 1);
        self.counters.dated_records += u64::from(record.last_updated_on.is_some());
        if provider_type == "INDIVIDUAL" {
            self.counters.raw_individual_records += 1;
        } else {
            self.counters.raw_facility_records += 1;
            self.counters.named_facility_records += u64::from(record.facility_name.is_some());
            self.counters.facility_type_values += match &record.facility_type {
                Some(values) => values.len() as u64,
                None => 0,
            };
        }
        let accepting = accepting_code(record.accepting.as_deref())?;
        if accepting == Some("newpt") {
            self.counters.accepting_newpt_records += 1;
        } else if accepting == Some("nopt") {
            self.counters.accepting_nopt_records += 1;
        } else {
            self.counters.accepting_null_records += 1;
        }
        for address in &record.addresses {
            if address.phone.is_some() {
                if normalized_phone(address.phone.as_deref()).is_some() {
                    self.counters.valid_phone_count += 1;
                } else {
                    self.counters.invalid_phone_count += 1;
                }
            }
        }
        let mut provider_plan_year_rows = 0u64;
        for plan in &record.plans {
            required_years(&plan.years)?;
            if clean(plan.plan_id_type.as_deref(), false).is_none() {
                return Err(invalid("UHC provider plan ID type is empty"));
            }
            if clean(plan.plan_id.as_deref(), false).is_none() {
                return Err(invalid("UHC provider plan ID is empty"));
            }
            provider_plan_year_rows = provider_plan_year_rows
                .checked_add(plan.years.len() as u64)
                .ok_or_else(|| invalid("UHC provider plan-year count overflow"))?;
        }
        self.counters.plan_year_rows += provider_plan_year_rows;
        self.counters.raw_provider_plan_rows += provider_plan_year_rows;
        if npi_validity == NpiValidity::ChecksumInvalid {
            self.counters.invalid_npi_count += 1;
            self.counters.invalid_npi_individual_records +=
                u64::from(provider_type == "INDIVIDUAL");
            self.counters.invalid_npi_facility_records += u64::from(provider_type == "FACILITY");
            self.counters.invalid_npi_address_rows += record.addresses.len() as u64;
            self.counters.invalid_npi_provider_plan_rows += provider_plan_year_rows;
            return self.append_provider_quarantine(ordinal, bytes);
        }
        let evidence = EvidenceRow {
            occurrence_ordinal: ordinal,
            npi: record.npi.clone(),
            provider_type,
            name: optional_signature(&record.name)?,
            facility_name: optional_signature(&record.facility_name)?,
            facility_types: sorted_signature(&record.facility_type)?,
            gender: optional_signature(&record.gender)?,
            accepting: optional_signature(&record.accepting)?,
            address_sets: address_set_signature(&record.addresses)?,
            specialties: sorted_signature(&record.specialty)?,
            dates: optional_signature(&record.last_updated_on)?,
        };
        let payload = canonical_value_bytes(&record)?;
        if payload.len() > self.budget.max_record_bytes {
            return Err(invalid(
                "canonical UHC provider fact exceeds max record bytes",
            ));
        }
        self.append_fact(ordinal, &payload)?;
        self.push_evidence(evidence, bytes.len())
    }

    fn process_plan(&mut self, ordinal: u64, bytes: &[u8]) -> io::Result<()> {
        let record: PlanRecord = match serde_json::from_slice(bytes) {
            Ok(record) => record,
            Err(error) => return Err(invalid(format!("invalid retained UHC plan JSON: {error}"))),
        };
        required_years(&record.years)?;
        if clean(record.plan_id_type.as_deref(), false).is_none() {
            return Err(invalid("UHC plan ID type is empty"));
        }
        if clean(record.plan_id.as_deref(), false).is_none() {
            return Err(invalid("UHC plan ID is empty"));
        }
        self.counters.raw_plan_records += 1;
        self.counters.dated_records += u64::from(record.last_updated_on.is_some());
        self.counters.raw_formulary_entries += match &record.formulary {
            Some(entries) => entries.len() as u64,
            None => 0,
        };
        self.counters.plan_year_rows += record.years.len() as u64;
        let payload = canonical_value_bytes(&record)?;
        if payload.len() > self.budget.max_record_bytes {
            return Err(invalid("canonical UHC plan fact exceeds max record bytes"));
        }
        self.append_fact(ordinal, &payload)?;
        self.observe_peak(bytes.len())
    }

    fn process_record(&mut self, ordinal: u64, bytes: &[u8]) -> io::Result<()> {
        if bytes.is_empty() || bytes.len() > self.budget.max_record_bytes {
            return Err(invalid(format!(
                "UHC semantic record byte count is outside 1..={}",
                self.budget.max_record_bytes
            )));
        }
        self.observe_peak(bytes.len())?;
        match self.collection_kind {
            UhcCollectionKind::ProviderMembership => self.process_provider(ordinal, bytes),
            UhcCollectionKind::PlanReference => self.process_plan(ordinal, bytes),
        }
    }

    fn finish(mut self, started: Instant) -> io::Result<RangeWorkResult> {
        self.flush_evidence_run()?;
        if self.semantic_fact_count != self.range.record_count
            || self.fact_identity_count != self.range.record_count
        {
            return Err(invalid("UHC semantic range fact count does not match"));
        }
        if self.quarantine_identity_count != self.counters.invalid_npi_count {
            return Err(invalid(
                "UHC semantic range quarantine count does not match",
            ));
        }
        let expected_evidence = match self.collection_kind {
            UhcCollectionKind::ProviderMembership => self
                .range
                .record_count
                .checked_sub(self.counters.invalid_npi_count)
                .ok_or_else(|| invalid("UHC semantic quarantine count overflowed"))?,
            UhcCollectionKind::PlanReference => 0,
        };
        if self.evidence_count != expected_evidence
            || self.evidence_identity_count != expected_evidence
        {
            return Err(invalid("UHC semantic range evidence count does not match"));
        }

        let Some(encoder) = self.fact_encoder.take() else {
            return Err(invalid("UHC semantic fact encoder is already sealed"));
        };
        let hashing_writer = encoder.finish()?;
        let (mut fact_writer, payload_sha256, compressed_bytes) = hashing_writer.finish();
        fact_writer.flush()?;
        self.fact_payload.seek(SeekFrom::Start(0))?;

        self.fact_identity_spool.flush()?;
        let mut fact_identity_spool = match self.fact_identity_spool.into_inner() {
            Ok(spool) => spool,
            Err(error) => return Err(error.into_error()),
        };
        fact_identity_spool.seek(SeekFrom::Start(0))?;

        self.evidence_identity_spool.flush()?;
        let mut evidence_identity_spool = match self.evidence_identity_spool.into_inner() {
            Ok(spool) => spool,
            Err(error) => return Err(error.into_error()),
        };
        evidence_identity_spool.seek(SeekFrom::Start(0))?;

        self.evidence_spool.flush()?;
        let mut evidence_spool = match self.evidence_spool.into_inner() {
            Ok(spool) => spool,
            Err(error) => return Err(error.into_error()),
        };
        evidence_spool.seek(SeekFrom::Start(0))?;

        Ok(RangeWorkResult {
            fact_payload: self.fact_payload,
            evidence_spool,
            fact_identity_spool,
            evidence_identity_spool,
            quarantine_identity_bytes: self.quarantine_identity_bytes,
            counters: self.counters,
            fact_block: FactBlockProof {
                range_ordinal: self.range.range_ordinal,
                record_start: self.range.record_start,
                record_count: self.range.record_count,
                fact_count: self.semantic_fact_count,
                compressed_bytes,
                compressed_payload_sha256: payload_sha256,
                semantic_block_sha256: hex_digest(self.semantic_block_digest.finalize().as_slice()),
            },
            evidence_range: EvidenceRangeProof {
                range_ordinal: self.range.range_ordinal,
                evidence_count: self.evidence_count,
                run_count: self.evidence_run_ordinal,
                layout_sha256: hex_digest(self.evidence_layout_digest.finalize().as_slice()),
            },
            peak_reserved_bytes: self.peak_reserved_bytes,
            cpu_seconds: started.elapsed().as_secs_f64(),
        })
    }
}

fn process_range<S: AdmittedRangeSource>(
    source: &S,
    range: &AdmittedSemanticRange,
    budget: &SemanticMemoryBudget,
) -> io::Result<RangeWorkResult> {
    let started = Instant::now();
    let mut worker = RangeWorker::new(
        &source.lineage().source_file_id,
        source.lineage().collection_kind,
        range,
        budget,
    )?;
    let mut expected_ordinal = range.record_start;
    source.visit_verified_records(range, &mut |observed_ordinal, record| {
        if observed_ordinal != expected_ordinal {
            return Err(invalid("admitted UHC record ordinals are not contiguous"));
        }
        worker.process_record(observed_ordinal, record)?;
        let Some(next_ordinal) = expected_ordinal.checked_add(1) else {
            return Err(invalid("UHC semantic occurrence ordinal overflowed"));
        };
        expected_ordinal = next_ordinal;
        Ok(())
    })?;
    let Some(expected_end) = range.record_start.checked_add(range.record_count) else {
        return Err(invalid("UHC semantic range end overflowed"));
    };
    if expected_ordinal != expected_end {
        return Err(invalid(
            "admitted UHC record visit did not cover the complete range",
        ));
    }
    worker.finish(started)
}

fn write_null<W: Write>(writer: &mut W) -> io::Result<()> {
    writer.write_all(&(-1i32).to_be_bytes())
}

fn write_field<W: Write>(writer: &mut W, value: &[u8]) -> io::Result<()> {
    let length = match i32::try_from(value.len()) {
        Ok(length) => length,
        Err(_) => {
            return Err(invalid(
                "UHC semantic COPY field exceeds signed 32-bit bytes",
            ))
        }
    };
    writer.write_all(&length.to_be_bytes())?;
    writer.write_all(value)
}

fn write_i16<W: Write>(writer: &mut W, value: i16) -> io::Result<()> {
    writer.write_all(&2i32.to_be_bytes())?;
    writer.write_all(&value.to_be_bytes())
}

fn write_i64<W: Write>(writer: &mut W, value: u64) -> io::Result<()> {
    let value = match i64::try_from(value) {
        Ok(value) => value,
        Err(_) => {
            return Err(invalid(
                "UHC semantic COPY integer exceeds signed 64-bit range",
            ))
        }
    };
    writer.write_all(&8i32.to_be_bytes())?;
    writer.write_all(&value.to_be_bytes())
}

fn write_evidence_copy_row<W: Write>(
    writer: &mut W,
    range_ordinal: u64,
    run_ordinal: u64,
    row: &EvidenceRow,
) -> io::Result<()> {
    writer.write_all(&UHC_SEMANTIC_COPY_COLUMN_COUNT.to_be_bytes())?;
    write_i16(writer, COPY_ROW_NPI_EVIDENCE)?;
    write_i64(writer, range_ordinal)?;
    write_i64(writer, run_ordinal)?;
    write_i64(writer, row.occurrence_ordinal)?;
    write_null(writer)?;
    write_null(writer)?;
    write_field(writer, row.npi.as_bytes())?;
    write_field(writer, &row.conflict_signature_pack())?;
    write_null(writer)?;
    write_null(writer)?;
    write_null(writer)
}

fn write_fact_copy_row<W: Write>(
    writer: &mut W,
    block: &FactBlockProof,
    payload: &mut File,
) -> io::Result<()> {
    writer.write_all(&UHC_SEMANTIC_COPY_COLUMN_COUNT.to_be_bytes())?;
    write_i16(writer, COPY_ROW_FACT_BLOCK)?;
    write_i64(writer, block.range_ordinal)?;
    write_null(writer)?;
    write_null(writer)?;
    write_i64(writer, block.record_start)?;
    write_i64(writer, block.record_count)?;
    write_null(writer)?;
    write_null(writer)?;
    write_field(writer, block.compressed_payload_sha256.as_bytes())?;
    write_field(writer, block.semantic_block_sha256.as_bytes())?;
    let payload_length = match i32::try_from(block.compressed_bytes) {
        Ok(length) => length,
        Err(_) => {
            return Err(invalid(
                "UHC semantic compressed fact block exceeds COPY bytea limit",
            ))
        }
    };
    writer.write_all(&payload_length.to_be_bytes())?;
    let copied = io::copy(payload, writer)?;
    if copied != block.compressed_bytes {
        return Err(invalid(
            "UHC semantic compressed fact block ended unexpectedly",
        ));
    }
    Ok(())
}

struct CountingWriter<W> {
    inner: W,
    bytes: u64,
    digest: Sha256,
}

impl<W> CountingWriter<W> {
    fn new(inner: W) -> Self {
        Self {
            inner,
            bytes: 0,
            digest: Sha256::new(),
        }
    }
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        let Some(bytes) = self.bytes.checked_add(written as u64) else {
            return Err(invalid("UHC semantic COPY byte count overflowed"));
        };
        self.bytes = bytes;
        self.digest.update(&buffer[..written]);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

fn append_spool_digest(digest: &mut Sha256, count: &mut u64, spool: &mut File) -> io::Result<()> {
    spool.seek(SeekFrom::Start(0))?;
    let mut reader = BufReader::with_capacity(COPY_BUFFER_BYTES, spool);
    let mut buffer = vec![0u8; COPY_BUFFER_BYTES];
    let mut saw_bytes = false;
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        if *count != 0 && !saw_bytes {
            digest.update(b"\n");
        }
        digest.update(&buffer[..read]);
        saw_bytes = true;
    }
    if saw_bytes {
        *count += 1;
    }
    Ok(())
}

fn append_identity_bytes_digest(
    digest: &mut Sha256,
    count: &mut u64,
    identity_bytes: &[u8],
) -> io::Result<()> {
    if identity_bytes.is_empty() {
        return Ok(());
    }
    if *count != 0 {
        digest.update(b"\n");
    }
    digest.update(identity_bytes);
    *count = (*count)
        .checked_add(1)
        .ok_or_else(|| invalid("UHC identity spool count overflowed"))?;
    Ok(())
}

fn fact_set_sha256(blocks: &[FactBlockProof]) -> String {
    let mut digest = Sha256::new();
    for (index, block) in blocks.iter().enumerate() {
        if index != 0 {
            digest.update(b"\n");
        }
        let identity = serde_json::to_vec(&(
            UHC_SEMANTIC_FACT_CONTRACT_ID,
            block.range_ordinal,
            block.record_start,
            block.record_count,
            block.fact_count,
            block.compressed_payload_sha256.as_str(),
            block.semantic_block_sha256.as_str(),
        ))
        .expect("UHC semantic fact block identity is serializable");
        digest.update(identity);
    }
    hex_digest(digest.finalize().as_slice())
}

fn evidence_layout_set_sha256(ranges: &[EvidenceRangeProof]) -> String {
    let mut digest = Sha256::new();
    for (index, range) in ranges.iter().enumerate() {
        if index != 0 {
            digest.update(b"\n");
        }
        let identity = serde_json::to_vec(&(
            range.range_ordinal,
            range.evidence_count,
            range.run_count,
            range.layout_sha256.as_str(),
        ))
        .expect("UHC semantic evidence range identity is serializable");
        digest.update(identity);
    }
    hex_digest(digest.finalize().as_slice())
}

fn validate_source<S: AdmittedRangeSource>(source: &S) -> io::Result<()> {
    source.lineage().validate()?;
    let ranges = source.ranges();
    if ranges.is_empty() || ranges.len() > MAX_RANGE_COUNT {
        return Err(invalid(
            "admitted UHC semantic range count is outside 1..=256",
        ));
    }
    let mut next_record = 0u64;
    for (ordinal, range) in ranges.iter().enumerate() {
        require_sha256(&range.raw_sha256, "range raw SHA-256")?;
        require_sha256(&range.canonical_sha256, "range canonical SHA-256")?;
        if range.range_ordinal != ordinal as u64
            || range.record_start != next_record
            || range.record_count == 0
            || range.raw_byte_count == 0
        {
            return Err(invalid(
                "admitted UHC semantic ranges are not ordered, nonempty, and contiguous",
            ));
        }
        let Some(record_end) = next_record.checked_add(range.record_count) else {
            return Err(invalid("admitted UHC semantic record count overflowed"));
        };
        next_record = record_end;
    }
    Ok(())
}

/// Decode verified ranges into one PostgreSQL binary COPY stream.
///
/// Row kind 1 contains a zlib-compressed canonical fact block.  Row kind 2 is
/// an NPI evidence occurrence, sorted within a bounded run.  The database can
/// group evidence rows set-wise without decoding fact payloads or maintaining a
/// process-wide NPI map.  A failed call never returns a seal report; callers
/// must run COPY inside a transaction and roll the BUILDING stage back.
pub fn encode_admitted_ranges_to_copy<S, W>(
    source: &S,
    output: W,
    budget: &SemanticMemoryBudget,
) -> io::Result<SemanticEncodeReport>
where
    S: AdmittedRangeSource,
    W: Write + Send,
{
    let started = Instant::now();
    budget.validate()?;
    validate_source(source)?;
    let ranges = source.ranges();
    let worker_count = budget.worker_count.min(ranges.len()).max(1);
    let pool = match ThreadPoolBuilder::new()
        .num_threads(worker_count + 1)
        .thread_name(|index| format!("uhc-semantic-{index}"))
        .build()
    {
        Ok(pool) => pool,
        Err(error) => {
            return Err(invalid(format!(
                "cannot create UHC semantic workers: {error}"
            )))
        }
    };
    let next_range = AtomicUsize::new(0);
    let (sender, receiver) = crossbeam_channel::bounded(worker_count * 2);
    let mut writer = CountingWriter::new(BufWriter::with_capacity(COPY_BUFFER_BYTES, output));
    writer.write_all(b"PGCOPY\n\xff\r\n\0")?;
    writer.write_all(&0i32.to_be_bytes())?;
    writer.write_all(&0i32.to_be_bytes())?;

    let mut pending: BTreeMap<usize, io::Result<RangeWorkResult>> = BTreeMap::new();
    let mut next_write = 0usize;
    let mut first_error = None;
    let mut counters = UhcFileCounters::default();
    let mut fact_blocks = Vec::with_capacity(ranges.len());
    let mut evidence_ranges = Vec::with_capacity(ranges.len());
    let mut fact_identity_digest = Sha256::new();
    let mut evidence_identity_digest = Sha256::new();
    let mut quarantine_identity_digest = Sha256::new();
    let mut fact_identity_spools = 0u64;
    let mut evidence_identity_spools = 0u64;
    let mut quarantine_identity_spools = 0u64;
    let mut fact_count = 0u64;
    let mut evidence_count = 0u64;
    let mut evidence_run_count = 0u64;
    let mut peak_worker_reserved_bytes = 0usize;
    let mut peak_pending_range_results = 0usize;
    let mut range_cpu_seconds = 0.0f64;

    pool.scope(|scope| {
        for _ in 0..worker_count {
            let sender = sender.clone();
            let next_range = &next_range;
            scope.spawn(move |_| loop {
                let ordinal = next_range.fetch_add(1, Ordering::Relaxed);
                let Some(range) = ranges.get(ordinal) else {
                    break;
                };
                let result = process_range(source, range, budget);
                if sender.send((ordinal, result)).is_err() {
                    break;
                }
            });
        }
        drop(sender);
        while let Ok((ordinal, result)) = receiver.recv() {
            pending.insert(ordinal, result);
            peak_pending_range_results = peak_pending_range_results.max(pending.len());
            while let Some(result) = pending.remove(&next_write) {
                match result {
                    Ok(mut range) => {
                        peak_worker_reserved_bytes =
                            peak_worker_reserved_bytes.max(range.peak_reserved_bytes);
                        range_cpu_seconds += range.cpu_seconds;
                        if first_error.is_none() {
                            let mut write_result = write_fact_copy_row(
                                &mut writer,
                                &range.fact_block,
                                &mut range.fact_payload,
                            );
                            if write_result.is_ok() {
                                write_result =
                                    match io::copy(&mut range.evidence_spool, &mut writer) {
                                        Ok(_) => Ok(()),
                                        Err(error) => Err(error),
                                    };
                            }
                            if write_result.is_ok() {
                                write_result = append_spool_digest(
                                    &mut fact_identity_digest,
                                    &mut fact_identity_spools,
                                    &mut range.fact_identity_spool,
                                );
                            }
                            if write_result.is_ok() {
                                write_result = append_spool_digest(
                                    &mut evidence_identity_digest,
                                    &mut evidence_identity_spools,
                                    &mut range.evidence_identity_spool,
                                );
                            }
                            if write_result.is_ok() {
                                write_result = append_identity_bytes_digest(
                                    &mut quarantine_identity_digest,
                                    &mut quarantine_identity_spools,
                                    &range.quarantine_identity_bytes,
                                );
                            }
                            if let Err(error) = write_result {
                                first_error = Some(error);
                            }
                        }
                        fact_count += range.fact_block.fact_count;
                        evidence_count += range.evidence_range.evidence_count;
                        evidence_run_count += range.evidence_range.run_count;
                        counters.merge(&range.counters);
                        fact_blocks.push(range.fact_block);
                        evidence_ranges.push(range.evidence_range);
                    }
                    Err(error) if first_error.is_none() => first_error = Some(error),
                    Err(_) => {}
                }
                next_write += 1;
            }
        }
    });
    if next_write != ranges.len() && first_error.is_none() {
        first_error = Some(invalid("UHC semantic range worker output is incomplete"));
    }
    if let Some(error) = first_error {
        return Err(error);
    }
    writer.write_all(&(-1i16).to_be_bytes())?;
    writer.flush()?;
    let output_bytes = writer.bytes;
    let output_sha256 = hex_digest(writer.digest.finalize().as_slice());

    if fact_count != ranges.iter().map(|range| range.record_count).sum::<u64>() {
        return Err(invalid(
            "UHC semantic fact total does not match admitted records",
        ));
    }
    if counters.invalid_npi_count > provider_quarantine_limit(fact_count) {
        return Err(invalid(
            "UHC provider quarantine exceeds the file rate ceiling",
        ));
    }
    let expected_evidence = match source.lineage().collection_kind {
        UhcCollectionKind::ProviderMembership => fact_count
            .checked_sub(counters.invalid_npi_count)
            .ok_or_else(|| invalid("UHC semantic quarantine total overflowed"))?,
        UhcCollectionKind::PlanReference => 0,
    };
    if evidence_count != expected_evidence {
        return Err(invalid(
            "UHC semantic evidence total does not match admitted provider records",
        ));
    }
    let quarantine_count = counters.invalid_npi_count;

    Ok(SemanticEncodeReport {
        contract_id: UHC_SEMANTIC_FACT_CONTRACT_ID,
        contract_version: UHC_SEMANTIC_FACT_CONTRACT_VERSION,
        copy_format_id: UHC_SEMANTIC_COPY_FORMAT_ID,
        source_id: UHC_SEMANTIC_SOURCE_ID,
        lineage: source.lineage().into(),
        counters,
        fact_count,
        evidence_count,
        quarantine_count,
        quarantine_identity_set_sha256: hex_digest(
            quarantine_identity_digest.finalize().as_slice(),
        ),
        fact_set_sha256: fact_set_sha256(&fact_blocks),
        record_identity_set_sha256: hex_digest(fact_identity_digest.finalize().as_slice()),
        evidence_identity_set_sha256: hex_digest(evidence_identity_digest.finalize().as_slice()),
        evidence_layout_set_sha256: evidence_layout_set_sha256(&evidence_ranges),
        fact_blocks,
        evidence_ranges,
        evidence_run_count,
        output_bytes,
        output_sha256,
        copy_row_count: evidence_count + ranges.len() as u64,
        worker_count,
        per_worker_memory_budget_bytes: budget.per_worker_bytes,
        total_memory_budget_bytes: budget.total_bytes,
        max_record_bytes: budget.max_record_bytes,
        evidence_buffer_bytes: budget.evidence_buffer_bytes,
        pending_quarantine_identity_budget_bytes: PENDING_QUARANTINE_IDENTITY_BYTES,
        peak_worker_reserved_bytes,
        peak_pending_range_results,
        range_cpu_seconds,
        elapsed_seconds: started.elapsed().as_secs_f64(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::uhc_retained::{retain_uhc_artifact, UHCRetainRequest, UHCRetainedManifest};
    use std::fs;

    const PROVIDER_RECORD: &[u8] = br#"{
        "type":"INDIVIDUAL",
        "npi":"1003821380",
        "name":{"first":"Ada","middle":null,"last":"Lovelace"},
        "facility_name":null,
        "facility_type":null,
        "gender":"F",
        "accepting":"accepting",
        "addresses":[{
            "address":"1 Main St",
            "city":"Chicago",
            "state":"IL",
            "zip":"60601",
            "phone":"3125551212"
        }],
        "plans":[{
            "plan_id_type":"HIOS-PLAN-ID",
            "plan_id":"12345IL0010001",
            "years":[2026],
            "network_tier":"PREFERRED"
        }],
        "specialty":["Family Medicine"],
        "last_updated_on":"2026-07-01"
    }"#;

    const PLAN_RECORD: &[u8] = br#"{
        "plan_id_type":"HIOS-PLAN-ID",
        "plan_id":"12345IL0010001",
        "years":[2026],
        "marketing_name":"Example Plan",
        "marketing_url":"https://example.test/plan",
        "summary_url":null,
        "formulary_url":null,
        "plan_contact":"8005551212",
        "network":[{"network_tier":"PREFERRED"}],
        "formulary":[{"drug_tier":"GENERIC","mail_order":true}],
        "last_updated_on":"2026-07-01"
    }"#;

    const CHECKSUM_INVALID_PROVIDER_RECORD: &[u8] = br#"{
        "type":"INDIVIDUAL",
        "npi":"1003821381",
        "name":{"first":"Ada","middle":null,"last":"Lovelace"},
        "facility_name":null,
        "facility_type":null,
        "gender":"F",
        "accepting":"accepting",
        "addresses":[{
            "address":"1 Main St",
            "city":"Chicago",
            "state":"IL",
            "zip":"60601",
            "phone":"3125551212"
        }],
        "plans":[{
            "plan_id_type":"HIOS-PLAN-ID",
            "plan_id":"12345IL0010001",
            "years":[2026],
            "network_tier":"PREFERRED"
        }],
        "specialty":["Family Medicine"],
        "last_updated_on":"2026-07-01"
    }"#;

    struct SyntheticSource {
        lineage: AdmittedSemanticLineage,
        ranges: Vec<AdmittedSemanticRange>,
        record: Vec<u8>,
    }

    impl SyntheticSource {
        fn new(record_count: u64, collection_kind: UhcCollectionKind) -> Self {
            assert_eq!(record_count % 4, 0);
            let record = match collection_kind {
                UhcCollectionKind::ProviderMembership => PROVIDER_RECORD.to_vec(),
                UhcCollectionKind::PlanReference => PLAN_RECORD.to_vec(),
            };
            let per_range = record_count / 4;
            let ranges = (0..4)
                .map(|ordinal| AdmittedSemanticRange {
                    range_ordinal: ordinal,
                    record_start: ordinal * per_range,
                    record_count: per_range,
                    raw_byte_count: per_range * record.len() as u64,
                    raw_sha256: "a".repeat(64),
                    canonical_sha256: "b".repeat(64),
                })
                .collect();
            Self {
                lineage: AdmittedSemanticLineage {
                    artifact_sha256: "c".repeat(64),
                    manifest_sha256: "d".repeat(64),
                    range_set_sha256: "e".repeat(64),
                    source_file_id: "f".repeat(64),
                    source_binding_id: "synthetic/test".to_owned(),
                    collection_kind,
                },
                ranges,
                record,
            }
        }
    }

    impl AdmittedRangeSource for SyntheticSource {
        fn lineage(&self) -> &AdmittedSemanticLineage {
            &self.lineage
        }

        fn ranges(&self) -> &[AdmittedSemanticRange] {
            &self.ranges
        }

        fn visit_verified_records(
            &self,
            range: &AdmittedSemanticRange,
            visitor: &mut dyn FnMut(u64, &[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            for offset in 0..range.record_count {
                visitor(range.record_start + offset, &self.record)?;
            }
            Ok(())
        }
    }

    struct MixedProviderSource {
        inner: SyntheticSource,
        invalid_ordinals: Vec<u64>,
    }

    impl AdmittedRangeSource for MixedProviderSource {
        fn lineage(&self) -> &AdmittedSemanticLineage {
            self.inner.lineage()
        }

        fn ranges(&self) -> &[AdmittedSemanticRange] {
            self.inner.ranges()
        }

        fn visit_verified_records(
            &self,
            range: &AdmittedSemanticRange,
            visitor: &mut dyn FnMut(u64, &[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            for offset in 0..range.record_count {
                let ordinal = range.record_start + offset;
                let record = if self.invalid_ordinals.contains(&ordinal) {
                    CHECKSUM_INVALID_PROVIDER_RECORD
                } else {
                    PROVIDER_RECORD
                };
                visitor(ordinal, record)?;
            }
            Ok(())
        }
    }

    fn test_budget() -> SemanticMemoryBudget {
        SemanticMemoryBudget {
            worker_count: 2,
            per_worker_bytes: 4 * 1024 * 1024,
            total_bytes: 11 * 1024 * 1024,
            max_record_bytes: 16 * 1024,
            evidence_buffer_bytes: 64 * 1024,
        }
    }

    struct FailAfter {
        remaining: usize,
        bytes: Vec<u8>,
    }

    struct SerializationFailure;

    impl Serialize for SerializationFailure {
        fn serialize<S>(&self, _serializer: S) -> Result<S::Ok, S::Error>
        where
            S: serde::Serializer,
        {
            Err(serde::ser::Error::custom("injected serialization failure"))
        }
    }

    impl Write for FailAfter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            if self.remaining == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "injected COPY crash",
                ));
            }
            let accepted = buffer.len().min(self.remaining);
            self.bytes.extend_from_slice(&buffer[..accepted]);
            self.remaining -= accepted;
            Ok(accepted)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn copy_stream_and_proofs_are_deterministic() {
        let source = SyntheticSource::new(400, UhcCollectionKind::ProviderMembership);
        let budget = test_budget();
        let mut first = Vec::new();
        let first_report = encode_admitted_ranges_to_copy(&source, &mut first, &budget)
            .expect("encode first semantic stream");
        let mut second = Vec::new();
        let second_report = encode_admitted_ranges_to_copy(&source, &mut second, &budget)
            .expect("encode second semantic stream");

        assert_eq!(first, second);
        assert!(first.starts_with(b"PGCOPY\n\xff\r\n\0"));
        assert!(first.ends_with(&(-1i16).to_be_bytes()));
        assert_eq!(first_report.fact_count, 400);
        assert_eq!(first_report.evidence_count, 400);
        assert_eq!(first_report.fact_set_sha256, second_report.fact_set_sha256);
        assert_eq!(
            first_report.record_identity_set_sha256,
            second_report.record_identity_set_sha256
        );
        assert_eq!(
            first_report.evidence_identity_set_sha256,
            second_report.evidence_identity_set_sha256
        );

        let large_source = SyntheticSource::new(20_000, UhcCollectionKind::ProviderMembership);
        let evidence_failure = FailAfter {
            remaining: 256 * 1024,
            bytes: Vec::new(),
        };
        assert_eq!(
            encode_admitted_ranges_to_copy(&large_source, evidence_failure, &budget)
                .unwrap_err()
                .kind(),
            io::ErrorKind::BrokenPipe
        );
        assert_eq!(
            first_report.evidence_layout_set_sha256,
            second_report.evidence_layout_set_sha256
        );
        assert_eq!(first_report.counters, second_report.counters);
        assert!(first_report.peak_worker_reserved_bytes <= budget.per_worker_bytes);
    }

    #[test]
    fn checksum_invalid_npi_is_redacted_without_shifting_fact_ordinals() {
        let source = MixedProviderSource {
            inner: SyntheticSource::new(4, UhcCollectionKind::ProviderMembership),
            invalid_ordinals: vec![1],
        };
        let mut serial_budget = test_budget();
        serial_budget.worker_count = 1;
        let mut serial_copy = Vec::new();
        let serial_report =
            encode_admitted_ranges_to_copy(&source, &mut serial_copy, &serial_budget).unwrap();
        let mut parallel_copy = Vec::new();
        let parallel_report =
            encode_admitted_ranges_to_copy(&source, &mut parallel_copy, &test_budget()).unwrap();

        assert_eq!(serial_copy, parallel_copy);
        assert_eq!(serial_report.fact_count, 4);
        assert_eq!(serial_report.evidence_count, 3);
        assert_eq!(serial_report.quarantine_count, 1);
        assert_eq!(serial_report.counters.invalid_npi_count, 1);
        assert_eq!(serial_report.counters.invalid_npi_individual_records, 1);
        assert_eq!(serial_report.counters.invalid_npi_facility_records, 0);
        assert_eq!(serial_report.counters.invalid_npi_address_rows, 1);
        assert_eq!(serial_report.counters.invalid_npi_provider_plan_rows, 1);
        assert_eq!(
            serial_report.quarantine_identity_set_sha256,
            parallel_report.quarantine_identity_set_sha256
        );
        assert_eq!(serial_report.fact_blocks[1].record_start, 1);
        assert_eq!(serial_report.fact_blocks[2].record_start, 2);

        let invalid_range = &source.ranges()[1];
        let mut worker = RangeWorker::new(
            &source.lineage().source_file_id,
            source.lineage().collection_kind,
            invalid_range,
            &test_budget(),
        )
        .unwrap();
        worker
            .process_provider(1, CHECKSUM_INVALID_PROVIDER_RECORD)
            .unwrap();
        let mut result = worker.finish(Instant::now()).unwrap();
        let mut decoded = String::new();
        flate2::read::ZlibDecoder::new(&mut result.fact_payload)
            .read_to_string(&mut decoded)
            .unwrap();
        let fact: serde_json::Value = serde_json::from_str(decoded.trim()).unwrap();
        let payload = &fact["_healthporta_quarantine"];
        assert_eq!(fact.as_object().unwrap().len(), 1);
        assert_eq!(payload["contract_id"], UHC_PROVIDER_QUARANTINE_CONTRACT_ID);
        assert_eq!(
            payload["reason"],
            UHC_PROVIDER_QUARANTINE_REASON_INVALID_NPI_CHECKSUM
        );
        assert_eq!(payload["range_ordinal"], 1);
        assert_eq!(payload["occurrence_ordinal"], 1);
        assert_eq!(
            payload["record_sha256"],
            sha256(CHECKSUM_INVALID_PROVIDER_RECORD)
        );
        assert!(!decoded.contains("1003821381"));
        assert!(!decoded.contains("Lovelace"));
        assert!(!decoded.contains("addresses"));
        assert!(!decoded.contains("plans"));

        let multi_year_record = String::from_utf8(CHECKSUM_INVALID_PROVIDER_RECORD.to_vec())
            .unwrap()
            .replace("\"years\":[2026]", "\"years\":[2025,2026]")
            .into_bytes();
        let mut multi_year_worker = RangeWorker::new(
            &source.lineage().source_file_id,
            source.lineage().collection_kind,
            invalid_range,
            &test_budget(),
        )
        .unwrap();
        multi_year_worker
            .process_provider(1, &multi_year_record)
            .unwrap();
        let multi_year_result = multi_year_worker.finish(Instant::now()).unwrap();
        assert_eq!(multi_year_result.counters.raw_provider_plan_rows, 2);
        assert_eq!(multi_year_result.counters.plan_year_rows, 2);
        assert_eq!(multi_year_result.counters.invalid_npi_provider_plan_rows, 2);
    }

    #[test]
    fn encoder_enforces_quarantine_rate_and_other_fields_still_fail_closed() {
        let excessive = MixedProviderSource {
            inner: SyntheticSource::new(4, UhcCollectionKind::ProviderMembership),
            invalid_ordinals: vec![1, 2],
        };
        assert!(
            encode_admitted_ranges_to_copy(&excessive, io::sink(), &test_budget())
                .unwrap_err()
                .to_string()
                .contains("exceeds the file rate ceiling")
        );

        let mut malformed = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        malformed.record = String::from_utf8(CHECKSUM_INVALID_PROVIDER_RECORD.to_vec())
            .unwrap()
            .replace("\"accepting\":\"accepting\"", "\"accepting\":\"sometimes\"")
            .into_bytes();
        assert!(
            encode_admitted_ranges_to_copy(&malformed, io::sink(), &test_budget())
                .unwrap_err()
                .to_string()
                .contains("accepting status is unsupported")
        );
    }

    #[test]
    fn failed_copy_has_no_report_and_retry_is_byte_identical() {
        let source = SyntheticSource::new(400, UhcCollectionKind::ProviderMembership);
        let budget = test_budget();
        let failure = FailAfter {
            remaining: 8 * 1024,
            bytes: Vec::new(),
        };
        let error = encode_admitted_ranges_to_copy(&source, failure, &budget)
            .expect_err("injected COPY failure must abort");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);

        let mut first_retry = Vec::new();
        let first_report = encode_admitted_ranges_to_copy(&source, &mut first_retry, &budget)
            .expect("first retry");
        let mut second_retry = Vec::new();
        let second_report = encode_admitted_ranges_to_copy(&source, &mut second_retry, &budget)
            .expect("second retry");
        assert_eq!(first_retry, second_retry);
        assert_eq!(first_report.fact_set_sha256, second_report.fact_set_sha256);
        assert_eq!(
            first_report.evidence_identity_set_sha256,
            second_report.evidence_identity_set_sha256
        );
    }

    #[test]
    fn plan_files_emit_no_npi_evidence() {
        let source = SyntheticSource::new(40, UhcCollectionKind::PlanReference);
        let report = encode_admitted_ranges_to_copy(&source, io::sink(), &test_budget())
            .expect("encode plans");
        assert_eq!(report.fact_count, 40);
        assert_eq!(report.evidence_count, 0);
        assert_eq!(report.evidence_run_count, 0);
        assert_eq!(report.quarantine_count, 0);
        assert_eq!(report.quarantine_identity_set_sha256, sha256(b""));
        assert_eq!(report.counters.raw_plan_records, 40);
        assert_eq!(report.counters.raw_formulary_entries, 40);
        assert_eq!(report.counters.plan_year_rows, 40);
    }

    #[test]
    fn hard_budget_rejects_an_unbounded_configuration() {
        let budget = SemanticMemoryBudget {
            worker_count: 4,
            per_worker_bytes: 4 * 1024 * 1024,
            total_bytes: 32 * 1024 * 1024,
            max_record_bytes: 4 * 1024 * 1024,
            evidence_buffer_bytes: 8 * 1024 * 1024,
        };
        let error = budget
            .validate()
            .expect_err("configuration cannot overcommit a worker");
        assert!(error.to_string().contains("per-worker budget is too small"));
    }

    #[test]
    fn nested_unknown_fields_fail_closed() {
        let mut source = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        source.record = PROVIDER_RECORD
            .iter()
            .copied()
            .take_while(|byte| *byte != b'}')
            .collect();
        source.record = br#"{
            "type":"INDIVIDUAL",
            "npi":"1003821380",
            "name":{"first":"Ada","middle":null,"last":"Lovelace","nickname":"A"},
            "facility_name":null,
            "facility_type":null,
            "gender":"F",
            "accepting":"accepting",
            "addresses":[{"address":"1 Main","city":"Chicago","state":"IL","zip":"60601","phone":null}],
            "plans":[{"plan_id_type":"HIOS","plan_id":"P","years":[2026],"network_tier":null}],
            "specialty":null,
            "last_updated_on":null
        }"#
            .to_vec();
        let error = encode_admitted_ranges_to_copy(&source, io::sink(), &test_budget())
            .expect_err("unknown nested field must fail");
        assert!(error.to_string().contains("unknown field"));
    }

    #[test]
    fn admission_budget_and_source_validation_fail_closed_at_every_boundary() {
        let mut budget = test_budget();
        budget.worker_count = 0;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("1..=64"));
        budget.worker_count = 1;
        budget.max_record_bytes = 0;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("must be positive"));
        budget.max_record_bytes = 1;
        budget.evidence_buffer_bytes = 1;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("at least 64 KiB"));
        budget.evidence_buffer_bytes = 64 * 1024;
        budget.max_record_bytes = usize::MAX;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("overflowed"));
        budget.max_record_bytes = (usize::MAX - RECORD_FIXED_BYTES) / RECORD_EXPANSION_FACTOR;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("worker budget overflowed"));
        budget.max_record_bytes = 1;
        budget.evidence_buffer_bytes = usize::MAX;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("worker budget overflowed"));
        assert!(record_reservation(usize::MAX / RECORD_EXPANSION_FACTOR)
            .unwrap_err()
            .to_string()
            .contains("reservation overflowed"));
        budget.evidence_buffer_bytes = 64 * 1024;
        budget.max_record_bytes = 1;
        budget.per_worker_bytes = usize::MAX;
        budget.worker_count = 64;
        budget.total_bytes = usize::MAX;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("total budget overflowed"));
        budget.worker_count = 1;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("total budget overflowed"));
        budget.per_worker_bytes = 4 * 1024 * 1024;
        budget.worker_count = 2;
        budget.total_bytes = 4 * 1024 * 1024;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("total budget is too small"));
        let exact_total = budget.per_worker_bytes * budget.worker_count
            + COPY_BUFFER_BYTES
            + PENDING_QUARANTINE_IDENTITY_BYTES;
        budget.total_bytes = exact_total - 1;
        assert!(budget
            .validate()
            .unwrap_err()
            .to_string()
            .contains("total budget is too small"));
        budget.total_bytes = exact_total;
        budget.validate().expect("accept exact total memory bound");

        let valid = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        let mut invalid_lineage = valid.lineage.clone();
        invalid_lineage.artifact_sha256 = "A".repeat(64);
        assert!(invalid_lineage
            .validate()
            .unwrap_err()
            .to_string()
            .contains("lowercase SHA-256"));
        invalid_lineage = valid.lineage.clone();
        invalid_lineage.source_binding_id = " padded ".to_owned();
        assert!(invalid_lineage
            .validate()
            .unwrap_err()
            .to_string()
            .contains("binding ID"));

        let mut empty = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        empty.ranges.clear();
        assert!(validate_source(&empty)
            .unwrap_err()
            .to_string()
            .contains("range count"));
        let mut too_many = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        too_many.ranges = vec![too_many.ranges[0].clone(); MAX_RANGE_COUNT + 1];
        assert!(validate_source(&too_many)
            .unwrap_err()
            .to_string()
            .contains("range count"));

        for mutation in 0..7 {
            let mut malformed = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
            match mutation {
                0 => malformed.ranges[0].raw_sha256 = "x".to_owned(),
                1 => malformed.ranges[0].canonical_sha256 = "Y".repeat(64),
                2 => malformed.ranges[0].range_ordinal = 1,
                3 => malformed.ranges[0].record_start = 1,
                4 => malformed.ranges[0].record_count = 0,
                5 => malformed.ranges[0].raw_byte_count = 0,
                _ => malformed.ranges[0].canonical_sha256 = "x".to_owned(),
            }
            assert!(validate_source(&malformed).is_err());
        }
        for mutation in 0..7 {
            let mut malformed = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
            match mutation {
                0 => malformed.lineage.manifest_sha256 = "x".to_owned(),
                1 => malformed.lineage.range_set_sha256 = "x".to_owned(),
                2 => malformed.lineage.source_file_id = "x".to_owned(),
                3 => malformed.lineage.source_binding_id.clear(),
                4 => malformed.lineage.source_binding_id = "x".repeat(257),
                5 => malformed.lineage.source_binding_id = " padded".to_owned(),
                _ => malformed.lineage.source_binding_id = "control\nbyte".to_owned(),
            }
            assert!(validate_source(&malformed).is_err());
        }
        let mut overflow = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        overflow.ranges.truncate(2);
        overflow.ranges[0].record_count = u64::MAX;
        overflow.ranges[1].record_start = u64::MAX;
        assert!(validate_source(&overflow)
            .unwrap_err()
            .to_string()
            .contains("record count overflowed"));
    }

    #[test]
    fn normalization_and_copy_helpers_cover_success_and_rejection_shapes() {
        assert_eq!(clean(None, false), None);
        assert_eq!(clean(Some(" \0 "), false), None);
        assert_eq!(
            clean(Some(" mixed Case "), false).as_deref(),
            Some("mixed Case")
        );
        assert_eq!(
            clean(Some(" mixed Case "), true).as_deref(),
            Some("MIXED CASE")
        );
        assert_eq!(npi_validity("123"), NpiValidity::Invalid);
        assert_eq!(npi_validity("100382138x"), NpiValidity::Invalid);
        assert_eq!(npi_validity("0000000000"), NpiValidity::Invalid);
        assert_eq!(npi_validity("1003821381"), NpiValidity::ChecksumInvalid);
        assert_eq!(npi_validity("1003821380"), NpiValidity::Valid);
        assert_eq!(provider_quarantine_limit(0), 0);
        assert_eq!(provider_quarantine_limit(1), 1);
        assert_eq!(provider_quarantine_limit(1_000_000), 1);
        assert_eq!(provider_quarantine_limit(1_000_001), 2);
        assert_eq!(provider_quarantine_limit(32_000_000), 32);
        assert_eq!(provider_quarantine_limit(u64::MAX), 32);

        assert_eq!(accepting_code(None).unwrap(), None);
        assert_eq!(accepting_code(Some("yes")).unwrap(), Some("newpt"));
        assert_eq!(
            accepting_code(Some("accepting-new patients")).unwrap(),
            Some("newpt")
        );
        assert_eq!(accepting_code(Some("closed")).unwrap(), Some("nopt"));
        assert!(accepting_code(Some("sometimes")).is_err());
        assert_eq!(normalized_phone(Some("+1 (312) 555-1212")), None);
        assert_eq!(normalized_phone(None), None);
        assert_eq!(normalized_phone(Some("---")), None);
        assert_eq!(
            normalized_phone(Some("(312) 555-1212")).as_deref(),
            Some("3125551212")
        );
        assert_eq!(normalized_phone(Some("555-1212")), None);
        assert!(required_years(&[]).is_err());
        assert!(required_years(&[1999]).is_err());
        assert!(required_years(&[2101]).is_err());
        required_years(&[2000, 2100]).unwrap();
        assert_eq!(
            UhcCollectionKind::ProviderMembership.fact_type(),
            "ProviderMembershipRecord"
        );
        assert_eq!(
            UhcCollectionKind::PlanReference.fact_type(),
            "PlanReferenceRecord"
        );

        let mut bytes = Vec::new();
        assert!(write_i64(&mut bytes, u64::MAX)
            .unwrap_err()
            .to_string()
            .contains("signed 64-bit"));
        let mut payload = tempfile::tempfile().unwrap();
        payload.write_all(b"short").unwrap();
        payload.seek(SeekFrom::Start(0)).unwrap();
        let oversized = FactBlockProof {
            range_ordinal: 0,
            record_start: 0,
            record_count: 1,
            fact_count: 1,
            compressed_bytes: i32::MAX as u64 + 1,
            compressed_payload_sha256: "a".repeat(64),
            semantic_block_sha256: "b".repeat(64),
        };
        assert!(
            write_fact_copy_row(&mut Vec::new(), &oversized, &mut payload)
                .unwrap_err()
                .to_string()
                .contains("COPY bytea limit")
        );
        let mut truncated = oversized;
        truncated.compressed_bytes = 6;
        payload.seek(SeekFrom::Start(0)).unwrap();
        assert!(
            write_fact_copy_row(&mut Vec::new(), &truncated, &mut payload)
                .unwrap_err()
                .to_string()
                .contains("ended unexpectedly")
        );
        assert!(canonical_value_bytes(&SerializationFailure)
            .unwrap_err()
            .to_string()
            .contains("injected serialization failure"));
        assert!(optional_signature(&Some(SerializationFailure))
            .unwrap_err()
            .to_string()
            .contains("injected serialization failure"));
        assert!(sorted_signature(&Some(vec![SerializationFailure]))
            .unwrap_err()
            .to_string()
            .contains("injected serialization failure"));
    }

    #[test]
    fn copy_row_writers_propagate_failures_at_every_field_boundary() {
        let row = evidence_row(4, 7);
        let mut complete_evidence = Vec::new();
        write_evidence_copy_row(&mut complete_evidence, 3, 5, &row).unwrap();
        for remaining in 0..complete_evidence.len() {
            let mut failing = FailAfter {
                remaining,
                bytes: Vec::new(),
            };
            assert_eq!(
                write_evidence_copy_row(&mut failing, 3, 5, &row)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe,
                "evidence writer unexpectedly succeeded with {remaining} writable bytes",
            );
        }

        let payload_bytes = b"compressed semantic facts";
        let block = FactBlockProof {
            range_ordinal: 3,
            record_start: 12,
            record_count: 4,
            fact_count: 4,
            compressed_bytes: payload_bytes.len() as u64,
            compressed_payload_sha256: "a".repeat(64),
            semantic_block_sha256: "b".repeat(64),
        };
        let mut complete_payload = tempfile::tempfile().unwrap();
        complete_payload.write_all(payload_bytes).unwrap();
        complete_payload.seek(SeekFrom::Start(0)).unwrap();
        let mut complete_fact = Vec::new();
        write_fact_copy_row(&mut complete_fact, &block, &mut complete_payload).unwrap();
        for remaining in 0..complete_fact.len() {
            let mut payload = tempfile::tempfile().unwrap();
            payload.write_all(payload_bytes).unwrap();
            payload.seek(SeekFrom::Start(0)).unwrap();
            let mut failing = FailAfter {
                remaining,
                bytes: Vec::new(),
            };
            assert_eq!(
                write_fact_copy_row(&mut failing, &block, &mut payload)
                    .unwrap_err()
                    .kind(),
                io::ErrorKind::BrokenPipe,
                "fact writer unexpectedly succeeded with {remaining} writable bytes",
            );
        }
    }

    #[test]
    fn provider_and_plan_semantics_cover_rich_records_and_rejection_paths() {
        let mut facility = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        facility.record = br#"{
            "type":"FACILITY",
            "npi":"1003821380",
            "name":null,
            "facility_name":"Example Clinic",
            "facility_type":["Clinic","Urgent Care"],
            "gender":null,
            "accepting":"not accepting",
            "addresses":[
                {"address":"1 Main","city":"Chicago","state":"IL","zip":"60601","phone":"3125551212"},
                {"address":"2 Main","city":"Chicago","state":"IL","zip":"60602","phone":"555-1212"}
            ],
            "plans":[{"plan_id_type":"HIOS","plan_id":"P","years":[2025,2026],"network_tier":null}],
            "specialty":null,
            "last_updated_on":null
        }"#
        .to_vec();
        let record: ProviderRecord = serde_json::from_slice(&facility.record).unwrap();
        let facility_value: serde_json::Value =
            serde_json::from_slice(&canonical_value_bytes(&record).unwrap()).unwrap();
        for prohibited_field in ["tin", "tax_id"] {
            let mut invalid_value = facility_value.clone();
            invalid_value[prohibited_field] = serde_json::json!("123456789");
            assert!(serde_json::from_value::<ProviderRecord>(invalid_value).is_err());
        }
        let report = encode_admitted_ranges_to_copy(&facility, io::sink(), &test_budget()).unwrap();
        assert_eq!(report.counters.raw_facility_records, 4);
        assert_eq!(report.counters.named_facility_records, 4);
        assert_eq!(report.counters.facility_type_values, 8);
        assert_eq!(report.counters.accepting_nopt_records, 4);
        assert_eq!(report.counters.valid_phone_count, 4);
        assert_eq!(report.counters.invalid_phone_count, 4);
        assert_eq!(report.counters.multi_address_provider_records, 4);
        assert_eq!(report.counters.raw_provider_plan_rows, 8);
        assert_eq!(report.counters.plan_year_rows, 8);

        let mut facility_without_types =
            SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        facility_without_types.record = br#"{
            "type":"FACILITY","npi":"1003821380","name":null,
            "facility_name":"Example Clinic","facility_type":null,"gender":null,
            "accepting":null,"addresses":[{}],
            "plans":[{"plan_id_type":"HIOS","plan_id":"P","years":[2026],"network_tier":null}],
            "specialty":null,"last_updated_on":null
        }"#
        .to_vec();
        let report =
            encode_admitted_ranges_to_copy(&facility_without_types, io::sink(), &test_budget())
                .unwrap();
        assert_eq!(report.counters.facility_type_values, 0);

        let provider_failures: &[(&[u8], &str)] = &[
            (
                br#"{"type":"INDIVIDUAL","npi":"123","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "structurally valid",
            ),
            (
                br#"{"type":"UNKNOWN","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "type is unsupported",
            ),
            (
                br#"{"type":" ","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "type is empty",
            ),
            (
                br#"{"type":"INDIVIDUAL","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[],"plans":[{"plan_id_type":"H","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "must be nonempty",
            ),
            (
                br#"{"type":"INDIVIDUAL","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":"sometimes","addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "accepting status",
            ),
            (
                br#"{"type":"INDIVIDUAL","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":"P","years":[],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "years are invalid",
            ),
            (
                br#"{"type":"INDIVIDUAL","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":" ","plan_id":"P","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "plan ID type is empty",
            ),
            (
                br#"{"type":"INDIVIDUAL","npi":"1003821380","name":null,"facility_name":null,"facility_type":null,"gender":null,"accepting":null,"addresses":[{}],"plans":[{"plan_id_type":"H","plan_id":" ","years":[2026],"network_tier":null}],"specialty":null,"last_updated_on":null}"#,
                "plan ID is empty",
            ),
        ];
        for (record, expected) in provider_failures {
            let mut source = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
            source.record = record.to_vec();
            assert!(
                encode_admitted_ranges_to_copy(&source, io::sink(), &test_budget())
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }

        let plan_failures: &[(&[u8], &str)] = &[
            (b"not-json", "invalid retained UHC plan JSON"),
            (
                br#"{"plan_id_type":"H","plan_id":"P","years":[],"marketing_name":null,"marketing_url":null,"summary_url":null,"formulary_url":null,"plan_contact":null,"network":null,"formulary":null,"last_updated_on":null}"#,
                "years are invalid",
            ),
            (
                br#"{"plan_id_type":" ","plan_id":"P","years":[2026],"marketing_name":null,"marketing_url":null,"summary_url":null,"formulary_url":null,"plan_contact":null,"network":null,"formulary":null,"last_updated_on":null}"#,
                "plan ID type is empty",
            ),
            (
                br#"{"plan_id_type":"H","plan_id":" ","years":[2026],"marketing_name":null,"marketing_url":null,"summary_url":null,"formulary_url":null,"plan_contact":null,"network":null,"formulary":null,"last_updated_on":null}"#,
                "plan ID is empty",
            ),
        ];
        for (record, expected) in plan_failures {
            let mut source = SyntheticSource::new(4, UhcCollectionKind::PlanReference);
            source.record = record.to_vec();
            assert!(
                encode_admitted_ranges_to_copy(&source, io::sink(), &test_budget())
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }

        let provider_source = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        let provider_range = provider_source.ranges[0].clone();
        let mut provider_budget = test_budget();
        provider_budget.max_record_bytes = 1;
        let mut provider_worker = RangeWorker::new(
            &provider_source.lineage.source_file_id,
            provider_source.lineage.collection_kind,
            &provider_range,
            &provider_budget,
        )
        .unwrap();
        assert!(provider_worker
            .process_provider(0, &provider_source.record)
            .unwrap_err()
            .to_string()
            .contains("canonical UHC provider fact"));

        let plan_source = SyntheticSource::new(4, UhcCollectionKind::PlanReference);
        let plan_range = plan_source.ranges[0].clone();
        let mut plan_budget = test_budget();
        plan_budget.max_record_bytes = 1;
        let mut plan_worker = RangeWorker::new(
            &plan_source.lineage.source_file_id,
            plan_source.lineage.collection_kind,
            &plan_range,
            &plan_budget,
        )
        .unwrap();
        assert!(plan_worker
            .process_plan(0, br#"{"plan_id_type":"H","plan_id":"P","years":[2026],"marketing_name":null,"marketing_url":null,"summary_url":null,"formulary_url":null,"plan_contact":null,"network":null,"formulary":null,"last_updated_on":null}"#)
            .unwrap_err()
            .to_string()
            .contains("canonical UHC plan fact"));
    }

    struct FaultySource {
        inner: SyntheticSource,
        behavior: u8,
    }

    impl AdmittedRangeSource for FaultySource {
        fn lineage(&self) -> &AdmittedSemanticLineage {
            &self.inner.lineage
        }

        fn ranges(&self) -> &[AdmittedSemanticRange] {
            &self.inner.ranges
        }

        fn visit_verified_records(
            &self,
            range: &AdmittedSemanticRange,
            visitor: &mut dyn FnMut(u64, &[u8]) -> io::Result<()>,
        ) -> io::Result<()> {
            match self.behavior {
                0 => visitor(range.record_start + 1, &self.inner.record),
                1 => Ok(()),
                2 => Err(invalid("injected retained visit failure")),
                3 => visitor(range.record_start, &self.inner.record),
                _ => Err(invalid("injected retained visit behavior")),
            }
        }
    }

    #[test]
    fn retained_visits_require_contiguous_complete_successful_records() {
        for (behavior, expected) in [
            (0, "record ordinals are not contiguous"),
            (1, "did not cover the complete range"),
            (2, "injected retained visit failure"),
            (4, "injected retained visit behavior"),
        ] {
            let source = FaultySource {
                inner: SyntheticSource::new(4, UhcCollectionKind::ProviderMembership),
                behavior,
            };
            assert!(
                encode_admitted_ranges_to_copy(&source, io::sink(), &test_budget())
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }

        let mut occurrence_overflow = FaultySource {
            inner: SyntheticSource::new(4, UhcCollectionKind::ProviderMembership),
            behavior: 3,
        };
        occurrence_overflow.inner.ranges[0].record_start = u64::MAX;
        assert!(process_range(
            &occurrence_overflow,
            &occurrence_overflow.inner.ranges[0],
            &test_budget(),
        )
        .err()
        .expect("occurrence overflow")
        .to_string()
        .contains("occurrence ordinal overflowed"));

        let mut range_end_overflow = FaultySource {
            inner: SyntheticSource::new(4, UhcCollectionKind::ProviderMembership),
            behavior: 1,
        };
        range_end_overflow.inner.ranges[0].record_start = u64::MAX;
        assert!(process_range(
            &range_end_overflow,
            &range_end_overflow.inner.ranges[0],
            &test_budget(),
        )
        .err()
        .expect("range-end overflow")
        .to_string()
        .contains("range end overflowed"));
    }

    fn evidence_row(text_bytes: usize, occurrence_ordinal: u64) -> EvidenceRow {
        EvidenceRow {
            occurrence_ordinal,
            npi: "1003821380".to_owned(),
            provider_type: "INDIVIDUAL".to_owned(),
            name: "n".repeat(text_bytes),
            facility_name: String::new(),
            facility_types: String::new(),
            gender: String::new(),
            accepting: String::new(),
            address_sets: String::new(),
            specialties: String::new(),
            dates: String::new(),
        }
    }

    #[test]
    fn range_worker_memory_counts_and_seals_are_strict() {
        let source = SyntheticSource::new(4, UhcCollectionKind::ProviderMembership);
        let range = source.ranges[0].clone();
        let budget = test_budget();

        let mut empty_flush = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        empty_flush.flush_evidence_run().unwrap();
        assert_eq!(empty_flush.evidence_run_ordinal, 0);

        let mut sorted = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        sorted.push_evidence(evidence_row(1, 2), 1).unwrap();
        sorted.push_evidence(evidence_row(1, 1), 1).unwrap();
        sorted.flush_evidence_run().unwrap();
        assert_eq!(sorted.evidence_run_ordinal, 1);
        assert_eq!(sorted.evidence_count, 2);

        let mut compacted = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        compacted.evidence_rows = Vec::with_capacity(512);
        compacted
            .push_evidence(evidence_row(1, 0), 1)
            .expect("an empty over-reserved buffer is compacted");
        assert_eq!(compacted.evidence_count, 1);

        let mut compacted_after_flush = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        compacted_after_flush.evidence_rows = Vec::with_capacity(512);
        let buffered = evidence_row(1, 0);
        compacted_after_flush.evidence_rows_bytes = buffered.heap_bytes();
        compacted_after_flush.evidence_rows.push(buffered);
        compacted_after_flush
            .push_evidence(evidence_row(1, 1), 1)
            .expect("a flushed over-reserved buffer is compacted");
        assert_eq!(compacted_after_flush.evidence_run_ordinal, 1);

        let mut multiple_runs = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        for ordinal in 0..8 {
            multiple_runs
                .push_evidence(evidence_row(12 * 1024, ordinal), 1)
                .unwrap();
        }
        multiple_runs.flush_evidence_run().unwrap();
        assert!(multiple_runs.evidence_run_ordinal > 1);

        let mut oversized = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        assert!(oversized
            .push_evidence(evidence_row(70 * 1024, 0), 1)
            .unwrap_err()
            .to_string()
            .contains("hard buffer budget"));

        let mut overflow = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        overflow.evidence_rows_bytes = usize::MAX;
        assert!(overflow
            .push_evidence(evidence_row(1, 0), 1)
            .unwrap_err()
            .to_string()
            .contains("overflowed"));

        let mut hard_limit = budget.clone();
        hard_limit.per_worker_bytes = WORKER_FIXED_BYTES;
        let mut over_budget = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &hard_limit,
        )
        .unwrap();
        assert!(over_budget
            .observe_peak(1)
            .unwrap_err()
            .to_string()
            .contains("hard memory budget"));

        let mut reservation_overflow = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        let near_max_record = (usize::MAX - RECORD_FIXED_BYTES) / RECORD_EXPANSION_FACTOR;
        assert!(reservation_overflow
            .observe_peak(near_max_record)
            .unwrap_err()
            .to_string()
            .contains("worker reservation overflowed"));

        let mut row_bytes_overflow = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        row_bytes_overflow.evidence_rows_bytes = usize::MAX;
        assert!(row_bytes_overflow
            .observe_peak(1)
            .unwrap_err()
            .to_string()
            .contains("worker reservation overflowed"));

        let mut capacity_overflow = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        capacity_overflow.evidence_rows = Vec::with_capacity(1);
        let base_reservation = WORKER_FIXED_BYTES + record_reservation(1).unwrap();
        capacity_overflow.evidence_rows_bytes = usize::MAX - base_reservation;
        assert!(capacity_overflow
            .observe_peak(1)
            .unwrap_err()
            .to_string()
            .contains("worker reservation overflowed"));

        let mut projected_overflow = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        projected_overflow.evidence_rows_bytes = usize::MAX - 4 * size_of::<EvidenceRow>();
        assert!(projected_overflow
            .push_evidence(evidence_row(1, 0), 1)
            .unwrap_err()
            .to_string()
            .contains("vector overflowed"));

        let mut unwritable = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        unwritable.evidence_spool =
            BufWriter::new(File::open("/dev/null").expect("read-only evidence sink"));
        let row = evidence_row(12 * 1024, 0);
        unwritable.evidence_rows_bytes = row.heap_bytes();
        unwritable.evidence_rows.push(row);
        unwritable.flush_evidence_run().unwrap();
        assert!(unwritable.evidence_spool.flush().is_err());

        let mut unwritable_fact_identity = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        unwritable_fact_identity.fact_identity_spool =
            BufWriter::with_capacity(0, File::open("/dev/null").unwrap());
        assert!(unwritable_fact_identity.append_fact(0, b"{}").is_err());

        let mut unwritable_evidence_identity = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        unwritable_evidence_identity.evidence_identity_spool =
            BufWriter::with_capacity(0, File::open("/dev/null").unwrap());
        assert!(unwritable_evidence_identity
            .push_evidence(evidence_row(1, 0), 1)
            .is_err());

        let mut unwritable_evidence_row = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        unwritable_evidence_row.evidence_spool =
            BufWriter::with_capacity(0, File::open("/dev/null").unwrap());
        let row = evidence_row(1, 0);
        unwritable_evidence_row.evidence_rows_bytes = row.heap_bytes();
        unwritable_evidence_row.evidence_rows.push(row);
        assert!(unwritable_evidence_row.flush_evidence_run().is_err());

        let mut sealed = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        sealed.fact_encoder.take();
        assert!(sealed
            .append_fact(0, b"{}")
            .unwrap_err()
            .to_string()
            .contains("already sealed"));

        let mut malformed = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        assert!(malformed.process_record(0, b"").is_err());
        assert!(malformed
            .process_record(0, &vec![b'x'; budget.max_record_bytes + 1])
            .is_err());
        assert!(malformed
            .finish(Instant::now())
            .err()
            .expect("fact count mismatch")
            .to_string()
            .contains("fact count does not match"));

        let mut count_mismatch = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        count_mismatch.semantic_fact_count = range.record_count;
        count_mismatch.fact_identity_count = range.record_count;
        assert!(count_mismatch
            .finish(Instant::now())
            .err()
            .expect("evidence count mismatch")
            .to_string()
            .contains("evidence count does not match"));

        let mut already_sealed = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        already_sealed.semantic_fact_count = range.record_count;
        already_sealed.fact_identity_count = range.record_count;
        already_sealed.evidence_count = range.record_count;
        already_sealed.evidence_identity_count = range.record_count;
        already_sealed.fact_encoder.take();
        assert!(already_sealed
            .finish(Instant::now())
            .err()
            .expect("sealed encoder")
            .to_string()
            .contains("already sealed"));

        for spool in 0..4 {
            let mut flush_failure = RangeWorker::new(
                &source.lineage.source_file_id,
                source.lineage.collection_kind,
                &range,
                &budget,
            )
            .unwrap();
            flush_failure.semantic_fact_count = range.record_count;
            flush_failure.fact_identity_count = range.record_count;
            flush_failure.evidence_count = range.record_count;
            flush_failure.evidence_identity_count = range.record_count;
            let mut unwritable = BufWriter::with_capacity(1024, File::open("/dev/null").unwrap());
            unwritable.write_all(b"pending").unwrap();
            match spool {
                0 => flush_failure.fact_identity_spool = unwritable,
                1 => flush_failure.evidence_identity_spool = unwritable,
                2 => flush_failure.evidence_spool = unwritable,
                _ => flush_failure.evidence_spool = unwritable,
            }
            assert!(flush_failure.finish(Instant::now()).is_err());
        }

        let mut identity_newline_failure = RangeWorker::new(
            &source.lineage.source_file_id,
            source.lineage.collection_kind,
            &range,
            &budget,
        )
        .unwrap();
        identity_newline_failure.fact_identity_spool =
            BufWriter::with_capacity(0, File::open("/dev/null").unwrap());
        identity_newline_failure.fact_identity_count = 1;
        assert!(identity_newline_failure.append_fact(0, b"{}").is_err());
    }

    #[test]
    fn replay_lineage_and_range_membership_stay_bound_to_admitted_bytes() {
        let provider = std::str::from_utf8(PROVIDER_RECORD).unwrap();
        let payload = format!("[{provider},{provider},{provider},{provider}]").into_bytes();
        let directory = tempfile::tempdir().unwrap();
        let source_path = directory.path().join("source.json");
        let retained_path = directory.path().join("retained");
        fs::write(&source_path, &payload).unwrap();
        fs::create_dir(&retained_path).unwrap();
        let summary = retain_uhc_artifact(&UHCRetainRequest {
            source_path,
            output_root: retained_path,
            expected_sha256: sha256(&payload),
            expected_byte_count: payload.len() as u64,
            range_count: 4,
        })
        .unwrap();
        let manifest: UHCRetainedManifest =
            serde_json::from_slice(&fs::read(&summary.manifest_path).unwrap()).unwrap();
        let request = UHCVerifiedReplayRequest {
            raw_path: summary.raw_artifact_path.into(),
            manifest_path: summary.manifest_path.into(),
            expected_artifact_sha256: summary.raw_artifact_sha256.clone(),
            expected_artifact_byte_count: summary.raw_artifact_byte_count,
            expected_manifest_sha256: summary.manifest_sha256.clone(),
            expected_range_set_sha256: manifest.range_set_sha256,
            expected_record_count: summary.record_count,
            expected_range_count: summary.range_count as usize,
        };
        let mut lineage = AdmittedSemanticLineage {
            artifact_sha256: summary.raw_artifact_sha256.clone(),
            manifest_sha256: summary.manifest_sha256.clone(),
            range_set_sha256: request.expected_range_set_sha256.clone(),
            source_file_id: summary.raw_artifact_sha256,
            source_binding_id: "synthetic/replay".to_owned(),
            collection_kind: UhcCollectionKind::ProviderMembership,
        };
        lineage.artifact_sha256 = "0".repeat(64);
        assert!(UhcSemanticReplaySource::open(&request, lineage)
            .err()
            .expect("lineage mismatch")
            .to_string()
            .contains("does not match"));

        let mut invalid_request = request.clone();
        invalid_request.expected_record_count = 0;
        let invalid_lineage = AdmittedSemanticLineage {
            artifact_sha256: invalid_request.expected_artifact_sha256.clone(),
            manifest_sha256: invalid_request.expected_manifest_sha256.clone(),
            range_set_sha256: invalid_request.expected_range_set_sha256.clone(),
            source_file_id: invalid_request.expected_artifact_sha256.clone(),
            source_binding_id: "synthetic/replay".to_owned(),
            collection_kind: UhcCollectionKind::ProviderMembership,
        };
        assert!(UhcSemanticReplaySource::open(&invalid_request, invalid_lineage).is_err());

        let invalid_source_lineage = AdmittedSemanticLineage {
            artifact_sha256: request.expected_artifact_sha256.clone(),
            manifest_sha256: request.expected_manifest_sha256.clone(),
            range_set_sha256: request.expected_range_set_sha256.clone(),
            source_file_id: "invalid".to_owned(),
            source_binding_id: "synthetic/replay".to_owned(),
            collection_kind: UhcCollectionKind::ProviderMembership,
        };
        assert!(UhcSemanticReplaySource::open(&request, invalid_source_lineage).is_err());

        let admitted = UhcSemanticReplaySource::open(
            &request,
            AdmittedSemanticLineage {
                artifact_sha256: request.expected_artifact_sha256.clone(),
                manifest_sha256: request.expected_manifest_sha256.clone(),
                range_set_sha256: request.expected_range_set_sha256.clone(),
                source_file_id: request.expected_artifact_sha256.clone(),
                source_binding_id: "synthetic/replay".to_owned(),
                collection_kind: UhcCollectionKind::ProviderMembership,
            },
        )
        .unwrap();
        let mut foreign = admitted.ranges()[0].clone();
        foreign.range_ordinal = 99;
        assert!(admitted
            .visit_verified_records(&foreign, &mut |_, _| Ok(()))
            .unwrap_err()
            .to_string()
            .contains("does not belong"));
    }

    #[test]
    fn hashing_writer_detects_byte_count_overflow() {
        let mut writer = HashingWriter {
            inner: io::sink(),
            digest: Sha256::new(),
            bytes: u64::MAX,
        };
        assert!(writer
            .write(b"x")
            .unwrap_err()
            .to_string()
            .contains("byte count overflowed"));
        writer.flush().unwrap();

        let mut counting = CountingWriter::new(io::sink());
        counting.flush().unwrap();
        counting.bytes = u64::MAX;
        assert!(counting
            .write(b"x")
            .unwrap_err()
            .to_string()
            .contains("COPY byte count overflowed"));

        let directory = tempfile::tempdir().unwrap();
        let output = File::create(directory.path().join("counted.copy")).unwrap();
        let mut persisted = CountingWriter::new(BufWriter::new(output));
        persisted.write_all(b"ok").unwrap();
        persisted.flush().unwrap();
        assert_eq!(persisted.bytes, 2);

        let mut failing = FailAfter {
            remaining: 1,
            bytes: Vec::new(),
        };
        failing.flush().unwrap();
    }

    #[test]
    fn large_file_working_set_is_independent_of_total_records() {
        let budget = test_budget();
        let small = SyntheticSource::new(4_000, UhcCollectionKind::ProviderMembership);
        let large = SyntheticSource::new(20_000, UhcCollectionKind::ProviderMembership);
        let small_report = encode_admitted_ranges_to_copy(&small, io::sink(), &budget)
            .expect("encode small synthetic file");
        let large_report = encode_admitted_ranges_to_copy(&large, io::sink(), &budget)
            .expect("encode large synthetic file");

        assert_eq!(small_report.fact_count, 4_000);
        assert_eq!(large_report.fact_count, 20_000);
        assert!(large_report.output_bytes > small_report.output_bytes);
        assert!(small_report.evidence_run_count > 4);
        assert!(large_report.evidence_run_count > small_report.evidence_run_count);
        assert!(small_report.peak_worker_reserved_bytes <= budget.per_worker_bytes);
        assert!(large_report.peak_worker_reserved_bytes <= budget.per_worker_bytes);
        assert!(
            large_report.peak_worker_reserved_bytes
                <= small_report.peak_worker_reserved_bytes + size_of::<EvidenceRow>()
        );
        assert!(large_report.peak_pending_range_results <= 4);
    }
}
