// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Whole-batch extraction of source-declared Organization and InsurancePlan facts.
//! The caller supplies one Python sorted, compact, Unicode JSON envelope. Resource
//! hashes bind the exact received bytes; this extractor does not certify that an
//! arbitrary JSON representation is canonical or establish database evidence.

use crate::network_membership_codec::{write_field, COPY_HEADER};
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, Write};

pub const MAX_INPUT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_RESOURCES: usize = 1_000;
pub const MAX_REFERENCE_ITEMS: usize = MAX_OUTPUT_BYTES / 3;
pub const MAX_PAYLOAD_COLLECTION_ITEMS: usize = MAX_OUTPUT_BYTES / 2;
pub const MAX_REFERENCE_BYTES: usize = MAX_OUTPUT_BYTES - 2;
pub const MAX_COPY_BYTES: usize = 64 * 1024 * 1024;
pub const COPY_COLUMNS: [&str; 5] = [
    "source_row_ordinal",
    "resource_id",
    "payload_sha256",
    "resource_json",
    "observation_json",
];

#[derive(Debug, Eq, PartialEq)]
pub struct EncodedFhirNetworkIdentityBatch {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
    pub input_count: usize,
    pub duplicate_count: usize,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FhirNetworkOrganization {
    pub source_row_ordinal: usize,
    pub resource_id: String,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FhirNetworkPlan {
    pub source_row_ordinal: usize,
    pub plan_resource_id: String,
    pub network_refs: Vec<String>,
    pub candidate_local_target_ids: Vec<String>,
    pub exact_local_target_ids: Vec<String>,
    pub owned_by_ref: Option<String>,
    pub administered_by_ref: Option<String>,
    pub payload_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct FhirNetworkIdentityBatch {
    pub source_id: String,
    pub release_id: String,
    pub input_count: usize,
    pub organizations: Vec<FhirNetworkOrganization>,
    pub plans: Vec<FhirNetworkPlan>,
    pub duplicate_count: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct FhirNetworkIdentityError {
    pub code: &'static str,
    pub source_row_ordinal: Option<usize>,
}

impl fmt::Display for FhirNetworkIdentityError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "FHIR network batch rejected: {}", self.code)
    }
}

impl std::error::Error for FhirNetworkIdentityError {}

fn failure(code: &'static str, row: Option<usize>) -> FhirNetworkIdentityError {
    FhirNetworkIdentityError {
        code,
        source_row_ordinal: row,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchInput {
    source_id: String,
    release_id: String,
    resource_type: String,
    resources: Box<RawValue>,
}

struct ResourceVisitor<'a>(&'a mut bool);

impl<'de> Visitor<'de> for ResourceVisitor<'_> {
    type Value = Vec<Box<RawValue>>;

    fn expecting(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("a bounded resource array")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Self::Value, A::Error> {
        let mut resources = Vec::new();
        while resources.len() < MAX_RESOURCES {
            let Some(resource) = sequence.next_element::<Box<RawValue>>()? else {
                return Ok(resources);
            };
            resources.push(resource);
        }
        if sequence.next_element::<de::IgnoredAny>()?.is_some() {
            *self.0 = true;
            return Err(de::Error::custom("resource count limit"));
        }
        Ok(resources)
    }
}

fn resources(raw: &RawValue) -> Result<Vec<Box<RawValue>>, FhirNetworkIdentityError> {
    let mut overflow = false;
    let mut decoder = serde_json::Deserializer::from_str(raw.get());
    let result = decoder.deserialize_seq(ResourceVisitor(&mut overflow));
    if overflow {
        return Err(failure("resource_count", Some(MAX_RESOURCES + 1)));
    }
    let result = result.map_err(|_| failure("invalid_resources", None))?;
    decoder
        .end()
        .map_err(|_| failure("invalid_resources", None))?;
    if result.is_empty() {
        return Err(failure("resource_count", None));
    }
    Ok(result)
}

fn python_whitespace(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}')
}

fn identity(value: &str, limit: usize, row: Option<usize>) -> Result<(), FhirNetworkIdentityError> {
    if value.is_empty()
        || value.chars().count() > limit
        || value.trim_matches(python_whitespace) != value
    {
        return Err(failure("invalid_identity", row));
    }
    Ok(())
}

fn fhir_id(value: &str) -> bool {
    (1..=64).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'))
}

fn network_role(resource: &Map<String, Value>) -> bool {
    resource
        .get("type")
        .and_then(Value::as_array)
        .is_some_and(|entries| {
            entries.iter().any(|entry| {
                let Some(entry) = entry.as_object() else {
                    return false;
                };
                entry.get("text").and_then(Value::as_str) == Some("ntwk")
                    || entry
                        .get("coding")
                        .and_then(Value::as_array)
                        .is_some_and(|codings| {
                            codings.iter().any(|coding| {
                                coding.get("code").and_then(Value::as_str) == Some("ntwk")
                            })
                        })
            })
        })
}

#[derive(Default)]
struct CollectionBudget {
    payload_items: usize,
    reference_items: usize,
    retained_bytes: usize,
}

impl CollectionBudget {
    fn retained(&mut self, text: &str, row: usize) -> Result<(), FhirNetworkIdentityError> {
        self.retained_bytes = self
            .retained_bytes
            .checked_add(text.len())
            .ok_or_else(|| failure("output_limit", Some(row)))?;
        if self.retained_bytes > MAX_OUTPUT_BYTES {
            return Err(failure("output_limit", Some(row)));
        }
        Ok(())
    }

    fn payload(&mut self, value: &Value, row: usize) -> Result<(), FhirNetworkIdentityError> {
        let count = match value {
            Value::Array(items) => items.len(),
            Value::Object(items) => items.len(),
            _ => 0,
        };
        self.payload_items += count;
        if self.payload_items > MAX_PAYLOAD_COLLECTION_ITEMS {
            return Err(failure("payload_collection_limit", Some(row)));
        }
        match value {
            Value::Array(items) => {
                for entry in items {
                    self.payload(entry, row)?;
                }
            }
            Value::Object(items) => {
                for entry in items.values() {
                    self.payload(entry, row)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn reference_items(value: &Value) -> &[Value] {
    match value {
        Value::Array(items) => items,
        _ => std::slice::from_ref(value),
    }
}

fn network_fields(
    resource: &Map<String, Value>,
    row: usize,
) -> Result<Vec<&Value>, FhirNetworkIdentityError> {
    let mut fields = Vec::new();
    if let Some(network) = resource.get("network") {
        fields.push(network);
    }
    for field in ["plan", "coverage"] {
        match resource.get(field) {
            Some(Value::Array(entries)) => {
                for entry in entries {
                    if let Some(network) = entry.as_object().and_then(|entry| entry.get("network"))
                    {
                        fields.push(network);
                    }
                }
            }
            Some(Value::Bool(true)) => {
                return Err(failure("invalid_network_collection", Some(row)))
            }
            Some(Value::Number(number)) if number.as_f64() != Some(0.0) => {
                return Err(failure("invalid_network_collection", Some(row)))
            }
            _ => {}
        }
    }
    Ok(fields)
}

fn append_unique(
    text: &str,
    seen: &mut BTreeSet<String>,
    output: &mut Vec<String>,
    budget: &mut CollectionBudget,
    row: usize,
) -> Result<(), FhirNetworkIdentityError> {
    if !seen.contains(text) {
        budget.retained(text, row)?;
        seen.insert(text.to_owned());
        output.push(text.to_owned());
    }
    Ok(())
}

fn local_target(reference: &str) -> Option<&str> {
    reference
        .strip_prefix("Organization/")
        .filter(|id| fhir_id(id))
}

type PlanNetworkReferences = (Vec<String>, Vec<String>, Vec<String>);

fn plan_references(
    resource: &Map<String, Value>,
    row: usize,
    budget: &mut CollectionBudget,
) -> Result<PlanNetworkReferences, FhirNetworkIdentityError> {
    let (mut refs, mut candidates, mut exact) = (Vec::new(), Vec::new(), Vec::new());
    let (mut ref_seen, mut candidate_seen, mut exact_seen) =
        (BTreeSet::new(), BTreeSet::new(), BTreeSet::new());
    for field in network_fields(resource, row)? {
        for item in reference_items(field) {
            budget.reference_items += 1;
            if budget.reference_items > MAX_REFERENCE_ITEMS {
                return Err(failure("reference_count", Some(row)));
            }
            let Some(reference) = item
                .as_object()
                .and_then(|item| item.get("reference"))
                .and_then(Value::as_str)
            else {
                continue;
            };
            if reference.len() > MAX_REFERENCE_BYTES {
                return Err(failure("reference_limit", Some(row)));
            }
            append_unique(reference, &mut ref_seen, &mut refs, budget, row)?;
            if let Some(id) = local_target(reference.trim_matches(python_whitespace)) {
                append_unique(id, &mut candidate_seen, &mut candidates, budget, row)?;
            }
            if let Some(id) = local_target(reference) {
                append_unique(id, &mut exact_seen, &mut exact, budget, row)?;
            }
        }
    }
    Ok((refs, candidates, exact))
}

fn payer_reference(
    resource: &Map<String, Value>,
    field: &str,
    row: usize,
    budget: &mut CollectionBudget,
) -> Result<Option<String>, FhirNetworkIdentityError> {
    let reference = resource
        .get(field)
        .and_then(Value::as_object)
        .and_then(|object| object.get("reference"))
        .and_then(Value::as_str);
    if let Some(reference) = reference {
        budget.retained(reference, row)?;
    }
    Ok(reference.map(str::to_owned))
}

fn extract_plan(
    resource: &Map<String, Value>,
    id: String,
    digest: String,
    row: usize,
    budget: &mut CollectionBudget,
) -> Result<FhirNetworkPlan, FhirNetworkIdentityError> {
    let (network_refs, candidate_local_target_ids, exact_local_target_ids) =
        plan_references(resource, row, budget)?;
    Ok(FhirNetworkPlan {
        source_row_ordinal: row,
        plan_resource_id: id,
        network_refs,
        candidate_local_target_ids,
        exact_local_target_ids,
        owned_by_ref: payer_reference(resource, "ownedBy", row, budget)?,
        administered_by_ref: payer_reference(resource, "administeredBy", row, budget)?,
        payload_sha256: digest,
    })
}

fn duplicate(
    seen: &mut BTreeMap<String, String>,
    id: &str,
    digest: &str,
    row: usize,
) -> Result<bool, FhirNetworkIdentityError> {
    if let Some(previous) = seen.get(id) {
        if previous != digest {
            return Err(failure("payload_conflict", Some(row)));
        }
        return Ok(true);
    }
    seen.insert(id.to_owned(), digest.to_owned());
    Ok(false)
}

struct OutputCounter(usize);

impl Write for OutputCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_OUTPUT_BYTES)
            .ok_or_else(|| io::Error::other("FHIR network output limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn extract_resources(
    input: &BatchInput,
    resources: &[Box<RawValue>],
) -> Result<FhirNetworkIdentityBatch, FhirNetworkIdentityError> {
    let mut batch = FhirNetworkIdentityBatch {
        source_id: input.source_id.clone(),
        release_id: input.release_id.clone(),
        input_count: resources.len(),
        organizations: Vec::new(),
        plans: Vec::new(),
        duplicate_count: 0,
    };
    let mut seen = BTreeMap::new();
    let mut budget = CollectionBudget::default();
    for (index, raw) in resources.iter().enumerate() {
        let row = index + 1;
        let payload: Value =
            serde_json::from_str(raw.get()).map_err(|_| failure("invalid_resource", Some(row)))?;
        let resource = payload
            .as_object()
            .ok_or_else(|| failure("invalid_resource", Some(row)))?;
        if resource.get("resourceType").and_then(Value::as_str)
            != Some(input.resource_type.as_str())
        {
            return Err(failure("resource_type_mismatch", Some(row)));
        }
        let id = resource
            .get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| failure("invalid_identity", Some(row)))?;
        identity(id, 256, Some(row))?;
        budget.payload(&payload, row)?;
        let digest = Sha256::digest(raw.get().as_bytes())
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        batch.duplicate_count += usize::from(duplicate(&mut seen, id, &digest, row)?);
        if input.resource_type == "Organization" {
            if network_role(resource) {
                if !fhir_id(id) {
                    return Err(failure("invalid_organization_id", Some(row)));
                }
                batch.organizations.push(FhirNetworkOrganization {
                    source_row_ordinal: row,
                    resource_id: id.to_owned(),
                    payload_sha256: digest,
                });
            }
        } else {
            batch.plans.push(extract_plan(
                resource,
                id.to_owned(),
                digest,
                row,
                &mut budget,
            )?);
        }
    }
    serde_json::to_writer(OutputCounter(0), &batch).map_err(|_| failure("output_limit", None))?;
    Ok(batch)
}

fn parse_input(input: &[u8]) -> Result<(BatchInput, Vec<Box<RawValue>>), FhirNetworkIdentityError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None));
    }
    let input: BatchInput =
        serde_json::from_slice(input).map_err(|_| failure("invalid_batch", None))?;
    identity(&input.source_id, 64, None)?;
    identity(&input.release_id, 256, None)?;
    if !matches!(
        input.resource_type.as_str(),
        "Organization" | "InsurancePlan"
    ) {
        return Err(failure("invalid_resource_type", None));
    }
    let resources = resources(&input.resources)?;
    Ok((input, resources))
}

/// Extract one complete batch; caller canonical JSON bytes are hashed without reencoding.
/// Successful data is source evidence only. Conflicting scoped payloads reject every row.
pub fn extract_fhir_network_identity_batch(
    input: &[u8],
) -> Result<FhirNetworkIdentityBatch, FhirNetworkIdentityError> {
    let (input, resources) = parse_input(input)?;
    extract_resources(&input, &resources)
}

fn validate_jsonb_strings(value: &Value, row: usize) -> Result<(), FhirNetworkIdentityError> {
    match value {
        Value::String(text) if text.contains('\0') => {
            return Err(failure("invalid_jsonb", Some(row)));
        }
        Value::Array(items) => {
            for item in items {
                validate_jsonb_strings(item, row)?;
            }
        }
        Value::Object(items) => {
            for (key, item) in items {
                if key.contains('\0') {
                    return Err(failure("invalid_jsonb", Some(row)));
                }
                validate_jsonb_strings(item, row)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn copy_observation(
    batch: &FhirNetworkIdentityBatch,
    resource: &Map<String, Value>,
    raw: &RawValue,
    row: usize,
) -> Result<Value, FhirNetworkIdentityError> {
    if !batch.plans.is_empty() {
        return serde_json::to_value(&batch.plans[row - 1])
            .map_err(|_| failure("invalid_jsonb", Some(row)));
    }
    let digest = Sha256::digest(raw.get().as_bytes())
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(serde_json::json!({
        "source_row_ordinal": row,
        "resource_id": resource["id"],
        "payload_sha256": digest,
        "network_role": network_role(resource),
    }))
}

fn checked_copy_size(
    current: usize,
    lengths: &[usize; 5],
    row: usize,
) -> Result<usize, FhirNetworkIdentityError> {
    let row_bytes = lengths.iter().try_fold(2usize, |total, length| {
        i32::try_from(*length).ok()?;
        total.checked_add(length.checked_add(4)?)
    });
    row_bytes
        .and_then(|size| current.checked_add(size))
        .filter(|size| *size <= MAX_COPY_BYTES)
        .ok_or_else(|| failure("copy_limit", Some(row)))
}

fn prepare_copy_rows(
    batch: &FhirNetworkIdentityBatch,
    resources: &[Box<RawValue>],
) -> Result<(usize, Vec<Value>), FhirNetworkIdentityError> {
    let mut size = COPY_HEADER.len() + 2;
    let mut observations = Vec::with_capacity(resources.len());
    for (index, raw) in resources.iter().enumerate() {
        let row = index + 1;
        let payload: Value =
            serde_json::from_str(raw.get()).map_err(|_| failure("invalid_jsonb", Some(row)))?;
        validate_jsonb_strings(&payload, row)?;
        let resource = payload
            .as_object()
            .ok_or_else(|| failure("invalid_resource", Some(row)))?;
        let observation = copy_observation(batch, resource, raw, row)?;
        let mut count = OutputCounter(0);
        serde_json::to_writer(&mut count, &observation)
            .map_err(|_| failure("output_limit", Some(row)))?;
        let id = resource["id"]
            .as_str()
            .ok_or_else(|| failure("invalid_identity", Some(row)))?;
        size = checked_copy_size(
            size,
            &[4, id.len(), 64, raw.get().len() + 1, count.0 + 1],
            row,
        )?;
        observations.push(observation);
    }
    Ok((size, observations))
}

fn write_jsonb_field(bytes: &[u8], output: &mut Vec<u8>) {
    // All field lengths and the complete allocation were preflighted below int32 bounds.
    output.extend_from_slice(&((bytes.len() + 1) as i32).to_be_bytes());
    output.push(1);
    output.extend_from_slice(bytes);
}

/// Encode every validated source row, retaining exact received resource JSON bytes.
/// JSONB-unrepresentable NUL text and late failures reject the complete private batch.
pub fn encode_fhir_network_identity_batch(
    input: &[u8],
) -> Result<EncodedFhirNetworkIdentityBatch, FhirNetworkIdentityError> {
    let (input, resources) = parse_input(input)?;
    let batch = extract_resources(&input, &resources)?;
    if input.source_id.contains('\0') || input.release_id.contains('\0') {
        return Err(failure("invalid_jsonb", None));
    }
    let (size, observations) = prepare_copy_rows(&batch, &resources)?;
    let mut copy_bytes = Vec::new();
    copy_bytes
        .try_reserve_exact(size)
        .map_err(|_| failure("allocation_failed", None))?;
    copy_bytes.extend_from_slice(COPY_HEADER);
    for (index, (raw, observation)) in resources.iter().zip(observations).enumerate() {
        let row = index + 1;
        let ordinal = i32::try_from(row).map_err(|_| failure("invalid_source_row", Some(row)))?;
        let resource_id = observation
            .get("resource_id")
            .or_else(|| observation.get("plan_resource_id"))
            .and_then(Value::as_str)
            .ok_or_else(|| failure("invalid_identity", Some(row)))?;
        let digest = observation["payload_sha256"]
            .as_str()
            .ok_or_else(|| failure("invalid_resource", Some(row)))?;
        let encoded =
            serde_json::to_vec(&observation).map_err(|_| failure("invalid_jsonb", Some(row)))?;
        copy_bytes.extend_from_slice(&5i16.to_be_bytes());
        write_field(&ordinal.to_be_bytes(), &mut copy_bytes);
        write_field(resource_id.as_bytes(), &mut copy_bytes);
        write_field(digest.as_bytes(), &mut copy_bytes);
        write_jsonb_field(raw.get().as_bytes(), &mut copy_bytes);
        write_jsonb_field(&encoded, &mut copy_bytes);
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedFhirNetworkIdentityBatch {
        copy_bytes,
        row_count: batch.input_count,
        input_count: batch.input_count,
        duplicate_count: batch.duplicate_count,
    })
}

#[cfg(test)]
mod copy_bound_tests {
    use super::{checked_copy_size, MAX_COPY_BYTES};

    #[test]
    fn preflight_accepts_exact_limit_and_rejects_overflow_before_casts() {
        let lengths = [4, 6, 64, 100, 200];
        let row_bytes = 2 + lengths.iter().map(|size| 4 + size).sum::<usize>();
        assert_eq!(
            checked_copy_size(MAX_COPY_BYTES - row_bytes, &lengths, 1),
            Ok(MAX_COPY_BYTES)
        );
        assert_eq!(
            checked_copy_size(MAX_COPY_BYTES - row_bytes + 1, &lengths, 2)
                .unwrap_err()
                .code,
            "copy_limit"
        );
        assert_eq!(
            checked_copy_size(0, &[usize::MAX, 0, 0, 0, 0], 3)
                .unwrap_err()
                .source_row_ordinal,
            Some(3)
        );
        assert_eq!(
            checked_copy_size(0, &[i32::MAX as usize + 1, 0, 0, 0, 0], 4)
                .unwrap_err()
                .code,
            "copy_limit"
        );
    }
}
