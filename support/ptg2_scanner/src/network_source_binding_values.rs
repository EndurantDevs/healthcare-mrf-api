//! Bounded syntax validation and binary COPY of reviewed source-scoped bindings.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_EXPECTED_REVISION: i64 = i64::MAX - 2;
pub const COPY_COLUMNS: [&str; 16] = [
    "binding_id",
    "source_system",
    "source_id",
    "dataset_schema",
    "dataset_id",
    "producer_id",
    "edition_id",
    "source_key",
    "source_scope_json",
    "binding_key",
    "network_id",
    "evidence_id",
    "evidence_sha256",
    "operation",
    "expected_revision",
    "expected_network_id",
];
const KEY_DOMAIN: &[u8] = b"registry_network_source_binding:v1:";
const JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI";

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct SourceBindingError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
}

impl fmt::Display for SourceBindingError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("Source binding batch is invalid or exceeds its bounds")
    }
}

impl std::error::Error for SourceBindingError {}

fn failure(
    code: &'static str,
    row: Option<usize>,
    field: Option<&'static str>,
) -> SourceBindingError {
    SourceBindingError {
        code,
        row_index: row,
        field,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BindingInput {
    binding_id: String,
    source_system: String,
    source_id: String,
    dataset_schema: String,
    dataset_id: String,
    producer_id: String,
    edition_id: String,
    source_key: String,
    source_scope_json: Box<RawValue>,
    network_id: i32,
    evidence_id: String,
    evidence_sha256: String,
    operation: String,
    expected_revision: i64,
    #[serde(deserialize_with = "required_network_id")]
    expected_network_id: Option<i32>,
}

fn required_network_id<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<i32>, D::Error> {
    Option::<i32>::deserialize(decoder)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct AcaScope {
    issuer_id: String,
    state: String,
    plan_year: i32,
    plan_id: String,
    checksum_network: i32,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PtgScope {
    cohort_id: String,
    snapshot_id: String,
    company_key: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct PublishedPlanScope {
    review_type: String,
    scope_id: String,
    approval_sha256: String,
    snapshot_id: String,
    plan_id: String,
    plan_market_type: String,
    selection_mode: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct FhirScope {
    organization_id: String,
    legacy_uuid: String,
    alias_scope: String,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ValidatedSourceBinding {
    pub binding_id: String,
    pub source_system: String,
    pub source_id: String,
    pub dataset_schema: String,
    pub dataset_id: String,
    pub producer_id: String,
    pub edition_id: String,
    pub source_key: String,
    pub source_scope_json: serde_json::Value,
    pub binding_key: String,
    pub network_id: i32,
    pub evidence_id: String,
    pub evidence_sha256: String,
    pub operation: String,
    pub expected_revision: i64,
    pub expected_network_id: Option<i32>,
}

#[derive(Debug, Eq, PartialEq)]
pub struct EncodedSourceBindingBatch {
    pub rows: Vec<ValidatedSourceBinding>,
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
}

fn valid_text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn canonical_uuid(value: &str) -> bool {
    value.bytes().all(|byte| !byte.is_ascii_uppercase()) && uuid_bytes(value).is_some()
}

fn valid_schema(value: &str) -> bool {
    let bytes = value.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 63
        && (bytes[0].is_ascii_alphabetic() || bytes[0] == b'_')
        && bytes
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'_')
}

fn valid_plan(scope: &AcaScope) -> bool {
    let prefix = format!("{}{}", scope.issuer_id, scope.state);
    let Some(suffix) = scope.plan_id.strip_prefix(&prefix) else {
        return false;
    };
    let bytes = suffix.as_bytes();
    match bytes.len() {
        7 => bytes.iter().all(u8::is_ascii_digit),
        10 => {
            bytes[..7].iter().all(u8::is_ascii_digit)
                && bytes[7] == b'-'
                && bytes[8..].iter().all(u8::is_ascii_digit)
        }
        _ => false,
    }
}

fn aca_scope(raw: &str) -> Option<serde_json::Value> {
    let scope: AcaScope = serde_json::from_str(raw).ok()?;
    if scope.issuer_id.len() != 5
        || scope.issuer_id == "00000"
        || !scope.issuer_id.bytes().all(|byte| byte.is_ascii_digit())
        || !JURISDICTIONS
            .split_whitespace()
            .any(|state| state == scope.state)
        || !(2010..=2100).contains(&scope.plan_year)
        || !valid_plan(&scope)
    {
        return None;
    }
    serde_json::to_value(scope).ok()
}

fn ptg_scope(raw: &str) -> Option<serde_json::Value> {
    if let Ok(scope) = serde_json::from_str::<PublishedPlanScope>(raw) {
        return published_plan_scope(scope);
    }
    let scope: PtgScope = serde_json::from_str(raw).ok()?;
    if !valid_text(&scope.cohort_id, 128)
        || !valid_text(&scope.snapshot_id, 128)
        || !valid_text(&scope.company_key, 512)
    {
        return None;
    }
    serde_json::to_value(scope).ok()
}

fn published_plan_scope(scope: PublishedPlanScope) -> Option<serde_json::Value> {
    if scope.review_type != "published_complete_snapshot_plan"
        || scope.selection_mode != "complete_snapshot_source_set"
        || !canonical_uuid(&scope.scope_id)
        || scope.approval_sha256.len() != 64
        || !scope
            .approval_sha256
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
        || !valid_text(&scope.snapshot_id, usize::MAX)
        || scope.snapshot_id.chars().count() > 96
        || !valid_text(&scope.plan_id, usize::MAX)
        || scope.plan_id.chars().count() > 64
        || !valid_text(&scope.plan_market_type, usize::MAX)
        || scope.plan_market_type.chars().count() > 32
        || scope.plan_market_type != scope.plan_market_type.to_lowercase()
    {
        return None;
    }
    serde_json::to_value(scope).ok()
}

fn fhir_scope(raw: &str) -> Option<serde_json::Value> {
    let scope: FhirScope = serde_json::from_str(raw).ok()?;
    if scope.organization_id.is_empty()
        || scope.organization_id.len() > 64
        || !scope
            .organization_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
        || !canonical_uuid(&scope.legacy_uuid)
        || !valid_text(&scope.alias_scope, 512)
    {
        return None;
    }
    serde_json::to_value(scope).ok()
}

fn validate_fields(input: &BindingInput, index: usize) -> Result<(), SourceBindingError> {
    for (field, value, maximum) in [
        ("source_id", input.source_id.as_str(), 128),
        ("dataset_id", input.dataset_id.as_str(), 128),
        ("producer_id", input.producer_id.as_str(), 128),
        ("edition_id", input.edition_id.as_str(), 128),
        ("source_key", input.source_key.as_str(), 512),
        ("evidence_id", input.evidence_id.as_str(), 512),
    ] {
        if !valid_text(value, maximum) {
            return Err(failure("invalid_text", Some(index), Some(field)));
        }
    }
    if !canonical_uuid(&input.binding_id) {
        return Err(failure("invalid_uuid", Some(index), Some("binding_id")));
    }
    if !valid_schema(&input.dataset_schema) {
        return Err(failure(
            "invalid_schema",
            Some(index),
            Some("dataset_schema"),
        ));
    }
    if input.evidence_sha256.len() != 64
        || !input
            .evidence_sha256
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
    {
        return Err(failure(
            "invalid_digest",
            Some(index),
            Some("evidence_sha256"),
        ));
    }
    validate_operation(input, index)
}

fn validate_operation(input: &BindingInput, index: usize) -> Result<(), SourceBindingError> {
    let revision = input.expected_revision;
    let previous = input.expected_network_id;
    let valid = input.network_id > 0
        && (0..=MAX_EXPECTED_REVISION).contains(&revision)
        && match input.operation.as_str() {
            "bind" => revision == 0 && previous.is_none(),
            "rebind" => revision > 0 && previous.is_some_and(|network| network > 0),
            "close" => revision > 0 && previous == Some(input.network_id),
            _ => false,
        };
    if !valid {
        return Err(failure("invalid_operation", Some(index), None));
    }
    Ok(())
}

fn binding_key(input: &BindingInput, scope: &serde_json::Value) -> String {
    let canonical = serde_json::json!({
        "source_system": input.source_system, "source_id": input.source_id,
        "dataset_schema": input.dataset_schema, "dataset_id": input.dataset_id,
        "producer_id": input.producer_id, "edition_id": input.edition_id,
        "source_key": input.source_key, "source_scope_json": scope,
    });
    let mut digest = Sha256::new();
    digest.update(KEY_DOMAIN);
    digest.update(serde_json::to_vec(&canonical).expect("validated source coordinates serialize"));
    digest
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn validate_row(
    raw: &RawValue,
    index: usize,
) -> Result<ValidatedSourceBinding, SourceBindingError> {
    let input: BindingInput =
        serde_json::from_str(raw.get()).map_err(|_| failure("invalid_row", Some(index), None))?;
    validate_fields(&input, index)?;
    let scope = match input.source_system.as_str() {
        "aca" => aca_scope(input.source_scope_json.get()),
        "ptg" => ptg_scope(input.source_scope_json.get()),
        "fhir" => fhir_scope(input.source_scope_json.get()),
        _ => None,
    }
    .ok_or_else(|| failure("invalid_scope", Some(index), Some("source_scope_json")))?;
    let binding_key = binding_key(&input, &scope);
    Ok(ValidatedSourceBinding {
        binding_id: input.binding_id,
        source_system: input.source_system,
        source_id: input.source_id,
        dataset_schema: input.dataset_schema,
        dataset_id: input.dataset_id,
        producer_id: input.producer_id,
        edition_id: input.edition_id,
        source_key: input.source_key,
        source_scope_json: scope,
        binding_key,
        network_id: input.network_id,
        evidence_id: input.evidence_id,
        evidence_sha256: input.evidence_sha256,
        operation: input.operation,
        expected_revision: input.expected_revision,
        expected_network_id: input.expected_network_id,
    })
}

fn copy_row(row: &ValidatedSourceBinding, output: &mut Vec<u8>) {
    output.extend_from_slice(&16i16.to_be_bytes());
    write_field(
        &uuid_bytes(&row.binding_id).expect("validated UUID"),
        output,
    );
    for value in [
        &row.source_system,
        &row.source_id,
        &row.dataset_schema,
        &row.dataset_id,
        &row.producer_id,
        &row.edition_id,
        &row.source_key,
    ] {
        write_field(value.as_bytes(), output);
    }
    let mut scope = vec![1];
    scope.extend_from_slice(
        &serde_json::to_vec(&row.source_scope_json).expect("validated scope serializes"),
    );
    write_field(&scope, output);
    write_field(row.binding_key.as_bytes(), output);
    write_field(&row.network_id.to_be_bytes(), output);
    write_field(row.evidence_id.as_bytes(), output);
    write_field(row.evidence_sha256.as_bytes(), output);
    write_field(row.operation.as_bytes(), output);
    write_field(&row.expected_revision.to_be_bytes(), output);
    if let Some(network) = row.expected_network_id {
        write_field(&network.to_be_bytes(), output);
    } else {
        output.extend_from_slice(&(-1i32).to_be_bytes());
    }
}

/// Validate the entire batch before exposing any row or binary COPY output.
pub fn encode_network_source_binding_batch(
    input: &[u8],
) -> Result<EncodedSourceBindingBatch, SourceBindingError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None, None));
    }
    let raw_rows: Vec<&RawValue> =
        serde_json::from_slice(input).map_err(|_| failure("invalid_json", None, None))?;
    if raw_rows.len() > MAX_ROWS {
        return Err(failure("row_limit", None, None));
    }
    let mut rows = Vec::with_capacity(raw_rows.len());
    let mut identities = BTreeSet::new();
    let mut keys = BTreeSet::new();
    for (index, raw) in raw_rows.into_iter().enumerate() {
        let row = validate_row(raw, index)?;
        if !identities.insert(row.binding_id.clone()) || !keys.insert(row.binding_key.clone()) {
            return Err(failure("duplicate_binding", Some(index), None));
        }
        rows.push(row);
    }
    let mut copy_bytes = COPY_HEADER.to_vec();
    for (index, row) in rows.iter().enumerate() {
        copy_row(row, &mut copy_bytes);
        if copy_bytes.len() + 2 > MAX_COPY_BYTES {
            return Err(failure("output_limit", Some(index), None));
        }
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedSourceBindingBatch {
        row_count: rows.len(),
        rows,
        copy_bytes,
    })
}
