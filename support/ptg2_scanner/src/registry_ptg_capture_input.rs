//! Bounded validation and native COPY encoding of exact provider-office assertions.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use crate::npi_identifier::{npi_validity, NpiValidity};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};
use std::{collections::HashSet, fmt};

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_CONTEXT_BYTES: usize = 16 * 1024;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_OFFICE_EVIDENCE_BYTES: usize = 16 * 1024;
pub const COPY_COLUMNS: [&str; 20] = [
    "ordinal",
    "source_record_key",
    "binding_source_key",
    "company_key",
    "cohort_id",
    "snapshot_id",
    "provider_system",
    "provider_id",
    "location_id",
    "location_key",
    "location_hash",
    "address_row_sha256",
    "dense_source_key",
    "source_record_ordinal",
    "provider_group_ref",
    "provider_witness_sha256",
    "office_evidence_kind",
    "office_evidence_json",
    "office_evidence_sha256",
    "evidence_id",
];

#[derive(Debug, Eq, PartialEq)]
pub struct EncodedRegistryPTGCaptureBatch {
    pub copy_bytes: Vec<u8>,
    pub canonical_ndjson: Vec<u8>,
    pub row_count: usize,
    pub last_ordinal: u64,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryPTGCaptureInputError {
    pub code: &'static str,
    pub row_ordinal: Option<u64>,
}

impl fmt::Display for RegistryPTGCaptureInputError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.code)
    }
}

impl std::error::Error for RegistryPTGCaptureInputError {}

fn error(code: &'static str, row_ordinal: Option<u64>) -> RegistryPTGCaptureInputError {
    RegistryPTGCaptureInputError { code, row_ordinal }
}

#[derive(Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct BindingCoordinates {
    source_system: String,
    source_id: String,
    dataset_schema: String,
    dataset_id: String,
    producer_id: String,
    edition_id: String,
}

#[derive(Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
struct CohortScope {
    company_key: String,
    cohort_id: String,
    snapshot_id: String,
}

#[derive(Deserialize, Serialize, Eq, PartialEq)]
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

#[derive(Deserialize, Serialize, Eq, PartialEq)]
#[serde(untagged)]
enum SourceScope {
    Cohort(CohortScope),
    Published(PublishedPlanScope),
}

impl SourceScope {
    fn snapshot_id(&self) -> &str {
        match self {
            Self::Cohort(scope) => &scope.snapshot_id,
            Self::Published(scope) => &scope.snapshot_id,
        }
    }
    fn cohort_labels(&self) -> (Option<&str>, Option<&str>) {
        match self {
            Self::Cohort(scope) => (Some(&scope.company_key), Some(&scope.cohort_id)),
            Self::Published(_) => (None, None),
        }
    }
    fn valid(&self) -> bool {
        match self {
            Self::Cohort(scope) => {
                text(&scope.company_key, 512)
                    && text(&scope.cohort_id, 128)
                    && text(&scope.snapshot_id, 96)
            }
            Self::Published(scope) => {
                scope.review_type == "published_complete_snapshot_plan"
                    && uuid_bytes(&scope.scope_id).is_some_and(|bytes| bytes != [0; 16])
                    && !scope.scope_id.bytes().any(|byte| byte.is_ascii_uppercase())
                    && hex(&scope.approval_sha256, 64)
                    && text(&scope.snapshot_id, 96)
                    && text(&scope.plan_id, 64)
                    && text(&scope.plan_market_type, 32)
                    && scope.plan_market_type == scope.plan_market_type.to_lowercase()
                    && scope.selection_mode == "complete_snapshot_source_set"
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Context {
    binding_coordinates: BindingCoordinates,
    binding_source_key: String,
    source_scope: SourceScope,
    office_evidence_kind: String,
    snapshot_key: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct OfficeEvidence {
    contract: String,
    kind: String,
    assertion_id: String,
    source_record_key: String,
    binding_coordinates: BindingCoordinates,
    binding_source_key: String,
    source_scope: SourceScope,
    provider_system: String,
    provider_id: String,
    location_id: String,
    location_key: String,
    address_row_sha256: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct RawRow {
    ordinal: u64,
    source_record_key: String,
    binding_source_key: String,
    company_key: Option<String>,
    cohort_id: Option<String>,
    snapshot_id: String,
    provider_system: String,
    provider_id: String,
    location_id: String,
    location_key: String,
    location_hash: String,
    address_row_sha256: String,
    dense_source_key: i32,
    source_record_ordinal: u64,
    provider_group_ref: String,
    provider_witness_sha256: String,
    office_evidence_kind: String,
    office_evidence_json: OfficeEvidence,
    office_evidence_sha256: String,
    evidence_id: String,
}

fn text(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn hex(value: &str, length: usize) -> bool {
    value.len() == length
        && value
            .bytes()
            .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
}

fn valid_kind(value: &str) -> bool {
    matches!(value, "payer_exact_office" | "reviewed_exact_office")
}

fn valid_context(context: &Context) -> bool {
    let coordinates = &context.binding_coordinates;
    let schema = coordinates.dataset_schema.as_bytes();
    coordinates.source_system == "ptg"
        && !schema.is_empty()
        && schema.len() <= 63
        && (schema[0].is_ascii_alphabetic() || schema[0] == b'_')
        && schema
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'_')
        && [
            &coordinates.source_id,
            &coordinates.dataset_id,
            &coordinates.producer_id,
            &coordinates.edition_id,
        ]
        .iter()
        .all(|value| text(value, 128))
        && text(&context.binding_source_key, 512)
        && context.source_scope.valid()
        && valid_kind(&context.office_evidence_kind)
        && context.snapshot_key > 0
        && context.snapshot_key <= i64::MAX as u64
}

fn canonical<T: Serialize>(value: &T) -> Vec<u8> {
    let mut value = serde_json::to_value(value).expect("validated values serialize");
    value.sort_all_objects();
    serde_json::to_vec(&value).expect("validated values serialize")
}

fn sha256(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn validate_row(
    row: &RawRow,
    context: &Context,
    expected: u64,
) -> Result<([u8; 16], Vec<u8>), RegistryPTGCaptureInputError> {
    let fail = |code| error(code, Some(expected));
    if row.ordinal != expected
        || row.source_record_ordinal > i64::MAX as u64
        || row.dense_source_key < 0
    {
        return Err(fail("registry_ptg_accounting_invalid"));
    }
    if !text(&row.source_record_key, 128) || !text(&row.office_evidence_json.assertion_id, 128) {
        return Err(fail("registry_ptg_input_invalid"));
    }
    if row.binding_source_key != context.binding_source_key
        || row.company_key.as_deref() != context.source_scope.cohort_labels().0
        || row.cohort_id.as_deref() != context.source_scope.cohort_labels().1
        || row.snapshot_id != context.source_scope.snapshot_id()
        || row.office_evidence_kind != context.office_evidence_kind
    {
        return Err(fail("registry_ptg_scope_changed"));
    }
    if row.provider_system != "npi" || npi_validity(&row.provider_id) != NpiValidity::Valid {
        return Err(fail("registry_ptg_provider_invalid"));
    }
    let location = uuid_bytes(&row.location_id)
        .filter(|_| !row.location_id.bytes().any(|b| b.is_ascii_uppercase()))
        .ok_or_else(|| fail("registry_ptg_office_invalid"))?;
    if !hex(&row.location_key, 64)
        || row.location_hash != format!("entity_address_unified:{}", row.location_key)
        || !hex(&row.provider_group_ref, 32)
        || [
            &row.address_row_sha256,
            &row.provider_witness_sha256,
            &row.office_evidence_sha256,
            &row.evidence_id,
        ]
        .iter()
        .any(|value| !hex(value, 64))
    {
        return Err(fail("registry_ptg_input_invalid"));
    }
    let office = &row.office_evidence_json;
    if office.contract != "registry_ptg_office_assertion.v1"
        || office.kind != row.office_evidence_kind
        || office.source_record_key != row.source_record_key
        || office.binding_coordinates != context.binding_coordinates
        || office.binding_source_key != row.binding_source_key
        || office.source_scope != context.source_scope
        || office.provider_system != row.provider_system
        || office.provider_id != row.provider_id
        || office.location_id != row.location_id
        || office.location_key != row.location_key
        || office.address_row_sha256 != row.address_row_sha256
    {
        return Err(fail("registry_ptg_office_invalid"));
    }
    let office_bytes = canonical(office);
    if office_bytes.len() > MAX_OFFICE_EVIDENCE_BYTES {
        return Err(fail("registry_ptg_batch_bounds"));
    }
    let witness = serde_json::json!({
        "snapshot_key": context.snapshot_key, "dense_source_key": row.dense_source_key,
        "source_record_ordinal": row.source_record_ordinal, "provider_group_ref": row.provider_group_ref,
        "provider_system": row.provider_system, "provider_id": row.provider_id,
    });
    let evidence = serde_json::json!([
        context.binding_coordinates,
        context.source_scope,
        row.binding_source_key,
        row.provider_system,
        row.provider_id,
        row.location_id,
        row.office_evidence_sha256,
        row.provider_witness_sha256,
        row.address_row_sha256,
    ]);
    if sha256(&canonical(&witness)) != row.provider_witness_sha256
        || sha256(&office_bytes) != row.office_evidence_sha256
        || sha256(&canonical(&evidence)) != row.evidence_id
    {
        return Err(fail("registry_ptg_digest_invalid"));
    }
    Ok((location, office_bytes))
}

/// Context is a syntactic expectation; producer and retained-office authority are checked elsewhere.
pub fn encode_registry_ptg_capture_batch(
    input: &[u8],
    context_json: &[u8],
    after_ordinal: u64,
) -> Result<EncodedRegistryPTGCaptureBatch, RegistryPTGCaptureInputError> {
    if input.len() > MAX_INPUT_BYTES || context_json.len() > MAX_CONTEXT_BYTES {
        return Err(error("registry_ptg_batch_bounds", None));
    }
    if after_ordinal >= i64::MAX as u64 {
        return Err(error("registry_ptg_accounting_invalid", None));
    }
    let context: Context = serde_json::from_slice(context_json)
        .map_err(|_| error("registry_ptg_context_invalid", None))?;
    if !valid_context(&context) {
        return Err(error("registry_ptg_context_invalid", None));
    }
    let rows: Vec<&RawValue> =
        serde_json::from_slice(input).map_err(|_| error("registry_ptg_input_invalid", None))?;
    if rows.is_empty() || rows.len() > MAX_ROWS {
        return Err(error("registry_ptg_batch_bounds", None));
    }
    if rows.len() as u64 > i64::MAX as u64 - after_ordinal {
        return Err(error("registry_ptg_accounting_invalid", None));
    }
    let mut output = COPY_HEADER.to_vec();
    let mut ndjson = Vec::with_capacity(input.len());
    let mut source_keys = HashSet::with_capacity(rows.len());
    let mut offices = HashSet::with_capacity(rows.len());
    for (index, raw) in rows.iter().enumerate() {
        let ordinal = after_ordinal + index as u64 + 1;
        let row: RawRow = serde_json::from_str(raw.get())
            .map_err(|_| error("registry_ptg_input_invalid", Some(ordinal)))?;
        let (location, office) = validate_row(&row, &context, ordinal)?;
        if !source_keys.insert(row.source_record_key.clone())
            || !offices.insert((row.provider_id.clone(), row.location_id.clone()))
        {
            return Err(error("registry_ptg_rows_invalid", Some(ordinal)));
        }
        let ordinal_bytes = row.ordinal.to_be_bytes();
        let dense_bytes = row.dense_source_key.to_be_bytes();
        let source_ordinal_bytes = row.source_record_ordinal.to_be_bytes();
        let mut office_jsonb = Vec::with_capacity(office.len() + 1);
        office_jsonb.push(1);
        office_jsonb.extend_from_slice(&office);
        let fields: [Option<&[u8]>; 20] = [
            Some(&ordinal_bytes),
            Some(row.source_record_key.as_bytes()),
            Some(row.binding_source_key.as_bytes()),
            row.company_key.as_deref().map(str::as_bytes),
            row.cohort_id.as_deref().map(str::as_bytes),
            Some(row.snapshot_id.as_bytes()),
            Some(row.provider_system.as_bytes()),
            Some(row.provider_id.as_bytes()),
            Some(&location),
            Some(row.location_key.as_bytes()),
            Some(row.location_hash.as_bytes()),
            Some(row.address_row_sha256.as_bytes()),
            Some(&dense_bytes),
            Some(&source_ordinal_bytes),
            Some(row.provider_group_ref.as_bytes()),
            Some(row.provider_witness_sha256.as_bytes()),
            Some(row.office_evidence_kind.as_bytes()),
            Some(&office_jsonb),
            Some(row.office_evidence_sha256.as_bytes()),
            Some(row.evidence_id.as_bytes()),
        ];
        if output.len()
            + 2
            + fields.len() * 4
            + fields
                .iter()
                .map(|field| field.map_or(0, |bytes| bytes.len()))
                .sum::<usize>()
            + 2
            > MAX_COPY_BYTES
        {
            return Err(error("registry_ptg_batch_bounds", Some(ordinal)));
        }
        output.extend_from_slice(&20i16.to_be_bytes());
        for field in fields {
            match field {
                Some(bytes) => write_field(bytes, &mut output),
                None => output.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        ndjson.extend_from_slice(&canonical(&row));
        ndjson.push(b'\n');
    }
    output.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedRegistryPTGCaptureBatch {
        copy_bytes: output,
        canonical_ndjson: ndjson,
        row_count: rows.len(),
        last_ordinal: after_ordinal + rows.len() as u64,
    })
}

#[cfg(feature = "python")]
#[pyo3::pyfunction(name = "encode_registry_ptg_capture_batch")]
pub(crate) fn encode_registry_ptg_capture_batch_py(
    py: pyo3::Python<'_>,
    input: &[u8],
    context_json: &[u8],
    after_ordinal: &pyo3::Bound<'_, pyo3::PyAny>,
) -> pyo3::PyResult<(
    pyo3::Py<pyo3::types::PyBytes>,
    pyo3::Py<pyo3::types::PyBytes>,
    usize,
    u64,
)> {
    use pyo3::{
        exceptions::PyValueError,
        prelude::*,
        types::{PyBool, PyBytes, PyInt},
    };
    let boundary_error = || {
        PyValueError::new_err("{\"code\":\"registry_ptg_accounting_invalid\",\"row_ordinal\":null}")
    };
    if after_ordinal.is_instance_of::<PyBool>() || !after_ordinal.is_instance_of::<PyInt>() {
        return Err(boundary_error());
    }
    let ordinal = after_ordinal
        .extract::<u64>()
        .map_err(|_| boundary_error())?;
    let batch = py
        .detach(|| encode_registry_ptg_capture_batch(input, context_json, ordinal))
        .map_err(|failure| {
            PyValueError::new_err(serde_json::to_string(&failure).expect("error serializes"))
        })?;
    Ok((
        PyBytes::new(py, &batch.copy_bytes).unbind(),
        PyBytes::new(py, &batch.canonical_ndjson).unbind(),
        batch.row_count,
        batch.last_ordinal,
    ))
}

#[cfg(all(test, feature = "python"))]
mod python_boundary_tests {
    use super::*;
    use pyo3::{exceptions::PyValueError, prelude::*, types::PyBytes};

    #[test]
    fn python_ordinal_boundary_rejects_boolean_negative_fractional_and_overflow() {
        Python::initialize();
        Python::attach(|py| {
            let function =
                pyo3::wrap_pyfunction!(encode_registry_ptg_capture_batch_py, py).unwrap();
            assert_eq!(
                function
                    .getattr("__name__")
                    .unwrap()
                    .extract::<String>()
                    .unwrap(),
                "encode_registry_ptg_capture_batch"
            );
            for expression in [
                c"True", c"False", c"-1", c"1.0", c"'1'", c"2**64", c"2**63-1",
            ] {
                let ordinal = py.eval(expression, None, None).unwrap();
                let failure = function
                    .call1((PyBytes::new(py, b"[]"), PyBytes::new(py, b"{}"), ordinal))
                    .unwrap_err();
                assert!(failure.is_instance_of::<PyValueError>(py));
                let message = failure.value(py).to_string();
                assert_eq!(
                    message,
                    "{\"code\":\"registry_ptg_accounting_invalid\",\"row_ordinal\":null}"
                );
            }
        });
    }
}
