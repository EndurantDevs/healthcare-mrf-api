// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded retained-payload validation, root commitments and derived COPY framing.
//! No bytes escape a failed batch; callers retain independent sealed verification.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
#[path = "retained_fhir_capture/hash.rs"]
pub(crate) mod hash;
use hash::{object_json_bytes, payload_hash, payload_hash_owned, write_array, write_string};
use rayon::prelude::*;
use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use serde_json::{Map, Value};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};

pub const MAX_INPUT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_ROWS: usize = 1_000;
pub const MAX_COPY_ROWS: usize = 5_000;
pub const MAX_COLLECTION_ITEMS: usize = 1_000_000;
pub const BINDING_COLUMNS: [&str; 6] = [
    "source_id",
    "resource_type",
    "resource_id",
    "organization_id",
    "site_id",
    "created_at",
];
pub const EVIDENCE_COLUMNS: [&str; 7] = [
    "source_id",
    "resource_type",
    "resource_id",
    "release_id",
    "payload_sha256",
    "payload_json",
    "observed_at",
];
pub const PLAN_COLUMNS: [&str; 11] = [
    "source_id",
    "release_id",
    "network_resource_type",
    "network_resource_id",
    "insurance_plan_resource_id",
    "network_refs",
    "owned_by_ref",
    "administered_by_ref",
    "plan_payload_sha256",
    "plan_payload_json",
    "observed_at",
];

#[derive(Debug, Eq, PartialEq)]
pub struct CopyBatch {
    pub bytes: Vec<u8>,
    pub row_count: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RootFamily {
    pub bytes: Vec<u8>,
    pub row_count: usize,
}

#[derive(Debug, Eq, PartialEq)]
pub struct RetainedFhirCaptureBatch {
    pub binding_batches: Vec<CopyBatch>,
    pub evidence_batches: Vec<CopyBatch>,
    pub plan_batches: Vec<CopyBatch>,
    /// Newline-separated compact ASCII JSON rows, without a leading/trailing newline.
    pub root_bytes: Vec<u8>,
    pub families: BTreeMap<String, RootFamily>,
    pub row_count: usize,
    /// These rows require reference validation and derived encoding before any output is used.
    pub fallback_rows: Vec<usize>,
    pub owned_output_bytes: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RetainedFhirCaptureError {
    pub code: &'static str,
    pub row_index: Option<usize>,
}

impl fmt::Display for RetainedFhirCaptureError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "Retained FHIR batch rejected: {}", self.code)
    }
}
impl std::error::Error for RetainedFhirCaptureError {}

fn failure(code: &'static str, row_index: Option<usize>) -> RetainedFhirCaptureError {
    RetainedFhirCaptureError { code, row_index }
}

type Result<T> = std::result::Result<T, RetainedFhirCaptureError>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Input<'a> {
    #[serde(borrow)]
    source_id: Cow<'a, str>,
    #[serde(borrow)]
    release_id: Cow<'a, str>,
    observed_at_pg_micros: i64,
    #[serde(borrow)]
    hash_contract: Cow<'a, str>,
    #[serde(default)]
    completion_required_version: Option<u8>,
    resource_types: Vec<String>,
    derive: bool,
    #[serde(borrow)]
    rows: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row<'a> {
    #[serde(borrow)]
    resource_type: Cow<'a, str>,
    #[serde(borrow)]
    resource_id: Cow<'a, str>,
    #[serde(borrow)]
    payload_hash: Cow<'a, str>,
    #[serde(borrow)]
    payload_json: Cow<'a, str>,
    #[serde(borrow)]
    acquired_resource_sha256: &'a RawValue,
    #[serde(default, borrow)]
    site_id: Option<Cow<'a, str>>,
}

/// Borrowed batch context for direct row-slice encoding without outer JSON serialization.
pub struct NativeCaptureContext<'a> {
    pub source_id: &'a str,
    pub release_id: &'a str,
    pub observed_at_pg_micros: i64,
    pub hash_contract: &'a str,
    pub completion_required_version: Option<u8>,
    pub resource_types: &'a [String],
    pub derive: bool,
}

/// Borrowed row fields for direct row-slice encoding without outer JSON serialization.
pub struct NativeCaptureRow<'a> {
    pub resource_type: &'a str,
    pub resource_id: &'a str,
    pub payload_hash: &'a str,
    pub payload_json: &'a str,
    pub acquired_resource_sha256: Option<&'a str>,
    pub site_id: Option<&'a str>,
}

/// The caller must validate every fallback row before consuming roots or publishing COPY.
/// `derive=false` validates independently without preparing derived tables.
pub fn encode_retained_fhir_capture_batch(input: &[u8]) -> Result<RetainedFhirCaptureBatch> {
    encode(input, None)
}

const PARALLEL_RETAINED_THRESHOLD: usize = 16;

fn format_root_line(resource_type: &str, resource_id: &str, payload_hash: &str) -> Vec<u8> {
    let mut out =
        Vec::with_capacity(resource_type.len() + resource_id.len() + payload_hash.len() + 10);
    out.push(b'[');
    write_string(resource_type, true, &mut out);
    out.push(b',');
    write_string(resource_id, true, &mut out);
    out.push(b',');
    write_string(payload_hash, true, &mut out);
    out.push(b']');
    out
}

fn is_valid_marker(completion_version: Option<u8>, marker: Option<&str>) -> bool {
    match (completion_version, marker) {
        (Some(3), Some(value)) => {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        }
        (None, None) => true,
        _ => false,
    }
}

fn is_valid_hex_64(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

struct PreparedNativeOutcome {
    fallback: bool,
    root_line: Vec<u8>,
    derived_payload: Option<Map<String, Value>>,
}

enum PreparedNativeRow {
    EarlyError(RetainedFhirCaptureError),
    AfterSyntax {
        syntax_items: usize,
        parse_or_items_error: Option<RetainedFhirCaptureError>,
        items: usize,
        outcome: Result<PreparedNativeOutcome>,
    },
}

fn prepare_native_row(
    context: &NativeCaptureContext<'_>,
    allowed_families: &BTreeSet<&str>,
    row: &NativeCaptureRow<'_>,
    row_index: usize,
) -> PreparedNativeRow {
    let index = Some(row_index);
    if !is_valid_marker(
        context.completion_required_version,
        row.acquired_resource_sha256,
    ) {
        return PreparedNativeRow::EarlyError(failure("invalid_rows", index));
    }
    if !allowed_families.contains(row.resource_type)
        || !source_resource_id(row.resource_id)
        || !is_valid_hex_64(row.payload_hash)
    {
        return PreparedNativeRow::EarlyError(failure("invalid_row", index));
    }
    let syntax_items = match scan_payload(row.payload_json, None) {
        Ok(count) => count,
        Err(code) => return PreparedNativeRow::EarlyError(failure(code, index)),
    };
    let parsed: Option<Value> = serde_json::from_str(row.payload_json).ok();
    let mut fallback = parsed.is_none();
    let mut items = 0usize;
    let mut derived_payload: Option<Map<String, Value>> = None;
    if let Some(payload) = parsed {
        let Some(object) = payload.as_object() else {
            return PreparedNativeRow::AfterSyntax {
                syntax_items,
                parse_or_items_error: Some(failure("invalid_payload", index)),
                items: 0,
                outcome: Err(failure("invalid_payload", index)),
            };
        };
        if object.get("resource_id").and_then(Value::as_str) != Some(row.resource_id)
            || object
                .get("source_id")
                .is_some_and(|source| source.as_str() != Some(context.source_id))
        {
            let err = failure("payload_identity", index);
            return PreparedNativeRow::AfterSyntax {
                syntax_items,
                parse_or_items_error: Some(err),
                items: 0,
                outcome: Err(failure("payload_identity", index)),
            };
        }
        match validate_values(&payload, &mut items, None) {
            Ok(needs_fallback) => fallback = needs_fallback,
            Err(code) => {
                return PreparedNativeRow::AfterSyntax {
                    syntax_items,
                    parse_or_items_error: Some(failure(code, index)),
                    items,
                    outcome: Err(failure(code, index)),
                };
            }
        }
        let Value::Object(object_owned) = payload else {
            unreachable!();
        };
        if !fallback {
            let needs_derived = context.derive
                && matches!(
                    row.resource_type,
                    "Organization" | "Location" | "InsurancePlan"
                );
            let hash_result = if needs_derived {
                let res = payload_hash(
                    &object_owned,
                    row.payload_json,
                    context.hash_contract,
                    row.resource_type,
                    context.completion_required_version == Some(3),
                );
                if res.is_ok() {
                    derived_payload = Some(object_owned);
                }
                res
            } else {
                payload_hash_owned(
                    object_owned,
                    row.payload_json,
                    context.hash_contract,
                    row.resource_type,
                    context.completion_required_version == Some(3),
                )
            };
            match hash_result {
                Ok(hash) if hash != row.payload_hash => {
                    return PreparedNativeRow::AfterSyntax {
                        syntax_items,
                        parse_or_items_error: None,
                        items,
                        outcome: Err(failure("payload_hash", index)),
                    };
                }
                Ok(_) => {}
                Err("python_semantics_required" | "unsupported_unicode_semantics") => {
                    fallback = true;
                    derived_payload = None;
                }
                Err(code) => {
                    return PreparedNativeRow::AfterSyntax {
                        syntax_items,
                        parse_or_items_error: None,
                        items,
                        outcome: Err(failure(code, index)),
                    };
                }
            }
        }
    }
    let root_line = format_root_line(row.resource_type, row.resource_id, row.payload_hash);
    PreparedNativeRow::AfterSyntax {
        syntax_items,
        parse_or_items_error: None,
        items,
        outcome: Ok(PreparedNativeOutcome {
            fallback,
            root_line,
            derived_payload,
        }),
    }
}

/// Encode pre-extracted rows directly without JSON-serializing the outer batch envelope.
pub fn encode_retained_fhir_capture_rows(
    context: &NativeCaptureContext<'_>,
    rows: &[NativeCaptureRow<'_>],
) -> Result<RetainedFhirCaptureBatch> {
    validate_context(context)?;
    if rows.len() > MAX_ROWS {
        return Err(failure("row_limit", Some(MAX_ROWS)));
    }
    let estimated_input_bytes: usize = context.source_id.len()
        + context.release_id.len()
        + rows
            .iter()
            .map(|row| {
                128 + row.resource_type.len()
                    + row.resource_id.len()
                    + row.payload_hash.len()
                    + row.payload_json.len()
                    + row.acquired_resource_sha256.map_or(4, str::len)
                    + row.site_id.map_or(4, str::len)
            })
            .sum::<usize>();
    if estimated_input_bytes > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None));
    }
    let allowed_families: BTreeSet<&str> =
        context.resource_types.iter().map(String::as_str).collect();
    let prepared_rows: Vec<PreparedNativeRow> = if rows.len() >= PARALLEL_RETAINED_THRESHOLD {
        rows.par_iter()
            .enumerate()
            .map(|(row_index, row)| prepare_native_row(context, &allowed_families, row, row_index))
            .collect()
    } else {
        rows.iter()
            .enumerate()
            .map(|(row_index, row)| prepare_native_row(context, &allowed_families, row, row_index))
            .collect()
    };
    let mut encoder = Encoder::new(context, None);
    encoder.output.root_bytes.reserve(rows.len() * 96);
    for (row, prepared) in rows.iter().zip(prepared_rows) {
        encoder.commit_prepared_row(row, prepared)?;
    }
    Ok(encoder.finish())
}

/// Cancellation discards every private output buffer, including completed COPY chunks.
pub fn encode_retained_fhir_capture_batch_cancellable(
    input: &[u8],
    cancelled: &AtomicBool,
) -> Result<RetainedFhirCaptureBatch> {
    encode(input, Some(cancelled))
}

fn cancelled(flag: Option<&AtomicBool>, row: Option<usize>) -> Result<()> {
    if flag.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err(failure("cancelled", row));
    }
    Ok(())
}

fn python_whitespace(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}')
}

fn exact_text(text: &str, maximum: usize) -> bool {
    !text.is_empty()
        && text.len() <= maximum
        && text.trim() == text
        && !text.chars().any(char::is_control)
}

fn fhir_id(text: &str) -> bool {
    (1..=64).contains(&text.len()) && id_bytes(text)
}

// Source rows keep publisher ids up to their 256-character column; some exceed FHIR's 64-character limit.
fn source_resource_id(text: &str) -> bool {
    (1..=256).contains(&text.len()) && id_bytes(text)
}

fn id_bytes(text: &str) -> bool {
    text.bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-'))
}

fn validate_context(context: &NativeCaptureContext<'_>) -> Result<()> {
    if !exact_text(context.source_id, 64)
        || !exact_text(context.release_id, 256)
        || context.resource_types.is_empty()
        || context.resource_types.len() > 64
        || context
            .resource_types
            .iter()
            .any(|kind| !exact_text(kind, 64))
        || context.resource_types.iter().collect::<BTreeSet<_>>().len()
            != context.resource_types.len()
        || !matches!(
            context.hash_contract,
            "transport_bound_v1"
                | "transport_neutral_v2"
                | "semantic_content_v3"
                | "semantic_content_v4"
        )
        || context
            .completion_required_version
            .is_some_and(|version| version != 3 || context.hash_contract != "transport_neutral_v2")
    {
        return Err(failure("invalid_context", None));
    }
    Ok(())
}

fn encode(bytes: &[u8], flag: Option<&AtomicBool>) -> Result<RetainedFhirCaptureBatch> {
    cancelled(flag, None)?;
    if bytes.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None));
    }
    let input: Input<'_> =
        serde_json::from_slice(bytes).map_err(|_| failure("invalid_input", None))?;
    let context = NativeCaptureContext {
        source_id: input.source_id.as_ref(),
        release_id: input.release_id.as_ref(),
        observed_at_pg_micros: input.observed_at_pg_micros,
        hash_contract: input.hash_contract.as_ref(),
        completion_required_version: input.completion_required_version,
        resource_types: &input.resource_types,
        derive: input.derive,
    };
    validate_context(&context)?;
    let mut encoder = Encoder::new(&context, flag);
    let mut decoder = serde_json::Deserializer::from_str(input.rows.get());
    if (&mut encoder).deserialize(&mut decoder).is_err() {
        return Err(encoder
            .failure
            .unwrap_or_else(|| failure("invalid_rows", Some(encoder.output.row_count))));
    }
    decoder.end().map_err(|_| failure("invalid_rows", None))?;
    cancelled(flag, None)?;
    Ok(encoder.finish())
}

struct Encoder<'a> {
    input: &'a NativeCaptureContext<'a>,
    flag: Option<&'a AtomicBool>,
    output: RetainedFhirCaptureBatch,
    bytes: usize,
    items: usize,
    syntax_items: usize,
    failure: Option<RetainedFhirCaptureError>,
}

impl<'a> Encoder<'a> {
    fn new(input: &'a NativeCaptureContext<'a>, flag: Option<&'a AtomicBool>) -> Self {
        Self {
            input,
            flag,
            output: RetainedFhirCaptureBatch {
                binding_batches: Vec::new(),
                evidence_batches: Vec::new(),
                plan_batches: Vec::new(),
                root_bytes: Vec::new(),
                families: input
                    .resource_types
                    .iter()
                    .map(|kind| {
                        (
                            kind.clone(),
                            RootFamily {
                                bytes: Vec::new(),
                                row_count: 0,
                            },
                        )
                    })
                    .collect(),
                row_count: 0,
                fallback_rows: Vec::new(),
                owned_output_bytes: 0,
            },
            bytes: 0,
            items: 0,
            syntax_items: 0,
            failure: None,
        }
    }

    fn finish(mut self) -> RetainedFhirCaptureBatch {
        for batches in [
            &mut self.output.binding_batches,
            &mut self.output.evidence_batches,
            &mut self.output.plan_batches,
        ] {
            if let Some(last) = batches.last_mut() {
                last.bytes.extend_from_slice(&(-1i16).to_be_bytes());
            }
        }
        self.output.owned_output_bytes = self.bytes;
        self.output
    }

    fn charge(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| failure("output_limit", Some(self.output.row_count)))?;
        if self.bytes > MAX_OUTPUT_BYTES {
            return Err(failure("output_limit", Some(self.output.row_count)));
        }
        Ok(())
    }

    fn row(&mut self, row: Row<'_>) -> Result<()> {
        let index = Some(self.output.row_count);
        let acquired: Option<Cow<'_, str>> =
            serde_json::from_str(row.acquired_resource_sha256.get())
                .map_err(|_| failure("invalid_rows", index))?;
        let native = NativeCaptureRow {
            resource_type: row.resource_type.as_ref(),
            resource_id: row.resource_id.as_ref(),
            payload_hash: row.payload_hash.as_ref(),
            payload_json: row.payload_json.as_ref(),
            acquired_resource_sha256: acquired.as_deref(),
            site_id: row.site_id.as_deref(),
        };
        self.encode_row(&native)
    }

    fn encode_row(&mut self, row: &NativeCaptureRow<'_>) -> Result<()> {
        let index = Some(self.output.row_count);
        cancelled(self.flag, index)?;
        if self.output.row_count == MAX_ROWS {
            return Err(failure("row_limit", index));
        }
        if !is_valid_marker(
            self.input.completion_required_version,
            row.acquired_resource_sha256,
        ) {
            return Err(failure("invalid_rows", index));
        }
        if !self.output.families.contains_key(row.resource_type)
            || !source_resource_id(row.resource_id)
            || !is_valid_hex_64(row.payload_hash)
        {
            return Err(failure("invalid_row", index));
        }
        self.syntax_items +=
            scan_payload(row.payload_json, self.flag).map_err(|code| failure(code, index))?;
        if self.syntax_items > 2 * MAX_COLLECTION_ITEMS {
            return Err(failure("collection_limit", index));
        }
        let payload: Option<Value> = serde_json::from_str(row.payload_json).ok();
        let mut fallback = payload.is_none();
        let mut derived_payload: Option<Map<String, Value>> = None;
        if let Some(payload) = payload {
            let object = payload
                .as_object()
                .ok_or_else(|| failure("invalid_payload", index))?;
            if object.get("resource_id").and_then(Value::as_str) != Some(row.resource_id)
                || object
                    .get("source_id")
                    .is_some_and(|source| source.as_str() != Some(self.input.source_id))
            {
                return Err(failure("payload_identity", index));
            }
            fallback = validate_values(&payload, &mut self.items, self.flag)
                .map_err(|code| failure(code, index))?;
            let Value::Object(object_owned) = payload else {
                unreachable!();
            };
            if !fallback {
                let needs_derived = self.input.derive
                    && matches!(
                        row.resource_type,
                        "Organization" | "Location" | "InsurancePlan"
                    );
                let hash_result = if needs_derived {
                    let res = payload_hash(
                        &object_owned,
                        row.payload_json,
                        self.input.hash_contract,
                        row.resource_type,
                        self.input.completion_required_version == Some(3),
                    );
                    if res.is_ok() {
                        derived_payload = Some(object_owned);
                    }
                    res
                } else {
                    payload_hash_owned(
                        object_owned,
                        row.payload_json,
                        self.input.hash_contract,
                        row.resource_type,
                        self.input.completion_required_version == Some(3),
                    )
                };
                match hash_result {
                    Ok(hash) if hash != row.payload_hash => {
                        return Err(failure("payload_hash", index))
                    }
                    Ok(_) => {}
                    Err("python_semantics_required" | "unsupported_unicode_semantics") => {
                        fallback = true;
                        derived_payload = None;
                    }
                    Err(code) => return Err(failure(code, index)),
                }
            }
        }
        cancelled(self.flag, index)?;
        let root_line = format_root_line(row.resource_type, row.resource_id, row.payload_hash);
        self.commit_row_output(row, fallback, &root_line, derived_payload.as_ref())
    }

    fn commit_prepared_row(
        &mut self,
        row: &NativeCaptureRow<'_>,
        prepared: PreparedNativeRow,
    ) -> Result<()> {
        let index = Some(self.output.row_count);
        match prepared {
            PreparedNativeRow::EarlyError(err) => Err(err),
            PreparedNativeRow::AfterSyntax {
                syntax_items,
                parse_or_items_error,
                items,
                outcome,
            } => {
                self.syntax_items += syntax_items;
                if self.syntax_items > 2 * MAX_COLLECTION_ITEMS {
                    return Err(failure("collection_limit", index));
                }
                if let Some(err) = parse_or_items_error {
                    return Err(err);
                }
                self.items += items;
                if self.items > MAX_COLLECTION_ITEMS {
                    return Err(failure("collection_limit", index));
                }
                let prepared = outcome?;
                self.commit_row_output(
                    row,
                    prepared.fallback,
                    &prepared.root_line,
                    prepared.derived_payload.as_ref(),
                )
            }
        }
    }

    fn commit_row_output(
        &mut self,
        row: &NativeCaptureRow<'_>,
        fallback: bool,
        root_line: &[u8],
        derived_payload: Option<&Map<String, Value>>,
    ) -> Result<()> {
        if fallback {
            self.charge(std::mem::size_of::<usize>())?;
            self.output.fallback_rows.push(self.output.row_count);
        }
        let newline = usize::from(self.output.row_count > 0);
        let family_newline = usize::from(self.output.families[row.resource_type].row_count > 0);
        self.charge(root_line.len() * 2 + newline + family_newline)?;
        if newline != 0 {
            self.output.root_bytes.push(b'\n');
        }
        self.output.root_bytes.extend_from_slice(root_line);
        let family = self.output.families.get_mut(row.resource_type).unwrap();
        if family_newline != 0 {
            family.bytes.push(b'\n');
        }
        family.bytes.extend_from_slice(root_line);
        family.row_count += 1;
        if let Some(payload) = derived_payload {
            self.derived(row, payload)?;
        }
        self.output.row_count += 1;
        Ok(())
    }

    fn append(&mut self, table: usize, row: &[Option<&[u8]>]) -> Result<()> {
        cancelled(self.flag, Some(self.output.row_count))?;
        let width = 2 + row
            .iter()
            .map(|field| 4 + field.map_or(0, <[u8]>::len))
            .sum::<usize>();
        self.charge(width)?;
        let batches = match table {
            0 => &mut self.output.binding_batches,
            1 => &mut self.output.evidence_batches,
            _ => &mut self.output.plan_batches,
        };
        if batches
            .last()
            .is_none_or(|batch| batch.row_count == MAX_COPY_ROWS)
        {
            self.bytes += COPY_HEADER.len() + 2;
            if self.bytes > MAX_OUTPUT_BYTES {
                return Err(failure("output_limit", Some(self.output.row_count)));
            }
            if let Some(last) = batches.last_mut() {
                last.bytes.extend_from_slice(&(-1i16).to_be_bytes());
            }
            batches.push(CopyBatch {
                bytes: COPY_HEADER.to_vec(),
                row_count: 0,
            });
        }
        let batch = batches.last_mut().unwrap();
        batch
            .bytes
            .extend_from_slice(&(row.len() as i16).to_be_bytes());
        for field in row {
            match field {
                Some(field) => write_field(field, &mut batch.bytes),
                None => batch.bytes.extend_from_slice(&(-1i32).to_be_bytes()),
            }
        }
        batch.row_count += 1;
        Ok(())
    }

    fn derived(&mut self, row: &NativeCaptureRow<'_>, payload: &Map<String, Value>) -> Result<()> {
        let kind = row.resource_type;
        let source = self.input.source_id.as_bytes();
        let release = self.input.release_id.as_bytes();
        let time = self.input.observed_at_pg_micros.to_be_bytes();
        if matches!(kind, "Organization" | "Location") {
            let mut jsonb = vec![1];
            jsonb.extend(object_json_bytes(payload, false, false));
            self.append(
                1,
                &[
                    Some(source),
                    Some(kind.as_bytes()),
                    Some(row.resource_id.as_bytes()),
                    Some(release),
                    Some(row.payload_hash.as_bytes()),
                    Some(&jsonb),
                    Some(&time),
                ],
            )?;
        }
        if kind == "Location" {
            let site = match row.site_id {
                Some(explicit) => uuid_bytes(explicit)
                    .ok_or_else(|| failure("invalid_site_id", Some(self.output.row_count)))?,
                None => crate::fhir_witness_page::registry_location_uuid_bytes(
                    self.input.source_id,
                    row.resource_id,
                )
                .map_err(|code| failure(code, Some(self.output.row_count)))?,
            };
            self.append(
                0,
                &[
                    Some(source),
                    Some(b"Location"),
                    Some(row.resource_id.as_bytes()),
                    None,
                    Some(&site),
                    Some(&time),
                ],
            )?;
        }
        if kind == "InsurancePlan" {
            let empty = Value::Array(Vec::new());
            let references = payload.get("network_refs").unwrap_or(&empty);
            let references = references
                .as_array()
                .ok_or_else(|| failure("invalid_references", Some(self.output.row_count)))?;
            let mut targets = BTreeSet::new();
            for reference in references {
                let reference = reference
                    .as_str()
                    .ok_or_else(|| failure("invalid_references", Some(self.output.row_count)))?;
                let id = reference
                    .strip_prefix("Organization/")
                    .filter(|id| fhir_id(id))
                    .ok_or_else(|| failure("invalid_references", Some(self.output.row_count)))?;
                targets.insert(id);
            }
            let mut refs_jsonb = vec![1];
            write_array(references, false, false, &mut refs_jsonb);
            let mut jsonb = vec![1];
            jsonb.extend(object_json_bytes(payload, false, false));
            let owned = optional_text(payload.get("owned_by_ref"))
                .map_err(|code| failure(code, Some(self.output.row_count)))?;
            let administered = optional_text(payload.get("administered_by_ref"))
                .map_err(|code| failure(code, Some(self.output.row_count)))?;
            for target in targets {
                self.append(
                    2,
                    &[
                        Some(source),
                        Some(release),
                        Some(b"Organization"),
                        Some(target.as_bytes()),
                        Some(row.resource_id.as_bytes()),
                        Some(&refs_jsonb),
                        owned,
                        administered,
                        Some(row.payload_hash.as_bytes()),
                        Some(&jsonb),
                        Some(&time),
                    ],
                )?;
            }
        }
        Ok(())
    }
}

impl<'de> DeserializeSeed<'de> for &mut Encoder<'_> {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for &mut Encoder<'_> {
    type Value = ();
    fn expecting(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("bounded retained rows")
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> std::result::Result<(), S::Error> {
        while let Some(row) = sequence.next_element::<Row<'_>>()? {
            if let Err(error) = self.row(row) {
                self.failure = Some(error);
                return Err(de::Error::custom("retained row rejected"));
            }
        }
        Ok(())
    }
}

fn optional_text(value: Option<&Value>) -> std::result::Result<Option<&[u8]>, &'static str> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text.as_bytes())),
        _ => Err("invalid_reference_value"),
    }
}

fn scan_payload(raw: &str, flag: Option<&AtomicBool>) -> std::result::Result<usize, &'static str> {
    let (mut quoted, mut escaped, mut items) = (false, false, 0);
    for (index, byte) in raw.bytes().enumerate() {
        if index % 4096 == 0 && flag.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
            return Err("cancelled");
        }
        if !quoted && matches!(byte, b',' | b':') {
            items += 1;
            if items > 2 * MAX_COLLECTION_ITEMS {
                return Err("collection_limit");
            }
        }
        if !escaped && byte == b'"' {
            quoted = !quoted;
        }
        escaped = !escaped && byte == b'\\';
    }
    Ok(items)
}

fn validate_values(
    value: &Value,
    items: &mut usize,
    flag: Option<&AtomicBool>,
) -> std::result::Result<bool, &'static str> {
    if flag.is_some_and(|flag| flag.load(Ordering::Relaxed)) {
        return Err("cancelled");
    }
    let mut fallback = false;
    match value {
        // shortcut: Python float spelling requires its encoder, use the reference path for non-integer numbers.
        Value::Number(number) => {
            let text = number.as_str();
            fallback = text.contains(['.', 'e', 'E']) || text.len() > 4_300;
        }
        Value::Array(values) => {
            *items += values.len();
            if *items > MAX_COLLECTION_ITEMS {
                return Err("collection_limit");
            }
            for value in values {
                fallback |= validate_values(value, items, flag)?;
            }
        }
        Value::Object(values) => {
            *items += values.len();
            if *items > MAX_COLLECTION_ITEMS {
                return Err("collection_limit");
            }
            for value in values.values() {
                fallback |= validate_values(value, items, flag)?;
            }
        }
        _ => {}
    }
    Ok(fallback)
}
