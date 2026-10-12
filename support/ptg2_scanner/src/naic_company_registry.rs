// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded byte-preserving company-listing observations, without identity allocation.
//!
//! The caller verifies archive/layout custody. This component verifies the exact
//! member bytes and headers; unprojected fields never acquire a guessed charset.

pub use crate::cms_mlr_registry::COPY_COLUMNS;
use crate::cms_mlr_registry::{MlrConflict, MlrRowIssue, RowStatus};
use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 5_000;
pub const MAX_FIELD_BYTES: usize = 4_096;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const PARSER_VERSION: &str = "naic-company-listings-byte-projection-v1";
pub const HLTH_HEADERS: [&str; 14] = [
    "COMPANY NAME",
    "STATE OF DOMICILE",
    "GROUP CODE",
    "COMPANY CODE",
    "FEIN NUMBER",
    "RJIN FLAG",
    "CRIN ",
    "RJIN",
    "COMPANY STATUS",
    "ADDRESS",
    "CITY",
    "STATE",
    "ZIP ",
    "PHONE",
];
pub const GPAL_HEADERS: [&str; 2] = ["GROUP NAME", "GROUP CODE"];
pub const GPNM_HEADERS: [&str; 7] = [
    "GROUP NAME",
    "GROUP CODE",
    "COMPANY CODE",
    "FEIN NUMBER",
    "COMPANY STATUS",
    "STATE OF DOMICILE",
    "COMPANY NAME",
];

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum NaicMember {
    #[serde(rename = "HLTH.csv")]
    Health,
    #[serde(rename = "GPAL.csv")]
    Groups,
    #[serde(rename = "GPNM.csv")]
    GroupCompanies,
}

impl NaicMember {
    pub fn name(self) -> &'static str {
        match self {
            Self::Health => "HLTH.csv",
            Self::Groups => "GPAL.csv",
            Self::GroupCompanies => "GPNM.csv",
        }
    }

    pub fn headers(self) -> &'static [&'static str] {
        match self {
            Self::Health => &HLTH_HEADERS,
            Self::Groups => &GPAL_HEADERS,
            Self::GroupCompanies => &GPNM_HEADERS,
        }
    }

    pub fn layout(self) -> &'static str {
        match self {
            Self::Health => "naic-cl-hlth-2026-v1",
            Self::Groups => "naic-cl-gpal-2026-v1",
            Self::GroupCompanies => "naic-cl-gpnm-2026-v1",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct NaicEdition {
    pub snapshot_id: String,
    pub edition_id: String,
    pub reporting_year: u16,
    pub artifact_sha256: String,
    pub member: NaicMember,
    pub member_sha256: String,
    pub layout: String,
    pub layout_pdf_sha256: String,
    pub header_sha256: String,
    pub parser_version: String,
}

#[derive(Debug, Serialize)]
pub struct NaicSourceEvidence {
    pub member_name: &'static str,
    pub member_sha256: String,
    pub artifact_sha256: String,
    pub layout: &'static str,
    pub layout_pdf_sha256: String,
    pub header_sha256: String,
    pub parser_version: &'static str,
    pub source_byte_start: usize,
    /// Exclusive, excluding the record terminator but including embedded newlines.
    pub source_byte_end: usize,
    pub raw_record_sha256: String,
    /// Exact unescaped field bytes; source framing remains in the retained member.
    pub raw_fields_hex: BTreeMap<&'static str, String>,
}

#[derive(Debug, Serialize)]
pub struct NaicObservation {
    pub source_record_key: String,
    pub source_row_number: usize,
    pub row_kind: &'static str,
    pub normalized_naic_company: Option<String>,
    pub normalized_naic_group: Option<String>,
    pub normalized_ein: Option<String>,
    pub group_kind: Option<&'static str>,
    pub relationship_kind: Option<&'static str>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    /// Only the explicitly projected ASCII fields, with original header names.
    pub raw_fields: BTreeMap<&'static str, String>,
    pub source_evidence: NaicSourceEvidence,
    pub status: RowStatus,
    pub issues: Vec<MlrRowIssue>,
}

#[derive(Default, Debug, Serialize)]
pub struct NaicCounts {
    pub input_rows: usize,
    pub accepted_rows: usize,
    pub unresolved_rows: usize,
    pub rejected_rows: usize,
    pub distinct_naic_company_codes: usize,
    pub distinct_naic_group_codes: usize,
}

#[derive(Debug, Serialize)]
pub struct NaicRegistryBatch {
    pub edition: NaicEdition,
    pub observations: Vec<NaicObservation>,
    pub conflicts: Vec<MlrConflict>,
    pub counts: NaicCounts,
}

#[derive(Debug)]
pub struct EncodedNaicObservations {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
    pub batch: NaicRegistryBatch,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct NaicRegistryError {
    pub code: &'static str,
    pub source_row_number: Option<usize>,
}

impl fmt::Display for NaicRegistryError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(output, "Company listing rejected: {}", self.code)
    }
}
impl std::error::Error for NaicRegistryError {}

fn fail(code: &'static str, row: Option<usize>) -> NaicRegistryError {
    NaicRegistryError {
        code,
        source_row_number: row,
    }
}

fn digest(input: &[u8]) -> String {
    hex(&Sha256::digest(input))
}

fn hex(input: &[u8]) -> String {
    const DIGITS: &[u8] = b"0123456789abcdef";
    let mut result = String::with_capacity(input.len() * 2);
    for byte in input {
        result.push(DIGITS[(byte >> 4) as usize] as char);
        result.push(DIGITS[(byte & 15) as usize] as char);
    }
    result
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn validate_edition(input: &[u8], edition: &NaicEdition) -> Result<(), NaicRegistryError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(fail("input_limit", None));
    }
    if uuid_bytes(&edition.snapshot_id).is_none()
        || edition.snapshot_id.len() != 36
        || edition.snapshot_id.to_ascii_lowercase() != edition.snapshot_id
        || edition.reporting_year != 2026
        || edition.edition_id.is_empty()
        || edition.edition_id.len() > 128
        || edition.edition_id.trim() != edition.edition_id
        || edition.edition_id.chars().any(char::is_control)
        || edition.layout != edition.member.layout()
        || edition.parser_version != PARSER_VERSION
        || [
            &edition.artifact_sha256,
            &edition.member_sha256,
            &edition.layout_pdf_sha256,
            &edition.header_sha256,
        ]
        .iter()
        .any(|value| !valid_digest(value))
    {
        return Err(fail("invalid_edition", None));
    }
    if digest(input) != edition.member_sha256 {
        return Err(fail("digest_mismatch", None));
    }
    let headers = serde_json::to_vec(edition.member.headers())
        .map_err(|_| fail("invalid_headers", Some(1)))?;
    if digest(&headers) != edition.header_sha256 {
        return Err(fail("header_digest_mismatch", Some(1)));
    }
    Ok(())
}

#[derive(Clone, Copy)]
enum CsvState {
    FieldStart,
    Unquoted,
    Quoted,
    AfterQuote,
}

struct RecordSpan {
    start: usize,
    end: usize,
    line: usize,
}

// Byte framing closes the csv crate's permissive quote/blank-record cases.
fn record_spans(input: &[u8], width: usize) -> Result<Vec<RecordSpan>, NaicRegistryError> {
    let mut spans = Vec::new();
    let (mut state, mut columns) = (CsvState::FieldStart, 1);
    let (mut start, mut line, mut start_line, mut index) = (0, 1, 1, 0);
    while index < input.len() {
        let byte = input[index];
        match (state, byte) {
            (CsvState::Quoted, b'"') => state = CsvState::AfterQuote,
            (CsvState::Quoted, _) => {}
            (CsvState::AfterQuote, b'"') => state = CsvState::Quoted,
            (CsvState::FieldStart, b'"') => state = CsvState::Quoted,
            (CsvState::Unquoted, b'"') => return Err(fail("invalid_csv", Some(start_line))),
            (_, b',') => {
                columns += 1;
                state = CsvState::FieldStart;
            }
            (_, b'\r' | b'\n') => {
                push_span(&mut spans, start, index, start_line, columns, width)?;
                if byte == b'\r' && input.get(index + 1) == Some(&b'\n') {
                    index += 1;
                }
                start = index + 1;
                start_line = line + 1;
                columns = 1;
                state = CsvState::FieldStart;
            }
            (CsvState::AfterQuote, _) => return Err(fail("invalid_csv", Some(start_line))),
            _ => state = CsvState::Unquoted,
        }
        if byte == b'\n' || byte == b'\r' && input.get(index + 1) != Some(&b'\n') {
            line += 1;
        }
        index += 1;
    }
    if matches!(state, CsvState::Quoted) {
        return Err(fail("invalid_csv", Some(start_line)));
    }
    if start < input.len() {
        push_span(&mut spans, start, input.len(), start_line, columns, width)?;
    }
    if spans.is_empty() {
        return Err(fail("invalid_headers", Some(1)));
    }
    Ok(spans)
}

fn push_span(
    spans: &mut Vec<RecordSpan>,
    start: usize,
    end: usize,
    line: usize,
    columns: usize,
    width: usize,
) -> Result<(), NaicRegistryError> {
    if start == end || columns != width {
        return Err(fail(
            if spans.is_empty() {
                "invalid_headers"
            } else {
                "invalid_csv"
            },
            Some(line),
        ));
    }
    if spans.len() > MAX_ROWS {
        return Err(fail("row_limit", Some(line)));
    }
    spans.push(RecordSpan { start, end, line });
    Ok(())
}

pub fn parse_naic_company_registry_member(
    input: &[u8],
    edition: &NaicEdition,
) -> Result<NaicRegistryBatch, NaicRegistryError> {
    validate_edition(input, edition)?;
    let spans = record_spans(input, edition.member.headers().len())?;
    if !input[..spans[0].end].is_ascii() {
        return Err(fail("invalid_headers", Some(1)));
    }
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(false)
        .from_reader(input);
    let mut record = csv::ByteRecord::new();
    let mut observations = Vec::new();
    for (ordinal, span) in spans.iter().enumerate() {
        if !reader
            .read_byte_record(&mut record)
            .map_err(|_| fail("invalid_csv", Some(span.line)))?
        {
            return Err(fail("invalid_csv", Some(span.line)));
        }
        if ordinal == 0 {
            if !record.iter().eq(edition
                .member
                .headers()
                .iter()
                .map(|header| header.as_bytes()))
            {
                return Err(fail("invalid_headers", Some(1)));
            }
        } else {
            if record.iter().any(|field| field.len() > MAX_FIELD_BYTES) {
                return Err(fail("field_limit", Some(span.line)));
            }
            observations.push(observation(input, edition, span, &record));
        }
    }
    if reader
        .read_byte_record(&mut record)
        .map_err(|_| fail("invalid_csv", None))?
    {
        return Err(fail("invalid_csv", None));
    }
    let conflicts = classify_conflicts(&mut observations);
    let counts = counts(&observations);
    Ok(NaicRegistryBatch {
        edition: edition.clone(),
        observations,
        conflicts,
        counts,
    })
}

fn issue(issues: &mut Vec<MlrRowIssue>, field: &'static str, code: &'static str, rejecting: bool) {
    issues.push(MlrRowIssue {
        field,
        code,
        rejecting,
    });
}

fn classify(issues: &[MlrRowIssue]) -> RowStatus {
    if issues.iter().any(|issue| issue.rejecting) {
        RowStatus::Rejected
    } else if issues.is_empty() {
        RowStatus::Accepted
    } else {
        RowStatus::Unresolved
    }
}

fn number(
    raw: &str,
    company: bool,
    field: &'static str,
    issues: &mut Vec<MlrRowIssue>,
) -> Option<String> {
    let value = raw.trim_matches(' ');
    if value.is_empty() {
        issue(issues, field, "missing_identifier", false);
        return None;
    }
    if value.len() > 5
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.bytes().all(|byte| byte == b'0')
    {
        issue(issues, field, "invalid_identifier", true);
        return None;
    }
    Some(if company {
        format!("{value:0>5}")
    } else {
        value.trim_start_matches('0').to_owned()
    })
}

fn ein(raw: &str, issues: &mut Vec<MlrRowIssue>) -> Option<String> {
    let value = raw.trim_matches(' ');
    if value.is_empty() {
        issue(issues, "FEIN NUMBER", "missing_identifier", false);
        return None;
    }
    let normalized = if value.len() == 10 && value.as_bytes()[2] == b'-' {
        value.replace('-', "")
    } else {
        value.to_owned()
    };
    if normalized.len() == 9
        && normalized.bytes().all(|byte| byte.is_ascii_digit())
        && normalized.bytes().any(|byte| byte != b'0')
    {
        Some(normalized)
    } else {
        issue(issues, "FEIN NUMBER", "invalid_identifier", true);
        None
    }
}

fn observation(
    input: &[u8],
    edition: &NaicEdition,
    span: &RecordSpan,
    record: &csv::ByteRecord,
) -> NaicObservation {
    let mut issues = Vec::new();
    let mut raw_fields = BTreeMap::new();
    let mut raw_fields_hex = BTreeMap::new();
    for (header, bytes) in edition.member.headers().iter().zip(record.iter()) {
        raw_fields_hex.insert(*header, hex(bytes));
        if matches!(
            *header,
            "COMPANY NAME"
                | "GROUP NAME"
                | "STATE OF DOMICILE"
                | "GROUP CODE"
                | "COMPANY CODE"
                | "FEIN NUMBER"
                | "COMPANY STATUS"
        ) {
            if bytes.iter().all(|byte| matches!(byte, b' '..=b'~')) {
                raw_fields.insert(
                    *header,
                    String::from_utf8(bytes.to_vec()).expect("ASCII was checked"),
                );
            } else {
                issue(&mut issues, header, "invalid_ascii_projection", true);
            }
        }
    }
    let normalized_naic_company = (edition.member != NaicMember::Groups)
        .then(|| {
            number(
                raw_fields.get("COMPANY CODE").map_or("", String::as_str),
                true,
                "COMPANY CODE",
                &mut issues,
            )
        })
        .flatten();
    let normalized_naic_group = number(
        raw_fields.get("GROUP CODE").map_or("", String::as_str),
        false,
        "GROUP CODE",
        &mut issues,
    );
    let normalized_ein = (edition.member != NaicMember::Groups)
        .then(|| {
            ein(
                raw_fields.get("FEIN NUMBER").map_or("", String::as_str),
                &mut issues,
            )
        })
        .flatten();
    if let Some(status) = raw_fields.get("COMPANY STATUS")
        && !matches!(status.as_str(), "0" | "1" | "4" | "6")
    {
        issue(
            &mut issues,
            "COMPANY STATUS",
            "invalid_company_status",
            true,
        );
    }
    let relationship_kind = if normalized_naic_company.is_some() && normalized_naic_group.is_some()
    {
        issue(&mut issues, "valid_from", "unknown_effective_date", false);
        Some("reported_affiliation")
    } else {
        None
    };
    NaicObservation {
        source_record_key: format!("{}:row:{}", edition.member.name(), span.line),
        source_row_number: span.line,
        row_kind: if edition.member == NaicMember::Groups {
            "group"
        } else {
            "company"
        },
        normalized_naic_company,
        group_kind: normalized_naic_group.as_ref().map(|_| "naic_group"),
        normalized_naic_group,
        normalized_ein,
        relationship_kind,
        valid_from: None,
        valid_to: None,
        raw_fields,
        source_evidence: NaicSourceEvidence {
            member_name: edition.member.name(),
            member_sha256: edition.member_sha256.clone(),
            artifact_sha256: edition.artifact_sha256.clone(),
            layout: edition.member.layout(),
            layout_pdf_sha256: edition.layout_pdf_sha256.clone(),
            header_sha256: edition.header_sha256.clone(),
            parser_version: PARSER_VERSION,
            source_byte_start: span.start,
            source_byte_end: span.end,
            raw_record_sha256: digest(&input[span.start..span.end]),
            raw_fields_hex,
        },
        status: classify(&issues),
        issues,
    }
}

fn classify_conflicts(observations: &mut [NaicObservation]) -> Vec<MlrConflict> {
    let mut scopes: BTreeMap<(&str, String), BTreeMap<String, Vec<usize>>> = BTreeMap::new();
    for (index, row) in observations.iter().enumerate() {
        for (field, key, value) in [
            (
                "FEIN NUMBER",
                row.normalized_naic_company.as_ref(),
                row.normalized_ein.as_ref(),
            ),
            (
                "GROUP CODE",
                row.normalized_naic_company.as_ref(),
                row.normalized_naic_group.as_ref(),
            ),
            (
                "COMPANY CODE",
                row.normalized_ein.as_ref(),
                row.normalized_naic_company.as_ref(),
            ),
        ] {
            if let (Some(key), Some(value)) = (key, value) {
                scopes
                    .entry((field, key.clone()))
                    .or_default()
                    .entry(value.clone())
                    .or_default()
                    .push(index);
            }
        }
    }
    let mut conflicts = Vec::new();
    for ((field, _), values) in scopes.into_iter().filter(|(_, values)| values.len() > 1) {
        let mut source_rows = Vec::new();
        for index in values.values().flatten().copied() {
            issue(
                &mut observations[index].issues,
                field,
                "conflicting_company_identifier_evidence",
                false,
            );
            observations[index].status = classify(&observations[index].issues);
            source_rows.push(observations[index].source_row_number);
        }
        source_rows.sort_unstable();
        conflicts.push(MlrConflict {
            code: "conflicting_company_identifier_evidence",
            field,
            source_rows,
            values: values.into_keys().collect(),
        });
    }
    conflicts
}

fn counts(observations: &[NaicObservation]) -> NaicCounts {
    let mut result = NaicCounts {
        input_rows: observations.len(),
        ..NaicCounts::default()
    };
    let (mut companies, mut groups) = (BTreeSet::new(), BTreeSet::new());
    for row in observations {
        match row.status {
            RowStatus::Accepted => result.accepted_rows += 1,
            RowStatus::Unresolved => result.unresolved_rows += 1,
            RowStatus::Rejected => result.rejected_rows += 1,
        }
        companies.extend(row.normalized_naic_company.as_ref());
        groups.extend(row.normalized_naic_group.as_ref());
    }
    result.distinct_naic_company_codes = companies.len();
    result.distinct_naic_group_codes = groups.len();
    result
}

/// One complete member per frame; callers retain their existing edition-wide caps.
pub fn encode_naic_company_registry_observations(
    input: &[u8],
    edition: &NaicEdition,
) -> Result<EncodedNaicObservations, NaicRegistryError> {
    let batch = parse_naic_company_registry_member(input, edition)?;
    let snapshot_id =
        uuid_bytes(&edition.snapshot_id).ok_or_else(|| fail("invalid_edition", None))?;
    let mut copy_bytes = COPY_HEADER.to_vec();
    for row in &batch.observations {
        let mut observation_json = vec![1];
        if serde_json::to_writer(&mut observation_json, row).is_err() {
            return Err(fail("invalid_jsonb", Some(row.source_row_number)));
        }
        let mut issues_json = vec![1];
        if serde_json::to_writer(&mut issues_json, &row.issues).is_err() {
            return Err(fail("invalid_jsonb", Some(row.source_row_number)));
        }
        let source_row = i32::try_from(row.source_row_number)
            .map_err(|_| fail("invalid_source_row", Some(row.source_row_number)))?
            .to_be_bytes();
        let status: &[u8] = match row.status {
            RowStatus::Accepted => b"accepted",
            RowStatus::Unresolved => b"unresolved",
            RowStatus::Rejected => b"rejected",
        };
        let fields: [&[u8]; 6] = [
            &snapshot_id,
            row.source_record_key.as_bytes(),
            &source_row,
            status,
            &observation_json,
            &issues_json,
        ];
        if copy_bytes.len() + 2 + fields.iter().map(|field| 4 + field.len()).sum::<usize>() + 2
            > MAX_COPY_BYTES
        {
            return Err(fail("output_limit", Some(row.source_row_number)));
        }
        copy_bytes.extend_from_slice(&(COPY_COLUMNS.len() as i16).to_be_bytes());
        for field in fields {
            write_field(field, &mut copy_bytes);
        }
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedNaicObservations {
        row_count: batch.observations.len(),
        copy_bytes,
        batch,
    })
}
