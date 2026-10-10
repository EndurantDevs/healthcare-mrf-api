// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded observations from the inspected HIOS issuer workbook input layout.
//! Acceptance describes syntax, not identity resolution or current ownership.

use crate::cms_mlr_registry::{MlrConflict, MlrRowIssue, RowStatus};
use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 5_000;
pub const MAX_FIELD_BYTES: usize = 32_768;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const COPY_COLUMNS: [&str; 6] = [
    "snapshot_id",
    "source_record_key",
    "source_row_number",
    "status",
    "observation_json",
    "issues_json",
];
pub const HEADERS: [&str; 19] = [
    "hios_issuer_id",
    "issr_lgl_name",
    "marketingname",
    "state",
    "individualmarket",
    "smallgroupmarket",
    "unknownmarket",
    "largemarket",
    "federal_ein",
    "active",
    "datecreated",
    "lastmodifieddate",
    "databasecompanyid",
    "org_adr1",
    "org_adr2",
    "org_city",
    "org_state",
    "org_zip",
    "org_zip4",
];
pub const LAYOUT: &str = "hios-2026-08-06-issuer-v1";
const JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFinderEdition {
    pub snapshot_id: String,
    pub reporting_year: u16,
    pub input_sha256: String,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum WorkbookCellType {
    #[serde(rename = "s")]
    SharedString,
    #[serde(rename = "n")]
    Numeric,
    #[serde(rename = "inlineStr")]
    InlineString,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PlanFinderWorkbookRow {
    pub source_row: usize,
    pub values: [Option<String>; 19],
    pub raw_values: [Option<String>; 19],
    pub cell_types: [Option<WorkbookCellType>; 19],
    pub style_ids: [Option<u8>; 19],
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkbookInput {
    component: String,
    revision: u8,
    layout: String,
    artifact_sha256: String,
    workbook_sha256: String,
    sheet: String,
    headers: [String; 19],
    rows: Vec<PlanFinderWorkbookRow>,
}

#[derive(Debug, Serialize)]
pub struct PlanFinderSourceEvidence {
    pub component: &'static str,
    pub revision: u8,
    pub artifact_sha256: String,
    pub workbook_sha256: String,
    pub batch_sha256: String,
    pub layout: &'static str,
    pub sheet: &'static str,
    pub headers: [&'static str; 19],
    pub source_row: usize,
    pub values: [Option<String>; 19],
    pub raw_values: [Option<String>; 19],
    pub cell_types: [Option<WorkbookCellType>; 19],
    pub style_ids: [Option<u8>; 19],
}

#[derive(Debug, Serialize)]
pub struct PlanFinderAssertion {
    pub source_row_number: usize,
    pub submission_id: String,
    pub row_kind: &'static str,
    pub state: Option<String>,
    pub normalized_ein: Option<String>,
    pub normalized_naic_company: Option<String>,
    pub normalized_naic_group: Option<String>,
    pub hios: Option<String>,
    /// This source anchor is meaningful only together with edition.snapshot_id.
    pub company_key: Option<String>,
    pub group_kind: Option<&'static str>,
    pub status: RowStatus,
    pub issues: Vec<MlrRowIssue>,
    /// Exactly the nineteen observed fields, including missing/null evidence.
    pub raw_fields: BTreeMap<&'static str, Option<String>>,
    pub source_evidence: PlanFinderSourceEvidence,
}

#[derive(Default, Debug, Serialize)]
pub struct PlanFinderCounts {
    pub input_rows: usize,
    pub accepted_rows: usize,
    pub unresolved_rows: usize,
    pub rejected_rows: usize,
    pub issuer_rows: usize,
    pub source_company_anchors: usize,
    pub distinct_hios_issuers: usize,
}

#[derive(Debug, Serialize)]
pub struct PlanFinderRegistryBatch {
    pub edition: PlanFinderEdition,
    pub observations: Vec<PlanFinderAssertion>,
    /// Conflicts require the complete edition landing, rather than one input page.
    pub conflicts: Vec<MlrConflict>,
    pub counts: PlanFinderCounts,
}

#[derive(Debug)]
pub struct EncodedPlanFinderObservations {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
    pub batch: PlanFinderRegistryBatch,
}

#[derive(Debug, Serialize)]
pub struct PlanFinderRegistryError {
    pub code: &'static str,
    pub source_row_number: Option<usize>,
    pub message: &'static str,
}

impl fmt::Display for PlanFinderRegistryError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.message)
    }
}
impl std::error::Error for PlanFinderRegistryError {}

fn fail(code: &'static str, row: Option<usize>, message: &'static str) -> PlanFinderRegistryError {
    PlanFinderRegistryError {
        code,
        source_row_number: row,
        message,
    }
}

fn digest_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn validate_input(
    input: &[u8],
    edition: &PlanFinderEdition,
) -> Result<WorkbookInput, PlanFinderRegistryError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(fail(
            "input_limit",
            None,
            "Plan Finder batch exceeds byte limit",
        ));
    }
    if uuid_bytes(&edition.snapshot_id).is_none()
        || !(2010..=2100).contains(&edition.reporting_year)
        || !digest_valid(&edition.input_sha256)
    {
        return Err(fail(
            "invalid_edition",
            None,
            "Plan Finder edition metadata is invalid",
        ));
    }
    let supplied: WorkbookInput = serde_json::from_slice(input).map_err(|_| {
        fail(
            "invalid_json",
            None,
            "Plan Finder batch has an invalid or unknown DTO field",
        )
    })?;
    if supplied.component != "cms_planfinder_workbook_input"
        || supplied.revision != 1
        || supplied.layout != LAYOUT
        || supplied.sheet != "ISSUER_1"
        || supplied.headers.iter().map(String::as_str).ne(HEADERS)
        || !digest_valid(&supplied.artifact_sha256)
        || !digest_valid(&supplied.workbook_sha256)
    {
        return Err(fail(
            "invalid_layout",
            None,
            "Plan Finder workbook scope or header pin is invalid",
        ));
    }
    if supplied.rows.is_empty() || supplied.rows.len() > MAX_ROWS {
        return Err(fail(
            "row_limit",
            None,
            "Plan Finder batch requires one through 5000 rows",
        ));
    }
    if supplied.workbook_sha256 != edition.input_sha256 {
        return Err(fail(
            "digest_mismatch",
            None,
            "Plan Finder workbook digest does not match edition",
        ));
    }
    Ok(supplied)
}

// Narrow exact decimal presentation normalization for workbook identifier cells.
// Unsupported/nonintegral/negative values remain raw for semantic rejection.
fn numeric_identifier(raw: &str, width: Option<usize>) -> Option<String> {
    let value = raw.trim();
    let negative = value.starts_with('-');
    let unsigned = value.strip_prefix(['-', '+']).unwrap_or(value);
    let mut parts = unsigned.split(['e', 'E']);
    let mantissa = parts.next()?;
    let exponent = parts
        .next()
        .map(str::parse::<i32>)
        .transpose()
        .ok()?
        .unwrap_or(0);
    if parts.next().is_some() {
        return None;
    }
    let mut decimals = mantissa.split('.');
    let integer = decimals.next()?;
    let fraction = decimals.next().unwrap_or("");
    if decimals.next().is_some()
        || integer.is_empty()
        || !integer
            .bytes()
            .chain(fraction.bytes())
            .all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{integer}{fraction}");
    let digits = digits.trim_start_matches('0');
    let coefficient = if digits.is_empty() {
        0
    } else {
        digits.parse::<u128>().ok()?
    };
    let power = exponent.checked_sub(i32::try_from(fraction.len()).ok()?)?;
    let number = if coefficient == 0 {
        0
    } else if power >= 0 {
        coefficient.checked_mul(10u128.checked_pow(power as u32)?)?
    } else {
        let divisor = 10u128.checked_pow(power.checked_neg()? as u32)?;
        if coefficient % divisor != 0 {
            return None;
        }
        coefficient / divisor
    };
    if (negative && number != 0) || number >= 10u128.pow(20) {
        return None;
    }
    let text = number.to_string();
    Some(width.map_or(text.clone(), |width| format!("{text:0>width$}")))
}

fn validate_row(row: &PlanFinderWorkbookRow) -> Result<(), PlanFinderRegistryError> {
    let position = Some(row.source_row);
    if !(2..=100_001).contains(&row.source_row) {
        return Err(fail(
            "invalid_source_row",
            position,
            "Plan Finder source row is outside workbook limits",
        ));
    }
    for index in 0..HEADERS.len() {
        for text in [row.values[index].as_ref(), row.raw_values[index].as_ref()]
            .into_iter()
            .flatten()
        {
            if text.len() > MAX_FIELD_BYTES || text.contains('\0') {
                return Err(fail(
                    "invalid_field",
                    position,
                    "Plan Finder field exceeds limits or contains NUL",
                ));
            }
        }
        let (Some(cell_type), Some(style)) = (row.cell_types[index], row.style_ids[index]) else {
            if row.cell_types[index].is_some()
                || row.style_ids[index].is_some()
                || row.raw_values[index].is_some()
                || row.values[index].is_some()
            {
                return Err(fail(
                    "invalid_cell",
                    position,
                    "Plan Finder missing-cell evidence is inconsistent",
                ));
            }
            continue;
        };
        if style > 2 {
            return Err(fail(
                "invalid_cell",
                position,
                "Plan Finder style is unsupported",
            ));
        }
        let mut expected = row.raw_values[index].clone();
        if cell_type == WorkbookCellType::Numeric
            && let Some(raw) = &expected
        {
            if !raw.parse::<f64>().is_ok_and(f64::is_finite) {
                return Err(fail(
                    "invalid_cell",
                    position,
                    "Plan Finder numeric cell is malformed",
                ));
            }
            let width = match index {
                0 | 17 => Some(5),
                8 => Some(9),
                18 => Some(4),
                _ => None,
            };
            if matches!(index, 0 | 8 | 12 | 17 | 18) {
                expected = Some(numeric_identifier(raw, width).unwrap_or_else(|| raw.clone()));
            }
        }
        if expected != row.values[index] {
            return Err(fail(
                "invalid_cell",
                position,
                "Plan Finder normalized and raw cell evidence disagree",
            ));
        }
    }
    Ok(())
}

fn issue(issues: &mut Vec<MlrRowIssue>, field: &'static str, code: &'static str, rejecting: bool) {
    issues.push(MlrRowIssue {
        field,
        code,
        rejecting,
    });
}

fn identifier(
    raw: Option<&str>,
    width: usize,
    field: &'static str,
    required: bool,
    issues: &mut Vec<MlrRowIssue>,
) -> Option<String> {
    let value = raw.unwrap_or("");
    if value.is_empty() {
        issue(issues, field, "missing_identifier", required);
        None
    } else if value.len() != width
        || !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.bytes().all(|byte| byte == b'0')
    {
        issue(issues, field, "invalid_identifier", true);
        None
    } else {
        Some(value.to_owned())
    }
}

fn assertion(
    row: PlanFinderWorkbookRow,
    input: &WorkbookInput,
    batch_sha256: &str,
) -> PlanFinderAssertion {
    let mut issues = Vec::new();
    let hios = identifier(
        row.values[0].as_deref(),
        5,
        "hios_issuer_id",
        true,
        &mut issues,
    );
    let ein_text = row.values[8]
        .as_deref()
        .unwrap_or("")
        .replace([' ', '-'], "");
    let normalized_ein = identifier(Some(&ein_text), 9, "federal_ein", false, &mut issues);
    if row.raw_values[1].as_deref().unwrap_or("").trim().is_empty() {
        issue(&mut issues, "issr_lgl_name", "missing_company_name", false);
    }
    let state_text = row.values[3]
        .as_deref()
        .unwrap_or("")
        .trim()
        .to_ascii_uppercase();
    let state = JURISDICTIONS
        .split_ascii_whitespace()
        .any(|state| state == state_text)
        .then_some(state_text.clone());
    if state.is_none() {
        issue(
            &mut issues,
            "state",
            if state_text.is_empty() {
                "missing_jurisdiction"
            } else {
                "invalid_jurisdiction"
            },
            !state_text.is_empty(),
        );
    }
    let status = if issues.iter().any(|issue| issue.rejecting) {
        RowStatus::Rejected
    } else if issues.is_empty() {
        RowStatus::Accepted
    } else {
        RowStatus::Unresolved
    };
    let raw_fields = HEADERS
        .into_iter()
        .zip(row.raw_values.iter().cloned())
        .collect();
    PlanFinderAssertion {
        source_row_number: row.source_row,
        submission_id: format!("row:{}", row.source_row),
        row_kind: "issuer_filing",
        state,
        hios,
        company_key: normalized_ein
            .as_ref()
            .map(|ein| format!("cms_planfinder:ein:{ein}")),
        normalized_ein,
        normalized_naic_company: None,
        normalized_naic_group: None,
        group_kind: None,
        status,
        issues,
        raw_fields,
        source_evidence: PlanFinderSourceEvidence {
            component: "cms_planfinder_workbook_input",
            revision: 1,
            artifact_sha256: input.artifact_sha256.clone(),
            workbook_sha256: input.workbook_sha256.clone(),
            batch_sha256: batch_sha256.to_owned(),
            layout: LAYOUT,
            sheet: "ISSUER_1",
            headers: HEADERS,
            source_row: row.source_row,
            values: row.values,
            raw_values: row.raw_values,
            cell_types: row.cell_types,
            style_ids: row.style_ids,
        },
    }
}

/// Normalize one verified decoder batch; cross-batch identity conflicts remain edition-wide set work.
pub fn parse_cms_planfinder_batch(
    input: &[u8],
    edition: &PlanFinderEdition,
) -> Result<PlanFinderRegistryBatch, PlanFinderRegistryError> {
    let mut supplied = validate_input(input, edition)?;
    let batch_sha256 = Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut previous = None;
    for row in &supplied.rows {
        validate_row(row)?;
        if previous.is_some_and(|position| row.source_row != position + 1) {
            return Err(fail(
                "invalid_source_row",
                Some(row.source_row),
                "Plan Finder batch row positions are noncontiguous",
            ));
        }
        previous = Some(row.source_row);
    }
    let observations: Vec<_> = std::mem::take(&mut supplied.rows)
        .into_iter()
        .map(|row| assertion(row, &supplied, &batch_sha256))
        .collect();
    let mut counts = PlanFinderCounts {
        input_rows: observations.len(),
        ..PlanFinderCounts::default()
    };
    let mut companies = BTreeSet::new();
    let mut issuers = BTreeSet::new();
    for observation in &observations {
        match observation.status {
            RowStatus::Accepted => counts.accepted_rows += 1,
            RowStatus::Unresolved => counts.unresolved_rows += 1,
            RowStatus::Rejected => counts.rejected_rows += 1,
        }
        if let Some(hios) = &observation.hios {
            counts.issuer_rows += 1;
            issuers.insert(hios);
        }
        if let Some(company) = &observation.company_key {
            companies.insert(company);
        }
    }
    counts.source_company_anchors = companies.len();
    counts.distinct_hios_issuers = issuers.len();
    Ok(PlanFinderRegistryBatch {
        edition: edition.clone(),
        observations,
        conflicts: Vec::new(),
        counts,
    })
}

fn jsonb_bytes(
    value: &impl Serialize,
    position: usize,
) -> Result<Vec<u8>, PlanFinderRegistryError> {
    let mut output = vec![1];
    serde_json::to_writer(&mut output, value).map_err(|_| {
        fail(
            "invalid_jsonb",
            Some(position),
            "Plan Finder JSONB cannot be encoded",
        )
    })?;
    Ok(output)
}

fn encode_observation(
    observation: &PlanFinderAssertion,
    snapshot: &[u8; 16],
    output: &mut Vec<u8>,
) -> Result<(), PlanFinderRegistryError> {
    let position = observation.source_row_number;
    let source_key = format!("row:{position}");
    let source_row = (position as i32).to_be_bytes();
    let status: &[u8] = match observation.status {
        RowStatus::Accepted => b"accepted",
        RowStatus::Unresolved => b"unresolved",
        RowStatus::Rejected => b"rejected",
    };
    let observation_json = jsonb_bytes(observation, position)?;
    let issues_json = jsonb_bytes(&observation.issues, position)?;
    let fields: [&[u8]; 6] = [
        snapshot,
        source_key.as_bytes(),
        &source_row,
        status,
        &observation_json,
        &issues_json,
    ];
    if output.len() + 2 + fields.iter().map(|field| 4 + field.len()).sum::<usize>() + 2
        > MAX_COPY_BYTES
    {
        return Err(fail(
            "output_limit",
            Some(position),
            "Plan Finder COPY exceeds byte limit",
        ));
    }
    output.extend_from_slice(&6i16.to_be_bytes());
    for field in fields {
        write_field(field, output);
    }
    Ok(())
}

/// Return complete six-column binary COPY or an error with no partial output.
pub fn encode_cms_planfinder_observations(
    input: &[u8],
    edition: &PlanFinderEdition,
) -> Result<EncodedPlanFinderObservations, PlanFinderRegistryError> {
    let batch = parse_cms_planfinder_batch(input, edition)?;
    let snapshot = uuid_bytes(&edition.snapshot_id)
        .ok_or_else(|| fail("invalid_edition", None, "Snapshot UUID is invalid"))?;
    let mut copy_bytes = Vec::with_capacity(input.len().min(64 * 1024));
    copy_bytes.extend_from_slice(COPY_HEADER);
    for observation in &batch.observations {
        encode_observation(observation, &snapshot, &mut copy_bytes)?;
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedPlanFinderObservations {
        row_count: batch.observations.len(),
        copy_bytes,
        batch,
    })
}
