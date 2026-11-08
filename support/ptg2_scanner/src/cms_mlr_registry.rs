//! Bounded CMS commercial MLR source assertions, without durable identity allocation.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 5_000;
pub const MAX_FIELD_BYTES: usize = 4_096;
pub const MAX_COPY_BYTES: usize = 16 * 1024 * 1024;
pub const COPY_COLUMNS: [&str; 6] = [
    "snapshot_id",
    "source_record_key",
    "source_row_number",
    "status",
    "observation_json",
    "issues_json",
];
pub const HEADERS: [&str; 17] = [
    "mr_submission_template_id",
    "business_state",
    "group_affiliation",
    "company_pk",
    "hios_issuer_id",
    "company_name",
    "company_address",
    "domiciliary_state",
    "naic_group_code",
    "naic_company_code",
    "federal_ein",
    "am_best_number",
    "dba_marketing_name",
    "not_for_profit",
    "created_date",
    "merge_markets_ind_small_grp",
    "fit_exempt",
];
const JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI";

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct MlrEdition {
    pub snapshot_id: String,
    pub reporting_year: u16,
    pub input_sha256: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RowStatus {
    Accepted,
    Unresolved,
    Rejected,
}

#[derive(Debug, Serialize)]
pub struct MlrRowIssue {
    pub field: &'static str,
    pub code: &'static str,
    pub rejecting: bool,
}

#[derive(Debug, Serialize)]
pub struct MlrAssertion {
    pub source_row_number: usize,
    pub submission_id: String,
    pub row_kind: &'static str,
    pub state: Option<String>,
    pub normalized_ein: Option<String>,
    pub normalized_naic_company: Option<String>,
    pub normalized_naic_group: Option<String>,
    pub hios: Option<String>,
    /// This anchor is meaningful only together with the enclosing edition.snapshot_id.
    pub company_key: Option<String>,
    pub group_kind: Option<&'static str>,
    pub status: RowStatus,
    pub issues: Vec<MlrRowIssue>,
    pub raw_fields: BTreeMap<&'static str, String>,
}

#[derive(Debug, Serialize)]
pub struct MlrConflict {
    pub code: &'static str,
    pub field: &'static str,
    pub source_rows: Vec<usize>,
    pub values: Vec<String>,
}

#[derive(Default, Debug, Serialize)]
pub struct MlrCounts {
    pub input_rows: usize,
    pub accepted_rows: usize,
    pub unresolved_rows: usize,
    pub rejected_rows: usize,
    pub grand_total_rows: usize,
    pub issuer_rows: usize,
    pub nonempty_raw_company_pk_rows: usize,
    pub source_company_anchors: usize,
    pub distinct_hios_issuers: usize,
    pub distinct_naic_company_codes: usize,
    pub distinct_naic_group_codes: usize,
    pub ambiguous_group_labels: usize,
}

#[derive(Debug, Serialize)]
pub struct MlrRegistryBatch {
    pub edition: MlrEdition,
    pub observations: Vec<MlrAssertion>,
    pub conflicts: Vec<MlrConflict>,
    pub counts: MlrCounts,
}

#[derive(Debug)]
pub struct EncodedMlrObservations {
    pub copy_bytes: Vec<u8>,
    pub row_count: usize,
    pub batch: MlrRegistryBatch,
}

#[derive(Debug, Serialize)]
pub struct MlrRegistryError {
    pub code: &'static str,
    pub source_row_number: Option<usize>,
    pub message: &'static str,
}

impl fmt::Display for MlrRegistryError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str(self.message)
    }
}
impl std::error::Error for MlrRegistryError {}

fn fail(code: &'static str, row: Option<usize>, message: &'static str) -> MlrRegistryError {
    MlrRegistryError {
        code,
        source_row_number: row,
        message,
    }
}

pub fn parse_cms_mlr_edition(
    input: &[u8],
    edition: &MlrEdition,
) -> Result<MlrRegistryBatch, MlrRegistryError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(fail("input_limit", None, "MLR edition exceeds byte limit"));
    }
    if edition.snapshot_id.is_empty()
        || edition.snapshot_id.len() > MAX_FIELD_BYTES
        || edition.snapshot_id.trim() != edition.snapshot_id
        || edition.snapshot_id.chars().any(char::is_control)
        || !(2010..=2100).contains(&edition.reporting_year)
        || edition.input_sha256.len() != 64
        || !edition
            .input_sha256
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(fail(
            "invalid_edition",
            None,
            "MLR edition metadata is invalid",
        ));
    }
    let digest: String = Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    if digest != edition.input_sha256 {
        return Err(fail(
            "digest_mismatch",
            None,
            "MLR CSV digest does not match edition",
        ));
    }
    let csv_input = input.strip_prefix(b"\xef\xbb\xbf").unwrap_or(input);
    let mut reader = csv::ReaderBuilder::new()
        .flexible(false)
        .from_reader(csv_input);
    let supplied = reader
        .headers()
        .map_err(|_| fail("invalid_csv", Some(1), "MLR CSV header is malformed"))?
        .clone();
    let header_set: BTreeSet<_> = supplied.iter().collect();
    if supplied.len() != HEADERS.len()
        || header_set.len() != HEADERS.len()
        || HEADERS.iter().any(|header| !header_set.contains(header))
    {
        return Err(fail(
            "invalid_headers",
            Some(1),
            "MLR CSV requires the exact unique header set",
        ));
    }
    let positions: Vec<_> = HEADERS
        .iter()
        .map(|header| {
            supplied
                .iter()
                .position(|candidate| candidate == *header)
                .unwrap()
        })
        .collect();
    let mut observations = Vec::new();
    let mut record = csv::StringRecord::new();
    let mut scanned_bytes = 0;
    let mut physical_line = 1;
    while reader.read_record(&mut record).map_err(|csv_error| {
        fail(
            "invalid_csv",
            csv_error.position().map(|position| {
                physical_source_line(csv_input, position, &mut scanned_bytes, &mut physical_line)
            }),
            "MLR CSV row is malformed",
        )
    })? {
        let source_row = record
            .position()
            .map(|position| {
                physical_source_line(csv_input, position, &mut scanned_bytes, &mut physical_line)
            })
            .unwrap_or(observations.len() + 2);
        if observations.len() >= MAX_ROWS {
            return Err(fail(
                "row_limit",
                Some(source_row),
                "MLR edition exceeds row limit",
            ));
        }
        if record.iter().any(|field| field.len() > MAX_FIELD_BYTES) {
            return Err(fail(
                "field_limit",
                Some(source_row),
                "MLR CSV field exceeds byte limit",
            ));
        }
        let raw_fields = HEADERS
            .iter()
            .zip(&positions)
            .map(|(header, position)| (*header, record[*position].to_owned()))
            .collect();
        observations.push(assertion(source_row, raw_fields));
    }
    if observations.is_empty() {
        return Err(fail(
            "empty_edition",
            None,
            "MLR CSV contains no filing observations",
        ));
    }
    let conflicts = conflict_facts(&mut observations);
    let counts = count_observations(&observations, &conflicts);
    Ok(MlrRegistryBatch {
        edition: edition.clone(),
        observations,
        conflicts,
        counts,
    })
}

// CSV positions may point before a pending LF or blank record separators.
// Scan each byte at most once to retain physical CRLF, LF and CR line starts.
fn physical_source_line(
    input: &[u8],
    position: &csv::Position,
    scanned_bytes: &mut usize,
    physical_line: &mut usize,
) -> usize {
    let mut record_start = position.byte() as usize;
    while matches!(input.get(record_start), Some(b'\r' | b'\n')) {
        record_start += 1;
    }
    while *scanned_bytes < record_start {
        let byte = input[*scanned_bytes];
        *scanned_bytes += 1;
        if byte == b'\n' || (byte == b'\r' && input.get(*scanned_bytes) != Some(&b'\n')) {
            *physical_line += 1;
        }
    }
    *physical_line
}

/// Encode all assertions privately; parsing or output failure returns no COPY bytes.
pub fn encode_cms_mlr_observations(
    input: &[u8],
    edition: &MlrEdition,
) -> Result<EncodedMlrObservations, MlrRegistryError> {
    let snapshot_id = uuid_bytes(&edition.snapshot_id).ok_or_else(|| {
        fail(
            "invalid_snapshot_id",
            None,
            "MLR COPY requires a nonzero hyphenated snapshot UUID",
        )
    })?;
    let batch = parse_cms_mlr_edition(input, edition)?;
    let mut copy_bytes = Vec::with_capacity(input.len().min(64 * 1024));
    copy_bytes.extend_from_slice(COPY_HEADER);
    for observation in &batch.observations {
        encode_observation(observation, &snapshot_id, &mut copy_bytes)?;
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok(EncodedMlrObservations {
        row_count: batch.observations.len(),
        copy_bytes,
        batch,
    })
}

fn jsonb_bytes(assertion: &impl Serialize, source_row: usize) -> Result<Vec<u8>, MlrRegistryError> {
    let mut encoded = vec![1];
    serde_json::to_writer(&mut encoded, assertion).map_err(|_| {
        fail(
            "invalid_jsonb",
            Some(source_row),
            "MLR assertion cannot be encoded as JSONB",
        )
    })?;
    Ok(encoded)
}

fn encode_observation(
    observation: &MlrAssertion,
    snapshot_id: &[u8; 16],
    copy_bytes: &mut Vec<u8>,
) -> Result<(), MlrRegistryError> {
    let source_row = observation.source_row_number;
    if observation
        .raw_fields
        .values()
        .any(|text| text.contains('\0'))
    {
        return Err(fail(
            "invalid_jsonb",
            Some(source_row),
            "MLR assertion contains text unsupported by PostgreSQL JSONB",
        ));
    }
    let source_row_int = i32::try_from(source_row).map_err(|_| {
        fail(
            "invalid_source_row",
            Some(source_row),
            "MLR source row exceeds int32 bounds",
        )
    })?;
    let source_key = format!("row:{source_row}");
    let status: &[u8] = match observation.status {
        RowStatus::Accepted => b"accepted",
        RowStatus::Unresolved => b"unresolved",
        RowStatus::Rejected => b"rejected",
    };
    let observation_json = jsonb_bytes(observation, source_row)?;
    let issues_json = jsonb_bytes(&observation.issues, source_row)?;
    let source_row_bytes = source_row_int.to_be_bytes();
    let fields: [&[u8]; 6] = [
        snapshot_id,
        source_key.as_bytes(),
        &source_row_bytes,
        status,
        &observation_json,
        &issues_json,
    ];
    let encoded_row_bytes = 2 + fields.iter().map(|field| 4 + field.len()).sum::<usize>();
    if copy_bytes.len() + encoded_row_bytes + 2 > MAX_COPY_BYTES {
        return Err(fail(
            "output_limit",
            Some(source_row),
            "MLR COPY exceeds byte limit",
        ));
    }
    copy_bytes.extend_from_slice(&6i16.to_be_bytes());
    for field in fields {
        write_field(field, copy_bytes);
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

fn digits(
    raw: &str,
    width: usize,
    pad: bool,
    field: &'static str,
    required: bool,
    issues: &mut Vec<MlrRowIssue>,
) -> Option<String> {
    let value = raw.trim();
    if value.is_empty() {
        issue(issues, field, "missing_identifier", required);
        return None;
    }
    if !value.bytes().all(|byte| byte.is_ascii_digit())
        || value.len() > width
        || (!pad && value.len() != width)
        || value.bytes().all(|byte| byte == b'0')
    {
        issue(issues, field, "invalid_identifier", true);
        return None;
    }
    Some(if pad {
        format!("{value:0>width$}")
    } else {
        value.to_owned()
    })
}

fn assertion(source_row_number: usize, raw_fields: BTreeMap<&'static str, String>) -> MlrAssertion {
    let mut issues = Vec::new();
    let submission_id = raw_fields["mr_submission_template_id"].trim().to_owned();
    if submission_id.is_empty() {
        issue(
            &mut issues,
            "mr_submission_template_id",
            "missing_submission_id",
            true,
        );
    }
    if raw_fields["company_name"].trim().is_empty() {
        issue(&mut issues, "company_name", "missing_company_name", false);
    }
    let jurisdiction = raw_fields["business_state"].trim();
    let grand_total = jurisdiction.eq_ignore_ascii_case("Grand Total");
    let state = (!grand_total).then(|| jurisdiction.to_ascii_uppercase());
    let state = state.filter(|state| {
        JURISDICTIONS
            .split_ascii_whitespace()
            .any(|valid| valid == state)
    });
    if !grand_total && state.is_none() {
        issue(&mut issues, "business_state", "invalid_jurisdiction", true);
    }
    let ein_digits: String = raw_fields["federal_ein"]
        .chars()
        .filter(|character| !matches!(character, ' ' | '-'))
        .collect();
    // EIN normalization deliberately does not remove tabs or other whitespace.
    let normalized_ein = if ein_digits.bytes().all(|byte| byte.is_ascii_digit())
        && ein_digits.len() == 9
        && ein_digits.bytes().any(|byte| byte != b'0')
    {
        Some(ein_digits)
    } else {
        issue(
            &mut issues,
            "federal_ein",
            if ein_digits.is_empty() {
                "missing_identifier"
            } else {
                "invalid_identifier"
            },
            !ein_digits.is_empty(),
        );
        None
    };
    let normalized_naic_company = digits(
        &raw_fields["naic_company_code"],
        5,
        true,
        "naic_company_code",
        false,
        &mut issues,
    );
    let group_raw = raw_fields["naic_group_code"].trim();
    let normalized_naic_group = if group_raw.is_empty()
        || (group_raw.len() <= 5 && group_raw.bytes().all(|byte| byte == b'0'))
    {
        issue(
            &mut issues,
            "naic_group_code",
            "missing_group_identifier",
            false,
        );
        None
    } else if group_raw.len() <= 5 && group_raw.bytes().all(|byte| byte.is_ascii_digit()) {
        Some(group_raw.trim_start_matches('0').to_owned())
    } else {
        issue(&mut issues, "naic_group_code", "invalid_identifier", true);
        None
    };
    let hios = if grand_total {
        if !raw_fields["hios_issuer_id"].trim().is_empty() {
            issue(
                &mut issues,
                "hios_issuer_id",
                "grand_total_has_issuer",
                true,
            );
        }
        None
    } else {
        digits(
            &raw_fields["hios_issuer_id"],
            5,
            false,
            "hios_issuer_id",
            false,
            &mut issues,
        )
    };
    if normalized_naic_group.is_some() && raw_fields["group_affiliation"].trim().is_empty() {
        issue(
            &mut issues,
            "group_affiliation",
            "missing_group_label",
            false,
        );
    }
    let company_key = normalized_ein
        .as_ref()
        .map(|ein| format!("cms_mlr:ein:{ein}"));
    let group_kind = normalized_naic_group.as_ref().map(|_| "naic_group");
    MlrAssertion {
        source_row_number,
        submission_id,
        row_kind: if grand_total {
            "grand_total"
        } else {
            "issuer_filing"
        },
        state,
        normalized_ein,
        normalized_naic_company,
        normalized_naic_group,
        hios,
        company_key,
        group_kind,
        status: classify(&issues),
        issues,
        raw_fields,
    }
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

type FactIndex = BTreeMap<String, BTreeMap<String, Vec<usize>>>;
fn fact(index: &mut FactIndex, key: Option<&String>, value: Option<&str>, row: usize) {
    if let (Some(key), Some(value)) = (key, value.filter(|value| !value.is_empty())) {
        index
            .entry(key.clone())
            .or_default()
            .entry(value.to_owned())
            .or_default()
            .push(row);
    }
}

fn conflict_facts(observations: &mut [MlrAssertion]) -> Vec<MlrConflict> {
    let mut company_names = FactIndex::new();
    let mut company_naic = FactIndex::new();
    let mut naic_eins = FactIndex::new();
    let mut issuer_identity = FactIndex::new();
    let mut company_groups = FactIndex::new();
    let mut group_labels = FactIndex::new();
    let mut submissions: BTreeMap<String, Vec<usize>> = BTreeMap::new();
    for (index, row) in observations.iter().enumerate() {
        let legal_name = row.raw_fields["company_name"]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        let group_label = row.raw_fields["group_affiliation"]
            .split_whitespace()
            .collect::<Vec<_>>()
            .join(" ");
        fact(
            &mut company_names,
            row.normalized_ein.as_ref(),
            Some(&legal_name),
            index,
        );
        fact(
            &mut company_naic,
            row.normalized_ein.as_ref(),
            row.normalized_naic_company.as_deref(),
            index,
        );
        fact(
            &mut naic_eins,
            row.normalized_naic_company.as_ref(),
            row.normalized_ein.as_deref(),
            index,
        );
        fact(
            &mut company_groups,
            row.normalized_ein.as_ref(),
            row.normalized_naic_group.as_deref(),
            index,
        );
        fact(
            &mut group_labels,
            row.normalized_naic_group.as_ref(),
            Some(&group_label),
            index,
        );
        if let (Some(ein), Some(state)) = (&row.normalized_ein, &row.state) {
            fact(
                &mut issuer_identity,
                row.hios.as_ref(),
                Some(&format!("{ein}:{state}")),
                index,
            );
        }
        if !row.submission_id.is_empty() {
            submissions
                .entry(row.submission_id.clone())
                .or_default()
                .push(index);
        }
    }
    let mut conflicts = Vec::new();
    for (index, code, field) in [
        (
            company_names,
            "conflicting_company_names_for_ein",
            "company_name",
        ),
        (
            company_naic,
            "conflicting_naic_codes_for_ein",
            "naic_company_code",
        ),
        (naic_eins, "conflicting_eins_for_naic_code", "federal_ein"),
        (
            issuer_identity,
            "conflicting_issuer_identity",
            "hios_issuer_id",
        ),
        (
            company_groups,
            "conflicting_group_codes_for_company",
            "naic_group_code",
        ),
        (group_labels, "ambiguous_group_label", "group_affiliation"),
    ] {
        for values in index.values().filter(|values| values.len() > 1) {
            let mut source_rows = Vec::new();
            for row_index in values.values().flatten().copied() {
                issue(&mut observations[row_index].issues, field, code, false);
                observations[row_index].status = classify(&observations[row_index].issues);
                source_rows.push(observations[row_index].source_row_number);
            }
            source_rows.sort_unstable();
            source_rows.dedup();
            conflicts.push(MlrConflict {
                code,
                field,
                source_rows,
                values: values.keys().cloned().collect(),
            });
        }
    }
    for (submission, rows) in submissions.into_iter().filter(|(_, rows)| rows.len() > 1) {
        let mut source_rows = Vec::new();
        for row_index in rows {
            issue(
                &mut observations[row_index].issues,
                "mr_submission_template_id",
                "duplicate_submission_id",
                true,
            );
            observations[row_index].status = RowStatus::Rejected;
            source_rows.push(observations[row_index].source_row_number);
        }
        conflicts.push(MlrConflict {
            code: "duplicate_submission_id",
            field: "mr_submission_template_id",
            source_rows,
            values: vec![submission],
        });
    }
    conflicts
}

fn count_observations(observations: &[MlrAssertion], conflicts: &[MlrConflict]) -> MlrCounts {
    let mut counts = MlrCounts {
        input_rows: observations.len(),
        ambiguous_group_labels: conflicts
            .iter()
            .filter(|conflict| conflict.code == "ambiguous_group_label")
            .count(),
        ..MlrCounts::default()
    };
    let mut eins = BTreeSet::new();
    let mut hios = BTreeSet::new();
    let mut naic = BTreeSet::new();
    let mut groups = BTreeSet::new();
    for row in observations {
        match row.status {
            RowStatus::Accepted => counts.accepted_rows += 1,
            RowStatus::Unresolved => counts.unresolved_rows += 1,
            RowStatus::Rejected => counts.rejected_rows += 1,
        }
        if row.row_kind == "grand_total" {
            counts.grand_total_rows += 1;
        } else {
            counts.issuer_rows += 1;
        }
        counts.nonempty_raw_company_pk_rows +=
            usize::from(!row.raw_fields["company_pk"].trim().is_empty());
        eins.extend(row.normalized_ein.as_ref());
        hios.extend(row.hios.as_ref());
        naic.extend(row.normalized_naic_company.as_ref());
        groups.extend(row.normalized_naic_group.as_ref());
    }
    counts.source_company_anchors = eins.len();
    counts.distinct_hios_issuers = hios.len();
    counts.distinct_naic_company_codes = naic.len();
    counts.distinct_naic_group_codes = groups.len();
    counts
}
