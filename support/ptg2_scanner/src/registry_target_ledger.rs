// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded source target evidence without company or canonical network binding.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, Write};

pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_ROWS: usize = 50_000;
pub const MAX_TARGETS: usize = 5_000;
pub const MAX_FIELD_BYTES: usize = 4_096;
pub const MAX_FC_BYTES: usize = 128;
pub const MAX_RIBBON_BYTES: usize = 65_536;
pub const MAX_RIBBON_IDS: usize = 100;
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_COPY_BYTES: usize = MAX_OUTPUT_BYTES + 1_024;
pub const PARSER_VERSION: &str = "registry-target-ledger-v1";
pub const COMPONENT: &str = "registry_required_target_ledger";
pub const HEADERS: [&str; 8] = [
    "COMPANY_ALIAS",
    "COMPANY_NAME",
    "PLAN_NAME",
    "PLAN_TYPE",
    "PLAN_CARRIER",
    "FIND_CARE_NETWORK_NAME",
    "NETWORK_FC_ID",
    "RIBBON_IDS",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RegistryTargetLedgerObservation {
    pub source_row_ordinal: usize,
    pub raw_cells: [String; 8],
    pub target_keys: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RegistryTargetLedgerTarget {
    pub target_key: String,
    pub fc_network_id: Option<String>,
    pub ribbon_id: Option<String>,
    pub source_row_ordinals: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RegistryTargetLedger {
    pub source_sha256: String,
    pub row_count: usize,
    pub observations: Vec<RegistryTargetLedgerObservation>,
    pub targets: Vec<RegistryTargetLedgerTarget>,
}

#[derive(Serialize)]
struct RegistryTargetLedgerDocument {
    component: &'static str,
    revision: u8,
    parser_version: &'static str,
    ledger: RegistryTargetLedger,
}

#[derive(Serialize)]
struct RegistryTargetLedgerArtifactDescriptor<'a> {
    component: &'static str,
    revision: u8,
    parser_version: &'static str,
    snapshot_id: &'a str,
    source_sha256: String,
    artifact_sha256: String,
    source_rows: usize,
    target_count: usize,
    physical_records: usize,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryTargetLedgerError {
    pub code: &'static str,
    pub source_row_ordinal: Option<usize>,
}

impl fmt::Display for RegistryTargetLedgerError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Target ledger rejected: {}", self.code)
    }
}

impl std::error::Error for RegistryTargetLedgerError {}

fn failure(code: &'static str, row: Option<usize>) -> RegistryTargetLedgerError {
    RegistryTargetLedgerError {
        code,
        source_row_ordinal: row,
    }
}

#[derive(Clone, Copy)]
enum CsvState {
    FieldStart,
    Unquoted,
    Quoted,
    AfterQuote,
}

fn check_record(columns: usize, completed: usize) -> Result<(), RegistryTargetLedgerError> {
    let row = (completed > 0).then_some(completed);
    if columns != HEADERS.len() {
        return Err(failure(
            if row.is_some() {
                "invalid_csv"
            } else {
                "invalid_headers"
            },
            row,
        ));
    }
    if completed > MAX_ROWS {
        return Err(failure("row_limit", row));
    }
    Ok(())
}

/// The csv decoder permits unterminated quotes, trailing quote text and blank records.
/// Check those concrete gaps before decoding so no source record is silently lost.
fn strict_record_count(input: &[u8]) -> Result<usize, RegistryTargetLedgerError> {
    let mut state = CsvState::FieldStart;
    let mut columns = 1;
    let mut completed = 0;
    let mut started = false;
    let mut index = 0;
    while index < input.len() {
        let byte = input[index];
        let row = (completed > 0).then_some(completed);
        match (state, byte) {
            (CsvState::Quoted, b'"') => state = CsvState::AfterQuote,
            (CsvState::Quoted, _) => {}
            (CsvState::AfterQuote, b'"') => state = CsvState::Quoted,
            (CsvState::FieldStart, b'"') => {
                state = CsvState::Quoted;
                started = true;
            }
            (CsvState::Unquoted, b'"') => return Err(failure("invalid_csv", row)),
            (_, b',') => {
                columns += 1;
                state = CsvState::FieldStart;
                started = true;
            }
            (_, b'\r' | b'\n') => {
                check_record(columns, completed)?;
                completed += 1;
                columns = 1;
                state = CsvState::FieldStart;
                started = false;
                if byte == b'\r' && input.get(index + 1) == Some(&b'\n') {
                    index += 1;
                }
            }
            (CsvState::AfterQuote, _) => return Err(failure("invalid_csv", row)),
            _ => {
                state = CsvState::Unquoted;
                started = true;
            }
        }
        index += 1;
    }
    finish_record_count(state, started, columns, completed)
}

fn finish_record_count(
    state: CsvState,
    started: bool,
    columns: usize,
    mut completed: usize,
) -> Result<usize, RegistryTargetLedgerError> {
    if matches!(state, CsvState::Quoted) {
        return Err(failure("invalid_csv", (completed > 0).then_some(completed)));
    }
    if started {
        check_record(columns, completed)?;
        completed += 1;
    }
    completed
        .checked_sub(1)
        .ok_or_else(|| failure("invalid_headers", None))
}

fn source_ids(
    cells: &[String; 8],
    row: usize,
) -> Result<(Option<String>, BTreeSet<String>), RegistryTargetLedgerError> {
    for (index, cell) in cells.iter().enumerate() {
        let limit = if index == 7 {
            MAX_RIBBON_BYTES
        } else {
            MAX_FIELD_BYTES
        };
        if cell.len() > limit {
            return Err(failure("field_limit", Some(row)));
        }
    }
    let fc = &cells[6];
    if !fc.is_empty()
        && (fc.len() > MAX_FC_BYTES
            || fc.starts_with('0')
            || !fc.bytes().all(|byte| byte.is_ascii_digit()))
    {
        return Err(failure("invalid_fc_id", Some(row)));
    }
    let ribbons: Vec<String> = if cells[7].is_empty() {
        Vec::new()
    } else {
        serde_json::from_str(&cells[7]).map_err(|_| failure("invalid_ribbon_json", Some(row)))?
    };
    if ribbons.len() > MAX_RIBBON_IDS {
        return Err(failure("ribbon_id_limit", Some(row)));
    }
    if ribbons
        .iter()
        .any(|id| uuid_bytes(id).is_none() || id.bytes().any(|byte| byte.is_ascii_uppercase()))
    {
        return Err(failure("invalid_ribbon_id", Some(row)));
    }
    Ok((
        (!fc.is_empty()).then(|| fc.clone()),
        ribbons.into_iter().collect(),
    ))
}

fn row_targets(
    fc: Option<&str>,
    ribbons: &BTreeSet<String>,
    digest: &str,
    row: usize,
) -> Vec<(String, Option<String>)> {
    if ribbons.is_empty() {
        let key = fc.map_or_else(
            || format!("csv:{digest}:row:{row}"),
            |id| format!("fc:{id}:ribbon:missing"),
        );
        return vec![(key, None)];
    }
    ribbons
        .iter()
        .map(|ribbon| {
            let key = fc.map_or_else(
                || format!("ribbon:{ribbon}:fc:missing"),
                |id| format!("fc:{id}:ribbon:{ribbon}"),
            );
            (key, Some(ribbon.clone()))
        })
        .collect()
}

struct OutputCounter(usize);

impl Write for OutputCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .checked_add(bytes.len())
            .filter(|size| *size <= MAX_OUTPUT_BYTES)
            .ok_or_else(|| io::Error::other("target ledger output limit"))?;
        self.0 = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn decode_observation(
    record: csv::StringRecord,
    row: usize,
    digest: &str,
    targets: &mut BTreeMap<String, RegistryTargetLedgerTarget>,
) -> Result<RegistryTargetLedgerObservation, RegistryTargetLedgerError> {
    if record.len() != HEADERS.len() {
        return Err(failure("invalid_csv", Some(row)));
    }
    let raw_cells = std::array::from_fn(|index| record[index].to_owned());
    let (fc, ribbons) = source_ids(&raw_cells, row)?;
    let mut target_keys = Vec::new();
    for (target_key, ribbon_id) in row_targets(fc.as_deref(), &ribbons, digest, row) {
        let target =
            targets
                .entry(target_key.clone())
                .or_insert_with(|| RegistryTargetLedgerTarget {
                    target_key: target_key.clone(),
                    fc_network_id: fc.clone(),
                    ribbon_id,
                    source_row_ordinals: Vec::new(),
                });
        target.source_row_ordinals.push(row);
        target_keys.push(target_key);
    }
    if targets.len() > MAX_TARGETS {
        return Err(failure("target_limit", Some(row)));
    }
    target_keys.sort();
    Ok(RegistryTargetLedgerObservation {
        source_row_ordinal: row,
        raw_cells,
        target_keys,
    })
}

/// Decode one complete source ledger atomically, retaining raw cells and all source relations.
pub fn parse_registry_target_ledger(
    input: &[u8],
) -> Result<RegistryTargetLedger, RegistryTargetLedgerError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None));
    }
    std::str::from_utf8(input).map_err(|_| failure("invalid_utf8", None))?;
    if input.starts_with(b"\xef\xbb\xbf") {
        return Err(failure("invalid_headers", None));
    }
    let expected_rows = strict_record_count(input)?;
    if expected_rows == 0 {
        return Err(failure("empty_ledger", None));
    }
    let source_sha256 = Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    let mut reader = csv::ReaderBuilder::new()
        .flexible(false)
        .trim(csv::Trim::None)
        .from_reader(input);
    let headers = reader
        .headers()
        .map_err(|_| failure("invalid_headers", None))?;
    if headers.iter().ne(HEADERS.iter().copied()) {
        return Err(failure("invalid_headers", None));
    }
    let mut targets = BTreeMap::<String, RegistryTargetLedgerTarget>::new();
    let observations = reader
        .records()
        .enumerate()
        .map(|(index, record)| {
            let row = index + 1;
            let record = record.map_err(|_| failure("invalid_csv", Some(row)))?;
            decode_observation(record, row, &source_sha256, &mut targets)
        })
        .collect::<Result<Vec<_>, _>>()?;
    if observations.len() != expected_rows {
        return Err(failure("invalid_csv", None));
    }
    let ledger = RegistryTargetLedger {
        source_sha256,
        row_count: observations.len(),
        observations,
        targets: targets.into_values().collect(),
    };
    serde_json::to_writer(OutputCounter(0), &ledger).map_err(|_| failure("output_limit", None))?;
    Ok(ledger)
}

/// Retain the complete logical ledger in one bounded native observation record.
pub fn encode_registry_target_ledger_artifact(
    input: &[u8],
    snapshot_id: &str,
) -> Result<(Vec<u8>, Vec<u8>), RegistryTargetLedgerError> {
    let snapshot = uuid_bytes(snapshot_id)
        .filter(|_| !snapshot_id.bytes().any(|byte| byte.is_ascii_uppercase()))
        .ok_or_else(|| failure("invalid_snapshot_id", None))?;
    let ledger = parse_registry_target_ledger(input)?;
    if let Some(observation) = ledger
        .observations
        .iter()
        .find(|row| row.raw_cells.iter().any(|cell| cell.contains('\0')))
    {
        return Err(failure(
            "invalid_jsonb",
            Some(observation.source_row_ordinal),
        ));
    }
    let mut descriptor = RegistryTargetLedgerArtifactDescriptor {
        component: COMPONENT,
        revision: 1,
        parser_version: PARSER_VERSION,
        snapshot_id,
        source_sha256: ledger.source_sha256.clone(),
        artifact_sha256: String::new(),
        source_rows: ledger.row_count,
        target_count: ledger.targets.len(),
        physical_records: 1,
    };
    let document = serde_json::to_value(RegistryTargetLedgerDocument {
        component: COMPONENT,
        revision: 1,
        parser_version: PARSER_VERSION,
        ledger,
    })
    .map_err(|_| failure("output_limit", None))?;
    serde_json::to_writer(OutputCounter(0), &document)
        .map_err(|_| failure("output_limit", None))?;
    let artifact = serde_json::to_vec(&document).map_err(|_| failure("output_limit", None))?;
    descriptor.artifact_sha256 = Sha256::digest(&artifact)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    let descriptor = serde_json::to_value(descriptor).map_err(|_| failure("output_limit", None))?;
    let descriptor_bytes =
        serde_json::to_vec(&descriptor).map_err(|_| failure("output_limit", None))?;

    let mut observation_json = vec![1];
    observation_json.extend_from_slice(&artifact);
    let fields: [&[u8]; 6] = [
        &snapshot,
        b"ledger:v1",
        &1i32.to_be_bytes(),
        b"accepted",
        &observation_json,
        b"\x01[]",
    ];
    let copy_size =
        COPY_HEADER.len() + 2 + fields.iter().map(|field| 4 + field.len()).sum::<usize>() + 2;
    if copy_size > MAX_COPY_BYTES {
        return Err(failure("output_limit", None));
    }
    let mut copy_bytes = Vec::with_capacity(copy_size);
    copy_bytes.extend_from_slice(COPY_HEADER);
    copy_bytes.extend_from_slice(&6i16.to_be_bytes());
    for field in fields {
        write_field(field, &mut copy_bytes);
    }
    copy_bytes.extend_from_slice(&(-1i16).to_be_bytes());
    Ok((copy_bytes, descriptor_bytes))
}
