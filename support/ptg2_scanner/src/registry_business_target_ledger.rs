// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Versioned business-context targets; names supply no canonical identity authority.

use crate::registry_target_ledger::{
    self as legacy, failure, OutputCounter, RegistryTargetLedgerError,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

pub const PARSER_VERSION: &str = "registry-target-ledger-v3";
pub const HEADERS: [&str; 6] = [
    "COMPANY_ALIAS",
    "COMPANY_NAME",
    "PLAN_NAME",
    "PLAN_TYPE",
    "PLAN_CARRIER",
    "FIND_CARE_NETWORK_NAME",
];

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessContext {
    pub plan_carrier: String,
    pub network_name: String,
    pub plan_type: String,
}

impl BusinessContext {
    pub(crate) fn from_cells(cells: &[String; 6]) -> Self {
        Self {
            plan_carrier: cells[4].trim().to_owned(),
            network_name: cells[5].trim().to_owned(),
            plan_type: cells[3].trim().to_owned(),
        }
    }
}

fn hex_digest(input: &[u8]) -> String {
    Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

pub(crate) fn target_key(context: &BusinessContext) -> Result<String, RegistryTargetLedgerError> {
    let encoded = serde_json::to_vec(&[
        &context.plan_carrier,
        &context.network_name,
        &context.plan_type,
    ])
    .map_err(|_| failure("output_limit", None))?;
    Ok(format!("business:v1:{}", hex_digest(&encoded)))
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BusinessTarget {
    pub target_key: String,
    pub business_context: BusinessContext,
    pub source_row_ordinals: Vec<usize>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BusinessObservation {
    pub source_row_ordinal: usize,
    pub raw_cells: [String; 6],
    pub target_keys: Vec<String>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct BusinessLedger {
    pub source_sha256: String,
    pub row_count: usize,
    pub observations: Vec<BusinessObservation>,
    pub targets: Vec<BusinessTarget>,
}

pub fn parse_business_target_ledger(
    input: &[u8],
) -> Result<BusinessLedger, RegistryTargetLedgerError> {
    if input.len() > legacy::MAX_INPUT_BYTES {
        return Err(failure("input_limit", None));
    }
    std::str::from_utf8(input).map_err(|_| failure("invalid_utf8", None))?;
    if input.starts_with(b"\xef\xbb\xbf") {
        return Err(failure("invalid_headers", None));
    }
    let expected_rows = legacy::strict_record_count(input, HEADERS.len())?;
    if expected_rows == 0 {
        return Err(failure("empty_ledger", None));
    }
    let mut reader = csv::ReaderBuilder::new()
        .flexible(false)
        .trim(csv::Trim::None)
        .from_reader(input);
    if reader
        .headers()
        .map_err(|_| failure("invalid_headers", None))?
        .iter()
        .ne(HEADERS.iter().copied())
    {
        return Err(failure("invalid_headers", None));
    }
    let mut targets = BTreeMap::<String, BusinessTarget>::new();
    let mut observations = Vec::new();
    for (index, record) in reader.records().enumerate() {
        let row = index + 1;
        let record = record.map_err(|_| failure("invalid_csv", Some(row)))?;
        if record.len() != HEADERS.len() {
            return Err(failure("invalid_csv", Some(row)));
        }
        let raw_cells: [String; 6] = std::array::from_fn(|i| record[i].to_owned());
        if raw_cells
            .iter()
            .any(|cell| cell.len() > legacy::MAX_FIELD_BYTES)
        {
            return Err(failure("field_limit", Some(row)));
        }
        if raw_cells.iter().any(|cell| cell.contains('\0')) {
            return Err(failure("invalid_jsonb", Some(row)));
        }
        let context = BusinessContext::from_cells(&raw_cells);
        let key = target_key(&context)?;
        let target = targets
            .entry(key.clone())
            .or_insert_with(|| BusinessTarget {
                target_key: key.clone(),
                business_context: context.clone(),
                source_row_ordinals: Vec::new(),
            });
        if target.business_context != context {
            return Err(failure("target_conflict", Some(row)));
        }
        target.source_row_ordinals.push(row);
        if targets.len() > legacy::MAX_TARGETS {
            return Err(failure("target_limit", Some(row)));
        }
        observations.push(BusinessObservation {
            source_row_ordinal: row,
            raw_cells,
            target_keys: vec![key],
        });
    }
    if observations.len() != expected_rows {
        return Err(failure("invalid_csv", None));
    }
    let ledger = BusinessLedger {
        source_sha256: hex_digest(input),
        row_count: observations.len(),
        observations,
        targets: targets.into_values().collect(),
    };
    serde_json::to_writer(OutputCounter(0), &ledger).map_err(|_| failure("output_limit", None))?;
    Ok(ledger)
}

pub fn encode_business_target_ledger_artifact(
    input: &[u8],
    snapshot_id: &str,
) -> Result<(Vec<u8>, Vec<u8>), RegistryTargetLedgerError> {
    let ledger = parse_business_target_ledger(input)?;
    let document = serde_json::json!({"component":legacy::COMPONENT,"revision":1,"parser_version":PARSER_VERSION,"ledger":ledger});
    legacy::encode_document(document, snapshot_id, PARSER_VERSION, b"ledger:v3")
}
