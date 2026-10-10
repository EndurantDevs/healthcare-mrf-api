// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded review evidence for an exact retained required-target ledger.

use crate::network_membership_codec::{uuid_bytes, write_field, COPY_HEADER};
use crate::network_source_binding_values::{validate_row, ValidatedSourceBinding};
use crate::registry_network_coverage::MAX_TARGET_KEY_BYTES;
use crate::registry_target_ledger;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::io::{self, Write};

pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_LEDGER_INPUT_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_DOCUMENT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_COPY_BYTES: usize = MAX_DOCUMENT_BYTES + 1_024;
pub const MAX_DECISIONS: usize = 5_000;
pub const MAX_BUNDLE_BYTES: usize = 64 * 1024 * 1024;
pub const COMPONENT: &str = "registry_required_target_review";
pub const PARSER_VERSION: &str = "registry-target-review-v1";

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryRequiredTargetReviewError {
    pub code: &'static str,
}

impl fmt::Display for RegistryRequiredTargetReviewError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        output.write_str("Required target review is invalid or exceeds its bounds")
    }
}

impl std::error::Error for RegistryRequiredTargetReviewError {}

fn failure(code: &'static str) -> RegistryRequiredTargetReviewError {
    RegistryRequiredTargetReviewError { code }
}

fn required_option<'de, D, T>(decoder: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(decoder)
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerDocument {
    component: String,
    revision: u8,
    parser_version: String,
    ledger: Ledger,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Ledger {
    source_sha256: String,
    row_count: usize,
    observations: Vec<LedgerObservation>,
    targets: Vec<LedgerTarget>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerObservation {
    source_row_ordinal: usize,
    raw_cells: [String; 8],
    target_keys: Vec<String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LedgerTarget {
    target_key: String,
    #[serde(deserialize_with = "required_option")]
    fc_network_id: Option<String>,
    #[serde(deserialize_with = "required_option")]
    ribbon_id: Option<String>,
    source_row_ordinals: Vec<usize>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewInput {
    ledger_snapshot_id: String,
    ledger_artifact_sha256: String,
    decisions: Vec<DecisionInput>,
}

#[derive(Clone, Copy, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum ResolutionStatus {
    Resolved,
    Unresolved,
    Conflicting,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DecisionInput {
    target_key: String,
    resolution_status: ResolutionStatus,
    #[serde(deserialize_with = "required_option")]
    network_id: Option<i32>,
    #[serde(deserialize_with = "required_option")]
    source_binding: Option<BindingInput>,
    evidence_reference: String,
    evidence_sha256: String,
    reason: String,
}

#[derive(Deserialize, Serialize)]
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
}

#[derive(Serialize)]
struct BindingValidation<'a> {
    #[serde(flatten)]
    projection: &'a BindingInput,
    network_id: i32,
    evidence_id: &'static str,
    evidence_sha256: &'static str,
    operation: &'static str,
    expected_revision: i64,
    expected_network_id: Option<i32>,
}

#[derive(Clone, Eq, PartialEq, Serialize)]
struct BindingProjection {
    binding_id: String,
    source_system: String,
    source_id: String,
    dataset_schema: String,
    dataset_id: String,
    producer_id: String,
    edition_id: String,
    source_key: String,
    source_scope_json: Value,
    binding_key: String,
}

impl From<ValidatedSourceBinding> for BindingProjection {
    fn from(row: ValidatedSourceBinding) -> Self {
        Self {
            binding_id: row.binding_id,
            source_system: row.source_system,
            source_id: row.source_id,
            dataset_schema: row.dataset_schema,
            dataset_id: row.dataset_id,
            producer_id: row.producer_id,
            edition_id: row.edition_id,
            source_key: row.source_key,
            source_scope_json: row.source_scope_json,
            binding_key: row.binding_key,
        }
    }
}

#[derive(Serialize)]
struct Decision {
    target_key: String,
    resolution_status: ResolutionStatus,
    network_id: Option<i32>,
    source_binding: Option<BindingProjection>,
    evidence_reference: String,
    evidence_sha256: String,
    reason: String,
}

#[derive(Serialize)]
struct ReviewDocument {
    component: &'static str,
    revision: u8,
    parser_version: &'static str,
    source_sha256: String,
    ledger_snapshot_id: String,
    ledger_artifact_sha256: String,
    ledger_source_sha256: String,
    decisions: Vec<Decision>,
}

#[derive(Serialize)]
struct Descriptor<'a> {
    component: &'static str,
    revision: u8,
    parser_version: &'static str,
    snapshot_id: &'a str,
    source_sha256: String,
    artifact_sha256: String,
    ledger_snapshot_id: String,
    ledger_artifact_sha256: String,
    decision_count: usize,
    resolved_count: usize,
    physical_records: usize,
}

fn canonical_uuid(value: &str) -> bool {
    uuid_bytes(value).is_some() && !value.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn digest(value: &[u8]) -> String {
    Sha256::digest(value)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn printable(value: &str, maximum: usize) -> bool {
    !value.is_empty() && value.len() <= maximum && !value.chars().any(char::is_control)
}

struct OutputCounter(usize);

impl Write for OutputCounter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len())
            .filter(|length| *length <= MAX_DOCUMENT_BYTES)
            .ok_or_else(|| io::Error::other("review document limit"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn canonical<T: Serialize>(value: T) -> Result<Vec<u8>, RegistryRequiredTargetReviewError> {
    let value =
        serde_json::to_value(value).map_err(|_| failure("registry_target_review_invalid"))?;
    serde_json::to_writer(OutputCounter(0), &value)
        .map_err(|_| failure("registry_target_review_limit"))?;
    serde_json::to_vec(&value).map_err(|_| failure("registry_target_review_invalid"))
}

fn ledger_targets(
    ledger: &LedgerDocument,
) -> Result<BTreeSet<&str>, RegistryRequiredTargetReviewError> {
    let invalid = || failure("registry_target_review_ledger_invalid");
    if ledger.component != registry_target_ledger::COMPONENT
        || ledger.revision != 1
        || ledger.parser_version != registry_target_ledger::PARSER_VERSION
        || !valid_digest(&ledger.ledger.source_sha256)
        || !(1..=registry_target_ledger::MAX_ROWS).contains(&ledger.ledger.row_count)
        || ledger.ledger.observations.len() != ledger.ledger.row_count
        || ledger.ledger.targets.is_empty()
        || ledger.ledger.targets.len() > registry_target_ledger::MAX_TARGETS
    {
        return Err(invalid());
    }
    let mut keys = BTreeSet::new();
    let mut relations = BTreeMap::<&str, Vec<usize>>::new();
    for target in &ledger.ledger.targets {
        if target.target_key.is_empty()
            || target.target_key.len() > MAX_TARGET_KEY_BYTES
            || !keys.insert(target.target_key.as_str())
            || target.source_row_ordinals.is_empty()
            || target
                .source_row_ordinals
                .windows(2)
                .any(|rows| rows[0] >= rows[1])
            || target
                .source_row_ordinals
                .iter()
                .any(|row| !(1..=ledger.ledger.row_count).contains(row))
            || target.fc_network_id.as_ref().is_some_and(|value| {
                value.len() > registry_target_ledger::MAX_FC_BYTES
                    || value.is_empty()
                    || value.starts_with('0')
                    || !value.bytes().all(|byte| byte.is_ascii_digit())
            })
            || target
                .ribbon_id
                .as_ref()
                .is_some_and(|value| !canonical_uuid(value))
        {
            return Err(invalid());
        }
        relations.insert(&target.target_key, Vec::new());
    }
    for (index, observation) in ledger.ledger.observations.iter().enumerate() {
        if observation.source_row_ordinal != index + 1
            || observation.target_keys.is_empty()
            || observation.target_keys.len() > registry_target_ledger::MAX_RIBBON_IDS
            || observation
                .target_keys
                .windows(2)
                .any(|keys| keys[0] >= keys[1])
            || observation
                .raw_cells
                .iter()
                .enumerate()
                .any(|(field, cell)| {
                    cell.contains('\0')
                        || cell.len()
                            > if field == 7 {
                                registry_target_ledger::MAX_RIBBON_BYTES
                            } else {
                                registry_target_ledger::MAX_FIELD_BYTES
                            }
                })
        {
            return Err(invalid());
        }
        for key in &observation.target_keys {
            relations
                .get_mut(key.as_str())
                .ok_or_else(invalid)?
                .push(index + 1);
        }
    }
    for target in &ledger.ledger.targets {
        if relations[&target.target_key.as_str()] != target.source_row_ordinals {
            return Err(invalid());
        }
    }
    Ok(keys)
}

fn source_binding(
    input: &BindingInput,
    network_id: i32,
    index: usize,
) -> Result<BindingProjection, RegistryRequiredTargetReviewError> {
    let validation = BindingValidation {
        projection: input,
        network_id,
        evidence_id: "validation-only",
        evidence_sha256: "0000000000000000000000000000000000000000000000000000000000000000",
        operation: "bind",
        expected_revision: 0,
        expected_network_id: None,
    };
    let raw = serde_json::value::to_raw_value(&validation)
        .map_err(|_| failure("registry_target_review_binding_invalid"))?;
    validate_row(&raw, index)
        .map(BindingProjection::from)
        .map_err(|_| failure("registry_target_review_binding_invalid"))
}

fn validate_decisions(
    rows: Vec<DecisionInput>,
    targets: &BTreeSet<&str>,
) -> Result<(Vec<Decision>, usize), RegistryRequiredTargetReviewError> {
    if rows.is_empty() || rows.len() > MAX_DECISIONS {
        return Err(failure("registry_target_review_limit"));
    }
    let mut seen = BTreeSet::new();
    let mut bindings = BTreeMap::<String, (BindingProjection, i32)>::new();
    let mut decisions = Vec::with_capacity(rows.len());
    let mut resolved_count = 0;
    for (index, row) in rows.into_iter().enumerate() {
        if row.target_key.len() > MAX_TARGET_KEY_BYTES
            || !targets.contains(row.target_key.as_str())
            || !seen.insert(row.target_key.clone())
            || !printable(&row.evidence_reference, 512)
            || !valid_digest(&row.evidence_sha256)
            || !printable(&row.reason, 1000)
        {
            return Err(failure("registry_target_review_invalid"));
        }
        let binding = match (row.resolution_status, row.network_id, row.source_binding) {
            (ResolutionStatus::Resolved, Some(network), Some(input)) if network > 0 => {
                let projection = source_binding(&input, network, index)?;
                if let Some((previous, previous_network)) = bindings.get(&projection.binding_id) {
                    if previous != &projection || *previous_network != network {
                        return Err(failure("registry_target_review_binding_conflict"));
                    }
                } else {
                    bindings.insert(projection.binding_id.clone(), (projection.clone(), network));
                }
                resolved_count += 1;
                Some(projection)
            }
            (ResolutionStatus::Unresolved | ResolutionStatus::Conflicting, None, None) => None,
            _ => return Err(failure("registry_target_review_invalid")),
        };
        decisions.push(Decision {
            target_key: row.target_key,
            resolution_status: row.resolution_status,
            network_id: row.network_id,
            source_binding: binding,
            evidence_reference: row.evidence_reference,
            evidence_sha256: row.evidence_sha256,
            reason: row.reason,
        });
    }
    Ok((decisions, resolved_count))
}

/// Validate the complete review before exposing one retained observation artifact.
pub fn encode_registry_required_target_review_artifact(
    input: &[u8],
    ledger_document: &[u8],
    snapshot_id: &str,
) -> Result<(Vec<u8>, Vec<u8>), RegistryRequiredTargetReviewError> {
    if input.len() > MAX_INPUT_BYTES || ledger_document.len() > MAX_LEDGER_INPUT_BYTES {
        return Err(failure("registry_target_review_limit"));
    }
    if !canonical_uuid(snapshot_id) {
        return Err(failure("registry_target_review_invalid"));
    }
    let input_decoded: ReviewInput =
        serde_json::from_slice(input).map_err(|_| failure("registry_target_review_invalid"))?;
    if !canonical_uuid(&input_decoded.ledger_snapshot_id)
        || !valid_digest(&input_decoded.ledger_artifact_sha256)
        || input_decoded.decisions.len() > MAX_DECISIONS
    {
        return Err(failure("registry_target_review_invalid"));
    }
    let ledger: LedgerDocument = serde_json::from_slice(ledger_document)
        .map_err(|_| failure("registry_target_review_ledger_invalid"))?;
    let targets = ledger_targets(&ledger)?;
    let ledger_digest = digest(&canonical(&ledger)?);
    if ledger_digest != input_decoded.ledger_artifact_sha256 {
        return Err(failure("registry_target_review_ledger_mismatch"));
    }
    let (decisions, resolved_count) = validate_decisions(input_decoded.decisions, &targets)?;
    let decision_count = decisions.len();
    let source_sha256 = digest(input);
    let document = canonical(ReviewDocument {
        component: COMPONENT,
        revision: 1,
        parser_version: PARSER_VERSION,
        source_sha256: source_sha256.clone(),
        ledger_snapshot_id: input_decoded.ledger_snapshot_id.clone(),
        ledger_artifact_sha256: ledger_digest.clone(),
        ledger_source_sha256: ledger.ledger.source_sha256,
        decisions,
    })?;
    let descriptor = canonical(Descriptor {
        component: COMPONENT,
        revision: 1,
        parser_version: PARSER_VERSION,
        snapshot_id,
        source_sha256,
        artifact_sha256: digest(&document),
        ledger_snapshot_id: input_decoded.ledger_snapshot_id,
        ledger_artifact_sha256: ledger_digest,
        decision_count,
        resolved_count,
        physical_records: 1,
    })?;
    let mut jsonb = vec![1];
    jsonb.extend_from_slice(&document);
    let snapshot = uuid_bytes(snapshot_id).expect("validated review UUID");
    let fields: [&[u8]; 6] = [
        &snapshot,
        b"review:v1",
        &1i32.to_be_bytes(),
        b"accepted",
        &jsonb,
        b"\x01[]",
    ];
    let size =
        COPY_HEADER.len() + 2 + fields.iter().map(|field| 4 + field.len()).sum::<usize>() + 2;
    if size > MAX_COPY_BYTES {
        return Err(failure("registry_target_review_limit"));
    }
    let mut copy = Vec::with_capacity(size);
    copy.extend_from_slice(COPY_HEADER);
    copy.extend_from_slice(&6i16.to_be_bytes());
    for field in fields {
        write_field(field, &mut copy);
    }
    copy.extend_from_slice(&(-1i16).to_be_bytes());
    Ok((copy, descriptor))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ArtifactBundle {
    reviews: Vec<RetainedArtifact>,
    ledgers: Vec<RetainedArtifact>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RetainedArtifact {
    snapshot_id: String,
    artifact_sha256: String,
    document: Box<RawValue>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredReview {
    component: String,
    revision: u8,
    parser_version: String,
    source_sha256: String,
    ledger_snapshot_id: String,
    ledger_artifact_sha256: String,
    ledger_source_sha256: String,
    decisions: Vec<StoredDecision>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StoredDecision {
    target_key: String,
    resolution_status: ResolutionStatus,
    #[serde(deserialize_with = "required_option")]
    network_id: Option<i32>,
    #[serde(deserialize_with = "required_option")]
    source_binding: Option<Box<RawValue>>,
    evidence_reference: String,
    evidence_sha256: String,
    reason: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredBinding {
    binding_id: String,
    source_system: String,
    source_id: String,
    dataset_schema: String,
    dataset_id: String,
    producer_id: String,
    edition_id: String,
    source_key: String,
    source_scope_json: Box<RawValue>,
    binding_key: String,
}

fn stored_decision(
    row: StoredDecision,
) -> Result<DecisionInput, RegistryRequiredTargetReviewError> {
    let binding = row
        .source_binding
        .map(|raw| {
            let saved: StoredBinding = serde_json::from_str(raw.get())
                .map_err(|_| failure("registry_target_review_binding_invalid"))?;
            if !valid_digest(&saved.binding_key) {
                return Err(failure("registry_target_review_binding_invalid"));
            }
            Ok(BindingInput {
                binding_id: saved.binding_id,
                source_system: saved.source_system,
                source_id: saved.source_id,
                dataset_schema: saved.dataset_schema,
                dataset_id: saved.dataset_id,
                producer_id: saved.producer_id,
                edition_id: saved.edition_id,
                source_key: saved.source_key,
                source_scope_json: saved.source_scope_json,
            })
        })
        .transpose()?;
    Ok(DecisionInput {
        target_key: row.target_key,
        resolution_status: row.resolution_status,
        network_id: row.network_id,
        source_binding: binding,
        evidence_reference: row.evidence_reference,
        evidence_sha256: row.evidence_sha256,
        reason: row.reason,
    })
}

/// Verify one retained ledger independently of review or approval state.
pub fn validate_registry_required_target_ledger_artifact(
    input: &[u8],
) -> Result<Vec<u8>, RegistryRequiredTargetReviewError> {
    if input.len() > MAX_LEDGER_INPUT_BYTES {
        return Err(failure("registry_target_review_limit"));
    }
    let artifact: RetainedArtifact = serde_json::from_slice(input)
        .map_err(|_| failure("registry_target_review_ledger_invalid"))?;
    if !canonical_uuid(&artifact.snapshot_id) || !valid_digest(&artifact.artifact_sha256) {
        return Err(failure("registry_target_review_ledger_invalid"));
    }
    let ledger: LedgerDocument = serde_json::from_str(artifact.document.get())
        .map_err(|_| failure("registry_target_review_ledger_invalid"))?;
    let target_count = ledger_targets(&ledger)?.len();
    if digest(&canonical(&ledger)?) != artifact.artifact_sha256 {
        return Err(failure("registry_target_review_ledger_mismatch"));
    }
    canonical(serde_json::json!({
        "component": "registry_required_target_ledger_validation", "revision": 1,
        "snapshot_id": artifact.snapshot_id, "artifact_sha256": artifact.artifact_sha256,
        "source_sha256": ledger.ledger.source_sha256, "source_rows": ledger.ledger.row_count,
        "target_count": target_count,
    }))
}

/// Recheck retained body hashes and reviewed source coordinates in one native pass.
pub fn validate_registry_required_target_review_artifacts(
    input: &[u8],
) -> Result<Vec<u8>, RegistryRequiredTargetReviewError> {
    if input.len() > MAX_BUNDLE_BYTES {
        return Err(failure("registry_target_review_limit"));
    }
    let bundle: ArtifactBundle =
        serde_json::from_slice(input).map_err(|_| failure("registry_target_review_invalid"))?;
    if bundle.reviews.len() > MAX_DECISIONS || bundle.ledgers.len() > MAX_DECISIONS {
        return Err(failure("registry_target_review_limit"));
    }
    let review_count = bundle.reviews.len();
    let ledger_count = bundle.ledgers.len();
    let mut identities = BTreeSet::new();
    let mut ledgers = BTreeMap::<String, (String, String, BTreeSet<String>)>::new();
    for artifact in bundle.ledgers {
        if !canonical_uuid(&artifact.snapshot_id)
            || !valid_digest(&artifact.artifact_sha256)
            || !identities.insert(artifact.snapshot_id.clone())
        {
            return Err(failure("registry_target_review_ledger_invalid"));
        }
        let ledger: LedgerDocument = serde_json::from_str(artifact.document.get())
            .map_err(|_| failure("registry_target_review_ledger_invalid"))?;
        let targets = ledger_targets(&ledger)?
            .into_iter()
            .map(str::to_owned)
            .collect();
        if digest(&canonical(&ledger)?) != artifact.artifact_sha256 {
            return Err(failure("registry_target_review_ledger_mismatch"));
        }
        ledgers.insert(
            artifact.snapshot_id,
            (
                artifact.artifact_sha256,
                ledger.ledger.source_sha256,
                targets,
            ),
        );
    }
    let mut referenced = BTreeSet::new();
    for artifact in bundle.reviews {
        if !canonical_uuid(&artifact.snapshot_id)
            || !valid_digest(&artifact.artifact_sha256)
            || !identities.insert(artifact.snapshot_id)
        {
            return Err(failure("registry_target_review_invalid"));
        }
        let saved: StoredReview = serde_json::from_str(artifact.document.get())
            .map_err(|_| failure("registry_target_review_invalid"))?;
        if saved.component != COMPONENT
            || saved.revision != 1
            || saved.parser_version != PARSER_VERSION
            || !valid_digest(&saved.source_sha256)
            || !canonical_uuid(&saved.ledger_snapshot_id)
            || !valid_digest(&saved.ledger_artifact_sha256)
            || !valid_digest(&saved.ledger_source_sha256)
            || saved.decisions.len() > MAX_DECISIONS
        {
            return Err(failure("registry_target_review_invalid"));
        }
        if digest(&canonical(&saved)?) != artifact.artifact_sha256 {
            return Err(failure("registry_target_review_ledger_mismatch"));
        }
        let (ledger_hash, source_hash, targets) = ledgers
            .get(&saved.ledger_snapshot_id)
            .ok_or_else(|| failure("registry_target_review_ledger_mismatch"))?;
        if ledger_hash != &saved.ledger_artifact_sha256
            || source_hash != &saved.ledger_source_sha256
        {
            return Err(failure("registry_target_review_ledger_mismatch"));
        }
        referenced.insert(saved.ledger_snapshot_id);
        let original = serde_json::to_value(&saved.decisions)
            .map_err(|_| failure("registry_target_review_invalid"))?;
        let rows = saved
            .decisions
            .into_iter()
            .map(stored_decision)
            .collect::<Result<Vec<_>, _>>()?;
        let target_refs = targets.iter().map(String::as_str).collect();
        let (validated, _) = validate_decisions(rows, &target_refs)?;
        if serde_json::to_value(validated).map_err(|_| failure("registry_target_review_invalid"))?
            != original
        {
            return Err(failure("registry_target_review_binding_invalid"));
        }
    }
    if referenced.len() != ledgers.len() {
        return Err(failure("registry_target_review_ledger_mismatch"));
    }
    canonical(serde_json::json!({
        "component": "registry_required_target_review_validation", "revision": 1,
        "review_count": review_count, "ledger_count": ledger_count,
    }))
}
