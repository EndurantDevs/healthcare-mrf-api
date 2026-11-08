//! Bounded evidence review only; classifications neither admit writes nor prove source custody.

use crate::network_membership_codec::uuid_bytes;
use serde::de::{self, DeserializeOwned, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_REFERENCE_BYTES: usize = 128;
pub const MAX_LABEL_BYTES: usize = 1_000;

/// Supplied semantics preserve the existing group and network interval boundaries.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReviewPolicy {
    GroupInclusive,
    NetworkHalfOpen,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct ScopedIdentifier {
    system: String,
    value: String,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct SourceEdition {
    source_system: String,
    source_id: String,
    edition_id: String,
    snapshot_id: String,
}

#[derive(Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd)]
#[serde(deny_unknown_fields)]
struct Evidence {
    source_edition: SourceEdition,
    record_key: String,
    original_identifier: ScopedIdentifier,
    document_sha256: String,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Assertion {
    subject_id: String,
    predicate: String,
    scope: ScopedIdentifier,
    object_identifier: Option<ScopedIdentifier>,
    valid_from: Option<String>,
    valid_to: Option<String>,
    evidence: Evidence,
    source_label: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionRow<'a> {
    subject_id: String,
    predicate: String,
    scope: ScopedIdentifier,
    #[serde(borrow)]
    object_identifier: &'a RawValue,
    #[serde(borrow)]
    valid_from: &'a RawValue,
    #[serde(borrow)]
    valid_to: &'a RawValue,
    evidence: Evidence,
    source_label: Option<String>,
}

#[derive(Debug)]
struct RetainedAssertion {
    value: Assertion,
    raw: Box<RawValue>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Disposition {
    Accepted,
    Duplicate,
    ReviewRequired,
}

/// The original JSON assertion is retained, including its supplied evidence and dates.
#[derive(Debug, Serialize)]
pub struct ClassifiedAssertion {
    pub assertion: Box<RawValue>,
    pub disposition: Disposition,
    pub duplicate_of: Option<usize>,
}

/// Indices address canonical output rows, never unstable input positions.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionReview {
    pub code: &'static str,
    pub assertion_indices: Vec<usize>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionCounts {
    pub input_rows: usize,
    pub accepted_rows: usize,
    pub duplicate_rows: usize,
    pub review_required_rows: usize,
}

#[derive(Debug, Serialize)]
pub struct ReviewBatch {
    pub policy: ReviewPolicy,
    pub assertions: Vec<ClassifiedAssertion>,
    pub reviews: Vec<AssertionReview>,
    pub counts: AssertionCounts,
}

/// Refusals expose only static codes and input coordinates, never supplied values.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionReviewError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
}

impl fmt::Display for AssertionReviewError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "Assertion review refused: {}", self.code)
    }
}
impl std::error::Error for AssertionReviewError {}

fn failure(
    code: &'static str,
    row: Option<usize>,
    field: Option<&'static str>,
) -> AssertionReviewError {
    AssertionReviewError {
        code,
        row_index: row,
        field,
    }
}

/// Validate the complete batch before emitting bounded, deterministic review classifications.
pub fn classify_registry_assertions(
    input: &[u8],
    policy: ReviewPolicy,
) -> Result<ReviewBatch, AssertionReviewError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None, None));
    }
    let mut validator = AssertionValidator {
        rows: Vec::new(),
        policy,
        error: None,
    };
    let mut decoder = serde_json::Deserializer::from_slice(input);
    if (&mut validator).deserialize(&mut decoder).is_err() {
        return Err(validator
            .error
            .unwrap_or_else(|| failure("invalid_json", None, None)));
    }
    if decoder.end().is_err() {
        return Err(failure("invalid_json", None, None));
    }
    validator
        .rows
        .sort_by(|left, right| (&left.value, left.raw.get()).cmp(&(&right.value, right.raw.get())));
    let rows = validator.rows;
    let duplicate_of = duplicate_indices(&rows);
    let mut reviews = missing_reviews(&rows);
    reviews.extend(evidence_reviews(&rows));
    reviews.extend(overlap_reviews(&rows, policy));
    reviews.sort_by(|left, right| {
        (left.code, &left.assertion_indices).cmp(&(right.code, &right.assertion_indices))
    });
    let reviewed: BTreeSet<_> = reviews
        .iter()
        .flat_map(|review| review.assertion_indices.iter().copied())
        .collect();
    let batch = classified_batch(rows, duplicate_of, reviewed, reviews, policy);
    let encoded = serde_json::to_vec(&batch).map_err(|_| failure("output_invalid", None, None))?;
    if encoded.len() > MAX_OUTPUT_BYTES {
        return Err(failure("output_limit", None, None));
    }
    Ok(batch)
}

struct AssertionValidator {
    rows: Vec<RetainedAssertion>,
    policy: ReviewPolicy,
    error: Option<AssertionReviewError>,
}

impl<'de> DeserializeSeed<'de> for &mut AssertionValidator {
    type Value = ();
    fn deserialize<D: serde::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}

impl<'de> Visitor<'de> for &mut AssertionValidator {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("one bounded assertion array")
    }
    fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<(), A::Error> {
        while let Some(raw) = sequence.next_element::<&RawValue>()? {
            let index = self.rows.len();
            let row = if index == MAX_ROWS {
                Err(failure("row_limit", Some(index), None))
            } else {
                validate_row(raw, index, self.policy)
            };
            match row {
                Ok(value) => self.rows.push(RetainedAssertion {
                    value,
                    raw: raw.to_owned(),
                }),
                Err(error) => {
                    self.error = Some(error);
                    return Err(de::Error::custom("assertion refused"));
                }
            }
        }
        Ok(())
    }
}

fn reference(value: &str, maximum: usize) -> bool {
    !value.is_empty()
        && value.len() <= maximum
        && value.trim() == value
        && !value.chars().any(char::is_control)
}

fn identifier(value: &ScopedIdentifier) -> bool {
    reference(&value.system, MAX_REFERENCE_BYTES) && reference(&value.value, MAX_REFERENCE_BYTES)
}

fn canonical_uuid(value: &str) -> bool {
    uuid_bytes(value).is_some() && !value.bytes().any(|byte| byte.is_ascii_uppercase())
}

fn evidence_valid(evidence: &Evidence) -> bool {
    let edition = &evidence.source_edition;
    [
        &edition.source_system,
        &edition.source_id,
        &edition.edition_id,
        &evidence.record_key,
    ]
    .iter()
    .all(|value| reference(value, MAX_REFERENCE_BYTES))
        && canonical_uuid(&edition.snapshot_id)
        && identifier(&evidence.original_identifier)
        && evidence.document_sha256.len() == 64
        && evidence
            .document_sha256
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn nullable<T: DeserializeOwned>(
    raw: &RawValue,
    index: usize,
    field: &'static str,
) -> Result<Option<T>, AssertionReviewError> {
    serde_json::from_str(raw.get()).map_err(|_| failure("invalid_field", Some(index), Some(field)))
}

fn validate_row(
    raw: &RawValue,
    index: usize,
    policy: ReviewPolicy,
) -> Result<Assertion, AssertionReviewError> {
    let row: AssertionRow<'_> =
        serde_json::from_str(raw.get()).map_err(|_| failure("invalid_row", Some(index), None))?;
    if !canonical_uuid(&row.subject_id) {
        return Err(failure("invalid_uuid", Some(index), Some("subject_id")));
    }
    if row.predicate.is_empty()
        || row.predicate.len() > 64
        || !row.predicate.bytes().enumerate().all(|(index, byte)| {
            byte.is_ascii_lowercase() || index > 0 && (byte.is_ascii_digit() || byte == b'_')
        })
    {
        return Err(failure("invalid_predicate", Some(index), Some("predicate")));
    }
    let object_identifier: Option<ScopedIdentifier> =
        nullable(row.object_identifier, index, "object_identifier")?;
    if !identifier(&row.scope)
        || object_identifier
            .as_ref()
            .is_some_and(|value| !identifier(value))
    {
        return Err(failure("invalid_identifier", Some(index), None));
    }
    let valid_from: Option<String> = nullable(row.valid_from, index, "valid_from")?;
    let valid_to: Option<String> = nullable(row.valid_to, index, "valid_to")?;
    for (field, value) in [("valid_from", &valid_from), ("valid_to", &valid_to)] {
        if value.as_deref().is_some_and(|value| !canonical_date(value)) {
            return Err(failure("invalid_date", Some(index), Some(field)));
        }
    }
    if matches!((&valid_from, &valid_to), (Some(start), Some(end)) if start > end || start == end && policy == ReviewPolicy::NetworkHalfOpen)
    {
        return Err(failure("invalid_period", Some(index), Some("valid_to")));
    }
    if !evidence_valid(&row.evidence) {
        return Err(failure("invalid_evidence", Some(index), Some("evidence")));
    }
    if row
        .source_label
        .as_deref()
        .is_some_and(|value| !reference(value, MAX_LABEL_BYTES))
    {
        return Err(failure("invalid_label", Some(index), Some("source_label")));
    }
    Ok(Assertion {
        subject_id: row.subject_id,
        predicate: row.predicate,
        scope: row.scope,
        object_identifier,
        valid_from,
        valid_to,
        evidence: row.evidence,
        source_label: row.source_label,
    })
}

// Exact existing company-group/network Gregorian algorithm; their helpers are private.
fn canonical_date(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 10
        || bytes.iter().enumerate().any(|(index, byte)| {
            if matches!(index, 4 | 7) {
                *byte != b'-'
            } else {
                !byte.is_ascii_digit()
            }
        })
    {
        return false;
    }
    let year = value[..4].parse::<u16>().unwrap_or(0);
    let month = value[5..7].parse::<u8>().unwrap_or(0);
    let day = value[8..].parse::<u8>().unwrap_or(0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || year.is_multiple_of(4) && !year.is_multiple_of(100) => 29,
        2 => 28,
        _ => 0,
    };
    year > 0 && day > 0 && day <= days
}

fn duplicate_indices(rows: &[RetainedAssertion]) -> Vec<Option<usize>> {
    let mut first_by_fact = BTreeMap::new();
    rows.iter()
        .enumerate()
        .map(|(index, row)| {
            let value = &row.value;
            let key = (
                &value.subject_id,
                &value.predicate,
                &value.scope,
                &value.object_identifier,
                &value.valid_from,
                &value.valid_to,
                &value.evidence,
            );
            match first_by_fact.entry(key) {
                std::collections::btree_map::Entry::Vacant(entry) => {
                    entry.insert(index);
                    None
                }
                std::collections::btree_map::Entry::Occupied(entry) => Some(*entry.get()),
            }
        })
        .collect()
}

fn missing_reviews(rows: &[RetainedAssertion]) -> Vec<AssertionReview> {
    let mut missing: BTreeMap<_, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        if row.value.object_identifier.is_none() {
            missing.entry("missing_object").or_default().push(index);
        }
        if row.value.valid_from.is_none() {
            missing
                .entry("missing_effective_start")
                .or_default()
                .push(index);
        }
    }
    missing
        .into_iter()
        .map(|(code, assertion_indices)| AssertionReview {
            code,
            assertion_indices,
        })
        .collect()
}

fn evidence_reviews(rows: &[RetainedAssertion]) -> Vec<AssertionReview> {
    let mut identities: BTreeMap<_, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let evidence = &row.value.evidence;
        identities
            .entry((
                &evidence.source_edition,
                &evidence.record_key,
                &evidence.original_identifier,
            ))
            .or_default()
            .push(index);
    }
    identities
        .into_values()
        .filter_map(|indices| {
            let digests: BTreeSet<_> = indices
                .iter()
                .map(|index| &rows[*index].value.evidence.document_sha256)
                .collect();
            (digests.len() > 1).then_some(AssertionReview {
                code: "evidence_digest_conflict",
                assertion_indices: indices,
            })
        })
        .collect()
}

fn retain_component(
    rows: &[RetainedAssertion],
    component: &mut Vec<usize>,
    policy: ReviewPolicy,
    reviews: &mut Vec<AssertionReview>,
) {
    let code = match policy {
        ReviewPolicy::GroupInclusive => {
            let objects: BTreeSet<_> = component
                .iter()
                .map(|index| &rows[*index].value.object_identifier)
                .collect();
            (objects.len() > 1).then_some("contradictory_overlapping_objects")
        }
        ReviewPolicy::NetworkHalfOpen => {
            let intervals: BTreeSet<_> = component
                .iter()
                .map(|index| {
                    let row = &rows[*index].value;
                    (&row.valid_from, &row.valid_to)
                })
                .collect();
            (intervals.len() > 1).then_some("overlapping_periods")
        }
    };
    if let Some(code) = code {
        component.sort_unstable();
        reviews.push(AssertionReview {
            code,
            assertion_indices: std::mem::take(component),
        });
    } else {
        component.clear();
    }
}

fn overlap_reviews(rows: &[RetainedAssertion], policy: ReviewPolicy) -> Vec<AssertionReview> {
    let mut scopes: BTreeMap<_, Vec<usize>> = BTreeMap::new();
    for (index, row) in rows.iter().enumerate() {
        let value = &row.value;
        if value.valid_from.is_some() && value.object_identifier.is_some() {
            let object =
                (policy == ReviewPolicy::NetworkHalfOpen).then_some(&value.object_identifier);
            scopes
                .entry((&value.subject_id, &value.predicate, &value.scope, object))
                .or_default()
                .push(index);
        }
    }
    let mut reviews = Vec::new();
    for indices in scopes.values_mut() {
        indices.sort_by_key(|index| {
            (
                &rows[*index].value.valid_from,
                &rows[*index].value.valid_to,
                *index,
            )
        });
        let mut component = Vec::new();
        let mut maximum_end: Option<&str> = None;
        for index in indices.iter().copied() {
            let value = &rows[index].value;
            let separated = matches!((value.valid_from.as_deref(), maximum_end), (Some(start), Some(end)) if start > end || start == end && policy == ReviewPolicy::NetworkHalfOpen);
            if !component.is_empty() && separated {
                retain_component(rows, &mut component, policy, &mut reviews);
            }
            maximum_end = if component.is_empty() {
                value.valid_to.as_deref()
            } else {
                match (maximum_end, value.valid_to.as_deref()) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    _ => None,
                }
            };
            component.push(index);
        }
        retain_component(rows, &mut component, policy, &mut reviews);
    }
    reviews
}

fn classified_batch(
    rows: Vec<RetainedAssertion>,
    duplicate_of: Vec<Option<usize>>,
    reviewed: BTreeSet<usize>,
    reviews: Vec<AssertionReview>,
    policy: ReviewPolicy,
) -> ReviewBatch {
    let mut counts = AssertionCounts {
        input_rows: rows.len(),
        accepted_rows: 0,
        duplicate_rows: 0,
        review_required_rows: 0,
    };
    let assertions = rows
        .into_iter()
        .enumerate()
        .map(|(index, row)| {
            let disposition = if reviewed.contains(&index) {
                counts.review_required_rows += 1;
                Disposition::ReviewRequired
            } else if duplicate_of[index].is_some() {
                counts.duplicate_rows += 1;
                Disposition::Duplicate
            } else {
                counts.accepted_rows += 1;
                Disposition::Accepted
            };
            ClassifiedAssertion {
                assertion: row.raw,
                disposition,
                duplicate_of: duplicate_of[index],
            }
        })
        .collect();
    ReviewBatch {
        policy,
        assertions,
        reviews,
        counts,
    }
}
