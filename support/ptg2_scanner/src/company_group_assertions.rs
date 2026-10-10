//! Bounded dated company-group computation; Unicode acceptance belongs to the Python boundary.

use crate::network_membership_codec::uuid_bytes;
use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_REFERENCE_BYTES: usize = 128;

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum GroupKind {
    CorporateParent,
    NaicGroup,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipKind {
    ReportedAffiliation,
    VerifiedOwnership,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ProvenanceKind {
    ManualReference,
    SourceReference,
}

/// Declared references supply neither native source custody nor human approval.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct AssertionProvenance {
    pub kind: ProvenanceKind,
    pub evidence_ref: String,
    pub snapshot_id: Option<String>,
    pub source_record_key: Option<String>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CompanyGroupAssertion {
    pub assertion_id: String,
    pub company_id: String,
    pub group_id: Option<String>,
    pub group_kind: GroupKind,
    pub relationship_kind: RelationshipKind,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub provenance: AssertionProvenance,
}

/// Indices refer to retained canonical assertions, never submitted row positions.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionReview {
    pub code: &'static str,
    pub assertion_indices: Vec<usize>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionCounts {
    pub input_rows: usize,
    pub effective_rows: usize,
    pub review_rows: usize,
}

/// Structural reduction only: the boundary must call Python str.isprintable once
/// on provenance_text() before exposing any serialized result as accepted.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct UncheckedCompanyGroupReduction {
    pub assertions: Vec<CompanyGroupAssertion>,
    pub effective_assertion_indices: Vec<usize>,
    pub reviews: Vec<AssertionReview>,
    pub counts: AssertionCounts,
    #[serde(skip)]
    provenance_text: String,
}

impl UncheckedCompanyGroupReduction {
    /// Contains every bounded provenance text value separated by printable ASCII.
    pub fn provenance_text(&self) -> &str {
        &self.provenance_text
    }
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct CompanyGroupAssertionError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
}

impl fmt::Display for CompanyGroupAssertionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Company-group assertions rejected: {}",
            self.code
        )
    }
}
impl std::error::Error for CompanyGroupAssertionError {}

fn failure(
    code: &'static str,
    row: usize,
    field: Option<&'static str>,
) -> CompanyGroupAssertionError {
    CompanyGroupAssertionError {
        code,
        row_index: Some(row),
        field,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProvenanceRow<'a> {
    kind: ProvenanceKind,
    evidence_ref: String,
    #[serde(borrow)]
    snapshot_id: &'a RawValue,
    #[serde(borrow)]
    source_record_key: &'a RawValue,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionRow<'a> {
    assertion_id: String,
    company_id: String,
    #[serde(borrow)]
    group_id: &'a RawValue,
    group_kind: GroupKind,
    relationship_kind: RelationshipKind,
    #[serde(borrow)]
    valid_from: &'a RawValue,
    #[serde(borrow)]
    valid_to: &'a RawValue,
    #[serde(borrow)]
    provenance: ProvenanceRow<'a>,
}

/// Validate structure atomically and retain every assertion, including unknown periods.
/// This function deliberately does not claim Python Unicode/provenance acceptance.
pub fn reduce_company_group_assertions(
    input: &[u8],
) -> Result<UncheckedCompanyGroupReduction, CompanyGroupAssertionError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(CompanyGroupAssertionError {
            code: "input_limit",
            row_index: None,
            field: None,
        });
    }
    let mut validator = AssertionValidator {
        assertions: Vec::new(),
        identities: BTreeSet::new(),
        error: None,
    };
    let mut decoder = serde_json::Deserializer::from_slice(input);
    if (&mut validator).deserialize(&mut decoder).is_err() {
        return Err(validator
            .error
            .unwrap_or_else(|| failure("invalid_json", validator.assertions.len(), None)));
    }
    if decoder.end().is_err() {
        return Err(failure("invalid_json", validator.assertions.len(), None));
    }
    validator.assertions.sort_unstable();
    let assertions = validator.assertions;
    let mut reviews = missing_reviews(&assertions);
    reviews.extend(overlap_reviews(&assertions));
    reviews.sort_by(|left, right| {
        (left.code, &left.assertion_indices).cmp(&(right.code, &right.assertion_indices))
    });
    let review_indices: BTreeSet<_> = reviews
        .iter()
        .flat_map(|review| review.assertion_indices.iter().copied())
        .collect();
    let effective_assertion_indices: Vec<_> = (0..assertions.len())
        .filter(|index| !review_indices.contains(index))
        .collect();
    let mut provenance_text = String::from(" ");
    for assertion in &assertions {
        provenance_text.push_str(&assertion.provenance.evidence_ref);
        provenance_text.push(' ');
        if let Some(key) = &assertion.provenance.source_record_key {
            provenance_text.push_str(key);
            provenance_text.push(' ');
        }
    }
    let counts = AssertionCounts {
        input_rows: assertions.len(),
        effective_rows: effective_assertion_indices.len(),
        review_rows: review_indices.len(),
    };
    let reduction = UncheckedCompanyGroupReduction {
        assertions,
        effective_assertion_indices,
        reviews,
        counts,
        provenance_text,
    };
    let encoded = serde_json::to_vec(&reduction).map_err(|_| failure("output_invalid", 0, None))?;
    if encoded.len() > MAX_OUTPUT_BYTES {
        return Err(failure("output_limit", 0, None));
    }
    Ok(reduction)
}

struct AssertionValidator {
    assertions: Vec<CompanyGroupAssertion>,
    identities: BTreeSet<String>,
    error: Option<CompanyGroupAssertionError>,
}
impl<'de> DeserializeSeed<'de> for &mut AssertionValidator {
    type Value = ();
    fn deserialize<D: de::Deserializer<'de>>(self, decoder: D) -> Result<(), D::Error> {
        decoder.deserialize_seq(self)
    }
}
impl<'de> Visitor<'de> for &mut AssertionValidator {
    type Value = ();
    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("an array of exact company-group assertions")
    }
    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        while let Some(raw) = sequence.next_element::<&RawValue>()? {
            let index = self.assertions.len();
            let result = if index == MAX_ROWS {
                Err(failure("row_limit", index, None))
            } else {
                validate_row(raw, index)
            };
            match result {
                Ok(assertion) if self.identities.insert(assertion.assertion_id.clone()) => {
                    self.assertions.push(assertion)
                }
                Ok(_) => {
                    self.error = Some(failure(
                        "duplicate_assertion_id",
                        index,
                        Some("assertion_id"),
                    ));
                    return Err(de::Error::custom("Company-group assertion rejected"));
                }
                Err(error) => {
                    self.error = Some(error);
                    return Err(de::Error::custom("Company-group assertion rejected"));
                }
            }
        }
        Ok(())
    }
}
fn canonical_uuid(value: &str) -> bool {
    uuid_bytes(value).is_some() && value.to_ascii_lowercase() == value
}
fn optional_text(
    raw: &RawValue,
    index: usize,
    field: &'static str,
) -> Result<Option<String>, CompanyGroupAssertionError> {
    serde_json::from_str(raw.get()).map_err(|_| failure("invalid_field", index, Some(field)))
}
fn reference(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= MAX_REFERENCE_BYTES
        && value.trim() == value
        && !value.chars().any(char::is_control)
}
fn validate_row(
    raw: &RawValue,
    index: usize,
) -> Result<CompanyGroupAssertion, CompanyGroupAssertionError> {
    let row: AssertionRow<'_> =
        serde_json::from_str(raw.get()).map_err(|_| failure("invalid_row", index, None))?;
    for (field, value) in [
        ("assertion_id", &row.assertion_id),
        ("company_id", &row.company_id),
    ] {
        if !canonical_uuid(value) {
            return Err(failure("invalid_uuid", index, Some(field)));
        }
    }
    let group_id = optional_text(row.group_id, index, "group_id")?;
    if group_id
        .as_deref()
        .is_some_and(|value| !canonical_uuid(value))
    {
        return Err(failure("invalid_uuid", index, Some("group_id")));
    }
    let valid_from = date(row.valid_from, index, "valid_from")?;
    let valid_to = date(row.valid_to, index, "valid_to")?;
    if matches!((&valid_from, &valid_to), (Some(start), Some(end)) if start > end) {
        return Err(failure("invalid_period", index, Some("valid_to")));
    }
    let snapshot_id = optional_text(row.provenance.snapshot_id, index, "snapshot_id")?;
    let source_record_key =
        optional_text(row.provenance.source_record_key, index, "source_record_key")?;
    if !reference(&row.provenance.evidence_ref) {
        return Err(failure("invalid_provenance", index, Some("evidence_ref")));
    }
    let provenance_valid = match row.provenance.kind {
        ProvenanceKind::ManualReference => {
            snapshot_id.is_none() && source_record_key.is_none() && group_id.is_some()
        }
        ProvenanceKind::SourceReference => {
            snapshot_id.as_deref().is_some_and(canonical_uuid)
                && source_record_key.as_deref().is_some_and(reference)
        }
    };
    if !provenance_valid {
        return Err(failure("invalid_provenance", index, Some("provenance")));
    }
    Ok(CompanyGroupAssertion {
        assertion_id: row.assertion_id,
        company_id: row.company_id,
        group_id,
        group_kind: row.group_kind,
        relationship_kind: row.relationship_kind,
        valid_from,
        valid_to,
        provenance: AssertionProvenance {
            kind: row.provenance.kind,
            evidence_ref: row.provenance.evidence_ref,
            snapshot_id,
            source_record_key,
        },
    })
}

fn date(
    raw: &RawValue,
    index: usize,
    field: &'static str,
) -> Result<Option<String>, CompanyGroupAssertionError> {
    let value = optional_text(raw, index, field)?;
    if value.as_deref().is_some_and(|value| !canonical_date(value)) {
        return Err(failure("invalid_date", index, Some(field)));
    }
    Ok(value)
}
// The existing company-network Gregorian helper is private; retain its algorithm here.
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

fn missing_reviews(assertions: &[CompanyGroupAssertion]) -> Vec<AssertionReview> {
    let mut reviews: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (index, assertion) in assertions.iter().enumerate() {
        let code = match (assertion.group_id.is_none(), assertion.valid_from.is_none()) {
            (true, true) => "missing_group_and_effective_start",
            (true, false) => "missing_group",
            (false, true) => "missing_effective_start",
            (false, false) => continue,
        };
        reviews.entry(code).or_default().push(index);
    }
    reviews
        .into_iter()
        .map(|(code, assertion_indices)| AssertionReview {
            code,
            assertion_indices,
        })
        .collect()
}
fn retain_component(
    assertions: &[CompanyGroupAssertion],
    component: &mut Vec<usize>,
    reviews: &mut Vec<AssertionReview>,
) {
    let groups: BTreeSet<_> = component
        .iter()
        .map(|index| &assertions[*index].group_id)
        .collect();
    if groups.len() > 1 {
        component.sort_unstable();
        reviews.push(AssertionReview {
            code: "contradictory_overlapping_groups",
            assertion_indices: std::mem::take(component),
        });
    } else {
        component.clear();
    }
}
fn overlap_reviews(assertions: &[CompanyGroupAssertion]) -> Vec<AssertionReview> {
    let mut scopes: BTreeMap<_, Vec<usize>> = BTreeMap::new();
    for (index, assertion) in assertions.iter().enumerate() {
        if assertion.group_id.is_some() && assertion.valid_from.is_some() {
            scopes
                .entry((
                    &assertion.company_id,
                    assertion.group_kind,
                    assertion.relationship_kind,
                ))
                .or_default()
                .push(index);
        }
    }
    let mut reviews = Vec::new();
    for indices in scopes.values_mut() {
        indices.sort_by_key(|index| {
            (
                &assertions[*index].valid_from,
                &assertions[*index].valid_to,
                *index,
            )
        });
        let mut component = Vec::new();
        let mut maximum_end: Option<&str> = None;
        for index in indices.iter().copied() {
            let assertion = &assertions[index];
            if !component.is_empty()
                && matches!((assertion.valid_from.as_deref(), maximum_end), (Some(start), Some(end)) if start > end)
            {
                retain_component(assertions, &mut component, &mut reviews);
            }
            maximum_end = if component.is_empty() {
                assertion.valid_to.as_deref()
            } else {
                match (maximum_end, assertion.valid_to.as_deref()) {
                    (Some(left), Some(right)) => Some(left.max(right)),
                    _ => None,
                }
            };
            component.push(index);
        }
        retain_component(assertions, &mut component, &mut reviews);
    }
    reviews
}
