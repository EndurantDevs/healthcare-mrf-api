//! Explicit company-network relationships with bounded, half-open coverage periods.

use crate::network_membership_codec::uuid_bytes;
use serde::de::{self, DeserializeSeed, SeqAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_EVIDENCE_CHARACTERS: usize = 1_000;
const JURISDICTIONS: &str = "AL AK AZ AR CA CO CT DE DC FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA WA WV WI WY AS GU MP PR VI";

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RelationshipRole {
    Uses,
    Operates,
    Administers,
    Publishes,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum BenefitDomain {
    Medical,
    Dental,
    Vision,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Applicability {
    National,
    States,
}

/// A relationship assertion never establishes corporate ownership or inherited access.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub struct CompanyNetworkAssertion {
    pub company_id: String,
    pub network_id: i32,
    pub relationship_role: RelationshipRole,
    pub benefit_domain: Option<BenefitDomain>,
    pub applicability: Applicability,
    pub states: Vec<String>,
    pub valid_from: Option<String>,
    pub valid_to: Option<String>,
    pub evidence_text: String,
}

/// Indices refer to the sorted output; each connected overlap remains for review.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct AssertionConflict {
    pub code: &'static str,
    pub assertion_indices: Vec<usize>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct ValidatedCompanyNetworkAssertions {
    pub assertions: Vec<CompanyNetworkAssertion>,
    pub conflicts: Vec<AssertionConflict>,
}

/// Static diagnostics identify invalid fields without echoing submitted evidence.
#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct CompanyNetworkAssertionError {
    pub code: &'static str,
    pub row_index: Option<usize>,
    pub field: Option<&'static str>,
}

impl fmt::Display for CompanyNetworkAssertionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Company-network assertions rejected: {}",
            self.code
        )
    }
}

impl std::error::Error for CompanyNetworkAssertionError {}

fn failure(
    code: &'static str,
    row_index: Option<usize>,
    field: Option<&'static str>,
) -> CompanyNetworkAssertionError {
    CompanyNetworkAssertionError {
        code,
        row_index,
        field,
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertionRow<'a> {
    company_id: String,
    network_id: i32,
    relationship_role: RelationshipRole,
    #[serde(borrow)]
    benefit_domain: &'a RawValue,
    applicability: Applicability,
    states: Vec<String>,
    #[serde(borrow)]
    valid_from: &'a RawValue,
    #[serde(borrow)]
    valid_to: &'a RawValue,
    evidence_text: String,
}

/// Validate atomically, sort deterministically, and retain overlapping evidence.
pub fn validate_company_network_assertions(
    input: &[u8],
) -> Result<ValidatedCompanyNetworkAssertions, CompanyNetworkAssertionError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(failure("input_limit", None, None));
    }
    let mut validator = AssertionValidator {
        assertions: Vec::new(),
        error: None,
    };
    let mut decoder = serde_json::Deserializer::from_slice(input);
    if (&mut validator).deserialize(&mut decoder).is_err() {
        return Err(validator
            .error
            .unwrap_or_else(|| failure("invalid_json", Some(validator.assertions.len()), None)));
    }
    if decoder.end().is_err() {
        return Err(failure("invalid_json", None, None));
    }
    validator.assertions.sort_unstable();
    if let Some(duplicate) = validator
        .assertions
        .windows(2)
        .find(|pair| pair[0].0 == pair[1].0)
    {
        return Err(failure(
            "duplicate_assertion",
            Some(duplicate[0].1.max(duplicate[1].1)),
            None,
        ));
    }
    let assertions: Vec<_> = validator
        .assertions
        .into_iter()
        .map(|(assertion, _)| assertion)
        .collect();
    let conflicts = overlap_conflicts(&assertions);
    Ok(ValidatedCompanyNetworkAssertions {
        assertions,
        conflicts,
    })
}

struct AssertionValidator {
    assertions: Vec<(CompanyNetworkAssertion, usize)>,
    error: Option<CompanyNetworkAssertionError>,
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
        formatter.write_str("an array of exact company-network assertions")
    }

    fn visit_seq<S: SeqAccess<'de>>(self, mut sequence: S) -> Result<(), S::Error> {
        while let Some(raw) = sequence.next_element::<&RawValue>()? {
            let index = self.assertions.len();
            let result = if index == MAX_ROWS {
                Err(failure("row_limit", Some(index), None))
            } else {
                validate_row(raw, index)
            };
            match result {
                Ok(assertion) => self.assertions.push((assertion, index)),
                Err(error) => {
                    self.error = Some(error);
                    return Err(de::Error::custom("Company-network assertion rejected"));
                }
            }
        }
        Ok(())
    }
}

fn validate_row(
    raw: &RawValue,
    index: usize,
) -> Result<CompanyNetworkAssertion, CompanyNetworkAssertionError> {
    let row: AssertionRow<'_> =
        serde_json::from_str(raw.get()).map_err(|_| failure("invalid_row", Some(index), None))?;
    if uuid_bytes(&row.company_id).is_none()
        || row.company_id.to_ascii_lowercase() != row.company_id
    {
        return Err(failure(
            "invalid_company_id",
            Some(index),
            Some("company_id"),
        ));
    }
    if row.network_id <= 0 {
        return Err(failure(
            "invalid_network_id",
            Some(index),
            Some("network_id"),
        ));
    }
    let benefit_domain = serde_json::from_str(row.benefit_domain.get()).map_err(|_| {
        failure(
            "invalid_benefit_domain",
            Some(index),
            Some("benefit_domain"),
        )
    })?;
    if (row.applicability == Applicability::National) != row.states.is_empty()
        || row.states.windows(2).any(|pair| pair[0] >= pair[1])
        || row.states.iter().any(|state| {
            !JURISDICTIONS
                .split_ascii_whitespace()
                .any(|known| state == known)
        })
    {
        return Err(failure("invalid_states", Some(index), Some("states")));
    }
    let valid_from = validate_date(row.valid_from, index, "valid_from")?;
    let valid_to = validate_date(row.valid_to, index, "valid_to")?;
    if matches!((&valid_from, &valid_to), (Some(start), Some(end)) if start >= end) {
        return Err(failure("invalid_period", Some(index), Some("valid_to")));
    }
    if row.evidence_text.trim().is_empty()
        || row.evidence_text.chars().count() > MAX_EVIDENCE_CHARACTERS
        || row.evidence_text.chars().any(char::is_control)
    {
        return Err(failure(
            "invalid_evidence",
            Some(index),
            Some("evidence_text"),
        ));
    }
    Ok(CompanyNetworkAssertion {
        company_id: row.company_id,
        network_id: row.network_id,
        relationship_role: row.relationship_role,
        benefit_domain,
        applicability: row.applicability,
        states: row.states,
        valid_from,
        valid_to,
        evidence_text: row.evidence_text,
    })
}

fn validate_date(
    raw: &RawValue,
    index: usize,
    field: &'static str,
) -> Result<Option<String>, CompanyNetworkAssertionError> {
    let date: Option<String> = serde_json::from_str(raw.get())
        .map_err(|_| failure("invalid_date", Some(index), Some(field)))?;
    if date.as_deref().is_some_and(|date| !is_canonical_date(date)) {
        return Err(failure("invalid_date", Some(index), Some(field)));
    }
    Ok(date)
}

fn is_canonical_date(date: &str) -> bool {
    let bytes = date.as_bytes();
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
    let year = date[..4].parse::<u16>().unwrap_or(0);
    let month = date[5..7].parse::<u8>().unwrap_or(0);
    let day = date[8..].parse::<u8>().unwrap_or(0);
    let days = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 if year.is_multiple_of(400) || year.is_multiple_of(4) && !year.is_multiple_of(100) => 29,
        2 => 28,
        _ => 0,
    };
    year > 0 && day > 0 && day <= days
}

fn same_scope(left: &CompanyNetworkAssertion, right: &CompanyNetworkAssertion) -> bool {
    left.company_id == right.company_id
        && left.network_id == right.network_id
        && left.relationship_role == right.relationship_role
        && left.benefit_domain == right.benefit_domain
        && left.applicability == right.applicability
        && left.states == right.states
}

/// A sorted interval sweep bounds both work and diagnostic size by the input count.
fn overlap_conflicts(assertions: &[CompanyNetworkAssertion]) -> Vec<AssertionConflict> {
    let mut conflicts = Vec::new();
    let mut component = Vec::new();
    let mut maximum_end: Option<&str> = None;
    for (index, assertion) in assertions.iter().enumerate() {
        let separated = index > 0
            && (!same_scope(&assertions[index - 1], assertion)
                || matches!((assertion.valid_from.as_deref(), maximum_end), (Some(start), Some(end)) if start >= end));
        if separated {
            retain_conflict(&mut conflicts, &mut component);
            maximum_end = assertion.valid_to.as_deref();
        } else if component.is_empty() {
            maximum_end = assertion.valid_to.as_deref();
        } else {
            maximum_end = match (maximum_end, assertion.valid_to.as_deref()) {
                (Some(previous), Some(current)) => Some(previous.max(current)),
                _ => None,
            };
        }
        component.push(index);
    }
    retain_conflict(&mut conflicts, &mut component);
    conflicts
}

fn retain_conflict(conflicts: &mut Vec<AssertionConflict>, component: &mut Vec<usize>) {
    if component.len() > 1 {
        conflicts.push(AssertionConflict {
            code: "overlapping_periods",
            assertion_indices: std::mem::take(component),
        });
    } else {
        component.clear();
    }
}
