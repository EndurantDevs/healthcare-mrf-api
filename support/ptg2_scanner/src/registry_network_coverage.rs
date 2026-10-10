//! Bounded coverage of a supplied target ledger using independent network evidence.

use serde::{Deserialize, Deserializer, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

pub const MAX_ROWS: usize = 5_000;
pub const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_TARGET_KEY_BYTES: usize = 192;

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryNetworkCoverageError {
    pub code: &'static str,
}

impl fmt::Display for RegistryNetworkCoverageError {
    fn fmt(&self, output: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            output,
            "Registry network coverage is invalid or exceeds its bounds"
        )
    }
}

impl std::error::Error for RegistryNetworkCoverageError {}

fn invalid() -> RegistryNetworkCoverageError {
    RegistryNetworkCoverageError {
        code: "registry_coverage_invalid",
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceStatus {
    Current,
    Historical,
    Unavailable,
    Unresolved,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    target_key: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Binding {
    target_key: String,
    #[serde(deserialize_with = "required_network_id")]
    network_id: Option<i32>,
    resolution_status: String,
}

fn required_network_id<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<i32>, D::Error> {
    Option::<i32>::deserialize(decoder)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Evidence {
    network_id: i32,
    company_link_verified: bool,
    exact_membership_count: u64,
    #[serde(deserialize_with = "required_pricing_count")]
    pricing_evidence_count: Option<u64>,
    source_status: SourceStatus,
    unresolved_location_count: u64,
}

fn required_pricing_count<'de, D: Deserializer<'de>>(decoder: D) -> Result<Option<u64>, D::Error> {
    Option::<u64>::deserialize(decoder)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CoverageInput {
    targets: Vec<Target>,
    bindings: Vec<Binding>,
    evidence: Vec<Evidence>,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryNetworkCoverageTarget {
    pub target_key: String,
    pub network_id: Option<i32>,
    pub mapping_status: &'static str,
    pub company_link_verified: bool,
    pub directory_available: bool,
    pub priceable: Option<bool>,
    pub source_status: Option<SourceStatus>,
    pub unresolved_location_count: u64,
    pub gaps: Vec<&'static str>,
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
pub struct RegistryCoverageSourceCounts {
    pub current: u64,
    pub historical: u64,
    pub unavailable: u64,
    pub unresolved: u64,
}

#[derive(Debug, Default, Eq, PartialEq, Serialize)]
pub struct RegistryNetworkCoverageTotals {
    pub ledger_targets: u64,
    pub mapped_targets: u64,
    pub company_link_verified: u64,
    pub directory_available: u64,
    pub priceable: u64,
    pub targets_with_gaps: u64,
    /// Evidence sums and source counts include each evidenced network exactly once.
    pub exact_membership_count: u64,
    pub pricing_evidence_count: Option<u64>,
    pub unresolved_location_count: u64,
    pub source_status_counts: RegistryCoverageSourceCounts,
}

#[derive(Debug, Eq, PartialEq, Serialize)]
pub struct RegistryNetworkCoverage {
    pub targets: Vec<RegistryNetworkCoverageTarget>,
    pub totals: RegistryNetworkCoverageTotals,
}

fn checked_add(total: &mut u64, amount: u64) -> Result<(), RegistryNetworkCoverageError> {
    *total = total
        .checked_add(amount)
        .ok_or(RegistryNetworkCoverageError {
            code: "registry_coverage_overflow",
        })?;
    Ok(())
}

fn target_keys(input: &CoverageInput) -> Result<BTreeSet<&str>, RegistryNetworkCoverageError> {
    let mut keys = BTreeSet::new();
    for target in &input.targets {
        let key = target.target_key.as_str();
        if key.is_empty() || key.len() > MAX_TARGET_KEY_BYTES || !keys.insert(key) {
            return Err(invalid());
        }
    }
    Ok(keys)
}

fn binding_lookup<'a>(
    input: &'a CoverageInput,
    keys: &BTreeSet<&str>,
) -> Result<BTreeMap<&'a str, &'a Binding>, RegistryNetworkCoverageError> {
    let mut bindings = BTreeMap::new();
    for binding in &input.bindings {
        let valid_status = match binding.resolution_status.as_str() {
            "resolved" => binding.network_id.is_some_and(|network| network > 0),
            "unresolved" | "conflicting" => binding.network_id.is_none(),
            _ => false,
        };
        if !valid_status
            || !keys.contains(binding.target_key.as_str())
            || bindings
                .insert(binding.target_key.as_str(), binding)
                .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(bindings)
}

fn evidence_lookup(
    input: &CoverageInput,
) -> Result<BTreeMap<i32, &Evidence>, RegistryNetworkCoverageError> {
    let referenced: BTreeSet<_> = input
        .bindings
        .iter()
        .filter_map(|binding| binding.network_id)
        .collect();
    let mut evidence = BTreeMap::new();
    for row in &input.evidence {
        if row.network_id <= 0
            || !referenced.contains(&row.network_id)
            || evidence.insert(row.network_id, row).is_some()
        {
            return Err(invalid());
        }
    }
    Ok(evidence)
}

fn evidence_totals(
    evidence: &BTreeMap<i32, &Evidence>,
) -> Result<RegistryNetworkCoverageTotals, RegistryNetworkCoverageError> {
    let mut totals = RegistryNetworkCoverageTotals::default();
    let mut known_pricing_count = 0;
    let mut all_pricing_assessed = !evidence.is_empty();
    for row in evidence.values() {
        checked_add(
            &mut totals.exact_membership_count,
            row.exact_membership_count,
        )?;
        match row.pricing_evidence_count {
            Some(count) => checked_add(&mut known_pricing_count, count)?,
            None => all_pricing_assessed = false,
        }
        checked_add(
            &mut totals.unresolved_location_count,
            row.unresolved_location_count,
        )?;
        let count = match row.source_status {
            SourceStatus::Current => &mut totals.source_status_counts.current,
            SourceStatus::Historical => &mut totals.source_status_counts.historical,
            SourceStatus::Unavailable => &mut totals.source_status_counts.unavailable,
            SourceStatus::Unresolved => &mut totals.source_status_counts.unresolved,
        };
        checked_add(count, 1)?;
    }
    totals.pricing_evidence_count = all_pricing_assessed.then_some(known_pricing_count);
    Ok(totals)
}

fn coverage_target(
    key: &str,
    binding: Option<&Binding>,
    evidence: Option<&Evidence>,
) -> RegistryNetworkCoverageTarget {
    let mut target = RegistryNetworkCoverageTarget {
        target_key: key.to_owned(),
        network_id: binding.and_then(|row| row.network_id),
        mapping_status: "missing",
        company_link_verified: false,
        directory_available: false,
        priceable: None,
        source_status: None,
        unresolved_location_count: 0,
        gaps: Vec::new(),
    };
    let Some(binding) = binding else {
        target.gaps.push("mapping_missing");
        return target;
    };
    match binding.resolution_status.as_str() {
        "unresolved" => {
            target.mapping_status = "unresolved";
            target.gaps.push("mapping_unresolved");
        }
        "conflicting" => {
            target.mapping_status = "conflicting";
            target.gaps.push("mapping_conflicting");
        }
        _ => target.mapping_status = "resolved",
    }
    if target.mapping_status != "resolved" {
        return target;
    }
    let Some(evidence) = evidence else {
        target.gaps.push("evidence_missing");
        return target;
    };
    target.company_link_verified = evidence.company_link_verified;
    target.directory_available = evidence.exact_membership_count > 0;
    target.priceable = evidence.pricing_evidence_count.map(|count| count > 0);
    target.source_status = Some(evidence.source_status);
    target.unresolved_location_count = evidence.unresolved_location_count;
    target.gaps = evidence_gaps(evidence);
    target
}

fn evidence_gaps(evidence: &Evidence) -> Vec<&'static str> {
    let mut gaps = Vec::new();
    if !evidence.company_link_verified {
        gaps.push("company_link_unresolved");
    }
    if evidence.exact_membership_count == 0 {
        gaps.push("directory_membership_missing");
    }
    match evidence.pricing_evidence_count {
        None => gaps.push("pricing_not_assessed"),
        Some(0) => gaps.push("pricing_evidence_missing"),
        Some(_) => (),
    }
    match evidence.source_status {
        SourceStatus::Unavailable => gaps.push("source_unavailable"),
        SourceStatus::Historical => gaps.push("source_historical"),
        SourceStatus::Unresolved => gaps.push("source_unresolved"),
        SourceStatus::Current => (),
    }
    if evidence.unresolved_location_count > 0 {
        gaps.push("location_unresolved");
    }
    gaps
}

/// Cover every supplied target; independent booleans never imply another dimension.
pub fn build_registry_network_coverage(
    input: &[u8],
) -> Result<RegistryNetworkCoverage, RegistryNetworkCoverageError> {
    if input.len() > MAX_INPUT_BYTES {
        return Err(RegistryNetworkCoverageError {
            code: "registry_coverage_limit",
        });
    }
    let input: CoverageInput = serde_json::from_slice(input).map_err(|_| invalid())?;
    if [
        input.targets.len(),
        input.bindings.len(),
        input.evidence.len(),
    ]
    .into_iter()
    .any(|count| count > MAX_ROWS)
    {
        return Err(RegistryNetworkCoverageError {
            code: "registry_coverage_limit",
        });
    }
    let keys = target_keys(&input)?;
    let bindings = binding_lookup(&input, &keys)?;
    let evidence = evidence_lookup(&input)?;
    let mut totals = evidence_totals(&evidence)?;
    let mut targets = Vec::with_capacity(keys.len());
    for key in keys {
        let binding = bindings.get(key).copied();
        let row = binding
            .and_then(|binding| binding.network_id)
            .and_then(|network| evidence.get(&network).copied());
        let target = coverage_target(key, binding, row);
        checked_add(&mut totals.ledger_targets, 1)?;
        checked_add(
            &mut totals.mapped_targets,
            u64::from(target.mapping_status == "resolved"),
        )?;
        checked_add(
            &mut totals.company_link_verified,
            u64::from(target.company_link_verified),
        )?;
        checked_add(
            &mut totals.directory_available,
            u64::from(target.directory_available),
        )?;
        checked_add(
            &mut totals.priceable,
            u64::from(target.priceable == Some(true)),
        )?;
        checked_add(
            &mut totals.targets_with_gaps,
            u64::from(!target.gaps.is_empty()),
        )?;
        targets.push(target);
    }
    Ok(RegistryNetworkCoverage { targets, totals })
}
