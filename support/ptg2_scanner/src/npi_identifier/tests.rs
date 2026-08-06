use super::{npi_validity, NpiValidity};
use serde::Deserialize;
use std::collections::BTreeSet;

const FROZEN_VECTORS: &str = include_str!("../../tests/fixtures/npi_identifier_vectors_v1.json");
const CONTRACT_ID: &str = "healthporta.npi-identifier-classification.v1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorContract {
    contract_id: String,
    version: u64,
    cases: Vec<VectorCase>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorCase {
    id: String,
    value: String,
    classification: String,
}

#[test]
fn frozen_vectors_cover_every_classification() {
    let contract: VectorContract = serde_json::from_str(FROZEN_VECTORS).unwrap();
    assert_eq!(contract.contract_id, CONTRACT_ID);
    assert_eq!(contract.version, 1);
    assert!(!contract.cases.is_empty());

    let mut case_ids = BTreeSet::new();
    let mut classifications = BTreeSet::new();
    for case in contract.cases {
        assert!(case_ids.insert(case.id));
        classifications.insert(case.classification.clone());
        assert_eq!(
            classification_name(npi_validity(&case.value)),
            case.classification
        );
    }
    assert_eq!(
        classifications,
        BTreeSet::from([
            "checksum_invalid".to_owned(),
            "invalid".to_owned(),
            "structural_invalid".to_owned(),
            "valid".to_owned(),
        ])
    );
}

#[test]
fn representation_is_exact_ascii_without_trimming() {
    assert_eq!(npi_validity(" 1000000491"), NpiValidity::Invalid);
    assert_eq!(npi_validity("1000000491 "), NpiValidity::Invalid);
    assert_eq!(npi_validity("10000004-1"), NpiValidity::Invalid);
}

fn classification_name(validity: NpiValidity) -> &'static str {
    match validity {
        NpiValidity::Valid => "valid",
        NpiValidity::ChecksumInvalid => "checksum_invalid",
        NpiValidity::StructuralInvalid => "structural_invalid",
        NpiValidity::Invalid => "invalid",
    }
}
