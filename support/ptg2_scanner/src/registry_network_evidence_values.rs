// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
//! Bounded syntax for explicit network pricing and benefit references.
//!
//! Parsing preserves unresolved (`null`) and reviewed-none (`[]`) references.
//! It does not verify network association, approval, physical facts, caller
//! authorization or retention. Those remain separate protected caller gates.

use serde::de::{self, Deserialize, Deserializer, SeqAccess, Visitor};
use serde::{Deserialize as DeriveDeserialize, Serialize};
use serde_json::value::RawValue;
use std::fmt;

pub const MAX_EVIDENCE_BYTES: usize = 16 * 1024;
pub const MAX_REFERENCES_PER_KIND: usize = 16;
pub const ERROR_CODE: &str = "registry_network_evidence_invalid";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RegistryNetworkEvidenceError;

impl fmt::Display for RegistryNetworkEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(ERROR_CODE)
    }
}

impl std::error::Error for RegistryNetworkEvidenceError {}

type Result<T> = std::result::Result<T, RegistryNetworkEvidenceError>;

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RegistryNetworkEvidence {
    pub network_id: i32,
    pub expected_record_revision: i64,
    pub pricing_refs: Option<Vec<PricingReference>>,
    pub benefit_refs: Option<Vec<BenefitReference>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, DeriveDeserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PricingRole {
    InNetwork,
    AllowedAmounts,
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveDeserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct PricingReference {
    pub healthporta_plan_id: String,
    pub plan_release_id: String,
    pub serving_revision_id: String,
    pub role: PricingRole,
    #[serde(deserialize_with = "integer_i32")]
    pub ordinal: i32,
    pub snapshot_id: String,
}

#[derive(Clone, Debug, Eq, PartialEq, DeriveDeserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct BenefitReference {
    pub healthporta_plan_id: String,
    pub plan_version_id: String,
    pub observation_id: String,
    pub alias_binding_id: String,
    pub provenance_id: String,
    pub semantic_digest: String,
    pub observation_digest: String,
    pub document_sha256: String,
}

#[derive(DeriveDeserialize)]
#[serde(deny_unknown_fields)]
struct BorrowedEvidence<'a> {
    #[serde(deserialize_with = "integer_i32")]
    network_id: i32,
    #[serde(deserialize_with = "integer_i64")]
    expected_record_revision: i64,
    #[serde(borrow)]
    pricing_refs: &'a RawValue,
    #[serde(borrow)]
    benefit_refs: &'a RawValue,
}

// Parse an integer token directly: serde's arbitrary-precision typed decoder
// rejects JSON -0, which the controller correctly treats as integer zero.
fn integer_i32<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<i32, D::Error> {
    let raw: &RawValue = Deserialize::deserialize(deserializer)?;
    raw.get().parse().map_err(|_| de::Error::custom(ERROR_CODE))
}

fn integer_i64<'de, D: Deserializer<'de>>(deserializer: D) -> std::result::Result<i64, D::Error> {
    let raw: &RawValue = Deserialize::deserialize(deserializer)?;
    raw.get().parse().map_err(|_| de::Error::custom(ERROR_CODE))
}

// Count borrowed elements before allocating any owned reference vector or text.
// Missing fields and duplicate/unknown keys are rejected by the closed structs.
struct ReferenceCount;

impl<'de> Deserialize<'de> for ReferenceCount {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> std::result::Result<Self, D::Error> {
        struct CountVisitor;
        impl<'de> Visitor<'de> for CountVisitor {
            type Value = ReferenceCount;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bounded reference array")
            }

            fn visit_seq<A: SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                for _ in 0..MAX_REFERENCES_PER_KIND {
                    if sequence.next_element::<&'de RawValue>()?.is_none() {
                        return Ok(ReferenceCount);
                    }
                }
                if sequence.next_element::<de::IgnoredAny>()?.is_some() {
                    return Err(de::Error::custom(ERROR_CODE));
                }
                Ok(ReferenceCount)
            }
        }
        deserializer.deserialize_seq(CountVisitor)
    }
}

fn bounded_references(raw: &RawValue) -> Result<()> {
    if raw.get() != "null" {
        serde_json::from_str::<ReferenceCount>(raw.get())
            .map_err(|_| RegistryNetworkEvidenceError)?;
    }
    Ok(())
}

fn owned_references<T>(raw: &RawValue, validate: impl Fn(&T) -> bool) -> Result<Option<Vec<T>>>
where
    T: for<'de> Deserialize<'de> + PartialEq,
{
    if raw.get() == "null" {
        return Ok(None);
    }
    let references: Vec<T> =
        serde_json::from_str(raw.get()).map_err(|_| RegistryNetworkEvidenceError)?;
    for (index, reference) in references.iter().enumerate() {
        if !validate(reference) || references[..index].contains(reference) {
            return Err(RegistryNetworkEvidenceError);
        }
    }
    Ok(Some(references))
}

// Python str.strip also includes the four information separators.
fn python_whitespace(character: char) -> bool {
    character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}')
}

fn bounded_text(text: &str) -> bool {
    !text.is_empty()
        && text.chars().count() <= 96
        && text.trim_matches(python_whitespace) == text
        && !text.contains('\0')
}

fn registry_id(text: &str, prefix: &str) -> bool {
    bounded_text(text)
        && text.strip_prefix(prefix).is_some_and(|suffix| {
            suffix.len() == 26
                && suffix.bytes().all(|byte| {
                    matches!(byte, b'0'..=b'9' | b'A'..=b'H' | b'J'..=b'K' | b'M'..=b'N' | b'P'..=b'T' | b'V'..=b'Z')
                })
        })
}

fn opaque_id(text: &str, prefix: &str) -> bool {
    bounded_text(text)
        && text.strip_prefix(prefix).is_some_and(|suffix| {
            (26..=32).contains(&suffix.len())
                && suffix.bytes().all(|byte| byte.is_ascii_alphanumeric())
        })
}

fn hex_digest(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn pricing_reference(reference: &PricingReference) -> bool {
    registry_id(&reference.healthporta_plan_id, "hpplan_")
        && registry_id(&reference.plan_release_id, "hprelease_")
        && registry_id(&reference.serving_revision_id, "hpserve_")
        && reference.ordinal >= 0
        && bounded_text(&reference.snapshot_id)
}

fn benefit_reference(reference: &BenefitReference) -> bool {
    registry_id(&reference.healthporta_plan_id, "hpplan_")
        && registry_id(&reference.plan_version_id, "hpversion_")
        && opaque_id(&reference.observation_id, "hpobs_")
        && opaque_id(&reference.alias_binding_id, "hpbinding_")
        && opaque_id(&reference.provenance_id, "hpprov_")
        && hex_digest(&reference.semantic_digest)
        && hex_digest(&reference.observation_digest)
        && hex_digest(&reference.document_sha256)
}

/// Parse one complete evidence object without authenticating its association.
/// Raw bytes and both array counts are checked before owning reference values.
/// Every refusal returns the same static, value-free error.
pub fn parse_registry_network_evidence(raw: &[u8]) -> Result<RegistryNetworkEvidence> {
    if raw.is_empty() || raw.len() > MAX_EVIDENCE_BYTES {
        return Err(RegistryNetworkEvidenceError);
    }
    let borrowed: BorrowedEvidence<'_> =
        serde_json::from_slice(raw).map_err(|_| RegistryNetworkEvidenceError)?;
    if borrowed.network_id <= 0 || borrowed.expected_record_revision <= 0 {
        return Err(RegistryNetworkEvidenceError);
    }
    // Validate both bounds before allocating either owned array.
    bounded_references(borrowed.pricing_refs)?;
    bounded_references(borrowed.benefit_refs)?;
    Ok(RegistryNetworkEvidence {
        network_id: borrowed.network_id,
        expected_record_revision: borrowed.expected_record_revision,
        pricing_refs: owned_references(borrowed.pricing_refs, pricing_reference)?,
        benefit_refs: owned_references(borrowed.benefit_refs, benefit_reference)?,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    fn pricing() -> Value {
        json!({"healthporta_plan_id": format!("hpplan_{}", "0".repeat(26)),
            "plan_release_id": format!("hprelease_{}", "A".repeat(26)),
            "serving_revision_id": format!("hpserve_{}", "Z".repeat(26)),
            "role": "in_network", "ordinal": 0, "snapshot_id": "snapshot-one"})
    }

    fn benefit() -> Value {
        json!({"healthporta_plan_id": format!("hpplan_{}", "0".repeat(26)),
            "plan_version_id": format!("hpversion_{}", "A".repeat(26)),
            "observation_id": format!("hpobs_{}", "a0Z".repeat(9)),
            "alias_binding_id": format!("hpbinding_{}", "b".repeat(32)),
            "provenance_id": format!("hpprov_{}", "C".repeat(26)),
            "semantic_digest": "a".repeat(64), "observation_digest": "0".repeat(64),
            "document_sha256": "f".repeat(64)})
    }

    fn evidence() -> Value {
        json!({"network_id": 7, "expected_record_revision": 3,
            "pricing_refs": [pricing()], "benefit_refs": [benefit()]})
    }

    fn parse(document: &Value) -> Result<RegistryNetworkEvidence> {
        parse_registry_network_evidence(&serde_json::to_vec(document).unwrap())
    }

    #[test]
    fn preflight_borrows_values_and_checks_both_array_counts() {
        let raw = serde_json::to_vec(&evidence()).unwrap();
        let borrowed: BorrowedEvidence<'_> = serde_json::from_slice(&raw).unwrap();
        let input_range = raw.as_ptr() as usize..raw.as_ptr() as usize + raw.len();
        for reference in [borrowed.pricing_refs, borrowed.benefit_refs] {
            assert!(input_range.contains(&(reference.get().as_ptr() as usize)));
            bounded_references(reference).unwrap();
        }
        let too_many = serde_json::to_string(&vec![Value::Null; 17]).unwrap();
        let raw: &RawValue = serde_json::from_str(&too_many).unwrap();
        assert!(bounded_references(raw).is_err());
    }

    #[test]
    fn full_reference_round_trip_preserves_closed_values() {
        let document = evidence();
        let parsed = parse(&document).unwrap();
        assert_eq!(serde_json::to_value(parsed).unwrap(), document);
    }

    #[test]
    fn unresolved_and_reviewed_none_are_distinct_and_required() {
        for pricing in [Value::Null, json!([])] {
            for benefit in [Value::Null, json!([])] {
                let document = json!({"network_id": 1, "expected_record_revision": 1,
                    "pricing_refs": pricing, "benefit_refs": benefit});
                let parsed = parse(&document).unwrap();
                assert_eq!(serde_json::to_value(parsed).unwrap(), document);
            }
        }
        for field in [
            "network_id",
            "expected_record_revision",
            "pricing_refs",
            "benefit_refs",
        ] {
            let mut document = evidence();
            document.as_object_mut().unwrap().remove(field);
            assert!(parse(&document).is_err());
        }
    }

    #[test]
    fn unknown_and_missing_reference_fields_refuse() {
        for kind in ["pricing_refs", "benefit_refs"] {
            let baseline = evidence();
            for field in baseline[kind][0].as_object().unwrap().keys() {
                let mut document = baseline.clone();
                document[kind][0].as_object_mut().unwrap().remove(field);
                assert!(parse(&document).is_err());
            }
            let mut document = baseline;
            document[kind][0]["verified"] = json!(true);
            assert!(parse(&document).is_err());
        }
        let mut document = evidence();
        document["approval"] = json!(true);
        assert!(parse(&document).is_err());
    }

    #[test]
    fn duplicate_keys_including_escaped_keys_refuse() {
        let raw = r#"{"network_id":1,"network_id":1,"expected_record_revision":1,"pricing_refs":null,"benefit_refs":null}"#;
        assert!(parse_registry_network_evidence(raw.as_bytes()).is_err());
        for field in ["pricing_refs", "benefit_refs"] {
            let duplicate_null = format!("{{\"network_id\":1,\"expected_record_revision\":1,\"pricing_refs\":null,\"benefit_refs\":null,\"{field}\":null}}");
            assert!(parse_registry_network_evidence(duplicate_null.as_bytes()).is_err());
        }
        let raw = raw.replace(
            "\"network_id\":1,\"network_id\"",
            "\"network_id\":1,\"network_\\u0069d\"",
        );
        assert!(parse_registry_network_evidence(raw.as_bytes()).is_err());
        let document = serde_json::to_string(&evidence()).unwrap();
        for field in ["snapshot_id", "document_sha256"] {
            let needle = format!("\"{field}\":");
            let duplicate = document.replace(&needle, &format!("\"{field}\":null,{needle}"));
            assert!(parse_registry_network_evidence(duplicate.as_bytes()).is_err());
        }
    }

    #[test]
    fn exact_duplicate_refs_and_wrong_array_shapes_refuse() {
        let first = serde_json::to_string(&pricing()).unwrap();
        let escaped = first.replace("hpplan_", "\\u0068pplan_");
        let duplicate = format!("{{\"network_id\":1,\"expected_record_revision\":1,\"pricing_refs\":[{first},{escaped}],\"benefit_refs\":null}}");
        assert!(parse_registry_network_evidence(duplicate.as_bytes()).is_err());
        for kind in ["pricing_refs", "benefit_refs"] {
            let mut document = evidence();
            let item = document[kind][0].clone();
            document[kind] = json!([item.clone(), item]);
            assert!(parse(&document).is_err());
            for wrong in [
                json!({}),
                json!(false),
                json!(1),
                json!("null"),
                json!([null]),
            ] {
                document[kind] = wrong;
                assert!(parse(&document).is_err());
            }
        }
    }

    #[test]
    fn arrays_are_bounded_but_distinct_binding_ordinals_are_valid() {
        let mut document = evidence();
        document["pricing_refs"] = Value::Array(
            (0..16)
                .map(|ordinal| {
                    let mut reference = pricing();
                    reference["ordinal"] = json!(ordinal);
                    reference
                })
                .collect(),
        );
        assert_eq!(parse(&document).unwrap().pricing_refs.unwrap().len(), 16);
        document["pricing_refs"]
            .as_array_mut()
            .unwrap()
            .push(pricing());
        assert!(parse(&document).is_err());
        // Count refusal also happens for malformed items, before any typed ownership parse.
        document["benefit_refs"] = Value::Array(vec![Value::Null; 17]);
        assert!(parse(&document).is_err());
    }

    #[test]
    fn integer_bounds_and_types_are_exact() {
        let mut document = evidence();
        document["network_id"] = json!(i32::MAX);
        document["expected_record_revision"] = json!(i64::MAX);
        document["pricing_refs"][0]["ordinal"] = json!(i32::MAX);
        assert!(parse(&document).is_ok());
        for (field, invalid) in [
            (
                "network_id",
                vec![
                    json!(0),
                    json!(-1),
                    json!(i32::MAX as i64 + 1),
                    json!(true),
                    json!(1.0),
                    json!("1"),
                ],
            ),
            (
                "expected_record_revision",
                vec![
                    json!(0),
                    json!(-1),
                    json!(u64::MAX),
                    json!(false),
                    json!(1.0),
                    json!("1"),
                ],
            ),
        ] {
            for number in invalid {
                let mut document = evidence();
                document[field] = number;
                assert!(parse(&document).is_err());
            }
        }
        for number in [
            json!(-1),
            json!(i32::MAX as i64 + 1),
            json!(false),
            json!(1.0),
            json!("0"),
        ] {
            let mut document = evidence();
            document["pricing_refs"][0]["ordinal"] = number;
            assert!(parse(&document).is_err());
        }
        let raw = serde_json::to_string(&evidence())
            .unwrap()
            .replace("\"ordinal\":0", "\"ordinal\":-0");
        assert!(parse_registry_network_evidence(raw.as_bytes()).is_ok());
        assert!(parse_registry_network_evidence(raw.replace("-0", "0e0").as_bytes()).is_err());
    }

    #[test]
    fn prefix_alphabet_width_and_role_substitutions_refuse() {
        for (kind, field, wrong) in [
            (
                "pricing_refs",
                "healthporta_plan_id",
                format!("hpversion_{}", "A".repeat(26)),
            ),
            (
                "pricing_refs",
                "plan_release_id",
                format!("hprelease_{}", "I".repeat(26)),
            ),
            (
                "pricing_refs",
                "serving_revision_id",
                format!("hpserve_{}", "a".repeat(26)),
            ),
            (
                "benefit_refs",
                "plan_version_id",
                format!("hpversion_{}", "A".repeat(25)),
            ),
            (
                "benefit_refs",
                "observation_id",
                format!("hpobs_{}", "a".repeat(33)),
            ),
            (
                "benefit_refs",
                "alias_binding_id",
                format!("hpobs_{}", "a".repeat(26)),
            ),
            (
                "benefit_refs",
                "provenance_id",
                format!("hpprov_{}", "_".repeat(26)),
            ),
            ("pricing_refs", "role", "IN_NETWORK".to_string()),
        ] {
            let mut document = evidence();
            document[kind][0][field] = json!(wrong);
            assert!(parse(&document).is_err());
        }
        let mut document = evidence();
        document["pricing_refs"][0]["role"] = json!("allowed_amounts");
        assert!(parse(&document).is_ok());
    }

    #[test]
    fn lowercase_hex_and_opaque_snapshot_semantics_match() {
        for field in ["semantic_digest", "observation_digest", "document_sha256"] {
            for wrong in ["A".repeat(64), "a".repeat(63), "g".repeat(64)] {
                let mut document = evidence();
                document["benefit_refs"][0][field] = json!(wrong);
                assert!(parse(&document).is_err());
            }
        }
        for valid in ["é".repeat(96), "opaque interior\tvalue".to_string()] {
            let mut document = evidence();
            document["pricing_refs"][0]["snapshot_id"] = json!(valid);
            assert!(parse(&document).is_ok());
        }
        for invalid in [
            String::new(),
            "é".repeat(97),
            " value".to_string(),
            "value\u{001c}".to_string(),
            "value\u{0085}".to_string(),
            "nul\0value".to_string(),
        ] {
            let mut document = evidence();
            document["pricing_refs"][0]["snapshot_id"] = json!(invalid);
            assert!(parse(&document).is_err());
        }
    }

    #[test]
    fn raw_bounds_invalid_utf8_and_nested_shapes_are_value_free() {
        let mut raw = serde_json::to_vec(&evidence()).unwrap();
        raw.resize(MAX_EVIDENCE_BYTES, b' ');
        assert!(parse_registry_network_evidence(&raw).is_ok());
        raw.push(b' ');
        assert_eq!(
            parse_registry_network_evidence(&raw)
                .unwrap_err()
                .to_string(),
            ERROR_CODE
        );
        for raw in [
            b"".as_slice(),
            b"\xff".as_slice(),
            b"[]".as_slice(),
            b"null".as_slice(),
            b"{} {}".as_slice(),
        ] {
            assert_eq!(
                parse_registry_network_evidence(raw)
                    .unwrap_err()
                    .to_string(),
                ERROR_CODE
            );
        }
        let mut document = evidence();
        document["pricing_refs"] = json!([[[[]]]]);
        assert!(parse(&document).is_err());
    }
}
