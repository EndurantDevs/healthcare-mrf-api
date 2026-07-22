// Licensed under the HealthPorta Non-Commercial License (see LICENSE).
use super::canonical::{sha256_bytes, stable_hash};
use super::contracts::{
    invalid_data, ProjectedResourceRow, ProjectionCopyContext, SEMANTIC_EVIDENCE_HASH_DOMAIN,
    SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID,
};
use super::fhir_values::{
    fhir_list, normalized_address, normalized_period, normalized_position, normalized_text,
    optional_text, profile_concepts, profile_contacts, profile_names,
};
use super::references::profile_references;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io;

const SUPPORTED_RESOURCE_TYPES: [&str; 8] = [
    "Endpoint",
    "HealthcareService",
    "InsurancePlan",
    "Location",
    "Organization",
    "OrganizationAffiliation",
    "Practitioner",
    "PractitionerRole",
];

pub fn project_resource(
    input_ordinal: usize,
    payload_json: Value,
    context: &ProjectionCopyContext,
) -> io::Result<(ProjectedResourceRow, Vec<u8>)> {
    let resource = payload_json.as_object().ok_or_else(|| {
        invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} must be an object"
        ))
    })?;
    let resource_type = resource
        .get("resourceType")
        .and_then(Value::as_str)
        .filter(|candidate| SUPPORTED_RESOURCE_TYPES.binary_search(candidate).is_ok())
        .ok_or_else(|| invalid_record(input_ordinal))?;
    let resource_id = resource
        .get("id")
        .and_then(Value::as_str)
        .filter(|candidate| is_fhir_id(candidate))
        .ok_or_else(|| invalid_record(input_ordinal))?;
    let canonical_payload = serde_json::to_vec(&payload_json).map_err(|error| {
        invalid_data(format!(
            "provider-directory FHIR resource {input_ordinal} cannot be canonicalized: {error}"
        ))
    })?;
    let payload_hash = sha256_bytes(&canonical_payload);
    let profile_evidence = profile_evidence(resource, resource_type)?;
    let expanded_evidence = expanded_profile_evidence(&profile_evidence);
    let typed_evidence = typed_evidence(resource_type, &expanded_evidence)?;
    let semantic_evidence_sha256 = stable_hash(&typed_evidence, SEMANTIC_EVIDENCE_HASH_DOMAIN);
    let addresses = expanded_evidence["addresses"]
        .as_array()
        .expect("expanded profile evidence arrays");
    let relationships = typed_evidence["relationships"]
        .as_array()
        .expect("typed relationship evidence array");
    let period = normalized_period(resource.get("period"))?;
    let period_object = period.as_ref().and_then(Value::as_object);
    let metadata = match resource.get("meta") {
        None | Some(Value::Null) => None,
        Some(Value::Object(metadata)) => Some(metadata),
        Some(_) => return Err(invalid_field("meta")),
    };
    let address_count = i32::try_from(addresses.len())
        .map_err(|_| invalid_data("provider-directory address count overflowed"))?;
    let relationship_count = i32::try_from(relationships.len())
        .map_err(|_| invalid_data("provider-directory relationship count overflowed"))?;
    let input_ordinal_u64 = u64::try_from(input_ordinal)
        .map_err(|_| invalid_data("provider-directory input ordinal overflowed"))?;
    let source_rank = format!(
        "{:020}:{payload_hash}:{:020}",
        context.partition_ordinal, input_ordinal_u64
    );
    let summary_npi = summary_npi(resource, resource_type, resource_id)?;
    let active = active_status(resource, resource_type)?;
    let effective_start = period_object.and_then(|period| normalized_text(period.get("start"), 64));
    let effective_end = period_object.and_then(|period| normalized_text(period.get("end"), 64));
    let observed_at = metadata
        .map(|metadata| optional_text(metadata, "lastUpdated", 64))
        .transpose()?
        .flatten();
    let profile_evidence_json =
        (!profile_evidence.is_empty()).then_some(Value::Object(profile_evidence));
    let summary_addressed_location = resource_type == "Location" && address_count > 0;
    let summary_geocoded_location = typed_evidence["geocodes"]
        .as_array()
        .is_some_and(|geocodes| !geocodes.is_empty());
    let summary_network_link_count = if resource_type == "InsurancePlan" {
        relationship_count
    } else {
        0
    };
    let summary_affiliation_link_count = if resource_type == "OrganizationAffiliation" {
        relationship_count
    } else {
        0
    };
    let resource_type = resource_type.to_owned();
    let resource_id = resource_id.to_owned();
    let projected = ProjectedResourceRow {
        physical_projection_id: context.recipe_id.clone(),
        resource_type,
        resource_id,
        proof_partition_id: context.partition_id.clone(),
        payload_hash,
        payload_json,
        source_rank,
        summary_npi,
        summary_address_count: address_count,
        summary_addressed_location,
        summary_geocoded_location,
        summary_network_link_count,
        summary_affiliation_link_count,
        active,
        effective_start,
        effective_end,
        observed_at,
        profile_evidence_json,
        semantic_evidence_sha256,
    };
    Ok((projected, canonical_payload))
}

fn profile_evidence(
    resource: &Map<String, Value>,
    resource_type: &str,
) -> io::Result<Map<String, Value>> {
    let entries = [
        ("names", profile_names(resource)?),
        ("specialties", profile_specialties(resource, resource_type)?),
        ("contacts", profile_contacts(resource, resource_type)?),
        ("addresses", profile_addresses(resource, resource_type)?),
        ("references", profile_references(resource, resource_type)?),
    ];
    Ok(entries
        .into_iter()
        .filter(|(_name, values)| !values.is_empty())
        .map(|(name, values)| (name.to_owned(), Value::Array(values)))
        .collect())
}

fn expanded_profile_evidence(profile: &Map<String, Value>) -> Value {
    Value::Object(
        [
            "names",
            "specialties",
            "contacts",
            "addresses",
            "references",
        ]
        .into_iter()
        .map(|name| {
            (
                name.to_owned(),
                profile
                    .get(name)
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new())),
            )
        })
        .collect(),
    )
}

fn profile_addresses(resource: &Map<String, Value>, resource_type: &str) -> io::Result<Vec<Value>> {
    if !matches!(
        resource_type,
        "HealthcareService" | "Location" | "Organization" | "Practitioner"
    ) {
        return Ok(Vec::new());
    }
    let mut addresses = match resource.get("address") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Array(entries)) => entries
            .iter()
            .map(normalized_address)
            .collect::<io::Result<Vec<_>>>()?,
        Some(Value::Object(_)) => vec![normalized_address(resource.get("address").unwrap())?],
        Some(_) => return Err(invalid_field("address")),
    };
    let position = normalized_position(resource.get("position"))?;
    if resource_type == "Location" && !addresses.is_empty() {
        if let Some(position) = position {
            addresses[0]
                .as_object_mut()
                .expect("normalized address object")
                .insert("geocode_evidence".to_owned(), position);
        }
    }
    Ok(addresses)
}

fn profile_specialties(
    resource: &Map<String, Value>,
    resource_type: &str,
) -> io::Result<Vec<Value>> {
    let mut specialties = match resource_type {
        "HealthcareService" | "OrganizationAffiliation" | "PractitionerRole" => {
            profile_concepts(resource.get("specialty"))?
        }
        _ => Vec::new(),
    };
    if resource_type == "Practitioner" {
        specialties.extend(practitioner_taxonomies(resource)?);
    }
    Ok(specialties)
}

fn practitioner_taxonomies(resource: &Map<String, Value>) -> io::Result<Vec<Value>> {
    let mut specialties = Vec::new();
    for qualification in fhir_list(resource, "qualification")? {
        let qualification = qualification
            .as_object()
            .ok_or_else(|| invalid_field("qualification"))?;
        for concept in profile_concepts(qualification.get("code"))? {
            let concept = concept.as_object().expect("normalized concept object");
            let taxonomies = concept
                .get("coding")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter(|coding| is_taxonomy_coding(coding))
                .cloned()
                .collect::<Vec<_>>();
            if taxonomies.is_empty() {
                continue;
            }
            let mut specialty = Map::new();
            if let Some(text) = concept.get("text") {
                specialty.insert("text".to_owned(), text.clone());
            }
            specialty.insert("coding".to_owned(), Value::Array(taxonomies));
            specialties.push(Value::Object(specialty));
        }
    }
    Ok(specialties)
}

fn is_taxonomy_coding(coding: &Value) -> bool {
    let Some(coding) = coding.as_object() else {
        return false;
    };
    let system = coding
        .get("system")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_lowercase();
    let code = coding.get("code").and_then(Value::as_str).unwrap_or("");
    system.contains("nucc")
        || system.contains("taxonomy")
        || (code.len() == 10 && code.bytes().all(|byte| byte.is_ascii_alphanumeric()))
}

fn typed_evidence(resource_type: &str, expanded_profile: &Value) -> io::Result<Value> {
    let profile = expanded_profile
        .as_object()
        .expect("expanded profile evidence object");
    let addresses = profile["addresses"]
        .as_array()
        .expect("profile addresses array");
    let references = profile["references"]
        .as_array()
        .expect("profile references array");
    let mut geocodes = Vec::new();
    for (ordinal, address) in addresses.iter().enumerate() {
        let Some(position) = address
            .as_object()
            .and_then(|address| address.get("geocode_evidence"))
        else {
            continue;
        };
        if resource_type != "Location" {
            return Err(invalid_data(
                "provider_directory_projection_geocode_evidence_invalid",
            ));
        }
        let position = position.as_object().ok_or_else(|| {
            invalid_data("provider_directory_projection_geocode_evidence_invalid")
        })?;
        let latitude = position
            .get("latitude_microdegrees")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                invalid_data("provider_directory_projection_geocode_evidence_invalid")
            })?;
        let longitude = position
            .get("longitude_microdegrees")
            .and_then(Value::as_i64)
            .ok_or_else(|| {
                invalid_data("provider_directory_projection_geocode_evidence_invalid")
            })?;
        let mut geocode = Map::new();
        geocode.insert("address_ordinal".to_owned(), Value::from(ordinal as u64));
        geocode.insert("latitude_microdegrees".to_owned(), Value::from(latitude));
        geocode.insert("longitude_microdegrees".to_owned(), Value::from(longitude));
        geocodes.push(Value::Object(geocode));
    }
    let mut relationships = Vec::new();
    let mut seen = HashSet::new();
    for (ordinal, reference) in references.iter().enumerate() {
        let Some(relationship) = reference
            .as_object()
            .and_then(|reference| reference.get("relationship_evidence"))
        else {
            continue;
        };
        let relationship = relationship.as_object().ok_or_else(|| {
            invalid_data("provider_directory_projection_relationship_evidence_invalid")
        })?;
        let kind = relationship
            .get("kind")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid_data("provider_directory_projection_relationship_evidence_invalid")
            })?;
        let target_type = relationship
            .get("target_resource_type")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid_data("provider_directory_projection_relationship_evidence_invalid")
            })?;
        let target_id = relationship
            .get("target_resource_id")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                invalid_data("provider_directory_projection_relationship_evidence_invalid")
            })?;
        if !seen.insert((kind, target_type, target_id)) {
            return Err(invalid_data(
                "provider_directory_projection_relationship_evidence_invalid",
            ));
        }
        let mut typed = Map::new();
        typed.insert("reference_ordinal".to_owned(), Value::from(ordinal as u64));
        typed.insert("kind".to_owned(), Value::String(kind.to_owned()));
        typed.insert(
            "target_resource_type".to_owned(),
            Value::String(target_type.to_owned()),
        );
        typed.insert(
            "target_resource_id".to_owned(),
            Value::String(target_id.to_owned()),
        );
        relationships.push(Value::Object(typed));
    }
    let mut typed = Map::new();
    typed.insert(
        "contract_id".to_owned(),
        Value::String(SEMANTIC_TYPED_EVIDENCE_CONTRACT_ID.to_owned()),
    );
    typed.insert("geocodes".to_owned(), Value::Array(geocodes));
    typed.insert("relationships".to_owned(), Value::Array(relationships));
    Ok(Value::Object(typed))
}

fn summary_npi(
    resource: &Map<String, Value>,
    resource_type: &str,
    resource_id: &str,
) -> io::Result<Option<i64>> {
    if !matches!(
        resource_type,
        "HealthcareService" | "Organization" | "Practitioner" | "PractitionerRole"
    ) {
        return Ok(None);
    }
    let mut candidates = Vec::new();
    for (ordinal, identifier) in fhir_list(resource, "identifier")?.iter().enumerate() {
        let identifier = identifier
            .as_object()
            .ok_or_else(|| invalid_field("identifier"))?;
        let Some(value) = optional_text(identifier, "value", 64)? else {
            continue;
        };
        let system = identifier
            .get("system")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_lowercase();
        let descriptor = format!("{system} {}", identifier_type_text(identifier)?);
        if !descriptor.contains("npi") && !descriptor.contains("national provider") {
            continue;
        }
        let digits = value
            .chars()
            .filter(|character| character.is_ascii_digit())
            .collect::<String>();
        let npi = digits.parse::<i64>().ok().filter(valid_npi);
        if digits.len() == 10 {
            if let Some(npi) = npi {
                candidates.push((system != "http://hl7.org/fhir/sid/us-npi", ordinal, npi));
            }
        }
    }
    if let Some((_system_rank, _ordinal, npi)) = candidates.into_iter().min() {
        return Ok(Some(npi));
    }
    if matches!(resource_type, "Organization" | "Practitioner")
        && resource_id.len() == 10
        && resource_id.bytes().all(|byte| byte.is_ascii_digit())
    {
        return Ok(resource_id.parse::<i64>().ok().filter(valid_npi));
    }
    Ok(None)
}

fn identifier_type_text(identifier: &Map<String, Value>) -> io::Result<String> {
    let Some(identifier_type) = identifier.get("type") else {
        return Ok(String::new());
    };
    if identifier_type.is_null() {
        return Ok(String::new());
    }
    let identifier_type = identifier_type
        .as_object()
        .ok_or_else(|| invalid_field("identifier_type"))?;
    let mut parts = vec![json_scalar_text(identifier_type.get("text"))];
    for coding in fhir_list(identifier_type, "coding")? {
        let coding = coding
            .as_object()
            .ok_or_else(|| invalid_field("identifier_type"))?;
        for field in ["system", "code", "display"] {
            parts.push(json_scalar_text(coding.get(field)));
        }
    }
    Ok(parts.join(" ").to_lowercase())
}

fn json_scalar_text(value: Option<&Value>) -> String {
    match value {
        None | Some(Value::Null) => String::new(),
        Some(Value::String(text)) => text.clone(),
        Some(value) => value.to_string(),
    }
}

fn valid_npi(npi: &i64) -> bool {
    (1_000_000_000..=2_999_999_999).contains(npi)
}

fn active_status(resource: &Map<String, Value>, resource_type: &str) -> io::Result<Option<bool>> {
    let enumerated: Option<&[(&str, bool)]> = match resource_type {
        "Endpoint" => Some(&[
            ("active", true),
            ("suspended", false),
            ("error", false),
            ("off", false),
            ("entered-in-error", false),
            ("test", false),
        ]),
        "InsurancePlan" => Some(&[
            ("active", true),
            ("draft", false),
            ("retired", false),
            ("unknown", false),
        ]),
        "Location" => Some(&[("active", true), ("suspended", false), ("inactive", false)]),
        _ => None,
    };
    if let Some(outcomes) = enumerated {
        let Some(status) = optional_text(resource, "status", 64)? else {
            return Ok(None);
        };
        return outcomes
            .iter()
            .find_map(|(candidate, active)| (*candidate == status).then_some(Some(*active)))
            .ok_or_else(|| invalid_field("status"));
    }
    match resource.get("active") {
        None => Ok(None),
        Some(Value::Bool(active)) => Ok(Some(*active)),
        Some(_) => Err(invalid_field("active")),
    }
}

fn is_fhir_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= 64
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
}

fn invalid_record(input_ordinal: usize) -> io::Error {
    invalid_data(format!(
        "provider-directory FHIR resource {input_ordinal} has invalid identity"
    ))
}

fn invalid_field(field: &str) -> io::Error {
    invalid_data(format!(
        "provider_directory_projection_fhir_{field}_invalid"
    ))
}
