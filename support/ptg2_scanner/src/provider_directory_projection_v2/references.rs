// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::contracts::invalid_data;
use super::fhir_values::{fhir_list, normalized_text, optional_text};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::io;

const NETWORK_REFERENCE_URL: &str =
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference";
const PARTICIPATING_NETWORK_URL: &str =
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-participatingnetwork-extension";

pub fn profile_references(
    resource: &Map<String, Value>,
    resource_type: &str,
) -> io::Result<Vec<Value>> {
    let mut references = Vec::new();
    for field in reference_fields(resource_type) {
        references.extend(
            reference_entries(resource.get(*field))?
                .into_iter()
                .map(normalized_reference)
                .collect::<io::Result<Vec<_>>>()?,
        );
    }
    let extension_references = extension_network_references(resource.get("extension"))?;
    references.extend(
        extension_references
            .iter()
            .map(|reference| normalized_reference(reference))
            .collect::<io::Result<Vec<_>>>()?,
    );
    references.extend(relationship_evidence(resource, resource_type)?);
    deduplicate_values(references)
}

fn reference_fields(resource_type: &str) -> &'static [&'static str] {
    match resource_type {
        "Endpoint" => &["managingOrganization"],
        "HealthcareService" => &["providedBy", "location", "coverageArea", "endpoint"],
        "InsurancePlan" => &[
            "ownedBy",
            "administeredBy",
            "network",
            "coverageArea",
            "endpoint",
        ],
        "Location" => &["managingOrganization", "partOf", "endpoint"],
        "Organization" => &["partOf", "endpoint"],
        "OrganizationAffiliation" => &[
            "organization",
            "participatingOrganization",
            "network",
            "location",
            "healthcareService",
            "endpoint",
        ],
        "PractitionerRole" => &[
            "practitioner",
            "organization",
            "location",
            "healthcareService",
            "endpoint",
            "network",
            "insurancePlan",
        ],
        _ => &[],
    }
}

fn reference_entries(value: Option<&Value>) -> io::Result<Vec<&Map<String, Value>>> {
    match value {
        None | Some(Value::Null) => Ok(Vec::new()),
        Some(Value::Object(object)) => Ok(vec![object]),
        Some(Value::Array(entries)) => entries
            .iter()
            .map(|entry| entry.as_object().ok_or_else(|| invalid_field("reference")))
            .collect(),
        Some(_) => Err(invalid_field("reference")),
    }
}

fn normalized_reference(reference: &Map<String, Value>) -> io::Result<Value> {
    let mut normalized = Map::new();
    for field in ["reference", "type", "display", "id"] {
        if let Some(text) = optional_text(reference, field, 2048)? {
            normalized.insert(field.to_owned(), Value::String(text));
        }
    }
    if let Some(identifier) = reference.get("identifier") {
        if !identifier.is_null() {
            let identifier = identifier
                .as_object()
                .ok_or_else(|| invalid_field("reference_identifier"))?;
            let mut normalized_identifier = Map::new();
            for field in ["system", "value"] {
                if let Some(text) = optional_text(identifier, field, 2048)? {
                    normalized_identifier.insert(field.to_owned(), Value::String(text));
                }
            }
            if !normalized_identifier.contains_key("value") {
                return Err(invalid_field("reference_identifier"));
            }
            normalized.insert(
                "identifier".to_owned(),
                Value::Object(normalized_identifier),
            );
        }
    }
    if !(normalized.contains_key("reference")
        || normalized.contains_key("identifier")
        || (normalized.contains_key("type") && normalized.contains_key("id")))
    {
        return Err(invalid_field("reference"));
    }
    Ok(Value::Object(normalized))
}

fn normalized_extension_url(value: Option<&Value>) -> Option<String> {
    let text = normalized_text(value, 2048)?;
    let versionless = text.split('|').next().unwrap_or("").trim_end_matches('/');
    let mut normalized = versionless.to_lowercase();
    if let Some(remainder) = normalized.strip_prefix("https://") {
        normalized = format!("http://{remainder}");
    }
    Some(normalized)
}

fn extension_network_references(
    extension_value: Option<&Value>,
) -> io::Result<Vec<&Map<String, Value>>> {
    let mut references = Vec::new();
    visit_extensions(extension_value, &mut references)?;
    Ok(references)
}

fn visit_extensions<'a>(
    extension_value: Option<&'a Value>,
    references: &mut Vec<&'a Map<String, Value>>,
) -> io::Result<()> {
    let Some(extension_value) = extension_value else {
        return Ok(());
    };
    if extension_value.is_null() {
        return Ok(());
    }
    let entries = match extension_value {
        Value::Array(entries) => entries.iter().collect::<Vec<_>>(),
        value => vec![value],
    };
    for entry in entries {
        let extension = entry
            .as_object()
            .ok_or_else(|| invalid_field("extension"))?;
        let recognized = normalized_extension_url(extension.get("url")).is_some_and(|url| {
            matches!(
                url.as_str(),
                NETWORK_REFERENCE_URL | PARTICIPATING_NETWORK_URL
            )
        });
        if recognized {
            let found = reference_entries(extension.get("valueReference"))?;
            if found.is_empty() {
                return Err(invalid_field("network_extension"));
            }
            references.extend(found);
        }
        visit_extensions(extension.get("extension"), references)?;
    }
    Ok(())
}

fn relationship_evidence(
    resource: &Map<String, Value>,
    resource_type: &str,
) -> io::Result<Vec<Value>> {
    let contracts: &[(&str, &str, &str)] = match resource_type {
        "InsurancePlan" => &[("network", "network", "Organization")],
        "OrganizationAffiliation" => &[
            ("organization", "organization", "Organization"),
            (
                "participatingOrganization",
                "participating_organization",
                "Organization",
            ),
            ("network", "network", "Organization"),
            ("location", "location", "Location"),
            (
                "healthcareService",
                "healthcare_service",
                "HealthcareService",
            ),
            ("endpoint", "endpoint", "Endpoint"),
        ],
        _ => &[],
    };
    let mut evidence = Vec::new();
    let mut seen = HashSet::new();
    for (field, kind, target_type) in contracts {
        for reference in relationship_references(resource, resource_type, field)? {
            let target_id = reference_target_id(reference.get("reference"), target_type)
                .ok_or_else(|| invalid_field("relationship_reference"))?;
            if !seen.insert((
                (*kind).to_owned(),
                (*target_type).to_owned(),
                target_id.clone(),
            )) {
                continue;
            }
            let mut relationship = Map::new();
            relationship.insert("kind".to_owned(), Value::String((*kind).to_owned()));
            relationship.insert(
                "target_resource_type".to_owned(),
                Value::String((*target_type).to_owned()),
            );
            relationship.insert("target_resource_id".to_owned(), Value::String(target_id));
            let mut envelope = Map::new();
            envelope.insert(
                "relationship_evidence".to_owned(),
                Value::Object(relationship),
            );
            evidence.push(Value::Object(envelope));
        }
    }
    Ok(evidence)
}

fn relationship_references<'a>(
    resource: &'a Map<String, Value>,
    resource_type: &str,
    field: &str,
) -> io::Result<Vec<&'a Map<String, Value>>> {
    let mut references = reference_entries(resource.get(field))?;
    if field == "network" {
        references.extend(extension_network_references(resource.get("extension"))?);
    }
    if resource_type == "InsurancePlan" && field == "network" {
        for backbone in ["plan", "coverage"] {
            for entry in fhir_list(resource, backbone)? {
                let object = entry.as_object().ok_or_else(|| invalid_field(backbone))?;
                references.extend(reference_entries(object.get("network"))?);
            }
        }
    }
    Ok(references)
}

fn reference_target_id(value: Option<&Value>, expected_type: &str) -> Option<String> {
    let reference = normalized_text(value, 2048)?;
    let clean = reference
        .split('#')
        .next()
        .unwrap_or("")
        .split('?')
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    let mut segments = clean
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>();
    if segments.len() == 1 && is_fhir_id(segments[0]) {
        return Some(segments[0].to_owned());
    }
    if segments.len() >= 2 && segments[segments.len() - 2] == "_history" {
        segments.truncate(segments.len() - 2);
    }
    segments.windows(2).rev().find_map(|pair| {
        (pair[0] == expected_type && is_fhir_id(pair[1])).then(|| pair[1].to_owned())
    })
}

fn is_fhir_id(candidate: &str) -> bool {
    !candidate.is_empty()
        && candidate.len() <= 64
        && candidate
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
}

fn deduplicate_values(values: Vec<Value>) -> io::Result<Vec<Value>> {
    let mut seen = HashSet::new();
    let mut unique = Vec::new();
    for value in values {
        let key =
            serde_json::to_vec(&value).expect("serde_json::Value serialization is infallible");
        if seen.insert(key) {
            unique.push(value);
        }
    }
    Ok(unique)
}

fn invalid_field(field: &str) -> io::Error {
    invalid_data(format!(
        "provider_directory_projection_fhir_{field}_invalid"
    ))
}
