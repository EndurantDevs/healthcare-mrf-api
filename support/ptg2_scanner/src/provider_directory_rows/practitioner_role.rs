//! The PractitionerRole mapping of `parse_fhir_resource`, after the shared base fields.

use super::fields::{
    ascii_lower, clean_text, codings, dict_items, explicit_npi, extension_url, first_reference,
    loop_items, organization_reference_identity, period, references, string_list,
};
use super::value::{get, Fields, Value};
use super::{unsupported, Unsupported};

const CMS_NPD_SOURCE_ID: &str = "cms-npd";
const NETWORK_REFERENCE_EXTENSION_URLS: [&str; 3] = [
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/network-reference",
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-participatingnetwork-extension",
];
const NEW_PATIENTS_EXTENSION_URLS: [&str; 4] = [
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/newpatients",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/newpatients",
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-newpatients-extension",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/plannet-newpatients-extension",
];
const ACCEPTING_PATIENTS_CODES: [&str; 4] = ["nopt", "newpt", "existptonly", "existptfam"];
const DELIVERY_METHOD_EXTENSION_URLS: [&str; 2] = [
    "http://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/delivery-method",
    "https://hl7.org/fhir/us/davinci-pdex-plan-net/structuredefinition/delivery-method",
];
const TELEHEALTH_EXTENSION_URL: &str = "https://healthporta.com/fhir/provider-directory/telehealth";
const ACCEPTING_MEDICAID_EXTENSION_URL: &str =
    "https://healthporta.com/fhir/provider-directory/accepting-medicaid";

fn url_in(url: &Option<String>, urls: &[&str]) -> bool {
    url.as_deref().is_some_and(|url| urls.contains(&url))
}

/// `_extension_references`: matching extensions at any depth, first occurrence order.
fn extension_references(resource: &Fields, urls: &[&str]) -> Result<Vec<String>, Unsupported> {
    fn visit(raw: &Value, urls: &[&str], found: &mut Vec<String>) -> Result<(), Unsupported> {
        let extensions = match raw {
            Value::Object(_) => std::slice::from_ref(raw),
            Value::List(items) => items.as_slice(),
            _ => return Ok(()),
        };
        for extension in extensions {
            let Value::Object(fields) = extension else {
                continue;
            };
            if url_in(&extension_url(fields)?, urls) {
                found.extend(references(get(fields, "valueReference"))?);
            }
            visit(get(fields, "extension"), urls, found)?;
        }
        Ok(())
    }
    let mut found = Vec::new();
    visit(get(resource, "extension"), urls, &mut found)?;
    let mut unique: Vec<String> = Vec::new();
    for reference in found {
        if !unique.contains(&reference) {
            unique.push(reference);
        }
    }
    Ok(unique)
}

/// `_network_references`.
fn network_references(resource: &Fields) -> Result<Vec<String>, Unsupported> {
    let mut candidates = references(get(resource, "network"))?;
    for reference in extension_references(resource, &NETWORK_REFERENCE_EXTENSION_URLS)? {
        if organization_reference_identity(&reference)?.is_some() {
            candidates.push(reference);
        }
    }
    let mut seen: Vec<String> = Vec::new();
    let mut kept = Vec::new();
    for reference in candidates {
        let identity = organization_reference_identity(&reference)?
            .unwrap_or_else(|| format!("raw:{reference}"));
        if seen.contains(&identity) {
            continue;
        }
        seen.push(identity);
        kept.push(reference);
    }
    Ok(kept)
}

/// `_plan_net_accepting_patient_entry`.
fn accepting_patient_entry(extension: &Value) -> Result<Option<Value>, Unsupported> {
    let Value::Object(extension) = extension else {
        return Ok(None);
    };
    if !url_in(&extension_url(extension)?, &NEW_PATIENTS_EXTENSION_URLS) {
        return Ok(None);
    }
    let mut accepting_code: Option<String> = None;
    let mut network_reference: Option<String> = None;
    for nested in loop_items(get(extension, "extension"))? {
        let Value::Object(nested) = nested else {
            continue;
        };
        match clean_text(get(nested, "url"))?.as_deref() {
            Some("acceptingPatients") => {
                let concept = get(nested, "valueCodeableConcept");
                let concept_codings = if concept.is_object() {
                    concept.get("coding")
                } else {
                    &Value::Null
                };
                for coding in loop_items(concept_codings)? {
                    let Value::Object(coding) = coding else {
                        continue;
                    };
                    let code = clean_text(get(coding, "code"))?.unwrap_or_default();
                    if let Some(code) = ascii_lower(&code)?
                        && ACCEPTING_PATIENTS_CODES.contains(&code.as_str())
                    {
                        accepting_code = Some(code);
                        break;
                    }
                }
            }
            Some("fromNetwork") => {
                network_reference = first_reference(get(nested, "valueReference"))?
            }
            _ => {}
        }
    }
    let Some(code) = accepting_code else {
        return Ok(None);
    };
    let mut entry = vec![("code".to_owned(), Value::Str(code))];
    if let Some(reference) = network_reference {
        entry.push(("from_network_ref".to_owned(), Value::Str(reference)));
    }
    Ok(Some(Value::Object(entry)))
}

/// `_reviewed_telehealth`.
fn telehealth(resource: &Fields) -> Result<Vec<Value>, Unsupported> {
    let mut entries = Vec::new();
    for extension in loop_items(get(resource, "extension"))? {
        let Value::Object(extension) = extension else {
            continue;
        };
        let url = extension_url(extension)?;
        if url.as_deref() == Some(TELEHEALTH_EXTENSION_URL) {
            if let Value::Bool(flag) = get(extension, "valueBoolean") {
                entries.push(Value::Object(vec![(
                    "supported".to_owned(),
                    Value::Bool(*flag),
                )]));
            }
            continue;
        }
        if !url_in(&url, &DELIVERY_METHOD_EXTENSION_URLS) {
            continue;
        }
        let mut type_codes = Vec::new();
        let mut virtual_modalities = Vec::new();
        for nested in loop_items(get(extension, "extension"))? {
            let Value::Object(nested) = nested else {
                continue;
            };
            match get(nested, "url") {
                Value::Str(url) if url == "type" => {
                    type_codes.extend(codings(get(nested, "valueCodeableConcept"))?)
                }
                Value::Str(url) if url == "virtualModalities" => {
                    virtual_modalities.extend(codings(get(nested, "valueCodeableConcept"))?)
                }
                _ => {}
            }
        }
        let mut is_virtual = false;
        for coding in &type_codes {
            let code = clean_text(coding.get("code"))?.unwrap_or_default();
            is_virtual |= ascii_lower(&code)?.as_deref() == Some("virtual");
        }
        if is_virtual || !virtual_modalities.is_empty() {
            entries.push(Value::Object(vec![
                ("supported".to_owned(), Value::Bool(true)),
                ("type_codes".to_owned(), Value::List(type_codes)),
                (
                    "virtual_modalities".to_owned(),
                    Value::List(virtual_modalities),
                ),
            ]));
        }
    }
    Ok(entries)
}

/// `_reviewed_boolean_extension`: the value only when every matching extension agrees.
fn reviewed_boolean(resource: &Fields, url: &str) -> Result<Value, Unsupported> {
    let mut values: Vec<bool> = Vec::new();
    for extension in loop_items(get(resource, "extension"))? {
        if let Value::Object(extension) = extension
            && extension_url(extension)?.as_deref() == Some(url)
            && let Value::Bool(flag) = get(extension, "valueBoolean")
            && !values.contains(flag)
        {
            values.push(*flag);
        }
    }
    Ok(match values.as_slice() {
        [flag] => Value::Bool(*flag),
        _ => Value::Null,
    })
}

/// The PractitionerRole fields that follow the shared base fields, in Python's dict order.
pub fn fields(resource: &Fields, source_id: &str) -> Result<Fields, Unsupported> {
    // `explicit_npi(resource) or npi_from_resource_id(None)`: Python reads a zero NPI as none.
    let npi = explicit_npi(resource, source_id == CMS_NPD_SOURCE_ID)?.filter(|npi| *npi != 0);
    let active = match get(resource, "active") {
        Value::Bool(flag) => Value::Bool(*flag),
        _ => Value::Null,
    };
    let mut accepting_patients = Vec::new();
    for extension in loop_items(get(resource, "extension"))? {
        if let Some(entry) = accepting_patient_entry(extension)? {
            accepting_patients.push(entry);
        }
    }
    let (period_start, period_end) = period(resource)?;
    let named = [
        (
            "npi",
            npi.map_or(Value::Null, |npi| Value::Int(npi.to_string())),
        ),
        ("active", active),
        (
            "identifiers",
            Value::List(super::fields::normalized_identifiers(get(
                resource,
                "identifier",
            ))?),
        ),
        (
            "practitioner_ref",
            Value::str_or_none(first_reference(get(resource, "practitioner"))?),
        ),
        (
            "organization_ref",
            Value::str_or_none(first_reference(get(resource, "organization"))?),
        ),
        (
            "location_refs",
            string_list(references(get(resource, "location"))?),
        ),
        (
            "healthcare_service_refs",
            string_list(references(get(resource, "healthcareService"))?),
        ),
        ("network_refs", string_list(network_references(resource)?)),
        (
            "insurance_plan_refs",
            string_list(references(get(resource, "insurancePlan"))?),
        ),
        (
            "endpoint_refs",
            string_list(references(get(resource, "endpoint"))?),
        ),
        (
            "specialty_codes",
            Value::List(codings(get(resource, "specialty"))?),
        ),
        ("code_codes", Value::List(codings(get(resource, "code"))?)),
        (
            "telecom",
            Value::List(dict_items(get(resource, "telecom"))?),
        ),
        (
            "accepting_patients",
            Value::List(accepting_patients.clone()),
        ),
        (
            "available_time",
            Value::List(dict_items(get(resource, "availableTime"))?),
        ),
        (
            "not_available",
            Value::List(dict_items(get(resource, "notAvailable"))?),
        ),
        (
            "availability_exceptions",
            Value::str_or_none(clean_text(get(resource, "availabilityExceptions"))?),
        ),
        ("new_patient_acceptance", Value::List(accepting_patients)),
        ("telehealth", Value::List(telehealth(resource)?)),
        (
            "accepting_medicaid",
            reviewed_boolean(resource, ACCEPTING_MEDICAID_EXTENSION_URL)?,
        ),
        ("period_start", period_start),
        ("period_end", period_end),
    ];
    if named.iter().any(|(_, value)| contains_float(value)) {
        return Err(unsupported("float_value"));
    }
    Ok(named
        .into_iter()
        .map(|(name, value)| (name.to_owned(), value))
        .collect())
}

fn contains_float(value: &Value) -> bool {
    match value {
        Value::Float(_) => true,
        Value::List(items) => items.iter().any(contains_float),
        Value::Object(fields) => fields.iter().any(|(_, value)| contains_float(value)),
        _ => false,
    }
}
