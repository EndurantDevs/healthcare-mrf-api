// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Rows for the single-table resources: Location, Endpoint, HealthcareService,
//! InsurancePlan and OrganizationAffiliation.

use super::fhir::{
    contacts, day, extension, network_id, org_ref, org_refs, parse_record, period_dates,
    reference_key, resource_key, slice, updated_at, uuid_refs, write_address_fields, Address,
    CodeableConcept, Coding, Extension, Meta, Period, Reference, Str, Telecom,
};
use super::organization::VERIFICATION_EXTENSION;
use super::pg::{parse_uuid, Row};
use super::Output;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct Position {
    longitude: Option<f64>,
    latitude: Option<f64>,
}

#[derive(Deserialize)]
struct Location<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    #[serde(borrow)]
    name: Option<Str<'a>>,
    #[serde(borrow)]
    description: Option<Str<'a>>,
    #[serde(borrow)]
    status: Option<Str<'a>>,
    #[serde(borrow)]
    address: Option<Address<'a>>,
    position: Option<Position>,
    #[serde(borrow, rename = "managingOrganization")]
    managing_organization: Option<Reference<'a>>,
    #[serde(borrow)]
    telecom: Option<Vec<Telecom<'a>>>,
    #[serde(borrow, rename = "partOf")]
    part_of: Option<Reference<'a>>,
}

pub(crate) fn encode_location(line: &[u8], out: &mut Output) -> Result<(), String> {
    let location: Location = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &location.resource_type,
        &location.id,
        "Location",
    )?)?;
    let name = location.name.as_deref();
    let description = location
        .description
        .as_deref()
        .filter(|text| Some(*text) != name);
    let position = location.position.as_ref();
    let telecom = contacts(&location.telecom);
    let part_of = location
        .part_of
        .as_ref()
        .map(|reference| reference_key(reference, "Location").and_then(parse_uuid))
        .transpose()?;
    let mut row = Row::start(&mut out.main, 17);
    row.uuid(Some(id));
    row.text(name)?;
    row.text(description)?;
    row.text(location.status.as_deref())?;
    write_address_fields(&mut row, location.address.as_ref())?;
    row.float8(position.and_then(|position| position.longitude));
    row.float8(position.and_then(|position| position.latitude));
    row.text(org_ref(&location.managing_organization)?)?;
    row.text_array(&telecom.phones)?;
    row.text_array(&telecom.faxes)?;
    row.uuid(part_of);
    row.timestamptz(updated_at(&location.meta)?);
    out.rows += 1;
    Ok(())
}

#[derive(Deserialize)]
struct Endpoint<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    #[serde(borrow, rename = "connectionType")]
    connection_type: Option<Coding<'a>>,
    #[serde(borrow)]
    address: Option<Str<'a>>,
    #[serde(borrow)]
    name: Option<Str<'a>>,
    #[serde(borrow)]
    status: Option<Str<'a>>,
    #[serde(borrow)]
    extension: Option<Vec<Extension<'a>>>,
    #[serde(borrow, rename = "managingOrganization")]
    managing_organization: Option<Reference<'a>>,
}

pub(crate) fn encode_endpoint(line: &[u8], out: &mut Output) -> Result<(), String> {
    let endpoint: Endpoint = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &endpoint.resource_type,
        &endpoint.id,
        "Endpoint",
    )?)?;
    let mut row = Row::start(&mut out.main, 8);
    row.uuid(Some(id));
    row.text(
        endpoint
            .connection_type
            .as_ref()
            .and_then(|coding| coding.code.as_deref()),
    )?;
    row.text(endpoint.address.as_deref())?;
    row.text(endpoint.name.as_deref())?;
    row.text(endpoint.status.as_deref())?;
    row.text(extension(&endpoint.extension, VERIFICATION_EXTENSION).and_then(Extension::code))?;
    row.text(org_ref(&endpoint.managing_organization)?)?;
    row.timestamptz(updated_at(&endpoint.meta)?);
    out.rows += 1;
    Ok(())
}

#[derive(Deserialize)]
struct HealthcareService<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    active: Option<bool>,
    #[serde(borrow)]
    extension: Option<Vec<Extension<'a>>>,
    #[serde(borrow, rename = "providedBy")]
    provided_by: Option<Reference<'a>>,
    #[serde(borrow)]
    location: Option<Vec<Reference<'a>>>,
}

pub(crate) fn encode_healthcare_service(line: &[u8], out: &mut Output) -> Result<(), String> {
    let service: HealthcareService = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &service.resource_type,
        &service.id,
        "HealthcareService",
    )?)?;
    let mut row = Row::start(&mut out.main, 6);
    row.uuid(Some(id));
    row.bool(service.active);
    row.text(network_id(&service.extension)?)?;
    row.text(org_ref(&service.provided_by)?)?;
    row.uuid_array(&uuid_refs(&service.location, "Location")?);
    row.timestamptz(updated_at(&service.meta)?);
    out.rows += 1;
    Ok(())
}

#[derive(Deserialize)]
struct InsurancePlan<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    #[serde(borrow)]
    name: Option<Str<'a>>,
    #[serde(borrow)]
    status: Option<Str<'a>>,
    #[serde(borrow)]
    alias: Option<Vec<Str<'a>>>,
    #[serde(borrow, rename = "ownedBy")]
    owned_by: Option<Reference<'a>>,
    #[serde(borrow)]
    network: Option<Vec<Reference<'a>>>,
    #[serde(borrow, rename = "type")]
    type_: Option<Vec<CodeableConcept<'a>>>,
    #[serde(borrow)]
    period: Option<Period<'a>>,
    #[serde(borrow, rename = "administeredBy")]
    administered_by: Option<Reference<'a>>,
    #[serde(borrow, rename = "coverageArea")]
    coverage_area: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    plan: Option<Vec<PlanPart<'a>>>,
    #[serde(borrow)]
    coverage: Option<Vec<PlanPart<'a>>>,
}

/// The parts of `plan[]` and `coverage[]` elements that carry references.
#[derive(Deserialize)]
struct PlanPart<'a> {
    #[serde(borrow)]
    network: Option<Vec<Reference<'a>>>,
    #[serde(borrow, rename = "coverageArea")]
    coverage_area: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    period: Option<Period<'a>>,
}

/// One `nested_references` element. The period is the parent element's when it has one,
/// otherwise the plan's (the old relationship ledger's rule).
#[derive(Serialize)]
struct NestedReference<'s> {
    field: &'static str,
    parent: usize,
    ord: usize,
    target: &'s str,
    period_start: Option<&'s str>,
    period_end: Option<&'s str>,
}

fn nested_references(plan: &InsurancePlan<'_>) -> Result<Option<Vec<u8>>, String> {
    let mut found = Vec::new();
    let fields: [(&'static str, &Option<Vec<PlanPart<'_>>>, bool); 3] = [
        ("plan.network", &plan.plan, true),
        ("plan.coverageArea", &plan.plan, false),
        ("coverage.network", &plan.coverage, true),
    ];
    for (field, parts, network) in fields {
        for (parent, part) in slice(parts).iter().enumerate() {
            let period = part.period.as_ref().or(plan.period.as_ref());
            let period_start = day(period.and_then(|period| period.start.as_deref()))?;
            let period_end = day(period.and_then(|period| period.end.as_deref()))?;
            let references = if network {
                &part.network
            } else {
                &part.coverage_area
            };
            for (ord, reference) in slice(references).iter().enumerate() {
                let target = if network {
                    reference_key(reference, "Organization")?
                } else {
                    let key = reference_key(reference, "Location")?;
                    parse_uuid(key)?;
                    key
                };
                found.push(NestedReference {
                    field,
                    parent,
                    ord,
                    target,
                    period_start,
                    period_end,
                });
            }
        }
    }
    if found.is_empty() {
        return Ok(None);
    }
    serde_json::to_vec(&found)
        .map(Some)
        .map_err(|error| error.to_string())
}

pub(crate) fn encode_insurance_plan(line: &[u8], out: &mut Output) -> Result<(), String> {
    let plan: InsurancePlan = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &plan.resource_type,
        &plan.id,
        "InsurancePlan",
    )?)?;
    let (period_start, period_end) = period_dates(&plan.period)?;
    let aliases: Vec<&str> = slice(&plan.alias).iter().map(|alias| &**alias).collect();
    let nested = nested_references(&plan)?;
    let mut row = Row::start(&mut out.main, 13);
    row.uuid(Some(id));
    row.text(plan.name.as_deref())?;
    row.text(plan.status.as_deref())?;
    row.text_array(&aliases)?;
    row.text(org_ref(&plan.owned_by)?)?;
    row.text_array(&org_refs(&plan.network)?)?;
    row.text(
        slice(&plan.type_)
            .first()
            .and_then(|plan_type| plan_type.text.as_deref()),
    )?;
    row.date(period_start);
    row.date(period_end);
    row.text(org_ref(&plan.administered_by)?)?;
    row.uuid_array(&uuid_refs(&plan.coverage_area, "Location")?);
    row.jsonb(nested.as_deref());
    row.timestamptz(updated_at(&plan.meta)?);
    out.rows += 1;
    Ok(())
}

#[derive(Deserialize)]
struct OrganizationAffiliation<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    active: Option<bool>,
    #[serde(borrow)]
    code: Option<Vec<CodeableConcept<'a>>>,
    #[serde(borrow)]
    organization: Option<Reference<'a>>,
    #[serde(borrow, rename = "participatingOrganization")]
    participating_organization: Option<Reference<'a>>,
    #[serde(borrow)]
    network: Option<Vec<Reference<'a>>>,
}

pub(crate) fn encode_organization_affiliation(line: &[u8], out: &mut Output) -> Result<(), String> {
    let affiliation: OrganizationAffiliation = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &affiliation.resource_type,
        &affiliation.id,
        "OrganizationAffiliation",
    )?)?;
    let mut row = Row::start(&mut out.main, 7);
    row.uuid(Some(id));
    row.bool(affiliation.active);
    row.text(
        slice(&affiliation.code)
            .first()
            .and_then(CodeableConcept::first_code),
    )?;
    row.text(org_ref(&affiliation.organization)?)?;
    row.text(org_ref(&affiliation.participating_organization)?)?;
    row.text_array(&org_refs(&affiliation.network)?)?;
    row.timestamptz(updated_at(&affiliation.meta)?);
    out.rows += 1;
    Ok(())
}
