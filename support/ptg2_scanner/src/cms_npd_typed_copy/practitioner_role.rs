// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! `practitioner_role` rows.

use super::fhir::{
    contacts, extension, network_id, npi_ref, org_ref, org_refs, parse_record, period_dates,
    resource_key, slice, updated_at, uuid_refs, CodeableConcept, Extension, Meta, Period,
    Reference, Str, Telecom,
};
use super::pg::{parse_uuid, Row};
use super::Output;
use serde::Deserialize;

#[derive(Deserialize)]
struct PractitionerRole<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    active: Option<bool>,
    #[serde(borrow)]
    practitioner: Option<Reference<'a>>,
    #[serde(borrow)]
    organization: Option<Reference<'a>>,
    #[serde(borrow)]
    location: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    endpoint: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    extension: Option<Vec<Extension<'a>>>,
    #[serde(borrow)]
    specialty: Option<Vec<CodeableConcept<'a>>>,
    #[serde(borrow)]
    period: Option<Period<'a>>,
    #[serde(borrow)]
    telecom: Option<Vec<Telecom<'a>>>,
    #[serde(borrow, rename = "insurancePlan")]
    insurance_plan: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    network: Option<Vec<Reference<'a>>>,
}

pub(crate) fn encode(line: &[u8], out: &mut Output) -> Result<(), String> {
    let role: PractitionerRole = parse_record(line)?;
    let id = parse_uuid(resource_key(
        &role.resource_type,
        &role.id,
        "PractitionerRole",
    )?)?;
    let new_patients = extension(&role.extension, "base-ext-newpatients")
        .and_then(|extension| slice(&extension.extension).first())
        .and_then(Extension::code);
    let specialty_codes: Vec<&str> = slice(&role.specialty)
        .iter()
        .filter_map(|specialty| specialty.first_code().or(specialty.text.as_deref()))
        .collect();
    let (period_start, period_end) = period_dates(&role.period)?;
    let telecom = contacts(&role.telecom);

    let mut row = Row::start(&mut out.main, 16);
    row.uuid(Some(id));
    row.int8(npi_ref(&role.practitioner, "Practitioner")?);
    row.text(org_ref(&role.organization)?)?;
    row.bool(role.active);
    row.uuid_array(&uuid_refs(&role.location, "Location")?);
    row.uuid_array(&uuid_refs(&role.endpoint, "Endpoint")?);
    row.text(network_id(&role.extension)?)?;
    row.text(new_patients)?;
    row.text_array(&specialty_codes)?;
    row.date(period_start);
    row.date(period_end);
    row.text_array(&telecom.phones)?;
    row.text_array(&telecom.faxes)?;
    row.uuid_array(&uuid_refs(&role.insurance_plan, "InsurancePlan")?);
    row.text_array(&org_refs(&role.network)?)?;
    row.timestamptz(updated_at(&role.meta)?);
    out.rows += 1;
    Ok(())
}
