// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! `organization` and `organization_address` rows.

use super::fhir::{
    contacts, date, extension, org_ref, parse_record, resource_key, slice, updated_at, uuid_refs,
    write_address_rows, Address, CodeableConcept, Extension, Identifier, Meta, Owner, Reference,
    Str, Telecom,
};
use super::pg::{parse_npi, parse_uuid, Row};
use super::Output;
use serde::Deserialize;

pub(crate) const NPI_SYSTEM: &str = "http://terminology.hl7.org/NamingSystem/npi";
const PSEUDO_EIN_SYSTEM: &str = "https://npd.cms.gov/fhir/sid/us-pseudo-ein";
pub(crate) const VERIFICATION_EXTENSION: &str = "base-ext-verification-status";

#[derive(Deserialize)]
struct Organization<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    #[serde(borrow)]
    identifier: Option<Vec<Identifier<'a>>>,
    #[serde(borrow)]
    name: Option<Str<'a>>,
    active: Option<bool>,
    #[serde(borrow, rename = "type")]
    type_: Option<Vec<CodeableConcept<'a>>>,
    #[serde(borrow, rename = "partOf")]
    part_of: Option<Reference<'a>>,
    #[serde(borrow)]
    endpoint: Option<Vec<Reference<'a>>>,
    #[serde(borrow)]
    extension: Option<Vec<Extension<'a>>>,
    #[serde(borrow)]
    telecom: Option<Vec<Telecom<'a>>>,
    #[serde(borrow)]
    address: Option<Vec<Address<'a>>>,
}

/// Every further code or text of `type[]` (texts equal to their element's first display are
/// skipped, as for `type_text`), distinct and not already `type_code`/`type_text`, source order.
fn other_type_codes<'s>(
    types: &'s [CodeableConcept<'_>],
    type_code: Option<&str>,
    type_text: Option<&str>,
) -> Vec<&'s str> {
    let mut found: Vec<&str> = Vec::new();
    for (index, concept) in types.iter().enumerate() {
        let codes = slice(&concept.coding)
            .iter()
            .skip(usize::from(index == 0))
            .filter_map(|coding| coding.code.as_deref());
        let display = concept
            .first_coding()
            .and_then(|coding| coding.display.as_deref());
        let text = concept
            .text
            .as_deref()
            .filter(|text| index > 0 && Some(*text) != display);
        for value in codes.chain(text) {
            if Some(value) != type_code && Some(value) != type_text && !found.contains(&value) {
                found.push(value);
            }
        }
    }
    found
}

pub(crate) fn encode(line: &[u8], out: &mut Output) -> Result<(), String> {
    let org: Organization = parse_record(line)?;
    let id = resource_key(&org.resource_type, &org.id, "Organization")?;

    let mut npi_identifiers = Vec::new();
    let mut pseudo_eins = Vec::new();
    for identifier in slice(&org.identifier) {
        let value = identifier.value.as_deref();
        match identifier.system.as_deref() {
            Some(NPI_SYSTEM) => npi_identifiers.push(identifier),
            Some(PSEUDO_EIN_SYSTEM) => pseudo_eins.push(parse_uuid(value.unwrap_or_default())?),
            _ => {}
        }
    }
    let npis = npi_identifiers
        .iter()
        .map(|identifier| parse_npi(identifier.value.as_deref().unwrap_or_default()))
        .collect::<Result<Vec<_>, _>>()?;
    let npi_since = npi_identifiers
        .first()
        .and_then(|identifier| identifier.period.as_ref())
        .and_then(|period| period.start.as_deref());
    let org_type = slice(&org.type_).first();
    let type_coding = org_type.and_then(|org_type| org_type.first_coding());
    let type_text = org_type
        .and_then(|org_type| org_type.text.as_deref())
        .filter(|text| Some(*text) != type_coding.and_then(|coding| coding.display.as_deref()));
    let other_type_codes = other_type_codes(
        slice(&org.type_),
        type_coding.and_then(|coding| coding.code.as_deref()),
        type_text,
    );
    let verification = extension(&org.extension, VERIFICATION_EXTENSION).and_then(Extension::code);
    let telecom = contacts(&org.telecom);

    let mut row = Row::start(&mut out.main, 17);
    row.text(Some(id))?;
    row.int8(npis.first().copied());
    row.int8_array(npis.get(1..).unwrap_or_default());
    row.uuid_array(&pseudo_eins);
    row.date(date(npi_since)?);
    row.text(org.name.as_deref())?;
    row.bool(org.active);
    row.text(type_coding.and_then(|coding| coding.code.as_deref()))?;
    row.text(type_text)?;
    row.text_array(&other_type_codes)?;
    row.text(org_ref(&org.part_of)?)?;
    row.text(verification)?;
    row.uuid_array(&uuid_refs(&org.endpoint, "Endpoint")?);
    row.text_array(&telecom.phones)?;
    row.text_array(&telecom.faxes)?;
    row.text_array(&telecom.urls)?;
    row.timestamptz(updated_at(&org.meta)?);
    out.rows += 1;
    out.address_rows +=
        write_address_rows(&mut out.address, Owner::Organization(id), &org.address)?;
    Ok(())
}
