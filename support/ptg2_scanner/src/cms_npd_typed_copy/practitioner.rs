// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! `practitioner` and `practitioner_address` rows.

use super::fhir::{
    contacts, date, extension, join, parse_record, reference_key, resource_key, slice, updated_at,
    write_address_rows, Address, CodeableConcept, Extension, Identifier, Meta, Owner, Period,
    Reference, Str, Telecom,
};
use super::organization::NPI_SYSTEM;
use super::pg::{parse_date, parse_npi, Row};
use super::Output;
use crate::npi_identifier::{npi_validity, NpiValidity};
use serde::{Deserialize, Serialize};

/// Also an NPI identifier system for the explicit-NPI check; `old_npis` keeps only `NPI_SYSTEM`.
const US_NPI_SYSTEM: &str = "http://hl7.org/fhir/sid/us-npi";
const TAXONOMY_SYSTEM: &str = "http://hl7.org/fhir/us/ndh/ValueSet/HealthcareIndividualTaxonomyVS";
const FLAG_EXTENSIONS: [&str; 4] = [
    "base-ext-cms-identity-verified",
    "base-ext-cms_medicare_enrollment",
    "base-ext-hhs-in-exclusion-list",
    "base-ext-cms_aligned_with_data_network",
];

#[derive(Deserialize)]
struct HumanName<'a> {
    #[serde(borrow, rename = "use")]
    use_: Option<Str<'a>>,
    #[serde(borrow)]
    family: Option<Str<'a>>,
    #[serde(borrow)]
    given: Option<Vec<Str<'a>>>,
    #[serde(borrow)]
    prefix: Option<Vec<Str<'a>>>,
    #[serde(borrow)]
    suffix: Option<Vec<Str<'a>>>,
    #[serde(borrow)]
    period: Option<Period<'a>>,
}

#[derive(Deserialize)]
struct Qualification<'a> {
    #[serde(borrow)]
    code: Option<CodeableConcept<'a>>,
    #[serde(borrow)]
    identifier: Option<Vec<Identifier<'a>>>,
    #[serde(borrow)]
    issuer: Option<Reference<'a>>,
}

#[derive(Deserialize)]
struct Practitioner<'a> {
    #[serde(borrow, rename = "resourceType")]
    resource_type: Str<'a>,
    #[serde(borrow)]
    id: Str<'a>,
    #[serde(borrow)]
    meta: Option<Meta<'a>>,
    #[serde(borrow)]
    identifier: Option<Vec<Identifier<'a>>>,
    active: Option<bool>,
    #[serde(borrow)]
    gender: Option<Str<'a>>,
    #[serde(borrow)]
    name: Option<Vec<HumanName<'a>>>,
    #[serde(borrow)]
    extension: Option<Vec<Extension<'a>>>,
    #[serde(borrow)]
    qualification: Option<Vec<Qualification<'a>>>,
    #[serde(borrow)]
    telecom: Option<Vec<Telecom<'a>>>,
    #[serde(borrow)]
    address: Option<Vec<Address<'a>>>,
}

/// One `other_names` jsonb element; prefix and suffix parts are joined with a space.
#[derive(Serialize)]
struct OtherName<'s> {
    #[serde(rename = "use")]
    use_: Option<&'s str>,
    family: Option<&'s str>,
    given: Vec<&'s str>,
    prefix: Option<String>,
    suffix: Option<String>,
    since: Option<&'s str>,
}

fn strs<'s>(items: &'s Option<Vec<Str<'_>>>) -> Vec<&'s str> {
    slice(items).iter().map(|item| &**item).collect()
}

fn other_names(names: &[HumanName<'_>]) -> Result<Option<Vec<u8>>, String> {
    if names.is_empty() {
        return Ok(None);
    }
    let mut encoded = Vec::with_capacity(names.len());
    for name in names {
        let since = name
            .period
            .as_ref()
            .and_then(|period| period.start.as_deref());
        if let Some(since) = since {
            parse_date(since)?;
        }
        encoded.push(OtherName {
            use_: name.use_.as_deref(),
            family: name.family.as_deref(),
            given: strs(&name.given),
            prefix: join(&name.prefix, " "),
            suffix: join(&name.suffix, " "),
            since: since.map(|since| &since[..10]),
        });
    }
    Ok(Some(
        serde_json::to_vec(&encoded).map_err(|error| error.to_string())?,
    ))
}

#[derive(Default)]
struct Qualifications<'s> {
    taxonomy_codes: Vec<&'s str>,
    credentials: Vec<&'s str>,
    license_states: Vec<&'s str>,
    license_numbers: Vec<&'s str>,
    license_codes: Vec<&'s str>,
}

fn qualifications<'s>(items: &'s [Qualification<'_>]) -> Result<Qualifications<'s>, String> {
    let mut out = Qualifications::default();
    for item in items {
        let coding = item.code.as_ref().and_then(CodeableConcept::first_coding);
        let code = coding.and_then(|coding| coding.code.as_deref());
        let text = item.code.as_ref().and_then(|code| code.text.as_deref());
        match (code, coding.and_then(|coding| coding.system.as_deref())) {
            (Some(code), Some(TAXONOMY_SYSTEM)) if !out.taxonomy_codes.contains(&code) => {
                out.taxonomy_codes.push(code)
            }
            (Some(_), Some(TAXONOMY_SYSTEM)) => {}
            (Some(code), _) => out.credentials.push(code),
            (None, _) => out.credentials.extend(text),
        }
        let number = slice(&item.identifier)
            .first()
            .and_then(|identifier| identifier.value.as_deref());
        if let Some(number) = number {
            let state = match &item.issuer {
                Some(issuer) => {
                    let key = reference_key(issuer, "Organization")?;
                    key.strip_prefix("State-")
                        .ok_or_else(|| format!("invalid license issuer {key:?}"))?
                }
                None => "",
            };
            out.license_states.push(state);
            out.license_numbers.push(number);
            out.license_codes.push(code.or(text).unwrap_or_default());
        }
    }
    Ok(out)
}

pub(crate) fn encode(line: &[u8], out: &mut Output) -> Result<(), String> {
    let practitioner: Practitioner = parse_record(line)?;
    let key = resource_key(
        &practitioner.resource_type,
        &practitioner.id,
        "Practitioner",
    )?;
    let npi = parse_npi(key)?;

    let mut own_period = None;
    let mut old_npis = Vec::new();
    let (mut own_identified, mut other_us_npi) = (false, false);
    for identifier in slice(&practitioner.identifier) {
        let value = identifier.value.as_deref().unwrap_or_default();
        match identifier.system.as_deref() {
            Some(NPI_SYSTEM) if value != key => old_npis.push(parse_npi(value)?),
            Some(NPI_SYSTEM) => {
                own_identified = true;
                own_period.get_or_insert(identifier.period.as_ref());
            }
            Some(US_NPI_SYSTEM) if value != key => other_us_npi = true,
            Some(US_NPI_SYSTEM) => own_identified = true,
            _ => {}
        }
    }
    // The id's NPI is the record's only explicit, checksum-valid NPI identifier.
    let npi_explicit = own_identified && !other_us_npi && npi_validity(key) == NpiValidity::Valid;
    let own_period = own_period.flatten();
    let names = slice(&practitioner.name);
    let official = names.first();
    let flags = FLAG_EXTENSIONS.map(|suffix| {
        extension(&practitioner.extension, suffix).and_then(|extension| extension.value_boolean)
    });
    let qualifications = qualifications(slice(&practitioner.qualification))?;
    let telecom = contacts(&practitioner.telecom);

    let mut row = Row::start(&mut out.main, 24);
    row.int8(Some(npi));
    row.bool(practitioner.active);
    row.text(practitioner.gender.as_deref())?;
    row.text(official.and_then(|name| name.family.as_deref()))?;
    row.text_array(&official.map(|name| strs(&name.given)).unwrap_or_default())?;
    row.text(official.and_then(|name| join(&name.prefix, " ")).as_deref())?;
    row.text(official.and_then(|name| join(&name.suffix, " ")).as_deref())?;
    row.jsonb(other_names(names.get(1..).unwrap_or_default())?.as_deref());
    row.int8_array(&old_npis);
    row.bool(Some(npi_explicit));
    row.date(date(own_period.and_then(|period| period.start.as_deref()))?);
    row.date(date(own_period.and_then(|period| period.end.as_deref()))?);
    for flag in flags {
        row.bool(flag);
    }
    row.text_array(&qualifications.taxonomy_codes)?;
    row.text_array(&qualifications.credentials)?;
    row.text_array(&qualifications.license_states)?;
    row.text_array(&qualifications.license_numbers)?;
    row.text_array(&qualifications.license_codes)?;
    row.text_array(&telecom.phones)?;
    row.text_array(&telecom.faxes)?;
    row.timestamptz(updated_at(&practitioner.meta)?);
    out.rows += 1;
    out.address_rows += write_address_rows(
        &mut out.address,
        Owner::Practitioner(npi),
        &practitioner.address,
    )?;
    Ok(())
}
