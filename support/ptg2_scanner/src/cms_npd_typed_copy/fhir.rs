// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! Borrowed serde views of the FHIR fragments shared by the CMS NPD resources.
//! shortcut: unknown source fields are dropped silently, add drift counting if CMS changes the schema

use super::pg::{parse_date, parse_npi, parse_timestamptz, parse_uuid, Row, Uuid};
use serde::Deserialize;
use std::borrow::Cow;
use std::ops::Deref;

/// A JSON string borrowed from the input line unless it contains escapes.
/// Serde only borrows a bare `Cow` field, so `Option<Cow>`/`Vec<Cow>` go through this wrapper.
#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct Str<'a>(#[serde(borrow)] pub(crate) Cow<'a, str>);

impl Deref for Str<'_> {
    type Target = str;
    fn deref(&self) -> &str {
        &self.0
    }
}

#[derive(Deserialize)]
pub(crate) struct Meta<'a> {
    #[serde(borrow, rename = "lastUpdated")]
    pub(crate) last_updated: Option<Str<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct Reference<'a> {
    #[serde(borrow)]
    pub(crate) reference: Str<'a>,
}

#[derive(Deserialize)]
pub(crate) struct Coding<'a> {
    #[serde(borrow)]
    pub(crate) system: Option<Str<'a>>,
    #[serde(borrow)]
    pub(crate) code: Option<Str<'a>>,
    #[serde(borrow)]
    pub(crate) display: Option<Str<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct CodeableConcept<'a> {
    #[serde(borrow)]
    pub(crate) coding: Option<Vec<Coding<'a>>>,
    #[serde(borrow)]
    pub(crate) text: Option<Str<'a>>,
}

impl CodeableConcept<'_> {
    pub(crate) fn first_coding(&self) -> Option<&Coding<'_>> {
        slice(&self.coding).first()
    }

    pub(crate) fn first_code(&self) -> Option<&str> {
        self.first_coding()
            .and_then(|coding| coding.code.as_deref())
    }
}

#[derive(Deserialize)]
pub(crate) struct Period<'a> {
    #[serde(borrow)]
    pub(crate) start: Option<Str<'a>>,
    #[serde(borrow)]
    pub(crate) end: Option<Str<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct Identifier<'a> {
    #[serde(borrow)]
    pub(crate) system: Option<Str<'a>>,
    #[serde(borrow)]
    pub(crate) value: Option<Str<'a>>,
    #[serde(borrow)]
    pub(crate) period: Option<Period<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct Telecom<'a> {
    #[serde(borrow)]
    system: Option<Str<'a>>,
    #[serde(borrow)]
    value: Option<Str<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct Address<'a> {
    #[serde(borrow, rename = "use")]
    use_: Option<Str<'a>>,
    #[serde(borrow, rename = "type")]
    type_: Option<Str<'a>>,
    #[serde(borrow)]
    line: Option<Vec<Str<'a>>>,
    #[serde(borrow)]
    city: Option<Str<'a>>,
    #[serde(borrow)]
    state: Option<Str<'a>>,
    #[serde(borrow, rename = "postalCode")]
    postal_code: Option<Str<'a>>,
    #[serde(borrow)]
    country: Option<Str<'a>>,
}

#[derive(Deserialize)]
pub(crate) struct Extension<'a> {
    #[serde(borrow)]
    url: Option<Str<'a>>,
    #[serde(rename = "valueBoolean")]
    pub(crate) value_boolean: Option<bool>,
    #[serde(borrow, rename = "valueCodeableConcept")]
    pub(crate) value_codeable_concept: Option<CodeableConcept<'a>>,
    #[serde(borrow, rename = "valueReference")]
    pub(crate) value_reference: Option<Reference<'a>>,
    #[serde(borrow)]
    pub(crate) extension: Option<Vec<Extension<'a>>>,
}

impl Extension<'_> {
    pub(crate) fn code(&self) -> Option<&str> {
        self.value_codeable_concept.as_ref()?.first_code()
    }
}

pub(crate) fn parse_record<'a, T: Deserialize<'a>>(line: &'a [u8]) -> Result<T, String> {
    serde_json::from_slice(line).map_err(|error| format!("invalid record: {error}"))
}

pub(crate) fn period_dates(
    period: &Option<Period<'_>>,
) -> Result<(Option<i32>, Option<i32>), String> {
    let period = period.as_ref();
    Ok((
        date(period.and_then(|period| period.start.as_deref()))?,
        date(period.and_then(|period| period.end.as_deref()))?,
    ))
}

pub(crate) fn slice<T>(items: &Option<Vec<T>>) -> &[T] {
    items.as_deref().unwrap_or(&[])
}

/// First extension whose url ends with `suffix`.
pub(crate) fn extension<'e, 'a>(
    extensions: &'e Option<Vec<Extension<'a>>>,
    suffix: &str,
) -> Option<&'e Extension<'a>> {
    slice(extensions).iter().find(|extension| {
        extension
            .url
            .as_deref()
            .is_some_and(|url| url.ends_with(suffix))
    })
}

/// Network extension shared by HealthcareService and PractitionerRole.
pub(crate) fn network_id<'e>(
    extensions: &'e Option<Vec<Extension<'_>>>,
) -> Result<Option<&'e str>, String> {
    extension(extensions, "base-ext-network-reference")
        .and_then(|extension| extension.value_reference.as_ref())
        .map(|reference| reference_key(reference, "Organization"))
        .transpose()
}

/// Checks `resourceType` and strips `"<ResourceType>-"` from the id.
pub(crate) fn resource_key<'s>(
    resource_type: &str,
    id: &'s str,
    expected: &str,
) -> Result<&'s str, String> {
    if resource_type != expected {
        return Err(format!("resourceType {resource_type:?} is not {expected}"));
    }
    match id
        .strip_prefix(expected)
        .and_then(|rest| rest.strip_prefix('-'))
    {
        Some(key) if !key.is_empty() => Ok(key),
        _ => Err(format!("invalid {expected} id {id:?}")),
    }
}

/// A few source records carry no `meta.lastUpdated`; they load with a NULL timestamp.
pub(crate) fn updated_at(meta: &Option<Meta<'_>>) -> Result<Option<i64>, String> {
    let text = meta.as_ref().and_then(|meta| meta.last_updated.as_deref());
    text.map(parse_timestamptz).transpose()
}

/// A validated date as its `YYYY-MM-DD` text.
pub(crate) fn day(text: Option<&str>) -> Result<Option<&str>, String> {
    text.map(|text| parse_date(text).map(|_| &text[..10]))
        .transpose()
}

pub(crate) fn date(text: Option<&str>) -> Result<Option<i32>, String> {
    text.map(parse_date).transpose()
}

/// `"<Type>/<Type>-<key>"` → `<key>`.
pub(crate) fn reference_key<'s>(
    reference: &'s Reference<'_>,
    target: &str,
) -> Result<&'s str, String> {
    let text: &str = &reference.reference;
    let key = text
        .strip_prefix(target)
        .and_then(|rest| rest.strip_prefix('/'))
        .and_then(|rest| rest.strip_prefix(target))
        .and_then(|rest| rest.strip_prefix('-'));
    match key {
        Some(key) if !key.is_empty() => Ok(key),
        _ => Err(format!("invalid {target} reference {text:?}")),
    }
}

pub(crate) fn org_ref<'s>(reference: &'s Option<Reference<'_>>) -> Result<Option<&'s str>, String> {
    reference
        .as_ref()
        .map(|reference| reference_key(reference, "Organization"))
        .transpose()
}

pub(crate) fn org_refs<'s>(
    references: &'s Option<Vec<Reference<'_>>>,
) -> Result<Vec<&'s str>, String> {
    slice(references)
        .iter()
        .map(|reference| reference_key(reference, "Organization"))
        .collect()
}

pub(crate) fn uuid_refs(
    references: &Option<Vec<Reference<'_>>>,
    target: &str,
) -> Result<Vec<Uuid>, String> {
    slice(references)
        .iter()
        .map(|reference| reference_key(reference, target).and_then(parse_uuid))
        .collect()
}

pub(crate) fn npi_ref(
    reference: &Option<Reference<'_>>,
    target: &str,
) -> Result<Option<i64>, String> {
    reference
        .as_ref()
        .map(|reference| reference_key(reference, target).and_then(parse_npi))
        .transpose()
}

#[derive(Default)]
pub(crate) struct Contacts<'s> {
    pub(crate) phones: Vec<&'s str>,
    pub(crate) faxes: Vec<&'s str>,
    pub(crate) urls: Vec<&'s str>,
}

/// Splits `telecom[]` by system; other systems (email, ...) are not columns.
pub(crate) fn contacts<'s>(telecom: &'s Option<Vec<Telecom<'_>>>) -> Contacts<'s> {
    let mut contacts = Contacts::default();
    for entry in slice(telecom) {
        let target = match entry.system.as_deref() {
            Some("phone") => &mut contacts.phones,
            Some("fax") => &mut contacts.faxes,
            Some("url") => &mut contacts.urls,
            _ => continue,
        };
        target.extend(entry.value.as_deref());
    }
    contacts
}

pub(crate) fn join(parts: &Option<Vec<Str<'_>>>, separator: &str) -> Option<String> {
    let parts = slice(parts);
    (!parts.is_empty()).then(|| {
        parts
            .iter()
            .map(|part| &**part)
            .collect::<Vec<_>>()
            .join(separator)
    })
}

/// line1, line2 (`line[1..]` joined with ", "), city, state, postal_code, country.
pub(crate) fn write_address_fields(
    row: &mut Row<'_>,
    address: Option<&Address<'_>>,
) -> Result<(), String> {
    let lines = address.map_or(&[][..], |address| slice(&address.line));
    let line2 = (lines.len() > 1).then(|| {
        lines[1..]
            .iter()
            .map(|line| &**line)
            .collect::<Vec<_>>()
            .join(", ")
    });
    row.text(lines.first().map(|line| &**line))?;
    row.text(line2.as_deref())?;
    for field in [
        address.and_then(|address| address.city.as_deref()),
        address.and_then(|address| address.state.as_deref()),
        address.and_then(|address| address.postal_code.as_deref()),
        address.and_then(|address| address.country.as_deref()),
    ] {
        row.text(field)?;
    }
    Ok(())
}

pub(crate) enum Owner<'s> {
    Organization(&'s str),
    Practitioner(i64),
}

/// One `*_address` row per `address[]` entry; returns the row count.
pub(crate) fn write_address_rows(
    out: &mut Vec<u8>,
    owner: Owner<'_>,
    addresses: &Option<Vec<Address<'_>>>,
) -> Result<u64, String> {
    for (ord, address) in slice(addresses).iter().enumerate() {
        let mut row = Row::start(out, 10);
        match owner {
            Owner::Organization(id) => row.text(Some(id))?,
            Owner::Practitioner(npi) => row.int8(Some(npi)),
        }
        row.int4(i32::try_from(ord).map_err(|_| "too many addresses")?);
        row.text(address.use_.as_deref())?;
        row.text(address.type_.as_deref())?;
        write_address_fields(&mut row, Some(address))?;
    }
    Ok(slice(addresses).len() as u64)
}
