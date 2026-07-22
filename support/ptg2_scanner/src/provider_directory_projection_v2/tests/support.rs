// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use super::super::contracts::{ProjectionCopyContext, ProviderDirectoryProjectionCopySpool};
use super::super::encode::project_provider_directory_copy;
use crate::provider_directory_projection::contracts::ProviderDirectoryInputFraming;
use serde_json::Value;

pub const RECIPE_ID: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
pub const PARTITION_ID: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

pub fn context() -> ProjectionCopyContext {
    ProjectionCopyContext {
        recipe_id: RECIPE_ID.to_owned(),
        partition_id: PARTITION_ID.to_owned(),
        partition_ordinal: 7,
    }
}

pub fn rich_resources() -> Vec<Value> {
    serde_json::from_str(
        r#"[
{"resourceType":"Endpoint","id":"endpoint-1","status":"active","contact":[{"system":"phone","value":"2025550100"}],"managingOrganization":{"reference":"Organization/org-1"}},
{"resourceType":"HealthcareService","id":"service-1","active":true,"name":"Cardiology Service","specialty":[{"coding":[{"system":"urn:test","code":"207RC0000X"}]}],"address":{"city":"Prague","country":"CZ"},"providedBy":{"reference":"Organization/org-1"}},
{"resourceType":"InsurancePlan","id":"plan-1","status":"active","name":"Plan One","network":[{"reference":"Organization/network-1"}],"plan":[{"network":[{"reference":"Organization/network-2"}]}]},
{"resourceType":"Location","id":"location-1","status":"active","name":"Klinika Žluťoučký 😀","address":{"line":["1 Main St"],"city":"Iowa City","state":"IA","postalCode":"52240","country":"US"},"position":{"latitude":41.6000005,"longitude":-93.6000015},"partOf":{"reference":"Location/root"}},
{"resourceType":"Organization","id":"org-1","active":true,"name":"Org One","identifier":[{"system":"http://hl7.org/fhir/sid/us-npi","value":"1234567893"}],"address":[{"city":"Austin","state":"TX","country":"US"}]},
{"resourceType":"OrganizationAffiliation","id":"aff-1","active":true,"organization":{"reference":"Organization/org-1"},"participatingOrganization":{"reference":"Organization/org-2"},"network":[{"reference":"Organization/network-1"}],"location":[{"reference":"Location/location-1"}],"specialty":[{"coding":[{"system":"urn:test","code":"207Q00000X"}]}]},
{"resourceType":"Practitioner","id":"practitioner-1","active":false,"identifier":[{"system":"http://hl7.org/fhir/sid/us-npi","value":"1987654321"}],"name":[{"family":"Müller","given":["Zoë"]}],"telecom":[{"system":"phone","value":"2025550199","rank":1}],"qualification":[{"code":{"text":"Family Medicine","coding":[{"system":"urn:vendor","code":"207Q00000X"}]}}]},
{"resourceType":"PractitionerRole","id":"role-1","active":true,"practitioner":{"reference":"Practitioner/practitioner-1"},"organization":{"reference":"Organization/org-1"},"location":[{"reference":"Location/location-1"}],"insurancePlan":[{"reference":"InsurancePlan/plan-1"}],"specialty":[{"coding":[{"system":"urn:test","code":"207Q00000X"}]}]}
]"#,
    )
    .unwrap()
}

pub fn ndjson(resources: &[Value]) -> Vec<u8> {
    let mut input = Vec::new();
    for resource in resources {
        input.extend_from_slice(&serde_json::to_vec(resource).unwrap());
        input.push(b'\n');
    }
    input
}

pub fn rich_spool() -> ProviderDirectoryProjectionCopySpool {
    spool_for_resources(&rich_resources())
}

pub fn spool_for_resources(resources: &[Value]) -> ProviderDirectoryProjectionCopySpool {
    project_provider_directory_copy(
        &ndjson(resources),
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
    .unwrap()
}

pub fn try_spool_for_resources(
    resources: &[Value],
) -> std::io::Result<ProviderDirectoryProjectionCopySpool> {
    project_provider_directory_copy(
        &ndjson(resources),
        ProviderDirectoryInputFraming::Ndjson,
        &context(),
        0.0,
    )
}

pub fn wire_bytes(spool: &ProviderDirectoryProjectionCopySpool) -> Vec<u8> {
    let mut wire = Vec::with_capacity(spool.wire_byte_count());
    wire.extend_from_slice(&spool.header_bytes);
    wire.extend_from_slice(&spool.copy_bytes);
    wire.push(0xff);
    wire
}

#[derive(Debug)]
pub struct DecodedCopyRow {
    fields: Vec<Option<Vec<u8>>>,
}

impl DecodedCopyRow {
    pub fn text(&self, ordinal: usize) -> Option<&str> {
        self.fields[ordinal]
            .as_deref()
            .map(|bytes| std::str::from_utf8(bytes).unwrap())
    }

    pub fn jsonb(&self, ordinal: usize) -> Option<Value> {
        self.fields[ordinal].as_deref().map(|bytes| {
            assert_eq!(bytes[0], 1);
            serde_json::from_slice(&bytes[1..]).unwrap()
        })
    }

    pub fn i64(&self, ordinal: usize) -> Option<i64> {
        self.fields[ordinal]
            .as_deref()
            .map(|bytes| i64::from_be_bytes(bytes.try_into().unwrap()))
    }

    pub fn i32(&self, ordinal: usize) -> Option<i32> {
        self.fields[ordinal]
            .as_deref()
            .map(|bytes| i32::from_be_bytes(bytes.try_into().unwrap()))
    }

    pub fn boolean(&self, ordinal: usize) -> Option<bool> {
        self.fields[ordinal].as_deref().map(|bytes| match bytes {
            [0] => false,
            [1] => true,
            _ => panic!("invalid binary boolean"),
        })
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }
}

pub fn decode_copy_rows(copy: &[u8]) -> Vec<DecodedCopyRow> {
    assert!(copy.starts_with(b"PGCOPY\n\xff\r\n\0"));
    assert_eq!(&copy[11..19], &[0; 8]);
    let mut offset = 19;
    let mut rows = Vec::new();
    loop {
        let field_count = read_i16(copy, &mut offset);
        if field_count == -1 {
            break;
        }
        assert_eq!(field_count, 18);
        let mut fields = Vec::new();
        for _ in 0..field_count {
            let length = read_i32(copy, &mut offset);
            if length == -1 {
                fields.push(None);
                continue;
            }
            let length = usize::try_from(length).unwrap();
            let end = offset + length;
            fields.push(Some(copy[offset..end].to_vec()));
            offset = end;
        }
        rows.push(DecodedCopyRow { fields });
    }
    assert_eq!(offset, copy.len());
    rows
}

fn read_i16(bytes: &[u8], offset: &mut usize) -> i16 {
    let end = *offset + 2;
    let value = i16::from_be_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    value
}

fn read_i32(bytes: &[u8], offset: &mut usize) -> i32 {
    let end = *offset + 4;
    let value = i32::from_be_bytes(bytes[*offset..end].try_into().unwrap());
    *offset = end;
    value
}
