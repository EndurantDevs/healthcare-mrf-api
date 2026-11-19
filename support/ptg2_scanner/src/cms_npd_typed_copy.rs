// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

//! `--cms-npd-typed-copy`: CMS National Provider Directory FHIR NDJSON on stdin to
//! PostgreSQL binary COPY files with one typed row per resource (plus address child rows).
//! Column order follows the CMS NPD typed candidate table definitions.

mod directory;
mod fhir;
mod organization;
mod pg;
mod practitioner;
mod practitioner_role;
mod stream;
#[cfg(test)]
mod tests;

use std::fs::File;
use std::io;

pub const USAGE: &str =
    "usage: ptg2_scanner --cms-npd-typed-copy <ResourceType> <main_out_path> [<address_out_path>]";

/// Encodes one NDJSON line into `Output`; the error is the reason without a line number.
pub(crate) type Encoder = fn(&[u8], &mut Output) -> Result<(), String>;

/// COPY bytes for a batch of lines.
#[derive(Default)]
pub(crate) struct Output {
    pub(crate) main: Vec<u8>,
    pub(crate) address: Vec<u8>,
    pub(crate) rows: u64,
    pub(crate) address_rows: u64,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct Summary {
    pub rows: u64,
    pub address_rows: u64,
    pub input_bytes: u64,
}

/// `(encoder, writes an address table)` for a `<ResourceType>` argument.
fn encoder_for(resource_type: &str) -> Option<(Encoder, bool)> {
    Some(match resource_type {
        "Organization" => (organization::encode, true),
        "Practitioner" => (practitioner::encode, true),
        "Location" => (directory::encode_location, false),
        "Endpoint" => (directory::encode_endpoint, false),
        "HealthcareService" => (directory::encode_healthcare_service, false),
        "InsurancePlan" => (directory::encode_insurance_plan, false),
        "OrganizationAffiliation" => (directory::encode_organization_affiliation, false),
        "PractitionerRole" => (practitioner_role::encode, false),
        _ => return None,
    })
}

fn usage_error(reason: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, format!("{reason}; {USAGE}"))
}

pub fn run_cli(arguments: &[String]) -> io::Result<()> {
    let [resource_type, main_path, rest @ ..] = arguments else {
        return Err(usage_error("missing arguments"));
    };
    let (encoder, has_address) = encoder_for(resource_type)
        .ok_or_else(|| usage_error(&format!("unsupported resource type {resource_type:?}")))?;
    let address_path = match (has_address, rest) {
        (true, [path]) => Some(path),
        (false, []) => None,
        (true, _) => {
            return Err(usage_error(&format!(
                "{resource_type} needs exactly one address output path"
            )))
        }
        (false, _) => {
            return Err(usage_error(&format!(
                "{resource_type} takes no address output path"
            )))
        }
    };
    let main = File::create(main_path)?;
    let address = address_path.map(File::create).transpose()?;
    let (summary, _, _) = stream::copy_stream(encoder, io::stdin(), main, address)?;
    println!(
        "{{\"resource_type\":\"{resource_type}\",\"rows\":{},\"address_rows\":{},\"input_bytes\":{}}}",
        summary.rows, summary.address_rows, summary.input_bytes
    );
    Ok(())
}
