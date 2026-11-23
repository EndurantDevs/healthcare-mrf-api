//! Byte-exact native preparation of Provider Directory FHIR NDJSON batches.
//!
//! `prepare` reproduces, for a whole batch, the Python chain `json.loads` -> `parse_fhir_resource` ->
//! `_endpoint_dataset_resource_rows` -> `merged_role_rows`, or refuses the batch with a reason code so the
//! caller runs that chain instead. It never guesses: every input whose Python result it cannot prove equal
//! is refused. Shared helpers live in `value` (decoding and the three encodings) and `fields` (FHIR fields);
//! each resource type adds only its own mapping module.

pub mod fields;
mod practitioner_role;
pub mod value;

use sha2::{Digest, Sha256};
use value::{get, set, write_list, write_object, write_value, Fields, Style, Value};

pub use value::Value as RowValue;

/// Bump whenever any prepared byte could change; Python refuses a mismatched build.
pub const CONTRACT: &str = "provider_directory_rows_v1";

/// Why a batch is left to the Python chain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Unsupported(pub &'static str);

fn unsupported(reason: &'static str) -> Unsupported {
    Unsupported(reason)
}

const LEGACY_CONTRACT: &str = "transport_bound_v1";
const TRANSPORT_NEUTRAL_CONTRACT: &str = "transport_neutral_v2";
const SEMANTIC_CONTRACTS: [&str; 2] = ["semantic_content_v3", "semantic_content_v4"];
const TRANSPORT_FIELDS: [&str; 4] = [
    "resource_url",
    "fhir_self_url",
    "fhir_fetch_url",
    "fhir_fetch_mode",
];
const NATIVE_ROLES: &str = "native_roles";
const ROLE_NETWORK_FIELDS: [&str; 3] = ["network_refs", "identifiers", "insurance_plan_refs"];
const ROLE_ACCEPTANCE_FIELDS: [&str; 2] = ["accepting_patients", "new_patient_acceptance"];
/// `sorted(_ROLE_IDENTITY_FIELDS)`.
const SORTED_ROLE_IDENTITY_FIELDS: [&str; 7] = [
    "fhir_fetch_mode",
    "fhir_fetch_url",
    "fhir_meta",
    "fhir_self_url",
    NATIVE_ROLES,
    "resource_id",
    "resource_url",
];

/// The batch context the Python chain reads.
pub struct Context<'a> {
    pub source_id: &'a str,
    pub resource_type: &'a str,
    pub resource_url: Option<&'a str>,
    pub self_url: Option<&'a str>,
    pub fetch_url: Option<&'a str>,
    pub fetch_mode: Option<&'a str>,
    pub dataset_id: Option<&'a str>,
    pub resource_hash_contract: &'a str,
    pub merge_roles: bool,
    pub payload_text: bool,
}

/// One `_retained_dataset_resource_record` (or merged role) row.
pub struct PreparedRow {
    pub resource_id: String,
    pub payload_hash: String,
    pub payload: Fields,
    /// The compact `ensure_ascii=False` text stored for the payload, when requested.
    pub payload_text: Option<String>,
    /// `(entry hashes, composed hash)` of a merged role; its base hash is `resource_id`.
    pub merged_role: Option<(Vec<String>, String)>,
}

pub struct Prepared {
    pub resource_type: &'static str,
    pub rows: Vec<PreparedRow>,
    pub rows_fetched: usize,
}

fn sha256_hex(text: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let digest = Sha256::digest(text.as_bytes());
    let mut hex = String::with_capacity(64);
    for byte in digest {
        hex.push(char::from(HEX[usize::from(byte >> 4)]));
        hex.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    hex
}

fn is_sha256_hex(text: &str) -> bool {
    text.len() == 64
        && text
            .bytes()
            .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
}

fn is_semantic(contract: &str) -> bool {
    SEMANTIC_CONTRACTS.contains(&contract)
}

/// Prepare one batch of raw NDJSON lines, or refuse it.
pub fn prepare(lines: &[&[u8]], context: &Context<'_>) -> Result<Prepared, Unsupported> {
    let resource_type = resource_type_name(context.resource_type)?;
    let base = BaseFields::new(context)?;
    let mut payloads = Vec::new();
    for line in lines {
        let Some(resource) = value::decode_line(line)? else {
            continue;
        };
        let Value::Object(resource) = resource else {
            return Err(unsupported("line_not_object"));
        };
        if !matches!(get(&resource, "resourceType"), Value::Str(name) if name == resource_type) {
            return Err(unsupported("line_resource_type"));
        }
        let mut payload = base.fields(&resource)?;
        payload.extend(practitioner_role::fields(&resource, context.source_id)?);
        payloads.push(payload);
    }
    let rows_fetched = payloads.len();
    if context.dataset_id.is_none_or(str::is_empty) {
        return Ok(Prepared {
            resource_type,
            rows: Vec::new(),
            rows_fetched,
        });
    }
    let contract = context.resource_hash_contract;
    if contract != LEGACY_CONTRACT
        && contract != TRANSPORT_NEUTRAL_CONTRACT
        && !is_semantic(contract)
    {
        return Err(unsupported("hash_contract"));
    }
    let mut rows = retained_rows(payloads, contract)?;
    if context.merge_roles && is_semantic(contract) && !rows.is_empty() {
        rows = merged_role_rows(rows)?;
    }
    if context.payload_text {
        for row in &mut rows {
            let mut text = String::new();
            write_object(
                row.payload.iter().map(|(k, v)| (k.as_str(), v)),
                Style::Storage,
                &mut text,
            )?;
            row.payload_text = Some(text);
        }
    }
    Ok(Prepared {
        resource_type,
        rows,
        rows_fetched,
    })
}

/// What one line of `prepare_lines` produced.
pub enum LineEntry {
    /// The row's id (the merged-role id under a semantic contract) and its group file line.
    Row { resource_id: String, text: String },
    /// A line today's handling skips: blank, or a non-object or foreign type when not strict.
    Skipped,
    /// A line left to the Python chain.
    Unsupported(&'static str),
}

pub struct PreparedLines {
    pub entries: Vec<LineEntry>,
    /// The batch error today's handling returns at the line after the last entry, if any.
    pub error: Option<&'static str>,
}

const INVALID_NDJSON: &str = "invalid_ndjson";
const RESOURCE_TYPE_MISMATCH: &str = "bulk_export_output_resource_type_mismatch";

enum LineStep {
    Skip,
    Error(&'static str),
    Payload(Fields),
}

fn resource_type_name(name: &str) -> Result<&'static str, Unsupported> {
    match name {
        "PractitionerRole" => Ok("PractitionerRole"),
        _ => Err(unsupported("resource_type_not_ported")),
    }
}

fn line_step(
    line: &[u8],
    context: &Context<'_>,
    base: &BaseFields,
    strict: bool,
) -> Result<LineStep, Unsupported> {
    let Some(resource) = value::decode_line(line)? else {
        return Ok(LineStep::Skip);
    };
    let Value::Object(resource) = resource else {
        return Ok(if strict {
            LineStep::Error(INVALID_NDJSON)
        } else {
            LineStep::Skip
        });
    };
    if !matches!(get(&resource, "resourceType"), Value::Str(name) if name == context.resource_type)
    {
        return Ok(if strict {
            LineStep::Error(RESOURCE_TYPE_MISMATCH)
        } else {
            LineStep::Skip
        });
    }
    let mut payload = base.fields(&resource)?;
    payload.extend(practitioner_role::fields(&resource, context.source_id)?);
    Ok(LineStep::Payload(payload))
}

/// The group row for one line prepared alone, as the Python chain would for a one-line batch with merged roles.
fn line_row(
    payload: Fields,
    context: &Context<'_>,
    resource_type: &str,
) -> Result<LineEntry, Unsupported> {
    let Some(dataset_id) = context
        .dataset_id
        .filter(|dataset_id| !dataset_id.is_empty())
    else {
        return Err(unsupported("dataset_missing"));
    };
    let contract = context.resource_hash_contract;
    let mut rows = retained_rows(vec![payload], contract)?;
    if is_semantic(contract) && !rows.is_empty() {
        rows = merged_role_rows(rows)?;
    }
    let [row] = <[PreparedRow; 1]>::try_from(rows).map_err(|_| unsupported("line_row_count"))?;
    let fields = [
        ("dataset_id", Value::Str(dataset_id.to_owned())),
        ("resource_type", Value::Str(resource_type.to_owned())),
        ("resource_id", Value::Str(row.resource_id.clone())),
        ("payload_hash", Value::Str(row.payload_hash)),
        ("payload_json", Value::Object(row.payload)),
        ("acquired_resource_sha256", Value::Null),
    ];
    // The grouping module's line: `json.dumps(row, allow_nan=False, separators=(",", ":"))` plus a newline.
    let mut text = String::new();
    write_object(
        fields.iter().map(|(name, value)| (*name, value)),
        Style::Group,
        &mut text,
    )?;
    text.push('\n');
    Ok(LineEntry::Row {
        resource_id: row.resource_id,
        text,
    })
}

/// Prepare each line on its own, in order, with merged roles on: no in-batch identity rules apply.
/// Stops at the first line for which today's handling returns a batch error.
pub fn prepare_lines(
    lines: &[&[u8]],
    context: &Context<'_>,
    strict: bool,
) -> Result<PreparedLines, Unsupported> {
    let resource_type = resource_type_name(context.resource_type)?;
    let contract = context.resource_hash_contract;
    if contract != LEGACY_CONTRACT
        && contract != TRANSPORT_NEUTRAL_CONTRACT
        && !is_semantic(contract)
    {
        return Err(unsupported("hash_contract"));
    }
    let base = BaseFields::new(context)?;
    let mut entries = Vec::with_capacity(lines.len());
    for line in lines {
        let entry = match line_step(line, context, &base, strict) {
            Ok(LineStep::Skip) => LineEntry::Skipped,
            Ok(LineStep::Error(error)) => {
                return Ok(PreparedLines {
                    entries,
                    error: Some(error),
                })
            }
            Ok(LineStep::Payload(payload)) => line_row(payload, context, resource_type)
                .unwrap_or_else(|reason| LineEntry::Unsupported(reason.0)),
            Err(reason) => LineEntry::Unsupported(reason.0),
        };
        entries.push(entry);
    }
    Ok(PreparedLines {
        entries,
        error: None,
    })
}

/// `_fhir_resource_base_fields` without the four fields `_canonical_resource_payload` removes.
struct BaseFields {
    resource_url: Value,
    self_url: Value,
    fetch_url: Value,
    fetch_mode: Value,
}

fn context_url(url: Option<&str>) -> Result<Value, Unsupported> {
    let url = url.map_or(Value::Null, |url| Value::Str(url.to_owned()));
    Ok(Value::str_or_none(fields::url_identity(&url)?))
}

impl BaseFields {
    fn new(context: &Context<'_>) -> Result<Self, Unsupported> {
        Ok(Self {
            resource_url: context_url(context.resource_url)?,
            self_url: context_url(context.self_url)?,
            fetch_url: context_url(context.fetch_url)?,
            fetch_mode: context
                .fetch_mode
                .map_or(Value::Null, |mode| Value::Str(mode.to_owned())),
        })
    }

    fn fields(&self, resource: &Fields) -> Result<Fields, Unsupported> {
        let resource_id = match fields::clean_text(get(resource, "id"))? {
            Some(raw) => fields::truncate(raw, 256),
            None => {
                let mut text = String::new();
                write_object(
                    resource.iter().map(|(k, v)| (k.as_str(), v)),
                    Style::Hash,
                    &mut text,
                )?;
                format!("generated-{}", &sha256_hex(&text)[..48])
            }
        };
        Ok(vec![
            ("resource_id".to_owned(), Value::Str(resource_id)),
            ("resource_url".to_owned(), self.resource_url.clone()),
            (
                "fhir_meta".to_owned(),
                fields::sanitized_meta(get(resource, "meta"))?,
            ),
            ("fhir_self_url".to_owned(), self.self_url.clone()),
            ("fhir_fetch_url".to_owned(), self.fetch_url.clone()),
            ("fhir_fetch_mode".to_owned(), self.fetch_mode.clone()),
        ])
    }
}

fn entries(payload: &Fields) -> impl Iterator<Item = (&str, &Value)> {
    payload.iter().map(|(name, value)| (name.as_str(), value))
}

fn hash_entries<'a>(
    fields: impl Iterator<Item = (&'a str, &'a Value)>,
) -> Result<String, Unsupported> {
    let mut text = String::new();
    write_object(fields, Style::Hash, &mut text)?;
    Ok(sha256_hex(&text))
}

fn stable_text(value: &Value) -> Result<String, Unsupported> {
    value::encode(value, Style::Stable)
}

/// `semantic_resource_payload_sha256` for a payload without Practitioner name fields.
fn semantic_hash(payload: &Fields) -> Result<String, Unsupported> {
    if payload.iter().any(|(name, _)| name == "names") {
        return Err(unsupported("practitioner_names"));
    }
    let stable_meta = match get(payload, "fhir_meta") {
        Value::Object(meta) => {
            let kept: Fields = meta
                .iter()
                .filter(|(name, _)| name != "lastUpdated")
                .cloned()
                .collect();
            Some(if kept.is_empty() {
                Value::Null
            } else {
                Value::Object(kept)
            })
        }
        _ => None,
    };
    hash_entries(
        entries(payload)
            .filter(|(name, _)| !TRANSPORT_FIELDS.contains(name))
            .map(|(name, value)| match (&stable_meta, name) {
                (Some(meta), "fhir_meta") => (name, meta),
                _ => (name, value),
            }),
    )
}

/// `resource_payload_sha256_for_contract` for a role payload.
fn contract_hash(payload: &Fields, contract: &str) -> Result<String, Unsupported> {
    if is_semantic(contract) {
        if let Value::Str(role_id) = get(payload, "resource_id")
            && is_sha256_hex(role_id)
            && !payload.iter().any(|(name, _)| name == "merged_role_repeat")
            && *role_id == merged_role_id(payload)?
        {
            return Ok(merged_role_components(payload)?.1);
        }
        return semantic_hash(payload);
    }
    if contract == TRANSPORT_NEUTRAL_CONTRACT {
        return hash_entries(entries(payload).filter(|(name, _)| !TRANSPORT_FIELDS.contains(name)));
    }
    hash_entries(entries(payload))
}

/// Validate stored merged roles, leaving ambiguous rows to the reference validator.
/// The identity bytes have the same order and framing as the Python content proof.
#[cfg(feature = "python")]
pub(crate) fn verify_merged_role_page(
    rows: &[(String, String, String)],
) -> Option<(String, Vec<usize>)> {
    use rayon::prelude::*;

    if !rows
        .iter()
        .all(|(id, hash, _)| is_sha256_hex(id) && is_sha256_hex(hash))
    {
        return None;
    }
    let verified: Vec<_> = rows
        .par_iter()
        .map(|(id, hash, json)| {
            fn verify(id: &str, hash: &str, json: &str) -> Result<bool, Unsupported> {
                let Value::Object(payload) = value::decode_json(json)? else {
                    return Ok(false);
                };
                Ok(
                    matches!(get(&payload, "resource_id"), Value::Str(role_id) if role_id == id)
                        && !payload.iter().any(|(name, _)| name == "merged_role_repeat")
                        && merged_role_id(&payload)? == id
                        && merged_role_components(&payload)?.1 == hash,
                )
            }
            verify(id, hash, json).unwrap_or(false)
        })
        .collect();
    let mut identities = String::new();
    let mut fallback = Vec::new();
    for (index, ((id, hash, _), verified)) in rows.iter().zip(verified).enumerate() {
        if !verified {
            fallback.push(index);
        }
        if index != 0 {
            identities.push('\n');
        }
        identities.push_str("[\"PractitionerRole\",\"");
        identities.push_str(id);
        identities.push_str("\",\"");
        identities.push_str(hash);
        identities.push_str("\"]");
    }
    Some((identities, fallback))
}

/// `_dataset_payloads_by_resource_id` and `_endpoint_dataset_resource_rows`: one row per identity, sorted.
fn retained_rows(payloads: Vec<Fields>, contract: &str) -> Result<Vec<PreparedRow>, Unsupported> {
    let mut by_identity: std::collections::HashMap<String, Fields> =
        std::collections::HashMap::new();
    for payload in payloads {
        let Value::Str(resource_id) = get(&payload, "resource_id") else {
            return Err(unsupported("resource_id"));
        };
        let identity = fields::python_strip(resource_id);
        if identity.is_empty() {
            continue;
        }
        let identity = identity.to_owned();
        let merged = match by_identity.remove(&identity) {
            None => payload,
            Some(existing) if is_semantic(contract) => {
                // A repeated id that is itself a content hash may take the merged-role union path in Python.
                if is_sha256_hex(resource_id) {
                    return Err(unsupported("hash_shaped_duplicate_id"));
                }
                if semantic_hash(&existing)? != semantic_hash(&payload)? {
                    // Python raises provider_directory_resource_payload_conflict; let it.
                    return Err(unsupported("payload_conflict"));
                }
                let existing_text = stable_text(&Value::Object(existing.clone()))?;
                let incoming_text = stable_text(&Value::Object(payload.clone()))?;
                if incoming_text > existing_text {
                    payload
                } else {
                    existing
                }
            }
            Some(_) => payload,
        };
        by_identity.insert(identity, merged);
    }
    let mut identities: Vec<String> = by_identity.keys().cloned().collect();
    identities.sort_unstable();
    identities
        .into_iter()
        .map(|identity| {
            let payload = by_identity.remove(&identity).unwrap_or_default();
            Ok(PreparedRow {
                payload_hash: contract_hash(&payload, contract)?,
                resource_id: identity,
                payload,
                payload_text: None,
                merged_role: None,
            })
        })
        .collect()
}

/// `_role_network_fields`.
fn role_network_fields(payload: &Fields) -> Result<Vec<&'static str>, Unsupported> {
    let mut names = ROLE_NETWORK_FIELDS.to_vec();
    for name in ROLE_ACCEPTANCE_FIELDS {
        if let Value::List(entries) = get(payload, name)
            && !entries.is_empty()
        {
            let mut all_scoped = true;
            for entry in entries {
                all_scoped &= entry.is_object() && entry.get("from_network_ref").truthy()?;
            }
            if all_scoped {
                names.push(name);
            }
        }
    }
    Ok(names)
}

/// `merged_role_id`.
fn merged_role_id(payload: &Fields) -> Result<String, Unsupported> {
    let network_fields = role_network_fields(payload)?;
    let meta = match get(payload, "fhir_meta") {
        Value::Object(meta) => Value::Object(
            meta.iter()
                .filter(|(name, _)| name != "versionId" && name != "lastUpdated")
                .cloned()
                .collect(),
        ),
        other => other.clone(),
    };
    let mut text = String::from("[");
    write_object(
        entries(payload).filter(|(name, _)| {
            !matches!(*name, "resource_id" | "fhir_meta" | NATIVE_ROLES)
                && !TRANSPORT_FIELDS.contains(name)
                && !network_fields.contains(name)
        }),
        Style::Stable,
        &mut text,
    )?;
    text.push(',');
    write_value(&meta, Style::Stable, &mut text)?;
    text.push(',');
    let names: Vec<Value> = network_fields
        .iter()
        .map(|name| Value::Str((*name).to_owned()))
        .collect();
    write_list(names.iter(), Style::Stable, &mut text)?;
    text.push_str(if get(payload, "network_refs").truthy()? {
        ",true]"
    } else {
        ",false]"
    });
    Ok(sha256_hex(&text))
}

/// The items of `entries or ()` for a role list field; any other truthy value is refused.
fn role_list(value: &Value) -> Result<&[Value], Unsupported> {
    match value {
        Value::List(items) => Ok(items),
        other if !other.truthy()? => Ok(&[]),
        _ => Err(unsupported("role_list_shape")),
    }
}

/// `_role_union`: unique by compact text, sorted by it; the last equal entry is kept.
fn role_union(lists: &[&Value]) -> Result<Value, Unsupported> {
    let mut by_identity: std::collections::BTreeMap<String, Value> =
        std::collections::BTreeMap::new();
    for list in lists {
        for entry in role_list(list)? {
            by_identity.insert(stable_text(entry)?, entry.clone());
        }
    }
    Ok(Value::List(by_identity.into_values().collect()))
}

fn entry_sha256(name: &str, entry: &Value) -> Result<String, Unsupported> {
    let mut text = String::from("[");
    value::write_string(name, Style::Stable, &mut text);
    text.push(',');
    write_value(entry, Style::Stable, &mut text)?;
    text.push(']');
    Ok(sha256_hex(&text))
}

/// `merged_role_hash_components` without the base hash: `(entry hashes, composed hash)`.
fn merged_role_components(payload: &Fields) -> Result<(Vec<String>, String), Unsupported> {
    let role_id = match get(payload, "resource_id") {
        Value::Str(role_id) => role_id.clone(),
        _ => return Err(unsupported("resource_id")),
    };
    let mut hashes = std::collections::BTreeSet::new();
    for name in role_network_fields(payload)? {
        for entry in role_list(get(payload, name))? {
            hashes.insert(entry_sha256(name, entry)?);
        }
    }
    let entry_hashes: Vec<String> = hashes.into_iter().collect();
    let mut text = String::from("[\"merged-practitioner-role\",");
    value::write_string(&role_id, Style::Stable, &mut text);
    text.push(',');
    let entry_values: Vec<Value> = entry_hashes.iter().cloned().map(Value::Str).collect();
    write_list(entry_values.iter(), Style::Stable, &mut text)?;
    text.push(']');
    let composed = sha256_hex(&text);
    Ok((entry_hashes, composed))
}

/// `_merge_role_payloads` for two full observations of one merged role.
fn merge_role_payloads(first: Fields, second: Fields) -> Result<Fields, Unsupported> {
    for observation in [&first, &second] {
        let Value::Str(role_id) = get(observation, "resource_id") else {
            return Err(unsupported("merged_role_identity"));
        };
        if *role_id != merged_role_id(observation)? {
            return Err(unsupported("merged_role_identity"));
        }
    }
    let provenance = |observation: &Fields| -> Result<String, Unsupported> {
        let values: Vec<Value> = SORTED_ROLE_IDENTITY_FIELDS
            .iter()
            .map(|name| get(observation, name).clone())
            .collect();
        stable_text(&Value::List(values))
    };
    let network_fields = role_network_fields(&first)?;
    let mut merged = if provenance(&second)? > provenance(&first)? {
        second.clone()
    } else {
        first.clone()
    };
    for name in network_fields.into_iter().chain([NATIVE_ROLES]) {
        let (left, right) = (get(&first, name), get(&second, name));
        if matches!(left, Value::List(_)) || matches!(right, Value::List(_)) {
            set(&mut merged, name, role_union(&[left, right])?);
        }
    }
    Ok(merged)
}

/// `merged_role_rows` for an opted-in source under a semantic contract.
fn merged_role_rows(rows: Vec<PreparedRow>) -> Result<Vec<PreparedRow>, Unsupported> {
    let mut by_role_id: std::collections::BTreeMap<String, Fields> =
        std::collections::BTreeMap::new();
    for row in rows {
        let role_id = merged_role_id(&row.payload)?;
        let mut payload = row.payload;
        for name in role_network_fields(&payload)? {
            if matches!(get(&payload, name), Value::List(_)) {
                let union = role_union(&[get(&payload, name)])?;
                set(&mut payload, name, union);
            }
        }
        set(&mut payload, "resource_id", Value::Str(role_id.clone()));
        let native_role = format!(
            "{}{}",
            &sha256_hex(&row.resource_id)[..24],
            &row.payload_hash[..16]
        );
        set(
            &mut payload,
            NATIVE_ROLES,
            Value::List(vec![Value::Str(native_role)]),
        );
        let merged = match by_role_id.remove(&role_id) {
            None => payload,
            Some(previous) => merge_role_payloads(previous, payload)?,
        };
        by_role_id.insert(role_id, merged);
    }
    by_role_id
        .into_iter()
        .map(|(role_id, payload)| {
            let (entry_hashes, composed) = merged_role_components(&payload)?;
            Ok(PreparedRow {
                resource_id: role_id,
                payload_hash: composed.clone(),
                payload,
                payload_text: None,
                merged_role: Some((entry_hashes, composed)),
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const LINES: [&[u8]; 3] = [
        br#"{"resourceType":"PractitionerRole","id":"r1","meta":{"lastUpdated":"2026-01-01"},"network":[{"reference":"Organization/n1"}],"identifier":[{"system":"http://hl7.org/fhir/sid/us-npi","value":"1234567893"}]}"#,
        b"  ",
        br#"{"resourceType":"PractitionerRole","id":"r2","meta":{"lastUpdated":"2026-01-02"},"network":[{"reference":"Organization/n2"}],"identifier":[{"system":"http://hl7.org/fhir/sid/us-npi","value":"1234567893"}]}"#,
    ];

    fn context(contract: &str, merge_roles: bool) -> Context<'_> {
        Context {
            source_id: "s",
            resource_type: "PractitionerRole",
            resource_url: Some("https://bulk.example.test/<redacted>:sha256:00"),
            self_url: None,
            fetch_url: Some("https://bulk.example.test/<redacted>:sha256:00"),
            fetch_mode: Some("bulk_ndjson"),
            dataset_id: Some("d"),
            resource_hash_contract: contract,
            merge_roles,
            payload_text: true,
        }
    }

    fn identities(prepared: &Prepared) -> Vec<(&str, &str)> {
        prepared
            .rows
            .iter()
            .map(|row| (row.resource_id.as_str(), row.payload_hash.as_str()))
            .collect()
    }

    // Goldens printed by the Python chain for the same lines and context.
    #[test]
    fn prepared_rows_match_python_goldens() {
        let semantic = prepare(&LINES, &context("semantic_content_v4", false)).unwrap();
        assert_eq!(semantic.rows_fetched, 2);
        assert_eq!(
            identities(&semantic),
            [
                (
                    "r1",
                    "3936cb322370ae8934b5a3b1428a7fab85e6e68b980807449a3be6e8549826e7"
                ),
                (
                    "r2",
                    "9c4c0846eb803073dca895a194c91da6178c0cb44c6ca56ddceb39b04cef1234"
                ),
            ]
        );
        let legacy = prepare(&LINES, &context("transport_bound_v1", true)).unwrap();
        assert_eq!(
            identities(&legacy),
            [
                (
                    "r1",
                    "c216b71a60386eb83a2bb616559a22f96c86bfe22020b1a3b40b9df0119338b3"
                ),
                (
                    "r2",
                    "07302d79f1ac0e3df0c349d6413fa660b03624a6316cc7baabb260a13e61e864"
                ),
            ]
        );
        let merged = prepare(&LINES, &context("semantic_content_v4", true)).unwrap();
        let role_id = "b3d6339a13a32abc16e8879684c95e7dc68df2ce7143088b0bf3974d8d35c84a";
        assert_eq!(
            identities(&merged),
            [(
                role_id,
                "a76675442ea95d0785f68162fc190a6e6ac8a3387ce5dbdec5bc45320397e404"
            )]
        );
        let text = merged.rows[0].payload_text.as_deref().unwrap();
        assert!(text
            .starts_with(r#"{"accepting_medicaid":null,"accepting_patients":[],"active":null,"#));
        assert!(text.contains(r#""native_roles":["82f3e9c695dc6b8d1b11818d3936cb322370ae89","db77fd01af957221a4989b649c4c0846eb803073"],"network_refs":["Organization/n1","Organization/n2"],"#));
        assert!(text.ends_with(r#""specialty_codes":[],"telecom":[],"telehealth":[]}"#));
    }

    #[test]
    fn refusals_name_their_reason() {
        let refuse = |lines: &[&[u8]], contract: &str| {
            prepare(lines, &context(contract, false)).err().map(|e| e.0)
        };
        assert_eq!(refuse(&LINES, "unknown"), Some("hash_contract"));
        let conflict: [&[u8]; 2] = [
            br#"{"resourceType":"PractitionerRole","id":"x","active":true}"#,
            br#"{"resourceType":"PractitionerRole","id":"x","active":false}"#,
        ];
        assert_eq!(
            refuse(&conflict, "semantic_content_v3"),
            Some("payload_conflict")
        );
        assert_eq!(refuse(&conflict, "transport_neutral_v2"), None);
        let mut other = context("semantic_content_v4", false);
        other.resource_type = "Practitioner";
        assert_eq!(
            prepare(&LINES, &other).err().map(|e| e.0),
            Some("resource_type_not_ported")
        );
    }

    #[test]
    fn lines_are_prepared_alone_and_stop_at_batch_errors() {
        let lines: [&[u8]; 5] = [LINES[0], br#"{"id": NaN}"#, b"", b"[1]", LINES[2]];
        let role_id = "b3d6339a13a32abc16e8879684c95e7dc68df2ce7143088b0bf3974d8d35c84a";
        let prepared =
            prepare_lines(&lines, &context("semantic_content_v4", false), false).unwrap();
        assert!(prepared.error.is_none());
        let kinds: Vec<&str> = prepared
            .entries
            .iter()
            .map(|entry| match entry {
                LineEntry::Row { resource_id, text } => {
                    assert_eq!(resource_id, role_id);
                    assert!(text
                        .starts_with(r#"{"dataset_id":"d","resource_type":"PractitionerRole","#));
                    assert!(text.ends_with("\"acquired_resource_sha256\":null}\n"));
                    "row"
                }
                LineEntry::Skipped => "skipped",
                LineEntry::Unsupported(reason) => reason,
            })
            .collect();
        assert_eq!(
            kinds,
            ["row", "non_finite_number", "skipped", "skipped", "row"]
        );
        let strict = prepare_lines(&lines, &context("semantic_content_v4", false), true).unwrap();
        assert_eq!(
            (strict.entries.len(), strict.error),
            (3, Some(INVALID_NDJSON))
        );
        let wide: [&[u8]; 1] =
            [br#"{"resourceType":"PractitionerRole","telecom":[{"rank":18446744073709551616}]}"#];
        let wide_rows =
            prepare_lines(&wide, &context("semantic_content_v4", false), false).unwrap();
        assert!(
            matches!(&wide_rows.entries[..], [LineEntry::Row { text, .. }] if text.contains("18446744073709551616"))
        );
    }
}
