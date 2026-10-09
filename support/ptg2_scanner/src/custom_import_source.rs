//! Bounded canonical SOURCE documents; admission and publication remain external.

use serde::Serialize;
use sha2::{Digest, Sha256};

pub const MAX_SOURCE_ROWS: usize = 64;
pub const MAX_SOURCE_FIELDS: usize = 64;
pub const MAX_SOURCE_INPUT_BYTES: usize = 131_072;
pub const MAX_SOURCE_OUTPUT_BYTES: usize = 4_194_304;
pub const MAX_SOURCE_TEXT_BYTES: usize = 4_096;
const PREFIX: &[u8] = b"custom-import/v1\0candidate-runner/1\0";

pub struct SourceLayout {
    pub child: bool,
    pub fields: Vec<(String, String)>,
    pub root_key: Vec<(String, usize)>,
    pub child_key: Vec<usize>,
}

pub enum SourceValue {
    Missing,
    Null,
    Text(String),
    Integer(i64),
    Boolean(bool),
}

pub struct SourceDocuments {
    pub root_key: String,
    pub root_hash: [u8; 32],
    pub payload: String,
    pub payload_hash: [u8; 32],
    pub child_key: Option<String>,
    pub child_hash: Option<[u8; 32]>,
}

#[derive(Serialize)]
#[serde(untagged)]
enum Scalar<'a> {
    Text(&'a str),
    Integer(i64),
    Boolean(bool),
}

#[derive(Serialize)]
struct ValueDocument<'a> {
    state: &'static str,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    kind: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<Scalar<'a>>,
}

#[derive(Serialize)]
struct FieldDocument<'a> {
    field: &'a str,
    value: ValueDocument<'a>,
}

#[derive(Serialize)]
struct Document<'a> {
    contract: &'static str,
    fields: Vec<FieldDocument<'a>>,
}

fn field_document<'a>(name: &'a str, kind: &'a str, cell: &'a SourceValue) -> FieldDocument<'a> {
    let (state, kind, value) = match cell {
        SourceValue::Missing => ("missing", None, None),
        SourceValue::Null => ("null", Some(kind), None),
        SourceValue::Text(value) => ("value", Some(kind), Some(Scalar::Text(value))),
        SourceValue::Integer(value) => ("value", Some(kind), Some(Scalar::Integer(*value))),
        SourceValue::Boolean(value) => ("value", Some(kind), Some(Scalar::Boolean(*value))),
    };
    FieldDocument {
        field: name,
        value: ValueDocument { state, kind, value },
    }
}

fn document(
    contract: &'static str,
    domain: &[u8],
    fields: Vec<FieldDocument<'_>>,
) -> Result<(String, [u8; 32]), &'static str> {
    let canonical = serde_json::to_string(&Document { contract, fields })
        .map_err(|_| "source document encoding failed")?;
    let mut digest = Sha256::new();
    digest.update(PREFIX);
    digest.update(domain);
    digest.update(b"\0");
    digest.update(canonical.as_bytes());
    Ok((canonical, digest.finalize().into()))
}

/// Inputs have already passed the bounded ABI; no Python callbacks run here.
pub fn source_documents(
    layout: &SourceLayout,
    cells: &[SourceValue],
) -> Result<SourceDocuments, &'static str> {
    let (root_key, root_hash) = document(
        "custom-import-key/v1",
        b"root-key",
        layout
            .root_key
            .iter()
            .map(|(name, index)| field_document(name, &layout.fields[*index].1, &cells[*index]))
            .collect(),
    )?;
    let (payload, payload_hash) = document(
        "custom-import-record/v1",
        if layout.child {
            b"child-payload"
        } else {
            b"root-payload"
        },
        layout
            .fields
            .iter()
            .zip(cells)
            .map(|((name, kind), cell)| field_document(name, kind, cell))
            .collect(),
    )?;
    let child = if layout.child {
        Some(document(
            "custom-import-key/v1",
            b"child-key",
            layout
                .child_key
                .iter()
                .map(|index| {
                    let (name, kind) = &layout.fields[*index];
                    field_document(name, kind, &cells[*index])
                })
                .collect(),
        )?)
    } else {
        None
    };
    let (child_key, child_hash) = child.map_or((None, None), |(key, hash)| (Some(key), Some(hash)));
    Ok(SourceDocuments {
        root_key,
        root_hash,
        payload,
        payload_hash,
        child_key,
        child_hash,
    })
}

pub fn source_value_bytes(cell: &SourceValue) -> usize {
    match cell {
        SourceValue::Text(value) => value.len(),
        SourceValue::Integer(_) => 20,
        SourceValue::Boolean(_) => 5,
        SourceValue::Missing | SourceValue::Null => 0,
    }
}

/// Six bytes per UTF-8 input byte covers every JSON control-character escape.
pub fn source_output_bound(layout: &SourceLayout, cells: &[SourceValue]) -> usize {
    let field =
        |name: &str, index: usize| 192 + 6 * (name.len() + source_value_bytes(&cells[index]));
    let root = 192
        + layout
            .root_key
            .iter()
            .map(|(name, index)| field(name, *index))
            .sum::<usize>();
    let payload = 192
        + layout
            .fields
            .iter()
            .enumerate()
            .map(|(index, (name, _))| field(name, index))
            .sum::<usize>();
    let child = if layout.child {
        192 + layout
            .child_key
            .iter()
            .map(|index| field(&layout.fields[*index].0, *index))
            .sum::<usize>()
    } else {
        0
    };
    root + payload + child
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordered_source_documents_keep_null_missing_unicode_and_domains() {
        let layout = SourceLayout {
            child: true,
            fields: vec![
                ("b".into(), "string".into()),
                ("a".into(), "decimal".into()),
                ("c".into(), "integer".into()),
                ("d".into(), "boolean".into()),
            ],
            root_key: vec![("root".into(), 0)],
            child_key: vec![0, 1],
        };
        let cells = vec![
            SourceValue::Text("Ω\n\0\"\\".into()),
            SourceValue::Text("1.25".into()),
            SourceValue::Missing,
            SourceValue::Null,
        ];
        let result = source_documents(&layout, &cells).unwrap();
        assert_eq!(result.payload, "{\"contract\":\"custom-import-record/v1\",\"fields\":[{\"field\":\"b\",\"value\":{\"state\":\"value\",\"type\":\"string\",\"value\":\"Ω\\n\\u0000\\\"\\\\\"}},{\"field\":\"a\",\"value\":{\"state\":\"value\",\"type\":\"decimal\",\"value\":\"1.25\"}},{\"field\":\"c\",\"value\":{\"state\":\"missing\"}},{\"field\":\"d\",\"value\":{\"state\":\"null\",\"type\":\"boolean\"}}]}");
        assert_ne!(result.root_hash, result.child_hash.unwrap());
        let mut expected = Sha256::new();
        expected.update(PREFIX);
        expected.update(b"child-payload\0");
        expected.update(result.payload.as_bytes());
        assert_eq!(result.payload_hash, <[u8; 32]>::from(expected.finalize()));
        assert!(
            result.root_key.len() + result.payload.len() + result.child_key.unwrap().len()
                <= source_output_bound(&layout, &cells)
        );
    }
}
