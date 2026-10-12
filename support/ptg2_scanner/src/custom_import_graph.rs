//! Closed, bounded graph decoding. Unsupported records retain the Python oracle.

use crate::custom_import_scalar::SCALAR_TYPES;
use crate::custom_import_source::{
    source_documents, source_output_bound, source_value_bytes, SourceDocuments, SourceLayout,
    SourceValue, MAX_SOURCE_FIELDS, MAX_SOURCE_INPUT_BYTES, MAX_SOURCE_OUTPUT_BYTES,
    MAX_SOURCE_ROWS, MAX_SOURCE_TEXT_BYTES,
};
use serde::de::{self, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer};
use std::collections::BTreeSet;
use std::fmt;

pub const MAX_GRAPH_CELLS: usize = MAX_SOURCE_ROWS * MAX_SOURCE_FIELDS;
pub const MAX_GRAPH_PROJECTED_FIELDS: usize = 20;
// Each decoded cell returns two items; each projected scalar returns ten.
pub const MAX_GRAPH_OUTPUT_CELLS: usize =
    MAX_SOURCE_ROWS * (2 * MAX_SOURCE_FIELDS + 10 * MAX_GRAPH_PROJECTED_FIELDS + 8);
const MAX_GRAPH_STRING_BYTES: usize = 2_048;
const MAX_GRAPH_DECIMAL_BYTES: usize = 64;

#[derive(Clone, Copy)]
pub struct GraphBinding {
    pub nullable: bool,
    pub field_slot: i16,
    pub projection_slot: Option<i16>,
}

pub struct GraphLayout {
    pub source: SourceLayout,
    pub bindings: Vec<GraphBinding>,
}

pub struct GraphChild {
    pub documents: SourceDocuments,
    pub cells: Vec<SourceValue>,
    pub projected_indices: Vec<usize>,
    pub scalar_model_bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
    contract: String,
    fields: Fields,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Field {
    field: String,
    value: Cell,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Cell {
    state: String,
    #[serde(rename = "type")]
    kind: Option<String>,
    value: Option<Primitive>,
}

struct Fields(Vec<Field>);

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct FieldVisitor;
        impl<'de> Visitor<'de> for FieldVisitor {
            type Value = Fields;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a bounded graph field sequence")
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut sequence: A) -> Result<Fields, A::Error> {
                let mut fields =
                    Vec::with_capacity(sequence.size_hint().unwrap_or(0).min(MAX_SOURCE_FIELDS));
                while let Some(field) = sequence.next_element()? {
                    if fields.len() == MAX_SOURCE_FIELDS {
                        return Err(de::Error::custom("graph field bound exceeded"));
                    }
                    fields.push(field);
                }
                Ok(Fields(fields))
            }
        }
        deserializer.deserialize_seq(FieldVisitor)
    }
}

enum Primitive {
    Text(String),
    Integer(i64),
    Boolean(bool),
}

impl<'de> Deserialize<'de> for Primitive {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct PrimitiveVisitor;
        impl<'de> Visitor<'de> for PrimitiveVisitor {
            type Value = Primitive;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string, signed 64-bit integer or boolean")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> Result<Primitive, E> {
                if value.len() > MAX_SOURCE_TEXT_BYTES {
                    return Err(E::custom("graph scalar text bound exceeded"));
                }
                Ok(Primitive::Text(value.to_owned()))
            }
            fn visit_string<E: de::Error>(self, value: String) -> Result<Primitive, E> {
                if value.len() > MAX_SOURCE_TEXT_BYTES {
                    return Err(E::custom("graph scalar text bound exceeded"));
                }
                Ok(Primitive::Text(value))
            }
            fn visit_i64<E: de::Error>(self, value: i64) -> Result<Primitive, E> {
                Ok(Primitive::Integer(value))
            }
            fn visit_u64<E: de::Error>(self, value: u64) -> Result<Primitive, E> {
                i64::try_from(value)
                    .map(Primitive::Integer)
                    .map_err(|_| E::custom("graph integer is outside the native domain"))
            }
            fn visit_bool<E: de::Error>(self, value: bool) -> Result<Primitive, E> {
                Ok(Primitive::Boolean(value))
            }
            // In particular, do not accept arbitrary_precision's reserved map.
            // A real object, a float or a wide integer belongs to the oracle.
        }
        deserializer.deserialize_any(PrimitiveVisitor)
    }
}

fn identifier(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 63
        && name.as_bytes()[0].is_ascii_lowercase()
        && name
            .bytes()
            .all(|value| value.is_ascii_lowercase() || value.is_ascii_digit() || value == b'_')
}

pub fn graph_layout_bound(layout: &GraphLayout) -> Result<usize, &'static str> {
    let source = &layout.source;
    if !source.child
        || source.fields.is_empty()
        || source.fields.len() > MAX_SOURCE_FIELDS
        || source.fields.len() != layout.bindings.len()
        || !(1..=3).contains(&source.root_key.len())
        || !(1..=3).contains(&source.child_key.len())
    {
        return Err("graph layout shape differs");
    }
    let mut names = BTreeSet::new();
    let mut fields = BTreeSet::new();
    let mut projections = BTreeSet::new();
    for ((name, kind), binding) in source.fields.iter().zip(&layout.bindings) {
        if !identifier(name)
            || !SCALAR_TYPES.contains(&kind.as_str())
            || !names.insert(name)
            || binding.field_slot <= 0
            || !fields.insert(binding.field_slot)
            || binding.projection_slot.is_some_and(|slot| {
                !(1..=MAX_GRAPH_PROJECTED_FIELDS as i16).contains(&slot)
                    || !projections.insert(slot)
            })
        {
            return Err("graph field binding differs");
        }
    }
    names.clear();
    let mut child_indices = BTreeSet::new();
    if source.root_key.iter().any(|(name, index)| {
        !identifier(name) || *index >= source.fields.len() || !names.insert(name)
    }) || source
        .child_key
        .iter()
        .any(|index| *index >= source.fields.len() || !child_indices.insert(*index))
    {
        return Err("graph key binding differs");
    }
    Ok(256
        + source
            .fields
            .iter()
            .map(|(name, kind)| 128 + name.len() + kind.len())
            .sum::<usize>()
        + source
            .root_key
            .iter()
            .map(|(name, _)| 64 + name.len())
            .sum::<usize>())
}

/// Accept only the exact plain-text form emitted by normalized v1 decimals.
pub(crate) fn decimal_supported(value: &str, projected: bool) -> bool {
    if value.is_empty() || value.len() > MAX_GRAPH_DECIMAL_BYTES {
        return false;
    }
    let unsigned = value.strip_prefix('-').unwrap_or(value);
    let (whole, fraction) = unsigned
        .split_once('.')
        .map_or((unsigned, None), |(a, b)| (a, Some(b)));
    if whole.is_empty()
        || !whole.bytes().all(|value| value.is_ascii_digit())
        || (whole.len() > 1 && whole.starts_with('0'))
        || fraction.is_some_and(|value| {
            value.is_empty()
                || value.ends_with('0')
                || !value.bytes().all(|value| value.is_ascii_digit())
        })
        || value == "-0"
    {
        return false;
    }
    !projected || (whole.len() <= 18 && fraction.is_none_or(|value| value.len() <= 12))
}

fn decode_cells(layout: &GraphLayout, payload: &str) -> Option<Vec<SourceValue>> {
    if layout
        .source
        .fields
        .iter()
        .any(|(_, kind)| matches!(kind.as_str(), "date" | "timestamp"))
    {
        return None;
    }
    let document: Payload = serde_json::from_str(payload).ok()?;
    if document.contract != "custom-import-record/v1"
        || document.fields.0.len() != layout.source.fields.len()
    {
        return None;
    }
    let mut cells = Vec::with_capacity(layout.source.fields.len());
    for ((field, (name, kind)), binding) in document
        .fields
        .0
        .into_iter()
        .zip(&layout.source.fields)
        .zip(&layout.bindings)
    {
        if field.field != *name {
            return None;
        }
        let cell = match (
            field.value.state.as_str(),
            field.value.kind.as_deref(),
            field.value.value,
        ) {
            ("missing", None, None) if binding.nullable => SourceValue::Missing,
            ("null", Some(actual), None) if binding.nullable && actual == kind => SourceValue::Null,
            ("value", Some(actual), Some(value)) if actual == kind => {
                match (kind.as_str(), value) {
                    ("string", Primitive::Text(value)) => {
                        if binding.projection_slot.is_some()
                            && (value.contains('\0') || value.len() > MAX_GRAPH_STRING_BYTES)
                        {
                            return None;
                        }
                        SourceValue::Text(value)
                    }
                    ("decimal", Primitive::Text(value))
                        if decimal_supported(&value, binding.projection_slot.is_some()) =>
                    {
                        SourceValue::Text(value)
                    }
                    ("integer", Primitive::Integer(value)) => SourceValue::Integer(value),
                    ("boolean", Primitive::Boolean(value)) => SourceValue::Boolean(value),
                    _ => return None,
                }
            }
            _ => return None,
        };
        cells.push(cell);
    }
    if layout
        .source
        .root_key
        .iter()
        .map(|(_, index)| index)
        .chain(&layout.source.child_key)
        .any(|index| matches!(cells[*index], SourceValue::Missing | SourceValue::Null))
    {
        return None;
    }
    Some(cells)
}

/// Only unsupported rows return None. Malformed ABI/batch bounds are separate.
pub fn graph_children(
    layout: &GraphLayout,
    payloads: &[String],
) -> Result<Vec<Option<GraphChild>>, &'static str> {
    let mut input_bytes = graph_layout_bound(layout)?;
    if payloads.is_empty()
        || payloads.len() > MAX_SOURCE_ROWS
        || payloads.len() * layout.source.fields.len() > MAX_GRAPH_CELLS
    {
        return Err("graph row or cell bound exceeded");
    }
    for payload in payloads {
        input_bytes = input_bytes.saturating_add(64).saturating_add(payload.len());
        if input_bytes > MAX_SOURCE_INPUT_BYTES {
            return Err("graph input byte bound exceeded");
        }
    }
    let mut output_bytes = 0usize;
    let mut output_cells = 0usize;
    let mut result = Vec::with_capacity(payloads.len());
    for payload in payloads {
        let Some(cells) = decode_cells(layout, payload) else {
            result.push(None);
            continue;
        };
        let projected_indices: Vec<_> = layout
            .bindings
            .iter()
            .enumerate()
            .filter_map(|(index, binding)| {
                (binding.projection_slot.is_some() && !matches!(cells[index], SourceValue::Missing))
                    .then_some(index)
            })
            .collect();
        let row_bytes = source_output_bound(&layout.source, &cells)
            + cells
                .iter()
                .map(|cell| 64 + source_value_bytes(cell))
                .sum::<usize>()
            + projected_indices.len() * 128;
        let row_cells = 8 + cells.len() * 2 + projected_indices.len() * 10;
        if output_bytes.saturating_add(row_bytes) > MAX_SOURCE_OUTPUT_BYTES
            || output_cells.saturating_add(row_cells) > MAX_GRAPH_OUTPUT_CELLS
        {
            return Err("graph output bound exceeded");
        }
        let documents = source_documents(&layout.source, &cells)?;
        // Reject unknown members, duplicate members, escapes, numeric spelling,
        // missing/null extras and any otherwise noncanonical source document.
        if documents.payload != *payload {
            result.push(None);
            continue;
        }
        let scalar_model_bytes = projected_indices
            .iter()
            .map(|index| {
                let kind = &layout.source.fields[*index].1;
                kind.len()
                    + if matches!(cells[*index], SourceValue::Null) {
                        4
                    } else {
                        5
                    }
                    + match &cells[*index] {
                        SourceValue::Text(value) if kind == "string" => value.len(),
                        _ => 0,
                    }
            })
            .sum();
        output_bytes += row_bytes;
        output_cells += row_cells;
        result.push(Some(GraphChild {
            documents,
            cells,
            projected_indices,
            scalar_model_bytes,
        }));
    }
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn layout() -> GraphLayout {
        GraphLayout {
            source: SourceLayout {
                child: true,
                fields: vec![
                    ("key".into(), "string".into()),
                    ("amount".into(), "decimal".into()),
                    ("count".into(), "integer".into()),
                    ("active".into(), "boolean".into()),
                ],
                root_key: vec![("parent".into(), 0)],
                child_key: vec![2, 0],
            },
            bindings: vec![
                GraphBinding {
                    nullable: false,
                    field_slot: 9,
                    projection_slot: Some(3),
                },
                GraphBinding {
                    nullable: true,
                    field_slot: 2,
                    projection_slot: Some(1),
                },
                GraphBinding {
                    nullable: false,
                    field_slot: 4,
                    projection_slot: None,
                },
                GraphBinding {
                    nullable: true,
                    field_slot: 8,
                    projection_slot: Some(2),
                },
            ],
        }
    }

    fn payload(layout: &GraphLayout, amount: SourceValue, active: SourceValue) -> String {
        source_documents(
            &layout.source,
            &[
                SourceValue::Text("Ω\n\"\\".into()),
                amount,
                SourceValue::Integer(i64::MIN),
                active,
            ],
        )
        .unwrap()
        .payload
    }

    #[test]
    fn graph_rows_preserve_keys_hashes_values_and_projection_accounting() {
        let layout = layout();
        let original = payload(
            &layout,
            SourceValue::Text("-1.25".into()),
            SourceValue::Boolean(true),
        );
        let rows = graph_children(&layout, std::slice::from_ref(&original)).unwrap();
        let row = rows[0].as_ref().unwrap();
        assert_eq!(row.documents.payload, original);
        assert_eq!(row.projected_indices, [0, 1, 3]);
        assert_eq!(
            row.scalar_model_bytes,
            6 + 5 + "Ω\n\"\\".len() + 7 + 5 + 7 + 5
        );
        let expected = source_documents(&layout.source, &row.cells).unwrap();
        assert_eq!(row.documents.root_hash, expected.root_hash);
        assert_eq!(row.documents.child_hash, expected.child_hash);
        assert_eq!(row.documents.payload_hash, expected.payload_hash);
        assert!(row.documents.root_key.contains("\"field\":\"parent\""));
        assert!(
            row.documents
                .child_key
                .as_ref()
                .unwrap()
                .find("\"count\"")
                .unwrap()
                < row
                    .documents
                    .child_key
                    .as_ref()
                    .unwrap()
                    .find("\"key\"")
                    .unwrap()
        );
    }

    #[test]
    fn graph_nullable_missing_and_null_remain_distinct() {
        let mut layout = layout();
        let original = payload(&layout, SourceValue::Missing, SourceValue::Null);
        let rows = graph_children(&layout, std::slice::from_ref(&original)).unwrap();
        let row = rows[0].as_ref().unwrap();
        assert!(matches!(row.cells[1], SourceValue::Missing));
        assert!(matches!(row.cells[3], SourceValue::Null));
        assert_eq!(row.projected_indices, [0, 3]);
        layout.bindings[1].nullable = false;
        assert!(graph_children(&layout, &[original]).unwrap()[0].is_none());
    }

    #[test]
    fn graph_noncanonical_and_unknown_documents_use_the_oracle() {
        let layout = layout();
        let original = payload(
            &layout,
            SourceValue::Text("1.25".into()),
            SourceValue::Boolean(false),
        );
        for changed in [
            format!(" {original}"),
            original.replacen("\"contract\":", "\"extra\":0,\"contract\":", 1),
            original.replacen(
                "\"contract\":",
                "\"contract\":\"custom-import-record/v1\",\"contract\":",
                1,
            ),
            original.replacen("\"field\":\"key\"", "\"extra\":0,\"field\":\"key\"", 1),
            original.replacen("\"state\":\"value\"", "\"extra\":0,\"state\":\"value\"", 1),
            original.replace('Ω', "\\u03a9"),
            original.replace("-9223372036854775808", "9223372036854775808"),
            original.replace("-9223372036854775808", "1.0"),
            original.replace(
                "-9223372036854775808",
                "{\"$serde_json::private::Number\":\"1\"}",
            ),
            original.replace("\"1.25\"", "\"1.250\""),
            original.replace("\"1.25\"", "\"-0\""),
        ] {
            assert!(graph_children(&layout, &[changed]).unwrap()[0].is_none());
        }
        assert!(
            graph_children(&layout, &["{\"fields\":[]}".into(), original]).unwrap()[1].is_some()
        );
    }

    #[test]
    fn graph_decimal_domain_is_normalized_and_projection_scoped() {
        for accepted in [
            "0",
            "1",
            "-1",
            "0.01",
            "-0.000000000001",
            "999999999999999999.999999999999",
        ] {
            assert!(decimal_supported(accepted, true), "{accepted}");
        }
        for rejected in [
            "",
            "-0",
            "00",
            "01",
            "+1",
            " 1",
            "1.",
            ".1",
            "1.0",
            "1e2",
            "NaN",
            "１",
            "0.0000000000001",
            "1000000000000000000",
        ] {
            assert!(!decimal_supported(rejected, true), "{rejected}");
        }
        assert!(decimal_supported("0.0000000000001", false));
        assert!(decimal_supported("1000000000000000000", false));
    }

    #[test]
    fn graph_string_storage_dates_and_keys_fall_back_without_partial_rows() {
        let mut layout = layout();
        let original = payload(&layout, SourceValue::Null, SourceValue::Missing);
        let nul = original.replace('Ω', "\\u0000");
        assert!(graph_children(&layout, std::slice::from_ref(&nul)).unwrap()[0].is_none());
        layout.bindings[0].projection_slot = None;
        assert!(graph_children(&layout, &[nul]).unwrap()[0].is_some());
        let absent_key = original.replace(
            "\"state\":\"value\",\"type\":\"integer\",\"value\":-9223372036854775808",
            "\"state\":\"null\",\"type\":\"integer\"",
        );
        layout.bindings[2].nullable = true;
        assert!(graph_children(&layout, &[absent_key]).unwrap()[0].is_none());
        layout.source.fields[1].1 = "date".into();
        assert!(graph_children(&layout, &[original]).unwrap()[0].is_none());
    }

    #[test]
    fn graph_batch_and_layout_bounds_precede_document_work() {
        let mut layout = layout();
        assert!(graph_children(&layout, &[]).is_err());
        assert!(graph_children(&layout, &vec![String::new(); MAX_SOURCE_ROWS + 1]).is_err());
        assert!(graph_children(&layout, &["x".repeat(MAX_SOURCE_INPUT_BYTES)]).is_err());
        layout.bindings[1].field_slot = layout.bindings[0].field_slot;
        assert!(graph_children(&layout, &["{}".into()]).is_err());
        layout.bindings[1].field_slot = 2;
        layout.bindings[1].projection_slot = Some(3);
        assert!(graph_children(&layout, &["{}".into()]).is_err());
        layout.bindings[1].projection_slot = Some(1);
        layout.source.child_key[0] = usize::MAX;
        assert!(graph_children(&layout, &["{}".into()]).is_err());
    }
}
