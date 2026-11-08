//! Ordered, bounded child projection columns; unsupported rows keep the serial oracle.

use crate::custom_import_graph::{
    decimal_supported, graph_children, graph_layout_bound, GraphLayout,
};
use crate::custom_import_source::{
    SourceValue, MAX_SOURCE_INPUT_BYTES, MAX_SOURCE_OUTPUT_BYTES, MAX_SOURCE_ROWS,
};
use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::OnceLock;

const MAX_CONTEXT_BYTES: usize = 8_192;
const CONTEXT_PREFIX: &[u8] = b"custom-import/v1\0winner-context\0";
static GRAPH_PROJECTION_POOL: OnceLock<Result<ThreadPool, String>> = OnceLock::new();

pub struct QueryField {
    pub kind: String,
    pub nullable: bool,
    pub slot: i16,
}

pub struct ProjectionProfile {
    pub slot: i16,
    pub id: String,
    pub selection: Vec<usize>,
    pub dimensions: Vec<usize>,
}

pub struct ProjectionCollection {
    pub slot: i16,
    pub graph: GraphLayout,
    pub query_indices: Vec<usize>,
    pub profiles: Vec<ProjectionProfile>,
}

pub struct ProjectionLayout {
    pub roots: Vec<QueryField>,
    pub collections: Vec<ProjectionCollection>,
}

pub struct ProjectionRoot {
    pub key: String,
    pub hash: [u8; 32],
    pub entity_id: i64,
    pub family_id: i64,
    pub family_hash: [u8; 32],
    pub cells: Vec<SourceValue>,
}

pub struct ProjectionInput {
    pub collection: usize,
    pub root: usize,
    pub child_id: i64,
    pub parent_key: String,
    pub parent_hash: [u8; 32],
    pub child_key: String,
    pub child_hash: [u8; 32],
    pub payload_hash: [u8; 32],
    pub payload: String,
}

pub enum ColumnValue {
    Null,
    Text(String),
    Integer(i64),
    Boolean(bool),
    Hash([u8; 32]),
}

pub type ProjectionSpan = (usize, usize, usize, usize, usize, usize, usize);

pub struct ProjectionColumns {
    pub rows: Vec<Option<ProjectionSpan>>,
    pub scalars: [Vec<ColumnValue>; 10],
    pub contexts: [Vec<ColumnValue>; 4],
}

struct PreparedChild {
    scalars: [Vec<ColumnValue>; 10],
    contexts: [Vec<ColumnValue>; 4],
    scalar_bytes: usize,
    context_bytes: usize,
    argument_bytes: usize,
}

fn projection_pool() -> Result<&'static ThreadPool, String> {
    GRAPH_PROJECTION_POOL
        .get_or_init(|| {
            let workers = std::thread::available_parallelism()
                .map(usize::from)
                .unwrap_or(4)
                .min(8);
            ThreadPoolBuilder::new()
                .num_threads(workers)
                .stack_size(1_048_576)
                .thread_name(|index| format!("graph-project-{index}"))
                .build()
                .map_err(|error| error.to_string())
        })
        .as_ref()
        .map_err(Clone::clone)
}

pub fn projection_layout_bound(layout: &ProjectionLayout) -> Result<usize, &'static str> {
    if layout.roots.len() > 20 || layout.collections.is_empty() || layout.collections.len() > 8 {
        return Err("graph projection layout bound exceeded");
    }
    let mut bytes = 256 + layout.roots.len() * 128;
    let mut slots = std::collections::BTreeSet::new();
    for field in &layout.roots {
        if field.slot <= 0
            || !slots.insert(field.slot)
            || !matches!(
                field.kind.as_str(),
                "string" | "integer" | "decimal" | "boolean"
            )
        {
            return Err("graph projection root field differs");
        }
    }
    let mut collections = std::collections::BTreeSet::new();
    for collection in &layout.collections {
        bytes += graph_layout_bound(&collection.graph)?;
        if collection.slot <= 0
            || !collections.insert(collection.slot)
            || collection.query_indices.len() + layout.roots.len() > 20
            || collection.profiles.len() > 4
        {
            return Err("graph projection collection differs");
        }
        let mut indices = std::collections::BTreeSet::new();
        if collection
            .query_indices
            .iter()
            .any(|index| *index >= collection.graph.bindings.len() || !indices.insert(index))
        {
            return Err("graph projection query binding differs");
        }
        let mut profiles = std::collections::BTreeSet::new();
        for profile in &collection.profiles {
            let fields = layout.roots.len() + collection.query_indices.len();
            if !(1..=4).contains(&profile.slot)
                || !profiles.insert(profile.slot)
                || profile.id.is_empty()
                || profile.id.len() > 63
                || profile.selection.len() > 3
                || profile.dimensions.len() > 2
                || profile
                    .selection
                    .iter()
                    .chain(&profile.dimensions)
                    .any(|index| *index >= fields)
                || profile
                    .dimensions
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len()
                    != profile.dimensions.len()
            {
                return Err("graph projection profile differs");
            }
            bytes += 256 + profile.id.len();
        }
    }
    Ok(bytes)
}

/// Reserve the entire packet, including simultaneous decode scratch and boxed columns.
pub fn projection_workspace_bound(
    layout: &ProjectionLayout,
    roots: &[Option<ProjectionRoot>],
    rows: &[Option<ProjectionInput>],
) -> Result<usize, &'static str> {
    // Unsupported ordinals still own outcome and span containers.
    let mut bound = rows.len().saturating_mul(16_384);
    for row in rows.iter().flatten() {
        let collection = layout
            .collections
            .get(row.collection)
            .ok_or("graph projection collection index differs")?;
        let Some(root) = roots
            .get(row.root)
            .ok_or("graph projection root index differs")?
            .as_ref()
        else {
            continue;
        };
        let root_text = root
            .cells
            .iter()
            .map(|cell| match cell {
                SourceValue::Text(value) => value.len(),
                _ => 32,
            })
            .sum::<usize>();
        bound = bound.saturating_add(
            24 * row.payload.len()
                // One UTF-8 allocation plus worst-case four-byte PyUnicode.
                // Charge escaped JSON and its metadata before workers start.
                + 5 * collection.profiles.len() * (512 + 6 * (root_text + row.payload.len())),
        );
    }
    Ok(bound)
}

fn normalized(kind: &str, nullable: bool, cell: &SourceValue) -> bool {
    match cell {
        SourceValue::Missing => true,
        SourceValue::Null => nullable,
        SourceValue::Text(value) if kind == "string" => {
            !value.contains('\0') && value.len() <= 2_048
        }
        SourceValue::Text(value) if kind == "decimal" => decimal_supported(value, true),
        SourceValue::Integer(_) => kind == "integer",
        SourceValue::Boolean(_) => kind == "boolean",
        _ => false,
    }
}

#[derive(Serialize)]
#[serde(untagged)]
enum ContextValue<'a> {
    Text(&'a str),
    Integer(i64),
    Boolean(bool),
}

#[derive(Serialize)]
struct Dimension<'a> {
    field_slot: i16,
    field_type: &'a str,
    value: Option<ContextValue<'a>>,
    value_state: &'static str,
}

#[derive(Serialize)]
struct ContextDocument<'a> {
    dimensions: Vec<Dimension<'a>>,
    profile_id: &'a str,
    scope: &'static str,
}

fn state(cell: &SourceValue) -> &'static str {
    match cell {
        SourceValue::Missing => "missing",
        SourceValue::Null => "null",
        _ => "value",
    }
}

fn context_document(
    profile: &ProjectionProfile,
    fields: &[(&str, bool, i16, &SourceValue)],
) -> Option<(String, [u8; 32])> {
    if profile
        .selection
        .iter()
        .chain(&profile.dimensions)
        .any(|index| !fields[*index].1 && matches!(fields[*index].3, SourceValue::Missing))
    {
        return None;
    }
    let dimensions = profile
        .dimensions
        .iter()
        .map(|index| {
            let (kind, _, slot, cell) = fields[*index];
            let value = match cell {
                SourceValue::Text(value) => Some(ContextValue::Text(value)),
                SourceValue::Integer(value) => Some(ContextValue::Integer(*value)),
                SourceValue::Boolean(value) => Some(ContextValue::Boolean(*value)),
                _ => None,
            };
            Dimension {
                field_slot: slot,
                field_type: kind,
                value,
                value_state: state(cell),
            }
        })
        .collect();
    let canonical = serde_json::to_string(&ContextDocument {
        dimensions,
        profile_id: &profile.id,
        scope: "child",
    })
    .ok()?;
    if canonical.len() > MAX_CONTEXT_BYTES {
        return None;
    }
    let mut digest = Sha256::new();
    digest.update(CONTEXT_PREFIX);
    digest.update(canonical.as_bytes());
    Some((canonical, digest.finalize().into()))
}

fn push_scalar(
    output: &mut PreparedChild,
    child_id: i64,
    slot: i16,
    kind: &str,
    cell: &SourceValue,
) {
    use ColumnValue as V;
    output.scalars[0].push(V::Integer(child_id));
    output.scalars[1].push(V::Integer(slot.into()));
    output.scalars[2].push(V::Text(kind.to_owned()));
    output.scalars[3].push(V::Text(state(cell).into()));
    for lane in 4..10 {
        let value = match (lane, kind, cell) {
            (4, "string", SourceValue::Text(value)) | (6, "decimal", SourceValue::Text(value)) => {
                V::Text(value.clone())
            }
            (5, "integer", SourceValue::Integer(value)) => V::Integer(*value),
            (7, "boolean", SourceValue::Boolean(value)) => V::Boolean(*value),
            _ => V::Null,
        };
        output.scalars[lane].push(value);
    }
    let strings = kind.len() + state(cell).len();
    let text_bytes = if let ("string", SourceValue::Text(value)) = (kind, cell) {
        Some(value.len())
    } else {
        None
    };
    output.scalar_bytes += strings + text_bytes.unwrap_or(0);
    output.argument_bytes += strings + text_bytes.map_or(256, |size| 224 + size);
}

fn prepare_child(
    layout: &ProjectionLayout,
    roots: &[Option<ProjectionRoot>],
    row: &ProjectionInput,
) -> Result<Option<PreparedChild>, &'static str> {
    let collection = &layout.collections[row.collection];
    let Some(root) = &roots[row.root] else {
        return Ok(None);
    };
    let Some(child) = graph_children(&collection.graph, std::slice::from_ref(&row.payload))
        .ok()
        .and_then(|mut children| children.pop().flatten())
    else {
        return Ok(None);
    };
    if row.child_id <= 0
        || row.parent_key != root.key
        || row.parent_hash != root.hash
        || child.documents.child_key.as_ref() != Some(&row.child_key)
        || child.documents.child_hash != Some(row.child_hash)
        || child.documents.payload_hash != row.payload_hash
    {
        return Ok(None);
    }
    let mut output = PreparedChild {
        scalars: std::array::from_fn(|_| Vec::new()),
        contexts: std::array::from_fn(|_| Vec::new()),
        scalar_bytes: 0,
        context_bytes: 0,
        argument_bytes: 0,
    };
    for index in child.projected_indices {
        push_scalar(
            &mut output,
            row.child_id,
            collection.graph.bindings[index].field_slot,
            &collection.graph.source.fields[index].1,
            &child.cells[index],
        );
    }
    if output.scalar_bytes != child.scalar_model_bytes {
        return Ok(None);
    }
    if collection.profiles.is_empty() {
        return Ok(Some(output));
    }
    if root.entity_id <= 0 || root.family_id <= 0 {
        return Ok(None);
    }
    let fields = layout
        .roots
        .iter()
        .zip(&root.cells)
        .map(|(field, cell)| (field.kind.as_str(), field.nullable, field.slot, cell))
        .chain(collection.query_indices.iter().map(|index| {
            let binding = &collection.graph.bindings[*index];
            (
                collection.graph.source.fields[*index].1.as_str(),
                binding.nullable,
                binding.field_slot,
                &child.cells[*index],
            )
        }))
        .collect::<Vec<_>>();
    if fields
        .iter()
        .any(|(kind, nullable, _, cell)| !normalized(kind, *nullable, cell))
    {
        return Ok(None);
    }
    for profile in &collection.profiles {
        let Some((canonical, hash)) = context_document(profile, &fields) else {
            return Ok(None);
        };
        output.context_bytes += canonical.len() + 32;
        output.argument_bytes += canonical.len() + 64;
        output.contexts[0].push(ColumnValue::Integer(row.child_id));
        output.contexts[1].push(ColumnValue::Integer(profile.slot.into()));
        output.contexts[2].push(ColumnValue::Text(canonical));
        output.contexts[3].push(ColumnValue::Hash(hash));
    }
    Ok(Some(output))
}

pub fn projection_columns(
    layout: &ProjectionLayout,
    roots: &[Option<ProjectionRoot>],
    rows: &[Option<ProjectionInput>],
    input_bytes: usize,
) -> Result<ProjectionColumns, String> {
    projection_layout_bound(layout)?;
    if rows.is_empty()
        || rows.len() > MAX_SOURCE_ROWS
        || roots.len() > MAX_SOURCE_ROWS
        || input_bytes > MAX_SOURCE_INPUT_BYTES
        || roots
            .iter()
            .flatten()
            .any(|root| root.cells.len() != layout.roots.len())
        || projection_workspace_bound(layout, roots, rows)? > MAX_SOURCE_OUTPUT_BYTES
    {
        return Err("graph projection batch bound exceeded".into());
    }
    let outcomes = projection_pool()?.install(|| {
        rows.par_iter()
            .map(|row| match row {
                Some(row) => prepare_child(layout, roots, row),
                None => Ok(None),
            })
            .collect::<Vec<_>>()
    });
    let mut output = ProjectionColumns {
        rows: Vec::with_capacity(rows.len()),
        scalars: std::array::from_fn(|_| Vec::new()),
        contexts: std::array::from_fn(|_| Vec::new()),
    };
    for outcome in outcomes {
        let Some(child) = outcome? else {
            output.rows.push(None);
            continue;
        };
        output.rows.push(Some((
            output.scalars[0].len(),
            child.scalars[0].len(),
            output.contexts[0].len(),
            child.contexts[0].len(),
            child.scalar_bytes,
            child.context_bytes,
            child.argument_bytes,
        )));
        for (target, column) in output.scalars.iter_mut().zip(child.scalars) {
            target.extend(column);
        }
        for (target, column) in output.contexts.iter_mut().zip(child.contexts) {
            target.extend(column);
        }
    }
    Ok(output)
}

#[cfg(test)]
#[path = "custom_import_graph_projection_tests.rs"]
mod tests;
