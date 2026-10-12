use super::*;
use crate::custom_import_graph::GraphBinding;
use crate::custom_import_source::{source_documents, SourceLayout};

fn fixture(
    count: usize,
) -> (
    ProjectionLayout,
    Vec<Option<ProjectionRoot>>,
    Vec<Option<ProjectionInput>>,
) {
    let graph = GraphLayout {
        source: SourceLayout {
            child: true,
            fields: vec![
                ("key".into(), "string".into()),
                ("amount".into(), "decimal".into()),
            ],
            root_key: vec![("parent".into(), 0)],
            child_key: vec![0],
        },
        bindings: vec![
            GraphBinding {
                nullable: false,
                field_slot: 2,
                projection_slot: Some(1),
            },
            GraphBinding {
                nullable: true,
                field_slot: 3,
                projection_slot: Some(2),
            },
        ],
    };
    let documents = source_documents(
        &graph.source,
        &[
            SourceValue::Text("key".into()),
            SourceValue::Text("1.25".into()),
        ],
    )
    .unwrap();
    let rows = (1..=count)
        .map(|ordinal| {
            Some(ProjectionInput {
                collection: 0,
                root: 0,
                child_id: ordinal as i64,
                parent_key: documents.root_key.clone(),
                parent_hash: documents.root_hash,
                child_key: documents.child_key.clone().unwrap(),
                child_hash: documents.child_hash.unwrap(),
                payload_hash: documents.payload_hash,
                payload: documents.payload.clone(),
            })
        })
        .collect();
    let roots = vec![Some(ProjectionRoot {
        key: documents.root_key,
        hash: documents.root_hash,
        entity_id: 1,
        family_id: 2,
        family_hash: [7; 32],
        cells: vec![SourceValue::Text("Ω\n".into())],
    })];
    let layout = ProjectionLayout {
        roots: vec![QueryField {
            kind: "string".into(),
            nullable: false,
            slot: 1,
        }],
        collections: vec![ProjectionCollection {
            slot: 1,
            graph,
            query_indices: vec![0, 1],
            profiles: vec![ProjectionProfile {
                slot: 1,
                id: "default".into(),
                selection: vec![2],
                dimensions: vec![0, 2],
            }],
        }],
    };
    (layout, roots, rows)
}

#[test]
fn parallel_projection_keeps_order_context_hashes_and_exact_costs() {
    let (layout, roots, rows) = fixture(64);
    let result = projection_columns(&layout, &roots, &rows, 65_536).unwrap();
    let canonical = "{\"dimensions\":[{\"field_slot\":1,\"field_type\":\"string\",\"value\":\"Ω\\n\",\"value_state\":\"value\"},{\"field_slot\":3,\"field_type\":\"decimal\",\"value\":\"1.25\",\"value_state\":\"value\"}],\"profile_id\":\"default\",\"scope\":\"child\"}";
    for (index, span) in result.rows.iter().enumerate() {
        assert_eq!(
            *span,
            Some((
                index * 2,
                2,
                index,
                1,
                26,
                canonical.len() + 32,
                570 + canonical.len()
            ))
        );
        assert!(
            matches!(&result.scalars[0][index * 2], ColumnValue::Integer(identity) if *identity == index as i64 + 1)
        );
        assert!(matches!(&result.contexts[2][index], ColumnValue::Text(key) if key == canonical));
        let mut digest = Sha256::new();
        digest.update(CONTEXT_PREFIX);
        digest.update(canonical.as_bytes());
        assert!(
            matches!(&result.contexts[3][index], ColumnValue::Hash(hash) if *hash == <[u8; 32]>::from(digest.finalize()))
        );
    }
}

#[test]
fn ordered_unsupported_rows_do_not_expose_later_semantic_errors() {
    let (layout, roots, mut rows) = fixture(4);
    rows[1].as_mut().unwrap().payload = "{}".into();
    rows[2].as_mut().unwrap().payload_hash = [0; 32];
    let result = projection_columns(&layout, &roots, &rows, 4096).unwrap();
    assert!(result.rows[0].is_some());
    assert!(result.rows[1].is_none());
    assert!(result.rows[2].is_none());
    assert_eq!(result.rows[3].unwrap().0, 2);
    assert_eq!(result.scalars[0].len(), 4);
}

#[test]
fn aggregate_reservation_rejects_before_any_outcomes_escape() {
    let (layout, roots, rows) = fixture(64);
    assert!(projection_columns(&layout, &roots, &rows, MAX_SOURCE_INPUT_BYTES + 1).is_err());
    let (mut layout, mut roots, rows) = fixture(64);
    roots[0].as_mut().unwrap().cells[0] = SourceValue::Text("x".repeat(4096));
    layout.collections[0].profiles = (1..=4)
        .map(|slot| ProjectionProfile {
            slot,
            id: format!("profile_{slot}"),
            selection: vec![2],
            dimensions: vec![0, 2],
        })
        .collect();
    assert!(projection_workspace_bound(&layout, &roots, &rows).unwrap() > MAX_SOURCE_OUTPUT_BYTES);
    assert!(projection_columns(&layout, &roots, &rows, 65_536).is_err());
    roots[0] = None;
    assert_eq!(
        projection_workspace_bound(&layout, &roots, &rows),
        Ok(64 * 16_384)
    );
    assert!(projection_columns(&layout, &roots, &rows, 65_536)
        .unwrap()
        .rows
        .iter()
        .all(Option::is_none));
}

#[test]
fn dedicated_pool_has_only_the_bounded_native_workers() {
    let pool = projection_pool().unwrap();
    let expected = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(4)
        .min(8);
    assert_eq!(pool.current_num_threads(), expected);
    let indices = pool.broadcast(|context| context.index());
    assert_eq!(indices, (0..expected).collect::<Vec<_>>());
}
