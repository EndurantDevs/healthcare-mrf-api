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

#[test]
fn projection_layout_rejects_ambiguous_slots_and_profile_indices() {
    for invalid in 0..10 {
        let (mut layout, roots, rows) = fixture(1);
        match invalid {
            0 => layout.collections.clear(),
            1 => layout.roots[0].slot = 0,
            2 => layout.roots[0].kind = "timestamp".into(),
            3 => layout.collections[0].slot = 0,
            4 => layout.collections[0].query_indices = vec![0, 0],
            5 => layout.collections[0].query_indices = vec![2],
            6 => layout.collections[0].profiles[0].slot = 0,
            7 => layout.collections[0].profiles[0].id.clear(),
            8 => layout.collections[0].profiles[0].selection = vec![3],
            9 => layout.collections[0].profiles[0].dimensions = vec![0, 0],
            _ => unreachable!(),
        }
        assert!(projection_layout_bound(&layout).is_err(), "case {invalid}");
        assert!(projection_columns(&layout, &roots, &rows, 4096).is_err());
    }
}

#[test]
fn projection_batch_rejects_invalid_shapes_and_indices_before_workers() {
    for invalid in 0..6 {
        let (layout, mut roots, mut rows) = fixture(1);
        let mut expected = "graph projection batch bound exceeded";
        match invalid {
            0 => rows.clear(),
            1 => rows.resize_with(65, || None),
            2 => roots.resize_with(65, || None),
            3 => roots[0].as_mut().unwrap().cells.clear(),
            4 => {
                rows[0].as_mut().unwrap().collection = 1;
                expected = "graph projection collection index differs";
            }
            5 => {
                rows[0].as_mut().unwrap().root = 1;
                expected = "graph projection root index differs";
            }
            _ => unreachable!(),
        }
        assert_eq!(
            projection_columns(&layout, &roots, &rows, 4096)
                .err()
                .as_deref(),
            Some(expected),
            "case {invalid}"
        );
    }
}

#[test]
fn projection_distinguishes_typed_null_and_missing_context_values() {
    let (mut layout, mut roots, rows) = fixture(1);
    layout.roots = vec![
        QueryField {
            kind: "integer".into(),
            nullable: true,
            slot: 1,
        },
        QueryField {
            kind: "boolean".into(),
            nullable: true,
            slot: 4,
        },
    ];
    layout.collections[0].profiles[0].selection = vec![3];
    layout.collections[0].profiles[0].dimensions = vec![0, 1];
    for (cells, expected) in [
        (
            vec![SourceValue::Integer(-7), SourceValue::Boolean(false)],
            r#"[{"field_slot":1,"field_type":"integer","value":-7,"value_state":"value"},{"field_slot":4,"field_type":"boolean","value":false,"value_state":"value"}]"#,
        ),
        (
            vec![SourceValue::Null, SourceValue::Missing],
            r#"[{"field_slot":1,"field_type":"integer","value":null,"value_state":"null"},{"field_slot":4,"field_type":"boolean","value":null,"value_state":"missing"}]"#,
        ),
    ] {
        roots[0].as_mut().unwrap().cells = cells;
        let result = projection_columns(&layout, &roots, &rows, 4096).unwrap();
        let canonical =
            format!(r#"{{"dimensions":{expected},"profile_id":"default","scope":"child"}}"#);
        assert!(result.rows[0].is_some());
        assert!(matches!(&result.contexts[2][0], ColumnValue::Text(value) if value == &canonical));
        let hash: [u8; 32] = Sha256::digest([CONTEXT_PREFIX, canonical.as_bytes()].concat()).into();
        assert!(matches!(&result.contexts[3][0], ColumnValue::Hash(value) if value == &hash));
    }
    layout.roots[1].nullable = false;
    let result = projection_columns(&layout, &roots, &rows, 4096).unwrap();
    assert_eq!(result.rows, vec![None]);
    assert!(result.scalars.iter().all(Vec::is_empty));
    assert!(result.contexts.iter().all(Vec::is_empty));
}

#[test]
fn projection_falls_back_without_leaking_partial_columns() {
    for invalid in 0..6 {
        let (mut layout, mut roots, rows) = fixture(1);
        match invalid {
            0 => roots[0].as_mut().unwrap().entity_id = 0,
            1 => roots[0].as_mut().unwrap().family_id = 0,
            2 => roots[0].as_mut().unwrap().cells[0] = SourceValue::Boolean(true),
            3 => roots[0].as_mut().unwrap().cells[0] = SourceValue::Null,
            4 => {
                roots[0].as_mut().unwrap().cells[0] = SourceValue::Text("\u{1}".repeat(2048));
                layout.collections[0].profiles[0].dimensions = vec![0];
            }
            5 => layout.roots[0].kind = "integer".into(),
            _ => unreachable!(),
        }
        let result = projection_columns(&layout, &roots, &rows, 4096).unwrap();
        assert_eq!(result.rows, vec![None], "case {invalid}");
        assert!(result.scalars.iter().all(Vec::is_empty));
        assert!(result.contexts.iter().all(Vec::is_empty));
        layout.collections[0].profiles.clear();
        let scalars_only = projection_columns(&layout, &roots, &rows, 4096).unwrap();
        assert_eq!(scalars_only.rows[0].unwrap().1, 2);
        assert!(scalars_only.contexts.iter().all(Vec::is_empty));
    }
}
