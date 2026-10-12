use crate::custom_import_graph_projection::{
    projection_columns, projection_layout_bound, ColumnValue, ProjectionCollection,
    ProjectionInput, ProjectionLayout, ProjectionProfile, ProjectionRoot, QueryField,
};

fn projection_indices(
    value: &Bound<'_, PyAny>,
    fields: usize,
    maximum: usize,
) -> PyResult<Vec<usize>> {
    let values = value.cast::<PyTuple>()?;
    if values.len() > maximum {
        return Err(PyValueError::new_err(
            "graph projection index bound exceeded",
        ));
    }
    values
        .iter()
        .map(|item| source_index(&item, fields))
        .collect()
}

fn projection_profiles(
    value: &Bound<'_, PyAny>,
    fields: usize,
) -> PyResult<Vec<ProjectionProfile>> {
    let values = value.cast::<PyTuple>()?;
    if values.len() > 4 {
        return Err(PyValueError::new_err(
            "graph projection profile bound exceeded",
        ));
    }
    values
        .iter()
        .map(|value| {
            let value = verification_tuple(&value, 4)?;
            Ok(ProjectionProfile {
                slot: i16::try_from(verification_integer(&value.get_item(0)?)?)
                    .map_err(|_| PyValueError::new_err("graph profile slot differs"))?,
                id: source_identifier(&value.get_item(1)?)?,
                selection: projection_indices(&value.get_item(2)?, fields, 3)?,
                dimensions: projection_indices(&value.get_item(3)?, fields, 2)?,
            })
        })
        .collect()
}

fn projection_layout_input(value: &Bound<'_, PyTuple>) -> PyResult<ProjectionLayout> {
    let value = verification_tuple(value.as_any(), 2)?;
    let roots = value.get_item(0)?;
    let roots = roots.cast::<PyTuple>()?;
    if roots.len() > 20 {
        return Err(PyValueError::new_err(
            "graph projection root field bound exceeded",
        ));
    }
    let roots = roots
        .iter()
        .map(|field| {
            let field = verification_tuple(&field, 3)?;
            bounded_scalar_text(&field.get_item(0)?, 16)?;
            if !field.get_item(1)?.is_exact_instance_of::<PyBool>() {
                return Err(PyValueError::new_err("graph query nullable differs"));
            }
            Ok(QueryField {
                kind: field.get_item(0)?.extract()?,
                nullable: field.get_item(1)?.extract()?,
                slot: i16::try_from(verification_integer(&field.get_item(2)?)?)
                    .map_err(|_| PyValueError::new_err("graph query field slot differs"))?,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let collections = value.get_item(1)?;
    let collections = collections.cast::<PyTuple>()?;
    if collections.is_empty() || collections.len() > 8 {
        return Err(PyValueError::new_err(
            "graph projection collection bound exceeded",
        ));
    }
    let collections = collections
        .iter()
        .map(|collection| {
            let collection = verification_tuple(&collection, 4)?;
            let graph = collection.get_item(1)?;
            let graph = graph_layout_input(graph.cast::<PyTuple>()?)?;
            let query_indices =
                projection_indices(&collection.get_item(2)?, graph.bindings.len(), 20)?;
            let profiles =
                projection_profiles(&collection.get_item(3)?, roots.len() + query_indices.len())?;
            Ok(ProjectionCollection {
                slot: i16::try_from(verification_integer(&collection.get_item(0)?)?)
                    .map_err(|_| PyValueError::new_err("graph collection slot differs"))?,
                graph,
                query_indices,
                profiles,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let layout = ProjectionLayout { roots, collections };
    projection_layout_bound(&layout).map_err(PyValueError::new_err)?;
    Ok(layout)
}

fn projection_charge(bytes: &mut usize, extra: usize) -> PyResult<()> {
    *bytes = bytes.saturating_add(extra);
    if *bytes > MAX_SOURCE_INPUT_BYTES {
        return Err(PyValueError::new_err(
            "graph projection input byte bound exceeded",
        ));
    }
    Ok(())
}

fn projection_text(value: &Bound<'_, PyAny>, bytes: &mut usize) -> PyResult<String> {
    if value.is_none() {
        return Err(PyValueError::new_err("graph projection text differs"));
    }
    bounded_scalar_text(value, MAX_SOURCE_INPUT_BYTES.saturating_sub(*bytes))?;
    let value = value.cast::<PyString>()?.to_str()?;
    projection_charge(bytes, value.len())?;
    Ok(value.to_owned())
}

fn projection_hash(value: &Bound<'_, PyAny>, bytes: &mut usize) -> PyResult<[u8; 32]> {
    if !value.is_exact_instance_of::<PyBytes>() {
        return Err(PyValueError::new_err("graph projection digest differs"));
    }
    let value = value.cast::<PyBytes>()?.as_bytes();
    let value = value
        .try_into()
        .map_err(|_| PyValueError::new_err("graph projection digest differs"))?;
    projection_charge(bytes, 32)?;
    Ok(value)
}

fn projection_roots_input(
    layout: &ProjectionLayout,
    values: &Bound<'_, PyList>,
    bytes: &mut usize,
) -> PyResult<Vec<Option<ProjectionRoot>>> {
    if values.len() > MAX_SOURCE_ROWS {
        return Err(PyValueError::new_err(
            "graph projection root bound exceeded",
        ));
    }
    values
        .iter()
        .map(|value| {
            projection_charge(bytes, 64)?;
            if value.is_none() {
                return Ok(None);
            }
            let value = verification_tuple(&value, 6)?;
            let cells = value.get_item(5)?;
            let cells = verification_tuple(&cells, layout.roots.len())?;
            let key = projection_text(&value.get_item(0)?, bytes)?;
            let hash = projection_hash(&value.get_item(1)?, bytes)?;
            let family_hash = projection_hash(&value.get_item(4)?, bytes)?;
            let cells = cells
                .iter()
                .zip(&layout.roots)
                .map(|(cell, field)| {
                    let cell = source_cell_input(&cell, &field.kind)?;
                    projection_charge(bytes, 64 + source_value_bytes(&cell))?;
                    Ok(cell)
                })
                .collect::<PyResult<Vec<_>>>()?;
            Ok(Some(ProjectionRoot {
                key,
                hash,
                family_hash,
                cells,
                entity_id: verification_integer(&value.get_item(2)?)?,
                family_id: verification_integer(&value.get_item(3)?)?,
            }))
        })
        .collect()
}

fn projection_rows_input(
    layout: &ProjectionLayout,
    roots: usize,
    values: &Bound<'_, PyList>,
    bytes: &mut usize,
) -> PyResult<Vec<Option<ProjectionInput>>> {
    if values.is_empty() || values.len() > MAX_SOURCE_ROWS {
        return Err(PyValueError::new_err("graph projection row bound exceeded"));
    }
    values
        .iter()
        .map(|value| {
            projection_charge(bytes, 64)?;
            if value.is_none() {
                return Ok(None);
            }
            let value = verification_tuple(&value, 9)?;
            Ok(Some(ProjectionInput {
                collection: source_index(&value.get_item(0)?, layout.collections.len())?,
                root: source_index(&value.get_item(1)?, roots)?,
                child_id: verification_integer(&value.get_item(2)?)?,
                parent_key: projection_text(&value.get_item(3)?, bytes)?,
                parent_hash: projection_hash(&value.get_item(4)?, bytes)?,
                child_key: projection_text(&value.get_item(5)?, bytes)?,
                child_hash: projection_hash(&value.get_item(6)?, bytes)?,
                payload_hash: projection_hash(&value.get_item(7)?, bytes)?,
                payload: projection_text(&value.get_item(8)?, bytes)?,
            }))
        })
        .collect()
}

fn projection_column_output<'py>(
    py: Python<'py>,
    column: Vec<ColumnValue>,
) -> PyResult<Bound<'py, PyTuple>> {
    PyTuple::new(
        py,
        column.into_iter().map(|value| match value {
            ColumnValue::Null => py.None().into_bound(py),
            ColumnValue::Text(value) => graph_py_value(py, value),
            ColumnValue::Integer(value) => graph_py_value(py, value),
            ColumnValue::Boolean(value) => graph_py_value(py, value),
            ColumnValue::Hash(value) => PyBytes::new(py, &value).into_any(),
        }),
    )
}

#[pyfunction]
fn custom_import_graph_projection_columns_v1<'py>(
    py: Python<'py>,
    layout: &Bound<'py, PyTuple>,
    roots: &Bound<'py, PyList>,
    rows: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyTuple>> {
    let _slot = py
        .detach(source_encoding_slot)
        .map_err(PyValueError::new_err)?;
    // Copy only after admission, with the GIL held; no Python object enters Rayon.
    let layout = projection_layout_input(layout)?;
    let mut bytes = projection_layout_bound(&layout).map_err(PyValueError::new_err)?;
    let roots = projection_roots_input(&layout, roots, &mut bytes)?;
    let rows = projection_rows_input(&layout, roots.len(), rows, &mut bytes)?;
    let result = py
        .detach(|| projection_columns(&layout, &roots, &rows, bytes))
        .map_err(PyValueError::new_err)?;
    let spans = PyTuple::new(py, result.rows)?;
    let scalars = PyTuple::new(
        py,
        result
            .scalars
            .into_iter()
            .map(|column| projection_column_output(py, column))
            .collect::<PyResult<Vec<_>>>()?,
    )?;
    let contexts = PyTuple::new(
        py,
        result
            .contexts
            .into_iter()
            .map(|column| projection_column_output(py, column))
            .collect::<PyResult<Vec<_>>>()?,
    )?;
    PyTuple::new(
        py,
        [spans.into_any(), scalars.into_any(), contexts.into_any()],
    )
}

#[cfg(test)]
mod graph_projection_python_tests {
    use super::*;
    use pyo3::ffi::c_str;

    #[test]
    fn projection_wire_bounds_reject_before_output_and_keep_admission_reusable() {
        Python::initialize();
        Python::attach(|py| {
            let globals = PyDict::new(py);
            globals
                .set_item(
                    "project",
                    pyo3::wrap_pyfunction!(custom_import_graph_projection_columns_v1, py)
                        .unwrap(),
                )
                .unwrap();
            py.run(
                c_str!(r#"
graph = ((True, (('key', 'string'),), (('parent', 0),), (0,)), ((False, 2, 1),))
profile = (1, 'default', (), (0,))
collection = (1, graph, (0,), (profile,))
layout = ((('string', False, 1),), (collection,))
root = ('parent', b'h' * 32, 1, 2, b'f' * 32, (('value', 'display'),))
row = (0, 0, 1, 'parent', b'h' * 32, 'child', b'c' * 32, b'p' * 32, '{}')
empty = ((None,), ((),) * 10, ((),) * 4)
assert project(layout, [root], [None]) == empty
cases = [
    ((), [root], [None]),
    ((layout[0] * 21, layout[1]), [root], [None]),
    ((layout[0], ()), [root], [None]),
    ((layout[0], layout[1] * 9), [root], [None]),
    (((('string', 1, 1),), layout[1]), [root], [None]),
    (((('string', False),), layout[1]), [root], [None]),
    (((('string', False, 40000),), layout[1]), [root], [None]),
    (((('timestamp', False, 1),), layout[1]), [root], [None]),
    ((layout[0], ((40000, *collection[1:]),)), [root], [None]),
    ((layout[0], (collection[:-1],)), [root], [None]),
    ((layout[0], (collection, collection)), [root], [None]),
    ((layout[0], ((1, graph, (0, 0), (profile,)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), (profile,) * 5),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), (profile[:-1],)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), ((1, '', (), (0,)),)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), ((40000, *profile[1:]),)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), ((1, 'default', (0,) * 4, (0,)),)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), ((1, 'default', (), (0,) * 3),)),)), [root], [None]),
    ((layout[0], ((1, graph, (0,), ((1, 'default', (), (0, 0)),)),)), [root], [None]),
    (layout, [root] * 65, [None]),
    (layout, [root], []),
    (layout, [root], [None] * 65),
    (layout, [root[:-1]], [None]),
    (layout, [(*root[:5], ())], [None]),
    (layout, [(*root[:5], (('value', False),))], [None]),
    (layout, [(*root[:2], False, *root[3:])], [None]),
    (layout, [(*root[:3], False, *root[4:])], [None]),
    (layout, [(None, *root[1:])], [None]),
    (layout, [(root[0], bytearray(32), *root[2:])], [None]),
    (layout, [(root[0], b'short', *root[2:])], [None]),
    (layout, [(*root[:4], b'short', root[5])], [None]),
    (layout, [root], [row[:-1]]),
    (layout, [root], [(False, *row[1:])]),
    (layout, [root], [(*row[:2], False, *row[3:])]),
    (layout, [root], [(*row[:3], None, *row[4:])]),
    (layout, [root], [(*row[:4], b'short', *row[5:])]),
    (layout, [root], [(*row[:5], None, *row[6:])]),
    (layout, [root], [(*row[:6], b'short', *row[7:])]),
    (layout, [root], [(*row[:7], b'short', row[8])]),
    (layout, [root], [(*row[:8], None)]),
]
wide_layout = (tuple(('string', False, slot) for slot in range(1, 21)), ((1, graph, (), ()),))
wide_root = (*root[:5], (('value', 'x' * 8192),) * 20)
cases.append((wide_layout, [wide_root], [None]))
for bad_layout, bad_roots, bad_rows in cases:
    try:
        project(bad_layout, bad_roots, bad_rows)
    except ValueError:
        pass
    else:
        raise AssertionError('malformed projection packet escaped')
    assert project(layout, [root], [None]) == empty
assert project(layout, [root], [row]) == empty
"#),
                Some(&globals),
                Some(&globals),
            )
            .unwrap();
        });
    }

    #[test]
    fn projection_admission_rechecks_mutated_inputs_and_releases_on_error() {
        Python::initialize();
        let slot = source_encoding_slot().unwrap();
        let (layout, roots, rows, mutable_rows) = Python::attach(|py| {
            let layout = py.eval(c_str!("((), ((1, ((True, (('key', 'string'),), (('parent', 0),), (0,)), ((False, 1, 1),)), (), ()),))"), None, None).unwrap().cast_into::<PyTuple>().unwrap().unbind();
            let roots = PyList::empty(py).unbind();
            let rows = PyList::new(py, [py.None()]).unwrap().unbind();
            let mutable_rows = rows.clone_ref(py);
            (layout, roots, rows, mutable_rows)
        });
        let (started, ready) = std::sync::mpsc::channel();
        let waiting = std::thread::spawn(move || {
            Python::attach(|py| {
                started.send(()).unwrap();
                let failed = custom_import_graph_projection_columns_v1(
                    py,
                    layout.bind(py),
                    roots.bind(py),
                    rows.bind(py),
                )
                .unwrap_err();
                assert!(failed.is_instance_of::<PyValueError>(py));
                rows.bind(py).del_slice(1, rows.bind(py).len()).unwrap();
                let recovered = custom_import_graph_projection_columns_v1(
                    py,
                    layout.bind(py),
                    roots.bind(py),
                    rows.bind(py),
                )
                .unwrap();
                assert!(recovered
                    .get_item(0)
                    .unwrap()
                    .get_item(0)
                    .unwrap()
                    .is_none());
            })
        });
        ready
            .recv_timeout(std::time::Duration::from_secs(5))
            .unwrap();
        Python::attach(|py| {
            for _ in 0..MAX_SOURCE_ROWS {
                mutable_rows.bind(py).append(py.None()).unwrap();
            }
        });
        drop(slot);
        waiting.join().unwrap();
    }
}
