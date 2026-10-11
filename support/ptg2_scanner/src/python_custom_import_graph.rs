use crate::custom_import_graph::{graph_children, graph_layout_bound, GraphBinding, GraphLayout};

fn graph_layout_input(layout: &Bound<'_, PyTuple>) -> PyResult<GraphLayout> {
    let layout = verification_tuple(layout.as_any(), 2)?;
    let source = layout.get_item(0)?;
    let source = source_layout_input(source.cast::<PyTuple>()?)?;
    let bindings = layout.get_item(1)?;
    let bindings = verification_tuple(&bindings, source.fields.len())?;
    let bindings = bindings
        .iter()
        .map(|binding| {
            let binding = verification_tuple(&binding, 3)?;
            if !binding.get_item(0)?.is_exact_instance_of::<PyBool>() {
                return Err(PyValueError::new_err("graph nullable binding differs"));
            }
            let nullable = binding.get_item(0)?.extract()?;
            let field_slot = i16::try_from(verification_integer(&binding.get_item(1)?)?)
                .map_err(|_| PyValueError::new_err("graph field slot differs"))?;
            let projection = binding.get_item(2)?;
            let projection_slot = if projection.is_none() {
                None
            } else {
                Some(
                    i16::try_from(verification_integer(&projection)?)
                        .map_err(|_| PyValueError::new_err("graph projection slot differs"))?,
                )
            };
            Ok(GraphBinding {
                nullable,
                field_slot,
                projection_slot,
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let layout = GraphLayout { source, bindings };
    graph_layout_bound(&layout).map_err(PyValueError::new_err)?;
    Ok(layout)
}

fn graph_payload_input(layout: &GraphLayout, rows: &Bound<'_, PyList>) -> PyResult<Vec<String>> {
    if rows.is_empty() || rows.len() > MAX_SOURCE_ROWS {
        return Err(PyValueError::new_err("graph row bound exceeded"));
    }
    let mut bytes = graph_layout_bound(layout).map_err(PyValueError::new_err)?;
    let mut payloads = Vec::with_capacity(rows.len());
    for row in rows.iter() {
        bytes += 64;
        // Do not invoke user __str__, __iter__ or integer coercion methods.
        // Nontext and non-UTF-8 records are unsupported at their own ordinal.
        if !row.is_exact_instance_of::<PyString>() {
            payloads.push(String::new());
            continue;
        }
        let row = row.cast::<PyString>()?;
        if row.len()? > MAX_SOURCE_INPUT_BYTES.saturating_sub(bytes) {
            return Err(PyValueError::new_err("graph input byte bound exceeded"));
        }
        let Some(text) = graph_payload_text(rows.py(), row.to_str())? else {
            payloads.push(String::new());
            continue;
        };
        bytes += text.len();
        if bytes > MAX_SOURCE_INPUT_BYTES {
            return Err(PyValueError::new_err("graph input byte bound exceeded"));
        }
        payloads.push(text.to_owned());
    }
    Ok(payloads)
}

fn graph_payload_text<'a>(py: Python<'_>, text: PyResult<&'a str>) -> PyResult<Option<&'a str>> {
    match text {
        Ok(text) => Ok(Some(text)),
        Err(error) if error.is_instance_of::<pyo3::exceptions::PyUnicodeEncodeError>(py) => {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

fn graph_py_value<'py, T>(py: Python<'py>, value: T) -> Bound<'py, PyAny>
where
    T: IntoPyObject<'py, Error = Infallible>,
{
    match value.into_pyobject(py) {
        Ok(value) => value.into_any().into_bound(),
        Err(error) => match error {},
    }
}

#[pyfunction]
fn custom_import_graph_children_v1<'py>(
    py: Python<'py>,
    layout: &Bound<'py, PyTuple>,
    payloads: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyList>> {
    // Share SOURCE admission: one bounded owned allocation window, no new pool.
    let _slot = py
        .detach(source_encoding_slot)
        .map_err(PyValueError::new_err)?;
    // Inputs may mutate while admission waits. Inspect only after reacquiring
    // the GIL, and copy all accepted text before native decoding detaches.
    let layout = graph_layout_input(layout)?;
    let payloads = graph_payload_input(&layout, payloads)?;
    let rows = py
        .detach(|| graph_children(&layout, &payloads))
        .map_err(PyValueError::new_err)?;
    let result = PyList::empty(py);
    for row in rows {
        let Some(row) = row else {
            result.append(py.None())?;
            continue;
        };
        let mut decoded = Vec::with_capacity(row.cells.len());
        let mut values = Vec::with_capacity(row.cells.len());
        for cell in &row.cells {
            let (state, value) = match cell {
                SourceValue::Missing => ("missing", py.None().into_bound(py)),
                SourceValue::Null => ("null", py.None().into_bound(py)),
                // Decimal text stays bounded and canonical. The caller uses its
                // captured constructor after this allocation slot is released.
                SourceValue::Text(value) => ("value", graph_py_value(py, value)),
                SourceValue::Integer(value) => ("value", graph_py_value(py, *value)),
                SourceValue::Boolean(value) => ("value", graph_py_value(py, *value)),
            };
            decoded.push(PyTuple::new(
                py,
                [graph_py_value(py, state), value.clone()],
            )?);
            values.push(value);
        }
        let mut scalars = Vec::with_capacity(row.projected_indices.len());
        for index in row.projected_indices {
            let binding = layout.bindings[index];
            let kind = &layout.source.fields[index].1;
            let mut typed = Vec::from_iter((0..6).map(|_| py.None().into_bound(py)));
            let state = if matches!(row.cells[index], SourceValue::Null) {
                "null"
            } else {
                let slot = match kind.as_str() {
                    "string" => 0,
                    "integer" => 1,
                    "decimal" => 2,
                    "boolean" => 3,
                    _ => return Err(PyValueError::new_err("graph native scalar type differs")),
                };
                typed[slot] = values[index].clone();
                "value"
            };
            let projection_slot = binding
                .projection_slot
                .ok_or_else(|| PyValueError::new_err("graph projection binding is unavailable"))?;
            let mut scalar = vec![
                graph_py_value(py, binding.field_slot),
                graph_py_value(py, projection_slot),
                graph_py_value(py, kind),
                graph_py_value(py, state),
            ];
            scalar.extend(typed);
            scalars.push(PyTuple::new(py, scalar)?);
        }
        let child_key = row
            .documents
            .child_key
            .ok_or_else(|| PyValueError::new_err("graph child key is unavailable"))?;
        let child_hash = row
            .documents
            .child_hash
            .ok_or_else(|| PyValueError::new_err("graph child digest is unavailable"))?;
        result.append((
            row.documents.root_key,
            PyBytes::new(py, &row.documents.root_hash),
            child_key,
            PyBytes::new(py, &child_hash),
            PyBytes::new(py, &row.documents.payload_hash),
            PyTuple::new(py, decoded)?,
            PyTuple::new(py, scalars)?,
            row.scalar_model_bytes,
        ))?;
    }
    Ok(result)
}

#[cfg(test)]
mod graph_python_tests {
    use super::*;
    use pyo3::ffi::c_str;

    fn layout<'py>(py: Python<'py>) -> Bound<'py, PyTuple> {
        py.eval(c_str!("((True, (('key', 'string'), ('amount', 'decimal'), ('active', 'boolean')), (('parent', 0),), (0,)), ((False, 9, 3), (True, 2, 1), (True, 8, 2)))"), None, None)
            .unwrap().cast_into::<PyTuple>().unwrap()
    }

    fn payload() -> &'static str {
        "{\"contract\":\"custom-import-record/v1\",\"fields\":[{\"field\":\"key\",\"value\":{\"state\":\"value\",\"type\":\"string\",\"value\":\"a\"}},{\"field\":\"amount\",\"value\":{\"state\":\"value\",\"type\":\"decimal\",\"value\":\"1.25\"}},{\"field\":\"active\",\"value\":{\"state\":\"null\",\"type\":\"boolean\"}}]}"
    }

    #[test]
    fn graph_python_returns_decimal_text_and_reuses_projection_object() {
        Python::initialize();
        Python::attach(|py| {
            let rows = PyList::new(py, [payload(), "{}", payload()]).unwrap();
            let result = custom_import_graph_children_v1(py, &layout(py), &rows).unwrap();
            assert_eq!(result.len(), 3);
            assert!(result.get_item(1).unwrap().is_none());
            let first = result.get_item(0).unwrap();
            let first = first.cast::<PyTuple>().unwrap();
            assert_eq!(first.len(), 8);
            assert_eq!(
                first
                    .get_item(1)
                    .unwrap()
                    .cast::<PyBytes>()
                    .unwrap()
                    .as_bytes()
                    .len(),
                32
            );
            let decoded = first.get_item(5).unwrap();
            let amount = decoded.get_item(1).unwrap().get_item(1).unwrap();
            assert!(amount.is_exact_instance_of::<PyString>());
            assert_eq!(amount.cast::<PyString>().unwrap().to_str().unwrap(), "1.25");
            let scalars = first.get_item(6).unwrap();
            assert_eq!(scalars.len().unwrap(), 3);
            let projected = scalars.get_item(1).unwrap();
            assert!(projected.get_item(6).unwrap().is(&amount));
            assert_eq!(projected.get_item(0).unwrap().extract::<i16>().unwrap(), 2);
            assert_eq!(projected.get_item(1).unwrap().extract::<i16>().unwrap(), 1);
            assert_eq!(
                first.get_item(7).unwrap().extract::<usize>().unwrap(),
                6 + 5 + 1 + 7 + 5 + 7 + 4
            );
        });
    }

    #[test]
    fn graph_python_never_looks_up_a_decimal_constructor_inside_admission() {
        Python::initialize();
        Python::attach(|py| {
            let globals = PyDict::new(py);
            globals
                .set_item(
                    "decode",
                    wrap_pyfunction!(custom_import_graph_children_v1, py).unwrap(),
                )
                .unwrap();
            globals.set_item("layout", layout(py)).unwrap();
            globals.set_item("payload", payload()).unwrap();
            py.run(
                c_str!(
                    r#"
import decimal
import threading

original_type = type(decimal)
test_thread = threading.get_ident()

class GuardedDecimalModule(original_type):
    def __getattribute__(self, name):
        if name == "Decimal" and threading.get_ident() == test_thread:
            raise AssertionError("native decoding looked up a reentrant constructor")
        return super().__getattribute__(name)

decimal.__class__ = GuardedDecimalModule
try:
    row = decode(layout, [payload])[0]
    assert row[5][1][1] == "1.25"
    assert row[6][1][6] is row[5][1][1]
finally:
    decimal.__class__ = original_type
"#
                ),
                Some(&globals),
                None,
            )
            .unwrap();
        });
    }

    #[test]
    fn graph_python_only_unicode_encoding_errors_select_fallback() {
        Python::initialize();
        Python::attach(|py| {
            assert_eq!(
                graph_payload_text(py, Ok("\u{03a9}")).unwrap(),
                Some("\u{03a9}")
            );
            let surrogate = py.eval(c_str!("'\\ud800'"), None, None).unwrap();
            assert!(
                graph_payload_text(py, surrogate.cast::<PyString>().unwrap().to_str())
                    .unwrap()
                    .is_none()
            );
            for error in [
                pyo3::exceptions::PyMemoryError::new_err("synthetic allocation failure"),
                PyRuntimeError::new_err("synthetic conversion failure"),
            ] {
                let expected = error.clone_ref(py);
                let actual = graph_payload_text(py, Err(error)).unwrap_err();
                assert!(actual.value(py).is(expected.value(py)));
            }
        });
    }

    #[test]
    fn graph_python_invalid_rows_and_bounds_preserve_order_and_admission() {
        Python::initialize();
        Python::attach(|py| {
            let rows = PyList::empty(py);
            rows.append(py.None()).unwrap();
            rows.append(py.eval(c_str!("'\\ud800'"), None, None).unwrap())
                .unwrap();
            rows.append(payload()).unwrap();
            let result = custom_import_graph_children_v1(py, &layout(py), &rows).unwrap();
            assert!(result.get_item(0).unwrap().is_none());
            assert!(result.get_item(1).unwrap().is_none());
            assert!(!result.get_item(2).unwrap().is_none());
            let too_many = PyList::new(py, vec![payload(); MAX_SOURCE_ROWS + 1]).unwrap();
            assert!(custom_import_graph_children_v1(py, &layout(py), &too_many).is_err());
            let too_wide = PyList::new(py, ["x".repeat(MAX_SOURCE_INPUT_BYTES)]).unwrap();
            assert!(custom_import_graph_children_v1(py, &layout(py), &too_wide).is_err());
            assert!(custom_import_graph_children_v1(py, &layout(py), &rows).is_ok());
        });
    }
}
