use crate::custom_import_source_columns::{
    outcome_bytes, prepare_column_row, validate_columns, Column, ColumnLayout, ColumnOutcome,
};

fn column_exact_tuple<'py>(
    value: &Bound<'py, PyAny>,
    size: Option<usize>,
) -> PyResult<Bound<'py, PyTuple>> {
    if !value.is_exact_instance_of::<PyTuple>() {
        return Err(PyValueError::new_err("source column tuple differs"));
    }
    let tuple = value.cast::<PyTuple>()?;
    if size.is_some_and(|length| tuple.len() != length) {
        return Err(PyValueError::new_err("source column tuple shape differs"));
    }
    Ok(tuple.clone())
}

fn column_bool(value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if !value.is_exact_instance_of::<PyBool>() {
        return Err(PyValueError::new_err("source column boolean differs"));
    }
    value.extract()
}

fn column_layout_input(value: &Bound<'_, PyTuple>) -> PyResult<ColumnLayout> {
    let value = column_exact_tuple(value.as_any(), Some(5))?;
    let child = column_bool(&value.get_item(0)?)?;
    let field_values = column_exact_tuple(&value.get_item(1)?, None)?;
    if !(1..=MAX_SOURCE_FIELDS).contains(&field_values.len()) {
        return Err(PyValueError::new_err("source field bound exceeded"));
    }
    let mut fields = Vec::with_capacity(field_values.len());
    let mut nullable = Vec::with_capacity(field_values.len());
    let mut projected = Vec::with_capacity(field_values.len());
    for value in field_values.iter() {
        let field = column_exact_tuple(&value, Some(4))?;
        let name = source_identifier(&field.get_item(0)?)?;
        bounded_scalar_text(&field.get_item(1)?, 16)?;
        let kind: String = field.get_item(1)?.extract()?;
        if !SCALAR_TYPES.contains(&kind.as_str()) || fields.iter().any(|(prior, _)| prior == &name)
        {
            return Err(PyValueError::new_err("source field declaration differs"));
        }
        fields.push((name, kind));
        nullable.push(column_bool(&field.get_item(2)?)?);
        projected.push(column_bool(&field.get_item(3)?)?);
    }
    let root_values = column_exact_tuple(&value.get_item(2)?, None)?;
    if !(1..=3).contains(&root_values.len()) {
        return Err(PyValueError::new_err("source root key bound exceeded"));
    }
    let mut root_key = Vec::with_capacity(root_values.len());
    for key in root_values.iter() {
        let key = column_exact_tuple(&key, Some(2))?;
        let name = source_identifier(&key.get_item(0)?)?;
        let index = source_index(&key.get_item(1)?, fields.len())?;
        if root_key.iter().any(|(prior, _)| prior == &name) {
            return Err(PyValueError::new_err("source root key differs"));
        }
        root_key.push((name, index));
    }
    let child_values = column_exact_tuple(&value.get_item(3)?, None)?;
    if (child && !(1..=3).contains(&child_values.len())) || (!child && !child_values.is_empty()) {
        return Err(PyValueError::new_err("source child key bound exceeded"));
    }
    let mut child_key = Vec::with_capacity(child_values.len());
    for key in child_values.iter() {
        let index = source_index(&key, fields.len())?;
        if child_key.contains(&index) {
            return Err(PyValueError::new_err("source child key differs"));
        }
        child_key.push(index);
    }
    let entity_value = value.get_item(4)?;
    let entity = if entity_value.is_none() {
        None
    } else {
        Some(source_index(&entity_value, fields.len())?)
    };
    if child && entity.is_some() {
        return Err(PyValueError::new_err("source child entity differs"));
    }
    Ok(ColumnLayout {
        source: SourceLayout {
            child,
            fields,
            root_key,
            child_key,
        },
        nullable,
        projected,
        entity,
    })
}

fn column_bytes_len(value: &Bound<'_, PyAny>, nullable: bool) -> PyResult<usize> {
    if nullable && value.is_none() {
        return Ok(0);
    }
    if !value.is_exact_instance_of::<PyBytes>() {
        return Err(PyValueError::new_err(
            "source column requires immutable bytes",
        ));
    }
    Ok(value.cast::<PyBytes>()?.as_bytes().len())
}

fn column_bytes_copy(value: &Bound<'_, PyAny>, nullable: bool) -> PyResult<Option<Vec<u8>>> {
    if nullable && value.is_none() {
        return Ok(None);
    }
    Ok(Some(value.cast::<PyBytes>()?.as_bytes().to_vec()))
}

/// One bounded immutable layout, never row buffers, results or an encoding slot.
#[pyclass(frozen, module = "ptg2_address_canon", name = "_SourceColumnsPlan")]
struct SourceColumnsPlan {
    layout: ColumnLayout,
    input_bytes: usize,
}

impl SourceColumnsPlan {
    fn compile(value: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let layout = column_layout_input(value)?;
        let input_bytes = layout
            .source
            .fields
            .iter()
            .map(|(name, kind)| 128 + name.len() + kind.len())
            .sum::<usize>()
            + layout
                .source
                .root_key
                .iter()
                .map(|(name, _)| 64 + name.len())
                .sum::<usize>()
            + 8 * layout.source.child_key.len();
        Ok(Self {
            layout,
            input_bytes,
        })
    }
}

#[pymethods]
impl SourceColumnsPlan {
    #[pyo3(signature = (columns, page_byte_limit, include_preparation_bounds=false))]
    fn prepare<'py>(
        &self,
        py: Python<'py>,
        columns: &Bound<'py, PyTuple>,
        page_byte_limit: &Bound<'py, PyAny>,
        include_preparation_bounds: bool,
    ) -> PyResult<Option<Bound<'py, PyList>>> {
        prepare_source_columns(
            py,
            self,
            columns,
            page_byte_limit,
            include_preparation_bounds,
        )
    }
}

#[pyfunction]
fn custom_import_source_columns_plan_v1(
    layout: &Bound<'_, PyTuple>,
) -> PyResult<SourceColumnsPlan> {
    SourceColumnsPlan::compile(layout)
}

#[pyfunction(signature = (layout, columns, page_byte_limit, include_preparation_bounds=false))]
fn custom_import_source_columns_v1<'py>(
    py: Python<'py>,
    layout: &Bound<'py, PyTuple>,
    columns: &Bound<'py, PyTuple>,
    page_byte_limit: &Bound<'py, PyAny>,
    include_preparation_bounds: bool,
) -> PyResult<Option<Bound<'py, PyList>>> {
    let plan = SourceColumnsPlan::compile(layout)?;
    prepare_source_columns(
        py,
        &plan,
        columns,
        page_byte_limit,
        include_preparation_bounds,
    )
}

fn prepare_source_columns<'py>(
    py: Python<'py>,
    plan: &SourceColumnsPlan,
    columns: &Bound<'py, PyTuple>,
    page_byte_limit: &Bound<'py, PyAny>,
    include_preparation_bounds: bool,
) -> PyResult<Option<Bound<'py, PyList>>> {
    let layout = &plan.layout;
    let page_limit = verification_integer(page_byte_limit)?;
    if page_limit < 1 {
        return Err(PyValueError::new_err("source column page limit differs"));
    }
    let columns = column_exact_tuple(columns.as_any(), None)?;
    if columns.len() != layout.source.fields.len() {
        return Err(PyValueError::new_err("source column field count differs"));
    }
    if layout
        .source
        .fields
        .iter()
        .any(|(_, kind)| matches!(kind.as_str(), "date" | "timestamp"))
    {
        return Ok(None);
    }
    // The retained layout is still charged against every operation's input cap.
    let mut input = plan.input_bytes;
    let mut row_count = None;
    // Tuples, bytes, strings and integer metadata are exact immutable primitives.
    // No owned Arrow bytes or scalar conversion precedes complete input admission.
    let mut descriptors = Vec::with_capacity(columns.len());
    for (index, value) in columns.iter().enumerate() {
        let descriptor = column_exact_tuple(&value, Some(9))?;
        let name = source_identifier(&descriptor.get_item(0)?)?;
        if name != layout.source.fields[index].0 {
            return Err(PyValueError::new_err("source column identity differs"));
        }
        bounded_scalar_text(&descriptor.get_item(1)?, 16)?;
        let kind: String = descriptor.get_item(1)?.extract()?;
        if !matches!(
            kind.as_str(),
            "int8"
                | "int16"
                | "int32"
                | "int64"
                | "bool"
                | "utf8"
                | "large_utf8"
                | "decimal128"
                | "null"
        ) {
            return Ok(None);
        }
        let bit_offset = verification_integer(&descriptor.get_item(2)?)?;
        let rows = verification_integer(&descriptor.get_item(3)?)?;
        let precision = verification_integer(&descriptor.get_item(4)?)?;
        let scale = verification_integer(&descriptor.get_item(5)?)?;
        if bit_offset < 0 || bit_offset > 7 {
            return Err(PyValueError::new_err("source column bit offset differs"));
        }
        if !(1..=MAX_SOURCE_ROWS as i64).contains(&rows) {
            return Ok(None);
        }
        if row_count.is_some_and(|count| count != rows as usize) {
            return Err(PyValueError::new_err("source column row count differs"));
        }
        if row_count.is_none() {
            input += rows as usize * (64 + 64 * columns.len());
            row_count = Some(rows as usize);
        }
        input += 128 + name.len() + kind.len();
        for item in 6..=8 {
            input += column_bytes_len(&descriptor.get_item(item)?, item < 8)?;
        }
        descriptors.push((
            descriptor,
            kind,
            bit_offset as usize,
            rows as usize,
            precision,
            scale,
        ));
    }
    if input > MAX_SOURCE_INPUT_BYTES {
        return Ok(None);
    }
    let slot = py
        .detach(source_encoding_slot)
        .map_err(PyValueError::new_err)?;
    let owned = descriptors
        .into_iter()
        .map(|(value, kind, bit_offset, rows, precision, scale)| {
            Ok(Column {
                kind,
                bit_offset,
                rows,
                precision,
                scale,
                validity: column_bytes_copy(&value.get_item(6)?, true)?,
                offsets: column_bytes_copy(&value.get_item(7)?, true)?,
                data: column_bytes_copy(&value.get_item(8)?, false)?.unwrap(),
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let computed = py.detach(move || {
        if !validate_columns(layout, &owned, include_preparation_bounds).map_err(str::to_owned)? {
            return Ok(None);
        }
        let outcomes = if owned[0].rows == 1 {
            vec![prepare_column_row(layout, &owned, 0, page_limit as usize)
                .map_err(str::to_owned)?]
        } else {
            let pool = source_document_pool()?;
            pool.install(|| {
                (0..owned[0].rows)
                    .into_par_iter()
                    .map(|row| prepare_column_row(layout, &owned, row, page_limit as usize))
                    .collect::<Vec<_>>()
            })
            .into_iter()
            .collect::<Result<Vec<_>, _>>()
            .map_err(str::to_owned)?
        };
        let extra_metadata_bytes = if include_preparation_bounds {
            256 * outcomes.len()
        } else {
            0
        };
        if outcomes.iter().map(outcome_bytes).sum::<usize>() + extra_metadata_bytes
            > MAX_SOURCE_OUTPUT_BYTES
        {
            return Err("source column output byte bound exceeded".to_owned());
        }
        Ok(Some(outcomes))
    });
    drop(slot);
    let outcomes = computed.map_err(PyValueError::new_err)?;
    let Some(outcomes) = outcomes else {
        return Ok(None);
    };
    let result = PyList::empty(py);
    for outcome in outcomes {
        let (input_bytes, output_bytes) = outcome.preparation_bounds();
        let encoded_outcome = match outcome {
            ColumnOutcome::Accepted {
                raw,
                documents,
                bytes,
                transport,
                ..
            } => (
                raw.0,
                PyBytes::new(py, &raw.1),
                documents.root_key,
                PyBytes::new(py, &documents.root_hash),
                documents.payload,
                PyBytes::new(py, &documents.payload_hash),
                documents.child_key,
                documents.child_hash.map(|v| PyBytes::new(py, &v)),
                bytes,
                transport,
            )
                .into_pyobject(py)?,
            ColumnOutcome::Rejected {
                code,
                raw,
                typed,
                transport,
            } => {
                let (raw_text, raw_hash) =
                    raw.map_or((None, None), |v| (Some(v.0), Some(PyBytes::new(py, &v.1))));
                let (typed_text, typed_hash) =
                    typed.map_or((None, None), |v| (Some(v.0), Some(PyBytes::new(py, &v.1))));
                (code, raw_text, raw_hash, typed_text, typed_hash, transport).into_pyobject(py)?
            }
            ColumnOutcome::Error {
                category,
                code,
                transport,
                ..
            } => (category, code, transport).into_pyobject(py)?,
        };
        if include_preparation_bounds {
            result.append((encoded_outcome, input_bytes, output_bytes))?;
        } else {
            result.append(encoded_outcome)?;
        }
    }
    Ok(Some(result))
}
