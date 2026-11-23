fn provider_directory_value<'py>(
    py: Python<'py>,
    value: &crate::provider_directory_rows::RowValue,
) -> PyResult<Bound<'py, PyAny>> {
    use crate::provider_directory_rows::RowValue;
    Ok(match value {
        RowValue::Null => py.None().into_bound(py),
        RowValue::Bool(flag) => flag.into_pyobject(py)?.to_owned().into_any(),
        RowValue::Int(text) => match text.parse::<i64>() {
            Ok(number) => number.into_pyobject(py)?.into_any(),
            Err(_) => py.get_type::<pyo3::types::PyInt>().call1((text.as_str(),))?,
        },
        RowValue::Float(_) => return Err(PyValueError::new_err("provider_directory_rows_float_value")),
        RowValue::Str(text) => text.into_pyobject(py)?.into_any(),
        RowValue::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(provider_directory_value(py, item)?)?;
            }
            list.into_any()
        }
        RowValue::Object(fields) => provider_directory_dict(py, fields)?.into_any(),
    })
}

fn provider_directory_dict<'py>(
    py: Python<'py>,
    fields: &[(String, crate::provider_directory_rows::RowValue)],
) -> PyResult<Bound<'py, PyDict>> {
    let dict = PyDict::new(py);
    for (name, value) in fields {
        dict.set_item(name, provider_directory_value(py, value)?)?;
    }
    Ok(dict)
}

/// The batch context, read from a dict with exactly these keys.
#[derive(FromPyObject)]
struct ProviderDirectoryRowsContext {
    #[pyo3(item)]
    source_id: String,
    #[pyo3(item)]
    dataset_id: Option<String>,
    #[pyo3(item)]
    resource_hash_contract: String,
    #[pyo3(item)]
    merge_roles: bool,
    #[pyo3(item)]
    resource_url: Option<String>,
    #[pyo3(item)]
    self_url: Option<String>,
    #[pyo3(item)]
    fetch_url: Option<String>,
    #[pyo3(item)]
    fetch_mode: Option<String>,
    #[pyo3(item)]
    payload_text: bool,
    #[pyo3(item)]
    strict: bool,
}

impl ProviderDirectoryRowsContext {
    fn rows_context<'a>(&'a self, resource_type: &'a str) -> crate::provider_directory_rows::Context<'a> {
        crate::provider_directory_rows::Context {
            source_id: &self.source_id,
            resource_type,
            resource_url: self.resource_url.as_deref(),
            self_url: self.self_url.as_deref(),
            fetch_url: self.fetch_url.as_deref(),
            fetch_mode: self.fetch_mode.as_deref(),
            dataset_id: self.dataset_id.as_deref(),
            resource_hash_contract: &self.resource_hash_contract,
            merge_roles: self.merge_roles,
            payload_text: self.payload_text,
        }
    }
}

fn provider_directory_line_bytes<'py>(lines: &Bound<'py, PyList>) -> PyResult<Vec<Bound<'py, PyBytes>>> {
    lines
        .iter()
        .map(|line| line.cast_into::<PyBytes>().map_err(PyErr::from))
        .collect()
}

/// Prepare one NDJSON batch exactly as the Python chain does, or report it unsupported.
///
/// Returns `("rows", rows, rows_fetched, payload_texts, merged_roles)` or `("unsupported", reason)`.
/// `merged_roles` holds `(base_hash, entry_hashes, composed_hash)` per merged-role row, else `None`.
#[pyfunction]
fn prepare_provider_directory_rows_v1<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
    resource_type: &str,
    context: ProviderDirectoryRowsContext,
) -> PyResult<Bound<'py, pyo3::types::PyTuple>> {
    let owned_lines = provider_directory_line_bytes(lines)?;
    let line_bytes: Vec<&[u8]> = owned_lines.iter().map(|line| line.as_bytes()).collect();
    let dataset_id = context.dataset_id.as_deref();
    let prepared = match py.detach(|| {
        crate::provider_directory_rows::prepare(&line_bytes, &context.rows_context(resource_type))
    }) {
        Ok(prepared) => prepared,
        Err(reason) => return pyo3::types::PyTuple::new(py, ["unsupported", reason.0]),
    };
    let rows = PyList::empty(py);
    let texts = PyList::empty(py);
    let merged_roles = PyList::empty(py);
    for row in &prepared.rows {
        let dict = PyDict::new(py);
        dict.set_item("dataset_id", dataset_id)?;
        dict.set_item("resource_type", prepared.resource_type)?;
        dict.set_item("resource_id", &row.resource_id)?;
        dict.set_item("payload_hash", &row.payload_hash)?;
        dict.set_item("payload_json", provider_directory_dict(py, &row.payload)?)?;
        dict.set_item("acquired_resource_sha256", py.None())?;
        rows.append(dict)?;
        texts.append(row.payload_text.as_deref())?;
        match &row.merged_role {
            Some((entry_hashes, composed)) => {
                merged_roles.append((row.resource_id.as_str(), entry_hashes.clone(), composed.as_str()))?
            }
            None => merged_roles.append(py.None())?,
        }
    }
    let texts = if context.payload_text { texts.into_any() } else { py.None().into_bound(py) };
    pyo3::types::PyTuple::new(
        py,
        [
            "rows".into_pyobject(py)?.into_any(),
            rows.into_any(),
            prepared.rows_fetched.into_pyobject(py)?.into_any(),
            texts,
            merged_roles.into_any(),
        ],
    )
}

/// Prepare each line on its own as one compact JSON group row, with merged roles on.
///
/// Returns `("lines", entries, error)` or `("unsupported", reason)` for the whole call. Each entry is
/// `(resource_id, row_bytes)`, `None` for a skipped line, or the reason string for a line left to Python.
/// `error` is the batch error today's handling returns at the line after the last entry.
#[pyfunction]
fn prepare_provider_directory_row_lines_v1<'py>(
    py: Python<'py>,
    lines: &Bound<'py, PyList>,
    resource_type: &str,
    context: ProviderDirectoryRowsContext,
) -> PyResult<Bound<'py, pyo3::types::PyTuple>> {
    use crate::provider_directory_rows::LineEntry;
    let owned_lines = provider_directory_line_bytes(lines)?;
    let line_bytes: Vec<&[u8]> = owned_lines.iter().map(|line| line.as_bytes()).collect();
    let prepared = match py.detach(|| {
        crate::provider_directory_rows::prepare_lines(&line_bytes, &context.rows_context(resource_type), context.strict)
    }) {
        Ok(prepared) => prepared,
        Err(reason) => return pyo3::types::PyTuple::new(py, ["unsupported", reason.0]),
    };
    let entries = PyList::empty(py);
    for entry in &prepared.entries {
        match entry {
            LineEntry::Row { resource_id, text } => {
                entries.append((resource_id.as_str(), PyBytes::new(py, text.as_bytes())))?
            }
            LineEntry::Skipped => entries.append(py.None())?,
            LineEntry::Unsupported(reason) => entries.append(*reason)?,
        }
    }
    pyo3::types::PyTuple::new(
        py,
        [
            "lines".into_pyobject(py)?.into_any(),
            entries.into_any(),
            prepared.error.into_pyobject(py)?.into_any(),
        ],
    )
}

#[pyfunction]
fn provider_directory_rows_version() -> &'static str {
    crate::provider_directory_rows::CONTRACT
}

#[pyfunction]
fn verify_provider_directory_merged_role_page_v1(
    py: Python<'_>,
    rows: Vec<(String, String, String)>,
) -> Option<(Py<PyBytes>, Vec<usize>)> {
    py.detach(|| crate::provider_directory_rows::verify_merged_role_page(&rows))
        .map(|(identities, fallback)| (PyBytes::new(py, identities.as_bytes()).unbind(), fallback))
}
