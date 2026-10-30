#[pyfunction]
fn prepare_entity_address_rows(py: Python<'_>, input: &[u8]) -> PyResult<(Py<PyBytes>, usize)> {
    let prepared = py
        .detach(|| crate::entity_address_rows::prepare_entity_address_rows(input))
        .map_err(PyValueError::new_err)?;
    Ok((
        PyBytes::new(py, &prepared.copy_bytes).unbind(),
        prepared.row_count,
    ))
}

#[pyfunction]
fn entity_address_rows_version() -> &'static str {
    crate::entity_address_rows::CONTRACT
}
