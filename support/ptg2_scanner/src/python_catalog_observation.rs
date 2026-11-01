// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use pyo3::pybacked::PyBackedStr;

type CatalogObservationRow = (PyBackedStr, PyBackedStr, PyBackedStr, PyBackedStr);

#[pyfunction]
fn encode_catalog_observation_page(
    py: Python<'_>,
    rows: &Bound<'_, pyo3::types::PyTuple>,
    page_bytes: usize,
    max_references: usize,
    int_max_digits: usize,
) -> PyResult<Option<Vec<Py<PyBytes>>>> {
    if rows.len() > 1_000 {
        return Ok(None);
    }
    let rows: Vec<CatalogObservationRow> = rows.extract()?;
    let input_bytes: usize = rows
        .iter()
        .map(|(kind, id, hash, raw)| kind.len() + id.len() + hash.len() + raw.len())
        .sum();
    if input_bytes > 32 * 1024 * 1024 {
        return Ok(None);
    }
    let result = py.detach(|| {
        let rows: Vec<_> = rows
            .iter()
            .map(|(kind, id, hash, raw)| (&**kind, &**id, &**hash, &**raw))
            .collect();
        crate::catalog_observation::encode_page(&rows, page_bytes, max_references, int_max_digits)
    });
    Ok(result.map(|rows| {
        rows.into_iter()
            .map(|row| PyBytes::new(py, &row).unbind())
            .collect()
    }))
}
