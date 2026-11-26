// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

type RetainedCopyBuffers = Vec<(Py<PyBytes>, usize)>;
type RetainedCaptureOutput = (
    RetainedCopyBuffers,
    RetainedCopyBuffers,
    RetainedCopyBuffers,
    Py<PyBytes>,
    std::collections::BTreeMap<String, (Py<PyBytes>, usize)>,
    usize,
    Vec<usize>,
    usize,
);

fn retained_copy_buffers(
    py: Python<'_>,
    batches: Vec<crate::retained_fhir_capture::CopyBatch>,
) -> RetainedCopyBuffers {
    batches
        .into_iter()
        .map(|batch| (PyBytes::new(py, &batch.bytes).unbind(), batch.row_count))
        .collect()
}

fn finish_retained_output(
    py: Python<'_>,
    batch: crate::retained_fhir_capture::RetainedFhirCaptureBatch,
) -> RetainedCaptureOutput {
    let families = batch
        .families
        .into_iter()
        .map(|(kind, family)| {
            (
                kind,
                (PyBytes::new(py, &family.bytes).unbind(), family.row_count),
            )
        })
        .collect();
    (
        retained_copy_buffers(py, batch.binding_batches),
        retained_copy_buffers(py, batch.evidence_batches),
        retained_copy_buffers(py, batch.plan_batches),
        PyBytes::new(py, &batch.root_bytes).unbind(),
        families,
        batch.row_count,
        batch.fallback_rows,
        batch.owned_output_bytes,
    )
}

#[pyfunction]
fn encode_retained_fhir_capture_batch(
    py: Python<'_>,
    input: &[u8],
) -> PyResult<RetainedCaptureOutput> {
    let batch = py
        .detach(|| crate::retained_fhir_capture::encode_retained_fhir_capture_batch(input))
        .map_err(|error| {
            PyValueError::new_err(
                serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_owned()),
            )
        })?;
    Ok(finish_retained_output(py, batch))
}

type RetainedNativeInputRow = (
    pyo3::pybacked::PyBackedStr,
    pyo3::pybacked::PyBackedStr,
    pyo3::pybacked::PyBackedStr,
    pyo3::pybacked::PyBackedStr,
    Option<pyo3::pybacked::PyBackedStr>,
    Option<pyo3::pybacked::PyBackedStr>,
);

type RetainedNativeContext<'a> = (&'a str, &'a str, i64, &'a str, Option<u8>, Vec<String>, bool);

#[pyfunction]
fn encode_retained_fhir_capture_rows(
    py: Python<'_>,
    context: RetainedNativeContext<'_>,
    rows: Vec<RetainedNativeInputRow>,
) -> PyResult<RetainedCaptureOutput> {
    let (
        source_id,
        release_id,
        observed_at_pg_micros,
        hash_contract,
        completion_required_version,
        resource_types,
        derive,
    ) = context;
    let pool = location_canon_pool()?;
    let batch = py
        .detach(|| {
            pool.install(|| {
                let context = crate::retained_fhir_capture::NativeCaptureContext {
                    source_id,
                    release_id,
                    observed_at_pg_micros,
                    hash_contract,
                    completion_required_version,
                    resource_types: &resource_types,
                    derive,
                };
                let borrowed_rows: Vec<crate::retained_fhir_capture::NativeCaptureRow<'_>> = rows
                    .iter()
                    .map(|row| crate::retained_fhir_capture::NativeCaptureRow {
                        resource_type: &row.0,
                        resource_id: &row.1,
                        payload_hash: &row.2,
                        payload_json: &row.3,
                        acquired_resource_sha256: row.4.as_deref(),
                        site_id: row.5.as_deref(),
                    })
                    .collect();
                crate::retained_fhir_capture::encode_retained_fhir_capture_rows(
                    &context,
                    &borrowed_rows,
                )
            })
        })
        .map_err(|error| {
            PyValueError::new_err(
                serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_owned()),
            )
        })?;
    Ok(finish_retained_output(py, batch))
}
