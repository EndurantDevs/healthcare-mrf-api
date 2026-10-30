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
    Ok((
        retained_copy_buffers(py, batch.binding_batches),
        retained_copy_buffers(py, batch.evidence_batches),
        retained_copy_buffers(py, batch.plan_batches),
        PyBytes::new(py, &batch.root_bytes).unbind(),
        families,
        batch.row_count,
        batch.fallback_rows,
        batch.owned_output_bytes,
    ))
}
