// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

#[pyfunction]
fn parse_naic_company_registry_member(
    py: Python<'_>,
    input: &[u8],
    edition_json: &[u8],
) -> PyResult<Py<PyBytes>> {
    if edition_json.len() > 65_536 {
        return Err(PyValueError::new_err(
            "Company listing edition metadata exceeds byte limit",
        ));
    }
    let encoded = py.detach(|| {
        let edition: crate::naic_company_registry::NaicEdition =
            serde_json::from_slice(edition_json).map_err(|_| {
                PyValueError::new_err("Company listing edition metadata is invalid")
            })?;
        let batch =
            crate::naic_company_registry::parse_naic_company_registry_member(input, &edition)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or(error.code.to_string()),
                    )
                })?;
        let encoded = match serde_json::to_vec(&batch) {
            Ok(encoded) => encoded,
            Err(_) => return Err(PyValueError::new_err("Company listing encoding failed")),
        };
        if encoded.len() > 16 * 1024 * 1024 {
            return Err(PyValueError::new_err(
                "Company listing output exceeds byte limit",
            ));
        }
        Ok::<_, PyErr>(encoded)
    })?;
    Ok(PyBytes::new(py, &encoded).unbind())
}

#[pyfunction]
fn encode_naic_company_registry_observations(
    py: Python<'_>,
    input: &[u8],
    edition_json: &[u8],
) -> PyResult<(Py<PyBytes>, usize, Py<PyBytes>)> {
    if edition_json.len() > 65_536 {
        return Err(PyValueError::new_err(
            "Company listing edition metadata exceeds byte limit",
        ));
    }
    let (copy_bytes, row_count, metadata) = py.detach(|| {
        let edition: crate::naic_company_registry::NaicEdition =
            serde_json::from_slice(edition_json).map_err(|_| {
                PyValueError::new_err("Company listing edition metadata is invalid")
            })?;
        let encoded = crate::naic_company_registry::encode_naic_company_registry_observations(
            input, &edition,
        )
        .map_err(|error| {
            PyValueError::new_err(
                serde_json::to_string(&error).unwrap_or(error.code.to_string()),
            )
        })?;
        let metadata = match serde_json::to_vec(&serde_json::json!({
            "edition": &encoded.batch.edition,
            "counts": &encoded.batch.counts,
            "conflicts": &encoded.batch.conflicts,
        })) {
            Ok(metadata) => metadata,
            Err(_) => return Err(PyValueError::new_err("Company listing encoding failed")),
        };
        if metadata.len() > 16 * 1024 * 1024 {
            return Err(PyValueError::new_err(
                "Company listing output exceeds byte limit",
            ));
        }
        Ok::<_, PyErr>((encoded.copy_bytes, encoded.row_count, metadata))
    })?;
    Ok((
        PyBytes::new(py, &copy_bytes).unbind(),
        row_count,
        PyBytes::new(py, &metadata).unbind(),
    ))
}

#[pyfunction]
fn classify_registry_assertions(
    py: Python<'_>,
    input: &[u8],
    policy: &str,
) -> PyResult<Py<PyBytes>> {
    use crate::registry_assertion_conflicts::ReviewPolicy;
    let policy = match policy {
        "group_inclusive" => ReviewPolicy::GroupInclusive,
        "network_half_open" => ReviewPolicy::NetworkHalfOpen,
        _ => return Err(PyValueError::new_err("registry_assertion_policy_invalid")),
    };
    let encoded = py.detach(|| {
        let batch =
            crate::registry_assertion_conflicts::classify_registry_assertions(input, policy)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or(error.code.to_string()),
                    )
                })?;
        match serde_json::to_vec(&batch) {
            Ok(encoded) => Ok(encoded),
            Err(_) => Err(PyValueError::new_err("Assertion review encoding failed")),
        }
    })?;
    Ok(PyBytes::new(py, &encoded).unbind())
}

#[pyfunction]
fn validate_network_catalog_batch(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
    let encoded = py.detach(|| {
        let batch = crate::canonical_network_values::validate_network_catalog_batch(input)
            .map_err(|error| {
                PyValueError::new_err(
                    serde_json::to_string(&error).unwrap_or(error.code.to_string()),
                )
            })?;
        let encoded = match serde_json::to_vec(&batch) {
            Ok(encoded) => encoded,
            Err(_) => return Err(PyValueError::new_err("Network catalog encoding failed")),
        };
        if encoded.len() > 16 * 1024 * 1024 {
            return Err(PyValueError::new_err(
                "Network catalog output exceeds byte limit",
            ));
        }
        Ok::<_, PyErr>(encoded)
    })?;
    Ok(PyBytes::new(py, &encoded).unbind())
}
