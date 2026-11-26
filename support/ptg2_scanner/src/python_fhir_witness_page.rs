// Licensed under the HealthPorta Non-Commercial License (see LICENSE).

use pyo3::intern;
use pyo3::pybacked::PyBackedStr as WitnessBackedStr;
use pyo3::types::{PyAnyMethods, PyDictMethods, PySequence};
use std::collections::{BTreeMap, BTreeSet};

type ExtractedWitnessRow = (
    WitnessBackedStr,
    WitnessBackedStr,
    WitnessBackedStr,
    WitnessBackedStr,
    WitnessBackedStr,
    Option<WitnessBackedStr>,
);

enum ExtractWitnessRows {
    Extracted(Vec<ExtractedWitnessRow>),
    FallbackRequired,
}

#[inline]
fn extract_exact_str_item(
    item: Bound<'_, PyAny>,
    err_code: &'static str,
) -> PyResult<WitnessBackedStr> {
    if !item.is_exact_instance_of::<PyString>() {
        return Err(PyValueError::new_err(err_code));
    }
    item.extract::<WitnessBackedStr>()
}

fn extract_witness_rows(py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<ExtractWitnessRows> {
    let sequence = rows
        .cast::<PySequence>()
        .map_err(|_| PyValueError::new_err("page_bounds"))?;
    let count = sequence.len()?;
    if count == 0 || count > crate::fhir_witness_page::MAX_WITNESS_ROWS {
        return Err(PyValueError::new_err("page_bounds"));
    }
    let key_dataset = intern!(py, "dataset_id");
    let key_type = intern!(py, "resource_type");
    let key_id = intern!(py, "resource_id");
    let key_hash = intern!(py, "payload_hash");
    let key_payload = intern!(py, "payload_json");
    let key_acquired = intern!(py, "acquired_resource_sha256");
    let mut extracted = Vec::with_capacity(count);
    for index in 0..count {
        let item = sequence.get_item(index)?;
        if !item.is_exact_instance_of::<PyDict>() {
            return Err(PyValueError::new_err("resource_shape"));
        }
        let dict = item.cast::<PyDict>()?;
        if dict.len() != 6 {
            return Err(PyValueError::new_err("resource_shape"));
        }
        let Some(dataset_item) = dict.get_item(key_dataset)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        let Some(type_item) = dict.get_item(key_type)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        let Some(id_item) = dict.get_item(key_id)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        let Some(hash_item) = dict.get_item(key_hash)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        let Some(payload_item) = dict.get_item(key_payload)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        let Some(acquired_item) = dict.get_item(key_acquired)? else {
            return Err(PyValueError::new_err("resource_shape"));
        };
        if !payload_item.is_exact_instance_of::<PyString>() {
            return Ok(ExtractWitnessRows::FallbackRequired);
        }
        let dataset_id = extract_exact_str_item(dataset_item, "resource_scope")?;
        let acquired = if acquired_item.is_none() {
            None
        } else if acquired_item.is_exact_instance_of::<PyString>() {
            Some(acquired_item.extract::<WitnessBackedStr>()?)
        } else {
            return Err(PyValueError::new_err("resource_scope"));
        };
        let resource_type = extract_exact_str_item(type_item, "resource_identity")?;
        let resource_id = extract_exact_str_item(id_item, "resource_identity")?;
        let payload_hash = extract_exact_str_item(hash_item, "resource_hash")?;
        let payload_json = payload_item.extract::<WitnessBackedStr>()?;
        extracted.push((
            dataset_id,
            resource_type,
            resource_id,
            payload_hash,
            payload_json,
            acquired,
        ));
    }
    Ok(ExtractWitnessRows::Extracted(extracted))
}

type ValidatedWitnessResult = (String, String, BTreeMap<String, usize>);
type PreparedTypedPageResult = (String, usize, String, String, BTreeMap<String, usize>);

#[pyfunction]
fn validate_fhir_witness_page(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    dataset_id: &str,
    allowed_kinds: Option<Vec<String>>,
    after: Option<(&str, &str)>,
    flags: (bool, bool, bool, bool),
    max_bytes: usize,
) -> PyResult<Option<ValidatedWitnessResult>> {
    let extracted = match extract_witness_rows(py, rows)? {
        ExtractWitnessRows::Extracted(items) => items,
        ExtractWitnessRows::FallbackRequired => return Ok(None),
    };
    let allowed_set = allowed_kinds.map(|items| items.into_iter().collect::<BTreeSet<_>>());
    let (check_marker, completion_v3, validate_ids, validate_hashes) = flags;
    let summary = py
        .detach(|| {
            let borrowed: Vec<crate::fhir_witness_page::WitnessPageRow<'_>> = extracted
                .iter()
                .map(|row| crate::fhir_witness_page::WitnessPageRow {
                    dataset_id: &row.0,
                    resource_type: &row.1,
                    resource_id: &row.2,
                    payload_hash: &row.3,
                    payload_json: &row.4,
                    acquired_resource_sha256: row.5.as_deref(),
                })
                .collect();
            let options = crate::fhir_witness_page::WitnessValidationOptions {
                dataset_id,
                allowed_kinds: allowed_set.as_ref(),
                after,
                check_marker,
                completion_v3,
                validate_ids,
                validate_hashes,
                max_bytes,
            };
            crate::fhir_witness_page::validate_witness_page(&borrowed, &options)
        })
        .map_err(PyValueError::new_err)?;
    Ok(Some((summary.last_type, summary.last_id, summary.counts)))
}

#[pyfunction]
fn prepare_typed_fhir_page(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    scope: (&str, &str),
    kinds: (Vec<String>, Vec<String>),
    after: Option<(&str, &str)>,
    completion_v3: bool,
    max_bytes: usize,
) -> PyResult<Option<PreparedTypedPageResult>> {
    let extracted = match extract_witness_rows(py, rows)? {
        ExtractWitnessRows::Extracted(items) => items,
        ExtractWitnessRows::FallbackRequired => return Ok(None),
    };
    let (dataset_id, source_id) = scope;
    let (allowed_kinds, project_kinds) = kinds;
    let allowed_set: BTreeSet<String> = allowed_kinds.into_iter().collect();
    let project_set: BTreeSet<String> = project_kinds.into_iter().collect();
    let (json_text, projected_count, summary) = py
        .detach(|| {
            let borrowed: Vec<crate::fhir_witness_page::WitnessPageRow<'_>> = extracted
                .iter()
                .map(|row| crate::fhir_witness_page::WitnessPageRow {
                    dataset_id: &row.0,
                    resource_type: &row.1,
                    resource_id: &row.2,
                    payload_hash: &row.3,
                    payload_json: &row.4,
                    acquired_resource_sha256: row.5.as_deref(),
                })
                .collect();
            let options = crate::fhir_witness_page::WitnessValidationOptions {
                dataset_id,
                allowed_kinds: Some(&allowed_set),
                after,
                check_marker: true,
                completion_v3,
                validate_ids: true,
                validate_hashes: true,
                max_bytes,
            };
            crate::fhir_witness_page::prepare_typed_page_json(
                &borrowed,
                &options,
                source_id,
                &project_set,
            )
        })
        .map_err(PyValueError::new_err)?;
    Ok(Some((
        json_text,
        projected_count,
        summary.last_type,
        summary.last_id,
        summary.counts,
    )))
}

type FoldedRootsOutput = (Py<PyBytes>, BTreeMap<String, (Py<PyBytes>, usize)>);

#[pyfunction]
fn fold_stored_resource_roots_page(
    py: Python<'_>,
    rows: &Bound<'_, PyAny>,
    allowed_kinds: Vec<String>,
) -> PyResult<FoldedRootsOutput> {
    let sequence = rows
        .cast::<PySequence>()
        .map_err(|_| PyValueError::new_err("membership_page_bounds"))?;
    let count = sequence.len()?;
    if count > crate::fhir_witness_page::MAX_WITNESS_ROWS {
        return Err(PyValueError::new_err("membership_page_bounds"));
    }
    let key_type = intern!(py, "resource_type");
    let key_id = intern!(py, "resource_id");
    let key_hash = intern!(py, "payload_hash");
    let mut extracted: Vec<(WitnessBackedStr, WitnessBackedStr, WitnessBackedStr)> =
        Vec::with_capacity(count);
    for index in 0..count {
        let item = sequence.get_item(index)?;
        let kind_obj = item
            .get_item(key_type)
            .map_err(|_| PyValueError::new_err("membership_row_changed"))?;
        let id_obj = item
            .get_item(key_id)
            .map_err(|_| PyValueError::new_err("membership_row_changed"))?;
        let hash_obj = item
            .get_item(key_hash)
            .map_err(|_| PyValueError::new_err("membership_row_changed"))?;
        if !kind_obj.is_exact_instance_of::<PyString>()
            || !id_obj.is_exact_instance_of::<PyString>()
            || !hash_obj.is_exact_instance_of::<PyString>()
        {
            return Err(PyValueError::new_err("membership_row_changed"));
        }
        extracted.push((
            kind_obj.extract()?,
            id_obj.extract()?,
            hash_obj.extract()?,
        ));
    }
    let allowed_set: BTreeSet<String> = allowed_kinds.into_iter().collect();
    let (root_bytes, families) = py
        .detach(|| {
            let borrowed: Vec<(&str, &str, &str)> = extracted
                .iter()
                .map(|(kind, id, hash)| (&**kind, &**id, &**hash))
                .collect();
            crate::fhir_witness_page::fold_stored_resource_roots_page(&borrowed, &allowed_set)
        })
        .map_err(PyValueError::new_err)?;
    let py_families = families
        .into_iter()
        .map(|(kind, (bytes, row_count))| (kind, (PyBytes::new(py, &bytes).unbind(), row_count)))
        .collect();
    Ok((PyBytes::new(py, &root_bytes).unbind(), py_families))
}

/// Measure exact `len(json.dumps(rows, ensure_ascii=False, separators=(",", ":")).encode()) <= max_bytes`
/// for flat scalar dicts without allocating a serialized JSON string.
/// Returns `None` when a nested or non-scalar value requires Python's JSON encoder.
#[pyfunction]
fn compact_json_utf8_len_fits(rows: &Bound<'_, PyAny>, max_bytes: usize) -> PyResult<Option<bool>> {
    let Ok(sequence) = rows.cast::<PySequence>() else {
        return Ok(None);
    };
    let count = sequence.len()?;
    let mut total: usize = 2 + count.saturating_sub(1);
    if total > max_bytes {
        return Ok(Some(false));
    }
    let mut itoa_buf = itoa::Buffer::new();
    for index in 0..count {
        let item = sequence.get_item(index)?;
        if !item.is_exact_instance_of::<PyDict>() {
            return Ok(None);
        }
        let dict = item.cast::<PyDict>()?;
        let entries = dict.len();
        total = total.saturating_add(2 + entries.saturating_sub(1) + entries);
        if total > max_bytes {
            return Ok(Some(false));
        }
        for (key, value) in dict.iter() {
            if !key.is_exact_instance_of::<PyString>() {
                return Ok(None);
            }
            let key_str = key.extract::<WitnessBackedStr>()?;
            total = total.saturating_add(crate::fhir_witness_page::json_str_utf8_len(&key_str));
            let value_len = if value.is_none() {
                4
            } else if value.is_exact_instance_of::<PyString>() {
                let text = value.extract::<WitnessBackedStr>()?;
                crate::fhir_witness_page::json_str_utf8_len(&text)
            } else if value.is_exact_instance_of::<PyBool>() {
                if value.extract::<bool>()? { 4 } else { 5 }
            } else if value.is_exact_instance_of::<PyInt>() {
                match value.extract::<i64>() {
                    Ok(number) => itoa_buf.format(number).len(),
                    Err(_) => return Ok(None),
                }
            } else {
                return Ok(None);
            };
            total = total.saturating_add(value_len);
            if total > max_bytes {
                return Ok(Some(false));
            }
        }
    }
    Ok(Some(true))
}
