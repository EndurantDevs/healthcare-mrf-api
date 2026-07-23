//! Library modules for the PTG2 scanner binaries.

pub mod address_canon;
pub mod config;
pub mod contact_canon;
pub mod copy_format;
pub mod dedupe;
pub mod hashing;
pub mod input;
pub mod manifest;
pub mod normalize;
pub mod output;
pub mod progress;
pub mod provider_directory_projection;
pub mod provider_graph_v4;
pub mod rate_schedule_observe;
pub mod shared_graph;
pub mod uhc_retained;
pub mod v3_dense;
pub mod v3_runs;

/// Intersect two strictly increasing u32 vectors in one linear pass.
pub fn intersect_sorted_unique_u32(left: &[u32], right: &[u32]) -> Result<Vec<u32>, &'static str> {
    if left.windows(2).any(|window| window[0] >= window[1])
        || right.windows(2).any(|window| window[0] >= window[1])
    {
        return Err("PTG V4 intersection inputs must be strictly increasing");
    }
    let mut intersection = Vec::with_capacity(left.len().min(right.len()));
    let (mut left_index, mut right_index) = (0usize, 0usize);
    while left_index < left.len() && right_index < right.len() {
        match left[left_index].cmp(&right[right_index]) {
            std::cmp::Ordering::Less => left_index += 1,
            std::cmp::Ordering::Greater => right_index += 1,
            std::cmp::Ordering::Equal => {
                intersection.push(left[left_index]);
                left_index += 1;
                right_index += 1;
            }
        }
    }
    Ok(intersection)
}

/// Decode a packed little-endian u32 member page with strict framing.
pub fn decode_u32_le(bytes: &[u8]) -> Result<Vec<u32>, &'static str> {
    if !bytes.len().is_multiple_of(std::mem::size_of::<u32>()) {
        return Err("PTG V4 packed u32 page length must be divisible by four");
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|chunk| u32::from_le_bytes(chunk.try_into().expect("exact u32 chunk")))
        .collect())
}

#[cfg(feature = "python")]
mod python_api {
    use crate::address_canon::{canon_version_json, canonicalize_address, CanonicalAddress};
    use crate::contact_canon::canonicalize_contact_pair;
    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList};
    use rayon::prelude::*;
    use serde_json::Value;

    type AddressRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    type ContactRow = (Option<String>, Option<String>, Option<String>);

    #[pyfunction]
    fn canonicalize_batch(py: Python<'_>, rows: Vec<AddressRow>) -> PyResult<Py<PyList>> {
        let results: Vec<CanonicalAddress> = py.detach(|| {
            rows.par_iter()
                .map(|row| {
                    canonicalize_address(
                        row.0.as_deref(),
                        row.1.as_deref(),
                        row.2.as_deref(),
                        row.3.as_deref(),
                        row.4.as_deref(),
                        row.5.as_deref(),
                    )
                })
                .collect()
        });
        let list = PyList::empty(py);
        for item in results {
            list.append(canonical_to_dict(py, &item)?)?;
        }
        Ok(list.into())
    }

    #[pyfunction]
    fn canonicalize_contact_batch(py: Python<'_>, rows: Vec<ContactRow>) -> PyResult<Py<PyList>> {
        let results = py.detach(|| {
            rows.par_iter()
                .map(|row| {
                    canonicalize_contact_pair(row.0.as_deref(), row.1.as_deref(), row.2.as_deref())
                })
                .collect::<Vec<_>>()
        });
        let list = PyList::empty(py);
        for item in results {
            let dict = PyDict::new(py);
            dict.set_item("phone_number", item.phone.number.as_deref())?;
            dict.set_item("phone_extension", item.phone.extension.as_deref())?;
            dict.set_item("phone_is_international", item.phone.is_international)?;
            dict.set_item("phone_valid_for_fallback", item.phone.valid_for_fallback)?;
            dict.set_item("fax_number", item.fax.number.as_deref())?;
            dict.set_item("fax_number_digits", item.fax.number.as_deref())?;
            dict.set_item("fax_extension", item.fax.extension.as_deref())?;
            dict.set_item("fax_is_international", item.fax.is_international)?;
            dict.set_item("fax_valid_for_fallback", item.fax.valid_for_fallback)?;
            list.append(dict)?;
        }
        Ok(list.into())
    }

    #[pyfunction]
    fn canon_version(py: Python<'_>) -> PyResult<Py<PyDict>> {
        let payload: Value = serde_json::from_str(&canon_version_json())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let dict = PyDict::new(py);
        if let Some(version) = payload.get("identity_version").and_then(Value::as_u64) {
            dict.set_item("identity_version", version)?;
        }
        if let Some(prefix) = payload.get("identity_prefix").and_then(Value::as_str) {
            dict.set_item("identity_prefix", prefix)?;
        }
        if let Some(version) = payload.get("ruleset_version").and_then(Value::as_u64) {
            dict.set_item("ruleset_version", version)?;
        }
        if let Some(hash) = payload.get("pub28_sha256").and_then(Value::as_str) {
            dict.set_item("pub28_sha256", hash)?;
        }
        Ok(dict.into())
    }

    #[pyfunction(name = "intersect_sorted_u32")]
    fn intersect_sorted_u32_py(left: Vec<u32>, right: Vec<u32>) -> PyResult<Vec<u32>> {
        super::intersect_sorted_unique_u32(&left, &right).map_err(PyValueError::new_err)
    }

    #[pyfunction(name = "ptg2_decode_u32_le")]
    fn decode_u32_le_py(payload: &Bound<'_, PyBytes>) -> PyResult<Vec<u32>> {
        super::decode_u32_le(payload.as_bytes()).map_err(PyValueError::new_err)
    }

    fn canonical_to_dict<'py>(
        py: Python<'py>,
        item: &CanonicalAddress,
    ) -> PyResult<Bound<'py, PyDict>> {
        let dict = PyDict::new(py);
        dict.set_item("address_key", item.address_key.as_deref())?;
        dict.set_item("identity_key", item.identity_key.as_deref())?;
        dict.set_item("premise_key", item.premise_key.as_deref())?;
        dict.set_item("premise_identity_key", item.premise_identity_key.as_deref())?;
        dict.set_item("line1_norm", item.line1_norm.as_deref())?;
        dict.set_item("unit_norm", &item.unit_norm)?;
        dict.set_item("city_norm", item.city_norm.as_deref())?;
        dict.set_item("state_code", item.state_code.as_deref())?;
        dict.set_item("zip5", item.zip5.as_deref())?;
        dict.set_item("zip4", item.zip4.as_deref())?;
        dict.set_item("country_code", &item.country_code)?;
        Ok(dict)
    }

    #[pymodule]
    fn ptg2_address_canon(m: &Bound<'_, PyModule>) -> PyResult<()> {
        m.add_function(wrap_pyfunction!(canonicalize_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canonicalize_contact_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canon_version, m)?)?;
        m.add_function(wrap_pyfunction!(intersect_sorted_u32_py, m)?)?;
        m.add_function(wrap_pyfunction!(decode_u32_le_py, m)?)?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn python_module_exports_real_address_contact_and_version_payloads() {
            Python::initialize();
            Python::attach(|py| {
                let addresses = canonicalize_batch(
                    py,
                    vec![(
                        Some("123 Main Street".to_owned()),
                        Some("Suite 2".to_owned()),
                        Some("Austin".to_owned()),
                        Some("TX".to_owned()),
                        Some("78701".to_owned()),
                        Some("US".to_owned()),
                    )],
                )
                .unwrap();
                assert_eq!(addresses.bind(py).len(), 1);

                let contacts = canonicalize_contact_batch(
                    py,
                    vec![(
                        Some("+1 (202) 555-0199 ext 4".to_owned()),
                        Some("202-555-0100".to_owned()),
                        Some("US".to_owned()),
                    )],
                )
                .unwrap();
                assert_eq!(contacts.bind(py).len(), 1);

                let version = canon_version(py).unwrap();
                assert!(version.bind(py).contains("ruleset_version").unwrap());
                assert!(version.bind(py).contains("pub28_sha256").unwrap());

                let module = PyModule::new(py, "ptg2_address_canon").unwrap();
                ptg2_address_canon(&module).unwrap();
                assert!(module.hasattr("canonicalize_batch").unwrap());
                assert!(module.hasattr("canonicalize_contact_batch").unwrap());
                assert!(module.hasattr("canon_version").unwrap());
                assert!(module.hasattr("intersect_sorted_u32").unwrap());
                assert!(module.hasattr("ptg2_decode_u32_le").unwrap());

                assert_eq!(
                    intersect_sorted_u32_py(vec![1, 3, 5], vec![2, 3, 5]).unwrap(),
                    vec![3, 5],
                );
                assert!(intersect_sorted_u32_py(vec![1, 1], vec![1]).is_err());

                let packed = PyBytes::new(py, &[1, 0, 0, 0, 255, 0, 0, 0]);
                assert_eq!(decode_u32_le_py(&packed).unwrap(), vec![1, 255]);
                let malformed = PyBytes::new(py, &[1, 2, 3]);
                assert!(decode_u32_le_py(&malformed).is_err());
            });
        }
    }
}

#[cfg(test)]
mod v4_intersection_tests {
    use super::{decode_u32_le, intersect_sorted_unique_u32};

    #[test]
    fn intersects_sorted_unique_values_exactly() {
        assert_eq!(
            intersect_sorted_unique_u32(&[1, 3, 7, 9], &[2, 3, 4, 9]).unwrap(),
            vec![3, 9]
        );
        assert!(intersect_sorted_unique_u32(&[1, 1], &[1]).is_err());
        assert!(intersect_sorted_unique_u32(&[2, 1], &[1]).is_err());
    }

    #[test]
    fn decodes_packed_u32_pages_with_strict_framing() {
        assert_eq!(
            decode_u32_le(&[1, 0, 0, 0, 255, 0, 0, 0]).unwrap(),
            vec![1, 255]
        );
        assert!(decode_u32_le(&[1, 2, 3]).is_err());
    }
}
