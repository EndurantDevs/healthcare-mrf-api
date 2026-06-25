//! Library modules for the PTG2 scanner binaries.

pub mod address_canon;
pub mod config;
pub mod copy_format;
pub mod dedupe;
pub mod hashing;
pub mod input;
pub mod manifest;
pub mod normalize;
pub mod output;
pub mod progress;

#[cfg(feature = "python")]
mod python_api {
    use crate::address_canon::{canon_version_json, canonicalize_address, CanonicalAddress};
    use pyo3::exceptions::PyRuntimeError;
    use pyo3::prelude::*;
    use pyo3::types::{PyDict, PyList};
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
        m.add_function(wrap_pyfunction!(canon_version, m)?)?;
        Ok(())
    }
}
