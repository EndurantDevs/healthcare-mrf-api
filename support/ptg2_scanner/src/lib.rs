//! Library modules for the PTG2 scanner binaries.

pub mod address_canon;
pub mod address_evidence_alias;
pub mod canonical_network_values;
pub mod cms_mlr_registry;
pub mod cms_planfinder_registry;
pub mod company_network_assertions;
pub mod config;
pub mod contact_canon;
pub mod copy_format;
pub mod custom_import_scalar;
pub mod custom_import_source;
pub mod dedupe;
pub mod fhir_network_identity;
pub mod hashing;
pub mod hospital_mrf;
pub mod hospital_price_block;
pub mod hospital_price_selector_block;
pub mod hospital_price_service_block;
pub mod input;
pub mod manifest;
pub mod network_membership_codec;
pub mod network_source_binding_values;
pub mod normalize;
mod npi_identifier;
pub mod output;
pub mod progress;
pub mod provider_directory_projection;
pub mod provider_graph_v4;
pub mod rate_schedule_observe;
pub mod registry_network_coverage;
pub mod registry_network_evidence_values;
pub mod registry_ptg_capture_input;
pub mod registry_ptg_graph_python;
pub mod registry_ptg_graph_witness;
pub mod registry_required_target_review;
pub mod registry_target_ledger;
pub mod shared_graph;
pub mod tax_identity;
pub mod tax_identity_sidecar_bundle;
pub mod tax_identity_sidecar_pair;
pub mod tax_identity_sidecar_v1;
pub mod tax_identity_sidecar_v2;
pub mod uhc_retained;
pub mod uhc_semantic;
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
        .as_chunks::<4>()
        .0
        .iter()
        .map(|chunk| u32::from_le_bytes(*chunk))
        .collect())
}

#[cfg(feature = "python")]
mod python_api {
    use crate::address_canon::{canon_version_json, canonicalize_address, CanonicalAddress};
    use crate::contact_canon::canonicalize_contact_pair;
    use pyo3::exceptions::{PyRuntimeError, PyValueError};
    use pyo3::prelude::*;
    use pyo3::types::{PyBytes, PyDict, PyList};
    use pyo3::BoundObject;
    use rayon::{prelude::*, ThreadPool, ThreadPoolBuilder};
    use serde_json::Value;
    use std::{convert::Infallible, sync::OnceLock};

    type AddressRow = (
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
        Option<String>,
    );
    type ContactRow = (Option<String>, Option<String>, Option<String>);
    type LocationCanonicalRow = (Option<String>, Option<String>, Option<String>);
    static LOCATION_CANON_POOL: OnceLock<ThreadPool> = OnceLock::new();

    include!("python_hospital_price.rs");
    include!("python_custom_import.rs");

    #[pyfunction]
    fn encode_network_membership_batch(
        py: Python<'_>,
        input: &[u8],
    ) -> PyResult<(Py<PyBytes>, usize)> {
        let batch = py
            .detach(|| crate::network_membership_codec::encode_network_membership_batch(input))
            .map_err(|error| {
                PyValueError::new_err(
                    serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                )
            })?;
        Ok((
            PyBytes::new(py, &batch.copy_bytes).unbind(),
            batch.row_count,
        ))
    }

    #[pyfunction]
    fn encode_cms_mlr_observations(
        py: Python<'_>,
        input: &[u8],
        edition_json: &[u8],
    ) -> PyResult<(Py<PyBytes>, usize, Py<PyBytes>)> {
        if edition_json.len() > 65_536 {
            return Err(PyValueError::new_err(
                "MLR edition metadata exceeds byte limit",
            ));
        }
        let (encoded, metadata) = py.detach(|| {
            let edition: crate::cms_mlr_registry::MlrEdition = serde_json::from_slice(edition_json)
                .map_err(|_| PyValueError::new_err("MLR edition metadata is invalid"))?;
            let encoded = crate::cms_mlr_registry::encode_cms_mlr_observations(input, &edition)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                    )
                })?;
            let metadata = serde_json::to_vec(&serde_json::json!({
                "edition": &encoded.batch.edition,
                "counts": &encoded.batch.counts,
                "conflicts": &encoded.batch.conflicts,
            }))
            .map_err(|_| PyValueError::new_err("MLR metadata encoding failed"))?;
            if metadata.len() > 16 * 1024 * 1024 {
                return Err(PyValueError::new_err("MLR metadata exceeds byte limit"));
            }
            Ok::<_, PyErr>((encoded, metadata))
        })?;
        Ok((
            PyBytes::new(py, &encoded.copy_bytes).unbind(),
            encoded.row_count,
            PyBytes::new(py, &metadata).unbind(),
        ))
    }

    #[pyfunction]
    fn validate_network_catalog_batch(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let batch = crate::canonical_network_values::validate_network_catalog_batch(input)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                    )
                })?;
            let encoded = serde_json::to_vec(&batch)
                .map_err(|_| PyValueError::new_err("Network catalog encoding failed"))?;
            if encoded.len() > 16 * 1024 * 1024 {
                return Err(PyValueError::new_err(
                    "Network catalog output exceeds byte limit",
                ));
            }
            Ok::<_, PyErr>(encoded)
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    fn location_canon_pool() -> PyResult<&'static ThreadPool> {
        if let Some(pool) = LOCATION_CANON_POOL.get() {
            return Ok(pool);
        }
        let pool = match ThreadPoolBuilder::new().num_threads(4).build() {
            Ok(pool) => pool,
            Err(error) => return Err(PyRuntimeError::new_err(error.to_string())),
        };
        let _ = LOCATION_CANON_POOL.set(pool);
        match LOCATION_CANON_POOL.get() {
            Some(pool) => Ok(pool),
            None => Err(PyRuntimeError::new_err(
                "Location canonicalizer pool unavailable",
            )),
        }
    }

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
    fn canonicalize_location_batch(
        py: Python<'_>,
        rows: Vec<AddressRow>,
    ) -> PyResult<Vec<LocationCanonicalRow>> {
        let pool = location_canon_pool()?;
        Ok(py.detach(|| {
            pool.install(|| {
                rows.par_iter()
                    .map(|row| {
                        let canonical = canonicalize_address(
                            row.0.as_deref(),
                            row.1.as_deref(),
                            row.2.as_deref(),
                            row.3.as_deref(),
                            row.4.as_deref(),
                            row.5.as_deref(),
                        );
                        (
                            canonical.address_key,
                            canonical.state_code,
                            canonical.city_norm,
                        )
                    })
                    .collect()
            })
        }))
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

    #[pyfunction]
    fn encode_cms_planfinder_observations(
        py: Python<'_>,
        input: &[u8],
        edition_json: &[u8],
    ) -> PyResult<(Py<PyBytes>, usize, Py<PyBytes>)> {
        if edition_json.len() > 65_536 {
            return Err(PyValueError::new_err(
                "Plan Finder edition metadata exceeds byte limit",
            ));
        }
        let (encoded, metadata) = py.detach(|| {
            let edition: crate::cms_planfinder_registry::PlanFinderEdition =
                serde_json::from_slice(edition_json).map_err(|_| {
                    PyValueError::new_err("Plan Finder edition metadata is invalid")
                })?;
            let encoded =
                crate::cms_planfinder_registry::encode_cms_planfinder_observations(input, &edition)
                    .map_err(|error| {
                        PyValueError::new_err(
                            serde_json::to_string(&error)
                                .unwrap_or_else(|_| error.code.to_string()),
                        )
                    })?;
            let metadata = serde_json::to_vec(&serde_json::json!({
                "edition": &encoded.batch.edition,
                "counts": &encoded.batch.counts,
                "conflicts": &encoded.batch.conflicts,
            }))
            .map_err(|_| PyValueError::new_err("Plan Finder metadata encoding failed"))?;
            Ok::<_, PyErr>((encoded, metadata))
        })?;
        Ok((
            PyBytes::new(py, &encoded.copy_bytes).unbind(),
            encoded.row_count,
            PyBytes::new(py, &metadata).unbind(),
        ))
    }

    #[pyfunction]
    fn validate_company_network_assertions(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let validated =
                crate::company_network_assertions::validate_company_network_assertions(input)
                    .map_err(|error| {
                        PyValueError::new_err(
                            serde_json::to_string(&error)
                                .unwrap_or_else(|_| error.code.to_string()),
                        )
                    })?;
            let encoded = serde_json::to_vec(&validated)
                .map_err(|_| PyValueError::new_err("Company network assertion encoding failed"))?;
            if encoded.len() > 16 * 1024 * 1024 {
                return Err(PyValueError::new_err(
                    "Company network assertion output exceeds byte limit",
                ));
            }
            Ok::<_, PyErr>(encoded)
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction]
    fn encode_network_source_binding_batch(
        py: Python<'_>,
        input: &[u8],
    ) -> PyResult<(Py<PyBytes>, usize)> {
        let encoded = py.detach(|| {
            crate::network_source_binding_values::encode_network_source_binding_batch(input)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                    )
                })
        })?;
        Ok((
            PyBytes::new(py, &encoded.copy_bytes).unbind(),
            encoded.row_count,
        ))
    }

    #[pyfunction]
    fn parse_registry_target_ledger(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let ledger = crate::registry_target_ledger::parse_registry_target_ledger(input)
                .map_err(|error| PyValueError::new_err(error.code))?;
            serde_json::to_vec(&ledger)
                .map_err(|_| PyValueError::new_err("Registry target ledger encoding failed"))
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction]
    fn parse_registry_network_evidence(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let evidence =
                crate::registry_network_evidence_values::parse_registry_network_evidence(input)
                    .map_err(|error| PyValueError::new_err(error.to_string()))?;
            serde_json::to_value(&evidence)
                .and_then(|document| serde_json::to_vec(&document))
                .map_err(|_| PyValueError::new_err("registry_network_evidence_invalid"))
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction]
    fn encode_registry_target_ledger_artifact(
        py: Python<'_>,
        input: &[u8],
        snapshot_id: &str,
    ) -> PyResult<(Py<PyBytes>, Py<PyBytes>)> {
        let (copy_bytes, descriptor_bytes) = py.detach(|| {
            crate::registry_target_ledger::encode_registry_target_ledger_artifact(
                input,
                snapshot_id,
            )
            .map_err(|error| PyValueError::new_err(error.code))
        })?;
        Ok((
            PyBytes::new(py, &copy_bytes).unbind(),
            PyBytes::new(py, &descriptor_bytes).unbind(),
        ))
    }

    #[pyfunction]
    fn encode_registry_required_target_review_artifact(
        py: Python<'_>,
        input: &[u8],
        ledger_document: &[u8],
        snapshot_id: &str,
    ) -> PyResult<(Py<PyBytes>, Py<PyBytes>)> {
        let (copy, descriptor) = py.detach(|| {
            crate::registry_required_target_review::encode_registry_required_target_review_artifact(
                input,
                ledger_document,
                snapshot_id,
            )
            .map_err(|error| PyValueError::new_err(error.code))
        })?;
        Ok((
            PyBytes::new(py, &copy).unbind(),
            PyBytes::new(py, &descriptor).unbind(),
        ))
    }

    #[pyfunction]
    fn validate_registry_required_target_ledger_artifact(
        py: Python<'_>,
        input: &[u8],
    ) -> PyResult<Py<PyBytes>> {
        let descriptor = py.detach(|| {
            crate::registry_required_target_review::validate_registry_required_target_ledger_artifact(input)
                .map_err(|error| PyValueError::new_err(error.code))
        })?;
        Ok(PyBytes::new(py, &descriptor).unbind())
    }

    #[pyfunction]
    fn validate_registry_required_target_review_artifacts(
        py: Python<'_>,
        input: &[u8],
    ) -> PyResult<Py<PyBytes>> {
        let descriptor = py.detach(|| {
            crate::registry_required_target_review::validate_registry_required_target_review_artifacts(input)
                .map_err(|error| PyValueError::new_err(error.code))
        })?;
        Ok(PyBytes::new(py, &descriptor).unbind())
    }

    #[pyfunction]
    fn extract_fhir_network_identity_batch(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let batch = crate::fhir_network_identity::extract_fhir_network_identity_batch(input)
                .map_err(|error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                    )
                })?;
            serde_json::to_vec(&batch)
                .map_err(|_| PyValueError::new_err("FHIR network batch encoding failed"))
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction]
    fn encode_fhir_network_identity_batch(
        py: Python<'_>,
        input: &[u8],
    ) -> PyResult<(Py<PyBytes>, usize, usize, usize)> {
        let encoded = py.detach(|| {
            crate::fhir_network_identity::encode_fhir_network_identity_batch(input).map_err(
                |error| {
                    PyValueError::new_err(
                        serde_json::to_string(&error).unwrap_or_else(|_| error.code.to_string()),
                    )
                },
            )
        })?;
        Ok((
            PyBytes::new(py, &encoded.copy_bytes).unbind(),
            encoded.row_count,
            encoded.input_count,
            encoded.duplicate_count,
        ))
    }

    #[pyfunction]
    fn build_registry_network_coverage(py: Python<'_>, input: &[u8]) -> PyResult<Py<PyBytes>> {
        let encoded = py.detach(|| {
            let coverage = crate::registry_network_coverage::build_registry_network_coverage(input)
                .map_err(|error| PyValueError::new_err(error.code))?;
            let encoded = serde_json::to_vec(&coverage)
                .map_err(|_| PyValueError::new_err("Registry coverage encoding failed"))?;
            if encoded.len() > 16 * 1024 * 1024 {
                return Err(PyValueError::new_err(
                    "Registry coverage output exceeds byte limit",
                ));
            }
            Ok::<_, PyErr>(encoded)
        })?;
        Ok(PyBytes::new(py, &encoded).unbind())
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
        m.add_function(wrap_pyfunction!(
            crate::registry_ptg_graph_python::locator,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            crate::registry_ptg_graph_python::members,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            crate::registry_ptg_graph_python::verification,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            crate::registry_ptg_capture_input::encode_registry_ptg_capture_batch_py,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(encode_network_membership_batch, m)?)?;
        m.add_function(wrap_pyfunction!(encode_cms_mlr_observations, m)?)?;
        m.add_function(wrap_pyfunction!(encode_cms_planfinder_observations, m)?)?;
        m.add_function(wrap_pyfunction!(validate_company_network_assertions, m)?)?;
        m.add_function(wrap_pyfunction!(encode_network_source_binding_batch, m)?)?;
        m.add_function(wrap_pyfunction!(build_registry_network_coverage, m)?)?;
        m.add_function(wrap_pyfunction!(parse_registry_target_ledger, m)?)?;
        m.add_function(wrap_pyfunction!(parse_registry_network_evidence, m)?)?;
        m.add_function(wrap_pyfunction!(encode_registry_target_ledger_artifact, m)?)?;
        m.add_function(wrap_pyfunction!(
            validate_registry_required_target_ledger_artifact,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            encode_registry_required_target_review_artifact,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(
            validate_registry_required_target_review_artifacts,
            m
        )?)?;
        m.add_function(wrap_pyfunction!(extract_fhir_network_identity_batch, m)?)?;
        m.add_function(wrap_pyfunction!(encode_fhir_network_identity_batch, m)?)?;
        m.add_function(wrap_pyfunction!(validate_network_catalog_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canonicalize_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canonicalize_location_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canonicalize_contact_batch, m)?)?;
        m.add_function(wrap_pyfunction!(canon_version, m)?)?;
        m.add_function(wrap_pyfunction!(intersect_sorted_u32_py, m)?)?;
        m.add_function(wrap_pyfunction!(decode_u32_le_py, m)?)?;
        m.add_function(wrap_pyfunction!(hospital_price_selector_sha256, m)?)?;
        m.add_function(wrap_pyfunction!(hospital_price_decode_selector_page, m)?)?;
        m.add_function(wrap_pyfunction!(hospital_price_decode_payer_plan_keys, m)?)?;
        m.add_function(wrap_pyfunction!(hospital_price_decode_service_block, m)?)?;
        m.add_function(wrap_pyfunction!(hospital_price_decode_fact_block, m)?)?;
        m.add_function(wrap_pyfunction!(custom_import_scalar_frames_v1, m)?)?;
        m.add_function(wrap_pyfunction!(custom_import_source_documents_v1, m)?)?;
        m.add_function(wrap_pyfunction!(
            custom_import_verified_scalar_frames_v1,
            m
        )?)?;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        include!("python_hospital_price_tests.rs");

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

                let locations = canonicalize_location_batch(
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
                assert_eq!(locations.len(), 1);
                assert!(locations[0].0.is_some());
                assert!(canonicalize_location_batch(py, vec![]).unwrap().is_empty());

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
                assert!(module.hasattr("canonicalize_location_batch").unwrap());
                assert!(module.hasattr("canonicalize_contact_batch").unwrap());
                assert!(module.hasattr("custom_import_source_documents_v1").unwrap());
                assert!(module.hasattr("canon_version").unwrap());
                assert!(module.hasattr("intersect_sorted_u32").unwrap());
                assert!(module.hasattr("ptg2_decode_u32_le").unwrap());
                assert!(module.hasattr("hospital_price_selector_sha256").unwrap());
                assert!(module
                    .hasattr("hospital_price_decode_selector_page")
                    .unwrap());
                assert!(module
                    .hasattr("hospital_price_decode_service_block")
                    .unwrap());
                assert!(module.hasattr("hospital_price_decode_fact_block").unwrap());
                let empty_locations = module
                    .getattr("canonicalize_location_batch")
                    .unwrap()
                    .call1((Vec::<AddressRow>::new(),))
                    .unwrap();
                assert_eq!(empty_locations.len().unwrap(), 0);

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
