use crate::hospital_price_block::{decode_fact_block, HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS};
use crate::hospital_price_selector_block::{
    decode_selector_page, selector_key_sha256, HospitalPriceSelectorKey,
};
use crate::hospital_price_service_block::decode_service_block;

const HOSPITAL_PRICE_PUBLIC_SELECTOR_MAX_REFS: usize = 10_001;

fn hospital_price_py_value<'py, T>(py: Python<'py>, value: T) -> Bound<'py, PyAny>
where
    T: IntoPyObject<'py, Error = Infallible>,
{
    match value.into_pyobject(py) {
        Ok(value) => value.into_any().into_bound(),
        Err(error) => match error {},
    }
}

fn hospital_price_dict<'py>(
    py: Python<'py>,
    fields: &[(&str, Bound<'py, PyAny>)],
) -> PyResult<Bound<'py, PyDict>> {
    let payload = PyDict::new(py);
    for (name, value) in fields {
        payload.set_item(name, value)?;
    }
    Ok(payload)
}

fn hospital_price_dict_list<'py>(
    py: Python<'py>,
    payloads: &mut dyn Iterator<Item = PyResult<Bound<'py, PyDict>>>,
) -> PyResult<Bound<'py, PyList>> {
    let payloads = payloads.collect::<PyResult<Vec<_>>>()?;
    PyList::new(py, payloads)
}

fn hospital_price_selector_key(
    kind: &str,
    first: &str,
    second: &str,
) -> PyResult<HospitalPriceSelectorKey> {
    match kind {
        "code" => Ok(HospitalPriceSelectorKey::Code {
            code_type: first.to_owned(),
            code: second.to_owned(),
        }),
        "payer_plan" => Ok(HospitalPriceSelectorKey::PayerPlan {
            payer_name: first.to_owned(),
            plan_name: second.to_owned(),
        }),
        _ => Err(PyValueError::new_err(
            "hospital price selector kind is invalid",
        )),
    }
}

#[pyfunction]
fn hospital_price_selector_sha256<'py>(
    py: Python<'py>,
    kind: &str,
    first: &str,
    second: &str,
) -> PyResult<Bound<'py, PyBytes>> {
    let key = hospital_price_selector_key(kind, first, second)?;
    Ok(PyBytes::new(py, &selector_key_sha256(&key)))
}

#[pyfunction]
fn hospital_price_decode_selector_page<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyBytes>,
    kind: &str,
    first: &str,
    second: &str,
    ranges: Vec<(u64, u64)>,
    max_refs: usize,
) -> PyResult<Bound<'py, PyDict>> {
    let key = hospital_price_selector_key(kind, first, second)?;
    if ranges.is_empty()
        || max_refs == 0
        || max_refs > HOSPITAL_PRICE_PUBLIC_SELECTOR_MAX_REFS
        || ranges
            .iter()
            .any(|(range_start, range_end)| range_start >= range_end)
        || ranges
            .windows(2)
            .any(|pair| pair[0].1 > pair[1].0)
    {
        return Err(PyValueError::new_err(
            "hospital price selector ranges are invalid",
        ));
    }
    let payload = payload.as_bytes().to_vec();
    let (
        page_index,
        page_count,
        row_count,
        page_ref_count,
        found,
        ref_count,
        first_ref,
        refs,
        truncated,
    ) = py
        .detach(move || {
            let page = decode_selector_page(&payload)?;
            let page_refs = page.exact_refs(&key);
            let mut selected = Vec::with_capacity(
                page_refs.map_or(0, |references| max_refs.min(references.len())),
            );
            let mut truncated = false;
            if let Some(page_refs) = page_refs {
                'ranges: for (range_start, range_end) in ranges {
                    let start = page_refs.partition_point(|reference| *reference < range_start);
                    let end = page_refs.partition_point(|reference| *reference < range_end);
                    for reference in &page_refs[start..end] {
                        if selected.len() == max_refs {
                            truncated = true;
                            break 'ranges;
                        }
                        selected.push(*reference);
                    }
                }
            }
            Ok::<_, String>((
                page.page_index,
                page.page_count,
                page.row_count(),
                page.ref_count(),
                page_refs.is_some(),
                page_refs.map_or(0, |references| references.len()),
                page_refs.and_then(|references| references.first().copied()),
                selected,
                truncated,
            ))
        })
        .map_err(PyValueError::new_err)?;
    let refs = PyList::new(py, refs)?;
    hospital_price_dict(
        py,
        &[
            ("page_index", hospital_price_py_value(py, page_index)),
            ("page_count", hospital_price_py_value(py, page_count)),
            ("row_count", hospital_price_py_value(py, row_count)),
            ("page_ref_count", hospital_price_py_value(py, page_ref_count)),
            ("found", hospital_price_py_value(py, found)),
            ("ref_count", hospital_price_py_value(py, ref_count)),
            ("first_ref", hospital_price_py_value(py, first_ref)),
            ("refs", refs.into_any()),
            ("truncated", hospital_price_py_value(py, truncated)),
        ],
    )
}

fn hospital_price_code_payload<'py>(
    py: Python<'py>,
    code: &crate::hospital_price_service_block::HospitalPriceServiceCode,
) -> PyResult<Bound<'py, PyDict>> {
    hospital_price_dict(
        py,
        &[
            (
                "code_type",
                hospital_price_py_value(py, code.code_type.as_str()),
            ),
            ("code", hospital_price_py_value(py, code.code.as_str())),
        ],
    )
}

fn hospital_price_charge_payload<'py>(
    py: Python<'py>,
    charge: &crate::hospital_price_service_block::HospitalPriceChargeRow,
) -> PyResult<Bound<'py, PyDict>> {
    let modifier_codes = PyList::new(py, &charge.modifier_codes)?;
    hospital_price_dict(
        py,
        &[
            (
                "charge_key",
                hospital_price_py_value(py, charge.charge_key),
            ),
            (
                "charge_ordinal",
                hospital_price_py_value(py, charge.charge_ordinal),
            ),
            (
                "setting",
                hospital_price_py_value(py, charge.setting.as_str()),
            ),
            (
                "billing_class",
                hospital_price_py_value(py, charge.billing_class.as_deref()),
            ),
            ("modifier_codes", modifier_codes.into_any()),
            (
                "gross_charge",
                hospital_price_py_value(py, charge.gross_charge.as_deref()),
            ),
            (
                "discounted_cash",
                hospital_price_py_value(py, charge.discounted_cash.as_deref()),
            ),
            (
                "minimum",
                hospital_price_py_value(py, charge.minimum.as_deref()),
            ),
            (
                "maximum",
                hospital_price_py_value(py, charge.maximum.as_deref()),
            ),
            (
                "additional_generic_notes",
                hospital_price_py_value(py, charge.additional_generic_notes.as_deref()),
            ),
            (
                "first_fact_ordinal",
                hospital_price_py_value(py, charge.first_fact_ordinal),
            ),
            (
                "fact_count",
                hospital_price_py_value(py, charge.fact_count),
            ),
        ],
    )
}

fn hospital_price_service_payload<'py>(
    py: Python<'py>,
    service: &crate::hospital_price_service_block::HospitalPriceServiceRow,
) -> PyResult<Bound<'py, PyDict>> {
    let mut code_payloads = service
        .codes
        .iter()
        .map(|code| hospital_price_code_payload(py, code));
    let codes = hospital_price_dict_list(py, &mut code_payloads)?;
    let mut charge_payloads = service
        .charges
        .iter()
        .map(|charge| hospital_price_charge_payload(py, charge));
    let charges = hospital_price_dict_list(py, &mut charge_payloads)?;
    hospital_price_dict(
        py,
        &[
            (
                "service_ordinal",
                hospital_price_py_value(py, service.service_ordinal),
            ),
            (
                "description",
                hospital_price_py_value(py, service.description.as_str()),
            ),
            (
                "drug_unit",
                hospital_price_py_value(py, service.drug_unit.as_deref()),
            ),
            (
                "drug_type",
                hospital_price_py_value(py, service.drug_type.as_deref()),
            ),
            ("codes", codes.into_any()),
            ("charges", charges.into_any()),
        ],
    )
}

#[pyfunction]
fn hospital_price_decode_service_block<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let payload = payload.as_bytes().to_vec();
    let services = py
        .detach(move || decode_service_block(&payload))
        .map_err(PyValueError::new_err)?;
    let mut output = services
        .iter()
        .map(|service| hospital_price_service_payload(py, service));
    hospital_price_dict_list(py, &mut output)
}

fn hospital_price_fact_payload<'py>(
    py: Python<'py>,
    fact: &crate::hospital_price_block::HospitalPriceFactRow,
) -> PyResult<Bound<'py, PyDict>> {
    hospital_price_dict(
        py,
        &[
            (
                "charge_key",
                hospital_price_py_value(py, fact.charge_key),
            ),
            (
                "payer_name",
                hospital_price_py_value(py, fact.payer_name.as_str()),
            ),
            (
                "plan_name",
                hospital_price_py_value(py, fact.plan_name.as_str()),
            ),
            (
                "negotiated_dollar",
                hospital_price_py_value(py, fact.negotiated_dollar.as_deref()),
            ),
            (
                "negotiated_percentage",
                hospital_price_py_value(py, fact.negotiated_percentage.as_deref()),
            ),
            (
                "negotiated_algorithm",
                hospital_price_py_value(py, fact.negotiated_algorithm.as_deref()),
            ),
            (
                "methodology",
                hospital_price_py_value(py, fact.methodology.as_str()),
            ),
            (
                "median_amount",
                hospital_price_py_value(py, fact.median_amount.as_deref()),
            ),
            (
                "percentile_10",
                hospital_price_py_value(py, fact.percentile_10.as_deref()),
            ),
            (
                "percentile_90",
                hospital_price_py_value(py, fact.percentile_90.as_deref()),
            ),
            (
                "allowed_count",
                hospital_price_py_value(py, fact.allowed_count.as_deref()),
            ),
            (
                "additional_payer_notes",
                hospital_price_py_value(py, fact.additional_payer_notes.as_deref()),
            ),
            (
                "comparison_amount",
                hospital_price_py_value(py, fact.comparison_amount.as_deref()),
            ),
        ],
    )
}

#[pyfunction]
fn hospital_price_decode_fact_block<'py>(
    py: Python<'py>,
    payload: &Bound<'py, PyBytes>,
) -> PyResult<Bound<'py, PyList>> {
    let payload = payload.as_bytes().to_vec();
    let facts = py
        .detach(move || {
            decode_fact_block(
                &payload,
                None,
                None,
                0,
                HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS,
            )
        })
        .map_err(PyValueError::new_err)?;
    let mut output = facts
        .iter()
        .map(|fact| hospital_price_fact_payload(py, fact));
    hospital_price_dict_list(py, &mut output)
}
