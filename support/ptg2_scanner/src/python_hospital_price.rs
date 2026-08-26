use crate::hospital_price_block::{decode_fact_block, HOSPITAL_PRICE_FACT_BLOCK_MAX_ROWS};
use crate::hospital_price_selector_block::{
    decode_selector_page, selector_key_sha256, HospitalPriceSelectorKey,
};
use crate::hospital_price_service_block::decode_service_block;

const HOSPITAL_PRICE_PUBLIC_SELECTOR_MAX_REFS: usize = 10_001;

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
    let (page_index, page_count, ref_count, first_ref, refs, truncated) = py
        .detach(move || {
            let page = decode_selector_page(&payload)?;
            let page_refs = page
                .exact_refs(&key)
                .ok_or_else(|| "hospital price selector key is absent".to_owned())?;
            let mut selected = Vec::with_capacity(max_refs.min(page_refs.len()));
            let mut truncated = false;
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
            Ok::<_, String>((
                page.page_index,
                page.page_count,
                page_refs.len(),
                page_refs.first().copied(),
                selected,
                truncated,
            ))
        })
        .map_err(PyValueError::new_err)?;
    let result = PyDict::new(py);
    result.set_item("page_index", page_index)?;
    result.set_item("page_count", page_count)?;
    result.set_item("ref_count", ref_count)?;
    result.set_item("first_ref", first_ref)?;
    result.set_item("refs", refs)?;
    result.set_item("truncated", truncated)?;
    Ok(result)
}

fn hospital_price_code_payload<'py>(
    py: Python<'py>,
    code: &crate::hospital_price_service_block::HospitalPriceServiceCode,
) -> PyResult<Bound<'py, PyDict>> {
    let payload = PyDict::new(py);
    payload.set_item("code_type", &code.code_type)?;
    payload.set_item("code", &code.code)?;
    Ok(payload)
}

fn hospital_price_charge_payload<'py>(
    py: Python<'py>,
    charge: &crate::hospital_price_service_block::HospitalPriceChargeRow,
) -> PyResult<Bound<'py, PyDict>> {
    let payload = PyDict::new(py);
    payload.set_item("charge_key", charge.charge_key)?;
    payload.set_item("charge_ordinal", charge.charge_ordinal)?;
    payload.set_item("setting", &charge.setting)?;
    payload.set_item("billing_class", charge.billing_class.as_deref())?;
    payload.set_item("modifier_codes", &charge.modifier_codes)?;
    payload.set_item("gross_charge", charge.gross_charge.as_deref())?;
    payload.set_item("discounted_cash", charge.discounted_cash.as_deref())?;
    payload.set_item("minimum", charge.minimum.as_deref())?;
    payload.set_item("maximum", charge.maximum.as_deref())?;
    payload.set_item(
        "additional_generic_notes",
        charge.additional_generic_notes.as_deref(),
    )?;
    payload.set_item("first_fact_ordinal", charge.first_fact_ordinal)?;
    payload.set_item("fact_count", charge.fact_count)?;
    Ok(payload)
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
    let output = PyList::empty(py);
    for service in services {
        let item = PyDict::new(py);
        item.set_item("service_ordinal", service.service_ordinal)?;
        item.set_item("description", &service.description)?;
        item.set_item("drug_unit", service.drug_unit.as_deref())?;
        item.set_item("drug_type", service.drug_type.as_deref())?;
        let codes = PyList::empty(py);
        for code in &service.codes {
            codes.append(hospital_price_code_payload(py, code)?)?;
        }
        item.set_item("codes", codes)?;
        let charges = PyList::empty(py);
        for charge in &service.charges {
            charges.append(hospital_price_charge_payload(py, charge)?)?;
        }
        item.set_item("charges", charges)?;
        output.append(item)?;
    }
    Ok(output)
}

fn hospital_price_fact_payload<'py>(
    py: Python<'py>,
    fact: &crate::hospital_price_block::HospitalPriceFactRow,
) -> PyResult<Bound<'py, PyDict>> {
    let payload = PyDict::new(py);
    payload.set_item("charge_key", fact.charge_key)?;
    payload.set_item("payer_name", &fact.payer_name)?;
    payload.set_item("plan_name", &fact.plan_name)?;
    payload.set_item("negotiated_dollar", fact.negotiated_dollar.as_deref())?;
    payload.set_item(
        "negotiated_percentage",
        fact.negotiated_percentage.as_deref(),
    )?;
    payload.set_item(
        "negotiated_algorithm",
        fact.negotiated_algorithm.as_deref(),
    )?;
    payload.set_item("methodology", &fact.methodology)?;
    payload.set_item("median_amount", fact.median_amount.as_deref())?;
    payload.set_item("percentile_10", fact.percentile_10.as_deref())?;
    payload.set_item("percentile_90", fact.percentile_90.as_deref())?;
    payload.set_item("allowed_count", fact.allowed_count.as_deref())?;
    payload.set_item(
        "additional_payer_notes",
        fact.additional_payer_notes.as_deref(),
    )?;
    payload.set_item("comparison_amount", fact.comparison_amount.as_deref())?;
    Ok(payload)
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
    let output = PyList::empty(py);
    for fact in facts {
        output.append(hospital_price_fact_payload(py, &fact)?)?;
    }
    Ok(output)
}
