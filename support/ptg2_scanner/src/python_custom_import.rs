use crate::custom_import_scalar::{
    scalar_digest_frames, verified_scalar_frames, ScalarDigestColumns, ScalarDigestRow,
    VerificationLayout, VerificationRevision, VerificationRow, VerificationValue,
    MAX_SCALAR_DIGEST_ROWS, MAX_VERIFICATION_REVISIONS, MAX_VERIFICATION_ROWS, SCALAR_TYPES,
};
use pyo3::types::{PyBool, PyInt, PyString, PyTuple};

type ScalarDigestBinding = (i16, i16, i16, Option<i16>, String, String);
type ScalarDigestValues = (
    Option<String>,
    Option<i64>,
    Option<String>,
    Option<bool>,
    Option<String>,
    Option<String>,
);
type ScalarDigestInput = (
    String,
    Option<String>,
    ScalarDigestBinding,
    ScalarDigestValues,
);

fn bounded_scalar_text(value: &Bound<'_, PyAny>, limit: usize) -> PyResult<()> {
    if value.is_none() {
        return Ok(());
    }
    if !value.is_exact_instance_of::<PyString>() {
        return Err(PyValueError::new_err(
            "scalar digest text requires a native string",
        ));
    }
    let text = value.cast::<PyString>()?;
    // Bound Unicode size before requesting its UTF-8 representation, then
    // check borrowed UTF-8 bytes before String extraction allocates a copy.
    if text.len()? > limit || text.to_str()?.len() > limit {
        return Err(PyValueError::new_err(
            "scalar digest text exceeds its admitted encoding",
        ));
    }
    Ok(())
}

fn validate_scalar_input(row: &Bound<'_, PyAny>) -> PyResult<()> {
    let row = row.cast::<PyTuple>()?;
    if row.len() != 4 {
        return Err(PyValueError::new_err(
            "scalar digest tuple shape is invalid",
        ));
    }
    bounded_scalar_text(&row.get_item(0)?, 64)?;
    bounded_scalar_text(&row.get_item(1)?, 64)?;
    let binding = row.get_item(2)?;
    let binding = binding.cast::<PyTuple>()?;
    let values = row.get_item(3)?;
    let values = values.cast::<PyTuple>()?;
    if binding.len() != 6 || values.len() != 6 {
        return Err(PyValueError::new_err(
            "scalar digest tuple shape is invalid",
        ));
    }
    bounded_scalar_text(&binding.get_item(4)?, 16)?;
    bounded_scalar_text(&binding.get_item(5)?, 8)?;
    for (index, limit) in [(0, 2_048), (2, 64), (4, 64), (5, 64)] {
        bounded_scalar_text(&values.get_item(index)?, limit)?;
    }
    if values.get_item(1)?.is_instance_of::<PyBool>() {
        return Err(PyValueError::new_err(
            "scalar digest integer cannot be boolean",
        ));
    }
    Ok(())
}

#[pyfunction]
fn custom_import_scalar_frames_v1<'py>(
    py: Python<'py>,
    child: bool,
    rows: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyBytes>> {
    if rows.len() > MAX_SCALAR_DIGEST_ROWS {
        return Err(PyValueError::new_err(
            "scalar digest batch exceeds its row bound",
        ));
    }
    let rows = rows
        .iter()
        .map(|row| {
            validate_scalar_input(&row)?;
            let (root_key_sha256, child_key_sha256, binding, values): ScalarDigestInput =
                row.extract()?;
            Ok(ScalarDigestRow {
                child_key_sha256,
                root_key_sha256,
                scalar: ScalarDigestColumns {
                    boolean_value: values.3,
                    collection_slot: binding.3,
                    date_value: values.4,
                    decimal_value: values.2,
                    field_collection_slot: binding.1,
                    field_slot: binding.0,
                    field_type: binding.4,
                    integer_value: values.1,
                    projection_slot: binding.2,
                    string_value: values.0,
                    timestamp_value: values.5,
                    value_state: binding.5,
                },
            })
        })
        .collect::<PyResult<Vec<_>>>()?;
    let frames = py
        .detach(move || scalar_digest_frames(child, &rows))
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &frames))
}

struct VerificationPythonTypes<'py> {
    decimal: Bound<'py, PyAny>,
    date: Bound<'py, PyAny>,
    datetime: Bound<'py, PyAny>,
    utc: Bound<'py, PyAny>,
}

fn verification_tuple<'py>(
    value: &Bound<'py, PyAny>,
    size: usize,
) -> PyResult<Bound<'py, PyTuple>> {
    let tuple = value.cast::<PyTuple>()?;
    if tuple.len() != size {
        return Err(PyValueError::new_err(
            "scalar verification tuple shape differs",
        ));
    }
    Ok(tuple.clone())
}

fn verification_integer(value: &Bound<'_, PyAny>) -> PyResult<i64> {
    if !value.is_exact_instance_of::<PyInt>() {
        return Err(PyValueError::new_err(
            "scalar verification identity requires an integer",
        ));
    }
    value.extract()
}

fn verification_decimal(value: &Bound<'_, PyAny>) -> PyResult<VerificationValue> {
    // Exact Decimal instances only. Bound their coefficient allocation before
    // as_tuple, then the fixed-point output before formatting or integer ratio.
    if value.call_method0("__sizeof__")?.extract::<usize>()? > 512
        || !value.call_method0("is_finite")?.extract::<bool>()?
    {
        return Err(PyValueError::new_err(
            "scalar verification decimal exceeds its bound",
        ));
    }
    let parts = value.call_method0("as_tuple")?;
    let digits = parts.get_item(1)?.len()?;
    let exponent: i64 = parts.get_item(2)?.extract()?;
    let zero: bool = value.call_method0("is_zero")?.extract()?;
    let sign: usize = parts.get_item(0)?.extract()?;
    let width = if exponent >= 0 {
        if zero {
            1
        } else {
            digits.saturating_add(exponent as usize)
        }
    } else {
        digits
            .max(exponent.unsigned_abs() as usize + 1)
            .saturating_add(1)
    };
    if width.saturating_add(sign) > 64 {
        return Err(PyValueError::new_err(
            "scalar verification decimal encoding exceeds its bound",
        ));
    }
    let ratio = if zero {
        (0, 1)
    } else {
        value
            .call_method0("as_integer_ratio")?
            .extract::<(i128, i128)>()?
    };
    if ratio.1 <= 0
        || ratio.1 > 1_000_000_000_000
        || 1_000_000_000_000_i128 % ratio.1 != 0
        || ratio.0.unsigned_abs() >= 1_000_000_000_000_000_000_u128 * ratio.1 as u128
    {
        return Err(PyValueError::new_err(
            "scalar verification decimal exceeds storage",
        ));
    }
    let text = if zero && exponent >= 0 {
        if sign == 0 {
            "0".to_owned()
        } else {
            "-0".to_owned()
        }
    } else {
        value
            .call_method1("__format__", ("f",))?
            .extract::<String>()?
    };
    Ok(VerificationValue::Decimal(ratio, text))
}

fn verification_value(
    value: &Bound<'_, PyAny>,
    kind: &str,
    types: &VerificationPythonTypes<'_>,
) -> PyResult<VerificationValue> {
    if value.is_none() {
        return Ok(VerificationValue::Null);
    }
    let malformed = || PyValueError::new_err("scalar verification value type differs");
    match kind {
        "string" => {
            bounded_scalar_text(value, 2_048)?;
            let text: String = value.extract()?;
            if text.contains('\0') {
                return Err(malformed());
            }
            Ok(VerificationValue::String(text))
        }
        "integer" => Ok(VerificationValue::Integer(verification_integer(value)?)),
        "boolean" if value.is_exact_instance_of::<PyBool>() => {
            Ok(VerificationValue::Boolean(value.extract()?))
        }
        "decimal" if value.get_type().is(&types.decimal) => verification_decimal(value),
        "date" if value.get_type().is(&types.date) => Ok(VerificationValue::Date(
            value.call_method0("isoformat")?.extract()?,
        )),
        "timestamp" if value.get_type().is(&types.datetime) => {
            if value.call_method0("utcoffset")?.is_none() {
                return Err(malformed());
            }
            let text: String = value
                .call_method1("astimezone", (&types.utc,))?
                .call_method0("isoformat")?
                .extract()?;
            Ok(VerificationValue::Timestamp(text.replace("+00:00", "Z")))
        }
        _ => Err(malformed()),
    }
}

fn verification_layout_input(layouts: &Bound<'_, PyList>) -> PyResult<Vec<VerificationLayout>> {
    if layouts.len() > 8 {
        return Err(PyValueError::new_err("scalar layout bound exceeded"));
    }
    let mut result = Vec::with_capacity(layouts.len());
    let mut count = 0;
    for layout in layouts.iter() {
        let layout = verification_tuple(&layout, 2)?;
        let slot = verification_integer(&layout.get_item(0)?)?;
        let field_items = layout.get_item(1)?;
        let field_items = field_items.cast::<PyTuple>()?;
        count += field_items.len();
        if count > 20 {
            return Err(PyValueError::new_err("scalar field bound exceeded"));
        }
        let mut fields = Vec::with_capacity(field_items.len());
        for field in field_items.iter() {
            let field = verification_tuple(&field, 4)?;
            verification_integer(&field.get_item(0)?)?;
            verification_integer(&field.get_item(1)?)?;
            bounded_scalar_text(&field.get_item(2)?, 16)?;
            if !field.get_item(3)?.is_exact_instance_of::<PyBool>() {
                return Err(PyValueError::new_err("scalar nullable binding differs"));
            }
            fields.push(field.extract()?);
        }
        result.push((
            i16::try_from(slot).map_err(|_| PyValueError::new_err("scalar collection differs"))?,
            fields,
        ));
    }
    Ok(result)
}

fn verification_actual_row(
    value: &Bound<'_, PyAny>,
    types: &VerificationPythonTypes<'_>,
) -> PyResult<VerificationRow> {
    let row = verification_tuple(value, 3)?;
    let identity = verification_tuple(&row.get_item(0)?, 4)?;
    for item in identity.iter() {
        verification_integer(&item)?;
    }
    let binding = verification_tuple(&row.get_item(1)?, 6)?;
    for index in 0..4 {
        let item = binding.get_item(index)?;
        if index != 3 || !item.is_none() {
            verification_integer(&item)?;
        }
    }
    bounded_scalar_text(&binding.get_item(4)?, 16)?;
    bounded_scalar_text(&binding.get_item(5)?, 8)?;
    let cells = verification_tuple(&row.get_item(2)?, 6)?;
    let mut values = Vec::with_capacity(6);
    for (index, kind) in SCALAR_TYPES.iter().enumerate() {
        values.push(verification_value(&cells.get_item(index)?, kind, types)?);
    }
    Ok(VerificationRow {
        identity: identity.extract()?,
        binding: binding.extract()?,
        values: values
            .try_into()
            .map_err(|_| PyValueError::new_err("scalar value columns differ"))?,
    })
}

fn verification_revision_input(
    value: &Bound<'_, PyAny>,
    layouts: &[VerificationLayout],
    types: &VerificationPythonTypes<'_>,
) -> PyResult<VerificationRevision> {
    let revision = verification_tuple(value, 4)?;
    let target = verification_tuple(&revision.get_item(0)?, 3)?;
    for item in target.iter() {
        verification_integer(&item)?;
    }
    let target: (i64, i64, i16) = target.extract()?;
    let layout = &layouts
        .iter()
        .find(|item| item.0 == target.2)
        .ok_or_else(|| PyValueError::new_err("scalar collection differs"))?
        .1;
    let keys = verification_tuple(&revision.get_item(1)?, 2)?;
    for key in keys.iter() {
        bounded_scalar_text(&key, 64)?;
    }
    let cells = revision.get_item(2)?;
    let cells = cells.cast::<PyTuple>()?;
    if cells.len() != layout.len() {
        return Err(PyValueError::new_err("scalar expected fields differ"));
    }
    let mut expected = Vec::with_capacity(cells.len());
    for (cell, field) in cells.iter().zip(layout) {
        let cell = verification_tuple(&cell, 2)?;
        bounded_scalar_text(&cell.get_item(0)?, 8)?;
        expected.push((
            cell.get_item(0)?.extract()?,
            verification_value(&cell.get_item(1)?, &field.2, types)?,
        ));
    }
    let actual = revision.get_item(3)?;
    let actual = actual.cast::<PyList>()?;
    if actual.len() > 20 {
        return Err(PyValueError::new_err("scalar revision row bound exceeded"));
    }
    let mut rows = Vec::with_capacity(actual.len());
    for row in actual.iter() {
        rows.push(verification_actual_row(&row, types)?);
    }
    Ok(VerificationRevision {
        target,
        keys: keys.extract()?,
        expected,
        rows,
    })
}

#[pyfunction]
fn custom_import_verified_scalar_frames_v1<'py>(
    py: Python<'py>,
    child: bool,
    owner: &Bound<'py, PyTuple>,
    layouts: &Bound<'py, PyList>,
    revisions: &Bound<'py, PyList>,
) -> PyResult<Bound<'py, PyBytes>> {
    let owner = verification_tuple(owner.as_any(), 2)?;
    for item in owner.iter() {
        verification_integer(&item)?;
    }
    if revisions.len() > MAX_VERIFICATION_REVISIONS {
        return Err(PyValueError::new_err(
            "scalar revision batch bound exceeded",
        ));
    }
    let layouts = verification_layout_input(layouts)?;
    let mut cells = 0;
    let mut rows = 0;
    // Check aggregate cardinality before copying any revision values.
    for revision in revisions.iter() {
        let revision = verification_tuple(&revision, 4)?;
        cells += revision.get_item(2)?.cast::<PyTuple>()?.len();
        rows += revision.get_item(3)?.cast::<PyList>()?.len();
    }
    if cells > MAX_VERIFICATION_ROWS || rows > MAX_VERIFICATION_ROWS {
        return Err(PyValueError::new_err(
            "scalar verification batch bound exceeded",
        ));
    }
    let datetime = py.import("datetime")?;
    let types = VerificationPythonTypes {
        decimal: py.import("decimal")?.getattr("Decimal")?,
        date: datetime.getattr("date")?,
        datetime: datetime.getattr("datetime")?,
        utc: datetime.getattr("UTC")?,
    };
    let mut prepared = Vec::with_capacity(revisions.len());
    for revision in revisions.iter() {
        prepared.push(verification_revision_input(&revision, &layouts, &types)?);
    }
    let owner = owner.extract()?;
    let frames = py
        .detach(move || verified_scalar_frames(child, owner, &layouts, prepared))
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &frames))
}
