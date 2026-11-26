use crate::custom_import_direct_stage::{
    compute_pack_hash, finalize_direct_stage_copy, prepare_direct_stage_batch,
    DirectStageBatchConfig, DirectStageBatchResult, MAX_DIRECT_STAGE_BUCKETS,
    MAX_DIRECT_STAGE_INPUT_BYTES, MAX_DIRECT_STAGE_ROWS,
};

/// Compiled immutable layout for direct compact v2 columnar staging.
#[pyclass(
    frozen,
    module = "ptg2_address_canon",
    name = "_DirectStageColumnsPlan"
)]
struct DirectStageColumnsPlan {
    layout: ColumnLayout,
    input_bytes: usize,
}

impl DirectStageColumnsPlan {
    fn compile(value: &Bound<'_, PyTuple>) -> PyResult<Self> {
        let layout = column_layout_input(value)?;
        let input_bytes = layout
            .source
            .fields
            .iter()
            .map(|(name, kind)| 128 + name.len() + kind.len())
            .sum::<usize>()
            + layout
                .source
                .root_key
                .iter()
                .map(|(name, _)| 64 + name.len())
                .sum::<usize>()
            + 8 * layout.source.child_key.len();
        Ok(Self {
            layout,
            input_bytes,
        })
    }
}

fn direct_stage_usize(
    value: &Bound<'_, PyAny>,
    minimum: i64,
    maximum: i64,
    label: &str,
) -> PyResult<usize> {
    let parsed = verification_integer(value)?;
    if parsed < minimum || parsed > maximum {
        return Err(PyValueError::new_err(format!("{label} is out of bounds")));
    }
    Ok(parsed as usize)
}

fn direct_stage_i16(
    value: &Bound<'_, PyAny>,
    minimum: i64,
    maximum: i64,
    label: &str,
) -> PyResult<i16> {
    let parsed = verification_integer(value)?;
    if parsed < minimum || parsed > maximum {
        return Err(PyValueError::new_err(format!("{label} is out of bounds")));
    }
    Ok(parsed as i16)
}

fn direct_stage_config_input(options: &Bound<'_, PyTuple>) -> PyResult<DirectStageBatchConfig> {
    let options = column_exact_tuple(options.as_any(), Some(10))?;
    let first_source_ordinal = verification_integer(&options.get_item(3)?)?;
    if first_source_ordinal < 0 {
        return Err(PyValueError::new_err(
            "first_source_ordinal must be non-negative",
        ));
    }
    Ok(DirectStageBatchConfig {
        record_kind_code: direct_stage_i16(&options.get_item(0)?, 0, 1, "record_kind_code")?,
        stream_slot: direct_stage_i16(&options.get_item(1)?, 1, i16::MAX as i64, "stream_slot")?,
        collection_slot: direct_stage_i16(
            &options.get_item(2)?,
            0,
            i16::MAX as i64,
            "collection_slot",
        )?,
        first_source_ordinal,
        bucket_count: direct_stage_usize(
            &options.get_item(4)?,
            1,
            MAX_DIRECT_STAGE_BUCKETS as i64,
            "bucket_count",
        )?,
        page_byte_limit: direct_stage_usize(
            &options.get_item(5)?,
            1,
            268_435_456,
            "page_byte_limit",
        )?,
        maximum_record_bytes: direct_stage_usize(
            &options.get_item(6)?,
            1,
            268_435_456,
            "maximum_record_bytes",
        )?,
        max_rows: direct_stage_usize(&options.get_item(7)?, 0, 10_000_000, "max_rows")?,
        max_bytes: direct_stage_usize(&options.get_item(8)?, 0, 268_435_456, "max_bytes")?,
        has_prior_rows: column_bool(&options.get_item(9)?)?,
    })
}

#[pymethods]
impl DirectStageColumnsPlan {
    fn prepare_batch<'py>(
        &self,
        py: Python<'py>,
        columns: &Bound<'py, PyTuple>,
        options: &Bound<'py, PyTuple>,
    ) -> PyResult<Option<Py<PyAny>>> {
        let layout = &self.layout;
        if layout
            .source
            .fields
            .iter()
            .any(|(_, kind)| matches!(kind.as_str(), "date" | "timestamp"))
        {
            return Ok(None);
        }
        let cfg = direct_stage_config_input(options)?;

        let columns = column_exact_tuple(columns.as_any(), None)?;
        if columns.len() != layout.source.fields.len() {
            return Err(PyValueError::new_err(
                "direct stage column field count differs",
            ));
        }
        let mut input = self.input_bytes;
        let mut row_count = None;
        let mut descriptors = Vec::with_capacity(columns.len());
        for (index, value) in columns.iter().enumerate() {
            let descriptor = column_exact_tuple(&value, Some(9))?;
            let name = source_identifier(&descriptor.get_item(0)?)?;
            if name != layout.source.fields[index].0 {
                return Err(PyValueError::new_err(
                    "direct stage column identity differs",
                ));
            }
            bounded_scalar_text(&descriptor.get_item(1)?, 16)?;
            let kind: String = descriptor.get_item(1)?.extract()?;
            if !matches!(
                kind.as_str(),
                "int8"
                    | "int16"
                    | "int32"
                    | "int64"
                    | "bool"
                    | "utf8"
                    | "large_utf8"
                    | "decimal128"
                    | "null"
            ) {
                return Ok(None);
            }
            let bit_offset = verification_integer(&descriptor.get_item(2)?)?;
            let rows = verification_integer(&descriptor.get_item(3)?)?;
            let precision = verification_integer(&descriptor.get_item(4)?)?;
            let scale = verification_integer(&descriptor.get_item(5)?)?;
            if !(0..=7).contains(&bit_offset) {
                return Err(PyValueError::new_err(
                    "direct stage column bit offset differs",
                ));
            }
            if !(0..=MAX_DIRECT_STAGE_ROWS as i64).contains(&rows) {
                return Ok(None);
            }
            if row_count.is_some_and(|count| count != rows as usize) {
                return Err(PyValueError::new_err(
                    "direct stage column row count differs",
                ));
            }
            if row_count.is_none() {
                row_count = Some(rows as usize);
            }
            input += 128 + name.len() + kind.len();
            for item in 6..=8 {
                input += column_bytes_len(&descriptor.get_item(item)?, item < 8)?;
            }
            if input > MAX_DIRECT_STAGE_INPUT_BYTES {
                return Ok(None);
            }
            descriptors.push((
                descriptor,
                kind,
                bit_offset as usize,
                rows as usize,
                precision,
                scale,
            ));
        }

        let slot = py
            .detach(source_encoding_slot)
            .map_err(PyValueError::new_err)?;
        let owned = descriptors
            .into_iter()
            .map(|(value, kind, bit_offset, rows, precision, scale)| {
                Ok(Column {
                    kind,
                    bit_offset,
                    rows,
                    precision,
                    scale,
                    validity: column_bytes_copy(&value.get_item(6)?, true)?,
                    offsets: column_bytes_copy(&value.get_item(7)?, true)?,
                    data: column_bytes_copy(&value.get_item(8)?, false)?.unwrap(),
                })
            })
            .collect::<PyResult<Vec<_>>>()?;

        let computed = py.detach(move || {
            let pool = source_document_pool()?;
            prepare_direct_stage_batch(layout, &owned, &cfg, Some(pool)).map_err(str::to_owned)
        });
        drop(slot);
        let outcome = computed.map_err(PyValueError::new_err)?;
        match outcome {
            DirectStageBatchResult::Fallback => Ok(None),
            DirectStageBatchResult::Error {
                category,
                code,
                row_offset,
            } => Ok(Some(
                ("error", category, code, row_offset)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind(),
            )),
            DirectStageBatchResult::Prepared {
                taken_rows,
                taken_bytes,
                stopped_by_budget,
                valid_payload_hashes,
                buckets,
            } => {
                let bucket_list = PyList::empty(py);
                for bucket in buckets {
                    bucket_list.append((
                        bucket.bucket_idx,
                        bucket.row_count,
                        PyBytes::new(py, &bucket.copy_body),
                        PyBytes::new(py, &bucket.pack_id_offsets),
                    ))?;
                }
                Ok(Some(
                    (
                        "ok",
                        taken_rows,
                        taken_bytes,
                        stopped_by_budget,
                        PyBytes::new(py, &valid_payload_hashes),
                        bucket_list,
                    )
                        .into_pyobject(py)?
                        .into_any()
                        .unbind(),
                ))
            }
        }
    }
}

#[pyfunction]
fn custom_import_direct_stage_plan_v1(
    layout: &Bound<'_, PyTuple>,
) -> PyResult<DirectStageColumnsPlan> {
    DirectStageColumnsPlan::compile(layout)
}

#[pyfunction]
fn custom_import_finalize_direct_stage_copy_v1<'py>(
    py: Python<'py>,
    chunks: &Bound<'py, PyAny>,
    pack_id: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    let pack_id = verification_integer(pack_id)?;
    let mut owned_chunks: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for entry in chunks.try_iter()? {
        let entry = entry?;
        let pair = column_exact_tuple(&entry, Some(2))?;
        let body = pair.get_item(0)?;
        let offsets = pair.get_item(1)?;
        if !body.is_exact_instance_of::<PyBytes>() || !offsets.is_exact_instance_of::<PyBytes>() {
            return Err(PyValueError::new_err(
                "direct stage COPY chunk requires bytes",
            ));
        }
        owned_chunks.push((
            body.cast::<PyBytes>()?.as_bytes().to_vec(),
            offsets.cast::<PyBytes>()?.as_bytes().to_vec(),
        ));
    }
    let assembled = py
        .detach(move || {
            let borrowed: Vec<(&[u8], &[u8])> = owned_chunks
                .iter()
                .map(|(body, offsets)| (body.as_slice(), offsets.as_slice()))
                .collect();
            finalize_direct_stage_copy(&borrowed, pack_id).map_err(str::to_owned)
        })
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &assembled))
}

#[pyfunction]
fn custom_import_pack_hash_v1<'py>(
    py: Python<'py>,
    label: &Bound<'py, PyAny>,
    hash_chunks: &Bound<'py, PyAny>,
) -> PyResult<Bound<'py, PyBytes>> {
    bounded_scalar_text(label, 128)?;
    if label.is_none() {
        return Err(PyValueError::new_err("pack hash label is required"));
    }
    let label: String = label.extract()?;
    let mut owned_chunks: Vec<Vec<u8>> = Vec::new();
    for entry in hash_chunks.try_iter()? {
        let entry = entry?;
        if !entry.is_exact_instance_of::<PyBytes>() {
            return Err(PyValueError::new_err("pack hash chunk requires bytes"));
        }
        owned_chunks.push(entry.cast::<PyBytes>()?.as_bytes().to_vec());
    }
    let digest = py
        .detach(move || {
            let borrowed: Vec<&[u8]> = owned_chunks.iter().map(Vec::as_slice).collect();
            compute_pack_hash(&label, &borrowed).map_err(str::to_owned)
        })
        .map_err(PyValueError::new_err)?;
    Ok(PyBytes::new(py, &digest))
}
