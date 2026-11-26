use crate::custom_import_v2_family::{
    encode_v2_family_batch, V2ChildInput, V2CollectionSpec, V2FamilyInput, V2FamilyOutput,
    V2FamilyPlan, V2FieldSpec, MAX_NATIVE_V2_BATCH_FAMILIES,
};

/// Compiled immutable plan for parallel compact v2 family hashing and framing.
#[pyclass(frozen, module = "ptg2_address_canon", name = "_V2FamilyEncoderPlan")]
struct V2FamilyEncoderPlan {
    plan: V2FamilyPlan,
}

fn v2_field_specs(value: &Bound<'_, PyAny>) -> PyResult<Vec<V2FieldSpec>> {
    let items = column_exact_tuple(value, None)?;
    let mut specs = Vec::with_capacity(items.len());
    for entry in items.iter() {
        let pair = column_exact_tuple(&entry, Some(3))?;
        let field_id = source_identifier(&pair.get_item(0)?)?;
        let value_type: String = pair.get_item(1)?.extract()?;
        let has_projection = column_bool(&pair.get_item(2)?)?;
        specs.push(
            V2FieldSpec::compile(&field_id, &value_type, has_projection)
                .map_err(PyValueError::new_err)?,
        );
    }
    Ok(specs)
}

fn v2_bytes32(value: &Bound<'_, PyAny>, label: &str) -> PyResult<[u8; 32]> {
    let raw: &[u8] = value
        .cast::<PyBytes>()
        .map_err(|_| PyValueError::new_err(format!("{label} must be bytes")))?
        .as_bytes();
    raw.try_into()
        .map_err(|_| PyValueError::new_err(format!("{label} must be 32 bytes")))
}

fn v2_child_inputs(value: &Bound<'_, PyAny>) -> PyResult<Vec<V2ChildInput>> {
    let list = value
        .cast::<PyList>()
        .map_err(|_| PyValueError::new_err("v2 child collection must be a list"))?;
    let mut out = Vec::with_capacity(list.len());
    for item in list.iter() {
        let tuple = column_exact_tuple(&item, Some(6))?;
        let revision_id = verification_integer(&tuple.get_item(0)?)?;
        let pack_id = verification_integer(&tuple.get_item(1)?)?;
        let source_ordinal = verification_integer(&tuple.get_item(2)?)?;
        let packed_slots = tuple
            .get_item(3)?
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("v2 child packed_slots must be bytes"))?
            .as_bytes()
            .to_vec();
        let child_key_sha256 = v2_bytes32(&tuple.get_item(4)?, "child_key_sha256")?;
        let child_key_canonical: String = tuple.get_item(5)?.extract()?;
        out.push(V2ChildInput {
            revision_id,
            pack_id,
            source_ordinal,
            packed_slots,
            child_key_sha256,
            child_key_canonical,
        });
    }
    Ok(out)
}

fn v2_family_inputs(
    families: &Bound<'_, PyList>,
    expected_collections: usize,
) -> PyResult<Vec<V2FamilyInput>> {
    if families.len() > MAX_NATIVE_V2_BATCH_FAMILIES {
        return Err(PyValueError::new_err(
            "v2 family batch exceeds maximum size",
        ));
    }
    let mut inputs = Vec::with_capacity(families.len());
    for item in families.iter() {
        let tuple = column_exact_tuple(&item, Some(7))?;
        let root_revision_id = verification_integer(&tuple.get_item(0)?)?;
        let root_pack_id = verification_integer(&tuple.get_item(1)?)?;
        let root_source_ordinal = verification_integer(&tuple.get_item(2)?)?;
        let root_packed_slots = tuple
            .get_item(3)?
            .cast::<PyBytes>()
            .map_err(|_| PyValueError::new_err("v2 root packed_slots must be bytes"))?
            .as_bytes()
            .to_vec();
        let root_key_canonical: String = tuple.get_item(4)?.extract()?;
        let root_payload_sha256 = v2_bytes32(&tuple.get_item(5)?, "root_payload_sha256")?;
        let collections_tuple =
            column_exact_tuple(&tuple.get_item(6)?, Some(expected_collections))?;
        let mut children_by_collection = Vec::with_capacity(expected_collections);
        for col_entry in collections_tuple.iter() {
            children_by_collection.push(v2_child_inputs(&col_entry)?);
        }
        inputs.push(V2FamilyInput {
            root_revision_id,
            root_pack_id,
            root_source_ordinal,
            root_packed_slots,
            root_key_canonical,
            root_payload_sha256,
            children_by_collection,
        });
    }
    Ok(inputs)
}

#[pymethods]
impl V2FamilyEncoderPlan {
    fn encode_batch<'py>(
        &self,
        py: Python<'py>,
        families: &Bound<'py, PyList>,
    ) -> PyResult<Py<PyList>> {
        let inputs = v2_family_inputs(families, self.plan.collections.len())?;
        let plan = &self.plan;
        let slot = py
            .detach(source_encoding_slot)
            .map_err(PyValueError::new_err)?;
        let computed = py.detach(move || {
            let pool = source_document_pool()?;
            encode_v2_family_batch(plan, &inputs, Some(pool)).map_err(str::to_owned)
        });
        drop(slot);
        let outputs = computed.map_err(PyValueError::new_err)?;
        let py_out = PyList::empty(py);
        for output in outputs {
            match output {
                V2FamilyOutput::Inline {
                    family_sha256,
                    storage_sha256,
                    root_scalar_count,
                } => {
                    py_out.append((
                        "inline",
                        PyBytes::new(py, &family_sha256),
                        PyBytes::new(py, &storage_sha256),
                        root_scalar_count,
                    ))?;
                }
                V2FamilyOutput::Fragmented {
                    family_sha256,
                    root_scalar_count,
                    child_scalar_count,
                    raw_bytes,
                    fragments,
                } => {
                    let py_frags = PyList::empty(py);
                    for frag in fragments {
                        py_frags.append((
                            PyBytes::new(py, &frag.raw_bytes),
                            frag.first_record,
                            frag.record_starts,
                            frag.first_offset,
                            frag.continuation,
                        ))?;
                    }
                    py_out.append((
                        "fragmented",
                        PyBytes::new(py, &family_sha256),
                        root_scalar_count,
                        child_scalar_count,
                        raw_bytes,
                        py_frags,
                    ))?;
                }
                V2FamilyOutput::Fallback => {
                    py_out.append(("fallback",))?;
                }
            }
        }
        Ok(py_out.unbind())
    }
}

#[pyfunction]
fn custom_import_v2_family_plan_v1(spec: &Bound<'_, PyTuple>) -> PyResult<V2FamilyEncoderPlan> {
    let spec = column_exact_tuple(spec.as_any(), Some(7))?;
    let definition_digest: String = spec.get_item(0)?.extract()?;
    if definition_digest.len() != 64 || !definition_digest.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err(PyValueError::new_err(
            "v2 definition digest must be 64 hex chars",
        ));
    }
    let root_fields = v2_field_specs(&spec.get_item(1)?)?;
    let raw_collections = column_exact_tuple(&spec.get_item(2)?, None)?;
    let mut collections = Vec::with_capacity(raw_collections.len());
    for entry in raw_collections.iter() {
        let pair = column_exact_tuple(&entry, Some(2))?;
        let name = source_identifier(&pair.get_item(0)?)?;
        let fields = v2_field_specs(&pair.get_item(1)?)?;
        collections.push(V2CollectionSpec::compile(name, fields).map_err(PyValueError::new_err)?);
    }
    let can_inline_root = column_bool(&spec.get_item(3)?)?;
    let fragment_bytes =
        direct_stage_usize(&spec.get_item(4)?, 64, 16 * 1024 * 1024, "fragment_bytes")?;
    let max_record_bytes = direct_stage_usize(
        &spec.get_item(5)?,
        64,
        512 * 1024 * 1024,
        "max_record_bytes",
    )?;
    let max_spool_bytes = direct_stage_usize(
        &spec.get_item(6)?,
        64,
        1024 * 1024 * 1024,
        "max_spool_bytes",
    )?;
    Ok(V2FamilyEncoderPlan {
        plan: V2FamilyPlan {
            definition_digest,
            root_fields,
            collections,
            can_inline_root,
            fragment_bytes,
            max_record_bytes,
            max_spool_bytes,
        },
    })
}
