//! Immutable-byte Python boundary for selected retained graph codec checks.
//! The caller independently authenticates snapshot roots, producer scope and capture custody.

use crate::registry_ptg_graph_witness as graph;
use serde::{de, Deserialize, Serialize};
use std::fmt;

pub const MAX_METADATA_BYTES: usize = 16 * 1024 * 1024;
pub const MAX_LOCATOR_METADATA_BYTES: usize = 16 * 1024;
const MAX_OUTPUT_BYTES: usize = 4 * 1024 * 1024;
type Result<T> = std::result::Result<T, &'static str>;

#[derive(Clone, Copy)]
struct HexHash([u8; 32]);

impl<'de> Deserialize<'de> for HexHash {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor;
        impl de::Visitor<'_> for Visitor {
            type Value = HexHash;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("64 lowercase hexadecimal characters")
            }
            fn visit_str<E: de::Error>(self, value: &str) -> std::result::Result<HexHash, E> {
                if value.len() != 64
                    || !value
                        .bytes()
                        .all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f'))
                {
                    return Err(E::custom("invalid hash"));
                }
                let digit = |byte: u8| {
                    if byte <= b'9' {
                        byte - b'0'
                    } else {
                        byte - b'a' + 10
                    }
                };
                Ok(HexHash(std::array::from_fn(|index| {
                    digit(value.as_bytes()[index * 2]) * 16 + digit(value.as_bytes()[index * 2 + 1])
                })))
            }
        }
        deserializer.deserialize_str(Visitor)
    }
}

impl Serialize for HexHash {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        let mut encoded = [0u8; 64];
        for (index, byte) in self.0.iter().enumerate() {
            encoded[index * 2] = b"0123456789abcdef"[(byte >> 4) as usize];
            encoded[index * 2 + 1] = b"0123456789abcdef"[(byte & 15) as usize];
        }
        serializer.serialize_str(std::str::from_utf8(&encoded).expect("hex is ASCII"))
    }
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Identity {
    snapshot_key: u64,
    layout_generation: String,
    layout_mapping_sha256: HexHash,
    map_sha256: HexHash,
    finalizer_map_sha256: HexHash,
    source_assignments_sha256: HexHash,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Context {
    graph_identity: Identity,
    after_ordinal: u64,
    last_ordinal: u64,
    row_count: u32,
}

impl Context {
    fn native(&self) -> graph::BatchContext {
        let identity = &self.graph_identity;
        graph::BatchContext {
            graph_identity: graph::GraphIdentity {
                snapshot_key: identity.snapshot_key,
                layout_generation: identity.layout_generation.clone(),
                layout_mapping_sha256: identity.layout_mapping_sha256.0,
                map_sha256: identity.map_sha256.0,
                finalizer_map_sha256: identity.finalizer_map_sha256.0,
                source_assignments_sha256: identity.source_assignments_sha256.0,
            },
            after_ordinal: self.after_ordinal,
            last_ordinal: self.last_ordinal,
            row_count: self.row_count,
        }
    }
}

// This pass counts metadata rows without allocating their Strings or Vecs.
struct Control;
impl<'de> Deserialize<'de> for Control {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        let raw: &serde_json::value::RawValue = Deserialize::deserialize(deserializer)?;
        if raw.get().len() > MAX_LOCATOR_METADATA_BYTES {
            return Err(de::Error::custom("context byte limit"));
        }
        Ok(Control)
    }
}

struct Count<const MAX: usize>;
impl<'de, const MAX: usize> Deserialize<'de> for Count<MAX> {
    fn deserialize<D: de::Deserializer<'de>>(
        deserializer: D,
    ) -> std::result::Result<Self, D::Error> {
        struct Visitor<const MAX: usize>;
        impl<'de, const MAX: usize> de::Visitor<'de> for Visitor<MAX> {
            type Value = Count<MAX>;
            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("bounded metadata array")
            }
            fn visit_seq<A: de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> std::result::Result<Self::Value, A::Error> {
                let mut count = 0;
                while sequence.next_element::<de::IgnoredAny>()?.is_some() {
                    count += 1;
                    if count > MAX {
                        return Err(de::Error::custom("metadata count limit"));
                    }
                }
                Ok(Count)
            }
        }
        deserializer.deserialize_seq(Visitor::<MAX>)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LocatorMetadata {
    context: Context,
    manifest: graph::RelationManifest,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CasMetadata {
    block_hash: HexHash,
    format_version: i16,
    object_kind: String,
    codec: String,
    entry_count: u64,
    raw_byte_count: u64,
    stored_byte_count: u64,
    payload_index: usize,
}

impl CasMetadata {
    fn native<'a>(&'a self, payloads: &'a [&[u8]]) -> graph::CasBlock<'a> {
        graph::CasBlock {
            block_hash: self.block_hash.0,
            format_version: self.format_version,
            object_kind: &self.object_kind,
            codec: &self.codec,
            entry_count: self.entry_count,
            raw_byte_count: self.raw_byte_count,
            stored_byte_count: self.stored_byte_count,
            payload: payloads[self.payload_index],
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PackMetadata {
    snapshot_key: u64,
    object_kind: String,
    pack_no: u32,
    first_block_key: u64,
    first_fragment_no: u32,
    last_block_key: u64,
    last_fragment_no: u32,
    coordinate_count: u32,
    pack_entry_count: u64,
    block: CasMetadata,
}

impl PackMetadata {
    fn native<'a>(&'a self, payloads: &'a [&[u8]]) -> graph::MapPack<'a> {
        graph::MapPack {
            snapshot_key: self.snapshot_key,
            object_kind: &self.object_kind,
            pack_no: self.pack_no,
            first_block_key: self.first_block_key,
            first_fragment_no: self.first_fragment_no,
            last_block_key: self.last_block_key,
            last_fragment_no: self.last_fragment_no,
            coordinate_count: self.coordinate_count,
            pack_entry_count: self.pack_entry_count,
            block: self.block.native(payloads),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchMetadata<C, M, P, B, H> {
    context: C,
    manifest: M,
    map_packs: P,
    blocks: B,
    heavy: H,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VerifyMetadata<C, M, P, B, H> {
    expected_context: C,
    actual_context: C,
    manifest: M,
    map_packs: P,
    blocks: B,
    heavy: H,
}

type Batch = BatchMetadata<
    Context,
    graph::RelationManifest,
    Vec<PackMetadata>,
    Vec<CasMetadata>,
    Vec<graph::HeavyOwner>,
>;
type Verification = VerifyMetadata<
    Context,
    graph::RelationManifest,
    Vec<PackMetadata>,
    Vec<CasMetadata>,
    Vec<graph::HeavyOwner>,
>;
type PreflightBatch = BatchMetadata<
    Control,
    de::IgnoredAny,
    Count<{ graph::MAX_GRAPH_PAGES }>,
    Count<{ graph::MAX_GRAPH_PAGES }>,
    Count<{ graph::MAX_SELECTED_EDGES }>,
>;
type PreflightVerify = VerifyMetadata<
    Control,
    de::IgnoredAny,
    Count<{ graph::MAX_GRAPH_PAGES }>,
    Count<{ graph::MAX_GRAPH_PAGES }>,
    Count<{ graph::MAX_SELECTED_EDGES }>,
>;

fn parse<'a, T: Deserialize<'a>>(metadata: &'a [u8]) -> Result<T> {
    serde_json::from_slice(metadata).map_err(|_| "registry_ptg_graph_metadata_invalid")
}

fn bounds(metadata: &[u8], selected: &[u8], payloads: &[&[u8]], cap: usize) -> Result<()> {
    if metadata.len() > cap
        || selected.len() > graph::MAX_SELECTED_EDGES * 8
        || payloads.len() > graph::MAX_GRAPH_PAGES * 2
    {
        return Err("registry_ptg_graph_budget");
    }
    let mut raw_bytes = 0usize;
    for payload in payloads {
        raw_bytes = raw_bytes
            .checked_add(payload.len())
            .ok_or("registry_ptg_graph_budget")?;
        if payload.len() > graph::MAX_PAGE_BYTES || raw_bytes > graph::MAX_BATCH_BYTES {
            return Err("registry_ptg_graph_budget");
        }
    }
    Ok(())
}

fn mapped_inputs<'a>(
    packs: &'a [PackMetadata],
    blocks: &'a [CasMetadata],
    payloads: &'a [&[u8]],
) -> Result<(Vec<graph::MapPack<'a>>, Vec<graph::CasBlock<'a>>)> {
    if packs.len() + blocks.len() != payloads.len() {
        return Err("registry_ptg_graph_payload_mapping_invalid");
    }
    let frames = packs.iter().map(|pack| &pack.block).chain(blocks);
    for (index, frame) in frames.enumerate() {
        if frame.payload_index != index
            || frame.raw_byte_count != payloads[index].len() as u64
            || frame.stored_byte_count != frame.raw_byte_count
        {
            return Err("registry_ptg_graph_payload_mapping_invalid");
        }
    }
    Ok((
        packs.iter().map(|pack| pack.native(payloads)).collect(),
        blocks.iter().map(|block| block.native(payloads)).collect(),
    ))
}

fn output<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let mut value = serde_json::to_value(value).map_err(|_| "registry_ptg_graph_output_invalid")?;
    value.sort_all_objects();
    let encoded = serde_json::to_vec(&value).map_err(|_| "registry_ptg_graph_output_invalid")?;
    if encoded.len() > MAX_OUTPUT_BYTES {
        return Err("registry_ptg_graph_budget");
    }
    Ok(encoded)
}

pub fn plan_locator(metadata: &[u8], selected: &[u8]) -> Result<Vec<u8>> {
    bounds(metadata, selected, &[], MAX_LOCATOR_METADATA_BYTES)?;
    let input: LocatorMetadata = parse(metadata)?;
    output(&graph::plan_group_npi_locator_pages(
        &input.context.native(),
        &input.manifest,
        selected,
    )?)
}

pub fn plan_members(metadata: &[u8], selected: &[u8], payloads: &[&[u8]]) -> Result<Vec<u8>> {
    bounds(metadata, selected, payloads, MAX_METADATA_BYTES)?;
    let _: PreflightBatch = parse(metadata)?;
    let input: Batch = parse(metadata)?;
    let (packs, blocks) = mapped_inputs(&input.map_packs, &input.blocks, payloads)?;
    output(&graph::plan_group_npi_member_pages(
        &input.context.native(),
        &input.manifest,
        selected,
        &packs,
        &blocks,
        &input.heavy,
    )?)
}

#[derive(Serialize)]
struct Proof<'a> {
    contract: &'static str,
    context: &'a Context,
    selected_edges_sha256: HexHash,
    edge_count: u32,
    verified_edge_count: u32,
    missing_edge_count: u32,
    selected_owner_count: u32,
    map_pack_count: u32,
    authenticated_graph_page_count: u32,
    authenticated_raw_bytes: u64,
    decoded_bytes: u64,
    checked_member_count: u64,
}

pub fn verify(metadata: &[u8], selected: &[u8], payloads: &[&[u8]]) -> Result<Vec<u8>> {
    bounds(metadata, selected, payloads, MAX_METADATA_BYTES)?;
    let _: PreflightVerify = parse(metadata)?;
    let input: Verification = parse(metadata)?;
    let (packs, blocks) = mapped_inputs(&input.map_packs, &input.blocks, payloads)?;
    let proof = graph::verify_group_npi_batch(
        &input.expected_context.native(),
        &input.actual_context.native(),
        &input.manifest,
        selected,
        &packs,
        &blocks,
        &input.heavy,
    )?;
    output(&Proof {
        contract: proof.contract,
        context: &input.actual_context,
        selected_edges_sha256: HexHash(proof.selected_edges_sha256),
        edge_count: proof.edge_count,
        verified_edge_count: proof.verified_edge_count,
        missing_edge_count: proof.missing_edge_count,
        selected_owner_count: proof.selected_owner_count,
        map_pack_count: proof.map_pack_count,
        authenticated_graph_page_count: proof.authenticated_graph_page_count,
        authenticated_raw_bytes: proof.authenticated_raw_bytes,
        decoded_bytes: proof.decoded_bytes,
        checked_member_count: proof.checked_member_count,
    })
}

#[cfg(feature = "python")]
mod python {
    use super::*;
    use pyo3::{
        exceptions::PyValueError,
        prelude::*,
        types::{PyBytes, PyTuple},
    };

    type Operation = fn(&[u8], &[u8], &[&[u8]]) -> Result<Vec<u8>>;

    fn run(
        py: Python<'_>,
        metadata: &Bound<'_, PyBytes>,
        selected: &Bound<'_, PyBytes>,
        payloads: &Bound<'_, PyTuple>,
        operation: Operation,
    ) -> PyResult<Py<PyBytes>> {
        if metadata.as_bytes().len() > MAX_METADATA_BYTES
            || selected.as_bytes().len() > graph::MAX_SELECTED_EDGES * 8
            || payloads.len() > graph::MAX_GRAPH_PAGES * 2
        {
            return Err(PyValueError::new_err("registry_ptg_graph_budget"));
        }
        let mut total = 0usize;
        for item in payloads.iter() {
            let bytes = item.cast::<PyBytes>()?;
            total = total
                .checked_add(bytes.as_bytes().len())
                .ok_or_else(|| PyValueError::new_err("registry_ptg_graph_budget"))?;
            if bytes.as_bytes().len() > graph::MAX_PAGE_BYTES || total > graph::MAX_BATCH_BYTES {
                return Err(PyValueError::new_err("registry_ptg_graph_budget"));
            }
        }
        let buffers: Vec<_> = payloads
            .iter()
            .map(|item| item.cast_into::<PyBytes>().map_err(Into::into))
            .collect::<PyResult<_>>()?;
        let borrowed: Vec<_> = buffers.iter().map(|buffer| buffer.as_bytes()).collect();
        let metadata = metadata.as_bytes();
        let selected = selected.as_bytes();
        let encoded = py
            .detach(|| operation(metadata, selected, &borrowed))
            .map_err(PyValueError::new_err)?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction(name = "plan_registry_ptg_graph_locator_pages")]
    pub fn locator(
        py: Python<'_>,
        metadata: &Bound<'_, PyBytes>,
        selected_edges: &Bound<'_, PyBytes>,
    ) -> PyResult<Py<PyBytes>> {
        let metadata = metadata.as_bytes();
        let selected = selected_edges.as_bytes();
        let encoded = py
            .detach(|| plan_locator(metadata, selected))
            .map_err(PyValueError::new_err)?;
        Ok(PyBytes::new(py, &encoded).unbind())
    }

    #[pyfunction(name = "plan_registry_ptg_graph_member_pages")]
    pub fn members(
        py: Python<'_>,
        metadata: &Bound<'_, PyBytes>,
        selected_edges: &Bound<'_, PyBytes>,
        payloads: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyBytes>> {
        run(py, metadata, selected_edges, payloads, plan_members)
    }

    #[pyfunction(name = "verify_registry_ptg_graph_batch")]
    pub fn verification(
        py: Python<'_>,
        metadata: &Bound<'_, PyBytes>,
        selected_edges: &Bound<'_, PyBytes>,
        payloads: &Bound<'_, PyTuple>,
    ) -> PyResult<Py<PyBytes>> {
        run(py, metadata, selected_edges, payloads, verify)
    }
}

#[cfg(feature = "python")]
pub use python::{locator, members, verification};
