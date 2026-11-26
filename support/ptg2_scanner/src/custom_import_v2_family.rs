//! Parallel compact v2 family semantic hashing and fragment framing.
//!
//! Reads already-validated MessagePack positional v2 slot buffers directly from
//! direct-stage buckets, streams exact canonical ``custom-import-record/v1``
//! JSON into the ``custom-import-family/v1`` SHA-256 commitment, and emits
//! length-prefixed compact v2 wire fragments ready for Zstd compression.

use rayon::{prelude::*, ThreadPool};
use sha2::{Digest, Sha256};

const COMPACT_V2_CONTRACT: &str = "custom-import/family-block/v2";
const FAMILY_HASH_PREFIX: &[u8] = b"custom-import/v1\0candidate-runner/1\0family\0";
const ROOT_PAYLOAD_HASH_PREFIX: &[u8] = b"custom-import/v1\0candidate-runner/1\0root-payload\0";
const INLINE_STORAGE_DOMAIN: &[u8] = b"custom-import/family-block/v2\0inline\0";
const PARALLEL_FAMILY_THRESHOLD: usize = 16;
pub const MAX_NATIVE_V2_FAMILY_RAW_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_NATIVE_V2_BATCH_FAMILIES: usize = 65_536;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum V2FieldKind {
    String,
    Integer,
    Boolean,
    Decimal,
    Date,
    Timestamp,
}

impl V2FieldKind {
    pub fn parse(value_type: &str) -> Result<Self, &'static str> {
        match value_type {
            "string" => Ok(Self::String),
            "integer" => Ok(Self::Integer),
            "boolean" => Ok(Self::Boolean),
            "decimal" => Ok(Self::Decimal),
            "date" => Ok(Self::Date),
            "timestamp" => Ok(Self::Timestamp),
            _ => Err("unsupported v2 field value type"),
        }
    }
}

pub struct V2FieldSpec {
    pub kind: V2FieldKind,
    pub has_projection_slot: bool,
    pub missing_json: String,
    pub null_json: String,
    pub value_prefix_json: String,
}

impl V2FieldSpec {
    pub fn compile(
        field_id: &str,
        value_type: &str,
        has_projection_slot: bool,
    ) -> Result<Self, &'static str> {
        let kind = V2FieldKind::parse(value_type)?;
        let quoted_id =
            serde_json::to_string(field_id).map_err(|_| "v2 field identifier encoding failed")?;
        Ok(Self {
            kind,
            has_projection_slot,
            missing_json: format!("{{\"field\":{quoted_id},\"value\":{{\"state\":\"missing\"}}}}"),
            null_json: format!(
                "{{\"field\":{quoted_id},\"value\":{{\"state\":\"null\",\"type\":\"{value_type}\"}}}}"
            ),
            value_prefix_json: format!(
                "{{\"field\":{quoted_id},\"value\":{{\"state\":\"value\",\"type\":\"{value_type}\",\"value\":"
            ),
        })
    }
}

pub struct V2CollectionSpec {
    pub name: String,
    pub name_json: String,
    pub fields: Vec<V2FieldSpec>,
}

impl V2CollectionSpec {
    pub fn compile(name: String, fields: Vec<V2FieldSpec>) -> Result<Self, &'static str> {
        let name_json =
            serde_json::to_string(&name).map_err(|_| "v2 collection name encoding failed")?;
        Ok(Self {
            name,
            name_json,
            fields,
        })
    }
}

pub struct V2FamilyPlan {
    pub definition_digest: String,
    pub root_fields: Vec<V2FieldSpec>,
    pub collections: Vec<V2CollectionSpec>,
    pub can_inline_root: bool,
    pub fragment_bytes: usize,
    pub max_record_bytes: usize,
    pub max_spool_bytes: usize,
}

pub struct V2ChildInput {
    pub revision_id: i64,
    pub pack_id: i64,
    pub source_ordinal: i64,
    pub packed_slots: Vec<u8>,
    pub child_key_sha256: [u8; 32],
    pub child_key_canonical: String,
}

pub struct V2FamilyInput {
    pub root_revision_id: i64,
    pub root_pack_id: i64,
    pub root_source_ordinal: i64,
    pub root_packed_slots: Vec<u8>,
    pub root_key_canonical: String,
    pub root_payload_sha256: [u8; 32],
    pub children_by_collection: Vec<Vec<V2ChildInput>>,
}

pub struct V2RawFragment {
    pub raw_bytes: Vec<u8>,
    pub first_record: i64,
    pub record_starts: i64,
    pub first_offset: i64,
    pub continuation: i64,
}

pub enum V2FamilyOutput {
    Inline {
        family_sha256: [u8; 32],
        storage_sha256: [u8; 32],
        root_scalar_count: usize,
    },
    Fragmented {
        family_sha256: [u8; 32],
        root_scalar_count: usize,
        child_scalar_count: usize,
        raw_bytes: usize,
        fragments: Vec<V2RawFragment>,
    },
    Fallback,
}

fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8, &'static str> {
    let byte = *buf.get(*pos).ok_or("msgpack slot buffer truncated")?;
    *pos += 1;
    Ok(byte)
}

fn read_exact<'a>(buf: &'a [u8], pos: &mut usize, len: usize) -> Result<&'a [u8], &'static str> {
    let end = pos
        .checked_add(len)
        .ok_or("msgpack slot buffer length overflow")?;
    let slice = buf.get(*pos..end).ok_or("msgpack slot buffer truncated")?;
    *pos = end;
    Ok(slice)
}

fn read_array_len(buf: &[u8], pos: &mut usize) -> Result<usize, &'static str> {
    let tag = read_u8(buf, pos)?;
    match tag {
        0x90..=0x9f => Ok((tag & 0x0f) as usize),
        0xdc => {
            let raw = read_exact(buf, pos, 2)?;
            Ok(u16::from_be_bytes([raw[0], raw[1]]) as usize)
        }
        0xdd => {
            let raw = read_exact(buf, pos, 4)?;
            Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize)
        }
        _ => Err("v2 slot buffer is not a msgpack array"),
    }
}

fn read_msgpack_str<'a>(buf: &'a [u8], pos: &mut usize, tag: u8) -> Result<&'a str, &'static str> {
    let len = match tag {
        0xa0..=0xbf => (tag & 0x1f) as usize,
        0xd9 => read_u8(buf, pos)? as usize,
        0xda => {
            let raw = read_exact(buf, pos, 2)?;
            u16::from_be_bytes([raw[0], raw[1]]) as usize
        }
        0xdb => {
            let raw = read_exact(buf, pos, 4)?;
            u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as usize
        }
        _ => return Err("v2 string slot is malformed"),
    };
    let bytes = read_exact(buf, pos, len)?;
    std::str::from_utf8(bytes).map_err(|_| "v2 string slot is not valid UTF-8")
}

fn read_msgpack_i64(buf: &[u8], pos: &mut usize, tag: u8) -> Result<i64, &'static str> {
    match tag {
        0x00..=0x7f => Ok(tag as i64),
        0xe0..=0xff => Ok((tag as i8) as i64),
        0xcc => Ok(read_u8(buf, pos)? as i64),
        0xcd => {
            let raw = read_exact(buf, pos, 2)?;
            Ok(u16::from_be_bytes([raw[0], raw[1]]) as i64)
        }
        0xce => {
            let raw = read_exact(buf, pos, 4)?;
            Ok(u32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as i64)
        }
        0xcf => {
            let raw = read_exact(buf, pos, 8)?;
            let unsigned = u64::from_be_bytes(raw.try_into().unwrap());
            i64::try_from(unsigned).map_err(|_| "v2 integer slot exceeds i64")
        }
        0xd0 => Ok((read_u8(buf, pos)? as i8) as i64),
        0xd1 => {
            let raw = read_exact(buf, pos, 2)?;
            Ok(i16::from_be_bytes([raw[0], raw[1]]) as i64)
        }
        0xd2 => {
            let raw = read_exact(buf, pos, 4)?;
            Ok(i32::from_be_bytes([raw[0], raw[1], raw[2], raw[3]]) as i64)
        }
        0xd3 => {
            let raw = read_exact(buf, pos, 8)?;
            Ok(i64::from_be_bytes(raw.try_into().unwrap()))
        }
        _ => Err("v2 integer slot is malformed"),
    }
}

fn push_json_escaped_str(out: &mut String, text: &str) {
    out.push('"');
    let mut start = 0;
    let bytes = text.as_bytes();
    for (idx, &byte) in bytes.iter().enumerate() {
        let replacement = match byte {
            b'"' => Some("\\\""),
            b'\\' => Some("\\\\"),
            0x08 => Some("\\b"),
            0x0c => Some("\\f"),
            b'\n' => Some("\\n"),
            b'\r' => Some("\\r"),
            b'\t' => Some("\\t"),
            0x00..=0x1f => None,
            _ => continue,
        };
        if idx > start {
            out.push_str(&text[start..idx]);
        }
        if let Some(esc) = replacement {
            out.push_str(esc);
        } else {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            out.push_str("\\u00");
            out.push(HEX[(byte >> 4) as usize] as char);
            out.push(HEX[(byte & 0x0f) as usize] as char);
        }
        start = idx + 1;
    }
    if start < text.len() {
        out.push_str(&text[start..]);
    }
    out.push('"');
}

fn hash_json_escaped_str(hasher: &mut Sha256, text: &str) {
    hasher.update(b"\"");
    let mut start = 0;
    let bytes = text.as_bytes();
    for (idx, &byte) in bytes.iter().enumerate() {
        let replacement: Option<&[u8]> = match byte {
            b'"' => Some(b"\\\""),
            b'\\' => Some(b"\\\\"),
            0x08 => Some(b"\\b"),
            0x0c => Some(b"\\f"),
            b'\n' => Some(b"\\n"),
            b'\r' => Some(b"\\r"),
            b'\t' => Some(b"\\t"),
            0x00..=0x1f => None,
            _ => continue,
        };
        if idx > start {
            hasher.update(&bytes[start..idx]);
        }
        if let Some(esc) = replacement {
            hasher.update(esc);
        } else {
            const HEX: &[u8; 16] = b"0123456789abcdef";
            let esc = [
                b'\\',
                b'u',
                b'0',
                b'0',
                HEX[(byte >> 4) as usize],
                HEX[(byte & 0x0f) as usize],
            ];
            hasher.update(esc);
        }
        start = idx + 1;
    }
    if start < bytes.len() {
        hasher.update(&bytes[start..]);
    }
    hasher.update(b"\"");
}

/// Validate a packed positional v2 slot vector and emit canonical ``custom-import-record/v1`` JSON.
pub fn slots_to_canonical_json(
    fields: &[V2FieldSpec],
    packed_slots: &[u8],
    out: &mut String,
) -> Result<usize, &'static str> {
    let mut pos = 0;
    let slot_len = read_array_len(packed_slots, &mut pos)?;
    if slot_len != fields.len() {
        return Err("v2 slot vector shape differs");
    }
    out.clear();
    out.push_str("{\"contract\":\"custom-import-record/v1\",\"fields\":[");
    let mut scalar_count = 0usize;
    let mut int_buf = itoa::Buffer::new();
    for (idx, field) in fields.iter().enumerate() {
        if idx > 0 {
            out.push(',');
        }
        let tag = read_u8(packed_slots, &mut pos)?;
        if tag == 0x90 {
            out.push_str(&field.missing_json);
            continue;
        }
        if tag == 0xc0 {
            out.push_str(&field.null_json);
            if field.has_projection_slot {
                scalar_count += 1;
            }
            continue;
        }
        if field.has_projection_slot {
            scalar_count += 1;
        }
        out.push_str(&field.value_prefix_json);
        match field.kind {
            V2FieldKind::String => {
                let text = read_msgpack_str(packed_slots, &mut pos, tag)?;
                push_json_escaped_str(out, text);
            }
            V2FieldKind::Integer => {
                let num = read_msgpack_i64(packed_slots, &mut pos, tag)?;
                out.push_str(int_buf.format(num));
            }
            V2FieldKind::Boolean => match tag {
                0xc2 => out.push_str("false"),
                0xc3 => out.push_str("true"),
                _ => return Err("v2 boolean slot is malformed"),
            },
            V2FieldKind::Decimal | V2FieldKind::Date | V2FieldKind::Timestamp => {
                let text = read_msgpack_str(packed_slots, &mut pos, tag)?;
                if text.bytes().any(|b| b < 0x20 || b == b'"' || b == b'\\') {
                    return Err("v2 canonical temporal/decimal slot contains invalid characters");
                }
                out.push('"');
                out.push_str(text);
                out.push('"');
            }
        }
        out.push_str("}}");
    }
    if pos != packed_slots.len() {
        return Err("v2 slot buffer contains trailing bytes");
    }
    out.push_str("]}");
    Ok(scalar_count)
}

fn write_msgpack_str(out: &mut Vec<u8>, text: &str) {
    let bytes = text.as_bytes();
    let len = bytes.len();
    if len <= 31 {
        out.push(0xa0 | (len as u8));
    } else if len <= u8::MAX as usize {
        out.push(0xd9);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xda);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xdb);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_msgpack_bin(out: &mut Vec<u8>, bytes: &[u8]) {
    let len = bytes.len();
    if len <= u8::MAX as usize {
        out.push(0xc4);
        out.push(len as u8);
    } else if len <= u16::MAX as usize {
        out.push(0xc5);
        out.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
        out.push(0xc6);
        out.extend_from_slice(&(len as u32).to_be_bytes());
    }
    out.extend_from_slice(bytes);
}

fn write_msgpack_i64(out: &mut Vec<u8>, value: i64) {
    if value >= 0 {
        if value <= 127 {
            out.push(value as u8);
        } else if value <= u8::MAX as i64 {
            out.push(0xcc);
            out.push(value as u8);
        } else if value <= u16::MAX as i64 {
            out.push(0xcd);
            out.extend_from_slice(&(value as u16).to_be_bytes());
        } else if value <= u32::MAX as i64 {
            out.push(0xce);
            out.extend_from_slice(&(value as u32).to_be_bytes());
        } else {
            out.push(0xcf);
            out.extend_from_slice(&(value as u64).to_be_bytes());
        }
    } else if value >= -32 {
        out.push((value as i8) as u8);
    } else if value >= i8::MIN as i64 {
        out.push(0xd0);
        out.push((value as i8) as u8);
    } else if value >= i16::MIN as i64 {
        out.push(0xd1);
        out.extend_from_slice(&(value as i16).to_be_bytes());
    } else if value >= i32::MIN as i64 {
        out.push(0xd2);
        out.extend_from_slice(&(value as i32).to_be_bytes());
    } else {
        out.push(0xd3);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

fn inline_storage_sha256(
    definition_digest: &str,
    family_sha256: &[u8; 32],
    root_payload_sha256: &[u8; 32],
    root_revision_id: i64,
) -> [u8; 32] {
    let mut payload = Vec::with_capacity(160);
    payload.push(0x94);
    write_msgpack_str(&mut payload, definition_digest);
    write_msgpack_bin(&mut payload, family_sha256);
    write_msgpack_bin(&mut payload, root_payload_sha256);
    write_msgpack_i64(&mut payload, root_revision_id);
    let mut hasher = Sha256::new();
    hasher.update(INLINE_STORAGE_DOMAIN);
    hasher.update(&payload);
    hasher.finalize().into()
}

struct FragmentSlicer {
    fragment_bytes: usize,
    max_record_bytes: usize,
    max_raw_bytes: usize,
    pending: Vec<u8>,
    fragments: Vec<V2RawFragment>,
    raw_bytes: usize,
    next_record: i64,
    first_record: i64,
    first_offset: i64,
    record_starts: i64,
    continuation: i64,
    exceeded_native_cap: bool,
}

impl FragmentSlicer {
    fn new(fragment_bytes: usize, max_record_bytes: usize, max_raw_bytes: usize) -> Self {
        Self {
            fragment_bytes,
            max_record_bytes,
            max_raw_bytes,
            pending: Vec::with_capacity(fragment_bytes.min(8192)),
            fragments: Vec::new(),
            raw_bytes: 0,
            next_record: 0,
            first_record: -1,
            first_offset: -1,
            record_starts: 0,
            continuation: 0,
            exceeded_native_cap: false,
        }
    }

    fn write_framed(&mut self, packed: &[u8], is_record: bool) -> Result<(), &'static str> {
        if packed.len() > self.max_record_bytes {
            return Err("family record exceeds the caller's resource budget");
        }
        let added = 8usize
            .checked_add(packed.len())
            .ok_or("family stream length overflow")?;
        if self.raw_bytes + self.pending.len() + added > self.max_raw_bytes {
            self.exceeded_native_cap = true;
            return Ok(());
        }
        if is_record {
            if self.record_starts == 0 {
                self.first_record = self.next_record;
                self.first_offset = self.pending.len() as i64;
            }
            self.record_starts += 1;
            self.next_record += 1;
        }
        let len_prefix = (packed.len() as u64).to_be_bytes();
        let mut remaining = added;
        for chunk in [&len_prefix[..], packed] {
            let mut pos = 0;
            while pos < chunk.len() {
                let space = self.fragment_bytes - self.pending.len();
                let count = space.min(chunk.len() - pos);
                self.pending.extend_from_slice(&chunk[pos..pos + count]);
                pos += count;
                remaining -= count;
                if self.pending.len() == self.fragment_bytes {
                    self.flush();
                    self.continuation = self.fragment_bytes.min(remaining) as i64;
                }
            }
        }
        Ok(())
    }

    fn flush(&mut self) {
        if self.pending.is_empty() {
            return;
        }
        self.raw_bytes += self.pending.len();
        let raw_bytes = std::mem::take(&mut self.pending);
        self.fragments.push(V2RawFragment {
            raw_bytes,
            first_record: self.first_record,
            record_starts: self.record_starts,
            first_offset: self.first_offset,
            continuation: self.continuation,
        });
        self.first_record = -1;
        self.first_offset = -1;
        self.record_starts = 0;
        self.continuation = 0;
    }
}

fn compute_family_hash_and_counts(
    plan: &V2FamilyPlan,
    family: &V2FamilyInput,
) -> Result<([u8; 32], usize, usize, usize), &'static str> {
    if family.children_by_collection.len() != plan.collections.len() {
        return Err("v2 family collections differ from plan");
    }
    let mut root_payload = String::with_capacity(256);
    let root_scalar_count = slots_to_canonical_json(
        &plan.root_fields,
        &family.root_packed_slots,
        &mut root_payload,
    )?;

    let mut root_hasher = Sha256::new();
    root_hasher.update(ROOT_PAYLOAD_HASH_PREFIX);
    root_hasher.update(root_payload.as_bytes());
    let actual_root_hash: [u8; 32] = root_hasher.finalize().into();
    if actual_root_hash != family.root_payload_sha256 {
        return Err("v2 root payload hash differs from staged payload digest");
    }

    let mut family_hasher = Sha256::new();
    family_hasher.update(FAMILY_HASH_PREFIX);
    family_hasher.update(b"{\"children\":{");

    let mut child_payload = String::with_capacity(256);
    let mut prev_payload = String::new();
    let mut child_scalar_count = 0usize;
    let mut total_child_count = 0usize;

    for (col_idx, (col_spec, children)) in plan
        .collections
        .iter()
        .zip(family.children_by_collection.iter())
        .enumerate()
    {
        if col_idx > 0 {
            family_hasher.update(b",");
        }
        family_hasher.update(col_spec.name_json.as_bytes());
        family_hasher.update(b":[");
        let mut prev_key: Option<(&[u8; 32], &str)> = None;
        prev_payload.clear();
        for (child_idx, child) in children.iter().enumerate() {
            child_scalar_count +=
                slots_to_canonical_json(&col_spec.fields, &child.packed_slots, &mut child_payload)?;
            if let Some((prev_hash, prev_canonical)) = prev_key {
                let cmp = prev_hash
                    .cmp(&child.child_key_sha256)
                    .then_with(|| prev_canonical.cmp(child.child_key_canonical.as_str()))
                    .then_with(|| prev_payload.as_str().cmp(child_payload.as_str()));
                if cmp.is_gt() {
                    return Err("family child documents are not ordered");
                }
            }
            if child_idx > 0 {
                family_hasher.update(b",");
            }
            family_hasher.update(b"{\"key\":");
            hash_json_escaped_str(&mut family_hasher, &child.child_key_canonical);
            family_hasher.update(b",\"payload\":");
            hash_json_escaped_str(&mut family_hasher, &child_payload);
            family_hasher.update(b"}");
            prev_key = Some((&child.child_key_sha256, &child.child_key_canonical));
            std::mem::swap(&mut prev_payload, &mut child_payload);
        }
        total_child_count += children.len();
        family_hasher.update(b"]");
    }

    family_hasher.update(b"},\"contract\":\"custom-import-family/v1\",\"root_key\":");
    hash_json_escaped_str(&mut family_hasher, &family.root_key_canonical);
    family_hasher.update(b",\"root_payload\":");
    hash_json_escaped_str(&mut family_hasher, &root_payload);
    family_hasher.update(b"}");
    Ok((
        family_hasher.finalize().into(),
        root_scalar_count,
        child_scalar_count,
        total_child_count,
    ))
}

fn encode_single_family(
    plan: &V2FamilyPlan,
    family: &V2FamilyInput,
) -> Result<V2FamilyOutput, &'static str> {
    let (family_sha256, root_scalar_count, child_scalar_count, total_child_count) =
        compute_family_hash_and_counts(plan, family)?;

    if total_child_count == 0 && plan.can_inline_root {
        let storage_sha256 = inline_storage_sha256(
            &plan.definition_digest,
            &family_sha256,
            &family.root_payload_sha256,
            family.root_revision_id,
        );
        return Ok(V2FamilyOutput::Inline {
            family_sha256,
            storage_sha256,
            root_scalar_count,
        });
    }

    let max_raw = plan.max_spool_bytes.min(MAX_NATIVE_V2_FAMILY_RAW_BYTES);
    let mut slicer = FragmentSlicer::new(plan.fragment_bytes, plan.max_record_bytes, max_raw);
    let mut scratch = Vec::with_capacity(512);

    // Header: ["custom-import/family-block/v2", definition_digest, bin32(family_sha256), false]
    scratch.push(0x94);
    write_msgpack_str(&mut scratch, COMPACT_V2_CONTRACT);
    write_msgpack_str(&mut scratch, &plan.definition_digest);
    write_msgpack_bin(&mut scratch, &family_sha256);
    scratch.push(0xc2); // retained = false
    slicer.write_framed(&scratch, false)?;

    // Root record: [root_rev_id, root_pack_id, root_source_ordinal, <packed_slots>]
    scratch.clear();
    scratch.push(0x94);
    write_msgpack_i64(&mut scratch, family.root_revision_id);
    write_msgpack_i64(&mut scratch, family.root_pack_id);
    write_msgpack_i64(&mut scratch, family.root_source_ordinal);
    scratch.extend_from_slice(&family.root_packed_slots);
    slicer.write_framed(&scratch, true)?;

    for (col_spec, children) in plan
        .collections
        .iter()
        .zip(family.children_by_collection.iter())
    {
        if slicer.exceeded_native_cap {
            return Ok(V2FamilyOutput::Fallback);
        }
        // ["collection", col_name]
        scratch.clear();
        scratch.push(0x92);
        write_msgpack_str(&mut scratch, "collection");
        write_msgpack_str(&mut scratch, &col_spec.name);
        slicer.write_framed(&scratch, false)?;

        for child in children {
            if slicer.exceeded_native_cap {
                return Ok(V2FamilyOutput::Fallback);
            }
            // ["record", [rev_id, pack_id, source_ordinal, <packed_slots>]]
            scratch.clear();
            scratch.push(0x92);
            write_msgpack_str(&mut scratch, "record");
            scratch.push(0x94);
            write_msgpack_i64(&mut scratch, child.revision_id);
            write_msgpack_i64(&mut scratch, child.pack_id);
            write_msgpack_i64(&mut scratch, child.source_ordinal);
            scratch.extend_from_slice(&child.packed_slots);
            slicer.write_framed(&scratch, true)?;
        }

        // ["end", child_count]
        scratch.clear();
        scratch.push(0x92);
        write_msgpack_str(&mut scratch, "end");
        write_msgpack_i64(&mut scratch, children.len() as i64);
        slicer.write_framed(&scratch, false)?;
    }

    if slicer.exceeded_native_cap {
        return Ok(V2FamilyOutput::Fallback);
    }

    // ["complete", total_child_count]
    scratch.clear();
    scratch.push(0x92);
    write_msgpack_str(&mut scratch, "complete");
    write_msgpack_i64(&mut scratch, total_child_count as i64);
    slicer.write_framed(&scratch, false)?;
    if slicer.exceeded_native_cap {
        return Ok(V2FamilyOutput::Fallback);
    }
    slicer.flush();

    Ok(V2FamilyOutput::Fragmented {
        family_sha256,
        root_scalar_count,
        child_scalar_count,
        raw_bytes: slicer.raw_bytes,
        fragments: slicer.fragments,
    })
}

pub fn encode_v2_family_batch(
    plan: &V2FamilyPlan,
    families: &[V2FamilyInput],
    pool: Option<&ThreadPool>,
) -> Result<Vec<V2FamilyOutput>, &'static str> {
    if families.len() > MAX_NATIVE_V2_BATCH_FAMILIES {
        return Err("v2 family batch exceeds maximum family count");
    }
    if families.len() >= PARALLEL_FAMILY_THRESHOLD
        && let Some(pool) = pool
    {
        return pool.install(|| {
            families
                .par_iter()
                .map(|family| encode_single_family(plan, family))
                .collect()
        });
    }
    families
        .iter()
        .map(|family| encode_single_family(plan, family))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::custom_import_direct_stage::encode_msgpack_slots;
    use crate::custom_import_source::SourceValue;

    #[test]
    fn slots_to_json_matches_canonical_record_contract() {
        let fields = vec![
            V2FieldSpec::compile("npi", "string", true).unwrap(),
            V2FieldSpec::compile("score", "decimal", true).unwrap(),
            V2FieldSpec::compile("count", "integer", true).unwrap(),
            V2FieldSpec::compile("active", "boolean", false).unwrap(),
            V2FieldSpec::compile("note", "string", true).unwrap(),
        ];
        let packed = encode_msgpack_slots(&[
            SourceValue::Text("1003000126".to_owned()),
            SourceValue::Text("42.5".to_owned()),
            SourceValue::Integer(-7),
            SourceValue::Boolean(true),
            SourceValue::Missing,
        ]);
        let mut out = String::new();
        let scalars = slots_to_canonical_json(&fields, &packed, &mut out).unwrap();
        assert_eq!(scalars, 3);
        assert_eq!(
            out,
            concat!(
                "{\"contract\":\"custom-import-record/v1\",\"fields\":[",
                "{\"field\":\"npi\",\"value\":{\"state\":\"value\",\"type\":\"string\",\"value\":\"1003000126\"}},",
                "{\"field\":\"score\",\"value\":{\"state\":\"value\",\"type\":\"decimal\",\"value\":\"42.5\"}},",
                "{\"field\":\"count\",\"value\":{\"state\":\"value\",\"type\":\"integer\",\"value\":-7}},",
                "{\"field\":\"active\",\"value\":{\"state\":\"value\",\"type\":\"boolean\",\"value\":true}},",
                "{\"field\":\"note\",\"value\":{\"state\":\"missing\"}}",
                "]}"
            )
        );
    }

    #[test]
    fn encodes_inline_and_fragmented_families() {
        let root_fields = vec![
            V2FieldSpec::compile("npi", "string", true).unwrap(),
            V2FieldSpec::compile("score", "decimal", true).unwrap(),
        ];
        let root_slots = encode_msgpack_slots(&[
            SourceValue::Text("1003000126".to_owned()),
            SourceValue::Text("1.5".to_owned()),
        ]);
        let mut root_json = String::new();
        slots_to_canonical_json(&root_fields, &root_slots, &mut root_json).unwrap();
        let mut h = Sha256::new();
        h.update(ROOT_PAYLOAD_HASH_PREFIX);
        h.update(root_json.as_bytes());
        let root_payload_sha256: [u8; 32] = h.finalize().into();

        let inline_plan = V2FamilyPlan {
            definition_digest: "a".repeat(64),
            root_fields,
            collections: vec![],
            can_inline_root: true,
            fragment_bytes: 65_536,
            max_record_bytes: 1_048_576,
            max_spool_bytes: 16 * 1024 * 1024,
        };
        let input = V2FamilyInput {
            root_revision_id: 11,
            root_pack_id: 3,
            root_source_ordinal: 0,
            root_packed_slots: root_slots.clone(),
            root_key_canonical: "{\"contract\":\"custom-import-key/v1\",\"fields\":[]}".to_owned(),
            root_payload_sha256,
            children_by_collection: vec![],
        };
        let out = encode_v2_family_batch(&inline_plan, &[input], None).unwrap();
        assert!(matches!(
            out[0],
            V2FamilyOutput::Inline {
                root_scalar_count: 2,
                ..
            }
        ));

        let frag_plan = V2FamilyPlan {
            can_inline_root: false,
            ..inline_plan
        };
        let input2 = V2FamilyInput {
            root_revision_id: 11,
            root_pack_id: 3,
            root_source_ordinal: 0,
            root_packed_slots: root_slots,
            root_key_canonical: "{\"contract\":\"custom-import-key/v1\",\"fields\":[]}".to_owned(),
            root_payload_sha256,
            children_by_collection: vec![],
        };
        let out2 = encode_v2_family_batch(&frag_plan, &[input2], None).unwrap();
        match &out2[0] {
            V2FamilyOutput::Fragmented {
                root_scalar_count,
                child_scalar_count,
                fragments,
                ..
            } => {
                assert_eq!(*root_scalar_count, 2);
                assert_eq!(*child_scalar_count, 0);
                assert_eq!(fragments.len(), 1);
                assert_eq!(fragments[0].first_record, 0);
                assert_eq!(fragments[0].record_starts, 1);
            }
            _ => panic!("expected fragmented output"),
        }
    }
}
